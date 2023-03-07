package com.pristine.dto.offermgmt;

public class PRRecommendationRunHeader {
	private long runId = 0;
	private int calendarId = 0;
	private int notificationType =0;
	public int locationLevelId = 0;
	public int locationId = 0;
	public int productLevelId = 0;
	public int productId = 0;
	private Boolean isAutomaticRecommendation = true;
	private Boolean isPriceZone = true;
	private int curRetailCalendarId = 0;
	private String runType = "";
	private String predictedBy = "";
	private String startRunTime = "";
	private String startDate = "";
	private String endDate = "";
	private int currentStatusRoleId;
	private int currentStatusWorkflowLevel;
	private int approvalRoleId;
	private int approvalWorkflowLevel;
	private int sortingOder;
	private int nextOrder;
	
	public int getSortingOder() {
		return sortingOder;
	}

	public void setSortingOder(int sortingOder) {
		this.sortingOder = sortingOder;
	}

	public int getNextOrder() {
		return nextOrder;
	}

	public void setNextOrder(int nextOrder) {
		this.nextOrder = nextOrder;
	}

	public long getRunId() {
		return runId;
	}

	public void setRunId(long runId) {
		this.runId = runId;
	}

	public int getLocationLevelId() {
		return locationLevelId;
	}

	public void setLocationLevelId(int locationLevelId) {
		this.locationLevelId = locationLevelId;
	}

	public int getLocationId() {
		return locationId;
	}

	public void setLocationId(int locationId) {
		this.locationId = locationId;
	}

	public int getProductLevelId() {
		return productLevelId;
	}

	public void setProductLevelId(int productLevelId) {
		this.productLevelId = productLevelId;
	}

	public int getProductId() {
		return productId;
	}

	public void setProductId(int productId) {
		this.productId = productId;
	}

	public int getCalendarId() {
		return calendarId;
	}

	public void setCalendarId(int calendarId) {
		this.calendarId = calendarId;
	}

	public Boolean getIsAutomaticRecommendation() {
		return isAutomaticRecommendation;
	}

	public void setIsAutomaticRecommendation(Boolean isAutomaticRecommendation) {
		this.isAutomaticRecommendation = isAutomaticRecommendation;
	}

	public Boolean getIsPriceZone() {
		return isPriceZone;
	}

	public void setIsPriceZone(Boolean isPriceZone) {
		this.isPriceZone = isPriceZone;
	}

	public int getCurRetailCalendarId() {
		return curRetailCalendarId;
	}

	public void setCurRetailCalendarId(int curRetailCalendarId) {
		this.curRetailCalendarId = curRetailCalendarId;
	}

	public String getRunType() {
		return runType;
	}

	public void setRunType(String runType) {
		this.runType = runType;
	}

	public int getNotificationType() {
		return notificationType;
	}

	public void setNotificationType(int notificationType) {
		this.notificationType = notificationType;
	}

	public String getPredictedBy() {
		return predictedBy;
	}

	public void setPredictedBy(String predictedBy) {
		this.predictedBy = predictedBy;
	}

	public String getStartRunTime() {
		return startRunTime;
	}

	public void setStartRunTime(String startRunTime) {
		this.startRunTime = startRunTime;
	}

	public String getStartDate() {
		return startDate;
	}

	public void setStartDate(String startDate) {
		this.startDate = startDate;
	}

	public String getEndDate() {
		return endDate;
	}

	public void setEndDate(String endDate) {
		this.endDate = endDate;
	}

	public int getCurrentStatusRoleId() {
		return currentStatusRoleId;
	}

	public void setCurrentStatusRoleId(int currentStatusRoleId) {
		this.currentStatusRoleId = currentStatusRoleId;
	}

	public int getCurrentStatusWorkflowLevel() {
		return currentStatusWorkflowLevel;
	}

	public void setCurrentStatusWorkflowLevel(int currentStatusWorkflowLevel) {
		this.currentStatusWorkflowLevel = currentStatusWorkflowLevel;
	}

	public int getApprovalRoleId() {
		return approvalRoleId;
	}

	public void setApprovalRoleId(int approvalRoleId) {
		this.approvalRoleId = approvalRoleId;
	}

	public int getApprovalWorkflowLevel() {
		return approvalWorkflowLevel;
	}

	public void setApprovalWorkflowLevel(int approvalWorkflowLevel) {
		this.approvalWorkflowLevel = approvalWorkflowLevel;
	}


}
