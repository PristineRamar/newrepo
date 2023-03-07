package com.pristine.dto.offermgmt;

public class PRExportRunHeader {
	private long exportId;
	private int status;// 0 - Submitted, 1 - InProgress, 2 - Completed, 3 - Error
	private String startTime;
	private String endTime;
	private String userId;
	private String exportFilePath;// - Absolute path
	private char runType;// - 'B' for execution via batch file or 'D' for executions from the dashboard.
	private String exportType;// - S, H, or SH
	private String effectiveDate;
	private int sfThreshold;
	
	public long getExportId() {
		return exportId;
	}
	public void setExportId(long exportId) {
		this.exportId = exportId;
	}
	public int getStatus() {
		return status;
	}
	public void setStatus(int status) {
		this.status = status;
	}
	public String getStartTime() {
		return startTime;
	}
	public void setStartTime(String startTime) {
		this.startTime = startTime;
	}
	public String getEndTime() {
		return endTime;
	}
	public void setEndTime(String endTime) {
		this.endTime = endTime;
	}
	public String getUserId() {
		return userId;
	}
	public void setUserId(String userId) {
		this.userId = userId;
	}
	public String getExportFilePath() {
		return exportFilePath;
	}
	public void setExportFilePath(String exportFilePath) {
		this.exportFilePath = exportFilePath;
	}
	public char getRunType() {
		return runType;
	}
	public void setRunType(char runType) {
		this.runType = runType;
	}
	public String getExportType() {
		return exportType;
	}
	public void setExportType(String exportType) {
		this.exportType = exportType;
	}
	public String getEffectiveDate() {
		return effectiveDate;
	}
	public void setEffectiveDate(String effectiveDate) {
		this.effectiveDate = effectiveDate;
	}
	public int getSfThreshold() {
		return sfThreshold;
	}
	public void setSfThreshold(int sfThreshold) {
		this.sfThreshold = sfThreshold;
	}

}
