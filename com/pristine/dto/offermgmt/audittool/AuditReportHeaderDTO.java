package com.pristine.dto.offermgmt.audittool;

public class AuditReportHeaderDTO {
	private long reportId;
	private long paramHeaderId;
	private char runType;
	private String startRunTime;
	private String endRunTime;
	private String percentCompleted;
	private String message;
	private long runId;
	private boolean updateRequired;
	private String auditBy;
	private String audited; 
	private long apVersionId;

	public long getApVersionId() {
		return apVersionId;
	}
	public void setApVersionId(long apVersionId) {
		this.apVersionId = apVersionId;
	}
	public long getReportId() {
		return reportId;
	}
	public void setReportId(long reportId) {
		this.reportId = reportId;
	}
	public long getParamHeaderId() {
		return paramHeaderId;
	}
	public void setParamHeaderId(long paramHeaderId) {
		this.paramHeaderId = paramHeaderId;
	}
	public char getRunType() {
		return runType;
	}
	public void setRunType(char runType) {
		this.runType = runType;
	}
	public String getStartRunTime() {
		return startRunTime;
	}
	public void setStartRunTime(String startRunTime) {
		this.startRunTime = startRunTime;
	}
	public String getEndRunTime() {
		return endRunTime;
	}
	public void setEndRunTime(String endRunTime) {
		this.endRunTime = endRunTime;
	}
	public String getPercentCompleted() {
		return percentCompleted;
	}
	public void setPercentCompleted(String percentCompleted) {
		this.percentCompleted = percentCompleted;
	}
	public String getMessage() {
		return message;
	}
	public void setMessage(String message) {
		this.message = message;
	}
	public long getRunId() {
		return runId;
	}
	public void setRunId(long runId) {
		this.runId = runId;
	}
	public boolean isUpdateRequired() {
		return updateRequired;
	}
	public void setUpdateRequired(boolean updateRequired) {
		this.updateRequired = updateRequired;
	}
	public String getAuditBy() {
		return auditBy;
	}
	public void setAuditBy(String auditBy) {
		this.auditBy = auditBy;
	}
	public String getAudited() {
		return audited;
	}
	public void setAudited(String audited) {
		this.audited = audited;
	}

}
