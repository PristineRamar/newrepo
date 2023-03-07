package com.pristine.dto.offermgmt.prediction;

import java.util.Date;

public class PredictionRunHeaderDTO {
	private long runId;
	private Date startRunTime;
	private Date endRunTime;
	private String runType;
	private String runStatus;
	private int percentageCompleted;
	private String message;
	private Date predicted;
	private String predictedBy;	
	private int startCalendarId;
	private int endCalendarId;
	private int locationLevelId;
	private int locationId;
	
	public long getRunId() {
		return runId;
	}
	public void setRunId(long runId) {
		this.runId = runId;
	}
	public Date getStartRunTime() {
		return startRunTime;
	}
	public void setStartRunTime(Date startRunTime) {
		this.startRunTime = startRunTime;
	}
	public Date getEndRunTime() {
		return endRunTime;
	}
	public void setEndRunTime(Date endRunTime) {
		this.endRunTime = endRunTime;
	}
	public String getRunType() {
		return runType;
	}
	public void setRunType(String runType) {
		this.runType = runType;
	}
	public String getRunStatus() {
		return runStatus;
	}
	public void setRunStatus(String runStatus) {
		this.runStatus = runStatus;
	}
	public int getPercentageCompleted() {
		return percentageCompleted;
	}
	public void setPercentageCompleted(int percentageCompleted) {
		this.percentageCompleted = percentageCompleted;
	}
	public String getMessage() {
		return message;
	}
	public void setMessage(String message) {
		this.message = message;
	}
	public Date getPredicted() {
		return predicted;
	}
	public void setPredicted(Date predicted) {
		this.predicted = predicted;
	}
	public String getPredictedBy() {
		return predictedBy;
	}
	public void setPredictedBy(String predictedBy) {
		this.predictedBy = predictedBy;
	}
	public int getStartCalendarId() {
		return startCalendarId;
	}
	public void setStartCalendarId(int startCalendarId) {
		this.startCalendarId = startCalendarId;
	}
	public int getEndCalendarId() {
		return endCalendarId;
	}
	public void setEndCalendarId(int endCalendarId) {
		this.endCalendarId = endCalendarId;
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
	
	 
}
