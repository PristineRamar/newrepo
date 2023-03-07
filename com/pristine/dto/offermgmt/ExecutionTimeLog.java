package com.pristine.dto.offermgmt;

import java.text.DateFormat;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.List;

public class ExecutionTimeLog {

	private long runId;
	private int calendarId;
	private int locationLevelId;
	private int locationId;
	private int productLevelId;
	private int productId;
	private int executionOrder;
	private String functionalityName = "";
	private Date startTime = null;
	private Date endTime = null;
	private String totalTime = null;
	private long totalTimeSeconds = 0;
	private int pricePoints = 0;
	DateFormat dateFormat = new SimpleDateFormat("MM/dd/yyyy HH:mm:ss.SSS");
	
	public ExecutionTimeLog(String functionalityName) {
		this.executionOrder = ++ExecutionOrder.currentExecutionNumber;
		this.functionalityName = functionalityName;
		try {
			this.startTime = new SimpleDateFormat("MM/dd/yyyy HH:mm:ss.SSS").parse(dateFormat.format(new Date()));
		} catch (ParseException e) {
		}
	}
	
	public long getRunId() {
		return runId;
	}
	public void setRunId(long runId) {
		this.runId = runId;
	}
	public String getFunctionalityName() {
		return functionalityName;
	}
	public void setFunctionalityName(String functionalityName) {
		this.functionalityName = functionalityName;
	}
	public Date getStartTime() {
		return startTime;
	}
	public void setStartTime(Date startTime) {
		this.startTime = startTime;
	}
	public Date getEndTime() {
		return endTime;
	}	 
 
	public int getPricePoints() {
		return pricePoints;
	}
	public void setPricePoints(int pricePoints) {
		this.pricePoints = pricePoints;
	}

	public String getTotalTime() {
		if (this.startTime != null && this.endTime != null) {
			// in milliseconds
			long diffMilliSeconds = this.endTime.getTime() - this.startTime.getTime();
			long diffSeconds = diffMilliSeconds / 1000 % 60;
			long diffMinutes = diffMilliSeconds / (60 * 1000) % 60;
			long diffHours = diffMilliSeconds / (60 * 60 * 1000) % 24;

			totalTime = diffHours + ":" + diffMinutes + ":" + diffSeconds + ":" + diffMilliSeconds;
		}
		return totalTime;
	}
	
	public void setEndTime() {
		try {
			this.endTime = new SimpleDateFormat("MM/dd/yyyy HH:mm:ss.SSS").parse(dateFormat.format(new Date()));
		} catch (ParseException e) {
		}
	}

	public int getExecutionOrder() {
		return executionOrder;
	}

	public void setExecutionOrder(int executionOrder) {
		this.executionOrder = executionOrder;
	}

	public int getCalendarId() {
		return calendarId;
	}

	public void setCalendarId(int calendarId) {
		this.calendarId = calendarId;
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

	public long getTotalTimeSeconds() {
		if (this.startTime != null && this.endTime != null) {
			// in milliseconds
			long diffMilliSeconds = this.endTime.getTime() - this.startTime.getTime();
			long diffSeconds = diffMilliSeconds / 1000 % 60;
			long diffMinutes = diffMilliSeconds / (60 * 1000) % 60;
			long diffHours = diffMilliSeconds / (60 * 60 * 1000) % 24;
			
			totalTimeSeconds = diffSeconds + (diffMinutes * 60) + (diffHours * 60 * 60);
		}
		return totalTimeSeconds;
	}	
	 
	public void fillRecommendationHeader(PRRecommendationRunHeader recommendationRunHeader, List<ExecutionTimeLog> executionTimeLogs){
		for (ExecutionTimeLog executionTimeLog : executionTimeLogs) {
			executionTimeLog.runId = recommendationRunHeader.getRunId();
			executionTimeLog.calendarId = recommendationRunHeader.getCalendarId();
			executionTimeLog.locationLevelId = recommendationRunHeader.getLocationLevelId();
			executionTimeLog.locationId = recommendationRunHeader.getLocationId();
			executionTimeLog.productLevelId = recommendationRunHeader.getProductLevelId();
			executionTimeLog.productId = recommendationRunHeader.getProductId();
		}
	}
}
