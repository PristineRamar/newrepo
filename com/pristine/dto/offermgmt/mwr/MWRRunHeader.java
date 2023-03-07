package com.pristine.dto.offermgmt.mwr;

import com.pristine.util.offermgmt.PRConstants;

public class MWRRunHeader {

	
	/*
	
	PR_QUARTER_REC_HEADER
	
	RUN_ID, LOCATION_LEVEL_ID, LOCATION_ID, PRODUCT_LEVEL_ID, PRODUCT_ID, START_CALENDAR_ID, 
	END_CALENDAR_ID, ACTUAL_START_CALENDAR_ID, ACTUAL_END_CALENDAR_ID, RUN_TYPE, START_RUN_TIME, 
	END_RUN_TIME, PERCENT_COMPLETION, MESSAGE, RUN_STATUS, PREDICTED_BY, PREDICTED, PARENT_RUN_ID
	
	
	*/

	
	private long runId;
	private int locationLevelId;
	private int locationId;
	private int productLevelId;
	private int productId;
	private int startCalendarId;
	private int endCalendarId;
	private int actualStartCalendarId;
	private int actualEndCalendarId;
	private String runType;
	private String startRunTime;
	private String endRunTime;
	private int percentCompleted;
	private String message;
	private String runStatus;
	private String predictedBy;
	private String predicted;
	private long parentRunId;
	
	
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
	public int getActualStartCalendarId() {
		return actualStartCalendarId;
	}
	public void setActualStartCalendarId(int actualStartCalendarId) {
		this.actualStartCalendarId = actualStartCalendarId;
	}
	public int getActualEndCalendarId() {
		return actualEndCalendarId;
	}
	public void setActualEndCalendarId(int actualEndCalendarId) {
		this.actualEndCalendarId = actualEndCalendarId;
	}
	public String getRunType() {
		return runType;
	}
	public void setRunType(String runType) {
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
	public int getPercentCompleted() {
		return percentCompleted;
	}
	public void setPercentCompleted(int percentCompleted) {
		this.percentCompleted = percentCompleted;
	}
	public String getMessage() {
		return message;
	}
	public void setMessage(String message) {
		this.message = message;
	}
	public String getRunStatus() {
		return runStatus;
	}
	public void setRunStatus(String runStatus) {
		this.runStatus = runStatus;
	}
	public String getPredictedBy() {
		return predictedBy;
	}
	public void setPredictedBy(String predictedBy) {
		this.predictedBy = predictedBy;
	}
	public String getPredicted() {
		return predicted;
	}
	public void setPredicted(String predicted) {
		this.predicted = predicted;
	}
	public long getParentRunId() {
		return parentRunId;
	}
	public void setParentRunId(long parentRunId) {
		this.parentRunId = parentRunId;
	}
	
	
	public static MWRRunHeader getRunHeaderDTO(RecommendationInputDTO recommendationInputDTO, long runId,
			long parentRunId) {
		
		MWRRunHeader mwrRunHeader = new MWRRunHeader();
		
		mwrRunHeader.setRunId(runId);
		mwrRunHeader.setLocationLevelId(recommendationInputDTO.getLocationLevelId());
		mwrRunHeader.setLocationId(recommendationInputDTO.getLocationId());
		mwrRunHeader.setProductLevelId(recommendationInputDTO.getProductLevelId());
		mwrRunHeader.setProductId(recommendationInputDTO.getProductId());
		mwrRunHeader.setStartCalendarId(recommendationInputDTO.getStartCalendarId());
		mwrRunHeader.setEndCalendarId(recommendationInputDTO.getEndCalendarId());
		mwrRunHeader.setActualStartCalendarId(recommendationInputDTO.getActualStartCalendarId());
		mwrRunHeader.setActualEndCalendarId(recommendationInputDTO.getActualEndCalendarId());
		mwrRunHeader.setRunType(String.valueOf(recommendationInputDTO.getRunType()));
		mwrRunHeader.setPercentCompleted(0);
		mwrRunHeader.setPredictedBy(recommendationInputDTO.getUserId());
		mwrRunHeader.setParentRunId(parentRunId);
		
		return mwrRunHeader;
	}
}
