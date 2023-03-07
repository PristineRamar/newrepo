package com.pristine.dto.offermgmt.prediction;

public class StuckRunDTO {
	
	private long runId;
	private String runType;
	private String runStatus;
	private int progressPercent;
	private String startTime;
	private String startedBy;
	private String userId;
	private String product;
	private String productId;
	private String location;
	private int locationId;
	private String message;
	
	@Override
	public String toString() {
		
		return Long.toString(runId) + ',' + getRunType() + ',' + getRunStatus() + ',' + getProgressPercent() + ',' + 
				getStartTime()+ ',' + getStartedBy() + ',' + getUserId() + ',' + getProduct() + ',' + 
				getProductId() + ',' + getLocation() + ',' + getLocationId() + ',' + getMessage();
	}
	
	/**
	 * @return the runId
	 */
	public long getRunId() {
		return runId;
	}
	/**
	 * @param runId the runId to set
	 */
	public StuckRunDTO setRunId(long runId) {
		this.runId = runId;
		return this;
	}
	/**
	 * @return the runType
	 */
	public String getRunType() {
		return runType;
	}
	/**
	 * @param runType the runType to set
	 */
	public StuckRunDTO setRunType(String runType) {
		this.runType = runType;
		return this;
	}
	/**
	 * @return the runStatus
	 */
	public String getRunStatus() {
		return runStatus;
	}
	/**
	 * @param runStatus the runStatus to set
	 */
	public StuckRunDTO setRunStatus(String runStatus) {
		this.runStatus = runStatus;
		return this;
	}
	/**
	 * @return the progressPercent
	 */
	public int getProgressPercent() {
		return progressPercent;
	}
	/**
	 * @param progressPercent the progressPercent to set
	 */
	public StuckRunDTO setProgressPercent(int progressPercent) {
		this.progressPercent = progressPercent;
		return this;
	}
	/**
	 * @return the startTime
	 */
	public String getStartTime() {
		return startTime;
	}
	/**
	 * @param startTime the startTime to set
	 */
	public StuckRunDTO setStartTime(String startTime) {
		this.startTime = startTime;
		return this;
	}
	/**
	 * @return the startedBy
	 */
	public String getStartedBy() {
		return startedBy;
	}
	/**
	 * @param startedBy the startedBy to set
	 */
	public StuckRunDTO setStartedBy(String startedBy) {
		this.startedBy = startedBy;
		return this;
	}
	/**
	 * @return the userId
	 */
	public String getUserId() {
		return userId;
	}
	/**
	 * @param userId the userId to set
	 */
	public StuckRunDTO setUserId(String userId) {
		this.userId = userId;
		return this;
	}
	/**
	 * @return the product
	 */
	public String getProduct() {
		return product;
	}
	/**
	 * @param product the product to set
	 */
	public StuckRunDTO setProduct(String product) {
		this.product = product;
		return this;
	}
	/**
	 * @return the productId
	 */
	public String getProductId() {
		return productId;
	}
	/**
	 * @param productId the productId to set
	 */
	public StuckRunDTO setProductId(String productId) {
		this.productId = productId;
		return this;
	}
	/**
	 * @return the location
	 */
	public String getLocation() {
		return location;
	}
	/**
	 * @param location the location to set
	 */
	public StuckRunDTO setLocation(String location) {
		this.location = location;
		return this;
	}
	/**
	 * @return the locationId
	 */
	public int getLocationId() {
		return locationId;
	}
	/**
	 * @param locationId the locationId to set
	 */
	public StuckRunDTO setLocationId(int locationId) {
		this.locationId = locationId;
		return this;
	}
	/**
	 * @return the message
	 */
	public String getMessage() {
		return message;
	}
	/**
	 * @param message the message to set
	 */
	public StuckRunDTO setMessage(String message) {
		this.message = message;
		return this;
	}
	
}
