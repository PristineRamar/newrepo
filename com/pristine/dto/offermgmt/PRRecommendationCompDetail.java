package com.pristine.dto.offermgmt;

public class PRRecommendationCompDetail {
	private long runId = 0;
	private int locationLevelId = 0;
	private int locationId = 0;
	private int totalItems = 0;
	private int totalDistinctItems = 0;
	private Double currentSimpleIndex = null;
	private Double futureSimpleIndex = null;
	private Integer priceCheckListId = null;
	
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
	public int getTotalItems() {
		return totalItems;
	}
	public void setTotalItems(int totalItems) {
		this.totalItems = totalItems;
	}
	public int getTotalDistinctItems() {
		return totalDistinctItems;
	}
	public void setTotalDistinctItems(int totalDistinctItems) {
		this.totalDistinctItems = totalDistinctItems;
	}
	public Double getCurrentSimpleIndex() {
		return currentSimpleIndex;
	}
	public void setCurrentSimpleIndex(Double currentSimpleIndex) {
		this.currentSimpleIndex = currentSimpleIndex;
	}
	public Double getFutureSimpleIndex() {
		return futureSimpleIndex;
	}
	public void setFutureSimpleIndex(Double futureSimpleIndex) {
		this.futureSimpleIndex = futureSimpleIndex;
	}
	public Integer getPriceCheckListId() {
		return priceCheckListId;
	}
	public void setPriceCheckListId(Integer priceCheckListId) {
		this.priceCheckListId = priceCheckListId;
	}
	
}
