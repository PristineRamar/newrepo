package com.pristine.dto.offermgmt.prediction;

public class AccuracyComparisonHeaderDTO {
	private long comparisonHeaderId;
	private int calendarId;
	private int locationLevelId;
	private int locationId;
	private int productLevelId;
	private int productId;
	private int reportTypeId;
	private int subReportTypeId;
	
	public long getComparisonHeaderId() {
		return comparisonHeaderId;
	}
	public void setComparisonHeaderId(long comparisonHeaderId) {
		this.comparisonHeaderId = comparisonHeaderId;
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
	public int getReportTypeId() {
		return reportTypeId;
	}
	public void setReportTypeId(int reportTypeId) {
		this.reportTypeId = reportTypeId;
	}
	public int getSubReportTypeId() {
		return subReportTypeId;
	}
	public void setSubReportTypeId(int subReportTypeId) {
		this.subReportTypeId = subReportTypeId;
	}
}
