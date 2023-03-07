package com.pristine.dto.offermgmt.audittool;

public class AuditDashboardDTO {
	private long dashboardId;
	private int locationLevelId;
	private int locationId;
	private int productLevelId;
	private int productId;
	private long paramHeaderId;
	private long reportId;
	
	public long getDashboardId() {
		return dashboardId;
	}
	public void setDashboardId(long dashboardId) {
		this.dashboardId = dashboardId;
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
	public long getParamHeaderId() {
		return paramHeaderId;
	}
	public void setParamHeaderId(long paramHeaderId) {
		this.paramHeaderId = paramHeaderId;
	}
	public long getReportId() {
		return reportId;
	}
	public void setReportId(long reportId) {
		this.reportId = reportId;
	}
}
