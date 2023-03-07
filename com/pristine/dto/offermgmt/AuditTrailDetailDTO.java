package com.pristine.dto.offermgmt;


public class AuditTrailDetailDTO {

	private long auditTrailId;
	private long locationId;
	private int locationLevelId;
	private long productId;
	private long productLevelId;
	private long auditKey1;
	private int auditTrailType;
	private int statusType;
	private String userId;
	private int auditType;
	private int auditSubType;
	private int auditSubStatus;
	private int exportItemCount;
	private int exportStoreCount;
	private int exportTime;

	public int getExportTime() {
		return exportTime;
	}

	public void setExportTime(int exportTime) {
		this.exportTime = exportTime;
	}

	public int getExportItemCount() {
		return exportItemCount;
	}

	public void setExportItemCount(int exportItemCount) {
		this.exportItemCount = exportItemCount;
	}

	public int getExportStoreCount() {
		return exportStoreCount;
	}

	public void setExportStoreCount(int exportStoreCount) {
		this.exportStoreCount = exportStoreCount;
	}

	private int auditKey3;
	
	public int getAuditKey3() {
		return auditKey3;
	}

	public void setAuditKey3(int auditKey3) {
		this.auditKey3 = auditKey3;
	}

	public long getAuditTrailId() {
		return auditTrailId;
	}

	public void setAuditTrailId(long auditTrailId) {
		this.auditTrailId = auditTrailId;
	}

	public long getLocationId() {
		return locationId;
	}

	public void setLocationId(long locationId) {
		this.locationId = locationId;
	}

	public int getLocationLevelId() {
		return locationLevelId;
	}

	public void setLocationLevelId(int locationLevelId) {
		this.locationLevelId = locationLevelId;
	}

	public long getProductId() {
		return productId;
	}

	public void setProductId(long productId) {
		this.productId = productId;
	}

	public long getProductLevelId() {
		return productLevelId;
	}

	public void setProductLevelId(long productLevelId) {
		this.productLevelId = productLevelId;
	}

	public String getUserId() {
		return userId;
	}

	public void setUserId(String userId) {
		this.userId = userId;
	}

	public long getAuditKey1() {
		return auditKey1;
	}

	public void setAuditKey1(long auditKey1) {
		this.auditKey1 = auditKey1;
	}

	public int getAuditTrailType() {
		return auditTrailType;
	}

	public void setAuditTrailType(int auditTrailType) {
		this.auditTrailType = auditTrailType;
	}

	public int getStatusType() {
		return statusType;
	}

	public void setStatusType(int statusType) {
		this.statusType = statusType;
	}

	public int getAuditType() {
		return auditType;
	}

	public void setAuditType(int auditType) {
		this.auditType = auditType;
	}

	public int getAuditSubType() {
		return auditSubType;
	}

	public void setAuditSubType(int auditSubType) {
		this.auditSubType = auditSubType;
	}

	public int getAuditSubStatus() {
		return auditSubStatus;
	}

	public void setAuditSubStatus(int auditSubStatus) {
		this.auditSubStatus = auditSubStatus;
	}

	

	
}
