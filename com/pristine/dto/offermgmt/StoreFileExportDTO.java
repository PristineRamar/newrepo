package com.pristine.dto.offermgmt;

public class StoreFileExportDTO implements Cloneable{
	private long runId;
	private int locationLevelId;
	private int locationId;
	private int productLevelId;
	private int productId;
	private String bannerCode;
	private String zoneNo;
	private String compStrNo;
	private String vendorNo;
	private String retItemCode;
	private int quantity;
	private double price;
	private int currentQuantity;
	private double currentPrice;
	private int compQuantity;
	private double compPrice;
	private String regEffectiveDate;
	private String NLPFlag;
	private String NLPEndDate;
	private String LPFlag;
	private String LPEndDate;
	private int strId;
	private String zoneName;
	private String deptId;
	private String catCode;

	
	// Added for Rite Aid - Begins
	private String userId;
	private String itemIndicator;
	private String UOMData;
	private String reasonCode;
	private String comment;
	private String timeStampData;
	private String approvalReason;
	// Added for Rite Aid - Ends

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

	public String getBannerCode() {
		return bannerCode;
	}
	public void setBannerCode(String bannerCode) {
		this.bannerCode = bannerCode;
	}
	
	public String getZoneNo() {
		return zoneNo;
	}
	public void setZoneNo(String zoneNo) {
		this.zoneNo = zoneNo;
	}
	
	public String getVendorNo() {
		return vendorNo;
	}
	public void setVendorNo(String vendorNo) {
		this.vendorNo = vendorNo;
	}	
	
	public String getCompStrNo() {
		return compStrNo;
	}
	public void setCompStrNo(String compStrNo) {
		this.compStrNo = compStrNo;
	}
	public String getRetItemCode() {
		return retItemCode;
	}
	public void setRetItemCode(String retItemCode) {
		this.retItemCode = retItemCode;
	}
	public int getQuantity() {
		return quantity;
	}
	public void setQuantity(int quantity) {
		this.quantity = quantity;
	}
	public double getPrice() {
		return price;
	}
	public void setPrice(double price) {
		this.price = price;
	}

	public int getCurrentQuantity() {
		return currentQuantity;
	}
	public void setCurrentQuantity(int currentQuantity) {
		this.currentQuantity = currentQuantity;
	}
	
	public double getCurrentPrice() {
		return currentPrice;
	}
	public void setCurrentPrice(double currentPrice) {
		this.currentPrice = currentPrice;
	}	
	
	public int getCompQuantity() {
		return compQuantity;
	}
	public void setCompQuantity(int compQuantity) {
		this.compQuantity = compQuantity;
	}

	public double getCompPrice() {
		return compPrice;
	}
	public void setCompPrice(double compPrice) {
		this.compPrice = compPrice;
	}	
	
	public String getRegEffectiveDate() {
		return regEffectiveDate;
	}
	public void setRegEffectiveDate(String regEffectiveDate) {
		this.regEffectiveDate = regEffectiveDate;
	}
	
	public String getNLPFlag() {
		return NLPFlag;
	}
	public void setNLPFlag(String NLPFlag) {
		this.NLPFlag = NLPFlag;
	}
	
	public String getNLPEndDate() {
		return NLPEndDate;
	}
	public void setNLPEndDate(String NLPEndDate) {
		this.NLPEndDate = NLPEndDate;
	}
	
	public String getLPFlag() {
		return LPFlag;
	}
	public void setLPFlag(String LPFlag) {
		this.LPFlag = LPFlag;
	}
	
	public String getLPEndDate() {
		return LPEndDate;
	}
	public void setLPEndDate(String LPEndDate) {
		this.LPEndDate = LPEndDate;
	}
	public String getApprovalReason() {
		return approvalReason;
	}
	public void setApprovalReason(String approvalReason) {
		this.approvalReason = approvalReason;
	}
	public String getUserId() {
		return userId;
	}
	public void setUserId(String userId) {
		this.userId = userId;
	}
	public String getItemIndicator() {
		return itemIndicator;
	}
	public void setItemIndicator(String itemIndicator) {
		this.itemIndicator = itemIndicator;
	}
	public String getUOMData() {
		return UOMData;
	}
	public void setUOMData(String uOMData) {
		UOMData = uOMData;
	}
	public String getReasonCode() {
		return reasonCode;
	}
	public void setReasonCode(String reasonCode) {
		this.reasonCode = reasonCode;
	}
	public String getComment() {
		return comment;
	}
	public void setComment(String comment) {
		this.comment = comment;
	}
	public String getTimeStampData() {
		return timeStampData;
	}
	public void setTimeStampData(String timeStampData) {
		this.timeStampData = timeStampData;
	}

	public int getStrId() {
		return strId;
	}

	public String getZoneName() {
		return zoneName;
	}
	public String getCatCode() {
		return catCode;
	}
	public void setCatCode(String catCode) {
		this.catCode = catCode;
	}
	public void setZoneName(String zoneName) {
		this.zoneName = zoneName;
	}
	public void setStrId(int strId) {
		this.strId = strId;
	}
	public String getDeptId() {
		return deptId;
	}
	public void setDeptId(String deptId) {
		this.deptId = deptId;
	}

	@Override
	public Object clone() throws CloneNotSupportedException {
		return super.clone();
	}
}

