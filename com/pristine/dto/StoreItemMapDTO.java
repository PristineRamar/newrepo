package com.pristine.dto;

public class StoreItemMapDTO {

	private String retItemCode;
	private String compStrNo;
	private String itemStatus;
	private String version;
	private String authRec;
	private String isAuthorized;
	private int itemCode;
	private int compStrId;
	private String vendorNo;
	private String bannerCode;
	private String zoneNum;
	private String prcGrpCode;
	private String zoneNumCombined;
	private int priceZoneId;
	private Long vendorId;
	private String distFlag;
	public void copy(StoreItemMapDTO tempDTO){
		this.retItemCode = tempDTO.retItemCode;
		this.itemCode = tempDTO.itemCode;
		this.compStrId = tempDTO.compStrId;
		this.compStrNo = tempDTO.compStrNo;
		this.version = tempDTO.version;
		this.authRec = tempDTO.authRec;
		this.isAuthorized = tempDTO.isAuthorized;
		this.itemStatus = tempDTO.itemStatus;
		this.bannerCode = tempDTO.bannerCode;
		this.zoneNum = tempDTO.zoneNum;
		this.prcGrpCode = tempDTO.prcGrpCode;
		this.vendorNo = tempDTO.vendorNo;
		this.zoneNumCombined = tempDTO.zoneNumCombined;
		this.priceZoneId = tempDTO.priceZoneId;
		this.vendorId = tempDTO.vendorId;
		this.distFlag = tempDTO.distFlag;
	}
	
	public String getRetItemCode() {
		return retItemCode;
	}

	public void setRetItemCode(String retItemCode) {
		this.retItemCode = retItemCode;
	}

	public String getCompStrNo() {
		return compStrNo;
	}

	public void setCompStrNo(String compStrNo) {
		this.compStrNo = compStrNo;
	}

	public String getItemStatus() {
		return itemStatus;
	}

	public void setItemStatus(String itemStatus) {
		this.itemStatus = itemStatus;
	}

	public String getVersion() {
		return version;
	}

	public void setVersion(String version) {
		this.version = version;
	}

	public String getAuthRec() {
		return authRec;
	}

	public void setAuthRec(String authRec) {
		this.authRec = authRec;
	}

	public String getIsAuthorized() {
		return isAuthorized;
	}

	public void setIsAuthorized(String isAuthorized) {
		this.isAuthorized = isAuthorized;
	}

	public int getItemCode() {
		return itemCode;
	}

	public void setItemCode(int itemCode) {
		this.itemCode = itemCode;
	}

	public int getCompStrId() {
		return compStrId;
	}

	public void setCompStrId(int compStrId) {
		this.compStrId = compStrId;
	}

	public String getVendorNo() {
		return vendorNo;
	}

	public void setVendorNo(String vendorNo) {
		this.vendorNo = vendorNo;
	}

	public String getBannerCode() {
		return bannerCode;
	}

	public void setBannerCode(String bannerCode) {
		this.bannerCode = bannerCode;
	}

	public String getZoneNum() {
		return zoneNum;
	}

	public void setZoneNum(String zoneNum) {
		this.zoneNum = zoneNum;
	}

	public String getPrcGrpCode() {
		return prcGrpCode;
	}

	public void setPrcGrpCode(String prcGrpCode) {
		this.prcGrpCode = prcGrpCode;
	}

	public String getZoneNumCombined() {
		return zoneNumCombined;
	}

	public void setZoneNumCombined(String zoneNumCombined) {
		this.zoneNumCombined = zoneNumCombined;
	}

	public int getPriceZoneId() {
		return priceZoneId;
	}

	public void setPriceZoneId(int priceZoneId) {
		this.priceZoneId = priceZoneId;
	}

	public Long getVendorId() {
		return vendorId;
	}

	public void setVendorId(Long vendorId) {
		this.vendorId = vendorId;
	}

	public String getDistFlag() {
		return distFlag;
	}

	public void setDistFlag(String distFlag) {
		this.distFlag = distFlag;
	}
    
}
