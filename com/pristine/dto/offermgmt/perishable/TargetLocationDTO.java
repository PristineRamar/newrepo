package com.pristine.dto.offermgmt.perishable;

public class TargetLocationDTO {

	private int zoneId;
	private int storeId;
	private String zoneNum;
	private String storeNo;

	public int getZoneId() {
		return zoneId;
	}
	public void setZoneId(int zoneId) {
		this.zoneId = zoneId;
	}
	public int getStoreId() {
		return storeId;
	}
	public void setStoreId(int storeId) {
		this.storeId = storeId;
	}

	public String getZoneNum() {
		return zoneNum;
	}
	public void setZoneNum(String zoneNum) {
		this.zoneNum = zoneNum;
	}
	public String getStoreNo() {
		return storeNo;
	}
	public void setStoreNo(String storeNo) {
		this.storeNo = storeNo;
	}
}
