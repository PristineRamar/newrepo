package com.pristine.dto;

public class PriceTestDTO {

	private long priceTestID;
	private String priceTestName;
	private int locationId;
	private int locationLevelID;
	private int storeListLocationLevelId;
	private int storeListLocationId;
	private int productId;
	private int productLevelID;
	private int priceCheckListID;
	private String requestorName;
	private String Date;
	private int status;
	private boolean isActive;
	private String zoneNum;

	public long getPriceTestID() {
		return priceTestID;
	}

	public void setPriceTestID(long priceTestID) {
		this.priceTestID = priceTestID;
	}


	public String getPriceTestName() {
		return priceTestName;
	}

	public void setPriceTestName(String priceTestName) {
		this.priceTestName = priceTestName;
	}

	public int getLocationId() {
		return locationId;
	}

	public void setLocationId(int locationId) {
		this.locationId = locationId;
	}

	public int getLocationLevelID() {
		return locationLevelID;
	}

	public void setLocationLevelID(int locationLevelID) {
		this.locationLevelID = locationLevelID;
	}

	public int getStoreListLocationLevelId() {
		return storeListLocationLevelId;
	}

	public void setStoreListLocationLevelId(int storeListLocationLevelId) {
		this.storeListLocationLevelId = storeListLocationLevelId;
	}

	public int getStoreListLocationId() {
		return storeListLocationId;
	}

	public void setStoreListLocationId(int storeListLocationId) {
		this.storeListLocationId = storeListLocationId;
	}

	public int getProductId() {
		return productId;
	}

	public void setProductId(int productId) {
		this.productId = productId;
	}

	public int getProductLevelID() {
		return productLevelID;
	}

	public void setProductLevelID(int productLevelID) {
		this.productLevelID = productLevelID;
	}

	public int getPriceCheckListID() {
		return priceCheckListID;
	}

	public void setPriceCheckListID(int priceCheckListID) {
		this.priceCheckListID = priceCheckListID;
	}

	public String getRequestorName() {
		return requestorName;
	}

	public void setRequestorName(String requestorName) {
		this.requestorName = requestorName;
	}

	public String getDate() {
		return Date;
	}

	public void setDate(String date) {
		Date = date;
	}

	public int getStatus() {
		return status;
	}

	public void setStatus(int status) {
		this.status = status;
	}

	public boolean isActive() {
		return isActive;
	}

	public void setActive(boolean isActive) {
		this.isActive = isActive;
	}

	public String getZoneNum() {
		return zoneNum;
	}

	public void setZoneNum(String zoneNum) {
		this.zoneNum = zoneNum;
	}

}
