package com.pristine.dataload.service;

import java.util.List;

import com.pristine.util.Constants;

public class DataInputForAPI {
	int priceTypeId;
	private int fetchStorePrice=0;
	private int locationLevelId = 0;
	private int locationId = 0;
	private String startDate = null;
	private String endDate = null;
	private List<Integer> itemCodes = null;
	private String calType;
	private String fiscalOrAdCalendar;
	private int costTypeId=Constants.REGULAR_COST_ID;
	private int productId;
	private List<Integer> calendarId=null;
	private int productLevelId;
	private boolean movingItemsPresent;
	private boolean fetchFuturePrice=false;
	private boolean fetchFutureCost=false;
	 
	public String getStartDate() {
		return startDate;
	}

	public void setStartDate(String startDate) {
		this.startDate = startDate;
	}

	public String getEndDate() {
		return endDate;
	}

	public void setEndDate(String endDate) {
		this.endDate = endDate;
	}

	

	public int getFetchStorePrice() {
		return fetchStorePrice;
	}

	public void setFetchStorePrice(int fetchStorePrice) {
		this.fetchStorePrice = fetchStorePrice;
	}



	public int getPriceTypeId() {
		return priceTypeId;
	}

	public void setPriceTypeId(int priceTypeId) {
		this.priceTypeId = priceTypeId;
	}

	public String getCalType() {
		return calType;
	}

	public void setCalType(String calType) {
		this.calType = calType;
	}

	public String getFiscalOrAdCalendar() {
		return fiscalOrAdCalendar;
	}

	public void setFiscalOrAdCalendar(String fiscalOrAdCalendar) {
		this.fiscalOrAdCalendar = fiscalOrAdCalendar;
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

	
	public List<Integer> getItemCodes() {
		return itemCodes;
	}

	public void setItemCodes(List<Integer> itemCodes) {
		this.itemCodes = itemCodes;
	}

	public int getCostTypeId() {
		return costTypeId;
	}

	public int getProductId() {
		return productId;
	}

	public void setCostTypeId(int costTypeId) {
		this.costTypeId = costTypeId;
	}

	public void setProductId(int productId) {
		this.productId = productId;
	}

	public List<Integer> getCalendarId() {
		return calendarId;
	}

	public void setCalendarId(List<Integer> calendarId) {
		this.calendarId = calendarId;
	}

	public int getProductLevelId() {
		return productLevelId;
	}

	public void setProductLevelId(int productLevelId) {
		this.productLevelId = productLevelId;
	}

	public boolean isMovingItemsPresent() {
		return movingItemsPresent;
	}

	public void setMovingItemsPresent(boolean movingItemsPresent) {
		this.movingItemsPresent = movingItemsPresent;
	}
	
	
	public boolean isFetchFuturePrice() {
		return fetchFuturePrice;
	}

	public void setFetchFuturePrice(boolean fetchFuturePrice) {
		this.fetchFuturePrice = fetchFuturePrice;
	}

	public boolean isFetchFutureCost() {
		return fetchFutureCost;
	}

	public void setFetchFutureCost(boolean fetchFutureCost) {
		this.fetchFutureCost = fetchFutureCost;
	}

	@Override
	public String toString() {
		return "DataInputForAPI [priceTypeId=" + priceTypeId + ", fetchStorePrice=" + fetchStorePrice
				+ ", locationLevelId=" + locationLevelId + ", locationId=" + locationId + ", startDate=" + startDate
				+ ", endDate=" + endDate + ", itemCodes=" + itemCodes + ", calType=" + calType + ", fiscalOrAdCalendar="
				+ fiscalOrAdCalendar + ", costTypeId=" + costTypeId + ", productId=" + productId + ", calendarId="
				+ calendarId + ", productLevelId=" + productLevelId + "]";
	}

	
	

}
