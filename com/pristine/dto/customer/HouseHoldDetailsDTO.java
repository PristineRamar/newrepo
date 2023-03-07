package com.pristine.dto.customer;

public class HouseHoldDetailsDTO {
	private String householdNumber; 
	private String storeId;
	private String ItemId;
	private String monthInfo;
	private Double amountSpendByHH;
	private long noOfVisits;
	private long quantity;
	private long plItems;
	private long activeItemsPerMonth;
	private long totalItemsByHH;
	private long noOfItemsInMonth;
	private Double itemsPI;
	private Double finalPrice;
	private String houseHoldFirstTrans;
	private Double piBasedOnPreviousMonth;
	private Double estimatedItemPrice;
	private Double weight;
	public String getHouseholdNumber() {
		return householdNumber;
	}
	public void setHouseholdNumber(String householdNumber) {
		this.householdNumber = householdNumber;
	}
	public String getStoreId() {
		return storeId;
	}
	public void setStoreId(String storeId) {
		this.storeId = storeId;
	}
	public String getMonthInfo() {
		return monthInfo;
	}
	public void setMonthInfo(String monthInfo) {
		this.monthInfo = monthInfo;
	}
	public Double getAmountSpendByHH() {
		return amountSpendByHH;
	}
	public void setAmountSpendByHH(Double amountSpendByHH) {
		this.amountSpendByHH = amountSpendByHH;
	}
	public long getNoOfVisits() {
		return noOfVisits;
	}
	public void setNoOfVisits(long noOfVisits) {
		this.noOfVisits = noOfVisits;
	}
	public long getPlItems() {
		return plItems;
	}
	public void setPlItems(long plItems) {
		this.plItems = plItems;
	}
	public long getActiveItemsPerMonth() {
		return activeItemsPerMonth;
	}
	public void setActiveItemsPerMonth(long activeItemsPerMonth) {
		this.activeItemsPerMonth = activeItemsPerMonth;
	}
	public long getTotalItemsByHH() {
		return totalItemsByHH;
	}
	public void setTotalItemsByHH(long totalItemsByHH) {
		this.totalItemsByHH = totalItemsByHH;
	}
	public String getItemId() {
		return ItemId;
	}
	public void setItemId(String itemId) {
		ItemId = itemId;
	}
	public long getQuantity() {
		return quantity;
	}
	public void setQuantity(long quantity) {
		this.quantity = quantity;
	}
	public Double getFinalPrice() {
		return finalPrice;
	}
	public void setFinalPrice(Double finalPrice) {
		this.finalPrice = finalPrice;
	}
	public String getHouseHoldFirstTrans() {
		return houseHoldFirstTrans;
	}
	public void setHouseHoldFirstTrans(String houseHoldFirstTrans) {
		this.houseHoldFirstTrans = houseHoldFirstTrans;
	}
	public Double getPiBasedOnPreviousMonth() {
		return piBasedOnPreviousMonth;
	}
	public void setPiBasedOnPreviousMonth(Double piBasedOnPreviousMonth) {
		this.piBasedOnPreviousMonth = piBasedOnPreviousMonth;
	}
	public Double getEstimatedItemPrice() {
		return estimatedItemPrice;
	}
	public void setEstimatedItemPrice(Double estimatedItemPrice) {
		this.estimatedItemPrice = estimatedItemPrice;
	}
	public long getNoOfItemsInMonth() {
		return noOfItemsInMonth;
	}
	public void setNoOfItemsInMonth(long noOfItemsInMonth) {
		this.noOfItemsInMonth = noOfItemsInMonth;
	}
	public Double getItemsPI() {
		return itemsPI;
	}
	public void setItemsPI(Double itemsPI) {
		this.itemsPI = itemsPI;
	}
	public Double getWeight() {
		return weight;
	}
	public void setWeight(Double weight) {
		this.weight = weight;
	}

	
}
