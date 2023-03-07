package com.pristine.dto.offermgmt.predictionAnalysis;

public class PredictionAnalysisOutputDTO {
	private long prestoItemCode;
	private String retailerItemCode;
	private String UPC;
	private String itemName;
	private String ligName;
	private String itemCreatedDate;
	private double correlation;
	private int noOfRegPriceChangesIn78Weeks;
	private int noOfDistinctRegPriceIn78Weeks;
	private int noOfWeekWithZeroMovRegPriceIn78Weeks;
	private double avgMovRegPriceLast13Weeks;
	private String categoryName;
	private String zone;
	private boolean isRecPricePresentIn78Weeks;
	
	public long getPrestoItemCode() {
		return prestoItemCode;
	}
	public void setPrestoItemCode(long prestoItemCode) {
		this.prestoItemCode = prestoItemCode;
	}
	public String getRetailerItemCode() {
		return retailerItemCode;
	}
	public void setRetailerItemCode(String retailerItemCode) {
		this.retailerItemCode = retailerItemCode;
	}
	public String getUPC() {
		return UPC;
	}
	public void setUPC(String uPC) {
		UPC = uPC;
	}
	public double getCorrelation() {
		return correlation;
	}
	public void setCorrelation(double correlation) {
		this.correlation = correlation;
	}
	public int getNoOfRegPriceChangesIn78Weeks() {
		return noOfRegPriceChangesIn78Weeks;
	}
	public void setNoOfRegPriceChangesIn78Weeks(int noOfRegPriceChangesIn78Weeks) {
		this.noOfRegPriceChangesIn78Weeks = noOfRegPriceChangesIn78Weeks;
	}
	public int getNoOfDistinctRegPriceIn78Weeks() {
		return noOfDistinctRegPriceIn78Weeks;
	}
	public void setNoOfDistinctRegPriceIn78Weeks(int noOfDistinctRegPriceIn78Weeks) {
		this.noOfDistinctRegPriceIn78Weeks = noOfDistinctRegPriceIn78Weeks;
	}
	public int getNoOfWeekWithZeroMovRegPriceIn78Weeks() {
		return noOfWeekWithZeroMovRegPriceIn78Weeks;
	}
	public void setNoOfWeekWithZeroMovRegPriceIn78Weeks(int noOfWeekWithZeroMovRegPriceIn78Weeks) {
		this.noOfWeekWithZeroMovRegPriceIn78Weeks = noOfWeekWithZeroMovRegPriceIn78Weeks;
	}
	public double getAvgMovRegPriceLast13Weeks() {
		return avgMovRegPriceLast13Weeks;
	}
	public void setAvgMovRegPriceLast13Weeks(double avgMovRegPriceLast13Weeks) {
		this.avgMovRegPriceLast13Weeks = avgMovRegPriceLast13Weeks;
	}
	public String getItemCreatedDate() {
		return itemCreatedDate;
	}
	public void setItemCreatedDate(String itemCreatedDate) {
		this.itemCreatedDate = itemCreatedDate;
	}
	public String getCategoryName() {
		return categoryName;
	}
	public void setCategoryName(String categoryName) {
		this.categoryName = categoryName;
	}
	public String getZone() {
		return zone;
	}
	public void setZone(String zone) {
		this.zone = zone;
	}
	public String getItemName() {
		return itemName;
	}
	public void setItemName(String itemName) {
		this.itemName = itemName;
	}
	public String getLigName() {
		return ligName;
	}
	public void setLigName(String ligName) {
		this.ligName = ligName;
	}
	public boolean isRecPricePresentIn78Weeks() {
		return isRecPricePresentIn78Weeks;
	}
	public void setRecPricePresentIn78Weeks(boolean isRecPricePresentIn78Weeks) {
		this.isRecPricePresentIn78Weeks = isRecPricePresentIn78Weeks;
	}
}
