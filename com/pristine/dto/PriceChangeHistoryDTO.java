package com.pristine.dto;

public class PriceChangeHistoryDTO {
	private int itemCode;
	private String retailerItemCode;
	private String itemName; 
	private String lirName; 
	private double regPrice; 
	private double listCost;
	private double marginPct;
	private String recentPriceChangeDate; 
	private String recenCostChangeDate;
	private int numofPriceChanges;
	private int numofCostChanges;
	private double currentCompPrice;
	private String categoryName;
	private int numofWeeksAnalyzed;
	private int numofWeeksOnSale;
	private double annualRevenue;
	private double dealCostWhenLastSale;
	private double recentSalePrice;
	private String recentSalePriceOn;
	
	public double getDealCostWhenLastSale() {
		return dealCostWhenLastSale;
	}
	public void setDealCostWhenLastSale(double dealCostWhenLastSale) {
		this.dealCostWhenLastSale = dealCostWhenLastSale;
	}
	public double getRecentSalePrice() {
		return recentSalePrice;
	}
	public void setRecentSalePrice(double recentSalePrice) {
		this.recentSalePrice = recentSalePrice;
	}
	public String getRecentSalePriceOn() {
		return recentSalePriceOn;
	}
	public void setRecentSalePriceOn(String recentSalePriceOn) {
		this.recentSalePriceOn = recentSalePriceOn;
	}
	public int getItemCode() {
		return itemCode;
	}
	public void setItemCode(int itemCode) {
		this.itemCode = itemCode;
	}
	public String getRetailerItemCode() {
		return retailerItemCode;
	}
	public void setRetailerItemCode(String retailerItemCode) {
		this.retailerItemCode = retailerItemCode;
	}
	public String getItemName() {
		return itemName;
	}
	public void setItemName(String itemName) {
		this.itemName = itemName;
	}
	public String getLirName() {
		return lirName;
	}
	public void setLirName(String lirName) {
		this.lirName = lirName;
	}
	public double getRegPrice() {
		return regPrice;
	}
	public void setRegPrice(double regPrice) {
		this.regPrice = regPrice;
	}
	public double getListCost() {
		return listCost;
	}
	public void setListCost(double listCost) {
		this.listCost = listCost;
	}
	public double getMarginPct() {
		return marginPct;
	}
	public void setMarginPct(double marginPct) {
		this.marginPct = marginPct;
	}
	public String getRecentPriceChangeDate() {
		return recentPriceChangeDate;
	}
	public void setRecentPriceChangeDate(String recentPriceChangeDate) {
		this.recentPriceChangeDate = recentPriceChangeDate;
	}
	public String getRecenCostChangeDate() {
		return recenCostChangeDate;
	}
	public void setRecenCostChangeDate(String recenCostChangeDate) {
		this.recenCostChangeDate = recenCostChangeDate;
	}
	public int getNumofPriceChanges() {
		return numofPriceChanges;
	}
	public void setNumofPriceChanges(int numofPriceChanges) {
		this.numofPriceChanges = numofPriceChanges;
	}
	public int getNumofCostChanges() {
		return numofCostChanges;
	}
	public void setNumofCostChanges(int numofCostChanges) {
		this.numofCostChanges = numofCostChanges;
	}
	public double getCurrentCompPrice() {
		return currentCompPrice;
	}
	public void setCurrentCompPrice(double currentCompPrice) {
		this.currentCompPrice = currentCompPrice;
	}
	public String getCategoryName() {
		return categoryName;
	}
	public void setCategoryName(String categoryName) {
		this.categoryName = categoryName;
	}
	public int getNumofWeeksAnalyzed() {
		return numofWeeksAnalyzed;
	}
	public void setNumofWeeksAnalyzed(int numofWeeksAnalyzed) {
		this.numofWeeksAnalyzed = numofWeeksAnalyzed;
	}
	public int getNumofWeeksOnSale() {
		return numofWeeksOnSale;
	}
	public void setNumofWeeksOnSale(int numofWeeksOnSale) {
		this.numofWeeksOnSale = numofWeeksOnSale;
	}
	public double getAnnualRevenue() {
		return annualRevenue;
	}
	public void setAnnualRevenue(double annualRevenue) {
		this.annualRevenue = annualRevenue;
	}
}
