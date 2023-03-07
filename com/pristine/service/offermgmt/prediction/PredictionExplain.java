package com.pristine.service.offermgmt.prediction;

public class PredictionExplain {

	private int lirIdOrItemCode;
	private int isLIG;
	private double regPrice;
	private double salePrice;
	private double minPrice;
	private int page;
	private int ad;
	private String display = "";
	private int totalInstances;
	private long avgMov;
	private String lastObservedData = "";
	
	public int getLirIdOrItemCode() {
		return lirIdOrItemCode;
	}
	public void setLirIdOrItemCode(int lirIdOrItemCode) {
		this.lirIdOrItemCode = lirIdOrItemCode;
	}
	public int getIsLIG() {
		return isLIG;
	}
	public void setIsLIG(int isLIG) {
		this.isLIG = isLIG;
	}
	public double getRegPrice() {
		return regPrice;
	}
	public void setRegPrice(double regPrice) {
		this.regPrice = regPrice;
	}
	public double getSalePrice() {
		return salePrice;
	}
	public void setSalePrice(double salePrice) {
		this.salePrice = salePrice;
	}
	public double getMinPrice() {
		return minPrice;
	}
	public void setMinPrice(double minPrice) {
		this.minPrice = minPrice;
	}
	public int getPage() {
		return page;
	}
	public void setPage(int page) {
		this.page = page;
	}
	public int getAd() {
		return ad;
	}
	public void setAd(int ad) {
		this.ad = ad;
	}
	public String getDisplay() {
		return display;
	}
	public void setDisplay(String display) {
		this.display = display;
	}
	public int getTotalInstances() {
		return totalInstances;
	}
	public void setTotalInstances(int totalInstances) {
		this.totalInstances = totalInstances;
	}
	public long getAvgMov() {
		return avgMov;
	}
	public void setAvgMov(long avgMov) {
		this.avgMov = avgMov;
	}
	public String getLastObservedDate() {
		return lastObservedData;
	}
	public void setLastObservedDate(String lastObservedDate) {
		this.lastObservedData = lastObservedDate;
	}
}
