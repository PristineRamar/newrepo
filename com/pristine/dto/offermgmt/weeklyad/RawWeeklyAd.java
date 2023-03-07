package com.pristine.dto.offermgmt.weeklyad;

//import java.text.DecimalFormat;

public class RawWeeklyAd {
	private int topsPage;
	private int topsBlock;
	private int guPage;
	private int guBlock;
	private int pageNo;
	private int blockNo;
	private String itemCode;
	private String itemDesc;
	private String adLocation;
	private int casePack;
	private double listCost;
	private String tpr;
	private double offInvoiceCost;
	private double billbackCost;
	private double scanCost;
	private double dealCost;
	private double unitPrice;
	private String adRetail;
	private String displayType;
	private String storeLocation;
	private String topsAdjustedUnits;
	private String guAdjustedUnits;
	
	public String getTopsAdjustedUnits() {
		return topsAdjustedUnits;
	}
	public void setTopsAdjustedUnits(String topsAdjustedUnits) {
		this.topsAdjustedUnits = topsAdjustedUnits;
	}
	public int getTopsPage() {
		return topsPage;
	}
	public void setTopsPage(int page) {
		this.topsPage = page;
	}
	public int getTopsBlock() {
		return topsBlock;
	}
	public void setTopsBlock(int block) {
		this.topsBlock = block;
	}
	public String getItemCode() {
		return itemCode;
	}
	public void setItemCode(String itemCode) {
		this.itemCode = itemCode;
	}
	public String getItemDesc() {
		return itemDesc;
	}
	public void setItemDesc(String itemDesc) {
		this.itemDesc = itemDesc;
	}
	public String getAdLocation() {
		return adLocation;
	}
	public void setAdLocation(String adLocation) {
		this.adLocation = adLocation;
	}
	public int getCasePack() {
		return casePack;
	}
	public void setCasePack(int casePack) {
		this.casePack = casePack;
	}
	public double getListCost() {
		return listCost;
	}
	public void setListCost(double listCost) {
		this.listCost = listCost;
	}
	public String getTpr() {
		return tpr;
	}
	public void setTpr(String tpr) {
		this.tpr = tpr;
	}
	public double getOffInvoiceCost() {
		return offInvoiceCost;
	}
	public void setOffInvoiceCost(double offInvoiceCost) {
		this.offInvoiceCost = offInvoiceCost;
	}
	public double getBillbackCost() {
		return billbackCost;
	}
	public void setBillbackCost(double billbackCost) {
		this.billbackCost = billbackCost;
	}
	public double getScanCost() {
		return scanCost;
	}
	public void setScanCost(double scanCost) {
		this.scanCost = scanCost;
	}
	public double getDealCost() {
		return dealCost;
	}
	public void setDealCost(double dealCost) {
		this.dealCost = dealCost;
	}
	public double getUnitPrice() {
		return unitPrice;
	}
	public void setUnitPrice(double unitPrice) {
		this.unitPrice = unitPrice;
	}
	public String getAdRetail() {
		return adRetail;
	}
	public void setAdRetail(String adRetail) {
		this.adRetail = adRetail;
	}
	public String getDisplayType() {
		return displayType;
	}
	public void setDisplayType(String displayType) {
		this.displayType = displayType;
	}
	public String getStoreLocation() {
		return storeLocation;
	}
	public void setStoreLocation(String storeLocation) {
		this.storeLocation = storeLocation;
	}
	public int getGuPage() {
		return guPage;
	}
	public void setGuPage(int guPage) {
		this.guPage = guPage;
	}
	public int getGuBlock() {
		return guBlock;
	}
	public void setGuBlock(int guBlock) {
		this.guBlock = guBlock;
	}
	public String getGuAdjustedUnits() {
		return guAdjustedUnits;
	}
	public void setGuAdjustedUnits(String guAdjustedUnits) {
		this.guAdjustedUnits = guAdjustedUnits;
	}
	public int getPageNo() {
		return pageNo;
	}
	public void setPageNo(int pageNo) {
		this.pageNo = pageNo;
	}
	public int getBlockNo() {
		return blockNo;
	}
	public void setBlockNo(int blockNo) {
		this.blockNo = blockNo;
	}
	@Override
	public String toString() {
		return "RawWeeklyAd [topsPage=" + topsPage + ", topsBlock=" + topsBlock + ", guPage=" + guPage + ", guBlock=" + guBlock + ", pageNo=" + pageNo
				+ ", blockNo=" + blockNo + ", itemCode=" + itemCode + ", itemDesc=" + itemDesc + ", adLocation=" + adLocation + ", casePack="
				+ casePack + ", listCost=" + listCost + ", tpr=" + tpr + ", offInvoiceCost=" + offInvoiceCost + ", billbackCost=" + billbackCost
				+ ", scanCost=" + scanCost + ", dealCost=" + dealCost + ", unitPrice=" + unitPrice + ", adRetail=" + adRetail + ", displayType="
				+ displayType + ", storeLocation=" + storeLocation + ", topsAdjustedUnits=" + topsAdjustedUnits + ", guAdjustedUnits="
				+ guAdjustedUnits + "]";
	}
	
	
}
