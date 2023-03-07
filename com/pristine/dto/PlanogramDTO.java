package com.pristine.dto;

public class PlanogramDTO {
	private String storeNo;  //0-5
	private int storeId;     
	private String retailerItemCode;//5-12
	private String aisleFix; //17 - 20
	private int aislePos; //37-3
	private String shelf;//40-20
	private int shelfPos;//60 - 3
	private int shelfProdPos;//63 - 3
	private String upc; //66 - 14
	private String planogramNo; //80-9
	private String capacity;//165-11
	private int itemCode;
		
	
	public String getStoreNo() {
		return storeNo;
	}
	public void setStoreNo(String storeNo) {
		this.storeNo = storeNo;
	}
	public int getStoreId() {
		return storeId;
	}
	public void setStoreId(int storeId) {
		this.storeId = storeId;
	}
	public String getRetailerItemCode() {
		return retailerItemCode;
	}
	public void setRetailerItemCode(String retailerItemCode) {
		this.retailerItemCode = retailerItemCode;
	}
	public String getAisleFix() {
		return aisleFix;
	}
	public void setAisleFix(String aisleFix) {
		this.aisleFix = aisleFix;
	}
	public int getAislePos() {
		return aislePos;
	}
	public void setAislePos(int aislePos) {
		this.aislePos = aislePos;
	}
	public String getShelf() {
		return shelf;
	}
	public void setShelf(String shelf) {
		this.shelf = shelf;
	}
	public int getShelfPos() {
		return shelfPos;
	}
	public void setShelfPos(int shelfPos) {
		this.shelfPos = shelfPos;
	}
	public int getShelfProdPos() {
		return shelfProdPos;
	}
	public void setShelfProdPos(int shelfProdPos) {
		this.shelfProdPos = shelfProdPos;
	}
	public String getUpc() {
		return upc;
	}
	public void setUpc(String upc) {
		this.upc = upc;
	}
	public String getPlanogramNo() {
		return planogramNo;
	}
	public void setPlanogramNo(String planogramNo) {
		this.planogramNo = planogramNo;
	}
	public String getCapacity() {
		return capacity;
	}
	public void setCapacity(String capacity) {
		this.capacity = capacity;
	}
	public int getItemCode() {
		return itemCode;
	}
	public void setItemCode(int itemCode) {
		this.itemCode = itemCode;
	}
	
	
}
