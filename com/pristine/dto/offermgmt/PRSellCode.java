package com.pristine.dto.offermgmt;

public class PRSellCode {

	private String sellCode = "";
	private String sellCodeDesc = "";
	private String upc = "";
	private int itemCode;
	private Double yield = 0d;
	
	public String getSellCode() {
		return sellCode;
	}
	public void setSellCode(String sellCode) {
		this.sellCode = sellCode;
	}
	public String getSellCodeDesc() {
		return sellCodeDesc;
	}
	public void setSellCodeDesc(String sellCodeDesc) {
		this.sellCodeDesc = sellCodeDesc;
	}
	public String getUpc() {
		return upc;
	}
	public void setUpc(String upc) {
		this.upc = upc;
	}
	public int getItemCode() {
		return itemCode;
	}
	public void setItemCode(int itemCode) {
		this.itemCode = itemCode;
	}
	public Double getYield() {
		return yield;
	}
	public void setYield(Double yield) {
		this.yield = yield;
	}
	
}
