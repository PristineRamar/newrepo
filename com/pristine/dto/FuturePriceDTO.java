package com.pristine.dto;

import java.sql.Date;

public class FuturePriceDTO implements Cloneable {

	public String storenum;
	public String retailerItemCode;
	public String effectivestartdate;
	public String portfolio;
	public String itemdesc;
	public int offerqty;
	public double offeramt;
	public int regqty;
	public double regprice;
	public String item2;
	public String item3;
	public String strategyzone;
	public double diff;
	
	public String getCompStrNo() {
		return storenum;
	}
	public void setCompStrNo(String strcompStrNo) {
		storenum = strcompStrNo;
	}
	
	public String getRetailerItemCode() {
		return retailerItemCode;
	}
	public void setRetailerItemCode(String strretailerItemCode) {
		retailerItemCode = strretailerItemCode;
	}
	
	public String getEffectivestartdate() {
		return effectivestartdate;
	}
	public void setEffectivestartdate(String streffectivestartdate) {
		effectivestartdate = streffectivestartdate;
	}
	
	public String getPortfolio() {
		return portfolio;
	}
	public void setPortfolio(String strportfolio) {
		portfolio = strportfolio;
	}
	
	public String getItemDesc() {
		return itemdesc;
	}
	public void setItemDesc(String stritemdesc) {
		itemdesc = stritemdesc;
	}
	
	public int getOfferQty() {
		return offerqty;
	}
	public void setOfferQty(int offerquantity) {
		offerqty = offerquantity;
	}
	
	public double getOfferAmt() {
		return offeramt;
	}
	public void setOfferAmt(double offerAmount) {
		this.offeramt = offerAmount;
	}
	
	public int getRegQty() {
		return regqty;
	}
	public void setRegQty(int regularquantity) {
		regqty = regularquantity;
	}
	
	public double getRegAmt() {
		return regprice;
	}
	public void setRegAmt(double regularAmount) {
		this.regprice = regularAmount;
	}
	
	public String getItem2() {
		return item2;
	}
	public void setItem2(String stritem2) {
		item2 = stritem2;
	}
	public String getItem3() {
		return item3;
	}
	public void setItem3(String stritem3) {
		item3 = stritem3;
	}
	public String getStrategyZone() {
		return strategyzone;
	}
	public void setStrategyZone(String strstrategyzone) {
		strategyzone = strstrategyzone;
	}
	public double getDiff() {
		return diff;
	}
	public void setDiff(double difference) {
		this.diff = difference;
	}
	
	
	
	public void clear(){
		storenum= "";
		retailerItemCode ="";
		effectivestartdate ="";
		portfolio ="";
		itemdesc ="";
		offerqty =0;
		offeramt =0;
		regqty =0;
		regprice =0;
		item2 ="";
		item3 ="";
		strategyzone ="";
		diff =0;
	}
}
