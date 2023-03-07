package com.pristine.dto;

import java.util.Date;

public class StoreCouponDTO {
	
    private String company;
    private String store;
    private int terminal;
    private int    transactionNo;
    private String operator;
    private String itemDateTimeStr;
    private Date   itemDateTime;
    private String customerId = "";
    private String couponNumber;
    private String itemUPC;
    private double itemNetPrice = 0;
    private int posDept = 0;
    private String filler1;

	private String filler2;
	private String filler3;
    private String couponType;
    private int calendarId;
    private int cpnFamily;
    private int itemCode;
    
    private String strCpnQty;
    private String strCpnWeight;
    
    private String cpnDate;
	private String cpnTime;
    private int cpnFamilyCurr;
    private int cpnFamilyPrev;
    private int cpnMfgNbr;
    private int cpnCnt;
    private String discSalePrcAmt;
    private String discTaxExempt;
    private String discSalePrice;
    private int discQty;
    private int cpnDiscGrp;
    private String cpnDiscPct;
    private String cpnNotUsed;
    private String cpnMult;
    public String getStrCpnQty() {
		return strCpnQty;
	}
	public void setStrCpnQty(String strCpnQty) {
		this.strCpnQty = strCpnQty;
	}
	public String getStrCpnWeight() {
		return strCpnWeight;
	}
	public void setStrCpnWeight(String strCpnWeight) {
		this.strCpnWeight = strCpnWeight;
	}
	private double cpnQty;
    private double cpnWeight;
    

    public String getFiller3() {
		return filler3;
	}
	public void setFiller3(String filler3) {
		this.filler3 = filler3;
	}
	public double getCpnQty() {
		return cpnQty;
	}
	public void setCpnQty(double cpnQty) {
		this.cpnQty = cpnQty;
	}
	public double getCpnWeight() {
		return cpnWeight;
	}
	public void setCpnWeight(double cpnWeight) {
		this.cpnWeight = cpnWeight;
	}
	public int getItemCode() {
		return itemCode;
	}
	public void setItemCode(int itemCode) {
		this.itemCode = itemCode;
	}
	public int getCpnFamily() {
		return cpnFamily;
	}
	public void setCpnFamily(int cpnFamily) {
		this.cpnFamily = cpnFamily;
	}
    
    public int getPosDept() {
		return posDept;
	}
	public void setPosDept(int posDept) {
		this.posDept = posDept;
	}
	public String getCompany() {
		return company;
	}
	public void setCompany(String company) {
		this.company = company;
	}
	public String getStore() {
		return store;
	}
	public void setStore(String store) {
		this.store = store;
	}
	public int getTerminal() {
		return terminal;
	}
	public void setTerminal(int terminal) {
		this.terminal = terminal;
	}

	public int getTransactionNo() {
		return transactionNo;
	}
	public void setTransactionNo(int transactionNo) {
		this.transactionNo = transactionNo;
	}
	public String getOperator() {
		return operator;
	}
	public void setOperator(String operator) {
		this.operator = operator;
	}
	public String getItemDateTimeStr() {
		return itemDateTimeStr;
	}
	public void setItemDateTimeStr(String itemDateTimeStr) {
		this.itemDateTimeStr = itemDateTimeStr;
	}
	public Date getItemDateTime() {
		return itemDateTime;
	}
	public void setItemDateTime(Date itemDateTime) {
		this.itemDateTime = itemDateTime;
	}
	public String getCustomerId() {
		return customerId;
	}
	public void setCustomerId(String customerId) {
		this.customerId = customerId;
	}
	public String getCouponNumber() {
		return couponNumber;
	}
	public void setCouponNumber(String couponNumber) {
		this.couponNumber = couponNumber;
	}
	public String getItemUPC() {
		return itemUPC;
	}
	public void setItemUPC(String itemUPC) {
		this.itemUPC = itemUPC;
	}
	public double getItemNetPrice() {
		return itemNetPrice;
	}
	public void setItemNetPrice(double itemNetPrice) {
		this.itemNetPrice = itemNetPrice;
	}
	public String getFiller1() {
		return filler1;
	}
	public void setFiller1(String filler1) {
		this.filler1 = filler1;
	}
	public String getFiller2() {
		return filler2;
	}
	public void setFiller2(String filler2) {
		this.filler2 = filler2;
	}
	public String getCouponType() {
		return couponType;
	}
	public void setCouponType(String couponType) {
		this.couponType = couponType;
	}
	public void setCalendarId(int calId) {
		this.calendarId = calId;
		
	}
	public int getCalendarId() {
		return calendarId;
		
	}
    
	public String getCpnDate() {
		return cpnDate;
	}
	public void setCpnDate(String cpnDate) {
		this.cpnDate = cpnDate;
	}
	public String getCpnTime() {
		return cpnTime;
	}
	public void setCpnTime(String cpnTime) {
		this.cpnTime = cpnTime;
	}
	public int getCpnFamilyCurr() {
		return cpnFamilyCurr;
	}
	public void setCpnFamilyCurr(int cpnFamilyCurr) {
		this.cpnFamilyCurr = cpnFamilyCurr;
	}
	public int getCpnFamilyPrev() {
		return cpnFamilyPrev;
	}
	public void setCpnFamilyPrev(int cpnFamilyPrev) {
		this.cpnFamilyPrev = cpnFamilyPrev;
	}
	public int getCpnMfgNbr() {
		return cpnMfgNbr;
	}
	public void setCpnMfgNbr(int cpnMfgNbr) {
		this.cpnMfgNbr = cpnMfgNbr;
	}
	public int getCpnCnt() {
		return cpnCnt;
	}
	public void setCpnCnt(int cpnCnt) {
		this.cpnCnt = cpnCnt;
	}
	public String getDiscSalePrcAmt() {
		return discSalePrcAmt;
	}
	public void setDiscSalePrcAmt(String discSalePrcAmt) {
		this.discSalePrcAmt = discSalePrcAmt;
	}
	public String getDiscTaxExempt() {
		return discTaxExempt;
	}
	public void setDiscTaxExempt(String discTaxExempt) {
		this.discTaxExempt = discTaxExempt;
	}
	public String getDiscSalePrice() {
		return discSalePrice;
	}
	public void setDiscSalePrice(String discSalePrice) {
		this.discSalePrice = discSalePrice;
	}
	public int getDiscQty() {
		return discQty;
	}
	public void setDiscQty(int discQty) {
		this.discQty = discQty;
	}
	public int getCpnDiscGrp() {
		return cpnDiscGrp;
	}
	public void setCpnDiscGrp(int cpnDiscGrp) {
		this.cpnDiscGrp = cpnDiscGrp;
	}
	public String getCpnDiscPct() {
		return cpnDiscPct;
	}
	public void setCpnDiscPct(String cpnDiscPct) {
		this.cpnDiscPct = cpnDiscPct;
	}
	public String getCpnNotUsed() {
		return cpnNotUsed;
	}
	public void setCpnNotUsed(String cpnNotUsed) {
		this.cpnNotUsed = cpnNotUsed;
	}
	public String getCpnMult() {
		return cpnMult;
	}
	public void setCpnMult(String cpnMult) {
		this.cpnMult = cpnMult;
	}
}
