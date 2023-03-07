package com.pristine.dto;

public class ScanBackDTO implements Cloneable{
	
	private String retailerItemCode;
	private String itemCode;
	private String levelId;
	private String zoneNumber;
	private String splrNo;
	private String bnrCD;
	private String scanBackAmt1;
	private String scanBackStartDate1;
	private String scanBackEndDate1;
	private String scanBackAmt2;
	private String scanBackStartDate2;
	private String scanBackEndDate2;
	private String scanBackAmt3;
	private String scanBackStartDate3;
	private String scanBackEndDate3;
	private float scanBackTotalAmt;
	private String scanBackAmt;
	private String scanBackStartDate;
	private String scanBackEndDate;
	private String scanBackNo;
	private String dealId;
	private String prcGrpCode;
	
	public String getRetailerItemCode() {
		return retailerItemCode;
	}
	public void setRetailerItemCode(String retailerItemCode) {
		this.retailerItemCode = retailerItemCode;
	}
	public String getZoneNumber() {
		return zoneNumber;
	}
	public void setZoneNumber(String zoneNumber) {
		this.zoneNumber = zoneNumber;
	}
	public String getSplrNo() {
		return splrNo;
	}
	public void setSplrNo(String splrNo) {
		this.splrNo = splrNo;
	}
	public String getBnrCD() {
		return bnrCD;
	}
	public void setBnrCD(String bnrCD) {
		this.bnrCD = bnrCD;
	}
	public String getScanBackAmt1() {
		return scanBackAmt1;
	}
	public void setScanBackAmt1(String scanBackAmt1) {
		this.scanBackAmt1 = scanBackAmt1;
	}
	public String getScanBackStartDate1() {
		return scanBackStartDate1;
	}
	public void setScanBackStartDate1(String scanBackStartDate1) {
		this.scanBackStartDate1 = scanBackStartDate1;
	}
	public String getScanBackEndDate1() {
		return scanBackEndDate1;
	}
	public void setScanBackEndDate1(String scanBackEndDate1) {
		this.scanBackEndDate1 = scanBackEndDate1;
	}
	public String getScanBackAmt2() {
		return scanBackAmt2;
	}
	public void setScanBackAmt2(String scanBackAmt2) {
		this.scanBackAmt2 = scanBackAmt2;
	}
	public String getScanBackStartDate2() {
		return scanBackStartDate2;
	}
	public void setScanBackStartDate2(String scanBackStartDate2) {
		this.scanBackStartDate2 = scanBackStartDate2;
	}
	public String getScanBackEndDate2() {
		return scanBackEndDate2;
	}
	public void setScanBackEndDate2(String scanBackEndDate2) {
		this.scanBackEndDate2 = scanBackEndDate2;
	}
	public String getScanBackAmt3() {
		return scanBackAmt3;
	}
	public void setScanBackAmt3(String scanBackAmt3) {
		this.scanBackAmt3 = scanBackAmt3;
	}
	public String getScanBackStartDate3() {
		return scanBackStartDate3;
	}
	public void setScanBackStartDate3(String scanBackStartDate3) {
		this.scanBackStartDate3 = scanBackStartDate3;
	}
	public String getScanBackEndDate3() {
		return scanBackEndDate3;
	}
	public void setScanBackEndDate3(String scanBackEndDate3) {
		this.scanBackEndDate3 = scanBackEndDate3;
	}
	public String getItemCode() {
		return itemCode;
	}
	public void setItemCode(String itemCode) {
		this.itemCode = itemCode;
	}
	public String getLevelId() {
		return levelId;
	}
	public void setLevelId(String levelId) {
		this.levelId = levelId;
	}
	public float getScanBackTotalAmt() {
		return scanBackTotalAmt;
	}
	public void setScanBackTotalAmt(float scanBackTotalAmt) {
		this.scanBackTotalAmt = scanBackTotalAmt;
	}
	
	@Override
    public Object clone() throws CloneNotSupportedException {
		ScanBackDTO cloned = (ScanBackDTO)super.clone();
		return cloned;
	}
	public String getScanBackAmt() {
		return scanBackAmt;
	}
	public void setScanBackAmt(String scanBackAmt) {
		this.scanBackAmt = scanBackAmt;
	}
	public String getScanBackStartDate() {
		return scanBackStartDate;
	}
	public void setScanBackStartDate(String scanBackStartDate) {
		this.scanBackStartDate = scanBackStartDate;
	}
	public String getScanBackEndDate() {
		return scanBackEndDate;
	}
	public void setScanBackEndDate(String scanBackEndDate) {
		this.scanBackEndDate = scanBackEndDate;
	}
	public String getScanBackNo() {
		return scanBackNo;
	}
	public void setScanBackNo(String scanBackNo) {
		this.scanBackNo = scanBackNo;
	}
	public String getDealId() {
		return dealId;
	}
	public void setDealId(String dealId) {
		this.dealId = dealId;
	}
	public String getPrcGrpCode() {
		return prcGrpCode;
	}
	public void setPrcGrpCode(String prcGrpCode) {
		this.prcGrpCode = prcGrpCode;
	} 
}
