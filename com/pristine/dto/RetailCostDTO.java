/*
 * Title: DTO for Retail Cost Setup
 *
 *******************************************************************************
 * Modification History
 *------------------------------------------------------------------------------
 * Version		Date		Name			Remarks
 *------------------------------------------------------------------------------
 * Version 0.1	04/19/2012	Janani			Initial Version 
 * Version 0.2	08/20/2012	Janani			Changes to include offInvoiceCost
 * 											for TOPS in RETAIL_COST_INFO
 *******************************************************************************
 */
package com.pristine.dto;


public class RetailCostDTO implements Cloneable{
	private int calendarId;
	private String upc;
	private String retailerItemCode;
	private String itemcode;
	private int levelTypeId;
	private String levelId;
	private String storeNbr;
	private String zoneNbr;
	private String chainId;
	private float listCost;
	private float dealCost;
	private String promotionFlag;
	private String updateTimeStamp;
	private String effListCostDate;
	private String dealStartDate;
	private String dealEndDate;
	private boolean isProcessedFlag = false;
	
	private float level2Cost; // Changes to include LEVEL2COST for TOPS in RETAIL_COST_INFO
	
	// Changes to parse cost file from Ahold
	private float vipCost;
	private float avgCost;
	private String vendorNumber;
	private String vendorName;
	private String itemNumber;
	private String distFlag;
	private long vendorId;
	// Changes to parse cost file from Ahold - Ends
	
	// Changes to store Off Invoice start and end date for TOPS
	private String level2StartDate;
	private String level2EndDate;
	// Changes to store Off Invoice start and end date for TOPS - Ends
	
	//location_id added
	private Integer locationId;
	private boolean isFutureAsRegular;
	
	//Changes related to Scan back details in Giant Eagle(09/30/16 - By Dinesh) 
	private float scanBackAmt1;
	private String scanBackStartDate1;
	private String scanBackEndDate1;
	private float scanBackAmt2;
	private String scanBackStartDate2;
	private String scanBackEndDate2;
	private float scanBackAmt3;
	private String scanBackStartDate3;
	private String scanBackEndDate3;
	private float allowanceAmount;
	private String longTermFlag;
	private String allowStartDate;
	private String allowEndDate;
	private boolean whseZoneRolledUpRecord;
	private String prcGrpCode;
	private float finalListCost;
	private String longTermScan1 = "N";
	private String longTermScan2 = "N";
	private String longTermScan3 = "N";
	private double cwacCoreCost;
	private double nipoBaseCost=0;
	
	//added for IMS Schunck
	private int lirId;
	private String calStartDate;
	private String calEndDate;
	private String startDate;
	private String endDate;
	public int costTypeId;
	String calType ;
	String fiscalOrAdCalendar ;
	
	private double cwagBaseCost=0;
	public String getCalType() {
		return calType;
	}
	public void setCalType(String calType) {
		this.calType = calType;
	}
	public String getFiscalOrAdCalendar() {
		return fiscalOrAdCalendar;
	}
	public void setFiscalOrAdCalendar(String fiscalOrAdCalendar) {
		this.fiscalOrAdCalendar = fiscalOrAdCalendar;
	}
	public boolean isFutureAsRegular() {
		return isFutureAsRegular;
	}
	public void setFutureAsRegular(boolean isFutureAsRegular) {
		this.isFutureAsRegular = isFutureAsRegular;
	}
	public Integer getLocationId() {
		return locationId;
	}
	public void setLocationId(Integer locationId) {
		this.locationId = locationId;
	}
	public long getVendorId() {
		return vendorId;
	}
	public void setVendorId(long vendorId) {
		this.vendorId = vendorId;
	}
	public int getCalendarId() {
		return calendarId;
	}
	public void setCalendarId(int calendarId) {
		this.calendarId = calendarId;
	}
	public String getUpc() {
		return upc;
	}
	public void setUpc(String upc) {
		this.upc = upc;
	}
	public String getRetailerItemCode() {
		return retailerItemCode;
	}
	public void setRetailerItemCode(String retailerItemCode) {
		this.retailerItemCode = retailerItemCode;
	}
	public String getItemcode() {
		return itemcode;
	}
	public void setItemcode(String itemcode) {
		this.itemcode = itemcode;
	}
	public int getLevelTypeId() {
		return levelTypeId;
	}
	public void setLevelTypeId(int levelTypeId) {
		this.levelTypeId = levelTypeId;
	}
	public String getLevelId() {
		return levelId;
	}
	public void setLevelId(String levelId) {
		this.levelId = levelId;
	}
	public String getStoreNbr() {
		return storeNbr;
	}
	public void setStoreNbr(String storeNbr) {
		this.storeNbr = storeNbr;
	}
	public String getZoneNbr() {
		return zoneNbr;
	}
	public void setZoneNbr(String zoneNbr) {
		this.zoneNbr = zoneNbr;
	}
	public String getChainId() {
		return chainId;
	}
	public void setChainId(String chainId) {
		this.chainId = chainId;
	}
	public float getListCost() {
		return listCost;
	}
	public void setListCost(float listCost) {
		this.listCost = listCost;
	}
	public float getDealCost() {
		return dealCost;
	}
	public void setDealCost(float dealCost) {
		this.dealCost = dealCost;
	}
	public String getPromotionFlag() {
		return promotionFlag;
	}
	public void setPromotionFlag(String promotionFlag) {
		this.promotionFlag = promotionFlag;
	}
	public String getUpdateTimeStamp() {
		return updateTimeStamp;
	}
	public void setUpdateTimeStamp(String updateTimeStamp) {
		this.updateTimeStamp = updateTimeStamp;
	}
	public String getEffListCostDate() {
		return effListCostDate;
	}
	public void setEffListCostDate(String effListCostDate) {
		this.effListCostDate = effListCostDate;
	}
	public String getDealStartDate() {
		return dealStartDate;
	}
	public void setDealStartDate(String dealStartDate) {
		this.dealStartDate = dealStartDate;
	}
	public String getDealEndDate() {
		return dealEndDate;
	}
	public void setDealEndDate(String dealEndDate) {
		this.dealEndDate = dealEndDate;
	}
	public boolean isProcessedFlag() {
		return isProcessedFlag;
	}
	public void setProcessedFlag(boolean isProcessedFlag) {
		this.isProcessedFlag = isProcessedFlag;
	}
	
	public void copy(RetailCostDTO tempDTO){
		this.calendarId = tempDTO.calendarId;
		this.itemcode = tempDTO.itemcode;
		this.levelId = tempDTO.levelId;
		this.levelTypeId = tempDTO.levelTypeId;
		this.promotionFlag = tempDTO.promotionFlag;
		this.listCost = tempDTO.listCost;
		this.dealCost = tempDTO.dealCost;
		this.effListCostDate = tempDTO.effListCostDate;
		this.dealStartDate = tempDTO.dealStartDate;
		this.dealEndDate = tempDTO.dealEndDate;
		this.level2Cost = tempDTO.level2Cost;// Changes to include offInvoiceCost for TOPS in RETAIL_COST_INFO 
		this.vipCost = tempDTO.vipCost;//changes to setup vip cost.
		this.level2StartDate = tempDTO.level2StartDate;
		this.level2EndDate = tempDTO.level2EndDate;
		this.scanBackAmt1 = tempDTO.scanBackAmt1;
		this.scanBackAmt2 = tempDTO.scanBackAmt2;
		this.scanBackAmt3 = tempDTO.scanBackAmt3;
		this.allowanceAmount = tempDTO.allowanceAmount;
		this.longTermFlag = tempDTO.longTermFlag;
		this.scanBackStartDate1 = tempDTO.scanBackStartDate1;
		this.scanBackStartDate2 = tempDTO.scanBackStartDate2;
		this.scanBackStartDate3 = tempDTO.scanBackStartDate3;
		this.scanBackEndDate1 = tempDTO.scanBackEndDate1;
		this.scanBackEndDate2 = tempDTO.scanBackEndDate2;
		this.scanBackEndDate3 = tempDTO.scanBackEndDate3;
		this.allowStartDate = tempDTO.allowStartDate;
		this.allowEndDate = tempDTO.allowEndDate;
		this.longTermScan1 = tempDTO.longTermScan1;
		this.longTermScan2 = tempDTO.longTermScan2;
		this.longTermScan3 = tempDTO.longTermScan3;
		this.finalListCost = tempDTO.finalListCost;
		this.cwacCoreCost = tempDTO.cwacCoreCost;
		this.nipoBaseCost=tempDTO.nipoBaseCost;
		this.cwagBaseCost=tempDTO.cwagBaseCost;
	}
	
	// Changes to include LEVEL2COST for TOPS in RETAIL_COST_INFO - Starts
	public float getLevel2Cost() {
		return level2Cost;
	}
	public void setLevel2Cost(float level2Cost) {
		this.level2Cost = level2Cost;
	}
	// Changes to include LEVEL2COST for TOPS in RETAIL_COST_INFO - Ends

	// Changes to parse cost file from Ahold
	public float getVipCost() {
		return vipCost;
	}
	public void setVipCost(float vipCost) {
		this.vipCost = vipCost;
	}
	public float getAvgCost() {
		return avgCost;
	}
	public void setAvgCost(float avgCost) {
		this.avgCost = avgCost;
	}
	public String getVendorNumber() {
		return vendorNumber;
	}
	public void setVendorNumber(String vendorNumber) {
		this.vendorNumber = vendorNumber;
	}
	public String getVendorName() {
		return vendorName;
	}
	public void setVendorName(String vendorName) {
		this.vendorName = vendorName;
	}
	public String getItemNumber() {
		return itemNumber;
	}
	public void setItemNumber(String itemNumber) {
		this.itemNumber = itemNumber;
	}
	public String getDistFlag() {
		return distFlag;
	}
	public void setDistFlag(String distFlag) {
		this.distFlag = distFlag;
	}
	// Changes to parse cost file from Ahold - Ends

	// Changes to store Off Invoice start and end date for TOPS
	public String getLevel2StartDate() {
		return level2StartDate;
	}
	public void setLevel2StartDate(String level2StartDate) {
		this.level2StartDate = level2StartDate;
	}
	public String getLevel2EndDate() {
		return level2EndDate;
	}
	public void setLevel2EndDate(String level2EndDate) {
		this.level2EndDate = level2EndDate;
	}
	// Changes to store Off Invoice start and end date for TOPS - Ends
	
	@Override
    public Object clone() throws CloneNotSupportedException {
		RetailCostDTO cloned = (RetailCostDTO)super.clone();
		return cloned;
	}
	
	
	@Override
	public String toString(){
		return "Item code: " + itemcode + ", Level Id: " + levelId + ", Level type Id: " + levelTypeId + ", List Cost: "
				+ listCost + ", CalendarId: " + calendarId + ", Deal cost: " + dealCost + ", Eff. Date: "
				+ effListCostDate;
	}
	
	public float getScanBackAmt1() {
		return scanBackAmt1;
	}
	public void setScanBackAmt1(float scanBackAmt1) {
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
	public float getScanBackAmt2() {
		return scanBackAmt2;
	}
	public void setScanBackAmt2(float scanBackAmt2) {
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
	public float getScanBackAmt3() {
		return scanBackAmt3;
	}
	public void setScanBackAmt3(float scanBackAmt3) {
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
	public float getAllowanceAmount() {
		return allowanceAmount;
	}
	public void setAllowanceAmount(float allowanceAmount) {
		this.allowanceAmount = allowanceAmount;
	}
	public String getLongTermFlag() {
		return longTermFlag;
	}
	public void setLongTermFlag(String longTermFlag) {
		this.longTermFlag = longTermFlag;
	}
	public String getAllowStartDate() {
		return allowStartDate;
	}
	public void setAllowStartDate(String allowStartDate) {
		this.allowStartDate = allowStartDate;
	}
	public String getAllowEndDate() {
		return allowEndDate;
	}
	public void setAllowEndDate(String allowEndDate) {
		this.allowEndDate = allowEndDate;
	}
	public boolean isWhseZoneRolledUpRecord() {
		return whseZoneRolledUpRecord;
	}
	public void setWhseZoneRolledUpRecord(boolean whseZoneRolledUpRecord) {
		this.whseZoneRolledUpRecord = whseZoneRolledUpRecord;
	}
	public String getPrcGrpCode() {
		return prcGrpCode;
	}
	public void setPrcGrpCode(String prcGrpCode) {
		this.prcGrpCode = prcGrpCode;
	}

	public float getListCostMinusLongTermAllowances() {
		float finalListCost = this.getListCost();
		if(this.getFinalListCost() > 0){
			finalListCost = this.getFinalListCost(); 
		}
		return finalListCost;
	}
	
	public float getFinalListCost() {
		return finalListCost;
	}
	public void setFinalListCost(float finalListCost) {
		this.finalListCost = finalListCost;
	}
	public String getLongTermScan1() {
		return longTermScan1;
	}
	public void setLongTermScan1(String longTermScan1) {
		this.longTermScan1 = longTermScan1;
	}
	public String getLongTermScan2() {
		return longTermScan2;
	}
	public void setLongTermScan2(String longTermScan2) {
		this.longTermScan2 = longTermScan2;
	}
	public String getLongTermScan3() {
		return longTermScan3;
	}
	public void setLongTermScan3(String longTermScan3) {
		this.longTermScan3 = longTermScan3;
	}
	public double getCwacCoreCost() {
		return cwacCoreCost;
	}
	public void setCwacCoreCost(double cwacCoreCost) {
		this.cwacCoreCost = cwacCoreCost;
	}
	public double getNipoBaseCost() {
		return nipoBaseCost;
	}
	public void setNipoBaseCost(double nipoBaseCost) {
		this.nipoBaseCost = nipoBaseCost;
	}
	public int getLirId() {
		return lirId;
	}
	public void setLirId(int lirId) {
		this.lirId = lirId;
	}
	public String getCalStartDate() {
		return calStartDate;
	}
	public void setCalStartDate(String calStartDate) {
		this.calStartDate = calStartDate;
	}
	public String getCalEndDate() {
		return calEndDate;
	}
	public void setCalEndDate(String calEndDate) {
		this.calEndDate = calEndDate;
	}
	public String getStartDate() {
		return startDate;
	}
	public void setStartDate(String startDate) {
		this.startDate = startDate;
	}
	public String getEndDate() {
		return endDate;
	}
	public void setEndDate(String endDate) {
		this.endDate = endDate;
	}
	public int getCostTypeId() {
		return costTypeId;
	}
	public void setCostTypeId(int costTypeId) {
		this.costTypeId = costTypeId;
	}
	public double getCwagBaseCost() {
		return cwagBaseCost;
	}
	public void setCwagBaseCost(double cwagBaseCost) {
		this.cwagBaseCost = cwagBaseCost;
	}
	
}
