/*
 * Title: RetailPriceDTO for Retail Price Setup
 *
 *******************************************************************************
 * Modification History
 *------------------------------------------------------------------------------
 * Version		Date		Name			Remarks
 *------------------------------------------------------------------------------
 * Version 0.1	04/03/2012	Janani			Initial Version 
 *******************************************************************************
 */
package com.pristine.dto;

import java.text.SimpleDateFormat;

import com.pristine.dto.offermgmt.MultiplePrice;
import com.pristine.dto.offermgmt.oos.OOSItemDTO;

public class RetailPriceDTO implements Comparable<RetailPriceDTO>, Cloneable{
	private int calendarId;
	private String upc;
	private String retailerItemCode;
	private String itemcode;
	private int levelTypeId;
	private String levelId;
	private String storeNbr;
	private String zoneNbr;
	private String chainId;
	private float regPrice;
	private int regQty;
	private float regMPrice;
	private float salePrice;
	private int saleQty;
	private float saleMPrice;
	private String promotionFlag;
	private String updateTimeStamp;
	private String regEffectiveDate;
	private String saleStartDate;
	private String saleEndDate;
	private boolean isProcessedFlag = false;
	private String deptName;
	private String categoryName;
	private String itemName;
	
	// Changes for handling multiples at zone level
	private float unitRegPrice;
	private float unitSalePrice;
	private int regMPack;
	private int saleMPack;
	private int totalMovement;
	// Changes for handling multiples at zone level Ends
	
	// Changes for handling future price
	private float futureRegPrice;
	private int futureRegQty;
	private String futureRegEffDate;
	// Changes for handling future price Ends
	//Added for storing internal location id for chain, zone and store. 
	private Integer locationId;
	
	private boolean isZonePriceDiff;
	private String vendorNumber;
	private String vendorName;
	
	private MultiplePrice regularPrice;
	private boolean whseZoneRolledUpRecord;
	private String prcGrpCode;
	private double coreRetail;
	//ADDED FOR AUTOZONE
	private String coreRetailValue;
	private String vdprRetail;
	
	
	public String getCoreRetailValue() {
		return coreRetailValue;
	}
	public void setCoreRetailValue(String coreRetailValue) {
		this.coreRetailValue = coreRetailValue;
	}
	public String getVdprRetail() {
		return vdprRetail;
	}
	public void setVdprRetail(String vdprRetail) {
		this.vdprRetail = vdprRetail;
	}
	public Integer getLocationId() {
		return locationId;
	}
	public void setLocationId(Integer locationId) {
		this.locationId = locationId;
	}

	private int prePrice;
	
	public int getPrePrice() {
		return prePrice;
	}
	public void setPrePrice(int prePrice) {
		this.prePrice = prePrice;
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
		this.retailerItemCode = Integer.toString(Integer.parseInt(retailerItemCode));
	}
	public void setRetailerItemCodeAsItIs(String retailerItemCode) {
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

	public float getRegPrice() {
		return regPrice;
	}
	public void setRegPrice(float regPrice) {
		this.regPrice = regPrice;
	}
	public int getRegQty() {
		return regQty;
	}
	public void setRegQty(int regQty) {
		this.regQty = regQty;
		/*if(regQty > 1){
			this.regMPrice = this.regPrice;
			this.regPrice = 0;
		}*/
	}
	public float getRegMPrice() {
		return regMPrice;
	}
	public void setRegMPrice(float regMPrice) {
		this.regMPrice = regMPrice;
	}
	public float getSalePrice() {
		return salePrice;
	}
	public void setSalePrice(float salePrice) {
		this.salePrice = salePrice;
	}
	public int getSaleQty() {
		return saleQty;
	}
	public void setSaleQty(int saleQty) {
		this.saleQty = saleQty;
		/*if(saleQty > 1){
			this.saleMPrice = this.salePrice;
			this.salePrice = 0;
		}*/
	}
	public float getSaleMPrice() {
		return saleMPrice;
	}
	public void setSaleMPrice(float saleMPrice) {
		this.saleMPrice = saleMPrice;
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
	public String getRegEffectiveDate() {
		return regEffectiveDate;
	}
	public void setRegEffectiveDate(String regEffectiveDate) {
		this.regEffectiveDate = regEffectiveDate;
	}
	public String getSaleStartDate() {
		return saleStartDate;
	}
	public void setSaleStartDate(String saleStartDate) {
		this.saleStartDate = saleStartDate;
	}
	public String getSaleEndDate() {
		return saleEndDate;
	}
	public void setSaleEndDate(String saleEndDate) {
		this.saleEndDate = saleEndDate;
	}
	public boolean isProcessedFlag() {
		return isProcessedFlag;
	}
	public void setProcessedFlag(boolean isProcessedFlag) {
		this.isProcessedFlag = isProcessedFlag;
	}

	public String getDeptName() {
		return deptName;
	}
	public void setDeptName(String deptName) {
		this.deptName = deptName;
	}
	public String getCategoryName() {
		return categoryName;
	}
	public void setCategoryName(String categoryName) {
		this.categoryName = categoryName;
	}

	public String getItemName() {
		return itemName;
	}
	public void setItemName(String itemName) {
		this.itemName = itemName;
	}
	
	// Changes for handling multiples at zone level
	public float getUnitRegPrice() {
		return unitRegPrice;
	}
	public void setUnitRegPrice(float unitRegPrice) {
		this.unitRegPrice = unitRegPrice;
	}
	public float getUnitSalePrice() {
		return unitSalePrice;
	}
	public void setUnitSalePrice(float unitSalePrice) {
		this.unitSalePrice = unitSalePrice;
	}
	public int getRegMPack() {
		return regMPack;
	}
	public void setRegMPack(int regMPack) {
		this.regMPack = regMPack;
	}
	public int getSaleMPack() {
		return saleMPack;
	}
	public void setSaleMPack(int saleMPack) {
		this.saleMPack = saleMPack;
	}
	public int getTotalMovement() {
		return totalMovement;
	}
	public void setTotalMovement(int totalMovement) {
		this.totalMovement = totalMovement;
	}
	// Changes for handling multiples at zone level - Ends
	
	// Changes for handling future price
	public float getFutureRegPrice() {
		return futureRegPrice;
	}
	public void setFutureRegPrice(float futureRegPrice) {
		this.futureRegPrice = futureRegPrice;
	}
	public int getFutureRegQty() {
		return futureRegQty;
	}
	public void setFutureRegQty(int futureRegQty) {
		this.futureRegQty = futureRegQty;
	}
	public String getFutureRegEffDate() {
		return futureRegEffDate;
	}
	public void setFutureRegEffDate(String futureRegEffDate) {
		this.futureRegEffDate = futureRegEffDate;
	}
	// Changes for handling future price Ends
	
	public void copy(RetailPriceDTO tempDTO){
		this.calendarId = tempDTO.calendarId;
		this.itemcode = tempDTO.itemcode;
		this.levelId = tempDTO.levelId;
		this.levelTypeId = tempDTO.levelTypeId;
		this.promotionFlag = tempDTO.promotionFlag;
		this.regPrice = tempDTO.regPrice;
		this.regQty = tempDTO.regQty;
		this.regMPrice = tempDTO.regMPrice;
		this.salePrice = tempDTO.salePrice;
		this.saleQty = tempDTO.saleQty;
		this.saleMPrice = tempDTO.saleMPrice;
		this.regEffectiveDate = tempDTO.regEffectiveDate;
		this.saleStartDate = tempDTO.saleStartDate;
		this.saleEndDate = tempDTO.saleEndDate;
		this.futureRegPrice = tempDTO.futureRegPrice;
		this.futureRegQty = tempDTO.futureRegQty;
		this.futureRegEffDate = tempDTO.futureRegEffDate;
		this.regularPrice = tempDTO.regularPrice;
		this.prcGrpCode = tempDTO.prcGrpCode;
		this.whseZoneRolledUpRecord = tempDTO.whseZoneRolledUpRecord;
		this.coreRetail = tempDTO.coreRetail;
	}
	
	public int compareTo(RetailPriceDTO dto)
	{
	     return(dto.levelTypeId - levelTypeId);
	}
	public boolean isZonePriceDiff() {
		return isZonePriceDiff;
	}
	public void setZonePriceDiff(boolean isZonePriceDiff) {
		this.isZonePriceDiff = isZonePriceDiff;
	}
	
	@Override
    public Object clone() throws CloneNotSupportedException {
		RetailPriceDTO cloned = (RetailPriceDTO)super.clone();
		return cloned;
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
	
	
	@Override
	public String toString(){
		return "Item code: " + itemcode + ", Level Id: " + levelId + ", Level type Id: " + levelTypeId + ", Reg Price: "
				+ regPrice + ", CalendarId: " + calendarId + ", Sale Price : " + salePrice + ", Eff. Date: "
				+ regEffectiveDate;
	}
	public MultiplePrice getRegularPrice() {
		return regularPrice;
	}
	public void setRegularPrice(MultiplePrice regularPrice) {
		this.regularPrice = regularPrice;
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
	public double getCoreRetail() {
		return coreRetail;
	}
	public void setCoreRetail(double coreRetail) {
		this.coreRetail = coreRetail;
	}
	
	public RetailPriceDTO() {
		
	}
	
	public RetailPriceDTO(int calendarId, String itemcode, int levelTypeId, String levelId, float regPrice, int regQty,
			float regMPrice, float salePrice, int saleQty, float saleMPrice, String promotionFlag,
			String regEffectiveDate, String saleStartDate, String saleEndDate) {
		super();
		this.calendarId = calendarId;
		this.itemcode = itemcode;
		this.levelTypeId = levelTypeId;
		this.levelId = levelId;
		this.regPrice = regPrice;
		this.regQty = regQty;
		this.regMPrice = regMPrice;
		this.salePrice = salePrice;
		this.saleQty = saleQty;
		this.saleMPrice = saleMPrice;
		this.promotionFlag = promotionFlag;
		this.regEffectiveDate = regEffectiveDate;
		this.saleStartDate = saleStartDate;
		this.saleEndDate = saleEndDate;
	}
	
}
