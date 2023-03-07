package com.pristine.dto;

//import com.pristine.dto.offermgmt.PRGuidelinesDTO;

public class PromoDataDTO implements Cloneable{

	private String buyerCode;
	private String adDate1;
	private String adDate2;
	private String adDate3;
	private String adDate4;
	private String promoStartDate;
	private String promoEndDate;
	private String sourceVendorNo;
	private String itemNo;
	private String itemDesc;
	private String priceZone;
	private String storeNo;
	private String storeZoneData;
	private int regQty;
	private float regPrice;
	private int saleQty;
	private float salePrice;
	private double saveAmt;
	private String pageNo;
	private String blockNo;
	private String userChange;
	private String promoNo;
	private int lirId;
	private long noOfDaysInPromoDuration;
	private int prestoItemCode;
	
	public int getLirId() {
		return lirId;
	}
	public void setLirId(int lirId) {
		this.lirId = lirId;
	}
	public String getBuyerCode() {
		return buyerCode;
	}
	public void setBuyerCode(String buyerCode) {
		this.buyerCode = buyerCode;
	}
	public String getAdDate1() {
		return adDate1;
	}
	public void setAdDate1(String adDate1) {
		this.adDate1 = adDate1;
	}
	public String getAdDate2() {
		return adDate2;
	}
	public void setAdDate2(String adDate2) {
		this.adDate2 = adDate2;
	}
	public String getAdDate3() {
		return adDate3;
	}
	public void setAdDate3(String adDate3) {
		this.adDate3 = adDate3;
	}
	public String getAdDate4() {
		return adDate4;
	}
	public void setAdDate4(String adDate4) {
		this.adDate4 = adDate4;
	}
	public String getPromoStartDate() {
		return promoStartDate;
	}
	public void setPromoStartDate(String promoStartDate) {
		this.promoStartDate = promoStartDate;
	}
	public String getPromoEndDate() {
		return promoEndDate;
	}
	public void setPromoEndDate(String promoEndDate) {
		this.promoEndDate = promoEndDate;
	}
	public String getSourceVendorNo() {
		return sourceVendorNo;
	}
	public void setSourceVendorNo(String sourceVendorNo) {
		this.sourceVendorNo = sourceVendorNo;
	}
	public String getItemNo() {
		return itemNo;
	}
	public void setItemNo(String itemNo) {
		this.itemNo = itemNo;
	}
	public String getItemDesc() {
		return itemDesc;
	}
	public void setItemDesc(String itemDesc) {
		this.itemDesc = itemDesc;
	}
	public String getPriceZone() {
		return priceZone;
	}
	public void setPriceZone(String priceZone) {
		this.priceZone = priceZone;
	}
	public String getStoreNo() {
		return storeNo;
	}
	public void setStoreNo(String storeNo) {
		this.storeNo = storeNo;
	}
	public String getStoreZoneData() {
		return storeZoneData;
	}
	public void setStoreZoneData(String storeZoneData) {
		this.storeZoneData = storeZoneData;
	}
	public int getRegQty() {
		return regQty;
	}
	public void setRegQty(int reqQty) {
		this.regQty = reqQty;
	}
	public float getRegPrice() {
		return regPrice;
	}
	public void setRegPrice(float reqPrice) {
		this.regPrice = reqPrice;
	}
	public int getSaleQty() {
		return saleQty;
	}
	public void setSaleQty(int saleQty) {
		this.saleQty = saleQty;
	}
	public float getSalePrice() {
		return salePrice;
	}
	public void setSalePrice(float salePrice) {
		this.salePrice = salePrice;
	}
	public double getSaveAmt() {
		return saveAmt;
	}
	public void setSaveAmt(double saveAmt) {
		this.saveAmt = saveAmt;
	}
	public String getPageNo() {
		return pageNo;
	}
	public void setPageNo(String pageNo) {
		this.pageNo = pageNo;
	}
	public String getBlockNo() {
		return blockNo;
	}
	public void setBlockNo(String blockNo) {
		this.blockNo = blockNo;
	}
	public String getUserChange() {
		return userChange;
	}
	public void setUserChange(String userChange) {
		this.userChange = userChange;
	}
	public String getPromoNo() {
		return promoNo;
	}
	public void setPromoNo(String promoNo) {
		this.promoNo = promoNo;
	}
	
	@Override
	public Object clone() throws CloneNotSupportedException {
		return super.clone();
	}
	public long getNoOfDaysInPromoDuration() {
		return noOfDaysInPromoDuration;
	}
	public void setNoOfDaysInPromoDuration(long noOfDaysInPromoDuration) {
		this.noOfDaysInPromoDuration = noOfDaysInPromoDuration;
	}
	public int getPrestoItemCode() {
		return prestoItemCode;
	}
	public void setPrestoItemCode(int prestoItemCode) {
		this.prestoItemCode = prestoItemCode;
	}
}
