package com.pristine.dto.offermgmt.prediction;

import com.fasterxml.jackson.annotation.JsonProperty;
import com.pristine.dto.offermgmt.MultiplePrice;
import com.pristine.util.Constants;

public class PredictionReportItemUIDTO {
	@JsonProperty("p-l-i")
	private int productLevelId;
	@JsonProperty("p-i")
	private int productId;
	@JsonProperty("l-l-i")
	private int locationLevelId;
	@JsonProperty("l-i")
	private int locationId;
	@JsonProperty("r-p")
	private MultiplePrice regPrice = null;
	@JsonProperty("s-p")
	private MultiplePrice salePrice = null;
	@JsonProperty("r-l-c")
	private String retailerItemCode;
	@JsonProperty("i-n")
	private String itemName;
	@JsonProperty("c-n")
	private String categoryName;
	@JsonProperty("a-p-n")
	private int adPageNo = Constants.NULLID;
	@JsonProperty("b-n")
	private int blockNo = Constants.NULLID;
	@JsonProperty("d-t-i")
	private int displayTypeId = Constants.NULLID;
	@JsonProperty("p-t-i")
	private int promoTypeId = Constants.NULLID;
	@JsonProperty("r-l-i")
	private int retLirId;
	@JsonProperty("l-n")
	private String lirName;
	@JsonProperty("w-p-m")
	private long weeklyPredictedMovement = 0l;
	@JsonProperty("c-w-p-m")
	private long clientWeeklyPredictedMovement =0l;
	@JsonProperty("w-a-m")
	private long weeklyActualMovement = 0l;
	@JsonProperty("i-c-b")
	private int noOfLigorNonLigInABlock =0;
	
	public int getProductLevelId() {
		return productLevelId;
	}
	public void setProductLevelId(int productLevelId) {
		this.productLevelId = productLevelId;
	}
	public int getProductId() {
		return productId;
	}
	public void setProductId(int productId) {
		this.productId = productId;
	}
	public int getLocationLevelId() {
		return locationLevelId;
	}
	public void setLocationLevelId(int locationLevelId) {
		this.locationLevelId = locationLevelId;
	}
	public int getLocationId() {
		return locationId;
	}
	public void setLocationId(int locationId) {
		this.locationId = locationId;
	}
	public MultiplePrice getRegPrice() {
		return regPrice;
	}
	public void setRegPrice(MultiplePrice regPrice) {
		this.regPrice = regPrice;
	}
	public MultiplePrice getSalePrice() {
		return salePrice;
	}
	public void setSalePrice(MultiplePrice salePrice) {
		this.salePrice = salePrice;
	}
	public String getRetailerItemCode() {
		return retailerItemCode;
	}
	public void setRetailerItemCode(String retailerItemCode) {
		this.retailerItemCode = retailerItemCode;
	}
	public String getItemName() {
		return itemName;
	}
	public void setItemName(String itemName) {
		this.itemName = itemName;
	}
	public String getCategoryName() {
		return categoryName;
	}
	public void setCategoryName(String categoryName) {
		this.categoryName = categoryName;
	}
	public int getAdPageNo() {
		return adPageNo;
	}
	public void setAdPageNo(int adPageNo) {
		this.adPageNo = adPageNo;
	}
	public int getBlockNo() {
		return blockNo;
	}
	public void setBlockNo(int blockNo) {
		this.blockNo = blockNo;
	}
	public int getDisplayTypeId() {
		return displayTypeId;
	}
	public void setDisplayTypeId(int displayTypeId) {
		this.displayTypeId = displayTypeId;
	}
	public int getPromoTypeId() {
		return promoTypeId;
	}
	public void setPromoTypeId(int promoTypeId) {
		this.promoTypeId = promoTypeId;
	}
	public int getRetLirId() {
		return retLirId;
	}
	public void setRetLirId(int retLirId) {
		this.retLirId = retLirId;
	}
	public String getLirName() {
		return lirName;
	}
	public void setLirName(String lirName) {
		this.lirName = lirName;
	}
	public long getWeeklyPredictedMovement() {
		return weeklyPredictedMovement;
	}
	public void setWeeklyPredictedMovement(long weeklyPredictedMovement) {
		this.weeklyPredictedMovement = weeklyPredictedMovement;
	}
	public long getClientWeeklyPredictedMovement() {
		return clientWeeklyPredictedMovement;
	}
	public void setClientWeeklyPredictedMovement(long clientWeeklyPredictedMovement) {
		this.clientWeeklyPredictedMovement = clientWeeklyPredictedMovement;
	}
	public long getWeeklyActualMovement() {
		return weeklyActualMovement;
	}
	public void setWeeklyActualMovement(long weeklyActualMovement) {
		this.weeklyActualMovement = weeklyActualMovement;
	}
	public int getNoOfLigorNonLigInABlock() {
		return noOfLigorNonLigInABlock;
	}
	public void setNoOfLigorNonLigInABlock(int noOfLigorNonLigInABlock) {
		this.noOfLigorNonLigInABlock = noOfLigorNonLigInABlock;
	}
}
