package com.pristine.dto.offermgmt.prediction;

import com.pristine.dto.offermgmt.MultiplePrice;
import com.pristine.util.Constants;

public class PredictionReportItemDTO {
	private int productLevelId;
	private int productId;
	private int locationLevelId;
	private int locationId;
	private MultiplePrice regPrice = null;
	private MultiplePrice salePrice = null;
	private String retailerItemCode;
	private String itemName;
	private String categoryName;
	private int adPageNo = Constants.NULLID;
	private int blockNo = Constants.NULLID;
	private int displayTypeId = Constants.NULLID;
	private int promoTypeId = Constants.NULLID;
	private int retLirId;
	private String lirName;
	private long weeklyPredictedMovement = 0l;
	private long clientWeeklyPredictedMovement =0l;
	private long weeklyActualMovement = 0l;
	
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
}
