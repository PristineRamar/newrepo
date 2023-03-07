package com.pristine.dto.pricingalert;

import java.util.HashMap;

public class GoalSettingsDTO {
	/*private int chainLocationLevelId;
	private int chainLocationId;
	private int locationLevelId;
	private int locationId;
	private int productLevelId;
	private int productId;
	private int priceCheckListId;
	private int itemCode;*/
	
	private int curRegMinPriceIndex;
	private int curRegMaxPriceIndex;
	private int curSaleMinPriceIndex;
	private int curSaleMaxPriceIndex;
	
	private int futRegMinPriceIndex;
	private int futRegMaxPriceIndex;
	private int futSaleMinPriceIndex;
	private int futSaleMaxPriceIndex;
	
	private int curRegMinMargin;
	private int curRegMaxMargin;
	private int curSaleMinMargin;
	private int curSaleMaxMargin;
	
	private int futRegMinMargin;
	private int futRegMaxMargin;
	private int futSaleMinMargin;
	private int futSaleMaxMargin;
	
	private GoalSettingsDTO chainLevelGoal;
	private GoalSettingsDTO locationLevelGoal;
	private GoalSettingsDTO productLevelGoal;
	private HashMap<Integer, GoalSettingsDTO> priceCheckLevelGoal;
	private HashMap<Integer, GoalSettingsDTO> itemLevelGoal;
	private HashMap<Integer, GoalSettingsDTO> brandLevelGoal;
	
	private String itemName;
	private String priceCheckListName;
	private String brandName;
	private String chainName;
	private String locationName;
	
	private int locationLevelId;
	private int locationId;
	private int priceCheckListId;
	private int brandId;
	
	private Double overallGoal;
	
	public int getCurRegMinPriceIndex() {
		return curRegMinPriceIndex;
	}
	public void setCurRegMinPriceIndex(int curRegMinPriceIndex) {
		this.curRegMinPriceIndex = curRegMinPriceIndex;
	}
	public int getCurRegMaxPriceIndex() {
		return curRegMaxPriceIndex;
	}
	public void setCurRegMaxPriceIndex(int curRegMaxPriceIndex) {
		this.curRegMaxPriceIndex = curRegMaxPriceIndex;
	}
	public int getCurSaleMinPriceIndex() {
		return curSaleMinPriceIndex;
	}
	public void setCurSaleMinPriceIndex(int curSaleMinPriceIndex) {
		this.curSaleMinPriceIndex = curSaleMinPriceIndex;
	}
	public int getCurSaleMaxPriceIndex() {
		return curSaleMaxPriceIndex;
	}
	public void setCurSaleMaxPriceIndex(int curSaleMaxPriceIndex) {
		this.curSaleMaxPriceIndex = curSaleMaxPriceIndex;
	}
	public int getFutRegMinPriceIndex() {
		return futRegMinPriceIndex;
	}
	public void setFutRegMinPriceIndex(int futRegMinPriceIndex) {
		this.futRegMinPriceIndex = futRegMinPriceIndex;
	}
	public int getFutRegMaxPriceIndex() {
		return futRegMaxPriceIndex;
	}
	public void setFutRegMaxPriceIndex(int futRegMaxPriceIndex) {
		this.futRegMaxPriceIndex = futRegMaxPriceIndex;
	}
	public int getFutSaleMinPriceIndex() {
		return futSaleMinPriceIndex;
	}
	public void setFutSaleMinPriceIndex(int futSaleMinPriceIndex) {
		this.futSaleMinPriceIndex = futSaleMinPriceIndex;
	}
	public int getFutSaleMaxPriceIndex() {
		return futSaleMaxPriceIndex;
	}
	public void setFutSaleMaxPriceIndex(int futSaleMaxPriceIndex) {
		this.futSaleMaxPriceIndex = futSaleMaxPriceIndex;
	}
	public int getCurRegMinMargin() {
		return curRegMinMargin;
	}
	public void setCurRegMinMargin(int curRegMinMargin) {
		this.curRegMinMargin = curRegMinMargin;
	}
	public int getCurRegMaxMargin() {
		return curRegMaxMargin;
	}
	public void setCurRegMaxMargin(int curRegMaxMargin) {
		this.curRegMaxMargin = curRegMaxMargin;
	}
	public int getCurSaleMinMargin() {
		return curSaleMinMargin;
	}
	public void setCurSaleMinMargin(int curSaleMinMargin) {
		this.curSaleMinMargin = curSaleMinMargin;
	}
	public int getCurSaleMaxMargin() {
		return curSaleMaxMargin;
	}
	public void setCurSaleMaxMargin(int curSaleMaxMargin) {
		this.curSaleMaxMargin = curSaleMaxMargin;
	}
	public int getFutRegMinMargin() {
		return futRegMinMargin;
	}
	public void setFutRegMinMargin(int futRegMinMargin) {
		this.futRegMinMargin = futRegMinMargin;
	}
	public int getFutRegMaxMargin() {
		return futRegMaxMargin;
	}
	public void setFutRegMaxMargin(int futRegMaxMargin) {
		this.futRegMaxMargin = futRegMaxMargin;
	}
	public int getFutSaleMinMargin() {
		return futSaleMinMargin;
	}
	public void setFutSaleMinMargin(int futSaleMinMargin) {
		this.futSaleMinMargin = futSaleMinMargin;
	}
	public int getFutSaleMaxMargin() {
		return futSaleMaxMargin;
	}
	public void setFutSaleMaxMargin(int futSaleMaxMargin) {
		this.futSaleMaxMargin = futSaleMaxMargin;
	}
	public GoalSettingsDTO getChainLevelGoal() {
		return chainLevelGoal;
	}
	public void setChainLevelGoal(GoalSettingsDTO chainLevelGoal) {
		this.chainLevelGoal = chainLevelGoal;
	}
	public GoalSettingsDTO getLocationLevelGoal() {
		return locationLevelGoal;
	}
	public void setLocationLevelGoal(GoalSettingsDTO locationLevelGoal) {
		this.locationLevelGoal = locationLevelGoal;
	}
	public GoalSettingsDTO getProductLevelGoal() {
		return productLevelGoal;
	}
	public void setProductLevelGoal(GoalSettingsDTO productLevelGoal) {
		this.productLevelGoal = productLevelGoal;
	}
	public HashMap<Integer, GoalSettingsDTO> getPriceCheckLevelGoal() {
		return priceCheckLevelGoal;
	}
	public void setPriceCheckLevelGoal(HashMap<Integer, GoalSettingsDTO> priceCheckLevelGoal) {
		this.priceCheckLevelGoal = priceCheckLevelGoal;
	}
	public HashMap<Integer, GoalSettingsDTO> getItemLevelGoal() {
		return itemLevelGoal;
	}
	public void setItemLevelGoal(HashMap<Integer, GoalSettingsDTO> itemLevelGoal) {
		this.itemLevelGoal = itemLevelGoal;
	}
	public HashMap<Integer, GoalSettingsDTO> getBrandLevelGoal() {
		return brandLevelGoal;
	}
	public void setBrandLevelGoal(HashMap<Integer, GoalSettingsDTO> brandLevelGoal) {
		this.brandLevelGoal = brandLevelGoal;
	}
	public String getItemName() {
		return itemName;
	}
	public void setItemName(String itemName) {
		this.itemName = itemName;
	}
	public String getPriceCheckListName() {
		return priceCheckListName;
	}
	public void setPriceCheckListName(String priceCheckListName) {
		this.priceCheckListName = priceCheckListName;
	}
	public String getBrandName() {
		return brandName;
	}
	public void setBrandName(String brandName) {
		this.brandName = brandName;
	}
	public String getChainName() {
		return chainName;
	}
	public void setChainName(String chainName) {
		this.chainName = chainName;
	}
	public String getLocationName() {
		return locationName;
	}
	public void setLocationName(String locationName) {
		this.locationName = locationName;
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
	public Double getOverallGoal() {
		return overallGoal;
	}
	public void setOverallGoal(Double overallGoal) {
		this.overallGoal = overallGoal;
	}
	public int getPriceCheckListId() {
		return priceCheckListId;
	}
	public void setPriceCheckListId(int priceCheckListId) {
		this.priceCheckListId = priceCheckListId;
	}
	public int getBrandId() {
		return brandId;
	}
	public void setBrandId(int brandId) {
		this.brandId = brandId;
	}
}
