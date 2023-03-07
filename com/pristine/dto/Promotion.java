package com.pristine.dto;

import com.fasterxml.jackson.annotation.JsonIgnore;

public class Promotion {

	private int itemCode;
	private int constraint;
	private int lirId;
	private int calendarId;
	private long promoDefinitionId;
	private int locationLevelId;
	private int locationId;
	private String startDate;
	private String endDate;
	private String calendarStartDate;
	private String calendarEndDate;
	private int promotionTypeId;
	private String promoGroupId;
	private String promoNumber;
	private int offerPriceQty;
	private double offerPrice;
	private int mustBuyPriceQty;
	private double mustBuyPrice;
	private int offerCount;
	private int offerTypeId;
	private String offerType;
	private String offerUnitType;
	private double offerValue;
	private int buyX;
	private int getY;
	private int minQtyReqd;
	private double minAmtReqd;
	private int regPriceQty;
	private double regPrice;
	private int storeCount;
	private int startCalendarId;
	private String promotionName;
	private String promotionTypeName;
	private String globalPromotion;

	@JsonIgnore
	private Integer pageNumber = -1;
	@JsonIgnore
	private Integer blockNumber = -1;
	@JsonIgnore
	private int displayTypeId;
	@JsonIgnore
	private int subDisplayTypeId;
	@JsonIgnore
	private String adStartDate;
	@JsonIgnore
	private String adEndDate;
	@JsonIgnore
	private String displayStartDate;
	@JsonIgnore
	private String displayEndDate;

	public int getItemCode() {
		return itemCode;
	}

	public void setItemCode(int itemCode) {
		this.itemCode = itemCode;
	}

	public int getConstraint() {
		return constraint;
	}

	public void setConstraint(int constraint) {
		this.constraint = constraint;
	}

	public int getLirId() {
		return lirId;
	}

	public void setLirId(int lirId) {
		this.lirId = lirId;
	}

	public int getCalendarId() {
		return calendarId;
	}

	public void setCalendarId(int calendarId) {
		this.calendarId = calendarId;
	}

	public long getPromoDefinitionId() {
		return promoDefinitionId;
	}

	public void setPromoDefinitionId(long promoDefinitionId) {
		this.promoDefinitionId = promoDefinitionId;
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

	public String getCalendarStartDate() {
		return calendarStartDate;
	}

	public void setCalendarStartDate(String calendarStartDate) {
		this.calendarStartDate = calendarStartDate;
	}

	public String getCalendarEndDate() {
		return calendarEndDate;
	}

	public void setCalendarEndDate(String calendarEndDate) {
		this.calendarEndDate = calendarEndDate;
	}

	public int getPromotionTypeId() {
		return promotionTypeId;
	}

	public void setPromotionTypeId(int promotionTypeId) {
		this.promotionTypeId = promotionTypeId;
	}

	public String getPromoGroupId() {
		return promoGroupId;
	}

	public void setPromoGroupId(String promoGroupId) {
		this.promoGroupId = promoGroupId;
	}

	public String getPromoNumber() {
		return promoNumber;
	}

	public void setPromoNumber(String promoNumber) {
		this.promoNumber = promoNumber;
	}

	public int getOfferCount() {
		return offerCount;
	}

	public void setOfferCount(int offerCount) {
		this.offerCount = offerCount;
	}

	public int getOfferTypeId() {
		return offerTypeId;
	}

	public void setOfferTypeId(int offerTypeId) {
		this.offerTypeId = offerTypeId;
	}

	public String getOfferType() {
		return offerType;
	}

	public void setOfferType(String offerType) {
		this.offerType = offerType;
	}

	public String getOfferUnitType() {
		return offerUnitType;
	}

	public void setOfferUnitType(String offerUnitType) {
		this.offerUnitType = offerUnitType;
	}

	public double getOfferValue() {
		return offerValue;
	}

	public void setOfferValue(double offerValue) {
		this.offerValue = offerValue;
	}

	public int getBuyX() {
		return buyX;
	}

	public void setBuyX(int buyX) {
		this.buyX = buyX;
	}

	public int getGetY() {
		return getY;
	}

	public void setGetY(int getY) {
		this.getY = getY;
	}

	public int getMinQtyReqd() {
		return minQtyReqd;
	}

	public void setMinQtyReqd(int minQtyReqd) {
		this.minQtyReqd = minQtyReqd;
	}

	public double getMinAmtReqd() {
		return minAmtReqd;
	}

	public void setMinAmtReqd(double minAmtReqd) {
		this.minAmtReqd = minAmtReqd;
	}

	public int getOfferPriceQty() {
		return offerPriceQty;
	}

	public void setOfferPriceQty(int offerPriceQty) {
		this.offerPriceQty = offerPriceQty;
	}

	public double getOfferPrice() {
		return offerPrice;
	}

	public void setOfferPrice(double offerPrice) {
		this.offerPrice = offerPrice;
	}

	public int getMustBuyPriceQty() {
		return mustBuyPriceQty;
	}

	public void setMustBuyPriceQty(int mustBuyPriceQty) {
		this.mustBuyPriceQty = mustBuyPriceQty;
	}

	public double getMustBuyPrice() {
		return mustBuyPrice;
	}

	public void setMustBuyPrice(double mustBuyPrice) {
		this.mustBuyPrice = mustBuyPrice;
	}

	public int getRegPriceQty() {
		return regPriceQty;
	}

	public void setRegPriceQty(int regPriceQty) {
		this.regPriceQty = regPriceQty;
	}

	public double getRegPrice() {
		return regPrice;
	}

	public void setRegPrice(double regPrice) {
		this.regPrice = regPrice;
	}

	public int getStoreCount() {
		return storeCount;
	}

	public void setStoreCount(int storeCount) {
		this.storeCount = storeCount;
	}

	public Integer getPageNumber() {
		return pageNumber;
	}

	public void setPageNumber(Integer pageNumber) {
		this.pageNumber = pageNumber;
	}

	public Integer getBlockNumber() {
		return blockNumber;
	}

	public void setBlockNumber(Integer blockNumber) {
		this.blockNumber = blockNumber;
	}

	public int getDisplayTypeId() {
		return displayTypeId;
	}

	public void setDisplayTypeId(int displayTypeId) {
		this.displayTypeId = displayTypeId;
	}

	public int getSubDisplayTypeId() {
		return subDisplayTypeId;
	}

	public void setSubDisplayTypeId(int subDisplayTypeId) {
		this.subDisplayTypeId = subDisplayTypeId;
	}

	public String getAdStartDate() {
		return adStartDate;
	}

	public void setAdStartDate(String adStartDate) {
		this.adStartDate = adStartDate;
	}

	public String getAdEndDate() {
		return adEndDate;
	}

	public void setAdEndDate(String adEndDate) {
		this.adEndDate = adEndDate;
	}

	public String getDisplayStartDate() {
		return displayStartDate;
	}

	public void setDisplayStartDate(String displayStartDate) {
		this.displayStartDate = displayStartDate;
	}

	public String getDisplayEndDate() {
		return displayEndDate;
	}

	public void setDisplayEndDate(String displayEndDate) {
		this.displayEndDate = displayEndDate;
	}

	public int getStartCalendarId() {
		return startCalendarId;
	}

	public void setStartCalendarId(int startCalendarId) {
		this.startCalendarId = startCalendarId;
	}

	public String getPromotionName() {
		return promotionName;
	}

	public void setPromotionName(String promotionName) {
		this.promotionName = promotionName;
	}

	public String getPromotionTypeName() {
		return promotionTypeName;
	}

	public void setPromotionTypeName(String promotionTypeName) {
		this.promotionTypeName = promotionTypeName;
	}

	public String getGlobalPromotion() {
		return globalPromotion;
	}

	public void setGlobalPromotion(String globalPromotion) {
		this.globalPromotion = globalPromotion;
	}

	@Override
	public String toString() {
		return "Promotion [itemCode=" + itemCode + ", constraint=" + constraint + ", lirId=" + lirId + ", calendarId="
				+ calendarId + ", promoDefinitionId=" + promoDefinitionId + ", locationLevelId=" + locationLevelId
				+ ", locationId=" + locationId + ", startDate=" + startDate + ", endDate=" + endDate
				+ ", calendarStartDate=" + calendarStartDate + ", calendarEndDate=" + calendarEndDate
				+ ", promotionTypeId=" + promotionTypeId + ", promoGroupId=" + promoGroupId + ", promoNumber="
				+ promoNumber + ", offerPriceQty=" + offerPriceQty + ", offerPrice=" + offerPrice + ", mustBuyPriceQty="
				+ mustBuyPriceQty + ", mustBuyPrice=" + mustBuyPrice + ", offerCount=" + offerCount + ", offerTypeId="
				+ offerTypeId + ", offerType=" + offerType + ", offerUnitType=" + offerUnitType + ", offerValue="
				+ offerValue + ", buyX=" + buyX + ", getY=" + getY + ", minQtyReqd=" + minQtyReqd + ", minAmtReqd="
				+ minAmtReqd + ", regPriceQty=" + regPriceQty + ", regPrice=" + regPrice + ", storeCount=" + storeCount
				+ ", startCalendarId=" + startCalendarId + ", promotionName=" + promotionName + ", promotionTypeName="
				+ promotionTypeName + ", globalPromotion=" + globalPromotion + ", pageNumber=" + pageNumber
				+ ", blockNumber=" + blockNumber + ", displayTypeId=" + displayTypeId + ", subDisplayTypeId="
				+ subDisplayTypeId + ", adStartDate=" + adStartDate + ", adEndDate=" + adEndDate + ", displayStartDate="
				+ displayStartDate + ", displayEndDate=" + displayEndDate + ", getItemCode()=" + getItemCode()
				+ ", getConstraint()=" + getConstraint() + ", getLirId()=" + getLirId() + ", getCalendarId()="
				+ getCalendarId() + ", getPromoDefinitionId()=" + getPromoDefinitionId() + ", getLocationLevelId()="
				+ getLocationLevelId() + ", getLocationId()=" + getLocationId() + ", getStartDate()=" + getStartDate()
				+ ", getEndDate()=" + getEndDate() + ", getCalendarStartDate()=" + getCalendarStartDate()
				+ ", getCalendarEndDate()=" + getCalendarEndDate() + ", getPromotionTypeId()=" + getPromotionTypeId()
				+ ", getPromoGroupId()=" + getPromoGroupId() + ", getPromoNumber()=" + getPromoNumber()
				+ ", getOfferCount()=" + getOfferCount() + ", getOfferTypeId()=" + getOfferTypeId()
				+ ", getOfferType()=" + getOfferType() + ", getOfferUnitType()=" + getOfferUnitType()
				+ ", getOfferValue()=" + getOfferValue() + ", getBuyX()=" + getBuyX() + ", getGetY()=" + getGetY()
				+ ", getMinQtyReqd()=" + getMinQtyReqd() + ", getMinAmtReqd()=" + getMinAmtReqd()
				+ ", getOfferPriceQty()=" + getOfferPriceQty() + ", getOfferPrice()=" + getOfferPrice()
				+ ", getMustBuyPriceQty()=" + getMustBuyPriceQty() + ", getMustBuyPrice()=" + getMustBuyPrice()
				+ ", getRegPriceQty()=" + getRegPriceQty() + ", getRegPrice()=" + getRegPrice() + ", getStoreCount()="
				+ getStoreCount() + ", getPageNumber()=" + getPageNumber() + ", getBlockNumber()=" + getBlockNumber()
				+ ", getDisplayTypeId()=" + getDisplayTypeId() + ", getSubDisplayTypeId()=" + getSubDisplayTypeId()
				+ ", getAdStartDate()=" + getAdStartDate() + ", getAdEndDate()=" + getAdEndDate()
				+ ", getDisplayStartDate()=" + getDisplayStartDate() + ", getDisplayEndDate()=" + getDisplayEndDate()
				+ ", getStartCalendarId()=" + getStartCalendarId() + ", getPromotionName()=" + getPromotionName()
				+ ", getPromotionTypeName()=" + getPromotionTypeName() + ", getGlobalPromotion()="
				+ getGlobalPromotion() + ", getClass()=" + getClass() + ", hashCode()=" + hashCode() + ", toString()="
				+ super.toString() + "]";
	}

	
}
