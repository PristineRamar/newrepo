package com.pristine.dto.offermgmt.multiWeekPrediction;

import java.time.LocalDate;

import com.fasterxml.jackson.annotation.JsonIgnore;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.pristine.dto.offermgmt.mwr.MultiWeekPredKey;
import com.pristine.util.offermgmt.PRCommonUtil;

public class MultiWeekPredEngineItemDTO implements Cloneable{
	private int scenarioId;
	@JsonProperty("prestoItemCode")
	private int itemCode;
	private int locationLevelId;
	private int locationId;
	private int productLevelId;
	private int productId;
	@JsonProperty("upc")
	private String UPC;
	private String startDate;
	@JsonProperty("regQuantity")
	private int regMultiple;
	private double regPrice;
	@JsonProperty("saleQuantity")
	private int saleMultiple;
	private double salePrice;
	@JsonProperty("adPage")
	private int adPageNo;
	@JsonProperty("adBlock")
	private int adBlockNo;
	private int displayTypeId;
	private int promoTypeId;
	private double predictedMovement = 0d;
	private int predictionStatus;
	@JsonIgnore
	private int retLirId;
	@JsonIgnore
	private int calendarId;
	@JsonProperty("offer_unit_type")
	private String offerUnitType;
	@JsonProperty("offer_value")
	private double offerValue;
	@JsonProperty("MIN_QTY_REQUIRED")
	private int minQtyReqd;
	@JsonIgnore
	private boolean sendToPrediction;
	@JsonIgnore
	private boolean noRecentWeeksMov;
	
	@JsonProperty("finalCost")
	private double finalCost=0;
	@JsonProperty("LIST_COST")
	private double listCost;
	@JsonProperty("DEAL_COST")
	private double dealCost;
	@JsonProperty("VIP_COST")
	private double vipCost;
	private double predictedSales = 0d;
	private double predictedMargin = 0d;
	private String promoStartDate;
	private String promoEndDate;
	private String adStartDate;
	private String adEndDate;
	private String displayStartDate;
	private String displayEndDate;
	private double predictedASP;

	public int getScenarioId() {
		return scenarioId;
	}

	public void setScenarioId(int scenarioId) {
		this.scenarioId = scenarioId;
	}

	public int getItemCode() {
		return itemCode;
	}

	public void setItemCode(int itemCode) {
		this.itemCode = itemCode;
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

	public String getUPC() {
		return UPC;
	}

	public void setUPC(String uPC) {
		UPC = uPC;
	}

	public String getStartDate() {
		return startDate;
	}

	public void setStartDate(String startDate) {
		this.startDate = startDate;
	}

	public int getRegMultiple() {
		return regMultiple;
	}

	public void setRegMultiple(int regMultiple) {
		this.regMultiple = regMultiple;
	}

	public double getRegPrice() {
		return regPrice;
	}

	public void setRegPrice(double regPrice) {
		this.regPrice = regPrice;
	}

	public int getSaleMultiple() {
		return saleMultiple;
	}

	public void setSaleMultiple(int saleMultiple) {
		this.saleMultiple = saleMultiple;
	}

	public double getSalePrice() {
		return salePrice;
	}

	public void setSalePrice(double salePrice) {
		this.salePrice = salePrice;
	}

	public int getAdPageNo() {
		return adPageNo;
	}

	public void setAdPageNo(int adPageNo) {
		this.adPageNo = adPageNo;
	}

	public int getAdBlockNo() {
		return adBlockNo;
	}

	public void setAdBlockNo(int adBlockNo) {
		this.adBlockNo = adBlockNo;
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

	public double getPredictedMovement() {
		return predictedMovement;
	}

	public void setPredictedMovement(double predictedMovement) {
		this.predictedMovement = predictedMovement;
	}

	public int getPredictionStatus() {
		return predictionStatus;
	}

	public void setPredictionStatus(int predictionStatus) {
		this.predictionStatus = predictionStatus;
	}

	public int getRetLirId() {
		return retLirId;
	}

	public void setRetLirId(int retLirId) {
		this.retLirId = retLirId;
	}

	public int getCalendarId() {
		return calendarId;
	}

	public void setCalendarId(int calendarId) {
		this.calendarId = calendarId;
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

	public int getMinQtyReqd() {
		return minQtyReqd;
	}

	public void setMinQtyReqd(int minQtyReqd) {
		this.minQtyReqd = minQtyReqd;
	}

	public static MultiWeekPredKey getMultiWeekPredKey(MultiWeekPredEngineItemDTO multiWeekPredEngineItemDTO) {
		MultiWeekPredKey key = new MultiWeekPredKey(multiWeekPredEngineItemDTO.itemCode,
				multiWeekPredEngineItemDTO.regMultiple, multiWeekPredEngineItemDTO.regPrice,
				multiWeekPredEngineItemDTO.saleMultiple, multiWeekPredEngineItemDTO.salePrice,
				multiWeekPredEngineItemDTO.adPageNo, multiWeekPredEngineItemDTO.adBlockNo,
				multiWeekPredEngineItemDTO.displayTypeId, multiWeekPredEngineItemDTO.promoTypeId,
				multiWeekPredEngineItemDTO.calendarId);
		return key;
	}

	@Override
    public Object clone() throws CloneNotSupportedException {
		return (MultiWeekPredEngineItemDTO) super.clone();
	}
	
	@JsonIgnore
	public LocalDate getStartDateAsLocalDate() {
		if (this.startDate == null) {
			return null;
		} else {
			try {
				return LocalDate.parse(this.startDate, PRCommonUtil.getDateFormatter());
			} catch (Exception e) {
				return null;
			}
		}
	}

	public boolean isSendToPrediction() {
		return sendToPrediction;
	}

	public void setSendToPrediction(boolean isStrRevless) {
		this.sendToPrediction = isStrRevless;
	}

	public boolean isNoRecentWeeksMov() {
		return noRecentWeeksMov;
	}

	public void setNoRecentWeeksMov(boolean noRecentWeeksMov) {
		this.noRecentWeeksMov = noRecentWeeksMov;
	}

	public double getFinalCost() {
		return finalCost;
	}

	public void setFinalCost(double finalCost) {
		this.finalCost = finalCost;
	}

	public double getListCost() {
		return listCost;
	}

	public void setListCost(double listCost) {
		this.listCost = listCost;
	}

	public double getDealCost() {
		return dealCost;
	}

	public void setDealCost(double dealCost) {
		this.dealCost = dealCost;
	}

	public double getVipCost() {
		return vipCost;
	}

	public void setVipCost(double vipCost) {
		this.vipCost = vipCost;
	}

	public double getPredictedSales() {
		return predictedSales;
	}

	public void setPredictedSales(double predictedSales) {
		this.predictedSales = predictedSales;
	}

	public double getPredictedMargin() {
		return predictedMargin;
	}

	public void setPredictedMargin(double predictedMargin) {
		this.predictedMargin = predictedMargin;
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

	public double getPredictedASP() {
		return predictedASP;
	}

	public void setPredictedASP(double predictedASP) {
		this.predictedASP = predictedASP;
	}
	
	
}
