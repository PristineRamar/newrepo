package com.pristine.dto.offermgmt.multiWeekPrediction;

import com.fasterxml.jackson.annotation.JsonIgnore;
import com.fasterxml.jackson.annotation.JsonProperty;

public class MultiWeekPredItemAPIDTO {
	
	@JsonProperty("i-c")
	private int itemCode;
	@JsonProperty("p-l-i")
	private int productLevelId;
	@JsonProperty("p-i")
	private int productId;
	@JsonProperty("u")
	private String UPC;
	@JsonProperty("s-d")
	private String startDate;
	@JsonProperty("e-d")
	private String endDate;
	@JsonProperty("r-m")
	private int regMultiple;
	@JsonProperty("r-p")
	private double regPrice;
	@JsonProperty("s-m")
	private int saleMultiple;
	@JsonProperty("s-p")
	private double salePrice;
	@JsonProperty("a-p-n")
	private int adPageNo;
	@JsonProperty("a-b-n")
	private int adBlockNo;
	@JsonProperty("d-t-i")
	private int displayTypeId;
	@JsonProperty("p-t-i")
	private int promoTypeId;
	@JsonProperty("p-m")
	private double predictedMovement = 0d;
	@JsonProperty("p-s")
	private int predictionStatus;

	public int getItemCode() {
		return itemCode;
	}

	public void setItemCode(int itemCode) {
		this.itemCode = itemCode;
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

	public String getEndDate() {
		return endDate;
	}

	public void setEndDate(String endDate) {
		this.endDate = endDate;
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

}