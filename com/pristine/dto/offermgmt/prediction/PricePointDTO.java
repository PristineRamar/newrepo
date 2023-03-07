package com.pristine.dto.offermgmt.prediction;

import java.io.Serializable;
import java.text.DecimalFormat;

import com.fasterxml.jackson.annotation.JsonIgnore;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.pristine.service.offermgmt.prediction.PredictionStatus;
//import com.pristine.util.Constants;

public class PricePointDTO implements Serializable{
	/**
	 * 
	 */
	private static final long serialVersionUID = 6140935445588108976L;

	//DecimalFormat twoDigitFormat = new DecimalFormat("######.##");
	final DecimalFormat twoDigitFormat = new DecimalFormat("0.00");
	
	@JsonProperty("r-q")
	private int regQuantity = 1;
	@JsonProperty("r-p")
	private Double regPrice;
	
	@JsonProperty("s-q")
	private int saleQuantity;
	@JsonProperty("s-p")
	private Double salePrice;
	@JsonProperty("a-p-n")
	private int adPageNo;
	@JsonProperty("b-n")
	private int blockNo;
	@JsonProperty("d-t-i")
	private int displayTypeId ;
	@JsonProperty("p-t-i")
	private int promoTypeId;
	
	@JsonProperty("p-m")
	private Double predictedMovement = 0d;
	@JsonProperty("p-m-b-s")
	private Double predictedMovementBeforeSubsAdjustment = 0d;
	@JsonProperty("e-p")
	private String explainPrediction = null;
	@JsonProperty("i-a-p")
	private Boolean isAlreadyPredicted = false;
	@JsonIgnore
	private Double confidenceLevelLower = 0d;
	@JsonIgnore
	private Double confidenceLevelUpper = 0d;
	
	@JsonIgnore
	PredictionStatus predictionStatus;
	@JsonProperty("p-s")
	int predictionStatusForUi;
	//Kept as String as it may have NA/0/positive/negative number as value and 0 is diff from NA
	@JsonProperty("s-i-min")
	private String substituteImpactMin = "";
	@JsonProperty("s-i-max")
	private String substituteImpactMax = "";
	
	private String questionablePrediction = "";
	
	private boolean isCurrentPrice = false; //default value is false
	private boolean isNewPricePoint = false;//default value is false
	private boolean isInputPricePoint = true;
	
	public int getRegQuantity() {
		return regQuantity;
	}
	public void setRegQuantity(int regQuantity) {
		this.regQuantity = regQuantity;
	}
	public Double getRegPrice() {
		return regPrice;
	}
	public void setRegPrice(Double regPrice) {
		this.regPrice = regPrice;
	}
	public Double getPredictedMovement() {
		return predictedMovement;
	}
	public void setPredictedMovement(Double predictedMovement) {
		this.predictedMovement = predictedMovement;
	}
	public String getExplainPrediction() {
		return explainPrediction;
	}
	public void setExplainPrediction(String explainPrediction) {
		this.explainPrediction = explainPrediction;
	}
	public Boolean getIsAlreadyPredicted() {
		return isAlreadyPredicted;
	}
	public void setIsAlreadyPredicted(Boolean isAlreadyPredicted) {
		this.isAlreadyPredicted = isAlreadyPredicted;
	}
	@JsonIgnore
	public Double getFormattedRegPrice() {
		Double formattedRegPrice = new Double(twoDigitFormat.format(regPrice));
		return formattedRegPrice;
	}
	@JsonIgnore
	public Double getFormattedSalePrice() {
		if (salePrice != null) {
			Double formattedSalePrice = new Double(twoDigitFormat.format(salePrice));
			return formattedSalePrice;
		} else {
			return 0d;
		}
	}
	public PredictionStatus getPredictionStatus() {
		return predictionStatus;
	}
	public void setPredictionStatus(PredictionStatus predictionStatus) {
		this.predictionStatus = predictionStatus;
	}
	
	@Override
	public String toString() {
		return "PricePointDTO [regPrice=" + regQuantity + "/" + regPrice + ", salePrice=" + saleQuantity + "/" + salePrice + ", adPageNo=" + adPageNo
				+ ", blockNo=" + blockNo + ", displayTypeId=" + displayTypeId + ", promoTypeId=" + promoTypeId + ", predictedMovement="
				+ predictedMovement + ", predictionStatus=" + predictionStatus + ", questionablePrediction=" + questionablePrediction +  "]";
	}
	public int getPredictionStatusForUi() {		
		if(this.predictionStatus != null)
			return this.predictionStatus.getStatusCode();
		else
			return 0;
	}
	public String getSubstituteImpactMin() {
		return substituteImpactMin;
	}
	public void setSubstituteImpactMin(String substituteImpactMin) {
		this.substituteImpactMin = substituteImpactMin;
	}
	public String getSubstituteImpactMax() {
		return substituteImpactMax;
	}
	public void setSubstituteImpactMax(String substituteImpactMax) {
		this.substituteImpactMax = substituteImpactMax;
	}
	public int getSaleQuantity() {
		return saleQuantity;
	}
	public void setSaleQuantity(int saleQuantity) {
		this.saleQuantity = saleQuantity;
	}
	public Double getSalePrice() {
		if(salePrice != null)
			return salePrice;
		else
			return 0d;
	}
	public void setSalePrice(Double salePrice) {
		this.salePrice = salePrice;
	}
	public int getAdPageNo() {
		return adPageNo;
	}
	public void setAdPageNo(int adPageNo) {
		this.adPageNo = adPageNo;
	}
	public int getAdBlockNo() {
		return blockNo;
	}
	public void setAdBlockNo(int blockNo) {
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
	public Double getConfidenceLevelLower() {
		return confidenceLevelLower;
	}
	public void setConfidenceLevelLower(Double confidenceLevelLower) {
		this.confidenceLevelLower = confidenceLevelLower;
	}
	public Double getConfidenceLevelUpper() {
		return confidenceLevelUpper;
	}
	public void setConfidenceLevelUpper(Double confidenceLevelUpper) {
		this.confidenceLevelUpper = confidenceLevelUpper;
	}
	public Double getPredictedMovementBeforeSubsAdjustment() {
		return predictedMovementBeforeSubsAdjustment;
	}
	public void setPredictedMovementBeforeSubsAdjustment(Double predictedMovementBeforeSubsAdjustment) {
		this.predictedMovementBeforeSubsAdjustment = predictedMovementBeforeSubsAdjustment;
	}
	
	public String getQuestionablePrediction() {
		return questionablePrediction;
	}
	public void setQuestionablePrediction(String questionablePrediction) {
		this.questionablePrediction = questionablePrediction;
	}
	public boolean isCurrentPrice() {
		return isCurrentPrice;
	}
	public void setIsCurrentPrice(boolean isCurrentPrice) {
		this.isCurrentPrice = isCurrentPrice;
	}
	public boolean isNewPricePoint() {
		return isNewPricePoint;
	}
	public void setNewPricePoint(boolean isNewPricePoint) {
		this.isNewPricePoint = isNewPricePoint;
	}
	public boolean isInputPricePoint() {
		return isInputPricePoint;
	}
	public void setInputPricePoint(boolean isInputPricePoint) {
		this.isInputPricePoint = isInputPricePoint;
	}
	
	
	public void copy(PricePointDTO pricePointDTO){
		this.regQuantity = pricePointDTO.regQuantity;
		this.regPrice = pricePointDTO.regPrice;
		this.saleQuantity = pricePointDTO.saleQuantity;
		this.salePrice = pricePointDTO.salePrice;
		this.adPageNo = pricePointDTO.adPageNo;
		this.blockNo = pricePointDTO.blockNo;
		this.displayTypeId = pricePointDTO.displayTypeId;
		this.promoTypeId = pricePointDTO.promoTypeId;
		this.predictedMovement = pricePointDTO.predictedMovement;
		this.predictionStatus = pricePointDTO.predictionStatus;
	}
}
