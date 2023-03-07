package com.pristine.service.offermgmt.prediction;

import com.pristine.util.Constants;

public class PredictionEngineItem {
	int locationLevelId; //zone
	int locationId; // zone id
	int dealType;
	int itemCode;
	String upc;
	double regPrice;
	int regQuantity;
	char useSubstFlag = Constants.NO;
	char mainOrImpactFlag  = Constants.NO;
	Boolean usePrediction = null;
	Boolean isAvgMovAlreadySet = null;
	String predictionStartDateStr;
	String predictionPeriodStartDateStr;
	int saleQuantity;
	double salePrice;
	int adPageNo;
	int adBlockNo;
	int displayTypeId;
	int promoTypeId;
	int retLirId;
	
	// NU:: 9th Feb 2017, for substitutes adjustment
	public int subsScenarioId;
	
	// NU:: 9th Feb 2017, for substitutes adjustment, pred and flag is passed, as it will be there while calling substitute
	// adjustment, so that substitute need not call the prediction again
	public double predictedMovement = 0;
	public int predictionStatus = PredictionStatus.PREDICTION_APP_EXCEPTION.getStatusCode();
	
	public PredictionEngineItem(int locationLevelId, int locationId, String predictionStartDateStr, int dealType, int itemCode, String upc,
			double regPrice, int regQuantity, char useSubstFlag, char mainOrImpactFlag, Boolean usePrediction, Boolean isAvgMovAlreadySet,
			String predictionPeriodStartDateStr, int saleQuantity, double salePrice, int adPageNo, int adBlockNo, int promoTypeId, int displayTypeId,
			int subsScenarioId, double predictedMovement, int predictionStatus, int retLirId) {
		super();
		this.locationLevelId = locationLevelId;
		this.locationId = locationId;
		this.predictionStartDateStr = predictionStartDateStr;
		this.predictionPeriodStartDateStr = predictionPeriodStartDateStr;
		this.dealType = dealType;
		this.itemCode = itemCode;
		this.upc = upc;
		this.regPrice = regPrice;
		this.regQuantity = regQuantity;
		this.useSubstFlag = useSubstFlag;
		this.mainOrImpactFlag = mainOrImpactFlag;
		this.usePrediction = usePrediction;
		this.isAvgMovAlreadySet = isAvgMovAlreadySet;
		this.saleQuantity = saleQuantity;
		this.salePrice = salePrice;
		this.adPageNo = adPageNo;
		this.adBlockNo = adBlockNo;
		this.displayTypeId = displayTypeId;
		this.promoTypeId = promoTypeId;
		this.subsScenarioId = subsScenarioId;
		this.predictedMovement = predictedMovement;
		this.predictionStatus = predictionStatus;
		this.retLirId = retLirId;
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

	public int getDealType() {
		return dealType;
	}

	public void setDealType(int dealType) {
		this.dealType = dealType;
	}

	public int getItemCode() {
		return itemCode;
	}

	public void setItemCode(int itemCode) {
		this.itemCode = itemCode;
	}

	public String getUpc() {
		return upc;
	}

	public void setUpc(String upc) {
		this.upc = upc;
	}

	public double getRegPrice() {
		return regPrice;
	}

	public void setRegPrice(double regPrice) {
		this.regPrice = regPrice;
	}

	public int getRegQuantity() {
		return regQuantity;
	}

	public void setRegQuantity(int regQuantity) {
		this.regQuantity = regQuantity;
	}

	public char getUseSubstFlag() {
		return useSubstFlag;
	}

	public void setUseSubstFlag(char useSubstFlag) {
		this.useSubstFlag = useSubstFlag;
	}

	public char getMainOrImpactFlag() {
		return mainOrImpactFlag;
	}

	public void setMainOrImpactFlag(char mainOrImpactFlag) {
		this.mainOrImpactFlag = mainOrImpactFlag;
	}

	public Boolean getUsePrediction() {
		return usePrediction;
	}

	public void setUsePrediction(Boolean usePrediction) {
		this.usePrediction = usePrediction;
	}

	public Boolean getIsAvgMovAlreadySet() {
		return isAvgMovAlreadySet;
	}

	public void setIsAvgMovAlreadySet(Boolean isAvgMovAlreadySet) {
		this.isAvgMovAlreadySet = isAvgMovAlreadySet;
	}

	public String getPredictionStartDateStr() {
		return predictionStartDateStr;
	}

	public void setPredictionStartDateStr(String predictionStartDateStr) {
		this.predictionStartDateStr = predictionStartDateStr;
	}

	public String getPredictionPeriodStartDateStr() {
		return predictionPeriodStartDateStr;
	}

	public void setPredictionPeriodStartDateStr(String predictionPeriodStartDateStr) {
		this.predictionPeriodStartDateStr = predictionPeriodStartDateStr;
	}

	public int getSaleQuantity() {
		return saleQuantity;
	}

	public void setSaleQuantity(int saleQuantity) {
		this.saleQuantity = saleQuantity;
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

	public int getRetLirId() {
		return retLirId;
	}

	public void setRetLirId(int retLirId) {
		this.retLirId = retLirId;
	}

	public int getSubsScenarioId() {
		return subsScenarioId;
	}

	public void setSubsScenarioId(int subsScenarioId) {
		this.subsScenarioId = subsScenarioId;
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
