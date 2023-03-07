package com.pristine.dto.offermgmt.prediction;


import java.text.DecimalFormat;

//import com.pristine.service.offermgmt.DisplayTypeLookup;
//import com.pristine.util.Constants;

public class PredictionDetailDTO {

	DecimalFormat twoDigitFormat = new DecimalFormat("0.00");	 
	//DecimalFormat twoDigitFormat = new DecimalFormat("######.##");
//Test
	private int startCalendarId;
	private int endCalendarId;
	private int locationLevelId;
	private int locationId;
	private long lirOrItemCode;
	private char lirIndicator;
	private int regularQuantity;
	private Double regularPrice;
	private double predictedMovement = 0;
	private double confidenceLevelLower = 0;
	private double confidenceLevelUpper = 0;
	//private long predictedMargin = 0;
	//private long predictedRevenue = 0;	 
	private String explainPrediction = "";
	private boolean isAlreadyPredicted = false;
	private int predictionStatus;
	
	private int saleQuantity;
	private Double salePrice;
	private int adPageNo;
	private int blockNo;
	private int displayTypeId;
	private int promoTypeId;
	private double predictedMovementWithoutSubsEffect = 0;
	
	private String questionablePrediction = "";
	//private Double listCost = null;
	
//	public Double getListCost() {
//		return listCost;
//	}
//
//	public void setListCost(Double listCost) {
//		this.listCost = listCost;
//	}
	
//	public String getFormattedRegPrice() {		
//		return twoDigitFormat.format(regularPrice ) ;
//	}	 

	public int getPredictionStatus() {
		return predictionStatus;
	}

	public void setPredictionStatus(int predictionStatus) {
		this.predictionStatus = predictionStatus;
	}

	public int getEndCalendarId() {
		return endCalendarId;
	}

	public void setEndCalendarId(int endCalendarId) {
		this.endCalendarId = endCalendarId;
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

	public long getLirOrItemCode() {
		return lirOrItemCode;
	}

	public void setLirOrItemCode(long lirOrItemCode) {
		this.lirOrItemCode = lirOrItemCode;
	}

	public char getLirIndicator() {
		return lirIndicator;
	}

	public void setLirIndicator(char lirIndicator) {
		this.lirIndicator = lirIndicator;
	}

	public int getRegularQuantity() {
		return regularQuantity;
	}

	public void setRegularQuantity(int regularQuantity) {
		this.regularQuantity = regularQuantity;
	}

	public Double getRegularPrice() {
		//Double formattedRegPrice = new Double(twoDigitFormat.format(regularPrice));		 
		return regularPrice ;
	}

	public void setRegularPrice(Double regularPrice) {
		Double formattedRegPrice = new Double(twoDigitFormat.format(regularPrice));		 
		this.regularPrice = formattedRegPrice;
	}

	public double getPredictedMovement() {
		return predictedMovement;
	}

	public void setPredictedMovement(double predictedMovement) {
		this.predictedMovement = predictedMovement;
	}

//	public long getPredictedMargin() {
//		return predictedMargin;
//	}
//
//	public void setPredictedMargin(long predictedMargin) {
//		this.predictedMargin = predictedMargin;
//	}
//
//	public long getPredictedRevenue() {
//		return predictedRevenue;
//	}
//
//	public void setPredictedRevenue(long predictedRevenue) {
//		this.predictedRevenue = predictedRevenue;
//	}	 

	public String getExplainPrediction() {
		return explainPrediction;
	}

	public void setExplainPrediction(String explainPrediction) {
		this.explainPrediction = explainPrediction;
	}

	public int getStartCalendarId() {
		return startCalendarId;
	}

	public void setStartCalendarId(int startCalendarId) {
		this.startCalendarId = startCalendarId;
	}

	public boolean getIsAlreadyPredicted() {
		return isAlreadyPredicted;
	}

	public void setIsAlreadyPredicted(boolean isAlreadyPredicted) {
		this.isAlreadyPredicted = isAlreadyPredicted;
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

	public double getConfidenceLevelLower() {
		return confidenceLevelLower;
	}

	public void setConfidenceLevelLower(double confidenceLevelLower) {
		this.confidenceLevelLower = confidenceLevelLower;
	}

	public double getConfidenceLevelUpper() {
		return confidenceLevelUpper;
	}

	public void setConfidenceLevelUpper(double confidenceLevelUpper) {
		this.confidenceLevelUpper = confidenceLevelUpper;
	}

	public double getPredictedMovementWithoutSubsEffect() {
		return predictedMovementWithoutSubsEffect;
	}

	public void setPredictedMovementWithoutSubsEffect(double predictedMovementWithoutSubsEffect) {
		this.predictedMovementWithoutSubsEffect = predictedMovementWithoutSubsEffect;
	}
	
	public String getQuestionablePrediction() {
		return questionablePrediction;
	}

	public void setQuestionablePrediction(String questionablePrediction) {
		this.questionablePrediction = questionablePrediction;
	}
}

