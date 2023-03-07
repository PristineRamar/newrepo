package com.pristine.dto.offermgmt;

public class PRRecommendation {

	private int itemCode;
	private double avgMovement = 0;
	private long runId = 0;
	private int currentRegmultiple;
	private Double currentRegPrice = null;
	private Integer recRegMultiple = null;
	private Double recRegPrice = null;
	private Double predictedMovement;
	private int predictionStatus;
	private int priceRecommended;
	private MultiplePrice recRegPriceObj = null;
	
	public double getAvgMovement() {
		return avgMovement;
	}

	public void setAvgMovement(double avgMovement) {
		this.avgMovement = avgMovement;
	}

	public int getItemCode() {
		return itemCode;
	}

	public void setItemCode(int itemCode) {
		this.itemCode = itemCode;
	}
	
	public int getCurrentRegmultiple() {
		return currentRegmultiple;
	}

	public void setCurrentRegmultiple(int currentRegmultiple) {
		this.currentRegmultiple = currentRegmultiple;
	}

	public Double getCurrentRegPrice() {
		return currentRegPrice;
	}

	public void setCurrentRegPrice(Double currentRegPrice) {
		this.currentRegPrice = currentRegPrice;
	}

	public Integer getRecRegMultiple() {
		return recRegMultiple;
	}

	public void setRecRegMultiple(Integer recRegMultiple) {
		this.recRegMultiple = recRegMultiple;
	}

	public Double getRecRegPrice() {
		return recRegPrice;
	}

	public void setRecRegPrice(Double recRegPrice) {
		this.recRegPrice = recRegPrice;
	}

	public Double getPredictedMovement() {
		return predictedMovement;
	}

	public void setPredictedMovement(Double predictedMovement) {
		this.predictedMovement = predictedMovement;
	}

	public int getPredictionStatus() {
		return predictionStatus;
	}

	public void setPredictionStatus(int predictionStatus) {
		this.predictionStatus = predictionStatus;
	}

	public int getPriceRecommended() {
		return priceRecommended;
	}

	public void setPriceRecommended(int priceRecommended) {
		this.priceRecommended = priceRecommended;
	}
	
	public long getRunId() {
		return runId;
	}

	public void setRunId(long runId) {
		this.runId = runId;
	}

	public MultiplePrice getRecRegPriceObj() {
		return recRegPriceObj;
	}

	public void setRecRegPriceObj(MultiplePrice recRegPriceObj) {
		this.recRegPriceObj = recRegPriceObj;
	}
}
