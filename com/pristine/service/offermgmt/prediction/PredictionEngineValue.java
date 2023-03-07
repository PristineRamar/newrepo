package com.pristine.service.offermgmt.prediction;

public class PredictionEngineValue {
	public double predictedMovement;
	public PredictionStatus predictionStatus;
	public String minImpact = "";
	public String maxImpact = "";
	public double confidenceLevelLower;
	public double confidenceLevelUpper;
	public double predictedMovementBeforeSubsAdjustment;
	
	public PredictionEngineValue(double predictedMovement,
			PredictionStatus predictionStatus, String minImpact, String maxImpact,
			double confidenceLevelLower, double confidenceLevelUpper, double predictedMovementBeforeSubsAdjustment) {
		super();
		this.predictedMovement = predictedMovement;
		this.predictionStatus = predictionStatus;
		this.minImpact = minImpact;
		this.maxImpact = maxImpact;
		this.confidenceLevelLower = confidenceLevelLower;
		this.confidenceLevelUpper = confidenceLevelUpper;
		this.predictedMovementBeforeSubsAdjustment = predictedMovementBeforeSubsAdjustment;
	}	
}
