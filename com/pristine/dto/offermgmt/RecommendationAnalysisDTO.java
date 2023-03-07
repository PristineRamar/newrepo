package com.pristine.dto.offermgmt;

public class RecommendationAnalysisDTO {
	private long runId;
	private boolean hasPredictions;
	private boolean hasAtleastOneRecommendation;
	private boolean hasAtleastOneNewPrice;
	
	public long getRunId() {
		return runId;
	}
	public void setRunId(long runId) {
		this.runId = runId;
	}
	public boolean isHasPredictions() {
		return hasPredictions;
	}
	public void setHasPredictions(boolean hasPredictions) {
		this.hasPredictions = hasPredictions;
	}
	public boolean isHasAtleastOneRecommendation() {
		return hasAtleastOneRecommendation;
	}
	public void setHasAtleastOneRecommendation(boolean hasAtleastOneRecommendation) {
		this.hasAtleastOneRecommendation = hasAtleastOneRecommendation;
	}
	public boolean isHasAtleastOneNewPrice() {
		return hasAtleastOneNewPrice;
	}
	public void setHasAtleastOneNewPrice(boolean hasAtleastOneNewPrice) {
		this.hasAtleastOneNewPrice = hasAtleastOneNewPrice;
	}
}
