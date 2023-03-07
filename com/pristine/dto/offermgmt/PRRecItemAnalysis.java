package com.pristine.dto.offermgmt;

public class PRRecItemAnalysis {
	private long recommendationId;
	private int isForecastUpRetailUp;
	private int isForecastDownRetailDown;
	private int isHigherThanXWeeksAvg;
	private int isLowerThanXWeeksAvg;
	private int isLowerThanMinSoldInXWeeks;
	private int isHigherThanMaxSoldInXWeeks;
	private boolean isPassesAtleastOneCase;
	
	public long getRecommendationId() {
		return recommendationId;
	}
	public void setRecommendationId(long recommendationId) {
		this.recommendationId = recommendationId;
	}
	public int getIsForecastUpRetailUp() {
		return isForecastUpRetailUp;
	}
	public void setIsForecastUpRetailUp(int isForecastUpRetailUp) {
		this.isForecastUpRetailUp = isForecastUpRetailUp;
	}
	public int getIsForecastDownRetailDown() {
		return isForecastDownRetailDown;
	}
	public void setIsForecastDownRetailDown(int isForecastDownRetailDown) {
		this.isForecastDownRetailDown = isForecastDownRetailDown;
	}
	public int getIsHigherThanXWeeksAvg() {
		return isHigherThanXWeeksAvg;
	}
	public void setIsHigherThanXWeeksAvg(int isHigherThanXWeeksAvg) {
		this.isHigherThanXWeeksAvg = isHigherThanXWeeksAvg;
	}
	public int getIsLowerThanXWeeksAvg() {
		return isLowerThanXWeeksAvg;
	}
	public void setIsLowerThanXWeeksAvg(int isLowerThanXWeeksAvg) {
		this.isLowerThanXWeeksAvg = isLowerThanXWeeksAvg;
	}
	public int getIsLowerThanMinSoldInXWeeks() {
		return isLowerThanMinSoldInXWeeks;
	}
	public void setIsLowerThanMinSoldInXWeeks(int isLowerThanMinSoldInXWeeks) {
		this.isLowerThanMinSoldInXWeeks = isLowerThanMinSoldInXWeeks;
	}
	public int getIsHigherThanMaxSoldInXWeeks() {
		return isHigherThanMaxSoldInXWeeks;
	}
	public void setIsHigherThanMaxSoldInXWeeks(int isHigherThanMaxSoldInXWeeks) {
		this.isHigherThanMaxSoldInXWeeks = isHigherThanMaxSoldInXWeeks;
	}
	public boolean isPassesAtleastOneCase() {
		return isPassesAtleastOneCase;
	}
	public void setPassesAtleastOneCase(boolean isPassesAtleaseOneCase) {
		this.isPassesAtleastOneCase = isPassesAtleaseOneCase;
	}
}
