package com.pristine.dto.offermgmt.mwr;

import com.fasterxml.jackson.annotation.JsonIgnore;

public class CLPDLPPredictionDTO implements Cloneable {

	
	private int scenarioId;
	private int locationLevelId;
	private int locationId;
	private int productLevelId;
	private int productId;
	private String startDateInput;
	private double avgMinPrice;
	private double avgPromoPrice;
	private int totalPromoItems;
	private int totalAdItems;
	private int totalDisplayItems;
	private int totalItemsOnFirstPage;
	private int totalItemsOnMiddlePage;
	private int totalItemsOnLastPage;
	private int totalItemsOnGatePage;
	private int totalItemsOnFastwall;
	private int totalItemsOnEnd;
	private int totalItemsOnShipper;
	private int totalItemsOnLobby;
	private int totalItemsOnWing;
	private double predictedRevenue;
	private double predictedMovement;
	private double predictedMargin;
	private int predictionStatus;
	private double avgCost;
	private double predictedsellingPrice;
	private double predictedVisitCount;
	private double predictedHHCount;
	@JsonIgnore
	private int calendarId;
	@JsonIgnore
	private int scenarioTypeId;
	
	public int getScenarioId() {
		return scenarioId;
	}
	public void setScenarioId(int scenarioId) {
		this.scenarioId = scenarioId;
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
	public String getStartDateInput() {
		return startDateInput;
	}
	public void setStartDateInput(String startDateInput) {
		this.startDateInput = startDateInput;
	}
	public double getAvgMinPrice() {
		return avgMinPrice;
	}
	public void setAvgMinPrice(double avgMinPrice) {
		this.avgMinPrice = avgMinPrice;
	}
	public double getAvgPromoPrice() {
		return avgPromoPrice;
	}
	public void setAvgPromoPrice(double avgPromoPrice) {
		this.avgPromoPrice = avgPromoPrice;
	}
	public int getTotalPromoItems() {
		return totalPromoItems;
	}
	public void setTotalPromoItems(int totalPromoItems) {
		this.totalPromoItems = totalPromoItems;
	}
	public int getTotalAdItems() {
		return totalAdItems;
	}
	public void setTotalAdItems(int totalAdItems) {
		this.totalAdItems = totalAdItems;
	}
	public int getTotalDisplayItems() {
		return totalDisplayItems;
	}
	public void setTotalDisplayItems(int totalDisplayItems) {
		this.totalDisplayItems = totalDisplayItems;
	}
	public int getTotalItemsOnFirstPage() {
		return totalItemsOnFirstPage;
	}
	public void setTotalItemsOnFirstPage(int totalItemsOnFirstPage) {
		this.totalItemsOnFirstPage = totalItemsOnFirstPage;
	}
	public int getTotalItemsOnMiddlePage() {
		return totalItemsOnMiddlePage;
	}
	public void setTotalItemsOnMiddlePage(int totalItemsOnMiddlePage) {
		this.totalItemsOnMiddlePage = totalItemsOnMiddlePage;
	}
	public int getTotalItemsOnLastPage() {
		return totalItemsOnLastPage;
	}
	public void setTotalItemsOnLastPage(int totalItemsOnLastPage) {
		this.totalItemsOnLastPage = totalItemsOnLastPage;
	}
	public int getTotalItemsOnGatePage() {
		return totalItemsOnGatePage;
	}
	public void setTotalItemsOnGatePage(int totalItemsOnGatePage) {
		this.totalItemsOnGatePage = totalItemsOnGatePage;
	}
	public int getTotalItemsOnFastwall() {
		return totalItemsOnFastwall;
	}
	public void setTotalItemsOnFastwall(int totalItemsOnFastwall) {
		this.totalItemsOnFastwall = totalItemsOnFastwall;
	}
	public int getTotalItemsOnEnd() {
		return totalItemsOnEnd;
	}
	public void setTotalItemsOnEnd(int totalItemsOnEnd) {
		this.totalItemsOnEnd = totalItemsOnEnd;
	}
	public int getTotalItemsOnShipper() {
		return totalItemsOnShipper;
	}
	public void setTotalItemsOnShipper(int totalItemsOnShipper) {
		this.totalItemsOnShipper = totalItemsOnShipper;
	}
	public int getTotalItemsOnLobby() {
		return totalItemsOnLobby;
	}
	public void setTotalItemsOnLobby(int totalItemsOnLobby) {
		this.totalItemsOnLobby = totalItemsOnLobby;
	}
	public int getTotalItemsOnWing() {
		return totalItemsOnWing;
	}
	public void setTotalItemsOnWing(int totalItemsOnWing) {
		this.totalItemsOnWing = totalItemsOnWing;
	}
	public double getPredictedRevenue() {
		return predictedRevenue;
	}
	public void setPredictedRevenue(double predictedRevenue) {
		this.predictedRevenue = predictedRevenue;
	}
	public double getPredictedMovement() {
		return predictedMovement;
	}
	public void setPredictedMovement(double predictedMovement) {
		this.predictedMovement = predictedMovement;
	}
	public double getPredictedMargin() {
		return predictedMargin;
	}
	public void setPredictedMargin(double predictedMargin) {
		this.predictedMargin = predictedMargin;
	}
	public int getPredictionStatus() {
		return predictionStatus;
	}
	public void setPredictionStatus(int predictionStatus) {
		this.predictionStatus = predictionStatus;
	}

	
	public int getCalendarId() {
		return calendarId;
	}
	public void setCalendarId(int calendarId) {
		this.calendarId = calendarId;
	}
	@Override
    public Object clone() throws CloneNotSupportedException {
		return (CLPDLPPredictionDTO) super.clone();
	}
	
	@Override
	public String toString() {
		return "CLPDLPPredictionDTO [scenarioId=" + scenarioId + ", locationLevelId=" + locationLevelId
				+ ", locationId=" + locationId + ", productLevelId=" + productLevelId + ", productId=" + productId
				+ ", startDateInput=" + startDateInput + ", avgMinPrice=" + avgMinPrice + ", avgPromoPrice="
				+ avgPromoPrice + ", totalPromoItems=" + totalPromoItems + ", totalAdItems=" + totalAdItems
				+ ", totalDisplayItems=" + totalDisplayItems + ", totalItemsOnFirstPage=" + totalItemsOnFirstPage
				+ ", totalItemsOnMiddlePage=" + totalItemsOnMiddlePage + ", totalItemsOnLastPage="
				+ totalItemsOnLastPage + ", totalItemsOnGatePage=" + totalItemsOnGatePage + ", totalItemsOnFastwall="
				+ totalItemsOnFastwall + ", totalItemsOnEnd=" + totalItemsOnEnd + ", totalItemsOnShipper="
				+ totalItemsOnShipper + ", totalItemsOnLobby=" + totalItemsOnLobby + ", totalItemsOnWing="
				+ totalItemsOnWing + ", predictedRevenue=" + predictedRevenue + ", predictedMovement="
				+ predictedMovement + ", predictedMargin=" + predictedMargin + ", predictionStatus=" + predictionStatus
				+ "]";
	}
	public int getScenarioTypeId() {
		return scenarioTypeId;
	}
	public void setScenarioTypeId(int scenarioTypeId) {
		this.scenarioTypeId = scenarioTypeId;
	}
	public double getAvgCost() {
		return avgCost;
	}
	public void setAvgCost(double avgCost) {
		this.avgCost = avgCost;
	}
	public double getPredictedsellingPrice() {
		return predictedsellingPrice;
	}
	public void setPredictedsellingPrice(double predictedsellingPrice) {
		this.predictedsellingPrice = predictedsellingPrice;
	}
	public double getPredictedVisitCount() {
		return predictedVisitCount;
	}
	public void setPredictedVisitCount(double predictedVisitCount) {
		this.predictedVisitCount = predictedVisitCount;
	}
	public double getPredictedHHCount() {
		return predictedHHCount;
	}
	public void setPredictedHHCount(double predictedHHCount) {
		this.predictedHHCount = predictedHHCount;
	}
	
}
