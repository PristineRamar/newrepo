package com.pristine.dto.pricingalert;

public class LocationCompetitorMapDTO {
	private int locationCompetitorMapId;
	private int baseLocationLevelId;
	private int baseLocationId;
	private int compLocationLevelId;
	private int compLocationId;
	private int compLocationTypeId;
	private int productLevelId;
	private int productId;
	private int priceCheckListId;
	private int calendarId;
	private String priceIndexEnabled;
	private String changeAnalysisEnabled;
	private String pricingAlertEnabled;
	private String userId;
	private String productName;
	private String weekStartDate;
	private String weekEndDate;
	private String compStrNo;
	private String competitorName;
	private String locationName;
	
	public void setUserId(String userId) {
		this.userId = userId;
	}
	public int getLocationCompetitorMapId() {
		return locationCompetitorMapId;
	}
	public void setLocationCompetitorMapId(int locationCompetitorMapId) {
		this.locationCompetitorMapId = locationCompetitorMapId;
	}
	public int getBaseLocationLevelId() {
		return baseLocationLevelId;
	}
	public void setBaseLocationLevelId(int baseLocationLevelId) {
		this.baseLocationLevelId = baseLocationLevelId;
	}
	public int getBaseLocationId() {
		return baseLocationId;
	}
	public void setBaseLocationId(int baseLocationId) {
		this.baseLocationId = baseLocationId;
	}
	public int getCompLocationLevelId() {
		return compLocationLevelId;
	}
	public void setCompLocationLevelId(int compLocationLevelId) {
		this.compLocationLevelId = compLocationLevelId;
	}
	public int getCompLocationId() {
		return compLocationId;
	}
	public void setCompLocationId(int compLocationId) {
		this.compLocationId = compLocationId;
	}
	public int getCompLocationTypeId() {
		return compLocationTypeId;
	}
	public void setCompLocationTypeId(int compLocationTypeId) {
		this.compLocationTypeId = compLocationTypeId;
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
	public int getPriceCheckListId() {
		return priceCheckListId;
	}
	public void setPriceCheckListId(int priceCheckListId) {
		this.priceCheckListId = priceCheckListId;
	}
	public String getPriceIndexEnabled() {
		return priceIndexEnabled;
	}
	public String getProductName() {
		return productName;
	}
	public void setProductName(String productName) {
		this.productName = productName;
	}
	public void setPriceIndexEnabled(String priceIndexEnabled) {
		this.priceIndexEnabled = priceIndexEnabled;
	}
	public String getChangeAnalysisEnabled() {
		return changeAnalysisEnabled;
	}
	public void setChangeAnalysisEnabled(String changeAnalysisEnabled) {
		this.changeAnalysisEnabled = changeAnalysisEnabled;
	}
	public String getPricingAlertEnabled() {
		return pricingAlertEnabled;
	}
	public void setPricingAlertEnabled(String pricingAlertEnabled) {
		this.pricingAlertEnabled = pricingAlertEnabled;
	}
	public String getUserId() {
		return userId;
	}
	public int getCalendarId() {
		return calendarId;
	}
	public void setCalendarId(int calendarId) {
		this.calendarId = calendarId;
	}
	public String getWeekStartDate() {
		return weekStartDate;
	}
	public void setWeekStartDate(String weekStartDate) {
		this.weekStartDate = weekStartDate;
	}
	public String getWeekEndDate() {
		return weekEndDate;
	}
	public void setWeekEndDate(String weekEndDate) {
		this.weekEndDate = weekEndDate;
	}
	public String getCompStrNo() {
		return compStrNo;
	}
	public void setCompStrNo(String compStrNo) {
		this.compStrNo = compStrNo;
	}
	public String getCompetitorName() {
		return competitorName;
	}
	public void setCompetitorName(String competitorName) {
		this.competitorName = competitorName;
	}
	public String getLocationName() {
		return locationName;
	}
	public void setLocationName(String locationName) {
		this.locationName = locationName;
	}
}
