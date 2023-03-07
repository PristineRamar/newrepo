package com.pristine.dto.offermgmt.perishable;

public class PerishableCostFeedDTO {

	private int locationType;
	private String locationCode;
	private String retailerItemCode;
	private double cost;
	private String costEffectiveDate;

	public int getLocationType() {
		return locationType;
	}
	public void setLocationType(int locationType) {
		this.locationType = locationType;
	}
	public String getLocationCode() {
		return locationCode;
	}
	public void setLocationCode(String locationCode) {
		this.locationCode = locationCode;
	}

	public String getRetailerItemCode() {
		return retailerItemCode;
	}
	public void setRetailerItemCode(String retailerItemCode) {
		this.retailerItemCode = retailerItemCode;
	}
	public double getCost() {
		return cost;
	}
	public void setCost(double cost) {
		this.cost = cost;
	}

	public String getCostEffectiveDate() {
		return costEffectiveDate;
	}
	public void setCostEffectiveDate(String costEffectiveDate) {
		this.costEffectiveDate = costEffectiveDate;
	}
}
