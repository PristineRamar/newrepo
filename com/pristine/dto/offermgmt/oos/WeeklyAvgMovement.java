package com.pristine.dto.offermgmt.oos;

public class WeeklyAvgMovement {
	private int locationLevelId;
	private int locationId;
	private int productLevelId;
	private int productId;
	private int dayId;
	private int dayPartId;
	private double averageMovement;
	
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
	public int getDayId() {
		return dayId;
	}
	public void setDayId(int dayId) {
		this.dayId = dayId;
	}
	public int getDayPartId() {
		return dayPartId;
	}
	public void setDayPartId(int dayPartId) {
		this.dayPartId = dayPartId;
	}
	public double getAverageMovement() {
		return averageMovement;
	}
	public void setAverageMovement(double averageMovement) {
		this.averageMovement = averageMovement;
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
}
