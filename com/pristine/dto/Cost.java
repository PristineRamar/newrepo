package com.pristine.dto;

public class Cost {

	private int itemCode;
	private int lirId;
	private int calendarId;
	private int locationLevelId;
	private int locationId;
	private int costTypeId ;
	private String startDate;
	private String endDate;
	private double cost;
	private String calendarStartDate;
	private String calendarEndDate;
	private double vipCost;
	private double nipoBaseCost=0;
	private double cwagCoreCost=0;
	private double nipoCoreCost=0;
	
	public int getItemCode() {
		return itemCode;
	}
	public void setItemCode(int itemCode) {
		this.itemCode = itemCode;
	}
	public int getCalendarId() {
		return calendarId;
	}
	public void setCalendarId(int calendarId) {
		this.calendarId = calendarId;
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
	public int getCostTypeId() {
		return costTypeId;
	}
	public void setCostTypeId(int costTypeId) {
		this.costTypeId = costTypeId;
	}
	public String getStartDate() {
		return startDate;
	}
	public void setStartDate(String startDate) {
		this.startDate = startDate;
	}
	public String getEndDate() {
		return endDate;
	}
	public void setEndDate(String endDate) {
		this.endDate = endDate;
	}
	public double getCost() {
		return cost;
	}
	public void setCost(double cost) {
		this.cost = cost;
	}
	public int getLirId() {
		return lirId;
	}
	public void setLirId(int lirId) {
		this.lirId = lirId;
	}
	public String getCalendarStartDate() {
		return calendarStartDate;
	}
	public void setCalendarStartDate(String calendarStartDate) {
		this.calendarStartDate = calendarStartDate;
	}
	public String getCalendarEndDate() {
		return calendarEndDate;
	}
	public void setCalendarEndDate(String calendarEndDate) {
		this.calendarEndDate = calendarEndDate;
	}
	public double getVipCost() {
		return vipCost;
	}
	public void setVipCost(double vipCost) {
		this.vipCost = vipCost;
	}
	public double getNipoBaseCost() {
		return nipoBaseCost;
	}
	public void setNipoBaseCost(double nipoBaseCost) {
		this.nipoBaseCost = nipoBaseCost;
	}
	public double getCwagCoreCost() {
		return cwagCoreCost;
	}
	public void setCwagCoreCost(double cwagCoreCost) {
		this.cwagCoreCost = cwagCoreCost;
	}
	public double getNipoCoreCost() {
		return nipoCoreCost;
	}
	public void setNipoCoreCost(double nipoCoreCost) {
		this.nipoCoreCost = nipoCoreCost;
	}
	
}
