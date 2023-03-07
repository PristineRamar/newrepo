package com.pristine.dto;

import com.fasterxml.jackson.annotation.JsonIgnore;
import com.pristine.dto.offermgmt.MultiplePrice;

public class Price {

	private int itemCode;
	private int lirId;
	private int calendarId;
	private int locationLevelId;
	private int locationId;
	private int priceTypeId;
	private double price;
	private int priceQty;
	private String startDate;
	private String endDate;
	private String calendarStartDate;
	private String calendarEndDate;
	private double coreRetail = 0;
	private double vdpRetail = 0;

	public int getItemCode() {
		return itemCode;
	}

	public void setItemCode(int itemCode) {
		this.itemCode = itemCode;
	}

	public int getLirId() {
		return lirId;
	}

	public void setLirId(int lirId) {
		this.lirId = lirId;
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

	public int getPriceTypeId() {
		return priceTypeId;
	}

	public void setPriceTypeId(int priceTypeId) {
		this.priceTypeId = priceTypeId;
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

	public double getPrice() {
		return price;
	}

	public void setPrice(double price) {
		this.price = price;
	}

	public int getPriceQty() {
		return priceQty;
	}

	public void setPriceQty(int priceQty) {
		this.priceQty = priceQty;
	}

	@JsonIgnore
	public MultiplePrice getMultiPrice() {
		return new MultiplePrice(this.priceQty, this.price);
	}

	public double getCoreRetail() {
		return coreRetail;
	}

	public void setCoreRetail(double coreRetail) {
		this.coreRetail = coreRetail;
	}

	public double getVdpRetail() {
		return vdpRetail;
	}

	public void setVdpRetail(double vdpRetail) {
		this.vdpRetail = vdpRetail;
	}

}
