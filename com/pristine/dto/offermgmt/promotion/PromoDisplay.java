package com.pristine.dto.offermgmt.promotion;

public class PromoDisplay {
	private long promoDefId;
	private int locationLevelId;
	private int locationId;
	private int displayTypeId;
	private int subDisplayTypeId;
	private int calendarId;
	public int getCalendarId() {
		return calendarId;
	}
	public void setCalendarId(int calendarId) {
		this.calendarId = calendarId;
	}
	public long getPromoDefId() {
		return promoDefId;
	}
	public void setPromoDefId(long promoDefId) {
		this.promoDefId = promoDefId;
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
	public int getDisplayTypeId() {
		return displayTypeId;
	}
	public void setDisplayTypeId(int displayTypeId) {
		this.displayTypeId = displayTypeId;
	}
	public int getSubDisplayTypeId() {
		return subDisplayTypeId;
	}
	public void setSubDisplayTypeId(int subDisplayTypeId) {
		this.subDisplayTypeId = subDisplayTypeId;
	}
	
}
