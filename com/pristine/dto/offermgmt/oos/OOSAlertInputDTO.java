package com.pristine.dto.offermgmt.oos;

import java.util.List;

public class OOSAlertInputDTO {
	private List<Integer> locationList;
	private int dayPartId;
	private int dayPartExecOrder;
	private int calendarId;
	private int locationLevelId;
	public List<Integer> getLocationList() {
		return locationList;
	}
	public void setLocationList(List<Integer> locationList) {
		this.locationList = locationList;
	}
	public int getDayPartId() {
		return dayPartId;
	}
	public void setDayPartId(int dayPartId) {
		this.dayPartId = dayPartId;
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
	public int getDayPartExecOrder() {
		return dayPartExecOrder;
	}
	public void setDayPartExecOrder(int dayPartExecOrder) {
		this.dayPartExecOrder = dayPartExecOrder;
	}
}
