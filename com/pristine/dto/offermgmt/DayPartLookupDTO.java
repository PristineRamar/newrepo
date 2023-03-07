package com.pristine.dto.offermgmt;

public class DayPartLookupDTO {
	private int dayPartId;
	private String startTime;
	private String endTime;
	private int order;
	private boolean isSlotSpanDays;
	private boolean isDayPartSpanPrevDay;
	public boolean isDayPartSpanPrevDay() {
		return isDayPartSpanPrevDay;
	}
	public void setDayPartSpanPrevDay(boolean isDayPartSpanPrevDay) {
		this.isDayPartSpanPrevDay = isDayPartSpanPrevDay;
	}
	public boolean isSlotSpanDays() {
		return isSlotSpanDays;
	}
	public void setSlotSpanDays(boolean isMultiSlot) {
		this.isSlotSpanDays = isMultiSlot;
	}
	public int getDayPartId() {
		return dayPartId;
	}
	public void setDayPartId(int dayPartId) {
		this.dayPartId = dayPartId;
	}
	public String getStartTime() {
		return startTime;
	}
	public void setStartTime(String startTime) {
		this.startTime = startTime;
	}
	public String getEndTime() {
		return endTime;
	}
	public void setEndTime(String endTime) {
		this.endTime = endTime;
	}
	public int getOrder() {
		return order;
	}
	public void setOrder(int order) {
		this.order = order;
	}
}