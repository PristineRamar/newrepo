package com.pristine.dto.salesanalysis;

public class PeriodCalendarDTO {
	private int _periodCalendarId;
	private String _periodStartDate;
	private String _periodEndDate;
	private int _actualNo;
	private String _calendarMode;
	private int _lastPeriodCalendarId;
	private int _dayCalendarId;
	private String _processingDate;
	
	
	public int getPeriodCalendarId() {
		return _periodCalendarId;
	}
	public void setPeriodCalendarId(int v) {
		this._periodCalendarId = v;
	}

	public String getCalendarMode() {
		return _calendarMode;
	}
	public void setCalendarMode(String v) {
		this._calendarMode = v;
	}
	
	public int getActualNo() {
		return _actualNo;
	}

	public void setActualNo(int v) {
		this._actualNo = v;
	}
	
	public void setPeriodStartDate(String v) {
		this._periodStartDate = v;
	}
	
	public String getPeriodStartDate() {
		return _periodStartDate;
	}

	public void setPeriodEndDate(String v) {
		this._periodEndDate = v;
	}

	public String getPeriodEndDate() {
		return _periodEndDate;
	}
	
	public int getLastPeriodCalendarId() {
		return _lastPeriodCalendarId;
	}
	public void setLastPeriodCalendarId(int v) {
		this._lastPeriodCalendarId = v;
	}
	
	public int getDayCalendarId() {
		return _dayCalendarId;
	}
	public void setDayCalendarId(int v) {
		this._dayCalendarId = v;
	}
	
	public String getProcessingDate() {
		return _processingDate;
	}
	public void setProcessingDate(String v) {
		this._processingDate = v;
	}
}
