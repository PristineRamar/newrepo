package com.pristine.dto;

import java.time.LocalDate;

import com.pristine.util.Constants;
import com.pristine.util.DateUtil;

public class RetailCalendarDTO {
	
	private int     _calendarId;
	private String  _startDate;
	private String  _endDate;
	private String  _calendarMode; 
	private int     _lstCalendarId;
	private int _actualNo;
	private int _calYear;
	
	private int _weekNo;
	private int _specialDayId;
	private String _rowType;
 
	public int getCalendarId() {
		return _calendarId;
	}
	public void setCalendarId(int v) {
		this._calendarId = v;
	}

	public String getStartDate() {
		return _startDate;
	}
	public void setStartDate(String v) {
		this._startDate = v;
	}

	public String getEndDate() {
		return _endDate;
	}
	public void setEndDate(String v) {
		this._endDate = v;
	}
	public String getcalendarMode() {
		return _calendarMode;
	}
	public void setcalendarMode(String _calendarMode) {
		this._calendarMode = _calendarMode;
	}
	public void setlstCalendarId(int _lstCalendarId) {
		this._lstCalendarId = _lstCalendarId;
	}
	public int getlstCalendarId() {
		return _lstCalendarId;
	}
	
	public int getActualNo() {
		return _actualNo;
	}
	public void setActualNo(int _actualNo) {
		this._actualNo = _actualNo;
	}
	public int getCalYear() {
		return _calYear;
	}
	public void setCalYear(int _calYear) {
		this._calYear = _calYear;
	}
	public int getWeekNo() {
		return _weekNo;
	}
	public void setWeekNo(int _weekNo) {
		this._weekNo = _weekNo;
	}
	public int getSpecialDayId() {
		return _specialDayId;
	}
	public void setSpecialDayId(int _specialDayId) {
		this._specialDayId = _specialDayId;
	}
	public String getRowType() {
		return _rowType;
	}
	public void setRowType(String _rowType) {
		this._rowType = _rowType;
	}
	@Override
	public String toString() {
		return "RetailCalendarDTO [_calendarId=" + _calendarId + ", _startDate=" + _startDate + ", _endDate=" + _endDate + ", _calendarMode="
				+ _calendarMode + ", _lstCalendarId=" + _lstCalendarId + ", _actualNo=" + _actualNo + ", _calYear=" + _calYear + ", _weekNo="
				+ _weekNo + ", _specialDayId=" + _specialDayId + ", _rowType=" + _rowType + "]";
	}
	
	public LocalDate getStartDateAsDate() {
		return DateUtil.stringToLocalDate(this.getStartDate(), Constants.APP_DATE_FORMAT);
	}

	public LocalDate getEndDateAsDate() {
		return DateUtil.stringToLocalDate(this.getEndDate(), Constants.APP_DATE_FORMAT);
	}
	
	public RetailCalendarDTO(int _calendarId, String _startDate, String _endDate, int _lstCalendarId) {
		super();
		this._calendarId = _calendarId;
		this._startDate = _startDate;
		this._endDate = _endDate;
		this._lstCalendarId = _lstCalendarId;
	}
	public RetailCalendarDTO() {
		super();
		// TODO Auto-generated constructor stub
	}
}
