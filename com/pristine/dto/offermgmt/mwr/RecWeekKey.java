package com.pristine.dto.offermgmt.mwr;

import java.time.LocalDate;

import com.pristine.util.Constants;
import com.pristine.util.DateUtil;

public class RecWeekKey {

	public int calendarId;
	public String weekStartDate;
	public String recWeekType;
	public RecWeekKey(int calendarId, String weekStartDate) {
		this.calendarId = calendarId;
		this.weekStartDate = weekStartDate;
	}
	
	@Override
	public int hashCode() {
		final int prime = 31;
		int result = 1;
		result = prime * result + calendarId;
		result = prime * result + ((weekStartDate == null) ? 0 : weekStartDate.hashCode());
		return result;
	}

	@Override
	public boolean equals(Object obj) {
		if (this == obj)
			return true;
		if (obj == null)
			return false;
		if (getClass() != obj.getClass())
			return false;
		RecWeekKey other = (RecWeekKey) obj;
		if (calendarId != other.calendarId)
			return false;
		if (weekStartDate == null) {
			if (other.weekStartDate != null)
				return false;
		} else if (!weekStartDate.equals(other.weekStartDate))
			return false;
		return true;
	}

	public LocalDate getStartDateAsDate() {
		return DateUtil.stringToLocalDate(this.weekStartDate, Constants.APP_DATE_FORMAT);
	}
	
}
