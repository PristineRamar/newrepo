package com.pristine.dto;

public class SummaryDailyDTO extends SummaryWeeklyDTO
{
	public int getDayOfWeekId () { return _DayOfWeekId; }
	public void setDayOfWeekId(int v) { _DayOfWeekId = v; }
	private int _DayOfWeekId = -1;

	public int getTimeOfDayId () { return _TimeOfDayId; }
	public void setTimeOfDayId(int v) { _TimeOfDayId = v; }
	private int _TimeOfDayId = -1;
}
