package com.pristine.dto;

public class SummaryGoalDTO {
	public String getWeekEndDate () { return _weekEndDate; }
	public void setWeekEndDate(String v) { _weekEndDate = v; }
	private String _weekEndDate = null;
	
	public int getLevelTypeID () { return _levelTypeID; }
	public void setLevelTypeID(int v) { _levelTypeID = v; }
	private int _levelTypeID = -1;
	
	public int getLevelId () { return _levelId; }
	public void setLevelId(int v) { _levelId = v; }
	private int _levelId = 0;
	
	public double getBudget () { return _budget; }
	public void setBudget(double v) { _budget = v; }
	private double _budget = 0;	
	
	public double getForecast () { return _forecast; }
	public void setForecast(double v) { _forecast = v; }
	private double _forecast = 0;
}
