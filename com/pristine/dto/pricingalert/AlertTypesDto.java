package com.pristine.dto.pricingalert;

import com.pristine.util.PropertyManager;

public class AlertTypesDto{
	private int alertTypeId;
	private String name;
	private String displayName;
	private String description;
	private String technicalCode;
	private String baseCurDataRange;
	private String basePreDataRange;
	private String baseFutDataRange;
	private String compCurDataRange;
	private String compPreDataRange;
	private String weeksInMonth;
	
	public int getAlertTypeId() {
		return alertTypeId;
	}
	public void setAlertTypeId(int alertTypeId) {
		this.alertTypeId = alertTypeId;
	}
	public String getName() {
		return name;
	}
	public void setName(String name) {
		this.name = name;
	}
	public String getDisplayName() {
		return displayName;
	}
	public void setDisplayName(String displayName) {
		this.displayName = displayName;
	}
	public String getDescription() {
		return description;
	}
	public void setDescription(String description) {
		this.description = description;
	}
	public String getTechnicalCode() {
		return technicalCode;
	}
	public void setTechnicalCode(String technicalCode) {
		this.technicalCode = technicalCode;
	}
	public String getBaseCurDataRange() {
		return baseCurDataRange;
	}
	public void setBaseCurDataRange(String baseCurDataRange) {
		this.baseCurDataRange = baseCurDataRange;
	}
	public String getBasePreDataRange() {
		return basePreDataRange;
	}
	public void setBasePreDataRange(String basePreDataRange) {
		this.basePreDataRange = basePreDataRange;
	}
	public String getBaseFutDataRange() {
		return baseFutDataRange;
	}
	public void setBaseFutDataRange(String baseFutDataRange) {
		this.baseFutDataRange = baseFutDataRange;
	}
	public String getCompCurDataRange() {
		return compCurDataRange;
	}
	public void setCompCurDataRange(String compCurDataRange) {
		this.compCurDataRange = compCurDataRange;
	}
	public String getCompPreDataRange() {
		return compPreDataRange;
	}
	public void setCompPreDataRange(String compPreDataRange) {
		this.compPreDataRange = compPreDataRange;
	}
	
	public int getWeeksInMonth(){
		if(weeksInMonth == null){
			weeksInMonth = PropertyManager.getProperty("PA_FETCHANDFILTER.WEEKSINMONTH", "4");
		}
		return Integer.parseInt(weeksInMonth);
	}
	
	public int getBasePreDataRangeInDays(){
		int weeksInMonth = getWeeksInMonth();
		int days = 0;
		String range = getBasePreDataRange();
		if(range != null){
			int value = Integer.parseInt(range.substring(0, range.length() - 1));
			char key = range.substring(range.length() - 1).charAt(0);
			
			switch(key){
				case 'd':
					days = value;
					break;
				case 'w':
					days = value * 7;
					break;
				case 'm':
					days = value * weeksInMonth * 7;
					break;
				default:
					days = 0;
			}
		}
		return days;
	}
	
	public int getCompPreDataRangeInDays(){
		int weeksInMonth = getWeeksInMonth();
		int days = 0;
		String range = getCompPreDataRange();
		if(range != null){
			int value = Integer.parseInt(range.substring(0, range.length() - 1));
			char key = range.substring(range.length() - 1).charAt(0);
			
			switch(key){
				case 'd':
					days = value;
					break;
				case 'w':
					days = value * 7;
					break;
				case 'm':
					days = value * weeksInMonth * 7;
					break;
				default:
					days = 0;
			}
		}
		return days;
	}
	
	public int getBaseCurDataRangeInDays(){
		int weeksInMonth = getWeeksInMonth();
		int days = 0;
		String range = getBaseCurDataRange();
		int value = Integer.parseInt(range.substring(0, range.length() - 1));
		char key = range.substring(range.length() - 1).charAt(0);
		
		switch(key){
			case 'd':
				days = value;
				break;
			case 'w':
				days = value * 7;
				break;
			case 'm':
				days = value * weeksInMonth * 7;
				break;
			default:
				days = 0;
		}
		
		return days;
	}
	
	public int getCompCurDataRangeInDays(){
		int weeksInMonth = getWeeksInMonth();
		int days = 0;
		String range = getCompCurDataRange();
		int value = Integer.parseInt(range.substring(0, range.length() - 1));
		char key = range.substring(range.length() - 1).charAt(0);
		
		switch(key){
			case 'd':
				days = value;
				break;
			case 'w':
				days = value * 7;
				break;
			case 'm':
				days = value * weeksInMonth * 7;
				break;
			default:
				days = 0;
		}
		
		return days;
	}
}
