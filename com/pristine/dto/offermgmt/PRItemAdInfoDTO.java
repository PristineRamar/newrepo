package com.pristine.dto.offermgmt;

import java.io.Serializable;

public class PRItemAdInfoDTO implements Serializable{
	/**
	 * 
	 */
	private static final long serialVersionUID = 5998289761697193363L;
	private String weeklyAdStartDate;
	private int adPageNo;
	private int adBlockNo;
	
	public String getWeeklyAdStartDate() {
		return weeklyAdStartDate;
	}
	public void setWeeklyAdStartDate(String weeklyAdStartDate) {
		this.weeklyAdStartDate = weeklyAdStartDate;
	}
	public int getAdPageNo() {
		return adPageNo;
	}
	public void setAdPageNo(int adPageNo) {
		this.adPageNo = adPageNo;
	}
	public int getAdBlockNo() {
		return adBlockNo;
	}
	public void setAdBlockNo(int adBlockNo) {
		this.adBlockNo = adBlockNo;
	}
}
