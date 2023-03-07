package com.pristine.dto.offermgmt;

import java.io.Serializable;

import com.pristine.service.offermgmt.DisplayTypeLookup;

public class PRItemDisplayInfoDTO implements Serializable{
	/**
	 * 
	 */
	private static final long serialVersionUID = 8244168537005826545L;
	private String displayWeekStartDate;
	private DisplayTypeLookup displayTypeLookup;
	
	public String getDisplayWeekStartDate() {
		return displayWeekStartDate;
	}
	public void setDisplayWeekStartDate(String displayWeekStartDate) {
		this.displayWeekStartDate = displayWeekStartDate;
	}
	public DisplayTypeLookup getDisplayTypeLookup() {
		return displayTypeLookup;
	}
	public void setDisplayTypeLookup(DisplayTypeLookup displayTypeLookup) {
		this.displayTypeLookup = displayTypeLookup;
	}
}
