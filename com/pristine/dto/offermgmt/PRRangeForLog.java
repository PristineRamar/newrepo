package com.pristine.dto.offermgmt;

import com.fasterxml.jackson.annotation.JsonProperty;

public class PRRangeForLog {
	@JsonProperty("s-v")
	private String startVal = "";
	
	@JsonProperty("e-v")
	private String endVal = "";
	
	public String getStartVal() {
		return startVal;
	}
	public void setStartVal(String startVal) {
		this.startVal = startVal;
	}
	public String getEndVal() {
		return endVal;
	}
	public void setEndVal(String endVal) {
		this.endVal = endVal;
	}
	
	
}
