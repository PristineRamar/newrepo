package com.pristine.dto.offermgmt;

import com.fasterxml.jackson.annotation.JsonIgnore;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.pristine.util.offermgmt.PRFormatHelper;

public class PRGuardRailLog {
	
	
	@JsonProperty("c-id")
	private int compStrId;
	@JsonProperty("m")
	private String multiple;
	@JsonProperty("r-p")
	private String regPrice;
	@JsonProperty("c-d")
	private String checkDate;
	@JsonProperty("p-p")
	private boolean isPricePresent;
	@JsonProperty("m-r-t")
	private String maxRetailText;
	@JsonIgnore
	private PRRange guidelineRange;
	@JsonIgnore
	private PRRange finalRange;
	@JsonProperty("g-r")
	private PRRangeForLog guidelineRangeForLog;
	@JsonProperty("f-r")
	private PRRangeForLog finalRangeForLog;
	@JsonProperty("cr-ig")
	private String compRetailBelowCost;
	
	public PRGuardRailLog(){
		this.maxRetailText = "";
		this.guidelineRange = new PRRange();
		this.finalRange = new PRRange();
		this.guidelineRangeForLog = new PRRangeForLog(); 
		this.finalRangeForLog = new PRRangeForLog();
	}

	public int getCompStrId() {
		return compStrId;
	}

	public void setCompStrId(int compStrId) {
		this.compStrId = compStrId;
	}

	public String getMultiple() {
		return multiple;
	}

	public void setMultiple(String multiple) {
		this.multiple = multiple;
	}

	public String getRegPrice() {
		return regPrice;
	}

	public void setRegPrice(String regPrice) {
		this.regPrice = regPrice;
	}

	public String getCheckDate() {
		return checkDate;
	}

	public void setCheckDate(String checkDate) {
		this.checkDate = checkDate;
	}

	public boolean isPricePresent() {
		return isPricePresent;
	}

	public void setPricePresent(boolean isPricePresent) {
		this.isPricePresent = isPricePresent;
	}

	public String getMaxRetailText() {
		return maxRetailText;
	}

	public void setMaxRetailText(String maxRetailText) {
		this.maxRetailText = maxRetailText;
	}

	public PRRange getGuidelineRange() {
		return guidelineRange;
	}

	public void setGuidelineRange(PRRange guidelineRange) {
		this.guidelineRange = guidelineRange;
	}

	public PRRange getFinalRange() {
		return finalRange;
	}

	public void setFinalRange(PRRange finalRange) {
		this.finalRange = finalRange;
	}

	public PRRangeForLog getGuidelineRangeForLog() {
		PRFormatHelper.formatRangeForLog(this.getGuidelineRange(), this.guidelineRangeForLog);
		return guidelineRangeForLog;
	}

	public void setGuidelineRangeForLog(PRRangeForLog guidelineRangeForLog) {
		this.guidelineRangeForLog = guidelineRangeForLog;
	}

	public PRRangeForLog getFinalRangeForLog() {
		PRFormatHelper.formatRangeForLog(this.getFinalRange(), this.finalRangeForLog);
		return this.finalRangeForLog;
	}


	public void setFinalRangeForLog(PRRangeForLog finalRangeForLog) {
		this.finalRangeForLog = finalRangeForLog;
	}

	public String getCompRetailBelowCost() {
		return compRetailBelowCost;
	}

	public void setCompRetailBelowCost(String compRetailBelowCost) {
		this.compRetailBelowCost = compRetailBelowCost;
	}

}
