package com.pristine.dto.offermgmt;

public class NLPRuleDTO {
	private double minRange;
	private double maxRange;
	private double priceDiff;

	public double getMinRange() {
		return minRange;
	}
	public void setMinRange(double minRange) {
		this.minRange = minRange;
	}

	public double getMaxRange() {
		return maxRange;
	}
	public void setMaxRange(double maxRange) {
		this.maxRange = maxRange;
	}	
	
	public double getPriceDiff() {
		return priceDiff;
	}
	public void setPriceDiff(double priceDiff) {
		this.priceDiff = priceDiff;
	}	
}

