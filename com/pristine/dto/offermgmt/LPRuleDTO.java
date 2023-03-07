package com.pristine.dto.offermgmt;

public class LPRuleDTO {
	private double minRange;
	private double maxRange;
	private double priceIndex;

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
	
	public double getPriceIndex() {
		return priceIndex;
	}
	public void setPriceDiff(double priceIndex) {
		this.priceIndex = priceIndex;
	}	
}

