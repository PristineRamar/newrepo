package com.pristine.dto.offermgmt;

public class PRRoundingTableDTO {
	private Double startPrice;
	private Double endPrice;
	private String allowedEndDigits;
	private String allowedPrices;
	private String excludedPrices;
	
	public Double getStartPrice() {
		return startPrice;
	}
	public void setStartPrice(Double startPrice) {
		this.startPrice = startPrice;
	}
	public Double getEndPrice() {
		return endPrice;
	}
	public void setEndPrice(Double endPrice) {
		this.endPrice = endPrice;
	}
	public String getAllowedEndDigits() {
		return allowedEndDigits;
	}
	public void setAllowedEndDigits(String allowedEndDigits) {
		this.allowedEndDigits = allowedEndDigits;
	}
	public String getAllowedPrices() {
		return allowedPrices;
	}
	public void setAllowedPrices(String allowedPrices) {
		this.allowedPrices = allowedPrices;
	}
	public String getExcludedPrices() {
		return excludedPrices;
	}
	public void setExcludedPrices(String excludedPrices) {
		this.excludedPrices = excludedPrices;
	}
}
