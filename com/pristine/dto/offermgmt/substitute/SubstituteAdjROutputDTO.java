package com.pristine.dto.offermgmt.substitute;

import com.pristine.service.offermgmt.ItemKey;

public class SubstituteAdjROutputDTO {
	private ItemKey itemKey;
	private Double adjRegMov;
	private Double adjSaleMov;
	
	public ItemKey getItemKey() {
		return itemKey;
	}
	public void setItemKey(ItemKey itemKey) {
		this.itemKey = itemKey;
	}
	public Double getAdjRegMov() {
		return adjRegMov;
	}
	public void setAdjRegMov(Double adjRegMov) {
		this.adjRegMov = adjRegMov;
	}
	public Double getAdjSaleMov() {
		return adjSaleMov;
	}
	public void setAdjSaleMov(Double adjSaleMov) {
		this.adjSaleMov = adjSaleMov;
	}
}
