package com.pristine.dto.offermgmt.substitute;

import com.pristine.dto.offermgmt.MultiplePrice;
import com.pristine.service.offermgmt.ItemKey;

public class SubstituteAdjRSubsItemDTO {
	private ItemKey itemKey;
	private MultiplePrice recWeekRegPrice;
	private MultiplePrice recWeekSalePrice;
	private MultiplePrice curRegPrice;
	private MultiplePrice curSalePrice;
	private Double impactFactor;

	public ItemKey getItemKey() {
		return itemKey;
	}

	public void setItemKey(ItemKey itemKey) {
		this.itemKey = itemKey;
	}

	public MultiplePrice getRecWeekRegPrice() {
		return recWeekRegPrice;
	}

	public void setRecWeekRegPrice(MultiplePrice recWeekRegPrice) {
		this.recWeekRegPrice = recWeekRegPrice;
	}

	public MultiplePrice getRecWeekSalePrice() {
		return recWeekSalePrice;
	}

	public void setRecWeekSalePrice(MultiplePrice recWeekSalePrice) {
		this.recWeekSalePrice = recWeekSalePrice;
	}

	public MultiplePrice getCurRegPrice() {
		return curRegPrice;
	}

	public void setCurRegPrice(MultiplePrice curRegPrice) {
		this.curRegPrice = curRegPrice;
	}

	public MultiplePrice getCurSalePrice() {
		return curSalePrice;
	}

	public void setCurSalePrice(MultiplePrice curSalePrice) {
		this.curSalePrice = curSalePrice;
	}

	public Double getImpactFactor() {
		return impactFactor;
	}

	public void setImpactFactor(Double impactFactor) {
		this.impactFactor = impactFactor;
	}
}
