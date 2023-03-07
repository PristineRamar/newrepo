package com.pristine.dto.offermgmt.substitute;

import java.util.List;

import com.pristine.dto.offermgmt.MultiplePrice;
import com.pristine.service.offermgmt.ItemKey;

public class SubjstituteAdjRMainItemDTO {
	private ItemKey itemKey;
	private MultiplePrice recWeekRegPrice;
	private MultiplePrice recWeekSalePrice;
	private Double regPrediction;
	private Double salePrediction;
	private List<SubstituteAdjRSubsItemDTO> substituteItems;

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

	public Double getRegPrediction() {
		return regPrediction;
	}

	public void setRegPrediction(Double regPrediction) {
		this.regPrediction = regPrediction;
	}

	public Double getSalePrediction() {
		return salePrediction;
	}

	public void setSalePrediction(Double salePrediction) {
		this.salePrediction = salePrediction;
	}

	public List<SubstituteAdjRSubsItemDTO> getSubstituteItems() {
		return substituteItems;
	}

	public void setSubstituteItems(List<SubstituteAdjRSubsItemDTO> substituteItems) {
		this.substituteItems = substituteItems;
	}
}
