package com.pristine.dto.offermgmt;

import com.fasterxml.jackson.annotation.JsonProperty;

public class PRStorePrice {
	private int itemCode;
	private int storeId;
	private Double regPrice = null;
	private Integer regMPack = null;
	private Double regMPrice = null;
	
	@JsonProperty("i-c")
	public int getItemCode() {
		return itemCode;
	}
	public void setItemCode(int itemCode) {
		this.itemCode = itemCode;
	}
	@JsonProperty("s-i")
	public int getStoreId() {
		return storeId;
	}
	public void setStoreId(int storeId) {
		this.storeId = storeId;
	}
	@JsonProperty("r-p")
	public Double getRegPrice() {
		return regPrice;
	}
	public void setRegPrice(Double regPrice) {
		this.regPrice = regPrice;
	}
	@JsonProperty("r-m")
	public Integer getRegMPack() {
		return regMPack;
	}
	public void setRegMPack(Integer regMPack) {
		this.regMPack = regMPack;
	}
	@JsonProperty("r-m-p")
	public Double getRegMPrice() {
		return regMPrice;
	}
	public void setRegMPrice(Double regMPrice) {
		this.regMPrice = regMPrice;
	}
}


