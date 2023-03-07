package com.pristine.dto.offermgmt;

import java.io.Serializable;

//public class PRSizeDTO implements Serializable{
public class PRSizeDTO {
	private int sizeRelnId;
	private String sizeClass;
	private String sizeFamily;
	private int itemCode;
	private int itemSize;
	private int relatedItemCode;
	private String relatedItemSizeClass;
	private int relatedItemSize;
	private Double relatedItemPrice;
	
	public int getSizeRelnId() {
		return sizeRelnId;
	}
	public void setSizeRelnId(int sizeRelnId) {
		this.sizeRelnId = sizeRelnId;
	}
	public String getSizeClass() {
		return sizeClass;
	}
	public void setSizeClass(String sizeClass) {
		this.sizeClass = sizeClass;
	}
	public String getSizeFamily() {
		return sizeFamily;
	}
	public void setSizeFamily(String sizeFamily) {
		this.sizeFamily = sizeFamily;
	}
	public int getItemCode() {
		return itemCode;
	}
	public void setItemCode(int itemCode) {
		this.itemCode = itemCode;
	}
	public int getItemSize() {
		return itemSize;
	}
	public void setItemSize(int itemSize) {
		this.itemSize = itemSize;
	}
	public int getRelatedItemCode() {
		return relatedItemCode;
	}
	public void setRelatedItemCode(int relatedItemCode) {
		this.relatedItemCode = relatedItemCode;
	}
	public String getRelatedItemSizeClass() {
		return relatedItemSizeClass;
	}
	public void setRelatedItemSizeClass(String relatedItemSizeClass) {
		this.relatedItemSizeClass = relatedItemSizeClass;
	}
	public int getRelatedItemSize() {
		return relatedItemSize;
	}
	public void setRelatedItemSize(int relatedItemSize) {
		this.relatedItemSize = relatedItemSize;
	}
	public Double getRelatedItemPrice() {
		return relatedItemPrice;
	}
	public void setRelatedItemPrice(Double relatedItemPrice) {
		this.relatedItemPrice = relatedItemPrice;
	}
	public void copy(PRSizeDTO sizeDto){
		this.sizeRelnId = sizeDto.getSizeRelnId();
		this.sizeClass = sizeDto.getSizeClass();
		this.sizeFamily = sizeDto.getSizeFamily();
		this.itemCode = sizeDto.getItemCode();
		this.itemSize = sizeDto.getItemSize();
		this.relatedItemCode = sizeDto.getRelatedItemCode();
		this.relatedItemSizeClass = sizeDto.getRelatedItemSizeClass();
		this.relatedItemSize = sizeDto.getRelatedItemSize();
		this.relatedItemPrice = sizeDto.getRelatedItemPrice();
	}
}