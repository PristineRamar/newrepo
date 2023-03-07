package com.pristine.dto.offermgmt.promotion;

public class BrandDetail {

	private int brandId;
	private double itemMinSize;
	private double itemMaxSize;
	private int itemUOMID;
	private double itemSize;
	
	public int getBrandId() {
		return brandId;
	}
	public void setBranId(int branId) {
		this.brandId = branId;
	}
	public double getItemMinSize() {
		return itemMinSize;
	}
	public void setItemMinSize(double itemMinSize) {
		this.itemMinSize = itemMinSize;
	}
	public double getItemMaxSize() {
		return itemMaxSize;
	}
	public void setItemMaxSize(double itemMaxSize) {
		this.itemMaxSize = itemMaxSize;
	}
	public int getItemUOMID() {
		return itemUOMID;
	}
	public void setItemUOMID(int itemUOMID) {
		this.itemUOMID = itemUOMID;
	}
	public double getItemSize() {
		return itemSize;
	}
	public void setItemSize(double itemSize) {
		this.itemSize = itemSize;
	}
	
	@Override
	public String toString() {
		return "BrandDetail [brandId=" + brandId + ", itemMinSize=" + itemMinSize + ", itemMaxSize=" + itemMaxSize + ", itemUOMID=" + itemUOMID
				+ ", itemSize=" + itemSize + "]";
	}
	
}
