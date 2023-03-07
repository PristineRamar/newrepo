package com.pristine.dto;

public class ItemListSetupInputDTO {
	private int masterListId; 
	private int targetListId; 
	private int priceZoneId;
	private int compStoreId; 
	private double startRange; 
	private double endRange; 
	private String itemType;
	
	
	public int getMasterListId() {
		return masterListId;
	}
	public void setMasterListId(int masterListId) {
		this.masterListId = masterListId;
	}
	public int getTargetListId() {
		return targetListId;
	}
	public void setTargetListId(int targetListId) {
		this.targetListId = targetListId;
	}
	public int getPriceZoneId() {
		return priceZoneId;
	}
	public void setPriceZoneId(int priceZoneId) {
		this.priceZoneId = priceZoneId;
	}
	public int getCompStoreId() {
		return compStoreId;
	}
	public void setCompStoreId(int compStoreId) {
		this.compStoreId = compStoreId;
	}
	public double getStartRange() {
		return startRange;
	}
	public void setStartRange(double startRange) {
		this.startRange = startRange;
	}
	public double getEndRange() {
		return endRange;
	}
	public void setEndRange(float endRange) {
		this.endRange = endRange;
	}
	public String getItemType() {
		return itemType;
	}
	public void setItemType(String itemType) {
		this.itemType = itemType;
	}
	@Override
	public String toString() {
		return "ItemListSetupInputDTO [masterListId=" + masterListId + ", targetListId=" + targetListId
				+ ", priceZoneId=" + priceZoneId + ", compStoreId=" + compStoreId + ", startRange=" + startRange
				+ ", endRange=" + endRange + ", itemType=" + itemType + "]";
	} 
}
