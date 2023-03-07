package com.pristine.dto.offermgmt.weeklyad;

public class WeeklyAdPromo {
	private long blockId;
	private long promoDefnId;
	private int totalItems;
	private int totalPromotions;
	private int status;
	private String createdBy;
	private String approvedBy;
	
	public long getBlockId() {
		return blockId;
	}
	public void setBlockId(long blockId) {
		this.blockId = blockId;
	}
	public long getPromoDefnId() {
		return promoDefnId;
	}
	public void setPromoDefnId(long promoDefnId) {
		this.promoDefnId = promoDefnId;
	}
	public int getTotalItems() {
		return totalItems;
	}
	public void setTotalItems(int totalItems) {
		this.totalItems = totalItems;
	}
	public int getTotalPromotions() {
		return totalPromotions;
	}
	public void setTotalPromotions(int totalPromotions) {
		this.totalPromotions = totalPromotions;
	}
	public int getStatus() {
		return status;
	}
	public void setStatus(int status) {
		this.status = status;
	}
	public String getCreatedBy() {
		return createdBy;
	}
	public void setCreatedBy(String createdBy) {
		this.createdBy = createdBy;
	}
	public String getApprovedBy() {
		return approvedBy;
	}
	public void setApprovedBy(String approvedBy) {
		this.approvedBy = approvedBy;
	}
}
