package com.pristine.dto.offermgmt.weeklyad;

import java.util.ArrayList;

import com.pristine.dto.offermgmt.promotion.PromoDefinition;

public class WeeklyAdBlock{
	private long blockId;
	private long pageId;
	private int blockNumber;
	private int totalPromotions;
	private int status;
	private long adjustedUnits;
	private int actualTotalItems;
	private int adPlexTotalItems;
	private ArrayList<PromoDefinition> promotions = new ArrayList<PromoDefinition>();
	
	public long getAdjustedUnits() {
		return adjustedUnits;
	}
	public void setAdjustedUnits(long adjustedUnits) {
		this.adjustedUnits = adjustedUnits;
	}
	public long getBlockId() {
		return blockId;
	}
	public void setBlockId(long blockId) {
		this.blockId = blockId;
	}
	public long getPageId() {
		return pageId;
	}
	public void setPageId(long pageId) {
		this.pageId = pageId;
	}
	public int getBlockNumber() {
		return blockNumber;
	}
	public void setBlockNumber(int blockNumber) {
		this.blockNumber = blockNumber;
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
	public ArrayList<PromoDefinition> getPromotions() {
		return promotions;
	}
	public void setPromotions(ArrayList<PromoDefinition> promotions) {
		this.promotions = promotions;
	}
	public void addPromotion(PromoDefinition promotion){
		this.promotions.add(promotion);
	}
	public int getActualTotalItems() {
		return actualTotalItems;
	}
	public void setActualTotalItems(int actualTotalItems) {
		this.actualTotalItems = actualTotalItems;
	}
	public int getAdPlexTotalItems() {
		return adPlexTotalItems;
	}
	public void setAdPlexTotalItems(int adPlexTotalItems) {
		this.adPlexTotalItems = adPlexTotalItems;
	}
	
	
}
