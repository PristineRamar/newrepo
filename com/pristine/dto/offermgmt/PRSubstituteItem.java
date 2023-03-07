package com.pristine.dto.offermgmt;

import com.pristine.service.offermgmt.ItemKey;

public class PRSubstituteItem {
//	private int itemAType;
//	private int itemAId;
//	private int itemBType;
//	private int itemBId;
	private ItemKey baseProductKey;
	private ItemKey subsProductKey;
	private double impactFactor;
	
//	public int getItemAType() {
//		return itemAType;
//	}
//	public void setItemAType(int itemAType) {
//		this.itemAType = itemAType;
//	}
//	public int getItemAId() {
//		return itemAId;
//	}
//	public void setItemAId(int itemAId) {
//		this.itemAId = itemAId;
//	}
//	public int getItemBType() {
//		return itemBType;
//	}
//	public void setItemBType(int itemBType) {
//		this.itemBType = itemBType;
//	}
//	public int getItemBId() {
//		return itemBId;
//	}
//	public void setItemBId(int itemBId) {
//		this.itemBId = itemBId;
//	}
	
	public double getImpactFactor() {
		return impactFactor;
	}

	public void setImpactFactor(double impactFactor) {
		this.impactFactor = impactFactor;
	}
	public ItemKey getBaseProductKey() {
		return baseProductKey;
	}
	public void setBaseProductKey(ItemKey baseProductKey) {
		this.baseProductKey = baseProductKey;
	}
	public ItemKey getSubsProductKey() {
		return subsProductKey;
	}
	public void setSubsProductKey(ItemKey subsProductKey) {
		this.subsProductKey = subsProductKey;
	}
}
