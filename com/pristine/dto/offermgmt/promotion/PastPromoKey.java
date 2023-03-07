package com.pristine.dto.offermgmt.promotion;

import java.util.HashSet;

import com.pristine.dto.offermgmt.MultiplePrice;

public class PastPromoKey {

	private int promoTypeId;
	private int displayTypeId;
	private MultiplePrice salePrice;
	private HashSet<Integer> ppgGroupIds;
	
	public PastPromoKey(MultiplePrice salePrice, int promoTypeId, int displayTypeId, 
			HashSet<Integer> ppgGroupIds) {
		this.salePrice = salePrice;
		this.promoTypeId = promoTypeId;
		this.displayTypeId = displayTypeId;
		this.ppgGroupIds = new HashSet<Integer>();
		this.ppgGroupIds.addAll(ppgGroupIds);
	}

	public int getPromoTypeId() {
		return promoTypeId;
	}

	public void setPromoTypeId(int promoTypeId) {
		this.promoTypeId = promoTypeId;
	}

	public int getDisplayTypeId() {
		return displayTypeId;
	}

	public void setDisplayTypeId(int displayTypeId) {
		this.displayTypeId = displayTypeId;
	}

	public MultiplePrice getSalePrice() {
		return salePrice;
	}

	public void setSalePrice(MultiplePrice salePrice) {
		this.salePrice = salePrice;
	}	
	
	@Override
	public int hashCode() {
		final int prime = 31;
		int result = 1;
		result = prime * result + displayTypeId;
		result = prime * result + promoTypeId;
		result = result + salePrice.hashCode();
		for(Integer ppgGroupId : ppgGroupIds){
			result = prime * ppgGroupId + result;
		}
		return result;
	}

	@Override
	public boolean equals(Object obj) {
		if (this == obj)
			return true;
		if (obj == null)
			return false;
		if (getClass() != obj.getClass())
			return false;
		PastPromoKey other = (PastPromoKey) obj;
		if (!salePrice.equals(other.salePrice))
			return false;
		if (displayTypeId != other.displayTypeId)
			return false;
		if (promoTypeId != other.promoTypeId)
			return false;
		
		if(!ppgGroupIds.equals(other.ppgGroupIds)){
			return false;
		}
		
		return true;
	}

	@Override
	public String toString() {
		return "[promoTypeId=" + promoTypeId + ", displayTypeId=" + displayTypeId + 
				", salePrice = " + salePrice + ", PPGGroupIds = " + ppgGroupIds + " ]";
	}
	
}
