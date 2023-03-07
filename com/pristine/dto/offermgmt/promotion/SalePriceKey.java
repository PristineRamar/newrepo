package com.pristine.dto.offermgmt.promotion;

import com.pristine.dto.offermgmt.MultiplePrice;

public class SalePriceKey {
	private MultiplePrice regPrice;
	private MultiplePrice salePrice;
	private int promoTypeId;
	
	
	public MultiplePrice getSalePrice() {
		return salePrice;
	}
	public void setSalePrice(MultiplePrice salePrice) {
		this.salePrice = salePrice;
	}
	
	public MultiplePrice getRegPrice() {
		return regPrice;
	}
	public void setRegPrice(MultiplePrice regPrice) {
		this.regPrice = regPrice;
	}
	public int getPromoTypeId() {
		return promoTypeId;
	}
	public void setPromoTypeId(int promoTypeId) {
		this.promoTypeId = promoTypeId;
	}
	@Override
	public String toString() {
		return "SalePriceKey [regPrice=" + regPrice + ", salePrice=" + salePrice + ", promoTypeId=" + promoTypeId + "]";
	}
	@Override
	public int hashCode() {
		final int prime = 31;
		int result = 1;
		result = prime * result + promoTypeId;
		result = prime * result + ((regPrice == null) ? 0 : regPrice.hashCode());
		result = prime * result + ((salePrice == null) ? 0 : salePrice.hashCode());
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
		SalePriceKey other = (SalePriceKey) obj;
		if (promoTypeId != other.promoTypeId)
			return false;
		if (regPrice == null) {
			if (other.regPrice != null)
				return false;
		} else if (!regPrice.equals(other.regPrice))
			return false;
		if (salePrice == null) {
			if (other.salePrice != null)
				return false;
		} else if (!salePrice.equals(other.salePrice))
			return false;
		return true;
	}
 
	
}
