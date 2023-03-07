package com.pristine.dto;

public class PriceGroupAndCategoryKey {
	String prcGrpCd;
	int productId;
	public PriceGroupAndCategoryKey(String prcGrpCd, int productId) {
		super();
		this.prcGrpCd = prcGrpCd;
		this.productId = productId;
	}
	@Override
	public int hashCode() {
		final int prime = 31;
		int result = 1;
		result = prime * result + ((prcGrpCd == null) ? 0 : prcGrpCd.hashCode());
		result = prime * result + productId;
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
		PriceGroupAndCategoryKey other = (PriceGroupAndCategoryKey) obj;
		if (prcGrpCd == null) {
			if (other.prcGrpCd != null)
				return false;
		} else if (!prcGrpCd.equals(other.prcGrpCd))
			return false;
		if (productId != other.productId)
			return false;
		return true;
	}
}
