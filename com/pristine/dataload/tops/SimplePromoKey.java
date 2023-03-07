package com.pristine.dataload.tops;

public class SimplePromoKey {
	private String promoStartDate;
	private String promoEndDate;
	private int productLevelId;
	private String productId;
	
	public String getPromoStartDate() {
		return promoStartDate;
	}
	public void setPromoStartDate(String promoStartDate) {
		this.promoStartDate = promoStartDate;
	}
	public String getPromoEndDate() {
		return promoEndDate;
	}
	public void setPromoEndDate(String promoEndDate) {
		this.promoEndDate = promoEndDate;
	}
	public int getProductLevelId() {
		return productLevelId;
	}
	public void setProductLevelId(int productLevelId) {
		this.productLevelId = productLevelId;
	}
	public String getProductId() {
		return productId;
	}
	public void setProductId(String productId) {
		this.productId = productId;
	}
	@Override
	public int hashCode() {
		final int prime = 31;
		int result = 1;
		result = prime * result + ((productId == null) ? 0 : productId.hashCode());
		result = prime * result + productLevelId;
		result = prime * result + ((promoEndDate == null) ? 0 : promoEndDate.hashCode());
		result = prime * result + ((promoStartDate == null) ? 0 : promoStartDate.hashCode());
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
		SimplePromoKey other = (SimplePromoKey) obj;
		if (productId == null) {
			if (other.productId != null)
				return false;
		} else if (!productId.equals(other.productId))
			return false;
		if (productLevelId != other.productLevelId)
			return false;
		if (promoEndDate == null) {
			if (other.promoEndDate != null)
				return false;
		} else if (!promoEndDate.equals(other.promoEndDate))
			return false;
		if (promoStartDate == null) {
			if (other.promoStartDate != null)
				return false;
		} else if (!promoStartDate.equals(other.promoStartDate))
			return false;
		return true;
	}
	
	public SimplePromoKey(String promoStartDate, String promoEndDate, int productLevelId, String productId) {
		super();
		this.promoStartDate = promoStartDate;
		this.promoEndDate = promoEndDate;
		this.productLevelId = productLevelId;
		this.productId = productId;
	}
	
}
