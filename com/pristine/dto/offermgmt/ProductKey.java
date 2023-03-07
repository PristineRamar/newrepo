package com.pristine.dto.offermgmt;

public class ProductKey {
	private int productId;
	private int productLevelId;
	
	public ProductKey(int productLevelId, int productId){
		this.productLevelId = productLevelId;
		this.productId = productId;		
	}
	
	public int getProductId() {
		return productId;
	}

	public void setProductId(int productId) {
		this.productId = productId;
	}

	public int getProductLevelId() {
		return productLevelId;
	}

	public void setProductLevelId(int productLevelId) {
		this.productLevelId = productLevelId;
	}
	
	@Override
	public int hashCode() {
		final int prime = 31;
		int result = 1;
		result = prime * result + productId;
		result = prime * result + productLevelId;
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
		ProductKey other = (ProductKey) obj;
		if (productId != other.productId)
			return false;
		if (productLevelId != other.productLevelId)
			return false;
		return true;
	}

	@Override
	public String toString() {
		return productLevelId + "-" + productId;
	}

	
}
