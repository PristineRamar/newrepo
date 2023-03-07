package com.pristine.dto.offermgmt;

public class ProductLocationMappingKey {

	private int productLevelId;
	private int productId;
	private int locationLevelId;
	private int locationId;
	
	public ProductLocationMappingKey(int productLevelId, int productId, int locationLevelId, int locationId){
		this.productLevelId = productLevelId;
		this.productId = productId;
		this.locationLevelId = locationLevelId;
		this.locationId = locationId;
	}
	
	public int getProductLevelId() {
		return productLevelId;
	}
	public void setProductLevelId(int productLevelId) {
		this.productLevelId = productLevelId;
	}
	public int getProductId() {
		return productId;
	}
	public void setProductId(int productId) {
		this.productId = productId;
	}
	public int getLocationLevelId() {
		return locationLevelId;
	}
	public void setLocationLevelId(int locationLevelId) {
		this.locationLevelId = locationLevelId;
	}
	public int getLocationId() {
		return locationId;
	}
	public void setLocationId(int locationId) {
		this.locationId = locationId;
	}
	
	@Override
	public int hashCode() {
		final int prime = 31;
		int result = 1;
		result = prime * result + locationId;
		result = prime * result + locationLevelId;
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
		ProductLocationMappingKey other = (ProductLocationMappingKey) obj;
		if (locationId != other.locationId)
			return false;
		if (locationLevelId != other.locationLevelId)
			return false;
		if (productId != other.productId)
			return false;
		if (productLevelId != other.productLevelId)
			return false;
		return true;
	}
	
	
}
