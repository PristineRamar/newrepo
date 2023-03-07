package com.pristine.dto.offermgmt;

public class StrategyKey {
	public int locationLevelId;
	public int locationId;
	public int productLevelId;
	public int productId;
	
	public StrategyKey(int locationLevelId, int locationId, int productLevelId, int productId) {
		super();
		this.locationLevelId = locationLevelId;
		this.locationId = locationId;
		this.productLevelId = productLevelId;
		this.productId = productId;
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
		StrategyKey other = (StrategyKey) obj;
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

	@Override
	public String toString() {
		return "StrategyKey [locationLevelId=" + locationLevelId + ", locationId=" + locationId + ", productLevelId=" + productLevelId
				+ ", productId=" + productId + "]";
	}
	
	
}
