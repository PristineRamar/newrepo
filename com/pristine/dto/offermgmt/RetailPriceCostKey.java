package com.pristine.dto.offermgmt;

public class RetailPriceCostKey {
	public int levelTypeId;
	public int levelId;
	
	public RetailPriceCostKey(int levelTypeId, int levelId){
		this.levelTypeId = levelTypeId;
		this.levelId = levelId;
	}
	
	@Override
	public int hashCode() {
		final int prime = 31;
		int result = 1;
		result = prime * result + levelTypeId;
		result = prime * result + levelId;
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
		RetailPriceCostKey other = (RetailPriceCostKey) obj;
		if (levelTypeId != other.levelTypeId)
			return false;
		if (levelId != other.levelId)
			return false;
		return true;
	}

	@Override
	public String toString() {
		return "RetailCostKey [levelTypeId=" + levelTypeId + ", levelId=" + levelId + "]";
	}

}
