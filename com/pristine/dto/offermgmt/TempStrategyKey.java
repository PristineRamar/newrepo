package com.pristine.dto.offermgmt;

public class TempStrategyKey {

	public int locationLevelId;
	public int locationId;
	public int productLevelId;
	public int productId;
	public int priceCheckListId;
	public int vendorId;
	public int stateId;
	public char dsdRecommendationFlag;
	public int criteriaId;

	public TempStrategyKey(int locationLevelId, int locationId, int productLevelId, int productId, int priceCheckListId,
			int vendorId, int stateId, char dsdRecommendationFlag, int criteriaId) {
		super();
		this.locationLevelId = locationLevelId;
		this.locationId = locationId;
		this.productLevelId = productLevelId;
		this.productId = productId;
		this.priceCheckListId = priceCheckListId;
		this.vendorId = vendorId;
		this.stateId = stateId;
		this.dsdRecommendationFlag = dsdRecommendationFlag;
		this.criteriaId = criteriaId;
	}
	
	@Override
	public boolean equals(Object obj) {
		if (this == obj)
			return true;
		if (obj == null)
			return false;
		if (getClass() != obj.getClass())
			return false;
		TempStrategyKey other = (TempStrategyKey) obj;
		if (locationId != other.locationId)
			return false;
		if (locationLevelId != other.locationLevelId)
			return false;
		if (productId != other.productId)
			return false;
		if (productLevelId != other.productLevelId)
			return false;
		if (priceCheckListId != other.priceCheckListId)
			return false;
		if (vendorId != other.vendorId)
			return false;
		if (stateId != other.stateId)
			return false;
		if (dsdRecommendationFlag != other.dsdRecommendationFlag)
			return false;
		if (criteriaId != other.criteriaId)
			return false;
		
		return true;
	}

	@Override
	public int hashCode() {
		final int prime = 31;
		int result = 1;
		result = prime * result + locationId;
		result = prime * result + locationLevelId;
		result = prime * result + productId;
		result = prime * result + productLevelId;
		result = prime * result + priceCheckListId;
		result = prime * result + vendorId;
		result = prime * result + stateId;
		result = prime * result + dsdRecommendationFlag;
		result = prime * result + criteriaId;
		return result;
	}
	
	

	@Override
	public String toString() {
		return "StrategyKey [locationLevelId=" + locationLevelId + ", locationId=" + locationId + ", productLevelId="
				+ productLevelId + ", productId=" + productId + ", priceCheckListId=" + priceCheckListId + ", vendorId="
				+ vendorId + ", stateId=" + stateId + ", dsdRecommendationFlag=" + dsdRecommendationFlag
				+ ", criteriaId=" + criteriaId + "]";
	}

}
