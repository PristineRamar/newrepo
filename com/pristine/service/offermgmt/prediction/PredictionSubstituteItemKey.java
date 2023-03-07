package com.pristine.service.offermgmt.prediction;

import com.pristine.service.offermgmt.ItemKey;

public class PredictionSubstituteItemKey {
	private ItemKey itemKey;
	private int scenarioId;
	private double pricePoint;
	private int pricePointType;
	
	public PredictionSubstituteItemKey(ItemKey itemKey, int scenarioId) {
		super();
		this.itemKey = itemKey;
		this.scenarioId = scenarioId;
	}
	 
	public ItemKey getItemKey() {
		return itemKey;
	}

	public void setItemKey(ItemKey itemKey) {
		this.itemKey = itemKey;
	}

	public double getPricePoint() {
		return pricePoint;
	}

	public void setPricePoint(double pricePoint) {
		this.pricePoint = pricePoint;
	}

	public int getPricePointType() {
		return pricePointType;
	}

	public void setPricePointType(int pricePointType) {
		this.pricePointType = pricePointType;
	}


	public int getScenarioId() {
		return scenarioId;
	}


	public void setScenarioId(int scenarioId) {
		this.scenarioId = scenarioId;
	}


	@Override
	public int hashCode() {
		final int prime = 31;
		int result = 1;
		result = prime * result + ((itemKey == null) ? 0 : itemKey.hashCode());
		result = prime * result + scenarioId;
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
		PredictionSubstituteItemKey other = (PredictionSubstituteItemKey) obj;
		if (itemKey == null) {
			if (other.itemKey != null)
				return false;
		} else if (!itemKey.equals(other.itemKey))
			return false;
		if (scenarioId != other.scenarioId)
			return false;
		return true;
	}

	@Override
	public String toString() {
		return "PredictionSubstituteItemKey [itemKey=" + itemKey + ", scenarioId=" + scenarioId + ", pricePoint="
				+ pricePoint + ", pricePointType=" + pricePointType + "]";
	}

 
}
