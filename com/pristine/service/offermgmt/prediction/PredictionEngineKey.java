package com.pristine.service.offermgmt.prediction;

public class PredictionEngineKey {
	
	int locationLevelId;
	int locationId;
	//double upc;
	//double minPrice;
	int itemCode;
	int regQuantity;
	double regPrice;
	int saleQuantity;
	double salePrice;
	int adPageNo;
	int adBlockNo;
	int displayTypeId;
	int promoTypeId;
	
//	public PredictionEngineKey(int locationLevelId, int locationId, double upc, double minPrice) {
//		super();
//		this.locationLevelId = locationLevelId;
//		this.locationId = locationId;
//		this.upc = upc;
//		this.minPrice = minPrice;
//	}
	
	public PredictionEngineKey(int locationLevelId, int locationId, int itemCode, int regQuantity, double regPrice,
			int saleQuantity, double salePrice, int adPageNo, int adBlockNo, int promoTypeId, int displayTypeId) {
		super();
		this.locationLevelId = locationLevelId;
		this.locationId = locationId;
		this.itemCode = itemCode;
		this.regQuantity = regQuantity;
		this.regPrice = regPrice;
		this.saleQuantity = saleQuantity;
		this.salePrice = salePrice;
		this.adPageNo = adPageNo;
		this.adBlockNo = adBlockNo;
		this.promoTypeId = promoTypeId;
		this.displayTypeId = displayTypeId;		
	}
	
//	@Override
//	public String toString() {
//		return "PredictionKey [upc=" + upc + ", minPrice=" + minPrice + "]";
//	}

//	public double getUpc() {
//		return upc;
//	}
//
//	public void setUpc(double upc) {
//		this.upc = upc;
//	}

//	public double getMinPrice() {
//		return minPrice;
//	}
//
//	public void setMinPrice(double minPrice) {
//		this.minPrice = minPrice;
//	}

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
		result = prime * result + adBlockNo;
		result = prime * result + adPageNo;
		result = prime * result + displayTypeId;
		result = prime * result + itemCode;
		result = prime * result + locationId;
		result = prime * result + locationLevelId;
		result = prime * result + promoTypeId;
		long temp;
		temp = Double.doubleToLongBits(regPrice);
		result = prime * result + (int) (temp ^ (temp >>> 32));
		result = prime * result + regQuantity;
		temp = Double.doubleToLongBits(salePrice);
		result = prime * result + (int) (temp ^ (temp >>> 32));
		result = prime * result + saleQuantity;
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
		PredictionEngineKey other = (PredictionEngineKey) obj;
		if (adBlockNo != other.adBlockNo)
			return false;
		if (adPageNo != other.adPageNo)
			return false;
		if (displayTypeId != other.displayTypeId)
			return false;
		if (itemCode != other.itemCode)
			return false;
		if (locationId != other.locationId)
			return false;
		if (locationLevelId != other.locationLevelId)
			return false;
		if (promoTypeId != other.promoTypeId)
			return false;
		if (Double.doubleToLongBits(regPrice) != Double.doubleToLongBits(other.regPrice))
			return false;
		if (regQuantity != other.regQuantity)
			return false;
		if (Double.doubleToLongBits(salePrice) != Double.doubleToLongBits(other.salePrice))
			return false;
		if (saleQuantity != other.saleQuantity)
			return false;
		return true;
	}
	 
}
