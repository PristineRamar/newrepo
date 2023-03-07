package com.pristine.service.offermgmt.prediction;

//import com.pristine.util.Constants;

public class PredictionDetailKey {

	private int locationLevelId;
	private int locationId;
	private long lirOrItemCode;
	private int regQuantity;
	private Double regPrice;
	private int saleQuantity;
	private Double salePrice;
	private int adPageNo;
	private int blockNo;
	private int displayTypeId;
	private int promoTypeId;
	
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
	public long getLirOrItemCode() {
		return lirOrItemCode;
	}
	public void setLirOrItemCode(long lirOrItemCode) {
		this.lirOrItemCode = lirOrItemCode;
	}
	public int getRegQuantity() {
		return regQuantity;
	}
	public void setRegQuantity(int regQuantity) {
		this.regQuantity = regQuantity;
	}
	public Double getRegPrice() {
		return regPrice;
	}
	public void setRegPrice(Double regPrice) {
		this.regPrice = regPrice;
	}
	
	public PredictionDetailKey(int locationLevelId, int locationId, long lirOrItemCode, int regQuantity, Double regPrice,
			int saleQuantity, Double salePrice, int adPageNo, int blockNo, int promoTypeId, int displayTypeId){
		this.locationLevelId = locationLevelId;
		this.locationId = locationId;
		this.lirOrItemCode = lirOrItemCode;
		this.regQuantity = regQuantity;
		this.regPrice = regPrice;
//		this.saleQuantity = (saleQuantity == 0 ? Constants.NULLID : saleQuantity);
//		this.salePrice = (salePrice == 0 ? Constants.NULLID : salePrice);
//		this.promoTypeId = (promoTypeId == 0 ? Constants.NULLID : promoTypeId);
//		this.adPageNo = (adPageNo == 0 ? Constants.NULLID : adPageNo);
//		this.blockNo = (blockNo == 0 ? Constants.NULLID : blockNo);
//		this.displayTypeId = (displayTypeId == 0 ? Constants.NULLID : displayTypeId);
		this.saleQuantity = saleQuantity;
		this.salePrice = salePrice;
		this.promoTypeId = promoTypeId;
		this.adPageNo = adPageNo;
		this.blockNo = blockNo;
		this.displayTypeId = displayTypeId;
	}
	
	 
	public int getSaleQuantity() {
		return saleQuantity;
	}
	public void setSaleQuantity(int saleQuantity) {
		this.saleQuantity = saleQuantity;
	}
	public Double getSalePrice() {
		return salePrice;
	}
	public void setSalePrice(Double salePrice) {
		this.salePrice = salePrice;
	}
	public int getAdPageNo() {
		return adPageNo;
	}
	public void setAdPageNo(int adPageNo) {
		this.adPageNo = adPageNo;
	}
	public int getBlockNo() {
		return blockNo;
	}
	public void setBlockNo(int blockNo) {
		this.blockNo = blockNo;
	}
	public int getDisplayTypeId() {
		return displayTypeId;
	}
	public void setDisplayTypeId(int displayTypeId) {
		this.displayTypeId = displayTypeId;
	}
	@Override
	public int hashCode() {
		final int prime = 31;
		int result = 1;
		result = prime * result + adPageNo;
		result = prime * result + blockNo;
		result = prime * result + displayTypeId;
		result = prime * result + (int) (lirOrItemCode ^ (lirOrItemCode >>> 32));
		result = prime * result + locationId;
		result = prime * result + locationLevelId;
		result = prime * result + promoTypeId;
		result = prime * result + ((regPrice == null) ? 0 : regPrice.hashCode());
		result = prime * result + regQuantity;
		result = prime * result + ((salePrice == null) ? 0 : salePrice.hashCode());
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
		PredictionDetailKey other = (PredictionDetailKey) obj;
		if (adPageNo != other.adPageNo)
			return false;
		if (blockNo != other.blockNo)
			return false;
		if (displayTypeId != other.displayTypeId)
			return false;
		if (lirOrItemCode != other.lirOrItemCode)
			return false;
		if (locationId != other.locationId)
			return false;
		if (locationLevelId != other.locationLevelId)
			return false;
		if (promoTypeId != other.promoTypeId)
			return false;
		if (regPrice == null) {
			if (other.regPrice != null)
				return false;
		} else if (!regPrice.equals(other.regPrice))
			return false;
		if (regQuantity != other.regQuantity)
			return false;
		if (salePrice == null) {
			if (other.salePrice != null)
				return false;
		} else if (!salePrice.equals(other.salePrice))
			return false;
		if (saleQuantity != other.saleQuantity)
			return false;
		return true;
	}
	
	@Override
	public String toString() {
		return "PredictionDetailKey [Location=" + locationLevelId + "-" + locationId + ", lirOrItemCode=" + lirOrItemCode
				+ ", RegPrice=" + regQuantity + "/" + regPrice + ", SalePrice=" + saleQuantity + "/" + salePrice
				+ ", PageNo=" + adPageNo + ", BlockNo=" + blockNo + ", DisplayTypeId=" + displayTypeId + ", PromoTypeId=" + promoTypeId + "]";
	}
	public int getPromoTypeId() {
		return promoTypeId;
	}
	public void setPromoTypeId(int promoTypeId) {
		this.promoTypeId = promoTypeId;
	}
}
