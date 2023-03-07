package com.pristine.dto.offermgmt.promotion;

import java.util.ArrayList;

public class PromoOfferDetail {
	private long promoOfferDetailId;
	private long promoBuyReqId;
	private int promoOfferTypeId;
	private String offerUnitType;
	private Integer offerUnitCount;
	private Double offerValue;
	private Integer offerLimit;
	
	private ArrayList<PromoOfferItem> offerItems = new ArrayList<PromoOfferItem>();  
	
	public long getPromoOfferDetailId() {
		return promoOfferDetailId;
	}
	public void setPromoOfferDetailId(long promoOfferDetailId) {
		this.promoOfferDetailId = promoOfferDetailId;
	}
	public long getPromoBuyReqId() {
		return promoBuyReqId;
	}
	public void setPromoBuyReqId(long promoBuyReqId) {
		this.promoBuyReqId = promoBuyReqId;
	}
	public int getPromoOfferTypeId() {
		return promoOfferTypeId;
	}
	public void setPromoOfferTypeId(int promoOfferTypeId) {
		this.promoOfferTypeId = promoOfferTypeId;
	}
	public String getOfferUnitType() {
		return offerUnitType;
	}
	public void setOfferUnitType(String offerUnitType) {
		this.offerUnitType = offerUnitType;
	}
	public Integer getOfferUnitCount() {
		return offerUnitCount;
	}
	public void setOfferUnitCount(Integer offerUnitCount) {
		this.offerUnitCount = offerUnitCount;
	}
	public Double getOfferValue() {
		return offerValue;
	}
	public void setOfferValue(Double offerValue) {
		this.offerValue = offerValue;
	}
	public Integer getOfferLimit() {
		return offerLimit;
	}
	public void setOfferLimit(Integer offerLimit) {
		this.offerLimit = offerLimit;
	}
	public ArrayList<PromoOfferItem> getOfferItems() {
		return offerItems;
	}
	public void setOfferItems(ArrayList<PromoOfferItem> offerItems) {
		this.offerItems = offerItems;
	}
	public void addOfferItems(PromoOfferItem offerItem) {
		this.offerItems.add(offerItem);
	}
	@Override
	public int hashCode() {
		final int prime = 31;
		int result = 1;
		result = prime * result + ((offerItems == null) ? 0 : offerItems.hashCode());
		result = prime * result + ((offerLimit == null) ? 0 : offerLimit.hashCode());
		result = prime * result + ((offerUnitCount == null) ? 0 : offerUnitCount.hashCode());
		result = prime * result + ((offerUnitType == null) ? 0 : offerUnitType.hashCode());
		result = prime * result + ((offerValue == null) ? 0 : offerValue.hashCode());
		result = prime * result + (int) (promoBuyReqId ^ (promoBuyReqId >>> 32));
		result = prime * result + (int) (promoOfferDetailId ^ (promoOfferDetailId >>> 32));
		result = prime * result + promoOfferTypeId;
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
		PromoOfferDetail other = (PromoOfferDetail) obj;
		if (offerItems == null) {
			if (other.offerItems != null)
				return false;
		} else if (!offerItems.equals(other.offerItems))
			return false;
		if (offerLimit == null) {
			if (other.offerLimit != null)
				return false;
		} else if (!offerLimit.equals(other.offerLimit))
			return false;
		if (offerUnitCount == null) {
			if (other.offerUnitCount != null)
				return false;
		} else if (!offerUnitCount.equals(other.offerUnitCount))
			return false;
		if (offerUnitType == null) {
			if (other.offerUnitType != null)
				return false;
		} else if (!offerUnitType.equals(other.offerUnitType))
			return false;
		if (offerValue == null) {
			if (other.offerValue != null)
				return false;
		} else if (!offerValue.equals(other.offerValue))
			return false;
		if (promoBuyReqId != other.promoBuyReqId)
			return false;
		if (promoOfferDetailId != other.promoOfferDetailId)
			return false;
		if (promoOfferTypeId != other.promoOfferTypeId)
			return false;
		return true;
	}
}
