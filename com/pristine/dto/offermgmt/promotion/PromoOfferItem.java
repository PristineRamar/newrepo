package com.pristine.dto.offermgmt.promotion;

public class PromoOfferItem {
	private long promoOfferDetailId;
	private int itemCode;
	
	public long getPromoOfferDetailId() {
		return promoOfferDetailId;
	}
	public void setPromoOfferDetailId(long promoOfferDetailId) {
		this.promoOfferDetailId = promoOfferDetailId;
	}
	public int getItemCode() {
		return itemCode;
	}
	public void setItemCode(int itemCode) {
		this.itemCode = itemCode;
	}
	@Override
	public int hashCode() {
		final int prime = 31;
		int result = 1;
		result = prime * result + itemCode;
		result = prime * result + (int) (promoOfferDetailId ^ (promoOfferDetailId >>> 32));
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
		PromoOfferItem other = (PromoOfferItem) obj;
		if (itemCode != other.itemCode)
			return false;
		if (promoOfferDetailId != other.promoOfferDetailId)
			return false;
		return true;
	}

}
