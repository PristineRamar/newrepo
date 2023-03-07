package com.pristine.dto.offermgmt.promotion;

import java.util.HashMap;

import com.pristine.dto.offermgmt.ProductKey;
import com.pristine.lookup.offermgmt.promotion.PromoTypeLookup;

public class PromoProductGroup {
	private long groupId;
	private double minRegUnitPrice;
	private ProductKey leadItem;
	private double maxRegUnitPrice;
	private HashMap<ProductKey, PromoItemDTO> items = new HashMap<ProductKey, PromoItemDTO>();
	private PromoTypeLookup supportedPromoType;
	private HashMap<ProductKey, Long> lastXWeeksTotalMovement;
	private PPGAdditionalDetail additionalDetail = new PPGAdditionalDetail();

	public long getGroupId() {
		return groupId;
	}

	public void setGroupId(long groupId) {
		this.groupId = groupId;
	}

	public double getMinRegUnitPrice() {
		return minRegUnitPrice;
	}

	public void setMinRegUnitPrice(double minRegUnitPrice) {
		this.minRegUnitPrice = minRegUnitPrice;
	}

	public double getMaxRegUnitPrice() {
		return maxRegUnitPrice;
	}

	public void setMaxRegUnitPrice(double maxRegUnitPrice) {
		this.maxRegUnitPrice = maxRegUnitPrice;
	}

	public HashMap<ProductKey, PromoItemDTO> getItems() {
		return items;
	}

	public void setItems(HashMap<ProductKey, PromoItemDTO> items) {
		this.items = items;
	}

	public PromoTypeLookup getSupportedPromoType() {
		return supportedPromoType;
	}

	public void setSupportedPromoType(PromoTypeLookup supportedPromoType) {
		this.supportedPromoType = supportedPromoType;
	}

	@Override
	public int hashCode() {
		final int prime = 31;
		int result = 1;
		result = prime * result + (int) (groupId ^ (groupId >>> 32));
		result = prime * result + ((items == null) ? 0 : items.hashCode());
		long temp;
		temp = Double.doubleToLongBits(maxRegUnitPrice);
		result = prime * result + (int) (temp ^ (temp >>> 32));
		temp = Double.doubleToLongBits(minRegUnitPrice);
		result = prime * result + (int) (temp ^ (temp >>> 32));
		result = prime * result + ((supportedPromoType == null) ? 0 : supportedPromoType.hashCode());
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
		PromoProductGroup other = (PromoProductGroup) obj;
		if (groupId != other.groupId)
			return false;
		if (items == null) {
			if (other.items != null)
				return false;
		} else if (!items.equals(other.items))
			return false;
		if (Double.doubleToLongBits(maxRegUnitPrice) != Double.doubleToLongBits(other.maxRegUnitPrice))
			return false;
		if (Double.doubleToLongBits(minRegUnitPrice) != Double.doubleToLongBits(other.minRegUnitPrice))
			return false;
		if (supportedPromoType != other.supportedPromoType)
			return false;
		return true;
	}

	public ProductKey getLeadItem() {
		return leadItem;
	}

	public void setLeadItem(ProductKey leadItem) {
		this.leadItem = leadItem;
	}

	public HashMap<ProductKey, Long> getLastXWeeksTotalMovement() {
		return lastXWeeksTotalMovement;
	}

	public void setLastXWeeksTotalMovement(HashMap<ProductKey, Long> lastXWeeksTotalMovement) {
		this.lastXWeeksTotalMovement = lastXWeeksTotalMovement;
	}

	public PPGAdditionalDetail getAdditionalDetail() {
		return additionalDetail;
	}

	public void setAdditionalDetail(PPGAdditionalDetail additionalDetail) {
		this.additionalDetail = additionalDetail;
	}

//	public String getWeeksInWhichPromoted() {
//		return weeksInWhichPromoted;
//	}

//	public void setWeeksInWhichPromoted(String weeksInWhichPromoted) {
//		this.weeksInWhichPromoted = weeksInWhichPromoted;
//	}
	
	
}
