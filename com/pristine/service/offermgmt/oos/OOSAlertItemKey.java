package com.pristine.service.offermgmt.oos;

import com.pristine.dto.offermgmt.MultiplePrice;

public class OOSAlertItemKey {
	private int locationLevelId;
	private int locationId;
	private int productLevelId;
	private int productId;
	private int calendarId;
	private int dayPartId;
	private MultiplePrice regPrice;
	private MultiplePrice salePrice;
	private int adPageNo;
	private int displayTypeId;

	public OOSAlertItemKey(int locationLevelId, int locationId, int productLevelId, int productId, int calendarId, int dayPartId,
			MultiplePrice regPrice, MultiplePrice salePrice, int adPageNo, int displayTypeId) {
		this.locationLevelId = locationLevelId;
		this.locationId = locationId;
		this.productLevelId = productLevelId;
		this.productId = productId;
		this.calendarId = calendarId;
		this.dayPartId = dayPartId;
		this.regPrice = regPrice;
		this.salePrice = salePrice;
		this.adPageNo = adPageNo;
		this.displayTypeId = displayTypeId;
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

	public int getCalendarId() {
		return calendarId;
	}

	public void setCalendarId(int calendarId) {
		this.calendarId = calendarId;
	}

	public int getDayPartId() {
		return dayPartId;
	}

	public void setDayPartId(int dayPartId) {
		this.dayPartId = dayPartId;
	}

	public int getAdPageNo() {
		return adPageNo;
	}

	public void setAdPageNo(int adPageNo) {
		this.adPageNo = adPageNo;
	}

	public int getDisplayTypeId() {
		return displayTypeId;
	}

	public void setDisplayTypeId(int displayTypeId) {
		this.displayTypeId = displayTypeId;
	}

	public MultiplePrice getRegPrice() {
		return regPrice;
	}

	public void setRegPrice(MultiplePrice regPrice) {
		this.regPrice = regPrice;
	}

	public MultiplePrice getSalePrice() {
		return salePrice;
	}

	public void setSalePrice(MultiplePrice salePrice) {
		this.salePrice = salePrice;
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

	@Override
	public int hashCode() {
		final int prime = 31;
		int result = 1;
		result = prime * result + adPageNo;
		result = prime * result + calendarId;
		result = prime * result + dayPartId;
		result = prime * result + displayTypeId;
		result = prime * result + locationId;
		result = prime * result + locationLevelId;
		result = prime * result + productId;
		result = prime * result + productLevelId;
		result = prime * result + ((regPrice == null) ? 0 : regPrice.hashCode());
		result = prime * result + ((salePrice == null) ? 0 : salePrice.hashCode());
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
		OOSAlertItemKey other = (OOSAlertItemKey) obj;
		if (adPageNo != other.adPageNo)
			return false;
		if (calendarId != other.calendarId)
			return false;
		if (dayPartId != other.dayPartId)
			return false;
		if (displayTypeId != other.displayTypeId)
			return false;
		if (locationId != other.locationId)
			return false;
		if (locationLevelId != other.locationLevelId)
			return false;
		if (productId != other.productId)
			return false;
		if (productLevelId != other.productLevelId)
			return false;
		if (regPrice == null) {
			if (other.regPrice != null)
				return false;
		} else if (!regPrice.equals(other.regPrice))
			return false;
		if (salePrice == null) {
			if (other.salePrice != null)
				return false;
		} else if (!salePrice.equals(other.salePrice))
			return false;
		return true;
	}

	 
}
