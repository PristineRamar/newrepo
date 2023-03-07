package com.pristine.service.offermgmt.oos;

public class OOSDayPartDetailKey {
	private int calendarId;
	private int dayPartId;
	private int productLevelId;
	private int productId;
	
	public OOSDayPartDetailKey(int calendarId, int dayPartId, int productLevelId, int productId) {
		super();
		this.calendarId = calendarId;
		this.dayPartId = dayPartId;
		this.productLevelId = productLevelId;
		this.productId = productId;
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
	public String toString() {
		return "OOSDayPartDetailKey [calendarId=" + calendarId + ", dayPartId=" + dayPartId + ", productLevelId=" + productLevelId + ", productId="
				+ productId + "]";
	}

	@Override
	public int hashCode() {
		final int prime = 31;
		int result = 1;
		result = prime * result + calendarId;
		result = prime * result + dayPartId;
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
		OOSDayPartDetailKey other = (OOSDayPartDetailKey) obj;
		if (calendarId != other.calendarId)
			return false;
		if (dayPartId != other.dayPartId)
			return false;
		if (productId != other.productId)
			return false;
		if (productLevelId != other.productLevelId)
			return false;
		return true;
	}
}
