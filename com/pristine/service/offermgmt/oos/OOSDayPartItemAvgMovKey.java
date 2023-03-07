package com.pristine.service.offermgmt.oos;

public class OOSDayPartItemAvgMovKey {
	private int dayId;
	private int dayPartId;
	private int productLevelId;
	private int productId;
	
	public OOSDayPartItemAvgMovKey(int dayId, int dayPartId, int productLevelId, int productId) {
		super();
		this.dayId = dayId;
		this.dayPartId = dayPartId;
		this.productLevelId = productLevelId;
		this.productId = productId;
	}
	
	public int getDayId() {
		return dayId;
	}
	public void setDayId(int dayId) {
		this.dayId = dayId;
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
	public int hashCode() {
		final int prime = 31;
		int result = 1;
		result = prime * result + dayId;
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
		OOSDayPartItemAvgMovKey other = (OOSDayPartItemAvgMovKey) obj;
		if (dayId != other.dayId)
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
