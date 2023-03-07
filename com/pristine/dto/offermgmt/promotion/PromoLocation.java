package com.pristine.dto.offermgmt.promotion;

public class PromoLocation implements Cloneable {
	private long promoDefnId;
	private int locationLevelId;
	private int locationId;
	
	public long getPromoDefnId() {
		return promoDefnId;
	}
	public void setPromoDefnId(long promoDefnId) {
		this.promoDefnId = promoDefnId;
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
	@Override
	public int hashCode() {
		final int prime = 31;
		int result = 1;
		result = prime * result + locationId;
		result = prime * result + locationLevelId;
		result = prime * result + (int) (promoDefnId ^ (promoDefnId >>> 32));
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
		PromoLocation other = (PromoLocation) obj;
		if (locationId != other.locationId)
			return false;
		if (locationLevelId != other.locationLevelId)
			return false;
		if (promoDefnId != other.promoDefnId)
			return false;
		return true;
	}
	
	@Override
	public Object clone() throws CloneNotSupportedException {
        return super.clone();
    }
}
