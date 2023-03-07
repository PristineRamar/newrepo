package com.pristine.dto.offermgmt;

import java.io.Serializable;

public class LocationKey implements Serializable{
	/**
	 * 
	 */
	private static final long serialVersionUID = 4375730181266076437L;
	private int locationId;
	private int locationLevelId;
	private int chainId;
	
	public LocationKey(int locationLevelId, int locationId){
		this.locationLevelId = locationLevelId;
		this.locationId = locationId;		
	}
	
	public int getLocationId() {
		return locationId;
	}
	public void setLocationId(int locationId) {
		this.locationId = locationId;
	}
	public int getLocationLevelId() {
		return locationLevelId;
	}
	public void setLocationLevelId(int locationLevelId) {
		this.locationLevelId = locationLevelId;
	}
	
	@Override
	public String toString() {
		return locationLevelId + "-" + locationId;
	}

	@Override
	public int hashCode() {
		final int prime = 31;
		int result = 1;
		result = prime * result + locationId;
		result = prime * result + locationLevelId;
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
		LocationKey other = (LocationKey) obj;
		if (locationId != other.locationId)
			return false;
		if (locationLevelId != other.locationLevelId)
			return false;
		return true;
	}

	public int getChainId() {
		return chainId;
	}

	public void setChainId(int chainId) {
		this.chainId = chainId;
	}
	
	
}
