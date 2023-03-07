package com.pristine.dataload.tops;

public class ItemStoreKey {

	private String storeOrZoneKey;
	private String itemCode;
	
	public ItemStoreKey(String storeOrZoneKey, String itemCode) {
		super();
		this.storeOrZoneKey = storeOrZoneKey;
		this.itemCode = itemCode;
	}

	public String getStoreOrZoneKey() {
		return storeOrZoneKey;
	}
	public void setStoreOrZoneKey(String storeOrZoneKey) {
		this.storeOrZoneKey = storeOrZoneKey;
	}
	public String getItemCode() {
		return itemCode;
	}
	public void setItemCode(String itemCode) {
		this.itemCode = itemCode;
	}
	
	@Override
	public int hashCode() {
		final int prime = 31;
		int result = 1;
		result = prime * result + ((itemCode == null) ? 0 : itemCode.hashCode());
		result = prime * result + ((storeOrZoneKey == null) ? 0 : storeOrZoneKey.hashCode());
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
		ItemStoreKey other = (ItemStoreKey) obj;
		if (itemCode == null) {
			if (other.itemCode != null)
				return false;
		} else if (!itemCode.equals(other.itemCode))
			return false;
		if (storeOrZoneKey == null) {
			if (other.storeOrZoneKey != null)
				return false;
		} else if (!storeOrZoneKey.equals(other.storeOrZoneKey))
			return false;
		return true;
	}
	
	@Override
	public String toString() {
		return "[" + storeOrZoneKey + "-" + itemCode + "]";
	}
}
