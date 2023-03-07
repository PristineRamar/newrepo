package com.pristine.dto;

public class ItemDetailKey {
	private String upc;

	private String retailerItemCode;
	
	
	public ItemDetailKey(String upc, String retailerItemCode){
		this.retailerItemCode = retailerItemCode;
		this.upc = upc;
	}
	
	public String getUpc() {
		return upc;
	}
	public void setUpc(String upc) {
		this.upc = upc;
	}
	public String getRetailerItemCode() {
		return retailerItemCode;
	}
	public void setRetailerItemCode(String retailerItemCode) {
		this.retailerItemCode = retailerItemCode;
	}
	
	@Override
	public int hashCode() {
		final int prime = 31;
		int result = 1;
		result = prime
				* result
				+ ((retailerItemCode == null) ? 0 : retailerItemCode.hashCode());
		result = prime * result + ((upc == null) ? 0 : upc.hashCode());
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
		ItemDetailKey other = (ItemDetailKey) obj;
		if (retailerItemCode == null) {
			if (other.retailerItemCode != null)
				return false;
		} else if (!retailerItemCode.equals(other.retailerItemCode))
			return false;
		if (upc == null) {
			if (other.upc != null)
				return false;
		} else if (!upc.equals(other.upc))
			return false;
		return true;
	}
	
	@Override
	public String toString(){
		return "{upc:" + this.upc + ", retailer-item-code:" + this.retailerItemCode + "}";
	}
	
}
