package com.pristine.dto;

public class CompStoreItemDetailsKey {
	private String compStrNo;
	private String upc;
	private String itemCode;
	
	public CompStoreItemDetailsKey(String compStrNo, String upc, String itemCode) {
		this.compStrNo = compStrNo;
		this.upc = upc;
		this.itemCode = itemCode;
	}

	
	@Override
	public int hashCode() {
		final int prime = 31;
		int result = 1;
		result = prime * result + ((compStrNo == null) ? 0 : compStrNo.hashCode());
		result = prime * result + ((itemCode == null) ? 0 : itemCode.hashCode());
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
		CompStoreItemDetailsKey other = (CompStoreItemDetailsKey) obj;
		if (compStrNo == null) {
			if (other.compStrNo != null)
				return false;
		} else if (!compStrNo.equals(other.compStrNo))
			return false;
		if (itemCode == null) {
			if (other.itemCode != null)
				return false;
		} else if (!itemCode.equals(other.itemCode))
			return false;
		if (upc == null) {
			if (other.upc != null)
				return false;
		} else if (!upc.equals(other.upc))
			return false;
		return true;
	}
	
	
	public String getCompStrNo() {
		return compStrNo;
	}
	public void setCompStrNo(String compStrNo) {
		this.compStrNo = compStrNo;
	}
	public String getUpc() {
		return upc;
	}
	public void setUpc(String upc) {
		this.upc = upc;
	}
	public String getItemCode() {
		return itemCode;
	}
	public void setItemCode(String itemCode) {
		this.itemCode = itemCode;
	}

	
	
}
