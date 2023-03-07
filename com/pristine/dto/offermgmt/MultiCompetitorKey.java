package com.pristine.dto.offermgmt;

public class MultiCompetitorKey {

	private int compStrId;
	private int itemCode;
	
	public MultiCompetitorKey(int compStrId, int itemCode){
		this.compStrId = compStrId;
		this.itemCode = itemCode;
	}
	
	public int getCompStrId() {
		return compStrId;
	}
	public void setCompStrId(int compStrId) {
		this.compStrId = compStrId;
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
		result = prime * result + compStrId;
		result = prime * result + itemCode;
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
		MultiCompetitorKey other = (MultiCompetitorKey) obj;
		if (compStrId != other.compStrId)
			return false;
		if (itemCode != other.itemCode)
			return false;
		return true;
	}
	
	
	
}
