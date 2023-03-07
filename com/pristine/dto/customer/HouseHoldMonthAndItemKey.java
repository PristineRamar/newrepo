package com.pristine.dto.customer;

public class HouseHoldMonthAndItemKey {
	private String Month;
	private String ItemId;
	public String getMonth() {
		return Month;
	}
	public void setMonth(String month) {
		Month = month;
	}
	public String getItemId() {
		return ItemId;
	}
	public void setItemId(String itemId) {
		ItemId = itemId;
	}

	
	public HouseHoldMonthAndItemKey(String month, String itemId) {
		this.Month = month;
		this.ItemId = itemId;
	}
	@Override
	public int hashCode() {
		final int prime = 31;
		int result = 1;
		result = prime * result + ((ItemId == null) ? 0 : ItemId.hashCode());
		result = prime * result + ((Month == null) ? 0 : Month.hashCode());
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
		HouseHoldMonthAndItemKey other = (HouseHoldMonthAndItemKey) obj;
		if (ItemId == null) {
			if (other.ItemId != null)
				return false;
		} else if (!ItemId.equals(other.ItemId))
			return false;
		if (Month == null) {
			if (other.Month != null)
				return false;
		} else if (!Month.equals(other.Month))
			return false;
		return true;
	}

}
