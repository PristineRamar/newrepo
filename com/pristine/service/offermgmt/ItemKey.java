package com.pristine.service.offermgmt;

public class ItemKey {
	private int itemCodeOrRetLirId;
	private int lirIndicator = 0;
	
	public ItemKey(int itemCodeOrRetLirId, int lirIndicator) {
		super();
		this.itemCodeOrRetLirId = itemCodeOrRetLirId;
		this.lirIndicator = lirIndicator;
	}

	public int getItemCodeOrRetLirId() {
		return itemCodeOrRetLirId;
	}

	public void setItemCodeOrRetLirId(int itemCodeOrRetLirId) {
		this.itemCodeOrRetLirId = itemCodeOrRetLirId;
	}

	public int getLirIndicator() {
		return lirIndicator;
	}

	public void setLirIndicator(int lirIndicator) {
		this.lirIndicator = lirIndicator;
	}

	@Override
	public int hashCode() {
		final int prime = 31;
		int result = 1;
		result = prime * result + itemCodeOrRetLirId;
		result = prime * result + lirIndicator;
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
		ItemKey other = (ItemKey) obj;
		if (itemCodeOrRetLirId != other.itemCodeOrRetLirId)
			return false;
		if (lirIndicator != other.lirIndicator)
			return false;
		return true;
	}

	@Override
	public String toString() {
		return "[" + itemCodeOrRetLirId + "-" + lirIndicator + "]";
	}
	
	
}
