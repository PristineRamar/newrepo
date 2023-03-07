package com.pristine.dataload.gianteagle;

import com.pristine.dto.offermgmt.MultiplePrice;

public class ItemAndPriceKey {
	private int itemCode;
	private MultiplePrice regularPrice;

	public ItemAndPriceKey(int itemCode, MultiplePrice regularPrice) {
		this.itemCode = itemCode;
		this.regularPrice = regularPrice;
	}

	public int getItemCode() {
		return itemCode;
	}

	public void setItemCode(int itemCode) {
		this.itemCode = itemCode;
	}

	public MultiplePrice getRegularPrice() {
		return regularPrice;
	}

	public void setRegularPrice(MultiplePrice regularPrice) {
		this.regularPrice = regularPrice;
	}

	@Override
	public int hashCode() {
		final int prime = 31;
		int result = 1;
		result = prime * result + itemCode;
		result = prime * result + ((regularPrice == null) ? 0 : regularPrice.hashCode());
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
		ItemAndPriceKey other = (ItemAndPriceKey) obj;
		if (itemCode != other.itemCode)
			return false;
		if (regularPrice == null) {
			if (other.regularPrice != null)
				return false;
		} else if (!regularPrice.equals(other.regularPrice))
			return false;
		return true;
	}
}
