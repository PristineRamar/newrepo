package com.pristine.dto.offermgmt;

public class RoundingRangeKey {
	private Double startPrice;
	private Double endPrice;

	public RoundingRangeKey(Double startPrice, Double endPrice) {
		super();
		this.startPrice = startPrice;
		this.endPrice = endPrice;
	}

	public Double getStartPrice() {
		return startPrice;
	}

	public void setStartPrice(Double startPrice) {
		this.startPrice = startPrice;
	}

	public Double getEndPrice() {
		return endPrice;
	}

	public void setEndPrice(Double endPrice) {
		this.endPrice = endPrice;
	}

	@Override
	public int hashCode() {
		final int prime = 31;
		int result = 1;
		result = prime * result + ((endPrice == null) ? 0 : endPrice.hashCode());
		result = prime * result + ((startPrice == null) ? 0 : startPrice.hashCode());
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
		RoundingRangeKey other = (RoundingRangeKey) obj;
		if (endPrice == null) {
			if (other.endPrice != null)
				return false;
		} else if (!endPrice.equals(other.endPrice))
			return false;
		if (startPrice == null) {
			if (other.startPrice != null)
				return false;
		} else if (!startPrice.equals(other.startPrice))
			return false;
		return true;
	}

	@Override
	public String toString() {
		return "RoundingRangeKey [startPrice=" + startPrice + ", endPrice=" + endPrice + "]";
	}
	
	
}
