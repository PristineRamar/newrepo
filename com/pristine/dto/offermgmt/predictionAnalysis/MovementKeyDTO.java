package com.pristine.dto.offermgmt.predictionAnalysis;

public class MovementKeyDTO {

	private long productId;
	private int calendarId;

	public MovementKeyDTO(long productId, int calendarId) {
		this.calendarId = calendarId;
		this.productId = productId;
	}

	@Override
	public int hashCode() {
		final int prime = 31;
		int result = 1;
		result = prime * result + calendarId;
		result = prime * result + (int) (productId ^ (productId >>> 32));
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
		MovementKeyDTO other = (MovementKeyDTO) obj;
		if (calendarId != other.calendarId)
			return false;
		if (productId != other.productId)
			return false;
		return true;
	}

}
