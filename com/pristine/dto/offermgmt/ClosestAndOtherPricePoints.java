package com.pristine.dto.offermgmt;

public class ClosestAndOtherPricePoints {
	
	private double closestPP;
	private double otherPP;
	
	public ClosestAndOtherPricePoints(double closestPP, double otherPP) {
		this.closestPP = closestPP;
		this.otherPP = otherPP;
	}
	
	public double getClosestPP() {
		return closestPP;
	}
	public void setClosestPP(double closestPP) {
		this.closestPP = closestPP;
	}
	public double getOtherPP() {
		return otherPP;
	}
	public void setOtherPP(double otherPP) {
		this.otherPP = otherPP;
	}
	
	@Override
	public int hashCode() {
		final int prime = 31;
		int result = 1;
		long temp;
		temp = Double.doubleToLongBits(closestPP);
		result = prime * result + (int) (temp ^ (temp >>> 32));
		temp = Double.doubleToLongBits(otherPP);
		result = prime * result + (int) (temp ^ (temp >>> 32));
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
		ClosestAndOtherPricePoints other = (ClosestAndOtherPricePoints) obj;
		if (Double.doubleToLongBits(closestPP) != Double.doubleToLongBits(other.closestPP))
			return false;
		if (Double.doubleToLongBits(otherPP) != Double.doubleToLongBits(other.otherPP))
			return false;
		return true;
	}
	
}
