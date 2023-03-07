package com.pristine.dto.offermgmt;

public class PRConstraintLocPrice  implements  Cloneable{
	private long cLocPriceId;
	private long cId;
	private double value;
	private int quantity;
	
	public long getcLocPriceId() {
		return cLocPriceId;
	}
	public void setcLocPriceId(long cLocPriceId) {
		this.cLocPriceId = cLocPriceId;
	}
	public long getcId() {
		return cId;
	}
	public void setcId(long cId) {
		this.cId = cId;
	}
	public double getValue() {
		return value;
	}
	public void setValue(double value) {
		this.value = value;
	}
	public int getQuantity() {
		return quantity;
	}
	public void setQuantity(int quantity) {
		this.quantity = quantity;
	}
	
	@Override
    protected Object clone() throws CloneNotSupportedException {
        return super.clone();
    }
}
