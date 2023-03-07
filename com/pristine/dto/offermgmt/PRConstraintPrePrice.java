package com.pristine.dto.offermgmt;

public class PRConstraintPrePrice  implements  Cloneable{
	private long cPrePriceId;
	private long cId;
	private double value;
	private int quantity;
	
	public long getcPrePriceId() {
		return cPrePriceId;
	}
	public void setcPrePriceId(long cPrePriceId) {
		this.cPrePriceId = cPrePriceId;
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
