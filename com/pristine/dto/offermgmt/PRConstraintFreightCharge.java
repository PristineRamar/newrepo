package com.pristine.dto.offermgmt;

public class PRConstraintFreightCharge implements  Cloneable {
	
	
	private long constraintID;
	private long strategyID;
	private int  constraintTypeID;
	private boolean isfreightcharge;
	
	
	public long getConstraintID() {
		return constraintID;
	}
	public void setConstraintID(long constraintID) {
		this.constraintID = constraintID;
	}
	public long getStrategyID() {
		return strategyID;
	}
	public void setStrategyID(long strategyID) {
		this.strategyID = strategyID;
	}
	public int getConstraintTypeID() {
		return constraintTypeID;
	}
	public void setConstraintTypeID(int constraintTypeID) {
		this.constraintTypeID = constraintTypeID;
	}
	public boolean isIsfreightcharge() {
		return isfreightcharge;
	}
	public void setIsfreightcharge(boolean isfreightcharge) {
		this.isfreightcharge = isfreightcharge;
	}
	
	

	
	

}
