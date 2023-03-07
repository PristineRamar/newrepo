package com.pristine.dto.offermgmt;

public class PRConstraintCost  implements  Cloneable{
	private long cCostId;
	private long cId;
	private boolean isRecBelowCost =false;
	
	public PRConstraintCost(){
		this.isRecBelowCost = false;
	}
	
	public long getcCostId() {
		return cCostId;
	}
	public void setcCostId(long cCostId) {
		this.cCostId = cCostId;
	}
	public long getcId() {
		return cId;
	}
	public void setcId(long cId) {
		this.cId = cId;
	}
	public boolean getIsRecBelowCost() {
		return isRecBelowCost;
	}
	public void setIsRecBelowCost(boolean isRecBelowCost) {
		this.isRecBelowCost = isRecBelowCost;
	}
	
	@Override
    protected Object clone() throws CloneNotSupportedException {
        return super.clone();
    }
}
