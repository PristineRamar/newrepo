package com.pristine.dto.offermgmt;

import com.pristine.util.offermgmt.PRConstants;

public class PRConstraintLowerHigher  implements  Cloneable{
	private long cLHId;
	private long cId;
	private char lowerHigherRetailFlag;
	
	public PRConstraintLowerHigher() {
		this.lowerHigherRetailFlag = PRConstants.CONSTRAINT_NO_LOWER_HIGHER;
	}
	
	public long getcLHId() {
		return cLHId;
	}
	public void setcLHId(long cLHId) {
		this.cLHId = cLHId;
	}
	public long getcId() {
		return cId;
	}
	public void setcId(long cId) {
		this.cId = cId;
	}

	public char getLowerHigherRetailFlag() {
		return lowerHigherRetailFlag;
	}

	public void setLowerHigherRetailFlag(char lowerHigherRetailFlag) {
		this.lowerHigherRetailFlag = lowerHigherRetailFlag;
	}	
	
	@Override
    protected Object clone() throws CloneNotSupportedException {
        return super.clone();
    }
}
