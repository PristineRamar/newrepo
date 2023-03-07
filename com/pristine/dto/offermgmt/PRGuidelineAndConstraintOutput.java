package com.pristine.dto.offermgmt;

public class PRGuidelineAndConstraintOutput {

	public PRRange outputPriceRange;
	public boolean isBreakThreshold;
	public boolean isGuidelineApplied;
	//public boolean isConflict;	
	//public boolean isIgnorePriceRangeAsMarginApplied;
	//public boolean isIgnorePriceRangeAsStoreBrandApplied;
	
	public PRGuidelineAndConstraintOutput() {
		//Assign defaults
		this.outputPriceRange = new PRRange();
		this.isBreakThreshold = false;
		this.isGuidelineApplied = false;
		//this.isConflict = false;
		//this.isIgnorePriceRangeAsMarginApplied = false;
		//this.isIgnorePriceRangeAsStoreBrandApplied =false;
	}
}
