
package com.pristine.dto.offermgmt;


//public class PRConstraintsDTO implements Serializable, Cloneable{
public class PRConstraintsDTO implements Cloneable{
//	private PRConstraintPrePrice prePriceConstraint = null;
	private PRConstraintLocPrice locPriceConstraint = null;
	private PRConstraintThreshold thresholdConstraint = null;
	private PRConstraintRounding roundingConstraint = null;
	private PRConstraintMinMax minMaxConstraint = null;
	private PRConstraintLIG ligConstraint = null;
	private PRConstraintCost costConstraint = null;
	private PRConstraintLowerHigher lowerHigherConstraint = null;
	private PRConstraintGuardrail guardrailConstraint = null;
	private PRConstraintFreightCharge freightChargeConstraint= null;
//	public PRConstraintPrePrice getPrePriceConstraint() {
//		return prePriceConstraint;
//	}
//	public void setPrePriceConstraint(PRConstraintPrePrice prePriceConstraint) {
//		this.prePriceConstraint = prePriceConstraint;
//	}
	public PRConstraintLocPrice getLocPriceConstraint() {
		return locPriceConstraint;
	}
	public void setLocPriceConstraint(PRConstraintLocPrice locPriceConstraint) {
		this.locPriceConstraint = locPriceConstraint;
	}
	public PRConstraintThreshold getThresholdConstraint() {
		return thresholdConstraint;
	}
	public void setThresholdConstraint(PRConstraintThreshold thresholdConstraint) {
		this.thresholdConstraint = thresholdConstraint;
	}
	public PRConstraintRounding getRoundingConstraint() {
		return roundingConstraint;
	}
	public void setRoundingConstraint(PRConstraintRounding roundingConstraint) {
		this.roundingConstraint = roundingConstraint;
	}
	public PRConstraintMinMax getMinMaxConstraint() {
		return minMaxConstraint;
	}
	public void setMinMaxConstraint(PRConstraintMinMax minMaxConstraint) {
		this.minMaxConstraint = minMaxConstraint;
	}
	public PRConstraintLIG getLigConstraint() {
		return ligConstraint;
	}
	public void setLigConstraint(PRConstraintLIG ligConstraint) {
		this.ligConstraint = ligConstraint;
	}
	public PRConstraintCost getCostConstraint() {
		return costConstraint;
	}
	public void setCostConstraint(PRConstraintCost costConstraint) {
		this.costConstraint = costConstraint;
	}
	public PRConstraintLowerHigher getLowerHigherConstraint() {
		return lowerHigherConstraint;
	}
	public void setLowerHigherConstraint(PRConstraintLowerHigher lowerHigherConstraint) {
		this.lowerHigherConstraint = lowerHigherConstraint;
	}
	 
	@Override
	protected Object clone() throws CloneNotSupportedException {
		PRConstraintsDTO cloned = (PRConstraintsDTO)super.clone();
//		if (cloned.getPrePriceConstraint() != null)
//			cloned.setPrePriceConstraint((PRConstraintPrePrice) cloned.getPrePriceConstraint().clone());
		
		if (cloned.getLocPriceConstraint() != null)
			cloned.setLocPriceConstraint((PRConstraintLocPrice) cloned.getLocPriceConstraint().clone());
		
		if (cloned.getMinMaxConstraint() != null)
			cloned.setMinMaxConstraint((PRConstraintMinMax) cloned.getMinMaxConstraint().clone());
		
		if (cloned.getThresholdConstraint() != null)
			cloned.setThresholdConstraint((PRConstraintThreshold) cloned.getThresholdConstraint().clone());
		
		if (cloned.getCostConstraint() != null)
			cloned.setCostConstraint((PRConstraintCost) cloned.getCostConstraint().clone());
		
		if (cloned.getLigConstraint() != null)
			cloned.setLigConstraint((PRConstraintLIG) cloned.getLigConstraint().clone());
		
		if (cloned.getLowerHigherConstraint() != null)
			cloned.setLowerHigherConstraint((PRConstraintLowerHigher) cloned.getLowerHigherConstraint().clone());
		
		if (cloned.getRoundingConstraint() != null)
			cloned.setRoundingConstraint((PRConstraintRounding) cloned.getRoundingConstraint().clone());
		
		return cloned;
	}
	public PRConstraintGuardrail getGuardrailConstraint() {
		return guardrailConstraint;
	}
	public void setGuardrailConstraint(PRConstraintGuardrail guardrailConstraint) {
		this.guardrailConstraint = guardrailConstraint;
	}
	public PRConstraintFreightCharge getFreightChargeConstraint() {
		return freightChargeConstraint;
	}
	public void setFreightChargeConstraint(PRConstraintFreightCharge freightChargeConstraint) {
		this.freightChargeConstraint = freightChargeConstraint;
	}
	
}