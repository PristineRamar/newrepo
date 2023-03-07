package com.pristine.dto.offermgmt;

//import java.io.Serializable;
import java.text.DecimalFormat;
import java.util.ArrayList;
import java.util.List;

//import net.sf.ehcache.pool.sizeof.annotations.IgnoreSizeOf;

import com.fasterxml.jackson.annotation.JsonIgnore;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.pristine.util.offermgmt.PRFormatHelper;

public class PRGuidelineAndConstraintLog  {
	DecimalFormat format = new DecimalFormat("######.##"); 
	
	@JsonProperty("g-t-id")
	private int guidelineTypeId;
	@JsonProperty("c-t-id")
	private int constraintTypeId;
	@JsonProperty("conflict")
	private boolean isConflict;
	@JsonProperty("applied")
	private boolean isGuidelineOrConstraintApplied;
	
	@JsonProperty("pr1")
	private PRRange guidelineOrConstraintPriceRange1;
	@JsonProperty("pr2")
	private PRRange guidelineOrConstraintPriceRange2;
	@JsonProperty("opr")
	private PRRange outputPriceRange;
	
	@JsonProperty("c-d")
	private List<PRCompDetailLog> compDetails;
	
	//For log
	@JsonProperty("r1")
	private PRRangeForLog range1;
	@JsonProperty("r2")
	private PRRangeForLog range2;
	@JsonProperty("o-r")
	private PRRangeForLog outputRange;
	
	@JsonProperty("msg")
	private String message;

	//Used for pre-price, locked-price
	@JsonProperty("r-m")
	private Integer recommendedRegMultiple;
	@JsonProperty("r-p")
	private Double recommendedRegPrice;
	
	//Used in Brand/Size Relation
	@JsonProperty("lir")
	private boolean isLirInd;
	@JsonProperty("r-i-c")
	private int relationItemCode;
	@JsonProperty("r-i-s")
	private String relatedItemSize;
	@JsonProperty("r-u-n")
	private String relatedUOMName;
	@JsonProperty("r-t")
	private String relationText;
	@JsonIgnore
	private String operatorText;
	@JsonProperty("r-pe")
	private MultiplePrice relatedPrice;
	
	@JsonProperty("gr-c-d")
	private List<PRGuardRailLog> guardRailList;
	
	//Used in Rounding
	@JsonProperty("r-d")
	private List<Double> roundingDigits; 
	
	//Used in LIG Constraint
	@JsonProperty("l-c")
	private boolean isSameForAllLigMember;
	
	@JsonIgnore
	private boolean isBreakingConstraint = false;
	
	//NU::26th Jul 2016, to save the comp price used in price index in the log
	//To solve below issue: if a lig has different comp price, then most common price is shown
	//at lig level and explain log will be the representing item's explain log
	//if the representing item comp is different from lig comp price, then
	//the guideline range doesn't looking good as lig comp price is shown
	@JsonProperty("i-c-p")
	private MultiplePrice indexCompPrice;
	
	public PRGuidelineAndConstraintLog(){
		this.isConflict = false;
		this.isGuidelineOrConstraintApplied = false;
		this.guidelineOrConstraintPriceRange1 = new PRRange();
		this.guidelineOrConstraintPriceRange2 = new PRRange();
		this.outputPriceRange = new PRRange();
		this.message = "";
		this.isLirInd =false;
		this.roundingDigits = new ArrayList<Double>(); 
		this.isSameForAllLigMember = true;
		this.range1 = new PRRangeForLog();
		this.range2 = new PRRangeForLog();
		this.outputRange = new PRRangeForLog();
		this.compDetails = new ArrayList<PRCompDetailLog>();
		this.relationText = "";
		this.operatorText = "";
		this.relatedItemSize = "";
		this.relatedUOMName = "";
		this.isBreakingConstraint = false;
		this.indexCompPrice = null;
		this.guardRailList= new ArrayList<PRGuardRailLog>();
	}

	public Integer getRecommendedRegMultiple() {
		return recommendedRegMultiple;
	}

	public void setRecommendedRegMultiple(Integer recommendedRegMultiple) {
		this.recommendedRegMultiple = recommendedRegMultiple;
	}

	public Double getRecommendedRegPrice() {
		return recommendedRegPrice;
	}

	public void setRecommendedRegPrice(Double recommendedRegPrice) {
		this.recommendedRegPrice = recommendedRegPrice;
	}
	
	public String getMessage() {
		return message;
	}

	public void setMessage(String message) {
		this.message = message;
	}
	
	public int getGuidelineTypeId() {
		return guidelineTypeId;
	}

	public void setGuidelineTypeId(int guidelineTypeId) {
		this.guidelineTypeId = guidelineTypeId;
	}

	public int getConstraintTypeId() {
		return constraintTypeId;
	}

	public void setConstraintTypeId(int constraintTypeId) {
		this.constraintTypeId = constraintTypeId;
	}
	
	public boolean getIsConflict() {
		return isConflict;
	}

	public void setIsConflict(boolean isConflict) {
		this.isConflict = isConflict;
	}

	public boolean getIsGuidelineOrConstraintApplied() {
		return isGuidelineOrConstraintApplied;
	}

	public void setIsGuidelineOrConstraintApplied(boolean isGuidelineOrConstraintApplied) {
		this.isGuidelineOrConstraintApplied = isGuidelineOrConstraintApplied;
	}
	
	public PRRange getOutputPriceRange() {
		return outputPriceRange;
	}

	public void setOutputPriceRange(PRRange outputPriceRange) {
		this.outputPriceRange = outputPriceRange;
	}

	public boolean getIsLirInd() {
		return isLirInd;
	}

	public void setIsLirInd(boolean isLirInd) {
		this.isLirInd = isLirInd;
	}

	public PRRange getGuidelineOrConstraintPriceRange1() {
		return guidelineOrConstraintPriceRange1;
	}

	public void setGuidelineOrConstraintPriceRange1(PRRange guidelineOrConstraintPriceRange1) {
		this.guidelineOrConstraintPriceRange1 = guidelineOrConstraintPriceRange1;
	}

	public PRRange getGuidelineOrConstraintPriceRange2() {
		return guidelineOrConstraintPriceRange2;
	}

	public void setGuidelineOrConstraintPriceRange2(PRRange guidelineOrConstraintPriceRange2) {
		this.guidelineOrConstraintPriceRange2 = guidelineOrConstraintPriceRange2;
	}

	public int getRelationItemCode() {
		return relationItemCode;
	}

	public void setRelationItemCode(int relationItemCode) {
		this.relationItemCode = relationItemCode;
	}

	public List<Double> getRoundingDigits() {
		return roundingDigits;
	}

	public void setRoundingDigits(List<Double> roundingDigits) {
		this.roundingDigits = roundingDigits;
	}
	
	public boolean getIsSameForAllLigMember() {
		return isSameForAllLigMember;
	}

	public void setIsSameForAllLigMember(boolean isSameForAllLigMember) {
		this.isSameForAllLigMember = isSameForAllLigMember;
	}

	public List<PRCompDetailLog> getCompDetails() {
		return compDetails;
	}

	public void setCompDetails(List<PRCompDetailLog> compDetails) {
		this.compDetails = compDetails;
	}

	public String getRelationText() {
		return relationText;
	}

	public void setRelationText(String relationText) {
		this.relationText = relationText;
	} 
	
	public PRRangeForLog getRange1() {
		PRFormatHelper.formatRangeForLog(this.getGuidelineOrConstraintPriceRange1(), this.range1);
		return this.range1;
	}
	
	public PRRangeForLog getRange2() {
		PRFormatHelper.formatRangeForLog(this.getGuidelineOrConstraintPriceRange2(), this.range2);
		return this.range2;
	}

	public PRRangeForLog getOutputRange() {
		PRFormatHelper.formatRangeForLog(this.getOutputPriceRange(), this.outputRange);
		return this.outputRange;
	}
	
	public String getOperatorText() {
		return operatorText;
	}

	public void setOperatorText(String operatorText) {
		this.operatorText = operatorText;
	}

	public String getRelatedItemSize() {
		return relatedItemSize;
	}

	public void setRelatedItemSize(String relatedItemSize) {
		this.relatedItemSize = relatedItemSize;
	}

	public String getRelatedUOMName() {
		return relatedUOMName;
	}

	public void setRelatedUOMName(String relatedUOMName) {
		this.relatedUOMName = relatedUOMName;
	}

	public boolean getIsBreakingConstraint() {
		return isBreakingConstraint;
	}

	public void setIsBreakingConstraint(boolean isBreakingConstraint) {
		this.isBreakingConstraint = isBreakingConstraint;
	}

	public MultiplePrice getRelatedPrice() {
		return relatedPrice;
	}

	public void setRelatedPrice(MultiplePrice relatedPrice) {
		this.relatedPrice = relatedPrice;
	}
	
	
	

	public List<PRGuardRailLog> getGuardRailList() {
		return guardRailList;
	}

	public void setGuardRailList(List<PRGuardRailLog> guardRailList) {
		this.guardRailList = guardRailList;
	}

	@Override
	public int hashCode() {
		final int prime = 31;
		int result = 1;
		result = prime * result + constraintTypeId;
		result = prime * result + ((guidelineOrConstraintPriceRange1 == null) ? 0 : guidelineOrConstraintPriceRange1.toString().hashCode());
		result = prime * result + ((guidelineOrConstraintPriceRange2 == null) ? 0 : guidelineOrConstraintPriceRange2.toString().hashCode());
		result = prime * result + guidelineTypeId;
		result = prime * result + (isConflict ? 1231 : 1237);
		result = prime * result + (isGuidelineOrConstraintApplied ? 1231 : 1237);
		result = prime * result + (isLirInd ? 1231 : 1237);
		result = prime * result + ((message == null) ? 0 : message.hashCode());
		result = prime * result + ((outputPriceRange == null) ? 0 : outputPriceRange.toString().hashCode());
		result = prime * result + ((recommendedRegMultiple == null) ? 0 : recommendedRegMultiple.hashCode());
		result = prime * result + ((recommendedRegPrice == null) ? 0 : recommendedRegPrice.hashCode());
		result = prime * result + relationItemCode;
		result = prime * result + ((roundingDigits == null) ? 0 : roundingDigits.hashCode());
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
		PRGuidelineAndConstraintLog other = (PRGuidelineAndConstraintLog) obj;
		if (constraintTypeId != other.constraintTypeId)
			return false;
		if (guidelineOrConstraintPriceRange1 == null) {
			if (other.guidelineOrConstraintPriceRange1 != null)
				return false;
		} else if (!guidelineOrConstraintPriceRange1.toString().equals(other.guidelineOrConstraintPriceRange1.toString()))
			return false;
		if (guidelineOrConstraintPriceRange2 == null) {
			if (other.guidelineOrConstraintPriceRange2 != null)
				return false;
		} else if (!guidelineOrConstraintPriceRange2.toString().equals(other.guidelineOrConstraintPriceRange2.toString()))
			return false;
		if (guidelineTypeId != other.guidelineTypeId)
			return false;
		if (isConflict != other.isConflict)
			return false;
		if (isGuidelineOrConstraintApplied != other.isGuidelineOrConstraintApplied)
			return false;
		if (isLirInd != other.isLirInd)
			return false;
		if (message == null) {
			if (other.message != null)
				return false;
		} else if (!message.equals(other.message))
			return false;
		if (outputPriceRange == null) {
			if (other.outputPriceRange != null)
				return false;
		} else if (!outputPriceRange.toString().equals(other.outputPriceRange.toString()))
			return false;
		if (recommendedRegMultiple == null) {
			if (other.recommendedRegMultiple != null)
				return false;
		} else if (!recommendedRegMultiple.equals(other.recommendedRegMultiple))
			return false;
		if (recommendedRegPrice == null) {
			if (other.recommendedRegPrice != null)
				return false;
		} else if (!recommendedRegPrice.equals(other.recommendedRegPrice))
			return false;
		if (relationItemCode != other.relationItemCode)
			return false;
		if (roundingDigits == null) {
			if (other.roundingDigits != null)
				return false;
		} else if (!roundingDigits.equals(other.roundingDigits))
			return false;
		return true;
	}

	public MultiplePrice getIndexCompPrice() {
		if(this.indexCompPrice != null) {
			this.indexCompPrice.price = Double.valueOf(PRFormatHelper.doubleToTwoDigitString(this.indexCompPrice.price));
		}
		return indexCompPrice;
	}

	public void setIndexCompPrice(MultiplePrice indexCompPrice) {
		this.indexCompPrice = indexCompPrice;
	}
}
