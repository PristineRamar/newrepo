package com.pristine.dto.offermgmt;

import java.text.DecimalFormat;
import java.util.ArrayList;

import com.fasterxml.jackson.annotation.JsonIgnore;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.pristine.util.Constants;
import com.pristine.util.offermgmt.PRCommonUtil;
import com.pristine.util.offermgmt.PRConstants;

import net.sf.ehcache.pool.sizeof.annotations.IgnoreSizeOf;

@IgnoreSizeOf
//public class PRRange implements Serializable{
public class PRRange {
	@JsonProperty("s-v")
	private double startVal = Constants.DEFAULT_NA;
	@JsonProperty("e-v")
	private double endVal = Constants.DEFAULT_NA;
	@JsonIgnore
	private String actualRange;
	@JsonIgnore
	private String finalRange;
	
	@JsonIgnore
	private boolean conflict;
//	@JsonIgnore
//	private boolean isHigherSideCarried = false;
//	@JsonIgnore
//	private boolean isLowerSideCarried = false;
	//@JsonIgnore
	//DecimalFormat twoDForm = new DecimalFormat("##########.##");
	
	@JsonIgnore
	public double getStartValWithNoRounding() {
		return startVal;
	}
	
	@JsonIgnore
	public double getEndValWithNoRounding() {
		return endVal;
	}
	
	public double getStartVal() {
		return Double.parseDouble(PRCommonUtil.getTwoDForm().format(startVal));
	}
	public void setStartVal(double startVal) {
		this.startVal = startVal;
	}
	public double getEndVal() {
		return Double.parseDouble(PRCommonUtil.getTwoDForm().format(endVal));
	}
	public void setEndVal(double endVal) {
		this.endVal = endVal;
	}
	public String getActualRange() {
		return actualRange;
	}
	public void setActualRange(String inputRange) {
		this.actualRange = inputRange;
	}
	public String getFinalRange() {
		return finalRange;
	}
	public void setFinalRange(String finalRange) {
		this.finalRange = finalRange;
	}
	public boolean isConflict() {
		return conflict;
	}
	public void setConflict(boolean conflict) {
		this.conflict = conflict;
	}

	
	/**
	 * Returns Range of numbers between startVal and endVal that ends with the input roundingDigits
	 * @param roundingDigits
	 * @return
	 */
	public double[] getRange(PRConstraintRounding roundingConstraint){
		double[] priceRange = roundingConstraint.getRange(startVal, endVal, PRConstants.ROUND_CLOSEST);
		this.setFinalRange(getPriceRangeString(priceRange));
		return priceRange;
	}
	
	/**
	 * Returns Range of numbers between startVal and endVal that ends with the input roundingDigits that is a subset
	 * of numbers in input range
	 * @param roundingConstraint
	 * @param inputRange
	 * @return
	 */
	public double[] getRange(PRConstraintRounding roundingConstraint, double[] inputRange){
		double[] newRange = roundingConstraint.getRange(startVal, endVal, PRConstants.ROUND_CLOSEST);
		this.setActualRange(getPriceRangeString(newRange));
		ArrayList<Double> subset = new ArrayList<Double>();
		ArrayList<Double> oldRangeList = new ArrayList<Double>();
		for(Double price : inputRange){
			oldRangeList.add(price);
		}
		
		for(Double price : newRange){
			if(oldRangeList.contains(price)){
				subset.add(price);
			}
		}
		
		double[] subsetArr = new double[subset.size()]; 
		int counter = 0;
		for (Double price : subset){
			subsetArr[counter] = price;
			counter++;
		}
		this.setFinalRange(getPriceRangeString(subsetArr));
		return subsetArr;
	}
	
	public PRRange getRange(PRRange inputRange){
		PRRange outputRange = new PRRange();
		if(inputRange.startVal != Constants.DEFAULT_NA){
			if(inputRange.endVal != Constants.DEFAULT_NA){
				if(this.startVal != Constants.DEFAULT_NA)
					if(this.startVal >= inputRange.startVal && this.startVal <= inputRange.endVal)
						outputRange.startVal = this.startVal;
					else
						outputRange.startVal = inputRange.startVal;
				else
					outputRange.startVal = inputRange.startVal;
		
				if(this.endVal != Constants.DEFAULT_NA)
					if(this.endVal >= inputRange.startVal && this.endVal <= inputRange.endVal)
						outputRange.endVal = this.endVal;
					else
						outputRange.endVal = inputRange.endVal;
				else
					outputRange.endVal = inputRange.endVal;
			}else{
				if(this.startVal != Constants.DEFAULT_NA)
					if(this.startVal >= inputRange.startVal)
						outputRange.startVal = this.startVal;
					else
						outputRange.startVal = inputRange.startVal;
				else
					outputRange.startVal = inputRange.startVal;
				
				
				if(this.endVal != Constants.DEFAULT_NA)
					if(this.endVal >= inputRange.startVal)
						outputRange.endVal = this.endVal;
			}
		}else{
			if(inputRange.endVal != Constants.DEFAULT_NA){
				if(this.startVal != Constants.DEFAULT_NA)
					if(this.startVal <= inputRange.endVal)
						outputRange.startVal = this.startVal;
		
				if(this.endVal != Constants.DEFAULT_NA)
					if(this.endVal <= inputRange.endVal)
						outputRange.endVal = this.endVal;
					else
						outputRange.endVal = inputRange.endVal;
				else
					outputRange.endVal = inputRange.endVal;
			}else{
				outputRange.startVal = this.startVal;
				outputRange.endVal = this.endVal;
			}
		}
		return outputRange;
	}
	
	/**
	 * Returns price range as a String
	 * @param priceRange
	 * @return
	 */
	private String getPriceRangeString(double[] priceRange){
		StringBuffer priceRangeStr = new StringBuffer();
		int count = 0;
		for(double price : priceRange){
			if(count == 0){
				priceRangeStr.append(price);
				count++;
			}else
				priceRangeStr.append(", " + price);
			
		}
		return priceRangeStr.toString();
	}
	
	@Override
	public String toString(){
		DecimalFormat format = new DecimalFormat("######.##"); 
		StringBuffer rangeStr = new StringBuffer();
		if(this.startVal != Constants.DEFAULT_NA){
			rangeStr.append(format.format(this.startVal));
		}else{
			rangeStr.append("-");
		}
		if(this.endVal != Constants.DEFAULT_NA){
			rangeStr.append(" ");
			rangeStr.append(format.format(this.endVal));
		}else{
			rangeStr.append(" ");
			rangeStr.append("-");
		}
		return rangeStr.toString();
	}
	
	public void copy(PRRange range){
		if(range != null){
			this.startVal = range.getStartVal();
			this.endVal = range.getEndVal();
		}
	}

//	public boolean getIsHigherSideCarried() {
//		return isHigherSideCarried;
//	}
//
//	public void setIsHigherSideCarried(boolean isHigherSideCarried) {
//		this.isHigherSideCarried = isHigherSideCarried;
//	}
//
//	public boolean getIsLowerSideCarried() {
//		return isLowerSideCarried;
//	}
//
//	public void setIsLowerSideCarried(boolean isLowerSideCarried) {
//		this.isLowerSideCarried = isLowerSideCarried;
//	}
}
