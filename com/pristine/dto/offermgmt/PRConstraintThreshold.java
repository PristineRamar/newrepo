package com.pristine.dto.offermgmt;

import java.text.DecimalFormat;
import java.util.HashSet;
import java.util.List;
import java.util.Set;



import com.pristine.service.offermgmt.ConstraintTypeLookup;
import com.pristine.service.offermgmt.FilterPriceRange;
import com.pristine.service.offermgmt.GuidelineTypeLookup;
import com.pristine.service.offermgmt.guideline.BrandGuideline;
import com.pristine.util.Constants;
import com.pristine.util.PropertyManager;
import com.pristine.util.offermgmt.PRConstants;

public class PRConstraintThreshold  implements  Cloneable{
	
	private String roundingDigitLogic = PropertyManager.getProperty("PR_COST_CHANGE_ROUNDING_DIGIT_BEHAVIOR", "");
	
	private long cThresholdId;
	private long cId;
	private char valueType;
	private double minValue = Constants.DEFAULT_NA;
	private double maxValue = Constants.DEFAULT_NA;
	private double maxValue2 = Constants.DEFAULT_NA;
	private String priceRange;
	private String conflictLog;
	private int isConflict;
	private String thresholdLog;
	private PRRange priceRangeToRounding = null;
	private String thresholdRange = "";
	private char overrideThreshold;
	
	public String getThresholdRange() {
		return thresholdRange;
	}
	public void setThresholdRange(String thresholdRange) {
		this.thresholdRange = thresholdRange;
	}
	public long getcThresholdId() {
		return cThresholdId;
	}
	public void setcThresholdId(long cThresholdId) {
		this.cThresholdId = cThresholdId;
	}
	public long getcId() {
		return cId;
	}
	public void setcId(long cId) {
		this.cId = cId;
	}
	public char getValueType() {
		return valueType;
	}
	public void setValueType(char valueType) {
		this.valueType = valueType;
	}
	public double getMinValue() {
		return minValue;
	}
	public void setMinValue(double minValue) {
		this.minValue = minValue;
	}
	public double getMaxValue() {
		return maxValue;
	}
	public void setMaxValue(double maxValue) {
		this.maxValue = maxValue;
	}
	public double getMaxValue2() {
		return maxValue2;
	}
	public void setMaxValue2(double maxValue) {
		this.maxValue2 = maxValue;
	}
	public String getPriceRange() {
		return priceRange;
	}
	public void setPriceRange(String priceRange) {
		this.priceRange = priceRange;
	}
	public PRRange getPriceRangeToRounding() {
		return priceRangeToRounding;
	}
	public void setPriceRangeToRounding(PRRange priceRangeToRounding) {
		this.priceRangeToRounding = priceRangeToRounding;
	}
			
	public char getOverrideThreshold() {
		return overrideThreshold;
	}
	public void setOverrideThreshold(char overrideThreshold) {
		this.overrideThreshold = overrideThreshold;
	}
	public Double[] getPriceRange(double price, PRRange inputRange, PRConstraintRounding rConstraintDTO,
			PRItemDTO itemInfo, PRExplainLog explainLog){
		Double[] range = null;
		 
		range = getPriceRangeWithThreshold(price, inputRange, rConstraintDTO, itemInfo, explainLog);
		 
		this.setPriceRange(getPriceRangeString(range));
		return range;
	}
	
	public PRGuidelineAndConstraintOutput applyThreshold(PRItemDTO itemInfo, PRRange inputPriceRange, PRExplainLog explainLog){
		PRGuidelineAndConstraintOutput output = new PRGuidelineAndConstraintOutput();		
		PRGuidelineAndConstraintLog guidelineAndConstraintLog = new PRGuidelineAndConstraintLog();
		FilterPriceRange filterPriceRange = new FilterPriceRange();	
		PRRange filteredPriceRange;
		PRRange threshRange = new PRRange();
//		DecimalFormat df = new DecimalFormat("###.##");
		
		output.outputPriceRange = inputPriceRange;
		guidelineAndConstraintLog.setConstraintTypeId(ConstraintTypeLookup.THRESHOLD.getConstraintTypeId());		
		PRConstraintThreshold thresholdConstraint=null;
		if (itemInfo.getRegPrice() != null) {
			threshRange = getThresholdRange(itemInfo);
			if (itemInfo.getStrategyDTO() != null) {
				 thresholdConstraint = itemInfo.getStrategyDTO().getConstriants()
						.getThresholdConstraint();
			}

			if (
					itemInfo.getCostChangeBehavior().equals(PRConstants.COST_CHANGE_AHOLD_MARGIN) || 
					itemInfo.getCostChangeBehavior().equals(PRConstants.COST_CHANGE_AHOLD_STORE_BRAND)) {
				filteredPriceRange = filterPriceRange.filterRange(inputPriceRange, threshRange);
			} else {
				
				//NU:: 5th Jul 2017, this is implemented when objective "Maximize Margin $ while maintaining current Movement (Units)" and 
				//guideline "item level maintain penny profit on cost change" is introduced,
				//If there is cost increase and if the threshold prevents the price increase, then relax
				//threshold so that it can allow price to go up
				boolean isMarginGuidelineApplied = filterPriceRange.checkIfGuidelineApplied(explainLog,
						GuidelineTypeLookup.MARGIN.getGuidelineTypeId());
				PRRange marginRange = filterPriceRange.getGuidelineRange(explainLog, GuidelineTypeLookup.MARGIN.getGuidelineTypeId());

				if (isMarginGuidelineApplied && marginRange.getStartVal() != Constants.DEFAULT_NA
						&& itemInfo.getCostChgIndicator() == PRConstants.COST_INCREASE
						&& marginRange.getStartVal() > threshRange.getEndVal() && thresholdConstraint != null
						&& thresholdConstraint.getOverrideThreshold() == 'Y') {
//					logger.debug("Threshold end range altered from:" + threshRange.getEndVal() + " to: " + marginRange.getStartVal() + " for item :"
//							+ itemInfo.getItemCode());
					threshRange.setEndVal(marginRange.getStartVal());
				}

				//Added by Karishma ,this is implemented to fix issues in RA 
				boolean  isMinMaxApplied=filterPriceRange.checkIfConstraintApplied(explainLog, ConstraintTypeLookup.MIN_MAX.getConstraintTypeId());			
				PRRange minMaxRange = filterPriceRange.getConstraintRange(explainLog, ConstraintTypeLookup.MIN_MAX.getConstraintTypeId());

				if (isMinMaxApplied) {

					PRRange newRange = new PRRange();

					boolean isConflict = FilterPriceRange.checkIfOutsideRange(threshRange, minMaxRange, newRange);

					if (isConflict) {
						filteredPriceRange = filterPriceRange.filterRange(inputPriceRange, threshRange);
					} else
						filteredPriceRange = filterPriceRange.filterRange(threshRange, inputPriceRange);

				} else {

					// Don't break threshold
					filteredPriceRange = filterPriceRange.filterRange(threshRange, inputPriceRange);
				}
				
				if (filteredPriceRange.isConflict()) {
					// Find round up/round down
					itemInfo.setRoundingLogic(filterPriceRange.findRoudingLogic(filteredPriceRange, threshRange));
					guidelineAndConstraintLog.setIsBreakingConstraint(true);
				} else {
					// (make sure rounding down doesn't break Threshold constraint)
					double newRangeArr[] = itemInfo.getStrategyDTO().getConstriants().getRoundingConstraint()
							.getRange(filteredPriceRange.getStartVal(), filteredPriceRange.getEndVal(), itemInfo.getRoundingLogic());
					
					//Check if the rounding digits are within the range					
					List<Double> newRangeList = filterPriceRange.getRoundingDigitsWithinRange(newRangeArr, threshRange);				 
					if (newRangeList.size() == 0) {
						itemInfo.setRoundingLogic(filterPriceRange.findRoudingLogic(filteredPriceRange, threshRange));
						guidelineAndConstraintLog.setIsBreakingConstraint(true);
					}
				}
			}

			//If Cost is not available pass input range as output range
			if ((itemInfo.getListCost() == null || itemInfo.getListCost() <= 0)
					&& (itemInfo.getRegPrice() != null && itemInfo.getRegPrice() > 0)) 
				output.outputPriceRange = inputPriceRange;
			else
				output.outputPriceRange = filteredPriceRange;
			
			guidelineAndConstraintLog.setIsGuidelineOrConstraintApplied(true);
			guidelineAndConstraintLog.setGuidelineOrConstraintPriceRange1(threshRange);
			guidelineAndConstraintLog.setOutputPriceRange(output.outputPriceRange);
			explainLog.getGuidelineAndConstraintLogs().add(guidelineAndConstraintLog);
		} else {
			// If regular price is null, pass the input range as output range
			guidelineAndConstraintLog.setIsGuidelineOrConstraintApplied(false);
			guidelineAndConstraintLog.setMessage("Current Retail not available");
			guidelineAndConstraintLog.setOutputPriceRange(output.outputPriceRange);
			explainLog.getGuidelineAndConstraintLogs().add(guidelineAndConstraintLog);
		}

//		logger.debug("Threshold Constraint -- " + 
//				"Is Applied: " + guidelineAndConstraintLog.getIsGuidelineOrConstraintApplied() +
//				",Input Range: " + inputPriceRange.toString() + 
//				",Threshold Range: " + threshRange.toString() + 
//				",Output Range: " + output.outputPriceRange.toString());
		
		return output;		
	}
	
	
	private PRRange getThresholdRange(PRItemDTO itemInfo){
		double price = itemInfo.getRegPrice();
		PRRange outputRange = new PRRange();	 
		double maxVal = -1;   //Max value on increase
		double maxVal2 = -1;  //Max value on decrease		
		
		Integer defaultMaxVal = Integer.parseInt(PropertyManager.getProperty("PR_DEFAULT_THRESHOLD_MAX"));	
		
		if(valueType == PRConstants.VALUE_TYPE_PCT && minValue == Constants.DEFAULT_NA){			
			maxVal = ((maxValue == Constants.DEFAULT_NA) ? ((price * defaultMaxVal) / 100) : ((price * maxValue) / 100));
			maxVal2 = ((maxValue2 == Constants.DEFAULT_NA) ? maxVal : ((price * maxValue2) / 100));
		} else if (valueType == PRConstants.VALUE_TYPE_PCT && minValue != Constants.DEFAULT_NA) {
			maxVal = (price * minValue) / 100;
			maxVal2 = (price * minValue) / 100;
		} else if (valueType == PRConstants.VALUE_TYPE_$ && minValue != Constants.DEFAULT_NA) {
			maxVal = minValue;
			maxVal2 = minValue;
		} else{			
			maxVal = ((maxValue == Constants.DEFAULT_NA) ? ((price * defaultMaxVal) / 100) : maxValue);
			maxVal2 = ((maxValue2 == Constants.DEFAULT_NA) ? maxVal : maxValue2);
		}		 
		outputRange.setStartVal(price - maxVal2);
		outputRange.setEndVal(price + maxVal);	
		
		
		
		return outputRange;
	}
	
	/**
	 * Methods that returns Price Range for Threshold Constraint
	 * @return
	 */
	public PRRange getPriceRange(double price){
		PRRange range = null;
		FilterPriceRange filterPriceRange = new FilterPriceRange();
		if(valueType == PRConstants.VALUE_TYPE_PCT){
			range = getPriceRangeWithThresholdPct(price);
		}else{
			range = getPriceRangeWithThreshold$(price);
		}
		//Adjust if there is negative range
		filterPriceRange.handleNegativeRange(range);
		return range;
	}
	
	/**
	 * Method that returns Price Range for Threshold Pct Constraint
	 * @return
	 */
	private PRRange getPriceRangeWithThresholdPct(double price){
		setConflictLog("");
		double maxVal = -1;
		
		if(maxValue != Constants.DEFAULT_NA && minValue == Constants.DEFAULT_NA){
			maxVal = (price * maxValue)/100;
		}else if(minValue != Constants.DEFAULT_NA && maxValue == Constants.DEFAULT_NA){
			maxVal = (price * 30)/100;
		}else if(minValue != Constants.DEFAULT_NA && maxValue != Constants.DEFAULT_NA){
			maxVal = (price * maxValue)/100;
		}else if(minValue == Constants.DEFAULT_NA && maxValue == Constants.DEFAULT_NA){
			maxVal = (price * 30)/100;
		}
		
		PRRange outputRange = new PRRange();
		outputRange.setStartVal(price - maxVal);
		outputRange.setEndVal(price + maxVal);
			
		return outputRange;
	}
	
	private PRRange getPriceRangeWithThreshold$(double price){
		setConflictLog("");
		double maxVal = -1;
		if(maxValue != Constants.DEFAULT_NA && minValue == Constants.DEFAULT_NA){
			maxVal = maxValue;
		}else if(minValue != Constants.DEFAULT_NA && maxValue == Constants.DEFAULT_NA){
			maxVal = (price * 30)/100;
		}else if(minValue != Constants.DEFAULT_NA && maxValue != Constants.DEFAULT_NA){
			maxVal = maxValue;
		}else if(minValue == Constants.DEFAULT_NA && maxValue == Constants.DEFAULT_NA){
			maxVal = (price * 30)/100;
		}
		
		PRRange outputRange = new PRRange();
		outputRange.setStartVal(price - maxVal);
		outputRange.setEndVal(price + maxVal);
		
		return outputRange;
	}
	 	
	public Double[] getPriceRangeWithThreshold(double price, PRRange inputRange, PRConstraintRounding rConstraintDTO,
			PRItemDTO itemInfo, PRExplainLog explainLog){
		setConflictLog("");
		PRGuidelineAndConstraintLog guidelineAndConstraintLog = new PRGuidelineAndConstraintLog();	
		FilterPriceRange filterPriceRange = new FilterPriceRange();
		DecimalFormat df = new DecimalFormat("###.##");
		double minVal = -1;
		double maxVal = -1;   //Max value on increase
		double maxVal2 = -1;  //Max value on decrease
		
		Double defaultMinVal = Double.parseDouble(PropertyManager.getProperty("PR_DEFAULT_THRESHOLD_MIN"));
		Integer defaultMaxVal = Integer.parseInt(PropertyManager.getProperty("PR_DEFAULT_THRESHOLD_MAX"));		
		
		guidelineAndConstraintLog.setConstraintTypeId(ConstraintTypeLookup.THRESHOLD.getConstraintTypeId());
		
		if(valueType == PRConstants.VALUE_TYPE_PCT){
			minVal = ((minValue == Constants.DEFAULT_NA) ? defaultMinVal : ((price * minValue) / 100));
			maxVal = ((maxValue == Constants.DEFAULT_NA) ? ((price * defaultMaxVal) / 100) : ((price * maxValue) / 100));
			maxVal2 = ((maxValue2 == Constants.DEFAULT_NA) ? maxVal : ((price * maxValue2) / 100));
		}
		else{
			minVal = ((minValue == Constants.DEFAULT_NA) ? defaultMinVal : minValue);
			maxVal = ((maxValue == Constants.DEFAULT_NA) ? ((price * defaultMaxVal) / 100) : maxValue);
			maxVal2 = ((maxValue2 == Constants.DEFAULT_NA) ? maxVal : maxValue2);
		}		 
		
		PRRange outputRange = new PRRange();
		PRRange positiveRange = new PRRange();
		//Price: 3.19, minVal: (3.19*10)/100, maxVal: (3.19*20)/100, Positive Range: 3.51-3.83
		//Price: 3.19, minVal: 0, maxVal: (3.19*10)/100, Positive Range: 3.19-3.83
		positiveRange.setStartVal(price + minVal);
		positiveRange.setEndVal(price + maxVal);
		
		//Price: 3.19, minVal: (3.19*10)/100, maxVal: (3.19*20)/100, Negative Range: 2.55-2.87
		PRRange negativeRange = new PRRange();
		negativeRange.setStartVal(price - maxVal2);
		negativeRange.setEndVal(price - minVal);
		
		minVal = Double.valueOf(df.format(minVal));
		maxVal = Double.valueOf(df.format(maxVal));
		maxVal2 = Double.valueOf(df.format(maxVal2));	
		
		//Adjust if there is negative range
		filterPriceRange.handleNegativeRange(positiveRange);
		
		//Adjust if there is negative range
		filterPriceRange.handleNegativeRange(negativeRange);
		
		Set<Double> priceList = new HashSet<Double>();
		setThresholdLog(positiveRange, negativeRange, inputRange, guidelineAndConstraintLog);
		
		// Ahold, Cost Change
		if (itemInfo.getCostChangeBehavior().equals(PRConstants.COST_CHANGE_AHOLD_STORE_BRAND)
				|| itemInfo.getCostChangeBehavior().equals(PRConstants.COST_CHANGE_AHOLD_MARGIN)) {
			
				//Mark if threshold in conflict and assign brand guideline as price range to rounding,
				//threshold can be broken if it is in conflict with store brand guideline for ahold
				if(inputRange.getStartVal() != Constants.DEFAULT_NA && inputRange.getEndVal() != Constants.DEFAULT_NA){
					if(inputRange.getEndVal()<= negativeRange.getStartVal() ||
							inputRange.getStartVal()>= positiveRange.getEndVal()){				
						guidelineAndConstraintLog.setIsConflict(true);					 
					}
				}
				if(inputRange.getEndVal() != Constants.DEFAULT_NA){
					if(inputRange.getEndVal()<= negativeRange.getStartVal()){
						guidelineAndConstraintLog.setIsConflict(true);						 
					}
				}else if(inputRange.getStartVal() != Constants.DEFAULT_NA){
					if(inputRange.getStartVal()>= positiveRange.getEndVal()){	
						guidelineAndConstraintLog.setIsConflict(true);					 
					}
				}
			 	
				if(itemInfo.getCostChangeBehavior().equals(PRConstants.COST_CHANGE_AHOLD_STORE_BRAND) && 
						guidelineAndConstraintLog.getIsConflict()){
					priceRangeToRounding = itemInfo.getStoreBrandRelationRange();
				}
					
			
			BrandGuideline brandGuidline = new BrandGuideline();
			double priceRange[] = null;
			
			if(itemInfo.getCostChangeBehavior().equals(PRConstants.COST_CHANGE_AHOLD_STORE_BRAND))	 {
				priceRange = brandGuidline.applyStoreBrandGuideline(itemInfo.getStoreBrandRelationRange(), itemInfo.getStoreBrandRelationOperator(), 
						priceRangeToRounding, rConstraintDTO);
			} else {
				priceRange = rConstraintDTO.getRange(inputRange.getStartVal(), inputRange.getEndVal(), roundingDigitLogic);
				priceRangeToRounding = inputRange;
			}
			
			for(Double tempPrice : priceRange){
				priceList.add(tempPrice);
			}
		}
		else{			
			//Both Start and End value is present in Input Price Range
			if(inputRange.getStartVal() != Constants.DEFAULT_NA && inputRange.getEndVal() != Constants.DEFAULT_NA){
				double priceRange[] = rConstraintDTO.getRange(inputRange.getStartVal(), inputRange.getEndVal(), PRConstants.ROUND_CLOSEST);
				for(Double tempPrice : priceRange){
					//Diff between cur reg price and price from rounding
					double diff = Double.valueOf(df.format(price - tempPrice));			
					//ignore the price point which is outside the threshold
					//if(((diff) >= -maxVal2 && (diff) <= -minVal) || ((diff) >= minVal && (diff) <= maxVal)){
					if(((diff) >= -maxVal && (diff) <= -minVal) || ((diff) >= minVal && (diff) <= maxVal) ||
							((diff) >= -maxVal2 && (diff) <= -minVal) || ((diff) >= minVal && (diff) <= maxVal2)){
						priceList.add(tempPrice);
					}
				}
			//Start alone present in Input Price Range	
			}else if(inputRange.getStartVal() != Constants.DEFAULT_NA && inputRange.getEndVal() == Constants.DEFAULT_NA){
				outputRange = new PRRange();
				// Input Range is within positive range
				if (inputRange.getStartVal() >= positiveRange.getStartVal() && inputRange.getStartVal() <= positiveRange.getEndVal()) {
					outputRange.setStartVal(inputRange.getStartVal());
					outputRange.setEndVal(positiveRange.getEndVal());
				}
				// there is no end range
				else if (positiveRange.getStartVal() >= inputRange.getStartVal()) {
					outputRange.setStartVal(positiveRange.getStartVal());
					outputRange.setEndVal(positiveRange.getEndVal());
				}
				double priceRange[] = rConstraintDTO
						.getRange(outputRange.getStartVal(), outputRange.getEndVal(), PRConstants.ROUND_CLOSEST);
				for (Double tempPrice : priceRange) {
					double diff = Double.valueOf(df.format(price - tempPrice));
					if (((diff) >= -maxVal && (diff) <= -minVal) || ((diff) >= minVal && (diff) <= maxVal)) {
						priceList.add(tempPrice);
					}
				}

				outputRange = new PRRange();
				// Input Range is within negative range
				if (inputRange.getStartVal() >= negativeRange.getStartVal() && inputRange.getStartVal() <= negativeRange.getEndVal()) {
					outputRange.setStartVal(inputRange.getStartVal());
					outputRange.setEndVal(negativeRange.getEndVal());
				}// there is no end range
				else if (negativeRange.getStartVal() >= inputRange.getStartVal()) {
					outputRange.setStartVal(negativeRange.getStartVal());
					outputRange.setEndVal(negativeRange.getEndVal());
				}

				double priceRange1[] = rConstraintDTO.getRange(outputRange.getStartVal(), outputRange.getEndVal(),
						PRConstants.ROUND_CLOSEST);

				for (Double tempPrice : priceRange1) {
					double diff = Double.valueOf(df.format(price - tempPrice));
					if (((diff) >= -maxVal2 && (diff) <= -minVal) || ((diff) >= minVal && (diff) <= maxVal2)) {
						priceList.add(tempPrice);
					}
				}
			//End value alone present in Input Price Range	
			}else if(inputRange.getStartVal() == Constants.DEFAULT_NA && inputRange.getEndVal() != Constants.DEFAULT_NA){
				outputRange = new PRRange();
				// Input Range is within positive range
				if (inputRange.getEndVal() <= positiveRange.getEndVal() && inputRange.getEndVal() >= positiveRange.getStartVal()) {
					outputRange.setStartVal(positiveRange.getStartVal());
					outputRange.setEndVal(inputRange.getEndVal());
				}// there is no start range
				else if (positiveRange.getEndVal() <= inputRange.getEndVal()) {
					outputRange.setStartVal(positiveRange.getStartVal());
					outputRange.setEndVal(positiveRange.getEndVal());
				}

				double priceRange[] = rConstraintDTO
						.getRange(outputRange.getStartVal(), outputRange.getEndVal(), PRConstants.ROUND_CLOSEST);

				for (Double tempPrice : priceRange) {
					double diff = Double.valueOf(df.format(price - tempPrice));
					if (((diff) >= -maxVal && (diff) <= -minVal) || ((diff) >= minVal && (diff) <= maxVal)) {
						priceList.add(tempPrice);
					}
				}

				outputRange = new PRRange();
				// Input Range is within negative range
				if (inputRange.getEndVal() <= negativeRange.getEndVal() && inputRange.getEndVal() >= negativeRange.getStartVal()) {
					outputRange.setStartVal(negativeRange.getStartVal());
					outputRange.setEndVal(inputRange.getEndVal());
					// there is no start range
				} else if (negativeRange.getEndVal() <= inputRange.getEndVal()) {
					outputRange.setStartVal(negativeRange.getStartVal());
					outputRange.setEndVal(negativeRange.getEndVal());
				}

				double priceRange1[] = rConstraintDTO.getRange(outputRange.getStartVal(), outputRange.getEndVal(),
						PRConstants.ROUND_CLOSEST);

				for (Double tempPrice : priceRange1) {
					double diff = Double.valueOf(df.format(price - tempPrice));
					if (((diff) >= -maxVal2 && (diff) <= -minVal) || ((diff) >= minVal && (diff) <= maxVal2)) {
						priceList.add(tempPrice);
					}
				}
				
			//If both the Price Point of Input is not available (happens when no other guideline has data)	
			}else if(inputRange.getStartVal() == Constants.DEFAULT_NA && inputRange.getEndVal() == Constants.DEFAULT_NA){
				//Use threshold range itself
				double priceRange[] = rConstraintDTO.getRange(priceRangeToRounding.getStartVal(), priceRangeToRounding.getEndVal(), 
						PRConstants.ROUND_CLOSEST);
				
				for(Double tempPrice : priceRange){
					double diff = Double.valueOf(df.format(price - tempPrice));
					if(((diff) >= -maxVal && (diff) <= -minVal) || ((diff) >= minVal && (diff) <= maxVal) ||
							((diff) >= -maxVal2 && (diff) <= -minVal) || ((diff) >= minVal && (diff) <= maxVal2)){
						priceList.add(tempPrice);
					}
				}
			}
			
			//No price point found which is within the threshold from the input price range
			// but the input price range overlaps with the threshold price range
			if(priceList.size() <= 0){
				if((inputRange.getStartVal() >= positiveRange.getStartVal() && inputRange.getStartVal() <= positiveRange.getEndVal())
						|| (inputRange.getEndVal() >= positiveRange.getStartVal() && inputRange.getEndVal() <= positiveRange.getEndVal())){
					//setConflictLog("Rounding,");
					double priceRange1[] = rConstraintDTO.getRange(positiveRange.getStartVal(), positiveRange.getEndVal(), PRConstants.ROUND_CLOSEST);
					double lowestDiff = 0;
					double lowestPrice = 0;
					double highestDiff = 0;
					double highestPrice = 0;
					boolean lowestFirst = true;
					boolean highestFirst = true;
					
					// Find price with lowest difference which is below the input start price range
					// and price with highest difference which is above the input end price range
					// < 3.61 -- 3.61 >
					
					for(double tPrice : priceRange1){
						if(tPrice < inputRange.getStartVal()){
							if(lowestFirst == true){
								lowestDiff = Math.abs(tPrice - inputRange.getStartVal());
								lowestPrice = tPrice;
								lowestFirst = false;
							}else{
								double diff = Math.abs(tPrice - inputRange.getStartVal());
								if(diff < lowestDiff){
									lowestPrice = tPrice;
								}
							}
						}
						
						if(tPrice > inputRange.getEndVal()){
							if(highestFirst == true){
								highestDiff = Math.abs(tPrice - inputRange.getEndVal());
								highestPrice = tPrice;
								highestFirst = false;
							}else{
								double diff = Math.abs(tPrice - inputRange.getEndVal());
								if(diff < highestDiff){
									highestPrice = tPrice;
								}
							}
						}
					}
					
					if(!lowestFirst)
						priceList.add(lowestPrice);
					if(!highestFirst)
						priceList.add(highestPrice);
				}
			}
			
			if(priceList.size() <= 0){
				if((inputRange.getStartVal() >= negativeRange.getStartVal() && inputRange.getStartVal() <= negativeRange.getEndVal())
						||(inputRange.getEndVal() >= negativeRange.getStartVal() && inputRange.getEndVal() <= negativeRange.getEndVal()) ){
					//setConflictLog("Rounding,");
					double priceRange1[] = rConstraintDTO.getRange(negativeRange.getStartVal(), negativeRange.getEndVal(), PRConstants.ROUND_CLOSEST);
					double lowestDiff = 0;
					double lowestPrice = 0;
					double highestDiff = 0;
					double highestPrice = 0;
					boolean lowestFirst = true;
					boolean highestFirst = true;
					
					for(double tPrice : priceRange1){
						if(tPrice < inputRange.getStartVal()){
							if(lowestFirst == true){
								lowestDiff = Math.abs(tPrice - inputRange.getStartVal());
								lowestPrice = tPrice;
								lowestFirst = false;
							}else{
								double diff = Math.abs(tPrice - inputRange.getStartVal());
								if(diff < lowestDiff){
									lowestPrice = tPrice;
								}
							}
						}
						
						if(tPrice > inputRange.getEndVal()){
							if(highestFirst == true){
								highestDiff = Math.abs(tPrice - inputRange.getEndVal());
								highestPrice = tPrice;
								highestFirst = false;
							}else{
								double diff = Math.abs(tPrice - inputRange.getEndVal());
								if(diff < highestDiff){
									highestPrice = tPrice;
								}
							}
						}
					}
					
					if(!lowestFirst)
						priceList.add(lowestPrice);
					if(!highestFirst)
						priceList.add(highestPrice);
				}
			
			}
			
			//No price point found within the threshold
			//If the input price range does not overlap with the threshold price range
			if(priceList.size() <= 0){ 
				//If the threshold is in conflict and its in higher side, then use threshold highest range as both lowest and highest
				//don't break the threshold suggest the price below the threshold round down
				// -- 3.52, 2.87 -- 3.51
				//Assign input start and end range, to handle following scenario
				// (- -- 20.17, 11.06 -- 13.52), (9.02 -- -, 11.06 -- 13.52)
				
				double startVal = inputRange.getStartVal();
				double endVal = inputRange.getEndVal();
				
				if(startVal == Constants.DEFAULT_NA){
					startVal = inputRange.getEndVal();
				}
				
				if(endVal == Constants.DEFAULT_NA){
					endVal = inputRange.getStartVal();
				}
				
				if(startVal != Constants.DEFAULT_NA){
					if(startVal >= positiveRange.getEndVal()){
						priceRangeToRounding.setStartVal(positiveRange.getEndVal());
						priceRangeToRounding.setEndVal(positiveRange.getEndVal());
						double priceRange1[] = rConstraintDTO.getRange(positiveRange.getEndVal(), positiveRange.getEndVal(),
								PRConstants.ROUND_DOWN);
						for (double tPrice : priceRange1) {
							priceList.add(tPrice);
						}
						if(getConflictLog() == null || getConflictLog().length() <= 0){
							//setConflictLog("Threshold,");
							//guidelineAndConstraintLog.setIsConflict(true);
						}
					}					
				}						
				
				//If the threshold is in conflict and its in lower side, then use threshold lowest range as both lowest and highest
				// -- 2.50, 2.87 -- 3.51
				if (endVal != Constants.DEFAULT_NA) {
					if (endVal <= negativeRange.getStartVal()) {
						priceRangeToRounding.setStartVal(negativeRange.getStartVal());
						priceRangeToRounding.setEndVal(negativeRange.getStartVal());
						double priceRange1[] = rConstraintDTO.getRange(negativeRange.getStartVal(), negativeRange.getStartVal(), PRConstants.ROUND_UP);
						for (double tPrice : priceRange1) {
							priceList.add(tPrice);
						}
						if(getConflictLog() == null || getConflictLog().length() <= 0){
							//setConflictLog("Threshold,");
							//guidelineAndConstraintLog.setIsConflict(true);
						}
					}
				}
			}			 
		}
		
		Double[] finalRange = new Double[priceList.size()];
		int counter = 0;
		for(Double tPrice : priceList){
			finalRange[counter] = tPrice;
			counter++;
		}
		
		guidelineAndConstraintLog.setOutputPriceRange(priceRangeToRounding);
		explainLog.getGuidelineAndConstraintLogs().add(guidelineAndConstraintLog);
		return finalRange;
	}
		
	/**
	 * Returns price range as a String
	 * @param priceRange
	 * @return
	 */
	private String getPriceRangeString(Double[] priceRange){
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
	
	public String getConflictLog(){
		return this.conflictLog;
	}
	
	public void setConflictLog(String conflictLog) {
		this.conflictLog = conflictLog;
	}
	
	public int getIsConflict() {
		if(this.getConflictLog() != null && this.getConflictLog().length() > 0){
			this.isConflict = 1;
		}else{
			this.isConflict = 0;
			setConflictLog("Conflict:;");
		}
		return isConflict;
	}
	
	public String getThresholdLog() {
		return thresholdLog;
	}
	
	public void setThresholdLog(PRRange positiveRange, PRRange negativeRange, PRRange inputRange, PRGuidelineAndConstraintLog guidelineAndConstraintLog ) {
//		DecimalFormat format = new DecimalFormat("######.##"); 
		
		/*if(negativeRange.getEndVal() == positiveRange.getStartVal()){
			this.thresholdLog = Constants.YES + "," + format.format(negativeRange.getStartVal()) + " " + format.format(positiveRange.getEndVal());	
		}else{
			this.thresholdLog = Constants.YES + "," + format.format(negativeRange.getStartVal()) + "-" + format.format(negativeRange.getEndVal()) + " " + format.format(positiveRange.getStartVal()) + "-" + format.format(positiveRange.getEndVal());
		}*/
		
		if(negativeRange.getEndVal() == positiveRange.getStartVal()){
			PRRange tempRange = new PRRange();
			guidelineAndConstraintLog.setIsGuidelineOrConstraintApplied(true);
			tempRange.setStartVal(negativeRange.getStartVal());
			tempRange.setEndVal(positiveRange.getEndVal());
			guidelineAndConstraintLog.setGuidelineOrConstraintPriceRange1(tempRange);
			//this.setThresholdRange(format.format(negativeRange.getStartVal()) + " " + format.format(positiveRange.getEndVal()));
			//this.thresholdLog = Constants.YES + "," + this.getThresholdRange();	
		}else{
			PRRange tempRange;
			guidelineAndConstraintLog.setIsGuidelineOrConstraintApplied(true);
			
			tempRange = new PRRange();
			tempRange.setStartVal(negativeRange.getStartVal());
			tempRange.setEndVal(negativeRange.getEndVal());
			guidelineAndConstraintLog.setGuidelineOrConstraintPriceRange1(tempRange);
			
			tempRange = new PRRange();
			tempRange.setStartVal(positiveRange.getStartVal());
			tempRange.setEndVal(positiveRange.getEndVal());
			guidelineAndConstraintLog.setGuidelineOrConstraintPriceRange2(tempRange);
			
			/*this.setThresholdRange(format.format(negativeRange.getStartVal()) + "-" +			
					format.format(negativeRange.getEndVal()) + " " + format.format(positiveRange.getStartVal()) + "-" 
					+ format.format(positiveRange.getEndVal()));*/
			//this.thresholdLog = Constants.YES + "," +  this.getThresholdRange();
		}
		
		PRRange nRange = filterRange(negativeRange, inputRange);
		PRRange pRange = filterRange(positiveRange, inputRange);
		if(!nRange.isConflict() && !pRange.isConflict()){
			if(nRange.getEndVal() == pRange.getStartVal()){
				PRRange rangeToRounding = new PRRange();
				rangeToRounding.setStartVal(nRange.getStartVal());
				rangeToRounding.setEndVal(pRange.getEndVal());
				priceRangeToRounding = rangeToRounding;
			}else if(nRange.getStartVal() == pRange.getStartVal() && 
					nRange.getEndVal() == Constants.DEFAULT_NA && pRange.getEndVal() != Constants.DEFAULT_NA){
				PRRange rangeToRounding = new PRRange();
				rangeToRounding.setStartVal(pRange.getStartVal());
				rangeToRounding.setEndVal(pRange.getEndVal());
				priceRangeToRounding = rangeToRounding;
			}
			else if(nRange.getEndVal() == pRange.getEndVal() && 
					pRange.getStartVal() == Constants.DEFAULT_NA && nRange.getStartVal() != Constants.DEFAULT_NA){
				PRRange rangeToRounding = new PRRange();
				rangeToRounding.setStartVal(pRange.getStartVal());
				rangeToRounding.setEndVal(pRange.getEndVal());
				priceRangeToRounding = rangeToRounding;
			}
		}else if(!nRange.isConflict()){
			priceRangeToRounding = nRange;
		}else if(!pRange.isConflict()){
			priceRangeToRounding = pRange;
		}else if(nRange.isConflict() && pRange.isConflict()){
			priceRangeToRounding = pRange;
		}
	}
	
	protected PRRange filterRange(PRRange thresholdRange, PRRange inputRange){
		PRRange range = new PRRange();
		range.copy(thresholdRange);
		if(inputRange != null){
			range = range.getRange(inputRange);
			Boolean isOutsideRange = checkIfOutsideRange(thresholdRange, range);
			range.setConflict(isOutsideRange);
		}
		
		return range;
	}
	
	private Boolean checkIfOutsideRange(PRRange outputRange, PRRange inputRange) {
		Boolean isOutsideRange = false;
		if(inputRange.getStartVal() != Constants.DEFAULT_NA && inputRange.getEndVal() != Constants.DEFAULT_NA){
			if(outputRange.getStartVal() != Constants.DEFAULT_NA && outputRange.getEndVal() != Constants.DEFAULT_NA){
				if(!(inputRange.getStartVal() >= outputRange.getStartVal() && inputRange.getEndVal() <= outputRange.getEndVal()))
					isOutsideRange = true;
			}else if(outputRange.getStartVal() != Constants.DEFAULT_NA){
				if(!(inputRange.getStartVal() >= outputRange.getStartVal() && inputRange.getEndVal() <= outputRange.getStartVal()))
					isOutsideRange = true;
			}else if(outputRange.getEndVal() != Constants.DEFAULT_NA){
				if(!(inputRange.getStartVal() >= outputRange.getStartVal() && inputRange.getEndVal() <= outputRange.getStartVal()))
					isOutsideRange = true;
			}
		}else if(inputRange.getStartVal() != Constants.DEFAULT_NA){
			if(outputRange.getStartVal() != Constants.DEFAULT_NA && outputRange.getEndVal() != Constants.DEFAULT_NA){
				if(!(inputRange.getStartVal() >= outputRange.getStartVal() && inputRange.getStartVal() <= outputRange.getEndVal()))
					isOutsideRange = true;
			}else if(outputRange.getStartVal() != Constants.DEFAULT_NA){
				if(!(inputRange.getStartVal() >= outputRange.getStartVal()))
					isOutsideRange = true;
			}else if(outputRange.getEndVal() != Constants.DEFAULT_NA){
				if(!(inputRange.getStartVal() <= outputRange.getEndVal()))
					isOutsideRange = true;
			}
		}else if(inputRange.getEndVal() != Constants.DEFAULT_NA){
			if(outputRange.getStartVal() != Constants.DEFAULT_NA && outputRange.getEndVal() != Constants.DEFAULT_NA){
				if(!(inputRange.getEndVal() >= outputRange.getStartVal() && inputRange.getEndVal() <= outputRange.getEndVal()))
					isOutsideRange = true;
			}else if(outputRange.getStartVal() != Constants.DEFAULT_NA){
				if(!(inputRange.getEndVal() >= outputRange.getStartVal()))
					isOutsideRange = true;
			}else if(outputRange.getEndVal() != Constants.DEFAULT_NA){
				if(!(inputRange.getEndVal() <= outputRange.getEndVal()))
					isOutsideRange = true;
			}
		}
		return isOutsideRange;
	}
	
	@Override
    protected Object clone() throws CloneNotSupportedException {
        return super.clone();
    }
}
