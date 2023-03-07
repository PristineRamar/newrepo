package com.pristine.service.offermgmt;

import java.util.ArrayList;
import java.util.List;

//import org.apache.log4j.Logger;

import com.pristine.dto.offermgmt.PRExplainLog;
import com.pristine.dto.offermgmt.PRGuidelineAndConstraintLog;
import com.pristine.dto.offermgmt.PRItemDTO;
import com.pristine.dto.offermgmt.PRRange;
import com.pristine.util.Constants;
import com.pristine.util.offermgmt.PRConstants;
import com.pristine.util.offermgmt.PRFormatHelper;

public class FilterPriceRange {
//	private static Logger logger = Logger.getLogger("FilterPriceRange");
	/**
	 * Filters price range and sets conflict flag.
	 * @param inputRange		Input price range to a guideline
	 * @param guidelineRange	Price range resulting from a guideline
	 * @return
	 */
	/*
	 * 1) Input Range      2.45	2.85
	 * 	  Guideline Range  2.50	2.80
	 * 	  Return 		   2.50	2.80
	 * 
	 * 2) Input Range      2.45	2.85
	 * 	  Guideline Range  2.50	-
	 * 	  Return 		   2.50	2.85
	 * 	
	 * 3) Input Range      -	2.85
	 * 	  Guideline Range  2.50	-
	 * 	  Return 		   2.50	2.85
	 * 
	 * 4) Input Range      -	2.85
	 * 	  Guideline Range  -	2.60
	 * 	  Return 		   -	2.60
	 * 
	 * 5) Input Range      2.45	2.85
	 * 	  Guideline Range  2.90	3.60
	 * 	  Return 		   2.45	2.85		
	 */
	public PRRange filterRange(PRRange inputRange, PRRange guidelineRange){
		PRRange range = new PRRange();
		PRRange newRange = new PRRange();
		range.copy(guidelineRange);
		
		if(range.getStartVal() != Constants.DEFAULT_NA)
			range.setStartVal(PRFormatHelper.roundToTwoDecimalDigitAsDouble(range.getStartVal()));
		if(range.getEndVal() != Constants.DEFAULT_NA)
			range.setEndVal(PRFormatHelper.roundToTwoDecimalDigitAsDouble(range.getEndVal()));
			
		if(inputRange != null){
			
			if(inputRange.getStartVal() != Constants.DEFAULT_NA)
				inputRange.setStartVal(PRFormatHelper.roundToTwoDecimalDigitAsDouble(inputRange.getStartVal()));
			if(inputRange.getEndVal() != Constants.DEFAULT_NA)
				inputRange.setEndVal(PRFormatHelper.roundToTwoDecimalDigitAsDouble(inputRange.getEndVal()));
			
			range = range.getRange(inputRange);
			//logger.debug("Actual Start Value - " + range.getStartVal() + " End Value - " + range.getEndVal());		
			
			if(checkIfOutsideRange(range, guidelineRange, newRange)){
				range = newRange;
				range.setConflict(true);				
			}
		}
		return range;
	}
	
	/**
	 * Checks if price range returned from a guideline is outside filtered price range from a guideline.
	 * Returns true if outside. 
	 * @param filteredPriceRange		Filtered price range from a guideline using input price range to a guideline and price range from a guideline 
	 * @param guidelinePriceRange		Price range from a guideline
	 * @return
	 */
	/*
	 * Assume a strategy has Margin and PI guideline
	 * Price Range from margin guideline - 2.25		4.25
	 * Price Range from PI guideline	 - 4.50		4.80
	 * filteredPriceRange				 - 2.25		4.25
	 * guidelinePriceRange				 - 4.50		4.80
	 * Method returns true in this case.			 
	 */
	public static Boolean checkIfOutsideRange(PRRange filteredPriceRange, PRRange guidelinePriceRange, PRRange newRange) {
		/*When any two guidelines are in conflict, final price range that comes out of them doesn't give 
		 * any weightage to the second guideline in conflict. We can partially give importance to the second 
		 * guideline with the following approach:

		1) Identify the lowest price in the final price range as the price closer to the lowest limit of conflicting guideline, 
		if it suggests a price higher than the first guideline.

		2) Identify the highest price in the final range as the price closer to the highest limit of conflicting guideline, 
		if it suggests a price lower than the first guideline.

		3) Also mark second guideline as conflict.*/
		
		Boolean isOutsideRange = false;
		
		//When both Price Point is there in the Guideline Price Range
		if (guidelinePriceRange.getStartVal() != Constants.DEFAULT_NA
				&& guidelinePriceRange.getEndVal() != Constants.DEFAULT_NA) {
			if (filteredPriceRange.getStartVal() != Constants.DEFAULT_NA
					&& filteredPriceRange.getEndVal() != Constants.DEFAULT_NA) {
				//Filter Range    -> 1.89  2.19 	1.89  2.19
				//Guideline Range -> 1.59  1.79     2.20  2.39
				// !(1.89>=1.59 && 2.19<=1.79) -> (t&&f) -> (f) -> (t)
				// !(1.89>=2.20 && 2.19<=2.39) -> (f&&t) -> (f) -> (t)
				if (!(filteredPriceRange.getStartVal() >= guidelinePriceRange.getStartVal() && filteredPriceRange
						.getEndVal() <= guidelinePriceRange.getEndVal())) {
					// Lower side 2.19<=2.20
					if (filteredPriceRange.getEndVal() <= guidelinePriceRange.getStartVal()) {
						newRange.setStartVal(filteredPriceRange.getEndVal());
						newRange.setEndVal(filteredPriceRange.getEndVal());
						// newRange.setIsHigherSideCarried(true);
						isOutsideRange = true;
					}
					// Higher side 1.89>=1.79
					else if (filteredPriceRange.getStartVal() >= guidelinePriceRange.getEndVal()) {
						newRange.setStartVal(filteredPriceRange.getStartVal());
						newRange.setEndVal(filteredPriceRange.getStartVal());
						// newRange.setIsLowerSideCarried(true);
						isOutsideRange = true;
					}
				}
			} //Filter Range    -> 1.89  -- 
			  //Guideline Range -> 1.59  1.79  
			else if (filteredPriceRange.getStartVal() != Constants.DEFAULT_NA) {
				// !(1.89>=1.59 && 1.89<=1.79) -> (t&&f) -> (f) -> (t)
				if (!(filteredPriceRange.getStartVal() >= guidelinePriceRange.getStartVal() && filteredPriceRange
						.getStartVal() <= guidelinePriceRange.getEndVal())) {
					newRange.setStartVal(filteredPriceRange.getStartVal());
					newRange.setEndVal(filteredPriceRange.getStartVal());
					// newRange.setIsLowerSideCarried(true);
					isOutsideRange = true;
				}
			} //Filter Range    -> 	 --  1.49 
			  //Guideline Range -> 1.59  1.79 
			else if (filteredPriceRange.getEndVal() != Constants.DEFAULT_NA) {
				// !(1.49>=1.59 && 1.49<=1.79) -> (f&&t) -> (f) -> (t)
				if (!(filteredPriceRange.getEndVal() >= guidelinePriceRange.getStartVal() && filteredPriceRange
						.getEndVal() <= guidelinePriceRange.getEndVal())) {
					newRange.setStartVal(filteredPriceRange.getEndVal());
					newRange.setEndVal(filteredPriceRange.getEndVal());
					// newRange.setIsHigherSideCarried(true);
					isOutsideRange = true;
				}
			}
			// When Start Price Point is present in the Guideline Price Range
		} else if (guidelinePriceRange.getStartVal() != Constants.DEFAULT_NA) {
			//Filter Range    -> 1.39  1.49 
			//Guideline Range -> 1.59    -- 
			if (filteredPriceRange.getStartVal() != Constants.DEFAULT_NA && filteredPriceRange.getEndVal() != Constants.DEFAULT_NA) {
				// 1.59 >= 1.39 && 1.59 >= 14.9
				if (!(guidelinePriceRange.getStartVal() <= filteredPriceRange.getStartVal() && guidelinePriceRange.getStartVal() <= filteredPriceRange
						.getEndVal())) {
					// Lower Side 1.49 <= 1.59
					if (filteredPriceRange.getEndVal() <= guidelinePriceRange.getStartVal()) {
						newRange.setStartVal(filteredPriceRange.getEndVal());
						newRange.setEndVal(filteredPriceRange.getEndVal());
						//newRange.setIsHigherSideCarried(true);
						isOutsideRange = true;
					}
					// Higher side(4.50 -- 4.80, 2.25 -- ) -- Not Sure if code reaches this condition
					else if (filteredPriceRange.getStartVal() >= guidelinePriceRange.getStartVal()) {
						newRange.setStartVal(filteredPriceRange.getStartVal());
						newRange.setEndVal(filteredPriceRange.getStartVal());
						//newRange.setIsLowerSideCarried(true);
						isOutsideRange = true;
					}
				}
			} //Filter Range    -> 1.59 --
			  //Guideline Range -> 1.49 --   
			/*else if (filteredPriceRange.getStartVal() != Constants.DEFAULT_NA) {
				if (!(guidelinePriceRange.getStartVal() >= filteredPriceRange.getStartVal())) {
					newRange.setStartVal(filteredPriceRange.getStartVal());
					newRange.setEndVal(filteredPriceRange.getStartVal());
					//newRange.setIsLowerSideCarried(true);
					isOutsideRange = true;
				}
			}*/ //Filter Range    -> --    1.59 
			    //Guideline Range -> 1.69  --  
			else if (filteredPriceRange.getEndVal() != Constants.DEFAULT_NA) {
				if (!(guidelinePriceRange.getStartVal() <= filteredPriceRange.getEndVal())) {
					newRange.setStartVal(filteredPriceRange.getEndVal());
					newRange.setEndVal(filteredPriceRange.getEndVal());
					//newRange.setIsHigherSideCarried(true);
					isOutsideRange = true;
				}
			}
		//When End Price Point is present in the Guideline Price Range
		} else if (guidelinePriceRange.getEndVal() != Constants.DEFAULT_NA) {
			//Filter Range    -> 1.59  1.63 
			//Guideline Range -> --    1.43 
			if (filteredPriceRange.getStartVal() != Constants.DEFAULT_NA && filteredPriceRange.getEndVal() != Constants.DEFAULT_NA) {
				//1.43 < 1.59 && 1.43 < 1.63
				if (!(guidelinePriceRange.getEndVal() >= filteredPriceRange.getStartVal() && guidelinePriceRange
						.getEndVal() >= filteredPriceRange.getEndVal())) {
					// Lower Side(2.25 -- 4.25, -- 4.80) -- Not Sure if code reaches this condition
					if (filteredPriceRange.getEndVal() <= guidelinePriceRange.getEndVal()) {
						newRange.setStartVal(filteredPriceRange.getEndVal());
						newRange.setEndVal(filteredPriceRange.getEndVal());
						// newRange.setIsHigherSideCarried(true);
						isOutsideRange = true;
					}
					// Higher side 1.59 >= 1.43
					else if (filteredPriceRange.getStartVal() >= guidelinePriceRange.getEndVal()) {
						newRange.setStartVal(filteredPriceRange.getStartVal());
						newRange.setEndVal(filteredPriceRange.getStartVal());
						// newRange.setIsLowerSideCarried(true);
						isOutsideRange = true;
					}
				}
			}//Filter Range    -> 1.59  -- 
			 //Guideline Range -> --    1.43 
			else if (filteredPriceRange.getStartVal() != Constants.DEFAULT_NA) {
				if (!(guidelinePriceRange.getEndVal() >= filteredPriceRange.getStartVal())) {
					newRange.setStartVal(filteredPriceRange.getStartVal());
					newRange.setEndVal(filteredPriceRange.getStartVal());
					//newRange.setIsLowerSideCarried(true);
					isOutsideRange = true;
				}
			} /* Filter Range    -> -- 1.59
			     Guideline Range -> -- 1.83
			else if (filteredPriceRange.getEndVal() != Constants.DEFAULT_NA) {
				if (!(guidelinePriceRange.getEndVal() <= filteredPriceRange.getEndVal())) {
					newRange.setStartVal(filteredPriceRange.getEndVal());
					newRange.setEndVal(filteredPriceRange.getEndVal());
					//newRange.setIsHigherSideCarried(true);
					isOutsideRange = true;
				}
			}*/
		}
		return isOutsideRange;
	}

	public void handleNegativeRange(PRRange inputRange) {
	 
		/* Possible Ranges 
		 * -9999.99 -- -9999.99(-9999.99 -- -9999.99), -9999.99 -- 3.19(-9999.99 -- 3.19), 
		 * 3.19 -- -9999.99(3.19 -- -9999.99), 3.19 -- 3.19(3.19 -- 3.19) 
		 * -5.2 -- -5.2(0 -- 0)
		 * -9999.99 -- -3.19(0 -- 0), 4.19 -- -3.20(0 -- 0)
		 * -3.19 -- -9999.99(0 -- -9999.99), -3.19 -- 5.2 (0 -- 5.2)
		 */
		
		//If both the ends of the Range is negative
		if((inputRange.getStartVal() != Constants.DEFAULT_NA && inputRange.getStartVal() < 0) && 
				((inputRange.getEndVal() != Constants.DEFAULT_NA && inputRange.getEndVal() < 0))){
			inputRange.setStartVal(0);
			inputRange.setEndVal(0);
		}		
		//If lower side of the Range is negative
		else if(inputRange.getStartVal() != Constants.DEFAULT_NA && inputRange.getStartVal() < 0){
			inputRange.setStartVal(0); 
		}
		//If higher side of the Range is negative
		else if(inputRange.getEndVal() != Constants.DEFAULT_NA && inputRange.getEndVal() < 0){
			inputRange.setStartVal(0);
			inputRange.setEndVal(0);
		}
	}
	
	//Check if a guideline or constraint is already applied
	public boolean checkIfConstraintApplied(PRExplainLog explainLog, int constraintId){
		boolean isConstraintApplied = false;
		
		for (PRGuidelineAndConstraintLog guidelineAndConstraintLog : explainLog.getGuidelineAndConstraintLogs()) {
			if(guidelineAndConstraintLog.getConstraintTypeId() == constraintId){
				if(guidelineAndConstraintLog.getIsGuidelineOrConstraintApplied()){
					isConstraintApplied = true;
				}
			}
		}		
		return isConstraintApplied;
	}
	
	public boolean checkIfGuidelineApplied(PRExplainLog explainLog, int guidelineId){
		boolean isGuidelineApplied = false;
		
		for (PRGuidelineAndConstraintLog guidelineAndConstraintLog : explainLog.getGuidelineAndConstraintLogs()) {
			if(guidelineAndConstraintLog.getGuidelineTypeId() == guidelineId){
				if(guidelineAndConstraintLog.getIsGuidelineOrConstraintApplied()){
					isGuidelineApplied = true;
				}
			}
		}		
		return isGuidelineApplied;
	}
	
	public boolean isRangeBreakingConstraint(PRExplainLog explainLog) {
		boolean isBreakingConstraint = false;
		for (PRGuidelineAndConstraintLog guidelineAndConstraintLog : explainLog.getGuidelineAndConstraintLogs()) {
			if (guidelineAndConstraintLog.getConstraintTypeId() > 0) {
				if (guidelineAndConstraintLog.getIsBreakingConstraint()) {
					isBreakingConstraint = true;
					break;
				}
			}
		}
		return isBreakingConstraint;
	}

	public PRRange getFirstAvailableGuidelineRange(PRExplainLog explainLog){
		PRRange guidelineRange = null;
		for (PRGuidelineAndConstraintLog guidelineAndConstraintLog : explainLog.getGuidelineAndConstraintLogs()) {
			if(guidelineAndConstraintLog.getGuidelineTypeId() > 0){
				if(guidelineAndConstraintLog.getGuidelineOrConstraintPriceRange1().getStartVal() !=  Constants.DEFAULT_NA ||
						guidelineAndConstraintLog.getGuidelineOrConstraintPriceRange1().getEndVal() !=  Constants.DEFAULT_NA ){
					guidelineRange = guidelineAndConstraintLog.getGuidelineOrConstraintPriceRange1();
					break;
				}
			}
		}		
		return guidelineRange;
	}
	
	public PRRange getConstraintRange(PRExplainLog explainLog, int constraintId){
		PRRange outputRange = new PRRange();
		
		for (PRGuidelineAndConstraintLog guidelineAndConstraintLog : explainLog.getGuidelineAndConstraintLogs()) {
			if(guidelineAndConstraintLog.getConstraintTypeId() == constraintId){
				if(guidelineAndConstraintLog.getIsGuidelineOrConstraintApplied()){
					outputRange = guidelineAndConstraintLog.getGuidelineOrConstraintPriceRange1();
				}
			}
		}		
		return outputRange;
	}
	
	public PRRange getGuidelineRange(PRExplainLog explainLog, int guidelineId){
		PRRange outputRange = new PRRange();
		
		for (PRGuidelineAndConstraintLog guidelineAndConstraintLog : explainLog.getGuidelineAndConstraintLogs()) {
			if(guidelineAndConstraintLog.getGuidelineTypeId() == guidelineId){
				if(guidelineAndConstraintLog.getIsGuidelineOrConstraintApplied()){
					outputRange = guidelineAndConstraintLog.getGuidelineOrConstraintPriceRange1();
				}
			}
		}		
		return outputRange;
	}
	 
	public String findRoudingLogic(PRRange inputRange, PRRange constraintRange){
		String roundingLogic = PRConstants.ROUND_UP;	
		if(inputRange.getStartVal() == Constants.DEFAULT_NA && inputRange.getEndVal() == Constants.DEFAULT_NA && 
				constraintRange.getStartVal() == Constants.DEFAULT_NA && constraintRange.getEndVal() == Constants.DEFAULT_NA){			
		}
		//When both range of Constraint and Start Range of Input exists
		else if(inputRange.getStartVal() != Constants.DEFAULT_NA && constraintRange.getStartVal() != Constants.DEFAULT_NA && 
				constraintRange.getEndVal() != Constants.DEFAULT_NA){
			//If Input start range is closer to Constraint start range
			if((inputRange.getStartVal() - constraintRange.getStartVal()) < 
					(constraintRange.getEndVal() - inputRange.getStartVal())){
				roundingLogic = PRConstants.ROUND_UP;	
			}else{
				roundingLogic = PRConstants.ROUND_DOWN;			
			}
		}
		//When both range of Constraint and End Range of Input exists
		else if(inputRange.getEndVal() != Constants.DEFAULT_NA && constraintRange.getStartVal() != Constants.DEFAULT_NA && 
				constraintRange.getEndVal() != Constants.DEFAULT_NA){
			//If Input end range is closer to Constraint end range
			if((inputRange.getEndVal() - constraintRange.getEndVal()) < 
					(constraintRange.getStartVal() - inputRange.getEndVal())){
				roundingLogic = PRConstants.ROUND_DOWN;	
			}else{
				roundingLogic = PRConstants.ROUND_UP;			
			}
		}
		return roundingLogic;
	}
	
	public void updateRoundingLogic(PRItemDTO itemDTO, PRRange inputRange, PRRange guidelineOrConstraintRange, 
			PRRange filteredRange, String relationOperator) {
		
		//When both the ends of the input range is not defined, find rounding logic based on guideline range 
		//(This is applicable, for the first guideline applied) 
		if (inputRange.getStartVal() == Constants.DEFAULT_NA
				&& inputRange.getEndVal() == Constants.DEFAULT_NA) {
			// If both the range present
			if (guidelineOrConstraintRange.getStartVal() != Constants.DEFAULT_NA
					&& guidelineOrConstraintRange.getEndVal() != Constants.DEFAULT_NA) {
				//Note: When there is no relational operator or if the operator is equals. This is ignored here and 
				//existing rounding logic is carried or default rounding or rounding based on threshold will be used
				if (relationOperator != null && relationOperator != "") {
					if (PRConstants.PRICE_GROUP_EXPR_ABOVE.equals(relationOperator)
							|| PRConstants.PRICE_GROUP_EXPR_GREATER_SYM.equals(relationOperator)
							|| PRConstants.PRICE_GROUP_EXPR_GREATER_THAN_EQUAL_SYM.equals(relationOperator)) {
						itemDTO.setRoundingLogic(PRConstants.ROUND_UP);
					}else if(PRConstants.PRICE_GROUP_EXPR_BELOW.equals(relationOperator)
							|| PRConstants.PRICE_GROUP_EXPR_LESSER_SYM.equals(relationOperator)
							|| PRConstants.PRICE_GROUP_EXPR_LESSER_THAN_EQUAL_SYM.equals(relationOperator)){
						itemDTO.setRoundingLogic(PRConstants.ROUND_DOWN);
					}
				}
			}
			// Start val alone present
			else if (guidelineOrConstraintRange.getStartVal() != Constants.DEFAULT_NA
					&& guidelineOrConstraintRange.getEndVal() == Constants.DEFAULT_NA) {
				// should not recommend below
				itemDTO.setRoundingLogic(PRConstants.ROUND_UP);
			}
			// End val alone present
			else if (guidelineOrConstraintRange.getStartVal() == Constants.DEFAULT_NA
					&& guidelineOrConstraintRange.getEndVal() != Constants.DEFAULT_NA) {
				// should not recommend above
				itemDTO.setRoundingLogic(PRConstants.ROUND_DOWN);
			}	
		}
		//When input range is conflict with guideline range and if the input range has both ends defined, then change the rounding logic
		//When input range is not conflict with guideline range, then carry the rounding logic
		else if (inputRange.getStartVal() != Constants.DEFAULT_NA
				&& inputRange.getEndVal() != Constants.DEFAULT_NA) {
			if(guidelineOrConstraintRange.isConflict()){
				//check if filtered range used start / end val of the input range
				//input range 2.69 -- 2.89, guideline 3.26 -- , filtered range 2.89 -- 2.89
				if(filteredRange.getStartVal() == inputRange.getStartVal()){
					itemDTO.setRoundingLogic(PRConstants.ROUND_UP);
				}else{
					itemDTO.setRoundingLogic(PRConstants.ROUND_DOWN);
				}
			}			
		}
	}
	
	public List<Double> getRoundingDigitsWithinRange(double roundingDigits[], PRRange guidelineOrConstraintRange){
		List<Double> newRangeList = new ArrayList<Double>();
		for (double price : roundingDigits) {
			if (guidelineOrConstraintRange.getStartVal() != Constants.DEFAULT_NA && guidelineOrConstraintRange.getEndVal() != Constants.DEFAULT_NA) {
				if (price >= guidelineOrConstraintRange.getStartVal() && price <= guidelineOrConstraintRange.getEndVal()) {
					newRangeList.add(price);
				}
			} else if (guidelineOrConstraintRange.getStartVal() != Constants.DEFAULT_NA) {
				if (price >= guidelineOrConstraintRange.getStartVal()) {
					newRangeList.add(price);
				}
			} else if (guidelineOrConstraintRange.getEndVal() != Constants.DEFAULT_NA) {
				if (price <= guidelineOrConstraintRange.getEndVal()) {
					newRangeList.add(price);
				}
			}
		}
		return newRangeList;
	}
	
	public PRRange filterGuardrrailRange(PRRange compRange, PRRange guidelineRange) {
		PRRange range = new PRRange();
		range.copy(guidelineRange);

		if (range.getStartVal() != Constants.DEFAULT_NA)
			range.setStartVal(PRFormatHelper.roundToTwoDecimalDigitAsDouble(range.getStartVal()));
		if (range.getEndVal() != Constants.DEFAULT_NA)
			range.setEndVal(PRFormatHelper.roundToTwoDecimalDigitAsDouble(range.getEndVal()));

		if (compRange != null) {

			if (compRange.getStartVal() != Constants.DEFAULT_NA)
				if (range.getStartVal() > compRange.getStartVal())
					range.setStartVal(compRange.getStartVal());
		}
		if (compRange.getEndVal() != Constants.DEFAULT_NA) {
			if (range.getEndVal() > compRange.getEndVal())
				range.setEndVal(compRange.getEndVal());
		}

		return range;
	}
	
	
	
}
