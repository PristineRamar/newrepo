package com.pristine.service.offermgmt.constraint;

import org.apache.log4j.Logger;

import com.pristine.dto.offermgmt.PRExplainLog;
import com.pristine.dto.offermgmt.PRGuidelineAndConstraintLog;
import com.pristine.dto.offermgmt.PRGuidelineAndConstraintOutput;
import com.pristine.dto.offermgmt.PRItemDTO;
import com.pristine.dto.offermgmt.PRRange;
import com.pristine.service.offermgmt.ConstraintTypeLookup;
import com.pristine.service.offermgmt.FilterPriceRange;
import com.pristine.util.Constants;
//import com.pristine.util.PropertyManager;
import com.pristine.util.offermgmt.PRConstants;

public class LowerHigherConstraint {
	private static Logger logger = Logger.getLogger("LowerHigherConstraint");
	private PRItemDTO inputItem;
	private PRRange inputPriceRange;
	private PRExplainLog explainLog;
	
	FilterPriceRange filterPriceRange = new FilterPriceRange();	
	PRGuidelineAndConstraintLog guidelineAndConstraintLog = new PRGuidelineAndConstraintLog();
	
	public LowerHigherConstraint(PRItemDTO inputItem, PRRange inputPriceRange, PRExplainLog explainLog) {
		this.inputItem = inputItem;
		this.inputPriceRange = inputPriceRange;
		this.explainLog = explainLog;		
		guidelineAndConstraintLog.setConstraintTypeId(ConstraintTypeLookup.LOWER_HIGHER.getConstraintTypeId());
	}
	
	public PRGuidelineAndConstraintOutput applyLowerHigherConstraint() {
		PRRange filteredPriceRange;
		PRRange lowerHigherRange = new PRRange();
		PRGuidelineAndConstraintOutput guidelineAndConstraintOutput = new PRGuidelineAndConstraintOutput();
		guidelineAndConstraintOutput.outputPriceRange = inputPriceRange;

		if (inputItem.getStrategyDTO().getConstriants().getLowerHigherConstraint() == null){}
		else if ((inputItem.getStrategyDTO().getConstriants().getLowerHigherConstraint()
				.getLowerHigherRetailFlag() == PRConstants.CONSTRAINT_NO_LOWER_HIGHER)) {
		} else if (inputItem.getRegPrice() == null) {
			// Its not possible to apply this constraint when there is no current retail
			guidelineAndConstraintLog.setIsGuidelineOrConstraintApplied(false);
			guidelineAndConstraintLog.setMessage("Current Retail not available");
			guidelineAndConstraintLog.setOutputPriceRange(guidelineAndConstraintOutput.outputPriceRange);
			explainLog.getGuidelineAndConstraintLogs().add(guidelineAndConstraintLog);
		} else {
			lowerHigherRange = applyLowerHigherConstraint(inputItem);		
			
			//If min/max is already applied don't break it
			//Break lower/higher Constraint
			if (filterPriceRange.checkIfConstraintApplied(explainLog, ConstraintTypeLookup.MIN_MAX.getConstraintTypeId())) {
				filteredPriceRange = filterPriceRange.filterRange(inputPriceRange, lowerHigherRange);
			}else {
				PRRange costRange = filterPriceRange.getConstraintRange(explainLog, ConstraintTypeLookup.COST.getConstraintTypeId());
				
				//Don't break lower/higher Constraint
				filteredPriceRange = filterPriceRange.filterRange(lowerHigherRange, inputPriceRange);
				
				/** Find Rounding Logic **/
				if(filteredPriceRange.isConflict()){
					if(inputItem.getStrategyDTO().getConstriants().getLowerHigherConstraint().getLowerHigherRetailFlag()
							== PRConstants.CONSTRAINT_HIGHER){		
						inputItem.setRoundingLogic(PRConstants.ROUND_UP);
						guidelineAndConstraintLog.setIsBreakingConstraint(true);
					}else{
						inputItem.setRoundingLogic(PRConstants.ROUND_DOWN);
						guidelineAndConstraintLog.setIsBreakingConstraint(true);
					}
				}
				if (inputItem.getStrategyDTO().getConstriants().getLowerHigherConstraint().getLowerHigherRetailFlag() == PRConstants.CONSTRAINT_LOWER) {
					
					//(make sure rounding up doesn't break lower constraint) there may be range which will break the lower
					//e.g. Cost Range: 14.29 - , Input Range 15.22 - 15.28, rounding up here will break the lower constraint	
					double rd[] = inputItem.getStrategyDTO().getConstriants().getRoundingConstraint()
							.getRange(inputPriceRange.getEndVal(), lowerHigherRange.getStartVal());

					if (rd.length == 0) {
						inputItem.setRoundingLogic(PRConstants.ROUND_DOWN);
						guidelineAndConstraintLog.setIsBreakingConstraint(true);
					}
					
					//Make sure rounding down doesn't break cost constraint					
					if (costRange.getStartVal() != Constants.DEFAULT_NA) {
						double rd1[] = inputItem.getStrategyDTO().getConstriants().getRoundingConstraint()
								.getRange(costRange.getStartVal(), lowerHigherRange.getEndVal());

						if (rd1.length == 0) {
							inputItem.setRoundingLogic(PRConstants.ROUND_UP);
							guidelineAndConstraintLog.setIsBreakingConstraint(true);
						}
					}					

				}
				/** Find Rounding Logic **/
				
				//If cost constraint is already carried				
				//Cost constraint can't be broken at any situation (give precedence to Cost and check if cost constraint is followed)
				PRRange filterByCost = filterPriceRange.filterRange(costRange, filteredPriceRange);
				if(filterByCost.isConflict()){
					filteredPriceRange = filterByCost;
					inputItem.setRoundingLogic(PRConstants.ROUND_UP);
					guidelineAndConstraintLog.setIsBreakingConstraint(true);
				}
			}
			
			
			guidelineAndConstraintLog.setIsGuidelineOrConstraintApplied(true);
			guidelineAndConstraintLog.setGuidelineOrConstraintPriceRange1(lowerHigherRange);
			
			//If Cost is not available pass input range as output range
			if ((inputItem.getListCost() == null || inputItem.getListCost() <= 0)
					&& (inputItem.getRegPrice() != null && inputItem.getRegPrice() > 0)) {
				guidelineAndConstraintOutput.outputPriceRange = inputPriceRange;
				guidelineAndConstraintLog.setOutputPriceRange(inputPriceRange);
			}
			else{
				guidelineAndConstraintOutput.outputPriceRange = filteredPriceRange;
				guidelineAndConstraintLog.setOutputPriceRange(guidelineAndConstraintOutput.outputPriceRange);
			}
			explainLog.getGuidelineAndConstraintLogs().add(guidelineAndConstraintLog);
		}

//		logger.debug("Lower/Higher Constraint -- " + 
//				"Is Applied: " + guidelineAndConstraintLog.getIsGuidelineOrConstraintApplied() +
//				",Input Range: " + inputPriceRange.toString() + 
//				",Lower/Higher Range: " + lowerHigherRange.toString() + 
//				",Output Range: " + guidelineAndConstraintOutput.outputPriceRange.toString());
		
		return guidelineAndConstraintOutput;
	}
	
	public PRRange applyLowerHigherConstraint(PRItemDTO inputItem){
		PRRange lowerHigherPriceRange = new PRRange();
		double price = inputItem.getRegPrice();
		//Higher Range
		if(inputItem.getStrategyDTO().getConstriants().getLowerHigherConstraint().getLowerHigherRetailFlag()
				== PRConstants.CONSTRAINT_HIGHER){			 		
			//lowerHigherPriceRange.setStartVal(price + 0.01);
			lowerHigherPriceRange.setStartVal(price);
			
		}else {			
			//Lower Range
			/*if (price - 0.01 <= 0) {				 
				double pct = Double.parseDouble(PropertyManager.getProperty("PR_SIZE_RELATION_PCT_DIFF", "1"));
				lowerHigherPriceRange.setEndVal(price - (price * pct / 100));
			} else
				lowerHigherPriceRange.setEndVal((price - 0.01));*/
			
			lowerHigherPriceRange.setEndVal(price);			 
		}				
		return lowerHigherPriceRange;
	}
}
