package com.pristine.service.offermgmt.constraint;

//import org.apache.log4j.Logger;

import com.pristine.dto.offermgmt.PRConstraintMinMax;
import com.pristine.dto.offermgmt.PRConstraintsDTO;
import com.pristine.dto.offermgmt.PRExplainLog;
import com.pristine.dto.offermgmt.PRGuidelineAndConstraintLog;
import com.pristine.dto.offermgmt.PRGuidelineAndConstraintOutput;
import com.pristine.dto.offermgmt.PRItemDTO;
import com.pristine.dto.offermgmt.PRRange;
import com.pristine.dto.offermgmt.PRStrategyDTO;
import com.pristine.service.offermgmt.ConstraintTypeLookup;
import com.pristine.service.offermgmt.FilterPriceRange;
//import com.pristine.util.offermgmt.PRConstants;

public class MinMaxConstraint {
	//private static Logger logger = Logger.getLogger("MinMaxConstraint");
	private PRItemDTO inputItem;
	private PRRange inputPriceRange;
	private PRExplainLog explainLog;
	
	FilterPriceRange filterPriceRange = new FilterPriceRange();	
	PRGuidelineAndConstraintLog guidelineAndConstraintLog = new PRGuidelineAndConstraintLog();
	
	public MinMaxConstraint(PRItemDTO inputItem, PRRange inputPriceRange, PRExplainLog explainLog) {
		this.inputItem = inputItem;
		this.inputPriceRange = inputPriceRange;
		this.explainLog = explainLog;		
		guidelineAndConstraintLog.setConstraintTypeId(ConstraintTypeLookup.MIN_MAX.getConstraintTypeId());
	}
	
	
	public PRGuidelineAndConstraintOutput applyMinMaxConstraint() {
		PRStrategyDTO strategyDTO = inputItem.getStrategyDTO();	 
		PRGuidelineAndConstraintOutput guidelineAndConstraintOutput = new PRGuidelineAndConstraintOutput();			
		PRRange filteredPriceRange;
		PRRange minMaxPriceRange =  new PRRange();
		
		guidelineAndConstraintOutput.outputPriceRange = inputPriceRange;		
		
		minMaxPriceRange = applyMinMaxConstraint(strategyDTO);
		//Make Sure min/max Constraint is defined
		if(minMaxPriceRange != null){		
			//Don't break min/max Constraint
			filteredPriceRange = filterPriceRange.filterRange(minMaxPriceRange, inputPriceRange);
			
			
			guidelineAndConstraintLog.setIsGuidelineOrConstraintApplied(true);
			guidelineAndConstraintLog.setGuidelineOrConstraintPriceRange1(minMaxPriceRange);
			
			//If Cost is not available pass input range as output range
			if ((inputItem.getListCost() == null || inputItem.getListCost() <= 0)
					&& (inputItem.getRegPrice() != null && inputItem.getRegPrice() > 0)){
				guidelineAndConstraintOutput.outputPriceRange = inputPriceRange;	
				guidelineAndConstraintLog.setOutputPriceRange(inputPriceRange);					
			}
			else{
				guidelineAndConstraintOutput.outputPriceRange = filteredPriceRange;		
				guidelineAndConstraintLog.setOutputPriceRange(guidelineAndConstraintOutput.outputPriceRange);
			}
				
			explainLog.getGuidelineAndConstraintLogs().add(guidelineAndConstraintLog);
		}
//		logger.debug("Min/Max Constraint -- " + 
//				"Is Applied: " + guidelineAndConstraintLog.getIsGuidelineOrConstraintApplied() +
//				",Input Range: " + inputPriceRange.toString() + 
//				",Min/Max Range: " + (minMaxPriceRange != null ?  minMaxPriceRange.toString() : "- -") + 
//				",Output Range: " + guidelineAndConstraintOutput.outputPriceRange.toString());
		
		return guidelineAndConstraintOutput;
	}
	
	private PRRange applyMinMaxConstraint(PRStrategyDTO strategyDTO){
		PRRange range  = null;
		PRConstraintsDTO constraintDTO = strategyDTO.getConstriants();
		if(constraintDTO != null){
			PRConstraintMinMax minMaxConstraint = constraintDTO.getMinMaxConstraint();
			if(minMaxConstraint != null){
				if(minMaxConstraint.getMinValue() == 0 && minMaxConstraint.getMaxValue() == 0) {
					if(inputItem.getMinRetail() > 0 || inputItem.getMaxRetail() > 0) {
						range = PRConstraintMinMax.getPriceRange(inputItem.getMinRetail(), inputItem.getMaxRetail());		
					}
				} else {
					range = minMaxConstraint.getPriceRange();	
				}
			}else if (inputItem.getMinRetail() > 0 || inputItem.getMaxRetail() > 0) {
				range = PRConstraintMinMax.getPriceRange(inputItem.getMinRetail(), inputItem.getMaxRetail());
			}
		}
		return range;
	}
}
