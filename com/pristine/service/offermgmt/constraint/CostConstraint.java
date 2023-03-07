package com.pristine.service.offermgmt.constraint;

import org.apache.log4j.Logger;

import com.pristine.dto.offermgmt.PRExplainLog;
import com.pristine.dto.offermgmt.PRGuidelineAndConstraintLog;
import com.pristine.dto.offermgmt.PRGuidelineAndConstraintOutput;
import com.pristine.dto.offermgmt.PRItemDTO;
import com.pristine.dto.offermgmt.PRRange;
import com.pristine.service.offermgmt.ConstraintTypeLookup;
import com.pristine.service.offermgmt.FilterPriceRange;
import com.pristine.util.offermgmt.PRConstants;

public class CostConstraint {
	private static Logger logger = Logger.getLogger("CostConstraint");
	private PRItemDTO inputItem;
	private PRRange inputPriceRange;
	private PRExplainLog explainLog;
	
	FilterPriceRange filterPriceRange = new FilterPriceRange();	
	PRGuidelineAndConstraintLog guidelineAndConstraintLog = new PRGuidelineAndConstraintLog();
	
	public CostConstraint(PRItemDTO inputItem, PRRange inputPriceRange, PRExplainLog explainLog) {
		this.inputItem = inputItem;
		this.inputPriceRange = inputPriceRange;
		this.explainLog = explainLog;		
		guidelineAndConstraintLog.setConstraintTypeId(ConstraintTypeLookup.COST.getConstraintTypeId());
	}
	
	public PRGuidelineAndConstraintOutput applyCostConstraint() {
		return applyCostConstraint(false);
	}
	
	private PRGuidelineAndConstraintOutput applyCostConstraint(boolean passInputRangeAsOutputRange) {
		PRRange filteredPriceRange;
		PRRange costPriceRange = new PRRange();
		PRGuidelineAndConstraintOutput guidelineAndConstraintOutput = new PRGuidelineAndConstraintOutput();
		guidelineAndConstraintOutput.outputPriceRange = inputPriceRange;

		if (inputItem.getStrategyDTO().getConstriants().getCostConstraint() == null) {
		} else if (inputItem.getStrategyDTO().getConstriants().getCostConstraint().getIsRecBelowCost()) {
		} else if (inputItem.getListCost() == null) {
			// Cost Constraint can't be applied when there is no cost
			guidelineAndConstraintLog.setIsGuidelineOrConstraintApplied(false);
			guidelineAndConstraintLog.setMessage("Cost not available");
			guidelineAndConstraintLog.setOutputPriceRange(guidelineAndConstraintOutput.outputPriceRange);
			explainLog.getGuidelineAndConstraintLogs().add(guidelineAndConstraintLog);
		} else {
			costPriceRange = applyCostConstraint(inputItem);

			// Break Cost Constraint
			if (filterPriceRange.checkIfConstraintApplied(explainLog,
					ConstraintTypeLookup.MIN_MAX.getConstraintTypeId())) {
				filteredPriceRange = filterPriceRange.filterRange(inputPriceRange, costPriceRange);
			} else {
				// Don't break Cost Constraint
				filteredPriceRange = filterPriceRange.filterRange(costPriceRange, inputPriceRange);

				// (make sure rounding down doesn't break cost constraint) there
				// may be range which will break the cost
				// e.g. Cost Range: 2.50 - , Input Range 2.52 - 3.59, rounding
				// down here will break the cost constraint
				double rd[] = inputItem.getStrategyDTO().getConstriants().getRoundingConstraint()
						.getRange(costPriceRange.getStartVal(), filteredPriceRange.getStartVal(), "NO_CLOSEST");

				if (filteredPriceRange.isConflict() || rd.length == 0) {
					inputItem.setRoundingLogic(PRConstants.ROUND_UP);
					guidelineAndConstraintLog.setIsBreakingConstraint(true);
				}
			}

			guidelineAndConstraintOutput.outputPriceRange = filteredPriceRange;
			guidelineAndConstraintLog.setIsGuidelineOrConstraintApplied(true);
			guidelineAndConstraintLog.setGuidelineOrConstraintPriceRange1(costPriceRange);
			if(passInputRangeAsOutputRange)
				guidelineAndConstraintLog.setOutputPriceRange(inputPriceRange);
			else
				guidelineAndConstraintLog.setOutputPriceRange(filteredPriceRange);
			explainLog.getGuidelineAndConstraintLogs().add(guidelineAndConstraintLog);
		}

//		logger.debug("Cost Constraint -- " + "Is Applied: "
//				+ guidelineAndConstraintLog.getIsGuidelineOrConstraintApplied() + ",Input Range: "
//				+ inputPriceRange.toString() + ",Cost Range: " + costPriceRange.toString() + ",Output Range: "
//				+ guidelineAndConstraintOutput.outputPriceRange.toString());

		return guidelineAndConstraintOutput;
	}
	
	public PRGuidelineAndConstraintOutput getCostRange() {
		return applyCostConstraint(true);
	}
	
	private PRRange applyCostConstraint(PRItemDTO inputItem){
		PRRange range  = new PRRange();
		//15th Jan 2015 -- Use VIP Cost while applying cost constraint (if it is available)
		if(inputItem.getVipCost() != null){
			range.setStartVal(inputItem.getVipCost());
		}else if (inputItem.getFutureListCost() != null && inputItem.getCostChgIndicator()!=0) {
			range.setStartVal(inputItem.getFutureListCost());
		}else if (inputItem.getListCost() != null) {
			range.setStartVal(inputItem.getListCost());
		}
		return range;
	}
}
