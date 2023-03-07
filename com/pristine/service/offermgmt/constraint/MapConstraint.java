package com.pristine.service.offermgmt.constraint;

import org.apache.log4j.Logger;

import com.pristine.dto.offermgmt.PRExplainLog;
import com.pristine.dto.offermgmt.PRGuidelineAndConstraintLog;
import com.pristine.dto.offermgmt.PRGuidelineAndConstraintOutput;
import com.pristine.dto.offermgmt.PRItemDTO;
import com.pristine.dto.offermgmt.PRRange;
import com.pristine.service.offermgmt.ConstraintTypeLookup;
import com.pristine.service.offermgmt.FilterPriceRange;
import com.pristine.util.PropertyManager;
import com.pristine.util.offermgmt.PRConstants;

public class MapConstraint {
	private static Logger logger = Logger.getLogger("MapConstraint");
	private PRItemDTO inputItem;
	private PRRange inputPriceRange;
	private PRExplainLog explainLog;
	
	FilterPriceRange filterPriceRange = new FilterPriceRange();	
	PRGuidelineAndConstraintLog guidelineAndConstraintLog = new PRGuidelineAndConstraintLog();
	
	public MapConstraint(PRItemDTO inputItem, PRRange inputPriceRange, PRExplainLog explainLog) {
		this.inputItem = inputItem;
		this.inputPriceRange = inputPriceRange;
		this.explainLog = explainLog;		
		
	}
	
	public PRGuidelineAndConstraintOutput applyMapConstraint() {
		return applyMapConstraint(false);
	}
	
	private PRGuidelineAndConstraintOutput applyMapConstraint(boolean passInputRangeAsOutputRange) {
		PRRange filteredPriceRange;
		PRRange mapRange = new PRRange();
		PRGuidelineAndConstraintOutput guidelineAndConstraintOutput = new PRGuidelineAndConstraintOutput();
		guidelineAndConstraintOutput.outputPriceRange = inputPriceRange;
		
		//Code changes to use the max map retail for LIG members 
		/**PRES-213 change**/
		
		
		
		double mapRetail = inputItem.getMapRetail();
		
		 if (mapRetail==0) {
			
			// Map Constraint can't be applied when there is no Map retail
			guidelineAndConstraintLog.setIsGuidelineOrConstraintApplied(false);
			guidelineAndConstraintLog.setMessage("MAP Retail Not Available");
			guidelineAndConstraintLog.setOutputPriceRange(guidelineAndConstraintOutput.outputPriceRange);
			explainLog.getGuidelineAndConstraintLogs().add(guidelineAndConstraintLog);
		} else {
			guidelineAndConstraintLog.setConstraintTypeId(ConstraintTypeLookup.MAP.getConstraintTypeId());
			mapRange = applyMapConstraint(inputItem);

				filteredPriceRange = filterPriceRange.filterRange(mapRange, inputPriceRange);
				String roundingConfig = PropertyManager.getProperty("ROUNDING_LOGIC", PRConstants.ROUND_UP);
				
				double rd[] = inputItem.getStrategyDTO().getConstriants().getRoundingConstraint()
						.getRange(mapRange.getStartVal(), filteredPriceRange.getStartVal(), roundingConfig);

			if (filteredPriceRange.isConflict() || rd.length == 0) {
				inputItem.setRoundingLogic(PRConstants.ROUND_UP);
				guidelineAndConstraintLog.setIsBreakingConstraint(true);

			}

			guidelineAndConstraintOutput.outputPriceRange = filteredPriceRange;
			guidelineAndConstraintLog.setIsGuidelineOrConstraintApplied(true);
			guidelineAndConstraintLog.setGuidelineOrConstraintPriceRange1(mapRange);
			
			guidelineAndConstraintLog.setOutputPriceRange(filteredPriceRange);
			explainLog.getGuidelineAndConstraintLogs().add(guidelineAndConstraintLog);
			
			logger.debug("Map  Constraint -- " + "Is Applied: to item " + inputItem.getItemCode() + " is gd applied "
			+ guidelineAndConstraintLog.getIsGuidelineOrConstraintApplied() + ",Input Range: "
			+ inputPriceRange.toString() + ",Map Range: " + mapRange.toString() + ",Output Range: "
			+ guidelineAndConstraintOutput.outputPriceRange.toString());
		}

		

		return guidelineAndConstraintOutput;
	}
	

	
	private PRRange applyMapConstraint(PRItemDTO inputItem) {
		PRRange range = new PRRange();
		range.setStartVal(inputItem.getMapRetail());

		return range;
	}
}

