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

public class FreightChargeConstraint {

	private static Logger logger = Logger.getLogger("FreightChargeConstraint");
	private PRItemDTO inputItem;
	private PRRange inputPriceRange;
	private PRExplainLog explainLog;

	FilterPriceRange filterPriceRange = new FilterPriceRange();
	PRGuidelineAndConstraintLog guidelineAndConstraintLog = new PRGuidelineAndConstraintLog();

	public FreightChargeConstraint(PRItemDTO inputItem, PRRange inputPriceRange, PRExplainLog explainLog) {
		this.inputItem = inputItem;
		this.inputPriceRange = inputPriceRange;
		this.explainLog = explainLog;
		guidelineAndConstraintLog.setConstraintTypeId(ConstraintTypeLookup.FREIGHT.getConstraintTypeId());
	}

	
	/** PROM-2274 changes start **/
	/**
	 * Enhancement for AZ to add the freight charge to the cost if its selected from
	 * startegy
	 */
	public void applyFreightChargeToCost() {

		if (inputItem.getStrategyDTO().getConstriants().getFreightChargeConstraint() == null) {
			inputItem.setListCostWtotFrChg(inputItem.getListCost());
		} else {
			if (inputItem.getUserAttr15() != null) {
				inputItem.setListCostWtotFrChg(inputItem.getListCost());
				double freightListCost = inputItem.getListCost() + Double.parseDouble(inputItem.getUserAttr15());
				inputItem.setListCost(freightListCost);
				inputItem.setFreightChargeIncluded(1);
				inputItem.setFreightCostSet(true);
				/*
				 * logger.debug("applyFreightChargeToCost()- item code:" +
				 * inputItem.getItemCode() + "current cost :" + inputItem.getListCostWtotFrChg()
				 * + " freight charge: " + inputItem.getUserAttr15() + " new cost : " +
				 * inputItem.getListCost());
				 */

			}

		}
	}

	/** PROM-2274 changes end **/

	public PRGuidelineAndConstraintOutput applyFreightChargeConstraint() {
		PRGuidelineAndConstraintOutput guidelineAndConstraintOutput = new PRGuidelineAndConstraintOutput();

		guidelineAndConstraintOutput.outputPriceRange = inputPriceRange;
		if (inputItem.getStrategyDTO().getConstriants().getFreightChargeConstraint() == null) {
		} else {
			PRRange freightChargeRange = applyFreightChargeConstraint(inputItem, inputPriceRange);

			if (freightChargeRange != null) {
				// PRRange filteredPriceRange = filterPriceRange.filterRange(freightChargeRange,
				// inputPriceRange);
				guidelineAndConstraintLog.setIsGuidelineOrConstraintApplied(true);
				guidelineAndConstraintLog.setGuidelineOrConstraintPriceRange1(freightChargeRange);

				guidelineAndConstraintOutput.outputPriceRange = freightChargeRange;

				guidelineAndConstraintLog.setOutputPriceRange(guidelineAndConstraintOutput.outputPriceRange);

			}

			explainLog.getGuidelineAndConstraintLogs().add(guidelineAndConstraintLog);
			logger.debug("Freight Charge Constraint -- " + "Is Applied: "
					+ guidelineAndConstraintLog.getIsGuidelineOrConstraintApplied() + ",Input Range: "
					+ inputPriceRange.toString() + ",Freight Range Range: "
					+ (freightChargeRange != null ? freightChargeRange.toString() : "- -") + ",Output Range: "
					+ guidelineAndConstraintOutput.outputPriceRange.toString());

		}
		return guidelineAndConstraintOutput;
	}

	private PRRange applyFreightChargeConstraint(PRItemDTO inputItem, PRRange inputPriceRange) {

		PRRange range = null;

		if (inputItem.getUserAttr15() != null) {
			range = new PRRange();
			if (inputPriceRange.getStartVal() != Constants.DEFAULT_NA) {
				double startVal = inputPriceRange.getStartVal() + Double.parseDouble(inputItem.getUserAttr15());
				range.setStartVal(startVal);
			}
			if (inputPriceRange.getEndVal() != Constants.DEFAULT_NA) {
				double endVal = inputPriceRange.getEndVal() + Double.parseDouble(inputItem.getUserAttr15());
				range.setEndVal(endVal);
			}

		} else {
			guidelineAndConstraintLog.setIsGuidelineOrConstraintApplied(false);
			guidelineAndConstraintLog.setMessage("No Freight Charge Present");
			guidelineAndConstraintLog.setOutputPriceRange(inputPriceRange);
			logger.debug("Freight Charge not present for item" + inputItem.getItemCode());
		}

		return range;
	}

}
