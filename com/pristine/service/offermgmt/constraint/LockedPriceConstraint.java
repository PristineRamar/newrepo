package com.pristine.service.offermgmt.constraint;

//import com.jcraft.jsch.Logger;

//import org.apache.log4j.Logger;

import com.pristine.dto.offermgmt.MultiplePrice;
import com.pristine.dto.offermgmt.PRConstraintLocPrice;
import com.pristine.dto.offermgmt.PRExplainLog;
import com.pristine.dto.offermgmt.PRGuidelineAndConstraintLog;
import com.pristine.dto.offermgmt.PRGuidelineAndConstraintOutput;
import com.pristine.dto.offermgmt.PRItemDTO;
import com.pristine.dto.offermgmt.PRRange;
import com.pristine.dto.offermgmt.PRStrategyDTO;
import com.pristine.service.offermgmt.ConstraintTypeLookup;
import com.pristine.service.offermgmt.FilterPriceRange;
import com.pristine.util.offermgmt.PRCommonUtil;
import com.pristine.util.offermgmt.PRFormatHelper;

public class LockedPriceConstraint {
	//private static Logger logger = Logger.getLogger("LockedPriceConstraint");
	private PRItemDTO inputItem;
	private PRExplainLog explainLog;
	//private String costChangeLogic;
	
	FilterPriceRange filterPriceRange = new FilterPriceRange();	
	PRGuidelineAndConstraintLog guidelineAndConstraintLog = new PRGuidelineAndConstraintLog();
	
	public LockedPriceConstraint(PRItemDTO inputItem, PRExplainLog explainLog, String costChangeLogic) {
		this.inputItem = inputItem;
		this.explainLog = explainLog;		
	//	this.costChangeLogic = costChangeLogic;
		guidelineAndConstraintLog.setConstraintTypeId(ConstraintTypeLookup.LOCKED_PRICE.getConstraintTypeId());
	}
	
	public void applyLockedPriceConstraint(String applyMapConstraint) {
		double lockedPrice = 0;
		int multiple = 1;
		PRStrategyDTO strategyDTO = inputItem.getStrategyDTO();
		PRConstraintLocPrice locPriceConstraint = strategyDTO.getConstriants().getLocPriceConstraint();
		PRRange priceRange = new PRRange();
		PRGuidelineAndConstraintOutput guidelineAndConstraintOutputLocal;
		Double[] priceRangeArr = null;

		
		multiple = locPriceConstraint.getQuantity() == 0 ? 1 : locPriceConstraint.getQuantity();
		lockedPrice = locPriceConstraint.getValue();	
		
		lockedPrice = PRFormatHelper.roundToTwoDecimalDigitAsDouble(lockedPrice);
		
		MultiplePrice curRegPrice = PRCommonUtil.getMultiplePrice(inputItem.getRegMPack(), inputItem.getRegPrice(),
				inputItem.getRegMPrice());
		
		if (lockedPrice == 0 && inputItem.getLockedRetail() == 0) {
			lockedPrice = PRCommonUtil.getUnitPrice(curRegPrice, true);
		} else if (lockedPrice == 0) {

			lockedPrice = inputItem.getLockedRetail();

		}
		
		double actLockedPrice = lockedPrice;
		
		
		priceRange.setStartVal(lockedPrice);
		priceRange.setEndVal(lockedPrice);		
		guidelineAndConstraintLog.setIsGuidelineOrConstraintApplied(true);
		guidelineAndConstraintLog.setGuidelineOrConstraintPriceRange1(priceRange);
		guidelineAndConstraintLog.setOutputPriceRange(priceRange);
		explainLog.getGuidelineAndConstraintLogs().add(guidelineAndConstraintLog);

		/*
		 * Code refactored on 02/17/2022 by Karishma for AZ and added the existing
		 * working in else part to work as it is for other clients . If item is on
		 * Locked Price and there is a MAP Retail, then locked price should be compared
		 * with MAP. If lock is below MAP then MAP should be recommended by applying the
		 * rounding rules else locked retail should be recommended as received
		 */
		if (applyMapConstraint.equalsIgnoreCase("TRUE")) {
			
			MapConstraint mapConstraint = new MapConstraint(inputItem, priceRange, explainLog);
			guidelineAndConstraintOutputLocal = mapConstraint.applyMapConstraint();
			priceRange = guidelineAndConstraintOutputLocal.outputPriceRange;
			/**PROM-2281 ISSUE FIX FOR NOT USING ROUNDING FOR LOCKED RETAILS Change**/
			priceRangeArr = strategyDTO.getConstriants().getRoundingConstraint().getRoundingDigits(inputItem,
					priceRange, explainLog);

			double mapRetail = inputItem.getMapRetail();
			

			if (mapRetail > 0) {
				if (actLockedPrice == 0) {
					//If the item is locked at current Price ,check if its not below the map
					if (curRegPrice.getUnitPrice() >= mapRetail) {
						inputItem.setPriceRange(new Double[] { curRegPrice.getUnitPrice() });
						inputItem.setRecommendedRegPrice(curRegPrice);
					} else {
						// If MAP retail is applied instead of current retail  then use MAP value with rounding
						inputItem.setPriceRange(priceRangeArr);
						inputItem.setRecommendedRegPrice(new MultiplePrice(multiple, priceRangeArr[0]));
					}

				} else if (lockedPrice >= mapRetail) {
					inputItem.setPriceRange(new Double[] { lockedPrice });
					inputItem.setRecommendedRegPrice(new MultiplePrice(multiple, lockedPrice));
				} else {

					// If MAP retail is applied instead of locked retail then
					// apply MAP retail with rounding constraint
					inputItem.setPriceRange(priceRangeArr);
					inputItem.setRecommendedRegPrice(new MultiplePrice(multiple, priceRangeArr[0]));
				}

			} else {
				inputItem.setPriceRange(new Double[] { lockedPrice });
				if (actLockedPrice == 0) {
					inputItem.setRecommendedRegPrice(curRegPrice);
				} else {
					inputItem.setRecommendedRegPrice(new MultiplePrice(multiple, lockedPrice));
				}
			}
			
			
			inputItem.setExplainLog(explainLog);
			
			/**PROM-2281 ISSUE FIX FOR NOT USING ROUNDING FOR LOCKED RETAILS END **/
		
		} else {
			//Commented  code to check fo cost change or apply rounding constraint for locked startegy
			// for FF 08/19/22 
			/**
			if (inputItem.getCostChgIndicator() != 0
					&& inputItem.getObjectiveTypeId() != ObjectiveTypeLookup.HIGHEST_MARGIN_DOLLAR_USING_CURRENT_RETAIL
							.getObjectiveTypeId()) {
				// Apply margin, cost, rounding constraint
				// Check if margin guideline available
				if (strategyDTO.getGuidelines().getMarginGuideline() != null) {
					MarginGuideline marginGuideline = new MarginGuideline(inputItem, priceRange, explainLog,
							costChangeLogic);
					guidelineAndConstraintOutputLocal = marginGuideline.applyMarginGuideline();
					priceRange = guidelineAndConstraintOutputLocal.outputPriceRange;
				}

				CostConstraint costConstraint = new CostConstraint(inputItem, priceRange, explainLog);
				guidelineAndConstraintOutputLocal = costConstraint.applyCostConstraint();
				priceRange = guidelineAndConstraintOutputLocal.outputPriceRange;

				priceRangeArr = strategyDTO.getConstriants().getRoundingConstraint().getRoundingDigits(inputItem,
						priceRange, explainLog);

				if (priceRangeArr.length > 0) {
					multiple = 1;
					lockedPrice = priceRangeArr[0];
//				logger.debug("Rounding -- Input Range: "
//						+ priceRange.toString() + ",Output: " + lockedPrice);
				}
			} else {
				// Update margin, cost, rounding range. Don't apply
				if (strategyDTO.getGuidelines().getMarginGuideline() != null) {
					MarginGuideline marginGuideline = new MarginGuideline(inputItem, priceRange, explainLog,
							costChangeLogic);
					guidelineAndConstraintOutputLocal = marginGuideline.getMarginRange();
					priceRange = guidelineAndConstraintOutputLocal.outputPriceRange;
				}

				CostConstraint costConstraint = new CostConstraint(inputItem, priceRange, explainLog);
				guidelineAndConstraintOutputLocal = costConstraint.getCostRange();
				priceRange = guidelineAndConstraintOutputLocal.outputPriceRange;
				priceRangeArr = strategyDTO.getConstriants().getRoundingConstraint().getRoundingDigits(inputItem,
						priceRange, explainLog);
			}
**/
			
			priceRangeArr = strategyDTO.getConstriants().getRoundingConstraint().getRoundingDigits(inputItem,
					priceRange, explainLog);
			inputItem.setPriceRange(new Double[] { lockedPrice });
		
			if (actLockedPrice == 0) {
				inputItem.setRecommendedRegPrice(curRegPrice);
			} else {
				inputItem.setRecommendedRegPrice(new MultiplePrice(multiple, lockedPrice));
			}

			inputItem.setExplainLog(explainLog);
		}
	}
}
