package com.pristine.service.offermgmt.guideline;

import java.util.ArrayList;
import java.util.List;

import org.apache.log4j.Logger;
import com.pristine.dto.offermgmt.PRConstraintThreshold;
import com.pristine.dto.offermgmt.PRExplainLog;
import com.pristine.dto.offermgmt.PRGuidelineAndConstraintLog;
import com.pristine.dto.offermgmt.PRGuidelineAndConstraintOutput;
import com.pristine.dto.offermgmt.PRGuidelineMargin;
import com.pristine.dto.offermgmt.PRItemDTO;
import com.pristine.dto.offermgmt.PRRange;
import com.pristine.dto.offermgmt.PRStrategyDTO;
import com.pristine.service.offermgmt.FilterPriceRange;
import com.pristine.service.offermgmt.GuidelineTypeLookup;
import com.pristine.util.Constants;
import com.pristine.util.PropertyManager;
import com.pristine.util.offermgmt.PRConstants;

public class MarginGuideline {
	private static Logger logger = Logger.getLogger("MarginGuideline");
	private PRItemDTO inputItem;
	private PRRange inputPriceRange;
	private PRExplainLog explainLog;
	private String costChangeLogic;
	
	FilterPriceRange filterPriceRange = new FilterPriceRange();	
	PRGuidelineAndConstraintLog guidelineAndConstraintLog = new PRGuidelineAndConstraintLog();
	boolean chkFutureCost = Boolean.parseBoolean(PropertyManager.getProperty("USE_FUTURECOST", "FALSE"));

	public MarginGuideline(PRItemDTO inputItem, PRRange inputPriceRange, PRExplainLog explainLog, String costChangeLogic) {
		this.inputItem = inputItem;
		this.inputPriceRange = inputPriceRange;
		this.explainLog = explainLog;
		this.costChangeLogic = costChangeLogic;
	}

	public PRGuidelineAndConstraintOutput applyMarginGuideline() {
		return applyMarginGuideline(false);
	}
	
	public PRGuidelineAndConstraintOutput getMarginRange() {
		return applyMarginGuideline(true);
	}
	
	private PRGuidelineAndConstraintOutput applyMarginGuideline(boolean passInputRangeAsOutputRange) {
		PRStrategyDTO strategyDTO = inputItem.getStrategyDTO();	 
		PRRange marginPriceRange = null;
		PRGuidelineAndConstraintOutput guidelineAndConstraintOutput = new PRGuidelineAndConstraintOutput();		 
		PRRange filteredPriceRange;
		// Apply margin guideline.
		/* If margin guideline is maintain current margin, do not apply guideline when current cost and previous cost are not available If
		 * margin guideline is mentioned as % or $, do not apply guideline when current cost is not available. Append log messages accordingly.
		 */
		List<PRGuidelineMargin> marginGuidelines = strategyDTO.getGuidelines().getMarginGuideline();
		//Find out which margin guideline to be used
		PRGuidelineMargin choosenGuidelineMargin = findMarginGuideline(marginGuidelines, inputItem);
		guidelineAndConstraintLog.setGuidelineTypeId(GuidelineTypeLookup.MARGIN.getGuidelineTypeId());
		

		if (inputItem.getListCost() != null && inputItem.getListCost() > 0) {
			// If there is no margin guideline
			if (choosenGuidelineMargin != null) {
				char isCurrentMargin = choosenGuidelineMargin.getCurrentMargin();

				// Handling Current Margin Guideline
				if (PRConstants.VALUE_TYPE_PCT == isCurrentMargin || PRConstants.VALUE_TYPE_$ == isCurrentMargin
						|| PRConstants.VALUE_TYPE_I == isCurrentMargin) {
					if (inputItem.getRegPrice() != null) {
						//Added existing property check to the function because future cost functionality is applicable for RA only and not FF
						///For FF future cost is for display purpose
						if (chkFutureCost && inputItem.getFutureListCost() != null && inputItem.getFutureListCost() > 0  && inputItem.getCostChgIndicator()!=0) {
							// Set Margin Min and Max Range by using future cost as current cost
							marginPriceRange = applyMarginGuideline(choosenGuidelineMargin,
									inputItem.getFutureListCost(), inputItem.getListCost(), inputItem.getRegPrice(),
									inputItem.getCostChgIndicator(),
									strategyDTO.getConstriants().getThresholdConstraint());
							// Adjust if there is negative range
							filterPriceRange.handleNegativeRange(marginPriceRange);

							filteredPriceRange = filterPriceRange.filterRange(inputPriceRange, marginPriceRange);

							// Set Price Range carried to next guideline
							guidelineAndConstraintOutput.outputPriceRange = filteredPriceRange;
							filterPriceRange.updateRoundingLogic(inputItem, inputPriceRange, marginPriceRange,
									filteredPriceRange, "");

							inputItem.setIsMarginGuidelineApplied(true);
							inputItem.setMarginGuidelineRange(marginPriceRange);
							guidelineAndConstraintOutput.isGuidelineApplied = true;
							guidelineAndConstraintLog
									.setIsGuidelineOrConstraintApplied(guidelineAndConstraintOutput.isGuidelineApplied);
							guidelineAndConstraintLog.setGuidelineOrConstraintPriceRange1(marginPriceRange);
							guidelineAndConstraintLog
									.setOutputPriceRange(guidelineAndConstraintOutput.outputPriceRange);

						}
						  else if (inputItem.getPreListCost() != null && inputItem.getPreListCost() > 0) {

							// Set Margin Min and Max Range
							marginPriceRange = applyMarginGuideline(choosenGuidelineMargin, inputItem.getListCost(),
									inputItem.getPreListCost(), inputItem.getRegPrice(),
									inputItem.getCostChgIndicator(), strategyDTO.getConstriants()
											.getThresholdConstraint());
							// Adjust if there is negative range
							filterPriceRange.handleNegativeRange(marginPriceRange);

							filteredPriceRange = filterPriceRange.filterRange(inputPriceRange, marginPriceRange);

							// Ahold - Cost Change and Store Brand Relation
							if (inputItem.getCostChangeBehavior().equals(PRConstants.COST_CHANGE_AHOLD_STORE_BRAND)) {
								// If Store Brand Relation is already applied
								// (i.e. brand has higher precedence)
								if (inputItem.getIsStoreBrandRelationApplied()) {
									// If margin not in conflict
									if (!filteredPriceRange.isConflict()) {
										// Set max range as brand relation range
										filteredPriceRange.setEndVal(inputPriceRange.getEndVal());
										// guidelineAndConstraintOutput.outputPriceRange
										// = filteredPriceRange;
									}
									guidelineAndConstraintOutput.outputPriceRange = filteredPriceRange;
								} else {
									// Margin has higher precedence
									guidelineAndConstraintOutput.outputPriceRange = marginPriceRange;
								}
							}
							// Ahold - Cost Change and No Store Brand Relation
							else if (inputItem.getCostChangeBehavior().equals(PRConstants.COST_CHANGE_AHOLD_MARGIN)) {
								// Pass Margin Price Range as Output Price Range
								guidelineAndConstraintOutput.outputPriceRange = marginPriceRange;
							} else {
								// Set Price Range carried to next guideline
								guidelineAndConstraintOutput.outputPriceRange = filteredPriceRange;
								filterPriceRange.updateRoundingLogic(inputItem, inputPriceRange, marginPriceRange,
										filteredPriceRange, "");
							}

							inputItem.setIsMarginGuidelineApplied(true);
							inputItem.setMarginGuidelineRange(marginPriceRange);
							guidelineAndConstraintOutput.isGuidelineApplied = true;
							guidelineAndConstraintLog
									.setIsGuidelineOrConstraintApplied(guidelineAndConstraintOutput.isGuidelineApplied);
							guidelineAndConstraintLog.setGuidelineOrConstraintPriceRange1(marginPriceRange);
							guidelineAndConstraintLog
									.setOutputPriceRange(guidelineAndConstraintOutput.outputPriceRange);
						}
						
						
						else {
							guidelineAndConstraintOutput.outputPriceRange = inputPriceRange;
							guidelineAndConstraintOutput.isGuidelineApplied = false;
							// guidelineAndConstraintLog.setIsConflict(false);
							guidelineAndConstraintLog
									.setIsGuidelineOrConstraintApplied(guidelineAndConstraintOutput.isGuidelineApplied);
							guidelineAndConstraintLog.setMessage("Previous Cost not available");
							guidelineAndConstraintLog
									.setOutputPriceRange(guidelineAndConstraintOutput.outputPriceRange);
						}
					} else {
						guidelineAndConstraintOutput.outputPriceRange = inputPriceRange;
						guidelineAndConstraintOutput.isGuidelineApplied = false;
						guidelineAndConstraintLog
								.setIsGuidelineOrConstraintApplied(guidelineAndConstraintOutput.isGuidelineApplied);
						guidelineAndConstraintLog.setMessage("Current Price not available");
						guidelineAndConstraintLog.setOutputPriceRange(guidelineAndConstraintOutput.outputPriceRange);
					}
				} else {
					// Handling other than maintain current margin
					//Use the future cost if present and there is a cost change
					if (inputItem.getFutureListCost() != null && inputItem.getFutureListCost() > 0 && inputItem.getCostChgIndicator()!=0)
						marginPriceRange = applyMarginGuideline(choosenGuidelineMargin, inputItem.getFutureListCost(),
								0, inputItem.getRegPrice(), inputItem.getCostChgIndicator(),
								strategyDTO.getConstriants().getThresholdConstraint());
					else

						marginPriceRange = applyMarginGuideline(choosenGuidelineMargin, inputItem.getListCost(), 0,
								inputItem.getRegPrice(), inputItem.getCostChgIndicator(),
								strategyDTO.getConstriants().getThresholdConstraint());
					
					guidelineAndConstraintOutput.outputPriceRange = filterPriceRange.filterRange(inputPriceRange,
							marginPriceRange);
					guidelineAndConstraintOutput.isGuidelineApplied = true;
					guidelineAndConstraintLog
							.setIsGuidelineOrConstraintApplied(guidelineAndConstraintOutput.isGuidelineApplied);
					guidelineAndConstraintLog.setGuidelineOrConstraintPriceRange1(marginPriceRange);
					guidelineAndConstraintLog.setOutputPriceRange(guidelineAndConstraintOutput.outputPriceRange);
				}
			} else {
				//Send empty range
				guidelineAndConstraintOutput.outputPriceRange = inputPriceRange;
				guidelineAndConstraintOutput.isGuidelineApplied = true;
				guidelineAndConstraintLog
						.setIsGuidelineOrConstraintApplied(guidelineAndConstraintOutput.isGuidelineApplied);
				guidelineAndConstraintLog.setOutputPriceRange(guidelineAndConstraintOutput.outputPriceRange);
				logger.debug("Item Level Guideline is not Found - " + inputItem.getItemCode());
			}
		} else {
			guidelineAndConstraintOutput.outputPriceRange = inputPriceRange;
			guidelineAndConstraintOutput.isGuidelineApplied = false;
			guidelineAndConstraintLog
					.setIsGuidelineOrConstraintApplied(guidelineAndConstraintOutput.isGuidelineApplied);
			guidelineAndConstraintLog.setMessage("Cost not available");
			guidelineAndConstraintLog.setOutputPriceRange(guidelineAndConstraintOutput.outputPriceRange);
			logger.debug("No Cost data found for item code - " + inputItem.getItemCode());
		}

		//pass just input range as output range, used where just margin guideline shown in explain, not applied actually(in loced price)
		if(passInputRangeAsOutputRange){
			guidelineAndConstraintOutput.outputPriceRange = inputPriceRange;
			guidelineAndConstraintLog.setOutputPriceRange(inputPriceRange);
		}
		
		
		explainLog.getGuidelineAndConstraintLogs().add(guidelineAndConstraintLog);
		
//		logger.debug("Margin Guideline -- " + 
//				"Is Applied: " + guidelineAndConstraintLog.getIsGuidelineOrConstraintApplied() +
//				",Input Range: " + inputPriceRange.toString() + 
//				",Margin Range: " + (marginPriceRange != null ?  marginPriceRange.toString() : "- -") + 
//				",Output Range: " + guidelineAndConstraintOutput.outputPriceRange.toString());		
		 
		return guidelineAndConstraintOutput;
	}
	
	private PRGuidelineMargin findMarginGuideline(List<PRGuidelineMargin> marginGuidelines, PRItemDTO itemInfo) {
		PRGuidelineMargin choosenGuidelineMargin = null;
		// Get all item level guidelines
		List<PRGuidelineMargin> itemLevelMarginGuidelines = getItemLevelMarginGuidelines(marginGuidelines);

		//If there is no item level guidelines, then use all available guideline (will be category level guideline)
		if (itemLevelMarginGuidelines.size() == 0)
			itemLevelMarginGuidelines = marginGuidelines;

		if (itemLevelMarginGuidelines.size() > 0) {
//			// If there is only one margin guideline, return that
//			if(itemLevelMarginGuidelines.size() == 1){
//				choosenGuidelineMargin = itemLevelMarginGuidelines.get(0);
//			}
			//check if item is non moving and if margin guideline is defined for non moving item 
			if (itemInfo.isNonMovingItem()) {
				choosenGuidelineMargin = getMarginGuidelineForNonMovingItems(itemLevelMarginGuidelines,
						PRConstants.MAR_NON_MOVING_FLAG);
			}
			// If the cost is increased or decreased
			if (choosenGuidelineMargin == null &&itemInfo.getCostChgIndicator() == PRConstants.COST_INCREASE) {
				// If cost is increased
				// Check if any margin guideline available with I in COST_FLAG
				choosenGuidelineMargin = getMarginGuideline(itemLevelMarginGuidelines, PRConstants.MAR_COST_INCREASED_FLAG);
				
				// Check if any margin guideline available with C in COST_FLAG
				if(choosenGuidelineMargin == null)
					choosenGuidelineMargin = getMarginGuideline(itemLevelMarginGuidelines, PRConstants.MAR_COST_CHANGED_FLAG);

			}  if (choosenGuidelineMargin == null && itemInfo.getCostChgIndicator() == PRConstants.COST_DECREASE) {
				// If cost is decreased
				// Check if any margin guideline available with D in COST_FLAG
				choosenGuidelineMargin = getMarginGuideline(itemLevelMarginGuidelines, PRConstants.MAR_COST_DECREASED_FLAG);
				
				// Check if any margin guideline available with C in COST_FLAG
				if(choosenGuidelineMargin == null)
					choosenGuidelineMargin = getMarginGuideline(itemLevelMarginGuidelines, PRConstants.MAR_COST_CHANGED_FLAG);
			}  if (choosenGuidelineMargin == null && itemInfo.getCostChgIndicator() == PRConstants.COST_NO_CHANGE) {
				// Check if any margin guideline available with U in COST_FLAG
				choosenGuidelineMargin = getMarginGuideline(itemLevelMarginGuidelines, PRConstants.MAR_COST_UNCHANGED_FLAG);
			}
			// Look if any margin guideline available with A in COST_FLAG
			if(choosenGuidelineMargin == null)
				choosenGuidelineMargin = getMarginGuideline(itemLevelMarginGuidelines, PRConstants.MAR_COST_ALL_FLAG);
		}
		return choosenGuidelineMargin;
	}
	
	private List<PRGuidelineMargin> getItemLevelMarginGuidelines(List<PRGuidelineMargin> marginGuidelines) {
		List<PRGuidelineMargin> itemLevelMarginGuidelines = new ArrayList<PRGuidelineMargin>();
		for (PRGuidelineMargin guidelineMargin : marginGuidelines) {
			if(guidelineMargin.getItemLevelFlag() == Constants.YES)
				itemLevelMarginGuidelines.add(guidelineMargin);
		}
		return itemLevelMarginGuidelines;
	}

	private PRGuidelineMargin getMarginGuideline(List<PRGuidelineMargin> marginGuidelines, char flag) {
		PRGuidelineMargin choosenGuidelineMargin = null;
		for (PRGuidelineMargin guidelineMargin : marginGuidelines) {
			// added condition to check only for itemflag=A as it will be N for strategy to
			// be applied to non moving items
			if (guidelineMargin.getCostFlag() == flag
					&& guidelineMargin.getItemFlag() == PRConstants.MAR_COST_ALL_FLAG) {
				choosenGuidelineMargin = guidelineMargin;
				break;
			}
		}
		return choosenGuidelineMargin;
	}
	private PRRange applyMarginGuideline(PRGuidelineMargin marginGuideline, double listCost, double preCost, Double regPrice,
			int costChgIndicator, PRConstraintThreshold prConstraintThreshold) {
		PRRange range = getPriceRange(marginGuideline, listCost, preCost, regPrice, costChgIndicator, prConstraintThreshold, costChangeLogic);
		//logger.debug("Margin Guideline Cost - " + listCost + " Start Value - " + range.getStartVal() + " End Value - " + range.getEndVal());
		return range;
	}
	
	/**
	 * Methods that returns Price Range for Margin Guideline
	 * @param prConstraintThreshold 
	 * @return
	 */
	public PRRange getPriceRange(PRGuidelineMargin marginGuideline, double listCost, double preCost, Double regPrice,
			int costChgIndicator, PRConstraintThreshold prConstraintThreshold, String costChangeLogic) {
		PRRange range = new PRRange();

		if (PRConstants.VALUE_TYPE_PCT == marginGuideline.getCurrentMargin()
				|| PRConstants.VALUE_TYPE_$ == marginGuideline.getCurrentMargin()
				|| PRConstants.VALUE_TYPE_I == marginGuideline.getCurrentMargin()) {
			/*if (costChangeLogic.toUpperCase().equals(PRConstants.AHOLD)
					&& costChgIndicator == PRConstants.COST_NO_CHANGE) {
				// Don't find margin price range if the cost remains same for
				// AHOLD
			} else {
				range = getPriceRangeWithCurrentMargin(marginGuideline, listCost, preCost, regPrice, costChgIndicator,
						prConstraintThreshold, costChangeLogic);
			}*/
			
			range = getPriceRangeWithCurrentMargin(marginGuideline, listCost, preCost, regPrice, costChgIndicator,
					prConstraintThreshold, costChangeLogic);
		} else if (marginGuideline.getValueType() == PRConstants.VALUE_TYPE_PCT) {
			range = getPriceRangeWithMarginPct(marginGuideline, listCost);
		} else if (marginGuideline.getValueType() == PRConstants.VALUE_TYPE_$) {
			range = getPriceRangeWithMargin$(marginGuideline, listCost);
		}

		return range;
	}
	
	/**
	 * Method that returns Price Range for Margin Pct Guideline
	 * @return
	 */
	private PRRange getPriceRangeWithMarginPct(PRGuidelineMargin marginGuideline, double listCost) {
		PRRange range = new PRRange();
		if (marginGuideline.getMinMarginPct() != Constants.DEFAULT_NA) {
			double startVal = listCost / (1 - (marginGuideline.getMinMarginPct() / 100));
			range.setStartVal(startVal);
		}
		if (marginGuideline.getMaxMarginPct() != Constants.DEFAULT_NA) {
			double endVal = listCost / (1 - (marginGuideline.getMaxMarginPct() / 100));
			range.setEndVal(endVal);
		}
		return range;
	}
	
	/**
	 * Method that returns Price Range for Margin $ Guideline
	 * @return
	 */
	private PRRange getPriceRangeWithMargin$(PRGuidelineMargin marginGuideline, double listCost) {
		PRRange range = new PRRange();
		if (marginGuideline.getMinMargin$() != Constants.DEFAULT_NA) {
			double startVal = marginGuideline.getMinMargin$() + listCost;
			range.setStartVal(startVal);
		}
		if (marginGuideline.getMaxMargin$() != Constants.DEFAULT_NA) {
			double endVal = marginGuideline.getMaxMargin$() + listCost;
			range.setEndVal(endVal);
		}
		return range;
	}
	
	public PRRange getPriceRangeWithCurrentMargin(PRGuidelineMargin marginGuideline, double listCost, double preCost,
			double regPrice, int costChdIndicator, PRConstraintThreshold prConstraintThreshold, String costChangeLogic) {
		PRRange range = new PRRange();
		//1. Find Min Margin $/% based on current Margin $/%
		double defaultMaxMargin = Double.parseDouble(PropertyManager.getProperty("PR_DEFAULT_MAINTAIN_CURRENT_MARGIN_MAX", "30"));
		if(marginGuideline.getCurrentMargin() == PRConstants.VALUE_TYPE_PCT){
			double marginPct = ((regPrice - preCost)/regPrice) * 100;
			marginGuideline.setMinMarginPct(marginPct);
			//For Ahold, the min and max price would be the same
			if (costChangeLogic.toUpperCase().equals(PRConstants.AHOLD)) {
				marginGuideline.setMaxMarginPct(marginGuideline.getMinMarginPct());
			}
			range = getPriceRangeWithMarginPct(marginGuideline, listCost);
		}else if(marginGuideline.getCurrentMargin() == PRConstants.VALUE_TYPE_$){
			marginGuideline.setMinMargin$(regPrice - preCost);
			//For Ahold, the min and max price would be the same
			if (costChangeLogic.toUpperCase().equals(PRConstants.AHOLD)) {
				marginGuideline.setMaxMargin$(marginGuideline.getMinMargin$());
			}
			range = getPriceRangeWithMargin$(marginGuideline, listCost);
		}else if(marginGuideline.getCurrentMargin() == PRConstants.VALUE_TYPE_I){
			//Cost Remained Same
			/*if(costChdIndicator == 0){
				double marginPct = ((regPrice - preCost)/regPrice) * 100;
				marginGuideline.setMaxMarginPct(marginPct);
				range = getPriceRangeWithMarginPct(marginGuideline, listCost);
			//Cost Increased	
			}else*/
			if(costChdIndicator > 0){
				double marginPct = ((regPrice - preCost)/regPrice) * 100;
				marginGuideline.setMinMarginPct(marginPct);
				//For Ahold, the min and max price would be same when the cost is increased
				if (costChangeLogic.toUpperCase().equals(PRConstants.AHOLD)) {
					marginGuideline.setMaxMarginPct(marginGuideline.getMinMarginPct());
				}
				range = getPriceRangeWithMarginPct(marginGuideline, listCost);
			}
			//Cost Decreased
			else if (costChdIndicator < 0){
				marginGuideline.setMinMargin$(regPrice - preCost);
				//For Ahold, the min and max price would be same when the cost is decreased
				if (costChangeLogic.toUpperCase().equals(PRConstants.AHOLD)) {
					marginGuideline.setMaxMargin$(marginGuideline.getMinMargin$());
				}
				range = getPriceRangeWithMargin$(marginGuideline, listCost);
			}
		}
		
		//This is not needed for Ahold
		if (!costChangeLogic.toUpperCase().equals(PRConstants.AHOLD)) {
			//Set a Max Margin Limit, If the margin range is out of threshold,
			//then limit the max margin price to default max margin
			PRRange thresholdRange = prConstraintThreshold.getPriceRange(regPrice);		
			
			if(thresholdRange.getEndVal() <= range.getStartVal()){
				double endVal = range.getStartVal() + (range.getStartVal() * defaultMaxMargin / 100);
				range.setEndVal(endVal);
			}else
				range.setEndVal(thresholdRange.getEndVal());
		}
		
		return range;
	}
	
	private PRGuidelineMargin getMarginGuidelineForNonMovingItems(List<PRGuidelineMargin> marginGuidelines, char flag) {
		PRGuidelineMargin choosenGuidelineMargin = null;
		for (PRGuidelineMargin guidelineMargin : marginGuidelines) {
			if (guidelineMargin.getItemFlag() == flag) {
				choosenGuidelineMargin = guidelineMargin;
				break;
			}
		}
		return choosenGuidelineMargin;
	}
}
