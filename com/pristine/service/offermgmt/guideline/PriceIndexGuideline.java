package com.pristine.service.offermgmt.guideline;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;

import org.apache.log4j.Logger;

import com.pristine.dto.offermgmt.LocationKey;
import com.pristine.dto.offermgmt.MultiplePrice;
import com.pristine.dto.offermgmt.PRExplainLog;
import com.pristine.dto.offermgmt.PRGuidelineAndConstraintLog;
import com.pristine.dto.offermgmt.PRGuidelineAndConstraintOutput;
//import com.pristine.dto.offermgmt.PRGuidelineMargin;
import com.pristine.dto.offermgmt.PRGuidelinePI;
import com.pristine.dto.offermgmt.PRItemDTO;
import com.pristine.dto.offermgmt.PRRange;
import com.pristine.dto.offermgmt.PRStrategyDTO;
import com.pristine.service.offermgmt.FilterPriceRange;
import com.pristine.service.offermgmt.GuidelineTypeLookup;
import com.pristine.service.offermgmt.PriceIndexCalculation;
//import com.pristine.service.offermgmt.PricingEngineService;
import com.pristine.util.Constants;
import com.pristine.util.PropertyManager;
import com.pristine.util.offermgmt.PRCommonUtil;
import com.pristine.util.offermgmt.PRConstants;

public class PriceIndexGuideline {

	private static Logger logger = Logger.getLogger("PriceIndexGuideline");
	PRItemDTO inputItem;
	PRRange inputPriceRange;	 
	FilterPriceRange filterPriceRange = new FilterPriceRange();
	HashMap<Integer, LocationKey> compIdMap;
	private PRExplainLog explainLog;
	
	PRGuidelineAndConstraintLog guidelineAndConstraintLog = new PRGuidelineAndConstraintLog();
	
	double minBadIndex = Double.parseDouble(PropertyManager.getProperty("MIN_BAD_INDEX", "0"));
	double maxBadIndex = Double.parseDouble(PropertyManager.getProperty("MAX_BAD_INDEX", "0"));
	
	//double  Double.parsed.v PropertyManager.getProperty("MIN_BAD_INDEX");
	//PropertyManager.getProperty("MAX_BAD_INDEX");
	
	public PriceIndexGuideline(PRItemDTO inputItem, PRRange inputPriceRange, HashMap<Integer, LocationKey> compIdMap, PRExplainLog explainLog) {
		this.inputItem = inputItem;
		this.inputPriceRange = inputPriceRange;
		this.compIdMap = compIdMap;
		this.explainLog = explainLog;
	}
	
	public PRGuidelineAndConstraintOutput applyPriceIndexGuideline() {
		PRGuidelineAndConstraintOutput guidelineAndConstraintOutput = new PRGuidelineAndConstraintOutput();
		PRStrategyDTO strategyDTO = inputItem.getStrategyDTO();
		PRRange filteredPriceRange;
		PRRange piPriceRange = new PRRange();
		List<PRGuidelinePI> piGuidelines = null;
		PRGuidelinePI piGuidelineToUse = null;
		PriceIndexCalculation pic = new PriceIndexCalculation();
		// PricingEngineService pricingEngineService = new PricingEngineService();

		// Get item level guideline
		piGuidelines = getItemLevelPIGuidelines(strategyDTO.getGuidelines().getPiGuideline());
		if (piGuidelines.size() > 0)
			piGuidelineToUse = piGuidelines.get(0);

		// If there is no item level guideline, then pick cat level guideline
		if (piGuidelines.size() == 0 && strategyDTO.getGuidelines().getPiGuideline().size() > 0) {
			piGuidelineToUse = strategyDTO.getGuidelines().getPiGuideline().get(0);
		}

		// There is no Price index guideline
		if (piGuidelineToUse != null) {
			guidelineAndConstraintLog.setGuidelineTypeId(GuidelineTypeLookup.PRICE_INDEX.getGuidelineTypeId());
			// Do not apply PI guideline when there is no primary competitor
			// comp price. Append log message accordingly
			LocationKey compStrId = null;
			// Competitor can also be defined from strategy screen,
			// Give preference to competitor defined at strategy screen
			if (piGuidelineToUse.getCompStrId() > 0) {
				// compStrId = piGuidelineToUse.getCompStrId();
				compStrId = new LocationKey(Constants.STORE_LEVEL_ID, piGuidelineToUse.getCompStrId());
			} else if (compIdMap != null && compIdMap.get(PRConstants.COMP_TYPE_1) != null) {
				compStrId = compIdMap.get(PRConstants.COMP_TYPE_1);
			}

			// if (compIdMap != null && compIdMap.get(PRConstants.COMP_TYPE_PRIMARY) != null) {
			if (compStrId != null) {
				// Integer compStrId = compIdMap.get(PRConstants.COMP_TYPE_PRIMARY);
				if (inputItem.getAllCompPrice().get(compStrId) != null) {

					MultiplePrice curRegPrice = PRCommonUtil.getCurRegPrice(inputItem);
					
					//8th March 2017, to handle bad comp check
					if (!pic.isBadCompCheck(curRegPrice, inputItem.getAllCompPrice().get(compStrId), minBadIndex, maxBadIndex)) {

						// Find unit price
						double compUnitPrice = PRCommonUtil.getUnitPrice(inputItem.getAllCompPrice().get(compStrId), true);
						guidelineAndConstraintLog.setIndexCompPrice(inputItem.getAllCompPrice().get(compStrId));
						piPriceRange = applyPIGuideline(piGuidelineToUse, compUnitPrice);
						// Adjust if there is negative range
						filterPriceRange.handleNegativeRange(piPriceRange);

						filteredPriceRange = filterPriceRange.filterRange(inputPriceRange, piPriceRange);

						// Ahold, Cost Change - ignore the price index
						if (inputItem.getCostChangeBehavior().equals(PRConstants.COST_CHANGE_AHOLD_STORE_BRAND)
								|| inputItem.getCostChangeBehavior().equals(PRConstants.COST_CHANGE_AHOLD_MARGIN)) {
							// If this is the first guideline
							if (inputPriceRange.getStartVal() == Constants.DEFAULT_NA && inputPriceRange.getEndVal() == Constants.DEFAULT_NA) {
								guidelineAndConstraintOutput.outputPriceRange = filteredPriceRange;
							} else {
								if (inputItem.getIsMarginGuidelineApplied() || inputItem.getIsStoreBrandRelationApplied()) {
									guidelineAndConstraintOutput.outputPriceRange = inputPriceRange;
								} else {
									guidelineAndConstraintOutput.outputPriceRange = filteredPriceRange;
								}
							}
						} else {
							guidelineAndConstraintOutput.outputPriceRange = filteredPriceRange;
							filterPriceRange.updateRoundingLogic(inputItem, inputPriceRange, piPriceRange, filteredPriceRange, "");
						}

						guidelineAndConstraintOutput.isGuidelineApplied = true;
						guidelineAndConstraintLog.setIsGuidelineOrConstraintApplied(guidelineAndConstraintOutput.isGuidelineApplied);
						guidelineAndConstraintLog.setGuidelineOrConstraintPriceRange1(piPriceRange);
						// If Cost is not available pass input range as output range
						if ((inputItem.getListCost() == null || inputItem.getListCost() <= 0)
								&& (inputItem.getRegPrice() != null && inputItem.getRegPrice() > 0)) {
							guidelineAndConstraintOutput.outputPriceRange = inputPriceRange;
							guidelineAndConstraintLog.setOutputPriceRange(inputPriceRange);
						} else
							guidelineAndConstraintLog.setOutputPriceRange(guidelineAndConstraintOutput.outputPriceRange);
					} else {
						guidelineAndConstraintLog.setIndexCompPrice(inputItem.getAllCompPrice().get(compStrId));
						guidelineAndConstraintOutput.outputPriceRange = inputPriceRange;
						guidelineAndConstraintOutput.isGuidelineApplied = false;
						guidelineAndConstraintLog.setIsGuidelineOrConstraintApplied(guidelineAndConstraintOutput.isGuidelineApplied);
						guidelineAndConstraintLog.setMessage("Invalid Competition Price");
						guidelineAndConstraintLog.setOutputPriceRange(guidelineAndConstraintOutput.outputPriceRange);
					}
				} else {
					guidelineAndConstraintOutput.outputPriceRange = inputPriceRange;
					guidelineAndConstraintOutput.isGuidelineApplied = false;
					// guidelineAndConstraintLog.setIsConflict(false);
					guidelineAndConstraintLog.setIsGuidelineOrConstraintApplied(guidelineAndConstraintOutput.isGuidelineApplied);
					guidelineAndConstraintLog.setMessage("No Competition Price");
					guidelineAndConstraintLog.setOutputPriceRange(guidelineAndConstraintOutput.outputPriceRange);
					// logger.debug("No Comp Price data found for item code - "
					// + inputItem.getItemCode());
				}
			} else {
				guidelineAndConstraintOutput.outputPriceRange = inputPriceRange;
				guidelineAndConstraintOutput.isGuidelineApplied = false;
				// guidelineAndConstraintLog.setIsConflict(false);
				guidelineAndConstraintLog.setIsGuidelineOrConstraintApplied(guidelineAndConstraintOutput.isGuidelineApplied);
				guidelineAndConstraintLog.setMessage("No Competitor Found");
				guidelineAndConstraintLog.setOutputPriceRange(guidelineAndConstraintOutput.outputPriceRange);
				// logger.debug("No Competitor Found - " +
				// inputItem.getItemCode());
			}
			explainLog.getGuidelineAndConstraintLogs().add(guidelineAndConstraintLog);
		} else {
			guidelineAndConstraintOutput.outputPriceRange = inputPriceRange;
		}
		logger.debug("Price Index Guideline -- " + "Is Applied: " + guidelineAndConstraintLog.getIsGuidelineOrConstraintApplied() + ",Input Range: "
				+ inputPriceRange.toString() + ",Price Index Range: " + piPriceRange.toString() + ",Output Range: "
				+ guidelineAndConstraintOutput.outputPriceRange.toString());

		return guidelineAndConstraintOutput;
	}
	
	protected PRRange applyPIGuideline(PRGuidelinePI piGuideline, double price){
		PRRange range = piGuideline.getPriceRange(price);
		//logger.debug("PI Guideline Price - " + price + " Start Value - " + range.getStartVal() + " End Value - " + range.getEndVal());
		return range;
	}
	
	private List<PRGuidelinePI> getItemLevelPIGuidelines(List<PRGuidelinePI> piGuidelines) {
		List<PRGuidelinePI> itemLevelPIGuidelines = new ArrayList<PRGuidelinePI>();
		for (PRGuidelinePI guidelinePI : piGuidelines) {
			if(guidelinePI.getItemLevelFlag() == Constants.YES)
				itemLevelPIGuidelines.add(guidelinePI);
		}
		return itemLevelPIGuidelines;
	}
	
	
	
}
