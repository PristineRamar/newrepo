package com.pristine.service.offermgmt.guideline;

import java.util.HashMap;
import java.util.List;

import org.apache.log4j.Logger;

import com.pristine.dto.offermgmt.LocationKey;
import com.pristine.dto.offermgmt.MultiplePrice;
import com.pristine.dto.offermgmt.PRExplainLog;
import com.pristine.dto.offermgmt.PRGuidelineAndConstraintLog;
import com.pristine.dto.offermgmt.PRGuidelineAndConstraintOutput;
import com.pristine.dto.offermgmt.PRGuidelineLeadZoneDTO;
import com.pristine.dto.offermgmt.PRGuidelineMargin;
import com.pristine.dto.offermgmt.PRItemDTO;
import com.pristine.dto.offermgmt.PRRange;
import com.pristine.dto.offermgmt.PRRecommendation;
import com.pristine.dto.offermgmt.PRStrategyDTO;
import com.pristine.service.offermgmt.FilterPriceRange;
import com.pristine.service.offermgmt.GuidelineTypeLookup;
import com.pristine.service.offermgmt.ItemKey;
import com.pristine.service.offermgmt.PricingEngineService;
import com.pristine.util.Constants;
import com.pristine.util.offermgmt.PRCommonUtil;
import com.pristine.util.offermgmt.PRConstants;

public class LeadZoneGuideline {
	private static Logger logger = Logger.getLogger("LeadZoneGuideline");
	private PRItemDTO inputItem;
	private PRRange inputPriceRange;
	FilterPriceRange filterPriceRange = new FilterPriceRange();
	private PRExplainLog explainLog;
	private HashMap<LocationKey, HashMap<ItemKey, PRRecommendation>> leadZoneDetails;
	PRGuidelineAndConstraintLog guidelineAndConstraintLog = new PRGuidelineAndConstraintLog();

	public LeadZoneGuideline(PRItemDTO inputItem, PRRange inputPriceRange, PRExplainLog explainLog,
			HashMap<LocationKey, HashMap<ItemKey, PRRecommendation>> leadZoneDetails) {
		this.inputItem = inputItem;
		this.inputPriceRange = inputPriceRange;
		this.explainLog = explainLog;
		this.leadZoneDetails = leadZoneDetails;
	}

	public PRGuidelineAndConstraintOutput applyLeadZoneGuideline() {
		PRStrategyDTO strategyDTO = inputItem.getStrategyDTO();
		PRRange filteredPriceRange;
		PRRange lzPriceRange = new PRRange();
		PricingEngineService pricingEngineService = new PricingEngineService();
		PRGuidelineAndConstraintOutput guidelineAndConstraintOutput = new PRGuidelineAndConstraintOutput();
		PRGuidelineLeadZoneDTO leadZoneGuideline = strategyDTO.getGuidelines().getLeadZoneGuideline();

		//There is Lead Zone Guideline
		if (leadZoneGuideline != null) {
			guidelineAndConstraintLog.setGuidelineTypeId(GuidelineTypeLookup.LEAD_ZONE.getGuidelineTypeId());
			
			// Get lead zone item detail
			PRRecommendation leadZoneItem = pricingEngineService.getLeadZoneItem(inputItem, leadZoneDetails);

			//The item is present in lead zone
			if (leadZoneItem != null && leadZoneItem.getRecRegPriceObj() != null) {
				double leadZoneUnitPrice = PRCommonUtil.getUnitPrice(leadZoneItem.getRecRegPriceObj(), true);
				lzPriceRange = applyLeadZoneGuideline(leadZoneGuideline, leadZoneUnitPrice);
				// Adjust if there is negative range
				filterPriceRange.handleNegativeRange(lzPriceRange);
				filteredPriceRange = filterPriceRange.filterRange(inputPriceRange, lzPriceRange);
				filterPriceRange.updateRoundingLogic(inputItem, inputPriceRange, lzPriceRange, filteredPriceRange, "");

				guidelineAndConstraintOutput.outputPriceRange = filteredPriceRange;
				guidelineAndConstraintOutput.isGuidelineApplied = true;
				guidelineAndConstraintLog.setRelatedPrice(leadZoneItem.getRecRegPriceObj());
				guidelineAndConstraintLog.setIsGuidelineOrConstraintApplied(guidelineAndConstraintOutput.isGuidelineApplied);
				guidelineAndConstraintLog.setGuidelineOrConstraintPriceRange1(lzPriceRange);
				guidelineAndConstraintLog.setOutputPriceRange(guidelineAndConstraintOutput.outputPriceRange);
			} else {
				guidelineAndConstraintOutput.outputPriceRange = inputPriceRange;
				guidelineAndConstraintOutput.isGuidelineApplied = false;
				guidelineAndConstraintLog.setIsGuidelineOrConstraintApplied(guidelineAndConstraintOutput.isGuidelineApplied);
				guidelineAndConstraintLog.setMessage("No Lead Zone Price");
				guidelineAndConstraintLog.setOutputPriceRange(guidelineAndConstraintOutput.outputPriceRange);
			}
			explainLog.getGuidelineAndConstraintLogs().add(guidelineAndConstraintLog);
		} else {
			guidelineAndConstraintOutput.outputPriceRange = inputPriceRange;
		}

		return guidelineAndConstraintOutput;
	}
	
	
	
	private PRRange applyLeadZoneGuideline(PRGuidelineLeadZoneDTO leadZoneGuideline, double leadZoneUnitPrice) {
		PRRange range = new PRRange();
		String operatorText = leadZoneGuideline.getOperatorText();
		double minValue = leadZoneGuideline.getMinValue();
		double maxValue = leadZoneGuideline.getMaxValue();
		
		if(PRConstants.PRICE_GROUP_EXPR_EQUAL_SYM.equals(operatorText)){
			range.setStartVal(leadZoneUnitPrice);
			range.setEndVal(leadZoneUnitPrice);
		} else if(PRConstants.PRICE_GROUP_EXPR_BELOW.equals(operatorText)){
			if(minValue != Constants.DEFAULT_NA && maxValue != Constants.DEFAULT_NA){
				range.setStartVal(leadZoneUnitPrice - (leadZoneUnitPrice * (maxValue/100)));
				range.setEndVal(leadZoneUnitPrice - (leadZoneUnitPrice * (minValue/100)));
			}else if(minValue != Constants.DEFAULT_NA){
				range.setStartVal(leadZoneUnitPrice - (leadZoneUnitPrice * (minValue/100)));
				range.setEndVal(leadZoneUnitPrice - (leadZoneUnitPrice * (minValue/100)));
			}else if(maxValue != Constants.DEFAULT_NA){
				range.setStartVal(leadZoneUnitPrice);
				range.setEndVal(leadZoneUnitPrice - (leadZoneUnitPrice * (maxValue/100)));
			}
		} else if(PRConstants.PRICE_GROUP_EXPR_ABOVE.equals(operatorText)){
			if(minValue != Constants.DEFAULT_NA && maxValue != Constants.DEFAULT_NA){
				range.setStartVal(leadZoneUnitPrice + (leadZoneUnitPrice * (minValue/100)));
				range.setEndVal(leadZoneUnitPrice + (leadZoneUnitPrice * (maxValue/100)));
			}else if(minValue != Constants.DEFAULT_NA){
				range.setStartVal(leadZoneUnitPrice + (leadZoneUnitPrice * (minValue/100)));
				range.setEndVal(leadZoneUnitPrice + (leadZoneUnitPrice * (minValue/100)));
			}else if(maxValue != Constants.DEFAULT_NA){
				range.setStartVal(leadZoneUnitPrice);
				range.setEndVal(leadZoneUnitPrice + (leadZoneUnitPrice * (maxValue/100)));
			}
		}
		return range;
	}
}
