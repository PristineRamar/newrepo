package com.pristine.service.offermgmt.recommendationrule;

import java.util.ArrayList;
import java.util.List;

import org.apache.log4j.Logger;

import com.pristine.dto.offermgmt.MultiplePrice;
import com.pristine.dto.offermgmt.PRGuidelineAndConstraintLog;
import com.pristine.dto.offermgmt.PRItemDTO;
import com.pristine.dto.offermgmt.prediction.PricePointDTO;
import com.pristine.service.offermgmt.GuidelineTypeLookup;
import com.pristine.service.offermgmt.PricingEngineService;
import com.pristine.util.offermgmt.PRCommonUtil;

public class RecommendationRuleHelper {
	private static Logger logger = Logger.getLogger("RecommendationRuleHelper");

	public boolean isCostConstraintViolated(PRItemDTO itemDTO) {
		boolean isCostConstraintViolated = false;
		MultiplePrice curRegPrice = PRCommonUtil.getMultiplePrice(itemDTO.getRegMPack(), itemDTO.getRegPrice(), itemDTO.getRegMPrice());
		Double curRecUnitPrice = PRCommonUtil.getUnitPrice(curRegPrice, true);
		if (itemDTO.getListCost() != null && itemDTO.getListCost() > curRecUnitPrice) {
			isCostConstraintViolated = true;
		}
//		logger.debug("Item Code: " + itemDTO.getItemCode() + " Cur Rec unit price: " + curRecUnitPrice + " List Cost: " + itemDTO.getListCost()
//				+ " cost violated:" + isCostConstraintViolated);
		return isCostConstraintViolated;
	}
	
	public List<String> getAddtDetails(PricePointDTO curPricePredMov, boolean hasPredForOneOfThePricePoint){
		List<String> additionalDetails = new ArrayList<String>();
		if(curPricePredMov == null || (curPricePredMov != null && curPricePredMov.getPredictedMovement() <=0)) {
			additionalDetails.add("Predicted movement not available");
		}
		if(!hasPredForOneOfThePricePoint) {
			additionalDetails.add("None of the price points has valid prediction");
		}
		return additionalDetails;
		
	}
	
	
	public boolean checkPriceRangeUsingRelatedItem(PRItemDTO itemDTO, int inpRegMultiple, double inpRegPrice) {
		boolean isPriceWithInRange = true;
		int regMultiple = 0;
		double regPrice=0.0;
		
		if(inpRegMultiple > 0 && inpRegPrice>0) {
			regMultiple  = inpRegMultiple;
			regPrice = inpRegPrice;
		}else {
			regMultiple  = itemDTO.getRecommendedRegPrice().multiple;
			regPrice = itemDTO.getRecommendedRegPrice().price;
		}
		
		PricingEngineService pricingEngineService = new PricingEngineService();
		for (PRGuidelineAndConstraintLog guidelineAndConstraintLog : itemDTO.getExplainLog().getGuidelineAndConstraintLogs()) {
			if (guidelineAndConstraintLog.getGuidelineTypeId() == GuidelineTypeLookup.BRAND.getGuidelineTypeId()
					|| guidelineAndConstraintLog.getGuidelineTypeId() == GuidelineTypeLookup.SIZE.getGuidelineTypeId()) {		
				if (!pricingEngineService.checkIfPriceWithinRange(regMultiple, regPrice,
						guidelineAndConstraintLog.getOutputPriceRange(), guidelineAndConstraintLog)) {
					isPriceWithInRange = false;
				}
			}
		}
		return isPriceWithInRange;
	}
}
