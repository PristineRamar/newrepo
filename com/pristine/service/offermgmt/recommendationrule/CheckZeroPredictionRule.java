package com.pristine.service.offermgmt.recommendationrule;

import java.util.HashMap;
import java.util.List;

import org.apache.log4j.Logger;

import com.pristine.dto.RetailPriceDTO;
import com.pristine.dto.offermgmt.PRItemDTO;
import com.pristine.dto.offermgmt.RecommendationRuleMapDTO;
import com.pristine.exception.GeneralException;
import com.pristine.service.offermgmt.GuidelineTypeLookup;

public class CheckZeroPredictionRule extends RetainCurrentRetailRule {
	
	private static Logger logger = Logger.getLogger("CheckZeroPredictionRule");
	private PRItemDTO itemDTO;
	HashMap<String, List<RecommendationRuleMapDTO>> recommendationRuleMap;
	HashMap<String, RetailPriceDTO> itemPriceHistory;
	String inpBaseWeekStartDate, priceType;
	int maxRegRetailInLastXWeeks =0;
	private final String ruleCode ="R11";
	
	public CheckZeroPredictionRule(PRItemDTO itemDTO, HashMap<String, List<RecommendationRuleMapDTO>> recommendationRuleMap) {
		super(itemDTO, recommendationRuleMap);
		this.itemDTO =itemDTO;
		this.recommendationRuleMap = recommendationRuleMap;
	}
	
	@Override
	public boolean isCurrentRetailRetained() throws Exception, GeneralException {
		boolean retainCurrentRetail = false;
		
		if(isRuleEnabled(ruleCode) && isZeroPrediction() && !isCostConstraintViolated() && checkPriceRangeUsingRelatedItem()) {
			retainCurrentRetail = true;
		}
		
		return retainCurrentRetail;
	}
	
	/***
	 * Retain current retail for non-LIG or LIG items without 0 prediction 
	 * except non-LIG or any of the LIG-member which is part of price group
	 * 
	 * @param ligOrNonLigItem
	 * @return
	 */
	private boolean isZeroPrediction() {
		boolean isZeroPrediction = false;

		logger.debug("ligRepOrNonLigItem.getPredictedMovement():" + itemDTO.getPredictedMovement()
				+ ",ligRepOrNonLigItem.getCurRegPricePredictedMovement():" + itemDTO.getCurRegPricePredictedMovement());
		if (itemDTO.getPredictedMovement() == null || itemDTO.getPredictedMovement() <= 0
				|| itemDTO.getCurRegPricePredictedMovement() == null  || itemDTO.getCurRegPricePredictedMovement() <= 0) {
			isZeroPrediction = true;
			logger.debug("isZeroPrediction:" + isZeroPrediction);
		}

		return isZeroPrediction;
	}

	@Override
	public boolean isRuleEnabled() {
		return isRuleEnabled(ruleCode);
	}
}
