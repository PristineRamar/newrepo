package com.pristine.service.offermgmt.recommendationrule;

import java.util.HashMap;
import java.util.List;

import org.apache.log4j.Logger;

import com.pristine.dto.RetailPriceDTO;
import com.pristine.dto.offermgmt.MultiplePrice;
import com.pristine.dto.offermgmt.PRItemDTO;
import com.pristine.dto.offermgmt.RecommendationRuleMapDTO;
import com.pristine.exception.GeneralException;
import com.pristine.service.offermgmt.GuidelineTypeLookup;
import com.pristine.util.PropertyManager;
import com.pristine.util.offermgmt.PRCommonUtil;

public class CheckInSufficientMarginBenefitRule extends RetainCurrentRetailRule {
	private static Logger logger = Logger.getLogger("CheckInSufficientMarginBenefitRule");
	private PRItemDTO itemDTO;
	HashMap<String, List<RecommendationRuleMapDTO>> recommendationRuleMap;
	int noOfStores;
	private final String ruleCode ="R10";
	
	public CheckInSufficientMarginBenefitRule(PRItemDTO itemDTO, HashMap<String, List<RecommendationRuleMapDTO>> recommendationRuleMap,
			int noOfStores) {
		super(itemDTO, recommendationRuleMap);
		this.itemDTO =itemDTO;
		this.recommendationRuleMap = recommendationRuleMap;
		this.noOfStores = noOfStores;
	}

	
	protected boolean isInSufficientMarginBenefit() {
		boolean isInSufficientMarginBenefit = false;
		int minMarginGainCents = Integer.parseInt(PropertyManager.getProperty("MIN_MARGIN_GAIN_CENTS"));
		Double minMarginDollarBenefit = (minMarginGainCents * noOfStores) / 100.0;
		
		MultiplePrice curRegPrice = PRCommonUtil.getMultiplePrice(itemDTO.getRegMPack(), itemDTO.getRegPrice(), itemDTO.getRegMPrice());
		Double curMargin = itemDTO.getCurRetailMarginDollar();
		Double recMargin = itemDTO.getRecRetailMarginDollar();
		
		logger.debug("minMarginGainCents:" + minMarginGainCents + ",noOfStore:" + noOfStores + ",minMarginDollarBenefit:" + minMarginDollarBenefit
				+ ",recMargin:" + recMargin + ",curMargin:" + curMargin);

		//there is new price and margin is not sufficient
		if (itemDTO.getRecommendedRegPrice() != null && !itemDTO.getRecommendedRegPrice().equals(curRegPrice)
				&& (recMargin - curMargin) < minMarginDollarBenefit) {
			isInSufficientMarginBenefit = true;
		}
		return isInSufficientMarginBenefit;
	}

	@Override
	public boolean isCurrentRetailRetained() throws Exception, GeneralException {
		boolean retainCurrentRetail = false;
		
		if(isRuleEnabled(ruleCode) && isInSufficientMarginBenefit() && !isCostConstraintViolated() && checkPriceRangeUsingRelatedItem()) {
			retainCurrentRetail = true;
		}
		
		return retainCurrentRetail;
	}

	@Override
	public boolean isRuleEnabled() {
		return isRuleEnabled(ruleCode);
	}
}
