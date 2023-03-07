package com.pristine.service.offermgmt.recommendationrule;

import java.util.HashMap;
import java.util.List;

import org.apache.log4j.Logger;

import com.pristine.dto.RetailPriceDTO;
import com.pristine.dto.offermgmt.PRItemDTO;
import com.pristine.dto.offermgmt.RecommendationRuleMapDTO;
import com.pristine.exception.GeneralException;
import com.pristine.service.offermgmt.GuidelineTypeLookup;

public class CheckRetailCurMultiplePriceRule extends RetainCurrentRetailRule  {
	private static Logger logger = Logger.getLogger("CheckRetailCurMultiplePriceRule");
	private PRItemDTO itemDTO;
	HashMap<String, List<RecommendationRuleMapDTO>> recommendationRuleMap;
	private final String ruleCode ="R12";
	
	public CheckRetailCurMultiplePriceRule(PRItemDTO itemDTO, HashMap<String, List<RecommendationRuleMapDTO>> recommendationRuleMap) {
		super(itemDTO, recommendationRuleMap);
		this.itemDTO =itemDTO;
		this.recommendationRuleMap = recommendationRuleMap;
	}

	@Override
	public boolean isCurrentRetailRetained() throws Exception, GeneralException {
		boolean retainCurRetail = false;
		if (isRuleEnabled(ruleCode) && !isCostConstraintViolated()) {
			retainCurRetail = true;
		}
		return retainCurRetail;
	}

	@Override
	public boolean isRuleEnabled() {
		return isRuleEnabled(ruleCode);
	}
}
