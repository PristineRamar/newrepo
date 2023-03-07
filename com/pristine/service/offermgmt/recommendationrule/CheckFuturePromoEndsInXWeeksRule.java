package com.pristine.service.offermgmt.recommendationrule;

import java.util.HashMap;
import java.util.List;

import com.pristine.dto.RetailPriceDTO;
import com.pristine.dto.offermgmt.PRItemDTO;
import com.pristine.dto.offermgmt.RecommendationRuleMapDTO;
import com.pristine.exception.GeneralException;

public class CheckFuturePromoEndsInXWeeksRule extends RetainCurrentRetailRule  {

	private PRItemDTO itemDTO;
	HashMap<String, List<RecommendationRuleMapDTO>> recommendationRuleMap;
	HashMap<String, RetailPriceDTO> itemPriceHistory;
	String inpBaseWeekStartDate, priceType;
	int maxRegRetailInLastXWeeks =0;
	private final String ruleCode ="R8";
	
	public CheckFuturePromoEndsInXWeeksRule(PRItemDTO itemDTO, HashMap<String, List<RecommendationRuleMapDTO>> recommendationRuleMap) {
		super(itemDTO, recommendationRuleMap);
		this.itemDTO =itemDTO;
		this.recommendationRuleMap = recommendationRuleMap;
	}

	@Override
	public boolean isCurrentRetailRetained() throws Exception, GeneralException {
		boolean retainCurrentRetail = false;
		
		if(isRuleEnabled(ruleCode) && !isCostConstraintViolated() && checkPriceRangeUsingRelatedItem()) {
			retainCurrentRetail = true;
		}
		
		return retainCurrentRetail;
	}
	
	@Override
	public boolean isRuleEnabled() {
		return isRuleEnabled(ruleCode);
	}

}
