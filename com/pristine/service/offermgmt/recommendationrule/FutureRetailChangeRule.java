package com.pristine.service.offermgmt.recommendationrule;

import java.util.HashMap;
import java.util.List;

import com.pristine.dto.RetailPriceDTO;
import com.pristine.dto.offermgmt.PRItemDTO;
import com.pristine.dto.offermgmt.RecommendationRuleMapDTO;
import com.pristine.exception.GeneralException;

public class FutureRetailChangeRule extends RetainCurrentRetailRule  {

	private PRItemDTO itemDTO;
	HashMap<String, List<RecommendationRuleMapDTO>> recommendationRuleMap;
	HashMap<String, RetailPriceDTO> itemPriceHistory;
	String inpBaseWeekStartDate, priceType;
	int maxRegRetailInLastXWeeks = 0;
	private final String ruleCode ="R18";
	
	public FutureRetailChangeRule(PRItemDTO itemDTO, HashMap<String, List<RecommendationRuleMapDTO>> recommendationRuleMap) {
		super(itemDTO, recommendationRuleMap);
		this.itemDTO =itemDTO;
		this.recommendationRuleMap = recommendationRuleMap;
	}

	@Override
	public boolean isCurrentRetailRetained() throws Exception, GeneralException {
		boolean retainCurrentRetail = false;
		boolean isCompPriceChanged = itemDTO.getCompPriceChgIndicator() == 0 ? false : true;
		boolean isCostChanged = itemDTO.getCostChgIndicator() == 0 ? false : true;
		if (isRuleEnabled(ruleCode) 
				&& itemDTO.getFutureRecRetail() != null 
				&& !isCostConstraintViolated()
				&& checkPriceRangeUsingRelatedItem() 
				&& !isCompPriceChanged 
				&& !isCostChanged
				&& !itemDTO.isFutureRetailRecommended()) {
			retainCurrentRetail = true;
		}
		return retainCurrentRetail;
	}
	
	@Override
	public boolean isRuleEnabled() {
		return isRuleEnabled(ruleCode);
	}

}
