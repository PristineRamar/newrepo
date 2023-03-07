package com.pristine.service.offermgmt.recommendationrule;

import java.util.HashMap;
import java.util.List;
import com.pristine.dto.offermgmt.PRItemDTO;
import com.pristine.dto.offermgmt.RecommendationRuleMapDTO;
import com.pristine.exception.GeneralException;

public class PastOverridenRetailRule extends RetainCurrentRetailRule  {

	private PRItemDTO itemDTO;
	HashMap<String, List<RecommendationRuleMapDTO>> recommendationRuleMap;
	private final String ruleCode ="R17";
	
	public PastOverridenRetailRule(PRItemDTO itemDTO, HashMap<String, List<RecommendationRuleMapDTO>> recommendationRuleMap) {
		super(itemDTO, recommendationRuleMap);
		this.itemDTO = itemDTO;
		this.recommendationRuleMap = recommendationRuleMap;
	}

	@Override
	public boolean isCurrentRetailRetained() throws Exception, GeneralException {
		return false;
	}
	
	@Override
	public boolean isRuleEnabled() {
		return isRuleEnabled(ruleCode);
	}
	
	/**
	 * 
	 * @return override retail
	 * @throws Exception
	 * @throws GeneralException
	 */
	public boolean isOverriddeRetailRetained() throws Exception, GeneralException {
		boolean retainCurrentRetail = false;
		boolean isCompPriceChanged = itemDTO.getCompPriceChgIndicator() == 0 ? false : true;
		boolean isCostChanged = itemDTO.getCostChgIndicator() == 0 ? false : true;
		if (isRuleEnabled(ruleCode) && !isCostConstraintViolated() && !isCompPriceChanged && !isCostChanged
				&& !itemDTO.isFutureRetailPresent()) {
			if (itemDTO.getOverriddenRegularPrice() != null) {
				retainCurrentRetail = true;
			}
		}
		return retainCurrentRetail;
	}
}
