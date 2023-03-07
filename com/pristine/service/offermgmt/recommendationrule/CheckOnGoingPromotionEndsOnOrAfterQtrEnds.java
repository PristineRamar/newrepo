package com.pristine.service.offermgmt.recommendationrule;

import java.util.HashMap;
import java.util.List;

import com.pristine.dto.offermgmt.PRItemDTO;
import com.pristine.dto.offermgmt.RecommendationRuleMapDTO;
import com.pristine.exception.GeneralException;

public class CheckOnGoingPromotionEndsOnOrAfterQtrEnds extends RetainCurrentRetailRule {

	@SuppressWarnings("unused")
	private PRItemDTO itemDTO;
	HashMap<String, List<RecommendationRuleMapDTO>> recommendationRuleMap;
	private final String ruleCode = "R21";

	public CheckOnGoingPromotionEndsOnOrAfterQtrEnds(PRItemDTO itemDTO,
			HashMap<String, List<RecommendationRuleMapDTO>> recommendationRuleMap) {
		super(itemDTO, recommendationRuleMap);
		this.itemDTO = itemDTO;
		this.recommendationRuleMap = recommendationRuleMap;
	}

	@Override
	public boolean isCurrentRetailRetained() throws Exception, GeneralException {
		boolean retainCurrentRetail = false;

		if (isRuleEnabled(ruleCode)) {
			retainCurrentRetail = true;
		}

		return retainCurrentRetail;
	}

	@Override
	public boolean isRuleEnabled() throws Exception, GeneralException {
		return isRuleEnabled(ruleCode);
	}

}
