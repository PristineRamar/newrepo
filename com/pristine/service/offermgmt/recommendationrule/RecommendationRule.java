package com.pristine.service.offermgmt.recommendationrule;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;

//import org.apache.log4j.Logger;

import com.pristine.dto.offermgmt.PRItemDTO;
import com.pristine.dto.offermgmt.RecommendationRuleMapDTO;

public abstract class RecommendationRule {
	//private static Logger logger = Logger.getLogger("RecommendationRule");
	List<Double> actualPricePoints = new ArrayList<>();
	private PRItemDTO itemDTO;
	HashMap<String, List<RecommendationRuleMapDTO>> recommendationRuleMap;
	
	public RecommendationRule(PRItemDTO itemDTO, HashMap<String, List<RecommendationRuleMapDTO>> recommendationRuleMap) {
		this.itemDTO = itemDTO;
		this.recommendationRuleMap = recommendationRuleMap;
	}
	protected boolean isRuleEnabled(String ruleCode) {
		boolean isRuleEnabled = false;
		boolean isObjSpecificRuleDefined = false;
		int objectiveTypeId = itemDTO.getObjectiveTypeId();
		//Rule code available
		if(recommendationRuleMap.get(ruleCode) != null) {
			List<RecommendationRuleMapDTO> recommendationRules = recommendationRuleMap.get(ruleCode);
			
			//Check if there is objective specific rule
			for(RecommendationRuleMapDTO recommendationRule : recommendationRules) {
				if(recommendationRule.getObjectiveTypeId() == objectiveTypeId) {
					if(recommendationRule.isEnabled()) {
						isRuleEnabled = true;	
					}
					isObjSpecificRuleDefined = true;
					break;
				}
			}
			
			//If not check if defined at higher level
			if (!isObjSpecificRuleDefined) {
				for (RecommendationRuleMapDTO recommendationRule : recommendationRules) {
					if (recommendationRule.getObjectiveTypeId() == 0 && recommendationRule.isEnabled()) {
						isRuleEnabled = true;
						break;
					}
				}
			}
		}
		return isRuleEnabled;
	}
	
	protected boolean isCostConstraintViolated() {
		
		RecommendationRuleHelper recHelper = new RecommendationRuleHelper();
		
		return recHelper.isCostConstraintViolated(itemDTO);
		
	}
}
