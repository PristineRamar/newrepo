package com.pristine.service.offermgmt.recommendationrule;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;

//import org.apache.log4j.Logger;

import com.pristine.dto.offermgmt.MultiplePrice;
import com.pristine.dto.offermgmt.PRGuidelineAndConstraintLog;
import com.pristine.dto.offermgmt.PRItemDTO;
import com.pristine.dto.offermgmt.RecommendationRuleMapDTO;
import com.pristine.exception.GeneralException;
import com.pristine.service.offermgmt.GuidelineTypeLookup;
import com.pristine.service.offermgmt.PricingEngineService;
import com.pristine.util.offermgmt.PRCommonUtil;

public abstract class RetainCurrentRetailRule extends RecommendationRule{
	//private static Logger logger = Logger.getLogger("RetainCurrentRetailRule");
	
	List<Double> actualPricePoints = new ArrayList<>();
	private PRItemDTO itemDTO;
	HashMap<String, List<RecommendationRuleMapDTO>> recommendationRuleMap;
	
	abstract public boolean isCurrentRetailRetained() throws Exception, GeneralException;
	abstract public boolean isRuleEnabled() throws Exception, GeneralException;
	
	public RetainCurrentRetailRule(PRItemDTO itemDTO, HashMap<String, List<RecommendationRuleMapDTO>> recommendationRuleMap) {
		super(itemDTO, recommendationRuleMap);
		this.itemDTO = itemDTO;
		this.recommendationRuleMap = recommendationRuleMap;
	}
	
	protected boolean checkPriceRangeUsingRelatedItem() {
		boolean isPriceWithInRange = true;
		PricingEngineService pricingEngineService = new PricingEngineService();
		for (PRGuidelineAndConstraintLog guidelineAndConstraintLog : itemDTO.getExplainLog().getGuidelineAndConstraintLogs()) {
			if (guidelineAndConstraintLog.getGuidelineTypeId() == GuidelineTypeLookup.BRAND.getGuidelineTypeId()
					|| guidelineAndConstraintLog.getGuidelineTypeId() == GuidelineTypeLookup.SIZE.getGuidelineTypeId()) {
				if (itemDTO.getRegPrice() != null) {
					MultiplePrice curRegPrice = PRCommonUtil.getMultiplePrice(itemDTO.getRegMPack(), itemDTO.getRegPrice(),
							itemDTO.getRegMPrice());
					if (!pricingEngineService.checkIfPriceWithinRange(curRegPrice.multiple, curRegPrice.price,
							guidelineAndConstraintLog.getOutputPriceRange(), guidelineAndConstraintLog)) {
						isPriceWithInRange = false;
					}
				}
			}
		}
		return isPriceWithInRange;
	}
}
