package com.pristine.service.offermgmt.recommendationrule;

import java.util.HashMap;
import java.util.List;

//import org.apache.log4j.Logger;

import com.pristine.dto.RetailPriceDTO;
import com.pristine.dto.offermgmt.PRItemDTO;
import com.pristine.dto.offermgmt.RecommendationRuleMapDTO;
import com.pristine.exception.GeneralException;

public class CheckOnGoingPromoEndsInXWeeksRule extends RetainCurrentRetailRule  {
	//private static Logger logger = Logger.getLogger("CheckOnGoingPromoEndsInXWeeksRule");
	@SuppressWarnings("unused")
	private PRItemDTO itemDTO;
	HashMap<String, List<RecommendationRuleMapDTO>> recommendationRuleMap;
	HashMap<String, RetailPriceDTO> itemPriceHistory;
	String inpBaseWeekStartDate, priceType;
	int maxRegRetailInLastXWeeks =0;
	private final String ruleCode ="R9";
	
	public CheckOnGoingPromoEndsInXWeeksRule(PRItemDTO itemDTO, HashMap<String, List<RecommendationRuleMapDTO>> recommendationRuleMap) {
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
		/*logger.debug("Is Rule Enabled: "+isRuleEnabled(ruleCode)+" is Cost violated: "+isCostConstraintViolated()+" Brand/Size in Range: "+
				checkPriceRangeUsingRelatedItem()+" Retain Current Retail: "+retainCurrentRetail);*/
		return retainCurrentRetail;
	}

	@Override
	public boolean isRuleEnabled() {
		return isRuleEnabled(ruleCode);
	}
}
