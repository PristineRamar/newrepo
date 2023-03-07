package com.pristine.service.offermgmt.recommendationrule;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;

import com.pristine.dto.RetailPriceDTO;
import com.pristine.dto.offermgmt.PRItemDTO;
import com.pristine.dto.offermgmt.RecommendationRuleMapDTO;
import com.pristine.exception.GeneralException;

/**
 * @author Dinesh
 *
 */
public class SameRetailInLastXWeeksRule extends FilterPricePointsRule {

	private PRItemDTO itemDTO;
	private List<Double> actualPricePoints;
	HashMap<String, List<RecommendationRuleMapDTO>> recommendationRuleMap;
	HashMap<String, RetailPriceDTO> itemPriceHistory;
	String inpBaseWeekStartDate, priceType;
	int noOfSalePriceWeeks = 0;
	double curUnitRegPrice;
	private final String ruleCode ="R4";
	boolean isPriceUnchanged=false;
	FilterPricePointsRule recommendationRuleR4B, recommendationRuleR4C;
	
	public SameRetailInLastXWeeksRule(PRItemDTO itemDTO, HashMap<String, List<RecommendationRuleMapDTO>> recommendationRuleMap, 
			FilterPricePointsRule recommendationRuleR4B, FilterPricePointsRule recommendationRuleR4C) {
		super(itemDTO, recommendationRuleMap);
		this.itemDTO =itemDTO;
		this.recommendationRuleMap = recommendationRuleMap;
	}
	
	public SameRetailInLastXWeeksRule(HashMap<String, List<RecommendationRuleMapDTO>> recommendationRuleMap, PRItemDTO itemDTO,
			List<Double> actualPricePoints, HashMap<String, RetailPriceDTO> itemPriceHistory, String inpBaseWeekStartDate,
			int noSaleInLastXWeeks, String priceType, Double curUnitRegPrice, boolean isPriceUnchanged) {
		super(itemDTO, actualPricePoints, recommendationRuleMap);
		this.itemDTO = itemDTO;
		this.actualPricePoints = actualPricePoints;
		this.itemPriceHistory = itemPriceHistory;
		this.inpBaseWeekStartDate = inpBaseWeekStartDate;
		this.priceType = priceType;
		this.noOfSalePriceWeeks = noSaleInLastXWeeks;
		this.recommendationRuleMap = recommendationRuleMap;
		this.curUnitRegPrice = curUnitRegPrice;
		this.isPriceUnchanged = isPriceUnchanged;
	}
	

	/* (non-Javadoc)
	 * @see com.pristine.service.offermgmt.recommendationrule.RecommendationRule#applyRule()
	 */
	@Override
	public List<Double> applyRule() throws Exception, GeneralException {

		List<Double> filteredPricePoints = new ArrayList<>();
		if (isPriceUnchanged && isRuleEnabled(ruleCode)) {
			// The retail will not be reduced below the highest sale/TPR price observed in the last 52 weeks
			// unless there is size/brand violation. Price points which are below the highest sale/TPR will be ignored
			FilterPricePointsRule recommendationRuleR4B = new CheckPPLessThanHighestSalePriceInLastXWeeksRule(recommendationRuleMap, itemDTO,
					actualPricePoints, itemPriceHistory, inpBaseWeekStartDate, noOfSalePriceWeeks, priceType, curUnitRegPrice);
			filteredPricePoints = recommendationRuleR4B.applyRule();
			
			// If an item was never on sale during the last 52 weeks, the retail not be reduced by >5%
			// unless there is a size/brand violation. Price points which are less than 5% will be ignored.
			// If all of the price points are less than 5%, then the price point which is closest to 5% will be chosen
			FilterPricePointsRule recommendationRuleR4C = new checkIsItemOnSaleInLastXWeeksRule(recommendationRuleMap, itemDTO, filteredPricePoints,
					itemPriceHistory, inpBaseWeekStartDate, noOfSalePriceWeeks, curUnitRegPrice);
			filteredPricePoints = recommendationRuleR4C.applyRule();
		} else {
			filteredPricePoints.addAll(actualPricePoints);
		}
		return filteredPricePoints;
	}

}
