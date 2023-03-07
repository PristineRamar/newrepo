package com.pristine.service.offermgmt.recommendationrule;

import java.util.ArrayList;
import java.util.Comparator;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Optional;
import java.util.Set;
import java.util.stream.Collectors;

import org.apache.log4j.Logger;

import com.pristine.dto.RetailPriceDTO;
import com.pristine.dto.offermgmt.MultiplePrice;
import com.pristine.dto.offermgmt.PRItemDTO;
import com.pristine.dto.offermgmt.RecommendationRuleMapDTO;
import com.pristine.exception.GeneralException;
import com.pristine.lookup.offermgmt.ExplainRetailNoteTypeLookup;
import com.pristine.service.offermgmt.GuidelineTypeLookup;
import com.pristine.service.offermgmt.PricingEngineService;
import com.pristine.util.PropertyManager;
import com.pristine.util.offermgmt.PRCommonUtil;
import com.pristine.util.offermgmt.PRFormatHelper;

/**
 * A retail is not increased greater than 5% than the highest Reg price point in the last 52 weeks, 
 * unless there is a cost increase or brand/size violation.   
 * Price points which are more than 5% will be ignored.  
 * If all the price points are more than 5%, then the price point which is closer to 5% will be chosen.
 * @author Dinesh
 *
 */
public class CheckPPWithinXPctThanHighestPrevPriceRule extends FilterPricePointsRule{

	private static Logger logger = Logger.getLogger("CheckPPWithinXPctThanHighestPrevPriceRule"); 
	private PRItemDTO itemDTO;
	private List<Double>actualPricePoints;
	HashMap<String, List<RecommendationRuleMapDTO>> recommendationRuleMap;
	HashMap<String, RetailPriceDTO> itemPriceHistory;
	String inpBaseWeekStartDate, priceType;
	int maxRegRetailInLastXWeeks =0;
	private final String ruleCode ="R3";
	
	public CheckPPWithinXPctThanHighestPrevPriceRule(PRItemDTO itemDTO, HashMap<String, List<RecommendationRuleMapDTO>> recommendationRuleMap) {
		super(itemDTO, recommendationRuleMap);
		this.itemDTO =itemDTO;
		this.recommendationRuleMap = recommendationRuleMap;
	}

	public CheckPPWithinXPctThanHighestPrevPriceRule(HashMap<String, List<RecommendationRuleMapDTO>> recommendationRuleMap,
			PRItemDTO itemDTO, List<Double> actualPricePoints, HashMap<String, RetailPriceDTO> itemPriceHistory,
			String inpBaseWeekStartDate, int noOfWeeks, String priceType) {
		super(itemDTO, actualPricePoints, recommendationRuleMap);
		this.itemDTO = itemDTO;
		this.actualPricePoints = actualPricePoints;
		this.itemPriceHistory = itemPriceHistory;
		this.inpBaseWeekStartDate = inpBaseWeekStartDate;
		this.priceType = priceType;
		this.maxRegRetailInLastXWeeks = noOfWeeks;
		this.recommendationRuleMap = recommendationRuleMap;
	}
	
	/* (non-Javadoc)
	 * @see com.pristine.service.offermgmt.recommendationrule.RecommendationRule#applyRule()
	 */
	@Override
	/**
	 * 1. Check Cost is changed and Brand/Size guideline is violated
	 * 2. Unless there is a cost increase or brand/size violation, new price points greater than 5% 
	 * 		than the highest Reg price point in the last 52 weeks will not be considered.
	 */
	public List<Double> applyRule() throws Exception, GeneralException {
		PricingEngineService pricingEngineService = new PricingEngineService();
		int maxRegRetailIncreasePct = Integer.parseInt(PropertyManager.getProperty("MAX_REG_RETAIL_INCREASE_PCT"));
		MultiplePrice maxRegRetail = getMaxPriceInLastXWeeks(itemPriceHistory, inpBaseWeekStartDate, maxRegRetailInLastXWeeks, priceType);

		Double maxRegUnitPrice = PRCommonUtil.getUnitPrice(maxRegRetail, true);
		Double maxIncreaseRetail = Double
				.valueOf(PRFormatHelper.roundToTwoDecimalDigit((maxRegUnitPrice + (maxRegUnitPrice * (maxRegRetailIncreasePct / 100.0)))));
		Set<Double> finalFilteredPriceRange = new HashSet<Double>();
		if (isRuleEnabled(ruleCode) && maxRegRetail !=null) {
			
			//Filter given price points according to the rule (Rule: A retail is not increased greater than 5% than the highest 
			// Reg price point in the last 52 weeks)
			
			Set<Double> filterPricePoints = getFilteredPricePoints(maxIncreaseRetail, true);
			
			// Check cost changed. If cost changed, then apply Margin guidelines and constraints for all the price points
			// Based on price points obtained by Applying margin guidelines, check price points less than 5%, if not choose all
			// the price points
			getPricePointsBasedOnCostChgInd(finalFilteredPriceRange, filterPricePoints);

			// Check Brand and Size guideline violated for the given price points and choose price points which is lesser than 5%
			// than highest cost
			// if none of price points available, then consider all the price points
			getPricePointsBasedOnBrandAndSize(finalFilteredPriceRange, filterPricePoints);

			logger.debug("max price point: " + maxRegRetail.toString() + " in last " + maxRegRetailInLastXWeeks + " weeks. Do not raise more than "
					+ maxIncreaseRetail + " (" + maxRegRetailIncreasePct + "%) from max price point." + " First filter price points:"
					+ PRCommonUtil.getCommaSeperatedStringFromDouble(actualPricePoints) + ",Final filtered price points: "
					+ finalFilteredPriceRange.stream().map(s -> s.toString()).collect(Collectors.joining(",")));

			if (actualPricePoints.size() > 0 && finalFilteredPriceRange.size() == 0) {
				logger.debug("6. Addition log");
				List<String> additionalDetails = new ArrayList<String>();
				additionalDetails.add(String.valueOf(maxRegRetailIncreasePct) + "%");
				additionalDetails.add(String.valueOf(maxRegRetailInLastXWeeks));
				pricingEngineService.writeAdditionalExplainLog(itemDTO, ExplainRetailNoteTypeLookup.CUR_REG_RETAINED_EXCEED_HIGHEST_RETAIL,
						additionalDetails);
			} else {

				String pricePoints = getIgnoredPricePoints(finalFilteredPriceRange);
				if (pricePoints != "") {
					logger.debug("6-1. Addition log");
					List<String> additionalDetails = new ArrayList<String>();
					additionalDetails.add(pricePoints);
					additionalDetails.add(String.valueOf(maxRegRetailIncreasePct) + "%");
					additionalDetails.add(String.valueOf(maxRegRetailInLastXWeeks));

					pricingEngineService.writeAdditionalExplainLog(itemDTO, ExplainRetailNoteTypeLookup.PP_IGNORED_EXCEED_HIGHEST_RETAIL,
							additionalDetails);
				}
			}
		} else {
			finalFilteredPriceRange.addAll(actualPricePoints);
		}

		return finalFilteredPriceRange.stream().collect(Collectors.toList());
	}
	
	private void getPricePointsBasedOnCostChgInd(Set<Double> finalFilteredPriceRange, Set<Double> filteredPricePoints) {
		// If Margin guideline applied, then check cost is changed
		
		// If Cost increased, then check Guideline is applied
		if (itemDTO.getCostChgIndicator() > 0 || isCostConstraintViolated() ) {
			if(isGuidelineApplied(itemDTO, GuidelineTypeLookup.MARGIN.getGuidelineTypeId())) {
				
				// Filtered Price points from actuals
				if(filteredPricePoints.size()>0 && filteredPricePoints.size() != actualPricePoints.size()) {
					finalFilteredPriceRange.addAll(filteredPricePoints);
				}
				
				// All price points not in range
				else if(filteredPricePoints.size() == 0){
					Optional<Double> closestPP = actualPricePoints.stream().min(Comparator.comparing(Double::valueOf));
					if (closestPP.isPresent()) {
						finalFilteredPriceRange.add(closestPP.get());
					}
				}
				// Both Filter and Actual has same price points
				else {
					finalFilteredPriceRange.addAll(actualPricePoints);
				}
//				// If cost got changed, then check the price points available less then 5%, else get all the price points
//				costPPs = actualPricePoints.stream().filter(pp -> pp <= maxIncreaseRetail).collect(Collectors.toList());
//				if (costPPs.size() == 0) {
//					Optional<Double> closestPP = actualPricePoints.stream().min(Comparator.comparing(Double::valueOf));
//					if (closestPP.isPresent()) {
//						costPPs.add(closestPP.get());
//					}
//				}
			}else {
				finalFilteredPriceRange.addAll(actualPricePoints);
			}
		}
		// If cost is same or decreased, then Price points which are more than 5% will be ignored
		else {
			finalFilteredPriceRange.addAll(filteredPricePoints);
		}
		
	}
	
	private void getPricePointsBasedOnBrandAndSize(Set<Double> finalFilteredPriceRange, Set<Double> filteredPricePoints) {

		// Check Brand/Size guideline violated
		List<Double> ppBasedOnBrandAndSizeList = getPPBasedOnBrandAndSizeRelation(filteredPricePoints, true);
		
		if(ppBasedOnBrandAndSizeList!=null && ppBasedOnBrandAndSizeList.size()>0) {
			
			finalFilteredPriceRange.addAll(ppBasedOnBrandAndSizeList);
//			Set<Double> filteredPriceRange = ppBasedOnBrandAndSizeList.stream().filter(price -> price <= maxIncreaseRetail)
//					.collect(Collectors.toSet());
//			if (filteredPriceRange == null || filteredPriceRange.size() == 0) {
//				Optional<Double> closestPP = ppBasedOnBrandAndSizeList.stream().min(Comparator.comparing(Double::valueOf));
//				if (closestPP.isPresent()) {
//					finalFilteredPriceRange.add(closestPP.get());
//				}
//			}else {
//				finalFilteredPriceRange.addAll(filteredPriceRange);
//			}
		}else {
			finalFilteredPriceRange.addAll(filteredPricePoints);
		}
	}
}
