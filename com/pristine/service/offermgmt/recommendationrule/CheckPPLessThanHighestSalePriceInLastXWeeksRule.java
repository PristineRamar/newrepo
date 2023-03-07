package com.pristine.service.offermgmt.recommendationrule;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.stream.Collectors;

import org.apache.log4j.Logger;

import com.pristine.dto.RetailPriceDTO;
import com.pristine.dto.offermgmt.MultiplePrice;
import com.pristine.dto.offermgmt.PRItemDTO;
import com.pristine.dto.offermgmt.RecommendationRuleMapDTO;
import com.pristine.exception.GeneralException;
import com.pristine.lookup.offermgmt.ExplainRetailNoteTypeLookup;
import com.pristine.service.offermgmt.PricingEngineService;
import com.pristine.util.PropertyManager;
import com.pristine.util.offermgmt.PRCommonUtil;

public class CheckPPLessThanHighestSalePriceInLastXWeeksRule extends FilterPricePointsRule {

	private static Logger logger = Logger.getLogger("CheckPPLessThanHighestSalePriceInLastXWeeksRule");
	private PRItemDTO itemDTO;
	private List<Double> actualPricePoints;
	HashMap<String, List<RecommendationRuleMapDTO>> recommendationRuleMap;
	HashMap<String, RetailPriceDTO> itemPriceHistory;
	String inpBaseWeekStartDate, priceType;
	int noOfSalePriceWeeks = 0;
	double curUnitRegPrice;
	private final String ruleCode = "R4B";

	public CheckPPLessThanHighestSalePriceInLastXWeeksRule(PRItemDTO itemDTO, HashMap<String, List<RecommendationRuleMapDTO>> recommendationRuleMap) {
		super(itemDTO, recommendationRuleMap);
		this.itemDTO = itemDTO;
		this.recommendationRuleMap = recommendationRuleMap;
	}

	public CheckPPLessThanHighestSalePriceInLastXWeeksRule(HashMap<String, List<RecommendationRuleMapDTO>> recommendationRuleMap, PRItemDTO itemDTO,
			List<Double> actualPricePoints, HashMap<String, RetailPriceDTO> itemPriceHistory, String inpBaseWeekStartDate, int noOfWeeks,
			String priceType, Double curUnitRegPrice) {
		super(itemDTO, actualPricePoints, recommendationRuleMap);
		this.itemDTO = itemDTO;
		this.actualPricePoints = actualPricePoints;
		this.itemPriceHistory = itemPriceHistory;
		this.inpBaseWeekStartDate = inpBaseWeekStartDate;
		this.priceType = priceType;
		this.noOfSalePriceWeeks = noOfWeeks;
		this.recommendationRuleMap = recommendationRuleMap;
		this.curUnitRegPrice = curUnitRegPrice;
	}

	/**
	 * The retail will not be reduced below the highest sale/TPR price observed in the last 52 weeks unless there is size/brand
	 * violation. Price points which are below the highest sale/TPR will be ignored.
	 */
	@Override
	public List<Double> applyRule() throws Exception, GeneralException {
		PricingEngineService pricingEngineService = new PricingEngineService();
		int regRetailUnchangedInLastXWeeks = Integer.parseInt(PropertyManager.getProperty("REG_RETAIL_UNCHANGED_IN_LAST_X_WEEKS"));
		Set<Double> finalFilteredPriceRange = new HashSet<Double>();
		MultiplePrice maxSalePrice = getMaxPriceInLastXWeeks(itemPriceHistory, inpBaseWeekStartDate, noOfSalePriceWeeks, priceType);
		
		if (isRuleEnabled(ruleCode) && maxSalePrice != null) {

			Double maxSaleUnitPrice = PRCommonUtil.getUnitPrice(maxSalePrice, true);
			
			Set<Double> filterPricePoints = getFilteredPricePoints(maxSaleUnitPrice, false);
			// Check Brand/Size guideline violated
			List<Double> pricePointsBasedOnBrandAndSize = getPPBasedOnBrandAndSizeRelation(filterPricePoints, false);
			
			if (pricePointsBasedOnBrandAndSize != null && pricePointsBasedOnBrandAndSize.size() > 0) {
				
				finalFilteredPriceRange.addAll(pricePointsBasedOnBrandAndSize);
//				// Remove price points below the highest sale unit price
//				Set<Double> filteredPriceRange = pricePointsBasedOnBrandAndSize.stream().filter(pp -> pp >= maxSaleUnitPrice)
//						.collect(Collectors.toSet());
//				if (filteredPriceRange == null || (filteredPriceRange != null && filteredPriceRange.size() == 0)) {
//					finalFilteredPriceRange.addAll(actualPricePoints);
//				}else {
//					finalFilteredPriceRange.addAll(filteredPriceRange);
//				}
			}else {
				finalFilteredPriceRange.addAll(filterPricePoints);
//				finalFilteredPriceRange.addAll(actualPricePoints.stream().filter(pp -> pp >= maxSaleUnitPrice).collect(Collectors.toSet()));
			}

			logger.debug("Don't reduce below highest sale price: " + maxSaleUnitPrice + " in last " + noOfSalePriceWeeks
					+ " weeks, unless cost decrease. " + "Cur Reg Unit Price:" + curUnitRegPrice + ",Actual price points: "
					+ PRCommonUtil.getCommaSeperatedStringFromDouble(actualPricePoints) + ",Filtered price points: "
					+ finalFilteredPriceRange.stream().map(s -> s.toString()).collect(Collectors.joining(",")));
			
			if (actualPricePoints.size() > 0 && finalFilteredPriceRange.size() == 0) {
				logger.debug("4. Addition log");
				List<String> additionalDetails = new ArrayList<String>();
				additionalDetails.add(String.valueOf(regRetailUnchangedInLastXWeeks));
				additionalDetails.add(String.valueOf(noOfSalePriceWeeks));
				pricingEngineService.writeAdditionalExplainLog(itemDTO, ExplainRetailNoteTypeLookup.CUR_REG_RETAINED_RETAIL_UNCHANGED_CANNOT_DECREASE,
						additionalDetails);
			} else {
				String pricePoints = getIgnoredPricePoints(finalFilteredPriceRange);
				if (pricePoints != "") {
					logger.debug("4-1. Addition log");
					List<String> additionalDetails = new ArrayList<String>();
					additionalDetails.add(pricePoints);
					additionalDetails.add(String.valueOf(noOfSalePriceWeeks));
					additionalDetails.add(String.valueOf(regRetailUnchangedInLastXWeeks));

					pricingEngineService.writeAdditionalExplainLog(itemDTO, ExplainRetailNoteTypeLookup.PP_IGNORED_CANNOT_DECREASE,
							additionalDetails);
				}
			}
		} else {
			finalFilteredPriceRange.addAll(actualPricePoints);
		}
		
		return finalFilteredPriceRange.stream().collect(Collectors.toList());
	}
}
