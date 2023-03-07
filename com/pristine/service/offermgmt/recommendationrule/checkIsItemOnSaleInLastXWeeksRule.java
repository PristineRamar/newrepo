package com.pristine.service.offermgmt.recommendationrule;

import java.time.LocalDate;
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
import com.pristine.dto.offermgmt.PRItemDTO;
import com.pristine.dto.offermgmt.RecommendationRuleMapDTO;
import com.pristine.exception.GeneralException;
import com.pristine.lookup.offermgmt.ExplainRetailNoteTypeLookup;
import com.pristine.service.offermgmt.PricingEngineService;
import com.pristine.util.Constants;
import com.pristine.util.DateUtil;
import com.pristine.util.PropertyManager;
import com.pristine.util.offermgmt.PRCommonUtil;
import com.pristine.util.offermgmt.PRFormatHelper;

public class checkIsItemOnSaleInLastXWeeksRule extends FilterPricePointsRule {

	private static Logger logger = Logger.getLogger("checkIsItemOnSaleInLastXWeeksRule");
	private PRItemDTO itemDTO;
	private List<Double> actualPricePoints;
	HashMap<String, List<RecommendationRuleMapDTO>> recommendationRuleMap;
	HashMap<String, RetailPriceDTO> itemPriceHistory;
	String inpBaseWeekStartDate;
	int noSaleInLastXWeeks = 0;
	private final String ruleCode = "R4C";
	private Double curUnitRegPrice;

	public checkIsItemOnSaleInLastXWeeksRule(PRItemDTO itemDTO, HashMap<String, List<RecommendationRuleMapDTO>> recommendationRuleMap) {
		super(itemDTO, recommendationRuleMap);
		this.itemDTO = itemDTO;
		this.recommendationRuleMap = recommendationRuleMap;
	}

	public checkIsItemOnSaleInLastXWeeksRule(HashMap<String, List<RecommendationRuleMapDTO>> recommendationRuleMap, PRItemDTO itemDTO,
			List<Double> actualPricePoints, HashMap<String, RetailPriceDTO> itemPriceHistory, String inpBaseWeekStartDate, int noOfWeeks,
			Double curUnitRegPrice) {
		super(itemDTO, actualPricePoints, recommendationRuleMap);
		this.itemDTO = itemDTO;
		this.actualPricePoints = actualPricePoints;
		this.itemPriceHistory = itemPriceHistory;
		this.inpBaseWeekStartDate = inpBaseWeekStartDate;
		this.noSaleInLastXWeeks = noOfWeeks;
		this.recommendationRuleMap = recommendationRuleMap;
		this.curUnitRegPrice = curUnitRegPrice;
	}

	/**
	 * If an item was never on sale during the last 52 weeks, the retail not be reduced by >5% unless there is a size/brand violation.  
	 * Price points which are less than 5% will be ignored.  If all of the price points are less than 5%, 
	 * then the price point which is closest to 5% will be chosen
	 */
	@Override
	public List<Double> applyRule() throws Exception, GeneralException {
		PricingEngineService pricingEngineService = new PricingEngineService();
		boolean isItemOnSaleBefore = isItemInSaleXWeeks(itemPriceHistory, inpBaseWeekStartDate, noSaleInLastXWeeks);
		int noSaleItemDecreasePct = Integer.parseInt(PropertyManager.getProperty("NO_SALE_ITEM_DECREASE_PCT"));
		int regRetailUnchangedInLastXWeeks = Integer.parseInt(PropertyManager.getProperty("REG_RETAIL_UNCHANGED_IN_LAST_X_WEEKS"));
		
		Set<Double> finalFilteredPriceRange = new HashSet<Double>();
		if (isRuleEnabled(ruleCode) && !isItemOnSaleBefore) {

			// Don't go below 5% of cur price
			Double minPrice = Double
					.valueOf(PRFormatHelper.roundToTwoDecimalDigit((curUnitRegPrice - (curUnitRegPrice * (noSaleItemDecreasePct / 100.0)))));
			
			Set<Double> filterPricePoints = getFilteredPricePoints(minPrice, false);
			
			// Check Brand/Size guideline violated
			List<Double> pricePointsBasedOnBrandAndSize = getPPBasedOnBrandAndSizeRelation(filterPricePoints, false);
			if(pricePointsBasedOnBrandAndSize!=null && pricePointsBasedOnBrandAndSize.size()>0) {
				
				finalFilteredPriceRange.addAll(pricePointsBasedOnBrandAndSize);
				
//				Set<Double> filteredPriceRange = pricePointsBasedOnBrandAndSize.stream().filter(pp -> pp >= minPrice).collect(Collectors.toSet());
//				if (filteredPriceRange == null || (filteredPriceRange != null && filteredPriceRange.size() == 0)) {
//					// If all the price points are less than 5%, then the price point which is closest to 5% will be chosen
//					Optional<Double> closestPP = actualPricePoints.stream().max(Comparator.comparing(Double::valueOf));
//					if (closestPP.isPresent()) {
//						finalFilteredPriceRange.add(closestPP.get());
//					} 
//				}else {
//					finalFilteredPriceRange.addAll(filteredPriceRange);
//				}
			}else {
				finalFilteredPriceRange.addAll(filterPricePoints);
//				finalFilteredPriceRange.addAll(actualPricePoints.stream().filter(pp -> pp >= minPrice).collect(Collectors.toSet()));
			}

			logger.debug("Item was never on sale in last " + noSaleInLastXWeeks + " weeks, don't reduce > " + noSaleItemDecreasePct + "% (" + minPrice
					+ ")unless cost or comp decrease. " + "Cur Reg Unit Price:" + curUnitRegPrice + ",Actual price points: "
					+ PRCommonUtil.getCommaSeperatedStringFromDouble(actualPricePoints) + ",Filtered price points: "
					+ finalFilteredPriceRange.stream().map(s -> s.toString()).collect(Collectors.joining(",")));

			if (actualPricePoints.size() > 0 && finalFilteredPriceRange.size() == 0) {
				logger.debug("5. Addition log");
				List<String> additionalDetails = new ArrayList<String>();
				additionalDetails.add(String.valueOf(regRetailUnchangedInLastXWeeks));
				additionalDetails.add(String.valueOf(noSaleItemDecreasePct) + "%");
				pricingEngineService.writeAdditionalExplainLog(itemDTO, ExplainRetailNoteTypeLookup.CUR_REG_RETAINED_RETAIL_UNCHANGED_NO_SALE,
						additionalDetails);
			} else {
				String pricePoints = getIgnoredPricePoints(finalFilteredPriceRange);
				if (pricePoints != "") {
					logger.debug("5-1. Addition log");
					List<String> additionalDetails = new ArrayList<String>();
					additionalDetails.add(pricePoints);
					additionalDetails.add(String.valueOf(noSaleItemDecreasePct) + "%");
					additionalDetails.add(String.valueOf(regRetailUnchangedInLastXWeeks));
					pricingEngineService.writeAdditionalExplainLog(itemDTO, ExplainRetailNoteTypeLookup.PP_IGNORED_NO_SALE, additionalDetails);
				}
			}
		}else {
			finalFilteredPriceRange.addAll(actualPricePoints);
		}
		return finalFilteredPriceRange.stream().collect(Collectors.toList());
	}

	private boolean isItemInSaleXWeeks(HashMap<String, RetailPriceDTO> itemPriceHistory, String inpBaseWeekStartDate, int noOfWeeks)
			throws GeneralException {
		
		boolean isItemInSale = false;
		LocalDate baseWeekStartDate = DateUtil.toDateAsLocalDate(inpBaseWeekStartDate);

		for (int i = 0; i < noOfWeeks; i++) {
			String tempWeekStartDate = DateUtil.localDateToString(baseWeekStartDate.minusDays(7 * (i)), Constants.APP_DATE_FORMAT);

			if (itemPriceHistory.get(tempWeekStartDate) != null) {
				RetailPriceDTO itemPrice = itemPriceHistory.get(tempWeekStartDate);
				Double unitPrice = PRCommonUtil.getUnitPrice(itemPrice.getSaleMPack(), Double.valueOf(itemPrice.getSalePrice()),
						Double.valueOf(itemPrice.getSaleMPrice()), true);
				if(unitPrice != null && unitPrice > 0) {
					isItemInSale = true;
					break;
				}
			}
		}
		return isItemInSale;
	}
}
