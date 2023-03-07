package com.pristine.service.offermgmt.recommendationrule;

import java.time.LocalDate;
import java.util.*;
import java.util.stream.Collectors;

import org.apache.log4j.Logger;

import com.pristine.dto.RetailPriceDTO;
import com.pristine.dto.offermgmt.MultiplePrice;
import com.pristine.dto.offermgmt.PRGuidelineAndConstraintLog;
import com.pristine.dto.offermgmt.PRItemDTO;
import com.pristine.dto.offermgmt.RecommendationRuleMapDTO;
import com.pristine.exception.GeneralException;
import com.pristine.service.offermgmt.GuidelineTypeLookup;
import com.pristine.service.offermgmt.PricingEngineService;
import com.pristine.util.Constants;
import com.pristine.util.DateUtil;
import com.pristine.util.PropertyManager;
import com.pristine.util.offermgmt.PRCommonUtil;
import com.pristine.util.offermgmt.PRFormatHelper;

public abstract class FilterPricePointsRule  extends RecommendationRule{
	private static Logger logger = Logger.getLogger("RecommendationRule");
	List<Double> actualPricePoints = new ArrayList<>();
	private PRItemDTO itemDTO;
	HashMap<String, List<RecommendationRuleMapDTO>> recommendationRuleMap;

	abstract public List<Double> applyRule() throws Exception, GeneralException;
//	abstract public boolean isRuleEnabled() throws Exception, GeneralException;
//	abstract public boolean checkBrandAndSizeOrCostViolated() throws Exception, GeneralException;
	
	public FilterPricePointsRule(PRItemDTO itemDTO, List<Double> actualPricePoints,
			HashMap<String, List<RecommendationRuleMapDTO>> recommendationRuleMap) {
		super(itemDTO, recommendationRuleMap);
		this.itemDTO = itemDTO;
		this.actualPricePoints = actualPricePoints;
		this.recommendationRuleMap = recommendationRuleMap;
	}

	public FilterPricePointsRule(PRItemDTO itemDTO, HashMap<String, List<RecommendationRuleMapDTO>> recommendationRuleMap) {
		super(itemDTO, recommendationRuleMap);
		this.itemDTO = itemDTO;
		this.recommendationRuleMap = recommendationRuleMap;
	}

	/**
	 * @return
	 */
	protected List<Double> getPPBasedOnBrandAndSizeRelation(Set<Double> filteredPricePoints, boolean getMinValue) {

		boolean isBrandOrSizeGuidelineApplied = false;
		List<Double> pricePointsList = new ArrayList<>();
		PricingEngineService pricingEngineService = new PricingEngineService();
		for (PRGuidelineAndConstraintLog guidelineAndConstraintLog : itemDTO.getExplainLog().getGuidelineAndConstraintLogs()) {
			if (guidelineAndConstraintLog.getGuidelineTypeId() == GuidelineTypeLookup.BRAND.getGuidelineTypeId()
					|| guidelineAndConstraintLog.getGuidelineTypeId() == GuidelineTypeLookup.SIZE.getGuidelineTypeId()) {
				// Check Brand/Size violated
				isBrandOrSizeGuidelineApplied = true;
				filteredPricePoints.forEach(pp -> {
					if (pricingEngineService.checkIfPriceWithinRange(1, pp, guidelineAndConstraintLog.getOutputPriceRange(),
							guidelineAndConstraintLog)) {
						pricePointsList.add(pp);
					}
				});
				
				if(pricePointsList.size()==0) {
					List<Double> tempPricePoints = new ArrayList<>();
					actualPricePoints.forEach(pp -> {
						if (pricingEngineService.checkIfPriceWithinRange(1, pp, guidelineAndConstraintLog.getOutputPriceRange(),
								guidelineAndConstraintLog)) {
							tempPricePoints.add(pp);
						}
					});
					Optional<Double> closestPPs;
					if(getMinValue) {
						closestPPs = tempPricePoints.stream().min(Comparator.comparing(Double::valueOf));
					}else {
						closestPPs = tempPricePoints.stream().max(Comparator.comparing(Double::valueOf));
					}
					
					if(closestPPs!=null && closestPPs.isPresent()) {
						pricePointsList.add(closestPPs.get());
					}
				}
				
			}
		}
		
		if(isBrandOrSizeGuidelineApplied && pricePointsList.size()==0) {
			pricePointsList.addAll(actualPricePoints);
		}
		return pricePointsList;
	}
	
	public List<Double> maintainBrandAndSizeRelation() {
		List<Double> filteredPricePoints = new ArrayList<>();
		PricingEngineService pricingEngineService = new PricingEngineService();
		boolean isBrandGuidelineApplied = pricingEngineService.isGuidelineApplied(itemDTO,
				GuidelineTypeLookup.BRAND.getGuidelineTypeId());
		boolean isSizeGuidelineApplied = pricingEngineService.isGuidelineApplied(itemDTO,
				GuidelineTypeLookup.SIZE.getGuidelineTypeId());
		if ((isBrandGuidelineApplied || isSizeGuidelineApplied) && filteredPricePoints.size() == 0) {
			for (Double pricePoint : actualPricePoints) {
				filteredPricePoints.add(pricePoint);
			}
			logger.debug(
					"No price point available after filtering, so price points are retained as it may break the brand and size relation");
		}
		return filteredPricePoints;
	}

	protected MultiplePrice getMaxPriceInLastXWeeks(HashMap<String, RetailPriceDTO> itemPriceHistory,
			String inpBaseWeekStartDate, int noOfWeeks, String priceType) throws GeneralException {
		MultiplePrice maxPrice = null;
		Double lastUnitPrice = 0d;
		RetailPriceDTO retailPriceDTO = null;

		LocalDate baseWeekStartDate = DateUtil.toDateAsLocalDate(inpBaseWeekStartDate);

		for (int i = 0; i < noOfWeeks; i++) {
			String tempWeekStartDate = DateUtil.localDateToString(baseWeekStartDate.minusDays(7 * (i)),
					Constants.APP_DATE_FORMAT);

			// Get that week price
			if (itemPriceHistory.get(tempWeekStartDate) != null) {
				RetailPriceDTO itemPrice = itemPriceHistory.get(tempWeekStartDate);
				
				Double unitPrice = null;
				if (priceType.equals("REG")) {
					unitPrice = PRCommonUtil.getUnitPrice(itemPrice.getRegMPack(),
							Double.valueOf(itemPrice.getRegPrice()), Double.valueOf(itemPrice.getRegMPrice()), true);
				} else {
					unitPrice = PRCommonUtil.getUnitPrice(itemPrice.getSaleMPack(),
							Double.valueOf(itemPrice.getSalePrice()), Double.valueOf(itemPrice.getSaleMPrice()), true);
				}
				// logger.debug("unit price for week " + tempWeekStartDate + ":" + unitPrice);

				if (unitPrice != null && unitPrice > 0) {
					if (lastUnitPrice == 0) {
						retailPriceDTO = itemPrice;
						lastUnitPrice = unitPrice;
					} else if (unitPrice > lastUnitPrice) {
						retailPriceDTO = itemPrice;
						lastUnitPrice = unitPrice;
					}
				}
			}
		}

		if (retailPriceDTO != null) {
			if (priceType.equals("REG")) {
				maxPrice = PRCommonUtil.getMultiplePrice(retailPriceDTO.getRegMPack(),
						Double.valueOf(retailPriceDTO.getRegPrice()), Double.valueOf(retailPriceDTO.getRegMPrice()));
			} else {
				maxPrice = PRCommonUtil.getMultiplePrice(retailPriceDTO.getSaleMPack(),
						Double.valueOf(retailPriceDTO.getSalePrice()), Double.valueOf(retailPriceDTO.getSaleMPrice()));
			}
		}
		return maxPrice;
	}

	protected String getIgnoredPricePoints(Set<Double> filteredPricePoints) {
		String ignoredPricePoints = "";
		List<Double> tempList = new ArrayList<>(actualPricePoints);
		tempList.removeAll(filteredPricePoints);

		for (Double d : tempList) {
			ignoredPricePoints = ignoredPricePoints + "," + PRFormatHelper.doubleToTwoDigitString(d);
		}

		if (ignoredPricePoints != "") {
			ignoredPricePoints = ignoredPricePoints.substring(1);
		}

		return ignoredPricePoints;
	}
	
	protected boolean checkPriceRangeUsingRelatedItem(PRItemDTO itemDTO, int inpRegMultiple, double inpRegPrice) {
		boolean isPriceWithInRange = true;
		int regMultiple = 0;
		double regPrice=0.0;
		
		if(inpRegMultiple > 0 && inpRegPrice>0) {
			regMultiple  = inpRegMultiple;
			regPrice = inpRegPrice;
		}else {
			regMultiple  = itemDTO.getRecommendedRegPrice().multiple;
			regPrice = itemDTO.getRecommendedRegPrice().price;
		}
		
		PricingEngineService pricingEngineService = new PricingEngineService();
		for (PRGuidelineAndConstraintLog guidelineAndConstraintLog : itemDTO.getExplainLog().getGuidelineAndConstraintLogs()) {
			if (guidelineAndConstraintLog.getGuidelineTypeId() == GuidelineTypeLookup.BRAND.getGuidelineTypeId()
					|| guidelineAndConstraintLog.getGuidelineTypeId() == GuidelineTypeLookup.SIZE.getGuidelineTypeId()) {		
				if (!pricingEngineService.checkIfPriceWithinRange(regMultiple, regPrice,
						guidelineAndConstraintLog.getOutputPriceRange(), guidelineAndConstraintLog)) {
					isPriceWithInRange = false;
				}
			}
		}
		return isPriceWithInRange;
	}
	
	public boolean isGuidelineApplied(PRItemDTO itemDTO, int guidelineTypeId) {
		boolean isGuidelineApplied = false;
		if (itemDTO.getExplainLog() != null) {
			for (PRGuidelineAndConstraintLog guidelineAndConstraintLog : itemDTO.getExplainLog().getGuidelineAndConstraintLogs()) {
				if (guidelineAndConstraintLog.getGuidelineTypeId() == guidelineTypeId
						&& guidelineAndConstraintLog.getIsGuidelineOrConstraintApplied()) {
					isGuidelineApplied = true;
				}
			}
		}
		return isGuidelineApplied;
	}
	
//	private boolean isRegPriceChangedInLastXWeeks(HashMap<String, RetailPriceDTO> itemPriceHistory, String baseWeekStartDate, int noOfWeeks)
//			throws GeneralException {
//
//		int priceChangeStatus = itemPriceChangeStatus(itemPriceHistory, DateUtil.toDateAsLocalDate(baseWeekStartDate), noOfWeeks);
//
//		boolean isPriceChanged = (priceChangeStatus == 0 ? false : true);
//
//		return isPriceChanged;
//	}

	/**
	 * return -1 - decreased, 0 - unchanged, 1 - increased
	 * @param itemPriceHistory
	 * @param itemCode
	 * @param inpWeekStartDate
	 * @param noOfDays
	 * @return
	 * @throws GeneralException
	 */
//	private int itemPriceChangeStatus(HashMap<String, RetailPriceDTO> itemPriceHistory, LocalDate baseWeekStartDate, int noOfWeeks)
//			throws GeneralException {
//		int itemPriceChangeStatus = 0;
//
//		LocalDate baseWeekEndDate = baseWeekStartDate.plusDays(6);
//		LocalDate inpStartDateRange = baseWeekStartDate.minusWeeks(noOfWeeks - 1);
//		
//		// Convert to weeks so that price history
//		// is checked within these weeks
////		long daysBetween = java.time.temporal.ChronoUnit.DAYS.between(inpStartDateRange, baseWeekEndDate);
////		int noOfWeeks = (int) Math.ceil(daysBetween / 7.0);
//		Double lastRegUnitPrice = 0d;
//		for (int i = 0; i < noOfWeeks; i++) {
//			String tempWeekStartDate = DateUtil.localDateToString(baseWeekStartDate.minusDays(7 * (i)), Constants.APP_DATE_FORMAT);
//
//			// Get that week price
//			if (itemPriceHistory.get(tempWeekStartDate) != null) {
//				RetailPriceDTO itemPrice = itemPriceHistory.get(tempWeekStartDate);
//				Double regUnitPrice = PRCommonUtil.getUnitPrice(itemPrice.getRegMPack(), Double.valueOf(itemPrice.getRegPrice()),
//						Double.valueOf(itemPrice.getRegMPrice()), true);
//
//				if (regUnitPrice > 0 && itemPrice.getRegEffectiveDate() != null) {
//					LocalDate regEffectiveDate = DateUtil.toDateAsLocalDate(itemPrice.getRegEffectiveDate());
//
//					// Check if there is price within the date range
//					if (regEffectiveDate.isAfter(inpStartDateRange) && regEffectiveDate.isBefore(baseWeekEndDate) && lastRegUnitPrice == 0) {
//						lastRegUnitPrice = regUnitPrice;
//					//} else if (regEffectiveDate.isAfter(inpStartDateRange) && regEffectiveDate.isBefore(baseWeekEndDate)) {
//					} else {
//
//						if (lastRegUnitPrice > 0 && lastRegUnitPrice - regUnitPrice > 0) {
//							itemPriceChangeStatus = 1;
//							break;
//						} else if (lastRegUnitPrice > 0 && lastRegUnitPrice - regUnitPrice < 0) {
//							itemPriceChangeStatus = -1;
//							break;
//						}
//					}
//				}
//			}
//		}
//
//		return itemPriceChangeStatus;
//	}
	
	protected Set<Double> getFilteredPricePoints(Double inputPrice, boolean getMinValue){
		
		Set<Double> filteredPriceRange = new HashSet<>();
		if(getMinValue) {
			filteredPriceRange = actualPricePoints.stream().filter(price -> price <= inputPrice)
					.collect(Collectors.toSet());
		}else {
			filteredPriceRange = actualPricePoints.stream().filter(price -> price >= inputPrice)
					.collect(Collectors.toSet());
		}
		return filteredPriceRange;
	}
}
