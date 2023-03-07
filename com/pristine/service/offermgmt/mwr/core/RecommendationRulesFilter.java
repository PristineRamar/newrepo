package com.pristine.service.offermgmt.mwr.core;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;

import com.pristine.dto.RetailPriceDTO;
import com.pristine.dto.offermgmt.MultiplePrice;
import com.pristine.dto.offermgmt.PRItemDTO;
import com.pristine.dto.offermgmt.RecommendationRuleMapDTO;
import com.pristine.dto.offermgmt.mwr.MWRItemDTO;
import com.pristine.exception.GeneralException;
import com.pristine.lookup.offermgmt.ExplainRetailNoteTypeLookup;
import com.pristine.service.offermgmt.PricingEngineService;
import com.pristine.service.offermgmt.recommendationrule.CheckFuturePromoEndsInXWeeksRule;
import com.pristine.service.offermgmt.recommendationrule.CheckOnGoingPromoEndsInXWeeksRule;
import com.pristine.service.offermgmt.recommendationrule.CheckRegRetailChangedInLastXWeeksRule;
import com.pristine.service.offermgmt.recommendationrule.RecommendationRuleHelper;
import com.pristine.util.PropertyManager;
import com.pristine.util.offermgmt.PRCommonUtil;

public class RecommendationRulesFilter {

	//private static Logger logger = Logger.getLogger("RecommendationRulesFilter");
	int regRetailChangeInLastXWeeks = Integer.parseInt(PropertyManager.getProperty("REG_RETAIL_CHANGE_IN_LAST_X_WEEKS"));
	public void applyRecommendationRules(PRItemDTO itemDTO,
			HashMap<String, List<RecommendationRuleMapDTO>> recommendationRuleMap,
			HashMap<Integer, HashMap<String, RetailPriceDTO>> priceHistory, String inpBaseWeekStartDate,
			MWRItemDTO mwrItemDTO) throws Exception, GeneralException {

		// Check if new price is required when there is a cost or comp change or brand or size violation
		boolean isNewPriceRequired = isNewPriceToBeRecommended(itemDTO);

		if (!isNewPriceRequired) {

			// 1. retain current price if there is a price change in last X weeks
			boolean isCurrPriceRetainRequired = isCurrPriceRetainRequired(itemDTO, recommendationRuleMap, priceHistory,
					inpBaseWeekStartDate);

			if (isCurrPriceRetainRequired) {
				MultiplePrice curRegPrice = PRCommonUtil.getMultiplePrice(itemDTO.getRegMPack(), itemDTO.getRegPrice(),
						itemDTO.getRegMPrice());
				mwrItemDTO.setPriceRange(new Double[] { PRCommonUtil.getUnitPrice(curRegPrice, false) });
			}
		}
	}

	/**
	 * 
	 * @param itemDTO
	 * @param recommendationRuleMap
	 * @param priceHistory
	 * @param inpBaseWeekStartDate
	 * @throws Exception
	 * @throws GeneralException
	 */
	public boolean isCurrPriceRetainRequired(PRItemDTO itemDTO,
			HashMap<String, List<RecommendationRuleMapDTO>> recommendationRuleMap,
			HashMap<Integer, HashMap<String, RetailPriceDTO>> priceHistory, String inpBaseWeekStartDate)
					throws Exception, GeneralException {
		
		boolean isCurrPriceRetainRequired = false;

		// Retain current price
		boolean isRetailChangedInLastXWeeks = isRetailChangedInLastXWeeks(itemDTO, recommendationRuleMap, priceHistory,
				inpBaseWeekStartDate);
		
		//Added by Karishma on 04/19/2022
				// Function is modified to set the explain log for LIG member only  if current retail
				// is retained because if LIG
				// member whose current price is retained is not the LIG Rep then this log
				// is not created ahead in PriceRollbackService
		if (isRetailChangedInLastXWeeks && itemDTO.getRetLirId() > 0
				&& itemDTO.getStrategyDTO().getConstriants() != null
				&& itemDTO.getStrategyDTO().getConstriants().getLocPriceConstraint() == null) {
			List<String> additionalDetails = new ArrayList<String>();
			additionalDetails.add(String.valueOf(regRetailChangeInLastXWeeks));
			new PricingEngineService().writeExplainLogForLIGItem(itemDTO,
					ExplainRetailNoteTypeLookup.CUR_REG_RETAINED_RETAIL_CHANGED_WITHIN_X_WEEKS, additionalDetails);
		}
		 
	
		// Retain current price
		boolean isOnGoingPromoEndsInXWeeks = isOnGoingPromoEndsInXWeeks(itemDTO, recommendationRuleMap);

		// Retain current price
		boolean isFuturePromoEndsInXWeeksRule = isFuturePromoEndsInXWeeksRule(itemDTO, recommendationRuleMap);

		if (isRetailChangedInLastXWeeks || isOnGoingPromoEndsInXWeeks || isFuturePromoEndsInXWeeksRule) {
			//condition added to not retain the current price when item is on locked strategy
			if (itemDTO.getStrategyDTO().getConstriants() != null
					&& itemDTO.getStrategyDTO().getConstriants().getLocPriceConstraint() == null) {
				isCurrPriceRetainRequired = true;
			}
			
		}

		return isCurrPriceRetainRequired;

	}

	/**
	 * 
	 * @param itemDTO
	 * @return true if current retail breaks cost/comp/size or brand guidlines
	 */
	public boolean isNewPriceToBeRecommended(PRItemDTO itemDTO) {
		boolean isNewPriceToBeRecommended = false;

		// Recommend new price
		boolean isCostChanged = itemDTO.getCostChgIndicator() == 0 ? false : true;

		// Recommend new price
		boolean isCompPriceChanged = itemDTO.getCompPriceChgIndicator() == 0 ? false : true;

		// Recommend new price
		boolean isCurrentPriceViolatesBrandSizeGuide = isCurrentPriceViolatesBrandSizeGuide(itemDTO);

		if (isCostChanged || isCompPriceChanged || isCurrentPriceViolatesBrandSizeGuide) {
			isNewPriceToBeRecommended = true;
		}

		return isNewPriceToBeRecommended;
	}

	/**
	 * 
	 * @param itemDTO
	 * @param recommendationRuleMap
	 * @param priceHistory
	 * @param inpBaseWeekStartDate
	 * @return true if there is price change in last X weeks
	 * @throws Exception
	 * @throws GeneralException
	 */
	public boolean isRetailChangedInLastXWeeks(PRItemDTO itemDTO,
			HashMap<String, List<RecommendationRuleMapDTO>> recommendationRuleMap,
			HashMap<Integer, HashMap<String, RetailPriceDTO>> priceHistory, String inpBaseWeekStartDate)
					throws Exception, GeneralException {

		boolean isRetailChangedInLastXWeeks = false;

		HashMap<String, RetailPriceDTO> itemPriceHistory = priceHistory.get(itemDTO.getItemCode());

		CheckRegRetailChangedInLastXWeeksRule checkRegRetailChangedInLastXWeeksRule = new CheckRegRetailChangedInLastXWeeksRule(
				itemDTO, recommendationRuleMap, itemPriceHistory, inpBaseWeekStartDate);

		if (checkRegRetailChangedInLastXWeeksRule.isCurrentRetailRetained()) {

			isRetailChangedInLastXWeeks = true;

		}

		return isRetailChangedInLastXWeeks;
	}

	/**
	 * 
	 * @param itemDTO
	 * @param recommendationRuleMap
	 * @param inpBaseWeekStartDate
	 * @return true if on going promo ends in X weeks
	 * @throws Exception
	 * @throws GeneralException
	 */
	public boolean isOnGoingPromoEndsInXWeeks(PRItemDTO itemDTO,
			HashMap<String, List<RecommendationRuleMapDTO>> recommendationRuleMap) throws Exception, GeneralException {

		boolean retainCurrentPrice = false;

		MultiplePrice curRegPrice = PRCommonUtil.getMultiplePrice(itemDTO.getRegMPack(), itemDTO.getRegPrice(),
				itemDTO.getRegMPrice());

		CheckOnGoingPromoEndsInXWeeksRule checkOnGoingPromoEndsInXWeeksRule = new CheckOnGoingPromoEndsInXWeeksRule(
				itemDTO, recommendationRuleMap);

		CheckFuturePromoEndsInXWeeksRule checkFuturePromoEndsInXWeeksRule = new CheckFuturePromoEndsInXWeeksRule(
				itemDTO, recommendationRuleMap);

		if ((itemDTO.isOnGoingPromotion() && !itemDTO.isFuturePromotion() && !itemDTO.isPromoEndsWithinXWeeks()
				&& checkOnGoingPromoEndsInXWeeksRule.isCurrentRetailRetained())
				|| (!checkFuturePromoEndsInXWeeksRule.isRuleEnabled() && itemDTO.isOnGoingPromotion()
						&& itemDTO.isFuturePromotion() && !itemDTO.isPromoEndsWithinXWeeks()
						&& checkOnGoingPromoEndsInXWeeksRule.isCurrentRetailRetained())) {
			if(itemDTO.getRecommendedRegPrice() != null && curRegPrice != null) {
				if (!itemDTO.getRecommendedRegPrice().equals(curRegPrice)) {
					retainCurrentPrice = true;
				}	
			}
		}

		return retainCurrentPrice;
	}

	/**
	 * 
	 * @param itemDTO
	 * @param recommendationRuleMap
	 * @return true if future promo ends in X weeks
	 * @throws Exception
	 * @throws GeneralException
	 */
	public boolean isFuturePromoEndsInXWeeksRule(PRItemDTO itemDTO,
			HashMap<String, List<RecommendationRuleMapDTO>> recommendationRuleMap) throws Exception, GeneralException {

		boolean retainCurrentPrice = false;

		MultiplePrice curRegPrice = PRCommonUtil.getMultiplePrice(itemDTO.getRegMPack(), itemDTO.getRegPrice(),
				itemDTO.getRegMPrice());

		CheckFuturePromoEndsInXWeeksRule checkFuturePromoEndsInXWeeksRule = new CheckFuturePromoEndsInXWeeksRule(
				itemDTO, recommendationRuleMap);

		if (itemDTO.isFuturePromotion() && !itemDTO.isPromoEndsWithinXWeeks()
				&& checkFuturePromoEndsInXWeeksRule.isCurrentRetailRetained()) {
			if(itemDTO.getRecommendedRegPrice() != null && curRegPrice != null) {
				if (!itemDTO.getRecommendedRegPrice().equals(curRegPrice)) {
					retainCurrentPrice = true;
				}
			}
		}

		return retainCurrentPrice;
	}

	/**
	 * 
	 * @param itemDTO
	 * @param recommendationRuleMap
	 * @return true if currnent price vioates brand/size guideline
	 */
	public boolean isCurrentPriceViolatesBrandSizeGuide(PRItemDTO itemDTO) {
		boolean isCurrentPriceViolatesBrandSizeGuide = true;

		RecommendationRuleHelper recommendationRuleHelper = new RecommendationRuleHelper();
		
		if (itemDTO.getRegPrice() != null) {
			MultiplePrice curRegPrice = PRCommonUtil.getMultiplePrice(itemDTO.getRegMPack(), itemDTO.getRegPrice(),
					itemDTO.getRegMPrice());
			if(curRegPrice != null) {
				if (recommendationRuleHelper.checkPriceRangeUsingRelatedItem(itemDTO, curRegPrice.multiple,
						curRegPrice.price)) {
					isCurrentPriceViolatesBrandSizeGuide = false;
				}	
			}
		} else {
			isCurrentPriceViolatesBrandSizeGuide = false;
		}

		return isCurrentPriceViolatesBrandSizeGuide;
	}
}