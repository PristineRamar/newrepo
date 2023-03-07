package com.pristine.service.offermgmt;

import java.text.ParseException;
import java.time.LocalDate;
import java.time.format.DateTimeFormatter;
import java.time.temporal.ChronoUnit;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;

import org.apache.log4j.Logger;

import com.pristine.dto.CompetitiveDataDTO;
import com.pristine.dto.RetailCalendarDTO;
import com.pristine.dto.RetailPriceDTO;
import com.pristine.dto.offermgmt.LocationKey;
import com.pristine.dto.offermgmt.MultiCompetitorKey;
import com.pristine.dto.offermgmt.MultiplePrice;
import com.pristine.dto.offermgmt.PRExplainAdditionalDetail;
import com.pristine.dto.offermgmt.PRItemAdInfoDTO;
import com.pristine.dto.offermgmt.PRItemDTO;
import com.pristine.dto.offermgmt.PRItemSaleInfoDTO;
import com.pristine.dto.offermgmt.PRRecommendation;
import com.pristine.dto.offermgmt.PRRecommendationRunHeader;
import com.pristine.dto.offermgmt.RecommendationRuleMapDTO;
import com.pristine.exception.GeneralException;
import com.pristine.exception.OfferManagementException;
import com.pristine.lookup.offermgmt.ExplainRetailNoteTypeLookup;
import com.pristine.service.offermgmt.constraint.LIGConstraint;
import com.pristine.service.offermgmt.recommendationrule.CheckFuturePromoEndsInXWeeksRule;
import com.pristine.service.offermgmt.recommendationrule.CheckInSufficientMarginBenefitRule;
import com.pristine.service.offermgmt.recommendationrule.CheckOnGoingPromoEndsInXWeeksRule;
import com.pristine.service.offermgmt.recommendationrule.CheckPromoStartsWithinXWeeksRule;
import com.pristine.service.offermgmt.recommendationrule.CheckRegRetailChangedInLastXWeeksRule;
import com.pristine.service.offermgmt.recommendationrule.CheckRetailCurMultiplePriceRule;
import com.pristine.service.offermgmt.recommendationrule.CheckZeroPredictionRule;
import com.pristine.service.offermgmt.recommendationrule.FutureRetailChangeRule;
import com.pristine.service.offermgmt.recommendationrule.PastOverridenRetailRule;
import com.pristine.service.offermgmt.recommendationrule.RetainCurrentRetailRule;
import com.pristine.util.Constants;
import com.pristine.util.DateUtil;
import com.pristine.util.PropertyManager;
import com.pristine.util.offermgmt.PRCommonUtil;
import com.pristine.util.offermgmt.PRConstants;
import com.pristine.util.offermgmt.PRFormatHelper;

public class PriceRollbackService {
	private static Logger logger = Logger.getLogger("PriceRollbackService");

	private static final boolean checkForGuardRailConstraint = Boolean
			.parseBoolean(PropertyManager.getProperty("CHECK_GUARDRAIL_CONSTRAINT_APPLIED", "FALSE"));
	boolean chkFutureCost = Boolean.parseBoolean(PropertyManager.getProperty("USE_FUTURECOST", "FALSE"));
	int longTermPromoDays = Integer.parseInt(PropertyManager.getProperty("LONG_TERM_PROMO_DAYS","180"));
	int xWeeksOfPromo = Integer.parseInt(PropertyManager.getProperty("PROMO_X_WEEKS_FROM_REC_WEEK"));


	DateTimeFormatter formatter = DateTimeFormatter.ofPattern(Constants.APP_DATE_FORMAT);

	public void retainCurrentRetail(List<PRItemDTO> itemListWithRecPrice,
			PRRecommendationRunHeader recommendationRunHeader, HashMap<Integer, List<PRItemSaleInfoDTO>> saleDetails,
			HashMap<Integer, List<PRItemAdInfoDTO>> adDetails, int noOfStores, HashMap<ItemKey, PRItemDTO> itemDataMap,
			HashMap<Integer, LocationKey> compIdMap, HashMap<Integer, HashMap<Integer, Character>> retLirConstraintMap,
			HashMap<MultiCompetitorKey, CompetitiveDataDTO> multiCompLatestPriceMap, String curWeekStartDate,
			HashMap<LocationKey, HashMap<ItemKey, PRRecommendation>> leadZoneDetails, boolean isRecommendAtStoreLevel,
			HashMap<Integer, HashMap<String, RetailPriceDTO>> itemZonePriceHistory, RetailCalendarDTO curCalDTO,
			double maxUnitPriceDiff, HashMap<String, List<RecommendationRuleMapDTO>> recommendationRuleMap)
			throws ParseException, GeneralException {
		
		PricingEngineService pricingEngineService = new PricingEngineService();
		CheckPromoStartsWithinXWeeksRule checkPromoStartsWithinXWeeksRule=new CheckPromoStartsWithinXWeeksRule(new PRItemDTO(), recommendationRuleMap);	
		
		try {
			// Loop each non-lig and lig
			for (PRItemDTO itemDTO : itemListWithRecPrice) {
				// Changes done for RA-43 to skip post recommendation rules if guardrail
				// constraint is set for a lead zone.
				// Property added for FF and condition corrected by Karishma on 09/06 for
				// checking if guardrail constraint is null
				if (checkForGuardRailConstraint && (itemDTO.getStrategyDTO().getConstriants()
						.getGuardrailConstraint() == null
						|| (itemDTO.getStrategyDTO().getConstriants().getGuardrailConstraint() != null && !itemDTO
								.getStrategyDTO().getConstriants().getGuardrailConstraint().isZonePresent()))) {

					validateAndRetainCurrentRetail(itemDTO, itemListWithRecPrice, saleDetails, adDetails,
							recommendationRunHeader, noOfStores, maxUnitPriceDiff, recommendationRuleMap,
							itemZonePriceHistory, curCalDTO);
				} else
					validateAndRetainCurrentRetail(itemDTO, itemListWithRecPrice, saleDetails, adDetails,
							recommendationRunHeader, noOfStores, maxUnitPriceDiff, recommendationRuleMap,
							itemZonePriceHistory, curCalDTO);
			}

		
			HashMap<Integer, List<PRItemDTO>> ligMap = pricingEngineService.formLigMap(itemDataMap);

			pricingEngineService.prioritizeAdditionalLog(itemListWithRecPrice);

			// update lig level data as related item can be a LIG
			new LIGConstraint().applyLIGConstraint(ligMap, itemDataMap, retLirConstraintMap);

			// Again recommend the prices of price group relation items
			new PriceGroupService().recommendPriceGroupRelatedItems(recommendationRunHeader, itemListWithRecPrice,
					itemDataMap, compIdMap, retLirConstraintMap, multiCompLatestPriceMap, curWeekStartDate,
					leadZoneDetails, isRecommendAtStoreLevel, itemZonePriceHistory, curCalDTO, maxUnitPriceDiff,
					recommendationRuleMap);

			
			// Update recommended prices prediction and status and pred mov wo subs,
			// isconflict and conflict in explain log.
			pricingEngineService.updateItemAttributes(itemListWithRecPrice, recommendationRunHeader);

			// Reset applyObj to FALSE as LIg constraint has been applied by this step and
			// repItemselection should not happen again
			ligMap.forEach((locationId, members) -> {

				members.forEach(item -> {
					item.setFinalObjectiveApplied(true);
				});

			});

			for (PRItemDTO itemDTO : itemListWithRecPrice) {
				// Added By Karishma on 02/22/2022 for AZ
				// if the current retail is retained then compare it with MAP retail
				// if present and it should not be below MAP,if its below MAP then use MAP
				// retail with rounding applied to it

				if ((itemDTO.isLir() || (!itemDTO.isLir() && itemDTO.getRetLirId() == 0))
						&& itemDTO.getStrategyDTO().getConstriants() != null
						&& itemDTO.getStrategyDTO().getConstriants().getLocPriceConstraint() == null) {
					checkCurrentRetailWithMapIfCurrentRetailIsRetained(itemDTO, itemListWithRecPrice);
				}
			}

			new LIGConstraint().applyLIGConstraint(ligMap, itemDataMap, retLirConstraintMap);
			
			//Postdating items on sale and having new recommendation after promotion ends
			//This Step is moved and added at end  after the final objective is applied for LIG
			//if rule R21 is enabled then no postdating should happen for FF
			if (!checkPromoStartsWithinXWeeksRule.isRuleEnabled()) {
				logger.info("Postdating promoted items...");
				new PricingEngineService().setEffectiveDate(itemListWithRecPrice, saleDetails, adDetails,
						recommendationRunHeader, recommendationRuleMap);
			}

		} catch (Exception | OfferManagementException ex) {
			ex.printStackTrace();
			logger.error("Error in rollbackPrices() " + ex);
			throw new GeneralException("Error in rollbackPrices() - " + ex.getMessage());
		}

	}

	/**
	 * @param itemDTO
	 * @param itemListWithRecPrice
	 * @param saleDetails
	 * @param adDetails
	 * @param recommendationRunHeader
	 * @param noOfStores
	 * @param maxUnitPriceDiff
	 * @param recommendationRuleMap
	 * @param itemZonePriceHistory
	 * @param curCalDTO
	 * @throws GeneralException
	 * @throws Exception
	 */
	public void validateAndRetainCurrentRetail(PRItemDTO itemDTO, List<PRItemDTO> itemListWithRecPrice,
			HashMap<Integer, List<PRItemSaleInfoDTO>> saleDetails, HashMap<Integer, List<PRItemAdInfoDTO>> adDetails,
			PRRecommendationRunHeader recommendationRunHeader, int noOfStores, double maxUnitPriceDiff,
			HashMap<String, List<RecommendationRuleMapDTO>> recommendationRuleMap,
			HashMap<Integer, HashMap<String, RetailPriceDTO>> itemZonePriceHistory, RetailCalendarDTO curCalDTO)
			throws GeneralException, Exception {

		PricingEngineService pricingEngineService = new PricingEngineService();
		MultiplePrice curRegPrice = PRCommonUtil.getMultiplePrice(itemDTO.getRegMPack(), itemDTO.getRegPrice(),
				itemDTO.getRegMPrice());
		int longTermPromoWeeks = Integer.parseInt(PropertyManager.getProperty("PROMO_X_WEEKS_FROM_REC_WEEK"));
		DateTimeFormatter formatter = DateTimeFormatter.ofPattern(Constants.APP_DATE_FORMAT);
		// lig or non-lig
		if ((itemDTO.isLir() || (!itemDTO.isLir() && itemDTO.getRetLirId() <= 0)) && curRegPrice != null
				&& itemDTO.getStrategyDTO().getConstriants() != null
				&& itemDTO.getStrategyDTO().getConstriants().getLocPriceConstraint() == null) {
			boolean retainCurrentRetail = false;
			
			// If processing Lig item, then consider price history of Representing item,
			// Since Lig item dont have price history details

			int processingItemCode = itemDTO.isLir() ? itemDTO.getLigRepItemCode() : itemDTO.getItemCode();
			// Double curRegUnitPrice = PRCommonUtil.getUnitPrice(curRegPrice, true);
			Double recRegUnitPrice = PRCommonUtil.getUnitPrice(itemDTO.getRecommendedRegPrice(), true);

			RetainCurrentRetailRule checkRetailCurMultiplePriceRule = new CheckRetailCurMultiplePriceRule(itemDTO,
					recommendationRuleMap);
			RetainCurrentRetailRule checkInSufficientMarginBenefitRule = new CheckInSufficientMarginBenefitRule(itemDTO,
					recommendationRuleMap, noOfStores);
			RetainCurrentRetailRule checkZeroPredictionRule = new CheckZeroPredictionRule(itemDTO,
					recommendationRuleMap);
			RetainCurrentRetailRule checkFuturePromoEndsInXWeeksRule = new CheckFuturePromoEndsInXWeeksRule(itemDTO,
					recommendationRuleMap);
			RetainCurrentRetailRule checkOnGoingPromoEndsInXWeeksRule = new CheckOnGoingPromoEndsInXWeeksRule(itemDTO,
					recommendationRuleMap);
			RetainCurrentRetailRule checkRegRetailChangedInLastXWeeksRule = new CheckRegRetailChangedInLastXWeeksRule(
					itemDTO, recommendationRuleMap, itemZonePriceHistory.get(processingItemCode),
					curCalDTO.getStartDate());
			RetainCurrentRetailRule futureRetailChangeRule = new FutureRetailChangeRule(itemDTO, recommendationRuleMap);
			PastOverridenRetailRule pastOverrideRule = new PastOverridenRetailRule(itemDTO, recommendationRuleMap);
			//rules added for FF
			CheckPromoStartsWithinXWeeksRule checkPromoStartsWithinXWeeksRule=new CheckPromoStartsWithinXWeeksRule(itemDTO, recommendationRuleMap);	
			List<PRItemDTO> ligMembersOrNonLig = pricingEngineService.getLigMembersOrNonLigItem(itemDTO,
					itemListWithRecPrice);
		
			
			// retain current retail only for item/lig member member having clearnce price
			if (itemDTO.isClearanceItem()) {

				logger.debug("*** Current retail is retained as item is on Clearance:"
						+ (itemDTO.isLir() ? " LIG:" : " non-lig:") + itemDTO.getItemCode() + " *** ");
				for (PRItemDTO item : ligMembersOrNonLig) {
					MultiplePrice regPrice = setRegPrice(item);
					if (item.isClearanceItem() && item.getRecommendedRegPrice() != null
							&& !item.getRecommendedRegPrice().equals(regPrice) && regPrice != null
							&& regPrice.getUnitPrice() > 0) {
						item.setRecommendedRegPrice(regPrice);
						item.setCurPriceRetained(true);
						List<String> addtLogDetails = new ArrayList<String>();
						addtLogDetails.add(String.valueOf(item.getClearanceRetail()));
						addtLogDetails.add(item.getClearanceRetailEffDate());
						pricingEngineService.writeAdditionalExplainLog(item, itemListWithRecPrice,
								ExplainRetailNoteTypeLookup.CUR_REG_RETAINED_SINCE_ITEM_IS_ON_CLEARNCE, addtLogDetails);
					}
				}
			} // retain current retail even if one member in LIG has a future price
			 if (itemDTO.isFuturePricePresent()) {
				for (PRItemDTO item : ligMembersOrNonLig) {
					MultiplePrice curPrice = setRegPrice(item);
					if (!item.isClearanceItem()) {
						if (item.isFuturePricePresent() && item.getRecommendedRegPrice() != null && curPrice != null
								&& curPrice.getUnitPrice() > 0 && !item.getRecommendedRegPrice().equals(curPrice)) {
							logger.debug("*** Current retail is retained as item has Future Price:" + item.getItemCode()
									+ " *** ");
							item.setRecommendedRegPrice(curPrice);
							item.setCurPriceRetained(true);
							List<String> addtLogDetails = new ArrayList<String>();
							addtLogDetails.add(String.valueOf(item.getFutureUnitPrice()));
							addtLogDetails.add(item.getFuturePriceEffDate());
							pricingEngineService.writeAdditionalExplainLog(item, itemListWithRecPrice,
									ExplainRetailNoteTypeLookup.CUR_REG_RETAINED_SINCE_ITEM_HAS_FUTURE_PRICE,
									addtLogDetails);

						} else if (item.getRecommendedRegPrice() != null && curPrice != null
								&& curPrice.getUnitPrice() > 0 && !item.getRecommendedRegPrice().equals(curPrice)) {
							item.setRecommendedRegPrice(curPrice);
							item.setCurPriceRetained(true);
							logger.debug("*** Current retail is retained as LIG item has Future Price:"
									+ item.getItemCode() + " *** ");
							List<String> addtLogDetails = new ArrayList<String>();
							pricingEngineService.writeAdditionalExplainLog(item, itemListWithRecPrice,
									ExplainRetailNoteTypeLookup.CUR_REG_RETAINED_SINCE_LIG_HAS_FUTURE_PRICE,
									addtLogDetails);
						}
					}
			}
			 }

			/*
			 * Rule R9: If an item is in on-going promotion and if it is going to end after
			 * 6 weeks from recommendation week, then do not change the retail unless the
			 * brand/size relation warrants the change.
			 */
			else if ((itemDTO.isOnGoingPromotion() && !itemDTO.isFuturePromotion() && !itemDTO.isPromoEndsWithinXWeeks()
					&& checkOnGoingPromoEndsInXWeeksRule.isCurrentRetailRetained())
					|| (!checkFuturePromoEndsInXWeeksRule.isRuleEnabled() && itemDTO.isOnGoingPromotion()
							&& itemDTO.isFuturePromotion() && !itemDTO.isPromoEndsWithinXWeeks()
							&& checkOnGoingPromoEndsInXWeeksRule.isCurrentRetailRetained())) {
				if ((itemDTO.getRecommendedRegPrice() != null && !itemDTO.getRecommendedRegPrice().equals(curRegPrice)
						&& chkFutureCost && itemDTO.getFutureListCost() == null
								|| (itemDTO.getFutureListCost() != null && itemDTO.getCostChgIndicator() == 0))
						|| (itemDTO.getRecommendedRegPrice() != null && !itemDTO.getRecommendedRegPrice().equals(curRegPrice)
						&& !chkFutureCost)) {
					retainCurrentRetail = true;

					logger.debug("*** Current retail is retained as item in long term promotion:"
							+ (itemDTO.isLir() ? " LIG:" : " non-lig:") + itemDTO.getItemCode() + " *** ");

					List<String> addtLogDetails = new ArrayList<String>();
					addtLogDetails
							.add(formatter.format(LocalDate.parse(recommendationRunHeader.getStartDate(), formatter)
									.plus(longTermPromoWeeks, ChronoUnit.WEEKS).plus(6, ChronoUnit.DAYS)));
					pricingEngineService.writeAddtExpLogForTPRRelLigItems(itemDTO, itemListWithRecPrice,
							ExplainRetailNoteTypeLookup.CUR_REG_RETAINED_PROMO_ENDS_AFTER_X_WEEKS, addtLogDetails);
				}
			}

			/*
			 * Rule R8: If an item is in future promotion and if it is going to end after 6
			 * weeks from recommendation week, then do not change the retail unless the
			 * brand/size relation warrants the change.
			 */
			else if (itemDTO.isFuturePromotion() && !itemDTO.isPromoEndsWithinXWeeks()
					&& checkFuturePromoEndsInXWeeksRule.isCurrentRetailRetained()) {
				if ((itemDTO.getRecommendedRegPrice() != null && !itemDTO.getRecommendedRegPrice().equals(curRegPrice)
						&& (chkFutureCost && itemDTO.getFutureListCost() == null
								|| (itemDTO.getFutureListCost() != null && itemDTO.getCostChgIndicator() == 0))) ||
						(itemDTO.getRecommendedRegPrice() != null && !itemDTO.getRecommendedRegPrice().equals(curRegPrice)
						&& ! chkFutureCost )) {
					List<String> additionalDetails = new ArrayList<String>();
					additionalDetails.add(String.valueOf(recRegUnitPrice));
					retainCurrentRetail = true;

					logger.debug("*** Current retail is retained for" + (itemDTO.isLir() ? " LIG:" : " non-lig:")
							+ itemDTO.getItemCode() + " as there is no rec retail and item is on sale ***");
					List<String> addtLogDetails = new ArrayList<String>();
					addtLogDetails
							.add(formatter.format(LocalDate.parse(recommendationRunHeader.getStartDate(), formatter)
									.plus(longTermPromoWeeks, ChronoUnit.WEEKS).plus(6, ChronoUnit.DAYS)));

					pricingEngineService.writeAddtExpLogForTPRRelLigItems(itemDTO, itemListWithRecPrice,
							ExplainRetailNoteTypeLookup.CUR_REG_RETAINED_PROMO_ENDS_AFTER_X_WEEKS, addtLogDetails);
				}
			}

			else if (checkZeroPredictionRule.isCurrentRetailRetained()) {
				logger.debug("*** Current retail is retained for" + (itemDTO.isLir() ? " LIG:" : " non-lig:")
						+ itemDTO.getItemCode() + " as there is no valid prediction ***");
				retainCurrentRetail = true;
				pricingEngineService.writeAdditionalExplainLog(itemDTO, itemListWithRecPrice,
						ExplainRetailNoteTypeLookup.CUR_REG_RETAINED_AS_NO_VALID_PRED, new ArrayList<String>());
			} else if (checkInSufficientMarginBenefitRule.isCurrentRetailRetained()) {
				logger.debug("*** Current retail is retained for" + (itemDTO.isLir() ? " LIG:" : " non-lig:")
						+ itemDTO.getItemCode() + " as there is no sufficient margin benefit ***");
				retainCurrentRetail = true;
				List<String> additionalDetails = new ArrayList<String>();
				additionalDetails.add(String.valueOf(recRegUnitPrice));
				pricingEngineService.writeAdditionalExplainLog(itemDTO, itemListWithRecPrice,
						ExplainRetailNoteTypeLookup.CUR_REG_RETAINED_NO_MARGIN_BENEFIT, additionalDetails);
				// Fix for R12 message appearing in Explain retail
			} else if (checkRetailCurMultiplePriceRule.isCurrentRetailRetained() && pricingEngineService
					.retainCurrentMultipleRetail(itemDTO, itemListWithRecPrice, maxUnitPriceDiff)) {
				retainCurrentRetail = true;
			} else if (futureRetailChangeRule.isCurrentRetailRetained()) {
				logger.debug("*** Current retail is retained for" + (itemDTO.isLir() ? " LIG:" : " non-lig:")
						+ itemDTO.getItemCode() + " as there is a future retail "
						+ itemDTO.getFutureRecRetail().toString() + " in " + itemDTO.getRecPriceEffectiveDate()
						+ " ***");
				retainCurrentRetail = true;
				itemDTO.setFutureRetailPresent(true);
				for (PRItemDTO itemOrLig : ligMembersOrNonLig) {
					itemOrLig.setFutureRetailPresent(true);
					itemOrLig.setRecPriceEffectiveDate(itemDTO.getRecPriceEffectiveDate());
				}

				List<String> additionalDetails = new ArrayList<String>();
				additionalDetails
						.add(itemDTO.getFutureRecRetail().multiple > 1 ? itemDTO.getFutureRecRetail().toString()
								: PRFormatHelper.roundToTwoDecimalDigit(itemDTO.getFutureRecRetail().price));
				additionalDetails.add(itemDTO.getRecPriceEffectiveDate());

				pricingEngineService.writeAdditionalExplainLog(itemDTO, itemListWithRecPrice,
						ExplainRetailNoteTypeLookup.CUR_REG_RETAINED_FUTURE_RETAIL_CHANGE_IN_X_WEEKS,
						additionalDetails);
			} else if (pastOverrideRule.isOverriddeRetailRetained()) {
				new OverrideService().retainOverrides(itemDTO, itemListWithRecPrice);
			}
			 if (checkPromoStartsWithinXWeeksRule.isRuleEnabled()) {

				long membersCount = ligMembersOrNonLig.stream().filter(x -> x.isPromoStartsWithinXWeeks()).count();
				
				if (membersCount > 0) {
					for (PRItemDTO item : ligMembersOrNonLig) {
						MultiplePrice regPrice = setRegPrice(item);

						if (item.getRecommendedRegPrice() != null && regPrice != null && regPrice.getUnitPrice() > 0
								&& !item.getRecommendedRegPrice().equals(regPrice) && !item.isClearanceItem() && !item.isFuturePricePresent()) {
							if (item.isPromoStartsWithinXWeeks()) {
								item.setRecommendedRegPrice(regPrice);
								item.setCurPriceRetained(true);
								List<String> addtLogDetails = new ArrayList<String>();
								addtLogDetails.add(String.valueOf(longTermPromoWeeks));
								pricingEngineService.writeAdditionalExplainLog(item, itemListWithRecPrice,
										ExplainRetailNoteTypeLookup.CUR_REG_RETAINED_SINCE_PROMO_STARTS_WITHIN_X_WEEKS,
										addtLogDetails);
							} else {
								item.setRecommendedRegPrice(regPrice);
								item.setCurPriceRetained(true);
								List<String> addtLogDetails = new ArrayList<String>();
								addtLogDetails.add(String.valueOf(longTermPromoWeeks));
								pricingEngineService.writeAdditionalExplainLog(item, itemListWithRecPrice,
										ExplainRetailNoteTypeLookup.CUR_REG_RETAINED_SINCE_LIG_PROMO_STARTS_WITHIN_X_WEEKS,
										addtLogDetails);
							}
						}
					}
				}
			} 
			int regRetailChangeInLastXWeeks = Integer
					.parseInt(PropertyManager.getProperty("REG_RETAIL_CHANGE_IN_LAST_X_WEEKS"));
			// If a regular retail was changed during the last 13 weeks, then the retail
			// will not be changed for 13 weeks
			// unless the current retail is going to break the brand, size or the cost
			// constraint.
			if (checkRegRetailChangedInLastXWeeksRule.isCurrentRetailRetained()) {
				retainCurrentRetail = true;

				if (!retainCurrentRetail) {
					pricingEngineService.clearAdditionalExplainLog(itemDTO);
				}

				logger.debug("*** Current retail is retained for" + (itemDTO.isLir() ? " LIG:" : " non-lig:") + itemDTO.getItemCode()
						+ " as regular retail was changed during within last "+ regRetailChangeInLastXWeeks +" weeks ***");
				List<String> additionalDetails = new ArrayList<>();
				additionalDetails.add(String.valueOf(regRetailChangeInLastXWeeks));
				pricingEngineService.writeAdditionalExplainLog(itemDTO, itemListWithRecPrice,
						ExplainRetailNoteTypeLookup.CUR_REG_RETAINED_RETAIL_CHANGED_WITHIN_X_WEEKS, additionalDetails);
			} 
	
		}

		
	
	
		if (itemDTO.getPricePointsFiltered() > 0) {
			List<String> additionalDetails = new ArrayList<String>();
			additionalDetails.add(String.valueOf(itemDTO.getPricePointsFiltered()));
			pricingEngineService.writeAdditionalExplainLog(itemDTO, itemListWithRecPrice,
					ExplainRetailNoteTypeLookup.PRICE_POINTS_FILTERED, additionalDetails);

			
		}

		
	}

//	/***
//	 * Retain current retail for non-LIG or LIG items without 0 prediction 
//	 * except non-LIG or any of the LIG-member which is part of price group
//	 * 
//	 * @param ligOrNonLigItem
//	 * @return
//	 */
//	private boolean isZeroPrediction(PRItemDTO ligRepOrNonLigItem, List<PRItemDTO> itemListWithRecPrice) {
//		boolean isZeroPrediction = false;
//
//		logger.debug("ligRepOrNonLigItem.getPredictedMovement():" + ligRepOrNonLigItem.getPredictedMovement()
//				+ ",ligRepOrNonLigItem.getCurRegPricePredictedMovement():" + ligRepOrNonLigItem.getCurRegPricePredictedMovement());
//		if (ligRepOrNonLigItem.getPredictedMovement() == null || ligRepOrNonLigItem.getPredictedMovement() <= 0
//				|| ligRepOrNonLigItem.getCurRegPricePredictedMovement() == null  || ligRepOrNonLigItem.getCurRegPricePredictedMovement() <= 0) {
//			isZeroPrediction = true;
//			logger.debug("isZeroPrediction:" + isZeroPrediction);
//		}
//
//		return isZeroPrediction;
//	}

	public boolean isItemPromoEndsWithinXWeeks(PRItemDTO ligRepOrNonLigItem, List<PRItemDTO> itemListWithRecPrice, String recWeekStartDate)
			throws ParseException {
		boolean isItemPromoEndsWithinXWeeks = true;

		PricingEngineService pricingEngineService = new PricingEngineService();
		List<PRItemDTO> ligMembersOrNonLig = pricingEngineService.getLigMembersOrNonLigItem(ligRepOrNonLigItem, itemListWithRecPrice);

		// Even if any one of the item in the LIG is long term promoted, then
		// don't recommended for the entire LIG
		for (PRItemDTO itemDTO : ligMembersOrNonLig) {
			// Item is promoted and has sale price,
			if (itemDTO.getFutWeekSaleInfo().getSalePrice() != null
					&& itemDTO.getFutWeekSaleInfo().getSaleStartDate() != null) {
				// if there is start date, but no end date
				if (itemDTO.getFutWeekSaleInfo().getSaleStartDate() != null && (itemDTO.getFutWeekSaleInfo().getSaleEndDate() == null
								|| itemDTO.getFutWeekSaleInfo().getSaleEndDate().trim().isEmpty())) {
					isItemPromoEndsWithinXWeeks = false;
				} else {
					// promotion ends after 6 weeks from recommendation week
					long promoDuration = DateUtil.getDateDiff(itemDTO.getFutWeekSaleInfo().getSaleEndDate(), recWeekStartDate);
					if ((promoDuration + 1) > (7 * xWeeksOfPromo)) {
						isItemPromoEndsWithinXWeeks = false;
					}
				}
			} else if (itemDTO.getRecWeekSaleInfo().getSalePrice() != null
					&& itemDTO.getRecWeekSaleInfo().getSaleStartDate() != null) {
				// if there is start date, but no end date
				if (itemDTO.getRecWeekSaleInfo().getSaleStartDate() != null && (itemDTO.getRecWeekSaleInfo().getSaleEndDate() == null
								|| itemDTO.getRecWeekSaleInfo().getSaleEndDate().isEmpty())) {
					isItemPromoEndsWithinXWeeks = false;
				} else {
					// promotion ends after 6 weeks from recommendation week
					long promoDuration = DateUtil.getDateDiff(itemDTO.getRecWeekSaleInfo().getSaleEndDate(), recWeekStartDate);
					if ((promoDuration + 1) > (7 * xWeeksOfPromo)) {
						isItemPromoEndsWithinXWeeks = false;
					}
				}
			}
			// Even one of the item is in long term promo
			if (!isItemPromoEndsWithinXWeeks) {
				break;
			}
		}

		return isItemPromoEndsWithinXWeeks;
	}


	private void retainCurrentRetail(PRItemDTO ligRepOrNonLigItem, List<PRItemDTO> itemListWithRecPrice) {
		PricingEngineService pricingEngineService = new PricingEngineService();
		List<PRItemDTO> ligMembersOrNonLig = pricingEngineService.getLigMembersOrNonLigItem(ligRepOrNonLigItem, itemListWithRecPrice);

		// lig members
		for (PRItemDTO itemDTO : ligMembersOrNonLig) {
			MultiplePrice curRegPrice = PRCommonUtil.getMultiplePrice(itemDTO.getRegMPack(), itemDTO.getRegPrice(), itemDTO.getRegMPrice());
			itemDTO.setRecommendedRegPrice(curRegPrice);
			itemDTO.setCurPriceRetained(true);
		}
	}

	public void retainCurrentRetailForTPRItems(PRItemDTO itemDTO, List<PRItemDTO> itemListWithRecPrice, PricingEngineService pricingEngineService) {
		List<PRItemDTO> tprItemList = new ArrayList<PRItemDTO>();
		List<PRItemDTO> ligMembers = pricingEngineService.getLigMembersOrNonLigItem(itemDTO, itemListWithRecPrice);
		ligMembers.forEach(prItemDTO -> {
			if (prItemDTO.getIsTPR() == 1) {
				tprItemList.add(prItemDTO);
			}
		});
		pricingEngineService.writeAdditionalExplainLog(itemDTO, tprItemList, ExplainRetailNoteTypeLookup.CUR_REG_RETAINED_SINCE_ITEM_IN_TPR,
				new ArrayList<String>());

	}

	public void checkRetailRecommendedWithinXWeeks(PRItemDTO itemDTO, List<PRItemDTO> itemListWithRecPrice,
			PRRecommendationRunHeader recommendationRunHeader, HashMap<Integer, List<PRItemSaleInfoDTO>> saleDetails,
			HashMap<Integer, List<PRItemAdInfoDTO>> adDetails,
			HashMap<Integer, HashMap<String, RetailPriceDTO>> itemZonePriceHistory,
			HashMap<String, List<RecommendationRuleMapDTO>> recommendationRuleMap,
			PricingEngineService pricingEngineService, RetailCalendarDTO curCalDTO) throws Exception, GeneralException {
		new PricingEngineService().setEffectiveDate(itemListWithRecPrice, saleDetails, adDetails,
				recommendationRunHeader, recommendationRuleMap);
		RetainCurrentRetailRule checkRegRetailChangedInLastXWeeksRule = new CheckRegRetailChangedInLastXWeeksRule(
				itemDTO, recommendationRuleMap, itemZonePriceHistory.get(itemDTO.getItemCode()),
				curCalDTO.getStartDate());
		int regRetailChangeInLastXWeeks = Integer
				.parseInt(PropertyManager.getProperty("REG_RETAIL_CHANGE_IN_LAST_X_WEEKS"));
		// If a regular retail was changed during the last 13 weeks, then the retail
		// will not be changed for 13 weeks
		// unless the current retail is going to break the brand, size or the cost
		// constraint.
		if (checkRegRetailChangedInLastXWeeksRule.isCurrentRetailRetained()) {
			logger.debug("*** Current retail is retained for" + (itemDTO.isLir() ? " LIG:" : " non-lig:")
					+ itemDTO.getItemCode() + " as regular retail was changed during within last 13 weeks ***");
			List<String> additionalDetails = new ArrayList<String>();
			additionalDetails.add(String.valueOf(regRetailChangeInLastXWeeks));
			pricingEngineService.writeAdditionalExplainLog(itemDTO, itemListWithRecPrice,
					ExplainRetailNoteTypeLookup.CUR_REG_RETAINED_RETAIL_CHANGED_WITHIN_X_WEEKS, additionalDetails);

			itemDTO.setCurPriceRetained(true);
			retainCurrentRetail(itemDTO, itemListWithRecPrice);
		}
	}

	private void checkCurrentRetailWithMapIfCurrentRetailIsRetained(PRItemDTO ligRepOrNonLigItem,
			List<PRItemDTO> itemListWithRecPrice) throws Exception {
		PricingEngineService pricingEngineService = new PricingEngineService();
		List<PRItemDTO> ligMembersOrNonLig = pricingEngineService.getLigMembersOrNonLigItem(ligRepOrNonLigItem,
				itemListWithRecPrice);

		String roundingConfig = PropertyManager.getProperty("ROUNDING_LOGIC", PRConstants.ROUND_UP);
		// lig members
		for (PRItemDTO itemDTO : ligMembersOrNonLig) {

			MultiplePrice PriceToCompare = null;
			//due to LIg same rule, the new recc Price can be different from current retail, 
			// hence in this case recommended reg price needs to be compared with MAP
			if (itemDTO.getRecommendedRegPrice() != null) {
				PriceToCompare = itemDTO.getRecommendedRegPrice();
			} else {
				PriceToCompare = PRCommonUtil.getMultiplePrice(itemDTO.getRegMPack(), itemDTO.getRegPrice(),
						itemDTO.getRegMPrice());
			}


			if (itemDTO.isCurPriceRetained()) {
				if (itemDTO.getMapRetail() > 0) {
					if (PriceToCompare != null && PriceToCompare.getUnitPrice() < itemDTO.getMapRetail()) {

						MultiplePrice mapPrice = new MultiplePrice(1, itemDTO.getMapRetail());
						double[] priceRangeArr = itemDTO.getStrategyDTO().getConstriants().getRoundingConstraint()
								.getRange(mapPrice.price, mapPrice.price, roundingConfig);
						// If rounding does not provide any out put the throw exception
						try {
							mapPrice = new MultiplePrice(1, priceRangeArr[0]);

						} catch (Exception ex) {
							throw new Exception(
									"Rounding Exception for itemCode:  " + itemDTO.getItemCode() + "is: " + ex);
						}
						itemDTO.setRecommendedRegPrice(mapPrice);
						itemDTO.setCurPriceRetained(true);
						itemDTO.setCurrentPriceBelowMAP(true);
						itemDTO.setRecommendedPricewithMap(mapPrice.getUnitPrice());

					} else {
						itemDTO.setRecommendedRegPrice(PriceToCompare);
						itemDTO.setCurPriceRetained(true);
					}
				}
			}

			
		}
	}

	/**
	 * For items having pending retails, set it as Recommended Retail clear the
	 * explain log if current retail was retained or curr retail is below the Map
	 * then make that flag as false since we are not going with new recommendation
	 * 
	 * @param itemListWithLIG
	 */
	public void setPendingRetail(List<PRItemDTO> itemListWithLIG) {
		for (PRItemDTO itemDTO : itemListWithLIG) {

			if (!itemDTO.isLir() && itemDTO.getPendingRetail() != null
					&& itemDTO.getPendingRetail().getUnitPrice() > 0) {
				itemDTO.setRecommendedRegPrice(itemDTO.getPendingRetail());
				itemDTO.setIsPendingRetailRecommended(1);

				if (itemDTO.isCurPriceRetained() || itemDTO.isCurrentPriceBelowMAP()) {
					itemDTO.setCurPriceRetained(false);
					itemDTO.setCurrentPriceBelowMAP(false);
				}
				// clear other messages from explain logs and set message for pending retail recommended
				if (itemDTO.getExplainLog() != null) {
					List<PRExplainAdditionalDetail> explainAdditionalDetail = new ArrayList<PRExplainAdditionalDetail>();
					itemDTO.getExplainLog().setExplainAdditionalDetail(new PricingEngineService().setPendingRetailsLog(explainAdditionalDetail));
				}
				new PricingEngineService().updateConflicts(itemDTO);
			}

		}
	}

	
	
	
	/**
	 * Check if the items which have future prommotion if they start within 6 weeks from recc wek
	 * @param ligMembersOrNonLig
	 * @param recWeekStartDate
	 * @return
	 * @throws ParseException
	 */
	public void  isItemPromoStartsWithinXWeeks(List<PRItemDTO> ligMembersOrNonLig,
			String recWeekStartDate) throws ParseException {
		int xWeeksFromRecWeek = Integer.parseInt(PropertyManager.getProperty("PROMO_X_WEEKS_FROM_REC_WEEK"));
		
		for (PRItemDTO itemDTO : ligMembersOrNonLig) {
			// if ongoing promotion does not end before recc week start date
			if (itemDTO.getCurSaleInfo() != null && itemDTO.getCurSaleInfo().getSaleStartDate() != null && !itemDTO.isLongTermpromotion()) {
				// if there is start date, but no end date
				if (itemDTO.getCurSaleInfo().getSaleStartDate() != null
						&& (itemDTO.getCurSaleInfo().getSaleEndDate() == null
								|| itemDTO.getCurSaleInfo().getSaleEndDate().trim().isEmpty())) {
				} else if (DateUtil
						.stringToLocalDate(itemDTO.getCurSaleInfo().getSaleEndDate(), Constants.APP_DATE_FORMAT)
						.isAfter(DateUtil.stringToLocalDate(recWeekStartDate, Constants.APP_DATE_FORMAT))) {
					long promoDuration = DateUtil.getDateDiff(itemDTO.getCurSaleInfo().getSaleStartDate(),
							recWeekStartDate);
					if ((promoDuration + 1) < (7 * xWeeksFromRecWeek)) {
						itemDTO.setPromoStartsWithinXWeeks(true);
					}
				}
			}

			if (itemDTO.getRecWeekSaleInfo().getSalePrice() != null
					&& itemDTO.getRecWeekSaleInfo().getSaleStartDate() != null && !itemDTO.isLongTermpromotion()) {
				// if there is start date, but no end date
				if (itemDTO.getRecWeekSaleInfo().getSaleStartDate() != null) {
					// promotion starts within X weeks from recommendation week
					long promoDuration = DateUtil.getDateDiff(itemDTO.getRecWeekSaleInfo().getSaleStartDate(),
							recWeekStartDate);
					if ((promoDuration + 1) < (7 * xWeeksFromRecWeek)) {
						itemDTO.setPromoStartsWithinXWeeks(true);
					}
				}
			}
			// Item is promoted and has sale price,
			if (itemDTO.getFutWeekSaleInfo().getSalePrice() != null
					&& itemDTO.getFutWeekSaleInfo().getSaleStartDate() != null && !itemDTO.isLongTermpromotion()) {
				// if there is start date, but no end date
				if (itemDTO.getFutWeekSaleInfo().getSaleStartDate() != null) {
					// promotion starts within X weeks from recommendation week
					long promoDuration = DateUtil.getDateDiff(itemDTO.getFutWeekSaleInfo().getSaleStartDate(),
							recWeekStartDate);
					if ((promoDuration + 1) < (7 * xWeeksFromRecWeek)) {
						itemDTO.setPromoStartsWithinXWeeks(true);
					}
				}
			}
		}
	}


	// long term promotion
	public void isItemOnLongTermPromotion(List<PRItemDTO> ligMembersOrNonLig) throws ParseException {
		for (PRItemDTO itemDTO : ligMembersOrNonLig) {
			// Item is promoted and has sale price,
			if (itemDTO.getCurSaleInfo() != null && itemDTO.getCurSaleInfo().getSaleStartDate() != null) {
				// if there is start date, but no end date
				if (itemDTO.getCurSaleInfo().getSaleStartDate() != null
						&& (itemDTO.getCurSaleInfo().getSaleEndDate() == null
								|| itemDTO.getCurSaleInfo().getSaleEndDate().trim().isEmpty())) {
				} else {
					long promoDuration = DateUtil.getDateDiff(itemDTO.getCurSaleInfo().getSaleEndDate(),
							itemDTO.getCurSaleInfo().getSaleStartDate());
					if ((promoDuration + 1) >= longTermPromoDays) {
						itemDTO.setLongTermpromotion(true);
					}
				}
			}
			if (itemDTO.getRecWeekSaleInfo().getSalePrice() != null
					&& itemDTO.getRecWeekSaleInfo().getSaleStartDate() != null) {
				// if there is start date, but no end date
				if (itemDTO.getRecWeekSaleInfo().getSaleStartDate() != null
						&& (itemDTO.getRecWeekSaleInfo().getSaleEndDate() == null
								|| itemDTO.getRecWeekSaleInfo().getSaleEndDate().isEmpty())) {
				} else {
					long promoDuration = DateUtil.getDateDiff(itemDTO.getRecWeekSaleInfo().getSaleEndDate(),
							itemDTO.getRecWeekSaleInfo().getSaleStartDate());
					if ((promoDuration + 1) >= longTermPromoDays) {
						itemDTO.setLongTermpromotion(true);
					}
				}

			}

			// check future week rec sale info
			if (itemDTO.getFutWeekSaleInfo().getSalePrice() != null
					&& itemDTO.getFutWeekSaleInfo().getSaleStartDate() != null && itemDTO.isPromoStartsWithinXWeeks()) {
				// if there is start date, but no end date
				if (itemDTO.getFutWeekSaleInfo().getSaleStartDate() != null
						&& (itemDTO.getFutWeekSaleInfo().getSaleEndDate() == null
								|| itemDTO.getFutWeekSaleInfo().getSaleEndDate().trim().isEmpty())) {
				} else {
					long promoDuration = DateUtil.getDateDiff(itemDTO.getFutWeekSaleInfo().getSaleEndDate(),
							itemDTO.getFutWeekSaleInfo().getSaleStartDate());
					if ((promoDuration + 1) >= longTermPromoDays) {
						itemDTO.setLongTermpromotion(true);
					}
				}
			}
		}

	}

	private MultiplePrice setRegPrice(PRItemDTO item) {
		return PRCommonUtil.getMultiplePrice(item.getRegMPack(), item.getRegPrice(), item.getRegMPrice());
	}
}
