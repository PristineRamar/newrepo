package com.pristine.dataload.offermgmt.mwr.summary;

import java.sql.Connection;
import java.time.LocalDate;
import java.time.format.DateTimeFormatter;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.OptionalDouble;
import java.util.Set;
import java.util.stream.Collectors;

import org.apache.log4j.Logger;

import com.pristine.dao.offermgmt.mwr.MWRSummaryDAO;
import com.pristine.dto.offermgmt.MultiplePrice;
import com.pristine.dto.offermgmt.mwr.MWRItemDTO;
import com.pristine.dto.offermgmt.mwr.MWRSummaryDTO;
import com.pristine.dto.offermgmt.mwr.RecWeekKey;
import com.pristine.dto.offermgmt.mwr.RecommendationInputDTO;
import com.pristine.dto.offermgmt.mwr.SummarySplitupDTO;
import com.pristine.exception.GeneralException;
import com.pristine.service.offermgmt.ItemKey;
import com.pristine.service.offermgmt.mwr.basedata.BaseData;
import com.pristine.service.offermgmt.mwr.core.CoreRecommendationService;
import com.pristine.util.Constants;
import com.pristine.util.DateUtil;
import com.pristine.util.PropertyManager;
import com.pristine.util.offermgmt.PRCommonUtil;
import com.pristine.util.offermgmt.PRConstants;
import com.pristine.util.offermgmt.PRFormatHelper;

public class MultiWeekRecSummary {

	private static Logger logger = Logger.getLogger("MultiWeekRecSummary");
	DateTimeFormatter formatter = DateTimeFormatter.ofPattern(Constants.APP_DATE_FORMAT);
	
	//config for FF
	boolean checkFutureCost=Boolean.parseBoolean(PropertyManager.getProperty("FETCH_FUTURE_COST", "FALSE"));
	//config for FF
	private static final boolean checkFuturePrice = Boolean.parseBoolean(PropertyManager.getProperty("FETCH_FUTURE_PRICE", "FALSE"));
	//config for AZ to check for pending retails
	boolean checkPendingRetail=Boolean.parseBoolean(PropertyManager.getProperty("CHECK_PENDING_RETAILS", "FALSE"));
	//config for using aggregated impact of z4,16 for global zone for AZ
	boolean useAggImpactForGlobalZone=Boolean.parseBoolean(PropertyManager.getProperty("CALC_AGG_IMPACT_FOR_GLOBAL_ZONE", "FALSE"));
	
	
	/**
	 * 
	 * @param recommendationInputDTO
	 * @throws GeneralException
	 * @throws Exception 
	 */
	public void calculateSummary(Connection conn, 
			BaseData baseData, RecommendationInputDTO recommendationInputDTO)
			throws GeneralException, Exception {

		MWRSummaryDAO mwrSummaryDAO = new MWRSummaryDAO();
		
		// 11. Update weekly summary
		List<SummarySplitupDTO> weeklySummary = new SummarySplitup(baseData, conn).calculateWeeklySummary(recommendationInputDTO);
		
		if(recommendationInputDTO.getRecType().equals(PRConstants.MW_WEEK_RECOMMENDATION)) {
			// Quarter level item summary
			List<MWRItemDTO> quarterItemSummary = getMultiWeekItemSummary(recommendationInputDTO, baseData.getWeeklyItemDataMap());
			
			MWRSummaryDTO mwrSummaryDTO = getQuarterSummary(recommendationInputDTO, quarterItemSummary, weeklySummary,
					baseData.getWeeklyItemDataMap());
			
			// Save Quarter summary
			mwrSummaryDAO.insertMultiWeekSummary(conn, mwrSummaryDTO);
		} else {
			// Quarter level item summary
			List<MWRItemDTO> quarterItemSummary = getMultiWeekItemSummary(recommendationInputDTO, baseData.getWeeklyItemDataMap());
			
			// Period Level item Summary
			List<MWRItemDTO> periodItemSummary = getPeriodLevelItemSummary(recommendationInputDTO, baseData.getWeeklyItemDataMap());
			
			MWRSummaryDTO mwrSummaryDTO = getQuarterSummary(recommendationInputDTO, quarterItemSummary, weeklySummary,
					baseData.getWeeklyItemDataMap());
			
			// Save Impact Flag at Quarter level item summary
			Map<Integer, MWRItemDTO> ligMap =setImpctInclFlag(quarterItemSummary);
			
			// Save Quarter level item summary
			mwrSummaryDAO.saveItemSummaryForQuarter(conn, recommendationInputDTO, quarterItemSummary,ligMap);
			
			// Save Period level item summary
			mwrSummaryDAO.saveItemSummaryForQuarter(conn, recommendationInputDTO, periodItemSummary,null);
			
			mwrSummaryDAO.insertSecondaryZoneRecs(conn, recommendationInputDTO, quarterItemSummary);
			// Save Quarter summary
			mwrSummaryDAO.insertMultiWeekSummary(conn, mwrSummaryDTO);
		}
	}
	
	
	public MWRSummaryDTO getQuarterSummary(RecommendationInputDTO recommendationInputDTO, List<MWRItemDTO> itemSummary,
			List<SummarySplitupDTO> weeklySummary, HashMap<RecWeekKey, HashMap<ItemKey, MWRItemDTO>> weeklyItemDataMap) {

		MWRSummaryDTO mwrSummaryDTO = calculateMultiWeekLevelSummary(recommendationInputDTO, weeklyItemDataMap);

		calculateTotalMetrics(mwrSummaryDTO, weeklySummary, itemSummary);

		calculateTotalItems(itemSummary, mwrSummaryDTO);

		return mwrSummaryDTO;
	}

	/**
	 * 
	 * @param recommendationInputDTO
	 * @param weeklyItemDataMap
	 */
	private MWRSummaryDTO calculateMultiWeekLevelSummary(RecommendationInputDTO recommendationInputDTO,
			HashMap<RecWeekKey, HashMap<ItemKey, MWRItemDTO>> weeklyItemDataMap) {

		MWRSummaryDTO mwrSummaryDTO = new MWRSummaryDTO();

		mwrSummaryDTO.setRunId(recommendationInputDTO.getRunId());
		
		LocalDate recStartWeek = LocalDate.parse(recommendationInputDTO.getStartWeek(),
				PRCommonUtil.getDateFormatter());
		
		LocalDate recBaseWeek = LocalDate.parse(recommendationInputDTO.getBaseWeek(), PRCommonUtil.getDateFormatter());

		for (Map.Entry<RecWeekKey, HashMap<ItemKey, MWRItemDTO>> multiWeekEntry : weeklyItemDataMap.entrySet()) {

			RecWeekKey recWeekKey = multiWeekEntry.getKey();

			boolean isCompletedWeek = false;
			boolean isInBetweenWeek = false;
			LocalDate weekStartDate = LocalDate.parse(recWeekKey.weekStartDate, PRCommonUtil.getDateFormatter());

			if (weekStartDate.isBefore(recStartWeek)) {
				isCompletedWeek = true;
			}

			if (weekStartDate.isBefore(recStartWeek)
					&& (weekStartDate.isAfter(recBaseWeek) || weekStartDate.isEqual(recBaseWeek))) {
				isInBetweenWeek = true;
			}

			for (Map.Entry<ItemKey, MWRItemDTO> itemEntry : multiWeekEntry.getValue().entrySet()) {

				MWRItemDTO mwrItemDTO = itemEntry.getValue();

				// Set recommended price as current retail
				MultiplePrice curRegPrice = PRCommonUtil.getMultiplePrice(mwrItemDTO.getRegMultiple(),
						mwrItemDTO.getRegPrice(), mwrItemDTO.getRegPrice());

				Double cost = mwrItemDTO.getCost();

				Double finalPricePredictedMovement = null;
				if(mwrItemDTO.getFinalPricePredictedMovement() != null){
					finalPricePredictedMovement = mwrItemDTO.getFinalPricePredictedMovement() == -1 ? 0 : mwrItemDTO.getFinalPricePredictedMovement();	
				}
				 
				
				if (!mwrItemDTO.isLir() && PRCommonUtil.canConsiderItemForCalculation(curRegPrice, cost,
						mwrItemDTO.getFinalPricePredictionStatus())) {
					// Completed weeks
					if (isCompletedWeek && !isInBetweenWeek) {

						mwrSummaryDTO.setUnitsActual((mwrSummaryDTO.getUnitsActual()
								+ (Double.valueOf(PRFormatHelper.roundToTwoDecimalDigit(checkNullAndReturnZero(finalPricePredictedMovement))))));
						mwrSummaryDTO.setSalesActual(mwrSummaryDTO.getSalesActual()
								+ Double.valueOf(PRFormatHelper.roundToTwoDecimalDigit(checkNullAndReturnZero(mwrItemDTO.getFinalPriceRevenue()))));
						mwrSummaryDTO.setMarginActual((mwrSummaryDTO.getMarginActual()
								+ Double.valueOf(PRFormatHelper.roundToTwoDecimalDigit(checkNullAndReturnZero(mwrItemDTO.getFinalPriceMargin())))));

					} else if (isInBetweenWeek) { // In between weeks

						mwrSummaryDTO.setUnitsInBetWeeks(mwrSummaryDTO.getUnitsInBetWeeks()
								+ (Double.valueOf(PRFormatHelper.roundToTwoDecimalDigit(checkNullAndReturnZero(finalPricePredictedMovement)))));
						mwrSummaryDTO.setSalesInBetWeeks(mwrSummaryDTO.getSalesInBetWeeks()
								+ Double.valueOf(PRFormatHelper.roundToTwoDecimalDigit(checkNullAndReturnZero(mwrItemDTO.getFinalPriceRevenue()))));
						mwrSummaryDTO.setMarginInBetWeeks(mwrSummaryDTO.getMarginInBetWeeks()
								+ Double.valueOf(PRFormatHelper.roundToTwoDecimalDigit(checkNullAndReturnZero(mwrItemDTO.getFinalPriceMargin()))));

						if (mwrItemDTO.getSalePrice() != null && mwrItemDTO.getSalePrice() > 0) {

							mwrSummaryDTO.setCurrentUnitsTotal(mwrSummaryDTO.getCurrentUnitsTotal()
									+ (Math.round(checkNullAndReturnZero(finalPricePredictedMovement))));
							mwrSummaryDTO.setCurrentSalesTotal(mwrSummaryDTO.getCurrentSalesTotal()
									+ Double.valueOf(PRFormatHelper.roundToTwoDecimalDigit(checkNullAndReturnZero(mwrItemDTO.getFinalPriceRevenue()))));
							mwrSummaryDTO.setCurrentMarginTotal(mwrSummaryDTO.getCurrentMarginTotal()
									+ Double.valueOf(PRFormatHelper.roundToTwoDecimalDigit(checkNullAndReturnZero(mwrItemDTO.getFinalPriceMargin()))));
							
							mwrSummaryDTO.setPromoUnits(mwrSummaryDTO.getPromoUnits()
									+ Double.valueOf(PRFormatHelper.roundToTwoDecimalDigit(checkNullAndReturnZero(finalPricePredictedMovement))));
							mwrSummaryDTO.setPromoSales(mwrSummaryDTO.getPromoSales()
									+ Double.valueOf(PRFormatHelper.roundToTwoDecimalDigit(checkNullAndReturnZero(mwrItemDTO.getFinalPriceRevenue()))));
							mwrSummaryDTO.setPromoMargin(mwrSummaryDTO.getPromoMargin()
									+ Double.valueOf(PRFormatHelper.roundToTwoDecimalDigit(checkNullAndReturnZero(mwrItemDTO.getFinalPriceMargin()))));

						} else {

							mwrSummaryDTO.setCurrentUnitsTotal(mwrSummaryDTO.getCurrentUnitsTotal()
									+ Math.round(checkNullAndReturnZero(mwrItemDTO.getCurrentRegUnits())));
							mwrSummaryDTO.setCurrentSalesTotal((mwrSummaryDTO.getCurrentSalesTotal()
									+ Double.valueOf(PRFormatHelper.roundToTwoDecimalDigit(checkNullAndReturnZero(mwrItemDTO.getCurrentRegRevenue())))));
							mwrSummaryDTO.setCurrentMarginTotal(mwrSummaryDTO.getCurrentMarginTotal()
									+ Double.valueOf(PRFormatHelper.roundToTwoDecimalDigit(checkNullAndReturnZero(mwrItemDTO.getCurrentRegMargin()))));

						}

					} else { // Remaining weeks

						mwrSummaryDTO.setUnitsPredicted(mwrSummaryDTO.getUnitsPredicted()
								+ checkNullAndReturnZero(finalPricePredictedMovement));
						mwrSummaryDTO.setSalesPredicted(mwrSummaryDTO.getSalesPredicted()
								+ checkNullAndReturnZero(mwrItemDTO.getFinalPriceRevenue()));
						mwrSummaryDTO.setMarginPredicted(mwrSummaryDTO.getMarginPredicted()
								+ checkNullAndReturnZero(mwrItemDTO.getFinalPriceMargin()));

						if (mwrItemDTO.getSalePrice() != null && mwrItemDTO.getSalePrice() > 0) {

							mwrSummaryDTO.setCurrentUnitsTotal((mwrSummaryDTO.getCurrentUnitsTotal()
									+ Math.round(checkNullAndReturnZero(finalPricePredictedMovement))));
							mwrSummaryDTO.setCurrentSalesTotal(mwrSummaryDTO.getCurrentSalesTotal()
									+ Double.valueOf(PRFormatHelper.roundToTwoDecimalDigit(checkNullAndReturnZero(mwrItemDTO.getFinalPriceRevenue()))));
							mwrSummaryDTO.setCurrentMarginTotal(mwrSummaryDTO.getCurrentMarginTotal()
									+ Double.valueOf(PRFormatHelper.roundToTwoDecimalDigit(checkNullAndReturnZero(mwrItemDTO.getFinalPriceMargin()))));
							
							mwrSummaryDTO.setPromoUnits(mwrSummaryDTO.getPromoUnits()
									+ Double.valueOf(PRFormatHelper.roundToTwoDecimalDigit(checkNullAndReturnZero(finalPricePredictedMovement))));
							mwrSummaryDTO.setPromoSales(mwrSummaryDTO.getPromoSales()
									+Double.valueOf(PRFormatHelper.roundToTwoDecimalDigit(checkNullAndReturnZero(mwrItemDTO.getFinalPriceRevenue()))));
							mwrSummaryDTO.setPromoMargin((mwrSummaryDTO.getPromoMargin()
									+ Double.valueOf(PRFormatHelper.roundToTwoDecimalDigit(checkNullAndReturnZero(mwrItemDTO.getFinalPriceMargin())))));

						} else {

							mwrSummaryDTO.setCurrentUnitsTotal((mwrSummaryDTO.getCurrentUnitsTotal()
									+ Math.round(checkNullAndReturnZero(mwrItemDTO.getCurrentRegUnits()))));
							mwrSummaryDTO.setCurrentSalesTotal(mwrSummaryDTO.getCurrentSalesTotal()
									+ Double.valueOf(PRFormatHelper.roundToTwoDecimalDigit(checkNullAndReturnZero(mwrItemDTO.getCurrentRegRevenue()))));
							mwrSummaryDTO.setCurrentMarginTotal(mwrSummaryDTO.getCurrentMarginTotal()
									+ Double.valueOf(PRFormatHelper.roundToTwoDecimalDigit(checkNullAndReturnZero(mwrItemDTO.getCurrentRegMargin()))));

						}
					}
				}
			}
		}
		
		return mwrSummaryDTO;
	}
	
	
	private void calculateTotalItems(List<MWRItemDTO> mwItemSummary, MWRSummaryDTO mwrSummaryDTO) {

		int totalItems = 0;
		int totalDistinctItems = 0;
		int totalPromoItems = 0;
		int totalDistinctPromoItems = 0;
		int totalEDLPItems = 0;
		int totalDistinctEDLPItems = 0;
		
		HashMap<Integer, List<MWRItemDTO>> uniqueMap = new HashMap<Integer, List<MWRItemDTO>>();
		HashMap<Integer, List<MWRItemDTO>> itemMap = new HashMap<Integer, List<MWRItemDTO>>();
		
		for (MWRItemDTO mwrItemDTO : mwItemSummary) {

			if (mwrItemDTO.getEDLPORPROMO().equals(PRConstants.MW_EDLP_ITEM)) {

				if (mwrItemDTO.getProductLevelId() == Constants.PRODUCT_LEVEL_ID_LIG) {
					totalDistinctEDLPItems++;
				} else if (mwrItemDTO.getProductLevelId() == Constants.ITEM_LEVEL_CHILD_PRODUCT_LEVEL_ID
						&& mwrItemDTO.getRetLirId() == 0) {
					totalDistinctEDLPItems++;
				}

				if (mwrItemDTO.getProductLevelId() == Constants.ITEM_LEVEL_CHILD_PRODUCT_LEVEL_ID) {
					totalEDLPItems++;
				}

			} else if (mwrItemDTO.getEDLPORPROMO().equals(PRConstants.MW_PROMO_ITEM)) {
				if (mwrItemDTO.getProductLevelId() == Constants.PRODUCT_LEVEL_ID_LIG) {
					totalDistinctPromoItems++;
				} else if (mwrItemDTO.getProductLevelId() == Constants.ITEM_LEVEL_CHILD_PRODUCT_LEVEL_ID
						&& mwrItemDTO.getRetLirId() == 0) {
					totalDistinctPromoItems++;
				}

				if (mwrItemDTO.getProductLevelId() == Constants.ITEM_LEVEL_CHILD_PRODUCT_LEVEL_ID) {
					totalPromoItems++;
				}
			}

			if (mwrItemDTO.getProductLevelId() == Constants.PRODUCT_LEVEL_ID_LIG) {
				totalDistinctItems++;
			} else if (mwrItemDTO.getProductLevelId() == Constants.ITEM_LEVEL_CHILD_PRODUCT_LEVEL_ID
					&& mwrItemDTO.getRetLirId() == 0) {
				totalDistinctItems++;
			}

			if (mwrItemDTO.getProductLevelId() == Constants.ITEM_LEVEL_CHILD_PRODUCT_LEVEL_ID) {
				totalItems++;
			}
			
		
			// Code changes made by Karishma on 07/12/21 to make the logic of counting the
			// cost,comp and recommende items
			// similar to Audit for PROM-2185
			
			boolean isCostIncreased = checkCostIncreased(mwrItemDTO);
			if (isCostIncreased) {
				addItems(itemMap, mwrItemDTO, PRConstants.COST_INCREASED);
				addUniqueItems(uniqueMap, mwrItemDTO, PRConstants.COST_INCREASED);
			}
			
			boolean isCostDecreased = checkCostDecreased(mwrItemDTO);
			if (isCostDecreased) {
				addItems(itemMap, mwrItemDTO, PRConstants.COST_DECREASED);
				addUniqueItems(uniqueMap, mwrItemDTO, PRConstants.COST_DECREASED);
			}
			
			boolean isCompPriceIncreased = checkCompPriceIncreased(mwrItemDTO);
			if (isCompPriceIncreased) {
				addItems(itemMap, mwrItemDTO, PRConstants.COMP_INCREASED);
				addUniqueItems(uniqueMap, mwrItemDTO, PRConstants.COMP_INCREASED);
			}
			boolean isCompPriceDecreased = checkCompPriceDecreased(mwrItemDTO);
			if (isCompPriceDecreased) {
				addItems(itemMap, mwrItemDTO, PRConstants.COMP_DECREASED);
				addUniqueItems(uniqueMap, mwrItemDTO, PRConstants.COMP_DECREASED);
			}
			
			if (mwrItemDTO.getCurrentPrice() != null) {
				boolean isRetailIncreased = checkRetailIncreased(mwrItemDTO);
				if (isRetailIncreased) {
					addItems(itemMap, mwrItemDTO, PRConstants.RETAIL_INCREASED);
					addUniqueItems(uniqueMap, mwrItemDTO, PRConstants.RETAIL_INCREASED);
				}

				boolean isRetailDecreased = checkRetailDecreased(mwrItemDTO);
				if (isRetailDecreased) {
					addItems(itemMap, mwrItemDTO, PRConstants.RETAIL_DECREASED);
					addUniqueItems(uniqueMap, mwrItemDTO, PRConstants.RETAIL_DECREASED);
				}
			}

		}
		
		setUniqueItemsCount(uniqueMap, mwrSummaryDTO);
		setItemsCount(itemMap, mwrSummaryDTO);
		
		mwrSummaryDTO.setTotalItems(totalItems);
		mwrSummaryDTO.setTotalDistinctItems(totalDistinctItems);
		mwrSummaryDTO.setTotalPromoItems(totalPromoItems);
		mwrSummaryDTO.setTotalDistinctPromoItems(totalDistinctPromoItems);
		mwrSummaryDTO.setTotalEDLPItems(totalEDLPItems);
		mwrSummaryDTO.setTotalDistinctEDLPItems(totalDistinctEDLPItems);
		mwrSummaryDTO.setTotalRecommendedDistinctItems(mwrSummaryDTO.getRetailIncreasedUnq() + mwrSummaryDTO.getRetailDecreasedUnq());
		mwrSummaryDTO.setTotalRecommendedItems(mwrSummaryDTO.getRetailIncreased() + mwrSummaryDTO.getRetailDecreased());
		mwrSummaryDTO.setTotalCostChangedDistinctItems(mwrSummaryDTO.getCostIncreasedUnq() + mwrSummaryDTO.getCostDecreasedUnq());
		mwrSummaryDTO.setTotalCostChangedItems(mwrSummaryDTO.getCostIncreased() + mwrSummaryDTO.getCostDecreased());
		mwrSummaryDTO.setTotalCompChangedDistinctItems(mwrSummaryDTO.getCompPriceIncreasedUnq() + mwrSummaryDTO.getCompPriceDecreasedUnq());
		mwrSummaryDTO.setTotalCompChangedItems(mwrSummaryDTO.getCompPriceIncreased() + mwrSummaryDTO.getCompPriceDecreased());
	}
	
	/**
	 * 
	 * @param value
	 * @return 0 or value
	 */
	private double checkNullAndReturnZero(Double value){
		if(value == null){
			return 0;
		}else{
			return value;
		}
	}
	
	
	/**
	 * 
	 * @param mwrSummaryDTO
	 */
	private void calculateTotalMetrics(MWRSummaryDTO mwrSummaryDTO, List<SummarySplitupDTO> weeklySummary, List<MWRItemDTO> mwItemSummary) {

		// Total movement (actual + predicted)
		mwrSummaryDTO.setUnitsTotal(mwrSummaryDTO.getUnitsActual() + mwrSummaryDTO.getUnitsInBetWeeks()
				+ mwrSummaryDTO.getUnitsPredicted());

		// Total sales (actual + predicted)
		mwrSummaryDTO.setSalesTotal(mwrSummaryDTO.getSalesActual() + mwrSummaryDTO.getSalesInBetWeeks()
				+ mwrSummaryDTO.getSalesPredicted());

		// Total margin (actual + predicted)
		mwrSummaryDTO.setMarginTotal(mwrSummaryDTO.getMarginActual() + mwrSummaryDTO.getMarginInBetWeeks()
				+ mwrSummaryDTO.getMarginPredicted());

		// Total margin pct (actual + predicted)
		mwrSummaryDTO.setMarginPctTotal(
				PRCommonUtil.getDirectMarginPCT(mwrSummaryDTO.getSalesTotal(), mwrSummaryDTO.getMarginTotal()));

		// Margin pct actual
		mwrSummaryDTO.setMarginPctActual(
				PRCommonUtil.getDirectMarginPCT(mwrSummaryDTO.getSalesActual(), mwrSummaryDTO.getMarginActual()));

		// Margin pct in between weeks
		mwrSummaryDTO.setMarginPctInBetWeeks(PRCommonUtil.getDirectMarginPCT(mwrSummaryDTO.getSalesInBetWeeks(),
				mwrSummaryDTO.getMarginInBetWeeks()));

		// Margin pct predicted
		mwrSummaryDTO.setMarginPctPredicted(
				PRCommonUtil.getDirectMarginPCT(mwrSummaryDTO.getSalesPredicted(), mwrSummaryDTO.getMarginPredicted()));
		
		// Margin pct predicted
		mwrSummaryDTO.setCurrentMarginPctTotal(
				PRCommonUtil.getDirectMarginPCT(mwrSummaryDTO.getCurrentSalesTotal(), mwrSummaryDTO.getCurrentMarginTotal()));
		
		// Margin pct predicted
		mwrSummaryDTO.setPromoMarginPct(
				PRCommonUtil.getDirectMarginPCT(mwrSummaryDTO.getPromoSales(), mwrSummaryDTO.getPromoMargin()));
		
		
		
		/*// Total movement (actual + predicted)
		mwrSummaryDTO.setCurrentUnitsTotal(mwrSummaryDTO.getUnitsActual() + mwrSummaryDTO.getUnitsInBetWeeks()
				+ mwrSummaryDTO.getCurrentUnitsTotal());

		// Total sales (actual + predicted)
		mwrSummaryDTO.setCurrentSalesTotal(mwrSummaryDTO.getSalesActual() + mwrSummaryDTO.getSalesInBetWeeks()
				+ mwrSummaryDTO.getCurrentSalesTotal());

		// Total margin (actual + predicted)
		mwrSummaryDTO.setCurrentMarginTotal(mwrSummaryDTO.getMarginActual() + mwrSummaryDTO.getMarginInBetWeeks()
				+ mwrSummaryDTO.getCurrentMarginTotal());

		// Total margin pct (actual + predicted)
		mwrSummaryDTO.setMarginPctTotal(PRCommonUtil.getDirectMarginPCT(mwrSummaryDTO.getCurrentSalesTotal(),
				mwrSummaryDTO.getCurrentMarginTotal()));*/
		
		
		// Set price index total
		OptionalDouble optionalDouble = weeklySummary.stream().filter(a -> a.getTotalPriceIndexRec() > 0)
				.mapToDouble(a -> a.getTotalPriceIndexRec()).average();
		if(optionalDouble.isPresent()){
			double pi = optionalDouble.getAsDouble();
			mwrSummaryDTO.setPriceIndex(pi);
		}
		
		// Set price index promo
		OptionalDouble optionalDouble1 = weeklySummary.stream().filter(a -> a.getPriceIndexPromo() > 0)
				.mapToDouble(a -> a.getPriceIndexPromo()).average();
		if(optionalDouble1.isPresent()){
			double pi = optionalDouble1.getAsDouble();
			mwrSummaryDTO.setPriceIndexPromo(pi);
		}
		
		// Set price index EDLP Rec
		OptionalDouble optionalDouble2 = weeklySummary.stream().filter(a -> a.getEdlpPriceIndexRec() > 0)
				.mapToDouble(a -> a.getEdlpPriceIndexRec()).average();
		if(optionalDouble2.isPresent()){
			double pi = optionalDouble2.getAsDouble();
			mwrSummaryDTO.setPriceIndexEDLPRec(pi);
		}
		
		OptionalDouble optionalDouble3 = weeklySummary.stream().filter(a -> a.getTotalPriceIndexCurr() > 0)
				.mapToDouble(a -> a.getTotalPriceIndexCurr()).average();
		if (optionalDouble3.isPresent()) {
			double pi = optionalDouble3.getAsDouble();
			mwrSummaryDTO.setCurrentPriceIndex(pi);
		}
		
		// Set price index EDLP Curr
		OptionalDouble optionalDouble4 = weeklySummary.stream().filter(a -> a.getEdlpPriceIndexCurr() > 0)
				.mapToDouble(a -> a.getEdlpPriceIndexCurr()).average();
		if(optionalDouble4.isPresent()){
			double pi = optionalDouble4.getAsDouble();
			mwrSummaryDTO.setPriceIndexEDLPCurr(pi);
		}
		
		mwrSummaryDTO.setCurrentUnitsTotalClpDlp(
				weeklySummary.stream().mapToDouble(SummarySplitupDTO::getCurrentUnitsTotalClpDlp).sum());
		mwrSummaryDTO.setCurrentSalesTotalClpDlp(
				weeklySummary.stream().mapToDouble(SummarySplitupDTO::getCurrentSalesTotalClpDlp).sum());
		mwrSummaryDTO.setCurrentMarginTotalClpDlp(
				weeklySummary.stream().mapToDouble(SummarySplitupDTO::getCurrentMarginTotalClpDlp).sum());
		mwrSummaryDTO.setCurrentMarginPctTotalClpDlp(PRCommonUtil.getDirectMarginPCT(
				mwrSummaryDTO.getCurrentSalesTotalClpDlp(), mwrSummaryDTO.getCurrentMarginTotalClpDlp()));
		
		
		mwrSummaryDTO.setUnitsTotalClpDlp(
				weeklySummary.stream().mapToDouble(SummarySplitupDTO::getUnitsTotalClpDlp).sum());
		mwrSummaryDTO.setSalesTotalClpDlp(
				weeklySummary.stream().mapToDouble(SummarySplitupDTO::getSalesTotalClpDlp).sum());
		mwrSummaryDTO.setMarginTotalClpDlp(
				weeklySummary.stream().mapToDouble(SummarySplitupDTO::getMarginTotalClpDlp).sum());
		mwrSummaryDTO.setMarginPctTotalClpDlp(PRCommonUtil.getDirectMarginPCT(
				mwrSummaryDTO.getSalesTotalClpDlp(), mwrSummaryDTO.getMarginTotalClpDlp()));
		
		
		mwrSummaryDTO.setCurrentUnitsTotal(
				weeklySummary.stream().mapToDouble(SummarySplitupDTO::getCurrentUnitsTotal).sum());
		mwrSummaryDTO.setCurrentSalesTotal(
				weeklySummary.stream().mapToDouble(SummarySplitupDTO::getCurrentSalesTotal).sum());
		mwrSummaryDTO.setCurrentMarginTotal(
				weeklySummary.stream().mapToDouble(SummarySplitupDTO::getCurrentMarginTotal).sum());
		mwrSummaryDTO.setCurrentMarginPctTotal(PRCommonUtil.getDirectMarginPCT(
				mwrSummaryDTO.getCurrentSalesTotal(), mwrSummaryDTO.getCurrentMarginTotal()));
		
		mwrSummaryDTO.setPriceChangeImpact(0.0);
		
		for (MWRItemDTO mwrItemDTO : mwItemSummary) {
			
			if(!mwrItemDTO.isLir())
			{
				mwrSummaryDTO.setPriceChangeImpact((mwrSummaryDTO.getPriceChangeImpact()
						+ (Double.valueOf(PRFormatHelper.roundToTwoDecimalDigit(checkNullAndReturnZero(mwrItemDTO.getPriceChangeImpact()))))));
			
				if(mwrItemDTO.getPriceChangeImpact()>0)
				{
					mwrSummaryDTO.setMarkUP((mwrSummaryDTO.getMarkUP()
							+ (Double.valueOf(PRFormatHelper.roundToTwoDecimalDigit(checkNullAndReturnZero(mwrItemDTO.getPriceChangeImpact()))))));
				}
			
				if(mwrItemDTO.getPriceChangeImpact()<0)
				{
					mwrSummaryDTO.setMarkDown((mwrSummaryDTO.getMarkDown()
							+ (Double.valueOf(PRFormatHelper.roundToTwoDecimalDigit(checkNullAndReturnZero(mwrItemDTO.getPriceChangeImpact()))))));
				}
			}
			
		}
	}
	
	
	/**
	 * 
	 * @param weeklyItemDataMap
	 * @return item level multi week summary
	 */
	public List<MWRItemDTO> getMultiWeekItemSummary(RecommendationInputDTO recommendationInputDTO,
			HashMap<RecWeekKey, HashMap<ItemKey, MWRItemDTO>> weeklyItemDataMap) 
					throws CloneNotSupportedException, GeneralException {

		HashMap<ItemKey, MWRItemDTO> itemSummary = new HashMap<>();
		
		HashMap<Integer, List<MWRItemDTO>> itemMapForallWeeks= new HashMap<>();
		//Added to ssave the lig members which have future cost
		//This map will be used to set the most recent price for LIG row
		HashMap<Integer, List<MWRItemDTO>> lirIdToMembersWithFutureCostMap= new HashMap<>();
		HashMap<Integer, List<MWRItemDTO>> lirIdToMembersWithFutureWeekPriceMap= new HashMap<>();
		// Add week level entries for an item or setting the future cost,price,pending
		// retail for Quarter level
		MWRItemDTO item;
		List<MWRItemDTO> temp, membersWithFutureCost, membersWithFutureWeekPrice;
			for (Map.Entry<RecWeekKey, HashMap<ItemKey, MWRItemDTO>> WeekEntry : weeklyItemDataMap.entrySet()) {
				for (Map.Entry<ItemKey, MWRItemDTO> itemEntry : WeekEntry.getValue().entrySet()) {
				item = itemEntry.getValue();
				if(item.isLir())
					continue;
				temp = new ArrayList<>();
				if (itemMapForallWeeks.containsKey(item.getItemCode())) {
					temp = itemMapForallWeeks.get(item.getItemCode());
				}
				temp.add(item);
				itemMapForallWeeks.put(item.getItemCode(), temp);

				// store lig members
				if (item.getRetLirId() > 0) {
					if(item.getFutureCost() != null && item.getFutureCost() > 0) {
						if (lirIdToMembersWithFutureCostMap.containsKey(item.getRetLirId())) {
							membersWithFutureCost = lirIdToMembersWithFutureCostMap.get(item.getRetLirId());
						} else {
							membersWithFutureCost = new ArrayList<>();
						}
						membersWithFutureCost.add(item);
						lirIdToMembersWithFutureCostMap.put(item.getRetLirId(), membersWithFutureCost);
					}
					if(null != item.getFutureWeekPrice() && 0 < item.getFutureWeekPrice()) {
						if (lirIdToMembersWithFutureWeekPriceMap.containsKey(item.getRetLirId())) {
							membersWithFutureWeekPrice = lirIdToMembersWithFutureWeekPriceMap.get(item.getRetLirId());
						} else {
							membersWithFutureWeekPrice = new ArrayList<>();
				}
						membersWithFutureWeekPrice.add(item);
						lirIdToMembersWithFutureWeekPriceMap.put(item.getRetLirId(), membersWithFutureWeekPrice);
			}
					
		}
			}
		}
		
		for (Map.Entry<RecWeekKey, HashMap<ItemKey, MWRItemDTO>> multiWeekEntry : weeklyItemDataMap.entrySet()) {
			for (Map.Entry<ItemKey, MWRItemDTO> itemEntry : multiWeekEntry.getValue().entrySet()) {
				MWRItemDTO mwrItemDTO = (MWRItemDTO) itemEntry.getValue().clone();
				mwrItemDTO.setImmediatePromoInfo(itemEntry.getValue().getImmediatePromoInfo());
				
				Double finalPricePredictedMovement = null;
				if(mwrItemDTO.getFinalPricePredictedMovement() != null){
					finalPricePredictedMovement = mwrItemDTO.getFinalPricePredictedMovement() == -1 ? 0 : mwrItemDTO.getFinalPricePredictedMovement();	
				}
				
				
				
				if (itemSummary.containsKey(itemEntry.getKey())) {
					MWRItemDTO mwrItemDTOBase = itemSummary.get(itemEntry.getKey());
					
					// Added by Karishma on 02/06/2021.
					// If its an LIR ,set the impact without checking the isnewpriceRecommended
					// condition as any one member of LIG can have impact which may not be
					// representative item

					if (mwrItemDTOBase.isLir()) {
						if (mwrItemDTOBase.getPriceChangeImpact() == 0.0 && mwrItemDTO.getPriceChangeImpact() != 0.0) {
							mwrItemDTOBase.setPriceChangeImpact(mwrItemDTO.getPriceChangeImpact());
						}
					}

					if (!mwrItemDTOBase.isRecUpdated() && mwrItemDTO.isNewPriceRecommended()) {
						mwrItemDTOBase.setRecommendedRegPrice(mwrItemDTO.getRecommendedRegPrice());
						mwrItemDTOBase.setExplainLog(mwrItemDTO.getExplainLog());
						mwrItemDTOBase.setFinalPricePredictionStatus(mwrItemDTO.getFinalPricePredictionStatus());
						mwrItemDTOBase.setRecUpdated(true);
						mwrItemDTOBase.setRecWeekStartCalendarId(multiWeekEntry.getKey().calendarId);
						mwrItemDTOBase.setNewPriceRecommended(mwrItemDTO.isNewPriceRecommended());
						mwrItemDTOBase.setPriceChangeImpact(mwrItemDTO.getPriceChangeImpact());//Saranya
						
					}else {
						mwrItemDTOBase.setRecWeekStartCalendarId(recommendationInputDTO.getStartCalendarId());
						
					}
					
					//Added by Karishma for AI#17 to mark the quarter row for an item as recommended if its a global zone and has an impact calculated 
					if (recommendationInputDTO.isGlobalZone() && mwrItemDTO.isIsnewImpactCalculated() && mwrItemDTO.getPriceChangeImpact()!=0) {
						mwrItemDTOBase.setNewPriceRecommended(true);
						mwrItemDTOBase.setPriceChangeImpact(mwrItemDTO.getPriceChangeImpact());// Saranya
					}
					
					if(mwrItemDTO.getFuturePrice() != null) {
						mwrItemDTOBase.setFuturePrice(mwrItemDTO.getFuturePrice());
						mwrItemDTOBase.setFuturePriceEffDate(mwrItemDTO.getFuturePriceEffDate());
					}
					if (mwrItemDTO.getPostdatedPrice() != null) {
						mwrItemDTOBase.setFuturePrice(mwrItemDTO.getPostdatedPrice());
						mwrItemDTOBase.setFuturePriceEffDate(mwrItemDTO.getPostDatedPriceEffDate());
						mwrItemDTOBase.setNewPriceRecommended(true);
						mwrItemDTOBase.setRecommendedRegPrice(mwrItemDTO.getPostdatedPrice());
						mwrItemDTOBase.setPriceChangeImpact(mwrItemDTO.getPriceChangeImpact());//Saranya
						
					}

					if (mwrItemDTO.getSalePrice() != null && mwrItemDTO.getSalePrice() > 0) {

						mwrItemDTOBase.setRegUnits(checkNullAndReturnZero(mwrItemDTOBase.getRegUnits())
								+ checkNullAndReturnZero(finalPricePredictedMovement));
						mwrItemDTOBase.setRegRevenue(checkNullAndReturnZero(mwrItemDTOBase.getRegRevenue())
								+ checkNullAndReturnZero(mwrItemDTO.getFinalPriceRevenue()));
						mwrItemDTOBase.setRegMargin(checkNullAndReturnZero(mwrItemDTOBase.getRegMargin())
								+ checkNullAndReturnZero(mwrItemDTO.getFinalPriceMargin()));

						mwrItemDTOBase.setCurrentRegUnits(checkNullAndReturnZero(mwrItemDTOBase.getCurrentRegUnits())
								+ checkNullAndReturnZero(finalPricePredictedMovement));
						mwrItemDTOBase.setCurrentRegRevenue(checkNullAndReturnZero(mwrItemDTOBase.getCurrentRegRevenue())
								+ checkNullAndReturnZero(mwrItemDTO.getFinalPriceRevenue()));
						mwrItemDTOBase.setCurrentRegMargin(checkNullAndReturnZero(mwrItemDTOBase.getCurrentRegMargin())
								+ checkNullAndReturnZero(mwrItemDTO.getFinalPriceMargin()));
						
					} else {

						mwrItemDTOBase.setRegUnits(checkNullAndReturnZero(mwrItemDTOBase.getRegUnits())
								+ checkNullAndReturnZero(finalPricePredictedMovement));
						mwrItemDTOBase.setRegRevenue(checkNullAndReturnZero(mwrItemDTOBase.getRegRevenue())
								+ checkNullAndReturnZero(mwrItemDTO.getFinalPriceRevenue()));
						mwrItemDTOBase.setRegMargin(checkNullAndReturnZero(mwrItemDTOBase.getRegMargin())
								+ checkNullAndReturnZero(mwrItemDTO.getFinalPriceMargin()));
						
						mwrItemDTOBase.setCurrentRegUnits(checkNullAndReturnZero(mwrItemDTOBase.getCurrentRegUnits())
								+ checkNullAndReturnZero(mwrItemDTO.getCurrentRegUnits()));
						mwrItemDTOBase.setCurrentRegRevenue(checkNullAndReturnZero(mwrItemDTOBase.getCurrentRegRevenue())
								+ checkNullAndReturnZero(mwrItemDTO.getCurrentRegRevenue()));
						mwrItemDTOBase.setCurrentRegMargin(checkNullAndReturnZero(mwrItemDTOBase.getCurrentRegMargin())
								+ checkNullAndReturnZero(mwrItemDTO.getCurrentRegMargin()));

					}
					
				} else {

					mwrItemDTO.setEDLPORPROMO(PRConstants.MW_EDLP_ITEM);
					mwrItemDTO.setRecUpdated(mwrItemDTO.isNewPriceRecommended());
					mwrItemDTO.setCalType(Constants.CALENDAR_QUARTER);
					mwrItemDTO.setPeriodCalendarId(0);
				
					if (mwrItemDTO.getSalePrice() != null && mwrItemDTO.getSalePrice() > 0) {
		
						mwrItemDTO.setRegUnits(checkNullAndReturnZero(finalPricePredictedMovement));
						mwrItemDTO.setRegRevenue(checkNullAndReturnZero(mwrItemDTO.getFinalPriceRevenue()));
						mwrItemDTO.setRegMargin(checkNullAndReturnZero(mwrItemDTO.getFinalPriceMargin()));

						mwrItemDTO.setCurrentRegUnits(
								checkNullAndReturnZero(finalPricePredictedMovement));
						mwrItemDTO.setCurrentRegRevenue(checkNullAndReturnZero(mwrItemDTO.getFinalPriceRevenue()));
						mwrItemDTO.setCurrentRegMargin(checkNullAndReturnZero(mwrItemDTO.getFinalPriceMargin()));

					} else {

						mwrItemDTO.setRegUnits(checkNullAndReturnZero(finalPricePredictedMovement));
						mwrItemDTO.setRegRevenue(checkNullAndReturnZero(mwrItemDTO.getFinalPriceRevenue()));
						mwrItemDTO.setRegMargin(checkNullAndReturnZero(mwrItemDTO.getFinalPriceMargin()));

					}
					
					if (mwrItemDTO.getImmediatePromoInfo().getSalePrice() != null) {
						mwrItemDTO.setEDLPORPROMO(PRConstants.MW_PROMO_ITEM);
						mwrItemDTO.setSaleMultiple(mwrItemDTO.getImmediatePromoInfo().getSalePrice().multiple);
						mwrItemDTO.setSalePrice(mwrItemDTO.getImmediatePromoInfo().getSalePrice().price);
						mwrItemDTO.setPromoTypeId(
								mwrItemDTO.getImmediatePromoInfo().getPromoTypeId());
					} else {
						mwrItemDTO.setSaleMultiple(0);
						mwrItemDTO.setSalePrice(0.0);
						mwrItemDTO.setPromoTypeId(0);
					}
					
					//To show the futureCost at Quarter level
					if (mwrItemDTO.getFutureCostForQtrLevel() != null ) {
						mwrItemDTO.setFutureCost(mwrItemDTO.getFutureCostForQtrLevel());
						mwrItemDTO.setFutureCostEffDate(mwrItemDTO.getFutureCostEffDateForQtrLevel());
					}
					
					//Changes for AZ to mark z1000 recommended even if there is no new price but there is a new impact calculated  for any week 
					if (recommendationInputDTO.isGlobalZone() && !mwrItemDTO.isLir() && itemMapForallWeeks.size() > 0 && useAggImpactForGlobalZone) {
						if (itemMapForallWeeks.containsKey(mwrItemDTO.getItemCode())) {
							List<MWRItemDTO> items = itemMapForallWeeks.get(mwrItemDTO.getItemCode());
							for (MWRItemDTO mrwItem : items) {
								if (mrwItem.isIsnewImpactCalculated() && mrwItem.getPriceChangeImpact() != 0) {
								
									// mark it as recommended only if pending retail is not present
									if (mwrItemDTO.getPendingRetail() == null) {
										mwrItemDTO.setPriceChangeImpact(mrwItem.getPriceChangeImpact());
										mwrItemDTO.setNewPriceRecommended(true);
									}
									break;
								}
							}

						}

					}
				
					if (itemMapForallWeeks.containsKey(mwrItemDTO.getItemCode())) {
						List<MWRItemDTO> items = itemMapForallWeeks.get(mwrItemDTO.getItemCode());
						// Check for the most recent future cost to be set at Quarter Level for an item
						if (checkFutureCost) {
							setFutureCostAtQuarterLevel(items, mwrItemDTO);
						}
						if (checkFuturePrice) {
							setFuturePriceAtQuarterLevel(mwrItemDTO, items);
						}
						// Get the pending retail recommended row for Qtr level if pending retail is
						// present and copy the explain log as well
						if (checkPendingRetail) {
						for (MWRItemDTO mrwItem : items) {
							if (mrwItem.getIsPendingRetailRecommended() == 1) {
								mwrItemDTO.setIsPendingRetailRecommended(mrwItem.getIsPendingRetailRecommended());
								mwrItemDTO.setExplainLog(mrwItem.getExplainLog());
								// for the quarter row show the recommended retail as pending retail
								mwrItemDTO.setRecommendedRegPrice(mrwItem.getPendingRetail());
								break;
							}
						}
						}

					}
					
					  // Check for the most recent future cost to be set at Quarter Level for LIG
					if (checkFutureCost 
							&& mwrItemDTO.isLir() 
							&& lirIdToMembersWithFutureCostMap.containsKey(mwrItemDTO.getRetLirId())) {
						setFutureCostAtQuarterLevel(lirIdToMembersWithFutureCostMap.get(mwrItemDTO.getRetLirId()), mwrItemDTO);
					}
					if(checkFuturePrice
							&& mwrItemDTO.isLir()
							&& lirIdToMembersWithFutureWeekPriceMap.containsKey(mwrItemDTO.getRetLirId())) {
						setFuturePriceAtQuarterLevel(mwrItemDTO, lirIdToMembersWithFutureWeekPriceMap.get(mwrItemDTO.getRetLirId()));
					}

					// Setting the pending retail at LIG rep
					if (mwrItemDTO.isLir() && itemMapForallWeeks.containsKey(mwrItemDTO.getLigRepItemCode())
							&& checkPendingRetail) {
						List<MWRItemDTO> itemsList=itemMapForallWeeks.get(mwrItemDTO.getLigRepItemCode());
						for (MWRItemDTO mrwItem : itemsList) {
							if (mrwItem.getIsPendingRetailRecommended() == 1) {
								mwrItemDTO.setIsPendingRetailRecommended(mrwItem.getIsPendingRetailRecommended());
								mwrItemDTO.setExplainLog(mrwItem.getExplainLog());
								// for the quarter row show the recommended retail as pending retail
								mwrItemDTO.setRecommendedRegPrice(mrwItem.getPendingRetail());
								break;
							}
						}
						
					}

					
					itemSummary.put(itemEntry.getKey(), mwrItemDTO);
				}
			}
		}
		
		
		List<MWRItemDTO> multiWeekItemSummary = itemSummary.values().stream().collect(Collectors.toList());
		
		return multiWeekItemSummary;
	}
	
	/**
	 * 
	 * @param weeklyItemDataMap
	 * @return item level multi week summary
	 */
	private List<MWRItemDTO> getPeriodLevelItemSummary(RecommendationInputDTO recommendationInputDTO,
			HashMap<RecWeekKey, HashMap<ItemKey, MWRItemDTO>> weeklyItemDataMap) throws Exception{
		List<MWRItemDTO> periodLevelItemSummary = new ArrayList<>();
		HashMap<Integer, HashMap<ItemKey, List<MWRItemDTO>>> periodSummary = new HashMap<>();
		for (Map.Entry<RecWeekKey, HashMap<ItemKey, MWRItemDTO>> multiWeekEntry : weeklyItemDataMap.entrySet()) {
			for (Map.Entry<ItemKey, MWRItemDTO> itemEntry : multiWeekEntry.getValue().entrySet()) {
				MWRItemDTO mwrItemDTO = (MWRItemDTO) itemEntry.getValue().clone();
				mwrItemDTO.setRecWeekKey(multiWeekEntry.getKey());
				if (periodSummary.containsKey(mwrItemDTO.getPeriodCalendarId())) {
					HashMap<ItemKey, List<MWRItemDTO>> itemSummary = periodSummary.get(mwrItemDTO.getPeriodCalendarId());
					List<MWRItemDTO> itemList = new ArrayList<>(); 
					if(itemSummary.containsKey(itemEntry.getKey())) {
						itemList = itemSummary.get(itemEntry.getKey());
					}
					itemList.add(mwrItemDTO);
					itemSummary.put(itemEntry.getKey(), itemList);
				}else {
					HashMap<ItemKey, List<MWRItemDTO>> itemSummary = new HashMap<>();
					List<MWRItemDTO> itemList = new ArrayList<>(); 
					itemList.add(mwrItemDTO);
					itemSummary.put(itemEntry.getKey(), itemList);
					periodSummary.put(mwrItemDTO.getPeriodCalendarId(), itemSummary);	
				}
			}
		}
		
		
		for (Map.Entry<Integer, HashMap<ItemKey, List<MWRItemDTO>>> multiWeekEntry : periodSummary.entrySet()) {
			for (Map.Entry<ItemKey, List<MWRItemDTO>> itemEntry : multiWeekEntry.getValue().entrySet()) {
				List<MWRItemDTO> itemList = itemEntry.getValue();
				
				MWRItemDTO mwrItemDTOBase = null;
				for(MWRItemDTO item: itemList) {
					
					Double finalPricePredictedMovement = null;
					if(item.getFinalPricePredictedMovement() != null){
						finalPricePredictedMovement = item.getFinalPricePredictedMovement() == -1 ? 0 : item.getFinalPricePredictedMovement();	
					}
					
					if(mwrItemDTOBase == null) {
						mwrItemDTOBase = (MWRItemDTO)item.clone();
						mwrItemDTOBase.setImmediatePromoInfo(item.getImmediatePromoInfo());
						mwrItemDTOBase.setCalType(Constants.CALENDAR_PERIOD);
						mwrItemDTOBase.setEDLPORPROMO(PRConstants.MW_EDLP_ITEM);
						mwrItemDTOBase.setRecUpdated(mwrItemDTOBase.isNewPriceRecommended());
						mwrItemDTOBase.setRecWeekStartCalendarId(item.getRecWeekKey().calendarId);
						
						if (mwrItemDTOBase.getSalePrice() != null && mwrItemDTOBase.getSalePrice() > 0) {

							mwrItemDTOBase.setEDLPORPROMO(PRConstants.MW_PROMO_ITEM);
							
							mwrItemDTOBase.setRegUnits(checkNullAndReturnZero(finalPricePredictedMovement));
							mwrItemDTOBase.setRegRevenue(checkNullAndReturnZero(mwrItemDTOBase.getFinalPriceRevenue()));
							mwrItemDTOBase.setRegMargin(checkNullAndReturnZero(mwrItemDTOBase.getFinalPriceMargin()));

							mwrItemDTOBase.setCurrentRegUnits(
									checkNullAndReturnZero(finalPricePredictedMovement));
							mwrItemDTOBase.setCurrentRegRevenue(checkNullAndReturnZero(mwrItemDTOBase.getFinalPriceRevenue()));
							mwrItemDTOBase.setCurrentRegMargin(checkNullAndReturnZero(mwrItemDTOBase.getFinalPriceMargin()));

						} else {

							mwrItemDTOBase.setRegUnits(checkNullAndReturnZero(finalPricePredictedMovement));
							mwrItemDTOBase.setRegRevenue(checkNullAndReturnZero(mwrItemDTOBase.getFinalPriceRevenue()));
							mwrItemDTOBase.setRegMargin(checkNullAndReturnZero(mwrItemDTOBase.getFinalPriceMargin()));

						}
					} else {
						if (!mwrItemDTOBase.isRecUpdated() && mwrItemDTOBase.isNewPriceRecommended()) {
							mwrItemDTOBase.setRecommendedRegPrice(mwrItemDTOBase.getRecommendedRegPrice());
							mwrItemDTOBase.setExplainLog(mwrItemDTOBase.getExplainLog());
							mwrItemDTOBase.setFinalPricePredictionStatus(mwrItemDTOBase.getFinalPricePredictionStatus());
							mwrItemDTOBase.setRecUpdated(true);
						}
						
						if(item.getFuturePrice() != null) {
							mwrItemDTOBase.setFuturePrice(item.getFuturePrice());
							mwrItemDTOBase.setFuturePriceEffDate(item.getFuturePriceEffDate());
						}
						
						if (mwrItemDTOBase.getSalePrice() != null && mwrItemDTOBase.getSalePrice() > 0) {

							mwrItemDTOBase.setEDLPORPROMO(PRConstants.MW_PROMO_ITEM);
							
							mwrItemDTOBase.setRegUnits(checkNullAndReturnZero(mwrItemDTOBase.getRegUnits())
									+ checkNullAndReturnZero(finalPricePredictedMovement));
							mwrItemDTOBase.setRegRevenue(checkNullAndReturnZero(mwrItemDTOBase.getRegRevenue())
									+ checkNullAndReturnZero(item.getFinalPriceRevenue()));
							mwrItemDTOBase.setRegMargin(checkNullAndReturnZero(mwrItemDTOBase.getRegMargin())
									+ checkNullAndReturnZero(item.getFinalPriceMargin()));

							mwrItemDTOBase.setCurrentRegUnits(checkNullAndReturnZero(mwrItemDTOBase.getCurrentRegUnits())
									+ checkNullAndReturnZero(finalPricePredictedMovement));
							mwrItemDTOBase.setCurrentRegRevenue(checkNullAndReturnZero(mwrItemDTOBase.getCurrentRegRevenue())
									+ checkNullAndReturnZero(item.getFinalPriceRevenue()));
							mwrItemDTOBase.setCurrentRegMargin(checkNullAndReturnZero(mwrItemDTOBase.getCurrentRegMargin())
									+ checkNullAndReturnZero(item.getFinalPriceMargin()));
							
						} else {

							mwrItemDTOBase.setRegUnits(checkNullAndReturnZero(mwrItemDTOBase.getRegUnits())
									+ checkNullAndReturnZero(finalPricePredictedMovement));
							mwrItemDTOBase.setRegRevenue(checkNullAndReturnZero(mwrItemDTOBase.getRegRevenue())
									+ checkNullAndReturnZero(item.getFinalPriceRevenue()));
							mwrItemDTOBase.setRegMargin(checkNullAndReturnZero(mwrItemDTOBase.getRegMargin())
									+ checkNullAndReturnZero(item.getFinalPriceMargin()));
							
							mwrItemDTOBase.setCurrentRegUnits(checkNullAndReturnZero(mwrItemDTOBase.getCurrentRegUnits())
									+ checkNullAndReturnZero(item.getCurrentRegUnits()));
							mwrItemDTOBase.setCurrentRegRevenue(checkNullAndReturnZero(mwrItemDTOBase.getCurrentRegRevenue())
									+ checkNullAndReturnZero(item.getCurrentRegRevenue()));
							mwrItemDTOBase.setCurrentRegMargin(checkNullAndReturnZero(mwrItemDTOBase.getCurrentRegMargin())
									+ checkNullAndReturnZero(item.getCurrentRegMargin()));

						}
					}
				}
				
				periodLevelItemSummary.add(mwrItemDTOBase);
			}
		}
		
		return periodLevelItemSummary;
	}
	
	/**
	 * 
	 * @param mwrItemDTO
	 * @return
	 */
	private boolean checkRetailDecreased(MWRItemDTO mwrItemDTO) {
		boolean isItemToBeAdded = false;
		double recommendedUnitPrice = checkOverrideAndReturnPrice(mwrItemDTO);
		double currUnitPrice = mwrItemDTO.getCurrentPrice().getUnitPrice();
		if (mwrItemDTO.getCurrentPrice().multiple > 1) {
			currUnitPrice =mwrItemDTO.getCurrentPrice().getUnitPrice() /mwrItemDTO.getCurrentPrice().multiple ;
		}
		if (!mwrItemDTO.isLir()) {
			if (recommendedUnitPrice > 0 && currUnitPrice > 0) {
				// Retail increased
				if (currUnitPrice > recommendedUnitPrice) {
					isItemToBeAdded = true;
				}
			}
		}
		return isItemToBeAdded;
	}

	/**
	 * 
	 * @param uniqueMap
	 * @param mwrItemDTO
	 * @param auditParamId
	 */
	private void addUniqueItems(HashMap<Integer, List<MWRItemDTO>> uniqueMap, MWRItemDTO mwrItemDTO, int filterType) {
		if (uniqueMap.get(filterType) == null) {
			List<MWRItemDTO> itemList = new ArrayList<MWRItemDTO>();
			itemList.add(mwrItemDTO);
			uniqueMap.put(filterType, itemList);
		} else {
			List<MWRItemDTO> itemList = uniqueMap.get(filterType);
			boolean canBeAdded = true;
			for (MWRItemDTO itemDTO : itemList) {
				if (itemDTO.getRetLirId() == mwrItemDTO.getRetLirId()) {
					canBeAdded = false;
				}
			}
			if (canBeAdded || mwrItemDTO.getRetLirId() == 0) {
				itemList.add(mwrItemDTO);
				uniqueMap.put(filterType, itemList);
			}
		}

	}

	/**
	 * 
	 * @param itemMap
	 * @param mwrItemDTO
	 * @param auditParamId
	 */
	private void addItems(HashMap<Integer, List<MWRItemDTO>> itemMap, MWRItemDTO mwrItemDTO, int filterType) {
		if (itemMap.get(filterType) == null) {
			List<MWRItemDTO> itemList = new ArrayList<MWRItemDTO>();
			itemList.add(mwrItemDTO);
			itemMap.put(filterType, itemList);
		} else {
			List<MWRItemDTO> itemList = itemMap.get(filterType);
			itemList.add(mwrItemDTO);
			itemMap.put(filterType, itemList);
		}

	}

	/**
	 * 
	 * @param mwrItemDTO
	 * @return
	 */
	private boolean checkRetailIncreased(MWRItemDTO mwrItemDTO) {
		boolean isItemToBeAdded = false;
		double recommendedUnitPrice = checkOverrideAndReturnPrice(mwrItemDTO);
		
		double currUnitPrice = mwrItemDTO.getCurrentPrice().getUnitPrice();
		if (mwrItemDTO.getCurrentPrice().multiple > 1) {
			currUnitPrice = mwrItemDTO.getCurrentPrice().getUnitPrice() / mwrItemDTO.getCurrentPrice().multiple;
		}
		if (!mwrItemDTO.isLir()) {
			if (recommendedUnitPrice > 0 && currUnitPrice > 0) {
				// Retail increased
				if (currUnitPrice < recommendedUnitPrice) {
					isItemToBeAdded = true;
				}
			}
		}
		return isItemToBeAdded;
	}

	/**
	 * 
	 * @param mwrItemDTO
	 * @return
	 */
	private double checkOverrideAndReturnPrice(MWRItemDTO mwrItemDTO) {
		double recommendedUnitPrice = PRCommonUtil.getUnitPrice(mwrItemDTO.getRecommendedRegPrice(), true);
		if (mwrItemDTO.getOverrideRegPrice() != null) {
			double overridePrice = 0;
			if (mwrItemDTO.getOverrideRegPrice().multiple > 1) {
				overridePrice = mwrItemDTO.getOverrideRegPrice().price / mwrItemDTO.getOverrideRegPrice().multiple;
			} else {
				overridePrice = mwrItemDTO.getOverrideRegPrice().price;
			}
			recommendedUnitPrice = overridePrice;
		}

		return recommendedUnitPrice;
	}

	/**
	 * 
	 * @param itemMap
	 * @param mwrSummaryDTO
	 */
	private void setItemsCount(HashMap<Integer, List<MWRItemDTO>> itemMap, MWRSummaryDTO mwrSummaryDTO) {
		for (Map.Entry<Integer, List<MWRItemDTO>> entry : itemMap.entrySet()) {
			int key = entry.getKey();
			if (key == PRConstants.RETAIL_DECREASED) {
				mwrSummaryDTO.setRetailDecreased(entry.getValue().size());
			} else if (key == PRConstants.RETAIL_INCREASED) {
				mwrSummaryDTO.setRetailIncreased(entry.getValue().size());
			} else if (key == PRConstants.COST_INCREASED) {
				mwrSummaryDTO.setCostIncreased(entry.getValue().size());
			} else if (key == PRConstants.COST_DECREASED) {
				mwrSummaryDTO.setCostDecreased(entry.getValue().size());
			} else if (key == PRConstants.COMP_INCREASED) {
				mwrSummaryDTO.setCompPriceIncreased(entry.getValue().size());
			} else if (key == PRConstants.COMP_DECREASED) {
				mwrSummaryDTO.setCompPriceDecreased(entry.getValue().size());
			}
		}
	}

	/**
	 * 
	 * @param uniqueMap
	 * @param mwrSummaryDTO
	 */
	private void setUniqueItemsCount(HashMap<Integer, List<MWRItemDTO>> uniqueMap, MWRSummaryDTO mwrSummaryDTO) {

		for (Map.Entry<Integer, List<MWRItemDTO>> entry : uniqueMap.entrySet()) {
			int key = entry.getKey();
			if (key == PRConstants.RETAIL_DECREASED) {
				mwrSummaryDTO.setRetailDecreasedUnq(entry.getValue().size());
			} else if (key == PRConstants.RETAIL_INCREASED) {
				mwrSummaryDTO.setRetailIncreasedUnq(entry.getValue().size());
			} else if (key == PRConstants.COST_INCREASED) {
				mwrSummaryDTO.setCostIncreasedUnq(entry.getValue().size());
			} else if (key == PRConstants.COST_DECREASED) {
				mwrSummaryDTO.setCostDecreasedUnq(entry.getValue().size());
			} else if (key == PRConstants.COMP_INCREASED) {
				mwrSummaryDTO.setCompPriceIncreasedUnq(entry.getValue().size());
			} else if (key == PRConstants.COMP_DECREASED) {
				mwrSummaryDTO.setCompPriceDecreasedUnq(entry.getValue().size());
			}
		}
	}

	/**
	 * 
	 * @param mwrItemDTO
	 * @return
	 */
	private boolean checkCostIncreased(MWRItemDTO mwrItemDTO) {

		if (!mwrItemDTO.isLir() && mwrItemDTO.getCostChangeIndicator() > 0) {
			return true;
		} else {

			return false;
		}
	}

	/**
	 * 
	 * @param mwrItemDTO
	 * @return
	 */
	private boolean checkCostDecreased(MWRItemDTO mwrItemDTO) {

		if (!mwrItemDTO.isLir() && mwrItemDTO.getCostChangeIndicator() < 0) {
			return true;
		} else {

			return false;
		}
	}

	/**
	 * 
	 * @param mwrItemDTO
	 * @return
	 */
	private boolean checkCompPriceIncreased(MWRItemDTO mwrItemDTO) {

		if (!mwrItemDTO.isLir() && mwrItemDTO.getCompPriceChangeIndicator() > 0) {

			return true;
		} else {

			return false;
		}
	}

	/**
	 * 
	 * @param mwrItemDTO
	 * @return
	 */
	private boolean checkCompPriceDecreased(MWRItemDTO mwrItemDTO) {

		if (!mwrItemDTO.isLir() && mwrItemDTO.getCompPriceChangeIndicator() < 0) {

			return true;
		} else {

			return false;
		}
	}
	

	/*
	 * 
	 */
	public void setFutureCostAtQuarterLevel(List<MWRItemDTO> items, MWRItemDTO mwrItemDTO) {

		try {
			Map<String, List<MWRItemDTO>> costMap = items.stream()
					.filter(a -> a.getFutureCostEffDate() != null && a.getFutureCostEffDate() != "")
					.collect(Collectors.groupingBy(MWRItemDTO::getFutureCostEffDate));
			
			if (costMap != null && costMap.size() > 0) {
				Set<String> futureCosEffDate = costMap.keySet();
				
				boolean isFirst = true;
				String latestDate = "";
				for (String effdate : futureCosEffDate) {
					if (isFirst) {
						latestDate = effdate;
						isFirst = false;
			} else {
						try {
							latestDate = DateUtil.getRecentDate(latestDate, effdate);
						} catch (GeneralException e) {
							e.printStackTrace();
			}
		}
				}
				if (latestDate != "") {
					mwrItemDTO.setFutureCost(costMap.get(latestDate).get(0).getFutureCost());
					mwrItemDTO.setFutureCostEffDate(costMap.get(latestDate).get(0).getFutureCostEffDate());
				}
			}
		} catch (Exception e) {
			logger.info("Exception in setFutureCostAtQuarterLevel for item:  " + mwrItemDTO.getItemCode() + ":: " + e);
		}
	}

	/**
	 * Sets the most recent future price from among items for the mwrItemDTO.
	 * @param mwrItemDTO
	 * @param items
	 * @throws GeneralException 
	 */
	private void setFuturePriceAtQuarterLevel(MWRItemDTO mwrItemDTO, List<MWRItemDTO> items) throws GeneralException {
		List<MWRItemDTO> itemsWithValidFutureWeekPriceEffDate = items.stream()
				.filter(item -> null!=item.getFutureWeekPriceEffDate() 
				&& ! item.getFutureWeekPriceEffDate().isEmpty())
				.collect(Collectors.toList());
		if (itemsWithValidFutureWeekPriceEffDate.isEmpty())
			return;
		try {
			MWRItemDTO mostRecent = itemsWithValidFutureWeekPriceEffDate.get(0); // Stores the item that has the most recent future week price effective date.
			String currentItemFutureWeekPriceEffDate, recentFutureWeekPriceEffDate;
			for (MWRItemDTO item : itemsWithValidFutureWeekPriceEffDate) {
				currentItemFutureWeekPriceEffDate = item.getFutureWeekPriceEffDate();
				recentFutureWeekPriceEffDate = DateUtil.getRecentDate(mostRecent.getFutureWeekPriceEffDate(),
						currentItemFutureWeekPriceEffDate);
				mostRecent = currentItemFutureWeekPriceEffDate.equalsIgnoreCase(recentFutureWeekPriceEffDate) ? item : mostRecent;
			}
			mwrItemDTO.setFutureWeekPrice(mostRecent.getFutureWeekPrice());
			mwrItemDTO.setFutureWeekPriceEffDate(mostRecent.getFutureWeekPriceEffDate());
		} catch (GeneralException ge) {
			logger.error("Error while trying to get the recent date!", ge);
			throw ge;
		}
	}

	private Map<Integer, MWRItemDTO> setImpctInclFlag(List<MWRItemDTO> quarterItemSummary) {
		Map<Integer, MWRItemDTO> ligMap = new HashMap<>();
		Map<Integer, List<MWRItemDTO>> ligMembersMap = new HashMap<>();

		for (MWRItemDTO mwrItemDTO : quarterItemSummary) {
			if (mwrItemDTO.isLir()) {
				ligMap.put(mwrItemDTO.getRetLirId(), mwrItemDTO);
			} else if (mwrItemDTO.getRetLirId() > 0) {
				CoreRecommendationService.setImpactField(mwrItemDTO);
				List<MWRItemDTO> tem = new ArrayList<>();
				if (ligMembersMap.containsKey(mwrItemDTO.getRetLirId())) {
					tem = ligMembersMap.get(mwrItemDTO.getRetLirId());
				}
				tem.add(mwrItemDTO);
				ligMembersMap.put(mwrItemDTO.getRetLirId(), tem);
			} else {
				CoreRecommendationService.setImpactField(mwrItemDTO);
			}
		}

		CoreRecommendationService.setImpctInclFlagForLIG(ligMembersMap, ligMap);

		return ligMap;
	}
	
	
	/*
	 * private void setFuturePriceForLIG(MWRItemDTO mwrItemDTO, List<MWRItemDTO>
	 * items) throws GeneralException { List<MWRItemDTO> itemWithValidaFtPrice =
	 * items.stream() .filter(item -> item.isFuturePricePresent() &&
	 * item.getFutureUnitPrice()!=0 && ! item.getFuturePriceDate().isEmpty())
	 * .collect(Collectors.toList());
	 * 
	 * if(itemWithValidaFtPrice.isEmpty()) return; try { MWRItemDTO mostRecent =
	 * itemWithValidaFtPrice.get(0); String currentItemFutureWeekPriceEffDate,
	 * recentFutureWeekPriceEffDate; for ( MWRItemDTO item : itemWithValidaFtPrice )
	 * { currentItemFutureWeekPriceEffDate = item.getFuturePriceDate();
	 * recentFutureWeekPriceEffDate =
	 * DateUtil.getRecentDate(mostRecent.getFuturePriceDate(),
	 * currentItemFutureWeekPriceEffDate); mostRecent =
	 * currentItemFutureWeekPriceEffDate.equalsIgnoreCase(
	 * recentFutureWeekPriceEffDate) ? item : mostRecent; }
	 * mwrItemDTO.setFutureWeekPriceEffDate(mostRecent.getFuturePriceDate());
	 * mwrItemDTO.setFutureWeekPrice(mostRecent.getFutureUnitPrice());
	 * 
	 * } catch (GeneralException ge) { logger.
	 * error("setFuturePriceForLIG -Error while trying to get the recent date!",
	 * ge); throw ge; } }
	 */
}
