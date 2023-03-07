package com.pristine.dataload.offermgmt.mwr.summary;

import java.sql.Connection;
import java.time.LocalDate;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

//import org.apache.log4j.Logger;

import com.pristine.dao.offermgmt.mwr.MWRSummaryDAO;
import com.pristine.dto.offermgmt.MultiplePrice;
import com.pristine.dto.offermgmt.mwr.CLPDLPPredictionDTO;
import com.pristine.dto.offermgmt.mwr.MWRItemDTO;
import com.pristine.dto.offermgmt.mwr.RecWeekKey;
import com.pristine.dto.offermgmt.mwr.RecommendationInputDTO;
import com.pristine.dto.offermgmt.mwr.ScenarioType;
import com.pristine.dto.offermgmt.mwr.SummarySplitupDTO;
import com.pristine.exception.GeneralException;
import com.pristine.service.offermgmt.ItemKey;
import com.pristine.service.offermgmt.mwr.basedata.BaseData;
import com.pristine.util.Constants;
import com.pristine.util.PropertyManager;
import com.pristine.util.offermgmt.PRCommonUtil;
import com.pristine.util.offermgmt.PRConstants;
import com.pristine.util.offermgmt.PRFormatHelper;

public class SummarySplitup {

	
	private BaseData baseData;
	private Connection conn;
	//private static Logger logger = Logger.getLogger("SummarySplitup");
	public SummarySplitup(BaseData baseData, Connection conn) {
		this.baseData = baseData;
		this.conn = conn;
	}
	
	
	
	/**
	 * 
	 * @param recommendationInputDTO
	 * @param weeklyItemDataMap
	 * @param clpDLpNumbers
	 * @return weekly summary
	 */ 
	public List<SummarySplitupDTO> getWeeklySummary(RecommendationInputDTO recommendationInputDTO, 
			HashMap<RecWeekKey, HashMap<ItemKey, MWRItemDTO>> weeklyItemDataMap, HashMap<Integer, List<CLPDLPPredictionDTO>> clpDLpNumbers){
		
		List<SummarySplitupDTO> weeklySummary = calculateWeeklySummary(recommendationInputDTO,
				weeklyItemDataMap);

		updateClpDlpNumbersWeekly(clpDLpNumbers, weeklySummary);
		
		return weeklySummary;
	}
	
	
	/**
	 * 
	 * @param recommendationInputDTO
	 * @param weeklyItemDataMap
	 * @param clpDLpNumbers
	 * @return weekly summary
	 */ 
	public List<SummarySplitupDTO> getPeriodSummary(RecommendationInputDTO recommendationInputDTO,
			HashMap<RecWeekKey, HashMap<ItemKey, MWRItemDTO>> weeklyItemDataMap,
			List<SummarySplitupDTO> weeklySummary) {

		List<SummarySplitupDTO> periodSummary = calculatePeriodSummary(recommendationInputDTO,
				weeklyItemDataMap);

		updateClpDlpNumbersPeriod(weeklySummary, periodSummary);

		return weeklySummary;
	}
	
	/**
	 * 
	 * @param recommendationInputDTO
	 * @throws GeneralException
	 */
	public List<SummarySplitupDTO> calculateWeeklySummary(RecommendationInputDTO recommendationInputDTO) throws GeneralException {

		MWRSummaryDAO summaryDAO = new MWRSummaryDAO();
		List<SummarySplitupDTO> weeklySummary = calculateWeeklySummary(recommendationInputDTO,
				baseData.getWeeklyItemDataMap());

		updateClpDlpNumbersWeekly(baseData.getClpDlpPredictions(), weeklySummary);

		summaryDAO.insertWeeklySummary(conn, weeklySummary);
		
		if (!recommendationInputDTO.getRecType().equals(PRConstants.MW_WEEK_RECOMMENDATION)) {
			List<SummarySplitupDTO> periodSummary = calculatePeriodSummary(recommendationInputDTO,
					baseData.getWeeklyItemDataMap());

			updateClpDlpNumbersPeriod(weeklySummary, periodSummary);

			summaryDAO.insertWeeklySummary(conn, periodSummary);
		}
		return weeklySummary;
	}
	
	
	/**
	 * 
	 * @param recommendationInputDTO
	 * @param weeklyItemDataMap
	 */
	private List<SummarySplitupDTO> calculateWeeklySummary(RecommendationInputDTO recommendationInputDTO,
			HashMap<RecWeekKey, HashMap<ItemKey, MWRItemDTO>> weeklyItemDataMap) {

		List<SummarySplitupDTO> weeklySummary = new ArrayList<>();

		/*LocalDate recStartWeek = LocalDate.parse(recommendationInputDTO.getStartWeek(),
				PRCommonUtil.getDateFormatter());*/

		LocalDate baseWeek = LocalDate.parse(recommendationInputDTO.getBaseWeek(),
				PRCommonUtil.getDateFormatter());
		
		for (Map.Entry<RecWeekKey, HashMap<ItemKey, MWRItemDTO>> multiWeekEntry : weeklyItemDataMap.entrySet()) {

			SummarySplitupDTO weeklySummaryDTO = new SummarySplitupDTO();

			RecWeekKey recWeekKey = multiWeekEntry.getKey();

			weeklySummaryDTO.setRunId(recommendationInputDTO.getRunId());
			weeklySummaryDTO.setCalendarId(recWeekKey.calendarId);
			weeklySummaryDTO.setCalType(Constants.CALENDAR_WEEK);
			
			boolean isCompletedWeek = false;
			LocalDate weekStartDate = LocalDate.parse(recWeekKey.weekStartDate, PRCommonUtil.getDateFormatter());

			if (weekStartDate.isBefore(baseWeek)) {
				isCompletedWeek = true;
			}
			
			double recPriceSum = 0;
			double currPriceSum = 0;
			double compPriceSum = 0;
			double compSalePrSum = 0;
			double salePriceSum = 0;
			
			for (Map.Entry<ItemKey, MWRItemDTO> itemEntry : multiWeekEntry.getValue().entrySet()) {

				MWRItemDTO mwrItemDTO = itemEntry.getValue();
				weeklySummaryDTO.setPeriodCalendarId(mwrItemDTO.getPeriodCalendarId());
				// Set recommended price as current retail
				MultiplePrice curRegPrice = PRCommonUtil.getMultiplePrice(mwrItemDTO.getRegMultiple(),
						mwrItemDTO.getRegPrice(), mwrItemDTO.getRegPrice());

				Double cost = mwrItemDTO.getCost();

				if(!mwrItemDTO.isLir()) {
					weeklySummaryDTO.setPriceChangeImpact(weeklySummaryDTO.getPriceChangeImpact()
							+ checkNullAndReturnZero(mwrItemDTO.getPriceChangeImpact()));
				}
				
				
				//Changes done by Bhargavi on 01/05/2021
				//update the MarkUp and MarkDown values for Rite Aid
				String impactFactor = PropertyManager.getProperty("IMPACT_CALCULATION_FACTOR", Constants.X_WEEKS_MOV);
				if (Constants.STORE_INVENTORY.equals(impactFactor))
				{
					if(mwrItemDTO.getPriceChangeImpact() >= 0)
					{
						if(!mwrItemDTO.isLir()) {
							weeklySummaryDTO.setMarkUp(weeklySummaryDTO.getMarkUp()
									+ checkNullAndReturnZero(mwrItemDTO.getPriceChangeImpact()));
						}
					}

					if(mwrItemDTO.getPriceChangeImpact() < 0)
					{
						if(!mwrItemDTO.isLir()) {
							weeklySummaryDTO.setMarkDown(weeklySummaryDTO.getMarkDown()
									+ checkNullAndReturnZero(mwrItemDTO.getPriceChangeImpact()));
						}
					}
				}
				//Changes-ended
	
				if (!mwrItemDTO.isLir() && PRCommonUtil.canConsiderItemForCalculation(curRegPrice, cost,
						mwrItemDTO.getFinalPricePredictionStatus())) {
					
					
					
					if(mwrItemDTO.getCompRegPrice() != null && mwrItemDTO.getCompRegPrice() > 0){
						MultiplePrice compPr = new MultiplePrice(mwrItemDTO.getCompRegMultiple(),
								mwrItemDTO.getCompRegPrice());
						compPriceSum += PRCommonUtil.getUnitPrice(compPr, true);
						recPriceSum += PRCommonUtil.getUnitPrice(mwrItemDTO.getRecommendedRegPrice(), true);
						currPriceSum += PRCommonUtil.getUnitPrice(curRegPrice, true);
					}
					
					// Completed weeks
					if (isCompletedWeek) {

						weeklySummaryDTO.setUnitsActual(weeklySummaryDTO.getUnitsActual()
								+ checkNullAndReturnZero(mwrItemDTO.getFinalPricePredictedMovement()));
						weeklySummaryDTO.setSalesActual(weeklySummaryDTO.getSalesActual()
								+ checkNullAndReturnZero(mwrItemDTO.getFinalPriceRevenue()));
						weeklySummaryDTO.setMarginActual(weeklySummaryDTO.getMarginActual()
								+ checkNullAndReturnZero(mwrItemDTO.getFinalPriceMargin()));

						weeklySummaryDTO.setCompletedWeek(true);
					} else {
						
						//Changes done by Bhargavi on 3/12/2021 and 7/19/2021 for null check
						if(checkNullAndReturnZero(mwrItemDTO.getFinalPricePredictedMovement()) >= 0)
//						if(mwrItemDTO.getFinalPricePredictedMovement() >= 0)
						{
						weeklySummaryDTO.setUnitsPredicted(weeklySummaryDTO.getUnitsPredicted()
								+ checkNullAndReturnZero(mwrItemDTO.getFinalPricePredictedMovement()));
						}
						weeklySummaryDTO.setSalesPredicted(weeklySummaryDTO.getSalesPredicted()
								+ checkNullAndReturnZero(mwrItemDTO.getFinalPriceRevenue()));
						weeklySummaryDTO.setMarginPredicted(weeklySummaryDTO.getMarginPredicted()
								+ checkNullAndReturnZero(mwrItemDTO.getFinalPriceMargin()));

						if (mwrItemDTO.getSalePrice() != null && mwrItemDTO.getSalePrice() > 0) {

							
							MultiplePrice salePr = new MultiplePrice(mwrItemDTO.getSaleMultiple(), mwrItemDTO.getSalePrice());
							
							

							if(mwrItemDTO.getCompSalePrice() != null && mwrItemDTO.getCompSalePrice() > 0){
								MultiplePrice compPr = new MultiplePrice(mwrItemDTO.getCompSaleMultiple(),
										mwrItemDTO.getCompSalePrice());
								compSalePrSum += PRCommonUtil.getUnitPrice(compPr, true);
								salePriceSum += PRCommonUtil.getUnitPrice(salePr, true);
							}else if(mwrItemDTO.getCompRegPrice() != null && mwrItemDTO.getCompRegPrice() > 0){
								MultiplePrice compPr = new MultiplePrice(mwrItemDTO.getCompRegMultiple(),
										mwrItemDTO.getCompRegPrice());
								compSalePrSum += PRCommonUtil.getUnitPrice(compPr, true);
								salePriceSum += PRCommonUtil.getUnitPrice(salePr, true);
							}
							
							weeklySummaryDTO.setCurrentUnitsTotal(weeklySummaryDTO.getCurrentUnitsTotal()
									+ checkNullAndReturnZero(mwrItemDTO.getFinalPricePredictedMovement()));
							weeklySummaryDTO.setCurrentSalesTotal(weeklySummaryDTO.getCurrentSalesTotal()
									+ checkNullAndReturnZero(mwrItemDTO.getFinalPriceRevenue()));
							weeklySummaryDTO.setCurrentMarginTotal(weeklySummaryDTO.getCurrentMarginTotal()
									+ checkNullAndReturnZero(mwrItemDTO.getFinalPriceMargin()));
							
							weeklySummaryDTO.setPromoUnits(weeklySummaryDTO.getPromoUnits()
									+ checkNullAndReturnZero(mwrItemDTO.getFinalPricePredictedMovement()));
							weeklySummaryDTO.setPromoSales(weeklySummaryDTO.getPromoSales()
									+ checkNullAndReturnZero(mwrItemDTO.getFinalPriceRevenue()));
							weeklySummaryDTO.setPromoMargin(weeklySummaryDTO.getPromoMargin()
									+ checkNullAndReturnZero(mwrItemDTO.getFinalPriceMargin()));


						} else {
							weeklySummaryDTO.setCurrentUnitsTotal(weeklySummaryDTO.getCurrentUnitsTotal()
									+ checkNullAndReturnZero(mwrItemDTO.getCurrentRegUnits()));
							weeklySummaryDTO.setCurrentSalesTotal(weeklySummaryDTO.getCurrentSalesTotal()
									+ checkNullAndReturnZero(mwrItemDTO.getCurrentRegRevenue()));
							weeklySummaryDTO.setCurrentMarginTotal(weeklySummaryDTO.getCurrentMarginTotal()
									+ checkNullAndReturnZero(mwrItemDTO.getCurrentRegMargin()));
						}

					}
				}
			}

			weeklySummaryDTO.setMarginPctActual(PRCommonUtil.getDirectMarginPCT(weeklySummaryDTO.getSalesActual(),
					weeklySummaryDTO.getMarginActual()));

			weeklySummaryDTO.setMarginPctPredicted(PRCommonUtil.getDirectMarginPCT(weeklySummaryDTO.getSalesPredicted(),
					weeklySummaryDTO.getMarginPredicted()));
			
			weeklySummaryDTO.setCurrentMarginPctTotal(PRCommonUtil.getDirectMarginPCT(weeklySummaryDTO.getCurrentSalesTotal(),
					weeklySummaryDTO.getCurrentMarginTotal()));

			weeklySummaryDTO.setPromoMarginPct(
					PRCommonUtil.getDirectMarginPCT(weeklySummaryDTO.getPromoSales(), weeklySummaryDTO.getPromoSales()));
			
			weeklySummaryDTO.setUnitsTotal(weeklySummaryDTO.getUnitsActual() + weeklySummaryDTO.getUnitsPredicted());
			weeklySummaryDTO.setSalesTotal(weeklySummaryDTO.getSalesActual() + weeklySummaryDTO.getSalesPredicted());
			weeklySummaryDTO.setMarginTotal(weeklySummaryDTO.getMarginActual() + weeklySummaryDTO.getMarginPredicted());
			weeklySummaryDTO.setMarginPctTotal(PRCommonUtil.getDirectMarginPCT(weeklySummaryDTO.getSalesTotal(),
					weeklySummaryDTO.getMarginTotal()));

			weeklySummaryDTO.setCurrentUnitsTotal(weeklySummaryDTO.getUnitsActual() + weeklySummaryDTO.getCurrentUnitsTotal());
			weeklySummaryDTO.setCurrentSalesTotal(weeklySummaryDTO.getSalesActual() + weeklySummaryDTO.getCurrentSalesTotal());
			weeklySummaryDTO.setCurrentMarginTotal(weeklySummaryDTO.getMarginActual() + weeklySummaryDTO.getCurrentMarginTotal());
			weeklySummaryDTO.setCurrentMarginPctTotal(PRCommonUtil.getDirectMarginPCT(weeklySummaryDTO.getCurrentSalesTotal(),
					weeklySummaryDTO.getCurrentMarginTotal()));
			
			weeklySummaryDTO.setEdlpPriceIndexRec(getPriceIndex(compPriceSum, recPriceSum));
			weeklySummaryDTO.setEdlpPriceIndexCurr(getPriceIndex(compPriceSum, currPriceSum));
			weeklySummaryDTO.setTotalPriceIndexRec(getPriceIndex(compPriceSum + compSalePrSum, recPriceSum + salePriceSum));
			weeklySummaryDTO.setTotalPriceIndexCurr(getPriceIndex(compPriceSum + compSalePrSum, currPriceSum + salePriceSum));
			weeklySummaryDTO.setPriceIndexPromo(getPriceIndex(compSalePrSum, salePriceSum));
			
			/*
			 * logger.debug("Edlp Price Index Rec: " +
			 * weeklySummaryDTO.getEdlpPriceIndexRec());
			 * logger.debug("Edlp Price Index Curr: " +
			 * weeklySummaryDTO.getEdlpPriceIndexCurr());
			 * logger.debug("Total Price Index Rec: " +
			 * weeklySummaryDTO.getTotalPriceIndexRec());
			 * logger.debug("Total Price Index Curr: " +
			 * weeklySummaryDTO.getTotalPriceIndexCurr());
			 * logger.debug("Total Price Index Promo: " +
			 * weeklySummaryDTO.getPriceIndexPromo());
			 * 
			 * 
			 * logger.debug("weeklySummaryDTO: " + weeklySummaryDTO.toString());
			 */
			weeklySummary.add(weeklySummaryDTO);

		}

		return weeklySummary;
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
	
	private double getPriceIndex(double comp, double base){
		double pi = 0;
		String indexCalcType =  PropertyManager.getProperty("PI_CALC_TYPE", "").trim();
		if (base > 0 && comp > 0) {
			pi = comp / base * 100;
			/*// 13th May 2016, Added reverse price index support
			if (indexCalcType.equals(Constants.PI_CALC_TYPE_REVERSE)) {
				pi = pi > 0 ? ((1 / (pi / 100)) * 100) : 0;
			}*/
			
			if (indexCalcType.equals(Constants.PI_CALC_TYPE_REVERSE)) {
				pi = base / comp * 100;
			}
		}
		
		/*
		 * logger.debug("Comp: " + comp); logger.debug("Base: " + base);
		 * logger.debug("Pi: " + pi);
		 */
		pi = Double.valueOf(PRFormatHelper.roundToTwoDecimalDigit(pi));
		return pi;
	}
	
	
	
	/**
	 * 
	 * @param recommendationInputDTO
	 * @param weeklyItemDataMap
	 */
	private List<SummarySplitupDTO> calculatePeriodSummary(RecommendationInputDTO recommendationInputDTO,
			HashMap<RecWeekKey, HashMap<ItemKey, MWRItemDTO>> weeklyItemDataMap) {

		List<SummarySplitupDTO> periodSummary = new ArrayList<>();
		
		LocalDate baseWeek = LocalDate.parse(recommendationInputDTO.getBaseWeek(),
				PRCommonUtil.getDateFormatter());
		
		HashMap<Integer, List<MWRItemDTO>> periodDataMap = new HashMap<>();
		weeklyItemDataMap.forEach((recWeekKey, itemMap) -> {
			itemMap.forEach((key, mwrItemDTO) ->{
				mwrItemDTO.setRecWeekKey(recWeekKey);
				List<MWRItemDTO> itemList = new ArrayList<>();
				if(periodDataMap.containsKey(mwrItemDTO.getPeriodCalendarId())) {
					itemList = periodDataMap.get(mwrItemDTO.getPeriodCalendarId());
				}
				itemList.add(mwrItemDTO);
				periodDataMap.put(mwrItemDTO.getPeriodCalendarId(), itemList);
			});
		});
		
		
		
		for (Map.Entry<Integer, List<MWRItemDTO>> multiWeekEntry : periodDataMap.entrySet()) {

			SummarySplitupDTO weeklySummaryDTO = new SummarySplitupDTO();

			weeklySummaryDTO.setRunId(recommendationInputDTO.getRunId());
			weeklySummaryDTO.setCalendarId(multiWeekEntry.getKey());
			weeklySummaryDTO.setCalType(Constants.CALENDAR_PERIOD);
			
			boolean isCompletedWeek = false;
			
			double recPriceSum = 0;
			double currPriceSum = 0;
			double compPriceSum = 0;
			double compSalePrSum = 0;
			double salePriceSum = 0;
			
			for (MWRItemDTO mwrItemDTO: multiWeekEntry.getValue()) {

				MultiplePrice curRegPrice = PRCommonUtil.getMultiplePrice(mwrItemDTO.getRegMultiple(),
						mwrItemDTO.getRegPrice(), mwrItemDTO.getRegPrice());

				Double cost = mwrItemDTO.getCost();

				LocalDate weekStartDate = LocalDate.parse(mwrItemDTO.getRecWeekKey().weekStartDate, PRCommonUtil.getDateFormatter());

				if (weekStartDate.isBefore(baseWeek)) {
					isCompletedWeek = true;
				}
				
				if (!mwrItemDTO.isLir() && PRCommonUtil.canConsiderItemForCalculation(curRegPrice, cost,
						mwrItemDTO.getFinalPricePredictionStatus())) {
					
					if(mwrItemDTO.getCompRegPrice() != null && mwrItemDTO.getCompRegPrice() > 0){
						MultiplePrice compPr = new MultiplePrice(mwrItemDTO.getCompRegMultiple(),
								mwrItemDTO.getCompRegPrice());
						compPriceSum += PRCommonUtil.getUnitPrice(compPr, true);
						recPriceSum += PRCommonUtil.getUnitPrice(mwrItemDTO.getFinalRecPrice(), true);
						currPriceSum += PRCommonUtil.getUnitPrice(curRegPrice, true);
					}
					
					// Completed weeks
					if (isCompletedWeek) {

						weeklySummaryDTO.setUnitsActual(weeklySummaryDTO.getUnitsActual()
								+ checkNullAndReturnZero(mwrItemDTO.getFinalPricePredictedMovement()));
						weeklySummaryDTO.setSalesActual(weeklySummaryDTO.getSalesActual()
								+ checkNullAndReturnZero(mwrItemDTO.getFinalPriceRevenue()));
						weeklySummaryDTO.setMarginActual(weeklySummaryDTO.getMarginActual()
								+ checkNullAndReturnZero(mwrItemDTO.getFinalPriceMargin()));
						
						weeklySummaryDTO.setCompletedWeek(true);
					} else {

						weeklySummaryDTO.setUnitsPredicted(weeklySummaryDTO.getUnitsPredicted()
								+ checkNullAndReturnZero(mwrItemDTO.getFinalPricePredictedMovement()));
						weeklySummaryDTO.setSalesPredicted(weeklySummaryDTO.getSalesPredicted()
								+ checkNullAndReturnZero(mwrItemDTO.getFinalPriceRevenue()));
						weeklySummaryDTO.setMarginPredicted(weeklySummaryDTO.getMarginPredicted()
								+ checkNullAndReturnZero(mwrItemDTO.getFinalPriceMargin()));

						if (mwrItemDTO.getSalePrice() != null && mwrItemDTO.getSalePrice() > 0) {

							
							MultiplePrice salePr = new MultiplePrice(mwrItemDTO.getSaleMultiple(), mwrItemDTO.getSalePrice());
							
							

							if(mwrItemDTO.getCompSalePrice() != null && mwrItemDTO.getCompSalePrice() > 0){
								MultiplePrice compPr = new MultiplePrice(mwrItemDTO.getCompSaleMultiple(),
										mwrItemDTO.getCompSalePrice());
								compSalePrSum += PRCommonUtil.getUnitPrice(compPr, true);
								salePriceSum += PRCommonUtil.getUnitPrice(salePr, true);
							}else if(mwrItemDTO.getCompRegPrice() != null && mwrItemDTO.getCompRegPrice() > 0){
								MultiplePrice compPr = new MultiplePrice(mwrItemDTO.getCompRegMultiple(),
										mwrItemDTO.getCompRegPrice());
								compSalePrSum += PRCommonUtil.getUnitPrice(compPr, true);
								salePriceSum += PRCommonUtil.getUnitPrice(salePr, true);
							}
							
							weeklySummaryDTO.setCurrentUnitsTotal(weeklySummaryDTO.getCurrentUnitsTotal()
									+ checkNullAndReturnZero(mwrItemDTO.getFinalPricePredictedMovement()));
							weeklySummaryDTO.setCurrentSalesTotal(weeklySummaryDTO.getCurrentSalesTotal()
									+ checkNullAndReturnZero(mwrItemDTO.getFinalPriceRevenue()));
							weeklySummaryDTO.setCurrentMarginTotal(weeklySummaryDTO.getCurrentMarginTotal()
									+ checkNullAndReturnZero(mwrItemDTO.getFinalPriceMargin()));
							
							weeklySummaryDTO.setPromoUnits(weeklySummaryDTO.getPromoUnits()
									+ checkNullAndReturnZero(mwrItemDTO.getFinalPricePredictedMovement()));
							weeklySummaryDTO.setPromoSales(weeklySummaryDTO.getPromoSales()
									+ checkNullAndReturnZero(mwrItemDTO.getFinalPriceRevenue()));
							weeklySummaryDTO.setPromoMargin(weeklySummaryDTO.getPromoMargin()
									+ checkNullAndReturnZero(mwrItemDTO.getFinalPriceMargin()));


						} else {

							weeklySummaryDTO.setCurrentUnitsTotal(weeklySummaryDTO.getCurrentUnitsTotal()
									+ checkNullAndReturnZero(mwrItemDTO.getCurrentRegUnits()));
							weeklySummaryDTO.setCurrentSalesTotal(weeklySummaryDTO.getCurrentSalesTotal()
									+ checkNullAndReturnZero(mwrItemDTO.getCurrentRegRevenue()));
							weeklySummaryDTO.setCurrentMarginTotal(weeklySummaryDTO.getCurrentMarginTotal()
									+ checkNullAndReturnZero(mwrItemDTO.getCurrentRegMargin()));
						}

					}
				}
			}

			weeklySummaryDTO.setMarginPctActual(PRCommonUtil.getDirectMarginPCT(weeklySummaryDTO.getSalesActual(),
					weeklySummaryDTO.getMarginActual()));

			weeklySummaryDTO.setMarginPctPredicted(PRCommonUtil.getDirectMarginPCT(weeklySummaryDTO.getSalesPredicted(),
					weeklySummaryDTO.getMarginPredicted()));
			
			weeklySummaryDTO.setCurrentMarginPctTotal(PRCommonUtil.getDirectMarginPCT(weeklySummaryDTO.getCurrentSalesTotal(),
					weeklySummaryDTO.getCurrentMarginTotal()));

			weeklySummaryDTO.setPromoMarginPct(
					PRCommonUtil.getDirectMarginPCT(weeklySummaryDTO.getPromoSales(), weeklySummaryDTO.getPromoSales()));
			
			weeklySummaryDTO.setCurrentUnitsTotal(weeklySummaryDTO.getUnitsActual() + weeklySummaryDTO.getCurrentUnitsTotal());
			weeklySummaryDTO.setCurrentSalesTotal(weeklySummaryDTO.getSalesActual() + weeklySummaryDTO.getCurrentSalesTotal());
			weeklySummaryDTO.setCurrentMarginTotal(weeklySummaryDTO.getMarginActual() + weeklySummaryDTO.getCurrentMarginTotal());
			weeklySummaryDTO.setCurrentMarginPctTotal(PRCommonUtil.getDirectMarginPCT(weeklySummaryDTO.getCurrentSalesTotal(),
					weeklySummaryDTO.getCurrentMarginTotal()));

			
			weeklySummaryDTO.setUnitsTotal(weeklySummaryDTO.getUnitsActual() + weeklySummaryDTO.getUnitsPredicted());
			weeklySummaryDTO.setSalesTotal(weeklySummaryDTO.getSalesActual() + weeklySummaryDTO.getSalesPredicted());
			weeklySummaryDTO.setMarginTotal(weeklySummaryDTO.getMarginActual() + weeklySummaryDTO.getMarginPredicted());
			weeklySummaryDTO.setMarginPctTotal(PRCommonUtil.getDirectMarginPCT(weeklySummaryDTO.getSalesTotal(),
					weeklySummaryDTO.getMarginTotal()));
			
			weeklySummaryDTO.setEdlpPriceIndexRec(getPriceIndex(compPriceSum, recPriceSum));
			weeklySummaryDTO.setEdlpPriceIndexCurr(getPriceIndex(compPriceSum, currPriceSum));
			weeklySummaryDTO.setTotalPriceIndexRec(getPriceIndex(compPriceSum + compSalePrSum, recPriceSum + salePriceSum));
			weeklySummaryDTO.setTotalPriceIndexCurr(getPriceIndex(compPriceSum + compSalePrSum, currPriceSum + salePriceSum));
			weeklySummaryDTO.setPriceIndexPromo(getPriceIndex(compSalePrSum, salePriceSum));
			
			/*
			 * logger.debug("Edlp Price Index Rec: " +
			 * weeklySummaryDTO.getEdlpPriceIndexRec());
			 * logger.debug("Edlp Price Index Curr: " +
			 * weeklySummaryDTO.getEdlpPriceIndexCurr());
			 * logger.debug("Total Price Index Rec: " +
			 * weeklySummaryDTO.getTotalPriceIndexRec());
			 * logger.debug("Total Price Index Curr: " +
			 * weeklySummaryDTO.getTotalPriceIndexCurr());
			 * logger.debug("Total Price Index Promo: " +
			 * weeklySummaryDTO.getPriceIndexPromo());
			 */
			
			
			periodSummary.add(weeklySummaryDTO);

		}

		return periodSummary;
	}
	
	
	
	/**
	 * 
	 * @param clpDlpPredictions
	 * @param weeklyNumbers
	 */
	private void updateClpDlpNumbersWeekly(HashMap<Integer, List<CLPDLPPredictionDTO>> clpDlpPredictions,
			List<SummarySplitupDTO> weeklyNumbers) {
		if(clpDlpPredictions != null) {
			weeklyNumbers.forEach(summarySplitUp -> {
				if (clpDlpPredictions.containsKey(summarySplitUp.getCalendarId())) {
					List<CLPDLPPredictionDTO> clpDlpNumbers = clpDlpPredictions.get(summarySplitUp.getCalendarId());
					clpDlpNumbers.forEach(clpDlpPredictionDTO -> {
						if(summarySplitUp.isCompletedWeek()) {
							
							summarySplitUp.setCurrentUnitsTotalClpDlp(summarySplitUp.getCurrentUnitsTotal());
							summarySplitUp.setCurrentSalesTotalClpDlp(summarySplitUp.getCurrentSalesTotal());
							summarySplitUp.setCurrentMarginTotalClpDlp(summarySplitUp.getCurrentMarginTotal());
							summarySplitUp.setCurrentMarginPctTotalClpDlp(
									PRCommonUtil.getDirectMarginPCT(summarySplitUp.getCurrentSalesTotalClpDlp(),
											summarySplitUp.getCurrentMarginTotalClpDlp()));
							
							
							summarySplitUp.setUnitsTotalClpDlp(summarySplitUp.getUnitsTotal());
							summarySplitUp.setSalesTotalClpDlp(summarySplitUp.getSalesTotal());
							summarySplitUp.setMarginTotalClpDlp(summarySplitUp.getMarginTotal());
							summarySplitUp.setMarginPctTotalClpDlp(PRCommonUtil.getDirectMarginPCT(
									summarySplitUp.getSalesTotalClpDlp(), summarySplitUp.getMarginTotalClpDlp()));
						}else {
							if (clpDlpPredictionDTO.getScenarioTypeId() == ScenarioType.TOTAL_CURRENT.getScenarioTypeId()) {
								summarySplitUp.setCurrentUnitsTotalClpDlp(clpDlpPredictionDTO.getPredictedMovement());
								summarySplitUp.setCurrentSalesTotalClpDlp(clpDlpPredictionDTO.getPredictedRevenue());
								summarySplitUp.setCurrentMarginTotalClpDlp(clpDlpPredictionDTO.getPredictedMargin());
								summarySplitUp.setCurrentMarginPctTotalClpDlp(
										PRCommonUtil.getDirectMarginPCT(summarySplitUp.getCurrentSalesTotalClpDlp(),
												summarySplitUp.getCurrentMarginTotalClpDlp()));
							} else if (clpDlpPredictionDTO.getScenarioTypeId() == ScenarioType.TOTAL_NEW.getScenarioTypeId()) {
								summarySplitUp.setUnitsTotalClpDlp(clpDlpPredictionDTO.getPredictedMovement());
								summarySplitUp.setSalesTotalClpDlp(clpDlpPredictionDTO.getPredictedRevenue());
								summarySplitUp.setMarginTotalClpDlp(clpDlpPredictionDTO.getPredictedMargin());
								summarySplitUp.setMarginPctTotalClpDlp(PRCommonUtil.getDirectMarginPCT(
										summarySplitUp.getSalesTotalClpDlp(), summarySplitUp.getMarginTotalClpDlp()));
							}
						}
					});
				}
			});
		}
	}
	
	
	/**
	 * 
	 * @param weeklySummary
	 * @param periodSummary
	 */
	private void updateClpDlpNumbersPeriod(List<SummarySplitupDTO> weeklySummary,
			List<SummarySplitupDTO> periodSummary) {

		HashMap<Integer, List<SummarySplitupDTO>> summaryByPeriod = (HashMap<Integer, List<SummarySplitupDTO>>) weeklySummary
				.stream().collect(Collectors.groupingBy(SummarySplitupDTO::getPeriodCalendarId));

		periodSummary.forEach(summarySplitup -> {
			if (summaryByPeriod.containsKey(summarySplitup.getCalendarId())) {
				if (summarySplitup.isCompletedWeek()) {

					summarySplitup.setCurrentUnitsTotalClpDlp(summarySplitup.getCurrentUnitsTotal());
					summarySplitup.setCurrentSalesTotalClpDlp(summarySplitup.getCurrentSalesTotal());
					summarySplitup.setCurrentMarginTotalClpDlp(summarySplitup.getCurrentMarginTotal());
					summarySplitup.setCurrentMarginPctTotalClpDlp(PRCommonUtil.getDirectMarginPCT(
							summarySplitup.getCurrentSalesTotalClpDlp(), summarySplitup.getCurrentMarginTotalClpDlp()));

					summarySplitup.setUnitsTotalClpDlp(summarySplitup.getUnitsTotal());
					summarySplitup.setSalesTotalClpDlp(summarySplitup.getSalesTotal());
					summarySplitup.setMarginTotalClpDlp(summarySplitup.getMarginTotal());
					summarySplitup.setMarginPctTotalClpDlp(PRCommonUtil.getDirectMarginPCT(
							summarySplitup.getSalesTotalClpDlp(), summarySplitup.getMarginTotalClpDlp()));
				} else {

					List<SummarySplitupDTO> weeklyData = summaryByPeriod.get(summarySplitup.getCalendarId());

					summarySplitup.setCurrentUnitsTotalClpDlp(
							weeklyData.stream().mapToDouble(SummarySplitupDTO::getCurrentUnitsTotalClpDlp).sum());
					summarySplitup.setCurrentSalesTotalClpDlp(
							weeklyData.stream().mapToDouble(SummarySplitupDTO::getCurrentSalesTotalClpDlp).sum());
					summarySplitup.setCurrentMarginTotalClpDlp(
							weeklyData.stream().mapToDouble(SummarySplitupDTO::getCurrentMarginTotalClpDlp).sum());
					summarySplitup.setCurrentMarginPctTotalClpDlp(PRCommonUtil.getDirectMarginPCT(
							summarySplitup.getCurrentSalesTotalClpDlp(), summarySplitup.getCurrentMarginTotalClpDlp()));

					summarySplitup.setUnitsTotalClpDlp(
							weeklyData.stream().mapToDouble(SummarySplitupDTO::getUnitsTotalClpDlp).sum());
					summarySplitup.setSalesTotalClpDlp(
							weeklyData.stream().mapToDouble(SummarySplitupDTO::getSalesTotalClpDlp).sum());
					summarySplitup.setMarginTotalClpDlp(
							weeklyData.stream().mapToDouble(SummarySplitupDTO::getMarginTotalClpDlp).sum());
					summarySplitup.setMarginPctTotalClpDlp(PRCommonUtil.getDirectMarginPCT(
							summarySplitup.getSalesTotalClpDlp(), summarySplitup.getMarginTotalClpDlp()));
					
					summarySplitup.setPriceChangeImpact(0.0);
					summarySplitup.setMarkUp(0.0);
					summarySplitup.setMarkDown(0.0);
					
					for(SummarySplitupDTO weekly : weeklyData)
					{
						if(weekly.getPriceChangeImpact()!=0)
							summarySplitup.setPriceChangeImpact(weekly.getPriceChangeImpact());
						if(weekly.getMarkUp()!=0)
							summarySplitup.setMarkUp(weekly.getMarkUp());
						if(weekly.getMarkDown()!=0)
							summarySplitup.setMarkDown(weekly.getMarkDown());
					}
				}
			}
		});
	}
	
}
