package com.pristine.dao.offermgmt.mwr;

import java.sql.Clob;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Types;
import java.time.LocalDate;
import java.time.format.DateTimeFormatter;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.stream.Collectors;

import org.apache.log4j.Logger;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.pristine.dto.LigFlagsDTO;
import com.pristine.dto.offermgmt.MultiplePrice;
import com.pristine.dto.offermgmt.PRExplainLog;
import com.pristine.dto.offermgmt.PRItemSaleInfoDTO;
import com.pristine.dto.offermgmt.mwr.MWRItemDTO;
import com.pristine.dto.offermgmt.mwr.RecWeekKey;
import com.pristine.dto.offermgmt.mwr.RecommendationInputDTO;
import com.pristine.dto.offermgmt.prediction.PricePointDTO;
import com.pristine.exception.GeneralException;
import com.pristine.service.offermgmt.ItemKey;
import com.pristine.util.Constants;
import com.pristine.util.PristineDBUtil;
import com.pristine.util.PropertyManager;
import com.pristine.util.offermgmt.PRCommonUtil;

public class WeeklyRecDAO {

	private static Logger logger = Logger.getLogger("WeeklyRecDAO");
	/*	public WeeklyRecDAO(BaseData baseData){

		this.baseData = baseData;
	}

	public WeeklyRecDAO(){

	}

	public void saveWeeklyRecommendation(Connection conn, RecommendationInputDTO recommendationInputDTO)
			throws JsonProcessingException, GeneralException {

		saveRecommendationDetails(conn, recommendationInputDTO, baseData.getWeeklyItemDataMap(),
				Constants.CALENDAR_WEEK);

	}*/


	private static final String INSERT_PR_WEEKLY_REC_ITEM = "INSERT INTO PR_WEEKLY_REC_ITEM "
			+ " (PR_REC_ID, RUN_ID, WEEK_CALENDAR_ID, PRODUCT_LEVEL_ID, PRODUCT_ID, RET_LIR_ID, "
			+ " STRATEGY_ID, ITEM_SIZE, UOM_ID, PRICE_CHECK_LIST_ID, LIG_REP_ITEM_CODE, REG_MULTIPLE, "
			+ " REG_PRICE, SALE_MULTIPLE, SALE_PRICE, PROMO_TYPE_ID, AD_PAGE_NO, AD_BLOCK_NO, DISPLAY_TYPE_ID, "
			+ " SALE_START_DATE, SALE_END_DATE, LIST_COST, DEAL_COST, REC_REG_MULTIPLE, REC_REG_PRICE, "
			+ " PRED_MOV_FINAL_PRICE, PRED_STATUS_FINAL_PRICE, EXPLAIN_LOG, COST_CHG_IND, COMP_PRICE_CHG_IND, "
			+ " COMP_STR_ID, COMP_REG_MULTIPLE, COMP_REG_PRICE, COMP_SALE_MULTIPLE, COMP_SALE_PRICE, "
			+ " COMP_PRICE_CHECK_DATE, REVENUE_FINAL_PRICE, MARGIN_FINAL_PRICE, CURR_UNITS, CURR_REVENUE, CURR_MARGIN, "
			+ " CONFLICT, NEWPRICE_RECOMMENDED, CURR_REG_EFF_DATE, COST_EFF_DATE,"
			+ " IS_ERROR_REC, ERROR_REC_CODES, PERIOD_CALENDAR_ID, IS_TPR, IS_PRE_PRICED, IS_LOCK_PRICED, PREV_COST, "
			+ " PREV_COMP_RETAIL, FUTURE_RETAIL_MULTIPLE, FUTURE_RETAIL, "
			+ " FUTURE_RETAIL_EFF_DATE, FUTURE_COST, FUTURE_COST_EFF_DATE, VENDOR_ID,"
			+ " COMP_1_STR_ID, COMP_2_STR_ID, COMP_3_STR_ID, COMP_4_STR_ID, COMP_5_STR_ID, "
			+ " COMP_1_RETAIL_MUL, COMP_1_RETAIL, COMP_2_RETAIL_MUL, COMP_2_RETAIL, COMP_3_RETAIL_MUL, "
			+ " COMP_3_RETAIL, COMP_4_RETAIL_MUL, COMP_4_RETAIL, COMP_5_RETAIL_MUL, COMP_5_RETAIL, CORE_RETAIL, "
			+ " CWAC_CORE_COST, PRC_CHANGE_IMPACT, ORIGINAL_COST, IS_CHILD_LOC_REC,MAP_RETAIL, IS_HOLD,IS_IMPACT_CALCULATED, "
			+ "IS_PENDING_RETAIL_RECOMMENDED, FUTURE_WEEK_PRICE, FUTURE_WEEK_PRICE_EFF_DATE,IS_IMPACT_INCL_IN_SMRY_CALC ) "
			+ " VALUES ( PR_MULTI_REC_ID_SEQ.NEXTVAL, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, "
			+ "TO_DATE(?, '" + Constants.DB_DATE_FORMAT + "'), TO_DATE(?, '" + Constants.DB_DATE_FORMAT + "'), ?, ?, ?, ?, ?, "
			+ "?, ?, ?, ?, ?, ?, ?, ?, ?, TO_DATE(?, '" + Constants.DB_DATE_FORMAT + "'), ?, ?, ?, ?, ?, ?, ?, "
			+ "TO_DATE(?, '" + Constants.DB_DATE_FORMAT + "'), TO_DATE(?, '" + Constants.DB_DATE_FORMAT + "'), ?, ?, ?, ?, ?, "
			+ "?, ?, ?, ?, ?, TO_DATE(?, '" + Constants.DB_DATE_FORMAT + "') , ?, TO_DATE(?, '" + Constants.DB_DATE_FORMAT + "'), ?, "
			+ " ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, TO_DATE(?, '" + Constants.DB_DATE_FORMAT + "'),? )";

	boolean populateExplainLogForUpdateRec = Boolean.parseBoolean(PropertyManager.getProperty("POPULATE_EXPLAIN_LOG_FOR_UPDATE_RECCS", "FALSE"));


	public void saveRecommendationDetails(Connection conn, RecommendationInputDTO recommendationInputDTO,
			HashMap<RecWeekKey, HashMap<ItemKey, MWRItemDTO>> weeklyItemDataMap, String calType) throws GeneralException, JsonProcessingException {

		PreparedStatement statement = null;
		ObjectMapper mapper = new ObjectMapper();
		/** Added configuration as this is applicable only for FF **/
		boolean markOnHoldForNewRecommendation = Boolean
				.parseBoolean(PropertyManager.getProperty("MARK_ON_HOLD_FOR_NEW_RECC", "FALSE"));
		DateTimeFormatter formatter = DateTimeFormatter.ofPattern(Constants.APP_DATE_FORMAT);
		LocalDate recStartWeek = LocalDate.parse(recommendationInputDTO.getStartWeek(), formatter);

		try {
			int itemsInBatch = 0;
			statement = conn.prepareStatement(INSERT_PR_WEEKLY_REC_ITEM);
			for (Map.Entry<RecWeekKey, HashMap<ItemKey, MWRItemDTO>> weekEntry : weeklyItemDataMap.entrySet()) {
				for (Map.Entry<ItemKey, MWRItemDTO> itemEntry : weekEntry.getValue().entrySet()) {
					int colCount = 0;
					MWRItemDTO mwrItemDTO = itemEntry.getValue();
					
					LocalDate weekStart = LocalDate.parse(weekEntry.getKey().weekStartDate , formatter);

					// Store the future cost/futurePrice for the week after recommendation week
					boolean futurePricePresent = false, futureCostPresent = false;
					try {
						if (mwrItemDTO.getFutureWeekPrice() != null && mwrItemDTO.getFutureWeekPrice() > 0
								&& mwrItemDTO.getFutureWeekPriceEffDate() != null && LocalDate
										.parse(mwrItemDTO.getFutureWeekPriceEffDate(), formatter).isAfter(recStartWeek)
								&& weekStart.isAfter(recStartWeek)) {
							futurePricePresent = true;
						}
					} catch (Exception ex) {
						logger.error(
								"saveRecommendationDetails()-Exception for item while setting futurePrice at Week level:"
										+ mwrItemDTO.CustomtoString1());
					}
					try {
						if (mwrItemDTO.getFutureCost() != null && mwrItemDTO.getFutureCost() > 0
								&& mwrItemDTO.getFutureCostEffDate() != null
								&& LocalDate.parse(mwrItemDTO.getFutureCostEffDate(), formatter).isAfter(recStartWeek)
								&& weekStart.isAfter(recStartWeek)) {
							futureCostPresent = true;
						}
					} catch (Exception ex) {
						logger.error(
								"saveRecommendationDetails()-Exception for item while setting futureCost at Week level:"
										+ mwrItemDTO.CustomtoString1());
					}
					statement.setLong(++colCount, recommendationInputDTO.getRunId());
					PristineDBUtil.setInt(++colCount, weekEntry.getKey().calendarId, statement);
					PristineDBUtil.setInt(++colCount, mwrItemDTO.getProductLevelId(), statement);
					PristineDBUtil.setInt(++colCount, mwrItemDTO.getProductId(), statement);
					PristineDBUtil.setInt(++colCount, mwrItemDTO.getRetLirId(), statement);
					statement.setLong(++colCount, mwrItemDTO.getStrategyId());
					PristineDBUtil.setDouble(++colCount, mwrItemDTO.getItemSize(), statement);
					PristineDBUtil.setString(++colCount, mwrItemDTO.getUomId(), statement);
					PristineDBUtil.setInt(++colCount, mwrItemDTO.getPriceCheckListId(), statement);
					PristineDBUtil.setInt(++colCount, mwrItemDTO.getLigRepItemCode(), statement);
					PristineDBUtil.setInt(++colCount, mwrItemDTO.getRegMultiple(), statement);
					PristineDBUtil.setDouble(++colCount, mwrItemDTO.getRegPrice(), statement);
					PristineDBUtil.setInt(++colCount, mwrItemDTO.getSaleMultiple(), statement);
					PristineDBUtil.setDouble(++colCount, mwrItemDTO.getSalePrice(), statement);
					PristineDBUtil.setInt(++colCount, mwrItemDTO.getPromoTypeId(), statement);
					PristineDBUtil.setInt(++colCount, mwrItemDTO.getAdPageNo(), statement);
					PristineDBUtil.setInt(++colCount, mwrItemDTO.getAdBlockNo(), statement);
					PristineDBUtil.setInt(++colCount, mwrItemDTO.getDisplayTypeId(), statement);
					PristineDBUtil.setString(++colCount, mwrItemDTO.getSaleStartDate(), statement);
					PristineDBUtil.setString(++colCount, mwrItemDTO.getSaleEndDate(), statement);
					PristineDBUtil.setDouble(++colCount, mwrItemDTO.getListCost(), statement);
					PristineDBUtil.setDouble(++colCount, mwrItemDTO.getDealCost(), statement);
					if (mwrItemDTO.getRecommendedRegPrice() != null) {
						PristineDBUtil.setInt(++colCount, mwrItemDTO.getRecommendedRegPrice().multiple, statement);
						PristineDBUtil.setDouble(++colCount, mwrItemDTO.getRecommendedRegPrice().price, statement);
					} else {
						PristineDBUtil.setInt(++colCount, null, statement);
						PristineDBUtil.setDouble(++colCount, null, statement);
					}
					PristineDBUtil.setDouble(++colCount, mwrItemDTO.getFinalPricePredictedMovement(), statement);
					PristineDBUtil.setInt(++colCount, mwrItemDTO.getFinalPricePredictionStatus(), statement);
					PristineDBUtil.setString(++colCount, mapper.writeValueAsString(mwrItemDTO.getExplainLog()), statement);
					PristineDBUtil.setInt(++colCount, mwrItemDTO.getCostChangeIndicator(), statement);
					PristineDBUtil.setInt(++colCount, mwrItemDTO.getCompPriceChangeIndicator(), statement);
					PristineDBUtil.setInt(++colCount, mwrItemDTO.getCompStrId(), statement);
					PristineDBUtil.setInt(++colCount, mwrItemDTO.getCompRegMultiple(), statement);
					PristineDBUtil.setDouble(++colCount, mwrItemDTO.getCompRegPrice(), statement);
					PristineDBUtil.setInt(++colCount, mwrItemDTO.getCompSaleMultiple(), statement);
					PristineDBUtil.setDouble(++colCount, mwrItemDTO.getCompSalePrice(), statement);
					PristineDBUtil.setString(++colCount, mwrItemDTO.getCompPriceCheckDate(), statement);
					PristineDBUtil.setDouble(++colCount, mwrItemDTO.getFinalPriceRevenue(), statement);
					PristineDBUtil.setDouble(++colCount, mwrItemDTO.getFinalPriceMargin(), statement);
					PristineDBUtil.setDouble(++colCount, mwrItemDTO.getCurrentRegUnits(), statement);
					PristineDBUtil.setDouble(++colCount, mwrItemDTO.getCurrentRegRevenue(), statement);
					PristineDBUtil.setDouble(++colCount, mwrItemDTO.getCurrentRegMargin(), statement);
					PristineDBUtil.setInt(++colCount, mwrItemDTO.getIsConflict(), statement);
					// PristineDBUtil.setInt(++colCount, mwrItemDTO.isNewPriceRecommended() ? 1 : 0,
					// statement);
					PristineDBUtil.setInt(++colCount,
							mwrItemDTO.isIsnewImpactCalculated() || mwrItemDTO.isNewPriceRecommended() ? 1 : 0,
									statement);
					PristineDBUtil.setString(++colCount, mwrItemDTO.getCurrPriceEffDate(), statement);
					PristineDBUtil.setString(++colCount, mwrItemDTO.getCostEffDate(), statement);
					PristineDBUtil.setInt(++colCount, mwrItemDTO.isRecError() ? 1 : 0, statement);
					PristineDBUtil.setString(++colCount,
							PRCommonUtil.getCommaSeperatedStringFromIntArray(mwrItemDTO.getRecErrorCodes()), statement);
					PristineDBUtil.setInt(++colCount, mwrItemDTO.getPeriodCalendarId(), statement);
					PristineDBUtil.setInt(++colCount, mwrItemDTO.getIsTPR(), statement);
					PristineDBUtil.setInt(++colCount, mwrItemDTO.getIsPrePriced(), statement);
					PristineDBUtil.setInt(++colCount, mwrItemDTO.getIsLockPriced(), statement);
					PristineDBUtil.setDouble(++colCount, mwrItemDTO.getPrevListCost(), statement);
					if (mwrItemDTO.getPrevCompPrice() != null) {
						PristineDBUtil.setDouble(++colCount,
								PRCommonUtil.getUnitPrice(mwrItemDTO.getPrevCompPrice(), true), statement);
					} else {
						PristineDBUtil.setDouble(++colCount, null, statement);
					}

					if (mwrItemDTO.getFuturePrice() != null) {
						PristineDBUtil.setInt(++colCount, mwrItemDTO.getFuturePrice().multiple, statement);
						PristineDBUtil.setDouble(++colCount, mwrItemDTO.getFuturePrice().price, statement);
					} else {
						PristineDBUtil.setInt(++colCount, null, statement);
						PristineDBUtil.setDouble(++colCount, null, statement);
					}
					PristineDBUtil.setString(++colCount, mwrItemDTO.getFuturePriceEffDate(), statement);
					if (futureCostPresent) {
						PristineDBUtil.setDouble(++colCount, mwrItemDTO.getFutureCost(), statement);
						PristineDBUtil.setString(++colCount, mwrItemDTO.getFutureCostEffDate(), statement);
					} else {
						PristineDBUtil.setInt(++colCount, null, statement);
						PristineDBUtil.setDouble(++colCount, null, statement);
					}
					PristineDBUtil.setInt(++colCount, mwrItemDTO.getVendorId(), statement);
					PristineDBUtil.setInt(++colCount, mwrItemDTO.getComp1StrId(), statement);
					PristineDBUtil.setInt(++colCount, mwrItemDTO.getComp2StrId(), statement);
					PristineDBUtil.setInt(++colCount, mwrItemDTO.getComp3StrId(), statement);
					PristineDBUtil.setInt(++colCount, mwrItemDTO.getComp4StrId(), statement);
					PristineDBUtil.setInt(++colCount, mwrItemDTO.getComp5StrId(), statement);
					PristineDBUtil.setMultiplePriceQty(++colCount, mwrItemDTO.getComp1Retail(), statement);
					PristineDBUtil.setMultiplePrice(++colCount, mwrItemDTO.getComp1Retail(), statement);
					PristineDBUtil.setMultiplePriceQty(++colCount, mwrItemDTO.getComp2Retail(), statement);
					PristineDBUtil.setMultiplePrice(++colCount, mwrItemDTO.getComp2Retail(), statement);
					PristineDBUtil.setMultiplePriceQty(++colCount, mwrItemDTO.getComp3Retail(), statement);
					PristineDBUtil.setMultiplePrice(++colCount, mwrItemDTO.getComp3Retail(), statement);
					PristineDBUtil.setMultiplePriceQty(++colCount, mwrItemDTO.getComp4Retail(), statement);
					PristineDBUtil.setMultiplePrice(++colCount, mwrItemDTO.getComp4Retail(), statement);
					PristineDBUtil.setMultiplePriceQty(++colCount, mwrItemDTO.getComp5Retail(), statement);
					PristineDBUtil.setMultiplePrice(++colCount, mwrItemDTO.getComp5Retail(), statement);
					PristineDBUtil.setDouble(++colCount, mwrItemDTO.getCoreRetail(), statement);
					PristineDBUtil.setDouble(++colCount, mwrItemDTO.getCwacCoreCost(), statement);
					//if item has pending retail and no override then save the approved impact  which will be used for for display 
					if (mwrItemDTO.getPendingRetail() != null && mwrItemDTO.getOverrideRegPrice() == null)
						PristineDBUtil.setDouble(++colCount, mwrItemDTO.getApprovedImpact(), statement);
					else
						PristineDBUtil.setDouble(++colCount, mwrItemDTO.getPriceChangeImpact(), statement);
					PristineDBUtil.setDouble(++colCount, mwrItemDTO.getOriginalListCost(), statement);
					PristineDBUtil.setInt(++colCount, mwrItemDTO.isSecondaryZoneRecPresent() ? 1 : 0, statement);
					PristineDBUtil.setDouble(++colCount, mwrItemDTO.getMapRetail(), statement);
					if (markOnHoldForNewRecommendation)
						PristineDBUtil.setString(++colCount, mwrItemDTO.isNewPriceRecommended() ? "Y" : "N", statement);
					else
						PristineDBUtil.setString(++colCount, "N", statement);

					PristineDBUtil.setInt(++colCount, mwrItemDTO.isIsnewImpactCalculated() ? 1 : 0, statement);
					PristineDBUtil.setInt(++colCount, mwrItemDTO.getIsPendingRetailRecommended(), statement);
					
					PristineDBUtil.setDouble(++colCount, futurePricePresent ? mwrItemDTO.getFutureWeekPrice() :null, statement);
					PristineDBUtil.setString(++colCount, futurePricePresent ? mwrItemDTO.getFutureWeekPriceEffDate():null, statement);
					PristineDBUtil.setString(++colCount,
							String.valueOf(mwrItemDTO.getIsImpactIncludedInSummaryCalculation()), statement);

					statement.addBatch();
					itemsInBatch++;
					if (itemsInBatch % Constants.BATCH_UPDATE_COUNT == 0) {
						statement.executeBatch();
						statement.clearBatch();
						itemsInBatch = 0;
					}
				}
			}

			if (itemsInBatch > 0) {
				statement.executeBatch();
				statement.clearBatch();
			}
		} catch (SQLException sqlE) {
			throw new GeneralException("saveRecommendationDetails() - Error while inserting into INSERT_PR_WEEKLY_REC_ITEM", sqlE);
		}
	}



	private static final String GET_PARENT_PR_WEEKLY_REC_ITEM = "SELECT PRODUCT_LEVEL_ID, PRODUCT_ID, "
			+ " WEEK_CALENDAR_ID, PRED_MOV_FINAL_PRICE, CURR_REVENUE, CURR_MARGIN, CURR_UNITS, REG_MULTIPLE, REG_PRICE, "
			+ " LIST_COST, REC_REG_MULTIPLE, REC_REG_PRICE, OVERRIDE_REG_MULTIPLE, OVERRIDE_REG_PRICE, "
			+ " OVERRIDE_PRICE_PREDICTED_MOV FROM PR_WEEKLY_REC_ITEM WHERE RUN_ID = ?";


	/**
	 * 
	 * @param conn
	 * @param parentRunId
	 * @return parent recommendation details
	 * @throws GeneralException
	 */
	public List<MWRItemDTO> getParentRecommendationDetails(Connection conn, long parentRunId) throws GeneralException {
		PreparedStatement statement = null;
		ResultSet resultSet = null;
		List<MWRItemDTO> parentRecommnedationDetails = new ArrayList<>();
		try {

			statement = conn.prepareStatement(GET_PARENT_PR_WEEKLY_REC_ITEM);
			statement.setLong(1, parentRunId);
			resultSet = statement.executeQuery();
			while (resultSet.next()) {
				MWRItemDTO mwrItemDTO = new MWRItemDTO();
				mwrItemDTO.setProductLevelId(resultSet.getInt("PRODUCT_LEVEL_ID"));
				mwrItemDTO.setProductId(resultSet.getInt("PRODUCT_ID"));
				mwrItemDTO.setWeekCalendarId(resultSet.getInt("WEEK_CALENDAR_ID"));
				mwrItemDTO.setFinalPricePredictedMovement(resultSet.getDouble("PRED_MOV_FINAL_PRICE"));

				// Changes done by Bhargavi on 3/12/2020
				// Recommendation Forecast
				mwrItemDTO.setFinalPriceRevenue(resultSet.getDouble("CURR_REVENUE"));
				mwrItemDTO.setFinalPriceMargin(resultSet.getDouble("CURR_MARGIN"));
				MultiplePrice currentPrice = new MultiplePrice(resultSet.getInt("REG_MULTIPLE"),
						resultSet.getDouble("REG_PRICE"));
				PricePointDTO currPricePP = new PricePointDTO();
				currPricePP.setPredictedMovement(resultSet.getDouble("CURR_UNITS"));
				mwrItemDTO.addRegPricePrediction(currentPrice, currPricePP);

				MultiplePrice newPrice = new MultiplePrice(resultSet.getInt("REC_REG_MULTIPLE"),
						resultSet.getDouble("REC_REG_PRICE"));
				PricePointDTO newPricePP = new PricePointDTO();
				newPricePP.setPredictedMovement(resultSet.getDouble("PRED_MOV_FINAL_PRICE"));
				mwrItemDTO.addRegPricePrediction(newPrice, newPricePP);

				MultiplePrice overRidePrice = new MultiplePrice(resultSet.getInt("OVERRIDE_REG_MULTIPLE"),
						resultSet.getDouble("OVERRIDE_REG_PRICE"));
				PricePointDTO overRidePricePP = new PricePointDTO();
				if(resultSet.getDouble("OVERRIDE_PRICE_PREDICTED_MOV") > 0)
				{
					overRidePricePP.setPredictedMovement(resultSet.getDouble("OVERRIDE_PRICE_PREDICTED_MOV"));
					mwrItemDTO.addRegPricePrediction(overRidePrice, overRidePricePP);

				}
				// Changes-ended

				parentRecommnedationDetails.add(mwrItemDTO);
			}
		} catch (SQLException sqlE) {
			throw new GeneralException("getParentRecommendationDetails() - Error while getting parent recommendation", sqlE);
		}

		return parentRecommnedationDetails;
	}


	private static final String GET_MULTI_WEEK_ITEM_DETAILS = "SELECT PRODUCT_ID, PRODUCT_LEVEL_ID, WEEK_CALENDAR_ID, "
			+ " TO_CHAR(RC.START_DATE,'" + Constants.DB_DATE_FORMAT + "') AS START_DATE, "
			+ " RUN_ID, REG_PRICE, PRR.RET_LIR_ID, REG_MULTIPLE, REG_PRICE, SALE_MULTIPLE, SALE_PRICE, PROMO_TYPE_ID, AD_PAGE_NO, "
			+ " AD_BLOCK_NO, DISPLAY_TYPE_ID, LIST_COST, DEAL_COST, REC_REG_MULTIPLE, REC_REG_PRICE, "
			+ " OVERRIDE_REG_MULTIPLE, OVERRIDE_REG_PRICE, CURR_UNITS, CURR_REVENUE, CURR_MARGIN, "
			+ " PRED_MOV_FINAL_PRICE, REVENUE_FINAL_PRICE, MARGIN_FINAL_PRICE, PERIOD_CALENDAR_ID, "
			+ " NEWPRICE_RECOMMENDED, IL.UPC, PRED_STATUS_FINAL_PRICE, PRC_CHANGE_IMPACT,PRR.COMP_REG_PRICE,PRR.COMP_REG_MULTIPLE,PRR.COMP_STR_ID,"
			+ " PRR.FUTURE_COST,PRR.FUTURE_COST_EFF_DATE,PRR.MAP_RETAIL,PRR.IS_PENDING_RETAIL_RECOMMENDED,PRR.EXPLAIN_LOG,PRR.IS_USER_OVERRIDE,PRR.IS_IMPACT_INCL_IN_SMRY_CALC  "
			+ " ,PRR.LIG_REP_ITEM_CODE FROM PR_WEEKLY_REC_ITEM PRR "
			+ " LEFT JOIN RETAIL_CALENDAR RC ON PRR.WEEK_CALENDAR_ID = RC.CALENDAR_ID"
			+ " LEFT JOIN ITEM_LOOKUP IL ON PRR.PRODUCT_ID = IL.ITEM_CODE AND PRR.PRODUCT_LEVEL_ID = " + Constants.ITEMLEVELID
			+ " WHERE PRR.RUN_ID IN (%RUN_IDS%)";



	/**
	 * 
	 * @param conn
	 * @param runIds
	 * @return multi week item collection
	 * @throws GeneralException
	 */
	public HashMap<Long, HashMap<RecWeekKey, HashMap<ItemKey, MWRItemDTO>>> getMultiWeeKItemsByRunId(Connection conn, List<Long> runIds) throws GeneralException{

		HashMap<Long, HashMap<RecWeekKey, HashMap<ItemKey, MWRItemDTO>>> itemsByRunId = new HashMap<>();

		List<MWRItemDTO> multiWeekItems = getMultiWeekRecDetails(conn, runIds);


		HashMap<Long, List<MWRItemDTO>> groupByRunId = (HashMap<Long, List<MWRItemDTO>>) multiWeekItems.stream().collect(Collectors.groupingBy(MWRItemDTO::getRunId));

		groupByRunId.forEach((runId, itemList) -> {

			HashMap<RecWeekKey, List<MWRItemDTO>> itemsByWeek = (HashMap<RecWeekKey, List<MWRItemDTO>>) itemList
					.stream().collect(Collectors.groupingBy(MWRItemDTO::getRecWeekKey));

			HashMap<RecWeekKey, HashMap<ItemKey, MWRItemDTO>> weeklyItemMap = new HashMap<>();

			itemsByWeek.forEach((recWeekKey, items) -> {
				HashMap<ItemKey, MWRItemDTO> itemMap = new HashMap<>();
				items.forEach(item -> {
					int lirInd = item.getProductLevelId() == Constants.PRODUCT_LEVEL_ID_LIG ? 1 : 0;
					ItemKey itemKey = new ItemKey(item.getProductId(), lirInd);
					itemMap.put(itemKey, item);
				});

				weeklyItemMap.put(recWeekKey, itemMap);
			});

			itemsByRunId.put(runId, weeklyItemMap);
		});
		return itemsByRunId;
	}


	/**
	 * 
	 * @param conn
	 * @param runIds
	 * @return multi week item details
	 * @throws GeneralException
	 */
	private List<MWRItemDTO> getMultiWeekRecDetails(Connection conn, List<Long> runIds) throws GeneralException {
		PreparedStatement statement = null;
		ResultSet resultSet = null;
		List<MWRItemDTO> multiWeekItems = new ArrayList<>();
		try {
			String runs = "";
			for (Long runId : runIds) {
				runs = runs + "," + runId;
			}

			runs = runs.substring(1);
			String query = new String(GET_MULTI_WEEK_ITEM_DETAILS);
			query = query.replaceAll("%RUN_IDS%", runs);
			statement = conn.prepareStatement(query);
			resultSet = statement.executeQuery();
			while (resultSet.next()) {

				MWRItemDTO mwrItemDTO = new MWRItemDTO();
				mwrItemDTO.setRunId(resultSet.getLong("RUN_ID"));
				mwrItemDTO.setProductId(resultSet.getInt("PRODUCT_ID"));
				mwrItemDTO.setProductLevelId(resultSet.getInt("PRODUCT_LEVEL_ID"));
				mwrItemDTO.setRetLirId(resultSet.getInt("RET_LIR_ID"));
				if (mwrItemDTO.getProductLevelId() == Constants.PRODUCT_LEVEL_ID_LIG) {
					mwrItemDTO.setItemCode(mwrItemDTO.getRetLirId());
					mwrItemDTO.setLir(true);
				} else {
					mwrItemDTO.setItemCode(mwrItemDTO.getProductId());
				}
				mwrItemDTO.setUpc(resultSet.getString("UPC"));
				mwrItemDTO.setWeekCalendarId(resultSet.getInt("WEEK_CALENDAR_ID"));
				mwrItemDTO.setWeekStartDate(resultSet.getString("START_DATE"));
				RecWeekKey recWeekKey = new RecWeekKey(mwrItemDTO.getWeekCalendarId(), mwrItemDTO.getWeekStartDate());
				mwrItemDTO.setRecWeekKey(recWeekKey);
				mwrItemDTO.setRegMultiple(resultSet.getInt("REG_MULTIPLE"));
				mwrItemDTO.setRegPrice(resultSet.getDouble("REG_PRICE"));
				mwrItemDTO.setSaleMultiple(resultSet.getInt("SALE_MULTIPLE"));
				mwrItemDTO.setSalePrice(resultSet.getDouble("SALE_PRICE"));
				mwrItemDTO.setPromoTypeId(resultSet.getInt("PROMO_TYPE_ID"));
				mwrItemDTO.setAdPageNo(resultSet.getInt("AD_PAGE_NO"));
				mwrItemDTO.setAdBlockNo(resultSet.getInt("AD_BLOCK_NO"));
				mwrItemDTO.setDisplayTypeId(resultSet.getInt("DISPLAY_TYPE_ID"));
				mwrItemDTO.setListCost(resultSet.getDouble("LIST_COST"));
				mwrItemDTO.setDealCost(resultSet.getDouble("DEAL_COST"));
				MultiplePrice recRegPrice = new MultiplePrice(resultSet.getInt("REC_REG_MULTIPLE"), resultSet.getDouble("REC_REG_PRICE"));
				mwrItemDTO.setRecommendedRegPrice(recRegPrice);

				if (resultSet.getDouble("OVERRIDE_REG_PRICE") > 0) {
					MultiplePrice overRegPrice = new MultiplePrice(resultSet.getInt("OVERRIDE_REG_MULTIPLE"),
							resultSet.getDouble("OVERRIDE_REG_PRICE"));
					mwrItemDTO.setOverrideRegPrice(overRegPrice);
					mwrItemDTO.setUserOverrideFlag(true);
					mwrItemDTO.setRecommendedRegPrice(overRegPrice);
				}

				if (resultSet.getInt("IS_USER_OVERRIDE") == 1) {
					mwrItemDTO.setUserOverrideFlag(true);
				}

				MultiplePrice currRegPrice = new MultiplePrice(mwrItemDTO.getRegMultiple(), mwrItemDTO.getRegPrice());
				mwrItemDTO.setCurrentPrice(currRegPrice);

				mwrItemDTO.setCurrentRegUnits(resultSet.getDouble("CURR_UNITS"));
				mwrItemDTO.setCurrentRegRevenue(resultSet.getDouble("CURR_REVENUE"));
				mwrItemDTO.setCurrentRegMargin(resultSet.getDouble("CURR_MARGIN"));

				mwrItemDTO.setFinalPricePredictedMovement(resultSet.getDouble("PRED_MOV_FINAL_PRICE"));
				mwrItemDTO.setFinalPriceRevenue(resultSet.getDouble("REVENUE_FINAL_PRICE"));
				mwrItemDTO.setFinalPriceMargin(resultSet.getDouble("MARGIN_FINAL_PRICE"));

				if (resultSet.getObject("PRED_STATUS_FINAL_PRICE") != null) {
					mwrItemDTO.setFinalPricePredictionStatus(resultSet.getInt("PRED_STATUS_FINAL_PRICE"));
				}


				mwrItemDTO.setPeriodCalendarId(resultSet.getInt("PERIOD_CALENDAR_ID"));
				mwrItemDTO.setNewPriceRecommended(resultSet.getInt("NEWPRICE_RECOMMENDED") == 1 ? true : false);

				PRItemSaleInfoDTO immediatePromoInfo = new PRItemSaleInfoDTO();
				if (mwrItemDTO.getSalePrice() > 0) {
					immediatePromoInfo.setSalePrice(new MultiplePrice(mwrItemDTO.getSaleMultiple(), mwrItemDTO.getSalePrice()));
					immediatePromoInfo.setPromoTypeId(mwrItemDTO.getPromoTypeId());
				}
				mwrItemDTO.setImmediatePromoInfo(immediatePromoInfo);
				mwrItemDTO.setPriceChangeImpact(resultSet.getDouble("PRC_CHANGE_IMPACT"));
				// Changes done on 05/07/2022
				// This field is added to compare the impact that is calculated before update
				// and wil be used to compare the impact after override
				// to identify items whose override is removed
				// field populated for using the approved impact for Items on pending retail in
				// final write
				mwrItemDTO.setApprovedImpact(resultSet.getDouble("PRC_CHANGE_IMPACT"));

				mwrItemDTO.setSendToPrediction(true);

				if (resultSet.getInt("COMP_REG_PRICE") > 0 && resultSet.getDouble("COMP_REG_MULTIPLE") > 0) {
					mwrItemDTO.setCompStrId(resultSet.getInt("COMP_STR_ID"));
					mwrItemDTO.setCompRegPrice(resultSet.getDouble("COMP_REG_PRICE"));
					mwrItemDTO.setCompRegMultiple(resultSet.getInt("COMP_REG_MULTIPLE"));
				}
				// Added by Karishma for future cost enhancement RA
				// populate the future cost if present
				if (resultSet.getString("FUTURE_COST") != null) {
					mwrItemDTO.setFutureCost(Double.parseDouble(resultSet.getString("FUTURE_COST")));
					mwrItemDTO.setFutureCostEffDate(resultSet.getString("FUTURE_COST_EFF_DATE"));
				}

				mwrItemDTO.setMapRetail(resultSet.getDouble("MAP_RETAIL"));
				mwrItemDTO.setIsPendingRetailRecommended(resultSet.getInt("IS_PENDING_RETAIL_RECOMMENDED"));

				try {
					if(populateExplainLogForUpdateRec)
					{
						Clob clob = resultSet.getClob("EXPLAIN_LOG");
						int clobLength = (int) clob.length();
						long i = 1;
						String stringClob = clob.getSubString(i, clobLength);
						ObjectMapper mapper = new ObjectMapper();
						PRExplainLog explainLog = mapper.readValue(stringClob, new TypeReference<PRExplainLog>() {
						});
						mwrItemDTO.setExplainLog(explainLog);
					}
				} catch (Exception e) {
					mwrItemDTO.setExplainLog(null);
					logger.error("Error while reading EXPLAIN_LOG : " + e);
				}

				if (resultSet.getString("IS_IMPACT_INCL_IN_SMRY_CALC") != null) {
					mwrItemDTO.setIsImpactIncludedInSummaryCalculation(
							resultSet.getString("IS_IMPACT_INCL_IN_SMRY_CALC").charAt(0));
				}
				mwrItemDTO.setLigRepItemCode(resultSet.getInt("LIG_REP_ITEM_CODE"));
			
				multiWeekItems.add(mwrItemDTO);
			}
		} catch (SQLException sqlE) {
			throw new GeneralException("getMultiWeekRecDetails() - Error while multi week recommendations", sqlE);
		}

		return multiWeekItems;
	}



	public void updateOverrideStatus(Connection conn,
			HashMap<RecWeekKey, HashMap<ItemKey, MWRItemDTO>> overrideItemDataMap) throws GeneralException {
		PreparedStatement stmt = null;
		try {
			stmt = conn.prepareStatement("UPDATE PR_WEEKLY_REC_ITEM SET OVERRIDE_PRED_UPDATE_STATUS = 0 "
					+ " WHERE RUN_ID = ? AND PRODUCT_LEVEL_ID = ? AND PRODUCT_ID = ?");

			for (Map.Entry<RecWeekKey, HashMap<ItemKey, MWRItemDTO>> overrideEntry : overrideItemDataMap.entrySet()) {
				for (Map.Entry<ItemKey, MWRItemDTO> itemEntry : overrideEntry.getValue().entrySet()) {
					MWRItemDTO mwrItemDTO = itemEntry.getValue();
					int counter = 0;
					//					stmt.setDouble(++counter, mwrItemDTO.getPriceChangeImpact());
					stmt.setLong(++counter, mwrItemDTO.getRunId());
					stmt.setInt(++counter, mwrItemDTO.getProductLevelId());
					stmt.setInt(++counter, mwrItemDTO.getProductId());
					stmt.addBatch();
				}
				break;
			}

			stmt.executeBatch();
			stmt.clearBatch();
		} catch (SQLException e) {
			logger.error("Error in updateReRecommendationDetails() - ", e);
			throw new GeneralException("Error in updateReRecommendationDetails() - " + e);
		} finally {
			PristineDBUtil.close(stmt);
		}

	}

	private static final String UPDATE_RE_RECOMMENDATION_DETAILS = "UPDATE PR_WEEKLY_REC_ITEM SET OVERRIDE_REG_MULTIPLE = ?, "
			+ " OVERRIDE_REG_PRICE = ?, OVERRIDE_SALES_D =?, OVERRIDE_MARGIN_D = ?, OVERRIDE_PRICE_PREDICTED_MOV = ?, IS_SYSTEM_OVERRIDE = ?, "
			+ " OVERRIDE_PRED_UPDATE_STATUS=?, OVERRIDE_PRICE_PRED_STATUS = ?, "
			+ " IS_USER_OVERRIDE = ?, SYS_OVERRIDE_LOG = ?, PRC_CHANGE_IMPACT = ? "
			+ " WHERE RUN_ID = ? AND PRODUCT_LEVEL_ID = ? AND PRODUCT_ID = ? AND WEEK_CALENDAR_ID = ? ";

	public String updateReRecommendationDetails(Connection conn, HashMap<RecWeekKey, HashMap<ItemKey, MWRItemDTO>> overrideItemDataMap) throws GeneralException {
		Set<Integer> userOverride = new HashSet<Integer>();
		Set<Integer> sysOverride = new HashSet<Integer>();
		PreparedStatement stmt = null;
		try {
			ObjectMapper mapper = new ObjectMapper();
			stmt = conn.prepareStatement(UPDATE_RE_RECOMMENDATION_DETAILS);
			String explainLogAsJson = "";

			for (Map.Entry<RecWeekKey, HashMap<ItemKey, MWRItemDTO>> overrideEntry : overrideItemDataMap.entrySet()) {

				for (Map.Entry<ItemKey, MWRItemDTO> itemEntry : overrideEntry.getValue().entrySet()) {
					MWRItemDTO mwrItemDTO = itemEntry.getValue();
					int counter = 0;
					if ((mwrItemDTO.isSystemOverrideFlag() || mwrItemDTO.isUserOverrideFlag())) {
						explainLogAsJson = "";
						// Update System Override price only if it is different from actual rec reg price
						if (mwrItemDTO.isSystemOverrideFlag()) {
							if (mwrItemDTO.getRecommendedRegPriceBeforeUpdateRec().price.equals(mwrItemDTO.getRecommendedRegPrice().price)
									&& mwrItemDTO.getRecommendedRegPriceBeforeUpdateRec().multiple.equals(mwrItemDTO.getRecommendedRegPrice().multiple)) {
								stmt.setNull(++counter, Types.NULL);
								stmt.setNull(++counter, Types.NULL);
								stmt.setNull(++counter, Types.NULL);
								stmt.setNull(++counter, Types.NULL);
								stmt.setNull(++counter, Types.NULL);
								stmt.setInt(++counter, 0);
							} else {
								sysOverride.add((Integer) (mwrItemDTO.getRetLirId() > 0 ? mwrItemDTO.getRetLirId() : mwrItemDTO.getItemCode()));
								if(mwrItemDTO.getRecommendedRegPrice() != null && mwrItemDTO.getRecommendedRegPrice().price > 0) {
									stmt.setInt(++counter, mwrItemDTO.getRecommendedRegPrice() != null ? mwrItemDTO.getRecommendedRegPrice().multiple : 0);
									stmt.setDouble(++counter, mwrItemDTO.getRecommendedRegPrice() != null ? mwrItemDTO.getRecommendedRegPrice().price : 0);
									PristineDBUtil.setDouble(++counter, mwrItemDTO.getFinalPriceRevenue(), stmt);
									PristineDBUtil.setDouble(++counter, mwrItemDTO.getFinalPriceMargin(), stmt);
								} else {
									stmt.setNull(++counter, java.sql.Types.INTEGER);
									stmt.setNull(++counter, java.sql.Types.DOUBLE);
									stmt.setNull(++counter, Types.NULL);
									stmt.setNull(++counter, Types.NULL);
								}

								PristineDBUtil.setDouble(++counter, mwrItemDTO.getFinalPricePredictedMovement(), stmt);
								stmt.setInt(++counter, mwrItemDTO.isSystemOverrideFlag() ? 1 : 0);
							}
						} else {
							userOverride.add((Integer) (mwrItemDTO.getRetLirId() > 0 ? mwrItemDTO.getRetLirId() : mwrItemDTO.getItemCode()));
							if(mwrItemDTO.getRecommendedRegPrice() != null && mwrItemDTO.getRecommendedRegPrice().price > 0) {
								stmt.setInt(++counter, mwrItemDTO.getRecommendedRegPrice() != null ? mwrItemDTO.getRecommendedRegPrice().multiple : 0);
								stmt.setDouble(++counter, mwrItemDTO.getRecommendedRegPrice() != null ? mwrItemDTO.getRecommendedRegPrice().price : 0);
								PristineDBUtil.setDouble(++counter, mwrItemDTO.getFinalPriceRevenue(), stmt);
								PristineDBUtil.setDouble(++counter, mwrItemDTO.getFinalPriceMargin(), stmt);
							} else {
								stmt.setNull(++counter, java.sql.Types.INTEGER);
								stmt.setNull(++counter, java.sql.Types.DOUBLE);
								stmt.setNull(++counter, Types.NULL);
								stmt.setNull(++counter, Types.NULL);
							}

							PristineDBUtil.setDouble(++counter, mwrItemDTO.getFinalPricePredictedMovement(), stmt);
							stmt.setInt(++counter, mwrItemDTO.isSystemOverrideFlag() ? 1 : 0);
						}
						stmt.setInt(++counter, 0);
						if (mwrItemDTO.getFinalPricePredictionStatus() != null) {
							stmt.setInt(++counter, mwrItemDTO.getFinalPricePredictionStatus());
						} else {
							stmt.setNull(++counter, java.sql.Types.INTEGER);
						}
						stmt.setInt(++counter, mwrItemDTO.isUserOverrideFlag() ? 1 : 0);
						if (mwrItemDTO.isSystemOverrideFlag()) {
							try {
								explainLogAsJson = mapper.writeValueAsString(mwrItemDTO.getExplainLog());
								stmt.setString(++counter, explainLogAsJson);
							} catch (JsonProcessingException e) {
								explainLogAsJson = "";
								logger.error("Error when converting explain log to json string - " + mwrItemDTO.getItemCode(), e);
							}
						} else {
							stmt.setNull(++counter, Types.NULL);
						}

					} else {
						stmt.setNull(++counter, Types.NULL);
						stmt.setNull(++counter, Types.NULL);
						stmt.setNull(++counter, Types.NULL);
						stmt.setNull(++counter, Types.NULL);
						stmt.setNull(++counter, Types.NULL);
						stmt.setInt(++counter, 0);
						stmt.setInt(++counter, 0);
						stmt.setNull(++counter, Types.NULL);
						stmt.setInt(++counter, 0);
						stmt.setNull(++counter, Types.NULL);
					}

					// if there is pending retail and no override then use the approved impact
					if (mwrItemDTO.getPendingRetail() != null && mwrItemDTO.getOverrideRegPrice()==null)
						stmt.setDouble(++counter, mwrItemDTO.getApprovedImpact());
					else
						stmt.setDouble(++counter, mwrItemDTO.getPriceChangeImpact());
					stmt.setLong(++counter, mwrItemDTO.getRunId());
					stmt.setInt(++counter, mwrItemDTO.getProductLevelId());
					stmt.setInt(++counter, mwrItemDTO.getProductId());
					stmt.setInt(++counter, mwrItemDTO.getWeekCalendarId());
					stmt.addBatch();
				}
			}
			stmt.executeBatch();
			stmt.clearBatch();
		} catch (SQLException e) {
			logger.error("Error in updateReRecommendationDetails() - ", e);
			throw new GeneralException("Error in updateReRecommendationDetails() - " + e);
		} finally {
			PristineDBUtil.close(stmt);
		}
		return "(# of User Override:" + userOverride.size() + " # of System Override:" + sysOverride.size() + ") ";
	}
	
		private static final String UPDATE_RE_RECOMMENDATION_AT_QUARTER_LEVEL_V2 = "UPDATE PR_QUARTER_REC_ITEM SET OVERRIDE_REG_MULTIPLE = ?, "
			+ " OVERRIDE_REG_PRICE = ?, OVERRIDE_SALES_D =?, OVERRIDE_MARGIN_D = ?, OVERRIDE_PRICE_PREDICTED_MOV = ?, IS_SYSTEM_OVERRIDE = ?, "
			+ " OVERRIDE_PRED_UPDATE_STATUS=?, OVERRIDE_PRICE_PRED_STATUS = ?, "
			+ " IS_USER_OVERRIDE = ?, SYS_OVERRIDE_LOG = ?, PRC_CHANGE_IMPACT = ?, NEWPRICE_RECOMMENDED = ? ,WEIGHT_NEW_RETAIL=?, IS_PENDING_RETAIL_RECOMMENDED=?"
			+ " ,EXPLAIN_LOG=? ,CONFLICT=? ,IS_IMPACT_INCL_IN_SMRY_CALC=? "
			+ " WHERE RUN_ID = ? "
			+ " AND PRODUCT_LEVEL_ID = ? AND PRODUCT_ID = ? AND CAL_TYPE = ? ";
	
	private static final String UPDATE_RE_RECOMMENDATION_AT_QUARTER_LEVEL = "UPDATE PR_QUARTER_REC_ITEM SET OVERRIDE_REG_MULTIPLE = ?, "
			+ " OVERRIDE_REG_PRICE = ?, OVERRIDE_SALES_D =?, OVERRIDE_MARGIN_D = ?, OVERRIDE_PRICE_PREDICTED_MOV = ?, IS_SYSTEM_OVERRIDE = ?, "
			+ " OVERRIDE_PRED_UPDATE_STATUS=?, OVERRIDE_PRICE_PRED_STATUS = ?, "
			+ " IS_USER_OVERRIDE = ?, SYS_OVERRIDE_LOG = ?, PRC_CHANGE_IMPACT = ?, NEWPRICE_RECOMMENDED=? WHERE RUN_ID = ? "
			+ " AND PRODUCT_LEVEL_ID = ? AND PRODUCT_ID = ? AND CAL_TYPE = ? ";
	
	/**
	 * 
	 * @param conn
	 * @param overrideList
	 * @throws GeneralException
	 */
	public void updateReRecommendationDetailsQuarterly(Connection conn, List<MWRItemDTO> overrideList, String calType)
			throws GeneralException {
		PreparedStatement stmt = null;
		try {
			ObjectMapper mapper = new ObjectMapper();
			stmt = conn.prepareStatement(UPDATE_RE_RECOMMENDATION_AT_QUARTER_LEVEL);
			String explainLogAsJson = "";

			for (MWRItemDTO mwrItemDTO : overrideList) {
				int counter = 0;
				if ((mwrItemDTO.isSystemOverrideFlag() || mwrItemDTO.isUserOverrideFlag())) {
					explainLogAsJson = "";
					// Update System Override price only if it is different from actual rec reg price
					if (mwrItemDTO.isSystemOverrideFlag()) {
						if (mwrItemDTO.getRecommendedRegPriceBeforeUpdateRec().price
								.equals(mwrItemDTO.getRecommendedRegPrice().price)
								&& mwrItemDTO.getRecommendedRegPriceBeforeUpdateRec().multiple
										.equals(mwrItemDTO.getRecommendedRegPrice().multiple)) {
							stmt.setNull(++counter, Types.NULL);
							stmt.setNull(++counter, Types.NULL);
							stmt.setNull(++counter, Types.NULL);
							stmt.setNull(++counter, Types.NULL);
							stmt.setNull(++counter, Types.NULL);
							stmt.setInt(++counter, 0);
						} else {
							if (mwrItemDTO.getRecommendedRegPrice() != null && mwrItemDTO.getRecommendedRegPrice().price > 0) {
								stmt.setInt(++counter,
										mwrItemDTO.getRecommendedRegPrice() != null
												? mwrItemDTO.getRecommendedRegPrice().multiple
												: 0);
								stmt.setDouble(++counter,
										mwrItemDTO.getRecommendedRegPrice() != null
												? mwrItemDTO.getRecommendedRegPrice().price
												: 0);
								PristineDBUtil.setDouble(++counter, mwrItemDTO.getRegRevenue(), stmt);
								PristineDBUtil.setDouble(++counter, mwrItemDTO.getRegMargin(), stmt);
							} else {
								stmt.setNull(++counter, java.sql.Types.INTEGER);
								stmt.setNull(++counter, java.sql.Types.DOUBLE);
								stmt.setNull(++counter, Types.NULL);
								stmt.setNull(++counter, Types.NULL);
							}

							PristineDBUtil.setDouble(++counter, mwrItemDTO.getRegUnits(), stmt);
						
							stmt.setInt(++counter, mwrItemDTO.isSystemOverrideFlag() ? 1 : 0);
						}
					} else {
						if (mwrItemDTO.getRecommendedRegPrice() != null && mwrItemDTO.getRecommendedRegPrice().price > 0) {
							stmt.setInt(++counter,
									mwrItemDTO.getRecommendedRegPrice() != null
											? mwrItemDTO.getRecommendedRegPrice().multiple
											: 0);
							stmt.setDouble(++counter,
									mwrItemDTO.getRecommendedRegPrice() != null
											? mwrItemDTO.getRecommendedRegPrice().price
											: 0);
							PristineDBUtil.setDouble(++counter, mwrItemDTO.getRegRevenue(), stmt);
							PristineDBUtil.setDouble(++counter, mwrItemDTO.getRegMargin(), stmt);
						} else {
							stmt.setNull(++counter, java.sql.Types.INTEGER);
							stmt.setNull(++counter, java.sql.Types.DOUBLE);
							stmt.setNull(++counter, Types.NULL);
							stmt.setNull(++counter, Types.NULL);
						}

						PristineDBUtil.setDouble(++counter, mwrItemDTO.getRegUnits(), stmt);

						stmt.setInt(++counter, mwrItemDTO.isSystemOverrideFlag() ? 1 : 0);
					}

					stmt.setInt(++counter, 0);
					if (mwrItemDTO.getFinalPricePredictionStatus() != null) {
						stmt.setInt(++counter, mwrItemDTO.getFinalPricePredictionStatus());
					} else {
						stmt.setNull(++counter, java.sql.Types.INTEGER);
					}

					stmt.setInt(++counter, mwrItemDTO.isUserOverrideFlag() ? 1 : 0);
					if (mwrItemDTO.isSystemOverrideFlag()) {
						try {
							explainLogAsJson = mapper.writeValueAsString(mwrItemDTO.getExplainLog());
							stmt.setString(++counter, explainLogAsJson);
						} catch (JsonProcessingException e) {
							explainLogAsJson = "";
							logger.error(
									"Error when converting explain log to json string - " + mwrItemDTO.getItemCode(),
									e);
						}
					} else {
						stmt.setNull(++counter, Types.NULL);
					}

				} else {
					stmt.setNull(++counter, Types.NULL);
					stmt.setNull(++counter, Types.NULL);
					stmt.setNull(++counter, Types.NULL);
					stmt.setNull(++counter, Types.NULL);
					stmt.setNull(++counter, Types.NULL);
					stmt.setInt(++counter, 0);
					stmt.setInt(++counter, 0);
					stmt.setNull(++counter, Types.NULL);
					stmt.setInt(++counter, 0);
					stmt.setNull(++counter, Types.NULL);
				}

				// if there is pending retail and no override then use the approved impact
				if (mwrItemDTO.getPendingRetail() != null && mwrItemDTO.getOverrideRegPrice()==null)
					stmt.setDouble(++counter, mwrItemDTO.getApprovedImpact());
				else
					stmt.setDouble(++counter, mwrItemDTO.getPriceChangeImpact());
				
				stmt.setInt(++counter, mwrItemDTO.isNewPriceRecommended() ? 1 : 0);
				stmt.setLong(++counter, mwrItemDTO.getRunId());
				stmt.setInt(++counter, mwrItemDTO.getProductLevelId());
				stmt.setInt(++counter, mwrItemDTO.getProductId());
				stmt.setString(++counter, calType);
				stmt.addBatch();
			}
			stmt.executeBatch();
			stmt.clearBatch();
		} catch (SQLException e) {
			logger.error("Error in updateReRecommendationDetailsQuarterly() - ", e);
			throw new GeneralException("Error in updateReRecommendationDetailsQuarterly() - " + e);
		} finally {
			PristineDBUtil.close(stmt);
		}
	}

	/**
	 * 
	 * @param conn
	 * @param overrideList
	 * @throws GeneralException
	 */
	public void updateReRecommendationDetailsQuarterlyV2(Connection conn, List<MWRItemDTO> overrideList, String calType)
			throws GeneralException {
		PreparedStatement stmt = null;
		try {
			ObjectMapper mapper = new ObjectMapper();
			stmt = conn.prepareStatement(UPDATE_RE_RECOMMENDATION_AT_QUARTER_LEVEL_V2);
			String explainLogAsJson = "";

			for (MWRItemDTO mwrItemDTO : overrideList) {
				int counter = 0;
				double weightedRecRetail = 0;
				double xweekxMovement = mwrItemDTO.getxWeeksMov();

				if (mwrItemDTO.getOverrideRegPrice() != null && mwrItemDTO.getOverrideRegPrice().getUnitPrice() > 0) {

					weightedRecRetail = mwrItemDTO.getOverrideRegPrice().getUnitPrice() * xweekxMovement;

				} else if (mwrItemDTO.getRecommendedRegPrice() != null) {
					weightedRecRetail = mwrItemDTO.getRecommendedRegPrice().getUnitPrice() * xweekxMovement;

				}

				if ((mwrItemDTO.isSystemOverrideFlag() || mwrItemDTO.isUserOverrideFlag())) {
					explainLogAsJson = "";
					// Update System Override price only if it is different from actual rec reg price
					if (mwrItemDTO.isSystemOverrideFlag()) {
						if (mwrItemDTO.getRecommendedRegPriceBeforeUpdateRec().price
								.equals(mwrItemDTO.getRecommendedRegPrice().price)
								&& mwrItemDTO.getRecommendedRegPriceBeforeUpdateRec().multiple
								.equals(mwrItemDTO.getRecommendedRegPrice().multiple)) {
							stmt.setNull(++counter, Types.NULL);
							stmt.setNull(++counter, Types.NULL);
							stmt.setNull(++counter, Types.NULL);
							stmt.setNull(++counter, Types.NULL);
							stmt.setNull(++counter, Types.NULL);
							stmt.setInt(++counter, 0);
						} else {
							if (mwrItemDTO.getRecommendedRegPrice() != null && mwrItemDTO.getRecommendedRegPrice().price > 0) {
								stmt.setInt(++counter,
										mwrItemDTO.getRecommendedRegPrice() != null
										? mwrItemDTO.getRecommendedRegPrice().multiple
												: 0);
								stmt.setDouble(++counter,
										mwrItemDTO.getRecommendedRegPrice() != null
										? mwrItemDTO.getRecommendedRegPrice().price
												: 0);
								PristineDBUtil.setDouble(++counter, mwrItemDTO.getRegRevenue(), stmt);
								PristineDBUtil.setDouble(++counter, mwrItemDTO.getRegMargin(), stmt);
							} else {
								stmt.setNull(++counter, java.sql.Types.INTEGER);
								stmt.setNull(++counter, java.sql.Types.DOUBLE);
								stmt.setNull(++counter, Types.NULL);
								stmt.setNull(++counter, Types.NULL);
							}

							PristineDBUtil.setDouble(++counter, mwrItemDTO.getRegUnits(), stmt);

							stmt.setInt(++counter, mwrItemDTO.isSystemOverrideFlag() ? 1 : 0);
						}
					} else {
						if (mwrItemDTO.getRecommendedRegPrice() != null && mwrItemDTO.getRecommendedRegPrice().price > 0) {
							stmt.setInt(++counter,
									mwrItemDTO.getRecommendedRegPrice() != null
									? mwrItemDTO.getRecommendedRegPrice().multiple
											: 0);
							stmt.setDouble(++counter,
									mwrItemDTO.getRecommendedRegPrice() != null
									? mwrItemDTO.getRecommendedRegPrice().price
											: 0);
							PristineDBUtil.setDouble(++counter, mwrItemDTO.getRegRevenue(), stmt);
							PristineDBUtil.setDouble(++counter, mwrItemDTO.getRegMargin(), stmt);
						} else {
							stmt.setNull(++counter, java.sql.Types.INTEGER);
							stmt.setNull(++counter, java.sql.Types.DOUBLE);
							stmt.setNull(++counter, Types.NULL);
							stmt.setNull(++counter, Types.NULL);
						}

						PristineDBUtil.setDouble(++counter, mwrItemDTO.getRegUnits(), stmt);

						stmt.setInt(++counter, mwrItemDTO.isSystemOverrideFlag() ? 1 : 0);
					}

					stmt.setInt(++counter, 0);
					if (mwrItemDTO.getFinalPricePredictionStatus() != null) {
						stmt.setInt(++counter, mwrItemDTO.getFinalPricePredictionStatus());
					} else {
						stmt.setNull(++counter, java.sql.Types.INTEGER);
					}

					stmt.setInt(++counter, mwrItemDTO.isUserOverrideFlag() ? 1 : 0);
					if (mwrItemDTO.isSystemOverrideFlag()) {
						try {
							explainLogAsJson = mapper.writeValueAsString(mwrItemDTO.getExplainLog());
							stmt.setString(++counter, explainLogAsJson);
						} catch (JsonProcessingException e) {
							explainLogAsJson = "";
							logger.error(
									"Error when converting explain log to json string - " + mwrItemDTO.getItemCode(),
									e);
						}
					} else {
						stmt.setNull(++counter, Types.NULL);
					}

				} else {
					stmt.setNull(++counter, Types.NULL);
					stmt.setNull(++counter, Types.NULL);
					stmt.setNull(++counter, Types.NULL);
					stmt.setNull(++counter, Types.NULL);
					stmt.setNull(++counter, Types.NULL);
					stmt.setInt(++counter, 0);
					stmt.setInt(++counter, 0);
					stmt.setNull(++counter, Types.NULL);
					stmt.setInt(++counter, 0);
					stmt.setNull(++counter, Types.NULL);
				}

				// if there is pending retail and no override then use the approved impact
				if (mwrItemDTO.getPendingRetail() != null && mwrItemDTO.getOverrideRegPrice() == null) {
					stmt.setDouble(++counter, mwrItemDTO.getApprovedImpact());
				} else {
					stmt.setDouble(++counter, mwrItemDTO.getPriceChangeImpact());
				}

				stmt.setInt(++counter, mwrItemDTO.isNewPriceRecommended() ? 1 : 0);
				stmt.setDouble(++counter, weightedRecRetail);
				stmt.setInt(++counter, mwrItemDTO.getIsPendingRetailRecommended());

				explainLogAsJson = "";
				try {
					explainLogAsJson = mapper.writeValueAsString(mwrItemDTO.getExplainLog());
				} catch (JsonProcessingException e) {
					explainLogAsJson = "";
					logger.error(
							"updateReRecommendationDetailsQuarterlyV2()-Error when converting explain log to json string - "
									+ mwrItemDTO.getItemCode(),
									e);
				}
				stmt.setString(++counter, explainLogAsJson);
				stmt.setInt(++counter, mwrItemDTO.getIsConflict());
				stmt.setLong(++counter, mwrItemDTO.getRunId());
				stmt.setInt(++counter, mwrItemDTO.getProductLevelId());
				stmt.setInt(++counter, mwrItemDTO.getProductId());
				stmt.setString(++counter, calType);
				stmt.addBatch();
			}
			stmt.executeBatch();
			stmt.clearBatch();
		} catch (SQLException e) {
			logger.error("Error in updateReRecommendationDetailsQuarterlyV2() - ", e);
			throw new GeneralException("Error in updateReRecommendationDetailsQuarterlyV2() - " + e);
		} finally {
			PristineDBUtil.close(stmt);
		}
	}


	private static final String UPDATE_RE_RECOMMENDATION_AT_PERIOD_LEVEL = "UPDATE PR_QUARTER_REC_ITEM SET OVERRIDE_REG_MULTIPLE = ?, "
			+ " OVERRIDE_REG_PRICE = ?, OVERRIDE_SALES_D =?, OVERRIDE_MARGIN_D = ?, OVERRIDE_PRICE_PREDICTED_MOV = ?, IS_SYSTEM_OVERRIDE = ?, "
			+ " OVERRIDE_PRED_UPDATE_STATUS=?, OVERRIDE_PRICE_PRED_STATUS = ?, "
			+ " IS_USER_OVERRIDE = ?, SYS_OVERRIDE_LOG = ?, PRC_CHANGE_IMPACT = ? WHERE RUN_ID = ? "
			+ " AND PRODUCT_LEVEL_ID = ? AND PRODUCT_ID = ? AND CAL_TYPE = ? AND PERIOD_CALENDAR_ID = ? ";

	/**
	 * 
	 * @param conn
	 * @param overrideList
	 * @throws GeneralException
	 * @throws JsonProcessingException 
	 */
	public void updateReRecommendationDetailsPeriod(Connection conn, List<MWRItemDTO> overrideList, String calType)
			throws GeneralException, JsonProcessingException {
		PreparedStatement stmt = null;
		try {
			ObjectMapper mapper = new ObjectMapper();
			stmt = conn.prepareStatement(UPDATE_RE_RECOMMENDATION_AT_PERIOD_LEVEL);
			String explainLogAsJson = "";

			for (MWRItemDTO mwrItemDTO : overrideList) {
				int counter = 0;
				if ((mwrItemDTO.isSystemOverrideFlag() || mwrItemDTO.isUserOverrideFlag())) {
					explainLogAsJson = "";
					// Update System Override price only if it is different from actual rec reg price
					if (mwrItemDTO.isSystemOverrideFlag()) {
						if (mwrItemDTO.getRecommendedRegPriceBeforeUpdateRec().price
								.equals(mwrItemDTO.getRecommendedRegPrice().price)
								&& mwrItemDTO.getRecommendedRegPriceBeforeUpdateRec().multiple
								.equals(mwrItemDTO.getRecommendedRegPrice().multiple)) {
							stmt.setNull(++counter, Types.NULL);
							stmt.setNull(++counter, Types.NULL);
							stmt.setNull(++counter, Types.NULL);
							stmt.setNull(++counter, Types.NULL);
							stmt.setNull(++counter, Types.NULL);
							stmt.setInt(++counter, 0);
						} else {
							if (mwrItemDTO.getRecommendedRegPrice() != null) {
								stmt.setInt(++counter,
										mwrItemDTO.getRecommendedRegPrice() != null
										? mwrItemDTO.getRecommendedRegPrice().multiple
												: 0);
								stmt.setDouble(++counter,
										mwrItemDTO.getRecommendedRegPrice() != null
										? mwrItemDTO.getRecommendedRegPrice().price
												: 0);
								PristineDBUtil.setDouble(++counter, mwrItemDTO.getRegRevenue(), stmt);
								PristineDBUtil.setDouble(++counter, mwrItemDTO.getRegMargin(), stmt);
							} else {
								stmt.setNull(++counter, java.sql.Types.INTEGER);
								stmt.setNull(++counter, java.sql.Types.DOUBLE);
								stmt.setNull(++counter, Types.NULL);
								stmt.setNull(++counter, Types.NULL);
							}

							PristineDBUtil.setDouble(++counter, mwrItemDTO.getRegUnits(), stmt);
							stmt.setInt(++counter, mwrItemDTO.isSystemOverrideFlag() ? 1 : 0);
						}
					} else {
						if (mwrItemDTO.getRecommendedRegPrice() != null) {
							stmt.setInt(++counter,
									mwrItemDTO.getRecommendedRegPrice() != null
									? mwrItemDTO.getRecommendedRegPrice().multiple
											: 0);
							stmt.setDouble(++counter,
									mwrItemDTO.getRecommendedRegPrice() != null
									? mwrItemDTO.getRecommendedRegPrice().price
											: 0);
							PristineDBUtil.setDouble(++counter, mwrItemDTO.getRegRevenue(), stmt);
							PristineDBUtil.setDouble(++counter, mwrItemDTO.getRegMargin(), stmt);
						} else {
							stmt.setNull(++counter, java.sql.Types.INTEGER);
							stmt.setNull(++counter, java.sql.Types.DOUBLE);
							stmt.setNull(++counter, Types.NULL);
							stmt.setNull(++counter, Types.NULL);
						}

						PristineDBUtil.setDouble(++counter, mwrItemDTO.getRegUnits(), stmt);
						stmt.setInt(++counter, mwrItemDTO.isSystemOverrideFlag() ? 1 : 0);
					}

					stmt.setInt(++counter, 0);
					if (mwrItemDTO.getFinalPricePredictionStatus() != null) {
						stmt.setInt(++counter, mwrItemDTO.getFinalPricePredictionStatus());
					} else {
						stmt.setNull(++counter, java.sql.Types.INTEGER);
					}

					stmt.setInt(++counter, mwrItemDTO.isUserOverrideFlag() ? 1 : 0);
					if (mwrItemDTO.isSystemOverrideFlag()) {
						try {
							explainLogAsJson = mapper.writeValueAsString(mwrItemDTO.getExplainLog());
							stmt.setString(++counter, explainLogAsJson);
						} catch (JsonProcessingException e) {
							explainLogAsJson = "";
							logger.error(
									"Error when converting explain log to json string - " + mwrItemDTO.getItemCode(),
									e);
						}
					} else {
						stmt.setNull(++counter, Types.NULL);
					}

				} else {
					stmt.setNull(++counter, Types.NULL);
					stmt.setNull(++counter, Types.NULL);
					stmt.setNull(++counter, Types.NULL);
					stmt.setNull(++counter, Types.NULL);
					stmt.setNull(++counter, Types.NULL);
					stmt.setInt(++counter, 0);
					stmt.setInt(++counter, 0);
					stmt.setNull(++counter, Types.NULL);
					stmt.setInt(++counter, 0);
					stmt.setNull(++counter, Types.NULL);
				}
				stmt.setDouble(++counter, mwrItemDTO.getPriceChangeImpact());
				stmt.setLong(++counter, mwrItemDTO.getRunId());
				stmt.setInt(++counter, mwrItemDTO.getProductLevelId());
				stmt.setInt(++counter, mwrItemDTO.getProductId());
				stmt.setString(++counter, calType);
				stmt.setInt(++counter, mwrItemDTO.getPeriodCalendarId());
				stmt.addBatch();
			}
			stmt.executeBatch();
			stmt.clearBatch();
		} catch (SQLException e) {
			logger.error("Error in updateReRecommendationDetails() - ", e);
			throw new GeneralException("Error in updateReRecommendationDetails() - " + e);
		} finally {
			PristineDBUtil.close(stmt);
		}
	}


	private static final String UPDATE_RE_RECC_WEIGHTED_RETAIL_AT_QUARTER_LEVEL = "UPDATE PR_QUARTER_REC_ITEM SET WEIGHT_NEW_RETAIL= ? WHERE RUN_ID = ?  "
			+ " AND PRODUCT_LEVEL_ID = ? AND PRODUCT_ID = ? AND CAL_TYPE = ?  ";

	public void updateWeightedPriceDetailsQuarterly(Connection conn, List<MWRItemDTO> overiddenMap, Long runId,
			String calType) throws GeneralException {
		PreparedStatement stmt = null;
		try {

			stmt = conn.prepareStatement(UPDATE_RE_RECC_WEIGHTED_RETAIL_AT_QUARTER_LEVEL);

			for (MWRItemDTO mwrItemDTO : overiddenMap) {
				double weightedRecRetail = 0;
				int counter = 0;
				//
				double xweekxMovement = mwrItemDTO.getxWeeksMov();

				if (mwrItemDTO.getOverrideRegPrice() != null && mwrItemDTO.getOverrideRegPrice().getUnitPrice() > 0) {

					weightedRecRetail = mwrItemDTO.getOverrideRegPrice().getUnitPrice() * xweekxMovement;

				} else if (mwrItemDTO.getRecommendedRegPrice() != null) {
					weightedRecRetail = mwrItemDTO.getRecommendedRegPrice().getUnitPrice() * xweekxMovement;

				}

				stmt.setDouble(++counter, weightedRecRetail);
				stmt.setLong(++counter, runId);
				stmt.setInt(++counter, mwrItemDTO.getProductLevelId());
				stmt.setInt(++counter, mwrItemDTO.getProductId());
				stmt.setString(++counter, calType);
				stmt.addBatch();
			}
			stmt.executeBatch();
			stmt.clearBatch();
		} catch (SQLException e) {
			logger.error("Error in updateWeightedPriceDetailsQuarterly() - ", e);
			throw new GeneralException("Error in updateWeightedPriceDetailsQuarterly() - " + e);
		} finally {
			PristineDBUtil.close(stmt);
		}

	}



	private static final String UPDATE_NEWPRICE_RECOMMENDED_FLAG = "UPDATE PR_QUARTER_REC_ITEM SET NEWPRICE_RECOMMENDED = ? WHERE RUN_ID = ?  "
			+ " AND PRODUCT_LEVEL_ID = ? AND PRODUCT_ID = ? AND CAL_TYPE = ?  ";
	/**
	 * 
	 * @param conn
	 * @param recommendedItemMap
	 * @param notRecommendedItemMap
	 * @param key
	 * @throws GeneralException
	 */
	public void updateRecommededFlagAtQtrLevel(Connection conn, List<MWRItemDTO> recommendedItemMap, Long runId)
			throws GeneralException {

		PreparedStatement stmt = null;
		try {

			stmt = conn.prepareStatement(UPDATE_NEWPRICE_RECOMMENDED_FLAG);

			for (MWRItemDTO mwrItemDTO : recommendedItemMap) {

				int counter = 0;

				stmt.setLong(++counter, runId);
				stmt.setInt(++counter, mwrItemDTO.isNewPriceRecommended() ? 1 : 0);
				stmt.setInt(++counter, mwrItemDTO.getProductLevelId());
				stmt.setInt(++counter, mwrItemDTO.getProductId());
				stmt.setString(++counter, Constants.CALENDAR_QUARTER);
				stmt.addBatch();
			}
			stmt.executeBatch();
			stmt.clearBatch();

		} catch (SQLException e) {
			logger.error("Error in updateIsReccFlagForGlobalZone() - ", e);
			throw new GeneralException("Error in updateIsReccFlagForGlobalZone() - " + e);
		} finally {
			PristineDBUtil.close(stmt);
		}

	}

	private static final String UPDATE_WEEKLY_TABLE_DETAILS = "UPDATE PR_WEEKLY_REC_ITEM SET OVERRIDE_REG_MULTIPLE = ?, "
			+ " OVERRIDE_REG_PRICE = ?, OVERRIDE_SALES_D =?, OVERRIDE_MARGIN_D = ?, OVERRIDE_PRICE_PREDICTED_MOV = ?, IS_SYSTEM_OVERRIDE = ?, "
			+ " OVERRIDE_PRED_UPDATE_STATUS=?, OVERRIDE_PRICE_PRED_STATUS = ?, "
			+ " IS_USER_OVERRIDE = ?, SYS_OVERRIDE_LOG = ?, PRC_CHANGE_IMPACT = ?, NEWPRICE_RECOMMENDED=? ,IS_PENDING_RETAIL_RECOMMENDED=?"
			+ ",EXPLAIN_LOG=?,CONFLICT=?,IS_IMPACT_INCL_IN_SMRY_CALC=? "
			+ " WHERE RUN_ID = ? AND PRODUCT_LEVEL_ID = ? AND PRODUCT_ID = ? AND WEEK_CALENDAR_ID = ? ";

	public  String updateWeeklyRecommendationDetails(Connection conn, HashMap<RecWeekKey, HashMap<ItemKey, MWRItemDTO>> overrideItemDataMap) throws GeneralException {
		Set<Integer> userOverride = new HashSet<Integer>();
		Set<Integer> sysOverride = new HashSet<Integer>();
		PreparedStatement stmt = null;
		try {
			ObjectMapper mapper = new ObjectMapper();
			stmt = conn.prepareStatement(UPDATE_WEEKLY_TABLE_DETAILS);
			String explainLogAsJson = "";

			for (Map.Entry<RecWeekKey, HashMap<ItemKey, MWRItemDTO>> overrideEntry : overrideItemDataMap.entrySet()) {

				for (Map.Entry<ItemKey, MWRItemDTO> itemEntry : overrideEntry.getValue().entrySet()) {
					MWRItemDTO mwrItemDTO = itemEntry.getValue();
					int counter = 0;
					if ((mwrItemDTO.isSystemOverrideFlag() || mwrItemDTO.isUserOverrideFlag())) {
						explainLogAsJson = "";
						// Update System Override price only if it is different from actual rec reg price
						if (mwrItemDTO.isSystemOverrideFlag()) {
							if (mwrItemDTO.getRecommendedRegPriceBeforeUpdateRec().price.equals(mwrItemDTO.getRecommendedRegPrice().price)
									&& mwrItemDTO.getRecommendedRegPriceBeforeUpdateRec().multiple.equals(mwrItemDTO.getRecommendedRegPrice().multiple)) {
								stmt.setNull(++counter, Types.NULL);
								stmt.setNull(++counter, Types.NULL);
								stmt.setNull(++counter, Types.NULL);
								stmt.setNull(++counter, Types.NULL);
								stmt.setNull(++counter, Types.NULL);
								stmt.setInt(++counter, 0);
							} else {
								sysOverride.add((Integer) (mwrItemDTO.getRetLirId() > 0 ? mwrItemDTO.getRetLirId() : mwrItemDTO.getItemCode()));
								if(mwrItemDTO.getRecommendedRegPrice() != null && mwrItemDTO.getRecommendedRegPrice().price > 0) {
									stmt.setInt(++counter, mwrItemDTO.getRecommendedRegPrice() != null ? mwrItemDTO.getRecommendedRegPrice().multiple : 0);
									stmt.setDouble(++counter, mwrItemDTO.getRecommendedRegPrice() != null ? mwrItemDTO.getRecommendedRegPrice().price : 0);
									PristineDBUtil.setDouble(++counter, mwrItemDTO.getFinalPriceRevenue(), stmt);
									PristineDBUtil.setDouble(++counter, mwrItemDTO.getFinalPriceMargin(), stmt);
								} else {
									stmt.setNull(++counter, java.sql.Types.INTEGER);
									stmt.setNull(++counter, java.sql.Types.DOUBLE);
									stmt.setNull(++counter, Types.NULL);
									stmt.setNull(++counter, Types.NULL);
								}

								PristineDBUtil.setDouble(++counter, mwrItemDTO.getFinalPricePredictedMovement(), stmt);
								stmt.setInt(++counter, mwrItemDTO.isSystemOverrideFlag() ? 1 : 0);
							}
						} else {
							userOverride.add((Integer) (mwrItemDTO.getRetLirId() > 0 ? mwrItemDTO.getRetLirId() : mwrItemDTO.getItemCode()));
							if(mwrItemDTO.getRecommendedRegPrice() != null && mwrItemDTO.getRecommendedRegPrice().price > 0) {
								stmt.setInt(++counter, mwrItemDTO.getRecommendedRegPrice() != null ? mwrItemDTO.getRecommendedRegPrice().multiple : 0);
								stmt.setDouble(++counter, mwrItemDTO.getRecommendedRegPrice() != null ? mwrItemDTO.getRecommendedRegPrice().price : 0);
								PristineDBUtil.setDouble(++counter, mwrItemDTO.getFinalPriceRevenue(), stmt);
								PristineDBUtil.setDouble(++counter, mwrItemDTO.getFinalPriceMargin(), stmt);
							} else {
								stmt.setNull(++counter, java.sql.Types.INTEGER);
								stmt.setNull(++counter, java.sql.Types.DOUBLE);
								stmt.setNull(++counter, Types.NULL);
								stmt.setNull(++counter, Types.NULL);
							}

							PristineDBUtil.setDouble(++counter, mwrItemDTO.getFinalPricePredictedMovement(), stmt);
							stmt.setInt(++counter, mwrItemDTO.isSystemOverrideFlag() ? 1 : 0);
						}
						stmt.setInt(++counter, 0);
						if (mwrItemDTO.getFinalPricePredictionStatus() != null) {
							stmt.setInt(++counter, mwrItemDTO.getFinalPricePredictionStatus());
						} else {
							stmt.setNull(++counter, java.sql.Types.INTEGER);
						}
						stmt.setInt(++counter, mwrItemDTO.isUserOverrideFlag() ? 1 : 0);
						if (mwrItemDTO.isSystemOverrideFlag()) {
							try {
								explainLogAsJson = mapper.writeValueAsString(mwrItemDTO.getExplainLog());
								stmt.setString(++counter, explainLogAsJson);
							} catch (JsonProcessingException e) {
								explainLogAsJson = "";
								logger.error("updateWeeklyRecommendationDetails()-Error when converting explain log to json string - " + mwrItemDTO.getItemCode(), e);
							}
						} else {
							stmt.setNull(++counter, Types.NULL);
						}

					} else {
						stmt.setNull(++counter, Types.NULL);
						stmt.setNull(++counter, Types.NULL);
						stmt.setNull(++counter, Types.NULL);
						stmt.setNull(++counter, Types.NULL);
						stmt.setNull(++counter, Types.NULL);
						stmt.setInt(++counter, 0);
						stmt.setInt(++counter, 0);
						stmt.setNull(++counter, Types.NULL);
						stmt.setInt(++counter, 0);
						stmt.setNull(++counter, Types.NULL);
					}

					// if there is pending retail and no override then use the approved impact
					if (mwrItemDTO.getPendingRetail() != null && mwrItemDTO.getOverrideRegPrice()==null)
						stmt.setDouble(++counter, mwrItemDTO.getApprovedImpact());
					else
						stmt.setDouble(++counter, mwrItemDTO.getPriceChangeImpact());
					//update is recommended flag after Update Recc
					stmt.setInt(++counter,
							mwrItemDTO.isIsnewImpactCalculated() || mwrItemDTO.isNewPriceRecommended() ? 1 : 0);

					stmt.setInt(++counter, mwrItemDTO.getIsPendingRetailRecommended());


					explainLogAsJson = "";
					try {
						explainLogAsJson = mapper.writeValueAsString(mwrItemDTO.getExplainLog());

					} catch (JsonProcessingException e) {
						explainLogAsJson = "";
						logger.error(
								"updateWeeklyRecommendationDetails()-Error when converting explain log to json string - "
										+ mwrItemDTO.getItemCode(),
										e);
					}

					stmt.setString(++counter, explainLogAsJson);
					stmt.setInt(++counter, mwrItemDTO.getIsConflict()); 
					stmt.setString(++counter, String.valueOf(mwrItemDTO.getIsImpactIncludedInSummaryCalculation()));	
					stmt.setLong(++counter, mwrItemDTO.getRunId());
					stmt.setInt(++counter, mwrItemDTO.getProductLevelId());
					stmt.setInt(++counter, mwrItemDTO.getProductId());
					stmt.setInt(++counter, mwrItemDTO.getWeekCalendarId());
					stmt.addBatch();
				}
			}
			stmt.executeBatch();
			stmt.clearBatch();
		} catch (SQLException e) {
			logger.error("Error in updateReRecommendationDetails() - ", e);
			throw new GeneralException("Error in updateReRecommendationDetails() - " + e);
		} finally {
			PristineDBUtil.close(stmt);
		}
		return "(# of User Override:" + userOverride.size() + " # of System Override:" + sysOverride.size() + ") ";
	}
	
	ObjectMapper mapper = new ObjectMapper();
	private static final String UPDATE_WEEKLY_TABLE_ENTRIES = "UPDATE PR_WEEKLY_REC_ITEM "
			+ " SET IS_IMPACT_INCL_IN_SMRY_CALC = ? , NEWPRICE_RECOMMENDED=? , EXPLAIN_LOG=?,  IS_USER_OVERRIDE=?,"
			+ " OVERRIDE_REG_PRICE=? ,OVERRIDE_REG_MULTIPLE=?,IS_PENDING_RETAIL_RECOMMENDED=? , OVERRIDE_SALES_D =?,"
			+ " OVERRIDE_MARGIN_D = ?, OVERRIDE_PRICE_PREDICTED_MOV = ? WHERE RUN_ID = ?  " + " AND PRODUCT_LEVEL_ID ="
			+ Constants.PRODUCT_LEVEL_ID_LIG + " AND PRODUCT_ID = ? AND WEEK_CALENDAR_ID = ?  ";

	public void updateStatusFlagForLIGRow(Connection conn, Map<RecWeekKey, HashMap<Integer, LigFlagsDTO>> lIGMapAtWeekLevel,
			Long runId) {
		PreparedStatement stmt = null;
		try {
		
			stmt = conn.prepareStatement(UPDATE_WEEKLY_TABLE_ENTRIES);

			for (Map.Entry<RecWeekKey, HashMap<Integer, LigFlagsDTO>> overrideEntry : lIGMapAtWeekLevel.entrySet()) {
				int calendarId = overrideEntry.getKey().calendarId;
				for (Map.Entry<Integer, LigFlagsDTO> itemEntry : overrideEntry.getValue().entrySet()) {
					int counter = 0;
					stmt.setString(++counter, itemEntry.getValue().getIsImpactIncluded());
					stmt.setInt(++counter, itemEntry.getValue().getIsnewPriceRecommended());
					String explainLogAsJson = "";
					try {
						explainLogAsJson = mapper.writeValueAsString(itemEntry.getValue().getExplainLog());
						stmt.setString(++counter, explainLogAsJson);
					} catch (JsonProcessingException e) {
						explainLogAsJson = "";
						logger.error(
								"updateStatusFlagForLIGRow()-Error when converting explain log to json string - "
										+ itemEntry.getKey(),
								e);
					}
					stmt.setInt(++counter, itemEntry.getValue().isUserOverrideFlag()? 1:0);
					if (itemEntry.getValue().getOverriddenRegularPrice() != null) {
						stmt.setDouble(++counter, itemEntry.getValue().getOverriddenRegularPrice().price);
						stmt.setInt(++counter, itemEntry.getValue().getOverriddenRegularPrice().multiple);
					} else {
						stmt.setDouble(++counter, 0.0);
						stmt.setInt(++counter, 0);
					}
					stmt.setInt(++counter, itemEntry.getValue().getIspendingRetailRecommended());
					stmt.setDouble(++counter, itemEntry.getValue().getFinalPriceRevenue());
					stmt.setDouble(++counter, itemEntry.getValue().getFinalPriceMargin());
					stmt.setDouble(++counter,itemEntry.getValue().getFinalpredictedMovement());
					stmt.setLong(++counter, runId);
					stmt.setInt(++counter, itemEntry.getKey());
					stmt.setInt(++counter, calendarId);
					stmt.addBatch();
				}
			}
			stmt.executeBatch();
			stmt.clearBatch();

		} catch (SQLException e) {
			logger.error("Error in updateStatusFlagForLIGRow() - ", e);

		} finally {
			PristineDBUtil.close(stmt);
		}
	}

	private static final String UPDATE_QUARTERLY_TABLE_ENTRIES = "UPDATE PR_QUARTER_REC_ITEM"
			+ " SET IS_IMPACT_INCL_IN_SMRY_CALC = ? , NEWPRICE_RECOMMENDED=?, EXPLAIN_LOG =? ,IS_USER_OVERRIDE=? ,"
			+ "OVERRIDE_REG_PRICE=? ,OVERRIDE_REG_MULTIPLE=?,IS_PENDING_RETAIL_RECOMMENDED=?,"
			+ "OVERRIDE_SALES_D =?, OVERRIDE_MARGIN_D = ?, OVERRIDE_PRICE_PREDICTED_MOV = ? " + " WHERE RUN_ID = ?  "
			+ " AND PRODUCT_LEVEL_ID =" + Constants.PRODUCT_LEVEL_ID_LIG + " AND PRODUCT_ID = ? AND CAL_TYPE ='"
			+ Constants.CALENDAR_QUARTER + "'";

	public void updateStatusFlagForLIGRowForQtr(Connection conn, HashMap<Integer, LigFlagsDTO> LIGMapAtQtrLevel,
			Long runId) {
		PreparedStatement stmt = null;
		try {
			stmt = conn.prepareStatement(UPDATE_QUARTERLY_TABLE_ENTRIES);
			for (Map.Entry<Integer, LigFlagsDTO> overrideEntry : LIGMapAtQtrLevel.entrySet()) {
				int counter = 0;
				stmt.setString(++counter, overrideEntry.getValue().getIsImpactIncluded());
				stmt.setInt(++counter, overrideEntry.getValue().getIsnewPriceRecommended());
				String explainLogAsJson = "";
				try {
					explainLogAsJson = mapper.writeValueAsString(overrideEntry.getValue().getExplainLog());
					stmt.setString(++counter, explainLogAsJson);
				} catch (JsonProcessingException e) {
					explainLogAsJson = "";
					logger.error("updateStatusFlagForLIGRowForQtr()-Error when converting explain log to json string - "
									+ overrideEntry.getKey() + e);
				}
				stmt.setInt(++counter, overrideEntry.getValue().isUserOverrideFlag()? 1:0);
				if (overrideEntry.getValue().getOverriddenRegularPrice() != null) {
					stmt.setDouble(++counter, overrideEntry.getValue().getOverriddenRegularPrice().price);
					stmt.setInt(++counter, overrideEntry.getValue().getOverriddenRegularPrice().multiple);
				} else {
					stmt.setDouble(++counter, 0.0);
					stmt.setInt(++counter, 0);
				}
				stmt.setInt(++counter, overrideEntry.getValue().getIspendingRetailRecommended());
				stmt.setDouble(++counter, overrideEntry.getValue().getRegRevenue());
				stmt.setDouble(++counter, overrideEntry.getValue().getRegMargin());
				stmt.setDouble(++counter,overrideEntry.getValue().getRegUnits());
				stmt.setLong(++counter, runId);
				stmt.setInt(++counter, overrideEntry.getKey());
				stmt.addBatch();
			}
			stmt.executeBatch();
			stmt.clearBatch();

		} catch (SQLException e) {
			logger.error("Error in updateStatusFlagForLIGRowForQtr() - ", e);

		} finally {
			PristineDBUtil.close(stmt);
		}
	}

}


