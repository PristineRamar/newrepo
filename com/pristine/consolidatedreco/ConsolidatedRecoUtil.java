package com.pristine.consolidatedreco;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Date;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.TreeMap;

import org.apache.log4j.Logger;

import com.pristine.dto.RetailCalendarDTO;
import com.pristine.dto.offermgmt.MultiplePrice;
import com.pristine.dto.offermgmt.PRStrategyDTO;
import com.pristine.exception.GeneralException;
import com.pristine.service.offermgmt.prediction.PredictionStatus;
import com.pristine.util.PristineDBUtil;
import com.pristine.util.offermgmt.PRConstants;

public class ConsolidatedRecoUtil {

	private static Logger logger = Logger.getLogger("ConsolidatedRecoUtil");

	// Query to get latest run id for given week
	private static final String GET_LATEST_RUN_ID_BY_WEEK_ID = "SELECT MAX(RUN_ID) AS RUN_ID, CALENDAR_ID FROM (SELECT RUN_ID, CALENDAR_ID, RANK() OVER "
			+ " (PARTITION BY PRODUCT_ID ORDER BY END_RUN_TIME DESC) AS RANK FROM PR_RECOMMENDATION_RUN_HEADER "
			+ " WHERE CALENDAR_ID IN (%CAL_IDS%) AND LOCATION_LEVEL_ID = ? AND LOCATION_ID = ? AND PRODUCT_ID = ? AND "
			+ " PREDICTED_BY <> 'CONSOLIDATED' AND RUN_TYPE <> '" + PRConstants.RUN_TYPE_TEMP
			+ "' AND END_RUN_TIME IS NOT NULL) GROUP BY CALENDAR_ID";

	private static final String GET_RECOMMENDATIONS_ITEMS_OF_WEEKS = "SELECT PRR.RUN_ID, PRR.PR_RECOMMENDATION_ID, PRR.ITEM_CODE, "
			+ " CASE PRR.LIR_IND WHEN 'Y' THEN null ELSE IL.RET_LIR_ID END AS RET_LIR_ID, PRR.LIR_IND, PRR.LIR_ID_OR_ITEM_CODE, "
			+ " PRR.CHILD_LOCATION_LEVEL_ID, PRR.CHILD_LOCATION_ID, PRR.RECOMMENDED_REG_MULTIPLE, PRR.RECOMMENDED_REG_PRICE, "
			+ " PRR.REC_WEEK_SALE_MULTIPLE, PRR.REC_WEEK_SALE_PRICE, PRR.REC_WEEK_AD_PAGE_NO, PRR.REC_WEEK_DISPLAY_TYPE_ID ,"
			+ " PRR.REC_WEEK_SALE_START_DATE, PRR.REC_WEEK_SALE_END_DATE, PRR.PREDICTED_MOVEMENT, PRR.REC_SALES_D, PRR.REC_MARGIN_D, "
			+ " PRR.REC_WEEK_SALE_PRED_AT_REC_REG, PRR.REC_WEEK_SALE_PRED_STATUS_REC, PRR.CUR_LIST_COST, "
			+ " CUR_COMP_REG_MULTIPLE, CUR_COMP_REG_PRICE, CUR_COMP_SALE_MULTIPLE, "
			+ " CUR_COMP_SALE_PRICE, COMP_LOCATION_TYPES_ID, COMP_STR_ID , COMP_PRICE_CHECK_DATE, RANK, "
			+ " CUR_SALES_D, CUR_MARGIN_D, CUR_REG_PRICE_PREDICTED_MOV , CUR_REG_MULTIPLE, CUR_REG_PRICE, REC_WEEK_SALE_COST, "
			+ " REC_WEEK_SALE_PRED_AT_CUR_REG, REC_WEEK_SALE_PRED_STATUS_CUR, PRR.REC_REG_PRED_REASONS, PRR.REC_SALE_PRED_REASONS  FROM PR_RECOMMENDATION PRR "
			+ " LEFT JOIN ITEM_LOOKUP IL ON PRR.ITEM_CODE = IL.ITEM_CODE " + " WHERE PRR.RUN_ID IN(%RUN_IDS%)";

	public static final String GET_NEXT_CALENDAR = "SELECT CALENDAR_ID FROM RETAIL_CALENDAR WHERE START_DATE = ( "
			+ "SELECT MIN(START_DATE) FROM RETAIL_CALENDAR WHERE START_DATE> TO_DATE(?,'MM/dd/yyyy') AND ROW_TYPE='W' ) AND ROW_TYPE='W' ";

	private static final String INSERT_RECOMMENDATION_HEADER = "INSERT INTO PR_RECOMMENDATION_RUN_HEADER "
			+ "(RUN_ID, CALENDAR_ID, LOCATION_LEVEL_ID, LOCATION_ID, PRODUCT_LEVEL_ID, PRODUCT_ID, RUN_TYPE, "
			+ " START_RUN_TIME, PREDICTED_BY, PREDICTED, END_RUN_TIME, PERCENT_COMPLETION, RUN_STATUS, STATUS "
			+ ") VALUES (?, ?, ?, ?, ?, ?, 'B', SYSDATE, ?, SYSDATE, SYSDATE, 100, 'S', 1 )";

	private static final String GET_RECOMMENDATION_RUN_ID = "SELECT PR_RECOMMENDATION_RUN_ID_SEQ.NEXTVAL AS RUN_ID FROM DUAL";

	private static final String INSERT_RECOMMENDATION_ITEM = "INSERT INTO PR_RECOMMENDATION (PR_RECOMMENDATION_ID, RUN_ID, ITEM_CODE,  "
			+ "RECOMMENDED_REG_MULTIPLE, RECOMMENDED_REG_PRICE,  "
			+ "IS_CONFLICT, CHILD_LOCATION_LEVEL_ID, CHILD_LOCATION_ID, PRE_REG_PRICE, PRE_LIST_COST, "
			+ "LIR_IND, LIR_ID_OR_ITEM_CODE, REC_WEEK_SALE_MULTIPLE, REC_WEEK_SALE_PRICE, REC_WEEK_PROMO_TYPE_ID, "
			+ "REC_WEEK_SALE_START_DATE, REC_WEEK_SALE_END_DATE, REC_WEEK_AD_PAGE_NO, REC_WEEK_AD_BLOCK_NO, "
			+ "REC_WEEK_DISPLAY_TYPE_ID, STRATEGY_ID, COST_CHG_IND,COMP_PRICE_CHG_IND, DIST_FLAG, OVERRIDE_PRED_UPDATE_STATUS,"
			+ "IS_REC_PRICE_ADJUSTED, IS_CUR_RET_SAME_IN_ALL_STORES, ACTUAL_ZONE_ID, IS_SUBSTITUTE,"
			+ "CUR_LIST_COST, LOG, IS_PRICE_RECOMMENDED, PREDICTION_STATUS, ITEM_SIZE, UOM_ID , "
			+ "IS_REC_ERROR, REC_ERROR_CODE, IS_INCLUDE_FOR_SUM_CAL, LIG_REP_ITEM_CODE, IS_TPR, IS_SALE, IS_AD, "
			+ "PREDICTED_MOVEMENT, REC_SALES_D, REC_MARGIN_D, REC_WEEK_SALE_PRED_AT_REC_REG, REC_WEEK_SALE_PRED_STATUS_REC, "
			+ "CUR_COMP_REG_MULTIPLE, CUR_COMP_REG_PRICE, CUR_COMP_SALE_MULTIPLE, "
			+ "CUR_COMP_SALE_PRICE, COMP_LOCATION_TYPES_ID, COMP_STR_ID, COMP_PRICE_CHECK_DATE, "
			+ "CUR_SALES_D, CUR_MARGIN_D, CUR_REG_PRICE_PREDICTED_MOV, CUR_REG_PRICE, CUR_REG_MULTIPLE, IS_GROUPED, REC_WEEK_SALE_COST, "
			+ "RANK, REC_WEEK_SALE_PRED_AT_CUR_REG, REC_WEEK_SALE_PRED_STATUS_CUR, REC_REG_PRED_REASONS, REC_SALE_PRED_REASONS ) "
			+ "SELECT PR_RECOMMENDATION_ID_SEQ.NEXTVAL, ?, ITEM_CODE,  "
			+ "RECOMMENDED_REG_MULTIPLE, RECOMMENDED_REG_PRICE, "
			+ "IS_CONFLICT, CHILD_LOCATION_LEVEL_ID, CHILD_LOCATION_ID, PRE_REG_PRICE, PRE_LIST_COST, "
			+ "LIR_IND, LIR_ID_OR_ITEM_CODE, REC_WEEK_SALE_MULTIPLE, REC_WEEK_SALE_PRICE, REC_WEEK_PROMO_TYPE_ID, "
			+ "TO_DATE(?,'MM/dd/yyyy'), TO_DATE(?,'MM/dd/yyyy'), REC_WEEK_AD_PAGE_NO, REC_WEEK_AD_BLOCK_NO, "
			+ "REC_WEEK_DISPLAY_TYPE_ID, STRATEGY_ID, COST_CHG_IND,COMP_PRICE_CHG_IND, DIST_FLAG, OVERRIDE_PRED_UPDATE_STATUS,"
			+ "IS_REC_PRICE_ADJUSTED, 'Y' , CHILD_LOCATION_ID, 'N' , "
			+ "CUR_LIST_COST, LOG, IS_PRICE_RECOMMENDED, ?, ITEM_SIZE, UOM_ID , "
			+ "IS_REC_ERROR, REC_ERROR_CODE, ?, LIG_REP_ITEM_CODE, IS_TPR, IS_SALE, IS_AD, "
			+ "?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ? "
			+ "FROM PR_RECOMMENDATION WHERE PR_RECOMMENDATION_ID = ?";

	private static final String UPDATE_PR_DASHBOARD = "UPDATE PR_DASHBOARD SET RECOMMENDATION_RUN_ID = ? WHERE "
			+ " PRODUCT_ID = ? and LOCATION_LEVEL_ID = ? and LOCATION_ID = ? and PRODUCT_LEVEL_ID = ?";

	/**
	 * Extracting latest Run Id values for given week list and other input
	 * parameters
	 * 
	 * @param conn
	 * @param calendarList
	 * @param locationLevelId
	 * @param locationId
	 * @param productId
	 * 
	 * @return hashMap of calendar as Key and latestRunId as Value
	 * @throws GeneralException
	 */
	public HashMap<Integer, Long> getLatestRecommendationRunIdforCalendar(Connection conn,
			ArrayList<RetailCalendarDTO> calendarList, int locationLevelId, int locationId, int productId)
			throws GeneralException {

		logger.debug("Start of getLatestRecommendationRunIdforCalendar ...");

		PreparedStatement stmt = null;
		ResultSet rs = null;
		String calendarIds = "";
		HashMap<Integer, Long> runIdsList = null;

		try {

			if (calendarList != null && calendarList.size() > 0) {

				for (RetailCalendarDTO retailCalDTO : calendarList) {
					calendarIds = calendarIds + "," + String.valueOf(retailCalDTO.getCalendarId());
				}
				calendarIds = calendarIds.substring(1);

			} else {
				logger.error("Calendar List is null or empty.");
				throw new Exception("Calendar List is null or empty.");
			}

			String query = new String(GET_LATEST_RUN_ID_BY_WEEK_ID);
			query = query.replaceAll("%CAL_IDS%", calendarIds);

			logger.debug("GET_LATEST_RUN_ID_BY_WEEK_ID:" + query);
			stmt = conn.prepareStatement(query);

			stmt.setInt(1, locationLevelId);
			stmt.setInt(2, locationId);
			stmt.setInt(3, productId);
			rs = stmt.executeQuery();
			runIdsList = new HashMap<Integer, Long>();

			while (rs.next()) {
				runIdsList.put(rs.getInt("CALENDAR_ID"), rs.getLong("RUN_ID"));
			}
			logger.debug("Run Id List retrieved successfully");

		} catch (SQLException exception) {
			logger.error("SQL Error in getLatestRecommendationRunId() - " + exception.toString());
			throw new GeneralException("SQL Error in getLatestRecommendationRunId() - " + exception.toString());
		} catch (Exception exception) {
			logger.error("Error in getLatestRecommendationRunId() - " + exception.toString());
			throw new GeneralException("Error in getLatestRecommendationRunId() - " + exception.toString());
		} finally {
			PristineDBUtil.close(rs);
			PristineDBUtil.close(stmt);
		}

		logger.debug("End of getLatestRecommendationRunIdforCalendar ...");
		return runIdsList;
	}

	/**
	 * Retrieving the recommendation data for given run Id list for consolidation
	 * 
	 * @param conn
	 * @param runIdsList
	 * @return List of PR Recommendation DTO
	 * @throws GeneralException
	 */
	public List<PRRecomDTO> getRecommendationItemsForConsolidation(Connection conn, HashMap<Integer, Long> runIdsList)
			throws GeneralException {

		logger.debug("Start of getRecommendationItemsForConsolidation()");
		PreparedStatement stmt = null;
		ResultSet rs = null;
		List<PRRecomDTO> prRecomDTOList = null;

		try {

			prRecomDTOList = new ArrayList<PRRecomDTO>();
			String runs = "";

			for (Long runId : runIdsList.values()) {
				runs = runs + "," + runId;
			}

			runs = runs.substring(1);
			String query = new String(GET_RECOMMENDATIONS_ITEMS_OF_WEEKS);
			query = query.replaceAll("%RUN_IDS%", runs);
			stmt = conn.prepareStatement(query);

			logger.debug("GET_RECOMMENDATIONS_ITEMS_OF_WEEKS:" + query);
			rs = stmt.executeQuery();

			while (rs.next()) {

				PRRecomDTO prrecomDTO = new PRRecomDTO();

				if (rs.getObject("RUN_ID") != null) {
					prrecomDTO.setRunId(rs.getLong("RUN_ID"));
				}
				if (rs.getObject("PR_RECOMMENDATION_ID") != null) {
					prrecomDTO.setRecommendationId(rs.getLong("PR_RECOMMENDATION_ID"));
				}
				if (rs.getObject("ITEM_CODE") != null) {
					prrecomDTO.setItemCode(rs.getInt("ITEM_CODE"));
				}
				if (rs.getObject("RET_LIR_ID") != null) {
					prrecomDTO.setRetLirId(rs.getString("RET_LIR_ID"));
				}
				if (rs.getObject("LIR_IND") != null) {
					prrecomDTO.setLirIndicator(rs.getString("LIR_IND"));
				}
				if (rs.getObject("LIR_ID_OR_ITEM_CODE") != null) {
					prrecomDTO.setLirIdOrItemCode(rs.getInt("LIR_ID_OR_ITEM_CODE"));
				}
				if (rs.getObject("CHILD_LOCATION_LEVEL_ID") != null) {
					prrecomDTO.setChildLocationLevelId(rs.getInt("CHILD_LOCATION_LEVEL_ID"));
				}
				if (rs.getObject("CHILD_LOCATION_ID") != null) {
					prrecomDTO.setChildLocationId(rs.getInt("CHILD_LOCATION_ID"));
				}

				if ((rs.getObject("RECOMMENDED_REG_MULTIPLE") != null)
						&& (rs.getObject("RECOMMENDED_REG_PRICE") != null)) {

					prrecomDTO.setRecommendedRegPrice(new MultiplePrice(rs.getInt("RECOMMENDED_REG_MULTIPLE"),
							rs.getDouble("RECOMMENDED_REG_PRICE")));
				}

				if ((rs.getObject("REC_WEEK_SALE_MULTIPLE") != null) && (rs.getObject("REC_WEEK_SALE_PRICE") != null)) {
					prrecomDTO.setRecommendedWeekSalePrice(new MultiplePrice(rs.getInt("REC_WEEK_SALE_MULTIPLE"),
							rs.getDouble("REC_WEEK_SALE_PRICE")));
				}

				if (rs.getObject("REC_WEEK_AD_PAGE_NO") != null) {
					prrecomDTO.setRecommendedWeekAdPageNo(rs.getInt("REC_WEEK_AD_PAGE_NO"));
				}
				if (rs.getObject("REC_WEEK_DISPLAY_TYPE_ID") != null) {
					prrecomDTO.setRecommendedWeekDisplayTypeId(rs.getInt("REC_WEEK_DISPLAY_TYPE_ID"));
				}

				// Start of changes

				if (rs.getObject("PREDICTED_MOVEMENT") != null) {
					prrecomDTO.setPredictedMovement(rs.getDouble("PREDICTED_MOVEMENT"));
				}
				if (rs.getObject("REC_SALES_D") != null) {
					prrecomDTO.setRecSalesD(rs.getDouble("REC_SALES_D"));
				}
				if (rs.getObject("REC_MARGIN_D") != null) {
					prrecomDTO.setRecMarginD(rs.getDouble("REC_MARGIN_D"));
				}
				if (rs.getObject("REC_WEEK_SALE_PRED_AT_REC_REG") != null) {
					prrecomDTO.setRecWeekSalePredAtRecReg(rs.getDouble("REC_WEEK_SALE_PRED_AT_REC_REG"));
				}
				if (rs.getObject("REC_WEEK_SALE_PRED_STATUS_REC") != null) {
					prrecomDTO.setRecWeekSalePredStatusRec(rs.getInt("REC_WEEK_SALE_PRED_STATUS_REC"));
				}
				if (rs.getObject("CUR_LIST_COST") != null) {
					prrecomDTO.setCurrentListCost(rs.getDouble("CUR_LIST_COST"));
				}

				if (rs.getObject("CUR_COMP_REG_MULTIPLE") != null) {
					prrecomDTO.setCurCompRegMultiple(rs.getInt("CUR_COMP_REG_MULTIPLE"));
				}
				if (rs.getObject("CUR_COMP_REG_PRICE") != null) {
					prrecomDTO.setCurCompRegPrice(rs.getDouble("CUR_COMP_REG_PRICE"));
				}
				if (rs.getObject("CUR_COMP_SALE_MULTIPLE") != null) {
					prrecomDTO.setCurCompSaleMultiple(rs.getInt("CUR_COMP_SALE_MULTIPLE"));
				}
				if (rs.getObject("CUR_COMP_SALE_PRICE") != null) {
					prrecomDTO.setCurCompSalePrice(rs.getDouble("CUR_COMP_SALE_PRICE"));
				}
				if (rs.getObject("COMP_LOCATION_TYPES_ID") != null) {
					prrecomDTO.setCompLocationTypesId(rs.getInt("COMP_LOCATION_TYPES_ID"));
				}
				if (rs.getObject("COMP_STR_ID") != null) {
					prrecomDTO.setCompStrId(rs.getInt("COMP_STR_ID"));
				}
				if (rs.getObject("COMP_PRICE_CHECK_DATE") != null) {
					prrecomDTO.setCompPriceCheckDate(rs.getDate("COMP_PRICE_CHECK_DATE"));
				}
				if (rs.getObject("CUR_SALES_D") != null) {
					prrecomDTO.setCurSalesD(rs.getDouble("CUR_SALES_D"));
				}
				if (rs.getObject("CUR_MARGIN_D") != null) {
					prrecomDTO.setCurMarginD(rs.getDouble("CUR_MARGIN_D"));
				}
				if (rs.getObject("CUR_REG_PRICE_PREDICTED_MOV") != null) {
					prrecomDTO.setCurRegPricePredictedMov(rs.getDouble("CUR_REG_PRICE_PREDICTED_MOV"));
				}

				if ((rs.getObject("CUR_REG_MULTIPLE") != null) && (rs.getObject("CUR_REG_PRICE") != null)) {
					prrecomDTO.setCurRegPrice(
							new MultiplePrice(rs.getInt("CUR_REG_MULTIPLE"), rs.getDouble("CUR_REG_PRICE")));
				}
				if (rs.getObject("REC_WEEK_SALE_COST") != null) {
					prrecomDTO.setRecWeekSaleCost(rs.getDouble("REC_WEEK_SALE_COST"));
				}
				if (rs.getObject("RANK") != null) {
					prrecomDTO.setRank(rs.getInt("RANK"));
				}
				if (rs.getObject("REC_WEEK_SALE_PRED_AT_CUR_REG") != null) {
					prrecomDTO.setRecWeekSalePredAtCurReg(rs.getDouble("REC_WEEK_SALE_PRED_AT_CUR_REG"));
				}
				if (rs.getObject("REC_WEEK_SALE_PRED_STATUS_CUR") != null) {
					prrecomDTO.setRecWeekSalePredStatusCur(rs.getInt("REC_WEEK_SALE_PRED_STATUS_CUR"));
				}
				if (rs.getObject("REC_REG_PRED_REASONS") != null) {
					prrecomDTO.setRegPricePredReasons(rs.getString("REC_REG_PRED_REASONS"));
				}
				if (rs.getObject("REC_SALE_PRED_REASONS") != null) {
					prrecomDTO.setSalePricePredReasons(rs.getString("REC_SALE_PRED_REASONS"));
				}

				prRecomDTOList.add(prrecomDTO);
			}
			logger.debug(" Successfully retrieved records from PR_Recommendation table");

		} catch (Exception ex) {
			logger.error("Error in getRecommendationItemsForConsolidation() -- " + ex.toString(), ex);
			throw new GeneralException("Error in getRecommendationItemsForConsolidation() - " + ex.toString());

		} finally {
			PristineDBUtil.close(rs);
			PristineDBUtil.close(stmt);
		}
		logger.debug("End of getRecommendationItemsForConsolidation()");
		return prRecomDTOList;

	}

	/**
	 * Consolidate the recommendation records in ascending order of weeks
	 * 
	 * @param inputPrRecomList
	 * @param sortedStartDates
	 * @param calendarList
	 * @param runIdsList
	 * @return List of PR Recommendation DTO
	 * @throws GeneralException
	 */
	public List<PRRecomDTO> processConsolidation(List<PRRecomDTO> inputPrRecomList,
			TreeMap<Date, String> sortedStartDates, ArrayList<RetailCalendarDTO> calendarList,
			HashMap<Integer, Long> runIdsList) throws GeneralException {

		logger.debug("Start of processConsolidation()");

		ArrayList<PRRecomDTO> prRecomDTOOutputList = null;

		try {
			prRecomDTOOutputList = new ArrayList<PRRecomDTO>();

			ArrayList<Integer> itemList = new ArrayList<Integer>();

			TreeMap<Date, Long> runIdSeq = getRunIdSeq(sortedStartDates, calendarList, runIdsList);

			logger.debug("processConsolidation() : Extracting the distinct item list");
			// Extracting the distinct item list
			for (PRRecomDTO prDTOcur : inputPrRecomList) {
				if (!itemList.contains(prDTOcur.getItemCode())) {
					itemList.add(prDTOcur.getItemCode());
				} else {
					continue;
				}
			}

			// Extracting records for items one by one
			for (Integer item : itemList) {

				ArrayList<PRRecomDTO> prItemWiseoutput = new ArrayList<PRRecomDTO>();

				ArrayList<PRRecomDTO> prItemWiseAggroutput = new ArrayList<PRRecomDTO>();

				TreeMap<Long, PRRecomDTO> prRecomListTmp = new TreeMap<Long, PRRecomDTO>();
				for (PRRecomDTO prDTOcur : inputPrRecomList) {
					if (item.intValue() == prDTOcur.getItemCode()) {
						prRecomListTmp.put(prDTOcur.getRunId(), prDTOcur);
					}
				}

				logger.debug("processConsolidation() : Processing each item in Ascending order of weeks");
				// Processing each item in Ascending order of weeks

				PRRecomDTO precomDTOpre = null;
				// PRRecomDTO precomDTOtemp = null;
				boolean isCommon = true;
				int recordCount = 0; // to check if it is last record
				String commonRegPriceReson = "";
				String commonSalePriceReson = "";

				for (Long runId : runIdSeq.values()) {
					PRRecomDTO prrecom = prRecomListTmp.get(runId);

					recordCount++;
					if (precomDTOpre == null) {
						precomDTOpre = prrecom;
						continue;
					}

					if (isCommon && ((precomDTOpre.getRecommendedRegPrice() != null
							&& precomDTOpre.getRecommendedRegPrice().equals(prrecom.getRecommendedRegPrice()))
							|| (precomDTOpre.getRecommendedRegPrice() == null
									&& prrecom.getRecommendedRegPrice() == null))) {
						isCommon = true;
					} else {
						isCommon = false;
					}

					if (isCommon && ((precomDTOpre.getRecommendedWeekSalePrice() != null
							&& precomDTOpre.getRecommendedWeekSalePrice().equals(prrecom.getRecommendedWeekSalePrice()))
							|| (precomDTOpre.getRecommendedWeekSalePrice() == null
									&& prrecom.getRecommendedWeekSalePrice() == null))) {
						isCommon = true;
					} else {
						isCommon = false;
					}

					if (precomDTOpre.getRecommendedWeekAdPageNo() != prrecom.getRecommendedWeekAdPageNo() && isCommon) {
						isCommon = false;
					}

					if (precomDTOpre.getRecommendedWeekDisplayTypeId() != prrecom.getRecommendedWeekDisplayTypeId()
							&& isCommon) {
						isCommon = false;
					}

					// Prediction Confidence most common occurrence changes
					/*
					 * if(predConfidenceFrequency.get(precomDTOpre.getRecRegPricePredConfidence())
					 * != null ){ Integer frequency =
					 * predConfidenceFrequency.get(precomDTOpre.getRecRegPricePredConfidence()) + 1;
					 * predConfidenceFrequency.put(precomDTOpre.getRecRegPricePredConfidence(),
					 * frequency); } else {
					 * predConfidenceFrequency.put(precomDTOpre.getRecRegPricePredConfidence(), 1);
					 * }
					 */

					// Questionable Prediction
					if (prrecom.getRegPricePredReasons() != null && prrecom.getRegPricePredReasons().length() > 0) {
						commonRegPriceReson = commonRegPriceReson + "," + prrecom.getRegPricePredReasons();
					}
					if (prrecom.getSalePricePredReasons() != null && prrecom.getSalePricePredReasons().length() > 0) {
						commonSalePriceReson = commonSalePriceReson + "," + prrecom.getSalePricePredReasons();
					}

					if (isCommon) {

						// Changes start for not null values

						if (precomDTOpre.getCurCompRegMultiple() == null && prrecom.getCurCompRegMultiple() != null) {
							precomDTOpre.setCurCompRegMultiple(prrecom.getCurCompRegMultiple());
						}
						if (precomDTOpre.getCurCompRegPrice() == null && prrecom.getCurCompRegPrice() != null) {
							precomDTOpre.setCurCompRegPrice(prrecom.getCurCompRegPrice());
						}
						if (precomDTOpre.getCurCompSaleMultiple() == null && prrecom.getCurCompSaleMultiple() != null) {
							precomDTOpre.setCurCompSaleMultiple(prrecom.getCurCompSaleMultiple());
						}
						if (precomDTOpre.getCurCompSalePrice() == null && prrecom.getCurCompSalePrice() != null) {
							precomDTOpre.setCurCompSalePrice(prrecom.getCurCompSalePrice());
						}
						if (precomDTOpre.getCompLocationTypesId() == null && prrecom.getCompLocationTypesId() != null) {
							precomDTOpre.setCompLocationTypesId(prrecom.getCompLocationTypesId());
						}
						if (precomDTOpre.getCompStrId() == null && prrecom.getCompStrId() != null) {
							precomDTOpre.setCompStrId(prrecom.getCompStrId());
						}
						if (precomDTOpre.getCompPriceCheckDate() == null && prrecom.getCompPriceCheckDate() != null) {
							precomDTOpre.setCompPriceCheckDate(prrecom.getCompPriceCheckDate());
						}
						if (precomDTOpre.getRecWeekSaleCost() == null && prrecom.getRecWeekSaleCost() != null) {
							precomDTOpre.setRecWeekSaleCost(prrecom.getRecWeekSaleCost());
						}

						// PP:: 3-Jul-2017 :: setting the most occurring prediction confidence value
						/*
						 * Integer commonPredConfidence = 1; Integer highestFrequency = 0;
						 * if(!predConfidenceFrequency.isEmpty()){ for(Integer confidenceValue:
						 * predConfidenceFrequency.keySet()){
						 * if(predConfidenceFrequency.get(confidenceValue) > highestFrequency){
						 * highestFrequency = predConfidenceFrequency.get(confidenceValue);
						 * commonPredConfidence = confidenceValue; } } }
						 * precomDTOpre.setRecRegPricePredConfidence(commonPredConfidence);
						 * predConfidenceFrequency = new HashMap<Integer, Integer>();
						 */
						// End of changes for not null values

						// Questionable Pred reason consolidation

						if (commonRegPriceReson.length() > 0) {
							String[] finalRegPriceReasons = commonRegPriceReson.split(",");
							HashSet<String> regPriceReasonsMap = new HashSet<String>();

							for (String reason : finalRegPriceReasons) {
								if (reason.trim().length() > 0) {
									regPriceReasonsMap.add(reason.trim());
								}
							}
							commonRegPriceReson = regPriceReasonsMap.toString();
							commonRegPriceReson = commonRegPriceReson.toString().replace(" ", "");
							commonRegPriceReson = commonRegPriceReson.substring(1, commonRegPriceReson.length() - 1);

							precomDTOpre.setRegPricePredReasons(commonRegPriceReson);
						}

						if (commonSalePriceReson.length() > 0) {
							String[] finalSalePriceReasons = commonSalePriceReson.split(",");
							HashSet<String> salePriceReasonsMap = new HashSet<String>();

							for (String reason : finalSalePriceReasons) {
								if (reason.trim().length() > 0) {
									salePriceReasonsMap.add(reason.trim());
								}
							}
							commonSalePriceReson = salePriceReasonsMap.toString();
							commonSalePriceReson = commonSalePriceReson.toString().replace(" ", "");
							commonSalePriceReson = commonSalePriceReson.substring(1, commonSalePriceReson.length() - 1);

							precomDTOpre.setSalePricePredReasons(commonSalePriceReson);
						}

						logger.debug("processConsolidation() : Aggregation of predicted values and rank");
						// Aggregating predicted movement
						if (precomDTOpre.getPredictedMovement() != null || prrecom.getPredictedMovement() != null) {
							if (precomDTOpre.getPredictedMovement() != null && prrecom.getPredictedMovement() != null) {
								precomDTOpre.setPredictedMovement(precomDTOpre.getPredictedMovement().doubleValue()
										+ prrecom.getPredictedMovement().doubleValue());
							}
							if (precomDTOpre.getPredictedMovement() == null) { // if existing value is null
								precomDTOpre.setPredictedMovement(prrecom.getPredictedMovement());
							}
						}

						// Aggregating REC_WEEK_SALE_PRED_AT_REC_REG
						if (precomDTOpre.getRecWeekSalePredAtRecReg() != null
								|| prrecom.getRecWeekSalePredAtRecReg() != null) {
							if (precomDTOpre.getRecWeekSalePredAtRecReg() != null
									&& prrecom.getRecWeekSalePredAtRecReg() != null) {
								precomDTOpre.setRecWeekSalePredAtRecReg(
										precomDTOpre.getRecWeekSalePredAtRecReg().doubleValue()
												+ prrecom.getRecWeekSalePredAtRecReg().doubleValue());
							}
							if (precomDTOpre.getRecWeekSalePredAtRecReg() == null) { // if existing value is null
								precomDTOpre.setRecWeekSalePredAtRecReg(prrecom.getRecWeekSalePredAtRecReg());
							}
						}

						// Aggregating REC_WEEK_SALE_PRED_AT_CUR_REG
						if (precomDTOpre.getRecWeekSalePredAtCurReg() != null
								|| prrecom.getRecWeekSalePredAtCurReg() != null) {
							if (precomDTOpre.getRecWeekSalePredAtCurReg() != null
									&& prrecom.getRecWeekSalePredAtCurReg() != null) {
								precomDTOpre.setRecWeekSalePredAtCurReg(
										precomDTOpre.getRecWeekSalePredAtCurReg().doubleValue()
												+ prrecom.getRecWeekSalePredAtCurReg().doubleValue());
							}
							if (precomDTOpre.getRecWeekSalePredAtCurReg() == null) { // if existing value is null
								precomDTOpre.setRecWeekSalePredAtCurReg(prrecom.getRecWeekSalePredAtCurReg());
							}
						}

						// Aggregating REC_WEEK_SALE_PRED_STATUS_CUR -- getting a numeric value

						if (precomDTOpre.getRecWeekSalePredStatusCur() == null
								&& prrecom.getRecWeekSalePredStatusCur() != null) {
							precomDTOpre.setRecWeekSalePredStatusCur(prrecom.getRecWeekSalePredStatusCur());
						}

						// Aggregating RANK
						if (precomDTOpre.getRank() != null || prrecom.getRank() != null) {
							if (precomDTOpre.getRank() != null && prrecom.getRank() != null) {
								precomDTOpre.setRank(precomDTOpre.getRank().intValue() + prrecom.getRank().intValue());
							}
							if (precomDTOpre.getRank() == null) { // if existing value is null
								precomDTOpre.setRank(prrecom.getRank());
							}
						}

						// Aggregating REC_WEEK_SALE_PRED_STATUS_REC -- getting a numeric value

						if (precomDTOpre.getRecWeekSalePredStatusRec() == null
								&& prrecom.getRecWeekSalePredStatusRec() != null) {
							precomDTOpre.setRecWeekSalePredStatusRec(prrecom.getRecWeekSalePredStatusRec());
						}

						logger.debug("processConsolidation() : Aggregation of sales, margin and movement.");
						// Further aggregation of CUR_SALES_D, CUR_MARGIN_D, CUR_REG_PRICE_PREDICTED_MOV

						if (precomDTOpre.getCurSalesD() != null || prrecom.getCurSalesD() != null) {
							if (precomDTOpre.getCurSalesD() != null && prrecom.getCurSalesD() != null) {
								precomDTOpre.setCurSalesD(precomDTOpre.getCurSalesD().doubleValue()
										+ prrecom.getCurSalesD().doubleValue());
							}
							if (precomDTOpre.getCurSalesD() == null) { // if existing value is null
								precomDTOpre.setCurSalesD(prrecom.getCurSalesD());
							}
						}

						if (precomDTOpre.getCurMarginD() != null || prrecom.getCurMarginD() != null) {
							if (precomDTOpre.getCurMarginD() != null && prrecom.getCurMarginD() != null) {
								precomDTOpre.setCurMarginD(precomDTOpre.getCurMarginD().doubleValue()
										+ prrecom.getCurMarginD().doubleValue());
							}
							if (precomDTOpre.getCurMarginD() == null) { // if existing value is null
								precomDTOpre.setCurMarginD(prrecom.getCurMarginD());
							}
						}

						if (precomDTOpre.getCurRegPricePredictedMov() != null
								|| prrecom.getCurRegPricePredictedMov() != null) {
							if (precomDTOpre.getCurRegPricePredictedMov() != null
									&& prrecom.getCurRegPricePredictedMov() != null) {
								if (precomDTOpre.getCurRegPricePredictedMov().doubleValue() > (double) 0.0
										&& prrecom.getCurRegPricePredictedMov().doubleValue() > (double) 0.0) {
									precomDTOpre.setCurRegPricePredictedMov(
											precomDTOpre.getCurRegPricePredictedMov().doubleValue()
													+ prrecom.getCurRegPricePredictedMov().doubleValue());
								}
								if (precomDTOpre.getCurRegPricePredictedMov().doubleValue() <= (double) 0.0
										&& prrecom.getCurRegPricePredictedMov().doubleValue() > (double) 0.0) {
									precomDTOpre.setCurRegPricePredictedMov(
											prrecom.getCurRegPricePredictedMov().doubleValue());
								}
							}
							if (precomDTOpre.getCurRegPricePredictedMov() == null) { // if existing value is null
								precomDTOpre.setCurRegPricePredictedMov(prrecom.getCurRegPricePredictedMov());
							}
						}

						if (precomDTOpre.getRecSalesD() != null || prrecom.getRecSalesD() != null) {
							if (precomDTOpre.getRecSalesD() != null && prrecom.getRecSalesD() != null) {
								precomDTOpre.setRecSalesD(precomDTOpre.getRecSalesD().doubleValue()
										+ prrecom.getRecSalesD().doubleValue());
							}
							if (precomDTOpre.getRecSalesD() == null) { // if existing value is null
								precomDTOpre.setRecSalesD(prrecom.getRecSalesD());
							}
						}

						if (precomDTOpre.getRecMarginD() != null || prrecom.getRecMarginD() != null) {
							if (precomDTOpre.getRecMarginD() != null && prrecom.getRecMarginD() != null) {
								precomDTOpre.setRecMarginD(precomDTOpre.getRecMarginD().doubleValue()
										+ prrecom.getRecMarginD().doubleValue());
							}
							if (precomDTOpre.getRecMarginD() == null) { // if existing value is null
								precomDTOpre.setRecMarginD(prrecom.getRecMarginD());
							}
						}

						if (precomDTOpre.getCurRegPrice() == null && prrecom.getCurRegPrice() != null) {
							precomDTOpre.setCurRegPrice(prrecom.getCurRegPrice());
						}

						// End of further aggregation

					} else {
						prItemWiseoutput.add(precomDTOpre);
						precomDTOpre = prrecom;
						isCommon = true;
					}
					if (recordCount == runIdSeq.size()) { // adding last record
						prItemWiseoutput.add(precomDTOpre);
					}

				}

				if (prItemWiseoutput != null && prItemWiseoutput.size() > 0) {
					prItemWiseoutput = updateStartAndEndDates(prItemWiseoutput, sortedStartDates, calendarList,
							runIdsList);

					if (prItemWiseoutput != null && prItemWiseoutput.size() > 0) {

						logger.debug("processConsolidation() : Sales Dollar and Margin Dollar calculation initiated");
						for (PRRecomDTO prRecomAgr : prItemWiseoutput) {
//
//							//Sales Dollar and Margin Dollar calculation
//							
//							Double salesDollar = (double) 0.0;
//							Double marginDollar = (double) 0.0;
//							
//
//								if (prRecomAgr.getRecommendedWeekSalePrice() != null && 
//										prRecomAgr.getRecommendedWeekSalePrice().price.doubleValue() > (double)0.0){
//																
//									salesDollar = PRCommonUtil.getSalesDollar(prRecomAgr.getRecommendedWeekSalePrice(),
//											prRecomAgr.getPredictedMovement() );
//									
//									if (prRecomAgr.getRecWeekSaleCost() != null && 
//											prRecomAgr.getRecWeekSaleCost().doubleValue() > (double)0.0){
//									
//										marginDollar = PRCommonUtil.getMarginDollar(prRecomAgr.getRecommendedWeekSalePrice(), 
//												prRecomAgr.getRecWeekSaleCost(), prRecomAgr.getPredictedMovement());
//									}
//									else{
//										marginDollar = PRCommonUtil.getMarginDollar(prRecomAgr.getRecommendedWeekSalePrice(), 
//												prRecomAgr.getCurrentListCost(), prRecomAgr.getPredictedMovement());
//									}
//								}
//								else{
//									if(prRecomAgr.getRecommendedRegPrice()!=null && 
//											prRecomAgr.getRecommendedRegPrice().price.doubleValue() > (double)0.0){
//								
//										salesDollar = PRCommonUtil.getSalesDollar(prRecomAgr.getRecommendedRegPrice(),
//												prRecomAgr.getPredictedMovement() );
//										
//										marginDollar = PRCommonUtil.getMarginDollar(prRecomAgr.getRecommendedRegPrice(), 
//												prRecomAgr.getCurrentListCost(), prRecomAgr.getPredictedMovement());
//									}
//								}
//								
//								prRecomAgr.setRecSalesD(salesDollar);
//								prRecomAgr.setRecMarginD(marginDollar);	
//								
//								
							prItemWiseAggroutput.add(prRecomAgr);
						}

						PRRecomDTO finalConsolidatedRecord = null;

						int recCount = 0;
						String finalRegConsolidatedReasons = "";
						String finalSaleConsolidatedReasons = "";
						logger.debug(
								"processConsolidation() : Creating final consolidated record for single line aggregate display.");
						for (PRRecomDTO consolRecord : prItemWiseAggroutput) {
							recCount++;
							if (finalConsolidatedRecord == null) {
								finalConsolidatedRecord = new PRRecomDTO(consolRecord);
								finalConsolidatedRecord.setFinalConsolidated(true);
								continue;
							} else {

								if (recCount == prItemWiseAggroutput.size()) {
									finalConsolidatedRecord.setRecoEndDate(consolRecord.getRecoEndDate());
								}

								// current Regular Price predicted movement
								if (finalConsolidatedRecord.getCurRegPricePredictedMov() != null
										&& consolRecord.getCurRegPricePredictedMov() != null) {
									finalConsolidatedRecord
											.setCurRegPricePredictedMov(consolRecord.getCurRegPricePredictedMov()
													+ finalConsolidatedRecord.getCurRegPricePredictedMov());
								}
								if (finalConsolidatedRecord.getCurRegPricePredictedMov() == null
										&& consolRecord.getCurRegPricePredictedMov() != null) {
									finalConsolidatedRecord
											.setCurRegPricePredictedMov(consolRecord.getCurRegPricePredictedMov());
								}

								// current Margin Dollar
								if (finalConsolidatedRecord.getCurMarginD() != null
										&& consolRecord.getCurMarginD() != null) {
									finalConsolidatedRecord.setCurMarginD(
											consolRecord.getCurMarginD() + finalConsolidatedRecord.getCurMarginD());
								}
								if (finalConsolidatedRecord.getCurMarginD() == null
										&& consolRecord.getCurMarginD() != null) {
									finalConsolidatedRecord.setCurMarginD(consolRecord.getCurMarginD());
								}

								// current Sales Dollar
								if (finalConsolidatedRecord.getCurSalesD() != null
										&& consolRecord.getCurSalesD() != null) {
									finalConsolidatedRecord.setCurSalesD(
											consolRecord.getCurSalesD() + finalConsolidatedRecord.getCurSalesD());
								}
								if (finalConsolidatedRecord.getCurSalesD() == null
										&& consolRecord.getCurSalesD() != null) {
									finalConsolidatedRecord.setCurSalesD(consolRecord.getCurSalesD());
								}

								// RecWeekSalePredAtRecReg
								if (finalConsolidatedRecord.getRecWeekSalePredAtRecReg() != null
										&& consolRecord.getRecWeekSalePredAtRecReg() != null) {
									finalConsolidatedRecord
											.setRecWeekSalePredAtRecReg(consolRecord.getRecWeekSalePredAtRecReg()
													+ finalConsolidatedRecord.getRecWeekSalePredAtRecReg());
								}
								if (finalConsolidatedRecord.getRecWeekSalePredAtRecReg() == null
										&& consolRecord.getRecWeekSalePredAtRecReg() != null) {
									finalConsolidatedRecord
											.setRecWeekSalePredAtRecReg(consolRecord.getRecWeekSalePredAtRecReg());
								}

								// RecWeekSalePredAtCurReg
								if (finalConsolidatedRecord.getRecWeekSalePredAtCurReg() != null
										&& consolRecord.getRecWeekSalePredAtCurReg() != null) {
									finalConsolidatedRecord
											.setRecWeekSalePredAtCurReg(consolRecord.getRecWeekSalePredAtCurReg()
													+ finalConsolidatedRecord.getRecWeekSalePredAtCurReg());
								}
								if (finalConsolidatedRecord.getRecWeekSalePredAtCurReg() == null
										&& consolRecord.getRecWeekSalePredAtCurReg() != null) {
									finalConsolidatedRecord
											.setRecWeekSalePredAtCurReg(consolRecord.getRecWeekSalePredAtCurReg());
								}

								// predicted movement
								if (finalConsolidatedRecord.getPredictedMovement() != null
										&& consolRecord.getPredictedMovement() != null) {
									finalConsolidatedRecord.setPredictedMovement(consolRecord.getPredictedMovement()
											+ finalConsolidatedRecord.getPredictedMovement());
								}
								if (finalConsolidatedRecord.getPredictedMovement() == null
										&& consolRecord.getPredictedMovement() != null) {
									finalConsolidatedRecord.setPredictedMovement(consolRecord.getPredictedMovement());
								}

								// sales dollar
								if (finalConsolidatedRecord.getRecSalesD() != null
										&& consolRecord.getRecSalesD() != null) {
									finalConsolidatedRecord.setRecSalesD(
											consolRecord.getRecSalesD() + finalConsolidatedRecord.getRecSalesD());
								}
								if (finalConsolidatedRecord.getRecSalesD() == null
										&& consolRecord.getRecSalesD() != null) {
									finalConsolidatedRecord.setRecSalesD(consolRecord.getRecSalesD());
								}

								// margin dollar
								if (finalConsolidatedRecord.getRecMarginD() != null
										&& consolRecord.getRecMarginD() != null) {
									finalConsolidatedRecord.setRecMarginD(
											consolRecord.getRecMarginD() + finalConsolidatedRecord.getRecMarginD());
								}
								if (finalConsolidatedRecord.getRecMarginD() == null
										&& consolRecord.getRecMarginD() != null) {
									finalConsolidatedRecord.setRecMarginD(consolRecord.getRecMarginD());
								}

								// Rank
								if (finalConsolidatedRecord.getRank() != null && consolRecord.getRank() != null) {
									finalConsolidatedRecord
											.setRank(consolRecord.getRank() + finalConsolidatedRecord.getRank());
								}
								if (finalConsolidatedRecord.getRank() == null && consolRecord.getRank() != null) {
									finalConsolidatedRecord.setRank(consolRecord.getRank());
								}

								// Prediction Confidence most common occurrence changes
								/*
								 * if(consPredConfidenceFrequency.get(precomDTOpre.getRecRegPricePredConfidence(
								 * )) != null ){ Integer frequency =
								 * consPredConfidenceFrequency.get(precomDTOpre.getRecRegPricePredConfidence())
								 * + 1;
								 * consPredConfidenceFrequency.put(precomDTOpre.getRecRegPricePredConfidence(),
								 * frequency); } else {
								 * consPredConfidenceFrequency.put(precomDTOpre.getRecRegPricePredConfidence(),
								 * 1); }
								 */
								if (consolRecord.getRegPricePredReasons() != null
										&& consolRecord.getRegPricePredReasons().length() > 0) {
									finalRegConsolidatedReasons = finalRegConsolidatedReasons + ","
											+ consolRecord.getRegPricePredReasons();
								}
								if (consolRecord.getSalePricePredReasons() != null
										&& consolRecord.getSalePricePredReasons().length() > 0) {
									finalSaleConsolidatedReasons = finalSaleConsolidatedReasons + ","
											+ consolRecord.getSalePricePredReasons();
								}

								if (commonSalePriceReson.length() > 0) {
									String[] finalSalePriceReasons = commonSalePriceReson.split(",");
									HashSet<String> SalePriceReasonsMap = new HashSet<String>();

									for (String reason : finalSalePriceReasons) {
										if (reason.trim().length() > 0) {
											SalePriceReasonsMap.add(reason.trim());
										}
									}
									commonSalePriceReson = commonSalePriceReson.toString().replace(" ", "");
									commonSalePriceReson = commonSalePriceReson.substring(1,
											commonSalePriceReson.length() - 1);

									precomDTOpre.setSalePricePredReasons(commonSalePriceReson);
								}

							}

						}

						/*
						 * //PP:: 3-Jul-2017 :: setting the most occurring prediction confidence value
						 * Integer consCommonPredConfidence = 1; Integer consHighestFrequency = 0;
						 * if(!predConfidenceFrequency.isEmpty()){ for(Integer confidenceValue:
						 * consPredConfidenceFrequency.keySet()){
						 * if(predConfidenceFrequency.get(confidenceValue) > consHighestFrequency){
						 * consHighestFrequency = predConfidenceFrequency.get(confidenceValue);
						 * consCommonPredConfidence = confidenceValue; } } }
						 * finalConsolidatedRecord.setRecRegPricePredConfidence(consCommonPredConfidence
						 * ); consPredConfidenceFrequency = new HashMap<Integer, Integer>();
						 */

						// Processing questionable reasons

						if (finalRegConsolidatedReasons.length() > 0) {
							String[] finalConsRegPriceReasons = finalRegConsolidatedReasons.split(",");
							HashSet<String> consRegPriceReasonsMap = new HashSet<String>();

							for (String reason : finalConsRegPriceReasons) {
								if (reason.trim().length() > 0) {
									consRegPriceReasonsMap.add(reason.trim());
								}
							}
							finalRegConsolidatedReasons = consRegPriceReasonsMap.toString();
							finalRegConsolidatedReasons = finalRegConsolidatedReasons.toString().replace(" ", "");
							finalRegConsolidatedReasons = finalRegConsolidatedReasons.substring(1,
									finalRegConsolidatedReasons.length() - 1);

							finalConsolidatedRecord.setRegPricePredReasons(finalRegConsolidatedReasons);
						}

						if (finalSaleConsolidatedReasons.length() > 0) {
							String[] finalConsSalePriceReasons = finalSaleConsolidatedReasons.split(",");
							HashSet<String> consSalePriceReasonsMap = new HashSet<String>();

							for (String reason : finalConsSalePriceReasons) {
								if (reason.trim().length() > 0) {
									consSalePriceReasonsMap.add(reason.trim());
								}
							}
							finalSaleConsolidatedReasons = consSalePriceReasonsMap.toString();
							finalSaleConsolidatedReasons = finalSaleConsolidatedReasons.toString().replace(" ", "");
							finalSaleConsolidatedReasons = finalSaleConsolidatedReasons.substring(1,
									finalSaleConsolidatedReasons.length() - 1);

							finalConsolidatedRecord.setSalePricePredReasons(finalSaleConsolidatedReasons);
						}

						prItemWiseAggroutput.add(finalConsolidatedRecord);

						prRecomDTOOutputList.addAll(prItemWiseAggroutput);
					}
				}

			}

			logger.debug("processConsolidation() : Consolidation process completed successfully");

		} catch (Exception ex) {
			logger.error("Error in processConsolidation() -- " + ex.toString(), ex);
			throw new GeneralException("Error in processConsolidation() - " + ex.toString());
		}

		logger.debug("End of processConsolidation()");
		return prRecomDTOOutputList;
	}

	/**
	 * Saving consolidation data in database
	 * 
	 * @param conn
	 * @param calendarId
	 * @param locationLevelId
	 * @param locationId
	 * @param productId
	 * @param productLevelId
	 * @param consolidateData
	 * @throws GeneralException
	 */
	public void saveConsolidatedData(Connection conn, int calendarId, int locationLevelId, int locationId,
			int productId, int productLevelId, List<PRRecomDTO> consolidateData) throws GeneralException {

		try {

			logger.debug("Start of saveConsolidatedData()");

			PRStrategyDTO prstrategyDTO = new PRStrategyDTO();
			char runType = 'B';
			prstrategyDTO.setRunType(runType);
			prstrategyDTO.setLocationId(locationId);
			prstrategyDTO.setLocationLevelId(locationLevelId);
			prstrategyDTO.setProductId(productId);
			prstrategyDTO.setProductLevelId(productLevelId);
			prstrategyDTO.setPredictedBy("CONSOLIDATED");

			long runIdHeader = insertRecommendationHeader(conn, calendarId, prstrategyDTO);

			insertRecommendationItems(conn, consolidateData, runIdHeader);

//			updatePRDashboard(conn, runIdHeader, locationLevelId, locationId, 
//				 productId , productLevelId);

			logger.debug("Consolidated Data saved successfully");

		} catch (Exception exception) {
			logger.error("Error in saveConsolidatedData() - " + exception.toString());
			throw new GeneralException("Error in processConsolidation() - " + exception.toString());
		}
		logger.debug("End of saveConsolidatedData()");
	}

	/**
	 * Inserting consolidated recommendation records in pr_recommmendation
	 * 
	 * @param conn
	 * @param listPRRecmDTO
	 * @param runId
	 * @throws GeneralException
	 */
	private void insertRecommendationItems(Connection conn, List<PRRecomDTO> listPRRecmDTO, long runId)
			throws GeneralException {

		PreparedStatement stmt = null;

		try {
			logger.debug("Start of insertRecommendationItems()");

			for (PRRecomDTO conRecom : listPRRecmDTO) {
				int counter = 0;

				stmt = conn.prepareStatement(INSERT_RECOMMENDATION_ITEM);
				stmt.setLong(++counter, runId);
				stmt.setString(++counter, conRecom.getRecoStartDate());
				stmt.setString(++counter, conRecom.getRecoEndDate());

				if (conRecom.getPredictedMovement() != null && conRecom.getPredictedMovement() > 0) {
					conRecom.setRecPricePredStatus(PredictionStatus.SUCCESS.getStatusCode());
				}

				stmt.setInt(++counter, conRecom.getRecPricePredStatus());

				if (conRecom.isFinalConsolidated()) {
					stmt.setString(++counter, "N");
				} else {
					stmt.setString(++counter, "Y");
				}

				if (conRecom.getPredictedMovement() != null && conRecom.getPredictedMovement() > 0) {
					stmt.setDouble(++counter, conRecom.getPredictedMovement());
				} else {
					stmt.setNull(++counter, java.sql.Types.DOUBLE);
				}

				if (conRecom.getRecSalesD() != null) {
					stmt.setDouble(++counter, conRecom.getRecSalesD());
				} else {
					stmt.setNull(++counter, java.sql.Types.DOUBLE);
				}

				if (conRecom.getRecMarginD() != null) {
					stmt.setDouble(++counter, conRecom.getRecMarginD());
				} else {
					stmt.setNull(++counter, java.sql.Types.DOUBLE);
				}

				if (conRecom.getRecWeekSalePredAtRecReg() != null) {
					stmt.setDouble(++counter, conRecom.getRecWeekSalePredAtRecReg());
				} else {
					stmt.setNull(++counter, java.sql.Types.DOUBLE);
				}

				if (conRecom.getRecWeekSalePredStatusRec() != null) {
					stmt.setDouble(++counter, conRecom.getRecWeekSalePredStatusRec());
				} else {
					stmt.setNull(++counter, java.sql.Types.INTEGER);
				}

				// for not null values

				if (conRecom.getCurCompRegMultiple() != null) {
					stmt.setInt(++counter, conRecom.getCurCompRegMultiple());
				} else {
					stmt.setNull(++counter, java.sql.Types.INTEGER);
				}

				if (conRecom.getCurCompRegPrice() != null) {
					stmt.setDouble(++counter, conRecom.getCurCompRegPrice());
				} else {
					stmt.setNull(++counter, java.sql.Types.DOUBLE);
				}

				if (conRecom.getCurCompSaleMultiple() != null) {
					stmt.setInt(++counter, conRecom.getCurCompSaleMultiple());
				} else {
					stmt.setNull(++counter, java.sql.Types.INTEGER);
				}

				if (conRecom.getCurCompSalePrice() != null) {
					stmt.setDouble(++counter, conRecom.getCurCompSalePrice());
				} else {
					stmt.setNull(++counter, java.sql.Types.DOUBLE);
				}

				if (conRecom.getCompLocationTypesId() != null) {
					stmt.setInt(++counter, conRecom.getCompLocationTypesId());
				} else {
					stmt.setNull(++counter, java.sql.Types.INTEGER);
				}

				if (conRecom.getCompStrId() != null) {
					stmt.setInt(++counter, conRecom.getCompStrId());
				} else {
					stmt.setNull(++counter, java.sql.Types.INTEGER);
				}

				if (conRecom.getCompPriceCheckDate() != null) {
					stmt.setDate(++counter, conRecom.getCompPriceCheckDate());
				} else {
					stmt.setNull(++counter, java.sql.Types.DATE);
				}

				if (conRecom.getCurSalesD() != null) {
					stmt.setDouble(++counter, conRecom.getCurSalesD());
				} else {
					stmt.setNull(++counter, java.sql.Types.DOUBLE);
				}

				if (conRecom.getCurMarginD() != null) {
					stmt.setDouble(++counter, conRecom.getCurMarginD());
				} else {
					stmt.setNull(++counter, java.sql.Types.DOUBLE);
				}

				if (conRecom.getCurRegPricePredictedMov() != null) {
					stmt.setDouble(++counter, conRecom.getCurRegPricePredictedMov());
				} else {
					stmt.setNull(++counter, java.sql.Types.DOUBLE);
				}

				if (conRecom.getCurRegPrice() != null) {
					if (conRecom.getCurRegPrice().price != null) {
						stmt.setDouble(++counter, conRecom.getCurRegPrice().price);
					} else {
						stmt.setNull(++counter, java.sql.Types.DOUBLE);
					}

					if (conRecom.getCurRegPrice().multiple != null) {
						stmt.setDouble(++counter, conRecom.getCurRegPrice().multiple);
					} else {
						stmt.setNull(++counter, java.sql.Types.INTEGER);
					}
				} else {
					stmt.setNull(++counter, java.sql.Types.DOUBLE);
					stmt.setNull(++counter, java.sql.Types.INTEGER);
				}

				if (conRecom.isFinalConsolidated()) {
					stmt.setString(++counter, "Y");
				} else {
					stmt.setString(++counter, "N");
				}

				if (conRecom.getRecWeekSaleCost() != null) {
					stmt.setDouble(++counter, conRecom.getRecWeekSaleCost());
				} else {
					stmt.setNull(++counter, java.sql.Types.DOUBLE);
				}

				if (conRecom.getRank() != null) {
					stmt.setInt(++counter, conRecom.getRank());
				} else {
					stmt.setNull(++counter, java.sql.Types.INTEGER);
				}

				if (conRecom.getRecWeekSalePredAtCurReg() != null) {
					stmt.setDouble(++counter, conRecom.getRecWeekSalePredAtCurReg());
				} else {
					stmt.setNull(++counter, java.sql.Types.DOUBLE);
				}

				if (conRecom.getRecWeekSalePredStatusCur() != null) {
					stmt.setInt(++counter, conRecom.getRecWeekSalePredStatusCur());
				} else {
					stmt.setNull(++counter, java.sql.Types.INTEGER);
				}

				if (conRecom.getRegPricePredReasons() != null) {
					stmt.setString(++counter, conRecom.getRegPricePredReasons());
				} else {
					stmt.setNull(++counter, java.sql.Types.VARCHAR);
				}

				if (conRecom.getSalePricePredReasons() != null) {
					stmt.setString(++counter, conRecom.getSalePricePredReasons());
				} else {
					stmt.setNull(++counter, java.sql.Types.VARCHAR);
				}

				// end of changes

				stmt.setLong(++counter, conRecom.getRecommendationId());

				stmt.executeUpdate();
				PristineDBUtil.close(stmt);
			}

			logger.debug("Consolidated recommendation is saved in PR_recommendation successfully)");

		} catch (SQLException exception) {
			logger.error("SQL Error in insertRecommendationItems() - " + exception.toString());
			throw new GeneralException("SQL Error in insertRecommendationItems() - " + exception.toString());
		} catch (Exception exception) {
			logger.error("Error in insertRecommendationItems() - " + exception.toString());
			throw new GeneralException("Error in insertRecommendationItems() - " + exception.toString());
		} finally {
			PristineDBUtil.close(stmt);
		}

		logger.debug("End of insertRecommendationItems()");

	}

	/**
	 * Sorting the input start dates in ascending order
	 * 
	 * @param inputDateList
	 * @return TreeMap of Dates and String form of input dates
	 * @throws GeneralException
	 */
	public TreeMap<Date, String> getSortedDate(String[] inputDateList) throws GeneralException {

		logger.debug("Start of getSortedDate()");

		TreeMap<Date, String> sortedDates = new TreeMap<Date, String>();

		try {
			SimpleDateFormat sdf = new SimpleDateFormat("MM/dd/yyyy");

			for (String date : inputDateList) {
				sortedDates.put(sdf.parse(date), date);
			}
		} catch (Exception exception) {
			logger.error("Error in getSortedDate() -- " + exception.toString(), exception);
			throw new GeneralException("Error in getSortedDate() - " + exception.toString());
		}
		logger.debug("End of getSortedDate()");
		return sortedDates;
	}

	/**
	 * Retrieve next calendar id of maximum calendar id in input
	 * 
	 * @param conn
	 * @param startDate
	 * @return
	 * @throws GeneralException
	 */
	public int getNextCalendarId(Connection conn, String startDate) throws GeneralException {

		PreparedStatement statement = null;
		ResultSet resultSet = null;
		int calendarId = 0;
		try {
			logger.debug("Start of getNextCalendarId()");

			statement = conn.prepareStatement(GET_NEXT_CALENDAR);
			statement.setString(1, startDate);

			resultSet = statement.executeQuery();
			if (resultSet.next()) {
				calendarId = resultSet.getInt("CALENDAR_ID");
			}
			logger.debug("Next calendar id retrieved successfuly.");
		} catch (SQLException e) {
			logger.error("Error while executing getNextCalendarId");
			throw new GeneralException("Error while executing getNextCalendarId", e);
		} finally {
			PristineDBUtil.close(resultSet);
			PristineDBUtil.close(statement);
		}
		logger.debug("End of getNextCalendarId()");
		return calendarId;
	}

	/**
	 * Inserting the Consolidation record in pr_recommendation_header
	 * 
	 * @param conn
	 * @param calendarId
	 * @param strategy
	 * @return
	 * @throws GeneralException
	 */
	private long insertRecommendationHeader(Connection conn, int calendarId, PRStrategyDTO strategy)
			throws GeneralException {

		PreparedStatement stmt = null;
		ResultSet rs = null;
		long runId = -1;
		logger.debug("Start of insertRecommendationHeader()");
		try {
			stmt = conn.prepareStatement(GET_RECOMMENDATION_RUN_ID);
			rs = stmt.executeQuery();
			if (rs.next()) {
				runId = rs.getLong("RUN_ID");
			}
		} catch (SQLException exception) {
			logger.error("Error when retrieving recommendation run id - " + exception.toString());
			throw new GeneralException("Error when retrieving recommendation run id - " + exception.toString());
		} finally {
			PristineDBUtil.close(rs);
			PristineDBUtil.close(stmt);
		}

		try {
			stmt = conn.prepareStatement(INSERT_RECOMMENDATION_HEADER);
			int counter = 0;
			stmt.setLong(++counter, runId);
			stmt.setInt(++counter, calendarId);
			stmt.setInt(++counter, strategy.getLocationLevelId());
			stmt.setInt(++counter, strategy.getLocationId());
			stmt.setInt(++counter, strategy.getProductLevelId());
			stmt.setInt(++counter, strategy.getProductId());
			stmt.setString(++counter, strategy.getPredictedBy());
			stmt.executeUpdate();
			logger.debug("RecommendationHeader updated successfully");
		} catch (SQLException exception) {
			logger.error("Error when inserting recommendation header - " + exception.toString());
			throw new GeneralException("Error when inserting recommendation header - " + exception.toString());
		} finally {
			PristineDBUtil.close(rs);
			PristineDBUtil.close(stmt);
		}
		logger.debug("End of insertRecommendationHeader()");
		return runId;
	}

	/**
	 * Sorting RunIds in ascending order of calendar
	 * 
	 * @param sortedStartDates
	 * @param calendarList
	 * @param runIdsList
	 * @return Tree Map of Calendar as Key and Run Id as Value
	 * @throws Gene
	 */
	private TreeMap<Date, Long> getRunIdSeq(TreeMap<Date, String> sortedStartDates,
			ArrayList<RetailCalendarDTO> calendarList, HashMap<Integer, Long> runIdsList) throws GeneralException {

		TreeMap<Date, Long> sortedRunId = new TreeMap<Date, Long>();
		try {

			for (Date d : sortedStartDates.keySet()) {
				for (RetailCalendarDTO rdto : calendarList) {
					if (rdto.getStartDate().equals(sortedStartDates.get(d))
							&& runIdsList.containsKey(rdto.getCalendarId())) {
						sortedRunId.put(d, runIdsList.get(rdto.getCalendarId()));
					}
				}
			}
		} catch (Exception exception) {
			logger.error("Error in sorting runIds calendarwise - " + exception.toString());
			throw new GeneralException("Error in sorting runIds calendarwise - " + exception.toString());
		}

		return sortedRunId;

	}

	/**
	 * Updating start and end dates for consolidated recommendation records for each
	 * item
	 * 
	 * @param outputPrRecomList
	 * @param sortedStartDates
	 * @param calendarList
	 * @param runIdsList
	 * @return ArrayList of consolidated recommendation
	 * @throws GeneralException
	 */
	private ArrayList<PRRecomDTO> updateStartAndEndDates(ArrayList<PRRecomDTO> outputPrRecomList,
			TreeMap<Date, String> sortedStartDates, ArrayList<RetailCalendarDTO> calendarList,
			HashMap<Integer, Long> runIdsList) throws GeneralException {

		ArrayList<PRRecomDTO> finalPrRecomList = new ArrayList<PRRecomDTO>();
		PRRecomDTO prevVal = null;
		RetailCalendarDTO prevDTO = null;
		Long currentRunId = 0L;
		int i = 0;
		logger.debug("Start of updateStartAndEndDates()");

		try {
			for (Date d : sortedStartDates.keySet()) {

				for (RetailCalendarDTO rdto : calendarList) {
					if (rdto.getStartDate().equals(sortedStartDates.get(d))
							&& runIdsList.containsKey(rdto.getCalendarId())) {

						currentRunId = runIdsList.get(rdto.getCalendarId());

						for (PRRecomDTO prDTO : outputPrRecomList) {

							if (prDTO.getRunId() == currentRunId) {
								if (i > 0) {
									prevVal.setRecoEndDate(prevDTO.getEndDate());
									finalPrRecomList.add(prevVal);
								}

								prevVal = prDTO;
								prevVal.setRecoStartDate(rdto.getStartDate());
								i++;

								if (i == outputPrRecomList.size()) {
									for (RetailCalendarDTO rdtoin : calendarList) {
										if (rdtoin.getStartDate()
												.equals(sortedStartDates.get(sortedStartDates.lastKey()))) {
											prevVal.setRecoEndDate(rdtoin.getEndDate());
										}
									}
									finalPrRecomList.add(prevVal);
								}

								break;
							}

						}
						prevDTO = rdto;
						break;

					}

				}

			}

		} catch (Exception exception) {
			logger.error("Error in updateStartAndEndDates - " + exception.toString());
			throw new GeneralException("Error in updateStartAndEndDates - " + exception.toString());
		}
		logger.debug("End of updateStartAndEndDates()");
		return finalPrRecomList;

	}

	/**
	 * Updating PR DashBoard for given product id
	 * 
	 * @param conn
	 * @param runId
	 * @param locationLevelId
	 * @param locationId
	 * @param productId
	 * @param productLevelId
	 * @throws GeneralException
	 */
	private void updatePRDashboard(Connection conn, long runId, int locationLevelId, int locationId, int productId,
			int productLevelId) throws GeneralException {

		PreparedStatement stmt = null;
		logger.debug("Start of updatePRDashboard()");
		try {
			stmt = conn.prepareStatement(UPDATE_PR_DASHBOARD);
			stmt.setLong(1, runId);
			stmt.setInt(2, productId);
			stmt.setInt(3, locationLevelId);
			stmt.setInt(4, locationId);
			stmt.setInt(5, productLevelId);

			stmt.executeUpdate();
			logger.debug("PRDashboard updated successfully.");

		} catch (SQLException exception) {
			logger.error("Error when updating run id to pr_dashboard- " + exception.toString());
			throw new GeneralException("Error when updating run id to pr_dashboard - " + exception.toString());
		} finally {
			PristineDBUtil.close(stmt);
		}

		logger.debug("End of updatePRDashboard()");

	}

}
