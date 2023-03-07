package com.pristine.dao.offermgmt.prediction;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
//import java.text.DecimalFormat;
import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

//import org.apache.commons.lang.StringUtils;
import org.apache.log4j.Logger;
import com.pristine.dao.IDAO;
//import com.pristine.dto.offermgmt.PRItemDTO;
//import com.pristine.dto.offermgmt.PRStrategyDTO;
import com.pristine.dto.offermgmt.PRItemDTO;
import com.pristine.dto.offermgmt.prediction.PredictionDetailDTO;
import com.pristine.dto.offermgmt.prediction.PredictionInputDTO;
import com.pristine.dto.offermgmt.prediction.PredictionItemDTO;
import com.pristine.dto.offermgmt.prediction.PredictionRunHeaderDTO;
import com.pristine.dto.offermgmt.prediction.PricePointDTO;
import com.pristine.exception.GeneralException;
import com.pristine.service.offermgmt.ItemKey;
import com.pristine.service.offermgmt.prediction.PredictionDetailKey;
import com.pristine.service.offermgmt.prediction.PredictionEngineValue;
import com.pristine.service.offermgmt.prediction.PredictionExplain;
import com.pristine.service.offermgmt.prediction.PredictionStatus;
import com.pristine.util.Constants;
import com.pristine.util.PristineDBUtil;
import com.pristine.util.offermgmt.PRCommonUtil;
import com.pristine.util.offermgmt.PRConstants;

public class PredictionDAO implements IDAO {
	private static Logger logger = Logger.getLogger("PredictionDAO");

	private static final String GET_PREDICTION_DETAILS = "SELECT PD.* FROM PR_PREDICTION_RUN_HEADER RH "
			+ " JOIN PR_PREDICTION_DETAIL PD ON RH.RUN_ID = PD.RUN_ID "
			+ " WHERE %FILTER_BY_PRODUCT_LEVEL% "
			+ " %PREDICTION_STATUS_FILTER% "		
			+ " RH.START_CALENDAR_ID = ? AND RH.END_CALENDAR_ID = ? "
			//+ " AND PD.LOCATION_LEVEL_ID = ? AND PD.LOCATION_ID = ? "
			+ " %FILTER_BY_ITEM% "
			+ " ORDER BY RH.END_RUN_TIME DESC ";
	
	private static final String GET_PREDICTION_RUN_ID = "SELECT PR_PREDICTION_RUN_HEADER_SEQ.NEXTVAL AS RUN_ID FROM DUAL";

	private static final String INSERT_PREDICTION_RUN_HEADER = "INSERT INTO PR_PREDICTION_RUN_HEADER (RUN_ID, START_RUN_TIME, " +
			" RUN_TYPE, PREDICTED, PREDICTED_BY, START_CALENDAR_ID, END_CALENDAR_ID " +
			//", LOCATION_LEVEL_ID, LOCATION_ID " + 
			") VALUES " +
			"(?, SYSDATE, ?, SYSDATE, ?, ?, ?)" ;
			//", ?, ?)";

	private static final String INSERT_PREDICTION_DETAIL = 
			" INSERT INTO PR_PREDICTION_DETAIL (RUN_ID,LIR_ID_OR_ITEM_CODE, LIR_IND, REG_PRICE, REG_QUANTITY, " +			
			" PREDICTED_MOVEMENT,EXPLAIN_PREDICTION,PREDICTION_STATUS, " +
			" LOCATION_LEVEL_ID, LOCATION_ID, SALE_QUANTITY, SALE_PRICE, AD_PAGE_NO, AD_BLOCK_NO, " +
			" DISPLAY_TYPE_ID, PROMO_TYPE_ID, CONFIDENCE_LEVEL_LOWER, CONFIDENCE_LEVEL_UPPER, QUESTIONABLE_PRED_REASONS " +
			" ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)";
	
	private static final String UPDATE_RUN_AS_COMPLETE = "UPDATE PR_PREDICTION_RUN_HEADER SET END_RUN_TIME = SYSDATE, "
			+ " RUN_STATUS = ?, MESSAGE = 'Prediction Complete', PERCENT_COMPLETION = 100 WHERE RUN_ID = ?";
	
	private static final String UPDATE_RUN_AS_FAILED = "UPDATE PR_PREDICTION_RUN_HEADER SET "
			+ " RUN_STATUS = ? WHERE RUN_ID = ?";
	
	private static final String UPDATE_RUN_HEADER_MSG_PCT = "UPDATE PR_PREDICTION_RUN_HEADER SET "
			+ " MESSAGE = ?, PERCENT_COMPLETION = ? WHERE RUN_ID = ?";

	private static final String GET_ITEMS_TO_BE_PREDICTED = 
			" SELECT PR.RUN_ID, PR.ITEM_CODE, IL.UPC, IL.RET_LIR_ID, IL.LIR_IND, PR.RECOMMENDATION_STORE, PR.CHILD_LOCATION_LEVEL_ID AS ZONE_LEVEL_ID," +
			" PR.CHILD_LOCATION_ID AS ZONE_ID, PR.OVERRIDE_REG_MULTIPLE AS ZONE_OVERRIDE_MULTIPLE, PR.OVERRIDE_REG_PRICE AS ZONE_OVERRIDE_PRICE," +
			" PR.AVG_MOVEMENT AS ZONE_AVG_MOVEMENT, PRS.STORE_ID, PRS.OVERRIDE_REG_MULTIPLE AS STORE_OVERRIDE_MULTIPLE," +
			" PRS.OVERRIDE_REG_PRICE AS STORE_OVERRIDE_PRICE, PRS.AVG_MOVEMENT AS STORE_AVG_MOVEMENT" +
			" FROM PR_RECOMMENDATION PR LEFT JOIN PR_RECOMMENDATION_STORE PRS	ON PR.PR_RECOMMENDATION_ID = PRS.PR_RECOMMENDATION_ID" +
			" LEFT JOIN ITEM_LOOKUP IL ON PR.ITEM_CODE = IL.ITEM_CODE" +
			" WHERE PR.RUN_ID IN (%RUN_IDS%)" +
			" AND ((PR.OVERRIDE_PRED_UPDATE_STATUS = 1 AND PR.OVERRIDE_REG_PRICE IS NOT NULL) OR " +
			" (PRS.OVERRIDE_PRED_UPDATE_STATUS = 1 AND PRS.OVERRIDE_REG_PRICE IS NOT NULL))";
	
	private static final String UPDATE_RECOMMENDATION_HEADER = "UPDATE PR_RECOMMENDATION_RUN_HEADER SET OVERRIDE_PRED_UPDATE_STATUS = 0 " +
			" WHERE RUN_ID = ?";
	
	private static final String UPDATE_RECOMMENDATION = "UPDATE PR_RECOMMENDATION SET OVERRIDE_PRED_UPDATE_STATUS = 0 " +
			" WHERE OVERRIDE_PRED_UPDATE_STATUS = 1 AND RUN_ID = ? AND CHILD_LOCATION_LEVEL_ID = 6 AND CHILD_LOCATION_ID = ?";
	
	private static final String UPDATE_OVERRIDE_ZONE = "UPDATE PR_RECOMMENDATION SET OVERRIDE_PRICE_PREDICTED_MOV = ?, " +
			" OVERRIDE_PRICE_PRED_STATUS = ? WHERE RUN_ID = ? AND ITEM_CODE = ? AND LIR_IND='N'";
	
	private static final String UPDATE_RECOMMENDATION_DATA_ZONE = "UPDATE PR_RECOMMENDATION SET OVERRIDE_PRICE_PREDICTED_MOV = ?, " +
			" OVERRIDE_PRICE_PRED_STATUS = ?, " +
			//" REC_SALES_D = ?, REC_MARGIN_D = ? " +
			" OVERRIDE_SALES_D = ?, OVERRIDE_MARGIN_D = ? " +
			" WHERE PR_RECOMMENDATION_ID = ?";
	
	private static final String GET_OVERRIDE_DATA_ZONE = " SELECT PR_RECOMMENDATION_ID, PR.ITEM_CODE, IL.RET_LIR_ID, PR.LIR_IND, "
			+ " CUR_REG_MULTIPLE, CUR_REG_PRICE, CUR_LIST_COST,CUR_VIP_COST, OVERRIDE_REG_MULTIPLE, OVERRIDE_REG_PRICE,"
			+ " OVERRIDE_PRICE_PREDICTED_MOV, OVERRIDE_PRICE_PRED_STATUS, "
			+ " IS_INCLUDE_FOR_SUM_CAL, PR.PREDICTION_STATUS FROM PR_RECOMMENDATION PR LEFT JOIN ITEM_LOOKUP IL ON PR.ITEM_CODE = IL.ITEM_CODE "
			+ " WHERE PR.RUN_ID = ? AND PR.OVERRIDE_REG_PRICE IS NOT NULL";
	
	private static final String UPDATE_RECOMMENDATION_STORE = "UPDATE PR_RECOMMENDATION_STORE SET OVERRIDE_PRED_UPDATE_STATUS = 0 " +
			" WHERE PR_RECOMMENDATION_ID IN (SELECT PR_RECOMMENDATION_ID FROM PR_RECOMMENDATION " +
			" WHERE RUN_ID = ?) AND  OVERRIDE_PRED_UPDATE_STATUS = 1 AND STORE_ID =  ? "; 
	
//	private static final String UPDATE_OVERRIDE_STORE = "UPDATE PR_RECOMMENDATION_STORE SET OVERRIDE_PRICE_PREDICTED_MOV = ?, " +
//			" OVERRIDE_PRICE_PRED_STATUS = ? WHERE RUN_ID = ? AND ITEM_CODE = ? AND LIR_IND='N'";
//	
//	private static final String UPDATE_RECOMMENDATION_DATA_STORE = "UPDATE PR_RECOMMENDATION_STORE SET OVERRIDE_PRICE_PREDICTED_MOV = ?, " +
//			" OVERRIDE_PRICE_PRED_STATUS = ?, OVERRIDE_SALES_D = ?, OVERRIDE_MARGIN_D = ? WHERE PR_RECOMMENDATION_ID = ?";
//	
//	private static final String GET_OVERRIDE_DATA_STORE = " SELECT PR_RECOMMENDATION_ID, PR.ITEM_CODE, IL.RET_LIR_ID, PR.LIR_IND, "
//			+ " CUR_REG_MULTIPLE, CUR_REG_PRICE, CUR_LIST_COST,CUR_VIP_COST, OVERRIDE_REG_MULTIPLE, OVERRIDE_REG_PRICE,"
//			+ " OVERRIDE_PRICE_PREDICTED_MOV, OVERRIDE_PRICE_PRED_STATUS, "
//			+ " IS_INCLUDE_FOR_SUM_CAL, PR.PREDICTION_STATUS FROM PR_RECOMMENDATION PR LEFT JOIN ITEM_LOOKUP IL ON PR.ITEM_CODE = IL.ITEM_CODE "
//			+ " WHERE PR.RUN_ID = ? AND PR.OVERRIDE_REG_PRICE IS NOT NULL";
	
	private static final String INSERT_PREDICTION_EXPLAIN = 
			"INSERT INTO PR_PREDICTION_EXPLAIN (PREDICTION_EXPLAIN_ID, RUN_ID, LIR_ID_OR_ITEM_CODE, LIR_IND, REG_PRICE, SALE_PRICE, " +			
			"MIN_PRICE,PAGE,AD,DISPLAY,TOTAL_INSTANCE,AVG_MOV,LAST_OBSERVED_DATA" +
			" ) VALUES (PREDICTION_EXPLAIN_ID_SEQ.NEXTVAL, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)";
	
	private static final String DELETE_PREDICTION = " DELETE FROM PR_PREDICTION_DETAIL WHERE RUN_ID IN ( " 
			+ " SELECT PD.RUN_ID FROM PR_PREDICTION_RUN_HEADER RH JOIN PR_PREDICTION_DETAIL PD ON RH.RUN_ID = PD.RUN_ID "
			+ " WHERE PD.LOCATION_LEVEL_ID = ? AND PD.LOCATION_ID = ? AND RH.START_CALENDAR_ID = ? "
			+ " AND RH.END_CALENDAR_ID = ?) AND LIR_ID_OR_ITEM_CODE IN (%ITEMS%)" ;
	 
	private static final String UPDATE_PREDICTION = "UPDATE PR_PREDICTION_DETAIL SET PRED_MOV_WO_SUBS_EFFECT = PREDICTED_MOVEMENT,"
			+ " PREDICTED_MOVEMENT = ?"
			+ " WHERE LIR_ID_OR_ITEM_CODE=? AND REG_PRICE=? AND REG_QUANTITY=? AND LOCATION_LEVEL_ID=? AND LOCATION_ID=? "
			+ " AND SALE_QUANTITY=? AND SALE_PRICE=? AND AD_PAGE_NO=? AND AD_BLOCK_NO=? AND DISPLAY_TYPE_ID=? "
			+ " AND PROMO_TYPE_ID=? AND "
			+ " RUN_ID IN (SELECT RUN_ID FROM PR_PREDICTION_RUN_HEADER WHERE START_CALENDAR_ID = ? AND END_CALENDAR_ID = ?)";
	
	/***
	 * Return already prediction movement of all items irrespective of the status
	 * @param conn
	 * @param productLevelId
	 * @param productId
	 * @param startCalendarId
	 * @param endCalendarId
	 * @param predictionInputDTOs
	 * @param isFilterByItem
	 * @return
	 * @throws GeneralException
	 */
	public HashMap<PredictionDetailKey, PredictionDetailDTO> getAllPredictedMovement(Connection conn,
			int productLevelId, int productId, int startCalendarId, int endCalendarId,
			List<PredictionInputDTO> predictionInputDTOs, boolean isFilterByItem) throws GeneralException {
		HashMap<PredictionDetailKey, PredictionDetailDTO> predictions = getPredictionDetail(conn, productLevelId,
				productId, startCalendarId, endCalendarId, predictionInputDTOs, isFilterByItem, true);
		return predictions;
	}
	
	/***
	 * Return items which as succesfull prediction
	 * @param conn
	 * @param productLevelId
	 * @param productId
	 * @param startCalendarId
	 * @param endCalendarId
	 * @param predictionInputDTOs
	 * @param isFilterByItem
	 * @return
	 * @throws GeneralException
	 */
	public HashMap<PredictionDetailKey, PredictionDetailDTO> getSucessfullPredictedMovement(
			Connection conn, int productLevelId, int productId, int startCalendarId, int endCalendarId,
			List<PredictionInputDTO> predictionInputDTOs, boolean isFilterByItem) throws GeneralException {
		HashMap<PredictionDetailKey, PredictionDetailDTO> predictions = getPredictionDetail(conn, productLevelId,
				productId, startCalendarId, endCalendarId, predictionInputDTOs, isFilterByItem, true);
		return predictions;
	}
	
	// /Get Prediction Detail for all the items in a location and calendar
	private HashMap<PredictionDetailKey, PredictionDetailDTO> getPredictionDetail(
			Connection conn, int productLevelId, int productId, int startCalendarId, int endCalendarId,
			List<PredictionInputDTO> predictionInputDTOs, boolean isFilterByItem, boolean isConsiderErrorItems) throws GeneralException {
		PreparedStatement stmt = null;
		ResultSet rs = null;
		HashMap<PredictionDetailKey, PredictionDetailDTO> predictionDetails = new HashMap<PredictionDetailKey, PredictionDetailDTO>();
		String sql = GET_PREDICTION_DETAILS;	
//		String itemLevelKey = "";
		PredictionDetailDTO predictionDetailDTO;
		PredictionDetailKey predictionDetailKey;
		try {		 

			if (isConsiderErrorItems) {
				sql = sql.replaceAll("%PREDICTION_STATUS_FILTER%", "");
			} else {
//				String predictionStatusFilter = " ( PD.PREDICTION_STATUS = " + PredictionStatus.SUCCESS.getStatusCode()
//						+ " OR PD.PREDICTION_STATUS = "
//						+ PredictionStatus.ERROR_NO_MOV_DATA_SPECIFIC_UPC.getStatusCode()
//						+ " OR PD.PREDICTION_STATUS = "
//						+ PredictionStatus.ERROR_NO_PRICE_DATA_SPECIFIC_UPC.getStatusCode() 
//						+ " OR PD.PREDICTION_STATUS = "
//						+ PredictionStatus.NO_RECENT_MOVEMENT.getStatusCode() 
//						+ " ) AND ";
				
				String predictionStatusFilter = " PD.PREDICTION_STATUS IN ( " + PredictionStatus.SUCCESS.getStatusCode()
						+ "," + PredictionStatus.ERROR_NO_MOV_DATA_SPECIFIC_UPC.getStatusCode()
						+ "," + PredictionStatus.ERROR_NO_PRICE_DATA_SPECIFIC_UPC.getStatusCode()
						+ "," + PredictionStatus.NO_RECENT_MOVEMENT.getStatusCode()
						+ "," + PredictionStatus.ERROR_NO_MOV_DATA_ANY_UPC.getStatusCode()
						+ "," + PredictionStatus.ERROR_NO_PRICE_DATA_ANY_UPC.getStatusCode()
						+ "," + PredictionStatus.SHIPPER_ITEM.getStatusCode()
						+ "," + PredictionStatus.NEW_ITEM.getStatusCode()
						+ "," + PredictionStatus.ERROR_MODEL_UNAVAILABLE.getStatusCode()
						+ " ) AND ";

				sql = sql.replaceAll("%PREDICTION_STATUS_FILTER%", predictionStatusFilter);
			}
			
			if (isFilterByItem) {
				sql = sql.replaceAll("%FILTER_BY_PRODUCT_LEVEL%", "");
				String locationAndItsItems = "";
//				String items = "";
				int counter = 0;
				for (PredictionInputDTO predictionInputDTO : predictionInputDTOs) {
					int limitcount = 0;
					int commitCount = 1000;
					int loopCounter = 0;
					List<Integer> pendingItems = new ArrayList<Integer>();
//					items = "";
					
					if (predictionInputDTO.predictionItems.size() > 0) {
						locationAndItsItems = counter > 0 ? (locationAndItsItems + " OR ") : locationAndItsItems;
						locationAndItsItems = locationAndItsItems + " (PD.LOCATION_LEVEL_ID = "
								+ predictionInputDTO.locationLevelId + " AND PD.LOCATION_ID = "
								+ predictionInputDTO.locationId + " AND ( ";
						for (PredictionItemDTO predictionItemDTO : predictionInputDTO.predictionItems) {
							pendingItems.add(predictionItemDTO.itemCodeOrLirId);
							limitcount++;
							if ((limitcount > 0) && (limitcount % commitCount == 0)) {
								if (loopCounter == 0) {
									locationAndItsItems = locationAndItsItems + " PD.LIR_ID_OR_ITEM_CODE IN ("
											+ PRCommonUtil.getCommaSeperatedStringFromIntArray(pendingItems) + ")";
								} else {
									locationAndItsItems = locationAndItsItems + " OR PD.LIR_ID_OR_ITEM_CODE IN ("
											+ PRCommonUtil.getCommaSeperatedStringFromIntArray(pendingItems) + ")";
								}
								pendingItems.clear();
								loopCounter = loopCounter + 1;
							}
						}
						if (pendingItems.size() > 0) {
							if (loopCounter == 0)
								locationAndItsItems = locationAndItsItems + " PD.LIR_ID_OR_ITEM_CODE IN (" + PRCommonUtil.getCommaSeperatedStringFromIntArray(pendingItems) + ")";
							else 
								locationAndItsItems = locationAndItsItems + " OR PD.LIR_ID_OR_ITEM_CODE IN (" + PRCommonUtil.getCommaSeperatedStringFromIntArray(pendingItems) + ")";
						}
						locationAndItsItems = locationAndItsItems + "))";
					}
					
					counter++;
					/*for (PredictionItemDTO predictionItemDTO : predictionInputDTO.predictionItems) {
						items = items + "," + predictionItemDTO.itemCodeOrLirId;
					}
					if (items != "") {
						items = items.substring(1);
						locationAndItsItems = counter > 0 ? (locationAndItsItems + " OR ") : locationAndItsItems;
						locationAndItsItems = locationAndItsItems + " (PD.LOCATION_LEVEL_ID = "
								+ predictionInputDTO.locationLevelId + " AND PD.LOCATION_ID = "
								+ predictionInputDTO.locationId + " AND (PD.LIR_ID_OR_ITEM_CODE IN (" + items + ")) ";
					}
					counter++;*/
				}
				
				if(locationAndItsItems != "")
					sql = sql.replaceAll("%FILTER_BY_ITEM%", " AND (" + locationAndItsItems + ")");
				else
					sql = sql.replaceAll("%FILTER_BY_ITEM%", "");
			}
			else {
				sql = sql.replaceAll("%FILTER_BY_ITEM%", "");	
				
				String locations = "";
				int counter = 0;
				for (PredictionInputDTO predictionInputDTO : predictionInputDTOs) {
					locations =  counter > 0 ? (locations + " OR ") : locations ;
					locations = locations + " (PD.LOCATION_LEVEL_ID = " + predictionInputDTO.locationLevelId
							+ " AND PD.LOCATION_ID = " + predictionInputDTO.locationId + ") ";
					counter++;
				}
				if(productLevelId > 0 && productId > 0) {				
					sql = sql.replaceAll("%FILTER_BY_PRODUCT_LEVEL%", (locations != "" ? ("(" + locations + ") AND ") : "") 
							+ " LIR_ID_OR_ITEM_CODE IN ( " + "SELECT CHILD_PRODUCT_ID FROM (SELECT * FROM PRODUCT_GROUP_RELATION_REC PGR " +						
							formQueryToGetProductLevelItems(productLevelId, productId)	+ " ) WHERE CHILD_PRODUCT_LEVEL_ID = 1 ) AND ");	
				}
				else
				{
					sql = sql.replaceAll("%FILTER_BY_PRODUCT_LEVEL%", (locations != "" ? ("(" + locations + ")") : "") + " ");
				}							
			}
			
			logger.debug("pred query: " + sql);
			stmt = conn.prepareStatement(sql);
			int counter = 0;			
			stmt.setInt(++counter, startCalendarId);
			stmt.setInt(++counter, endCalendarId);			 
			//stmt.setInt(++counter, predictionInputDTO.locationLevelId);
			//stmt.setInt(++counter, predictionInputDTO.locationId);
//			logger.debug("Prediction detail query:" + sql);
			stmt.setFetchSize(200000);
			rs = stmt.executeQuery();

			while (rs.next()) {
				// Fill to predictionDTO
				predictionDetailDTO = new PredictionDetailDTO();		
				predictionDetailDTO.setLocationLevelId(rs.getInt("LOCATION_LEVEL_ID"));
				predictionDetailDTO.setLocationId(rs.getInt("LOCATION_ID"));
				predictionDetailDTO.setLirOrItemCode(rs.getLong("LIR_ID_OR_ITEM_CODE"));
				predictionDetailDTO.setLirIndicator(rs.getString("LIR_IND").charAt(0));
				predictionDetailDTO.setRegularPrice(rs.getDouble("REG_PRICE"));
				predictionDetailDTO.setRegularQuantity(rs.getInt("REG_QUANTITY"));
				
				//If null, leave with default values
				if (rs.getObject("SALE_QUANTITY") != null)
					predictionDetailDTO.setSaleQuantity(rs.getInt("SALE_QUANTITY"));
				
				if (rs.getObject("SALE_PRICE") != null)
					predictionDetailDTO.setSalePrice(rs.getDouble("SALE_PRICE"));
				else
					predictionDetailDTO.setSalePrice(0d);
				
				if (rs.getObject("AD_PAGE_NO") != null)
					predictionDetailDTO.setAdPageNo(rs.getInt("AD_PAGE_NO"));

				if (rs.getObject("AD_BLOCK_NO") != null)
					predictionDetailDTO.setBlockNo(rs.getInt("AD_BLOCK_NO"));
					
				if (rs.getObject("DISPLAY_TYPE_ID") != null)
					predictionDetailDTO.setDisplayTypeId(rs.getInt("DISPLAY_TYPE_ID"));
						
				if (rs.getObject("PROMO_TYPE_ID") != null)
					predictionDetailDTO.setPromoTypeId(rs.getInt("PROMO_TYPE_ID"));
				
				//Bug Fix: 26th Apr 2016, return prediction in decimals
				if (rs.getObject("PREDICTED_MOVEMENT") != null)
					predictionDetailDTO.setPredictedMovement(rs.getDouble("PREDICTED_MOVEMENT"));
					//predictionDetailDTO.setPredictedMovement(rs.getLong("PREDICTED_MOVEMENT"));
				else
					predictionDetailDTO.setPredictedMovement(0);

				if (rs.getObject("CONFIDENCE_LEVEL_LOWER") != null)
					predictionDetailDTO.setConfidenceLevelLower(rs.getDouble("CONFIDENCE_LEVEL_LOWER"));
					//predictionDetailDTO.setConfidenceLevelLower(rs.getLong("CONFIDENCE_LEVEL_LOWER"));
				else
					predictionDetailDTO.setConfidenceLevelLower(0);
				
				if (rs.getObject("CONFIDENCE_LEVEL_UPPER") != null)
					predictionDetailDTO.setConfidenceLevelUpper(rs.getDouble("CONFIDENCE_LEVEL_UPPER"));
					//predictionDetailDTO.setConfidenceLevelUpper(rs.getLong("CONFIDENCE_LEVEL_UPPER"));
				else
					predictionDetailDTO.setConfidenceLevelUpper(0);
				
				if (rs.getObject("EXPLAIN_PREDICTION") != null)
					predictionDetailDTO.setExplainPrediction(rs.getString("EXPLAIN_PREDICTION"));
				else
					predictionDetailDTO.setExplainPrediction("");

				predictionDetailDTO.setPredictionStatus(rs.getInt("PREDICTION_STATUS"));
				
				//NU:: 14th Oct 2016, for substitution adjustment
				if (rs.getObject("PRED_MOV_WO_SUBS_EFFECT") != null)
					predictionDetailDTO.setPredictedMovementWithoutSubsEffect(rs.getDouble("PRED_MOV_WO_SUBS_EFFECT"));
				else
					predictionDetailDTO.setPredictedMovementWithoutSubsEffect(0);
				
				if (rs.getObject("QUESTIONABLE_PRED_REASONS") != null){
					predictionDetailDTO.setQuestionablePrediction(rs.getString("QUESTIONABLE_PRED_REASONS"));
				}
//				predictionDetailKey = new PredictionDetailKey();
//				
//				predictionDetailKey.setLirOrItemCode(predictionDetailDTO.getLirOrItemCode());
//				predictionDetailKey.setRegQuantity(predictionDetailDTO.getRegularQuantity());
//				predictionDetailKey.setRegPrice(predictionDetailDTO.getRegularPrice());
				
				predictionDetailKey = new PredictionDetailKey(predictionDetailDTO.getLocationLevelId(),
						predictionDetailDTO.getLocationId(), predictionDetailDTO.getLirOrItemCode(),
						predictionDetailDTO.getRegularQuantity(), predictionDetailDTO.getRegularPrice(),
						predictionDetailDTO.getSaleQuantity(), predictionDetailDTO.getSalePrice(),
						predictionDetailDTO.getAdPageNo(), predictionDetailDTO.getBlockNo(),
						predictionDetailDTO.getPromoTypeId(), predictionDetailDTO.getDisplayTypeId());
				//logger.debug("getPredictionDetail:" + itemLevelKey);
				/*
				 * Convert the data in to hashmap where key is
				 * ITEM_CODE-REG_QUANTITY-REG_PRICE and value as PredictionDTO
				 * If more than one recored for a location, calendar, item code,
				 * price combination is available, then pick the first one which 
				 * will be latest as the date is sorted by end time
				 */
				if(predictionDetails.get(predictionDetailKey) == null)				
					predictionDetails.put(predictionDetailKey, predictionDetailDTO);
			}
		} catch (SQLException exception) {
			predictionDetails = new HashMap<PredictionDetailKey, PredictionDetailDTO>();		
			logger.error("Error in getPredictionDetail() -- ");
			throw new GeneralException("Error in getPredictionDetail() -- "
					+ exception.toString());
		} catch (Exception exception) {
			predictionDetails = new HashMap<PredictionDetailKey, PredictionDetailDTO>();
			logger.error("Error in getPredictionDetail() -- ");
			throw new GeneralException("Error in getPredictionDetail() -- "
					+ exception.toString());
		}finally {
			PristineDBUtil.close(rs);
			PristineDBUtil.close(stmt);
		}
		return predictionDetails;
	}

	private String formQueryToGetProductLevelItems(int productLevelId, int productId){
		StringBuffer subQuery = new StringBuffer("");
		if(productLevelId > 1){
			subQuery = new StringBuffer("START WITH PRODUCT_LEVEL_ID = " + productLevelId);
			if(productId > 0){
				subQuery.append(" AND PRODUCT_ID = " + productId);
			}
			subQuery.append(" CONNECT BY  PRIOR CHILD_PRODUCT_ID = PRODUCT_ID  AND  PRIOR CHILD_PRODUCT_LEVEL_ID = PRODUCT_LEVEL_ID");	
		}else if(productLevelId == 1){
			subQuery = new StringBuffer("WHERE CHILD_PRODUCT_LEVEL_ID = 1 AND CHILD_PRODUCT_ID = " + productId);
		}
		
		return subQuery.toString();
	}
	
	public long insertPredictionRunHeader(Connection conn,
			PredictionRunHeaderDTO predictionRunHeaderDTO) throws GeneralException {
		PreparedStatement stmt = null;
		ResultSet rs = null;
		long predictionRunHeaderId = -1;
		try {
			stmt = conn.prepareStatement(GET_PREDICTION_RUN_ID);
			//logger.debug("insertPredictionRunHeader Query 1 : " + GET_PREDICTION_RUN_ID);
			rs = stmt.executeQuery();
			//logger.debug("insertPredictionRunHeader 1");
			if (rs.next()) {
				//logger.debug("insertPredictionRunHeader 2");
				predictionRunHeaderId = rs.getLong("RUN_ID");
				//logger.debug("insertPredictionRunHeader 3");
			}
			//logger.debug("insertPredictionRunHeader 4");
		} catch (SQLException exception) {
			logger.error("Error in insertPredictionRunHeader() -- "	+ exception.toString());
			throw new GeneralException("Error in insertPredictionRunHeader() -- " + exception.toString());
		}
		//logger.debug("insertPredictionRunHeader 5");
		PristineDBUtil.close(rs);
		PristineDBUtil.close(stmt);
		//logger.debug("insertPredictionRunHeader 6");

		try {
			stmt = conn.prepareStatement(INSERT_PREDICTION_RUN_HEADER);
			//logger.debug("insertPredictionRunHeader Query 2 : " + INSERT_PREDICTION_RUN_HEADER);
			int counter = 0;
			stmt.setLong(++counter, predictionRunHeaderId);			
			stmt.setString(++counter, String.valueOf(predictionRunHeaderDTO.getRunType()));			
			stmt.setString(++counter, predictionRunHeaderDTO.getPredictedBy());
			stmt.setInt(++counter, predictionRunHeaderDTO.getStartCalendarId());
			stmt.setInt(++counter, predictionRunHeaderDTO.getEndCalendarId());
			//logger.debug("insertPredictionRunHeader 7");
			//stmt.setInt(++counter, predictionRunHeaderDTO.getLocationLevelId());
			//stmt.setInt(++counter, predictionRunHeaderDTO.getLocationId());
			stmt.executeUpdate();	
			//logger.debug("insertPredictionRunHeader 8");
		} catch (SQLException exception) {
			logger.error("Error in insertPredictionRunHeader() -- "	+ exception.toString());
			throw new GeneralException("Error in insertPredictionRunHeader() -- " + exception.toString());
		} finally {
			PristineDBUtil.close(rs);
			PristineDBUtil.close(stmt);
		}
		return predictionRunHeaderId;
	}

	public void insertPredictionDetail(Connection conn, long predictionRunHeaderId,
			ArrayList<PredictionDetailDTO> predictionDetailDTOCol) throws GeneralException {
		PreparedStatement stmt = null;
		
		try {
			stmt = conn.prepareStatement(INSERT_PREDICTION_DETAIL);		 
			int itemNoInBatch = 0;
			for (PredictionDetailDTO predictionDetailDTO : predictionDetailDTOCol) {
				int counter = 0;

				stmt.setLong(++counter, predictionRunHeaderId);		
				stmt.setLong(++counter, predictionDetailDTO.getLirOrItemCode());
				stmt.setString(++counter, String.valueOf(predictionDetailDTO.getLirIndicator()));				
				stmt.setDouble(++counter, predictionDetailDTO.getRegularPrice());
				stmt.setInt(++counter, predictionDetailDTO.getRegularQuantity());
				stmt.setDouble(++counter, predictionDetailDTO.getPredictedMovement());	
				stmt.setString(++counter, predictionDetailDTO.getExplainPrediction());
				stmt.setInt(++counter, predictionDetailDTO.getPredictionStatus());
				stmt.setInt(++counter, predictionDetailDTO.getLocationLevelId());
				stmt.setInt(++counter, predictionDetailDTO.getLocationId());
				stmt.setInt(++counter, predictionDetailDTO.getSaleQuantity());
				if(predictionDetailDTO.getSalePrice() != null)
					stmt.setDouble(++counter, predictionDetailDTO.getSalePrice());
				else
					stmt.setDouble(++counter, 0d);
				stmt.setInt(++counter, predictionDetailDTO.getAdPageNo());
				stmt.setInt(++counter, predictionDetailDTO.getBlockNo());
				stmt.setInt(++counter, predictionDetailDTO.getDisplayTypeId());
				stmt.setInt(++counter, predictionDetailDTO.getPromoTypeId());				
				stmt.setDouble(++counter, predictionDetailDTO.getConfidenceLevelLower());	
				stmt.setDouble(++counter, predictionDetailDTO.getConfidenceLevelUpper());	
				stmt.setString(++counter, predictionDetailDTO.getQuestionablePrediction());	
				
				stmt.addBatch();
				itemNoInBatch++;

				if (itemNoInBatch % Constants.BATCH_UPDATE_COUNT == 0) {
					stmt.executeBatch();
					stmt.clearBatch();
					itemNoInBatch = 0;
				}
			}
			if (itemNoInBatch > 0) {
				stmt.executeBatch();
				stmt.clearBatch();
			}

		} catch (SQLException exception) {
			logger.error("Error in insertPredictionDetail() -- ");
			throw new GeneralException("Error in insertPredictionDetail() -- "
					+ exception.toString());
		} catch (Exception exception) {			 
			logger.error("Error in insertPredictionDetail() -- ");
			throw new GeneralException("Error in insertPredictionDetail() -- "
					+ exception.toString());
		} finally {
			PristineDBUtil.close(stmt);
		}
	}

	public void updateRunStatusAsComplete(Connection conn, long predictionRunHeaderId) throws GeneralException {
		PreparedStatement stmt = null;
		try {
			stmt = conn.prepareStatement(UPDATE_RUN_AS_COMPLETE);
			stmt.setString(1, PRConstants.RUN_STATUS_SUCCESS);
			stmt.setLong(2, predictionRunHeaderId);
			stmt.executeUpdate();
		} catch (SQLException exception) {
			logger.error("Error in updateRunStatusAsComplete() -- ");
			throw new GeneralException("Error in updateRunStatusAsComplete() -- "
					+ exception.toString());		 
		} finally {
			PristineDBUtil.close(stmt);
		}
	}
	
	public void updateRunStatusAsFailed(Connection conn, long predictionRunHeaderId) throws GeneralException {
		PreparedStatement stmt = null;
		try {
			stmt = conn.prepareStatement(UPDATE_RUN_AS_FAILED);
			stmt.setString(1, PRConstants.RUN_STATUS_ERROR);
			stmt.setLong(2, predictionRunHeaderId);
			stmt.executeUpdate();
		} catch (SQLException exception) {
			logger.error("Error in updateRunStatusAsComplete() -- ");
			throw new GeneralException("Error in updateRunStatusAsComplete() -- "
					+ exception.toString());		 
		} finally {
			PristineDBUtil.close(stmt);
		}
	}
	
	public void updateRunHeaderMsgAndPCT(Connection conn, long predictionRunHeaderId, String msg, int pct) throws GeneralException {
		PreparedStatement stmt = null;
		try {
			stmt = conn.prepareStatement(UPDATE_RUN_HEADER_MSG_PCT);
			stmt.setString(1, msg);
			stmt.setInt(2, pct);
			stmt.setLong(3, predictionRunHeaderId);
			stmt.executeUpdate();
			conn.commit();
		} catch (SQLException exception) {
			logger.error("Error in updateRunHeaderMsgAndPCT() -- ");
			throw new GeneralException("Error in updateRunHeaderMsgAndPCT() -- "
					+ exception.toString());		 
		} finally {
			PristineDBUtil.close(stmt);
		}
	}

	public List<PredictionInputDTO> getItemsForWhichPredictionToBeUpdated(Connection conn, 
			List<PredictionInputDTO> predictionInputDTOs)  throws GeneralException{
		PreparedStatement stmt = null;
		ResultSet rs = null;
		//List<PredictionInputDTO> outputPredictionInputDtos = new ArrayList<PredictionInputDTO>();
		String sql = GET_ITEMS_TO_BE_PREDICTED;
		String runIds = "";
		HashMap<ItemToPredictKey, PredictionInputDTO> outputPredictionInputDtos = new HashMap<ItemToPredictKey, PredictionInputDTO>();
		try {
			for (PredictionInputDTO predictionInputDTO : predictionInputDTOs) {              
				runIds = predictionInputDTO.recommendationRunId + "," + runIds; 
			}
			runIds = runIds.substring(0, runIds.length() - 1);
			sql = sql.replaceAll("%RUN_IDS%", runIds);	
			
			stmt = conn.prepareStatement(sql);
			stmt.setFetchSize(200000);
			rs = stmt.executeQuery();

			while (rs.next()) {
				ItemToPredictKey itemToPredictKey = null;
				
				//Process zone item
				if (rs.getObject("ZONE_OVERRIDE_PRICE") != null) {
					itemToPredictKey = new ItemToPredictKey(rs.getLong("RUN_ID"), rs.getInt("ZONE_LEVEL_ID"), rs.getInt("ZONE_ID"));
					processZoneStoreItems(outputPredictionInputDtos, predictionInputDTOs, rs, itemToPredictKey);
				}
				//Process store item
				if (rs.getObject("STORE_OVERRIDE_PRICE") != null) {
					itemToPredictKey = new ItemToPredictKey(rs.getLong("RUN_ID"), Constants.STORE_LEVEL_ID, rs.getInt("STORE_ID"));
					processZoneStoreItems(outputPredictionInputDtos, predictionInputDTOs, rs, itemToPredictKey);
				}
			}
		} catch (Exception exception) {
			logger.error("Error in getItemsForWhichPredictionToBeUpdated() -- ");
			throw new GeneralException("Error in getItemsForWhichPredictionToBeUpdated() -- "
					+ exception.toString());
		}finally {
			PristineDBUtil.close(rs);
			PristineDBUtil.close(stmt);
		}
		
		List<PredictionInputDTO> output = new ArrayList<PredictionInputDTO>();
		
		 
		for (Map.Entry<ItemToPredictKey, PredictionInputDTO> entry : outputPredictionInputDtos.entrySet()) {
			output.add(entry.getValue());
		} 
		
		return output;
	}
	
	private void processZoneStoreItems(HashMap<ItemToPredictKey, PredictionInputDTO> outputPredictionInputDtos
			,List<PredictionInputDTO> predictionInputDTOs, ResultSet rs, ItemToPredictKey itemToPredictKey ) throws SQLException{
		PredictionInputDTO predictionInputDTO;
		//Fill prediction input dto
		if (outputPredictionInputDtos.get(itemToPredictKey) != null) {
			predictionInputDTO = outputPredictionInputDtos.get(itemToPredictKey);
		} else {
			predictionInputDTO = new PredictionInputDTO();
			predictionInputDTO.recommendationRunId = itemToPredictKey.getRecommendationRunId();
			predictionInputDTO.locationLevelId = itemToPredictKey.getLocationLevelId();
			predictionInputDTO.locationId = itemToPredictKey.getLocationId();
			for (PredictionInputDTO pi : predictionInputDTOs) { 
				if(pi.recommendationRunId == predictionInputDTO.recommendationRunId){
					predictionInputDTO.productLevelId = pi.productLevelId;
					predictionInputDTO.productId = 	pi.productId;
					predictionInputDTO.startCalendarId = pi.startCalendarId;
					predictionInputDTO.endCalendarId = pi.endCalendarId;
					predictionInputDTO.startWeekDate = pi.startWeekDate;
					predictionInputDTO.endWeekDate = pi.endWeekDate;
				}
			}
		}
		
		if(predictionInputDTO.predictionItems == null)
			predictionInputDTO.predictionItems = new ArrayList<PredictionItemDTO>();
		
		//Keep only lig members and non lig. Ignore lig representation
		if(rs.getString("LIR_IND") != null && (Constants.NO == rs.getString("LIR_IND").charAt(0))){
			PricePointDTO pricePointDTO = new PricePointDTO();
			PredictionItemDTO prdItmDto = new PredictionItemDTO();
			boolean isItemAlreadyExists = false;
			
			//Check if item already exists, get price points from it
			for (PredictionItemDTO tempItemDTO : predictionInputDTO.predictionItems) {
				if(tempItemDTO.itemCodeOrLirId == rs.getInt("ITEM_CODE")){
					prdItmDto = tempItemDTO;
					isItemAlreadyExists = true;
					break;
				}
			}
			
			prdItmDto.itemCodeOrLirId = rs.getInt("ITEM_CODE");
			prdItmDto.upc = rs.getString("UPC");
			prdItmDto.lirInd = false;
			
			if(prdItmDto.pricePoints == null)
				prdItmDto.pricePoints = new ArrayList<PricePointDTO>();
			
			if (itemToPredictKey.getLocationLevelId() == Constants.STORE_LEVEL_ID) {
				//Make use of already fetched avg mov
				pricePointDTO.setRegQuantity(rs.getInt("STORE_OVERRIDE_MULTIPLE"));
				pricePointDTO.setRegPrice(rs.getDouble("STORE_OVERRIDE_PRICE"));
				pricePointDTO.setPredictedMovement(rs.getDouble("STORE_AVG_MOVEMENT"));
				pricePointDTO.setPredictionStatus(PredictionStatus.SUCCESS);
				prdItmDto.usePrediction = false;
				prdItmDto.isAvgMovAlreadySet = true;
			} else {
				if (rs.getString("RECOMMENDATION_STORE") != null
						&& (Constants.YES == rs.getString("RECOMMENDATION_STORE").charAt(0))) {
					//Make use of already fetched avg mov. For items which has store recommendation need to use avg mov
					prdItmDto.usePrediction = false;
					prdItmDto.isAvgMovAlreadySet = true;
					pricePointDTO.setPredictionStatus(PredictionStatus.SUCCESS);
				}
				pricePointDTO.setRegQuantity(rs.getInt("ZONE_OVERRIDE_MULTIPLE"));
				pricePointDTO.setRegPrice(rs.getDouble("ZONE_OVERRIDE_PRICE"));
				pricePointDTO.setPredictedMovement(rs.getDouble("ZONE_AVG_MOVEMENT"));
			}
			boolean priceAlreadyExists = false;
			//Don't add if the price point point already present
			for (PricePointDTO ppDTO : prdItmDto.pricePoints) {
				if(ppDTO.getRegQuantity() == pricePointDTO.getRegQuantity() && 
						ppDTO.getRegPrice().equals(pricePointDTO.getRegPrice())){
					priceAlreadyExists = true;
					break;
				}
			}
			if(!priceAlreadyExists)
				prdItmDto.pricePoints.add(pricePointDTO);
			if(!isItemAlreadyExists)
				predictionInputDTO.predictionItems.add(prdItmDto);
			outputPredictionInputDtos.put(itemToPredictKey, predictionInputDTO);
		}
	}
	
	public void updateOverridePredictionStatusZone(Connection conn, long recommendationRunId, int zoneId) throws GeneralException{
		PreparedStatement stmt = null;
		try {

			stmt = conn.prepareStatement(UPDATE_RECOMMENDATION);
			stmt.setLong(1, recommendationRunId);
			stmt.setInt(2, zoneId);
			stmt.executeUpdate();
		} catch (SQLException exception) {
			logger.error("Error in updateOverridePredictionStatusZone() - " + exception.toString());
			throw new GeneralException("Error in updateOverridePredictionStatusZone() - " + exception.toString());
		} finally {
			PristineDBUtil.close(stmt);
		}
	}
	
	public void updateOverridePricePredictionZone(Connection conn, long recommendationRunId,
			PredictionInputDTO predictionInputDTO) throws GeneralException {
		PreparedStatement stmt = null;
		try {
			stmt = conn.prepareStatement(UPDATE_OVERRIDE_ZONE);
			int itemNoInBatch = 0;
			for (PredictionItemDTO predItems : predictionInputDTO.predictionItems) {
				if (predItems.pricePoints.size() > 0) {
					if (predItems.pricePoints.get(0).getPredictedMovement() != null)
						stmt.setDouble(1, predItems.pricePoints.get(0).getPredictedMovement());
					else
						stmt.setNull(1, java.sql.Types.DOUBLE);
					
					if (predItems.pricePoints.get(0).getPredictionStatus() != null)
						stmt.setInt(2, predItems.pricePoints.get(0).getPredictionStatus().getStatusCode());
					else
						stmt.setNull(2, java.sql.Types.INTEGER);

					stmt.setLong(3, recommendationRunId);
					stmt.setLong(4, predItems.itemCodeOrLirId);
					stmt.addBatch();
					itemNoInBatch++;

					if (itemNoInBatch % Constants.BATCH_UPDATE_COUNT == 0) {
						stmt.executeBatch();
						stmt.clearBatch();
						itemNoInBatch = 0;
					}
				}
			}
			if (itemNoInBatch > 0) {
				stmt.executeBatch();
				stmt.clearBatch();
			}

		} catch (SQLException exception) {
			logger.error("Error in updateOverridePricePredictionZone() - " + exception.toString());
			throw new GeneralException("Error in updateOverridePricePredictionZone() - " + exception.toString());
		} finally {
			PristineDBUtil.close(stmt);
		}
	}
	
	public void updateRecommendationDataZone(Connection conn, Collection<PRItemDTO> items) throws GeneralException {
		PreparedStatement stmt = null;
		try {
			stmt = conn.prepareStatement(UPDATE_RECOMMENDATION_DATA_ZONE);
			int itemNoInBatch = 0;
			for (PRItemDTO itemDTO : items) {
				if (itemDTO.getOverridePredictedMovement() != null)
					stmt.setDouble(1, itemDTO.getOverridePredictedMovement());
				else
					stmt.setNull(1, java.sql.Types.DOUBLE);

				if (itemDTO.getOverridePredictionStatus() != null)
					stmt.setInt(2, itemDTO.getOverridePredictionStatus());
				else
					stmt.setNull(2, java.sql.Types.INTEGER);

				stmt.setDouble(3, itemDTO.getOverrideRetailSalesDollar());
				stmt.setDouble(4, itemDTO.getOverrideRetailMarginDollar());
				stmt.setLong(5, itemDTO.getPrRecommendationId());
				stmt.addBatch();
				itemNoInBatch++;

				if (itemNoInBatch % Constants.BATCH_UPDATE_COUNT == 0) {
					stmt.executeBatch();
					stmt.clearBatch();
					itemNoInBatch = 0;
				}
			}
			if (itemNoInBatch > 0) {
				stmt.executeBatch();
				stmt.clearBatch();
			}

		} catch (SQLException exception) {
			logger.error("Error in updateRecommendationDataZone() - " + exception.toString());
			throw new GeneralException("Error in updateRecommendationDataZone() - " + exception.toString());
		} finally {
			PristineDBUtil.close(stmt);
		}
	}
	
	public HashMap<ItemKey, PRItemDTO> getOverrideDataZone(Connection conn, long recommendationRunId)
			throws GeneralException {
		HashMap<ItemKey, PRItemDTO> overrideItems = new HashMap<ItemKey, PRItemDTO>();
		PreparedStatement stmt = null;
		ResultSet rs = null;
		String sql = GET_OVERRIDE_DATA_ZONE;

		try {
			stmt = conn.prepareStatement(sql);
			stmt.setLong(1, recommendationRunId);
			logger.debug("GET_OVERRIDE_DATA_ZONE:" + sql);
			stmt.setFetchSize(200000);
			rs = stmt.executeQuery();

			while (rs.next()) {
				PRItemDTO itemDTO = new PRItemDTO();
				ItemKey itemKey;
				boolean isLir = false;

				itemDTO.setPrRecommendationId(rs.getLong("PR_RECOMMENDATION_ID"));
				itemDTO.setItemCode(rs.getInt("ITEM_CODE"));
				String lirInd = rs.getString("LIR_IND");

				if (String.valueOf(Constants.YES).equalsIgnoreCase(lirInd)) {
					isLir = true;
				}
				itemKey = PRCommonUtil.getItemKey(itemDTO.getItemCode(), isLir);

				itemDTO.setLir(isLir);
				if (rs.getObject("RET_LIR_ID") != null && !isLir)
					itemDTO.setRetLirId(rs.getInt("RET_LIR_ID"));
				else
					itemDTO.setRetLirId(-1);

				itemDTO.setRegMPack(rs.getInt("CUR_REG_MULTIPLE") );
				if (itemDTO.getRegMPack() > 1)
					itemDTO.setRegMPrice(rs.getDouble("CUR_REG_PRICE"));
				else
					itemDTO.setRegPrice(rs.getDouble("CUR_REG_PRICE"));
				
				if (rs.getObject("CUR_LIST_COST") != null)
					itemDTO.setListCost(rs.getDouble("CUR_LIST_COST"));

				if (rs.getObject("CUR_VIP_COST") != null)
					itemDTO.setVipCost(rs.getDouble("CUR_VIP_COST"));

				if (rs.getObject("OVERRIDE_REG_MULTIPLE") != null)
					itemDTO.setOverrideRegMultiple(rs.getInt("OVERRIDE_REG_MULTIPLE"));
				
				if (rs.getObject("OVERRIDE_REG_PRICE") != null)
					itemDTO.setOverrideRegPrice(rs.getDouble("OVERRIDE_REG_PRICE"));
				
				if (rs.getObject("OVERRIDE_PRICE_PREDICTED_MOV") != null)
					itemDTO.setOverridePredictedMovement(rs.getDouble("OVERRIDE_PRICE_PREDICTED_MOV"));

				if (rs.getObject("OVERRIDE_PRICE_PRED_STATUS") != null)
					itemDTO.setOverridePredictionStatus(rs.getInt("OVERRIDE_PRICE_PRED_STATUS"));
				
				if (rs.getObject("PREDICTION_STATUS") != null)
					itemDTO.setPredictionStatus(rs.getInt("PREDICTION_STATUS"));

				overrideItems.put(itemKey, itemDTO);
			}
		} catch (SQLException exception) {
			logger.error("Error in getOverrideDataZone() - " + exception.toString());
			throw new GeneralException("Error in getOverrideDataZone() - " + exception.toString());
		} finally {
			PristineDBUtil.close(rs);
			PristineDBUtil.close(stmt);
		}
		return overrideItems;
	}
	
	public void updateOverridePredictionStatusStore(Connection conn, long recommendationRunId, int storeId) throws GeneralException{
		PreparedStatement stmt = null;
		try {

			stmt = conn.prepareStatement(UPDATE_RECOMMENDATION_STORE);
			stmt.setLong(1, recommendationRunId);
			stmt.setInt(2, storeId);
			stmt.executeUpdate();
		} catch (SQLException exception) {
			logger.error("Error in updateOverridePredictionStatusStore() - " + exception.toString());
			throw new GeneralException("Error in updateOverridePredictionStatusStore() - " + exception.toString());
		} finally {
			PristineDBUtil.close(stmt);
		}
	}
	
	public void updateOverridePredictionStatusInRunHeader(Connection conn, long recommendationRunId) throws GeneralException{
		PreparedStatement stmt = null;
		try {
			stmt = conn.prepareStatement(UPDATE_RECOMMENDATION_HEADER);
			stmt.setLong(1, recommendationRunId);
			stmt.executeUpdate();
		} catch (SQLException exception) {
			logger.error("Error in updateOverridePredictionStatusZone() - " + exception.toString());
			throw new GeneralException("Error in updateOverridePredictionStatusZone() - " + exception.toString());
		} finally {
			PristineDBUtil.close(stmt);
		}
	}
	
	public void insertPredictionExplain(Connection conn, long predictionRunHeaderId,
			List<PredictionExplain> predictionExplain) throws GeneralException {
		PreparedStatement stmt = null;
		
		try {
			stmt = conn.prepareStatement(INSERT_PREDICTION_EXPLAIN);		 
			int itemNoInBatch = 0;
			for (PredictionExplain prdExplain : predictionExplain) {
				int counter = 0;

				stmt.setLong(++counter, predictionRunHeaderId);		
				stmt.setInt(++counter, prdExplain.getLirIdOrItemCode());
				if(prdExplain.getIsLIG() == 1)
					stmt.setString(++counter, String.valueOf(Constants.YES));
				else
					stmt.setString(++counter, String.valueOf(Constants.NO));				
				if(prdExplain.getRegPrice() > 0)
					stmt.setDouble(++counter, prdExplain.getRegPrice());
				else
					stmt.setNull(++counter, java.sql.Types.DOUBLE);
				if(prdExplain.getSalePrice() > 0)
					stmt.setDouble(++counter, prdExplain.getSalePrice());
				else
					stmt.setNull(++counter, java.sql.Types.DOUBLE);
				if(prdExplain.getMinPrice() > 0)
					stmt.setDouble(++counter, prdExplain.getMinPrice());
				else
					stmt.setNull(++counter, java.sql.Types.DOUBLE);
				stmt.setInt(++counter, prdExplain.getPage());
				stmt.setInt(++counter, prdExplain.getAd());
				stmt.setString(++counter, prdExplain.getDisplay());		
				stmt.setInt(++counter, prdExplain.getTotalInstances());
				stmt.setLong(++counter, prdExplain.getAvgMov());
				stmt.setString(++counter, prdExplain.getLastObservedDate());
				
				stmt.addBatch();
				itemNoInBatch++;

				if (itemNoInBatch % Constants.BATCH_UPDATE_COUNT == 0) {
					stmt.executeBatch();
					stmt.clearBatch();
					itemNoInBatch = 0;
				}
			}
			if (itemNoInBatch > 0) {
				stmt.executeBatch();
				stmt.clearBatch();
			}

		} catch (SQLException exception) {
			logger.error("Error in insertPredictionExplain() -- ");
			throw new GeneralException("Error in insertPredictionExplain() -- "
					+ exception.toString());
		} catch (Exception exception) {			 
			logger.error("Error in insertPredictionExplain() -- ");
			throw new GeneralException("Error in insertPredictionExplain() -- "
					+ exception.toString());
		} finally {
			PristineDBUtil.close(stmt);
		}
	}
	
	public int deletePrediction(Connection conn, int locationLevelId, int locationId, int calendarId,
			List<Integer> items) throws GeneralException {
		PreparedStatement stmt = null;
		int totalDeleteCnt = 0;
		try {
			String sql = DELETE_PREDICTION;
			sql = sql.replaceAll("%ITEMS%", PRCommonUtil.getCommaSeperatedStringFromIntArray(items));
			
			logger.debug("Delete prediction query: " + sql);
			if (items.size() > 0) {
				stmt = conn.prepareStatement(sql);
				stmt.setLong(1, locationLevelId);
				stmt.setLong(2, locationId);
				stmt.setLong(3, calendarId);
				stmt.setLong(4, calendarId);
				totalDeleteCnt = stmt.executeUpdate();
			}
		} catch (SQLException exception) {
			logger.error("Error in deletePrediction() -- ");
			throw new GeneralException("Error in deletePrediction() -- "
					+ exception.toString());		 
		} finally {
			if (items.size() > 0)
				PristineDBUtil.close(stmt);
		}
		return totalDeleteCnt;
	}
	
	public int updatePredictedMovement(Connection conn, int startCalendarId, int endCalendarId, HashMap<PredictionDetailKey, PredictionEngineValue> movementMap)
			throws GeneralException {
		PreparedStatement statement = null;
		int rowsAffected = 0;
		try {
			statement = conn.prepareStatement(UPDATE_PREDICTION);
			int batchUpdateCount = 0;
			for (Map.Entry<PredictionDetailKey, PredictionEngineValue> entry : movementMap.entrySet()) {
				int counter = 0;
				PredictionEngineValue predictionEngineValue = entry.getValue();
				PredictionDetailKey pdk = entry.getKey();
				batchUpdateCount++;
				
				statement.setDouble(++counter, predictionEngineValue.predictedMovement);
				statement.setLong(++counter, pdk.getLirOrItemCode());
				statement.setDouble(++counter, pdk.getRegPrice());
				statement.setInt(++counter, pdk.getRegQuantity());
				statement.setInt(++counter, pdk.getLocationLevelId());
				statement.setInt(++counter, pdk.getLocationId());
				statement.setInt(++counter, pdk.getSaleQuantity());
				statement.setDouble(++counter, pdk.getSalePrice());
				statement.setInt(++counter, pdk.getAdPageNo());
				statement.setInt(++counter, pdk.getBlockNo());
				statement.setInt(++counter, pdk.getDisplayTypeId());
				statement.setInt(++counter, pdk.getPromoTypeId());
				statement.setInt(++counter, startCalendarId);
				statement.setInt(++counter, endCalendarId);
				
				logger.debug("Updating prediction cache for:" + pdk.toString());
				statement.addBatch();
				if (batchUpdateCount % Constants.BATCH_UPDATE_COUNT == 0) {
					int[] count = statement.executeBatch();
					statement.clearBatch();
					batchUpdateCount = 0;
					rowsAffected += PristineDBUtil.getUpdateCount(count);
				}
			}

			if (batchUpdateCount > 0) {
				int[] count = statement.executeBatch();
				statement.clearBatch();
				rowsAffected += PristineDBUtil.getUpdateCount(count);
			}
		} catch (SQLException sqlE) {
			logger.error("Error -- updatePredictedMovement()", sqlE);
			throw new GeneralException("Error -- updatePredictedMovement()", sqlE);
		} finally {
			PristineDBUtil.close(statement);
		}
		return rowsAffected;
	}
}

class ItemToPredictKey{
	private long recommendationRunId;
	private int locationLevelId;
	private int locationId;
	
	public ItemToPredictKey(long recommendationRunId, int locationLevelId, int locationId){
		this.recommendationRunId = recommendationRunId;
		this.locationLevelId = locationLevelId;
		this.locationId = locationId;
	}

	public long getRecommendationRunId() {
		return recommendationRunId;
	}

	public void setRecommendationRunId(long recommendationRunId) {
		this.recommendationRunId = recommendationRunId;
	}

	public int getLocationLevelId() {
		return locationLevelId;
	}

	public void setLocationLevelId(int locationLevelId) {
		this.locationLevelId = locationLevelId;
	}

	public int getLocationId() {
		return locationId;
	}

	public void setLocationId(int locationId) {
		this.locationId = locationId;
	}

	@Override
	public int hashCode() {
		final int prime = 31;
		int result = 1;
		result = prime * result + locationId;
		result = prime * result + locationLevelId;
		result = prime * result + (int) (recommendationRunId ^ (recommendationRunId >>> 32));
		return result;
	}

	@Override
	public boolean equals(Object obj) {
		if (this == obj)
			return true;
		if (obj == null)
			return false;
		if (getClass() != obj.getClass())
			return false;
		ItemToPredictKey other = (ItemToPredictKey) obj;
		if (locationId != other.locationId)
			return false;
		if (locationLevelId != other.locationLevelId)
			return false;
		if (recommendationRunId != other.recommendationRunId)
			return false;
		return true;
	}
}

