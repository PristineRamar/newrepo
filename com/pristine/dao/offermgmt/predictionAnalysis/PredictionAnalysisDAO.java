package com.pristine.dao.offermgmt.predictionAnalysis;

import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.HashMap;
import java.util.LinkedHashMap;

import org.apache.log4j.Logger;

import java.sql.Connection;
import com.pristine.dto.offermgmt.predictionAnalysis.MovementKeyDTO;
import com.pristine.dto.ProductMetricsDataDTO;
import com.pristine.dto.RetailCalendarDTO;
import com.pristine.dto.offermgmt.PRItemDTO;
import com.pristine.dto.offermgmt.PRRecommendation;
import com.pristine.exception.GeneralException;
import com.pristine.util.Constants;
import com.pristine.util.PristineDBUtil;
import com.pristine.util.offermgmt.PRConstants;

public class PredictionAnalysisDAO {
	private static Logger logger = Logger.getLogger("PredictionAnalysisDAO");
	
	private static final String GET_ITEM_DETAILS = "SELECT IL.ITEM_CODE, IL.ITEM_NAME, IL.RET_LIR_ID,IL.UPC, IL.RETAILER_ITEM_CODE, LIG.RET_LIR_NAME, "
			+ "TO_CHAR(IL.CREATE_TIMESTAMP,'MM/DD/YYYY') AS CREATE_TIMESTAMP FROM (SELECT DISTINCT (ITEM_CODE) AS DISTINCT_ITEM_CODE FROM "
			+ "STORE_ITEM_MAP SIM WHERE SIM.LEVEL_TYPE_ID = ? AND SIM.LEVEL_ID IN (SELECT STORE_ID FROM PR_PRODUCT_LOCATION_STORE WHERE "
			+ "PRODUCT_LOCATION_MAPPING_ID IN(SELECT PRODUCT_LOCATION_MAPPING_ID FROM PR_PRODUCT_LOCATION_MAPPING "
			+ "WHERE PRODUCT_LEVEL_ID = ? AND PRODUCT_ID = ? AND LOCATION_ID = ?)) AND SIM.ITEM_CODE IN (SELECT CHILD_PRODUCT_ID FROM "
			+ "(SELECT CHILD_PRODUCT_ID, CHILD_PRODUCT_LEVEL_ID FROM PRODUCT_GROUP_RELATION PGR START WITH PRODUCT_LEVEL_ID = ? AND PRODUCT_ID = ? "
			+ "CONNECT BY PRIOR CHILD_PRODUCT_ID = PRODUCT_ID AND PRIOR CHILD_PRODUCT_LEVEL_ID = PRODUCT_LEVEL_ID) WHERE CHILD_PRODUCT_LEVEL_ID = ? ) "
			+ "AND SIM.IS_AUTHORIZED = ?)LEFT JOIN ITEM_LOOKUP IL ON DISTINCT_ITEM_CODE = IL.ITEM_CODE "
			+ "LEFT JOIN RETAILER_LIKE_ITEM_GROUP LIG ON IL.RET_LIR_ID = LIG.RET_LIR_ID WHERE IL.ACTIVE_INDICATOR = ?";
	
	private static final String GET_RETAIL_CALENDAR_DETAIL = "SELECT CALENDAR_ID, START_DATE, TO_CHAR(START_DATE,'MM/DD/YYYY') AS FORMATTED_START_DATE"
			+ " FROM RETAIL_CALENDAR RC WHERE RC.START_DATE <= TO_DATE(?,'MM/DD/YYYY')-1 "
			+ " AND RC.START_DATE >= TO_DATE(?,'MM/DD/YYYY')-7*? AND ROW_TYPE = 'W' ORDER BY START_DATE DESC ";

	private static final String GET_ZONE_MOVEMENT_HISTORY = "SELECT CALENDAR_ID, PRODUCT_ID, STATS_MODE(REG_UNIT_PRICE) AS REG_UNIT_PRICE, "
			+ "STATS_MODE(SALE_UNIT_PRICE) AS SALE_UNIT_PRICE, SUM(REG_MOVEMENT) AS REG_MOVEMENT, "
			+ "SUM(SALE_MOVEMENT) AS SALE_MOVEMENT, SUM(TOT_MOVEMENT) AS TOT_MOVEMENT FROM (SELECT CALENDAR_ID, PRODUCT_ID, "
			+ "CASE WHEN NVL(REG_M_PACK,0) <= 1 THEN NVL(REG_PRICE,0) "
			+ "WHEN NVL(REG_M_PACK,0) >1 THEN ROUND((REG_M_PRICE/REG_M_PACK),2)  "
			+ "ELSE 0 END AS REG_UNIT_PRICE, "
			+ "REG_MOVEMENT, "
			+ "CASE WHEN NVL(SALE_M_PACK,0) <= 1 THEN NVL(SALE_PRICE,0) "
			+ "WHEN NVL(SALE_M_PACK,0) >1 THEN ROUND((SALE_M_PRICE/SALE_M_PACK),2)  "
			+ "ELSE 0 END AS SALE_UNIT_PRICE, "
			+ "SALE_MOVEMENT, TOT_MOVEMENT "
			+ "FROM ITEM_METRIC_SUMMARY_WEEKLY IMS WHERE product_id IN "
			/*+ "(SELECT ITEM_CODE FROM (SELECT DISTINCT (ITEM_CODE) AS DISTINCT_ITEM_CODE FROM STORE_ITEM_MAP SIM "
			+ "WHERE SIM.LEVEL_TYPE_ID = ? AND SIM.LEVEL_ID IN (SELECT STORE_ID FROM PR_PRODUCT_LOCATION_STORE "
			+ "WHERE PRODUCT_LOCATION_MAPPING_ID IN(SELECT PRODUCT_LOCATION_MAPPING_ID FROM PR_PRODUCT_LOCATION_MAPPING WHERE PRODUCT_LEVEL_ID = ? "
			+ "AND PRODUCT_ID = ? AND LOCATION_ID = ?)) AND SIM.ITEM_CODE IN (SELECT CHILD_PRODUCT_ID FROM (SELECT CHILD_PRODUCT_ID, "
			+ "CHILD_PRODUCT_LEVEL_ID FROM PRODUCT_GROUP_RELATION PGR START WITH PRODUCT_LEVEL_ID = ? AND PRODUCT_ID = ? "
			+ "CONNECT BY PRIOR CHILD_PRODUCT_ID = PRODUCT_ID AND PRIOR CHILD_PRODUCT_LEVEL_ID = PRODUCT_LEVEL_ID) "
			+ "WHERE CHILD_PRODUCT_LEVEL_ID = ? ) AND SIM.IS_AUTHORIZED = ?) LEFT JOIN ITEM_LOOKUP IL ON DISTINCT_ITEM_CODE = IL.ITEM_CODE "
			+ "WHERE IL.ACTIVE_INDICATOR = ?) " */
			+ "(%ITEM_CODES%)"
			+ "AND CALENDAR_ID IN (SELECT CALENDAR_ID FROM RETAIL_CALENDAR RC "
			+ "WHERE  RC.START_DATE <= TO_DATE(?,'MM/DD/YYYY') -1 AND RC.START_DATE >= TO_DATE(?,'MM/DD/YYYY')-7*? "
			+ "AND ROW_TYPE = 'W' "
			+ " AND IMS.LOCATION_ID IN  (SELECT STORE_ID FROM PR_PRODUCT_LOCATION_STORE "
			+ "WHERE PRODUCT_LOCATION_MAPPING_ID IN (SELECT PRODUCT_LOCATION_MAPPING_ID FROM PR_PRODUCT_LOCATION_MAPPING "
			+ "WHERE PRODUCT_LEVEL_ID = ? AND PRODUCT_ID = ? AND LOCATION_LEVEL_ID = ? AND LOCATION_ID = ?)))) "
			+ "GROUP BY CALENDAR_ID, PRODUCT_ID";

	private static final String GET_CORRELATION_OF_ITEMS = "SELECT LIR_ID_OR_ITEM_CODE, ROUND(CORR(ROUND(REG_PRICE/REG_QUANTITY,2), PREDICTED_MOVEMENT),4) AS CORELATION "
			+ "FROM PR_PREDICTION_RUN_HEADER RH JOIN PR_PREDICTION_DETAIL PD ON RH.RUN_ID = PD.RUN_ID WHERE RH.START_CALENDAR_ID = ? "
			+ "AND RH.END_CALENDAR_ID = ? AND LOCATION_ID = ? AND PD.LIR_ID_OR_ITEM_CODE IN (SELECT DISTINCT ITEM_CODE FROM PR_RECOMMENDATION "
			+ "WHERE RUN_ID = ? AND LIR_IND = 'N' "
			//temp code due to other bug, this is included
			//+ " AND CUR_REG_MULTIPLE <= 1"
			+ ") AND PREDICTION_STATUS = 0 AND PREDICTED_MOVEMENT > 0 GROUP BY LIR_ID_OR_ITEM_CODE HAVING "
			+ "CORR(ROUND(REG_PRICE/REG_QUANTITY,2), PREDICTED_MOVEMENT) > ? ORDER BY CORR(ROUND(REG_PRICE/REG_QUANTITY,2), PREDICTED_MOVEMENT) DESC";
	
	private static final String GET_LATEST_RUN_ID = "SELECT RUN_ID FROM (SELECT RUN_ID, RANK() OVER (PARTITION BY PRODUCT_ID ORDER BY END_RUN_TIME DESC) "
			+ "AS RANK FROM PR_RECOMMENDATION_RUN_HEADER WHERE CALENDAR_ID = ? AND PRODUCT_ID = ? AND LOCATION_ID=? AND RUN_TYPE <> '" + 
			PRConstants.RUN_TYPE_TEMP + "' AND END_RUN_TIME IS NOT NULL) WHERE RANK=1";

	private static final String GET_RECOMMENDATION_ITEMS = "SELECT PR.ITEM_CODE,RECOMMENDED_REG_MULTIPLE, RECOMMENDED_REG_PRICE "
			+ " FROM PR_RECOMMENDATION PR WHERE RUN_ID = ? AND LIR_IND = 'N'";
	/**
	 * 
	 * @param connection
	 * @param locationLevelId
	 * @param locationId
	 * @param productLevelId
	 * @param productId
	 * @return
	 * @throws GeneralException
	 */
	public HashMap<Integer, PRItemDTO> getItemDetail(Connection connection, int locationLevelId, int locationId, int productLevelId,
			int productId) throws GeneralException {
		HashMap<Integer, PRItemDTO> prProductLocationDetails = new HashMap<Integer, PRItemDTO>();
		PreparedStatement statement = null;
		ResultSet resultSet = null;
		try {
			String query = new String(GET_ITEM_DETAILS);
			logger.debug(query);
			statement = connection.prepareStatement(query);

			int counter = 0;
			statement.setLong(++counter, Constants.STORE_LEVEL_TYPE_ID);
			statement.setInt(++counter, productLevelId);
			statement.setLong(++counter, productId);
			statement.setLong(++counter, locationId);
			statement.setInt(++counter, productLevelId);
			statement.setLong(++counter, productId);
			statement.setLong(++counter, Constants.ZONE_LEVEL_TYPE_ID);
			statement.setString(++counter, Constants.AUTHORIZED_ITEM);
			statement.setString(++counter, Constants.AUTHORIZED_ITEM);
			resultSet = statement.executeQuery();

			while (resultSet.next()) {
				PRItemDTO prItemDTO = new PRItemDTO();
				prItemDTO.setItemCode(resultSet.getInt("ITEM_CODE"));
				prItemDTO.setItemName(resultSet.getString("ITEM_NAME"));
				prItemDTO.setRetLirId(resultSet.getInt("RET_LIR_ID"));
				prItemDTO.setUpc(resultSet.getString("UPC"));
				prItemDTO.setRetailerItemCode(resultSet.getString("RETAILER_ITEM_CODE"));
				prItemDTO.setRetLirName(resultSet.getString("RET_LIR_NAME"));
				prItemDTO.setCreateTimeStamp(resultSet.getString("CREATE_TIMESTAMP"));
				prProductLocationDetails.put(prItemDTO.getItemCode(), prItemDTO);
			}

		} catch (SQLException e) {
			logger.error("Error while executing getItemDetails " + e.toString(), e);
			throw new GeneralException("Error while executing getItemDetails " + e.toString(), e);
		} finally {
			PristineDBUtil.close(resultSet);
			PristineDBUtil.close(statement);
		}

		return prProductLocationDetails;

	}

	/**
	 * 
	 * @param connection
	 * @param date
	 * @return
	 * @throws GeneralException
	 */
	public LinkedHashMap<Integer, RetailCalendarDTO> getRetailCalendarDetail(Connection connection, String date, int noOfWeeksBefore)
			throws GeneralException {
		LinkedHashMap<Integer, RetailCalendarDTO> retailCalendarDetailList = new LinkedHashMap<Integer, RetailCalendarDTO>();
		PreparedStatement statement = null;
		ResultSet resultSet = null;
		try {
			String query = new String(GET_RETAIL_CALENDAR_DETAIL);
			logger.debug(query);
			statement = connection.prepareStatement(query);

			int counter = 0;
			statement.setString(++counter, date);
			statement.setString(++counter, date);
			statement.setInt(++counter, noOfWeeksBefore);

			resultSet = statement.executeQuery();

			while (resultSet.next()) {
				RetailCalendarDTO retailCalendarDTO = new RetailCalendarDTO();
				retailCalendarDTO.setCalendarId(resultSet.getInt("CALENDAR_ID"));
//				retailCalendarDTO.setCalYear(resultSet.getInt("CAL_YEAR"));
				retailCalendarDTO.setStartDate(resultSet.getString("FORMATTED_START_DATE"));
//				retailCalendarDTO.setEndDate(resultSet.getString("END_DATE"));
//				retailCalendarDTO.setActualNo(resultSet.getInt("ACTUAL_NO"));
				retailCalendarDetailList.put(retailCalendarDTO.getCalendarId(), retailCalendarDTO);
			}

		} catch (SQLException e) {
			logger.error("Error while executing getRetailCalendarDetailList " + e.toString(), e);
			throw new GeneralException("Error while executing getRetailCalendarDetailList " + e.toString(), e);
		} finally {
			PristineDBUtil.close(resultSet);
			PristineDBUtil.close(statement);
		}
		return retailCalendarDetailList;

	}

	/**
	 * 
	 * @param connection
	 * @param locationLevelId
	 * @param locationId
	 * @param productLevelId
	 * @param productId
	 * @param date
	 * @param noOfWeeksBefore
	 * @return
	 * @throws GeneralException
	 */
	public HashMap<MovementKeyDTO, ProductMetricsDataDTO> getZoneMovementHistoryList(Connection connection, int locationLevelId, int locationId,
			int productLevelId, int productId, String date, int noOfWeeksBefore, String itemCodes) throws GeneralException {
		HashMap<MovementKeyDTO, ProductMetricsDataDTO> zoneMovementHistoryList = new HashMap<MovementKeyDTO, ProductMetricsDataDTO>();
		PreparedStatement statement = null;
		ResultSet resultSet = null;
		try {
			String query = new String(GET_ZONE_MOVEMENT_HISTORY);
			query = query.replaceAll("%ITEM_CODES%", itemCodes);
			logger.debug(query);

			int counter = 0;
			statement = connection.prepareStatement(query);
			statement.setString(++counter, date);
			statement.setString(++counter, date);
			statement.setInt(++counter, noOfWeeksBefore);
			statement.setInt(++counter, productLevelId);
			statement.setLong(++counter, productId);
			statement.setLong(++counter, locationLevelId);
			statement.setLong(++counter, locationId);
//			statement.setLong(++counter, Constants.STORE_LEVEL_TYPE_ID);
//			statement.setInt(++counter, productLevelId);
//			statement.setLong(++counter, productId);
//			statement.setLong(++counter, locationId);
//			statement.setInt(++counter, productLevelId);
//			statement.setLong(++counter, productId);
//			statement.setLong(++counter, Constants.ITEM_LEVEL_CHILD_PRODUCT_LEVEL_ID);
//			statement.setString(++counter, String.valueOf(Constants.YES));
//			statement.setString(++counter, String.valueOf(Constants.YES));
			resultSet = statement.executeQuery();

			while (resultSet.next()) {
				ProductMetricsDataDTO productMetricsDataDTO = new ProductMetricsDataDTO();
				MovementKeyDTO movementKey = new MovementKeyDTO(resultSet.getLong("PRODUCT_ID"), resultSet.getInt("CALENDAR_ID"));
				productMetricsDataDTO.setRegularPrice(resultSet.getDouble("REG_UNIT_PRICE"));
				productMetricsDataDTO.setSalePrice(resultSet.getDouble("SALE_UNIT_PRICE"));
				productMetricsDataDTO.setRegularMovement(resultSet.getDouble("REG_MOVEMENT"));
				productMetricsDataDTO.setSaleMovement(resultSet.getDouble("SALE_MOVEMENT"));
				productMetricsDataDTO.setTotalMovement(resultSet.getDouble("TOT_MOVEMENT"));
				zoneMovementHistoryList.put(movementKey, productMetricsDataDTO);
			}

		} catch (SQLException e) {
			logger.error("Error while executing getZoneMovementHistoryList " + e.toString(), e);
			throw new GeneralException("Error while executing getZoneMovementHistoryList " + e.toString(), e);
		} finally {
			PristineDBUtil.close(resultSet);
			PristineDBUtil.close(statement);
		}
		return zoneMovementHistoryList;

	}

	/**
	 * 
	 * @param connection
	 * @param locationId
	 * @param productId
	 * @param calendarId
	 * @return
	 * @throws GeneralException
	 */
	public HashMap<Integer, Double> getCorrelationOfItems(Connection connection, int locationId, int calendarId, long runId, double correlationFactor)
			throws GeneralException {
		HashMap<Integer, Double> correlationOfItems = new HashMap<Integer, Double>();
		PreparedStatement statement = null;
		ResultSet resultSet = null;
		try {
			String query = new String(GET_CORRELATION_OF_ITEMS);
			logger.debug(query);
			statement = connection.prepareStatement(query);

			int counter = 0;
			statement.setInt(++counter, calendarId);
			statement.setInt(++counter, calendarId);
			statement.setLong(++counter, locationId);
			// statement.setInt(++counter, calendarId);
			// statement.setLong(++counter, productId);
			// statement.setLong(++counter, locationId);
			statement.setLong(++counter, runId);
			statement.setDouble(++counter, correlationFactor);
			resultSet = statement.executeQuery();

			while (resultSet.next()) {
				correlationOfItems.put(resultSet.getInt("LIR_ID_OR_ITEM_CODE"), resultSet.getDouble("CORELATION"));
			}

		} catch (SQLException e) {
			logger.error("Error while executing getCorrelationOfItems " + e.toString(), e);
			throw new GeneralException("Error while executing getCorrelationOfItems " + e.toString(), e);
		} finally {
			PristineDBUtil.close(resultSet);
			PristineDBUtil.close(statement);
		}

		return correlationOfItems;

	}
	
	public long getLatestRecommendationRunId(Connection conn, int calendarId, int locationId, int productId) {
		PreparedStatement stmt = null;
		ResultSet rs = null;
		long recRunId = 0;
		try {
			logger.debug(GET_LATEST_RUN_ID);
			stmt = conn.prepareStatement(GET_LATEST_RUN_ID);
			stmt.setInt(1, calendarId);
			stmt.setInt(2, productId);
			stmt.setInt(3, locationId);
			rs = stmt.executeQuery();
			if (rs.next()) {
				recRunId = rs.getLong("RUN_ID");
			}
		} catch (SQLException exception) {
			logger.error("Error in getLatestRecommendationRunId() - " + exception.toString());
		} finally {
			PristineDBUtil.close(rs);
			PristineDBUtil.close(stmt);
		}
		return recRunId;
	}
	
	public HashMap<Integer, PRRecommendation> getRecommendationItems(Connection conn, long runId) throws GeneralException{
		PreparedStatement stmt = null;
		ResultSet rs = null;
		HashMap<Integer, PRRecommendation> itemList = new HashMap<Integer, PRRecommendation>();
		try{
	    	stmt = conn.prepareStatement(GET_RECOMMENDATION_ITEMS);
	    	logger.debug(GET_RECOMMENDATION_ITEMS);
	    	stmt.setLong(1, runId);
	    	rs = stmt.executeQuery();
	    	while(rs.next()){
	    		PRRecommendation item = new PRRecommendation();
	    		item.setItemCode(rs.getInt("ITEM_CODE"));
	    		item.setRecRegMultiple(rs.getInt("RECOMMENDED_REG_Multiple"));
	    		item.setRecRegPrice(rs.getDouble("RECOMMENDED_REG_PRICE"));
	    		itemList.put(item.getItemCode(), item);
	    	}
	    }catch (SQLException e){
			logger.error("Error while retrieving recommendation list " + e.getMessage());
			throw new GeneralException("Error in getRecommendationItems() - " + e);
		}finally{
			PristineDBUtil.close(rs);
			PristineDBUtil.close(stmt);
		}
		return itemList;
	}
}
