package com.pristine.dao.offermgmt.mwr.prediction;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.List;

import org.apache.log4j.Logger;

import com.pristine.dto.offermgmt.multiWeekPrediction.MultiWeekPredEngineItemDTO;
import com.pristine.dto.offermgmt.mwr.MWRRunHeader;
import com.pristine.dto.offermgmt.mwr.RecommendationInputDTO;
import com.pristine.exception.GeneralException;
import com.pristine.util.Constants;
import com.pristine.util.PristineDBUtil;
import com.pristine.util.offermgmt.PRConstants;

public class MultiWeekPredictionDAO {

	private static Logger logger = Logger.getLogger("MultiWeekPredictionDAO");
	/**
	 * 
	 * @param conn
	 * @param recommendationInputDTO
	 * @throws GeneralException
	 */
	public long insertPreditionRunHeader(Connection conn, RecommendationInputDTO recommendationInputDTO)
			throws GeneralException {

		long runId = getMultiWeekPredRunId(conn);

		insertPredictionHeader(conn, runId, recommendationInputDTO.getMwrRunHeader());

		return runId;
	}

	private static final String INSERT_PR_QUARTER_REC_PRED_HEADER = "INSERT INTO PR_QUARTER_REC_PRED_HEADER (RUN_ID, "
			+ " LOCATION_LEVEL_ID, LOCATION_ID, PRODUCT_LEVEL_ID, PRODUCT_ID, START_CALENDAR_ID, END_CALENDAR_ID, "
			+ " ACTUAL_START_CALENDAR_ID, ACTUAL_END_CALENDAR_ID, RUN_TYPE, START_RUN_TIME, END_RUN_TIME, "
			+ " PERCENT_COMPLETION, MESSAGE, RUN_STATUS, PREDICTED_BY, PREDICTED) "
			+ " VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, SYSDATE, NULL, ?, NULL, NULL, ?, NULL) ";

	/**
	 * 
	 * @param conn
	 * @param mwrRunHeader
	 * @throws GeneralException
	 */
	private void insertPredictionHeader(Connection conn, long runId, MWRRunHeader mwrRunHeader)
			throws GeneralException {
		PreparedStatement statement = null;
		try {
			statement = conn.prepareStatement(INSERT_PR_QUARTER_REC_PRED_HEADER);
			int counter = 0;
			statement.setLong(++counter, runId);
			statement.setInt(++counter, mwrRunHeader.getLocationLevelId());
			statement.setInt(++counter, mwrRunHeader.getLocationId());
			statement.setInt(++counter, mwrRunHeader.getProductLevelId());
			statement.setInt(++counter, mwrRunHeader.getProductId());
			statement.setInt(++counter, mwrRunHeader.getStartCalendarId());
			statement.setInt(++counter, mwrRunHeader.getEndCalendarId());
			statement.setInt(++counter, mwrRunHeader.getActualStartCalendarId());
			statement.setInt(++counter, mwrRunHeader.getActualEndCalendarId());
			statement.setString(++counter, mwrRunHeader.getRunType());
			statement.setInt(++counter, mwrRunHeader.getPercentCompleted());
			statement.setString(++counter, mwrRunHeader.getPredictedBy());
			statement.executeUpdate();
		} catch (SQLException sqlE) {
			throw new GeneralException("insertMWRRunHeader() - Error while inserting PR_QUARTER_REC_PRED_HEADER", sqlE);
		} finally {
			PristineDBUtil.close(statement);
		}
	}

	private static final String GET_PR_QUARTER_PRED_RUN_ID = "SELECT PR_QUARTER_PRED_RUN_ID_SEQ.NEXTVAL AS RUN_ID FROM DUAL";

	/**
	 * 
	 * @param conn
	 * @return run id
	 * @throws GeneralException
	 */
	private long getMultiWeekPredRunId(Connection conn) throws GeneralException {
		long runId = 0;
		PreparedStatement statement = null;
		ResultSet resultSet = null;
		try {
			statement = conn.prepareStatement(GET_PR_QUARTER_PRED_RUN_ID);
			resultSet = statement.executeQuery();
			if (resultSet.next()) {
				runId = resultSet.getLong("RUN_ID");
			}
		} catch (SQLException sqlE) {
			throw new GeneralException("getMultiWeekRecRunId() - Error while getting run id", sqlE);
		} finally {
			PristineDBUtil.close(resultSet);
			PristineDBUtil.close(statement);
		}
		return runId;
	}

	private static final String UPDATE_PR_QUARTER_REC_PRED_HEADER = "UPDATE PR_QUARTER_REC_PRED_HEADER SET PERCENT_COMPLETION = ?, "
			+ " MESSAGE = ?, RUN_STATUS = ? WHERE RUN_ID = ? ";

	/**
	 * 
	 * @param conn
	 * @param mwrRunHeader
	 * @throws GeneralException
	 */
	public void updatePredictionRunHeader(Connection conn, int percentCompleted, String message, String runStatus,
			long runId) throws GeneralException {
		PreparedStatement statement = null;
		try {
			statement = conn.prepareStatement(UPDATE_PR_QUARTER_REC_PRED_HEADER);
			int counter = 0;
			statement.setInt(++counter, percentCompleted);
			statement.setString(++counter, message);
			statement.setString(++counter, runStatus);
			statement.setLong(++counter, runId);
			statement.executeUpdate();
		} catch (SQLException sqlE) {
			throw new GeneralException("updatePredictionRunHeader() - Error while updating PR_QUARTER_REC_PRED_HEADER",
					sqlE);
		} finally {
			PristineDBUtil.close(statement);
		}
	}

	private static final String UPDATE_PR_QUARTER_REC_PRED_HEADER_END_TIME = "UPDATE PR_QUARTER_REC_PRED_HEADER SET END_RUN_TIME = SYSDATE "
			+ " WHERE RUN_ID = ? ";

	/**
	 * 
	 * @param conn
	 * @param mwrRunHeader
	 * @throws GeneralException
	 */
	public void updateEndTimeInHeader(Connection conn, long runId) throws GeneralException {
		PreparedStatement statement = null;
		try {
			statement = conn.prepareStatement(UPDATE_PR_QUARTER_REC_PRED_HEADER_END_TIME);
			int counter = 0;
			statement.setLong(++counter, runId);
			statement.executeUpdate();
		} catch (SQLException sqlE) {
			throw new GeneralException("updatePredictionRunHeader() - Error while updating PR_QUARTER_REC_PRED_HEADER",
					sqlE);
		} finally {
			PristineDBUtil.close(statement);
		}
	}

	private static final String INSERT_PR_QUARTER_REC_PRED_DATA = "INSERT INTO PR_QUARTER_REC_PRED_DATA (RUN_ID, "
			+ " CALENDAR_ID, PRODUCT_LEVEL_ID, PRODUCT_ID, REG_PRICE, REG_QUANTITY, SALE_QUANTITY, SALE_PRICE, PROMO_TYPE_ID, "
			+ " AD_PAGE_NO, AD_BLOCK_NO, DISPLAY_TYPE_ID, PREDICTED_MOVEMENT, PREDICTION_STATUS, SCENARIO_ID) "
			+ " VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?) ";

	/**
	 * 
	 * @param conn
	 * @param runId
	 * @param multiWeekPredOutput
	 * @throws GeneralException
	 */
	public void saveMultiWeekPrediction(Connection conn, long runId,
			List<MultiWeekPredEngineItemDTO> multiWeekPredOutput) throws GeneralException {
		PreparedStatement statement = null;
		try {
			statement = conn.prepareStatement(INSERT_PR_QUARTER_REC_PRED_DATA);
			int itemsInBatch = 0;
			for (MultiWeekPredEngineItemDTO multiWeekPredEngineItemDTO : multiWeekPredOutput) {

				int count = 0;
				statement.setLong(++count, runId);
				statement.setInt(++count, multiWeekPredEngineItemDTO.getCalendarId());
				statement.setInt(++count, Constants.ITEMLEVELID);
				statement.setInt(++count, multiWeekPredEngineItemDTO.getItemCode());
				statement.setDouble(++count, multiWeekPredEngineItemDTO.getRegPrice());
				statement.setInt(++count, multiWeekPredEngineItemDTO.getRegMultiple());
				statement.setInt(++count, multiWeekPredEngineItemDTO.getSaleMultiple());
				statement.setDouble(++count, multiWeekPredEngineItemDTO.getSalePrice());
				statement.setInt(++count, multiWeekPredEngineItemDTO.getPromoTypeId());
				statement.setInt(++count, multiWeekPredEngineItemDTO.getAdPageNo());
				statement.setInt(++count, multiWeekPredEngineItemDTO.getAdBlockNo());
				statement.setInt(++count, multiWeekPredEngineItemDTO.getDisplayTypeId());
				statement.setDouble(++count, multiWeekPredEngineItemDTO.getPredictedMovement());
				statement.setInt(++count, multiWeekPredEngineItemDTO.getPredictionStatus());
				statement.setInt(++count, multiWeekPredEngineItemDTO.getScenarioId());
				statement.addBatch();
				itemsInBatch++;
				if (itemsInBatch % Constants.BATCH_UPDATE_COUNT == 0) {
					statement.executeBatch();
					statement.clearBatch();
					itemsInBatch = 0;
				}
			}

			if (itemsInBatch > 0) {
				statement.executeBatch();
				statement.clearBatch();
			}
		} catch (SQLException sqlE) {
			throw new GeneralException("updatePredictionRunHeader() - Error while inserting PR_QUARTER_REC_PRED_DATA",
					sqlE);
		} finally {
			PristineDBUtil.close(statement);
		}
	}

	
	
	/**
	 * 
	 * @param conn
	 * @param mwrRunHeader
	 * @return prediction cache
	 * @throws GeneralException
	 */
	public List<MultiWeekPredEngineItemDTO> getPredictionCache(Connection conn,
			MWRRunHeader mwrRunHeader) throws GeneralException {

		long runId = getLatestPredictionRunId(conn, mwrRunHeader);

		return getPredictionCache(conn, runId);
	}
	
	
	private static final String GET_QUARTER_PRED_LATEST_RUN_ID = "SELECT MAX(RUN_ID) AS RUN_ID FROM PR_QUARTER_REC_PRED_HEADER "
			+ " WHERE LOCATION_LEVEL_ID = ? AND LOCATION_ID = ? AND PRODUCT_LEVEL_ID = ? AND PRODUCT_ID = ? "
			+ " AND ACTUAL_START_CALENDAR_ID = ? AND ACTUAL_END_CALENDAR_ID = ? AND START_CALENDAR_ID = ? AND END_CALENDAR_ID = ?"
			+ " AND RUN_STATUS = '" + PRConstants.RUN_STATUS_SUCCESS + "' ";
	
	/**
	 * 
	 * @param conn
	 * @param mwrRunHeader
	 * @return latest run id
	 * @throws GeneralException
	 */
	private long getLatestPredictionRunId(Connection conn, MWRRunHeader mwrRunHeader) throws GeneralException {
		long runId = -1;
		PreparedStatement statement = null;
		ResultSet rs = null;
		try {

			statement = conn.prepareStatement(GET_QUARTER_PRED_LATEST_RUN_ID);
			logger.debug("getLatestPredictionRunId qry : " + GET_QUARTER_PRED_LATEST_RUN_ID);
			logger.debug(
					" Parameters Passed 1. " + mwrRunHeader.getLocationLevelId() + " ;2 " + mwrRunHeader.getLocationId()
							+ " ;3 " + mwrRunHeader.getProductLevelId() + " ;4 " + mwrRunHeader.getProductId() + " ;5 "
							+ mwrRunHeader.getActualStartCalendarId() + " ;6 " + mwrRunHeader.getActualEndCalendarId()
							+ ";7 " + mwrRunHeader.getStartCalendarId() + " ;8 " + mwrRunHeader.getEndCalendarId());
			int colCount = 0;
			statement.setInt(++colCount, mwrRunHeader.getLocationLevelId());
			statement.setInt(++colCount, mwrRunHeader.getLocationId());
			statement.setInt(++colCount, mwrRunHeader.getProductLevelId());
			statement.setInt(++colCount, mwrRunHeader.getProductId());
			statement.setInt(++colCount, mwrRunHeader.getActualStartCalendarId());
			statement.setInt(++colCount, mwrRunHeader.getActualEndCalendarId());
			statement.setInt(++colCount, mwrRunHeader.getStartCalendarId());
			statement.setInt(++colCount, mwrRunHeader.getEndCalendarId());

			rs = statement.executeQuery();
			if (rs.next()) {
				runId = rs.getLong("RUN_ID");
			}
		} catch (SQLException sqlE) {
			throw new GeneralException("Error--Getting prediction runid", sqlE);
		} finally {
			PristineDBUtil.close(rs);
			PristineDBUtil.close(statement);
		}
		logger.debug(" LatestPredictionRunId  : " + runId);
		return runId;
	}
	
	
	private static final String GET_PR_QUARTER_REC_PRED_DATA = "SELECT RUN_ID, "
			+ " PD.CALENDAR_ID, TO_CHAR(RC.START_DATE, '"+ Constants.DB_DATE_FORMAT + "') AS START_DATE, "
			+ " PRODUCT_LEVEL_ID, PRODUCT_ID, REG_PRICE, REG_QUANTITY, SALE_QUANTITY, SALE_PRICE, PROMO_TYPE_ID, "
			+ " AD_PAGE_NO, AD_BLOCK_NO, DISPLAY_TYPE_ID, PREDICTED_MOVEMENT, PREDICTION_STATUS, SCENARIO_ID FROM PR_QUARTER_REC_PRED_DATA PD"
			+ " LEFT JOIN RETAIL_CALENDAR RC ON RC.CALENDAR_ID = PD.CALENDAR_ID WHERE RUN_ID = ? ";
	/**
	 * 
	 * @param conn
	 * @param runId
	 * @return prediction cache
	 * @throws GeneralException
	 */
	private List<MultiWeekPredEngineItemDTO> getPredictionCache(Connection conn,
			long runId) throws GeneralException {
		List<MultiWeekPredEngineItemDTO> predictionCache = new ArrayList<>();
		PreparedStatement statement = null;
		ResultSet rs = null;
		try {
			statement = conn.prepareStatement(GET_PR_QUARTER_REC_PRED_DATA);
			logger.debug("getPredictionCache qry: " + GET_PR_QUARTER_REC_PRED_DATA + " run id :  " + runId);
			statement.setLong(1, runId);
			rs = statement.executeQuery();
			while (rs.next()) {
				
				MultiWeekPredEngineItemDTO multiWeekPredEngineItemDTO = new MultiWeekPredEngineItemDTO();	
				multiWeekPredEngineItemDTO.setItemCode(rs.getInt("PRODUCT_ID"));
				multiWeekPredEngineItemDTO.setCalendarId(rs.getInt("CALENDAR_ID"));
				multiWeekPredEngineItemDTO.setStartDate(rs.getString("START_DATE"));
				multiWeekPredEngineItemDTO.setRegPrice(rs.getDouble("REG_PRICE"));
				multiWeekPredEngineItemDTO.setRegMultiple(rs.getInt("REG_QUANTITY"));
				multiWeekPredEngineItemDTO.setSalePrice(rs.getDouble("SALE_PRICE"));
				multiWeekPredEngineItemDTO.setSaleMultiple(rs.getInt("SALE_QUANTITY"));
				multiWeekPredEngineItemDTO.setPromoTypeId(rs.getInt("PROMO_TYPE_ID"));
				multiWeekPredEngineItemDTO.setAdPageNo(rs.getInt("AD_PAGE_NO"));
				multiWeekPredEngineItemDTO.setAdBlockNo(rs.getInt("AD_BLOCK_NO"));
				multiWeekPredEngineItemDTO.setDisplayTypeId(rs.getInt("DISPLAY_TYPE_ID"));
				multiWeekPredEngineItemDTO.setPredictedMovement(rs.getDouble("PREDICTED_MOVEMENT"));
				multiWeekPredEngineItemDTO.setPredictionStatus(rs.getInt("PREDICTION_STATUS"));
				multiWeekPredEngineItemDTO.setScenarioId(rs.getInt("SCENARIO_ID"));
				
				predictionCache.add(multiWeekPredEngineItemDTO);
			}
		} catch (SQLException sqlE) {
			throw new GeneralException("Error--Getting prediction cache", sqlE);
		} finally {
			PristineDBUtil.close(rs);
			PristineDBUtil.close(statement);
		}
		return predictionCache;
	}
}
