package com.pristine.dao.offermgmt.mwr;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Types;
import java.util.Set;

import org.apache.log4j.Logger;

import com.pristine.dao.PriceTestDAO;
import com.pristine.dto.offermgmt.mwr.MWRRunHeader;
import com.pristine.dto.offermgmt.mwr.RecWeekKey;
import com.pristine.dto.offermgmt.mwr.RecommendationInputDTO;
import com.pristine.exception.GeneralException;
import com.pristine.util.PristineDBUtil;
import com.pristine.util.offermgmt.PRConstants;


public class MWRRunHeaderDAO {

	private static Logger logger = Logger.getLogger("MWRRunHeaderDAO");
	
	/**
	 * @param conn
	 * @param recommendationInputDTO
	 * @throws GeneralException
	 */
	public long insertMultiWeekRecRunHeader(Connection conn, RecommendationInputDTO recommendationInputDTO)
			throws GeneralException {

		long runId = getMultiWeekRecRunId(conn);

		long parentRunId = setParentRunId(conn, recommendationInputDTO);

		MWRRunHeader mwrRunHeader = MWRRunHeader.getRunHeaderDTO(recommendationInputDTO, runId, parentRunId);

		insertMWRRunHeader(conn, mwrRunHeader);
		
		recommendationInputDTO.setMwrRunHeader(mwrRunHeader);
		recommendationInputDTO.setRunId(runId);
		
		return runId;
	}

	private static final String GET_MULTI_WEEK_REC_RUN_ID = "SELECT PR_QUARTER_REC_RUN_ID_SEQ.NEXTVAL AS RUN_ID FROM DUAL";

	/**
	 * 
	 * @param conn
	 * @return run id
	 * @throws GeneralException
	 */
	private long getMultiWeekRecRunId(Connection conn) throws GeneralException {
		long runId = 0;
		PreparedStatement statement = null;
		ResultSet resultSet = null;
		try {
			statement = conn.prepareStatement(GET_MULTI_WEEK_REC_RUN_ID);
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

//	private static final String GET_PARENT_RUN_ID = "SELECT MAX(RUN_ID) RUN_ID FROM PR_QUARTER_REC_HEADER "
//			+ " WHERE LOCATION_LEVEL_ID = ? AND LOCATION_ID = ? " + " AND PRODUCT_LEVEL_ID = ? AND PRODUCT_ID = ? "
//			+ " AND ACTUAL_START_CALENDAR_ID = ? AND START_CALENDAR_ID = ? "
//			+ " AND ACTUAL_END_CALENDAR_ID = ? AND END_CALENDAR_ID = ?";
	//Updated by Bhargavi 3/26/2021
	//updated the query to fetch the parent run id from the last successful run in that quarter instead of the previous week
	private static final String GET_PARENT_RUN_ID = "SELECT RUN_ID "
			+ "FROM (SELECT RUN_ID, RANK() OVER (PARTITION BY LOCATION_LEVEL_ID, LOCATION_ID, "
			+ "PRODUCT_LEVEL_ID, PRODUCT_ID, RUN_STATUS ORDER BY PRH.START_RUN_TIME DESC) AS RANK "
			+ "FROM PR_QUARTER_REC_HEADER PRH LEFT JOIN RETAIL_CALENDAR RC ON RC.CALENDAR_ID = PRH.START_CALENDAR_ID "
			+ "and RC.ROW_TYPE = 'W' WHERE PRH.LOCATION_LEVEL_ID = ?  AND  PRH.LOCATION_ID = ?  AND PRH.PRODUCT_LEVEL_ID = ? "
			+ "AND PRH.PRODUCT_ID = ?  AND PRH.RUN_STATUS = 'S' AND  PRH.RUN_TYPE IN ('B', 'D') AND  PRH.ACTUAL_START_CALENDAR_ID = ? "
			+ "AND PRH.ACTUAL_END_CALENDAR_ID = ? AND PRH.END_CALENDAR_ID = ? AND RC.START_DATE <= "
			+ "(TO_DATE(?, 'MM/DD/YYYY') - 7)) WHERE RANK=1";
	

	/**
	 * 
	 * @param conn
	 * @param recommendationInputDTO
	 * @return parent run id
	 * @throws GeneralException
	 */
	public long setParentRunId(Connection conn, RecommendationInputDTO recommendationInputDTO)
			throws GeneralException {
		long runId = 0;
		PreparedStatement statement = null;
		ResultSet resultSet = null;
		logger.info("Start Week: " +  recommendationInputDTO.getStartWeek());
		try {
			statement = conn.prepareStatement(GET_PARENT_RUN_ID);
			int count = 0;
			statement.setInt(++count, recommendationInputDTO.getLocationLevelId());
			statement.setInt(++count, recommendationInputDTO.getLocationId());
			statement.setInt(++count, recommendationInputDTO.getProductLevelId());
			statement.setInt(++count, recommendationInputDTO.getProductId());
			statement.setInt(++count, recommendationInputDTO.getActualStartCalendarId());
//			statement.setInt(++count, recommendationInputDTO.getActualStartCalendarId());
			statement.setInt(++count, recommendationInputDTO.getActualEndCalendarId());
			statement.setInt(++count, recommendationInputDTO.getActualEndCalendarId());
			statement.setString(++count, recommendationInputDTO.getStartWeek());
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

	
	
	private static final String INSERT_PR_QUARTER_REC_HEADER = "INSERT INTO PR_QUARTER_REC_HEADER (RUN_ID, "
			+ " LOCATION_LEVEL_ID, LOCATION_ID, PRODUCT_LEVEL_ID, PRODUCT_ID, START_CALENDAR_ID, END_CALENDAR_ID, "
			+ " ACTUAL_START_CALENDAR_ID, ACTUAL_END_CALENDAR_ID, RUN_TYPE, START_RUN_TIME, END_RUN_TIME, "
			+ " PERCENT_COMPLETION, MESSAGE, RUN_STATUS, PREDICTED_BY, PREDICTED, PARENT_RUN_ID) "
			+ " VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, SYSDATE, NULL, ?, NULL, NULL, ?, NULL, ?) ";

	/**
	 * 
	 * @param conn
	 * @param mwrRunHeader
	 * @throws GeneralException
	 */
	private void insertMWRRunHeader(Connection conn, MWRRunHeader mwrRunHeader) throws GeneralException {
		PreparedStatement statement = null;
		try {
			statement = conn.prepareStatement(INSERT_PR_QUARTER_REC_HEADER);
			int counter = 0;
			statement.setLong(++counter, mwrRunHeader.getRunId());
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
			statement.setLong(++counter, mwrRunHeader.getParentRunId());
			statement.executeUpdate();
		} catch (SQLException sqlE) {
			throw new GeneralException("insertMWRRunHeader() - Error while inserting PR_QUARTER_REC_HEADER", sqlE);
		} finally {
			PristineDBUtil.close(statement);
		}
	}

	
	private static final String UPDATE_PR_QUARTER_REC_HEADER = "UPDATE PR_QUARTER_REC_HEADER SET PERCENT_COMPLETION = ?, "
			+ " MESSAGE = ?, RUN_STATUS = ?, STATUS = ?, PREDICTED = SYSDATE, STATUS_DATE = SYSDATE, STATUS_BY = ?  WHERE RUN_ID = ? ";

	/**
	 * 
	 * @param conn
	 * @param mwrRunHeader
	 * @throws GeneralException
	 */
	public void updateMWRRunHeader(Connection conn, MWRRunHeader mwrRunHeader) throws GeneralException {
		PreparedStatement statement = null;
		try {
			statement = conn.prepareStatement(UPDATE_PR_QUARTER_REC_HEADER);
			int counter = 0;
			statement.setInt(++counter, mwrRunHeader.getPercentCompleted());
			statement.setString(++counter, mwrRunHeader.getMessage());
			statement.setString(++counter, mwrRunHeader.getRunStatus());
			if (mwrRunHeader.getPercentCompleted() == 100) {
				statement.setInt(++counter,
						mwrRunHeader.getRunStatus().equals(PRConstants.RUN_STATUS_ERROR) ? PRConstants.ERROR_REC
								: PRConstants.STATUS_RECOMMENDED);
				statement.setString(++counter, PRConstants.BATCH_USER);
			} else {
				statement.setNull(++counter, Types.INTEGER);
				statement.setNull(++counter, Types.VARCHAR);
			}
			statement.setLong(++counter, mwrRunHeader.getRunId());
			statement.executeUpdate();
		} catch (SQLException sqlE) {
			throw new GeneralException("insertMWRRunHeader() - Error while inserting PR_QUARTER_REC_HEADER", sqlE);
		} finally {
			PristineDBUtil.close(statement);
		}
	}
	
	
	private static final String UPDATE_PR_QUARTER_REC_HEADER_END_TIME = "UPDATE PR_QUARTER_REC_HEADER SET END_RUN_TIME = SYSDATE "
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
			statement = conn.prepareStatement(UPDATE_PR_QUARTER_REC_HEADER_END_TIME);
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
	
	
	
	private static final String INSERT_PR_QUARTER_WEEK_REC_HEADER = "INSERT INTO PR_QUARTER_WEEK_REC_HEADER (RUN_ID, WEEK_CALENDAR_ID, WEEK_TYPE) "
			+ " VALUES (?, ?, ?)";

	/**
	 * 
	 * @param conn
	 * @param mwrRunHeader
	 * @throws GeneralException
	 */
	public void insertWeeklyRunHeader(Connection conn, RecommendationInputDTO recommendationInputDTO,
			Set<RecWeekKey> weekSet) throws GeneralException {
		PreparedStatement statement = null;
		try {
			statement = conn.prepareStatement(INSERT_PR_QUARTER_WEEK_REC_HEADER);
			for(RecWeekKey recWeekKey: weekSet){
				int counter = 0;
				statement.setLong(++counter, recommendationInputDTO.getRunId());
				statement.setInt(++counter, recWeekKey.calendarId);
				statement.setString(++counter, recWeekKey.recWeekType);
				statement.addBatch();
			}
			statement.executeBatch();
		} catch (SQLException sqlE) {
			throw new GeneralException("insertWeeklyRunHeader() - Error while inserting PR_QUARTER_REC_HEADER", sqlE);
		} finally {
			PristineDBUtil.close(statement);
		}
	}
	
	
	private static final String INSERT_RECOMMENDATION_STATUS = "INSERT INTO PR_QUARTER_REC_STATUS "
			+ "(RECOMMENDATION_STATUS_ID, RUN_ID, STATUS, UPDATED_BY, UPDATED, MESSAGE) "
			+ " VALUES (RECOMMENDATION_STATUS_ID_SEQ.NEXTVAL, ?, ?, ?, SYSDATE, ?)";

	/**
	 * 
	 * @param conn
	 * @param mwrRunHeader
	 * @throws GeneralException
	 */
	public void insertRecommendationStatus(Connection conn, MWRRunHeader mwrRunHeader
			, RecommendationInputDTO recommendationInputDTO) throws GeneralException {
		PreparedStatement statement = null;
		try {
			statement = conn.prepareStatement(INSERT_RECOMMENDATION_STATUS);
			int counter = 0;
			statement.setLong(++counter, mwrRunHeader.getRunId());
			statement.setInt(++counter, PRConstants.STATUS_RECOMMENDED);
			statement.setString(++counter, recommendationInputDTO.getUserId());
			statement.setString(++counter, mwrRunHeader.getMessage());
			statement.executeUpdate();
		} catch (SQLException sqlE) {
			throw new GeneralException("insertMWRRunHeader() - Error while inserting PR_RECOMMENDATION_STATUS", sqlE);
		} finally {
			PristineDBUtil.close(statement);
		}
	}
	
	private static final String GET_RECC_STATUS = "SELECT IS_AUTOMATIC_RECOMMENDATION FROM PR_QUARTER_DASHBOARD "
			+ " WHERE LOCATION_LEVEL_ID=? AND LOCATION_ID=? AND PRODUCT_LEVEL_ID=? AND PRODUCT_ID=?";

	public String getReccomendationStatus(Connection conn, RecommendationInputDTO recommendationInputDTO)
			throws GeneralException {
		String status = null;
		String priceTestStatus=null;
		PreparedStatement statement = null;
		ResultSet resultSet = null;
	
		PriceTestDAO priceTestDAO = new PriceTestDAO();
		try {
			logger.debug("recommendationInputDTO.getLocationLevelId(): " + recommendationInputDTO.getLocationLevelId()
					+ " recommendationInputDTO.getLocationId()" + recommendationInputDTO.getLocationId()
					+ " recommendationInputDTO.getProductLevelId()" + recommendationInputDTO.getProductLevelId()
					+ " recommendationInputDTO.getProductId()" + recommendationInputDTO.getProductId());

			statement = conn.prepareStatement(GET_RECC_STATUS);
			int count = 0;
			statement.setInt(++count, recommendationInputDTO.getLocationLevelId());
			statement.setInt(++count, recommendationInputDTO.getLocationId());
			statement.setInt(++count, recommendationInputDTO.getProductLevelId());
			statement.setInt(++count, recommendationInputDTO.getProductId());

			resultSet = statement.executeQuery();
			if (resultSet.next()) {
				status = resultSet.getString("IS_AUTOMATIC_RECOMMENDATION");
			}
		} catch (SQLException sqlE) {
			logger.error("getReccomendationStatus() query -" + GET_RECC_STATUS);
			throw new GeneralException("Error -- getReccomendationStatus()", sqlE);
		} finally {
			PristineDBUtil.close(resultSet);
			PristineDBUtil.close(statement);
		}

		if (recommendationInputDTO.isPriceTestZone()) {

			priceTestStatus = priceTestDAO.getPriceTestStatus(conn, recommendationInputDTO);
		}
		if (status == null && !recommendationInputDTO.isPriceTestZone()) {

			status = "Y";
		} 
		else if (recommendationInputDTO.isPriceTestZone()) {
			if (status != null && priceTestStatus != null) {
				if (!status.equalsIgnoreCase("N")) {
					status = priceTestStatus;
				}

			} else if (priceTestStatus.equalsIgnoreCase("N")) {
				status = "N";
			} else {
				status = priceTestStatus;
			}

		}
		return status;
	}

	private static final String UPDATE_PR_REC_QUEUED_HEADER = "UPDATE PR_REC_QUEUED_HEADER SET RUN_ID = ?, HOLD='N',REC_QUEUED='N'"
			+ " WHERE QUEUE_ID = ? ";
	
	/**
	 * 
	 * @param conn
	 * @param recommendationInputDTO
	 * @throws GeneralException 
	 */
	public void updateRecQueueHeader(Connection conn, RecommendationInputDTO recommendationInputDTO) throws GeneralException {
		
		PreparedStatement statement = null;
		try {
			statement = conn.prepareStatement(UPDATE_PR_REC_QUEUED_HEADER);
			int counter = 0;
			statement.setLong(++counter, recommendationInputDTO.getRunId());
			statement.setLong(++counter, recommendationInputDTO.getQueueId());
			
			statement.executeUpdate();
		} catch (SQLException sqlE) {
			throw new GeneralException("updateRecQueueHeader() - Error while updating PR_REC_QUEUED_HEADER",
					sqlE);
		} finally {
			PristineDBUtil.close(statement);
		}
		
	}

	
	private static final String INSERT_INTO_PR_STRATEGY_SCENARIO = "INSERT INTO PR_STRATEGY_SCENARIO (STRATEGY_SCENARIO_ID, LOCATION_LEVEL_ID, LOCATION_ID,PRODUCT_LEVEL_ID, PRODUCT_ID, APPLY_TO, VENDOR_ID, CRITERIA_ID ,STATE_ID, STRATEGY_ID, RUN_ID, STRATEGY_ID_ALL,COMPLETION_STATUS) "
			+ " VALUES (PR_STRATEGY_SCENARIO_SEQ.NEXTVAL,?,?,?,?,?,?,?,?,?,?,?,?)";
		
	
	public void insertScenario(Connection conn, RecommendationInputDTO recommendationInputDTO, long queueId) throws GeneralException {
		PreparedStatement statement = null;
		try {
			statement = conn.prepareStatement(INSERT_INTO_PR_STRATEGY_SCENARIO);
			int counter = 0;
			statement.setInt(++counter, recommendationInputDTO.getLocationLevelId());
			statement.setInt(++counter, recommendationInputDTO.getLocationId());
			statement.setInt(++counter, recommendationInputDTO.getProductLevelId());
			statement.setInt(++counter, recommendationInputDTO.getProductId());
			statement.setInt(++counter, -1);
			statement.setInt(++counter, -1);
			statement.setInt(++counter, -1);
			statement.setInt(++counter,-1);
			statement.setLong(++counter,recommendationInputDTO.getStrategyId());
			statement.setLong(++counter,recommendationInputDTO.getRunId());
			statement.setString(++counter,String.valueOf(recommendationInputDTO.getStrategyId()));
			statement.setInt(++counter,0);
			statement.executeUpdate();
		} catch (SQLException sqlE) {
			throw new GeneralException("insertScenario() - Error while INSERT_INTO_PR_STRATEGY_SCENARIO ",
					sqlE);
		} finally {
			PristineDBUtil.close(statement);
		}
		
	}
	

	private static final String UPDATE_PR_QUARTER_REC_STATUS = "UPDATE PR_QUARTER_REC_STATUS SET UPDATED_BY=? WHERE RUN_ID = ? ";

	/**
	 * 
	 * @param conn
	 * @param recommendationInputDTO
	 * @throws GeneralException
	 */
	public void updateRecStatusUser(Connection conn, RecommendationInputDTO recommendationInputDTO)
			throws GeneralException {

		PreparedStatement statement = null;
		try {
			statement = conn.prepareStatement(UPDATE_PR_QUARTER_REC_STATUS);
			int counter = 0;
			statement.setString(++counter, recommendationInputDTO.getUserId());
			statement.setLong(++counter, recommendationInputDTO.getRunId());

			statement.executeUpdate();
		} catch (SQLException sqlE) {
			throw new GeneralException("updateRecStatusUser() - Error while updating PR_QUARTER_REC_STATUS", sqlE);
		} finally {
			PristineDBUtil.close(statement);
		}

	}

	private static final String UPDATE_PR_QUARTER_REC_HEADER_STATUS = "UPDATE PR_QUARTER_REC_HEADER SET PREDICTED_BY =? WHERE RUN_ID = ? ";

	public void updateRecHeaderUser(Connection conn, RecommendationInputDTO recommendationInputDTO)
			throws GeneralException {
		PreparedStatement statement = null;
		try {
			statement = conn.prepareStatement(UPDATE_PR_QUARTER_REC_HEADER_STATUS);
			int counter = 0;
			statement.setString(++counter, recommendationInputDTO.getUserId());
			statement.setLong(++counter, recommendationInputDTO.getRunId());

			statement.executeUpdate();
		} catch (SQLException sqlE) {
			throw new GeneralException("updateRecHeaderUser() - Error while updating PR_QUARTER_REC_HEADER_STATUS",
					sqlE);
		} finally {
			PristineDBUtil.close(statement);
		}

	}

	
	private static final String UPDATE_PR_STRATEGY_SCENARIO = "UPDATE PR_STRATEGY_SCENARIO SET COMPLETION_STATUS=? WHERE RUN_ID=?";
		
	
	public void updateScenario(Connection conn, long runId,int status) throws GeneralException {
		
		
		PreparedStatement statement = null;
		try {
			statement = conn.prepareStatement(UPDATE_PR_STRATEGY_SCENARIO);
			int counter = 0;
			statement.setInt(++counter, status);
			statement.setLong(++counter, runId);

			statement.executeUpdate();
		} catch (SQLException sqlE) {
			throw new GeneralException("updateScenario() - Error while updating PR_QUARTER_REC_STATUS", sqlE);
		} finally {
			PristineDBUtil.close(statement);
		}

		
	}
	
	
	
}
