package com.pristine.dao.offermgmt;

import java.sql.Connection;
import java.sql.PreparedStatement;
//import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Types;
import java.util.List;

import org.apache.log4j.Logger;

import com.pristine.dto.offermgmt.PRRecItemAnalysis;
//import com.pristine.dto.offermgmt.PRRecommendation;
import com.pristine.dto.offermgmt.PRRecommendationCompDetail;
import com.pristine.dto.offermgmt.RecommendationAnalysisDTO;
import com.pristine.exception.GeneralException;
import com.pristine.util.Constants;
import com.pristine.util.PristineDBUtil;

public class RecommendationAnalysisDAO {

	private static Logger logger = Logger.getLogger("RecommendationAnalysisDAO");

//	private static final String GET_PR_RECOMMENDATION = " SELECT CUR_REG_MULTIPLE, CUR_REG_PRICE, RECOMMENDED_REG_MULTIPLE, RECOMMENDED_REG_PRICE,"
//			+ "PREDICTED_MOVEMENT, PREDICTION_STATUS, IS_PRICE_RECOMMENDED"
//			+ " FROM PR_RECOMMENDATION WHERE RUN_ID = ? and LIR_IND = 'N'";

	private static final String INSERT_RECOMMENDATION = "INSERT INTO PR_RECOMMENDATION_ANALYSIS"
			+ "(RUN_ID, HAS_PREDICATIONS, HAS_ONE_NEW_PRICE, HAS_ONE_RECOMMENDATION)" + "values(?,?,?,?)";

	private static final String DELETE_FROM_RECOMMENDED_ANALYSIS = "DELETE FROM PR_RECOMMENDATION_ANALYSIS WHERE RUN_ID = ?";
	
	private static final String DELETE_RECOMMENDATION_COMP_DETAIL = "DELETE FROM PR_RECOMMENDATION_COMP_DETAILS WHERE RUN_ID = ?";

	private static final String INSERT_RECOMMENDATION_COMP_DETAIL = "INSERT INTO PR_RECOMMENDATION_COMP_DETAILS"
			+ "(RUN_ID, LOCATION_LEVEL_ID, LOCATION_ID, TOTAL_ITEMS, DIST_TOTAL_ITEMS, CURRENT_SIMPLE_INDEX, "
			+ "FUTURE_SIMPLE_INDEX, PRICE_CHECK_LIST_ID)" + " VALUES (?,?,?,?,?,?,?,?)";
	
	private static final String INSERT_REC_ITEM_ANALYSIS = "INSERT INTO PR_REC_ITEM_ANALYSIS"
			+ "(PR_RECOMMENDATION_ID, IS_FRCST_UP_RETAIL_UP, IS_FRCST_DOWN_RETAIL_DOWN, IS_HT_X_WEEKS_AVG, IS_LT_X_WEEKS_AVG,"
			+ "IS_LT_MIN_SOLD_IN_X_WEEKS, IS_HT_MAX_SOLD_IN_X_WEEKS) "
			+ "VALUES (?,?,?,?,?,?,?)";
	
	
	private static final String DELETE_REC_ITEM_ANALYSIS = "DELETE FROM PR_REC_ITEM_ANALYSIS WHERE PR_RECOMMENDATION_ID IN "
			+ " (SELECT PR_RECOMMENDATION_ID FROM PR_RECOMMENDATION WHERE RUN_ID = ?) ";
	 
	
	/**
	 * to get recommendationAnalysis values
	 * 
	 * @param connection
	 * @param runId
	 * @return
	 * @throws GeneralException
	 */
//	public PRRecommendation getRecommendationAnlaysis(Connection connection, long runId) throws GeneralException {
//		PRRecommendation prRecommendation = new PRRecommendation();
//		PreparedStatement statement = null;
//		ResultSet resultSet = null;
//
//		try {
//
//			String query = new String(GET_PR_RECOMMENDATION);
////			logger.info("statement query executing:" + query);
//			statement = connection.prepareStatement(query);
//
//			int counter = 0;
//			statement.setLong(++counter, runId);
//			resultSet = statement.executeQuery();
//			logger.debug("GET_PR_RECOMMENDATION query executed");
//
//			while (resultSet.next()) {
//
//				prRecommendation.setRunId(runId);
//				prRecommendation.setCurrentRegmultiple(resultSet.getInt("CUR_REG_MULTIPLE"));
//				prRecommendation.setCurrentRegPrice(resultSet.getDouble("CUR_REG_PRICE"));
//				prRecommendation.setRecRegMultiple(resultSet.getInt("RECOMMENDED_REG_MULTIPLE"));
//				prRecommendation.setRecRegPrice(resultSet.getDouble("RECOMMENDED_REG_PRICE"));
//				prRecommendation.setPredictedMovement(resultSet.getDouble("PREDICTED_MOVEMENT"));
//				prRecommendation.setPredictionStatus(resultSet.getInt("PREDICTION_STATUS"));
//				prRecommendation.setPriceRecommended(resultSet.getInt("IS_PRICE_RECOMMENDED"));
//			}
//		} catch (SQLException e) {
//			logger.error("Error while executing GET_PR_RECOMMENDATION " + e.toString(), e);
//			throw new GeneralException("Error while executing GET_PR_RECOMMENDATION " + e.toString(), e);
//		} finally {
//			PristineDBUtil.close(resultSet);
//			PristineDBUtil.close(statement);
//		}
//		return prRecommendation;
//
//	}

	/**
	 * Insert Recommendation Analysis details into the table
	 * PR_RECOMMENDATION_ANALYSIS
	 * 
	 * @param connection
	 * @param recommendationAnalysisDTO
	 * @throws GeneralException
	 */
	public void insertRecommendationDetails(Connection connection, RecommendationAnalysisDTO recommendationAnalysisDTO)
			throws GeneralException {

		PreparedStatement statement = null;
		try {
			statement = connection.prepareStatement(INSERT_RECOMMENDATION);
			logger.debug("INSERT_RECOMMENDATION " + statement);

			int counter = 0;
			statement.setLong(++counter, recommendationAnalysisDTO.getRunId());
			if (recommendationAnalysisDTO.isHasPredictions()) {
				statement.setString(++counter, String.valueOf('Y'));
			} else {
				statement.setString(++counter, String.valueOf('N'));
			}
			if (recommendationAnalysisDTO.isHasAtleastOneNewPrice()) {
				statement.setString(++counter, String.valueOf('Y'));
			} else {
				statement.setString(++counter, String.valueOf('N'));
			}
			if (recommendationAnalysisDTO.isHasAtleastOneRecommendation()) {
				statement.setString(++counter, String.valueOf('Y'));
			} else {
				statement.setString(++counter, String.valueOf('N'));
			}

			statement.executeUpdate();
			statement.clearBatch();
			
		} catch (SQLException e) {
			logger.error("Error in insertRecommendationDetails()" + e.toString(), e);
			throw new GeneralException("Error in insertRecommendationDetails() " + e.toString(), e);
		} finally {
			PristineDBUtil.close(statement);
		}
	}

	/**
	 * delete row for the exiting runID
	 * @param connection
	 * @param recommendationAnalysisDTO
	 * @throws GeneralException
	 */
	public void deleteFromRecommendedAnalysis(Connection connection,
			RecommendationAnalysisDTO recommendationAnalysisDTO) throws GeneralException {
		PreparedStatement statement = null;
		try {
			statement = connection.prepareStatement(DELETE_FROM_RECOMMENDED_ANALYSIS);
			logger.debug("DELETE_IN_RECOMMENDED_ANALYSIS " + statement);
			int counter = 0;
			statement.setLong(++counter, recommendationAnalysisDTO.getRunId());
			statement.executeUpdate();
			statement.clearBatch();

		} catch (SQLException e) {
			logger.error("Error in deleteExistingTable()" + e.toString(), e);
			throw new GeneralException("Error in deleteExistingTable() " + e.toString(), e);
		} finally {
			PristineDBUtil.close(statement);
		}
	}
	
	public void deleteRecommendationCompDetail(Connection connection, long runId) throws GeneralException {
		PreparedStatement statement = null;
		try {
			statement = connection.prepareStatement(DELETE_RECOMMENDATION_COMP_DETAIL);
			logger.debug("DELETE_RECOMMENDATION_COMP_DETAIL " + statement);
			int counter = 0;
			statement.setLong(++counter, runId);
			statement.executeUpdate();
			statement.clearBatch();

		} catch (SQLException e) {
			logger.error("Error in deleteRecommendationCompDetail()" + e.toString(), e);
			throw new GeneralException("Error in deleteRecommendationCompDetail() " + e.toString(), e);
		} finally {
			PristineDBUtil.close(statement);
		}
	}
	
	public void insertRecommendationCompDetail(Connection connection, List<PRRecommendationCompDetail> compDetails) throws GeneralException {
		PreparedStatement stmt = null;
		try {
			stmt = connection.prepareStatement(INSERT_RECOMMENDATION_COMP_DETAIL);
			logger.debug("INSERT_RECOMMENDATION_COMP_DETAIL " + stmt);

			int itemNoInBatch = 0;
			for (PRRecommendationCompDetail compDetail : compDetails) {
				int counter = 0;
				stmt.setLong(++counter, compDetail.getRunId());
				stmt.setInt(++counter, compDetail.getLocationLevelId());
				stmt.setInt(++counter, compDetail.getLocationId());
				stmt.setInt(++counter, compDetail.getTotalItems());
				stmt.setInt(++counter, compDetail.getTotalDistinctItems());
				
				if(compDetail.getCurrentSimpleIndex() != null) {
					stmt.setDouble(++counter, compDetail.getCurrentSimpleIndex());
				} else {
					stmt.setNull(++counter, Types.DOUBLE);
				}
			
				if(compDetail.getFutureSimpleIndex() != null) {
					stmt.setDouble(++counter, compDetail.getFutureSimpleIndex());
				} else {
					stmt.setNull(++counter, Types.DOUBLE);
				}
				
				if(compDetail.getPriceCheckListId() != null) {
					stmt.setInt(++counter, compDetail.getPriceCheckListId());
				} else {
					stmt.setNull(++counter, Types.INTEGER);
				}

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
		} catch (SQLException e) {
			logger.error("Error in insertRecommendationCompDetail()" + e.toString(), e);
			throw new GeneralException("Error in insertRecommendationCompDetail() " + e.toString(), e);
		} finally {
			PristineDBUtil.close(stmt);
		}
	}
	
	public void insertRecommendationItemAnalysis(Connection connection, List<PRRecItemAnalysis> recAnalysisItems) throws GeneralException {
		PreparedStatement stmt = null;
		try {
			stmt = connection.prepareStatement(INSERT_REC_ITEM_ANALYSIS);
			logger.debug("INSERT_REC_ITEM_ANALYSIS " + stmt);

			int itemNoInBatch = 0;
			for (PRRecItemAnalysis recItemAnalysis : recAnalysisItems) {
				int counter = 0;
				stmt.setLong(++counter, recItemAnalysis.getRecommendationId());
				stmt.setInt(++counter, recItemAnalysis.getIsForecastUpRetailUp());
				stmt.setInt(++counter, recItemAnalysis.getIsForecastDownRetailDown());
				stmt.setInt(++counter, recItemAnalysis.getIsHigherThanXWeeksAvg());
				stmt.setInt(++counter, recItemAnalysis.getIsLowerThanXWeeksAvg());
				stmt.setInt(++counter, recItemAnalysis.getIsLowerThanMinSoldInXWeeks());
				stmt.setInt(++counter, recItemAnalysis.getIsHigherThanMaxSoldInXWeeks());

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
		} catch (SQLException e) {
			logger.error("Error in insertRecommendationItemAnalysis()" + e.toString(), e);
			throw new GeneralException("Error in insertRecommendationItemAnalysis() " + e.toString(), e);
		} finally {
			PristineDBUtil.close(stmt);
		}
	}
	
	public void deleteRecommendationItemAnalysis(Connection connection, long runId) throws GeneralException {
		PreparedStatement statement = null;
		try {
			statement = connection.prepareStatement(DELETE_REC_ITEM_ANALYSIS);
			logger.debug("DELETE_REC_ITEM_ANALYSIS " + statement);
			int counter = 0;
			statement.setLong(++counter, runId);
			statement.executeUpdate();
			statement.clearBatch();

		} catch (SQLException e) {
			logger.error("Error in deleteRecommendationItemAnalysis()" + e.toString(), e);
			throw new GeneralException("Error in deleteRecommendationItemAnalysis() " + e.toString(), e);
		} finally {
			PristineDBUtil.close(statement);
		}
	}
}
