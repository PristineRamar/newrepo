package com.pristine.dao.offermgmt.mwr;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.*;

import com.pristine.dto.offermgmt.mwr.RecommendationQueueDTO;
import com.pristine.exception.GeneralException;
import com.pristine.util.Constants;
import com.pristine.util.*;

public class RecommendationQueueDAO {

	public List<RecommendationQueueDTO> getAllQueuedRecommendation(Connection conn) throws GeneralException {

		// Get all the Queued Recommendations

		PreparedStatement statement = null;
		ResultSet resultSet = null;
		List<RecommendationQueueDTO> queuedRecom = new ArrayList<RecommendationQueueDTO>();
		try {
			
			StringBuilder sb = new StringBuilder();
			sb.append(" SELECT QUEUE_ID, PRODUCT_ID, PRODUCT_LEVEL_ID, LOCATION_ID, LOCATION_LEVEL_ID, REC_TYPE, STRATEGY_ID, CONSIDER_SCENARIO, ");
			sb.append(" SUBMITTED_BY, TO_CHAR(SUBMITTED, 'MM/DD/YYYY HH24:MI') AS SUBMITTED_TIME FROM PR_REC_QUEUED_HEADER ");
			sb.append(" WHERE REC_QUEUED = 'Y' AND HOLD = 'N' ORDER BY SUBMITTED ASC ");
			
			statement = conn.prepareStatement(sb.toString());
			resultSet = statement.executeQuery();
			while (resultSet.next()) {
				
				RecommendationQueueDTO queueRecomDTO = new RecommendationQueueDTO();
				queueRecomDTO.setQueueId(resultSet.getLong("QUEUE_ID"));
				queueRecomDTO.setProductId(resultSet.getString("PRODUCT_ID"));
				queueRecomDTO.setProductLevelId(resultSet.getString("PRODUCT_LEVEL_ID"));
				queueRecomDTO.setLocationId(resultSet.getString("LOCATION_ID"));
				queueRecomDTO.setLocationLevelId(resultSet.getString("LOCATION_LEVEL_ID"));
				queueRecomDTO.setSubmittedTime(resultSet.getString("SUBMITTED_TIME"));
				queueRecomDTO.setSubmittedBy(resultSet.getString("SUBMITTED_BY"));
				queueRecomDTO.setRecType(resultSet.getString("REC_TYPE"));
				queueRecomDTO.setStrategyId(resultSet.getLong("STRATEGY_ID"));
				queueRecomDTO.setConsiderScenario(resultSet.getInt("CONSIDER_SCENARIO"));
				queuedRecom.add(queueRecomDTO);
			}
		}catch(SQLException sqlE) {
			throw new GeneralException("getAllQueuedRecommendation() - Error while multi week recommendations", sqlE);
		}
		
		return queuedRecom;
		
	}

	public void updateOnHoldFlag(Connection conn, String holdFlag, 
			List<RecommendationQueueDTO> queuedRecommendations) throws GeneralException {
		// Updating the On Hold Flag
		PreparedStatement statement = null;

		try {

			StringBuilder sb = new StringBuilder();
			sb.append(" UPDATE PR_REC_QUEUED_HEADER SET HOLD = '"+ holdFlag +"' WHERE QUEUE_ID = ? ");

			statement = conn.prepareStatement(sb.toString());
			int counter = 0;

			for (RecommendationQueueDTO dto : queuedRecommendations) {
				counter++;
				int colIndex = 0;
				statement.setLong(++colIndex, dto.getQueueId());
				statement.addBatch();
				if (counter % Constants.BATCH_UPDATE_COUNT == 0) {
					statement.executeBatch();
					statement.clearBatch();
					counter = 0;
				}
			}

			if (counter > 0) {
				statement.executeBatch();
				statement.clearBatch();
			}
			

		} catch (SQLException e) {
			throw new GeneralException(
					"updateHoldFlag() - Error while updating HOLD flag for recommendation in queue", e);
		} finally {
			PristineDBUtil.close(statement);
		}
	}

	public void deleteOlderQueuedRecommendation(Connection conn) throws GeneralException {
		
		PreparedStatement statement = null;
		
		try {

			StringBuilder sb = new StringBuilder();
			sb.append(" DELETE FROM PR_REC_QUEUED_HEADER WHERE SUBMITTED < (SYSDATE - 7) ");

			statement = conn.prepareStatement(sb.toString());

			statement.execute();

		} catch (SQLException e) {
			throw new GeneralException(
					"deleteOlderQueuedRecommendation() - Error while deleting the old recommendation in queue", e);
		} finally {
			PristineDBUtil.close(statement);
		}
	}

	public int getCountOfRunningRecommendations(Connection conn) throws GeneralException {
		PreparedStatement statement = null;
		ResultSet resultSet = null;
		int countOfRecommendation = 0;
		try {

			StringBuilder sb = new StringBuilder();
			sb.append(" SELECT COUNT(*) AS COUNT FROM PR_QUARTER_REC_HEADER ");
			sb.append(" WHERE PERCENT_COMPLETION <> 100 AND RUN_STATUS IS NULL AND ");
			sb.append(" (RUN_TYPE = 'D' OR RUN_TYPE = 'T' OR RUN_TYPE = 'B') AND ");
			sb.append(" (sysdate -  start_run_time)*24 <24 ");

			statement = conn.prepareStatement(sb.toString());

			resultSet = statement.executeQuery();
			while(resultSet.next()) {
				countOfRecommendation = resultSet.getInt("COUNT");	
			}

		} catch (SQLException e) {
			throw new GeneralException(
					"getCountOfRunningRecommendations() - Error while getting the count of running recomendation", e);
		} finally {
			PristineDBUtil.close(statement);
		}
		return countOfRecommendation;
	}

}
