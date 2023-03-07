package com.pristine.dao.offermgmt.promotion;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import com.pristine.dto.offermgmt.ProductKey;
import com.pristine.dto.offermgmt.mwr.RecWeekKey;
import com.pristine.dto.offermgmt.mwr.RecommendationInputDTO;
import com.pristine.dto.offermgmt.promotion.PromoItemDTO;
import com.pristine.exception.GeneralException;
import com.pristine.util.Constants;
import com.pristine.util.PristineDBUtil;

public class PromoExplainationDAO {
	
	
	/**
	 * 
	 * @param conn
	 * @param recommendationInputDTO
	 * @param candidateItemMap
	 * @param itemGroups
	 * @throws GeneralException
	 */
	public void savePromoExplanationData(Connection conn, RecommendationInputDTO recommendationInputDTO,
			HashMap<RecWeekKey, HashMap<ProductKey, List<PromoItemDTO>>> candidateItemMap) throws GeneralException {

		for (Map.Entry<RecWeekKey, HashMap<ProductKey, List<PromoItemDTO>>> weekEntry : candidateItemMap.entrySet()) {

			HashMap<Integer, HashMap<ProductKey, List<PromoItemDTO>>> groupMap = getGroupMap(weekEntry.getValue());
			
			long headerId = getSequenceValue(conn, "OR_PROMO_HEADER_SEQ");

			recommendationInputDTO.setPromoHeaderId(headerId);

			saveHeaderData(conn, recommendationInputDTO);

			saveScenarios(conn, recommendationInputDTO, groupMap);

			saveCandidateItems(conn, groupMap);

			savePromoOptions(conn, groupMap);
		}
	}
	
	
	
	
	private static final String INSERT_PR_PROMO_HEADER = "INSERT INTO OR_PROMO_HEADER (PROMO_HEADER_ID, "
			+ "LOCATION_LEVEL_ID, LOCATION_ID, PRODUCT_LEVEL_ID, PRODUCT_ID, START_CALENDAR_ID, "
			+ "END_CALENDAR_ID, CALENDAR_TYPE) VALUES(?, ?, ?, ?, ?, ?, ?, ?)";
	
	
	
	public void saveHeaderData(Connection conn, RecommendationInputDTO recommendationInputDTO) throws GeneralException {
		PreparedStatement statement = null;
		try {
			statement = conn.prepareStatement(INSERT_PR_PROMO_HEADER);
			int counter = 0;
			statement.setLong(++counter, recommendationInputDTO.getPromoHeaderId());
			statement.setInt(++counter, recommendationInputDTO.getLocationLevelId());
			statement.setInt(++counter, recommendationInputDTO.getLocationId());
			statement.setInt(++counter, recommendationInputDTO.getProductLevelId());
			statement.setInt(++counter, recommendationInputDTO.getProductId());
			statement.setInt(++counter, recommendationInputDTO.getStartCalendarId());
			statement.setInt(++counter, recommendationInputDTO.getEndCalendarId());
			statement.setString(++counter, Constants.CALENDAR_WEEK);
			
			statement.executeUpdate();
		} catch (SQLException sqlE) {
			throw new GeneralException("insertMWRRunHeader() - Error while inserting PR_PROMO_HEADER", sqlE);
		} finally {
			PristineDBUtil.close(statement);
		}
	}
	
	
	private static final String INSERT_PR_PROMO_SCENARIO = "INSERT INTO OR_PROMO_SCENARIO (PROMO_SCENARIO_ID, "
			+ "PROMO_HEADER_ID, ITEM_SUB_GROUP_ID, PROMO_OBJECTIVE_TYPE_ID) VALUES(?, ?, ?, ?)";
	
	
	/**
	 * 
	 * @param conn
	 * @param recommendationInputDTO
	 * @param itemGroups
	 * @throws GeneralException 
	 */
	private void saveScenarios(Connection conn, RecommendationInputDTO recommendationInputDTO,
			HashMap<Integer, HashMap<ProductKey, List<PromoItemDTO>>> itemGroups) throws GeneralException {

		PreparedStatement statement = null;
		try {
			statement = conn.prepareStatement(INSERT_PR_PROMO_SCENARIO);
			for(Map.Entry<Integer, HashMap<ProductKey, List<PromoItemDTO>>> groupEntry: itemGroups.entrySet()) {
				
				long scenarioId = getSequenceValue(conn, "OR_PROMO_SCENARIO_SEQ");
				
				setScenarioId(groupEntry.getValue(), scenarioId);
				
				int objectiveTypeId = getObjectiveTypeId(groupEntry.getValue());
				
				int counter = 0;
				statement.setLong(++counter, scenarioId);
				statement.setLong(++counter, recommendationInputDTO.getPromoHeaderId());
				statement.setInt(++counter, groupEntry.getKey());
				statement.setInt(++counter, objectiveTypeId);
				statement.addBatch();
			}
			
			statement.executeBatch();
		} catch (SQLException sqlE) {
			throw new GeneralException("saveScenarios() - Error while inserting INSERT_PR_PROMO_HEADER", sqlE);
		} finally {
			PristineDBUtil.close(statement);
		}
	}
	
	
	
	
	private static final String INSERT_PR_PROMO_CANDIDATE_ITEMS = "INSERT INTO OR_PROMO_CANDIDATE_ITEMS (PROMO_CANDIDATE_ITEMS_ID, "
			+ " PROMO_SCENARIO_ID, PRODUCT_LEVEL_ID, PRODUCT_ID, FUNDING_AVAILABLE, PREV_PROMO_DURATION, PROMO_FREQUENCY_TYPE, "
			+ " PROMO_FREQUENCY, REG_RETAIL, REG_QTY, LIST_COST, UNITS, SALES, MARGIN, SHORT_LISTED, "
			+ " IS_GROUP_PROMO, ANCHOR_PRD_LEVEL_ID, ANCHOR_PRD_ID) "
			+ " VALUES(?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)";
	
	
	/**
	 * 
	 * @param conn
	 * @param itemGroups
	 * @throws GeneralException
	 */
	private void saveCandidateItems(Connection conn,
			HashMap<Integer, HashMap<ProductKey, List<PromoItemDTO>>> itemGroups) throws GeneralException {

		PreparedStatement statement = null;
		try {
			statement = conn.prepareStatement(INSERT_PR_PROMO_CANDIDATE_ITEMS);
			for (Map.Entry<Integer, HashMap<ProductKey, List<PromoItemDTO>>> groupEntry : itemGroups.entrySet()) {
				for (Map.Entry<ProductKey, List<PromoItemDTO>> itemEntry : groupEntry.getValue().entrySet()) {
					long candidateItemsId = getSequenceValue(conn, "OR_PROMO_CANDIDATE_SEQ");

					setCandidateItemsId(itemEntry.getValue(), candidateItemsId);

					PromoItemDTO itemDTO = itemEntry.getValue().get(0);

					int counter = 0;
					statement.setLong(++counter, candidateItemsId);
					statement.setLong(++counter, itemDTO.getScenarioId());
					statement.setInt(++counter, itemEntry.getKey().getProductLevelId());
					statement.setInt(++counter, itemEntry.getKey().getProductId());
					statement.setString(++counter, itemDTO.isFundingAvailable() ? String.valueOf(Constants.YES)
							: String.valueOf(Constants.NO));
					statement.setInt(++counter, itemDTO.getPrevPromoDuration());
					statement.setString(++counter, Constants.CALENDAR_QUARTER);
					statement.setInt(++counter, itemDTO.getPromoFrequency());
					statement.setDouble(++counter, itemDTO.getRegPrice().price);
					statement.setInt(++counter, itemDTO.getRegPrice().multiple);
					statement.setDouble(++counter, itemDTO.getListCost());
					PristineDBUtil.setDouble(++counter, itemDTO.getPredMovReg(), statement);
					PristineDBUtil.setDouble(++counter, itemDTO.getPredRevReg(), statement);
					PristineDBUtil.setDouble(++counter, itemDTO.getPredMarReg(), statement);
					statement.setString(++counter,
							itemDTO.isShortListed() ? String.valueOf(Constants.YES) : String.valueOf(Constants.NO));
					PristineDBUtil.setString(++counter, itemDTO.isGroupLevelPromo() ? "Y" : "N", statement);
					PristineDBUtil.setInt(++counter,
							itemDTO.getAnchorProductKey() != null
									? itemDTO.getAnchorProductKey().getProductLevelId()
									: null,
							statement);
					PristineDBUtil.setInt(++counter,
							itemDTO.getAnchorProductKey() != null
									? itemDTO.getAnchorProductKey().getProductId()
									: null,
							statement);
					statement.addBatch();
				}
			}
			statement.executeBatch();
		} catch (SQLException sqlE) {
			throw new GeneralException("saveScenarios() - Error while inserting INSERT_PR_PROMO_HEADER", sqlE);
		} finally {
			PristineDBUtil.close(statement);
		}
	}
	
	
	private static final String INSERT_PR_PROMO_OPTIONS = "INSERT INTO OR_PROMO_OPTIONS (PROMO_OPTIONS_ID, "
			+ " PROMO_CANDIDATE_ITEMS_ID, PROMO_TYPE, PROMO_RETAIL, PROMO_QTY, UNIT_COST, UNITS, SALES, "
			+ " MARGIN, SHORT_LISTED, RANK, PRED_INC_UNIS, PRED_INC_SALES, PRED_INC_MARGIN, OFFER_TYPE, "
			+ " OFFER_VALUE, MIN_QTY_REQD, IS_CURRENT_PROMO) "
			+ " VALUES(OR_PROMO_OPTIONS_SEQ.NEXTVAL, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)";
	
	
	private void savePromoOptions(Connection conn,
			HashMap<Integer, HashMap<ProductKey, List<PromoItemDTO>>> itemGroups) throws GeneralException {

		PreparedStatement statement = null;
		try {
			statement = conn.prepareStatement(INSERT_PR_PROMO_OPTIONS);
			for (Map.Entry<Integer, HashMap<ProductKey, List<PromoItemDTO>>> groupEntry : itemGroups.entrySet()) {
				for (Map.Entry<ProductKey, List<PromoItemDTO>> itemEntry : groupEntry.getValue().entrySet()) {
					for (PromoItemDTO promoItemDTO : itemEntry.getValue()) {
						int counter = 0;
						statement.setLong(++counter, promoItemDTO.getCandidateItemsId());
						statement.setInt(++counter,
								promoItemDTO.getSaleInfo().getPromoTypeId());
						statement.setDouble(++counter, promoItemDTO.getSaleInfo().getSalePrice().price);
						statement.setInt(++counter, promoItemDTO.getSaleInfo().getSalePrice().multiple);
						statement.setDouble(++counter,
								promoItemDTO.getDealCost() != null && promoItemDTO.getDealCost() > 0
										? promoItemDTO.getDealCost()
										: promoItemDTO.getListCost());

						PristineDBUtil.setDouble(++counter, promoItemDTO.getPredMov(), statement);
						PristineDBUtil.setDouble(++counter, promoItemDTO.getPredRev(), statement);
						PristineDBUtil.setDouble(++counter, promoItemDTO.getPredMar(), statement);
						statement.setString(++counter, promoItemDTO.isFinalized() ? String.valueOf(Constants.YES)
								: String.valueOf(Constants.NO));

						statement.setInt(++counter, promoItemDTO.getRank());
						if (promoItemDTO.getPromoAnalysisDTO() == null) {
							PristineDBUtil.setDouble(++counter, null, statement);
							PristineDBUtil.setDouble(++counter, null, statement);
							PristineDBUtil.setDouble(++counter, null, statement);
						} else {
							PristineDBUtil.setInt(++counter,
									promoItemDTO.getPromoAnalysisDTO().getNet_incremental_units_avg(), statement);
							PristineDBUtil.setDouble(++counter,
									promoItemDTO.getPromoAnalysisDTO().getNet_incremental_sales_avg(), statement);
							PristineDBUtil.setDouble(++counter,
									promoItemDTO.getPromoAnalysisDTO().getNet_incremental_margin_avg(), statement);
						}

						if (promoItemDTO.getSaleInfo().getOfferUnitType() != null
								&& promoItemDTO.getSaleInfo().getOfferUnitType().toLowerCase().equals("none")) {
							PristineDBUtil.setString(++counter, null, statement);
						} else {
							PristineDBUtil.setString(++counter, promoItemDTO.getSaleInfo().getOfferUnitType(),
									statement);
						}

						PristineDBUtil.setDouble(++counter, promoItemDTO.getSaleInfo().getOfferValue(), statement);
						PristineDBUtil.setInt(++counter, promoItemDTO.getSaleInfo().getMinQtyReqd(), statement);
						statement.setString(++counter, promoItemDTO.isDefaultEntry() ? String.valueOf(Constants.YES)
								: String.valueOf(Constants.NO));
						
						
						statement.addBatch();
					}
				}
			}
			statement.executeBatch();
		} catch (SQLException sqlE) {
			throw new GeneralException("saveScenarios() - Error while inserting INSERT_PR_PROMO_CANDIDATE_ITEMS", sqlE);
		} finally {
			PristineDBUtil.close(statement);
		}

	}
	
	/**
	 * 
	 * @param items
	 * @return objective type id
	 */
	private int getObjectiveTypeId(HashMap<ProductKey, List<PromoItemDTO>> items) {
		int objectiveTypeId = 0;
		for(Map.Entry<ProductKey, List<PromoItemDTO>> itemEntry: items.entrySet()) {
			PromoItemDTO promoItemDTO = itemEntry.getValue().get(0);
			objectiveTypeId = promoItemDTO.getObjectiveTypeId();
		}
		
		return objectiveTypeId;
	}
	
	
	/**
	 *  
	 * @param items
	 * @param scenarioId
	 */
	private void setScenarioId(HashMap<ProductKey, List<PromoItemDTO>> items, long scenarioId) {
		items.forEach((productKey, promoList) -> {
			promoList.forEach(p -> {
				p.setScenarioId(scenarioId);
			});
		});
	}
	
	/**
	 *  
	 * @param items
	 * @param scenarioId
	 */
	private void setCandidateItemsId(List<PromoItemDTO> items, long candidateItemsId) {
		items.forEach(p -> {
			p.setCandidateItemsId(candidateItemsId);
		});
	}
	
	
	/**
	 * 
	 * @param conn
	 * @param sequenceName
	 * @return sequence value
	 * @throws GeneralException
	 */
	public long getSequenceValue(Connection conn, String sequenceName) throws GeneralException {
		long runId = 0;

		String seqQuery = "SELECT " + sequenceName + ".NEXTVAL AS SEQ_VAL FROM DUAL";
		PreparedStatement statement = null;
		ResultSet resultSet = null;
		try {
			statement = conn.prepareStatement(seqQuery);
			resultSet = statement.executeQuery();
			if (resultSet.next()) {
				runId = resultSet.getLong("SEQ_VAL");
			}
		} catch (SQLException sqlE) {
			throw new GeneralException("getSequenceValue() - Error while getting sequence" + sequenceName, sqlE);
		} finally {
			PristineDBUtil.close(resultSet);
			PristineDBUtil.close(statement);
		}

		return runId;
	}
	
	private HashMap<Integer, HashMap<ProductKey, List<PromoItemDTO>>> getGroupMap(HashMap<ProductKey, List<PromoItemDTO>> candidateMap){
		HashMap<Integer, HashMap<ProductKey, List<PromoItemDTO>>> groupMap = new HashMap<>();
		
		int counter = 0;
		for(Map.Entry<ProductKey, List<PromoItemDTO>> entry: candidateMap.entrySet()) {
			HashMap<ProductKey, List<PromoItemDTO>> itemMap = new HashMap<>();
			itemMap.put(entry.getKey(), entry.getValue());
			counter++;
			groupMap.put(counter, itemMap);
		}
		return groupMap;
	}
}
