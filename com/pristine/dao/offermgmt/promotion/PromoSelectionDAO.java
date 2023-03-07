package com.pristine.dao.offermgmt.promotion;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;

import org.apache.log4j.Logger;

import com.pristine.dto.offermgmt.ProductKey;
import com.pristine.dto.offermgmt.mwr.RecommendationInputDTO;
import com.pristine.dto.offermgmt.promotion.PromoAnalysisDTO;
import com.pristine.dto.offermgmt.promotion.PromoItemDTO;
import com.pristine.exception.GeneralException;
import com.pristine.util.Constants;
import com.pristine.util.PristineDBUtil;
import com.pristine.util.PropertyManager;

public class PromoSelectionDAO {

	private static Logger logger = Logger.getLogger("PromoSelectionDAO");

	private static final String GET_PROMO_OPTIONS = " SELECT PROMO_ANALYSIS_ID, LOCATION_LEVEL_ID, LOCATION_ID, "
			+ " PRODUCT_LEVEL_ID, PRODUCT_ID, PROMO_DEFINITION_ID, PROMO_GROUP_ID, PROMO_TYPE_ID, OFFER_TYPE, "
			+ " SALE_PRICE, SALE_QTY, TOTAL_UNITS_AVG, TOTAL_REVENUE_AVG, TOTAL_MARGIN_AVG, TOTAL_HH_AVG, "
			+ " UNIT_HH_AVG, REVENUE_HH_AVG, MARGIN_HH_AVG, UNIT_HH_ANALYZED_AVG, REVENUE_HH_ANALYZED_AVG, "
			+ " MARGIN_HH_ANALYZED_AVG, HH_ANALYZED_AVG, GROSS_INCREMENTAL_UNITS_AVG, GIU_A_AVG, GIU_B_AVG, GIU_C_AVG, "
			+ " GIU_D_AVG, GIU_E_AVG, GIU_F_AVG, GROSS_INCREMENTAL_SALES_AVG, GROSS_INCREMENTAL_MARGIN_AVG, "
			+ " NET_INCREMENTAL_UNITS_AVG, NET_INCREMENTAL_SALES_AVG, NET_INCREMENTAL_MARGIN_AVG, BASE_UNITS_PER_HH_AVG, "
			+ " UNIT_PER_HH_AVG, HH_AVAILING_PROMO_AVG, HH_AVAILING_SAME_LIR_AVG, BASE_UNITS_PER_HH_TYPICAL, "
			+ " UNIT_PER_HH_TYPICAL, START_CALENDAR_ID, END_CALENDAR_ID, UPDATED, IS_ACTIVE, IS_GROUP_PROMO, "
			+ " OFFER_NAME, OFFER_VALUE, NO_OF_WEEKS " 
			+ " FROM %TABLENAME% "
			+ " WHERE LOCATION_LEVEL_ID = ? AND LOCATION_ID = ? AND PRODUCT_LEVEL_ID = ? AND PRODUCT_ID IN (%s)";

	// yet to write
	private static final String GET_PROMO_GROUP_OPTIONS = "";

	public List<PromoAnalysisDTO> getPromoOptionsFromPIDB(Connection conn, List<Integer> nonLigItemCodes,
			List<Integer> ligItems, HashMap<ProductKey, List<PromoItemDTO>> candidateItemMap,
			RecommendationInputDTO recommendationInputDTO) throws GeneralException {

		List<PromoAnalysisDTO> consolidatedPromoHistory = new ArrayList<>();

		// Get promo options for Non Lig
		List<PromoAnalysisDTO> promoOptionsNonLig = getPromoOptions(conn, nonLigItemCodes, Constants.ITEMLEVELID,
				"OR_PROMO_ANALYSIS_SUMMARY", recommendationInputDTO);

		// Get promo options for Lig
		List<PromoAnalysisDTO> promoOptionsLig = getPromoOptions(conn, ligItems, Constants.PRODUCT_LEVEL_ID_LIG,
				"OR_PROMO_ANALYSIS_SUMMARY", recommendationInputDTO);

		/*
		 * // Get promo options for Non Lig List<PromoAnalysisDTO> promoGroupOptionsNonLig = getPromoOptions(conn,
		 * nonLigItemCodes, Constants.ITEMLEVELID, "OR_PROMO_GRP_ANALYSIS_SUMMARY", true);
		 * 
		 * // Get promo options for Lig List<PromoAnalysisDTO> promoGroupOptionsLig = getPromoOptions(conn, ligItems,
		 * Constants.PRODUCT_LEVEL_ID_LIG, "OR_PROMO_GRP_ANALYSIS_SUMMARY", true);
		 */

		mergeData(promoOptionsNonLig, promoOptionsLig, consolidatedPromoHistory);

		return consolidatedPromoHistory;
	}

	
	/**
	 * 
	 * @param conn
	 * @param itemCodes
	 * @param productLevelIdItem
	 * @param tableName
	 * @return promo options
	 * @throws GeneralException
	 */
	private List<PromoAnalysisDTO> getPromoOptions(Connection conn, List<Integer> itemCodes, int productLevelIdItem,
			String tableName, RecommendationInputDTO recommendationInputDTO) throws GeneralException {
		int counter = 0;
		List<PromoAnalysisDTO> promoOptionsList = new ArrayList<>();
		List<Integer> tempCollection = new ArrayList<Integer>();

		for (int productId : itemCodes) {
			counter++;
			tempCollection.add(productId);
			if (counter % Constants.LIMIT_COUNT == 0) {
				Object[] array = tempCollection.toArray();
				retrievePromoOptions(conn, promoOptionsList, productLevelIdItem, tableName, array,
						recommendationInputDTO);
				tempCollection.clear();
				counter = 0;
			}
		}

		if (tempCollection.size() > 0) {
			Object[] array = tempCollection.toArray();
			retrievePromoOptions(conn, promoOptionsList, productLevelIdItem, tableName, array, recommendationInputDTO);
		}

		return promoOptionsList;
	}
	
	
	/**
	 * 
	 * @param conn
	 * @param promoOptionsList
	 * @param productLevelId
	 * @param tableName
	 * @param values
	 * @throws GeneralException
	 */
	private void retrievePromoOptions(Connection conn, List<PromoAnalysisDTO> promoOptionsList,
			int productLevelId, String tableName, Object[] values, RecommendationInputDTO recommendationInputDTO)
			throws GeneralException {

		PreparedStatement statement = null;
		ResultSet resultSet = null;
		try {
			
			int promoAnalysisLocationLevelId = Integer.parseInt(PropertyManager.getProperty("PROMO_ANALYSIS_LOCATION_LEVEL_ID", "0"));
			int promoAnalysisLocationId = Integer.parseInt(PropertyManager.getProperty("PROMO_ANALYSIS_LOCATION_ID", "0"));
			
			String sql = "";
			if (tableName.equals("OR_PROMO_ANALYSIS_SUMMARY")) {
				sql = GET_PROMO_OPTIONS;
			} else {
				sql = GET_PROMO_GROUP_OPTIONS;
			}
			sql = sql.replaceAll("%TABLENAME%", tableName);
			sql = String.format(sql, PristineDBUtil.preparePlaceHolders(values.length));

			logger.debug("GET_PROMO_OPTIONS :" + sql);
			statement = conn.prepareStatement(sql);
			int counter = 0;
			if(promoAnalysisLocationLevelId > 0) {
				statement.setInt(++counter, promoAnalysisLocationLevelId);
				statement.setInt(++counter, promoAnalysisLocationId);
			}else {
				statement.setInt(++counter, recommendationInputDTO.getLocationLevelId());
				statement.setInt(++counter, recommendationInputDTO.getLocationId());	
			}
			
			statement.setInt(++counter, productLevelId);
			PristineDBUtil.setValues(statement, ++counter, values);

			// statement.setFetchSize(200000);
			resultSet = statement.executeQuery();

			while (resultSet.next()) {

				/*
				 * 
				 * PROMO_ANALYSIS_ID, LOCATION_LEVEL_ID, LOCATION_ID, PRODUCT_LEVEL_ID, PRODUCT_ID, PROMO_DEFINITION_ID,
				 * PROMO_GROUP_ID, PROMO_TYPE_ID, OFFER_TYPE, SALE_PRICE, SALE_QTY, TOTAL_UNITS_AVG, TOTAL_REVENUE_AVG,
				 * TOTAL_MARGIN_AVG, TOTAL_HH_AVG, UNIT_HH_AVG, REVENUE_HH_AVG, MARGIN_HH_AVG, UNIT_HH_ANALYZED_AVG,
				 * REVENUE_HH_ANALYZED_AVG, MARGIN_HH_ANALYZED_AVG, HH_ANALYZED_AVG, GROSS_INCREMENTAL_UNITS_AVG, GIU_A_AVG,
				 * GIU_B_AVG, GIU_C_AVG, GIU_D_AVG, GIU_E_AVG, GIU_F_AVG, GROSS_INCREMENTAL_SALES_AVG,
				 * GROSS_INCREMENTAL_MARGIN_AVG, NET_INCREMENTAL_UNITS_AVG, NET_INCREMENTAL_SALES_AVG, NET_INCREMENTAL_MARGIN_AVG,
				 * BASE_UNITS_PER_HH_AVG, UNIT_PER_HH_AVG, HH_AVALING_PROMO_AVG, HH_AVAILING_SAME_LIR_AVG,
				 * BASE_UNITS_PER_HH_TYPICAL, UNIT_PER_HH_TYPICAL, START_CALENDAR_ID, END_CALENDAR_ID, UPDATED, IS_ACTIVE,
				 * IS_GROUP_PROMO, OFFER_NAME, OFFER_VALUE, NO_OF_WEEKS
				 * 
				 */
				PromoAnalysisDTO promoAnalysis = new PromoAnalysisDTO();
				promoAnalysis.setPromo_analysis_id(resultSet.getInt("PROMO_ANALYSIS_ID"));
				promoAnalysis.setLocation_level_id(resultSet.getInt("LOCATION_LEVEL_ID"));
				promoAnalysis.setLocation_id(resultSet.getInt("LOCATION_ID"));
				promoAnalysis.setProduct_level_id(resultSet.getInt("PRODUCT_LEVEL_ID"));
				promoAnalysis.setProduct_id(resultSet.getInt("PRODUCT_ID"));
				promoAnalysis.setPromo_definition_id(resultSet.getInt("PROMO_DEFINITION_ID"));
				promoAnalysis.setPromo_group_id(resultSet.getString("PROMO_GROUP_ID"));
				promoAnalysis.setPromo_type_id(resultSet.getInt("PROMO_TYPE_ID"));
				promoAnalysis.setOffer_type(resultSet.getString("OFFER_TYPE"));
				promoAnalysis.setSale_price(resultSet.getDouble("SALE_PRICE"));
				promoAnalysis.setSale_qty(resultSet.getInt("SALE_QTY"));
				promoAnalysis.setTotal_units_avg(resultSet.getInt("TOTAL_UNITS_AVG"));
				promoAnalysis.setTotal_revenue_avg(resultSet.getDouble("TOTAL_REVENUE_AVG"));
				promoAnalysis.setTotal_margin_avg(resultSet.getDouble("TOTAL_MARGIN_AVG"));
				promoAnalysis.setTotal_households_avg(resultSet.getInt("TOTAL_HH_AVG"));
				promoAnalysis.setUnit_ch_avg(resultSet.getInt("UNIT_HH_AVG"));
				promoAnalysis.setRevenue_ch_analyzed_avg(resultSet.getDouble("REVENUE_HH_AVG"));
				promoAnalysis.setMargin_ch_avg(resultSet.getDouble("MARGIN_HH_AVG"));
				promoAnalysis.setUnit_ch_analyzed_avg(resultSet.getInt("UNIT_HH_ANALYZED_AVG"));
				promoAnalysis.setRevenue_ch_analyzed_avg(resultSet.getInt("REVENUE_HH_ANALYZED_AVG"));
				promoAnalysis.setMargin_ch_analyzed_avg(resultSet.getInt("MARGIN_HH_ANALYZED_AVG"));
				promoAnalysis.setHouseholds_analyzed_avg(resultSet.getInt("HH_ANALYZED_AVG"));
				promoAnalysis.setGross_incremental_units_avg(resultSet.getInt("GROSS_INCREMENTAL_UNITS_AVG"));
				promoAnalysis.setGiu_a_avg(resultSet.getInt("GIU_A_AVG"));
				promoAnalysis.setGiu_b_avg(resultSet.getInt("GIU_B_AVG"));
				promoAnalysis.setGiu_c_avg(resultSet.getInt("GIU_C_AVG"));
				promoAnalysis.setGiu_d_avg(resultSet.getInt("GIU_D_AVG"));
				promoAnalysis.setGiu_e_avg(resultSet.getInt("GIU_E_AVG"));
				promoAnalysis.setGiu_f_avg(resultSet.getInt("GIU_F_AVG"));
				promoAnalysis.setGross_incremental_sales_avg(resultSet.getDouble("GROSS_INCREMENTAL_SALES_AVG"));
				promoAnalysis.setGross_incremental_margin_avg(resultSet.getDouble("GROSS_INCREMENTAL_MARGIN_AVG"));
				promoAnalysis.setNet_incremental_units_avg(resultSet.getInt("NET_INCREMENTAL_UNITS_AVG"));
				promoAnalysis.setNet_incremental_sales_avg(resultSet.getDouble("NET_INCREMENTAL_SALES_AVG"));
				promoAnalysis.setNet_incremental_margin_avg(resultSet.getDouble("NET_INCREMENTAL_MARGIN_AVG"));
				promoAnalysis.setBase_units_per_hh_avg(resultSet.getInt("BASE_UNITS_PER_HH_AVG"));
				promoAnalysis.setUnits_per_hh_avg(resultSet.getInt("UNIT_PER_HH_AVG"));
				promoAnalysis.setHh_avaling_promo_avg(resultSet.getInt("HH_AVAILING_PROMO_AVG"));
				promoAnalysis.setHh_availing_same_lir_avg(resultSet.getInt("HH_AVAILING_SAME_LIR_AVG"));
				promoAnalysis.setBase_units_per_hh_avg(resultSet.getInt("BASE_UNITS_PER_HH_TYPICAL"));
				promoAnalysis.setUnit_per_hh_typical(resultSet.getInt("UNIT_PER_HH_TYPICAL"));
				promoAnalysis.setUpdated(resultSet.getString("UPDATED"));
				promoAnalysis.setIs_active(resultSet.getString("IS_ACTIVE"));
				promoAnalysis.setOffer_name(resultSet.getString("OFFER_NAME"));
				promoAnalysis.setOffer_value(resultSet.getDouble("OFFER_VALUE"));
				promoAnalysis.setNo_of_weeks(resultSet.getInt("NO_OF_WEEKS"));
				promoAnalysis
						.setGroupPromotion(resultSet.getString("IS_GROUP_PROMO").equals(String.valueOf(Constants.YES)));
				promoOptionsList.add(promoAnalysis);
			}
		} catch (Exception e) {
			logger.error("Error while executing GET_PROMO_OPTIONS ", e);
			throw new GeneralException("Exception in retrievePromoOptions()", e);
		} finally {
			PristineDBUtil.close(resultSet);
			PristineDBUtil.close(statement);
		}
	}

	
	/**
	 * TODO
	 * @param promoOptionsNonLig
	 * @param promoOptionsLig
	 * @param promoGroupOptionsNonLig
	 * @param promoGroupOptionsLig
	 * @param consolidatedList
	 */
	private void mergeData(List<PromoAnalysisDTO> promoOptionsNonLig,
			List<PromoAnalysisDTO> promoOptionsLig, List<PromoAnalysisDTO> consolidatedList) {
		
		
		consolidatedList.addAll(promoOptionsNonLig);
		consolidatedList.addAll(promoOptionsLig);

	}
	
}
