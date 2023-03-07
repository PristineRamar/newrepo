package com.pristine.dao.offermgmt.prediction;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;

import org.apache.log4j.Logger;

import com.pristine.dao.IDAO;
import com.pristine.dto.offermgmt.MultiplePrice;
import com.pristine.dto.offermgmt.PRItemDTO;
import com.pristine.dto.offermgmt.oos.OOSItemDTO;
import com.pristine.exception.GeneralException;
import com.pristine.service.ProductService;
import com.pristine.util.Constants;
import com.pristine.util.PristineDBUtil;
import com.pristine.util.offermgmt.PRCommonUtil;
import com.pristine.util.offermgmt.PRConstants;

public class PredictionReportDAO implements IDAO {
	private static Logger logger = Logger.getLogger("PredictionReportDAO");
	
	private static final String GET_ITEM_FROM_RECOMMENDATION = " SELECT PR.ITEM_CODE, IL.UPC, PR.RECOMMENDED_REG_MULTIPLE, PR.RECOMMENDED_REG_PRICE "
			+ " FROM PR_RECOMMENDATION PR LEFT JOIN ITEM_LOOKUP IL ON PR.ITEM_CODE = IL.ITEM_CODE "
			+ " WHERE RUN_ID IN (SELECT RUN_ID FROM (SELECT RUN_ID, RANK() OVER (ORDER BY END_RUN_TIME DESC) AS LATEST_RUN "
			+ " FROM PR_RECOMMENDATION_RUN_HEADER WHERE LOCATION_LEVEL_ID = ? AND LOCATION_ID = ? "
			+ " AND PRODUCT_LEVEL_ID = ? AND PRODUCT_ID = ? AND CALENDAR_ID = ?"
			+ " AND RUN_STATUS = '" + PRConstants.RUN_STATUS_SUCCESS  + "'"
			+ " AND RUN_TYPE <> '" + PRConstants.RUN_TYPE_TEMP + "')"
			+ " WHERE LATEST_RUN = 1) AND PR.LIR_IND = 'N' ";
	
//	private static final String GET_PREDICTION_REPORT_ITEMS_ZONE_FORECAST = " SELECT PRODUCT_HIERARCHY.*, STORE_ITEM_MAP.*, PG.NAME AS CATEGORY_NAME, "
//			+ " PG1.NAME AS DEPARTMENT_NAME FROM "
//			+ " (SELECT OOS_ITEM.*, "
//			+ " IL.UPC, IL.RET_LIR_ID, "
//			+ " IL.RETAILER_ITEM_CODE, RLIG.RET_LIR_NAME, IL.ITEM_NAME FROM "
//			+ " (SELECT OOS_F.LOCATION_LEVEL_ID, OOS_F.LOCATION_ID, "
//			+ " OOS_F.CALENDAR_ID, OOS_F.ITEM_CODE, OOS_F.PRESTO_FORECAST, "
//			+ " OOS_F.CLIENT_FORECAST_ACTUAL, OOS_F.REG_PRICE, OOS_F.REG_QUANTITY, OOS_F.SALE_QUANTITY, OOS_F.SALE_PRICE, "
//			+ " OOS_F.AD_PAGE_NO, OOS_F.AD_BLOCK_NO, OOS_F.DISPLAY_TYPE_ID, OOS_F.PROMO_TYPE_ID, OOS_F.PRESTO_CONFIDENCE_LEVEL_LOWER, "
//			+ " OOS_F.PRESTO_CONFIDENCE_LEVEL_UPPER, OOS_F.DIST_FLAG FROM OOS_CANDIDATE_ITEM OOS_F ";
	
	private static final String GET_PREDICTION_REPORT_ITEMS_ZONE_FORECAST = " SELECT PRODUCT_HIERARCHY.*, STORE_ITEM_MAP.*, PG.NAME AS CATEGORY_NAME, "
			+ " PG1.NAME AS DEPARTMENT_NAME FROM "
			+ " (SELECT REC_ITEM.*, "
			+ " IL.UPC, IL.RET_LIR_ID, "
			+ " IL.RETAILER_ITEM_CODE, RLIG.RET_LIR_NAME, IL.ITEM_NAME, NVL(IMS2.TOT_MOVEMENT,0) AS TOT_MOVEMENT FROM ";
	
	public List<PRItemDTO> getCandidateItemsForZoneForecastFromRec(Connection conn, int locationLevelId, int locationId, int productLevelId,
			int productId, int weekCalendarId) throws GeneralException {
		List<PRItemDTO> prItems = new ArrayList<>();
		PreparedStatement stmt = null;
		ResultSet rs = null;
		try {
			String query = new String(GET_ITEM_FROM_RECOMMENDATION);
			stmt = conn.prepareStatement(query);
			stmt.setInt(1, locationLevelId);
			stmt.setInt(2, locationId);
			stmt.setInt(3, productLevelId);
			stmt.setInt(4, productId);
			stmt.setInt(5, weekCalendarId);
			logger.debug("GET_ITEM_FROM_RECOMMENDATION:" + GET_ITEM_FROM_RECOMMENDATION);
			rs = stmt.executeQuery();
			while (rs.next()) {
				PRItemDTO prItemDTO = new PRItemDTO();
				prItemDTO.setCategoryProductId(productId);
				prItemDTO.setItemCode(rs.getInt("ITEM_CODE"));
				prItemDTO.setUpc(rs.getString("UPC"));
				prItemDTO.setRegMPack(rs.getInt("RECOMMENDED_REG_MULTIPLE"));
				if(prItemDTO.getRegMPack() == 0) {
					prItemDTO.setRegMPrice(0d);
					prItemDTO.setRegPrice(0d);
				} else if(prItemDTO.getRegMPack() == 1) {
					prItemDTO.setRegPrice(rs.getDouble("RECOMMENDED_REG_PRICE"));
				} else {
					prItemDTO.setRegMPrice(rs.getDouble("RECOMMENDED_REG_PRICE"));
				}
				
				prItemDTO.setSaleMPack(0);
				prItemDTO.setSaleMPrice(0d);
				prItemDTO.setSalePrice(0d);
				prItemDTO.setPageNumber(0);
				prItemDTO.setBlockNumber(0);
				prItemDTO.setPromoTypeId(0);
				prItemDTO.setDisplayTypeId(0);
				prItems.add(prItemDTO);
			}
		} catch (SQLException sqlE) {
			logger.error("Error -- getCandidateItemsForZoneForecastFromRec() - " + sqlE.toString());
			throw new GeneralException("Error -- getCandidateItemsForZoneForecastFromRec()", sqlE);
		} finally {
			PristineDBUtil.close(rs);
			PristineDBUtil.close(stmt);
		}
		return prItems;
	}
	
	public List<OOSItemDTO> getPredictionReportItemForZoneForecast(Connection conn, int locationLevelId, int locationId, int productLevelId,
			int productId, int weekCalendarId) throws GeneralException {
		List<OOSItemDTO> oosCandidateItems = new ArrayList<>();
		PreparedStatement statement = null;
		ResultSet resultSet = null;
		com.pristine.dao.offermgmt.ItemDAO itemDAO = new com.pristine.dao.offermgmt.ItemDAO();
		StringBuilder sb = new StringBuilder();
		ProductService productService = new ProductService();
		try {
			HashMap<Integer, Integer> productLevelPointer = productService.getProductLevelPointer(conn, Constants.DEPARTMENTLEVELID);

			sb.append(GET_PREDICTION_REPORT_ITEMS_ZONE_FORECAST);
//			sb.append(" WHERE ");
//			sb.append(" OOS_F.LOCATION_LEVEL_ID = " + locationLevelId);
//			sb.append(" AND OOS_F.LOCATION_ID IN (" + locationId + ") ");
//			sb.append(" AND OOS_F.CALENDAR_ID = " + weekCalendarId);
//			sb.append(" AND OOS_F.IS_OOS_ANALYSIS = '" + Constants.YES + "' ");
//			sb.append(" AND ITEM_CODE IN (SELECT CHILD_PRODUCT_ID FROM (SELECT CHILD_PRODUCT_ID,");
//			sb.append(" CHILD_PRODUCT_LEVEL_ID FROM PRODUCT_GROUP_RELATION PGR ");
//			sb.append(" START WITH PRODUCT_LEVEL_ID = ").append(productLevelId);
//			sb.append(" AND PRODUCT_ID = ").append(productId);
//			sb.append(" CONNECT BY  PRIOR CHILD_PRODUCT_ID = PRODUCT_ID  AND  PRIOR CHILD_PRODUCT_LEVEL_ID = PRODUCT_LEVEL_ID) ");
//			sb.append(" where CHILD_PRODUCT_LEVEL_ID = 1) ");
//			sb.append("  ) OOS_ITEM LEFT JOIN ITEM_LOOKUP IL ON IL.ITEM_CODE = OOS_ITEM.ITEM_CODE ");

			sb.append(" (SELECT ").append(locationLevelId).append(" AS LOCATION_LEVEL_ID, ");
			sb.append(locationId).append(" AS LOCATION_ID, ").append(weekCalendarId).append(" AS CALENDAR_ID, ");
			sb.append(" PR.ITEM_CODE, PREDICTED_MOVEMENT AS PRESTO_FORECAST, 0 AS CLIENT_FORECAST_ACTUAL, ");
			sb.append(" RECOMMENDED_REG_MULTIPLE AS REG_QUANTITY, RECOMMENDED_REG_PRICE AS REG_PRICE, ");
			sb.append(" 0 AS SALE_QUANTITY, 0 AS SALE_PRICE, ");
			sb.append(" 0 as AD_PAGE_NO, 0 AS AD_BLOCK_NO, 0 AS DISPLAY_TYPE_ID, 0 AS PROMO_TYPE_ID, ");
			sb.append(" 0 AS PRESTO_CONFIDENCE_LEVEL_LOWER, 0 AS PRESTO_CONFIDENCE_LEVEL_UPPER, '' AS DIST_FLAG ");		
			sb.append(" FROM PR_RECOMMENDATION PR LEFT JOIN ITEM_LOOKUP IL ON PR.ITEM_CODE = IL.ITEM_CODE ");
			sb.append(" WHERE RUN_ID IN (SELECT RUN_ID FROM (SELECT RUN_ID, RANK() OVER (ORDER BY END_RUN_TIME DESC) AS LATEST_RUN ");
			sb.append(" FROM PR_RECOMMENDATION_RUN_HEADER WHERE LOCATION_LEVEL_ID = ").append(locationLevelId);
			sb.append(" AND LOCATION_ID = ").append(locationId);
			sb.append(" AND PRODUCT_LEVEL_ID = ").append(productLevelId);
			sb.append(" AND PRODUCT_ID = ").append(productId).append(" AND CALENDAR_ID = ").append(weekCalendarId);
			sb.append(" AND RUN_STATUS = '").append(PRConstants.RUN_STATUS_SUCCESS).append("'"); 
			sb.append(" AND RUN_TYPE <> '").append(PRConstants.RUN_TYPE_TEMP).append("') WHERE LATEST_RUN = 1) AND PR.LIR_IND = 'N' "); 
			sb.append("  ) REC_ITEM LEFT JOIN ITEM_LOOKUP IL ON IL.ITEM_CODE = REC_ITEM.ITEM_CODE ");
			sb.append(" LEFT JOIN RETAILER_LIKE_ITEM_GROUP RLIG ON IL.RET_LIR_ID = RLIG.RET_LIR_ID ");
			
			sb.append(" LEFT JOIN (SELECT PRODUCT_ID, SUM(TOT_MOVEMENT) TOT_MOVEMENT FROM ");
			sb.append(" ITEM_METRIC_SUMMARY_WEEKLY WHERE CALENDAR_ID = " + weekCalendarId );
			sb.append(" AND LOCATION_LEVEL_ID = " + Constants.STORE_LEVEL_ID);
			sb.append(" AND LOCATION_ID IN ");
			
			sb.append(" (SELECT STORE_ID FROM PR_PRODUCT_LOCATION_STORE WHERE PRODUCT_LOCATION_MAPPING_ID IN ( ");
			sb.append(" SELECT PRODUCT_LOCATION_MAPPING_ID FROM PR_PRODUCT_LOCATION_MAPPING ");
			sb.append(" WHERE PRODUCT_LEVEL_ID = ").append(productLevelId).append(" AND PRODUCT_ID = ").append(productId);
			sb.append(" AND LOCATION_LEVEL_ID = ").append(locationLevelId).append(" AND LOCATION_ID = ").append(locationId).append(")) ");
			
			sb.append("  GROUP BY PRODUCT_ID ) IMS2 ");
			sb.append(" ON IMS2.PRODUCT_ID = REC_ITEM.ITEM_CODE ");
			
			
			sb.append(" ) STORE_ITEM_MAP LEFT JOIN ");
			sb.append(itemDAO.getProductHierarchy(conn, Constants.DEPARTMENTLEVELID, productService));
			sb.append(" LEFT JOIN PRODUCT_GROUP PG ON ");
			sb.append(" PRODUCT_HIERARCHY.P_ID_" + productLevelPointer.get(Constants.CATEGORYLEVELID) + " = PG.PRODUCT_ID AND ");
			sb.append(" PG.PRODUCT_LEVEL_ID = " + Constants.CATEGORYLEVELID);
			sb.append(" LEFT JOIN PRODUCT_GROUP PG1 ON ");
			sb.append(" PRODUCT_HIERARCHY.P_ID_" + productLevelPointer.get(Constants.DEPARTMENTLEVELID) + " = PG1.PRODUCT_ID AND ");
			sb.append(" PG1.PRODUCT_LEVEL_ID = " + Constants.DEPARTMENTLEVELID);

			logger.debug("GET_PREDICTION_REPORT_ITEMS_ZONE_FORECAST -- " + sb.toString());
			statement = conn.prepareStatement(sb.toString());
			resultSet = statement.executeQuery();
			while (resultSet.next()) {

				OOSItemDTO oosItemDTO = new OOSItemDTO();
				oosItemDTO.setProductId(resultSet.getInt("ITEM_CODE"));
				oosItemDTO.setProductLevelId(Constants.ITEMLEVELID);
				oosItemDTO.setLocationLevelId(resultSet.getInt("LOCATION_LEVEL_ID"));
				oosItemDTO.setLocationId(resultSet.getInt("LOCATION_ID"));
				MultiplePrice regPrice = PRCommonUtil.getMultiplePrice(resultSet.getInt("REG_QUANTITY"), resultSet.getDouble("REG_PRICE"),
						resultSet.getDouble("REG_PRICE"));
				oosItemDTO.setRegPrice(regPrice);

				MultiplePrice salePrice = PRCommonUtil.getMultiplePrice(resultSet.getInt("SALE_QUANTITY"), resultSet.getDouble("SALE_PRICE"),
						resultSet.getDouble("SALE_PRICE"));
				oosItemDTO.setSalePrice(salePrice);
				oosItemDTO.setCategoryName(resultSet.getString("CATEGORY_NAME"));
				oosItemDTO.setDepartmentName(resultSet.getString("DEPARTMENT_NAME"));
				oosItemDTO.setCatProductId(resultSet.getInt("P_ID_" + productLevelPointer.get(Constants.CATEGORYLEVELID)));
				oosItemDTO.setDeptProductId(resultSet.getInt("P_ID_" + productLevelPointer.get(Constants.DEPARTMENTLEVELID)));
				oosItemDTO.setUpc(resultSet.getString("UPC"));
				oosItemDTO.setRetailerItemCode(resultSet.getString("RETAILER_ITEM_CODE"));
				oosItemDTO.setItemName(resultSet.getString("ITEM_NAME"));
				oosItemDTO.setPromoTypeId(resultSet.getInt("PROMO_TYPE_ID"));

				oosItemDTO.setAdPageNoGU(resultSet.getInt("AD_PAGE_NO"));
				oosItemDTO.setBlockNoGU(resultSet.getInt("AD_BLOCK_NO"));
				oosItemDTO.setDisplayTypeId(resultSet.getInt("DISPLAY_TYPE_ID"));

				oosItemDTO.setRetLirId(resultSet.getInt("RET_LIR_ID"));
				oosItemDTO.setLirName(resultSet.getString("RET_LIR_NAME"));
				
//				if (resultSet.getString("DIST_FLAG").equals(String.valueOf(Constants.DSD)))
//					oosItemDTO.setDistFlag(Constants.DSD);
//				else
//					oosItemDTO.setDistFlag(Constants.WAREHOUSE);

				if (resultSet.getLong("PRESTO_FORECAST") > 0) {
					oosItemDTO.setWeeklyPredictedMovement(Math.round(resultSet.getDouble("PRESTO_FORECAST")));
				} else {
					oosItemDTO.setWeeklyPredictedMovement(0);
				}

				oosItemDTO.setWeeklyPredictedMovementGU(oosItemDTO.getWeeklyPredictedMovement());
				if (resultSet.getLong("CLIENT_FORECAST_ACTUAL") > 0) {
					oosItemDTO.setClientWeeklyPredictedMovement(resultSet.getLong("CLIENT_FORECAST_ACTUAL"));
				} else {
					oosItemDTO.setClientWeeklyPredictedMovement(0);
				}

				if (resultSet.getLong("PRESTO_CONFIDENCE_LEVEL_LOWER") > 0)
					oosItemDTO.setWeeklyConfidenceLevelLower(Math.round(resultSet.getDouble("PRESTO_CONFIDENCE_LEVEL_LOWER")));
				else
					oosItemDTO.setWeeklyConfidenceLevelLower(0);

				if (resultSet.getLong("PRESTO_CONFIDENCE_LEVEL_UPPER") > 0)
					oosItemDTO.setWeeklyConfidenceLevelUpper(Math.round(resultSet.getDouble("PRESTO_CONFIDENCE_LEVEL_UPPER")));
				else
					oosItemDTO.setWeeklyConfidenceLevelUpper(0);

				oosItemDTO.setWeeklyActualMovement(resultSet.getLong("TOT_MOVEMENT"));

				oosCandidateItems.add(oosItemDTO);
			}
		} catch (Exception sqlE) {
			logger.error("Error -- getOOSCandidateItems() - " + sqlE.toString());
			throw new GeneralException("Error -- getOOSCandidateItems()", sqlE);
		}
		return oosCandidateItems;
	}
}
