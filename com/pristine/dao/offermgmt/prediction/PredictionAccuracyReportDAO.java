package com.pristine.dao.offermgmt.prediction;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;

import org.apache.log4j.Logger;

import com.pristine.dao.RetailCalendarDAO;
import com.pristine.dto.RetailCalendarDTO;
import com.pristine.dto.offermgmt.MultiplePrice;
import com.pristine.dto.offermgmt.oos.OOSItemDTO;
import com.pristine.dto.offermgmt.prediction.AccuracyComparisonDetailDTO;
import com.pristine.dto.offermgmt.prediction.AccuracyComparisonHeaderDTO;
import com.pristine.dto.offermgmt.prediction.PredictionAccuracyReportDTO;
import com.pristine.exception.GeneralException;
import com.pristine.service.ProductService;
import com.pristine.service.offermgmt.prediction.PredictionStatus;
import com.pristine.util.Constants;
import com.pristine.util.PristineDBUtil;
import com.pristine.util.PropertyManager;
import com.pristine.util.offermgmt.PRCommonUtil;

public class PredictionAccuracyReportDAO {
	private static Logger logger = Logger.getLogger("PredictionAccuracyReportDAO");
	
	private static final String INSERT_ACCURACY_COMPARISON_HEADER = "INSERT INTO ACCURACY_COMPARISON_HEADER"
			+ " (COMPARISON_HEADER_ID, CALENDAR_ID, LOCATION_LEVEL_ID, LOCATION_ID, "
			+ "PRODUCT_LEVEL_ID, PRODUCT_ID, REPORT_TYPE_ID, SUB_REPORT_TYPE_ID) VALUES (?, ?, ?, ?, ?, ?, ?, ?)";
	
	private static final String INSERT_ACCURACY_COMPARISON_DETAIL = "INSERT INTO ACCURACY_COMPARISON_DETAIL"
			+ " (COMPARISON_HEADER_ID, COMPARISON_PARAM_ID, PARAMETER_VALUE, PARAMETER_VALUE_TEXT, "
			+ "ORDER_NO, VERSION_TYPE) VALUES (?, ?, ?, ?, ?, ?)";
	
	private static final String GET_MAX_COMPARISON_HEADER_ID = "SELECT MAX(COMPARISON_HEADER_ID) AS COMPARISON_HEADER_ID "
			+ " FROM ACCURACY_COMPARISON_HEADER";
	
	private static final String DELETE_COMPARISON_HEADER = "DELETE FROM ACCURACY_COMPARISON_HEADER WHERE "
			+ " CALENDAR_ID = ? AND LOCATION_LEVEL_ID = ? AND LOCATION_ID = ? AND PRODUCT_LEVEL_ID = ? "
			+ " AND PRODUCT_ID = ? AND REPORT_TYPE_ID = ? AND SUB_REPORT_TYPE_ID = ? ";
	
	private static final String DELETE_COMPARISON_DETAIL = "DELETE FROM ACCURACY_COMPARISON_DETAIL WHERE "
			+ " COMPARISON_HEADER_ID IN (SELECT COMPARISON_HEADER_ID FROM ACCURACY_COMPARISON_HEADER "
			+ " WHERE CALENDAR_ID = ? AND LOCATION_LEVEL_ID = ? AND LOCATION_ID = ? AND PRODUCT_LEVEL_ID = ? "
			+ " AND PRODUCT_ID = ? AND REPORT_TYPE_ID = ? AND SUB_REPORT_TYPE_ID = ?) ";
	
	private static final String GET_ITEMS_WITH_FORECAST = " SELECT PRODUCT_HIERARCHY.*, STORE_ITEM_MAP.*, PG.NAME AS CATEGORY_NAME, "
			+ " PG1.NAME AS DEPARTMENT_NAME FROM "
			+ " (SELECT OOS_ITEM.*, "
			//+ " NVL(IMS1.TOT_MOVEMENT_GU,0) + NVL(IMS2.TOT_MOVEMENT_TOPS,0) AS TOT_MOVEMENT, "
			+ " NVL(IMS2.ACTUAL_MOVEMENT,0) AS ACTUAL_MOVEMENT, "
			+ " IL.UPC, IL.RETAILER_ITEM_CODE, RLIG.RET_LIR_NAME, "
			+ " IL.ITEM_NAME, WAB.ADJUSTED_UNITS AS CLIENT_FORECAST_ACTUAL, WAB.NO_OF_LIG_OR_NON_LIG FROM "
			+ " (SELECT OOS_F.LOCATION_LEVEL_ID, OOS_F.LOCATION_ID, "
			+ " OOS_F.CALENDAR_ID, OOS_F.ITEM_CODE, OOS_F.PRESTO_FORECAST, "
			+ " OOS_F.REG_PRICE, OOS_F.REG_QUANTITY, OOS_F.SALE_QUANTITY, OOS_F.SALE_PRICE, "
			+ " OOS_F.AD_PAGE_NO, OOS_F.AD_BLOCK_NO, OOS_F.DISPLAY_TYPE_ID, OOS_F.PROMO_TYPE_ID, OOS_F.PRESTO_CONFIDENCE_LEVEL_LOWER, "
			+ " OOS_F.PRESTO_CONFIDENCE_LEVEL_UPPER, OOS_F.DIST_FLAG, OOS_F.RET_LIR_ID, OOS_F.PREDICTION_STATUS FROM OOS_CANDIDATE_ITEM OOS_F ";
	
	public void getAccuracySummary(PredictionAccuracyReportDTO predictionAccuracyReportDTO, Connection conn, int weekCalendarId, int chainId,
			int locationLevelId, int locationId) throws Exception, GeneralException {
		PreparedStatement stmt = null, stmt1 = null;
		ResultSet rs = null, rs1 = null;
		ProductService productService = new ProductService();
		HashSet<Integer> allCategories = new HashSet<Integer>();
		HashSet<String> allItems = new HashSet<String>();
		HashSet<Integer> allRetLirIds = new HashSet<Integer>();
		HashSet<String> allNonLigs = new HashSet<String>();
		HashMap<Integer, Integer> productLevelPointer = null;
		List<String> perishablesDeptIds = Arrays.asList(PropertyManager.getProperty("PERISHABLES_DEPARTMENT_IDS").split("\\s*,\\s*"));

		HashSet<Integer> nonPerishablesCategories = new HashSet<Integer>();
		HashSet<String> nonPerishablesItems = new HashSet<String>();
		HashSet<Integer> nonPerishablesRetLirIds = new HashSet<Integer>();
		HashSet<String> nonPerishablesNonLigs = new HashSet<String>();

		HashSet<Integer> nonPerishablesAdCategoriesWPred = new HashSet<Integer>();
		HashSet<String> nonPerishablesAdItemsWPred = new HashSet<String>();
		HashSet<Integer> nonPerishablesLIGsWPred = new HashSet<Integer>();
		HashSet<String> nonPerishablesNonLIGsWPred = new HashSet<String>();

		HashSet<String> nonPerishablesAdItemsWZeroPred = new HashSet<String>();
		HashSet<Integer> nonPerishablesLIGsWZeroPred = new HashSet<Integer>();
		HashSet<String> nonPerishablesNonLIGsWZeroPred = new HashSet<String>();

		HashSet<Integer> nonPerishablesAdCategoriesWOPred = new HashSet<Integer>();
		HashSet<String> nonPerishablesAdItemsWOPred = new HashSet<String>();
		HashSet<Integer> nonPerishablesLIGsWOPred = new HashSet<Integer>();
		HashSet<String> nonPerishablesNonLIGsWOPred = new HashSet<String>();

		HashSet<Integer> nonPerishablesW1LIGor1NonLIGCategories = new HashSet<Integer>();
		HashSet<String> nonPerishablesW1LIGor1NonLIGItems = new HashSet<String>();
		HashSet<Integer> nonPerishablesW1LIGor1NonLIGRetLirIds = new HashSet<Integer>();
		HashSet<String> nonPerishablesW1LIGor1NonLIGNonLigs = new HashSet<String>();

		HashSet<Integer> nonPerishablesW1LIGor1NonLIGCategoriesWPrediction = new HashSet<Integer>();
		HashSet<Integer> nonPerishablesW1LIGor1NonLIGCategoriesWOPrediction = new HashSet<Integer>();
		HashMap<String, Integer> nonPerishablesCategoriesWOPrediction = new HashMap<String, Integer>();

		productService.getProductLevelRelationMap(conn, Constants.CATEGORYLEVELID);
		productLevelPointer = productService.getProductLevelPointer(conn, Constants.CATEGORYLEVELID);

		try {
			HashSet<String> authorizedItemCodes = new HashSet<String>();
			
			List<OOSItemDTO> oosCandidateItems = new ArrayList<OOSItemDTO>();
			String query = getOOSCandidateItem(conn, productService, productLevelPointer, weekCalendarId, locationLevelId, locationId);
			logger.debug("getOOSCandidateItem:" + query);
			stmt1 = conn.prepareStatement(query);
			stmt1.setFetchSize(200000);
			rs1 = stmt1.executeQuery();
			while (rs1.next()) {
				OOSItemDTO oosCandidateItem = new OOSItemDTO();
				oosCandidateItem.setRetLirId(rs1.getInt("RET_LIR_ID"));
				oosCandidateItem.setProductId(rs1.getInt("ITEM_CODE"));
				oosCandidateItem.setRetailerItemCode(rs1.getString("RETAILER_ITEM_CODE"));
				oosCandidateItem.setCatProductId(rs1.getInt("P_ID_" + productLevelPointer.get(Constants.CATEGORYLEVELID)));
				oosCandidateItem.setCategoryName(rs1.getString("NAME"));
				oosCandidateItem.setWeeklyPredictionStatus(rs1.getInt("PREDICTION_STATUS"));
				oosCandidateItems.add(oosCandidateItem);
				authorizedItemCodes.add(rs1.getString("RETAILER_ITEM_CODE"));
			}

			List<OOSItemDTO> adItems = new ArrayList<OOSItemDTO>();
			query = getAllAdItemsQuery(conn, productService, weekCalendarId, chainId, locationLevelId, locationId);
			logger.debug("getAllAdItemsQuery:" + query);
			stmt = conn.prepareStatement(query);
			stmt.setFetchSize(200000);
			rs = stmt.executeQuery();
			while (rs.next()) {
				OOSItemDTO oosItemDTO = new OOSItemDTO();
				oosItemDTO.setCatProductId(rs.getInt("P_ID_" + productLevelPointer.get(Constants.CATEGORYLEVELID)));
				oosItemDTO.setDeptProductId(rs.getInt("P_ID_" + productLevelPointer.get(Constants.DEPARTMENTLEVELID)));
				oosItemDTO.setRetLirId(rs.getInt("RET_LIR_ID"));
				oosItemDTO.setRetailerItemCode(rs.getString("RETAILER_ITEM_CODE"));
				oosItemDTO.setProductId(rs.getInt("ITEM_CODE"));
				oosItemDTO.setNoOfLigOrNonLig(rs.getInt("NO_OF_LIG_OR_NON_LIG"));
				adItems.add(oosItemDTO);
			}
			
			for (OOSItemDTO adItem : adItems) {
				allCategories.add(adItem.getCatProductId());
				allItems.add(adItem.getRetailerItemCode());

				if (adItem.getRetLirId() > 0)
					allRetLirIds.add(adItem.getRetLirId());
				else {
					allNonLigs.add(adItem.getRetailerItemCode());
				}

				// if present in oos candidate items
				if (authorizedItemCodes.contains(adItem.getRetailerItemCode())) {
					// non perishables
					if (!perishablesDeptIds.contains(String.valueOf(adItem.getDeptProductId()))) {
						nonPerishablesCategories.add(adItem.getCatProductId());
						nonPerishablesItems.add(adItem.getRetailerItemCode());
						if (adItem.getRetLirId() > 0)
							nonPerishablesRetLirIds.add(adItem.getRetLirId());
						else {
							nonPerishablesNonLigs.add(adItem.getRetailerItemCode());
						}

						// page & block with one item
						if (adItem.getNoOfLigOrNonLig() == 1) {
							nonPerishablesW1LIGor1NonLIGCategories.add(adItem.getCatProductId());
							nonPerishablesW1LIGor1NonLIGItems.add(adItem.getRetailerItemCode());
							if (adItem.getRetLirId() > 0)
								nonPerishablesW1LIGor1NonLIGRetLirIds.add(adItem.getRetLirId());
							else {
								nonPerishablesW1LIGor1NonLIGNonLigs.add(adItem.getRetailerItemCode());
							}
						}
					}
				}
			}
			
			
			predictionAccuracyReportDTO.totNoOfAdCategories = allCategories.size();
			predictionAccuracyReportDTO.totNoOfAdItems = allItems.size();
			predictionAccuracyReportDTO.totNoOfLIGs = allRetLirIds.size();
			predictionAccuracyReportDTO.totNoOfNonLIGs = allNonLigs.size();

			predictionAccuracyReportDTO.totNoOfNonPerishablesAdCategories = nonPerishablesCategories.size();
			predictionAccuracyReportDTO.totNoOfNonPerishablesAdItems = nonPerishablesItems.size();
			predictionAccuracyReportDTO.totNoOfNonPerishablesLIGs = nonPerishablesRetLirIds.size();
			predictionAccuracyReportDTO.totNoOfNonPerishablesNonLIGs = nonPerishablesNonLigs.size();

			predictionAccuracyReportDTO.totNoOfNonPerishablesWith1LIGor1NonLigAdCategories = nonPerishablesW1LIGor1NonLIGCategories.size();
			predictionAccuracyReportDTO.totNoOfNonPerishablesWith1LIGor1NonLigAdItems = nonPerishablesW1LIGor1NonLIGItems.size();
			predictionAccuracyReportDTO.totNoOfNonPerishablesWith1LIGor1NonLigLIGs = nonPerishablesW1LIGor1NonLIGRetLirIds.size();
			predictionAccuracyReportDTO.totNoOfNonPerishablesWith1LIGor1NonLigNonLIGs = nonPerishablesW1LIGor1NonLIGNonLigs.size();


			HashSet<Integer> tempNonPerishablesAdCategoriesWOPred = new HashSet<Integer>(); 
			// Fill no of categories with prediction
			for (OOSItemDTO oosItemDTO : oosCandidateItems) {
				if (oosItemDTO.getWeeklyPredictionStatus() == PredictionStatus.SUCCESS.getStatusCode()
						|| oosItemDTO.getWeeklyPredictionStatus() == PredictionStatus.ERROR_NO_MOV_DATA_SPECIFIC_UPC.getStatusCode()
						|| oosItemDTO.getWeeklyPredictionStatus() == PredictionStatus.ERROR_NO_PRICE_DATA_SPECIFIC_UPC.getStatusCode()
						|| oosItemDTO.getWeeklyPredictionStatus() == PredictionStatus.NO_RECENT_MOVEMENT.getStatusCode()
						|| oosItemDTO.getWeeklyPredictionStatus() == PredictionStatus.ERROR_NO_MOV_DATA_ANY_UPC.getStatusCode()
						|| oosItemDTO.getWeeklyPredictionStatus() == PredictionStatus.ERROR_NO_PRICE_DATA_ANY_UPC.getStatusCode()
						|| oosItemDTO.getWeeklyPredictionStatus() == PredictionStatus.SHIPPER_ITEM.getStatusCode()
						|| oosItemDTO.getWeeklyPredictionStatus() == PredictionStatus.NEW_ITEM.getStatusCode()) {
					nonPerishablesAdCategoriesWPred.add(oosItemDTO.getCatProductId());
				} else {
					tempNonPerishablesAdCategoriesWOPred.add(oosItemDTO.getCatProductId());
				}
			}

			// Fill no of categories with out prediction
			for (OOSItemDTO oosItemDTO : oosCandidateItems) {
				// Even if one item has valid prediction then consider that
				// category is predicted
				if (!nonPerishablesAdCategoriesWPred.contains(oosItemDTO.getCatProductId()))
					nonPerishablesAdCategoriesWOPred.add(oosItemDTO.getCatProductId());
			}
			//NU::1st Aug 2016, consider even if one of the item is not having prediction
			nonPerishablesAdCategoriesWOPred = tempNonPerishablesAdCategoriesWOPred;
			
			for (Integer catId : tempNonPerishablesAdCategoriesWOPred) {
				if (nonPerishablesAdCategoriesWPred.contains(catId)) {
					logger.warn("Same category has both valid and in-valid predictions :" + catId);
				}
			}

			// Categories with valid predictions
			for (OOSItemDTO oosItemDTO : oosCandidateItems) {
				if (nonPerishablesAdCategoriesWPred.contains(oosItemDTO.getCatProductId())) {

					if (nonPerishablesW1LIGor1NonLIGCategories.contains(oosItemDTO.getCatProductId())) {
						nonPerishablesW1LIGor1NonLIGCategoriesWPrediction.add(oosItemDTO.getCatProductId());
					}

					nonPerishablesAdItemsWPred.add(oosItemDTO.getRetailerItemCode());
					if (oosItemDTO.getRetLirId() > 0)
						nonPerishablesLIGsWPred.add(oosItemDTO.getRetLirId());
					else
						nonPerishablesNonLIGsWPred.add(oosItemDTO.getRetailerItemCode());

					if (oosItemDTO.getWeeklyPredictionStatus() != PredictionStatus.SUCCESS.getStatusCode()) {
						nonPerishablesAdItemsWZeroPred.add(oosItemDTO.getRetailerItemCode());
						if (oosItemDTO.getRetLirId() > 0)
							nonPerishablesLIGsWZeroPred.add(oosItemDTO.getRetLirId());
						else
							nonPerishablesNonLIGsWZeroPred.add(oosItemDTO.getRetailerItemCode());
					}
				} 
			}
			
			// Categories with in-valid predictions
			for (OOSItemDTO oosItemDTO : oosCandidateItems) {
				if (nonPerishablesAdCategoriesWOPred.contains(oosItemDTO.getCatProductId())) {
					nonPerishablesAdItemsWOPred.add(oosItemDTO.getRetailerItemCode());
					if (oosItemDTO.getRetLirId() > 0) {
						//item in a lig may span across categories, in this case
						//even if one item has valid prediction, consider that
						//lig has valid prediction
						if(!nonPerishablesLIGsWPred.contains(oosItemDTO.getRetLirId()))
							nonPerishablesLIGsWOPred.add(oosItemDTO.getRetLirId());
					} else {
						nonPerishablesNonLIGsWOPred.add(oosItemDTO.getRetailerItemCode());
					}

					if (nonPerishablesW1LIGor1NonLIGCategories.contains(oosItemDTO.getCatProductId())) {
						nonPerishablesW1LIGor1NonLIGCategoriesWOPrediction.add(oosItemDTO.getCatProductId());
					}
					nonPerishablesCategoriesWOPrediction.put(oosItemDTO.getCategoryName(), oosItemDTO.getWeeklyPredictionStatus());
				}
			}

			predictionAccuracyReportDTO.totNoOfNonPerishablesAdCategoriesWPred = nonPerishablesAdCategoriesWPred.size();
			predictionAccuracyReportDTO.totNoOfNonPerishablesAdItemsWPred = nonPerishablesAdItemsWPred.size();
			predictionAccuracyReportDTO.totNoOfNonPerishablesLIGsWPred = nonPerishablesLIGsWPred.size();
			predictionAccuracyReportDTO.totNoOfNonPerishablesNonLIGsWPred = nonPerishablesNonLIGsWPred.size();

			predictionAccuracyReportDTO.totNoOfNonPerishablesAdItemsWZeroPred = nonPerishablesAdItemsWZeroPred.size();
			predictionAccuracyReportDTO.totNoOfNonPerishablesLIGsWZeroPred = nonPerishablesLIGsWZeroPred.size();
			predictionAccuracyReportDTO.totNoOfNonPerishablesNonLIGsWZeroPred = nonPerishablesNonLIGsWZeroPred.size();

			predictionAccuracyReportDTO.totNoOfNonPerishablesAdCategoriesWOPred = nonPerishablesAdCategoriesWOPred.size();
			predictionAccuracyReportDTO.totNoOfNonPerishablesAdItemsWOPred = nonPerishablesAdItemsWOPred.size();
			predictionAccuracyReportDTO.totNoOfNonPerishablesLIGsWOPred = nonPerishablesLIGsWOPred.size();
			predictionAccuracyReportDTO.totNoOfNonPerishablesNonLIGsWOPred = nonPerishablesNonLIGsWOPred.size();

			predictionAccuracyReportDTO.totNoOfNonPerishablesWith1LIGor1NonLigAdCategoriesWPred = nonPerishablesW1LIGor1NonLIGCategoriesWPrediction
					.size();
			predictionAccuracyReportDTO.totNoOfNonPerishablesWith1LIGor1NonLigAdCategoriesWoPred = nonPerishablesW1LIGor1NonLIGCategoriesWOPrediction
					.size();
			predictionAccuracyReportDTO.totNoOfNonPerishablesCategoriesWOPrediction = nonPerishablesCategoriesWOPrediction;
		} catch (SQLException ex) {
			logger.error("Error in getAllAdItems() -- " + ex.toString(), ex);
			throw new Exception();
		} finally {
			PristineDBUtil.close(rs);
			PristineDBUtil.close(stmt);
			PristineDBUtil.close(rs1);
			PristineDBUtil.close(stmt1);
		}
	}
	
	private String getAllAdItemsQuery(Connection conn, ProductService productService, int weekCalendarId, 
			int chainId, int locationLevelId, int locationId) throws GeneralException {
		com.pristine.dao.offermgmt.ItemDAO itemDAO = new com.pristine.dao.offermgmt.ItemDAO();
		RetailCalendarDAO calDAO = new RetailCalendarDAO();
		StringBuilder sb = new StringBuilder();
		
		RetailCalendarDTO retailCalendarDTO = calDAO.getCalendarDetail(conn, weekCalendarId) ;
		RetailCalendarDTO startCalendarDTO = calDAO.getCalendarId(conn, retailCalendarDTO.getStartDate(), Constants.CALENDAR_DAY);
		RetailCalendarDTO endCalendarDTO = calDAO.getCalendarId(conn, retailCalendarDTO.getEndDate(), Constants.CALENDAR_DAY);
		
		sb.append("SELECT * FROM (SELECT * FROM (SELECT ");
		sb.append(" IL.UPC, IL.ITEM_CODE, IL.RETAILER_ITEM_CODE, WEEKLY_AD.PAGE_NUMBER, WEEKLY_AD.BLOCK_NUMBER, ");
		sb.append(" WEEKLY_AD.NO_OF_LIG_OR_NON_LIG, PD.RET_LIR_ID, PD.PROMO_DEFINITION_ID, PD.SRC_VENDOR_AND_ITEM_ID ");
		sb.append(" FROM PM_PROMO_DEFINITION PD JOIN PM_PROMO_BUY_ITEM PBI ON PBI.PROMO_DEFINITION_ID = PD.PROMO_DEFINITION_ID ");
		sb.append(" AND PBI.IS_IN_AD_PLEX  = 'Y' ");
		sb.append(" JOIN ");
		sb.append("(SELECT WAD.AD_NAME, WAD.LOCATION_LEVEL_ID, WAD.LOCATION_ID, WAD.CALENDAR_ID, WAD.TOTAL_PAGES,");
		sb.append(" WAP.PAGE_NUMBER, WAB.BLOCK_NUMBER, WAB.NO_OF_LIG_OR_NON_LIG, WAPR.PROMO_DEFINITION_ID, WAPR.TOTAL_ITEMS");
		sb.append(" FROM PM_WEEKLY_AD_DEFINITION WAD");
		sb.append(" JOIN PM_WEEKLY_AD_PAGE WAP ON WAD.WEEKLY_AD_ID = WAP.WEEKLY_AD_ID");
		sb.append(" JOIN PM_WEEKLY_AD_BLOCK WAB ON WAB.PAGE_ID = WAP.PAGE_ID");
		sb.append(" JOIN PM_WEEKLY_AD_PROMO WAPR ON WAPR.BLOCK_ID  = WAB.BLOCK_ID");
		sb.append(" WHERE WAD.CALENDAR_ID = " + weekCalendarId );
		sb.append(" AND ");
		sb.append(PRCommonUtil.getWeeklyAdLocationQuery(chainId, locationLevelId, locationId));
		sb.append(") WEEKLY_AD ON WEEKLY_AD.PROMO_DEFINITION_ID = PD.PROMO_DEFINITION_ID ");
		sb.append(" JOIN ITEM_LOOKUP IL ON IL.ITEM_CODE = PBI.ITEM_CODE ) PD");
		sb.append(" WHERE PD.PROMO_DEFINITION_ID IN ( ");
		sb.append(" SELECT DISTINCT(PROMO_DEFINITION_ID) FROM PM_PROMO_LOCATION PL WHERE ");
		sb.append(" PL.PROMO_DEFINITION_ID IN (SELECT PROMO_DEFINITION_ID ");
		sb.append(" FROM PM_PROMO_DEFINITION WHERE START_CALENDAR_ID = ").append(startCalendarDTO.getCalendarId());
		sb.append(" AND END_CALENDAR_ID= ").append(endCalendarDTO.getCalendarId()).append(" ) AND ");
		sb.append(PRCommonUtil.getPromotionLocationQuery(chainId, locationLevelId, locationId));
		sb.append(" )) STORE_ITEM_MAP ");
		sb.append(" LEFT JOIN ");
		sb.append(itemDAO.getProductHierarchy(conn, Constants.CATEGORYLEVELID, productService));
		return sb.toString();
	}
	
	private String getOOSCandidateItem(Connection conn, ProductService productService, HashMap<Integer, Integer> productLevelPointer,
			int weekCalendarId, int locationLevelId, int locationId) throws GeneralException {
		StringBuilder sb = new StringBuilder();
		com.pristine.dao.offermgmt.ItemDAO itemDAO = new com.pristine.dao.offermgmt.ItemDAO();
		sb.append("SELECT STORE_ITEM_MAP.*, PRODUCT_HIERARCHY.*, PG.*, IL.RETAILER_ITEM_CODE");
		sb.append(" FROM (SELECT ITEM_CODE, RET_LIR_ID, PREDICTION_STATUS FROM OOS_CANDIDATE_ITEM ");
		sb.append(" WHERE CALENDAR_ID = " + weekCalendarId);
		sb.append(" AND LOCATION_LEVEL_ID = " + locationLevelId);
		sb.append(" AND LOCATION_ID = " + locationId);
		sb.append(" ) STORE_ITEM_MAP ");
		sb.append(" LEFT JOIN ITEM_LOOKUP IL ON STORE_ITEM_MAP.ITEM_CODE = IL.ITEM_CODE ");
		sb.append(" LEFT JOIN ");
		sb.append(itemDAO.getProductHierarchy(conn, Constants.CATEGORYLEVELID, productService));
		sb.append(" JOIN PRODUCT_GROUP PG ON ");
		sb.append(" PRODUCT_HIERARCHY.P_ID_" + productLevelPointer.get(Constants.CATEGORYLEVELID) + " = PG.PRODUCT_ID AND ");
		sb.append(" PG.PRODUCT_LEVEL_ID = " + Constants.CATEGORYLEVELID);
		return sb.toString();
	}
	
	public void insertAccuracyComparisonHeader(Connection conn, AccuracyComparisonHeaderDTO accuracyComparisonHeaderDTO) throws GeneralException {
		PreparedStatement stmt = null;
		try {
			stmt = conn.prepareStatement(INSERT_ACCURACY_COMPARISON_HEADER);
			logger.debug("INSERT_ACCURACY_COMPARISON_HEADER:" + INSERT_ACCURACY_COMPARISON_HEADER);
			int counter = 0;
			stmt.setLong(++counter, accuracyComparisonHeaderDTO.getComparisonHeaderId());
			stmt.setInt(++counter, accuracyComparisonHeaderDTO.getCalendarId());
			stmt.setInt(++counter, accuracyComparisonHeaderDTO.getLocationLevelId());
			stmt.setInt(++counter, accuracyComparisonHeaderDTO.getLocationId());
			stmt.setInt(++counter, accuracyComparisonHeaderDTO.getProductLevelId());
			stmt.setInt(++counter, accuracyComparisonHeaderDTO.getProductId());
			stmt.setInt(++counter, accuracyComparisonHeaderDTO.getReportTypeId());
			stmt.setInt(++counter, accuracyComparisonHeaderDTO.getSubReportTypeId());
			stmt.executeUpdate();
		} catch (SQLException e) {
			logger.error("Error in insertAccuracyComparisonHeader() - " + e);
			throw new GeneralException("Error in insertAccuracyComparisonHeader()", e);
		} finally {
			PristineDBUtil.close(stmt);
		}
	}

	public void insertAccuracyComparisonDetail(Connection conn, AccuracyComparisonHeaderDTO accuracyComparisonHeaderDTO,
			List<AccuracyComparisonDetailDTO> accuaryComparisonDetailsDTO) throws GeneralException {
		PreparedStatement stmt = null;
		try {
			stmt = conn.prepareStatement(INSERT_ACCURACY_COMPARISON_DETAIL);
			logger.debug("INSERT_ACCURACY_COMPARISON_DETAIL:" + INSERT_ACCURACY_COMPARISON_DETAIL);
			for (AccuracyComparisonDetailDTO accuracyComparisonDetail : accuaryComparisonDetailsDTO) {
				int counter = 0;
				stmt.setLong(++counter, accuracyComparisonHeaderDTO.getComparisonHeaderId());
				stmt.setInt(++counter, accuracyComparisonDetail.getComparisonParamId());
				stmt.setDouble(++counter, accuracyComparisonDetail.getParameterValue());
				stmt.setString(++counter, accuracyComparisonDetail.getParameterValueText());
				stmt.setInt(++counter, accuracyComparisonDetail.getOrderNo());
				stmt.setString(++counter, accuracyComparisonDetail.getVersionType());
				stmt.addBatch();
			}
			stmt.executeBatch();
		} catch (SQLException e) {
			logger.error("Error in insertAccuracyComparisonDetail() - " + e);
			throw new GeneralException("Error in insertAccuracyComparisonDetail()", e);
		} finally {
			PristineDBUtil.close(stmt);
		}
	}

	public long getMaxComparisonHeaderId(Connection conn) throws GeneralException {
		PreparedStatement stmt = null;
		ResultSet rs = null;
		long headerId = -1;
		try {
			stmt = conn.prepareStatement(GET_MAX_COMPARISON_HEADER_ID);
			logger.debug("GET_MAX_COMPARISON_HEADER_ID:" + GET_MAX_COMPARISON_HEADER_ID);
			rs = stmt.executeQuery();
			if (rs.next()) {
				headerId = rs.getLong("COMPARISON_HEADER_ID");
			}
		} catch (SQLException exception) {
			logger.error("Error in getMaxComparisonHeaderId() - " + exception.toString());
			throw new GeneralException("Error in getMaxComparisonHeaderId() - " + exception.toString());
		} finally {
			PristineDBUtil.close(rs);
			PristineDBUtil.close(stmt);
		}
		return headerId;
	}
	
	public void deleteComparisonHeader(Connection conn,  AccuracyComparisonHeaderDTO accuracyComparisonHeaderDTO) throws GeneralException {
		PreparedStatement stmt = null;
		try {
			stmt = conn.prepareStatement(DELETE_COMPARISON_HEADER);
			int counter = 0;
			stmt.setInt(++counter, accuracyComparisonHeaderDTO.getCalendarId());
			stmt.setInt(++counter, accuracyComparisonHeaderDTO.getLocationLevelId());
			stmt.setInt(++counter, accuracyComparisonHeaderDTO.getLocationId());
			stmt.setInt(++counter, accuracyComparisonHeaderDTO.getProductLevelId());
			stmt.setInt(++counter, accuracyComparisonHeaderDTO.getProductId());
			stmt.setInt(++counter, accuracyComparisonHeaderDTO.getReportTypeId());
			stmt.setInt(++counter, accuracyComparisonHeaderDTO.getSubReportTypeId());
			logger.debug("DELETE_COMPARISON_HEADER:" + DELETE_COMPARISON_HEADER);
			stmt.executeUpdate();
		} catch (SQLException exception) {
			logger.error("Error in deleteComparisonHeader() - " + exception);
			throw new GeneralException("Error in deleteComparisonHeader() - " + exception.toString());
		} finally {
			PristineDBUtil.close(stmt);
		}
	}
	
	public void deleteComparisonDetail(Connection conn,  AccuracyComparisonHeaderDTO accuracyComparisonHeaderDTO) throws GeneralException {
		PreparedStatement stmt = null;
		try {
			stmt = conn.prepareStatement(DELETE_COMPARISON_DETAIL);
			int counter = 0;
			stmt.setInt(++counter, accuracyComparisonHeaderDTO.getCalendarId());
			stmt.setInt(++counter, accuracyComparisonHeaderDTO.getLocationLevelId());
			stmt.setInt(++counter, accuracyComparisonHeaderDTO.getLocationId());
			stmt.setInt(++counter, accuracyComparisonHeaderDTO.getProductLevelId());
			stmt.setInt(++counter, accuracyComparisonHeaderDTO.getProductId());
			stmt.setInt(++counter, accuracyComparisonHeaderDTO.getReportTypeId());
			stmt.setInt(++counter, accuracyComparisonHeaderDTO.getSubReportTypeId());
			logger.debug("DELETE_COMPARISON_DETAIL:" + DELETE_COMPARISON_DETAIL);
			stmt.executeUpdate();
		} catch (SQLException exception) {
			logger.error("Error in deleteComparisonDetail() - " + exception);
			throw new GeneralException("Error in deleteComparisonDetail() - " + exception.toString());
		} finally {
			PristineDBUtil.close(stmt);
		}
	}
	
	public List<OOSItemDTO> getItemsWithForecast(Connection conn, int locationLevelId, int locationId, int calendarId) throws GeneralException {
		List<OOSItemDTO> oosCandidateItems = new ArrayList<>();
		PreparedStatement statement = null;
		ResultSet resultSet = null;
		com.pristine.dao.offermgmt.ItemDAO itemDAO = new com.pristine.dao.offermgmt.ItemDAO();
		StringBuilder sb = new StringBuilder();
		ProductService productService = new ProductService();
		try {
			HashMap<Integer, Integer> productLevelPointer = productService.getProductLevelPointer(conn, Constants.DEPARTMENTLEVELID);

			sb.append(GET_ITEMS_WITH_FORECAST);
			sb.append(" WHERE ");
			sb.append(" OOS_F.LOCATION_LEVEL_ID = " + locationLevelId);
			sb.append(" AND OOS_F.LOCATION_ID  = " + locationId);
			sb.append(" AND OOS_F.CALENDAR_ID = " + calendarId);
			//sb.append(" AND OOS_F.IS_OOS_ANALYSIS = '" + Constants.YES + "' ");
			sb.append("  ) OOS_ITEM LEFT JOIN ITEM_LOOKUP IL ON IL.ITEM_CODE = OOS_ITEM.ITEM_CODE ");
			sb.append(" LEFT JOIN RETAILER_LIKE_ITEM_GROUP RLIG ON OOS_ITEM.RET_LIR_ID = RLIG.RET_LIR_ID ");
			sb.append(" LEFT JOIN PM_WEEKLY_AD_DEFINITION WAD ON (OOS_ITEM.CALENDAR_ID = WAD.CALENDAR_ID ");
			sb.append(" AND WAD.LOCATION_LEVEL_ID = " + locationLevelId + " AND WAD.LOCATION_ID = " + locationId + ")");
			sb.append(" LEFT JOIN PM_WEEKLY_AD_PAGE WAP ON (WAP.PAGE_NUMBER = OOS_ITEM.AD_PAGE_NO ");
			sb.append(" AND WAD.WEEKLY_AD_ID = WAP.WEEKLY_AD_ID) ");
			sb.append(" LEFT JOIN PM_WEEKLY_AD_BLOCK WAB ON (WAB.PAGE_ID = WAP.PAGE_ID ");
			sb.append(" AND WAB.BLOCK_NUMBER = OOS_ITEM.AD_BLOCK_NO ) ");
			sb.append(" LEFT JOIN (SELECT PRODUCT_ID, SUM(TOT_MOVEMENT) ACTUAL_MOVEMENT FROM ");
			sb.append(" ITEM_METRIC_SUMMARY_WEEKLY WHERE CALENDAR_ID = " + calendarId );
			sb.append(" AND LOCATION_LEVEL_ID = " + Constants.STORE_LEVEL_ID);
			sb.append(" AND LOCATION_ID IN (SELECT CHILD_LOCATION_ID FROM ");
			sb.append(" LOCATION_GROUP_RELATION WHERE LOCATION_ID = " + locationId);
			sb.append(" AND LOCATION_LEVEL_ID = " + Constants.STORE_LIST_LEVEL_ID + ") ");
			sb.append("  GROUP BY PRODUCT_ID ) IMS2 ");
			sb.append(" ON IMS2.PRODUCT_ID = OOS_ITEM.ITEM_CODE ");
			sb.append(" AND OOS_ITEM.LOCATION_ID = " + locationId);
			sb.append(" ) STORE_ITEM_MAP LEFT JOIN ");
			sb.append(itemDAO.getProductHierarchy(conn, Constants.DEPARTMENTLEVELID, productService));
			sb.append(" LEFT JOIN PRODUCT_GROUP PG ON ");
			sb.append(" PRODUCT_HIERARCHY.P_ID_" + productLevelPointer.get(Constants.CATEGORYLEVELID) + " = PG.PRODUCT_ID AND ");
			sb.append(" PG.PRODUCT_LEVEL_ID = " + Constants.CATEGORYLEVELID);
			sb.append(" LEFT JOIN PRODUCT_GROUP PG1 ON ");
			sb.append(" PRODUCT_HIERARCHY.P_ID_" + productLevelPointer.get(Constants.DEPARTMENTLEVELID) + " = PG1.PRODUCT_ID AND ");
			sb.append(" PG1.PRODUCT_LEVEL_ID = " + Constants.DEPARTMENTLEVELID);

			logger.debug("Query - GET_ITEMS_WITH_FORECAST -- " + sb.toString());
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
				oosItemDTO.setWeeklyPredictionStatus(resultSet.getInt("PREDICTION_STATUS"));
				 
				oosItemDTO.setAdPageNo(resultSet.getInt("AD_PAGE_NO"));
				oosItemDTO.setBlockNo(resultSet.getInt("AD_BLOCK_NO"));
				 
				oosItemDTO.setDisplayTypeId(resultSet.getInt("DISPLAY_TYPE_ID"));

				oosItemDTO.setRetLirId(resultSet.getInt("RET_LIR_ID"));
				oosItemDTO.setLirName(resultSet.getString("RET_LIR_NAME"));
				if (resultSet.getString("DIST_FLAG").equals(String.valueOf(Constants.DSD)))
					oosItemDTO.setDistFlag(Constants.DSD);
				else
					oosItemDTO.setDistFlag(Constants.WAREHOUSE);

				 
				oosItemDTO.setWeeklyPredictedMovement(resultSet.getLong("PRESTO_FORECAST") > 0 ? resultSet.getLong("PRESTO_FORECAST") : 0);
				
				oosItemDTO.setClientWeeklyPredictedMovement(
						resultSet.getLong("CLIENT_FORECAST_ACTUAL") > 0 ? resultSet.getLong("CLIENT_FORECAST_ACTUAL") : 0);
				
				oosItemDTO.setWeeklyConfidenceLevelLower(
						resultSet.getLong("PRESTO_CONFIDENCE_LEVEL_LOWER") > 0 ? resultSet.getLong("PRESTO_CONFIDENCE_LEVEL_LOWER") : 0);

				oosItemDTO.setWeeklyConfidenceLevelUpper(
						resultSet.getLong("PRESTO_CONFIDENCE_LEVEL_UPPER") > 0 ? resultSet.getLong("PRESTO_CONFIDENCE_LEVEL_UPPER") : 0);

				oosItemDTO.setWeeklyActualMovement(resultSet.getLong("ACTUAL_MOVEMENT"));

				oosItemDTO.setNoOfLigOrNonLig(resultSet.getInt("NO_OF_LIG_OR_NON_LIG"));
				oosCandidateItems.add(oosItemDTO);
			}
		} catch (SQLException sqlE) {
			logger.error("Error -- getItemsWithForecast() - " + sqlE.toString());
			throw new GeneralException("Error -- getItemsWithForecast()", sqlE);
		}
		return oosCandidateItems;
	}
}
