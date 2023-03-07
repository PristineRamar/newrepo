package com.pristine.dao.offermgmt;

import java.sql.Connection;
import java.sql.JDBCType;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Types;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.time.LocalDateTime;
import java.time.format.DateTimeFormatter;
import java.util.ArrayList;
import java.util.Date;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Set;
import java.util.stream.Collectors;

import org.apache.log4j.Logger;

import com.pristine.dto.ReDTO;
import com.pristine.dto.RetailPriceDTO;
import com.pristine.dto.offermgmt.MultiplePrice;
import com.pristine.dto.offermgmt.PRItemDTO;
import com.pristine.exception.GeneralException;
import com.pristine.lookup.offermgmt.RecommendationStatusLookup;
import com.pristine.util.Constants;

import com.pristine.util.PristineDBUtil;
import com.pristine.util.PropertyManager;
import com.pristine.util.offermgmt.PRCommonUtil;
import com.pristine.util.offermgmt.PRConstants;


public class PriceExportDAO {

	private static Logger logger = Logger.getLogger("PriceExportDAO");
	
	public static String GET_SQL_FOR_EMPTY_START_DATE = "INSERT INTO PR_EC_EXPORT_DETAIL (EC_EXPORT_DETAIL_ID, EC_EXPORT_HEADER_ID,"
			+ " PRICE_CHECK_LIST_ID, ITEM_CODE, STORE_ID, COMMENTS, EC_RETAIL, ZONE_NUM) VALUES ( PR_EC_EXPORT_DET_SEQ.NEXTVAL,"
			+ " ?, ?, ?, ?, ?, ?, ?)";
	public static String GET_SQL_FOR_DATA_WITH_START_DATE = "INSERT INTO PR_EC_EXPORT_DETAIL (EC_EXPORT_DETAIL_ID, EC_EXPORT_HEADER_ID,"
			+ " PRICE_CHECK_LIST_ID, ITEM_CODE, STORE_ID, COMMENTS, START_DATE, EC_RETAIL, ZONE_NUM) VALUES ( PR_EC_EXPORT_DET_SEQ.NEXTVAL,"
			+ " ?, ?, ?, ?, ?, TO_DATE(?,'MM/dd/yyyy'), ?, ?)";

	// private static final String GET_RUNIDLIST_FOR_REGULAR_ITEMS = "SELECT
	// PRODUCT_ID, LOCATION_ID, MAX(RUN_ID) AS RUN_ID FROM "
	// + " PR_QUARTER_REC_HEADER WHERE ACTUAL_START_CALENDAR_ID = ? AND
	// ACTUAL_END_CALENDAR_ID = ? AND STATUS IN (%s) ";

	public String getUserDetails(Connection conn, String userId) {
		String userDetail = null;
		PreparedStatement stmt = null;
		ResultSet rs = null;
		try {
			StringBuffer sb = new StringBuffer();
			sb.append("SELECT FIRST_NAME, LAST_NAME FROM USER_DETAILS WHERE USER_ID = '").append(userId).append("'");

			stmt = conn.prepareStatement(sb.toString());
			rs = stmt.executeQuery();
			while (rs.next()) {
				userDetail = rs.getString("FIRST_NAME") + " " + rs.getString("LAST_NAME");
			}
		} catch (SQLException ex) {
			logger.error("Error when getting user details - " + ex.getMessage());
		} finally {
			PristineDBUtil.close(rs);
			PristineDBUtil.close(stmt);
		}
		return userDetail;
	}

	public List<PRItemDTO> getItemsFromApprovedRecommendations(Connection conn, List<Long> runIdList,
			String priceExportType, boolean emergency, String currWeekEndDate) throws GeneralException {
		List<PRItemDTO> baseList = new ArrayList<PRItemDTO>();
		List<String> stringConvertedRunIds = new ArrayList<String>();

		runIdList.forEach(runId -> {
			stringConvertedRunIds.add(String.valueOf(runId));
		});

		if (runIdList.size() > 0) {

			StringBuffer sb = new StringBuffer();
			sb.append(
					" SELECT EX.RUN_ID, EX.PRICE_TYPE, RI.PRODUCT_LEVEL_ID, EX.ITEM_CODE AS PRODUCT_ID, RH.LOCATION_LEVEL_ID,");
			sb.append(" RH.LOCATION_ID, RI.REC_REG_PRICE , RI.CORE_RETAIL, RI.RET_LIR_ID, IL.RETAILER_ITEM_CODE, ");
			sb.append(
					" RI.IS_EXPORTED,TO_CHAR(RI.REG_EFF_DATE, 'MM/DD/YYYY HH24:MI:SS') AS REG_EFF_DATE, RI.CORE_RETAIL, IL.USER_ATTR_6, PG.NAME, ");
			sb.append(" IL.USER_ATTR_3, RI.REG_PRICE, RI.REG_MULTIPLE, RH.PREDICTED_BY, ");
			sb.append(" RPZ.PRICE_ZONE_ID, RPZ.NAME AS ZONE_NAME, RI.OVERRIDE_REG_PRICE, QRS.UPDATED_BY, ");
			sb.append(" UD.FIRST_NAME, UD.LAST_NAME, ");
			sb.append(
					" RPZ.ZONE_NUM,  RI.REC_REG_MULTIPLE, TO_CHAR(RH.PREDICTED, 'MM/DD/YYYY HH24:MI:SS') AS PREDICTED, ");
			sb.append(" RI.PRC_CHANGE_IMPACT, RI.OVERRIDE_REG_MULTIPLE ");
			sb.append(" FROM PR_PRICE_EXPORT EX LEFT JOIN PR_QUARTER_REC_ITEM RI ");
			sb.append(" ON RI.RUN_ID = EX.RUN_ID AND RI.PRODUCT_ID = EX.ITEM_CODE AND RI.PRODUCT_LEVEL_ID = 1");
			sb.append(" LEFT JOIN PR_QUARTER_REC_HEADER RH ON RI.RUN_ID = RH.RUN_ID  LEFT JOIN PRODUCT_GROUP PG ");
			sb.append(" ON RH.PRODUCT_ID = PG.PRODUCT_ID LEFT JOIN RETAIL_PRICE_ZONE RPZ ");
			sb.append(" ON RH.LOCATION_ID = RPZ.PRICE_ZONE_ID LEFT JOIN ITEM_LOOKUP IL ");
			sb.append(" ON RI.PRODUCT_ID = IL.ITEM_CODE ").append(" LEFT JOIN PR_QUARTER_REC_STATUS QRS ");
			sb.append(" ON RI.RUN_ID = QRS.RUN_ID ");
			sb.append(" LEFT JOIN USER_DETAILS UD ON UD.USER_ID = QRS.UPDATED_BY ");
			sb.append(" WHERE RI.CAL_TYPE='Q' AND RI.PRODUCT_LEVEL_ID = 1 ");
			sb.append(" AND QRS.STATUS IN ( ").append(RecommendationStatusLookup.APPROVED.getStatusId()).append(" , ");
			sb.append(RecommendationStatusLookup.EMERGENCY_APPROVED.getStatusId()).append(")");
			sb.append(" AND RI.RUN_ID IN (").append(String.join(",", stringConvertedRunIds)).append(")");
			if(emergency) {
				sb.append(" AND EX.PRICE_TYPE = '").append(Constants.EMERGENCY).append("'");
			}

			if ((priceExportType.equals(Constants.EMERGENCY_OR_HARDPART) && !emergency)
					|| priceExportType.equals(Constants.HARD_PART_ITEMS)) {
				sb.append(" AND IL.USER_ATTR_6 = '").append(Constants.HARD_PART_ITEMS).append("'");
				// em , false;
			} else if ((priceExportType.equals(Constants.EMERGENCY_OR_SALESFLOOR) && !emergency)
					|| priceExportType.equals(Constants.SALE_FLOOR_ITEMS)) {
				sb.append(" AND IL.USER_ATTR_6 = '").append(Constants.SALE_FLOOR_ITEMS).append("'");
				// em = false;
			} /*
				 * else if (priceExportType.equals(Constants.BOTH_SALESFLOOR_AND_HARDPART)) { em
				 * = false; }
				 */
			if(currWeekEndDate!="") {
				sb.append(" AND TO_DATE(TO_CHAR(RI.REG_EFF_DATE,'MM/dd/yyyy'),'MM/dd/yyyy')<=TO_DATE('"+currWeekEndDate+"','MM/dd/yyyy') ");
			}
			
			logger.debug(sb.toString());
			PreparedStatement stmt = null;
			ResultSet rs = null;
			try {

				stmt = conn.prepareStatement(sb.toString());
				// stmt.setString(1, itemType);
				rs = stmt.executeQuery();
				while (rs.next()) {
					PRItemDTO item = new PRItemDTO();

					item.setRunId(rs.getLong("RUN_ID"));
					item.setProductLevelId(rs.getInt("PRODUCT_LEVEL_ID"));
					item.setItemCode(rs.getInt("PRODUCT_ID"));
					item.setChildLocationLevelId(rs.getInt("LOCATION_LEVEL_ID"));
					item.setChildLocationId(rs.getInt("LOCATION_ID"));
					item.setItemType(rs.getString("USER_ATTR_6"));
					item.setRetLirId(rs.getInt("RET_LIR_ID"));
					item.setPriceZoneId(rs.getInt("PRICE_ZONE_ID"));
					item.setPriceZoneNo(rs.getString("ZONE_NUM"));
					item.setRetailerItemCode(rs.getString("RETAILER_ITEM_CODE"));

					MultiplePrice recommendedPrice = new MultiplePrice(rs.getInt("REC_REG_MULTIPLE"),
							rs.getDouble("REC_REG_PRICE"));
					item.setRecommendedRegPrice(recommendedPrice);

					MultiplePrice currentPrice = new MultiplePrice(rs.getInt("REG_MULTIPLE"),
							rs.getDouble("REG_PRICE"));
					item.setCurrentRegPrice(currentPrice);

					item.setRegEffDate(rs.getString("REG_EFF_DATE"));

					item.setPriceExportType(rs.getString("PRICE_TYPE"));

					item.setApprovedBy(rs.getString("UPDATED_BY"));
					item.setApproverName(rs.getString("FIRST_NAME") + " " + rs.getString("LAST_NAME"));

					item.setVdpRetail(item.getRecommendedRegPrice().getUnitPrice());
					item.setCoreRetail(rs.getDouble("CORE_RETAIL"));
					item.setImpact(rs.getDouble("PRC_CHANGE_IMPACT"));
					// item.setPredicted(rs.getString("PREDICTED"));
					item.setPredicted(rs.getString("PREDICTED"));

					item.setPartNumber(rs.getString("USER_ATTR_3"));
					item.setOverrideRegMultiple(rs.getInt("OVERRIDE_REG_MULTIPLE"));
					item.setOverrideRegPrice(rs.getDouble("OVERRIDE_REG_PRICE"));

					MultiplePrice overridePrice = new MultiplePrice(rs.getInt("OVERRIDE_REG_MULTIPLE"),
							rs.getDouble("OVERRIDE_REG_PRICE"));
					item.setOverriddenRegularPrice(overridePrice);

					if (overridePrice.price > 0) {
						item.setDiffRetail(overridePrice.getUnitPrice() - currentPrice.getUnitPrice());
						double diffInPrice = round(item.getDiffRetail(), 2);
						item.setDiffRetail(diffInPrice);
						item.setVdpRetail(overridePrice.getUnitPrice());
					} else {
						item.setDiffRetail(recommendedPrice.getUnitPrice() - currentPrice.getUnitPrice());
						double diffInPrice = round(item.getDiffRetail(), 2);
						item.setDiffRetail(diffInPrice);
						item.setVdpRetail(recommendedPrice.getUnitPrice());
					}

					item.setZoneName(rs.getString("ZONE_NAME"));
					item.setRecommendationUnit(rs.getString("NAME"));
					/*
					 * if (em) { item.setEmergencyInHardPart(true); } if (em) {
					 * item.setEmergencyInSaleFloor(true); }
					 */
					boolean isItemRecommended = true;
					if (overridePrice.price > 0) {
						if (overridePrice.equals(currentPrice)) {
							isItemRecommended = false;
						}
					}else {
						if (recommendedPrice.equals(currentPrice)) {
							isItemRecommended = false;
						}
					}
					
					if (isItemRecommended) {
						baseList.add(item);
					}
				}
			} catch (Exception e) {
				throw new GeneralException("getBaseDataForExport() - Error getting recommneded items for runids", e);
			} finally {
				PristineDBUtil.close(rs);
				PristineDBUtil.close(stmt);
			}
		}

		logger.info(
				"getItemsFromApprovedRecommendations() - # of approved items in candidate list is: " + baseList.size());
		return baseList;
	}

	public List<Long> getRunId(Connection conn, String priceExportType, boolean emergencyInHardPart,
			boolean emergencyInSaleFloor) throws GeneralException {
		List<Long> runIdList = new ArrayList<Long>();
		PreparedStatement stmt = null;
		ResultSet rs = null;
		try {

			StringBuffer sb = new StringBuffer();
			sb.append("SELECT DISTINCT(RUN_ID) AS RUN_ID FROM PR_PRICE_EXPORT WHERE PRICE_TYPE = ? ");
			logger.debug(sb.toString());
			stmt = conn.prepareStatement(sb.toString());

			if (priceExportType.equals(Constants.EMERGENCY_OR_SALESFLOOR)
					|| priceExportType.equals(Constants.EMERGENCY_OR_HARDPART)
					|| priceExportType.equals(Constants.EMERGENCY)) {
				stmt.setString(1, Constants.EMERGENCY);
			}
			rs = stmt.executeQuery();
			while (rs.next()) {
				runIdList.add(rs.getLong("RUN_ID"));
			}

		} catch (SQLException e) {
			throw new GeneralException("getRunId() - Error while getting runIds", e);
		} finally {
			PristineDBUtil.close(rs);
			PristineDBUtil.close(stmt);
		}
		return runIdList;
	}

	public List<String> getStoresOfZones(Connection conn, int price_zone_id) {

		List<String> storeData = new ArrayList<String>();
		StringBuffer sb = new StringBuffer();
		sb.append("SELECT COMP_STR_NO FROM COMPETITOR_STORE WHERE PRICE_ZONE_ID = ").append(price_zone_id);
		PreparedStatement stmt = null;
		ResultSet rs = null;

		try {
			stmt = conn.prepareStatement(sb.toString());
			rs = stmt.executeQuery();
			while (rs.next()) {
				storeData.add(rs.getString("COMP_STR_NO"));
			}
			// storeData.addAll(price_zone_id);
		} catch (SQLException e) {
			e.printStackTrace();
		} finally {
			PristineDBUtil.close(rs);
			PristineDBUtil.close(stmt);
		}

		return storeData;
	}

	public void updateExportItems(Connection conn, List<PRItemDTO> exportList) throws GeneralException {

		PreparedStatement statement = null;
		try {

			StringBuilder sb = new StringBuilder();
			sb.append(" UPDATE PR_QUARTER_REC_ITEM SET IS_EXPORTED = 'Y' WHERE PRODUCT_ID = ?  ");
			sb.append(" AND PRODUCT_LEVEL_ID = ").append(Constants.ITEMLEVELID)
					.append(" AND RUN_ID = ? AND CAL_TYPE = 'Q' ");
			statement = conn.prepareStatement(sb.toString());
			logger.debug("update item table query: " + sb.toString());
			int counter = 0;
			for (PRItemDTO item : exportList) {
				counter++;
				int colIndex = 0;
				statement.setInt(++colIndex, item.getItemCode());
				statement.setLong(++colIndex, item.getRunId());
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
			logger.error("updateExportItems() - Error updating export status for items");
			throw new GeneralException("updateExportItems() - Error updating export status for items", e);
		} finally {
			PristineDBUtil.close(statement);
		}

	}

	public void updateExportStatus(Connection conn, List<PRItemDTO> finalExportList, List<Long> runIdList)
			throws GeneralException {

		PreparedStatement stmt = null;
		int statusCode;
		try {

			StringBuilder runIdStr = new StringBuilder();
			for (int i = 0; i < runIdList.size(); i++) {
				long runId = runIdList.get(i);
				if (i == runIdList.size() - 1) {
					runIdStr.append(runId);
				} else {
					runIdStr.append(runId).append(",");
				}
			}

			StringBuffer sb = new StringBuffer();

			sb.append(" UPDATE PR_QUARTER_REC_HEADER SET EXPORTED = SYSDATE, ");
			sb.append(" EXPORTED_BY = ?, STATUS_DATE = SYSDATE, STATUS = ?, STATUS_ROLE = 0, ");
			sb.append(" STATUS_BY = ? WHERE RUN_ID = ? ");
			/*
			 * sb.append(" STATUS_BY = ? WHERE RUN_ID IN (").append(runIdStr.toString());
			 * sb.append(")");
			 */
			logger.debug(sb.toString());
			stmt = conn.prepareStatement(sb.toString());
			int counter = 0;

			HashMap<Long, Integer> countByRunId = getNotExportedCountForRunIds(conn, runIdList, finalExportList);

			for (Map.Entry<Long, Integer> countEntry : countByRunId.entrySet()) {
				long runId = countEntry.getKey();
				int count = countByRunId.get(runId);

				if (count > 0) {
					statusCode = PRConstants.STATUS_PARTIALLY_EXPORTED;
					logger.debug("updateExportStatus() - Export Status: Exported Partially, status code = "
							+ PRConstants.STATUS_PARTIALLY_EXPORTED);

				} else {
					statusCode = PRConstants.EXPORT_STATUS;
					logger.debug("updateExportStatus() - Export Status: Exported, status code = "
							+ PRConstants.EXPORT_STATUS);

				}

				counter++;
				int colIndex = 0;
				stmt.setString(++colIndex, PRConstants.BATCH_USER);
				stmt.setInt(++colIndex, statusCode);
				stmt.setString(++colIndex, PRConstants.BATCH_USER);
				stmt.setLong(++colIndex, runId);

				stmt.addBatch();
				if (counter % Constants.BATCH_UPDATE_COUNT == 0) {
					stmt.executeBatch();
					stmt.clearBatch();
					counter = 0;
				}

				if (counter > 0) {
					stmt.executeBatch();
					stmt.clearBatch();
				}
			}
		} catch (Exception e) {
			logger.error("updateExportStatus() - error while updating the status in summary ");
			throw new GeneralException("updateExportStatus() - error while updating the status in summary: ", e);
		} finally {
			PristineDBUtil.close(stmt);
		}

	}

	public void insertExportStatus(Connection conn, String exportStatus, List<Long> runIdList,
			List<PRItemDTO> itemsFiltered) throws GeneralException {

		PreparedStatement stmt = null;
		int statusCode;

		try {
			StringBuilder sb = new StringBuilder();
			sb.append(" INSERT INTO PR_QUARTER_REC_STATUS (RECOMMENDATION_STATUS_ID, RUN_ID, ");
			sb.append(
					" STATUS, UPDATED_BY, UPDATED, STATUS_ROLE, MESSAGE)  VALUES (RECOMMENDATION_STATUS_ID_SEQ.NEXTVAL, ");
			sb.append(" ?, ?, ?, SYSDATE, 0, ?)");

			stmt = conn.prepareStatement(sb.toString());
			logger.debug("Query for insertExportStatus() - " + sb.toString());
			int itemCountInBatch = 0;

			HashMap<Long, Integer> countByRunId = getNotExportedCountForRunIds(conn, runIdList, itemsFiltered);

			for (Map.Entry<Long, Integer> countEntry : countByRunId.entrySet()) {
				long runId = countEntry.getKey();
				int count = countByRunId.get(runId);

				if (count > 0) {
					statusCode = PRConstants.STATUS_PARTIALLY_EXPORTED;
					exportStatus = "Partially Exported";
				} else {
					statusCode = PRConstants.EXPORT_STATUS;
					exportStatus = "Exported";
				}

				itemCountInBatch++;
				int colIndex = 0;
				stmt.setLong(++colIndex, runId);
				stmt.setInt(++colIndex, statusCode);
				stmt.setString(++colIndex, PRConstants.BATCH_USER);
				stmt.setString(++colIndex, exportStatus);
				stmt.addBatch();

			}
			if (itemCountInBatch % Constants.BATCH_UPDATE_COUNT == 0) {
				stmt.executeBatch();
				stmt.clearBatch();
				itemCountInBatch = 0;
			}

			if (itemCountInBatch > 0) {
				stmt.executeBatch();
				stmt.clearBatch();
			}
		} catch (SQLException e) {
			logger.error("insertExportStatus() - Error inserting export status for items");
			throw new GeneralException("insertExportStatus() - Error inserting export status for items", e);
		} finally {
			PristineDBUtil.close(stmt);
		}

		// stmt.executeUpdate();

	}

	/*
	 * public HashMap<String, String> getUserDetails(Connection conn) throws
	 * SQLException { HashMap<String, String> userDetails = new HashMap<String,
	 * String>(); PreparedStatement stmt = null; ResultSet rs = null; stmt =
	 * conn.prepareStatement(GET_USER_DETAILS); String userId =
	 * rs.getString("USER_ID"); String userName = rs.getString("FIRST_NAME") + " " +
	 * rs.getString("LAST_NAME"); userDetails.put(userId, userName); return
	 * userDetails; }
	 */
	public HashMap<Integer, List<PRItemDTO>> getItemListInAllLocations(Connection conn, String priceExportType,
			boolean emergencyInHardPart, boolean emergencyInSaleFloor) throws GeneralException {

		logger.debug("getting disapproved items from other locations..");

		HashMap<Integer, List<PRItemDTO>> disApprovedItemListInOtherLocations = new HashMap<Integer, List<PRItemDTO>>();
		StringBuffer sb = new StringBuffer();
		sb.append(" SELECT QRI.PRODUCT_ID, OTHER_LOC.LOCATION_LEVEL_ID, OTHER_LOC.LOCATION_ID, QRI.REG_PRICE, ");
		sb.append(" QRI.OVERRIDE_REG_PRICE, QRI.OVERRIDE_REG_MULTIPLE, ");
		sb.append(" QRI.REG_MULTIPLE, IL.USER_ATTR_6 FROM PR_QUARTER_REC_ITEM QRI ");
		sb.append(" JOIN (SELECT PRH_OTHERS.LOCATION_ID, PRH_OTHERS.LOCATION_LEVEL_ID, ");
		sb.append(" MAX(PRH_OTHERS.RUN_ID) RUN_ID FROM PR_QUARTER_REC_HEADER PRH_OTHERS ");
		sb.append(" JOIN PR_QUARTER_REC_HEADER  PRH_BASE ON PRH_OTHERS.PRODUCT_LEVEL_ID = PRH_BASE.PRODUCT_LEVEL_ID ");
		sb.append(" AND PRH_OTHERS.PRODUCT_ID = PRH_BASE.PRODUCT_ID ");
		sb.append(" AND PRH_OTHERS.ACTUAL_START_CALENDAR_ID = PRH_BASE.ACTUAL_START_CALENDAR_ID ");
		sb.append(" AND PRH_OTHERS.ACTUAL_END_CALENDAR_ID = PRH_BASE.ACTUAL_END_CALENDAR_ID ");
		sb.append(" AND PRH_BASE.RUN_ID IN (SELECT DISTINCT RUN_ID FROM PR_PRICE_EXPORT) ");
		sb.append(" WHERE PRH_OTHERS.STATUS NOT in (").append(RecommendationStatusLookup.APPROVED.getStatusId())
				.append(",");
		sb.append(RecommendationStatusLookup.EMERGENCY_APPROVED.getStatusId()).append(",");
		sb.append(RecommendationStatusLookup.EXPORTED.getStatusId()).append(",")
				.append(RecommendationStatusLookup.EXPORTED_PARTIALLY.getStatusId()).append(") ");
		sb.append(" GROUP BY PRH_OTHERS.LOCATION_ID, PRH_OTHERS.LOCATION_LEVEL_ID) OTHER_LOC ON ");
		sb.append(" OTHER_LOC.RUN_ID = QRI.RUN_ID LEFT JOIN ITEM_LOOKUP IL ON QRI.PRODUCT_ID = IL.ITEM_CODE ");
		sb.append(
				" WHERE QRI.CAL_TYPE = 'Q' AND OTHER_LOC.LOCATION_ID <> (SELECT PRICE_ZONE_ID FROM RETAIL_PRICE_ZONE WHERE GLOBAL_ZONE = 'Y')");

		// sb.append(" AND QRI.PRODUCT_ID = 295158");
		if (priceExportType.equals(Constants.EMERGENCY_OR_HARDPART) && !emergencyInHardPart) {
			sb.append(" AND IL.USER_ATTR_6 = '").append(Constants.HARD_PART_ITEMS).append("'");
		} else if (priceExportType.equals(Constants.EMERGENCY_OR_SALESFLOOR) && !emergencyInSaleFloor) {
			sb.append(" AND IL.USER_ATTR_6 = '").append(Constants.SALE_FLOOR_ITEMS).append("'");
		}

		PreparedStatement stmt = null;
		ResultSet rs = null;

		try {
			stmt = conn.prepareStatement(sb.toString());
			// stmt.setString(1, itemType);
			logger.debug("Query for getItemListInAllLocations() - " + sb.toString());
			rs = stmt.executeQuery();
			while (rs.next()) {

				List<PRItemDTO> priceList = new ArrayList<PRItemDTO>();
				PRItemDTO dto = new PRItemDTO();
				int itemCode = rs.getInt("PRODUCT_ID");
				MultiplePrice recommendedPrice = new MultiplePrice(rs.getInt("REG_MULTIPLE"),
						rs.getDouble("REG_PRICE"));
				dto.setRecommendedRegPrice(recommendedPrice);
				dto.setPriceZoneId(rs.getInt("LOCATION_ID"));
				dto.setChildLocationLevelId(rs.getInt("LOCATION_LEVEL_ID"));
				MultiplePrice overridenPrice = new MultiplePrice(rs.getInt("OVERRIDE_REG_MULTIPLE"),
						rs.getDouble("OVERRIDE_REG_PRICE"));
				dto.setOverriddenRegularPrice(overridenPrice);

				if (disApprovedItemListInOtherLocations.containsKey(itemCode)) {
					priceList = disApprovedItemListInOtherLocations.get(itemCode);
				}

				priceList.add(dto);
				disApprovedItemListInOtherLocations.put(itemCode, priceList);

			}
		} catch (SQLException e) {
			logger.error("getItemListInAllLocations() - Error when getting items from all zones");
			throw new GeneralException(
					"getItemListInAllLocations() - Error when getting items from all zones - " + e.getMessage());
		} finally {
			PristineDBUtil.close(rs);
			PristineDBUtil.close(stmt);
		}

		return disApprovedItemListInOtherLocations;
	}

	public HashMap<String, Integer> getZoneIdMap(Connection conn) {
		PreparedStatement stmt = null;
		ResultSet rs = null;
		String chainId = PropertyManager.getProperty("PRESTO_SUBSCRIBER");
		HashMap<String, Integer> zoneIdMap = new HashMap<String, Integer>();
		try {
			StringBuffer db = new StringBuffer();
			db.append("SELECT ZONE_NUM, PRICE_ZONE_ID FROM RETAIL_PRICE_ZONE WHERE PRICE_ZONE_ID ");
			db.append(" IN (SELECT DISTINCT(PRICE_ZONE_ID) FROM COMPETITOR_STORE WHERE COMP_CHAIN_ID = ")
					.append(chainId).append(")");
			stmt = conn.prepareStatement(db.toString());
			logger.debug("Query for getZoneIdMap() - " + db.toString());
			rs = stmt.executeQuery();
			while (rs.next()) {
				zoneIdMap.put((rs.getString("ZONE_NUM")), rs.getInt("PRICE_ZONE_ID"));
			}
		} catch (SQLException ex) {
			logger.error("getZoneIdMap() - Error when retrieving zone id map - " + ex.getMessage());
		} finally {
			PristineDBUtil.close(rs);
			PristineDBUtil.close(stmt);
		}
		return zoneIdMap;
	}
	
	
	public HashMap<String, Integer> getZonesPartOfGlobalZone(Connection conn) {
		PreparedStatement stmt = null;
		ResultSet rs = null;
		String chainId = PropertyManager.getProperty("PRESTO_SUBSCRIBER");
		HashMap<String, Integer> zoneIdMap = new HashMap<String, Integer>();
		try {
			StringBuffer db = new StringBuffer();
			db.append("SELECT ZONE_NUM, PRICE_ZONE_ID FROM RETAIL_PRICE_ZONE WHERE PRICE_ZONE_ID ");
			db.append(" IN (SELECT DISTINCT(PRICE_ZONE_ID) FROM COMPETITOR_STORE WHERE COMP_CHAIN_ID = ")
					.append(chainId)
					.append(" AND PRICE_ZONE_ID_3 = (SELECT PRICE_ZONE_ID FROM RETAIL_PRICE_ZONE WHERE GLOBAL_ZONE = 'Y'))");
			stmt = conn.prepareStatement(db.toString());
			logger.debug("Query for getZoneIdMap() - " + db.toString());
			rs = stmt.executeQuery();
			while (rs.next()) {
				zoneIdMap.put((rs.getString("ZONE_NUM")), rs.getInt("PRICE_ZONE_ID"));
			}
		} catch (SQLException ex) {
			logger.error("getZoneIdMap() - Error when retrieving zone id map - " + ex.getMessage());
		} finally {
			PristineDBUtil.close(rs);
			PristineDBUtil.close(stmt);
		}
		return zoneIdMap;
	}

	public HashMap<String, Integer> getZoneIdMapForVirtualZone(Connection conn, String virtualZoneNum, List<String> excludeZones) {
		PreparedStatement stmt = null;
		ResultSet rs = null;

		HashMap<String, Integer> zoneIdMap = new HashMap<String, Integer>();
		try {
			StringBuffer db = new StringBuffer();
			db.append("SELECT ZONE_NUM, PRICE_ZONE_ID FROM RETAIL_PRICE_ZONE WHERE GLOBAL_ZONE <> 'Y'");
			db.append(" AND ZONE_NUM <> '").append(virtualZoneNum).append("'");
			//filter added on 02/04/2021 - kirthi
			//to exclude Test zones and excluded zones of AZ for zone 30 price calculation.
			db.append(" AND ZONE_TYPE <> 'T' ");	
			db.append(" AND PRICE_ZONE_ID NOT IN (").append(String.join(",", excludeZones)).append(")");

			stmt = conn.prepareStatement(db.toString());
			logger.debug("getZoneIdMapForVirtualZone() query - " + db.toString());
			rs = stmt.executeQuery();
			while (rs.next()) {
				zoneIdMap.put((rs.getString("ZONE_NUM")), rs.getInt("PRICE_ZONE_ID"));
			}
		} catch (SQLException ex) {
			logger.error("getZoneIdMapForVirtualZone() - Error when retrieving zone id map for virtual zone - "
					+ ex.getMessage());
		} finally {
			PristineDBUtil.close(rs);
			PristineDBUtil.close(stmt);
		}
		return zoneIdMap;
	}

	public String getZoneNameForVirtualZone(Connection conn, String zoneNum) throws GeneralException {
		PreparedStatement stmt = null;
		ResultSet rs = null;

		String zoneName = null;
		try {
			StringBuffer db = new StringBuffer();
			db.append("SELECT name FROM RETAIL_PRICE_ZONE where zone_num = '").append(zoneNum).append("'");

			stmt = conn.prepareStatement(db.toString());
			logger.debug("Query for getZoneNameForVirtualZone() - " + db.toString());
			rs = stmt.executeQuery();
			while (rs.next()) {
				zoneName = rs.getString("name");
			}
		} catch (SQLException ex) {
			logger.error("getZoneNameForVirtualZone() - Error when getting zone name ");
			throw new GeneralException("getZoneNameForVirtualZone() - Error when getting zone name - ", ex);
		} finally {
			PristineDBUtil.close(rs);
			PristineDBUtil.close(stmt);
		}
		return zoneName;
	}

	public HashMap<String, Integer> getExcludedStoresFromItemList(Connection conn, int itemCode, int zoneId,
			boolean expiryOnFutureDate) {

		String priceCheckListIdStr = PropertyManager.getProperty("PRICE_CHECK_LIST_TYPE_ID");
		int priceCheckListId = Integer.parseInt(priceCheckListIdStr);

		// HashMap<String, List<String>> excludedStoreNumFromItemList = new
		// HashMap<String, List<String>>();

		HashMap<String, Integer> mergedExcludeStoreList = new HashMap<String, Integer>();

		HashMap<String, Integer> excludeStoreListOne = getExcludeStoresByItemsAndStoreLock(conn, itemCode, zoneId,
				priceCheckListId, expiryOnFutureDate);

		HashMap<String, Integer> excludeStoreListTwo = getExcludeStoresByItemsAndStoreLockAndLocationLevelId(conn,
				itemCode, zoneId, priceCheckListId, expiryOnFutureDate);

		// logger.debug("# of records in excludeStoreListTwo: " +
		// excludeStoreListTwo.size());

		if (excludeStoreListOne.size() > 0 && excludeStoreListTwo.size() == 0) {
			mergedExcludeStoreList.putAll(excludeStoreListOne);
			return mergedExcludeStoreList;
		}

		else if (excludeStoreListTwo.size() > 0 && excludeStoreListOne.size() == 0) {
			mergedExcludeStoreList.putAll(excludeStoreListTwo);
			return mergedExcludeStoreList;
		}

		else if (excludeStoreListOne.size() > 0 && excludeStoreListTwo.size() > 0) {

			excludeStoreListOne.forEach((store, itemcode) -> {
				mergedExcludeStoreList.put(store, itemcode);
			});
			excludeStoreListTwo.forEach((store, itemcode) -> {
				mergedExcludeStoreList.put(store, itemcode);
			});

		}
		return mergedExcludeStoreList;

	}

	private HashMap<String, Integer> getExcludeStoresByItemsAndStoreLockAndLocationLevelId(Connection conn,
			int itemCode, int zoneId, int priceCheckListId, boolean expiryOnFutureDate) {

		PreparedStatement stmt = null;
		ResultSet rs = null;
		HashMap<String, Integer> excludeStoreList = new HashMap<String, Integer>();
		try {
			StringBuffer sb = new StringBuffer();

			sb.append(
					" select cs.comp_str_no as store_num, pcli.item_code as item_code from price_check_list_items pcli ");
			sb.append(" left join price_check_list pcl on pcli.PRICE_CHECK_LIST_ID = pcl.ID ");
			sb.append(" left join location_group_relation lgr on pcl.location_level_id = lgr.location_level_id ");
			sb.append("  and pcl.location_id = lgr.location_id ");
			sb.append(" left join competitor_store cs on lgr.child_location_id = cs.comp_str_id ");
			sb.append(" where pcli.item_code in ( ").append(itemCode).append(")");
			sb.append(" and lgr.child_location_id in (select comp_str_id from competitor_store where price_zone_id = ")
					.append(zoneId).append(")");
			sb.append(" and pcl.PRICE_CHECK_LIST_TYPE_ID = ").append(priceCheckListId);
			sb.append(" and lgr.location_level_id = ").append(Constants.STORE_LIST_LEVEL_ID);

			if (expiryOnFutureDate) {
				sb.append(
						" and ((PCLI.END_DATE > sysdate or PCLI.END_DATE IS NULL) AND (PCLI.IS_EXPORTED = 'N' or PCLI.IS_EXPORTED IS NULL))");
			} /*
				 * else if (expiryOnPastDays) {
				 * 
				 * sb.append(
				 * " and PCLI.END_DATE <= sysdate and  END_DATE >=  sysdate -7 AND (PCLI.IS_EXPORTED = 'N' or PCLI.IS_EXPORTED IS NULL) "
				 * ); }
				 */

			stmt = conn.prepareStatement(sb.toString());
			logger.debug("getExcludeStoresByItemsAndStoreLockAndLocationLevelId() - query: " + sb.toString());
			rs = stmt.executeQuery();
			while (rs.next()) {
				excludeStoreList.put(rs.getString("store_num"), rs.getInt("item_code"));
			}
		} catch (SQLException ex) {
			logger.error(
					"getExcludeStoresByItemsAndStoreLockAndLocationLevelId() - Error when getting excluded stores from itemList - "
							+ ex.getMessage());
		} finally {
			PristineDBUtil.close(rs);
			PristineDBUtil.close(stmt);
		}
		return excludeStoreList;
	}

	private HashMap<String, Integer> getExcludeStoresByItemsAndStoreLock(Connection conn, int itemCode, int zoneId,
			int priceCheckListId, boolean expiryOnFutureDate) {

		PreparedStatement stmt = null;
		ResultSet rs = null;
		HashMap<String, Integer> excludeStoreList = new HashMap<String, Integer>();
		try {
			StringBuffer sb = new StringBuffer();

			sb.append(
					" select cs.comp_str_no as store_num, pcli.ITEM_CODE as ITEM_CODE, pcli.end_date from price_check_list_items pcli left join competitor_store cs ")
					.append(" on pcli.store_id = cs.comp_str_id where pcli.item_code in ('").append(itemCode)
					.append("')");
			sb.append(" and pcli.store_id in (select comp_str_id from competitor_store where price_zone_id = ")
					.append(zoneId).append(")");
			sb.append(
					" and pcli.PRICE_CHECK_LIST_ID in (select id from price_check_list where PRICE_CHECK_LIST_TYPE_ID = ")
					.append(priceCheckListId).append(") ");

			if (expiryOnFutureDate) {
				sb.append(
						" and ((PCLI.END_DATE > sysdate or PCLI.END_DATE IS NULL) AND (PCLI.IS_EXPORTED = 'N' or PCLI.IS_EXPORTED IS NULL)) ");
			}
			/*
			 * if (expiryOnPastDays) {
			 * 
			 * sb.append(
			 * " and PCLI.END_DATE <= sysdate and  END_DATE >=  sysdate -7 AND (PCLI.IS_EXPORTED = 'N' or PCLI.IS_EXPORTED IS NULL) "
			 * ); }
			 */

			stmt = conn.prepareStatement(sb.toString());
			logger.debug("getExcludeStoresByItemsAndStoreLock() - Query: " + sb.toString());
			rs = stmt.executeQuery();
			while (rs.next()) {

				excludeStoreList.put(rs.getString("store_num"), rs.getInt("ITEM_CODE"));

			}
		} catch (SQLException ex) {
			logger.error("getExcludeStoresByItemsAndStoreLock() - Error when getting excluded stores from itemList - "
					+ ex.getMessage());
		} finally {
			PristineDBUtil.close(rs);
			PristineDBUtil.close(stmt);
		}
		return excludeStoreList;
	}

	public static double round(double value, int places) {
		if (places < 0)
			throw new IllegalArgumentException();

		long factor = (long) Math.pow(10, places);
		value = value * factor;
		long tmp = Math.round(value);
		return (double) tmp / factor;
	}

	/**
	 * 
	 * @param conn
	 * @param runIds
	 * @param itemsFiltered
	 * @return
	 * @throws GeneralException
	 */
	public HashMap<Long, Integer> getNotExportedCountForRunIds(Connection conn, List<Long> runIds,
			List<PRItemDTO> itemsFiltered) throws GeneralException {
		HashMap<Long, Integer> notExportedCountMap = new HashMap<Long, Integer>();
		PreparedStatement statement = null;
		ResultSet resultSet = null;
		try {
			StringBuilder runIdStr = new StringBuilder();
			for (int i = 0; i < runIds.size(); i++) {
				long runId = runIds.get(i);
				if (i == runIds.size() - 1) {
					runIdStr.append(runId);
				} else {
					runIdStr.append(runId).append(",");
				}
			}

			StringBuilder sb = new StringBuilder();
			sb.append("SELECT RUN_ID, COUNT(*) AS COUNT FROM PR_PRICE_EXPORT WHERE ");
			sb.append(" RUN_ID IN (").append(runIdStr.toString()).append(") GROUP BY RUN_ID");
			statement = conn.prepareStatement(sb.toString());
			logger.debug("Query of - getNotExportedCountForRunIds() : " + sb.toString());
			resultSet = statement.executeQuery();
			while (resultSet.next()) {
				notExportedCountMap.put(resultSet.getLong("RUN_ID"), resultSet.getInt("COUNT"));
			}

			for (long runId : runIds) {
				if (!notExportedCountMap.containsKey(runId)) {
					notExportedCountMap.put(runId, 0);
				}
			}
		} catch (SQLException e) {
			logger.error("getNotExportedCountForRunIds() - Error while getting export status of items", e);
			throw new GeneralException("getNotExportedCountForRunIds() - Error while getting export status of items",
					e);
		} finally {
			PristineDBUtil.close(resultSet);
			PristineDBUtil.close(statement);
		}
		return notExportedCountMap;
	}

	public void updateExportLigItems(Connection conn, List<PRItemDTO> itemsFiltered) throws GeneralException {
		PreparedStatement statement = null;
		try {

			StringBuilder sb = new StringBuilder();
			sb.append(" UPDATE PR_QUARTER_REC_ITEM SET IS_EXPORTED = 'Y' WHERE PRODUCT_ID = ?  ");
			sb.append(" AND PRODUCT_LEVEL_ID = ").append(Constants.PRODUCT_LEVEL_ID_LIG)
					.append(" AND RUN_ID = ? AND CAL_TYPE = 'Q'");
			statement = conn.prepareStatement(sb.toString());
			logger.debug("Query for updateExportLigItems() - " + sb.toString());
			int counter = 0;
			HashMap<Integer, List<PRItemDTO>> groupByLIR = (HashMap<Integer, List<PRItemDTO>>) itemsFiltered.stream()
					.filter(p -> p.getRetLirId() > 0).collect(Collectors.groupingBy(PRItemDTO::getRetLirId));

			for (Map.Entry<Integer, List<PRItemDTO>> lirEntry : groupByLIR.entrySet()) {
				int lirId = lirEntry.getKey();
				long runId = lirEntry.getValue().get(0).getRunId();
				counter++;
				int colIndex = 0;
				statement.setInt(++colIndex, lirId);
				statement.setLong(++colIndex, runId);
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
			logger.error("updateExportLigItems() - Error updating export status for lig items");
			throw new GeneralException("updateExportLigItems() - Error updating export status for lig items");
		} finally {
			PristineDBUtil.close(statement);
		}

	}

	public void deleteProcessedRunIds(Connection conn, List<PRItemDTO> itemsFiltered) throws GeneralException {
		PreparedStatement statement = null;
		try {

			StringBuilder sb = new StringBuilder();
			sb.append(" DELETE FROM PR_PRICE_EXPORT WHERE RUN_ID = ? AND ITEM_CODE = ? ");

			statement = conn.prepareStatement(sb.toString());
			logger.debug("deleteProcessedRunIds() - delete query: " + sb.toString());

			int counter = 0;
			for (PRItemDTO item : itemsFiltered) {
				int colIndex = 0;
				counter++;
				logger.debug(
						"deleteProcessedRunIds() - run id: " + item.getRunId() + ", item code: " + item.getItemCode());
				statement.setLong(++colIndex, item.getRunId());
				statement.setInt(++colIndex, item.getItemCode());
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
			logger.error("deleteProcessedRunIds() - Error deleting the items");
			throw new GeneralException("deleteProcessedRunIds() - Error deleting the items");
		} finally {
			PristineDBUtil.close(statement);
		}

	}

	public List<Long> getRunIdListForESandEHItems(Connection conn) throws GeneralException {
		List<Long> runIdList = new ArrayList<Long>();
		PreparedStatement stmt = null;
		ResultSet rs = null;
		try {

			StringBuffer sb = new StringBuffer();
			sb.append("SELECT DISTINCT(RUN_ID) FROM PR_PRICE_EXPORT WHERE PRICE_TYPE = 'N' ");
			logger.debug(sb.toString());
			stmt = conn.prepareStatement(sb.toString());

			rs = stmt.executeQuery();
			while (rs.next()) {
				runIdList.add(rs.getLong("RUN_ID"));
			}

		} catch (SQLException e) {
			throw new GeneralException(
					"getRunIdListForESandEHItems() - Error while getting runIds hardPart/salesfloor or emergency", e);
		} finally {
			PristineDBUtil.close(rs);
			PristineDBUtil.close(stmt);
		}
		return runIdList;
	}

	public HashMap<Long, Integer> getRunIdWithStatusCode(Connection conn, List<Long> runIdList)
			throws GeneralException {
		HashMap<Long, Integer> runIdAndStatusCode = new HashMap<Long, Integer>();
		PreparedStatement stmt = null;
		ResultSet rs = null;
		try {
			/*
			 * StringBuilder runIdStr = new StringBuilder(); for (int i = 0; i <
			 * runIdList.size(); i++) { long runId = runIdList.get(i); if (i ==
			 * runIdList.size() - 1) { runIdStr.append(runId); } else {
			 * runIdStr.append(runId).append(","); } }
			 */
			for (int i = 0; i < runIdList.size(); i++) {
				long runId = runIdList.get(i);
				StringBuffer sb = new StringBuffer();
				sb.append(" SELECT RUN_ID, STATUS FROM PR_QUARTER_REC_HEADER ");
				sb.append(" WHERE RUN_ID = ").append(runId);
				logger.debug(sb.toString());
				stmt = conn.prepareStatement(sb.toString());

				rs = stmt.executeQuery();
				while (rs.next()) {
					runIdAndStatusCode.put(rs.getLong("RUN_ID"), rs.getInt("STATUS"));
				}
			}
		} catch (SQLException e) {
			throw new GeneralException("getRunIdWithStatusCode() - Error while getting runIds with status code", e);
		} finally {
			PristineDBUtil.close(rs);
			PristineDBUtil.close(stmt);
		}

		return runIdAndStatusCode;
	}

	public HashMap<Long, List<PRItemDTO>> getProductLocationDetail(Connection conn, List<Long> runIdList)
			throws GeneralException {
		HashMap<Long, List<PRItemDTO>> productLocationDetailForRunIds = new HashMap<Long, List<PRItemDTO>>();

		PreparedStatement stmt = null;
		ResultSet rs = null;
		try {
			/*
			 * StringBuilder runIdStr = new StringBuilder(); for (int i = 0; i <
			 * runIdList.size(); i++) { long runId = runIdList.get(i); if (i ==
			 * runIdList.size() - 1) { runIdStr.append(runId); } else {
			 * runIdStr.append(runId).append(","); } }
			 */
			for (int i = 0; i < runIdList.size(); i++) {
				long runId = runIdList.get(i);
				StringBuffer sb = new StringBuffer();
				sb.append(" SELECT RUN_ID, PRODUCT_ID, PRODUCT_LEVEL_ID, LOCATION_ID, LOCATION_LEVEL_ID, STATUS FROM ");
				sb.append(" PR_QUARTER_REC_HEADER  WHERE RUN_ID = ").append(runId);
				logger.debug("Query for getProductLocationDetail() - " + sb.toString());
				stmt = conn.prepareStatement(sb.toString());
				rs = stmt.executeQuery();

				List<PRItemDTO> productAndLocationList = new ArrayList<PRItemDTO>();
				while (rs.next()) {
					PRItemDTO itemDto = new PRItemDTO();
					itemDto.setItemCode(rs.getInt("PRODUCT_ID"));
					itemDto.setProductLevelId(rs.getInt("PRODUCT_LEVEL_ID"));
					itemDto.setPriceZoneId(rs.getInt("LOCATION_ID"));
					itemDto.setChildLocationLevelId(rs.getInt("LOCATION_LEVEL_ID"));
					itemDto.setStatusCode(rs.getInt("STATUS"));
					if (productLocationDetailForRunIds.containsKey(rs.getLong("RUN_ID"))) {
						productAndLocationList = productLocationDetailForRunIds.get(rs.getLong("RUN_ID"));
					}
					productAndLocationList.add(itemDto);
					productLocationDetailForRunIds.put(rs.getLong("RUN_ID"), productAndLocationList);

				}
			}

		} catch (SQLException e) {
			logger.error(
					"getProductLocationDetail() - Error while getting product and location detail from header table");
			throw new GeneralException(
					"getProductLocationDetail() - Error while getting product and location detail from header table",
					e);
		} finally {
			PristineDBUtil.close(rs);
			PristineDBUtil.close(stmt);
		}
		return productLocationDetailForRunIds;
	}

	public HashMap<Integer, String> getZoneNoForZoneId(Connection conn) {
		PreparedStatement stmt = null;
		ResultSet rs = null;

		HashMap<Integer, String> zoneIdMap = new HashMap<Integer, String>();
		try {
			StringBuffer db = new StringBuffer();
			db.append("SELECT ZONE_NUM, PRICE_ZONE_ID FROM RETAIL_PRICE_ZONE");

			stmt = conn.prepareStatement(db.toString());
			rs = stmt.executeQuery();
			while (rs.next()) {
				zoneIdMap.put((rs.getInt("PRICE_ZONE_ID")), rs.getString("ZONE_NUM"));
			}
		} catch (SQLException ex) {
			logger.error("getZoneNoForZoneNum() - Error when getting ZoneNoForZoneNum - " + ex.getMessage());
		} finally {
			PristineDBUtil.close(rs);
			PristineDBUtil.close(stmt);
		}
		return zoneIdMap;
	}
	
	public HashMap<Integer, String> getZoneNoForZoneIdWithoutTestZone(Connection conn) {
		PreparedStatement stmt = null;
		ResultSet rs = null;

		HashMap<Integer, String> zoneIdMap = new HashMap<Integer, String>();
		try {
			StringBuffer db = new StringBuffer();
			db.append("SELECT ZONE_NUM, PRICE_ZONE_ID FROM RETAIL_PRICE_ZONE WHERE ZONE_TYPE <> 'T'");

			stmt = conn.prepareStatement(db.toString());
			rs = stmt.executeQuery();
			while (rs.next()) {
				zoneIdMap.put((rs.getInt("PRICE_ZONE_ID")), rs.getString("ZONE_NUM"));
			}
		} catch (SQLException ex) {
			logger.error("getZoneNoForZoneNum() - Error when getting ZoneNoForZoneNum - " + ex.getMessage());
		} finally {
			PristineDBUtil.close(rs);
			PristineDBUtil.close(stmt);
		}
		return zoneIdMap;
	}

	public void updateExpiryExportFlagForStoreList(Connection conn, List<PRItemDTO> expiredStoreData)
			throws GeneralException {
		PreparedStatement statement = null;

		try {

			StringBuilder sb = new StringBuilder();
			sb.append(" UPDATE PRICE_CHECK_LIST_ITEMS SET IS_EXPORTED = 'Y' WHERE ITEM_CODE = ? ");
			sb.append(" AND PRICE_CHECK_LIST_ID = ? ");

			statement = conn.prepareStatement(sb.toString());
			logger.debug("Query for updateExportFlagOfStoreLockItems() - " + sb.toString());

			int counter = 0;

			for (PRItemDTO dto : expiredStoreData) {
				counter++;
				int colIndex = 0;
				statement.setInt(++colIndex, dto.getItemCode());
				statement.setInt(++colIndex, dto.getPriceCheckListId());
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
			logger.error("updateExpiryExportFlagForStoreList() - Error updating export flag for store list expiry store lock items");
			throw new GeneralException(
					"updateExpiryExportFlagForStoreList()- Error updating export flag for store list expiry store lock items", e);
		} finally {
			PristineDBUtil.close(statement);
		}
	}

	public void updateExpiryExportFlagForRegularItemList(Connection conn, List<PRItemDTO> expiredStoreData)
			throws GeneralException {
		PreparedStatement statement = null;

		try {

			StringBuilder sb = new StringBuilder();
			sb.append(" UPDATE PRICE_CHECK_LIST_ITEMS SET IS_EXPORTED = 'Y' WHERE ITEM_CODE = ? ");
			sb.append(" AND STORE_ID IN (SELECT COMP_STR_ID FROM COMPETITOR_STORE WHERE COMP_STR_NO = ?) ");
			sb.append(" AND PRICE_CHECK_LIST_ID = ? ");

			statement = conn.prepareStatement(sb.toString());
			logger.debug("Query for updateExportFlagOfStoreLockItems() - " + sb.toString());

			int counter = 0;

			for (PRItemDTO dto : expiredStoreData) {
				counter++;
				int colIndex = 0;
				statement.setInt(++colIndex, dto.getItemCode());
				statement.setString(++colIndex, dto.getStoreNo());
				statement.setInt(++colIndex, dto.getPriceCheckListId());
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
			logger.error("updateExpiryExportFlagForRegularItemList() - Error updating export flag for regular expiry store lock items");
			throw new GeneralException(
					"updateExpiryExportFlagForRegularItemList()- Error updating export flag for regular expiry store lock items", e);
		} finally {
			PristineDBUtil.close(statement);
		}
	}
	
	public HashMap<Integer, List<PRItemDTO>> setPriceForExpiryItems(boolean ExpiryOnCurrentDate, Connection conn, String priceExportType) {

		String priceCheckListIdStr = PropertyManager.getProperty("PRICE_CHECK_LIST_TYPE_ID");
		int priceCheckListId = Integer.parseInt(priceCheckListIdStr);
		
		// contains ==> <itemcode, List<storeNum,priceCheckListId>>
		HashMap<Integer, List<PRItemDTO>> expiryItemMap = getExpiryItemsFromStoreLock(conn, priceCheckListId, priceExportType);
		HashMap<Integer, List<PRItemDTO>> expiryItemMapStoreList = getExpiryItemsFromStoreLockFromLocation(conn,
				priceCheckListId, priceExportType);

		HashMap<Integer, List<PRItemDTO>> mergedMap = new HashMap<>();

		if (expiryItemMap.size() > 0) {	
			
			// map1 containg All itemcode as key and empty list as value
			expiryItemMap.forEach((itemCode, storeList) -> {
				List<PRItemDTO> storeListEmpty = new ArrayList<>();
				mergedMap.put(itemCode, storeListEmpty);
			});
		}

		if (expiryItemMapStoreList.size() > 0) {
			
			// map2 containg All itemcode as key and empty list as value
			expiryItemMapStoreList.forEach((itemCode, storeList) -> {
				List<PRItemDTO> storeListEmpty = new ArrayList<>();				
				mergedMap.put(itemCode, storeListEmpty);
			});
		}
		mergedMap.forEach((itemCode, storeList) -> {
			
			if (expiryItemMap.containsKey(itemCode) && expiryItemMapStoreList.containsKey(itemCode)) {
				List<PRItemDTO> expiryStores = expiryItemMap.get(itemCode);
				List<PRItemDTO> expiryStoresFromStoreList = expiryItemMapStoreList.get(itemCode);
				Set<String> distinctStores = new HashSet<>();
				// HashMap<Integer, List<PRItemDTO>> commonMapWithUniquePCLId = new HashMap<>();
				// List<PRItemDTO> storeAndPCLId = new ArrayList<>();
				for (PRItemDTO objects : expiryStores) {
					
					if (!distinctStores.contains(objects.getStoreNo())) {
						distinctStores.add(objects.getStoreNo());
						storeList.add(objects);
					}
				}

				for (PRItemDTO storeFromStoreList : expiryStoresFromStoreList) {
					
					if (!distinctStores.contains(storeFromStoreList.getStoreNo())) {
						distinctStores.add(storeFromStoreList.getStoreNo());
						storeList.add(storeFromStoreList);
					}
				}
			} else if (expiryItemMap.containsKey(itemCode)) {
				
				List<PRItemDTO> expiryStores = expiryItemMap.get(itemCode);
				storeList.addAll(expiryStores);
			} else if (expiryItemMapStoreList.containsKey(itemCode)) {
				
				List<PRItemDTO> expiryStoresFromStoreList = expiryItemMapStoreList.get(itemCode);
				storeList.addAll(expiryStoresFromStoreList);
			}
		});

		return mergedMap;
	}

	private HashMap<Integer, List<PRItemDTO>> getExpiryItemsFromStoreLockFromLocation(Connection conn,
			int priceCheckListId, String priceExportType) {
		PreparedStatement stmt = null;
		ResultSet rs = null;

		HashMap<Integer, List<PRItemDTO>> expiryItemMapTwo = new HashMap<>();

		try {
			StringBuffer sb = new StringBuffer();
			sb.append(
					" select cs.comp_str_no, pcli.item_code as item_code, pcli.price_check_list_id from price_check_list_items pcli ");
			sb.append(" left join price_check_list pcl on pcli.PRICE_CHECK_LIST_ID = pcl.ID ");
			sb.append(" left join location_group_relation lgr on pcl.location_level_id = lgr.location_level_id ");
			sb.append(" and pcl.location_id = lgr.location_id ");
			sb.append(" left join competitor_store cs on lgr.child_location_id = cs.comp_str_id ");
			sb.append(" left join item_lookup il on pcli.item_code = il.item_code ");
			sb.append(" where pcl.PRICE_CHECK_LIST_TYPE_ID = ").append(priceCheckListId);
			sb.append(" and (pcli.IS_EXPORTED = 'N' or pcli.IS_EXPORTED IS NULL) ");
			sb.append(" and lgr.location_level_id = ").append(Constants.STORE_LIST_LEVEL_ID);
			sb.append(" and pcli.END_DATE <= sysdate and pcli.END_DATE >=  sysdate -7 ");
			sb.append(" AND (CS.STORE_EXCLUSION_FLAG = 'N' OR CS.STORE_EXCLUSION_FLAG IS NULL)");
			if (priceExportType.equals(Constants.SALE_FLOOR_ITEMS)
					|| priceExportType.equals(Constants.EMERGENCY_OR_SALESFLOOR)) {
				sb.append(" and il.USER_ATTR_6 = '").append(Constants.SALE_FLOOR_ITEMS).append("'");
			}
			if (priceExportType.equals(Constants.HARD_PART_ITEMS)
					|| priceExportType.equals(Constants.EMERGENCY_OR_HARDPART)) {
				sb.append(" and il.USER_ATTR_6 = '").append(Constants.HARD_PART_ITEMS).append("'");
			}

			stmt = conn.prepareStatement(sb.toString());
			logger.debug("getExpiryItemsFromStoreLockFromLocation() query - " + sb.toString());
			rs = stmt.executeQuery();
		
			while (rs.next()) {
			
				List<PRItemDTO> expList = new ArrayList<PRItemDTO>();
				PRItemDTO dto = new PRItemDTO();
				dto.setStoreNo(rs.getString("comp_str_no"));
				dto.setPriceCheckListId(rs.getInt("price_check_list_id"));
				dto.setStoreListExpiry(true);
				dto.setItemCode(rs.getInt("item_code"));
				if (expiryItemMapTwo.containsKey(rs.getInt("item_code"))) {
					expList = expiryItemMapTwo.get(rs.getInt("item_code"));
				}
				expList.add(dto);
				expiryItemMapTwo.put((rs.getInt("item_code")), expList);
				
			}
		} catch (Exception ex) {
			logger.error("getExpiryItemsFromStoreLockFromLocation() - Error when getting expiry date item - "
					+ ex.getMessage());
		} finally {
			PristineDBUtil.close(rs);
			PristineDBUtil.close(stmt);
		}
		
		return expiryItemMapTwo;
	}

	private HashMap<Integer, List<PRItemDTO>> getExpiryItemsFromStoreLock(Connection conn, int priceCheckListId, String priceExportType) {

		PreparedStatement stmt = null;
		ResultSet rs = null;

		HashMap<Integer, List<PRItemDTO>> expiryItemMapOne = new HashMap<>();

		try {
			StringBuffer db = new StringBuffer();
			db.append(" select distinct(pcli.item_code) as item_code, cs.comp_str_no, ");
			db.append(" pcli.price_check_list_id from price_check_list_items pcli ");
			db.append(" left join competitor_store cs on pcli.store_id = cs.comp_str_id ");
			db.append(" left join item_lookup il on pcli.item_code = il.item_code ");
			db.append(" where (pcli.IS_EXPORTED = 'N' or pcli.IS_EXPORTED IS NULL)");
			db.append(" and pcli.PRICE_CHECK_LIST_ID in (select id from price_check_list where ");
			db.append(" PRICE_CHECK_LIST_TYPE_ID = ").append(priceCheckListId).append(") ");
			db.append(" and pcli.END_DATE <= sysdate and pcli.END_DATE >=  sysdate -7 ");
			db.append(" and pcli.store_id is not null AND (CS.STORE_EXCLUSION_FLAG = 'N' OR CS.STORE_EXCLUSION_FLAG IS NULL)");
			if(priceExportType.equals(Constants.SALE_FLOOR_ITEMS) || priceExportType.equals(Constants.EMERGENCY_OR_SALESFLOOR)) {
			db.append(" and il.USER_ATTR_6 = '").append(Constants.SALE_FLOOR_ITEMS).append("'");
			}
			if(priceExportType.equals(Constants.HARD_PART_ITEMS) || priceExportType.equals(Constants.EMERGENCY_OR_HARDPART)) {
				db.append(" and il.USER_ATTR_6 = '").append(Constants.HARD_PART_ITEMS).append("'");
			}

			stmt = conn.prepareStatement(db.toString());
			logger.debug("getExpiryItemsFromStoreLock()  Query - " + db.toString());
			rs = stmt.executeQuery();
			while (rs.next()) {
				List<PRItemDTO> expList = new ArrayList<PRItemDTO>();
				PRItemDTO dto = new PRItemDTO();
				dto.setStoreNo(rs.getString("comp_str_no"));
				dto.setStoreListExpiry(false);
				dto.setPriceCheckListId(rs.getInt("price_check_list_id"));
				dto.setItemCode(rs.getInt("item_code"));
				if (expiryItemMapOne.containsKey(rs.getInt("item_code"))) {
					expList = expiryItemMapOne.get(rs.getInt("item_code"));
				}
				expList.add(dto);
				expiryItemMapOne.put((rs.getInt("item_code")), expList);

			}
		} catch (Exception ex) {
			logger.error("getExpiryItemsFromStoreLock() - Error when getting expiry date item- " + ex.getMessage());
		} finally {
			PristineDBUtil.close(rs);
			PristineDBUtil.close(stmt);
		}
		
		return expiryItemMapOne;
	}

	public List<ReDTO> getNextWeekExpiredItems(Connection conn) {
		String priceCheckListIdStr = PropertyManager.getProperty("PRICE_CHECK_LIST_TYPE_ID");
		int priceCheckListId = Integer.parseInt(priceCheckListIdStr);
		PreparedStatement stmt = null;
		ResultSet rs = null;

		List<ReDTO> expiredItemsOnNextWeek = new ArrayList<ReDTO>();

		try {
			StringBuffer db = new StringBuffer();

			db.append(" select pcli.item_code, il.retailer_item_code, cs.comp_str_no, pcli.end_date, rpz.name, ");
			db.append(" rpz.zone_num from price_check_list_items pcli ");
			db.append(" left join competitor_store cs on pcli.store_id = cs.comp_str_id ");
			db.append(" left join retail_price_zone rpz on cs.price_zone_id = rpz.price_zone_id ");
			db.append(" left join item_lookup il on pcli.item_code = il.item_code ");
			db.append(" where (pcli.IS_EXPORTED = 'N' or pcli.IS_EXPORTED IS NULL)");
			db.append(" and pcli.PRICE_CHECK_LIST_ID in (select id from price_check_list where ");
			db.append(" PRICE_CHECK_LIST_TYPE_ID = ").append(priceCheckListId).append(") ");
			db.append(" and pcli.END_DATE > sysdate and pcli.END_DATE <=  sysdate +7 ");

			stmt = conn.prepareStatement(db.toString());
			logger.debug("getNextWeekExpiredItems() - query: " + db.toString());
			rs = stmt.executeQuery();
			while (rs.next()) {

				ReDTO dto = new ReDTO();

				dto.setStoreNo(rs.getString("comp_str_no"));
				dto.setEndDate(rs.getString("end_date"));
				dto.setItemCode(rs.getInt("item_code"));
				dto.setRetailerItemCode(rs.getString("retailer_item_code"));
				dto.setZoneName(rs.getString("name"));
				dto.setZoneNo(rs.getString("zone_num"));

				expiredItemsOnNextWeek.add(dto);

			}
		} catch (SQLException ex) {
			logger.error(
					"getNextWeekExpiredItems() - Error when getting next week expiry date item - " + ex.getMessage());
		} finally {
			PristineDBUtil.close(rs);
			PristineDBUtil.close(stmt);
		}
		return expiredItemsOnNextWeek;

	}

	public HashMap<Integer, String> getCategoryAndRecomUnitOfItem(Connection conn,
			HashMap<Integer, List<ReDTO>> expiredItemsOnNextWeek, int productLevelId) {

		HashMap<Integer, String> nameAndItemPair = new HashMap<Integer, String>();

		PreparedStatement stmt = null;
		ResultSet rs = null;

		for (Map.Entry<Integer, List<ReDTO>> itemObj : expiredItemsOnNextWeek.entrySet()) {

			try {
				StringBuffer sb = new StringBuffer();

				if (productLevelId == 4) {
					sb.append(" select pgc.name from product_group pg ");
				} else if (productLevelId == 7) {
					sb.append(" select pgcr.name from product_group pg ");
				}
				sb.append(
						" left join product_group_relation pgr on pgr.product_id = pg.product_id and pgr.product_level_id = pg.product_level_id ");
				sb.append(
						" left join product_group pgc on pgr.child_product_level_id = pgc.product_level_id and pgr.child_product_id = pgc.product_id ");

				sb.append(
						" left join product_group_relation_rec pgrr on pgrr.product_id = pg.product_id and pgrr.product_level_id = pg.product_level_id ");
				sb.append(
						" left join product_group pgcr on pgrr.child_product_level_id = pgcr.product_level_id and pgrr.child_product_id = pgcr.product_id ");

				if (productLevelId == 4) {
					sb.append(
							" where ( pgc.product_id in (SELECT product_id FROM (SELECT * FROM PRODUCT_GROUP_RELATION_REC PGR ");
					sb.append(" START WITH CHILD_PRODUCT_LEVEL_ID = 1 AND CHILD_PRODUCT_ID in (")
							.append(itemObj.getKey()).append(") ");
					sb.append(
							" CONNECT BY  PRIOR PRODUCT_ID = CHILD_PRODUCT_ID AND  PRIOR PRODUCT_LEVEL_ID = CHILD_PRODUCT_LEVEL_ID) ");
					sb.append(" WHERE PRODUCT_LEVEL_ID = ").append(productLevelId).append(" )) ");
				} else if (productLevelId == 7) {
					sb.append(
							" where ( pgcr.product_id in (SELECT product_id FROM (SELECT * FROM PRODUCT_GROUP_RELATION_REC PGR ");
					sb.append(" START WITH CHILD_PRODUCT_LEVEL_ID = 1 AND CHILD_PRODUCT_ID in (")
							.append(itemObj.getKey()).append(") ");
					sb.append(
							" CONNECT BY  PRIOR PRODUCT_ID = CHILD_PRODUCT_ID AND  PRIOR PRODUCT_LEVEL_ID = CHILD_PRODUCT_LEVEL_ID) ");
					sb.append(" WHERE PRODUCT_LEVEL_ID = ").append(productLevelId).append(" )) ");
				}

				stmt = conn.prepareStatement(sb.toString());
				rs = stmt.executeQuery();
				while (rs.next()) {
					if (productLevelId == 4) {
						nameAndItemPair.put(itemObj.getKey(), rs.getString("name"));
					} else if (productLevelId == 7) {
						nameAndItemPair.put(itemObj.getKey(), rs.getString("name"));
					}
				}

			} catch (SQLException ex) {
				logger.error(
						"getCategoryAndRecomUnitOfItem() - Error when getting Category/recommendation Unit of item - "
								+ ex.getMessage());
			} finally {
				PristineDBUtil.close(rs);
				PristineDBUtil.close(stmt);
			}
		}
		return nameAndItemPair;
	}

	public List<Long> getRunIdForItemsOfAllType(Connection conn) throws GeneralException {
		List<Long> runIdList = new ArrayList<Long>();
		PreparedStatement stmt = null;
		ResultSet rs = null;
		try {

			StringBuffer sb = new StringBuffer();
			sb.append("SELECT DISTINCT(RUN_ID) AS RUN_ID FROM PR_PRICE_EXPORT");
			logger.debug(sb.toString());
			stmt = conn.prepareStatement(sb.toString());

			rs = stmt.executeQuery();
			while (rs.next()) {
				runIdList.add(rs.getLong("RUN_ID"));
			}

		} catch (SQLException e) {
			throw new GeneralException("getRunIdForItemsOfAllType() - Error while getting all runIds", e);
		} finally {
			PristineDBUtil.close(rs);
			PristineDBUtil.close(stmt);
		}
		return runIdList;
	}

	public HashMap<Integer, String> getZoneNameForZoneId(Connection conn) {
		PreparedStatement stmt = null;
		ResultSet rs = null;

		HashMap<Integer, String> zoneNameMap = new HashMap<Integer, String>();
		try {
			StringBuffer db = new StringBuffer();
			db.append("SELECT NAME, PRICE_ZONE_ID FROM RETAIL_PRICE_ZONE ");

			stmt = conn.prepareStatement(db.toString());
			rs = stmt.executeQuery();
			while (rs.next()) {
				zoneNameMap.put((rs.getInt("PRICE_ZONE_ID")), rs.getString("NAME"));
			}
		} catch (SQLException ex) {
			logger.error("getZoneNameForZoneId() - Error when retrieving zone name map - " + ex.getMessage());
		} finally {
			PristineDBUtil.close(rs);
			PristineDBUtil.close(stmt);
		}
		return zoneNameMap;
	}

	public <K, V> K getKey(Map<K, V> map, V value) {
		for (K key : map.keySet()) {
			if (value.equals(map.get(key))) {
				return key;
			}
		}
		return null;
	}

	public void insertExportTrackId(Connection conn, List<Long> runIdList) throws GeneralException {

		PreparedStatement stmt = null;
		try {
			StringBuilder sb = new StringBuilder();
			sb.append(" INSERT INTO PR_PRICE_EXPORT_TRACK (PRICE_EXPORT_TRACK_ID, RUN_ID, ");
			sb.append(" EXPORTED)  VALUES (PRICE_EXPORT_TRACK_ID_SEQ.NEXTVAL, ?, SYSDATE)");

			stmt = conn.prepareStatement(sb.toString());
			logger.debug("insertExportTrackId() Query- " + sb.toString());
			int itemCountInBatch = 0;

			for (Long runId : runIdList) {
				itemCountInBatch++;
				int colIndex = 0;
				stmt.setLong(++colIndex, runId);
				stmt.addBatch();
			}

			if (itemCountInBatch % Constants.BATCH_UPDATE_COUNT == 0) {
				stmt.executeBatch();
				stmt.clearBatch();
				itemCountInBatch = 0;
			}

			if (itemCountInBatch > 0) {
				stmt.executeBatch();
				stmt.clearBatch();
			}
		} catch (SQLException e) {
			logger.error("insertExportTrackId() - Error inserting export track Id");
			throw new GeneralException("insertExportTrackId() - Error inserting export track id", e);
		} finally {
			PristineDBUtil.close(stmt);
		}

	}

	public List<Long> getEmergencyRunIds(Connection conn, String currWeekEndDate) throws GeneralException {
		List<Long> runIdList = new ArrayList<Long>();
		PreparedStatement stmt = null;
		ResultSet rs = null;
		try {

			StringBuffer sb = new StringBuffer();
			sb.append("SELECT DISTINCT(PE.RUN_ID) FROM PR_PRICE_EXPORT PE ");
			sb.append(" LEFT JOIN PR_QUARTER_REC_ITEM PQI ON PE.ITEM_CODE = PQI.PRODUCT_ID AND PE.RUN_ID = PQI.RUN_ID AND PQI.PRODUCT_LEVEL_ID = 1  AND PQI.CAL_TYPE = 'Q' ");
			sb.append(" WHERE PE.PRICE_TYPE = 'E' ");
			
			if(currWeekEndDate!="")
			sb.append(" AND TO_DATE(TO_CHAR(PQI.REG_EFF_DATE,'MM/dd/yyyy'),'MM/dd/yyyy')<=TO_DATE('"+currWeekEndDate+"','MM/dd/yyyy') ");
			
			logger.debug(sb.toString());
			stmt = conn.prepareStatement(sb.toString());

			rs = stmt.executeQuery();
			while (rs.next()) {
				runIdList.add(rs.getLong("RUN_ID"));
			}

		} catch (SQLException e) {
			throw new GeneralException("getEmergencyRunIds() - Error while getting runIds for emergrncy items", e);
		} finally {
			PristineDBUtil.close(rs);
			PristineDBUtil.close(stmt);
		}
		return runIdList;
	}

	public List<Long> getNormalRunIds(Connection conn, String currWeekEndDate) throws GeneralException {
		List<Long> runIdList = new ArrayList<Long>();
		PreparedStatement stmt = null;
		ResultSet rs = null;
		try {

			StringBuffer sb = new StringBuffer();
			sb.append("SELECT DISTINCT(PE.RUN_ID) FROM PR_PRICE_EXPORT PE ");
			sb.append(" LEFT JOIN PR_QUARTER_REC_ITEM PQI ON PE.ITEM_CODE = PQI.PRODUCT_ID AND PE.RUN_ID = PQI.RUN_ID AND PQI.PRODUCT_LEVEL_ID = 1  AND PQI.CAL_TYPE = 'Q' ");
			sb.append(" WHERE PE.PRICE_TYPE = 'N' ");
			
			if(currWeekEndDate!="")
			sb.append(" AND TO_DATE(TO_CHAR(PQI.REG_EFF_DATE,'MM/dd/yyyy'),'MM/dd/yyyy')<=TO_DATE('"+currWeekEndDate+"','MM/dd/yyyy') ");
			
			logger.debug(sb.toString());
			stmt = conn.prepareStatement(sb.toString());

			rs = stmt.executeQuery();
			while (rs.next()) {
				runIdList.add(rs.getLong("RUN_ID"));
			}

		} catch (SQLException e) {
			throw new GeneralException("getNormalRunIds() - Error while getting runIds for normal items", e);
		} finally {
			PristineDBUtil.close(rs);
			PristineDBUtil.close(stmt);
		}
		return runIdList;
	}
	

	
	
	/**
	 * 
	 * @param conn
	 * @param itemCodes
	 * @param zones
	 * @param lockListTypeId
	 * @return stores from store lock list
	 */
	public void getExcludeStoresFromStoreLockList(Connection conn, Set<Integer> itemCodes, Set<Integer> zones,
			int lockListTypeId, int storeLockLocationLevelId,
			HashMap<Integer, HashMap<Integer, Set<String>>> itemStoreLockMap, String priceExportType) {
		List<Integer> itemCodeList = new ArrayList<Integer>();
		int limitcount = 0;
		for (Integer itemCode : itemCodes) {
			itemCodeList.add(itemCode);
			limitcount++;
			if (limitcount > 0 && (limitcount % Constants.BATCH_UPDATE_COUNT == 0)) {
				Object[] values = itemCodeList.toArray();
				retrieveLockedStoresForItems(conn,priceExportType, lockListTypeId, zones, itemStoreLockMap, storeLockLocationLevelId,
						values);
				itemCodeList.clear();
			}
		}

		if (itemCodeList.size() > 0) {
			Object[] values = itemCodeList.toArray();
			retrieveLockedStoresForItems(conn, priceExportType, lockListTypeId, zones, itemStoreLockMap, storeLockLocationLevelId,
					values);
			itemCodeList.clear();
		}
	}

	private static final String GET_LOCKED_STORES_FROM_PRICE_CHECK_LIST = " SELECT CS.COMP_STR_NO AS STORE_NUM, "
			+ " PCLI.STORE_ID, CS.PRICE_ZONE_ID, PCLI.ITEM_CODE AS ITEM_CODE, "
			+ " PCLI.END_DATE FROM PRICE_CHECK_LIST_ITEMS PCLI LEFT JOIN COMPETITOR_STORE CS  ON PCLI.STORE_ID = CS.COMP_STR_ID WHERE PCLI.ITEM_CODE IN (%s) "
			+ " AND PCLI.STORE_ID IN (SELECT COMP_STR_ID FROM COMPETITOR_STORE "
			+ " WHERE PRICE_ZONE_ID IN (%ZONES%) AND (STORE_EXCLUSION_FLAG = 'N' OR STORE_EXCLUSION_FLAG IS NULL)) "
			+ " AND PCLI.PRICE_CHECK_LIST_ID IN (SELECT ID FROM PRICE_CHECK_LIST WHERE PRICE_CHECK_LIST_TYPE_ID = %CHECK_LIST%) "
			+ " AND (PCLI.END_DATE > SYSDATE OR PCLI.END_DATE IS NULL) ";
	

	private static final String GET_LOCKED_STORES_FROM_PRICE_CHECK_LIST_AT_STORELIST = "SELECT CS.COMP_STR_NO AS STORE_NUM, "
			+ " PCLI.ITEM_CODE AS ITEM_CODE, CS.PRICE_ZONE_ID FROM PRICE_CHECK_LIST_ITEMS PCLI "
			+ " LEFT JOIN PRICE_CHECK_LIST PCL ON PCLI.PRICE_CHECK_LIST_ID = PCL.ID "
			+ " LEFT JOIN LOCATION_GROUP_RELATION LGR ON PCL.LOCATION_LEVEL_ID = LGR.LOCATION_LEVEL_ID "
			+ " AND PCL.LOCATION_ID = LGR.LOCATION_ID "
			+ " LEFT JOIN COMPETITOR_STORE CS ON LGR.CHILD_LOCATION_ID = CS.COMP_STR_ID "
			+ " WHERE PCLI.ITEM_CODE IN (%s) AND LGR.CHILD_LOCATION_ID IN "
			+ " (SELECT COMP_STR_ID FROM COMPETITOR_STORE "
			+ " WHERE PRICE_ZONE_ID IN (%ZONES%)  AND (STORE_EXCLUSION_FLAG = 'N' OR STORE_EXCLUSION_FLAG IS NULL)) "
			+ " AND PCL.PRICE_CHECK_LIST_TYPE_ID = %CHECK_LIST% AND LGR.LOCATION_LEVEL_ID = %LOC_LEVEL% "
			+ " AND (PCLI.END_DATE > SYSDATE OR PCLI.END_DATE IS NULL)";

	/**
	 * 
	 * @param conn
	 * @param lockListTypeId
	 * @param zones
	 * @param itemStoreLockMap
	 * @param values
	 */
	private void retrieveLockedStoresForItems(Connection conn, String priceExportType, int lockListTypeId, Set<Integer> zones,
			HashMap<Integer, HashMap<Integer, Set<String>>> itemStoreLockMap, int storeLockLocationLevelId,
			Object... values) {
		PreparedStatement stmt = null;
		ResultSet rs = null;
		try {
			//for store level id
			StringBuilder db = new StringBuilder();
			db.append(" SELECT CS.COMP_STR_NO AS STORE_NUM, ");
			db.append(" PCLI.STORE_ID, CS.PRICE_ZONE_ID, PCLI.ITEM_CODE AS ITEM_CODE, ");
			db.append(" PCLI.END_DATE FROM PRICE_CHECK_LIST_ITEMS PCLI LEFT JOIN COMPETITOR_STORE CS ON PCLI.STORE_ID = CS.COMP_STR_ID  ");
			db.append(" left join item_lookup il on pcli.item_code = il.item_code ");
			db.append(" WHERE PCLI.ITEM_CODE IN (%s) ");
			db.append(" AND PCLI.STORE_ID IN (SELECT COMP_STR_ID FROM COMPETITOR_STORE ");
			db.append(
					" WHERE PRICE_ZONE_ID IN (%ZONES%) AND (STORE_EXCLUSION_FLAG = 'N' OR STORE_EXCLUSION_FLAG IS NULL)) ");
			db.append(
					" AND PCLI.PRICE_CHECK_LIST_ID IN (SELECT ID FROM PRICE_CHECK_LIST WHERE PRICE_CHECK_LIST_TYPE_ID = %CHECK_LIST%) ");
			db.append(" AND (PCLI.END_DATE > SYSDATE OR PCLI.END_DATE IS NULL) ");
			if (priceExportType.equals(Constants.SALE_FLOOR_ITEMS)
					|| priceExportType.equals(Constants.EMERGENCY_OR_SALESFLOOR)) {
				db.append(" and il.USER_ATTR_6 = '").append(Constants.SALE_FLOOR_ITEMS).append("'");
			}
			if (priceExportType.equals(Constants.HARD_PART_ITEMS)
					|| priceExportType.equals(Constants.EMERGENCY_OR_HARDPART)) {
				db.append(" and il.USER_ATTR_6 = '").append(Constants.HARD_PART_ITEMS).append("'");
			}
			
			//for store list level id
			StringBuilder sb = new StringBuilder();
			sb.append(" SELECT CS.COMP_STR_NO AS STORE_NUM, ");
			sb.append(" PCLI.ITEM_CODE AS ITEM_CODE, CS.PRICE_ZONE_ID FROM PRICE_CHECK_LIST_ITEMS PCLI ");
			sb.append(" left join item_lookup il on pcli.item_code = il.item_code ");
			sb.append(" LEFT JOIN PRICE_CHECK_LIST PCL ON PCLI.PRICE_CHECK_LIST_ID = PCL.ID ");
			sb.append(" LEFT JOIN LOCATION_GROUP_RELATION LGR ON PCL.LOCATION_LEVEL_ID = LGR.LOCATION_LEVEL_ID ");
			sb.append(" AND PCL.LOCATION_ID = LGR.LOCATION_ID ");
			sb.append(" LEFT JOIN COMPETITOR_STORE CS ON LGR.CHILD_LOCATION_ID = CS.COMP_STR_ID ");
			sb.append(" WHERE PCLI.ITEM_CODE IN (%s) AND LGR.CHILD_LOCATION_ID IN ");
			sb.append(" (SELECT COMP_STR_ID FROM COMPETITOR_STORE ");
			sb.append(" WHERE PRICE_ZONE_ID IN (%ZONES%)  AND (STORE_EXCLUSION_FLAG = 'N' OR STORE_EXCLUSION_FLAG IS NULL)) ");
			sb.append(" AND PCL.PRICE_CHECK_LIST_TYPE_ID = %CHECK_LIST% AND LGR.LOCATION_LEVEL_ID = %LOC_LEVEL% ");
			sb.append(" AND (PCLI.END_DATE > SYSDATE OR PCLI.END_DATE IS NULL)");
			if (priceExportType.equals(Constants.SALE_FLOOR_ITEMS)
					|| priceExportType.equals(Constants.EMERGENCY_OR_SALESFLOOR)) {
				sb.append(" and il.USER_ATTR_6 = '").append(Constants.SALE_FLOOR_ITEMS).append("'");
			}
			if (priceExportType.equals(Constants.HARD_PART_ITEMS)
					|| priceExportType.equals(Constants.EMERGENCY_OR_HARDPART)) {
				sb.append(" and il.USER_ATTR_6 = '").append(Constants.HARD_PART_ITEMS).append("'");
			}
			
			if (storeLockLocationLevelId == Constants.STORE_LEVEL_ID) {
				String sql = db.toString()
						.replaceAll("%ZONES%", PRCommonUtil.getCommaSeperatedStringFromIntSet(zones))
						.replaceAll("%CHECK_LIST%", String.valueOf(lockListTypeId));
				
				stmt = conn.prepareStatement(String.format(sql, PristineDBUtil.preparePlaceHolders(values.length)));
				logger.debug("sql of store lock items at store level: " + sql);
				PristineDBUtil.setValues(stmt, values);
			} else if (storeLockLocationLevelId == Constants.STORE_LIST_LEVEL_ID) {
				String sql = sb.toString()
						.replaceAll("%ZONES%", PRCommonUtil.getCommaSeperatedStringFromIntSet(zones))
						.replaceAll("%CHECK_LIST%", String.valueOf(lockListTypeId))
						.replaceAll("%LOC_LEVEL%", String.valueOf(Constants.STORE_LIST_LEVEL_ID));
				stmt = conn.prepareStatement(String.format(sql, PristineDBUtil.preparePlaceHolders(values.length)));
				logger.debug("sql of store lock items at store list level: "  + sql);
				PristineDBUtil.setValues(stmt, values);
			}

			stmt.setFetchSize(50000);
			rs = stmt.executeQuery();

			while (rs.next()) {
				logger.debug("itemcode: " + rs.getInt("ITEM_CODE") + " zoneId: " + rs.getInt("PRICE_ZONE_ID") + " storeNO: " + rs.getString("STORE_NUM"));
				int itemCode = rs.getInt("ITEM_CODE");
				int priceZoneId = rs.getInt("PRICE_ZONE_ID");
				String storeNo = rs.getString("STORE_NUM");
				
				if (itemStoreLockMap.containsKey(itemCode)) {
					HashMap<Integer, Set<String>> zoneStoreMap = itemStoreLockMap.get(itemCode);
					if (zoneStoreMap.containsKey(priceZoneId)) {
						Set<String> stores = zoneStoreMap.get(priceZoneId);
						stores.add(storeNo);
						zoneStoreMap.put(priceZoneId, stores);
					} else {
						Set<String> stores = new HashSet<>();
						stores.add(storeNo);
						zoneStoreMap.put(priceZoneId, stores);
					}
					itemStoreLockMap.put(itemCode, zoneStoreMap);
				} else {
					HashMap<Integer, Set<String>> zoneStoreMap = new HashMap<>();
					Set<String> stores = new HashSet<>();
					stores.add(storeNo);
					zoneStoreMap.put(priceZoneId, stores);
					itemStoreLockMap.put(itemCode, zoneStoreMap);
				}
			}
		} catch (SQLException ex) {
			logger.error("getExcludeStoresByItemsAndStoreLock() - Error when getting excluded stores from itemList - "
					+ ex.getMessage());
		} finally {
			PristineDBUtil.close(rs);
			PristineDBUtil.close(stmt);
		}
	}

	public List<PRItemDTO> getEmergencyAndClearanceItems(Connection conn) {
		List<PRItemDTO> clearanceItemlist = new ArrayList<>();
		PreparedStatement stmt = null;
		ResultSet rs = null;
		try {
		StringBuilder sb = new StringBuilder();
		sb.append(" SELECT PCLI.ITEM_CODE, PCLI.EC_RETAIL, TO_CHAR(PCLI.START_DATE, 'MM/DD/YYYY HH24:MI:SS') START_DATE, TO_CHAR(PCLI.END_DATE, 'MM/DD/YYYY HH24:MI:SS') END_DATE, PCLI.PRICE_CHECK_LIST_ID, IL.RETAILER_ITEM_CODE,");
		sb.append(" PCL.CREATE_USER_ID, PCL.PRICE_CHECK_LIST_TYPE_ID, CS.COMP_STR_NO, CS.COMP_STR_ID, PCLI.ZONE_NUM, PCLI.DEFINED_BY, UD.FIRST_NAME, UD.LAST_NAME, ");
		sb.append(" IL.USER_ATTR_6, IL.USER_ATTR_3");
		sb.append(" FROM PRICE_CHECK_LIST_ITEMS PCLI LEFT JOIN PRICE_CHECK_LIST PCL ON PCLI.PRICE_CHECK_LIST_ID = PCL.ID ");
		sb.append(" LEFT JOIN COMPETITOR_STORE CS ON PCLI.STORE_ID = CS.COMP_STR_ID ");
		sb.append(" LEFT JOIN ITEM_LOOKUP IL ON PCLI.ITEM_CODE = IL.ITEM_CODE ");
		sb.append(" LEFT JOIN USER_DETAILS UD ON PCLI.DEFINED_BY = UD.USER_ID ");
		sb.append(" WHERE PCL.PRICE_CHECK_LIST_TYPE_ID IN (").append(Integer.parseInt(Constants.CLEARANCE_LIST_TYPE));
		sb.append(",").append(Integer.parseInt(Constants.EMERGENCY_LIST_TYPE)).append(")");
		sb.append(" AND ((PCLI.START_DATE > SYSDATE AND PCLI.START_DATE <= SYSDATE+7) OR PCLI.START_DATE IS NULL)");
		sb.append(" AND (PCLI.IS_EXPORTED = 'N' OR PCLI.IS_EXPORTED IS NULL) ");
		
		logger.debug("getEmergencyAndClearanceItems() - query: " +sb.toString());
		
		stmt = conn.prepareStatement(sb.toString());

		rs = stmt.executeQuery();
		while (rs.next()) {			
			PRItemDTO dto = new PRItemDTO();
			dto.setItemCode(rs.getInt("ITEM_CODE"));
			dto.setPartNumber(rs.getString("USER_ATTR_3"));
			dto.setItemType(rs.getString("USER_ATTR_6"));
			dto.setRetailerItemCode(rs.getString("RETAILER_ITEM_CODE"));
			dto.setECRetail(rs.getDouble("EC_RETAIL"));
			dto.setStartDate(rs.getString("START_DATE"));
			dto.setEndDate(rs.getString("END_DATE"));
			dto.setPriceCheckListId(rs.getInt("PRICE_CHECK_LIST_ID"));
			dto.setPriceCheckListTypeId(rs.getInt("PRICE_CHECK_LIST_TYPE_ID"));
			dto.setApprovedBy(rs.getString("CREATE_USER_ID"));
			dto.setStoreNo(rs.getString("COMP_STR_NO"));
			dto.setStoreId(rs.getString("COMP_STR_ID"));
			dto.setPriceZoneNo(rs.getString("ZONE_NUM"));
			dto.setApprovedBy(rs.getString("DEFINED_BY"));
			if(dto.getApprovedBy() == null) {
				dto.setApprovedBy("");
			}
			if(rs.getString("FIRST_NAME") == null) {
				dto.setApproverName("");
			}else {
			dto.setApproverName(rs.getString("FIRST_NAME") + " " +rs.getString("LAST_NAME"));
			}
			clearanceItemlist.add(dto);
			
		}
		}catch(Exception ex) {
			logger.error("getEmergencyAndClearanceItems() - Error when getting Emergency and Clearance Items from itemList - "
					+ ex.getMessage());
		} finally {
			PristineDBUtil.close(rs);
			PristineDBUtil.close(stmt);
		}
		
		return clearanceItemlist;
	}

	public void updateClearanceItemsStatus(Connection conn, List<PRItemDTO> clearanceItem) throws GeneralException {
		SimpleDateFormat format = new SimpleDateFormat("MM/dd/yyyy");
		PreparedStatement stmt = null;
		try {
			StringBuilder sb = new StringBuilder();
			sb.append(" UPDATE PRICE_CHECK_LIST_ITEMS SET IS_EXPORTED = 'Y' WHERE ITEM_CODE = ? ");
			sb.append(" AND PRICE_CHECK_LIST_ID = ? ");
			
			stmt = conn.prepareStatement(sb.toString());
			logger.debug("updateClearanceItemsStatus() Query- " + sb.toString());
			int itemCountInBatch = 0;

			for (PRItemDTO dto : clearanceItem) {
				itemCountInBatch++;
				int colIndex = 0;
				stmt.setInt(++colIndex, dto.getItemCode());
				stmt.setInt(++colIndex, dto.getPriceCheckListId());
				stmt.addBatch();
			}

			if (itemCountInBatch % Constants.BATCH_UPDATE_COUNT == 0) {
				stmt.executeBatch();
				stmt.clearBatch();
				itemCountInBatch = 0;
			}

			if (itemCountInBatch > 0) {
				stmt.executeBatch();
				stmt.clearBatch();
			}
		} catch (SQLException e) {
			logger.error("updateClearanceItemsStatus() - Error updating clearance Items");
			throw new GeneralException("updateClearanceItemsStatus() - Error updating clearance Items", e);
		} finally {
			PristineDBUtil.close(stmt);
		}

	}

	public void clearRetailOfClearanceItems(Connection conn, List<PRItemDTO> ECItem) throws GeneralException {
		List<PRItemDTO> clearanceItem = ECItem.stream().filter(e -> e.getPriceCheckListTypeId() == 
				Integer.parseInt(Constants.CLEARANCE_LIST_TYPE)).collect(Collectors.toList());
		if(clearanceItem.size() > 0) {
		logger.debug("Clearing the retail of clearance Items..");
		PreparedStatement stmt = null;
		try {
			StringBuilder sb = new StringBuilder();
			sb.append(" UPDATE PRICE_CHECK_LIST_ITEMS SET EC_RETAIL = ? WHERE IS_EXPORTED = 'Y' AND ITEM_CODE = ? ");
			sb.append(" AND PRICE_CHECK_LIST_ID = ?");

			stmt = conn.prepareStatement(sb.toString());
			logger.debug("clearRetailOfClearanceItems() Query- " + sb.toString());
			int itemCountInBatch = 0;

			for (PRItemDTO dto : clearanceItem) {
				itemCountInBatch++;
				int colIndex = 0;
				stmt.setNull(++colIndex, Types.NULL);
				stmt.setInt(++colIndex, dto.getItemCode());
				stmt.setInt(++colIndex, dto.getPriceCheckListId());
				//stmt.setString(++colIndex, dto.getEndDate());
				stmt.addBatch();
			}

			if (itemCountInBatch % Constants.BATCH_UPDATE_COUNT == 0) {
				stmt.executeBatch();
				stmt.clearBatch();
				itemCountInBatch = 0;
			}

			if (itemCountInBatch > 0) {
				stmt.executeBatch();
				stmt.clearBatch();
			}
		} catch (SQLException e) {
			logger.error("clearRetailOfClearanceItems() - Error clearing retail of clearance Items");
			throw new GeneralException("clearRetailOfClearanceItems() - Error clearing retail of clearance Items", e);
		} finally {
			PristineDBUtil.close(stmt);
		}
		}
	}

	public void insertECDataToHeader(Connection conn, List<PRItemDTO> ECItems) throws GeneralException {
		PreparedStatement stmt = null;
		HashMap<Integer, List<PRItemDTO>> mapByPriceCheckListId = (HashMap<Integer, List<PRItemDTO>>) ECItems.stream()
				.collect(Collectors.groupingBy(PRItemDTO :: getPriceCheckListId));
		try {
			StringBuilder sb = new StringBuilder();
			sb.append(" INSERT INTO PR_EC_EXPORT_HEADER (EC_EXPORT_HEADER_ID, ITEM_LIST_ID, EXPORTED, EXPORTED_BY) ");
			sb.append(" VALUES (PR_EC_EXPORT_SEQ.NEXTVAL, ?, SYSDATE, ?) ");
			
			stmt = conn.prepareStatement(sb.toString());
			logger.debug("insertECDataToHeader() Query- " + sb.toString());
			int itemCountInBatch = 0;

			for (Map.Entry<Integer, List<PRItemDTO>> entry : mapByPriceCheckListId.entrySet()) {
				List<PRItemDTO> valuesOfPriceCheckListId = entry.getValue();
				//for(PRItemDTO dto : valuesOfPriceCheckListId){
				itemCountInBatch++;
				int colIndex = 0;
				stmt.setInt(++colIndex, entry.getKey());	
				stmt.setString(++colIndex, valuesOfPriceCheckListId.get(0).getApprovedBy());
				stmt.addBatch();
				//}
			}

			if (itemCountInBatch % Constants.BATCH_UPDATE_COUNT == 0) {
				stmt.executeBatch();
				stmt.clearBatch();
				itemCountInBatch = 0;
			}

			if (itemCountInBatch > 0) {
				stmt.executeBatch();
				stmt.clearBatch();
			}
		} catch (SQLException e) {
			logger.error("insertECDataToHeader() - Error updating clearance Items in to header");
			throw new GeneralException("insertECDataToHeader() - Error inserting clearance Items in to header", e);
		} finally {
			PristineDBUtil.close(stmt);
		}
	}

	public void insertECDataToDetail(Connection conn, List<PRItemDTO> clearanceItem) throws GeneralException, ParseException {
		
		List<PRItemDTO> dataFromItemList = getItemListDetail(conn, clearanceItem);
		
		List<PRItemDTO> emptyStartDateList = dataFromItemList.stream().filter(e -> e.getStartDate().length() < 0).filter(e -> e.getStartDate().isEmpty() ).collect(Collectors.toList());
		List<PRItemDTO> startDateList = dataFromItemList.stream().filter(e -> e.getStartDate().length() > 0 ) .collect(Collectors.toList());
	
			if(emptyStartDateList.size() > 0) {
				insertECDataWithoutStartDate(conn, emptyStartDateList);
			}
			if(startDateList.size() > 0) {
				insertECDataWithStartDate(conn, startDateList);
			}
			
	}

	private void insertECDataWithStartDate(Connection conn, List<PRItemDTO> startDateList) throws GeneralException, ParseException {
		PreparedStatement stmt = null;
		try {
		stmt = conn.prepareStatement(GET_SQL_FOR_DATA_WITH_START_DATE);
		logger.debug("insertECDataWithStartDate() Query: " + GET_SQL_FOR_DATA_WITH_START_DATE);
		int itemCountInBatch = 0;
		for (PRItemDTO dto : startDateList) {
			itemCountInBatch++;
			int colIndex = 0;
			stmt.setInt(++colIndex, dto.getItemListHeaderId());
			stmt.setInt(++colIndex, dto.getPriceCheckListId());	
			stmt.setInt(++colIndex, dto.getItemCode());
			stmt.setString(++colIndex, (dto.getStoreId() == null) ? null : dto.getStoreId());
			stmt.setString(++colIndex, (dto.getItemListComments() == null) ? null : dto.getItemListComments());		
			stmt.setString(++colIndex, dto.getStartDate());
			try {
				String retail = String.valueOf(dto.getECRetail());
				if(!retail.isEmpty()) {
				stmt.setString(++colIndex, retail);
				}
			}catch(Exception e) {
				stmt.setNull(++colIndex, Types.NULL);
			}
			if(dto.getPriceZoneNo() == null) {
				stmt.setNull(++colIndex, Types.NULL);
			}else {
				stmt.setString(++colIndex, dto.getPriceZoneNo());
			}
			stmt.addBatch();
		
		}
		if (itemCountInBatch % Constants.BATCH_UPDATE_COUNT == 0) {
			stmt.executeBatch();
			stmt.clearBatch();
			itemCountInBatch = 0;
		}

		if (itemCountInBatch > 0) {
			stmt.executeBatch();
			stmt.clearBatch();
		}
		
	} catch (SQLException e) {
		logger.error("insertECDataToDetail() - Error updating clearance Items in to Detail Table");
		throw new GeneralException("insertECDataToDetail() - Error inserting clearance Items in to Detail Table", e);
	} finally {
		PristineDBUtil.close(stmt);
	}
		
	}

	private void insertECDataWithoutStartDate(Connection conn, List<PRItemDTO> emptyStartDateList) throws GeneralException {
		PreparedStatement stmt = null;
		try {
		stmt = conn.prepareStatement(GET_SQL_FOR_EMPTY_START_DATE);
		
		int itemCountInBatch = 0;
		for (PRItemDTO dto : emptyStartDateList) {
			itemCountInBatch++;
			int colIndex = 0;
			stmt.setInt(++colIndex, dto.getItemListHeaderId());
			stmt.setInt(++colIndex, dto.getPriceCheckListId());	
			stmt.setInt(++colIndex, dto.getItemCode());
			stmt.setString(++colIndex, (dto.getStoreId() == null) ? null : dto.getStoreId());
			stmt.setString(++colIndex, (dto.getItemListComments() == null) ? null : dto.getItemListComments());
			
			try {
				String retail = String.valueOf(dto.getECRetail());
				stmt.setString(++colIndex, retail);
			}catch(Exception e) {
				stmt.setNull(++colIndex, Types.NULL);
			}
			if(dto.getPriceZoneNo() == null) {
				stmt.setNull(++colIndex, Types.NULL);
			}else {
				stmt.setString(++colIndex, dto.getPriceZoneNo());
			}
			stmt.addBatch();
		
		}
		if (itemCountInBatch % Constants.BATCH_UPDATE_COUNT == 0) {
			stmt.executeBatch();
			stmt.clearBatch();
			itemCountInBatch = 0;
		}

		if (itemCountInBatch > 0) {
			stmt.executeBatch();
			stmt.clearBatch();
		}
		
	} catch (SQLException e) {
		logger.error("insertECDataToDetail() - Error updating clearance Items in to Detail Table");
		throw new GeneralException("insertECDataToDetail() - Error inserting clearance Items in to Detail Table", e);
	} finally {
		PristineDBUtil.close(stmt);
	}
		
	}

	private List<PRItemDTO> getItemListDetail(Connection conn, List<PRItemDTO> clearanceItem) {
		List<PRItemDTO> listItems = new ArrayList<PRItemDTO>();
		
		PreparedStatement stmt = null;
		ResultSet rs = null;
		for(PRItemDTO item : clearanceItem) {
		try {
		StringBuilder sb = new StringBuilder();
		sb.append(" SELECT PCLI.PRICE_CHECK_LIST_ID, PCLI.ITEM_CODE, PCLI.STORE_ID, PCLI.COMMENTS, TO_CHAR(PCLI.END_DATE, 'MM/DD/YYYY') AS END_DATE, TO_CHAR(PCLI.START_DATE, 'MM/DD/YYYY') AS START_DATE, ");
		sb.append(" PCLI.MIN_PRICE, PCLI.MAX_PRICE, PCLI.LOCKED_RETAIL, PCLI.IS_EXPORTED, PCLI.EC_RETAIL, PCLI.ZONE_NUM, ECH.EC_EXPORT_HEADER_ID ");
		sb.append(" FROM PRICE_CHECK_LIST_ITEMS PCLI LEFT JOIN PR_EC_EXPORT_HEADER ECH ON PCLI.PRICE_CHECK_LIST_ID = ECH.ITEM_LIST_ID ");
		sb.append(" WHERE PCLI.ITEM_CODE = ").append(item.getItemCode()).append(" AND PCLI.PRICE_CHECK_LIST_ID = ").append(item.getPriceCheckListId());
		
		//logger.debug("getClearanceItems() - query: " +sb.toString());
		stmt = conn.prepareStatement(sb.toString());

		rs = stmt.executeQuery();
		while (rs.next()) {			
			PRItemDTO dto = new PRItemDTO();			
			dto.setItemCode(rs.getInt("ITEM_CODE"));
			dto.setECRetail(rs.getDouble("EC_RETAIL"));
			dto.setStartDate(rs.getString("START_DATE"));
			if(dto.getStartDate() == null) {
				dto.setStartDate("");
			}
			dto.setEndDate(rs.getString("END_DATE"));
			dto.setPriceCheckListId(rs.getInt("PRICE_CHECK_LIST_ID"));
			dto.setStoreId(rs.getString("STORE_ID"));
			dto.setItemListComments(rs.getString("COMMENTS"));
			dto.setMinRetail(rs.getDouble("MIN_PRICE"));
			dto.setMaxRetail(rs.getDouble("MAX_PRICE"));
			dto.setLockedRetail(rs.getDouble("LOCKED_RETAIL"));
			dto.setItemListHeaderId(rs.getInt("EC_EXPORT_HEADER_ID"));
			dto.setECRetail(rs.getDouble("EC_RETAIL"));
			listItems.add(dto);
		}
		
		}catch(Exception ex) {
			logger.error("getItemListDetail() - Error when getting itemList detail - "
					+ ex.getMessage());
		} finally {
			PristineDBUtil.close(rs);
			PristineDBUtil.close(stmt);
		}
		}
		return listItems;
	}

	public void updateEmergencyItemsStatus(Connection conn, List<PRItemDTO> emergencyItems) throws GeneralException {
		PreparedStatement stmt = null;
		try {
			StringBuilder sb = new StringBuilder();
			sb.append(" UPDATE PRICE_CHECK_LIST_ITEMS SET IS_EXPORTED = 'Y' WHERE ITEM_CODE = ?");
			sb.append(" AND PRICE_CHECK_LIST_ID = ?");

			stmt = conn.prepareStatement(sb.toString());
			logger.debug("updateEmergencyItemsStatus() Query- " + sb.toString());
			int itemCountInBatch = 0;

			for (PRItemDTO dto : emergencyItems) {
				itemCountInBatch++;
				int colIndex = 0;
				stmt.setInt(++colIndex, dto.getItemCode());				
				stmt.setInt(++colIndex, dto.getPriceCheckListId());
				stmt.addBatch();
			}

			if (itemCountInBatch % Constants.BATCH_UPDATE_COUNT == 0) {
				stmt.executeBatch();
				stmt.clearBatch();
				itemCountInBatch = 0;
			}

			if (itemCountInBatch > 0) {
				stmt.executeBatch();
				stmt.clearBatch();
			}
		} catch (SQLException e) {
			logger.error("updateEmergencyItemsStatus() - Error updating emergency Items");
			throw new GeneralException("updateEmergencyItemsStatus() - Error updating emergency Items", e);
		} finally {
			PristineDBUtil.close(stmt);
		}
	}

	public List<PRItemDTO> getHardpartItemLeftout(Connection conn, List<Long> runIdList) {
		List<PRItemDTO> hardpartItems = new ArrayList<PRItemDTO>();
		
		StringBuilder runIdStr = new StringBuilder();
		for (int i = 0; i < runIdList.size(); i++) {
			long runId = runIdList.get(i);
			if (i == runIdList.size() - 1) {
				runIdStr.append(runId);
			} else {
				runIdStr.append(runId).append(",");
			}
		}
		
		PreparedStatement stmt = null;
		ResultSet rs = null;
		try {
			StringBuilder sb = new StringBuilder();
			sb.append(" SELECT DISTINCT(EX.RUN_ID) AS RUN_ID, EX.PRICE_TYPE AS PRICE_TYPE, EX.ITEM_CODE AS PRODUCT_ID, RI.IS_EXPORTED, ");
			sb.append(" TO_CHAR(RI.REG_EFF_DATE, 'MM/DD/YYYY') AS REG_EFF_DATE ");
			sb.append(" FROM PR_PRICE_EXPORT EX LEFT JOIN PR_QUARTER_REC_ITEM RI ");
			sb.append(" ON RI.RUN_ID = EX.RUN_ID AND RI.PRODUCT_ID = EX.ITEM_CODE AND RI.PRODUCT_LEVEL_ID = 1");
			sb.append(" LEFT JOIN ITEM_LOOKUP IL ON RI.PRODUCT_ID = IL.ITEM_CODE ");
			sb.append(" WHERE RI.CAL_TYPE='Q' AND RI.PRODUCT_LEVEL_ID = 1 AND IL.USER_ATTR_6 = '").append(Constants.HARD_PART_ITEMS).append("'");
			sb.append(" AND (RI.IS_EXPORTED IS NULL OR RI.IS_EXPORTED = 'N') AND RI.REG_EFF_DATE <= SYSDATE ");
			sb.append(" AND EX.PRICE_TYPE = 'N' ");
			stmt = conn.prepareStatement(sb.toString());
			logger.debug("getHardpartItemLeftout() Query - " + sb.toString());

		rs = stmt.executeQuery();
		while (rs.next()) {			
			PRItemDTO item = new PRItemDTO();
			item.setRunId(rs.getLong("RUN_ID"));
			item.setPriceExportType(rs.getString("PRICE_TYPE"));
			item.setItemCode(rs.getInt("PRODUCT_ID"));
			item.setRegEffDate("REG_EFF_DATE");
			hardpartItems.add(item);
		}
		
		}catch(Exception ex) {
			logger.error("getHardpartItemLeftout() - Error when getting leftout hardpart items in export queue - "
					+ ex.getMessage());
		} finally {
			PristineDBUtil.close(rs);
			PristineDBUtil.close(stmt);
		}
		
		return hardpartItems;
	}

	public void changeEffectiveDate(Connection conn, List<PRItemDTO> leftoutHardPartItems, int countOfIncrementingDays) throws GeneralException {
		PreparedStatement stmt = null;
		SimpleDateFormat format = new SimpleDateFormat("MM/dd/yyyy");
		try {
			StringBuilder sb = new StringBuilder();
			sb.append(" UPDATE PR_QUARTER_REC_ITEM SET REG_EFF_DATE = SYSDATE + ").append(countOfIncrementingDays);
			sb.append(" WHERE PRODUCT_ID = ? AND CAL_TYPE='Q' AND PRODUCT_LEVEL_ID = 1 AND RUN_ID = ? ");

			stmt = conn.prepareStatement(sb.toString());
			logger.debug("changeEffectiveDate() Query- " + sb.toString());
			int itemCountInBatch = 0;

			for (PRItemDTO dto : leftoutHardPartItems) {
				itemCountInBatch++;
				int colIndex = 0;
				stmt.setInt(++colIndex, dto.getItemCode());				
				stmt.setLong(++colIndex, dto.getRunId());
				stmt.addBatch();
			}

			if (itemCountInBatch % Constants.BATCH_UPDATE_COUNT == 0) {
				stmt.executeBatch();
				stmt.clearBatch();
				itemCountInBatch = 0;
			}

			if (itemCountInBatch > 0) {
				stmt.executeBatch();
				stmt.clearBatch();
			}
		} catch (SQLException e) {
			logger.error("changeEffectiveDate() - Error while changing effective date for hard part Items");
			throw new GeneralException("changeEffectiveDate() - Error while changing effective date for hard part Items", e);
		} finally {
			PristineDBUtil.close(stmt);
		}
		
	}

	public List<PRItemDTO> getSalesFloorItemsLeftout(Connection conn, List<PRItemDTO> salesFloorFiltered) {
		List<PRItemDTO> leftoutSalesfloorItems = new ArrayList<PRItemDTO>();
		PreparedStatement stmt = null;
		ResultSet rs = null;
		try {
			StringBuilder sb = new StringBuilder();
			sb.append(" SELECT DISTINCT(EX.RUN_ID) AS RUN_ID, EX.PRICE_TYPE AS PRICE_TYPE, EX.ITEM_CODE AS PRODUCT_ID, RI.IS_EXPORTED,");
			sb.append(" TO_CHAR(RI.REG_EFF_DATE, 'MM/DD/YYYY') AS REG_EFF_DATE ");
			sb.append(" FROM PR_PRICE_EXPORT EX LEFT JOIN PR_QUARTER_REC_ITEM RI ");
			sb.append(" ON RI.RUN_ID = EX.RUN_ID AND RI.PRODUCT_ID = EX.ITEM_CODE AND RI.PRODUCT_LEVEL_ID = 1");
			sb.append(" LEFT JOIN ITEM_LOOKUP IL ON RI.PRODUCT_ID = IL.ITEM_CODE ");
			sb.append(" WHERE IL.USER_ATTR_6 = '").append(Constants.SALE_FLOOR_ITEMS).append("' AND (RI.IS_EXPORTED IS NULL OR RI.IS_EXPORTED = 'N')");
			sb.append(" AND RI.CAL_TYPE='Q' AND RI.PRODUCT_LEVEL_ID = 1 AND RI.REG_EFF_DATE <= SYSDATE + 6 ");
			sb.append(" AND EX.PRICE_TYPE = 'N' ");
			stmt = conn.prepareStatement(sb.toString());
			logger.debug("getSalesFloorItemsLeftout() Query - " + sb.toString());

		rs = stmt.executeQuery();
		while (rs.next()) {			
			PRItemDTO item = new PRItemDTO();
			item.setRunId(rs.getLong("RUN_ID"));
			item.setPriceExportType(rs.getString("PRICE_TYPE"));
			item.setItemCode(rs.getInt("PRODUCT_ID"));
			item.setRegEffDate("REG_EFF_DATE");
			leftoutSalesfloorItems.add(item);
		}
		
		}catch(Exception ex) {
			logger.error("getHardpartItemLeftout() - Error when getting leftout hardpart items in export queue - "
					+ ex.getMessage());
		} finally {
			PristineDBUtil.close(rs);
			PristineDBUtil.close(stmt);
		}
		return leftoutSalesfloorItems;
	}

	public String getRecomName(Connection conn, int itemcode, int productLevelId) {
				
		PreparedStatement stmt = null;
		ResultSet rs = null;
		String recName = "";
		try {
				StringBuilder sb = new StringBuilder();
				sb.append(" select pgcr.name from product_group pg ");
				sb.append(
						" left join product_group_relation_rec pgrr on pgrr.product_id = pg.product_id and pgrr.product_level_id = pg.product_level_id ");
				sb.append(
						" left join product_group pgcr on pgrr.child_product_level_id = pgcr.product_level_id and pgrr.child_product_id = pgcr.product_id ");
				sb.append(
						" where ( pgcr.product_id in (SELECT product_id FROM (SELECT * FROM PRODUCT_GROUP_RELATION_REC PGR ");
				sb.append(" START WITH CHILD_PRODUCT_LEVEL_ID = 1 AND CHILD_PRODUCT_ID in (")
						.append(itemcode).append(") ");
				sb.append(
						" CONNECT BY  PRIOR PRODUCT_ID = CHILD_PRODUCT_ID AND  PRIOR PRODUCT_LEVEL_ID = CHILD_PRODUCT_LEVEL_ID) ");
				sb.append(" WHERE PRODUCT_LEVEL_ID = ").append(productLevelId).append(" )) ");

				stmt = conn.prepareStatement(sb.toString());
				rs = stmt.executeQuery();
				while (rs.next()) {
					recName = rs.getString("name");
				}
			
		} catch (SQLException ex) {
			logger.error("getCategoryAndRecomUnitOfItem() - Error when getting Category/recommendation Unit of item - "
					+ ex.getMessage());
		} finally {
			PristineDBUtil.close(rs);
			PristineDBUtil.close(stmt);
		}
		return recName;
	}

	public void SeparateTestZoneRunIdsAndRegularRunIds(Connection conn, List<Long> runIdList,
			List<Long> testZoneRunIdList) {
		
		try {
			List<Long> NormalRunIds = new ArrayList<>();
			List<String> stringConvertedRunIds = new ArrayList<String>();
			runIdList.forEach(runId -> {
				stringConvertedRunIds.add(String.valueOf(runId));
			});
			StringBuilder sb = new StringBuilder();
			sb.append("SELECT PQRH.RUN_ID, RPZ.ZONE_TYPE FROM PR_QUARTER_REC_HEADER PQRH LEFT JOIN RETAIL_PRICE_ZONE RPZ ON PQRH.LOCATION_ID = RPZ.PRICE_ZONE_ID ");
			sb.append(" WHERE PQRH.RUN_ID IN (");
			sb.append(String.join(",", stringConvertedRunIds));
			sb.append(" )");
			
			logger.debug(sb.toString());
			PreparedStatement stmt = null;
			ResultSet rs = null;
			stmt = conn.prepareStatement(sb.toString());
			rs = stmt.executeQuery();
			
			while(rs.next()) {
				Long RunId = rs.getLong("RUN_ID");
				String ZoneType = rs.getString("ZONE_TYPE");
				ZoneType = ZoneType.trim();
				logger.info("Run Id:"+RunId+" ,Zone Type:"+ZoneType);
				if(ZoneType.equalsIgnoreCase("W") || ZoneType.equalsIgnoreCase("I")) {
					NormalRunIds.add(RunId);
				}
				else if(ZoneType.equalsIgnoreCase("T")) {
					testZoneRunIdList.add(RunId);
				}
				
			}
			runIdList.clear();
			runIdList.addAll(NormalRunIds);
			
		}catch(Exception ex) {
			logger.error("SeparateTestZoneRunIdsAndRegularRunIds() - Error while separating Run Ids for Test Zones and Normal Zones - "+ex.getMessage());
		}
		
	}

	public HashMap<Integer, List<PRItemDTO>> getTestZoneStoreCombinationsDictionary(Connection conn, List<Long> testZoneRunIdList) 
	{
		HashMap<Integer, List<PRItemDTO>> output = new HashMap();

		try {
			List<String> stringConvertedRunIds = new ArrayList<String>();
			testZoneRunIdList.forEach(runId -> {
				stringConvertedRunIds.add(String.valueOf(runId));
			});
			
			StringBuilder sb = new StringBuilder();
			sb.append(" SELECT DISTINCT PQRH.RUN_ID, LGR.CHILD_LOCATION_ID AS STORE_ID, PR.LOCATION_ID AS ZONE_ID, CS.COMP_STR_NO");
			sb.append(" ,RPZ.PRICE_ZONE_ID,RPZ.NAME,RPZ.ZONE_NUM");
			sb.append(" FROM LOCATION_GROUP_RELATION LGR ");
			sb.append(" LEFT JOIN PR_PRICE_TEST_REQUEST PR ");
			sb.append(" ON PR.SL_LEVEL_ID = LGR.LOCATION_LEVEL_ID");
			sb.append(" AND PR.SL_LOCATION_ID=LGR.LOCATION_ID");
			sb.append(" LEFT JOIN LOCATION_GROUP LG");
			sb.append(" ON LG.LOCATION_LEVEL_ID =LGR.LOCATION_LEVEL_ID");
			sb.append(" AND LG.LOCATION_ID      =LGR.LOCATION_LEVEL_ID");
		    sb.append(" LEFT JOIN PR_QUARTER_REC_HEADER PQRH");
			sb.append(" ON PR.LOCATION_ID = PQRH.LOCATION_ID");
			sb.append(" AND PR.LOCATION_LEVEL_ID=PQRH.LOCATION_LEVEL_ID");
			sb.append(" AND PR.PRODUCT_LEVEL_ID =PQRH.PRODUCT_LEVEL_ID");
			sb.append(" AND PR.PRODUCT_ID       =PQRH.PRODUCT_ID ");
			sb.append(" LEFT JOIN COMPETITOR_STORE CS ON ");
			sb.append(" LGR.CHILD_LOCATION_ID = CS.COMP_STR_ID");
			sb.append(" LEFT JOIN RETAIL_PRICE_ZONE RPZ ON CS.PRICE_ZONE_ID = RPZ.PRICE_ZONE_ID ");
			sb.append(" WHERE PQRH.RUN_ID IN ("+String.join(",", stringConvertedRunIds)+") AND PR.ACTIVE = 'Y'");
			logger.debug(sb.toString());
			PreparedStatement stmt = null;
			ResultSet rs = null;
			stmt = conn.prepareStatement(sb.toString());
			rs = stmt.executeQuery();
			
			while(rs.next()) {
				Long RunId = rs.getLong("RUN_ID");
				String StoreNum = rs.getString("COMP_STR_NO");
				String StoreId = rs.getString("STORE_ID");
				int ZoneId = rs.getInt("PRICE_ZONE_ID");
				int TestZoneId = rs.getInt("ZONE_ID");
				String ZoneName = rs.getString("NAME");
				String ZoneNum = rs.getString("ZONE_NUM");
				
				PRItemDTO temp = new PRItemDTO();
				temp.setRunId(RunId);
				temp.setStoreNo(StoreNum);
				temp.setStoreId(StoreId);
				temp.setPriceZoneId(ZoneId);
				temp.setZoneName(ZoneName);
				temp.setPriceZoneNo(ZoneNum);
				if(output.containsKey(TestZoneId))
				{
					output.get(TestZoneId).add(temp);
				}else {
					List<PRItemDTO> tempList = new ArrayList();
					tempList.add(temp);
					output.put(TestZoneId, tempList);
				}
			}
			
			
		}catch(Exception ex) {
			logger.error("getTestZoneStoreCombinationsDictionary() - Error in ");
		}
		
		return output;
	}

	public String getCurrWeekEndDate(Connection conn) {
	String output ="";
		try {
			String TodaysDate ="";
			 DateTimeFormatter dtf = DateTimeFormatter.ofPattern("MM/dd/yyyy");  
			   LocalDateTime now = LocalDateTime.now();  
			   TodaysDate = dtf.format(now); 
				StringBuilder sb = new StringBuilder();
				sb.append("SELECT TO_CHAR(END_DATE,'MM/dd/yyyy') AS END_DATE ");
				sb.append("FROM RETAIL_CALENDAR WHERE START_DATE <= TO_DATE('"+TodaysDate+"','MM/dd/yyyy') ");
				sb.append("AND END_DATE >= TO_DATE('"+TodaysDate+"','MM/dd/yyyy') AND ROW_TYPE = 'W' ");
				logger.debug("Query for getCurrWeekEndDate(): "+sb.toString());
				PreparedStatement stmt = null;
				ResultSet rs = null;
				stmt = conn.prepareStatement(sb.toString());
				rs = stmt.executeQuery();
				while(rs.next()) {
					output = rs.getString("END_DATE");
				}
				
			   
		}
		catch(Exception ex) {
			logger.error("getCurrWeekEndDate() - Error in "+ex.getMessage());
		}
		
		return output;
	}

	public HashMap<String, String> getStoreIdForNum(Connection conn) {
		PreparedStatement stmt = null;
		ResultSet rs = null;
		String chainId = PropertyManager.getProperty("PRESTO_SUBSCRIBER");
		HashMap<String, String> storeIdMap = new HashMap<String, String>();
		try {
			StringBuffer db = new StringBuffer();
			db.append("SELECT COMP_STR_NO, COMP_STR_ID FROM COMPETITOR_STORE WHERE COMP_CHAIN_ID = " + chainId);
			
			stmt = conn.prepareStatement(db.toString());
			logger.debug("Query for getStoreIdForNum() - " + db.toString());
			rs = stmt.executeQuery();
			while (rs.next()) {
				storeIdMap.put((rs.getString("COMP_STR_NO")), rs.getString("COMP_STR_ID"));
			}
		} catch (SQLException ex) {
			logger.error("getStoreIdForNum() - Error when retrieving storeId For store Num - " + ex.getMessage());
		} finally {
			PristineDBUtil.close(rs);
			PristineDBUtil.close(stmt);
		}
		return storeIdMap;
	}
	
}


