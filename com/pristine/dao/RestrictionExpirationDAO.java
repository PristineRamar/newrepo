package com.pristine.dao;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;

import org.apache.log4j.Logger;

import com.pristine.dto.ReDTO;
import com.pristine.dto.offermgmt.PRItemDTO;
import com.pristine.exception.GeneralException;
import com.pristine.util.Constants;
import com.pristine.util.PristineDBUtil;
import com.pristine.util.PropertyManager;


public class RestrictionExpirationDAO {
	
	private static Logger logger = Logger.getLogger("RestrictionExpirationDAO");

	public HashMap<Integer, String> getRetailerItemCodeForItemCode(Connection conn) throws GeneralException {
		HashMap<Integer, String> retItemCodeMap = new HashMap<Integer, String>();
		PreparedStatement stmt = null;
		ResultSet rs = null;
		try {

			StringBuffer sb = new StringBuffer();
			sb.append(" SELECT ITEM_CODE, RETAILER_ITEM_CODE FROM ITEM_LOOKUP ");
			
			logger.debug(sb.toString());
			stmt = conn.prepareStatement(sb.toString());

			rs = stmt.executeQuery();
			while (rs.next()) {				
				retItemCodeMap.put(rs.getInt("ITEM_CODE"), rs.getString("RETAILER_ITEM_CODE"));
			}

		} catch (SQLException e) {
			throw new GeneralException("getRetailerItemCodeForItemCode() - Error while retailer item code for item code", e);
		} finally {
			PristineDBUtil.close(rs);
			PristineDBUtil.close(stmt);
		}
		return retItemCodeMap;
	}

	


	
	public void updateExpiryExportFlagForRegularItemList(Connection conn, List<ReDTO> expiryItemListRegular) throws GeneralException {
		PreparedStatement statement = null;

		try {

			StringBuilder sb = new StringBuilder();
			sb.append(" UPDATE PRICE_CHECK_LIST_ITEMS SET IS_EXPORTED = 'Y' WHERE ITEM_CODE = ? ");
			sb.append(" AND STORE_ID IN (SELECT COMP_STR_ID FROM COMPETITOR_STORE WHERE COMP_STR_NO = ? )");
			sb.append(" AND PRICE_CHECK_LIST_ID = ? ");

			statement = conn.prepareStatement(sb.toString());
			logger.debug("Query for updateExpiryExportFlagForRegularItemList() - " + sb.toString());

			int counter = 0;

			for (ReDTO dto : expiryItemListRegular) {
				counter++;
				int colIndex = 0;
				statement.setInt(++colIndex, dto.getItemCode());
				statement.setString(++colIndex, dto.getStoreNo());
				statement.setString(++colIndex, dto.getPriceCheckListId());
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

	public void updateExpiryExportFlagNotRegular(Connection conn, List<ReDTO> expiryItemListAtStoreList) throws GeneralException {
		PreparedStatement statement = null;

		try {

			StringBuilder sb = new StringBuilder();
			sb.append(" UPDATE PRICE_CHECK_LIST_ITEMS SET IS_EXPORTED = 'Y' WHERE ITEM_CODE = ? ");
			sb.append(" AND PRICE_CHECK_LIST_ID = ? ");

			statement = conn.prepareStatement(sb.toString());
			logger.debug("Query for updateExpiryExportFlagNotRegular() - " + sb.toString());

			int counter = 0;

			for (ReDTO dto : expiryItemListAtStoreList) {
				counter++;
				int colIndex = 0;
				statement.setInt(++colIndex, dto.getItemCode());
				statement.setString(++colIndex, dto.getPriceCheckListId());
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
			logger.error("updateExpiryExportFlagNotRegular() - Error updating export flag for store list expiry store lock items");
			throw new GeneralException(
					"updateExpiryExportFlagNotRegular()- Error updating export flag for store list expiry store lock items", e);
		} finally {
			PristineDBUtil.close(statement);
		}
	}

	public List<ReDTO> getNextWeekExpiredItems(Connection conn) {
		String priceCheckListTypeIdStr = PropertyManager.getProperty("PRICE_CHECK_LIST_TYPE_ID_FOR_STORE_LOCK");
		//int priceCheckListTypeId = Integer.parseInt(priceCheckListTypeIdStr);
		PreparedStatement stmt = null;
		ResultSet rs = null;

		List<ReDTO> expiredItemsOnNextWeek = new ArrayList<ReDTO>();

		try {
			StringBuffer db = new StringBuffer();

			db.append(" select pcli.item_code, il.retailer_item_code, il.item_name, cs.comp_str_no, to_char(pcli.end_date,'mm/dd/yyyy') as end_date, rpz.name, ");
			db.append(" rpz.zone_num, to_char(pcli.start_date,'mm/dd/yyyy') as start_date, pcli.price_check_list_id, ");
			db.append(" pcl.price_check_list_type_id, pcli.comments, pcli.ec_retail, pcl.name as checklist_name,");
			db.append(" ud.first_name || ' ' || ud.last_name as defined_user from price_check_list_items pcli ");
			db.append(" left join price_check_list pcl on pcli.PRICE_CHECK_LIST_ID = pcl.ID ");
			db.append(" left join competitor_store cs on pcli.store_id = cs.comp_str_id ");
			db.append(" left join retail_price_zone rpz on cs.price_zone_id = rpz.price_zone_id ");
			db.append(" left join item_lookup il on pcli.item_code = il.item_code ");
			db.append(" left join user_details ud on ud.user_id = pcli.defined_by ");
			//db.append("  (pcli.IS_EXPORTED = 'N' or pcli.IS_EXPORTED IS NULL)");
			db.append(" where pcl.PRICE_CHECK_LIST_TYPE_ID in (").append(priceCheckListTypeIdStr).append(") ");
			db.append(" and ((pcli.END_DATE > sysdate and pcli.END_DATE <=  sysdate +7) or ");
			db.append(" (pcli.START_DATE > SYSDATE AND pcli.START_DATE <= SYSDATE + 7 )) ");
			db.append(" and pcli.store_id is not null");

			stmt = conn.prepareStatement(db.toString());
			logger.debug("getNextWeekExpiredItems() - query: " + db.toString());
			rs = stmt.executeQuery();
			while (rs.next()) {

				ReDTO dto = new ReDTO();

				dto.setRegularItem(true);
				dto.setItemListComments(rs.getString("comments"));
				dto.setStoreNo(rs.getString("comp_str_no"));
				dto.setPriceCheckListTypeId(rs.getString("price_check_list_type_id"));
				dto.setPriceCheckListId(rs.getString("price_check_list_id"));
				dto.setEndDate(rs.getString("end_date"));
				dto.setStartDate(rs.getString("start_date"));
				dto.setItemCode(rs.getInt("item_code"));
				dto.setItemName(rs.getString("item_name"));
				dto.setRetailerItemCode(rs.getString("retailer_item_code"));
				dto.setZoneName(rs.getString("name"));
				dto.setZoneNo(rs.getString("zone_num"));
				dto.setECRetail(rs.getString("ec_retail"));
				dto.setListName(rs.getString("checklist_name"));
				dto.setUserName(rs.getString("defined_user"));
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
	
	public void getNextWeekExpiryItemsFromStoreList(Connection conn, List<ReDTO> expItemList) {

		String priceCheckListIdStr = PropertyManager.getProperty("PRICE_CHECK_LIST_TYPE_ID_FOR_STORE_LOCK");
		// String[] priceCheckListIdArr = priceCheckListIdStr.split(",");
		// int priceCheckListTypeId = Integer.parseInt(priceCheckListIdStr);
		PreparedStatement stmt = null;
		ResultSet rs = null;

		try {
			StringBuffer sb = new StringBuffer();

			sb.append(
					" select pcli.item_code, il.retailer_item_code, il.item_name, cs.comp_str_no, to_char(pcli.end_date,'mm/dd/yyyy') as end_date, rpz.name, rpz.zone_num, to_char(pcli.start_date,'mm/dd/yyyy') as start_date, "
							+ "pcli.price_check_list_id, pcl.price_check_list_type_id, pcli.comments, pcli.ec_retail, pcl.name as checklist_name, ud.first_name || ' ' || ud.last_name as defined_user from price_check_list_items pcli  ");
			sb.append(" left join price_check_list pcl on pcli.PRICE_CHECK_LIST_ID = pcl.ID ");
			sb.append(" left join location_group_relation lgr on pcl.location_level_id = lgr.location_level_id ");
			sb.append(" and pcl.location_id = lgr.location_id ");
			sb.append(" left join competitor_store cs on lgr.child_location_id = cs.comp_str_id ");
			sb.append(" left join retail_price_zone rpz on cs.price_zone_id = rpz.price_zone_id ");
			sb.append(" left join item_lookup il on pcli.item_code = il.item_code ");
			sb.append(" left join user_details ud on ud.user_id = pcli.defined_by ");			
			sb.append(" where pcl.PRICE_CHECK_LIST_TYPE_ID in (").append(priceCheckListIdStr).append(")");
			// sb.append(" and (pcli.IS_EXPORTED = 'N' or pcli.IS_EXPORTED IS NULL) ");
			sb.append(" and lgr.location_level_id = ").append(Constants.STORE_LIST_LEVEL_ID);
			sb.append(" and ((pcli.END_DATE > sysdate and pcli.END_DATE <=  sysdate +7) OR ");
			sb.append(" (pcli.START_DATE > SYSDATE AND pcli.START_DATE <= SYSDATE + 7 )) ");

			stmt = conn.prepareStatement(sb.toString());
			logger.debug("getNextWeekExpiryItemsFromStoreList()- query: " + sb.toString());
			rs = stmt.executeQuery();
			while (rs.next()) {
				ReDTO dto = new ReDTO();
				dto.setRegularItem(false);
				dto.setItemListComments(rs.getString("comments"));
				dto.setStoreNo(rs.getString("comp_str_no"));
				dto.setPriceCheckListTypeId(rs.getString("price_check_list_type_id"));
				dto.setPriceCheckListId(rs.getString("price_check_list_id"));
				dto.setEndDate(rs.getString("end_date"));
				dto.setStartDate(rs.getString("start_date"));
				dto.setItemCode(rs.getInt("item_code"));
				dto.setItemName(rs.getString("item_name"));
				dto.setRetailerItemCode(rs.getString("retailer_item_code"));
				dto.setZoneName(rs.getString("name"));
				dto.setZoneNo(rs.getString("zone_num"));
				dto.setECRetail(rs.getString("ec_retail"));
				dto.setListName(rs.getString("checklist_name"));
				dto.setUserName(rs.getString("defined_user"));				

				expItemList.add(dto);

			}
		} catch (SQLException ex) {
			logger.error("getNextWeekExpiryItemsFromStoreList() - Error when getting next week expiry date item - "
					+ ex.getMessage());
		} finally {
			PristineDBUtil.close(rs);
			PristineDBUtil.close(stmt);
		}

	}
	
	public void getNextWeekExpiryItemsForMinMaxAndLockedRetail(Connection conn, List<ReDTO> expItemList) {
		String priceCheckListTypeIdStrMinMaxLock = PropertyManager.getProperty("PRICE_CHECK_LIST_TYPE_ID_FOR_MINMAX_LOCKED_RET");
		String priceCheckListTypeIdStrClear = PropertyManager.getProperty("PRICE_CHECK_LIST_TYPE_ID_FOR_CLEARANCE");
		
		String priceCheckListTypeIdStr = priceCheckListTypeIdStrMinMaxLock + "," + priceCheckListTypeIdStrClear;
		//int priceCheckListTypeId = Integer.parseInt(priceCheckListTypeIdStr);
		PreparedStatement stmt = null;
		ResultSet rs = null;

		try {
			StringBuffer db = new StringBuffer();

			db.append(" select pcli.item_code, il.retailer_item_code, il.item_name, cs.comp_str_no, to_char(pcli.end_date,'mm/dd/yyyy') as end_date, rpz.name, ");
			db.append(" rpz.zone_num, to_char(pcli.start_date,'mm/dd/yyyy') as start_date, pcli.price_check_list_id, ");
			db.append(" pcl.price_check_list_type_id, pcli.comments, pcli.ec_retail, pcl.name as checklist_name,");
			db.append(" ud.first_name || ' ' || ud.last_name as defined_user from price_check_list_items pcli  ");
			db.append(" left join price_check_list pcl on pcli.PRICE_CHECK_LIST_ID = pcl.ID ");			
			db.append(" left join item_lookup il on pcli.item_code = il.item_code ");
			db.append(" left join competitor_store cs on pcli.store_id = cs.comp_str_id ");
			db.append(" left join retail_price_zone rpz on cs.price_zone_id = rpz.price_zone_id ");
			db.append(" left join user_details ud on ud.user_id = pcli.defined_by ");			
			//db.append(" where (pcli.IS_EXPORTED = 'N' or pcli.IS_EXPORTED IS NULL)");
			db.append(" where pcl.PRICE_CHECK_LIST_TYPE_ID in (").append(priceCheckListTypeIdStr).append(") ");
			db.append(" and ((pcli.END_DATE > sysdate and pcli.END_DATE <=  sysdate +7) or ");
			db.append(" (pcli.START_DATE > SYSDATE AND pcli.START_DATE <= SYSDATE + 7 )) ");			

			stmt = conn.prepareStatement(db.toString());
			logger.debug("getNextWeekExpiryItemsForMinMaxAndLockedRetail() - query: " + db.toString());
			rs = stmt.executeQuery();
			while (rs.next()) {

				ReDTO dto = new ReDTO();
				dto.setRegularItem(false);
				dto.setStoreNo(rs.getString("comp_str_no"));
				dto.setItemListComments(rs.getString("comments"));
				dto.setPriceCheckListTypeId(rs.getString("price_check_list_type_id"));
				dto.setPriceCheckListId(rs.getString("price_check_list_id"));
				dto.setEndDate(rs.getString("end_date"));
				dto.setStartDate(rs.getString("start_date"));
				dto.setItemCode(rs.getInt("item_code"));
				dto.setItemName(rs.getString("item_name"));
				dto.setRetailerItemCode(rs.getString("retailer_item_code"));
				dto.setZoneName(rs.getString("name"));
				dto.setZoneNo(rs.getString("zone_num"));
				dto.setECRetail(rs.getString("ec_retail"));
				dto.setListName(rs.getString("checklist_name"));
				dto.setUserName(rs.getString("defined_user"));				
				
				expItemList.add(dto);
				
			}
		} catch (SQLException ex) {
			logger.error(
					"getNextWeekExpiryItemsForMinMaxAndLockedRetail() - Error when getting next week expiry date item - " + ex.getMessage());
		} finally {
			PristineDBUtil.close(rs);
			PristineDBUtil.close(stmt);
		}
				
	}

	/*
	 * public List<String> getEmailsOfUsers(Connection conn) {
	 * 
	 * List<String> emailsOfUsers = new ArrayList<String>(); PreparedStatement stmt
	 * = null; ResultSet rs = null; try {
	 * 
	 * StringBuffer sb = new StringBuffer();
	 * sb.append(" SELECT E_MAIL FROM USER_DETAILS WHERE USER_ID IN (").append();
	 * 
	 * 
	 * logger.debug(sb.toString()); stmt = conn.prepareStatement(sb.toString());
	 * 
	 * rs = stmt.executeQuery(); while (rs.next()) {
	 * emailsOfUsers.add(rs.getString("E_MAIL")); }
	 * 
	 * } catch (SQLException e) { throw new GeneralException(
	 * "getRetailerItemCodeForItemCode() - Error while retailer item code for item code"
	 * , e); } finally { PristineDBUtil.close(rs); PristineDBUtil.close(stmt); }
	 * return emailsOfUsers; }
	 */
	 

}
