package com.pristine.dao.offermgmt;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;

import org.apache.log4j.Logger;

import com.pristine.dao.IDAO;
import com.pristine.dto.offermgmt.PRItemDTO;
//import com.pristine.dto.offermgmt.PRSubstituteGroup;
import com.pristine.dto.offermgmt.PRSubstituteItem;
import com.pristine.exception.GeneralException;
import com.pristine.service.offermgmt.ItemKey;
import com.pristine.util.Constants;
import com.pristine.util.PristineDBUtil;
import com.pristine.util.offermgmt.PRCommonUtil;
import com.pristine.util.offermgmt.PRConstants;

public class SubstituteDAO implements IDAO {
	private static Logger logger = Logger.getLogger("SubstituteDAO");

//	private static final String GET_SUBSTITUTE_ITEMS = "SELECT A_ITEM_TYPE, A_ITEM_ID, B_ITEM_TYPE, B_ITEM_ID, "
//			+ " OVERALL_STRENGTH, GROUP_ITEM_TYPE, GROUP_ITEM_ID FROM PRD_SUBS_PILEADPAIRS_VIEW WHERE  "
//			+ " LOCATION_LEVEL_ID = ? AND LOCATION_ID = ? AND OVERALL_STRENGTH >=2 AND (%ITEM_FILTER%)";

	private static final String GET_SUBSTITUTE_ITEMS_NEW = " SELECT BASE_PRODUCT_LEVEL_ID, BASE_PRODUCT_ID, SUBS_PRODUCT_LEVEL_ID, "
			+ " SUBS_PRODUCT_ID, IMPACT_FACTOR FROM SUBS_IMPACT WHERE RUN_ID IN (SELECT RUN_ID FROM (SELECT RUN_ID, RANK() OVER "
			+ " (PARTITION BY LOCATION_LEVEL_ID, LOCATION_ID, PRODUCT_LEVEL_ID, PRODUCT_ID  ORDER BY UPDATED DESC) AS RANK "
			+ " FROM SUBS_IMPACT_RUN_HEADER WHERE LOCATION_LEVEL_ID = ? AND LOCATION_ID = ? AND PRODUCT_LEVEL_ID = ? AND PRODUCT_ID = ? "
			+ " AND ACTIVE='Y') WHERE RANK=1) AND (%ITEM_FILTER%)";

//	public HashMap<Integer, PRSubstituteGroup> getSubstituteItems(Connection conn, int locationLevelId, int locationId, List<Integer> retLirIds,
//			List<Integer> nonLigItems) throws GeneralException {
//		PreparedStatement stmt = null;
//		ResultSet rs = null;
//		int limitcount = 0;
//		int ligLimitcount = 0;
//		int commitCount = 1000;
//		List<Integer> nonLigItemList = new ArrayList<Integer>();
//		List<Integer> ligItemList = new ArrayList<Integer>();
//		HashMap<Integer, PRSubstituteGroup> substituteGroups = new HashMap<Integer, PRSubstituteGroup>();
//		int groupId = 1;
//		PRSubstituteGroup substituteGroup;
//		int totalBatches = 0;
//		try {
//			String filterByRetLirIdAndItem = "";
//			String query = new String(GET_SUBSTITUTE_ITEMS);
//
//			// if (retLirIds.size() > 0) {
//			// filterByRetLirIdAndItem = filterByRetLirIdAndItem + "(A_ITEM_TYPE
//			// = " + Constants.PRODUCT_LEVEL_ID_LIG
//			// + " AND A_ITEM_ID IN (" +
//			// PRCommonUtil.getCommaSeperatedStringFromIntArray(retLirIds) +
//			// "))";
//			//
//			// filterByRetLirIdAndItem = filterByRetLirIdAndItem + " OR
//			// (B_ITEM_TYPE = "
//			// + Constants.PRODUCT_LEVEL_ID_LIG + " AND B_ITEM_ID IN ("
//			// + PRCommonUtil.getCommaSeperatedStringFromIntArray(retLirIds) +
//			// "))";
//			// }
//
//			for (Integer lirId : retLirIds) {
//				ligItemList.add(lirId);
//				ligLimitcount++;
//				if ((ligLimitcount > 0) && (ligLimitcount % commitCount == 0)) {
//					if (totalBatches > 0)
//						filterByRetLirIdAndItem = filterByRetLirIdAndItem + " OR ";
//
//					filterByRetLirIdAndItem = filterByRetLirIdAndItem + "(A_ITEM_TYPE = " + Constants.PRODUCT_LEVEL_ID_LIG + " AND A_ITEM_ID IN ("
//							+ PRCommonUtil.getCommaSeperatedStringFromIntArray(ligItemList) + "))";
//
//					filterByRetLirIdAndItem = filterByRetLirIdAndItem + " OR (B_ITEM_TYPE = " + Constants.PRODUCT_LEVEL_ID_LIG + " AND B_ITEM_ID IN ("
//							+ PRCommonUtil.getCommaSeperatedStringFromIntArray(ligItemList) + "))";
//
//					ligItemList.clear();
//					totalBatches = totalBatches + 1;
//				}
//			}
//			if (ligItemList.size() > 0) {
//				if (totalBatches > 0)
//					filterByRetLirIdAndItem = filterByRetLirIdAndItem + " OR ";
//
//				filterByRetLirIdAndItem = filterByRetLirIdAndItem + "(A_ITEM_TYPE = " + Constants.PRODUCT_LEVEL_ID_LIG + " AND A_ITEM_ID IN ("
//						+ PRCommonUtil.getCommaSeperatedStringFromIntArray(ligItemList) + "))";
//
//				filterByRetLirIdAndItem = filterByRetLirIdAndItem + " OR (B_ITEM_TYPE = " + Constants.PRODUCT_LEVEL_ID_LIG + " AND B_ITEM_ID IN ("
//						+ PRCommonUtil.getCommaSeperatedStringFromIntArray(ligItemList) + "))";
//			}
//
//			totalBatches = 0;
//			for (Integer itemCode : nonLigItems) {
//				nonLigItemList.add(itemCode);
//				limitcount++;
//				if ((limitcount > 0) && (limitcount % commitCount == 0)) {
//					// If there are no lig and items less than 1000
//					if ((retLirIds.size() == 0 && totalBatches > 0) || retLirIds.size() > 0)
//						filterByRetLirIdAndItem = filterByRetLirIdAndItem + " OR ";
//
//					filterByRetLirIdAndItem = filterByRetLirIdAndItem + " (A_ITEM_TYPE = " + Constants.ITEMLEVELID + " AND A_ITEM_ID IN ("
//							+ PRCommonUtil.getCommaSeperatedStringFromIntArray(nonLigItemList) + "))";
//
//					filterByRetLirIdAndItem = filterByRetLirIdAndItem + " OR (B_ITEM_TYPE = " + Constants.ITEMLEVELID + " AND B_ITEM_ID IN ("
//							+ PRCommonUtil.getCommaSeperatedStringFromIntArray(nonLigItemList) + "))";
//
//					nonLigItemList.clear();
//					totalBatches = totalBatches + 1;
//				}
//			}
//			if (nonLigItemList.size() > 0) {
//				// If there are no lig and items less than 1000
//				if (retLirIds.size() == 0 && totalBatches > 0 || retLirIds.size() > 0)
//					filterByRetLirIdAndItem = filterByRetLirIdAndItem + " OR ";
//
//				filterByRetLirIdAndItem = filterByRetLirIdAndItem + " (A_ITEM_TYPE = " + Constants.ITEMLEVELID + " AND A_ITEM_ID IN ("
//						+ PRCommonUtil.getCommaSeperatedStringFromIntArray(nonLigItemList) + "))";
//
//				filterByRetLirIdAndItem = filterByRetLirIdAndItem + " OR (B_ITEM_TYPE = " + Constants.ITEMLEVELID + " AND B_ITEM_ID IN ("
//						+ PRCommonUtil.getCommaSeperatedStringFromIntArray(nonLigItemList) + "))";
//			}
//
//			query = query.replaceAll("%ITEM_FILTER%", filterByRetLirIdAndItem);
//			stmt = conn.prepareStatement(query);
//			logger.debug("Get Substitute Query:" + query);
//			stmt.setLong(1, locationLevelId);
//			stmt.setLong(2, locationId);
//			rs = stmt.executeQuery();
//			while (rs.next()) {
//				if (rs.getObject("A_ITEM_TYPE") != null && rs.getObject("A_ITEM_ID") != null && rs.getObject("B_ITEM_TYPE") != null
//						&& rs.getObject("B_ITEM_ID") != null && rs.getObject("GROUP_ITEM_ID") != null) {
//					PRSubstituteItem substituteItem = new PRSubstituteItem();
//
//					if (substituteGroups.get(rs.getInt("GROUP_ITEM_ID")) != null) {
//						substituteGroup = substituteGroups.get(rs.getInt("GROUP_ITEM_ID"));
//					} else {
//						substituteGroup = new PRSubstituteGroup();
//						substituteGroup.setGroupId(groupId++);
//						substituteGroup.setGroupItemId(rs.getInt("GROUP_ITEM_ID"));
//						substituteGroup.setGroupItemType(rs.getInt("GROUP_ITEM_TYPE"));
//					}
//
//					substituteItem.setItemAType(rs.getInt("A_ITEM_TYPE"));
//					substituteItem.setItemAId(rs.getInt("A_ITEM_ID"));
//					substituteItem.setItemBType(rs.getInt("B_ITEM_TYPE"));
//					substituteItem.setItemBId(rs.getInt("B_ITEM_ID"));
//					substituteGroup.getSubtituteItems().add(substituteItem);
//
//					substituteGroups.put(rs.getInt("GROUP_ITEM_ID"), substituteGroup);
//				}
//			}
//		} catch (Exception exception) {
//			logger.error("Exception in getSubstituteItems()");
//			throw new GeneralException("Error in getSubstituteItems() - " + exception);
//		} finally {
//			PristineDBUtil.close(rs);
//			PristineDBUtil.close(stmt);
//		}
//		return substituteGroups;
//	}

	//public HashMap<ItemKey, List<PRSubstituteItem>> getSubstituteItemsNew(Connection conn, int locationLevelId, int locationId, int productLevelId,
			//int productId, HashMap<Integer, List<PRItemDTO>> retLirMap, HashMap<ItemKey, PRItemDTO> itemDataMap) throws GeneralException {
	public List<PRSubstituteItem> getSubstituteItemsNew(Connection conn, int locationLevelId, int locationId, int productLevelId, int productId,
			HashMap<Integer, List<PRItemDTO>> retLirMap, HashMap<ItemKey, PRItemDTO> itemDataMap) throws GeneralException {
		PreparedStatement stmt = null;
		ResultSet rs = null;
		int limitcount = 0;
		int ligLimitcount = 0;
		int commitCount = 1000;
		List<Integer> nonLigItemList = new ArrayList<Integer>();
		List<Integer> ligItemList = new ArrayList<Integer>();
		// HashMap<ItemKey, List<PRSubstituteItem>> substituteItems = new HashMap<ItemKey, List<PRSubstituteItem>>();
		List<PRSubstituteItem> substituteItems = new ArrayList<PRSubstituteItem>();
		int totalBatches = 0;
		try {
			String filterByRetLirIdAndItem = "";
			List<Integer> retLirIds = new ArrayList<Integer>();
			List<Integer> nonLigItems = new ArrayList<Integer>();

			// All ret lir ids
			retLirIds.addAll(retLirMap.keySet());

			// All non lig's
			for (PRItemDTO itemDTO : itemDataMap.values()) {
				if (!itemDTO.isLir() && itemDTO.getRetLirId() < 1)
					nonLigItems.add(itemDTO.getItemCode());
			}

			String query = new String(GET_SUBSTITUTE_ITEMS_NEW);

			for (Integer lirId : retLirIds) {
				ligItemList.add(lirId);
				ligLimitcount++;
				if ((ligLimitcount > 0) && (ligLimitcount % commitCount == 0)) {
					if (totalBatches > 0)
						filterByRetLirIdAndItem = filterByRetLirIdAndItem + " OR ";

					filterByRetLirIdAndItem = filterByRetLirIdAndItem + "(BASE_PRODUCT_LEVEL_ID = " + Constants.PRODUCT_LEVEL_ID_LIG
							+ " AND BASE_PRODUCT_ID IN (" + PRCommonUtil.getCommaSeperatedStringFromIntArray(ligItemList) + "))";

					ligItemList.clear();
					totalBatches = totalBatches + 1;
				}
			}
			if (ligItemList.size() > 0) {
				if (totalBatches > 0)
					filterByRetLirIdAndItem = filterByRetLirIdAndItem + " OR ";

				filterByRetLirIdAndItem = filterByRetLirIdAndItem + "(BASE_PRODUCT_LEVEL_ID = " + Constants.PRODUCT_LEVEL_ID_LIG
						+ " AND BASE_PRODUCT_ID IN (" + PRCommonUtil.getCommaSeperatedStringFromIntArray(ligItemList) + "))";

			}

			totalBatches = 0;
			for (Integer itemCode : nonLigItems) {
				nonLigItemList.add(itemCode);
				limitcount++;
				if ((limitcount > 0) && (limitcount % commitCount == 0)) {
					// If there are no lig and items less than 1000
					if ((retLirIds.size() == 0 && totalBatches > 0) || retLirIds.size() > 0)
						filterByRetLirIdAndItem = filterByRetLirIdAndItem + " OR ";

					filterByRetLirIdAndItem = filterByRetLirIdAndItem + " (BASE_PRODUCT_LEVEL_ID = " + Constants.ITEMLEVELID
							+ " AND BASE_PRODUCT_ID IN (" + PRCommonUtil.getCommaSeperatedStringFromIntArray(nonLigItemList) + "))";

					nonLigItemList.clear();
					totalBatches = totalBatches + 1;
				}
			}
			if (nonLigItemList.size() > 0) {
				// If there are no lig and items less than 1000
				if (retLirIds.size() == 0 && totalBatches > 0 || retLirIds.size() > 0)
					filterByRetLirIdAndItem = filterByRetLirIdAndItem + " OR ";

				filterByRetLirIdAndItem = filterByRetLirIdAndItem + " (BASE_PRODUCT_LEVEL_ID = " + Constants.ITEMLEVELID + " AND BASE_PRODUCT_ID IN ("
						+ PRCommonUtil.getCommaSeperatedStringFromIntArray(nonLigItemList) + "))";
			}

			query = query.replaceAll("%ITEM_FILTER%", filterByRetLirIdAndItem);
			stmt = conn.prepareStatement(query);
			logger.debug("Get Substitute Query:" + query);
			stmt.setLong(1, locationLevelId);
			stmt.setLong(2, locationId);
			stmt.setInt(3, productLevelId);
			stmt.setInt(4, productId);
			rs = stmt.executeQuery();
			while (rs.next()) {
				if (rs.getObject("BASE_PRODUCT_LEVEL_ID") != null && rs.getObject("BASE_PRODUCT_ID") != null
						&& rs.getObject("SUBS_PRODUCT_LEVEL_ID") != null && rs.getObject("SUBS_PRODUCT_ID") != null) {
					PRSubstituteItem substituteItem = new PRSubstituteItem();
					// List<PRSubstituteItem> subsItemsTemp = new ArrayList<PRSubstituteItem>();

					ItemKey baseItemKey = new ItemKey(rs.getInt("BASE_PRODUCT_ID"),
							(rs.getInt("BASE_PRODUCT_LEVEL_ID") == Constants.PRODUCT_LEVEL_ID_LIG ? PRConstants.LIG_ITEM_INDICATOR
									: PRConstants.NON_LIG_ITEM_INDICATOR));

					ItemKey subsItemKey = new ItemKey(rs.getInt("SUBS_PRODUCT_ID"),
							(rs.getInt("SUBS_PRODUCT_LEVEL_ID") == Constants.PRODUCT_LEVEL_ID_LIG ? PRConstants.LIG_ITEM_INDICATOR
									: PRConstants.NON_LIG_ITEM_INDICATOR));

					// if (substituteItems.get(baseItemKey) != null) {
					// subsItemsTemp = substituteItems.get(baseItemKey);
					// }

					substituteItem.setBaseProductKey(baseItemKey);
					substituteItem.setSubsProductKey(subsItemKey);
					substituteItem.setImpactFactor(rs.getDouble("IMPACT_FACTOR"));
					substituteItems.add(substituteItem);
					// subsItemsTemp.add(substituteItem);

					// substituteItems.put(baseItemKey, subsItemsTemp);
				}
			}
		} catch (Exception exception) {
			logger.error("Exception in getSubstituteItemsNew()");
			throw new GeneralException("Error in getSubstituteItemsNew() - " + exception);
		} finally {
			PristineDBUtil.close(rs);
			PristineDBUtil.close(stmt);
		}
		// return substituteItems;
		return substituteItems;
	}
}
