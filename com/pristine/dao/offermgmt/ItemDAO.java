package com.pristine.dao.offermgmt;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;


import javax.sql.rowset.CachedRowSet;

import org.apache.log4j.Logger;

import com.pristine.dao.IDAO;
import com.pristine.dao.LocationGroupDAO;
import com.pristine.dao.RetailPriceZoneDAO;
import com.pristine.dto.ItemDTO;
import com.pristine.dto.PriceZoneDTO;
import com.pristine.dto.offermgmt.PRItemDTO;
import com.pristine.dto.offermgmt.PRStrategyDTO;
import com.pristine.exception.GeneralException;
import com.pristine.exception.OfferManagementException;
import com.pristine.service.ProductService;
import com.pristine.service.offermgmt.RecommendationErrorCode;
import com.pristine.util.Constants;
import com.pristine.util.PristineDBUtil;
import com.pristine.util.PropertyManager;
import com.pristine.util.offermgmt.PRCommonUtil;
import com.pristine.util.offermgmt.PRConstants;

@SuppressWarnings("unused")
public class ItemDAO implements IDAO {

	/*private static final String GET_AUTHORIZED_ITEMS_ZONE = 
			"SELECT * FROM (SELECT DISTINCT(SIM.ITEM_CODE), IL.RET_LIR_ID, LIG.RET_LIR_ITEM_CODE, IL.ITEM_SIZE, "
			+ "IL.UPC, IL.LIR_IND, IL.UOM_ID, UL.NAME AS UOM_NAME FROM STORE_ITEM_MAP SIM "
			+ "LEFT JOIN ITEM_LOOKUP IL ON SIM.ITEM_CODE = IL.ITEM_CODE "
			+ "LEFT JOIN RETAILER_LIKE_ITEM_GROUP LIG ON IL.RET_LIR_ID = LIG.RET_LIR_ID "
			+ "LEFT JOIN UOM_LOOKUP UL ON IL.UOM_ID = UL.ID "
			+ "WHERE LEVEL_TYPE_ID = 2 AND LEVEL_ID IN "
			+ "(SELECT COMP_STR_ID FROM COMPETITOR_STORE WHERE PRICE_ZONE_ID = ?) "
			+ "AND SIM.ITEM_CODE IN ("
			+ "SELECT CHILD_PRODUCT_ID FROM (SELECT CHILD_PRODUCT_ID,CHILD_PRODUCT_LEVEL_ID FROM PRODUCT_GROUP_RELATION PGR  "
			+ "%PRODUCT_CONDITION1%) WHERE CHILD_PRODUCT_LEVEL_ID = 1) "
			+ "AND IL.ACTIVE_INDICATOR = 'Y' " 
			+ ") STORE_ITEM_MAP LEFT JOIN %PRODUCT_CONDITION2% ";*/

	/*private static final String GET_AUTHORIZED_ITEMS_STORE = 
			"SELECT SIM.LEVEL_ID, SIM.ITEM_CODE, SIM.VENDOR_ID, SIM.DIST_FLAG " +
					" FROM STORE_ITEM_MAP SIM " + 		
					" LEFT JOIN ITEM_LOOKUP IL ON SIM.ITEM_CODE = IL.ITEM_CODE " +
					" WHERE SIM.LEVEL_TYPE_ID = 2 %DSD_FLAG% " +
					" AND SIM.LEVEL_ID IN (SELECT COMP_STR_ID FROM COMPETITOR_STORE WHERE PRICE_ZONE_ID = ?) " +  
					" AND IL.ACTIVE_INDICATOR = 'Y' " + 
					" AND SIM.ITEM_CODE IN (SELECT CHILD_PRODUCT_ID FROM (SELECT CHILD_PRODUCT_ID, CHILD_PRODUCT_LEVEL_ID " +
					" FROM PRODUCT_GROUP_RELATION PGR " +  
					" %QUERY_CONDITION%) WHERE CHILD_PRODUCT_LEVEL_ID = 1)";*/
	
	/*private static final String GET_AUTHORIZED_ITEMS_ZONE_AND_STORE = 
			"SELECT * FROM (SELECT SIM.LEVEL_ID, SIM.ITEM_CODE, SIM.VENDOR_ID, SIM.DIST_FLAG, IL.RET_LIR_ID, "
			+ " LIG.RET_LIR_ITEM_CODE, IL.ITEM_SIZE, "
			+ "IL.UPC, IL.LIR_IND, IL.UOM_ID, UL.NAME AS UOM_NAME FROM STORE_ITEM_MAP SIM "
			+ "LEFT JOIN ITEM_LOOKUP IL ON SIM.ITEM_CODE = IL.ITEM_CODE "
			+ "LEFT JOIN RETAILER_LIKE_ITEM_GROUP LIG ON IL.RET_LIR_ID = LIG.RET_LIR_ID "
			+ "LEFT JOIN UOM_LOOKUP UL ON IL.UOM_ID = UL.ID "
			+ "WHERE LEVEL_TYPE_ID = 2 AND LEVEL_ID IN "
			+ "(SELECT COMP_STR_ID FROM COMPETITOR_STORE WHERE PRICE_ZONE_ID = ?) "
			+ "AND SIM.ITEM_CODE IN ("
			+ "SELECT CHILD_PRODUCT_ID FROM (SELECT CHILD_PRODUCT_ID,CHILD_PRODUCT_LEVEL_ID FROM PRODUCT_GROUP_RELATION PGR  "
			+ "%PRODUCT_CONDITION1%) WHERE CHILD_PRODUCT_LEVEL_ID = 1) "
			+ "AND IL.ACTIVE_INDICATOR = 'Y' "
			+ ") STORE_ITEM_MAP LEFT JOIN %PRODUCT_CONDITION2% ";*/
	
	/*private static final String GET_AUTHORIZED_ITEMS_ZONE_AND_STORE_OPTIMIZED = 
			"SELECT SIM.*, IL.RET_LIR_ID, "
			+ " LIG.RET_LIR_ITEM_CODE, IL.ITEM_SIZE, "
			+ "IL.UPC, IL.LIR_IND, IL.BRAND_ID, IL.UOM_ID, UL.NAME AS UOM_NAME, IL.PREPRICED_IND, CS.PRICE_ZONE_ID FROM (SELECT * FROM "
			+ "(SELECT ITEM_CODE, LEVEL_ID, VENDOR_ID, DIST_FLAG, COST_INDICATOR FROM STORE_ITEM_MAP SIM "			
			+ "WHERE SIM.LEVEL_TYPE_ID = 2 AND SIM.LEVEL_ID IN "
			+ "(" 
			+ "%STORE_ID%"
			//"SELECT COMP_STR_ID FROM COMPETITOR_STORE WHERE PRICE_ZONE_ID = ?" +
			+ ") "
			+ "AND SIM.ITEM_CODE IN ("
			+ "SELECT CHILD_PRODUCT_ID FROM (SELECT CHILD_PRODUCT_ID,CHILD_PRODUCT_LEVEL_ID FROM PRODUCT_GROUP_RELATION PGR  "
			+ "%PRODUCT_CONDITION1%) WHERE CHILD_PRODUCT_LEVEL_ID = 1) "
			//+ "AND SIM.COST_INDICATOR = 'Y' "
			+ "AND SIM.IS_AUTHORIZED = 'Y' "
			+ ") STORE_ITEM_MAP LEFT JOIN %PRODUCT_CONDITION2% "
			+ ") SIM LEFT JOIN ITEM_LOOKUP IL ON SIM.ITEM_CODE = IL.ITEM_CODE " 
			+ " LEFT JOIN RETAILER_LIKE_ITEM_GROUP LIG ON IL.RET_LIR_ID = LIG.RET_LIR_ID "
			+ " LEFT JOIN UOM_LOOKUP UL ON IL.UOM_ID = UL.ID "
			+ " LEFT JOIN COMPETITOR_STORE CS "
			+ " ON SIM.LEVEL_ID = CS.COMP_STR_ID "
			+ " WHERE IL.ACTIVE_INDICATOR = 'Y' "; */
	
	private static final String GET_AUTHORIZED_ITEMS_ZONE_AND_STORE_OPTIMIZED = " SELECT SIM.*, IL.RET_LIR_ID, "
			+ " LIG.RET_LIR_ITEM_CODE, IL.ITEM_SIZE, "
			+ " LIG.RET_LIR_NAME, IL.ITEM_NAME, "
			+ " IL.UPC, IL.LIR_IND, IL.BRAND_ID, BL.BRAND_NAME, IL.RETAILER_ITEM_CODE, IL.UOM_ID, UL.NAME AS UOM_NAME, IL.PREPRICED_IND, "
			+ " IL.SHIPPER_FLAG, IL.USER_ATTR_3, IL.USER_ATTR_4, IL.USER_ATTR_5, IL.USER_ATTR_6, IL.USER_ATTR_7, "
			+ " IL.USER_ATTR_8, IL.USER_ATTR_9, IL.USER_ATTR_10, IL.USER_ATTR_11, "
			+ " IL.USER_ATTR_12, IL.USER_ATTR_13, IL.USER_ATTR_14, IL.USER_ATTR_15, IL.ITEM_SETUP_DATE, "
			+ " CS.PRICE_ZONE_ID, RPZ.ZONE_NUM, IL.ACTIVE_INDICATOR FROM (SELECT * FROM " + " (" + " %AUTHORIZED_ITEM_QUERY% "
			+ " ) STORE_ITEM_MAP LEFT JOIN %PRODUCT_CONDITION2% "
			+ " ) SIM LEFT JOIN ITEM_LOOKUP IL ON SIM.ITEM_CODE = IL.ITEM_CODE "
			+ " LEFT JOIN RETAILER_LIKE_ITEM_GROUP LIG ON IL.RET_LIR_ID = LIG.RET_LIR_ID "
			+ " LEFT JOIN UOM_LOOKUP UL ON IL.UOM_ID = UL.ID "
			+ " LEFT JOIN COMPETITOR_STORE CS ON SIM.LEVEL_ID = CS.COMP_STR_ID "
			+ " LEFT JOIN RETAIL_PRICE_ZONE RPZ ON CS.PRICE_ZONE_ID = RPZ.PRICE_ZONE_ID "
			+ " LEFT JOIN BRAND_LOOKUP BL ON IL.BRAND_ID = BL.BRAND_ID "
			+ " WHERE IL.ACTIVE_INDICATOR = 'Y' AND (IL.RECOMMEND_FLAG <> 'N' OR IL.RECOMMEND_FLAG IS NULL) ";
			//+ " AND IL.ITEM_CODE IN (6913,	6914,	13841,	13892,	13893,	13894,	13895,	13896,	13897,	13898,	13899,	14334,	15505,	15508,	15509,	17210,	17225,	17229,	17240,	17242,	17245,	17247,	17252,	17253,	17254,	20442,	20443,	24625,	24684,	24691,	24692,	24695,	24696,	24697,	24698,	24746,	24749,	24783,	24837,	24886,	24887,	24889,	24905,	25170,	25172,	25235,	25267,	25341,	25342,	25344,	25450,	28166,	28167,	28168,	28169,	28170,	28175,	28189,	28242,	28245,	28247,	28411,	28412,	28413,	28414,	28415,	28416,	28442,	28524,	28580,	28596,	28597,	28598,	28599,	28600,	28611,	28612,	28616,	28617,	28618,	28619,	28620,	28622,	28624,	28711,	28716,	28717,	28718,	28803,	28804,	28805,	28882,	28905,	28928,	28929,	28949,	28950,	28951,	28952,	28975)";
	
	private static final String GET_LIG_MEMBERS = " SELECT ITEM_CODE FROM ITEM_LOOKUP WHERE RET_LIR_ID = ? AND ACTIVE_INDICATOR = 'Y' AND LIR_IND = 'N' ";
	
	private static final String GET_AUTHORIZED_ITEMS = " SELECT SIM.*, IL.RET_LIR_ID, "
			+ " LIG.RET_LIR_ITEM_CODE, IL.ITEM_SIZE, "
			+ " LIG.RET_LIR_NAME, IL.ITEM_NAME, "
			+ " IL.UPC, IL.LIR_IND, IL.BRAND_ID, BL.BRAND_NAME, IL.RETAILER_ITEM_CODE, IL.UOM_ID, UL.NAME AS UOM_NAME, IL.PREPRICED_IND, "
			+ " IL.SHIPPER_FLAG "
			+ " FROM (SELECT * FROM " + " (" + " %AUTHORIZED_ITEM_QUERY% "
			+ " ) STORE_ITEM_MAP LEFT JOIN %PRODUCT_CONDITION2% "
			+ " ) SIM LEFT JOIN ITEM_LOOKUP IL ON SIM.ITEM_CODE = IL.ITEM_CODE "
			+ " LEFT JOIN RETAILER_LIKE_ITEM_GROUP LIG ON IL.RET_LIR_ID = LIG.RET_LIR_ID "
			+ " LEFT JOIN UOM_LOOKUP UL ON IL.UOM_ID = UL.ID "
			+ " LEFT JOIN BRAND_LOOKUP BL ON IL.BRAND_ID = BL.BRAND_ID"
			+ " WHERE IL.ACTIVE_INDICATOR = 'Y'";
	
	
	private static final String GET_ITEMS_FOR_PROMO_RECOMMENDATION = " SELECT SIM.*, IL.RET_LIR_ID, "
			+ " LIG.RET_LIR_ITEM_CODE, IL.ITEM_SIZE, "
			+ " LIG.RET_LIR_NAME, IL.ITEM_NAME, "
			+ " IL.UPC, IL.LIR_IND, IL.BRAND_ID, BL.BRAND_NAME, IL.RETAILER_ITEM_CODE, IL.UOM_ID, UL.NAME AS UOM_NAME, IL.PREPRICED_IND, "
			+ " IL.SHIPPER_FLAG, IL.ACTIVE_INDICATOR, "
			+ " CS.PRICE_ZONE_ID, RPZ.ZONE_NUM FROM (SELECT * FROM " + " (" + " %AUTHORIZED_ITEM_QUERY% "
			+ " ) STORE_ITEM_MAP LEFT JOIN %PRODUCT_CONDITION2% "
			+ " ) SIM LEFT JOIN ITEM_LOOKUP IL ON SIM.ITEM_CODE = IL.ITEM_CODE "
			+ " LEFT JOIN RETAILER_LIKE_ITEM_GROUP LIG ON IL.RET_LIR_ID = LIG.RET_LIR_ID "
			+ " LEFT JOIN UOM_LOOKUP UL ON IL.UOM_ID = UL.ID "
			+ " LEFT JOIN COMPETITOR_STORE CS ON SIM.LEVEL_ID = CS.COMP_STR_ID "
			+ " LEFT JOIN RETAIL_PRICE_ZONE RPZ ON CS.PRICE_ZONE_ID = RPZ.PRICE_ZONE_ID "
			+ " LEFT JOIN BRAND_LOOKUP BL ON IL.BRAND_ID = BL.BRAND_ID"
			+ " WHERE IL.ACTIVE_INDICATOR = 'Y' ";
	
	private static final String GET_ITEMS_FROM_PAST_RECOMMENDATION = "SELECT SIM.*, IL.RET_LIR_ID,  LIG.RET_LIR_ITEM_CODE, IL.ITEM_SIZE,  "
			+ " LIG.RET_LIR_NAME, IL.ITEM_NAME,  IL.UPC, IL.LIR_IND, IL.BRAND_ID, BL.BRAND_NAME, "
			+ " IL.RETAILER_ITEM_CODE, IL.UOM_ID, UL.NAME AS UOM_NAME, IL.PREPRICED_IND,  IL.SHIPPER_FLAG,  "
			+ " IL.USER_ATTR_3, IL.USER_ATTR_4, IL.USER_ATTR_5, IL.USER_ATTR_6, IL.USER_ATTR_7, "
			+ " IL.USER_ATTR_8, IL.USER_ATTR_9, IL.USER_ATTR_10, IL.USER_ATTR_11, "
			+ " IL.USER_ATTR_12, IL.USER_ATTR_13, IL.USER_ATTR_14, TO_CHAR(IL.ITEM_SETUP_DATE, " + Constants.DB_DATE_FORMAT + ") AS ITEM_SETUP_DATE, "
			+ " SIM.ACTUAL_ZONE_ID AS PRICE_ZONE_ID, RPZ.ZONE_NUM, IL.ACTIVE_INDICATOR FROM (SELECT * FROM  "
			+ " (SELECT LIR_ID_OR_ITEM_CODE AS ITEM_CODE, ACTUAL_ZONE_ID, VENDOR_ID, DIST_FLAG "
			+ " FROM PR_RECOMMENDATION WHERE RUN_ID = ?  AND LIR_IND = 'N') "
			+ " STORE_ITEM_MAP LEFT JOIN %PRODUCT_CONDITION2%  ) SIM "
			+ " LEFT JOIN ITEM_LOOKUP IL ON SIM.ITEM_CODE = IL.ITEM_CODE  "
			+ " LEFT JOIN RETAILER_LIKE_ITEM_GROUP LIG ON IL.RET_LIR_ID = LIG.RET_LIR_ID  "
			+ " LEFT JOIN UOM_LOOKUP UL ON IL.UOM_ID = UL.ID  "
			+ " LEFT JOIN RETAIL_PRICE_ZONE RPZ ON SIM.ACTUAL_ZONE_ID = RPZ.PRICE_ZONE_ID  "
			+ " LEFT JOIN BRAND_LOOKUP BL ON IL.BRAND_ID = BL.BRAND_ID  WHERE IL.ACTIVE_INDICATOR = 'Y' ";
	
	private static final String GET_ALL_ACTIVE_ITEMS = " SELECT PRODUCT_HIERARCHY.*, IL.RET_LIR_ID, "
			+ " LIG.RET_LIR_ITEM_CODE, IL.ITEM_SIZE,IL.SEND_TO_PREDICTION, "
			+ " LIG.RET_LIR_NAME, IL.ITEM_NAME, "
			+ " IL.UPC, IL.LIR_IND, IL.BRAND_ID, BL.BRAND_NAME, IL.RETAILER_ITEM_CODE, IL.UOM_ID, UL.NAME AS UOM_NAME, IL.PREPRICED_IND, "
			+ " IL.SHIPPER_FLAG, IL.USER_ATTR_3, IL.USER_ATTR_4, IL.USER_ATTR_5, IL.USER_ATTR_6, IL.USER_ATTR_7, "
			+ " IL.USER_ATTR_8, IL.USER_ATTR_9, IL.USER_ATTR_10, IL.USER_ATTR_11, "
			+ " IL.USER_ATTR_12, IL.USER_ATTR_13, IL.USER_ATTR_14, ITEM_SETUP_DATE,USER_ATTR_15,"
			+ " IL.ACTIVE_INDICATOR FROM  %PRODUCT_CONDITION2% "
			+ " LEFT JOIN ITEM_LOOKUP IL ON PRODUCT_HIERARCHY.ITEM_CODE = IL.ITEM_CODE "
			+ " LEFT JOIN RETAILER_LIKE_ITEM_GROUP LIG ON IL.RET_LIR_ID = LIG.RET_LIR_ID "
			+ " LEFT JOIN UOM_LOOKUP UL ON IL.UOM_ID = UL.ID "
			+ " LEFT JOIN BRAND_LOOKUP BL ON IL.BRAND_ID = BL.BRAND_ID "
		    + " WHERE IL.ACTIVE_INDICATOR = 'Y' ";
			//+ " WHERE IL.RETAILER_ITEM_CODE IN('411930') ";

	private static Logger logger = Logger.getLogger("ItemDAO");

	/*public HashMap<Integer, PRItemDTO> getAuthorizedItemsOfZone(Connection conn, PRStrategyDTO inputDTO) throws OfferManagementException {
		ProductService productService = new ProductService();
		HashMap<Integer, PRItemDTO> itemDataMap = new HashMap<Integer, PRItemDTO>();
		int productLevelId = inputDTO.getProductLevelId();
		int productId = inputDTO.getProductId();
		PreparedStatement stmt = null;
		ResultSet rs = null;

		try {
			HashMap<Integer, Integer> parentChildRelationMap = productService.getProductLevelRelationMap(conn, productLevelId);
			HashMap<Integer, Integer> productLevelPointer = productService.getProductLevelPointer(conn, productLevelId);
			int counter = 0;
			String sql = null;
			sql = GET_AUTHORIZED_ITEMS_ZONE;
			sql = sql.replaceAll("%PRODUCT_CONDITION1%", getQueryCondition(productLevelId, productId));
			sql = sql.replaceAll("%PRODUCT_CONDITION2%", getProductHierarchy(conn, productLevelId, productService));
			stmt = conn.prepareStatement(sql);
			stmt.setInt(++counter, inputDTO.getLocationId());
			if (productLevelId > 1) {
				stmt.setInt(++counter, productLevelId);
				if (productId > 0) {
					stmt.setLong(++counter, productId);
				}
			} else if (productLevelId == 1) {
				stmt.setLong(++counter, productId);
			}

			stmt.setFetchSize(200000);
			rs = stmt.executeQuery();

			while (rs.next()) {
				int itemCode = rs.getInt("ITEM_CODE");
				PRItemDTO itemDTO = new PRItemDTO();
				itemDTO.setItemCode(itemCode);
				itemDTO.setRetLirId(rs.getInt("RET_LIR_ID"));
				itemDTO.setRetLirItemCode(rs.getInt("RET_LIR_ITEM_CODE"));
				itemDTO.setItemSize(rs.getDouble("ITEM_SIZE"));
				itemDTO.setUpc(rs.getString("UPC")); // Used during prediction
				String lirInd = rs.getString("LIR_IND");
				if (String.valueOf(Constants.YES).equalsIgnoreCase(lirInd)) {
					itemDTO.setLir(true);
				}

				itemDTO.setUOMId(rs.getString("UOM_ID"));
				itemDTO.setUOMName(rs.getString("UOM_NAME"));
				for (Integer productLevel : parentChildRelationMap.keySet()) {
					if (productLevel == Constants.DEPARTMENTLEVELID)
						itemDTO.setDeptProductId(rs.getInt("P_ID_" + productLevelPointer.get(productLevel)));
					else if (productLevel == Constants.PORTFOLIO)
						itemDTO.setPortfolioProductId(rs.getInt("P_ID_" + productLevelPointer.get(productLevel)));
					else if (productLevel == Constants.CATEGORYLEVELID)
						itemDTO.setCategoryProductId(rs.getInt("P_ID_" + productLevelPointer.get(productLevel)));
					else if (productLevel == Constants.SUBCATEGORYLEVELID)
						itemDTO.setSubCatProductId(rs.getInt("P_ID_" + productLevelPointer.get(productLevel)));
					else if (productLevel == Constants.SEGMENTLEVELID)
						itemDTO.setSegmentProductId(rs.getInt("P_ID_" + productLevelPointer.get(productLevel)));
					else if (productLevel == Constants.ITEMLEVELID)
						itemDTO.setItemCode(rs.getInt("ITEM_CODE"));
				}
				if (itemDTO.getDeptProductId() > 0)
					itemDataMap.put(itemDTO.getItemCode(), itemDTO);
			}
		} catch (Exception | GeneralException exception) {
			throw new OfferManagementException("Error in getAuthorizedItemOfZone() - " + exception, 
					RecommendationErrorCode.DB_GET_AUTHORIZED_ITEMS);
		} finally {
			PristineDBUtil.close(rs);
			PristineDBUtil.close(stmt);
		}
		return itemDataMap;
	}*/
	
	private List<PRItemDTO> getAuthorizedItemsOfZoneAndStore(Connection conn, PRStrategyDTO inputDTO,
			List<Integer> priceZoneStores, boolean isItemListAlreadyKnown, List<Integer> items)
			throws OfferManagementException {
		ProductService productService = new ProductService();
		int productLevelId = inputDTO.getProductLevelId();
		int productId = inputDTO.getProductId();
		PreparedStatement stmt = null;
		ResultSet rs = null;
		List<PRItemDTO> itemList = new ArrayList<PRItemDTO>();
		
		try {
			HashMap<Integer, Integer> parentChildRelationMap = productService.getProductLevelRelationMapRec(conn, productLevelId);
			HashMap<Integer, Integer> productLevelPointer = productService.getProductLevelPointerRec(conn, productLevelId);
			int counter = 0;
			String sql = null;
			String storeIds = "";
			
			if (priceZoneStores.size() > 0) {
				// sql = GET_AUTHORIZED_ITEMS_ZONE_AND_STORE;
				sql = GET_AUTHORIZED_ITEMS_ZONE_AND_STORE_OPTIMIZED;
				storeIds = PRCommonUtil.getCommaSeperatedStringFromIntArray(priceZoneStores);
				//sql = sql.replaceAll("%STORE_ID%", storeIds);
				//sql = sql.replaceAll("%PRODUCT_CONDITION1%", getQueryCondition(productLevelId, productId));
				sql = sql.replaceAll("%AUTHORIZED_ITEM_QUERY%", queryToGetAuthorizedItems(conn, productLevelId, productId, storeIds,
						isItemListAlreadyKnown, items, inputDTO.getLocationId(), inputDTO.getLocationLevelId()));
				sql = sql.replaceAll("%PRODUCT_CONDITION2%", getProductHierarchy(conn, productLevelId, productService));

				logger.debug("Item Fetching Query: " + sql);

				stmt = conn.prepareStatement(sql);
				if (!isItemListAlreadyKnown) {
					if (productLevelId > 1) {
						stmt.setInt(++counter, productLevelId);
						if (productId > 0) {
							stmt.setLong(++counter, productId);
						}
					} else if (productLevelId == 1) {
						stmt.setLong(++counter, productId);
					}
				}

				stmt.setFetchSize(200000);
				rs = stmt.executeQuery();

				while (rs.next()) {
					int itemCode = rs.getInt("ITEM_CODE");
					PRItemDTO itemDTO = new PRItemDTO();
					itemDTO.setItemCode(itemCode);
					itemDTO.setItemName(rs.getString("ITEM_NAME"));
					itemDTO.setRetLirName(rs.getString("RET_LIR_NAME"));
					itemDTO.setRetLirId(rs.getInt("RET_LIR_ID"));
					// itemDTO.setRetLirItemCode(rs.getInt("RET_LIR_ITEM_CODE"));
					itemDTO.setItemSize(rs.getDouble("ITEM_SIZE"));
					itemDTO.setRetailerItemCode(rs.getString("RETAILER_ITEM_CODE"));
					itemDTO.setUpc(rs.getString("UPC")); // Used during
															// prediction
					//String lirInd = rs.getString("LIR_IND");
					itemDTO.setBrandId(rs.getInt("BRAND_ID"));
					itemDTO.setBrandName(rs.getString("BRAND_NAME"));
					
					if (rs.getObject("PREPRICED_IND") != null)
						itemDTO.setIsPrePriced(rs.getInt("PREPRICED_IND"));
					
					if (rs.getObject("SHIPPER_FLAG") != null) {
						if(String.valueOf(Constants.YES).equalsIgnoreCase(rs.getString("SHIPPER_FLAG"))){
							itemDTO.setShipperItem(true);
						}
					}

//					if (String.valueOf(Constants.YES).equalsIgnoreCase(lirInd)) {
//						itemDTO.setLir(true);
//					}

					itemDTO.setUOMId(rs.getString("UOM_ID"));
					itemDTO.setUOMName(rs.getString("UOM_NAME"));
					for (Integer productLevel : parentChildRelationMap.keySet()) {
						if (productLevel == Constants.DEPARTMENTLEVELID)
							itemDTO.setDeptProductId(rs.getInt("P_ID_" + productLevelPointer.get(productLevel)));
						else if (productLevel == Constants.PORTFOLIO)
							itemDTO.setPortfolioProductId(rs.getInt("P_ID_" + productLevelPointer.get(productLevel)));
						else if (productLevel == Constants.CATEGORYLEVELID)
							itemDTO.setCategoryProductId(rs.getInt("P_ID_" + productLevelPointer.get(productLevel)));
						else if (productLevel == Constants.SUBCATEGORYLEVELID)
							itemDTO.setSubCatProductId(rs.getInt("P_ID_" + productLevelPointer.get(productLevel)));
						else if (productLevel == Constants.SEGMENTLEVELID)
							itemDTO.setSegmentProductId(rs.getInt("P_ID_" + productLevelPointer.get(productLevel)));
						else if (productLevel == Constants.ITEMLEVELID)
							itemDTO.setItemCode(rs.getInt("ITEM_CODE"));
					}
					itemDTO.setChildLocationLevelId(Constants.STORE_LEVEL_ID);
					itemDTO.setChildLocationId(rs.getInt("LEVEL_ID"));
					itemDTO.setItemCode(rs.getInt("ITEM_CODE"));
					itemDTO.setVendorId(rs.getInt("VENDOR_ID"));
					if (rs.getObject("DIST_FLAG") != null)
						itemDTO.setDistFlag(rs.getString("DIST_FLAG").charAt(0));
					
					// Dinesh:: 01st Mar 2018, Consider item is in DSD Zone if DIST FLAG = "D"(Related to GE)
					if (rs.getObject("DIST_FLAG") != null && rs.getString("DIST_FLAG").trim().equals(String.valueOf(Constants.DSD))){
						itemDTO.setDSDItem(true);
					}

					boolean useStoreItemMapForZone = Boolean.parseBoolean
							(PropertyManager.getProperty("USE_ZONE_FROM_STORE_ITEM_MAP", "FALSE"));
					if(useStoreItemMapForZone)
						itemDTO.setPriceZoneId(rs.getInt("ALT_PRICE_ZONE_ID")); 
					else
						itemDTO.setPriceZoneId(rs.getInt("PRICE_ZONE_ID"));
					if(useStoreItemMapForZone){
						if (rs.getObject("ALT_ZONE_NUM") != null)
							itemDTO.setPriceZoneNo(rs.getString("ALT_ZONE_NUM"));
					}else{
						if (rs.getObject("ZONE_NUM") != null)
							itemDTO.setPriceZoneNo(rs.getString("ZONE_NUM"));
					}
					
					
					if (rs.getObject("ACTIVE_INDICATOR") != null) {
						if(String.valueOf(Constants.YES).equalsIgnoreCase(rs.getString("ACTIVE_INDICATOR"))){
							itemDTO.setActive(true);
						}
					}
					
					if (rs.getObject("IS_AUTHORIZED") != null) {
						if(String.valueOf(Constants.YES).equalsIgnoreCase(rs.getString("IS_AUTHORIZED"))){
							itemDTO.setAuthorized(true);
						}
					}
					
					itemDTO.setSendToPrediction(true);
					// Additional attributes
					itemDTO.setUserAttr3(rs.getString("USER_ATTR_3"));
					itemDTO.setUserAttr4(rs.getString("USER_ATTR_4"));
					itemDTO.setUserAttr5(rs.getString("USER_ATTR_5"));
					itemDTO.setUserAttr6(rs.getString("USER_ATTR_6"));
					itemDTO.setUserAttr7(rs.getString("USER_ATTR_7"));
					itemDTO.setUserAttr8(rs.getString("USER_ATTR_8"));
					itemDTO.setUserAttr9(rs.getString("USER_ATTR_9"));
					itemDTO.setUserAttr10(rs.getString("USER_ATTR_10"));
					itemDTO.setUserAttr11(rs.getString("USER_ATTR_11"));
					itemDTO.setUserAttr12(rs.getString("USER_ATTR_12"));
					itemDTO.setUserAttr13(rs.getString("USER_ATTR_13"));
					itemDTO.setUserAttr14(rs.getString("USER_ATTR_14"));
					itemDTO.setUserAttr15(rs.getString("USER_ATTR_15"));//FReight Charges for AZ
					itemDTO.setItemSetupDate(rs.getString("ITEM_SETUP_DATE"));
					if (itemDTO.getDeptProductId() > 0)
						itemList.add(itemDTO);
				}
			}
		} catch (Exception | GeneralException exception) {
			throw new OfferManagementException("Error in getAuthorizedItemsOfZoneAndStore() - " + exception, 
					RecommendationErrorCode.DB_GET_AUTHORIZED_ITEMS);
		} finally {
			if(rs != null)
				PristineDBUtil.close(rs);
			if(stmt != null)
				PristineDBUtil.close(stmt);
		}
		return itemList;
	}
	
	public List<PRItemDTO> getAuthorizedItemsOfZoneAndStore(Connection conn, PRStrategyDTO inputDTO,
			List<Integer> priceZoneStores) throws OfferManagementException {
		List<PRItemDTO> itemList = getAuthorizedItemsOfZoneAndStore(conn, inputDTO, priceZoneStores, false, null);
		return itemList;
	}
	
	public List<PRItemDTO> getAuthorizedItemsOfZoneAndStore(Connection conn, int productLevelId, int productId,
			List<Integer> items, List<Integer> priceZoneStores) throws OfferManagementException {
		PRStrategyDTO inputDTO = new PRStrategyDTO();
		inputDTO.setProductLevelId(productLevelId);
		inputDTO.setProductId(productId);
		List<PRItemDTO> itemList = getAuthorizedItemsOfZoneAndStore(conn, inputDTO, priceZoneStores, true, items);
		return itemList;
	}
	
	public List<PRItemDTO> getAuthorizedItemsOfZoneAndStore(Connection conn, int productLevelId, int productId, int locationLevelId, int locationId,
			List<Integer> priceZoneStores) throws OfferManagementException {
		PRStrategyDTO inputDTO = new PRStrategyDTO();
		inputDTO.setProductLevelId(productLevelId);
		inputDTO.setProductId(productId);
		inputDTO.setLocationLevelId(locationLevelId);
		inputDTO.setLocationId(locationId);
		List<PRItemDTO> itemList = getAuthorizedItemsOfZoneAndStore(conn, inputDTO, priceZoneStores, false, null);
		return itemList;
	}
	
	/*public List<PRItemDTO> getAuthorizedItemsOfStores(Connection conn, PRStrategyDTO inputDTO, 
			boolean onlyDsdItems) throws OfferManagementException {
		PreparedStatement stmt = null;
		ResultSet rs = null;
		List<PRItemDTO> dsdItems = new ArrayList<PRItemDTO>();
		String query;
		int vCounter = 0;

		try {
			query = new String(GET_AUTHORIZED_ITEMS_STORE);
			if(onlyDsdItems)
				query = query.replaceAll("%DSD_FLAG%", " AND DIST_FLAG = '" + Constants.DSD + "'");
			else
				query = query.replaceAll("%DSD_FLAG%", "");
					
			query = query.replaceAll("%QUERY_CONDITION%", getQueryCondition(inputDTO.getProductLevelId(), inputDTO.getProductId()));
			// logger.debug("DSD Items Query " + query);
			stmt = conn.prepareStatement(query);
			//stmt.setString(++vCounter, String.valueOf(Constants.DSD));
			stmt.setInt(++vCounter, inputDTO.getLocationId());

			if (inputDTO.getProductLevelId() > 1) {
				stmt.setInt(++vCounter, inputDTO.getProductLevelId());
				if (inputDTO.getProductId() > 0) {
					stmt.setLong(++vCounter, inputDTO.getProductId());
				}
			} else if (inputDTO.getProductLevelId() == 1) {
				stmt.setLong(++vCounter, inputDTO.getProductId());
			}

			logger.debug("Counter 1: " + String.valueOf(Constants.DSD) + ",Counter 2: " + inputDTO.getLocationId() + ",Counter 3: "
					+ inputDTO.getProductLevelId() + ",Counter 4: " + inputDTO.getProductId());

			stmt.setFetchSize(100000);
			rs = stmt.executeQuery();

			while (rs.next()) {
				PRItemDTO prItemDTO = new PRItemDTO();
				prItemDTO.setChildLocationLevelId(Constants.STORE_LEVEL_ID);
				prItemDTO.setChildLocationId(rs.getInt("LEVEL_ID"));
				prItemDTO.setItemCode(rs.getInt("ITEM_CODE"));
				prItemDTO.setVendorId(rs.getInt("VENDOR_ID"));
				if (rs.getObject("DIST_FLAG") != null)
					prItemDTO.setDistFlag(rs.getString("DIST_FLAG").charAt(0));
				dsdItems.add(prItemDTO);
			}
		} catch (Exception e) {
			throw new OfferManagementException("Error in getAuthorizedItemsOfStores() - " + e, 
					RecommendationErrorCode.DB_GET_STORE_ITEMS);
		} finally {
			PristineDBUtil.close(rs);
			PristineDBUtil.close(stmt);
		}
		return dsdItems;
	}*/

	public String getProductHierarchy(Connection conn, int productLevel, ProductService productService) throws GeneralException {
		StringBuffer sb = new StringBuffer();

		if (productService.productHierarchy.size() == 0)
			productService.getChildHierarchyRec(conn, productLevel);

		int productHierarchyCount = productService.productHierarchy.size();

		sb.append("(SELECT ITEM_CODE as ITEM_CODE_1,");
		for (int i = 1; i < productHierarchyCount; i++) {
			//sb.append("P_L_ID_" + i + ", P_ID_" + i + ", P" + i + ".NAME NAME_" + i + ",");
			sb.append("P_L_ID_" + i + ", P_ID_" + i + ",");
		}
		sb.delete((sb.length() - 1), sb.length());
		sb.append(" FROM ");

//		for (int s = 1; s < productHierarchyCount; s++) {
//			sb.append("( ");
//		}

//		sb.append(" SELECT ");
		sb.append(" (SELECT ");
		for (int j = 1; j < productHierarchyCount; j++) {
			if (j == 1) {
				sb.append(" A" + j + ".PRODUCT_LEVEL_ID P_L_ID_" + j + ", A" + j + ".PRODUCT_ID P_ID_" + j + ", A" + j
						+ ".CHILD_PRODUCT_ID ITEM_CODE ,");
			} else {
				sb.append(" A" + j + ".PRODUCT_LEVEL_ID P_L_ID_" + j + ", A" + j + ".PRODUCT_ID P_ID_" + j + ", A" + j
						+ ".CHILD_PRODUCT_ID CHILD_P_ID_" + j + ",");
			}
		}
		sb.delete((sb.length() - 1), sb.length());

		sb.append(" FROM ");
		for (int x = 1; x < productHierarchyCount; x++) {
			int loop = productHierarchyCount - 1;
			if (x > loop) {
				break;
			}
			sb.append("( ");
		}

		for (int k = 1; k <= productHierarchyCount; k++) {
			if (k == 1) {
				sb.append(" PRODUCT_GROUP_RELATION_REC A" + k + " ");
			} else if (k == 2) {
				sb.append(" LEFT JOIN PRODUCT_GROUP_RELATION_REC A" + k + " ");
				break;
			}
		}

		String firstLevel = "";
		String nextLevel = "";
		for (int a = 1; a < productHierarchyCount + 1; a++) {
			if (a == 1) {
				firstLevel = "A" + a;
			} else if (a == 2) {
				nextLevel = "A" + a;
				break;
			}
		}
		sb.append("ON " + firstLevel + ".PRODUCT_ID = " + nextLevel + ".CHILD_PRODUCT_ID AND " + firstLevel + ".PRODUCT_LEVEL_ID = "
				+ nextLevel + ".CHILD_PRODUCT_LEVEL_ID) ");

		for (int b = 1; b < productHierarchyCount + 1; b++) {
			int loopCheck = b;
			if (b >= 3) {
				sb.append("LEFT JOIN PRODUCT_GROUP_RELATION_REC A" + b + " ");
				sb.append(" ON A" + (loopCheck - 1) + ".PRODUCT_ID = A" + b + ".CHILD_PRODUCT_ID AND A" + (loopCheck - 1)
						+ ".PRODUCT_LEVEL_ID = A" + b + ".CHILD_PRODUCT_LEVEL_ID ) ");
			}
		}

		for (int c = 1; c < productHierarchyCount + 1; c++) {
			if (c == 1) {
				sb.append("WHERE A" + c + ".CHILD_PRODUCT_LEVEL_ID = 1 ");
				break;
			}
		}
		sb.append(") ");

//		for (int d = 1; d < productHierarchyCount; d++) {
//			sb.append("LEFT JOIN PRODUCT_GROUP P" + d + " ON P_ID_" + d + " = P" + d + ".PRODUCT_ID AND P_L_ID_" + d + " = P" + d
//					+ ".PRODUCT_LEVEL_ID ) ");
//		}
		//sb.append(" PRODUCT_HIERARCHY ON PRODUCT_HIERARCHY.ITEM_CODE_1 = STORE_ITEM_MAP.ITEM_CODE ");
		//sb.append(" AND PRODUCT_HIERARCHY.P_L_ID_");
		//sb.append(productHierarchyCount-1).append(" = ").append(Constants.DEPARTMENTLEVELID);
		
		sb.append(" WHERE P_L_ID_").append(productHierarchyCount-1).append(" = ").append(Constants.DEPARTMENTLEVELID);
		sb.append(" ) PRODUCT_HIERARCHY ON PRODUCT_HIERARCHY.ITEM_CODE_1 = STORE_ITEM_MAP.ITEM_CODE ");
		

		return sb.toString();
	}

	public String queryToGetAuthorizedItems(Connection conn, int productLevelId, int productId, String priceZoneStoreIds, int locationId, int locationLevelId)
			throws GeneralException {
		String subQuery = queryToGetAuthorizedItems(conn, productLevelId, productId, priceZoneStoreIds, false, null, locationId, locationLevelId);
		return subQuery;
	}
	  
	private String queryToGetAuthorizedItems(Connection conn, int productLevelId, int productId, String priceZoneStoreIds,
			boolean isItemListAlreadyKnown, List<Integer> items, int locationId, int locationLevelId) throws GeneralException {
		StringBuffer subQuery = new StringBuffer("");
		
		boolean isGlobalZone = new RetailPriceZoneDAO().getIsGlobalZone(conn, locationId);
		
		subQuery.append(" SELECT ITEM_CODE, LEVEL_ID, VENDOR_ID, DIST_FLAG, COST_INDICATOR, SIM.IS_AUTHORIZED, ");
		subQuery.append(" SIM.PRICE_ZONE_ID AS ALT_PRICE_ZONE_ID, RPZ2.ZONE_NUM AS ALT_ZONE_NUM  FROM STORE_ITEM_MAP SIM "); 
		subQuery.append(" LEFT JOIN RETAIL_PRICE_ZONE RPZ2 ON RPZ2.PRICE_ZONE_ID =  SIM.PRICE_ZONE_ID ");
		subQuery.append(" WHERE SIM.LEVEL_TYPE_ID = 2 AND SIM.LEVEL_ID IN ");
		
		if(isGlobalZone)
		{
			subQuery.append(" ( ");
			subQuery.append(" SELECT CS.COMP_STR_ID FROM COMPETITOR_STORE CS, RETAIL_PRICE_ZONE RPZ WHERE CS.PRICE_ZONE_ID_3 = RPZ.PRICE_ZONE_ID AND RPZ.PRICE_ZONE_ID =").append(locationId);
			subQuery.append(" ) ");
		}
		else
		{
			subQuery.append(" ( ");
			subQuery.append(priceZoneStoreIds);
			subQuery.append(" ) ");
		}
		
		//NU::  12th Feb 2018, temporary code block to handle duplicating of items across the
		//different pricing group. GE reported that items which are not supposed to show in 106-1 is shown.
		//the items are present in both 106-1 and 106-3
		/********************/
		boolean useStoreItemMapForZone = Boolean.parseBoolean(PropertyManager.getProperty("USE_ZONE_FROM_STORE_ITEM_MAP", "FALSE"));
		if (useStoreItemMapForZone) {
			//NU:: 15th Feb 2018, GE wanted to combine multiple zones in to single zone 
			//e.g. 106-1,24-1,26-1 to 106-1. For GE while picking authorized item price zone id
			//is also checked, this will ignore items from 24-1 and 26-1, in order to get those
			//zone items, those zones will also be included in the IN clause
			//List<Integer> primaryAndMatchingZoneIds = getPrimaryAndMatcingZoneIds(conn, locationId);
			//String locationIds = primaryAndMatchingZoneIds.stream().map(loc-> String.valueOf(loc)).collect(Collectors.joining(","));
			//Dinesh:: 28th Feb 2018, Query changes to include DSD zones along with Warehouese zones in recommendation
			subQuery.append(" AND (SIM.PRICE_ZONE_ID IN (SELECT LOCATION_ID FROM PR_PRODUCT_LOCATION_MAPPING WHERE PRODUCT_LEVEL_ID = ");
			subQuery.append(productLevelId).append("  AND PRODUCT_ID = ").append(productId);
			subQuery.append(" AND PARENT_LOCATION_ID IN (").append(locationId).append(" ) AND PARENT_LOCATION_LEVEL_ID = ").append(locationLevelId);
			subQuery.append(") OR SIM.PRICE_ZONE_ID IN ( ").append(locationId).append("))");
		}
		/********************/
		
		subQuery.append(" AND SIM.ITEM_CODE IN (");
		if (!isItemListAlreadyKnown) {
			subQuery.append(" SELECT CHILD_PRODUCT_ID FROM (SELECT CHILD_PRODUCT_ID,CHILD_PRODUCT_LEVEL_ID FROM PRODUCT_GROUP_RELATION_REC PGR ");
			subQuery.append(getQueryCondition(productLevelId, productId));
			subQuery.append(" ) WHERE CHILD_PRODUCT_LEVEL_ID = 1) ");
		} else {
			subQuery.append(PRCommonUtil.getCommaSeperatedStringFromIntArray(items) + ")");
		}
		subQuery.append(" AND SIM.IS_AUTHORIZED = 'Y' ");
		return subQuery.toString();
	}
	
//	public String queryToGetAuthorizedItems(int productLevelId, int productId, String priceZoneStoreIds){
//		StringBuffer subQuery = new StringBuffer("");
//		subQuery.append(" SELECT ITEM_CODE, LEVEL_ID, VENDOR_ID, DIST_FLAG, COST_INDICATOR FROM STORE_ITEM_MAP SIM ");			
//		subQuery.append(" WHERE SIM.LEVEL_TYPE_ID = 2 AND SIM.LEVEL_ID IN ");
//		subQuery.append(" ( " ); 
//		subQuery.append(priceZoneStoreIds);
//		subQuery.append(" ) ");
//		subQuery.append(" AND SIM.ITEM_CODE IN (" );
//		subQuery.append(" SELECT CHILD_PRODUCT_ID FROM (SELECT CHILD_PRODUCT_ID,CHILD_PRODUCT_LEVEL_ID FROM PRODUCT_GROUP_RELATION PGR ");
//		subQuery.append(getQueryCondition(productLevelId, productId));
//		subQuery.append(" ) WHERE CHILD_PRODUCT_LEVEL_ID = 1) ");
//		//For debugging only
//		/*subQuery.append(" SELECT ITEM_CODE FROM ITEM_LOOKUP WHERE RET_LIR_ID = 100685)");*/
//		subQuery.append(" AND SIM.IS_AUTHORIZED = 'Y' ");
//		return subQuery.toString();
//	}
	
	private String getQueryCondition(int productLevelId, int productId) {
		StringBuffer subQuery = new StringBuffer("");
		if (productLevelId > 1) {
			subQuery = new StringBuffer("start with product_level_id = ? ");
			if (productId > 0) {
				subQuery.append(" and product_id = ? ");
			}
			subQuery.append("connect by  prior child_product_id = product_id  and  prior child_product_level_id = product_level_id");
		} else if (productLevelId == 1) {
			subQuery = new StringBuffer("where child_product_level_id = 1 and child_product_id = ?");
		}

		return subQuery.toString();
	}
	
	public List<PRItemDTO> getLigMembers(Connection conn, int retLirId) throws GeneralException {
		PreparedStatement stmt = null;
		ResultSet rs = null;
		List<PRItemDTO> items = new ArrayList<PRItemDTO>();
		PRItemDTO itemDTO = null;
		try {
			stmt = conn.prepareStatement(GET_LIG_MEMBERS);
			stmt.setFetchSize(100000);
			stmt.setInt(1, retLirId);
			rs = stmt.executeQuery();
			while (rs.next()) {
				itemDTO = new PRItemDTO();
				itemDTO.setItemCode(rs.getInt("ITEM_CODE"));
				items.add(itemDTO);
			}
		} catch (SQLException e) {
			logger.error("Error in getLigMembers() - " + e);
			throw new GeneralException("Error in getLigMembers() - " + e);
		} finally {
			PristineDBUtil.close(rs);
			PristineDBUtil.close(stmt);
		}
		return items;
	}
	
	public List<PRItemDTO> getAuthorizedItems(Connection conn, int locationLevelId, int locationId, int productLevelId, int productId)
			throws OfferManagementException {
		ProductService productService = new ProductService();
		PreparedStatement stmt = null;
		ResultSet rs = null;
		List<PRItemDTO> itemList = new ArrayList<PRItemDTO>();

		try {
			List<Integer> storesInLocation = new LocationGroupDAO(conn).getStoresOfLocation(locationLevelId, locationId);

			HashMap<Integer, Integer> parentChildRelationMap = productService.getProductLevelRelationMap(conn, Constants.DEPARTMENTLEVELID);
			HashMap<Integer, Integer> productLevelPointer = productService.getProductLevelPointer(conn, Constants.DEPARTMENTLEVELID);
			String sql = null;
			String storeIds = "";
			int counter = 0;
			
			if (storesInLocation.size() > 0) {
				sql = GET_AUTHORIZED_ITEMS_ZONE_AND_STORE_OPTIMIZED;
				storeIds = PRCommonUtil.getCommaSeperatedStringFromIntArray(storesInLocation);
				sql = sql.replaceAll("%AUTHORIZED_ITEM_QUERY%", formQueryToGetAuthorizedItems(productLevelId, productId, storeIds));
				sql = sql.replaceAll("%PRODUCT_CONDITION2%", getProductHierarchy(conn, Constants.DEPARTMENTLEVELID, productService));

				logger.debug("Item Fetching Query: " + sql);

				stmt = conn.prepareStatement(sql);

				if (productLevelId > 1) {
					stmt.setInt(++counter, productLevelId);
					if (productId > 0) {
						stmt.setLong(++counter, productId);
					}
				} else if (productLevelId == 1) {
					stmt.setLong(++counter, productId);
				}
				
				stmt.setFetchSize(200000);
				rs = stmt.executeQuery();

				while (rs.next()) {
					int itemCode = rs.getInt("ITEM_CODE");
					PRItemDTO itemDTO = new PRItemDTO();
					itemDTO.setItemCode(itemCode);
					itemDTO.setItemName(rs.getString("ITEM_NAME"));
					itemDTO.setRetLirName(rs.getString("RET_LIR_NAME"));
					itemDTO.setRetLirId(rs.getInt("RET_LIR_ID"));
					itemDTO.setItemSize(rs.getDouble("ITEM_SIZE"));
					itemDTO.setRetailerItemCode(rs.getString("RETAILER_ITEM_CODE"));
					itemDTO.setUpc(rs.getString("UPC"));
					itemDTO.setBrandId(rs.getInt("BRAND_ID"));

					if (rs.getObject("PREPRICED_IND") != null)
						itemDTO.setIsPrePriced(rs.getInt("PREPRICED_IND"));

					if (rs.getObject("SHIPPER_FLAG") != null) {
						if (String.valueOf(Constants.YES).equalsIgnoreCase(rs.getString("SHIPPER_FLAG"))) {
							itemDTO.setShipperItem(true);
						}
					}

					itemDTO.setUOMId(rs.getString("UOM_ID"));
					itemDTO.setUOMName(rs.getString("UOM_NAME"));
					for (Integer productLevel : parentChildRelationMap.keySet()) {
						if (productLevel == Constants.DEPARTMENTLEVELID)
							itemDTO.setDeptProductId(rs.getInt("P_ID_" + productLevelPointer.get(productLevel)));
						else if (productLevel == Constants.PORTFOLIO)
							itemDTO.setPortfolioProductId(rs.getInt("P_ID_" + productLevelPointer.get(productLevel)));
						else if (productLevel == Constants.CATEGORYLEVELID)
							itemDTO.setCategoryProductId(rs.getInt("P_ID_" + productLevelPointer.get(productLevel)));
						else if (productLevel == Constants.SUBCATEGORYLEVELID)
							itemDTO.setSubCatProductId(rs.getInt("P_ID_" + productLevelPointer.get(productLevel)));
						else if (productLevel == Constants.SEGMENTLEVELID)
							itemDTO.setSegmentProductId(rs.getInt("P_ID_" + productLevelPointer.get(productLevel)));
						else if (productLevel == Constants.ITEMLEVELID)
							itemDTO.setItemCode(rs.getInt("ITEM_CODE"));
					}
					itemDTO.setChildLocationLevelId(Constants.STORE_LEVEL_ID);
					itemDTO.setChildLocationId(rs.getInt("LEVEL_ID"));
					itemDTO.setItemCode(rs.getInt("ITEM_CODE"));
					itemDTO.setVendorId(rs.getInt("VENDOR_ID"));
					if (rs.getObject("DIST_FLAG") != null)
						itemDTO.setDistFlag(rs.getString("DIST_FLAG").charAt(0));

					if (itemDTO.getDeptProductId() > 0)
						itemList.add(itemDTO);
				}
			}
		} catch (Exception | GeneralException exception) {
			throw new OfferManagementException("Error in getAuthorizedItemsOfZoneAndStore() - " + exception,
					RecommendationErrorCode.DB_GET_AUTHORIZED_ITEMS);
		} finally {
			if (rs != null)
				PristineDBUtil.close(rs);
			if (stmt != null)
				PristineDBUtil.close(stmt);
		}
		return itemList;
	}
	
	private String formQueryToGetAuthorizedItems(int productLevelId, int productId, String priceZoneStoreIds) {
		StringBuffer subQuery = new StringBuffer("");
		subQuery.append(" SELECT ITEM_CODE, LEVEL_ID, VENDOR_ID, DIST_FLAG, COST_INDICATOR, ");
		subQuery.append(" SIM.PRICE_ZONE_ID AS ALT_PRICE_ZONE_ID, RPZ2.ZONE_NUM AS ALT_ZONE_NUM  FROM STORE_ITEM_MAP SIM ");
		subQuery.append(" LEFT JOIN RETAIL_PRICE_ZONE RPZ2 ON RPZ2.PRICE_ZONE_ID =  SIM.PRICE_ZONE_ID ");
		subQuery.append(" WHERE SIM.LEVEL_TYPE_ID = 2 AND SIM.LEVEL_ID IN ");
		subQuery.append(" ( ");
		subQuery.append(priceZoneStoreIds);
		subQuery.append(" ) ");

		if (productLevelId > 0 && productId > 0) {
			subQuery.append(" AND SIM.ITEM_CODE IN (");
			subQuery.append(" SELECT CHILD_PRODUCT_ID FROM (SELECT CHILD_PRODUCT_ID,CHILD_PRODUCT_LEVEL_ID FROM PRODUCT_GROUP_RELATION PGR ");
			subQuery.append(getQueryCondition(productLevelId, productId));
			subQuery.append(" ) WHERE CHILD_PRODUCT_LEVEL_ID = 1) ");
		}

		subQuery.append(" AND SIM.IS_AUTHORIZED = 'Y' ");
		return subQuery.toString();

	}
	
	public List<PRItemDTO> getAuthorizedItems(Connection conn, int productLevelId, int productId, List<Integer> priceZoneStores) throws OfferManagementException {
		ProductService productService = new ProductService();
		PreparedStatement stmt = null;
		ResultSet rs = null;
		List<PRItemDTO> itemList = new ArrayList<PRItemDTO>();
		boolean isItemListAlreadyKnown = false;
		
		try {
			HashMap<Integer, Integer> parentChildRelationMap = productService.getProductLevelRelationMap(conn, productLevelId);
			HashMap<Integer, Integer> productLevelPointer = productService.getProductLevelPointer(conn, productLevelId);
			int counter = 0;
			String sql = null;
			String storeIds = "";

			if (priceZoneStores.size() > 0) {
				sql = GET_AUTHORIZED_ITEMS;
				storeIds = PRCommonUtil.getCommaSeperatedStringFromIntArray(priceZoneStores);
				sql = sql.replaceAll("%AUTHORIZED_ITEM_QUERY%",
						queryToGetDistinctAuthorizedItems(productLevelId, productId, storeIds, false, new ArrayList<Integer>()));
				sql = sql.replaceAll("%PRODUCT_CONDITION2%", getProductHierarchy(conn, productLevelId, productService));

				logger.debug("Item Fetching Query: " + sql);

				stmt = conn.prepareStatement(sql);
				if (!isItemListAlreadyKnown) {
					if (productLevelId > 1) {
						stmt.setInt(++counter, productLevelId);
						if (productId > 0) {
							stmt.setLong(++counter, productId);
						}
					} else if (productLevelId == 1) {
						stmt.setLong(++counter, productId);
					}
				}

				stmt.setFetchSize(200000);
				rs = stmt.executeQuery();

				while (rs.next()) {
					int itemCode = rs.getInt("ITEM_CODE");
					PRItemDTO itemDTO = new PRItemDTO();
					itemDTO.setItemCode(itemCode);
					itemDTO.setItemName(rs.getString("ITEM_NAME"));
					itemDTO.setRetLirName(rs.getString("RET_LIR_NAME"));
					itemDTO.setRetLirId(rs.getInt("RET_LIR_ID"));
					itemDTO.setItemSize(rs.getDouble("ITEM_SIZE"));
					itemDTO.setRetailerItemCode(rs.getString("RETAILER_ITEM_CODE"));
					itemDTO.setUpc(rs.getString("UPC"));  
					itemDTO.setBrandId(rs.getInt("BRAND_ID"));
					itemDTO.setBrandName(rs.getString("BRAND_NAME"));

					if (rs.getObject("PREPRICED_IND") != null)
						itemDTO.setIsPrePriced(rs.getInt("PREPRICED_IND"));

					if (rs.getObject("SHIPPER_FLAG") != null) {
						if (String.valueOf(Constants.YES).equalsIgnoreCase(rs.getString("SHIPPER_FLAG"))) {
							itemDTO.setShipperItem(true);
						}
					}

					itemDTO.setUOMId(rs.getString("UOM_ID"));
					itemDTO.setUOMName(rs.getString("UOM_NAME"));
					for (Integer productLevel : parentChildRelationMap.keySet()) {
						if (productLevel == Constants.DEPARTMENTLEVELID)
							itemDTO.setDeptProductId(rs.getInt("P_ID_" + productLevelPointer.get(productLevel)));
						else if (productLevel == Constants.PORTFOLIO)
							itemDTO.setPortfolioProductId(rs.getInt("P_ID_" + productLevelPointer.get(productLevel)));
						else if (productLevel == Constants.CATEGORYLEVELID)
							itemDTO.setCategoryProductId(rs.getInt("P_ID_" + productLevelPointer.get(productLevel)));
						else if (productLevel == Constants.SUBCATEGORYLEVELID)
							itemDTO.setSubCatProductId(rs.getInt("P_ID_" + productLevelPointer.get(productLevel)));
						else if (productLevel == Constants.SEGMENTLEVELID)
							itemDTO.setSegmentProductId(rs.getInt("P_ID_" + productLevelPointer.get(productLevel)));
						else if (productLevel == Constants.ITEMLEVELID)
							itemDTO.setItemCode(rs.getInt("ITEM_CODE"));
					}
//					itemDTO.setChildLocationLevelId(Constants.STORE_LEVEL_ID);
//					itemDTO.setChildLocationId(rs.getInt("LEVEL_ID"));
					itemDTO.setItemCode(rs.getInt("ITEM_CODE"));
//					itemDTO.setVendorId(rs.getInt("VENDOR_ID"));
//					if (rs.getObject("DIST_FLAG") != null)
//						itemDTO.setDistFlag(rs.getString("DIST_FLAG").charAt(0));

					if (itemDTO.getDeptProductId() > 0)
						itemList.add(itemDTO);
				}
			}
		} catch (Exception | GeneralException exception) {
			throw new OfferManagementException("Error in getAuthorizedItems() - " + exception,
					RecommendationErrorCode.DB_GET_AUTHORIZED_ITEMS);
		} finally {
			if (rs != null)
				PristineDBUtil.close(rs);
			if (stmt != null)
				PristineDBUtil.close(stmt);
		}
		return itemList;
	}
	
	private String queryToGetDistinctAuthorizedItems(int productLevelId, int productId, String priceZoneStoreIds,
			boolean isItemListAlreadyKnown, List<Integer> items) {
		StringBuffer subQuery = new StringBuffer("");
		subQuery.append(" SELECT DISTINCT(ITEM_CODE) AS ITEM_CODE  FROM STORE_ITEM_MAP SIM ");
		subQuery.append(" LEFT JOIN RETAIL_PRICE_ZONE RPZ2 ON RPZ2.PRICE_ZONE_ID =  SIM.PRICE_ZONE_ID ");
		subQuery.append(" WHERE SIM.LEVEL_TYPE_ID = 2 AND SIM.LEVEL_ID IN ");
		subQuery.append(" ( ");
		subQuery.append(priceZoneStoreIds);
		subQuery.append(" ) ");
		subQuery.append(" AND SIM.ITEM_CODE IN (");
		if (!isItemListAlreadyKnown) {
			subQuery.append(" SELECT CHILD_PRODUCT_ID FROM (SELECT CHILD_PRODUCT_ID,CHILD_PRODUCT_LEVEL_ID FROM PRODUCT_GROUP_RELATION PGR ");
			subQuery.append(getQueryCondition(productLevelId, productId));
			subQuery.append(" ) WHERE CHILD_PRODUCT_LEVEL_ID = 1) ");
		} else {
			subQuery.append(PRCommonUtil.getCommaSeperatedStringFromIntArray(items) + ")");
		}
		subQuery.append(" AND SIM.IS_AUTHORIZED = 'Y' ");
		return subQuery.toString();
	}
	
	private List<Integer> getPrimaryAndMatcingZoneIds(Connection conn, int primaryZoneId) throws GeneralException {
		List<PriceZoneDTO> matchingZones = new ProductLocationMappingDAO().getMatchingZones(conn, primaryZoneId);
		List<Integer> priceZoneIds = new ArrayList<Integer>();

		priceZoneIds.add(primaryZoneId);
		for (PriceZoneDTO priceZoneDTO : matchingZones) {
			priceZoneIds.add(priceZoneDTO.getPriceZoneId());
		}

		return priceZoneIds;
	}
	
	/**
	 * 
	 * @param conn
	 * @param inputDTO
	 * @param priceZoneStores
	 * @param isItemListAlreadyKnown
	 * @param items
	 * @return list of items
	 * @throws OfferManagementException
	 */
	public List<PRItemDTO> getItemsForPromoRecommendation(Connection conn, PRStrategyDTO inputDTO,
			List<Integer> priceZoneStores, boolean isItemListAlreadyKnown, List<Integer> items)
			throws OfferManagementException {
		ProductService productService = new ProductService();
		int productLevelId = inputDTO.getProductLevelId();
		int productId = inputDTO.getProductId();
		PreparedStatement stmt = null;
		ResultSet rs = null;
		List<PRItemDTO> itemList = new ArrayList<PRItemDTO>();
		
		try {
			HashMap<Integer, Integer> parentChildRelationMap = productService.getProductLevelRelationMap(conn, productLevelId);
			HashMap<Integer, Integer> productLevelPointer = productService.getProductLevelPointer(conn, productLevelId);
			int counter = 0;
			String sql = null;
			String storeIds = "";
			
			if (priceZoneStores.size() > 0) {
				// sql = GET_AUTHORIZED_ITEMS_ZONE_AND_STORE;
				sql = GET_ITEMS_FOR_PROMO_RECOMMENDATION;
				storeIds = PRCommonUtil.getCommaSeperatedStringFromIntArray(priceZoneStores);
				//sql = sql.replaceAll("%STORE_ID%", storeIds);
				//sql = sql.replaceAll("%PRODUCT_CONDITION1%", getQueryCondition(productLevelId, productId));
				sql = sql.replaceAll("%AUTHORIZED_ITEM_QUERY%", queryToGetItemsFromStoreItemMap(conn, productLevelId, productId, storeIds,
						isItemListAlreadyKnown, items, inputDTO.getLocationId(), inputDTO.getLocationLevelId()));
				sql = sql.replaceAll("%PRODUCT_CONDITION2%", getProductHierarchy(conn, productLevelId, productService));

				logger.debug("Item Fetching Query: " + sql);

				stmt = conn.prepareStatement(sql);
				if (!isItemListAlreadyKnown) {
					if (productLevelId > 1) {
						stmt.setInt(++counter, productLevelId);
						if (productId > 0) {
							stmt.setLong(++counter, productId);
						}
					} else if (productLevelId == 1) {
						stmt.setLong(++counter, productId);
					}
				}

				stmt.setFetchSize(200000);
				rs = stmt.executeQuery();

				while (rs.next()) {
					int itemCode = rs.getInt("ITEM_CODE");
					PRItemDTO itemDTO = new PRItemDTO();
					itemDTO.setItemCode(itemCode);
					itemDTO.setItemName(rs.getString("ITEM_NAME"));
					itemDTO.setRetLirName(rs.getString("RET_LIR_NAME"));
					itemDTO.setRetLirId(rs.getInt("RET_LIR_ID"));
					// itemDTO.setRetLirItemCode(rs.getInt("RET_LIR_ITEM_CODE"));
					itemDTO.setItemSize(rs.getDouble("ITEM_SIZE"));
					itemDTO.setRetailerItemCode(rs.getString("RETAILER_ITEM_CODE"));
					itemDTO.setUpc(rs.getString("UPC")); // Used during
															// prediction
					//String lirInd = rs.getString("LIR_IND");
					itemDTO.setBrandId(rs.getInt("BRAND_ID"));
					itemDTO.setBrandName(rs.getString("BRAND_NAME"));
					
					if (rs.getObject("PREPRICED_IND") != null)
						itemDTO.setIsPrePriced(rs.getInt("PREPRICED_IND"));
					
					if (rs.getObject("SHIPPER_FLAG") != null) {
						if(String.valueOf(Constants.YES).equalsIgnoreCase(rs.getString("SHIPPER_FLAG"))){
							itemDTO.setShipperItem(true);
						}
					}

//					if (String.valueOf(Constants.YES).equalsIgnoreCase(lirInd)) {
//						itemDTO.setLir(true);
//					}

					itemDTO.setUOMId(rs.getString("UOM_ID"));
					itemDTO.setUOMName(rs.getString("UOM_NAME"));
					for (Integer productLevel : parentChildRelationMap.keySet()) {
						if (productLevel == Constants.DEPARTMENTLEVELID)
							itemDTO.setDeptProductId(rs.getInt("P_ID_" + productLevelPointer.get(productLevel)));
						else if (productLevel == Constants.PORTFOLIO)
							itemDTO.setPortfolioProductId(rs.getInt("P_ID_" + productLevelPointer.get(productLevel)));
						else if (productLevel == Constants.CATEGORYLEVELID)
							itemDTO.setCategoryProductId(rs.getInt("P_ID_" + productLevelPointer.get(productLevel)));
						else if (productLevel == Constants.SUBCATEGORYLEVELID)
							itemDTO.setSubCatProductId(rs.getInt("P_ID_" + productLevelPointer.get(productLevel)));
						else if (productLevel == Constants.SEGMENTLEVELID)
							itemDTO.setSegmentProductId(rs.getInt("P_ID_" + productLevelPointer.get(productLevel)));
						else if (productLevel == Constants.ITEMLEVELID)
							itemDTO.setItemCode(rs.getInt("ITEM_CODE"));
					}
					itemDTO.setChildLocationLevelId(Constants.STORE_LEVEL_ID);
					itemDTO.setChildLocationId(rs.getInt("LEVEL_ID"));
					itemDTO.setItemCode(rs.getInt("ITEM_CODE"));
					itemDTO.setVendorId(rs.getInt("VENDOR_ID"));
					if (rs.getObject("DIST_FLAG") != null)
						itemDTO.setDistFlag(rs.getString("DIST_FLAG").charAt(0));
					
					// Dinesh:: 01st Mar 2018, Consider item is in DSD Zone if DIST FLAG = "D"(Related to GE)
					if (rs.getObject("DIST_FLAG") != null && rs.getString("DIST_FLAG").trim().equals(String.valueOf(Constants.DSD))){
						itemDTO.setDSDItem(true);
					}

					boolean useStoreItemMapForZone = Boolean.parseBoolean
							(PropertyManager.getProperty("USE_ZONE_FROM_STORE_ITEM_MAP", "FALSE"));
					if(useStoreItemMapForZone)
						itemDTO.setPriceZoneId(rs.getInt("ALT_PRICE_ZONE_ID")); 
					else
						itemDTO.setPriceZoneId(rs.getInt("PRICE_ZONE_ID"));
					if(useStoreItemMapForZone){
						if (rs.getObject("ALT_ZONE_NUM") != null)
							itemDTO.setPriceZoneNo(rs.getString("ALT_ZONE_NUM"));
					}else{
						if (rs.getObject("ZONE_NUM") != null)
							itemDTO.setPriceZoneNo(rs.getString("ZONE_NUM"));
					}
					
					if (rs.getObject("ACTIVE_INDICATOR") != null) {
						if(String.valueOf(Constants.YES).equalsIgnoreCase(rs.getString("ACTIVE_INDICATOR"))){
							itemDTO.setActive(true);
						}
					}
					
					if (rs.getObject("IS_AUTHORIZED") != null) {
						if(String.valueOf(Constants.YES).equalsIgnoreCase(rs.getString("IS_AUTHORIZED"))){
							itemDTO.setAuthorized(true);
						}
					}
					
					if (itemDTO.getDeptProductId() > 0)
						itemList.add(itemDTO);
				}
			}
		} catch (Exception | GeneralException exception) {
			throw new OfferManagementException("Error in getAuthorizedItemsOfZoneAndStore() - " + exception, 
					RecommendationErrorCode.DB_GET_AUTHORIZED_ITEMS);
		} finally {
			if(rs != null)
				PristineDBUtil.close(rs);
			if(stmt != null)
				PristineDBUtil.close(stmt);
		}
		return itemList;
	}
	
	private String queryToGetItemsFromStoreItemMap(Connection conn, int productLevelId, int productId, String priceZoneStoreIds,
			boolean isItemListAlreadyKnown, List<Integer> items, int locationId, int locationLevelId) throws GeneralException {
		StringBuffer subQuery = new StringBuffer("");
		subQuery.append(" SELECT ITEM_CODE, LEVEL_ID, VENDOR_ID, DIST_FLAG, COST_INDICATOR, ");
		subQuery.append(" SIM.PRICE_ZONE_ID AS ALT_PRICE_ZONE_ID, RPZ2.ZONE_NUM AS ALT_ZONE_NUM, IS_AUTHORIZED FROM STORE_ITEM_MAP SIM "); 
		subQuery.append(" LEFT JOIN RETAIL_PRICE_ZONE RPZ2 ON RPZ2.PRICE_ZONE_ID =  SIM.PRICE_ZONE_ID ");
		subQuery.append(" WHERE SIM.LEVEL_TYPE_ID = 2 AND SIM.LEVEL_ID IN ");
		subQuery.append(" ( ");
		subQuery.append(priceZoneStoreIds);
		subQuery.append(" ) ");
		
		//NU::  12th Feb 2018, temporary code block to handle duplicating of items across the
		//different pricing group. GE reported that items which are not supposed to show in 106-1 is shown.
		//the items are present in both 106-1 and 106-3
		/********************/
		boolean useStoreItemMapForZone = Boolean.parseBoolean(PropertyManager.getProperty("USE_ZONE_FROM_STORE_ITEM_MAP", "FALSE"));
		if (useStoreItemMapForZone) {
			//NU:: 15th Feb 2018, GE wanted to combine multiple zones in to single zone 
			//e.g. 106-1,24-1,26-1 to 106-1. For GE while picking authorized item price zone id
			//is also checked, this will ignore items from 24-1 and 26-1, in order to get those
			//zone items, those zones will also be included in the IN clause
			//List<Integer> primaryAndMatchingZoneIds = getPrimaryAndMatcingZoneIds(conn, locationId);
			//String locationIds = primaryAndMatchingZoneIds.stream().map(loc-> String.valueOf(loc)).collect(Collectors.joining(","));
			//Dinesh:: 28th Feb 2018, Query changes to include DSD zones along with Warehouese zones in recommendation
			subQuery.append(" AND (SIM.PRICE_ZONE_ID IN (SELECT LOCATION_ID FROM PR_PRODUCT_LOCATION_MAPPING WHERE PRODUCT_LEVEL_ID = ");
			subQuery.append(productLevelId).append("  AND PRODUCT_ID = ").append(productId);
			subQuery.append(" AND PARENT_LOCATION_ID IN (").append(locationId).append(" ) AND PARENT_LOCATION_LEVEL_ID = ").append(locationLevelId);
			subQuery.append(") OR SIM.PRICE_ZONE_ID IN ( ").append(locationId).append("))");
		}
		/********************/
		
		subQuery.append(" AND SIM.ITEM_CODE IN (");
		if (!isItemListAlreadyKnown) {
			subQuery.append(" SELECT CHILD_PRODUCT_ID FROM (SELECT CHILD_PRODUCT_ID,CHILD_PRODUCT_LEVEL_ID FROM PRODUCT_GROUP_RELATION PGR ");
			subQuery.append(getQueryCondition(productLevelId, productId));
			subQuery.append(" ) WHERE CHILD_PRODUCT_LEVEL_ID = 1) ");
		} else {
			subQuery.append(PRCommonUtil.getCommaSeperatedStringFromIntArray(items) + ")");
		}
		subQuery.append(" AND SIM.IS_AUTHORIZED = 'Y' ");
		return subQuery.toString();
	}
	
	
	/**
	 * 
	 * @param conn
	 * @param inputDTO
	 * @param priceZoneStores
	 * @param isItemListAlreadyKnown
	 * @param items
	 * @param runId
	 * @return items from past recommendation
	 * @throws OfferManagementException
	 */
	public List<PRItemDTO> getItemsFromPastRec(Connection conn, PRStrategyDTO inputDTO, long runId)
			throws OfferManagementException {
		ProductService productService = new ProductService();
		int productLevelId = inputDTO.getProductLevelId();
		int productId = inputDTO.getProductId();
		PreparedStatement stmt = null;
		ResultSet rs = null;
		List<PRItemDTO> itemList = new ArrayList<PRItemDTO>();

		try {
			HashMap<Integer, Integer> parentChildRelationMap = productService.getProductLevelRelationMapRec(conn,
					productLevelId);
			HashMap<Integer, Integer> productLevelPointer = productService.getProductLevelPointerRec(conn, productLevelId);
			int counter = 0;
			String sql = null;
			// sql = GET_AUTHORIZED_ITEMS_ZONE_AND_STORE;
			sql = GET_ITEMS_FROM_PAST_RECOMMENDATION;
			sql = sql.replaceAll("%PRODUCT_CONDITION2%", getProductHierarchy(conn, productLevelId, productService));

			logger.debug("Item Fetching Query: " + sql);

			stmt = conn.prepareStatement(sql);
			stmt.setLong(++counter, runId);
			

			stmt.setFetchSize(200000);
			rs = stmt.executeQuery();

			while (rs.next()) {
				int itemCode = rs.getInt("ITEM_CODE");
				PRItemDTO itemDTO = new PRItemDTO();
				itemDTO.setItemCode(itemCode);
				itemDTO.setItemName(rs.getString("ITEM_NAME"));
				itemDTO.setRetLirName(rs.getString("RET_LIR_NAME"));
				itemDTO.setRetLirId(rs.getInt("RET_LIR_ID"));
				// itemDTO.setRetLirItemCode(rs.getInt("RET_LIR_ITEM_CODE"));
				itemDTO.setItemSize(rs.getDouble("ITEM_SIZE"));
				itemDTO.setRetailerItemCode(rs.getString("RETAILER_ITEM_CODE"));
				itemDTO.setUpc(rs.getString("UPC")); // Used during
														// prediction
				// String lirInd = rs.getString("LIR_IND");
				itemDTO.setBrandId(rs.getInt("BRAND_ID"));
				itemDTO.setBrandName(rs.getString("BRAND_NAME"));

				if (rs.getObject("PREPRICED_IND") != null)
					itemDTO.setIsPrePriced(rs.getInt("PREPRICED_IND"));

				if (rs.getObject("SHIPPER_FLAG") != null) {
					if (String.valueOf(Constants.YES).equalsIgnoreCase(rs.getString("SHIPPER_FLAG"))) {
						itemDTO.setShipperItem(true);
					}
				}

				// if (String.valueOf(Constants.YES).equalsIgnoreCase(lirInd)) {
				// itemDTO.setLir(true);
				// }

				itemDTO.setUOMId(rs.getString("UOM_ID"));
				itemDTO.setUOMName(rs.getString("UOM_NAME"));
				for (Integer productLevel : parentChildRelationMap.keySet()) {
					if (productLevel == Constants.DEPARTMENTLEVELID)
						itemDTO.setDeptProductId(rs.getInt("P_ID_" + productLevelPointer.get(productLevel)));
					else if (productLevel == Constants.PORTFOLIO)
						itemDTO.setPortfolioProductId(rs.getInt("P_ID_" + productLevelPointer.get(productLevel)));
					else if (productLevel == Constants.CATEGORYLEVELID)
						itemDTO.setCategoryProductId(rs.getInt("P_ID_" + productLevelPointer.get(productLevel)));
					else if (productLevel == Constants.SUBCATEGORYLEVELID)
						itemDTO.setSubCatProductId(rs.getInt("P_ID_" + productLevelPointer.get(productLevel)));
					else if (productLevel == Constants.SEGMENTLEVELID)
						itemDTO.setSegmentProductId(rs.getInt("P_ID_" + productLevelPointer.get(productLevel)));
					else if (productLevel == Constants.ITEMLEVELID)
						itemDTO.setItemCode(rs.getInt("ITEM_CODE"));
				}
				
				
				itemDTO.setItemCode(rs.getInt("ITEM_CODE"));
				itemDTO.setVendorId(rs.getInt("VENDOR_ID"));
				if (rs.getObject("DIST_FLAG") != null)
					itemDTO.setDistFlag(rs.getString("DIST_FLAG").charAt(0));

				// Dinesh:: 01st Mar 2018, Consider item is in DSD Zone if DIST FLAG = "D"(Related to GE)
				if (rs.getObject("DIST_FLAG") != null
						&& rs.getString("DIST_FLAG").trim().equals(String.valueOf(Constants.DSD))) {
					itemDTO.setDSDItem(true);
				}

				boolean useStoreItemMapForZone = Boolean
						.parseBoolean(PropertyManager.getProperty("USE_ZONE_FROM_STORE_ITEM_MAP", "FALSE"));
				if (useStoreItemMapForZone)
					itemDTO.setPriceZoneId(rs.getInt("ALT_PRICE_ZONE_ID"));
				else
					itemDTO.setPriceZoneId(rs.getInt("PRICE_ZONE_ID"));
				if (useStoreItemMapForZone) {
					if (rs.getObject("ALT_ZONE_NUM") != null)
						itemDTO.setPriceZoneNo(rs.getString("ALT_ZONE_NUM"));
				} else {
					if (rs.getObject("ZONE_NUM") != null)
						itemDTO.setPriceZoneNo(rs.getString("ZONE_NUM"));
				}

				if (rs.getObject("ACTIVE_INDICATOR") != null) {
					if (String.valueOf(Constants.YES).equalsIgnoreCase(rs.getString("ACTIVE_INDICATOR"))) {
						itemDTO.setActive(true);
					}
				}

				itemDTO.setAuthorized(true);

				// Additional attributes
				itemDTO.setUserAttr3(rs.getString("USER_ATTR_3"));
				itemDTO.setUserAttr4(rs.getString("USER_ATTR_4"));
				itemDTO.setUserAttr5(rs.getString("USER_ATTR_5"));
				itemDTO.setUserAttr6(rs.getString("USER_ATTR_6"));
				itemDTO.setUserAttr7(rs.getString("USER_ATTR_7"));
				itemDTO.setUserAttr8(rs.getString("USER_ATTR_8"));
				itemDTO.setUserAttr9(rs.getString("USER_ATTR_9"));
				itemDTO.setUserAttr10(rs.getString("USER_ATTR_10"));
				itemDTO.setUserAttr11(rs.getString("USER_ATTR_11"));
				itemDTO.setUserAttr12(rs.getString("USER_ATTR_12"));
				itemDTO.setUserAttr13(rs.getString("USER_ATTR_13"));
				itemDTO.setUserAttr14(rs.getString("USER_ATTR_14"));
				itemDTO.setItemSetupDate(rs.getString("ITEM_SETUP_DATE"));
				
				if (itemDTO.getDeptProductId() > 0 && itemDTO.getCategoryProductId() == productId)
					itemList.add(itemDTO);
			}
		} catch (Exception | GeneralException exception) {
			throw new OfferManagementException("Error in getAuthorizedItemsOfZoneAndStore() - " + exception,
					RecommendationErrorCode.DB_GET_AUTHORIZED_ITEMS);
		} finally {
			if (rs != null)
				PristineDBUtil.close(rs);
			if (stmt != null)
				PristineDBUtil.close(stmt);
		}
		return itemList;
	}
	
	/**
	 * 
	 * @param conn
	 * @param inputDTO
	 * @return past recommendation run id
	 * @throws OfferManagementException
	 */
	public long getPastRecommendationRunId(Connection conn, PRStrategyDTO inputDTO) throws OfferManagementException {
		long runId = -1;
		PreparedStatement statement = null;
		ResultSet rs= null;
		try {
			
			StringBuilder sb = new StringBuilder();
			sb.append(" SELECT MAX(RUN_ID) AS RUN_ID FROM PR_RECOMMENDATION_RUN_HEADER WHERE ");
			sb.append(" LOCATION_LEVEL_ID = ? ");
			sb.append(" AND LOCATION_ID = ? ");
			sb.append(" AND PRODUCT_LEVEL_ID = ? ");
			sb.append(" AND PRODUCT_ID = ? ");
			sb.append(" AND RUN_TYPE <> '").append(PRConstants.RUN_TYPE_TEMP).append("'");
			sb.append(" AND START_RUN_TIME IS NOT NULL");
			sb.append(" AND END_RUN_TIME IS NOT NULL");
			
			statement = conn.prepareStatement(sb.toString());
			int counter = 0;
			statement.setInt(++counter, inputDTO.getLocationLevelId());
			statement.setInt(++counter, inputDTO.getLocationId());
			statement.setInt(++counter, inputDTO.getProductLevelId());
			statement.setInt(++counter, inputDTO.getProductId());
			
			rs = statement.executeQuery();
			if(rs.next()) {
				runId = rs.getLong("RUN_ID");
			}
			
		}catch(Exception e) {
			throw new OfferManagementException("Error in getAuthorizedItemsOfZoneAndStore() - " + e, 
					RecommendationErrorCode.DB_GET_AUTHORIZED_ITEMS);
		}finally {
			if(rs != null)
				PristineDBUtil.close(rs);
			if(statement != null)
				PristineDBUtil.close(statement);
		}
		return runId;
	}
	
	public List<PRItemDTO> getAllItems(Connection conn, PRStrategyDTO inputDTO,
			List<Integer> priceZoneStores, boolean isItemListAlreadyKnown, List<Integer> items)
			throws OfferManagementException {
		ProductService productService = new ProductService();
		int productLevelId = inputDTO.getProductLevelId();
		int productId = inputDTO.getProductId();
		PreparedStatement stmt = null;
		ResultSet rs = null;
		List<PRItemDTO> itemList = new ArrayList<PRItemDTO>();
		
		try {
			HashMap<Integer, Integer> parentChildRelationMap = productService.getProductLevelRelationMapRec(conn, productLevelId);
			HashMap<Integer, Integer> productLevelPointer = productService.getProductLevelPointerRec(conn, productLevelId);
			int counter = 0;
			String sql = null;
			
			if (priceZoneStores.size() > 0) {
				// sql = GET_AUTHORIZED_ITEMS_ZONE_AND_STORE;
				sql = GET_ALL_ACTIVE_ITEMS;
				sql = sql.replaceAll("%PRODUCT_CONDITION2%", getProductQuery(conn, productLevelId, productService, productLevelPointer));

				logger.info("Item Fetching Query: " + sql);

				stmt = conn.prepareStatement(sql);
				stmt.setInt(++counter, productLevelId);
				stmt.setInt(++counter, productId);
				
				rs = stmt.executeQuery();

				while (rs.next()) {
					int itemCode = rs.getInt("ITEM_CODE");
					PRItemDTO itemDTO = new PRItemDTO();
					itemDTO.setItemCode(itemCode);
					itemDTO.setItemName(rs.getString("ITEM_NAME"));
					itemDTO.setRetLirName(rs.getString("RET_LIR_NAME"));
					itemDTO.setRetLirId(rs.getInt("RET_LIR_ID"));
					// itemDTO.setRetLirItemCode(rs.getInt("RET_LIR_ITEM_CODE"));
					itemDTO.setItemSize(rs.getDouble("ITEM_SIZE"));
					itemDTO.setRetailerItemCode(rs.getString("RETAILER_ITEM_CODE"));
					itemDTO.setUpc(rs.getString("UPC")); // Used during
															// prediction
					//String lirInd = rs.getString("LIR_IND");
					itemDTO.setBrandId(rs.getInt("BRAND_ID"));
					itemDTO.setBrandName(rs.getString("BRAND_NAME"));
					
					if (rs.getObject("PREPRICED_IND") != null)
						itemDTO.setIsPrePriced(rs.getInt("PREPRICED_IND"));
					
					if (rs.getObject("SHIPPER_FLAG") != null) {
						if(String.valueOf(Constants.YES).equalsIgnoreCase(rs.getString("SHIPPER_FLAG"))){
							itemDTO.setShipperItem(true);
						}
					}

//					if (String.valueOf(Constants.YES).equalsIgnoreCase(lirInd)) {
//						itemDTO.setLir(true);
//					}

					itemDTO.setUOMId(rs.getString("UOM_ID"));
					itemDTO.setUOMName(rs.getString("UOM_NAME"));
					for (Integer productLevel : parentChildRelationMap.keySet()) {
						if (productLevel == Constants.DEPARTMENTLEVELID)
							itemDTO.setDeptProductId(rs.getInt("P_ID_" + productLevelPointer.get(productLevel)));
						else if (productLevel == Constants.PORTFOLIO)
							itemDTO.setPortfolioProductId(rs.getInt("P_ID_" + productLevelPointer.get(productLevel)));
						else if (productLevel == Constants.CATEGORYLEVELID)
							itemDTO.setCategoryProductId(rs.getInt("P_ID_" + productLevelPointer.get(productLevel)));
						else if (productLevel == Constants.RECOMMENDATIONUNIT)
							itemDTO.setRecUnitProductId(rs.getInt("P_ID_" + productLevelPointer.get(productLevel)));
						else if (productLevel == Constants.SUBCATEGORYLEVELID)
							itemDTO.setSubCatProductId(rs.getInt("P_ID_" + productLevelPointer.get(productLevel)));
						else if (productLevel == Constants.SEGMENTLEVELID)
							itemDTO.setSegmentProductId(rs.getInt("P_ID_" + productLevelPointer.get(productLevel)));
						else if (productLevel == Constants.ITEMLEVELID)
							itemDTO.setItemCode(rs.getInt("ITEM_CODE"));
					}
					itemDTO.setItemCode(rs.getInt("ITEM_CODE"));
					
					if (rs.getObject("ACTIVE_INDICATOR") != null) {
						if(String.valueOf(Constants.YES).equalsIgnoreCase(rs.getString("ACTIVE_INDICATOR"))){
							itemDTO.setActive(true);
						}
					}
					
					if (rs.getObject("SEND_TO_PREDICTION") != null) {
						if (String.valueOf(Constants.YES).equalsIgnoreCase(rs.getString("SEND_TO_PREDICTION"))) {
							itemDTO.setSendToPrediction(true);
						} else
							itemDTO.setSendToPrediction(false);
					} else {
						itemDTO.setSendToPrediction(true);
					}
				
					itemDTO.setAuthorized(true);
					
					itemDTO.setPriceZoneId(inputDTO.getLocationId());
					itemDTO.setPriceZoneNo(inputDTO.getZoneNum());
					// Additional attributes
					itemDTO.setUserAttr3(rs.getString("USER_ATTR_3"));
					itemDTO.setUserAttr4(rs.getString("USER_ATTR_4"));
					itemDTO.setUserAttr5(rs.getString("USER_ATTR_5"));
					itemDTO.setUserAttr6(rs.getString("USER_ATTR_6"));
					itemDTO.setUserAttr7(rs.getString("USER_ATTR_7"));
					itemDTO.setUserAttr8(rs.getString("USER_ATTR_8"));
					itemDTO.setUserAttr9(rs.getString("USER_ATTR_9"));
					itemDTO.setUserAttr10(rs.getString("USER_ATTR_10"));
					itemDTO.setUserAttr11(rs.getString("USER_ATTR_11"));
					itemDTO.setUserAttr12(rs.getString("USER_ATTR_12"));
					itemDTO.setUserAttr13(rs.getString("USER_ATTR_13"));
					itemDTO.setUserAttr14(rs.getString("USER_ATTR_14"));
					itemDTO.setUserAttr15(rs.getString("USER_ATTR_15"));//FReight Charges for AZ
					itemDTO.setItemSetupDate(rs.getString("ITEM_SETUP_DATE"));
					itemDTO.setFamilyName(rs.getString("USER_ATTR_14"));
					if (itemDTO.getDeptProductId() > 0)
						itemList.add(itemDTO);
				}
			}
		} catch (Exception | GeneralException exception) {
			throw new OfferManagementException("Error in getAuthorizedItemsOfZoneAndStore() - " + exception, 
					RecommendationErrorCode.DB_GET_AUTHORIZED_ITEMS);
		} finally {
			if(rs != null)
				PristineDBUtil.close(rs);
			if(stmt != null)
				PristineDBUtil.close(stmt);
		}
		
		logger.info("Total items"+ itemList.size());
		return itemList;
	}
	
	public String getProductQuery(Connection conn, int productLevel, ProductService productService, 
			HashMap<Integer, Integer> productLevelPointer) throws GeneralException {
		StringBuffer sb = new StringBuffer();

		if (productService.productHierarchy.size() == 0)
			productService.getChildHierarchyRec(conn, productLevel);

		int productHierarchyCount = productService.productHierarchy.size();

		sb.append("(SELECT ITEM_CODE,");
		for (int i = 1; i < productHierarchyCount; i++) {
			//sb.append("P_L_ID_" + i + ", P_ID_" + i + ", P" + i + ".NAME NAME_" + i + ",");
			sb.append("P_L_ID_" + i + ", P_ID_" + i + ",");
		}
		sb.delete((sb.length() - 1), sb.length());
		sb.append(" FROM ");

//		for (int s = 1; s < productHierarchyCount; s++) {
//			sb.append("( ");
//		}

//		sb.append(" SELECT ");
		sb.append(" (SELECT ");
		for (int j = 1; j < productHierarchyCount; j++) {
			if (j == 1) {
				sb.append(" A" + j + ".PRODUCT_LEVEL_ID P_L_ID_" + j + ", A" + j + ".PRODUCT_ID P_ID_" + j + ", A" + j
						+ ".CHILD_PRODUCT_ID ITEM_CODE ,");
			} else {
				sb.append(" A" + j + ".PRODUCT_LEVEL_ID P_L_ID_" + j + ", A" + j + ".PRODUCT_ID P_ID_" + j + ", A" + j
						+ ".CHILD_PRODUCT_ID CHILD_P_ID_" + j + ",");
			}
		}
		sb.delete((sb.length() - 1), sb.length());

		sb.append(" FROM ");
		for (int x = 1; x < productHierarchyCount; x++) {
			int loop = productHierarchyCount - 1;
			if (x > loop) {
				break;
			}
			sb.append("( ");
		}

		for (int k = 1; k <= productHierarchyCount; k++) {
			if (k == 1) {
				sb.append(" PRODUCT_GROUP_RELATION_REC A" + k + " ");
			} else if (k == 2) {
				sb.append(" LEFT JOIN PRODUCT_GROUP_RELATION_REC A" + k + " ");
				break;
			}
		}

		String firstLevel = "";
		String nextLevel = "";
		for (int a = 1; a < productHierarchyCount + 1; a++) {
			if (a == 1) {
				firstLevel = "A" + a;
			} else if (a == 2) {
				nextLevel = "A" + a;
				break;
			}
		}
		sb.append("ON " + firstLevel + ".PRODUCT_ID = " + nextLevel + ".CHILD_PRODUCT_ID AND " + firstLevel + ".PRODUCT_LEVEL_ID = "
				+ nextLevel + ".CHILD_PRODUCT_LEVEL_ID) ");

		for (int b = 1; b < productHierarchyCount + 1; b++) {
			int loopCheck = b;
			if (b >= 3) {
				sb.append("LEFT JOIN PRODUCT_GROUP_RELATION_REC A" + b + " ");
				sb.append(" ON A" + (loopCheck - 1) + ".PRODUCT_ID = A" + b + ".CHILD_PRODUCT_ID AND A" + (loopCheck - 1)
						+ ".PRODUCT_LEVEL_ID = A" + b + ".CHILD_PRODUCT_LEVEL_ID ) ");
			}
		}

		for (int c = 1; c < productHierarchyCount + 1; c++) {
			if (c == 1) {
				sb.append("WHERE A" + c + ".CHILD_PRODUCT_LEVEL_ID = 1 ");
				break;
			}
		}
		sb.append(") ");

//		for (int d = 1; d < productHierarchyCount; d++) {
//			sb.append("LEFT JOIN PRODUCT_GROUP P" + d + " ON P_ID_" + d + " = P" + d + ".PRODUCT_ID AND P_L_ID_" + d + " = P" + d
//					+ ".PRODUCT_LEVEL_ID ) ");
//		}
		//sb.append(" PRODUCT_HIERARCHY ON PRODUCT_HIERARCHY.ITEM_CODE_1 = STORE_ITEM_MAP.ITEM_CODE ");
		//sb.append(" AND PRODUCT_HIERARCHY.P_L_ID_");
		//sb.append(productHierarchyCount-1).append(" = ").append(Constants.DEPARTMENTLEVELID);
		
		sb.append(" WHERE P_L_ID_").append(productHierarchyCount-1).append(" = ").append(Constants.DEPARTMENTLEVELID);
		sb.append(" AND P_L_ID_").append(productLevelPointer.get(productLevel)).append(" = ? ");
		sb.append(" AND P_ID_").append(productLevelPointer.get(productLevel)).append(" = ? ");
		sb.append(" ) PRODUCT_HIERARCHY ");
		

		return sb.toString();
	}
	
	public List<PRItemDTO> getRUofItems(Connection conn) throws GeneralException{
		List<PRItemDTO> itemsAndRU = new ArrayList<>();
		
		StringBuffer sb = new StringBuffer();
		sb.append(" SELECT ITEM_CODE, RETAILER_ITEM_CODE, PG_RU.NAME RU_NAME, PG_RU.PRODUCT_ID RU_ID FROM ITEM_LOOKUP IL ");
		sb.append(" LEFT JOIN (SELECT CHILD_PRODUCT_ID,PRODUCT_ID SEGMENT_ID FROM PRODUCT_GROUP_RELATION_REC "); 
		sb.append(" WHERE CHILD_PRODUCT_LEVEL_ID = 1 AND PRODUCT_LEVEL_ID=2 ) SEGMENT "); 
		sb.append(" ON SEGMENT.CHILD_PRODUCT_ID = IL.ITEM_CODE LEFT JOIN PRODUCT_GROUP PG_SEG  ON PG_SEG.PRODUCT_ID ");  
		sb.append(" = SEGMENT.SEGMENT_ID  AND PG_SEG.PRODUCT_LEVEL_ID = 2 "); 
		sb.append(" LEFT JOIN (SELECT CHILD_PRODUCT_ID,PRODUCT_ID SUB_CATEGORY_ID FROM PRODUCT_GROUP_RELATION_REC "); 
		sb.append(" WHERE CHILD_PRODUCT_LEVEL_ID = 2 AND PRODUCT_LEVEL_ID=3 ) SUB_CATS  ");
		sb.append(" ON SUB_CATS.CHILD_PRODUCT_ID = SEGMENT.SEGMENT_ID LEFT JOIN PRODUCT_GROUP PG_SC  ON PG_SC.PRODUCT_ID  "); 
		sb.append(" = SUB_CATS.SUB_CATEGORY_ID  AND PG_SC.PRODUCT_LEVEL_ID = 3 "); 
		sb.append(" LEFT JOIN (SELECT CHILD_PRODUCT_ID,PRODUCT_ID RU_ID FROM PRODUCT_GROUP_RELATION_REC "); 
		sb.append(" WHERE CHILD_PRODUCT_LEVEL_ID = 3 AND PRODUCT_LEVEL_ID=7 ) RU "); 
		sb.append(" ON RU.CHILD_PRODUCT_ID = SUB_CATS.SUB_CATEGORY_ID LEFT JOIN PRODUCT_GROUP PG_RU ");  
		sb.append(" ON PG_RU.PRODUCT_ID  = RU.RU_ID  AND PG_RU.PRODUCT_LEVEL_ID = 7 "); 
		sb.append(" LEFT JOIN (SELECT CHILD_PRODUCT_ID,PRODUCT_ID CATEGORY_ID FROM PRODUCT_GROUP_RELATION_REC "); 
		sb.append(" WHERE CHILD_PRODUCT_LEVEL_ID = 7 AND PRODUCT_LEVEL_ID= 4 ) CATS "); 
		sb.append(" ON CATS.CHILD_PRODUCT_ID = RU.RU_ID LEFT JOIN PRODUCT_GROUP PG_C  ON PG_C.PRODUCT_ID  = CATS.CATEGORY_ID ");  
		sb.append(" AND PG_C.PRODUCT_LEVEL_ID = 4 "); 
		sb.append(" LEFT JOIN (SELECT CHILD_PRODUCT_ID,PRODUCT_ID DEPARTMENT_ID FROM PRODUCT_GROUP_RELATION_REC "); 
		sb.append(" WHERE CHILD_PRODUCT_LEVEL_ID = 4 AND PRODUCT_LEVEL_ID= 5 ) DEPTS "); 
		sb.append(" ON DEPTS.CHILD_PRODUCT_ID = CATS.CATEGORY_ID LEFT JOIN PRODUCT_GROUP PG_D  ON PG_D.PRODUCT_ID ");  
		sb.append(" = DEPTS.DEPARTMENT_ID  AND PG_D.PRODUCT_LEVEL_ID = 5 ");
		

		CachedRowSet rs = PristineDBUtil.executeQuery(conn, sb, "getItemDetails");
		PRItemDTO item = null;

		try {
			while (rs.next()) {
				item = new PRItemDTO();
				item.setItemCode(rs.getInt("ITEM_CODE"));
				item.setRetailerItemCode(rs.getString("RETAILER_ITEM_CODE"));
				item.setRecommendationUnit(rs.getString("RU_NAME"));
				item.setRecommendationUnitId(rs.getInt("RU_ID"));
				itemsAndRU.add(item);
			}
		} catch (Exception e) {
			e.printStackTrace();
			throw new GeneralException("Cached Rowset Access Exception", e);
		}

		
		return itemsAndRU;
	}

	public List<ItemDTO> getCSVdata(Connection conn, String storeNo, String week) throws GeneralException {
		
		List<ItemDTO> cvsItems = new ArrayList<>();
		
		StringBuffer sb = new StringBuffer();
		sb.append("  ");
		

		CachedRowSet rs = PristineDBUtil.executeQuery(conn, sb, "getItemDetails");
		ItemDTO item = null;

		try {
			while (rs.next()) {
				item = new ItemDTO();
				cvsItems.add(item);
			}
		} catch (Exception e) {
			e.printStackTrace();
			throw new GeneralException("Cached Rowset Access Exception", e);
		}
		
		return cvsItems;
	}

	public List<ItemDTO> getItemData(Connection conn) throws GeneralException {
		
		List<ItemDTO> itemData = new ArrayList<>();
		
		StringBuffer sb = new StringBuffer();
		sb.append(" SELECT D.NAME AS MAJOR_CATEGORY, C.NAME AS CATEGORY, S.NAME AS SUB_CATEGORY, ");
		sb.append(" IL.RETAILER_ITEM_CODE AS ITEM_CODE, IL.ITEM_NAME, IL.ITEM_SIZE,U.NAME AS UOM, SEG.NAME AS SEGMENT_NAME, ");
		sb.append(" RIL.RET_LIR_NAME, RIL.RET_LIR_CODE, IL.UPC, B.BRAND_NAME ");
		sb.append(" FROM ITEM_LOOKUP IL LEFT JOIN DEPARTMENT D ON D.ID = IL.DEPT_ID JOIN ITEM_SEGMENT SEG ON SEG.ID = IL.SEGMENT_ID ");
		sb.append(" LEFT JOIN CATEGORY C ON C.ID = IL.CATEGORY_ID LEFT JOIN DEPARTMENT D ON D.ID = IL.DEPT_ID ");
		sb.append(" LEFT JOIN CATEGORY C ON C.ID = IL.CATEGORY_ID LEFT JOIN SUB_CATEGORY S ON S.ID = IL.SUB_CATEGORY_ID ");
		sb.append(" LEFT JOIN UOM_LOOKUP U ON U.ID = IL.UOM_ID LEFT JOIN RETAILER_LIKE_ITEM_GROUP RIL ON RIL.RET_LIR_ID = IL.RET_LIR_ID ");
		sb.append(" LEFT JOIN BRAND_LOOKUP B ON B.BRAND_ID = IL.BRAND_ID ");
		
		CachedRowSet rs = PristineDBUtil.executeQuery(conn, sb, "getItemData");
		ItemDTO item = null;

		try {
			while (rs.next()) {
				item = new ItemDTO();
				item.setDeptName("MAJOR_CATEGORY");
				item.setCatName("CATEGORY");
				item.setSubCatName("SUB_CATEGORY");
				item.setRetailerItemCode("ITEM_CODE");
				item.setRetailerName("ITEM_NAME");
				item.setSize("ITEM_SIZE");
				item.setUom("UOM");
				item.setSegmentName("SEGMENT_NAME");
				item.setBrandName("BRAND_NAME");
				itemData.add(item);
			}
		} catch (Exception e) {
			e.printStackTrace();
			throw new GeneralException("Cached Rowset Access Exception", e);
		}
		return itemData;
	}

		public HashMap<String, Integer> getActiveItems(Connection conn) throws GeneralException {

		PreparedStatement stmt = null;
		ResultSet rs = null;
		HashMap<String, Integer> getTableValues = new HashMap<String, Integer>();
		StringBuilder sb = new StringBuilder();
		sb.append("SELECT ITEM_CODE, RETAILER_ITEM_CODE FROM ITEM_LOOKUP WHERE ACTIVE_INDICATOR = 'Y'");
		
		try {
			String sql = sb.toString();
			stmt = conn.prepareStatement(sql);
			rs = stmt.executeQuery();
			while (rs.next()) {
				getTableValues.put(rs.getString("RETAILER_ITEM_CODE"), rs.getInt("ITEM_CODE"));
			}

		} catch (Exception e) {
			throw new GeneralException("Exception in Getting key/value in getActiveItems()" + e);
		} finally {
			PristineDBUtil.close(rs);
			PristineDBUtil.close(stmt);
		}
		return getTableValues;

	}
}
