package com.pristine.dao.offermgmt;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map.Entry;

import org.apache.log4j.Logger;

import com.pristine.dao.IDAO;
import com.pristine.dto.PriceZoneDTO;
import com.pristine.dto.offermgmt.ProductLocationMappingDTO;
import com.pristine.dto.offermgmt.ProductLocationMappingKey;
import com.pristine.exception.GeneralException;
import com.pristine.util.Constants;
import com.pristine.util.PristineDBUtil;
import com.pristine.util.PropertyManager;
import com.pristine.util.offermgmt.PRCommonUtil;


public class ProductLocationMappingDAO implements IDAO{
	private static Logger logger = Logger.getLogger("ProductLocationMappingDAO");
	
	private final static String GET_CATEGORY_ID_INFO = " select code, product_id from product_group where active_indicator = 'Y' and "
			+ " product_level_id=?";

	public HashMap<String, List<Integer>> getCategoryCodeCategoryId(Connection conn) throws GeneralException {
		PreparedStatement stmt = null;
		ResultSet rs = null;
		HashMap<String, List<Integer>> categoryCodeId = new HashMap<String, List<Integer>>();

		try {
			logger.debug("Inside getCategoryCodeCategoryId of ProductLocationMappingDAO");
			stmt = conn.prepareStatement(GET_CATEGORY_ID_INFO);
			stmt.setInt(1, Constants.CATEGORYLEVELID);
			rs = stmt.executeQuery();
			while (rs.next()) {
				List<Integer> productIds = new ArrayList<Integer>();
				if(categoryCodeId.containsKey(rs.getString("code"))){
					productIds = categoryCodeId.get(rs.getString("code"));
				}
				productIds.add(rs.getInt("product_id"));
				categoryCodeId.put(rs.getString("code"), productIds);
			}
			logger.info("# of code/Category_id mapping " + categoryCodeId.size());

		} catch (Exception e) {
			throw new GeneralException("Exception in Getting code/product_id map", e);
		} finally {
			PristineDBUtil.close(rs);
			PristineDBUtil.close(stmt);
		}
		return categoryCodeId;
	}

	private final static String GET_COMP_STR_ID = "select comp_str_no,comp_str_id from competitor_store where comp_chain_id <> ?";

	public HashMap<Integer, Integer> getCompStrId(Connection conn, String chainId) throws GeneralException {
		PreparedStatement stmt = null;
		ResultSet rs = null;
		HashMap<Integer, Integer> compStrMapping = new HashMap<Integer, Integer>();

		try {
			logger.debug("Inside getCompStrId() of ProductLocationMappingDAO");
			stmt = conn.prepareStatement(GET_COMP_STR_ID);
			stmt.setString(1, chainId);
			rs = stmt.executeQuery();
			while (rs.next()) {
				compStrMapping.put(rs.getInt("comp_str_no"), rs.getInt("comp_str_id"));
			}
			logger.info("# of comp_str_no/comp_str_id mapping " + compStrMapping.size());

		} catch (Exception e) {
			throw new GeneralException("Exception in Getting comp_str_no/comp_str_id map", e);
		} finally {
			PristineDBUtil.close(rs);
			PristineDBUtil.close(stmt);
		}
		return compStrMapping;
	}

	public HashMap<String, Integer> getPriceZoneId(Connection conn) throws GeneralException {
		PreparedStatement stmt = null;
		ResultSet rs = null;
		HashMap<String, Integer> compStrMapping = new HashMap<String, Integer>();

		try {
			logger.debug("Inside getPriceZoneId() of ProductLocationMappingDAO");
			String sql = "select zone_num,price_zone_id from retail_price_zone";

			stmt = conn.prepareStatement(sql);
			rs = stmt.executeQuery();
			while (rs.next()) {
				compStrMapping.put(rs.getString("zone_num"), rs.getInt("price_zone_id"));
			}
			logger.info("# of zone_num/price_zone_id mapping " + compStrMapping.size());

		} catch (Exception e) {
			throw new GeneralException("Exception in Getting zone_num/price_zone_id map", e);
		} finally {
			PristineDBUtil.close(rs);
			PristineDBUtil.close(stmt);
		}
		return compStrMapping;
	}

	private final static String GET_STR_ID_INFO = "SELECT COMP_STR_NO, COMP_STR_ID FROM COMPETITOR_STORE WHERE COMP_CHAIN_ID = ?";

	public HashMap<String, Integer> getStoreIds(Connection conn, String chainId) throws GeneralException {
		logger.debug("Inside getStoreIds() of ProductLocationMappingDAO");
		HashMap<String, Integer> storeNumberMap = new HashMap<String, Integer>();
		PreparedStatement statement = null;
		ResultSet resultSet = null;
		PRCommonUtil prCommonUtil = new PRCommonUtil();
		try {
			String query = GET_STR_ID_INFO;
			if(PropertyManager.getProperty("TOPS_DIVISION_ID_IN_ZONE_MAPPING")!= null){
				String divisionId = PropertyManager.getProperty("TOPS_DIVISION_ID_IN_ZONE_MAPPING");
				query = query+ " AND DIVISION_ID IN ("+divisionId+")";
			}
			logger.info("Query: "+query);
			statement = conn.prepareStatement(query);
			statement.setString(1, chainId);
			resultSet = statement.executeQuery();
			while (resultSet.next()) {
				String storeNum = prCommonUtil.integerAsString(resultSet.getString("COMP_STR_NO"));
				storeNumberMap.put(storeNum, resultSet.getInt("COMP_STR_ID"));
			}
		} catch (Exception e) {
			throw new GeneralException("Exception in Getting str_num/str_id map", e);
		} finally {
			PristineDBUtil.close(resultSet);
			PristineDBUtil.close(statement);
		}
		return storeNumberMap;
	}

	private static final String INSERT_INTO_PR_PRODUCT_LOCATION_MAPPING = "MERGE INTO PR_PRODUCT_LOCATION_MAPPING PLM USING "
			+ "(SELECT ? PRODUCT_LEVEL_ID,? PRODUCT_ID, ? LOCATION_LEVEL_ID, ? LOCATION_ID, ? LOCATION_NO FROM DUAL ) D "
			+ "ON (PLM.PRODUCT_LEVEL_ID = D.PRODUCT_LEVEL_ID AND PLM.PRODUCT_ID = D.PRODUCT_ID AND PLM.LOCATION_LEVEL_ID = D.LOCATION_LEVEL_ID AND PLM.LOCATION_ID = D.LOCATION_ID) "
			+ "WHEN MATCHED THEN UPDATE SET PLM.LOCATION_NO = D.LOCATION_NO "
			+ "WHEN NOT MATCHED THEN INSERT (PLM.PRODUCT_LOCATION_MAPPING_ID, PLM.PRODUCT_LEVEL_ID, PLM.PRODUCT_ID, PLM.LOCATION_LEVEL_ID, PLM.LOCATION_ID, PLM.LOCATION_NO) "
			+ "VALUES (PR_PRODUCT_LOCATION_MAP_SEQ.NEXTVAL, D.PRODUCT_LEVEL_ID, D.PRODUCT_ID, D.LOCATION_LEVEL_ID, D.LOCATION_ID, D.LOCATION_NO)";

	public int populateProductLocationMapping(Connection conn,
			HashMap<ProductLocationMappingKey, List<ProductLocationMappingDTO>> finalMap) throws GeneralException {
		logger.debug("Inside populateProductLocationMapping() of ProductLocationMappingDAO");
		PreparedStatement statement = null;
		int itemNoInBatch = 0;
		int totalUpdateCnt = 0;
		try {
			statement = conn.prepareStatement(INSERT_INTO_PR_PRODUCT_LOCATION_MAPPING);

			for (Entry<ProductLocationMappingKey, List<ProductLocationMappingDTO>> entry : finalMap.entrySet()) {
				ProductLocationMappingKey productLocationMappingKey = entry.getKey();
				//get the zone no from first index (as it will be same for all)
				if (entry.getValue().size() > 0) {
					ProductLocationMappingDTO productLocationMappingDTO = entry.getValue().get(0);
					statement.setInt(1, productLocationMappingKey.getProductLevelId());
					statement.setInt(2, productLocationMappingKey.getProductId());
					statement.setInt(3, productLocationMappingKey.getLocationLevelId());
					statement.setInt(4, productLocationMappingKey.getLocationId());
					statement.setString(5, productLocationMappingDTO.getPriceZone());
					statement.addBatch();
					itemNoInBatch++;

					if (itemNoInBatch % Constants.BATCH_UPDATE_COUNT == 0) {
						logger.debug("Total count of records processed: " + itemNoInBatch);
						int[] count = statement.executeBatch();
						totalUpdateCnt = totalUpdateCnt + count.length;
						statement.clearBatch();
						itemNoInBatch = 0;
					}
				}
			}
			if (itemNoInBatch > 0) {
				int[] count = statement.executeBatch();
				totalUpdateCnt = totalUpdateCnt + count.length;
				statement.clearBatch();
			}
			statement.close();
			finalMap = null;
		} catch (Exception e) {
			logger.error("Error in populateProductLocationMapping()  - " + e.toString());
			throw new GeneralException("Error in populateProductLocationMapping()", e);
		} finally {
			PristineDBUtil.close(statement);
		}
		return totalUpdateCnt;
	}

	private static final String GET_PR_PRODUCT_LOCATION_MAPPING = "SELECT PRODUCT_LOCATION_MAPPING_ID, "
			+ "PRODUCT_LEVEL_ID, PRODUCT_ID, LOCATION_LEVEL_ID, LOCATION_ID FROM PR_PRODUCT_LOCATION_MAPPING";
	
	public HashMap<ProductLocationMappingKey, Long> getProductMappingId(Connection conn) throws GeneralException{
		logger.debug("Inside getProductMappingId()");
		HashMap<ProductLocationMappingKey, Long> productMappingId = new HashMap<ProductLocationMappingKey, Long>();
		PreparedStatement statement = null;
		ResultSet rs = null;
		try {
			statement = conn.prepareStatement(GET_PR_PRODUCT_LOCATION_MAPPING);
			rs = statement.executeQuery();
			while (rs.next()) {
				ProductLocationMappingKey plmKey = new ProductLocationMappingKey(rs.getInt("PRODUCT_LEVEL_ID"),
						rs.getInt("PRODUCT_ID"), rs.getInt("LOCATION_LEVEL_ID"), rs.getInt("LOCATION_ID"));
				productMappingId.put(plmKey, rs.getLong("PRODUCT_LOCATION_MAPPING_ID"));
			}
		} catch (Exception e) {
			logger.error("Error in getProductMappingId()  - " + e.toString());
			throw new GeneralException("Error in getProductMappingId()", e);
		} finally {
			PristineDBUtil.close(rs);
			PristineDBUtil.close(statement);
		}
		return productMappingId;
	}
	
	private static final String INSERT_PR_PRODUCT_LOCATION_MAPPING = "INSERT INTO PR_PRODUCT_LOCATION_MAPPING "
			+ " (PRODUCT_LOCATION_MAPPING_ID,PRODUCT_LEVEL_ID,PRODUCT_ID,LOCATION_LEVEL_ID,LOCATION_ID, "
			+ " LOCATION_NO, PARENT_LOCATION_LEVEL_ID, PARENT_LOCATION_ID) "
			+ " VALUES (PR_PRODUCT_LOCATION_MAP_SEQ.NEXTVAL, ?, ?, ?, ?, ?, ?, ?)";

	public int insertProductLocationMapping(Connection conn,
			HashMap<ProductLocationMappingKey, List<ProductLocationMappingDTO>> finalMap,
			HashMap<ProductLocationMappingKey, Long> productMappingId) throws GeneralException {

		logger.debug("Inside insertProductLocationMapping()");
		PreparedStatement statement = null;
		int itemNoInBatch = 0;
		int totalInsertCnt = 0;
		try {
			statement = conn.prepareStatement(INSERT_PR_PRODUCT_LOCATION_MAPPING);

			for (Entry<ProductLocationMappingKey, List<ProductLocationMappingDTO>> entry : finalMap.entrySet()) {
				ProductLocationMappingKey productLocationMappingKey = entry.getKey();
				//if combination is not already there
				if (productMappingId.get(productLocationMappingKey) == null && entry.getValue().size() > 0) {
					ProductLocationMappingDTO productLocationMappingDTO = entry.getValue().get(0);
					statement.setInt(1, productLocationMappingKey.getProductLevelId());
					statement.setInt(2, productLocationMappingKey.getProductId());
					statement.setInt(3, productLocationMappingKey.getLocationLevelId());
					statement.setInt(4, productLocationMappingKey.getLocationId());
					statement.setString(5, productLocationMappingDTO.getPriceZone());
					if(productLocationMappingDTO.parentLocationLevelId == 0){
						statement.setNull(6, java.sql.Types.INTEGER);
						statement.setNull(7, java.sql.Types.INTEGER);	
					}else{
						statement.setInt(6, productLocationMappingDTO.parentLocationLevelId);
						statement.setInt(7, productLocationMappingDTO.parentLocationId);	
					}
					statement.addBatch();
					itemNoInBatch++;

					if (itemNoInBatch % Constants.BATCH_UPDATE_COUNT == 0) {
						//logger.debug("Total count of records processed: " + itemNoInBatch);
						int[] count = statement.executeBatch();
						totalInsertCnt = totalInsertCnt + count.length;
						statement.clearBatch();
						itemNoInBatch = 0;
					}
				}
			}
			if (itemNoInBatch > 0) {
				int[] count = statement.executeBatch();
				totalInsertCnt = totalInsertCnt + count.length;
				statement.clearBatch();
			}
			statement.close();
			finalMap = null;
		} catch (Exception e) {
			logger.error("Error in insertProductLocationMapping()  - " + e.toString());
			throw new GeneralException("Error in insertProductLocationMapping()", e);
		} finally {
			PristineDBUtil.close(statement);
		}

		return totalInsertCnt;
	}
	
	private static final String UPDATE_PR_PRODUCT_LOCATION_MAPPING = "UPDATE PR_PRODUCT_LOCATION_MAPPING "
			+ " SET LOCATION_NO = ?, PARENT_LOCATION_LEVEL_ID = ?, PARENT_LOCATION_ID = ? WHERE PRODUCT_LOCATION_MAPPING_ID = ? ";

	public int updateProductLocationMapping(Connection conn,
			HashMap<ProductLocationMappingKey, List<ProductLocationMappingDTO>> finalMap,
			HashMap<ProductLocationMappingKey, Long> productMappingId) throws GeneralException {

		logger.debug("Inside updateProductLocationMapping()");
		PreparedStatement statement = null;
		int itemNoInBatch = 0;
		int totalUpdateCnt = 0;
		try {
			statement = conn.prepareStatement(UPDATE_PR_PRODUCT_LOCATION_MAPPING);

			for (Entry<ProductLocationMappingKey, List<ProductLocationMappingDTO>> entry : finalMap.entrySet()) {
				ProductLocationMappingKey productLocationMappingKey = entry.getKey();
				//if combination is already there
				if (productMappingId.get(productLocationMappingKey) != null && entry.getValue().size() > 0) {
					ProductLocationMappingDTO productLocationMappingDTO = entry.getValue().get(0);
					statement.setString(1, productLocationMappingDTO.getPriceZone());
					
					if(productLocationMappingDTO.parentLocationLevelId == 0){
						statement.setNull(2, java.sql.Types.INTEGER);
						statement.setNull(3, java.sql.Types.INTEGER);	
					}else{
						statement.setInt(2, productLocationMappingDTO.parentLocationLevelId);
						statement.setInt(3, productLocationMappingDTO.parentLocationId);	
					}
					statement.setLong(4, productMappingId.get(productLocationMappingKey));
					
					statement.addBatch();
					itemNoInBatch++;

					if (itemNoInBatch % Constants.BATCH_UPDATE_COUNT == 0) {
						//logger.debug("Total count of records processed: " + itemNoInBatch);
						int[] count = statement.executeBatch();
						totalUpdateCnt = totalUpdateCnt + count.length;
						statement.clearBatch();
						itemNoInBatch = 0;
					}
				}
			}
			if (itemNoInBatch > 0) {
				int[] count = statement.executeBatch();
				totalUpdateCnt = totalUpdateCnt + count.length;
				statement.clearBatch();
			}
			statement.close();
			finalMap = null;
		} catch (Exception e) {
			logger.error("Error in updateProductLocationMapping()  - " + e.toString());
			throw new GeneralException("Error in updateProductLocationMapping()", e);
		} finally {
			PristineDBUtil.close(statement);
		}

		return totalUpdateCnt;
	}
	
	private static final String INSERT_INTO_PR_PRODUCT_LOCATION_STORE = "INSERT INTO PR_PRODUCT_LOCATION_STORE (PRODUCT_LOCATION_MAPPING_ID,STORE_ID) "
			+ "VALUES ((select PRODUCT_LOCATION_MAPPING_ID from PR_PRODUCT_LOCATION_MAPPING where product_level_id = ? and product_id= ? and location_level_id = ? and location_id =?),?)";

	public int populateProductLocationStore(Connection conn,
			HashMap<ProductLocationMappingKey, List<ProductLocationMappingDTO>> finalMap) throws GeneralException {

		logger.debug("Inside populateProductLocationMapping() of ProductLocationMappingDAO");
		PreparedStatement statement = null;
		int itemNoInBatch = 0;
		int totalUpdateCnt = 0;
		StringBuilder sb = new StringBuilder();
		try {
			statement = conn.prepareStatement(INSERT_INTO_PR_PRODUCT_LOCATION_STORE);
			
			for (Entry<ProductLocationMappingKey, List<ProductLocationMappingDTO>> entry : finalMap.entrySet()) {
				for (ProductLocationMappingDTO priceZoneMappingDTO : entry.getValue()) {
					statement.setInt(1, priceZoneMappingDTO.getProductLevelId());
					sb.append("Prod_level_id:"+priceZoneMappingDTO.getProductLevelId());
					statement.setInt(2, priceZoneMappingDTO.getProductId());
					sb.append("Prod_id:"+priceZoneMappingDTO.getProductId());
					statement.setInt(3, priceZoneMappingDTO.getLocationLevelId());
					sb.append("Loc_Level_id:"+priceZoneMappingDTO.getLocationLevelId());
					statement.setInt(4, priceZoneMappingDTO.getLocationId());
					sb.append("Loc_id:"+priceZoneMappingDTO.getLocationId());
					statement.setInt(5, priceZoneMappingDTO.getStoreId());
					sb.append("Store id:"+priceZoneMappingDTO.getStoreId());
					sb.append("\n");
					statement.addBatch();
					itemNoInBatch++;

					if (itemNoInBatch % Constants.BATCH_UPDATE_COUNT == 0) {
						//logger.debug("Total count of records processed: " + itemNoInBatch);
						int[] count = statement.executeBatch();
						totalUpdateCnt = totalUpdateCnt + count.length;
						statement.clearBatch();
						sb = new StringBuilder();
						itemNoInBatch = 0;
					}
				}

			}
			if (itemNoInBatch > 0) {
				int[] count = statement.executeBatch();
				totalUpdateCnt = totalUpdateCnt + count.length;
				statement.clearBatch();
			}
			statement.close();
			finalMap = null;
		} catch (Exception e) {
			logger.error(sb.toString());
			logger.error("Error in populateProductLocationStore()  - " + e.toString());
			throw new GeneralException("Error in populateProductLocationStore()", e);
		} finally {
			PristineDBUtil.close(statement);
		}

		return totalUpdateCnt;
	}

	public void truncateProductLocationStore(Connection conn) throws GeneralException {
		String sql = "delete from PR_PRODUCT_LOCATION_STORE";
		logger.debug("Inside truncateProductLocationStore() of ProductLocationMappingDAO");
		PreparedStatement statement = null;
		ResultSet resultSet = null;
		try {
			statement = conn.prepareStatement(sql);
			resultSet = statement.executeQuery();
			statement.close();
		} catch (Exception e) {
			logger.error("Exception in truncateProductLocationStore()  - " + e.toString());
			throw new GeneralException("Error in truncateProductLocationStore()", e);
		} finally {
			PristineDBUtil.close(resultSet);
			PristineDBUtil.close(statement);
		}
	}
	
	
	public void deleteDefaultStores(Connection conn, int defaultProductId) throws GeneralException {
		String sql = "delete from PR_PRODUCT_LOCATION_STORE WHERE PRODUCT_LOCATION_MAPPING_ID IN ( " +
						"SELECT PRODUCT_LOCATION_MAPPING_ID FROM PR_PRODUCT_LOCATION_MAPPING WHERE PRODUCT_ID = " + defaultProductId
						+ " AND PRODUCT_LEVEL_ID = " + Constants.CATEGORYLEVELID + " )";
		logger.debug("Inside truncateProductLocationStore() of ProductLocationMappingDAO");
		PreparedStatement statement = null;
		ResultSet resultSet = null;
		try {
			statement = conn.prepareStatement(sql);
			resultSet = statement.executeQuery();
			statement.close();
		} catch (Exception e) {
			logger.error("Exception in truncateProductLocationStore()  - " + e.toString());
			throw new GeneralException("Error in truncateProductLocationStore()", e);
		} finally {
			PristineDBUtil.close(resultSet);
			PristineDBUtil.close(statement);
		}
	}

/*	private final static String DELETE_FROM_PR_PRODUCT_LOCATION_MAPPING = "DELETE FROM T_PR_PRODUCT_LOCATION_MAPPING "
			+ "WHERE NOT EXISTS (SELECT * FROM T_PR_PRODUCT_LOCATION_STORE "
			+ "WHERE T_PR_PRODUCT_LOCATION_STORE.T_PRODUCT_LOCATION_MAPPING_ID = T_PR_PRODUCT_LOCATION_MAPPING.T_PRODUCT_LOCATION_MAPPING_ID) "
			+ "AND PRODUCT_LEVEL_ID =4 AND LOCATION_LEVEL_ID=6";*/
	
	private final static String DELETE_FROM_PR_PRODUCT_LOCATION_MAPPING = 
			"DELETE FROM PR_PRODUCT_LOCATION_MAPPING WHERE PRODUCT_LOCATION_MAPPING_ID IN "+
			"(SELECT PRODUCT_LOCATION_MAPPING_ID FROM PR_PRODUCT_LOCATION_MAPPING WHERE PRODUCT_LOCATION_MAPPING_ID NOT IN "+
			"(SELECT PRODUCT_LOCATION_MAPPING_ID FROM PR_PRODUCT_LOCATION_STORE) AND PRODUCT_LEVEL_ID = ?"+
			"AND LOCATION_LEVEL_ID = ?)";

	public int deleteUnmatchedRecords(Connection conn) throws GeneralException {
		logger.debug("Inside deleteUnmatchedRecords() of ProductLocationMappingDAO");
		PreparedStatement statement = null;
		int totalDeleteCnt = 0;
		try {
			statement = conn.prepareStatement(DELETE_FROM_PR_PRODUCT_LOCATION_MAPPING);
			statement.setInt(1, Constants.CATEGORYLEVELID);
			statement.setInt(2, Constants.ZONE_LEVEL_ID);
			totalDeleteCnt = statement.executeUpdate();
			statement.close();
		} catch (Exception e) {
			logger.error("Exception deleteUnmatchedRecords()  - " + e.toString());
			throw new GeneralException("Error in deleteUnmatchedRecords()", e);
		} finally {
			PristineDBUtil.close(statement);
		}
		return totalDeleteCnt;
	}
	
	public void getPrizeZoneMappingDetail(Connection connection, String productId, String zoneNumber,
			List<ProductLocationMappingDTO> priceZoneMappingDetailList) throws GeneralException {
		PreparedStatement statement = null;
		ResultSet resultSet = null;
		try{
			try {
				int counter = 0;
				String query = new String(
						"WITH STORE_INFO (COMMON_ID,\"PRICE ZONE\",STORE) AS (SELECT ROWNUM,RPZ.ZONE_NUM,CS.COMP_STR_NO FROM "
						+ "RETAIL_PRICE_ZONE RPZ JOIN COMPETITOR_STORE CS ON RPZ.PRICE_ZONE_ID = CS.PRICE_ZONE_ID "
						+ "WHERE RPZ.PRICE_ZONE_ID IN (%ZoneNumber%) AND CS.COMP_CHAIN_ID =(SELECT COMP_CHAIN_ID FROM COMPETITOR_CHAIN "
						+ "WHERE PRESTO_SUBSCRIBER = 'Y') AND CS.ACTIVE_INDICATOR='Y' ORDER BY ROWNUM ), PRODUCT_GROUP_INFO (COMMON_ID,CATEGORY) AS (SELECT ROWNUM,CODE FROM "
						+ "PRODUCT_GROUP WHERE PRODUCT_ID IN (%ProductId%) AND PRODUCT_LEVEL_ID = ? AND CODE <> 'NULL') "
						+ "SELECT PRODUCT_GROUP_INFO.CATEGORY,' ' AS \"PRIMARY COMPETITOR\",STORE_INFO.\"PRICE ZONE\",STORE_INFO.STORE "
						+ "FROM PRODUCT_GROUP_INFO LEFT OUTER JOIN STORE_INFO ON STORE_INFO.COMMON_ID = STORE_INFO.COMMON_ID ORDER BY "
						+ "PRODUCT_GROUP_INFO.CATEGORY,STORE_INFO.\"PRICE ZONE\" ");
				query = query.replaceAll("%ZoneNumber%", zoneNumber);
				query = query.replaceAll("%ProductId%", productId);
				statement = connection.prepareStatement(query);
				logger.debug(query);
				statement.setInt(++counter, 4);
				resultSet = statement.executeQuery();
				while (resultSet.next()) {
					ProductLocationMappingDTO productLocationMappingDTO = new ProductLocationMappingDTO();
					productLocationMappingDTO.setCategory(resultSet.getString("CATEGORY"));
					productLocationMappingDTO.setPriceZone(resultSet.getString("PRICE ZONE"));
					productLocationMappingDTO.setStore(resultSet.getString("STORE"));
					priceZoneMappingDetailList.add(productLocationMappingDTO);
				}
			} catch (SQLException e) {
				logger.error("Error while executing getPrizeZoneMappingDetail() " + e);
				throw new GeneralException("Error while executing getPrizeZoneMappingDetail() " + e);
			}
		}
			finally {
			PristineDBUtil.close(resultSet);
			PristineDBUtil.close(statement);
		}
	}
	
	public HashMap<String, String> getPrimaryMatchingZoneNum(Connection connection) throws GeneralException {
		PreparedStatement statement = null;
		ResultSet resultSet = null;
		HashMap<String, String> primaryMatchingZoneMap = new HashMap<String, String>();
			try{
				try {
					String query = "SELECT RPZ1.ZONE_NUM AS PRIMARY_ZONE_NUM,RPZ2.ZONE_NUM AS MATCHING_ZONE_NUM  FROM PR_PRIMARY_MATCHING_ZONE_MAP PMZM"
							+ " JOIN RETAIL_PRICE_ZONE RPZ1 ON RPZ1.PRICE_ZONE_ID = PMZM.PRIMARY_ZONE_ID "
							+ " JOIN RETAIL_PRICE_ZONE RPZ2 ON RPZ2.PRICE_ZONE_ID = PMZM.MATCHING_ZONE_ID";
					statement = connection.prepareStatement(query);
					resultSet = statement.executeQuery();
					while (resultSet.next()) {
						primaryMatchingZoneMap.put(resultSet.getString("MATCHING_ZONE_NUM"), resultSet.getString("PRIMARY_ZONE_NUM"));
					}
				} catch (SQLException e) {
					logger.error("Error while executing getPrimaryMatchingZoneNum() " + e);
					throw new GeneralException("Error while executing getPrizeZoneMappingDetail() " + e);
				}
			}finally {
			PristineDBUtil.close(resultSet);
			PristineDBUtil.close(statement);
		}
		return primaryMatchingZoneMap;
	}
	
	
	/**
	 * 
	 * @param connection
	 * @return
	 * @throws GeneralException
	 */
	public HashMap<Integer, Integer> getPrimaryMatchingZoneId(Connection connection) throws GeneralException {
		PreparedStatement statement = null;
		ResultSet resultSet = null;
		HashMap<Integer, Integer> primaryMatchingZoneMap = new HashMap<>();
			try{
				try {
					String query = " SELECT MATCHING_ZONE_ID, PRIMARY_ZONE_ID FROM PR_PRIMARY_MATCHING_ZONE_MAP ";
					statement = connection.prepareStatement(query);
					resultSet = statement.executeQuery();
					while (resultSet.next()) {
						primaryMatchingZoneMap.put(resultSet.getInt("MATCHING_ZONE_ID"), resultSet.getInt("PRIMARY_ZONE_ID"));
					}
				} catch (SQLException e) {
					logger.error("Error while executing getPrimaryMatchingZoneId() " + e);
					throw new GeneralException("Error while executing getPrimaryMatchingZoneId() " + e);
				}
			}finally {
			PristineDBUtil.close(resultSet);
			PristineDBUtil.close(statement);
		}
		return primaryMatchingZoneMap;
	}
	/**
	 * 
	 * @param conn
	 * @return categories by price group code
	 * @throws GeneralException
	 */
	public List<ProductLocationMappingDTO> getCategoriesByPrcGrpCode(Connection conn) throws GeneralException{
		List<ProductLocationMappingDTO> categoriesToBeSetup = new ArrayList<>();
		PreparedStatement statement = null;
		ResultSet resultSet = null;
		try{
			StringBuilder sb = new StringBuilder();
			sb.append("SELECT DISTINCT IL.USER_ATTR_3, " + Constants.CATEGORYLEVELID + " AS PRODUCT_LEVEL_ID, ");
			sb.append(" PROD.CATEGORY_ID AS PRODUCT_ID FROM (SELECT ");
			sb.append(" START_WITH_CHILD_id AS item_code, ");
			sb.append(" MIN(CASE WHEN PH.PRODUCT_LEVEL_ID =  " + Constants.CATEGORYLEVELID + " THEN PH.PRODUCT_ID END) AS CATEGORY_ID, ");
			sb.append(" MIN(CASE WHEN PH.PRODUCT_LEVEL_ID =  " + Constants.CATEGORYLEVELID + " THEN PG.NAME END) AS CATEGORY_NAME ");
			sb.append(" FROM ( ");
			sb.append(" SELECT  PRODUCT_ID, PRODUCT_LEVEL_ID, CHILD_PRODUCT_ID, CHILD_PRODUCT_LEVEL_ID, ");
			sb.append(" CONNECT_BY_root CHILD_PRODUCT_ID AS START_WITH_CHILD_id FROM PRODUCT_GROUP_RELATION PGR ");
			sb.append(" START WITH CHILD_PRODUCT_ID in ( ");
			sb.append(" SELECT CHILD_PRODUCT_ID FROM (SELECT CHILD_PRODUCT_ID,CHILD_PRODUCT_LEVEL_ID ");
			sb.append(" FROM PRODUCT_GROUP_RELATION PGR ");
			sb.append(" START WITH PRODUCT_LEVEL_ID =  " + Constants.CATEGORYLEVELID + " ");
			sb.append(" CONNECT BY  PRIOR CHILD_PRODUCT_ID = PRODUCT_ID ");
			sb.append(" AND  PRIOR CHILD_PRODUCT_LEVEL_ID = PRODUCT_LEVEL_ID) ");
			sb.append(" WHERE CHILD_PRODUCT_LEVEL_ID = " + Constants.ITEM_LEVEL_CHILD_PRODUCT_LEVEL_ID + " )  "
					+ " AND CHILD_PRODUCT_LEVEL_ID =  " + Constants.ITEM_LEVEL_CHILD_PRODUCT_LEVEL_ID);
			sb.append(" CONNECT BY PRIOR PRODUCT_ID =  CHILD_PRODUCT_ID AND ");
			sb.append(" PRIOR PRODUCT_LEVEL_ID =  CHILD_PRODUCT_LEVEL_ID ) PH ");
			sb.append(" LEFT JOIN PRODUCT_GROUP PG ON PH.PRODUCT_LEVEL_ID = PG.PRODUCT_LEVEL_ID ");
			sb.append(" AND PH.PRODUCT_ID = PG.PRODUCT_ID GROUP BY START_WITH_CHILD_ID) PROD ");
			sb.append(" JOIN ITEM_LOOKUP IL ON IL.ITEM_CODE = PROD.ITEM_CODE AND ACTIVE_INDICATOR = 'Y' ");
			
			
			statement = conn.prepareStatement(sb.toString());
			resultSet = statement.executeQuery();
			while (resultSet.next()) {
				ProductLocationMappingDTO productLocationMappingDTO = new ProductLocationMappingDTO();
				
				productLocationMappingDTO.productLevelId = resultSet.getInt("PRODUCT_LEVEL_ID");
				productLocationMappingDTO.productId = resultSet.getInt("PRODUCT_ID");
				productLocationMappingDTO.prcGrpCode = resultSet.getString("USER_ATTR_3");
				
				categoriesToBeSetup.add(productLocationMappingDTO);
			}
		} catch (SQLException e) {
			logger.error("Error while executing getCategoriesByPrcGrpCode() " + e);
			throw new GeneralException("Error while executing getCategoriesByPrcGrpCode() " + e);
		}
		
		return categoriesToBeSetup;
	}
	
	public List<PriceZoneDTO> getMatchingZones(Connection connection, int primaryZoneId) throws GeneralException {
		PreparedStatement statement = null;
		ResultSet resultSet = null;
		List<PriceZoneDTO> matchingZones = new ArrayList<PriceZoneDTO>();
		try {
			try {
				String query = "SELECT RPZ1.ZONE_NUM AS PRIMARY_ZONE_NUM,RPZ2.ZONE_NUM AS MATCHING_ZONE_NUM,"
						+ " RPZ2.PRICE_ZONE_ID AS MATCHING_ZONE_ID  FROM PR_PRIMARY_MATCHING_ZONE_MAP PMZM"
						+ " JOIN RETAIL_PRICE_ZONE RPZ1 ON RPZ1.PRICE_ZONE_ID = PMZM.PRIMARY_ZONE_ID "
						+ " JOIN RETAIL_PRICE_ZONE RPZ2 ON RPZ2.PRICE_ZONE_ID = PMZM.MATCHING_ZONE_ID " + " WHERE PRIMARY_ZONE_ID = " + primaryZoneId;
				
				logger.debug("getMatchingZones Query:" + query);
				statement = connection.prepareStatement(query);
				resultSet = statement.executeQuery();
				while (resultSet.next()) {
					PriceZoneDTO priceZoneDTO = new PriceZoneDTO();
					priceZoneDTO.setPriceZoneId(resultSet.getInt("MATCHING_ZONE_ID"));
					priceZoneDTO.setPriceZoneNum(resultSet.getString("MATCHING_ZONE_NUM"));
					matchingZones.add(priceZoneDTO);
				}
			} catch (SQLException e) {
				logger.error("Error while executing getMatchingZones() " + e);
				throw new GeneralException("Error while executing getMatchingZones() " + e);
			}
		} finally {
			PristineDBUtil.close(resultSet);
			PristineDBUtil.close(statement);
		}
		return matchingZones;
	}
}
