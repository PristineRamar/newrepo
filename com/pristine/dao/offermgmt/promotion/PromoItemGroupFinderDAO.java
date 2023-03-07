package com.pristine.dao.offermgmt.promotion;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;

import org.apache.log4j.Logger;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.pristine.dao.IDAO;
import com.pristine.dto.offermgmt.ProductKey;
import com.pristine.dto.offermgmt.promotion.PromoItemDTO;
import com.pristine.dto.offermgmt.promotion.PromoProductGroup;
import com.pristine.exception.GeneralException;
import com.pristine.util.Constants;
import com.pristine.util.PristineDBUtil;
import com.pristine.util.offermgmt.PRCommonUtil;

public class PromoItemGroupFinderDAO implements IDAO {

	private static Logger logger = Logger.getLogger("PromoItemGroupFinderDAO");
	
	private static final String GET_MAX_GROUP_ID = "SELECT MAX(GROUP_ID) AS MAX_GROUP_ID FROM PM_PPG_HEADER";
	
	private static final String INSERT_PPG_HEADER = "INSERT INTO PM_PPG_HEADER (RUN_ID, GROUP_ID, REG_UNIT_PRICE_START, REG_UNIT_PRICE_END, "
			+ " PREFERRED_PROMO_TYPE_ID, DEPARTMENT_ID, PROMOTED_IN_WEEKS) VALUES "
			+ "(?, ?, ?, ?, ?, ?, ?)";
	
	private static final String INSERT_PPG_ITEMS = "INSERT INTO PM_PPG_ITEMS (GROUP_ID, ITEM_LEVEL_ID, ITEM_ID, IS_LEAD_ITEM, TOTAL_MOVEMENT) VALUES "
			+ "(?, ?, ?, ?, ?)";
	
	private static final String GET_LAST_X_MONTHS_MOV_FOR_STORELIST = " SELECT PRODUCT_ID, SUM(TOT_MOVEMENT) AS TOT_MOVEMENT "
			+ " FROM SYN_ITEM_METRIC_SUMMARY_WEEKLY IMS WHERE CALENDAR_ID IN (SELECT CALENDAR_ID FROM RETAIL_CALENDAR RC "
			+ " WHERE ROW_TYPE='W' AND RC.START_DATE <= TO_DATE(?,'MM/DD/YYYY') AND RC.START_DATE >= TO_DATE(?,'MM/DD/YYYY') - ?) "
			+ " AND LOCATION_LEVEL_ID = ? AND LOCATION_ID IN (SELECT CHILD_LOCATION_ID FROM LOCATION_GROUP_RELATION WHERE " 
			+ " LOCATION_LEVEL_ID = ? AND LOCATION_ID = ? AND CHILD_LOCATION_LEVEL_ID = ?) AND PRODUCT_LEVEL_ID =  ? AND PRODUCT_ID IN "
			+ "(%productIdList%) GROUP BY PRODUCT_ID";
	
	private static final String GET_PPG_RUN_ID = "SELECT PPG_RUN_ID_SEQ.NEXTVAL AS RUN_ID FROM DUAL";
	
	private long findMaxGroupId(Connection conn) throws GeneralException {
		long maxGroupId = 0;
		PreparedStatement stmt = null;
		ResultSet rs = null;
		try {
			stmt = conn.prepareStatement(GET_MAX_GROUP_ID);
			rs = stmt.executeQuery();
			if (rs.next()) {
				maxGroupId = rs.getLong("MAX_GROUP_ID");
			}
		} catch (SQLException exception) {
			logger.error("Error in findMaxGroupId() - " + exception.toString());
			throw new GeneralException("Error in findMaxGroupId() - " + exception.toString());
		} finally {
			PristineDBUtil.close(rs);
			PristineDBUtil.close(stmt);
		}

		return maxGroupId;
	}
	
	public void insertPPGHeader(Connection conn, List<PromoProductGroup> productGroups, int inputProductId) throws GeneralException{
		PreparedStatement stmt = null;
		
		ResultSet rs = null;
		long runId = -1;
		logger.debug("Start of insertPPGHeader()");
		try{
			stmt = conn.prepareStatement(GET_PPG_RUN_ID);
			rs = stmt.executeQuery();
			if(rs.next()){
				runId = rs.getLong("RUN_ID");
			}
			if(runId < 0 ){
				throw new Exception("Not able to retrieve Run Id.");
			}
		}catch(Exception exception){
			logger.error("Error when retrieving PPG run id - " + exception.toString());
			throw new GeneralException("Error when retrieving PPG run id - " +
			exception.toString());
		}

		PristineDBUtil.close(rs);
		PristineDBUtil.close(stmt);
		
		
		try {
			long maxGroupId = findMaxGroupId(conn);
			
			stmt = conn.prepareStatement(INSERT_PPG_HEADER);
			int itemNoInBatch = 0;
			String addDetailAsJson = "";
			ObjectMapper mapper = new ObjectMapper();
			
	        for(PromoProductGroup ppg:productGroups){
	        	addDetailAsJson = "";
	        	int counter = 0;
	        	maxGroupId++;
	        	ppg.setGroupId(maxGroupId);
	        	stmt.setLong(++counter, runId);
	        	stmt.setLong(++counter, ppg.getGroupId());
	        	stmt.setDouble(++counter, ppg.getMinRegUnitPrice());
	        	stmt.setDouble(++counter, ppg.getMaxRegUnitPrice());
	        	stmt.setInt(++counter, ppg.getSupportedPromoType().getPromoTypeId());
	        	stmt.setInt(++counter, inputProductId);
	        	
	        	try {
	        		addDetailAsJson = mapper.writeValueAsString(ppg.getAdditionalDetail());	
				} catch (JsonProcessingException e) {
					addDetailAsJson = "";				 
				}
	        	stmt.setString(++counter, addDetailAsJson);
	        	
	        	stmt.addBatch();
	        	itemNoInBatch++;
	        	
	        	if(itemNoInBatch % Constants.BATCH_UPDATE_COUNT == 0){
	        		stmt.executeBatch();
	        		stmt.clearBatch();
	        		itemNoInBatch = 0;
	        	}
	        }
	        
	        if(itemNoInBatch > 0){
	        	stmt.executeBatch();
	        	stmt.clearBatch();
	        }
	        
		} catch (SQLException exception) {
			logger.error("Error in insertPPGHeader() - " + exception.toString());
			throw new GeneralException("Error in insertPPGHeader() - " + exception.toString());
		} finally {
			PristineDBUtil.close(stmt);
		}
	}
	
	public void insertPPGItems(Connection conn, List<PromoProductGroup> productGroups) throws GeneralException {
		PreparedStatement stmt = null;

		try {

			stmt = conn.prepareStatement(INSERT_PPG_ITEMS);
			int itemNoInBatch = 0;

			for (PromoProductGroup ppg : productGroups) {
				for (Map.Entry<ProductKey, PromoItemDTO> promoItem : ppg.getItems().entrySet()) {
					int counter = 0;

					stmt.setLong(++counter, ppg.getGroupId());
					stmt.setInt(++counter, promoItem.getKey().getProductLevelId());
					stmt.setInt(++counter, promoItem.getKey().getProductId());
					stmt.setInt(++counter, promoItem.getKey().equals(ppg.getLeadItem()) ? 1:0);
					stmt.setLong(++counter, ppg.getLastXWeeksTotalMovement().get(promoItem.getKey()) != null ? 
							ppg.getLastXWeeksTotalMovement().get(promoItem.getKey()).longValue() : 0L);
					stmt.addBatch();
					itemNoInBatch++;

					if (itemNoInBatch % Constants.BATCH_UPDATE_COUNT == 0) {
						stmt.executeBatch();
						stmt.clearBatch();
						itemNoInBatch = 0;
					}
				}
			}

			if (itemNoInBatch > 0) {
				stmt.executeBatch();
				stmt.clearBatch();
			}

		} catch (SQLException exception) {
			logger.error("Error in insertPPGItems() - " + exception.toString());
			throw new GeneralException("Error in insertPPGItems() - " + exception.toString());
		} finally {
			PristineDBUtil.close(stmt);
		}
	}
	
	public HashMap<ProductKey, Long> getLastXWeeksMovForStoreList(Connection connection, int locationLevelId, int locationId, String weekStartDate, 
			int lastXWeeks, Set<Integer> itemCodeList) throws GeneralException {
		
		long startTime = System.currentTimeMillis();
		PreparedStatement statement = null;
		ResultSet resultSet = null;
		HashMap<ProductKey, Long> lastXWeeksMov = new HashMap<ProductKey, Long>();
		
		try {
			String query = new String(GET_LAST_X_MONTHS_MOV_FOR_STORELIST);
			logger.debug("Query " + query);
			String items = PRCommonUtil.getCommaSeperatedStringFromIntSet(itemCodeList);

			query = query.replaceAll("%productIdList%", items);
			
			logger.debug("Parameters: 1-" + weekStartDate + ",2-" + weekStartDate + ",3-" + lastXWeeks + ",4-" + Constants.STORE_LEVEL_ID
					+ ",5-" + locationLevelId + ",6-" + locationId + ",7-" + Constants.STORE_LEVEL_ID 
					+ ",8-" + Constants.ITEMLEVELID + ",9-" + items);
			statement = connection.prepareStatement(query);

			int counter = 0;
			statement.setString(++counter, weekStartDate);
			statement.setString(++counter, weekStartDate);
			statement.setInt(++counter, lastXWeeks*7);
			statement.setInt(++counter, Constants.STORE_LEVEL_ID);
			statement.setInt(++counter, locationLevelId);
			statement.setInt(++counter, locationId);
			statement.setInt(++counter, Constants.STORE_LEVEL_ID);
			statement.setInt(++counter, Constants.ITEMLEVELID);
			
			statement.setFetchSize(100000);
			resultSet = statement.executeQuery();
			long endTimeTemp = System.currentTimeMillis();
			logger.debug("Time taken by statement.executeQuery() " + (endTimeTemp - startTime));
			logger.debug("Query executed");

			while (resultSet.next()) {
				ProductKey productKey = new ProductKey(Constants.ITEMLEVELID, resultSet.getInt("PRODUCT_ID"));
				Long mov = resultSet.getLong("TOT_MOVEMENT");
				lastXWeeksMov.put(productKey, mov);
			}
		} catch (SQLException e) {
			logger.error("Error while executing getLastXWeeksMovForStoreList " + e);
			throw new GeneralException("Error while executing getLastXWeeksMovForStoreList " + e);
		} finally {
			PristineDBUtil.close(resultSet);
			PristineDBUtil.close(statement);
		}
		long endTime = System.currentTimeMillis();
		logger.debug("Time taken to retrieve getLastXWeeksMovForStoreList " + (endTime - startTime));
		return lastXWeeksMov;
	}

}
