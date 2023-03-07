package com.pristine.dao;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.Date;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

import javax.sql.rowset.CachedRowSet;

import org.apache.log4j.Logger;

import com.pristine.dto.CostDTO;
import com.pristine.dto.RetailCostDTO;
import com.pristine.exception.GeneralException;
import com.pristine.util.Constants;
import com.pristine.util.DateUtil;
import com.pristine.util.PristineDBUtil;

@SuppressWarnings("unused")
public class CostDAO implements IDAO {
	private static Logger	logger	= Logger.getLogger("CostDataDAO");
	
	private static final String GET_STORE_ZONE_INFO = "SELECT RPZ.ZONE_NUM, CS.COMP_STR_NO FROM COMPETITOR_STORE CS " +
			 										  "INNER JOIN RETAIL_PRICE_ZONE RPZ ON CS.PRICE_ZONE_ID = RPZ.PRICE_ZONE_ID " +
			 										  "WHERE COMP_CHAIN_ID = ?";
	
	private static final String INSERT_INTO_STORE_ITEM_MAP = "MERGE INTO STORE_ITEM_MAP STM USING " +
															//Input values
															"(SELECT ? LEVEL_TYPE_ID, ? LEVEL_ID, ? ITEM_CODE, ? STATUS, ? PRICE_INDICATOR, ? VENDOR_ID, ? DIST_FLAG FROM DUAL) D " +
															//Condition
															"ON (STM.LEVEL_TYPE_ID = D.LEVEL_TYPE_ID AND STM.LEVEL_ID = D.LEVEL_ID AND STM.ITEM_CODE = D.ITEM_CODE " +
															"AND STM.VENDOR_ID = D.VENDOR_ID)" + 
															//Update
															"WHEN MATCHED THEN UPDATE SET STM.UPDATE_TIMESTAMP = SYSDATE, STM.PRICE_INDICATOR = D.PRICE_INDICATOR, " +
															"STM.DIST_FLAG = D.DIST_FLAG, STM.STATUS = D.STATUS,  STM.PRICE_UPDATE_TIMESTAMP = SYSDATE " +
															//Insert
															"WHEN NOT MATCHED THEN INSERT (STM.LEVEL_TYPE_ID, STM.LEVEL_ID, STM.ITEM_CODE, STM.UPDATE_TIMESTAMP, STM.CREATE_TIMESTAMP, " + 
															"STM.STATUS, STM.PRICE_INDICATOR, STM.VENDOR_ID, STM.DIST_FLAG, STM.PRICE_UPDATE_TIMESTAMP) " +
															
															"VALUES (D.LEVEL_TYPE_ID, D.LEVEL_ID, D.ITEM_CODE, SYSDATE, SYSDATE, " +
															"D.STATUS, D.PRICE_INDICATOR, D.VENDOR_ID, D.DIST_FLAG, SYSDATE)";
	
	
	private static final String INSERT_INTO_STORE_ITEM_MAPV2 = "MERGE INTO STORE_ITEM_MAP STM USING " +
															//Input values
															"(SELECT ? LEVEL_TYPE_ID, ? LEVEL_ID, ? ITEM_CODE, ? STATUS, ? COST_INDICATOR, ? VENDOR_ID, ? DIST_FLAG, ? IS_AUTHORIZED FROM DUAL) D " +
															//Condition
															"ON (STM.LEVEL_TYPE_ID = D.LEVEL_TYPE_ID AND STM.LEVEL_ID = D.LEVEL_ID AND STM.ITEM_CODE = D.ITEM_CODE " +
															"AND STM.VENDOR_ID = D.VENDOR_ID)" + 
															//Update
															"WHEN MATCHED THEN UPDATE SET STM.UPDATE_TIMESTAMP = SYSDATE, STM.COST_INDICATOR =  D.COST_INDICATOR, " +
															"STM.DIST_FLAG = D.DIST_FLAG, STM.IS_AUTHORIZED = D.IS_AUTHORIZED, STM.STATUS = D.STATUS,  STM.COST_UPDATE_TIMESTAMP = SYSDATE " +
															//Insert
															"WHEN NOT MATCHED THEN INSERT (STM.LEVEL_TYPE_ID, STM.LEVEL_ID, STM.ITEM_CODE, STM.UPDATE_TIMESTAMP, STM.CREATE_TIMESTAMP, " +
															"STM.STATUS, STM.COST_INDICATOR, STM.VENDOR_ID, STM.DIST_FLAG, STM.IS_AUTHORIZED, STM.COST_UPDATE_TIMESTAMP) " +
															
															"VALUES (D.LEVEL_TYPE_ID, D.LEVEL_ID, D.ITEM_CODE, SYSDATE, SYSDATE, " +
															"D.STATUS, D.COST_INDICATOR, D.VENDOR_ID, D.DIST_FLAG, D.IS_AUTHORIZED, SYSDATE)";
	

	private static final String RETRIEVE_ITEM_STORE_MAPPING = "SELECT LEVEL_TYPE_ID, LEVEL_ID, ITEM_CODE, CREATE_TIMESTAMP, UPDATE_TIMESTAMP, STATUS, COST_INDICATOR, "
			+ "PRICE_INDICATOR FROM STORE_ITEM_MAP WHERE ITEM_CODE IN (%s)";
	
	private static final String RETRIEVE_ITEM_STORE_MAPPING_ONLY_AUTH = "SELECT LEVEL_TYPE_ID, LEVEL_ID, ITEM_CODE, CREATE_TIMESTAMP, UPDATE_TIMESTAMP, STATUS, COST_INDICATOR, "
			+ "PRICE_INDICATOR FROM STORE_ITEM_MAP WHERE ITEM_CODE IN (%s) AND IS_AUTHORIZED = 'Y'";
	
	private static final String RETRIEVE_MAPPING_FOR_STORE= "SELECT LEVEL_TYPE_ID, LEVEL_ID, ITEM_CODE, CREATE_TIMESTAMP, UPDATE_TIMESTAMP, STATUS FROM STORE_ITEM_MAP " +
			  												"WHERE LEVEL_ID = ? and ITEM_CODE IN (%s)";
	
	private final int commitCount = Constants.LIMIT_COUNT;
	
	public boolean insertCostData(Connection conn, CostDTO costDTO ) throws GeneralException {
	
		boolean retVal = true;
		//Setup Change Direction
		
		/*
		CHECK_DATA_ID, LIST_COST, EFF_LIST_COST_DATE, DEAL_COST, DEAL_START_DATE, DEAL_END_DATE, COST_CHG_DIRECTION */
		
		//See if the record exists
		//If not insert
		//else update
		
		try{
			setupCostChangeDirection(conn, costDTO);

			StringBuffer sb = new StringBuffer();
			
			
			sb.append("SELECT A.CHECK_DATA_ID COMP_CHECK_DATA_ID, NVL(B.CHECK_DATA_ID, -1) MOV_CHECK_DATA_ID FROM ");
			sb.append(" COMPETITIVE_DATA A, MOVEMENT_WEEKLY B WHERE ");
			sb.append(" A.SCHEDULE_ID = ").append(costDTO.scheduleId);
			sb.append(" AND A.ITEM_CODE = ").append(costDTO.itemCode);
			sb.append(" AND B.CHECK_DATA_ID (+)= A.CHECK_DATA_ID");
			
			//logger.debug( "Query - " + sb.toString());
			CachedRowSet result = PristineDBUtil.executeQuery(conn, sb, "getcheckDataId");

			if( result.next()){
				costDTO.checkItemId = result.getInt("COMP_CHECK_DATA_ID");
				boolean insert = (result.getInt("MOV_CHECK_DATA_ID") == -1 )? true: false;
			
				sb = new StringBuffer();
	
				if( insert ){
					sb.append("INSERT INTO MOVEMENT_WEEKLY ( ");
					sb.append(" CHECK_DATA_ID, COMP_STR_ID, ITEM_CODE, ");
					sb.append(" LIST_COST, DEAL_COST, EFF_LIST_COST_DATE, ");
					sb.append(" DEAL_START_DATE, DEAL_END_DATE, COST_CHG_DIRECTION ) VALUES (");
					sb.append( costDTO.checkItemId).append(", ");
					sb.append( costDTO.compStrId).append(", ");
					sb.append( costDTO.itemCode).append(", ");
					sb.append( costDTO.listCost).append(", ");
					sb.append( costDTO.dealCost).append(", ");
					
					if( costDTO.listCostEffDate == null || costDTO.listCostEffDate.equals("") )
						sb.append(" NULL, ");
					else
						sb.append(" TO_DATE( '").append(costDTO.listCostEffDate).append("','MM/DD/YYYY'), " );
					
					if( costDTO.promoCostStartDate== null || costDTO.promoCostStartDate.equals("") )
						sb.append(" NULL, ");
					else
						sb.append(" TO_DATE( '").append(costDTO.promoCostStartDate).append("','MM/DD/YYYY'), " );
		
					if( costDTO.promoCostEndDate== null || costDTO.promoCostEndDate.equals("") )
						sb.append(" NULL, ");
					else
						sb.append(" TO_DATE( '").append(costDTO.promoCostEndDate).append("','MM/DD/YYYY'), " );
					sb.append(costDTO.costChgDirection).append(")");
					//logger.debug( "Insert Query" + sb.toString());
					PristineDBUtil.execute(conn, sb, "Cost Data - insert");
				}else{
					
					sb.append( " UPDATE MOVEMENT_WEEKLY SET ");
					sb.append( " CHECK_DATA_ID = " );
					sb.append( costDTO.checkItemId).append(", ");
					sb.append( " LIST_COST = " );
					sb.append( costDTO.listCost).append(", ");
					sb.append( " DEAL_COST = " );
					sb.append( costDTO.dealCost).append(", ");
					sb.append( " EFF_LIST_COST_DATE = " );
					if( costDTO.listCostEffDate == null || costDTO.listCostEffDate.equals("") )
						sb.append(" NULL, ");
					else
						sb.append(" TO_DATE( '").append(costDTO.listCostEffDate).append("','MM/DD/YYYY'), " );
					
					sb.append( " DEAL_START_DATE = " );
					if( costDTO.promoCostStartDate== null || costDTO.promoCostStartDate.equals("") )
						sb.append(" NULL, ");
					else
						sb.append(" TO_DATE( '").append(costDTO.promoCostStartDate).append("','MM/DD/YYYY'), " );
		
					sb.append( " DEAL_END_DATE = " );
					if( costDTO.promoCostEndDate== null || costDTO.promoCostEndDate.equals("") )
						sb.append(" NULL, ");
					else
						sb.append(" TO_DATE( '").append(costDTO.promoCostEndDate).append("','MM/DD/YYYY'), " );
		
					sb.append( " COST_CHG_DIRECTION = " );
					sb.append(costDTO.costChgDirection);
					sb.append(" WHERE CHECK_DATA_ID = ");
					sb.append(costDTO.checkItemId);
					//logger.debug( "Update Query" + sb.toString());
					int updateCount = PristineDBUtil.executeUpdate(conn, sb, "Cost DATA - Update");
					if ( updateCount != 1){
						logger.error("Update Cost Data Unsuccessful, record count = " + updateCount);
						retVal = false;
					}
				}
			}else{
				retVal = false;
			}
		}catch( SQLException sqle){
			retVal = false;
			throw new GeneralException( "Cached Row Exception ", sqle);
		}
		
		return retVal;
	}
	
	private void setupCostChangeDirection(Connection conn, CostDTO costDTO) throws GeneralException, SQLException {
		costDTO.costChgDirection = Constants.PRICE_NA;
		if( costDTO.listCostEffDate!= null && !costDTO.listCostEffDate.isEmpty() ){
			Date currentWeekStartDate = DateUtil.toDate(costDTO.weekStartDate);
			Date effRegCostStartDate = currentWeekStartDate; //Default to current start which forces to lookup history
			if( !costDTO.listCostEffDate.equals (""))
				effRegCostStartDate = DateUtil.toDate(costDTO.listCostEffDate);
			
			//Price is not changed
			if( DateUtil.getDateDiff(currentWeekStartDate, effRegCostStartDate) > 8 && !costDTO.isPriceChanged )
				costDTO.costChgDirection = Constants.PRICE_NO_CHANGE;
			else{
				//get the previous cost and compute.
				StringBuffer sb = new StringBuffer();
				sb.append(" SELECT A.LIST_COST FROM MOVEMENT_WEEKLY A, COMPETITIVE_DATA B ");
				sb.append(" WHERE A.COMP_STR_ID = ").append(costDTO.compStrId); 
				sb.append(" AND A.ITEM_CODE = ").append(costDTO.itemCode);
				sb.append(" AND A.CHECK_DATA_ID <> ").append(costDTO.checkItemId);
				sb.append(" AND B.CHECK_DATA_ID = A.CHECK_DATA_ID ");
				sb.append(" ORDER BY B.CHECK_DATETIME DESC ");
				//logger.debug("Cost chg query: " + sb.toString());
				CachedRowSet result = PristineDBUtil.executeQuery(conn, sb, "getCostChangeDirection");
				if( result.next()){
					float prevCost=  result.getFloat("LIST_COST");
					if( costDTO.listCost > prevCost)
						costDTO.costChgDirection = Constants.PRICE_WENT_UP;
					else if ( costDTO.listCost < prevCost)
						costDTO.costChgDirection = Constants.PRICE_WENT_DOWN;
					else if ( Math.abs(costDTO.listCost - prevCost) < 0.01f)
						costDTO.costChgDirection = Constants.PRICE_NO_CHANGE;
				}
				
			}
		}
	}

	public CostDTO getCompDataForItem(Connection conn, int checkItemId) throws GeneralException, SQLException {
		// TODO Auto-generated method stub
		CostDTO costInfo = null;
		StringBuffer sb = new StringBuffer();
		sb.append(" SELECT A.LIST_COST, NVL(A.DEAL_COST,0) DEAL_COST , ");
		sb.append(" NVL(B.LIST_COST,0) LIG_LIST_COST, NVL(B.DEAL_COST,0) LIG_DEAL_COST ");
		sb.append(" FROM MOVEMENT_WEEKLY A, MOVEMENT_WEEKLY_LIG B");
		sb.append(" WHERE A.CHECK_DATA_ID = ").append(checkItemId);
		sb.append(" AND B.CHECK_DATA_ID (+) = A.CHECK_DATA_ID"); 
		CachedRowSet result = PristineDBUtil.executeQuery(conn, sb, "getCostChangeDirection");
		
		if( result.next()){
			costInfo = new CostDTO();
			//float ligListCost=  result.getFloat("LIG_LIST_COST");
			/*if( ligListCost > 0){
				costInfo.listCost = ligListCost;
				costInfo.dealCost = result.getFloat("LIG_DEAL_COST");
			}else
			*/
			{
				costInfo.listCost = result.getFloat("LIST_COST");
				costInfo.dealCost = result.getFloat("DEAL_COST");				
			}
		}
		return costInfo;
	}

	/**
	 * This method queries for store number and zone number for stores that are available for a particular chain
	 * @param chainId	Chain ID
	 * @return HashMap
	 * @throws GeneralException
	 */
	public HashMap<String, String> getStoreZoneInfo(Connection conn, String chainId) throws GeneralException{
		logger.debug("Inside getStoreZoneInfo() of CostDAO");
		HashMap<String, String> storeNumberMap = new HashMap<String, String>();
		PreparedStatement statement = null;
	    ResultSet resultSet = null;
		try{
			statement = conn.prepareStatement(GET_STORE_ZONE_INFO);
	        statement.setString(1, chainId);
	        resultSet = statement.executeQuery();
	        while(resultSet.next()){
	        	storeNumberMap.put(resultSet.getString("COMP_STR_NO"), resultSet.getString("ZONE_NUM"));
	        }
		}catch (SQLException e){
			logger.error("Error while executing GET_STORE_ZONE_INFO");
			throw new GeneralException("Error while executing GET_STORE_ZONE_INFO", e);
		}finally{
			PristineDBUtil.close(resultSet, statement);
		}
		return storeNumberMap;
	}

	/**
	 * This method inserts/updates/deletes data in store_item_map table
	 * @param conn			Database Connection
	 * @param insertList	List of items to be inserted
	 * @param updateList	List of items to be updated
	 * @param deleteList	List of items to be deleted
	 * @param startDate		Start date of the week being processed
	 */
	@SuppressWarnings("resource")
	public void mergeIntoStoreItemMap(Connection conn, List<RetailCostDTO> insertList, List<RetailCostDTO> deleteList, boolean costIndicator){
		logger.debug("Inside mergeIntoStoreItemMap() of CostDAO");
		try {
			boolean deleteFlag = false;
			mapItemsWithStore(conn, insertList, costIndicator, deleteFlag);
			deleteFlag = true;
			mapItemsWithStore(conn, deleteList, costIndicator, deleteFlag);
		} catch (GeneralException e) {
			// TODO Auto-generated catch block
			logger.error("mergeIntoStoreItemMap() - Error while executing UPDATE_STORE_ITEM_MAP during delete - " + e); 
			e.printStackTrace();
		}
	}
	
	/**
	 * Insert/Update store/zone mapping with items
	 * @param values			Contains the input with which mapping needs to be created		
	 * @throws GeneralException 
	 */
	public void mapItemsWithStore(Connection conn, List<RetailCostDTO> retailCostDTOList,  boolean costIndicator, boolean deleteFlag) throws GeneralException{
		logger.debug("Inside mapItemsWithStore() of RetailPriceDAO");
		PreparedStatement statement = null;
		try{
			String itemStatus =  Constants.ITEM_STATUS_ACTIVE;
			String costStaus = Constants.COST_INDICATOR;
			String priceStatus = Constants.PRICE_INDICATOR;
			String isAuthorized = Constants.AUTHORIZED_ITEM;
			
			if(deleteFlag){
				itemStatus = Constants.ITEM_STATUS_INACTIVE;
				costStaus = Constants.COST_INDICATOR_N;
				isAuthorized = Constants.UN_AUTHORIZED_ITEM;
				priceStatus = Constants.PRICE_INDICATOR_N;
			}
			if(costIndicator)
				statement = conn.prepareStatement(INSERT_INTO_STORE_ITEM_MAPV2);
			else
				statement = conn.prepareStatement(INSERT_INTO_STORE_ITEM_MAP);
			
			int itemNoInBatch = 0;
		    for(RetailCostDTO retailCostDTO:retailCostDTOList){
		    	int counter = 0;
		    	statement.setInt(++counter, retailCostDTO.getLevelTypeId());
			    statement.setString(++counter, retailCostDTO.getLevelId());
			    statement.setString(++counter, retailCostDTO.getItemcode());
			    statement.setString(++counter, itemStatus);
			    if(costIndicator)
			    	statement.setString(++counter, costStaus);
			    else
			    	statement.setString(++counter, priceStatus);
			    statement.setLong(++counter, retailCostDTO.getVendorId());
			    statement.setString(++counter, retailCostDTO.getDistFlag());
			    if(costIndicator)
			    	statement.setString(++counter, isAuthorized);
			    
			    statement.addBatch();
			    itemNoInBatch++;
			        	
			    if(itemNoInBatch % Constants.BATCH_UPDATE_COUNT == 0){
			    	int[] count = statement.executeBatch();
			        statement.clearBatch();
			        itemNoInBatch = 0;
			    }
		    }
			if(itemNoInBatch > 0){
	        	
				int[] count = statement.executeBatch();
        		statement.clearBatch();
	        }
	    }
		catch (SQLException e){
			logger.error("Error while executing MERGE_INTO_STORE_ITEM_MAP - " + e);
		}finally{
			PristineDBUtil.close(statement);
		}
		
	}
	
	
	
	public List<Long> getWarehouseVendors(Connection conn) throws GeneralException{
		logger.debug("Inside getWarehouseVendors() of CostDAO");
		PreparedStatement statement = null;
		ResultSet resultSet = null;
		List<Long> warehouseVendors = new ArrayList<Long>();
		try{
			statement = conn.prepareStatement("SELECT VENDOR_ID FROM VENDOR_LOOKUP WHERE DIST_FLAG='" + Constants.WAREHOUSE + "'");
			resultSet = statement.executeQuery();
			while(resultSet.next()){
				Long vendorNo = resultSet.getLong("VENDOR_ID");
				warehouseVendors.add(vendorNo);
			}
		}
		catch(SQLException sqlE){
			logger.error("getWarehouseVendors() - Error while retreiving warehouse vendors" + sqlE.toString());
			throw new GeneralException("getWarehouseVendors() - Error while retreiving warehouse vendors");
		} finally {
			PristineDBUtil.close(statement);
		}
		return warehouseVendors;
	}
	
	public HashMap<String, List<String>> getStoreItemMap(Connection conn, String inputDate, Set<String> itemCodeSet,
			String fileType, boolean authorizedItemsOnly) throws GeneralException {
		logger.debug("Inside getStoreItemMap() of CostDAO");
		int limitcount = 0;
		List<String> itemCodeList = new ArrayList<String>();

		HashMap<String, List<String>> storeItemMap = new HashMap<String, List<String>>();
		for (String itemCode : itemCodeSet) {
			itemCodeList.add(itemCode);
			limitcount++;
			if (limitcount > 0 && (limitcount % this.commitCount == 0)) {
				Object[] values = itemCodeList.toArray();
				
				
				if(authorizedItemsOnly)
					retrieveItemStoreMapWithAuth(conn, inputDate, storeItemMap, values);
				else
					retrieveItemStoreMapping(conn, inputDate, storeItemMap, values, fileType);
					
				itemCodeList.clear();
			}
		}
		if (itemCodeList.size() > 0) {
			Object[] values = itemCodeList.toArray();
			
			if(authorizedItemsOnly)
				retrieveItemStoreMapWithAuth(conn, inputDate, storeItemMap, values);
			else
				retrieveItemStoreMapping(conn, inputDate, storeItemMap, values, fileType);
			itemCodeList.clear();
		}
		// To remove duplicate items from list
		HashMap<String, List<String>> finalItemMap = new HashMap<String, List<String>>();
		storeItemMap.forEach((key, value)->{
			HashSet<String> tempList = new HashSet<String>(value);
			finalItemMap.put(key, new ArrayList<String>(tempList));
		});
		return finalItemMap;
	}
	
	public void retrieveItemStoreMapping(Connection conn, String inputDate, HashMap<String, List<String>> storeItemMap,
			Object[] values, String fileType) throws GeneralException {
		logger.debug("Inside retrieveItemStoreMapping() of CostDAO");
		PreparedStatement statement = null;
		ResultSet resultSet = null;
		// Date inputDt = DateUtil.toSQLDate(inputDate);

		try {
			statement = conn.prepareStatement(String.format(RETRIEVE_ITEM_STORE_MAPPING, PristineDBUtil.preparePlaceHolders(values.length)));
			PristineDBUtil.setValues(statement, values);
			resultSet = statement.executeQuery();
			int levelTypeId = -1;
			String levelId = null;
			String itemCode = null;
			String key = null;
			boolean itemToBeAdded = false;
			while (resultSet.next()) {
				itemToBeAdded = false;

				// Changed code to get item based on either Price or Cost
				// indicator to skip inactive stores.. By DINESH(03/14/17)
				if (fileType.toUpperCase().equals("PRICE")) {
					itemToBeAdded = resultSet.getString("PRICE_INDICATOR").equals("Y") ? true : false;
				} else if (fileType.toUpperCase().equals("COST")) {
					itemToBeAdded = resultSet.getString("COST_INDICATOR").equals("Y") ? true : false;
				}
				if (itemToBeAdded) {
					levelTypeId = resultSet.getInt("LEVEL_TYPE_ID");
					levelId = resultSet.getString("LEVEL_ID");
					itemCode = resultSet.getString("ITEM_CODE");
					key = levelTypeId + Constants.INDEX_DELIMITER + levelId;
					List<String> itemList = new ArrayList<String>();
					
					// Check whether the item is already added
					// **This condition added because an item can be
					// duplicated in STORE_ITEM_MAP based on multiple
					// vendors**
					// **Added by Pradeep on 09/20/2017 to fix price issue
					// in TOPS
					if (storeItemMap.containsKey(key)) {
						itemList = storeItemMap.get(key);
					}
					itemList.add(itemCode);

					// To remove duplicate item code in the list
//					HashSet<String> tempItemList = new HashSet<String>(itemList);
					storeItemMap.put(key, itemList);
				}
			}
		} catch (SQLException e) {
			logger.error("Error while executing RETRIEVE_ITEM_STORE_MAPPING - " + e);
			throw new GeneralException("Error while executing RETRIEVE_ITEM_STORE_MAPPING", e);
		} finally {
			PristineDBUtil.close(resultSet);
			PristineDBUtil.close(statement);
		}
	}
	
	
	public void retrieveItemStoreMapWithAuth(Connection conn, String inputDate, HashMap<String, List<String>> storeItemMap,
 Object[] values) throws GeneralException {
		logger.debug("Inside retrieveItemStoreMapping() of CostDAO");
		PreparedStatement statement = null;
		ResultSet resultSet = null;
		// Date inputDt = DateUtil.toSQLDate(inputDate);

		try {
			statement = conn.prepareStatement(String.format(RETRIEVE_ITEM_STORE_MAPPING_ONLY_AUTH,
					PristineDBUtil.preparePlaceHolders(values.length)));
			PristineDBUtil.setValues(statement, values);
			resultSet = statement.executeQuery();
			int levelTypeId = -1;
			String levelId = null;
			String itemCode = null;
			String key = null;
			while (resultSet.next()) {
				levelTypeId = resultSet.getInt("LEVEL_TYPE_ID");
				levelId = resultSet.getString("LEVEL_ID");
				itemCode = resultSet.getString("ITEM_CODE");
				key = levelTypeId + Constants.INDEX_DELIMITER + levelId;
				// Check whether the item is already added
				// **This condition added because an item can be
				// duplicated in STORE_ITEM_MAP based on multiple
				// vendors**
				// **Added by Pradeep on 09/20/2017 to fix price issue
				// in TOPS
				List<String> itemList = new ArrayList<String>();
				if (storeItemMap.containsKey(key)) {
					itemList = storeItemMap.get(key);
				}
				itemList.add(itemCode);
				storeItemMap.put(key, itemList);
			}
		} catch (SQLException e) {
			logger.error("Error while executing RETRIEVE_ITEM_STORE_MAPPING - " + e);
			throw new GeneralException("Error while executing RETRIEVE_ITEM_STORE_MAPPING", e);
		} finally {
			PristineDBUtil.close(resultSet);
			PristineDBUtil.close(statement);
		}
	}
	
	public HashMap<String, List<String>> getItemsInStore(Connection conn, String compStrNo, String inputDate, Set<String> itemCodeSet) throws GeneralException{
		logger.debug("Inside getItemsInStore() of CostDAO");
		int limitcount=0;
		List<String> itemCodeList = new ArrayList<String>();
		
		HashMap<String, List<String>> storeItemMap = new HashMap<String, List<String>>();
		for(String itemCode:itemCodeSet){
			itemCodeList.add(itemCode);
			limitcount++;
			if( limitcount > 0 && (limitcount%this.commitCount == 0)){
				Object[] values = itemCodeList.toArray();
				retrieveItemsForStore(conn, compStrNo, inputDate, storeItemMap, values);
				itemCodeList.clear();
            }
		}
		if(itemCodeList.size() > 0){
			Object[] values = itemCodeList.toArray();
			retrieveItemsForStore(conn, compStrNo, inputDate, storeItemMap, values);
			itemCodeList.clear();
		}
		
		return storeItemMap;
	}
	/**
	 * 
	 * @param compStrNo
	 * @param itemCodeList
	 * @return
	 * @throws GeneralException
	 */
	public void retrieveItemsForStore(Connection connection, String compStrNo, String inputDate, HashMap<String, List<String>> storeItemMap, Object[] values) throws GeneralException{
		logger.debug("Inside retrieveItemsForStore() of CompetitiveDataDAOV2");
		PreparedStatement statement = null;
	    ResultSet resultSet = null;
	    List<String> itemList = new ArrayList<String>();
	    Date inputDt = DateUtil.toSQLDate(inputDate);
	    
	    try{
	    	statement = connection.prepareStatement(String.format(RETRIEVE_MAPPING_FOR_STORE, PristineDBUtil.preparePlaceHolders(values.length)));
			statement.setString(1, compStrNo);
	    	PristineDBUtil.setValues(statement, 2, values);
			
	        resultSet = statement.executeQuery();
	        boolean itemToBeAdded = false;
	        while(resultSet.next()){
	        	itemToBeAdded = false;
	        	if(Constants.ITEM_STATUS_ACTIVE.equals(resultSet.getString("STATUS")) && (DateUtil.getDateDiff(inputDt, resultSet.getDate("CREATE_TIMESTAMP")) >= 0)){
	        		itemToBeAdded = true;
	        	}
	        	
	        	if(Constants.ITEM_STATUS_INACTIVE.equals(resultSet.getString("STATUS")) && (DateUtil.getDateDiff(inputDt, resultSet.getDate("CREATE_TIMESTAMP")) < 0 ||
	        			DateUtil.getDateDiff(inputDt, resultSet.getDate("UPDATE_TIMESTAMP")) >= 0 )){
	        		itemToBeAdded = true;
	        	}
	        	
	        	if(itemToBeAdded){
		        	String key = null;
			        int levelTypeId = resultSet.getInt("LEVEL_TYPE_ID");
			        String levelId = resultSet.getString("LEVEL_ID");
			        String itemCode = resultSet.getString("ITEM_CODE");
			        key = levelTypeId + Constants.INDEX_DELIMITER + levelId;
			        if(storeItemMap.containsKey(key)){
			        	itemList = storeItemMap.get(key);
			        	itemList.add(itemCode);
			        }else{
			        	itemList = new ArrayList<String>();
			        	itemList.add(itemCode);
			        }
			        storeItemMap.put(key, itemList);
	        	}
	        }
		}
		catch (SQLException e)
		{
			logger.error("Error while executing RETRIEVE_MAPPING_FOR_STORE");
			throw new GeneralException("Error while executing RETRIEVE_MAPPING_FOR_STORE", e);
		}finally{
			PristineDBUtil.close(resultSet, statement);
		}
	}
}
