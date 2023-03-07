package com.pristine.dao;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.text.DateFormat;
import java.text.SimpleDateFormat;
//import java.text.DateFormat;
//import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Date;
//import java.util.Date;
import java.util.HashMap;
//import java.util.HashSet;
import java.util.List;
import java.util.Set;

//import javax.sql.rowset.CachedRowSet;

import org.apache.log4j.Logger;
import com.pristine.dto.StoreItemMapDTO;
import com.pristine.dto.offermgmt.PRRecommendationRunHeader;
import com.pristine.exception.GeneralException;
import com.pristine.util.Constants;
//import com.pristine.util.PrestoUtil;
import com.pristine.util.PristineDBUtil;
//import com.sun.rowset.CachedRowSetImpl;
import com.pristine.util.offermgmt.PRConstants;

public class StoreItemMapDAO implements IDAO {
	
	static Logger logger = Logger.getLogger("StoreItemMapDAO");
	
	private static final String INSERT_INTO_STORE_ITEM_MAP = "MERGE INTO STORE_ITEM_MAP STM USING "+  
			 												 " (SELECT ? LEVEL_TYPE_ID,? LEVEL_ID, ? ITEM_CODE, ? IS_AUTHORIZED FROM DUAL ) D " +
			 												 " ON (STM.LEVEL_ID = D.LEVEL_ID AND STM.ITEM_CODE = D.ITEM_CODE) " +
			 												 " WHEN MATCHED THEN UPDATE SET STM.IS_AUTHORIZED = D.IS_AUTHORIZED, STM.UPDATE_TIMESTAMP = TRUNC(SYSDATE) " +
			 												 " WHEN NOT MATCHED THEN INSERT (STM.LEVEL_TYPE_ID, STM.LEVEL_ID, STM.ITEM_CODE, STM.UPDATE_TIMESTAMP, STM.DIST_FLAG, STM.IS_AUTHORIZED) " +
			 												 " VALUES (D.LEVEL_TYPE_ID, D.LEVEL_ID, D.ITEM_CODE, TRUNC(SYSDATE),'W' , D.IS_AUTHORIZED)";
	
	private static final String GET_ALL_ITEM_CODE_LIST_FOR_REATILER_ITEM_CODE = "SELECT RETAILER_ITEM_CODE, ITEM_CODE FROM ITEM_LOOKUP WHERE ACTIVE_INDICATOR = 'Y'";
	private static final String GET_ITEM_CODE_LIST_FOR_REATILER_ITEM_CODE = "SELECT RETAILER_ITEM_CODE, ITEM_CODE FROM ITEM_LOOKUP WHERE  ACTIVE_INDICATOR = 'Y' AND RETAILER_ITEM_CODE IN (%s)";
	private static final String INSERT_NOT_PROCESSED_RECORDS = "INSERT INTO UNPROCESSED_ITEM_AUTH_RECORDS (FILENAME, FILE_EFF_DATE, PROCESS_DATE, RET_ITEM_CODE, COMP_STR_NO, ITEM_STATUS, VERSION, AUTH_REC, ZONE_STR) VALUES (?, TO_DATE(?, 'MM/DD/YYYY'), SYSDATE, ?, ?, ?, ?, ?, ?)";
	
	private static final String GET_AUTHORIZED_STORE_OF_ITEM = "SELECT LEVEL_ID, ITEM_CODE FROM STORE_ITEM_MAP WHERE LEVEL_TYPE_ID =  "
			+ Constants.STORE_LEVEL_TYPE_ID + " AND IS_AUTHORIZED = '" + Constants.YES + "'";
			//Only for debugging
			//+ " AND ITEM_CODE IN (SELECT ITEM_CODE FROM ITEM_LOOKUP WHERE RET_LIR_ID=17298) ";
			//+ " AND ITEM_CODE IN (933611,934703,761303,883376,883379,761653,761312,761350,792737,761551,761657,439733,106008)"; 
	
	
	private static final String MERGE_INTO_STORE_ITEM_MAP = "MERGE INTO STORE_ITEM_MAP STM USING " +
			//Input values
			"(SELECT ? LEVEL_TYPE_ID, ? LEVEL_ID, ? ITEM_CODE, ? VENDOR_ID, ? DIST_FLAG, ? IS_AUTHORIZED, ? PRICE_ZONE_ID FROM DUAL) D " +
			//Condition
			"ON (STM.LEVEL_TYPE_ID = D.LEVEL_TYPE_ID AND STM.LEVEL_ID = D.LEVEL_ID AND STM.ITEM_CODE = D.ITEM_CODE " +
			"AND STM.VENDOR_ID = D.VENDOR_ID)" + 
			//Update
			"WHEN MATCHED THEN UPDATE SET STM.UPDATE_TIMESTAMP = SYSDATE,  " +
			"STM.DIST_FLAG = D.DIST_FLAG, STM.IS_AUTHORIZED = D.IS_AUTHORIZED, STM.PRICE_ZONE_ID = D.PRICE_ZONE_ID " +
			//Insert
			"WHEN NOT MATCHED THEN INSERT (STM.LEVEL_TYPE_ID, STM.LEVEL_ID, STM.ITEM_CODE, STM.UPDATE_TIMESTAMP, STM.CREATE_TIMESTAMP, " +
			"STM.VENDOR_ID, STM.DIST_FLAG, STM.IS_AUTHORIZED, STM.PRICE_ZONE_ID) " +
			
			"VALUES (D.LEVEL_TYPE_ID, D.LEVEL_ID, D.ITEM_CODE, SYSDATE, SYSDATE, " +
			"D.VENDOR_ID, D.DIST_FLAG, D.IS_AUTHORIZED, D.PRICE_ZONE_ID)";
	
	public int updateIsAuthorizedFlag(Connection conn,List<StoreItemMapDTO> storeItemMapList)throws GeneralException {
		// TODO Auto-generated method stub
		PreparedStatement statement = null;
		int itemNoInBatch = 0;
		
		try{
			statement = conn.prepareStatement(INSERT_INTO_STORE_ITEM_MAP);
			
			   
			for(StoreItemMapDTO storeItemMapDTO: storeItemMapList)
			{
				statement.setInt(1, Constants.STORE_LEVEL_TYPE_ID);
				statement.setInt(2, storeItemMapDTO.getCompStrId());
				statement.setInt(3, storeItemMapDTO.getItemCode());
				statement.setString(4, storeItemMapDTO.getIsAuthorized());
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
			
			statement.close();
			conn.commit();
			storeItemMapList = null;

	    }
		catch (Exception e){
			logger.error("Error while merging items with STORE_ITEM_MAP - " + e.toString());
			e.printStackTrace();
		}finally{
			PristineDBUtil.close(statement);
		}
		
		return itemNoInBatch;
	}
	
	/**
	 * 
	 * @param conn
	 * @param storeItemMapList
	 * @return
	 * @throws GeneralException
	 */
	public int setupStoreItemMap(Connection conn,List<StoreItemMapDTO> storeItemMapList)throws GeneralException {
		
		PreparedStatement statement = null;
		int itemNoInBatch = 0;
		
		try{
			//? LEVEL_TYPE_ID, ? LEVEL_ID, ? ITEM_CODE, ? VENDOR_ID, ? DIST_FLAG, ? IS_AUTHORIZED, ? PRICE_ZONE_ID
			statement = conn.prepareStatement(MERGE_INTO_STORE_ITEM_MAP);
			   
			   
			for(StoreItemMapDTO storeItemMapDTO: storeItemMapList)
			{
				int colCount = 0;
				statement.setInt(++colCount, Constants.STORE_LEVEL_TYPE_ID);
				statement.setInt(++colCount, storeItemMapDTO.getCompStrId());
				statement.setInt(++colCount, storeItemMapDTO.getItemCode());
				
				if (storeItemMapDTO.getDistFlag().equalsIgnoreCase(String.valueOf(Constants.DSD))) 
					statement.setLong(++colCount, storeItemMapDTO.getVendorId());
				else
					statement.setNull(++colCount, java.sql.Types.BIGINT);
				
				statement.setString(++colCount, storeItemMapDTO.getDistFlag());
				statement.setString(++colCount, storeItemMapDTO.getIsAuthorized());
				statement.setInt(++colCount, storeItemMapDTO.getPriceZoneId());
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
			
			statement.close();
			conn.commit();
			storeItemMapList = null;

	    }
		catch (Exception e){
			logger.error("Error while merging items with STORE_ITEM_MAP - " + e.toString());
			e.printStackTrace();
		}finally{
			PristineDBUtil.close(statement);
		}
		
		return itemNoInBatch;
	}

	public HashMap<String, List<Integer>> getItemCodeListForRetItemcodes(Connection conn,Set<String> retailerItemCodeSet) throws GeneralException{
		int limitcount=0;
		List<String> retItemList = new ArrayList<String>();
		
	    HashMap<String, List<Integer>> itemCodeMap = new HashMap<String, List<Integer>>();
		for(String retItemCode:retailerItemCodeSet){
			retItemList.add(retItemCode);
			limitcount++;
			if( limitcount > 0 && (limitcount%Constants.BATCH_UPDATE_COUNT == 0)){
				Object[] values = retItemList.toArray();
				retrieveItemCodeListForRetItemcodes(conn, itemCodeMap, values);
            	retItemList.clear();
            }
		}
		if(retItemList.size() > 0){
			Object[] values = retItemList.toArray();
			retrieveItemCodeListForRetItemcodes(conn, itemCodeMap, values);
        	retItemList.clear();
		}
		
		//logger.info("No of Retailer Item Code passed as input - " + retailerItemCodeSet.size());
		//logger.info("No of Retailer Item Code for which UPC was fetched - " + upcMap.size());
		return itemCodeMap;
	}
	/**
	 * This method queries the database for item code for every set of Retailer Item Code
	 * @param conn			Connection
	 * @param itemCodeMap	Map that will contain the result of the database retrieval
	 * @param values		Array of Retailer Item Code that will be passed as input to the query
	 * @throws GeneralException
	 */
	private void retrieveItemCodeListForRetItemcodes(Connection conn, HashMap<String, List<Integer>> itemCodeMap, Object... values) throws GeneralException{
		//logger.debug("Inside retrieveUPCList() of ItemDAO");
		PreparedStatement statement = null;
	    ResultSet resultSet = null;
		try{
			statement = conn.prepareStatement(String.format(GET_ITEM_CODE_LIST_FOR_REATILER_ITEM_CODE, PristineDBUtil.preparePlaceHolders(values.length)));
			PristineDBUtil.setValues(statement, values);
	        resultSet = statement.executeQuery();
	        while(resultSet.next()){
	        	String retailerItemCode = resultSet.getString("RETAILER_ITEM_CODE");
	        	if(itemCodeMap.get(retailerItemCode) != null){
	        		List<Integer> upcList = itemCodeMap.get(retailerItemCode);
	        		int itemCode = resultSet.getInt("ITEM_CODE");
					upcList.add(itemCode);
					itemCodeMap.put(retailerItemCode, upcList);
	        	}else{
	        		List<Integer> upcList = new ArrayList<Integer>();
	        		int itemCode = resultSet.getInt("ITEM_CODE");
					upcList.add(itemCode);
					itemCodeMap.put(retailerItemCode, upcList);
	        	}
	        }
		}
		catch (SQLException e)
		{
			logger.error("Error while executing GET_ITEM_CODE");
			e.printStackTrace();
			throw new GeneralException("Error while executing GET_ITEM_CODE", e);
		}finally{

			PristineDBUtil.close(resultSet);
			PristineDBUtil.close(statement);
		}
	}

	private void GetAllItemCodeListForRetItemcodes(Connection conn) throws GeneralException{
		HashMap<String, List<Integer>> itemCodeMap = new HashMap<String, List<Integer>>(); 
		
		PreparedStatement statement = null;
	    ResultSet resultSet = null;
		try{
			statement = conn.prepareStatement(GET_ALL_ITEM_CODE_LIST_FOR_REATILER_ITEM_CODE);
	        resultSet = statement.executeQuery();
	        while(resultSet.next()){
	        	String retailerItemCode = resultSet.getString("RETAILER_ITEM_CODE");
	        	if(itemCodeMap.get(retailerItemCode) != null){
	        		List<Integer> list = itemCodeMap.get(retailerItemCode);
	        		int itemCode = resultSet.getInt("ITEM_CODE");
	        		list.add(itemCode);
					itemCodeMap.put(retailerItemCode, list);
	        	}else{
	        		List<Integer> list = new ArrayList<Integer>();
	        		int itemCode = resultSet.getInt("ITEM_CODE");
	        		list.add(itemCode);
					itemCodeMap.put(retailerItemCode, list);
	        	}
	        }
		}
		catch (SQLException e)
		{
			logger.error("Error while executing GET_ALL_ITEM_CODE_LIST_FOR_REATILER_ITEM_CODE");
			e.printStackTrace();
			throw new GeneralException("Error while executing GET_ALL_ITEM_CODE_LIST_FOR_REATILER_ITEM_CODE", e);
		}finally{

			PristineDBUtil.close(resultSet);
			PristineDBUtil.close(statement);
		}
	}
		
	public void saveNotProcessedRecords(Connection conn, List<StoreItemMapDTO> skippedRec, String filename, String processDate){
		PreparedStatement statement = null;
		try{
			statement = conn.prepareStatement(INSERT_NOT_PROCESSED_RECORDS);
			int count = 0;
			for(StoreItemMapDTO storeItemMapDTO: skippedRec){
				statement.setString(1, filename);
				
				if (processDate != null)
					statement.setString(2, processDate);
				else
					statement.setNull(2, java.sql.Types.DATE);
					
				statement.setString(3, storeItemMapDTO.getRetItemCode());
				statement.setString(4, storeItemMapDTO.getCompStrNo());
				statement.setString(5, storeItemMapDTO.getItemStatus());
				statement.setString(6, storeItemMapDTO.getVersion());
				statement.setString(7, storeItemMapDTO.getAuthRec());
				statement.setString(8, storeItemMapDTO.getZoneNumCombined());
				statement.addBatch();
				count++;
				
				if(count % Constants.BATCH_UPDATE_COUNT == 0){
					logger.debug("Execute batch...");
					int counter[] = statement.executeBatch();
					statement.clearBatch();
				}
				
				if(count % 10000 == 0){
					logger.debug("Commit...");
					conn.commit();
				}
			}
			if(count > 0){
				logger.debug("Execute batch for remaing data...");
				int counter[] = statement.executeBatch();
				statement.clearBatch();
				}
		}
		catch(SQLException e){
			logger.error("Error while storing skipped records" + e.toString());
			e.printStackTrace();
		}
		catch(Exception ex){
			logger.error("Error while storing skipped records" + ex.toString());
			ex.printStackTrace();
		}
	}
	
	public HashMap<Integer, List<StoreItemMapDTO>> getAuthorizedStoresOfItems(Connection conn) throws GeneralException {
		HashMap<Integer, List<StoreItemMapDTO>> itemAndAuthorizedStores = new HashMap<Integer, List<StoreItemMapDTO>>();
		PreparedStatement statement = null;
		ResultSet resultSet = null;
		List<StoreItemMapDTO> tempList = null;
		long totalRecordCount = 0;
		StoreItemMapDTO storeItemMapDTO = null;
		try {
			statement = conn.prepareStatement(GET_AUTHORIZED_STORE_OF_ITEM);
			statement.setFetchSize(200000);
			resultSet = statement.executeQuery();
			while (resultSet.next()) {
				int compStrId = resultSet.getInt("LEVEL_ID");
				if (compStrId > 0) {
					Integer itemCode = resultSet.getInt("ITEM_CODE");
					if (itemAndAuthorizedStores.get(itemCode) != null)
						tempList = itemAndAuthorizedStores.get(itemCode);
					else
						tempList = new ArrayList<StoreItemMapDTO>();

					storeItemMapDTO = new StoreItemMapDTO();
					storeItemMapDTO.setCompStrId(compStrId);
					tempList.add(storeItemMapDTO);
					itemAndAuthorizedStores.put(itemCode, tempList);
					totalRecordCount = totalRecordCount + 1;
				}
			}
			logger.debug("No of Records in Store Item Map:" + totalRecordCount);
		} catch (SQLException e) {
			logger.error("Error in getAuthorizedStoresOfItems()");
			throw new GeneralException("Error in getAuthorizedStoresOfItems()", e);
		} finally {
			PristineDBUtil.close(resultSet);
			PristineDBUtil.close(statement);
		}
		return itemAndAuthorizedStores;
	}
	
	public int UnauthorizeAllData(Connection conn){
			
			int status =0;
			
			StringBuilder sb = new StringBuilder();
			PreparedStatement stmt = null;
			
			try {
				sb.append("UPDATE STORE_ITEM_MAP SET IS_AUTHORIZED= 'N'");
				logger.debug("UnauthorizeAllData SQL: " + sb.toString());

				stmt = conn.prepareStatement(sb.toString());
				stmt.executeUpdate();
				PristineDBUtil.commitTransaction(conn, "Commit UnauthorizeAllData");

				status = 1;
			} 
			catch (SQLException ex) {
				PristineDBUtil.rollbackTransaction(conn, "Rollback UnauthorizeAllData");
				logger.error("com.pristine.dao.StoreItemMapDAO.UnauthorizeAllData() - Error while updating Store Item Map" + ex.toString());
				status = 0;
			} 
			 catch (GeneralException ex) {
					PristineDBUtil.rollbackTransaction(conn, "Rollback UnauthorizeAllData");
					logger.error("com.pristine.dao.StoreItemMapDAO.UnauthorizeAllData() - Error while updating Store Item Map" + ex.toString());
					status = 0;
			}			
			
			finally {
				PristineDBUtil.close(stmt);
			}

			return status;
		}
	
}	

