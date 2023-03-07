package com.pristine.dao;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.List;

import org.apache.log4j.Logger;

import com.pristine.exception.GeneralException;
import com.pristine.util.PrestoUtil;
import com.pristine.util.PristineDBUtil;

public class LocationGroupDAO {
	private static Logger	logger	= Logger.getLogger("LocationGroupDAO");
	
	private static final String GET_GU_STORES = "SELECT COMP_STR_NO FROM LOCATION_GROUP LG, LOCATION_GROUP_RELATION LGR, COMPETITOR_STORE CS " +
												"WHERE LG.LOCATION_LEVEL_ID = 7 AND " +
												//"LG.NAME = 'GRAND UNION STORES' " +
												"LG.LOCATION_ID = ? " +
												"AND LG.LOCATION_ID = LGR.LOCATION_ID " +
												"AND LGR.CHILD_LOCATION_ID = CS.COMP_STR_ID";
	
	private static final String GET_GU_ZONES = " SELECT DISTINCT RPZ.ZONE_NUM FROM COMPETITOR_STORE CS, RETAIL_PRICE_ZONE RPZ " +
											   " WHERE COMP_CHAIN_ID = (SELECT COMP_CHAIN_ID FROM COMPETITOR_CHAIN WHERE PRESTO_SUBSCRIBER = 'Y') " +
											   " AND COMP_STR_ID IN ( " +
											   " SELECT CS.COMP_STR_ID FROM LOCATION_GROUP LG, LOCATION_GROUP_RELATION LGR, COMPETITOR_STORE CS " +
											   " WHERE LG.LOCATION_LEVEL_ID = 7 AND " +
											   //" LG.NAME = 'GRAND UNION STORES' " +
											   " LG.LOCATION_ID = ? " +
											   " AND LG.LOCATION_ID = LGR.LOCATION_ID " +
											   " AND LGR.CHILD_LOCATION_ID = CS.COMP_STR_ID " +
											   " ) AND CS.PRICE_ZONE_ID = RPZ.PRICE_ZONE_ID";
	
	private static final String GET_TOPS_STORES = "SELECT COMP_STR_NO FROM COMPETITOR_STORE WHERE COMP_CHAIN_ID = (SELECT COMP_CHAIN_ID FROM COMPETITOR_CHAIN WHERE PRESTO_SUBSCRIBER = 'Y') " +
												  "AND COMP_STR_ID NOT IN ( " +
												  "SELECT CS.COMP_STR_ID FROM LOCATION_GROUP LG, LOCATION_GROUP_RELATION LGR, COMPETITOR_STORE CS " +
												  "WHERE LG.LOCATION_LEVEL_ID = 7 AND " +
												  //"LG.NAME = 'GRAND UNION STORES' " +
												  " LG.LOCATION_ID = ? " +
												  "AND LG.LOCATION_ID = LGR.LOCATION_ID " +
												  "AND LGR.CHILD_LOCATION_ID = CS.COMP_STR_ID)";
	
	private static final String GET_TOPS_ZONES = " SELECT DISTINCT RPZ.ZONE_NUM FROM COMPETITOR_STORE CS, RETAIL_PRICE_ZONE RPZ " +
												 " WHERE COMP_CHAIN_ID = (SELECT COMP_CHAIN_ID FROM COMPETITOR_CHAIN WHERE PRESTO_SUBSCRIBER = 'Y') " +
												 " AND COMP_STR_ID NOT IN ( " +
												 " SELECT CS.COMP_STR_ID FROM LOCATION_GROUP LG, LOCATION_GROUP_RELATION LGR, COMPETITOR_STORE CS " +
												 " WHERE LG.LOCATION_LEVEL_ID = 7 AND " +
												 //" LG.NAME = 'GRAND UNION STORES' " +
												 " LG.LOCATION_ID = ? " +
												 " AND LG.LOCATION_ID = LGR.LOCATION_ID " +
												 " AND LGR.CHILD_LOCATION_ID = CS.COMP_STR_ID " +
												 " ) AND CS.PRICE_ZONE_ID = RPZ.PRICE_ZONE_ID";
	
	private static final String GET_LOCATION_ID = "SELECT LOCATION_ID FROM LOCATION_GROUP WHERE NAME = ?";
	
	private final String GET_STORES_OF_LOCATION = "SELECT CHILD_LOCATION_ID FROM LOCATION_GROUP_RELATION WHERE LOCATION_LEVEL_ID = ? AND LOCATION_ID = ?";
	
	private Connection conn;
	
	public LocationGroupDAO(Connection conn){
		this.conn = conn;
	}
	
	public List<String> getGUStores(int guStoreListId){
		PreparedStatement stmt = null;
		ResultSet rs = null;
		
		List<String> guStoreList = new ArrayList<String>();
		try{
			stmt = conn.prepareStatement(GET_GU_STORES);
			stmt.setInt(1, guStoreListId);
			rs = stmt.executeQuery();
			while(rs.next()){
				guStoreList.add(PrestoUtil.castStoreNumber(rs.getString("COMP_STR_NO")));
			}
		}catch(SQLException se){
			logger.error("Error when retrieving gu stores - " + se);
		}finally{
			PristineDBUtil.close(rs);
			PristineDBUtil.close(stmt);
		}
		return guStoreList;
	}
	
	public List<String> getGUZones(int guStoreListId){
		PreparedStatement stmt = null;
		ResultSet rs = null;
		
		List<String> guStoreList = new ArrayList<String>();
		try{
			stmt = conn.prepareStatement(GET_GU_ZONES);
			stmt.setInt(1, guStoreListId);
			rs = stmt.executeQuery();
			while(rs.next()){
				guStoreList.add(PrestoUtil.castZoneNumber(rs.getString("ZONE_NUM")));
			}
		}catch(SQLException se){
			logger.error("Error when retrieving gu stores - " + se);
		}finally{
			PristineDBUtil.close(rs);
			PristineDBUtil.close(stmt);
		}
		return guStoreList;
	}
	
	public List<String> getTopsStores(int guStoreListId){
		PreparedStatement stmt = null;
		ResultSet rs = null;
		
		List<String> guStoreList = new ArrayList<String>();
		try{
			stmt = conn.prepareStatement(GET_TOPS_STORES);
			stmt.setInt(1, guStoreListId);
			rs = stmt.executeQuery();
			while(rs.next()){
				guStoreList.add(PrestoUtil.castStoreNumber(rs.getString("COMP_STR_NO")));
			}
		}catch(SQLException se){
			logger.error("Error when retrieving gu stores - " + se);
		}finally{
			PristineDBUtil.close(rs);
			PristineDBUtil.close(stmt);
		}
		return guStoreList;
	}
	
	public List<String> getTopsZones(int guStoreListId){
		PreparedStatement stmt = null;
		ResultSet rs = null;
		
		List<String> guStoreList = new ArrayList<String>();
		try{
			stmt = conn.prepareStatement(GET_TOPS_ZONES);
			stmt.setInt(1, guStoreListId);
			rs = stmt.executeQuery();
			while(rs.next()){
				guStoreList.add(PrestoUtil.castZoneNumber(rs.getString("ZONE_NUM")));
			}
		}catch(SQLException se){
			logger.error("Error when retrieving gu stores - " + se);
		}finally{
			PristineDBUtil.close(rs);
			PristineDBUtil.close(stmt);
		}
		return guStoreList;
	}
	
	/**
	 * Returns location id for input store list name
	 * @param storeListName
	 * @return
	 */
	public int getLocationId(String storeListName){
		PreparedStatement stmt = null;
		ResultSet rs = null;
		int locationId = -1;
		try{
			stmt = conn.prepareStatement(GET_LOCATION_ID);
			stmt.setString(1, storeListName);
			rs = stmt.executeQuery();
			
			if(rs.next()){
				locationId = rs.getInt("LOCATION_ID");
			}
		}catch(SQLException exception){
			logger.error("Error when retrieving location id- " + exception);
		}finally{
			PristineDBUtil.close(rs);
			PristineDBUtil.close(stmt);
		}
		return locationId;
	}
	
	public List<Integer> getStoresOfLocation(int locationLevelId, int locationId) throws GeneralException {  

		PreparedStatement stmt = null;
		ResultSet rs = null;
		List<Integer> storeList = new ArrayList<Integer>();

		try {
			stmt = conn.prepareStatement(GET_STORES_OF_LOCATION);
			stmt.setInt(1, locationLevelId);
			stmt.setInt(2, locationId);
			logger.debug(GET_STORES_OF_LOCATION);
			rs = stmt.executeQuery();
			while (rs.next()) {
				if (rs.getObject("CHILD_LOCATION_ID") != null) {
					storeList.add(rs.getInt("CHILD_LOCATION_ID"));
				}
			}
		} catch (Exception e) {
			logger.error("Error while retrieving store list " + e.getMessage());
			throw new GeneralException("Error in getStoresOfStoreList() - " + e);
		} finally {
			PristineDBUtil.close(rs);
			PristineDBUtil.close(stmt);
		}
		return storeList;
	}
}
