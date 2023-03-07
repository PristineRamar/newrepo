package com.pristine.dao.offermgmt.promotion;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;

import org.apache.log4j.Logger;

import com.pristine.dto.offermgmt.promotion.PromoDisplay;
import com.pristine.exception.GeneralException;
import com.pristine.util.Constants;
import com.pristine.util.PristineDBUtil;

public class DisplayTypeDAO {
	static Logger logger = Logger.getLogger("DisplayTypeDAO");
	
	private Connection conn = null;
	
	private static final String GET_DISPLAY_TYPES = "SELECT DISPLAY_NAME, DISPLAY_TYPE_ID FROM PM_DISPLAY_TYPE_LOOKUP";
	private static final String INSERT_PROMO_DISPLAY = "INSERT INTO PM_PROMO_DISPLAY (PROMO_DEFINITION_ID, LOCATION_LEVEL_ID, LOCATION_ID, DISPLAY_TYPE_ID, SUB_DISPLAY_TYPE_ID, WEEK_CALENDAR_ID) VALUES(?, ?, ?, ?, ?, ?)";
	private static final String SELECT_PROMO_DISPLAY = "SELECT PROMO_DEFINITION_ID, DISPLAY_TYPE_ID, WEEK_CALENDAR_ID FROM PM_PROMO_DISPLAY";
	private static final String UPDATE_PROMO_DISPLAY = "UPDATE PM_PROMO_DISPLAY SET DISPLAY_TYPE_ID = ?, WEEK_CALENDAR_ID = ?, SUB_DISPLAY_TYPE_ID = ? WHERE PROMO_DEFINITION_ID = ?";
	public DisplayTypeDAO(Connection conn){
		this.conn = conn;
	}
	
	public HashMap<String, Integer> getDisplayTypes(){
		PreparedStatement stmt = null;
		ResultSet rs = null;
		HashMap<String, Integer> displayTypeMap = new HashMap<String, Integer>();
		try{
			stmt = conn.prepareStatement(GET_DISPLAY_TYPES);
			rs = stmt.executeQuery();
			while(rs.next()){
				displayTypeMap.put(rs.getString("DISPLAY_NAME"), rs.getInt("DISPLAY_TYPE_ID"));
			}
		}catch(SQLException e){
			logger.error("Error when retrieving display types " + e);
		}finally{
			PristineDBUtil.close(rs);
			PristineDBUtil.close(stmt);
		}
		return displayTypeMap;
	}
	

	
	public void insertOrUpdatePromoDispay(Connection conn, List<PromoDisplay> displayList) throws GeneralException{
		List<PromoDisplay> insertList = new ArrayList<PromoDisplay>();
		List<PromoDisplay> updateList = new ArrayList<PromoDisplay>();
		try{
			HashMap<String, Integer> promoAndDisplayTypeMap = getExistingPromoDisplay(conn);
			for(PromoDisplay promoDisplay: displayList){
				if(promoAndDisplayTypeMap.get(promoDisplay.getPromoDefId() + "-" + promoDisplay.getCalendarId()) != null)
					updateList.add(promoDisplay);
				else
					insertList.add(promoDisplay);
			}
			updatePromoDisplay(conn, updateList);
			insertPromoDisplay(conn, insertList);
		}
		catch(GeneralException ge){
			logger.info("Error insert/update promo display" + ge);
			throw new GeneralException("Error insert/update promo display", ge);
		}
	}
	
	private HashMap<String, Integer> getExistingPromoDisplay(Connection conn) throws GeneralException{
		PreparedStatement stmt = null;
		ResultSet rs = null;
		HashMap<String, Integer> promoAndDisplayTypeMap = new HashMap<String, Integer>();
		try{
			stmt = conn.prepareStatement(SELECT_PROMO_DISPLAY);
			rs = stmt.executeQuery();
			while(rs.next()){
				promoAndDisplayTypeMap.put(rs.getLong("PROMO_DEFINITION_ID") + "-" + rs.getInt("WEEK_CALENDAR_ID"), rs.getInt("DISPLAY_TYPE_ID"));
			}
		}
		catch(SQLException sqlE){
			logger.info("Error while getting promo display" + sqlE);
			throw new GeneralException("Error while updating promo display", sqlE);
		}
		return promoAndDisplayTypeMap;
	}
	
	private void updatePromoDisplay(Connection conn, List<PromoDisplay> updateList) throws GeneralException{
		PreparedStatement stmt = null;
		try{
			stmt = conn.prepareStatement(UPDATE_PROMO_DISPLAY);
			int itemsInBatch = 0;
			int updateCount = 0;
			for(PromoDisplay promoDisplay: updateList){
				int counter = 0;
				stmt.setInt(++counter, promoDisplay.getDisplayTypeId());
				stmt.setInt(++counter, promoDisplay.getCalendarId());
				stmt.setInt(++counter, promoDisplay.getSubDisplayTypeId());
				stmt.setLong(++counter, promoDisplay.getPromoDefId());
				stmt.addBatch();
				itemsInBatch++;
				if(itemsInBatch % Constants.BATCH_UPDATE_COUNT == 0){
	        		int[] count = stmt.executeBatch();
	        		stmt.clearBatch();
	        		itemsInBatch = 0;
	        		updateCount += PristineDBUtil.getUpdateCount(count);
	        	}
			}
			
			if(itemsInBatch > 0){
        		int[] count = stmt.executeBatch();
        		stmt.clearBatch();
        		updateCount +=  PristineDBUtil.getUpdateCount(count);
        	}
		}
		catch(SQLException sqlE){
			logger.info("Error while updating promo display" + sqlE);
			throw new GeneralException("Error while updating promo display", sqlE);
		}
		finally{
			PristineDBUtil.close(stmt);
		}
	}
	
	
	private void insertPromoDisplay(Connection conn, List<PromoDisplay> insertList) throws GeneralException{
		PreparedStatement stmt = null;
		try{
			stmt = conn.prepareStatement(INSERT_PROMO_DISPLAY);
			//PROMO_DEFINITION_ID, LOCATION_LEVEL_ID, LOCATION_ID, DISPLAY_TYPE_ID, SUB_DISPLAY_TYPE_ID
			int itemsInBatch = 0;
			int updateCount = 0;
			for(PromoDisplay promoDisplay: insertList){
				int counter = 0;
				stmt.setLong(++counter, promoDisplay.getPromoDefId());
				stmt.setInt(++counter, promoDisplay.getLocationLevelId());
				stmt.setInt(++counter, promoDisplay.getLocationId());
				stmt.setInt(++counter, promoDisplay.getDisplayTypeId());
				stmt.setInt(++counter, promoDisplay.getSubDisplayTypeId());
				stmt.setInt(++counter, promoDisplay.getCalendarId());
				stmt.addBatch();
				itemsInBatch++;
				if(itemsInBatch % Constants.BATCH_UPDATE_COUNT == 0){
	        		int[] count = stmt.executeBatch();
	        		stmt.clearBatch();
	        		itemsInBatch = 0;
	        		updateCount += PristineDBUtil.getUpdateCount(count);
	        	}
			}
			
			if(itemsInBatch > 0){
        		int[] count = stmt.executeBatch();
        		stmt.clearBatch();
        		updateCount +=  PristineDBUtil.getUpdateCount(count);
        	}
		}
		catch(SQLException sqlE){
			logger.info("Error while inserting promo display" + sqlE);
			throw new GeneralException("Error while inserting promo display", sqlE);
		}
		finally{
			PristineDBUtil.close(stmt);
		}
	}
	
}
