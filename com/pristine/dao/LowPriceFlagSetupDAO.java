package com.pristine.dao;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.util.List;

import com.pristine.dto.fileformatter.gianteagle.GiantEaglePriceDTO;
import com.pristine.exception.GeneralException;
import com.pristine.util.Constants;
import com.pristine.util.PristineDBUtil;

public class LowPriceFlagSetupDAO {

	
	/**
	 * 
	 * @param conn
	 * @param lowPriceFlags
	 * @throws GeneralException
	 */
	public void insertLowPriceFlags(Connection conn, List<GiantEaglePriceDTO> lowPriceFlags) throws GeneralException{

		String insertQuery = "INSERT INTO LOW_PRICE_FLAG_LOOKUP (CALENDAR_ID, LOCATION_LEVEL_ID, "
				+ " LOCATION_ID, ITEM_CODE, NEW_LOW_PRC_FG, NEW_LOW_PRC_END_DTE, LOW_PRC_FG, LOW_PRC_END_DTE) VALUES "
				+ " (?, ?, ?, ?, ?, TO_DATE(?,'MM/dd/yy'), ?, TO_DATE(?,'MM/dd/yy'))";
		
		PreparedStatement statement = null;
		try{
			
			statement = conn.prepareStatement(insertQuery);
			
			
			int itemsInBatch = 0;
			for(GiantEaglePriceDTO giantEaglePriceDTO: lowPriceFlags){
				int colCount = 0;
				statement.setInt(++colCount, giantEaglePriceDTO.getCalendarId());
				statement.setInt(++colCount, giantEaglePriceDTO.getLoctionLevelId());
				statement.setInt(++colCount, giantEaglePriceDTO.getLocationId());
				statement.setInt(++colCount, giantEaglePriceDTO.getItemCode());
				statement.setString(++colCount, giantEaglePriceDTO.getNEW_LOW_PRC_FG());
				statement.setString(++colCount, giantEaglePriceDTO.getNEW_LOW_PRC_END_DTE());
				statement.setString(++colCount, giantEaglePriceDTO.getLOW_PRC_FG());
				statement.setString(++colCount, giantEaglePriceDTO.getLOW_PRC_END_DTE());
				statement.addBatch();
				itemsInBatch++;
				
				if(itemsInBatch % Constants.BATCH_UPDATE_COUNT == 0){
					statement.executeBatch();
					itemsInBatch = 0;
					statement.clearBatch();
				}
			}
			if (itemsInBatch > 0) {
				statement.executeBatch();
				statement.clearBatch();
			}
			
		}catch(SQLException sqlException){
			throw new GeneralException("Error inserting low price flags", sqlException);
		}finally {
			PristineDBUtil.close(statement);
		}
	}
	
	/**
	 * 
	 * @param conn
	 * @return # of rows deleted
	 * @throws GeneralException
	 */
	public int deleteLowPriceFlags(Connection conn) throws GeneralException{
		String deleteQuery = "DELETE FROM LOW_PRICE_FLAG_LOOKUP";
		PreparedStatement statement = null;
		int rowsDeleted = 0;
		try{
			
			statement = conn.prepareStatement(deleteQuery);
			rowsDeleted = statement.executeUpdate();
			
		}catch(SQLException sqlE){
			throw new GeneralException("Error deleting LOW_PRICE_FLAG_LOOKUP", sqlE);
		}finally {
			PristineDBUtil.close(statement);
		}
		
		return rowsDeleted;
	}
	
	
	/**
	 * 
	 * @param conn
	 * @return # of rows deleted
	 * @throws GeneralException
	 */
	public int deleteLowPriceFlagsArchive(Connection conn) throws GeneralException {
		String deleteQuery = "DELETE FROM LOW_PRICE_FLAG_LOOKUP_ARCHIVE WHERE CALENDAR_ID IN "
				+ " (SELECT DISTINCT CALENDAR_ID FROM LOW_PRICE_FLAG_LOOKUP)";
		PreparedStatement statement = null;
		int rowsDeleted = 0;
		try {

			statement = conn.prepareStatement(deleteQuery);
			rowsDeleted = statement.executeUpdate();

		} catch (SQLException sqlE) {
			throw new GeneralException("Error deleting LOW_PRICE_FLAG_LOOKUP_ARCHIVE", sqlE);
		} finally {
			PristineDBUtil.close(statement);
		}

		return rowsDeleted;
	}
	
	
	/**
	 * 
	 * @param conn
	 * @return # of rows deleted
	 * @throws GeneralException
	 */
	public int archiveLowPriceFlags(Connection conn) throws GeneralException{
		String deleteQuery = "INSERT INTO LOW_PRICE_FLAG_LOOKUP_ARCHIVE (CALENDAR_ID, LOCATION_LEVEL_ID, "
				+ " LOCATION_ID, ITEM_CODE, NEW_LOW_PRC_FG, NEW_LOW_PRC_END_DTE, LOW_PRC_FG, LOW_PRC_END_DTE) "
				+ " (SELECT CALENDAR_ID, LOCATION_LEVEL_ID, LOCATION_ID, ITEM_CODE, NEW_LOW_PRC_FG, "
				+ " NEW_LOW_PRC_END_DTE, LOW_PRC_FG, LOW_PRC_END_DTE FROM LOW_PRICE_FLAG_LOOKUP)";
		PreparedStatement statement = null;
		int rowsInserted = 0;
		try{
			
			statement = conn.prepareStatement(deleteQuery);
			rowsInserted = statement.executeUpdate();
			
		}catch(SQLException sqlE){
			throw new GeneralException("Error archiving LOW_PRICE_FLAG_LOOKUP", sqlE);
		}finally {
			PristineDBUtil.close(statement);
		}
		
		return rowsInserted;
	}
}
