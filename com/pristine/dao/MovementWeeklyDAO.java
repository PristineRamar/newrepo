/*
 * Title: DAO class for Movement Weekly Data Load
 *
 *******************************************************************************
 * Modification History
 *------------------------------------------------------------------------------
 * Version		Date		Name			Remarks
 *------------------------------------------------------------------------------
 * Version 0.1	04/23/2012	Janani			Initial Version 
 *******************************************************************************
 */
package com.pristine.dao;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;

import org.apache.log4j.Logger;

import com.pristine.dto.MovementWeeklyDTO;
import com.pristine.exception.GeneralException;
import com.pristine.util.Constants;
import com.pristine.util.PristineDBUtil;

public class MovementWeeklyDAO {
	
	static Logger logger = Logger.getLogger("MovementWeeklyDAO");
	
	private Connection	connection	= null;
	private  final int commitCount = Constants.LIMIT_COUNT;
	
	public static final String GET_MOVEMENT_WEEKLY_ID = "SELECT CHECK_DATA_ID FROM MOVEMENT_WEEKLY WHERE CHECK_DATA_ID IN (%s)";
	
	public static final String INSERT_MOVEMENT_WEEKLY_FRM_DAILY_DATA = "INSERT INTO MOVEMENT_WEEKLY (COMP_STR_ID, ITEM_CODE, CHECK_DATA_ID, LIST_COST, " +
														"EFF_LIST_COST_DATE, DEAL_COST, DEAL_START_DATE, DEAL_END_DATE, "
														+ " PRICE_ZONE_ID, LOCATION_LEVEL_ID, LOCATION_ID) VALUES " +
														"(?,?,?,?,TO_DATE(?, 'MM/dd/yyyy'),?,TO_DATE(?, 'MM/dd/yyyy'),TO_DATE(?, 'MM/dd/yyyy'), ?, ?, ?)";
	
	public static final String UPDATE_MOVEMENT_WEEKLY_FRM_DAILY_DATA = "UPDATE MOVEMENT_WEEKLY SET COMP_STR_ID=?, ITEM_CODE=?, LIST_COST=?, " +
														"EFF_LIST_COST_DATE=TO_DATE(?, 'MM/dd/yyyy'), DEAL_COST=?, DEAL_START_DATE=TO_DATE(?, 'MM/dd/yyyy'), "
														+ "DEAL_END_DATE=TO_DATE(?, 'MM/dd/yyyy'), PRICE_ZONE_ID = ?, LOCATION_LEVEL_ID = ?, LOCATION_ID = ? WHERE " +
														"CHECK_DATA_ID=?";
	
	public static final String INSERT_MOVEMENT_WEEKLY_FRM_WEEKLY_DATA = "INSERT INTO MOVEMENT_WEEKLY (COMP_STR_ID, ITEM_CODE, CHECK_DATA_ID, LIST_COST, "
			+ "EFF_LIST_COST_DATE, DEAL_COST, DEAL_START_DATE, DEAL_END_DATE,"
			+ "REVENUE_REGULAR, QUANTITY_REGULAR, REVENUE_SALE, QUANTITY_SALE,"
			+ "TOTAL_MARGIN, MARGIN_PCT, VISIT_COUNT, LOCATION_LEVEL_ID, LOCATION_ID) VALUES "
			+ "(?,?,?,?,TO_DATE(?, 'MM/dd/yyyy'),?,TO_DATE(?, 'MM/dd/yyyy'),TO_DATE(?, 'MM/dd/yyyy'),?,?,?,?,?,?,?,?,?)";
	
	public static final String UPDATE_MOVEMENT_WEEKLY_FRM_WEEKLY_DATA = "UPDATE MOVEMENT_WEEKLY SET COMP_STR_ID=?, ITEM_CODE=?, LIST_COST=?, "
			+"EFF_LIST_COST_DATE=TO_DATE(?, 'MM/dd/yyyy'), DEAL_COST=?, DEAL_START_DATE=TO_DATE(?, 'MM/dd/yyyy'), DEAL_END_DATE=TO_DATE(?, 'MM/dd/yyyy'), "
			+"REVENUE_REGULAR = ?, QUANTITY_REGULAR = ?, REVENUE_SALE = ?, QUANTITY_SALE = ?, TOTAL_MARGIN = ?, "
			+"MARGIN_PCT = ?, VISIT_COUNT = ?, LOCATION_LEVEL_ID = ?, LOCATION_ID = ? "
			+"WHERE CHECK_DATA_ID=?";
	
	 
	
	
	/**
	 * Constructor
	 * @param conn Connection
	 */
	public MovementWeeklyDAO(Connection conn)
	{
		connection = conn;
	}
	
	/**
	 * This method determines if the checkDataId is already present in Movement Weekly table
	 * @param movementWeeklyDataMap	Map containing check data id as key and its movement weekly data as value
	 */
	public void populateCheckDataId(HashMap<Integer, MovementWeeklyDTO> movementWeeklyDataMap) throws GeneralException{
		logger.debug("Inside populateCheckDataId() in MovementWeeklyDAO");
		
		List<Integer> checkDataLst = new ArrayList<Integer>();
		int limitcount=0;
		for(Integer checkDataStr:movementWeeklyDataMap.keySet()){
			checkDataLst.add(checkDataStr);
			limitcount++;
			if( limitcount > 0 && (limitcount%this.commitCount == 0)){
				Object[] values = checkDataLst.toArray();
				retrieveCheckDataId(movementWeeklyDataMap, values);
				checkDataLst.clear();
            }
		}
		if(checkDataLst.size() > 0){
			Object[] values = checkDataLst.toArray();
			retrieveCheckDataId(movementWeeklyDataMap, values);
			checkDataLst.clear();
		}
	}

	/**
	 * This method retrieves check data Id from movement weekly table
	 * @param compDataMap	Map containing check data id as key and its movement weekly data as value
	 * @param values
	 * @throws GeneralException
	 */
	private void retrieveCheckDataId(HashMap<Integer, MovementWeeklyDTO> movementWeeklyDataMap, Object[] values) throws GeneralException{
		logger.debug("Inside retrieveCheckDataId() in MovementWeeklyDAO");
		PreparedStatement statement = null;
	    ResultSet resultSet = null;
	    try{
			statement = connection.prepareStatement(String.format(GET_MOVEMENT_WEEKLY_ID, PristineDBUtil.preparePlaceHolders(values.length)));
			PristineDBUtil.setValues(statement, values);
	        resultSet = statement.executeQuery();
	        Integer key = null;
	        
	        while(resultSet.next()){
	        	key = resultSet.getInt("CHECK_DATA_ID");
	        	MovementWeeklyDTO movementWeeklyDTO = movementWeeklyDataMap.get(key);
	        	movementWeeklyDTO.setFlag(Constants.UPDATE_FLAG);
	        }
		}
		catch (SQLException e)
		{
			logger.error("Error while executing GET_ITEM_CODE");
			throw new GeneralException("Error while executing GET_ITEM_CODE", e);
		}finally{
			PristineDBUtil.close(resultSet);
			PristineDBUtil.close(statement);
		}
	}

	/**
	 * This method inserts records in movement weekly table
	 * @param toBeInsertedList		List of records to be inserted
	 */
	public void insertMovementWeeklyWithDailyData(List<MovementWeeklyDTO> toBeInsertedList) throws GeneralException{
		logger.debug("Inside insertMovementWeeklyWithDailyData() in MovementWeeklyDAO");
		PreparedStatement statement = null;
	    try{
			statement = connection.prepareStatement(INSERT_MOVEMENT_WEEKLY_FRM_DAILY_DATA);
			SimpleDateFormat formatter = new SimpleDateFormat("MM/dd/yyyy");
			int itemNoInBatch = 0;
			int recordCount = 0;
			String effListCostDate = null;
			String dealStartDate = null;
			String dealEndDate = null;
	        for(MovementWeeklyDTO movementWeekly:toBeInsertedList){
	        	effListCostDate = null;
				dealStartDate = null;
				dealEndDate = null;
	        	int counter = 0;
	        	statement.setInt(++counter, movementWeekly.getCompStoreId());
	        	statement.setInt(++counter, movementWeekly.getItemCode());
	        	statement.setInt(++counter, movementWeekly.getCheckDataId());
	        	statement.setDouble(++counter, movementWeekly.getListCost());
	        	if(movementWeekly.getEffListCostDate() != null)
	        		effListCostDate = formatter.format(movementWeekly.getEffListCostDate());
	        	else
	        		effListCostDate = null;
	        	statement.setString(++counter, effListCostDate);
	        	statement.setDouble(++counter, movementWeekly.getDealCost());
	        	if(movementWeekly.getDealStartDate() != null)
	        		dealStartDate = formatter.format(movementWeekly.getDealStartDate());
	        	else
	        		dealStartDate = null;
	        	statement.setString(++counter, dealStartDate);
	        	if(movementWeekly.getDealEndDate() != null)
	        		dealEndDate = formatter.format(movementWeekly.getDealEndDate());
	        	else
	        		dealEndDate = null;
	        	statement.setString(++counter, dealEndDate);
	        	statement.setInt(++counter, movementWeekly.getPriceZoneId());
	        	statement.setInt(++counter, movementWeekly.getLocationLevelId());
	        	statement.setInt(++counter, movementWeekly.getLocationId());
	        	statement.addBatch();
	        	itemNoInBatch++;
	        	recordCount++;
	        	
	        	if(itemNoInBatch % Constants.BATCH_UPDATE_COUNT == 0){
	        		long startTime = System.currentTimeMillis();
	        		statement.executeBatch();
	        		long endTime = System.currentTimeMillis();
	        		logger.debug("Time taken for inserting a batch - " + (endTime - startTime) + "ms");
	        		statement.clearBatch();
	        		itemNoInBatch = 0;
	        	}
	        	
	        	if(recordCount % 10000 == 0){
	        		connection.commit();
	        	}
	        }
	        if(itemNoInBatch > 0){
	        	 statement.executeBatch();
        		statement.clearBatch();
	        }
		}
		catch (SQLException e)
		{
			logger.error("Error while executing INSERT_MOVEMENT_WEEKLY_FRM_DAILY_DATA");
			throw new GeneralException("Error while executing INSERT_MOVEMENT_WEEKLY_FRM_DAILY_DATA", e);
		}finally{
			PristineDBUtil.close(statement);
		}
	}

	/**
	 * This method updates records in movement weekly table based on check data id
	 * @param toBeUpdatedList		List of records to be updated
	 */
	public void updateMovementWeeklyWithDailyData(List<MovementWeeklyDTO> toBeUpdatedList) throws GeneralException{
		logger.debug("Inside updateMovementWeeklyWithDailyData() in MovementWeeklyDAO");
		PreparedStatement statement = null;
	    try{
			statement = connection.prepareStatement(UPDATE_MOVEMENT_WEEKLY_FRM_DAILY_DATA);
			SimpleDateFormat formatter = new SimpleDateFormat("MM/dd/yyyy");
			int itemNoInBatch = 0;
			String effListCostDate = null;
			String dealStartDate = null;
			String dealEndDate = null;
	        
	        for(MovementWeeklyDTO movementWeekly:toBeUpdatedList){
	        	int counter = 0;
	        	statement.setInt(++counter, movementWeekly.getCompStoreId());
	        	statement.setInt(++counter, movementWeekly.getItemCode());
	        	statement.setDouble(++counter, movementWeekly.getListCost());
	        	
	        	if(movementWeekly.getEffListCostDate() != null)
	        		effListCostDate = formatter.format(movementWeekly.getEffListCostDate());
	        	else
	        		effListCostDate = null;	        	
	        	statement.setString(++counter, effListCostDate);
	        	
	        	statement.setDouble(++counter, movementWeekly.getDealCost());
	        	
	        	if(movementWeekly.getDealStartDate() != null)
	        		dealStartDate = formatter.format(movementWeekly.getDealStartDate());
	        	else
	        		dealStartDate = null;
	        	statement.setString(++counter, dealStartDate);
	        	
	        	if(movementWeekly.getDealEndDate() != null)
	        		dealEndDate = formatter.format(movementWeekly.getDealEndDate());
	        	else
	        		dealEndDate = null;
	        	statement.setString(++counter, dealEndDate);
	        	
	        	statement.setInt(++counter, movementWeekly.getPriceZoneId());
	        	//Added to support chain level
				statement.setInt(++counter, movementWeekly.getLocationLevelId());
				statement.setInt(++counter, movementWeekly.getLocationId());
				//Where clause
	        	statement.setInt(++counter, movementWeekly.getCheckDataId());
	        	statement.addBatch();
	        	itemNoInBatch++;
	        	
	        	if(itemNoInBatch % Constants.BATCH_UPDATE_COUNT == 0){
	        		long startTime = System.currentTimeMillis();
	        		statement.executeBatch();
	        		long endTime = System.currentTimeMillis();
	        		logger.debug("Time taken for updating a batch - " + (endTime - startTime) + "ms");
	        		statement.clearBatch();
	        		itemNoInBatch = 0;
	        	}
	        }
	        if(itemNoInBatch > 0){
	        	statement.executeBatch();
        		statement.clearBatch();
	        }
		}
		catch (SQLException e)
		{
			logger.error("Error while executing UPDATE_MOVEMENT_WEEKLY_FRM_DAILY_DATA");
			throw new GeneralException("Error while executing UPDATE_MOVEMENT_WEEKLY_FRM_DAILY_DATA", e);
		}finally{
			PristineDBUtil.close(statement);
		}
	}
		
	/**
	 * This method inserts records in movement weekly table with weekly movement data
	 * @param toBeInsertedList		List of records to be inserted
	 */
	public void insertMovementWeeklyWithWeeklyData(List<MovementWeeklyDTO> toBeInsertedList) throws GeneralException{
		logger.debug("Inside insertMovementWeeklyWithWeeklyData() in MovementWeeklyDAO");
		PreparedStatement statement = null;
		 
	    try{
			statement = connection.prepareStatement(INSERT_MOVEMENT_WEEKLY_FRM_WEEKLY_DATA);
			SimpleDateFormat formatter = new SimpleDateFormat("MM/dd/yyyy");
			int itemNoInBatch = 0;
			String effListCostDate = null;
			String dealStartDate = null;
			String dealEndDate = null;
	        for(MovementWeeklyDTO movementWeekly:toBeInsertedList){
	        	effListCostDate = null;
				dealStartDate = null;
				dealEndDate = null;
	        	int counter = 0;
	        	statement.setInt(++counter, movementWeekly.getCompStoreId());
	        	statement.setInt(++counter, movementWeekly.getItemCode());
	        	statement.setInt(++counter, movementWeekly.getCheckDataId());
	        	statement.setDouble(++counter, movementWeekly.getListCost());
	        	if(movementWeekly.getEffListCostDate() != null)
	        		effListCostDate = formatter.format(movementWeekly.getEffListCostDate());
	        	else
	        		effListCostDate = null;
	        	statement.setString(++counter, effListCostDate);
	        	statement.setDouble(++counter, movementWeekly.getDealCost());
	        	if(movementWeekly.getDealStartDate() != null)
	        		dealStartDate = formatter.format(movementWeekly.getDealStartDate());
	        	else
	        		dealStartDate = null;
	        	statement.setString(++counter, dealStartDate);
	        	if(movementWeekly.getDealEndDate() != null)
	        		dealEndDate = formatter.format(movementWeekly.getDealEndDate());
	        	else
	        		dealEndDate = null;
	        	statement.setString(++counter, dealEndDate);
	        	
	        	statement.setObject(++counter, movementWeekly.getRevenueRegular());
	        	statement.setObject(++counter, movementWeekly.getQuantityRegular());
	        	statement.setObject(++counter, movementWeekly.getRevenueSale());
	        	statement.setObject(++counter, movementWeekly.getQuantitySale());
	        	
	        	
	        	// Added on 26th Sep 2012, to set upper limit for the margin
	        	double margin = movementWeekly.getMargin();
				if (margin > 99999.99) 
					margin = 99999.99;
				else if (margin < -99999.99)
					margin = -99999.99;	
	        	//statement.setObject(++counter, movementWeekly.getMargin());
	        	statement.setObject(++counter, margin);
				
				double marginPct = movementWeekly.getMarginPercent();
				if (marginPct > 999.99) 
					marginPct = 999.99;
				else if (marginPct < -999.99)
					marginPct = -999.99;	
				statement.setObject(++counter, marginPct);
					
				statement.setObject(++counter, movementWeekly.getVisitCount());				
				//Added to support chain level.
				statement.setInt(++counter, movementWeekly.getLocationLevelId());
				statement.setInt(++counter, movementWeekly.getLocationId());
	        	
	        	statement.addBatch();
	        	itemNoInBatch++;
	        	
	        	//logger.debug(movementWeekly.getCompStoreId() + "--" + movementWeekly.getItemCode() + "--" + movementWeekly.getCheckDataId());
	        	//logger.debug(movementWeekly.getListCost() + "--" + effListCostDate + "--" + movementWeekly.getDealCost());
	        	//logger.debug(dealStartDate + "--" + dealEndDate + "--" + movementWeekly.getRevenueRegular());
	        	//logger.debug(movementWeekly.getQuantityRegular() + "--" + movementWeekly.getRevenueSale() + "--" + movementWeekly.getQuantitySale());
	        	//logger.debug(movementWeekly.getMargin() + "--" + marginPct + "--" + movementWeekly.getVisitCount());
	        	 
	        	
	        	if(itemNoInBatch % Constants.BATCH_UPDATE_COUNT == 0){
	        		long startTime = System.currentTimeMillis();
	        		 statement.executeBatch();
	        		long endTime = System.currentTimeMillis();
	        		logger.debug("Time taken for inserting a batch - " + (endTime - startTime) + "ms");
	        		statement.clearBatch();
	        		itemNoInBatch = 0;
	        	}
	        }
	        if(itemNoInBatch > 0){
	        	statement.executeBatch();
        		statement.clearBatch();
	        }
		}
		catch (SQLException e)
		{
			logger.error("Error while executing INSERT_MOVEMENT_WEEKLY_FRM_WEEKLY_DATA");
			throw new GeneralException("Error while executing INSERT_MOVEMENT_WEEKLY_FRM_WEEKLY_DATA", e);
		}finally{
			PristineDBUtil.close(statement);
		}
	}
	
	/**
	 * This method updates records in movement weekly table with weekly movement data based on check data id
	 * @param toBeUpdatedList		List of records to be updated
	 */
	public void updateMovementWeeklyWithWeeklyData(List<MovementWeeklyDTO> toBeUpdatedList) throws GeneralException{
		logger.debug("Inside updateMovementWeeklyWithWeeklyData() in MovementWeeklyDAO");
		PreparedStatement statement = null;
	    try{
			statement = connection.prepareStatement(UPDATE_MOVEMENT_WEEKLY_FRM_WEEKLY_DATA);
			SimpleDateFormat formatter = new SimpleDateFormat("MM/dd/yyyy");
			int itemNoInBatch = 0;
			String effListCostDate = null;
			String dealStartDate = null;
			String dealEndDate = null;
	        
	        for(MovementWeeklyDTO movementWeekly:toBeUpdatedList){
	        	int counter = 0;
	        	statement.setInt(++counter, movementWeekly.getCompStoreId());
	        	statement.setInt(++counter, movementWeekly.getItemCode());
	        	statement.setDouble(++counter, movementWeekly.getListCost());
	        	if(movementWeekly.getEffListCostDate() != null)
	        		effListCostDate = formatter.format(movementWeekly.getEffListCostDate());
	        	else
	        		effListCostDate = null;
	        	statement.setString(++counter, effListCostDate);
	        	statement.setDouble(++counter, movementWeekly.getDealCost());
	        	if(movementWeekly.getDealStartDate() != null)
	        		dealStartDate = formatter.format(movementWeekly.getDealStartDate());
	        	else
	        		dealStartDate = null;
	        	statement.setString(++counter, dealStartDate);
	        	if(movementWeekly.getDealEndDate() != null)
	        		dealEndDate = formatter.format(movementWeekly.getDealEndDate());
	        	else
	        		dealEndDate = null;
	        	statement.setString(++counter, dealEndDate);
	        	statement.setObject(++counter, movementWeekly.getRevenueRegular());
	        	statement.setObject(++counter, movementWeekly.getQuantityRegular());
	        	statement.setObject(++counter, movementWeekly.getRevenueSale());
	        	statement.setObject(++counter, movementWeekly.getQuantitySale());
	        	 	        	
	        	// Added on 26th Sep 2012, to set upper limit for the margin
	        	double margin = movementWeekly.getMargin();
				if (margin > 99999.99) 
					margin = 99999.99;
				else if (margin < -99999.99)
					margin = -99999.99;	
	        	//statement.setObject(++counter, movementWeekly.getMargin());
	        	statement.setObject(++counter, margin);
	        	
				
				double marginPct = movementWeekly.getMarginPercent();
				if (marginPct > 999.99) 
					marginPct = 999.99;
				else if (marginPct < -999.99)
					marginPct = -999.99;	
				
				statement.setObject(++counter, marginPct);	
				statement.setObject(++counter, movementWeekly.getVisitCount());
				//Added to support chain level
				statement.setInt(++counter, movementWeekly.getLocationLevelId());
				statement.setInt(++counter, movementWeekly.getLocationId());
	        	//Where clause
	        	statement.setInt(++counter, movementWeekly.getCheckDataId());
	        	
	        	statement.addBatch();
	        	itemNoInBatch++;
	        	
	        	if(itemNoInBatch % Constants.BATCH_UPDATE_COUNT == 0){
	        		long startTime = System.currentTimeMillis();
	        		statement.executeBatch();
	        		long endTime = System.currentTimeMillis();
	        		logger.debug("Time taken for updating a batch - " + (endTime - startTime) + "ms");
	        		statement.clearBatch();
	        		itemNoInBatch = 0;
	        	}
	        }
	        if(itemNoInBatch > 0){
	        	 statement.executeBatch();
        		statement.clearBatch();
	        }
		}
		catch (SQLException e)
		{
			logger.error("Error while executing UPDATE_MOVEMENT_WEEKLY_FRM_WEEKLY_DATA");
			throw new GeneralException("Error while executing UPDATE_MOVEMENT_WEEKLY_FRM_WEEKLY_DATA", e);
		}finally{
			PristineDBUtil.close(statement);
		}
	}
}
