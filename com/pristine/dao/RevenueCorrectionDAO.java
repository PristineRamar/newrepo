package com.pristine.dao;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.text.DecimalFormat;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;
import java.util.TreeSet;

import org.apache.commons.lang.ArrayUtils;
import org.apache.log4j.Logger;

import com.pristine.dto.RetailCalendarDTO;
import com.pristine.exception.GeneralException;
import com.pristine.util.Constants;
import com.pristine.util.PristineDBUtil;

public class RevenueCorrectionDAO {

	static Logger logger = Logger.getLogger(RevenueCorrectionDAO.class);
	
	private static final String UPDATE_SALES_AGGR_DAILY = "UPDATE SALES_AGGR_DAILY SET TOT_REVENUE = ?, AVG_ORDER_SIZE = ROUND((?/TOT_VISIT_CNT), 2) WHERE CALENDAR_ID = ? AND LOCATION_ID = ? AND PRODUCT_LEVEL_ID IS NULL AND PRODUCT_ID IS NULL";
	
	private static final String UPDATE_SALES_AGGR_DAILY_ROLLUP = "UPDATE SALES_AGGR_DAILY_ROLLUP SET %REVENUECOLUMN% = ?, %AVGORDERSIZECOLUMN% = ROUND((?/%TOTVISITCNTCOLUMN%), 2) WHERE CALENDAR_ID = ? AND LOCATION_LEVEL_ID = ? AND LOCATION_ID = ? AND PRODUCT_LEVEL_ID IS NULL AND PRODUCT_ID IS NULL";
	
	private static final String UPDATE_SALES_AGGR_WEEKLY = "UPDATE SALES_AGGR_WEEKLY SET TOT_REVENUE = ?, AVG_ORDER_SIZE = CASE WHEN TOT_VISIT_CNT <> 0 THEN  ROUND((?/TOT_VISIT_CNT), 2) ELSE 0 END WHERE CALENDAR_ID = ? AND LOCATION_ID = ? AND PRODUCT_LEVEL_ID = 6 AND PRODUCT_ID = ?";
	
	private static final String UPDATE_SALES_AGGR_WEEKLY_STORE_DATA = "UPDATE SALES_AGGR_WEEKLY SET TOT_REVENUE = ?, TOT_VISIT_CNT = ?, AVG_ORDER_SIZE = ? WHERE CALENDAR_ID = ? AND LOCATION_ID = ? AND PRODUCT_LEVEL_ID IS NULL AND PRODUCT_ID IS NULL";
	
	private static final String UPDATE_SALES_AGGR_WEEKLY_STORE_DATA_VISIT_CNT = "UPDATE SALES_AGGR_WEEKLY SET TOT_VISIT_CNT = ?, AVG_ORDER_SIZE = ROUND((TOT_REVENUE/ ?), 2) WHERE CALENDAR_ID = ? AND LOCATION_ID = ? AND PRODUCT_LEVEL_ID IS NULL AND PRODUCT_ID IS NULL";
	
	private static final String GET_PRODUCT_GROUP = "SELECT PRODUCT_ID, NAME FROM PRODUCT_GROUP WHERE PRODUCT_LEVEL_ID = ?";
	
	private static final String GET_STORE_ID = "SELECT COMP_STR_NO, COMP_STR_ID FROM COMPETITOR_STORE WHERE COMP_CHAIN_ID = ?";
	
	private static final String GET_CALENDAR_INFO = "SELECT CALENDAR_ID, TO_CHAR(START_DATE, 'MM/dd/yyyy') START_DATE, TO_CHAR(END_DATE, 'MM/dd/yyyy') END_DATE FROM RETAIL_CALENDAR WHERE CAL_YEAR = ? AND ACTUAL_NO = ? AND ROW_TYPE='W'";
	
	private static final String GET_WEEKLY_CALENDARID_MAP = "SELECT RW.CALENDAR_ID AS WEEK_CALENDAR_ID, RD.CALENDAR_ID AS DAY_CALENDAR_ID FROM RETAIL_CALENDAR RD, RETAIL_CALENDAR RW " + 
															"WHERE RD.ROW_TYPE = 'D' AND RW.ROW_TYPE = 'W' " +
															"AND RD.END_DATE IS NULL AND RD.START_DATE = RW.END_DATE " +
															"AND RW.ACTUAL_NO BETWEEN ? AND ? AND RW.CAL_YEAR = ?";
	
	private static final String GET_FINANCE_DEPT_WEEKLY_DATA = "SELECT CALENDAR_ID, LOCATION_LEVEL_ID, LOCATION_ID, PRODUCT_ID, TOT_REVENUE, TOT_VISIT_CNT FROM SALES_AGGR_WEEKLY WHERE CALENDAR_ID IN (%s) AND PRODUCT_LEVEL_ID = 6 " +
															   "ORDER BY LOCATION_LEVEL_ID, LOCATION_ID, PRODUCT_ID";
	
	private static final String GET_FINANCE_DEPT_WEEKLY_ROLLUP_DATA = "SELECT CALENDAR_ID, LOCATION_LEVEL_ID, LOCATION_ID, PRODUCT_ID, TOT_REVENUE, TOT_VISIT_CNT, ID_TOT_REVENUE, ID_TOT_VISIT_CNT FROM SALES_AGGR_WEEKLY_ROLLUP WHERE CALENDAR_ID IN (%s) AND PRODUCT_LEVEL_ID = 6 " +
			   												   "ORDER BY LOCATION_LEVEL_ID, LOCATION_ID, PRODUCT_ID";
		
	private static final String UPDATE_CTD_DATA_FOR_FINANCE_DEPT = "UPDATE SALES_AGGR_CTD SET TOT_REVENUE = ?, TOT_VISIT_CNT = ?, AVG_ORDER_SIZE = ROUND(?/?, 2) WHERE SUMMARY_CTD_ID = " +
																		  "(SELECT SUMMARY_CTD_ID FROM SALES_AGGR_DAILY WHERE CALENDAR_ID = ? AND LOCATION_LEVEL_ID = ? AND LOCATION_ID = ? AND PRODUCT_LEVEL_ID = 6 AND PRODUCT_ID = ?) " +
																		  "AND CTD_TYPE = ?";
	
	private static final String UPDATE_ROLLEDUP_CTD_DATA_FOR_FINANCE_DEPT = "UPDATE SALES_AGGR_CTD SET TOT_REVENUE = ?, TOT_VISIT_CNT = ?, AVG_ORDER_SIZE = ROUND(?/?, 2), ID_TOT_REVENUE = ?, ID_TOT_VISIT_CNT = ?, ID_AVG_ORDER_SIZE = ROUND(?/?, 2) WHERE SUMMARY_CTD_ID = " +
			  														"(SELECT SUMMARY_CTD_ID FROM SALES_AGGR_DAILY_ROLLUP WHERE CALENDAR_ID = ? AND LOCATION_LEVEL_ID = ? AND LOCATION_ID = ? AND PRODUCT_LEVEL_ID = 6 AND PRODUCT_ID = ?) " +
			  														"AND CTD_TYPE = ?";
	
	private static final String GET_CALENDARID_MAP = "SELECT RP.CALENDAR_ID AS CALENDAR_ID, RW.CALENDAR_ID AS WEEK_CALENDAR_ID FROM RETAIL_CALENDAR RP, RETAIL_CALENDAR RW " + 
													 "WHERE RP.ROW_TYPE = ? AND RW.ROW_TYPE = 'W' "+
													 "AND RW.START_DATE BETWEEN RP.START_DATE AND RP.END_DATE " +
													 "AND RW.ACTUAL_NO BETWEEN ? AND ? AND RW.CAL_YEAR = ? " +
													 "ORDER BY RP.CALENDAR_ID, RW.CALENDAR_ID";
	
	private static final String GET_ROLLUP_LEVELIDS = "SELECT %COLUMNNAME%, %BASECOLUMN% FROM COMPETITOR_STORE WHERE COMP_CHAIN_ID = 50 AND %COLUMNNAME% IN " +
													  "(SELECT ID FROM %TABLENAME%) ";
	
	private static final String GET_ROLLUP_LEVELIDS_CONDITION = " AND OPEN_DATE < (TO_DATE(?,'MM/dd/yyyy') - 364)";
	
	private static final String UPDATE_SALES_AGGR_WEEKLY_ROLLUP = "UPDATE SALES_AGGR_WEEKLY_ROLLUP SET %REVENUECOLUMN% = ?, %AVGORDERSIZECOLUMN% = CASE WHEN %TOTVISITCNTCOLUMN% <> 0 THEN  ROUND((?/%TOTVISITCNTCOLUMN%), 2) ELSE 0 END WHERE CALENDAR_ID = ? AND LOCATION_LEVEL_ID = ? AND LOCATION_ID = ? AND PRODUCT_LEVEL_ID = 6 AND PRODUCT_ID = ?";
	
	private static final String UPDATE_SALES_AGGR_WEEKLY_ROLLUP_STORE_DATA = "UPDATE SALES_AGGR_WEEKLY_ROLLUP SET %REVENUECOLUMN% = ?, %TOTVISITCNTCOLUMN% = ?, %AVGORDERSIZECOLUMN%= ROUND(?/?, 2) WHERE CALENDAR_ID = ? AND LOCATION_LEVEL_ID = ? AND LOCATION_ID = ? AND PRODUCT_LEVEL_ID IS NULL AND PRODUCT_ID IS NULL";
	
	private static final String UPDATE_SALES_AGGR_WEEKLY_ROLLUP_STORE_DATA_VISIT_COUNT = "UPDATE SALES_AGGR_WEEKLY_ROLLUP SET %TOTVISITCNTCOLUMN% = ?, %AVGORDERSIZECOLUMN%= ROUND(%REVENUECOLUMN%/?, 2) WHERE CALENDAR_ID = ? AND LOCATION_LEVEL_ID = ? AND LOCATION_ID = ? AND PRODUCT_LEVEL_ID IS NULL AND PRODUCT_ID IS NULL";
	
	private static final String UPDATE_SALES_AGGR_FOR_FINANCE_DEPT = "UPDATE SALES_AGGR SET TOT_REVENUE = ?, TOT_VISIT_CNT = ?, AVG_ORDER_SIZE = ROUND(?/?, 2) WHERE CALENDAR_ID = ? AND LOCATION_LEVEL_ID = ? AND LOCATION_ID = ? AND PRODUCT_LEVEL_ID = 6 AND PRODUCT_ID = ?";
	
	private static final String UPDATE_SALES_AGGR_ROLLUP_FOR_FINANCE_DEPT = "UPDATE SALES_AGGR_ROLLUP SET TOT_REVENUE = ?, TOT_VISIT_CNT = ?, AVG_ORDER_SIZE = ROUND(?/?, 2), ID_TOT_REVENUE = ?, ID_TOT_VISIT_CNT = ?, ID_AVG_ORDER_SIZE = ROUND(?/?, 2) WHERE CALENDAR_ID = ? AND LOCATION_LEVEL_ID = ? AND LOCATION_ID = ? AND PRODUCT_LEVEL_ID = 6 AND PRODUCT_ID = ?";
	
	private static final String GET_STORE_LEVEL_WEEKLY_DATA = "SELECT CALENDAR_ID, LOCATION_LEVEL_ID, LOCATION_ID, TOT_REVENUE, TOT_VISIT_CNT FROM SALES_AGGR_WEEKLY WHERE CALENDAR_ID IN (%s) AND PRODUCT_LEVEL_ID IS NULL AND PRODUCT_ID IS NULL " +
			   												  "ORDER BY LOCATION_ID";
	
	private static final String GET_STORE_LEVEL_WEEKLY_ROLLUP_DATA = "SELECT CALENDAR_ID, LOCATION_LEVEL_ID, LOCATION_ID, TOT_REVENUE, TOT_VISIT_CNT, ID_TOT_REVENUE, ID_TOT_VISIT_CNT FROM SALES_AGGR_WEEKLY_ROLLUP WHERE CALENDAR_ID IN (%s) AND PRODUCT_LEVEL_ID IS NULL AND PRODUCT_ID IS NULL " +
				  											  "ORDER BY LOCATION_ID";

	private static final String UPDATE_CTD_DATA_FOR_STORE = "UPDATE SALES_AGGR_CTD SET TOT_REVENUE = ?, TOT_VISIT_CNT = ?, AVG_ORDER_SIZE = ROUND(?/?, 2) WHERE SUMMARY_CTD_ID = " +
														    "(SELECT SUMMARY_CTD_ID FROM SALES_AGGR_DAILY WHERE CALENDAR_ID = ? AND LOCATION_LEVEL_ID = ? AND LOCATION_ID = ? AND PRODUCT_LEVEL_ID IS NULL AND PRODUCT_ID IS NULL) AND CTD_TYPE = ?";
	
	private static final String UPDATE_CTD_DATA_FOR_STORE_VISIT_COUNT = "UPDATE SALES_AGGR_CTD SET TOT_VISIT_CNT = ?, AVG_ORDER_SIZE = ROUND(TOT_REVENUE/?, 2) WHERE SUMMARY_CTD_ID = " +
		    "(SELECT SUMMARY_CTD_ID FROM SALES_AGGR_DAILY WHERE CALENDAR_ID = ? AND LOCATION_LEVEL_ID = ? AND LOCATION_ID = ? AND PRODUCT_LEVEL_ID IS NULL AND PRODUCT_ID IS NULL) AND CTD_TYPE = ?";
	
	private static final String UPDATE_ROLLEDUP_CTD_DATA_FOR_STORE = "UPDATE SALES_AGGR_CTD SET TOT_REVENUE = ?, TOT_VISIT_CNT = ?, AVG_ORDER_SIZE = ROUND(?/?, 2), ID_TOT_REVENUE = ?, ID_TOT_VISIT_CNT = ?, ID_AVG_ORDER_SIZE = ROUND(?/?, 2) WHERE SUMMARY_CTD_ID = " +
			 "(SELECT SUMMARY_CTD_ID FROM SALES_AGGR_DAILY_ROLLUP WHERE CALENDAR_ID = ? AND LOCATION_LEVEL_ID = ? AND LOCATION_ID = ? AND PRODUCT_LEVEL_ID IS NULL AND PRODUCT_ID IS NULL) AND CTD_TYPE = ?";
	
	private static final String UPDATE_ROLLEDUP_CTD_DATA_FOR_STORE_VISIT_COUNT = "UPDATE SALES_AGGR_CTD SET TOT_VISIT_CNT = ?, AVG_ORDER_SIZE = ROUND(TOT_REVENUE/?, 2), ID_TOT_VISIT_CNT = ?, ID_AVG_ORDER_SIZE = ROUND(ID_TOT_REVENUE/?, 2) WHERE SUMMARY_CTD_ID = " +
			 "(SELECT SUMMARY_CTD_ID FROM SALES_AGGR_DAILY_ROLLUP WHERE CALENDAR_ID = ? AND LOCATION_LEVEL_ID = ? AND LOCATION_ID = ? AND PRODUCT_LEVEL_ID IS NULL AND PRODUCT_ID IS NULL) AND CTD_TYPE = ?";
	
	private static final String UPDATE_SALES_AGGR_FOR_STORE = "UPDATE SALES_AGGR SET TOT_REVENUE = ?, TOT_VISIT_CNT = ?, AVG_ORDER_SIZE = ROUND(?/?, 2) WHERE " +
														      "CALENDAR_ID = ? AND LOCATION_LEVEL_ID = ? AND LOCATION_ID = ? AND PRODUCT_LEVEL_ID IS NULL AND PRODUCT_ID IS NULL";
	
	private static final String UPDATE_SALES_AGGR_FOR_STORE_VISIT_COUNT = "UPDATE SALES_AGGR SET TOT_VISIT_CNT = ?, AVG_ORDER_SIZE = ROUND(TOT_REVENUE/?, 2) WHERE " +
		      "CALENDAR_ID = ? AND LOCATION_LEVEL_ID = ? AND LOCATION_ID = ? AND PRODUCT_LEVEL_ID IS NULL AND PRODUCT_ID IS NULL";
	
	private static final String UPDATE_SALES_AGGR_ROLLUP_FOR_STORE = "UPDATE SALES_AGGR_ROLLUP SET TOT_REVENUE = ?, TOT_VISIT_CNT = ?, AVG_ORDER_SIZE = ROUND(?/?, 2), ID_TOT_REVENUE = ?, ID_TOT_VISIT_CNT = ?, ID_AVG_ORDER_SIZE = ROUND(?/?, 2)  WHERE " +
		      												  "CALENDAR_ID = ? AND LOCATION_LEVEL_ID = ? AND LOCATION_ID = ? AND PRODUCT_LEVEL_ID IS NULL AND PRODUCT_ID IS NULL";
	
	private static final String UPDATE_SALES_AGGR_ROLLUP_FOR_STORE_VISIT_COUNT = "UPDATE SALES_AGGR_ROLLUP SET TOT_VISIT_CNT = ?, AVG_ORDER_SIZE = ROUND(TOT_REVENUE/?, 2), ID_TOT_VISIT_CNT = ?, ID_AVG_ORDER_SIZE = ROUND(ID_TOT_REVENUE/?, 2)  WHERE " +
			  "CALENDAR_ID = ? AND LOCATION_LEVEL_ID = ? AND LOCATION_ID = ? AND PRODUCT_LEVEL_ID IS NULL AND PRODUCT_ID IS NULL";
	
	/**
	 * Updates daily revenue for stores
	 * @param conn				Connection
	 * @param calendarId		Calendar Id
	 * @param storeRevenueMap	Map containing store id as key and revenue as value
	 * @throws GeneralException
	 */
	public void updateDailyRevenue(Connection conn, int calendarId, HashMap<Integer, Object> storeRevenueMap) throws GeneralException{
		logger.info("Inside updateDailyRevenue() of RevenueCorrectionDAO");
		PreparedStatement statement = null;
	    try{
			statement = conn.prepareStatement(UPDATE_SALES_AGGR_DAILY);
	        
			int itemNoInBatch = 0;
			
			for(Map.Entry<Integer, Object> entry : storeRevenueMap.entrySet()){
				int counter = 0;
				statement.setObject(++counter, entry.getValue());
				statement.setObject(++counter, entry.getValue());
				statement.setInt(++counter, calendarId);
	        	statement.setInt(++counter, entry.getKey());
	        	
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
		catch (SQLException e)
		{
			logger.error("Error while executing UPDATE_SALES_AGGR_DAILY");
			throw new GeneralException("Error while executing UPDATE_SALES_AGGR_DAILY", e);
		}finally{
			PristineDBUtil.close(statement);
		}
	}
	
	
	/**
	 * This method queries for store number and store id for stores that are available for PI for a particular chain
	 * @param chainId	Chain Id
	 * @return HashMap
	 * @throws GeneralException
	 */
	public HashMap<String, Integer> getStoreIds(Connection conn, String chainId) throws GeneralException{
		logger.debug("Inside getStoreIds() of RevenueCorrectionDAO");
		HashMap<String, Integer> storeNumberMap = new HashMap<String, Integer>();
		PreparedStatement statement = null;
	    ResultSet resultSet = null;
		try{
			statement = conn.prepareStatement(GET_STORE_ID);
	        statement.setString(1, chainId);
	        resultSet = statement.executeQuery();
	        while(resultSet.next()){
	        	storeNumberMap.put(resultSet.getString("COMP_STR_NO"), resultSet.getInt("COMP_STR_ID"));
	        }
		}
		catch (SQLException e)
		{
			logger.error("Error while executing GET_STORE_ID");
			throw new GeneralException("Error while executing GET_STORE_ID", e);
		}finally{
			PristineDBUtil.close(resultSet);
			PristineDBUtil.close(statement);
		}
		return storeNumberMap;
	}
	
	/**
	 * Retrieves product group for the specified product level id
	 * @param conn				Connection
	 * @param productLevelId	Product Level Id
	 * @return	HashMap containing product name as key and product id as value
	 * @throws GeneralException
	 */
	public HashMap<String, Integer> getProductGroup(Connection conn, Integer productLevelId) throws GeneralException{
		logger.debug("Inside getProductId() of RevenueCorrectionDAO");
		HashMap<String, Integer> storeNumberMap = new HashMap<String, Integer>();
		PreparedStatement statement = null;
	    ResultSet resultSet = null;
		try{
			statement = conn.prepareStatement(GET_PRODUCT_GROUP);
	        statement.setInt(1, productLevelId);
	        resultSet = statement.executeQuery();
	        while(resultSet.next()){
	        	storeNumberMap.put(resultSet.getString("NAME"), resultSet.getInt("PRODUCT_ID"));
	        }
		}
		catch (SQLException e)
		{
			logger.error("Error while executing GET_PRODUCT_GROUP");
			throw new GeneralException("Error while executing GET_PRODUCT_GROUP", e);
		}finally{
			PristineDBUtil.close(resultSet, statement);
		}
		return storeNumberMap;
	}
	
	/**
	 * Retrieves week calendar data for the actual week number
	 * @param conn			Connection
	 * @param weekNo		Week Number
	 * @return RetailCalendarDTO
	 * @throws GeneralException
	 */
	public RetailCalendarDTO getCalendarId(Connection conn, String weekNo, String calendarYear) throws GeneralException{
		logger.debug("Inside getCalendarId() of RevenueCorrectionDAO");
		PreparedStatement statement = null;
	    ResultSet resultSet = null;
	    RetailCalendarDTO calendarDTO = null;
		try{
			statement = conn.prepareStatement(GET_CALENDAR_INFO);
		    statement.setString(1, calendarYear);
		    statement.setString(2, weekNo);
		    
	        resultSet = statement.executeQuery();
	        if(resultSet.next()){
	        	calendarDTO = new RetailCalendarDTO();
	        	calendarDTO.setCalendarId(resultSet.getInt("CALENDAR_ID"));
	        	calendarDTO.setStartDate(resultSet.getString("START_DATE"));
	        	calendarDTO.setEndDate(resultSet.getString("END_DATE"));
	        }
		}
		catch (SQLException e)
		{
			logger.error("Error while executing GET_CALENDAR_INFO");
			throw new GeneralException("Error while executing GET_CALENDAR_INFO", e);
		}finally{
			try{
				if(resultSet != null){
					resultSet.close();
				}
				if(statement != null){
					statement.close();
				}
			}catch(SQLException e){
				logger.error("Error closing statement");
				throw new GeneralException("Error closing statement", e);
			}
		}
		return calendarDTO;
	}

	/**
	 * Updates revenue for Finance Department
	 * @param conn
	 * @param calendarId
	 * @param revenueMap
	 */
	public void updateWeeklyRevenueForFinanceDept(Connection conn, int calendarId,
			 HashMap<String, Integer> storeIdMap, HashMap<String, Double[]> revenueMap) throws GeneralException{
		logger.debug("Inside updateWeeklyRevenueForFinanceDept() of RevenueCorrectionDAO");
		HashMap<String, Integer> productIdMap = getProductGroup(conn, Constants.FINANCEDEPARTMENT);
		PreparedStatement statement = null;
		DecimalFormat decimalFormat = new DecimalFormat("############.##");
		try{
			statement = conn.prepareStatement(UPDATE_SALES_AGGR_WEEKLY);
	        
			int itemNoInBatch = 0;
			
			for(Map.Entry<String, Double[]> entry : revenueMap.entrySet()){
				Double[] revenueArr = entry.getValue();
				
				if(storeIdMap.get(entry.getKey()) != null){
					setValues(statement, storeIdMap.get(entry.getKey()), productIdMap.get("GROCERY"), Double.valueOf(decimalFormat.format(revenueArr[0])), calendarId, ++itemNoInBatch);
					setValues(statement, storeIdMap.get(entry.getKey()), productIdMap.get("DAIRY"), Double.valueOf(decimalFormat.format(revenueArr[1])), calendarId, ++itemNoInBatch);
					setValues(statement, storeIdMap.get(entry.getKey()), productIdMap.get("FROZEN FOOD"), Double.valueOf(decimalFormat.format(revenueArr[2])), calendarId, ++itemNoInBatch);
					setValues(statement, storeIdMap.get(entry.getKey()), productIdMap.get("GENERAL MERCH"), Double.valueOf(decimalFormat.format(revenueArr[3])), calendarId, ++itemNoInBatch);
					setValues(statement, storeIdMap.get(entry.getKey()), productIdMap.get("HBC"), Double.valueOf(decimalFormat.format(revenueArr[4])), calendarId, ++itemNoInBatch);
					setValues(statement, storeIdMap.get(entry.getKey()), productIdMap.get("PHARMACY"), Double.valueOf(decimalFormat.format(revenueArr[5])), calendarId, ++itemNoInBatch);
					setValues(statement, storeIdMap.get(entry.getKey()), productIdMap.get("PRODUCE"), Double.valueOf(decimalFormat.format(revenueArr[6])), calendarId, ++itemNoInBatch);
					setValues(statement, storeIdMap.get(entry.getKey()), productIdMap.get("FLORAL"), Double.valueOf(decimalFormat.format(revenueArr[7])), calendarId, ++itemNoInBatch);
					setValues(statement, storeIdMap.get(entry.getKey()), productIdMap.get("MEAT"), Double.valueOf(decimalFormat.format(revenueArr[8])), calendarId, ++itemNoInBatch);
					setValues(statement, storeIdMap.get(entry.getKey()), productIdMap.get("SEAFOOD"), Double.valueOf(decimalFormat.format(revenueArr[9])), calendarId, ++itemNoInBatch);
					setValues(statement, storeIdMap.get(entry.getKey()), productIdMap.get("DELI"), Double.valueOf(decimalFormat.format(revenueArr[10])), calendarId, ++itemNoInBatch);
					setValues(statement, storeIdMap.get(entry.getKey()), productIdMap.get("CARRY OUT CAFE"), Double.valueOf(decimalFormat.format(revenueArr[11])), calendarId, ++itemNoInBatch);
					setValues(statement, storeIdMap.get(entry.getKey()), productIdMap.get("BAKERY"), Double.valueOf(decimalFormat.format(revenueArr[12])), calendarId, ++itemNoInBatch);
					setValues(statement, storeIdMap.get(entry.getKey()), productIdMap.get("PROMOTION"), Double.valueOf(decimalFormat.format(revenueArr[13])), calendarId, ++itemNoInBatch);
					setValues(statement, storeIdMap.get(entry.getKey()), productIdMap.get("GAS"), Double.valueOf(decimalFormat.format(revenueArr[14])), calendarId, ++itemNoInBatch);
				}else{
					logger.info("Store not found in database for store number - " + entry.getKey());
				}
			}
			
	        if(itemNoInBatch > 0){
	        	int[] count = statement.executeBatch();
        		statement.clearBatch();
	        }
	    }
		catch (SQLException e)
		{
			e.printStackTrace();
			logger.error("Error while executing UPDATE_SALES_AGGR_WEEKLY" + e);
			throw new GeneralException("Error while executing UPDATE_SALES_AGGR_WEEKLY", e);
		}finally{
			PristineDBUtil.close(statement);
		}
	}


	private void setValues(PreparedStatement statement, Integer locationId,
			Integer productId, Double revenue, int calendarId, int itemNoInBatch) throws SQLException{
		int counter = 0;
		statement.setDouble(++counter, revenue);
		statement.setDouble(++counter, revenue);
		statement.setInt(++counter, calendarId);
    	statement.setInt(++counter, locationId);
    	statement.setInt(++counter, productId);

    	statement.addBatch();
    	
    	if(itemNoInBatch % Constants.BATCH_UPDATE_COUNT == 0){
    		int[] count = statement.executeBatch();
    		statement.clearBatch();
    		itemNoInBatch = 0;
    	}
	}
	
	/**
	 * Updates weekly revenue, average order size, total visit count for stores
	 * @param conn
	 * @param calendarId
	 * @param storeIdMap
	 * @param storeRevenueMap
	 * @throws GeneralException
	 */
	public void updateWeeklyVisitCount(Connection conn, int calendarId,
			 HashMap<String, Integer> storeIdMap, HashMap<String, Double[]> storeRevenueMap, boolean updateRevenue, boolean updateVisitCount) throws GeneralException{
		logger.debug("Inside updateWeeklyRevenue() of RevenueCorrectionDAO");
		PreparedStatement statement = null;
		DecimalFormat decimalFormat = new DecimalFormat("############.##");
		DecimalFormat decimalFormatAOS = new DecimalFormat("#####.##");
		try{
			String sql = UPDATE_SALES_AGGR_WEEKLY_STORE_DATA_VISIT_CNT;
			
			if(updateRevenue && updateVisitCount)
				sql = UPDATE_SALES_AGGR_WEEKLY_STORE_DATA;
			else if(updateVisitCount)
				sql = UPDATE_SALES_AGGR_WEEKLY_STORE_DATA_VISIT_CNT;
			
			statement = conn.prepareStatement(sql);
			
			int itemNoInBatch = 0;
			
			for(Map.Entry<String, Double[]> entry : storeRevenueMap.entrySet()){
				Double[] revenueArr = entry.getValue();
				int counter = 0;
				if(storeIdMap.get(entry.getKey()) != null){
					if(updateRevenue && updateVisitCount){
						statement.setDouble(++counter, Double.parseDouble(decimalFormat.format(revenueArr[0])));
						statement.setInt(++counter, revenueArr[1].intValue());
						statement.setDouble(++counter, Double.parseDouble(decimalFormatAOS.format(revenueArr[2])));
						statement.setInt(++counter, calendarId);
			        	statement.setInt(++counter, storeIdMap.get(entry.getKey()));
					}else{
						statement.setInt(++counter, revenueArr[1].intValue());
						statement.setInt(++counter, revenueArr[1].intValue());
						statement.setInt(++counter, calendarId);
			        	statement.setInt(++counter, storeIdMap.get(entry.getKey()));
					}
		        	
		        	statement.addBatch();
		        	itemNoInBatch++;
	        	
		        	if(itemNoInBatch % Constants.BATCH_UPDATE_COUNT == 0){
		        		int[] count = statement.executeBatch();
		        		statement.clearBatch();
		        		itemNoInBatch = 0;
		        	}
				}
			}
			
	        if(itemNoInBatch > 0){
	        	int[] count = statement.executeBatch();
        		statement.clearBatch();
	        }
	    }
		catch (SQLException e)
		{
			e.printStackTrace();
			logger.error("Error while executing UPDATE_SALES_AGGR_WEEKLY_STORE" + e);
			throw new GeneralException("Error while executing UPDATE_SALES_AGGR_WEEKLY_STORE", e);
		}finally{
			PristineDBUtil.close(statement);
		}
	}
	
	/**
	 * Returns a HashMap containing week calendar id as key and day calendar id of the corresponding week's end date as value
	 * for all the weeks between weekStartNumber and weekEndNumber for the given calendar  year 
	 * @param conn				Connection			
	 * @param weekStartNumber	Week Start Number
	 * @param weekEndNumber		Week End Number
	 * @param calYear			Calendar Year
	 * @return
	 * @throws GeneralException
	 */
	public HashMap<Integer, Integer> getWeeklyCalendarIdMapping(Connection conn, String weekStartNumber, String weekEndNumber, String calYear)
															throws GeneralException{
		logger.debug("Inside getWeeklyCalendarIdMapping() of RevenueCorrectionDAO");
		HashMap<Integer, Integer> calendarMap = new HashMap<Integer, Integer>();
		PreparedStatement stmt = null;
		ResultSet rs = null;
		try{
			stmt = conn.prepareStatement(GET_WEEKLY_CALENDARID_MAP);
			stmt.setString(1, weekStartNumber);
			stmt.setString(2, weekEndNumber);
			stmt.setString(3, calYear);
			rs = stmt.executeQuery();
			while(rs.next()){
				calendarMap.put(rs.getInt("WEEK_CALENDAR_ID"), rs.getInt("DAY_CALENDAR_ID"));
			}
		}catch (SQLException e){
			logger.error("Error while executing GET_WEEKLY_CALENDARID_MAP" + e);
			throw new GeneralException("Error while executing GET_WEEKLY_CALENDARID_MAP", e);
		}finally{
			PristineDBUtil.close(rs, stmt);
		}
		return calendarMap;
	}
	
	/**
	 * Retrieves finance department data for all calendar ids given in the input and returns a map 
	 * containing calendar id as key and map containing location and product id as key and corresponding 
	 * revenue, total visit count as array of values 
	 * @param conn				Connection
	 * @param calendarIdSet		Set of Calendar Ids
	 * @return	Map
	 * @throws GeneralException
	 */
	public HashMap<Integer, HashMap<String, Double[]>> getFinanceDeptWeeklyData(Connection conn, Set<Integer> calendarIdSet, boolean retrieveRolledupData)
															throws GeneralException{
		logger.debug("Inside getFinanceDeptWeeklyData() of RevenueCorrectionDAO");
		PreparedStatement stmt = null;
	    ResultSet rs = null;
	    HashMap<Integer, HashMap<String, Double[]>> financeDeptWeeklyData = new HashMap<Integer, HashMap<String,Double[]>>();
	    Object[] values = calendarIdSet.toArray();
	    try{
	    	if(!retrieveRolledupData)
	    		stmt = conn.prepareStatement(String.format(GET_FINANCE_DEPT_WEEKLY_DATA, PristineDBUtil.preparePlaceHolders(values.length)));
	    	else
	    		stmt = conn.prepareStatement(String.format(GET_FINANCE_DEPT_WEEKLY_ROLLUP_DATA, PristineDBUtil.preparePlaceHolders(values.length)));
			PristineDBUtil.setValues(stmt, values);
	        rs = stmt.executeQuery();
	        while(rs.next()){
	        	int calendarId = rs.getInt("CALENDAR_ID");
	        	if(financeDeptWeeklyData.get(calendarId) != null){
	        		if(!retrieveRolledupData){
		        		HashMap<String, Double[]> tempMap = financeDeptWeeklyData.get(calendarId);
		        		tempMap.put(rs.getString("LOCATION_LEVEL_ID") + Constants.INDEX_DELIMITER + rs.getString("LOCATION_ID") + Constants.INDEX_DELIMITER + rs.getString("PRODUCT_ID"), 
		        								new Double[]{rs.getDouble("TOT_REVENUE"), rs.getDouble("TOT_VISIT_CNT")});
		        		financeDeptWeeklyData.put(calendarId, tempMap);
	        		}else{
	        			HashMap<String, Double[]> tempMap = financeDeptWeeklyData.get(calendarId);
		        		tempMap.put(rs.getString("LOCATION_LEVEL_ID") + Constants.INDEX_DELIMITER + rs.getString("LOCATION_ID") + Constants.INDEX_DELIMITER + rs.getString("PRODUCT_ID"), 
		        								new Double[]{rs.getDouble("TOT_REVENUE"), rs.getDouble("TOT_VISIT_CNT"), rs.getDouble("ID_TOT_REVENUE"), rs.getDouble("ID_TOT_VISIT_CNT")});
		        		financeDeptWeeklyData.put(calendarId, tempMap);
	        		}
	        	}else{
	        		if(!retrieveRolledupData){
		        		HashMap<String, Double[]> tempMap = new HashMap<String, Double[]>();
		        		tempMap.put(rs.getString("LOCATION_LEVEL_ID") + Constants.INDEX_DELIMITER + rs.getString("LOCATION_ID") + Constants.INDEX_DELIMITER + rs.getString("PRODUCT_ID"), 
								new Double[]{rs.getDouble("TOT_REVENUE"), rs.getDouble("TOT_VISIT_CNT")});
		        		financeDeptWeeklyData.put(calendarId, tempMap);
	        		}else{
	        			HashMap<String, Double[]> tempMap = new HashMap<String, Double[]>();
		        		tempMap.put(rs.getString("LOCATION_LEVEL_ID") + Constants.INDEX_DELIMITER + rs.getString("LOCATION_ID") + Constants.INDEX_DELIMITER + rs.getString("PRODUCT_ID"), 
								new Double[]{rs.getDouble("TOT_REVENUE"), rs.getDouble("TOT_VISIT_CNT"), rs.getDouble("ID_TOT_REVENUE"), rs.getDouble("ID_TOT_VISIT_CNT")});
		        		financeDeptWeeklyData.put(calendarId, tempMap);
	        		}
	        	}
	        }
		}catch (SQLException e){
			logger.error("Error while executing GET_FINANCE_DEPT_WEEKLY_DATA" + e);
			throw new GeneralException("Error while executing GET_FINANCE_DEPT_WEEKLY_DATA", e);
		}finally{
			PristineDBUtil.close(rs, stmt);
		}
	    return financeDeptWeeklyData;
	}
	
	/**
	 * Retrieves store data for all calendar ids given in the input and returns a map 
	 * containing calendar id as key and map containing location and product id as key and corresponding 
	 * @param conn				Database Connection
	 * @param calendarIdSet		Set of Calendar Id(s)
	 * @return
	 * @throws GeneralException
	 */
	public HashMap<Integer, HashMap<String, Double[]>> getStoreLevelWeeklyData(Connection conn, Set<Integer> calendarIdSet, boolean retrieveRolledupData)
																					throws GeneralException{
		logger.debug("Inside getStoreLevelWeeklyData() of RevenueCorrectionDAO");
		PreparedStatement stmt = null;
		ResultSet rs = null;
		HashMap<Integer, HashMap<String, Double[]>> storeLevelWeeklyData = new HashMap<Integer, HashMap<String,Double[]>>();
		Object[] values = calendarIdSet.toArray();
		try{
			if(!retrieveRolledupData)
				stmt = conn.prepareStatement(String.format(GET_STORE_LEVEL_WEEKLY_DATA, PristineDBUtil.preparePlaceHolders(values.length)));
			else 
				stmt = conn.prepareStatement(String.format(GET_STORE_LEVEL_WEEKLY_ROLLUP_DATA, PristineDBUtil.preparePlaceHolders(values.length)));
			PristineDBUtil.setValues(stmt, values);
			rs = stmt.executeQuery();
			while(rs.next()){
				int calendarId = rs.getInt("CALENDAR_ID");
				if(storeLevelWeeklyData.get(calendarId) != null){
					if(!retrieveRolledupData){
						HashMap<String, Double[]> tempMap = storeLevelWeeklyData.get(calendarId);
						tempMap.put(rs.getString("LOCATION_LEVEL_ID") + Constants.INDEX_DELIMITER + rs.getString("LOCATION_ID"),new Double[]{rs.getDouble("TOT_REVENUE"), rs.getDouble("TOT_VISIT_CNT")});
						storeLevelWeeklyData.put(calendarId, tempMap);
					}else{
						HashMap<String, Double[]> tempMap = storeLevelWeeklyData.get(calendarId);
						tempMap.put(rs.getString("LOCATION_LEVEL_ID") + Constants.INDEX_DELIMITER + rs.getString("LOCATION_ID"),new Double[]{rs.getDouble("TOT_REVENUE"), rs.getDouble("TOT_VISIT_CNT"), rs.getDouble("ID_TOT_REVENUE"), rs.getDouble("ID_TOT_VISIT_CNT")});
						storeLevelWeeklyData.put(calendarId, tempMap);
					}
				}else{
					if(!retrieveRolledupData){
						HashMap<String, Double[]> tempMap = new HashMap<String, Double[]>();
						tempMap.put(rs.getString("LOCATION_LEVEL_ID") + Constants.INDEX_DELIMITER + rs.getString("LOCATION_ID"),new Double[]{rs.getDouble("TOT_REVENUE"), rs.getDouble("TOT_VISIT_CNT")}); 
						storeLevelWeeklyData.put(calendarId, tempMap);
					}else{
						HashMap<String, Double[]> tempMap = new HashMap<String, Double[]>();
						tempMap.put(rs.getString("LOCATION_LEVEL_ID") + Constants.INDEX_DELIMITER + rs.getString("LOCATION_ID"),new Double[]{rs.getDouble("TOT_REVENUE"), rs.getDouble("TOT_VISIT_CNT"), rs.getDouble("ID_TOT_REVENUE"), rs.getDouble("ID_TOT_VISIT_CNT")}); 
						storeLevelWeeklyData.put(calendarId, tempMap);
					}
				}
			}
		}catch (SQLException e){
			logger.error("Error while executing GET_STORE_LEVEL_WEEKLY_DATA" + e);
			throw new GeneralException("Error while executing GET_STORE_LEVEL_WEEKLY_DATA", e);
		}finally{
			PristineDBUtil.close(rs, stmt);
		}
		return storeLevelWeeklyData;
	}
	
	/**
	 * Updates Week to date data of finance dept
	 * @param conn					Connection
	 * @param financeDeptDataMap	Map containing calendar id as key and map containing location and product id as key and corresponding 
	 * 								revenue, total visit count as array of values 
	 * @param calendarMap			Map containing week calendar id as key and corresponding week's saturday calendar id as value
	 * @throws GeneralException
	 */
	public void updateWeekToDateData(Connection conn, HashMap<Integer, HashMap<String, Double[]>> financeDeptDataMap, HashMap<Integer, Integer> calendarMap, boolean updateRolledupData)
															throws GeneralException{
		logger.debug("Inside updateWeekToDateData() of RevenueCorrectionDAO");
		PreparedStatement stmt = null;
	    DecimalFormat decimalFormat = new DecimalFormat("############.##");
	    int itemNoInBatch = 0;
	    try{
	    	if(!updateRolledupData)
	    		stmt = conn.prepareStatement(UPDATE_CTD_DATA_FOR_FINANCE_DEPT);
	    	else
	    		stmt = conn.prepareStatement(UPDATE_ROLLEDUP_CTD_DATA_FOR_FINANCE_DEPT);
			for(Map.Entry<Integer, HashMap<String, Double[]>> entry : financeDeptDataMap.entrySet()){
				int calendarId = entry.getKey();
				for(Map.Entry<String, Double[]> inEntry : entry.getValue().entrySet()){
					String[] key = inEntry.getKey().split(Constants.INDEX_DELIMITER);
					Double[] value = inEntry.getValue();
					int counter = 0;
					stmt.setDouble(++counter, new Double(decimalFormat.format(value[0])));
					stmt.setInt(++counter, value[1].intValue());
					stmt.setDouble(++counter, value[0]);
					stmt.setInt(++counter, value[1].intValue());
					if(updateRolledupData){
						stmt.setDouble(++counter, new Double(decimalFormat.format(value[2])));
						stmt.setInt(++counter, value[3].intValue());
						stmt.setDouble(++counter, value[2]);
						stmt.setInt(++counter, value[3].intValue());
					}
					stmt.setInt(++counter, calendarMap.get(calendarId));
					stmt.setInt(++counter, Integer.parseInt(key[0]));
					stmt.setInt(++counter, Integer.parseInt(key[1]));
					stmt.setInt(++counter, Integer.parseInt(key[2]));
					stmt.setInt(++counter, Constants.CTD_WEEK);
					
					if(updateRolledupData){
						if(value[0] > 0 && value[1] > 0 && value[2] > 0 && value[3] > 0){
							stmt.addBatch();
							itemNoInBatch++;
						}
					}else{
						if(value[0] > 0 && value[1] > 0){
							stmt.addBatch();
							itemNoInBatch++;
						}
					}
					
					if(itemNoInBatch % Constants.BATCH_UPDATE_COUNT == 0){
		        		int[] count = stmt.executeBatch();
		        		stmt.clearBatch();
		        		itemNoInBatch = 0;
		        	}
				}
			}
			
			if(itemNoInBatch > 0){
        		int[] count = stmt.executeBatch();
        		stmt.clearBatch();
        		itemNoInBatch = 0;
        	}
	    }catch (SQLException e){
	    	e.printStackTrace();
			logger.error("Error while executing UPDATE_CTD_DATA_FOR_FINANCE_DEPT" + e);
			throw new GeneralException("Error while executing UPDATE_CTD_DATA_FOR_FINANCE_DEPT", e);
		}finally{
			PristineDBUtil.close(stmt);
		}
	}
	
	/**
	 * Updates Week to date data of Store
	 * @param conn					Connection
	 * @param storeDataMap	Map containing calendar id as key and map containing location level id and location id as key and corresponding 
	 * 								revenue, total visit count as array of values 
	 * @param calendarMap			Map containing week calendar id as key and corresponding week's saturday calendar id as value
	 * @throws GeneralException
	 */
	public void updateWeekToDateDataForStore(Connection conn, HashMap<Integer, HashMap<String, Double[]>> storeDataMap, HashMap<Integer, Integer> calendarMap, boolean updateRolledupData,
			boolean updateRevenue, boolean updateVisitCount)
																																throws GeneralException{
		logger.debug("Inside updateWeekToDateDataForStore() of RevenueCorrectionDAO");
		PreparedStatement stmt = null;
		DecimalFormat decimalFormat = new DecimalFormat("############.##");
		int itemNoInBatch = 0;
		try{
			if(!updateRolledupData){
				if(updateRevenue && updateVisitCount)
					stmt = conn.prepareStatement(UPDATE_CTD_DATA_FOR_STORE);
				else
					stmt = conn.prepareStatement(UPDATE_CTD_DATA_FOR_STORE_VISIT_COUNT);
			}else{
				if(updateRevenue && updateVisitCount)
					stmt = conn.prepareStatement(UPDATE_ROLLEDUP_CTD_DATA_FOR_STORE);
				else
					stmt = conn.prepareStatement(UPDATE_ROLLEDUP_CTD_DATA_FOR_STORE_VISIT_COUNT);
			}
			
			for(Map.Entry<Integer, HashMap<String, Double[]>> entry : storeDataMap.entrySet()){
				int calendarId = entry.getKey();
				for(Map.Entry<String, Double[]> inEntry : entry.getValue().entrySet()){
					String[] key = inEntry.getKey().split(Constants.INDEX_DELIMITER);
					Double[] value = inEntry.getValue();
					int counter = 0;
					
					if(updateRevenue && updateVisitCount){
						stmt.setDouble(++counter, new Double(decimalFormat.format(value[0])));
						stmt.setInt(++counter, value[1].intValue());
						stmt.setDouble(++counter, value[0]);
						stmt.setInt(++counter, value[1].intValue());
					}else{
						stmt.setInt(++counter, value[1].intValue());
						stmt.setInt(++counter, value[1].intValue());
					}
					if(updateRolledupData){
						if(updateRevenue && updateVisitCount){
							stmt.setDouble(++counter, new Double(decimalFormat.format(value[2])));
							stmt.setInt(++counter, value[3].intValue());
							stmt.setDouble(++counter, value[2]);
							stmt.setInt(++counter, value[3].intValue());
						}else{
							stmt.setInt(++counter, value[3].intValue());
							stmt.setInt(++counter, value[3].intValue());
						}
					}
					stmt.setInt(++counter, calendarMap.get(calendarId));
					stmt.setInt(++counter, Integer.parseInt(key[0]));
					stmt.setInt(++counter, Integer.parseInt(key[1]));
					stmt.setInt(++counter, Constants.CTD_WEEK);
					
					if(updateRolledupData){
						if(value[0] > 0 && value[1] > 0 && value[2] > 0 && value[3] > 0){
							stmt.addBatch();
							itemNoInBatch++;
				        }
					}else{
						if(value[0] > 0 && value[1] > 0){
							stmt.addBatch();
							itemNoInBatch++;
				        }
					}
					
					if(itemNoInBatch % Constants.BATCH_UPDATE_COUNT == 0){
					int[] count = stmt.executeBatch();
					stmt.clearBatch();
					itemNoInBatch = 0;
					}
				}
			}
			
			if(itemNoInBatch > 0){
				int[] count = stmt.executeBatch();
				stmt.clearBatch();
				itemNoInBatch = 0;
			}
		}catch (SQLException e){
			logger.error("Error while executing UPDATE_CTD_DATA_FOR_STORE" + e);
			throw new GeneralException("Error while executing UPDATE_CTD_DATA_FOR_STORE", e);
		}finally{
			PristineDBUtil.close(stmt);
		}
	}
	
	/**
	 * Returns a HashMap containing calendar id (of period/quarter/year) as key and set of weekly calendar ids as value
	 * for all the weeks between weekStartNumber and weekEndNumber for the given calendar  year 
	 * @param conn				Connection		
	 * @param rowType			P - Period/ Q - Quarter/ Y - Year	
	 * @param weekStartNumber	Week Start Number
	 * @param weekEndNumber		Week End Number
	 * @param calYear			Calendar Year
	 * @return
	 * @throws GeneralException
	 */
	public HashMap<Integer, TreeSet<Integer>> getCalendarIdMapping(Connection conn, String rowType, String weekStartNumber, String weekEndNumber, String calYear)
															throws GeneralException{
		logger.debug("Inside getCalendarIdMapping() of RevenueCorrectionDAO");
		HashMap<Integer, TreeSet<Integer>> calendarMap = new HashMap<Integer, TreeSet<Integer>>();
		PreparedStatement stmt = null;
		ResultSet rs = null;
		try{
			stmt = conn.prepareStatement(GET_CALENDARID_MAP);
			stmt.setString(1, rowType);
			stmt.setString(2, weekStartNumber);
			stmt.setString(3, weekEndNumber);
			stmt.setString(4, calYear);
			rs = stmt.executeQuery();
			while(rs.next()){
				if(calendarMap.get(rs.getInt("CALENDAR_ID")) != null){
					TreeSet<Integer> tempSet = calendarMap.get(rs.getInt("CALENDAR_ID"));
					tempSet.add(rs.getInt("WEEK_CALENDAR_ID"));
					calendarMap.put(rs.getInt("CALENDAR_ID"), tempSet);
				}else{
					TreeSet<Integer> tempSet = new TreeSet<Integer>();
					tempSet.add(rs.getInt("WEEK_CALENDAR_ID"));
					calendarMap.put(rs.getInt("CALENDAR_ID"), tempSet);
				}
			}
		}catch (SQLException e){
			logger.error("Error while executing GET_WEEKLY_CALENDARID_MAP" + e);
			throw new GeneralException("Error while executing GET_WEEKLY_CALENDARID_MAP", e);
		}finally{
			PristineDBUtil.close(rs, stmt);
		}
		return calendarMap;
	}

	/**
	 * Updates CTD data for Finance Department
	 * @param conn					Connection
	 * @param financeDeptMap		Map containing finance department revenue details
	 * @param periodToWeeksMap		Period Calendar Id to Week Calendar Id Mapping
	 * @param calendarMap			Week Calendar Id to Saturday Calendar Id Mapping
	 * @param ctdType				Week/Period/Quarter/Year
	 * @param updateRolledupData	If the update should happen in store table or roll up table
	 * @throws GeneralException
	 */
	public void updateCTDDataForFD(Connection conn,	HashMap<Integer, HashMap<String, Double[]>> financeDeptMap,
			HashMap<Integer, TreeSet<Integer>> periodToWeeksMap, HashMap<Integer, Integer> calendarMap, int ctdType, boolean updateRolledupData) throws GeneralException{
		logger.debug("Inside updateCTDData() of RevenueCorrectionDAO");
		
		DecimalFormat decimalFormat = new DecimalFormat("############.##");
		int itemNoInBatch = 0;
		PreparedStatement stmt = null;
		try{
			if(!updateRolledupData)
				stmt = conn.prepareStatement(UPDATE_CTD_DATA_FOR_FINANCE_DEPT);
			else
				stmt = conn.prepareStatement(UPDATE_ROLLEDUP_CTD_DATA_FOR_FINANCE_DEPT);
			for(Map.Entry<Integer, TreeSet<Integer>> entry : periodToWeeksMap.entrySet()){
				HashMap<String, Double[]> aggrFinanceDeptMap = new HashMap<String, Double[]>();
				for(int weekCalendarId : entry.getValue()){
					HashMap<String, Double[]> financeDataforWeek = financeDeptMap.get(weekCalendarId);
					
					if(financeDataforWeek != null){
						for(Map.Entry<String, Double[]> inEntry : financeDataforWeek.entrySet()){
							String[] key = inEntry.getKey().split(Constants.INDEX_DELIMITER);
							Double[] value = inEntry.getValue();
							if(aggrFinanceDeptMap.get(inEntry.getKey()) != null){
								Double[] aggrValue = aggrFinanceDeptMap.get(inEntry.getKey());
								Double totRevenue = aggrValue[0] + value[0];
								Double visitCnt = aggrValue[1] + value[1];
								Double idTotRevenue = null;
								Double idTotVisitCnt = null;
								int counter = 0;
								stmt.setDouble(++counter, new Double(decimalFormat.format(totRevenue)));
								stmt.setInt(++counter, visitCnt.intValue());
								stmt.setDouble(++counter, totRevenue);
								stmt.setInt(++counter, visitCnt.intValue());
								if(updateRolledupData){
									idTotRevenue = aggrValue[2] + value[2];
									idTotVisitCnt = aggrValue[3] + value[3];
									stmt.setDouble(++counter, new Double(decimalFormat.format(idTotRevenue)));
									stmt.setInt(++counter, idTotVisitCnt.intValue());
									stmt.setDouble(++counter, idTotRevenue);
									stmt.setInt(++counter, idTotVisitCnt.intValue());
								}
								stmt.setInt(++counter, calendarMap.get(weekCalendarId));
								stmt.setInt(++counter, Integer.parseInt(key[0]));
								stmt.setInt(++counter, Integer.parseInt(key[1]));
								stmt.setInt(++counter, Integer.parseInt(key[2]));
								stmt.setInt(++counter, ctdType);
								
								if(updateRolledupData){
									if(totRevenue > 0 && visitCnt > 0 && idTotRevenue > 0 && idTotVisitCnt > 0){
										stmt.addBatch();
										itemNoInBatch++;
									}
								}else{
									if(totRevenue > 0 && visitCnt > 0){
										stmt.addBatch();
										itemNoInBatch++;
									}
								}
								if(updateRolledupData)
									aggrFinanceDeptMap.put(inEntry.getKey(), new Double[]{totRevenue, visitCnt, idTotRevenue, idTotVisitCnt});
								else
									aggrFinanceDeptMap.put(inEntry.getKey(), new Double[]{totRevenue, visitCnt});
							}else{
								int counter = 0;
								stmt.setDouble(++counter, new Double(decimalFormat.format(value[0])));
								stmt.setInt(++counter, value[1].intValue());
								stmt.setDouble(++counter, value[0]);
								stmt.setInt(++counter, value[1].intValue());
								if(updateRolledupData){
									stmt.setDouble(++counter, new Double(decimalFormat.format(value[2])));
									stmt.setInt(++counter, value[3].intValue());
									stmt.setDouble(++counter, value[2]);
									stmt.setInt(++counter, value[3].intValue());
								}
								stmt.setInt(++counter, calendarMap.get(weekCalendarId));
								stmt.setInt(++counter, Integer.parseInt(key[0]));
								stmt.setInt(++counter, Integer.parseInt(key[1]));
								stmt.setInt(++counter, Integer.parseInt(key[2]));
								stmt.setInt(++counter, ctdType);
								
								if(updateRolledupData){
									if(value[0] > 0 && value[1] > 0 && value[2] > 0 && value[3] > 0){
										stmt.addBatch();
										itemNoInBatch++;
									}
								}else{
									if(value[0] > 0 && value[1] > 0){
										stmt.addBatch();
										itemNoInBatch++;
									}
								}
								
								aggrFinanceDeptMap.put(inEntry.getKey(), value);
							}
						}
						if(itemNoInBatch % Constants.BATCH_UPDATE_COUNT == 0){
			        		int[] count = stmt.executeBatch();
			        		stmt.clearBatch();
			        		itemNoInBatch = 0;
			        	}
					}
				}
				
				updateSalesAggrForFD(conn, entry.getKey(), aggrFinanceDeptMap, updateRolledupData);
			}
			
			if(itemNoInBatch > 0){
        		int[] count = stmt.executeBatch();
        		stmt.clearBatch();
        		itemNoInBatch = 0;
        	}
	    }catch (SQLException e){
	    	e.printStackTrace();
			logger.error("Error while executing UPDATE_CTD_DATA_FOR_FINANCE_DEPT" + e);
			throw new GeneralException("Error while executing UPDATE_CTD_DATA_FOR_FINANCE_DEPT", e);
		}finally{
			PristineDBUtil.close(stmt);
		}
	}
	
	private void updateSalesAggrForFD(Connection conn, Integer calendarId, HashMap<String, Double[]> aggrFinanceDeptMap, boolean updateRolledupData) throws GeneralException{
		logger.debug("Inside updateSalesAggr() of RevenueCorrectionDAO");
		DecimalFormat decimalFormat = new DecimalFormat("############.##");
		PreparedStatement stmt = null;
		int itemNoInBatch = 0;
		try{
			if(!updateRolledupData)
				stmt = conn.prepareStatement(UPDATE_SALES_AGGR_FOR_FINANCE_DEPT);
			else
				stmt = conn.prepareStatement(UPDATE_SALES_AGGR_ROLLUP_FOR_FINANCE_DEPT);
			for(Map.Entry<String, Double[]> entry : aggrFinanceDeptMap.entrySet()){
				String[] key = entry.getKey().split(Constants.INDEX_DELIMITER);
				Double[] value = entry.getValue();
				int counter = 0;
				stmt.setDouble(++counter, new Double(decimalFormat.format(value[0])));
				stmt.setInt(++counter, value[1].intValue());
				stmt.setDouble(++counter, value[0]);
				stmt.setInt(++counter, value[1].intValue());
				if(updateRolledupData){
					stmt.setDouble(++counter, new Double(decimalFormat.format(value[2])));
					stmt.setInt(++counter, value[3].intValue());
					stmt.setDouble(++counter, value[2]);
					stmt.setInt(++counter, value[3].intValue());
				}
				stmt.setInt(++counter, calendarId);
				stmt.setInt(++counter, Integer.parseInt(key[0]));
				stmt.setInt(++counter, Integer.parseInt(key[1]));
				stmt.setInt(++counter, Integer.parseInt(key[2]));
				
				if(updateRolledupData){
					if(value[0] > 0 && value[1] > 0 && value[2] > 0 && value[3] > 0)
						stmt.addBatch();
				}else{
					if(value[0] > 0 && value[1] > 0)
						stmt.addBatch();
				}
				
				if(itemNoInBatch % Constants.BATCH_UPDATE_COUNT == 0){
	        		int[] count = stmt.executeBatch();
	        		stmt.clearBatch();
	        		itemNoInBatch = 0;
	        	}
			}
			if(itemNoInBatch > 0){
        		int[] count = stmt.executeBatch();
        		stmt.clearBatch();
        		itemNoInBatch = 0;
        	}
		}catch (SQLException e){
			e.printStackTrace();
			logger.error("Error while executing UPDATE_CTD_DATA_FOR_FINANCE_DEPT" + e);
			throw new GeneralException("Error while executing UPDATE_CTD_DATA_FOR_FINANCE_DEPT", e);
		}finally{
			PristineDBUtil.close(stmt);
		}
	}

	/**	
	 * 
	 * @param conn
	 * @param storeDataMap
	 * @param periodToWeeksMap
	 * @param calendarMap
	 * @param ctdType
	 * @param updateRolledupData
	 * @throws GeneralException
	 */
	public void updateCTDDataForStore(Connection conn,	HashMap<Integer, HashMap<String, Double[]>> storeDataMap,
			HashMap<Integer, TreeSet<Integer>> periodToWeeksMap, HashMap<Integer, Integer> calendarMap, int ctdType, boolean updateRolledupData,
			boolean updateRevenue, boolean updateVisitCount) throws GeneralException{
		logger.debug("Inside updateCTDData() of RevenueCorrectionDAO");
		
		DecimalFormat decimalFormat = new DecimalFormat("############.##");
		int itemNoInBatch = 0;
		PreparedStatement stmt = null;
		try{
			if(!updateRolledupData){
				if(updateRevenue && updateVisitCount)
					stmt = conn.prepareStatement(UPDATE_CTD_DATA_FOR_STORE);
				else
					stmt = conn.prepareStatement(UPDATE_CTD_DATA_FOR_STORE_VISIT_COUNT);
			}else{
				if(updateRevenue && updateVisitCount)
					stmt = conn.prepareStatement(UPDATE_ROLLEDUP_CTD_DATA_FOR_STORE);
				else
					stmt = conn.prepareStatement(UPDATE_ROLLEDUP_CTD_DATA_FOR_STORE_VISIT_COUNT);
			}	
			for(Map.Entry<Integer, TreeSet<Integer>> entry : periodToWeeksMap.entrySet()){
				HashMap<String, Double[]> aggrFinanceDeptMap = new HashMap<String, Double[]>();
				for(int weekCalendarId : entry.getValue()){
					HashMap<String, Double[]> storeDataForWeek = storeDataMap.get(weekCalendarId);
					
					if(storeDataForWeek != null){
						for(Map.Entry<String, Double[]> inEntry : storeDataForWeek.entrySet()){
							String[] key = inEntry.getKey().split(Constants.INDEX_DELIMITER);
							Double[] value = inEntry.getValue();
							int counter = 0;
							if(aggrFinanceDeptMap.get(inEntry.getKey()) != null){
								Double[] aggrValue = aggrFinanceDeptMap.get(inEntry.getKey());
								Double totRevenue = aggrValue[0] + value[0];
								Double visitCnt = aggrValue[1] + value[1];
								Double idTotRevenue = null;
								Double idVisitCnt = null;
								
								if(updateRevenue && updateVisitCount){
									stmt.setDouble(++counter, new Double(decimalFormat.format(totRevenue)));
									stmt.setInt(++counter, visitCnt.intValue());
									stmt.setDouble(++counter, totRevenue);
									stmt.setInt(++counter, visitCnt.intValue());
								}else{
									stmt.setInt(++counter, visitCnt.intValue());
									stmt.setInt(++counter, visitCnt.intValue());
								}
								
								if(updateRolledupData){
									idTotRevenue = aggrValue[2] + value[2];
									idVisitCnt = aggrValue[3] + value[3];
									
									if(updateRevenue && updateVisitCount){
										stmt.setDouble(++counter, new Double(decimalFormat.format(idTotRevenue)));
										stmt.setInt(++counter, idVisitCnt.intValue());
										stmt.setDouble(++counter, idTotRevenue);
										stmt.setInt(++counter, idVisitCnt.intValue());
									}else{
										stmt.setInt(++counter, idVisitCnt.intValue());
										stmt.setInt(++counter, idVisitCnt.intValue());
									}
								}
								stmt.setInt(++counter, calendarMap.get(weekCalendarId));
								stmt.setInt(++counter, Integer.parseInt(key[0]));
								stmt.setInt(++counter, Integer.parseInt(key[1]));
								stmt.setInt(++counter, ctdType);
								
								if(updateRolledupData){
									if(updateRevenue && updateVisitCount){
										if(totRevenue > 0 && visitCnt > 0 && idTotRevenue > 0 && idVisitCnt > 0){
											stmt.addBatch();
											itemNoInBatch++;
										}
									}else{
										if(visitCnt > 0 && idVisitCnt > 0){
											stmt.addBatch();
											itemNoInBatch++;
										}
									}
								}else{
									if(updateRevenue && updateVisitCount){
										if(totRevenue > 0 && visitCnt > 0){
											stmt.addBatch();
											itemNoInBatch++;
										}
									}else{
										if(visitCnt > 0){
											stmt.addBatch();
											itemNoInBatch++;
										}
									}
								}
								if(updateRolledupData)
									aggrFinanceDeptMap.put(inEntry.getKey(), new Double[]{totRevenue, visitCnt, idTotRevenue, idVisitCnt});
								else
									aggrFinanceDeptMap.put(inEntry.getKey(), new Double[]{totRevenue, visitCnt});
							}else{
								
								if(updateRevenue && updateVisitCount){
									stmt.setDouble(++counter, new Double(decimalFormat.format(value[0])));
									stmt.setInt(++counter, value[1].intValue());
									stmt.setDouble(++counter, value[0]);
									stmt.setInt(++counter, value[1].intValue());
								}else{
									stmt.setInt(++counter, value[1].intValue());
									stmt.setInt(++counter, value[1].intValue());
								}
								
								if(updateRolledupData){
									if(updateRevenue && updateVisitCount){
										stmt.setDouble(++counter, new Double(decimalFormat.format(value[2])));
										stmt.setInt(++counter, value[3].intValue());
										stmt.setDouble(++counter, value[2]);
										stmt.setInt(++counter, value[3].intValue());
									}else{
										stmt.setInt(++counter, value[3].intValue());
										stmt.setInt(++counter, value[3].intValue());
									}
								}
								stmt.setInt(++counter, calendarMap.get(weekCalendarId));
								stmt.setInt(++counter, Integer.parseInt(key[0]));
								stmt.setInt(++counter, Integer.parseInt(key[1]));
								stmt.setInt(++counter, ctdType);
								
								if(updateRolledupData){
									if(updateRevenue && updateVisitCount){
										if(value[0] > 0 && value[1] > 0 && value[2] > 0 && value[3] > 0){
											stmt.addBatch();
											itemNoInBatch++;
										}
									}else{
										if(value[1] > 0 && value[3] > 0){
											stmt.addBatch();
											itemNoInBatch++;
										}
									}
								}else{
									if(updateRevenue && updateVisitCount){
										if(value[0] > 0 && value[1] > 0){
											stmt.addBatch();
											itemNoInBatch++;
										}
									}else{
										if(value[1] > 0){
											stmt.addBatch();
											itemNoInBatch++;
										}
									}
								}
								
								aggrFinanceDeptMap.put(inEntry.getKey(), value);
							}
						}
						if(itemNoInBatch % Constants.BATCH_UPDATE_COUNT == 0){
			        		int[] count = stmt.executeBatch();
			        		stmt.clearBatch();
			        		itemNoInBatch = 0;
			        	}
					}
				}
				
				updateSalesAggrForStore(conn, entry.getKey(), aggrFinanceDeptMap, updateRolledupData, updateRevenue, updateVisitCount);
			}
			
			if(itemNoInBatch > 0){
        		int[] count = stmt.executeBatch();
        		stmt.clearBatch();
        		itemNoInBatch = 0;
        	}
	    }catch (SQLException e){
			logger.error("Error while executing UPDATE_CTD_DATA_FOR_STORE" + e);
			throw new GeneralException("Error while executing UPDATE_CTD_DATA_FOR_STORE", e);
		}finally{
			PristineDBUtil.close(stmt);
		}
	}
	
	private void updateSalesAggrForStore(Connection conn, Integer calendarId, HashMap<String, Double[]> aggrFinanceDeptMap, boolean updateRolledupData, boolean updateRevenue, boolean updateVisitCount) throws GeneralException{
		logger.info("Inside updateSalesAggr() of RevenueCorrectionDAO");
		DecimalFormat decimalFormat = new DecimalFormat("############.##");
		PreparedStatement stmt = null;
		int itemNoInBatch = 0;
		try{
			if(!updateRolledupData){
				if(updateRevenue && updateVisitCount)
					stmt = conn.prepareStatement(UPDATE_SALES_AGGR_FOR_STORE);
				else
					stmt = conn.prepareStatement(UPDATE_SALES_AGGR_FOR_STORE_VISIT_COUNT);
			}else{
				if(updateRevenue && updateVisitCount)
					stmt = conn.prepareStatement(UPDATE_SALES_AGGR_ROLLUP_FOR_STORE);
				else
					stmt = conn.prepareStatement(UPDATE_SALES_AGGR_ROLLUP_FOR_STORE_VISIT_COUNT);
			}
			
			for(Map.Entry<String, Double[]> entry : aggrFinanceDeptMap.entrySet()){
				String[] key = entry.getKey().split(Constants.INDEX_DELIMITER);
				Double[] value = entry.getValue();
				int counter = 0;
				
				if(updateRevenue && updateVisitCount){
					stmt.setDouble(++counter, new Double(decimalFormat.format(value[0])));
					stmt.setInt(++counter, value[1].intValue());
					stmt.setDouble(++counter, value[0]);
					stmt.setInt(++counter, value[1].intValue());
				}else{
					stmt.setInt(++counter, value[1].intValue());
					stmt.setInt(++counter, value[1].intValue());
				}
				
				if(updateRolledupData){
					if(updateRevenue && updateVisitCount){
						stmt.setDouble(++counter, new Double(decimalFormat.format(value[2])));
						stmt.setInt(++counter, value[3].intValue());
						stmt.setDouble(++counter, value[2]);
						stmt.setInt(++counter, value[3].intValue());
					}else{
						stmt.setInt(++counter, value[3].intValue());
						stmt.setInt(++counter, value[3].intValue());
					}
				}
				stmt.setInt(++counter, calendarId);
				stmt.setInt(++counter, Integer.parseInt(key[0]));
				stmt.setInt(++counter, Integer.parseInt(key[1]));
				
				if(updateRolledupData){
					if(updateRevenue && updateVisitCount){
						if(value[0] > 0 && value[1] > 0 && value[2] > 0 && value[3] > 0)
							stmt.addBatch();
					}else{
						if(value[1] > 0 && value[3] > 0)
							stmt.addBatch();
					}
				}else{
					if(updateRevenue && updateVisitCount){
						if(value[0] > 0 && value[1] > 0)
							stmt.addBatch();
					}else{
						if(value[1] > 0)
							stmt.addBatch();
					}
				}
				
				if(itemNoInBatch % Constants.BATCH_UPDATE_COUNT == 0){
	        		int[] count = stmt.executeBatch();
	        		stmt.clearBatch();
	        		itemNoInBatch = 0;
	        	}
			}
			if(itemNoInBatch > 0){
        		int[] count = stmt.executeBatch();
        		stmt.clearBatch();
        		itemNoInBatch = 0;
        	}
		}catch (SQLException e){
			logger.error("Error while executing UPDATE_SALES_AGGR_FOR_STORE" + e);
			throw new GeneralException("Error while executing UPDATE_SALES_AGGR_FOR_STORE", e);
		}finally{
			PristineDBUtil.close(stmt);
		}
	}

	/**
	 * Returns a map containing district id/ region id/ division id as key and set of stores under each level
	 * as values.
	 * @param conn			Connection
	 * @param tableName		RETAIL_DISTRICT/RETAIL_REGION/RETAIL_DIVISION
	 * @param columnName	DISTRICT_ID, REGION_ID, DIVISION_ID
	 * @return	Map
	 * @throws GeneralException
	 */
	public HashMap<Integer, HashSet<String>> retrieveRollupLevelId(Connection conn, String tableName, String columnName, String baseColumnName, 
			String processDate, boolean retrieveIdenticalStores) throws GeneralException{
		logger.debug("Inside retrieveRollupLevelId() of RevenueCorrectionDAO");
		PreparedStatement stmt = null;
		ResultSet rs = null;
		String sql = GET_ROLLUP_LEVELIDS;
		sql = sql.replaceAll("%TABLENAME%", tableName).replaceAll("%COLUMNNAME%", columnName).replaceAll("%BASECOLUMN%", baseColumnName);
		if(retrieveIdenticalStores)
			sql = sql + GET_ROLLUP_LEVELIDS_CONDITION;
		
		HashMap<Integer, HashSet<String>> rollupLevelIdMap = new HashMap<Integer, HashSet<String>>();
		try{
			stmt = conn.prepareStatement(sql);
			if(retrieveIdenticalStores)
				stmt.setString(1, processDate);
			
			rs = stmt.executeQuery();
			while(rs.next()){
				int levelId = rs.getInt(columnName);
				if(rollupLevelIdMap.get(levelId) != null){
					HashSet<String> tempSet = rollupLevelIdMap.get(levelId);
					tempSet.add(rs.getString(baseColumnName));
					rollupLevelIdMap.put(levelId, tempSet);
				}else{
					HashSet<String> tempSet = new HashSet<String>();
					tempSet.add(rs.getString(baseColumnName));
					rollupLevelIdMap.put(levelId, tempSet);
				}
			}
		}catch (SQLException e){
			logger.error("Error while executing GET_ROLLUP_LEVELIDS" + e);
			throw new GeneralException("Error while executing GET_ROLLUP_LEVELIDS", e);
		}finally{
			PristineDBUtil.close(rs, stmt);
		}
		return rollupLevelIdMap;
	}

	/**
	 * Rollsup Finance Department Data at district level
	 * @param revenueMap		Store level Revenue Map
	 * @param districtIdMap		District Id to Store Number Map
	 * @return
	 */
	public HashMap<Integer, Double[]> rollupStoreDataForFD(HashMap<String, Double[]> revenueMap, HashMap<Integer, HashSet<String>> districtIdMap) {
		logger.debug("Inside rollupStoreDataForFD() of RevenueCorrectionDAO");
		HashMap<Integer, Double[]> districtFDRevenueMap = new HashMap<Integer, Double[]>();
		for(Map.Entry<Integer, HashSet<String>> entry : districtIdMap.entrySet()){
			logger.debug("Rolling up FD data for district " + entry.getKey());
			boolean isDataPresent = false;
			double[] districtRevenueArr = new double[15];
			for(String compStrNo : entry.getValue()){
				Double[] storeRevenueArr = revenueMap.get(compStrNo);
				if(storeRevenueArr != null){
					isDataPresent = true;
					for(int i = 0; i < 15; i++){
						districtRevenueArr[i] = districtRevenueArr[i] + storeRevenueArr[i];
					}
				}
			}
			if(isDataPresent)
				districtFDRevenueMap.put(entry.getKey(), ArrayUtils.toObject(districtRevenueArr));
		}
		return districtFDRevenueMap;
	}

	/**
	 * Rollsup Store Data at district level
	 * @param revenueMap		Store level Revenue Map
	 * @param districtIdMap		District Id to Store Number Map
	 * @return
	 */
	public HashMap<Integer, Double[]> rollupStoreData(HashMap<String, Double[]> storeDataMap, HashMap<Integer, HashSet<String>> districtIdMap) {
		logger.debug("Inside rollupStoreData() of RevenueCorrectionDAO");
		HashMap<Integer, Double[]> districtRevenueMap = new HashMap<Integer, Double[]>();
		for(Map.Entry<Integer, HashSet<String>> entry : districtIdMap.entrySet()){
			logger.debug("Rolling up FD data for district " + entry.getKey());
			boolean isDataPresent = false;
			double[] districtRevenueArr = new double[2];
			for(String compStrNo : entry.getValue()){
				Double[] storeRevenueArr = storeDataMap.get(compStrNo);
				if(storeRevenueArr != null){
					isDataPresent = true;
					for(int i = 0; i < 2; i++){
						districtRevenueArr[i] = districtRevenueArr[i] + storeRevenueArr[i];
					}
				}
			}
			if(isDataPresent)
				districtRevenueMap.put(entry.getKey(), ArrayUtils.toObject(districtRevenueArr));
		}
		return districtRevenueMap;
	}


	/**
	 * Updates weekly revenue for finance department
	 * @param conn					Connection	
	 * @param calendarId			Calendar Id
	 * @param revenueMap			Map containing revenue data
	 * @param productIdMap			Product Id to name map
	 * @param locationLevelId		District/Region/Division
	 * @param identicalStores		Identical Stores only needs to be processed
	 * @throws GeneralException
	 */
	public void updateRolledupWeeklyRevenue(Connection conn, int calendarId, HashMap<Integer, Double[]> revenueMap, HashMap<String, Integer> productIdMap, Integer locationLevelId, boolean identicalStores) 
											throws GeneralException{
		logger.debug("Inside updateRolledupWeeklyRevenue() of RevenueCorrectionDAO");
		PreparedStatement statement = null;
		DecimalFormat decimalFormat = new DecimalFormat("############.##");
		try{
			String sql = UPDATE_SALES_AGGR_WEEKLY_ROLLUP;
			if(!identicalStores){
				sql = sql.replaceAll("%REVENUECOLUMN%", Constants.REVENUE_COLUMN).replaceAll("%AVGORDERSIZECOLUMN%", Constants.AVGORDERSIZE_COLUMN).replaceAll("%TOTVISITCNTCOLUMN%", Constants.TOTVISITCNT_COLUMN);
			}else{
				sql = sql.replaceAll("%REVENUECOLUMN%", Constants.ID_REVENUE_COLUMN).replaceAll("%AVGORDERSIZECOLUMN%", Constants.ID_AVGORDERSIZE_COLUMN).replaceAll("%TOTVISITCNTCOLUMN%", Constants.ID_TOTVISITCNT_COLUMN);
			}
			statement = conn.prepareStatement(sql);
	        
			int itemNoInBatch = 0;
			
			for(Map.Entry<Integer, Double[]> entry : revenueMap.entrySet()){
				Double[] revenueArr = entry.getValue();
				
				setValues(statement, entry.getKey(), locationLevelId, productIdMap.get("GROCERY"), Double.valueOf(decimalFormat.format(revenueArr[0])), calendarId, ++itemNoInBatch);
				setValues(statement, entry.getKey(), locationLevelId, productIdMap.get("DAIRY"), Double.valueOf(decimalFormat.format(revenueArr[1])), calendarId, ++itemNoInBatch);
				setValues(statement, entry.getKey(), locationLevelId, productIdMap.get("FROZEN FOOD"), Double.valueOf(decimalFormat.format(revenueArr[2])), calendarId, ++itemNoInBatch);
				setValues(statement, entry.getKey(), locationLevelId, productIdMap.get("GENERAL MERCH"), Double.valueOf(decimalFormat.format(revenueArr[3])), calendarId, ++itemNoInBatch);
				setValues(statement, entry.getKey(), locationLevelId, productIdMap.get("HBC"), Double.valueOf(decimalFormat.format(revenueArr[4])), calendarId, ++itemNoInBatch);
				setValues(statement, entry.getKey(), locationLevelId, productIdMap.get("PHARMACY"), Double.valueOf(decimalFormat.format(revenueArr[5])), calendarId, ++itemNoInBatch);
				setValues(statement, entry.getKey(), locationLevelId, productIdMap.get("PRODUCE"), Double.valueOf(decimalFormat.format(revenueArr[6])), calendarId, ++itemNoInBatch);
				setValues(statement, entry.getKey(), locationLevelId, productIdMap.get("FLORAL"), Double.valueOf(decimalFormat.format(revenueArr[7])), calendarId, ++itemNoInBatch);
				setValues(statement, entry.getKey(), locationLevelId, productIdMap.get("MEAT"), Double.valueOf(decimalFormat.format(revenueArr[8])), calendarId, ++itemNoInBatch);
				setValues(statement, entry.getKey(), locationLevelId, productIdMap.get("SEAFOOD"), Double.valueOf(decimalFormat.format(revenueArr[9])), calendarId, ++itemNoInBatch);
				setValues(statement, entry.getKey(), locationLevelId, productIdMap.get("DELI"), Double.valueOf(decimalFormat.format(revenueArr[10])), calendarId, ++itemNoInBatch);
				setValues(statement, entry.getKey(), locationLevelId, productIdMap.get("CARRY OUT CAFE"), Double.valueOf(decimalFormat.format(revenueArr[11])), calendarId, ++itemNoInBatch);
				setValues(statement, entry.getKey(), locationLevelId, productIdMap.get("BAKERY"), Double.valueOf(decimalFormat.format(revenueArr[12])), calendarId, ++itemNoInBatch);
				setValues(statement, entry.getKey(), locationLevelId, productIdMap.get("PROMOTION"), Double.valueOf(decimalFormat.format(revenueArr[13])), calendarId, ++itemNoInBatch);
				setValues(statement, entry.getKey(), locationLevelId, productIdMap.get("GAS"), Double.valueOf(decimalFormat.format(revenueArr[14])), calendarId, ++itemNoInBatch);
			}
			
	        if(itemNoInBatch > 0){
	        	int[] count = statement.executeBatch();
        		statement.clearBatch();
	        }
	    }
		catch (SQLException e)
		{
			e.printStackTrace();
			logger.error("Error while executing UPDATE_SALES_AGGR_WEEKLY_ROLLUP" + e);
			throw new GeneralException("Error while executing UPDATE_SALES_AGGR_WEEKLY_ROLLUP", e);
		}finally{
			PristineDBUtil.close(statement);
		}		
	}
	
	private void setValues(PreparedStatement statement, Integer locationId, Integer locationLevelId,
			Integer productId, Double revenue, int calendarId, int itemNoInBatch) throws SQLException{
		int counter = 0;
		statement.setDouble(++counter, revenue);
		statement.setDouble(++counter, revenue);
		statement.setInt(++counter, calendarId);
		statement.setInt(++counter, locationLevelId);
    	statement.setInt(++counter, locationId);
    	statement.setInt(++counter, productId);
    	
    	statement.addBatch();
    	
    	if(itemNoInBatch % Constants.BATCH_UPDATE_COUNT == 0){
    		int[] count = statement.executeBatch();
    		statement.clearBatch();
    		itemNoInBatch = 0;
    	}
	}
	
	/**
	 * Updates rolled up visit count value
	 * @param conn					Connection
	 * @param calendarId			Calendar Id
	 * @param storeRevenueMap		Map containing revenue data
	 * @param locationLevelId		District/Region/Division
	 * @param identicalStores		Identical Stores only needs to be processed
	 * @throws GeneralException
	 */
	public void updateWeeklyRollupVisitCount(Connection conn, int calendarId, HashMap<Integer, Double[]> storeRevenueMap, int locationLevelId, boolean identicalStores, boolean updateRevenue, boolean updateVisitCount) throws GeneralException{
		logger.debug("Inside updateWeeklyRevenue() of RevenueCorrectionDAO");
		PreparedStatement statement = null;
		DecimalFormat decimalFormat = new DecimalFormat("############.##");
		try{
			String sql = UPDATE_SALES_AGGR_WEEKLY_ROLLUP_STORE_DATA_VISIT_COUNT;
			
			if(updateRevenue && updateVisitCount)
				sql = UPDATE_SALES_AGGR_WEEKLY_ROLLUP_STORE_DATA;
			else if(updateVisitCount)
				sql = UPDATE_SALES_AGGR_WEEKLY_ROLLUP_STORE_DATA_VISIT_COUNT;
			
			if(!identicalStores){
				sql = sql.replaceAll("%REVENUECOLUMN%", Constants.REVENUE_COLUMN).replaceAll("%AVGORDERSIZECOLUMN%", Constants.AVGORDERSIZE_COLUMN).replaceAll("%TOTVISITCNTCOLUMN%", Constants.TOTVISITCNT_COLUMN);
			}else{
				sql = sql.replaceAll("%REVENUECOLUMN%", Constants.ID_REVENUE_COLUMN).replaceAll("%AVGORDERSIZECOLUMN%", Constants.ID_AVGORDERSIZE_COLUMN).replaceAll("%TOTVISITCNTCOLUMN%", Constants.ID_TOTVISITCNT_COLUMN);
			}
			
			statement = conn.prepareStatement(sql);
	        
			int itemNoInBatch = 0;
			
			for(Map.Entry<Integer, Double[]> entry : storeRevenueMap.entrySet()){
				Double[] revenueArr = entry.getValue();
				int counter = 0;
				if(updateRevenue && updateVisitCount){
					statement.setDouble(++counter, new Double(decimalFormat.format(revenueArr[0])));
					statement.setInt(++counter, revenueArr[1].intValue());
					statement.setDouble(++counter, revenueArr[0]);
					statement.setDouble(++counter, revenueArr[1]);
					statement.setInt(++counter, calendarId);
					statement.setInt(++counter, locationLevelId);
			        statement.setInt(++counter, entry.getKey());
			        if(revenueArr[0] > 0 || revenueArr[1] > 0){
				        statement.addBatch();
				        itemNoInBatch++;
			        }
				}else{
					statement.setInt(++counter, revenueArr[1].intValue());
					statement.setInt(++counter, revenueArr[1].intValue());
					statement.setInt(++counter, calendarId);
					statement.setInt(++counter, locationLevelId);
			        statement.setInt(++counter, entry.getKey());
			        if(revenueArr[1] > 0){
				        statement.addBatch();
				        itemNoInBatch++;
			        }
				}
	        	
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
		catch (SQLException e)
		{
			e.printStackTrace();
			logger.error("Error while executing UPDATE_SALES_AGGR_WEEKLY_ROLLUP_VISTICNT" + e);
			throw new GeneralException("Error while executing UPDATE_SALES_AGGR_WEEKLY_ROLLUP_VISTICNT", e);
		}finally{
			PristineDBUtil.close(statement);
		}
	}
	
	/**
	 * Rolls up finance dept data to chain level
	 * @param revenueMap	Division level revenue map
	 * @return
	 */
	public HashMap<Integer, Double[]> rollUpChainLevelDataForFD(HashMap<Integer, Double[]> revenueMap) {
		logger.debug("Inside rollUpChainLevelDataForFD() of RevenueCorrectionDAO");
		HashMap<Integer, Double[]> fdRevenueMap = new HashMap<Integer, Double[]>();
		double[] districtRevenueArr = new double[15];
		for(Map.Entry<Integer, Double[]> entry : revenueMap.entrySet()){
			Double[] storeRevenueArr = entry.getValue();
			for(int i = 0; i < 15; i++){
				districtRevenueArr[i] = districtRevenueArr[i] + storeRevenueArr[i];
			}
		}
		fdRevenueMap.put(50, ArrayUtils.toObject(districtRevenueArr));
		return fdRevenueMap;
	}
	
	/**
	 * Rollsup Visit Count Data to chain level
	 * @param revenueMap		Revenue Map
	 * @return
	 */
	public HashMap<Integer, Double[]> rollupChainLevelData(HashMap<Integer, Double[]> revenueMap) {
		logger.debug("Inside rollupChainLevelData() of RevenueCorrectionDAO");
		HashMap<Integer, Double[]> districtFDRevenueMap = new HashMap<Integer, Double[]>();
		double[] districtRevenueArr = new double[2];
		for(Map.Entry<Integer, Double[]> entry : revenueMap.entrySet()){
			Double[] storeRevenueArr = entry.getValue();
			for(int i = 0; i < 2; i++){
				districtRevenueArr[i] = districtRevenueArr[i] + storeRevenueArr[i];
			}
		}
		districtFDRevenueMap.put(50, ArrayUtils.toObject(districtRevenueArr));
		return districtFDRevenueMap;
	}
	
	/**
	 * Updates daily revenue at rolledup levels
	 * @param conn				Connection
	 * @param revenueMap		Map containing level id as key and revenue as value
	 * @param locationLevelId	Location level id
	 * @param calendarId		Calendar id
	 * @throws GeneralException
	 */
	public void updateDailyRollupRevenue(Connection conn, HashMap<Integer, Double> revenueMap, int locationLevelId, int calendarId, boolean identicalStores) throws GeneralException{
		logger.debug("Inside updateDailyRollupRevenue() of RevenueCorrectionDAO");
		
		PreparedStatement statement = null;
		try{
			String sql = UPDATE_SALES_AGGR_DAILY_ROLLUP;
			
			if(!identicalStores){
				sql = sql.replaceAll("%REVENUECOLUMN%", Constants.REVENUE_COLUMN).replaceAll("%AVGORDERSIZECOLUMN%", Constants.AVGORDERSIZE_COLUMN).replaceAll("%TOTVISITCNTCOLUMN%", Constants.TOTVISITCNT_COLUMN);
			}else{
				sql = sql.replaceAll("%REVENUECOLUMN%", Constants.ID_REVENUE_COLUMN).replaceAll("%AVGORDERSIZECOLUMN%", Constants.ID_AVGORDERSIZE_COLUMN).replaceAll("%TOTVISITCNTCOLUMN%", Constants.ID_TOTVISITCNT_COLUMN);
			}
			
			statement = conn.prepareStatement(sql);
	        
			int itemNoInBatch = 0;
			
			for(Map.Entry<Integer, Double> entry : revenueMap.entrySet()){
				int counter = 0;
				statement.setDouble(++counter, entry.getValue());
				statement.setDouble(++counter, entry.getValue());
				statement.setInt(++counter, calendarId);
				statement.setInt(++counter, locationLevelId);
				statement.setInt(++counter, entry.getKey());
				
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
		catch (SQLException e)
		{
			e.printStackTrace();
			logger.error("Error while executing UPDATE_SALES_AGGR_DAILY_ROLLUP" + e);
			throw new GeneralException("Error while executing UPDATE_SALES_AGGR_DAILY_ROLLUP", e);
		}finally{
			PristineDBUtil.close(statement);
		}
	}
}
