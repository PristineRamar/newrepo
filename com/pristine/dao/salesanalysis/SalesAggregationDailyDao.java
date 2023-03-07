package com.pristine.dao.salesanalysis;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Date;
import java.util.HashMap;
import java.util.List;

import javax.sql.rowset.CachedRowSet;

import org.apache.log4j.Logger;

import com.pristine.dto.RetailCalendarDTO;
import com.pristine.dto.SummaryDailyDTO;
import com.pristine.dto.salesanalysis.SummaryDataDTO;
import com.pristine.exception.GeneralException;
import com.pristine.util.Constants;
import com.pristine.util.GenericUtil;
import com.pristine.util.PristineDBUtil;

public class SalesAggregationDailyDao {
	
	static Logger logger = Logger.getLogger("SalesAggregationDailyDao");

	/*
	 * ****************************************************************
	 * Method used to delete the previous aggregation for store
	 * Argument 1 : _Conn
	 * Argument 2 : calendarId
	 * Argument 3 : locationId
	 * @throws GeneralException , SQLException
	 * ****************************************************************
	 */
	
	public void deletePreviousAggregation(Connection _Conn, int calendarId, 
														int locationId) throws GeneralException {
		
		//logger.debug("Delete Previous Aggregation Starts");
		
		try {
		
			//StringBuffer CTDsql = new StringBuffer();
			StringBuffer dailysql = new StringBuffer();
			  
			//SQL to delete CTD data
			//CTDsql.append(" Delete from SALES_AGGR_CTD WHERE SUMMARY_CTD_ID IN (");
			//CTDsql.append(" SELECT SUMMARY_CTD_ID from SALES_AGGR_DAILY WHERE CALENDAR_ID = '" + calendarId +"'");
			//CTDsql.append(" and LOCATION_ID = '" + locationId + "')");
		 
			//SQL to delete Sales Aggr Daily data
			dailysql.append(" Delete from SALES_AGGR_DAILY WHERE CALENDAR_ID = '" + calendarId +"'");
		    dailysql.append(" and LOCATION_ID = '" + locationId + "'");
		 
		    //logger.debug("deletePreviousAggregation CTD SQL:" +CTDsql.toString());
		    logger.debug("deletePreviousAggregation SQL:" +dailysql.toString());

			
			// execute the Sales Aggr CTD data delete query
			//PristineDBUtil.executeUpdate(_Conn, CTDsql , "deletePreviousAggregation");
		    
		    // execute the Sales Aggr Daily data delete query
			PristineDBUtil.executeUpdate(_Conn, dailysql , 
												"deletePreviousAggregation");
			
			PristineDBUtil.commitTransaction(_Conn,	
										"Commit Aggregation delete process");

		} catch (GeneralException e) {
			 logger.error("Error while deleting previous Aggregation data" + e);
			throw new GeneralException("deletePreviousAggregation", e);		
		}
	}		 

	/*
	 * ****************************************************************
	 * Method to  insert the Store level aggregation records
	 * Argument 1 : Product HashMap
	 * Argument 2 : Connection
	 * Argument 3 : calendarId
	 * Arguemnt 4 : storeId
	 * call the execute batch method
	 * ****************************************************************
	 */
	public void insertSummaryDailyBatch(Connection _Conn, int calendarId,
			int storeId,
			HashMap<Integer, HashMap<String, SummaryDataDTO>> productMap,
			HashMap<String, Long> lastSummaryList, String store_Status) throws GeneralException {
			
		int storeLevelId = Constants.STORE_LEVEL_ID;
		
		try
		  {
	        PreparedStatement psmt=_Conn.prepareStatement(InsertSql("DAILY"));
	                
	        //logger.info(" Product HashMap Count  " + productMap.size());
	                        
	        Object[] outerLoop=productMap.values().toArray();
				for (int ii = 0; ii < outerLoop.length; ii++) {
					@SuppressWarnings("unchecked")
					HashMap<String, SummaryDailyDTO> subProductMap = (HashMap<String, SummaryDailyDTO>) outerLoop[ii];
	
					Object[] innerLoop = subProductMap.values().toArray();
	
					for (int jj = 0; jj < innerLoop.length; jj++) {
										
						SummaryDataDTO summaryDto = (SummaryDataDTO) innerLoop[jj];
						
						summaryDto.setLocationId(storeId);
						summaryDto.setcalendarId(calendarId);

						if (summaryDto.getProductLevelId() == 0){
							if (lastSummaryList
									.containsKey(Constants.STORE_LEVEL_ID+"_"+summaryDto.getProductLevelId()+"_"+null )) {
								
								summaryDto.setlastAggrSalesId(lastSummaryList.get(Constants.STORE_LEVEL_ID + "_"
												+ summaryDto.getProductLevelId() + "_"
												+ null));
							} 
						}
						else
						{
							if (lastSummaryList
									.containsKey(Constants.STORE_LEVEL_ID+"_"+summaryDto.getProductLevelId()+"_"+summaryDto.getProductId() )) {
								
								summaryDto.setlastAggrSalesId(lastSummaryList.get(Constants.STORE_LEVEL_ID + "_"
												+ summaryDto.getProductLevelId() + "_"
												+ summaryDto.getProductId()));
							} 
						}
					
						addSqlBatch(summaryDto,psmt ,storeLevelId , store_Status);
						
					}
				}
	
				logger.debug("Execue Insert");
				
				int[] count = psmt.executeBatch();
				logger.debug(" Insert record count:" + count.length);
				
				psmt.close();
				
				
			 
			}
		catch(Exception exe)
		{
			logger.error("Summary Daily Insert Error" , exe);
			throw new GeneralException("Summary Daily Insert Error" , exe);
		}
	}
	

	/*
	 * ****************************************************************
	 * Method to  insert the Store level aggregation records
	 * Argument 1 : Calendar Id to insert
	 * Argument 2 : Store ID to process
	 * Argument 3 : Product Group Type to decide product level insert
	 * Argument 4 : Aggregated product data
	 * Argument 5 : Last Year summary ID
	 * Argument 6 : Identical store status
	 * call the execute batch method
	 * ****************************************************************
	 */
	public void insertSummaryDailyBatchV2(Connection _Conn, int calendarId,
			int storeId, HashMap<Integer, Integer> _productGroupType, 
			HashMap<Integer, HashMap<String, SummaryDataDTO>> productMap,
			HashMap<String, Long> lastSummaryList, String store_Status) throws GeneralException {
			
		int storeLevelId = Constants.STORE_LEVEL_ID;
		
		try
		  {
	        PreparedStatement psmt=_Conn.prepareStatement(InsertSql("DAILY"));
	                
	        Object[] outerLoop=productMap.values().toArray();
				for (int ii = 0; ii < outerLoop.length; ii++) {
					@SuppressWarnings("unchecked")
					HashMap<String, SummaryDailyDTO> subProductMap = (HashMap<String, SummaryDailyDTO>) outerLoop[ii];
					
					Object[] innerLoop = subProductMap.values().toArray();
					
					for (int jj = 0; jj < innerLoop.length; jj++) {
						
						SummaryDataDTO summaryDto = (SummaryDataDTO) innerLoop[jj];

						if (summaryDto.getProductLevelId() > 0 && _productGroupType.get(summaryDto.getProductLevelId()) == 0){
							continue;
						}						
						
						summaryDto.setLocationId(storeId);
						summaryDto.setcalendarId(calendarId);

						if (summaryDto.getProductLevelId() == 0){
							if (lastSummaryList
									.containsKey(Constants.STORE_LEVEL_ID+"_"+summaryDto.getProductLevelId()+"_"+null )) {
								
								summaryDto.setlastAggrSalesId(lastSummaryList.get(Constants.STORE_LEVEL_ID + "_"
												+ summaryDto.getProductLevelId() + "_"
												+ null));
							} 
						}
						else
						{
							if (lastSummaryList
									.containsKey(Constants.STORE_LEVEL_ID+"_"+summaryDto.getProductLevelId()+"_"+summaryDto.getProductId() )) {
								
								summaryDto.setlastAggrSalesId(lastSummaryList.get(Constants.STORE_LEVEL_ID + "_"
												+ summaryDto.getProductLevelId() + "_"
												+ summaryDto.getProductId()));
							} 
						}
					
						addSqlBatch(summaryDto,psmt ,storeLevelId , store_Status);
						
					}
				}
	
				logger.debug("Execue Insert");
				
				int[] count = psmt.executeBatch();
				logger.debug(" Insert record count:" + count.length);
				
				psmt.close();
				
				
			 
			}
		catch(Exception exe)
		{
			logger.error("Summary Daily Insert Error" , exe);
			throw new GeneralException("Summary Daily Insert Error" , exe);
		}
	}
		
	/*
	 * ****************************************************************
	 * Method to  update the Store level aggregation records for PL
	 * Argument 1 : Calendar Id for update
	 * Argument 2 : Store ID to process
	 * Argument 3 : Product Group Type to decide product level update
	 * Argument 4 : Aggregated product data
	 * Argument 5 : Last Year summary ID
	 * Argument 6 : Identical store status
	 * call the execute batch method
	 * ****************************************************************
	 */
	public void updateSummaryDailyBatchV2(Connection _Conn, int calendarId,
			int storeId, HashMap<Integer, Integer> _productGroupType, 
			HashMap<Integer, HashMap<String, SummaryDataDTO>> productMap,
			HashMap<String, Long> lastSummaryList, String store_Status) throws GeneralException {
			
		int storeLevelId = Constants.STORE_LEVEL_ID;
		
		try
		  {
	        PreparedStatement psmt=_Conn.prepareStatement(updateSQL());
	                
	        Object[] outerLoop=productMap.values().toArray();
				for (int ii = 0; ii < outerLoop.length; ii++) {
					@SuppressWarnings("unchecked")
					HashMap<String, SummaryDailyDTO> subProductMap = (HashMap<String, SummaryDailyDTO>) outerLoop[ii];
					
					Object[] innerLoop = subProductMap.values().toArray();
					
					for (int jj = 0; jj < innerLoop.length; jj++) {
						
						SummaryDataDTO summaryDto = (SummaryDataDTO) innerLoop[jj];

						if (summaryDto.getProductLevelId() > 0 && _productGroupType.get(summaryDto.getProductLevelId()) == 0){
							continue;
						}						
						
						summaryDto.setLocationId(storeId);
						summaryDto.setcalendarId(calendarId);

						if (summaryDto.getProductLevelId() == 0){
							if (lastSummaryList
									.containsKey(Constants.STORE_LEVEL_ID+"_"+summaryDto.getProductLevelId()+"_"+null )) {
								
								summaryDto.setlastAggrSalesId(lastSummaryList.get(Constants.STORE_LEVEL_ID + "_"
												+ summaryDto.getProductLevelId() + "_"
												+ null));
							} 
						}
						else
						{
							if (lastSummaryList
									.containsKey(Constants.STORE_LEVEL_ID+"_"+summaryDto.getProductLevelId()+"_"+summaryDto.getProductId() )) {
								
								summaryDto.setlastAggrSalesId(lastSummaryList.get(Constants.STORE_LEVEL_ID + "_"
												+ summaryDto.getProductLevelId() + "_"
												+ summaryDto.getProductId()));
							} 
						}
					
						updateSqlBatch(summaryDto,psmt ,storeLevelId , store_Status);
					}
				}
	
				logger.debug("Execue Update");
				
				int[] count = psmt.executeBatch();
				logger.debug(" Update record count:" + count.length);
				
				psmt.close();
			}
		catch(Exception exe)
		{
			logger.error("Summary Daily Update Error" , exe);
			throw new GeneralException("Summary Daily Insert Error" , exe);
		}
	}

	
	

	
	
	/*
		 * ****************************************************************
		 * Method to Create the insert script and add the script into batch List
		 * Argument 1 : SummaryDailyDto
		 * @throws GeneralException , SQLException
		 * ****************************************************************
		 */

	public void addSqlBatch(SummaryDataDTO summaryDto, PreparedStatement psmt, 
					int storeLevelId, String store_Status) throws Exception {
			try
			{
			int i=0;
			psmt.setObject(++i,summaryDto.getcalendarId());
			psmt.setObject(++i, storeLevelId);
			psmt.setObject(++i, summaryDto.getLocationId());
			
			if( summaryDto.getProductLevelId() == 0)
				psmt.setNull(++i, java.sql.Types.INTEGER);
			else
				psmt.setObject(++i, summaryDto.getProductLevelId()); 
			
			if( summaryDto.getProductId() == "" | summaryDto.getProductId() == "0")
				psmt.setNull(++i, java.sql.Types.INTEGER);
			else
				psmt.setObject(++i, summaryDto.getProductId()); 
			
			psmt.setDouble(++i, GenericUtil.Round(summaryDto.getTotalVisitCount(),2));
			psmt.setDouble(++i, GenericUtil.Round(summaryDto.getAverageOrderSize(), 2));
			psmt.setDouble(++i, GenericUtil.Round(summaryDto.getTotalMovement(),2));
			psmt.setDouble(++i, GenericUtil.Round(summaryDto.getTotalRevenue(),2));
			psmt.setDouble(++i, GenericUtil.Round(summaryDto.getRegularRevenue(),2));
			psmt.setDouble(++i, GenericUtil.Round(summaryDto.getSaleRevenue(),2));
			psmt.setDouble(++i, GenericUtil.Round(summaryDto.getTotalMargin(),2));
			psmt.setDouble(++i, GenericUtil.Round(summaryDto.getRegularMargin(),2));
			psmt.setDouble(++i, GenericUtil.Round(summaryDto.getSaleMargin(),2));
			psmt.setDouble(++i, GenericUtil.Round(summaryDto.getTotalMarginPer(),2));
			psmt.setDouble(++i, GenericUtil.Round(summaryDto.getRegularMarginPer(),2));
			psmt.setDouble(++i, GenericUtil.Round(summaryDto.getSaleMarginPer(),2));
			psmt.setDouble(++i, GenericUtil.Round(summaryDto.getRegularMovement(),2));
			psmt.setDouble(++i, GenericUtil.Round(summaryDto.getSaleMovement(),2));
			
			if( summaryDto.getlastAggrSalesId() == 0)
				psmt.setNull(++i, java.sql.Types.INTEGER);
			else
				psmt.setObject(++i, summaryDto.getlastAggrSalesId()); 
			
			psmt.setDouble(++i, GenericUtil.Round(summaryDto.getregMovementVolume(),2));
			psmt.setDouble(++i, GenericUtil.Round(summaryDto.getsaleMovementVolume(),2));
			psmt.setDouble(++i, GenericUtil.Round(summaryDto.gettotMovementVolume(),2));
			psmt.setDouble(++i, GenericUtil.Round(summaryDto.getigRegVolumeRev(),2));
			psmt.setDouble(++i, GenericUtil.Round(summaryDto.getigSaleVolumeRev(),2));
			psmt.setDouble(++i, GenericUtil.Round(summaryDto.getigtotVolumeRev(),2));
			psmt.setObject(++i, store_Status);

			if (summaryDto.getPLTotalRevenue() != 0)
				psmt.setDouble(++i, GenericUtil.Round(summaryDto.getPLTotalRevenue(),2));
			else
				psmt.setNull(++i, java.sql.Types.INTEGER);

			if (summaryDto.getPLRegularRevenue() != 0)
				psmt.setDouble(++i, GenericUtil.Round(summaryDto.getPLRegularRevenue(),2));
			else
				psmt.setNull(++i, java.sql.Types.INTEGER);
			
			if (summaryDto.getPLSaleRevenue() != 0)
				psmt.setDouble(++i, GenericUtil.Round(summaryDto.getPLSaleRevenue(),2));
			else
				psmt.setNull(++i, java.sql.Types.INTEGER);

			if (summaryDto.getPLTotalMargin() != 0)
				psmt.setDouble(++i, GenericUtil.Round(summaryDto.getPLTotalMargin(),2));
			else
				psmt.setNull(++i, java.sql.Types.INTEGER);

			if (summaryDto.getPLRegularMargin() != 0)
				psmt.setDouble(++i, GenericUtil.Round(summaryDto.getPLRegularMargin(),2));
			else
				psmt.setNull(++i, java.sql.Types.INTEGER);

			if (summaryDto.getPLSaleMargin() != 0)
				psmt.setDouble(++i, GenericUtil.Round(summaryDto.getPLSaleMargin(),2));
			else
				psmt.setNull(++i, java.sql.Types.INTEGER);
			
			if (summaryDto.getPLTotalMarginPer() != 0)
				psmt.setDouble(++i, GenericUtil.Round(summaryDto.getPLTotalMarginPer(),2));
			else
				psmt.setNull(++i, java.sql.Types.INTEGER);
			
			if (summaryDto.getPLRegularMarginPer() != 0)
				psmt.setDouble(++i, GenericUtil.Round(summaryDto.getPLRegularMarginPer(),2));
			else
				psmt.setNull(++i, java.sql.Types.INTEGER);
			
			if (summaryDto.getPLSaleMarginPer() != 0)
				psmt.setDouble(++i, GenericUtil.Round(summaryDto.getPLSaleMarginPer(),2));
			else
				psmt.setNull(++i, java.sql.Types.INTEGER);
			
			if (summaryDto.getPLTotalMovement() != 0)
				psmt.setDouble(++i, GenericUtil.Round(summaryDto.getPLTotalMovement(),2));
			else
				psmt.setNull(++i, java.sql.Types.INTEGER);

			if (summaryDto.getPLRegularMovement() != 0)
				psmt.setDouble(++i, GenericUtil.Round(summaryDto.getPLRegularMovement(),2));
			else
				psmt.setNull(++i, java.sql.Types.INTEGER);
			
			if (summaryDto.getPLSaleMovement() != 0)
				psmt.setDouble(++i, GenericUtil.Round(summaryDto.getPLSaleMovement(),2));
			else
				psmt.setNull(++i, java.sql.Types.INTEGER);

			if (summaryDto.getPLtotMovementVolume() != 0)
				psmt.setDouble(++i, GenericUtil.Round(summaryDto.getPLtotMovementVolume(),2));
			else
				psmt.setNull(++i, java.sql.Types.INTEGER);

			if (summaryDto.getPLregMovementVolume() != 0)
				psmt.setDouble(++i, GenericUtil.Round(summaryDto.getPLregMovementVolume(),2));
			else
				psmt.setNull(++i, java.sql.Types.INTEGER);
			
			if (summaryDto.getPLsaleMovementVolume() != 0)
				psmt.setDouble(++i, GenericUtil.Round(summaryDto.getPLsaleMovementVolume(),2));
			else
				psmt.setNull(++i, java.sql.Types.INTEGER);

			if (summaryDto.getPLTotalVisitCount() != 0)
				psmt.setDouble(++i, GenericUtil.Round(summaryDto.getPLTotalVisitCount(),2));
			else
				psmt.setNull(++i, java.sql.Types.INTEGER);
			
			if (summaryDto.getPLAverageOrderSize() != 0)
				psmt.setDouble(++i, GenericUtil.Round(summaryDto.getPLAverageOrderSize(), 2));
			else
				psmt.setNull(++i, java.sql.Types.INTEGER);
			
			psmt.addBatch();
		} catch (Exception exe) {
			logger.error("Summary Daily Insert Error" , exe);
			throw new Exception("addSqlBatch", exe);	
			
		}
	}


	/*
	 * ****************************************************************
	 * Method to Create the insert script and add the script into batch List
	 * Argument 1 : SummaryDailyDto
	 * @throws GeneralException , SQLException
	 * ****************************************************************
	 */

	public void updateSqlBatch(SummaryDataDTO summaryDto, PreparedStatement psmt, 
				int storeLevelId, String store_Status) throws Exception {
		try
		{
			int i=0;

			if (summaryDto.getPLTotalRevenue() != 0)
				psmt.setDouble(++i, GenericUtil.Round(summaryDto.getPLTotalRevenue(),2));
			else
				psmt.setNull(++i, java.sql.Types.INTEGER);

			if (summaryDto.getPLRegularRevenue() != 0)
				psmt.setDouble(++i, GenericUtil.Round(summaryDto.getPLRegularRevenue(),2));
			else
				psmt.setNull(++i, java.sql.Types.INTEGER);
			
			if (summaryDto.getPLSaleRevenue() != 0)
				psmt.setDouble(++i, GenericUtil.Round(summaryDto.getPLSaleRevenue(),2));
			else
				psmt.setNull(++i, java.sql.Types.INTEGER);
			
			if (summaryDto.getPLTotalMargin() != 0)
				psmt.setDouble(++i, GenericUtil.Round(summaryDto.getPLTotalMargin(),2));
			else
				psmt.setNull(++i, java.sql.Types.INTEGER);
			
			if (summaryDto.getPLRegularMargin() != 0)
				psmt.setDouble(++i, GenericUtil.Round(summaryDto.getPLRegularMargin(),2));
			else
				psmt.setNull(++i, java.sql.Types.INTEGER);

			if (summaryDto.getPLSaleMargin() != 0)
				psmt.setDouble(++i, GenericUtil.Round(summaryDto.getPLSaleMargin(),2));
			else
				psmt.setNull(++i, java.sql.Types.INTEGER);
			
			if (summaryDto.getPLTotalMarginPer() != 0)
				psmt.setDouble(++i, GenericUtil.Round(summaryDto.getPLTotalMarginPer(),2));
			else
				psmt.setNull(++i, java.sql.Types.INTEGER);
			
			if (summaryDto.getPLRegularMarginPer() != 0)
				psmt.setDouble(++i, GenericUtil.Round(summaryDto.getPLRegularMarginPer(),2));
			else
				psmt.setNull(++i, java.sql.Types.INTEGER);
			
			if (summaryDto.getPLSaleMarginPer() != 0)
				psmt.setDouble(++i, GenericUtil.Round(summaryDto.getPLSaleMarginPer(),2));
			else
				psmt.setNull(++i, java.sql.Types.INTEGER);
			
			if (summaryDto.getPLTotalMovement() != 0)
				psmt.setDouble(++i, GenericUtil.Round(summaryDto.getPLTotalMovement(),2));
			else
				psmt.setNull(++i, java.sql.Types.INTEGER);

			if (summaryDto.getPLRegularMovement() != 0)
				psmt.setDouble(++i, GenericUtil.Round(summaryDto.getPLRegularMovement(),2));
			else
				psmt.setNull(++i, java.sql.Types.INTEGER);
			
			if (summaryDto.getPLSaleMovement() != 0)
				psmt.setDouble(++i, GenericUtil.Round(summaryDto.getPLSaleMovement(),2));
			else
				psmt.setNull(++i, java.sql.Types.INTEGER);

			if (summaryDto.getPLtotMovementVolume() != 0)
				psmt.setDouble(++i, GenericUtil.Round(summaryDto.getPLtotMovementVolume(),2));
			else
				psmt.setNull(++i, java.sql.Types.INTEGER);

			if (summaryDto.getPLregMovementVolume() != 0)
				psmt.setDouble(++i, GenericUtil.Round(summaryDto.getPLregMovementVolume(),2));
			else
				psmt.setNull(++i, java.sql.Types.INTEGER);
			
			if (summaryDto.getPLsaleMovementVolume() != 0)
				psmt.setDouble(++i, GenericUtil.Round(summaryDto.getPLsaleMovementVolume(),2));
			else
				psmt.setNull(++i, java.sql.Types.INTEGER);

			if (summaryDto.getPLTotalVisitCount() != 0)
				psmt.setDouble(++i, GenericUtil.Round(summaryDto.getPLTotalVisitCount(),2));
			else
				psmt.setNull(++i, java.sql.Types.INTEGER);
			
			if (summaryDto.getPLAverageOrderSize() != 0)
				psmt.setDouble(++i, GenericUtil.Round(summaryDto.getPLAverageOrderSize(), 2));
			else
				psmt.setNull(++i, java.sql.Types.INTEGER);
			
			psmt.setObject(++i,summaryDto.getcalendarId());
			psmt.setObject(++i, storeLevelId);
			psmt.setObject(++i, summaryDto.getLocationId());
		
			if( summaryDto.getProductLevelId() == 0)
				psmt.setInt(++i, -1);
			else
				psmt.setObject(++i, summaryDto.getProductLevelId()); 
			
			if(summaryDto.getProductId() == "" || summaryDto.getProductId() == "0")
				psmt.setInt(++i, -1);
			else
				psmt.setObject(++i, summaryDto.getProductId()); 
			
			psmt.addBatch();
		} catch (Exception exe) {
			logger.error("Summary Daily Insert Error" , exe);
			throw new Exception("addSqlBatch", exe);	
		}
	}
	
	
	/*
	 * ****************************************************************
	 * Method returns the Summary Daily Insert Query
	 * ****************************************************************
	 */

	private String InsertSql(String methodMode) {

		StringBuffer sql = new StringBuffer();
		sql.append(" insert into SALES_AGGR_DAILY (SUMMARY_DAILY_ID, CALENDAR_ID, LOCATION_LEVEL_ID, LOCATION_ID,PRODUCT_LEVEL_ID ");
		sql.append(", PRODUCT_ID , TOT_VISIT_CNT ,AVG_ORDER_SIZE,TOT_MOVEMENT, TOT_REVENUE , REG_REVENUE  ");
		sql.append(" , SALE_REVENUE ,TOT_MARGIN ,REG_MARGIN , SALE_MARGIN  ");
		sql.append(" ,TOT_MARGIN_PCT,REG_MARGIN_PCT,SALE_MARGIN_PCT, REG_MOVEMENT, SALE_MOVEMENT, SUMMARY_CTD_ID, LST_SUMMARY_DAILY_ID ");

		// INSERT COLUMN ADDED FOR MOVEMENT BY VOLUME
		// 6 COLUMNS ADDED FOR MOVEMENT BY VOLUME PROCESS
		sql.append(" ,REG_MOVEMENT_VOL,SALE_MOVEMENT_VOL,TOT_MOVEMENT_VOL,REG_IGVOL_REVENUE,SALE_IGVOL_REVENUE,TOT_IGVOL_REVENUE,STORE_TYPE,");

		sql.append(" PL_TOT_REVENUE, PL_REG_REVENUE, PL_SALE_REVENUE, ");
		sql.append(" PL_TOT_MARGIN, PL_REG_MARGIN, PL_SALE_MARGIN, ");
		sql.append(" PL_TOT_MARGIN_PCT, PL_REG_MARGIN_PCT, PL_SALE_MARGIN_PCT, ");
		sql.append(" PL_TOT_MOVEMENT, PL_REG_MOVEMENT, PL_SALE_MOVEMENT, ");
		sql.append(" PL_TOT_MOVEMENT_VOL, PL_REG_MOVEMENT_VOL, PL_SALE_MOVEMENT_VOL, ");
		sql.append(" PL_TOT_VISIT_CNT, PL_AVG_ORDER_SIZE)");
		
		sql.append(" values (SALES_AGGR_DAILY_SEQ.NEXTVAL,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?");
		sql.append(" ,NULL,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)"); 
		return sql.toString();
	}

	/*
	 * ****************************************************************
	 * Method returns the Summary Daily Insert Query
	 * ****************************************************************
	 */

	private String updateSQL() {
		StringBuffer sql = new StringBuffer();
		sql.append(" UPDATE SALES_AGGR_DAILY SET ");
		sql.append(" PL_TOT_REVENUE=?, PL_REG_REVENUE=?, PL_SALE_REVENUE=?, ");
		sql.append(" PL_TOT_MARGIN=?, PL_REG_MARGIN=?, PL_SALE_MARGIN=?, ");
		sql.append(" PL_TOT_MARGIN_PCT=?, PL_REG_MARGIN_PCT=?, PL_SALE_MARGIN_PCT=?, ");
		sql.append(" PL_TOT_MOVEMENT=?,PL_REG_MOVEMENT=?, PL_SALE_MOVEMENT=?, ");
		sql.append(" PL_TOT_MOVEMENT_VOL=?, PL_REG_MOVEMENT_VOL=?, PL_SALE_MOVEMENT_VOL=?, ");
		sql.append(" PL_TOT_VISIT_CNT=?, PL_AVG_ORDER_SIZE=?");
		sql.append(" WHERE CALENDAR_ID=? AND LOCATION_LEVEL_ID=? AND LOCATION_ID=?");
		sql.append(" AND NVL(PRODUCT_LEVEL_ID, -1) = ? AND NVL(PRODUCT_ID, -1) = ?");
		
		return sql.toString();
	}
	
	
	/*
	 * Method used to move the records from Source table to destination Folder
	 * Process added for repair process.
	 * Argument 1 : Connection
	 * Argument 2 : Calendar Id 
	 * Argument 3 : Location Id(Store Number)
	 * @throws GeneralException
	 */
	
	public void moveTempTable(Connection _conn, int calendarId, int locationId)
			throws GeneralException {

		// Query
		StringBuffer sql = new StringBuffer();
		sql.append(" insert into SALES_AGGR_DAILY_TEMP");
		sql.append(" select * from SALES_AGGR_DAILY");
		sql.append(" where CALENDAR_ID=").append(calendarId);
		sql.append(" and LOCATION_ID=").append(locationId);
		logger.debug("moveTempTable SQL:" + sql.toString());

		// Exceute the Query
		try {
			int insertCnt = PristineDBUtil.executeUpdate(_conn, sql, "moveTempTable");
			if(insertCnt <= 0){
				insertDummyRecord(_conn, calendarId, locationId, Constants.STORE_LEVEL_ID);
			}
		} catch (GeneralException e) {
			logger.error("Error While Move data to temp table:"	+ e.getMessage());
			throw new GeneralException("moveTempTable", e);
		}

	}
	
	/**
	 * Inserts a dummy record into sales_aggr_daily_temp table
	 * @param _conn				Connection
	 * @param calendarId		Calendar Id
	 * @param locationId		Location Id
	 * @param locationLevelId	Location Level Id
	 * @throws GeneralException
	 */
	public void insertDummyRecord(Connection _conn, int calendarId, int locationId, int locationLevelId) throws GeneralException{	
		StringBuffer sql = new StringBuffer();
		sql.append("INSERT INTO SALES_AGGR_DAILY_TEMP (SUMMARY_DAILY_ID, CALENDAR_ID, LOCATION_LEVEL_ID, LOCATION_ID, PRODUCT_LEVEL_ID, PRODUCT_ID, TOT_VISIT_CNT, TOT_MOVEMENT,  ");
		sql.append("REG_MOVEMENT, SALE_MOVEMENT, TOT_REVENUE, REG_REVENUE, SALE_REVENUE, AVG_ORDER_SIZE, TOT_MARGIN, REG_MARGIN, SALE_MARGIN, ");
		sql.append("TOT_MARGIN_PCT, REG_MARGIN_PCT, SALE_MARGIN_PCT, LOYALTY_CARD_SAVING, TOT_MOVEMENT_VOL, REG_MOVEMENT_VOL, SALE_MOVEMENT_VOL, ");
		sql.append("REG_IGVOL_REVENUE, STORE_TYPE, SALE_IGVOL_REVENUE, TOT_IGVOL_REVENUE, LST_SUMMARY_DAILY_ID, SUMMARY_CTD_ID) ");
		sql.append("VALUES (SALES_AGGR_DAILY_SEQ.NEXTVAL, ?, ?, ?, NULL, NULL, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, NULL, 0, 0, 0, 0) ");
		
		PreparedStatement statement = null;
		try{
			statement = _conn.prepareStatement(sql.toString());
			statement.setInt(1, calendarId);
			statement.setInt(2, locationLevelId);
			statement.setInt(3, locationId);
			
			int count = statement.executeUpdate();
		}catch (SQLException e) {
			logger.error("Error While Inserting Dummy Data:"	+ e.getMessage());
			throw new GeneralException("insert Temp Table" , e);
		}finally{
			PristineDBUtil.close(statement);
		}
	}
	
	/*
	 * Method used to get the not-processing calendar list 
	 * This method added for repair process
	 * Argument 1 : Connection
	 * Argument 2 : Store Number
	 * Argument 3 : Process date
	 * Return Not Processing Calendar List
	 * @Catch SQLException 
	 * @throws Gendral Exception
	 * 
	 */
	 public List<RetailCalendarDTO> getNotProcessingCalendar(Connection _conn,
			String storeNum, Date processDate , String repairStage) throws GeneralException {
	
		 // Object for return calendar list
		 List<RetailCalendarDTO> returnList = new ArrayList<RetailCalendarDTO>();
		 
		 SimpleDateFormat formatter = new SimpleDateFormat("dd-MM-yyyy");	
		 
		 try {
			// Query
			 StringBuffer sql = new StringBuffer();
			 
			 sql.append(" select DISTINCT MV.CALENDAR_ID,RC.START_DATE");
			 sql.append(" FROM MOVEMENT_DAILY MV , RETAIL_CALENDAR RC");
			 sql.append(" WHERE RC.START_DATE >= to_date('"+formatter.format(processDate)+"','dd-MM-yyyy')-").append(repairStage);
			 sql.append(" AND MV.COMP_STR_NO   ='").append(storeNum).append("'");
			 sql.append(" AND MV.CALENDAR_ID = RC.CALENDAR_ID");
			 sql.append(" AND MV.PROCESSED_FLAG='N'");
			 sql.append(" ORDER BY MV.CALENDAR_ID");
			 
			 logger.debug("GetNotProcessingCalendar SQL:" + sql.toString());
			
			 // Execute the query
			 CachedRowSet result = PristineDBUtil.executeQuery(_conn, sql, "getNotProcessingCalendar");
			 
			 while(result.next()){
				 
				 RetailCalendarDTO objCalendarDto = new RetailCalendarDTO();
				 objCalendarDto.setCalendarId(result.getInt("CALENDAR_ID"));
				 objCalendarDto.setStartDate(result.getString("START_DATE"));
				 returnList.add(objCalendarDto);
			 }
			 
			 result.close();
		} catch (SQLException e) {
			logger.error(" Error While Fetching Not processing Calendar List: ", e);
			throw new GeneralException("Error in getNotProcessingCalendar " , e);
		} catch (GeneralException e) {
			logger.error(" Error While Fetching Not processing Calendar List: " , e);
			throw new GeneralException("Error in getNotProcessingCalendar " , e);
		}
		 
		 
		return returnList;
	}

		/*
		 * ****************************************************************
		 * Method used to delete the previous no cost items
		 * Argument 1 : _Conn
		 * Argument 2 : calendarId
		 * Argument 3 : locationId
		 * @throws GeneralException , SQLException
		 * ****************************************************************
		 */
		
		public void deletePreviousNoCostItemsInfo(Connection _Conn, int calendarId, 
															int locationId) throws GeneralException {
			
			//logger.debug("Delete Previous Aggregation Starts");
			
			StringBuffer sql = new StringBuffer();
			  
			    sql.append(" Delete from SA_NOCOST_ITEM WHERE CALENDAR_ID = '" + calendarId +"'");
			    sql.append(" and LOCATION_ID = '" + locationId + "'");
			 
			logger.debug("deletePreviousAggregation SQL:" +sql.toString());
			
			try {
				
				// execute the delete query
				PristineDBUtil.executeUpdate(_Conn, sql , 
													"deletePreviousNoCostItemsInfo");
				PristineDBUtil.commitTransaction(_Conn,	
											"Commit No Cost item data delete process");

			} catch (GeneralException e) {
				 logger.error("Error while deleting previous No Cost item" + e);
				throw new GeneralException("deletePreviousNoCostItemsInfo", e);		
			}
		}		 

		/*
		 * ****************************************************************
		 * Method returns the No Cost Item Insert Query
		 * ****************************************************************
		 */

		private String insertNoCostItemInsertSql() {
			StringBuffer sql = new StringBuffer();
			sql.append(" insert into SA_NOCOST_ITEM (CALENDAR_ID,");
			sql.append(" LOCATION_LEVEL_ID, LOCATION_ID, PRODUCT_LEVEL_ID, ");
			sql.append(" PRODUCT_ID, TOT_REVENUE) VALUES (?,?,?,?,?,?)");
			return sql.toString();
		}
		

		/*
		 * ****************************************************************
		 * Method to Create the insert script and add the script into batch List
		 * Argument 1 : SummaryDailyDto
		 * @throws GeneralException , SQLException
		 * ****************************************************************
		 */

		public void addNoCostItemSqlBatch(PreparedStatement psmt1, int calendarId, int locationLevelId, 
						int locationId, int productLevelId, String productId, 
															double totRevenue) {
			try {
				psmt1.setObject(1, calendarId);
				psmt1.setObject(2, locationLevelId);
				psmt1.setObject(3, locationId);
				psmt1.setObject(4, productLevelId); 
				psmt1.setObject(5, productId);
				psmt1.setDouble(6, GenericUtil.Round(totRevenue,2));
				psmt1.addBatch();
			} catch (Exception exe) {
				logger.error("Summary Daily Insert Error" , exe);
			}
						
		}
		
		/*
		 * ****************************************************************
		 * Method to  insert the No cost item data
		 * Argument 1 : DB connection
		 * Argument 2 : calendarId
		 * Argument 3 : storeId
		 * Arguemnt 4 : No cost Item Map
		 * call the execute batch method
		 * ****************************************************************
		 */
		public void insertNoCostItemData(Connection _Conn, int calendarId,
							int storeId, HashMap<String, Double> noCostItemMap) 	
													throws GeneralException {
				
			int storeLevelId = Constants.STORE_LEVEL_ID;
			int productLevelId = Constants.ITEMLEVELID;
			
			try
			  {
		        PreparedStatement psmt1=_Conn.prepareStatement(insertNoCostItemInsertSql());
		                
		        for (String key : noCostItemMap.keySet()) {
		            //System.out.println("Key: " + key + ", Value: " + noCostMap.get(key));
		            addNoCostItemSqlBatch(psmt1, calendarId, storeLevelId, storeId, productLevelId, key, noCostItemMap.get(key));
		        }
		        
				logger.debug("Execue Insert");
				
				int[] count = psmt1.executeBatch();
				logger.debug(" Insert record count:" + count.length);
				
				psmt1.close();
				 
			}
			catch(Exception exe) {
				logger.error("No cost data Insert Error" , exe);
				throw new GeneralException("No cost data Insert Error" , exe);
			}
		}
		
		/*
		 * Method used to get the not-processing calendar list 
		 * This method added for repair process
		 * Argument 1 : Connection
		 * Argument 2 : Store Number
		 * Return Not Processing Calendar List
		 * @Catch SQLException 
		 * @throws Gendral Exception
		 * 
		 */
		 public List<RetailCalendarDTO> getTLNotProcessingCalendar(Connection _conn,
				int storeId) throws GeneralException {
		
			 // Object for return calendar list
			 List<RetailCalendarDTO> returnList = new ArrayList<RetailCalendarDTO>();
			 
			 try {
				// Query
				 StringBuffer sql = new StringBuffer();
				 
				 sql.append(" SELECT DISTINCT TL.CALENDAR_ID,RC.START_DATE");
				 sql.append(" FROM TRANSACTION_LOG TL , RETAIL_CALENDAR RC");
				 sql.append(" WHERE TL.STORE_ID =").append(storeId);
				 sql.append(" AND TL.CALENDAR_ID = RC.CALENDAR_ID");
				 sql.append(" AND TL.PROCESSED_FLAG='N'");
				 sql.append(" ORDER BY TL.CALENDAR_ID");
				 
				 logger.debug("TransactionLogDAO.GetNotProcessingCalendar SQL:" + sql.toString());
				
				 // Execute the query
				 CachedRowSet result = PristineDBUtil.executeQuery(_conn, sql, "getNotProcessingCalendar");
				 
				 while(result.next()){
					 
					 RetailCalendarDTO objCalendarDto = new RetailCalendarDTO();
					 objCalendarDto.setCalendarId(result.getInt("CALENDAR_ID"));
					 objCalendarDto.setStartDate(result.getString("START_DATE"));
					 returnList.add(objCalendarDto);
				 }
				 
				 result.close();
			} catch (SQLException e) {
				logger.error(" Error While Fetching Not processing Calendar List: ", e);
				throw new GeneralException("Error in getNotProcessingCalendar " , e);
			} catch (GeneralException e) {
				logger.error(" Error While Fetching Not processing Calendar List: " , e);
				throw new GeneralException("Error in getNotProcessingCalendar " , e);
			}
			return returnList;
		}
		
}
