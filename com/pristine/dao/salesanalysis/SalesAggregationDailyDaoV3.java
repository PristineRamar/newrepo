package com.pristine.dao.salesanalysis;

import java.io.StringWriter;
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

import com.pristine.dao.DBManager;
import com.pristine.dto.RetailCalendarDTO;
import com.pristine.dto.SummaryDailyDTO;
import com.pristine.dto.salesanalysis.ProductGroupDTO;
import com.pristine.dto.salesanalysis.SummaryDataDTO;
import com.pristine.exception.GeneralException;
import com.pristine.util.Constants;
import com.pristine.util.GenericUtil;
import com.pristine.util.PristineDBUtil;

public class SalesAggregationDailyDaoV3 {
	
	static Logger logger = Logger.getLogger("SalesAggregationDailyDaoV3");

	static String temp_insert_query = "INSERT INTO SALES_AGGR_DAILY_MOVEMENT_TEMP(CALENDAR_ID,  COMP_STR_NO , PRODUCT_LEVEL_ID , PRODUCT_ID ,  ITEM_CODE ,  SALE_FLAG ,  UOM_ID ,  POS_DEPARTMENT , " + 
			"VOLUME ,  QUANTITY ,  WEIGHT_ACTUAL ,  WEIGHT ,  REVENUE ,  GROSSREVENUE ) " + 
			"SELECT <cal_id>,MV.COMP_STR_NO ,<least_level>, PR0.PRODUCT_ID as PRODUCT_ID0,  MV.ITEM_CODE, MV.SALE_FLAG as FLAG, I.UOM_ID, MV.POS_DEPARTMENT,  " + 
			"sum(MV.QUANTITY * I.ITEM_SIZE) as VOLUME, sum(MV.QUANTITY) as QUANTITY, sum(MV.WEIGHT) as WEIGHT_ACTUAL, count(case when MV.WEIGHT=0  then null else 1 end) as WEIGHT, sum( MV.PRICE) as REVENUE,  " + 
			"sum(MV.EXTENDED_GROSS_PRICE) as GROSSREVENUE " + 
			"from MOVEMENT_DAILY MV Left Join ITEM_LOOKUP I On MV.ITEM_CODE=I.ITEM_CODE AND I.CATEGORY_ID NOT IN (<exclude_category>) " + 
			"LEFT OUTER JOIN PRODUCT_GROUP_RELATION PR0 ON MV.ITEM_CODE = PR0.CHILD_PRODUCT_ID AND PR0.PRODUCT_LEVEL_ID=<least_level>  " + 
			"where  MV.CALENDAR_ID = <cal_id> and MV.POS_DEPARTMENT <= 39 and MV.COMP_STR_NO in ( " + 
			"  select comp_str_no from competitor_store where comp_str_no='<store_no>' or district_id = <district_no> " + 
			") " + 
			"and (I.CATEGORY_ID NOT IN (<exclude_category>) OR I.CATEGORY_ID IS NULL)  " + 
			"group by MV.COMP_STR_NO, PR0.PRODUCT_ID,  MV.ITEM_CODE, MV.SALE_FLAG, I.UOM_ID, MV.POS_DEPARTMENT " +
			"order by PR0.PRODUCT_ID,  MV.ITEM_CODE, MV.SALE_FLAG  ";
	
	static String temp_zero_update_query = "update SALES_AGGR_DAILY_MOVEMENT_TEMP set VOLUME = coalesce(VOLUME ,0),  quantity = coalesce(quantity ,0),  " + 
			"  weight_actual = coalesce(weight_actual ,0),   WEIGHT=coalesce(WEIGHT,0),  revenue =coalesce(revenue ,0),    " + 
			"  grossrevenue =coalesce(grossrevenue ,0),  SaleQuantity =0,  saleMovementVolume  =0, saleGrossRevenue  =0,  RevenueSale  =0,  RegularQuantity  =0,   " + 
			"  regMovementVolume =0, regGrossRevenue  =0,  RevenueRegular  =0,  ActualWeight  =0,  ActualQuantity  =0, " + 
			"  reg_movement = 0,sale_movement = 0,reg_margin = 0,  sale_margin = 0,reg_movement_vol = 0,sale_movement_vol = 0, " + 
			"  reg_igvol_revenue = 0, sale_igvol_revenue = 0,TOT_IGVOL_REVENUE=0, reg_deal_cost = 0,sale_deal_cost = 0" ;
	
	static String summary_daily_insert_query = "INSERT INTO SALES_AGGR_DAILY(SUMMARY_DAILY_ID,CALENDAR_ID,LOCATION_LEVEL_ID,LOCATION_ID,PRODUCT_LEVEL_ID,PRODUCT_ID,TOT_VISIT_CNT, " + 
			"TOT_MOVEMENT,REG_MOVEMENT,SALE_MOVEMENT,TOT_REVENUE,REG_REVENUE,SALE_REVENUE,AVG_ORDER_SIZE,TOT_MARGIN,REG_MARGIN,SALE_MARGIN,TOT_MARGIN_PCT, " + 
			"REG_MARGIN_PCT,SALE_MARGIN_PCT,LOYALTY_CARD_SAVING,LST_SUMMARY_DAILY_ID,SUMMARY_CTD_ID,TOT_MOVEMENT_VOL,REG_MOVEMENT_VOL,SALE_MOVEMENT_VOL,TOT_IGVOL_REVENUE, " + 
			"REG_IGVOL_REVENUE,SALE_IGVOL_REVENUE,STORE_TYPE) " + 
			"SELECT "+DBManager.getIdentityIncrement("sales_aggr_daily_seq") +",<cal_id>,"+Constants.STORE_LEVEL_ID+", CS.COMP_STR_ID,<parent_level>, P0.PRODUCT_ID, 0 VISIT_COUNT,  " + 
			"SUM(TOT_MOVEMENT) TOT_MOVEMENT,SUM(REG_MOVEMENT) REG_MOVEMENT,SUM(SALE_MOVEMENT) SALE_MOVEMENT, " + 
			"SUM(TOT_REVENUE) TOT_REVENUE,SUM(REVENUEREGULAR  ) REG_REVENUE,SUM(REVENUESALE ) SALE_REVENUE, " + 
			"0 AVG_ORDER_SIZE, " + 
			"SUM(TOT_MARGIN) TOT_MARGIN, SUM(REG_MARGIN) REG_MARGIN,SUM(SALE_MARGIN) SALE_MARGIN, " + 
			"CASE WHEN SUM(TOT_REVENUE) = 0 THEN 0 ELSE ROUND( (SUM(TOT_MARGIN) /  SUM(TOT_REVENUE)) * 100, 2) END TOT_MARGIN_PCT, " + 
			"CASE WHEN SUM(REVENUEREGULAR) = 0 THEN 0 ELSE ROUND( (SUM(REG_MARGIN) /  SUM(REVENUEREGULAR)) * 100, 2) END REG_MARGIN_PCT, " + 
			"CASE WHEN SUM(REVENUESALE) = 0 THEN 0 ELSE ROUND( (SUM(SALE_MARGIN) /  SUM(REVENUESALE)) * 100, 2) END SALE_MARGIN_PCT, " + 
			"/*loyalty_card_saving*/0,/*lst_summary_daily_id*/0,NULL, " + 
			"SUM(TOT_MOVEMENT_VOL) TOT_MOVEMENT_VOL,SUM(REG_MOVEMENT_VOL) REG_MOVEMENT_VOL,SUM(SALE_MOVEMENT_VOL) SALE_MOVEMENT_VOL, " + 
			"SUM(TOT_IGVOL_REVENUE) TOT_IGVOL_REVENUE,SUM(REG_IGVOL_REVENUE) REG_IGVOL_REVENUE,SUM(SALE_IGVOL_REVENUE) SALE_IGVOL_REVENUE, " + 
			"MIN(M.STORE_TYPE) " + 
			"from SALES_AGGR_DAILY_MOVEMENT_TEMP M  " + 
			"INNER JOIN COMPETITOR_STORE CS ON M.COMP_STR_NO = CS.COMP_STR_NO " +
			" <product_hierarchy_query> "+
			"WHERE P0.PRODUCT_ID IS NOT NULL " + 
			"GROUP BY CS.COMP_STR_ID ,P0.PRODUCT_ID ORDER BY P0.PRODUCT_ID ";
	
	static String summary_daily_visitcount =  
			"SELECT CS.COMP_STR_ID,P0.PRODUCT_ID , COUNT(DISTINCT M.TRANSACTION_NO) as VISIT_COUNT from MOVEMENT_DAILY M LEFT JOIN ITEM_LOOKUP I ON I.ITEM_CODE = M.ITEM_CODE " + 
			" <product_hierarchy_query> "+ 
			" INNER JOIN COMPETITOR_STORE CS ON CS.COMP_STR_NO = M.COMP_STR_NO "+
			" where  M.CALENDAR_ID =<cal_id> and M.POS_DEPARTMENT <=" + Constants.POS_GASDEPARTMENT +" and ( M.COMP_STR_NO ='<store_no>' OR CS.district_id = <district_no> ) " + 
			"  and (I.CATEGORY_ID is null or I.CATEGORY_ID NOT IN (<exclude_category>)) " + 
			" group by CS.COMP_STR_ID, P0.PRODUCT_ID   " ;
			//ROUND(SUM(TOT_REVENUE) /  TBL_VISIT_COUNT.VISIT_COUNT, 2) AVG_ORDER_SIZE, VISIT_COUNT
	
	static String pos_insert_query = "INSERT INTO SALES_AGGR_DAILY(SUMMARY_DAILY_ID,CALENDAR_ID,LOCATION_LEVEL_ID,LOCATION_ID,PRODUCT_LEVEL_ID,PRODUCT_ID,TOT_VISIT_CNT, " + 
			"TOT_MOVEMENT,REG_MOVEMENT,SALE_MOVEMENT,TOT_REVENUE,REG_REVENUE,SALE_REVENUE,AVG_ORDER_SIZE,TOT_MARGIN,REG_MARGIN,SALE_MARGIN,TOT_MARGIN_PCT, " + 
			"REG_MARGIN_PCT,SALE_MARGIN_PCT,LOYALTY_CARD_SAVING,LST_SUMMARY_DAILY_ID,SUMMARY_CTD_ID,TOT_MOVEMENT_VOL,REG_MOVEMENT_VOL,SALE_MOVEMENT_VOL,TOT_IGVOL_REVENUE, " + 
			"REG_IGVOL_REVENUE,SALE_IGVOL_REVENUE,STORE_TYPE) " + 
			"SELECT "+DBManager.getIdentityIncrement("sales_aggr_daily_seq") +",<cal_id>,"+Constants.STORE_LEVEL_ID+", CS.COMP_STR_ID,"+Constants.POSDEPARTMENT+", M.POS_DEPARTMENT AS PRODUCT_ID, 0 VISIT_COUNT,  " + 
			"SUM(TOT_MOVEMENT) TOT_MOVEMENT,SUM(REG_MOVEMENT) REG_MOVEMENT,SUM(SALE_MOVEMENT) SALE_MOVEMENT, " + 
			"SUM(TOT_REVENUE) TOT_REVENUE,SUM(REVENUEREGULAR  ) REG_REVENUE,SUM(REVENUESALE ) SALE_REVENUE, " + 
			"0 AVG_ORDER_SIZE, " + 
			"SUM(TOT_MARGIN) TOT_MARGIN, SUM(REG_MARGIN) REG_MARGIN,SUM(SALE_MARGIN) SALE_MARGIN, " + 
			"CASE WHEN SUM(TOT_REVENUE) = 0 THEN 0 ELSE ROUND( (SUM(TOT_MARGIN) /  SUM(TOT_REVENUE)) * 100, 2) END TOT_MARGIN_PCT, " + 
			"CASE WHEN SUM(REVENUEREGULAR) = 0 THEN 0 ELSE ROUND( (SUM(REG_MARGIN) /  SUM(REVENUEREGULAR)) * 100, 2) END REG_MARGIN_PCT, " + 
			"CASE WHEN SUM(REVENUESALE) = 0 THEN 0 ELSE ROUND( (SUM(SALE_MARGIN) /  SUM(REVENUESALE)) * 100, 2) END SALE_MARGIN_PCT, " + 
			"/*loyalty_card_saving*/0,/*lst_summary_daily_id*/0,NULL, " + 
			"SUM(TOT_MOVEMENT_VOL) TOT_MOVEMENT_VOL,SUM(REG_MOVEMENT_VOL) REG_MOVEMENT_VOL,SUM(SALE_MOVEMENT_VOL) SALE_MOVEMENT_VOL, " + 
			"SUM(TOT_IGVOL_REVENUE) TOT_IGVOL_REVENUE,SUM(REG_IGVOL_REVENUE) REG_IGVOL_REVENUE,SUM(SALE_IGVOL_REVENUE) SALE_IGVOL_REVENUE, " + 
			"MIN(M.STORE_TYPE) " + 
			"from SALES_AGGR_DAILY_MOVEMENT_TEMP M  " + 
			"INNER JOIN COMPETITOR_STORE CS ON M.COMP_STR_NO = CS.COMP_STR_NO " +
			"WHERE M.ITEM_CODE IN (<item_code_list>)  " + 
			"GROUP BY CS.COMP_STR_ID ,M.POS_DEPARTMENT ORDER BY M.POS_DEPARTMENT ";
	
	static String pos_visit_count = "SELECT CS.COMP_STR_ID, M.POS_DEPARTMENT AS PRODUCT_ID , COUNT(DISTINCT M.TRANSACTION_NO) as VISIT_COUNT from MOVEMENT_DAILY M LEFT JOIN ITEM_LOOKUP I ON I.ITEM_CODE = M.ITEM_CODE " + 
			" INNER JOIN COMPETITOR_STORE CS ON CS.COMP_STR_NO = M.COMP_STR_NO "+
			" where  M.CALENDAR_ID =<cal_id> AND M.ITEM_CODE IN (<item_code_list>) and ( M.COMP_STR_NO ='<store_no>' OR CS.district_id = <district_no> ) " + 
			"  and (I.CATEGORY_ID is null or I.CATEGORY_ID NOT IN (<exclude_category>)) " + 
			" group by CS.COMP_STR_ID, M.POS_DEPARTMENT   " ;
	
	// Summary and visit count together
	static String store_summary_daily_insert_query = "INSERT INTO SALES_AGGR_DAILY(SUMMARY_DAILY_ID,CALENDAR_ID,LOCATION_LEVEL_ID,LOCATION_ID,PRODUCT_LEVEL_ID,PRODUCT_ID,TOT_VISIT_CNT, " + 
			"TOT_MOVEMENT,REG_MOVEMENT,SALE_MOVEMENT,TOT_REVENUE,REG_REVENUE,SALE_REVENUE,AVG_ORDER_SIZE,TOT_MARGIN,REG_MARGIN,SALE_MARGIN,TOT_MARGIN_PCT, " + 
			"REG_MARGIN_PCT,SALE_MARGIN_PCT,LOYALTY_CARD_SAVING,LST_SUMMARY_DAILY_ID,SUMMARY_CTD_ID,TOT_MOVEMENT_VOL,REG_MOVEMENT_VOL,SALE_MOVEMENT_VOL,TOT_IGVOL_REVENUE, " + 
			"REG_IGVOL_REVENUE,SALE_IGVOL_REVENUE,STORE_TYPE) " + 
			"SELECT "+DBManager.getIdentityIncrement("sales_aggr_daily_seq") +",<cal_id>,"+Constants.STORE_LEVEL_ID+", CS.COMP_STR_ID,null, null, TBL_VISIT_COUNT.VISIT_COUNT,  " + 
			"SUM(TOT_MOVEMENT) TOT_MOVEMENT,SUM(REG_MOVEMENT) REG_MOVEMENT,SUM(SALE_MOVEMENT) SALE_MOVEMENT, " + 
			"SUM(TOT_REVENUE) TOT_REVENUE,SUM(REVENUEREGULAR  ) REG_REVENUE,SUM(REVENUESALE ) SALE_REVENUE, " + 
			"ROUND(SUM(TOT_REVENUE) /  TBL_VISIT_COUNT.VISIT_COUNT, 2) AVG_ORDER_SIZE, " + 
			"SUM(TOT_MARGIN) TOT_MARGIN, SUM(REG_MARGIN) REG_MARGIN,SUM(SALE_MARGIN) SALE_MARGIN, " + 
			"CASE WHEN SUM(TOT_REVENUE) = 0 THEN 0 ELSE ROUND( (SUM(TOT_MARGIN) /  SUM(TOT_REVENUE)) * 100, 2) END TOT_MARGIN_PCT, " + 
			"CASE WHEN SUM(REVENUEREGULAR) = 0 THEN 0 ELSE ROUND( (SUM(REG_MARGIN) /  SUM(REVENUEREGULAR)) * 100, 2) END REG_MARGIN_PCT, " + 
			"CASE WHEN SUM(REVENUESALE) = 0 THEN 0 ELSE ROUND( (SUM(SALE_MARGIN) /  SUM(REVENUESALE)) * 100, 2) END SALE_MARGIN_PCT, " + 
			"/*loyalty_card_saving*/0,/*lst_summary_daily_id*/0,NULL, " + 
			"SUM(TOT_MOVEMENT_VOL) TOT_MOVEMENT_VOL,SUM(REG_MOVEMENT_VOL) REG_MOVEMENT_VOL,SUM(SALE_MOVEMENT_VOL) SALE_MOVEMENT_VOL, " + 
			"SUM(TOT_IGVOL_REVENUE) TOT_IGVOL_REVENUE,SUM(REG_IGVOL_REVENUE) REG_IGVOL_REVENUE,SUM(SALE_IGVOL_REVENUE) SALE_IGVOL_REVENUE, " + 
			"MIN(M.STORE_TYPE) " + 
			"from SALES_AGGR_DAILY_MOVEMENT_TEMP M  " + 
			"INNER JOIN COMPETITOR_STORE CS ON M.COMP_STR_NO = CS.COMP_STR_NO " +
			"LEFT JOIN ( " + 
			"SELECT M.COMP_STR_NO , COUNT(DISTINCT M.TRANSACTION_NO) as VISIT_COUNT from MOVEMENT_DAILY M LEFT JOIN ITEM_LOOKUP I ON I.ITEM_CODE = M.ITEM_CODE " + 
			"where  M.CALENDAR_ID =<cal_id> and M.POS_DEPARTMENT <=" + Constants.POS_GASDEPARTMENT +" and M.COMP_STR_NO IN ( " + 
			"  select comp_str_no from competitor_store where comp_str_no='<store_no>' or district_id = <district_no> " + 
			")  and (I.CATEGORY_ID is null or I.CATEGORY_ID NOT IN (<exclude_category>))  AND I.ITEM_CODE NOT IN (<gas_items>)  " + 
			"group by M.COMP_STR_NO  " + 
			") TBL_VISIT_COUNT on TBL_VISIT_COUNT.COMP_STR_NO = M.COMP_STR_NO " + 
			"WHERE M.ITEM_CODE NOT IN (<gas_items>) " +
			"GROUP BY CS.COMP_STR_ID ,TBL_VISIT_COUNT.VISIT_COUNT ";

	//This method returns the Calendar Id of the last data loading
	public int getLastUpdatedCalendarId(Connection _Conn, String compStrNo, String districtNum, String calendarMode){
		StringBuffer sql = new StringBuffer();
		//1. Retrieve the calendar id, when the last data loading happened
		sql.append(" SELECT CALENDAR_ID FROM RETAIL_CALENDAR WHERE ROW_TYPE='"+calendarMode+"' AND START_DATE IN " + 
				" (select MAX(RC.START_DATE) from SALES_AGGR_DAILY T INNER JOIN RETAIL_CALENDAR RC " + 
				"  ON RC.CALENDAR_ID = T.CALENDAR_ID WHERE RC.ROW_TYPE='"+calendarMode+"' AND LOCATION_ID IN  " +
				"  (select comp_str_id from competitor_store where comp_str_no='"+compStrNo+"' or district_id = "+districtNum+" )"+
				" )") ;
		
		logger.debug(sql.toString());
		
		int lastCalendarId = 0;
		
		try{
			String calIdStr = PristineDBUtil.getSingleColumnVal(_Conn, sql, "getCalendarIdsForDataLoading");
			lastCalendarId = Integer.parseInt(calIdStr);
		} catch ( GeneralException ge){
			lastCalendarId = 0;
		} catch ( NumberFormatException nfe){
			lastCalendarId = 0;
		}
		
		return lastCalendarId;
	}
	
	/*
	 * ****************************************************************
	 * Method used to delete the previous aggregation for store
	 * Argument 1 : _Conn
	 * Argument 2 : calendarId
	 * Argument 3 : compStrNo
	 * Argument 4 : districtNum
	 * @throws GeneralException , SQLException
	 * ****************************************************************
	 */
	
	public void deletePreviousAggregation(Connection _Conn, int calendarId, 
														String compStrNo, String districtNum) throws GeneralException {
		
		//logger.debug("Delete Previous Aggregation Starts");
		
		try {
		
			//StringBuffer CTDsql = new StringBuffer();
			StringBuffer dailysql = new StringBuffer();
			StringBuffer tempsql = new StringBuffer();
			  
			//SQL to delete CTD data
			//CTDsql.append(" Delete from SALES_AGGR_CTD WHERE SUMMARY_CTD_ID IN (");
			//CTDsql.append(" SELECT SUMMARY_CTD_ID from SALES_AGGR_DAILY WHERE CALENDAR_ID = '" + calendarId +"'");
			//CTDsql.append(" and LOCATION_ID in ( select comp_str_id from competitor_store where comp_str_no ='"+compStrNo+"' or district_id = "+districtNum+" ) )");
			
			// execute the Sales Aggr CTD data delete query
			//logger.debug("deletePreviousAggregation CTD SQL:" +CTDsql.toString());
			//PristineDBUtil.executeUpdate(_Conn, CTDsql , "deletePreviousAggregation");
			
			//SQL to delete Sales Aggr Daily data
			dailysql.append(" Delete from SALES_AGGR_DAILY WHERE CALENDAR_ID = '" + calendarId +"'");
		    dailysql.append(" and LOCATION_ID in ( select comp_str_id from competitor_store where comp_str_no ='"+compStrNo+"' or district_id = "+districtNum+" )");
		 
		    // execute the Sales Aggr Daily data delete query
		    logger.debug("deletePreviousAggregation SQL:" +dailysql.toString());
		    PristineDBUtil.executeUpdate(_Conn, dailysql , "deletePreviousAggregation");
		 			
		    //SQL to delete Temp table Aggr Daily data
		    tempsql.append("truncate table SALES_AGGR_DAILY_MOVEMENT_TEMP");
		    logger.debug("deleteTempAggregation SQL:" +tempsql.toString());
			PristineDBUtil.executeUpdate(_Conn, tempsql , "deleteTempAggregation");
			
			PristineDBUtil.commitTransaction(_Conn,	 "Commit Aggregation delete process");
		} catch (GeneralException e) {
			 logger.error("Error while deleting previous Aggregation data" + e);
			throw new GeneralException("deletePreviousAggregation", e);		
		}
	}
	
	public void deletePreviousNoCostItemsInfo(Connection _Conn, int calendarId, 
			String compStrNo, String districtNum) throws GeneralException {
		
		//logger.debug("Delete Previous Aggregation Starts");
		
		StringBuffer sql = new StringBuffer();
		  
		    sql.append(" Delete from SA_NOCOST_ITEM WHERE CALENDAR_ID = '" + calendarId +"'");
		    sql.append(" and LOCATION_ID in ( select comp_str_id from competitor_store where comp_str_no ='"+compStrNo+"' or district_id = "+districtNum+" )");
		 
		logger.debug("deletePreviousNoCostItemsInfo SQL:" +sql.toString());
		
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
	
	public void insertMovementDailyTemp(Connection _Conn, int calendarId,ArrayList<Integer> leastProductLevels , String compStrNo, String districtNum, int excludeCategory) throws GeneralException {
		try {
		
			StringBuffer sql = new StringBuffer();
			for (int leastLevel : leastProductLevels) {

				
				String tempSql = temp_insert_query.replaceAll("<cal_id>", calendarId + "");
				tempSql = tempSql.replaceAll("<least_level>", leastLevel + "");
				tempSql = tempSql.replaceAll("<exclude_category>", excludeCategory + "");
				tempSql = tempSql.replaceAll("<store_no>", compStrNo + "");
				tempSql = tempSql.replaceAll("<district_no>", districtNum + "");
				
				sql.setLength(0);
				sql.append(tempSql);
				
				//SQL to insert summary daily data
				logger.debug("insertTempAggregation SQL:" + tempSql);
				PristineDBUtil.executeUpdate(_Conn, sql , "insertTempAggregation");
	        }
			
			//Update null values to 0
			sql.setLength(0);
			sql.append(temp_zero_update_query );
			logger.debug("zeroUpdateQuery SQL:" + sql.toString());
			PristineDBUtil.executeUpdate(_Conn, sql , "zeroUpdateQuery");
			
			PristineDBUtil.commitTransaction(_Conn,	 "Commit Aggregation Temporary Insert process");
		} 
		catch (GeneralException e) {
			logger.error("Error while inserting Temporary Aggregation process" + e);
			throw new GeneralException("insertTemporaryAggregation", e);		
		}
	}
	
	
	public void retailDealCostUpdateToTemp(Connection _Conn, int calendarId, int weekCalId ) throws GeneralException {
		try {
		
			StringBuffer sql = new StringBuffer();
			
			String listQ = "SELECT SADMT.COMP_STR_NO,RCI.ITEM_CODE, RCI.LIST_COST, RCI.DEAL_COST FROM RETAIL_COST_INFO RCI " + 
					"INNER JOIN SALES_AGGR_DAILY_MOVEMENT_TEMP SADMT ON RCI.ITEM_CODE = SADMT.ITEM_CODE " + 
					"INNER JOIN COMPETITOR_STORE CS ON CS.COMP_STR_NO = SADMT.COMP_STR_NO " +
					//THE TRANSACTION_LOG TABLE IS JOINED TO MAKE SURE MOVEMENT DAILY HAS A T-LOG ENTRY
					"INNER JOIN TRANSACTION_LOG T ON CS.COMP_STR_ID = T.STORE_ID AND SADMT.ITEM_CODE = T.ITEM_ID AND SADMT.CALENDAR_ID=T.CALENDAR_ID " +
					"WHERE RCI.CALENDAR_ID = <weekcal_id> AND  SADMT.CALENDAR_ID = <cal_id> AND  /*Store|Zone|Chain*/ " +
					" <filter_clause> "+
					"GROUP BY SADMT.COMP_STR_NO,RCI.ITEM_CODE, RCI.LIST_COST, RCI.DEAL_COST ";
			
			listQ =  listQ.replaceAll("<cal_id>", calendarId+"");
			listQ =  listQ.replaceAll("<weekcal_id>", weekCalId+"");
			
			String storeCheck = "(LEVEL_TYPE_ID = "+Constants.STORE_LEVEL_TYPE_ID+" AND LEVEL_ID = SADMT.COMP_STR_NO ) " ;
			String zoneCheck = "(LEVEL_TYPE_ID = "+Constants.ZONE_LEVEL_TYPE_ID+" AND LTRIM(LEVEL_ID,'0') in (SELECT PRICE_ZONE_ID||'' FROM COMPETITOR_STORE WHERE COMP_STR_NO= SADMT.COMP_STR_NO) )" ;
			String chainCheck = "(LEVEL_TYPE_ID = "+Constants.CHAIN_LEVEL_TYPE_ID+" AND LTRIM(LEVEL_ID,'0') in (SELECT MIN(COMP_CHAIN_ID)||'' FROM COMPETITOR_STORE WHERE COMP_STR_NO= SADMT.COMP_STR_NO) )" ;
			
			CachedRowSet tablecolumns ;
			
			int i = Constants.CHAIN_LEVEL_TYPE_ID;
			while(i <= Constants.STORE_LEVEL_TYPE_ID){
				sql.setLength(0);
				if(i==Constants.CHAIN_LEVEL_TYPE_ID)
					sql.append(listQ.replaceAll("<filter_clause>", chainCheck));
				else if(i==Constants.ZONE_LEVEL_TYPE_ID)
					sql.append(listQ.replaceAll("<filter_clause>", zoneCheck));
				else if(i==Constants.STORE_LEVEL_TYPE_ID)
					sql.append(listQ.replaceAll("<filter_clause>", storeCheck));
				
				logger.debug("retailDealCostUpdateToTemp SQL:" + sql);
				tablecolumns = PristineDBUtil.executeQuery(_Conn, sql, "retailDealCostUpdateToTemp");
				
				PreparedStatement psmt = _Conn.prepareStatement("UPDATE SALES_AGGR_DAILY_MOVEMENT_TEMP SET SALE_DEAL_COST = CASE WHEN SALE_FLAG='Y' THEN ? ELSE SALE_DEAL_COST END , " + 
					"REG_DEAL_COST = CASE WHEN SALE_FLAG='N' THEN ? ELSE REG_DEAL_COST END WHERE ITEM_CODE = ? AND COMP_STR_NO = ? ");
			
				while (tablecolumns.next()) {
					String compStrNo = tablecolumns.getString("COMP_STR_NO");
					int itemCode = tablecolumns.getInt("ITEM_CODE");
					double listCost = tablecolumns.getDouble("LIST_COST");
					double dealCost = tablecolumns.getDouble("DEAL_COST");
					
					double cost = (dealCost>0 && dealCost < listCost) ? dealCost : listCost; 
					
					logger.debug("retailDealCostUpdateToTemp Parameters :cost = " + cost + ", itemCode = " + itemCode + ", compstrno = " + compStrNo);
					
					psmt.setDouble(1, cost);
					psmt.setDouble(2, cost);
					psmt.setInt(3, itemCode);
					psmt.setString(4, compStrNo);
					psmt.addBatch();
				}
				tablecolumns.close();
				psmt.executeBatch();
				psmt.close();
			
				i++;
			}
			
			PristineDBUtil.commitTransaction(_Conn,	 "Commit Retail Deal/List cost to Temp table");
			
		} catch (SQLException e) {
			// TODO Auto-generated catch block
			logger.error("Error while List/Deal cost update", e);
			throw new GeneralException("retailDealCostUpdateToTemp", e);	
		}

		catch (GeneralException e) {
			logger.error("Error while List/Deal cost update" + e);
			throw new GeneralException("retailDealCostUpdateToTemp", e);		
		}
	}
	
	public void segregateSaleAndRegularFromTemp(Connection _Conn) throws GeneralException {
		try {
		
			StringBuffer sql = new StringBuffer();
			//objMovementDto.setActualWeight(rst.getDouble("WEIGHT_ACTUAL"));
			//objMovementDto.setActualQuantity(rst.getDouble("QUANTITY"));
			//objMovementDto.setregMovementVolume(rst.getDouble("VOLUME") + rst.getDouble("WEIGHT"));
			String tempSql = "update SALES_AGGR_DAILY_MOVEMENT_TEMP set ActualWeight = weight_actual,ActualQuantity = QUANTITY ;"+
					"update SALES_AGGR_DAILY_MOVEMENT_TEMP set SaleQuantity =quantity+weight,  saleMovementVolume =volume+weight, saleGrossRevenue =grossrevenue,  RevenueSale =REVENUE where sale_flag='Y' ;"+
					"update SALES_AGGR_DAILY_MOVEMENT_TEMP set RegularQuantity =quantity+weight, regMovementVolume =volume+weight, regGrossRevenue =grossrevenue,  RevenueRegular =REVENUE where sale_flag='N' ;"+
					"update SALES_AGGR_DAILY_MOVEMENT_TEMP set sale_movement = SaleQuantity, reg_movement = RegularQuantity ;";
			
			//objMoveDto.getActualQuantity() > 0
			//calcDealCost = unitDealCost * objMoveDto.getActualQuantity()
			//objMoveDto.getActualWeight() > 0
			//calcDealCost = unitDealCost * objMoveDto.getActualWeight()
			
			tempSql += "UPDATE SALES_AGGR_DAILY_MOVEMENT_TEMP SET SALE_DEAL_COST = ActualQuantity * SALE_DEAL_COST where ActualQuantity > 0;";
			tempSql += "UPDATE SALES_AGGR_DAILY_MOVEMENT_TEMP SET REG_DEAL_COST = ActualQuantity * REG_DEAL_COST where ActualQuantity > 0;";
			tempSql += "UPDATE SALES_AGGR_DAILY_MOVEMENT_TEMP SET SALE_DEAL_COST = ActualWeight * SALE_DEAL_COST where ActualQuantity=0 and ActualWeight > 0;";
			tempSql += "UPDATE SALES_AGGR_DAILY_MOVEMENT_TEMP SET REG_DEAL_COST = ActualWeight * REG_DEAL_COST where ActualQuantity=0 and ActualWeight > 0;";
			
			
			tempSql = tempSql.trim();
			if(tempSql.endsWith(";")) tempSql = tempSql.substring(0, tempSql.lastIndexOf(";") );
			
			for(String query: tempSql.split(";")){
				sql.setLength(0);sql.append(query);
				logger.debug("segregateSaleAndRegularFromTemp SQL:" + query);
				PristineDBUtil.executeUpdate(_Conn, sql , "SaleAndRegularUpdate");
			}
			
			PristineDBUtil.commitTransaction(_Conn,	 "Commit Aggregation Sale and Regular segregation");
		} 
		catch (GeneralException e) {
			logger.error("Error while sale and regular update process" + e);
			throw new GeneralException("segregateSaleAndRegularFromTemp", e);		
		}
	}
	
	public void POSUpdateToTemp(Connection _Conn, HashMap<Integer, Integer> gasItems, HashMap<String, Double>  actualGasValueMap, int coupounAdjReq ) throws GeneralException {
		try {
			
			String itemCodeString = "0";
			for(Integer i: gasItems.keySet())	itemCodeString += "," + i;
			
			StringBuffer sql = new StringBuffer();
			String tempSql = "update SALES_AGGR_DAILY_MOVEMENT_TEMP set RevenueRegular = regGrossRevenue,RevenueSale = saleGrossRevenue where ITEM_CODE in ("+itemCodeString+");" ;
			
			for(String state : actualGasValueMap.keySet()){
				Double gasValue = actualGasValueMap.get(state);
				if(gasValue > 0){
					tempSql += "update SALES_AGGR_DAILY_MOVEMENT_TEMP set RevenueSale = CASE WHEN sale_flag='Y' THEN RevenueSale * "+gasValue+" ELSE  RevenueSale END," + 
							" RevenueRegular = CASE WHEN sale_flag='N' THEN RevenueRegular * "+gasValue+" ELSE  RevenueRegular END where COMP_STR_NO IN ( " + 
					"  select comp_str_no from competitor_store where STATE='"+state+"' ) and ITEM_CODE IN ("+itemCodeString+") ;"; 
				}
			}

			
			tempSql = tempSql.trim();
			if(tempSql.endsWith(";")) tempSql = tempSql.substring(0, tempSql.lastIndexOf(";") );
			
			for(String query: tempSql.split(";")){
				sql.setLength(0);sql.append(query);
				logger.debug("POSUpdateToTemp SQL:" + query);
				PristineDBUtil.executeUpdate(_Conn, sql , "POSUpdateToTemp");
			}
			
			PristineDBUtil.commitTransaction(_Conn,	 "Commit");
		} 
		catch (GeneralException e) {
			logger.error("Error while sale and regular update process" + e);
			throw new GeneralException("POSUpdateToTemp", e);		
		}
	}
	
	public void movementVolumeUpdateToTemp(Connection _Conn) throws GeneralException {
		try {
		
			StringBuffer sql = new StringBuffer();
			sql.append("SELECT id,name,ounce FROM UOM_Lookup where ounce is not null");
			CachedRowSet tablecolumns = PristineDBUtil.executeQuery(_Conn, sql, "movementVolumeUpdateToTemp");
			
			PreparedStatement psmt = _Conn.prepareStatement("update SALES_AGGR_DAILY_MOVEMENT_TEMP TMP set reg_movement_vol = ? * regMovementVolume," + 
					" sale_movement_vol = ? * saleMovementVolume where uom_id = ? ");
		
			while (tablecolumns.next()) {
				String uom_id = tablecolumns.getString("id");
				//String uom_name = tablecolumns.getString("name");
				double uom_oz = tablecolumns.getDouble("ounce");
				
				psmt.setDouble(1, uom_oz);
				psmt.setDouble(2, uom_oz);
				psmt.setString(3, uom_id);
				psmt.addBatch();
			}
			tablecolumns.close();
			psmt.executeBatch();
			psmt.close();
			
			//In case if the UOM id is unavailble set the movement volume to 0
				
			
			String tempSql = "UPDATE SALES_AGGR_DAILY_MOVEMENT_TEMP SET REG_IGVOL_REVENUE = REVENUEREGULAR WHERE UOM_ID IS NULL OR UOM_ID IN (SELECT ID FROM UOM_LOOKUP WHERE OUNCE IS NULL) ; " + 
					"UPDATE SALES_AGGR_DAILY_MOVEMENT_TEMP SET SALE_IGVOL_REVENUE = REVENUESALE WHERE UOM_ID IS NULL OR UOM_ID IN (SELECT ID FROM UOM_LOOKUP WHERE OUNCE IS NULL) ; " + 
					"UPDATE SALES_AGGR_DAILY_MOVEMENT_TEMP SET TOT_IGVOL_REVENUE = (REG_IGVOL_REVENUE + SALE_IGVOL_REVENUE) WHERE UOM_ID IS NULL OR UOM_ID IN (SELECT ID FROM UOM_LOOKUP WHERE OUNCE IS NULL)  ; " + 
					"UPDATE SALES_AGGR_DAILY_MOVEMENT_TEMP SET REG_MARGIN = REVENUEREGULAR - REG_DEAL_COST; " + 
					"UPDATE SALES_AGGR_DAILY_MOVEMENT_TEMP SET SALE_MARGIN = REVENUESALE - SALE_DEAL_COST; " + 
					"UPDATE SALES_AGGR_DAILY_MOVEMENT_TEMP SET TOT_MOVEMENT = (REG_MOVEMENT + SALE_MOVEMENT); " + 
					"UPDATE SALES_AGGR_DAILY_MOVEMENT_TEMP SET TOT_REVENUE = (REVENUEREGULAR + REVENUESALE); " + 
					"UPDATE SALES_AGGR_DAILY_MOVEMENT_TEMP SET TOT_MARGIN = (REG_MARGIN + SALE_MARGIN); " + 
					"UPDATE SALES_AGGR_DAILY_MOVEMENT_TEMP SET TOT_MOVEMENT_VOL = (REG_MOVEMENT_VOL + SALE_MOVEMENT_VOL); " ;
			
			tempSql = tempSql.trim();
			if(tempSql.endsWith(";")) tempSql = tempSql.substring(0, tempSql.lastIndexOf(";") );
			
			for(String query: tempSql.split(";")){
				sql.setLength(0);sql.append(query);
				logger.debug("movementVolumeUpdateToTemp SQL:" + query);
				PristineDBUtil.executeUpdate(_Conn, sql , "updateMovement");
			}
			
			PristineDBUtil.commitTransaction(_Conn,	 "Commit Aggregation Movement Volume Update process");
		} catch (SQLException e) {
			// TODO Auto-generated catch block
			logger.error("Error while UOM based volume update", e);
			throw new GeneralException("movementVolumeUpdateToTemp", e);	
		}

		catch (GeneralException e) {
			logger.error("Error while inserting Temporary Aggregation process" + e);
			throw new GeneralException("movementVolumeUpdateToTemp", e);		
		}
	}
	
	public void storeTypeUpdateToTemp(Connection _Conn, int calendarId, String compStrNo, String districtNum) throws GeneralException {
		try {
		
			StringBuffer sql = new StringBuffer();
			sql.append("select comp_str_no, case when A.START_DATE - 364 > open_date then 'I' when A.START_DATE - 364 < open_date then 'N' END as status "); 
			sql.append("from RETAIL_CALENDAR A INNER JOIN COMPETITOR_STORE CS ON comp_str_no='"+compStrNo+"' or district_id = "+districtNum+" ");
			sql.append("where (select start_date from RETAIL_CALENDAR where calendar_id="+calendarId+") between START_DATE and END_DATE and ROW_TYPE='W'");

			logger.debug("storeTypeUpdateToTemp SQL:" + sql.toString());
			CachedRowSet tablecolumns = PristineDBUtil.executeQuery(_Conn, sql, "movementCompetitorParamsToTemp");
			PreparedStatement psmt = _Conn.prepareStatement("update SALES_AGGR_DAILY_MOVEMENT_TEMP SET store_type = ? where COMP_STR_NO = ? ");
		
			while (tablecolumns.next()) {
				String comp_str_no = tablecolumns.getString("comp_str_no");
				String status = tablecolumns.getString("status");
				
				psmt.setString(1, status);
				psmt.setString(2, comp_str_no);
				psmt.addBatch();
			}
			tablecolumns.close();
			psmt.executeBatch();
			psmt.close();
			
			
			PristineDBUtil.commitTransaction(_Conn,	 "Commit Aggregation Movement Volume Update process");
		} catch (SQLException e) {
			// TODO Auto-generated catch block
			logger.error("Error while UOM based volume update", e);
			throw new GeneralException("movementVolumeUpdateToTemp", e);	
		}

		catch (GeneralException e) {
			logger.error("Error while inserting Temporary Aggregation process" + e);
			throw new GeneralException("movementVolumeUpdateToTemp", e);		
		}
	}
	
	
	public void moveTempToSalesAggregation(Connection _Conn, ArrayList<ProductGroupDTO> lstPGDTO, int calendarId, String compStrNo, 
			String districtNum, int excludeCategory) throws GeneralException {
		
		StringBuffer sql = new StringBuffer();
		String tempSql = summary_daily_insert_query;
		
		try {
			
			//form the Product Group DTO query
			int productGroupLength = lstPGDTO.size();
			int parentLevelID = lstPGDTO.get(0).getProductLevelId();
			
			//Skip the item level and perform aggregation for all product levels
			if(productGroupLength > 1 ){
				String product_hierarchy_query = "";
				int inc = 0;
				int prevlevel = 1;
				int i;
				for(i = 0 ; i < productGroupLength-2 ; i++){
					product_hierarchy_query = " LEFT JOIN PRODUCT_GROUP_RELATION P"+inc+" ON P"+ prevlevel +".PRODUCT_ID = P"+inc+".CHILD_PRODUCT_ID AND P"+inc+".PRODUCT_LEVEL_ID=" + lstPGDTO.get(i).getProductLevelId() + product_hierarchy_query;
					inc++;
					prevlevel = inc+1;
				}
				
				product_hierarchy_query = " LEFT JOIN PRODUCT_GROUP_RELATION P"+inc+" ON M.ITEM_CODE = P"+inc+".CHILD_PRODUCT_ID AND P"+inc+".PRODUCT_LEVEL_ID=" +lstPGDTO.get(i).getProductLevelId() +product_hierarchy_query ;
								
				tempSql = tempSql.replaceAll("<cal_id>", calendarId + "");
				tempSql = tempSql.replaceAll("<store_no>", compStrNo + "");
				tempSql = tempSql.replaceAll("<district_no>", districtNum + "");
				
				//Product Hierarchy query
				//LEFT JOIN PRODUCT_GROUP_RELATION P1 ON M.ITEM_CODE = P1.CHILD_PRODUCT_ID AND P1.PRODUCT_LEVEL_ID=2 
				//LEFT JOIN PRODUCT_GROUP_RELATION P0 ON M.PRODUCT_ID = P0.CHILD_PRODUCT_ID AND P0.PRODUCT_LEVEL_ID=3
				tempSql = tempSql.replaceAll("<product_hierarchy_query>", product_hierarchy_query );
				tempSql = tempSql.replaceAll("<parent_level>", parentLevelID+"");
				tempSql = tempSql.replaceAll("<exclude_category>", excludeCategory+"");
			
				sql.append(tempSql);
				
				//SQL to insert product summary daily data
				logger.debug("moveTempToSalesAggregation SQL:" + tempSql);
				PristineDBUtil.executeUpdate(_Conn, sql , "moveTempToSalesAggregation");
				
				//Update Visit count and Average order size
				tempSql = summary_daily_visitcount;
				tempSql = tempSql.replaceAll("<cal_id>", calendarId + "");
				tempSql = tempSql.replaceAll("<store_no>", compStrNo + "");
				tempSql = tempSql.replaceAll("<district_no>", districtNum + "");
				tempSql = tempSql.replaceAll("<product_hierarchy_query>", product_hierarchy_query );
				tempSql = tempSql.replaceAll("<parent_level>", parentLevelID+"");
				tempSql = tempSql.replaceAll("<exclude_category>", excludeCategory+"");
				
				sql.setLength(0);sql.append(tempSql);
				CachedRowSet tablecolumns = PristineDBUtil.executeQuery(_Conn, sql, "visitCountUpdate");
				PreparedStatement psmt = _Conn.prepareStatement("UPDATE SALES_AGGR_DAILY set TOT_VISIT_CNT= ?, AVG_ORDER_SIZE= ROUND(TOT_REVENUE / ?, 2) " + 
						" WHERE CALENDAR_ID = "+calendarId+" AND LOCATION_LEVEL_ID = "+Constants.STORE_LEVEL_ID+" AND LOCATION_ID = ? AND PRODUCT_LEVEL_ID= "+parentLevelID+" AND PRODUCT_ID= ? ");
				
				while (tablecolumns.next()) {
					
					int comp_str_id = tablecolumns.getInt("COMP_STR_ID");
					int prodId = tablecolumns.getInt("PRODUCT_ID");
					int visitCount = tablecolumns.getInt("VISIT_COUNT");
					
					if(visitCount > 0 ){
						psmt.setInt(1, visitCount);
						psmt.setInt(2, visitCount);
						psmt.setInt(3, comp_str_id);
						psmt.setInt(4, prodId);
						psmt.addBatch();
					}
				}
				tablecolumns.close();
				psmt.executeBatch();
				psmt.close();
			}
			PristineDBUtil.commitTransaction(_Conn,	 "Commit Aggregation Temporary Insert process");
			
		} catch (SQLException e) {
			// TODO Auto-generated catch block
			logger.error("Error while UOM based volume update", e);
			throw new GeneralException("couponAdjustment", e);	
		}
		catch (GeneralException e) {
			logger.error("Error while inserting Temporary Aggregation process" + e);
			throw new GeneralException("insertTemporaryAggregation", e);		
		}
	}
	
	public void couponAdjustment(Connection _Conn, int calendarId, String compStrNo, String districtNum) throws GeneralException {
		StringBuffer sql = new StringBuffer();
		String tempSql = "SELECT MV.COMP_STR_NO, MV.POS_DEPT, MV.ITEM_CODE,SUM(DECODE(mv.CPN_WEIGHT, 0, DECODE(mv.CPN_QTY, 0, 1, mv.CPN_QTY ), mv.CPN_WEIGHT) * mv.PRICE) AS COUPONPRICE " + 
		"FROM MOV_DAILY_COUPON_INFO MV WHERE MV.CALENDAR_ID = "+calendarId+" AND   MV.COMP_STR_NO IN ( " + 
		" SELECT comp_str_no from competitor_store where comp_str_no='"+compStrNo+"' or district_id = "+districtNum+"  ) AND " +
		" MV.POS_DEPT NOT IN("+Constants.GASPOSDEPARTMENT + ") AND MV.CPN_TYPE  <> 6 AND MV.UPC = '000000000000' GROUP BY MV.COMP_STR_NO, MV.POS_DEPT, MV.ITEM_CODE ";
		
		sql.append(tempSql);
		
		try{
			logger.debug("couponAdjustment SQL:" + tempSql);
			CachedRowSet tablecolumns = PristineDBUtil.executeQuery(_Conn, sql, "couponAdjustment");
			PreparedStatement psmt = _Conn.prepareStatement("UPDATE SALES_AGGR_DAILY_MOVEMENT_TEMP SET REVENUEREGULAR = REVENUEREGULAR - (CASE WHEN SALE_FLAG='Y' THEN ? ELSE 0 END) " +
					" WHERE ITEM_CODE = ? AND COMP_STR_NO = ? AND POS_DEPARTMENT = ? ");
		
			while (tablecolumns.next()) {
				String comp_str_no = tablecolumns.getString("COMP_STR_NO");
				int posDept = tablecolumns.getInt("POS_DEPT");
				int itemCode = tablecolumns.getInt("ITEM_CODE");
				double price = tablecolumns.getDouble("COUPONPRICE");
				
				psmt.setDouble(1, price);
				psmt.setInt(2, itemCode);
				psmt.setString(3, comp_str_no);
				psmt.setInt(4, posDept);
				psmt.addBatch();
				
				logger.debug("Competitor store no " + comp_str_no + ", posDept " + posDept + ", itemCode " + itemCode + ", price " + price);
			}
			tablecolumns.close();
			psmt.executeBatch();
			psmt.close();
			
			//Refresh the regular revenue based calculations
			tempSql = "UPDATE SALES_AGGR_DAILY_MOVEMENT_TEMP SET REG_IGVOL_REVENUE = REVENUEREGULAR WHERE UOM_ID IS NULL OR UOM_ID IN (SELECT ID FROM UOM_LOOKUP WHERE OUNCE IS NULL); " + 
					"UPDATE SALES_AGGR_DAILY_MOVEMENT_TEMP SET TOT_IGVOL_REVENUE = (REG_IGVOL_REVENUE + SALE_IGVOL_REVENUE) WHERE UOM_ID IS NULL OR UOM_ID IN (SELECT ID FROM UOM_LOOKUP WHERE OUNCE IS NULL) ; "+ 
					"UPDATE SALES_AGGR_DAILY_MOVEMENT_TEMP SET REG_MARGIN = REVENUEREGULAR - REG_DEAL_COST; " + 
					"UPDATE SALES_AGGR_DAILY_MOVEMENT_TEMP SET TOT_MOVEMENT = (REG_MOVEMENT + SALE_MOVEMENT); " + 
					"UPDATE SALES_AGGR_DAILY_MOVEMENT_TEMP SET TOT_REVENUE = (REVENUEREGULAR + REVENUESALE); " + 
					"UPDATE SALES_AGGR_DAILY_MOVEMENT_TEMP SET TOT_MARGIN = (REG_MARGIN + SALE_MARGIN); " + 
					"UPDATE SALES_AGGR_DAILY_MOVEMENT_TEMP SET TOT_MOVEMENT_VOL = (REG_MOVEMENT_VOL + SALE_MOVEMENT_VOL); "  ;
			
			tempSql = tempSql.trim();
			if(tempSql.endsWith(";")) tempSql = tempSql.substring(0, tempSql.lastIndexOf(";") );
			
			for(String query: tempSql.split(";")){
				sql.setLength(0);sql.append(query);
				logger.debug("couponAdjustment SQL:" + query);
				PristineDBUtil.executeUpdate(_Conn, sql , "couponAdjustment");
			}
			
			PristineDBUtil.commitTransaction(_Conn,	 "Commit Aggregation Movement Volume Update process");
		} catch (SQLException e) {
			// TODO Auto-generated catch block
			logger.error("Error while UOM based volume update", e);
			throw new GeneralException("couponAdjustment", e);	
		}

		catch (GeneralException e) {
			logger.error("Error while inserting Temporary Aggregation process" + e);
			throw new GeneralException("couponAdjustment", e);		
		}
	}
		
	public void moveTempToStoreSalesAggregation(Connection _Conn, int calendarId, String compStrNo, 
			String districtNum, HashMap<Integer,Integer> gasItems, int excludeCategory) throws GeneralException {
		
		StringBuffer sql = new StringBuffer();
		String tempSql = store_summary_daily_insert_query;
		
		String itemCodeString = "0";
		for(Integer i: gasItems.keySet())	itemCodeString += "," + i;
		
		try {
				
			//store level summary
			tempSql = tempSql.replaceAll("<cal_id>", calendarId + "");
			tempSql = tempSql.replaceAll("<store_no>", compStrNo + "");
			tempSql = tempSql.replaceAll("<district_no>", districtNum + "");
			tempSql = tempSql.replaceAll("<exclude_category>", excludeCategory+"");
			tempSql = tempSql.replaceAll("<gas_items>", itemCodeString+"");
			
			sql.setLength(0);
			sql.append(tempSql);
			
			//SQL to insert store summary daily data
			logger.debug("moveTempToStoreSalesAggregation SQL:" + tempSql);
			PristineDBUtil.executeUpdate(_Conn, sql , "moveTempToSalesAggregation");
			PristineDBUtil.commitTransaction(_Conn,	 "Commit Aggregation Temporary Insert process");
			
		} 
		catch (GeneralException e) {
			logger.error("Error while inserting Temporary Aggregation process" + e);
			throw new GeneralException("insertTemporaryAggregation", e);		
		}
	}
	
	public void moveTempToPOSSalesAggregation(Connection _Conn, int calendarId, String compStrNo, 
			String districtNum, HashMap<Integer,Integer> gasItems, int excludeCategory) throws GeneralException {
		//Aggregation is not required for POS department. The following code can be enabled if the Aggregation Required flag is enabled
		// in the product_group_type table for product level 10
		
		/*
		StringBuffer sql = new StringBuffer();
		String tempSql = pos_insert_query;
		
		String itemCodeString = "0";
		for(Integer i: gasItems.keySet())	itemCodeString += "," + i;
		
		try {
				
			//store level summary
			tempSql = tempSql.replaceAll("<item_code_list>", itemCodeString );
			tempSql = tempSql.replaceAll("<cal_id>", calendarId + "");
			tempSql = tempSql.replaceAll("<store_no>", compStrNo + "");
			tempSql = tempSql.replaceAll("<district_no>", districtNum + "");
			tempSql = tempSql.replaceAll("<exclude_category>", excludeCategory+"");
			
			sql.setLength(0);
			sql.append(tempSql);
			
			//SQL to insert store summary daily data
			logger.debug("moveTempToPOSSalesAggregation SQL:" + tempSql);
			PristineDBUtil.executeUpdate(_Conn, sql , "moveTempToPOSSalesAggregation");
			
			
			//Update Visit count and Average order size
			tempSql = pos_visit_count;
			tempSql = tempSql.replaceAll("<item_code_list>", itemCodeString );
			tempSql = tempSql.replaceAll("<cal_id>", calendarId + "");
			tempSql = tempSql.replaceAll("<store_no>", compStrNo + "");
			tempSql = tempSql.replaceAll("<district_no>", districtNum + "");
			tempSql = tempSql.replaceAll("<parent_level>", Constants.POSDEPARTMENT+"");
			tempSql = tempSql.replaceAll("<exclude_category>", excludeCategory+"");
			
			sql.setLength(0);sql.append(tempSql);
			CachedRowSet tablecolumns = PristineDBUtil.executeQuery(_Conn, sql, "moveTempToPOSSalesAggregation");
			PreparedStatement psmt = _Conn.prepareStatement("UPDATE SALES_AGGR_DAILY set TOT_VISIT_CNT= ?, AVG_ORDER_SIZE= ROUND(TOT_REVENUE / ?, 2) " + 
					" WHERE CALENDAR_ID = "+calendarId+" AND LOCATION_LEVEL_ID = "+Constants.STORE_LEVEL_ID+" AND LOCATION_ID = ? AND PRODUCT_LEVEL_ID= "+Constants.POSDEPARTMENT+" AND PRODUCT_ID= ? ");
			
			while (tablecolumns.next()) {
				
				//,P0. , COUNT(DISTINCT M.TRANSACTION_NO) as 
				
				int comp_str_id = tablecolumns.getInt("COMP_STR_ID");
				int prodId = tablecolumns.getInt("PRODUCT_ID");
				int visitCount = tablecolumns.getInt("VISIT_COUNT");
				
				if(visitCount > 0 ){
					psmt.setInt(1, visitCount);
					psmt.setInt(2, visitCount);
					psmt.setInt(3, comp_str_id);
					psmt.setInt(4, prodId);
					psmt.addBatch();
				}
			}
			tablecolumns.close();
			psmt.executeBatch();
			psmt.close();
			
			PristineDBUtil.commitTransaction(_Conn,	 "Commit");
			
		} catch (SQLException e) {
			// TODO Auto-generated catch block
			logger.error("Error while marking process completion", e);
			throw new GeneralException("markProcessCompletionForMovementDaily", e);	
		}
		catch (GeneralException e) {
			logger.error("Error while inserting Temporary Aggregation process" + e);
			throw new GeneralException("moveTempToPOSSalesAggregation", e);		
		}*/
	}
	
	public HashMap<String, Integer> UpdateLastYearSalesAggrList(Connection conn, int currCalendarId, int lstCalendarId) throws GeneralException {
		HashMap<String, Integer> lastSummaryList = new HashMap<String, Integer>();
		 
		StringBuffer sql = new StringBuffer();
		sql.append(" select LOCATION_LEVEL_ID,LOCATION_ID,PRODUCT_LEVEL_ID,PRODUCT_ID,SUMMARY_DAILY_ID ");
		sql.append(" from SALES_AGGR_DAILY where CALENDAR_ID="+lstCalendarId+"");
		
		logger.debug("UpdateLastYearSalesAggrList SQL:" + sql.toString());
		try{
		   
			CachedRowSet tablecolumns = PristineDBUtil.executeQuery(conn, sql, "getLastYearSalesAggrList");

			PreparedStatement psmt = conn.prepareStatement("UPDATE SALES_AGGR_DAILY SET lst_summary_daily_id =  ?  WHERE LOCATION_LEVEL_ID = ? AND LOCATION_ID = ? AND " +
					"( (PRODUCT_LEVEL_ID = ? AND PRODUCT_ID= ?) OR ( ? = 0 AND PRODUCT_ID IS NULL AND PRODUCT_LEVEL_ID IS NULL ) )  AND CALENDAR_ID = ? ");
			
			while (tablecolumns.next()) {
				int locId = tablecolumns.getInt("LOCATION_ID");
				int locLevelId = tablecolumns.getInt("LOCATION_LEVEL_ID");
				int prodLevelId = tablecolumns.getInt("PRODUCT_LEVEL_ID");
				int prodId = tablecolumns.getInt("PRODUCT_ID");
				int lastSummaryId = tablecolumns.getInt("SUMMARY_DAILY_ID");
				
				psmt.setInt(1, lastSummaryId);
				psmt.setInt(2, locLevelId);
				psmt.setInt(3, locId);
				psmt.setInt(4, prodLevelId);
				psmt.setInt(5, prodId);
				psmt.setInt(6, prodId);
				psmt.setInt(7, currCalendarId);
				
				psmt.addBatch();
			}
			tablecolumns.close();
			psmt.executeBatch();
			psmt.close();
			
			PristineDBUtil.commitTransaction(conn,	 "Commit");
		}catch(Exception exe){
			logger.error(exe);
			throw new GeneralException("UpdateLastYearSalesAggrList Method Error");
		} 
		 
		return lastSummaryList;
	}
	
	public void markProcessCompletionForMovementDaily(Connection _Conn) throws GeneralException {
		StringBuffer sql = new StringBuffer();
		sql.append(" select calendar_id,comp_str_no from SALES_AGGR_DAILY_MOVEMENT_TEMP  group by calendar_id,comp_str_no ");
		
		logger.debug("markProcessCompletionForMovementDaily SQL:" + sql.toString());
		try{
		   
			CachedRowSet tablecolumns = PristineDBUtil.executeQuery(_Conn, sql, "getLastYearSalesAggrList");

			PreparedStatement psmt = _Conn.prepareStatement("update MOVEMENT_DAILY set PROCESSED_FLAG='Y' WHERE CALENDAR_ID = ? AND comp_str_no = ? ");
			
			while (tablecolumns.next()) {
				int calendar_id = tablecolumns.getInt("calendar_id");
				String comp_str_no = tablecolumns.getString("comp_str_no");
				
				psmt.setInt(1, calendar_id);
				psmt.setString(2, comp_str_no);
				
				psmt.addBatch();
			}
			tablecolumns.close();
			psmt.executeBatch();
			psmt.close();
			
			PristineDBUtil.commitTransaction(_Conn,	 "Commit");
		} catch (SQLException e) {
			// TODO Auto-generated catch block
			logger.error("Error while marking process completion", e);
			throw new GeneralException("markProcessCompletionForMovementDaily", e);	
		}
		catch (GeneralException e) {
			logger.error("Error while update process" + e);
			throw new GeneralException("markProcessCompletionForMovementDaily", e);		
		}
	}
	
	public void addNoCostItemsInfoFromTemp(Connection _Conn)
									throws GeneralException {
		StringBuffer sql = new StringBuffer();
		sql.append(" insert into SA_NOCOST_ITEM (CALENDAR_ID, LOCATION_LEVEL_ID, LOCATION_ID, PRODUCT_LEVEL_ID, PRODUCT_ID, TOT_REVENUE) " +
				" select calendar_id,"+Constants.STORE_LEVEL_ID+" location_level_id,CS.COMP_STR_ID location_id,"+Constants.ITEMLEVELID+" product_level_id," +
				" item_code product_id, (RevenueRegular+RevenueSale) Revenue from  SALES_AGGR_DAILY_MOVEMENT_TEMP SADMT INNER JOIN COMPETITOR_STORE CS " +
				" ON CS.COMP_STR_NO = SADMT.COMP_STR_NO where sale_deal_cost = 0 and reg_deal_cost = 0 ");
		
		logger.debug("addNoCostItemsInfoFromTemp SQL:" + sql.toString());
		try
		{
			PristineDBUtil.executeUpdate(_Conn, sql , "addNoCostItemsInfoFromTemp");
			PristineDBUtil.commitTransaction(_Conn,	 "Commit");		 
		}
		catch(Exception exe) {
			logger.error("No cost data Insert Error" , exe);
			throw new GeneralException("No cost data Insert Error" , exe);
		}
	}
}
