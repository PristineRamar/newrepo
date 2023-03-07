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

import com.pristine.business.entity.SalesaggregationbusinessV2;
import com.pristine.dao.DBManager;
import com.pristine.dao.RetailCalendarDAO;
import com.pristine.dto.RetailCalendarDTO;
import com.pristine.dto.SummaryDailyDTO;
import com.pristine.dto.salesanalysis.PeriodCalendarDTO;
import com.pristine.dto.salesanalysis.ProductGroupDTO;
import com.pristine.dto.salesanalysis.SummaryDataDTO;
import com.pristine.exception.GeneralException;
import com.pristine.util.Constants;
import com.pristine.util.GenericUtil;
import com.pristine.util.PristineDBUtil;
import com.pristine.util.DateUtil;

public class SalesAggregationRollupDaoV3 {
	
	static Logger logger = Logger.getLogger("SalesAggregationRollUpDaoV3");
	
	private String SourceTable = "SALES_AGGR_DAILY_ROLLUP" ;
	private String SourceRowType = "D" ;
	
	static String select_query_daily = " SELECT 0,<cal_id> CALENDAR_ID,<location_level_id> LOCATION_LEVEL_ID,<location_id> LOCATION_ID,AL.PRODUCT_LEVEL_ID,AL.PRODUCT_ID, " +
	 "  SUM(AL.TOT_VISIT_CNT) TOT_VISIT_CNT, SUM(AL.TOT_MOVEMENT) TOT_MOVEMENT,SUM(AL.REG_MOVEMENT) REG_MOVEMENT,SUM(AL.SALE_MOVEMENT) SALE_MOVEMENT,SUM(AL.TOT_REVENUE) TOT_REVENUE,SUM(AL.REG_REVENUE) REG_REVENUE,SUM(AL.SALE_REVENUE) SALE_REVENUE,  " + 
	 "  SUM(AL.TOT_MARGIN) TOT_MARGIN,SUM(AL.REG_MARGIN) REG_MARGIN,SUM(AL.SALE_MARGIN) SALE_MARGIN,  " +
	 "  SUM(AL.TOT_MOVEMENT_VOL) TOT_MOVEMENT_VOL, SUM(AL.REG_MOVEMENT_VOL) REG_MOVEMENT_VOL, SUM(AL.SALE_MOVEMENT_VOL) SALE_MOVEMENT_VOL, SUM(AL.TOT_IGVOL_REVENUE) TOT_IGVOL_REVENUE, SUM(AL.REG_IGVOL_REVENUE) REG_IGVOL_REVENUE,SUM(AL.SALE_IGVOL_REVENUE) SALE_IGVOL_REVENUE, " + 
	 "  SUM(I.TOT_VISIT_CNT) ID_TOT_VISIT_CNT, SUM(I.TOT_MOVEMENT) ID_TOT_MOVEMENT,SUM(I.REG_MOVEMENT) ID_REG_MOVEMENT,SUM(I.SALE_MOVEMENT) ID_SALE_MOVEMENT,SUM(I.TOT_REVENUE) ID_TOT_REVENUE,SUM(I.REG_REVENUE) ID_REG_REVENUE,SUM(I.SALE_REVENUE) ID_SALE_REVENUE,  " + 
	 "  SUM(I.TOT_MARGIN) ID_TOT_MARGIN,SUM(I.REG_MARGIN) ID_REG_MARGIN,SUM(I.SALE_MARGIN) ID_SALE_MARGIN,  " +
	 "  SUM(I.TOT_MOVEMENT_VOL) ID_TOT_MOVEMENT_VOL, SUM(I.REG_MOVEMENT_VOL) ID_REG_MOVEMENT_VOL, SUM(I.SALE_MOVEMENT_VOL) ID_SALE_MOVEMENT_VOL, SUM(I.TOT_IGVOL_REVENUE) ID_TOT_IGVOL_REVENUE, SUM(I.REG_IGVOL_REVENUE) ID_REG_IGVOL_REVENUE,SUM(I.SALE_IGVOL_REVENUE) ID_SALE_IGVOL_REVENUE, " +
	 "  MAX(<last_proc_cal_id>) AS LAST_PROCESSESD_CALENDAR_ID, " +
	 "  MIN(LST.<table_name>_id) LST_ID "+
	 " FROM SALES_AGGR_DAILY AL LEFT JOIN SALES_AGGR_DAILY I " + 
	 "    ON AL.CALENDAR_ID = I.CALENDAR_ID AND AL.LOCATION_LEVEL_ID = I.LOCATION_LEVEL_ID AND AL.LOCATION_ID = I.LOCATION_ID AND " + 
	 "    (AL.PRODUCT_LEVEL_ID = I.PRODUCT_LEVEL_ID OR (AL.PRODUCT_LEVEL_ID IS NULL AND I.PRODUCT_LEVEL_ID IS NULL) ) AND " +
	 "    (AL.PRODUCT_ID = I.PRODUCT_ID OR (AL.PRODUCT_ID IS NULL AND I.PRODUCT_ID IS NULL) ) AND I.LOCATION_ID IN   " + 
	 "    (  <identical_store_query> )  " + 
	 " LEFT JOIN <table_name> LST ON " + 
	 " 	  LST.LOCATION_LEVEL_ID = <location_level_id> " + 
	 " 	  AND LST.LOCATION_ID = <location_id> " + 
	 " 	  AND (AL.PRODUCT_LEVEL_ID = LST.PRODUCT_LEVEL_ID OR (AL.PRODUCT_LEVEL_ID IS NULL AND LST.PRODUCT_LEVEL_ID IS NULL))" + 
	 " 	  AND (AL.PRODUCT_ID = LST.PRODUCT_ID OR (AL.PRODUCT_ID IS NULL AND LST.PRODUCT_ID IS NULL))" +
	 " 	  AND LST.CALENDAR_ID = <lst_cal_id> " +
	 " WHERE AL.CALENDAR_ID IN (select calendar_id from retail_calendar where start_date >= to_date('<start_date>','"+ Constants.DB_DATE_FORMAT + "') " + 
	 "  AND start_date <= to_date('<end_date>','"+ Constants.DB_DATE_FORMAT + "') and row_type='D') AND AL.LOCATION_ID IN   " + 
	 "  (  <store_query> )  " + 
	 "  GROUP BY AL.PRODUCT_LEVEL_ID,AL.PRODUCT_ID  "  ;
	
	static String insert_query_daily_dest = " INSERT INTO <table_name>( <table_name>_id,CALENDAR_ID,LOCATION_LEVEL_ID,LOCATION_ID,PRODUCT_LEVEL_ID,PRODUCT_ID,TOT_VISIT_CNT, " +
	 " TOT_MOVEMENT,REG_MOVEMENT,SALE_MOVEMENT,TOT_REVENUE,REG_REVENUE,SALE_REVENUE,AVG_ORDER_SIZE,TOT_MARGIN,REG_MARGIN,SALE_MARGIN,TOT_MARGIN_PCT, " +
	 " REG_MARGIN_PCT,SALE_MARGIN_PCT,lst_sales_aggr_rollup_id,TOT_MOVEMENT_VOL,REG_MOVEMENT_VOL,SALE_MOVEMENT_VOL,TOT_IGVOL_REVENUE, " +
	 " REG_IGVOL_REVENUE,SALE_IGVOL_REVENUE,ID_TOT_VISIT_CNT, " +
	 " ID_TOT_MOVEMENT,id_reg_moevemnt,ID_SALE_MOVEMENT,ID_TOT_REVENUE,ID_REG_REVENUE,ID_SALE_REVENUE,ID_AVG_ORDER_SIZE,ID_TOT_MARGIN,ID_REG_MARGIN,ID_SALE_MARGIN,ID_TOT_MARGIN_PCT, " +
	 " ID_REG_MARGIN_PCT,ID_SALE_MARGIN_PCT,ID_TOT_MOVEMENT_VOL,ID_REG_MOVEMENT_VOL,ID_SALE_MOVEMENT_VOL,ID_TOT_IGVOL_REVENUE, " +
	 " ID_REG_IGVOL_REVENUE,ID_SALE_IGVOL_REVENUE) VALUES( "+DBManager.getIdentityIncrement("<table_name>_seq") +",?,?,?,?,?,?, " +
	 " ?,?,?,?,?,?,?,?,?,?,?, ?,?,?,?,?,?,?,  ?,?,?,  ?,?,?,?,?,?,?,?,?,?,?,  ?,?,?,?,?,?,  ?,?)"; 
	
	static String select_query = " SELECT 0,<cal_id> CALENDAR_ID,<location_level_id> LOCATION_LEVEL_ID,<location_id> LOCATION_ID,AL.PRODUCT_LEVEL_ID,AL.PRODUCT_ID, " +
	"  SUM(AL.TOT_VISIT_CNT) TOT_VISIT_CNT, SUM(AL.TOT_MOVEMENT) TOT_MOVEMENT,SUM(AL.REG_MOVEMENT) REG_MOVEMENT,SUM(AL.SALE_MOVEMENT) SALE_MOVEMENT,SUM(AL.TOT_REVENUE) TOT_REVENUE,SUM(AL.REG_REVENUE) REG_REVENUE,SUM(AL.SALE_REVENUE) SALE_REVENUE,  " + 
	"  SUM(AL.TOT_MARGIN) TOT_MARGIN,SUM(AL.REG_MARGIN) REG_MARGIN,SUM(AL.SALE_MARGIN) SALE_MARGIN,  " +
	"  SUM(AL.TOT_MOVEMENT_VOL) TOT_MOVEMENT_VOL, SUM(AL.REG_MOVEMENT_VOL) REG_MOVEMENT_VOL, SUM(AL.SALE_MOVEMENT_VOL) SALE_MOVEMENT_VOL, SUM(AL.TOT_IGVOL_REVENUE) TOT_IGVOL_REVENUE, SUM(AL.REG_IGVOL_REVENUE) REG_IGVOL_REVENUE,SUM(AL.SALE_IGVOL_REVENUE) SALE_IGVOL_REVENUE, " + 
	"  SUM(AL.ID_TOT_VISIT_CNT) ID_TOT_VISIT_CNT, SUM(AL.ID_TOT_MOVEMENT) ID_TOT_MOVEMENT,SUM(AL.id_reg_moevemnt) ID_REG_MOVEMENT,SUM(AL.ID_SALE_MOVEMENT) ID_SALE_MOVEMENT,SUM(AL.ID_TOT_REVENUE) ID_TOT_REVENUE,SUM(AL.ID_REG_REVENUE) ID_REG_REVENUE,SUM(AL.ID_SALE_REVENUE) ID_SALE_REVENUE,  " + 
	"  SUM(AL.ID_TOT_MARGIN) ID_TOT_MARGIN,SUM(AL.ID_REG_MARGIN) ID_REG_MARGIN,SUM(AL.ID_SALE_MARGIN) ID_SALE_MARGIN,  " +
	"  SUM(AL.ID_TOT_MOVEMENT_VOL) ID_TOT_MOVEMENT_VOL, SUM(AL.ID_REG_MOVEMENT_VOL) ID_REG_MOVEMENT_VOL, SUM(AL.ID_SALE_MOVEMENT_VOL) ID_SALE_MOVEMENT_VOL, SUM(AL.ID_TOT_IGVOL_REVENUE) ID_TOT_IGVOL_REVENUE, SUM(AL.ID_REG_IGVOL_REVENUE) ID_REG_IGVOL_REVENUE,SUM(AL.ID_SALE_IGVOL_REVENUE) ID_SALE_IGVOL_REVENUE, " + 

	" SUM(AL.PL_TOT_REVENUE) AS PL_TOT_REVENUE," +
	" SUM(AL.PL_REG_REVENUE) AS PL_REG_REVENUE," +
	" SUM(AL.PL_SALE_REVENUE) AS PL_SALE_REVENUE," + 
	" SUM(AL.PL_TOT_MARGIN) AS PL_TOT_MARGIN," +
	" SUM(AL.PL_REG_MARGIN) AS PL_REG_MARGIN," + 
	" SUM(AL.PL_SALE_MARGIN) AS PL_SALE_MARGIN," + 
	" SUM(AL.PL_TOT_MOVEMENT) AS PL_TOT_MOVEMENT," +
	" SUM(AL.PL_REG_MOVEMENT) AS PL_REG_MOVEMENT," +
	" SUM(AL.PL_SALE_MOVEMENT) AS PL_SALE_MOVEMENT," +
	" SUM(AL.PL_TOT_MOVEMENT_VOL) AS PL_TOT_MOVEMENT_VOL," + 
	" SUM(AL.PL_REG_MOVEMENT_VOL) AS PL_REG_MOVEMENT_VOL," +
	" SUM(AL.PL_SALE_MOVEMENT_VOL) AS PL_SALE_MOVEMENT_VOL," +
	" SUM(AL.PL_TOT_VISIT_CNT) AS PL_TOT_VISIT_CNT," + 
	" SUM(AL.PL_ID_TOT_REVENUE) AS PL_ID_TOT_REVENUE," +
	" SUM(AL.PL_ID_REG_REVENUE) AS PL_ID_REG_REVENUE," +
	" SUM(AL.PL_ID_SALE_REVENUE) AS PL_ID_SALE_REVENUE, " +
	" SUM(AL.PL_ID_TOT_MARGIN) AS PL_ID_TOT_MARGIN," +
	" SUM(AL.PL_ID_REG_MARGIN) AS PL_ID_REG_MARGIN," +
	" SUM(AL.PL_ID_SALE_MARGIN) AS PL_ID_SALE_MARGIN," +
	" SUM(AL.PL_ID_TOT_MOVEMENT) AS PL_ID_TOT_MOVEMENT," +
	" SUM(AL.PL_id_reg_MOVEMENT) AS PL_ID_REG_MOVEMENT," +
	" SUM(AL.PL_ID_SALE_MOVEMENT) AS PL_ID_SALE_MOVEMENT," +
	" SUM(AL.PL_ID_TOT_MOVEMENT_VOL) AS PL_ID_TOT_MOVEMENT_VOL," + 
	" SUM(AL.PL_ID_REG_MOVEMENT_VOL) AS PL_ID_REG_MOVEMENT_VOL, " +
	" SUM(AL.PL_ID_SALE_MOVEMENT_VOL) AS PL_ID_SALE_MOVEMENT_VOL, " +
	" SUM(AL.PL_ID_TOT_VISIT_CNT) AS PL_ID_TOT_VISIT_CNT," +
	
	"  MAX(<last_proc_cal_id>) AS LAST_PROCESSESD_CALENDAR_ID, " +
	"  MIN(LST.<table_name>_id) LST_ID "+ 
	" FROM <source_table> AL " + 
	" LEFT JOIN <table_name> LST ON " + 
	" 	  LST.LOCATION_LEVEL_ID = <location_level_id> " + 
	 " 	  AND LST.LOCATION_ID = <location_id> " + 
	" 	  AND (AL.PRODUCT_LEVEL_ID = LST.PRODUCT_LEVEL_ID OR (AL.PRODUCT_LEVEL_ID IS NULL AND LST.PRODUCT_LEVEL_ID IS NULL))" + 
	" 	  AND (AL.PRODUCT_ID = LST.PRODUCT_ID OR (AL.PRODUCT_ID IS NULL AND LST.PRODUCT_ID IS NULL))" +
	" 	  AND LST.CALENDAR_ID = <lst_cal_id> " +
	" WHERE AL.CALENDAR_ID IN (select calendar_id from retail_calendar where start_date >= to_date('<start_date>','"+ Constants.DB_DATE_FORMAT + "') " + 
	" AND start_date <= to_date('<end_date>','"+ Constants.DB_DATE_FORMAT + "') and row_type='<source_row_type>') AND AL.LOCATION_LEVEL_ID = <source_location_level_id> AND AL.LOCATION_ID IN( <source_location_id>)" + 
	" GROUP BY AL.PRODUCT_LEVEL_ID,AL.PRODUCT_ID "  ;
	
	static String insert_query_dest = " INSERT INTO <table_name>( <table_name>_id,CALENDAR_ID,LOCATION_LEVEL_ID,LOCATION_ID,PRODUCT_LEVEL_ID,PRODUCT_ID,TOT_VISIT_CNT, " +
	 " TOT_MOVEMENT,REG_MOVEMENT,SALE_MOVEMENT,TOT_REVENUE,REG_REVENUE,SALE_REVENUE,AVG_ORDER_SIZE,TOT_MARGIN,REG_MARGIN,SALE_MARGIN,TOT_MARGIN_PCT, " +
	 " REG_MARGIN_PCT,SALE_MARGIN_PCT,lst_sales_aggr_rollup_id,TOT_MOVEMENT_VOL,REG_MOVEMENT_VOL,SALE_MOVEMENT_VOL,TOT_IGVOL_REVENUE, " +
	 " REG_IGVOL_REVENUE,SALE_IGVOL_REVENUE,ID_TOT_VISIT_CNT, " +
	 " ID_TOT_MOVEMENT,id_reg_moevemnt,ID_SALE_MOVEMENT,ID_TOT_REVENUE,ID_REG_REVENUE,ID_SALE_REVENUE,ID_AVG_ORDER_SIZE,ID_TOT_MARGIN,ID_REG_MARGIN,ID_SALE_MARGIN,ID_TOT_MARGIN_PCT, " +
	 " ID_REG_MARGIN_PCT,ID_SALE_MARGIN_PCT,ID_TOT_MOVEMENT_VOL,ID_REG_MOVEMENT_VOL,ID_SALE_MOVEMENT_VOL,ID_TOT_IGVOL_REVENUE, " +
	 " ID_REG_IGVOL_REVENUE,ID_SALE_IGVOL_REVENUE, LAST_AGGR_CALENDARID," +
	 
	 " PL_TOT_REVENUE, PL_REG_REVENUE, PL_SALE_REVENUE," +
	 " PL_TOT_MARGIN, PL_REG_MARGIN, PL_SALE_MARGIN," +
	 " PL_TOT_MARGIN_PCT, PL_REG_MARGIN_PCT, PL_SALE_MARGIN_PCT," +
	 " PL_TOT_MOVEMENT, PL_REG_MOVEMENT, PL_SALE_MOVEMENT," +
	 " PL_TOT_MOVEMENT_VOL, PL_REG_MOVEMENT_VOL, PL_SALE_MOVEMENT_VOL," +
	 " PL_TOT_VISIT_CNT, PL_AVG_ORDER_SIZE,"+

	 " PL_ID_TOT_REVENUE, PL_ID_REG_REVENUE, PL_ID_SALE_REVENUE," +
	 " PL_ID_TOT_MARGIN, PL_ID_REG_MARGIN, PL_ID_SALE_MARGIN," +
	 " PL_ID_TOT_MARGIN_PCT, PL_ID_REG_MARGIN_PCT, PL_ID_SALE_MARGIN_PCT," +
	 " PL_ID_TOT_MOVEMENT, PL_ID_REG_MOVEMENT, PL_ID_SALE_MOVEMENT," + 
	 " PL_ID_TOT_MOVEMENT_VOL, PL_ID_REG_MOVEMENT_VOL, PL_ID_SALE_MOVEMENT_VOL," +
	 " PL_ID_TOT_VISIT_CNT, PL_ID_AVG_ORDER_SIZE)" +
	 
	 " VALUES( "+DBManager.getIdentityIncrement("<table_name>_seq") +",?,?,?,?,?,?,  ?,?,?,?,?,?,?,?,?,?,?,  ?,?,?,?,?,?,?, " +
	 " ?,?,?, ?,?,?,?,?,?,?,?,?,?,?, ?,?,?,?,?,?, ?,?,?," +
	 " ?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?," +
	 " ?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)";
	
	static String update_query_dest = " UPDATE <table_name> SET " +
			 " PL_TOT_REVENUE=?, PL_REG_REVENUE=?, PL_SALE_REVENUE=?," +
			 " PL_TOT_MARGIN=?, PL_REG_MARGIN=?, PL_SALE_MARGIN=?," +
			 " PL_TOT_MARGIN_PCT=?, PL_REG_MARGIN_PCT=?, PL_SALE_MARGIN_PCT=?," +
			 " PL_TOT_MOVEMENT=?, PL_REG_MOVEMENT=?, PL_SALE_MOVEMENT=?," +
			 " PL_TOT_MOVEMENT_VOL=?, PL_REG_MOVEMENT_VOL=?, PL_SALE_MOVEMENT_VOL=?," +
			 " PL_TOT_VISIT_CNT=?, PL_AVG_ORDER_SIZE=?,"+
			 
			 " PL_ID_TOT_REVENUE=?, PL_ID_REG_REVENUE=?, PL_ID_SALE_REVENUE=?," +
			 " PL_ID_TOT_MARGIN=?, PL_ID_REG_MARGIN=?, PL_ID_SALE_MARGIN=?," +
			 " PL_ID_TOT_MARGIN_PCT=?, PL_ID_REG_MARGIN_PCT=?, PL_ID_SALE_MARGIN_PCT=?," +
			 " PL_ID_TOT_MOVEMENT=?, PL_ID_REG_MOVEMENT=?, PL_ID_SALE_MOVEMENT=?," + 
			 " PL_ID_TOT_MOVEMENT_VOL=?, PL_ID_REG_MOVEMENT_VOL=?, PL_ID_SALE_MOVEMENT_VOL=?," +
			 " PL_ID_TOT_VISIT_CNT=?, PL_ID_AVG_ORDER_SIZE=? " +
			 
			 " WHERE CALENDAR_ID = ? AND LOCATION_LEVEL_ID = ? AND LOCATION_ID = ?" + 
			 " AND NVL(PRODUCT_LEVEL_ID, -1) = ? AND NVL(PRODUCT_ID, -1) = ?";	
	
	//This method returns the Calendar Id of the last data loading
	public int getLastUpdatedCalendarId(Connection _Conn, String tableName,int locationLevelId, int locationId, String calendarMode){
		StringBuffer sql = new StringBuffer();
		//1. Retrieve the calendar id, when the last data loading happened
		sql.append(" SELECT CALENDAR_ID FROM RETAIL_CALENDAR WHERE ROW_TYPE='"+calendarMode+"' AND START_DATE IN " + 
				" (SELECT MAX(RC.START_DATE) from " + tableName + " T INNER JOIN RETAIL_CALENDAR RC " + 
				"  ON RC.CALENDAR_ID = T.CALENDAR_ID WHERE RC.ROW_TYPE='"+calendarMode+"' AND T.LOCATION_ID = " + locationId +
				"  and T.location_level_id = " + locationLevelId +
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
	 
	public void deletePreviousAggregation(Connection _Conn, PeriodCalendarDTO calendarDTO, String tableName, 
			int locationLevelId, int locationId) throws GeneralException {
		
		try {
			StringBuffer sql = new StringBuffer();
			
			//SQL to delete Sales Daily data
			sql.append(" Delete from "+tableName+" WHERE CALENDAR_ID = " + calendarDTO.getPeriodCalendarId());
			sql.append(" and LOCATION_ID =  " + locationId + "  and location_level_id = " + locationLevelId);
		 
		    // execute the Sales Aggr Daily data delete query
		    logger.debug("deletePreviousAggregation SQL:" +sql.toString());
		    PristineDBUtil.executeUpdate(_Conn, sql , "deletePreviousAggregation");
		 			
			PristineDBUtil.commitTransaction(_Conn,	 "Commit Aggregation delete process");
		} catch (GeneralException e) {
			 logger.error("Error while deleting previous Aggregation data" + e);
			throw new GeneralException("deletePreviousAggregation", e);		
		}
	}
	
	public void insertSalesAggregationDailyRollUp(Connection _Conn, PeriodCalendarDTO calendarDTO, String tableName, 
			int locationLevelId, int locationId, boolean _cascadeEnabled, String quarterSource, boolean updateOnlyPLMetrics) throws GeneralException {
		
		if(_cascadeEnabled & (locationLevelId < Constants.DISTRICT_LEVEL_ID | "W,P,Q,Y".indexOf(calendarDTO.getCalendarMode()) > -1) ){
			processSalesAggregationRollUp(_Conn, calendarDTO, tableName, locationLevelId, locationId, _cascadeEnabled, quarterSource, updateOnlyPLMetrics);
			return;
		}
	
		String locationName = "";
		
		if(locationLevelId == Constants.CHAIN_LEVEL_ID) locationName = "comp_chain_id";
		else if(locationLevelId == Constants.DIVISION_LEVEL_ID) locationName = "division_id";
		else if(locationLevelId == Constants.REGION_LEVEL_ID) locationName = "region_id";
		else if(locationLevelId == Constants.DISTRICT_LEVEL_ID) locationName = "district_id";
		
		String allStores = "select cs.comp_str_id from competitor_store cs inner join competitor_chain cc on " + 
				" cs.comp_chain_id = cc.comp_chain_id and cc.presto_subscriber='Y' where cs." + locationName + " = " + locationId;
		
		String identicalStores = "select cs.comp_str_id from competitor_store cs inner join competitor_chain cc on " + 
				" cs.comp_chain_id = cc.comp_chain_id and cc.presto_subscriber='Y' where cs." + locationName + " = " + locationId + 
				"  and CS.OPEN_DATE < to_date('"+ calendarDTO.getPeriodStartDate() + "','"+Constants.DB_DATE_FORMAT+"')-365 ";
		
		String selectSql = select_query_daily.replaceAll("<cal_id>", calendarDTO.getPeriodCalendarId() + "");
		selectSql = selectSql.replaceAll("<lst_cal_id>", calendarDTO.getLastPeriodCalendarId() + "");
		selectSql = selectSql.replaceAll("<table_name>", tableName );
		selectSql = selectSql.replaceAll("<start_date>", calendarDTO.getPeriodStartDate());
		selectSql = selectSql.replaceAll("<end_date>", ( calendarDTO.getPeriodEndDate() == null || calendarDTO.getPeriodEndDate().equalsIgnoreCase("") ) ? 
				calendarDTO.getPeriodStartDate() : calendarDTO.getPeriodEndDate() );
		selectSql = selectSql.replaceAll("<location_level_id>", locationLevelId + "");
		selectSql = selectSql.replaceAll("<location_id>", locationId + "");
		selectSql = selectSql.replaceAll("<store_query>", allStores + "");
		selectSql = selectSql.replaceAll("<identical_store_query>", identicalStores + "");
		selectSql = selectSql.replaceAll("<source_table>", SourceTable + "");
		selectSql = selectSql.replaceAll("<source_row_type>", SourceRowType + "");
		selectSql = selectSql.replaceAll("<last_proc_cal_id>", "AL.CALENDAR_ID");
		
		logger.debug("Sales Aggregation Fetch SQL:" + selectSql);
		
		String insertSql = insert_query_daily_dest;
		insertSql = insertSql.replaceAll("<table_name>", tableName );
		
		SalesaggregationbusinessV2 businessLogic = new SalesaggregationbusinessV2();
		StringBuilder sb = new StringBuilder();
		try {
			StringBuffer sql = new StringBuffer();
			sql.append(selectSql);
			
			List<SummaryDataDTO> lstSummary = new ArrayList<SummaryDataDTO>(); 

			CachedRowSet resultSet = PristineDBUtil.executeQuery(_Conn, sql,"getRollupAggregation");
			
			while (resultSet.next()) {
				SummaryDataDTO summaryDto = new SummaryDataDTO();
				copyResultSet(summaryDto, resultSet);
				
				summaryDto.setLocationLevelId(locationLevelId);
				
				// set the current calendar_id
				summaryDto.setcalendarId(calendarDTO.getPeriodCalendarId());
				summaryDto.setlastAggrSalesId(resultSet.getInt("LST_ID"));
				
				lstSummary.add(summaryDto);
			}
			
			// Add a dummy record indicating there are no sales for the period specified
			if(lstSummary.size() == 0){
				SummaryDataDTO summaryDto = new SummaryDataDTO();
				createDummySalesRecord(summaryDto, locationLevelId, locationId, 0, 0);
				
				// set the current calendar_id
				summaryDto.setcalendarId(calendarDTO.getPeriodCalendarId());
				summaryDto.setlastAggrSalesId(0);
				lstSummary.add(summaryDto);
			}
			
			PreparedStatement psmt = _Conn.prepareStatement(insertSql);
			sb = new StringBuilder();
			for (SummaryDataDTO summaryDataDTO : lstSummary) {
				// calculate the other metrics info
				businessLogic.CalculateMetrixInformation(summaryDataDTO, true);
				// add the values into psmt batch
				addSqlBatch(summaryDataDTO, psmt,sb);
			}
			
			// Prepare the Store Aggregation records
			int[] wcount = psmt.executeBatch();
			logger.debug("No of rollup records inserted in aggregation:" + wcount.length);
			
			//TODO : Update product_id to null if added as 0
			PristineDBUtil.commitTransaction(_Conn,	 "Commit rollup Aggregation Insert process");
			
			psmt.close();
		} catch (SQLException exe) {
			logger.error("Values:"+sb.toString());
			logger.error("....SQL Exception in sales rollup Aggregation Dao....", exe);
			throw new GeneralException(" SQL Exception in sales rollup Aggregation Dao....",
					exe);
		} catch (Exception exe) {
			logger.error("....Error in sales rollup Aggregation Dao....", exe);
			throw new GeneralException(" Error in sales rollup Aggregation Dao....",
					exe);
		}

	}
	
	public void createDummySalesRecord(SummaryDataDTO summaryDto, int locLevelId, int locId, int prodLevelId, int prodId){
		summaryDto.setLocationLevelId(locLevelId);
		summaryDto.setLocationId(locId);
		summaryDto.setProductLevelId(prodLevelId);
		
		if(prodId==0) summaryDto.setProductId("");
		else summaryDto.setProductId(prodId + "");
		
		summaryDto.setTotalVisitCount(0);
		summaryDto.setTotalMovement(0);
		summaryDto.setRegularMovement(0);
		summaryDto.setSaleMovement(0);
		summaryDto.setRegularRevenue(0);
		summaryDto.setSaleRevenue(0);
		summaryDto.setTotalMargin(0);
		summaryDto.setRegularMargin(0);
		summaryDto.setSaleMargin(0);
		
		
		// added for identical
		summaryDto.setitotalVisitCount(0);
		summaryDto.setitotalMovement(0);
		summaryDto.setiregularMovement(0);
		summaryDto.setisaleMovement(0);
		summaryDto.setiregularRevenue(0);
		summaryDto.setisaleRevenue(0);
		summaryDto.setitotalMargin(0);
		summaryDto.setiregularMargin(0);
		summaryDto.setisaleMargin(0);
		summaryDto.setitotalRevenue(0);
		summaryDto.setTotalRevenue(0);

		// code added for movement by volume
		summaryDto.setregMovementVolume(0);
		summaryDto.setsaleMovementVolume(0);
		summaryDto.setigRegVolumeRev(0);
		summaryDto.setigSaleVolumeRev(0);
		summaryDto.settotMovementVolume(0);
		summaryDto.setigtotVolumeRev(0);

		summaryDto.setidRegMovementVolume(0);
		summaryDto.setidSaleMovementVolume(0);
		summaryDto.setidIgRegVolumeRev(0);
		summaryDto.setidIgSaleVolumeRev(0);
		summaryDto.setidTotMovementVolume(0);
		summaryDto.setIdIgtotVolumeRev(0);
	}
	
	public void copyResultSet(SummaryDataDTO summaryDto, CachedRowSet resultSet) throws SQLException{
		if (resultSet.getString("PRODUCT_LEVEL_ID") == null)
			summaryDto.setProductLevelId(0);
		else
			summaryDto.setProductLevelId(resultSet.getInt("PRODUCT_LEVEL_ID"));
		
		summaryDto.setProductId(resultSet.getString("PRODUCT_ID"));
		summaryDto.setTotalVisitCount(resultSet.getDouble("TOT_VISIT_CNT"));
		summaryDto.setTotalMovement(resultSet.getDouble("TOT_MOVEMENT"));
		summaryDto.setRegularMovement(resultSet.getDouble("REG_MOVEMENT"));
		summaryDto.setSaleMovement(resultSet.getDouble("SALE_MOVEMENT"));
		summaryDto.setRegularRevenue(resultSet.getDouble("REG_REVENUE"));
		summaryDto.setSaleRevenue(resultSet.getDouble("SALE_REVENUE"));
		summaryDto.setTotalMargin(resultSet.getDouble("TOT_MARGIN"));
		summaryDto.setRegularMargin(resultSet.getDouble("REG_MARGIN"));
		summaryDto.setSaleMargin(resultSet.getDouble("SALE_MARGIN"));
		summaryDto.setLocationId(resultSet.getInt("LOCATION_ID"));
		
		// added for identical
		summaryDto.setitotalVisitCount(resultSet.getDouble("ID_TOT_VISIT_CNT"));
		summaryDto.setitotalMovement(resultSet.getDouble("ID_TOT_MOVEMENT"));
		summaryDto.setiregularMovement(resultSet.getDouble("ID_REG_MOVEMENT"));
		summaryDto.setisaleMovement(resultSet.getDouble("ID_SALE_MOVEMENT"));
		summaryDto.setiregularRevenue(resultSet.getDouble("ID_REG_REVENUE"));
		summaryDto.setisaleRevenue(resultSet.getDouble("ID_SALE_REVENUE"));
		summaryDto.setitotalMargin(resultSet.getDouble("ID_TOT_MARGIN"));
		summaryDto.setiregularMargin(resultSet.getDouble("ID_REG_MARGIN"));
		summaryDto.setisaleMargin(resultSet.getDouble("ID_SALE_MARGIN"));
		summaryDto.setitotalRevenue(resultSet.getDouble("ID_TOT_REVENUE"));
		summaryDto.setTotalRevenue(resultSet.getDouble("TOT_REVENUE"));

		// code added for movement by volume
		summaryDto.setregMovementVolume(resultSet.getDouble("REG_MOVEMENT_VOL"));
		summaryDto.setsaleMovementVolume(resultSet.getDouble("SALE_MOVEMENT_VOL"));
		summaryDto.setigRegVolumeRev(resultSet.getDouble("REG_IGVOL_REVENUE"));
		summaryDto.setigSaleVolumeRev(resultSet.getDouble("SALE_IGVOL_REVENUE"));
		summaryDto.settotMovementVolume(resultSet.getDouble("TOT_MOVEMENT_VOL"));
		summaryDto.setigtotVolumeRev(resultSet.getDouble("TOT_IGVOL_REVENUE"));

		summaryDto.setidRegMovementVolume(resultSet.getDouble("ID_REG_MOVEMENT_VOL"));
		summaryDto.setidSaleMovementVolume(resultSet.getDouble("ID_SALE_MOVEMENT_VOL"));
		summaryDto.setidIgRegVolumeRev(resultSet.getDouble("ID_REG_IGVOL_REVENUE"));
		summaryDto.setidIgSaleVolumeRev(resultSet.getDouble("ID_SALE_IGVOL_REVENUE"));
		summaryDto.setidTotMovementVolume(resultSet.getDouble("ID_TOT_MOVEMENT_VOL"));
		summaryDto.setIdIgtotVolumeRev(resultSet.getDouble("ID_TOT_IGVOL_REVENUE"));
		
		summaryDto.setlastAggrCalendarId(resultSet.getInt("LAST_PROCESSESD_CALENDAR_ID"));
		
		// Aggregation at Private label level
		summaryDto.setPLTotalRevenue(resultSet.getDouble("PL_TOT_REVENUE"));
		summaryDto.setPLRegularRevenue(resultSet.getDouble("PL_REG_REVENUE"));
		summaryDto.setPLSaleRevenue(resultSet.getDouble("PL_SALE_REVENUE"));
		summaryDto.setPLTotalMargin(resultSet.getDouble("PL_TOT_MARGIN"));
		summaryDto.setPLRegularMargin(resultSet.getDouble("PL_REG_MARGIN"));
		summaryDto.setPLSaleMargin(resultSet.getDouble("PL_SALE_MARGIN"));
		summaryDto.setPLTotalMovement(resultSet.getDouble("PL_TOT_MOVEMENT"));
		summaryDto.setPLRegularMovement(resultSet.getDouble("PL_REG_MOVEMENT"));
		summaryDto.setPLSaleMovement(resultSet.getDouble("PL_SALE_MOVEMENT"));
		summaryDto.setPLtotMovementVolume(resultSet.getDouble("PL_TOT_MOVEMENT_VOL"));
		summaryDto.setPLregMovementVolume(resultSet.getDouble("PL_REG_MOVEMENT_VOL"));
		summaryDto.setPLsaleMovementVolume(resultSet.getDouble("PL_SALE_MOVEMENT_VOL"));
		summaryDto.setPLTotalVisitCount(resultSet.getDouble("PL_TOT_VISIT_CNT"));
		
		summaryDto.setPLitotalRevenue(resultSet.getDouble("PL_ID_TOT_REVENUE"));
		summaryDto.setPLiregularRevenue(resultSet.getDouble("PL_ID_REG_REVENUE"));
		summaryDto.setPLisaleRevenue(resultSet.getDouble("PL_ID_SALE_REVENUE"));
		summaryDto.setPLitotalMargin(resultSet.getDouble("PL_ID_TOT_MARGIN"));
		summaryDto.setPLiregularMargin(resultSet.getDouble("PL_ID_REG_MARGIN"));
		summaryDto.setPLisaleMargin(resultSet.getDouble("PL_ID_SALE_MARGIN"));
		summaryDto.setPLitotalMovement(resultSet.getDouble("PL_ID_TOT_MOVEMENT"));
		summaryDto.setPLiregularMovement(resultSet.getDouble("PL_ID_REG_MOVEMENT"));
		summaryDto.setPLisaleMovement(resultSet.getDouble("PL_ID_SALE_MOVEMENT"));
		summaryDto.setPLidTotMovementVolume(resultSet.getDouble("PL_ID_TOT_MOVEMENT_VOL"));
		summaryDto.setPLidRegMovementVolume(resultSet.getDouble("PL_ID_REG_MOVEMENT_VOL"));
		summaryDto.setPLidSaleMovementVolume(resultSet.getDouble("PL_ID_SALE_MOVEMENT_VOL"));
		summaryDto.setPLitotalVisitCount(resultSet.getDouble("PL_ID_TOT_VISIT_CNT"));
	}

	
	public void processSalesAggregationRollUp(Connection _Conn, 
		PeriodCalendarDTO calendarDTO, String tableName, int locationLevelId, 
				int locationId, boolean _cascadeEnabled, String quarterSource, 
						boolean updateOnlyPLMetrics) throws GeneralException {
		
		String sourcelocationIdStr = locationId + "";
		int sourcelocationLevelId = locationLevelId;
		
		if(_cascadeEnabled){
			if(locationLevelId == Constants.DISTRICT_LEVEL_ID ){
				if(calendarDTO.getCalendarMode().equalsIgnoreCase("W")){
					SourceTable = "SALES_AGGR_DAILY_ROLLUP";
					SourceRowType = "D";
				} else if(calendarDTO.getCalendarMode().equalsIgnoreCase("P")){
					SourceTable = "SALES_AGGR_WEEKLY_ROLLUP";
					SourceRowType = "W";
				} else if(calendarDTO.getCalendarMode().equalsIgnoreCase("Q")){
					SourceTable = "SALES_AGGR_ROLLUP";
					SourceRowType = "P";
					if(quarterSource.equalsIgnoreCase("W")){
						SourceTable = "SALES_AGGR_WEEKLY_ROLLUP";
						SourceRowType = "W";
					}
				} else if(calendarDTO.getCalendarMode().equalsIgnoreCase("Y")){
					SourceTable = "SALES_AGGR_ROLLUP";
					SourceRowType = "Q";
				}
			} else {
				if(locationLevelId == Constants.REGION_LEVEL_ID ){
					SourceTable = tableName;
					SourceRowType = calendarDTO.getCalendarMode();
					sourcelocationLevelId = Constants.DISTRICT_LEVEL_ID;
					sourcelocationIdStr = "SELECT ID FROM RETAIL_DISTRICT WHERE REGION_ID=" + locationId;
				} else if(locationLevelId == Constants.DIVISION_LEVEL_ID ){
					SourceTable = tableName;
					SourceRowType = calendarDTO.getCalendarMode();
					sourcelocationLevelId = Constants.REGION_LEVEL_ID;
					sourcelocationIdStr = "SELECT ID FROM RETAIL_REGION WHERE DIVISION_ID=" + locationId;
				} else if(locationLevelId == Constants.CHAIN_LEVEL_ID ){
					SourceTable = tableName;
					SourceRowType = calendarDTO.getCalendarMode();
					sourcelocationLevelId = Constants.DIVISION_LEVEL_ID;
					sourcelocationIdStr = "SELECT ID FROM RETAIL_DIVISION WHERE CHAIN_ID=" + locationId;
				}
			}
		}

		String selectSql = select_query.replaceAll("<cal_id>", calendarDTO.getPeriodCalendarId() + "");
		selectSql = selectSql.replaceAll("<lst_cal_id>", calendarDTO.getLastPeriodCalendarId() + "");
		selectSql = selectSql.replaceAll("<table_name>", tableName );
		selectSql = selectSql.replaceAll("<start_date>", calendarDTO.getPeriodStartDate());
		selectSql = selectSql.replaceAll("<end_date>", ( calendarDTO.getPeriodEndDate() == null || calendarDTO.getPeriodEndDate().equalsIgnoreCase("") ) ? 
				calendarDTO.getPeriodStartDate() : calendarDTO.getPeriodEndDate());
		selectSql = selectSql.replaceAll("<location_level_id>", locationLevelId + "");
		selectSql = selectSql.replaceAll("<location_id>", locationId + "" );
		
		selectSql = selectSql.replaceAll("<source_location_level_id>", sourcelocationLevelId + "");
		selectSql = selectSql.replaceAll("<source_location_id>", sourcelocationIdStr + "" );
		selectSql = selectSql.replaceAll("<source_table>", SourceTable + "");
		selectSql = selectSql.replaceAll("<source_row_type>", SourceRowType + "");
		
		if(SourceTable.equalsIgnoreCase("SALES_AGGR_DAILY_ROLLUP"))
			selectSql = selectSql.replaceAll("<last_proc_cal_id>", "AL.CALENDAR_ID");
		else
			selectSql = selectSql.replaceAll("<last_proc_cal_id>", "AL.LAST_AGGR_CALENDARID");			
		
		logger.debug("Sales Aggregation Fetch SQL:" + selectSql);
		
		String insertSql = insert_query_dest;
		insertSql = insertSql.replaceAll("<table_name>", tableName );

		String updateSql = update_query_dest;
		updateSql = updateSql.replaceAll("<table_name>", tableName );		
		
		SalesaggregationbusinessV2 businessLogic = new SalesaggregationbusinessV2();
		StringBuilder sb = new StringBuilder();
		try {
			StringBuffer sql = new StringBuffer();
			sql.append(selectSql);
			
			List<SummaryDataDTO> lstSummary = new ArrayList<SummaryDataDTO>(); 

			CachedRowSet resultSet = PristineDBUtil.executeQuery(_Conn, sql,"getRollupAggregation");
			
			while (resultSet.next()) {
				SummaryDataDTO summaryDto = new SummaryDataDTO();
				copyResultSet(summaryDto, resultSet);
				
				summaryDto.setLocationLevelId(locationLevelId);
				
				// set the current calendar_id
				summaryDto.setcalendarId(calendarDTO.getPeriodCalendarId());
				summaryDto.setlastAggrSalesId(resultSet.getInt("LST_ID"));
				
				lstSummary.add(summaryDto);
			}
			
			// Add a dummy record indicating there are no sales for the period specified
			if(lstSummary.size() == 0){
				SummaryDataDTO summaryDto = new SummaryDataDTO();
				createDummySalesRecord(summaryDto, locationLevelId, locationId, 0, 0);
				
				// set the current calendar_id
				summaryDto.setcalendarId(calendarDTO.getPeriodCalendarId());
				summaryDto.setlastAggrSalesId(0);
				lstSummary.add(summaryDto);
			}
			
			if (updateOnlyPLMetrics){
				logger.debug(updateSql);
				PreparedStatement psmt = _Conn.prepareStatement(updateSql);
				
				for (SummaryDataDTO summaryDataDTO : lstSummary) {
					// calculate the other metrics info
					businessLogic.CalculateMetrixInformation(summaryDataDTO, true);
					// add the values into psmt batch
					updateSqlBatch(summaryDataDTO, psmt);
				}
				
				// Prepare the Store Aggregation records
				int[] wcount = psmt.executeBatch();
				logger.debug("No of rollup records updated in aggregation:" + wcount.length);
				
				//TODO : Update product_id to null if added as 0
				PristineDBUtil.commitTransaction(_Conn,	 "Commit rollup Aggregation Insert process");
				
				psmt.close();
			}
			else{
				logger.debug("Insert SQL:" + insertSql);
				PreparedStatement psmt = _Conn.prepareStatement(insertSql);
				sb = new StringBuilder();
				for (SummaryDataDTO summaryDataDTO : lstSummary) {
					// calculate the other metrics info
					businessLogic.CalculateMetrixInformation(summaryDataDTO, true);
					// add the values into psmt batch
					addSqlBatch(summaryDataDTO, psmt,sb);
				}
				
				// Prepare the Store Aggregation records
				int[] wcount = psmt.executeBatch();
				logger.debug("No of rollup records inserted in aggregation:" + wcount.length);
				
				//TODO : Update product_id to null if added as 0
				PristineDBUtil.commitTransaction(_Conn,	 "Commit rollup Aggregation Insert process");
				
				psmt.close();
			}
			

		} catch (SQLException exe) {
			logger.error("Values:"+sb.toString());
			logger.error("....SQL Exception in sales rollup Aggregation Dao....", exe);
			throw new GeneralException(" SQL Exception in sales rollup Aggregation Dao....",
					exe);
		} catch (Exception exe) {
			logger.error("....Error in sales rollup Aggregation Dao....", exe);
			throw new GeneralException(" Error in sales rollup Aggregation Dao....",
					exe);
		}
	}
	
	private void addSqlBatch(SummaryDataDTO summaryDto, PreparedStatement psmt,StringBuilder sb) {
		try {
			int i=0;
			
			psmt.setObject(++i, summaryDto.getcalendarId());
			sb.append(summaryDto.getcalendarId()).append(", ");
			psmt.setObject(++i, summaryDto.getLocationLevelId());
			sb.append(summaryDto.getLocationLevelId()).append(", ");
			psmt.setObject(++i, summaryDto.getLocationId());
			sb.append(summaryDto.getLocationId()).append(", ");
			
			if (summaryDto.getProductLevelId() == 0){
				psmt.setNull(++i, java.sql.Types.INTEGER);
				sb.append("null, ");
			}
			else{
				psmt.setObject(++i, summaryDto.getProductLevelId());
				sb.append(summaryDto.getProductLevelId()).append(", ");
			}
			if (summaryDto.getProductId() == null || summaryDto.getProductId().equalsIgnoreCase("")){
				psmt.setNull(++i, java.sql.Types.INTEGER);
				sb.append("null, ");
			}
			else{
				psmt.setObject(++i, Integer.parseInt( summaryDto.getProductId() ));
				sb.append(summaryDto.getProductId()).append(", ");
			}
				
			psmt.setDouble(++i, GenericUtil.Round(summaryDto.getTotalVisitCount(), 2));
			sb.append(GenericUtil.Round(summaryDto.getTotalVisitCount(), 2)).append(", ");
			psmt.setDouble(++i, GenericUtil.Round(summaryDto.getTotalMovement(), 2));
			sb.append(GenericUtil.Round(summaryDto.getTotalMovement(), 2)).append(", ");
			psmt.setDouble(++i, GenericUtil.Round(summaryDto.getRegularMovement(), 2));
			sb.append(GenericUtil.Round(summaryDto.getRegularMovement(), 2)).append(", ");
			psmt.setDouble(++i, GenericUtil.Round(summaryDto.getSaleMovement(), 2));
			sb.append(GenericUtil.Round(summaryDto.getSaleMovement(), 2)).append(", ");
			
			psmt.setDouble(++i, GenericUtil.Round(summaryDto.getTotalRevenue(), 2));
			sb.append(GenericUtil.Round(summaryDto.getTotalRevenue(), 2)).append(", ");
			psmt.setDouble(++i, GenericUtil.Round(summaryDto.getRegularRevenue(), 2));
			sb.append(GenericUtil.Round(summaryDto.getRegularRevenue(), 2)).append(", ");
			psmt.setDouble(++i, GenericUtil.Round(summaryDto.getSaleRevenue(), 2));
			sb.append(GenericUtil.Round(summaryDto.getSaleRevenue(), 2)).append(", ");
			
			psmt.setDouble(++i, GenericUtil.Round(summaryDto.getAverageOrderSize(), 2));
			sb.append(GenericUtil.Round(summaryDto.getAverageOrderSize(), 2)).append(", ");
			
			psmt.setDouble(++i, GenericUtil.Round(summaryDto.getTotalMargin(), 2));
			sb.append(GenericUtil.Round(summaryDto.getTotalMargin(), 2)).append(", ");
			psmt.setDouble(++i, GenericUtil.Round(summaryDto.getRegularMargin(), 2));
			sb.append(GenericUtil.Round(summaryDto.getRegularMargin(), 2)).append(", ");
			psmt.setDouble(++i, GenericUtil.Round(summaryDto.getSaleMargin(), 2));
			sb.append(GenericUtil.Round(summaryDto.getSaleMargin(), 2)).append(", ");
			
			psmt.setDouble(++i, GenericUtil.Round(summaryDto.getTotalMarginPer(), 2));
			sb.append(GenericUtil.Round(summaryDto.getTotalMarginPer(), 2)).append(", ");
			psmt.setDouble(++i, GenericUtil.Round(summaryDto.getRegularMarginPer(), 2));
			sb.append(GenericUtil.Round(summaryDto.getRegularMarginPer(), 2)).append(", ");
			psmt.setDouble(++i, GenericUtil.Round(summaryDto.getSaleMarginPer(), 2));
			sb.append(GenericUtil.Round(summaryDto.getSaleMarginPer(), 2)).append(", ");
			
			psmt.setLong(++i,summaryDto.getlastAggrSalesId());
			sb.append(summaryDto.getlastAggrSalesId()).append(", ");
			
			psmt.setDouble(++i, GenericUtil.Round(summaryDto.gettotMovementVolume(), 2));
			sb.append(GenericUtil.Round(summaryDto.gettotMovementVolume(), 2)).append(", ");
			psmt.setDouble(++i, GenericUtil.Round(summaryDto.getregMovementVolume(), 2));
			sb.append(GenericUtil.Round(summaryDto.getregMovementVolume(), 2)).append(", ");
			psmt.setDouble(++i, GenericUtil.Round(summaryDto.getsaleMovementVolume(), 2));
			sb.append(GenericUtil.Round(summaryDto.getsaleMovementVolume(), 2)).append(", ");
			
			// Code added for Movement By volume.....
			psmt.setDouble(++i, GenericUtil.Round(summaryDto.getigtotVolumeRev(), 2));
			sb.append(GenericUtil.Round(summaryDto.getigtotVolumeRev(), 2)).append(", ");
			psmt.setDouble(++i, GenericUtil.Round(summaryDto.getigRegVolumeRev(), 2));
			sb.append(GenericUtil.Round(summaryDto.getigRegVolumeRev(), 2)).append(", ");
			psmt.setDouble(++i, GenericUtil.Round(summaryDto.getigSaleVolumeRev(), 2));
			sb.append(GenericUtil.Round(summaryDto.getigSaleVolumeRev(), 2)).append(", ");
			
			psmt.setDouble(++i, GenericUtil.Round(summaryDto.getitotalVisitCount(), 2));
			sb.append(GenericUtil.Round(summaryDto.getitotalVisitCount(), 2)).append(", ");

			psmt.setDouble(++i, GenericUtil.Round(summaryDto.getitotalMovement(), 2));
			sb.append(GenericUtil.Round(summaryDto.getitotalMovement(), 2)).append(", ");
			psmt.setDouble(++i, GenericUtil.Round(summaryDto.getiregularMovement(), 2));
			sb.append(GenericUtil.Round(summaryDto.getiregularMovement(), 2)).append(", ");
			psmt.setDouble(++i, GenericUtil.Round(summaryDto.getisaleMovement(), 2));
			sb.append(GenericUtil.Round(summaryDto.getisaleMovement(), 2)).append(", ");
			psmt.setDouble(++i, GenericUtil.Round(summaryDto.getitotalRevenue(), 2));
			sb.append(GenericUtil.Round(summaryDto.getitotalRevenue(), 2)).append(", ");
			psmt.setDouble(++i, GenericUtil.Round(summaryDto.getiregularRevenue(), 2));
			sb.append(GenericUtil.Round(summaryDto.getiregularRevenue(), 2)).append(", ");
			psmt.setDouble(++i, GenericUtil.Round(summaryDto.getisaleRevenue(), 2));
			sb.append(GenericUtil.Round(summaryDto.getisaleRevenue(), 2)).append(", ");
			psmt.setDouble(++i, GenericUtil.Round(summaryDto.getiaverageOrderSize(), 2));
			sb.append(GenericUtil.Round(summaryDto.getiaverageOrderSize(), 2)).append(", ");
			
			psmt.setDouble(++i, GenericUtil.Round(summaryDto.getitotalMargin(), 2));
			sb.append(GenericUtil.Round(summaryDto.getitotalMargin(), 2)).append(", ");
			psmt.setDouble(++i, GenericUtil.Round(summaryDto.getiregularMargin(), 2));
			sb.append(GenericUtil.Round(summaryDto.getiregularMargin(), 2)).append(", ");
			psmt.setDouble(++i, GenericUtil.Round(summaryDto.getisaleMargin(), 2));
			sb.append(GenericUtil.Round(summaryDto.getisaleMargin(), 2)).append(", ");
			psmt.setDouble(++i, GenericUtil.Round(summaryDto.getitotalMarginPer(), 2));
			sb.append(GenericUtil.Round(summaryDto.getitotalMarginPer(), 2)).append(", ");
			psmt.setDouble(++i, GenericUtil.Round(summaryDto.getiregularMarginPer(), 2));
			sb.append(GenericUtil.Round(summaryDto.getiregularMarginPer(), 2)).append(", ");
			psmt.setDouble(++i, GenericUtil.Round(summaryDto.getisaleMarginPer(), 2));
			sb.append(GenericUtil.Round(summaryDto.getisaleMarginPer(), 2)).append(", ");

			// Movement By Volume for identical
			psmt.setDouble(++i, GenericUtil.Round(summaryDto.getidTotMovementVolume(), 2));
			sb.append(GenericUtil.Round(summaryDto.getidTotMovementVolume(), 2)).append(", ");
			psmt.setDouble(++i, GenericUtil.Round(summaryDto.getidRegMovementVolume(), 2));
			sb.append(GenericUtil.Round(summaryDto.getidRegMovementVolume(), 2)).append(", ");
			psmt.setDouble(++i, GenericUtil.Round(summaryDto.getidSaleMovementVolume(), 2));
			sb.append(GenericUtil.Round(summaryDto.getidSaleMovementVolume(), 2)).append(", ");
			psmt.setDouble(++i, GenericUtil.Round(summaryDto.getidIgRegVolumeRev(), 2));
			sb.append(GenericUtil.Round(summaryDto.getidIgRegVolumeRev(), 2)).append(", ");
			psmt.setDouble(++i, GenericUtil.Round(summaryDto.getidIgSaleVolumeRev(), 2));
			sb.append(GenericUtil.Round(summaryDto.getidIgSaleVolumeRev(), 2)).append(", ");
			psmt.setDouble(++i, GenericUtil.Round(summaryDto.getIdIgtotVolumeRev(), 2));
			sb.append(GenericUtil.Round(summaryDto.getIdIgtotVolumeRev(), 2)).append(", ");
			psmt.setDouble(++i, summaryDto.getlastAggrCalendarId());
			sb.append(summaryDto.getlastAggrCalendarId()).append(", ");

			// Metrics at Private label level
			psmt.setDouble(++i,GenericUtil.Round(summaryDto.getPLTotalRevenue(), 2));
			sb.append(GenericUtil.Round(summaryDto.getPLTotalRevenue(), 2)).append(", ");
			psmt.setDouble(++i,GenericUtil.Round(summaryDto.getPLRegularRevenue(), 2));
			sb.append(GenericUtil.Round(summaryDto.getPLRegularRevenue(), 2)).append(", ");
			psmt.setDouble(++i,GenericUtil.Round(summaryDto.getPLSaleRevenue(), 2));
			sb.append(GenericUtil.Round(summaryDto.getPLSaleRevenue(), 2)).append(", ");
			psmt.setDouble(++i, GenericUtil.Round(summaryDto.getPLTotalMargin(), 2));
			sb.append(GenericUtil.Round(summaryDto.getPLTotalMargin(), 2)).append(", ");
			psmt.setDouble(++i, GenericUtil.Round(summaryDto.getPLRegularMargin(), 2));
			sb.append(GenericUtil.Round(summaryDto.getPLRegularMargin(), 2)).append(", ");
			psmt.setDouble(++i, GenericUtil.Round(summaryDto.getPLSaleMargin(), 2));
			sb.append(GenericUtil.Round(summaryDto.getPLSaleMargin(), 2)).append(", ");
			psmt.setDouble(++i, GenericUtil.Round(summaryDto.getPLTotalMarginPer(), 2));
			sb.append(GenericUtil.Round(summaryDto.getPLTotalMarginPer(), 2)).append(", ");
			psmt.setDouble(++i, GenericUtil.Round(summaryDto.getPLRegularMarginPer(), 2));
			sb.append(GenericUtil.Round(summaryDto.getPLRegularMarginPer(), 2)).append(", ");
			psmt.setDouble(++i, GenericUtil.Round(summaryDto.getPLSaleMarginPer(), 2));
			sb.append(GenericUtil.Round(summaryDto.getPLSaleMarginPer(), 2)).append(", ");
			psmt.setDouble(++i,GenericUtil.Round(summaryDto.getPLTotalMovement(), 2));
			sb.append(GenericUtil.Round(summaryDto.getPLTotalMovement(), 2)).append(", ");
			psmt.setDouble(++i,GenericUtil.Round(summaryDto.getPLRegularMovement(), 2));
			sb.append(GenericUtil.Round(summaryDto.getPLRegularMovement(), 2)).append(", ");
			psmt.setDouble(++i,GenericUtil.Round(summaryDto.getPLSaleMovement(), 2));
			sb.append(GenericUtil.Round(summaryDto.getPLSaleMovement(), 2)).append(", ");
			psmt.setDouble(++i, GenericUtil.Round(summaryDto.getPLtotMovementVolume(), 2));
			sb.append(GenericUtil.Round(summaryDto.getPLtotMovementVolume(), 2)).append(", ");
			psmt.setDouble(++i, GenericUtil.Round(summaryDto.getPLregMovementVolume(), 2));
			sb.append(GenericUtil.Round(summaryDto.getPLregMovementVolume(), 2)).append(", ");
			psmt.setDouble(++i, GenericUtil.Round(summaryDto.getPLsaleMovementVolume(), 2));
			sb.append(GenericUtil.Round(summaryDto.getPLsaleMovementVolume(), 2)).append(", ");
			psmt.setDouble(++i,GenericUtil.Round(summaryDto.getPLTotalVisitCount(), 2));
			sb.append(GenericUtil.Round(summaryDto.getPLTotalVisitCount(), 2)).append(", ");
			psmt.setDouble(++i,GenericUtil.Round(summaryDto.getPLAverageOrderSize(), 2));
			sb.append(GenericUtil.Round(summaryDto.getPLAverageOrderSize(), 2)).append(", ");

			psmt.setDouble(++i,GenericUtil.Round(summaryDto.getPLitotalRevenue(), 2));
			sb.append(GenericUtil.Round(summaryDto.getPLitotalRevenue(), 2)).append(", ");
			psmt.setDouble(++i,GenericUtil.Round(summaryDto.getPLiregularRevenue(), 2));
			sb.append(GenericUtil.Round(summaryDto.getPLiregularRevenue(), 2)).append(", ");
			psmt.setDouble(++i,GenericUtil.Round(summaryDto.getPLisaleRevenue(), 2));
			sb.append(GenericUtil.Round(summaryDto.getPLisaleRevenue(), 2)).append(", ");
			psmt.setDouble(++i, GenericUtil.Round(summaryDto.getPLitotalMargin(), 2));
			sb.append(GenericUtil.Round(summaryDto.getPLitotalMargin(), 2)).append(", ");
			psmt.setDouble(++i, GenericUtil.Round(summaryDto.getPLiregularMargin(), 2));
			sb.append(GenericUtil.Round(summaryDto.getPLiregularMargin(), 2)).append(", ");
			psmt.setDouble(++i, GenericUtil.Round(summaryDto.getPLisaleMargin(), 2));
			sb.append(GenericUtil.Round(summaryDto.getPLisaleMargin(), 2)).append(", ");
			psmt.setDouble(++i, GenericUtil.Round(summaryDto.getPLitotalMarginPer(), 2));
			sb.append(GenericUtil.Round(summaryDto.getPLitotalMarginPer(), 2)).append(", ");
			psmt.setDouble(++i, GenericUtil.Round(summaryDto.getPLiregularMarginPer(), 2));
			sb.append(GenericUtil.Round(summaryDto.getPLiregularMarginPer(), 2)).append(", ");
			psmt.setDouble(++i, GenericUtil.Round(summaryDto.getPLisaleMarginPer(), 2));
			sb.append(GenericUtil.Round(summaryDto.getPLisaleMarginPer(), 2)).append(", ");
			psmt.setDouble(++i,GenericUtil.Round(summaryDto.getPLitotalMovement(), 2));
			sb.append(GenericUtil.Round(summaryDto.getPLitotalMovement(), 2)).append(", ");
			psmt.setDouble(++i, GenericUtil.Round(summaryDto.getPLiregularMovement(), 2));
			sb.append(GenericUtil.Round(summaryDto.getPLiregularMovement(), 2)).append(", ");
			psmt.setDouble(++i, GenericUtil.Round(summaryDto.getPLisaleMovement(), 2));
			sb.append(GenericUtil.Round(summaryDto.getPLisaleMovement(), 2)).append(", ");
			psmt.setDouble(++i, GenericUtil.Round(summaryDto.getPLidTotMovementVolume(), 2));
			sb.append(GenericUtil.Round(summaryDto.getPLidTotMovementVolume(), 2)).append(", ");
			psmt.setDouble(++i, GenericUtil.Round(summaryDto.getPLidRegMovementVolume(), 2));
			sb.append(GenericUtil.Round(summaryDto.getPLidRegMovementVolume(), 2)).append(", ");
			psmt.setDouble(++i, GenericUtil.Round(summaryDto.getPLidSaleMovementVolume(), 2));
			sb.append(GenericUtil.Round(summaryDto.getPLidSaleMovementVolume(), 2)).append(", ");
			psmt.setDouble(++i,GenericUtil.Round(summaryDto.getPLitotalVisitCount(), 2));
			sb.append(GenericUtil.Round(summaryDto.getPLitotalVisitCount(), 2)).append(", ");
			psmt.setDouble(++i,GenericUtil.Round(summaryDto.getPLiaverageOrderSize(), 2));
			sb.append(GenericUtil.Round(summaryDto.getPLiaverageOrderSize(), 2)).append(")");
			sb.append("\n");
			
			psmt.addBatch();
		} catch (Exception sql) {
			logger.error(sql.getMessage());
			sql.printStackTrace();
		}

	}

	private void updateSqlBatch(SummaryDataDTO summaryDto, PreparedStatement psmt) {

		try {
			int i=0;
			psmt.setDouble(++i,GenericUtil.Round(summaryDto.getPLTotalRevenue(), 2));
			psmt.setDouble(++i,GenericUtil.Round(summaryDto.getPLRegularRevenue(), 2));
			psmt.setDouble(++i,GenericUtil.Round(summaryDto.getPLSaleRevenue(), 2));
			
			psmt.setDouble(++i, GenericUtil.Round(summaryDto.getPLTotalMargin(), 2));
			psmt.setDouble(++i, GenericUtil.Round(summaryDto.getPLRegularMargin(), 2));
			psmt.setDouble(++i, GenericUtil.Round(summaryDto.getPLSaleMargin(), 2));
			
			psmt.setDouble(++i, GenericUtil.Round(summaryDto.getPLTotalMarginPer(), 2));
			psmt.setDouble(++i, GenericUtil.Round(summaryDto.getPLRegularMarginPer(), 2));
			psmt.setDouble(++i, GenericUtil.Round(summaryDto.getPLSaleMarginPer(), 2));
			
			psmt.setDouble(++i,GenericUtil.Round(summaryDto.getPLTotalMovement(), 2));
			psmt.setDouble(++i,GenericUtil.Round(summaryDto.getPLRegularMovement(), 2));
			psmt.setDouble(++i,GenericUtil.Round(summaryDto.getPLSaleMovement(), 2));
			
			psmt.setDouble(++i, GenericUtil.Round(summaryDto.getPLtotMovementVolume(), 2));
			psmt.setDouble(++i, GenericUtil.Round(summaryDto.getPLregMovementVolume(), 2));
			psmt.setDouble(++i, GenericUtil.Round(summaryDto.getPLsaleMovementVolume(), 2));
			
			psmt.setDouble(++i,GenericUtil.Round(summaryDto.getPLTotalVisitCount(), 2));
			psmt.setDouble(++i,GenericUtil.Round(summaryDto.getPLAverageOrderSize(), 2));
			
			psmt.setDouble(++i,GenericUtil.Round(summaryDto.getPLitotalRevenue(), 2));
			psmt.setDouble(++i,GenericUtil.Round(summaryDto.getPLiregularRevenue(), 2));
			psmt.setDouble(++i,GenericUtil.Round(summaryDto.getPLisaleRevenue(), 2));
			
			psmt.setDouble(++i, GenericUtil.Round(summaryDto.getPLitotalMargin(), 2));
			psmt.setDouble(++i, GenericUtil.Round(summaryDto.getPLiregularMargin(), 2));
			psmt.setDouble(++i, GenericUtil.Round(summaryDto.getPLisaleMargin(), 2));
			
			psmt.setDouble(++i, GenericUtil.Round(summaryDto.getPLitotalMarginPer(), 2));
			psmt.setDouble(++i, GenericUtil.Round(summaryDto.getPLiregularMarginPer(), 2));
			psmt.setDouble(++i, GenericUtil.Round(summaryDto.getPLisaleMarginPer(), 2));
			
			psmt.setDouble(++i,GenericUtil.Round(summaryDto.getPLitotalMovement(), 2));
			psmt.setDouble(++i, GenericUtil.Round(summaryDto.getPLiregularMovement(), 2));
			psmt.setDouble(++i, GenericUtil.Round(summaryDto.getPLisaleMovement(), 2));
			
			psmt.setDouble(++i, GenericUtil.Round(summaryDto.getPLidTotMovementVolume(), 2));
			psmt.setDouble(++i, GenericUtil.Round(summaryDto.getPLidRegMovementVolume(), 2));
			psmt.setDouble(++i, GenericUtil.Round(summaryDto.getPLidSaleMovementVolume(), 2));
			
			psmt.setDouble(++i,GenericUtil.Round(summaryDto.getPLitotalVisitCount(), 2));
			psmt.setDouble(++i,GenericUtil.Round(summaryDto.getPLiaverageOrderSize(), 2));
			
			psmt.setObject(++i, summaryDto.getcalendarId());
			psmt.setObject(++i, summaryDto.getLocationLevelId());
			psmt.setObject(++i, summaryDto.getLocationId());

			if (summaryDto.getProductLevelId() == 0)
				psmt.setInt(++i, -1);
			else
				psmt.setObject(++i, summaryDto.getProductLevelId());

			if (summaryDto.getProductId() == null || summaryDto.getProductId().equalsIgnoreCase(""))
				psmt.setInt(++i, -1);
			else
				psmt.setObject(++i, Integer.parseInt( summaryDto.getProductId() )); 
			
			psmt.addBatch();
		} catch (Exception sql) {
			logger.error(sql.getMessage());
			sql.printStackTrace();
		}

	}
	
}
