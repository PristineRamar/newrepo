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

public class SalesAggregationDaoV3 {
	
	static Logger logger = Logger.getLogger("SalesAggregationDaoV3");
	
	private String SourceTable = "SALES_AGGR_DAILY" ;
	private String SourceRowType = "D" ;
	
	 static String select_query = " SELECT 0,<cal_id>,SRC.LOCATION_LEVEL_ID,SRC.LOCATION_ID,SRC.PRODUCT_LEVEL_ID,SRC.PRODUCT_ID,SUM(SRC.TOT_VISIT_CNT) TOT_VISIT_CNT, " +
			 " SUM(SRC.TOT_MOVEMENT) TOT_MOVEMENT,SUM(SRC.REG_MOVEMENT) REG_MOVEMENT,SUM(SRC.SALE_MOVEMENT) SALE_MOVEMENT,SUM(SRC.TOT_REVENUE) TOT_REVENUE," + 
			 " SUM(SRC.REG_REVENUE) REG_REVENUE,SUM(SRC.SALE_REVENUE) SALE_REVENUE, MAX(<last_proc_cal_id>) AS LAST_PROCESSESD_CALENDAR_ID," +
			 " SUM(SRC.TOT_MARGIN) TOT_MARGIN,SUM(SRC.REG_MARGIN) REG_MARGIN,SUM(SRC.SALE_MARGIN) SALE_MARGIN, " +
			 " SUM(SRC.LOYALTY_CARD_SAVING) LOYALTY_CARD_SAVING,SUM(SRC.TOT_MOVEMENT_VOL) TOT_MOVEMENT_VOL,SUM(SRC.REG_MOVEMENT_VOL) REG_MOVEMENT_VOL," +
			 " SUM(SRC.SALE_MOVEMENT_VOL) SALE_MOVEMENT_VOL,SUM(SRC.TOT_IGVOL_REVENUE) TOT_IGVOL_REVENUE,SUM(SRC.REG_IGVOL_REVENUE) REG_IGVOL_REVENUE, " +
			 " SUM(SRC.SALE_IGVOL_REVENUE) SALE_IGVOL_REVENUE," +
			 
			" SUM(SRC.PL_TOT_REVENUE) AS PL_TOT_REVENUE," + 
			" SUM(SRC.PL_REG_REVENUE) AS PL_REG_REVENUE,"+ 
			" SUM(SRC.PL_SALE_REVENUE) AS PL_SALE_REVENUE," +
			" SUM(SRC.PL_TOT_MARGIN) AS PL_TOT_MARGIN," + 
			" SUM(SRC.PL_REG_MARGIN) AS PL_REG_MARGIN," + 
			" SUM(SRC.PL_SALE_MARGIN) AS PL_SALE_MARGIN," +
			" SUM(SRC.PL_TOT_MARGIN_PCT) AS PL_TOT_MARGIN_PCT," + 
			" SUM(SRC.PL_REG_MARGIN_PCT) AS PL_REG_MARGIN_PCT," + 
			" SUM(SRC.PL_SALE_MARGIN_PCT) AS PL_SALE_MARGIN_PCT," +
			" SUM(SRC.PL_TOT_MOVEMENT) AS PL_TOT_MOVEMENT," + 
			" SUM(SRC.PL_REG_MOVEMENT) AS PL_REG_MOVEMENT, " + 
			" SUM(SRC.PL_SALE_MOVEMENT) AS PL_SALE_MOVEMENT," +
			" SUM(SRC.PL_TOT_MOVEMENT_VOL) AS PL_TOT_MOVEMENT_VOL," + 
			" SUM(SRC.PL_REG_MOVEMENT_VOL) AS PL_REG_MOVEMENT_VOL," + 
			" SUM(SRC.PL_SALE_MOVEMENT_VOL) AS PL_SALE_MOVEMENT_VOL," +
			" SUM(SRC.PL_TOT_VISIT_CNT) AS PL_TOT_VISIT_CNT," +
			 
			 " MIN(SRC.STORE_TYPE) STORE_TYPE,MIN(LST.<table_name>_id) LST_ID FROM <source_table> SRC " +
			 
			 " LEFT JOIN <table_name> LST ON " + 
			 " SRC.LOCATION_LEVEL_ID = LST.LOCATION_LEVEL_ID " + 
			 " AND SRC.LOCATION_ID = LST.LOCATION_ID " + 
			 " AND (SRC.PRODUCT_LEVEL_ID = LST.PRODUCT_LEVEL_ID OR (SRC.PRODUCT_LEVEL_ID IS NULL AND LST.PRODUCT_LEVEL_ID IS NULL))" + 
			 " AND (SRC.PRODUCT_ID = LST.PRODUCT_ID OR (SRC.PRODUCT_ID IS NULL AND LST.PRODUCT_ID IS NULL))" +
			 " AND LST.CALENDAR_ID = <lst_cal_id> " + 
			 " WHERE SRC.CALENDAR_ID IN (select calendar_id from retail_calendar where start_date >= to_date('<start_date>','"+ Constants.DB_DATE_FORMAT + "') " + 
			 " AND start_date <= to_date('<end_date>','"+ Constants.DB_DATE_FORMAT + "') and row_type='<source_row_type>') " +
			 " AND SRC.LOCATION_LEVEL_ID = 5 AND SRC.LOCATION_ID IN  " +
			 " (  select comp_str_id from competitor_store where comp_str_no='<store_no>' or district_id = <district_no> ) " +
			 " GROUP BY SRC.LOCATION_LEVEL_ID,SRC.LOCATION_ID,SRC.PRODUCT_LEVEL_ID,SRC.PRODUCT_ID " ;
	 
	 static String insert_query_dest = " INSERT INTO <table_name>( <table_name>_id,CALENDAR_ID,LOCATION_LEVEL_ID,LOCATION_ID,PRODUCT_LEVEL_ID,PRODUCT_ID,TOT_VISIT_CNT, " +
			" AVG_ORDER_SIZE,TOT_MOVEMENT,TOT_REVENUE,REG_REVENUE,SALE_REVENUE,TOT_MARGIN,REG_MARGIN,SALE_MARGIN,TOT_MARGIN_PCT, " +
			" REG_MARGIN_PCT,SALE_MARGIN_PCT,lst_<table_name>_id,REG_MOVEMENT,SALE_MOVEMENT,REG_MOVEMENT_VOL,SALE_MOVEMENT_VOL,TOT_MOVEMENT_VOL, " +
			" REG_IGVOL_REVENUE,SALE_IGVOL_REVENUE,TOT_IGVOL_REVENUE,STORE_TYPE, LAST_AGGR_CALENDARID, " +
			 
			" PL_TOT_REVENUE, PL_REG_REVENUE, PL_SALE_REVENUE," +
			" PL_TOT_MARGIN, PL_REG_MARGIN, PL_SALE_MARGIN," +
			" PL_TOT_MARGIN_PCT, PL_REG_MARGIN_PCT, PL_SALE_MARGIN_PCT," +
			" PL_TOT_MOVEMENT, PL_REG_MOVEMENT, PL_SALE_MOVEMENT," +
			" PL_TOT_MOVEMENT_VOL, PL_REG_MOVEMENT_VOL, PL_SALE_MOVEMENT_VOL," +
			" PL_TOT_VISIT_CNT, PL_AVG_ORDER_SIZE)" +
			 
			 " VALUES ("+DBManager.getIdentityIncrement("<table_name>_seq") +",?,?,?,?,?,?, ?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?," +
			 " ?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)";
	 
	 static String update_query_dest = " UPDATE <table_name> SET" + 
				" PL_TOT_REVENUE=?, PL_REG_REVENUE=?, PL_SALE_REVENUE=?," +
				" PL_TOT_MARGIN=?, PL_REG_MARGIN=?, PL_SALE_MARGIN=?," +
				" PL_TOT_MARGIN_PCT=?, PL_REG_MARGIN_PCT=?, PL_SALE_MARGIN_PCT=?," +
				" PL_TOT_MOVEMENT=?, PL_REG_MOVEMENT=?, PL_SALE_MOVEMENT=?," +
				" PL_TOT_MOVEMENT_VOL=?, PL_REG_MOVEMENT_VOL=?, PL_SALE_MOVEMENT_VOL=?," +
				" PL_TOT_VISIT_CNT=?, PL_AVG_ORDER_SIZE=? " +
	 			" WHERE CALENDAR_ID=? AND LOCATION_LEVEL_ID=? AND LOCATION_ID=?" + 
				" AND NVL(PRODUCT_LEVEL_ID, -1) = ? AND NVL(PRODUCT_ID, -1) = ?";	 
	 
	//This method returns the Calendar Id of the last data loading
	public int getLastUpdatedCalendarId(Connection _Conn, String tableName,String compStrNo, String districtNum, String calendarMode){
		StringBuffer sql = new StringBuffer();
		//1. Retrieve the calendar id, when the last data loading happened
		sql.append(" SELECT CALENDAR_ID FROM RETAIL_CALENDAR WHERE ROW_TYPE='"+calendarMode+"' AND START_DATE IN " + 
				" (select MAX(RC.START_DATE) from " + tableName + " T INNER JOIN RETAIL_CALENDAR RC " + 
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
	 
	public void deletePreviousAggregation(Connection _Conn, PeriodCalendarDTO calendarDTO, String tableName, 
			String compStrNo, String districtNum) throws GeneralException {
		
		//logger.debug("Delete Previous Aggregation Starts");
		
		try {
			StringBuffer sql = new StringBuffer();
			
			//SQL to delete Sales Daily data
			sql.append(" Delete from "+tableName+" WHERE CALENDAR_ID = " + calendarDTO.getPeriodCalendarId());
			sql.append(" and LOCATION_ID in ( select comp_str_id from competitor_store where comp_str_no ='"+compStrNo+"' or district_id = "+districtNum+" )");
		 
		    // execute the Sales Aggr Daily data delete query
		    logger.debug("deletePreviousAggregation SQL:" +sql.toString());
		    PristineDBUtil.executeUpdate(_Conn, sql , "deletePreviousAggregation");
		 			
			PristineDBUtil.commitTransaction(_Conn,	 "Commit Aggregation delete process");
		} catch (GeneralException e) {
			 logger.error("Error while deleting previous Aggregation data" + e);
			throw new GeneralException("deletePreviousAggregation", e);		
		}
	}
	
	public void processSalesAggregation(Connection _Conn, PeriodCalendarDTO calendarDTO, String tableName, 
			String compStrNo, String districtNum, boolean cascadeEnabled, String quarterSource, boolean updateonlyPLFlag) throws GeneralException {
		
		if(cascadeEnabled){
			if(calendarDTO.getCalendarMode().equalsIgnoreCase(Constants.CALENDAR_PERIOD)){
				SourceTable = "SALES_AGGR_WEEKLY";
				SourceRowType = "W";
			} else if(calendarDTO.getCalendarMode().equalsIgnoreCase(Constants.CALENDAR_QUARTER)){
				SourceTable = "SALES_AGGR";
				SourceRowType = "P";
				if(quarterSource.equalsIgnoreCase("W")){
					SourceTable = "SALES_AGGR_WEEKLY";
					SourceRowType = "W";
				}
			} else if(calendarDTO.getCalendarMode().equalsIgnoreCase(Constants.CALENDAR_YEAR)){
				SourceTable = "SALES_AGGR";
				SourceRowType = "Q";
			}
		}
		logger.debug("Select SQL for " + calendarDTO.getCalendarMode());
		String selectSql = select_query.replaceAll("<cal_id>", calendarDTO.getPeriodCalendarId() + "");
		selectSql = selectSql.replaceAll("<lst_cal_id>", calendarDTO.getLastPeriodCalendarId() + "");
		selectSql = selectSql.replaceAll("<table_name>", tableName );
		selectSql = selectSql.replaceAll("<start_date>", calendarDTO.getPeriodStartDate());
		selectSql = selectSql.replaceAll("<end_date>", calendarDTO.getPeriodEndDate());
		selectSql = selectSql.replaceAll("<store_no>", compStrNo + "");
		selectSql = selectSql.replaceAll("<district_no>", districtNum + "");
		selectSql = selectSql.replaceAll("<source_table>", SourceTable + "");
		selectSql = selectSql.replaceAll("<source_row_type>", SourceRowType + "");
		
		if(SourceTable.equalsIgnoreCase("SALES_AGGR_DAILY"))
			selectSql = selectSql.replaceAll("<last_proc_cal_id>", "SRC.CALENDAR_ID");
		else
			selectSql = selectSql.replaceAll("<last_proc_cal_id>", "SRC.LAST_AGGR_CALENDARID");
		
		logger.debug("Sales Aggregation Fetch SQL:" + selectSql);
		
		String insertSql = insert_query_dest;
		insertSql = insertSql.replaceAll("<table_name>", tableName );
		
		String updateSql = update_query_dest;
		updateSql = updateSql.replaceAll("<table_name>", tableName );
		
		
		SalesaggregationbusinessV2 businessLogic = new SalesaggregationbusinessV2();
		
		try {
			StringBuffer sql = new StringBuffer();
			sql.append(selectSql);
			
			List<SummaryDataDTO> lstSummary = new ArrayList<SummaryDataDTO>(); 

			CachedRowSet resultSet = PristineDBUtil.executeQuery(_Conn, sql,"getAggregation");
			
			while (resultSet.next()) {
				SummaryDataDTO summaryDto = new SummaryDataDTO();
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

				// code added for Movement By Volume
				summaryDto.setregMovementVolume(resultSet.getDouble("REG_MOVEMENT_VOL"));
				summaryDto.setsaleMovementVolume(resultSet.getDouble("SALE_MOVEMENT_VOL"));
				summaryDto.setigRegVolumeRev(resultSet.getDouble("REG_IGVOL_REVENUE"));
				summaryDto.setigSaleVolumeRev(resultSet.getDouble("SALE_IGVOL_REVENUE"));
				summaryDto.settotMovementVolume(resultSet.getDouble("TOT_MOVEMENT_VOL"));
				summaryDto.setigtotVolumeRev(resultSet.getDouble("TOT_IGVOL_REVENUE"));
				summaryDto.setStoreStatus(resultSet.getString("STORE_TYPE"));
				summaryDto.setTotalRevenue(resultSet.getDouble("TOT_REVENUE"));
				
				// set the current calendar_id
				summaryDto.setcalendarId(calendarDTO.getPeriodCalendarId());
				summaryDto.setlastAggrSalesId(resultSet.getLong("LST_ID"));
				
				// Set last processed day calendar id
				summaryDto.setlastAggrCalendarId(resultSet.getInt("LAST_PROCESSESD_CALENDAR_ID"));
				
				//Aggregation at Private label level
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
				
				lstSummary.add(summaryDto);
			}
			
			// Add a dummy record indicating there are no sales for the period specified
			if(lstSummary.size() == 0){
				SummaryDataDTO summaryDto ;

				sql.setLength(0);
				String  str = "select cs.comp_str_id from competitor_store cs inner join competitor_chain cc on cc.presto_subscriber='Y' and cs.comp_chain_id = cc.comp_chain_id where cs.comp_str_no='"+compStrNo+"' or cs.district_id = "+districtNum ;
				sql.append(str);
				
				CachedRowSet locationSet = PristineDBUtil.executeQuery(_Conn,sql,"getLocations") ;
				while (locationSet.next()) {
					summaryDto = new SummaryDataDTO();
					createDummySalesRecord(summaryDto, Constants.STORE_LEVEL_ID, locationSet.getInt("comp_str_id"), 0, 0);
					
					// set the current calendar_id
					summaryDto.setcalendarId(calendarDTO.getPeriodCalendarId());
					summaryDto.setlastAggrSalesId(0);
					lstSummary.add(summaryDto);
				}
			}
			
			if (updateonlyPLFlag){
				logger.debug(updateSql);
				PreparedStatement psmt = _Conn.prepareStatement(updateSql);
				
				for (SummaryDataDTO summaryDataDTO : lstSummary) {
					// calculate the other metrics info
					businessLogic.CalculateMetrixInformation(summaryDataDTO, true);
	
					updateSqlBatch(summaryDataDTO, psmt);
				}
				
				// Prepare the Store Aggregation records
				int[] wcount = psmt.executeBatch();
				logger.debug("No of records update in aggregation:" + wcount.length);
				
				//TODO : Update product_id to null if added as 0
				PristineDBUtil.commitTransaction(_Conn,	 "Commit Aggregation update process");
				
				psmt.close();
			}
			else{
				PreparedStatement psmt = _Conn.prepareStatement(insertSql);
				
				for (SummaryDataDTO summaryDataDTO : lstSummary) {
					// calculate the other metrics info
					businessLogic.CalculateMetrixInformation(summaryDataDTO, true);
					
					addSqlBatch(summaryDataDTO, psmt);
				}
				
				// Prepare the Store Aggregation records
				int[] wcount = psmt.executeBatch();
				logger.debug("No of records inserted in aggregation:" + wcount.length);
				
				//TODO : Update product_id to null if added as 0
				PristineDBUtil.commitTransaction(_Conn,	 "Commit Aggregation Insert process");
				
				psmt.close();
			}
		} catch (SQLException exe) {
			logger.error("....SQL Exception in sales Aggregation Dao....", exe);
			throw new GeneralException(" SQL Exception in sales Aggregation Dao....",
					exe);
		} catch (Exception exe) {
			logger.error("....Error in sales Aggregation Dao....", exe);
			throw new GeneralException(" Error in sales Aggregation Dao....",
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

	private void addSqlBatch(SummaryDataDTO summaryDto, PreparedStatement psmt) throws GeneralException {
		try {
			int i=0;
			psmt.setObject(++i, summaryDto.getcalendarId());
			psmt.setObject(++i, Constants.STORE_LEVEL_ID);
			psmt.setObject(++i, summaryDto.getLocationId());
			if( summaryDto.getProductLevelId() == 0)
				psmt.setNull(++i, java.sql.Types.INTEGER);
			else
				psmt.setInt(++i, summaryDto.getProductLevelId());
			
			if (summaryDto.getProductId() == null || summaryDto.getProductId().equalsIgnoreCase(""))
				psmt.setNull(++i, java.sql.Types.INTEGER);
			else
				psmt.setObject(++i, Integer.parseInt( summaryDto.getProductId() )); 
			
			psmt.setDouble(++i, GenericUtil.Round(summaryDto.getTotalVisitCount(), 2));
			psmt.setDouble(++i,GenericUtil.Round(summaryDto.getAverageOrderSize(), 2));

			psmt.setDouble(++i, GenericUtil.Round(summaryDto.getTotalMovement(), 2));
			psmt.setDouble(++i, GenericUtil.Round(summaryDto.getTotalRevenue(), 2));
			psmt.setDouble(++i, GenericUtil.Round(summaryDto.getRegularRevenue(), 2));
			psmt.setDouble(++i, GenericUtil.Round(summaryDto.getSaleRevenue(), 2));
			
			psmt.setDouble(++i, GenericUtil.Round(summaryDto.getTotalMargin(), 2));
			psmt.setDouble(++i, GenericUtil.Round(summaryDto.getRegularMargin(), 2));
			psmt.setDouble(++i, GenericUtil.Round(summaryDto.getSaleMargin(), 2));
			psmt.setDouble(++i, GenericUtil.Round(summaryDto.getTotalMarginPer(), 2));
			psmt.setDouble(++i, GenericUtil.Round(summaryDto.getRegularMarginPer(), 2));
			psmt.setDouble(++i, GenericUtil.Round(summaryDto.getSaleMarginPer(), 2));
			
			psmt.setDouble(++i, summaryDto.getlastAggrSalesId());
			psmt.setDouble(++i, GenericUtil.Round(summaryDto.getRegularMovement(), 2));
			psmt.setDouble(++i, GenericUtil.Round(summaryDto.getSaleMovement(), 2));
			
			// code added for movement by volume process
			psmt.setDouble(++i, summaryDto.getregMovementVolume());
			psmt.setDouble(++i, summaryDto.getsaleMovementVolume());
			psmt.setDouble(++i, summaryDto.gettotMovementVolume());
			psmt.setDouble(++i, summaryDto.getigRegVolumeRev());
			psmt.setDouble(++i, summaryDto.getigSaleVolumeRev());
			psmt.setDouble(++i, summaryDto.getigtotVolumeRev());
			psmt.setObject(++i, summaryDto.getStoreStatus());
			psmt.setObject(++i, summaryDto.getlastAggrCalendarId());
			
			//Metrics at Private label level
			psmt.setDouble(++i, GenericUtil.Round(summaryDto.getTotalRevenue(), 2));
			psmt.setDouble(++i, GenericUtil.Round(summaryDto.getRegularRevenue(), 2));
			psmt.setDouble(++i, GenericUtil.Round(summaryDto.getSaleRevenue(), 2));
			
			psmt.setDouble(++i, GenericUtil.Round(summaryDto.getTotalMargin(), 2));
			psmt.setDouble(++i, GenericUtil.Round(summaryDto.getRegularMargin(), 2));
			psmt.setDouble(++i, GenericUtil.Round(summaryDto.getSaleMargin(), 2));
			
			psmt.setDouble(++i, GenericUtil.Round(summaryDto.getTotalMarginPer(), 2));
			psmt.setDouble(++i, GenericUtil.Round(summaryDto.getRegularMarginPer(), 2));
			psmt.setDouble(++i, GenericUtil.Round(summaryDto.getSaleMarginPer(), 2));
			
			psmt.setDouble(++i, GenericUtil.Round(summaryDto.getTotalMovement(), 2));
			psmt.setDouble(++i, GenericUtil.Round(summaryDto.getRegularMovement(), 2));
			psmt.setDouble(++i, GenericUtil.Round(summaryDto.getSaleMovement(), 2));
			
			psmt.setDouble(++i, summaryDto.getregMovementVolume());
			psmt.setDouble(++i, summaryDto.getsaleMovementVolume());
			psmt.setDouble(++i, summaryDto.gettotMovementVolume());
			
			psmt.setDouble(++i, GenericUtil.Round(summaryDto.getTotalVisitCount(), 2));
			psmt.setDouble(++i,GenericUtil.Round(summaryDto.getAverageOrderSize(), 2));
			
			psmt.addBatch();
		} catch (Exception exe) {
			logger.error("....Error in sales Aggregation Dao....", exe);
			throw new GeneralException(" Error in sales Aggregation Dao....",
					exe);
		}
	}

	private void updateSqlBatch(SummaryDataDTO summaryDto, PreparedStatement psmt) throws GeneralException {
		try {
			int i=0;
			//Metrics at Private label level
			psmt.setDouble(++i, GenericUtil.Round(summaryDto.getPLTotalRevenue(), 2));
			psmt.setDouble(++i, GenericUtil.Round(summaryDto.getPLRegularRevenue(), 2));
			psmt.setDouble(++i, GenericUtil.Round(summaryDto.getPLSaleRevenue(), 2));
			
			psmt.setDouble(++i, GenericUtil.Round(summaryDto.getPLTotalMargin(), 2));
			psmt.setDouble(++i, GenericUtil.Round(summaryDto.getPLRegularMargin(), 2));
			psmt.setDouble(++i, GenericUtil.Round(summaryDto.getPLSaleMargin(), 2));
			
			psmt.setDouble(++i, GenericUtil.Round(summaryDto.getPLTotalMarginPer(), 2));
			psmt.setDouble(++i, GenericUtil.Round(summaryDto.getPLRegularMarginPer(), 2));
			psmt.setDouble(++i, GenericUtil.Round(summaryDto.getPLSaleMarginPer(), 2));
			
			psmt.setDouble(++i, GenericUtil.Round(summaryDto.getPLTotalMovement(), 2));
			psmt.setDouble(++i, GenericUtil.Round(summaryDto.getPLRegularMovement(), 2));
			psmt.setDouble(++i, GenericUtil.Round(summaryDto.getPLSaleMovement(), 2));
			
			psmt.setDouble(++i, summaryDto.getPLtotMovementVolume());
			psmt.setDouble(++i, summaryDto.getPLregMovementVolume());
			psmt.setDouble(++i, summaryDto.getPLsaleMovementVolume());

			psmt.setDouble(++i, GenericUtil.Round(summaryDto.getPLTotalVisitCount(), 2));
			psmt.setDouble(++i,GenericUtil.Round(summaryDto.getPLAverageOrderSize(), 2));

			psmt.setObject(++i, summaryDto.getcalendarId());
			psmt.setObject(++i, Constants.STORE_LEVEL_ID);
			psmt.setObject(++i, summaryDto.getLocationId());

			if( summaryDto.getProductLevelId() == 0)
				psmt.setInt(++i, -1);
			else
				psmt.setInt(++i, summaryDto.getProductLevelId());
			
			if (summaryDto.getProductId() == null || summaryDto.getProductId().equalsIgnoreCase(""))
				psmt.setInt(++i, -1);
			else
				psmt.setObject(++i, Integer.parseInt( summaryDto.getProductId() )); 
			
			psmt.addBatch();
		} catch (Exception exe) {
			logger.error("....Error in sales Aggregation Dao....", exe);
			throw new GeneralException(" Error in sales Aggregation Dao....",
					exe);
		}
	}

}
