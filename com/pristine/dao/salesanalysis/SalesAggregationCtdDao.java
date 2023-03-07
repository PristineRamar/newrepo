package com.pristine.dao.salesanalysis;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.text.DateFormat;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.HashMap;
import org.apache.log4j.Logger;
import com.pristine.business.entity.SalesaggregationbusinessV2;
import com.pristine.dto.salesanalysis.SummaryDataDTO;
import com.pristine.exception.GeneralException;
import com.pristine.util.GenericUtil;
import com.pristine.util.PristineDBUtil;

public class SalesAggregationCtdDao {
	
	
	static Logger logger = Logger.getLogger("SalesAggregationCtdDao");
	/*
	 * ****************************************************************
	 * Delete the Previous Aggergation In Sales_Aggr_Ctd Table
	 * Argument 1 : Connection
	 * Argument 2 : locationId
	 * Argument 3 : Calendar List
	 * Argument 4 : Table Name
	 * catch GeneralException , SqlException .
	 * ****************************************************************
	 */

	public void deletePreviousCtdAggregation(Connection _conn, int locationId,
			int locationLevelId, String startDate,String endDate,
			String tableName, int ctdConstant) throws GeneralException, ParseException {
		
		try {
			DateFormat formatter = new SimpleDateFormat("yyyy-MM-dd");
			
			logger.debug(startDate);
			logger.debug(endDate);
			
			Date sDate = (Date) formatter.parse(startDate);
			Date eDate = (Date) formatter.parse(endDate);
			
			// Sales_aggr_ctd delete
			StringBuffer ctdSql = new StringBuffer();
			ctdSql.append(" delete from SALES_AGGR_CTD WHERE SUMMARY_CTD_ID in");
			ctdSql.append("(SELECT SUMMARY_CTD_ID  FROM "+ tableName + " ");
			ctdSql.append(" where CALENDAR_ID in(");
			ctdSql.append(" SELECT calendar_id FROM retail_calendar");
			ctdSql.append(" WHERE start_date >=to_date('"+formatter.format(sDate)+"','yyyy-mm-dd') ");
			ctdSql.append(" AND start_date<=to_date('"+formatter.format(eDate)+"', 'yyyy-mm-dd') and row_type='D')");
			ctdSql.append(" AND LOCATION_ID  ='" + locationId + "' and ");
			ctdSql.append(" LOCATION_LEVEL_ID='" + locationLevelId + "')  ");
			ctdSql.append(" and CTD_TYPE = '" +ctdConstant+ "'");

			logger.debug("deletePreviousCtdAggregation SQL:" + ctdSql.toString());
			PristineDBUtil.executeUpdate(_conn, ctdSql,
									"deletePreviousCtdAggregation");
		} catch (GeneralException ge) {
			logger.error("Error while deleting CTD data" + ge.getMessage());
			throw ge;
		}
	}
	
	
	

	/*
	 * ****************************************************************
	 * Insert the values into sales_aggr_ctd
	 * Argument 1 : Connection
	 * Argument 2 : Week Calendar Id
	 * Argument 3 : productMap
	 * Returns List<SummaryDataDTO> Weekly List
	 * catch GeneralException , SqlException .
	 * ****************************************************************
	 */
	
	public void insertSalesCtd(Connection _conn,
			HashMap<String, SummaryDataDTO> dailyMap, int ctdType)
			throws  GeneralException {
		
		try{
			SalesaggregationbusinessV2 businessLogic = new SalesaggregationbusinessV2();
		 
			PreparedStatement psmt = _conn
					.prepareStatement(processSql());

			Object[] outerLoop = dailyMap.values().toArray();
			for (int ii = 0; ii < outerLoop.length; ii++) {
				SummaryDataDTO summaryDto = (SummaryDataDTO) outerLoop[ii];
				
				if (summaryDto.getsummaryCtdId() != 0) {
			
					// calculate the other metrics info
					businessLogic.CalculateMetrixInformation(summaryDto,true);
													
					// add the values into psmt batch
					addSqlBatch(summaryDto, true, psmt, ctdType);
			}
			}

			// Prepare the Store Aggregation records
			int[] ccount = psmt.executeBatch();
			logger.debug("No of Records Insert in CTD:" + ccount.length);
			psmt.close();
		}catch(Exception exe){
			logger.error("Add ctd" , exe);
			throw new GeneralException("Add ctd" ,exe);
		}
	}
	
	
	/*
	 * ****************************************************************
	 * Summary Weekly insert and Update Sql
	 * Argument 1 : sqlmode
	 * returns Query
	 * catch GeneralException , SqlException .
	 * ****************************************************************
	 */
	
	
	private void addSqlBatch(SummaryDataDTO summaryDto, boolean insertMode,
			PreparedStatement psmt, int ctdType) throws SQLException, GeneralException {
		
		try{
		psmt.setObject(1,summaryDto.getsummaryCtdId());
		psmt.setObject(2,ctdType);
		psmt.setDouble(3,GenericUtil.Round(summaryDto.getTotalVisitCount(),2));
		psmt.setDouble(4,GenericUtil.Round(summaryDto.getAverageOrderSize(),2));
		psmt.setDouble(5,GenericUtil.Round(summaryDto.getTotalMovement(),2));
	    psmt.setDouble(6,GenericUtil.Round(summaryDto.getRegularMovement(),2));
	    psmt.setDouble(7,GenericUtil.Round( summaryDto.getSaleMovement(),2));
	    psmt.setDouble(8,GenericUtil.Round(summaryDto.getTotalRevenue(),2));
	    psmt.setDouble(9,GenericUtil.Round(summaryDto.getRegularRevenue(),2));
	    psmt.setDouble(10,GenericUtil.Round(summaryDto.getSaleRevenue(),2));
	   
	    psmt.setDouble(11, GenericUtil.Round(summaryDto.getTotalMargin(), 2));
		psmt.setDouble(12, GenericUtil.Round(summaryDto.getRegularMargin(), 2));
		psmt.setDouble(13, GenericUtil.Round(summaryDto.getSaleMargin(), 2));
		psmt.setDouble(14, GenericUtil.Round(summaryDto.getTotalMarginPer(), 2));
		psmt.setDouble(15, GenericUtil.Round(summaryDto.getRegularMarginPer(), 2));
		psmt.setDouble(16, GenericUtil.Round(summaryDto.getSaleMarginPer(), 2));
	    
	    psmt.setObject(17,GenericUtil.Round(summaryDto.getitotalMovement(),2));
		psmt.setObject(18,GenericUtil.Round(summaryDto.getitotalRevenue(),2));
		psmt.setObject(19,GenericUtil.Round(summaryDto.getiregularRevenue(),2));
		psmt.setObject(20,GenericUtil.Round(summaryDto.getisaleRevenue(),2));
		
		psmt.setDouble(21, GenericUtil.Round(summaryDto.getitotalMargin(), 2));
		psmt.setDouble(22, GenericUtil.Round(summaryDto.getiregularMargin(), 2));
		psmt.setDouble(23, GenericUtil.Round(summaryDto.getisaleMargin(), 2));
		psmt.setDouble(24, GenericUtil.Round(summaryDto.getitotalMarginPer(), 2));
		psmt.setDouble(25, GenericUtil.Round(summaryDto.getiregularMarginPer(), 2));
		psmt.setDouble(26, GenericUtil.Round(summaryDto.getisaleMarginPer(), 2));
		psmt.setObject(27,GenericUtil.Round(summaryDto.getiregularMovement(),2));
		psmt.setObject(28,GenericUtil.Round(summaryDto.getisaleMovement(),2));
		
		// Code added for Movement By volume.....
		psmt.setObject(29, GenericUtil.Round(summaryDto.getregMovementVolume(),2));
		psmt.setObject(30, GenericUtil.Round(summaryDto.getsaleMovementVolume(),2));
		psmt.setObject(31, GenericUtil.Round(summaryDto.getigRegVolumeRev(),2));
		psmt.setObject(32, GenericUtil.Round(summaryDto.getigSaleVolumeRev(),2));
		psmt.setObject(33, GenericUtil.Round(summaryDto.gettotMovementVolume(),2));
		psmt.setObject(34, GenericUtil.Round(summaryDto.getigtotVolumeRev(),2));
		
		psmt.setObject(35, GenericUtil.Round(summaryDto.getidRegMovementVolume(),2));
		psmt.setObject(36, GenericUtil.Round(summaryDto.getidSaleMovementVolume(),2));
		psmt.setObject(37, GenericUtil.Round(summaryDto.getidIgRegVolumeRev(),2));
		psmt.setObject(38, GenericUtil.Round(summaryDto.getidIgSaleVolumeRev(),2));
		psmt.setObject(39, GenericUtil.Round(summaryDto.getidTotMovementVolume(),2));
		psmt.setObject(40, GenericUtil.Round(summaryDto.getIdIgtotVolumeRev(),2));
		psmt.setObject(41,GenericUtil.Round(summaryDto.getitotalVisitCount(),2));
		psmt.setObject(42,GenericUtil.Round(summaryDto.getiaverageOrderSize(),2));
		
	    psmt.addBatch();
		}catch(Exception exe){
			logger.error(" Error in adding the batch...... ", exe);
			throw new GeneralException(" Error in adding the batch...... ", exe);
		} 
		
	}

	 



	private String processSql() {

		StringBuffer sql = new StringBuffer();

		sql.append(" insert into SALES_AGGR_CTD(SUMMARY_CTD_ID, CTD_TYPE, TOT_VISIT_CNT, AVG_ORDER_SIZE , TOT_MOVEMENT,REG_MOVEMENT ");
		
		sql.append(", SALE_MOVEMENT , TOT_REVENUE , REG_REVENUE, SALE_REVENUE, TOT_MARGIN  ");
		
		sql.append(" , REG_MARGIN , SALE_MARGIN , TOT_MARGIN_PCT , REG_MARGIN_PCT  ");
		
		sql.append(" , SALE_MARGIN_PCT,ID_TOT_MOVEMENT,ID_TOT_REVENUE,ID_REG_REVENUE,ID_SALE_REVENUE");
		
		sql.append(" ,ID_TOT_MARGIN,ID_REG_MARGIN,ID_SALE_MARGIN,ID_TOT_MARGIN_PCT,ID_REG_MARGIN_PCT,ID_SALE_MARGIN_PCT");
		
		sql.append(" ,ID_REG_MOEVEMNT,ID_SALE_MOVEMENT");
		
		// code added for Movement By Volume Proces....
		
		sql.append(" ,REG_MOVEMENT_VOL,SALE_MOVEMENT_VOL,REG_IGVOL_REVENUE,SALE_IGVOL_REVENUE");
		
		sql.append(" ,TOT_MOVEMENT_VOL,TOT_IGVOL_REVENUE,ID_REG_MOVEMENT_VOL,ID_SALE_MOVEMENT_VOL");
		
		sql.append(" ,ID_REG_IGVOL_REVENUE,ID_SALE_IGVOL_REVENUE,ID_TOT_MOVEMENT_VOL,ID_TOT_IGVOL_REVENUE");
		
		sql.append(" ,ID_TOT_VISIT_CNT,ID_AVG_ORDER_SIZE)");
				
		sql.append(" values (?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?");
		
		sql.append(" ,?,?,?,?,?,?,?,?,?,?,?,?)");

		logger.debug("Sales Aggr CTD -SQL: " + sql.toString());
		return sql.toString();
	}

	

}
