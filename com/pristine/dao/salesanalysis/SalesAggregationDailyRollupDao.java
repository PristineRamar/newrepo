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

import com.pristine.business.entity.SalesaggregationbusinessV2;
import com.pristine.dto.salesanalysis.SummaryDataDTO;
import com.pristine.exception.GeneralException;
import com.pristine.util.Constants;
import com.pristine.util.GenericUtil;
import com.pristine.util.PristineDBUtil;

public class SalesAggregationDailyRollupDao {
	
	static Logger logger = Logger.getLogger("SummaryRollupDao");
	
	/*
	 * ****************************************************************
	 * Method used to get the district level aggregation records
	 * Argument 1 : Connection
	 * Argument 2 : Calendar Id
	 * Argument 3 : DistrictId
	 * @catch GeneralException
	 * ****************************************************************
	 */

	public List<SummaryDataDTO> getDistrictAggrData(Connection _conn,
			int calendarId, String processId) {
			 		
		List<SummaryDataDTO> districtMap = new ArrayList<SummaryDataDTO>();             // List hold the district level records
		
		StringBuffer sql = new StringBuffer();
		
				sql.append(" select SA.PRODUCT_LEVEL_ID,SA.PRODUCT_ID") ;
				sql.append(" ,CS.DISTRICT_ID as locationId");
				sql.append(" ,sum(SA.TOT_VISIT_CNT) as TOT_VISIT_CNT ");
				sql.append(" ,sum(SA.TOT_MOVEMENT) as TOT_MOVEMENT");
				sql.append(" ,sum(SA.REG_MOVEMENT) as REG_MOVEMENT");
				sql.append(" ,sum(SA.SALE_MOVEMENT) as SALE_MOVEMENT ");
				sql.append(" ,sum(SA.REG_REVENUE) as REG_REVENUE");
				sql.append(" ,sum(SA.SALE_REVENUE) as SALE_REVENUE");
				sql.append(" ,sum(SA.TOT_MARGIN) as TOT_MARGIN ");
				sql.append(" ,sum(SA.REG_MARGIN) as REG_MARGIN");
				sql.append(" ,sum(SA.SALE_MARGIN) as SALE_MARGIN");
				sql.append(" ,sum(SA.TOT_REVENUE) as TOT_REVENUE");
				// Code added for Movement By Volume
				sql.append(" ,sum(REG_MOVEMENT_VOL) as REG_MOVEMENT_VOL");
				sql.append(" ,sum(SALE_MOVEMENT_VOL) as SALE_MOVEMENT_VOL");
				sql.append(" ,sum(REG_IGVOL_REVENUE) as REG_IGVOL_REVENUE");
				sql.append(" ,sum(SALE_IGVOL_REVENUE) as SALE_IGVOL_REVENUE");
				sql.append(" ,sum(TOT_MOVEMENT_VOL) as TOT_MOVEMENT_VOL");
				sql.append(" ,sum(TOT_IGVOL_REVENUE) as TOT_IGVOL_REVENUE");
				
				sql.append(" ,SUM(PL_TOT_REVENUE) AS PL_TOT_REVENUE"); 
				sql.append(" ,SUM(PL_REG_REVENUE) AS PL_REG_REVENUE");
				sql.append(" ,SUM(PL_SALE_REVENUE) AS PL_SALE_REVENUE");
				sql.append(" ,SUM(PL_TOT_MARGIN) AS PL_TOT_MARGIN");
				sql.append(" ,SUM(PL_REG_MARGIN) AS PL_REG_MARGIN");
				sql.append(" ,SUM(PL_SALE_MARGIN) AS PL_SALE_MARGIN");
				sql.append(" ,SUM(PL_TOT_MARGIN_PCT) AS PL_TOT_MARGIN_PCT");
				sql.append(" ,SUM(PL_REG_MARGIN_PCT) AS PL_REG_MARGIN_PCT");
				sql.append(" ,SUM(PL_SALE_MARGIN_PCT) AS PL_SALE_MARGIN_PCT");
				sql.append(" ,SUM(PL_TOT_MOVEMENT) AS PL_TOT_MOVEMENT");
				sql.append(" ,SUM(PL_REG_MOVEMENT) AS PL_REG_MOVEMENT");
				sql.append(" ,SUM(PL_SALE_MOVEMENT) AS PL_SALE_MOVEMENT");
				sql.append(" ,SUM(PL_TOT_MOVEMENT_VOL) AS PL_TOT_MOVEMENT_VOL");
				sql.append(" ,SUM(PL_REG_MOVEMENT_VOL) AS PL_REG_MOVEMENT_VOL");
				sql.append(" ,SUM(PL_SALE_MOVEMENT_VOL) AS PL_SALE_MOVEMENT_VOL");
				sql.append(" ,SUM(PL_AVG_ORDER_SIZE) AS PL_AVG_ORDER_SIZE");
				sql.append(" ,SUM(PL_TOT_VISIT_CNT) AS PL_TOT_VISIT_CNT");
				
				sql.append(" from");
				sql.append(" SALES_AGGR_DAILY SA"); 
				sql.append(" Inner Join COMPETITOR_STORE CS ");
				sql.append(" On SA.LOCATION_ID = CS.COMP_STR_ID"); 
				sql.append(" where");
				sql.append(" SA.CALENDAR_ID =" + calendarId + "");
				sql.append(" and CS.DISTRICT_ID =" + processId + "");
				sql.append(" group by CS.DISTRICT_ID, SA.PRODUCT_LEVEL_ID, SA.PRODUCT_ID");
				sql.append(" order by CS.DISTRICT_ID, SA.PRODUCT_LEVEL_ID, SA.PRODUCT_ID");
				
		logger.debug("District SQL" + sql.toString() );
		try{
			// Execute Query
			CachedRowSet resultSet = PristineDBUtil.executeQuery(_conn, sql, "fetchRollUpDaily");
			
			while(resultSet.next()){
				SummaryDataDTO summaryDto = new SummaryDataDTO();
				if(resultSet.getString("PRODUCT_LEVEL_ID")==null)
					summaryDto.setProductLevelId(0);
				else
				summaryDto.setProductLevelId(resultSet.getInt("PRODUCT_LEVEL_ID"));
				summaryDto.setProductId(resultSet.getString("PRODUCT_ID"));
				summaryDto.setTotalVisitCount(resultSet.getDouble("TOT_VISIT_CNT"));
				summaryDto.setTotalMovement(resultSet.getDouble("TOT_MOVEMENT"));
				summaryDto.setRegularMovement(resultSet.getDouble("REG_MOVEMENT"));
				summaryDto.setSaleMovement(resultSet.getDouble("SALE_MOVEMENT"));
				summaryDto.setTotalRevenue(resultSet.getDouble("TOT_REVENUE"));
				summaryDto.setRegularRevenue(resultSet.getDouble("REG_REVENUE"));
				summaryDto.setSaleRevenue(resultSet.getDouble("SALE_REVENUE"));
				summaryDto.setTotalMargin(resultSet.getDouble("TOT_MARGIN"));
				summaryDto.setRegularMargin(resultSet.getDouble("REG_MARGIN"));
				summaryDto.setSaleMargin(resultSet.getDouble("SALE_MARGIN"));
				summaryDto.setLocationId(resultSet.getInt("locationId"));
				
				// code added for Movement By Volume
				summaryDto.setregMovementVolume(resultSet.getDouble("REG_MOVEMENT_VOL"));
				summaryDto.setsaleMovementVolume(resultSet.getDouble("SALE_MOVEMENT_VOL"));
				summaryDto.setigRegVolumeRev(resultSet.getDouble("REG_IGVOL_REVENUE"));
				summaryDto.setigSaleVolumeRev(resultSet.getDouble("SALE_IGVOL_REVENUE"));
				summaryDto.settotMovementVolume(resultSet.getDouble("TOT_MOVEMENT_VOL"));
				summaryDto.setigtotVolumeRev(resultSet.getDouble("TOT_IGVOL_REVENUE"));
				
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
				
				// add the dto into district map
				districtMap.add(summaryDto);
				}
			//logger.debug(" District Map "+ processId + "Map Size" + districtMap.size());
					
		}catch(GeneralException exe){
			logger.error(exe);
		}catch(SQLException sq){
			logger.error(sq);
		}
			 
		return districtMap;   // return the district map
	}

	
	/*
	 * ****************************************************************
	 * Insert the daily rollup records for all level
	 * Argument 1 : Connection
	 * Argument 2 : Calendar Id
	 * Argument 3 : DistrictId
	 * the insert rollup daily method and insert the district level aggregation
	 * Argument 1 : calendarId 
	 * Argument 2 : SummaryRollupDao object
	 * @catch GeneralException
	 * ****************************************************************
	 */
	
	
	
	public boolean insertRollUpDaily(Connection _conn, int calendarId,
			List<SummaryDataDTO> productList, int locationLevelId,
			HashMap<String, Long> lastRollupList,
			HashMap<String, SummaryDataDTO> identicalMap)
			throws GeneralException {
				
 		    // Object for Sales aggregation business
			SalesaggregationbusinessV2 objSalesBusiness = new SalesaggregationbusinessV2();
			boolean insertFlag=false;
			try{
				PreparedStatement psmt = _conn.prepareStatement(insertSql());
					
			// Iterate the product List
			for (int iD = 0; iD < productList.size(); iD++) {
				
				SummaryDataDTO summaryDto = (SummaryDataDTO) productList.get(iD);
				summaryDto.setLocationLevelId(locationLevelId);
				summaryDto.setcalendarId(calendarId);
				
				
				if (summaryDto.getProductLevelId() == 0){
					//logger.info("Store level......" + summaryDto.getProductLevelId());
					
					if (lastRollupList.containsKey(locationLevelId + "_"
							+ summaryDto.getProductLevelId() + "_"
							+ null)) {
						summaryDto.setlastAggrSalesId(lastRollupList
								.get(locationLevelId + "_"
										+ summaryDto.getProductLevelId() + "_"
										+ null));
					}					
				}
				else
				{
					//logger.info("Product level......" + summaryDto.getProductLevelId());
					if (lastRollupList.containsKey(locationLevelId + "_"
							+ summaryDto.getProductLevelId() + "_"
							+ summaryDto.getProductId())) {
						summaryDto.setlastAggrSalesId(lastRollupList
								.get(locationLevelId + "_"
										+ summaryDto.getProductLevelId() + "_"
										+ summaryDto.getProductId()));
					}
				}
				
				if (lastRollupList.containsKey(locationLevelId + "_"
						+ summaryDto.getProductLevelId() + "_"
						+ null)) {
					summaryDto.setlastAggrSalesId(lastRollupList
							.get(locationLevelId + "_"
									+ summaryDto.getProductLevelId() + "_"
									+ null));
				}

				// calculate the other metrix information
				objSalesBusiness.CalculateMetrixInformation(summaryDto,true);
					
				if (locationLevelId == Constants.DISTRICT_LEVEL_ID) {
					
					if (identicalMap.containsKey(summaryDto.getProductLevelId()
							+ "_" + summaryDto.getProductId() + "_"
							+ summaryDto.getLocationId())) {

						SummaryDataDTO identicalDto = identicalMap
								.get(summaryDto.getProductLevelId() + "_"
										+ summaryDto.getProductId() + "_"
										+ summaryDto.getLocationId());

						objSalesBusiness.CalculateMetrixInformation(
								identicalDto, true);

						addIdenticalValues(summaryDto, identicalDto);

					}
					}
									
					// add the values into batch
					addSqlBatch(summaryDto, psmt);
				}
		 
			psmt.executeBatch();
			//logger.info(" Insert Count of productMap  " + count.length);
			insertFlag=true;
			psmt.close();
			
		 }
			catch(Exception exe){
			 logger.error("Error while inserting daily roll up data:" , exe);
			 throw new GeneralException("insertRollUpDaily" , exe);
		}
		return insertFlag;
	}

	
	
	/*
	 * ****************************************************************
	 * Insert the daily rollup records for all level
	 * Argument 1 : Connection
	 * Argument 2 : Calendar Id
	 * Argument 3 : DistrictId
	 * the insert rollup daily method and insert the district level aggregation
	 * Argument 1 : calendarId 
	 * Argument 2 : SummaryRollupDao object
	 * @catch GeneralException
	 * ****************************************************************
	 */
	public boolean updateRollUpDaily(Connection _conn, int calendarId,
			List<SummaryDataDTO> productList, int locationLevelId,
			HashMap<String, Long> lastRollupList,
			HashMap<String, SummaryDataDTO> identicalMap)
			throws GeneralException {
				
 		    // Object for Sales aggregation business
			SalesaggregationbusinessV2 objSalesBusiness = new SalesaggregationbusinessV2();
			boolean insertFlag=false;
			try{
				PreparedStatement psmt = _conn.prepareStatement(updateSql());
					
			// Iterate the product List
			for (int iD = 0; iD < productList.size(); iD++) {
				
				SummaryDataDTO summaryDto = (SummaryDataDTO) productList.get(iD);
				summaryDto.setLocationLevelId(locationLevelId);
				summaryDto.setcalendarId(calendarId);
				
				
				if (summaryDto.getProductLevelId() == 0){
					//logger.info("Store level......" + summaryDto.getProductLevelId());
					
					if (lastRollupList.containsKey(locationLevelId + "_"
							+ summaryDto.getProductLevelId() + "_"
							+ null)) {
						summaryDto.setlastAggrSalesId(lastRollupList
								.get(locationLevelId + "_"
										+ summaryDto.getProductLevelId() + "_"
										+ null));
					}					
				}
				else
				{
					//logger.info("Product level......" + summaryDto.getProductLevelId());
					if (lastRollupList.containsKey(locationLevelId + "_"
							+ summaryDto.getProductLevelId() + "_"
							+ summaryDto.getProductId())) {
						summaryDto.setlastAggrSalesId(lastRollupList
								.get(locationLevelId + "_"
										+ summaryDto.getProductLevelId() + "_"
										+ summaryDto.getProductId()));
					}
				}
				
				if (lastRollupList.containsKey(locationLevelId + "_"
						+ summaryDto.getProductLevelId() + "_"
						+ null)) {
					summaryDto.setlastAggrSalesId(lastRollupList
							.get(locationLevelId + "_"
									+ summaryDto.getProductLevelId() + "_"
									+ null));
				}

				// calculate the other metrics information
				objSalesBusiness.CalculateMetrixInformation(summaryDto,true);
					
				if (locationLevelId == Constants.DISTRICT_LEVEL_ID) {
					
					if (identicalMap.containsKey(summaryDto.getProductLevelId()
							+ "_" + summaryDto.getProductId() + "_"
							+ summaryDto.getLocationId())) {

						SummaryDataDTO identicalDto = identicalMap
								.get(summaryDto.getProductLevelId() + "_"
										+ summaryDto.getProductId() + "_"
										+ summaryDto.getLocationId());

						objSalesBusiness.CalculateMetrixInformation(
								identicalDto, true);

						addIdenticalValues(summaryDto, identicalDto);

					}
					}
									
					// add the values into batch
					updateSqlBatch(summaryDto, psmt);
				}
		 
			psmt.executeBatch();
			//logger.info(" Insert Count of productMap  " + count.length);
			insertFlag=true;
			psmt.close();
			
		 }
			catch(Exception exe){
			 logger.error("Error while inserting daily roll up data:" , exe);
			 throw new GeneralException("insertRollUpDaily" , exe);
		}
		return insertFlag;
	}
	
	
	
	
	
	
	private void addIdenticalValues(SummaryDataDTO allStoreDto,
			SummaryDataDTO identicalStoreDto) {
		 allStoreDto.setiaverageOrderSize(identicalStoreDto.getAverageOrderSize());
		 allStoreDto.setiregularMargin(identicalStoreDto.getRegularMargin());
		 allStoreDto.setiregularMarginPer(identicalStoreDto.getRegularMarginPer());
		 allStoreDto.setiregularMovement(identicalStoreDto.getRegularMovement());
		 allStoreDto.setiregularRevenue(identicalStoreDto.getRegularRevenue());
		 allStoreDto.setisaleMargin(identicalStoreDto.getSaleMargin());
		 allStoreDto.setisaleMarginPer(identicalStoreDto.getSaleMarginPer());
		 allStoreDto.setisaleMovement(identicalStoreDto.getSaleMovement());
		 allStoreDto.setisaleRevenue(identicalStoreDto.getSaleRevenue());
		 allStoreDto.setitotalMargin(identicalStoreDto.getTotalMargin());
		 allStoreDto.setitotalMarginPer(identicalStoreDto.getTotalMarginPer());
		 allStoreDto.setitotalMovement(identicalStoreDto.getTotalMovement());
		 allStoreDto.setitotalRevenue(identicalStoreDto.getTotalRevenue());
		 allStoreDto.setitotalVisitCount(identicalStoreDto.getTotalVisitCount());
		 
		// code added for Movement By Volume
		 allStoreDto.setidRegMovementVolume(identicalStoreDto.getregMovementVolume());
		 allStoreDto.setidSaleMovementVolume(identicalStoreDto.getsaleMovementVolume());
		 allStoreDto.setidIgRegVolumeRev(identicalStoreDto.getigRegVolumeRev());
		 allStoreDto.setidIgSaleVolumeRev(identicalStoreDto.getigSaleVolumeRev());
		 allStoreDto.setIdIgtotVolumeRev(identicalStoreDto.getigtotVolumeRev());
		 allStoreDto.setidTotMovementVolume(identicalStoreDto.gettotMovementVolume());
		 
		 // For private label metrics
		 allStoreDto.setPLitotalRevenue(identicalStoreDto.getPLTotalRevenue());
		 allStoreDto.setPLiregularRevenue(identicalStoreDto.getPLRegularRevenue());
		 allStoreDto.setPLisaleRevenue(identicalStoreDto.getPLSaleRevenue());
		 allStoreDto.setPLitotalMargin(identicalStoreDto.getPLTotalMargin());
		 allStoreDto.setPLiregularMargin(identicalStoreDto.getPLRegularMargin());
		 allStoreDto.setPLisaleMargin(identicalStoreDto.getPLSaleMargin());
		 allStoreDto.setPLitotalMarginPer(identicalStoreDto.getPLTotalMarginPer());
		 allStoreDto.setPLiregularMarginPer(identicalStoreDto.getPLRegularMarginPer());
		 allStoreDto.setPLisaleMarginPer(identicalStoreDto.getPLSaleMarginPer());
		 allStoreDto.setPLitotalMovement(identicalStoreDto.getPLTotalMovement());
		 allStoreDto.setPLiregularMovement(identicalStoreDto.getPLRegularMovement());
		 allStoreDto.setPLisaleMovement(identicalStoreDto.getPLSaleMovement());
		 allStoreDto.setPLidTotMovementVolume(identicalStoreDto.getPLtotMovementVolume());
		 allStoreDto.setPLidRegMovementVolume(identicalStoreDto.getPLregMovementVolume());
		 allStoreDto.setPLidSaleMovementVolume(identicalStoreDto.getPLsaleMovementVolume());
		 allStoreDto.setPLitotalVisitCount(identicalStoreDto.getPLTotalVisitCount());
		 allStoreDto.setPLiaverageOrderSize(identicalStoreDto.getPLAverageOrderSize());
	}

	private String insertSql() {

		StringBuffer sql = new StringBuffer();
		
		sql.append(" insert into SALES_AGGR_DAILY_ROLLUP (SALES_AGGR_DAILY_ROLLUP_ID, CALENDAR_ID, LOCATION_LEVEL_ID, LOCATION_ID,PRODUCT_LEVEL_ID ");

		sql.append(", PRODUCT_ID , TOT_VISIT_CNT ,AVG_ORDER_SIZE,TOT_MOVEMENT, TOT_REVENUE , REG_REVENUE  ");

		sql.append(" , SALE_REVENUE ,TOT_MARGIN ,REG_MARGIN , SALE_MARGIN  ");

		sql.append(", TOT_MARGIN_PCT, REG_MARGIN_PCT , SALE_MARGIN_PCT ,REG_MOVEMENT,SALE_MOVEMENT,SUMMARY_CTD_ID,LST_SALES_AGGR_ROLLUP_ID ");
		
		sql.append(" ,ID_TOT_VISIT_CNT,ID_AVG_ORDER_SIZE,ID_TOT_MOVEMENT,ID_TOT_REVENUE,ID_REG_REVENUE,ID_SALE_REVENUE");
		
		sql.append(" ,ID_TOT_MARGIN,ID_REG_MARGIN,ID_SALE_MARGIN,ID_TOT_MARGIN_PCT,ID_REG_MARGIN_PCT,ID_SALE_MARGIN_PCT");
		
		sql.append(" ,ID_REG_MOEVEMNT,ID_SALE_MOVEMENT");
		
		sql.append(" ,REG_MOVEMENT_VOL,SALE_MOVEMENT_VOL,REG_IGVOL_REVENUE,SALE_IGVOL_REVENUE");
		
		sql.append(" ,TOT_MOVEMENT_VOL,TOT_IGVOL_REVENUE,ID_REG_MOVEMENT_VOL,ID_SALE_MOVEMENT_VOL");
		
		sql.append(" ,ID_REG_IGVOL_REVENUE,ID_SALE_IGVOL_REVENUE,ID_TOT_MOVEMENT_VOL,ID_TOT_IGVOL_REVENUE,");
		
		sql.append(" PL_TOT_REVENUE, PL_REG_REVENUE, PL_SALE_REVENUE,");
		sql.append(" PL_TOT_MARGIN, PL_REG_MARGIN, PL_SALE_MARGIN,");
		sql.append(" PL_TOT_MARGIN_PCT, PL_REG_MARGIN_PCT, PL_SALE_MARGIN_PCT,");
		sql.append(" PL_TOT_MOVEMENT, PL_REG_MOVEMENT, PL_SALE_MOVEMENT,");
		sql.append(" PL_TOT_MOVEMENT_VOL, PL_REG_MOVEMENT_VOL, PL_SALE_MOVEMENT_VOL,");
		sql.append(" PL_TOT_VISIT_CNT, PL_AVG_ORDER_SIZE,");
		
		sql.append(" PL_ID_TOT_REVENUE,PL_ID_REG_REVENUE, PL_ID_SALE_REVENUE,");
		sql.append(" PL_ID_TOT_MARGIN,PL_ID_REG_MARGIN,PL_ID_SALE_MARGIN,");
		sql.append(" PL_ID_TOT_MARGIN_PCT,PL_ID_REG_MARGIN_PCT,PL_ID_SALE_MARGIN_PCT,");
		sql.append(" PL_ID_TOT_MOVEMENT,PL_ID_REG_MOVEMENT,PL_ID_SALE_MOVEMENT,");
		sql.append(" PL_ID_TOT_MOVEMENT_VOL,PL_ID_REG_MOVEMENT_VOL,PL_ID_SALE_MOVEMENT_VOL,");
		sql.append(" PL_ID_TOT_VISIT_CNT,PL_ID_AVG_ORDER_SIZE)");

		//sql.append(" values (SALES_AGGR_DAILY_ROLLUP_SEQ.NEXTVAL,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,SALES_AGGR_CTD_SEQ.NEXTVAL,?");
		sql.append(" values (SALES_AGGR_DAILY_ROLLUP_SEQ.NEXTVAL,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,NULL,?");
		
		sql.append(" ,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,");
		sql.append("?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,");
		sql.append("?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)");

		 logger.debug("insertSql SQL:" +sql.toString()) ;

		return sql.toString();
	}

	private String updateSql() {

		StringBuffer sql = new StringBuffer();
		
		sql.append(" UPDATE SALES_AGGR_DAILY_ROLLUP SET ");
		sql.append(" PL_TOT_REVENUE=?, PL_REG_REVENUE=?, PL_SALE_REVENUE=?,");
		sql.append(" PL_TOT_MARGIN=?, PL_REG_MARGIN=?, PL_SALE_MARGIN=?,");
		sql.append(" PL_TOT_MARGIN_PCT=?, PL_REG_MARGIN_PCT=?, PL_SALE_MARGIN_PCT=?,");
		sql.append(" PL_TOT_MOVEMENT=?, PL_REG_MOVEMENT=?, PL_SALE_MOVEMENT=?,");
		sql.append(" PL_TOT_MOVEMENT_VOL=?, PL_REG_MOVEMENT_VOL=?, PL_SALE_MOVEMENT_VOL=?,");
		sql.append(" PL_TOT_VISIT_CNT=?, PL_AVG_ORDER_SIZE=?,");
		sql.append(" PL_ID_TOT_REVENUE=?,PL_ID_REG_REVENUE=?, PL_ID_SALE_REVENUE=?,");
		sql.append(" PL_ID_TOT_MARGIN=?, PL_ID_REG_MARGIN=?, PL_ID_SALE_MARGIN=?,");
		sql.append(" PL_ID_TOT_MARGIN_PCT=?, PL_ID_REG_MARGIN_PCT=?, PL_ID_SALE_MARGIN_PCT=?,");
		sql.append(" PL_ID_TOT_MOVEMENT=?,PL_ID_REG_MOVEMENT=?, PL_ID_SALE_MOVEMENT=?,");
		sql.append(" PL_ID_TOT_MOVEMENT_VOL=?, PL_ID_REG_MOVEMENT_VOL=?, PL_ID_SALE_MOVEMENT_VOL=?,");
		sql.append(" PL_ID_TOT_VISIT_CNT=?, PL_ID_AVG_ORDER_SIZE=? ");
		sql.append(" WHERE CALENDAR_ID=? AND LOCATION_LEVEL_ID=? AND LOCATION_ID=?");
		sql.append(" AND NVL(PRODUCT_LEVEL_ID, -1) = ?");
		sql.append(" AND NVL(PRODUCT_ID, -1) = ?");

		 logger.debug("updateSql SQL:" +sql.toString()) ;

		return sql.toString();
	}
 

	/*
	 * ****************************************************************
	 * add the values into PreparedStatement
	 * ****************************************************************
	 */
	public void addSqlBatch(SummaryDataDTO summaryDto, PreparedStatement psmt)
			throws GeneralException {
		try {

			psmt.setObject(1, summaryDto.getcalendarId());
			psmt.setObject(2, summaryDto.getLocationLevelId());
			psmt.setObject(3, summaryDto.getLocationId());
			if (summaryDto.getProductLevelId() != 0)
				psmt.setInt(4, summaryDto.getProductLevelId());
			else
				psmt.setNull(4, java.sql.Types.INTEGER);
			psmt.setString(5, summaryDto.getProductId());
			
			psmt.setDouble(6, GenericUtil.Round(summaryDto.getTotalVisitCount(), 2));
			psmt.setDouble(7, GenericUtil.Round(summaryDto.getAverageOrderSize(), 2));
			psmt.setDouble(8, GenericUtil.Round(summaryDto.getTotalMovement(),2));
			psmt.setDouble(9, GenericUtil.Round(summaryDto.getTotalRevenue(),2));
			psmt.setDouble(10,GenericUtil.Round(summaryDto.getRegularRevenue(),2));
			psmt.setDouble(11,GenericUtil.Round(summaryDto.getSaleRevenue(),2));
			
			
			// margin calculation
			psmt.setDouble(12, GenericUtil.Round(summaryDto.getTotalMargin(), 2));
			psmt.setDouble(13, GenericUtil.Round(summaryDto.getRegularMargin(), 2));
			psmt.setDouble(14, GenericUtil.Round(summaryDto.getSaleMargin(), 2));
			psmt.setDouble(15, GenericUtil.Round(summaryDto.getTotalMarginPer(), 2));
			psmt.setDouble(16, GenericUtil.Round(summaryDto.getRegularMarginPer(), 2));
			psmt.setDouble(17, GenericUtil.Round(summaryDto.getSaleMarginPer(), 2));
			
			psmt.setDouble(18, GenericUtil.Round(summaryDto.getRegularMovement(), 2));
			psmt.setObject(19, GenericUtil.Round(summaryDto.getSaleMovement(),2));
			psmt.setLong(20, summaryDto.getlastAggrSalesId());
			psmt.setDouble(21, GenericUtil.Round(summaryDto.getitotalVisitCount(),2));
			psmt.setDouble(22, GenericUtil.Round(summaryDto.getiaverageOrderSize(),2));
			psmt.setDouble(23, GenericUtil.Round(summaryDto.getitotalMovement(),2));
			psmt.setDouble(24, GenericUtil.Round(summaryDto.getitotalRevenue(),2));
			psmt.setDouble(25, GenericUtil.Round(summaryDto.getiregularRevenue(),2));
			psmt.setDouble(26, GenericUtil.Round(summaryDto.getisaleRevenue(),2));
			
			// identical margin
			psmt.setDouble(27, GenericUtil.Round(summaryDto.getitotalMargin(), 2));
			psmt.setDouble(28, GenericUtil.Round(summaryDto.getiregularMargin(), 2));
			psmt.setDouble(29, GenericUtil.Round(summaryDto.getisaleMargin(), 2));
			psmt.setDouble(30, GenericUtil.Round(summaryDto.getitotalMarginPer(), 2));
			psmt.setDouble(31, GenericUtil.Round(summaryDto.getiregularMarginPer(), 2));
			psmt.setDouble(32, GenericUtil.Round(summaryDto.getisaleMarginPer(), 2));
			
			psmt.setDouble(33, GenericUtil.Round(summaryDto.getiregularMovement(),2));
			psmt.setDouble(34, GenericUtil.Round(summaryDto.getisaleMovement(),2));

			// Code added for Movement By volume.....
			psmt.setDouble(35, GenericUtil.Round(summaryDto.getregMovementVolume(), 2));
			psmt.setDouble(36, GenericUtil.Round(summaryDto.getsaleMovementVolume(), 2));
			psmt.setDouble(37, GenericUtil.Round(summaryDto.getigRegVolumeRev(), 2));
			psmt.setDouble(38, GenericUtil.Round(summaryDto.getigSaleVolumeRev(), 2));
			psmt.setDouble(39, GenericUtil.Round(summaryDto.gettotMovementVolume(), 2));
			psmt.setDouble(40, GenericUtil.Round(summaryDto.getigtotVolumeRev(), 2));
			psmt.setDouble(41, GenericUtil.Round(summaryDto.getidRegMovementVolume(), 2));
			psmt.setDouble(42, GenericUtil.Round(summaryDto.getidSaleMovementVolume(), 2));
			psmt.setDouble(43, GenericUtil.Round(summaryDto.getidIgRegVolumeRev(), 2));
			psmt.setDouble(44, GenericUtil.Round(summaryDto.getidIgSaleVolumeRev(), 2));
			psmt.setDouble(45, GenericUtil.Round(summaryDto.getidTotMovementVolume(), 2));
			psmt.setDouble(46, GenericUtil.Round(summaryDto.getIdIgtotVolumeRev(), 2));
			
			// For private label - all stores
			psmt.setDouble(47, GenericUtil.Round(summaryDto.getPLTotalRevenue(),2));
			psmt.setDouble(48,GenericUtil.Round(summaryDto.getPLRegularRevenue(),2));
			psmt.setDouble(49,GenericUtil.Round(summaryDto.getPLSaleRevenue(),2));
			psmt.setDouble(50, GenericUtil.Round(summaryDto.getPLTotalMargin(), 2));
			psmt.setDouble(51, GenericUtil.Round(summaryDto.getPLRegularMargin(), 2));
			psmt.setDouble(52, GenericUtil.Round(summaryDto.getPLSaleMargin(), 2));
			psmt.setDouble(53, GenericUtil.Round(summaryDto.getPLTotalMarginPer(), 2));
			psmt.setDouble(54, GenericUtil.Round(summaryDto.getPLRegularMarginPer(), 2));
			psmt.setDouble(55, GenericUtil.Round(summaryDto.getPLSaleMarginPer(), 2));
			psmt.setDouble(56, GenericUtil.Round(summaryDto.getPLTotalMovement(),2));
			psmt.setDouble(57, GenericUtil.Round(summaryDto.getPLRegularMovement(), 2));
			psmt.setObject(58, GenericUtil.Round(summaryDto.getPLSaleMovement(),2));
			psmt.setDouble(59, GenericUtil.Round(summaryDto.getPLtotMovementVolume(), 2));
			psmt.setDouble(60, GenericUtil.Round(summaryDto.getPLregMovementVolume(), 2));
			psmt.setDouble(61, GenericUtil.Round(summaryDto.getPLsaleMovementVolume(), 2));
			psmt.setDouble(62, GenericUtil.Round(summaryDto.getPLTotalVisitCount(), 2));
			psmt.setDouble(63, GenericUtil.Round(summaryDto.getPLAverageOrderSize(), 2));

			// For private label - identical stores
			psmt.setDouble(64, GenericUtil.Round(summaryDto.getPLitotalRevenue(),2));
			psmt.setDouble(65, GenericUtil.Round(summaryDto.getPLiregularRevenue(),2));
			psmt.setDouble(66, GenericUtil.Round(summaryDto.getPLisaleRevenue(),2));
			psmt.setDouble(67, GenericUtil.Round(summaryDto.getPLitotalMargin(), 2));
			psmt.setDouble(68, GenericUtil.Round(summaryDto.getPLiregularMargin(), 2));
			psmt.setDouble(69, GenericUtil.Round(summaryDto.getPLisaleMargin(), 2));
			psmt.setDouble(70, GenericUtil.Round(summaryDto.getPLitotalMarginPer(), 2));
			psmt.setDouble(71, GenericUtil.Round(summaryDto.getPLiregularMarginPer(), 2));
			psmt.setDouble(72, GenericUtil.Round(summaryDto.getPLisaleMarginPer(), 2));
			psmt.setDouble(73, GenericUtil.Round(summaryDto.getPLitotalMovement(),2));
			psmt.setDouble(74, GenericUtil.Round(summaryDto.getPLiregularMovement(),2));
			psmt.setDouble(75, GenericUtil.Round(summaryDto.getPLisaleMovement(),2));
			psmt.setDouble(76, GenericUtil.Round(summaryDto.getPLidTotMovementVolume(), 2));
			psmt.setDouble(77, GenericUtil.Round(summaryDto.getPLidRegMovementVolume(), 2));
			psmt.setDouble(78, GenericUtil.Round(summaryDto.getPLidSaleMovementVolume(), 2));
			psmt.setDouble(79, GenericUtil.Round(summaryDto.getPLitotalVisitCount(),2));
			psmt.setDouble(80, GenericUtil.Round(summaryDto.getPLiaverageOrderSize(),2));

			// add the values into psmt
			psmt.addBatch();
		} catch (SQLException sql) {
			logger.error(" Error while adding batch values..... ", sql);
			throw new GeneralException(
					" Error while adding batch values..... ", sql);
		}
	}

	
	/*
	 * ****************************************************************
	 * add the values into PreparedStatement
	 * ****************************************************************
	 */
	public void updateSqlBatch(SummaryDataDTO summaryDto, PreparedStatement psmt)
			throws GeneralException {
		try {

			psmt.setDouble(1, GenericUtil.Round(summaryDto.getPLTotalRevenue(),2));
			psmt.setDouble(2,GenericUtil.Round(summaryDto.getPLRegularRevenue(),2));
			psmt.setDouble(3,GenericUtil.Round(summaryDto.getPLSaleRevenue(),2));
			psmt.setDouble(4, GenericUtil.Round(summaryDto.getPLTotalMargin(), 2));
			psmt.setDouble(5, GenericUtil.Round(summaryDto.getPLRegularMargin(), 2));
			psmt.setDouble(6, GenericUtil.Round(summaryDto.getPLSaleMargin(), 2));
			psmt.setDouble(7, GenericUtil.Round(summaryDto.getPLTotalMarginPer(), 2));
			psmt.setDouble(8, GenericUtil.Round(summaryDto.getPLRegularMarginPer(), 2));
			psmt.setDouble(9, GenericUtil.Round(summaryDto.getPLSaleMarginPer(), 2));
			psmt.setDouble(10, GenericUtil.Round(summaryDto.getPLTotalMovement(),2));
			psmt.setDouble(11, GenericUtil.Round(summaryDto.getPLRegularMovement(), 2));
			psmt.setObject(12, GenericUtil.Round(summaryDto.getPLSaleMovement(),2));
			psmt.setDouble(13, GenericUtil.Round(summaryDto.getPLtotMovementVolume(), 2));
			psmt.setDouble(14, GenericUtil.Round(summaryDto.getPLregMovementVolume(), 2));
			psmt.setDouble(15, GenericUtil.Round(summaryDto.getPLsaleMovementVolume(), 2));
			psmt.setDouble(16, GenericUtil.Round(summaryDto.getPLTotalVisitCount(), 2));
			psmt.setDouble(17, GenericUtil.Round(summaryDto.getPLAverageOrderSize(), 2));

			// For private label - identical stores
			psmt.setDouble(18, GenericUtil.Round(summaryDto.getPLitotalRevenue(),2));
			psmt.setDouble(19, GenericUtil.Round(summaryDto.getPLiregularRevenue(),2));
			psmt.setDouble(20, GenericUtil.Round(summaryDto.getPLisaleRevenue(),2));
			psmt.setDouble(21, GenericUtil.Round(summaryDto.getPLitotalMargin(), 2));
			psmt.setDouble(22, GenericUtil.Round(summaryDto.getPLiregularMargin(), 2));
			psmt.setDouble(23, GenericUtil.Round(summaryDto.getPLisaleMargin(), 2));
			psmt.setDouble(24, GenericUtil.Round(summaryDto.getPLitotalMarginPer(), 2));
			psmt.setDouble(25, GenericUtil.Round(summaryDto.getPLiregularMarginPer(), 2));
			psmt.setDouble(26, GenericUtil.Round(summaryDto.getPLisaleMarginPer(), 2));
			psmt.setDouble(27, GenericUtil.Round(summaryDto.getPLitotalMovement(),2));
			psmt.setDouble(28, GenericUtil.Round(summaryDto.getPLiregularMovement(),2));
			psmt.setDouble(29, GenericUtil.Round(summaryDto.getPLisaleMovement(),2));
			psmt.setDouble(30, GenericUtil.Round(summaryDto.getPLidTotMovementVolume(), 2));
			psmt.setDouble(31, GenericUtil.Round(summaryDto.getPLidRegMovementVolume(), 2));
			psmt.setDouble(32, GenericUtil.Round(summaryDto.getPLidSaleMovementVolume(), 2));
			psmt.setDouble(33, GenericUtil.Round(summaryDto.getPLitotalVisitCount(),2));
			psmt.setDouble(34, GenericUtil.Round(summaryDto.getPLiaverageOrderSize(),2));

			psmt.setObject(35, summaryDto.getcalendarId());
			psmt.setObject(36, summaryDto.getLocationLevelId());
			psmt.setObject(37, summaryDto.getLocationId());
			
			if (summaryDto.getProductLevelId() != 0)
				psmt.setInt(38, summaryDto.getProductLevelId());
			else
				psmt.setInt(38, -1);

			
			if (summaryDto.getProductLevelId() != 0)
				psmt.setString(39, summaryDto.getProductId());
			else
				psmt.setInt(39, -1);

			// add the values into psmt
			psmt.addBatch();
		} catch (SQLException sql) {
			logger.error(" Error while updat batch values..... ", sql);
			throw new GeneralException(
					" Error while update batch values..... ", sql);
		}
	}
	
	/*
	 * ****************************************************************
	 * Delete the previous aggregation for all level 
	 * Argument 1 : Connection
	 * Argument 2 : Calendar Id 
	 * Argument 3 : location level id
	 * @catch GeneralException
	 * ****************************************************************
	 */

	public void deleteRollUp(Connection _conn, int calendarId,
			String _locationId, int locationLevelId) {

		// logger.debug("Delete Previous Rollup Starts");

		StringBuffer sql = new StringBuffer();

		sql.append(" Delete from SALES_AGGR_DAILY_ROLLUP WHERE CALENDAR_ID='"
				+ calendarId + "' and LOCATION_ID='" + _locationId
				+ "' and LOCATION_LEVEL_ID='" + locationLevelId + "'");

		// logger.debug("Sales Aggr Daily Rollup Delete SQL: " +sql.toString());

		try {

			// execute the delete query
			PristineDBUtil.executeUpdate(_conn, sql, "deleteRollUp");
		} catch (GeneralException e) {
			logger.error(e.getMessage());
		}
	}

	/*
	 * ****************************************************************
	 * Method used to get the region level aggregation records Argument 1 :
	 * Connection Argument 2 : Calendar Id Argument 3 : region
	 * 
	 * @catch GeneralException
	 * ****************************************************************
	 */

	public List<SummaryDataDTO> getRegionAggrData(Connection _conn,
			int calendarId, String _regionId, int districtLevelId) {

		List<SummaryDataDTO> regionList = new ArrayList<SummaryDataDTO>();

		StringBuffer sql = new StringBuffer();

		sql.append(" select SA.PRODUCT_LEVEL_ID,SA.PRODUCT_ID");
		sql.append(" ,RD.REGION_ID as locationId");
		sql.append(" ,sum(SA.TOT_VISIT_CNT) as TOT_VISIT_CNT ");
		sql.append(" ,sum(SA.TOT_MOVEMENT) as TOT_MOVEMENT");
		sql.append(" ,sum(SA.REG_MOVEMENT) as REG_MOVEMENT");
		sql.append(" ,sum(SA.SALE_MOVEMENT) as SALE_MOVEMENT ");
		sql.append(" ,sum(SA.REG_REVENUE) as REG_REVENUE");
		sql.append(" ,sum(SA.SALE_REVENUE) as SALE_REVENUE");
		sql.append(" ,sum(SA.TOT_MARGIN) as TOT_MARGIN ");
		sql.append(" ,sum(SA.REG_MARGIN) as REG_MARGIN");
		sql.append(" ,sum(SA.SALE_MARGIN) as SALE_MARGIN");
		sql.append(" ,sum(SA.TOT_REVENUE) as TOT_REVENUE");
		
		// code added for identical
		sql.append(" ,sum(SA.ID_TOT_VISIT_CNT) as ITOT_VISIT_CNT ");
		sql.append(" ,sum(SA.ID_TOT_MOVEMENT) as ITOT_MOVEMENT");
		sql.append(" ,sum(SA.ID_REG_MOEVEMNT) as IREG_MOVEMENT");
		sql.append(" ,sum(SA.ID_SALE_MOVEMENT) as ISALE_MOVEMENT ");
		sql.append(" ,sum(SA.ID_REG_REVENUE) as IREG_REVENUE");
		sql.append(" ,sum(SA.ID_SALE_REVENUE) as ISALE_REVENUE");
		sql.append(" ,sum(SA.ID_TOT_MARGIN) as ITOT_MARGIN ");
		sql.append(" ,sum(SA.ID_REG_MARGIN) as IREG_MARGIN");
		sql.append(" ,sum(SA.ID_SALE_MARGIN) as ISALE_MARGIN");
		sql.append(" ,sum(SA.ID_TOT_REVENUE) as ITOT_REVENUE");
		
		// code added for Movement By Volume....
		sql.append(" ,sum(REG_MOVEMENT_VOL) as REG_MOVEMENT_VOL");
		sql.append(" ,sum(SALE_MOVEMENT_VOL) as SALE_MOVEMENT_VOL");
		sql.append(" ,sum(REG_IGVOL_REVENUE) as REG_IGVOL_REVENUE");
		sql.append(" ,sum(SALE_IGVOL_REVENUE) as SALE_IGVOL_REVENUE");
		sql.append(" ,sum(TOT_MOVEMENT_VOL) as TOT_MOVEMENT_VOL");
		sql.append(" ,sum(TOT_IGVOL_REVENUE) as TOT_IGVOL_REVENUE");
				
		// identical movement by volume
		sql.append(" ,sum(ID_REG_MOVEMENT_VOL) as ID_REG_MOVEMENT_VOL");
		sql.append(" ,sum(ID_SALE_MOVEMENT_VOL) as ID_SALE_MOVEMENT_VOL");
		sql.append(" ,sum(ID_REG_IGVOL_REVENUE) as ID_REG_IGVOL_REVENUE");
		sql.append(" ,sum(ID_SALE_IGVOL_REVENUE) as ID_SALE_IGVOL_REVENUE");
		sql.append(" ,sum(ID_TOT_MOVEMENT_VOL) as ID_TOT_MOVEMENT_VOL");
		sql.append(" ,sum(ID_TOT_IGVOL_REVENUE) as ID_TOT_IGVOL_REVENUE");

		//Private label level aggregation
		sql.append(" ,sum(SA.PL_TOT_REVENUE) as PL_TOT_REVENUE");
		sql.append(" ,sum(SA.PL_REG_REVENUE) as PL_REG_REVENUE");
		sql.append(" ,sum(SA.PL_SALE_REVENUE) as PL_SALE_REVENUE");
		sql.append(" ,sum(SA.PL_TOT_MARGIN) as PL_TOT_MARGIN ");
		sql.append(" ,sum(SA.PL_REG_MARGIN) as PL_REG_MARGIN");
		sql.append(" ,sum(SA.PL_SALE_MARGIN) as PL_SALE_MARGIN");
		sql.append(" ,sum(SA.PL_TOT_MOVEMENT) as PL_TOT_MOVEMENT");
		sql.append(" ,sum(SA.PL_REG_MOVEMENT) as PL_REG_MOVEMENT");
		sql.append(" ,sum(SA.PL_SALE_MOVEMENT) as PL_SALE_MOVEMENT ");
		sql.append(" ,sum(SA.PL_TOT_MOVEMENT_VOL) as PL_TOT_MOVEMENT_VOL");
		sql.append(" ,sum(SA.PL_REG_MOVEMENT_VOL) as PL_REG_MOVEMENT_VOL");
		sql.append(" ,sum(SA.PL_SALE_MOVEMENT_VOL) as PL_SALE_MOVEMENT_VOL");
		sql.append(" ,sum(SA.PL_TOT_VISIT_CNT) as PL_TOT_VISIT_CNT ");
		
		sql.append(" ,sum(SA.PL_ID_TOT_REVENUE) as PL_ITOT_REVENUE");
		sql.append(" ,sum(SA.PL_ID_REG_REVENUE) as PL_IREG_REVENUE");
		sql.append(" ,sum(SA.PL_ID_SALE_REVENUE) as PL_ISALE_REVENUE");
		sql.append(" ,sum(SA.PL_ID_TOT_MARGIN) as PL_ITOT_MARGIN ");
		sql.append(" ,sum(SA.PL_ID_REG_MARGIN) as PL_IREG_MARGIN");
		sql.append(" ,sum(SA.PL_ID_SALE_MARGIN) as PL_ISALE_MARGIN");
		sql.append(" ,sum(SA.PL_ID_TOT_MOVEMENT) as PL_ITOT_MOVEMENT");
		sql.append(" ,sum(SA.PL_ID_REG_MOVEMENT) as PL_IREG_MOVEMENT");
		sql.append(" ,sum(SA.PL_ID_SALE_MOVEMENT) as PL_ISALE_MOVEMENT ");
		sql.append(" ,sum(SA.PL_ID_TOT_MOVEMENT_VOL) as PL_ID_TOT_MOVEMENT_VOL");
		sql.append(" ,sum(SA.PL_ID_REG_MOVEMENT_VOL) as PL_ID_REG_MOVEMENT_VOL");
		sql.append(" ,sum(SA.PL_ID_SALE_MOVEMENT_VOL) as PL_ID_SALE_MOVEMENT_VOL");
		sql.append(" ,sum(SA.PL_ID_TOT_VISIT_CNT) as PL_ITOT_VISIT_CNT ");
		
		sql.append(" from");
		sql.append(" SALES_AGGR_DAILY_ROLLUP SA");
		sql.append(" Inner Join RETAIL_DISTRICT RD ");
		sql.append(" On SA.LOCATION_ID=RD.ID");
		sql.append(" where");
		sql.append(" RD.REGION_ID='" + _regionId + "'");
		sql.append(" and SA.CALENDAR_ID =" + calendarId + "");
		sql.append(" and  SA.LOCATION_LEVEL_ID=" + districtLevelId + " ");
		sql.append(" group by RD.REGION_ID,SA.PRODUCT_LEVEL_ID,SA.PRODUCT_ID");
		sql.append(" order by RD.REGION_ID,SA.PRODUCT_LEVEL_ID,SA.PRODUCT_ID");

		logger.debug(" Region Sql" + sql.toString() );
		try {
			// Execute Query
			CachedRowSet resultSet = PristineDBUtil.executeQuery(_conn, sql,
					"fetchRollUpDaily");

			while (resultSet.next()) {
				SummaryDataDTO summaryDto = new SummaryDataDTO();
				if (resultSet.getString("PRODUCT_LEVEL_ID") == null)
					summaryDto.setProductLevelId(0);
				else
					summaryDto.setProductLevelId(resultSet
							.getInt("PRODUCT_LEVEL_ID"));
				summaryDto.setProductId(resultSet.getString("PRODUCT_ID"));
				
				summaryDto.setTotalVisitCount(resultSet.getDouble("TOT_VISIT_CNT"));
				summaryDto.setTotalMovement(resultSet.getDouble("TOT_MOVEMENT"));
				summaryDto.setRegularMovement(resultSet	.getDouble("REG_MOVEMENT"));
				summaryDto.setSaleMovement(resultSet.getDouble("SALE_MOVEMENT"));
				summaryDto.setRegularRevenue(resultSet.getDouble("REG_REVENUE"));
				summaryDto.setSaleRevenue(resultSet.getDouble("SALE_REVENUE"));
				summaryDto.setTotalMargin(resultSet.getDouble("TOT_MARGIN"));
				summaryDto.setRegularMargin(resultSet.getDouble("REG_MARGIN"));
				summaryDto.setSaleMargin(resultSet.getDouble("SALE_MARGIN"));
				
				// added for identical
				summaryDto.setitotalVisitCount(resultSet.getDouble("ITOT_VISIT_CNT"));
				summaryDto.setitotalMovement(resultSet.getDouble("ITOT_MOVEMENT")); 
				summaryDto.setiregularMovement(resultSet.getDouble("IREG_MOVEMENT"));
				summaryDto.setisaleMovement(resultSet.getDouble("ISALE_MOVEMENT"));
				summaryDto.setiregularRevenue(resultSet.getDouble("IREG_REVENUE"));
				summaryDto.setisaleRevenue(resultSet.getDouble("ISALE_REVENUE"));
				summaryDto.setitotalMargin(resultSet.getDouble("ITOT_MARGIN"));
				summaryDto.setiregularMargin(resultSet.getDouble("IREG_MARGIN"));
				summaryDto.setisaleMargin(resultSet.getDouble("ISALE_MARGIN"));
				summaryDto.setitotalRevenue(resultSet.getDouble("ITOT_REVENUE"));
				summaryDto.setLocationId(resultSet.getInt("locationId"));
				
				summaryDto.setTotalRevenue( resultSet.getDouble("TOT_REVENUE"));
				
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
				
				// Private label level aggregations
				summaryDto.setPLTotalRevenue( resultSet.getDouble("PL_TOT_REVENUE"));
				summaryDto.setPLRegularRevenue(resultSet.getDouble("PL_REG_REVENUE"));
				summaryDto.setPLSaleRevenue(resultSet.getDouble("PL_SALE_REVENUE"));
				summaryDto.setPLTotalMargin(resultSet.getDouble("PL_TOT_MARGIN"));
				summaryDto.setPLRegularMargin(resultSet.getDouble("PL_REG_MARGIN"));
				summaryDto.setPLSaleMargin(resultSet.getDouble("PL_SALE_MARGIN"));
				summaryDto.setPLTotalMovement(resultSet.getDouble("PL_TOT_MOVEMENT"));
				summaryDto.setPLRegularMovement(resultSet	.getDouble("PL_REG_MOVEMENT"));
				summaryDto.setPLSaleMovement(resultSet.getDouble("PL_SALE_MOVEMENT"));
				summaryDto.setPLtotMovementVolume(resultSet.getDouble("PL_TOT_MOVEMENT_VOL"));
				summaryDto.setPLregMovementVolume(resultSet.getDouble("PL_REG_MOVEMENT_VOL"));
				summaryDto.setPLsaleMovementVolume(resultSet.getDouble("PL_SALE_MOVEMENT_VOL"));
				summaryDto.setPLTotalVisitCount(resultSet.getDouble("PL_TOT_VISIT_CNT"));

				summaryDto.setPLitotalRevenue(resultSet.getDouble("PL_ITOT_REVENUE"));
				summaryDto.setPLiregularRevenue(resultSet.getDouble("PL_IREG_REVENUE"));
				summaryDto.setPLisaleRevenue(resultSet.getDouble("PL_ISALE_REVENUE"));
				summaryDto.setPLitotalMargin(resultSet.getDouble("PL_ITOT_MARGIN"));
				summaryDto.setPLiregularMargin(resultSet.getDouble("PL_IREG_MARGIN"));
				summaryDto.setPLisaleMargin(resultSet.getDouble("PL_ISALE_MARGIN"));
				summaryDto.setPLitotalMovement(resultSet.getDouble("PL_ITOT_MOVEMENT")); 
				summaryDto.setPLiregularMovement(resultSet.getDouble("PL_IREG_MOVEMENT"));
				summaryDto.setPLisaleMovement(resultSet.getDouble("PL_ISALE_MOVEMENT"));
				summaryDto.setPLidTotMovementVolume(resultSet.getDouble("PL_ID_TOT_MOVEMENT_VOL"));
				summaryDto.setPLidRegMovementVolume(resultSet.getDouble("PL_ID_REG_MOVEMENT_VOL"));
				summaryDto.setPLidSaleMovementVolume(resultSet.getDouble("PL_ID_SALE_MOVEMENT_VOL"));
				summaryDto.setPLitotalVisitCount(resultSet.getDouble("PL_ITOT_VISIT_CNT"));
				
				regionList.add(summaryDto);
			}
			// logger.debug(" Region Map "+ _regionId + "Map Size" +
			// regionList.size());

		} catch (GeneralException exe) {
			logger.error(exe);
		} catch (SQLException sq) {
			logger.error(sq);
		}

		return regionList;

	}

	/*
	 * ****************************************************************
	 * Method used to get the Division level aggregation records Argument 1 :
	 * Connection Argument 2 : Calendar Id Argument 3 : Division
	 * 
	 * @catch GeneralException
	 * ****************************************************************
	 */

	public List<SummaryDataDTO> getDivisionAggrData(Connection _conn,
			int calendarId, String _divisionId, int regionLevelId) {

		List<SummaryDataDTO> divisionList = new ArrayList<SummaryDataDTO>();

		StringBuffer sql = new StringBuffer();

		sql.append(" select SA.PRODUCT_LEVEL_ID,SA.PRODUCT_ID");
		sql.append(" ,RR.DIVISION_ID as locationId");
		sql.append(" ,sum(SA.TOT_VISIT_CNT) as TOT_VISIT_CNT ");
		sql.append(" ,sum(SA.TOT_MOVEMENT) as TOT_MOVEMENT");
		sql.append(" ,sum(SA.REG_MOVEMENT) as REG_MOVEMENT");
		sql.append(" ,sum(SA.SALE_MOVEMENT) as SALE_MOVEMENT ");
		sql.append(" ,sum(SA.REG_REVENUE) as REG_REVENUE");
		sql.append(" ,sum(SA.SALE_REVENUE) as SALE_REVENUE");
		sql.append(" ,sum(SA.TOT_MARGIN) as TOT_MARGIN ");
		sql.append(" ,sum(SA.REG_MARGIN) as REG_MARGIN");
		sql.append(" ,sum(SA.SALE_MARGIN) as SALE_MARGIN");
		sql.append(" ,sum(SA.ID_TOT_VISIT_CNT) as ITOT_VISIT_CNT ");
		sql.append(" ,sum(SA.ID_TOT_MOVEMENT) as ITOT_MOVEMENT");
		sql.append(" ,sum(SA.ID_REG_MOEVEMNT) as IREG_MOVEMENT");
		sql.append(" ,sum(SA.ID_SALE_MOVEMENT) as ISALE_MOVEMENT ");
		sql.append(" ,sum(SA.ID_REG_REVENUE) as IREG_REVENUE");
		sql.append(" ,sum(SA.ID_SALE_REVENUE) as ISALE_REVENUE");
		sql.append(" ,sum(SA.ID_TOT_MARGIN) as ITOT_MARGIN ");
		sql.append(" ,sum(SA.ID_REG_MARGIN) as IREG_MARGIN");
		sql.append(" ,sum(SA.ID_SALE_MARGIN) as ISALE_MARGIN");
		sql.append(" ,sum(SA.ID_TOT_REVENUE) as ITOT_REVENUE");
		
		sql.append(" ,sum(TOT_REVENUE) AS TOT_REVENUE");
		
		// code added for Movement By Volume....
		sql.append(" ,sum(REG_MOVEMENT_VOL) as REG_MOVEMENT_VOL");
		sql.append(" ,sum(SALE_MOVEMENT_VOL) as SALE_MOVEMENT_VOL");
		sql.append(" ,sum(REG_IGVOL_REVENUE) as REG_IGVOL_REVENUE");
		sql.append(" ,sum(SALE_IGVOL_REVENUE) as SALE_IGVOL_REVENUE");
		sql.append(" ,sum(TOT_MOVEMENT_VOL) as TOT_MOVEMENT_VOL");
		sql.append(" ,sum(TOT_IGVOL_REVENUE) as TOT_IGVOL_REVENUE");
				
		sql.append(" ,sum(ID_REG_MOVEMENT_VOL) as ID_REG_MOVEMENT_VOL");
		sql.append(" ,sum(ID_SALE_MOVEMENT_VOL) as ID_SALE_MOVEMENT_VOL");
		sql.append(" ,sum(ID_REG_IGVOL_REVENUE) as ID_REG_IGVOL_REVENUE");
		sql.append(" ,sum(ID_SALE_IGVOL_REVENUE) as ID_SALE_IGVOL_REVENUE");
		sql.append(" ,sum(ID_TOT_MOVEMENT_VOL) as ID_TOT_MOVEMENT_VOL");
		sql.append(" ,sum(ID_TOT_IGVOL_REVENUE) as ID_TOT_IGVOL_REVENUE");

		//Aggregation at Private Label level
		sql.append(" ,sum(SA.PL_TOT_REVENUE) as PL_TOT_REVENUE");
		sql.append(" ,sum(SA.PL_REG_REVENUE) as PL_REG_REVENUE");
		sql.append(" ,sum(SA.PL_SALE_REVENUE) as PL_SALE_REVENUE");
		sql.append(" ,sum(SA.PL_TOT_MARGIN) as PL_TOT_MARGIN ");
		sql.append(" ,sum(SA.PL_REG_MARGIN) as PL_REG_MARGIN");
		sql.append(" ,sum(SA.PL_SALE_MARGIN) as PL_SALE_MARGIN");
		sql.append(" ,sum(SA.PL_TOT_MOVEMENT) as PL_TOT_MOVEMENT");
		sql.append(" ,sum(SA.PL_REG_MOVEMENT) as PL_REG_MOVEMENT");
		sql.append(" ,sum(SA.PL_SALE_MOVEMENT) as PL_SALE_MOVEMENT ");
		sql.append(" ,sum(SA.PL_TOT_MOVEMENT_VOL) as PL_TOT_MOVEMENT_VOL");
		sql.append(" ,sum(SA.PL_REG_MOVEMENT_VOL) as PL_REG_MOVEMENT_VOL");
		sql.append(" ,sum(SA.PL_SALE_MOVEMENT_VOL) as PL_SALE_MOVEMENT_VOL");
		sql.append(" ,sum(SA.PL_TOT_VISIT_CNT) as PL_TOT_VISIT_CNT ");
		
		sql.append(" ,sum(SA.PL_ID_TOT_REVENUE) as PL_ITOT_REVENUE");
		sql.append(" ,sum(SA.PL_ID_REG_REVENUE) as PL_IREG_REVENUE");
		sql.append(" ,sum(SA.PL_ID_SALE_REVENUE) as PL_ISALE_REVENUE");
		sql.append(" ,sum(SA.PL_ID_TOT_MARGIN) as PL_ITOT_MARGIN ");
		sql.append(" ,sum(SA.PL_ID_REG_MARGIN) as PL_IREG_MARGIN");
		sql.append(" ,sum(SA.PL_ID_SALE_MARGIN) as PL_ISALE_MARGIN");
		sql.append(" ,sum(SA.PL_ID_TOT_MOVEMENT) as PL_ITOT_MOVEMENT");
		sql.append(" ,sum(SA.PL_ID_REG_MOVEMENT) as PL_IREG_MOVEMENT");
		sql.append(" ,sum(SA.PL_ID_SALE_MOVEMENT) as PL_ISALE_MOVEMENT ");
		sql.append(" ,sum(SA.PL_ID_TOT_MOVEMENT_VOL) as PL_ID_TOT_MOVEMENT_VOL");
		sql.append(" ,sum(SA.PL_ID_REG_MOVEMENT_VOL) as PL_ID_REG_MOVEMENT_VOL");
		sql.append(" ,sum(SA.PL_ID_SALE_MOVEMENT_VOL) as PL_ID_SALE_MOVEMENT_VOL");
		sql.append(" ,sum(SA.ID_TOT_VISIT_CNT) as PL_ITOT_VISIT_CNT ");
			
		sql.append(" from");
		sql.append(" SALES_AGGR_DAILY_ROLLUP SA");
		sql.append(" Inner Join RETAIL_REGION RR ");
		sql.append(" On SA.LOCATION_ID=RR.ID");
		sql.append(" where");
		sql.append(" RR.DIVISION_ID='" + _divisionId + "'");
		sql.append(" and SA.CALENDAR_ID =" + calendarId + "");
		sql.append(" and SA.LOCATION_LEVEL_ID=" + regionLevelId + " ");
		sql.append(" group by RR.DIVISION_ID,SA.PRODUCT_LEVEL_ID,SA.PRODUCT_ID");
		sql.append(" order by RR.DIVISION_ID,SA.PRODUCT_LEVEL_ID,SA.PRODUCT_ID");

		logger.debug(" Division Sql" + sql.toString());
		try {
			// Execute Query
			CachedRowSet resultSet = PristineDBUtil.executeQuery(_conn, sql,
					"fetchRollUpDaily");

			while (resultSet.next()) {
				SummaryDataDTO summaryDto = new SummaryDataDTO();
				if (resultSet.getString("PRODUCT_LEVEL_ID") == null)
					summaryDto.setProductLevelId(0);
				else
					summaryDto.setProductLevelId(resultSet
							.getInt("PRODUCT_LEVEL_ID"));
				summaryDto.setProductId(resultSet.getString("PRODUCT_ID"));
				summaryDto.setTotalVisitCount(resultSet
						.getDouble("TOT_VISIT_CNT"));
				summaryDto
						.setTotalMovement(resultSet.getDouble("TOT_MOVEMENT"));
				summaryDto.setRegularMovement(resultSet
						.getDouble("REG_MOVEMENT"));
				summaryDto
						.setSaleMovement(resultSet.getDouble("SALE_MOVEMENT"));
				summaryDto
						.setRegularRevenue(resultSet.getDouble("REG_REVENUE"));
				summaryDto.setSaleRevenue(resultSet.getDouble("SALE_REVENUE"));
				summaryDto.setTotalMargin(resultSet.getDouble("TOT_MARGIN"));
				summaryDto.setRegularMargin(resultSet.getDouble("REG_MARGIN"));
				summaryDto.setSaleMargin(resultSet.getDouble("SALE_MARGIN"));
				// added for identical
				summaryDto.setitotalVisitCount(resultSet
						.getDouble("ITOT_VISIT_CNT"));
				summaryDto.setitotalMovement(resultSet
						.getDouble("ITOT_MOVEMENT"));
				summaryDto.setiregularMovement(resultSet
						.getDouble("IREG_MOVEMENT"));
				summaryDto.setisaleMovement(resultSet
						.getDouble("ISALE_MOVEMENT"));
				summaryDto.setiregularRevenue(resultSet
						.getDouble("IREG_REVENUE"));
				summaryDto
						.setisaleRevenue(resultSet.getDouble("ISALE_REVENUE"));
				summaryDto.setitotalMargin(resultSet.getDouble("ITOT_MARGIN"));
				summaryDto
						.setiregularMargin(resultSet.getDouble("IREG_MARGIN"));
				summaryDto.setisaleMargin(resultSet.getDouble("ISALE_MARGIN"));
				summaryDto
						.setitotalRevenue(resultSet.getDouble("ITOT_REVENUE"));
				summaryDto.setLocationId(resultSet.getInt("locationId"));
				
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
				
				// Private label level aggregations
				summaryDto.setPLTotalRevenue( resultSet.getDouble("PL_TOT_REVENUE"));
				summaryDto.setPLRegularRevenue(resultSet.getDouble("PL_REG_REVENUE"));
				summaryDto.setPLSaleRevenue(resultSet.getDouble("PL_SALE_REVENUE"));
				summaryDto.setPLTotalMargin(resultSet.getDouble("PL_TOT_MARGIN"));
				summaryDto.setPLRegularMargin(resultSet.getDouble("PL_REG_MARGIN"));
				summaryDto.setPLSaleMargin(resultSet.getDouble("PL_SALE_MARGIN"));
				summaryDto.setPLTotalMovement(resultSet.getDouble("PL_TOT_MOVEMENT"));
				summaryDto.setPLRegularMovement(resultSet	.getDouble("PL_REG_MOVEMENT"));
				summaryDto.setPLSaleMovement(resultSet.getDouble("PL_SALE_MOVEMENT"));
				summaryDto.setPLtotMovementVolume(resultSet.getDouble("PL_TOT_MOVEMENT_VOL"));
				summaryDto.setPLregMovementVolume(resultSet.getDouble("PL_REG_MOVEMENT_VOL"));
				summaryDto.setPLsaleMovementVolume(resultSet.getDouble("PL_SALE_MOVEMENT_VOL"));
				summaryDto.setPLTotalVisitCount(resultSet.getDouble("PL_TOT_VISIT_CNT"));

				summaryDto.setPLitotalRevenue(resultSet.getDouble("PL_ITOT_REVENUE"));
				summaryDto.setPLiregularRevenue(resultSet.getDouble("PL_IREG_REVENUE"));
				summaryDto.setPLisaleRevenue(resultSet.getDouble("PL_ISALE_REVENUE"));
				summaryDto.setPLitotalMargin(resultSet.getDouble("PL_ITOT_MARGIN"));
				summaryDto.setPLiregularMargin(resultSet.getDouble("PL_IREG_MARGIN"));
				summaryDto.setPLisaleMargin(resultSet.getDouble("PL_ISALE_MARGIN"));
				summaryDto.setPLitotalMovement(resultSet.getDouble("PL_ITOT_MOVEMENT")); 
				summaryDto.setPLiregularMovement(resultSet.getDouble("PL_IREG_MOVEMENT"));
				summaryDto.setPLisaleMovement(resultSet.getDouble("PL_ISALE_MOVEMENT"));
				summaryDto.setPLidTotMovementVolume(resultSet.getDouble("PL_ID_TOT_MOVEMENT_VOL"));
				summaryDto.setPLidRegMovementVolume(resultSet.getDouble("PL_ID_REG_MOVEMENT_VOL"));
				summaryDto.setPLidSaleMovementVolume(resultSet.getDouble("PL_ID_SALE_MOVEMENT_VOL"));
				summaryDto.setPLitotalVisitCount(resultSet.getDouble("PL_ITOT_VISIT_CNT"));
				
				divisionList.add(summaryDto);
			}
			// logger.debug(" Division Map "+ _divisionId + "Map Size" +
			// divisionList.size());

		} catch (GeneralException exe) {
			logger.error(exe);
		} catch (SQLException sq) {
			logger.error(sq);
		}

		return divisionList;
	}

	/*
	 * ****************************************************************
	 * Method used to get the Chain level aggregation records Argument 1 :
	 * Connection Argument 2 : Calendar Id Argument 3 : Chain
	 * 
	 * @catch GeneralException
	 * ****************************************************************
	 */

	public List<SummaryDataDTO> getChainAggrData(Connection _conn,
			int calendarId, String _chainId, int divisionLevelId) {

		List<SummaryDataDTO> chainList = new ArrayList<SummaryDataDTO>();

		StringBuffer sql = new StringBuffer();

		sql.append(" select SA.PRODUCT_LEVEL_ID,SA.PRODUCT_ID");
		sql.append(" ,sum(SA.TOT_VISIT_CNT) as TOT_VISIT_CNT ");
		sql.append(" ,sum(SA.TOT_MOVEMENT) as TOT_MOVEMENT");
		sql.append(" ,sum(SA.REG_MOVEMENT) as REG_MOVEMENT");
		sql.append(" ,sum(SA.SALE_MOVEMENT) as SALE_MOVEMENT ");
		sql.append(" ,sum(SA.REG_REVENUE) as REG_REVENUE");
		sql.append(" ,sum(SA.SALE_REVENUE) as SALE_REVENUE");
		sql.append(" ,sum(SA.TOT_MARGIN) as TOT_MARGIN ");
		sql.append(" ,sum(SA.REG_MARGIN) as REG_MARGIN");
		sql.append(" ,sum(SA.SALE_MARGIN) as SALE_MARGIN");
		sql.append(" ,sum(SA.ID_TOT_VISIT_CNT) as ITOT_VISIT_CNT ");
		sql.append(" ,sum(SA.ID_TOT_MOVEMENT) as ITOT_MOVEMENT");
		sql.append(" ,sum(SA.ID_REG_MOEVEMNT) as IREG_MOVEMENT");
		sql.append(" ,sum(SA.ID_SALE_MOVEMENT) as ISALE_MOVEMENT ");
		sql.append(" ,sum(SA.ID_REG_REVENUE) as IREG_REVENUE");
		sql.append(" ,sum(SA.ID_SALE_REVENUE) as ISALE_REVENUE");
		sql.append(" ,sum(SA.ID_TOT_MARGIN) as ITOT_MARGIN ");
		sql.append(" ,sum(SA.ID_REG_MARGIN) as IREG_MARGIN");
		sql.append(" ,sum(SA.ID_SALE_MARGIN) as ISALE_MARGIN");
		sql.append(" ,sum(SA.ID_TOT_REVENUE) as ITOT_REVENUE");
		
		sql.append(" ,sum(TOT_REVENUE) AS TOT_REVENUE");
		
		// code added for Movement By Volume....
		sql.append(" ,sum(REG_MOVEMENT_VOL) as REG_MOVEMENT_VOL");
		sql.append(" ,sum(SALE_MOVEMENT_VOL) as SALE_MOVEMENT_VOL");
		sql.append(" ,sum(REG_IGVOL_REVENUE) as REG_IGVOL_REVENUE");
		sql.append(" ,sum(SALE_IGVOL_REVENUE) as SALE_IGVOL_REVENUE");
		sql.append(" ,sum(TOT_MOVEMENT_VOL) as TOT_MOVEMENT_VOL");
		sql.append(" ,sum(TOT_IGVOL_REVENUE) as TOT_IGVOL_REVENUE");
		sql.append(" ,sum(ID_REG_MOVEMENT_VOL) as ID_REG_MOVEMENT_VOL");
		sql.append(" ,sum(ID_SALE_MOVEMENT_VOL) as ID_SALE_MOVEMENT_VOL");
		sql.append(" ,sum(ID_REG_IGVOL_REVENUE) as ID_REG_IGVOL_REVENUE");
		sql.append(" ,sum(ID_SALE_IGVOL_REVENUE) as ID_SALE_IGVOL_REVENUE");
		sql.append(" ,sum(ID_TOT_MOVEMENT_VOL) as ID_TOT_MOVEMENT_VOL");
		sql.append(" ,sum(ID_TOT_IGVOL_REVENUE) as ID_TOT_IGVOL_REVENUE");
		
		// Aggregation at Private label level
		sql.append(" ,sum(SA.PL_TOT_REVENUE) as PL_TOT_REVENUE");
		sql.append(" ,sum(SA.PL_REG_REVENUE) as PL_REG_REVENUE");
		sql.append(" ,sum(SA.PL_SALE_REVENUE) as PL_SALE_REVENUE");
		sql.append(" ,sum(SA.PL_TOT_MARGIN) as PL_TOT_MARGIN ");
		sql.append(" ,sum(SA.PL_REG_MARGIN) as PL_REG_MARGIN");
		sql.append(" ,sum(SA.PL_SALE_MARGIN) as PL_SALE_MARGIN");
		sql.append(" ,sum(SA.PL_TOT_MOVEMENT) as PL_TOT_MOVEMENT");
		sql.append(" ,sum(SA.PL_REG_MOVEMENT) as PL_REG_MOVEMENT");
		sql.append(" ,sum(SA.PL_SALE_MOVEMENT) as PL_SALE_MOVEMENT ");
		sql.append(" ,sum(SA.PL_TOT_MOVEMENT_VOL) as PL_TOT_MOVEMENT_VOL");
		sql.append(" ,sum(SA.PL_REG_MOVEMENT_VOL) as PL_REG_MOVEMENT_VOL");
		sql.append(" ,sum(SA.PL_SALE_MOVEMENT_VOL) as PL_SALE_MOVEMENT_VOL");
		sql.append(" ,sum(SA.PL_TOT_VISIT_CNT) as PL_TOT_VISIT_CNT ");
		
		sql.append(" ,sum(SA.PL_ID_TOT_REVENUE) as PL_ITOT_REVENUE");
		sql.append(" ,sum(SA.PL_ID_REG_REVENUE) as PL_IREG_REVENUE");
		sql.append(" ,sum(SA.PL_ID_SALE_REVENUE) as PL_ISALE_REVENUE");
		sql.append(" ,sum(SA.PL_ID_TOT_MARGIN) as PL_ITOT_MARGIN ");
		sql.append(" ,sum(SA.PL_ID_REG_MARGIN) as PL_IREG_MARGIN");
		sql.append(" ,sum(SA.PL_ID_SALE_MARGIN) as PL_ISALE_MARGIN");
		sql.append(" ,sum(SA.PL_ID_TOT_MOVEMENT) as PL_ITOT_MOVEMENT");
		sql.append(" ,sum(SA.PL_ID_REG_MOVEMENT) as PL_IREG_MOVEMENT");
		sql.append(" ,sum(SA.PL_ID_SALE_MOVEMENT) as PL_ISALE_MOVEMENT ");
		sql.append(" ,sum(SA.PL_ID_TOT_MOVEMENT_VOL) as PL_ID_TOT_MOVEMENT_VOL");
		sql.append(" ,sum(SA.PL_ID_REG_MOVEMENT_VOL) as PL_ID_REG_MOVEMENT_VOL");
		sql.append(" ,sum(SA.PL_ID_SALE_MOVEMENT_VOL) as PL_ID_SALE_MOVEMENT_VOL");
		sql.append(" ,sum(SA.PL_ID_TOT_VISIT_CNT) as PL_ITOT_VISIT_CNT ");
		
		sql.append(" from");
		sql.append(" SALES_AGGR_DAILY_ROLLUP SA");
		sql.append(" join RETAIL_DIVISION RD ON RD.ID = SA.LOCATION_ID");
		sql.append(" AND RD.CHAIN_ID=").append(_chainId);

		sql.append(" where");
		sql.append("  SA.CALENDAR_ID =" + calendarId + " ");
		sql.append(" and SA.LOCATION_LEVEL_ID= " + divisionLevelId + " ");
		sql.append(" group by SA.PRODUCT_LEVEL_ID,SA.PRODUCT_ID");
		sql.append(" order by SA.PRODUCT_LEVEL_ID,SA.PRODUCT_ID");

		logger.debug(" Chain Sql" + sql.toString());
		try {
			// Execute Query
			CachedRowSet resultSet = PristineDBUtil.executeQuery(_conn, sql,
					"fetchRollUpDaily");

			while (resultSet.next()) {
				SummaryDataDTO summaryDto = new SummaryDataDTO();
				if (resultSet.getString("PRODUCT_LEVEL_ID") == null)
					summaryDto.setProductLevelId(0);
				else
					summaryDto.setProductLevelId(resultSet
							.getInt("PRODUCT_LEVEL_ID"));
				summaryDto.setProductId(resultSet.getString("PRODUCT_ID"));
				summaryDto.setTotalVisitCount(resultSet
						.getDouble("TOT_VISIT_CNT"));
				summaryDto
						.setTotalMovement(resultSet.getDouble("TOT_MOVEMENT"));
				summaryDto.setRegularMovement(resultSet
						.getDouble("REG_MOVEMENT"));
				summaryDto
						.setSaleMovement(resultSet.getDouble("SALE_MOVEMENT"));
				summaryDto
						.setRegularRevenue(resultSet.getDouble("REG_REVENUE"));
				summaryDto.setSaleRevenue(resultSet.getDouble("SALE_REVENUE"));
				summaryDto.setTotalMargin(resultSet.getDouble("TOT_MARGIN"));
				summaryDto.setRegularMargin(resultSet.getDouble("REG_MARGIN"));
				summaryDto.setSaleMargin(resultSet.getDouble("SALE_MARGIN"));

				// added for identical
				summaryDto.setitotalVisitCount(resultSet
						.getDouble("ITOT_VISIT_CNT"));
				summaryDto.setitotalMovement(resultSet
						.getDouble("ITOT_MOVEMENT"));
				summaryDto.setiregularMovement(resultSet
						.getDouble("IREG_MOVEMENT"));
				summaryDto.setisaleMovement(resultSet
						.getDouble("ISALE_MOVEMENT"));
				summaryDto.setiregularRevenue(resultSet
						.getDouble("IREG_REVENUE"));
				summaryDto
						.setisaleRevenue(resultSet.getDouble("ISALE_REVENUE"));
				summaryDto.setitotalMargin(resultSet.getDouble("ITOT_MARGIN"));
				summaryDto
						.setiregularMargin(resultSet.getDouble("IREG_MARGIN"));
				summaryDto.setisaleMargin(resultSet.getDouble("ISALE_MARGIN"));
				summaryDto
						.setitotalRevenue(resultSet.getDouble("ITOT_REVENUE"));
				summaryDto.setLocationId(Integer.valueOf(_chainId));
				
				summaryDto.setTotalRevenue(resultSet.getDouble("TOT_REVENUE"));

				// code added for movement by volume
				summaryDto.setregMovementVolume(resultSet
						.getDouble("REG_MOVEMENT_VOL"));
				summaryDto.setsaleMovementVolume(resultSet
						.getDouble("SALE_MOVEMENT_VOL"));
				summaryDto.setigRegVolumeRev(resultSet
						.getDouble("REG_IGVOL_REVENUE"));
				summaryDto.setigSaleVolumeRev(resultSet
						.getDouble("SALE_IGVOL_REVENUE"));
				summaryDto.settotMovementVolume(resultSet
						.getDouble("TOT_MOVEMENT_VOL"));
				summaryDto.setigtotVolumeRev(resultSet
						.getDouble("TOT_IGVOL_REVENUE"));
				summaryDto.setidRegMovementVolume(resultSet
						.getDouble("ID_REG_MOVEMENT_VOL"));
				summaryDto.setidSaleMovementVolume(resultSet
						.getDouble("ID_SALE_MOVEMENT_VOL"));
				summaryDto.setidIgRegVolumeRev(resultSet
						.getDouble("ID_REG_IGVOL_REVENUE"));
				summaryDto.setidIgSaleVolumeRev(resultSet
						.getDouble("ID_SALE_IGVOL_REVENUE"));
				summaryDto.setidTotMovementVolume(resultSet
						.getDouble("ID_TOT_MOVEMENT_VOL"));
				summaryDto.setIdIgtotVolumeRev(resultSet
						.getDouble("ID_TOT_IGVOL_REVENUE"));
				
				// Private label level aggregations
				summaryDto.setPLTotalRevenue( resultSet.getDouble("PL_TOT_REVENUE"));
				summaryDto.setPLRegularRevenue(resultSet.getDouble("PL_REG_REVENUE"));
				summaryDto.setPLSaleRevenue(resultSet.getDouble("PL_SALE_REVENUE"));
				summaryDto.setPLTotalMargin(resultSet.getDouble("PL_TOT_MARGIN"));
				summaryDto.setPLRegularMargin(resultSet.getDouble("PL_REG_MARGIN"));
				summaryDto.setPLSaleMargin(resultSet.getDouble("PL_SALE_MARGIN"));
				summaryDto.setPLTotalMovement(resultSet.getDouble("PL_TOT_MOVEMENT"));
				summaryDto.setPLRegularMovement(resultSet	.getDouble("PL_REG_MOVEMENT"));
				summaryDto.setPLSaleMovement(resultSet.getDouble("PL_SALE_MOVEMENT"));
				summaryDto.setPLtotMovementVolume(resultSet.getDouble("PL_TOT_MOVEMENT_VOL"));
				summaryDto.setPLregMovementVolume(resultSet.getDouble("PL_REG_MOVEMENT_VOL"));
				summaryDto.setPLsaleMovementVolume(resultSet.getDouble("PL_SALE_MOVEMENT_VOL"));
				summaryDto.setPLTotalVisitCount(resultSet.getDouble("PL_TOT_VISIT_CNT"));

				summaryDto.setPLitotalRevenue(resultSet.getDouble("PL_ITOT_REVENUE"));
				summaryDto.setPLiregularRevenue(resultSet.getDouble("PL_IREG_REVENUE"));
				summaryDto.setPLisaleRevenue(resultSet.getDouble("PL_ISALE_REVENUE"));
				summaryDto.setPLitotalMargin(resultSet.getDouble("PL_ITOT_MARGIN"));
				summaryDto.setPLiregularMargin(resultSet.getDouble("PL_IREG_MARGIN"));
				summaryDto.setPLisaleMargin(resultSet.getDouble("PL_ISALE_MARGIN"));
				summaryDto.setPLitotalMovement(resultSet.getDouble("PL_ITOT_MOVEMENT")); 
				summaryDto.setPLiregularMovement(resultSet.getDouble("PL_IREG_MOVEMENT"));
				summaryDto.setPLisaleMovement(resultSet.getDouble("PL_ISALE_MOVEMENT"));
				summaryDto.setPLidTotMovementVolume(resultSet.getDouble("PL_ID_TOT_MOVEMENT_VOL"));
				summaryDto.setPLidRegMovementVolume(resultSet.getDouble("PL_ID_REG_MOVEMENT_VOL"));
				summaryDto.setPLidSaleMovementVolume(resultSet.getDouble("PL_ID_SALE_MOVEMENT_VOL"));
				summaryDto.setPLitotalVisitCount(resultSet.getDouble("PL_ITOT_VISIT_CNT"));
				
				chainList.add(summaryDto);
			}
			logger.debug(" Chain Map " + _chainId + "Map Size"
					+ chainList.size());

		} catch (GeneralException exe) {
			logger.error(exe);
		} catch (SQLException sq) {
			logger.error(sq);
		}

		return chainList;
	}

	/*
	 * Method used to get the district wise identical store summary records
	 * Argument 1 : Connection Argument 2 : RetailCalendarDTO Argument 3 :
	 * LocationId
	 * 
	 * @throw GeneralException
	 * 
	 * @catch Exception
	 */

	public HashMap<String, SummaryDataDTO> getDistrictIdenticalAggrData(
			Connection _conn, int calendarId, Date processDate,
			String locationId) {

		// object for return hashmap
		HashMap<String, SummaryDataDTO> returnMap = new HashMap<String, SummaryDataDTO>();
		SimpleDateFormat formatter = new SimpleDateFormat("dd-MM-yyyy");

		StringBuffer sql = new StringBuffer();
		sql.append(" select SA.PRODUCT_LEVEL_ID,SA.PRODUCT_ID");
		sql.append(" ,CS.DISTRICT_ID as locationId");
		sql.append(" ,sum(SA.TOT_VISIT_CNT) as TOT_VISIT_CNT ");
		sql.append(" ,sum(SA.TOT_MOVEMENT) as TOT_MOVEMENT");
		sql.append(" ,sum(SA.REG_MOVEMENT) as REG_MOVEMENT");
		sql.append(" ,sum(SA.SALE_MOVEMENT) as SALE_MOVEMENT ");
		sql.append(" ,sum(SA.REG_REVENUE) as REG_REVENUE");
		sql.append(" ,sum(SA.SALE_REVENUE) as SALE_REVENUE");
		sql.append(" ,sum(SA.TOT_MARGIN) as TOT_MARGIN ");
		sql.append(" ,sum(SA.REG_MARGIN) as REG_MARGIN");
		sql.append(" ,sum(SA.SALE_MARGIN) as SALE_MARGIN");
		sql.append(" ,sum(SA.TOT_REVENUE) as TOT_REVENUE");
		
		// Code added for Movement By Volume
		sql.append(" ,sum(REG_MOVEMENT_VOL) as REG_MOVEMENT_VOL");
		sql.append(" ,sum(SALE_MOVEMENT_VOL) as SALE_MOVEMENT_VOL");
		sql.append(" ,sum(REG_IGVOL_REVENUE) as REG_IGVOL_REVENUE");
		sql.append(" ,sum(SALE_IGVOL_REVENUE) as SALE_IGVOL_REVENUE");
		sql.append(" ,sum(TOT_MOVEMENT_VOL) as TOT_MOVEMENT_VOL");
		sql.append(" ,sum(TOT_IGVOL_REVENUE) as TOT_IGVOL_REVENUE");
		
		// For Private label items
		sql.append(" ,SUM(PL_TOT_REVENUE) AS PL_TOT_REVENUE"); 
		sql.append(" ,SUM(PL_REG_REVENUE) AS PL_REG_REVENUE");
		sql.append(" ,SUM(PL_SALE_REVENUE) AS PL_SALE_REVENUE");
		sql.append(" ,SUM(PL_TOT_MARGIN) AS PL_TOT_MARGIN");
		sql.append(" ,SUM(PL_REG_MARGIN) AS PL_REG_MARGIN");
		sql.append(" ,SUM(PL_SALE_MARGIN) AS PL_SALE_MARGIN");
		sql.append(" ,SUM(PL_TOT_MARGIN_PCT) AS PL_TOT_MARGIN_PCT");
		sql.append(" ,SUM(PL_REG_MARGIN_PCT) AS PL_REG_MARGIN_PCT");
		sql.append(" ,SUM(PL_SALE_MARGIN_PCT) AS PL_SALE_MARGIN_PCT");
		sql.append(" ,SUM(PL_TOT_MOVEMENT) AS PL_TOT_MOVEMENT");
		sql.append(" ,SUM(PL_REG_MOVEMENT) AS PL_REG_MOVEMENT");
		sql.append(" ,SUM(PL_SALE_MOVEMENT) AS PL_SALE_MOVEMENT");
		sql.append(" ,SUM(PL_TOT_MOVEMENT_VOL) AS PL_TOT_MOVEMENT_VOL");
		sql.append(" ,SUM(PL_REG_MOVEMENT_VOL) AS PL_REG_MOVEMENT_VOL");
		sql.append(" ,SUM(PL_SALE_MOVEMENT_VOL) AS PL_SALE_MOVEMENT_VOL");
		sql.append(" ,SUM(PL_AVG_ORDER_SIZE) AS PL_AVG_ORDER_SIZE");
		sql.append(" ,SUM(PL_TOT_VISIT_CNT) AS PL_TOT_VISIT_CNT");		
		
		sql.append(" from");
		sql.append(" SALES_AGGR_DAILY SA");
		sql.append(" Inner Join COMPETITOR_STORE CS ");
		sql.append(" on SA.LOCATION_ID = CS.COMP_STR_ID");
		sql.append(" where");
		sql.append(" CS.DISTRICT_ID =" + locationId + "");
		sql.append(" and SA.CALENDAR_ID =" + calendarId + "");
		/* add_months( to_date('15-dec-2012'), -12 ) */
		sql.append(" and CS.OPEN_DATE < to_date('"+ formatter.format(processDate) + "','dd-mm-yyyy')-364");
		sql.append(" group by CS.DISTRICT_ID, SA.PRODUCT_LEVEL_ID, SA.PRODUCT_ID");
		sql.append(" order by CS.DISTRICT_ID, SA.PRODUCT_LEVEL_ID, SA.PRODUCT_ID");

		logger.debug("District Identical SQL" + sql.toString());
		try {
			// Execute Query
			CachedRowSet resultSet = PristineDBUtil.executeQuery(_conn, sql,
					"fetchRollUpDaily");

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
				summaryDto.setLocationId(resultSet.getInt("locationId"));
				summaryDto.setTotalRevenue(resultSet.getDouble("TOT_REVENUE"));

				// code added for Movement By Volume
				summaryDto.setregMovementVolume(resultSet.getDouble("REG_MOVEMENT_VOL"));
				summaryDto.setsaleMovementVolume(resultSet.getDouble("SALE_MOVEMENT_VOL"));
				summaryDto.setigRegVolumeRev(resultSet.getDouble("REG_IGVOL_REVENUE"));
				summaryDto.setigSaleVolumeRev(resultSet.getDouble("SALE_IGVOL_REVENUE"));
				summaryDto.settotMovementVolume(resultSet.getDouble("TOT_MOVEMENT_VOL"));
				summaryDto.setigtotVolumeRev(resultSet.getDouble("TOT_IGVOL_REVENUE"));
				
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

				// add the dto into district map
				returnMap.put(
						summaryDto.getProductLevelId() + "_"
								+ summaryDto.getProductId() + "_"
								+ summaryDto.getLocationId(), summaryDto);
			}
			// logger.debug(" District Map "+ processId + "Map Size" +
		 

		} catch (GeneralException exe) {
			logger.error(exe);
		} catch (SQLException sq) {
			logger.error(sq);
		}

		return returnMap; // return the district map

	}
	
	/*
	 * Method used to get the summarydaily-rollupid with retail calendar actual number...
	 * Argument 1 : Connection 
	 * Argument 2 : Process Year 
	 * @catch SqlException ,Exception
	 * @throws General exception
	 */

	public List<SummaryDataDTO> getLastYearDayLevel(Connection conn,
			int _processYear) throws GeneralException {
		 

		logger.info(" GET LAST-YEAR DAY LEVEL SUMMARY-DAILY BEGIN...................");
		
		List<SummaryDataDTO> returnList = new ArrayList<SummaryDataDTO>();
		try{
		StringBuffer sql = new StringBuffer();
		sql.append(" SELECT RA.ACTUAL_NO,SA.LOCATION_ID");
		sql.append(" ,SA.LOCATION_LEVEL_ID");
		sql.append(" , SA.SUMMARY_DAILY_ROLLUP_ID");
		sql.append(" from  SALES_AGGR_DAILY_ROLLUP  SA"); 
		sql.append(" inner join RETAIL_CALENDAR RA ON SA.CALENDAR_ID= RA.CALENDAR_ID");
		sql.append(" where RA.CAL_YEAR=").append(_processYear);
		sql.append(" AND RA.row_type='D' AND PRODUCT_ID IS NULL");
		logger.debug(" getLastYearDayLevel .... " + sql.toString());
		
		// execute the query....
		CachedRowSet result = PristineDBUtil.executeQuery(conn, sql, "getLastYearDayLevel");
		
		while( result.next()){
			
			SummaryDataDTO objDto = new SummaryDataDTO();
			objDto.setactualNo(result.getInt("ACTUAL_NO"));
			objDto.setLocationId(result.getInt("LOCATION_ID"));
			objDto.setLocationLevelId(result.getInt("LOCATION_LEVEL_ID"));
			objDto.setlastAggrSalesId(result.getInt("SUMMARY_DAILY_ID"));
			returnList.add(objDto);			
		}
				
		}catch(Exception exe){
			logger.error(" getLastYearDayLevel error.... " + exe);
			throw new GeneralException(" getLastYearDayLevel error.... " + exe);
			
		}
		return returnList;
	}

	/*
	 * Method used to check Daily temp table for a given district
	 * If data avilable in temp table means district under the repair mode
	 * Then we just move already processed records into dailyrollup temp table
	 * Argument 1 : conn
	 * Argument 2 : Calendar id
	 * Argument 3 : locationId Id
	 * Argument 4 : locationLevelId
	 * throws GeneralException 
	 */
	
	public void moveTempTable(Connection _conn, int calendarId,
			String locationId, int locationLevelId) throws GeneralException {

		StringBuffer sql = new StringBuffer();

		try {
			
			// check the availability for the given where condition
			// if available means delete and move the records
			sql.append(" delete from SALES_AGGR_DAILY_ROLLUP_TEMP");
			sql.append(" where LOCATION_ID=").append(locationId);
			sql.append(" and LOCATION_LEVEL_ID=").append(locationLevelId);
			sql.append(" and CALENDAR_ID=").append(calendarId);
			PristineDBUtil.execute(_conn, sql, "Deletetemptable");
			
			
			sql = new StringBuffer();
			sql.append(" insert into SALES_AGGR_DAILY_ROLLUP_TEMP");
			sql.append(" select * from SALES_AGGR_DAILY_ROLLUP");
			sql.append(" where LOCATION_ID=").append(locationId);
			sql.append(" and LOCATION_LEVEL_ID=").append(locationLevelId);
			sql.append(" and CALENDAR_ID=").append(calendarId);
			logger.debug(" Temp Movement ... " + sql.toString());

			int count = PristineDBUtil.executeUpdate(_conn, sql, "moveTempTable");
			
			if(count <= 0){
				insertDummyRecord(_conn, calendarId, locationId, locationLevelId);
			}
		} catch (GeneralException e) {
			logger.error("Error while Moving Temp table" + e.getMessage());
			throw new GeneralException("Error while Moving Temp table" 
														+ e.getMessage());
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
	public void insertDummyRecord(Connection _conn, int calendarId, String locationId, int locationLevelId) throws GeneralException{	
		StringBuffer sql = new StringBuffer();
		sql.append("INSERT INTO SALES_AGGR_DAILY_ROLLUP_TEMP ");
		sql.append("(SALES_AGGR_DAILY_ROLLUP_ID,CALENDAR_ID,LOCATION_LEVEL_ID,LOCATION_ID,PRODUCT_LEVEL_ID,PRODUCT_ID,TOT_VISIT_CNT,TOT_MOVEMENT, ");
		sql.append("REG_MOVEMENT,SALE_MOVEMENT,TOT_MOVEMENT_VOL,REG_MOVEMENT_VOL,SALE_MOVEMENT_VOL,TOT_REVENUE,REG_REVENUE,SALE_REVENUE,AVG_ORDER_SIZE, ");
		sql.append("TOT_MARGIN,REG_MARGIN,SALE_MARGIN,TOT_MARGIN_PCT,REG_MARGIN_PCT,SALE_MARGIN_PCT,LOYALTY_CARD_SAVING,ID_TOT_VISIT_CNT,ID_TOT_MOVEMENT, ");
		sql.append("ID_REG_MOEVEMNT,ID_SALE_MOVEMENT,ID_TOT_MOVEMENT_VOL,ID_REG_MOVEMENT_VOL,ID_SALE_MOVEMENT_VOL,ID_TOT_REVENUE,ID_REG_REVENUE,ID_SALE_REVENUE, ");
		sql.append("ID_AVG_ORDER_SIZE,ID_TOT_MARGIN,ID_REG_MARGIN,ID_SALE_MARGIN,ID_TOT_MARGIN_PCT,ID_REG_MARGIN_PCT,ID_SALE_MARGIN_PCT,REG_IGVOL_REVENUE, ");
		sql.append("SALE_IGVOL_REVENUE,TOT_IGVOL_REVENUE,ID_REG_IGVOL_REVENUE,ID_SALE_IGVOL_REVENUE,ID_TOT_IGVOL_REVENUE,LST_SALES_AGGR_ROLLUP_ID,SUMMARY_CTD_ID) ");
		sql.append("VALUES (SALES_AGGR_DAILY_ROLLUP_SEQ.NEXTVAL, ?, ?, ?, NULL, NULL, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, ");
		sql.append("0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0) ");
		
		PreparedStatement statement = null;
		try{
			statement = _conn.prepareStatement(sql.toString());
			statement.setInt(1, calendarId);
			statement.setInt(2, locationLevelId);
			statement.setString(3, locationId);
			
			int count = statement.executeUpdate();
		}catch (SQLException e) {
			logger.error("Error While Inserting Dummy Data:"	+ e.getMessage());
			throw new GeneralException("insert Temp Table" , e);
		}finally{
			PristineDBUtil.close(statement);
		}
	}
	
	/*
	 * get least non processed calendar Id for given 
	 * District / Region / Division / Chain
	 * Argument 1 : _conn
	 * Argument 2 : _districtId
	 * Argument 3 : _regionId
	 * Argument 4 : _divisionId
	 * Argument 5 : _chainId
	 * 
	 */

	public String getLeastNonProcessCalendar(Connection _conn,
			String _districtId, String _regionId, String _divisionId,
			String _chainId) throws GeneralException {
		
		String returnDate = null;
		
		try {
			// Query
			StringBuffer sql = new StringBuffer();
			
			if (_districtId != "") {

				sql.append(" select START_DATE from  RETAIL_CALENDAR");
				sql.append(" where CALENDAR_ID in(select min(CALENDAR_ID)");
				sql.append(" from SALES_AGGR_DAILY_TEMP where LOCATION_ID");
				sql.append(" in(select COMP_STR_ID from COMPETITOR_STORE");
				sql.append(" where  DISTRICT_ID =").append(_districtId)
						.append("))");
			}

			else if (_regionId != "") {
				sql.append(" select START_DATE from  RETAIL_CALENDAR");
				sql.append(" where CALENDAR_ID in(select min(CALENDAR_ID)");
				sql.append(" from SALES_AGGR_DAILY_ROLLUP_TEMP where");
				sql.append(" LOCATION_ID in(select ID from RETAIL_DISTRICT");
				sql.append("  where REGION_ID=").append(_regionId).append(")");
				sql.append(" and LOCATION_LEVEL_ID=")
						.append(Constants.DISTRICT_LEVEL_ID).append(")");
			} else if (_divisionId != "") {
				sql.append(" select START_DATE from RETAIL_CALENDAR");
				sql.append(" where CALENDAR_ID in(select min(CALENDAR_ID)");
				sql.append(" from SALES_AGGR_DAILY_ROLLUP_TEMP where");
				sql.append(" LOCATION_ID in(select ID from RETAIL_REGION");
				sql.append(" where DIVISION_ID=").append(_divisionId).append(")");
				sql.append(" AND LOCATION_LEVEL_ID=")
						.append(Constants.REGION_LEVEL_ID).append(")");

			} else if (_chainId != "") {
				sql.append(" select START_DATE from RETAIL_CALENDAR");
				sql.append(" where CALENDAR_ID in(select min(CALENDAR_ID)");
				sql.append(" from SALES_AGGR_DAILY_ROLLUP_TEMP where");
				sql.append(" LOCATION_LEVEL_ID=").append(Constants.DISTRICT_LEVEL_ID).append(")");
			}
			
			logger.debug("getLeastNonProcessCalendar SQL:" + sql.toString());
			
			// execute the query
			CachedRowSet result = PristineDBUtil.executeQuery(_conn, sql,
					"getLeastNonProcessCalendar");
			 

			while (result.next()) {
				returnDate = result.getString("START_DATE");
			}
			
			 
		} catch (SQLException e) {
			logger.error(" Error while fetching Least Calendar Id " , e);
			throw new GeneralException(" Error while fetching Least Calendar Id " , e);
		} catch (GeneralException e) {
			logger.error(" Error while fetching Least Calendar Id " , e);
			throw new GeneralException(" Error while fetching Least Calendar Id " , e);
		}
				
		return returnDate;
	}

}
