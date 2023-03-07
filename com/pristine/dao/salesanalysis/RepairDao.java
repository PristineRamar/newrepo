package com.pristine.dao.salesanalysis;

import java.sql.Connection;
import java.sql.SQLException;
import java.text.DateFormat;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.HashMap;
import java.util.List;

import javax.sql.rowset.CachedRowSet;

import org.apache.log4j.Logger;
import com.pristine.business.entity.SalesaggregationbusinessV2;
import com.pristine.dao.RetailCalendarDAO;
import com.pristine.dto.RetailCalendarDTO;
import com.pristine.dto.salesanalysis.SummaryDataDTO;
import com.pristine.exception.GeneralException;
import com.pristine.util.Constants;
import com.pristine.util.PristineDBUtil;

public class RepairDao {

	
	static Logger logger = Logger.getLogger("RepairDao");
	
	/**
	 * @param previousweeklyAggregation
	 * @param currentWeeklyAggregation
	 * @param _beforeSummarydailyDetails
	 * @param _afterSummarydailyDetails
	 */
	public void calculatRepairAggregation(
			HashMap<String, SummaryDataDTO> previousweeklyAggregation,
			HashMap<String, SummaryDataDTO> _beforeSummarydailyDetails,
			HashMap<String, SummaryDataDTO> _afterSummarydailyDetails) {
		
		logger.info(" before Repairing Process ....Total Processing Count is...... " + previousweeklyAggregation.size());
		
		Object[] mapArray = previousweeklyAggregation.values().toArray();
		
		for( int ii=0; ii < mapArray.length ; ii++)
		{
			SummaryDataDTO objweeklyAggregationDto =(SummaryDataDTO) mapArray[ii];
			
			
			if (_beforeSummarydailyDetails.containsKey(objweeklyAggregationDto
					.getLocationLevelId()
					+ "_"
					+ objweeklyAggregationDto.getLocationId()
					+ "_"
					+ objweeklyAggregationDto.getProductId()
					+ "_"
					+ objweeklyAggregationDto.getProductLevelId())) {
				
				minusCalculation(objweeklyAggregationDto,
						_beforeSummarydailyDetails.get(objweeklyAggregationDto
								.getLocationLevelId()
								+ "_"
								+ objweeklyAggregationDto.getLocationId()
								+ "_"
								+ objweeklyAggregationDto.getProductId()
								+ "_"
								+ objweeklyAggregationDto.getProductLevelId()));
			}
			
			if (_afterSummarydailyDetails.containsKey(objweeklyAggregationDto
					.getLocationLevelId()
					+ "_"
					+ objweeklyAggregationDto.getLocationId()
					+ "_"
					+ objweeklyAggregationDto.getProductId()
					+ "_"
					+ objweeklyAggregationDto.getProductLevelId())) {
						
				SummaryDataDTO afterSummaryDetails = _afterSummarydailyDetails
						.get(objweeklyAggregationDto.getLocationLevelId() + "_"
								+ objweeklyAggregationDto.getLocationId() + "_"
								+ objweeklyAggregationDto.getProductId() + "_"
								+ objweeklyAggregationDto.getProductLevelId());
				
					addCalculation(objweeklyAggregationDto ,afterSummaryDetails);
				}
							
		}
		
		
		logger.info(" After Repairing Process ....Total Processing Count is...... " + previousweeklyAggregation.size());
		
		
	}

	/**
	 * @param weekDto
	 * @param dayDto
	 */
	private void addCalculation(SummaryDataDTO weekDto,
			SummaryDataDTO dayDto) {
		
		weekDto.setRegularMargin(weekDto.getRegularMargin()+dayDto.getRegularMargin());
		weekDto.setRegularMovement(weekDto.getRegularMovement()+dayDto.getRegularMovement());
		weekDto.setRegularRevenue(weekDto.getRegularRevenue()+dayDto.getRegularRevenue());
		weekDto.setSaleMargin(weekDto.getSaleMargin()+dayDto.getSaleMargin());
		weekDto.setSaleMovement(weekDto.getSaleMovement()+dayDto.getSaleMovement());
		weekDto.setSaleRevenue(weekDto.getSaleRevenue()+dayDto.getSaleRevenue());
		weekDto.setTotalMargin(weekDto.getTotalMargin()+dayDto.getTotalMargin());
		weekDto.setTotalMovement(weekDto.getTotalMovement()+dayDto.getTotalMovement());
		weekDto.setTotalRevenue(weekDto.getTotalRevenue()+dayDto.getTotalRevenue());
		weekDto.setTotalVisitCount(weekDto.getTotalVisitCount()+dayDto.getTotalVisitCount());
		
		// for identical
		
		weekDto.setitotalMovement(weekDto.getitotalMovement()+ dayDto.getitotalMovement());
		weekDto.setiregularMovement(weekDto.getiregularMovement()+dayDto.getiregularMovement());
		weekDto.setisaleMovement(weekDto.getisaleMovement()+dayDto.getisaleMovement());
		weekDto.setitotalRevenue(weekDto.getitotalRevenue()+dayDto.getitotalRevenue());
		weekDto.setiregularRevenue(weekDto.getiregularRevenue()+dayDto.getiregularRevenue());
		weekDto.setisaleRevenue(weekDto.getisaleRevenue()+dayDto.getisaleRevenue());
		weekDto.setitotalMargin(weekDto.getitotalMargin()+dayDto.getitotalMargin());
		weekDto.setiregularMargin(weekDto.getiregularMargin()+dayDto.getiregularMargin());
		weekDto.setisaleMargin(weekDto.getisaleMargin()+dayDto.getisaleMargin());
		weekDto.setitotalVisitCount(weekDto.getitotalVisitCount()+dayDto.getitotalVisitCount());
		
		
		// for accounts
		weekDto.setAdjTotRevenue(weekDto.getAdjTotRevenue()+dayDto.getAdjTotRevenue());
		weekDto.setAdjIdTotRevenue(weekDto.getAdjIdTotRevenue()+dayDto.getAdjIdTotRevenue());
						
		// for Movement by volume...
		weekDto.setregMovementVolume(weekDto.getregMovementVolume() + dayDto.getregMovementVolume());
		weekDto.setsaleMovementVolume( weekDto.getsaleMovementVolume() + dayDto.getsaleMovementVolume());
		weekDto.setigRegVolumeRev(weekDto.getigRegVolumeRev() + dayDto.getigRegVolumeRev());
		weekDto.setigSaleVolumeRev(weekDto.getigSaleVolumeRev() + dayDto.getigSaleVolumeRev());
		weekDto.settotMovementVolume(weekDto.gettotMovementVolume() + dayDto.gettotMovementVolume());
		weekDto.setigtotVolumeRev(weekDto.getigtotVolumeRev() + dayDto.getigtotVolumeRev());
				
		weekDto.setidRegMovementVolume(weekDto.getidRegMovementVolume() +  dayDto.getidRegMovementVolume());
		weekDto.setidSaleMovementVolume(weekDto.getidSaleMovementVolume() + dayDto.getidSaleMovementVolume());
		weekDto.setidIgRegVolumeRev( weekDto.getidIgRegVolumeRev() + dayDto.getidIgRegVolumeRev());
		weekDto.setidIgSaleVolumeRev( weekDto.getidIgSaleVolumeRev() + dayDto.getidIgSaleVolumeRev());
		weekDto.setidTotMovementVolume( weekDto.getidTotMovementVolume() + dayDto.getidTotMovementVolume());
		weekDto.setIdIgtotVolumeRev( weekDto.getIdIgtotVolumeRev() + dayDto.getIdIgtotVolumeRev());
				
	}

	/**
	 * @param weekDto
	 * @param dayDto
	 */
	private void minusCalculation(SummaryDataDTO weekDto, SummaryDataDTO dayDto) {
		
		weekDto.setRegularMargin(weekDto.getRegularMargin()-dayDto.getRegularMargin());
		weekDto.setRegularMovement(weekDto.getRegularMovement()-dayDto.getRegularMovement());
		weekDto.setRegularRevenue(weekDto.getRegularRevenue()-dayDto.getRegularRevenue());
		weekDto.setSaleMargin(weekDto.getSaleMargin()-dayDto.getSaleMargin());
		weekDto.setSaleMovement(weekDto.getSaleMovement()-dayDto.getSaleMovement());
		weekDto.setSaleRevenue(weekDto.getSaleRevenue()-dayDto.getSaleRevenue());
		weekDto.setTotalMargin(weekDto.getTotalMargin()-dayDto.getTotalMargin());
		weekDto.setTotalMovement(weekDto.getTotalMovement()-dayDto.getTotalMovement());
		weekDto.setTotalRevenue(weekDto.getTotalRevenue()-dayDto.getTotalRevenue());
		weekDto.setTotalVisitCount(weekDto.getTotalVisitCount()-dayDto.getTotalVisitCount());
		
		// for identical
		weekDto.setitotalMovement(weekDto.getitotalMovement()- dayDto.getitotalMovement());
		weekDto.setiregularMovement(weekDto.getiregularMovement()-dayDto.getiregularMovement());
		weekDto.setisaleMovement(weekDto.getisaleMovement()-dayDto.getisaleMovement());
		weekDto.setitotalRevenue(weekDto.getitotalRevenue()-dayDto.getitotalRevenue());
		weekDto.setiregularRevenue(weekDto.getiregularRevenue()-dayDto.getiregularRevenue());
		weekDto.setisaleRevenue(weekDto.getisaleRevenue()-dayDto.getisaleRevenue());
		weekDto.setitotalMargin(weekDto.getitotalMargin()-dayDto.getitotalMargin());
		weekDto.setiregularMargin(weekDto.getiregularMargin()-dayDto.getiregularMargin());
		weekDto.setisaleMargin(weekDto.getisaleMargin()-dayDto.getisaleMargin());
		weekDto.setitotalVisitCount(weekDto.getitotalVisitCount()-dayDto.getitotalVisitCount());

		// for accounts
		weekDto.setAdjTotRevenue(weekDto.getAdjTotRevenue()-dayDto.getAdjTotRevenue());
		weekDto.setAdjIdTotRevenue(weekDto.getAdjIdTotRevenue()-dayDto.getAdjIdTotRevenue());
		
		// for Movement by volume...
		weekDto.setregMovementVolume(weekDto.getregMovementVolume() - dayDto.getregMovementVolume());
		weekDto.setsaleMovementVolume( weekDto.getsaleMovementVolume() - dayDto.getsaleMovementVolume());
		weekDto.setigRegVolumeRev(weekDto.getigRegVolumeRev() - dayDto.getigRegVolumeRev());
		weekDto.setigSaleVolumeRev(weekDto.getigSaleVolumeRev() - dayDto.getigSaleVolumeRev());
		weekDto.settotMovementVolume(weekDto.gettotMovementVolume() - dayDto.gettotMovementVolume());
		weekDto.setigtotVolumeRev(weekDto.getigtotVolumeRev() - dayDto.getigtotVolumeRev());
		
		weekDto.setidRegMovementVolume(weekDto.getidRegMovementVolume() -  dayDto.getidRegMovementVolume());
		weekDto.setidSaleMovementVolume(weekDto.getidSaleMovementVolume() - dayDto.getidSaleMovementVolume());
		weekDto.setidIgRegVolumeRev( weekDto.getidIgRegVolumeRev() - dayDto.getidIgRegVolumeRev());
		weekDto.setidIgSaleVolumeRev( weekDto.getidIgSaleVolumeRev() - dayDto.getidIgSaleVolumeRev());
		weekDto.setidTotMovementVolume( weekDto.getidTotMovementVolume() - dayDto.getidTotMovementVolume());
		weekDto.setIdIgtotVolumeRev( weekDto.getIdIgtotVolumeRev() - dayDto.getIdIgtotVolumeRev());
						
	}
	
	
	/**
	 * @param _conn
	 * @param dailyCalendarList
	 * @param businessLogic
	 * @param objSalesDao
	 * @param objCtdDao
	 * @param locationId
	 * @param locationLevelId
	 * @param ctdConstants
	 * @param processDate
	 * @param _currentDate
	 * @param tableName
	 * @param _objSalesRollupDao 
	 * @throws SQLException
	 * @throws GeneralException
	 * @throws ParseException
	 */
	public void ctdRepairAggregation(Connection _conn,
			List<RetailCalendarDTO> dailyCalendarList,
			SalesaggregationbusinessV2 businessLogic,
			SalesAggregationDao objSalesDao, SalesAggregationCtdDao objCtdDao,
			int locationId, int locationLevelId, int ctdConstants,
			String processDate, String _currentDate, String tableName,
			SalesAggregationRollupDao _objSalesRollupDao) throws SQLException,
			GeneralException, ParseException {

		//logger.debug(" Get Weekly Aggregation Records Starts ");
		
		logger.debug(" inside the Ctd repair Process.....");

		HashMap<String, SummaryDataDTO> productMap = new HashMap<String, SummaryDataDTO>();

		//objCtdDao.deletePreviousCtdAggregation(_conn, locationId,
		//		locationLevelId, processDate, _currentDate, tableName,
		//		ctdConstants);
		
		logger.debug(" Process Date...." + processDate);
		logger.debug(" Current Date......" +_currentDate);
		
		
		// Get the Previous Aggregation details from ctd table.......
		
		productMap = getPreviousCtdAggregationDetails(_conn , locationId ,locationLevelId, tableName , ctdConstants , processDate);
		
		
		try{
		// get the last aggregation records from sales_aggr_weekly table
			
		
			for (int cL = 0; cL < dailyCalendarList.size(); cL++) {
				RetailCalendarDTO calendarDto = dailyCalendarList.get(cL);

				// get daily aggregation for processing date
				logger.info("Fetch aggregation data for Calendar Id "
						+ calendarDto.getCalendarId());

				HashMap<String, SummaryDataDTO> dailyMap = null;

				if (tableName.equalsIgnoreCase("SALES_AGGR_DAILY")) {

					dailyMap = objSalesDao.getSalesAggregation(_conn,
							calendarDto.getCalendarId(), locationId, tableName);
				} else if (tableName
						.equalsIgnoreCase("SALES_AGGR_DAILY_ROLLUP")) {
					dailyMap = _objSalesRollupDao.rollupAggregation(_conn,
							calendarDto.getCalendarId(),
							String.valueOf(locationId), locationLevelId,
							tableName);

				}

				logger.info(" Daily Map Size " + dailyMap.size());

				if (dailyMap.size() != 0) {

					if (productMap.size() != 0) {

						// call the businesslogic method to sum up the daily and
						// weekly records
						productMap = businessLogic.sumupAggregation(dailyMap,
								productMap, calendarDto.getCalendarId());

						// insert Ctd process
						//logger.info("Insert WTD Data into CTD");
						//objCtdDao.insertSalesCtd(_conn, productMap, ctdConstants);

					} else {

						// Insert Ctd Process
						//logger.info("Insert WTD Data into CTD");
						//objCtdDao.insertSalesCtd(_conn, dailyMap, ctdConstants);
						productMap = dailyMap;
					}
				}

				else {

					break;
				}

				// commit the transaction
				PristineDBUtil.commitTransaction(_conn,
						"Commit the summary Weekly Process");

			}

		} catch (Exception exe) {

			PristineDBUtil.rollbackTransaction(_conn,
					"Rollback the Repair Ctd Transation");
			throw new GeneralException("Period Insertion Failed", exe);
		}
	}

	
	/**
	 * @param _conn
	 * @param locationId
	 * @param locationLevelId
	 * @param tableName
	 * @param ctdConstants
	 * @param processDate
	 * @return
	 * @throws GeneralException 
	 */
	private HashMap<String, SummaryDataDTO> getPreviousCtdAggregationDetails(
			Connection _conn, int locationId, int locationLevelId,
			String tableName, int ctdConstants, String processDate) throws GeneralException {
		
		logger.debug(" Process Enter into get previous aggregation details from ctd table...");
		HashMap<String, SummaryDataDTO> objReturnMap = new HashMap<String, SummaryDataDTO>();
		
		String calendarConst = "";
		if( ctdConstants ==2) calendarConst = Constants.CALENDAR_WEEK;
		if( ctdConstants ==3) calendarConst = Constants.CALENDAR_PERIOD;
		if( ctdConstants ==4) calendarConst = Constants.CALENDAR_QUARTER;
		if( ctdConstants ==5) calendarConst = Constants.CALENDAR_YEAR;
		
		RetailCalendarDAO objRetailDao = new RetailCalendarDAO();
		
		try {
			// check given date is week/period/quarter/year startdate or not
			boolean checkStartDate = objRetailDao.checkWeekStartDate(_conn, processDate, calendarConst);
			DateFormat sformatter = new SimpleDateFormat("yyyy-MM-dd");
			Date sDate = (Date) sformatter.parse(processDate);
			
			if( checkStartDate){
				
				try {
					StringBuffer sql = new StringBuffer();
					  sql.append(" SELECT SA.PRODUCT_LEVEL_ID,SA.PRODUCT_ID , SA.LOCATION_ID ,SC.TOT_VISIT_CNT,SA.SUMMARY_CTD_ID,");
					  sql.append(" SC.TOT_MOVEMENT ,SC.REG_MOVEMENT,SC.SALE_MOVEMENT,SC.REG_REVENUE,SC.SALE_REVENUE,");
					  sql.append(" SC.TOT_MARGIN,SC.REG_MARGIN,SC.SALE_MARGIN,SC.TOT_REVENUE,SC.ID_TOT_VISIT_CNT,SC.ID_TOT_MOVEMENT,");
					  sql.append(" SC.ID_REG_MOEVEMNT,SC.ID_SALE_MOVEMENT,SC.ID_REG_REVENUE,SC.ID_SALE_REVENUE,SC.ID_TOT_MARGIN,");
					  sql.append(" SC.ID_REG_MARGIN,SC.ID_SALE_MARGIN,SC.ID_TOT_REVENUE,SC.REG_MOVEMENT_VOL,SC.SALE_MOVEMENT_VOL,");
					  sql.append(" SC.REG_IGVOL_REVENUE,SC.SALE_IGVOL_REVENUE,SC.TOT_MOVEMENT_VOL, SC.TOT_IGVOL_REVENUE, SC.ID_REG_MOVEMENT_VOL,");
					  sql.append(" SC.ID_SALE_MOVEMENT_VOL, SC.ID_REG_IGVOL_REVENUE, SC.ID_SALE_IGVOL_REVENUE , SC.ID_TOT_MOVEMENT_VOL,SC.ID_TOT_IGVOL_REVENUE");
					  sql.append(" FROM SALES_AGGR_CTD SC INNER JOIN ").append(tableName).append( " SA ON SC.SUMMARY_CTD_ID= SA.SUMMARY_CTD_ID");
					  sql.append(" WHERE SA.LOCATION_ID=").append(locationId).append(" AND SA.LOCATION_LEVEL_ID=").append(locationLevelId);
					  sql.append(" AND SA.CALENDAR_ID IN").append(" (SELECT CALENDAR_ID FROM RETAIL_CALENDAR WHERE START_DATE=");
					  sql.append(" to_date('").append(sformatter.format(sDate)).append("','yyyy-mm-dd')-1")
					  								.append(" AND ROW_TYPE='").append(Constants.CALENDAR_DAY).append("')");
					 sql.append(" AND SC.ctd_type=").append(ctdConstants);
					 
					 logger.debug(" Sql for Previous Aggregation Ctd Details..."+sql);
					 
					 CachedRowSet resultSet = PristineDBUtil.executeQuery(_conn, sql, "GetPreviousAggregationDetails");
					
					 while(resultSet.next()){
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
							summaryDto.setsummaryCtdId(resultSet.getInt("SUMMARY_CTD_ID"));
							
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
						 
							objReturnMap.put(
									summaryDto.getProductLevelId() + "_"
											+ summaryDto.getProductId(), summaryDto);
						 
						 
					 }
				} catch (SQLException e) {
					throw new GeneralException(" Error while getting previous ctd details", e);
				}
				 
				
				return objReturnMap;
			}
			else{
				
				return objReturnMap;
			}
			
		} catch (ParseException e) {
			 throw new GeneralException(" Error while pasring the date..."+processDate+"", e); 
		} catch (GeneralException e) {
			 throw new GeneralException(" while checking the start date ", e); 
		}
		 	
	}
	
	

}
