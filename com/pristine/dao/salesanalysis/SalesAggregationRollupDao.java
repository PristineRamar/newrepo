package com.pristine.dao.salesanalysis;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.util.HashMap;
import javax.sql.rowset.CachedRowSet;
import org.apache.log4j.Logger;
import com.pristine.business.entity.SalesaggregationbusinessV2;
import com.pristine.dto.salesanalysis.SummaryDataDTO;
import com.pristine.exception.GeneralException;
import com.pristine.util.GenericUtil;
import com.pristine.util.PristineDBUtil;

public class SalesAggregationRollupDao {
	
	static Logger logger = Logger.getLogger("SalesAggregationRollupDao");
	
	/*
	 * ****************************************************************
	 * Get the weekly aggregtaion records for given input
	 * Argument 1 : Connection
	 * Argument 2 : Week Calendar Id
	 * Argument 3 : District Id
	 * Arguemnt 4 : locationLevelId
	 * Argument 5 : Process Table Name
	 * GeneralException , SqlException .
	 * ****************************************************************
	 */
	
	public HashMap<String, SummaryDataDTO> rollupAggregation(Connection _conn,
			int _weekCalendarId, String _locationId, int _locationLevelId,
			String tableName) throws GeneralException {

		HashMap<String, SummaryDataDTO> rollupMap = new HashMap<String, SummaryDataDTO>();
		try {
			StringBuffer sql = new StringBuffer();
			sql.append(" select SA.PRODUCT_LEVEL_ID,SA.PRODUCT_ID ,SA.LOCATION_ID");
			sql.append(" ,sum(SA.TOT_VISIT_CNT) as TOT_VISIT_CNT");
			sql.append(" ,sum(SA.TOT_MOVEMENT) as TOT_MOVEMENT");
			sql.append(" ,sum(SA.REG_MOVEMENT) as REG_MOVEMENT");
			sql.append(" ,sum(SA.SALE_MOVEMENT) as SALE_MOVEMENT");
			sql.append(" ,sum(SA.REG_REVENUE) as REG_REVENUE");
			sql.append(" ,sum(SA.SALE_REVENUE) as SALE_REVENUE");
			sql.append(" ,sum(SA.TOT_MARGIN) as TOT_MARGIN");
			sql.append(" ,sum(SA.REG_MARGIN) as REG_MARGIN");
			sql.append(" ,sum(SA.SALE_MARGIN) as SALE_MARGIN");
			sql.append(" ,sum(tot_revenue) as TOT_REVENUE");

			if (tableName.equalsIgnoreCase("SALES_AGGR_DAILY_ROLLUP"))
				sql.append(" ,SUMMARY_CTD_ID");

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

			sql.append(" from");
			sql.append(" " + tableName + " SA");
			sql.append(" where");
			sql.append(" SA.CALENDAR_ID =" + _weekCalendarId
					+ " and SA.LOCATION_ID=" + _locationId + "");
			sql.append(" and SA.LOCATION_LEVEL_ID=" + _locationLevelId + " ");
			sql.append(" group by SA.PRODUCT_LEVEL_ID,SA.PRODUCT_ID,SA.LOCATION_ID");
			if (tableName.equalsIgnoreCase("SALES_AGGR_DAILY_ROLLUP"))
				sql.append(" ,SUMMARY_CTD_ID");
			sql.append(" order by SA.PRODUCT_LEVEL_ID,SA.PRODUCT_ID,SA.LOCATION_ID");

			logger.debug("rollupAggregation SQL: " + sql.toString());

			CachedRowSet resultSet = PristineDBUtil.executeQuery(_conn, sql,
					"getWeeklyAggregation");
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
				summaryDto.setLocationId(resultSet.getInt("LOCATION_ID"));
				summaryDto.setLocationLevelId(_locationLevelId);

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

				if (tableName.equalsIgnoreCase("SALES_AGGR_DAILY_ROLLUP")) {
					summaryDto.setsummaryCtdId(resultSet
							.getInt("SUMMARY_CTD_ID"));
				}
				rollupMap.put(
						summaryDto.getProductLevelId() + "_"
								+ summaryDto.getProductId(), summaryDto);
			}

		} catch (Exception exe) {
			logger.error(" Error While Fetching the values.... " + exe);
			throw new GeneralException(" Error While Fetching the result..... "
					+ exe);
		}
		return rollupMap;
	}

	/*
	 * ****************************************************************
	 * Update the weekly data
	 * Argument 1 : Connection
	 * Argument 2 : Week Calendar Id
	 * Argument 3 : productMap
	 * Returns List<SummaryDataDTO> Weekly List
	 * catch GeneralException , SqlException .
	 * ****************************************************************
	 */
	
	public void updateSalesRollup(Connection _conn,
			HashMap<String, SummaryDataDTO> productMap, int aggrCalendarId,
			int rollupCalendarId, String tableName, String processMode)
			throws GeneralException {

		try {
			SalesaggregationbusinessV2 businessLogic = new SalesaggregationbusinessV2();

			PreparedStatement updatePsmt = _conn.prepareStatement(processSql(
					tableName, "UPDATE"));
			PreparedStatement updatePsmtstore = _conn
					.prepareStatement(processSql(tableName, "UPDATE_STORE"));

			Object[] outerLoop = productMap.values().toArray();
			for (int ii = 0; ii < outerLoop.length; ii++) {
				SummaryDataDTO summaryDto = (SummaryDataDTO) outerLoop[ii];
				summaryDto.setcalendarId(rollupCalendarId);

				summaryDto.setlastAggrCalendarId(aggrCalendarId);

				if (processMode.equalsIgnoreCase("ACTUAL"))
					// calculate the other metrics info
					businessLogic.CalculateMetrixInformation(summaryDto, true);

				if (summaryDto.getProductLevelId() == 0) {
					addSqlBatch(summaryDto, true, updatePsmtstore, "UPDATE");
				} else {
					// calculate the other metrics info
					addSqlBatch(summaryDto, false, updatePsmt, "UPDATE");
				}

				// add the values into psmt batch

			}
			// Prepare the Store Aggregation records
			int[] count = updatePsmt.executeBatch();
			logger.debug("Update record Count:" + count.length);
			count = updatePsmtstore.executeBatch();

			updatePsmtstore.close();
			updatePsmt.close();
		} catch (Exception exe) {
			logger.error(" Error while updating the results..... " + exe);
			throw new GeneralException(
					" Error while updating the results..... " + exe);

		}

	}
	
	/*
	 * ****************************************************************
	 * Method to  insert the Store level aggregation records
	 * Argument 1 : Product HashMap
	 * Argument 2 : Connection
	 * Argument 3 : calendarId
	 * Argument 4 : storeId
	 * call the execute batch method
	 * ****************************************************************
	 */

	public void insertSalesRollup(Connection _conn,
			HashMap<String, SummaryDataDTO> dailyMap, int aggrCalendarId,
			int rollupCalendarId, String tableName,
			HashMap<String, Long> lastSummaryList) throws GeneralException {

		try {
			SalesaggregationbusinessV2 objSalesBusiness = new SalesaggregationbusinessV2();

			PreparedStatement weekPsmt = _conn.prepareStatement(processSql(
					tableName, "INSERT"));

			Object[] outerLoop = dailyMap.values().toArray();
			for (int ii = 0; ii < outerLoop.length; ii++) {
				SummaryDataDTO summaryDto = (SummaryDataDTO) outerLoop[ii];
				summaryDto.setcalendarId(rollupCalendarId);
				summaryDto.setlastAggrCalendarId(aggrCalendarId);


				if (summaryDto.getLocationLevelId() == 0){
					if (lastSummaryList.containsKey(summaryDto.getLocationLevelId()
							+ "_" + summaryDto.getProductLevelId() + "_" + null)) {
	
						summaryDto.setlastAggrSalesId(lastSummaryList
								.get(summaryDto.getLocationLevelId() + "_"
										+ summaryDto.getProductLevelId() + "_"
										+ null));
					}
				}
				else{
					if (lastSummaryList.containsKey(summaryDto.getLocationLevelId()
							+ "_" + summaryDto.getProductLevelId() + "_" + summaryDto.getProductId())) {
	
						summaryDto.setlastAggrSalesId(lastSummaryList
								.get(summaryDto.getLocationLevelId() + "_"
										+ summaryDto.getProductLevelId() + "_"
										+ summaryDto.getProductId()));
					}
				}
					
				// calculate the other metrics info
				objSalesBusiness.CalculateMetrixInformation(summaryDto, true);

				// add the values into psmt batch
				addSqlBatch(summaryDto, true, weekPsmt, "INSERT");
			}
			// Prepare the Store Aggregation records
			int[] wcount = weekPsmt.executeBatch();
			logger.debug("Insert record Count:" + wcount.length);

			weekPsmt.close();
		} catch (Exception exe) {
			logger.error(" Error While inserting the data.... " + exe);
			throw new GeneralException(" Error While inserting the data.... "
					+ exe);
		}

	}

	/*
	 * ****************************************************************
	 * Add the values into PreparedStatement  
	 * Argument 1 : summaryDto
	 * Argument 2 : mode of sql
	 * Argument 3 : PreparedStatement
	 * Argument 4 : table name
	 * catch GeneralException , SqlException .
	 * ****************************************************************
	 */

	private void addSqlBatch(SummaryDataDTO summaryDto, boolean insertMode,
			PreparedStatement psmt, String tableName) {

		try {
			if (tableName.equalsIgnoreCase("INSERT")) {
				psmt.setObject(1, summaryDto.getcalendarId());
				psmt.setObject(2, summaryDto.getLocationLevelId());
				psmt.setObject(3, summaryDto.getLocationId());

				if (summaryDto.getProductLevelId() == 0)
					psmt.setNull(4, java.sql.Types.INTEGER);
				else
					psmt.setObject(4, summaryDto.getProductLevelId());

				psmt.setString(5, summaryDto.getProductId());
				psmt.setDouble(6,
						GenericUtil.Round(summaryDto.getTotalVisitCount(), 2));
				psmt.setDouble(7,
						GenericUtil.Round(summaryDto.getAverageOrderSize(), 2));
				psmt.setDouble(22,
						GenericUtil.Round(summaryDto.getitotalVisitCount(), 2));
				psmt.setDouble(23,
						GenericUtil.Round(summaryDto.getiaverageOrderSize(), 2));

				psmt.setDouble(8,
						GenericUtil.Round(summaryDto.getTotalMovement(), 2));
				psmt.setDouble(9,
						GenericUtil.Round(summaryDto.getTotalRevenue(), 2));
				psmt.setDouble(10,
						GenericUtil.Round(summaryDto.getRegularRevenue(), 2));
				psmt.setDouble(11,
						GenericUtil.Round(summaryDto.getSaleRevenue(), 2));

				psmt.setDouble(12, GenericUtil.Round(summaryDto.getTotalMargin(), 2));
				psmt.setDouble(13, GenericUtil.Round(summaryDto.getRegularMargin(), 2));
				psmt.setDouble(14, GenericUtil.Round(summaryDto.getSaleMargin(), 2));
				psmt.setDouble(15, GenericUtil.Round(summaryDto.getTotalMarginPer(), 2));
				psmt.setDouble(16, GenericUtil.Round(summaryDto.getRegularMarginPer(), 2));
				psmt.setDouble(17, GenericUtil.Round(summaryDto.getSaleMarginPer(), 2));

				psmt.setDouble(18, summaryDto.getlastAggrCalendarId());
				psmt.setDouble(19,GenericUtil.Round(summaryDto.getSaleMovement(), 2));
				psmt.setDouble(20,GenericUtil.Round(summaryDto.getRegularMovement(), 2));
				psmt.setLong(21,summaryDto.getlastAggrSalesId());
				psmt.setDouble(24,GenericUtil.Round(summaryDto.getitotalMovement(), 2));
				psmt.setDouble(25,GenericUtil.Round(summaryDto.getitotalRevenue(), 2));
				psmt.setDouble(26,GenericUtil.Round(summaryDto.getiregularRevenue(), 2));
				psmt.setDouble(27,GenericUtil.Round(summaryDto.getisaleRevenue(), 2));
				
				psmt.setDouble(28, GenericUtil.Round(summaryDto.getitotalMargin(), 2));
				psmt.setDouble(29, GenericUtil.Round(summaryDto.getiregularMargin(), 2));
				psmt.setDouble(30, GenericUtil.Round(summaryDto.getisaleMargin(), 2));
				psmt.setDouble(31, GenericUtil.Round(summaryDto.getitotalMarginPer(), 2));
				psmt.setDouble(32, GenericUtil.Round(summaryDto.getiregularMarginPer(), 2));
				psmt.setDouble(33, GenericUtil.Round(summaryDto.getisaleMarginPer(), 2));
				psmt.setDouble(34,
						GenericUtil.Round(summaryDto.getiregularMovement(), 2));
				psmt.setDouble(35,
						GenericUtil.Round(summaryDto.getisaleMovement(), 2));

				// Code added for Movement By volume.....
				psmt.setDouble(36,
						GenericUtil.Round(summaryDto.getregMovementVolume(), 2));
				psmt.setDouble(37, GenericUtil.Round(
						summaryDto.getsaleMovementVolume(), 2));
				psmt.setDouble(38,
						GenericUtil.Round(summaryDto.getigRegVolumeRev(), 2));
				psmt.setDouble(39,
						GenericUtil.Round(summaryDto.getigSaleVolumeRev(), 2));
				psmt.setDouble(40,
						GenericUtil.Round(summaryDto.gettotMovementVolume(), 2));
				psmt.setDouble(41,
						GenericUtil.Round(summaryDto.getigtotVolumeRev(), 2));

				// Movement By Volume for identical
				psmt.setDouble(42, GenericUtil.Round(
						summaryDto.getidRegMovementVolume(), 2));
				psmt.setDouble(43, GenericUtil.Round(
						summaryDto.getidSaleMovementVolume(), 2));
				psmt.setDouble(44,
						GenericUtil.Round(summaryDto.getidIgRegVolumeRev(), 2));
				psmt.setDouble(45,
						GenericUtil.Round(summaryDto.getidIgSaleVolumeRev(), 2));
				psmt.setDouble(46, GenericUtil.Round(
						summaryDto.getidTotMovementVolume(), 2));
				psmt.setDouble(47,
						GenericUtil.Round(summaryDto.getIdIgtotVolumeRev(), 2));

				psmt.addBatch();
			} else if (tableName.equalsIgnoreCase("UPDATE")) {

				psmt.setDouble(1,
						GenericUtil.Round(summaryDto.getTotalVisitCount(), 2));
				psmt.setDouble(2,
						GenericUtil.Round(summaryDto.getAverageOrderSize(), 2));
				psmt.setDouble(3,
						GenericUtil.Round(summaryDto.getitotalVisitCount(), 2));
				psmt.setDouble(4,
						GenericUtil.Round(summaryDto.getiaverageOrderSize(), 2));

				if (insertMode) {
					psmt.setDouble(44, summaryDto.getLocationLevelId());
				} else {
					psmt.setInt(44, summaryDto.getProductLevelId());
					psmt.setString(45, summaryDto.getProductId());
					psmt.setInt(46, summaryDto.getLocationLevelId());
				}
				psmt.setDouble(5,
						GenericUtil.Round(summaryDto.getTotalMovement(), 2));
				psmt.setDouble(6,
						GenericUtil.Round(summaryDto.getTotalRevenue(), 2));
				psmt.setDouble(7,
						GenericUtil.Round(summaryDto.getRegularRevenue(), 2));
				psmt.setDouble(8,
						GenericUtil.Round(summaryDto.getSaleRevenue(), 2));
				
				// margin calculation
				psmt.setDouble(9, GenericUtil.Round(summaryDto.getTotalMargin(), 2));
				psmt.setDouble(10, GenericUtil.Round(summaryDto.getRegularMargin(), 2));
				psmt.setDouble(11, GenericUtil.Round(summaryDto.getSaleMargin(), 2));
				psmt.setDouble(12, GenericUtil.Round(summaryDto.getTotalMarginPer(), 2));
				psmt.setDouble(13, GenericUtil.Round(summaryDto.getRegularMarginPer(), 2));
				psmt.setDouble(14, GenericUtil.Round(summaryDto.getSaleMarginPer(), 2));
				
				psmt.setInt(15, summaryDto.getlastAggrCalendarId());
				psmt.setDouble(16,
						GenericUtil.Round(summaryDto.getSaleMovement(), 2));
				psmt.setDouble(17,
						GenericUtil.Round(summaryDto.getRegularMovement(), 2));
				psmt.setDouble(18,
						GenericUtil.Round(summaryDto.getitotalMovement(), 2));
				psmt.setDouble(19,
						GenericUtil.Round(summaryDto.getitotalRevenue(), 2));
				psmt.setDouble(20,
						GenericUtil.Round(summaryDto.getiregularRevenue(), 2));
				psmt.setDouble(21,
						GenericUtil.Round(summaryDto.getisaleRevenue(), 2));
				
				psmt.setDouble(22, GenericUtil.Round(summaryDto.getitotalMargin(), 2));
				psmt.setDouble(23, GenericUtil.Round(summaryDto.getiregularMargin(), 2));
				psmt.setDouble(24, GenericUtil.Round(summaryDto.getisaleMargin(), 2));
				psmt.setDouble(25, GenericUtil.Round(summaryDto.getitotalMarginPer(), 2));
				psmt.setDouble(26, GenericUtil.Round(summaryDto.getiregularMarginPer(), 2));
				psmt.setDouble(27, GenericUtil.Round(summaryDto.getisaleMarginPer(), 2));
				
				psmt.setDouble(28,
						GenericUtil.Round(summaryDto.getiregularMovement(), 2));
				psmt.setDouble(29,
						GenericUtil.Round(summaryDto.getisaleMovement(), 2));

				// Code added for Movement By volume.....
				psmt.setDouble(30,
						GenericUtil.Round(summaryDto.getregMovementVolume(), 2));
				psmt.setDouble(31, GenericUtil.Round(
						summaryDto.getsaleMovementVolume(), 2));
				psmt.setDouble(32,
						GenericUtil.Round(summaryDto.getigRegVolumeRev(), 2));
				psmt.setDouble(33,
						GenericUtil.Round(summaryDto.getigSaleVolumeRev(), 2));
				psmt.setDouble(34,
						GenericUtil.Round(summaryDto.gettotMovementVolume(), 2));
				psmt.setDouble(35,
						GenericUtil.Round(summaryDto.getigtotVolumeRev(), 2));

				// Movement By Volume for identical
				psmt.setDouble(36, GenericUtil.Round(
						summaryDto.getidRegMovementVolume(), 2));
				psmt.setDouble(37, GenericUtil.Round(
						summaryDto.getidSaleMovementVolume(), 2));
				psmt.setDouble(38,
						GenericUtil.Round(summaryDto.getidIgRegVolumeRev(), 2));
				psmt.setDouble(39,
						GenericUtil.Round(summaryDto.getidIgSaleVolumeRev(), 2));
				psmt.setDouble(40, GenericUtil.Round(
						summaryDto.getidTotMovementVolume(), 2));
				psmt.setDouble(41,
						GenericUtil.Round(summaryDto.getIdIgtotVolumeRev(), 2));

				psmt.setInt(42, summaryDto.getcalendarId());
				psmt.setInt(43, summaryDto.getLocationId());
				psmt.addBatch();
			}
		} catch (Exception sql) {
			logger.error(sql.getMessage());
			sql.printStackTrace();
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

	private String processSql(String tableName, String sqlMode) {

		StringBuffer sql = new StringBuffer();

		if (sqlMode.equalsIgnoreCase("UPDATE")) {

			sql.append(" update  " + tableName + "  SET  ");

			sql.append(" TOT_VISIT_CNT=? ,AVG_ORDER_SIZE=?,ID_TOT_VISIT_CNT=?,ID_AVG_ORDER_SIZE=?,TOT_MOVEMENT=?, TOT_REVENUE=? , REG_REVENUE=?  ");

			sql.append(" , SALE_REVENUE=? ,TOT_MARGIN=? ,REG_MARGIN=? , SALE_MARGIN=?  ");

			sql.append(", TOT_MARGIN_PCT=?, REG_MARGIN_PCT=? , SALE_MARGIN_PCT=?,LAST_AGGR_CALENDARID=?,SALE_MOVEMENT=?,REG_MOVEMENT=?  ");

			sql.append(" ,ID_TOT_MOVEMENT=?,ID_TOT_REVENUE=?,ID_REG_REVENUE=?,ID_SALE_REVENUE=?");

			sql.append(" ,ID_TOT_MARGIN=?,ID_REG_MARGIN=?,ID_SALE_MARGIN=?,ID_TOT_MARGIN_PCT=?,ID_REG_MARGIN_PCT=?,ID_SALE_MARGIN_PCT=?");

			sql.append(" ,ID_REG_MOEVEMNT=?,ID_SALE_MOVEMENT=?");

			sql.append(" ,REG_MOVEMENT_VOL=?,SALE_MOVEMENT_VOL=?,REG_IGVOL_REVENUE=?,SALE_IGVOL_REVENUE=?");

			sql.append(" ,TOT_MOVEMENT_VOL=?,TOT_IGVOL_REVENUE=?,ID_REG_MOVEMENT_VOL=?,ID_SALE_MOVEMENT_VOL=?");

			sql.append(" ,ID_REG_IGVOL_REVENUE=?,ID_SALE_IGVOL_REVENUE=?,ID_TOT_MOVEMENT_VOL=?,ID_TOT_IGVOL_REVENUE=?");

			sql.append(" where CALENDAR_ID=? and LOCATION_ID=? and PRODUCT_LEVEL_ID=? and PRODUCT_ID=? and LOCATION_LEVEL_ID=? ");
		} else if (sqlMode.equalsIgnoreCase("INSERT")) {

			sql.append(" insert into "
					+ tableName
					+ " ("
					+ tableName
					+ "_ID, CALENDAR_ID, LOCATION_LEVEL_ID, LOCATION_ID,PRODUCT_LEVEL_ID ");

			sql.append(", PRODUCT_ID , TOT_VISIT_CNT ,AVG_ORDER_SIZE,TOT_MOVEMENT, TOT_REVENUE , REG_REVENUE  ");

			sql.append(" , SALE_REVENUE ,TOT_MARGIN ,REG_MARGIN , SALE_MARGIN  ");

			sql.append(", TOT_MARGIN_PCT, REG_MARGIN_PCT , SALE_MARGIN_PCT,LAST_AGGR_CALENDARID,SALE_MOVEMENT,REG_MOVEMENT,LST_SALES_AGGR_ROLLUP_ID  ");

			sql.append(" ,ID_TOT_VISIT_CNT,ID_AVG_ORDER_SIZE,ID_TOT_MOVEMENT,ID_TOT_REVENUE,ID_REG_REVENUE,ID_SALE_REVENUE");

			sql.append(" ,ID_TOT_MARGIN,ID_REG_MARGIN,ID_SALE_MARGIN,ID_TOT_MARGIN_PCT,ID_REG_MARGIN_PCT,ID_SALE_MARGIN_PCT");

			sql.append(" ,ID_REG_MOEVEMNT,ID_SALE_MOVEMENT");

			sql.append(" ,REG_MOVEMENT_VOL,SALE_MOVEMENT_VOL,REG_IGVOL_REVENUE,SALE_IGVOL_REVENUE");

			sql.append(" ,TOT_MOVEMENT_VOL,TOT_IGVOL_REVENUE,ID_REG_MOVEMENT_VOL,ID_SALE_MOVEMENT_VOL");

			sql.append(" ,ID_REG_IGVOL_REVENUE,ID_SALE_IGVOL_REVENUE,ID_TOT_MOVEMENT_VOL,ID_TOT_IGVOL_REVENUE)");

			sql.append(" values ("
					+ tableName
					+ "_SEQ.NEXTVAL,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?");

			sql.append(" ,?,?,?,?,?,?,?,?,?,?,?,?)");

		} else if (sqlMode.equalsIgnoreCase("UPDATE_STORE")) {

			sql.append(" update  " + tableName + " SET  ");

			sql.append(" TOT_VISIT_CNT=? ,AVG_ORDER_SIZE=?,ID_TOT_VISIT_CNT=?,ID_AVG_ORDER_SIZE=?,TOT_MOVEMENT=?, TOT_REVENUE=? , REG_REVENUE=?  ");

			sql.append(" , SALE_REVENUE=? ,TOT_MARGIN=? ,REG_MARGIN=? , SALE_MARGIN=?  ");

			sql.append(", TOT_MARGIN_PCT=?, REG_MARGIN_PCT=? , SALE_MARGIN_PCT=?,LAST_AGGR_CALENDARID=?,SALE_MOVEMENT=?,REG_MOVEMENT=?  ");

			sql.append(" ,ID_TOT_MOVEMENT=?,ID_TOT_REVENUE=?,ID_REG_REVENUE=?,ID_SALE_REVENUE=?");

			sql.append(" ,ID_TOT_MARGIN=?,ID_REG_MARGIN=?,ID_SALE_MARGIN=?,ID_TOT_MARGIN_PCT=?,ID_REG_MARGIN_PCT=?,ID_SALE_MARGIN_PCT=?");

			sql.append(" ,ID_REG_MOEVEMNT=?,ID_SALE_MOVEMENT=?");

			sql.append(" ,REG_MOVEMENT_VOL=?,SALE_MOVEMENT_VOL=?,REG_IGVOL_REVENUE=?,SALE_IGVOL_REVENUE=?");

			sql.append(" ,TOT_MOVEMENT_VOL=?,TOT_IGVOL_REVENUE=?,ID_REG_MOVEMENT_VOL=?,ID_SALE_MOVEMENT_VOL=?");

			sql.append(" ,ID_REG_IGVOL_REVENUE=?,ID_SALE_IGVOL_REVENUE=?,ID_TOT_MOVEMENT_VOL=?,ID_TOT_IGVOL_REVENUE=?");

			sql.append(" where CALENDAR_ID=? and LOCATION_ID=? and PRODUCT_LEVEL_ID is null and PRODUCT_ID is null and LOCATION_LEVEL_ID=? ");
		}

		logger.debug("processSql SQL:" + sql.toString());

		return sql.toString();
	}
	
	
	
}
