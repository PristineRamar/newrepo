package com.pristine.dataload.tops;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.util.HashMap;
import java.util.List;
import org.apache.log4j.Logger;
import org.apache.log4j.PropertyConfigurator;
import com.pristine.dao.CompStoreDAO;
import com.pristine.dao.DBManager;
import com.pristine.dao.RetailCalendarDAO;
import com.pristine.dao.salesanalysis.SalesAggregationProductGroupDAO;
import com.pristine.dto.salesanalysis.ProductDTO;
import com.pristine.exception.GeneralException;
import com.pristine.util.Constants;
import com.pristine.util.PristineDBUtil;
import com.pristine.util.PropertyManager;

public class TopsAccountsMapper {

	static Logger logger = Logger.getLogger("TestAccountsLoader");

	private Connection conn = null;

	public TopsAccountsMapper() throws GeneralException {

		PropertyManager.initialize("analysis.properties");
		
		
		try {
			conn = DBManager.getConnection();
		} catch (GeneralException e) {
			throw new GeneralException(" Connection Failed.....", e);

		}

	}

	public static void main(String args[]) throws SQLException {
		
		PropertyConfigurator.configure("log4j-LastYear-AccountsMapper.properties");
		
		TopsAccountsMapper objAccountsLoader = null;
		int processYear=0;

		try {
			
				if (args.length >0) {
					// get the process year from command line
					
					String arg = args[0];
					
					if (arg.startsWith("PROCESSYEAR")) {
						try {
							processYear = Integer.parseInt(arg
									.substring("PROCESSYEAR=".length()));

						
							objAccountsLoader = new TopsAccountsMapper();
							
							objAccountsLoader.loadAccounts(processYear);
						
						} catch (Exception exe) {
					
							logger.error(" Input Error : PROCESSYEAR " + exe);
							System.exit(1);
						}
					}
					else {
						logger.error(" Invalid Input, Please Input Process Year");
						System.exit(1);					
					}
				}
				else {
					logger.error(" Please Input Process Year");
					System.exit(1);					
				}

			
		} catch (GeneralException e) {
			logger.error(" Connection Failed....", e);

		} finally {

			try {

				PristineDBUtil.close(objAccountsLoader.conn);
			} catch (Exception e) {
				logger.error(" Error in closing the connection ....", e);
			}

		}

	}

	private void loadAccounts(int processingYear) throws GeneralException,
			SQLException {

		RetailCalendarDAO objCalendarDao = new RetailCalendarDAO();

		CompStoreDAO objCompStoreDao = new CompStoreDAO();

		SalesAggregationProductGroupDAO objProductGroupDao = new SalesAggregationProductGroupDAO();

		try {
			HashMap<String, Integer> periodCalendarMap = objCalendarDao
					.excelBasedCalendarList(conn, null,
							Constants.CALENDAR_PERIOD, processingYear, null);

			HashMap<String, Integer> quarterCalendarMap = objCalendarDao
					.excelBasedCalendarList(conn, null,
							Constants.CALENDAR_QUARTER, processingYear, null);

			HashMap<String, Integer> yearCalendarMap = objCalendarDao
					.excelBasedCalendarList(conn, null,
							Constants.CALENDAR_YEAR, processingYear, null);

			HashMap<String, Integer> storeMap = objCompStoreDao
					.getCompStrId(conn);

			HashMap<String, Integer> districtList = objCompStoreDao
					.getDistrictList(conn);

			HashMap<String, Integer> regionList = objCompStoreDao
					.getRegionList(conn);

			HashMap<String, Integer> divisionList = objCompStoreDao
					.getDivisionList(conn);

			List<ProductDTO> productList = objProductGroupDao.getProductGroup(conn, 6);

			PreparedStatement psmt = conn.prepareStatement(InsertSql("Period"));
			PreparedStatement ctdPsmt = conn.prepareStatement(InsertSql("Ctd"));
			PreparedStatement dailyPsmt = conn.prepareStatement(InsertSql("Daily"));

			// insert process starts

			periodProcess(periodCalendarMap, objCalendarDao, storeMap,
					productList, psmt, dailyPsmt, ctdPsmt,
					Constants.CTD_PERIOD, Constants.CALENDAR_PERIOD, 5,
					"SALES_AGGR_DAILY", processingYear);

			psmt = conn.prepareStatement(InsertSql("Period"));
			ctdPsmt = conn.prepareStatement(InsertSql("Ctd"));
			dailyPsmt = conn.prepareStatement(InsertSql("Daily"));

			periodProcess(quarterCalendarMap, objCalendarDao, storeMap,
					productList, psmt, dailyPsmt, ctdPsmt,
					Constants.CTD_QUARTER, Constants.CALENDAR_QUARTER, 5,
					"SALES_AGGR_DAILY", processingYear);

			psmt = conn.prepareStatement(InsertSql("Period"));
			ctdPsmt = conn.prepareStatement(InsertSql("Ctd"));
			dailyPsmt = conn.prepareStatement(InsertSql("Daily"));

			periodProcess(yearCalendarMap, objCalendarDao, storeMap,
					productList, psmt, dailyPsmt, ctdPsmt, Constants.CTD_YEAR,
					Constants.CALENDAR_YEAR, 5, "SALES_AGGR_DAILY",
					processingYear);

			psmt = conn.prepareStatement(InsertSql("PeriodRollup"));
			ctdPsmt = conn.prepareStatement(InsertSql("Ctd"));
			dailyPsmt = conn.prepareStatement(InsertSql("DailyRollup"));

			periodProcess(periodCalendarMap, objCalendarDao, districtList,
					productList, psmt, dailyPsmt, ctdPsmt,
					Constants.CTD_PERIOD, Constants.CALENDAR_PERIOD, 4,
					"SALES_AGGR_DAILY_ROLLUP", processingYear);

			psmt = conn.prepareStatement(InsertSql("PeriodRollup"));
			ctdPsmt = conn.prepareStatement(InsertSql("Ctd"));
			dailyPsmt = conn.prepareStatement(InsertSql("DailyRollup"));

			periodProcess(quarterCalendarMap, objCalendarDao, districtList,
					productList, psmt, dailyPsmt, ctdPsmt,
					Constants.CTD_QUARTER, Constants.CALENDAR_QUARTER, 4,
					"SALES_AGGR_DAILY_ROLLUP", processingYear);

			psmt = conn.prepareStatement(InsertSql("PeriodRollup"));
			ctdPsmt = conn.prepareStatement(InsertSql("Ctd"));
			dailyPsmt = conn.prepareStatement(InsertSql("DailyRollup"));

			periodProcess(yearCalendarMap, objCalendarDao, districtList,
					productList, psmt, dailyPsmt, ctdPsmt, Constants.CTD_YEAR,
					Constants.CALENDAR_YEAR, 4, "SALES_AGGR_DAILY_ROLLUP",
					processingYear);

			// / for region

			psmt = conn.prepareStatement(InsertSql("PeriodRollup"));
			ctdPsmt = conn.prepareStatement(InsertSql("Ctd"));
			dailyPsmt = conn.prepareStatement(InsertSql("DailyRollup"));

			periodProcess(periodCalendarMap, objCalendarDao, regionList,
					productList, psmt, dailyPsmt, ctdPsmt,
					Constants.CTD_PERIOD, Constants.CALENDAR_PERIOD, 3,
					"SALES_AGGR_DAILY_ROLLUP", processingYear);

			psmt = conn.prepareStatement(InsertSql("PeriodRollup"));
			ctdPsmt = conn.prepareStatement(InsertSql("Ctd"));
			dailyPsmt = conn.prepareStatement(InsertSql("DailyRollup"));

			periodProcess(quarterCalendarMap, objCalendarDao, regionList,
					productList, psmt, dailyPsmt, ctdPsmt,
					Constants.CTD_QUARTER, Constants.CALENDAR_QUARTER, 3,
					"SALES_AGGR_DAILY_ROLLUP", processingYear);

			psmt = conn.prepareStatement(InsertSql("PeriodRollup"));
			ctdPsmt = conn.prepareStatement(InsertSql("Ctd"));
			dailyPsmt = conn.prepareStatement(InsertSql("DailyRollup"));

			periodProcess(yearCalendarMap, objCalendarDao, regionList,
					productList, psmt, dailyPsmt, ctdPsmt, Constants.CTD_YEAR,
					Constants.CALENDAR_YEAR, 3, "SALES_AGGR_DAILY_ROLLUP",
					processingYear);

			// Division List

			psmt = conn.prepareStatement(InsertSql("PeriodRollup"));
			ctdPsmt = conn.prepareStatement(InsertSql("Ctd"));
			dailyPsmt = conn.prepareStatement(InsertSql("DailyRollup"));

			periodProcess(periodCalendarMap, objCalendarDao, divisionList,
					productList, psmt, dailyPsmt, ctdPsmt,
					Constants.CTD_PERIOD, Constants.CALENDAR_PERIOD, 2,
					"SALES_AGGR_DAILY_ROLLUP", processingYear);

			psmt = conn.prepareStatement(InsertSql("PeriodRollup"));
			ctdPsmt = conn.prepareStatement(InsertSql("Ctd"));
			dailyPsmt = conn.prepareStatement(InsertSql("DailyRollup"));

			periodProcess(quarterCalendarMap, objCalendarDao, divisionList,
					productList, psmt, dailyPsmt, ctdPsmt,
					Constants.CTD_QUARTER, Constants.CALENDAR_QUARTER, 2,
					"SALES_AGGR_DAILY_ROLLUP", processingYear);

			psmt = conn.prepareStatement(InsertSql("PeriodRollup"));
			ctdPsmt = conn.prepareStatement(InsertSql("Ctd"));
			dailyPsmt = conn.prepareStatement(InsertSql("DailyRollup"));

			periodProcess(yearCalendarMap, objCalendarDao, divisionList,
					productList, psmt, dailyPsmt, ctdPsmt, Constants.CTD_YEAR,
					Constants.CALENDAR_YEAR, 2, "SALES_AGGR_DAILY_ROLLUP",
					processingYear);
			
			
			// Chain list
			
			HashMap<String, Integer> chainList = new HashMap<String, Integer>();
			
			chainList.put("50", 50);
					 

			psmt = conn.prepareStatement(InsertSql("PeriodRollup"));
			ctdPsmt = conn.prepareStatement(InsertSql("Ctd"));
			dailyPsmt = conn.prepareStatement(InsertSql("DailyRollup"));

			periodProcess(periodCalendarMap, objCalendarDao, chainList,
							productList, psmt, dailyPsmt, ctdPsmt,
							Constants.CTD_PERIOD, Constants.CALENDAR_PERIOD, 1,
							"SALES_AGGR_DAILY_ROLLUP", processingYear);

			psmt = conn.prepareStatement(InsertSql("PeriodRollup"));
			ctdPsmt = conn.prepareStatement(InsertSql("Ctd"));
			dailyPsmt = conn.prepareStatement(InsertSql("DailyRollup"));

			periodProcess(quarterCalendarMap, objCalendarDao, chainList,
						productList, psmt, dailyPsmt, ctdPsmt,
						Constants.CTD_QUARTER, Constants.CALENDAR_QUARTER, 1,
						"SALES_AGGR_DAILY_ROLLUP", processingYear);

			psmt = conn.prepareStatement(InsertSql("PeriodRollup"));
					ctdPsmt = conn.prepareStatement(InsertSql("Ctd"));
					dailyPsmt = conn.prepareStatement(InsertSql("DailyRollup"));

			periodProcess(yearCalendarMap, objCalendarDao, chainList,
							productList, psmt, dailyPsmt, ctdPsmt, Constants.CTD_YEAR,
							Constants.CALENDAR_YEAR, 1, "SALES_AGGR_DAILY_ROLLUP",
							processingYear);

		} catch (GeneralException e) {
			throw new GeneralException(" Accounts Loader Error.....", e);

		}

	}

	

	private void periodProcess(HashMap<String, Integer> periodCalendarMap,
			RetailCalendarDAO objCalendarDao,
			HashMap<String, Integer> storeMap, List<ProductDTO> productList,
			PreparedStatement psmt, PreparedStatement dailyPsmt,
			PreparedStatement ctdPsmt, int ctdType, String calendarCons,
			int locationLevel, String tableName, int processingYear) throws GeneralException,
 SQLException {

		Object[] calendarArray = periodCalendarMap.values().toArray();

		for (int cA = 0; cA < calendarArray.length; cA++) {

			int monthEndCalendarId = objCalendarDao
					.calendarIdBasedActualNumber(conn, processingYear, 0,
							calendarCons, calendarArray[cA]);
			logger.info(" Processing Calendar Id..." + calendarArray[cA]);

			logger.info(" Month End Calendar id..... " + monthEndCalendarId);

			Object[] storeArray = storeMap.values().toArray();

			for (int sA = 0; sA < storeArray.length; sA++) {

				for (int pL = 0; pL < productList.size(); pL++) {

					ProductDTO objProductDto = productList.get(pL);
					/*
					 * logger.info(" Processing Calendar Id......." +
					 * calendarArray[cA] + " Processing Store...." +
					 * storeArray[sA] +"..Product Id.." +
					 * objProductDto.getProductId());
					 */

					psmt.setObject(1, 0);
					psmt.setObject(2, storeArray[sA]);
					psmt.setObject(3, locationLevel);
					psmt.setObject(4, objProductDto.getProductId());
					psmt.setObject(5, Constants.FINANCEDEPARTMENT);
					psmt.setObject(6, calendarArray[cA]);
					psmt.setObject(7, monthEndCalendarId);
					psmt.addBatch();

					if (ctdType == 3) {
						dailyPsmt.setObject(1, 0);
						dailyPsmt.setObject(2, storeArray[sA]);
						dailyPsmt.setObject(3, locationLevel);
						dailyPsmt.setObject(4, objProductDto.getProductId());
						dailyPsmt.setObject(5, Constants.FINANCEDEPARTMENT);
						dailyPsmt.setObject(6, monthEndCalendarId);
						dailyPsmt.addBatch();
						dailyPsmt.executeBatch();
					}

					/*int summaryCtdId = getSummaryCtdId(monthEndCalendarId,
							objProductDto.getProductId(), 
							Constants.FINANCEDEPARTMENT, tableName,
							locationLevel, storeArray[sA]);

					
					if(ctdType == Constants.CTD_PERIOD  ) {					
						ctdPsmt.setObject(1, summaryCtdId);
						ctdPsmt.setObject(2, Constants.CTD_PERIOD);
						ctdPsmt.setObject(3, 0);
						ctdPsmt.addBatch();

						ctdPsmt.setObject(1, summaryCtdId);
						ctdPsmt.setObject(2, Constants.CTD_QUARTER);
						ctdPsmt.setObject(3, 0);
						ctdPsmt.addBatch();
						
						ctdPsmt.setObject(1, summaryCtdId);
						ctdPsmt.setObject(2, Constants.CTD_YEAR);
						ctdPsmt.setObject(3, 0);
						ctdPsmt.addBatch();
					}*/
				}
			}
		}

		int[] count = psmt.executeBatch();
		//ctdPsmt.executeBatch();
		logger.info(" Period Count......" + count.length);
		PristineDBUtil.commitTransaction(conn, "Process Commited");

	}

	private int getSummaryCtdId(int monthEndCalendarId, int productId, int i,
			String tableName, int locationLevel, Object storeArray) throws GeneralException {
		 
		StringBuffer sql = new StringBuffer();
		
		sql.append(" select summary_ctd_id from ").append(tableName);
		sql.append(" where calendar_id=").append(monthEndCalendarId);
		sql.append(" and  product_level_id=").append(i);
		sql.append(" and product_id=").append(productId);
		sql.append(" and location_level_id=").append(locationLevel);
		sql.append(" and location_id=").append(storeArray);
		
		//logger.info(sql.toString());

		String ctdId = PristineDBUtil.getSingleColumnVal(conn, sql, "aa");
				
		return Integer.parseInt(ctdId);
	}

	private String InsertSql(String mode) {

		StringBuffer sql = new StringBuffer();

		if( mode.equalsIgnoreCase("Period")){
				sql.append(" INSERT INTO SALES_AGGR (SALES_AGGR_ID,TOT_REVENUE,");
				sql.append(" LOCATION_ID,");
				sql.append(" LOCATION_LEVEL_ID, PRODUCT_ID ,  PRODUCT_LEVEL_ID , CALENDAR_ID,LAST_AGGR_CALENDARID)");
				sql.append(" values(SALES_AGGR_SEQ.NEXTVAL,?,?,?,?,?,?,?)");
		}
		else if( mode.equalsIgnoreCase("Ctd")){
			
			sql.append(" insert into sales_aggr_ctd ");
			sql.append(" (summary_ctd_id,ctd_type,tot_revenue)");
			sql.append(" values(?,?,?)");
		}
		
		else if( mode.equalsIgnoreCase("Daily")){
			
			sql.append(" INSERT INTO SALES_AGGR_DAILY (SUMMARY_DAILY_ID,TOT_REVENUE,");
			sql.append(" LOCATION_ID,");
			sql.append(" LOCATION_LEVEL_ID, PRODUCT_ID ,  PRODUCT_LEVEL_ID , CALENDAR_ID,SUMMARY_CTD_ID)");
			//sql.append(" values(SALES_AGGR_DAILY_SEQ.NEXTVAL,?,?,?,?,?,?,SALES_AGGR_CTD_SEQ.NEXTVAL)");
			sql.append(" values(SALES_AGGR_DAILY_SEQ.NEXTVAL,?,?,?,?,?,?,NULL)");
		}
		
		else if( mode.equalsIgnoreCase("PeriodRollup")){
			
			sql.append(" INSERT INTO SALES_AGGR_ROLLUP (SALES_AGGR_ROLLUP_ID,TOT_REVENUE,");
			sql.append(" LOCATION_ID,");
			sql.append(" LOCATION_LEVEL_ID, PRODUCT_ID ,  PRODUCT_LEVEL_ID , CALENDAR_ID,LAST_AGGR_CALENDARID)");
			sql.append(" values(SALES_AGGR_ROLLUP_SEQ.NEXTVAL,?,?,?,?,?,?,?)");
			
		}
		
		else if( mode.equalsIgnoreCase("DailyRollup")){
			
			sql.append(" INSERT INTO SALES_AGGR_DAILY_ROLLUP (SALES_AGGR_DAILY_ROLLUP_ID,TOT_REVENUE,");
			sql.append(" LOCATION_ID,");
			sql.append(" LOCATION_LEVEL_ID, PRODUCT_ID ,  PRODUCT_LEVEL_ID , CALENDAR_ID,SUMMARY_CTD_ID)");
			//sql.append(" values(SALES_AGGR_DAILY_ROLLUP_SEQ.NEXTVAL,?,?,?,?,?,?,SALES_AGGR_CTD_SEQ.NEXTVAL)");
			sql.append(" values(SALES_AGGR_DAILY_ROLLUP_SEQ.NEXTVAL,?,?,?,?,?,?,NULL)");
		}
		
		
		
		logger.debug(sql.toString());

		return sql.toString();
	}

 

}
