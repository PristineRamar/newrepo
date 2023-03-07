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
import com.pristine.dto.salesanalysis.BudgetDto;
import com.pristine.exception.GeneralException;
import com.pristine.util.Constants;
import com.pristine.util.PristineDBUtil;

public class AccountsDao {

	static Logger logger = Logger.getLogger("AccountDao");
	SimpleDateFormat formatter = new SimpleDateFormat("dd-MM-yyyy");


	/**
	 * @param _conn
	 * @param accountsDetails
	 * @param calendarMap
	 * @param storeMap
	 * @param locationLevelId
	 * @param modeOfInsert
	 * @param calendarId
	 * @param periodCtdMap
	 * @return
	 * @throws GeneralException
	 * @throws SQLException
	 */
	public boolean accountsInsert(Connection _conn,
			HashMap<String, BudgetDto> accountsDetails,
			HashMap<String, Integer> calendarMap,
			HashMap<String, Integer> storeMap, int locationLevelId,
			String modeOfInsert, int calendarId,
			HashMap<String, Integer> periodCtdMap) throws GeneralException,
			SQLException {

		PreparedStatement psmt = null;

		PreparedStatement ctdPsmt = null;

		try {
			// Intialize the Prepared statment.
			psmt = _conn.prepareStatement(insertQuery(modeOfInsert));
			ctdPsmt = _conn.prepareStatement(insertPeriodQuery("CTDINSERT"));

		} catch (SQLException sql) {
			logger.error(" Error in Prepared statment", sql);
			throw new GeneralException(" Error in Prepared statment....", sql);
		}

		// itreate the map
		Object[] accountsArray = accountsDetails.values().toArray();

		int count = 0;

		int[] countBatch;

		for (int acc = 0; acc < accountsArray.length; acc++) {

			BudgetDto objDto = (BudgetDto) accountsArray[acc];
			if (storeMap.containsKey(objDto.getLocationId().trim())) {

				Integer locationid = storeMap
						.get(objDto.getLocationId().trim());

				addSqlBatch(psmt, objDto, locationid, calendarMap,
						locationLevelId, modeOfInsert, periodCtdMap, ctdPsmt,
						_conn);

				if (count == 100) {
					countBatch = psmt.executeBatch();

					psmt.clearBatch();

					logger.info(" Insert Finance Department Count I...... "
							+ countBatch.length);
					/*
					countBatch = ctdPsmt.executeBatch();

					ctdPsmt.clearBatch();

					logger.info(" Insert Finance Department Count Ctd I...... "
							+ countBatch.length);*/

					count = 0;

				}

				count++;
			}
		}

		try {
			countBatch = psmt.executeBatch();

			logger.info(" Insert Finance Department Count ...... "
					+ countBatch.length);
			/*
			countBatch = ctdPsmt.executeBatch();

			logger.info(" Insert Finance Department Count Ctd...... "
					+ countBatch.length);*/

			PristineDBUtil.commitTransaction(_conn,
					" Finance Accounts Commited");

		} catch (Exception e) {
			logger.error(" ... Exceute Error...." + e);
			throw new GeneralException("... Exceute Error...." + e);

		}
		return true;
	}
	
	/**
	 * @param psmt
	 * @param objDto
	 * @param locationid
	 * @param calendarMap
	 * @param locationLevelId
	 * @param modeOfInsert
	 * @param periodCtdMap
	 * @param ctdPsmt
	 * @param _conn
	 * @throws GeneralException
	 */
	private void addSqlBatch(PreparedStatement psmt, BudgetDto objDto,
			Integer locationid, HashMap<String, Integer> calendarMap,
			int locationLevelId, String modeOfInsert,
			HashMap<String, Integer> periodCtdMap, PreparedStatement ctdPsmt,
			Connection _conn) throws GeneralException {

		try {

			// logger.debug(objDto.getProductId() +"-" +locationid +"-"
			// +locationLevelId+"-"+objDto.getProductLevelId()+"-"+calendarMap.get("PERIOD1"));
			psmt.setObject(1, objDto.getPeriod_01());
			psmt.setObject(2, locationid);
			psmt.setObject(3, locationLevelId);
			psmt.setObject(4, objDto.getProductId());
			psmt.setObject(5, objDto.getProductLevelId());
			psmt.setObject(6, calendarMap.get("PERIOD1"));
			psmt.addBatch();

			int monthEndCalendarId = getMonthEndCalendarId(_conn,
					calendarMap.get("PERIOD1"));

			ctdPsmt.setObject(1, objDto.getPeriod_01());
			ctdPsmt.setObject(
					2,
					periodCtdMap.get(monthEndCalendarId + "_" + locationid
							+ "_" + objDto.getProductId() + "_"
							+ objDto.getProductLevelId() + "_"
							+ locationLevelId));
			ctdPsmt.setObject(3, Constants.CTD_PERIOD);
			ctdPsmt.addBatch();

			// logger.debug(objDto.getProductId() +"-" +locationid +"-"
			// +locationLevelId+"-"+objDto.getProductLevelId()+"-"+calendarMap.get("PERIOD2"));
			// logger.debug("objDto.getPeriod_02()" + objDto.getPeriod_02());
			psmt.setObject(1, objDto.getPeriod_02());
			psmt.setObject(6, calendarMap.get("PERIOD2"));
			psmt.addBatch();

			monthEndCalendarId = getMonthEndCalendarId(_conn,
					calendarMap.get("PERIOD2"));

			ctdPsmt.setObject(1, objDto.getPeriod_02());
			ctdPsmt.setObject(
					2,
					periodCtdMap.get(monthEndCalendarId + "_" + locationid
							+ "_" + objDto.getProductId() + "_"
							+ objDto.getProductLevelId() + "_"
							+ locationLevelId));
			ctdPsmt.setObject(3, Constants.CTD_PERIOD);
			ctdPsmt.addBatch();

			// logger.debug(objDto.getProductId() +"-" +locationid +"-"
			// +locationLevelId+"-"+objDto.getProductLevelId()+"-"+calendarMap.get("PERIOD3"));
			// /logger.debug("objDto.getPeriod_03()" + objDto.getPeriod_03());
			psmt.setObject(1, objDto.getPeriod_03());
			psmt.setObject(6, calendarMap.get("PERIOD3"));
			psmt.addBatch();

			monthEndCalendarId = getMonthEndCalendarId(_conn,
					calendarMap.get("PERIOD3"));

			ctdPsmt.setObject(1, objDto.getPeriod_03());
			ctdPsmt.setObject(
					2,
					periodCtdMap.get(monthEndCalendarId + "_" + locationid
							+ "_" + objDto.getProductId() + "_"
							+ objDto.getProductLevelId() + "_"
							+ locationLevelId));
			ctdPsmt.setObject(3, Constants.CTD_PERIOD);
			ctdPsmt.addBatch();

			// logger.debug(objDto.getProductId() +"-" +locationid +"-"
			// +locationLevelId+"-"+objDto.getProductLevelId()+"-"+calendarMap.get("PERIOD4"));
			// logger.debug("objDto.getPeriod_04()" + objDto.getPeriod_04());
			psmt.setObject(1, objDto.getPeriod_04());
			psmt.setObject(6, calendarMap.get("PERIOD4"));
			psmt.addBatch();

			monthEndCalendarId = getMonthEndCalendarId(_conn,
					calendarMap.get("PERIOD4"));

			ctdPsmt.setObject(1, objDto.getPeriod_04());
			ctdPsmt.setObject(
					2,
					periodCtdMap.get(monthEndCalendarId + "_" + locationid
							+ "_" + objDto.getProductId() + "_"
							+ objDto.getProductLevelId() + "_"
							+ locationLevelId));
			ctdPsmt.setObject(3, Constants.CTD_PERIOD);
			ctdPsmt.addBatch();

			// logger.debug(objDto.getProductId() +"-" +locationid +"-"
			// +locationLevelId+"-"+objDto.getProductLevelId()+"-"+calendarMap.get("PERIOD5"));
			// logger.debug("objDto.getPeriod_05()" + objDto.getPeriod_05());
			psmt.setObject(1, objDto.getPeriod_05());
			psmt.setObject(6, calendarMap.get("PERIOD5"));
			psmt.addBatch();

			monthEndCalendarId = getMonthEndCalendarId(_conn,
					calendarMap.get("PERIOD5"));

			ctdPsmt.setObject(1, objDto.getPeriod_05());
			ctdPsmt.setObject(
					2,
					periodCtdMap.get(monthEndCalendarId + "_" + locationid
							+ "_" + objDto.getProductId() + "_"
							+ objDto.getProductLevelId() + "_"
							+ locationLevelId));
			ctdPsmt.setObject(3, Constants.CTD_PERIOD);
			ctdPsmt.addBatch();

			// logger.debug(objDto.getProductId() +"-" +locationid +"-"
			// +locationLevelId+"-"+objDto.getProductLevelId()+"-"+calendarMap.get("PERIOD6"));
			// logger.debug("objDto.getPeriod_06()" + objDto.getPeriod_06());
			psmt.setObject(1, objDto.getPeriod_06());
			psmt.setObject(6, calendarMap.get("PERIOD6"));
			psmt.addBatch();

			monthEndCalendarId = getMonthEndCalendarId(_conn,
					calendarMap.get("PERIOD6"));

			ctdPsmt.setObject(1, objDto.getPeriod_06());
			ctdPsmt.setObject(
					2,
					periodCtdMap.get(monthEndCalendarId + "_" + locationid
							+ "_" + objDto.getProductId() + "_"
							+ objDto.getProductLevelId() + "_"
							+ locationLevelId));
			ctdPsmt.setObject(3, Constants.CTD_PERIOD);
			ctdPsmt.addBatch();

			// logger.debug(objDto.getProductId() +"-" +locationid +"-"
			// +locationLevelId+"-"+objDto.getProductLevelId()+"-"+calendarMap.get("PERIOD7"));
			// logger.debug("objDto.getPeriod_07()" + objDto.getPeriod_07());
			psmt.setObject(1, objDto.getPeriod_07());
			psmt.setObject(6, calendarMap.get("PERIOD7"));
			psmt.addBatch();

			monthEndCalendarId = getMonthEndCalendarId(_conn,
					calendarMap.get("PERIOD7"));

			ctdPsmt.setObject(1, objDto.getPeriod_07());
			ctdPsmt.setObject(
					2,
					periodCtdMap.get(monthEndCalendarId + "_" + locationid
							+ "_" + objDto.getProductId() + "_"
							+ objDto.getProductLevelId() + "_"
							+ locationLevelId));
			ctdPsmt.setObject(3, Constants.CTD_PERIOD);
			ctdPsmt.addBatch();

			// logger.debug(objDto.getProductId() +"-" +locationid +"-"
			// +locationLevelId+"-"+objDto.getProductLevelId()+"-"+calendarMap.get("PERIOD8"));
			// logger.debug("objDto.getPeriod_08()" + objDto.getPeriod_08());
			psmt.setObject(1, objDto.getPeriod_08());
			psmt.setObject(6, calendarMap.get("PERIOD8"));
			psmt.addBatch();

			monthEndCalendarId = getMonthEndCalendarId(_conn,
					calendarMap.get("PERIOD8"));

			ctdPsmt.setObject(1, objDto.getPeriod_08());
			ctdPsmt.setObject(
					2,
					periodCtdMap.get(monthEndCalendarId + "_" + locationid
							+ "_" + objDto.getProductId() + "_"
							+ objDto.getProductLevelId() + "_"
							+ locationLevelId));
			ctdPsmt.setObject(3, Constants.CTD_PERIOD);
			ctdPsmt.addBatch();

			// logger.debug(objDto.getProductId() +"-" +locationid +"-"
			// +locationLevelId+"-"+objDto.getProductLevelId()+"-"+calendarMap.get("PERIOD9"));
			// logger.debug("objDto.getPeriod_09()" + objDto.getPeriod_09());
			psmt.setObject(1, objDto.getPeriod_09());
			psmt.setObject(6, calendarMap.get("PERIOD9"));
			psmt.addBatch();

			monthEndCalendarId = getMonthEndCalendarId(_conn,
					calendarMap.get("PERIOD9"));

			ctdPsmt.setObject(1, objDto.getPeriod_09());
			ctdPsmt.setObject(
					2,
					periodCtdMap.get(monthEndCalendarId + "_" + locationid
							+ "_" + objDto.getProductId() + "_"
							+ objDto.getProductLevelId() + "_"
							+ locationLevelId));
			ctdPsmt.setObject(3, Constants.CTD_PERIOD);
			ctdPsmt.addBatch();

			// logger.debug(objDto.getProductId() +"-" +locationid +"-"
			// +locationLevelId+"-"+objDto.getProductLevelId()+"-"+calendarMap.get("PERIOD10"));
			// logger.debug("objDto.getPeriod_010()" + objDto.getPeriod_10());
			psmt.setObject(1, objDto.getPeriod_10());
			psmt.setObject(6, calendarMap.get("PERIOD10"));
			psmt.addBatch();

			monthEndCalendarId = getMonthEndCalendarId(_conn,
					calendarMap.get("PERIOD10"));

			ctdPsmt.setObject(1, objDto.getPeriod_10());
			ctdPsmt.setObject(
					2,
					periodCtdMap.get(monthEndCalendarId + "_" + locationid
							+ "_" + objDto.getProductId() + "_"
							+ objDto.getProductLevelId() + "_"
							+ locationLevelId));
			ctdPsmt.setObject(3, Constants.CTD_PERIOD);
			ctdPsmt.addBatch();

			// logger.debug(objDto.getProductId() +"-" +locationid +"-"
			// +locationLevelId+"-"+objDto.getProductLevelId()+"-"+calendarMap.get("PERIOD11"));
			psmt.setObject(1, objDto.getPeriod_11());
			psmt.setObject(6, calendarMap.get("PERIOD11"));
			psmt.addBatch();

			monthEndCalendarId = getMonthEndCalendarId(_conn,
					calendarMap.get("PERIOD11"));

			ctdPsmt.setObject(1, objDto.getPeriod_11());
			ctdPsmt.setObject(
					2,
					periodCtdMap.get(monthEndCalendarId + "_" + locationid
							+ "_" + objDto.getProductId() + "_"
							+ objDto.getProductLevelId() + "_"
							+ locationLevelId));
			ctdPsmt.setObject(3, Constants.CTD_PERIOD);
			ctdPsmt.addBatch();

			// logger.debug(objDto.getProductId() +"-" +locationid +"-"
			// +locationLevelId+"-"+objDto.getProductLevelId()+"-"+calendarMap.get("PERIOD12"));
			psmt.setObject(1, objDto.getPeriod_12());
			psmt.setObject(6, calendarMap.get("PERIOD12"));
			psmt.addBatch();

			monthEndCalendarId = getMonthEndCalendarId(_conn,
					calendarMap.get("PERIOD12"));

			ctdPsmt.setObject(1, objDto.getPeriod_12());
			ctdPsmt.setObject(
					2,
					periodCtdMap.get(monthEndCalendarId + "_" + locationid
							+ "_" + objDto.getProductId() + "_"
							+ objDto.getProductLevelId() + "_"
							+ locationLevelId));
			ctdPsmt.setObject(3, Constants.CTD_PERIOD);
			ctdPsmt.addBatch();

			// logger.debug(objDto.getProductId() +"-" +locationid +"-"
			// +locationLevelId+"-"+objDto.getProductLevelId()+"-"+calendarMap.get("PERIOD13"));
			psmt.setObject(1, objDto.getPeriod_13());
			psmt.setObject(6, calendarMap.get("PERIOD13"));
			psmt.addBatch();

			monthEndCalendarId = getMonthEndCalendarId(_conn,
					calendarMap.get("PERIOD13"));

			ctdPsmt.setObject(1, objDto.getPeriod_13());
			ctdPsmt.setObject(
					2,
					periodCtdMap.get(monthEndCalendarId + "_" + locationid
							+ "_" + objDto.getProductId() + "_"
							+ objDto.getProductLevelId() + "_"
							+ locationLevelId));
			ctdPsmt.setObject(3, Constants.CTD_PERIOD);
			ctdPsmt.addBatch();

		} catch (SQLException sql) {
			logger.error("Location Id ...." + locationid + "...Product Id...."
					+ objDto.getProductLevelId() + "....Store No ..."
					+ objDto.getLocationId() + "....." + sql);
		}

	}

	private int getMonthEndCalendarId(Connection _conn, Integer calendarId)
			throws GeneralException {

		int reVal = 0;

		StringBuffer sql = new StringBuffer();

		sql.append(" select calendar_id from retail_calendar where start_date in(");
		sql.append(" select end_date from retail_calendar where calendar_id=")
				.append(calendarId).append(")");
		sql.append(" and row_type='D'");

		String value = PristineDBUtil.getSingleColumnVal(_conn, sql,
				"getMonthEndCalendarId");

		if (value != null) {
			reVal = Integer.parseInt(value);
		}

		return reVal;
	}

	/*
	 * Sql query for budget
	 */
	private String insertQuery(String modeOfInsert) {

		StringBuffer sql = new StringBuffer();

		sql.append(" update  SALES_AGGR set ADJ_TOT_REVENUE = ?");
		sql.append(" where LOCATION_ID= ?");
		sql.append(" and LOCATION_LEVEL_ID = ? and PRODUCT_ID=? and PRODUCT_LEVEL_ID=? and CALENDAR_ID=?");

		return sql.toString();
	}

	
	/**
	 * @param _conn
	 * @param processYear
	 * @param startdate
	 * @param endDate
	 * @param calendarRowType
	 * @param methodName
	 * @return
	 * @throws GeneralException
	 */
	public HashMap<String, BudgetDto> getAccountsDetails(Connection _conn,
			int processYear, Date startdate, Date endDate,
			String calendarRowType, String methodName) throws GeneralException {

		HashMap<String, BudgetDto> returnMap = new HashMap<String, BudgetDto>();
		try {
			StringBuffer sql = new StringBuffer();

			sql.append(" select SA.LOCATION_ID, sum(ADJ_TOT_REVENUE) as TOT_REVENUE");

			if (methodName.equalsIgnoreCase("QUARTER"))
				sql.append(" ,SA.PRODUCT_ID,SA.PRODUCT_LEVEL_ID");
			else if (methodName.equalsIgnoreCase("YEAR"))
				sql.append(" ,SA.PRODUCT_ID,SA.PRODUCT_LEVEL_ID");
			else if (methodName.equalsIgnoreCase("PERIOD"))
				sql.append(" ,RC.CALENDAR_ID");

			sql.append(" from SALES_AGGR SA");
			sql.append(" inner join RETAIL_CALENDAR RC on RC.CALENDAR_ID = SA.CALENDAR_ID");
			sql.append(" where RC.CAL_YEAR =").append(processYear);
			sql.append(" and RC.ROW_TYPE='").append(calendarRowType)
					.append("'");

			if (methodName.equalsIgnoreCase("PERIOD")) {
				sql.append(" and SA.PRODUCT_ID NOT IN(");
				sql.append(" select product_id from product_group");
				sql.append(" where add_on_type='GAS')");
				sql.append(" and SA.PRODUCT_LEVEL_ID=").append(
						Constants.FINANCEDEPARTMENT);
			} else if (methodName.equalsIgnoreCase("QUARTER")) {
				sql.append(" and RC.CALENDAR_ID in(");
				sql.append(" select CALENDAR_ID from RETAIL_CALENDAR where");
				sql.append(" CAL_YEAR =").append(processYear);
				sql.append(" and START_DATE between to_date('")
						.append(formatter.format(startdate))
						.append("','dd-MM-yyyy')");
				sql.append(" and to_date('").append(formatter.format(endDate))
						.append("','dd-MM-yyyy')");
				sql.append(" and row_type='").append(calendarRowType)
						.append("'");
				sql.append(" )");
			}
			sql.append(" group by SA.LOCATION_ID");

			if (methodName.equalsIgnoreCase("QUARTER"))
				sql.append(" ,SA.PRODUCT_ID,SA.PRODUCT_LEVEL_ID");
			else if (methodName.equalsIgnoreCase("PERIOD"))
				sql.append(",RC.CALENDAR_ID");
			if (methodName.equalsIgnoreCase("YEAR"))
				sql.append(" ,SA.PRODUCT_ID,SA.PRODUCT_LEVEL_ID");
			sql.append(" order by SA.LOCATION_ID");
			if (methodName.equalsIgnoreCase("QUARTER"))
				sql.append(" ,SA.PRODUCT_ID,SA.PRODUCT_LEVEL_ID");
			else if (methodName.equalsIgnoreCase("PERIOD"))
				sql.append(",RC.CALENDAR_ID");
			else if (methodName.equalsIgnoreCase("YEAR"))
				sql.append(" ,SA.PRODUCT_ID,SA.PRODUCT_LEVEL_ID");
			logger.debug(methodName + "...." + sql.toString());

			try {

				CachedRowSet rest = PristineDBUtil.executeQuery(_conn, sql,
						"getAccountsDetails");

				while (rest.next()) {
					BudgetDto objbudgetDto = new BudgetDto();

					objbudgetDto.setLocationId(rest.getString("LOCATION_ID"));
					objbudgetDto.setStoreBudget(rest.getDouble("TOT_REVENUE"));

					if (methodName.equalsIgnoreCase("QUARTER")) {

						objbudgetDto.setProcessproductId(rest
								.getInt("PRODUCT_ID"));
						objbudgetDto.setProcessproductLevelId(rest
								.getInt("PRODUCT_LEVEL_ID"));

						returnMap.put(objbudgetDto.getLocationId() + "_"
								+ objbudgetDto.getProcessproductId() + "_"
								+ objbudgetDto.getProcessproductLevelId(),
								objbudgetDto);

					} else if (methodName.equalsIgnoreCase("PERIOD")) {
						objbudgetDto.setCalendarId(rest.getInt("CALENDAR_ID"));
						returnMap.put(objbudgetDto.getLocationId() + "_"
								+ objbudgetDto.getCalendarId(), objbudgetDto);
					}

					else if (methodName.equalsIgnoreCase("YEAR")) {
						objbudgetDto.setProcessproductId(rest
								.getInt("PRODUCT_ID"));
						objbudgetDto.setProcessproductLevelId(rest
								.getInt("PRODUCT_LEVEL_ID"));
						returnMap.put(objbudgetDto.getLocationId() + "_"
								+ objbudgetDto.getProcessproductId() + "_"
								+ objbudgetDto.getProcessproductLevelId(),
								objbudgetDto);

					}

				}

			} catch (SQLException exe) {
				logger.error("Error While Fetching Accounts Details...... "
						+ exe);
				throw new GeneralException("getAccountsDetails.....", exe);
			}
		} catch (Exception exe) {
			logger.error("Error While Fetching Accounts Details...... " + exe);
			throw new GeneralException("getAccountsDetails.....", exe);
		}
		return returnMap;
	}


	/**
	 * @param _conn
	 * @param _processYear
	 * @param startdate
	 * @param endDate
	 * @param calendarLevel
	 * @param locationLevelId
	 * @param methodName
	 * @return
	 * @throws GeneralException
	 */
	public HashMap<String, BudgetDto> getdistrictPeriodAccounts(
			Connection _conn, int _processYear, Date startdate, Date endDate,
			String calendarLevel, int locationLevelId, String methodName)
			throws GeneralException {

		HashMap<String, BudgetDto> returnMap = new HashMap<String, BudgetDto>();

		try {
			StringBuffer sql = new StringBuffer();

			sql.append(" select sum(ADJ_TOT_REVENUE) as TOT_REVENUE,DISTRICT_ID");
			sql.append(" ,A.PRODUCT_ID,A.PRODUCT_LEVEL_ID");

			if (methodName.equalsIgnoreCase("PERIOD"))
				sql.append(",A.CALENDAR_ID");

			sql.append(" from SALES_AGGR A");
			sql.append(" ,COMPETITOR_STORE b , RETAIL_CALENDAR c ");
			sql.append(" where B.DISTRICT_ID in (select ID from RETAIL_DISTRICT)");
			sql.append(" and A.LOCATION_ID  = B.COMP_STR_ID AND A.CALENDAR_ID = C.CALENDAR_ID ");
			sql.append(" AND C.CAL_YEAR=" + _processYear + " AND C.ROW_TYPE='"
					+ calendarLevel + "'");
			sql.append(" and A.LOCATION_LEVEL_ID=" + locationLevelId + "");
			sql.append(" and ADJ_TOT_REVENUE is not null");

			if (methodName.equalsIgnoreCase("QUARTER")) {
				sql.append(" and A.CALENDAR_ID in(");
				sql.append(" select CALENDAR_ID from RETAIL_CALENDAR where");
				sql.append(" CAL_YEAR =").append(_processYear);
				sql.append(" and START_DATE between to_date('")
						.append(formatter.format(startdate))
						.append("','dd-MM-yyyy')");
				sql.append(" and to_date('").append(formatter.format(endDate))
						.append("','dd-MM-yyyy')");
				sql.append(" and row_type='").append(calendarLevel).append("'");
				sql.append(" )");
			}

			sql.append(" group by DISTRICT_ID,A.PRODUCT_ID,A.PRODUCT_LEVEL_ID");
			if (methodName.equalsIgnoreCase("PERIOD"))
				sql.append(" ,A.CALENDAR_ID");
			sql.append(" order by DISTRICT_ID,A.PRODUCT_ID,A.PRODUCT_LEVEL_ID");
			if (methodName.equalsIgnoreCase("PERIOD"))
				sql.append(" ,A.CALENDAR_ID");

			logger.debug(methodName + "...." + sql.toString());

			try {

				// execute the query.
				CachedRowSet result = PristineDBUtil.executeQuery(_conn, sql,
						"getdistrictPeriodAccounts");
				while (result.next()) {
					BudgetDto objBudget = new BudgetDto();
					objBudget.setLocationId(result.getString("DISTRICT_ID"));
					objBudget.setStoreBudget(result.getDouble("TOT_REVENUE"));
					objBudget.setProcessproductId(result.getInt("PRODUCT_ID"));
					objBudget.setProcessproductLevelId(result
							.getInt("PRODUCT_LEVEL_ID"));
					if (methodName.equalsIgnoreCase("PERIOD")) {
						objBudget.setCalendarId(result.getInt("CALENDAR_ID"));
						returnMap.put(
								objBudget.getLocationId() + "_"
										+ objBudget.getCalendarId() + "_"
										+ objBudget.getProcessproductId() + "_"
										+ objBudget.getProcessproductLevelId(),
								objBudget);
					} else if (methodName.equalsIgnoreCase("QUARTER")
							|| methodName.equalsIgnoreCase("YEAR")
							|| methodName.equalsIgnoreCase("QUARTERIDENTICAL")) {

						returnMap.put(objBudget.getLocationId() + "_"
								+ objBudget.getProcessproductId() + "_"
								+ objBudget.getProcessproductLevelId(),
								objBudget);
					}

				}

			} catch (SQLException exe) {
				logger.error(exe);
				throw new GeneralException("getdistrictPeriodAccounts error",
						exe);
			}
		} catch (Exception exe) {
			logger.error(exe);
			throw new GeneralException("getdistrictPeriodAccounts error....",
					exe);
		}

		return returnMap;
	}

	/**
	 * @param _conn
	 * @param _processYear
	 * @param startDate
	 * @param endDate
	 * @param calendarPeriod
	 * @param locationLevelId
	 * @param methodName
	 * @return
	 * @throws GeneralException
	 */
	public HashMap<String, BudgetDto> getregionPeriodAccounts(Connection _conn,
			int _processYear, Date startDate, Date endDate,
			String calendarPeriod, int locationLevelId, String methodName)
			throws GeneralException {

		HashMap<String, BudgetDto> returnMap = new HashMap<String, BudgetDto>();
		try {
			SimpleDateFormat formatter = new SimpleDateFormat("dd-MM-yyyy");
			StringBuffer sql = new StringBuffer();
			sql.append(" select sum(ADJ_TOT_REVENUE) as TOT_REVENUE, REGION_ID");
			sql.append(" ,sum(ADJ_ID_TOT_REVENUE) AS ID_TOT_REVENUE");
			sql.append(" ,A.PRODUCT_ID,A.PRODUCT_LEVEL_ID");
			if (methodName.equalsIgnoreCase("PERIOD"))
				sql.append(" ,A.CALENDAR_ID");

			sql.append(" from SALES_AGGR_ROLLUP A , RETAIL_DISTRICT b, RETAIL_CALENDAR c  ");
			sql.append("  where B.ID = A.LOCATION_ID AND LOCATION_LEVEL_ID=")
					.append(locationLevelId);
			sql.append("  AND A.CALENDAR_ID = C.CALENDAR_ID ");
			sql.append(" AND C.CAL_YEAR=").append(_processYear);
			sql.append(" AND C.ROW_TYPE='").append(calendarPeriod).append("'");
			sql.append(" and A.LOCATION_LEVEL_ID=").append(locationLevelId);
			sql.append(" and ADJ_TOT_REVENUE is not null");

			if (methodName.equalsIgnoreCase("QUARTER")) {
				sql.append(" and A.CALENDAR_ID in(");
				sql.append(" select CALENDAR_ID from RETAIL_CALENDAR where");
				sql.append(" CAL_YEAR =").append(_processYear);
				sql.append(" and START_DATE between to_date('")
						.append(formatter.format(startDate))
						.append("','dd-MM-yyyy')");
				sql.append(" and to_date('").append(formatter.format(endDate))
						.append("','dd-MM-yyyy')");
				sql.append(" and row_type='").append(calendarPeriod)
						.append("'");
				sql.append(" )");
			}
			sql.append(" group by REGION_ID,A.PRODUCT_ID,A.PRODUCT_LEVEL_ID");
			if (methodName.equalsIgnoreCase("PERIOD"))
				sql.append(" ,A.CALENDAR_ID");
			sql.append(" order by REGION_ID,A.PRODUCT_ID,A.PRODUCT_LEVEL_ID");
			if (methodName.equalsIgnoreCase("PERIOD"))
				sql.append(" ,A.CALENDAR_ID");

			logger.debug(methodName + "...." + sql.toString());

			try {
				CachedRowSet result = PristineDBUtil.executeQuery(_conn, sql,
						"getregionForecaste");
				while (result.next()) {

					BudgetDto objBudget = new BudgetDto();
					objBudget.setLocationId(result.getString("REGION_ID"));
					objBudget.setStoreBudget(result.getDouble("TOT_REVENUE"));
					objBudget.setProcessproductId(result.getInt("PRODUCT_ID"));

					objBudget.setProcessproductLevelId(result
							.getInt("PRODUCT_LEVEL_ID"));
					objBudget.setIdenticalBudget(result
							.getDouble("ID_TOT_REVENUE"));

					if (methodName.equalsIgnoreCase("PERIOD")) {
						objBudget.setCalendarId(result.getInt("CALENDAR_ID"));
						returnMap.put(
								objBudget.getLocationId() + "_"
										+ objBudget.getCalendarId() + "_"
										+ objBudget.getProcessproductId() + "_"
										+ objBudget.getProcessproductLevelId(),
								objBudget);
					} else if (methodName.equalsIgnoreCase("QUARTER")
							|| methodName.equalsIgnoreCase("YEAR")) {
						returnMap.put(objBudget.getLocationId() + "_"
								+ objBudget.getProcessproductId() + "_"
								+ objBudget.getProcessproductLevelId(),
								objBudget);
					}

				}

			} catch (SQLException exe) {
				logger.error(exe);
				throw new GeneralException(
						"Fetch Region Level Accounts Error.....", exe);
			}
		} catch (Exception exe) {
			logger.error(exe);
			throw new GeneralException(
					"Fetch Region Level Accounts Error.....", exe);
		}

		return returnMap;
	}

	/**
	 * @param _conn
	 * @param _processYear
	 * @param startDate
	 * @param endDate
	 * @param calendarPeriod
	 * @param locationLevelId
	 * @param methodName
	 * @return
	 * @throws GeneralException
	 */
	public HashMap<String, BudgetDto> getDivisionPeriodAccounts(
			Connection _conn, int _processYear, Date startDate, Date endDate,
			String calendarPeriod, int locationLevelId, String methodName)
			throws GeneralException {

		HashMap<String, BudgetDto> returnMap = new HashMap<String, BudgetDto>();
		try {
			SimpleDateFormat formatter = new SimpleDateFormat("dd-MM-yyyy");
			StringBuffer sql = new StringBuffer();
			sql.append(" select sum(ADJ_TOT_REVENUE) as TOT_REVENUE, DIVISION_ID");
			sql.append(" ,sum(ADJ_ID_TOT_REVENUE) AS ID_TOT_REVENUE");
			sql.append(" ,A.PRODUCT_ID,A.PRODUCT_LEVEL_ID");
			if (methodName.equalsIgnoreCase("PERIOD"))
				sql.append(" ,A.CALENDAR_ID");

			sql.append(" from SALES_AGGR_ROLLUP A , RETAIL_REGION b, RETAIL_CALENDAR c  ");
			sql.append("  where B.ID = A.LOCATION_ID AND LOCATION_LEVEL_ID=")
					.append(locationLevelId);
			sql.append("  AND A.CALENDAR_ID = C.CALENDAR_ID ");
			sql.append(" AND C.CAL_YEAR=").append(_processYear);
			sql.append(" AND C.ROW_TYPE='").append(calendarPeriod).append("'");
			sql.append(" and A.LOCATION_LEVEL_ID=").append(locationLevelId);
			sql.append(" and ADJ_TOT_REVENUE is not null");

			if (methodName.equalsIgnoreCase("QUARTER")) {
				sql.append(" and A.CALENDAR_ID in(");
				sql.append(" select CALENDAR_ID from RETAIL_CALENDAR where");
				sql.append(" CAL_YEAR =").append(_processYear);
				sql.append(" and START_DATE between to_date('")
						.append(formatter.format(startDate))
						.append("','dd-MM-yyyy')");
				sql.append(" and to_date('").append(formatter.format(endDate))
						.append("','dd-MM-yyyy')");
				sql.append(" and row_type='").append(calendarPeriod)
						.append("'");
				sql.append(" )");
			}
			sql.append(" group by DIVISION_ID,A.PRODUCT_ID,A.PRODUCT_LEVEL_ID");
			if (methodName.equalsIgnoreCase("PERIOD"))
				sql.append(" ,A.CALENDAR_ID");
			sql.append(" order by DIVISION_ID,A.PRODUCT_ID,A.PRODUCT_LEVEL_ID");
			if (methodName.equalsIgnoreCase("PERIOD"))
				sql.append(" ,A.CALENDAR_ID");

			logger.debug(methodName + "...." + sql.toString());

			try {
				CachedRowSet result = PristineDBUtil.executeQuery(_conn, sql,
						"getregionForecaste");
				while (result.next()) {

					BudgetDto objBudget = new BudgetDto();
					objBudget.setLocationId(result.getString("DIVISION_ID"));
					objBudget.setStoreBudget(result.getDouble("TOT_REVENUE"));
					objBudget.setProcessproductId(result.getInt("PRODUCT_ID"));
					objBudget.setProcessproductLevelId(result
							.getInt("PRODUCT_LEVEL_ID"));
					objBudget.setIdenticalBudget(result
							.getDouble("ID_TOT_REVENUE"));

					if (methodName.equalsIgnoreCase("PERIOD")) {
						objBudget.setCalendarId(result.getInt("CALENDAR_ID"));
						returnMap.put(
								objBudget.getLocationId() + "_"
										+ objBudget.getCalendarId() + "_"
										+ objBudget.getProcessproductId() + "_"
										+ objBudget.getProcessproductLevelId(),
								objBudget);
					} else if (methodName.equalsIgnoreCase("QUARTER")
							|| methodName.equalsIgnoreCase("YEAR")) {
						returnMap.put(objBudget.getLocationId() + "_"
								+ objBudget.getProcessproductId() + "_"
								+ objBudget.getProcessproductLevelId(),
								objBudget);
					}

				}

			} catch (Exception exe) {
				logger.error(exe);
				throw new GeneralException("Get Divison Level Accounts Error",
						exe);
			}
		} catch (Exception exe) {
			logger.error(exe);
			throw new GeneralException("Get Divison Level Accounts Error", exe);
		}

		return returnMap;

	}


	/**
	 * @param _conn
	 * @param _processYear
	 * @param calendarLevel
	 * @param locationLevelId
	 * @return
	 * @throws GeneralException
	 */
	public HashMap<String, BudgetDto> getIdenticalDetails(Connection _conn,
			int _processYear, String calendarLevel, int locationLevelId)
			throws GeneralException {

		HashMap<String, BudgetDto> returnMap = new HashMap<String, BudgetDto>();

		try {
			StringBuffer sql = new StringBuffer();
			sql.append(" select sum(ADJ_TOT_REVENUE) as TOT_REVENUE, DISTRICT_ID");
			sql.append(" ,A.PRODUCT_ID,A.PRODUCT_LEVEL_ID");
			sql.append(",A.CALENDAR_ID");
			sql.append(" from SALES_AGGR A , COMPETITOR_STORE b , RETAIL_CALENDAR c ");
			sql.append(" where B.DISTRICT_ID in (select ID from RETAIL_DISTRICT)");
			sql.append(" and A.LOCATION_ID  = B.COMP_STR_ID AND A.CALENDAR_ID = C.CALENDAR_ID ");
			sql.append(" AND C.CAL_YEAR=" + _processYear + " AND C.ROW_TYPE='"
					+ calendarLevel + "'");
			sql.append(" and A.LOCATION_LEVEL_ID=" + locationLevelId + "");
			sql.append(" and B.OPEN_DATE < C.START_DATE-364");
			sql.append(" and ADJ_TOT_REVENUE is not null");
			sql.append(" group by DISTRICT_ID,A.PRODUCT_ID,A.PRODUCT_LEVEL_ID");
			sql.append(" ,A.CALENDAR_ID");
			sql.append(" order by DISTRICT_ID,A.PRODUCT_ID,A.PRODUCT_LEVEL_ID");
			sql.append(" ,A.CALENDAR_ID");

			logger.debug(" getIdenticalDetails ..............."
					+ sql.toString());

			try {

				// execute the query.
				CachedRowSet result = PristineDBUtil.executeQuery(_conn, sql,
						"getdistrictPeriodAccounts");
				while (result.next()) {
					BudgetDto objBudget = new BudgetDto();
					objBudget.setLocationId(result.getString("DISTRICT_ID"));
					objBudget.setIdenticalBudget(result
							.getDouble("TOT_REVENUE"));
					objBudget.setProcessproductId(result.getInt("PRODUCT_ID"));
					objBudget.setProcessproductLevelId(result
							.getInt("PRODUCT_LEVEL_ID"));
					objBudget.setCalendarId(result.getInt("CALENDAR_ID"));

					returnMap.put(
							objBudget.getLocationId() + "_"
									+ objBudget.getCalendarId() + "_"
									+ objBudget.getProcessproductId() + "_"
									+ objBudget.getProcessproductLevelId(),
							objBudget);

				}

			} catch (SQLException exe) {
				logger.error(exe);
				throw new GeneralException("getdistrictPeriodAccounts error",
						exe);
			}
		} catch (Exception exe) {
			logger.error(exe);
			throw new GeneralException("getdistrictPeriodAccounts error....",
					exe);
		}

		return returnMap;
	}

	/**
	 * @param _conn
	 * @param periodAccounts
	 * @param storeLevelId
	 * @param periodCtdMap
	 * @return
	 * @throws GeneralException
	 */
	public boolean periodInsert(Connection _conn,
			HashMap<String, BudgetDto> periodAccounts, int storeLevelId,
			HashMap<String, Integer> periodCtdMap) throws GeneralException {

		PreparedStatement psmt = null;
		PreparedStatement cpsmt = null;
		try {
			// Intialize the Prepared statment.
			psmt = _conn.prepareStatement(insertPeriodQuery("SALESINSERT"));
			cpsmt = _conn.prepareStatement(insertPeriodQuery("CTDINSERT"));
		} catch (SQLException sql) {
			logger.error(" Error in Prepared statment", sql);
			throw new GeneralException(" Error in Prepared statment....", sql);
		}

		// itreate the map
		Object[] accountsArray = periodAccounts.values().toArray();

		for (int acc = 0; acc < accountsArray.length; acc++) {

			BudgetDto objDto = (BudgetDto) accountsArray[acc];
			// for finance department
			// logger.debug("Location Id " +
			// objDto.getLocationId()+"....calendar id" +
			// objDto.getCalendarId());
			int monthEndCalendarId = getMonthEndCalendarId(_conn,
					objDto.getCalendarId());
			addStoreSqlBatch(psmt, objDto, periodCtdMap, cpsmt,
					monthEndCalendarId);
		}

		try {
			psmt.executeBatch();
			//cpsmt.executeBatch();
		} catch (Exception e) {
			logger.error(" ... Exceute Error...." + e);
			e.printStackTrace();
			throw new GeneralException("... Exceute Error...." + e);

		}
		return true;
	}
	
 

	/**
	 * @param psmt
	 * @param BudgetDto
	 * @param periodCtdMap
	 * @param cpsmt
	 * @param monthEndCalendarId
	 * @throws GeneralException
	 */
	private void addStoreSqlBatch(PreparedStatement psmt, BudgetDto BudgetDto,
			HashMap<String, Integer> periodCtdMap, PreparedStatement cpsmt,
			int monthEndCalendarId) throws GeneralException {

		try {
			psmt.setObject(1, BudgetDto.getStoreBudget());
			psmt.setObject(2, BudgetDto.getLocationId());
			psmt.setObject(3, Constants.STORE_LEVEL_ID);
			psmt.setObject(4, BudgetDto.getCalendarId());
			psmt.addBatch();

			// logger.error("--------------------" +
			// BudgetDto.getLocationId().trim() + "..... "
			// +summaryCtdMap.get(BudgetDto.getLocationId().trim()) );

			if (periodCtdMap.containsKey(monthEndCalendarId + "_"
					+ BudgetDto.getLocationId() + "_"
					+ BudgetDto.getProcessproductLevelId() + "_"
					+ BudgetDto.getProcessproductLevelId() + "_"
					+ Constants.STORE_LEVEL_ID))

			{
				cpsmt.setObject(1, BudgetDto.getStoreBudget());
				cpsmt.setObject(
						2,
						periodCtdMap.get(monthEndCalendarId + "_"
								+ BudgetDto.getLocationId().trim() + "_"
								+ BudgetDto.getProcessproductId() + "_"
								+ BudgetDto.getProcessproductLevelId() + "_"
								+ Constants.STORE_LEVEL_ID));
				cpsmt.setObject(3, Constants.CTD_PERIOD);
				cpsmt.addBatch();
			}

		} catch (SQLException e) {
			logger.error("addPeriodSqlBatch.........method error....", e);
			e.printStackTrace();

		}
	}

	private String insertPeriodQuery(String methodName) {

		StringBuffer sql = new StringBuffer();

		if (methodName.equalsIgnoreCase("SALESINSERT")) {
			sql.append(" update  SALES_AGGR set ADJ_TOT_REVENUE = ?");
			sql.append(" where LOCATION_ID= ?");
			sql.append(" and LOCATION_LEVEL_ID = ? and PRODUCT_ID is null and PRODUCT_LEVEL_ID is null and CALENDAR_ID=?");
		} else if (methodName.equalsIgnoreCase("CTDINSERT")) {
			sql.append(" update  SALES_AGGR_CTD set ADJ_TOT_REVENUE = ?");
			sql.append(" where SUMMARY_CTD_ID= ? AND CTD_TYPE=?");
			/* sql.append(" and ctd_type=").append(Constants.CTD_PERIOD); */

		}

		return sql.toString();
	}


	/**
	 * @param _conn
	 * @param _processYear
	 * @param locationLevelId
	 * @param calendarRowType
	 * @param quarterEndDate
	 * @return
	 * @throws GeneralException
	 */
	public HashMap<String, String> getPreviousCtdMap(Connection _conn,
			int _processYear, int locationLevelId, String calendarRowType,
			Date quarterEndDate) throws GeneralException {

		HashMap<String, String> returnMap = new HashMap<String, String>();

		StringBuffer sql = new StringBuffer();

		// for quarter and year process ... format the end date
		SimpleDateFormat formatter = new SimpleDateFormat("dd-MM-yyyy");
		sql.append(" select SUMMARY_CTD_ID,LOCATION_ID,CALENDAR_ID ");
		sql.append(" ,PRODUCT_ID,PRODUCT_LEVEL_ID");
		if (locationLevelId == 5)
			sql.append(" from SALES_AGGR_DAILY");
		else
			sql.append(" from SALES_AGGR_DAILY_ROLLUP");
		sql.append("  where  CALENDAR_ID IN (");
		sql.append(" select CALENDAR_ID FROM RETAIL_CALENDAR where CAL_YEAR =")
				.append(_processYear);
		sql.append("  and START_DATE");

		// for period ... get the end date based on period end date
		if (calendarRowType.equalsIgnoreCase("P")) {
			sql.append(
					" IN(select END_DATE from RETAIL_CALENDAR  where CAL_YEAR=")
					.append(_processYear);
			sql.append(" and ROW_TYPE  ='").append(calendarRowType)
					.append("')");
		}

		// for quarter .... get the end date based on quarter and year end date
		else if (calendarRowType.equalsIgnoreCase("Q")) {
			sql.append(" =to_date('").append(formatter.format(quarterEndDate))
					.append("'").append(",'dd-MM-yyyy')");
		}

		sql.append("  AND  ROW_TYPE      ='" + Constants.CALENDAR_DAY + "')");
		sql.append(" and (product_level_id is null or product_level_id=6)");
		sql.append(" and location_level_id=" + locationLevelId + "");
		sql.append(" order by calendar_id");

		logger.debug(" Get Summary Ctd Id query......... " + sql.toString());

		try {

			CachedRowSet rst = PristineDBUtil.executeQuery(_conn, sql,
					"Get Summary Ctd Id query");

			while (rst.next()) {

				returnMap.put(
						rst.getString("location_id") + "_"
								+ rst.getString("CALENDAR_ID") + "_"
								+ rst.getString("PRODUCT_ID") + "_"
								+ rst.getString("PRODUCT_LEVEL_ID"),
						rst.getString("summary_ctd_id"));
			}

		} catch (Exception exe) {
			logger.error(exe);
			throw new GeneralException("Error ", exe);
		}

		logger.info(" ..... Return Map size ...... " + returnMap.size());

		return returnMap;

	}

	/**
	 * @param _conn
	 * @param ditrictPeriodAccounts
	 * @param locationLevelId
	 * @param methodName
	 * @param identicalDetails
	 * @param summaryCtdMap
	 * @param calendarId
	 * @param ctdConstants
	 * @return
	 * @throws GeneralException
	 */
	public boolean districtInsert(Connection _conn,
			HashMap<String, BudgetDto> ditrictPeriodAccounts,
			int locationLevelId, String methodName,
			HashMap<String, BudgetDto> identicalDetails,
			HashMap<String, Integer> summaryCtdMap, int calendarId,
			int ctdConstants) throws GeneralException {

		PreparedStatement psmt = null;
		PreparedStatement cpsmt = null;
		PreparedStatement spsmt = null;
		try {
			// Intialize the Prepared statment.
			psmt = _conn
					.prepareStatement(insertDistrictQuery("SALESINSERT", 0));
			cpsmt = _conn.prepareStatement(insertDistrictQuery("CTDINSERT",
					ctdConstants));
			spsmt = _conn
					.prepareStatement(insertDistrictQuery("STOREINSERT", 0));
		} catch (SQLException sql) {
			logger.error(" Error in Prepared statment", sql);
			throw new GeneralException(" Error in Prepared statment....", sql);
		}

		// itreate the map
		Object[] accountsArray = ditrictPeriodAccounts.values().toArray();

		for (int acc = 0; acc < accountsArray.length; acc++) {

			BudgetDto objDto = (BudgetDto) accountsArray[acc];

			// for finance department
			if (calendarId != 0) {
				objDto.setCalendarId(calendarId);
			}

			int monthEndCalendarId = getMonthEndCalendarId(_conn,
					objDto.getCalendarId());

			if (objDto.getProcessproductId() == 0) {

				addDistrictSqlBatch(spsmt, objDto, summaryCtdMap, cpsmt,
						identicalDetails, "STORE", locationLevelId, methodName,
						ctdConstants, monthEndCalendarId);
			} else {

				addDistrictSqlBatch(psmt, objDto, summaryCtdMap, cpsmt,
						identicalDetails, "PRODUCT", locationLevelId,
						methodName, ctdConstants, monthEndCalendarId);
			}

		}

		try {
			psmt.executeBatch();
			//cpsmt.executeBatch();
			spsmt.executeBatch();
		} catch (Exception e) {
			logger.error(" ... Exceute Error...." + e);
			throw new GeneralException("... Exceute Error...." + e);

		}
		return true;

	}

	/**
	 * @param psmt
	 * @param BudgetDto
	 * @param summaryCtdMap
	 * @param cpsmt
	 * @param identicalDetails
	 * @param queryName
	 * @param locationLevelId
	 * @param methodName
	 * @param ctdConstants
	 * @param monthEndCalendarId
	 */
	private void addDistrictSqlBatch(PreparedStatement psmt,
			BudgetDto BudgetDto, HashMap<String, Integer> summaryCtdMap,
			PreparedStatement cpsmt,
			HashMap<String, BudgetDto> identicalDetails, String queryName,
			int locationLevelId, String methodName, int ctdConstants,
			int monthEndCalendarId) {

		try {

			psmt.setObject(1, BudgetDto.getStoreBudget());
			psmt.setObject(3, BudgetDto.getLocationId());
			if (queryName.equalsIgnoreCase("STORE")) {
				psmt.setObject(4, locationLevelId);
				psmt.setObject(5, BudgetDto.getCalendarId());
			} else if (queryName.equalsIgnoreCase("PRODUCT")) {
				psmt.setObject(4, locationLevelId);
				psmt.setObject(5, BudgetDto.getProcessproductId());
				psmt.setObject(6, BudgetDto.getProcessproductLevelId());
				psmt.setObject(7, BudgetDto.getCalendarId());
			}

			if (methodName.equalsIgnoreCase("DISTRICT")) {
				if (identicalDetails != null
						&& identicalDetails.containsKey(BudgetDto
								.getLocationId()
								+ "_"
								+ BudgetDto.getCalendarId()
								+ "_"
								+ BudgetDto.getProcessproductId()
								+ "_"
								+ BudgetDto.getProcessproductLevelId())) {

					BudgetDto identical = new BudgetDto();

					identical = identicalDetails.get(BudgetDto.getLocationId()
							+ "_" + BudgetDto.getCalendarId() + "_"
							+ BudgetDto.getProcessproductId() + "_"
							+ BudgetDto.getProcessproductLevelId());

					psmt.setObject(2, identical.getIdenticalBudget());
				} else {
					psmt.setObject(2, 0);
				}
			}

			else if (methodName.equalsIgnoreCase("REGION")) {
				psmt.setObject(2, BudgetDto.getIdenticalBudget());
			} else if (methodName.equalsIgnoreCase("DISTRICTQUARTER")) {
				psmt.setObject(2, BudgetDto.getIdenticalBudget());
			}

			psmt.addBatch();

			if (summaryCtdMap.containsKey(monthEndCalendarId + "_"
					+ BudgetDto.getLocationId().trim() + "_"
					+ BudgetDto.getProcessproductId() + "_"
					+ BudgetDto.getProcessproductLevelId() + "_"
					+ locationLevelId)) {

				cpsmt.setObject(1, BudgetDto.getStoreBudget());

				if (methodName.equalsIgnoreCase("DISTRICT")) {
					if (identicalDetails != null
							&& identicalDetails.containsKey(BudgetDto
									.getLocationId()
									+ "_"
									+ BudgetDto.getCalendarId()
									+ "_"
									+ BudgetDto.getProcessproductId()
									+ "_"
									+ BudgetDto.getProcessproductLevelId())) {
						BudgetDto identical = identicalDetails.get(BudgetDto
								.getLocationId()
								+ "_"
								+ BudgetDto.getCalendarId()
								+ "_"
								+ BudgetDto.getProcessproductId()
								+ "_"
								+ BudgetDto.getProcessproductLevelId());
						cpsmt.setObject(2, identical.getIdenticalBudget());

					} else {
						cpsmt.setObject(2, 0);
					}
				} else if (methodName.equalsIgnoreCase("REGION")) {
					cpsmt.setObject(2, BudgetDto.getIdenticalBudget());
				} else if (methodName.equalsIgnoreCase("DISTRICTQUARTER")) {
					cpsmt.setObject(2, BudgetDto.getIdenticalBudget());
				}

				cpsmt.setObject(
						3,
						summaryCtdMap.get(monthEndCalendarId + "_"
								+ BudgetDto.getLocationId().trim() + "_"
								+ BudgetDto.getProcessproductId() + "_"
								+ BudgetDto.getProcessproductLevelId() + "_"
								+ locationLevelId));

				/* cpsmt.setObject(4, ctdConstants); */
				cpsmt.addBatch();
			}

		} catch (SQLException e) {
			e.printStackTrace();
		}

	}

	private String insertDistrictQuery(String methodName, int ctdConstants) {

		StringBuffer sql = new StringBuffer();

		if (methodName.equalsIgnoreCase("STOREINSERT")) {
			sql.append(" update  SALES_AGGR_ROLLUP set ADJ_TOT_REVENUE = ?");
			sql.append(" , ADJ_ID_TOT_REVENUE =? ");
			sql.append(" where LOCATION_ID= ?");
			sql.append(" and LOCATION_LEVEL_ID = ? and PRODUCT_ID is null and PRODUCT_LEVEL_ID is null and CALENDAR_ID=?");
		} else if (methodName.equalsIgnoreCase("CTDINSERT")) {

			sql.append(" update  SALES_AGGR_CTD set ADJ_TOT_REVENUE = ?");
			sql.append(" , ADJ_ID_TOT_REVENUE =? ");
			sql.append(" where SUMMARY_CTD_ID= ?");
			sql.append(" and ctd_type = ").append(ctdConstants);
		} else if (methodName.equalsIgnoreCase("SALESINSERT")) {
			sql.append(" update  SALES_AGGR_ROLLUP set ADJ_TOT_REVENUE = ?");
			sql.append(" , ADJ_ID_TOT_REVENUE =? ");
			sql.append(" where LOCATION_ID= ?");
			sql.append(" and LOCATION_LEVEL_ID = ? and PRODUCT_ID = ? and PRODUCT_LEVEL_ID =? and CALENDAR_ID=?");
		}

		return sql.toString();

	}

	/**
	 * @param _conn
	 * @param quarterAccounts
	 * @param storeLevelId
	 * @param string
	 * @param object
	 * @param summaryCtdMap
	 * @param calendarId
	 * @param ctdConstants
	 * @return
	 * @throws GeneralException
	 */
	public boolean quarterInsert(Connection _conn,
			HashMap<String, BudgetDto> quarterAccounts, int storeLevelId,
			String string, Object object,
			HashMap<String, Integer> summaryCtdMap, int calendarId,
			int ctdConstants) throws GeneralException {

		PreparedStatement psmt = null;
		PreparedStatement cpsmt = null;
		PreparedStatement spsmt = null;
		try {
			// Intialize the Prepared statment.
			psmt = _conn.prepareStatement(insertQuarterQuery("SALESINSERT", 0));
			cpsmt = _conn.prepareStatement(insertQuarterQuery("CTDINSERT",
					ctdConstants));
			spsmt = _conn
					.prepareStatement(insertQuarterQuery("STOREINSERT", 0));
		} catch (SQLException sql) {
			logger.error(" Error in Prepared statment", sql);
			throw new GeneralException(" Error in Prepared statment....", sql);
		}

		// itreate the map
		Object[] accountsArray = quarterAccounts.values().toArray();

		for (int acc = 0; acc < accountsArray.length; acc++) {

			BudgetDto objDto = (BudgetDto) accountsArray[acc];

			// for finance department

			objDto.setCalendarId(calendarId);

			int monthEndCalendarId = getMonthEndCalendarId(_conn, calendarId);

			if (objDto.getProcessproductId() == 0) {

				addquarterSqlBatch(spsmt, objDto, summaryCtdMap, cpsmt,
						"STORE", storeLevelId, ctdConstants, monthEndCalendarId);

			} else {
				addquarterSqlBatch(psmt, objDto, summaryCtdMap, cpsmt,
						"PRODUCT", storeLevelId, ctdConstants,
						monthEndCalendarId);
			}

		}

		try {
			psmt.executeBatch();
			//cpsmt.executeBatch();
			spsmt.executeBatch();
		} catch (Exception e) {
			logger.error(" ... Exceute Error...." + e);
			throw new GeneralException("... Exceute Error...." + e);

		}
		return true;

	}

	/**
	 * @param psmt
	 * @param BudgetDto
	 * @param summaryCtdMap
	 * @param cpsmt
	 * @param methodName
	 * @param storeLevelId
	 * @param ctdConstants
	 * @param monthEndCalendarId
	 */
	private void addquarterSqlBatch(PreparedStatement psmt,
			BudgetDto BudgetDto, HashMap<String, Integer> summaryCtdMap,
			PreparedStatement cpsmt, String methodName, int storeLevelId,
			int ctdConstants, int monthEndCalendarId) {

		try {

			psmt.setObject(1, BudgetDto.getStoreBudget());
			psmt.setObject(2, BudgetDto.getLocationId());
			if (methodName.equalsIgnoreCase("STORE")) {
				psmt.setObject(3, storeLevelId);
				psmt.setObject(4, BudgetDto.getCalendarId());
			} else if (methodName.equalsIgnoreCase("PRODUCT")) {
				psmt.setObject(3, storeLevelId);
				psmt.setObject(4, BudgetDto.getProcessproductId());
				psmt.setObject(5, BudgetDto.getProcessproductLevelId());
				psmt.setObject(6, BudgetDto.getCalendarId());
			}

			psmt.addBatch();

			/*
			 * logger.error("--------------------" +
			 * BudgetDto.getLocationId().trim() + "..... " +
			 * summaryCtdMap.get(BudgetDto.getLocationId().trim()));
			 */
			if (summaryCtdMap
					.containsKey(monthEndCalendarId + "_"
							+ BudgetDto.getLocationId().trim() + "_"
							+ BudgetDto.getProcessproductId() + "_"
							+ BudgetDto.getProcessproductLevelId() + "_"
							+ storeLevelId)) {

				cpsmt.setObject(1, BudgetDto.getStoreBudget());

				cpsmt.setObject(
						2,
						summaryCtdMap.get(monthEndCalendarId + "_"
								+ BudgetDto.getLocationId().trim() + "_"
								+ BudgetDto.getProcessproductId() + "_"
								+ BudgetDto.getProcessproductLevelId() + "_"
								+ storeLevelId));
				cpsmt.addBatch();
			}

		} catch (SQLException e) {

			logger.error("addPeriodSqlBatch....method error ", e);

		}

	}

	private String insertQuarterQuery(String methodName, int ctdConstants) {

		StringBuffer sql = new StringBuffer();

		if (methodName.equalsIgnoreCase("STOREINSERT")) {
			sql.append(" update  SALES_AGGR set ADJ_TOT_REVENUE = ?");
			sql.append(" where LOCATION_ID= ?");
			sql.append(" and LOCATION_LEVEL_ID = ? and PRODUCT_ID is null and PRODUCT_LEVEL_ID is null and CALENDAR_ID=?");
		} else if (methodName.equalsIgnoreCase("CTDINSERT")) {

			sql.append(" update  SALES_AGGR_CTD set ADJ_TOT_REVENUE = ?");
			sql.append(" where SUMMARY_CTD_ID= ?");
			sql.append(" and ctd_type =").append(ctdConstants);
		} else if (methodName.equalsIgnoreCase("SALESINSERT")) {
			sql.append(" update  SALES_AGGR set ADJ_TOT_REVENUE = ?");
			sql.append(" where LOCATION_ID= ?");
			sql.append(" and LOCATION_LEVEL_ID = ? and PRODUCT_ID = ? and PRODUCT_LEVEL_ID =? and CALENDAR_ID=?");
		}
		return sql.toString();
	}
	
	
	/**
	 * @param _conn
	 * @param _processYear
	 * @param startDate
	 * @param endDate
	 * @param calendarPeriod
	 * @param locationLevelId
	 * @param methodName
	 * @return
	 * @throws GeneralException
	 */
	public HashMap<String, BudgetDto> getDistrictQuarterAccounts(
			Connection _conn, int _processYear, Date startDate, Date endDate,
			String calendarPeriod, int locationLevelId, String methodName)
			throws GeneralException {

		HashMap<String, BudgetDto> returnMap = new HashMap<String, BudgetDto>();
		try {
			SimpleDateFormat formatter = new SimpleDateFormat("dd-MM-yyyy");
			StringBuffer sql = new StringBuffer();
			sql.append(" select sum(ADJ_TOT_REVENUE) as TOT_REVENUE, LOCATION_ID");
			sql.append(" ,sum(ADJ_ID_TOT_REVENUE) AS ID_TOT_REVENUE");
			sql.append(" ,A.PRODUCT_ID,A.PRODUCT_LEVEL_ID");
			sql.append(" from SALES_AGGR_ROLLUP A , RETAIL_CALENDAR c  ");
			sql.append("  where LOCATION_LEVEL_ID=").append(locationLevelId);
			sql.append("  AND A.CALENDAR_ID = C.CALENDAR_ID ");
			sql.append(" AND C.CAL_YEAR=").append(_processYear);
			sql.append(" AND C.ROW_TYPE='").append(calendarPeriod).append("'");
			sql.append(" and A.LOCATION_LEVEL_ID=").append(locationLevelId);
			sql.append(" and ADJ_TOT_REVENUE is not null");
			sql.append(" and A.CALENDAR_ID in(");
			sql.append(" select CALENDAR_ID from RETAIL_CALENDAR where");
			sql.append(" CAL_YEAR =").append(_processYear);

			sql.append(" and START_DATE between to_date('")
					.append(formatter.format(startDate))
					.append("','dd-MM-yyyy')");
			sql.append(" and to_date('").append(formatter.format(endDate))
					.append("','dd-MM-yyyy')");
			sql.append(" and row_type='").append(calendarPeriod).append("'");
			sql.append(" )");

			sql.append(" group by LOCATION_ID,A.PRODUCT_ID,A.PRODUCT_LEVEL_ID");
			sql.append(" order by LOCATION_ID,A.PRODUCT_ID,A.PRODUCT_LEVEL_ID");
			logger.debug(methodName + "...." + sql.toString());

			try {
				CachedRowSet result = PristineDBUtil.executeQuery(_conn, sql,
						"getregionForecaste");
				while (result.next()) {

					BudgetDto objBudget = new BudgetDto();
					objBudget.setLocationId(result.getString("LOCATION_ID"));
					objBudget.setStoreBudget(result.getDouble("TOT_REVENUE"));
					objBudget.setProcessproductId(result.getInt("PRODUCT_ID"));
					objBudget.setProcessproductLevelId(result
							.getInt("PRODUCT_LEVEL_ID"));
					objBudget.setIdenticalBudget(result
							.getDouble("ID_TOT_REVENUE"));

					returnMap.put(
							objBudget.getLocationId() + "_"
									+ objBudget.getProcessproductId() + "_"
									+ objBudget.getProcessproductLevelId(),
							objBudget);

				}

			} catch (SQLException exe) {
				logger.error(exe);
				throw new GeneralException(
						"Fetch Region Level Accounts Error.....", exe);
			}
		} catch (Exception exe) {
			logger.error(exe);
			throw new GeneralException(
					"Fetch Region Level Accounts Error.....", exe);
		}

		return returnMap;
	}

	/**
	 * @param _conn
	 * @param calendarId
	 * @param ii
	 * @param tableName
	 * @return
	 * @throws GeneralException
	 */
	public List<BudgetDto> getrevenueforCtd(Connection _conn, int calendarId,
			int ii, String tableName) throws GeneralException {

		List<BudgetDto> returnList = new ArrayList<BudgetDto>();
		StringBuffer sql = new StringBuffer();
		sql.append(" select sum(ADJ_TOT_REVENUE) as ADJ_TOT_REVENUE,LOCATION_ID,LOCATION_LEVEL_ID,PRODUCT_ID,PRODUCT_LEVEL_ID ");
		if (tableName.equalsIgnoreCase("SALES_AGGR_ROLLUP")) {
			sql.append(" ,sum(ADJ_ID_TOT_REVENUE) as ADJ_ID_TOT_REVENUE");
		}
		sql.append(" from ").append(tableName);
		sql.append(" where CALENDAR_ID >=").append(calendarId - ii);
		sql.append(" and CALENDAR_ID <=").append(calendarId);
		sql.append(" and (product_level_id=").append(
				Constants.FINANCEDEPARTMENT);
		sql.append(" or product_level_id is null)");
		sql.append(" group by LOCATION_ID ,LOCATION_LEVEL_ID,PRODUCT_ID,PRODUCT_LEVEL_ID");
		// logger.info(" Ctd Query.... " + sql.toString());

		try {
			CachedRowSet result = PristineDBUtil.executeQuery(_conn, sql,
					"getrevenueforCtd");

			while (result.next()) {

				BudgetDto objBudget = new BudgetDto();

				objBudget.setLocationId(result.getString("LOCATION_ID"));
				objBudget.setLocationLevelId(result
						.getString("LOCATION_LEVEL_ID"));
				objBudget.setStoreBudget(result.getDouble("ADJ_TOT_REVENUE"));
				objBudget.setProcessproductId(result.getInt("PRODUCT_ID"));
				objBudget.setProcessproductLevelId(result
						.getInt("PRODUCT_LEVEL_ID"));
				if (tableName.equalsIgnoreCase("SALES_AGGR_ROLLUP")) {
					objBudget.setIdenticalBudget(result
							.getDouble("ADJ_ID_TOT_REVENUE"));
				}
				returnList.add(objBudget);

			}
		} catch (SQLException e) {
			logger.error(e);
			throw new GeneralException("Error....", e);
		} catch (GeneralException e) {
			logger.error(e);
			throw new GeneralException("Error....", e);
		}

		return returnList;
	}
	
	/**
	 * @param _conn
	 * @param calendarId
	 * @param tableName
	 * @param locatioId
	 * @param locationLevelId
	 * @param productId
	 * @param productLevelId
	 * @return
	 * @throws GeneralException
	 */
	public String getCtdid(Connection _conn, int calendarId, String tableName,
			String locatioId, Object locationLevelId, int productId,
			int productLevelId) throws GeneralException {

		String returnId = null;

		StringBuffer sql = new StringBuffer();

		// for quarter and year process ... format the end date

		sql.append(" select SUMMARY_CTD_ID");
		if (tableName.equalsIgnoreCase("SALES_AGGR"))
			sql.append(" from SALES_AGGR_DAILY");
		else
			sql.append(" from SALES_AGGR_DAILY_ROLLUP");

		sql.append("  where  CALENDAR_ID IN (");
		sql.append(" select CALENDAR_ID FROM RETAIL_CALENDAR where ");
		sql.append(
				" start_date in(select end_date from retail_calendar where calendar_id=")
				.append(calendarId);
		sql.append(" )");
		sql.append("  AND  ROW_TYPE ='" + Constants.CALENDAR_DAY + "')");
		sql.append(" and LOCATION_ID=").append(locatioId);
		sql.append(" and location_level_id=" + locationLevelId + "");
		if (productId == 0) {
			sql.append(" and product_id is null");
			sql.append(" and product_level_id is null");
		} else {
			sql.append(" and product_id=").append(productId);
			sql.append(" and product_level_id=").append(productLevelId);
		}

		// logger.debug(" Get Summary Ctd Id query......... " + sql.toString());

		try {

			returnId = PristineDBUtil.getSingleColumnVal(_conn, sql, "GetCtd");

		} catch (Exception exe) {
			logger.error(exe);
			throw new GeneralException("Error ", exe);
		}

		return returnId;

	}

	public void upDateCtd(Connection _conn, double storeBudget,
			double identicalBudget, String ctdId, int ctdConstant) throws GeneralException {
		
		StringBuffer sql = new StringBuffer();
		
		sql.append(" update sales_aggr_ctd ");
		sql.append(" set adj_tot_revenue=").append(storeBudget);
		sql.append( " ,adj_id_tot_revenue=").append(identicalBudget);
		sql.append(" where CTD_TYPE =").append(ctdConstant);
		sql.append( " and SUMMARY_CTD_ID=").append(ctdId);
		
		try {
			PristineDBUtil.executeUpdate(_conn, sql, "upDateCtd");
		} catch (GeneralException e) {
			logger.error(e);
			 throw new GeneralException(" Error.... " ,e);
		}
		
	 
		
	}

	
	/**
	 * @param _conn
	 * @param _processYear
	 * @param tableName
	 * @param ctdConstant
	 * @return
	 * @throws GeneralException
	 */
	public HashMap<String, Integer> getPeriodCtdMap(Connection _conn,
			int _processYear, String tableName, String ctdConstant) throws GeneralException {
	 
		HashMap<String, Integer> returnMap = new HashMap<String, Integer>();
		
		StringBuffer sql= new StringBuffer();
		
		try {
			sql.append(" select SUMMARY_CTD_ID , CALENDAR_ID,LOCATION_ID,PRODUCT_ID,PRODUCT_LEVEL_ID,LOCATION_LEVEL_ID");
			sql.append("  from ").append(tableName);
			sql.append(" where CALENDAR_ID in(select CALENDAR_ID FROM RETAIL_CALENDAR");
			sql.append(" WHERE cal_year=").append(_processYear).append("  AND ROW_TYPE='").append(Constants.CALENDAR_DAY).append("'");
			sql.append(" AND START_DATE IN(select END_DATE from RETAIL_CALENDAR");
			sql.append(" where CAL_YEAR=").append(_processYear).append(" and ROW_TYPE='").append(ctdConstant).append("'))");
			sql.append(" and (PRODUCT_LEVEL_ID=6 OR PRODUCT_LEVEL_ID IS NULL)");
			sql.append(" order by  CALENDAR_ID,LOCATION_ID,PRODUCT_ID,PRODUCT_LEVEL_ID ");
			 
			logger.debug(" Ctd Sql.... " + sql.toString());
			
			 final CachedRowSet result = PristineDBUtil.executeQuery(_conn, sql, "Get Period Ctd Map");
			 
			 while( result.next()){
				 
				 returnMap.put(result.getInt("CALENDAR_ID")+"_"+result.getInt("LOCATION_ID")+"_"+result.getInt("PRODUCT_ID")+"_"+
						 result.getInt("PRODUCT_LEVEL_ID")+"_"+result.getInt("LOCATION_LEVEL_ID"),result.getInt("SUMMARY_CTD_ID"));
			 }
		} catch (SQLException e) {
			 logger.error(e);
			 throw new GeneralException(" Error In GetPeriod Ctd Map.... " , e);
		} catch (GeneralException e) {
			logger.error(e);
			 throw new GeneralException(" Error In GetPeriod Ctd Map.... " , e);
		}
		
				
		return returnMap;
	}

	/**
	 * @param _conn
	 * @param _processYear
	 * @param startDate
	 * @param endDate
	 * @param calendarPeriod
	 * @param locationLevelId
	 * @param methodName
	 * @param chainId
	 * @return
	 * @throws GeneralException
	 */
	public HashMap<String, BudgetDto> getChainPeriodAccounts(
			Connection _conn, int _processYear, Date startDate, Date endDate,
			String calendarPeriod, int locationLevelId, String methodName ,int chainId)
			throws GeneralException {

		HashMap<String, BudgetDto> returnMap = new HashMap<String, BudgetDto>();
		try {
			SimpleDateFormat formatter = new SimpleDateFormat("dd-MM-yyyy");
			StringBuffer sql = new StringBuffer();
			sql.append(" select sum(ADJ_TOT_REVENUE) as TOT_REVENUE");
			sql.append(" ,sum(ADJ_ID_TOT_REVENUE) AS ID_TOT_REVENUE");
			sql.append(" ,A.PRODUCT_ID,A.PRODUCT_LEVEL_ID");
			if (methodName.equalsIgnoreCase("PERIOD"))
				sql.append(" ,A.CALENDAR_ID");

			sql.append(" from SALES_AGGR_ROLLUP A , RETAIL_CALENDAR B  ");
			sql.append("  where A.CALENDAR_ID = B.CALENDAR_ID ");
			sql.append(" AND B.CAL_YEAR=").append(_processYear);
			sql.append(" AND B.ROW_TYPE='").append(calendarPeriod).append("'");
			sql.append(" and A.LOCATION_LEVEL_ID=").append(locationLevelId);
			sql.append(" and ADJ_TOT_REVENUE is not null");

			if (methodName.equalsIgnoreCase("QUARTER")) {
				sql.append(" and A.CALENDAR_ID in(");
				sql.append(" select CALENDAR_ID from RETAIL_CALENDAR where");
				sql.append(" CAL_YEAR =").append(_processYear);
				sql.append(" and START_DATE between to_date('")
						.append(formatter.format(startDate))
						.append("','dd-MM-yyyy')");
				sql.append(" and to_date('").append(formatter.format(endDate))
						.append("','dd-MM-yyyy')");
				sql.append(" and row_type='").append(calendarPeriod)
						.append("'");
				sql.append(" )");
			}
			sql.append(" group by A.PRODUCT_ID,A.PRODUCT_LEVEL_ID");
			if (methodName.equalsIgnoreCase("PERIOD"))
				sql.append(" ,A.CALENDAR_ID");
			sql.append(" order by A.PRODUCT_ID,A.PRODUCT_LEVEL_ID");
			if (methodName.equalsIgnoreCase("PERIOD"))
				sql.append(" ,A.CALENDAR_ID");

			logger.debug(methodName + "...." + sql.toString());

			try {
				CachedRowSet result = PristineDBUtil.executeQuery(_conn, sql,
						"getChainForecast...");
				while (result.next()) {

					BudgetDto objBudget = new BudgetDto();
					objBudget.setLocationId(String.valueOf(chainId));
					objBudget.setStoreBudget(result.getDouble("TOT_REVENUE"));
					objBudget.setProcessproductId(result.getInt("PRODUCT_ID"));
					objBudget.setProcessproductLevelId(result
							.getInt("PRODUCT_LEVEL_ID"));
					objBudget.setIdenticalBudget(result
							.getDouble("ID_TOT_REVENUE"));

					if (methodName.equalsIgnoreCase("PERIOD")) {
						objBudget.setCalendarId(result.getInt("CALENDAR_ID"));
						returnMap.put(
								objBudget.getLocationId() + "_"
										+ objBudget.getCalendarId() + "_"
										+ objBudget.getProcessproductId() + "_"
										+ objBudget.getProcessproductLevelId(),
								objBudget);
					} else if (methodName.equalsIgnoreCase("QUARTER")
							|| methodName.equalsIgnoreCase("YEAR")) {
						returnMap.put(objBudget.getLocationId() + "_"
								+ objBudget.getProcessproductId() + "_"
								+ objBudget.getProcessproductLevelId(),
								objBudget);
					}

				}

			} catch (Exception exe) {
				logger.error(exe);
				throw new GeneralException("Get chain Level Accounts Error",exe);
			}
		} catch (Exception exe) {
			logger.error(exe);
			throw new GeneralException("Get chain Level Accounts Error", exe);
		}

		return returnMap;

	}
		
	
}
