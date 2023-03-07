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


/**
 * @author denish
 *
 */
public class BudgetDao {
	
	   static Logger logger = Logger.getLogger("AccountDao");
	   SimpleDateFormat formatter = new SimpleDateFormat("dd-MM-yyyy");	
		/*
		 * Method used to itreate the map and insert the values into table 
		 * Argument 1 : Connection
		 * Argument 2 : HashMap<String, BudgetDto> budgetDetails
		 * Argument 3 : HashMap<String, Integer> calendarMap
		 * Catch Exception , SqlException
		 * throws gendralexception 
		 */
	/**
	 * @param _conn
	 * @param budgetDetails
	 * @param calendarMap
	 * @param storeMap
	 * @param locationLevelId
	 * @param methodName
	 * @param calendarId
	 * @param previousBudget
	 * @param identicaldistrictPeriodbudget 
	 * @param modeOfcalling 
	 * @return
	 * @throws GeneralException
	 */
	public boolean budgetInsert(Connection _conn,
			HashMap<String, BudgetDto> budgetDetails,
			HashMap<String, Integer> calendarMap,
			HashMap<String, Integer> storeMap, int locationLevelId,
			String methodName, int calendarId,
			HashMap<String, String> previousBudget,
			HashMap<String, BudgetDto> identicaldistrictPeriodbudget,
			String modeOfcalling) throws GeneralException {

		PreparedStatement psmt = null;
		PreparedStatement upsmt = null;
		boolean commitFlag = false;
		try {
			// Intialize the Prepared statment.
			psmt = _conn.prepareStatement(insertQuery("INSERT"));
			upsmt = _conn.prepareStatement(insertQuery("UPDATE"));
		} catch (SQLException sql) {
			logger.error(" Error in Prepared statment", sql);
			throw new GeneralException(" Error in Prepared statment....", sql);
		}

		// itreate the map
		Object[] budgetArray = budgetDetails.values().toArray();

		for (int acc = 0; acc < budgetArray.length; acc++) {

			BudgetDto objDto = (BudgetDto) budgetArray[acc];

			if (methodName.equalsIgnoreCase("FINANCE")) {

				if (storeMap.containsKey(objDto.getLocationId().trim())) {

					Integer locationid = storeMap.get(objDto.getLocationId()
							.trim());

					addSqlBatch(psmt, objDto, locationid, calendarMap,
							locationLevelId, methodName, previousBudget, upsmt);
				}
			}

			else {

				if (calendarId != 0) {
					objDto.setCalendarId(calendarId);
				}

				if (modeOfcalling.equalsIgnoreCase("IDENTICAL")) {

					if (methodName.equalsIgnoreCase("DISTRICTPERIOD")) {
						if (identicaldistrictPeriodbudget.containsKey(objDto
								.getLocationId()
								+ "_"
								+ objDto.getCalendarId()
								+ "_"
								+ objDto.getProductId()
								+ "_"
								+ objDto.getProductLevelId())) {

							BudgetDto identicalBudgetDto = identicaldistrictPeriodbudget
									.get(objDto.getLocationId() + "_"
											+ objDto.getCalendarId() + "_"
											+ objDto.getProductId() + "_"
											+ objDto.getProductLevelId());

							objDto.setIdenticalBudget(identicalBudgetDto
									.getStoreBudget());
						}
					} else {
						if (identicaldistrictPeriodbudget.containsKey(objDto
								.getLocationId()
								+ "_"
								+ objDto.getProductId()
								+ "_" + objDto.getProductLevelId())) {

							BudgetDto identicalBudgetDto = identicaldistrictPeriodbudget
									.get(objDto.getLocationId() + "_"
											+ objDto.getProductId() + "_"
											+ objDto.getProductLevelId());

							objDto.setIdenticalBudget(identicalBudgetDto
									.getStoreBudget());
						}
					}

				}

				addSqlBatch(psmt, objDto, 0, null, locationLevelId, methodName,
						null, null);
			}
		}

		try {
			psmt.executeBatch();

			if (methodName.equalsIgnoreCase("FINANCE"))
				upsmt.executeBatch();

			commitFlag = true;

		} catch (Exception e) {
			logger.error(" ... Exceute Error...." + e);
			throw new GeneralException("... Exceute Error...." + e);

		}
		return commitFlag;
	}
		
	
		/*
		 * Method used to add the map values into batch
		 *   
		 */
	/**
	 * @param psmt
	 * @param objDto
	 * @param locationid
	 * @param calendarMap
	 * @param locationLevelId
	 * @param modeOfInsert
	 * @param previousBudget
	 * @param upsmt
	 */
	private void addSqlBatch(PreparedStatement psmt, BudgetDto objDto,
			Integer locationid, HashMap<String, Integer> calendarMap,
			int locationLevelId, String modeOfInsert,
			HashMap<String, String> previousBudget, PreparedStatement upsmt) {

		try {

			if (modeOfInsert.equalsIgnoreCase("PERIOD")
					|| modeOfInsert.equalsIgnoreCase("QUARTER")
					|| modeOfInsert.equalsIgnoreCase("DISTRICTPERIOD")
					|| modeOfInsert.equalsIgnoreCase("DISTRICTQUARTER")) {

				// logger.debug(" Store Level budget.....");
				psmt.setObject(1, objDto.getStoreBudget());
				psmt.setObject(2, objDto.getLocationId());
				psmt.setObject(3, locationLevelId);
				psmt.setString(4, (String) (objDto.getProductId() == "" ? ""
						: objDto.getProductId()));
				psmt.setString(5,
						(String) (objDto.getProductLevelId() == "" ? ""
								: objDto.getProductLevelId()));
				psmt.setObject(6, objDto.getCalendarId());
				psmt.setObject(7, objDto.getIdenticalBudget());
				psmt.addBatch();
			} else if (modeOfInsert.equalsIgnoreCase("FINANCE")) {
				// logger.debug(objDto.getProductId() +"-" +locationid +"-"
				// +locationLevelId+"-"+objDto.getProductLevelId()+"-"+calendarMap.get("PERIOD1"));
				if (previousBudget.containsKey(calendarMap.get("PERIOD1") + "_"
						+ locationid + "_" + objDto.getProductId() + "_"
						+ objDto.getProductLevelId())) {

					upsmt.setObject(1, objDto.getPeriod_01());
					/* upsmt.setObject(2, objDto.getGasperiod_01()); */
					upsmt.setObject(2, locationid);
					upsmt.setObject(3, locationLevelId);
					upsmt.setObject(4, objDto.getProductId());
					upsmt.setObject(5, objDto.getProductLevelId());
					upsmt.setObject(6, calendarMap.get("PERIOD1"));
					upsmt.addBatch();
				} else {
					psmt.setObject(1, objDto.getPeriod_01());
					/* psmt.setObject(2, objDto.getGasperiod_01()); */
					psmt.setObject(2, locationid);
					psmt.setObject(3, locationLevelId);
					psmt.setObject(4, objDto.getProductId());
					psmt.setObject(5, objDto.getProductLevelId());
					psmt.setObject(6, calendarMap.get("PERIOD1"));
					psmt.setObject(7, 0);
					psmt.addBatch();
				}

				// logger.debug(objDto.getProductId() +"-" +locationid +"-"
				// +locationLevelId+"-"+objDto.getProductLevelId()+"-"+calendarMap.get("PERIOD2"));
				// logger.debug("objDto.getPeriod_02()" +
				// objDto.getPeriod_02());
				if (previousBudget.containsKey(calendarMap.get("PERIOD2") + "_"
						+ locationid + "_" + objDto.getProductId() + "_"
						+ objDto.getProductLevelId())) {

					upsmt.setObject(1, objDto.getPeriod_02());
					/* upsmt.setObject(2, objDto.getGasperiod_02()); */
					upsmt.setObject(6, calendarMap.get("PERIOD2"));
					upsmt.addBatch();
				} else {
					psmt.setObject(1, objDto.getPeriod_02());
					/* psmt.setObject(2, objDto.getGasperiod_02()); */
					psmt.setObject(6, calendarMap.get("PERIOD2"));
					psmt.addBatch();
				}
				// logger.debug(objDto.getProductId() +"-" +locationid +"-"
				// +locationLevelId+"-"+objDto.getProductLevelId()+"-"+calendarMap.get("PERIOD3"));
				// logger.debug("objDto.getPeriod_03()" +
				// objDto.getPeriod_03());
				if (previousBudget.containsKey(calendarMap.get("PERIOD3") + "_"
						+ locationid + "_" + objDto.getProductId() + "_"
						+ objDto.getProductLevelId())) {

					upsmt.setObject(1, objDto.getPeriod_03());
					/* upsmt.setObject(2, objDto.getGasperiod_03()); */
					upsmt.setObject(6, calendarMap.get("PERIOD3"));
					upsmt.addBatch();
				} else {
					psmt.setObject(1, objDto.getPeriod_03());
					/* psmt.setObject(2, objDto.getGasperiod_03()); */
					psmt.setObject(6, calendarMap.get("PERIOD3"));
					psmt.addBatch();
				}
				// logger.debug(objDto.getProductId() +"-" +locationid +"-"
				// +locationLevelId+"-"+objDto.getProductLevelId()+"-"+calendarMap.get("PERIOD4"));
				// logger.debug("objDto.getPeriod_04()" +
				// objDto.getPeriod_04());
				if (previousBudget.containsKey(calendarMap.get("PERIOD4") + "_"
						+ locationid + "_" + objDto.getProductId() + "_"
						+ objDto.getProductLevelId())) {

					upsmt.setObject(1, objDto.getPeriod_04());
					/* upsmt.setObject(2, objDto.getGasperiod_04()); */
					upsmt.setObject(6, calendarMap.get("PERIOD4"));
					upsmt.addBatch();
				} else {
					psmt.setObject(1, objDto.getPeriod_04());
					/* psmt.setObject(2, objDto.getGasperiod_04()); */
					psmt.setObject(6, calendarMap.get("PERIOD4"));
					psmt.addBatch();
				}
				// logger.debug(objDto.getProductId() +"-" +locationid +"-"
				// +locationLevelId+"-"+objDto.getProductLevelId()+"-"+calendarMap.get("PERIOD5"));
				// logger.debug("objDto.getPeriod_05()" +
				// objDto.getPeriod_05());
				if (previousBudget.containsKey(calendarMap.get("PERIOD5") + "_"
						+ locationid + "_" + objDto.getProductId() + "_"
						+ objDto.getProductLevelId())) {

					upsmt.setObject(1, objDto.getPeriod_05());
					/* upsmt.setObject(2, objDto.getGasperiod_05()); */
					upsmt.setObject(6, calendarMap.get("PERIOD5"));
					upsmt.addBatch();
				} else {

					psmt.setObject(1, objDto.getPeriod_05());
					/* psmt.setObject(2, objDto.getGasperiod_05()); */
					psmt.setObject(6, calendarMap.get("PERIOD5"));
					psmt.addBatch();
				}

				// logger.debug(objDto.getProductId() +"-" +locationid +"-"
				// +locationLevelId+"-"+objDto.getProductLevelId()+"-"+calendarMap.get("PERIOD6"));
				// logger.debug("objDto.getPeriod_06()" +
				// objDto.getPeriod_06());
				if (previousBudget.containsKey(calendarMap.get("PERIOD6") + "_"
						+ locationid + "_" + objDto.getProductId() + "_"
						+ objDto.getProductLevelId())) {

					upsmt.setObject(1, objDto.getPeriod_06());
					/* upsmt.setObject(2, objDto.getGasperiod_06()); */
					upsmt.setObject(6, calendarMap.get("PERIOD6"));
					upsmt.addBatch();
				} else {
					psmt.setObject(1, objDto.getPeriod_06());
					/* psmt.setObject(2, objDto.getGasperiod_06()); */
					psmt.setObject(6, calendarMap.get("PERIOD6"));
					psmt.addBatch();
				}

				// logger.debug(objDto.getProductId() +"-" +locationid +"-"
				// +locationLevelId+"-"+objDto.getProductLevelId()+"-"+calendarMap.get("PERIOD7"));
				// logger.debug("objDto.getPeriod_07()" +
				// objDto.getPeriod_07());
				if (previousBudget.containsKey(calendarMap.get("PERIOD7") + "_"
						+ locationid + "_" + objDto.getProductId() + "_"
						+ objDto.getProductLevelId())) {

					upsmt.setObject(1, objDto.getPeriod_07());
					/* upsmt.setObject(2, objDto.getGasperiod_07()); */
					upsmt.setObject(6, calendarMap.get("PERIOD7"));
					upsmt.addBatch();
				} else {
					psmt.setObject(1, objDto.getPeriod_07());
					/* psmt.setObject(2, objDto.getGasperiod_07()); */
					psmt.setObject(6, calendarMap.get("PERIOD7"));
					psmt.addBatch();
				}
				// logger.debug(objDto.getProductId() +"-" +locationid +"-"
				// +locationLevelId+"-"+objDto.getProductLevelId()+"-"+calendarMap.get("PERIOD8"));
				// logger.debug("objDto.getPeriod_08()" +
				// objDto.getPeriod_08());
				if (previousBudget.containsKey(calendarMap.get("PERIOD8") + "_"
						+ locationid + "_" + objDto.getProductId() + "_"
						+ objDto.getProductLevelId())) {

					upsmt.setObject(1, objDto.getPeriod_08());
					/* upsmt.setObject(2, objDto.getGasperiod_08()); */
					upsmt.setObject(6, calendarMap.get("PERIOD8"));
					upsmt.addBatch();
				} else {
					psmt.setObject(1, objDto.getPeriod_08());
					/* psmt.setObject(2, objDto.getGasperiod_08()); */
					psmt.setObject(6, calendarMap.get("PERIOD8"));
					psmt.addBatch();
				}
				// logger.debug(objDto.getProductId() +"-" +locationid +"-"
				// +locationLevelId+"-"+objDto.getProductLevelId()+"-"+calendarMap.get("PERIOD9"));
				// logger.debug("objDto.getPeriod_09()" +
				// objDto.getPeriod_09());
				if (previousBudget.containsKey(calendarMap.get("PERIOD9") + "_"
						+ locationid + "_" + objDto.getProductId() + "_"
						+ objDto.getProductLevelId())) {

					upsmt.setObject(1, objDto.getPeriod_09());
					/* upsmt.setObject(2, objDto.getGasperiod_09()); */
					upsmt.setObject(6, calendarMap.get("PERIOD9"));
					upsmt.addBatch();
				} else {
					psmt.setObject(1, objDto.getPeriod_09());
					/* psmt.setObject(2, objDto.getGasperiod_09()); */
					psmt.setObject(6, calendarMap.get("PERIOD9"));
					psmt.addBatch();
				}
				// logger.debug(objDto.getProductId() +"-" +locationid +"-"
				// +locationLevelId+"-"+objDto.getProductLevelId()+"-"+calendarMap.get("PERIOD10"));
				// logger.debug("objDto.getPeriod_010()" +
				// objDto.getPeriod_10());
				if (previousBudget.containsKey(calendarMap.get("PERIOD10")
						+ "_" + locationid + "_" + objDto.getProductId() + "_"
						+ objDto.getProductLevelId())) {

					upsmt.setObject(1, objDto.getPeriod_10());
					/* upsmt.setObject(2, objDto.getGasperiod_10()); */
					upsmt.setObject(6, calendarMap.get("PERIOD10"));
					upsmt.addBatch();
				} else {
					psmt.setObject(1, objDto.getPeriod_10());
					/* psmt.setObject(2, objDto.getGasperiod_10()); */
					psmt.setObject(6, calendarMap.get("PERIOD10"));
					psmt.addBatch();
				}
				// logger.debug(objDto.getProductId() +"-" +locationid +"-"
				// +locationLevelId+"-"+objDto.getProductLevelId()+"-"+calendarMap.get("PERIOD11"));
				if (previousBudget.containsKey(calendarMap.get("PERIOD11")
						+ "_" + locationid + "_" + objDto.getProductId() + "_"
						+ objDto.getProductLevelId())) {

					upsmt.setObject(1, objDto.getPeriod_11());
					/* upsmt.setObject(2, objDto.getGasperiod_11()); */
					upsmt.setObject(6, calendarMap.get("PERIOD11"));
					upsmt.addBatch();
				} else {
					psmt.setObject(1, objDto.getPeriod_11());
					/* psmt.setObject(2, objDto.getGasperiod_11()); */
					psmt.setObject(6, calendarMap.get("PERIOD11"));
					psmt.addBatch();
				}
				// logger.debug(objDto.getProductId() +"-" +locationid +"-"
				// +locationLevelId+"-"+objDto.getProductLevelId()+"-"+calendarMap.get("PERIOD12"));
				if (previousBudget.containsKey(calendarMap.get("PERIOD12")
						+ "_" + locationid + "_" + objDto.getProductId() + "_"
						+ objDto.getProductLevelId())) {

					upsmt.setObject(1, objDto.getPeriod_12());
					/* upsmt.setObject(2, objDto.getGasperiod_12()); */
					upsmt.setObject(6, calendarMap.get("PERIOD12"));
					upsmt.addBatch();
				} else {
					psmt.setObject(1, objDto.getPeriod_12());
					/* psmt.setObject(2, objDto.getGasperiod_12()); */
					psmt.setObject(6, calendarMap.get("PERIOD12"));
					psmt.addBatch();
				}
				// logger.debug(objDto.getProductId() +"-" +locationid +"-"
				// +locationLevelId+"-"+objDto.getProductLevelId()+"-"+calendarMap.get("PERIOD13"));
				if (previousBudget.containsKey(calendarMap.get("PERIOD13")
						+ "_" + locationid + "_" + objDto.getProductId() + "_"
						+ objDto.getProductLevelId())) {

					upsmt.setObject(1, objDto.getPeriod_13());
					/* upsmt.setObject(2, objDto.getGasperiod_13()); */
					upsmt.setObject(6, calendarMap.get("PERIOD13"));
					upsmt.addBatch();
				} else {
					psmt.setObject(1, objDto.getPeriod_13());
					/* psmt.setObject(2, objDto.getGasperiod_13()); */
					psmt.setObject(6, calendarMap.get("PERIOD13"));
					psmt.addBatch();
				}
			}
		} catch (SQLException sql) {
			logger.error("Location Id ...." + locationid + "...Product Id...."
					+ objDto.getProductLevelId() + "....Store No ..."
					+ objDto.getLocationId() + "....." + sql);

		}

	}

		/*
		 * Sql query for budget
		 */
	private String insertQuery(String modeOfInsert) {

		StringBuffer sql = new StringBuffer();
		if (modeOfInsert.equalsIgnoreCase("INSERT")) {
			sql.append(" insert into SALES_AGGR_BUDGET (SALES_AGGR_BUDGET_ID,TOT_REVENUE");
			sql.append(" ,LOCATION_ID,LOCATION_LEVEL_ID,PRODUCT_ID,PRODUCT_LEVEL_ID,CALENDAR_ID,BUDGET_CTD_ID,ID_TOT_REVENUE)");
			//sql.append(" values(SALES_AGGR_BUDGET_SEQ.nextval,?,?,?,?,?,?,SALES_AGGR_BUDGET_CTD_SEQ.nextval,?)");
			sql.append(" values(SALES_AGGR_BUDGET_SEQ.nextval,?,?,?,?,?,?,NULL,?)");
		} else if (modeOfInsert.equalsIgnoreCase("UPDATE")) {
			sql.append(" update  SALES_AGGR_BUDGET set TOT_REVENUE=?");
			sql.append(" where  LOCATION_ID=? and  LOCATION_LEVEL_ID=? and  PRODUCT_ID=? and PRODUCT_LEVEL_ID=? and CALENDAR_ID=?");

		}
		return sql.toString();
	}

		/*
		 * method used to get the store level budget details.
		 * Argument 1 : Connection
		 * Argument 2 : Process year 
		 * catch sqlexception
		 * throws gendral exception
		 */
	/**
	 * @param _conn
	 * @param _processYear
	 * @param startdate
	 * @param endDate
	 * @param calendarLevel
	 * @param methodName
	 * @param storeLevelId
	 * @return
	 * @throws GeneralException
	 */
	public HashMap<String, BudgetDto> getbudgetDetails(Connection _conn,
			int _processYear, Date startdate, Date endDate,
			String calendarLevel, String methodName, int storeLevelId)
			throws GeneralException {

		HashMap<String, BudgetDto> returnMap = new HashMap<String, BudgetDto>();
		try {
			StringBuffer sql = new StringBuffer();

			sql.append(" select SA.LOCATION_ID, sum(TOT_REVENUE) as TOT_REVENUE");

			// for quarter to get the records with product
			if (methodName.equalsIgnoreCase("QUARTER")
					|| methodName.equalsIgnoreCase("YEAR"))
				sql.append(" ,SA.PRODUCT_ID,SA.PRODUCT_LEVEL_ID");
			else if (methodName.equalsIgnoreCase("PERIOD")) {
				sql.append(" ,RC.CALENDAR_ID");
				// sql.append(" ,(select )")
			}

			sql.append(" from SALES_AGGR_BUDGET SA");
			sql.append(" inner join RETAIL_CALENDAR RC on RC.CALENDAR_ID = SA.CALENDAR_ID");
			sql.append(" where RC.CAL_YEAR =").append(_processYear);
			sql.append(" and RC.ROW_TYPE='").append(calendarLevel).append("'");
			sql.append(" and SA.LOCATION_LEVEL_ID=").append(storeLevelId);

			// for avoid the Gas total into store toatal
			if (methodName.equalsIgnoreCase("PERIOD")) {

				sql.append(" and SA.PRODUCT_ID not in(select PRODUCT_ID FROM PRODUCT_GROUP");
				sql.append(" where ADD_ON_TYPE='GAS')");
			}

			if (methodName.equalsIgnoreCase("QUARTER")) {
				sql.append(" and RC.CALENDAR_ID in(");
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
			sql.append(" group by SA.LOCATION_ID");

			if (methodName.equalsIgnoreCase("QUARTER")
					|| methodName.equalsIgnoreCase("YEAR"))
				sql.append(" ,SA.PRODUCT_ID,SA.PRODUCT_LEVEL_ID");

			else if (methodName.equalsIgnoreCase("PERIOD"))
				sql.append(",RC.CALENDAR_ID");

			sql.append(" order by SA.LOCATION_ID");

			if (methodName.equalsIgnoreCase("QUARTER")
					|| methodName.equalsIgnoreCase("YEAR"))
				sql.append(" ,SA.PRODUCT_ID,SA.PRODUCT_LEVEL_ID");

			else if (methodName.equalsIgnoreCase("PERIOD"))
				sql.append(",RC.CALENDAR_ID");

			logger.debug(methodName + "...." + sql.toString());

			try {

				CachedRowSet rest = PristineDBUtil.executeQuery(_conn, sql,
						"getbudgetDetails");

				while (rest.next()) {
					BudgetDto objbudgetDto = new BudgetDto();

					objbudgetDto.setLocationId(rest.getString("LOCATION_ID"));
					objbudgetDto.setStoreBudget(rest.getDouble("TOT_REVENUE"));
					if (methodName.equalsIgnoreCase("QUARTER")) {
						objbudgetDto.setProductId(rest.getString("PRODUCT_ID"));
						objbudgetDto.setProductLevelId(rest
								.getString("PRODUCT_LEVEL_ID"));
						returnMap.put(objbudgetDto.getLocationId() + "_"
								+ objbudgetDto.getProductId() + "_"
								+ objbudgetDto.getProductLevelId(),
								objbudgetDto);
					} else if (methodName.equalsIgnoreCase("PERIOD")) {
						objbudgetDto.setCalendarId(rest.getInt("CALENDAR_ID"));
						returnMap.put(objbudgetDto.getLocationId() + "_"
								+ objbudgetDto.getCalendarId(), objbudgetDto);
					}

					else if (methodName.equalsIgnoreCase("YEAR")) {
						objbudgetDto.setProductId(rest.getString("PRODUCT_ID"));
						objbudgetDto.setProductLevelId(rest
								.getString("PRODUCT_LEVEL_ID"));
						returnMap.put(objbudgetDto.getLocationId() + "_"
								+ objbudgetDto.getProductId() + "_"
								+ objbudgetDto.getProductLevelId(),
								objbudgetDto);

					}

				}

			} catch (SQLException exe) {
				logger.error("Error While Fetching budget Details...... " + exe);
				throw new GeneralException("getbudgetDetails.....", exe);
			}
		} catch (Exception exe) {
			logger.error("Error While Fetching budget Details...... " + exe);
			throw new GeneralException("getbudgetDetails.....", exe);
		}
		return returnMap;
	}
		
		/*
		 *  Method used to get the district level budget data 
		 * Argument 1 : Connection 
		 * Argument 2 : processYear
		 * Argument 3 : calendarPeriod
		 * Argument 4 : methodName
		*/

	/**
	 * @param _conn
	 * @param _processYear
	 * @param startdate
	 * @param endDate
	 * @param calendarLevel
	 * @param locationLevelId
	 * @param methodName
	 * @param modeOfcalling 
	 * @return
	 * @throws GeneralException
	 */
	public HashMap<String, BudgetDto> getdistrictPeriodbudget(Connection _conn,
			int _processYear, Date startdate, Date endDate,
			String calendarLevel, int locationLevelId, String methodName,
			String modeOfcalling) throws GeneralException {

		HashMap<String, BudgetDto> returnMap = new HashMap<String, BudgetDto>();
		// Date Formatter

		StringBuffer sql = new StringBuffer();
		sql.append(" select sum(TOT_REVENUE) as TOT_REVENUE, DISTRICT_ID");
		sql.append(" ,A.PRODUCT_ID,A.PRODUCT_LEVEL_ID,sum(ID_TOT_REVENUE) as ID_TOT_REVENUE");
		if (methodName.equalsIgnoreCase("PERIOD"))
			sql.append(",A.CALENDAR_ID");

		sql.append(" from SALES_AGGR_BUDGET A , COMPETITOR_STORE b , RETAIL_CALENDAR c ");
		sql.append(" where B.DISTRICT_ID in (select ID from RETAIL_DISTRICT)");
		sql.append(" and A.LOCATION_ID  = B.COMP_STR_ID AND A.CALENDAR_ID = C.CALENDAR_ID ");
		sql.append(" AND C.CAL_YEAR=" + _processYear + " AND C.ROW_TYPE='"
				+ calendarLevel + "'");
		sql.append(" and A.LOCATION_LEVEL_ID=" + locationLevelId + "");

		if (modeOfcalling.equalsIgnoreCase("IDENTICAL")) {
			sql.append(" and b.OPEN_DATE < c.start_date-364");
		}

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
					"getdistrictPeriodbudget");
			while (result.next()) {
				BudgetDto objBudget = new BudgetDto();
				objBudget.setLocationId(result.getString("DISTRICT_ID"));
				objBudget.setStoreBudget(result.getDouble("TOT_REVENUE"));
				objBudget.setProductId(result.getString("PRODUCT_ID"));
				objBudget.setProductLevelId(result
						.getString("PRODUCT_LEVEL_ID"));
				objBudget
						.setIdenticalBudget(result.getDouble("ID_TOT_REVENUE"));

				if (methodName.equalsIgnoreCase("PERIOD")) {
					objBudget.setCalendarId(result.getInt("CALENDAR_ID"));
					returnMap.put(
							objBudget.getLocationId() + "_"
									+ objBudget.getCalendarId() + "_"
									+ objBudget.getProductId() + "_"
									+ objBudget.getProductLevelId(), objBudget);

				} else if (methodName.equalsIgnoreCase("QUARTER")
						|| methodName.equalsIgnoreCase("YEAR")) {

					returnMap.put(
							objBudget.getLocationId() + "_"
									+ objBudget.getProductId() + "_"
									+ objBudget.getProductLevelId(), objBudget);
				}

			}

		} catch (Exception exe) {
			logger.error(exe);
			throw new GeneralException("getdistrictPeriodbudget error", exe);
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
	public HashMap<String, BudgetDto> getregionPeriodbudget(Connection _conn,
			int _processYear, Date startDate, Date endDate,
			String calendarPeriod, int locationLevelId, String methodName)
			throws GeneralException {

		HashMap<String, BudgetDto> returnMap = new HashMap<String, BudgetDto>();
		SimpleDateFormat formatter = new SimpleDateFormat("dd-MM-yyyy");
		StringBuffer sql = new StringBuffer();
		sql.append(" select sum(TOT_REVENUE) as TOT_REVENUE, REGION_ID");
		sql.append(" ,A.PRODUCT_ID,A.PRODUCT_LEVEL_ID,sum(ID_TOT_REVENUE) as ID_TOT_REVENUE ");

		if (methodName.equalsIgnoreCase("PERIOD"))
			sql.append(" ,A.CALENDAR_ID");

		sql.append(" from SALES_AGGR_BUDGET A , RETAIL_DISTRICT b, RETAIL_CALENDAR c  ");
		sql.append("  where B.ID = A.LOCATION_ID AND LOCATION_LEVEL_ID=")
				.append(locationLevelId);
		sql.append("  AND A.CALENDAR_ID = C.CALENDAR_ID ");
		sql.append(" AND C.CAL_YEAR=").append(_processYear);
		sql.append(" AND C.ROW_TYPE='").append(calendarPeriod).append("'");
		sql.append(" and A.LOCATION_LEVEL_ID=").append(locationLevelId);

		if (methodName.equalsIgnoreCase("QUARTER")) {
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
				objBudget.setProductId(result.getString("PRODUCT_ID"));
				objBudget.setProductLevelId(result
						.getString("PRODUCT_LEVEL_ID"));
				objBudget
						.setIdenticalBudget(result.getDouble("ID_TOT_REVENUE"));

				if (methodName.equalsIgnoreCase("PERIOD")) {
					objBudget.setCalendarId(result.getInt("CALENDAR_ID"));
					returnMap.put(
							objBudget.getLocationId() + "_"
									+ objBudget.getCalendarId() + "_"
									+ objBudget.getProductId() + "_"
									+ objBudget.getProductLevelId(), objBudget);
				} else if (methodName.equalsIgnoreCase("QUARTER")
						|| methodName.equalsIgnoreCase("YEAR")) {
					returnMap.put(
							objBudget.getLocationId() + "_"
									+ objBudget.getProductId() + "_"
									+ objBudget.getProductLevelId(), objBudget);
				}

			}

		} catch (Exception exe) {
			logger.error(exe);
			throw new GeneralException("Region Forecast Error", exe);
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
	public HashMap<String, BudgetDto> getDivisionPeriodbudget(Connection _conn,
			int _processYear, Date startDate, Date endDate,
			String calendarPeriod, int locationLevelId, String methodName)
			throws GeneralException {

		HashMap<String, BudgetDto> returnMap = new HashMap<String, BudgetDto>();
		SimpleDateFormat formatter = new SimpleDateFormat("dd-MM-yyyy");
		StringBuffer sql = new StringBuffer();
		sql.append(" select sum(TOT_REVENUE) as TOT_REVENUE, DIVISION_ID");
		sql.append(" ,A.PRODUCT_ID,A.PRODUCT_LEVEL_ID,sum(ID_TOT_REVENUE) as ID_TOT_REVENUE");
		if (methodName.equalsIgnoreCase("PERIOD"))
			sql.append(" ,A.CALENDAR_ID");

		sql.append(" from SALES_AGGR_BUDGET A , RETAIL_REGION b, RETAIL_CALENDAR c  ");
		sql.append("  where B.ID = A.LOCATION_ID AND LOCATION_LEVEL_ID=")
				.append(locationLevelId);
		sql.append("  AND A.CALENDAR_ID = C.CALENDAR_ID ");
		sql.append(" AND C.CAL_YEAR=").append(_processYear);
		sql.append(" AND C.ROW_TYPE='").append(calendarPeriod).append("'");
		sql.append(" and A.LOCATION_LEVEL_ID=").append(locationLevelId);

		if (methodName.equalsIgnoreCase("QUARTER")) {
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
				objBudget.setProductId(result.getString("PRODUCT_ID"));
				objBudget.setProductLevelId(result
						.getString("PRODUCT_LEVEL_ID"));
				objBudget
						.setIdenticalBudget(result.getDouble("ID_TOT_REVENUE"));
				if (methodName.equalsIgnoreCase("PERIOD")) {
					objBudget.setCalendarId(result.getInt("CALENDAR_ID"));
					returnMap.put(
							objBudget.getLocationId() + "_"
									+ objBudget.getCalendarId() + "_"
									+ objBudget.getProductId() + "_"
									+ objBudget.getProductLevelId(), objBudget);
				} else if (methodName.equalsIgnoreCase("QUARTER")
						|| methodName.equalsIgnoreCase("YEAR")) {
					returnMap.put(
							objBudget.getLocationId() + "_"
									+ objBudget.getProductId() + "_"
									+ objBudget.getProductLevelId(), objBudget);
				}

			}

		} catch (Exception exe) {
			logger.error(exe);
			throw new GeneralException("Region Budget Error", exe);
		}

		return returnMap;

	}
	
	
	/**
	 * @param _conn
	 * @param _processYear
	 * @param rowType
	 * @param productLevelId
	 * @return
	 * @throws GeneralException
	 */
	public HashMap<String, String> getPreviousBudgetDetails(Connection _conn,
			int _processYear, String rowType, int productLevelId)
			throws GeneralException {

		HashMap<String, String> returnMap = new HashMap<String, String>();

		StringBuffer sql = new StringBuffer();
		sql.append(" select CALENDAR_ID, LOCATION_ID,PRODUCT_ID,PRODUCT_LEVEL_ID ");
		sql.append(" from SALES_AGGR_BUDGET");
		sql.append(" where CALENDAR_ID in ");
		sql.append(" (select CALENDAR_ID from RETAIL_CALENDAR where CAL_YEAR=")
				.append(_processYear);
		sql.append(" and ROW_TYPE='").append(rowType).append("'");
		sql.append(" )and PRODUCT_LEVEL_ID=").append(productLevelId);

		logger.debug("getPreviousBudgetDetails..." + sql.toString());

		try {
			CachedRowSet rs = PristineDBUtil.executeQuery(_conn, sql,
					"getPreviousBudgetDetails");

			while (rs.next()) {

				returnMap.put(
						rs.getInt("CALENDAR_ID") + "_"
								+ rs.getInt("LOCATION_ID") + "_"
								+ rs.getInt("PRODUCT_ID") + "_"
								+ rs.getString("PRODUCT_LEVEL_ID"), "");
			}

		} catch (Exception exe) {
			logger.error("getPreviousBudgetDetails...." + exe);
			throw new GeneralException("getPreviousBudgetDetails...." + exe);
		}

		return returnMap;
	}

	/*
	 * Delete the records in all levels....
	 * Table Name : SALES_AGGR_BUDGET
	 * Table Name : SALES_AGGR_BUDGET_CTD
	 *  
	 * 
	 */

	public void deletePreviousAggregation(Connection _conn, int _processYear)
			throws GeneralException {

		try {
			StringBuffer sql = new StringBuffer();

			/*sql.append(" delete from SALES_AGGR_BUDGET_CTD");
			sql.append(" where BUDGET_CTD_ID IN (SELECT BUDGET_CTD_ID");
			sql.append(" from SALES_AGGR_BUDGET where CALENDAR_ID IN(");
			sql.append(" select CALENDAR_ID from RETAIL_CALENDAR where");
			sql.append(" CAL_YEAR=").append(_processYear).append("))");

			logger.debug(" Delete Sql..." + sql.toString());

			PristineDBUtil.execute(_conn, sql, "deletePreviousAggregation");

			sql = new StringBuffer();*/

			sql.append(" delete from SALES_AGGR_BUDGET ");
			sql.append(" where CALENDAR_ID in (");
			sql.append(
					" select CALENDAR_ID from RETAIL_CALENDAR where CAL_YEAR=")
					.append(_processYear);
			sql.append(" and ROW_TYPE='" + Constants.CALENDAR_PERIOD
					+ "') and ");
			sql.append(" PRODUCT_ID is null");

			logger.debug(" Delete Sql..." + sql.toString());

			PristineDBUtil.execute(_conn, sql, "deletePreviousAggregation");

			sql = new StringBuffer();

			sql.append(" delete from SALES_AGGR_BUDGET");
			sql.append(" where CALENDAR_ID in (");
			sql.append(
					" select CALENDAR_ID from RETAIL_CALENDAR where CAL_YEAR=")
					.append(_processYear);
			sql.append(" and ROW_TYPE in ('" + Constants.CALENDAR_QUARTER
					+ "','" + Constants.CALENDAR_YEAR + "'))");

			logger.debug(" Delete Sql..." + sql.toString());

			PristineDBUtil.execute(_conn, sql, "deletePreviousAggregation");

			sql = new StringBuffer();

			sql.append(" delete from SALES_AGGR_BUDGET ");
			sql.append(" where CALENDAR_ID in (");
			sql.append(
					" select CALENDAR_ID from RETAIL_CALENDAR where CAL_YEAR=")
					.append(_processYear);
			sql.append(" and ROW_TYPE='P') and ");
			sql.append(" LOCATION_LEVEL_ID in(" + Constants.CHAIN_LEVEL_ID
					+ "," + Constants.DIVISION_LEVEL_ID
					+ "," + Constants.REGION_LEVEL_ID + ","
					+ Constants.DISTRICT_LEVEL_ID + ")");

			logger.debug(" Delete Sql..." + sql.toString());

			PristineDBUtil.execute(_conn, sql, "deletePreviousAggregation");

			PristineDBUtil.commitTransaction(_conn, "delete");
		} catch (GeneralException e) {
			throw new GeneralException(
					" Delete the Previous aggregation error", e);
		}
	}

	/**
	 * @param _conn
	 * @param _processYear
	 * @param startdate
	 * @param endDate
	 * @param calendarLevel
	 * @param methodName
	 * @param storeLevelId
	 * @return
	 * @throws GeneralException
	 */
	public HashMap<String, BudgetDto> getBudgetWOG(Connection _conn,
			int _processYear, Date startdate, Date endDate,
			String calendarLevel, String methodName, int storeLevelId)
			throws GeneralException {

		HashMap<String, BudgetDto> returnMap = new HashMap<String, BudgetDto>();
		try {
			StringBuffer sql = new StringBuffer();

			sql.append(" select SA.LOCATION_ID, sum(TOT_REVENUE_WG) as TOT_REVENUE");
			sql.append(" ,sum(TOT_REVENUE_WOG) as WOG");
			if (methodName.equalsIgnoreCase("QUARTER"))
				sql.append(" ,SA.PRODUCT_ID,SA.PRODUCT_LEVEL_ID");
			else if (methodName.equalsIgnoreCase("YEAR"))
				sql.append(" ,SA.PRODUCT_ID,SA.PRODUCT_LEVEL_ID");
			else if (methodName.equalsIgnoreCase("PERIOD")) {
				sql.append(" ,RC.CALENDAR_ID");

			}

			sql.append(" from SALES_AGGR_BUDGET SA");
			sql.append(" inner join RETAIL_CALENDAR RC on RC.CALENDAR_ID = SA.CALENDAR_ID");
			sql.append(" where RC.CAL_YEAR =").append(_processYear);
			sql.append(" and RC.ROW_TYPE='").append(calendarLevel).append("'");
			sql.append(" and SA.LOCATION_LEVEL_ID=").append(storeLevelId);
			sql.append(" and SA.PRODUCT_ID NOT IN(");
			sql.append(" select product_id from product_group");
			sql.append(" where add_on_type='GAS')");

			if (methodName.equalsIgnoreCase("QUARTER")) {
				sql.append(" and RC.CALENDAR_ID in(");
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
						"getbudgetDetails");

				while (rest.next()) {
					BudgetDto objbudgetDto = new BudgetDto();

					objbudgetDto.setLocationId(rest.getString("LOCATION_ID"));
					objbudgetDto.setStoreBudget(rest.getDouble("TOT_REVENUE"));
					objbudgetDto.setBudgetWog(rest.getDouble("WOG"));

					if (methodName.equalsIgnoreCase("QUARTER")) {
						objbudgetDto.setProductId(rest.getString("PRODUCT_ID"));
						objbudgetDto.setProductLevelId(rest
								.getString("PRODUCT_LEVEL_ID"));
						returnMap.put(objbudgetDto.getLocationId() + "_"
								+ objbudgetDto.getProductId() + "_"
								+ objbudgetDto.getProductLevelId(),
								objbudgetDto);
					} else if (methodName.equalsIgnoreCase("PERIOD")) {
						objbudgetDto.setCalendarId(rest.getInt("CALENDAR_ID"));
						returnMap.put(objbudgetDto.getLocationId() + "_"
								+ objbudgetDto.getCalendarId(), objbudgetDto);
					}

					else if (methodName.equalsIgnoreCase("YEAR")) {
						objbudgetDto.setProductId(rest.getString("PRODUCT_ID"));
						objbudgetDto.setProductLevelId(rest
								.getString("PRODUCT_LEVEL_ID"));
						returnMap.put(objbudgetDto.getLocationId() + "_"
								+ objbudgetDto.getProductId() + "_"
								+ objbudgetDto.getProductLevelId(),
								objbudgetDto);

					}

				}

			} catch (SQLException exe) {
				logger.error("Error While Fetching budget Details...... " + exe);
				throw new GeneralException("getbudgetDetails.....", exe);
			}
		} catch (Exception exe) {
			logger.error("Error While Fetching budget Details...... " + exe);
			throw new GeneralException("getbudgetDetails.....", exe);
		}
		return returnMap;
	}

	
	/*
	 * Get the Budget_ctd_id and Budget revenue from SALES_AGGR_BUDGET
	 *  
	 */
	
	public List<BudgetDto> getCtdRevenue(Connection _conn, int calendarId,
			int ii, String methodName, int ctdType) throws GeneralException {

		List<BudgetDto> returnList = new ArrayList<BudgetDto>();

		try {
			StringBuffer sql = new StringBuffer();

			if (methodName.equalsIgnoreCase("PERIOD")) {
				sql.append(" select BUDGET_CTD_ID,TOT_REVENUE,ID_TOT_REVENUE from SALES_AGGR_BUDGET");
				sql.append(" where CALENDAR_ID=").append(calendarId);
			} else if (methodName.equalsIgnoreCase("YEAR")) {
				sql.append(" select sum(TOT_REVENUE) as TOT_REVENUE,sum(ID_TOT_REVENUE) as ID_TOT_REVENUE");
				sql.append(" ,PRODUCT_ID,LOCATION_ID,LOCATION_LEVEL_ID,PRODUCT_LEVEL_ID");
				sql.append("  from SALES_AGGR_BUDGET");
				sql.append(" where CALENDAR_ID>=").append(calendarId - ii);
				sql.append(" and CALENDAR_ID <=").append(calendarId);
				sql.append(" GROUP BY PRODUCT_ID,LOCATION_ID,LOCATION_LEVEL_ID,PRODUCT_LEVEL_ID");

			}
			logger.debug(" Get Ctd Revenue Query..... " + sql.toString());

			CachedRowSet result = PristineDBUtil.executeQuery(_conn, sql,
					"GetCtdRevenue");

			while (result.next()) {

				BudgetDto objBudgetDto = new BudgetDto();

				if (methodName.equalsIgnoreCase("YEAR")) {

					objBudgetDto.setProcesslocationId(result
							.getInt("LOCATION_ID"));
					objBudgetDto.setProcesslocationLevelId(result
							.getInt("LOCATION_LEVEL_ID"));
					objBudgetDto.setProcessproductId(result
							.getInt("PRODUCT_ID"));
					objBudgetDto.setProcessproductLevelId(result
							.getInt("PRODUCT_LEVEL_ID"));

				} else if (methodName.equalsIgnoreCase("PERIOD")) {
					objBudgetDto.setBudgetCtdId(result.getInt("BUDGET_CTD_ID"));
				}

				objBudgetDto.setStoreBudget(result.getDouble("TOT_REVENUE"));
				objBudgetDto.setIdenticalBudget(result
						.getDouble("ID_TOT_REVENUE"));
				objBudgetDto.setCtdType(ctdType);

				returnList.add(objBudgetDto);

			}
		} catch (SQLException e) {
			logger.error(e);
			throw new GeneralException(" Error In Sql Level.... ", e);
		} catch (GeneralException e) {
			logger.error(e);
			throw new GeneralException(" Error In Method Level.... ", e);
		}
		return returnList;
	}


	/*
	 * Method used to insert the Ctd Values into SALES_AGGR_BUDGET
	 * Argument 1 : Connection
	 * Argument 2 : Period Calendar List
	 *  
	 */
	
	public boolean budgetCtdInsert(Connection _conn,
			List<BudgetDto> periodCtdList,
			HashMap<String, Integer> currentCtdMap) throws GeneralException {

		boolean CommitFlag = false;

		try {
			PreparedStatement psmt = _conn.prepareStatement(insertCtdSql());

			for (int ii = 0; ii < periodCtdList.size(); ii++) {

				BudgetDto objBudgetDto = periodCtdList.get(ii);

				if (currentCtdMap != null) {

					int ctdId = currentCtdMap.get(objBudgetDto
							.getProcessproductId()
							+ "_"
							+ objBudgetDto.getProcessproductLevelId()
							+ "_"
							+ objBudgetDto.getProcesslocationId()
							+ "_"
							+ objBudgetDto.getProcesslocationLevelId());

					psmt.setObject(1, ctdId);

				} else {
					psmt.setObject(1, objBudgetDto.getBudgetCtdId());
				}
				psmt.setObject(2, objBudgetDto.getCtdType());
				psmt.setObject(3, objBudgetDto.getStoreBudget());
				psmt.setObject(4, objBudgetDto.getIdenticalBudget());
				psmt.addBatch();
			}

			psmt.executeBatch();

			if (psmt.getUpdateCount() >= 0) {
				CommitFlag = true;
			}

			logger.info(" Update Counts.... " + psmt.getMaxRows());

		} catch (SQLException e) {
			logger.error(e);
			throw new GeneralException(" Error In Ctd Batch Update..... ", e);
		}

		return CommitFlag;
	}


	private String insertCtdSql() {
	 
		StringBuffer sql = new StringBuffer();
		
		sql.append(" insert into SALES_AGGR_BUDGET_CTD");
		sql.append(" (BUDGET_CTD_ID,CTD_TYPE,TOT_REVENUE,ID_TOT_REVENUE)");
		sql.append(" values(?,?,?,?)");
		
		return sql.toString();
	}


	public HashMap<String, Integer> getCurrentCtd(Connection _conn,
			int calendarId) throws GeneralException {

		HashMap<String, Integer> returnMap = new HashMap<String, Integer>();

		try {
			StringBuffer sql = new StringBuffer();

			sql.append(" select PRODUCT_ID,PRODUCT_LEVEL_ID,LOCATION_ID,LOCATION_LEVEL_ID,BUDGET_CTD_ID");
			sql.append(" from SALES_AGGR_BUDGET where calendar_id=").append(
					calendarId);

			CachedRowSet rst = PristineDBUtil.executeQuery(_conn, sql,
					"Get Current Ctd  Process....");

			while (rst.next()) {

				returnMap.put(
						rst.getInt("PRODUCT_ID") + "_"
								+ rst.getInt("PRODUCT_LEVEL_ID") + "_"
								+ rst.getInt("LOCATION_ID") + "_"
								+ rst.getInt("LOCATION_LEVEL_ID"),
						rst.getInt("BUDGET_CTD_ID"));

			}
		} catch (SQLException e) {
			logger.error(e);
			throw new GeneralException(" Ctd Process Error....", e);
		} catch (GeneralException e) {
			logger.error(e);
			throw new GeneralException(" Ctd Process Error....", e);

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
	public HashMap<String, BudgetDto> getChainPeriodbudget(Connection _conn,
			int _processYear, Date startDate, Date endDate,
			String calendarPeriod, int locationLevelId, String methodName , int chainId)
			throws GeneralException {

		HashMap<String, BudgetDto> returnMap = new HashMap<String, BudgetDto>();
		SimpleDateFormat formatter = new SimpleDateFormat("dd-MM-yyyy");
		StringBuffer sql = new StringBuffer();
		sql.append(" select sum(TOT_REVENUE) as TOT_REVENUE");
		sql.append(" ,A.PRODUCT_ID,A.PRODUCT_LEVEL_ID,sum(ID_TOT_REVENUE) as ID_TOT_REVENUE");
		if (methodName.equalsIgnoreCase("PERIOD"))
			sql.append(" ,A.CALENDAR_ID");

		sql.append(" from SALES_AGGR_BUDGET A , RETAIL_CALENDAR B  ");
		sql.append("  where A.CALENDAR_ID = B.CALENDAR_ID ");
		sql.append(" AND B.CAL_YEAR=").append(_processYear);
		sql.append(" AND B.ROW_TYPE='").append(calendarPeriod).append("'");
		sql.append(" and A.LOCATION_LEVEL_ID=").append(locationLevelId);

		if (methodName.equalsIgnoreCase("QUARTER")) {
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
					"getregionForecaste");
			while (result.next()) {

				BudgetDto objBudget = new BudgetDto();
				objBudget.setLocationId(String.valueOf(chainId));
				objBudget.setStoreBudget(result.getDouble("TOT_REVENUE"));
				objBudget.setProductId(result.getString("PRODUCT_ID"));
				objBudget.setProductLevelId(result
						.getString("PRODUCT_LEVEL_ID"));
				objBudget
						.setIdenticalBudget(result.getDouble("ID_TOT_REVENUE"));
				if (methodName.equalsIgnoreCase("PERIOD")) {
					objBudget.setCalendarId(result.getInt("CALENDAR_ID"));
					returnMap.put(
							objBudget.getLocationId() + "_"
									+ objBudget.getCalendarId() + "_"
									+ objBudget.getProductId() + "_"
									+ objBudget.getProductLevelId(), objBudget);
				} else if (methodName.equalsIgnoreCase("QUARTER")
						|| methodName.equalsIgnoreCase("YEAR")) {
					returnMap.put(
							objBudget.getLocationId() + "_"
									+ objBudget.getProductId() + "_"
									+ objBudget.getProductLevelId(), objBudget);
				}

			}

		} catch (Exception exe) {
			logger.error(exe);
			throw new GeneralException("Region Budget Error", exe);
		}

		return returnMap;

	}
	

}


