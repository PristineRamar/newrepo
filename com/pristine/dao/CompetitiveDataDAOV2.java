/*
 * Title: DAO to load Competitive Data for Price Index
 *
 *******************************************************************************
 * Modification History
 *------------------------------------------------------------------------------
 * Version		Date		Name			Remarks
 *------------------------------------------------------------------------------
 * Version 0.1	04/12/2012	Janani			Initial Version 
 * Version 0.2	04/26/2012	Janani			Changes for retrieving store to
 * 											item mapping
 * Version 0.3	05/18/2012	Janani			Process only items with pi analyze 
 * 											flag as Y in item lookup
 * Version 0.4	07/11/2012	Janani			New Approach for loading price data
 * 											due to tablespace issue
 * Version 0.5	01/08/2012	Janani			Fix the issue with 
 * 											retrieveStoreItemMap() trying to 
 * 											retrieve data for all items
 *******************************************************************************
 */

package com.pristine.dao;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

import javax.sql.rowset.CachedRowSet;

import org.apache.log4j.Logger;

import com.pristine.dto.CompetitiveDataDTO;
import com.pristine.dto.PriceZoneDTO;
import com.pristine.dto.StoreDTO;
import com.pristine.exception.GeneralException;
import com.pristine.util.Constants;
import com.pristine.util.PristineDBUtil;
import com.pristine.util.PropertyManager;

public class CompetitiveDataDAOV2 implements IDAO {

	private static Logger logger = Logger.getLogger("CompetitiveDataDAOV2");
	private String checkUser = PropertyManager.getProperty("DATALOAD.CHECK_USER_ID",
			Constants.EXTERNAL_PRICECHECK_USER);
	private String checkList = PropertyManager.getProperty("DATALOAD.CHECK_LIST_ID",
			Constants.EXTERNAL_PRICECHECK_LIST);

	private Connection connection = null;
	private static final int commitCount = Constants.LIMIT_COUNT;

	private static final String GET_STORES = "SELECT RPZ.ZONE_NUM, CS.COMP_STR_NO FROM COMPETITOR_STORE CS "
			+ "INNER JOIN RETAIL_PRICE_ZONE RPZ ON CS.PRICE_ZONE_ID = RPZ.PRICE_ZONE_ID "
			+ "WHERE COMP_CHAIN_ID = ? AND PI_AVAILABILITY = 'Y'";
	private static final String GET_STORE_ID = "SELECT COMP_STR_NO, COMP_STR_ID FROM COMPETITOR_STORE WHERE COMP_CHAIN_ID = ? AND PI_AVAILABILITY = 'Y'";
	private static final String GET_DEPARTMENTS = "SELECT ID FROM DEPARTMENT";
	private static final String GET_ALL_ITEM_CODES_FOR_A_DEPT = "SELECT ITEM_CODE FROM ITEM_LOOKUP WHERE DEPT_ID = ? AND ITEM_CODE IN "
			+ "(SELECT DISTINCT(ITEM_CODE) FROM RETAIL_PRICE_INFO)";
	private static final String GET_PI_ITEM_CODES_FOR_A_DEPT = "SELECT ITEM_CODE FROM ITEM_LOOKUP WHERE DEPT_ID = ? AND PI_ANALYZE_FLAG = 'Y' AND ITEM_CODE IN "
			+ "(SELECT DISTINCT(ITEM_CODE) FROM RETAIL_PRICE_INFO)";
	private static final String GET_SCHEDULE_ID = "SELECT SCHEDULE_ID FROM SCHEDULE WHERE "
			+ "COMP_STR_ID = ? AND START_DATE = TO_DATE (?, 'MM/dd/yyyy') "
			+ "AND END_DATE = TO_DATE (?, 'MM/dd/yyyy') AND PRICE_CHECK_LIST_ID = ?";
	private static final String INSERT_SCHEDULE_ID = "INSERT INTO SCHEDULE( SCHEDULE_ID, PRICE_CHECK_LIST_ID, PRICE_CHECKER_ID, "
			+ "CREATE_USER_ID, CREATE_DATETIME, COMP_STR_ID, START_DATE, END_DATE, "
			+ "CURRENT_STATUS, STATUS_CHG_DATE, NO_OF_ITEMS, CHECK_DATE ) VALUES "
			+ "(SCHEDULE_SEQ.NEXTVAL, (SELECT ID FROM PRICE_CHECK_LIST WHERE NAME = ?), "
			+ "?, ?, SYSDATE, ?, TO_DATE(?, 'MM/dd/yyyy'), TO_DATE(?, 'MM/dd/yyyy'), " + "2, SYSDATE, -1, SYSDATE)";
	private static final String GET_CHECK_DATA_ID = "SELECT CHECK_DATA_ID, SCHEDULE_ID, ITEM_CODE FROM COMPETITIVE_DATA WHERE (SCHEDULE_ID,ITEM_CODE) IN (%s)";
	private static final String GET_ALL_CHECK_DATA_ID = "SELECT CHECK_DATA_ID, ITEM_CODE FROM COMPETITIVE_DATA WHERE SCHEDULE_ID = ?";
	private static final String INSERT_COMPETITIVE_DATA = "INSERT INTO COMPETITIVE_DATA( CHECK_DATA_ID, SCHEDULE_ID, ITEM_CODE, "
			+ "REG_PRICE, REG_M_PACK, REG_M_PRICE, " + "SALE_PRICE, SALE_M_PACK, SALE_M_PRICE, "
			+ "ITEM_NOT_FOUND_FLG, PRICE_NOT_FOUND_FLG, PROMOTION_FLG, OUTSIDE_RANGE_IND, NEW_UOM, "
			+ "EFF_REG_START_DATE, EFF_SALE_START_DATE , EFF_SALE_END_DATE, CHECK_DATETIME, CREATE_DATETIME, IS_ZONE_PRICE_DIFF ) "
			+ "VALUES( COMP_CHECK_ID_SEQ.NEXTVAL, ?, ?, ?, ?, ? ,?, ?, ?, 'N', 'N', ?, ?, ?, "
			+ "TO_DATE(?, 'MM/dd/yyyy'), TO_DATE(?, 'MM/dd/yyyy'), TO_DATE(?, 'MM/dd/yyyy'), SYSDATE, SYSDATE, ?)";
	private static final String UPDATE_COMPETITIVE_DATA = "UPDATE COMPETITIVE_DATA SET REG_PRICE = ?, REG_M_PACK = ?, REG_M_PRICE = ?, "
			+ "SALE_PRICE = ?, SALE_M_PACK = ?, SALE_M_PRICE = ?, ITEM_NOT_FOUND_FLG = 'N', PRICE_NOT_FOUND_FLG = 'N', "
			+ "PROMOTION_FLG = ?, OUTSIDE_RANGE_IND = ?, NEW_UOM = ?, EFF_REG_START_DATE = TO_DATE(?, 'MM/dd/yyyy'), "
			+ "EFF_SALE_START_DATE = TO_DATE(?, 'MM/dd/yyyy'), EFF_SALE_END_DATE = TO_DATE(?, 'MM/dd/yyyy'), "
			+ "CHECK_DATETIME = SYSDATE, CREATE_DATETIME = SYSDATE, IS_ZONE_PRICE_DIFF = ? WHERE CHECK_DATA_ID = ?";
	private static final String RETRIEVE_STORE_ITEM_MAP_PI = "SELECT SIM.LEVEL_TYPE_ID, SIM.LEVEL_ID, SIM.ITEM_CODE FROM STORE_ITEM_MAP SIM, ITEM_LOOKUP IL "
			+ "WHERE IL.PI_ANALYZE_FLAG = 'Y' " + "AND IL.ITEM_CODE = SIM.ITEM_CODE " + "ORDER BY SIM.LEVEL_ID";
	private static final String RETRIEVE_STORE_ITEM_MAP = "SELECT SIM.LEVEL_TYPE_ID, SIM.LEVEL_ID, SIM.ITEM_CODE FROM STORE_ITEM_MAP SIM, ITEM_LOOKUP IL "
			+ "WHERE (IL.PI_ANALYZE_FLAG = 'N' OR IL.PI_ANALYZE_FLAG IS NULL) " + "AND IL.ITEM_CODE IN (%s) "
			+ "AND IL.ITEM_CODE = SIM.ITEM_CODE " + "ORDER BY SIM.LEVEL_ID";
	private static final String GET_STORE_INFO = "SELECT RPZ.ZONE_NUM, CS.COMP_STR_NO, CS.COMP_STR_ID, CS.DEPT1_ZONE_NO,  "
			+ "CS.DEPT2_ZONE_NO, CS.DEPT3_ZONE_NO, CS.PRICE_ZONE_ID FROM COMPETITOR_STORE CS "
			+ "INNER JOIN RETAIL_PRICE_ZONE RPZ ON CS.PRICE_ZONE_ID = RPZ.PRICE_ZONE_ID "
			+ "WHERE COMP_CHAIN_ID = ? AND COMP_STR_NO = ?";
	private static final String GET_ITEM_CODES = "SELECT DISTINCT(ITEM_CODE) FROM (" + " %12WEEKITEMS% "
			+ "SELECT DISTINCT(ITEM_CODE) FROM MOVEMENT_DAILY MD, COMPETITOR_STORE CS WHERE CS.COMP_STR_ID = ? "
			+ "AND MD.COMP_STR_NO = CS.COMP_STR_NO AND MD.ITEM_CODE <> 0 AND CALENDAR_ID IN "
			+ "(SELECT CALENDAR_ID FROM RETAIL_CALENDAR WHERE START_DATE >= TO_DATE(?,'MM/DD/YYYY') AND START_DATE <= TO_DATE(?,'MM/DD/YYYY') AND ROW_TYPE = 'D') "
			+ "UNION " + "SELECT DISTINCT(CD.ITEM_CODE) FROM SCHEDULE S, COMPETITIVE_DATA CD "
			+ "WHERE (S.COMP_STR_ID = (SELECT PRIMARY_COMP_STR_ID FROM STORE_SUMMARY WHERE STORE_ID=?) OR S.COMP_STR_ID = (SELECT SECONDARY_COMP_STR_ID_1 FROM STORE_SUMMARY WHERE STORE_ID=?)) "
			+ "AND S.START_DATE >= (TO_DATE(?,'MM/DD/YYYY') - INTERVAL '90' DAY) "
			+ "AND S.START_DATE <= TO_DATE(?,'MM/DD/YYYY') AND S.SCHEDULE_ID = CD.SCHEDULE_ID)";
	private static final String GET_ITEM_CODES_WEEKLY_MOV = "SELECT DISTINCT(ITEM_CODE) FROM (" + " %12WEEKITEMS% "
			+ "SELECT DISTINCT(CD.ITEM_CODE) FROM SCHEDULE S, COMPETITIVE_DATA CD "
			+ "WHERE (S.COMP_STR_ID = (SELECT PRIMARY_COMP_STR_ID FROM STORE_SUMMARY WHERE STORE_ID=?) "
			+ "OR S.COMP_STR_ID = (SELECT SECONDARY_COMP_STR_ID_1 FROM STORE_SUMMARY WHERE STORE_ID=?)) "
			+ "AND S.START_DATE >= (TO_DATE(?,'MM/DD/YYYY') - INTERVAL '90' DAY) "
			+ "AND S.START_DATE <= TO_DATE(?,'MM/DD/YYYY') AND S.SCHEDULE_ID = CD.SCHEDULE_ID)";

	private static final String GET_ITEM_CODES_SUBQUERY = "SELECT MW.ITEM_CODE FROM COMPETITIVE_DATA CD, MOVEMENT_WEEKLY MW, "
			+ "SCHEDULE S WHERE S.SCHEDULE_ID IN (SELECT SCHEDULE_ID FROM SCHEDULE "
			+ "WHERE START_DATE >= TO_DATE(?,'MM/DD/YYYY') AND END_DATE <= TO_DATE(?,'MM/DD/YYYY') "
			+ "AND COMP_STR_ID = ? ) AND S.SCHEDULE_ID = CD.SCHEDULE_ID "
			+ "AND CD.CHECK_DATA_ID = MW.CHECK_DATA_ID AND (MW.QUANTITY_REGULAR > 0 OR MW.QUANTITY_SALE > 0) "
			+ "UNION ";
	private static final String RETRIEVE_ITEMS_FOR_STORE = "SELECT ITEM_CODE FROM STORE_ITEM_MAP "
			+ "WHERE LEVEL_TYPE_ID = ? AND LEVEL_ID = ?";
	private static final String GET_ITEM_CODES_FOR_ZONE_Q1 = "SELECT ITEM_CODE, COMP_STR_NO, SUM(ABS(DECODE(QUANTITY,0,WEIGHT,QUANTITY))) AS QUANTITY FROM MOVEMENT_DAILY WHERE COMP_STR_NO = ? "
			+ "AND ITEM_CODE > 0 AND CALENDAR_ID IN  "
			+ "(SELECT CALENDAR_ID FROM RETAIL_CALENDAR WHERE START_DATE >= TO_DATE(?,'MM/DD/YYYY') AND START_DATE <= TO_DATE(?,'MM/DD/YYYY') AND ROW_TYPE = 'D') "
			+ "GROUP BY ITEM_CODE, COMP_STR_NO " + "ORDER BY ITEM_CODE ";
	private static final String GET_ITEM_CODES_FOR_ZONE_Q2 = "SELECT DISTINCT(CD.ITEM_CODE) FROM SCHEDULE S, COMPETITIVE_DATA CD "
			+ "WHERE (S.COMP_STR_ID = ? OR S.COMP_STR_ID = ? OR S.COMP_STR_ID = ? OR S.COMP_STR_ID = ?) "
			+ "AND S.START_DATE >= (TO_DATE(?,'MM/DD/YYYY') - INTERVAL '90' DAY) "
			+ "AND S.START_DATE <= TO_DATE(?,'MM/DD/YYYY') AND S.SCHEDULE_ID = CD.SCHEDULE_ID ";

	private static final String GET_ITEM_CODES_FOR_CHAIN_Q1 = "SELECT ITEM_CODE, COMP_STR_NO, SUM(ABS(DECODE(QUANTITY,0,WEIGHT,QUANTITY))) AS QUANTITY FROM MOVEMENT_DAILY WHERE COMP_STR_NO = ? "
			+ "AND ITEM_CODE > 0 AND CALENDAR_ID IN  "
			+ "(SELECT CALENDAR_ID FROM RETAIL_CALENDAR WHERE START_DATE >= TO_DATE(?,'MM/DD/YYYY') AND START_DATE <= TO_DATE(?,'MM/DD/YYYY') AND ROW_TYPE = 'D') "
			+ "GROUP BY ITEM_CODE, COMP_STR_NO " + "ORDER BY ITEM_CODE ";
	private static final String GET_ITEM_CODES_FOR_CHAIN_Q2 = "SELECT DISTINCT(CD.ITEM_CODE) FROM SCHEDULE S, COMPETITIVE_DATA CD "
			+ " WHERE S.START_DATE >= (TO_DATE(?,'MM/DD/YYYY') - INTERVAL '90' DAY) "
			+ "AND S.START_DATE <= TO_DATE(?,'MM/DD/YYYY') AND S.SCHEDULE_ID = CD.SCHEDULE_ID ";

	private static final String GET_AUTH_ITEM_CODE = "SELECT SIM.ITEM_CODE, CS.COMP_STR_NO FROM STORE_ITEM_MAP SIM,COMPETITOR_STORE CS, ITEM_LOOKUP IL"
			+ " WHERE CS.COMP_STR_ID = SIM.LEVEL_ID AND IL.ITEM_CODE = SIM.ITEM_CODE AND SIM.IS_AUTHORIZED = 'Y'"
			+ " AND IL.ACTIVE_INDICATOR ='Y' AND COMP_STR_NO = ? AND SIM.LEVEL_TYPE_ID = ?";

	/**
	 * Constructor
	 * 
	 * @param conn Connection
	 */
	public CompetitiveDataDAOV2(Connection conn) {
		connection = conn;
	}

	/**
	 * This method queries for store number and zone number for stores that are
	 * available for PI for a particular chain
	 * 
	 * @param chainId Chain ID
	 * @return HashMap
	 * @throws GeneralException
	 */
	public HashMap<String, String> getStoreNumbers(String chainId) throws GeneralException {
		logger.debug("Inside getStoreNumbers() of CompetitiveDataDAOV2");
		HashMap<String, String> storeNumberMap = new HashMap<String, String>();
		PreparedStatement statement = null;
		ResultSet resultSet = null;
		try {
			statement = connection.prepareStatement(GET_STORES);
			statement.setString(1, chainId);
			resultSet = statement.executeQuery();
			while (resultSet.next()) {
				storeNumberMap.put(resultSet.getString("COMP_STR_NO"), resultSet.getString("ZONE_NUM"));
			}
		} catch (SQLException e) {
			logger.error("Error while executing GET_STORES");
			throw new GeneralException("Error while executing GET_STORES", e);
		} finally {
			PristineDBUtil.close(resultSet);
			PristineDBUtil.close(statement);
		}
		return storeNumberMap;
	}

	/**
	 * This method queries for store number and store id for stores that are
	 * available for PI for a particular chain
	 * 
	 * @param chainId Chain Id
	 * @return HashMap
	 * @throws GeneralException
	 */
	public HashMap<String, Integer> getStoreIds(String chainId) throws GeneralException {
		logger.debug("Inside getStoreNumbers() of CompetitiveDataDAOV2");
		HashMap<String, Integer> storeNumberMap = new HashMap<String, Integer>();
		PreparedStatement statement = null;
		ResultSet resultSet = null;
		try {
			statement = connection.prepareStatement(GET_STORE_ID);
			statement.setString(1, chainId);
			resultSet = statement.executeQuery();
			while (resultSet.next()) {
				storeNumberMap.put(resultSet.getString("COMP_STR_NO"), resultSet.getInt("COMP_STR_ID"));
			}
		} catch (SQLException e) {
			logger.error("Error while executing GET_STORES");
			throw new GeneralException("Error while executing GET_STORES", e);
		} finally {
			PristineDBUtil.close(resultSet);
			PristineDBUtil.close(statement);
		}
		return storeNumberMap;
	}

	/**
	 * This method returns department id(s) from department table
	 * 
	 * @return List
	 * @throws GeneralException
	 */
	public List<Integer> getDepartments() throws GeneralException {
		logger.debug("Inside getDepartments() of CompetitiveDataDAOV2");
		List<Integer> deptLst = new ArrayList<Integer>();
		PreparedStatement statement = null;
		ResultSet resultSet = null;
		try {
			statement = connection.prepareStatement(GET_DEPARTMENTS);
			resultSet = statement.executeQuery();
			while (resultSet.next()) {
				deptLst.add(resultSet.getInt("ID"));
			}
		} catch (SQLException e) {
			logger.error("Error while executing GET_DEPARTMENTS");
			throw new GeneralException("Error while executing GET_DEPARTMENTS", e);
		} finally {
			PristineDBUtil.close(resultSet);
			PristineDBUtil.close(statement);
		}
		return deptLst;
	}

	/**
	 * This method returns item codes for items under given department
	 * 
	 * @param deptId Department Id
	 * @return Set
	 * @throws GeneralException
	 */
	public Set<String> getItemCodes(Integer deptId, boolean isPIAnalyzeItemsOnly) throws GeneralException {
		logger.debug("Inside getItemCodes() of CompetitiveDataDAOV2");
		Set<String> itemCodeSet = new HashSet<String>();
		PreparedStatement statement = null;
		ResultSet resultSet = null;
		try {
			if (isPIAnalyzeItemsOnly)
				statement = connection.prepareStatement(GET_PI_ITEM_CODES_FOR_A_DEPT);
			else
				statement = connection.prepareStatement(GET_ALL_ITEM_CODES_FOR_A_DEPT);
			statement.setInt(1, deptId);
			resultSet = statement.executeQuery();
			while (resultSet.next()) {
				itemCodeSet.add(resultSet.getString("ITEM_CODE"));
			}
		} catch (SQLException e) {
			logger.error("Error while executing GET_ITEM_CODES_FOR_A_DEPT");
			throw new GeneralException("Error while executing GET_ITEM_CODES_FOR_A_DEPT", e);
		} finally {
			PristineDBUtil.close(resultSet);
			PristineDBUtil.close(statement);
		}
		return itemCodeSet;
	}

	/**
	 * This method populates schedule id for every item in the list
	 * 
	 * @param strIdColln    Collection of stores that are available for PI
	 * @param weekStartDate Start date of week
	 * @param weekEndDate   End date of week
	 */
	public HashMap<Integer, Integer> populateScheduleId(Collection<Integer> strIdColln, String weekStartDate,
			String weekEndDate) throws GeneralException {
		logger.debug("Inside populateScheduleId() of CompetitiveDataDAOV2");

		HashMap<Integer, Integer> scheduleIdMap = new HashMap<Integer, Integer>();
		PreparedStatement statement = null;
		PreparedStatement insertStatement = null;
		ResultSet resultSet = null;
		CompetitiveDataDAO compDataDAO = new CompetitiveDataDAO(connection);
		try {
			int priceCheckListId = compDataDAO.getPriceCheckListID(checkList);
			statement = connection.prepareStatement(GET_SCHEDULE_ID);
			insertStatement = connection.prepareStatement(INSERT_SCHEDULE_ID);
			Integer scheduleId;
			for (Integer storeId : strIdColln) {
				scheduleId = -1;
				statement.setInt(1, storeId);
				statement.setString(2, weekStartDate);
				statement.setString(3, weekEndDate);
				statement.setInt(4, priceCheckListId);
				resultSet = statement.executeQuery();
				if (resultSet.next()) {
					scheduleId = resultSet.getInt("SCHEDULE_ID");
					scheduleIdMap.put(storeId, scheduleId);
				}
				if (scheduleId == -1) {
					resultSet.close();

					insertStatement.setString(1, checkList);
					insertStatement.setString(2, checkUser);
					insertStatement.setString(3, checkUser);
					insertStatement.setInt(4, storeId);
					insertStatement.setString(5, weekStartDate);
					insertStatement.setString(6, weekEndDate);
					insertStatement.executeUpdate();

					resultSet = statement.executeQuery();
					if (resultSet.next()) {
						scheduleId = resultSet.getInt("SCHEDULE_ID");
						scheduleIdMap.put(storeId, scheduleId);
					}
				}
			}
		} catch (SQLException e) {
			logger.error("Error while executing GET_SCHEDULE_ID");
			throw new GeneralException("Error while executing GET_SCHEDULE_ID", e);
		} finally {
			PristineDBUtil.close(resultSet);
			PristineDBUtil.close(statement);
			PristineDBUtil.close(insertStatement);
		}
		return scheduleIdMap;
	}

	/**
	 * This method populates check data Id
	 * 
	 * @param compDataMap Map containing schedule id and item code as key and its
	 *                    competitive data as value
	 */
	public void populateCheckDataId(HashMap<String, CompetitiveDataDTO> compDataMap) throws GeneralException {

		List<String> checkDataLst = new ArrayList<String>();
		int limitcount = 0;
		for (String checkDataStr : compDataMap.keySet()) {
			checkDataLst.add(checkDataStr);
			limitcount++;
			if (limitcount > 0 && (limitcount % CompetitiveDataDAOV2.commitCount == 0)) {
				Object[] values = checkDataLst.toArray();
				retrieveCheckDataId(compDataMap, values);
				checkDataLst.clear();
			}
		}
		if (checkDataLst.size() > 0) {
			Object[] values = checkDataLst.toArray();
			retrieveCheckDataId(compDataMap, values);
			checkDataLst.clear();
		}
	}

	/**
	 * This method retrieves check data Id from competitive data
	 * 
	 * @param compDataMap Map containing schedule id and item code as key and its
	 *                    competitive data as value
	 * @param values
	 * @throws GeneralException
	 */
	private void retrieveCheckDataId(HashMap<String, CompetitiveDataDTO> compDataMap, Object[] values)
			throws GeneralException {
		logger.debug("Inside retrieveCheckDataId() in CompetitiveDataDAOV2");
		PreparedStatement statement = null;
		ResultSet resultSet = null;
		try {
			statement = connection
					.prepareStatement(String.format(GET_CHECK_DATA_ID, preparePlaceHolders(values.length)));
			setValues(statement, values);
			resultSet = statement.executeQuery();
			String key = null;

			while (resultSet.next()) {
				key = resultSet.getString("SCHEDULE_ID") + "," + resultSet.getString("ITEM_CODE");
				CompetitiveDataDTO compData = compDataMap.get(key);
				compData.checkItemId = resultSet.getInt("CHECK_DATA_ID");
			}
		} catch (SQLException e) {
			logger.error("Error while executing GET_ITEM_CODE");
			throw new GeneralException("Error while executing GET_ITEM_CODE", e);
		} finally {
			PristineDBUtil.close(resultSet);
			PristineDBUtil.close(statement);
		}
	}

	public void populateCheckDataIdV2(HashMap<String, CompetitiveDataDTO> compDataMap, int scheduleId)
			throws GeneralException {
		PreparedStatement statement = null;
		ResultSet resultSet = null;
		try {
			statement = connection.prepareStatement(GET_ALL_CHECK_DATA_ID);
			statement.setInt(1, scheduleId);
			statement.setFetchSize(75000);
			resultSet = statement.executeQuery();
			String key = null;

			while (resultSet.next()) {
				key = scheduleId + "," + resultSet.getString("ITEM_CODE");
				CompetitiveDataDTO compData = compDataMap.get(key);
				if (compData != null)
					compData.checkItemId = resultSet.getInt("CHECK_DATA_ID");
			}
		} catch (SQLException e) {
			logger.error("Error while executing GET_ALL_CHECK_DATA_ID");
			throw new GeneralException("Error while executing GET_ALL_CHECK_DATA_ID", e);
		} finally {
			PristineDBUtil.close(resultSet);
			PristineDBUtil.close(statement);
		}
	}

	/**
	 * This method returns a String with specified number of bind parameters
	 * 
	 * @param length Number of bind parameters
	 * @return String
	 */
	public static String preparePlaceHolders(int length) {
		StringBuilder builder = new StringBuilder();
		for (int i = 0; i < length;) {
			builder.append("(?,?)");
			if (++i < length) {
				builder.append(",");
			}
		}
		return builder.toString();
	}

	/**
	 * This method is used when there are no other bind parameters to be set in
	 * Prepared Statement other than the ones present in the array.
	 * 
	 * @param preparedStatement PreparedStatement in which values need to be set
	 * @param values            Array of Values that needs to be set in the Prepared
	 *                          Statement
	 * @throws SQLException
	 */
	public static void setValues(PreparedStatement preparedStatement, Object... values) throws SQLException {
		String[] splitVal = null;
		int counter = 0;
		for (int i = 0; i < values.length * 2; i = i + 2) {
			splitVal = values[counter].toString().split(",");
			preparedStatement.setObject(i + 1, splitVal[0]);
			preparedStatement.setObject(i + 2, splitVal[1]);
			counter++;
		}
	}

	/**
	 * This method inserts records in competitive data table
	 * 
	 * @param toBeInsertedList List of records to be inserted
	 * @throws GeneralException
	 */
	public void insertCompetitiveData(List<CompetitiveDataDTO> toBeInsertedList) throws GeneralException {
		PreparedStatement statement = null;
		try {
			statement = connection.prepareStatement(INSERT_COMPETITIVE_DATA);

			int itemNoInBatch = 0;
			int recordCount = 0;
			for (CompetitiveDataDTO compData : toBeInsertedList) {
				int counter = 0;
				statement.setInt(++counter, compData.scheduleId);
				statement.setInt(++counter, compData.itemcode);
				statement.setFloat(++counter, compData.regPrice);
				statement.setInt(++counter, compData.regMPack);
				statement.setDouble(++counter, compData.regMPrice);
				statement.setFloat(++counter, compData.fSalePrice);
				statement.setInt(++counter, compData.saleMPack);
				statement.setDouble(++counter, compData.fSaleMPrice);
				statement.setString(++counter, compData.saleInd);
				statement.setString(++counter, compData.outSideRangeInd);
				statement.setString(++counter, compData.newUOM);
				statement.setString(++counter, compData.effRegRetailStartDate);
				statement.setString(++counter, compData.effSaleStartDate);
				statement.setString(++counter, compData.effSaleEndDate);
				if (compData.isZonePriceDiff)
					statement.setString(++counter, "Y");
				else
					statement.setString(++counter, "N");
				statement.addBatch();
				itemNoInBatch++;
				recordCount++;

				if (itemNoInBatch % Constants.BATCH_UPDATE_COUNT == 0) {
					long startTime = System.currentTimeMillis();
					int[] count = statement.executeBatch();
					long endTime = System.currentTimeMillis();
					logger.debug("Time taken for inserting a batch - " + (endTime - startTime) + "ms");
					statement.clearBatch();
					itemNoInBatch = 0;
				}

				if (recordCount % 10000 == 0) {
					connection.commit();
				}
			}
			if (itemNoInBatch > 0) {
				int[] count = statement.executeBatch();
				connection.commit();
				statement.clearBatch();
			}
		} catch (SQLException e) {
			logger.error("Error while executing INSERT_COMPETITIVE_DATA");
			throw new GeneralException("Error while executing INSERT_COMPETITIVE_DATA", e);
		} finally {
			PristineDBUtil.close(statement);
		}
	}

	/**
	 * This method updates records in competitive data table
	 * 
	 * @param toBeUpdatedList List of records to be updated
	 * @throws GeneralException
	 */
	public void updateCompetitiveData(List<CompetitiveDataDTO> toBeUpdatedList) throws GeneralException {
		PreparedStatement statement = null;
		try {
			statement = connection.prepareStatement(UPDATE_COMPETITIVE_DATA);

			int itemNoInBatch = 0;
			for (CompetitiveDataDTO compData : toBeUpdatedList) {
				int counter = 0;
				statement.setFloat(++counter, compData.regPrice);
				statement.setInt(++counter, compData.regMPack);
				statement.setDouble(++counter, compData.regMPrice);
				statement.setFloat(++counter, compData.fSalePrice);
				statement.setInt(++counter, compData.saleMPack);
				statement.setDouble(++counter, compData.fSaleMPrice);
				statement.setString(++counter, compData.saleInd);
				statement.setString(++counter, compData.outSideRangeInd);
				statement.setString(++counter, compData.newUOM);
				statement.setString(++counter, compData.effRegRetailStartDate);
				statement.setString(++counter, compData.effSaleStartDate);
				statement.setString(++counter, compData.effSaleEndDate);
				if (compData.isZonePriceDiff)
					statement.setString(++counter, "Y");
				else
					statement.setString(++counter, "N");
				// Where clause
				statement.setInt(++counter, compData.checkItemId);
				statement.addBatch();
				itemNoInBatch++;

				if (itemNoInBatch % Constants.BATCH_UPDATE_COUNT == 0) {
					int[] count = statement.executeBatch();
					statement.clearBatch();
					itemNoInBatch = 0;
				}
			}
			if (itemNoInBatch > 0) {
				int[] count = statement.executeBatch();
				statement.clearBatch();
			}
		} catch (SQLException e) {
			logger.error("Error while executing UPDATE_COMPETITIVE_DATA");
			throw new GeneralException("Error while executing UPDATE_COMPETITIVE_DATA", e);
		} finally {
			PristineDBUtil.close(statement);
		}
	}

	/**
	 * This method retrieves store item mapping from store_item_map table
	 * 
	 * @return HashMap containing store/zone# and list of items available in it as
	 *         its value
	 */
	public HashMap<String, List<String>> retrieveStoreItemMap() throws GeneralException {
		HashMap<String, List<String>> storeItemMap = new HashMap<String, List<String>>();
		List<String> itemList = new ArrayList<String>();
		PreparedStatement statement = null;
		ResultSet resultSet = null;
		try {
			statement = connection.prepareStatement(RETRIEVE_STORE_ITEM_MAP_PI);
			resultSet = statement.executeQuery();
			int levelTypeId = -1;
			String levelId = null;
			String itemCode = null;
			String key = null;
			while (resultSet.next()) {
				levelTypeId = resultSet.getInt("LEVEL_TYPE_ID");
				levelId = resultSet.getString("LEVEL_ID");
				itemCode = resultSet.getString("ITEM_CODE");
				key = levelTypeId + Constants.INDEX_DELIMITER + levelId;
				if (storeItemMap.containsKey(key)) {
					itemList = storeItemMap.get(key);
					itemList.add(itemCode);
				} else {
					itemList = new ArrayList<String>();
					itemList.add(itemCode);
				}
				storeItemMap.put(key, itemList);
			}
		} catch (SQLException e) {
			logger.error("Error while executing RETRIEVE_STORE_ITEM_MAP");
			throw new GeneralException("Error while executing RETRIEVE_STORE_ITEM_MAP", e);
		} finally {
			PristineDBUtil.close(resultSet);
			PristineDBUtil.close(statement);
		}
		return storeItemMap;
	}

	public HashMap<String, List<String>> retrieveStoreItemMapForItems(Set<String> itemCodeSet) throws GeneralException {
		int limitcount = 0;
		List<String> itemCodeList = new ArrayList<String>();

		HashMap<String, List<String>> storeItemMap = new HashMap<String, List<String>>();
		for (String itemCode : itemCodeSet) {
			itemCodeList.add(itemCode);
			limitcount++;
			if (limitcount > 0 && (limitcount % CompetitiveDataDAOV2.commitCount == 0)) {
				Object[] values = itemCodeList.toArray();
				getStoreItemMap(storeItemMap, values);
				itemCodeList.clear();
			}
		}
		if (itemCodeList.size() > 0) {
			Object[] values = itemCodeList.toArray();
			getStoreItemMap(storeItemMap, values);
			itemCodeList.clear();
		}

		return storeItemMap;
	}

	private void getStoreItemMap(HashMap<String, List<String>> storeItemMap, Object[] values) throws GeneralException {
		logger.debug("Inside getStoreItemMap() of CompetitiveDataDAOV2");
		List<String> itemList = new ArrayList<String>();
		PreparedStatement statement = null;
		ResultSet resultSet = null;
		try {
			statement = connection.prepareStatement(
					String.format(RETRIEVE_STORE_ITEM_MAP, PristineDBUtil.preparePlaceHolders(values.length)));
			PristineDBUtil.setValues(statement, values);
			resultSet = statement.executeQuery();
			int levelTypeId = -1;
			String levelId = null;
			String itemCode = null;
			String key = null;
			while (resultSet.next()) {
				levelTypeId = resultSet.getInt("LEVEL_TYPE_ID");
				levelId = resultSet.getString("LEVEL_ID");
				itemCode = resultSet.getString("ITEM_CODE");
				key = levelTypeId + Constants.INDEX_DELIMITER + levelId;
				if (storeItemMap.containsKey(key)) {
					itemList = storeItemMap.get(key);
					itemList.add(itemCode);
				} else {
					itemList = new ArrayList<String>();
					itemList.add(itemCode);
				}
				storeItemMap.put(key, itemList);
			}
		} catch (SQLException e) {
			logger.error("Error while executing RETRIEVE_STORE_ITEM_MAP");
			throw new GeneralException("Error while executing RETRIEVE_STORE_ITEM_MAP", e);
		} finally {
			PristineDBUtil.close(resultSet);
			PristineDBUtil.close(statement);
		}
	}

	/**
	 * This method retrieves store information for a given store number and chain
	 * 
	 * @param chainId  Chain Id
	 * @param storeNum String Number
	 * @return storeDTO StoreDTO containing Store Information
	 */
	public StoreDTO getStoreInfo(String chainId, String storeNum) throws GeneralException {
		PreparedStatement statement = null;
		ResultSet resultSet = null;

		StoreDTO storeDTO = null;
		try {
			statement = connection.prepareStatement(GET_STORE_INFO);
			statement.setString(1, chainId);
			statement.setString(2, storeNum);
			resultSet = statement.executeQuery();
			while (resultSet.next()) {
				storeDTO = new StoreDTO();
				storeDTO.strId = resultSet.getInt("COMP_STR_ID");
				storeDTO.strNum = resultSet.getString("COMP_STR_NO");
				storeDTO.zoneNum = resultSet.getString("ZONE_NUM");
				storeDTO.zoneId = resultSet.getInt("PRICE_ZONE_ID");
				storeDTO.dept1ZoneNum = resultSet.getString("DEPT1_ZONE_NO");
				storeDTO.dept2ZoneNum = resultSet.getString("DEPT2_ZONE_NO");
				storeDTO.dept3ZoneNum = resultSet.getString("DEPT3_ZONE_NO");
			}
		} catch (SQLException e) {
			logger.error("Error while executing GET_STORE_INFO");
			throw new GeneralException("Error while executing GET_STORE_INFO", e);
		} finally {
			PristineDBUtil.close(resultSet);
			PristineDBUtil.close(statement);
		}
		return storeDTO;
	}

	/**
	 * Get list of items to load price/cost data
	 * 
	 * @param strId                Store Id
	 * @param weekStartDate        Week start date
	 * @param weekEndDate          Week end date
	 * @param pastWeekStartDate    Week 12 start date
	 * @param includePastWeekItems If 12 week items needs to be included or not
	 * @param movFrequency         Movement frequency (DAILY/WEEKLY)
	 * @return itemCodeSet Set of item codes
	 */
	public Set<String> getItemCodes(int strId, String weekStartDate, String weekEndDate, String pastWeekStartDate,
			boolean includePastWeekItems, String movFrequency) throws GeneralException {
		PreparedStatement statement = null;
		ResultSet resultSet = null;

		Set<String> itemCodeSet = new HashSet<String>();
		int counter = 0;
		String sql = "";
		if (movFrequency.equals(Constants.WEEKLY))
			sql = GET_ITEM_CODES_WEEKLY_MOV;
		else
			sql = GET_ITEM_CODES;

		try {
			if (includePastWeekItems) {
				sql = sql.replaceAll("%12WEEKITEMS%", GET_ITEM_CODES_SUBQUERY);
			} else {
				sql = sql.replaceAll("%12WEEKITEMS%", Constants.EMPTY);
			}

			statement = connection.prepareStatement(sql);
			if (includePastWeekItems) {
				statement.setString(++counter, pastWeekStartDate);
				statement.setString(++counter, weekEndDate);
				statement.setInt(++counter, strId);
			}
			if (!movFrequency.equals(Constants.WEEKLY)) {
				statement.setInt(++counter, strId);
				statement.setString(++counter, weekStartDate);
				statement.setString(++counter, weekEndDate);
			}
			statement.setInt(++counter, strId);
			statement.setInt(++counter, strId);
			statement.setString(++counter, weekEndDate);
			statement.setString(++counter, weekEndDate);

			resultSet = statement.executeQuery();
			while (resultSet.next()) {
				itemCodeSet.add(resultSet.getString("ITEM_CODE"));
			}
		} catch (SQLException e) {
			logger.error("Error while executing GET_ITEM_CODES");
			throw new GeneralException("Error while executing GET_ITEM_CODES", e);
		} finally {
			PristineDBUtil.close(resultSet);
			PristineDBUtil.close(statement);
		}
		return itemCodeSet;
	}

	/**
	 * This method retrieves items in a store for which we received price data
	 * 
	 * @param storeInfo DTo containing store information
	 * @return itemCodeSet Set of item codes
	 * @throws GeneralException
	 */
	public Set<String> retrieveItemsForStore(StoreDTO storeInfo) throws GeneralException {
		PreparedStatement statement = null;
		ResultSet resultSet = null;

		Set<String> itemCodeSet = new HashSet<String>();
		try {

			statement = connection.prepareStatement(RETRIEVE_ITEMS_FOR_STORE);
			statement.setInt(1, Constants.STORE_LEVEL_TYPE_ID);
			statement.setInt(2, storeInfo.strId);
			resultSet = statement.executeQuery();
			CachedRowSet crs = PristineDBUtil.getCachedRowSet(resultSet);
			while (crs.next()) {
				itemCodeSet.add(crs.getString("ITEM_CODE"));
			}

			statement.setInt(1, Constants.ZONE_LEVEL_TYPE_ID);
			statement.setInt(2, storeInfo.zoneId);
			resultSet = statement.executeQuery();
			crs = PristineDBUtil.getCachedRowSet(resultSet);
			while (crs.next()) {
				itemCodeSet.add(crs.getString("ITEM_CODE"));
			}
			crs.close();
		} catch (SQLException e) {
			logger.error("Error while executing RETRIEVE_ITEMS_FOR_STORE");
			throw new GeneralException("Error while executing RETRIEVE_ITEMS_FOR_STORE", e);
		} finally {
			PristineDBUtil.close(statement);
		}
		return itemCodeSet;
	}

	/**
	 * Get list of items to load price/cost data
	 * 
	 * @param strId         Store Id
	 * @param weekStartDate Week start date
	 * @param weekEndDate   Week end date
	 * @return itemCodeSet Set of item codes
	 */
	public HashMap<String, HashMap<String, Integer>> getItemCodesForZone(PriceZoneDTO priceZoneDTO,
			String weekStartDate, String weekEndDate, HashMap<String, List<String>> authItemCodeMap)
			throws GeneralException {
		PreparedStatement statement = null;
		ResultSet resultSet = null;
		HashSet<String> distinctItemCode = new HashSet<String>();
		// To get all stores Active items
		if (!authItemCodeMap.isEmpty()) {
			authItemCodeMap.forEach((key, value) -> {
				value.forEach(itemCode -> {
					distinctItemCode.add(itemCode);
				});
			});
		}

		HashMap<String, HashMap<String, Integer>> itemMap = new HashMap<String, HashMap<String, Integer>>();
		int counter = 0;
		String sqlPart1 = GET_ITEM_CODES_FOR_ZONE_Q1;
		String sqlPart2 = GET_ITEM_CODES_FOR_ZONE_Q2;

		try {
			for (String compStrNo : priceZoneDTO.getCompStrNo()) {
				counter = 0;
				statement = connection.prepareStatement(sqlPart1);
				statement.setString(++counter, compStrNo);
				statement.setString(++counter, weekStartDate);
				statement.setString(++counter, weekEndDate);
				statement.setFetchSize(60000);
				resultSet = statement.executeQuery();
				while (resultSet.next()) {
					String itemCode = resultSet.getString("ITEM_CODE");
					// Get item code based on comp store no
					if (!authItemCodeMap.isEmpty()) {
						List<String> authItemCodes = authItemCodeMap.get(resultSet.getString("COMP_STR_NO"));
						// Process the item codes only if item code are authorized
						if (authItemCodes.contains(itemCode)) {
							if (itemMap.get(itemCode) != null) {
								HashMap<String, Integer> tempMap = itemMap.get(itemCode);
								tempMap.put(resultSet.getString("COMP_STR_NO"), resultSet.getInt("QUANTITY"));
								itemMap.put(itemCode, tempMap);
							} else {
								HashMap<String, Integer> tempMap = new HashMap<String, Integer>();
								tempMap.put(resultSet.getString("COMP_STR_NO"), resultSet.getInt("QUANTITY"));
								itemMap.put(itemCode, tempMap);
							}
						}
					} else {
						if (itemMap.get(itemCode) != null) {
							HashMap<String, Integer> tempMap = itemMap.get(itemCode);
							tempMap.put(resultSet.getString("COMP_STR_NO"), resultSet.getInt("QUANTITY"));
							itemMap.put(itemCode, tempMap);
						} else {
							HashMap<String, Integer> tempMap = new HashMap<String, Integer>();
							tempMap.put(resultSet.getString("COMP_STR_NO"), resultSet.getInt("QUANTITY"));
							itemMap.put(itemCode, tempMap);
						}
					}

				}
				statement.close();
				resultSet.close();
			}
			counter = 0;
			statement = connection.prepareStatement(sqlPart2);
			statement.setInt(++counter, priceZoneDTO.getPrimaryCompetitor());
			statement.setInt(++counter, priceZoneDTO.getSecComp1());
			statement.setInt(++counter, priceZoneDTO.getSecComp2());
			statement.setInt(++counter, priceZoneDTO.getSecComp3());
			statement.setString(++counter, weekEndDate);
			statement.setString(++counter, weekEndDate);
			statement.setFetchSize(60000);
			resultSet = statement.executeQuery();
			while (resultSet.next()) {
				String itemCode = resultSet.getString("ITEM_CODE");
				if (!distinctItemCode.isEmpty()) {
					if (itemMap.get(itemCode) == null && distinctItemCode.contains(itemCode)) {
						itemMap.put(itemCode, null);
					}
				} else if (itemMap.get(itemCode) == null) {
					// These are items which do not have movement but has competition data
					itemMap.put(itemCode, null);
				}
			}
		} catch (SQLException e) {
			logger.error("Error while executing GET_ITEM_CODES");
			throw new GeneralException("Error while executing GET_ITEM_CODES", e);
		} finally {
			PristineDBUtil.close(resultSet);
			PristineDBUtil.close(statement);
		}
		return itemMap;
	}

	/**
	 * Get list of items to load price/cost data
	 * 
	 * @param strId         Store Id
	 * @param weekStartDate Week start date
	 * @param weekEndDate   Week end date
	 * @return itemCodeSet Set of item codes
	 */
	public HashMap<String, HashMap<String, Integer>> getItemCodesForLocation(List<String> baseStores,
			String weekStartDate, String weekEndDate, HashMap<String, List<String>> authItemCodeMap)
			throws GeneralException {
		PreparedStatement statement = null;
		ResultSet resultSet = null;
		HashSet<String> distinctItemCode = new HashSet<String>();
		// To get all stores Active items
		if (!authItemCodeMap.isEmpty()) {
			authItemCodeMap.forEach((key, value) -> {
				value.forEach(itemCode -> {
					distinctItemCode.add(itemCode);
				});
			});
		}

		HashMap<String, HashMap<String, Integer>> itemMap = new HashMap<String, HashMap<String, Integer>>();
		int counter = 0;
		String sqlPart1 = GET_ITEM_CODES_FOR_CHAIN_Q1;
		String sqlPart2 = GET_ITEM_CODES_FOR_CHAIN_Q2;

		try {
			for (String compStrNo : baseStores) {
				counter = 0;
				statement = connection.prepareStatement(sqlPart1);
				statement.setString(++counter, compStrNo);
				statement.setString(++counter, weekStartDate);
				statement.setString(++counter, weekEndDate);
				statement.setFetchSize(60000);
				resultSet = statement.executeQuery();
				while (resultSet.next()) {
					String itemCode = resultSet.getString("ITEM_CODE");
					// Get item code based on comp store no
					if (!authItemCodeMap.isEmpty()) {
						List<String> authItemCodes = authItemCodeMap.get(resultSet.getString("COMP_STR_NO"));
						// Process the item codes only if item code are authorized
						if (authItemCodes.contains(itemCode)) {
							if (itemMap.get(itemCode) != null) {
								HashMap<String, Integer> tempMap = itemMap.get(itemCode);
								tempMap.put(resultSet.getString("COMP_STR_NO"), resultSet.getInt("QUANTITY"));
								itemMap.put(itemCode, tempMap);
							} else {
								HashMap<String, Integer> tempMap = new HashMap<String, Integer>();
								tempMap.put(resultSet.getString("COMP_STR_NO"), resultSet.getInt("QUANTITY"));
								itemMap.put(itemCode, tempMap);
							}
						}
					} else {
						if (itemMap.get(itemCode) != null) {
							HashMap<String, Integer> tempMap = itemMap.get(itemCode);
							tempMap.put(resultSet.getString("COMP_STR_NO"), resultSet.getInt("QUANTITY"));
							itemMap.put(itemCode, tempMap);
						} else {
							HashMap<String, Integer> tempMap = new HashMap<String, Integer>();
							tempMap.put(resultSet.getString("COMP_STR_NO"), resultSet.getInt("QUANTITY"));
							itemMap.put(itemCode, tempMap);
						}
					}

				}
				statement.close();
				resultSet.close();
			}
			counter = 0;
			statement = connection.prepareStatement(sqlPart2);
			/*
			 * statement.setInt(++counter, priceZoneDTO.getPrimaryCompetitor());
			 * statement.setInt(++counter, priceZoneDTO.getSecComp1());
			 * statement.setInt(++counter, priceZoneDTO.getSecComp2());
			 * statement.setInt(++counter, priceZoneDTO.getSecComp3());
			 */
			statement.setString(++counter, weekEndDate);
			statement.setString(++counter, weekEndDate);
			statement.setFetchSize(60000);
			resultSet = statement.executeQuery();
			while (resultSet.next()) {
				String itemCode = resultSet.getString("ITEM_CODE");
				if (!distinctItemCode.isEmpty()) {
					if (itemMap.get(itemCode) == null && distinctItemCode.contains(itemCode)) {
						itemMap.put(itemCode, null);
					}
				} else if (itemMap.get(itemCode) == null) {
					// These are items which do not have movement but has competition data
					itemMap.put(itemCode, null);
				}
			}
		} catch (SQLException e) {
			logger.error("Error while executing GET_ITEM_CODES");
			throw new GeneralException("Error while executing GET_ITEM_CODES", e);
		} finally {
			PristineDBUtil.close(resultSet);
			PristineDBUtil.close(statement);
		}
		return itemMap;
	}

	public HashMap<String, List<String>> getAuthItemCode(List<String> compStrNos) throws GeneralException {
		PreparedStatement statement = null;
		ResultSet resultSet = null;

		HashMap<String, List<String>> itemCodeMap = new HashMap<String, List<String>>();
		try {
			for (String storeNo : compStrNos) {
				String query = new String(GET_AUTH_ITEM_CODE);
				logger.debug("To get Auth item code from Store item Map: " + query);
				statement = connection.prepareStatement(query);
				statement.setString(1, storeNo);
				statement.setInt(2, Constants.STORE_LEVEL_TYPE_ID);
				resultSet = statement.executeQuery();
				CachedRowSet crs = PristineDBUtil.getCachedRowSet(resultSet);
				while (crs.next()) {
					List<String> itemCodeList = new ArrayList<String>();
					if (itemCodeMap.containsKey(crs.getString("COMP_STR_NO"))) {
						itemCodeList = itemCodeMap.get(crs.getString("COMP_STR_NO"));
					}
					itemCodeList.add(crs.getString("ITEM_CODE"));
					itemCodeMap.put(crs.getString("COMP_STR_NO"), itemCodeList);
				}
			}
		} catch (SQLException e) {
			logger.error("Error while executing getAuthItemCode");
			throw new GeneralException("Error while executing getAuthItemCode", e);
		} finally {
			PristineDBUtil.close(resultSet);
			PristineDBUtil.close(statement);
		}
		return itemCodeMap;
	}
}