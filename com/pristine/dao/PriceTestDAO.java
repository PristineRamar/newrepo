package com.pristine.dao;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

import org.apache.log4j.Logger;

import com.pristine.dto.PriceTestDTO;
import com.pristine.dto.offermgmt.mwr.RecommendationInputDTO;
import com.pristine.exception.GeneralException;
import com.pristine.lookup.offermgmt.PriceTestStatusLookUp;
import com.pristine.util.Constants;
import com.pristine.util.PristineDBUtil;

public class PriceTestDAO {

	static Logger logger = Logger.getLogger("PriceTestDAO");

	public static final String GET_STORE_LIST = " SELECT LGR.CHILD_LOCATION_ID AS STORE_ID "
			+ "	FROM LOCATION_GROUP_RELATION LGR  "
			+ "	LEFT JOIN PR_PRICE_TEST_REQUEST  PR ON PR.SL_LEVEL_ID=LGR.LOCATION_LEVEL_ID AND PR.SL_LOCATION_ID=LGR.LOCATION_ID  "
			+ "	LEFT JOIN LOCATION_GROUP LG ON LG.LOCATION_LEVEL_ID =LGR.LOCATION_LEVEL_ID AND LG.LOCATION_ID=LGR.LOCATION_LEVEL_ID "
			+ "	WHERE PR.LOCATION_ID =? AND PR.LOCATION_LEVEL_ID=? 	AND PR.PRODUCT_LEVEL_ID=? AND PR.PRODUCT_ID=? AND PR.ACTIVE='Y'";

	public static final String SET_ZONE_FOR_STORE = "SELECT CS.PRICE_ZONE_ID AS PRICE_ZONE_ID, Z.ZONE_NUM AS ZONE_NUM FROM COMPETITOR_STORE CS "
			+ "LEFT JOIN RETAIL_PRICE_ZONE Z ON Z.PRICE_ZONE_ID = CS.PRICE_ZONE_ID  WHERE CS.COMP_STR_ID IN(%s)";

	public static final String GET_PRICE_TEST_REQUEST = "SELECT PR.PRICE_TEST_ID, PR.TEST_NAME, PR.LOCATION_LEVEL_ID, PR.LOCATION_ID, PR.PRODUCT_LEVEL_ID, "
			+ "  PR.LOCATION_LEVEL_ID,  PR.PRODUCT_ID,  TO_CHAR(PR.REQUESTED, 'MM/DD/YYYY HH24:MI') AS SUBMITTED_TIME,PR.PRICE_TEST_ID,  "
			+ "  PR.STATUS,  PR.ACTIVE , ZN.ZONE_NUM , PR.REQUESTED_BY FROM PR_PRICE_TEST_REQUEST PR  "
			+ "  LEFT JOIN RETAIL_PRICE_ZONE ZN  ON ZN.PRICE_ZONE_ID= PR.LOCATION_ID  "
			+ "  WHERE PR.ACTIVE ='Y' AND PR.STATUS= " + PriceTestStatusLookUp.REQUESTED.getPriceTestTypeLookupId()
			+ "  ORDER BY SUBMITTED_TIME ASC";

	public static final String UPDATE_PRICE_TEST_STATUS = "UPDATE PR_PRICE_TEST_REQUEST SET STATUS=? WHERE LOCATION_ID =? AND LOCATION_LEVEL_ID=? AND PRODUCT_ID=? AND PRODUCT_LEVEL_ID=? AND ACTIVE='Y' ";

	private static final String PRICE_CHECK_LIST_FOR_PRICE_TEST = "SELECT PCLI.ITEM_CODE AS ITEM_CODE  FROM PRICE_CHECK_LIST_ITEMS PCLI  "
			+ "LEFT JOIN PRICE_CHECK_LIST	PCL ON PCL.ID=PCLI.PRICE_CHECK_lIST_ID "
			+ " LEFT JOIN PR_PRICE_TEST_REQUEST PR  ON PR.PRICE_CHECK_LIST_ID=PCL.ID "
			+ "	WHERE PR.LOCATION_ID =? AND PR.LOCATION_LEVEL_ID=? 	AND PR.PRODUCT_LEVEL_ID=? AND PR.PRODUCT_ID=? AND PR.ACTIVE='Y' ";

	private static final String ZONE_STR_COUNT = "SELECT RPZ.PRICE_ZONE_ID ,COUNT(*) AS COUNT FROM RETAIL_PRICE_ZONE RPZ INNER JOIN COMPETITOR_STORE CS "
			+ "ON RPZ.PRICE_ZONE_ID = CS.PRICE_ZONE_ID GROUP BY  RPZ.PRICE_ZONE_ID";

	private static final String GET_ALL_ITEMS_FROM_RU = " SELECT ITEM_CODE FROM ITEM_LOOKUP WHERE ACTIVE_INDICATOR='Y'  AND ITEM_CODE IN ( "
			+ " SELECT CHILD_PRODUCT_ID FROM (SELECT CHILD_PRODUCT_ID,CHILD_PRODUCT_LEVEL_ID FROM PRODUCT_GROUP_RELATION_REC PGR  "
			+ "	 start with product_level_id = ?  and product_id = ? "
			+ " connect by  prior child_product_id = product_id  and  prior child_product_level_id = product_level_id  "
			+ " ) WHERE CHILD_PRODUCT_LEVEL_ID = 1)  ";

	public static final String GET_PRICE_TEST_STATUS = "SELECT ACTIVE FROM PR_PRICE_TEST_REQUEST "
			+ "	WHERE LOCATION_ID =? AND LOCATION_LEVEL_ID=? AND PRODUCT_ID=? AND PRODUCT_LEVEL_ID=? ";

	private static final String GET_PRICE_CHECKLIST_ID = "SELECT PRICE_CHECK_LIST_ID FROM PR_PRICE_TEST_REQUEST WHERE LOCATION_ID =? AND LOCATION_LEVEL_ID=? AND PRODUCT_ID=? AND PRODUCT_LEVEL_ID=? AND ACTIVE='Y'";

	public static final String GET_PRICE_TEST_STATUS_FOR_MODEL_BUILDING = "SELECT PR.PRICE_TEST_ID, PR.TEST_NAME, PR.LOCATION_LEVEL_ID, PR.LOCATION_ID, PR.PRODUCT_LEVEL_ID, "
			+ "  PR.LOCATION_LEVEL_ID,  PR.PRODUCT_ID,  TO_CHAR(PR.REQUESTED, 'MM/DD/YYYY HH24:MI') AS SUBMITTED_TIME,  "
			+ "  PR.STATUS,  PR.ACTIVE  FROM PR_PRICE_TEST_REQUEST PR  WHERE PR.ACTIVE ='Y' AND PR.STATUS="
			+ PriceTestStatusLookUp.DATA_AGGREGATION_COMPLETE.getPriceTestTypeLookupId()
			+ "  ORDER BY SUBMITTED_TIME ASC";

	public static final String GET_PRICE_TEST_STATUS_FOR_RECOMMENDATIONS = "SELECT PR.PRICE_TEST_ID, PR.TEST_NAME, PR.LOCATION_LEVEL_ID, PR.LOCATION_ID, PR.PRODUCT_LEVEL_ID, "
			+ "  PR.LOCATION_LEVEL_ID,  PR.PRODUCT_ID,  TO_CHAR(PR.REQUESTED, 'MM/DD/YYYY HH24:MI') AS SUBMITTED_TIME,  "
			+ "  PR.STATUS, PR.ACTIVE, PR.REQUESTED_BY FROM PR_PRICE_TEST_REQUEST PR  WHERE PR.ACTIVE ='Y' AND( PR.STATUS="
			+ PriceTestStatusLookUp.MODEL_BUILDING_COMPLETED.getPriceTestTypeLookupId() + " OR PR.STATUS="
			+ PriceTestStatusLookUp.NO_RECENT_MOVEMENT.getPriceTestTypeLookupId() + ")  ORDER BY SUBMITTED_TIME ASC";

	public List<Integer> getStoreIdsforStoreList(Connection conn, int locationLevelId, int locationId, int productId,
			int productLevelId) throws GeneralException {
		PreparedStatement statement = null;
		ResultSet resultSet = null;
		List<Integer> storeList = new ArrayList<Integer>();
		try {
			int counter = 0;

			statement = conn.prepareStatement(GET_STORE_LIST);
			statement.setInt(++counter, locationId);
			statement.setInt(++counter, locationLevelId);
			statement.setInt(++counter, productLevelId);
			statement.setInt(++counter, productId);

			resultSet = statement.executeQuery();
			while (resultSet.next()) {
				storeList.add(resultSet.getInt("STORE_ID"));
			}
		} catch (SQLException e) {
			logger.error("getStoreIdsforStoreList()-Error while executing GET_STORE_LIST");
			throw new GeneralException("Error while executing GET_STORE_LIST", e);
		} finally {
			PristineDBUtil.close(resultSet);
			PristineDBUtil.close(statement);
		}
		return storeList;
	}

	public HashMap<String, Integer> setZoneCount(Connection conn, List<Integer> storeSet) throws GeneralException {
		HashMap<String, Integer> StrCount = new HashMap<String, Integer>();
		int limitcount = 0;
		Set<Integer> storeSubset = new HashSet<Integer>();
		for (Integer store : storeSet) {
			storeSubset.add(store);
			limitcount++;
			if (limitcount > 0 && (limitcount % Constants.LIMIT_COUNT == 0)) {
				Object[] values = storeSubset.toArray();
				retrieveZoneCount(StrCount, values, conn);
				storeSubset.clear();
			}
		}
		if (storeSubset.size() > 0) {
			Object[] values = storeSubset.toArray();
			retrieveZoneCount(StrCount, values, conn);
			storeSubset.clear();

		}
		return StrCount;
	}

	private void retrieveZoneCount(HashMap<String, Integer> strCount, Object[] values, Connection connection)
			throws GeneralException {

		PreparedStatement statement = null;
		ResultSet resultSet = null;

		try {
			String sql = SET_ZONE_FOR_STORE;

			sql = String.format(sql, PristineDBUtil.preparePlaceHolders(values.length));
			logger.debug("SET_ZONE_FOR_STORE:" + sql);
			statement = connection.prepareStatement(sql);

			int counter = 0;
			counter = counter + values.length;
			PristineDBUtil.setValues(statement, 1, values);

			statement.setFetchSize(2000);
			resultSet = statement.executeQuery();

			while (resultSet.next()) {
				int count = 0;
				String zoneNum = resultSet.getInt("PRICE_ZONE_ID") + ";" + resultSet.getString("ZONE_NUM");

				if (strCount.containsKey(zoneNum)) {
					count = strCount.get(zoneNum);
					strCount.put(zoneNum, count + 1);

				} else
					strCount.put(zoneNum, count + 1);

			}
		} catch (SQLException e) {
			logger.error("Error while executing SET_ZONE_FOR_STORE " + e);
			throw new GeneralException("Exception in retrieveZoneCount()", e);
		} finally {
			PristineDBUtil.close(resultSet);
			PristineDBUtil.close(statement);
		}
	}

	/**
	 * 
	 * @param conn
	 * @return
	 * @throws GeneralException
	 */
	public List<PriceTestDTO> getPriceTestRequest(Connection conn) throws GeneralException {
		PreparedStatement statement = null;
		ResultSet resultSet = null;
		List<PriceTestDTO> priceTestList = new ArrayList<PriceTestDTO>();
		try {
			statement = conn.prepareStatement(GET_PRICE_TEST_REQUEST);
			logger.debug("getPriceTestRequest() query: " + GET_PRICE_TEST_REQUEST);
			resultSet = statement.executeQuery();
			while (resultSet.next()) {
				PriceTestDTO prTestDTO = new PriceTestDTO();
				if (resultSet.getString("ACTIVE").equalsIgnoreCase("Y")) {
					prTestDTO.setActive(true);
				}
				prTestDTO.setDate(resultSet.getString("SUBMITTED_TIME"));
				prTestDTO.setLocationId(resultSet.getInt("LOCATION_ID"));
				prTestDTO.setLocationLevelID(resultSet.getInt("LOCATION_LEVEL_ID"));
				prTestDTO.setPriceTestID(resultSet.getInt("PRICE_TEST_ID"));
				prTestDTO.setProductId(resultSet.getInt("PRODUCT_ID"));
				prTestDTO.setProductLevelID(resultSet.getInt("PRODUCT_LEVEL_ID"));
				prTestDTO.setRequestorName(resultSet.getString("REQUESTED_BY"));
				prTestDTO.setZoneNum(resultSet.getString("ZONE_NUM"));
				prTestDTO.setStatus(resultSet.getInt("STATUS"));
				priceTestList.add(prTestDTO);
			}
		} catch (SQLException e) {
			logger.error("getPriceTestRequest()-Error while executing GET_PRICE_TEST_REQUEST");
			throw new GeneralException("Error while executing GET_PRICE_TEST_REQUEST", e);

		} finally {
			PristineDBUtil.close(resultSet);
			PristineDBUtil.close(statement);
		}
		return priceTestList;

	}

	public void updatePriceTestStatus(Connection conn, int statusId, int locationId, int locationLevelId, int productId,
			int productLevelId) throws GeneralException {
		PreparedStatement statement = null;
		ResultSet resultSet = null;
		try {
			int counter = 0;

			statement = conn.prepareStatement(UPDATE_PRICE_TEST_STATUS);
			statement.setInt(++counter, statusId);
			statement.setInt(++counter, locationId);
			statement.setInt(++counter, locationLevelId);
			statement.setInt(++counter, productId);
			statement.setInt(++counter, productLevelId);

			resultSet = statement.executeQuery();

			PristineDBUtil.commitTransaction(conn, "Updating status of PriceTest");

		} catch (SQLException e) {
			PristineDBUtil.rollbackTransaction(conn, "Error Updating status of PriceTest");

			logger.error("updatePriceTestStatus()-Error while executing UPDATE_PRICE_TEST_STATUS");
			throw new GeneralException("Error while executing UPDATE_PRICE_TEST_STATUS", e);
		} finally {
			PristineDBUtil.close(resultSet);
			PristineDBUtil.close(statement);
		}

	}

	/**
	 * 
	 * @param conn
	 * @param locationLevelId
	 * @param locationId
	 * @param priceTestID
	 * @return
	 * @throws GeneralException
	 */
	public List<Integer> getPriceTestItemList(Connection conn, int locationLevelId, int locationId, int productId,
			int productLevelId) throws GeneralException {
		PreparedStatement statement = null;
		ResultSet resultSet = null;
		List<Integer> itemList = new ArrayList<Integer>();
		try {

			int counter = 0;
			
				logger.debug("getPriceTestItemList()- query to get all items from RU : " + GET_ALL_ITEMS_FROM_RU);
				statement = conn.prepareStatement(GET_ALL_ITEMS_FROM_RU);
				statement.setInt(++counter, productLevelId);
				statement.setInt(++counter, productId);

				resultSet = statement.executeQuery();
				while (resultSet.next()) {
					itemList.add(resultSet.getInt("ITEM_CODE"));
				}

		} catch (SQLException e) {
			logger.error("getPriceTestItemList()-Error while executing ITEM_LIST_FOR_PRICE_TEST" + e);
			throw new GeneralException("Error while executing ITEM_LIST_FOR_PRICE_TEST ", e);
		} finally {
			PristineDBUtil.close(resultSet);
			PristineDBUtil.close(statement);
		}

		return itemList;
	}

	public HashMap<String, String> getWeekDates(Connection conn, String startDate, String endDate)
			throws GeneralException {
		PreparedStatement statement = null;
		ResultSet resultSet = null;
		HashMap<String, String> dateMap = new HashMap<String, String>();
		try {

			StringBuffer sb = new StringBuffer();

			String appendedDate = "'" + startDate + "'";
			String appendedEDate = "'" + endDate + "'";

			sb.append(
					"SELECT TO_CHAR(START_DATE, 'MM/DD/YYYY') AS WEEK_START_DATE, TO_CHAR(END_DATE, 'MM/DD/YYYY') AS WEEK_END_DATE FROM RETAIL_CALENDAR WHERE ROW_TYPE='W' AND ");
			sb.append(" START_DATE >= to_date( " + appendedDate + " ,'MM/DD/YYYY')   AND START_DATE <=  to_date("
					+ appendedEDate + ",'MM/DD/YYYY')");
			logger.debug("date q:-" + sb.toString());

			statement = conn.prepareStatement(sb.toString());

			resultSet = statement.executeQuery();
			while (resultSet.next()) {
				dateMap.put(resultSet.getString("WEEK_START_DATE"), resultSet.getString("WEEK_END_DATE"));
			}
		} catch (SQLException e) {
			logger.error("getWeekDates()-Error while executing GET_WEEK_START_DATES" + e);
			throw new GeneralException("Error while executing GET_WEEK_START_DATES", e);
		} finally {
			PristineDBUtil.close(resultSet);
			PristineDBUtil.close(statement);
		}
		return dateMap;
	}

	/**
	 * 
	 * @param conn
	 * @return
	 */
	public HashMap<Integer, Integer> getZoneStrcount(Connection conn) {

		HashMap<Integer, Integer> zoneStrList = new HashMap<>();

		PreparedStatement statement = null;
		ResultSet resultSet = null;

		try {
			statement = conn.prepareStatement(ZONE_STR_COUNT);
			logger.debug("getZoneStrcount query : " + ZONE_STR_COUNT);
			resultSet = statement.executeQuery();
			while (resultSet.next()) {
				int zoneNum = resultSet.getInt("PRICE_ZONE_ID");
				int storeCount = resultSet.getInt("COUNT");

				zoneStrList.put(zoneNum, storeCount);

			}
		}

		catch (Exception e) {
			logger.error("Error while executing getZoneStrcount()" + e);
		}

		finally {
			PristineDBUtil.close(resultSet);
			PristineDBUtil.close(statement);
		}
		return zoneStrList;
	}

	/**
	 * 
	 * @param conn
	 * @param locationLevelId
	 * @param locationId
	 * @param priceTestID
	 * @return
	 * @throws GeneralException
	 */
	public int getCheckListId(Connection conn, int locationLevelId, int locationId, int productId, int productLevelId)
			throws GeneralException {
		PreparedStatement statement = null;
		ResultSet resultSet = null;
		int priceCheckListId = 0;
		try {

			int counter = 0;

			logger.debug("getPriceTestItemList()-query  to get pricechecklistID: " + GET_PRICE_CHECKLIST_ID);
			statement = conn.prepareStatement(GET_PRICE_CHECKLIST_ID);
			statement.setInt(++counter, locationId);
			statement.setInt(++counter, locationLevelId);
			statement.setInt(++counter, productId);
			statement.setInt(++counter, productLevelId);

			resultSet = statement.executeQuery();
			while (resultSet.next()) {
				priceCheckListId = resultSet.getInt("PRICE_CHECK_LIST_ID");
			}

		} catch (Exception e) {
			logger.error("Error while executing getZoneStrcount()" + e);
		}

		finally {
			PristineDBUtil.close(resultSet);
			PristineDBUtil.close(statement);
		}
		return priceCheckListId;
	}

	public String getPriceTestStatus(Connection conn, RecommendationInputDTO recommendationInputDTO) {

		PreparedStatement statement = null;
		ResultSet resultSet = null;
		String status = "";
		Set<String> statusArr = new HashSet<String>();
		try {

			int counter = 0;

			logger.debug("getPriceTestStatus()- query  to get status: " + GET_PRICE_TEST_STATUS);
			
			statement = conn.prepareStatement(GET_PRICE_TEST_STATUS);
			statement.setInt(++counter, recommendationInputDTO.getLocationId());
			statement.setInt(++counter, recommendationInputDTO.getLocationLevelId());
			statement.setInt(++counter, recommendationInputDTO.getProductId());
			statement.setInt(++counter, recommendationInputDTO.getProductLevelId());
			
			logger.debug("getPriceTestStatus()-paramater 1." + recommendationInputDTO.getLocationId() + ";2."
					+ recommendationInputDTO.getLocationLevelId() + ";3." + recommendationInputDTO.getProductId() + ";4."
					+ recommendationInputDTO.getProductLevelId());
			resultSet = statement.executeQuery();
			while (resultSet.next()) {
				statusArr.add(resultSet.getString("ACTIVE"));
			}

		} catch (Exception e) {
			logger.error("Error while executing getPriceTestStatus()" + e);
		}

		finally {
			PristineDBUtil.close(resultSet);
			PristineDBUtil.close(statement);
		}

		if (statusArr.contains("Y")) {
			status = "Y";
		} else
			status = "N";
		logger.info("getPriceTestStatus status "+ status);
		return status;
	}

	public int getCountOfRunningIMS(Connection conn) throws GeneralException {
		PreparedStatement statement = null;
		ResultSet resultSet = null;
		int countOfIMS = 0;
		try {

			StringBuilder sb = new StringBuilder();
			sb.append(" SELECT COUNT(*) AS COUNT FROM PR_PRICE_TEST_REQUEST ");
			sb.append(" WHERE ACTIVE='Y' AND STATUS = "
					+ PriceTestStatusLookUp.DATA_AGGREGATION_INPROGRESS.getPriceTestTypeLookupId());

			statement = conn.prepareStatement(sb.toString());

			resultSet = statement.executeQuery();
			while (resultSet.next()) {
				countOfIMS = resultSet.getInt("COUNT");
			}

		} catch (SQLException e) {
			throw new GeneralException(
					"getCountOfRunningRecommendations() - Error while getting the count of running recomendation", e);
		} finally {
			PristineDBUtil.close(statement);
		}
		return countOfIMS;
	}

	public List<PriceTestDTO> getPriceTestStatusForModelBuilding(Connection conn) throws GeneralException {
		PreparedStatement statement = null;
		ResultSet resultSet = null;
		List<PriceTestDTO> priceTestList = new ArrayList<PriceTestDTO>();
		try {
			statement = conn.prepareStatement(GET_PRICE_TEST_STATUS_FOR_MODEL_BUILDING);
			resultSet = statement.executeQuery();
			while (resultSet.next()) {
				PriceTestDTO prTestDTO = new PriceTestDTO();
				if (resultSet.getString("ACTIVE").equalsIgnoreCase("Y")) {
					prTestDTO.setActive(true);
				}
				prTestDTO.setDate(resultSet.getString("SUBMITTED_TIME"));
				prTestDTO.setLocationId(resultSet.getInt("LOCATION_ID"));
				prTestDTO.setLocationLevelID(resultSet.getInt("LOCATION_LEVEL_ID"));
				prTestDTO.setProductId(resultSet.getInt("PRODUCT_ID"));
				prTestDTO.setProductLevelID(resultSet.getInt("PRODUCT_LEVEL_ID"));
				prTestDTO.setStatus(resultSet.getInt("STATUS"));
				priceTestList.add(prTestDTO);
			}
		} catch (SQLException e) {
			logger.error("getPriceTestRequest()-Error while executing GET_PRICE_TEST_STATUS_FOR_MODEL_BUILDING");
			throw new GeneralException("Error while executing GET_PRICE_TEST_STATUS_FOR_MODEL_BUILDING", e);

		} finally {
			PristineDBUtil.close(resultSet);
			PristineDBUtil.close(statement);
		}
		return priceTestList;

	}

	/**
	 * 
	 * @param conn
	 * @return
	 * @throws GeneralException
	 */
	public int getCountOfRunningModels(Connection conn) throws GeneralException {
		PreparedStatement statement = null;
		ResultSet resultSet = null;
		int countOfIMS = 0;
		try {

			StringBuilder sb = new StringBuilder();
			sb.append(" SELECT COUNT(*) AS COUNT FROM PR_PRICE_TEST_REQUEST ");
			sb.append(" WHERE ACTIVE= 'Y' AND STATUS = "
					+ PriceTestStatusLookUp.MODEL_BUILDING_INPROGRESS.getPriceTestTypeLookupId());

			statement = conn.prepareStatement(sb.toString());

			resultSet = statement.executeQuery();
			while (resultSet.next()) {
				countOfIMS = resultSet.getInt("COUNT");
			}

		} catch (SQLException e) {
			logger.info("getCountOfRunningModels() Error while getting the count of running Model:  " + e);
			throw new GeneralException("getCountOfRunningModels() - Error while getting the count of running Models",
					e);
		} finally {
			PristineDBUtil.close(statement);
		}
		return countOfIMS;
	}

	public List<PriceTestDTO> getPriceTestStatusForRecommendation(Connection conn) throws GeneralException {
		PreparedStatement statement = null;
		ResultSet resultSet = null;
		List<PriceTestDTO> priceTestList = new ArrayList<PriceTestDTO>();
		try {
			statement = conn.prepareStatement(GET_PRICE_TEST_STATUS_FOR_RECOMMENDATIONS);
			resultSet = statement.executeQuery();
			while (resultSet.next()) {
				PriceTestDTO prTestDTO = new PriceTestDTO();
				if (resultSet.getString("ACTIVE").equalsIgnoreCase("Y")) {
					prTestDTO.setActive(true);
				}
				prTestDTO.setDate(resultSet.getString("SUBMITTED_TIME"));
				prTestDTO.setLocationId(resultSet.getInt("LOCATION_ID"));
				prTestDTO.setLocationLevelID(resultSet.getInt("LOCATION_LEVEL_ID"));
				prTestDTO.setProductId(resultSet.getInt("PRODUCT_ID"));
				prTestDTO.setProductLevelID(resultSet.getInt("PRODUCT_LEVEL_ID"));
				prTestDTO.setStatus(resultSet.getInt("STATUS"));
				prTestDTO.setRequestorName(resultSet.getString("REQUESTED_BY"));
				priceTestList.add(prTestDTO);
			}
		} catch (SQLException e) {
			logger.error(
					"getPriceTestStatusForRecommendation()-Error while executing GET_PRICE_TEST_STATUS_FOR_RECOMMENDATIONS");
			throw new GeneralException("Error while executing GET_PRICE_TEST_STATUS_FOR_RECOMMENDATIONS", e);

		} finally {
			PristineDBUtil.close(resultSet);
			PristineDBUtil.close(statement);
		}
		return priceTestList;

	}

	/**
	 * 
	 * @param conn
	 * @return
	 * @throws GeneralException
	 */
	public int getCountOfRunningrecommendations(Connection conn) throws GeneralException {
		PreparedStatement statement = null;
		ResultSet resultSet = null;
		int countOfIMS = 0;
		try {

			StringBuilder sb = new StringBuilder();
			sb.append(" SELECT COUNT(*) AS COUNT FROM PR_PRICE_TEST_REQUEST ");
			sb.append(" WHERE ACTIVE= 'Y' AND STATUS = "
					+ PriceTestStatusLookUp.RECOMMENDATION_IN_PROGRESS.getPriceTestTypeLookupId());

			statement = conn.prepareStatement(sb.toString());

			resultSet = statement.executeQuery();
			while (resultSet.next()) {
				countOfIMS = resultSet.getInt("COUNT");
			}

		} catch (SQLException e) {
			logger.info("getCountOfRunningrecommendations() Error while getting the count of running recommendations:  "
					+ e);
			throw new GeneralException(
					"getCountOfRunningrecommendations() Error while getting the count of running recommendations:", e);
		} finally {
			PristineDBUtil.close(statement);
		}
		return countOfIMS;
	}

}
