
package com.pristine.dao;

import java.sql.BatchUpdateException;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.ListIterator;
import java.util.Map;
import java.util.Set;

import javax.sql.rowset.CachedRowSet;

import org.apache.log4j.Logger;

import com.pristine.dto.CompStoreItemDetailsKey;
import com.pristine.dto.CompetitiveDataDTO;
import com.pristine.dto.ItemDTO;
import com.pristine.dto.PriceSuggestionDTO;
import com.pristine.dto.StoreComparisonDTO;
import com.pristine.exception.GeneralException;
import com.pristine.util.Constants;
import com.pristine.util.ConstantsSI;
import com.pristine.util.GenericUtil;
import com.pristine.util.NumberUtil;
import com.pristine.util.PrestoUtil;
import com.pristine.util.PristineDBUtil;
import com.pristine.util.PropertyManager;

public class CompetitiveDataDAO implements IDAO {

	private static Logger logger = Logger.getLogger("CompetitiveDataDAO");

	private String checkUser = PropertyManager.getProperty("DATALOAD.CHECK_USER_ID",
			Constants.EXTERNAL_PRICECHECK_USER);
	private String checkList = PropertyManager.getProperty("DATALOAD.CHECK_LIST_ID",
			Constants.EXTERNAL_PRICECHECK_LIST);

	private HashSet<Integer> schedulesProcessed = new HashSet<Integer>();
	private HashMap<String, ArrayList<String>> storeIdMap = new HashMap<String, ArrayList<String>>();
	private HashMap<String, Integer> scheduleIdMap = new HashMap<String, Integer>();

	// Janani - 6/6/12 - Added this flag to include chain_id condition in
	// getStoreIdList() for computing movement weekly
	private boolean isSubscriber = false;
	private String chainId = null;

	private int priceCheckId = -1;

	private static final String INSERT_COMPETITIVE_DATA = "INSERT INTO COMPETITIVE_DATA( CHECK_DATA_ID, SCHEDULE_ID, ITEM_CODE, "
			+ "REG_PRICE, REG_M_PACK, REG_M_PRICE, " + "SALE_PRICE, SALE_M_PACK, SALE_M_PRICE, "
			+ "ITEM_NOT_FOUND_FLG, PRICE_NOT_FOUND_FLG, PROMOTION_FLG, OUTSIDE_RANGE_IND, NEW_UOM, "
			+ "EFF_REG_START_DATE, EFF_SALE_START_DATE , EFF_SALE_END_DATE, CHECK_DATETIME, CREATE_DATETIME, COMMENTS ) "
			+ "VALUES( COMP_CHECK_ID_SEQ.NEXTVAL, ?, ?, ?, ?, ? ,?, ?, ?, ?, ?, ?, ?, ?, "
			+ "TO_DATE(?, 'MM/dd/yyyy'), TO_DATE(?, 'MM/dd/yyyy'), TO_DATE(?, 'MM/dd/yyyy'), TO_DATE(?, 'MM/dd/yyyy'), SYSDATE, ?)";

	private static final String UPDATE_COMPETITIVE_DATA = "UPDATE COMPETITIVE_DATA SET REG_PRICE = ?, REG_M_PACK = ?, REG_M_PRICE = ?, "
			+ "SALE_PRICE = ?, SALE_M_PACK = ?, SALE_M_PRICE = ?, ITEM_NOT_FOUND_FLG = ?, PRICE_NOT_FOUND_FLG = ?, "
			+ "PROMOTION_FLG = ?, NEW_UOM = ?, EFF_REG_START_DATE = TO_DATE(?, 'MM/dd/yyyy'), "
			+ "EFF_SALE_START_DATE = TO_DATE(?, 'MM/dd/yyyy'), EFF_SALE_END_DATE = TO_DATE(?, 'MM/dd/yyyy'), "
			+ "CHECK_DATETIME =TO_DATE(?, 'MM/dd/yyyy'), CREATE_DATETIME = SYSDATE, COMMENTS = ? WHERE CHECK_DATA_ID = ?";

	private static final String INSERT_NOT_CARRIED_ITEMS = "INSERT INTO NOT_CARRIED_ITEMS(SCHEDULE_ID, UPC, ITEM_NAME, ITEM_SIZE,"
			+ " PACK, UOM_ID, REG_PRICE, REG_M_PACK, REG_M_PRICE,SALE_PRICE, SALE_M_PACK, SALE_M_PRICE,PROMOTION_FLG,"
			+ " CHECK_DATETIME, CREATE_DATETIME ) "
			+ " VALUES( ?, ?, ?, ?, ?, ? ,?, ?, ?, ?, ?, ?, ?, TO_DATE(?, 'MM/dd/yyyy'), SYSDATE )";

	private static final String GET_PRICECHECK_ID = "SELECT ID FROM PRICE_CHECK_LIST WHERE NAME = ?";

	private static final String UPDATE_COMPETITIVE_DATA_COMMENTS = "UPDATE COMPETITIVE_DATA SET COMMENTS = ? WHERE CHECK_DATA_ID = ?";

	private static final String GET_COMPETITION_DATA = "SELECT SCHEDULE_ID, CD.ITEM_CODE, REG_PRICE, REG_M_PACK, REG_M_PRICE, SALE_PRICE, SALE_M_PACK, SALE_M_PRICE,  "
			+ "ITEM_NOT_FOUND_FLG, PRICE_NOT_FOUND_FLG, PROMOTION_FLG, OUTSIDE_RANGE_IND, NEW_UOM,  "
			+ "TO_CHAR(EFF_REG_START_DATE, 'MM/DD/YYYY') EFF_REG_START_DATE, TO_CHAR(EFF_SALE_START_DATE, 'MM/DD/YYYY') EFF_SALE_START_DATE, "
			+ "TO_CHAR(EFF_SALE_END_DATE, 'MM/DD/YYYY') EFF_SALE_END_DATE, TO_CHAR(CHECK_DATETIME, 'MM/DD/YYYY') CHECK_DATE, COMMENTS, IL.UPC FROM "
			+ "COMPETITIVE_DATA CD LEFT JOIN ITEM_LOOKUP IL ON CD.ITEM_CODE = IL.ITEM_CODE "
			+ " WHERE SCHEDULE_ID = ? ";

	private static final String INSERT_INTO_RAW_COMPETITIVE_DATA = "INSERT INTO RAW_COMPETITIVE_DATA(COMP_STR_NO, WEEK_START_DATE, CHECK_DATETIME, ITEM_CODE,"
			+ " ITEM_DESC, REG_M_PACK, REG_PRICE, SIZE_UOM, UPC, OUTSIDE_RANGE_IND, ADDITIONAL_INFO, SALE_M_PACK, SALE_PRICE, SALE_START_DATE , PL_IND)"
			+ "VALUES(?, TO_DATE(?, 'MM/dd/yyyy'), TO_DATE(?, 'MM/dd/yyyy'),?,?,?,?,?,?,?,?,?,?,?,?)";

	private static final String INSERT_INTO_RAW_COMPETITIVE_DATA_TEST = "INSERT INTO RAW_COMPETITIVE_DATA_TEST(COMP_STR_NO, WEEK_START_DATE, CHECK_DATETIME, ITEM_CODE,"
			+ " ITEM_DESC, REG_M_PACK, REG_PRICE, SIZE_UOM, UPC, OUTSIDE_RANGE_IND, ADDITIONAL_INFO, SALE_M_PACK, SALE_PRICE, SALE_START_DATE , PL_IND)"
			+ "VALUES(?, TO_DATE(?, 'MM/dd/yyyy'), TO_DATE(?, 'MM/dd/yyyy'),?,?,?,?,?,?,?,?,?,?,?,?)";

	private static final String GET_COMP_DATA_FROM_RAW_COMPETITIVE_DATA = "SELECT * FROM RAW_COMPETITIVE_DATA_TEST  WHERE TO_DATE(WEEK_START_DATE,'MM/DD/YYYY') >= TO_DATE(?, '"
			+ Constants.DB_DATE_FORMAT + "')" + " AND COMP_STR_NO IN(%STORE_NUMBERS%) ";

	private static final String GET_COMPETITOR_STORE_INFO = "SELECT * FROM COMPETITOR_STORE_INFO";
	private static final String GET_COMPETITOR_STORE_NO = "SELECT COMP_STR_NO, NAME FROM COMPETITOR_STORE WHERE "
			+ "UPDATE_TIMESTAMP IS NOT NULL AND COMP_CHAIN_ID NOT IN (53)";

	private static final String UPDATE_RAW_COMPETITIVE_DATA_EXPORT_IND = "UPDATE RAW_COMPETITIVE_DATA SET IS_EXPORTED='Y'"
			+ " WHERE COMP_STR_NO =? AND UPC=? AND WEEK_START_DATE=TO_DATE(?, 'MM/dd/yyyy')";

	private static final String INSERT_INTO_COMP_UPC_MAPPING = "INSERT INTO COMP_UPC_MAPPING(BASE_UPC,BASE_RETAILER_ITEM_CODE,ITEM_NAME,"
			+ " COMP_SKU_OR_UPC,LOCATION_LEVEL_ID,LOCATION_ID) VALUES( ?, ?, ?, ?, ?, ?)";

	private static final String GET_COMP_UPC_MAPPING = "SELECT BASE_UPC, COMP_SKU_OR_UPC FROM ( SELECT BASE_UPC, COMP_SKU_OR_UPC, "
			+ " LOCATION_ID,LOCATION_LEVEL_ID, RANK() OVER(PARTITION BY BASE_UPC , RANK ORDER BY LOCATION_LEVEL_RANK ASC ) AS LOCATION_LEVEL_RANK"
			+ " FROM (SELECT BASE_UPC, COMP_SKU_OR_UPC, LOCATION_ID,LOCATION_LEVEL_ID, "
			+ " RANK() OVER (PARTITION BY COMP_SKU_OR_UPC,BASE_UPC ORDER BY CREATE_TIMESTAMP DESC) AS RANK,"
			+ " CASE WHEN LOCATION_LEVEL_ID = ? THEN 1 ELSE CASE WHEN LOCATION_LEVEL_ID = ? THEN 2 END END AS LOCATION_LEVEL_RANK"
			+ " FROM COMP_UPC_MAPPING WHERE LOCATION_ID IN (SELECT COMP_STR_ID FROM COMPETITOR_STORE WHERE COMP_STR_NO = ?) OR"
			+ " LOCATION_ID IN (SELECT COMP_CHAIN_ID FROM COMPETITOR_STORE WHERE COMP_STR_NO = ?))"
			+ " WHERE RANK=1) WHERE LOCATION_LEVEL_RANK = 1";

	private static final String GET_COMPETITOR_CHAIN_NO = "SELECT COMP_CHAIN_ID, COMP_CHAIN_NAME from COMPETITOR_CHAIN";

	private static final String GET_COMPETITOR_STORE_ID = "SELECT COMP_STR_ID, COMP_STR_NO FROM COMPETITOR_STORE";

	public HashSet<Integer> getSchedulesProcessed() {
		return schedulesProcessed;
	}

	public void setParameters(String userId, String checkListName) {
		checkUser = userId;
		checkList = checkListName;
	}

	/*
	 * Suresh - 9/28/09 - Changed the query to get the sale flag for two competitive
	 * stores.
	 */
	public CachedRowSet getCompetitiveDataForStoreComparision(Connection conn, int store1Id, int store2Id,
			int lagfactor, String fromDate, ArrayList<String> itemList) throws GeneralException {

		StringBuffer query = new StringBuffer();
		query.append(
				" SELECT A.SCHEDULE_ID STR1_SCHEDULE_ID, A.retailer_item_code item_code, to_char(A.WEEK_END_DATE,'MM/DD/YY') WEEK_END_DATE, A.ITEM_NAME, ");
		query.append(" A.COMP_STR_ID STORE1_ID , A.NAME STORE1_NAME,  ");
		query.append(" UNITPRICE(A.REG_PRICE, A.REG_M_PRICE, A.REG_M_PACK) REG_PRICE1, ");
		query.append(" DECODE( UNITPRICE(A.SALE_PRICE, A.SALE_M_PRICE, A.SALE_M_PACK), 0, ");
		query.append(" UNITPRICE(A.REG_PRICE, A.REG_M_PRICE, A.REG_M_PACK), ");
		query.append(" UNITPRICE(A.SALE_PRICE, A.SALE_M_PRICE, A.SALE_M_PACK)) SALE_PRICE1, ");
		query.append(" D.SCHEDULE_ID STR2_SCHEDULE_ID, D.COMP_STR_ID STORE2_ID, D.NAME STORE2_NAME, ");
		query.append(" UNITPRICE(D.REG_PRICE, D.REG_M_PRICE, D.REG_M_PACK) REG_PRICE2, ");
		query.append(" DECODE(UNITPRICE(D.SALE_PRICE, D.SALE_M_PRICE, D.SALE_M_PACK), 0, ");
		query.append(" UNITPRICE(D.REG_PRICE, D.REG_M_PRICE, D.REG_M_PACK), ");
		query.append(" UNITPRICE(D.SALE_PRICE, D.SALE_M_PRICE, D.SALE_M_PACK)) SALE_PRICE2, ");
		query.append(" NVL(A.PROMOTION_FLG, 'N') SALE_IND1, ");
		query.append(" NVL(D.PROMOTION_FLG, 'N') SALE_IND2 ");
		query.append(" from Competitive_data_view A, Competitive_data_view D  where ");
		query.append(" A.COMP_STR_ID =  ").append(store1Id).append(" AND ");
		query.append(" D.COMP_STR_ID =  ").append(store2Id).append(" AND ");
		query.append(" D.retailer_item_code = A.retailer_item_code AND ");
		query.append(" A.ITEM_NOT_FOUND_FLG = 'N' AND ");
		query.append(" D.ITEM_NOT_FOUND_FLG = 'N' AND ");
		query.append(" A.PRICE_NOT_FOUND_FLG = 'N' AND ");
		query.append(" D.PRICE_NOT_FOUND_FLG = 'N' AND ");
		query.append(" A.WEEK_END_DATE = D.WEEK_END_DATE + ").append(lagfactor * 7); // .append("
																						// AND
																						// ");
		// query.append(" A.schedule_id <> ").append(excludeScheduleId);
		if (itemList != null && itemList.size() > 0) {
			query.append(" AND A.retailer_Item_code in ("
					+ itemList.toString().substring(1, itemList.toString().length() - 1) + ")");
		}
		// Vaibav 1 - Write A.Item_code in itemlist
		if (fromDate != null && !fromDate.equals(""))
			query.append(" AND A.START_DATE > To_DATE( '").append(fromDate).append("', 'MM/DD/YYYY') ");
		query.append(" ORDER BY A.RETAILER_ITEM_CODE, A.WEEK_END_DATE ");
		logger.info(query);
		CachedRowSet result = PristineDBUtil.executeQuery(conn, query, "getCompetitiveDataForStoreComparision");
		return result;
	}

	public CachedRowSet getCompetitiveDataForSchedule(Connection conn, int scheduleId) throws GeneralException {

		StringBuffer query = new StringBuffer();
		query.append(
				" SELECT A.SCHEDULE_ID STR1_SCHEDULE_ID, A.retailer_item_code item_code, to_char(A.WEEK_END_DATE,'MM/DD/YY') WEEK_END_DATE, A.ITEM_NAME, ");
		query.append(" A.COMP_STR_ID STORE1_ID , A.NAME STORE1_NAME,  ");
		query.append(" UNITPRICE(A.REG_PRICE, A.REG_M_PRICE, A.REG_M_PACK) REG_PRICE1, ");
		query.append(" DECODE( UNITPRICE(A.SALE_PRICE, A.SALE_M_PRICE, A.SALE_M_PACK), 0, ");
		query.append(" UNITPRICE(A.REG_PRICE, A.REG_M_PRICE, A.REG_M_PACK), ");
		query.append(" UNITPRICE(A.SALE_PRICE, A.SALE_M_PRICE, A.SALE_M_PACK)) SALE_PRICE1 ");
		query.append(" from Competitive_data_view A where ");
		query.append(" A.SCHEDULE_ID =  ").append(scheduleId).append(" AND ");
		query.append(" A.ITEM_NOT_FOUND_FLG = 'N' AND ");
		query.append(" A.PRICE_NOT_FOUND_FLG = 'N' ");
		logger.debug(query);
		CachedRowSet result = PristineDBUtil.executeQuery(conn, query, "getCompetitiveDataForSchedule");
		return result;
	}

	public CachedRowSet getCompetitiveDataForItemComparision(Connection conn, int store1Id, int baseItemId,
			String compareItemList, String fromDate) throws GeneralException {

		StringBuffer query = new StringBuffer();
		query.append(" SELECT A.COMP_STR_NO STORE1_NO, A.COMP_STR_ID STORE1_ID, A.NAME STORE1_NAME, ");
		query.append(
				" to_char(A.WEEK_END_DATE,'MM/DD/YY') WEEK_END_DATE, A.ITEM_NAME ITEM1_NAME, A.RETAILER_ITEM_CODE ITEM1_CODE, ");
		query.append(" UNITPRICE(A.REG_PRICE, A.REG_M_PRICE, A.REG_M_PACK) REG1_PRICE, ");
		query.append(" DECODE( UNITPRICE(A.SALE_PRICE, A.SALE_M_PRICE, A.SALE_M_PACK), 0, ");
		query.append(" UNITPRICE(A.REG_PRICE, A.REG_M_PRICE, A.REG_M_PACK), ");
		query.append(" UNITPRICE(A.SALE_PRICE, A.SALE_M_PRICE, A.SALE_M_PACK)) SALE1_PRICE, ");
		query.append(" DECODE( UNITPRICE(A.SALE_PRICE, A.SALE_M_PRICE, A.SALE_M_PACK), 0, 'N', 'Y') SALE1_IND, ");
		query.append(" D.ITEM_NAME ITEM2_NAME, D.RETAILER_ITEM_CODE ITEM2_CODE,");
		query.append(" UNITPRICE(D.REG_PRICE, D.REG_M_PRICE, D.REG_M_PACK) REG2_PRICE, ");
		query.append(" DECODE(UNITPRICE(D.SALE_PRICE, D.SALE_M_PRICE, D.SALE_M_PACK), 0, ");
		query.append(" UNITPRICE(D.REG_PRICE, D.REG_M_PRICE, D.REG_M_PACK), ");
		query.append(" UNITPRICE(D.SALE_PRICE, D.SALE_M_PRICE, D.SALE_M_PACK)) SALE2_PRICE, ");
		query.append(" DECODE( UNITPRICE(D.SALE_PRICE, D.SALE_M_PRICE, D.SALE_M_PACK), 0, 'N', 'Y') SALE2_IND");
		query.append(" from Competitive_data_view A, Competitive_data_view D  where ");
		query.append(" A.COMP_STR_ID =  ").append(store1Id).append(" AND ");
		query.append(" A.COMP_STR_ID =  D.COMP_STR_ID AND ");
		query.append(" D.WEEK_END_DATE = A.WEEK_END_DATE AND ");
		query.append(" A.ITEM_NOT_FOUND_FLG = 'N' AND ");
		query.append(" D.ITEM_NOT_FOUND_FLG = 'N' AND ");
		query.append(" A.PRICE_NOT_FOUND_FLG = 'N' AND ");
		query.append(" D.PRICE_NOT_FOUND_FLG = 'N' AND ");
		query.append(" A.RETAILER_ITEM_CODE =  ").append(baseItemId).append(" AND ");
		query.append(" D.RETAILER_ITEM_CODE IN ( ").append(compareItemList).append(" ) ");
		if (fromDate != null && !fromDate.equals(""))
			query.append(" AND A.START_DATE > To_DATE( '").append(fromDate).append("', 'MM/DD/YYYY') ");
		query.append(" ORDER BY D.ITEM_CODE, A.WEEK_END_DATE ");
		logger.debug(query);
		CachedRowSet result = PristineDBUtil.executeQuery(conn, query, "getCompetitiveDataForStoreComparision");
		return result;

	}

	private Connection connection = null;

	public CompetitiveDataDAO(Connection conn) throws GeneralException {
		connection = conn;
		RetailPriceDAO retailPriceDAO = new RetailPriceDAO();
		chainId = retailPriceDAO.getChainId(conn);
	}

	public ArrayList<CompetitiveDataDTO> getCompetitiveDataItems(Integer scheduleID, int itemCode) {
		ArrayList<CompetitiveDataDTO> competitiveDataDTOs = new ArrayList<CompetitiveDataDTO>();

		PreparedStatement prepraredStmt = null;
		ResultSet resultSet = null;

		try {
			StringBuffer buffer = new StringBuffer();

			buffer.append(
					"SELECT cd.check_data_id, cd.schedule_id, cd.item_code, unitprice(cd.reg_price, cd.reg_m_price, cd.reg_m_pack) AS unit_reg_price, reg_price, reg_m_price, reg_m_pack, unitprice(cd.sale_price, cd.sale_m_price, cd.sale_m_pack) unit_sale_price, sale_price, sale_m_price, sale_m_pack, cd.item_not_found_flg, cd.price_not_found_flg, cd.check_datetime, cd.create_datetime, cd.change_direction, ");
			buffer.append(
					" cd.change_direction_sale, NVL(cd.NOT_FOUND_XTIMES_FLG, 'N')  NOT_FOUND_XTIMES_FLG, sd.comp_str_id, sd.start_date, sd.end_date, sd.STATUS_CHG_DATE FROM competitive_data cd ");
			buffer.append("INNER JOIN schedule sd ON (cd.schedule_id=sd.schedule_id) ");
			buffer.append("WHERE cd.schedule_id=? ");
			buffer.append("AND sd.current_status IN (2, 4) ");
			if (itemCode != ConstantsSI.INVALID_ITEM_CODE) { // for
																// testing/debugging
				buffer.append(" AND cd.item_code=?");
			}
			// buffer.append("ORDER BY check_data_id");

			try {

				prepraredStmt = connection.prepareStatement(buffer.toString());

				prepraredStmt.setInt(1, scheduleID);
				if (itemCode != ConstantsSI.INVALID_ITEM_CODE) { // for
																	// testing/debugging
					prepraredStmt.setInt(2, itemCode);
				}

				logger.debug("Competitive_Data query is " + buffer.toString() + ".");

				resultSet = prepraredStmt.executeQuery();

				logger.debug("Competitive_Data query executed successfully.");

				// Populating competitiveData object
				while (resultSet.next()) {
					CompetitiveDataDTO competitiveDataDTO = new CompetitiveDataDTO();

					competitiveDataDTO.setCheckDataID(resultSet.getInt("check_data_id"));
					competitiveDataDTO.setScheduleID(resultSet.getInt("schedule_id"));
					competitiveDataDTO.setItemCode(resultSet.getInt("item_code"));
					competitiveDataDTO.setUnitRegularPrice(resultSet.getFloat("unit_reg_price"));
					competitiveDataDTO.setUnitSalePrice(resultSet.getFloat("unit_sale_price"));
					competitiveDataDTO.setRegularPrice(resultSet.getFloat("reg_price"));
					competitiveDataDTO.setSalePrice(resultSet.getFloat("sale_price"));
					competitiveDataDTO.setRegularMPrice(resultSet.getFloat("reg_m_price"));
					competitiveDataDTO.setSaleMPrice(resultSet.getFloat("sale_m_price"));
					competitiveDataDTO.setRegularPack(resultSet.getInt("reg_m_pack"));
					competitiveDataDTO.setSalePack(resultSet.getInt("sale_m_pack"));
					competitiveDataDTO.setItemNotFoundFlg(resultSet.getString("item_not_found_flg").toCharArray()[0]);
					competitiveDataDTO.setPriceNotFoundFlg(resultSet.getString("price_not_found_flg").toCharArray()[0]);
					competitiveDataDTO
							.setitemNotFoundXTimesFlag(resultSet.getString("NOT_FOUND_XTIMES_FLG").toCharArray()[0]);
					competitiveDataDTO.setCompStrID(resultSet.getInt("comp_str_id"));
					competitiveDataDTO.setChangeDirection(resultSet.getInt("change_direction"));
					competitiveDataDTO.setChangeDirectionSale(resultSet.getInt("change_direction_sale"));
					competitiveDataDTO.setStartDate(resultSet.getDate("start_date"));
					competitiveDataDTO.setEndDate(resultSet.getDate("end_date"));
					competitiveDataDTO.setStatusChangeDate(resultSet.getDate("STATUS_CHG_DATE"));
					competitiveDataDTOs.add(competitiveDataDTO);
				}
			} catch (SQLException e) {
				throw new GeneralException("Prepared statement instantiation failed!", e);
			}
		} catch (GeneralException e) {
			GenericUtil.logError(e.getMessage(), e);
		} finally {
			PristineDBUtil.close(resultSet, prepraredStmt);
		}

		return competitiveDataDTOs;
	}

	public int getStoreId(int scheduleID) throws GeneralException {
		int ret = ConstantsSI.INVALID_ID;

		PreparedStatement prepraredStmt = null;
		ResultSet resultSet = null;
		StringBuffer buffer = new StringBuffer();

		buffer.append("SELECT comp_str_id FROM schedule");
		buffer.append(" WHERE schedule_id=?");

		try {
			prepraredStmt = connection.prepareStatement(buffer.toString());

			prepraredStmt.setInt(1, scheduleID);

			resultSet = prepraredStmt.executeQuery();

			if (resultSet.next()) {
				ret = resultSet.getInt("comp_str_id");
			}

			return ret;
		} catch (SQLException e) {
			throw new GeneralException("getStoreId: Prepared statement instantiation failed!", e);
		} finally {
			PristineDBUtil.close(prepraredStmt);
		}
	}

	public String getStatusChangeDate(int scheduleID) {
		return getDate(scheduleID, "status_chg_date");
	}

	public String getStartDate(int scheduleID) {
		return getDate(scheduleID, "start_date");
	}

	public String getDate(int scheduleID, String dateFieldName) {
		Statement stmt = null;
		ResultSet resultSet = null;

		String returnValue = ConstantsSI.CONST_SI_ANALYSIS_SCHEDULE_DOESNOT_EXIST;

		try {
			StringBuffer buffer = new StringBuffer();

			buffer.append("SELECT ").append(dateFieldName).append(" FROM schedule ");
			buffer.append("WHERE schedule_id=").append(scheduleID);

			try {
				stmt = connection.createStatement();
				resultSet = stmt.executeQuery(buffer.toString());
				if (resultSet.next())
					returnValue = resultSet.getDate(dateFieldName).toString();
			} catch (SQLException e) {
				throw new GeneralException("Statement instantiation failed!", e);
			}
		} catch (GeneralException e) {
			GenericUtil.logError(e.getMessage(), e);
		} finally {
			PristineDBUtil.close(resultSet, stmt);
		}

		return returnValue;
	}

	public boolean checkGPSViolation(int scheduleID) {
		PreparedStatement prepraredStmt = null;
		ResultSet resultSet = null;

		try {
			StringBuffer buffer = new StringBuffer();

			buffer.append("SELECT GPs_VIOLATION FROM performance_stat ");
			buffer.append("WHERE schedule_id=? ");

			try {
				prepraredStmt = connection.prepareStatement(buffer.toString());

				prepraredStmt.setInt(1, scheduleID);

				resultSet = prepraredStmt.executeQuery();

				if (resultSet.next()) {
					if (resultSet.getString("GPs_VIOLATION").equalsIgnoreCase("Y"))
						return true;
				}
			} catch (SQLException e) {
				throw new GeneralException("Prepared statement instantiation failed!", e);
			}
		} catch (GeneralException e) {
			GenericUtil.logError(e.getMessage(), e);
		} finally {
			PristineDBUtil.close(resultSet, prepraredStmt);
		}

		return false;
	}

	/**
	 * This function will give list of store comp dto for Item code link pop up in
	 * two store analysis
	 * 
	 * @param storeDataSet
	 * @return
	 */

	public ArrayList<StoreComparisonDTO> getStoreCompPopUp(CachedRowSet storeDataSet) {
		ArrayList<StoreComparisonDTO> storeCompDtoList = null;

		try {
			storeCompDtoList = new ArrayList<StoreComparisonDTO>();
			while (storeDataSet.next()) {
				StoreComparisonDTO storeDto = new StoreComparisonDTO();

				String sale1Price = storeDataSet.getString("SALE_PRICE1");
				String sale2Price = storeDataSet.getString("SALE_PRICE2");
				String reg1Price = storeDataSet.getString("REG_PRICE1");
				String reg2Price = storeDataSet.getString("REG_PRICE2");

				storeDto.setItemCode(storeDataSet.getString("item_code"));
				storeDto.setWeekEndDate(storeDataSet.getString("WEEK_END_DATE"));
				storeDto.setItemName(storeDataSet.getString("ITEM_NAME"));
				storeDto.setStore1Id(storeDataSet.getString("STORE1_ID"));
				storeDto.setStore1Name(storeDataSet.getString("STORE1_NAME"));
				storeDto.setStore1RegPrice(storeDataSet.getString("REG_PRICE1"));
				storeDto.setStore2Id(storeDataSet.getString("STORE2_ID"));
				storeDto.setStore2Name(storeDataSet.getString("STORE2_NAME"));
				storeDto.setStore2RegPrice(storeDataSet.getString("REG_PRICE2"));

				if (reg1Price.equalsIgnoreCase(sale1Price)) {
					sale1Price = "";
				}
				if (reg2Price.equalsIgnoreCase(sale2Price)) {
					sale2Price = "";
				}

				storeDto.setStore1SalePrice(sale1Price);
				storeDto.setStore2SalePrice(sale2Price);
				storeCompDtoList.add(storeDto);
			}

		} catch (Exception e) {
			e.printStackTrace();
		}
		return storeCompDtoList;
	}

	public void closeDBConnection() {
		GenericUtil.logMessage("Closing Connection for CompetitiveDAO");

		PristineDBUtil.close(connection);
	}

	/**
	 * @param storeDataSet
	 * @return
	 */
	public ArrayList<StoreComparisonDTO> getItemCompPopUp(CachedRowSet storeDataSet) {
		ArrayList<StoreComparisonDTO> itemCompList = null;
		try {
			itemCompList = new ArrayList<StoreComparisonDTO>();
			while (storeDataSet.next()) {
				String sale1Price = storeDataSet.getString("SALE1_PRICE");
				String sale2Price = storeDataSet.getString("SALE2_PRICE");
				String reg1Price = storeDataSet.getString("REG1_PRICE");
				String reg2Price = storeDataSet.getString("REG2_PRICE");

				StoreComparisonDTO storeDto = new StoreComparisonDTO();
				storeDto.setItemCode(storeDataSet.getString("ITEM1_CODE"));
				storeDto.setItemCode2(storeDataSet.getString("ITEM2_CODE"));
				storeDto.setWeekEndDate(storeDataSet.getString("WEEK_END_DATE"));
				storeDto.setItemName(storeDataSet.getString("ITEM1_NAME"));
				storeDto.setItemName2(storeDataSet.getString("ITEM2_NAME"));
				storeDto.setStore1Id(storeDataSet.getString("STORE1_ID"));
				storeDto.setStore1Name(storeDataSet.getString("STORE1_NAME"));
				storeDto.setStore1RegPrice(storeDataSet.getString("REG1_PRICE"));
				storeDto.setStore2RegPrice(storeDataSet.getString("REG2_PRICE"));

				if (reg1Price.equalsIgnoreCase(sale1Price)) {
					sale1Price = "";
				}
				if (reg2Price.equalsIgnoreCase(sale2Price)) {
					sale2Price = "";
				}

				storeDto.setStore1SalePrice(sale1Price);
				storeDto.setStore2SalePrice(sale2Price);
				itemCompList.add(storeDto);
			}

		} catch (Exception e) {
			e.printStackTrace();
		}

		return itemCompList;
	}

	public PriceSuggestionDTO getPriceSuggestionPrices(String scheduleId, String itemCode) {
		PreparedStatement prepraredStmt = null;
		ResultSet resultSet = null;
		PriceSuggestionDTO priceSuggDTO = null;
		String regPrice = null;
		String salePrice = null;
		try {
			priceSuggDTO = new PriceSuggestionDTO();

			StringBuffer buffer = new StringBuffer();

			buffer.append(" SELECT UNITPRICE(A.REG_PRICE, A.REG_M_PRICE, A.REG_M_PACK) REG_PRICE, ");
			buffer.append(" DECODE( UNITPRICE(A.SALE_PRICE, A.SALE_M_PRICE, A.SALE_M_PACK), 0, ");
			buffer.append(" UNITPRICE(A.REG_PRICE, A.REG_M_PRICE, A.REG_M_PACK), ");
			buffer.append(" UNITPRICE(A.SALE_PRICE, A.SALE_M_PRICE, A.SALE_M_PACK)) SALE_PRICE");
			buffer.append(" from Competitive_data_view A");
			buffer.append(" WHERE schedule_id= " + scheduleId);
			buffer.append(" AND retailer_item_code =" + itemCode);

			prepraredStmt = connection.prepareStatement(buffer.toString());
			resultSet = prepraredStmt.executeQuery();

			if (resultSet.next()) {
				regPrice = resultSet.getString("REG_PRICE");
				salePrice = resultSet.getString("SALE_PRICE");
			}

			if (regPrice.equalsIgnoreCase(salePrice)) {
				salePrice = "";
			}

			priceSuggDTO.setRegPrice(regPrice);
			priceSuggDTO.setSalePrice(salePrice);

		} catch (Exception e) {
			logger.error("Error in getting price suggestion prices!", e);
		} finally {
			PristineDBUtil.close(prepraredStmt);
		}
		return priceSuggDTO;
	}

	public ArrayList<String> getStoreIdList(Connection conn, String compStrNo) throws GeneralException, SQLException {
		ArrayList<String> storeIdList = null;
		if (storeIdMap.containsKey(compStrNo)) {
			storeIdList = storeIdMap.get(compStrNo);
		} else {
			storeIdList = new ArrayList<String>();
			StringBuffer sb = new StringBuffer();
			// get CompStrId
			sb.append("SELECT COMP_STR_ID FROM COMPETITOR_STORE WHERE ACTIVE_INDICATOR = 'Y' AND ");
			// Janani - 6/6/12 - Added this flag to include chain_id condition
			// in getStoreIdList() for computing movement weekly
			boolean loadCompDataForSubscriber = Boolean
					.parseBoolean(PropertyManager.getProperty("LOAD_COMP_DATA_FOR_SUBSCRIBER", "FALSE"));

			if (!loadCompDataForSubscriber) {
				if (isSubscriber) {
					sb.append(" COMP_CHAIN_ID = ").append(chainId).append(" AND ");
				} else {
					sb.append(" COMP_CHAIN_ID <> ").append(chainId).append(" AND ");
				}
			}
			sb.append(" COMP_STR_NO =  '").append(compStrNo).append("' OR ");
			sb.append(" GLN = '").append(compStrNo).append("'");
			CachedRowSet result = PristineDBUtil.executeQuery(conn, sb, "getCompetitiveStoreId");
			while (result.next()) {
				String compStrIdVal = result.getString("COMP_STR_ID");
				storeIdList.add(compStrIdVal);
			}

			/*
			 * This selection was added so that for Rite-Aid where TDLinx number if the
			 * leading character is 0 is stripped from the comp file and list is not
			 * matching
			 */
			if (storeIdList.size() == 0) {
				sb = new StringBuffer();
				sb.append("SELECT COMP_STR_ID FROM COMPETITOR_STORE WHERE ACTIVE_INDICATOR = 'Y' AND ");
				// Janani - 6/6/12 - Added this flag to include chain_id
				// condition in getStoreIdList() for computing movement weekly
				if (!loadCompDataForSubscriber) {
					if (isSubscriber) {
						sb.append(" COMP_CHAIN_ID = ").append(chainId).append(" AND ");
					} else {
						sb.append(" COMP_CHAIN_ID <> ").append(chainId).append(" AND ");
					}
				}
				sb.append(" COMP_STR_NO like  '%").append(compStrNo).append("' OR ");
				sb.append(" GLN like '%").append(compStrNo).append("'");
				result = PristineDBUtil.executeQuery(conn, sb, "getCompetitiveStoreId");
				while (result.next()) {
					String compStrIdVal = result.getString("COMP_STR_ID");
					storeIdList.add(compStrIdVal);
				}
			}

			if (storeIdList.size() == 0) {
				// logger.error("Error *** Invalid Store: " + compStrNo);
				throw new GeneralException("Invalid Store Number - " + compStrNo, GeneralException.LEVEL_WARNING);
			}
			storeIdMap.put(compStrNo, storeIdList);

		}
		return storeIdList;

	}

	public int getItemCode(Connection conn, CompetitiveDataDTO compData) throws GeneralException {
		int ret = -1;

		StringBuffer sb = new StringBuffer();
		sb.append("SELECT A.DEPT_ID, A.ITEM_CODE, NVL(B.CHECK_DATA_ID, -1) CHECK_DATA_ID FROM ");
		// Changes to include item_code in query instead of upc
		sb.append(" ITEM_LOOKUP A, COMPETITIVE_DATA B WHERE ");
		// Include item_code in condition if item code is greater than 0, if not
		// include upc in the query condition
		if (compData.itemcode > 0)
			sb.append("A.ITEM_CODE = " + compData.itemcode);
		else
			sb.append("A.STANDARD_UPC LIKE '").append(compData.upc).append("%'");
		sb.append(" AND B.SCHEDULE_ID (+)= ").append(compData.scheduleId);
		sb.append(" AND B.ITEM_CODE (+)= A.ITEM_CODE");

		try {
			CachedRowSet result = PristineDBUtil.executeQuery(conn, sb, "getItemCodeId");
			if (result.next()) {
				String itemcodeStr = result.getString("ITEM_CODE");
				if (itemcodeStr != null) {
					compData.checkItemId = result.getInt("CHECK_DATA_ID");
					compData.itemcode = Integer.parseInt(itemcodeStr);
					compData.deptId = result.getInt("DEPT_ID");
					ret = compData.itemcode;
				}
			}
		} catch (SQLException sqle) {
			throw new GeneralException("SQL Exception", sqle);
		}

		return ret;
	}

	/**
	 * @param conn
	 * @param compData
	 * @param populateSubGrps
	 * @return
	 * @throws GeneralException
	 */
	public boolean insertCompData(Connection conn, CompetitiveDataDTO compData, boolean populateSubGrps)
			throws GeneralException {
		boolean insert = false;

		try {

			ArrayList<String> storeIdList = getStoreIdList(conn, compData.compStrNo);
			Iterator<String> strItr = storeIdList.iterator();

			int i = 0;
			while (strItr.hasNext()) {

				String compStrIdVal = strItr.next();
				i++;
				// if( i>=2){
				// logger.debug("Multiple StoreId for " + compData.compStrNo);
				// }
				compData.compStrId = Integer.parseInt(compStrIdVal);

				// get ScheduleId
				compData.scheduleId = getScheduleID(conn, compData);

				if (!schedulesProcessed.contains(compData.scheduleId))
					schedulesProcessed.add(compData.scheduleId);

				// Get Itemcode
				StringBuffer sb = new StringBuffer();
				sb.append("SELECT A.ITEM_CODE, NVL(B.CHECK_DATA_ID, -1) CHECK_DATA_ID FROM ");
				sb.append(" ITEM_LOOKUP A, COMPETITIVE_DATA B WHERE A.STANDARD_UPC LIKE ");
				sb.append("'").append(compData.upc).append("%'");
				sb.append(" AND B.SCHEDULE_ID (+)= ").append(compData.scheduleId);
				sb.append(" AND B.ITEM_CODE (+)= A.ITEM_CODE");

				CachedRowSet result = PristineDBUtil.executeQuery(conn, sb, "getItemCodeId");
				if (result.next()) {
					String itemcodeStr = result.getString("ITEM_CODE");
					compData.checkItemId = result.getInt("CHECK_DATA_ID");
					compData.itemcode = Integer.parseInt(itemcodeStr);
					insert = insertCompetitiveData(conn, compData);
				} else {
					insertNonCarriedItem(conn, compData);
					insert = false;
				}

			}

		} catch (SQLException sqle) {
			throw new GeneralException("SQL Exception", sqle);
		}
		return insert;
		/*
		 * logger.info("Inserting UPC - " + compData.upc + " item - " +
		 * compData.itemName + compData.size + compData.uom); ItemDAO itemdao = new
		 * ItemDAO(); ItemDTO item = new ItemDTO(); item.clear(); if (
		 * compData.itemName.equals("")) item.itemName = "UNKNOWN"; else item.itemName =
		 * compData.itemName; item.catName = "UNKNOWN"; item.deptName = "UNKNOWN"; if(
		 * populateSubGrps){ item.subCatName="UNKNOWN"; //item.segmentCode="UNKNOWN";
		 * item.segmentCode=""; }else{ item.subCatName=""; item.segmentCode=""; }
		 * 
		 * item.retailerItemCode = compData.retailerItemCode; if( item.retailerItemCode
		 * == null || item.retailerItemCode.equals("")) item.retailerItemCode = "0";
		 * item.upc = compData.upc; item.size = compData.size; item.pack =
		 * compData.itemPack; item.uom = compData.uom; item.uomId = compData.uomId;
		 * itemdao.insertItem(conn, item, true); itemcodeStr =
		 * PristineDBUtil.getSingleColumnVal(conn, sb,
		 * "Insert Comp Data - get Item code"); if( itemcodeStr == null ) {
		 * logger.error("Invalid Retailer Item No - " + compData.retailerItemCode);
		 * throw new GeneralException( "Invalid Retailer Item No - " +
		 * compData.retailerItemCode); }
		 */

	}

	public int getScheduleID(Connection conn, CompetitiveDataDTO compData) throws GeneralException {
		return getScheduleID(conn, compData, true);
	}

	public int getScheduleID(Connection conn, CompetitiveDataDTO compData, boolean createSch) throws GeneralException {

		int scheduleId = -1;
		String key = compData.compStrId + "-" + compData.weekStartDate + "-" + compData.weekEndDate;
		if (scheduleIdMap.containsKey(key)) {
			scheduleId = scheduleIdMap.get(key);
		} else {
			StringBuffer sb = new StringBuffer();
			sb.append("SELECT SCHEDULE_ID FROM SCHEDULE WHERE ");
			// sb.append("PRICE_CHECKER_ID = '").append(checkUser).append("' AND
			// ");
			sb.append(" COMP_STR_ID = ");
			sb.append(compData.compStrId);
			sb.append(" AND START_DATE = ");
			sb.append("TO_DATE ( '").append(compData.weekStartDate).append("', 'MM/DD/YYYY')");
			sb.append(" AND END_DATE = ");
			sb.append("TO_DATE ( '").append(compData.weekEndDate).append("', 'MM/DD/YYYY')");

			// Include price check id condition in the query to retrieve
			// schedule
			if (priceCheckId < 0)
				priceCheckId = getPriceCheckListID(checkList);

			if (priceCheckId > 0)
				sb.append(" AND PRICE_CHECK_LIST_ID = ").append(priceCheckId);

			// get ScheduleId
			String schIDStr = PristineDBUtil.getSingleColumnVal(conn, sb, "Insert Comp Data - get Schedule ID");
			if (schIDStr == null && createSch) {
				// Inserting schedules
				createSchedule(conn, compData);

				schIDStr = PristineDBUtil.getSingleColumnVal(conn, sb, "Insert Comp Data - get Schedule ID");

				logger.debug("New Schedule = " + schIDStr);
				if (schIDStr == null) {
					logger.error("Missing schedule for Comp Store - " + compData.compStrNo + " Check date = "
							+ compData.checkDate);
					throw new GeneralException("Missing Schedule for Comp Store - " + compData.compStrNo
							+ " Check date = " + compData.checkDate);
				}
			}
			if (schIDStr != null) {
				scheduleId = Integer.parseInt(schIDStr);
				scheduleIdMap.put(key, scheduleId);
			}
		}
		return scheduleId;
	}

	/**
	 * Returns Price Check Id for the price checker name specified in the input
	 * 
	 * @param priceCheckUserName Pricecheck user name
	 * @return price check id
	 * @throws GeneralException
	 */
	public int getPriceCheckListID(String priceCheckUserName) throws GeneralException {
		logger.debug("Inside getPriceCheckListID() of CompetitiveDataDAO");
		PreparedStatement pstmt = null;
		ResultSet rs = null;
		int priceCheckId = -1;
		try {
			pstmt = connection.prepareStatement(GET_PRICECHECK_ID);
			pstmt.setString(1, priceCheckUserName);
			rs = pstmt.executeQuery();
			if (rs.next())
				priceCheckId = rs.getInt("ID");
		} catch (SQLException exception) {
			logger.error("Error in getPriceCheckListID - " + exception);
			throw new GeneralException("Error in retrieving Price Check Id");
		} finally {
			PristineDBUtil.close(rs, pstmt);
		}
		return priceCheckId;
	}

	public void createSchedule(Connection conn, CompetitiveDataDTO compData) throws GeneralException {

		StringBuffer sb = new StringBuffer();

		sb = new StringBuffer();
		sb.append(" INSERT INTO SCHEDULE( SCHEDULE_ID, PRICE_CHECK_LIST_ID, PRICE_CHECKER_ID, ");
		sb.append(" CREATE_USER_ID, CREATE_DATETIME, COMP_STR_ID, START_DATE, END_DATE, ");
		sb.append(" CURRENT_STATUS, STATUS_CHG_DATE, NO_OF_ITEMS, CHECK_DATE ) ");
		sb.append(" SELECT SCHEDULE_SEQ.NEXTVAL, ID,");
		sb.append("'" + checkUser + "', ");
		sb.append("'" + checkUser + "', ");
		sb.append(" TO_DATE( '").append(compData.weekStartDate).append("','MM/DD/YYYY'), ");
		sb.append(compData.compStrId).append(", ");
		sb.append(" TO_DATE( '").append(compData.weekStartDate).append("','MM/DD/YYYY'), ");
		sb.append(" TO_DATE( '").append(compData.weekEndDate).append("','MM/DD/YYYY'), ");
		sb.append(" 2, "); // Completed Status
		sb.append(" SYSDATE, ");
		sb.append(" -1, "); // Completed Status
		sb.append(" TO_DATE( '").append(compData.checkDate).append("','MM/DD/YYYY') ");
		sb.append("from price_check_list where name = '" + checkList + "'");
		logger.debug(sb.toString());
		PristineDBUtil.execute(conn, sb, "Competitive Data Load - create schedule");
	}

	private boolean insertCompetitiveData(Connection conn, CompetitiveDataDTO compData) throws GeneralException {
		boolean insert = true;
		StringBuffer sb;
		/*
		 * StringBuffer sb = new StringBuffer(); sb.append(
		 * "SELECT CHECK_DATA_ID FROM COMPETITIVE_DATA WHERE "); sb.append(
		 * " SCHEDULE_ID = ").append(compData.scheduleId); sb.append(
		 * " AND ITEM_CODE = ").append(compData.itemcode); String compDataID =
		 * PristineDBUtil.getSingleColumnVal(conn, sb,
		 * "Insert Comp Data - get Comp Data ID");
		 */
		if (compData.checkItemId == -1) {

			if (compData.outSideRangeInd == null || compData.outSideRangeInd.equals(""))
				compData.outSideRangeInd = "N";

			sb = new StringBuffer();
			sb.append(" INSERT INTO COMPETITIVE_DATA( CHECK_DATA_ID, SCHEDULE_ID, ITEM_CODE, ");
			sb.append(" REG_PRICE, REG_M_PACK, REG_M_PRICE, ");
			sb.append(" SALE_PRICE, SALE_M_PACK, SALE_M_PRICE, ");
			sb.append(" ITEM_NOT_FOUND_FLG, PRICE_NOT_FOUND_FLG, PROMOTION_FLG, OUTSIDE_RANGE_IND, NEW_UOM, ");
			sb.append(
					" EFF_REG_START_DATE, EFF_SALE_START_DATE , EFF_SALE_END_DATE, CHECK_DATETIME, CREATE_DATETIME )");
			sb.append(" VALUES( COMP_CHECK_ID_SEQ.NEXTVAL, ");
			sb.append(compData.scheduleId).append(", ");
			sb.append(compData.itemcode).append(", ");
			sb.append(((compData.regPrice < 0.01) ? "0.0" : PrestoUtil.round(compData.regPrice, 2))).append(",  ");
			sb.append(((compData.regMPack < 0.01) ? "0" : compData.regMPack)).append(",  ");
			sb.append(((compData.regMPrice < 0.01) ? "0.0" : PrestoUtil.round(compData.regMPrice, 2))).append(",  ");
			sb.append(((compData.fSalePrice < 0.01) ? "0.0" : PrestoUtil.round(compData.fSalePrice, 2))).append(",  ");
			sb.append(((compData.saleMPack < 0.01) ? "0" : compData.saleMPack)).append(",  ");
			sb.append(((compData.fSaleMPrice < 0.01) ? "0.0" : PrestoUtil.round(compData.fSaleMPrice, 2)))
					.append(",  ");
			sb.append("'").append(compData.itemNotFound).append("', ");
			sb.append("'").append(compData.priceNotFound).append("', ");
			sb.append("'").append(compData.saleInd).append("', ");
			sb.append("'").append(compData.outSideRangeInd).append("', ");
			sb.append(((compData.newUOM.equals("")) ? "NULL" : compData.newUOM)).append(",  ");

			if (compData.effRegRetailStartDate == null || compData.effRegRetailStartDate.equals(""))
				sb.append(" NULL, ");
			else
				sb.append(" TO_DATE( '").append(compData.effRegRetailStartDate).append("','MM/DD/YYYY'), ");

			if (compData.effSaleStartDate == null || compData.effSaleStartDate.equals(""))
				sb.append(" NULL, ");
			else
				sb.append(" TO_DATE( '").append(compData.effSaleStartDate).append("','MM/DD/YYYY'), ");

			if (compData.effSaleEndDate == null || compData.effSaleEndDate.equals(""))
				sb.append(" NULL, ");
			else
				sb.append(" TO_DATE( '").append(compData.effSaleEndDate).append("','MM/DD/YYYY'), ");
			sb.append(" TO_DATE( '").append(compData.checkDate).append("','MM/DD/YYYY'), ");
			sb.append(" SYSDATE ");
			sb.append(" )");
			// logger.debug(sb.toString());
			PristineDBUtil.execute(conn, sb, "Competitive Data Load - create schedule");

		} else {
			sb = new StringBuffer();
			sb.append(" UPDATE COMPETITIVE_DATA SET ");
			sb.append(" REG_PRICE = ");
			sb.append(((compData.regPrice < 0.01) ? "0.0" : PrestoUtil.round(compData.regPrice, 2))).append(",  ");
			sb.append(" REG_M_PACK = ");
			sb.append(((compData.regMPack < 0.01) ? "0" : compData.regMPack)).append(",  ");
			sb.append(" REG_M_PRICE = ");
			sb.append(((compData.regMPrice < 0.01) ? "0.0" : PrestoUtil.round(compData.regMPrice, 2))).append(",  ");
			sb.append(" SALE_PRICE = ");
			sb.append(((compData.fSalePrice < 0.01) ? "0.0" : PrestoUtil.round(compData.fSalePrice, 2))).append(",  ");
			sb.append(" SALE_M_PACK = ");
			sb.append(((compData.saleMPack < 0.01) ? "0" : compData.saleMPack)).append(",  ");
			sb.append(" SALE_M_PRICE = ");
			sb.append(((compData.fSaleMPrice < 0.01) ? "0.0" : PrestoUtil.round(compData.fSaleMPrice, 2)))
					.append(",  ");
			sb.append(" ITEM_NOT_FOUND_FLG ='").append(compData.itemNotFound).append("', ");
			sb.append(" PRICE_NOT_FOUND_FLG ='").append(compData.priceNotFound).append("', ");
			sb.append(" PROMOTION_FLG ='").append(compData.saleInd).append("', ");
			sb.append(" NEW_UOM = ");
			sb.append(((compData.newUOM.equals("")) ? "NULL" : compData.newUOM)).append(",  ");

			if (compData.effRegRetailStartDate != null && !compData.effRegRetailStartDate.equals("")) {
				sb.append(" EFF_REG_START_DATE  = ");
				sb.append(" TO_DATE( '").append(compData.effRegRetailStartDate).append("','MM/DD/YYYY'), ");
			}

			sb.append(" EFF_SALE_START_DATE = ");
			if (compData.effSaleStartDate == null || compData.effSaleStartDate.equals(""))
				sb.append(" NULL, ");
			else
				sb.append(" TO_DATE( '").append(compData.effSaleStartDate).append("','MM/DD/YYYY'), ");

			sb.append(" EFF_SALE_END_DATE = ");
			if (compData.effSaleEndDate == null || compData.effSaleEndDate.equals(""))
				sb.append(" NULL, ");
			else
				sb.append(" TO_DATE( '").append(compData.effSaleEndDate).append("','MM/DD/YYYY'), ");

			sb.append(" CHECK_DATETIME = ");
			sb.append(" TO_DATE( '").append(compData.checkDate).append("','MM/DD/YYYY'), ");
			sb.append(" CREATE_DATETIME = ").append(" SYSDATE ");

			sb.append(" WHERE CHECK_DATA_ID = ");
			sb.append(compData.checkItemId);
			int updateCount = PristineDBUtil.executeUpdate(conn, sb, "Competitive DATA - Update");
			if (updateCount != 1) {
				logger.error("Update Comp Data Unsuccessful, record count = " + updateCount);
			} else
				insert = true;
		}
		return insert;
	}

	public void setSuspectFlag(int scheduleId, ArrayList<Integer> itemList) throws GeneralException {
		updateSuspectFlag(scheduleId, itemList, true);
	}

	public void clearSuspectFlag(int scheduleId) throws GeneralException {
		updateSuspectFlag(scheduleId, null, false);
	}

	private void updateSuspectFlag(int scheduleId, ArrayList<Integer> itemList, boolean flag) throws GeneralException {
		if (scheduleId == ConstantsSI.INVALID_ID)
			return;

		char flagVal = flag ? 'Y' : 'N';
		StringBuffer sb = new StringBuffer("UPDATE Competitive_Data");
		sb.append(" SET Suspect='").append(flagVal).append('\'');
		sb.append(" WHERE Schedule_Id=").append(scheduleId);

		if (itemList != null && itemList.size() > 0) {
			sb.append(" AND ITEM_CODE IN (");

			boolean first = true;
			ListIterator<Integer> iterator = itemList.listIterator();
			while (iterator.hasNext()) {
				int itemCode = (int) iterator.next();
				if (first) {
					first = false;
				} else {
					sb.append(',');
				}
				sb.append(itemCode);
			}

			sb.append(')'); // close
		}

		logger.info(sb.toString());

		// Execute update
		int count = PristineDBUtil.executeUpdate(connection, sb, "Competitive Data Suspect Flag - Update");
		if (count <= 0) {
			logger.error("Update Comp Data Suspect Flag unsuccessful, record count = " + count);
		}
	}

	public CompetitiveDataDTO getCompDataForItem(Connection conn, int scheduleId, int itemCode, boolean showFlavors)
			throws GeneralException {
		StringBuffer buffer = new StringBuffer();
		CompetitiveDataDTO compData = new CompetitiveDataDTO();
		compData.clear();
		compData.itemNotFound = "Y";
		compData.itemcode = itemCode;
		buffer.append(" SELECT A.CHECK_DATA_ID, UNITPRICE(A.REG_PRICE, A.REG_M_PRICE, A.REG_M_PACK) REG_PRICE, ");
		buffer.append(" UNITPRICE(A.SALE_PRICE, A.SALE_M_PRICE, A.SALE_M_PACK) SALE_PRICE, ");
		buffer.append(" A.ITEM_NOT_FOUND_FLG, A.PRICE_NOT_FOUND_FLG, A.PROMOTION_FLG, ");
		buffer.append(" B.COMP_STR_ID, TO_CHAR( A.START_DATE, 'MM/DD/YY') START_DATE, A.ITEM_NAME ");
		buffer.append(" from SCHEDULE B, ");
		if (showFlavors)
			buffer.append(" Competitive_data_view  A ");
		else
			buffer.append(" comp_data_view_no_flavor  A ");

		buffer.append(" WHERE A.schedule_id= " + scheduleId);
		buffer.append(" AND B.schedule_id= A.schedule_id ");
		buffer.append(" AND A.item_code =" + itemCode);

		CachedRowSet result = PristineDBUtil.executeQuery(conn, buffer, "getCompDataForItem");
		try {
			if (result.next()) {
				compData = new CompetitiveDataDTO();
				compData.checkItemId = result.getInt("CHECK_DATA_ID");
				compData.regPrice = NumberUtil.RoundFloat(result.getFloat("REG_PRICE"), 2);
				compData.fSalePrice = NumberUtil.RoundFloat(result.getFloat("SALE_PRICE"), 2);
				compData.itemNotFound = result.getString("ITEM_NOT_FOUND_FLG");
				compData.priceNotFound = result.getString("PRICE_NOT_FOUND_FLG");
				compData.saleInd = result.getString("PROMOTION_FLG");
				// compData.chgDirection = result.getInt("CHANGE_DIRECTION");
				compData.compStrId = result.getInt("COMP_STR_ID");
				compData.itemcode = itemCode;
				compData.weekStartDate = result.getString("START_DATE");
				compData.itemName = result.getString("ITEM_NAME");
			}
		} catch (Exception e) {
			throw new GeneralException("Cached Rowset Access Exception", e);
		}
		return compData;

	}

	/*
	 * This method can be potentially merged with ItemHistory method and we can
	 * return the 0th item
	 */
	public CompetitiveDataDTO getCompDataForItemPreviousCheck(Connection conn, int compStrId, String weekStartDate,
			int itemCode, boolean showFlavors) throws GeneralException {

		CompetitiveDataDTO compData = new CompetitiveDataDTO();
		compData.clear();
		compData.itemNotFound = "Y";
		CachedRowSet result = getItemHistory(conn, compStrId, weekStartDate, itemCode, showFlavors);
		try {
			if (result.next()) {
				compData.checkItemId = result.getInt("CHECK_DATA_ID");
				compData.regPrice = NumberUtil.RoundFloat(result.getFloat("REG_PRICE"), 2);
				compData.fSalePrice = NumberUtil.RoundFloat(result.getFloat("SALE_PRICE"), 2);
				compData.itemNotFound = result.getString("ITEM_NOT_FOUND_FLG");
				compData.priceNotFound = result.getString("PRICE_NOT_FOUND_FLG");
				compData.saleInd = result.getString("PROMOTION_FLG");
				// compData.chgDirection = result.getInt("CHANGE_DIRECTION");
				compData.compStrId = result.getInt("COMP_STR_ID");
				compData.itemcode = itemCode;
				compData.weekStartDate = result.getString("START_DATE");
			}
		} catch (Exception e) {
			throw new GeneralException("Cached Rowset Access Exception", e);
		}
		return compData;

	}

	public ArrayList<CompetitiveDataDTO> getCompDataItemHistory(Connection conn, int compStrId, String weekStartDate,
			int itemCode, boolean showFlavors) throws GeneralException {

		ArrayList<CompetitiveDataDTO> compItemDataList = new ArrayList<CompetitiveDataDTO>();

		CompetitiveDataDTO compData;

		CachedRowSet result = getItemHistory(conn, compStrId, weekStartDate, itemCode, showFlavors);
		try {
			while (result.next()) {
				compData = new CompetitiveDataDTO();
				compData.clear();
				compData.checkItemId = result.getInt("CHECK_DATA_ID");
				compData.regPrice = NumberUtil.RoundFloat(result.getFloat("REG_PRICE"), 2);
				compData.fSalePrice = NumberUtil.RoundFloat(result.getFloat("SALE_PRICE"), 2);
				compData.itemNotFound = result.getString("ITEM_NOT_FOUND_FLG");
				compData.priceNotFound = result.getString("PRICE_NOT_FOUND_FLG");
				compData.saleInd = result.getString("PROMOTION_FLG");
				// compData.chgDirection = result.getInt("CHANGE_DIRECTION");
				compData.compStrId = result.getInt("COMP_STR_ID");
				compData.itemcode = itemCode;
				compData.weekStartDate = result.getString("START_DATE");
				compItemDataList.add(compData);
			}
		} catch (Exception e) {
			throw new GeneralException("Cached Rowset Access Exception", e);
		}
		return compItemDataList;

	}

	private CachedRowSet getItemHistory(Connection conn, int compStrId, String weekStartDate, int itemCode,
			boolean showFlavors) throws GeneralException {
		StringBuffer buffer = new StringBuffer();
		buffer.append(" SELECT A.CHECK_DATA_ID, UNITPRICE(A.REG_PRICE, A.REG_M_PRICE, A.REG_M_PACK) REG_PRICE, ");
		buffer.append(" UNITPRICE(A.SALE_PRICE, A.SALE_M_PRICE, A.SALE_M_PACK) SALE_PRICE, ");
		buffer.append(" A.ITEM_NOT_FOUND_FLG, A.PRICE_NOT_FOUND_FLG, A.PROMOTION_FLG, ");
		buffer.append(" A.COMP_STR_ID, TO_CHAR( A.START_DATE, 'MM/DD/YY') START_DATE ");

		if (showFlavors)
			buffer.append(" FROM Competitive_data  A ");
		else
			buffer.append(" FROM comp_data_view_no_flavor  A ");

		buffer.append(" WHERE A.COMP_STR_ID= ").append(compStrId);
		buffer.append(" AND A.item_code =").append(itemCode);
		buffer.append(" AND A.START_DATE < TO_DATE('").append(weekStartDate).append("', 'MM/DD/YY')");
		buffer.append(" AND A.START_DATE > TO_DATE('").append(weekStartDate).append("', 'MM/DD/YY') - 240");
		buffer.append(" ORDER BY A.START_DATE DESC");
		CachedRowSet result = PristineDBUtil.executeQuery(conn, buffer, "getCompDataForItem");
		return result;
	}

	/*
	 * ,
	 * 
	 * schedule_id = 123 and dept_id = 74 and category_id = 24
	 */
	public ArrayList<CompetitiveDataDTO> getCompData(Connection conn, int scheduleId, int deptId, int catId,
			boolean showFlavors) throws GeneralException {

		ArrayList<CompetitiveDataDTO> compDataList = new ArrayList<CompetitiveDataDTO>();
		StringBuffer sb = new StringBuffer();
		sb.append(
				" select check_data_id, item_code, item_name, unitprice( reg_price, reg_m_price,reg_m_pack) reg_unit_price, ");
		sb.append(" retailer_item_code, upc, dept_name, category_name, ");
		sb.append(" unitprice( sale_price, sale_m_price,sale_m_pack) sale_unit_price, ");
		sb.append(" TO_CHAR(EFF_REG_START_DATE,'MM/DD/YYYY') EFF_REG_START_DATE, ");
		sb.append(" TO_CHAR(START_DATE,'MM/DD/YYYY') WEEK_START_DATE, REG_M_PACK, SALE_M_PACK, ");
		sb.append(
				" promotion_flg, item_not_found_flg, price_not_found_flg, comp_str_id, comp_str_no, PI_ANALYZE_FLAG, DISCONT_FLAG, CATEGORY_ID,");
		sb.append(" NVL(RET_LIR_ID,-1) RET_LIR_ID, IS_ZONE_PRICE_DIFF from ");
		if (showFlavors)
			sb.append(" competitive_data_view  ");
		else
			sb.append(" comp_data_view_no_flavor  ");

		sb.append(" where schedule_id = ").append(scheduleId);
		if (deptId > 0)
			sb.append(" and dept_id = ").append(deptId);
		if (catId > 0)
			sb.append(" and category_id = ").append(catId);

		logger.debug("getCompData : "+ sb);
		CachedRowSet crs = PristineDBUtil.executeQuery(conn, sb, "getCompetitiveData");

		try {
			while (crs.next()) {
				CompetitiveDataDTO compData = new CompetitiveDataDTO();
				compData.itemcode = crs.getInt("item_code");
				compData.itemName = crs.getString("item_name");
				compData.regPrice = crs.getFloat("reg_unit_price");
				compData.fSalePrice = crs.getFloat("sale_unit_price");
				compData.saleInd = crs.getString("promotion_flg");
				compData.itemNotFound = crs.getString("item_not_found_flg");
				compData.priceNotFound = crs.getString("price_not_found_flg");
				compData.compStrId = crs.getInt("comp_str_id");
				compData.compStrNo = crs.getString("comp_str_no");
				compData.checkItemId = crs.getInt("check_data_id");
				compData.weekStartDate = crs.getString("WEEK_START_DATE");
				compData.effRegRetailStartDate = crs.getString("EFF_REG_START_DATE");
				compData.lirId = crs.getInt("RET_LIR_ID");
				compData.piAnalyzeFlag = crs.getString("PI_ANALYZE_FLAG");
				compData.discontFlag = crs.getString("DISCONT_FLAG");
				compData.scheduleId = scheduleId;
				compData.categoryId = crs.getInt("CATEGORY_ID");
				compData.regMPack = crs.getInt("REG_M_PACK");
				compData.saleMPack = crs.getInt("SALE_M_PACK");
				// get dept name, retailer itemocde, upc,size,uom
				compData.deptName = crs.getString("DEPT_NAME");
				compData.categoryName = crs.getString("CATEGORY_NAME");
				compData.upc = crs.getString("UPC");
				compData.retailerItemCode = crs.getString("RETAILER_ITEM_CODE");
				if ("Y".equals(crs.getString("IS_ZONE_PRICE_DIFF")))
					compData.isZonePriceDiff = true;
				else
					compData.isZonePriceDiff = false;
				compDataList.add(compData);
			}
		} catch (Exception e) {
			throw new GeneralException("Cached Rowset Access Exception", e);
		}
		return compDataList;

	}

	public ArrayList<CompetitiveDataDTO> getCompDataPI(Connection conn, int scheduleId) throws GeneralException {
		ArrayList<CompetitiveDataDTO> compDataList = new ArrayList<CompetitiveDataDTO>();
		StringBuffer sb = new StringBuffer();

		sb.append(" SELECT * FROM COMPETITIVE_DATA_PI WHERE SCHEDULE_ID = ");
		sb.append(scheduleId);

		logger.debug(sb);
		CachedRowSet crs = PristineDBUtil.executeQuery(conn, sb, "getCompetitiveData");

		try {
			while (crs.next()) {
				CompetitiveDataDTO compData = new CompetitiveDataDTO();
				compData.checkItemId = crs.getInt("check_data_id");
				compData.itemcode = crs.getInt("item_code");
				compDataList.add(compData);
			}
		} catch (Exception e) {
			throw new GeneralException("Cached Rowset Access Exception", e);
		}
		return compDataList;
	}

	public void insertNonCarriedItem(Connection conn, CompetitiveDataDTO compData) throws GeneralException {

		StringBuffer sb = new StringBuffer();
		sb.append("SELECT UPC FROM NOT_CARRIED_ITEMS WHERE ");
		sb.append(" SCHEDULE_ID = ").append(compData.scheduleId);
		sb.append(" AND UPC = '").append(compData.upc).append("'");
		String upcResult = PristineDBUtil.getSingleColumnVal(conn, sb, "Insert Comp Data - get Comp Data ID");
		if (upcResult == null) {
			sb = new StringBuffer();
			sb.append(" INSERT INTO NOT_CARRIED_ITEMS(SCHEDULE_ID, UPC, ITEM_NAME, ITEM_SIZE, ");
			sb.append(" PACK, UOM_ID,");
			sb.append(" REG_PRICE, REG_M_PACK, REG_M_PRICE, ");
			sb.append(" SALE_PRICE, SALE_M_PACK, SALE_M_PRICE, ");
			sb.append(" PROMOTION_FLG,");
			sb.append(" CHECK_DATETIME, CREATE_DATETIME  ) VALUES( ");
			sb.append(compData.scheduleId).append(", ");
			sb.append("'").append(compData.upc).append("', ");

			String itemName = compData.itemName;
			if (itemName.length() > 70)
				itemName = itemName.substring(0, 70);
			if (compData.itemName != null && !compData.itemName.equals(""))
				sb.append("'").append(itemName).append("', ");
			else
				sb.append("NULL, ");

			if (compData.size != null && !compData.size.equals(""))
				sb.append("'").append(compData.size).append("', ");
			else
				sb.append("NULL, ");

			if (compData.itemPack != null && !compData.itemPack.equals(""))
				sb.append("'").append(compData.itemPack).append("', ");
			else
				sb.append("NULL, ");

			if (compData.uomId != null && !compData.uomId.equals(""))
				sb.append("'").append(compData.uomId).append("', ");
			else
				sb.append("NULL, ");

			sb.append(((compData.regPrice < 0.01) ? "0.0" : PrestoUtil.round(compData.regPrice, 2))).append(",  ");
			sb.append(((compData.regMPack < 0.01) ? "0" : compData.regMPack)).append(",  ");
			sb.append(((compData.regMPrice < 0.01) ? "0.0" : PrestoUtil.round(compData.regMPrice, 2))).append(",  ");
			sb.append(((compData.fSalePrice < 0.01) ? "0.0" : PrestoUtil.round(compData.fSalePrice, 2))).append(",  ");
			sb.append(((compData.saleMPack < 0.01) ? "0" : compData.saleMPack)).append(",  ");
			sb.append(((compData.fSaleMPrice < 0.01) ? "0.0" : PrestoUtil.round(compData.fSaleMPrice, 2)))
					.append(",  ");
			sb.append("'").append(compData.saleInd).append("', ");
			sb.append(" TO_DATE( '").append(compData.checkDate).append("','MM/DD/YYYY'), ");
			sb.append(" SYSDATE ");

			sb.append(" )");
			try {
				PristineDBUtil.execute(conn, sb, "Competitive Data Load - create Not Carried Items");
			} catch (GeneralException e) {
				logger.info(sb.toString());
				logger.error("Competitive Data Load - create Not Carried Items", e);
			}

		}
	}

	public ArrayList getPreviousItemPrices(Connection conn, String scheduleList, int itemCode) throws GeneralException {
		ArrayList<Double> prevPrices = new ArrayList<Double>();
		ArrayList<Double> prevSalePrices = new ArrayList<Double>();
		StringBuffer sb = new StringBuffer();
		/*
		 * select * From ( select unitprice( reg_price, reg_M_price, reg_m_pack)
		 * prev_price, end_date from competitive_data_view where schedule_id in ( 1, 2,
		 * 3) and item_code = 1234 and item_not_found = 'N' and Price_not_found = 'N'
		 * union select min(unitprice( reg_price, reg_M_price, reg_m_pack)) prev_price,
		 * end_date from competitive_data_view where schedule_id in ( 1, 2, 3) and
		 * schedule_id not in ( select schedule_id from competitive_data_view where
		 * schedule_id in ( 1,2,3) and item_code = 1234)) and ret_lir_id = (select
		 * ret_lir_id from item_lookup where item_code = 123) and item_not_found = 'N'
		 * and Price_not_found = 'N' group by end_Date) order by end_date desc
		 */

		sb.append("select * From (");
		sb.append("select unitprice( reg_price, reg_M_price, reg_m_pack) prev_price, end_date, ");
		sb.append("unitprice( sale_price, sale_M_price, sale_m_pack) prev_sale_price from");
		sb.append(" competitive_data_view where schedule_id in (").append(scheduleList).append(")");
		sb.append(" and item_code = ").append(itemCode);
		sb.append(" and item_not_found_flg = 'N' and Price_not_found_flg = 'N') ");
		/*
		 * sb.append(" union "); sb.append(
		 * " select min(unitprice( reg_price, reg_M_price, reg_m_pack)) prev_price, end_date, "
		 * ); sb.append(
		 * " min(unitprice( sale_price, sale_M_price, sale_m_pack)) prev_sale_price from"
		 * ); sb.append(" competitive_data_view where schedule_id in ("
		 * ).append(scheduleList).append(")"); sb.append( " and schedule_id not in ( ");
		 * sb.append( " select schedule_id  from competitive_data_view where ");
		 * sb.append( " schedule_id in ( ").append(scheduleList).append(")");;
		 * sb.append( " and item_code = ").append(itemCode).append(")"); sb.append(
		 * " and ret_lir_id = "); sb.append(
		 * " (select ret_lir_id from item_lookup where item_code = "
		 * ).append(itemCode).append(")"); sb.append(
		 * " and item_not_found_flg = 'N' and Price_not_found_flg = 'N' ");
		 * sb.append(" group by end_Date) ");
		 */
		sb.append(" order by end_date desc");
		// logger.debug(sb);
		CachedRowSet crs = PristineDBUtil.executeQuery(conn, sb, "getCompetitiveData");

		try {
			while (crs.next()) {
				prevPrices.add(crs.getDouble("prev_price"));
				double salePrice = crs.getDouble("prev_sale_price");
				if (salePrice > 0) {
					prevSalePrices.add(salePrice);
				}

			}
		} catch (SQLException e) {
			throw new GeneralException("Cached Rowset Access Exception", e);
		}

		ArrayList prevPriceList = new ArrayList();
		prevPriceList.add(prevPrices);
		prevPriceList.add(prevSalePrices);
		return prevPriceList;
	}

	public void updatePriceChangeStats(Connection conn, CompetitiveDataDTO compData) throws GeneralException {

		StringBuffer sb = new StringBuffer();
		sb.append(" UPDATE COMPETITIVE_DATA SET ");
		sb.append(" CHANGE_DIRECTION = ");
		sb.append(compData.chgDirection);
		sb.append(", CHANGE_DIRECTION_SALE = ");
		sb.append(compData.saleChgDirection);
		sb.append(" WHERE CHECK_DATA_ID = ");
		sb.append(compData.checkItemId);
		int updateCount = PristineDBUtil.executeUpdate(conn, sb, "Competitive DATA - Update");
		if (updateCount != 1) {
			logger.error("Update Comp Data Unsuccessful, record count = " + updateCount);
		}
		return;
	}

	public void updatePriceChangeStatsAllItems(Connection conn, int scheduleId, int regPriceChangeDir)
			throws GeneralException {

		StringBuffer sb = new StringBuffer();
		sb.append(" UPDATE COMPETITIVE_DATA SET ");
		sb.append(" CHANGE_DIRECTION = ");
		sb.append(regPriceChangeDir);
		sb.append(", CHANGE_DIRECTION_SALE = ");
		sb.append(Constants.PRICE_NA);
		sb.append(" WHERE Schedule_id = ");
		sb.append(scheduleId);
		int updateCount = PristineDBUtil.executeUpdate(conn, sb, "Competitive DATA - Update");
		logger.debug("Update Comp Data, record count = " + updateCount);
		return;

	}

	public ArrayList<CompetitiveDataDTO> getCompDataForDebug(Connection conn, int scheduleId, int deptId, int catId,
			int lirId, boolean showFlavors) throws GeneralException {

		ArrayList<CompetitiveDataDTO> compDataList = new ArrayList<CompetitiveDataDTO>();
		StringBuffer sb = new StringBuffer();
		sb.append(
				" select A.check_data_id, A.item_code, A.item_name, unitprice( A.reg_price, A.reg_m_price, A.reg_m_pack) reg_unit_price, ");
		sb.append(" unitprice( A.sale_price, A.sale_m_price, A.sale_m_pack) sale_unit_price, ");
		sb.append(" TO_CHAR(A.EFF_REG_START_DATE,'MM/DD/YYYY') EFF_REG_START_DATE, ");
		sb.append(" TO_CHAR(A.START_DATE,'MM/DD/YYYY') WEEK_START_DATE, ");
		sb.append(
				" A.promotion_flg, A.item_not_found_flg, A.price_not_found_flg, A.comp_str_id, A.comp_str_no, A.PI_ANALYZE_FLAG, A.DISCONT_FLAG,");
		sb.append(" NVL(A.RET_LIR_ID,-1) RET_LIR_ID,  ");
		sb.append(" A.DEPT_NAME, A.RETAILER_ITEM_CODE, A.UPC, A.ITEM_SIZE, A.UOM_NAME, ");
		if (showFlavors) {
			sb.append(" B.QUANTITY_REGULAR+ B.QUANTITY_SALE QUANTITY_SOLD ");
			sb.append(" from competitive_data_view A, MOVEMENT_WEEKLY B ");
			sb.append(" WHERE b.check_data_id(+) = A.check_data_id AND");

		} else {
			sb.append(" A.QUANTITY_REGULAR + A.QUANTITY_SALE QUANTITY_SOLD");
			sb.append(" from competitive_data_pi_view A WHERE ");
		}

		sb.append(" schedule_id = ").append(scheduleId);
		if (deptId > 0)
			sb.append(" and dept_id = ").append(deptId);
		if (catId > 0)
			sb.append(" and category_id = ").append(catId);
		if (lirId > 0)
			sb.append(" and A.RET_LIR_ID = ").append(lirId);
		sb.append(" AND  A.item_not_found_flg = 'N'");
		// sb.append(" AND ROWNUM < 50");

		// logger.debug(sb);
		CachedRowSet crs = PristineDBUtil.executeQuery(conn, sb, "getCompetitiveData");

		try {
			while (crs.next()) {
				CompetitiveDataDTO compData = new CompetitiveDataDTO();
				compData.itemcode = crs.getInt("item_code");
				compData.itemName = crs.getString("item_name");
				compData.regPrice = crs.getFloat("reg_unit_price");
				compData.fSalePrice = crs.getFloat("sale_unit_price");
				compData.saleInd = crs.getString("promotion_flg");
				compData.itemNotFound = crs.getString("item_not_found_flg");
				compData.priceNotFound = crs.getString("price_not_found_flg");
				compData.compStrId = crs.getInt("comp_str_id");
				compData.compStrNo = crs.getString("comp_str_no");
				compData.checkItemId = crs.getInt("check_data_id");
				compData.weekStartDate = crs.getString("WEEK_START_DATE");
				compData.effRegRetailStartDate = crs.getString("EFF_REG_START_DATE");
				compData.lirId = crs.getInt("RET_LIR_ID");
				compData.piAnalyzeFlag = crs.getString("PI_ANALYZE_FLAG");
				compData.discontFlag = crs.getString("DISCONT_FLAG");
				compData.scheduleId = scheduleId;

				compData.deptName = crs.getString("DEPT_NAME");
				compData.retailerItemCode = crs.getString("RETAILER_ITEM_CODE");
				compData.upc = crs.getString("UPC");
				compData.size = crs.getString("ITEM_SIZE");
				compData.uom = crs.getString("UOM_NAME");

				String quantitySold = crs.getString("QUANTITY_SOLD");
				if (quantitySold != null && !quantitySold.equals(""))
					compData.quantitySold = Float.parseFloat(quantitySold);
				else
					compData.quantitySold = 0;

				compDataList.add(compData);
			}
		} catch (Exception e) {
			throw new GeneralException("Cached Rowset Access Exception", e);
		}
		return compDataList;

	}

	/**
	 * This method sets isSubscriber flag and chain id. This method is primarily
	 * used when computing movement weekly to include chain id condition in
	 * getStoreIdList() of this class
	 * 
	 * @param conn         Database Connection
	 * @param isSubscriber Subscriber Flag
	 * @throws GeneralException
	 */
	public void setSubscriber(Connection conn, boolean isSubscriber) throws GeneralException {
		this.isSubscriber = isSubscriber;
	}

	/**
	 * This method inserts records in competitive data table
	 * 
	 * @param toBeInsertedList List of records to be inserted
	 * @throws GeneralException
	 */
	public void insertCompetitiveData(List<CompetitiveDataDTO> toBeInsertedList) throws GeneralException {
		logger.debug("Inside insertCompetitiveData() of CompetitiveDataDAO");
		PreparedStatement statement = null;
		try {
			statement = connection.prepareStatement(INSERT_COMPETITIVE_DATA);

			int itemNoInBatch = 0;
			for (CompetitiveDataDTO compData : toBeInsertedList) {
				int counter = 0;
				float regPrice = (float) ((compData.regPrice < 0.01) ? 0.0 : PrestoUtil.round(compData.regPrice, 2));
				int regMPack = (compData.regMPack < 0.01) ? 0 : compData.regMPack;
				float regMPrice = (float) ((compData.regMPrice < 0.01) ? 0.0 : PrestoUtil.round(compData.regMPrice, 2));
				float fSalePrice = (float) ((compData.fSalePrice < 0.01) ? 0.0
						: PrestoUtil.round(compData.fSalePrice, 2));
				int saleMPack = (compData.saleMPack < 0.01) ? 0 : compData.saleMPack;
				float fSaleMPrice = (float) ((compData.fSaleMPrice < 0.01) ? 0.0
						: PrestoUtil.round(compData.fSaleMPrice, 2));

				if (compData.outSideRangeInd == null || compData.outSideRangeInd.equals(""))
					compData.outSideRangeInd = "N";

				statement.setInt(++counter, compData.scheduleId);
				statement.setInt(++counter, compData.itemcode);
				statement.setFloat(++counter, regPrice);
				statement.setInt(++counter, regMPack);
				statement.setFloat(++counter, regMPrice);
				statement.setFloat(++counter, fSalePrice);
				statement.setInt(++counter, saleMPack);
				statement.setFloat(++counter, fSaleMPrice);
				statement.setString(++counter, compData.itemNotFound);
				statement.setString(++counter, compData.priceNotFound);
				statement.setString(++counter, compData.saleInd);
				statement.setString(++counter, compData.outSideRangeInd);
				statement.setString(++counter,
						(compData.newUOM == null || compData.newUOM.equals("")) ? null : compData.newUOM);

				if (compData.effRegRetailStartDate == null || compData.effRegRetailStartDate.equals(""))
					statement.setString(++counter, null);
				else
					statement.setString(++counter, compData.effRegRetailStartDate);

				if (compData.effSaleStartDate == null || compData.effSaleStartDate.equals(""))
					statement.setString(++counter, null);
				else
					statement.setString(++counter, compData.effSaleStartDate);

				if (compData.effSaleEndDate == null || compData.effSaleEndDate.equals(""))
					statement.setString(++counter, null);
				else
					statement.setString(++counter, compData.effSaleEndDate);

				statement.setString(++counter, compData.checkDate);

				statement.setString(++counter, compData.comment);

				statement.addBatch();
				itemNoInBatch++;

				if (itemNoInBatch % Constants.BATCH_UPDATE_COUNT == 0) {
					long startTime = System.currentTimeMillis();
					int[] count = statement.executeBatch();
					long endTime = System.currentTimeMillis();
					logger.debug("Time taken for inserting a batch - " + (endTime - startTime) + "ms");
					statement.clearBatch();
					itemNoInBatch = 0;
				}
			}
			if (itemNoInBatch > 0) {
				int[] count = statement.executeBatch();
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
		logger.debug("Inside updateCompetitiveData() of CompetitiveDataDAO");
		PreparedStatement statement = null;
		try {
			statement = connection.prepareStatement(UPDATE_COMPETITIVE_DATA);

			int itemNoInBatch = 0;
			for (CompetitiveDataDTO compData : toBeUpdatedList) {
				int counter = 0;
				float regPrice = (float) ((compData.regPrice < 0.01) ? 0.0 : PrestoUtil.round(compData.regPrice, 2));
				int regMPack = (compData.regMPack < 0.01) ? 0 : compData.regMPack;
				float regMPrice = (float) ((compData.regMPrice < 0.01) ? 0.0 : PrestoUtil.round(compData.regMPrice, 2));
				float fSalePrice = (float) ((compData.fSalePrice < 0.01) ? 0.0
						: PrestoUtil.round(compData.fSalePrice, 2));
				int saleMPack = (compData.saleMPack < 0.01) ? 0 : compData.saleMPack;
				float fSaleMPrice = (float) ((compData.fSaleMPrice < 0.01) ? 0.0
						: PrestoUtil.round(compData.fSaleMPrice, 2));

				statement.setFloat(++counter, regPrice);
				statement.setInt(++counter, regMPack);
				statement.setFloat(++counter, regMPrice);
				statement.setFloat(++counter, fSalePrice);
				statement.setInt(++counter, saleMPack);
				statement.setFloat(++counter, fSaleMPrice);
				statement.setString(++counter, compData.itemNotFound);
				statement.setString(++counter, compData.priceNotFound);
				statement.setString(++counter, compData.saleInd);

				if (compData.newUOM == null)
					compData.newUOM = Constants.EMPTY;
				statement.setString(++counter, (compData.newUOM.equals("")) ? null : compData.newUOM);

				if (compData.effRegRetailStartDate != null && !compData.effRegRetailStartDate.equals(""))
					statement.setString(++counter, compData.effRegRetailStartDate);
				else
					statement.setString(++counter, null);

				if (compData.effSaleStartDate == null || compData.effSaleStartDate.equals(""))
					statement.setString(++counter, null);
				else
					statement.setString(++counter, compData.effSaleStartDate);

				if (compData.effSaleEndDate == null || compData.effSaleEndDate.equals(""))
					statement.setString(++counter, null);
				else
					statement.setString(++counter, compData.effSaleEndDate);

				statement.setString(++counter, compData.checkDate);
				statement.setString(++counter, compData.comment);
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
	 * This method inserts records in not_carried_items table
	 * 
	 * @param toBeInsertedList List of records to be inserted
	 * @throws GeneralException
	 */
	public void insertNotCarriedItems(List<CompetitiveDataDTO> toBeInsertedList) throws GeneralException {
		logger.debug("Inside insertNotCarriedItems() of CompetitiveDataDAO");
		PreparedStatement statement = null;
		try {
			statement = connection.prepareStatement(INSERT_NOT_CARRIED_ITEMS);

			int itemNoInBatch = 0;
			for (CompetitiveDataDTO compData : toBeInsertedList) {
				int counter = 0;
				float regPrice = (float) ((compData.regPrice < 0.01) ? 0.0 : PrestoUtil.round(compData.regPrice, 2));
				int regMPack = (compData.regMPack < 0.01) ? 0 : compData.regMPack;
				float regMPrice = (float) ((compData.regMPrice < 0.01) ? 0.0 : PrestoUtil.round(compData.regMPrice, 2));
				float fSalePrice = (float) ((compData.fSalePrice < 0.01) ? 0.0
						: PrestoUtil.round(compData.fSalePrice, 2));
				int saleMPack = (compData.saleMPack < 0.01) ? 0 : compData.saleMPack;
				float fSaleMPrice = (float) (((compData.fSaleMPrice < 0.01) ? 0.0
						: PrestoUtil.round(compData.fSaleMPrice, 2)));

				statement.setInt(++counter, compData.scheduleId);
				statement.setString(++counter, compData.upc);

				String itemName = compData.itemName;
				if (itemName.length() > 70)
					itemName = itemName.substring(0, 70);
				if (compData.itemName != null && !compData.itemName.equals(""))
					statement.setString(++counter, itemName);
				else
					statement.setString(++counter, null);

				if (compData.size != null && !compData.size.equals(""))
					statement.setString(++counter, compData.size);
				else
					statement.setString(++counter, null);

				if (compData.itemPack != null && !compData.itemPack.equals(""))
					statement.setString(++counter, compData.itemPack);
				else
					statement.setString(++counter, null);

				if (compData.uomId != null && !compData.uomId.equals(""))
					statement.setString(++counter, compData.uomId);
				else
					statement.setString(++counter, null);

				statement.setFloat(++counter, regPrice);
				statement.setInt(++counter, regMPack);
				statement.setFloat(++counter, regMPrice);
				statement.setFloat(++counter, fSalePrice);
				statement.setInt(++counter, saleMPack);
				statement.setFloat(++counter, fSaleMPrice);

				statement.setString(++counter, compData.saleInd);
				statement.setString(++counter, compData.checkDate);

				statement.addBatch();
				itemNoInBatch++;

				if (itemNoInBatch % Constants.BATCH_UPDATE_COUNT == 0) {
					long startTime = System.currentTimeMillis();
					int[] count = null;
					try {
						count = statement.executeBatch();
					} catch (BatchUpdateException bue) {

						// Skip if the record is already inserted
						// logger.debug("Not Carried Failed Items - " +
						// bue.getUpdateCounts() );
					}
					long endTime = System.currentTimeMillis();
					logger.debug("Time taken for inserting a batch - " + (endTime - startTime) + "ms");
					statement.clearBatch();
					itemNoInBatch = 0;
				}
			}
			if (itemNoInBatch > 0) {
				int[] count = null;
				try {
					count = statement.executeBatch();
				} catch (BatchUpdateException bue) {
					// Skip if the record is already inserted
					// logger.debug("Not Carried Failed Items - " +
					// bue.getUpdateCounts() );
				}
				statement.clearBatch();
			}
		}

		catch (SQLException e) {
			logger.error("Error while executing insertNotCarriedItems");
			throw new GeneralException("Error while executing insertNotCarriedItems", e);
		} finally {
			PristineDBUtil.close(statement);
		}
	}

	public int updateSuspectFlag(Connection conn, ArrayList<CompetitiveDataDTO> suspectCheckIdList)
			throws GeneralException {

		int updateCount = -1;
		StringBuffer sb = new StringBuffer();
		sb.append(" UPDATE COMPETITIVE_DATA SET ");
		sb.append(" SUSPECT = ?, COMMENTS = SUBSTR(? || COMMENTS,1,300) ");
		sb.append(" WHERE CHECK_DATA_ID = ?");
		PreparedStatement psmt = null;
		try {
			if (suspectCheckIdList.size() > 0) {
				psmt = conn.prepareStatement(sb.toString());
				for (CompetitiveDataDTO compData : suspectCheckIdList) {
					psmt.setString(1, "Y");
					psmt.setString(2, compData.comment);
					psmt.setInt(3, compData.checkItemId);
					psmt.addBatch();
				}
				psmt.executeBatch();
				psmt.close();
			}
		} catch (SQLException sqlce) {
			throw new GeneralException("Unexpected SQL Exception", sqlce);
		} finally {
			PristineDBUtil.close(psmt);
		}
		return updateCount;
	}

	/**
	 * This method updates records in competitive data table
	 * 
	 * @param toBeUpdatedList List of records to be updated
	 * @throws GeneralException
	 */
	public void updateCompetitiveDataComments(List<CompetitiveDataDTO> toBeUpdatedList) throws GeneralException {
		logger.debug("Inside updateCompetitiveDataComments() of CompetitiveDataDAO");
		PreparedStatement statement = null;
		try {
			statement = connection.prepareStatement(UPDATE_COMPETITIVE_DATA_COMMENTS);

			int itemNoInBatch = 0;
			for (CompetitiveDataDTO compData : toBeUpdatedList) {
				int counter = 0;
				statement.setString(++counter, compData.comment);
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
			logger.error("Error while executing UPDATE_COMPETITIVE_DATA_COMMENTS");
			throw new GeneralException("Error while executing UPDATE_COMPETITIVE_DATA_COMMENTS", e);
		} finally {
			PristineDBUtil.close(statement);
		}
	}

	public HashMap<String, CompetitiveDataDTO> getCompData(Connection conn, int scheduleId) throws GeneralException {

		HashMap<String, CompetitiveDataDTO> compDataMap = new HashMap<String, CompetitiveDataDTO>();
		PreparedStatement stmt = null;
		ResultSet rs = null;
		try {
			stmt = conn.prepareStatement(GET_COMPETITION_DATA);
			stmt.setInt(1, scheduleId);
			rs = stmt.executeQuery();
			while (rs.next()) {
				CompetitiveDataDTO compData = new CompetitiveDataDTO();
				compData.itemcode = rs.getInt("ITEM_CODE");
				compData.scheduleId = rs.getInt("SCHEDULE_ID");
				compData.regPrice = rs.getFloat("REG_PRICE");
				compData.regMPack = rs.getInt("REG_M_PACK");
				compData.regMPrice = rs.getFloat("REG_M_PRICE");
				compData.fSalePrice = rs.getFloat("SALE_PRICE");
				compData.saleMPack = rs.getInt("SALE_M_PACK");
				compData.fSaleMPrice = rs.getFloat("SALE_M_PRICE");
				compData.saleInd = rs.getString("PROMOTION_FLG");
				compData.itemNotFound = rs.getString("ITEM_NOT_FOUND_FLG");
				compData.priceNotFound = rs.getString("PRICE_NOT_FOUND_FLG");
				compData.outSideRangeInd = rs.getString("OUTSIDE_RANGE_IND");
				compData.newUOM = rs.getString("NEW_UOM");
				compData.effRegRetailStartDate = rs.getString("EFF_REG_START_DATE");
				compData.effSaleEndDate = rs.getString("EFF_SALE_START_DATE");
				compData.effSaleEndDate = rs.getString("EFF_SALE_END_DATE");
				compData.checkDate = rs.getString("CHECK_DATE");
				compData.comment = rs.getString("COMMENTS");
				compData.upc = rs.getString("UPC");
				compDataMap.put(compData.upc, compData);
			}
		} catch (Exception e) {
			throw new GeneralException("Error when retrieving competition data " + e);
		} finally {
			PristineDBUtil.close(stmt);
		}
		return compDataMap;

	}

	public void insertRawCompData(List<CompetitiveDataDTO> compDataList) throws GeneralException {
		logger.debug("Inside insertRawCompData() of CompetitiveDataDAO");
		PreparedStatement statement = null;
		try {
			statement = connection.prepareStatement(INSERT_INTO_RAW_COMPETITIVE_DATA_TEST);

			int itemNoInBatch = 0;
			for (CompetitiveDataDTO compData : compDataList) {
				int counter = 0;
				statement.setString(++counter, compData.compStrNo);
				statement.setString(++counter, compData.weekStartDate);
				statement.setString(++counter, compData.checkDate);
				statement.setInt(++counter, compData.itemcode);
				statement.setString(++counter, compData.itemName);
				statement.setInt(++counter, compData.regMPack);
				statement.setDouble(++counter, compData.regPrice);
				statement.setString(++counter, compData.size);
				statement.setString(++counter, compData.upc);
				statement.setString(++counter, compData.outSideRangeInd);
				statement.setString(++counter, compData.comment);
				statement.setInt(++counter, compData.saleMPack);
				statement.setDouble(++counter, compData.fSalePrice);
				statement.setString(++counter, null);
				statement.setString(++counter, compData.plFlag);
				logger.debug(compData.compStrNo + " " + compData.weekStartDate + " " + compData.checkDate + " "
						+ compData.itemcode + compData.itemName + " " + compData.regMPack + " " + compData.regPrice
						+ " " + compData.size + " " + compData.upc + " " + compData.outSideRangeInd + " "
						+ compData.comment + " " + compData.saleMPack + " " + compData.fSalePrice + " "
						+ compData.effSaleStartDate + " " + compData.plFlag);
				statement.addBatch();
				itemNoInBatch++;

				if (itemNoInBatch % Constants.BATCH_UPDATE_COUNT == 0) {
					long startTime = System.currentTimeMillis();
					int[] count = statement.executeBatch();
					long endTime = System.currentTimeMillis();
					logger.debug("Time taken for inserting a batch - " + (endTime - startTime) + "ms");
					statement.clearBatch();
					itemNoInBatch = 0;
				}
			}
			if (itemNoInBatch > 0) {
				int[] count = statement.executeBatch();
				statement.clearBatch();
			}
		} catch (SQLException e) {
			logger.error("Error while executing insertRawCompData");
			throw new GeneralException("Error while executing insertRawCompData", e);
		} finally {
			PristineDBUtil.close(statement);
		}
	}

	public HashMap<String, List<CompetitiveDataDTO>> getRawCompetitiveData(String weekStartDate, String storeNumbers)
			throws GeneralException {
		HashMap<String, List<CompetitiveDataDTO>> compDataMap = new HashMap<String, List<CompetitiveDataDTO>>();
		PreparedStatement stmt = null;
		ResultSet rs = null;
		try {

			String query = GET_COMP_DATA_FROM_RAW_COMPETITIVE_DATA.replace("%STORE_NUMBERS%", storeNumbers);
			stmt = connection.prepareStatement(query);
			stmt.setString(1, weekStartDate);
			stmt.setFetchSize(400000);
			rs = stmt.executeQuery();
			while (rs.next()) {
				CompetitiveDataDTO compData = new CompetitiveDataDTO();
				compData.compStrNo = rs.getString("COMP_STR_NO");
				compData.weekStartDate = rs.getString("WEEK_START_DATE");

				/*
				 * compData.weekStartDate = DateUtil.dateToString(rs.getDate("WEEK_START_DATE"),
				 * Constants.APP_DATE_FORMAT);
				 */
				// compData.checkDate = DateUtil.dateToString(rs.getDate("CHECK_DATETIME"),
				// Constants.APP_DATE_FORMAT);
				compData.itemCodeNo = rs.getString("ITEM_CODE");
				compData.itemName = rs.getString("ITEM_DESC");
				compData.regPrice = rs.getFloat("REG_PRICE");
				compData.regMPack = rs.getInt("REG_M_PACK");
				compData.size = rs.getString("SIZE_UOM");
				compData.upc = rs.getString("UPC");
				compData.fSalePrice = rs.getFloat("SALE_PRICE");
				compData.saleMPack = rs.getInt("SALE_M_PACK");
				compData.categoryName = rs.getString("CATEGORY");
				compData.compItemAddlDesc = rs.getString("ADDITIONAL_INFO");
				/*
				 * compData.outSideRangeInd = rs.getString("OUTSIDE_RANGE_IND");
				 * compData.effSaleStartDate = rs.getString("SALE_START_DATE"); compData.comment
				 * = rs.getString("ADDITIONAL_INFO"); compData.plFlag = rs.getString("PL_IND");
				 */
				List<CompetitiveDataDTO> tempList = new ArrayList<CompetitiveDataDTO>();
				if (compDataMap.containsKey(compData.weekStartDate)) {
					tempList.addAll(compDataMap.get(compData.weekStartDate));
				}

				tempList.add(compData);

				compDataMap.put(compData.weekStartDate, tempList);

			}

		} catch (Exception e) {
			throw new GeneralException("Error when retrieving competition data " + e);
		} finally {
			PristineDBUtil.close(stmt);
		}
		logger.info("getRawCompetitiveData()- Records fetched successfully : " + compDataMap.size());
		return compDataMap;

	}

	public HashMap<String, CompetitiveDataDTO> getCompStoreInfo() throws GeneralException {
		HashMap<String, CompetitiveDataDTO> compDataMap = new HashMap<String, CompetitiveDataDTO>();
		PreparedStatement stmt = null;
		ResultSet rs = null;
		try {
			stmt = connection.prepareStatement(GET_COMPETITOR_STORE_INFO);
			rs = stmt.executeQuery();
			while (rs.next()) {
				CompetitiveDataDTO compData = new CompetitiveDataDTO();
				compData.compStrNo = rs.getString("COMP_STR_NO");
				compData.storeNo = rs.getString("STORE_NO");
				compData.storeName = rs.getString("STORE_NAME");
				compData.storeAddr = rs.getString("STORE_DETAILS");
				compDataMap.put(compData.compStrNo, compData);
			}
		} catch (Exception e) {
			throw new GeneralException("Error when retrieving competition data " + e);
		} finally {
			PristineDBUtil.close(stmt);
		}
		return compDataMap;

	}

	public HashMap<String, String> getCompStoreNO() throws GeneralException {
		HashMap<String, String> compDataMap = new HashMap<String, String>();
		PreparedStatement stmt = null;
		ResultSet rs = null;
		try {
			stmt = connection.prepareStatement(GET_COMPETITOR_STORE_NO);
			rs = stmt.executeQuery();
			while (rs.next()) {
				compDataMap.put(rs.getString("NAME"), rs.getString("COMP_STR_NO"));
			}
		} catch (Exception e) {
			throw new GeneralException("Error when retrieving COMPETITOR STORE NUMBER IN getCompStoreNO() " + e);
		} finally {
			PristineDBUtil.close(stmt);
		}
		return compDataMap;

	}

	/**
	 * This method updates records in competitive data table
	 * 
	 * @param toBeUpdatedList List of records to be updated
	 * @throws GeneralException
	 */
	public void updateExportInd(Set<CompStoreItemDetailsKey> storeItemDetailsKey, String startDate)
			throws GeneralException {
		logger.debug("Inside updateCompetitiveDataComments() of CompetitiveDataDAO");
		PreparedStatement statement = null;
		try {
			statement = connection.prepareStatement(UPDATE_RAW_COMPETITIVE_DATA_EXPORT_IND);

			int itemNoInBatch = 0;
			for (CompStoreItemDetailsKey compData : storeItemDetailsKey) {
				int counter = 0;
				statement.setString(++counter, compData.getCompStrNo());
				statement.setString(++counter, compData.getUpc());
				statement.setString(++counter, startDate);

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
			logger.error("Error while executing updateExportInd");
			throw new GeneralException("Error while executing updateExportInd", e);
		} finally {
			PristineDBUtil.close(statement);
		}
	}

	public void insertCompUPCMapping(Connection conn,
			HashMap<String, List<CompetitiveDataDTO>> compDetailBasedOnStoreLocation,
			HashMap<String, Integer> compStrAndChainInfo) throws GeneralException {
		PreparedStatement statement = null;
		try {
			statement = conn.prepareStatement(INSERT_INTO_COMP_UPC_MAPPING);
			int itemNoInBatch = 0;
			for (Map.Entry<String, List<CompetitiveDataDTO>> entry : compDetailBasedOnStoreLocation.entrySet()) {
				String compStrNo = null;

				if ("colWM".equals(entry.getKey())) {
					compStrNo = "WM-7";
				} else if ("colKR".equals(entry.getKey())) {
					compStrNo = "KR-42";
				} else if ("colMJ".equals(entry.getKey())) {
					compStrNo = "MJ-106";
				} else if ("codWM".equals(entry.getKey())) {
					compStrNo = "WM-360";
				} else if ("podWM".equals(entry.getKey())) {
					compStrNo = "WM-32";
				}
				if ((compStrNo != null && compStrNo != "" && compStrAndChainInfo.containsKey(compStrNo))
						|| compStrAndChainInfo.containsKey(entry.getKey())) {
					for (CompetitiveDataDTO compData : entry.getValue()) {
						if (compData.compSKU != null && !compData.compSKU.trim().isEmpty()) {
							int counter = 0;
							statement.setString(++counter, compData.upc);
							statement.setString(++counter, compData.retailerItemCode);
							statement.setString(++counter, compData.itemName);
							statement.setString(++counter, compData.compSKU);
							if (compStrAndChainInfo.containsKey(compStrNo)) {
								statement.setInt(++counter, Constants.STORE_LEVEL_ID);
								statement.setInt(++counter, compStrAndChainInfo.get(compStrNo));
							} else if (compStrAndChainInfo.containsKey(entry.getKey())) {
								statement.setInt(++counter, Constants.CHAIN_LEVEL_ID);
								statement.setInt(++counter, compStrAndChainInfo.get(entry.getKey()));
							}

							statement.addBatch();
							itemNoInBatch++;
						}

						if (itemNoInBatch % Constants.BATCH_UPDATE_COUNT == 0) {
							int[] count = statement.executeBatch();
							statement.clearBatch();
							itemNoInBatch = 0;
						}
					}
				} else {
					logger.error(
							"Comp Store number not found for Comp and Store number combinations: " + entry.getKey());
				}
			}
			if (itemNoInBatch > 0) {
				int[] count = statement.executeBatch();
				statement.clearBatch();
			}
		} catch (SQLException sqlE) {
			logger.error("Error -- insertCompUPCMapping()", sqlE);
			throw new GeneralException("Error -- insertCompUPCMapping()", sqlE);
		} finally {
			PristineDBUtil.close(statement);
		}
	}

	public void insertCompUPCMapping(Connection conn,
			HashMap<String, List<CompetitiveDataDTO>> compDetailBasedOnStoreLocation,
			HashMap<String, Integer> compStrAndChainInfo, HashMap<String, List<ItemDTO>> retItemCodeAndItem)
			throws GeneralException {
		PreparedStatement statement = null;
		try {
			statement = conn.prepareStatement(INSERT_INTO_COMP_UPC_MAPPING);
			int itemNoInBatch = 0;
			for (Map.Entry<String, List<CompetitiveDataDTO>> entry : compDetailBasedOnStoreLocation.entrySet()) {
				String compStrNo = null;

				if ((compStrNo != null && compStrNo != "" && compStrAndChainInfo.containsKey(compStrNo))
						|| compStrAndChainInfo.containsKey(entry.getKey())) {
					for (CompetitiveDataDTO compData : entry.getValue()) {
//						if(retItemCodeAndItem.get(compData.retailerItemCode)!= null) {
//							for(ItemDTO itemDTO: retItemCodeAndItem.get(compData.retailerItemCode)) {
						int counter = 0;
						statement.setString(++counter, compData.upc);
						statement.setString(++counter, compData.retailerItemCode);
						statement.setString(++counter, compData.itemName);
						statement.setString(++counter, compData.compSKU);
						if (compStrAndChainInfo.containsKey(compStrNo)) {
							statement.setInt(++counter, Constants.STORE_LEVEL_ID);
							statement.setInt(++counter, compStrAndChainInfo.get(compStrNo));
						} else if (compStrAndChainInfo.containsKey(entry.getKey())) {
							statement.setInt(++counter, Constants.CHAIN_LEVEL_ID);
							statement.setInt(++counter, compStrAndChainInfo.get(entry.getKey()));
						}

						statement.addBatch();
						itemNoInBatch++;
//							}
//						}

						if (itemNoInBatch % Constants.BATCH_UPDATE_COUNT == 0) {
							int[] count = statement.executeBatch();
							statement.clearBatch();
							itemNoInBatch = 0;
						}
					}
				} else {
					logger.error(
							"Comp Store number not found for Comp and Store number combinations: " + entry.getKey());
				}
			}
			if (itemNoInBatch > 0) {
				int[] count = statement.executeBatch();
				statement.clearBatch();
			}
		} catch (SQLException sqlE) {
			logger.error("Error -- insertCompUPCMapping()", sqlE);
			throw new GeneralException("Error -- insertCompUPCMapping()", sqlE);
		} finally {
			PristineDBUtil.close(statement);
		}
	}

	public HashMap<String, List<String>> getCompUPCMapping(Connection conn, String compStrNo) throws GeneralException {
		HashMap<String, List<String>> compDataMap = new HashMap<>();
		PreparedStatement stmt = null;
		ResultSet rs = null;
		try {
			stmt = connection.prepareStatement(GET_COMP_UPC_MAPPING);
			stmt.setInt(1, Constants.STORE_LEVEL_ID);
			stmt.setInt(2, Constants.CHAIN_LEVEL_ID);
			stmt.setString(3, compStrNo);
			stmt.setString(4, compStrNo);
			rs = stmt.executeQuery();
			while (rs.next()) {
				List<String> tempList = new ArrayList<>();
				if (compDataMap.containsKey(rs.getString("COMP_SKU_OR_UPC"))) {
					tempList = compDataMap.get(rs.getString("COMP_SKU_OR_UPC"));
				}
				tempList.add(rs.getString("BASE_UPC"));
				compDataMap.put(rs.getString("COMP_SKU_OR_UPC"), tempList);
			}
		} catch (Exception e) {
			throw new GeneralException("Error when retrieving data from getCompUPCMapping " + e);
		} finally {
			PristineDBUtil.close(stmt);
		}
		return compDataMap;
	}

	public void getCompChainId(HashMap<String, Integer> compDataMap) throws GeneralException {
		PreparedStatement stmt = null;
		ResultSet rs = null;
		try {
			stmt = connection.prepareStatement(GET_COMPETITOR_CHAIN_NO);
			rs = stmt.executeQuery();
			while (rs.next()) {
				compDataMap.put(rs.getString("COMP_CHAIN_NAME"), rs.getInt("COMP_CHAIN_ID"));
			}
		} catch (Exception e) {
			throw new GeneralException("Error when retrieving COMPETITOR STORE NUMBER IN getCompStoreNO() " + e);
		} finally {
			PristineDBUtil.close(stmt);
		}
	}

	public HashMap<String, Integer> getCompStoreId() throws GeneralException {
		HashMap<String, Integer> compDataMap = new HashMap<>();
		PreparedStatement stmt = null;
		ResultSet rs = null;
		try {
			stmt = connection.prepareStatement(GET_COMPETITOR_STORE_ID);
			rs = stmt.executeQuery();
			while (rs.next()) {
				compDataMap.put(rs.getString("COMP_STR_NO"), rs.getInt("COMP_STR_ID"));
			}
		} catch (Exception e) {
			throw new GeneralException("Error when retrieving COMPETITOR STORE NUMBER IN getCompStoreNO() " + e);
		} finally {
			PristineDBUtil.close(stmt);
		}
		return compDataMap;

	}
}
