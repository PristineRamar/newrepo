package com.pristine.dao.offermgmt.oos;

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
import com.pristine.dto.offermgmt.LocationKey;
import com.pristine.dto.offermgmt.MultiplePrice;
import com.pristine.dto.offermgmt.ProductKey;
import com.pristine.dto.offermgmt.oos.OOSAlertInputDTO;
import com.pristine.dto.offermgmt.oos.OOSEmailAlertDTO;
import com.pristine.dto.offermgmt.oos.OOSItemDTO;
import com.pristine.exception.GeneralException;
import com.pristine.service.ProductService;
import com.pristine.service.offermgmt.oos.OOSAlertItemKey;
import com.pristine.util.Constants;
import com.pristine.util.PristineDBUtil;
import com.pristine.util.offermgmt.PRCommonUtil;

public class OOSAlertDAO {

	private static Logger logger = Logger.getLogger("OOSAlertDAO");

	private static final String GET_OOS_ITEM = 
			  " SELECT STORE_ITEM_MAP.*, PG.NAME AS CATEGORY_NAME, PG1.NAME AS DEPARTMENT_NAME FROM "
			+ "(SELECT OI.LOCATION_ID, OI.LOCATION_LEVEL_ID, OI.PRODUCT_LEVEL_ID, OI.PRODUCT_ID, IL.ITEM_NAME, "
			+ " IL.RETAILER_ITEM_CODE, IL.UPC, IL.ITEM_SIZE, OI.PRODUCT_ID ITEM_CODE, "
			+ " UL.NAME UOM, CS.COMP_STR_NO, LIR.RET_LIR_NAME, OI.REG_QUANTITY, OI.REG_PRICE, " 
			+ " OI.SALE_QUANTITY, OI.SALE_PRICE, OI.AD_PAGE_NO, OI.DISPLAY_TYPE_ID, OI.ZERO_MOV_X_WEEKS, "
			+ " OI.MIN_MOV_X_WEEKS, OI.MAX_MOV_X_WEEKS, OI.AVG_MOV_X_WEEKS, OI.PREDICTED_MOVEMENT, "
			+ " OI.CONFIDENCE_LEVEL_LOWER, OI.CONFIDENCE_LEVEL_UPPER, OI.CALENDAR_ID, OI.DAY_PART_ID, "
			+ " RD.NAME DISTRICT_NAME, OI.SEND_TO_CLIENT, OI.ALERT_SEND_STATUS, OI.ACTUAL_MOVEMENT, OI.CLIENT_PREDICTED_MOVEMENT, "
			+ " OI.NO_OF_TIME_MOVED_X_WEEKS, OI.CONSECUTIVE_TIME_SLOT_NO_MOV, OI.OOS_CRITERIA_ID, "
			+ " OI.RET_LIR_ID, OI.WEEKLY_PREDICTED_MOVEMENT, OI.TRX_BASED_EXP, OI.TRX_CNT_STORE,"
			+ " OI.TRX_CNT_STORE_PREV_DAYS, OI.TRX_CNT_ITEM_PREV_DAYS, OI.ACTUAL_MOV_ITEM_PREV_DAYS, OI.ACTUAL_MOV_PREV_SLOT, "
			+ " OI.SHELF_CAPACITY "
			+ " FROM OOS_ITEM OI "
			+ " LEFT JOIN ITEM_LOOKUP IL ON (IL.ITEM_CODE = OI.PRODUCT_ID"
			+ "	AND OI.PRODUCT_LEVEL_ID = " + Constants.ITEMLEVELID + " ) "
			+ " LEFT JOIN  UOM_LOOKUP UL ON UL.ID = IL.UOM_ID "
			+ " LEFT JOIN RETAILER_LIKE_ITEM_GROUP LIR ON ( LIR.RET_LIR_ID = OI.RET_LIR_ID "
			+ " AND OI.PRODUCT_LEVEL_ID = " + Constants.PRODUCT_LEVEL_ID_LIG + " ) "
			+ " JOIN COMPETITOR_STORE CS ON CS.COMP_STR_ID = OI.LOCATION_ID "
			+ " JOIN RETAIL_DISTRICT RD ON RD.ID = CS.DISTRICT_ID " ;
	
	private static final String GET_TIME_SLOT_OOS_ITEMS = 
			" SELECT OI.LOCATION_LEVEL_ID, OI.LOCATION_ID, OI.PRODUCT_LEVEL_ID, OI.PRODUCT_ID, OI.CALENDAR_ID, OI.DAY_PART_ID, "
			+ " OI.REG_QUANTITY, OI.REG_PRICE, OI.SALE_QUANTITY, OI.SALE_PRICE, OI.AD_PAGE_NO, OI.DISPLAY_TYPE_ID, "
			+ " OI.ACTUAL_MOVEMENT, OI.PREDICTED_MOVEMENT, "
			+ " OI.CONFIDENCE_LEVEL_LOWER, OI.TRX_BASED_EXP FROM OOS_ITEM OI "
			+ " LEFT JOIN OOS_DAY_PART_LOOKUP DL ON OI.DAY_PART_ID = DL.DAY_PART_ID "
			+ " WHERE OI.LOCATION_ID IN (%LOCATIONS%) AND OI.CALENDAR_ID = ? AND OI.DAY_PART_ID IN (%DAY_PARTS%)";
			//+ " DL.EXEC_ORDER < ? ";
			
	private static final String GET_MAIL_LIST = "SELECT LOCATION_LEVEL_ID, "
			+ "LOCATION_ID, MAIL_ID_TO, MAIL_ID_CC, MAIL_ID_BCC "
			+ "FROM OOS_EMAIL_ALERT WHERE LOCATION_LEVEL_ID = ? AND LOCATION_ID = ? AND IS_INTERNAL = ? ";

	private static final String UPDATE_ALERT_SEND_STATUS = "UPDATE OOS_ITEM SET ALERT_SEND_STATUS = 'Y' WHERE DAY_PART_ID = ? AND CALENDAR_ID = ? AND "
			+ " LOCATION_ID = ? AND LOCATION_LEVEL_ID = ? AND PRODUCT_LEVEL_ID = ? AND PRODUCT_ID = ?";

	private static final String GET_STORES_IN_DISTRICT_OOS_ITEM  = 
			"SELECT DISTINCT(LOCATION_ID) FROM OOS_ITEM WHERE CALENDAR_ID = ? AND "
			+ "LOCATION_ID IN(SELECT COMP_STR_ID FROM COMPETITOR_STORE WHERE DISTRICT_ID = ?)";
	
	/**
	 * Gives list of items grouped by each location as per current input
	 * @param conn
	 * @param oosAlertInputDTO
	 * @return HashMap<LocationKey, List<OOSItemDTO>>
	 * @throws GeneralException
	 */
	public HashMap<LocationKey, List<OOSItemDTO>> getItemsInOOSAlert(Connection conn, OOSAlertInputDTO oosAlertInputDTO)
			throws GeneralException {
		HashMap<LocationKey, List<OOSItemDTO>> oosItemMap = new HashMap<LocationKey, List<OOSItemDTO>>();
		PreparedStatement statement = null;
		ResultSet resultSet = null;
		com.pristine.dao.offermgmt.ItemDAO itemDAO = new com.pristine.dao.offermgmt.ItemDAO();
		StringBuilder sb = new StringBuilder();
		ProductService productService = new ProductService();
		try {
			HashMap<Integer, Integer> productLevelPointer = productService.getProductLevelPointer(conn,
					Constants.DEPARTMENTLEVELID);

			sb.append(GET_OOS_ITEM);
			sb.append(" WHERE CALENDAR_ID = " + oosAlertInputDTO.getCalendarId());
			sb.append(" AND DAY_PART_ID = " + oosAlertInputDTO.getDayPartId());
			if (oosAlertInputDTO.getLocationList().size() > 0) {
				sb.append(" AND LOCATION_ID IN ("
						+ PRCommonUtil.getCommaSeperatedStringFromIntArray(oosAlertInputDTO.getLocationList()) + ") ");
			}
			sb.append(" AND LOCATION_LEVEL_ID = " + oosAlertInputDTO.getLocationLevelId());

			sb.append(") STORE_ITEM_MAP LEFT JOIN ");
			sb.append(itemDAO.getProductHierarchy(conn, Constants.DEPARTMENTLEVELID, productService));
			//sb.append(" AND PRODUCT_LEVEL_ID = " + Constants.ITEMLEVELID);
			sb.append(" LEFT JOIN PRODUCT_GROUP PG ON ");
			sb.append(" PRODUCT_HIERARCHY.P_ID_" + productLevelPointer.get(Constants.CATEGORYLEVELID)
					+ " = PG.PRODUCT_ID AND ");
			sb.append(" PG.PRODUCT_LEVEL_ID = " + Constants.CATEGORYLEVELID);
			sb.append(" LEFT JOIN PRODUCT_GROUP PG1 ON ");
			sb.append(" PRODUCT_HIERARCHY.P_ID_" + productLevelPointer.get(Constants.DEPARTMENTLEVELID)
					+ " = PG1.PRODUCT_ID AND ");
			sb.append(" PG1.PRODUCT_LEVEL_ID = " + Constants.DEPARTMENTLEVELID);

			logger.debug(sb.toString());
			statement = conn.prepareStatement(sb.toString());
			resultSet = statement.executeQuery();
			int recordCount = 0;
			while (resultSet.next()) {
				recordCount = recordCount + 1;
				fillData(resultSet, oosItemMap);
			}
			if (recordCount == 0) {
				LocationKey locationKey = new LocationKey(oosAlertInputDTO.getLocationLevelId(),
						oosAlertInputDTO.getLocationList().get(0));
				List<OOSItemDTO> oosItemList = new ArrayList<OOSItemDTO>();
				oosItemMap.put(locationKey, oosItemList);
			}
		} catch (SQLException sqlE) {
			logger.error("Error while getting OOS items - ", sqlE);
			throw new GeneralException("Error while getting OOS items", sqlE);
		} finally {
			PristineDBUtil.close(resultSet);
			PristineDBUtil.close(statement);
		}
		return oosItemMap;
	}
	
	public HashMap<OOSAlertItemKey, OOSItemDTO> getOOSItems(Connection conn, int locationId, int calendarId, List<Integer> dayParts)
			throws GeneralException {
		PreparedStatement statement = null;
		ResultSet resultSet = null;
		HashMap<OOSAlertItemKey, OOSItemDTO> prevOOSItems = new HashMap<OOSAlertItemKey, OOSItemDTO>();
		OOSItemDTO oosItem = null;
		try {
			String query = new String(GET_TIME_SLOT_OOS_ITEMS);
			query = query.replaceAll("%LOCATIONS%", String.valueOf(locationId));
			query = query.replaceAll("%DAY_PARTS%", PRCommonUtil.getCommaSeperatedStringFromIntArray(dayParts));
			statement = conn.prepareStatement(query);
			int counter = 0;
			statement.setInt(++counter, calendarId);
			resultSet = statement.executeQuery();
			while (resultSet.next()) {
				int locationLevelId = resultSet.getInt("LOCATION_LEVEL_ID");
//				int itemCode = resultSet.getInt("ITEM_CODE");
				int dayPartId = resultSet.getInt("DAY_PART_ID");
				MultiplePrice regPrice = null;
				MultiplePrice salePrice = null;

				if (checkDBNull(resultSet.getObject("REG_PRICE"))) {
					regPrice = null;
				} else {
					regPrice = new MultiplePrice(resultSet.getInt("REG_QUANTITY"), resultSet.getDouble("REG_PRICE"));
				}

				if (checkDBNull(resultSet.getObject("SALE_PRICE"))) {
					salePrice = null;
				} else {
					salePrice = new MultiplePrice(resultSet.getInt("SALE_QUANTITY"), resultSet.getDouble("SALE_PRICE"));
				}

				int adPageNo = resultSet.getInt("AD_PAGE_NO");
				int displayTypeId = resultSet.getInt("DISPLAY_TYPE_ID");
				long actualMov = resultSet.getLong("ACTUAL_MOVEMENT");
				long confLevelLower = resultSet.getLong("CONFIDENCE_LEVEL_LOWER");
				long forecastMovOfProcessingTimeSlot = resultSet.getLong("PREDICTED_MOVEMENT");
				int productLevelId = resultSet.getInt("PRODUCT_LEVEL_ID");
				int productId = resultSet.getInt("PRODUCT_ID");
				long trxBasedExp = resultSet.getLong("TRX_BASED_EXP");
				
				OOSAlertItemKey oosAlertItemKey = new OOSAlertItemKey(locationLevelId, locationId, productLevelId, productId, calendarId, dayPartId,
						regPrice, salePrice, adPageNo, displayTypeId);
				oosItem = new OOSItemDTO();
				oosItem.setProductLevelId(productLevelId);
				oosItem.setProductId(productId);
				oosItem.getOOSCriteriaData().setActualMovOfProcessingTimeSlotOfItemOrLig(actualMov);
				oosItem.getOOSCriteriaData().setLowerConfidenceOfProcessingTimeSlotOfItemOrLig(confLevelLower);
				oosItem.getOOSCriteriaData().setForecastMovOfProcessingTimeSlotOfItemOrLig(forecastMovOfProcessingTimeSlot);
				oosItem.getOOSCriteriaData().setTrxBasedPredOfProcessingTimeSlotOfItemOrLig(trxBasedExp);
				prevOOSItems.put(oosAlertItemKey, oosItem);
			}
		} catch (SQLException sqlE) {
			logger.error("Error in getPreviuosTimeSlotOOSItems()", sqlE);
			throw new GeneralException("Error in getPreviuosTimeSlotOOSItems()", sqlE);
		} finally {
			PristineDBUtil.close(resultSet);
			PristineDBUtil.close(statement);
		}
		return prevOOSItems;
	}
	
	private void fillData(ResultSet resultSet, HashMap<LocationKey, List<OOSItemDTO>> oosItemMap) throws SQLException {
		OOSItemDTO oosItemDTO = new OOSItemDTO();
		oosItemDTO.setItemName(resultSet.getString("ITEM_NAME"));
		oosItemDTO.setItemSize(resultSet.getString("ITEM_SIZE"));
		oosItemDTO.setLirName(resultSet.getString("RET_LIR_NAME"));
		oosItemDTO.setRetailerItemCode(resultSet.getString("RETAILER_ITEM_CODE"));
		oosItemDTO.setStoreNo(resultSet.getString("COMP_STR_NO"));
		oosItemDTO.setUom(resultSet.getString("UOM"));
		oosItemDTO.setUpc(resultSet.getString("UPC"));
		oosItemDTO.setCategoryName(resultSet.getString("CATEGORY_NAME"));
		oosItemDTO.setDepartmentName(resultSet.getString("DEPARTMENT_NAME"));

		if (checkDBNull(resultSet.getObject("REG_PRICE"))) {
			oosItemDTO.setRegPrice(null);
		} else {
			MultiplePrice multiplePrice = new MultiplePrice(resultSet.getInt("REG_QUANTITY"),
					resultSet.getDouble("REG_PRICE"));
			oosItemDTO.setRegPrice(multiplePrice);
		}
		if (checkDBNull(resultSet.getObject("SALE_PRICE"))) {
			oosItemDTO.setSalePrice(null);
		} else {
			MultiplePrice multiplePrice = new MultiplePrice(resultSet.getInt("SALE_QUANTITY"),
					resultSet.getDouble("SALE_PRICE"));
			oosItemDTO.setSalePrice(multiplePrice);
		}
		oosItemDTO.setAdPageNo(resultSet.getInt("AD_PAGE_NO"));
		oosItemDTO.setDisplayTypeId(resultSet.getInt("DISPLAY_TYPE_ID"));
		oosItemDTO.setNoOfZeroMovInLastXWeeks(resultSet.getInt("ZERO_MOV_X_WEEKS"));
		oosItemDTO.setMinMovementInLastXWeeks(resultSet.getInt("MIN_MOV_X_WEEKS"));
		oosItemDTO.setMaxMovementInLastXWeeks(resultSet.getInt("MAX_MOV_X_WEEKS"));
		oosItemDTO.setAvgMovementInLastXWeeks(resultSet.getInt("AVG_MOV_X_WEEKS"));
		oosItemDTO.getOOSCriteriaData().setLowerConfidenceOfProcessingTimeSlotOfItemOrLig(resultSet.getLong("CONFIDENCE_LEVEL_LOWER"));
		oosItemDTO.getOOSCriteriaData().setUpperConfidenceOfProcessingTimeSlotOfItemOrLig(resultSet.getLong("CONFIDENCE_LEVEL_UPPER"));
		oosItemDTO.getOOSCriteriaData().setForecastMovOfProcessingTimeSlotOfItemOrLig(resultSet.getLong("PREDICTED_MOVEMENT"));
		oosItemDTO.setCalendarId(resultSet.getInt("CALENDAR_ID"));
		oosItemDTO.setProductLevelId(resultSet.getInt("PRODUCT_LEVEL_ID"));
		oosItemDTO.setProductId(resultSet.getInt("PRODUCT_ID"));
		oosItemDTO.setDayPartId(resultSet.getInt("DAY_PART_ID"));
		oosItemDTO.setDistrictName(resultSet.getString("DISTRICT_NAME"));
		oosItemDTO.setClientDayPartPredictedMovement(resultSet.getLong("CLIENT_PREDICTED_MOVEMENT"));
		oosItemDTO.setNoOfTimeMovedInLastXWeeksOfItemOrLig(resultSet.getInt("NO_OF_TIME_MOVED_X_WEEKS"));
		if (!checkDBNull(resultSet.getObject("CONSECUTIVE_TIME_SLOT_NO_MOV")))
			oosItemDTO.getOOSCriteriaData().setNoOfConsecutiveTimeSlotItemDidNotMove(resultSet.getInt("CONSECUTIVE_TIME_SLOT_NO_MOV"));
		oosItemDTO.setOOSCriteriaId(resultSet.getInt("OOS_CRITERIA_ID"));

		if (checkDBNull(resultSet.getObject("ACTUAL_MOVEMENT"))) {
			oosItemDTO.getOOSCriteriaData().setActualMovOfProcessingTimeSlotOfItemOrLig(0l);
		} else {
			oosItemDTO.getOOSCriteriaData().setActualMovOfProcessingTimeSlotOfItemOrLig(resultSet.getLong("ACTUAL_MOVEMENT"));
		}
		if (String.valueOf(Constants.YES).equalsIgnoreCase(resultSet.getString("SEND_TO_CLIENT"))) {
			oosItemDTO.setIsSendToClient(true);
		}
		oosItemDTO.setRetLirId(resultSet.getInt("RET_LIR_ID"));
		oosItemDTO.setWeeklyPredictedMovement(resultSet.getLong("WEEKLY_PREDICTED_MOVEMENT"));

		if(!checkDBNull(resultSet.getObject("TRX_BASED_EXP")))
			oosItemDTO.getOOSCriteriaData().setTrxBasedPredOfProcessingTimeSlotOfItemOrLig(resultSet.getLong("TRX_BASED_EXP"));
		if(!checkDBNull(resultSet.getObject("TRX_CNT_STORE")))
			oosItemDTO.getOOSCriteriaData().setTrxCntOfProcessingTimeSlotOfStore(resultSet.getInt("TRX_CNT_STORE"));
		if(!checkDBNull(resultSet.getObject("TRX_CNT_STORE_PREV_DAYS")))
			oosItemDTO.getOOSCriteriaData().setTrxCntOfPrevDaysOfStore(resultSet.getInt("TRX_CNT_STORE_PREV_DAYS"));
		if(!checkDBNull(resultSet.getObject("TRX_CNT_ITEM_PREV_DAYS")))
			oosItemDTO.getOOSCriteriaData().setTrxCntOfPrevDaysOfItemOrLig(resultSet.getInt("TRX_CNT_ITEM_PREV_DAYS"));
		if(!checkDBNull(resultSet.getObject("ACTUAL_MOV_ITEM_PREV_DAYS")))
			oosItemDTO.getOOSCriteriaData().setActualMovOfPrevDaysOfItemOrLig(resultSet.getInt("ACTUAL_MOV_ITEM_PREV_DAYS"));
		
		if (checkDBNull(resultSet.getObject("ACTUAL_MOV_PREV_SLOT"))) {
			oosItemDTO.getOOSCriteriaData().setActualMovOfPrevTimeSlotOfItemOrLig(0l);
		} else {
			oosItemDTO.getOOSCriteriaData().setActualMovOfPrevTimeSlotOfItemOrLig(resultSet.getLong("ACTUAL_MOV_PREV_SLOT"));
		}
		
		oosItemDTO.getOOSCriteriaData().setActualMovOfProcessingDayItemOrLig(
				oosItemDTO.getOOSCriteriaData().getActualMovOfProcessingTimeSlotOfItemOrLig()
						+ oosItemDTO.getOOSCriteriaData().getActualMovOfPrevTimeSlotOfItemOrLig());

		if (checkDBNull(resultSet.getObject("SHELF_CAPACITY"))) {
			oosItemDTO.getOOSCriteriaData().setShelfCapacityOfItemOrLig(-1);
		} else {
			oosItemDTO.getOOSCriteriaData().setShelfCapacityOfItemOrLig(resultSet.getInt("SHELF_CAPACITY"));
		}
		
		int locationId = resultSet.getInt("LOCATION_ID");
		int locationLevelId = resultSet.getInt("LOCATION_LEVEL_ID");
		oosItemDTO.setLocationId(locationId);
		oosItemDTO.setLocationLevelId(locationLevelId);
		LocationKey locationKey = new LocationKey(locationLevelId, locationId);
		if (oosItemMap.get(locationKey) == null) {
			List<OOSItemDTO> oosItemList = new ArrayList<OOSItemDTO>();
			oosItemList.add(oosItemDTO);
			oosItemMap.put(locationKey, oosItemList);
		} else {
			List<OOSItemDTO> oosItemList = oosItemMap.get(locationKey);
			oosItemList.add(oosItemDTO);
			oosItemMap.put(locationKey, oosItemList);
		}
	}

	/**
	 * Get distinct exported items from OOS_ITEM for the day 
	 * @param conn
	 * @param oosAlertInputDTO
	 * @return HashMap<LocationKey, List<OOSItemDTO>>
	 * @throws GeneralException
	 */
	
	public Set<ProductKey> getDistinctExportedItems(Connection conn, OOSAlertInputDTO oosAlertInputDTO) throws GeneralException {
		Set<ProductKey> oosItemSet = new HashSet<ProductKey>();
		PreparedStatement statement = null;
		ResultSet resultSet = null;
		StringBuilder sb = new StringBuilder();
		try {

			sb.append("SELECT PRODUCT_LEVEL_ID, PRODUCT_ID FROM OOS_ITEM WHERE");
			sb.append(" CALENDAR_ID = " + oosAlertInputDTO.getCalendarId());
			if (oosAlertInputDTO.getLocationList().size() > 0) {
				sb.append(" AND LOCATION_ID IN (" + PRCommonUtil.getCommaSeperatedStringFromIntArray(oosAlertInputDTO.getLocationList()) + ") ");
			}
			sb.append(" AND LOCATION_LEVEL_ID = " + oosAlertInputDTO.getLocationLevelId());
			sb.append(" AND ALERT_SEND_STATUS = 'Y' ");
			logger.debug(sb.toString());
			statement = conn.prepareStatement(sb.toString());
			resultSet = statement.executeQuery();
			while (resultSet.next()) {
				int productLevelId = resultSet.getInt("PRODUCT_LEVEL_ID");
				int productId = resultSet.getInt("PRODUCT_ID");
				ProductKey productKey = new ProductKey(productLevelId, productId);
				oosItemSet.add(productKey);
			}
		} catch (SQLException sqlE) {
			logger.error("Error -- getDistinctExportedItems() " + sqlE);
			throw new GeneralException("Error -- getDistinctExportedItems() ", sqlE);
		} finally {
			PristineDBUtil.close(resultSet);
			PristineDBUtil.close(statement);
		}
		return oosItemSet;
	}

	
	/**
	 * Gives to, cc and bcc list for current location
	 * @param conn
	 * @param locationId
	 * @param locationLevelId
	 * @return OOSEmailAlertDTO
	 * @throws GeneralException
	 */
	
	public OOSEmailAlertDTO getMailList(Connection conn, int locationId,
			int locationLevelId, boolean isInternalReport) throws GeneralException {
		OOSEmailAlertDTO oosEmailAlertDTO = null;
		PreparedStatement statement = null;
		ResultSet resultSet = null;
		StringBuilder sb = new StringBuilder();
		try {
			sb.append(GET_MAIL_LIST);
			statement = conn.prepareStatement(sb.toString());
			int counter = 0;
			if (isInternalReport) {
				statement.setInt(++counter, 0);
				statement.setInt(++counter, 0);		
				statement.setString(++counter, String.valueOf(Constants.YES));
			} else {
				statement.setInt(++counter, locationLevelId);
				statement.setInt(++counter, locationId);
				statement.setString(++counter, String.valueOf(Constants.NO));
			}
			
			resultSet = statement.executeQuery();
			if (resultSet.next()) {
				oosEmailAlertDTO = new OOSEmailAlertDTO();
				oosEmailAlertDTO.setMailBCC(resultSet.getString("MAIL_ID_BCC"));
				oosEmailAlertDTO.setMailCC(resultSet.getString("MAIL_ID_CC"));
				oosEmailAlertDTO.setMailTo(resultSet.getString("MAIL_ID_TO"));
				if(isInternalReport) {
					oosEmailAlertDTO.setLocationId(locationId);
					oosEmailAlertDTO.setLocationLevelId(locationLevelId);
				} else {
					oosEmailAlertDTO.setLocationId(resultSet.getInt("LOCATION_ID"));
					oosEmailAlertDTO.setLocationLevelId(resultSet.getInt("LOCATION_LEVEL_ID"));
				}
			}
		} catch (SQLException sqlE) {
			logger.error("Error while getting oos email list", sqlE);
			throw new GeneralException("Error while getting oos email list",
					sqlE);
		} finally {
			PristineDBUtil.close(resultSet);
			PristineDBUtil.close(statement);
		}
		return oosEmailAlertDTO;
	}

	/**
	 * Updates alert send status to Y for given items
	 * @param conn
	 * @param updateList
	 * @return
	 * @throws GeneralException
	 */
	
	public int updateAlertSendingStatus(Connection conn,
			List<OOSItemDTO> updateList) throws GeneralException {
		PreparedStatement statement = null;
		int count = 0;
		try {
			/*
			 * WHERE DAY_PART_ID = ? AND CALENDAR_ID = ? " +
			 * " LOCATION_ID = ? AND LOCATION_LEVEL_ID = ?";
			 */
			statement = conn.prepareStatement(UPDATE_ALERT_SEND_STATUS);
			for (OOSItemDTO oosItem : updateList) {
				int colCount = 0;
				//Update item that is marked as oos
				if (oosItem.getIsSendToClient()) {
					statement.setInt(++colCount, oosItem.getDayPartId());
					statement.setInt(++colCount, oosItem.getCalendarId());
					statement.setInt(++colCount, oosItem.getLocationId());
					statement.setInt(++colCount, oosItem.getLocationLevelId());
					statement.setInt(++colCount, oosItem.getProductLevelId());
					statement.setInt(++colCount, oosItem.getProductId());
					statement.addBatch();
					count++;
				}
				if (count % Constants.BATCH_UPDATE_COUNT == 0) {
					statement.executeBatch();
					statement.clearBatch();
					count = 0;
				}
			}
			if (count > 0) {
				statement.executeBatch();
				statement.clearBatch();
			}
		} catch (SQLException sqlE) {
			logger.error("Error while updating alert sending status", sqlE);
			throw new GeneralException(
					"Error while updating alert sending status", sqlE);
		} finally {
			PristineDBUtil.close(statement);
		}
		return count;
	}

	/**
	 * Checks null
	 * @param object
	 * @return is null
	 */
	
	private boolean checkDBNull(Object object) {
		boolean isNull = false;
		if (object == null)
			isNull = true;
		return isNull;
	}

	/***
	 * Get the distinct stores of the district from OOS_CANDIDATE_ITEM table
	 * @param conn
	 * @param calendarId
	 * @param districtId
	 * @return
	 * @throws GeneralException
	 */
	public List<Integer> getStoresInDistrictFromOOSItem(Connection conn, int calendarId, int districtId) throws GeneralException {
		List<Integer> stores = new ArrayList<Integer>();
		PreparedStatement statement = null;
		ResultSet resultSet = null;
		try {
			statement = conn.prepareStatement(GET_STORES_IN_DISTRICT_OOS_ITEM);
			int colIndex = 0;
			statement.setInt(++colIndex, calendarId);
			statement.setInt(++colIndex, districtId);
			resultSet = statement.executeQuery();
			while (resultSet.next()) {
				stores.add(resultSet.getInt("LOCATION_ID"));
			}
		} catch (SQLException sqlE) {
			logger.error("Error in getStoresInDistrictFromOOSItem", sqlE);
			throw new GeneralException("", sqlE);
		} finally {
			PristineDBUtil.close(resultSet);
			PristineDBUtil.close(statement);
		}
		return stores;
	}
}
