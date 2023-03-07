package com.pristine.dao.offermgmt.oos;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Timestamp;
import java.sql.Types;
//import java.text.DateFormat;
//import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;

import org.apache.log4j.Logger;

import com.pristine.dao.RetailCalendarDAO;
import com.pristine.dto.RetailCalendarDTO;
import com.pristine.dto.offermgmt.DayPartLookupDTO;
import com.pristine.dto.offermgmt.MultiplePrice;
import com.pristine.dto.offermgmt.PRItemDTO;
import com.pristine.dto.offermgmt.ProductKey;
import com.pristine.dto.offermgmt.oos.OOSExpectationDTO;
import com.pristine.dto.offermgmt.oos.OOSItemDTO;
import com.pristine.dto.offermgmt.oos.WeeklyAvgMovement;
import com.pristine.exception.GeneralException;
import com.pristine.service.ProductService;
import com.pristine.service.offermgmt.oos.OOSDayPartDetail;
import com.pristine.service.offermgmt.oos.OOSDayPartDetailKey;
import com.pristine.service.offermgmt.oos.OOSDayPartItemAvgMovKey;
import com.pristine.service.offermgmt.oos.OOSForecastItemKey;
import com.pristine.service.offermgmt.oos.OOSService;
import com.pristine.util.Constants;
import com.pristine.util.PristineDBUtil;
import com.pristine.util.PropertyManager;
import com.pristine.util.offermgmt.PRCommonUtil;

public class OOSAnalysisDAO {

	private static Logger logger = Logger.getLogger("OOSAnalysisDAO");

	private static final String INSERT_OOS_ITEM = "INSERT INTO OOS_ITEM (LOCATION_LEVEL_ID, LOCATION_ID, CALENDAR_ID, DAY_PART_ID, "
			+ " PREDICTED_MOVEMENT, ACTUAL_MOVEMENT, REG_QUANTITY, REG_PRICE, " + " SALE_QUANTITY, SALE_PRICE, AD_PAGE_NO, DISPLAY_TYPE_ID, "
			+ " ZERO_MOV_X_WEEKS, MIN_MOV_X_WEEKS, MAX_MOV_X_WEEKS, AVG_MOV_X_WEEKS, "
			+ " CONFIDENCE_LEVEL_LOWER, CONFIDENCE_LEVEL_UPPER, NO_OF_TIME_MOVED_X_WEEKS, IS_OOS, SEND_TO_CLIENT,"
			+ " CLIENT_PREDICTED_MOVEMENT, CONSECUTIVE_TIME_SLOT_NO_MOV, OOS_CRITERIA_ID, PRODUCT_LEVEL_ID, PRODUCT_ID, RET_LIR_ID, "
			+ " WEEKLY_PREDICTED_MOVEMENT, TRX_CNT_ITEM_PREV_DAYS, TRX_BASED_EXP, ACTUAL_MOV_PREV_SLOT, FORECAST_MOV_PREV_SLOT, "
			+ " ACTUAL_MOV_PREV_DAY_SLOT, "
			+ " TOTAL_X_SLOT_LOWER_CONF, TOTAL_X_SLOT_FORECAST, IS_ITEM_MOV_X_UNIT_X_SLOT, TRX_CNT_STORE, TRX_CNT_STORE_PREV_DAYS, "
			+ " ACTUAL_MOV_ITEM_PREV_DAYS, SHELF_CAPACITY " + ") "
			+ " VALUES(?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)";

	private static final String GET_WEEKLY_LEVEL_AVG_MOV = "SELECT PRODUCT_LEVEL_ID, PRODUCT_ID, "
			+ " WEEKLY_AVG_MOV FROM OOS_WEEKLY_AVERAGE_MOVEMENT WHERE "
			+ " LOCATION_LEVEL_ID = ? AND LOCATION_ID = ?";

	private static final String GET_DAY_PART_AVG_MOV = "SELECT PRODUCT_LEVEL_ID, PRODUCT_ID, "
			+ " DAY_PART_ID, DAY_ID, DAY_PART_AVG_MOV FROM OOS_DAY_PART_AVERAGE_MOVEMENT WHERE "
			+ " LOCATION_LEVEL_ID = ? AND LOCATION_ID = ? AND DAY_ID IN (%DAY_IDS%) ";
			//+ " AND DAY_PART_ID = ?";

	private static final String GET_DAY_PART_LOOKUP = "SELECT DAY_PART_ID, START_TIME, END_TIME, EXEC_ORDER, IS_SLOT_SPAN_DAYS FROM OOS_DAY_PART_LOOKUP";

//	private static final String GET_DAY_PART_DETAIL = " SELECT ITEM_ID, SUM(MOV) AS QUANTITY "
//			//+ " ,COUNT(DISTINCT CALENDAR_ID || TRX_NO) AS TRX_COUNT "
//			+ " FROM "
//			+ " (SELECT TL.ITEM_ID, TL.TRX_TIME, "
//			//+ " TL.CALENDAR_ID, TL.TRX_NO, "
//			+ " CASE WHEN TL.QUANTITY IS NOT NULL AND TL.QUANTITY > 0 THEN TL.QUANTITY "
//			+ " WHEN TL.WEIGHT IS NOT NULL AND TL.WEIGHT > 0 THEN 1 "
//			+ " ELSE 0 END AS MOV,"
//			+ " CASE WHEN (TL.TRX_TIME >= ? "
//			+ " AND TL.TRX_TIME < ?) THEN 1 "
//			+ " ELSE 0 END AS FILTER_ID "
//			+ " FROM TRANSACTION_LOG TL "
//			//TODO:: For debugging, comment below line and uncomment above line
//			//+ " FROM TRANSACTION_LOG_T1 TL "
//			+ " LEFT JOIN RETAIL_CALENDAR RC ON RC.CALENDAR_ID = TL.CALENDAR_ID "
//			+ " WHERE TL.STORE_ID = ? AND (RC.START_DATE = TO_CHAR(?, 'dd-MON-yy') OR "
//			+ " RC.START_DATE = TO_CHAR(?, 'dd-MON-yy'))) WHERE FILTER_ID = 1 GROUP BY ITEM_ID";
	
	private static final String GET_DAY_PART_DETAIL = " SELECT CALENDAR_ID, DAY_PART_ID, ITEM_ID, SUM(MOV) AS QUANTITY "
			+ " FROM "
			+ " (SELECT ITEM_ID,  TRX_TIME, NEW_TRX_DATE, RC1.CALENDAR_ID, MOV, DAY_PART_ID FROM"
			+ " (SELECT TL.ITEM_ID, TL.TRX_TIME, "
			// Day Case used to group by day
			+ " %DAY_CASE% ," 
			+ " CASE WHEN TL.QUANTITY IS NOT NULL AND TL.QUANTITY > 0 THEN TL.QUANTITY "
			+ " WHEN TL.WEIGHT IS NOT NULL AND TL.WEIGHT > 0 THEN 1 "
			+ " ELSE 0 END AS MOV,  TL.CALENDAR_ID,"
			+ " %DAY_PART_CASE% "
			+ " FROM TRANSACTION_LOG TL "
			//TODO:: For debugging, comment below line and uncomment above line
			//+ " FROM TRANSACTION_LOG_T1 TL "
			+ " LEFT JOIN RETAIL_CALENDAR RC ON RC.CALENDAR_ID = TL.CALENDAR_ID "
			+ " WHERE TL.STORE_ID = ? AND (RC.START_DATE = TO_CHAR(?, 'dd-MON-yy') OR "
			+ " RC.START_DATE = TO_CHAR(?, 'dd-MON-yy'))) "
			+ " LEFT JOIN RETAIL_CALENDAR RC1 ON NEW_TRX_DATE = RC1.START_DATE WHERE ROW_TYPE='D') "
			+ " WHERE NEW_TRX_DATE >= TO_CHAR(?, 'dd-MON-yy')  AND NEW_TRX_DATE   <= TO_CHAR(?, 'dd-MON-yy') "
			+ " GROUP BY CALENDAR_ID, DAY_PART_ID, ITEM_ID";
	
	private static final String GET_X_WEEKS_DATA = " SELECT ITEM_ID, WEEK_CALENDAR_ID, DAY_ID, DAY_PART_ID, SUM(MOV) AS MOV"
			+ " FROM (SELECT ITEM_ID, TRX_TIME, TRX_DATE, NEW_TRX_DATE, TO_CHAR(TO_DATE(NEW_TRX_DATE), 'D') AS DAY_ID,"
			+ " DAY_PART_ID, RC1.CALENDAR_ID AS WEEK_CALENDAR_ID, RC1.START_DATE  AS WEEK_START_DATE, "
			+ " RC1.END_DATE AS WEEK_END_DATE, MOV FROM(SELECT ITEM_ID, TRX_TIME, TO_CHAR(TRX_TIME, 'dd-MON-yy') AS TRX_DATE, "
			+
			// Day Case used to group by day
			" %DAY_CASE% ,"
			+
			// Day Part case used to group by time slot
			" %DAY_PART_CASE% ,"
			+
			/*
			 * CASE WHEN (TO_CHAR(TRX_TIME, 'HH24:MI:SS' ) >= '00:00:00' AND
			 * TO_CHAR(TRX_TIME, 'HH24:MI:SS' ) < '07:00:00' ) THEN
			 * TO_CHAR(TRX_TIME - 1, 'dd-MON-yy') ELSE TO_CHAR(TRX_TIME,
			 * 'dd-MON-yy')END AS NEW_TRX_DATE, CASE WHEN (TO_CHAR(TRX_TIME,
			 * 'HH24:MI:SS' ) >= '07:00:00' AND TO_CHAR(TRX_TIME, 'HH24:MI:SS' )
			 * < '11:00:00') THEN 1 WHEN (TO_CHAR(TRX_TIME, 'HH24:MI:SS' ) >=
			 * '11:00:00' AND TO_CHAR(TRX_TIME, 'HH24:MI:SS' ) < '15:00:00')
			 * THEN 2 WHEN (TO_CHAR(TRX_TIME, 'HH24:MI:SS' ) >= '15:00:00' AND
			 * TO_CHAR(TRX_TIME, 'HH24:MI:SS' ) < '18:00:00') THEN 3 WHEN
			 * (TO_CHAR(TRX_TIME, 'HH24:MI:SS' ) >= '18:00:00' AND
			 * TO_CHAR(TRX_TIME, 'HH24:MI:SS' ) < '21:00:00') THEN 4 WHEN
			 * (TO_CHAR(TRX_TIME, 'HH24:MI:SS' ) >= '21:00:00' AND
			 * TO_CHAR(TRX_TIME, 'HH24:MI:SS' ) <= '23:59:59') THEN 5 WHEN
			 * (TO_CHAR(TRX_TIME, 'HH24:MI:SS' ) >= '00:00:00' AND
			 * TO_CHAR(TRX_TIME, 'HH24:MI:SS' ) < '21:00:00') THEN 5 END AS
			 * DAY_PART_ID ,
			 */
			" CASE  WHEN QUANTITY IS NOT NULL AND QUANTITY > 0 THEN QUANTITY WHEN WEIGHT IS NOT NULL "
			+ " AND WEIGHT > 0  THEN 1 ELSE 0 END AS MOV "
			//+ " FROM TRANSACTION_LOG TL "
			+ " FROM TRANSACTION_LOG_T1 TL "
			// Join calendar to restrict the data range
			+ " LEFT JOIN RETAIL_CALENDAR RC ON TL.CALENDAR_ID = RC.CALENDAR_ID "
			// Go X weeks back
			+ " WHERE TL.STORE_ID   = ? AND (RC.START_DATE >= TRUNC(SYSDATE + (1 - TO_CHAR(SYSDATE, 'D')) - ? * 7) "
			// Ignore current week and end with last week last date
			+ " AND RC.START_DATE  <= TRUNC((SYSDATE + (1 - TO_CHAR(SYSDATE, 'D')) - 1) + 1)) "
			+ "AND ITEM_ID IN (%s)) "
			// Join calendar to group by week
			+ " LEFT JOIN RETAIL_CALENDAR RC1 ON (NEW_TRX_DATE >= RC1.START_DATE AND NEW_TRX_DATE <= RC1.END_DATE) "
			+ " WHERE RC1.ROW_TYPE = 'W') "
			// Condition added to ignore transaction happened on first week time
			// slot which spans day
			+ " WHERE  NEW_TRX_DATE >= TRUNC(SYSDATE + (1 - TO_CHAR(SYSDATE, 'D')) - ? * 7) "
			// Condition added to add transaction happened on last week next day
			// time slot which spans day
			+ " AND NEW_TRX_DATE <= TRUNC(SYSDATE + (1 - TO_CHAR(SYSDATE, 'D')) - 1) "
			+ " AND DAY_ID = ? AND DAY_PART_ID = ? "
			+ " GROUP BY ITEM_ID, WEEK_CALENDAR_ID, DAY_ID, DAY_PART_ID ";

	private static final String DELETE_OOS_ITEM = "DELETE FROM OOS_ITEM WHERE LOCATION_LEVEL_ID = ? "
			+ " AND LOCATION_ID = ? AND CALENDAR_ID = ? AND DAY_PART_ID = ?";

	private static final String DELETE_OOS_CANDIDATE_ITEM = "DELETE FROM OOS_CANDIDATE_ITEM "
			+ "WHERE LOCATION_LEVEL_ID = ? AND LOCATION_ID = ? AND CALENDAR_ID = ?";

	private static final String INSERT_OOS_CANDIDATE_ITEM = "INSERT INTO OOS_CANDIDATE_ITEM (CALENDAR_ID, LOCATION_LEVEL_ID, LOCATION_ID, "
			+ " ITEM_CODE, PRESTO_FORECAST, PRESTO_CONFIDENCE_LEVEL_LOWER, PRESTO_CONFIDENCE_LEVEL_UPPER, "
			+ " REG_PRICE, REG_QUANTITY, SALE_PRICE, SALE_QUANTITY, AD_PAGE_NO, AD_BLOCK_NO, DISPLAY_TYPE_ID, "
			+ " PROMO_TYPE_ID, PREDICTION_STATUS, IS_OOS_ANALYSIS, DIST_FLAG, CLIENT_FORECAST_ACTUAL, RET_LIR_ID) "
			+ " VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)";
	
	private static final String GET_WEEKLY_AVG_MOVEMENT_AT_LOCATION = "SELECT PRODUCT_ID, PRODUCT_LEVEL_ID, ROUND(SUM(TOT_MOVEMENT) / ?) AVG_MOV FROM "
			+ " (SELECT PRODUCT_ID, "
			+ " PRODUCT_LEVEL_ID, TOT_MOVEMENT FROM ITEM_METRIC_SUMMARY_WEEKLY TL LEFT JOIN RETAIL_CALENDAR RC ON TL.CALENDAR_ID = RC.CALENDAR_ID  "
			+ " WHERE "
			+ " (RC.START_DATE >= TRUNC(TO_DATE(?, '"
			+ Constants.DB_DATE_FORMAT
			+ "') + (1 - TO_CHAR(TO_DATE(?, '"
			+ Constants.DB_DATE_FORMAT
			+ "'), 'D')) - ? * 7) "
			+ " AND  RC.START_DATE <= TRUNC(TO_DATE(?, '"
			+ Constants.DB_DATE_FORMAT
			+ "') + (1 - TO_CHAR(TO_DATE(?, '"
			+ Constants.DB_DATE_FORMAT
			+ "'), 'D')) - 1)) AND  "
			+ " LOCATION_ID = ? AND LOCATION_LEVEL_ID = ?) "
			+ " GROUP BY PRODUCT_ID, PRODUCT_LEVEL_ID ";
	
	private static final String GET_WEEKLY_AVG_MOVEMENT_AT_CHAIN = "SELECT PRODUCT_ID, PRODUCT_LEVEL_ID, ROUND(SUM(TOT_MOVEMENT) / ?) AVG_MOV FROM "
			+ "(SELECT PRODUCT_ID, "
			+ " PRODUCT_LEVEL_ID, TOT_MOVEMENT FROM ITEM_METRIC_SUMMARY_WEEKLY TL LEFT JOIN RETAIL_CALENDAR RC ON TL.CALENDAR_ID = RC.CALENDAR_ID  "
			+ " WHERE "
			+ " (RC.START_DATE >= TRUNC(TO_DATE(?, '"
			+ Constants.DB_DATE_FORMAT
			+ "') + (1 - TO_CHAR(TO_DATE(?, '"
			+ Constants.DB_DATE_FORMAT
			+ "'), 'D')) - ? * 7) "
			+ " AND  RC.START_DATE <= TRUNC(TO_DATE(?, '"
			+ Constants.DB_DATE_FORMAT
			+ "') + (1 - TO_CHAR(TO_DATE(?, '"
			+ Constants.DB_DATE_FORMAT
			+ "'), 'D')) - 1))"
			+ " AND LOCATION_ID IN (SELECT CHILD_LOCATION_ID FROM LOCATION_GROUP_RELATION WHERE LOCATION_LEVEL_ID = ? AND LOCATION_ID = ?)) "
			+ " GROUP BY PRODUCT_ID, PRODUCT_LEVEL_ID ";
	
//	private static final String GET_OOS_CANDIDATE_ITEM = " SELECT LOCATION_LEVEL_ID, LOCATION_ID, CALENDAR_ID, ITEM_CODE, PRESTO_FORECAST, " +
//			" CLIENT_FORECAST, REG_PRICE, REG_QUANTITY, SALE_QUANTITY, SALE_PRICE, AD_PAGE_NO, AD_BLOCK_NO, " +
//			" DISPLAY_TYPE_ID,PROMO_TYPE_ID, PRESTO_CONFIDENCE_LEVEL_LOWER, PRESTO_CONFIDENCE_LEVEL_UPPER " +
//			" FROM OOS_CANDIDATE_ITEM " +
//			" WHERE LOCATION_LEVEL_ID = ? AND LOCATION_ID = ? AND CALENDAR_ID = ? ";
	
	private static final String GET_OOS_CANDIDATE_ITEM = 
			" SELECT OOS_ITEM.*, IMS.TOT_MOVEMENT FROM (SELECT OOS_F.LOCATION_LEVEL_ID, OOS_F.LOCATION_ID, OOS_F.CALENDAR_ID, "
			+ " OOS_F.ITEM_CODE, OOS_F.PRESTO_FORECAST, "
			+ " OOS_F.CLIENT_FORECAST, OOS_F.REG_PRICE, OOS_F.REG_QUANTITY, OOS_F.SALE_QUANTITY, OOS_F.SALE_PRICE, "
			+ " OOS_F.AD_PAGE_NO, OOS_F.AD_BLOCK_NO, OOS_F.DISPLAY_TYPE_ID, OOS_F.PROMO_TYPE_ID, OOS_F.PRESTO_CONFIDENCE_LEVEL_LOWER, "
			+ " OOS_F.PRESTO_CONFIDENCE_LEVEL_UPPER FROM OOS_CANDIDATE_ITEM OOS_F " +
			" WHERE OOS_F.LOCATION_LEVEL_ID = ? AND OOS_F.LOCATION_ID = ? AND OOS_F.CALENDAR_ID = ? " +
			") OOS_ITEM LEFT JOIN ITEM_METRIC_SUMMARY_WEEKLY IMS ON OOS_ITEM.ITEM_CODE = IMS.PRODUCT_ID " +
			" AND IMS.LOCATION_ID = ? AND IMS.CALENDAR_ID = ? AND IMS.PRODUCT_LEVEL_ID=" + Constants.ITEMLEVELID
			;
	
	
	private static final String UPDATE_CLIENT_FORECAST = " UPDATE OOS_CANDIDATE_ITEM SET CLIENT_FORECAST = ?, " +
			" AVG_MOV_CHAIN_X_WEEKS = ?, AVG_MOV_STORE_X_WEEKS = ?, AVG_MOV_PERCENT = ?, CLIENT_FORECAST_ACTUAL = ? "
			+ "WHERE LOCATION_LEVEL_ID = ? AND LOCATION_ID = ? AND CALENDAR_ID = ? AND ITEM_CODE = ?";

	private static final String GET_STORES_IN_DISTRICT_OOS_CANDIDATE_ITEM = 
			"SELECT DISTINCT(LOCATION_ID) FROM OOS_CANDIDATE_ITEM WHERE CALENDAR_ID = ? AND "
			+ "LOCATION_ID IN(SELECT COMP_STR_ID FROM COMPETITOR_STORE WHERE DISTRICT_ID = ?)";
	
	private static final String GET_DISTRICT_NAME = 
			" SELECT NAME FROM RETAIL_DISTRICT WHERE ID =? ";
	
	private static final String GET_TRANSACTION_DETAIL = 
			" SELECT CALENDAR_ID, STORE_ID, PRODUCT_LEVEL_ID, PRODUCT_ID, STORE_LEVEL_TRX_CNT, ITEM_LEVEL_TRX_CNT, ITEM_LEVEL_UNITS "
			+ " FROM OOS_TRX_BASED_EXPECTATION WHERE CALENDAR_ID = ? AND STORE_ID = ? AND PRODUCT_LEVEL_ID = ? AND PRODUCT_ID IN (%s) ";	
	
	
	private static final String GET_OOS_CANDIDATE_ITEMS = " SELECT PRODUCT_HIERARCHY.*, STORE_ITEM_MAP.*, PG.NAME AS CATEGORY_NAME, "
			+ " PG1.NAME AS DEPARTMENT_NAME, CS.COMP_STR_NO, RD.NAME AS DISTRICT_NAME, NO_OF_SHELF, SHELF_CAPACITY FROM "
			+ " (SELECT OOS_ITEM.*, IMS.TOT_MOVEMENT, IL.UPC, IL.RET_LIR_ID, IL.RETAILER_ITEM_CODE, IL.ITEM_NAME FROM "
			+ " (SELECT OOS_F.LOCATION_LEVEL_ID, OOS_F.LOCATION_ID, "
			+ " OOS_F.CALENDAR_ID, OOS_F.ITEM_CODE, OOS_F.PRESTO_FORECAST, "
			+ " OOS_F.CLIENT_FORECAST, OOS_F.REG_PRICE, OOS_F.REG_QUANTITY, OOS_F.SALE_QUANTITY, OOS_F.SALE_PRICE, "
			+ " OOS_F.AD_PAGE_NO, OOS_F.AD_BLOCK_NO, OOS_F.DISPLAY_TYPE_ID, OOS_F.PROMO_TYPE_ID, OOS_F.PRESTO_CONFIDENCE_LEVEL_LOWER, "
			+ " OOS_F.PRESTO_CONFIDENCE_LEVEL_UPPER, OOS_F.DIST_FLAG, OOS_F.RET_LIR_ID AS OOS_RET_LIR_ID FROM OOS_CANDIDATE_ITEM OOS_F ";
	
	private static final String GET_CANDIADATE_ITEMS_WITH_CHAIN_FORECAST = " SELECT PRODUCT_HIERARCHY.*, STORE_ITEM_MAP.*, PG.NAME AS CATEGORY_NAME, "
			+ " PG1.NAME AS DEPARTMENT_NAME FROM "
			+ " (SELECT OOS_ITEM.*, "
			//+ " NVL(IMS1.TOT_MOVEMENT_GU,0) + NVL(IMS2.TOT_MOVEMENT_TOPS,0) AS TOT_MOVEMENT, "
			+ " NVL(IMS2.TOT_MOVEMENT_TOPS,0) AS TOT_MOVEMENT, "
			+ " IL.UPC, IL.RETAILER_ITEM_CODE, RLIG.RET_LIR_NAME, "
			+ " IL.ITEM_NAME, WAB.ADJUSTED_UNITS AS CLIENT_FORECAST_ACTUAL, WAB.NO_OF_LIG_OR_NON_LIG FROM "
			+ " (SELECT OOS_F.LOCATION_LEVEL_ID, OOS_F.LOCATION_ID, "
			+ " OOS_F.CALENDAR_ID, OOS_F.ITEM_CODE, OOS_F.PRESTO_FORECAST, "
			+ " OOS_F.REG_PRICE, OOS_F.REG_QUANTITY, OOS_F.SALE_QUANTITY, OOS_F.SALE_PRICE, "
			+ " OOS_F.AD_PAGE_NO, OOS_F.AD_BLOCK_NO, OOS_F.DISPLAY_TYPE_ID, OOS_F.PROMO_TYPE_ID, OOS_F.PRESTO_CONFIDENCE_LEVEL_LOWER, "
			+ " OOS_F.PRESTO_CONFIDENCE_LEVEL_UPPER, OOS_F.DIST_FLAG, OOS_F.RET_LIR_ID, OOS_F.PREDICTION_STATUS FROM OOS_CANDIDATE_ITEM OOS_F ";
		
	private static final String GET_AD_ITEM_CHAIN_FORECAST = " SELECT PRODUCT_HIERARCHY.*, STORE_ITEM_MAP.*, PG.NAME AS CATEGORY_NAME, "
			+ " PG1.NAME AS DEPARTMENT_NAME, CS.COMP_STR_NO, RD.NAME AS DISTRICT_NAME FROM "
			+ " (SELECT OOS_ITEM.*, IL.UPC, IL.RET_LIR_ID, IL.RETAILER_ITEM_CODE, IL.ITEM_NAME FROM "
			+ " (SELECT OOS_F.LOCATION_LEVEL_ID, OOS_F.LOCATION_ID, "
			+ " OOS_F.CALENDAR_ID, OOS_F.ITEM_CODE, OOS_F.PRESTO_FORECAST, "
			+ " OOS_F.CLIENT_FORECAST, OOS_F.REG_PRICE, OOS_F.REG_QUANTITY, OOS_F.SALE_QUANTITY, OOS_F.SALE_PRICE, "
			+ " OOS_F.AD_PAGE_NO, OOS_F.AD_BLOCK_NO, OOS_F.DISPLAY_TYPE_ID, OOS_F.PROMO_TYPE_ID, OOS_F.PRESTO_CONFIDENCE_LEVEL_LOWER, "
			+ " OOS_F.PRESTO_CONFIDENCE_LEVEL_UPPER, OOS_F.PREDICTION_STATUS FROM OOS_CANDIDATE_ITEM OOS_F ";
	
	private static final String GET_ZONE_STORE_COUNT = "SELECT PRICE_ZONE_ID, COUNT(COMP_STR_ID) AS NO_OF_STORES FROM COMPETITOR_STORE "
			+ " WHERE PRICE_ZONE_ID IS NOT NULL GROUP BY PRICE_ZONE_ID";
	
	/**
	 * Gets all the authorized items from STORE_ITEM_MAP table for
	 * location/product combination. supports store level location only.
	 * 
	 * @param conn
	 * @param productLevelId
	 * @param productId
	 * @param locationLevelId
	 * @param locationId
	 * @return list of item codes for corresponding location and product
	 * @throws GeneralException
	 */
	public List<PRItemDTO> getAuthorizedItems(Connection conn, int productLevelId, int productId, int locationLevelId, int locationId)
			throws GeneralException {
		List<PRItemDTO> itemList = new ArrayList<PRItemDTO>();
		ProductService productService = new ProductService();
		HashMap<Integer, Integer> parentChildRelationMap = productService.getProductLevelRelationMap(conn, productLevelId);
		HashMap<Integer, Integer> productLevelPointer = productService.getProductLevelPointer(conn, productLevelId);
		PreparedStatement statement = null;
		ResultSet resultSet = null;
		try {
			statement = conn.prepareStatement(queryToGetAuthorizedItems(conn, productLevelId, productId, locationLevelId, locationId, productService,
					parentChildRelationMap, productLevelPointer));
			statement.setFetchSize(200000);
			if (productId > 0 && productLevelId > 0) {
				int counter = 0;
				statement.setInt(++counter, productLevelId);
				statement.setInt(++counter, productId);
			}
			resultSet = statement.executeQuery();
			while (resultSet.next()) {
				PRItemDTO itemDTO = new PRItemDTO();
				for (Integer productLevel : parentChildRelationMap.keySet()) {
					if (productLevel == Constants.DEPARTMENTLEVELID)
						itemDTO.setDeptProductId(resultSet.getInt("P_ID_" + productLevelPointer.get(productLevel)));
					else if (productLevel == Constants.PORTFOLIO)
						itemDTO.setPortfolioProductId(resultSet.getInt("P_ID_" + productLevelPointer.get(productLevel)));
					else if (productLevel == Constants.CATEGORYLEVELID)
						itemDTO.setCategoryProductId(resultSet.getInt("P_ID_" + productLevelPointer.get(productLevel)));
					else if (productLevel == Constants.SUBCATEGORYLEVELID)
						itemDTO.setSubCatProductId(resultSet.getInt("P_ID_" + productLevelPointer.get(productLevel)));
					else if (productLevel == Constants.SEGMENTLEVELID)
						itemDTO.setSegmentProductId(resultSet.getInt("P_ID_" + productLevelPointer.get(productLevel)));
					else if (productLevel == Constants.ITEMLEVELID)
						itemDTO.setItemCode(resultSet.getInt("ITEM_CODE"));
				}
				if (resultSet.getString("DIST_FLAG").equals(String.valueOf(Constants.DSD)))
					itemDTO.setDistFlag(Constants.DSD);
				else
					itemDTO.setDistFlag(Constants.WAREHOUSE);
				itemDTO.setCategoryName(resultSet.getString("PRODUCT_NAME"));
				itemDTO.setStoreNo(resultSet.getString("COMP_STR_NO"));
				itemDTO.setDistrictName(resultSet.getString("NAME"));
				itemDTO.setRetLirId(resultSet.getInt("RET_LIR_ID"));
				itemList.add(itemDTO);
			}
		} catch (SQLException sqlE) {
			logger.error("Error while getting authorized items", sqlE);
			throw new GeneralException("Error while getting authorized items", sqlE);
		} finally {
			PristineDBUtil.close(resultSet);
			PristineDBUtil.close(statement);
		}
		return itemList;
	}

	/**
	 * @param conn
	 * @param calendarId
	 * @param chainId
	 * @param locationId
	 * @param locationLevelId
	 * @return list of items which are in promotion as well as in page 1 of
	 *         weekly ad
	 * @throws GeneralException
	 */

	public List<PRItemDTO> getPromotionDetails(Connection conn, int calendarId, int chainId, int locationId, int locationLevelId, int productLevelId,
			int productId) throws GeneralException {
		List<PRItemDTO> promoDetailList = new ArrayList<PRItemDTO>();
		PreparedStatement statement = null;
		ResultSet resultSet = null;
		try {

			String promoQry = "";
//			if (locationLevelId == Constants.STORE_LIST_LEVEL_ID)
//				promoQry = promoQueryForStoreList(conn, calendarId, chainId, locationId, locationLevelId, productLevelId, productId);
//			else
//				promoQry = promotionQuery(calendarId, chainId, locationId, locationLevelId);
			
			promoQry = promoQueryForStoreList(conn, calendarId, chainId, locationId, locationLevelId, productLevelId, productId);

			logger.debug("Promo query -- " + promoQry);
			statement = conn.prepareStatement(promoQry);
			statement.setFetchSize(200000);
			if (productId > 0 && productLevelId > 0) {
				int counter = 0;
				statement.setInt(++counter, productLevelId);
				statement.setInt(++counter, productId);
			}
			resultSet = statement.executeQuery();
			while (resultSet.next()) {
				PRItemDTO prItemDTO = new PRItemDTO();
				// prItemDTO.setAdName(resultSet.getString("AD_NAME"));
				prItemDTO.setWeeklyAdLocationLevelId(resultSet.getInt("LOCATION_LEVEL_ID"));
				prItemDTO.setWeeklyAdLocationId(resultSet.getInt("LOCATION_ID"));
				prItemDTO.setItemCode(resultSet.getInt("ITEM_CODE"));
				prItemDTO.setRegMPack(resultSet.getInt("REG_QTY"));
				prItemDTO.setRegPrice(resultSet.getDouble("REG_PRICE"));
				prItemDTO.setRegMPrice(resultSet.getDouble("REG_M_PRICE"));
				prItemDTO.setSaleMPack(resultSet.getInt("SALE_QTY"));
				prItemDTO.setSalePrice(resultSet.getDouble("SALE_PRICE"));
				prItemDTO.setSaleMPrice(resultSet.getDouble("SALE_M_PRICE"));
				prItemDTO.setPageNumber(resultSet.getInt("PAGE_NUMBER"));
				prItemDTO.setBlockNumber(resultSet.getInt("BLOCK_NUMBER"));
				prItemDTO.setPromoTypeId(resultSet.getInt("PROMO_TYPE_ID"));
				prItemDTO.setDisplayTypeId(resultSet.getInt("DISPLAY_TYPE_ID"));
				prItemDTO.setAdjustedUnits(resultSet.getLong("CLIENT_FORECAST"));
				prItemDTO.setRetailerItemCode(resultSet.getString("SRC_VENDOR_AND_ITEM_ID"));
				prItemDTO.setUpc(resultSet.getString("UPC"));
				prItemDTO.setItemName(resultSet.getString("ITEM_NAME"));
				prItemDTO.setPromoDefinitionId(resultSet.getLong("PROMO_DEFINITION_ID"));
				prItemDTO.setRetLirPromoKey(resultSet.getInt("RET_LIR_ID"));
				prItemDTO.setPromoCreatedDate(resultSet.getDate("CREATED"));
				prItemDTO.setPromoModifiedDate(resultSet.getDate("MODIFIED"));
				prItemDTO.setRetailerItemCode(resultSet.getString("RETAILER_ITEM_CODE"));
			
//				prItemDTO.setDisplayName(resultSet.getString("DISPLAY_NAME"));
//				prItemDTO.setPromoTypeName(resultSet.getString("PROMO_TYPE_NAME"));
				promoDetailList.add(prItemDTO);
			}
		} catch (SQLException sqlE) {
			logger.error("Error while getting weekly ad and promo details", sqlE);
			throw new GeneralException("Error while getting weekly ad and promo details", sqlE);
		} finally {
			PristineDBUtil.close(resultSet);
			PristineDBUtil.close(statement);
		}
		return promoDetailList;
	}
	
	/**
	 * forms query to get authorized items from STORE_ITEM_MAP table.
	 * 
	 * @param productLevelId
	 * @param productId
	 * @param locationLevelId
	 * @param locationId
	 * @return select query
	 * @throws GeneralException
	 */
	private String queryToGetAuthorizedItems(Connection conn, int productLevelId, int productId, int locationLevelId, int locationId,
			ProductService productService, HashMap<Integer, Integer> parentChildRelationMap, HashMap<Integer, Integer> productLevelPointer)
					throws GeneralException {
		com.pristine.dao.offermgmt.ItemDAO itemDAO = new com.pristine.dao.offermgmt.ItemDAO();
		StringBuffer subQuery = new StringBuffer("");
		
		if (locationLevelId == Constants.STORE_LEVEL_ID) {
			subQuery.append(" SELECT  STORE_ITEM_MAP.ITEM_CODE ITEM_CODE, STORE_ITEM_MAP.RET_LIR_ID, STORE_ITEM_MAP.DIST_FLAG, ");
			subQuery.append(" STORE_ITEM_MAP.LEVEL_ID, CS.COMP_STR_NO, RD.NAME, ");
			subQuery.append(" PRODUCT_HIERARCHY.*, PG.NAME PRODUCT_NAME ");
			subQuery.append(" FROM (SELECT SIM.ITEM_CODE, IL.RET_LIR_ID, LEVEL_ID, VENDOR_ID, ");
			subQuery.append(" DIST_FLAG, COST_INDICATOR FROM STORE_ITEM_MAP SIM ");
			subQuery.append(" LEFT JOIN ITEM_LOOKUP IL ON SIM.ITEM_CODE = IL.ITEM_CODE ");
			subQuery.append(" WHERE SIM.LEVEL_TYPE_ID = 2 ");
			subQuery.append(" AND SIM.LEVEL_ID IN ( ");
			subQuery.append(locationId);
			subQuery.append(" ) ");
			if (productId > 0 && productLevelId > 0) {
				subQuery.append(" AND SIM.ITEM_CODE IN (");
				subQuery.append(" SELECT CHILD_PRODUCT_ID FROM (SELECT CHILD_PRODUCT_ID,CHILD_PRODUCT_LEVEL_ID FROM PRODUCT_GROUP_RELATION PGR ");
				subQuery.append(getQueryCondition(productLevelId, productId));
				subQuery.append(" ) WHERE CHILD_PRODUCT_LEVEL_ID = 1) ");
			}
			subQuery.append(" AND SIM.IS_AUTHORIZED = 'Y') STORE_ITEM_MAP LEFT JOIN ");
			subQuery.append(itemDAO.getProductHierarchy(conn, productLevelId, productService));
			subQuery.append(" JOIN PRODUCT_GROUP PG ON ");
			subQuery.append(" PRODUCT_HIERARCHY.P_ID_" + productLevelPointer.get(Constants.CATEGORYLEVELID) + " = PG.PRODUCT_ID AND ");
			subQuery.append(" PG.PRODUCT_LEVEL_ID = " + Constants.CATEGORYLEVELID);
			subQuery.append(" LEFT JOIN COMPETITOR_STORE CS ON CS.COMP_STR_ID = LEVEL_ID ");
			subQuery.append(" LEFT JOIN RETAIL_DISTRICT RD ON CS.DISTRICT_ID = RD.ID ");
		} else if (locationLevelId == Constants.STORE_LIST_LEVEL_ID) {
			subQuery.append(" SELECT STORE_ITEM_MAP.ITEM_CODE AS ITEM_CODE, IL.RET_LIR_ID, ' ' AS DIST_FLAG, ");
			subQuery.append(" 0 AS LEVEL_ID, ' ' AS COMP_STR_NO, ' ' AS NAME, ");
			subQuery.append(" PRODUCT_HIERARCHY.*, PG.NAME PRODUCT_NAME ");
			subQuery.append(" FROM (SELECT DISTINCT(SIM.ITEM_CODE) FROM STORE_ITEM_MAP SIM ");
			subQuery.append(" WHERE SIM.LEVEL_TYPE_ID = 2 ");
			subQuery.append(" AND SIM.LEVEL_ID IN ( ");
			subQuery.append(" SELECT CHILD_LOCATION_ID FROM LOCATION_GROUP_RELATION WHERE ");
			subQuery.append(" LOCATION_LEVEL_ID = " + locationLevelId + " AND LOCATION_ID = " + locationId);
			subQuery.append(" ) ");
			if (productId > 0 && productLevelId > 0) {
				subQuery.append(" AND SIM.ITEM_CODE IN (");
				subQuery.append(" SELECT CHILD_PRODUCT_ID FROM (SELECT CHILD_PRODUCT_ID,CHILD_PRODUCT_LEVEL_ID FROM PRODUCT_GROUP_RELATION PGR ");
				subQuery.append(getQueryCondition(productLevelId, productId));
				subQuery.append(" ) WHERE CHILD_PRODUCT_LEVEL_ID = 1) ");
			}
			subQuery.append(" AND SIM.IS_AUTHORIZED = 'Y') STORE_ITEM_MAP ");		
			subQuery.append(" LEFT JOIN ITEM_LOOKUP IL ON STORE_ITEM_MAP.ITEM_CODE = IL.ITEM_CODE ");
			subQuery.append(" LEFT JOIN ");
			subQuery.append(itemDAO.getProductHierarchy(conn, productLevelId, productService));
			subQuery.append(" JOIN PRODUCT_GROUP PG ON ");
			subQuery.append(" PRODUCT_HIERARCHY.P_ID_" + productLevelPointer.get(Constants.CATEGORYLEVELID) + " = PG.PRODUCT_ID AND ");
			subQuery.append(" PG.PRODUCT_LEVEL_ID = " + Constants.CATEGORYLEVELID);
		}
		// For debugging
		/*
		 * subQuery.append(
		 * " WHERE STORE_ITEM_MAP.ITEM_CODE IN (400491, 400492, 400493, 930693, 930694, 930695"
		 * +
		 * ",305543,305546,40022,40023,35313,825037,15632,305544,305545,220571,16563,25142,76752,55558)"
		 * );//Testing
		 */
		logger.debug("Authorization query: " + subQuery.toString());
		return subQuery.toString();
	}

	/**
	 * Gives product query to get the child products of given product id and
	 * product level id
	 * 
	 * @param productLevelId
	 * @param productId
	 * @return product query
	 */
	private String getQueryCondition(int productLevelId, int productId) {
		StringBuffer subQuery = new StringBuffer("");
		if (productLevelId > 1) {
			subQuery = new StringBuffer("start with product_level_id = ? ");
			if (productId > 0) {
				subQuery.append(" and product_id = ? ");
			}
			subQuery.append("connect by  prior child_product_id = product_id  and  prior child_product_level_id = product_level_id");
		} else if (productLevelId == 1) {
			subQuery = new StringBuffer(
					"where child_product_level_id = 1 and child_product_id = ?");
		}
		return subQuery.toString();
	}

	/**
	 * Query to get promotion and weekly ad information for given location
	 * 
	 * @param calendarId
	 * @param chainId
	 * @param locationId
	 * @param locationLevelId
	 * @return Query to get promotional information such as promotion type,
	 *         page, block and display etc.,
	 */
	@SuppressWarnings("unused")
	private String promotionQuery(int calendarId, int chainId, int locationId, int locationLevelId) {
		StringBuilder sb = new StringBuilder();
		sb.append(" SELECT PTL.PROMO_TYPE_NAME, PTL.PROMO_TYPE_ID, IL.ITEM_NAME,  IL.UPC, IL.ITEM_CODE, IL.RETAILER_ITEM_CODE, WEEKLY_AD.AD_NAME, ");
		sb.append(" PBI.REG_QTY, PBI.REG_PRICE, PBI.REG_M_PRICE, PBI.SALE_QTY, PBI.SALE_PRICE, PBI.SALE_M_PRICE, ");
		sb.append(" WEEKLY_AD.PAGE_NUMBER, WEEKLY_AD.BLOCK_NUMBER, PDTL.DISPLAY_NAME, PDTL.DISPLAY_TYPE_ID, ");
		sb.append(" PD.ADJUSTED_UNITS, WEEKLY_AD.LOCATION_LEVEL_ID, WEEKLY_AD.LOCATION_ID, PD.RET_LIR_ID, WEEKLY_AD.TOTAL_ITEMS, ");
		sb.append(" CASE WHEN (PD.RET_LIR_ID <> -1 AND PD.ADJUSTED_UNITS <> 0 AND PD.ADJUSTED_UNITS IS NOT NULL AND WEEKLY_AD.TOTAL_ITEMS > 0) ");
		sb.append(" THEN ROUND(PD.ADJUSTED_UNITS / WEEKLY_AD.TOTAL_ITEMS) ");
		sb.append(" ELSE PD.ADJUSTED_UNITS END AS CLIENT_FORECAST, PD.PROMO_DEFINITION_ID, PD.RET_LIR_ID, PD.SRC_VENDOR_AND_ITEM_ID, ");
		sb.append(" PD.CREATED, PD.MODIFIED ");
		sb.append(" FROM PM_PROMO_DEFINITION PD JOIN PM_PROMO_BUY_ITEM PBI ON PBI.PROMO_DEFINITION_ID = PD.PROMO_DEFINITION_ID ");
		sb.append(" JOIN (SELECT AD_NAME,  LOCATION_LEVEL_ID, LOCATION_ID, CALENDAR_ID, TOTAL_PAGES, ");
		sb.append(" PAGE_NUMBER, BLOCK_NUMBER, PROMO_DEFINITION_ID, TOTAL_ITEMS FROM ");

		// Add weekly ad query
		sb.append(" (" + weeklyAdQuery(calendarId, chainId, locationId, locationLevelId)
				+ ")) WEEKLY_AD ON WEEKLY_AD.PROMO_DEFINITION_ID = PD.PROMO_DEFINITION_ID ");
		sb.append(" JOIN PM_PROMO_LOCATION PL ON PD.PROMO_DEFINITION_ID = PL.PROMO_DEFINITION_ID ");

		// Join with item lookup
		sb.append(" JOIN ITEM_LOOKUP IL ON IL.ITEM_CODE = PBI.ITEM_CODE ");
		// Join with promo type lookup
		sb.append(" JOIN PM_PROMO_TYPE_LOOKUP PTL ON ");

		// Join with Display tables
		sb.append(" PTL.PROMO_TYPE_ID = PD.PROMO_TYPE_ID LEFT JOIN PM_PROMO_DISPLAY PDISP ON ");
		sb.append(" PDISP.PROMO_DEFINITION_ID = PD.PROMO_DEFINITION_ID LEFT JOIN PM_DISPLAY_TYPE_LOOKUP PDTL ON ");
		sb.append(" PDISP.DISPLAY_TYPE_ID = PDTL.DISPLAY_TYPE_ID WHERE ");

		// Check weekly ad is defined at chain level
		sb.append(" ((PL.LOCATION_ID = " + chainId + "AND PL.LOCATION_LEVEL_ID = " + Constants.CHAIN_LEVEL_ID + ") ");

		if (locationLevelId == Constants.STORE_LEVEL_ID) {
			// Check weekly ad is defined at zone list level
			sb.append(" OR (PL.LOCATION_ID IN (SELECT LOCATION_ID FROM LOCATION_GROUP_RELATION WHERE  CHILD_LOCATION_LEVEL_ID = "
					+ Constants.ZONE_LEVEL_ID + " AND ");
			sb.append(" CHILD_LOCATION_ID = (SELECT PRICE_ZONE_ID FROM COMPETITOR_STORE WHERE COMP_STR_ID = " + locationId + "))");
			sb.append(" AND PL.LOCATION_LEVEL_ID = " + Constants.ZONE_LIST_LEVEL_ID + ") ");

			// Check weekly ad is defined at store list level
			sb.append(" OR (PL.LOCATION_ID IN (SELECT LOCATION_ID FROM LOCATION_GROUP_RELATION ");
			sb.append(" WHERE CHILD_LOCATION_ID = " + locationId + " AND CHILD_LOCATION_LEVEL_ID = " + Constants.STORE_LEVEL_ID + ") ");
			sb.append(" AND PL.LOCATION_LEVEL_ID = " + Constants.STORE_LIST_LEVEL_ID + ") ");

			// Check weekly ad is defined at Zone level
			sb.append(" OR (PL.LOCATION_ID IN (SELECT PRICE_ZONE_ID FROM COMPETITOR_STORE WHERE COMP_STR_ID = " + locationId + ") ");
			sb.append(" AND PL.LOCATION_LEVEL_ID = " + Constants.ZONE_LEVEL_ID + ") ");

			// Check weekly ad is defined at store level
			sb.append(" OR (PL.LOCATION_ID = " + locationId + " AND PL.LOCATION_LEVEL_ID = " + Constants.STORE_LEVEL_ID + ")");
		} else if (locationLevelId == Constants.STORE_LIST_LEVEL_ID) {
			// Check weekly ad is defined at store list level
			sb.append(" OR (PL.LOCATION_LEVEL_ID = " + locationLevelId + " AND PL.LOCATION_ID = " + locationId + ")");

			// Check weekly ad is defined at store level
			sb.append(" OR (PL.LOCATION_LEVEL_ID  = " + Constants.STORE_LEVEL_ID + " AND PL.LOCATION_ID IN ");
			sb.append(" (SELECT CHILD_LOCATION_ID FROM LOCATION_GROUP_RELATION WHERE LOCATION_LEVEL_ID = " + locationLevelId);
			sb.append(" AND LOCATION_ID = " + locationId + " AND CHILD_LOCATION_LEVEL_ID = " + Constants.STORE_LEVEL_ID + "))");

			// Check weekly ad is defined at zone level
			sb.append(" OR (PL.LOCATION_LEVEL_ID = " + Constants.ZONE_LEVEL_ID + " AND PL.LOCATION_ID IN ");
			sb.append(" (SELECT PRICE_ZONE_ID FROM COMPETITOR_STORE WHERE COMP_STR_ID IN (SELECT CHILD_LOCATION_ID ");
			sb.append(" FROM LOCATION_GROUP_RELATION WHERE LOCATION_LEVEL_ID = " + locationLevelId);
			sb.append(" AND LOCATION_ID = " + locationId);
			sb.append(" AND CHILD_LOCATION_LEVEL_ID = " + Constants.STORE_LEVEL_ID + ")))");
		}
		sb.append(")");
		
		return sb.toString();
	}

	private String promoQueryForStoreList(Connection conn, int weekCalendarId, int chainId, int locationId, int locationLevelId, 
			int productLevelId, int productId) throws GeneralException {

		StringBuilder sb = new StringBuilder();
		String perishablesDeptIds = PropertyManager.getProperty("PERISHABLES_DEPARTMENT_IDS");
		com.pristine.dao.offermgmt.ItemDAO itemDAO = new com.pristine.dao.offermgmt.ItemDAO();
		ProductService productService = new ProductService();
		RetailCalendarDAO calDAO = new RetailCalendarDAO();
//		DateFormat appDateFormatter = new SimpleDateFormat(Constants.APP_DATE_FORMAT);
		
		RetailCalendarDTO retailCalendarDTO = calDAO.getCalendarDetail(conn, weekCalendarId) ;
		RetailCalendarDTO startCalendarDTO = calDAO.getCalendarId(conn, retailCalendarDTO.getStartDate(), Constants.CALENDAR_DAY);
		RetailCalendarDTO endCalendarDTO = calDAO.getCalendarId(conn, retailCalendarDTO.getEndDate(), Constants.CALENDAR_DAY);
		productService.getProductLevelRelationMap(conn, productLevelId);
		productService.getProductLevelPointer(conn, productLevelId);
		
		sb.append(" SELECT * FROM (SELECT * FROM (SELECT * FROM (SELECT PTL.PROMO_TYPE_ID, ");
		sb.append(" IL.ITEM_NAME, IL.UPC, IL.ITEM_CODE, IL.RETAILER_ITEM_CODE, PBI.REG_QTY, ");
		sb.append(" PBI.REG_PRICE, PBI.REG_M_PRICE, PBI.SALE_QTY, PBI.SALE_PRICE, PBI.SALE_M_PRICE, ");
		sb.append(" WEEKLY_AD.PAGE_NUMBER, WEEKLY_AD.BLOCK_NUMBER, PDTL.DISPLAY_TYPE_ID, ");
		sb.append(" WEEKLY_AD.LOCATION_LEVEL_ID, WEEKLY_AD.LOCATION_ID, PD.RET_LIR_ID, ");
		sb.append(" WEEKLY_AD.ADJUSTED_UNITS AS CLIENT_FORECAST, PD.PROMO_DEFINITION_ID, ");
		sb.append(" PD.SRC_VENDOR_AND_ITEM_ID, PD.CREATED, PD.MODIFIED ");
		sb.append(" FROM PM_PROMO_DEFINITION PD JOIN PM_PROMO_BUY_ITEM PBI ");
		sb.append(" ON PBI.PROMO_DEFINITION_ID = PD.PROMO_DEFINITION_ID ");
		//NU::10th Sep 2016, commented as it is handled using ad plex feed 
		//sb.append(" AND PBI.IS_IN_AD_PLEX = 'Y' ");
		sb.append(" JOIN ");
		sb.append(" (" + weeklyAdQuery(weekCalendarId, chainId, locationId, locationLevelId)
				+ ") WEEKLY_AD ON WEEKLY_AD.PROMO_DEFINITION_ID = PD.PROMO_DEFINITION_ID ");
		sb.append(" JOIN ITEM_LOOKUP IL ON IL.ITEM_CODE = PBI.ITEM_CODE ");
		sb.append(" JOIN PM_PROMO_TYPE_LOOKUP PTL ON PTL.PROMO_TYPE_ID = PD.PROMO_TYPE_ID ");
		sb.append(" LEFT JOIN PM_PROMO_DISPLAY PDISP ON PDISP.PROMO_DEFINITION_ID = PD.PROMO_DEFINITION_ID ");
		sb.append(" LEFT JOIN PM_DISPLAY_TYPE_LOOKUP PDTL ON PDISP.DISPLAY_TYPE_ID = PDTL.DISPLAY_TYPE_ID) PD ");
		sb.append(" WHERE PD.PROMO_DEFINITION_ID IN ( ");
		sb.append(" SELECT DISTINCT(PROMO_DEFINITION_ID) FROM PM_PROMO_LOCATION PL WHERE ");
		sb.append(" PL.PROMO_DEFINITION_ID IN (SELECT PROMO_DEFINITION_ID ");
		sb.append(" FROM PM_PROMO_DEFINITION WHERE START_CALENDAR_ID = ").append(startCalendarDTO.getCalendarId());
		sb.append(" AND END_CALENDAR_ID= ").append(endCalendarDTO.getCalendarId()).append(" ) AND ");
		sb.append(PRCommonUtil.getPromotionLocationQuery(chainId, locationLevelId, locationId));
		// Check if defined at chain level
//		sb.append(" ((PL.LOCATION_ID = " + chainId + "AND PL.LOCATION_LEVEL_ID = " + Constants.CHAIN_LEVEL_ID + ") ");
//
//		if (locationLevelId == Constants.STORE_LEVEL_ID) {
//			// Check if defined at store level
//			sb.append(" OR (PL.LOCATION_ID = " + locationId + " AND PL.LOCATION_LEVEL_ID = " + Constants.STORE_LEVEL_ID + ")");
//
//			// Check if defined at store list level
//			sb.append(" OR (PL.LOCATION_ID IN (SELECT LOCATION_ID FROM LOCATION_GROUP_RELATION ");
//			sb.append(" WHERE CHILD_LOCATION_ID = " + locationId + " AND CHILD_LOCATION_LEVEL_ID = " + Constants.STORE_LEVEL_ID + ") ");
//			sb.append(" AND PL.LOCATION_LEVEL_ID = " + Constants.STORE_LIST_LEVEL_ID + ") ");
//
//			// Check if defined at Zone level
//			sb.append(" OR (PL.LOCATION_ID IN (SELECT PRICE_ZONE_ID FROM COMPETITOR_STORE WHERE COMP_STR_ID = " + locationId + ") ");
//			sb.append(" AND PL.LOCATION_LEVEL_ID = " + Constants.ZONE_LEVEL_ID + ") ");
//			
//		} else if (locationLevelId == Constants.STORE_LIST_LEVEL_ID) {
//			// Check if defined at store list level
//			sb.append(" OR (PL.LOCATION_LEVEL_ID = " + locationLevelId + " AND PL.LOCATION_ID = " + locationId + ")");
//
//			// Check is defined at store level
//			sb.append(" OR (PL.LOCATION_LEVEL_ID  = " + Constants.STORE_LEVEL_ID + " AND PL.LOCATION_ID IN ");
//			sb.append(" (SELECT CHILD_LOCATION_ID FROM LOCATION_GROUP_RELATION WHERE LOCATION_LEVEL_ID = " + locationLevelId);
//			sb.append(" AND LOCATION_ID = " + locationId + " AND CHILD_LOCATION_LEVEL_ID = " + Constants.STORE_LEVEL_ID + "))");
//
//			// Check if defined at zone level
//			sb.append(" OR (PL.LOCATION_LEVEL_ID = " + Constants.ZONE_LEVEL_ID + " AND PL.LOCATION_ID IN ");
//			sb.append(" (SELECT PRICE_ZONE_ID FROM COMPETITOR_STORE WHERE COMP_STR_ID IN (SELECT CHILD_LOCATION_ID ");
//			sb.append(" FROM LOCATION_GROUP_RELATION WHERE LOCATION_LEVEL_ID = " + locationLevelId);
//			sb.append(" AND LOCATION_ID = " + locationId);
//			sb.append(" AND CHILD_LOCATION_LEVEL_ID = " + Constants.STORE_LEVEL_ID + ")))");
//		}
		sb.append(" )) STORE_ITEM_MAP ");
		sb.append(" LEFT JOIN ");
		sb.append(itemDAO.getProductHierarchy(conn, productLevelId, productService));		

		sb.append(" ) WHERE P_ID_").append(Constants.DEPARTMENTLEVELID - 1).append(" NOT IN (").append(perishablesDeptIds).append(")");
		if (productId > 0 && productLevelId > 0) {
			sb.append(" AND ITEM_CODE IN (");
			sb.append(" SELECT CHILD_PRODUCT_ID FROM (SELECT CHILD_PRODUCT_ID,CHILD_PRODUCT_LEVEL_ID FROM PRODUCT_GROUP_RELATION PGR ");
			sb.append(getQueryCondition(productLevelId, productId));
			sb.append(" ) WHERE CHILD_PRODUCT_LEVEL_ID = 1) ");
		}
		return sb.toString();
	}
	/**
	 * 
	 * @param calendarId
	 * @param chainId
	 * @param locationId
	 * @param locationLevelId
	 * @return Query to get Weekly ad info
	 */
	private String weeklyAdQuery(int calendarId, int chainId, int locationId, int locationLevelId) {
		StringBuilder sb = new StringBuilder();
		sb.append(" SELECT WAD.AD_NAME, WAD.LOCATION_LEVEL_ID, WAD.LOCATION_ID, WAD.CALENDAR_ID, WAD.TOTAL_PAGES, ");
		sb.append(" WAP.PAGE_NUMBER, WAB.BLOCK_NUMBER, WAB.ADJUSTED_UNITS, WAPR.PROMO_DEFINITION_ID, WAPR.TOTAL_ITEMS "
				+ " FROM PM_WEEKLY_AD_DEFINITION WAD  ");
		sb.append(" JOIN PM_WEEKLY_AD_PAGE WAP ON WAD.WEEKLY_AD_ID = WAP.WEEKLY_AD_ID JOIN PM_WEEKLY_AD_BLOCK WAB ");
		sb.append(" ON WAB.PAGE_ID = WAP.PAGE_ID JOIN PM_WEEKLY_AD_PROMO WAPR ON WAPR.BLOCK_ID = WAB.BLOCK_ID ");

		// Condition for calendar id
		sb.append(" WHERE WAD.CALENDAR_ID = " + calendarId);
		// + " AND WAP.PAGE_NUMBER = 1 ");
		sb.append(" AND ");
		sb.append(PRCommonUtil.getWeeklyAdLocationQuery(chainId, locationLevelId, locationId));

//		// Check weekly ad is defined at chain level
//		sb.append(" AND ((WAD.LOCATION_ID = " + chainId + "AND WAD.LOCATION_LEVEL_ID = " + Constants.CHAIN_LEVEL_ID + ") ");
//
//		if (locationLevelId == Constants.STORE_LEVEL_ID) {
//			// Check weekly ad is defined at zone list level
//			sb.append(" OR (WAD.LOCATION_ID IN (SELECT LOCATION_ID FROM LOCATION_GROUP_RELATION WHERE  CHILD_LOCATION_LEVEL_ID = "
//					+ Constants.ZONE_LEVEL_ID + " AND ");
//			sb.append(" CHILD_LOCATION_ID = (SELECT PRICE_ZONE_ID FROM COMPETITOR_STORE WHERE COMP_STR_ID = " + locationId + "))");
//			sb.append(" AND WAD.LOCATION_LEVEL_ID = " + Constants.ZONE_LIST_LEVEL_ID + ") ");
//
//			// Check weekly ad is defined at store list level
//			sb.append(" OR (WAD.LOCATION_ID IN (SELECT LOCATION_ID FROM LOCATION_GROUP_RELATION ");
//			sb.append(" WHERE CHILD_LOCATION_ID = " + locationId + " AND CHILD_LOCATION_LEVEL_ID = " + Constants.STORE_LEVEL_ID + ") ");
//			sb.append(" AND WAD.LOCATION_LEVEL_ID = " + Constants.STORE_LIST_LEVEL_ID + ") ");
//
//			// Check weekly ad is defined at Zone level
//			sb.append(" OR (WAD.LOCATION_ID IN (SELECT PRICE_ZONE_ID FROM COMPETITOR_STORE WHERE COMP_STR_ID = " + locationId + ") ");
//			sb.append(" AND WAD.LOCATION_LEVEL_ID = " + Constants.ZONE_LEVEL_ID + ") ");
//
//			// Check weekly ad is defined at store level
//			sb.append(" OR (WAD.LOCATION_ID = " + locationId + " AND WAD.LOCATION_LEVEL_ID = " + Constants.STORE_LEVEL_ID + ")");
//		} else if (locationLevelId == Constants.STORE_LIST_LEVEL_ID) {
//			// Check weekly ad is defined at store list level
//			sb.append(" OR (WAD.LOCATION_LEVEL_ID = " + locationLevelId + " AND WAD.LOCATION_ID = " + locationId + ")");
//
//			// Check weekly ad is defined at store level
//			sb.append(" OR (WAD.LOCATION_LEVEL_ID  = " + Constants.STORE_LEVEL_ID + " AND WAD.LOCATION_ID IN ");
//			sb.append(" (SELECT CHILD_LOCATION_ID FROM LOCATION_GROUP_RELATION WHERE LOCATION_LEVEL_ID = " + locationLevelId);
//			sb.append(" AND LOCATION_ID = " + locationId + " AND CHILD_LOCATION_LEVEL_ID = " + Constants.STORE_LEVEL_ID + "))");
//
//			// Check weekly ad is defined at zone level
//			sb.append(" OR (WAD.LOCATION_LEVEL_ID = " + Constants.ZONE_LEVEL_ID + " AND WAD.LOCATION_ID IN ");
//			sb.append(" (SELECT PRICE_ZONE_ID FROM COMPETITOR_STORE WHERE COMP_STR_ID IN (SELECT CHILD_LOCATION_ID ");
//			sb.append(" FROM LOCATION_GROUP_RELATION WHERE LOCATION_LEVEL_ID = " + locationLevelId);
//			sb.append(" AND LOCATION_ID = " + locationId);
//			sb.append(" AND CHILD_LOCATION_LEVEL_ID = " + Constants.STORE_LEVEL_ID + ")))");
//		}
//		sb.append(" ) ");
		return sb.toString();
	}

	/**
	 * Batch insert to OOS_ITEM table
	 * 
	 * @param conn
	 * @param outOfStockItems
	 * @throws Exception
	 */
	public void insertOOSItems(Connection conn, List<OOSItemDTO> outOfStockItems) throws Exception {
		PreparedStatement stmt = null;
		try {
			stmt = conn.prepareStatement(INSERT_OOS_ITEM);
			int insertCount = 0;
			for (OOSItemDTO oosItem : outOfStockItems) {
				int counter = 0;

				stmt.setInt(++counter, oosItem.getLocationLevelId());
				stmt.setInt(++counter, oosItem.getLocationId());
				stmt.setInt(++counter, oosItem.getCalendarId());
				stmt.setInt(++counter, oosItem.getDayPartId());
				stmt.setLong(++counter, oosItem.getOOSCriteriaData().getForecastMovOfProcessingTimeSlotOfItemOrLig());
				stmt.setLong(++counter, oosItem.getOOSCriteriaData().getActualMovOfProcessingTimeSlotOfItemOrLig());

				if (oosItem.getRegPrice() != null) {
					stmt.setInt(++counter, oosItem.getRegPrice().multiple);
					stmt.setDouble(++counter, oosItem.getRegPrice().price);
				} else {
					stmt.setNull(++counter, Types.INTEGER);
					stmt.setNull(++counter, Types.DOUBLE);
				}

				if (oosItem.getRegPrice() != null) {
					stmt.setInt(++counter, oosItem.getSalePrice().multiple);
					stmt.setDouble(++counter, oosItem.getSalePrice().price);
				} else {
					stmt.setNull(++counter, Types.INTEGER);
					stmt.setNull(++counter, Types.DOUBLE);
				}

				if (oosItem.getAdPageNo() != Constants.NULLID)
					stmt.setInt(++counter, oosItem.getAdPageNo());
				else
					stmt.setNull(++counter, Types.INTEGER);

				if (oosItem.getDisplayTypeId() != Constants.NULLID)
					stmt.setInt(++counter, oosItem.getDisplayTypeId());
				else
					stmt.setNull(++counter, Types.INTEGER);

				stmt.setInt(++counter, oosItem.getNoOfZeroMovInLastXWeeks());
				stmt.setLong(++counter, oosItem.getMinMovementInLastXWeeks());
				stmt.setLong(++counter, oosItem.getMaxMovementInLastXWeeks());
				stmt.setLong(++counter, oosItem.getAvgMovementInLastXWeeks());

				stmt.setLong(++counter, oosItem.getOOSCriteriaData().getLowerConfidenceOfProcessingTimeSlotOfItemOrLig());
				stmt.setLong(++counter, oosItem.getOOSCriteriaData().getUpperConfidenceOfProcessingTimeSlotOfItemOrLig());
				stmt.setInt(++counter, oosItem.getNoOfTimeMovedInLastXWeeksOfItemOrLig());
				
				if(oosItem.getIsOutOfStockItem())
	        		stmt.setString(++counter, String.valueOf(Constants.YES));
	        	else
	        		stmt.setString(++counter, String.valueOf(Constants.NO));
				
				if(oosItem.getIsSendToClient())
	        		stmt.setString(++counter, String.valueOf(Constants.YES));
	        	else
	        		stmt.setString(++counter, String.valueOf(Constants.NO));
				
				stmt.setLong(++counter, oosItem.getClientDayPartPredictedMovement());
				if(oosItem.getOOSCriteriaData().getNoOfConsecutiveTimeSlotItemDidNotMove() != null)
					stmt.setInt(++counter, oosItem.getOOSCriteriaData().getNoOfConsecutiveTimeSlotItemDidNotMove());
				else
					stmt.setNull(++counter, Types.INTEGER);
				
				stmt.setInt(++counter, oosItem.getOOSCriteriaId());
				
				stmt.setInt(++counter, oosItem.getProductLevelId());
				stmt.setInt(++counter, oosItem.getProductId());
				stmt.setInt(++counter, oosItem.getRetLirId());
				stmt.setLong(++counter, oosItem.getWeeklyPredictedMovement());
				if (oosItem.getOOSCriteriaData().getTrxCntOfPrevDaysOfItemOrLig() == -1) {
					stmt.setNull(++counter, Types.INTEGER);
				} else {
					stmt.setInt(++counter, oosItem.getOOSCriteriaData().getTrxCntOfPrevDaysOfItemOrLig());
				}
				if (oosItem.getOOSCriteriaData().getTrxBasedPredOfProcessingTimeSlotOfItemOrLig() == -1) {
					stmt.setNull(++counter, Types.INTEGER);
				} else {
					stmt.setLong(++counter,
							oosItem.getOOSCriteriaData().getTrxBasedPredOfProcessingTimeSlotOfItemOrLig());
				}
				stmt.setLong(++counter, oosItem.getOOSCriteriaData().getActualMovOfPrevTimeSlotOfItemOrLig());
				stmt.setLong(++counter, oosItem.getOOSCriteriaData().getForecastMovOfPrevTimeSlotOfItemOrLig());
//				stmt.setLong(++counter, oosItem.getOOSCriteriaData().getActualMovOfPrevDaySameTimeSlotOfItemOrLig());
				stmt.setLong(++counter, 0);
				stmt.setLong(++counter, oosItem.getOOSCriteriaData().getCombinedXTimeSlotLowerConfOfItemOrLig());
				stmt.setLong(++counter, oosItem.getOOSCriteriaData().getCombinedXTimeSlotForecastOfItemOrLig());
//				if(oosItem.getOOSCriteriaData().isItemMovedMoreThanXUnitsInEachXPrevTimeSlot())
//	        		stmt.setString(++counter, String.valueOf(Constants.YES));
//	        	else
//	        		stmt.setString(++counter, String.valueOf(Constants.NO));
				stmt.setString(++counter, String.valueOf(Constants.NO));
				if (oosItem.getOOSCriteriaData().getTrxCntOfProcessingTimeSlotOfStore() == -1) {
					stmt.setNull(++counter, Types.INTEGER);
				} else {
					stmt.setInt(++counter, oosItem.getOOSCriteriaData().getTrxCntOfProcessingTimeSlotOfStore());
				}

				if (oosItem.getOOSCriteriaData().getTrxCntOfPrevDaysOfStore() == -1) {
					stmt.setNull(++counter, Types.INTEGER);
				} else {
					stmt.setInt(++counter, oosItem.getOOSCriteriaData().getTrxCntOfPrevDaysOfStore());
				}

				if (oosItem.getOOSCriteriaData().getActualMovOfPrevDaysOfItemOrLig() == -1) {
					stmt.setNull(++counter, Types.INTEGER);
				} else {
					stmt.setInt(++counter, oosItem.getOOSCriteriaData().getActualMovOfPrevDaysOfItemOrLig());
				}
				stmt.setInt(++counter, oosItem.getOOSCriteriaData().getShelfCapacityOfItemOrLig());
				
				stmt.addBatch();
				insertCount++;

				if (insertCount % Constants.BATCH_UPDATE_COUNT == 0) {
					stmt.executeBatch();
					stmt.clearBatch();
					insertCount = 0;
				}
			}
			if (insertCount > 0) {
				stmt.executeBatch();
				stmt.clearBatch();
			}
		} catch (SQLException ex) {
			logger.error("Error in insertOOSItems() -- " + ex.toString(), ex);
			throw new Exception();
		} finally {
			PristineDBUtil.close(stmt);
		}
	}

	/**
	 * Get average movement of item at location level
	 * 
	 * @param conn
	 * @param locationLevelId
	 * @param locationId
	 * @return
	 * @throws Exception
	 */
	public HashMap<ProductKey, Double> getWeeklyItemAvgMovement(Connection conn, int locationLevelId, int locationId)
			throws Exception {
		PreparedStatement stmt = null;
		ResultSet rs = null;
		HashMap<ProductKey, Double> locationAvgMov = new HashMap<ProductKey, Double>();
		try {
			String query = new String(GET_WEEKLY_LEVEL_AVG_MOV);

			stmt = conn.prepareStatement(query);
			stmt.setInt(1, locationLevelId);
			stmt.setInt(2, locationId);
			stmt.setFetchSize(200000);
			rs = stmt.executeQuery();
			while (rs.next()) {
				ProductKey productKey = new ProductKey(rs.getInt("PRODUCT_LEVEL_ID"), rs.getInt("PRODUCT_ID"));
				locationAvgMov.put(productKey, rs.getDouble("WEEKLY_AVG_MOV"));
			}
		} catch (SQLException ex) {
			logger.error("Error in getWeeklyItemAvgMovement() -- " + ex.toString(), ex);
			throw new Exception();
		} finally {
			PristineDBUtil.close(rs);
			PristineDBUtil.close(stmt);
		}
		return locationAvgMov;
	}

	/**
	 * Get average movement of the day part
	 * 
	 * @param conn
	 * @param locationLevelId
	 * @param locationId
	 * @param dayId
	 * @return
	 * @throws Exception
	 */
	public HashMap<OOSDayPartItemAvgMovKey, WeeklyAvgMovement> getDayPartItemAvgMovement(Connection conn, int locationLevelId, int locationId,
			List<Integer> dayIds) throws Exception {
		PreparedStatement stmt = null;
		ResultSet rs = null;
		HashMap<OOSDayPartItemAvgMovKey, WeeklyAvgMovement> dayPartAvgMov = new HashMap<OOSDayPartItemAvgMovKey, WeeklyAvgMovement>();
		WeeklyAvgMovement weeklyAvgMovement = null;
		try {
			String query = new String(GET_DAY_PART_AVG_MOV);
			query = query.replaceAll("%DAY_IDS%", PRCommonUtil.getCommaSeperatedStringFromIntArray(dayIds));
			
			stmt = conn.prepareStatement(query);
			stmt.setInt(1, locationLevelId);
			stmt.setInt(2, locationId);
			//stmt.setInt(3, dayId);
			// stmt.setInt(4, dayPartId);
			stmt.setFetchSize(200000);
			rs = stmt.executeQuery();
			while (rs.next()) {
				OOSDayPartItemAvgMovKey oosDayPartItemAvgMovKey = new OOSDayPartItemAvgMovKey(rs.getInt("DAY_ID"), rs.getInt("DAY_PART_ID"),
						rs.getInt("PRODUCT_LEVEL_ID"), rs.getInt("PRODUCT_ID"));
				weeklyAvgMovement = new WeeklyAvgMovement();
				weeklyAvgMovement.setAverageMovement(rs.getDouble("DAY_PART_AVG_MOV"));
				dayPartAvgMov.put(oosDayPartItemAvgMovKey, weeklyAvgMovement);
			}
		} catch (SQLException ex) {
			logger.error("Error in getDayPartItemAvgMovement() -- " + ex.toString(), ex);
			throw new Exception();
		} finally {
			PristineDBUtil.close(rs);
			PristineDBUtil.close(stmt);
		}
		return dayPartAvgMov;
	}

	/**
	 * Get the entire DAY_PART_LOOKUP table
	 * 
	 * @param conn
	 * @return
	 * @throws GeneralException
	 */
	public List<DayPartLookupDTO> getDayPartLookup(Connection conn)
			throws GeneralException {
		List<DayPartLookupDTO> dayPartLookup = new ArrayList<DayPartLookupDTO>();
		PreparedStatement statement = null;
		ResultSet resultSet = null;
		try {
			statement = conn.prepareStatement(GET_DAY_PART_LOOKUP);
			resultSet = statement.executeQuery();
			while (resultSet.next()) {
				DayPartLookupDTO oosDayPartLookupDTO = new DayPartLookupDTO();
				oosDayPartLookupDTO.setDayPartId(resultSet
						.getInt("DAY_PART_ID"));
				oosDayPartLookupDTO.setStartTime(resultSet
						.getString("START_TIME"));
				oosDayPartLookupDTO.setEndTime(resultSet.getString("END_TIME"));
				oosDayPartLookupDTO.setOrder(resultSet.getInt("EXEC_ORDER"));
				if (resultSet.getString("IS_SLOT_SPAN_DAYS").equals("Y"))
					oosDayPartLookupDTO.setSlotSpanDays(true);
				else
					oosDayPartLookupDTO.setSlotSpanDays(false);
				dayPartLookup.add(oosDayPartLookupDTO);
			}
		} catch (SQLException sqlE) {
			logger.error("Error while getting day part lookup", sqlE);
			throw new GeneralException("Error while getting day part lookup",
					sqlE);
		} finally {
			PristineDBUtil.close(resultSet);
			PristineDBUtil.close(statement);
		}
		return dayPartLookup;
	}

	/***
	 * Get Details of all the items for a day part
	 * 
	 * @param conn
	 * @param storeId
	 * @param startTime
	 * @param endTime
	 * @return
	 * @throws GeneralException
	 */
	public HashMap<ProductKey, OOSItemDTO> getProcessingDayPartDetail(Connection conn,
			int storeId, Timestamp startTime, Timestamp endTime)
			throws GeneralException {
		HashMap<ProductKey, OOSItemDTO> itemAndItsDayPartDetail = new HashMap<ProductKey, OOSItemDTO>();
		PreparedStatement statement = null;
		ResultSet resultSet = null;
		// StringBuffer sql = new StringBuffer();
		try {
			String query = new String(GET_DAY_PART_DETAIL);
			statement = conn.prepareStatement(query);
			statement.setTimestamp(1, startTime);
			statement.setTimestamp(2, endTime);
			statement.setInt(3, storeId);
			statement.setTimestamp(4, startTime);
			statement.setTimestamp(5, endTime);
			statement.setFetchSize(200000);
			resultSet = statement.executeQuery();
			logger.debug("GET_DAY_PART_DETAIL:" + query);
			while (resultSet.next()) {
				int itemCode = resultSet.getInt("ITEM_ID");
				OOSItemDTO oosItemDTO = new OOSItemDTO();
				oosItemDTO.getOOSCriteriaData().setActualMovOfProcessingTimeSlotOfItemOrLig(resultSet.getLong("QUANTITY"));
				ProductKey productKey = new ProductKey(Constants.ITEMLEVELID, itemCode);
				itemAndItsDayPartDetail.put(productKey, oosItemDTO);
			}
		} catch (SQLException sqlE) {
			logger.error("Error in getActualMovementOfDayPart()", sqlE);
			throw new GeneralException("Error in getActualMovementOfDayPart()",
					sqlE);
		} finally {
			PristineDBUtil.close(resultSet);
			PristineDBUtil.close(statement);
		}
		return itemAndItsDayPartDetail;
	}
	
	public HashMap<OOSDayPartDetailKey, OOSDayPartDetail> getDayPartDetail(Connection conn, List<DayPartLookupDTO> dayPartLookup, int storeId,
			Timestamp startTime, Timestamp endTime) throws GeneralException {
		HashMap<OOSDayPartDetailKey, OOSDayPartDetail> itemAndItsDayPartDetail = new HashMap<OOSDayPartDetailKey, OOSDayPartDetail>();
		PreparedStatement statement = null;
		ResultSet resultSet = null;
		OOSService oosService = new OOSService();
		OOSDayPartDetailKey oosDayPartDetailKey;
		OOSDayPartDetail oosDayPartDetail;
		try {
			String query = new String(GET_DAY_PART_DETAIL);
			
			// Fill Day Case
			query = query.replaceAll("%DAY_CASE%", oosService.fillDayCase(dayPartLookup));
			
			query = query.replaceAll("%DAY_PART_CASE%", oosService.fillDayPartCase(dayPartLookup));

			statement = conn.prepareStatement(query);
			statement.setInt(1, storeId);
			statement.setTimestamp(2, startTime);
			statement.setTimestamp(3, endTime);
			statement.setTimestamp(4, startTime);
			statement.setTimestamp(5, endTime);
			statement.setFetchSize(200000);
			resultSet = statement.executeQuery();
			logger.debug("GET_DAY_PART_DETAIL:" + query);
			logger.debug("parameters: storeId:" + storeId + ",startTime:" + startTime + ",endTime:" + endTime);
			
			while (resultSet.next()) {
				int itemCode = resultSet.getInt("ITEM_ID");
				long movement = resultSet.getLong("QUANTITY");
				int calendarId = resultSet.getInt("CALENDAR_ID");
				int dayPartId = resultSet.getInt("DAY_PART_ID");

				oosDayPartDetailKey = new OOSDayPartDetailKey(calendarId, dayPartId, Constants.ITEMLEVELID, itemCode);
				oosDayPartDetail = new OOSDayPartDetail();
				oosDayPartDetail.setActualMovement(movement);
				//logger.debug("oosDayPartDetailKey:" + oosDayPartDetailKey.toString());
				itemAndItsDayPartDetail.put(oosDayPartDetailKey, oosDayPartDetail);
			}
		} catch (SQLException sqlE) {
			logger.error("Error in getDayPartDetail()", sqlE);
			throw new GeneralException("Error in getDayPartDetail()", sqlE);
		} finally {
			PristineDBUtil.close(resultSet);
			PristineDBUtil.close(statement);
		}
		return itemAndItsDayPartDetail;
	}

	/**
	 * Find movement of each day part in a day for each week for a item
	 * 
	 * @param conn
	 * @param dayPartLookup
	 * @param itemAndItsMovements
	 * @param noOfWeeksHistory
	 * @param storeId
	 * @param dayId
	 * @param dayPartId
	 * @param values
	 * @throws GeneralException
	 */
	public void getLastXWeeksInfo(Connection conn, List<DayPartLookupDTO> dayPartLookup, HashMap<ProductKey, List<OOSItemDTO>> itemAndItsMovements,
			int noOfWeeksHistory, int storeId, int dayId, int dayPartId, Object... values) throws GeneralException {
		PreparedStatement statement = null;
		ResultSet resultSet = null;
		OOSService oosService = new OOSService();
		int counter = 0;
		try {
			String query = new String(GET_X_WEEKS_DATA);
			// Fill Day Part Case
			query = query.replaceAll("%DAY_PART_CASE%", oosService.fillDayPartCase(dayPartLookup));

			// Fill Day Case
			query = query.replaceAll("%DAY_CASE%", oosService.fillDayCase(dayPartLookup));

			statement = conn.prepareStatement(String.format(query, PristineDBUtil.preparePlaceHolders(values.length)));

			statement.setInt(++counter, storeId);
			statement.setInt(++counter, noOfWeeksHistory);
			PristineDBUtil.setValues(statement, ++counter, values);
			counter = (counter + values.length) - 1;
			statement.setInt(++counter, noOfWeeksHistory);
			statement.setInt(++counter, dayId);
			statement.setInt(++counter, dayPartId);
			statement.setFetchSize(200000);

			logger.debug("GET_X_WEEKS_DATA Query: " + query);
			logger.debug("Query Parameters:1-" + storeId + ",2-" + noOfWeeksHistory + ",4-" + noOfWeeksHistory + ",5-" + dayId + ",6-" + dayPartId);
			resultSet = statement.executeQuery();
			while (resultSet.next()) {
				List<OOSItemDTO> movements = new ArrayList<OOSItemDTO>();
				OOSItemDTO oosItemDTO = new OOSItemDTO();
				double mov = resultSet.getDouble("MOV");
				int itemCode = resultSet.getInt("ITEM_ID");
				int calendarId = resultSet.getInt("WEEK_CALENDAR_ID");
				ProductKey productKey = new ProductKey(Constants.ITEMLEVELID, itemCode);
				oosItemDTO.setProductId(itemCode);
				oosItemDTO.setProductLevelId(Constants.ITEMLEVELID);
				oosItemDTO.setCalendarId(calendarId);
				oosItemDTO.setActualWeeklyMovOfDayPart(mov);
				if (itemAndItsMovements.get(productKey) != null) {
					movements = itemAndItsMovements.get(productKey);
				}
				if (mov > 0) {
					movements.add(oosItemDTO);
					itemAndItsMovements.put(productKey, movements);
				}
			}
		} catch (SQLException sqlE) {
			logger.error("Error in getLastXWeeksInfo()", sqlE);
			throw new GeneralException("Error in getLastXWeeksInfo()", sqlE);
		} finally {
			PristineDBUtil.close(resultSet);
			PristineDBUtil.close(statement);
		}
	}

	/**
	 * Delete records from oos_item table
	 * 
	 * @param conn
	 * @param locationLevelId
	 * @param locationId
	 * @param calendarId
	 * @param dayPartId
	 * @return
	 * @throws Exception
	 */
	public int deleteOOSItems(Connection conn, int locationLevelId,
			int locationId, int calendarId, int dayPartId) throws Exception {
		PreparedStatement stmt = null;
		int[] count = null;
		int deleteCount = 0;

		try {
			stmt = conn.prepareStatement(DELETE_OOS_ITEM);
			int counter = 0;
			stmt.setInt(++counter, locationLevelId);
			stmt.setInt(++counter, locationId);
			stmt.setInt(++counter, calendarId);
			stmt.setInt(++counter, dayPartId);
			stmt.addBatch();
			count = stmt.executeBatch();
		} catch (SQLException exception) {
			logger.error("Error in deleteOOSItems() - " + exception);
			throw new Exception();
		} finally {
			PristineDBUtil.close(stmt);
		}
		if (count != null)
			deleteCount = count.length;

		return deleteCount;
	}

	public int deleteForecastItem(Connection conn, int locationLevelId,
			int locationId, int calendarId) throws GeneralException {
		int deletedRows = 0;
		PreparedStatement stmt = null;

		try {
			stmt = conn.prepareStatement(DELETE_OOS_CANDIDATE_ITEM);
			int counter = 0;
			stmt.setInt(++counter, locationLevelId);
			stmt.setInt(++counter, locationId);
			stmt.setInt(++counter, calendarId);
			deletedRows = stmt.executeUpdate();
		} catch (SQLException sqlE) {
			logger.error("Error -- deleteForecastItem() - " + sqlE.toString());
			throw new GeneralException("Error -- deleteForecastItem()", sqlE);
		} finally {
			PristineDBUtil.close(stmt);
		}

		return deletedRows;
	}
	
	public int deleteForecastItem(Connection conn, int locationLevelId, int locationId, int productLevelId, int productId, int calendarId)
			throws GeneralException {
		int deletedRows = 0;
		PreparedStatement stmt = null;

		try {
			StringBuffer sb = new StringBuffer("DELETE FROM OOS_CANDIDATE_ITEM " + "WHERE LOCATION_LEVEL_ID = " + locationLevelId
					+ " AND LOCATION_ID = " + locationId + " AND CALENDAR_ID = " + calendarId + " AND ITEM_CODE IN ("
					+ " SELECT CHILD_PRODUCT_ID FROM (SELECT * FROM PRODUCT_GROUP_RELATION PGR " + " start with product_level_id = " + productLevelId
					+ " and product_id = " + productId
					+ " connect by  prior child_product_id = product_id  and  prior child_product_level_id = product_level_id "
					+ " ) WHERE CHILD_PRODUCT_LEVEL_ID = " + Constants.ITEMLEVELID + ")");

			PristineDBUtil.execute(conn, sb, "DELETE_OOS_CANDIDATE_ITEM");
		} catch (GeneralException ex) {
			logger.error("Error -- deleteForecastItem() - " + ex.toString());
			throw new GeneralException("Error -- deleteForecastItem()", ex);
		} finally {
			PristineDBUtil.close(stmt);
		}

		return deletedRows;
	}


	public void insertForecastItem(Connection conn, List<OOSItemDTO> oosItems) throws GeneralException {
		PreparedStatement stmt = null;
		int counter = 0;
		try {
			// CALENDAR_ID, LOCATION_LEVEL_ID, LOCATION_ID, ITEM_CODE,
			// PRESTO_FORECAST
			// AD_PAGE_NO, AD_BLOCK_NO, DISPLAY_TYPE_ID
			stmt = conn.prepareStatement(INSERT_OOS_CANDIDATE_ITEM);
			for (OOSItemDTO oosItemDTO : oosItems) {
				int colIndex = 0;
				stmt.setInt(++colIndex, oosItemDTO.getCalendarId());
				stmt.setInt(++colIndex, oosItemDTO.getLocationLevelId());
				stmt.setInt(++colIndex, oosItemDTO.getLocationId());
				stmt.setInt(++colIndex, oosItemDTO.getProductId());
				stmt.setLong(++colIndex, oosItemDTO.getWeeklyPredictedMovement());
				stmt.setLong(++colIndex, oosItemDTO.getWeeklyConfidenceLevelLower());
				stmt.setLong(++colIndex, oosItemDTO.getWeeklyConfidenceLevelUpper());
				if(oosItemDTO.getRegPrice() != null) {
					stmt.setDouble(++colIndex, oosItemDTO.getRegPrice().price);
					stmt.setInt(++colIndex, oosItemDTO.getRegPrice().multiple);
				} else {
					stmt.setDouble(++colIndex, 0);
					stmt.setInt(++colIndex, 0);
				}
				
				if (oosItemDTO.getSalePrice() == null) {
					stmt.setNull(++colIndex, Types.NULL);
					stmt.setNull(++colIndex, Types.NULL);
				} else {
					stmt.setDouble(++colIndex, oosItemDTO.getSalePrice().price);
					stmt.setInt(++colIndex, oosItemDTO.getSalePrice().multiple);
				}
				stmt.setInt(++colIndex, oosItemDTO.getAdPageNo());
				stmt.setInt(++colIndex, oosItemDTO.getBlockNo());
				stmt.setInt(++colIndex, oosItemDTO.getDisplayTypeId());
				stmt.setInt(++colIndex, oosItemDTO.getPromoTypeId());
				stmt.setInt(++colIndex, oosItemDTO.getWeeklyPredictionStatus());
				stmt.setString(++colIndex, String.valueOf(oosItemDTO.getIsOOSAnalysis()));
				stmt.setString(++colIndex, String.valueOf(oosItemDTO.getDistFlag()));
				stmt.setLong(++colIndex, oosItemDTO.getClientChainLevelWeeklyMov());
				stmt.setInt(++colIndex, oosItemDTO.getRetLirId());
				// Consider only categories for which prediction is there
				// if(oosItemDTO.getWeeklyPredictionStatus() ==
				// PredictionStatus.ERROR_MODEL_UNAVAILABLE.getStatusCode())
				// stmt.setString(++colIndex, String.valueOf(Constants.NO));
				// else
				// stmt.setString(++colIndex, String.valueOf(Constants.YES));

				stmt.addBatch();
				counter++;
				if ((counter % Constants.BATCH_UPDATE_COUNT) == 0) {
					stmt.executeBatch();
					stmt.clearBatch();
					counter = 0;
				}
			}

			if (counter > 0) {
				stmt.executeBatch();
				stmt.clearBatch();
			}
		} catch (SQLException sqlE) {
			logger.error("Error -- insertForecastItem() - " + sqlE.toString());
			throw new GeneralException("Error -- insertForecastItem()", sqlE);

		} finally {
			PristineDBUtil.close(stmt);
		}
	}
	
	public void getWeeklyMovAverageForLocation(Connection conn, int noOfWeeks, int locationLevelId, int locationId, String weekStartDate,
			HashMap<ProductKey, OOSItemDTO> movementMap) throws GeneralException {
		PreparedStatement statement = null;
		ResultSet resultSet = null;

		try {

			logger.debug("SELECT PRODUCT_ID, PRODUCT_LEVEL_ID, SUM(ROUND(TOT_MOVEMENT / " + noOfWeeks + ")) AVG_MOV FROM " + "(SELECT PRODUCT_ID, "
					+ " PRODUCT_LEVEL_ID, TOT_MOVEMENT FROM ITEM_METRIC_SUMMARY_WEEKLY TL LEFT JOIN RETAIL_CALENDAR RC ON TL.CALENDAR_ID = RC.CALENDAR_ID  "
					+ " WHERE " + " (RC.START_DATE >= TRUNC(TO_DATE('" + weekStartDate + "', '" + Constants.DB_DATE_FORMAT
					+ "') + (1 - TO_CHAR(TO_DATE('" + weekStartDate + "', '" + Constants.DB_DATE_FORMAT + "'), 'D')) - " + noOfWeeks + " * 7) "
					+ " AND  RC.START_DATE <= TRUNC(TO_DATE('" + weekStartDate + "', '" + Constants.DB_DATE_FORMAT + "') + (1 - TO_CHAR(TO_DATE('"
					+ weekStartDate + "', '" + Constants.DB_DATE_FORMAT + "'), 'D')) - 1))" + " AND LOCATION_ID = " + locationId
					+ " AND LOCATION_LEVEL_ID = " + locationLevelId + " )" + " GROUP BY PRODUCT_ID, PRODUCT_LEVEL_ID ");

			statement = conn.prepareStatement(GET_WEEKLY_AVG_MOVEMENT_AT_LOCATION);
			int colIndex = 0;
			statement.setInt(++colIndex, noOfWeeks);
			statement.setString(++colIndex, weekStartDate);
			statement.setString(++colIndex, weekStartDate);
			statement.setInt(++colIndex, noOfWeeks);
			statement.setString(++colIndex, weekStartDate);
			statement.setString(++colIndex, weekStartDate);
			statement.setInt(++colIndex, locationId);
			statement.setInt(++colIndex, locationLevelId);
			resultSet = statement.executeQuery();
			while (resultSet.next()) {
				OOSItemDTO oosItemDTO = new OOSItemDTO();
				oosItemDTO.setProductLevelId(Constants.ITEMLEVELID);
				oosItemDTO.setProductId(resultSet.getInt("PRODUCT_ID"));
				oosItemDTO.setxWeeksStoreLevelAvgMov(resultSet.getLong("AVG_MOV"));
				ProductKey productKey = new ProductKey(oosItemDTO.getProductLevelId(), oosItemDTO.getProductId());
				if (movementMap.get(productKey) != null) {
					OOSItemDTO tempDTO = movementMap.get(productKey);
					tempDTO.setxWeeksStoreLevelAvgMov(oosItemDTO.getxWeeksStoreLevelAvgMov());
					movementMap.put(productKey, tempDTO);
				}
			}
		} catch (SQLException sqlE) {
			logger.error("Error -- getWeeklyMovAverageForLocation() - " + sqlE);
			throw new GeneralException("Error -- getWeeklyMovAverageForLocation()", sqlE);
		} finally {
			PristineDBUtil.close(resultSet);
			PristineDBUtil.close(statement);
		}
	}
	
	public HashMap<ProductKey, OOSItemDTO> getWeeklyMovAverageAtChain(Connection conn, int noOfWeeks, int baseLocationLevelId, int baseLocationId,
			String weekStartDate) throws GeneralException {
		HashMap<ProductKey, OOSItemDTO> movementMap = new HashMap<ProductKey, OOSItemDTO>();
		PreparedStatement statement = null;
		ResultSet resultSet = null;
		try {

			logger.debug("SELECT PRODUCT_ID, PRODUCT_LEVEL_ID, SUM(ROUND(TOT_MOVEMENT / " + noOfWeeks + ")) AVG_MOV FROM " + "(SELECT PRODUCT_ID, "
					+ " PRODUCT_LEVEL_ID, TOT_MOVEMENT FROM ITEM_METRIC_SUMMARY_WEEKLY TL LEFT JOIN RETAIL_CALENDAR RC ON TL.CALENDAR_ID = RC.CALENDAR_ID  "
					+ " WHERE " + " (RC.START_DATE >= TRUNC(TO_DATE('" + weekStartDate + "', '" + Constants.DB_DATE_FORMAT
					+ "') + (1 - TO_CHAR(TO_DATE('" + weekStartDate + "', '" + Constants.DB_DATE_FORMAT + "'), 'D')) - " + noOfWeeks + " * 7) "
					+ " AND  RC.START_DATE <= TRUNC(TO_DATE('" + weekStartDate + "', '" + Constants.DB_DATE_FORMAT + "') + (1 - TO_CHAR(TO_DATE('"
					+ weekStartDate + "', '" + Constants.DB_DATE_FORMAT + "'), 'D')) - 1)))"
					// + " AND LOCATION_ID IN (SELECT CHILD_LOCATION_ID FROM
					// LOCATION_GROUP_RELATION "
					// + " WHERE LOCATION_LEVEL_ID = " + baseLocationLevelId + "
					// AND LOCATION_ID = " + baseLocationId + ")) "
					+ " GROUP BY PRODUCT_ID, PRODUCT_LEVEL_ID ");
			// PRODUCT_ID, PRODUCT_LEVEL_ID, SUM(ROUND(TOT_MOVEMENT / ?))
			// AVG_MOV
			String qry = String.format(GET_WEEKLY_AVG_MOVEMENT_AT_CHAIN);
			statement = conn.prepareStatement(qry);
			int colIndex = 0;
			statement.setInt(++colIndex, noOfWeeks);
			statement.setString(++colIndex, weekStartDate);
			statement.setString(++colIndex, weekStartDate);
			statement.setInt(++colIndex, noOfWeeks);
			statement.setString(++colIndex, weekStartDate);
			statement.setString(++colIndex, weekStartDate);
			statement.setInt(++colIndex, baseLocationLevelId);
			statement.setInt(++colIndex, baseLocationId);
			resultSet = statement.executeQuery();
			while (resultSet.next()) {
				OOSItemDTO oosItemDTO = new OOSItemDTO();
				oosItemDTO.setProductLevelId(Constants.ITEMLEVELID);
				oosItemDTO.setProductId(resultSet.getInt("PRODUCT_ID"));
				oosItemDTO.setxWeeksChainLevelAvgMov(resultSet.getLong("AVG_MOV"));
				ProductKey productKey = new ProductKey(oosItemDTO.getProductLevelId(), oosItemDTO.getProductId());
				movementMap.put(productKey, oosItemDTO);
			}
		} catch (SQLException sqlE) {
			logger.error("Error -- getWeeklyMovAverageAtChain() - " + sqlE);
			throw new GeneralException("Error -- getWeeklyMovAverageAtChain()", sqlE);
		} finally {
			PristineDBUtil.close(resultSet);
			PristineDBUtil.close(statement);
		}

		return movementMap;
	}
	
	public HashMap<OOSForecastItemKey, OOSItemDTO> getOOSForcastMovement(Connection conn, int locationLevelId, int locationId,
			int calendarId) throws GeneralException {
		HashMap<OOSForecastItemKey, OOSItemDTO> oosForecastMap = new HashMap<OOSForecastItemKey, OOSItemDTO>();
		PreparedStatement statement = null;
		ResultSet resultSet = null;
		OOSForecastItemKey oosForecastItemKey;
		try {
			statement = conn.prepareStatement(GET_OOS_CANDIDATE_ITEM);
			logger.debug("getOOSForcastMovement():" + GET_OOS_CANDIDATE_ITEM);
			
			int colIndex = 0;
			statement.setInt(++colIndex, locationLevelId);
			statement.setInt(++colIndex, locationId);
			statement.setInt(++colIndex, calendarId);
			statement.setInt(++colIndex, locationId);
			statement.setInt(++colIndex, calendarId);
			resultSet = statement.executeQuery();
			while (resultSet.next()) {
				// Fill to predictionDTO
				OOSItemDTO oosItemDTO = new OOSItemDTO();
				oosItemDTO.setLocationLevelId(resultSet.getInt("LOCATION_LEVEL_ID"));
				oosItemDTO.setLocationId(resultSet.getInt("LOCATION_ID"));
				oosItemDTO.setProductLevelId(Constants.ITEMLEVELID);
				oosItemDTO.setProductId(resultSet.getInt("ITEM_CODE"));
				oosItemDTO.setCalendarId(resultSet.getInt("CALENDAR_ID"));
				
				MultiplePrice regPrice = new MultiplePrice(resultSet.getInt("REG_QUANTITY"), resultSet.getDouble("REG_PRICE"));
				oosItemDTO.setRegPrice(regPrice);
				
				MultiplePrice salePrice = new MultiplePrice(resultSet.getInt("SALE_QUANTITY"), 
						(resultSet.getObject("SALE_PRICE") != null ? resultSet.getDouble("SALE_PRICE") : 0d));
				oosItemDTO.setSalePrice(salePrice);
				
				if (resultSet.getObject("AD_PAGE_NO") != null)
					oosItemDTO.setAdPageNo(resultSet.getInt("AD_PAGE_NO"));

				if (resultSet.getObject("AD_BLOCK_NO") != null)
					oosItemDTO.setBlockNo(resultSet.getInt("AD_BLOCK_NO"));
					
				if (resultSet.getObject("DISPLAY_TYPE_ID") != null)
					oosItemDTO.setDisplayTypeId(resultSet.getInt("DISPLAY_TYPE_ID"));
						
				if (resultSet.getObject("PROMO_TYPE_ID") != null)
					oosItemDTO.setPromoTypeId(resultSet.getInt("PROMO_TYPE_ID"));
				
				if (resultSet.getLong("PRESTO_FORECAST") > 0) {
					oosItemDTO.setWeeklyPredictedMovement(resultSet.getLong("PRESTO_FORECAST"));
				} else {
					oosItemDTO.setWeeklyPredictedMovement(0);
				}

				if (resultSet.getLong("CLIENT_FORECAST") > 0) {
					oosItemDTO.setClientWeeklyPredictedMovement(resultSet.getLong("CLIENT_FORECAST"));
				} else {
					oosItemDTO.setClientWeeklyPredictedMovement(0);
				}

				if (resultSet.getLong("PRESTO_CONFIDENCE_LEVEL_LOWER") > 0)
					oosItemDTO.setWeeklyConfidenceLevelLower(resultSet.getLong("PRESTO_CONFIDENCE_LEVEL_LOWER"));
				else
					oosItemDTO.setWeeklyConfidenceLevelLower(0);

				if (resultSet.getLong("PRESTO_CONFIDENCE_LEVEL_UPPER") > 0)
					oosItemDTO.setWeeklyConfidenceLevelUpper(resultSet.getLong("PRESTO_CONFIDENCE_LEVEL_UPPER"));
				else
					oosItemDTO.setWeeklyConfidenceLevelUpper(0);
				
				oosItemDTO.setWeeklyActualMovement(resultSet.getLong("TOT_MOVEMENT"));
				
				oosForecastItemKey = new OOSForecastItemKey(oosItemDTO.getLocationLevelId(),
						oosItemDTO.getLocationId(), oosItemDTO.getProductId(), oosItemDTO.getRegPrice().multiple,
						oosItemDTO.getRegPrice().price, oosItemDTO.getSalePrice().multiple,
						oosItemDTO.getSalePrice().price, oosItemDTO.getAdPageNo(), oosItemDTO.getBlockNo(),
						oosItemDTO.getPromoTypeId(), oosItemDTO.getDisplayTypeId());

				if(oosForecastMap.get(oosForecastItemKey) == null)				
					oosForecastMap.put(oosForecastItemKey, oosItemDTO);
				
			}
		} catch (SQLException sqlE) {
			logger.error("Error -- getAlreadyPredictedMovement() - " + sqlE.toString());
			throw new GeneralException("Error -- getAlreadyPredictedMovement()", sqlE);
		} finally {
			PristineDBUtil.close(resultSet);
			PristineDBUtil.close(statement);
		}

		return oosForecastMap;
	}
	
	public void updateStoreLevelClientForecast(Connection conn, List<OOSItemDTO> oosItems) throws GeneralException{
		PreparedStatement statement = null;
		try{
			statement = conn.prepareStatement(UPDATE_CLIENT_FORECAST);
			int counter = 0;
			for(OOSItemDTO oosItemDTO: oosItems){
				int colIndex = 0;
				statement.setLong(++colIndex, oosItemDTO.getClientWeeklyPredictedMovement());
				statement.setLong(++colIndex, oosItemDTO.getxWeeksChainLevelAvgMov());
				statement.setLong(++colIndex, oosItemDTO.getxWeeksStoreLevelAvgMov());
				statement.setDouble(++colIndex, oosItemDTO.getxWeeksStoreToChainPercent());
				statement.setLong(++colIndex, oosItemDTO.getClientChainLevelWeeklyMov());
				statement.setInt(++colIndex, oosItemDTO.getLocationLevelId());
				statement.setInt(++colIndex, oosItemDTO.getLocationId());
				statement.setInt(++colIndex, oosItemDTO.getCalendarId());
				statement.setInt(++colIndex, oosItemDTO.getProductId());
				counter++;
				statement.addBatch();
				if((counter % Constants.BATCH_UPDATE_COUNT) == 0){
					statement.executeBatch();
					statement.clearBatch();
					counter = 0;
				}
			}
			
			if(counter > 0){
				statement.executeBatch();
				statement.clearBatch();
				counter = 0;
			}
		}
		catch(SQLException sqlE){
			logger.error("Error -- updateStoreLevelClientForecast()" + sqlE.toString());
			throw new GeneralException("Error -- updateStoreLevelClientForecast()", sqlE);
		}
		finally{
			PristineDBUtil.close(statement);
		}
	}

	/***
	 * Get the distinct stores of the district from OOS_CANDIDATE_ITEM table
	 * @param conn
	 * @param calendarId
	 * @param districtId
	 * @return
	 * @throws GeneralException
	 */
	public List<Integer> getStoresInDistrictFromOOSForecastTable(Connection conn, int calendarId, int districtId)
			throws GeneralException {
		List<Integer> stores = new ArrayList<Integer>();
		PreparedStatement statement = null;
		ResultSet resultSet = null;
		try {
			statement = conn.prepareStatement(GET_STORES_IN_DISTRICT_OOS_CANDIDATE_ITEM);
			int colIndex = 0;
			statement.setInt(++colIndex, calendarId);
			statement.setInt(++colIndex, districtId);
			resultSet = statement.executeQuery();
			while (resultSet.next()) {
				stores.add(resultSet.getInt("LOCATION_ID"));
			}
		} catch (SQLException sqlE) {
			logger.error("Error in getStoresInDistrictFromOOSForecastTable", sqlE);
			throw new GeneralException("", sqlE);
		} finally {
			PristineDBUtil.close(resultSet);
			PristineDBUtil.close(statement);
		}
		return stores;
	}
	
	public String getDistrictName(Connection conn, int districtId)
			throws GeneralException {
		String districtName = "";
		PreparedStatement statement = null;
		ResultSet resultSet = null;
		try {
			statement = conn.prepareStatement(GET_DISTRICT_NAME);
			int colIndex = 0;
			statement.setInt(++colIndex, districtId);
			resultSet = statement.executeQuery();
			while (resultSet.next()) {
				districtName = resultSet.getString("NAME");
			}
		} catch (SQLException sqlE) {
			logger.error("Error in getDistrictName", sqlE);
			throw new GeneralException("", sqlE);
		} finally {
			PristineDBUtil.close(resultSet);
			PristineDBUtil.close(statement);
		}
		return districtName;
	}
	
	public void getTransactionDetail(Connection conn, int calendarId, int locationId, int productLevelId,
			HashMap<Integer, HashMap<ProductKey, OOSExpectationDTO>> itemLevelTransactionOfCalendar, Object... values) throws GeneralException {
		PreparedStatement statement = null;
		ResultSet resultSet = null;
		int counter = 0;
		HashMap<ProductKey, OOSExpectationDTO> itemLevelTransaction = null;

		if (itemLevelTransactionOfCalendar.get(calendarId) == null) {
			itemLevelTransaction = new HashMap<ProductKey, OOSExpectationDTO>();
		} else {
			itemLevelTransaction = itemLevelTransactionOfCalendar.get(calendarId);
		}

		try {
			String query = new String(GET_TRANSACTION_DETAIL);
			statement = conn.prepareStatement(String.format(query, PristineDBUtil.preparePlaceHolders(values.length)));

			statement.setInt(++counter, calendarId);
			statement.setInt(++counter, locationId);
			statement.setInt(++counter, productLevelId);
			PristineDBUtil.setValues(statement, ++counter, values);
			counter = (counter + values.length) - 1;
			statement.setFetchSize(200000);

			logger.debug("GET_TRANSACTION_DETAIL Query: " + query);
			logger.debug("Query Parameters:1-" + calendarId + ",2-" + locationId + ",3-" + productLevelId);
			resultSet = statement.executeQuery();
			while (resultSet.next()) {
				OOSExpectationDTO oosExpectationDTO = new OOSExpectationDTO();
				oosExpectationDTO.setStoreId(resultSet.getInt("STORE_ID"));
				oosExpectationDTO.setProductLevelId(resultSet.getInt("PRODUCT_LEVEL_ID"));
				oosExpectationDTO.setProductId(resultSet.getInt("PRODUCT_ID"));
				oosExpectationDTO.setStoreLevelTrxCount(resultSet.getInt("STORE_LEVEL_TRX_CNT"));
				oosExpectationDTO.setItemLevelTrxCount(resultSet.getInt("ITEM_LEVEL_TRX_CNT"));
				oosExpectationDTO.setItemLevelUnitsCount(resultSet.getInt("ITEM_LEVEL_UNITS"));
				ProductKey productKey = new ProductKey(oosExpectationDTO.getProductLevelId(), oosExpectationDTO.getProductId());
				itemLevelTransaction.put(productKey, oosExpectationDTO);
			}
			itemLevelTransactionOfCalendar.put(calendarId, itemLevelTransaction);
		} catch (SQLException sqlE) {
			logger.error("Error in getTransactionDetail()", sqlE);
			throw new GeneralException("Error in getTransactionDetail()", sqlE);
		} finally {
			PristineDBUtil.close(resultSet);
			PristineDBUtil.close(statement);
		}
	}
	
	public List<OOSItemDTO> getOOSCandidateItems(Connection conn, int locationLevelId, int locationId, int calendarId)
			throws GeneralException {
		List<OOSItemDTO> oosCandidateItems = new ArrayList<>();
		PreparedStatement statement = null;
		ResultSet resultSet = null;
		com.pristine.dao.offermgmt.ItemDAO itemDAO = new com.pristine.dao.offermgmt.ItemDAO();
		StringBuilder sb = new StringBuilder();
		ProductService productService = new ProductService();
		try {
			HashMap<Integer, Integer> productLevelPointer = productService.getProductLevelPointer(conn, Constants.DEPARTMENTLEVELID);

			sb.append(GET_OOS_CANDIDATE_ITEMS);
			sb.append(" WHERE ");
			sb.append(" OOS_F.LOCATION_LEVEL_ID = " + locationLevelId);
			sb.append(" AND OOS_F.LOCATION_ID = " + locationId);
			sb.append(" AND OOS_F.CALENDAR_ID = " + calendarId);
			sb.append(" AND OOS_F.IS_OOS_ANALYSIS = '" + Constants.YES + "' ");
			sb.append("  ) OOS_ITEM JOIN ITEM_LOOKUP IL ON IL.ITEM_CODE = OOS_ITEM.ITEM_CODE ");
			sb.append(" LEFT JOIN ITEM_METRIC_SUMMARY_WEEKLY IMS ");
			sb.append(" ON OOS_ITEM.ITEM_CODE   = IMS.PRODUCT_ID  ");
			sb.append(" AND IMS.LOCATION_ID = " + locationId);
			sb.append(" AND IMS.CALENDAR_ID = " + calendarId + " AND IMS.PRODUCT_LEVEL_ID = " + Constants.ITEMLEVELID);
			sb.append(" ) STORE_ITEM_MAP LEFT JOIN ");
			sb.append(itemDAO.getProductHierarchy(conn, Constants.DEPARTMENTLEVELID, productService));
			// sb.append(" AND PRODUCT_LEVEL_ID = " + Constants.ITEMLEVELID);
			sb.append(" LEFT JOIN PRODUCT_GROUP PG ON ");
			sb.append(" PRODUCT_HIERARCHY.P_ID_" + productLevelPointer.get(Constants.CATEGORYLEVELID) + " = PG.PRODUCT_ID AND ");
			sb.append(" PG.PRODUCT_LEVEL_ID = " + Constants.CATEGORYLEVELID);
			sb.append(" LEFT JOIN PRODUCT_GROUP PG1 ON ");
			sb.append(" PRODUCT_HIERARCHY.P_ID_" + productLevelPointer.get(Constants.DEPARTMENTLEVELID) + " = PG1.PRODUCT_ID AND ");
			sb.append(" PG1.PRODUCT_LEVEL_ID = " + Constants.DEPARTMENTLEVELID);
			sb.append(" JOIN COMPETITOR_STORE CS ON STORE_ITEM_MAP.LOCATION_ID = CS.COMP_STR_ID ");
			sb.append(" JOIN RETAIL_DISTRICT RD ON RD.ID = CS.DISTRICT_ID ");
			sb.append(" LEFT JOIN (SELECT PI.ITEM_CODE, COUNT(CAPACITY) AS NO_OF_SHELF, SUM(CAPACITY) AS SHELF_CAPACITY ");
			sb.append(" FROM PLANOGRAM_INFO PI WHERE STORE_ID = " + locationId + " GROUP BY PI.ITEM_CODE ");
			sb.append(" ) PLANOGRAM ON STORE_ITEM_MAP.ITEM_CODE = PLANOGRAM.ITEM_CODE ");

			logger.debug("Query - getOOSCandidateItems() -- " + sb.toString());
			statement = conn.prepareStatement(sb.toString());
			resultSet = statement.executeQuery();
			while (resultSet.next()) {

				OOSItemDTO oosItemDTO = new OOSItemDTO();
				oosItemDTO.setProductId(resultSet.getInt("ITEM_CODE"));
				oosItemDTO.setProductLevelId(Constants.ITEMLEVELID);
				MultiplePrice regPrice = PRCommonUtil.getMultiplePrice(resultSet.getInt("REG_QUANTITY"), resultSet.getDouble("REG_PRICE"),
						resultSet.getDouble("REG_PRICE"));
				oosItemDTO.setRegPrice(regPrice);

				MultiplePrice salePrice = PRCommonUtil.getMultiplePrice(resultSet.getInt("SALE_QUANTITY"), resultSet.getDouble("SALE_PRICE"),
						resultSet.getDouble("SALE_PRICE"));
				oosItemDTO.setSalePrice(salePrice);
				oosItemDTO.setCategoryName(resultSet.getString("CATEGORY_NAME"));
				oosItemDTO.setDepartmentName(resultSet.getString("DEPARTMENT_NAME"));
				oosItemDTO.setCatProductId(resultSet.getInt("P_ID_" + productLevelPointer.get(Constants.CATEGORYLEVELID)));
				oosItemDTO.setDeptProductId(resultSet.getInt("P_ID_" + productLevelPointer.get(Constants.DEPARTMENTLEVELID)));
				oosItemDTO.setUpc(resultSet.getString("UPC"));
				oosItemDTO.setRetailerItemCode(resultSet.getString("RETAILER_ITEM_CODE"));
				oosItemDTO.setItemName(resultSet.getString("ITEM_NAME"));
				oosItemDTO.setPromoTypeId(resultSet.getInt("PROMO_TYPE_ID"));
				oosItemDTO.setAdPageNo(resultSet.getInt("AD_PAGE_NO"));
				oosItemDTO.setBlockNo(resultSet.getInt("AD_BLOCK_NO"));
				oosItemDTO.setDisplayTypeId(resultSet.getInt("DISPLAY_TYPE_ID"));
				oosItemDTO.setStoreNo(resultSet.getString("COMP_STR_NO"));
				//21st Dec 2015, as there is variance in LIG items while running the forecast and generating the report
				//take the ret_lir_id from candidate item table itself, so that whatever data on forecast is used
				if(locationLevelId == Constants.STORE_LIST_LEVEL_ID)
					oosItemDTO.setRetLirId(resultSet.getInt("OOS_RET_LIR_ID"));
				else
					oosItemDTO.setRetLirId(resultSet.getInt("RET_LIR_ID"));					
				if (resultSet.getString("DIST_FLAG").equals(String.valueOf(Constants.DSD)))
					oosItemDTO.setDistFlag(Constants.DSD);
				else
					oosItemDTO.setDistFlag(Constants.WAREHOUSE);

				if (resultSet.getLong("PRESTO_FORECAST") > 0) {
					oosItemDTO.setWeeklyPredictedMovement(resultSet.getLong("PRESTO_FORECAST"));
				} else {
					oosItemDTO.setWeeklyPredictedMovement(0);
				}

				if (resultSet.getLong("CLIENT_FORECAST") > 0) {
					oosItemDTO.setClientWeeklyPredictedMovement(resultSet.getLong("CLIENT_FORECAST"));
				} else {
					oosItemDTO.setClientWeeklyPredictedMovement(0);
				}

				if (resultSet.getLong("PRESTO_CONFIDENCE_LEVEL_LOWER") > 0)
					oosItemDTO.setWeeklyConfidenceLevelLower(resultSet.getLong("PRESTO_CONFIDENCE_LEVEL_LOWER"));
				else
					oosItemDTO.setWeeklyConfidenceLevelLower(0);

				if (resultSet.getLong("PRESTO_CONFIDENCE_LEVEL_UPPER") > 0)
					oosItemDTO.setWeeklyConfidenceLevelUpper(resultSet.getLong("PRESTO_CONFIDENCE_LEVEL_UPPER"));
				else
					oosItemDTO.setWeeklyConfidenceLevelUpper(0);

				oosItemDTO.setWeeklyActualMovement(resultSet.getLong("TOT_MOVEMENT"));
				oosItemDTO.setDistrictName(resultSet.getString("DISTRICT_NAME"));
				oosItemDTO.setShelfCapacity(resultSet.getInt("SHELF_CAPACITY"));
				oosItemDTO.setNoOfShelfLocations(resultSet.getInt("NO_OF_SHELF"));
				oosCandidateItems.add(oosItemDTO);
			}
		} catch (SQLException sqlE) {
			logger.error("Error -- getOOSCandidateItems() - " + sqlE.toString());
			throw new GeneralException("Error -- getOOSCandidateItems()", sqlE);
		}
		return oosCandidateItems;
	}
	
	public List<OOSItemDTO> getOOSCandidateItemsForChainForecast(Connection conn, int locationLevelId, String locationIds, int calendarId,
			int topsLocationId, int guLocationId) throws GeneralException {
		List<OOSItemDTO> oosCandidateItems = new ArrayList<>();
		PreparedStatement statement = null;
		ResultSet resultSet = null;
		com.pristine.dao.offermgmt.ItemDAO itemDAO = new com.pristine.dao.offermgmt.ItemDAO();
		StringBuilder sb = new StringBuilder();
		ProductService productService = new ProductService();
		try {
			HashMap<Integer, Integer> productLevelPointer = productService.getProductLevelPointer(conn, Constants.DEPARTMENTLEVELID);

			sb.append(GET_CANDIADATE_ITEMS_WITH_CHAIN_FORECAST);
			sb.append(" WHERE ");
			sb.append(" OOS_F.LOCATION_LEVEL_ID = " + locationLevelId);
			sb.append(" AND OOS_F.LOCATION_ID IN (" + locationIds + ") ");
			sb.append(" AND OOS_F.CALENDAR_ID = " + calendarId);
			//sb.append(" AND OOS_F.IS_OOS_ANALYSIS = '" + Constants.YES + "' ");
			sb.append("  ) OOS_ITEM LEFT JOIN ITEM_LOOKUP IL ON IL.ITEM_CODE = OOS_ITEM.ITEM_CODE ");
			sb.append(" LEFT JOIN RETAILER_LIKE_ITEM_GROUP RLIG ON OOS_ITEM.RET_LIR_ID = RLIG.RET_LIR_ID ");
			//sb.append(" LEFT JOIN PM_WEEKLY_AD_DEFINITION WAD ON OOS_ITEM.CALENDAR_ID = WAD.CALENDAR_ID ");
			sb.append(" LEFT JOIN PM_WEEKLY_AD_DEFINITION WAD ON (OOS_ITEM.CALENDAR_ID = WAD.CALENDAR_ID ");
			sb.append(" AND WAD.LOCATION_LEVEL_ID = " + locationLevelId + " AND WAD.LOCATION_ID IN ( " + locationIds + " ))");
			sb.append(" LEFT JOIN PM_WEEKLY_AD_PAGE WAP ON (WAP.PAGE_NUMBER = OOS_ITEM.AD_PAGE_NO ");
			sb.append(" AND WAD.WEEKLY_AD_ID = WAP.WEEKLY_AD_ID) ");
			sb.append(" LEFT JOIN PM_WEEKLY_AD_BLOCK WAB ON (WAB.PAGE_ID = WAP.PAGE_ID ");
			sb.append(" AND WAB.BLOCK_NUMBER = OOS_ITEM.AD_BLOCK_NO ) ");
			//sb.append(" AND WAB.NO_OF_LIG_OR_NON_LIG = 1 ");
			/*sb.append(" LEFT JOIN (SELECT PRODUCT_ID, SUM(TOT_MOVEMENT) TOT_MOVEMENT_GU FROM ");
			sb.append(" ITEM_METRIC_SUMMARY_WEEKLY WHERE LOCATION_ID IN (SELECT CHILD_LOCATION_ID FROM ");
			sb.append(" LOCATION_GROUP_RELATION WHERE LOCATION_ID IN (" + guLocationId + ") ");
			sb.append(" AND LOCATION_LEVEL_ID = " + Constants.STORE_LIST_LEVEL_ID + ") ");
			sb.append(" AND LOCATION_LEVEL_ID = " + Constants.STORE_LEVEL_ID);
			sb.append(" AND CALENDAR_ID = " + calendarId + " GROUP BY PRODUCT_ID ) IMS1 ");
			sb.append(" ON IMS1.PRODUCT_ID = OOS_ITEM.ITEM_CODE ");
			sb.append(" AND OOS_ITEM.LOCATION_ID = " + guLocationId);*/
			sb.append(" LEFT JOIN (SELECT PRODUCT_ID, SUM(TOT_MOVEMENT) TOT_MOVEMENT_TOPS FROM ");
			sb.append(" ITEM_METRIC_SUMMARY_WEEKLY WHERE CALENDAR_ID = " + calendarId );
			sb.append(" AND LOCATION_LEVEL_ID = " + Constants.STORE_LEVEL_ID);
			sb.append(" AND LOCATION_ID IN (SELECT CHILD_LOCATION_ID FROM ");
			sb.append(" LOCATION_GROUP_RELATION WHERE LOCATION_ID IN (" + topsLocationId + ") ");
			sb.append(" AND LOCATION_LEVEL_ID = " + Constants.STORE_LIST_LEVEL_ID + ") ");
			sb.append("  GROUP BY PRODUCT_ID ) IMS2 ");
			sb.append(" ON IMS2.PRODUCT_ID = OOS_ITEM.ITEM_CODE ");
			sb.append(" AND OOS_ITEM.LOCATION_ID = " + topsLocationId);
			sb.append(" ) STORE_ITEM_MAP LEFT JOIN ");
			sb.append(itemDAO.getProductHierarchy(conn, Constants.DEPARTMENTLEVELID, productService));
			// sb.append(" AND PRODUCT_LEVEL_ID = " + Constants.ITEMLEVELID);
			sb.append(" LEFT JOIN PRODUCT_GROUP PG ON ");
			sb.append(" PRODUCT_HIERARCHY.P_ID_" + productLevelPointer.get(Constants.CATEGORYLEVELID) + " = PG.PRODUCT_ID AND ");
			sb.append(" PG.PRODUCT_LEVEL_ID = " + Constants.CATEGORYLEVELID);
			sb.append(" LEFT JOIN PRODUCT_GROUP PG1 ON ");
			sb.append(" PRODUCT_HIERARCHY.P_ID_" + productLevelPointer.get(Constants.DEPARTMENTLEVELID) + " = PG1.PRODUCT_ID AND ");
			sb.append(" PG1.PRODUCT_LEVEL_ID = " + Constants.DEPARTMENTLEVELID);

			logger.debug("Query - getOOSCandidateItems() -- " + sb.toString());
			statement = conn.prepareStatement(sb.toString());
			resultSet = statement.executeQuery();
			while (resultSet.next()) {

				OOSItemDTO oosItemDTO = new OOSItemDTO();
				oosItemDTO.setProductId(resultSet.getInt("ITEM_CODE"));
				oosItemDTO.setProductLevelId(Constants.ITEMLEVELID);
				oosItemDTO.setLocationLevelId(resultSet.getInt("LOCATION_LEVEL_ID"));
				oosItemDTO.setLocationId(resultSet.getInt("LOCATION_ID"));
				MultiplePrice regPrice = PRCommonUtil.getMultiplePrice(resultSet.getInt("REG_QUANTITY"), resultSet.getDouble("REG_PRICE"),
						resultSet.getDouble("REG_PRICE"));
				oosItemDTO.setRegPrice(regPrice);

				MultiplePrice salePrice = PRCommonUtil.getMultiplePrice(resultSet.getInt("SALE_QUANTITY"), resultSet.getDouble("SALE_PRICE"),
						resultSet.getDouble("SALE_PRICE"));
				oosItemDTO.setSalePrice(salePrice);
				oosItemDTO.setCategoryName(resultSet.getString("CATEGORY_NAME"));
				oosItemDTO.setDepartmentName(resultSet.getString("DEPARTMENT_NAME"));
				oosItemDTO.setCatProductId(resultSet.getInt("P_ID_" + productLevelPointer.get(Constants.CATEGORYLEVELID)));
				oosItemDTO.setDeptProductId(resultSet.getInt("P_ID_" + productLevelPointer.get(Constants.DEPARTMENTLEVELID)));
				oosItemDTO.setUpc(resultSet.getString("UPC"));
				oosItemDTO.setRetailerItemCode(resultSet.getString("RETAILER_ITEM_CODE"));
				oosItemDTO.setItemName(resultSet.getString("ITEM_NAME"));
				oosItemDTO.setPromoTypeId(resultSet.getInt("PROMO_TYPE_ID"));
				oosItemDTO.setWeeklyPredictionStatus(resultSet.getInt("PREDICTION_STATUS"));
				if (oosItemDTO.getLocationId() == topsLocationId) {
					oosItemDTO.setAdPageNo(resultSet.getInt("AD_PAGE_NO"));
					oosItemDTO.setBlockNo(resultSet.getInt("AD_BLOCK_NO"));
				} else if (oosItemDTO.getLocationId() == guLocationId) {
					oosItemDTO.setAdPageNoGU(resultSet.getInt("AD_PAGE_NO"));
					oosItemDTO.setBlockNoGU(resultSet.getInt("AD_BLOCK_NO"));
				}
				oosItemDTO.setDisplayTypeId(resultSet.getInt("DISPLAY_TYPE_ID"));

				oosItemDTO.setRetLirId(resultSet.getInt("RET_LIR_ID"));
				oosItemDTO.setLirName(resultSet.getString("RET_LIR_NAME"));
				if (resultSet.getString("DIST_FLAG").equals(String.valueOf(Constants.DSD)))
					oosItemDTO.setDistFlag(Constants.DSD);
				else
					oosItemDTO.setDistFlag(Constants.WAREHOUSE);

				if (resultSet.getLong("PRESTO_FORECAST") > 0) {
					oosItemDTO.setWeeklyPredictedMovement(resultSet.getLong("PRESTO_FORECAST"));
				} else {
					oosItemDTO.setWeeklyPredictedMovement(0);
				}

				if (topsLocationId == oosItemDTO.getLocationId()) {
					oosItemDTO.setWeeklyPredictedMovementTops(oosItemDTO.getWeeklyPredictedMovement());
				} else if (guLocationId == oosItemDTO.getLocationId()) {
					oosItemDTO.setWeeklyPredictedMovementGU(oosItemDTO.getWeeklyPredictedMovement());
				}

				if (resultSet.getLong("CLIENT_FORECAST_ACTUAL") > 0) {
					oosItemDTO.setClientWeeklyPredictedMovement(resultSet.getLong("CLIENT_FORECAST_ACTUAL"));
				} else {
					oosItemDTO.setClientWeeklyPredictedMovement(0);
				}

				if (resultSet.getLong("PRESTO_CONFIDENCE_LEVEL_LOWER") > 0)
					oosItemDTO.setWeeklyConfidenceLevelLower(resultSet.getLong("PRESTO_CONFIDENCE_LEVEL_LOWER"));
				else
					oosItemDTO.setWeeklyConfidenceLevelLower(0);

				if (resultSet.getLong("PRESTO_CONFIDENCE_LEVEL_UPPER") > 0)
					oosItemDTO.setWeeklyConfidenceLevelUpper(resultSet.getLong("PRESTO_CONFIDENCE_LEVEL_UPPER"));
				else
					oosItemDTO.setWeeklyConfidenceLevelUpper(0);

				oosItemDTO.setWeeklyActualMovement(resultSet.getLong("TOT_MOVEMENT"));

				oosItemDTO.setNoOfLigOrNonLig(resultSet.getInt("NO_OF_LIG_OR_NON_LIG"));
				oosCandidateItems.add(oosItemDTO);
			}
		} catch (SQLException sqlE) {
			logger.error("Error -- getOOSCandidateItems() - " + sqlE.toString());
			throw new GeneralException("Error -- getOOSCandidateItems()", sqlE);
		}
		return oosCandidateItems;
	}
	
	
	public List<OOSItemDTO> getOOSCandidateItemsForChainForecast(Connection conn, int weekCalendarId, int locationLevelId, int locationId,
			int productLevelId, int productId) throws GeneralException {
		List<OOSItemDTO> oosCandidateItems = new ArrayList<>();
		PreparedStatement statement = null;
		ResultSet resultSet = null;
		com.pristine.dao.offermgmt.ItemDAO itemDAO = new com.pristine.dao.offermgmt.ItemDAO();
		StringBuilder sb = new StringBuilder();
		ProductService productService = new ProductService();
		try {
			HashMap<Integer, Integer> productLevelPointer = productService.getProductLevelPointer(conn, Constants.DEPARTMENTLEVELID);

			String GET_CANDIADATE_ITEMS_WITH_CHAIN_FORECAST = " SELECT PRODUCT_HIERARCHY.*, STORE_ITEM_MAP.*, PG.NAME AS CATEGORY_NAME, "
					+ " PG1.NAME AS DEPARTMENT_NAME FROM " + " (SELECT OOS_ITEM.*, " + " NVL(TOTAL_MOVEMENT,0) AS TOT_MOVEMENT, "
					+ " IL.UPC, IL.RETAILER_ITEM_CODE, RLIG.RET_LIR_NAME, " + " IL.ITEM_NAME, WAB.NO_OF_LIG_OR_NON_LIG, "
					+ " WAB.ADJUSTED_UNITS AS CLIENT_FORECAST_ACTUAL FROM " + " (SELECT OOS_F.LOCATION_LEVEL_ID, OOS_F.LOCATION_ID, "
					+ " OOS_F.CALENDAR_ID, OOS_F.ITEM_CODE, OOS_F.PRESTO_FORECAST, "
					+ " OOS_F.REG_PRICE, OOS_F.REG_QUANTITY, OOS_F.SALE_QUANTITY, OOS_F.SALE_PRICE, "
					+ " OOS_F.AD_PAGE_NO, OOS_F.AD_BLOCK_NO, OOS_F.DISPLAY_TYPE_ID, OOS_F.PROMO_TYPE_ID, OOS_F.PRESTO_CONFIDENCE_LEVEL_LOWER, "
					+ " OOS_F.PRESTO_CONFIDENCE_LEVEL_UPPER, OOS_F.DIST_FLAG, OOS_F.RET_LIR_ID, OOS_F.PREDICTION_STATUS FROM OOS_CANDIDATE_ITEM OOS_F ";

			sb.append(GET_CANDIADATE_ITEMS_WITH_CHAIN_FORECAST);
			sb.append(" WHERE ");
			sb.append(" OOS_F.LOCATION_LEVEL_ID = " + locationLevelId);
			sb.append(" AND OOS_F.LOCATION_ID IN (" + locationId + ") ");
			sb.append(" AND OOS_F.CALENDAR_ID = " + weekCalendarId);
			//NU::27th Jul 2016, commented as many blocks are missing in the UI
			//which are reported in Accuracy report  
			//sb.append(" AND OOS_F.IS_OOS_ANALYSIS = '" + Constants.YES + "' ");
			sb.append("  ) OOS_ITEM LEFT JOIN ITEM_LOOKUP IL ON IL.ITEM_CODE = OOS_ITEM.ITEM_CODE ");
			sb.append(" LEFT JOIN RETAILER_LIKE_ITEM_GROUP RLIG ON OOS_ITEM.RET_LIR_ID = RLIG.RET_LIR_ID ");
			sb.append(" LEFT JOIN PM_WEEKLY_AD_DEFINITION WAD ON (OOS_ITEM.CALENDAR_ID = WAD.CALENDAR_ID ");
			sb.append(" AND WAD.LOCATION_LEVEL_ID = " + locationLevelId + " AND WAD.LOCATION_ID = " + locationId + " )");
			sb.append(" LEFT JOIN PM_WEEKLY_AD_PAGE WAP ON (WAP.PAGE_NUMBER = OOS_ITEM.AD_PAGE_NO ");
			sb.append(" AND WAD.WEEKLY_AD_ID = WAP.WEEKLY_AD_ID) ");
			sb.append(" LEFT JOIN PM_WEEKLY_AD_BLOCK WAB ON (WAB.PAGE_ID = WAP.PAGE_ID ");
			sb.append(" AND WAB.BLOCK_NUMBER = OOS_ITEM.AD_BLOCK_NO ) ");
			/*sb.append(" LEFT JOIN (SELECT PRODUCT_ID, SUM(TOT_MOVEMENT) TOT_MOVEMENT FROM ");
			sb.append(" ITEM_METRIC_SUMMARY_WEEKLY WHERE ");
			sb.append(" CALENDAR_ID = " + weekCalendarId );
			sb.append(" AND LOCATION_LEVEL_ID = " + Constants.STORE_LEVEL_ID);
			sb.append(" AND LOCATION_ID IN (SELECT CHILD_LOCATION_ID FROM ");
			sb.append(" LOCATION_GROUP_RELATION WHERE LOCATION_ID IN (" + locationId + ") ");
			sb.append(" AND LOCATION_LEVEL_ID = " + Constants.STORE_LIST_LEVEL_ID + ") ");
			sb.append(" GROUP BY PRODUCT_ID ) IMS ");*/
			//sb.append(" LEFT JOIN (SELECT ITEM_CODE, TOTAL_MOVEMENT FROM ");
			//sb.append(" IMS_STORELIST_MV WHERE ");
			sb.append(" LEFT JOIN (SELECT PRODUCT_ID AS ITEM_CODE, TOT_MOVEMENT AS TOTAL_MOVEMENT  FROM ");
			sb.append(" SYNONYM_IMS_WEEKLY_CHAIN WHERE ");
			sb.append(" CALENDAR_ID = " + weekCalendarId );
			sb.append(" AND LOCATION_LEVEL_ID = " + locationLevelId);
			sb.append(" AND LOCATION_ID = " +  locationId );
			sb.append(" ) IMS ");
			sb.append(" ON IMS.ITEM_CODE = OOS_ITEM.ITEM_CODE ");
			sb.append(" AND OOS_ITEM.LOCATION_ID = " + locationId);
			sb.append(" ) STORE_ITEM_MAP LEFT JOIN ");
			sb.append(itemDAO.getProductHierarchy(conn, Constants.DEPARTMENTLEVELID, productService));
			// sb.append(" AND PRODUCT_LEVEL_ID = " + Constants.ITEMLEVELID);
			sb.append(" LEFT JOIN PRODUCT_GROUP PG ON ");
			sb.append(" PRODUCT_HIERARCHY.P_ID_" + productLevelPointer.get(Constants.CATEGORYLEVELID) + " = PG.PRODUCT_ID AND ");
			sb.append(" PG.PRODUCT_LEVEL_ID = " + Constants.CATEGORYLEVELID);
			sb.append(" LEFT JOIN PRODUCT_GROUP PG1 ON ");
			sb.append(" PRODUCT_HIERARCHY.P_ID_" + productLevelPointer.get(Constants.DEPARTMENTLEVELID) + " = PG1.PRODUCT_ID AND ");
			sb.append(" PG1.PRODUCT_LEVEL_ID = " + Constants.DEPARTMENTLEVELID);

			logger.debug("Query - getOOSCandidateItems() -- " + sb.toString());
			statement = conn.prepareStatement(sb.toString());
			resultSet = statement.executeQuery();
			while (resultSet.next()) {

				OOSItemDTO oosItemDTO = new OOSItemDTO();
				oosItemDTO.setProductId(resultSet.getInt("ITEM_CODE"));
				oosItemDTO.setProductLevelId(Constants.ITEMLEVELID);
				oosItemDTO.setLocationLevelId(resultSet.getInt("LOCATION_LEVEL_ID"));
				oosItemDTO.setLocationId(resultSet.getInt("LOCATION_ID"));
//				MultiplePrice regPrice = PRCommonUtil.getMultiplePrice(resultSet.getInt("REG_QUANTITY"), resultSet.getDouble("REG_PRICE"),
//						resultSet.getDouble("REG_PRICE"));
				MultiplePrice regPrice = new MultiplePrice(resultSet.getInt("REG_QUANTITY"), resultSet.getDouble("REG_PRICE"));
				oosItemDTO.setRegPrice(regPrice);

//				MultiplePrice salePrice = PRCommonUtil.getMultiplePrice(resultSet.getInt("SALE_QUANTITY"), resultSet.getDouble("SALE_PRICE"),
//						resultSet.getDouble("SALE_PRICE"));
				MultiplePrice salePrice = new MultiplePrice(resultSet.getInt("SALE_QUANTITY"), resultSet.getDouble("SALE_PRICE"));
				oosItemDTO.setSalePrice(salePrice);
				oosItemDTO.setCategoryName(resultSet.getString("CATEGORY_NAME"));
				oosItemDTO.setDepartmentName(resultSet.getString("DEPARTMENT_NAME"));
				oosItemDTO.setCatProductId(resultSet.getInt("P_ID_" + productLevelPointer.get(Constants.CATEGORYLEVELID)));
				oosItemDTO.setDeptProductId(resultSet.getInt("P_ID_" + productLevelPointer.get(Constants.DEPARTMENTLEVELID)));
				oosItemDTO.setUpc(resultSet.getString("UPC"));
				oosItemDTO.setRetailerItemCode(resultSet.getString("RETAILER_ITEM_CODE"));
				oosItemDTO.setItemName(resultSet.getString("ITEM_NAME"));
				oosItemDTO.setPromoTypeId(resultSet.getInt("PROMO_TYPE_ID"));
				oosItemDTO.setWeeklyPredictionStatus(resultSet.getInt("PREDICTION_STATUS"));
				oosItemDTO.setAdPageNo(resultSet.getInt("AD_PAGE_NO"));
				oosItemDTO.setBlockNo(resultSet.getInt("AD_BLOCK_NO"));
				oosItemDTO.setDisplayTypeId(resultSet.getInt("DISPLAY_TYPE_ID"));

				oosItemDTO.setRetLirId(resultSet.getInt("RET_LIR_ID"));
				oosItemDTO.setLirName(resultSet.getString("RET_LIR_NAME"));
				if (resultSet.getString("DIST_FLAG").equals(String.valueOf(Constants.DSD)))
					oosItemDTO.setDistFlag(Constants.DSD);
				else
					oosItemDTO.setDistFlag(Constants.WAREHOUSE);

				if (resultSet.getLong("PRESTO_FORECAST") > 0) {
					oosItemDTO.setWeeklyPredictedMovement(resultSet.getLong("PRESTO_FORECAST"));
				} else {
					oosItemDTO.setWeeklyPredictedMovement(0);
				}

				if (resultSet.getLong("CLIENT_FORECAST_ACTUAL") > 0) {
					oosItemDTO.setClientWeeklyPredictedMovement(resultSet.getLong("CLIENT_FORECAST_ACTUAL"));
				} else {
					oosItemDTO.setClientWeeklyPredictedMovement(0);
				}
				
				oosItemDTO.setNoOfLigOrNonLig(resultSet.getInt("NO_OF_LIG_OR_NON_LIG"));

				if (resultSet.getLong("PRESTO_CONFIDENCE_LEVEL_LOWER") > 0)
					oosItemDTO.setWeeklyConfidenceLevelLower(resultSet.getLong("PRESTO_CONFIDENCE_LEVEL_LOWER"));
				else
					oosItemDTO.setWeeklyConfidenceLevelLower(0);

				if (resultSet.getLong("PRESTO_CONFIDENCE_LEVEL_UPPER") > 0)
					oosItemDTO.setWeeklyConfidenceLevelUpper(resultSet.getLong("PRESTO_CONFIDENCE_LEVEL_UPPER"));
				else
					oosItemDTO.setWeeklyConfidenceLevelUpper(0);

				oosItemDTO.setWeeklyActualMovement(resultSet.getLong("TOT_MOVEMENT"));

				if (productId == 0) {
					oosCandidateItems.add(oosItemDTO);
				} else {
					if (productId == oosItemDTO.getCatProductId())
						oosCandidateItems.add(oosItemDTO);
				}
			}
		} catch (SQLException sqlE) {
			logger.error("Error -- getOOSCandidateItems() - " + sqlE.toString());
			throw new GeneralException("Error -- getOOSCandidateItems()", sqlE);
		}
		return oosCandidateItems;
	}
	
	public List<OOSItemDTO> getAdItemsForChainLevelForecast(Connection conn, List<Integer> storeIds, int calendarId)
			throws GeneralException {
		List<OOSItemDTO> oosCandidateItems = new ArrayList<>();
		PreparedStatement statement = null;
		ResultSet resultSet = null;
		com.pristine.dao.offermgmt.ItemDAO itemDAO = new com.pristine.dao.offermgmt.ItemDAO();
		StringBuilder sb = new StringBuilder();
		ProductService productService = new ProductService();
		try {
			HashMap<Integer, Integer> productLevelPointer = productService.getProductLevelPointer(conn, Constants.DEPARTMENTLEVELID);

			sb.append(GET_AD_ITEM_CHAIN_FORECAST);
			sb.append(" WHERE ");
			sb.append(" OOS_F.CALENDAR_ID = " + calendarId);
			sb.append(" AND OOS_F.IS_OOS_ANALYSIS = '" + Constants.YES + "' ");
			sb.append("  ) OOS_ITEM JOIN ITEM_LOOKUP IL ON IL.ITEM_CODE = OOS_ITEM.ITEM_CODE ");
			sb.append(" ) STORE_ITEM_MAP LEFT JOIN ");
			sb.append(itemDAO.getProductHierarchy(conn, Constants.DEPARTMENTLEVELID, productService));
			sb.append(" LEFT JOIN PRODUCT_GROUP PG ON ");
			sb.append(" PRODUCT_HIERARCHY.P_ID_" + productLevelPointer.get(Constants.CATEGORYLEVELID) + " = PG.PRODUCT_ID AND ");
			sb.append(" PG.PRODUCT_LEVEL_ID = " + Constants.CATEGORYLEVELID);
			sb.append(" LEFT JOIN PRODUCT_GROUP PG1 ON ");
			sb.append(" PRODUCT_HIERARCHY.P_ID_" + productLevelPointer.get(Constants.DEPARTMENTLEVELID) + " = PG1.PRODUCT_ID AND ");
			sb.append(" PG1.PRODUCT_LEVEL_ID = " + Constants.DEPARTMENTLEVELID);
			sb.append(" JOIN COMPETITOR_STORE CS ON STORE_ITEM_MAP.LOCATION_ID = CS.COMP_STR_ID ");
			sb.append(" JOIN RETAIL_DISTRICT RD ON RD.ID = CS.DISTRICT_ID ");

			logger.debug("Query - getOOSCandidateItems() -- " + sb.toString());
			statement = conn.prepareStatement(sb.toString());
			resultSet = statement.executeQuery();
			while (resultSet.next()) {

				OOSItemDTO oosItemDTO = new OOSItemDTO();
				oosItemDTO.setProductId(resultSet.getInt("ITEM_CODE"));
				oosItemDTO.setProductLevelId(Constants.ITEMLEVELID);
				MultiplePrice regPrice = PRCommonUtil.getMultiplePrice(resultSet.getInt("REG_QUANTITY"), resultSet.getDouble("REG_PRICE"),
						resultSet.getDouble("REG_PRICE"));
				oosItemDTO.setRegPrice(regPrice);

				MultiplePrice salePrice = PRCommonUtil.getMultiplePrice(resultSet.getInt("SALE_QUANTITY"), resultSet.getDouble("SALE_PRICE"),
						resultSet.getDouble("SALE_PRICE"));
				oosItemDTO.setSalePrice(salePrice);
				oosItemDTO.setCategoryName(resultSet.getString("CATEGORY_NAME"));
				oosItemDTO.setDepartmentName(resultSet.getString("DEPARTMENT_NAME"));
				oosItemDTO.setCatProductId(resultSet.getInt("P_ID_" + productLevelPointer.get(Constants.CATEGORYLEVELID)));
				oosItemDTO.setDeptProductId(resultSet.getInt("P_ID_" + productLevelPointer.get(Constants.DEPARTMENTLEVELID)));
				oosItemDTO.setUpc(resultSet.getString("UPC"));
				oosItemDTO.setRetailerItemCode(resultSet.getString("RETAILER_ITEM_CODE"));
				oosItemDTO.setItemName(resultSet.getString("ITEM_NAME"));
				oosItemDTO.setPromoTypeId(resultSet.getInt("PROMO_TYPE_ID"));
				oosItemDTO.setAdPageNo(resultSet.getInt("AD_PAGE_NO"));
				oosItemDTO.setBlockNo(resultSet.getInt("AD_BLOCK_NO"));
				oosItemDTO.setDisplayTypeId(resultSet.getInt("DISPLAY_TYPE_ID"));
				oosItemDTO.setStoreNo(resultSet.getString("COMP_STR_NO"));
				oosItemDTO.setRetLirId(resultSet.getInt("RET_LIR_ID"));

				if (resultSet.getLong("PRESTO_FORECAST") > 0) {
					oosItemDTO.setWeeklyPredictedMovement(resultSet.getLong("PRESTO_FORECAST"));
				} else {
					oosItemDTO.setWeeklyPredictedMovement(0);
				}

				if (resultSet.getLong("CLIENT_FORECAST") > 0) {
					oosItemDTO.setClientWeeklyPredictedMovement(resultSet.getLong("CLIENT_FORECAST"));
				} else {
					oosItemDTO.setClientWeeklyPredictedMovement(0);
				}

				if (resultSet.getLong("PRESTO_CONFIDENCE_LEVEL_LOWER") > 0)
					oosItemDTO.setWeeklyConfidenceLevelLower(resultSet.getLong("PRESTO_CONFIDENCE_LEVEL_LOWER"));
				else
					oosItemDTO.setWeeklyConfidenceLevelLower(0);

				if (resultSet.getLong("PRESTO_CONFIDENCE_LEVEL_UPPER") > 0)
					oosItemDTO.setWeeklyConfidenceLevelUpper(resultSet.getLong("PRESTO_CONFIDENCE_LEVEL_UPPER"));
				else
					oosItemDTO.setWeeklyConfidenceLevelUpper(0);

				oosItemDTO.setDistrictName(resultSet.getString("DISTRICT_NAME"));
				oosCandidateItems.add(oosItemDTO);
			}
		} catch (SQLException sqlE) {
			logger.error("Error -- getOOSCandidateItems() - " + sqlE.toString());
			throw new GeneralException("Error -- getOOSCandidateItems()", sqlE);
		}
		return oosCandidateItems;
	}

	public HashMap<Integer, Integer> getNoOfStoresInPriceZone(Connection conn) throws GeneralException {
		PreparedStatement statement = null;
		ResultSet resultSet = null;
		HashMap<Integer, Integer> zoneStoreCount = new HashMap<Integer, Integer>();
		try {
			statement = conn.prepareStatement(GET_ZONE_STORE_COUNT);
			statement.setFetchSize(200000);
			resultSet = statement.executeQuery();
			logger.debug("statement is executed");
			while (resultSet.next()) {
				zoneStoreCount.put(resultSet.getInt("PRICE_ZONE_ID"), resultSet.getInt("NO_OF_STORES"));
			}
		} catch (SQLException SqlE) {
			throw new GeneralException("getNoOfStoresInPriceZone() - Error while retreiving promotions ", SqlE);
		} finally {
			PristineDBUtil.close(statement);
			PristineDBUtil.close(resultSet);
		}
		return zoneStoreCount;
	}
}
