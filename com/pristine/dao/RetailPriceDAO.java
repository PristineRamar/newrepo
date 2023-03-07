/*
 * Title: DAO class for Retail Price Setup
 *
 *******************************************************************************
 * Modification History
 *------------------------------------------------------------------------------
 * Version		Date		Name			Remarks
 *------------------------------------------------------------------------------
 * Version 0.1	04/03/2012	Janani			Initial Version 
 * Version 0.2	08/20/2012	Janani			Included an overloaded method
 * 											for mapItemsWithStore()
 *******************************************************************************
 */
package com.pristine.dao;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Types;
import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

import javax.sql.rowset.CachedRowSet;

import org.apache.log4j.Logger;

import com.pristine.dto.ItemDetailKey;
import com.pristine.dto.PriceAndCostDTO;
import com.pristine.dto.PriceZoneDTO;
import com.pristine.dto.RetailPriceDTO;
import com.pristine.exception.GeneralException;
import com.pristine.util.Constants;
import com.pristine.util.PrestoUtil;
import com.pristine.util.PristineDBUtil;
import com.pristine.util.PropertyManager;
import com.sun.rowset.CachedRowSetImpl;
@SuppressWarnings("resource")
public class RetailPriceDAO {
	
	static Logger logger = Logger.getLogger("RetailPriceDAO");
	
	private final int commitCount = Constants.LIMIT_COUNT;
	private static final String GET_ITEM_CODE = "SELECT ITEM_CODE, UPC FROM ITEM_LOOKUP WHERE UPC IN (%s)";
	private static final String GET_STORE_INFO = "SELECT RPZ.ZONE_NUM, CS.COMP_STR_NO " +
														"FROM COMPETITOR_STORE CS " +
														"INNER JOIN RETAIL_PRICE_ZONE RPZ ON CS.PRICE_ZONE_ID = RPZ.PRICE_ZONE_ID " +
														"WHERE CS.COMP_STR_NO IN (%s) ";
	private static final String GET_SINGLE_STORE_INFO = "SELECT RPZ.ZONE_NUM, CS.COMP_STR_NO " +
														"FROM COMPETITOR_STORE CS " +
														"INNER JOIN RETAIL_PRICE_ZONE RPZ ON CS.PRICE_ZONE_ID = RPZ.PRICE_ZONE_ID " +
														"WHERE CS.COMP_STR_NO = ?";
	private static final String GET_RETAIL_PRICE_INFO = "SELECT ITEM_CODE, CALENDAR_ID, LEVEL_TYPE_ID, LEVEL_ID, REG_PRICE, REG_QTY, REG_M_PRICE, " +
														"SALE_PRICE, SALE_QTY, SALE_M_PRICE, PROMOTION_FLG, " +
														"TO_CHAR(EFF_REG_START_DATE,'MM/dd/yyyy') EFF_REG_START_DATE, TO_CHAR(EFF_SALE_START_DATE,'MM/dd/yyyy') EFF_SALE_START_DATE, " +
														"TO_CHAR(EFF_SALE_END_DATE,'MM/dd/yyyy') EFF_SALE_END_DATE " +
														"FROM RETAIL_PRICE_INFO WHERE CALENDAR_ID = ? AND ITEM_CODE IN (%s) ORDER BY ITEM_CODE ASC, LEVEL_TYPE_ID DESC";
	private static final String SAVE_RETAIL_PRICE_INFO = "INSERT INTO RETAIL_PRICE_INFO (CALENDAR_ID, ITEM_CODE, LEVEL_TYPE_ID, LEVEL_ID, REG_PRICE, REG_QTY, REG_M_PRICE, " +
			 											 "SALE_PRICE, SALE_QTY, SALE_M_PRICE, PROMOTION_FLG, UPDATE_TIMESTAMP, EFF_REG_START_DATE, EFF_SALE_START_DATE, EFF_SALE_END_DATE, LOCATION_ID, IS_WHSE_MAPPED) VALUES " +
			 											 "(?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, TRUNC(SYSDATE), TO_DATE(?,'MM/dd/yy'), TO_DATE(?,'MM/dd/yy'), TO_DATE(?,'MM/dd/yy'), ?, ?)";
	private static final String GET_RETAIL_PRICE_INFO_5WK = "SELECT ITEM_CODE, CALENDAR_ID, LEVEL_TYPE_ID, LEVEL_ID, REG_PRICE, REG_QTY, REG_M_PRICE, " +
														"SALE_PRICE, SALE_QTY, SALE_M_PRICE, PROMOTION_FLG, " +
														"TO_CHAR(EFF_REG_START_DATE,'MM/dd/yyyy') EFF_REG_START_DATE, TO_CHAR(EFF_SALE_START_DATE,'MM/dd/yyyy') EFF_SALE_START_DATE, " +
														"TO_CHAR(EFF_SALE_END_DATE,'MM/dd/yyyy') EFF_SALE_END_DATE " +
														"FROM RETAIL_PRICE_INFO_5WK WHERE CALENDAR_ID = ? AND ITEM_CODE IN (%ITEM_CODE_QUERY%)";
	private static final String DELETE_RETAIL_PRICE_INFO = "DELETE FROM RETAIL_PRICE_INFO WHERE CALENDAR_ID = ? AND ITEM_CODE = ? AND LEVEL_TYPE_ID=? AND LEVEL_ID=?";
	private static final String UPDATE_RETAIL_PRICE_INFO = "UPDATE RETAIL_PRICE_INFO SET REG_PRICE = ?, REG_QTY = ?, REG_M_PRICE = ?, " +
			   											   "SALE_PRICE = ?, SALE_QTY = ?, SALE_M_PRICE = ?, PROMOTION_FLG = ?, UPDATE_TIMESTAMP = TRUNC(SYSDATE), " +
			   											   "EFF_REG_START_DATE = TO_DATE(?,'MM/dd/yy'), EFF_SALE_START_DATE = TO_DATE(?,'MM/dd/yy'), EFF_SALE_END_DATE = TO_DATE(?,'MM/dd/yy'), LOCATION_ID = ?, " +
			   											   "IS_WHSE_MAPPED = ? WHERE CALENDAR_ID = ? AND ITEM_CODE = ? AND LEVEL_TYPE_ID=? AND LEVEL_ID=?";
	private static final String GET_CHAINID = "SELECT COMP_CHAIN_ID FROM COMPETITOR_CHAIN WHERE PRESTO_SUBSCRIBER = 'Y'";
	private static final String GET_LATEST_RETAIL_PRICE_INFO = "SELECT ITEM_CODE, CALENDAR_ID, LEVEL_TYPE_ID, LEVEL_ID, REG_PRICE, REG_QTY, REG_M_PRICE, " +
															   "SALE_PRICE, SALE_QTY, SALE_M_PRICE, PROMOTION_FLG, " +
															   "TO_CHAR(EFF_REG_START_DATE,'MM/dd/yyyy') EFF_REG_START_DATE, TO_CHAR(EFF_SALE_START_DATE,'MM/dd/yyyy') EFF_SALE_START_DATE, " +
															   "TO_CHAR(EFF_SALE_END_DATE,'MM/dd/yyyy') EFF_SALE_END_DATE " +
															   "FROM RETAIL_PRICE_INFO WHERE (CALENDAR_ID, ITEM_CODE) IN " +
															   "(SELECT MAX(CALENDAR_ID) CALENDAR_ID, ITEM_CODE FROM RETAIL_PRICE_INFO " +
															   "WHERE CALENDAR_ID <= ? AND ITEM_CODE IN (%s) GROUP BY ITEM_CODE) ORDER BY ITEM_CODE ASC, LEVEL_TYPE_ID DESC";
	private static final String GET_RETAIL_PRICE_INFO_FROM_HISTORY = "SELECT ITEM_CODE, CALENDAR_ID, LEVEL_TYPE_ID, LEVEL_ID, REG_PRICE, REG_QTY, REG_M_PRICE, " +
															   "SALE_PRICE, SALE_QTY, SALE_M_PRICE, PROMOTION_FLG, " +
															   "TO_CHAR(EFF_REG_START_DATE,'MM/dd/yyyy') EFF_REG_START_DATE, TO_CHAR(EFF_SALE_START_DATE,'MM/dd/yyyy') EFF_SALE_START_DATE, " +
															   "TO_CHAR(EFF_SALE_END_DATE,'MM/dd/yyyy') EFF_SALE_END_DATE " +
															   "FROM RETAIL_PRICE_INFO WHERE (CALENDAR_ID, ITEM_CODE) IN " +
															   "(SELECT MAX(CALENDAR_ID) CALENDAR_ID, ITEM_CODE FROM RETAIL_PRICE_INFO " +
															   "WHERE CALENDAR_ID IN (%c) AND ITEM_CODE IN (%s) GROUP BY ITEM_CODE) ORDER BY ITEM_CODE ASC, LEVEL_TYPE_ID DESC";
/*	private static final String MERGE_INTO_STORE_ITEM_MAP = "MERGE INTO STORE_ITEM_MAP STM USING " +
															"(SELECT ? LEVEL_TYPE_ID, ? LEVEL_ID, ? ITEM_CODE FROM DUAL) D " +
															"ON (STM.LEVEL_TYPE_ID = D.LEVEL_TYPE_ID AND STM.LEVEL_ID = D.LEVEL_ID AND STM.ITEM_CODE = D.ITEM_CODE) " +
															"WHEN MATCHED THEN UPDATE SET STM.UPDATE_TIMESTAMP = SYSDATE " +
															"WHEN NOT MATCHED THEN INSERT (STM.LEVEL_TYPE_ID, STM.LEVEL_ID, STM.ITEM_CODE, STM.UPDATE_TIMESTAMP) " +
															"VALUES (D.LEVEL_TYPE_ID, D.LEVEL_ID, D.ITEM_CODE, SYSDATE)";*/
	
	private static final String MERGE_INTO_STORE_ITEM_MAP = "MERGE INTO STORE_ITEM_MAP STM USING " +
			"(SELECT ? LEVEL_TYPE_ID, ? LEVEL_ID, ? ITEM_CODE, ? PRICE_INDICATOR, ? DIST_FLAG, ? VENDOR_ID, ? IS_AUTHORIZED FROM DUAL) D " +
			"ON (STM.LEVEL_TYPE_ID = D.LEVEL_TYPE_ID AND STM.LEVEL_ID = D.LEVEL_ID AND STM.ITEM_CODE = D.ITEM_CODE ) " +
			"WHEN MATCHED THEN UPDATE SET STM.UPDATE_TIMESTAMP = SYSDATE, STM.PRICE_UPDATE_TIMESTAMP = SYSDATE, STM.DIST_FLAG = D.DIST_FLAG, " + 
			"STM.VENDOR_ID = D.VENDOR_ID, STM.PRICE_INDICATOR = D.PRICE_INDICATOR, STM.IS_AUTHORIZED = D.IS_AUTHORIZED " +
			"WHEN NOT MATCHED THEN INSERT (STM.LEVEL_TYPE_ID, STM.LEVEL_ID, STM.ITEM_CODE, " + 
			"STM.UPDATE_TIMESTAMP, STM.PRICE_UPDATE_TIMESTAMP, STM.PRICE_INDICATOR, STM.DIST_FLAG, STM.VENDOR_ID, STM.IS_AUTHORIZED) " +
			"VALUES (D.LEVEL_TYPE_ID, D.LEVEL_ID, D.ITEM_CODE, SYSDATE, SYSDATE, D.PRICE_INDICATOR, D.DIST_FLAG, D.VENDOR_ID, D.IS_AUTHORIZED)";
	
	
	private static final String MERGE_INTO_STORE_ITEM_MAP_WITH_PRC_ZONE = "MERGE INTO STORE_ITEM_MAP STM USING " +
			"(SELECT ? LEVEL_TYPE_ID, ? LEVEL_ID, ? ITEM_CODE, ? PRICE_INDICATOR, ? DIST_FLAG, ? VENDOR_ID, ? PRICE_ZONE_ID FROM DUAL) D " +
			"ON (STM.LEVEL_TYPE_ID = D.LEVEL_TYPE_ID AND STM.LEVEL_ID = D.LEVEL_ID AND STM.ITEM_CODE = D.ITEM_CODE " + 
			"AND STM.PRICE_ZONE_ID = D.PRICE_ZONE_ID) " +
			"WHEN MATCHED THEN UPDATE SET STM.UPDATE_TIMESTAMP = SYSDATE, STM.PRICE_UPDATE_TIMESTAMP = SYSDATE, STM.DIST_FLAG = D.DIST_FLAG, " + 
			"STM.VENDOR_ID = D.VENDOR_ID, STM.PRICE_INDICATOR = D.PRICE_INDICATOR " +
			"WHEN NOT MATCHED THEN INSERT (STM.LEVEL_TYPE_ID, STM.LEVEL_ID, STM.ITEM_CODE, " + 
			"STM.UPDATE_TIMESTAMP, STM.PRICE_UPDATE_TIMESTAMP, STM.PRICE_INDICATOR, STM.DIST_FLAG, STM.VENDOR_ID, STM.PRICE_ZONE_ID) " +
			"VALUES (D.LEVEL_TYPE_ID, D.LEVEL_ID, D.ITEM_CODE, SYSDATE, SYSDATE, D.PRICE_INDICATOR, D.DIST_FLAG, D.VENDOR_ID, D.PRICE_ZONE_ID)";
	
	
	private static final String GET_DEPT_ZONE_INFO = "SELECT COMP_STR_NO, DEPT1_ZONE_NO, DEPT2_ZONE_NO, DEPT3_ZONE_NO FROM COMPETITOR_STORE WHERE " +
													 "COMP_CHAIN_ID = ? AND PI_AVAILABILITY = 'Y'";
	
	private static final String DELETE_RETAIL_PRICE_INFO_WEEKLY_ITEMS = "DELETE FROM RETAIL_PRICE_INFO WHERE CALENDAR_ID = ? AND ITEM_CODE = ?";
	
	private static final String DELETE_RETAIL_PRICE_INFO_WEEKLY = "DELETE FROM RETAIL_PRICE_INFO WHERE CALENDAR_ID = ?";
	
	private static final String SETUP_RETAIL_PRICE_DATA = "INSERT INTO RETAIL_PRICE_INFO (CALENDAR_ID, ITEM_CODE, LEVEL_TYPE_ID, LEVEL_ID, REG_PRICE, REG_QTY, REG_M_PRICE, SALE_PRICE, SALE_QTY, SALE_M_PRICE, " + 
														 "PROMOTION_FLG, UPDATE_TIMESTAMP, EFF_REG_START_DATE, LOCATION_ID) (SELECT ?, ITEM_CODE, LEVEL_TYPE_ID, LEVEL_ID, REG_PRICE, REG_QTY, REG_M_PRICE, 0, 0, 0, " + 
														 "PROMOTION_FLG, SYSDATE, EFF_REG_START_DATE, LOCATION_ID FROM RETAIL_PRICE_INFO WHERE CALENDAR_ID = ? )";
	
	private static final String GET_ITEM_PRICE_IN_STORE = "SELECT TO_CHAR(RC.START_DATE, 'MM/DD/YYYY') START_DATE , RP.* FROM RETAIL_PRICE_INFO RP, RETAIL_CALENDAR RC WHERE ITEM_CODE = ? " +
														   "AND RP.CALENDAR_ID <= ?	AND (LEVEL_ID = ? " +
														   "OR LEVEL_ID = (SELECT ZONE_NUM FROM COMPETITOR_STORE CS, RETAIL_PRICE_ZONE RPZ WHERE CS.COMP_STR_NO=? AND CS.PRICE_ZONE_ID = RPZ.PRICE_ZONE_ID) " +
														   "OR LEVEL_ID = (SELECT DEPT1_ZONE_NO FROM COMPETITOR_STORE CS, RETAIL_PRICE_ZONE RPZ WHERE CS.COMP_STR_NO= ? AND CS.PRICE_ZONE_ID = RPZ.PRICE_ZONE_ID) " +
														   "OR LEVEL_ID = (SELECT DEPT1_ZONE_NO FROM COMPETITOR_STORE CS, RETAIL_PRICE_ZONE RPZ WHERE CS.COMP_STR_NO= ? AND CS.PRICE_ZONE_ID = RPZ.PRICE_ZONE_ID) " +
														   "OR LEVEL_ID = (SELECT DEPT1_ZONE_NO FROM COMPETITOR_STORE CS, RETAIL_PRICE_ZONE RPZ WHERE CS.COMP_STR_NO= ? AND CS.PRICE_ZONE_ID = RPZ.PRICE_ZONE_ID) " +
														   "OR LEVEL_ID = (SELECT TO_CHAR(COMP_CHAIN_ID) FROM COMPETITOR_STORE WHERE COMP_STR_NO=?)) " +
														   "AND RP.CALENDAR_ID = RC.CALENDAR_ID " +
														   "ORDER BY RP.CALENDAR_ID DESC, LEVEL_TYPE_ID DESC";
	
	private static final String GET_STORE_ID = "SELECT COMP_STR_NO, COMP_STR_ID FROM COMPETITOR_STORE WHERE COMP_CHAIN_ID = ? AND COMP_STR_NO IN (%s)";
	private static final String GET_ZONE_INFO = "SELECT ZONE_NUM, PRICE_ZONE_ID FROM RETAIL_PRICE_ZONE";

	private static final String INSERT_RETAIL_SALE_PRICE = "INSERT INTO RETAIL_SALE_PRICE_TEMP (COMP_STR_NO, UPC, SALE_PRICE, SALE_QTY, SALE_START_DATE, SALE_END_DATE) VALUES (?, ?, ?, ?, TO_DATE(?,'MM/dd/yyyy'), TO_DATE(?,'MM/dd/yyyy'))";
	
	/*private static final String GET_RETAIL_SALE_PRICE_FOR_DEPT = "SELECT COMP_STR_NO, UPC, SALE_PRICE, SALE_QTY, TO_CHAR(SALE_START_DATE, 'MM/dd/yyyy') AS SALE_START_DATE, TO_CHAR(SALE_END_DATE, 'MM/dd/yyyy') AS SALE_END_DATE " +
																" FROM RETAIL_SALE_PRICE_TEMP WHERE UPC IN" +
																" (" +
																" SELECT UPC FROM ( ( ( ( PRODUCT_GROUP_RELATION A1" +
																" JOIN PRODUCT_GROUP_RELATION A2" +
																" ON A1.PRODUCT_ID        = A2.CHILD_PRODUCT_ID" +
																" AND A1.PRODUCT_LEVEL_ID = A2.CHILD_PRODUCT_LEVEL_ID" +
																" AND A1.CHILD_PRODUCT_LEVEL_ID = 1)" +
																" LEFT JOIN PRODUCT_GROUP_RELATION A3" +
																" ON A2.PRODUCT_ID        = A3.CHILD_PRODUCT_ID" +
																" AND A2.PRODUCT_LEVEL_ID = A3.CHILD_PRODUCT_LEVEL_ID )" +
																" LEFT JOIN PRODUCT_GROUP_RELATION A4" +
																" ON A3.PRODUCT_ID        = A4.CHILD_PRODUCT_ID" +
																" AND A3.PRODUCT_LEVEL_ID = A4.CHILD_PRODUCT_LEVEL_ID )" +
																" JOIN PRODUCT_GROUP_RELATION A5" +
																" ON A4.PRODUCT_ID                = A5.CHILD_PRODUCT_ID" +
																" AND A4.PRODUCT_LEVEL_ID         = A5.CHILD_PRODUCT_LEVEL_ID " +
																" AND A5.PRODUCT_ID = ? )," +
																" ITEM_LOOKUP IL" +
																" WHERE A1.CHILD_PRODUCT_ID = IL.ITEM_CODE" +
																" )";*/
	private static final String GET_RETAIL_SALE_PRICE_FOR_DEPT = "SELECT COMP_STR_NO, RETAIL_SALE_PRICE_TEMP.UPC, SALE_PRICE, SALE_QTY, TO_CHAR(SALE_START_DATE, 'MM/dd/yyyy') AS SALE_START_DATE,  " + 
																" TO_CHAR(SALE_END_DATE, 'MM/dd/yyyy')   AS SALE_END_DATE FROM RETAIL_SALE_PRICE_TEMP JOIN  " + 
																" (SELECT UPC FROM (SELECT CHILD_PRODUCT_ID FROM  " + 
																" (SELECT child_product_id,child_product_level_id FROM product_group_relation pgr START WITH product_level_id = 5  " + 
																" AND product_id = ?  " + 
																" CONNECT BY prior child_product_id = product_id AND prior child_product_level_id  = product_level_id  " + 
																" )WHERE child_product_level_id = 1) P LEFT JOIN ITEM_LOOKUP IL ON P.CHILD_PRODUCT_ID = IL.ITEM_CODE) UPC_SET  " + 
																" ON RETAIL_SALE_PRICE_TEMP.UPC = UPC_SET.UPC";
	/*private static final String GET_RETAIL_SALE_PRICE_FOR_DEPT = "SELECT COMP_STR_NO, UPC, SALE_PRICE, SALE_QTY, TO_CHAR(SALE_START_DATE, 'MM/dd/yyyy') AS SALE_START_DATE, TO_CHAR(SALE_END_DATE, 'MM/dd/yyyy') AS SALE_END_DATE " +
																" FROM RETAIL_SALE_PRICE_TEMP WHERE UPC IN" +
																" (" +
																" SELECT UPC FROM ITEM_LOOKUP WHERE DEPT_ID = ?" +
																" )";*/
	

	private static final String TRUNCATE_RETAIL_SALE_PRICE = "TRUNCATE TABLE RETAIL_SALE_PRICE_TEMP";
	
	private static final String GET_ITEM_CODES_FOR_STORE = "SELECT DISTINCT(ITEM_CODE) FROM (" +
												 "SELECT DISTINCT(ITEM_CODE) FROM MOVEMENT_DAILY MD, COMPETITOR_STORE CS WHERE CS.COMP_STR_ID = ? " +
												 "AND MD.COMP_STR_NO = CS.COMP_STR_NO AND MD.ITEM_CODE <> 0 AND CALENDAR_ID IN " +
												 "(SELECT CALENDAR_ID FROM RETAIL_CALENDAR WHERE START_DATE >= TO_DATE(?,'MM/DD/YYYY') AND START_DATE <= TO_DATE(?,'MM/DD/YYYY') AND ROW_TYPE = 'D') " +
												 "UNION " +
												 "SELECT DISTINCT(CD.ITEM_CODE) FROM SCHEDULE S, COMPETITIVE_DATA CD " +
												 "WHERE (S.COMP_STR_ID = (SELECT PRIMARY_COMP_STR_ID FROM STORE_SUMMARY WHERE STORE_ID=?) OR S.COMP_STR_ID = (SELECT SECONDARY_COMP_STR_ID_1 FROM STORE_SUMMARY WHERE STORE_ID=?)) " +
												 "AND S.START_DATE >= (TO_DATE(?,'MM/DD/YYYY') - INTERVAL '90' DAY) " +
												 "AND S.START_DATE <= TO_DATE(?,'MM/DD/YYYY') AND S.SCHEDULE_ID = CD.SCHEDULE_ID)";
	private static final String GET_ITEM_CODES_FOR_ZONE = "SELECT DISTINCT(ITEM_CODE) FROM MOVEMENT_DAILY WHERE COMP_STR_NO IN " + 
														 "(SELECT COMP_STR_NO FROM COMPETITOR_STORE WHERE PRICE_ZONE_ID = ?) " +
														 "AND ITEM_CODE > 0 AND CALENDAR_ID IN " +
														 "(SELECT CALENDAR_ID FROM RETAIL_CALENDAR WHERE START_DATE >= TO_DATE(?,'MM/DD/YYYY') AND START_DATE <= TO_DATE(?,'MM/DD/YYYY') AND ROW_TYPE = 'D') " +
														 "UNION SELECT DISTINCT(CD.ITEM_CODE) FROM SCHEDULE S, COMPETITIVE_DATA CD " + 
														 "WHERE (S.COMP_STR_ID = ? OR S.COMP_STR_ID = ? OR S.COMP_STR_ID = ? OR S.COMP_STR_ID = ?) " +
														 "AND S.START_DATE >= (TO_DATE(?,'MM/DD/YYYY') - INTERVAL '90' DAY) " + 
														 "AND S.START_DATE <= TO_DATE(?,'MM/DD/YYYY') AND S.SCHEDULE_ID = CD.SCHEDULE_ID";
	
	private static final String GET_ITEM_CODES_FOR_CHAIN = "SELECT DISTINCT(ITEM_CODE) FROM MOVEMENT_DAILY WHERE COMP_STR_NO IN " + 
			 "(SELECT COMP_STR_NO FROM COMPETITOR_STORE WHERE COMP_CHAIN_ID = ?) " +
			 "AND ITEM_CODE > 0 AND CALENDAR_ID IN " +
			 "(SELECT CALENDAR_ID FROM RETAIL_CALENDAR WHERE START_DATE >= TO_DATE(?,'MM/DD/YYYY') AND START_DATE <= TO_DATE(?,'MM/DD/YYYY') AND ROW_TYPE = 'D') " +
			 "UNION SELECT DISTINCT(CD.ITEM_CODE) FROM SCHEDULE S, COMPETITIVE_DATA CD " + 
			 "WHERE  S.START_DATE >= (TO_DATE(?,'MM/DD/YYYY') - INTERVAL '90' DAY) " + 
			 "AND S.START_DATE <= TO_DATE(?,'MM/DD/YYYY') AND S.SCHEDULE_ID = CD.SCHEDULE_ID";
	
	private static final String GET_DEPT_ZONE = "SELECT COMP_STR_ID, DEPT1_ZONE_NO, DEPT2_ZONE_NO, DEPT3_ZONE_NO FROM COMPETITOR_STORE WHERE " +
			 									"COMP_CHAIN_ID = ?";
	
	private static final String GET_CHAIN_LEVEL_PRICE = "SELECT REG_QTY, REG_PRICE, REG_M_PRICE, SALE_QTY, SALE_PRICE, SALE_M_PRICE FROM RETAIL_PRICE_INFO WHERE CALENDAR_ID = ? AND ITEM_CODE = ? AND LEVEL_TYPE_ID = 0";
	
	private static final String GET_DEPT_ZONE_FOR_STORES_IN_ZONE = "SELECT COMP_STR_NO, DEPT1_ZONE_NO, DEPT2_ZONE_NO, DEPT3_ZONE_NO FROM COMPETITOR_STORE WHERE COMP_CHAIN_ID = ? AND PRICE_ZONE_ID = ?";
	
	private static final String GET_STORE_ID_MAP = "SELECT COMP_STR_NO, COMP_STR_ID FROM COMPETITOR_STORE WHERE COMP_CHAIN_ID = ?";
	
	private static final String GET_RETAIL_PRICE_ZONE = "SELECT ZONE_NUM, PRICE_ZONE_ID FROM RETAIL_PRICE_ZONE";
	
	private static final String INSERT_RETAIL_PRICE_INFO = "INSERT INTO RETAIL_PRICE_INFO (CALENDAR_ID, ITEM_CODE, LEVEL_TYPE_ID, LEVEL_ID, REG_PRICE, REG_QTY, REG_M_PRICE, " +
			 "SALE_PRICE, SALE_QTY, SALE_M_PRICE, PROMOTION_FLG, UPDATE_TIMESTAMP, EFF_REG_START_DATE, EFF_SALE_START_DATE, EFF_SALE_END_DATE, LOCATION_ID, IS_WHSE_MAPPED,CORE_RETAIL,VDPR_RETAIL) VALUES " +
			 "(?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, TRUNC(SYSDATE), TO_DATE(?,'MM/dd/yy'), TO_DATE(?,'MM/dd/yy'), TO_DATE(?,'MM/dd/yy'), ?, ?,?,?)";
	
	private static final String INSERT_RETAIL_PRICE_DATA = "INSERT INTO RETAIL_PRICE_INFO (CALENDAR_ID, ITEM_CODE, LEVEL_TYPE_ID, LEVEL_ID, REG_PRICE, REG_QTY, REG_M_PRICE, SALE_PRICE, SALE_QTY, SALE_M_PRICE, " + 
			 "PROMOTION_FLG, UPDATE_TIMESTAMP, EFF_REG_START_DATE, LOCATION_ID, CORE_RETAIL, VDPR_RETAIL) (SELECT ?, ITEM_CODE, LEVEL_TYPE_ID, LEVEL_ID, REG_PRICE, REG_QTY, REG_M_PRICE, 0, 0, 0, " + 
			 "PROMOTION_FLG, SYSDATE, EFF_REG_START_DATE, LOCATION_ID , CORE_RETAIL, VDPR_RETAIL  FROM RETAIL_PRICE_INFO WHERE CALENDAR_ID = ? )";
	
	private static final String GETALL_RETAIL_PRICE_INFO = "SELECT ITEM_CODE, CALENDAR_ID, LEVEL_TYPE_ID, LEVEL_ID, REG_PRICE, REG_QTY, REG_M_PRICE, " +
			"SALE_PRICE, SALE_QTY, SALE_M_PRICE, PROMOTION_FLG, " +
			"TO_CHAR(EFF_REG_START_DATE,'MM/dd/yyyy') EFF_REG_START_DATE, TO_CHAR(EFF_SALE_START_DATE,'MM/dd/yyyy') EFF_SALE_START_DATE, " +
			"TO_CHAR(EFF_SALE_END_DATE,'MM/dd/yyyy') EFF_SALE_END_DATE ,CORE_RETAIL, VDPR_RETAIL  " +
			"FROM RETAIL_PRICE_INFO WHERE CALENDAR_ID = ? AND ITEM_CODE IN (%s) ORDER BY ITEM_CODE ASC, LEVEL_TYPE_ID DESC";
	
	private HashMap<String, String> storeZoneMap = new HashMap<String, String>();
	
	private boolean checkRetailerItemCode = false; // Changes to retrieve item_code using upc and retailer_item_code
	
	
	//Merge is used to update price records when we receive duplicate entries. 
	/*
	private static final String MERGE_RETAIL_PRICE_INFO = "MERGE INTO RETAIL_PRICE_INFO RPI USING "
			+ "(SELECT ? CALENDAR_ID, ? ITEM_CODE, ? LEVEL_TYPE_ID, ? LEVEL_ID, ? REG_PRICE,? REG_QTY, "
			+ "? REG_M_PRICE, ? SALE_PRICE, ? SALE_QTY, ? SALE_M_PRICE, ? PROMOTION_FLG, TO_DATE(?,'MM/dd/yy') EFF_REG_START_DATE, "
			+ "TO_DATE(?,'MM/dd/yy') EFF_SALE_START_DATE, TO_DATE(?,'MM/dd/yy') EFF_SALE_END_DATE FROM DUAL) D  ON (RPI.CALENDAR_ID = D.CALENDAR_ID AND "
			+ "RPI.LEVEL_TYPE_ID = D.LEVEL_TYPE_ID AND RPI.LEVEL_ID = D.LEVEL_ID AND RPI.ITEM_CODE = D.ITEM_CODE) "
			+ "WHEN MATCHED THEN UPDATE SET RPI.UPDATE_TIMESTAMP = SYSDATE, RPI.REG_PRICE = D.REG_PRICE,  "
			+ "RPI.REG_QTY = D.REG_QTY,RPI.REG_M_PRICE = D.REG_M_PRICE, RPI.SALE_PRICE = D.SALE_PRICE,RPI.SALE_QTY = D.SALE_QTY, "
			+ "RPI.SALE_M_PRICE = D.SALE_M_PRICE, RPI.EFF_REG_START_DATE = D.EFF_REG_START_DATE, RPI.EFF_SALE_START_DATE = D.EFF_SALE_START_DATE, "
			+ "RPI.EFF_SALE_END_DATE = D.EFF_SALE_END_DATE WHEN NOT MATCHED THEN INSERT (RPI.CALENDAR_ID, RPI.ITEM_CODE, RPI.LEVEL_TYPE_ID, RPI.LEVEL_ID, "
			+ "RPI.REG_PRICE, RPI.REG_QTY, RPI.REG_M_PRICE,RPI.SALE_PRICE, RPI.SALE_QTY, RPI.SALE_M_PRICE, RPI.PROMOTION_FLG, RPI.UPDATE_TIMESTAMP, "
			+ "RPI.EFF_REG_START_DATE, RPI.EFF_SALE_START_DATE, RPI.EFF_SALE_END_DATE) "
			+ "VALUES (D.CALENDAR_ID, D.ITEM_CODE, D.LEVEL_TYPE_ID, D.LEVEL_ID, D.REG_PRICE, D.REG_QTY, D.REG_M_PRICE,D.SALE_PRICE, D.SALE_QTY, "
			+ "D.SALE_M_PRICE, D.PROMOTION_FLG, SYSDATE, D.EFF_REG_START_DATE, D.EFF_SALE_START_DATE, D.EFF_SALE_END_DATE)";
	*/
	private static final String CHECK_PRICE_AVAILABILITY_FOR_WEEK = "SELECT CALENDAR_ID FROM RETAIL_PRICE_INFO WHERE CALENDAR_ID = ?";
	
	
	private static final String SAVE_IGNORED_RECORDS = "INSERT INTO IGNORED_PRICE_RECORDS(CALENDAR_ID, SRC_VENDOR_AND_ITEM_ID, LEVEL_ID, "
													+ "RECORD_TYPE, STORE_OR_ZONE_NO, REG_PRICE, REG_QTY, EFF_DATE, PROCESSED_DATE, UPC) "
													+ "VALUES(?, ?, ?, ?, ?, ?, ?, TO_DATE(?,'MM/dd/yy'), SYSDATE, ?)";
	
	
	private static final String ZONE_STORE_MAPPING_QUERY = "SELECT PLM.LOCATION_NO, PLS.STORE_ID, CS.COMP_STR_NO "
			+ " FROM PR_PRODUCT_LOCATION_MAPPING PLM, PR_PRODUCT_LOCATION_STORE PLS, COMPETITOR_STORE CS WHERE "
			+ " PLM.PRODUCT_LOCATION_MAPPING_ID = PLS.PRODUCT_LOCATION_MAPPING_ID AND CS.COMP_STR_ID = PLS.STORE_ID ";
			/*+ " AND PLM.LOCATION_NO IN ('GE-83-5-40101',	'GE-4-5-40101',	'GE-1-5-40101',	'MI-1-5-40111',	"
			+ " 'GE-3-5-40101',	'GE-6-5-40101',	'GE-86-5-40101',	'GE-2-5-40101',	'GE-85-5-40101',"
			+ "	'GE-81-5-40101',	'GE-82-5-40101',	'GE-5-5-40101')";*/
	
	private static final String DELETE_FUTURE_PRICE = "DELETE FROM RETAIL_PRICE_INFO WHERE CALENDAR_ID IN (SELECT CALENDAR_ID FROM RETAIL_CALENDAR "
			+ " WHERE START_DATE > TO_DATE(?, '" + Constants.DB_DATE_FORMAT + "') AND ROW_TYPE = 'W') AND "
			+ " ITEM_CODE = ? AND LEVEL_ID = ? AND LEVEL_TYPE_ID = ? AND (SALE_M_PRICE = 0 OR SALE_PRICE = 0)";
	
	public RetailPriceDAO(){
		checkRetailerItemCode = Boolean.parseBoolean(PropertyManager.getProperty("ITEM_LOAD.CHECK_RETAILER_ITEM_CODE", "FALSE"));
	}
	
	/**
	 * This method is used to retrieve ITEM_CODE for the UPCs passed in the Set 
	 * @param conn		Connection
	 * @param upcSet	Contains set of UPCs for which ITEM_CODE needs to be retrieved
	 * @return HashMap	Contains UPC as key and ITEM_CODE as value
	 * @throws GeneralException
	 */
	public HashMap<String, String> getItemCode(Connection conn,Set<String> upcSet) throws GeneralException{
		logger.debug("Inside getItemCode of RetailPriceDAO");
		
		int limitcount=0;
		List<String> upcList = new ArrayList<String>();
		
	    HashMap<String, String> itemCodeMap = new HashMap<String, String>();
		for(String upc:upcSet){
			upcList.add(PrestoUtil.castUPC(upc,false));
			limitcount++;
			if( limitcount > 0 && (limitcount%this.commitCount == 0)){
				Object[] values = upcList.toArray();
				retrieveItemCode(conn, itemCodeMap, values);
            	upcList.clear();
            }
		}
		if(upcList.size() > 0){
			Object[] values = upcList.toArray();
			retrieveItemCode(conn, itemCodeMap, values);
        	upcList.clear();
		}
		
		logger.debug("No of UPCs passed as input - " + upcSet.size());
		logger.debug("No of UPCs for which ITEM_CODE was fetched - " + itemCodeMap.size());
		return itemCodeMap;
	}
	
	
	
	/**
	 * This method is used to retrieve ITEM_CODE for the UPCs passed in the Set 
	 * @param conn		Connection
	 * @param upcSet	Contains set of UPCs for which ITEM_CODE needs to be retrieved
	 * @return HashMap	Contains UPC as key and ITEM_CODE as value
	 * @throws GeneralException
	 */
	public HashMap<String, String> getItemCodeMap(Connection conn,Set<ItemDetailKey> itemSet) throws GeneralException{
		logger.debug("Inside getItemCode of RetailPriceDAO");
		
		int limitcount=0;
		List<String> upcList = new ArrayList<String>();
		List<String> retItemCodeList = new ArrayList<String>();
	    HashMap<String, String> itemCodeMap = new HashMap<String, String>();
		for(ItemDetailKey itemDetailKey:itemSet){
			upcList.add(PrestoUtil.castUPC(itemDetailKey.getUpc(),false));
			limitcount++;
			if( limitcount > 0 && (limitcount%this.commitCount == 0)){
				Object[] upcArray = upcList.toArray();
				Object[] retItemCodeArray = retItemCodeList.toArray();
				retrieveItemCodeMap(conn, itemCodeMap, upcArray, retItemCodeArray);
            	upcList.clear();
            }
		}
		if(upcList.size() > 0){
			Object[] upcArray = upcList.toArray();
			Object[] retItemCodeArray = retItemCodeList.toArray();
			retrieveItemCodeMap(conn, itemCodeMap, upcArray, retItemCodeArray);
        	upcList.clear();
		}
		
		logger.debug("No of UPCs passed as input - " + itemSet.size());
		logger.debug("No of UPCs for which ITEM_CODE was fetched - " + itemCodeMap.size());
		return itemCodeMap;
	}
	
	/**
	 * This method queries the database for ITEM_CODE for every set of UPCs
	 * @param conn			Connection
	 * @param itemCodeMap	Map that will contain the result of the database retrieval
	 * @param values		Array of UPCs that will be passed as input to the query
	 * @throws GeneralException
	 */
	private void retrieveItemCode(Connection conn, HashMap<String, String> itemCodeMap, Object... values) throws GeneralException{
		logger.debug("Inside retrieveItemCode() of RetailPriceDAO");
		PreparedStatement statement = null;
	    ResultSet resultSet = null;
		try{
			statement = conn.prepareStatement(String.format(GET_ITEM_CODE, preparePlaceHolders(values.length)));
	        setValues(statement, values);
	        resultSet = statement.executeQuery();
	      
			CachedRowSet crs = new CachedRowSetImpl();
	        crs.populate(resultSet);
	        resultSet.close();
	        while(crs.next()){
	        	itemCodeMap.put(crs.getString("UPC"), crs.getString("ITEM_CODE"));
	        }
		}
		catch (SQLException e)
		{
			logger.error("Error while executing GET_ITEM_CODE");
			throw new GeneralException("Error while executing GET_ITEM_CODE", e);
		}finally{
			PristineDBUtil.close(resultSet);
			PristineDBUtil.close(statement);
		}
	}
	
	
	
	/**
	 * This method queries the database for ITEM_CODE for every set of UPCs
	 * @param conn			Connection
	 * @param itemCodeMap	Map that will contain the result of the database retrieval
	 * @param values		Array of UPCs that will be passed as input to the query
	 * @throws GeneralException
	 */
	private void retrieveItemCodeMap(Connection conn, HashMap<String, String> itemCodeMap, Object[] upcArray, Object[] retItemCodeArray) throws GeneralException{
		logger.debug("Inside retrieveItemCode() of RetailPriceDAO");
		PreparedStatement statement = null;
	    ResultSet resultSet = null;
		try{
			statement = conn.prepareStatement(String.format(GET_ITEM_CODE, preparePlaceHolders(upcArray.length)));
	        setValues(statement, upcArray);
	        resultSet = statement.executeQuery();
	        CachedRowSet crs = new CachedRowSetImpl();
	        crs.populate(resultSet);
	        resultSet.close();
	        while(crs.next()){
	        	itemCodeMap.put(crs.getString("UPC"), crs.getString("ITEM_CODE"));
	        }
		}
		catch (SQLException e)
		{
			logger.error("Error while executing GET_ITEM_CODE");
			throw new GeneralException("Error while executing GET_ITEM_CODE", e);
		}finally{
			PristineDBUtil.close(resultSet);
			PristineDBUtil.close(statement);
		}
	}
	
	/**
	 * This method is used to retrieve Zone Number, Chain Id for the Store Number passed in the Set
	 * @param conn			Connection
	 * @param storeIdSet	Set containing list of Store Numbers for which Zone numbers needs to be retrieved
	 * @return HashMap		Contains Store Number as key and RetailPriceDTO as value
	 * @throws GeneralException
	 */
	public HashMap<String, String> getStoreInfo(Connection conn, Set<String> storeNbrSet) throws GeneralException{
		logger.debug("Inside getStoreInfo of RetailPriceDAO");
		
		int limitcount=0;
		List<String> storeIdList = new ArrayList<String>();
		
	    HashMap<String, String> storeZoneMapping = new HashMap<String, String>();
		for(String storeNbr:storeNbrSet){
			storeIdList.add(storeNbr);
			limitcount++;
			if( limitcount > 0 && (limitcount%this.commitCount == 0)){
				Object[] values = storeIdList.toArray();
				retrieveStoreInfo(conn, storeZoneMapping, values);
				storeIdList.clear();
            }
		}
		if(storeIdList.size() > 0){
			Object[] values = storeIdList.toArray();
			retrieveStoreInfo(conn, storeZoneMapping, values);
			storeIdList.clear();
		}
		
		logger.info("No of Store Numbers passed as input - " + storeNbrSet.size());
		logger.info("No of Store Numbers for which Zone Number was fetched - " + storeZoneMapping.size());
		return storeZoneMapping;
	}
	
	/**
	 * This method queries the database for Zone Number, Chain Id for every set of Store Numbers
	 * @param conn					Connection
	 * @param storeZoneMapping		Map that will contain the result of the database retrieval
	 * @param values				Array of UPCs that will be passed as input to the query
	 * @throws GeneralException
	 */
	private void retrieveStoreInfo(Connection conn, HashMap<String, String> storeZoneMapping, Object... values) throws GeneralException{
		logger.debug("Inside retrieveStoreInfo() of RetailPriceDAO");
		PreparedStatement statement = null;
	    ResultSet resultSet = null;
		try{
			statement = conn.prepareStatement(String.format(GET_STORE_INFO, preparePlaceHolders(values.length)));
	        setValues(statement, values);
	        resultSet = statement.executeQuery();
	        while(resultSet.next()){
	        	storeZoneMapping.put(resultSet.getString("COMP_STR_NO"), resultSet.getString("ZONE_NUM"));
	        }
		}
		catch (SQLException e)
		{
			logger.error("Error while executing GET_STORE_INFO");
			throw new GeneralException("Error while executing GET_STORE_INFO", e);
		}
		finally{
			PristineDBUtil.close(resultSet);
			PristineDBUtil.close(statement);
		}
	}
	
	/**
	 * This method queries the database for Zone Number, Chain Id for every set of Store Numbers
	 * @param conn					Connection
	 * @param storeZoneMapping		Map that will contain the result of the database retrieval
	 * @param values				Array of UPCs that will be passed as input to the query
	 * @throws GeneralException
	 */
	public String retrieveStoreInfo(Connection conn, String storeNum) throws GeneralException{
		logger.debug("Inside retrieveStoreInfo() of RetailPriceDAO");
		PreparedStatement statement = null;
	    ResultSet resultSet = null;
		try{
			if(storeZoneMap.get(storeNum) != null){
				return storeZoneMap.get(storeNum);
			}else{
				statement = conn.prepareStatement(GET_SINGLE_STORE_INFO);
		        statement.setString(1, storeNum);
		        resultSet = statement.executeQuery();
		        while(resultSet.next()){
		        	storeZoneMap.put(resultSet.getString("COMP_STR_NO"), resultSet.getString("ZONE_NUM"));
		        }
		        return storeZoneMap.get(storeNum);
			}
		}
		catch (SQLException e)
		{
			logger.error("Error while executing GET_STORE_INFO");
			throw new GeneralException("Error while executing GET_STORE_INFO", e);
		}
		finally{
			PristineDBUtil.close(resultSet);
			PristineDBUtil.close(statement);
		}
	}
	
	/**
	 * This method returns a String with specified number of bind parameters
	 * @param length	Number of bind parameters
	 * @return String with specified number of bind parameters	
	 */
	public static String preparePlaceHolders(int length) {
	    StringBuilder builder = new StringBuilder();
	    for (int i = 0; i < length;) {
	        builder.append("?");
	        if (++i < length) {
	            builder.append(",");
	        }
	    }
	    return builder.toString();
	}

	/**
	 * This method is used when there are no other bind parameters to be set in Prepared Statement
	 * other than the ones present in the array.
	 * @param preparedStatement 	PreparedStatement in which values need to be set
	 * @param values				Array of Values that needs to be set in the Prepared Statement
	 * @throws SQLException
	 */
	public static void setValues(PreparedStatement preparedStatement, Object... values) throws SQLException {
	    for (int i = 0; i < values.length; i++) {
	        preparedStatement.setObject(i + 1, values[i]);
	    }
	}
	
	/**
	 * This method is used when there are other bind parameters to be set in Prepared Statement
	 * other than the ones present in the array.
	 * @param preparedStatement		PreparedStatement in which values need to be set
	 * @param startCount			Count starting which the parameters needs to be bound to the prepared statement
	 * @param values				Array of Values that needs to be set in the Prepared Statement
	 * @throws SQLException
	 */
	public static void setValues(PreparedStatement preparedStatement, int startCount, Object... values) throws SQLException {
	    for (int i = 0; i < values.length ; i++) {
	        preparedStatement.setObject(i + startCount , values[i]);
	    }
	}
	
	/**
	 * This method retrieves values from RETAIL_PRICE_INFO based on ITEM_CODE
	 * @param conn				Connection
	 * @param itemCodeSet		Set containing item codes for which data needs to be retrieved from retail_price_info
	 * @param calendarId		Calendar Id
	 * @return HashMap containing item code as key and list of its price data as value
	 * @throws GeneralException
	 */
	public HashMap<String, List<RetailPriceDTO>> getRetailPriceInfo(Connection conn, Set<String> itemCodeSet, int calendarId, boolean isLatest) throws GeneralException{
		List<String> itemCodeList = new ArrayList<String>();
	    HashMap<String, List<RetailPriceDTO>> retailPriceDBMap = new HashMap<String, List<RetailPriceDTO>>();
	    int limitcount=0;
	    
		for(String itemCode:itemCodeSet){
			itemCodeList.add(itemCode);
			limitcount++;
			if( limitcount > 0 && (limitcount%this.commitCount == 0)){
				Object[] values = itemCodeList.toArray();
				List<RetailPriceDTO> retailPriceDBList = retrieveRetailPriceInfo(conn, calendarId, isLatest, values);
				
				for(RetailPriceDTO retailPriceDTO:retailPriceDBList){
					String dbItemCOde = retailPriceDTO.getItemcode();
					if(retailPriceDBMap.get(dbItemCOde) != null){
		        		List<RetailPriceDTO> tempList = retailPriceDBMap.get(dbItemCOde);
		        		tempList.add(retailPriceDTO);
		        		retailPriceDBMap.put(dbItemCOde, tempList);
		        	}else{
		        		List<RetailPriceDTO> tempList = new ArrayList<RetailPriceDTO>();
		        		tempList.add(retailPriceDTO);
		        		retailPriceDBMap.put(dbItemCOde, tempList);
		        	}
				}
				itemCodeList.clear();
            }
		}
		if(itemCodeList.size() > 0){
			Object[] values = itemCodeList.toArray();
			List<RetailPriceDTO> retailPriceDBList = retrieveRetailPriceInfo(conn, calendarId, isLatest, values);
			
			for(RetailPriceDTO retailPriceDTO:retailPriceDBList){
				String dbItemCOde = retailPriceDTO.getItemcode();
				if(retailPriceDBMap.get(dbItemCOde) != null){
	        		List<RetailPriceDTO> tempList = retailPriceDBMap.get(dbItemCOde);
	        		tempList.add(retailPriceDTO);
	        		retailPriceDBMap.put(dbItemCOde, tempList);
	        	}else{
	        		List<RetailPriceDTO> tempList = new ArrayList<RetailPriceDTO>();
	        		tempList.add(retailPriceDTO);
	        		retailPriceDBMap.put(dbItemCOde, tempList);
	        	}
			}
			itemCodeList.clear();
		}
		
		return retailPriceDBMap;
	}
	
	/**
	 * This method retrieves values from RETAIL_PRICE_INFO based on ITEM_CODE
	 * @param conn				Connection
	 * @param itemCodeSet		Set containing item codes for which data needs to be retrieved from retail_price_info
	 * @param calendarId		Calendar Id
	 * @param historyCalendarList 
	 * @return HashMap containing item code as key and list of its price data as value
	 * @throws GeneralException
	 */
	public HashMap<String, List<RetailPriceDTO>> getRetailPriceInfoWithHistory(Connection conn, Set<String> itemCodeSet, 
			int calendarId, boolean isLatest, List<Integer> historyCalendarList) throws GeneralException{
		List<String> itemCodeList = new ArrayList<String>();
	    HashMap<String, List<RetailPriceDTO>> retailPriceDBMap = new HashMap<String, List<RetailPriceDTO>>();
	    int limitcount=0;
	    
		for(String itemCode:itemCodeSet){
			itemCodeList.add(itemCode);
			limitcount++;
			if( limitcount > 0 && (limitcount%this.commitCount == 0)){
				Object[] values = itemCodeList.toArray();
				List<RetailPriceDTO> retailPriceDBList = retrieveRetailPriceInfo(conn, calendarId, isLatest, values);
				
				for(RetailPriceDTO retailPriceDTO:retailPriceDBList){
					String dbItemCOde = retailPriceDTO.getItemcode();
					if(retailPriceDBMap.get(dbItemCOde) != null){
		        		List<RetailPriceDTO> tempList = retailPriceDBMap.get(dbItemCOde);
		        		tempList.add(retailPriceDTO);
		        		retailPriceDBMap.put(dbItemCOde, tempList);
		        	}else{
		        		List<RetailPriceDTO> tempList = new ArrayList<RetailPriceDTO>();
		        		tempList.add(retailPriceDTO);
		        		retailPriceDBMap.put(dbItemCOde, tempList);
		        	}
				}
				itemCodeList.clear();
            }
		}
		if(itemCodeList.size() > 0){
			Object[] values = itemCodeList.toArray();
			List<RetailPriceDTO> retailPriceDBList = retrieveRetailPriceInfo(conn, calendarId, isLatest, values);
			
			for(RetailPriceDTO retailPriceDTO:retailPriceDBList){
				String dbItemCOde = retailPriceDTO.getItemcode();
				if(retailPriceDBMap.get(dbItemCOde) != null){
	        		List<RetailPriceDTO> tempList = retailPriceDBMap.get(dbItemCOde);
	        		tempList.add(retailPriceDTO);
	        		retailPriceDBMap.put(dbItemCOde, tempList);
	        	}else{
	        		List<RetailPriceDTO> tempList = new ArrayList<RetailPriceDTO>();
	        		tempList.add(retailPriceDTO);
	        		retailPriceDBMap.put(dbItemCOde, tempList);
	        	}
			}
			itemCodeList.clear();
		}
		   	
		
		Set<String> itemsNotPresent = new HashSet<String>(); 
		for(String itemCode : itemCodeSet){
			if(retailPriceDBMap.get(itemCode) == null)
				itemsNotPresent.add(itemCode);
		}
		if(itemsNotPresent.size() > 0){
			HashMap<String, List<RetailPriceDTO>> priceRolledUpMap2 = new RetailPriceDAO().getRetailPriceInfoHistory(conn, 
					itemsNotPresent, historyCalendarList);			
			retailPriceDBMap.putAll(priceRolledUpMap2);
		}
		
		return retailPriceDBMap;
	}
	
	
	/**
	 * This method queries the database for Retail Price Info for every set of Item Codes
	 * @param conn			Connection
	 * @param calendarId	Calendar Id
	 * @param values		Array of UPCs that will be passed as input to the query
	 * @return List of retail price data
	 * @throws GeneralException
	 */
	private List<RetailPriceDTO> retrieveRetailPriceInfo(Connection conn, int calendarId, boolean isLatest, Object... values) throws GeneralException{
		PreparedStatement statement = null;
	    ResultSet resultSet = null;
	    List<RetailPriceDTO> retailPriceDTOList = new ArrayList<RetailPriceDTO>();
		try{
			long startTime = System.currentTimeMillis();
			
			/* Else block added to retrieve latest retail price info for item codes.
			 * This will be used when loading competitive data table from retail price info table
			 */
			if(!isLatest){
				statement = conn.prepareStatement(String.format(GET_RETAIL_PRICE_INFO, preparePlaceHolders(values.length)));
				statement.setInt(1, calendarId);
		        setValues(statement, 2, values);
			}else{
				statement = conn.prepareStatement(String.format(GET_LATEST_RETAIL_PRICE_INFO, preparePlaceHolders(values.length)));
				statement.setInt(1, calendarId);
		        setValues(statement, 2, values);
			}
			statement.setFetchSize(100000);
	        resultSet = statement.executeQuery();
	        logger.debug("Query Executed");
	        RetailPriceDTO retailPriceDTO;
	        while(resultSet.next()){
	        	retailPriceDTO = new RetailPriceDTO();
	        	retailPriceDTO.setItemcode(resultSet.getString("ITEM_CODE"));
	        	retailPriceDTO.setCalendarId(resultSet.getInt("CALENDAR_ID"));
	        	retailPriceDTO.setLevelTypeId(resultSet.getInt("LEVEL_TYPE_ID"));
	        	retailPriceDTO.setLevelId(resultSet.getString("LEVEL_ID"));
	        	retailPriceDTO.setRegPrice(resultSet.getFloat("REG_PRICE"));
	        	retailPriceDTO.setRegQty(resultSet.getInt("REG_QTY"));
	        	retailPriceDTO.setRegMPrice(resultSet.getFloat("REG_M_PRICE"));
	        	retailPriceDTO.setSalePrice(resultSet.getFloat("SALE_PRICE"));
	        	retailPriceDTO.setSaleQty(resultSet.getInt("SALE_QTY"));
	        	retailPriceDTO.setSaleMPrice(resultSet.getFloat("SALE_M_PRICE"));
	        	retailPriceDTO.setPromotionFlag(resultSet.getString("PROMOTION_FLG"));
	        	retailPriceDTO.setRegEffectiveDate(resultSet.getString("EFF_REG_START_DATE"));
	        	retailPriceDTO.setSaleStartDate(resultSet.getString("EFF_SALE_START_DATE"));
	        	retailPriceDTO.setSaleEndDate(resultSet.getString("EFF_SALE_END_DATE"));
	        	retailPriceDTOList.add(retailPriceDTO);
	        }
	        logger.debug("Records populated");
	        long endTime = System.currentTimeMillis();
	        logger.debug("Time taken to execute and store price info in list- " + (endTime - startTime));
		}
		catch (SQLException e)
		{
			logger.error("Error while executing GET_RETAIL_PRICE_INFO");
			throw new GeneralException("Error while executing GET_RETAIL_PRICE_INFO", e);
		}finally{
			PristineDBUtil.close(resultSet);
			PristineDBUtil.close(statement);
		}
		return retailPriceDTOList;
	}
	
	/*public HashMap<String, List<RetailPriceDTO>> getRetailPriceInfo5Wk(Connection conn, Set<String> itemCodeSet, int calendarId, boolean isLatest) throws GeneralException{
		List<String> itemCodeList = new ArrayList<String>();
	    HashMap<String, List<RetailPriceDTO>> retailPriceDBMap = new HashMap<String, List<RetailPriceDTO>>();
	    int limitcount=0;
	    
		for(String itemCode:itemCodeSet){
			itemCodeList.add(itemCode);
			limitcount++;
			if( limitcount > 0 && (limitcount%this.commitCount == 0)){
				Object[] values = itemCodeList.toArray();
				List<RetailPriceDTO> retailPriceDBList = retrieveRetailPriceInfo5Wk(conn, calendarId, isLatest, values);
				
				for(RetailPriceDTO retailPriceDTO:retailPriceDBList){
					String dbItemCOde = retailPriceDTO.getItemcode();
					if(retailPriceDBMap.get(dbItemCOde) != null){
		        		List<RetailPriceDTO> tempList = retailPriceDBMap.get(dbItemCOde);
		        		tempList.add(retailPriceDTO);
		        		retailPriceDBMap.put(dbItemCOde, tempList);
		        	}else{
		        		List<RetailPriceDTO> tempList = new ArrayList<RetailPriceDTO>();
		        		tempList.add(retailPriceDTO);
		        		retailPriceDBMap.put(dbItemCOde, tempList);
		        	}
				}
				itemCodeList.clear();
            }
		}
		if(itemCodeList.size() > 0){
			Object[] values = itemCodeList.toArray();
			List<RetailPriceDTO> retailPriceDBList = retrieveRetailPriceInfo5Wk(conn, calendarId, isLatest, values);
			
			for(RetailPriceDTO retailPriceDTO:retailPriceDBList){
				String dbItemCOde = retailPriceDTO.getItemcode();
				if(retailPriceDBMap.get(dbItemCOde) != null){
	        		List<RetailPriceDTO> tempList = retailPriceDBMap.get(dbItemCOde);
	        		tempList.add(retailPriceDTO);
	        		retailPriceDBMap.put(dbItemCOde, tempList);
	        	}else{
	        		List<RetailPriceDTO> tempList = new ArrayList<RetailPriceDTO>();
	        		tempList.add(retailPriceDTO);
	        		retailPriceDBMap.put(dbItemCOde, tempList);
	        	}
			}
			itemCodeList.clear();
		}
		return retailPriceDBMap;
	}*/
	
	public HashMap<String, List<RetailPriceDTO>> retrieveRetailPriceInfo5Wk(Connection conn, int calendarId, int strId, PriceZoneDTO priceZoneDto, String weekStartDate, String weekEndDate) throws GeneralException{
		PreparedStatement statement = null;
	    ResultSet resultSet = null;
	    HashMap<String, List<RetailPriceDTO>> retailPriceDBMap = new HashMap<String, List<RetailPriceDTO>>();
		try{
			long startTime = System.currentTimeMillis();
			
			/* Else block added to retrieve latest retail price info for item codes.
			 * This will be used when loading competitive data table from retail price info table
			 */
			//Store level
			if(priceZoneDto == null){
				String sql = GET_RETAIL_PRICE_INFO_5WK.replaceAll("%ITEM_CODE_QUERY%", GET_ITEM_CODES_FOR_STORE);
				statement = conn.prepareStatement(sql);
				int counter = 0;
				statement.setInt(++counter, calendarId);
				statement.setInt(++counter, strId);
				statement.setString(++counter, weekStartDate);
				statement.setString(++counter, weekEndDate);
				statement.setInt(++counter, strId);
		        statement.setInt(++counter, strId);
		        statement.setString(++counter, weekEndDate);
		        statement.setString(++counter, weekEndDate);
			}
			//Added for supporting chain level price index
			else if(priceZoneDto.getLocationLevelId() == Constants.CHAIN_LEVEL_ID){
				String sql = GET_RETAIL_PRICE_INFO_5WK.replaceAll("%ITEM_CODE_QUERY%", GET_ITEM_CODES_FOR_CHAIN);
				statement = conn.prepareStatement(sql);
				int counter = 0;
				statement.setInt(++counter, calendarId);
				statement.setInt(++counter, priceZoneDto.getLocationId());
				statement.setString(++counter, weekStartDate);
				statement.setString(++counter, weekEndDate);
				/*statement.setInt(++counter, priceZoneDto.getPrimaryCompetitor());
		        statement.setInt(++counter, priceZoneDto.getSecComp1());
		        statement.setInt(++counter, priceZoneDto.getSecComp2());
		        statement.setInt(++counter, priceZoneDto.getSecComp3());*/
		        statement.setString(++counter, weekEndDate);
				statement.setString(++counter, weekEndDate);
			}
			//Zone level
			else{
				String sql = GET_RETAIL_PRICE_INFO_5WK.replaceAll("%ITEM_CODE_QUERY%", GET_ITEM_CODES_FOR_ZONE);
				statement = conn.prepareStatement(sql);
				int counter = 0;
				statement.setInt(++counter, calendarId);
				statement.setInt(++counter, priceZoneDto.getPriceZoneId());
				statement.setString(++counter, weekStartDate);
				statement.setString(++counter, weekEndDate);
				statement.setInt(++counter, priceZoneDto.getPrimaryCompetitor());
		        statement.setInt(++counter, priceZoneDto.getSecComp1());
		        statement.setInt(++counter, priceZoneDto.getSecComp2());
		        statement.setInt(++counter, priceZoneDto.getSecComp3());
		        statement.setString(++counter, weekEndDate);
				statement.setString(++counter, weekEndDate);
			}
			
			statement.setFetchSize(2000000);
	        resultSet = statement.executeQuery();
	        RetailPriceDTO retailPriceDTO;
	        while(resultSet.next()){
	        	retailPriceDTO = new RetailPriceDTO();
	        	retailPriceDTO.setItemcode(resultSet.getString("ITEM_CODE"));
	        	retailPriceDTO.setCalendarId(resultSet.getInt("CALENDAR_ID"));
	        	retailPriceDTO.setLevelTypeId(resultSet.getInt("LEVEL_TYPE_ID"));
	        	retailPriceDTO.setLevelId(resultSet.getString("LEVEL_ID"));
	        	retailPriceDTO.setRegPrice(resultSet.getFloat("REG_PRICE"));
	        	retailPriceDTO.setRegQty(resultSet.getInt("REG_QTY"));
	        	retailPriceDTO.setRegMPrice(resultSet.getFloat("REG_M_PRICE"));
	        	retailPriceDTO.setSalePrice(resultSet.getFloat("SALE_PRICE"));
	        	retailPriceDTO.setSaleQty(resultSet.getInt("SALE_QTY"));
	        	retailPriceDTO.setSaleMPrice(resultSet.getFloat("SALE_M_PRICE"));
	        	retailPriceDTO.setPromotionFlag(resultSet.getString("PROMOTION_FLG"));
	        	retailPriceDTO.setRegEffectiveDate(resultSet.getString("EFF_REG_START_DATE"));
	        	retailPriceDTO.setSaleStartDate(resultSet.getString("EFF_SALE_START_DATE"));
	        	retailPriceDTO.setSaleEndDate(resultSet.getString("EFF_SALE_END_DATE"));
	        	
	        	String dbItemCOde = retailPriceDTO.getItemcode();
				if(retailPriceDBMap.get(dbItemCOde) != null){
	        		List<RetailPriceDTO> tempList = retailPriceDBMap.get(dbItemCOde);
	        		tempList.add(retailPriceDTO);
	        		retailPriceDBMap.put(dbItemCOde, tempList);
	        	}else{
	        		List<RetailPriceDTO> tempList = new ArrayList<RetailPriceDTO>();
	        		tempList.add(retailPriceDTO);
	        		retailPriceDBMap.put(dbItemCOde, tempList);
	        	}
	        }
	        long endTime = System.currentTimeMillis();
	        logger.debug("Time taken to execute and store price info in list- " + (endTime - startTime));
		}
		catch (SQLException e)
		{
			logger.error("Error while executing GET_RETAIL_PRICE_INFO");
			throw new GeneralException("Error while executing GET_RETAIL_PRICE_INFO", e);
		}finally{
			PristineDBUtil.close(resultSet);
			PristineDBUtil.close(statement);
		}
		return retailPriceDBMap;
	}

	/**
	 * This method retrieves values from RETAIL_PRICE_INFO based on ITEM_CODE
	 * @param conn				Connection
	 * @param itemCodeSet		Set containing item codes for which data needs to be retrieved from retail_price_info
	 * @param calendarId		Calendar Id
	 * @return HashMap containing item code as key and list of its price data as value
	 * @throws GeneralException
	 */
	public HashMap<String, List<RetailPriceDTO>> getRetailPriceInfoHistory(Connection conn, Set<String> itemCodeSet, List<Integer> calendarIdList) throws GeneralException{
		List<String> itemCodeList = new ArrayList<String>();
	    HashMap<String, List<RetailPriceDTO>> retailPriceDBMap = new HashMap<String, List<RetailPriceDTO>>();
	    int limitcount=0;
	    
		for(String itemCode:itemCodeSet){
			itemCodeList.add(itemCode);
			limitcount++;
			if( limitcount > 0 && (limitcount%this.commitCount == 0)){
				Object[] values = itemCodeList.toArray();
				List<RetailPriceDTO> retailPriceDBList = retrieveRetailPriceInfoHistory(conn, calendarIdList, values);
				
				for(RetailPriceDTO retailPriceDTO:retailPriceDBList){
					String dbItemCOde = retailPriceDTO.getItemcode();
					if(retailPriceDBMap.get(dbItemCOde) != null){
		        		List<RetailPriceDTO> tempList = retailPriceDBMap.get(dbItemCOde);
		        		tempList.add(retailPriceDTO);
		        		retailPriceDBMap.put(dbItemCOde, tempList);
		        	}else{
		        		List<RetailPriceDTO> tempList = new ArrayList<RetailPriceDTO>();
		        		tempList.add(retailPriceDTO);
		        		retailPriceDBMap.put(dbItemCOde, tempList);
		        	}
				}
				itemCodeList.clear();
            }
		}
		if(itemCodeList.size() > 0){
			Object[] values = itemCodeList.toArray();
			List<RetailPriceDTO> retailPriceDBList = retrieveRetailPriceInfoHistory(conn, calendarIdList, values);
			
			for(RetailPriceDTO retailPriceDTO:retailPriceDBList){
				String dbItemCOde = retailPriceDTO.getItemcode();
				if(retailPriceDBMap.get(dbItemCOde) != null){
	        		List<RetailPriceDTO> tempList = retailPriceDBMap.get(dbItemCOde);
	        		tempList.add(retailPriceDTO);
	        		retailPriceDBMap.put(dbItemCOde, tempList);
	        	}else{
	        		List<RetailPriceDTO> tempList = new ArrayList<RetailPriceDTO>();
	        		tempList.add(retailPriceDTO);
	        		retailPriceDBMap.put(dbItemCOde, tempList);
	        	}
			}
			itemCodeList.clear();
		}
		return retailPriceDBMap;
	}
	
	/**
	 * This method queries the database for Retail Price Info for every set of Item Codes
	 * @param conn			Connection
	 * @param calendarId	Calendar Id
	 * @param values		Array of UPCs that will be passed as input to the query
	 * @return List of retail price data
	 * @throws GeneralException
	 */
	private List<RetailPriceDTO> retrieveRetailPriceInfoHistory(Connection conn, List<Integer> calendarIdList, Object... values) throws GeneralException{
		PreparedStatement statement = null;
	    ResultSet resultSet = null;
	    List<RetailPriceDTO> retailPriceDTOList = new ArrayList<RetailPriceDTO>();
		try{
			long startTime = System.currentTimeMillis();
			
        	StringBuffer calIdStr = new StringBuffer("");
		    int count = 0;
		    for(Integer calId : calendarIdList){
		    	calIdStr.append(calId);
		        if(count < (calendarIdList.size()-1)){
		        	calIdStr.append(",");
		        }
		        count++;
		    }

		    String sql = GET_RETAIL_PRICE_INFO_FROM_HISTORY.replaceAll("%c", calIdStr.toString());
		    logger.debug("Calendar String : " + calIdStr.toString());
		    logger.debug("retrieveRetailPriceInfoHistory sql: " + sql);
		    statement = conn.prepareStatement(String.format(sql, preparePlaceHolders(values.length)));
		    setValues(statement, values);
		    
	        resultSet = statement.executeQuery();
	        
	        RetailPriceDTO retailPriceDTO;
	        while(resultSet.next()){
	        	retailPriceDTO = new RetailPriceDTO();
	        	retailPriceDTO.setItemcode(resultSet.getString("ITEM_CODE"));
	        	retailPriceDTO.setCalendarId(resultSet.getInt("CALENDAR_ID"));
	        	retailPriceDTO.setLevelTypeId(resultSet.getInt("LEVEL_TYPE_ID"));
	        	retailPriceDTO.setLevelId(resultSet.getString("LEVEL_ID"));
	        	retailPriceDTO.setRegPrice(resultSet.getFloat("REG_PRICE"));
	        	retailPriceDTO.setRegQty(resultSet.getInt("REG_QTY"));
	        	retailPriceDTO.setRegMPrice(resultSet.getFloat("REG_M_PRICE"));
	        	retailPriceDTO.setSalePrice(resultSet.getFloat("SALE_PRICE"));
	        	retailPriceDTO.setSaleQty(resultSet.getInt("SALE_QTY"));
	        	retailPriceDTO.setSaleMPrice(resultSet.getFloat("SALE_M_PRICE"));
	        	retailPriceDTO.setPromotionFlag(resultSet.getString("PROMOTION_FLG"));
	        	retailPriceDTO.setRegEffectiveDate(resultSet.getString("EFF_REG_START_DATE"));
	        	retailPriceDTO.setSaleStartDate(resultSet.getString("EFF_SALE_START_DATE"));
	        	retailPriceDTO.setSaleEndDate(resultSet.getString("EFF_SALE_END_DATE"));
	        	retailPriceDTOList.add(retailPriceDTO);
	        }
	        long endTime = System.currentTimeMillis();
	        logger.debug("Time taken to execute and store price info in list- " + (endTime - startTime));
		}
		catch (SQLException e)
		{
			logger.error("Error while executing GET_RETAIL_PRICE_INFO");
			throw new GeneralException("Error while executing GET_RETAIL_PRICE_INFO", e);
		}finally{
			PristineDBUtil.close(resultSet);
			PristineDBUtil.close(statement);
		}
		return retailPriceDTOList;
	}
	
	/**
	 * This method inserts retail price data in RETAIL_PRICE_INFO table
	 * @param conn					Connection
	 * @param toBeInsertedList		List of records that needs to be inserted in retail_price_info
	 * @throws GeneralException
	 */
	public void saveRetailPriceData(Connection conn, List<RetailPriceDTO> toBeInsertedList) throws GeneralException{
		logger.debug("Inside saveRetailPriceData() of RetailPriceDAO");
		PreparedStatement statement = null;
	    try{
			statement = conn.prepareStatement(SAVE_RETAIL_PRICE_INFO);
	        
			int recordCnt = 0;
			int itemNoInBatch = 0;
	        for(RetailPriceDTO retailPriceDTO:toBeInsertedList){
	        	int counter = 0;
	        	logger.debug("Row Insert: " + retailPriceDTO.toString());
	        	statement.setInt(++counter, retailPriceDTO.getCalendarId());
	        	statement.setString(++counter, retailPriceDTO.getItemcode());
	        	statement.setInt(++counter, retailPriceDTO.getLevelTypeId());
	        	statement.setString(++counter, retailPriceDTO.getLevelId());
	        	statement.setFloat(++counter, retailPriceDTO.getRegPrice());
	        	statement.setInt(++counter, retailPriceDTO.getRegQty());
	        	statement.setFloat(++counter, retailPriceDTO.getRegMPrice());
	        	statement.setFloat(++counter, retailPriceDTO.getSalePrice());
	        	statement.setInt(++counter, retailPriceDTO.getSaleQty());
	        	statement.setFloat(++counter, retailPriceDTO.getSaleMPrice());
	        	statement.setString(++counter,retailPriceDTO.getPromotionFlag());
	        	statement.setString(++counter, retailPriceDTO.getRegEffectiveDate());
	        	statement.setString(++counter, retailPriceDTO.getSaleStartDate());
	        	statement.setString(++counter, retailPriceDTO.getSaleEndDate());
	        	if(retailPriceDTO.getLocationId() == null)
	        		statement.setNull(++counter, Types.NULL);
	        	else
	        		statement.setInt(++counter, retailPriceDTO.getLocationId());
	        	if(retailPriceDTO.isWhseZoneRolledUpRecord()){
	        		statement.setString(++counter, String.valueOf(Constants.YES));
	        	}else{
	        		statement.setString(++counter, String.valueOf(Constants.NO));
	        	}
	        	statement.addBatch();
	        	itemNoInBatch++;
	        	recordCnt++;
	        	if(itemNoInBatch % Constants.BATCH_UPDATE_COUNT == 0){
	        		logger.debug("Batch Execute...");
	        		statement.executeBatch();
	        		statement.clearBatch();
	        		itemNoInBatch = 0;
	        		logger.debug("Batch Execute is completed");
	        	}
	        	if(recordCnt % 10000 == 0){
	        		conn.commit();
	        	}
	        }
	        if(itemNoInBatch > 0){
	        	logger.debug("Batch Execute...");
	        	statement.executeBatch();
        		statement.clearBatch();
        		logger.debug("Batch Execute is completed");
	        }
		}
		catch (SQLException e)
		{
			logger.error("Error while executing SAVE_RETAIL_PRICE_INFO");
			throw new GeneralException("Error while executing SAVE_RETAIL_PRICE_INFO" + e);
		}finally{
			PristineDBUtil.close(statement);
		}
	}
	
	
	/**
	 * This method updates retail price data in RETAIL_PRICE_INFO table
	 * @param conn					Connection
	 * @param toBeUpdatedList		List of records that needs to be updated in retail_price_info
	 * @throws GeneralException
	 */
	public void updateRetailPriceData(Connection conn, List<RetailPriceDTO> toBeUpdatedList) throws GeneralException{
		logger.debug("Inside updateRetailPriceData() of RetailPriceDAO");
		PreparedStatement statement = null;
	    try{
			statement = conn.prepareStatement(UPDATE_RETAIL_PRICE_INFO);
	        
			int itemNoInBatch = 0;
	        for(RetailPriceDTO retailPriceDTO:toBeUpdatedList){
	        	int counter = 0;
	        	statement.setFloat(++counter, retailPriceDTO.getRegPrice());
	        	statement.setInt(++counter, retailPriceDTO.getRegQty());
	        	statement.setFloat(++counter, retailPriceDTO.getRegMPrice());
	        	statement.setFloat(++counter, retailPriceDTO.getSalePrice());
	        	statement.setInt(++counter, retailPriceDTO.getSaleQty());
	        	statement.setFloat(++counter, retailPriceDTO.getSaleMPrice());
	        	statement.setString(++counter,retailPriceDTO.getPromotionFlag());
	        	statement.setString(++counter, retailPriceDTO.getRegEffectiveDate());
	        	statement.setString(++counter, retailPriceDTO.getSaleStartDate());
	        	statement.setString(++counter, retailPriceDTO.getSaleEndDate());
	        	if(retailPriceDTO.getLocationId() == null)
	        		statement.setNull(++counter, Types.NULL);
	        	else
	        		statement.setInt(++counter, retailPriceDTO.getLocationId());
	        	if(retailPriceDTO.isWhseZoneRolledUpRecord()){
	        		statement.setString(++counter, String.valueOf(Constants.YES));
	        	}else{
	        		statement.setString(++counter, String.valueOf(Constants.NO));
	        	}
	        	statement.setInt(++counter, retailPriceDTO.getCalendarId());
	        	statement.setString(++counter, retailPriceDTO.getItemcode());
	        	statement.setInt(++counter, retailPriceDTO.getLevelTypeId());
	        	statement.setString(++counter, retailPriceDTO.getLevelId());
	        	
	        	statement.addBatch();
	        	itemNoInBatch++;
	        	
	        	if(itemNoInBatch % Constants.BATCH_UPDATE_COUNT == 0){
	        		statement.executeBatch();
	        		statement.clearBatch();
	        		itemNoInBatch = 0;
	        	}
	        }
	        if(itemNoInBatch > 0){
	        	statement.executeBatch();
        		statement.clearBatch();
	        }
	    }
		catch (SQLException e)
		{
			logger.error("Error while executing UPDATE_RETAIL_PRICE_INFO");
			throw new GeneralException("Error while executing UPDATE_RETAIL_PRICE_INFO", e);
		}finally{
			PristineDBUtil.close(statement);
		}
	}
	
	/**
	 * This method deletes retail price data into RETAIL_PRICE_INFO table
	 * @param conn				Connection
	 * @param toBeDeletedList	List of records that needs to be deleted from retail_price_info
	 * @throws GeneralException
	 */
	public void deleteRetailPriceData(Connection conn, List<RetailPriceDTO> toBeDeletedList) throws GeneralException{
		logger.debug("Inside deleteRetailPriceData() of RetailPriceDAO");
		PreparedStatement statement = null;
		try{
			statement = conn.prepareStatement(DELETE_RETAIL_PRICE_INFO);
	        
			int itemNoInBatch = 0;
	        for(RetailPriceDTO retailPriceDTO:toBeDeletedList){
	        	int counter = 0;
	        	statement.setInt(++counter, retailPriceDTO.getCalendarId());
	        	statement.setString(++counter, retailPriceDTO.getItemcode());
	        	statement.setInt(++counter, retailPriceDTO.getLevelTypeId());
	        	statement.setString(++counter, retailPriceDTO.getLevelId());
	        		        	
	        	statement.addBatch();
	        	itemNoInBatch++;
	        	
	        	if(itemNoInBatch % Constants.BATCH_UPDATE_COUNT == 0){
	        		statement.executeBatch();
	        		statement.clearBatch();
	        		itemNoInBatch = 0;
	        	}
	        }
	        if(itemNoInBatch > 0){
	        	statement.executeBatch();
        		statement.clearBatch();
	        }
		}
		catch (SQLException e)
		{
			logger.error("Error while executing DELETE_RETAIL_PRICE_INFO");
			throw new GeneralException("Error while executing DELETE_RETAIL_PRICE_INFO", e);
		}
		finally{
			PristineDBUtil.close(statement);
		}
	}

	
	/**
	 * Delete future cost data.
	 * @param conn
	 * @param recordsToBeDeleted
	 * @throws GeneralException 
	 */
	public void deleteFuturePriceData(Connection conn, List<RetailPriceDTO> recordsToBeDeleted, String weekStartDate) throws GeneralException{
		
		/*
		 * DELETE FROM RETAIL_PRICE_INFO WHERE CALENDAR_ID IN 
		 * (SELECT CALENDAR_ID FROM RETAIL_CALENDAR  
		 * WHERE START_DATE > TO_DATE(?, 'MM/DD/YYYY') AND ROW_TYPE = 'W') 
		 * AND  ITEM_CODE = ? AND LEVEL_ID = ? AND LEVEL_TYPE_ID = ? 
		 * AND (SALE_M_PRICE = 0 OR SALE_PRICE = 0)
		 * */
		
		PreparedStatement statement = null;
		try{
			statement = conn.prepareStatement(DELETE_FUTURE_PRICE);
			int itemsInBatch = 0;
			for(RetailPriceDTO retailPriceDTO: recordsToBeDeleted){
				int colCount = 0;
				statement.setString(++colCount, weekStartDate);
				statement.setString(++colCount, retailPriceDTO.getItemcode());
				statement.setString(++colCount, retailPriceDTO.getLevelId());
				statement.setInt(++colCount, retailPriceDTO.getLevelTypeId());
				statement.addBatch();
				itemsInBatch++;
				if(itemsInBatch % Constants.BATCH_UPDATE_COUNT == 0){
					statement.executeBatch();
					statement.clearBatch();
					itemsInBatch = 0;
				}
			}
			if(itemsInBatch > 0){
				statement.executeBatch();
				statement.clearBatch();
				itemsInBatch = 0;
			}
		}
		catch (SQLException e)
		{
			throw new GeneralException("Error while deleting future cost", e);
		}finally{
			PristineDBUtil.close(statement);
		}
	}
	
	
	/**
	 * This method retrieves Chain Id
	 * @param conn	Connection
	 * @return Chain Id
	 */
	public String getChainId(Connection conn) throws GeneralException{
		logger.debug("Inside getChainId() of RetailPriceDAO");
		PreparedStatement statement = null;
		ResultSet resultSet = null;
		String chainId = null;
		try{
			statement = conn.prepareStatement(GET_CHAINID);
			resultSet = statement.executeQuery();
			if(resultSet.next()){
				chainId = resultSet.getString("COMP_CHAIN_ID");
			}
		}
		catch (SQLException e)
		{
			logger.error("Error while executing GET_CHAINID");
			throw new GeneralException("Error while executing GET_CHAINID", e);
		}
		finally{
			PristineDBUtil.close(resultSet);
			PristineDBUtil.close(statement);
		}
		return chainId;
	}

	/**
	 * Insert/Update store/zone mapping with items
	 * @param values			Contains the input with which mapping needs to be created		
	 * @param itemCodeMap		Contains mapping between upc and item code
	 */
	public void mapItemsWithStore(Connection conn, Collection<List<RetailPriceDTO>> retailPriceColln,
			HashMap<String, String> itemCodeMap, HashMap<String, Integer> storeIdMap,
			HashMap<String, Integer> priceZoneIdMap, Set<String> noItemCodeSet,
			HashMap<String, List<Integer>> deptZoneMap, 
			HashMap<String, Long> vendorIdMap, boolean usePriceZoneMap, HashMap<String, List<Integer>> zoneStoreMap) {
		PreparedStatement statement = null;
		try {
			if(usePriceZoneMap)
				statement = conn.prepareStatement(MERGE_INTO_STORE_ITEM_MAP_WITH_PRC_ZONE);
			else
				statement = conn.prepareStatement(MERGE_INTO_STORE_ITEM_MAP);

			
			String storeItemMapSetupZones = PropertyManager.getProperty("ZONE_ID_TO_BE_SETUP_IN_STM", "");

			Set<String> zonesToBeSetup = new HashSet<>();
			if(storeItemMapSetupZones != Constants.EMPTY) {
				String zoneIds[] = storeItemMapSetupZones.split(",");
				for(String zoneId: zoneIds) {
					zonesToBeSetup.add(zoneId);
				}
			}
			
			int itemNoInBatch = 0;
			int cnt = 0;
			for (List<RetailPriceDTO> retailPriceDTOList : retailPriceColln) {
				for (RetailPriceDTO retailPriceDTO : retailPriceDTOList) {
					if (!checkRetailerItemCode)
						retailPriceDTO.setItemcode(itemCodeMap.get(PrestoUtil.castUPC(retailPriceDTO.getUpc(), false)));
					else {
						String key = PrestoUtil.castUPC(retailPriceDTO.getUpc(), false) + "-"
								+ retailPriceDTO.getRetailerItemCode();
						if (itemCodeMap.get(key) != null)
							retailPriceDTO.setItemcode(String.valueOf(itemCodeMap.get(key)));
						else
							continue;
					}
					//SELECT ? LEVEL_TYPE_ID, ? LEVEL_ID, ? ITEM_CODE, ? PRICE_INDICATOR, ? DIST_FLAG, ? VENDOR_ID, ? PRICE_ZONE_ID
					if (retailPriceDTO.getItemcode() != null) {
						int counter = 0;
						long vendorId = 0;
						Integer levelId = null;
						if (retailPriceDTO.getLevelTypeId() == 1)
							levelId = priceZoneIdMap.get(retailPriceDTO.getLevelId());
						else if (retailPriceDTO.getLevelTypeId() == 2)
							levelId = storeIdMap.get(retailPriceDTO.getLevelId());

						if (levelId != null) {
							if (retailPriceDTO.getLevelTypeId() == Constants.ZONE_LEVEL_TYPE_ID) {

								// If zones are setup in config, load store item map only for given zones
								if(zonesToBeSetup.size() > 0 && !zonesToBeSetup.contains(String.valueOf(levelId))){
									continue;
								}
								
								addBatchForZoneStoreLevel(statement, retailPriceDTO, levelId, vendorIdMap,
										usePriceZoneMap, priceZoneIdMap, Constants.ZONE_LEVEL_TYPE_ID);
								cnt++;
								if (zoneStoreMap != null) {
									if (zoneStoreMap.containsKey(retailPriceDTO.getLevelId())) {
										for (int storeId : zoneStoreMap.get(retailPriceDTO.getLevelId())) {
											levelId = storeId;
											// retailPriceDTO.setLevelTypeId(Constants.STORE_LEVEL_TYPE_ID);
											addBatchForZoneStoreLevel(statement, retailPriceDTO, levelId, vendorIdMap,
													usePriceZoneMap, priceZoneIdMap, Constants.STORE_LEVEL_TYPE_ID);

											itemNoInBatch++;
											cnt++;
											if (itemNoInBatch % Constants.BATCH_UPDATE_COUNT == 0) {
												statement.executeBatch();
												statement.clearBatch();
												itemNoInBatch = 0;
											}
											logProgress(cnt);
										}
									}
								}
							} else if (retailPriceDTO.getLevelTypeId() == Constants.STORE_LEVEL_TYPE_ID) {

								addBatchForZoneStoreLevel(statement, retailPriceDTO, levelId, vendorIdMap,
										usePriceZoneMap, priceZoneIdMap, Constants.STORE_LEVEL_TYPE_ID);

								itemNoInBatch++;
								cnt++;
								if (itemNoInBatch % Constants.BATCH_UPDATE_COUNT == 0) {
									statement.executeBatch();
									statement.clearBatch();
									itemNoInBatch = 0;
								}
								logProgress(cnt);
							}

							
							
							// statement.addBatch();

						} else {
							if (retailPriceDTO.getLevelTypeId() == 1) {
								// This could be a dept zone level record
								// Processing starts for dept zone level record
								List<Integer> levelIdList = deptZoneMap.get(retailPriceDTO.getLevelId());
								if (levelIdList != null && levelIdList.size() > 0) {
									for (int levelIdTemp : levelIdList) {
										counter = 0;
										statement.setInt(++counter, retailPriceDTO.getLevelTypeId());
										statement.setInt(++counter, levelIdTemp);
										statement.setString(++counter, retailPriceDTO.getItemcode());
										statement.setString(++counter, Constants.PRICE_INDICATOR);
										if (retailPriceDTO.getVendorNumber() == null
												|| Constants.EMPTY.equals(retailPriceDTO.getVendorNumber())) {
											statement.setString(++counter, Constants.EMPTY + Constants.WAREHOUSE);
										} else {
											statement.setString(++counter, Constants.EMPTY + Constants.DSD);
										}
										if (vendorIdMap.containsKey(retailPriceDTO.getVendorNumber())) {
											vendorId = vendorIdMap.get(retailPriceDTO.getVendorNumber());
										}
										if (vendorId != 0) {
											statement.setLong(++counter, vendorId);
										} else {
											statement.setLong(++counter, 1);
										}
										
										if (!usePriceZoneMap) {
											statement.setString(++counter, String.valueOf(Constants.YES));
										}
										
										if (usePriceZoneMap) {
											if (priceZoneIdMap.get(retailPriceDTO.getZoneNbr()) == null) {
												statement.setNull(++counter, Types.NULL);
											} else {
												statement.setInt(++counter,
														priceZoneIdMap.get(retailPriceDTO.getZoneNbr()));
											}
										}
										statement.addBatch();
										itemNoInBatch++;
										cnt++;
										if (itemNoInBatch % Constants.BATCH_UPDATE_COUNT == 0) {
											statement.executeBatch();
											statement.clearBatch();
											itemNoInBatch = 0;
										}
										logProgress(cnt);
									}
								}
								// Processing for dept zone level record ends
							}
						}

						
						/*try {
							if (itemNoInBatch > 0) {
								statement.executeBatch();
								statement.clearBatch();
								itemNoInBatch = 0;
							}
							if (cnt % 25000 == 0) {
								conn.commit();
							}
						} catch (SQLException exception) {
							logger.error("Error while executing MERGE_STORE_ITEM_MAP" + exception);
						}*/
					} else {
						noItemCodeSet.add(retailPriceDTO.getUpc());
					}
				}
			}
			if (itemNoInBatch > 0) {
				statement.executeBatch();
				statement.clearBatch();
			}
		} catch (SQLException e) {
			logger.error("Error while executing MERGE_STORE_ITEM_MAP" + e);
		} finally {
			PristineDBUtil.close(statement);
		}

	}
	
	private void logProgress(int cnt) {
		if (cnt % 100000 == 0) {
			logger.info("Number of records processed  - " + cnt);
		}

	}
	
	
	
	private void addBatchForZoneStoreLevel(PreparedStatement statement, 
			RetailPriceDTO retailPriceDTO, int levelId,
			HashMap<String, Long> vendorIdMap, boolean usePriceZoneMap, 
			HashMap<String, Integer> priceZoneIdMap, int levelTypeId)
					throws SQLException {
		int counter = 0;
		long vendorId = 0;
		statement.setInt(++counter, levelTypeId);
		statement.setInt(++counter, levelId);
		statement.setString(++counter, retailPriceDTO.getItemcode());
		statement.setString(++counter, Constants.PRICE_INDICATOR);
		if (retailPriceDTO.getVendorNumber() == null || Constants.EMPTY.equals(retailPriceDTO.getVendorNumber())) {
			statement.setString(++counter, Constants.EMPTY + Constants.WAREHOUSE);
		} else {
			statement.setString(++counter, Constants.EMPTY + Constants.DSD);
		}
		if (vendorIdMap.containsKey(retailPriceDTO.getVendorNumber())) {
			vendorId = vendorIdMap.get(retailPriceDTO.getVendorNumber());
		}
		if (vendorId != 0) {
			statement.setLong(++counter, vendorId);
		} else {
			statement.setLong(++counter, 1);
		}
		
		
		// Authorization flag
		if(!usePriceZoneMap) {
			statement.setString(++counter, String.valueOf(Constants.YES));
		}
		
		if (usePriceZoneMap) {
			if (priceZoneIdMap.get(retailPriceDTO.getZoneNbr()) == null) {
				statement.setNull(++counter, Types.NULL);
			} else {
				statement.setInt(++counter, priceZoneIdMap.get(retailPriceDTO.getZoneNbr()));
			}
		}
		
		statement.addBatch();
	}
	
	/**
	 * Insert/Update store/zone mapping with items
	 * @param values			Contains the input with which mapping needs to be created		
	 */
	public void mapItemsWithStore(Connection conn, List<RetailPriceDTO> retailPriceDTOList){
		logger.debug("Inside mapItemsWithStore() of RetailPriceDAO");
		PreparedStatement statement = null;
		try{
			statement = conn.prepareStatement(MERGE_INTO_STORE_ITEM_MAP);
			
			int itemNoInBatch = 0;
		    for(RetailPriceDTO retailPriceDTO:retailPriceDTOList){
		    	int counter = 0;
			    statement.setInt(++counter, retailPriceDTO.getLevelTypeId());
			    statement.setString(++counter, retailPriceDTO.getLevelId());
			    statement.setString(++counter, retailPriceDTO.getItemcode());
			        
			    statement.addBatch();
			    itemNoInBatch++;
			        	
			    if(itemNoInBatch % Constants.BATCH_UPDATE_COUNT == 0){
			    	statement.executeBatch();
			        statement.clearBatch();
			        itemNoInBatch = 0;
			    }
		    }
			if(itemNoInBatch > 0){
	        	statement.executeBatch();
        		statement.clearBatch();
	        }
	    }
		catch (SQLException e){
			logger.error("Error while executing MERGE_INTO_STORE_ITEM_MAP - " + e);
		}finally{
			PristineDBUtil.close(statement);
		}
		
	}
	
	/**
	 * This method retrieves a map containing dept zone as key and list of its corresponding store number as value
	 * @param 	Connection	Database connection
	 * @param	String		Chain Id
	 * @return	HashMap
	 */
	public HashMap<String,List<String>> getDeptZoneMap(Connection conn, String chainId) throws GeneralException{
		logger.debug("Inside getDeptZoneMap() of RetailPriceDAO");
		HashMap<String,List<String>> deptZoneMap = new HashMap<String, List<String>>();
		PreparedStatement statement = null;
	    ResultSet resultSet = null;
		try{
			statement = conn.prepareStatement(GET_DEPT_ZONE_INFO);
	        statement.setString(1, chainId);
	        resultSet = statement.executeQuery();
	        while(resultSet.next()){
	        	String dept1ZoneNo = resultSet.getString("DEPT1_ZONE_NO");
	        	String dept2ZoneNo = resultSet.getString("DEPT2_ZONE_NO");
	        	String dept3ZoneNo = resultSet.getString("DEPT3_ZONE_NO");
	        	String compStrNo = resultSet.getString("COMP_STR_NO");
	        	
	        	if(dept1ZoneNo != null){
	        		List<String> tempList = null;
	        		if(deptZoneMap.get(dept1ZoneNo) != null){
	        			tempList = deptZoneMap.get(dept1ZoneNo);
	        			tempList.add(compStrNo);
	        		}else{
	        			tempList = new ArrayList<String>();
	        			tempList.add(compStrNo);
	        		}
	        		deptZoneMap.put(dept1ZoneNo, tempList);
	        	}
	        	
	        	if(dept2ZoneNo != null){
	        		List<String> tempList = null;
	        		if(deptZoneMap.get(dept2ZoneNo) != null){
	        			tempList = deptZoneMap.get(dept2ZoneNo);
	        			tempList.add(compStrNo);
	        		}else{
	        			tempList = new ArrayList<String>();
	        			tempList.add(compStrNo);
	        		}
	        		deptZoneMap.put(dept2ZoneNo, tempList);
	        	}
	        	
	        	if(dept3ZoneNo != null){
	        		List<String> tempList = null;
	        		if(deptZoneMap.get(dept3ZoneNo) != null){
	        			tempList = deptZoneMap.get(dept3ZoneNo);
	        			tempList.add(compStrNo);
	        		}else{
	        			tempList = new ArrayList<String>();
	        			tempList.add(compStrNo);
	        		}
	        		deptZoneMap.put(dept3ZoneNo, tempList);
	        	}
	        }
		}
		catch (SQLException e)
		{
			logger.error("Error while executing GET_DEPT_ZONE_INFO");
			throw new GeneralException("Error while executing GET_DEPT_ZONE_INFO", e);
		}
		finally{
			PristineDBUtil.close(resultSet);
			PristineDBUtil.close(statement);
		}
		return deptZoneMap;
	}
	
	/**
	 * This method retrieves a map containing dept zone as key and list of its corresponding store number as value
	 * @param 	Connection	Database connection
	 * @param	String		Chain Id
	 * @return	HashMap
	 */
	public HashMap<String,List<String>> getStoreDeptZoneMap(Connection conn, String chainId) throws GeneralException{
		logger.debug("Inside getDeptZoneMap() of RetailPriceDAO");
		HashMap<String,List<String>> storeDeptZoneMap = new HashMap<String, List<String>>();
		List<String> deptZoneList = null;
		PreparedStatement statement = null;
	    ResultSet resultSet = null;
		try{
			statement = conn.prepareStatement(GET_DEPT_ZONE_INFO);
	        statement.setString(1, chainId);
	        resultSet = statement.executeQuery();
	        while(resultSet.next()){
	        	deptZoneList = new ArrayList<String>();
	        	String dept1ZoneNo = resultSet.getString("DEPT1_ZONE_NO");
	        	String dept2ZoneNo = resultSet.getString("DEPT2_ZONE_NO");
	        	String dept3ZoneNo = resultSet.getString("DEPT3_ZONE_NO");
	        	String compStrNo = resultSet.getString("COMP_STR_NO");
	        	
	        	if(dept1ZoneNo != null)
	        		deptZoneList.add(dept1ZoneNo);
	        	
	        	if(dept2ZoneNo != null)
	        		deptZoneList.add(dept2ZoneNo);
	        	
	        	if(dept3ZoneNo != null)
	        		deptZoneList.add(dept3ZoneNo);
	        	
	        	if(deptZoneList.size() > 0)
	        		storeDeptZoneMap.put(compStrNo, deptZoneList);
	        }
		}
		catch (SQLException e)
		{
			logger.error("Error while executing GET_DEPT_ZONE_INFO");
			throw new GeneralException("Error while executing GET_DEPT_ZONE_INFO", e);
		}
		finally{
			PristineDBUtil.close(resultSet);
			PristineDBUtil.close(statement);
		}
		return storeDeptZoneMap;
	}
	
	/**
	 * Deletes data from retail_price_info table for given calendar id
	 * @param conn			Database connection
	 * @param calendarId	Calendar Id
	 * @throws GeneralException
	 */
	public void deleteRetailPriceData(Connection conn, int calendarId) throws GeneralException{
		logger.debug("Inside deleteRetailCostData() of RetailPriceDAO");
		PreparedStatement statement = null;
		try{
			statement = conn.prepareStatement(DELETE_RETAIL_PRICE_INFO_WEEKLY);
	        
	        statement.setInt(1, calendarId);
	        statement.executeUpdate();
		}
		catch (SQLException e)
		{
			logger.error("Error while executing DELETE_RETAIL_PRICE_INFO_WEEKLY");
			throw new GeneralException("Error while executing DELETE_RETAIL_PRICE_INFO_WEEKLY", e);
		}
		finally{
			PristineDBUtil.close(statement);
		}
	}
	
	/**
	 * This method deletes retail price data into RETAIL_COST_INFO table
	 * @param conn				Connection
	 * @param toBeDeletedList	List of records that needs to be deleted from retail_price_info
	 * @throws GeneralException
	 */
	public void deleteRetailPriceData(Connection conn, int calendarId, List<String> itemCodeList) throws GeneralException{
		logger.debug("Inside deleteRetailPriceData() of RetailPriceDAO");
		PreparedStatement statement = null;
		try{
			statement = conn.prepareStatement(DELETE_RETAIL_PRICE_INFO_WEEKLY_ITEMS);
	        
			int itemNoInBatch = 0;
	        for(String itemCode : itemCodeList){
	        	int counter = 0;
	        	statement.setInt(++counter, calendarId);
	        	statement.setString(++counter, itemCode);
	        		        	
	        	statement.addBatch();
	        	itemNoInBatch++;
	        	
	        	if(itemNoInBatch % Constants.BATCH_UPDATE_COUNT == 0){
	        		statement.executeBatch();
	        		logger.debug("Batch executed");
	        		statement.clearBatch();
	        		itemNoInBatch = 0;
	        	}
	        }
	        if(itemNoInBatch > 0){
	        	statement.executeBatch();
        		statement.clearBatch();
	        }
		}
		catch (SQLException e)
		{
			logger.error("Error while executing DELETE_RETAIL_PRICE_INFO_WEEKLY_ITEMS");
			throw new GeneralException("Error while executing DELETE_RETAIL_PRICE_INFO_WEEKLY_ITEMS", e);
		}
		finally{
			PristineDBUtil.close(statement);
		}
	}
	/**
	 * Setup retail cost data for given calendar id from previous week's calendar id
	 * @param conn			Database connection
	 * @param calendarId	Calendar Id
	 * @throws GeneralException
	 */
	public boolean setupRetailPriceData(Connection conn, int calendarId, int prevWkCalendarId) throws GeneralException{
		logger.debug("Inside setupRetailCostData() of RetailPriceDAO");
		PreparedStatement statement = null;
		int count = 0;
		try{
			statement = conn.prepareStatement(SETUP_RETAIL_PRICE_DATA);
	        statement.setInt(1, calendarId);
	        statement.setInt(2, prevWkCalendarId);
	        count = statement.executeUpdate();
		}
		catch (SQLException e)
		{
			logger.error("Error while executing SETUP_RETAIL_PRICE_DATA");
			throw new GeneralException("Error while executing SETUP_RETAIL_PRICE_DATA", e);
		}
		finally{
			PristineDBUtil.close(statement);
		}
		return count > 0;
	}
	
	/**
	 * This method queries price of an item in a store
	 * @param conn				Connection
	 * @param prevCalendarId	Previous Calendar Id
	 * @param calendarId		Calendar Id
	 * @param itemCode			Item Code
	 * @param storeNo			Store Number
	 * @return
	 * @throws GeneralException
	 */
	public HashMap<Integer, List<RetailPriceDTO>> getItemPriceInStore(Connection conn, int prevCalendarId, int calendarId, String itemCode, String storeNo) throws GeneralException{
		logger.debug("Inside getItemPriceInStore() of RetailPriceDAO");
		PreparedStatement statement = null;
	    ResultSet resultSet = null;
	    HashMap<Integer, List<RetailPriceDTO>> retailPriceDTOMap = new HashMap<Integer, List<RetailPriceDTO>>();
	    
		try{
			long startTime = System.currentTimeMillis();
			
			statement = conn.prepareStatement(GET_ITEM_PRICE_IN_STORE);
			statement.setString(1, itemCode);
			statement.setInt(2, calendarId);
		    statement.setString(3, storeNo);
		    statement.setString(4, storeNo);
		    statement.setString(5, storeNo);
		    statement.setString(6, storeNo);
		    statement.setString(7, storeNo);
		    statement.setString(8, storeNo);

			resultSet = statement.executeQuery();
	        
	        RetailPriceDTO retailPriceDTO;
	        while(resultSet.next()){
	        	retailPriceDTO = new RetailPriceDTO();
	        	retailPriceDTO.setRegEffectiveDate(resultSet.getString("START_DATE"));
	        	retailPriceDTO.setItemcode(resultSet.getString("ITEM_CODE"));
	        	retailPriceDTO.setCalendarId(resultSet.getInt("CALENDAR_ID"));
	        	retailPriceDTO.setLevelTypeId(resultSet.getInt("LEVEL_TYPE_ID"));
	        	retailPriceDTO.setLevelId(resultSet.getString("LEVEL_ID"));
	        	retailPriceDTO.setRegPrice(resultSet.getFloat("REG_PRICE"));
	        	retailPriceDTO.setRegQty(resultSet.getInt("REG_QTY"));
	        	retailPriceDTO.setRegMPrice(resultSet.getFloat("REG_M_PRICE"));
	        	retailPriceDTO.setSalePrice(resultSet.getFloat("SALE_PRICE"));
	        	retailPriceDTO.setSaleQty(resultSet.getInt("SALE_QTY"));
	        	retailPriceDTO.setSaleMPrice(resultSet.getFloat("SALE_M_PRICE"));
	        	retailPriceDTO.setPromotionFlag(resultSet.getString("PROMOTION_FLG"));
	        	if(retailPriceDTOMap.get(retailPriceDTO.getCalendarId()) != null){
	        		List<RetailPriceDTO> tempList = retailPriceDTOMap.get(retailPriceDTO.getCalendarId());
	        		tempList.add(retailPriceDTO);
	        		retailPriceDTOMap.put(retailPriceDTO.getCalendarId(), tempList);
	        	}else{
	        		List<RetailPriceDTO> tempList = new ArrayList<RetailPriceDTO>();
	        		tempList.add(retailPriceDTO);
	        		retailPriceDTOMap.put(retailPriceDTO.getCalendarId(), tempList);
	        	}
	        }
	        long endTime = System.currentTimeMillis();
	        logger.debug("Time taken to execute and store price info in list- " + (endTime - startTime));
		}
		catch (SQLException e)
		{
			logger.error("Error while executing GET_ITEM_PRICE_IN_STORE");
			throw new GeneralException("Error while executing GET_ITEM_PRICE_IN_STORE", e);
		}finally{
			PristineDBUtil.close(resultSet);
			PristineDBUtil.close(statement);
		}
		return retailPriceDTOMap;
	}
	
	/**
	 * This method queries the database for Zone numbers and is Zone Ids
	 * @param conn					Connection
	 * @throws GeneralException
	 */
	public HashMap<String, Integer> getPriceZoneData(Connection conn) throws GeneralException{
		PreparedStatement statement = null;
	    ResultSet resultSet = null;
	    HashMap<String, Integer> zoneIdMap = new HashMap<String, Integer>();
		try{
			statement = conn.prepareStatement(GET_ZONE_INFO);
			resultSet = statement.executeQuery();
	        while(resultSet.next()){
	        	zoneIdMap.put(resultSet.getString("ZONE_NUM"), resultSet.getInt("PRICE_ZONE_ID"));
	        }
		}
		catch (SQLException e)
		{
			logger.error("Error while executing GET_ZONE_INFO");
			throw new GeneralException("Error while executing GET_ZONE_INFO", e);
		}
		finally{
			PristineDBUtil.close(resultSet);
			PristineDBUtil.close(statement);
		}
		return zoneIdMap;
	}
	
	/**
	 * This method is used to retrieve Store Id for the Store Number passed in the Set
	 * @param conn			Connection
	 * @param storeIdSet	Set containing list of Store Numbers for which Store Id needs to be retrieved
	 * @return HashMap		Contains Store Number as key and Store Id as value
	 * @throws GeneralException
	 */
	public HashMap<String, Integer> getStoreId(Connection conn, int chainId, Set<String> storeNbrSet) throws GeneralException{
		
		int limitcount=0;
		List<String> storeIdList = new ArrayList<String>();
		
	    HashMap<String, Integer> storeIdMapping = new HashMap<String, Integer>();
		for(String storeNbr:storeNbrSet){
			storeIdList.add(storeNbr);
			limitcount++;
			if( limitcount > 0 && (limitcount%this.commitCount == 0)){
				Object[] values = storeIdList.toArray();
				retrieveStoreId(conn, chainId, storeIdMapping, values);
				storeIdList.clear();
            }
		}
		if(storeIdList.size() > 0){
			Object[] values = storeIdList.toArray();
			retrieveStoreId(conn, chainId, storeIdMapping, values);
			storeIdList.clear();
		}
		
		return storeIdMapping;
	}
	
	
	
	
	/**
	 * This method queries the database for Store Id for every set of Store Numbers
	 * @param conn					Connection
	 * @param storeIdMapping		Map that will contain the result of the database retrieval
	 * @param values				Array of UPCs that will be passed as input to the query
	 * @throws GeneralException
	 */
	private void retrieveStoreId(Connection conn, int chainId, HashMap<String, Integer> storeIdMapping, Object... values) throws GeneralException{
		PreparedStatement statement = null;
	    ResultSet resultSet = null;
		try{
			statement = conn.prepareStatement(String.format(GET_STORE_ID, preparePlaceHolders(values.length)));
			statement.setInt(1, chainId);
	        PristineDBUtil.setValues(statement, 2, values);
	        resultSet = statement.executeQuery();
	        while(resultSet.next()){
	        	storeIdMapping.put(resultSet.getString("COMP_STR_NO"), resultSet.getInt("COMP_STR_ID"));
	        }
		}
		catch (SQLException e)
		{
			logger.error("Error while executing GET_STORE_ID");
			throw new GeneralException("Error while executing GET_STORE_ID", e);
		}
		finally{
			PristineDBUtil.close(resultSet);
			PristineDBUtil.close(statement);
		}
	}
	
	/**
	 * This method creates a  map with store number as key and store id as value.
	 * @param conn					Connection
	 * @param chainId
	 * @throws GeneralException
	 */
	public HashMap<String, Integer> getStoreIdMap(Connection conn, int chainId) throws GeneralException{
		HashMap<String, Integer> storeIdMap =  new HashMap<String, Integer>();
		PreparedStatement statement = null;
		ResultSet resultSet = null;
		try{
			statement = conn.prepareStatement(GET_STORE_ID_MAP);
			statement.setInt(1, chainId);
			resultSet = statement.executeQuery();
			while(resultSet.next()){
				String storeNumber = resultSet.getString("COMP_STR_NO");
				int storeId = resultSet.getInt("COMP_STR_ID");
				storeIdMap.put(storeNumber, storeId);
			}
		}
		catch (SQLException e)
		{
			logger.error("getStoreIdMap() - Error while store id map.");
			throw new GeneralException("getStoreIdMap() - Error while store id map.", e);
		}
		finally{
			PristineDBUtil.close(resultSet);
			PristineDBUtil.close(statement);
		}
		
		return storeIdMap;
	}
	
	/**
	 * This method creates a  map with zone number as key and zone id as value.
	 * @param conn					Connection
	 * @param chainId
	 * @throws GeneralException
	 */
	public HashMap<String, Integer> getRetailPriceZone(Connection conn) throws GeneralException{
		HashMap<String, Integer> zoneMap =  new HashMap<String, Integer>();
		PreparedStatement statement = null;
		ResultSet resultSet = null;
		try{
			statement = conn.prepareStatement(GET_RETAIL_PRICE_ZONE);
			resultSet = statement.executeQuery();
			while(resultSet.next()){
				String zoneNumber = resultSet.getString("ZONE_NUM");
				int zoneId = resultSet.getInt("PRICE_ZONE_ID");
				zoneMap.put(zoneNumber, zoneId);
			}
		}
		catch (SQLException e)
		{
			logger.error("getRetailPriceZone() - Error while getting retail price zone.");
			throw new GeneralException("getRetailPriceZone() - Error while getting retail price zone.", e);
		}
		finally{
			PristineDBUtil.close(resultSet);
			PristineDBUtil.close(statement);
		}
		
		return zoneMap;
	}
	
	
	
	/**
	 * This method inserts retail price data in RETAIL_PRICE_INFO table
	 * @param conn					Connection
	 * @param toBeInsertedList		List of records that needs to be inserted in retail_price_info
	 * @throws GeneralException
	 */
	public void insertRetailSalePriceData(Connection conn, List<RetailPriceDTO> toBeInsertedList) throws GeneralException{
		
		PreparedStatement statement = null;
	    try{
			statement = conn.prepareStatement(INSERT_RETAIL_SALE_PRICE);
	        
			int itemNoInBatch = 0;
	        for(RetailPriceDTO retailPriceDTO:toBeInsertedList){
	        	int counter = 0;
	        	statement.setString(++counter, retailPriceDTO.getLevelId());
	        	statement.setString(++counter, retailPriceDTO.getUpc());
	        	statement.setFloat(++counter, retailPriceDTO.getSalePrice());
	        	statement.setInt(++counter, retailPriceDTO.getSaleQty());
	        	statement.setString(++counter, retailPriceDTO.getSaleStartDate());
	        	statement.setString(++counter, retailPriceDTO.getSaleEndDate());
	        	statement.addBatch();
	        	itemNoInBatch++;
	        	
	        	if(itemNoInBatch % Constants.BATCH_UPDATE_COUNT == 0){
	        		statement.executeBatch();
	        		statement.clearBatch();
	        		itemNoInBatch = 0;
	        	}
	        }
	        if(itemNoInBatch > 0){
	        	statement.executeBatch();
        		statement.clearBatch();
	        }
		}
		catch (SQLException e)
		{
			logger.error("Error while executing INSERT_RETAIL_SALE_PRICE_INFO");
			throw new GeneralException("Error while executing INSERT_RETAIL_SALE_PRICE_INFO" + e);
		}finally{
			PristineDBUtil.close(statement);
		}
	}
	
	/**
	 * Retrieves sale price data from Retail_Sale_Price_temp based on department
	 * @param conn
	 * @param deptProductId
	 * @return
	 * @throws GeneralException
	 */
	public HashMap<String, HashMap<String, RetailPriceDTO>> getRetailSalePrice(Connection conn, int deptProductId) throws GeneralException{
		PreparedStatement statement = null;
	    ResultSet resultSet = null;
	    HashMap<String, HashMap<String, RetailPriceDTO>> salePriceMap = new HashMap<String, HashMap<String, RetailPriceDTO>>();
	    
		try{
			long startTime = System.currentTimeMillis();
			
			statement = conn.prepareStatement(GET_RETAIL_SALE_PRICE_FOR_DEPT);
			statement.setInt(1, deptProductId);
		   
			statement.setFetchSize(100000);
			resultSet = statement.executeQuery();
	        
	        RetailPriceDTO retailPriceDTO;
	        while(resultSet.next()){
	        	retailPriceDTO = new RetailPriceDTO();
	        	retailPriceDTO.setUpc(resultSet.getString("UPC"));
	        	retailPriceDTO.setLevelId(resultSet.getString("COMP_STR_NO"));
	        	retailPriceDTO.setSalePrice(resultSet.getFloat("SALE_PRICE"));
	        	retailPriceDTO.setSaleQty(resultSet.getInt("SALE_QTY"));
	        	retailPriceDTO.setSaleStartDate(resultSet.getString("SALE_START_DATE"));
	        	retailPriceDTO.setSaleEndDate(resultSet.getString("SALE_END_DATE"));
	        	if(salePriceMap.get(retailPriceDTO.getUpc()) != null){
	        		HashMap<String, RetailPriceDTO> tempMap = salePriceMap.get(retailPriceDTO.getUpc());
	        		tempMap.put(retailPriceDTO.getLevelId(), retailPriceDTO);
	        		salePriceMap.put(retailPriceDTO.getUpc(), tempMap);
	        	}else{
	        		HashMap<String, RetailPriceDTO> tempMap = new HashMap<String, RetailPriceDTO>();
	        		tempMap.put(retailPriceDTO.getLevelId(), retailPriceDTO);
	        		salePriceMap.put(retailPriceDTO.getUpc(), tempMap);
	        	}
	        }
	        long endTime = System.currentTimeMillis();
	        logger.debug("Time taken to execute and get sale price- " + (endTime - startTime));
		}
		catch (SQLException e)
		{
			logger.error("Error while executing GET_RETAIL_SALE_PRICE_FOR_DEPT");
			throw new GeneralException("Error while executing GET_RETAIL_SALE_PRICE_FOR_DEPT", e);
		}finally{
			PristineDBUtil.close(resultSet);
			PristineDBUtil.close(statement);
		}
		return salePriceMap;

	}
	
	/**
	 * Truncates Retail_Sale_Price_Temp table
	 * @param conn
	 * @throws GeneralException
	 */
	public void truncateRetailSalePrice(Connection conn) throws GeneralException{
		PreparedStatement statement = null;
	    
		try{
			statement = conn.prepareStatement(TRUNCATE_RETAIL_SALE_PRICE);
			
			statement.executeQuery();
		}
		catch (SQLException e)
		{
			logger.error("Error while executing TRUNCATE_RETAIL_SALE_PRICE");
			throw new GeneralException("Error while executing TRUNCATE_RETAIL_SALE_PRICE", e);
		}finally{
			PristineDBUtil.close(statement);
		}
	}
	
	public HashMap<String ,List<Integer>> getDeptZoneInfo(Connection conn, String chainId) throws GeneralException{
		logger.debug("Inside getDeptZoneInfo() of RetailPriceDAO");
		HashMap<String ,List<Integer>> deptZoneMap = new HashMap<String ,List<Integer>>();
		PreparedStatement statement = null;
	    ResultSet resultSet = null;
		try{
			statement = conn.prepareStatement(GET_DEPT_ZONE);
	        statement.setString(1, chainId);
	        resultSet = statement.executeQuery();
	        while(resultSet.next()){
	        	String dept1ZoneNo = resultSet.getString("DEPT1_ZONE_NO");
	        	String dept2ZoneNo = resultSet.getString("DEPT2_ZONE_NO");
	        	String dept3ZoneNo = resultSet.getString("DEPT3_ZONE_NO");
	        	int compStrId = resultSet.getInt("COMP_STR_ID");
	        	
	        	if(dept1ZoneNo != null){
	        		List<Integer> tempList = null;
	        		if(deptZoneMap.get(dept1ZoneNo) != null){
	        			tempList = deptZoneMap.get(dept1ZoneNo);
	        			tempList.add(compStrId);
	        		}else{
	        			tempList = new ArrayList<Integer>();
	        			tempList.add(compStrId);
	        		}
	        		deptZoneMap.put(dept1ZoneNo, tempList);
	        	}
	        	
	        	if(dept2ZoneNo != null){
	        		List<Integer> tempList = null;
	        		if(deptZoneMap.get(dept2ZoneNo) != null){
	        			tempList = deptZoneMap.get(dept2ZoneNo);
	        			tempList.add(compStrId);
	        		}else{
	        			tempList = new ArrayList<Integer>();
	        			tempList.add(compStrId);
	        		}
	        		deptZoneMap.put(dept2ZoneNo, tempList);
	        	}
	        	
	        	if(dept3ZoneNo != null){
	        		List<Integer> tempList = null;
	        		if(deptZoneMap.get(dept3ZoneNo) != null){
	        			tempList = deptZoneMap.get(dept3ZoneNo);
	        			tempList.add(compStrId);
	        		}else{
	        			tempList = new ArrayList<Integer>();
	        			tempList.add(compStrId);
	        		}
	        		deptZoneMap.put(dept3ZoneNo, tempList);
	        	}
	        }
		}
		catch (SQLException e)
		{
			logger.error("Error while executing GET_DEPT_ZONE " + e);
			throw new GeneralException("Error while executing GET_DEPT_ZONE", e);
		}
		finally{
			PristineDBUtil.close(resultSet);
			PristineDBUtil.close(statement);
		}
		return deptZoneMap;
	}
	
	public RetailPriceDTO getChainLevelPrice(Connection conn, int calendarId, int itemCode){
		PreparedStatement statement = null;
	    ResultSet resultSet = null;
	    RetailPriceDTO rpDTO = null;
		try{
			statement = conn.prepareStatement(GET_CHAIN_LEVEL_PRICE);
	        statement.setInt(1, calendarId);
	        statement.setInt(2, itemCode);
	        resultSet = statement.executeQuery();
	        if(resultSet.next()){
	        	rpDTO = new RetailPriceDTO();
	        	rpDTO.setRegQty(resultSet.getInt("REG_QTY"));
	        	rpDTO.setRegPrice(resultSet.getFloat("REG_PRICE"));
	        	rpDTO.setRegMPrice(resultSet.getFloat("REG_M_PRICE"));
	        	rpDTO.setSaleQty(resultSet.getInt("SALE_QTY"));
	        	rpDTO.setSalePrice(resultSet.getFloat("SALE_PRICE"));
	        	rpDTO.setSaleMPrice(resultSet.getFloat("SALE_M_PRICE"));
	        }
		}
		catch (SQLException e)
		{
			logger.error("Error while executing GET_CHAIN_LEVEL_PRICE " + e);
		}
		finally{
			PristineDBUtil.close(resultSet);
			PristineDBUtil.close(statement);
		}
		return rpDTO;
	}
	
	public HashMap<String ,List<String>> getDeptZoneInfo(Connection conn, String chainId, int priceZoneId) throws GeneralException{
		logger.debug("Inside getDeptZoneInfo() of RetailPriceDAO");
		HashMap<String ,List<String>> deptZoneMap = new HashMap<String ,List<String>>();
		PreparedStatement statement = null;
	    ResultSet resultSet = null;
		try{
			statement = conn.prepareStatement(GET_DEPT_ZONE_FOR_STORES_IN_ZONE);
	        statement.setString(1, chainId);
	        statement.setInt(2, priceZoneId);
	        resultSet = statement.executeQuery();
	        while(resultSet.next()){
	        	String dept1ZoneNo = resultSet.getString("DEPT1_ZONE_NO");
	        	String dept2ZoneNo = resultSet.getString("DEPT2_ZONE_NO");
	        	String dept3ZoneNo = resultSet.getString("DEPT3_ZONE_NO");
	        	String compStrNo = resultSet.getString("COMP_STR_NO");
	        	
	        	if(dept1ZoneNo != null){
	        		List<String> tempList = null;
	        		if(deptZoneMap.get(dept1ZoneNo) != null){
	        			tempList = deptZoneMap.get(dept1ZoneNo);
	        			tempList.add(compStrNo);
	        		}else{
	        			tempList = new ArrayList<String>();
	        			tempList.add(compStrNo);
	        		}
	        		deptZoneMap.put(dept1ZoneNo, tempList);
	        	}
	        	
	        	if(dept2ZoneNo != null){
	        		List<String> tempList = null;
	        		if(deptZoneMap.get(dept2ZoneNo) != null){
	        			tempList = deptZoneMap.get(dept2ZoneNo);
	        			tempList.add(compStrNo);
	        		}else{
	        			tempList = new ArrayList<String>();
	        			tempList.add(compStrNo);
	        		}
	        		deptZoneMap.put(dept2ZoneNo, tempList);
	        	}
	        	
	        	if(dept3ZoneNo != null){
	        		List<String> tempList = null;
	        		if(deptZoneMap.get(dept3ZoneNo) != null){
	        			tempList = deptZoneMap.get(dept3ZoneNo);
	        			tempList.add(compStrNo);
	        		}else{
	        			tempList = new ArrayList<String>();
	        			tempList.add(compStrNo);
	        		}
	        		deptZoneMap.put(dept3ZoneNo, tempList);
	        	}
	        }
		}
		catch (SQLException e)
		{
			logger.error("Error while executing GET_DEPT_ZONE " + e);
			throw new GeneralException("Error while executing GET_DEPT_ZONE", e);
		}
		finally{
			PristineDBUtil.close(resultSet);
			PristineDBUtil.close(statement);
		}
		return deptZoneMap;
	}
	
	/**
	 * Checks cost availability for a week
	 * @throws GeneralException 
	 * 
	 */
	public boolean checkPriceAvailablity(Connection conn, int calendarId) throws GeneralException{
		PreparedStatement statement = null;
		ResultSet resultSet = null;
		boolean isPriceAvailable = false;
		try{
			statement = conn.prepareStatement(CHECK_PRICE_AVAILABILITY_FOR_WEEK);
			int counter = 0;
			statement.setInt(++counter, calendarId);
			resultSet = statement.executeQuery();
			if(resultSet.next()){
				isPriceAvailable = true;
			}
		}
		catch(SQLException sqlE){
			logger.error("Error while checking cost availability - " + sqlE.toString());
			throw new GeneralException("Error while checking cost availability", sqlE);
		}
		
		return isPriceAvailable;
	}
	
	
	/**
	 * This method saves ignored records. 
	 * @param conn					Connection
	 * @param toBeInsertedList		List of records that needs to be inserted in retail_price_info
	 * @throws GeneralException
	 */
	public void saveIgnoredData(Connection conn, List<PriceAndCostDTO> ignoredRecords) throws GeneralException{
		logger.debug("Inside saveIgnoredData() of RetailPriceDAO");
		PreparedStatement statement = null;
	    try{
			statement = conn.prepareStatement(SAVE_IGNORED_RECORDS);
	        
			int recordCnt = 0;
			int itemNoInBatch = 0;
	        for(PriceAndCostDTO priceAndCostDTO:ignoredRecords){
	        	int counter = 0;
	        	//? CALENDAR_ID, ? SRC_VENDOR_AND_ITEM_ID, ? LEVEL_ID, ? RECORD_TYPE, ? STORE_OR_ZONE_NO, ? REG_PRICE, ? REG_QTY, TO_DATE(?,'MM/dd/yy') EFF_DATE 
	        	statement.setInt(++counter, priceAndCostDTO.getCalendarId());
	        	statement.setString(++counter, priceAndCostDTO.getVendorNo() + priceAndCostDTO.getItemNo());
	        	statement.setString(++counter, priceAndCostDTO.getSourceCode());
	        	statement.setString(++counter, priceAndCostDTO.getRecordType());
	        	statement.setString(++counter, priceAndCostDTO.getZone());
	        	statement.setString(++counter, priceAndCostDTO.getStrCurrRetail());
	        	statement.setInt(++counter, priceAndCostDTO.getRtlQuanity());
	        	statement.setString(++counter, priceAndCostDTO.getRetailEffDate());
	        	statement.setString(++counter, priceAndCostDTO.getUpc());
	        	statement.addBatch();
	        	itemNoInBatch++;
	        	recordCnt++;
	        	if(itemNoInBatch % Constants.BATCH_UPDATE_COUNT == 0){
	        		statement.executeBatch();
	        		statement.clearBatch();
	        		itemNoInBatch = 0;
	        	}
	        	if(recordCnt % 10000 == 0){
	        		conn.commit();
	        	}
	        }
	        if(itemNoInBatch > 0){
	        	statement.executeBatch();
        		statement.clearBatch();
	        }
		}
		catch (SQLException e)
		{
			logger.error("Error while executing SAVE_IGNORED_RECORDS");
			throw new GeneralException("Error while executing SAVE_IGNORED_RECORDS", e);
		}finally{
			PristineDBUtil.close(statement);
		}
	}

	/**
	 * 
	 * @param conn
	 * @return map of zones with stores
	 * @throws GeneralException 
	 */
	public HashMap<String, List<String>> getZoneStoreMapping(Connection conn) throws GeneralException{
		HashMap<String, List<String>> zoneStoreMap = new HashMap<>();
		PreparedStatement statement = null;
		ResultSet resultSet = null;
		try{
			StringBuilder sb = new StringBuilder(ZONE_STORE_MAPPING_QUERY);
			int defaultProdId = Integer.parseInt(PropertyManager.
					getProperty("DEFAULT_PRODUCT_ID_IN_PROD_LOC_MAPPING", "0"));
			
			if(defaultProdId != 0){
				sb.append(" AND PLM.PRODUCT_ID = " + defaultProdId + " AND PRODUCT_LEVEL_ID = " + Constants.ALLPRODUCTS);
			}
			logger.debug("Product-Location Query:" + sb.toString());
			statement = conn.prepareStatement(sb.toString());
			resultSet = statement.executeQuery();
			while(resultSet.next()){
				String zoneNum = resultSet.getString("LOCATION_NO");
				String compStrNo = resultSet.getString("COMP_STR_NO");
				if(zoneStoreMap.get(zoneNum) == null){
					List<String> tempList = new ArrayList<>();
					tempList.add(compStrNo);
					zoneStoreMap.put(zoneNum, tempList);
				}else{
					List<String> tempList = zoneStoreMap.get(zoneNum);
					tempList.add(compStrNo);
					zoneStoreMap.put(zoneNum, tempList);
				}
			}
		}catch(SQLException sqlE){
			throw new GeneralException("Error while getting zone store map", sqlE);
		}finally{
			PristineDBUtil.close(resultSet);
			PristineDBUtil.close(statement);
		}
		return zoneStoreMap;
	}
	
	
	/**
	 * 
	 * @param conn
	 * @return map of zones with stores
	 * @throws GeneralException 
	 */
	public HashMap<String, List<String>> getZoneStoreMappingWithStoreNo(Connection conn) throws GeneralException{
		HashMap<String, List<String>> zoneStoreMap = new HashMap<>();
		PreparedStatement statement = null;
		ResultSet resultSet = null;
		try{
			statement = conn.prepareStatement(ZONE_STORE_MAPPING_QUERY);
			resultSet = statement.executeQuery();
			while(resultSet.next()){
				String zoneNum = resultSet.getString("LOCATION_NO");
				String storeNum = resultSet.getString("COMP_STR_NO");
				if(zoneStoreMap.get(zoneNum) == null){
					List<String> tempList = new ArrayList<>();
					tempList.add(storeNum);
					zoneStoreMap.put(zoneNum, tempList);
				}else{
					List<String> tempList = zoneStoreMap.get(zoneNum);
					tempList.add(storeNum);
					zoneStoreMap.put(zoneNum, tempList);
				}
			}
		}catch(SQLException sqlE){
			throw new GeneralException("Error while getting zone store map", sqlE);
		}finally{
			PristineDBUtil.close(resultSet);
			PristineDBUtil.close(statement);
		}
		return zoneStoreMap;
	}
	
	
	public List<RetailPriceDTO> getBaseStoreCompDetails(Connection conn, String WeekStartDate) throws GeneralException {

		List<RetailPriceDTO> baseCompDetails =  new ArrayList<>();
		

		StringBuffer sql = new StringBuffer();
		sql.append("select il.retailer_item_code, il.item_code, rpi.level_type_id, rpi.level_id,");
		sql.append(" il.upc, rpi.reg_qty,case when rpi.reg_qty > 1 then rpi.reg_m_price else rpi.reg_price end reg_price,");
		sql.append(" rpi.sale_qty, case when rpi.sale_qty > 1 then rpi.sale_m_price else rpi.sale_price end sale_price from retail_price_info rpi");
		sql.append(" inner join item_lookup il on rpi.item_code = il.item_code where il.lir_ind = 'N' and rpi.calendar_id in (select calendar_id from retail_calendar where start_date = TO_DATE('" + WeekStartDate + "','MM/DD/YYYY') and row_type = 'W')");
		sql.append(" and ((rpi.level_id in ('GE-106-1','GE-106-3','GE-4-4') and rpi.level_type_id = 1) OR (rpi.level_id in (53) and rpi.level_type_id = 0))");
		
		logger.debug("getBaseStoreCompDetails SQL:" + sql.toString());

		try {
			CachedRowSet result = PristineDBUtil.executeQuery(conn, sql,
														"getBaseStoreCompDetails");
			while (result.next()) {
				RetailPriceDTO dataDto = new RetailPriceDTO();
				dataDto.setItemcode(result.getString("ITEM_CODE"));
				dataDto.setRetailerItemCode(result.getString("RETAILER_ITEM_CODE"));
				dataDto.setUpc(result.getString("UPC"));
				dataDto.setRegQty(result.getInt("REG_QTY"));
				dataDto.setRegPrice(result.getFloat("REG_PRICE"));
				dataDto.setSaleQty(result.getInt("SALE_QTY"));
				dataDto.setSalePrice(result.getFloat("SALE_PRICE"));
				dataDto.setLevelTypeId(result.getInt("LEVEL_TYPE_ID"));
				baseCompDetails.add(dataDto);
			}

		} catch (GeneralException gex) {
			logger.error("Error while fetching store data " + gex.getMessage());
			throw new GeneralException("getBaseStoreCompDetails", gex);			
		} catch (SQLException sex) {
			logger.error("Error while fetching store data " + sex.getMessage());
			throw new GeneralException("getBaseStoreCompDetails", sex);			
		}
		return baseCompDetails;
	}
	
	public List<RetailPriceDTO> getBaseZoneCompDetails(Connection conn, String WeekStartDate, String ZoneNo, Integer chainID) throws GeneralException {

		List<RetailPriceDTO> baseCompDetails =  new ArrayList<>();
		

		StringBuffer sql = new StringBuffer();
		sql.append("select il.retailer_item_code, il.item_code, rpi.level_type_id, rpi.level_id, rpi.eff_reg_start_date,");
		sql.append(" il.upc, rpi.reg_qty,case when rpi.reg_qty > 1 then rpi.reg_m_price else rpi.reg_price end reg_price,");		
		sql.append(" rpi.sale_qty, case when rpi.sale_qty > 1 then rpi.sale_m_price else rpi.sale_price end sale_price from retail_price_info rpi");
		sql.append(" inner join item_lookup il on rpi.item_code = il.item_code where il.lir_ind = 'N' and rpi.calendar_id in (select calendar_id from retail_calendar where start_date = TO_DATE('" + WeekStartDate + "','MM/DD/YYYY') and row_type = 'W')");
		sql.append(" and ((rpi.level_id in ('" + ZoneNo + "') and rpi.level_type_id = 1) OR (rpi.level_id in (" + chainID + ") and rpi.level_type_id = 0))");
		
		logger.debug("getBaseZoneCompDetails SQL:" + sql.toString());

		try {
			CachedRowSet result = PristineDBUtil.executeQuery(conn, sql,
														"getBaseZoneCompDetails");
			while (result.next()) {
				RetailPriceDTO dataDto = new RetailPriceDTO();
				dataDto.setItemcode(result.getString("ITEM_CODE"));
				dataDto.setRetailerItemCode(result.getString("RETAILER_ITEM_CODE"));
				dataDto.setRegEffectiveDate(result.getString("EFF_REG_START_DATE"));
				dataDto.setUpc(result.getString("UPC"));
				dataDto.setRegQty(result.getInt("REG_QTY"));
				dataDto.setRegPrice(result.getFloat("REG_PRICE"));
				dataDto.setSaleQty(result.getInt("SALE_QTY"));
				dataDto.setSalePrice(result.getFloat("SALE_PRICE"));
				dataDto.setLevelTypeId(result.getInt("LEVEL_TYPE_ID"));
				baseCompDetails.add(dataDto);
			}

		} catch (GeneralException gex) {
			logger.error("Error while fetching store data " + gex.getMessage());
			throw new GeneralException("getBaseZoneCompDetails", gex);			
		} catch (SQLException sex) {
			logger.error("Error while fetching store data " + sex.getMessage());
			throw new GeneralException("getBaseZoneCompDetails", sex);			
		}
		return baseCompDetails;
	}
	
	
/* This method inserts retail price data in RETAIL_PRICE_INFO table
	 * @param conn					Connection
	 * @param toBeInsertedList		List of records that needs to be inserted in retail_price_info
	 * @throws GeneralException
	 */
	@SuppressWarnings("unused")
	public void insertRetailPriceData(Connection conn, List<RetailPriceDTO> toBeInsertedList) throws GeneralException{
		logger.debug("Inside saveRetailPriceData() of RetailPriceDAO");
		PreparedStatement statement = null;
	    try{
			statement = conn.prepareStatement(INSERT_RETAIL_PRICE_INFO);
	        
			int recordCnt = 0;
			int itemNoInBatch = 0;
	        for(RetailPriceDTO retailPriceDTO:toBeInsertedList){
	        	int counter = 0;
	        	logger.debug("Row Insert: " + retailPriceDTO.toString());
	        	statement.setInt(++counter, retailPriceDTO.getCalendarId());
	        	statement.setString(++counter, retailPriceDTO.getItemcode());
	        	statement.setInt(++counter, retailPriceDTO.getLevelTypeId());
	        	statement.setString(++counter, retailPriceDTO.getLevelId());
	        	statement.setFloat(++counter, retailPriceDTO.getRegPrice());
	        	statement.setInt(++counter, retailPriceDTO.getRegQty());
	        	statement.setFloat(++counter, retailPriceDTO.getRegMPrice());
	        	statement.setFloat(++counter, retailPriceDTO.getSalePrice());
	        	statement.setInt(++counter, retailPriceDTO.getSaleQty());
	        	statement.setFloat(++counter, retailPriceDTO.getSaleMPrice());
	        	statement.setString(++counter,retailPriceDTO.getPromotionFlag());
	        	statement.setString(++counter, retailPriceDTO.getRegEffectiveDate());
	        	statement.setString(++counter, retailPriceDTO.getSaleStartDate());
	        	statement.setString(++counter, retailPriceDTO.getSaleEndDate());
	        	if(retailPriceDTO.getLocationId() == null)
	        		statement.setNull(++counter, Types.NULL);
	        	else
	        		statement.setInt(++counter, retailPriceDTO.getLocationId());
	        	if(retailPriceDTO.isWhseZoneRolledUpRecord()){
	        		statement.setString(++counter, String.valueOf(Constants.YES));
	        	}else{
	        		statement.setString(++counter, String.valueOf(Constants.NO));
	        	}
	        	statement.setString(++counter, retailPriceDTO.getCoreRetailValue());
	        	statement.setString(++counter, retailPriceDTO.getVdprRetail());
	        	statement.addBatch();
	        	itemNoInBatch++;
	        	recordCnt++;
	        	if(itemNoInBatch % Constants.BATCH_UPDATE_COUNT == 0){
	        		logger.debug("Batch Execute...");
	        		statement.executeBatch();
	        		statement.clearBatch();
	        		itemNoInBatch = 0;
	        		logger.debug("Batch Execute is completed");
	        	}
	        	if(recordCnt % 10000 == 0){
	        		conn.commit();
	        	}
	        }
	        if(itemNoInBatch > 0){
	        	logger.debug("Batch Execute...");
				statement.executeBatch();
       		statement.clearBatch();
       		logger.debug("Batch Execute is completed");
	        }
		}
		catch (SQLException e)
		{
			logger.error("Error while executing INSERT_RETAIL_PRICE_INFO");
			throw new GeneralException("Error while executing INSERT_RETAIL_PRICE_INFO" + e);
		}finally{
			PristineDBUtil.close(statement);
		}
	}
	
	
	/**
	 * Setup retail cost data for given calendar id from previous week's calendar id
	 * @param conn			Database connection
	 * @param calendarId	Calendar Id
	 * @throws GeneralException
	 */
	public boolean insertRetailPriceData(Connection conn, int calendarId, int prevWkCalendarId)
			throws GeneralException {
		logger.debug("Inside setupRetailCostData() of RetailPriceDAO");
		PreparedStatement statement = null;
		int count = 0;
		try {
			statement = conn.prepareStatement(INSERT_RETAIL_PRICE_DATA);
			statement.setInt(1, calendarId);
			statement.setInt(2, prevWkCalendarId);
			count = statement.executeUpdate();
		} catch (SQLException e) {
			logger.error("Error while executing INSERT_RETAIL_PRICE_DATA");
			throw new GeneralException("Error while executing INSERT_RETAIL_PRICE_DATA", e);
		} finally {
			PristineDBUtil.close(statement);
		}
		return count > 0;
	}
	
	/**
	 * This method retrieves values from RETAIL_PRICE_INFO based on ITEM_CODE
	 * @param conn				Connection
	 * @param itemCodeSet		Set containing item codes for which data needs to be retrieved from retail_price_info
	 * @param calendarId		Calendar Id
	 * @return HashMap containing item code as key and list of its price data as value
	 * @throws GeneralException
	 */
	@SuppressWarnings("static-access")
	public HashMap<String, List<RetailPriceDTO>> getRetailPriceInfoAZ(Connection conn, Set<String> itemCodeSet, int calendarId, boolean isLatest) throws GeneralException{
		List<String> itemCodeList = new ArrayList<String>();
	    HashMap<String, List<RetailPriceDTO>> retailPriceDBMap = new HashMap<String, List<RetailPriceDTO>>();
	    int limitcount=0;
	    
		for(String itemCode:itemCodeSet){
			itemCodeList.add(itemCode);
			limitcount++;
			if( limitcount > 0 && (limitcount%this.commitCount == 0)){
				Object[] values = itemCodeList.toArray();
				List<RetailPriceDTO> retailPriceDBList = getRetailPriceInfo(conn, calendarId, isLatest, values);
				
				for(RetailPriceDTO retailPriceDTO:retailPriceDBList){
					String dbItemCOde = retailPriceDTO.getItemcode();
					if(retailPriceDBMap.get(dbItemCOde) != null){
		        		List<RetailPriceDTO> tempList = retailPriceDBMap.get(dbItemCOde);
		        		tempList.add(retailPriceDTO);
		        		retailPriceDBMap.put(dbItemCOde, tempList);
		        	}else{
		        		List<RetailPriceDTO> tempList = new ArrayList<RetailPriceDTO>();
		        		tempList.add(retailPriceDTO);
		        		retailPriceDBMap.put(dbItemCOde, tempList);
		        	}
				}
				itemCodeList.clear();
            }
		}
		if(itemCodeList.size() > 0){
			Object[] values = itemCodeList.toArray();
			List<RetailPriceDTO> retailPriceDBList = getRetailPriceInfo(conn, calendarId, isLatest, values);
			
			for(RetailPriceDTO retailPriceDTO:retailPriceDBList){
				String dbItemCOde = retailPriceDTO.getItemcode();
				if(retailPriceDBMap.get(dbItemCOde) != null){
	        		List<RetailPriceDTO> tempList = retailPriceDBMap.get(dbItemCOde);
	        		tempList.add(retailPriceDTO);
	        		retailPriceDBMap.put(dbItemCOde, tempList);
	        	}else{
	        		List<RetailPriceDTO> tempList = new ArrayList<RetailPriceDTO>();
	        		tempList.add(retailPriceDTO);
	        		retailPriceDBMap.put(dbItemCOde, tempList);
	        	}
			}
			itemCodeList.clear();
		}
		return retailPriceDBMap;
	}
	
	/**
	 * This method queries the database for Retail Price Info for every set of Item Codes
	 * @param conn			Connection
	 * @param calendarId	Calendar Id
	 * @param values		Array of UPCs that will be passed as input to the query
	 * @return List of retail price data
	 * @throws GeneralException
	 */
	private List<RetailPriceDTO> getRetailPriceInfo(Connection conn, int calendarId, boolean isLatest, Object... values) throws GeneralException{
		PreparedStatement statement = null;
	    ResultSet resultSet = null;
	    List<RetailPriceDTO> retailPriceDTOList = new ArrayList<RetailPriceDTO>();
		try{
			long startTime = System.currentTimeMillis();
			
			/* Else block added to retrieve latest retail price info for item codes.
			 * This will be used when loading competitive data table from retail price info table
			 */
			
				statement = conn.prepareStatement(String.format(GETALL_RETAIL_PRICE_INFO, preparePlaceHolders(values.length)));
				statement.setInt(1, calendarId);
		        setValues(statement, 2, values);
			
			statement.setFetchSize(100000);
	        resultSet = statement.executeQuery();
	        logger.debug("Query Executed");
	        RetailPriceDTO retailPriceDTO;
	        while(resultSet.next()){
	        	retailPriceDTO = new RetailPriceDTO();
	        	retailPriceDTO.setItemcode(resultSet.getString("ITEM_CODE"));
	        	retailPriceDTO.setCalendarId(resultSet.getInt("CALENDAR_ID"));
	        	retailPriceDTO.setLevelTypeId(resultSet.getInt("LEVEL_TYPE_ID"));
	        	retailPriceDTO.setLevelId(resultSet.getString("LEVEL_ID"));
	        	retailPriceDTO.setRegPrice(resultSet.getFloat("REG_PRICE"));
	        	retailPriceDTO.setRegQty(resultSet.getInt("REG_QTY"));
	        	retailPriceDTO.setRegMPrice(resultSet.getFloat("REG_M_PRICE"));
	        	retailPriceDTO.setSalePrice(resultSet.getFloat("SALE_PRICE"));
	        	retailPriceDTO.setSaleQty(resultSet.getInt("SALE_QTY"));
	        	retailPriceDTO.setSaleMPrice(resultSet.getFloat("SALE_M_PRICE"));
	        	retailPriceDTO.setPromotionFlag(resultSet.getString("PROMOTION_FLG"));
	        	retailPriceDTO.setRegEffectiveDate(resultSet.getString("EFF_REG_START_DATE"));
	        	retailPriceDTO.setSaleStartDate(resultSet.getString("EFF_SALE_START_DATE"));
	        	retailPriceDTO.setSaleEndDate(resultSet.getString("EFF_SALE_END_DATE"));
	        	retailPriceDTO.setVdprRetail(resultSet.getString("VDPR_RETAIL"));
	        	retailPriceDTO.setCoreRetailValue(resultSet.getString("CORE_RETAIL"));
	        	retailPriceDTOList.add(retailPriceDTO);
	        }
	        logger.debug("Records populated");
	        long endTime = System.currentTimeMillis();
	        logger.debug("Time taken to execute and store price info in list- " + (endTime - startTime));
		}
		catch (SQLException e)
		{
			logger.error("Error while executing GETALL_RETAIL_PRICE_INFO");
			throw new GeneralException("Error while executing GETALL_RETAIL_PRICE_INFO", e);
		}finally{
			PristineDBUtil.close(resultSet);
			PristineDBUtil.close(statement);
		}
		return retailPriceDTOList;
	}
	
}



