package com.pristine.test.offermgmt;

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
import org.apache.log4j.PropertyConfigurator;

import com.pristine.dao.DBManager;
import com.pristine.dao.IDAO;
import com.pristine.dto.MovementWeeklyDTO;
import com.pristine.dto.offermgmt.ExecutionTimeLog;
import com.pristine.dto.offermgmt.PRItemDTO;
import com.pristine.dto.offermgmt.PRStrategyDTO;
import com.pristine.exception.GeneralException;
import com.pristine.exception.OfferManagementException;
import com.pristine.service.offermgmt.ItemKey;
import com.pristine.service.offermgmt.ItemService;
import com.pristine.util.Constants;
import com.pristine.util.PristineDBUtil;
import com.pristine.util.PropertyManager;
import com.pristine.util.offermgmt.PRCommonUtil;
import com.pristine.util.offermgmt.PRConstants;

public class DBPerformanceTest implements IDAO{
	private static Logger logger = Logger.getLogger("DBPerformanceTest");
	private Connection conn = null;
	
	//Original query
	private static final String GET_MOVE_DATA_FOR_ZONE_FROM_IMS_1 = "SELECT LOCATION_LEVEL_ID || '-' || LOCATION_ID AS COMP_STR_NO, "
			+ "PRODUCT_ID AS PRESTO_ITEM_CODE,  " 
			+ "IL.RETAILER_ITEM_CODE, IL.UPC, IL.RET_LIR_ID, " 
			+ "C.CALENDAR_ID, "
			+ "TO_CHAR(C.START_DATE,'YYYY-MM-DD') as WEEK_START_DATE, " + "TO_CHAR(C.END_DATE,'YYYY-MM-DD') as WEEK_END_DATE, "
			+ "TOT_MOVEMENT as QUANTITY, " + "CASE WHEN REG_M_PACK <= 1 THEN REG_PRICE ELSE 0 END AS REG_PRICE,  " + "REG_M_PACK, "
			+ "CASE WHEN REG_M_PACK > 1 THEN REG_PRICE ELSE 0 END AS REG_M_PRICE, "
			+ "CASE WHEN SALE_M_PACK <= 1 THEN SALE_PRICE ELSE 0 END AS SALE_PRICE, "
			+ "SALE_M_PACK, CASE WHEN SALE_M_PACK > 1 THEN SALE_PRICE ELSE 0 END AS SALE_M_PRICE, " + "SALE_FLAG  , " + "DECODE("
			+ "UNITPRICE(CASE WHEN SALE_M_PACK <= 1 THEN SALE_PRICE ELSE 0 END,CASE WHEN SALE_M_PACK > 1 THEN SALE_PRICE ELSE 0 END, SALE_M_PACK),0, "
			+ "UNITPRICE(CASE WHEN REG_M_PACK <= 1 THEN REG_PRICE ELSE 0 END,CASE WHEN REG_M_PACK > 1 THEN REG_PRICE ELSE 0 END, REG_M_PACK),"
			+ "UNITPRICE(CASE WHEN SALE_M_PACK <= 1 THEN SALE_PRICE ELSE 0 END,CASE WHEN SALE_M_PACK > 1 THEN SALE_PRICE ELSE 0 END, SALE_M_PACK)) AS FINAL_PRICE"
			+ " FROM ( " + "SELECT CALENDAR_ID, LOCATION_LEVEL_ID, LOCATION_ID, PRODUCT_ID, "
			+ "SALE_FLAG, TO_NUMBER(SUBSTR(REG_PRICE, 1, INSTR(REG_PRICE, '/', -1, 1) -1)) AS REG_M_PACK, "
			+ "TO_NUMBER(SUBSTR(REG_PRICE, INSTR(REG_PRICE, '/', -1, 1) +1)) AS REG_PRICE, "
			+ "TO_NUMBER(SUBSTR(SALE_PRICE, 1, INSTR(SALE_PRICE, '/', -1, 1) -1)) AS SALE_M_PACK,"
			+ " TO_NUMBER(SUBSTR(SALE_PRICE, INSTR(SALE_PRICE, '/', -1, 1) +1)) AS SALE_PRICE, " + " TOT_MOVEMENT " + " FROM( " + "SELECT "
			+ "CALENDAR_ID AS CALENDAR_ID, 6 AS LOCATION_LEVEL_ID, 6 AS LOCATION_ID, PRODUCT_ID,  " + "STATS_MODE(SALE_FLAG) AS SALE_FLAG, "
			+ "STATS_MODE(CASE  WHEN (NVL(REG_M_PACK,0) <= 1 AND REG_PRICE > 0) THEN  ('1' ||  '/' || NVL(REG_PRICE,0))  "
			+ "WHEN (NVL(REG_M_PACK,0) <= 1 AND REG_PRICE = 0) THEN  (REG_M_PACK ||  '/' || NVL(REG_PRICE,0))  "
			+ "ELSE (REG_M_PACK || '/' || NVL(REG_M_PRICE,0)) END) AS REG_PRICE , STATS_MODE(CASE  "
			+ "WHEN (NVL(SALE_M_PACK,0) <= 1 AND SALE_PRICE > 0) THEN  ('1' ||  '/' || NVL(SALE_PRICE,0))  "
			+ "WHEN (NVL(SALE_M_PACK,0) <= 1 AND SALE_PRICE = 0) THEN  (SALE_M_PACK ||  '/' || NVL(SALE_PRICE,0))  "
			+ "ELSE (SALE_M_PACK || '/' || NVL(SALE_M_PRICE,0)) END) AS SALE_PRICE,  " + "SUM(TOT_MOVEMENT) AS TOT_MOVEMENT " + " FROM   " + "(  "
			+ "SELECT CALENDAR_ID,PRODUCT_ID,SALE_FLAG,"
			+ "REG_PRICE,REG_M_PACK,REG_M_PRICE,SALE_PRICE,SALE_M_PACK,SALE_M_PRICE,TOT_MOVEMENT FROM ITEM_METRIC_SUMMARY_WEEKLY IMS  "
			+ "WHERE "
			+ " PRODUCT_ID IN( " 
			/*+ "SELECT IL.ITEM_CODE FROM "
			+ "(SELECT CHILD_PRODUCT_ID FROM (SELECT * FROM PRODUCT_GROUP_RELATION PGR "
			+ "START WITH PRODUCT_LEVEL_ID = 4 AND PRODUCT_ID = ? CONNECT BY  PRIOR CHILD_PRODUCT_ID = PRODUCT_ID "
			+ " AND  PRIOR CHILD_PRODUCT_LEVEL_ID = PRODUCT_LEVEL_ID)  " + "WHERE CHILD_PRODUCT_LEVEL_ID = 1 "
			+ ") ITEMS LEFT JOIN ITEM_LOOKUP IL ON ITEMS.CHILD_PRODUCT_ID = IL.ITEM_CODE WHERE IL.ACTIVE_INDICATOR = 'Y' " */
			+ "SELECT ITEM_CODE FROM ITEM_AND_PRODUCTS_MV WHERE CATEGORY_ID  = ? AND ACTIVE_INDICATOR='Y' "
			+ ")   "
			+ "AND IMS.CALENDAR_ID IN (" + "SELECT CALENDAR_ID  "
			+ "FROM RETAIL_CALENDAR RC  WHERE START_DATE BETWEEN TO_DATE(?, 'YYYY-MM-DD') AND"
			+ " TO_DATE(?, 'YYYY-MM-DD')AND ROW_TYPE          = 'W'  " + ")   " 
			+ " AND IMS.LOCATION_ID      IN " + "(SELECT STORE_ID FROM PR_PRODUCT_LOCATION_STORE WHERE PRODUCT_LOCATION_MAPPING_ID IN ( "
			+ "SELECT PRODUCT_LOCATION_MAPPING_ID FROM PR_PRODUCT_LOCATION_MAPPING " + "WHERE PRODUCT_LEVEL_ID = 4 AND PRODUCT_ID = ? "
			+ "AND LOCATION_LEVEL_ID = 6 AND LOCATION_ID = ?) " + " )     " + ")GROUP BY CALENDAR_ID , product_id)) IMS "
			+ "LEFT JOIN RETAIL_CALENDAR C ON IMS.CALENDAR_ID = C.CALENDAR_ID " + "LEFT JOIN ITEM_LOOKUP IL ON IMS.PRODUCT_ID  =  IL.ITEM_CODE ";
	
	//Product, Calendar & Location (item codes, calendar & location in IN clause)
	private static final String GET_MOVE_DATA_FOR_ZONE_FROM_IMS_2 = "SELECT LOCATION_LEVEL_ID || '-' || LOCATION_ID AS COMP_STR_NO, "
			+ "PRODUCT_ID AS PRESTO_ITEM_CODE,  " + "IL.RETAILER_ITEM_CODE,  " + " IL.UPC,  " + "IL.RET_LIR_ID, " + "C.CALENDAR_ID, "
			+ "TO_CHAR(C.START_DATE,'YYYY-MM-DD') as WEEK_START_DATE, " + "TO_CHAR(C.END_DATE,'YYYY-MM-DD') as WEEK_END_DATE, "
			+ "TOT_MOVEMENT as QUANTITY, " + "CASE WHEN REG_M_PACK <= 1 THEN REG_PRICE ELSE 0 END AS REG_PRICE,  " + "REG_M_PACK, "
			+ "CASE WHEN REG_M_PACK > 1 THEN REG_PRICE ELSE 0 END AS REG_M_PRICE, "
			+ "CASE WHEN SALE_M_PACK <= 1 THEN SALE_PRICE ELSE 0 END AS SALE_PRICE, "
			+ "SALE_M_PACK, CASE WHEN SALE_M_PACK > 1 THEN SALE_PRICE ELSE 0 END AS SALE_M_PRICE, " + "SALE_FLAG  , " + "DECODE("
			+ "UNITPRICE(CASE WHEN SALE_M_PACK <= 1 THEN SALE_PRICE ELSE 0 END,CASE WHEN SALE_M_PACK > 1 THEN SALE_PRICE ELSE 0 END, SALE_M_PACK),0, "
			+ "UNITPRICE(CASE WHEN REG_M_PACK <= 1 THEN REG_PRICE ELSE 0 END,CASE WHEN REG_M_PACK > 1 THEN REG_PRICE ELSE 0 END, REG_M_PACK),"
			+ "UNITPRICE(CASE WHEN SALE_M_PACK <= 1 THEN SALE_PRICE ELSE 0 END,CASE WHEN SALE_M_PACK > 1 THEN SALE_PRICE ELSE 0 END, SALE_M_PACK)) AS FINAL_PRICE"
			+ " FROM ( " + "SELECT CALENDAR_ID, LOCATION_LEVEL_ID, LOCATION_ID, PRODUCT_ID, "
			+ "SALE_FLAG, TO_NUMBER(SUBSTR(REG_PRICE, 1, INSTR(REG_PRICE, '/', -1, 1) -1)) AS REG_M_PACK, "
			+ "TO_NUMBER(SUBSTR(REG_PRICE, INSTR(REG_PRICE, '/', -1, 1) +1)) AS REG_PRICE, "
			+ "TO_NUMBER(SUBSTR(SALE_PRICE, 1, INSTR(SALE_PRICE, '/', -1, 1) -1)) AS SALE_M_PACK,"
			+ " TO_NUMBER(SUBSTR(SALE_PRICE, INSTR(SALE_PRICE, '/', -1, 1) +1)) AS SALE_PRICE, " + " TOT_MOVEMENT " + " FROM( " + "SELECT "
			+ "CALENDAR_ID AS CALENDAR_ID, 6 AS LOCATION_LEVEL_ID, 6 AS LOCATION_ID, PRODUCT_ID,  " + "STATS_MODE(SALE_FLAG) AS SALE_FLAG, "
			+ "STATS_MODE(CASE  WHEN (NVL(REG_M_PACK,0) <= 1 AND REG_PRICE > 0) THEN  ('1' ||  '/' || NVL(REG_PRICE,0))  "
			+ "WHEN (NVL(REG_M_PACK,0) <= 1 AND REG_PRICE = 0) THEN  (REG_M_PACK ||  '/' || NVL(REG_PRICE,0))  "
			+ "ELSE (REG_M_PACK || '/' || NVL(REG_M_PRICE,0)) END) AS REG_PRICE , STATS_MODE(CASE  "
			+ "WHEN (NVL(SALE_M_PACK,0) <= 1 AND SALE_PRICE > 0) THEN  ('1' ||  '/' || NVL(SALE_PRICE,0))  "
			+ "WHEN (NVL(SALE_M_PACK,0) <= 1 AND SALE_PRICE = 0) THEN  (SALE_M_PACK ||  '/' || NVL(SALE_PRICE,0))  "
			+ "ELSE (SALE_M_PACK || '/' || NVL(SALE_M_PRICE,0)) END) AS SALE_PRICE,  " + "SUM(TOT_MOVEMENT) AS TOT_MOVEMENT " + " FROM   " + "(  "
			+ "SELECT CALENDAR_ID,PRODUCT_ID,SALE_FLAG,"
			+ "REG_PRICE,REG_M_PACK,REG_M_PRICE,SALE_PRICE,SALE_M_PACK,SALE_M_PRICE,TOT_MOVEMENT FROM ITEM_METRIC_SUMMARY_WEEKLY IMS  "
			+ "WHERE PRODUCT_ID IN (%s) "
			+ "AND IMS.CALENDAR_ID IN  (%CALENDAR_IDS%) "
			+ "AND IMS.LOCATION_ID IN (%PRICE_ZONE_STORES%)"
			+ ")GROUP BY CALENDAR_ID , product_id)) IMS "
			+ "LEFT JOIN RETAIL_CALENDAR C ON IMS.CALENDAR_ID = C.CALENDAR_ID " + "LEFT JOIN ITEM_LOOKUP IL ON IMS.PRODUCT_ID  =  IL.ITEM_CODE";
	
	//Product, Calendar & Location (item codes in IN clause)
	private static final String GET_MOVE_DATA_FOR_ZONE_FROM_IMS_3 = "SELECT LOCATION_LEVEL_ID || '-' || LOCATION_ID AS COMP_STR_NO, "
			+ "PRODUCT_ID AS PRESTO_ITEM_CODE,  " + "IL.RETAILER_ITEM_CODE,  " + " IL.UPC,  " + "IL.RET_LIR_ID, " + "C.CALENDAR_ID, "
			+ "TO_CHAR(C.START_DATE,'YYYY-MM-DD') as WEEK_START_DATE, " + "TO_CHAR(C.END_DATE,'YYYY-MM-DD') as WEEK_END_DATE, "
			+ "TOT_MOVEMENT as QUANTITY, " + "CASE WHEN REG_M_PACK <= 1 THEN REG_PRICE ELSE 0 END AS REG_PRICE,  " + "REG_M_PACK, "
			+ "CASE WHEN REG_M_PACK > 1 THEN REG_PRICE ELSE 0 END AS REG_M_PRICE, "
			+ "CASE WHEN SALE_M_PACK <= 1 THEN SALE_PRICE ELSE 0 END AS SALE_PRICE, "
			+ "SALE_M_PACK, CASE WHEN SALE_M_PACK > 1 THEN SALE_PRICE ELSE 0 END AS SALE_M_PRICE, " + "SALE_FLAG  , " + "DECODE("
			+ "UNITPRICE(CASE WHEN SALE_M_PACK <= 1 THEN SALE_PRICE ELSE 0 END,CASE WHEN SALE_M_PACK > 1 THEN SALE_PRICE ELSE 0 END, SALE_M_PACK),0, "
			+ "UNITPRICE(CASE WHEN REG_M_PACK <= 1 THEN REG_PRICE ELSE 0 END,CASE WHEN REG_M_PACK > 1 THEN REG_PRICE ELSE 0 END, REG_M_PACK),"
			+ "UNITPRICE(CASE WHEN SALE_M_PACK <= 1 THEN SALE_PRICE ELSE 0 END,CASE WHEN SALE_M_PACK > 1 THEN SALE_PRICE ELSE 0 END, SALE_M_PACK)) AS FINAL_PRICE"
			+ " FROM ( " + "SELECT CALENDAR_ID, LOCATION_LEVEL_ID, LOCATION_ID, PRODUCT_ID, "
			+ "SALE_FLAG, TO_NUMBER(SUBSTR(REG_PRICE, 1, INSTR(REG_PRICE, '/', -1, 1) -1)) AS REG_M_PACK, "
			+ "TO_NUMBER(SUBSTR(REG_PRICE, INSTR(REG_PRICE, '/', -1, 1) +1)) AS REG_PRICE, "
			+ "TO_NUMBER(SUBSTR(SALE_PRICE, 1, INSTR(SALE_PRICE, '/', -1, 1) -1)) AS SALE_M_PACK,"
			+ " TO_NUMBER(SUBSTR(SALE_PRICE, INSTR(SALE_PRICE, '/', -1, 1) +1)) AS SALE_PRICE, " + " TOT_MOVEMENT " + " FROM( " + "SELECT "
			+ "CALENDAR_ID AS CALENDAR_ID, 6 AS LOCATION_LEVEL_ID, 6 AS LOCATION_ID, PRODUCT_ID,  " + "STATS_MODE(SALE_FLAG) AS SALE_FLAG, "
			+ "STATS_MODE(CASE  WHEN (NVL(REG_M_PACK,0) <= 1 AND REG_PRICE > 0) THEN  ('1' ||  '/' || NVL(REG_PRICE,0))  "
			+ "WHEN (NVL(REG_M_PACK,0) <= 1 AND REG_PRICE = 0) THEN  (REG_M_PACK ||  '/' || NVL(REG_PRICE,0))  "
			+ "ELSE (REG_M_PACK || '/' || NVL(REG_M_PRICE,0)) END) AS REG_PRICE , STATS_MODE(CASE  "
			+ "WHEN (NVL(SALE_M_PACK,0) <= 1 AND SALE_PRICE > 0) THEN  ('1' ||  '/' || NVL(SALE_PRICE,0))  "
			+ "WHEN (NVL(SALE_M_PACK,0) <= 1 AND SALE_PRICE = 0) THEN  (SALE_M_PACK ||  '/' || NVL(SALE_PRICE,0))  "
			+ "ELSE (SALE_M_PACK || '/' || NVL(SALE_M_PRICE,0)) END) AS SALE_PRICE,  " + "SUM(TOT_MOVEMENT) AS TOT_MOVEMENT " + " FROM   " + "(  "
			+ "SELECT CALENDAR_ID,PRODUCT_ID,SALE_FLAG,"
			+ "REG_PRICE,REG_M_PACK,REG_M_PRICE,SALE_PRICE,SALE_M_PACK,SALE_M_PRICE,TOT_MOVEMENT FROM ITEM_METRIC_SUMMARY_WEEKLY IMS  "
			+ "WHERE "
			+ "PRODUCT_ID IN (%s) "
			+ "AND IMS.CALENDAR_ID IN  "
			+ "("
			+ "SELECT CALENDAR_ID  "
			+ "FROM RETAIL_CALENDAR RC  WHERE START_DATE BETWEEN TO_DATE(?, 'YYYY-MM-DD') AND"
			+ " TO_DATE(?, 'YYYY-MM-DD')AND ROW_TYPE  = 'W'  "
			+ ") "
			+ "AND IMS.LOCATION_ID IN "
			+ "("
			+ "SELECT STORE_ID FROM PR_PRODUCT_LOCATION_STORE WHERE PRODUCT_LOCATION_MAPPING_ID IN ( "
			+ "SELECT PRODUCT_LOCATION_MAPPING_ID FROM PR_PRODUCT_LOCATION_MAPPING "
			+ "WHERE PRODUCT_LEVEL_ID = 4 AND PRODUCT_ID = ? "
			+ "AND LOCATION_LEVEL_ID = 6 AND LOCATION_ID = ?) "
			+ ")"
			+ ")GROUP BY CALENDAR_ID , product_id)) IMS "
			+ "LEFT JOIN RETAIL_CALENDAR C ON IMS.CALENDAR_ID = C.CALENDAR_ID " + "LEFT JOIN ITEM_LOOKUP IL ON IMS.PRODUCT_ID  =  IL.ITEM_CODE";
	
	private static final String GET_MOVE_DATA_FOR_ZONE_FROM_IMS_4 = "SELECT ROWNUM RN, CALENDAR_ID,PRODUCT_ID,SALE_FLAG,"
			+ "REG_PRICE,REG_M_PACK,REG_M_PRICE,SALE_PRICE,SALE_M_PACK,SALE_M_PRICE,TOT_MOVEMENT FROM ITEM_METRIC_SUMMARY_WEEKLY IMS " ;
	
	private static final String GET_MOVE_DATA_FOR_ZONE_FROM_IMS_7 = "SELECT ROWNUM RN, CALENDAR_ID,PRODUCT_ID,SALE_FLAG,"
			+ "REG_PRICE,REG_M_PACK,REG_M_PRICE,SALE_PRICE,SALE_M_PACK,SALE_M_PRICE,TOT_MOVEMENT FROM ITEM_METRIC_SUMMARY_WEEKLY_RD IMS " ;
	
			//+ " ORDER BY CALENDAR_ID DESC";
	
	private static final String GET_MOVE_DATA_FOR_ZONE_FROM_IMS_5 = "SELECT MIN(REG_PRICE), MAX(REG_MOVEMENT) FROM ITEM_METRIC_SUMMARY_WEEKLY IMS ";
	
	private static final String GET_MOVE_DATA_FOR_ZONE_FROM_IMS_6 = "SELECT * FROM (SELECT ROWNUM RN , CALENDAR_ID,PRODUCT_ID,SALE_FLAG,"
			+ "REG_PRICE,REG_M_PACK,REG_M_PRICE,SALE_PRICE,SALE_M_PACK,SALE_M_PRICE,TOT_MOVEMENT FROM ITEM_METRIC_SUMMARY_WEEKLY IMS)"
			+ " TEMP WHERE MOD(temp.rn,200) = 0 " ;
	
	private static final int commitCount = Constants.LIMIT_COUNT;
	
	private int locationLevelId, locationId, productLevelId, productId;
	private boolean getAllActiveItems = false;
	String queryName = "";
	
	public static void main(String[] args) throws Exception {
		DBPerformanceTest dbPerformanceTest = new DBPerformanceTest();
		PropertyConfigurator.configure("log4j-db-performance.properties");
		PropertyManager.initialize("recommendation.properties");

		for (String arg : args) {
			if (arg.startsWith("LOCATION_LEVEL_ID=")) {
				dbPerformanceTest.locationLevelId = Integer.parseInt(arg.substring("LOCATION_LEVEL_ID=".length()));
			} else if (arg.startsWith("LOCATION_ID=")) {
				dbPerformanceTest.locationId = Integer.parseInt(arg.substring("LOCATION_ID=".length()));
			} else if (arg.startsWith("PRODUCT_LEVEL_ID=")) {
				dbPerformanceTest.productLevelId = Integer.parseInt(arg.substring("PRODUCT_LEVEL_ID=".length()));
			} else if (arg.startsWith("PRODUCT_ID=")) {
				dbPerformanceTest.productId = Integer.parseInt(arg.substring("PRODUCT_ID=".length()));
			} else if (arg.startsWith("ALL_ITEMS=")) {
				dbPerformanceTest.getAllActiveItems = Boolean.parseBoolean(arg.substring("ALL_ITEMS=".length()));
			} else if (arg.startsWith("QUERY_NAME=")) {
				dbPerformanceTest.queryName = arg.substring("QUERY_NAME=".length());
			}
		}

		
		dbPerformanceTest.intialSetup();
		dbPerformanceTest.test();
	}
	
	private void test() {
		try {
			getMovementDataForZone();
		} catch (GeneralException | OfferManagementException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
	}
	
	private HashMap<Integer, List<MovementWeeklyDTO>> getMovementDataForZone() throws GeneralException, OfferManagementException {
		List<Integer> itemCodeList = new ArrayList<Integer>();
		HashMap<Integer, List<MovementWeeklyDTO>> movementData = new HashMap<Integer, List<MovementWeeklyDTO>>();
		int limitcount = 0;
		List<ExecutionTimeLog> executionTimeLogs = new ArrayList<ExecutionTimeLog>();
		ItemService itemService = new ItemService(executionTimeLogs);
		List<Integer> priceZoneStores = new ArrayList<Integer>();
		HashMap<ItemKey, PRItemDTO> itemDataMap = new HashMap<ItemKey, PRItemDTO>();
		List<PRItemDTO> allStoreItems = new ArrayList<PRItemDTO>();
		PRStrategyDTO inputDTO = new PRStrategyDTO();
		
		int noOfWeeksBehind = 114 * 7;
		String startDate = "2014-01-01";
		String endDate = "2016-02-28";
		inputDTO.setLocationLevelId(locationLevelId);
		inputDTO.setLocationId(locationId);
		inputDTO.setProductLevelId(productLevelId);
		inputDTO.setProductId(productId);
		
		priceZoneStores = itemService.getPriceZoneStores(conn, productLevelId, productId, locationLevelId, locationId);
		logger.debug("priceZoneStores:"+ priceZoneStores.toString());
		if (getAllActiveItems) {
			itemDataMap = getAllActiveItems();
		} else {
			allStoreItems = itemService.getAuthorizedItemsOfZoneAndStore(conn, inputDTO, priceZoneStores);
			itemDataMap = itemService.populateAuthorizedItemsOfZone(conn, 0, inputDTO, allStoreItems);
			logger.debug("allStoreItems.size:" + allStoreItems.size());
			logger.debug("itemDataMap.size:" + itemDataMap.size());
		}
		
		Set<Integer> itemList = new HashSet<Integer>();
		for(PRItemDTO prItemDTO : itemDataMap.values()){
			if(!prItemDTO.isLir())
				itemList.add(prItemDTO.getItemCode());
		}
		
		
		if (queryName.equals("GET_MOVE_DATA_FOR_ZONE_FROM_IMS_1")) {
			for (Integer itemCode : itemList) {
				itemCodeList.add(itemCode);
			}
			Object[] values = itemCodeList.toArray();
			logger.info("Distinct Item Codes:" + itemCodeList.size());
			retrieveMovementDataForZone(priceZoneStores, startDate, endDate, noOfWeeksBehind, movementData, values);
		} else {
			logger.info("Distinct Item Codes:" + itemList.size());
			for (Integer itemCode : itemList) {
				itemCodeList.add(itemCode);
				limitcount++;
				if (limitcount > 0 && (limitcount % this.commitCount == 0)) {
					Object[] values = itemCodeList.toArray();
					retrieveMovementDataForZone(priceZoneStores, startDate, endDate, noOfWeeksBehind, movementData, values);
					itemCodeList.clear();
				}
			}
			if (itemCodeList.size() > 0) {
				Object[] values = itemCodeList.toArray();
				retrieveMovementDataForZone(priceZoneStores, startDate, endDate, noOfWeeksBehind, movementData, values);
				itemCodeList.clear();
			}
		}
		

		return movementData;
	}
	
	private void retrieveMovementDataForZone(List<Integer> priceZoneStores, String weekStartDate, String weekEndDate, int maxDataLookupRange, 
			HashMap<Integer, List<MovementWeeklyDTO>> movementData, Object...values) throws GeneralException{
		long startTime = System.currentTimeMillis();
		PreparedStatement statement = null;
	    ResultSet resultSet = null;
	    String calendarIds = "5029,5030,5031,5032,5033,5034,5035,5036,5037,5038,5039,5040,5041,5042,5043,4555,4556,4557,"
	    		+ "4558,4559,4560,4561,4562,4563,4564,4565,4566,4567,4568,4569,4570,4571,4572,4573,4574,4575,4576,4577,4578,"
	    		+ "4579,4580,4581,4582,4583,4584,4585,4586,4587,4588,4589,4590,4591,4592,4593,4594,4595,4596,4597,4598,4599,"
	    		+ "4600,4601,4602,4603,4992,4993,4994,4995,4996,4997,4998,4999,5000,5001,5002,5003,5004,5005,5006,5007,5008,5009,"
	    		+ "5010,5011,5012,5013,5014,5015,5016,5017,5018,5019,5020,5021,5022,5023,5024,5025,5026,5027,5028,6364,6365,6366,"
	    		+ "6367,6368,6369,6370,6371,6372,5069";
		try{
			String query = "";
			String items = "";
			int counter = 0;
			
			String storeIds = PRCommonUtil.getCommaSeperatedStringFromIntArray(priceZoneStores);
			
			for(Object itemCode : values){
				items = items + "," + (Integer) itemCode;
			}
			
			if (queryName.equals("GET_MOVE_DATA_FOR_ZONE_FROM_IMS_1")) {
				query = new String(GET_MOVE_DATA_FOR_ZONE_FROM_IMS_1);
			} else if (queryName.equals("GET_MOVE_DATA_FOR_ZONE_FROM_IMS_2")) {
				query = new String(GET_MOVE_DATA_FOR_ZONE_FROM_IMS_2);
			} else if (queryName.equals("GET_MOVE_DATA_FOR_ZONE_FROM_IMS_3")) {
				query = new String(GET_MOVE_DATA_FOR_ZONE_FROM_IMS_3);
			} else if (queryName.equals("GET_MOVE_DATA_FOR_ZONE_FROM_IMS_4")) {
				query = new String(GET_MOVE_DATA_FOR_ZONE_FROM_IMS_4);
			} else if (queryName.equals("GET_MOVE_DATA_FOR_ZONE_FROM_IMS_5")) {
				query = new String(GET_MOVE_DATA_FOR_ZONE_FROM_IMS_5);
			} else if (queryName.equals("GET_MOVE_DATA_FOR_ZONE_FROM_IMS_6")) {
				query = new String(GET_MOVE_DATA_FOR_ZONE_FROM_IMS_6);
			} else if (queryName.equals("GET_MOVE_DATA_FOR_ZONE_FROM_IMS_7")) {
				query = new String(GET_MOVE_DATA_FOR_ZONE_FROM_IMS_7);
			} 
			
			
			if (queryName.equals("GET_MOVE_DATA_FOR_ZONE_FROM_IMS_1")) {
				//original query
				statement = conn.prepareStatement(query);
				statement.setInt(++counter,  productId);  
				statement.setString(++counter,  weekStartDate);  
				statement.setString(++counter,  weekEndDate);  
				statement.setInt(++counter,  productId);  
				statement.setInt(++counter,  locationId);  
				logger.debug("Movement Query " + query);
				logger.debug("Parameters: 1-" + productId + ",2-" + weekStartDate + ",3-" + weekEndDate + ",4-" + productId
						+ ",5-" + locationId);
			} else if (queryName.equals("GET_MOVE_DATA_FOR_ZONE_FROM_IMS_2")) {
				query = query.replaceAll("%CALENDAR_IDS%", calendarIds);
				query = query.replaceAll("%PRICE_ZONE_STORES%", storeIds);
				statement = conn.prepareStatement(String.format(query, PristineDBUtil.preparePlaceHolders(values.length)));
				//all values passed in IN clause
				PristineDBUtil.setValues(statement, 1, values);
				logger.debug("Movement Query " + query);
				logger.debug("Parameters: 1-" + items );
			} else if (queryName.equals("GET_MOVE_DATA_FOR_ZONE_FROM_IMS_3")) {
				statement = conn.prepareStatement(String.format(query, PristineDBUtil.preparePlaceHolders(values.length)));
				//Only item passed in IN clause
				PristineDBUtil.setValues(statement, 1, values);
				statement.setString(values.length + 1, weekStartDate);
				statement.setString(values.length + 2, weekEndDate);
				statement.setInt(values.length + 3, productId);
				statement.setInt(values.length + 4, locationId);
				logger.debug("Movement Query " + query);
				logger.debug("Parameters: 1-" + items + ",2-" + weekStartDate + ",3-" + weekEndDate + ",4-" + productId + ",5-" + locationId);
			} else if (queryName.equals("GET_MOVE_DATA_FOR_ZONE_FROM_IMS_4")) {
//				statement = conn.prepareStatement(String.format(query),
//						 ResultSet.TYPE_SCROLL_INSENSITIVE,
//						    ResultSet.CONCUR_READ_ONLY,
//						    ResultSet.CLOSE_CURSORS_AT_COMMIT
//						);.
				statement = conn.prepareStatement(String.format(query));
			} else if (queryName.equals("GET_MOVE_DATA_FOR_ZONE_FROM_IMS_5")) {
				statement = conn.prepareStatement(String.format(query));
			} else if (queryName.equals("GET_MOVE_DATA_FOR_ZONE_FROM_IMS_6")) {
				statement = conn.prepareStatement(String.format(query));
			}
			
			//statement.setFetchSize(10000000);
			statement.setFetchSize(100000);
			logger.info("Movement Query executed is Started...(" + locationId + "--" + productId + ")");
			resultSet = statement.executeQuery();
			
			// will put cursor before the first row
//			resultSet.beforeFirst(); 
			// will put cursor after the last line
//			resultSet.last(); 
//			int noOfRows = resultSet.getRow();
			
//			logger.debug("noOfRows:" + noOfRows);
			logger.info("Movement Query is Completed...(" + locationId + "--" + productId + ")" + (System.currentTimeMillis() - startTime));
			
			long count = 0;
			startTime = System.currentTimeMillis();
			logger.info("Reading the result set is Started...(" + locationId + "--" + productId + ")" + (System.currentTimeMillis() - startTime));
			while(resultSet.next()){ 
				long rowNo = resultSet.getLong("RN");
				count = count + 1;
//				if(count >= 100000000) {
//					logger.debug("rowNo:" + rowNo);
//					break;
//				}
				
				if(count >= 1500000){
					logger.debug("rowNo:" + rowNo);
					break;
				}
				
			}
			logger.info("Reading the result set is Completed...(" + locationId + "--" + productId + ")" + (System.currentTimeMillis() - startTime));
			
			logger.info("No of Rows:" + count);
		}
		catch (SQLException e)
		{
			logger.error("Error while executing retrieveMovementDataForZone " + e);
			throw new GeneralException("Error while executing retrieveMovementDataForZone " + e);
		}
		finally{
			PristineDBUtil.close(resultSet);
			PristineDBUtil.close(statement);
		}
	}
	
	private HashMap<ItemKey, PRItemDTO> getAllActiveItems() {
		HashMap<ItemKey, PRItemDTO> itemDataMap = new HashMap<ItemKey, PRItemDTO>();
		
		String query = "SELECT IL.ITEM_CODE FROM " + "(SELECT CHILD_PRODUCT_ID FROM (SELECT * FROM PRODUCT_GROUP_RELATION PGR "
				+ "START WITH PRODUCT_LEVEL_ID = 4 AND PRODUCT_ID = " + productId + " CONNECT BY  PRIOR CHILD_PRODUCT_ID = PRODUCT_ID "
				+ " AND  PRIOR CHILD_PRODUCT_LEVEL_ID = PRODUCT_LEVEL_ID)  " + "WHERE CHILD_PRODUCT_LEVEL_ID = 1 "
				+ ") ITEMS JOIN ITEM_LOOKUP IL ON ITEMS.CHILD_PRODUCT_ID = IL.ITEM_CODE WHERE IL.ACTIVE_INDICATOR = 'Y'";
		
		PreparedStatement statement = null;
	    ResultSet resultSet = null;
		try{
			statement = conn.prepareStatement(query);
			statement.setFetchSize(100000);
			resultSet = statement.executeQuery();
			
			while(resultSet.next()){ 
				ItemKey itemKey = new ItemKey(resultSet.getInt("ITEM_CODE"), PRConstants.NON_LIG_ITEM_INDICATOR);
				PRItemDTO itemDTO = new PRItemDTO();
				itemDTO.setItemCode(resultSet.getInt("ITEM_CODE"));
				itemDataMap.put(itemKey, itemDTO);
			}
		}
		catch (SQLException e)
		{
			logger.error("Error while executing retrieveMovementDataForZone " + e);
		}
		finally{
			PristineDBUtil.close(resultSet);
			PristineDBUtil.close(statement);
		}
		
		return itemDataMap;
	}
	
	public void intialSetup() {
		initialize();
	}
	
	protected void setConnection() {
		if (conn == null) {
			try {
				conn = DBManager.getConnection();
			} catch (GeneralException exe) {
				logger.error("Error while connecting to DB:" + exe);
				System.exit(1);
			}
		}
	}

	protected void setConnection(Connection conn) {
		this.conn = conn;
	}

	/**
	 * Initializes object
	 */
	protected void initialize() {
		setConnection();
	}
}
