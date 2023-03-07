/*
 * Title: DAO class for Retail Cost Setup
 *
 *******************************************************************************
 * Modification History
 *------------------------------------------------------------------------------
 * Version		Date		Name			Remarks
 *------------------------------------------------------------------------------
 * Version 0.1	04/23/2012	Janani			Initial Version 
 * Version 0.2	08/20/2012	Janani			Changes to include offInvoiceCost 
 * 											for TOPS in RETAIL_COST_INFO
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
import java.util.Date;
import java.util.HashMap;
import java.util.List;
import java.util.Set;

import org.apache.log4j.Logger;

import com.pristine.dto.ItemDetailKey;
import com.pristine.dto.PriceAndCostDTO;
import com.pristine.dto.RetailCostDTO;
import com.pristine.dto.StoreItemKey;
import com.pristine.exception.GeneralException;
import com.pristine.util.Constants;
import com.pristine.util.DateUtil;
import com.pristine.util.PrestoUtil;
import com.pristine.util.PristineDBUtil;
import com.pristine.util.PropertyManager;
import com.pristine.util.offermgmt.PRCommonUtil;

public class RetailCostDAO {
	
	static Logger logger = Logger.getLogger("RetailCostDAO");
	private static final int commitCount = Constants.LIMIT_COUNT;
	
	private static final String GET_RETAIL_COST_INFO = "SELECT ITEM_CODE, CALENDAR_ID, LEVEL_TYPE_ID, LEVEL_ID, LIST_COST, DEAL_COST, PROMOTION_FLG, " +
													   "TO_CHAR(EFF_LIST_COST_DATE,'MM/dd/yyyy') EFF_LIST_COST_DATE, TO_CHAR(DEAL_START_DATE,'MM/dd/yyyy') DEAL_START_DATE, " +
													   "TO_CHAR(DEAL_END_DATE,'MM/dd/yyyy') DEAL_END_DATE, LEVEL2COST, VIP_COST, LONG_TERM_FLAG, "
													   +" TO_CHAR(ALLOWANCE_START_DATE,'MM/dd/yyyy') ALLOWANCE_START_DATE, TO_CHAR(ALLOWANCE_END_DATE,'MM/dd/yyyy') ALLOWANCE_END_DATE,  " +
													   " ALLOWANCE_COST, TO_CHAR(LEVEL2_START_DATE,'MM/dd/yyyy') LEVEL2_START_DATE, TO_CHAR(LEVEL2_END_DATE,'MM/dd/yyyy') LEVEL2_END_DATE, "
													   + " IS_WHSE_MAPPED FROM RETAIL_COST_INFO WHERE CALENDAR_ID = ? AND ITEM_CODE IN (%s) ORDER BY ITEM_CODE ASC, LEVEL_TYPE_ID DESC";
	
	private static final String GET_RETAIL_COST_INFO_5WK = "SELECT ITEM_CODE, CALENDAR_ID, LEVEL_TYPE_ID, LEVEL_ID, LIST_COST, DEAL_COST, PROMOTION_FLG, " +
													   "TO_CHAR(EFF_LIST_COST_DATE,'MM/dd/yyyy') EFF_LIST_COST_DATE, TO_CHAR(DEAL_START_DATE,'MM/dd/yyyy') DEAL_START_DATE, " +
													   "TO_CHAR(DEAL_END_DATE,'MM/dd/yyyy') DEAL_END_DATE, LEVEL2COST " +
													   "FROM RETAIL_COST_INFO_5WK WHERE CALENDAR_ID = ? AND ITEM_CODE IN (%s) ORDER BY ITEM_CODE ASC, LEVEL_TYPE_ID DESC";
	
	private static final String SAVE_RETAIL_COST_INFO = "INSERT INTO RETAIL_COST_INFO (CALENDAR_ID, ITEM_CODE, LEVEL_TYPE_ID, LEVEL_ID, LIST_COST, DEAL_COST, " +
			 											 "PROMOTION_FLG, UPDATE_TIMESTAMP, EFF_LIST_COST_DATE, DEAL_START_DATE, DEAL_END_DATE, LEVEL2COST, VIP_COST, LEVEL2_START_DATE, LEVEL2_END_DATE, LOCATION_ID, ALLOWANCE_COST, "
			 											 + "SCANBACK_AMT1,SCANBACK_AMT2,SCANBACK_AMT3,SCANBACK_START_DATE1,SCANBACK_START_DATE2,SCANBACK_START_DATE3,SCANBACK_END_DATE1,SCANBACK_END_DATE2,SCANBACK_END_DATE3, IS_WHSE_MAPPED) VALUES " 
			 											 + "(?,?,?,?,?,?,?,TRUNC(SYSDATE),TO_DATE(?,'MM/dd/yy'),TO_DATE(?,'MM/dd/yy'),TO_DATE(?,'MM/dd/yy'), ?, ?, TO_DATE(?,'MM/dd/yy'), TO_DATE(?,'MM/dd/yy'),?, ?, ?, ?, ?, "
			 											 + "TO_DATE(?,'MM/dd/yy'),TO_DATE(?,'MM/dd/yy'),TO_DATE(?,'MM/dd/yy'), "
			 											 + "TO_DATE(?,'MM/dd/yy'),TO_DATE(?,'MM/dd/yy'),TO_DATE(?,'MM/dd/yy'), ?)";
	
	private static final String SAVE_RETAIL_COST_INFO_WITH_LONG_TERM_AND_ALLOWANCE = "INSERT INTO RETAIL_COST_INFO (CALENDAR_ID, ITEM_CODE, LEVEL_TYPE_ID, LEVEL_ID, LIST_COST, DEAL_COST, " +
			 "PROMOTION_FLG, UPDATE_TIMESTAMP, EFF_LIST_COST_DATE, DEAL_START_DATE, DEAL_END_DATE, LEVEL2COST, VIP_COST, LEVEL2_START_DATE, LEVEL2_END_DATE, LOCATION_ID, ALLOWANCE_COST, "
			 + "SCANBACK_AMT1,SCANBACK_AMT2,SCANBACK_AMT3,SCANBACK_START_DATE1,SCANBACK_START_DATE2,SCANBACK_START_DATE3,SCANBACK_END_DATE1,SCANBACK_END_DATE2,SCANBACK_END_DATE3, "
			 + "ALLOWANCE_START_DATE, ALLOWANCE_END_DATE, LONG_TERM_FLAG, LIST_COST_2, LONG_TERM_SCAN_1, LONG_TERM_SCAN_2, LONG_TERM_SCAN_3, IS_WHSE_MAPPED) VALUES " 
			 + "(?,?,?,?,?,?,?,TRUNC(SYSDATE),TO_DATE(?,'MM/dd/yy'),TO_DATE(?,'MM/dd/yy'),TO_DATE(?,'MM/dd/yy'), ?, ?, TO_DATE(?,'MM/dd/yy'), TO_DATE(?,'MM/dd/yy'),?, ?, ?, ?, ?, "
			 + "TO_DATE(?,'MM/dd/yy'),TO_DATE(?,'MM/dd/yy'),TO_DATE(?,'MM/dd/yy'), "
			 + "TO_DATE(?,'MM/dd/yy'),TO_DATE(?,'MM/dd/yy'),TO_DATE(?,'MM/dd/yy'), TO_DATE(?,'MM/dd/yy'), TO_DATE(?,'MM/dd/yy'), ?, ?, ?, ?, ?, ?)";
	
	private static final String DELETE_RETAIL_COST_INFO = "DELETE FROM RETAIL_COST_INFO WHERE CALENDAR_ID = ? AND ITEM_CODE = ? AND LEVEL_TYPE_ID=? AND LEVEL_ID=?";
	
	private static final String UPDATE_RETAIL_COST_INFO = "UPDATE RETAIL_COST_INFO SET LIST_COST=?, DEAL_COST=?, PROMOTION_FLG=?, UPDATE_TIMESTAMP=TRUNC(SYSDATE), " +
			   											   "EFF_LIST_COST_DATE=TO_DATE(?,'MM/dd/yy'), DEAL_START_DATE=TO_DATE(?,'MM/dd/yy'), DEAL_END_DATE=TO_DATE(?,'MM/dd/yy'), VIP_COST=?, LOCATION_ID = ?, " +
			   											   "IS_WHSE_MAPPED = ? WHERE CALENDAR_ID = ? AND ITEM_CODE = ? AND LEVEL_TYPE_ID=? AND LEVEL_ID=?";
	
	private static final String GET_LATEST_RETAIL_COST_INFO = "SELECT ITEM_CODE, CALENDAR_ID, LEVEL_TYPE_ID, LEVEL_ID, LIST_COST, DEAL_COST, PROMOTION_FLG, " +
															   "TO_CHAR(EFF_LIST_COST_DATE,'MM/dd/yyyy') EFF_LIST_COST_DATE, TO_CHAR(DEAL_START_DATE,'MM/dd/yyyy') DEAL_START_DATE, " +
															   "TO_CHAR(DEAL_END_DATE,'MM/dd/yyyy') DEAL_END_DATE, LEVEL2COST, VIP_COST, LONG_TERM_FLAG , "
															   +" TO_CHAR(ALLOWANCE_START_DATE,'MM/dd/yyyy') ALLOWANCE_START_DATE, TO_CHAR(ALLOWANCE_END_DATE,'MM/dd/yyyy') ALLOWANCE_END_DATE,  " +
															   " ALLOWANCE_COST, TO_CHAR(LEVEL2_START_DATE,'MM/dd/yyyy') LEVEL2_START_DATE, TO_CHAR(LEVEL2_END_DATE,'MM/dd/yyyy') LEVEL2_END_DATE "
															   + "FROM RETAIL_COST_INFO WHERE (CALENDAR_ID, ITEM_CODE) IN " +
															   "(SELECT MAX(CALENDAR_ID) CALENDAR_ID, ITEM_CODE FROM RETAIL_COST_INFO " +
															   "WHERE CALENDAR_ID <= ? AND ITEM_CODE IN (%s) GROUP BY ITEM_CODE) ORDER BY ITEM_CODE ASC, LEVEL_TYPE_ID DESC";
	
	private static final String GET_RETAIL_COST_INFO_FROM_HISTORY = "SELECT ITEM_CODE, CALENDAR_ID, LEVEL_TYPE_ID, LEVEL_ID, LIST_COST, DEAL_COST, PROMOTION_FLG, " +
															   "TO_CHAR(EFF_LIST_COST_DATE,'MM/dd/yyyy') EFF_LIST_COST_DATE, TO_CHAR(DEAL_START_DATE,'MM/dd/yyyy') DEAL_START_DATE, " +
															   "TO_CHAR(DEAL_END_DATE,'MM/dd/yyyy') DEAL_END_DATE, LEVEL2COST " +
															   "FROM RETAIL_COST_INFO WHERE (CALENDAR_ID, ITEM_CODE) IN " +
															   "(SELECT MAX(CALENDAR_ID) CALENDAR_ID, ITEM_CODE FROM RETAIL_COST_INFO " +
															   "WHERE CALENDAR_ID IN (%c) AND ITEM_CODE IN (%s) GROUP BY ITEM_CODE) ORDER BY ITEM_CODE ASC, LEVEL_TYPE_ID DESC";
	
	private static final String DELETE_RETAIL_COST_INFO_WEEKLY_ITEMS = "DELETE FROM RETAIL_COST_INFO WHERE CALENDAR_ID = ? AND ITEM_CODE = ?";
	private static final String DELETE_RETAIL_COST_INFO_WEEKLY_USING_SCANBACK_ITEMS = "DELETE FROM RETAIL_COST_INFO WHERE CALENDAR_ID = ? AND ITEM_CODE IN (%ITEM_CODE%) ";
	private static final String DELETE_RETAIL_COST_INFO_WEEKLY = "DELETE FROM RETAIL_COST_INFO WHERE CALENDAR_ID = ?";
	
	private static final String SETUP_RETAIL_COST_DATAV2 = "INSERT INTO RETAIL_COST_INFO (CALENDAR_ID, ITEM_CODE, LEVEL_TYPE_ID, LEVEL_ID, LIST_COST, EFF_LIST_COST_DATE, DEAL_COST, DEAL_START_DATE, DEAL_END_DATE, PROMOTION_FLG, " + 
														 "LEVEL2COST, UPDATE_TIMESTAMP, VIP_COST) (SELECT ?, ITEM_CODE, LEVEL_TYPE_ID, LEVEL_ID, LIST_COST, EFF_LIST_COST_DATE, LEVEL2COST, DEAL_START_DATE, DEAL_END_DATE, PROMOTION_FLG, " + 
														 "LEVEL2COST, SYSDATE, VIP_COST FROM RETAIL_COST_INFO WHERE CALENDAR_ID = ? )";
	
	
	private static final String SETUP_RETAIL_COST_DATA = "INSERT INTO RETAIL_COST_INFO (CALENDAR_ID, ITEM_CODE, LEVEL_TYPE_ID, LEVEL_ID, LIST_COST, EFF_LIST_COST_DATE, LEVEL2COST, " + 
			"LEVEL2_START_DATE, LEVEL2_END_DATE, UPDATE_TIMESTAMP, LOCATION_ID) (SELECT ?, ITEM_CODE, LEVEL_TYPE_ID, LEVEL_ID, LIST_COST, EFF_LIST_COST_DATE, " + 
			"(CASE WHEN RETAIL_CALENDAR.START_DATE >= LEVEL2_START_DATE AND RETAIL_CALENDAR.START_DATE <= LEVEL2_END_DATE THEN LEVEL2COST ELSE 0 END) LEVEL2COST, " +
			"(CASE WHEN RETAIL_CALENDAR.START_DATE >= LEVEL2_START_DATE AND RETAIL_CALENDAR.START_DATE <= LEVEL2_END_DATE " +
			"THEN LEVEL2_START_DATE ELSE NULL END) LEVEL2_START_DATE, " +
			"(CASE WHEN RETAIL_CALENDAR.START_DATE >= LEVEL2_START_DATE AND RETAIL_CALENDAR.START_DATE <= LEVEL2_END_DATE  " +
			"THEN LEVEL2_END_DATE ELSE NULL END) LEVEL2_END_DATE, " +
			"SYSDATE, LOCATION_ID FROM RETAIL_COST_INFO, RETAIL_CALENDAR WHERE RETAIL_COST_INFO.CALENDAR_ID = ? AND RETAIL_CALENDAR.CALENDAR_ID = ?)";
	
	private static final String SETUP_TEMP_RETAIL_COST_DATA = "INSERT INTO TEMP_RETAIL_COST_INFO (CALENDAR_ID, ITEM_CODE, LEVEL_TYPE_ID, LEVEL_ID, LIST_COST, EFF_LIST_COST_DATE, DEAL_COST, DEAL_START_DATE, DEAL_END_DATE, PROMOTION_FLG, " + 
			 "LEVEL2COST, UPDATE_TIMESTAMP, VIP_COST) (SELECT CALENDAR_ID, ITEM_CODE, LEVEL_TYPE_ID, LEVEL_ID, LIST_COST, EFF_LIST_COST_DATE, LEVEL2COST, DEAL_START_DATE, DEAL_END_DATE, PROMOTION_FLG, " + 
			 "LEVEL2COST, SYSDATE, VIP_COST FROM RETAIL_COST_INFO WHERE CALENDAR_ID = ? )";
	private static final String RESTORE_RETAIL_COST_DATA_FROM_TEMP = "INSERT INTO RETAIL_COST_INFO (CALENDAR_ID, ITEM_CODE, LEVEL_TYPE_ID, LEVEL_ID, LIST_COST, EFF_LIST_COST_DATE, DEAL_COST, DEAL_START_DATE, DEAL_END_DATE, PROMOTION_FLG, " + 
			 "LEVEL2COST, UPDATE_TIMESTAMP, VIP_COST) (SELECT CALENDAR_ID, ITEM_CODE, LEVEL_TYPE_ID, LEVEL_ID, LIST_COST, EFF_LIST_COST_DATE, LEVEL2COST, DEAL_START_DATE, DEAL_END_DATE, PROMOTION_FLG, " + 
			 "LEVEL2COST, SYSDATE, VIP_COST FROM TEMP_RETAIL_COST_INFO WHERE CALENDAR_ID = ? )";

	private static final String GET_CHAIN_LEVEL_COST = "SELECT LIST_COST, DEAL_COST FROM RETAIL_COST_INFO WHERE CALENDAR_ID = ? AND ITEM_CODE = ? AND LEVEL_TYPE_ID = 0";
	private static final String CHECK_UPC_FOR_WITH_STORE= "SELECT IL.UPC, CS.COMP_STR_NO FROM ITEM_LOOKUP IL, STORE_ITEM_MAP SIM , COMPETITOR_STORE CS WHERE IL.ITEM_CODE = SIM.ITEM_CODE " +
			  											  " AND SIM.LEVEL_ID = CS.COMP_STR_ID AND CS.COMP_CHAIN_ID = ? AND IL.UPC IN (%s)";
	
	
	//levelTypeId, levelId, getItemcode(), Constants.COST_INDICATOR, Constants.DSD, vendorId, Constants.AUTHORIZED_ITEM
	private static final String MERGE_INTO_STORE_ITEM_MAP = "MERGE INTO STORE_ITEM_MAP STM USING " +
			"(SELECT ? LEVEL_TYPE_ID, ? LEVEL_ID, ? ITEM_CODE, ? COST_INDICATOR, ? DIST_FLAG, ? VENDOR_ID, ? IS_AUTHORIZED FROM DUAL) D " +
			"ON (STM.LEVEL_TYPE_ID = D.LEVEL_TYPE_ID AND STM.LEVEL_ID = D.LEVEL_ID AND STM.ITEM_CODE = D.ITEM_CODE) " +
			"WHEN MATCHED THEN UPDATE SET STM.UPDATE_TIMESTAMP = SYSDATE, STM.COST_UPDATE_TIMESTAMP = SYSDATE, " + 
			"STM.DIST_FLAG = D.DIST_FLAG, STM.VENDOR_ID = D.VENDOR_ID, STM.COST_INDICATOR =  D.COST_INDICATOR, " +
			" STM.IS_AUTHORIZED = D.IS_AUTHORIZED " +
			"WHEN NOT MATCHED THEN INSERT (STM.LEVEL_TYPE_ID, STM.LEVEL_ID, STM.ITEM_CODE, " + 
			"STM.UPDATE_TIMESTAMP, STM.COST_UPDATE_TIMESTAMP, STM.COST_INDICATOR, STM.DIST_FLAG, STM.VENDOR_ID, STM.IS_AUTHORIZED) " +
			"VALUES (D.LEVEL_TYPE_ID, D.LEVEL_ID, D.ITEM_CODE, SYSDATE, SYSDATE, D.COST_INDICATOR, D.DIST_FLAG, D.VENDOR_ID, D.IS_AUTHORIZED)";
	
	
	
	private static final String MERGE_INTO_STORE_ITEM_MAP_WITH_PRC_ZONE = "MERGE INTO STORE_ITEM_MAP STM USING " +
			"(SELECT ? LEVEL_TYPE_ID, ? LEVEL_ID, ? ITEM_CODE, ? COST_INDICATOR, ? DIST_FLAG, ? VENDOR_ID, " + 
			" ? PRICE_ZONE_ID, ? IS_AUTHORIZED FROM DUAL) D " +
			"ON (STM.LEVEL_TYPE_ID = D.LEVEL_TYPE_ID AND STM.LEVEL_ID = D.LEVEL_ID AND STM.ITEM_CODE = D.ITEM_CODE " + 
			"AND STM.PRICE_ZONE_ID = D.PRICE_ZONE_ID) " +
			"WHEN MATCHED THEN UPDATE SET STM.UPDATE_TIMESTAMP = SYSDATE, STM.COST_UPDATE_TIMESTAMP = SYSDATE, " + 
			"STM.DIST_FLAG = D.DIST_FLAG, STM.VENDOR_ID = D.VENDOR_ID, STM.COST_INDICATOR =  D.COST_INDICATOR, " +
			"STM.IS_AUTHORIZED = D.IS_AUTHORIZED " +
			"WHEN NOT MATCHED THEN INSERT (STM.LEVEL_TYPE_ID, STM.LEVEL_ID, STM.ITEM_CODE, " + 
			"STM.UPDATE_TIMESTAMP, STM.COST_UPDATE_TIMESTAMP, STM.COST_INDICATOR, STM.DIST_FLAG, STM.VENDOR_ID, " + 
			"STM.PRICE_ZONE_ID, STM.IS_AUTHORIZED) " +
			"VALUES (D.LEVEL_TYPE_ID, D.LEVEL_ID, D.ITEM_CODE, SYSDATE, SYSDATE, D.COST_INDICATOR, D.DIST_FLAG, " + 
			"D.VENDOR_ID, D.PRICE_ZONE_ID, D.IS_AUTHORIZED)";
	
	
	
	
	private static final String RETRIEVE_ITEM_STORE_MAPPING = "SELECT LEVEL_TYPE_ID, LEVEL_ID, ITEM_CODE,  UPDATE_TIMESTAMP FROM STORE_ITEM_MAP " +
			  												 " WHERE ITEM_CODE IN (%s) ";
	
	
	private static final String RETRIEVE_ITEM_STORE_MAPPING_WITH_ZONE = "SELECT SIM.LEVEL_TYPE_ID, SIM.LEVEL_ID, SIM.ITEM_CODE,  "
						+ "	SIM.UPDATE_TIMESTAMP, RPZ.ZONE_NUM, CS.COMP_STR_NO FROM STORE_ITEM_MAP SIM LEFT JOIN RETAIL_PRICE_ZONE RPZ"
						+ " ON  RPZ.PRICE_ZONE_ID = SIM.PRICE_ZONE_ID "
						+ " LEFT JOIN COMPETITOR_STORE CS" 
						+ " ON CS.COMP_STR_ID = SIM.LEVEL_ID "
						+ " WHERE SIM.IS_AUTHORIZED = 'Y' AND SIM.ITEM_CODE IN (%s) ";
						
	private static final String INSERT_UNPROCESSED_RECORDS = "INSERT INTO UNPROCESSED_COST_RECORDS" +
			  												 "(FILE_NAME, FILE_EFF_DATE, RETAILER_ITEM_CODE, LEVEL_ID, EFF_LIST_COST_DATE, VIP_COST, AVG_COST, VENDOR_NUMBER, LIST_COST, VENDOR_NAME)" +
			  												 "VALUES(?, TO_DATE(? ,'MM/dd/yyyy'), ?, ?, TO_DATE(? ,'MM/dd/yyyy'), ?, ?, ?, ?, ?)";
	
	
	private static final String CLEAR_DEAL_COST = "UPDATE RETAIL_COST_INFO SET DEAL_COST = 0, DEAL_START_DATE = NULL, DEAL_END_DATE = NULL WHERE CALENDAR_ID = ?";
	
	private static final String POPULATE_DEAL_AS_LEVEL2 = "UPDATE RETAIL_COST_INFO SET DEAL_COST=LEVEL2COST, DEAL_START_DATE = LEVEL2_START_DATE, DEAL_END_DATE = LEVEL2_END_DATE WHERE " +
															"CALENDAR_ID = ? AND LEVEL2COST <> 0";
	
	
	private static final String CHECK_COST_AVAILABILITY_FOR_WEEK = "SELECT CALENDAR_ID FROM RETAIL_COST_INFO WHERE CALENDAR_ID = ?";
	
	private static final String SAVE_IGNORED_COST_RECORDS = "INSERT INTO IGNORED_COST_RECORDS (CALENDAR_ID, RECORD_TYPE, SRC_VENDOR_AND_ITEM_ID, "
														+ "UPC, LEVEL_ID, STORE_OR_ZONE_NO, EFF_DATE, LIST_COST, PROMO_EFF_START_DATE, PROMO_EFF_END_DATE, "
														+ "PROMO_COST, COMPANY_PACK, PROCESSED_DATE) VALUES (?, ?, ?, ?, ?, ?, TO_DATE(?,'MM/dd/yy'), ?, "
														+ "TO_DATE(?,'MM/dd/yy'), TO_DATE(?,'MM/dd/yy'), ?, ?, SYSDATE)";
	
	private static final String DELETE_FUTURE_COST = "DELETE FROM RETAIL_COST_INFO WHERE CALENDAR_ID IN (SELECT CALENDAR_ID FROM RETAIL_CALENDAR "
													+ " WHERE START_DATE > TO_DATE(?, '" + Constants.DB_DATE_FORMAT + "') AND ROW_TYPE = 'W') AND "
													+ " ITEM_CODE = ? AND LEVEL_ID = ? AND LEVEL_TYPE_ID = ?";
	
	private static final String GET_RETAIL_COST_INFO_TEMP = "SELECT ITEM_CODE, CALENDAR_ID, LEVEL_TYPE_ID, LEVEL_ID, LIST_COST, DEAL_COST, PROMOTION_FLG, " +
			   "TO_CHAR(EFF_LIST_COST_DATE,'MM/dd/yyyy') EFF_LIST_COST_DATE, TO_CHAR(DEAL_START_DATE,'MM/dd/yyyy') DEAL_START_DATE, " +
			   "TO_CHAR(DEAL_END_DATE,'MM/dd/yyyy') DEAL_END_DATE, LEVEL2COST, VIP_COST " +
			   "FROM TEMP_RETAIL_COST_INFO WHERE CALENDAR_ID = ? ORDER BY ITEM_CODE ASC, LEVEL_TYPE_ID DESC";
	
	private static final String GET_PRICE_ZONE_FROM_STORE_ITEM_MAP = "SELECT DISTINCT(RPZ.ZONE_NUM) AS PRICE_ZONE, ITEM_CODE FROM STORE_ITEM_MAP SIM "
			+ "LEFT JOIN RETAIL_PRICE_ZONE RPZ ON SIM.PRICE_ZONE_ID = RPZ.PRICE_ZONE_ID WHERE ITEM_CODE IN(%ITEM_CODE%) AND IS_AUTHORIZED = 'Y'";
	
	private static final String UPDATE_LISTCOST2_IN_RETAIL_PRICE_INFO = "UPDATE RETAIL_COST_INFO SET LIST_COST_2 = (LIST_COST - ALLOWANCE_COST)"
			+ " WHERE LONG_TERM_FLAG = 'Y' AND ALLOWANCE_COST > 0 AND CALENDAR_ID = ?";
	
	private static final String UPDATE_LISTCOST2_IN_RETAIL_PRICE_INFO_CORRECTION = "UPDATE RETAIL_COST_INFO SET LIST_COST_2 = (LIST_COST - ALLOWANCE_COST)"
			+ " WHERE LONG_TERM_FLAG = 'Y' AND ALLOWANCE_COST > 0 AND CALENDAR_ID = ? AND (LIST_COST_2 = 0 OR LIST_COST_2 IS NULL)";
	
	private static final String DELETE_FUTURE_RETAIL_COST_INFO = "DELETE FROM RETAIL_COST_INFO WHERE CALENDAR_ID IN (SELECT CALENDAR_ID FROM RETAIL_CALENDAR"
			+ " WHERE ROW_TYPE='W' AND START_DATE >(SELECT START_DATE FROM RETAIL_CALENDAR WHERE CALENDAR_ID = ?))";
	
	private static final String DELETE_CUR_AND_FUTURE_RETAIL_COST_INFO= "DELETE FROM RETAIL_COST_INFO WHERE CALENDAR_ID IN (SELECT CALENDAR_ID FROM "
			+ " RETAIL_CALENDAR WHERE ROW_TYPE = 'W' AND START_DATE >= TO_DATE(?,'MM/dd/yyyy') AND START_DATE   <= TO_DATE(?,'MM/dd/yyyy') + (? * 7))";
	
	/**
	 * This method retrieves values from RETAIL_COST_INFO based on ITEM_CODE
	 * @param conn				Connection
	 * @param itemCodeSet		Set containing item codes for which data needs to be retrieved from retail_cost_info
	 * @param calendarId		Calendar Id
	 * @return HashMap containing item code as key and list of its cost data as value
	 * @throws GeneralException
	 */
	public HashMap<String, List<RetailCostDTO>> getRetailCostInfo(Connection conn, Set<String> itemCodeSet, int calendarId, boolean isLatest) throws GeneralException{
		logger.debug("Inside getRetailCostInfo() of RetailCostDAO");
		
		List<String> itemCodeList = new ArrayList<String>();
	    HashMap<String, List<RetailCostDTO>> retailCostDBMap = new HashMap<String, List<RetailCostDTO>>();
	    int limitcount=0;
		for(String itemCode:itemCodeSet){
			itemCodeList.add(itemCode);
			limitcount++;
			if( limitcount > 0 && (limitcount%this.commitCount == 0)){
				Object[] values = itemCodeList.toArray();
				List<RetailCostDTO> retailCostDBList = retrieveRetailCostInfo(conn, calendarId, isLatest, values);
				
				StringBuffer valuesBuff = new StringBuffer();
				for(int i = 0;i <values.length; i++){
					valuesBuff.append(values[i] + ",");
				}
				for(RetailCostDTO retailCostDTO:retailCostDBList){
					String dbItemCOde = retailCostDTO.getItemcode();
					if(retailCostDBMap.get(dbItemCOde) != null){
		        		List<RetailCostDTO> tempList = retailCostDBMap.get(dbItemCOde);
		        		tempList.add(retailCostDTO);
		        		retailCostDBMap.put(dbItemCOde, tempList);
		        	}else{
		        		List<RetailCostDTO> tempList = new ArrayList<RetailCostDTO>();
		        		tempList.add(retailCostDTO);
		        		retailCostDBMap.put(dbItemCOde, tempList);
		        	}
				}
				itemCodeList.clear();
            }
		}
		if(itemCodeList.size() > 0){
			Object[] values = itemCodeList.toArray();
			List<RetailCostDTO> retailCostDBList = retrieveRetailCostInfo(conn, calendarId, isLatest, values);
			StringBuffer valuesBuff = new StringBuffer();
			for(int i = 0;i <values.length; i++){
				valuesBuff.append(values[i] + ",");
			}
			for(RetailCostDTO retailCostDTO:retailCostDBList){
				String dbItemCOde = retailCostDTO.getItemcode();
				if(retailCostDBMap.get(dbItemCOde) != null){
	        		List<RetailCostDTO> tempList = retailCostDBMap.get(dbItemCOde);
	        		tempList.add(retailCostDTO);
	        		retailCostDBMap.put(dbItemCOde, tempList);
	        	}else{
	        		List<RetailCostDTO> tempList = new ArrayList<RetailCostDTO>();
	        		tempList.add(retailCostDTO);
	        		retailCostDBMap.put(dbItemCOde, tempList);
	        	}
			}
			itemCodeList.clear();
		}
		return retailCostDBMap;
	}
	
	/**
	 * This method queries the database for Retail Cost Info for every set of Item Codes
	 * @param conn			Connection
	 * @param calendarId	Calendar Id
	 * @param values		Array of UPCs that will be passed as input to the query
	 * @return List of retail cost data
	 * @throws GeneralException
	 */
	private List<RetailCostDTO> retrieveRetailCostInfo(Connection conn, int calendarId, boolean isLatest, Object... values) throws GeneralException{
		logger.debug("Inside retrieveRetailCostInfo() of RetailCostDAO");
		PreparedStatement statement = null;
	    ResultSet resultSet = null;
	    List<RetailCostDTO> retailCostDTOList = new ArrayList<RetailCostDTO>();
		try{
			long startTime = System.currentTimeMillis();
			
			/* Else block added to retrieve latest retail cost info for item codes where no calendar id will be specified.
			 * This will be used when loading competitive data table from retail cost info table
			 */
			if(!isLatest){
				String qry = String.format(GET_RETAIL_COST_INFO, PristineDBUtil.preparePlaceHolders(values.length));
				statement = conn.prepareStatement(qry);
				statement.setInt(1, calendarId);
				PristineDBUtil.setValues(statement, 2, values);
			}else{
				statement = conn.prepareStatement(String.format(GET_LATEST_RETAIL_COST_INFO, PristineDBUtil.preparePlaceHolders(values.length)));
				statement.setInt(1, calendarId);
				PristineDBUtil.setValues(statement, 2, values);
			}
			
	        resultSet = statement.executeQuery();
	        
	        RetailCostDTO retailCostDTO;
	        while(resultSet.next()){
	        	retailCostDTO = new RetailCostDTO();
	        	retailCostDTO.setItemcode(resultSet.getString("ITEM_CODE"));
	        	retailCostDTO.setCalendarId(resultSet.getInt("CALENDAR_ID"));
	        	retailCostDTO.setLevelTypeId(resultSet.getInt("LEVEL_TYPE_ID"));
	        	retailCostDTO.setLevelId(resultSet.getString("LEVEL_ID"));
	        	retailCostDTO.setListCost(resultSet.getFloat("LIST_COST"));
	        	retailCostDTO.setEffListCostDate(resultSet.getString("EFF_LIST_COST_DATE"));
	        	retailCostDTO.setDealCost(resultSet.getFloat("DEAL_COST"));
	        	retailCostDTO.setDealStartDate(resultSet.getString("DEAL_START_DATE"));
	        	retailCostDTO.setDealEndDate(resultSet.getString("DEAL_END_DATE"));
	        	retailCostDTO.setPromotionFlag(resultSet.getString("PROMOTION_FLG"));
	        	retailCostDTO.setLevel2Cost(resultSet.getFloat("LEVEL2COST"));
	        	retailCostDTO.setLevel2StartDate(resultSet.getString("LEVEL2_START_DATE"));
	        	retailCostDTO.setLevel2EndDate(resultSet.getString("LEVEL2_END_DATE"));
	        	retailCostDTO.setVipCost(resultSet.getFloat("VIP_COST"));
	        	retailCostDTO.setLongTermFlag(resultSet.getString("LONG_TERM_FLAG"));
	        	retailCostDTO.setAllowanceAmount(resultSet.getFloat("ALLOWANCE_COST"));
	        	retailCostDTO.setAllowStartDate(resultSet.getString("ALLOWANCE_START_DATE"));
	        	retailCostDTO.setAllowEndDate(resultSet.getString("ALLOWANCE_END_DATE"));
	        	String isWhseMapped = resultSet.getString("IS_WHSE_MAPPED");
	        	retailCostDTO.setWhseZoneRolledUpRecord((isWhseMapped !=null && 
	        			String.valueOf(Constants.YES).equals(isWhseMapped)) ? true: false);
	        	retailCostDTOList.add(retailCostDTO);
	        }
	        long endTime = System.currentTimeMillis();
	        logger.debug("Time taken to execute and store cost info in list- " + (endTime - startTime));
		}
		catch (SQLException e)
		{
			logger.error("Error while executing GET_RETAIL_COST_INFO");
			throw new GeneralException("Error while executing GET_RETAIL_COST_INFO", e);
		}finally{
			PristineDBUtil.close(resultSet);
			PristineDBUtil.close(statement);
		}
		return retailCostDTOList;
	}
	
	public HashMap<String, List<RetailCostDTO>> getRetailCostInfo5Wk(Connection conn, Set<String> itemCodeSet, int calendarId, boolean isLatest) throws GeneralException{
		logger.debug("Inside getRetailCostInfo() of RetailCostDAO");
		
		List<String> itemCodeList = new ArrayList<String>();
	    HashMap<String, List<RetailCostDTO>> retailCostDBMap = new HashMap<String, List<RetailCostDTO>>();
	    int limitcount=0;
		for(String itemCode:itemCodeSet){
			itemCodeList.add(itemCode);
			limitcount++;
			if( limitcount > 0 && (limitcount%this.commitCount == 0)){
				Object[] values = itemCodeList.toArray();
				List<RetailCostDTO> retailCostDBList = retrieveRetailCostInfo5Wk(conn, calendarId, isLatest, values);
				
				StringBuffer valuesBuff = new StringBuffer();
				for(int i = 0;i <values.length; i++){
					valuesBuff.append(values[i] + ",");
				}
				for(RetailCostDTO retailCostDTO:retailCostDBList){
					String dbItemCOde = retailCostDTO.getItemcode();
					if(retailCostDBMap.get(dbItemCOde) != null){
		        		List<RetailCostDTO> tempList = retailCostDBMap.get(dbItemCOde);
		        		tempList.add(retailCostDTO);
		        		retailCostDBMap.put(dbItemCOde, tempList);
		        	}else{
		        		List<RetailCostDTO> tempList = new ArrayList<RetailCostDTO>();
		        		tempList.add(retailCostDTO);
		        		retailCostDBMap.put(dbItemCOde, tempList);
		        	}
				}
				itemCodeList.clear();
            }
		}
		if(itemCodeList.size() > 0){
			Object[] values = itemCodeList.toArray();
			List<RetailCostDTO> retailCostDBList = retrieveRetailCostInfo5Wk(conn, calendarId, isLatest, values);
			StringBuffer valuesBuff = new StringBuffer();
			for(int i = 0;i <values.length; i++){
				valuesBuff.append(values[i] + ",");
			}
			for(RetailCostDTO retailCostDTO:retailCostDBList){
				String dbItemCOde = retailCostDTO.getItemcode();
				if(retailCostDBMap.get(dbItemCOde) != null){
	        		List<RetailCostDTO> tempList = retailCostDBMap.get(dbItemCOde);
	        		tempList.add(retailCostDTO);
	        		retailCostDBMap.put(dbItemCOde, tempList);
	        	}else{
	        		List<RetailCostDTO> tempList = new ArrayList<RetailCostDTO>();
	        		tempList.add(retailCostDTO);
	        		retailCostDBMap.put(dbItemCOde, tempList);
	        	}
			}
			itemCodeList.clear();
		}
		return retailCostDBMap;
	}
	
	private List<RetailCostDTO> retrieveRetailCostInfo5Wk(Connection conn, int calendarId, boolean isLatest, Object... values) throws GeneralException{
		logger.debug("Inside retrieveRetailCostInfo() of RetailCostDAO");
		PreparedStatement statement = null;
	    ResultSet resultSet = null;
	    List<RetailCostDTO> retailCostDTOList = new ArrayList<RetailCostDTO>();
		try{
			long startTime = System.currentTimeMillis();
			
			/* Else block added to retrieve latest retail cost info for item codes where no calendar id will be specified.
			 * This will be used when loading competitive data table from retail cost info table
			 */
			statement = conn.prepareStatement(String.format(GET_RETAIL_COST_INFO_5WK, PristineDBUtil.preparePlaceHolders(values.length)));
			statement.setInt(1, calendarId);
			PristineDBUtil.setValues(statement, 2, values);
			statement.setFetchSize(10000);
	        resultSet = statement.executeQuery();
	        
	        RetailCostDTO retailCostDTO;
	        while(resultSet.next()){
	        	retailCostDTO = new RetailCostDTO();
	        	retailCostDTO.setItemcode(resultSet.getString("ITEM_CODE"));
	        	retailCostDTO.setCalendarId(resultSet.getInt("CALENDAR_ID"));
	        	retailCostDTO.setLevelTypeId(resultSet.getInt("LEVEL_TYPE_ID"));
	        	retailCostDTO.setLevelId(resultSet.getString("LEVEL_ID"));
	        	retailCostDTO.setListCost(resultSet.getFloat("LIST_COST"));
	        	retailCostDTO.setEffListCostDate(resultSet.getString("EFF_LIST_COST_DATE"));
	        	retailCostDTO.setDealCost(resultSet.getFloat("DEAL_COST"));
	        	retailCostDTO.setDealStartDate(resultSet.getString("DEAL_START_DATE"));
	        	retailCostDTO.setDealEndDate(resultSet.getString("DEAL_END_DATE"));
	        	retailCostDTO.setPromotionFlag(resultSet.getString("PROMOTION_FLG"));
	        	retailCostDTO.setLevel2Cost(resultSet.getFloat("LEVEL2COST"));
	        	retailCostDTOList.add(retailCostDTO);
	        }
	        long endTime = System.currentTimeMillis();
	        logger.debug("Time taken to execute and store cost info in list- " + (endTime - startTime));
		}
		catch (SQLException e)
		{
			logger.error("Error while executing GET_RETAIL_COST_INFO");
			throw new GeneralException("Error while executing GET_RETAIL_COST_INFO", e);
		}finally{
			PristineDBUtil.close(resultSet);
			PristineDBUtil.close(statement);
		}
		return retailCostDTOList;
	}

	/**
	 * This method retrieves values from RETAIL_COST_INFO based on ITEM_CODE
	 * @param conn				Connection
	 * @param itemCodeSet		Set containing item codes for which data needs to be retrieved from retail_cost_info
	 * @param calendarId		Calendar Id
	 * @return HashMap containing item code as key and list of its cost data as value
	 * @throws GeneralException
	 */
	public HashMap<String, List<RetailCostDTO>> getRetailCostInfoHistory(Connection conn, Set<String> itemCodeSet, List<Integer> calendarIdList) throws GeneralException{
		logger.debug("Inside getRetailCostInfo() of RetailCostDAO");
		
		List<String> itemCodeList = new ArrayList<String>();
	    HashMap<String, List<RetailCostDTO>> retailCostDBMap = new HashMap<String, List<RetailCostDTO>>();
	    int limitcount=0;
		for(String itemCode:itemCodeSet){
			itemCodeList.add(itemCode);
			limitcount++;
			if( limitcount > 0 && (limitcount%this.commitCount == 0)){
				Object[] values = itemCodeList.toArray();
				List<RetailCostDTO> retailCostDBList = retrieveRetailCostInfoHistory(conn, calendarIdList, values);
				
				StringBuffer valuesBuff = new StringBuffer();
				for(int i = 0;i <values.length; i++){
					valuesBuff.append(values[i] + ",");
				}
				for(RetailCostDTO retailCostDTO:retailCostDBList){
					String dbItemCOde = retailCostDTO.getItemcode();
					if(retailCostDBMap.get(dbItemCOde) != null){
		        		List<RetailCostDTO> tempList = retailCostDBMap.get(dbItemCOde);
		        		tempList.add(retailCostDTO);
		        		retailCostDBMap.put(dbItemCOde, tempList);
		        	}else{
		        		List<RetailCostDTO> tempList = new ArrayList<RetailCostDTO>();
		        		tempList.add(retailCostDTO);
		        		retailCostDBMap.put(dbItemCOde, tempList);
		        	}
				}
				itemCodeList.clear();
            }
		}
		if(itemCodeList.size() > 0){
			Object[] values = itemCodeList.toArray();
			List<RetailCostDTO> retailCostDBList = retrieveRetailCostInfoHistory(conn, calendarIdList, values);
			StringBuffer valuesBuff = new StringBuffer();
			for(int i = 0;i <values.length; i++){
				valuesBuff.append(values[i] + ",");
			}
			for(RetailCostDTO retailCostDTO:retailCostDBList){
				String dbItemCOde = retailCostDTO.getItemcode();
				if(retailCostDBMap.get(dbItemCOde) != null){
	        		List<RetailCostDTO> tempList = retailCostDBMap.get(dbItemCOde);
	        		tempList.add(retailCostDTO);
	        		retailCostDBMap.put(dbItemCOde, tempList);
	        	}else{
	        		List<RetailCostDTO> tempList = new ArrayList<RetailCostDTO>();
	        		tempList.add(retailCostDTO);
	        		retailCostDBMap.put(dbItemCOde, tempList);
	        	}
			}
			itemCodeList.clear();
		}
		return retailCostDBMap;
	}
	
	/**
	 * This method queries the database for Retail Cost Info for every set of Item Codes
	 * @param conn			Connection
	 * @param calendarId	Calendar Id
	 * @param values		Array of UPCs that will be passed as input to the query
	 * @return List of retail cost data
	 * @throws GeneralException
	 */
	private List<RetailCostDTO> retrieveRetailCostInfoHistory(Connection conn, List<Integer> calendarIdList, Object... values) throws GeneralException{
		PreparedStatement statement = null;
	    ResultSet resultSet = null;
	    List<RetailCostDTO> retailCostDTOList = new ArrayList<RetailCostDTO>();
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
			
		    String sql = GET_RETAIL_COST_INFO_FROM_HISTORY.replaceAll("%c", calIdStr.toString());
		    logger.debug("Calendar String : " + calIdStr.toString());
		    statement = conn.prepareStatement(String.format(sql, PristineDBUtil.preparePlaceHolders(values.length)));
		    PristineDBUtil.setValues(statement, values);
			
	        resultSet = statement.executeQuery();
	        
	        RetailCostDTO retailCostDTO;
	        while(resultSet.next()){
	        	retailCostDTO = new RetailCostDTO();
	        	retailCostDTO.setItemcode(resultSet.getString("ITEM_CODE"));
	        	retailCostDTO.setCalendarId(resultSet.getInt("CALENDAR_ID"));
	        	retailCostDTO.setLevelTypeId(resultSet.getInt("LEVEL_TYPE_ID"));
	        	retailCostDTO.setLevelId(resultSet.getString("LEVEL_ID"));
	        	retailCostDTO.setListCost(resultSet.getFloat("LIST_COST"));
	        	retailCostDTO.setEffListCostDate(resultSet.getString("EFF_LIST_COST_DATE"));
	        	retailCostDTO.setDealCost(resultSet.getFloat("DEAL_COST"));
	        	retailCostDTO.setDealStartDate(resultSet.getString("DEAL_START_DATE"));
	        	retailCostDTO.setDealEndDate(resultSet.getString("DEAL_END_DATE"));
	        	retailCostDTO.setPromotionFlag(resultSet.getString("PROMOTION_FLG"));
	        	retailCostDTO.setLevel2Cost(resultSet.getFloat("LEVEL2COST"));
	        	retailCostDTOList.add(retailCostDTO);
	        }
	        long endTime = System.currentTimeMillis();
	        logger.debug("Time taken to execute and store cost info in list- " + (endTime - startTime));
		}
		catch (SQLException e)
		{
			logger.error("Error while executing GET_RETAIL_COST_INFO");
			throw new GeneralException("Error while executing GET_RETAIL_COST_INFO", e);
		}finally{
			PristineDBUtil.close(resultSet);
			PristineDBUtil.close(statement);
		}
		return retailCostDTOList;
	}

	/**
	 * This method inserts retail cost data in RETAIL_COST_INFO table
	 * @param conn					Connection
	 * @param toBeInsertedList		List of records that needs to be inserted in retail_cost_info
	 * @throws GeneralException
	 */
	public void saveRetailCostData(Connection conn, List<RetailCostDTO> toBeInsertedList) throws GeneralException{
		logger.debug("Inside saveRetailCostData() of RetailCostDAO");
		logger.debug("LOAD_ALLOWANCE_AMT_AND_LONG_TRM_FLG is values :"+PropertyManager.
					getProperty("LOAD_ALLOWANCE_AMT_AND_LONG_TRM_FLG", "FALSE"));
		PreparedStatement statement = null;
		StringBuilder sb = new StringBuilder();
	    try{
	    	boolean loadLongTermFlgAndAllowance = Boolean.parseBoolean(PropertyManager.
					getProperty("LOAD_ALLOWANCE_AMT_AND_LONG_TRM_FLG", "FALSE"));
			if(loadLongTermFlgAndAllowance){
				logger.debug("Processing long term and Allowance amount details query");
				statement = conn.prepareStatement(SAVE_RETAIL_COST_INFO_WITH_LONG_TERM_AND_ALLOWANCE);
			}else{
				statement = conn.prepareStatement(SAVE_RETAIL_COST_INFO);	
			}
			
			int itemNoInBatch = 0;
			int recordCnt = 0;
	        for(RetailCostDTO retailCostDTO:toBeInsertedList){
	        	int counter = 0;
	        	statement.setInt(++counter, retailCostDTO.getCalendarId());
	        	statement.setString(++counter, retailCostDTO.getItemcode());
	        	statement.setInt(++counter, retailCostDTO.getLevelTypeId());
	        	statement.setString(++counter, retailCostDTO.getLevelId());
	        	statement.setFloat(++counter, retailCostDTO.getListCost());
	        	statement.setFloat(++counter, retailCostDTO.getDealCost());
	        	statement.setString(++counter,retailCostDTO.getPromotionFlag());
	        	statement.setString(++counter, retailCostDTO.getEffListCostDate());
	        	statement.setString(++counter, retailCostDTO.getDealStartDate());
	        	statement.setString(++counter, retailCostDTO.getDealEndDate());
	        	statement.setFloat(++counter, retailCostDTO.getLevel2Cost()); // Changes to include LEVEL2COST for TOPS in RETAIL_COST_INFO
	        	statement.setFloat(++counter, retailCostDTO.getVipCost()); // Changes to include VIP_COST for Ahold
	        	statement.setString(++counter, retailCostDTO.getLevel2StartDate());
	        	statement.setString(++counter, retailCostDTO.getLevel2EndDate());
	        	if(retailCostDTO.getLocationId() == null)
	        		statement.setNull(++counter, Types.NULL);
	        	else
	        		statement.setInt(++counter, retailCostDTO.getLocationId());
	        	//Adding allowance amount, ScanBackAmount(1,2,3) and Their start and End date(1,2,3)..By Dinesh(10/05/16)
	        	if(retailCostDTO.getAllowanceAmount() == 0){
	        		statement.setNull(++counter, Types.NULL);
	        	}else{
	        		statement.setFloat(++counter, retailCostDTO.getAllowanceAmount());
	        	}
	        	if(retailCostDTO.getScanBackAmt1() == 0){
	        		statement.setNull(++counter, Types.NULL);
	        	}
	        	else{
	        		statement.setFloat(++counter, retailCostDTO.getScanBackAmt1());
	        	}
	        	if(retailCostDTO.getScanBackAmt2() == 0){
	        		statement.setNull(++counter, Types.NULL);
	        	}
	        	else{
	        		statement.setFloat(++counter, retailCostDTO.getScanBackAmt2());
	        	}
	        	if(retailCostDTO.getScanBackAmt3() == 0){
	        		statement.setNull(++counter, Types.NULL);
	        	}
	        	else{
	        		statement.setFloat(++counter, retailCostDTO.getScanBackAmt3());
	        	}
	        	if(retailCostDTO.getScanBackStartDate1() == null && retailCostDTO.getScanBackStartDate1() ==""){
	        		statement.setNull(++counter, Types.NULL);
	        	}
	        	else{
	        		statement.setString(++counter, retailCostDTO.getScanBackStartDate1());
	        	}
	        	if(retailCostDTO.getScanBackStartDate2() == null && retailCostDTO.getScanBackStartDate2() ==""){
	        		statement.setNull(++counter, Types.NULL);
	        	}
	        	else{
	        		statement.setString(++counter, retailCostDTO.getScanBackStartDate2());
	        	}
	        	if(retailCostDTO.getScanBackStartDate3() == null && retailCostDTO.getScanBackStartDate3() ==""){
	        		statement.setNull(++counter, Types.NULL);
	        	}
	        	else{
	        		statement.setString(++counter, retailCostDTO.getScanBackStartDate3());
	        	}
	        	if(retailCostDTO.getScanBackEndDate1() == null && retailCostDTO.getScanBackEndDate1() ==""){
	        		statement.setNull(++counter, Types.NULL);
	        	}
	        	else{
	        		statement.setString(++counter, retailCostDTO.getScanBackEndDate1());
	        	}
	        	if(retailCostDTO.getScanBackEndDate2() == null && retailCostDTO.getScanBackEndDate2() ==""){
	        		statement.setNull(++counter, Types.NULL);
	        	}
	        	else{
	        		statement.setString(++counter, retailCostDTO.getScanBackEndDate2());
	        	}
	        	if(retailCostDTO.getScanBackEndDate3() == null && retailCostDTO.getScanBackEndDate3() ==""){
	        		statement.setNull(++counter, Types.NULL);
	        	}
	        	else{
	        		statement.setString(++counter, retailCostDTO.getScanBackEndDate3());
	        	}
	        	if(loadLongTermFlgAndAllowance){
		        	statement.setString(++counter, retailCostDTO.getAllowStartDate());
		        	statement.setString(++counter, retailCostDTO.getAllowEndDate());
		        	statement.setString(++counter, retailCostDTO.getLongTermFlag());
		        	statement.setFloat(++counter, retailCostDTO.getFinalListCost());
		        	statement.setString(++counter, retailCostDTO.getLongTermScan1());
		        	statement.setString(++counter, retailCostDTO.getLongTermScan2());
		        	statement.setString(++counter, retailCostDTO.getLongTermScan3());
	        	}
	        	
	        	if(retailCostDTO.isWhseZoneRolledUpRecord()){
	        		statement.setString(++counter, String.valueOf(Constants.YES));
	        	}else{
	        		statement.setString(++counter, String.valueOf(Constants.NO));
	        	}
	        	
	        	statement.addBatch();
	        	itemNoInBatch++;
	        	recordCnt++;
	        	sb.append(retailCostDTO.getCalendarId() + ", " +retailCostDTO.getItemcode()
	        			+ ", " +retailCostDTO.getLevelTypeId()
	        			+ ", " +retailCostDTO.getLevelId()
	        			+ ", " +retailCostDTO.getListCost()
	        			+ ", " +retailCostDTO.getDealCost()
	        			+ ", " +retailCostDTO.getPromotionFlag()
	        			+ ", " +retailCostDTO.getEffListCostDate()
	        			+ ", " +retailCostDTO.getDealStartDate()
	        			+ ", " +retailCostDTO.getDealEndDate()
	        			+ ", " +retailCostDTO.getLevel2Cost()
	        			+ ", " +retailCostDTO.getVipCost()
	        			+ ", " +retailCostDTO.getLevel2StartDate()
	        			+ ", " +retailCostDTO.getLevel2EndDate()
	        			+ ", " +retailCostDTO.getLocationId()
	        			+ ", " +retailCostDTO.getAllowanceAmount()
	        			+", " +retailCostDTO.getLongTermFlag()+"\n");
	        	if(itemNoInBatch % Constants.BATCH_UPDATE_COUNT == 0){
	        		int[] count = statement.executeBatch();
	        		logger.debug("batch executed");
	        		statement.clearBatch();
	        		sb = new StringBuilder();
	        		itemNoInBatch = 0;
	        	}
	        	if(recordCnt % 10000 == 0){
	        		conn.commit();
	        	}
	        	
	        }
	        if(itemNoInBatch > 0){
	        	int[] count = statement.executeBatch();
	        	sb = new StringBuilder();
        		statement.clearBatch();
	        }
		}
		catch (SQLException e)
		{
			logger.error("Error batch values: "+sb);
			logger.error("Error while executing SAVE_RETAIL_COST_INFO " + e);
			throw new GeneralException("Error while executing SAVE_RETAIL_COST_INFO "+ e);
		}finally{
			PristineDBUtil.close(statement);
		}
	}
	
	/**
	 * Delete future cost data.
	 * @param conn
	 * @param recordsToBeDeleted
	 * @throws GeneralException 
	 */
	public void deleteFutureCostData(Connection conn, List<RetailCostDTO> recordsToBeDeleted, String weekStartDate) throws GeneralException{
		/*private static final String DELETE_FUTURE_COST = "DELETE FROM RETAIL_COST_INFO WHERE CALENDAR_ID IN (SELECE CALENDAR_ID FROM RETAIL_CALENDAR "
				+ " WHERE START_DATE > TO_DATE(?, '" + Constants.DB_DATE_FORMAT + "' AND ROW_TYPE = 'W') AND "
				+ " ITEM_CODE = ? AND LEVEL_ID = ? AND LEVEL_TYPE_ID = ?";*/
		PreparedStatement statement = null;
		try{
			statement = conn.prepareStatement(DELETE_FUTURE_COST);
			int itemsInBatch = 0;
			for(RetailCostDTO retailCostDTO: recordsToBeDeleted){
				int colCount = 0;
				statement.setString(++colCount, weekStartDate);
				statement.setString(++colCount, retailCostDTO.getItemcode());
				statement.setString(++colCount, retailCostDTO.getLevelId());
				statement.setInt(++colCount, retailCostDTO.getLevelTypeId());
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
	 * Insert/Update store/zone mapping with items
	 * @param values			Contains the input with which mapping needs to be created		
	 * @param itemCodeMap		Contains mapping between upc and item code
	 * @throws GeneralException 
	 * @throws NumberFormatException 
	 */
	public void mapItemsWithStore(Connection conn, Collection<List<RetailCostDTO>> retailCostColln,
			HashMap<ItemDetailKey, String> itemCodeMap, HashMap<String, Integer> storeIdMap,
			HashMap<String, Integer> priceZoneIdMap, Set<String> noItemCodeSet,
			HashMap<String, List<Integer>> deptZoneMap, HashMap<String, Long> vendorIdMap,
			boolean ignoreUpdatingDistFlagFromRetItemCode, boolean usePriceZoneMap,
			HashMap<String, List<Integer>> storeZoneMap)
					throws NumberFormatException, GeneralException {
		PreparedStatement statement = null;
		try{
			if(usePriceZoneMap)
				statement = conn.prepareStatement(MERGE_INTO_STORE_ITEM_MAP_WITH_PRC_ZONE);
			else
				statement = conn.prepareStatement(MERGE_INTO_STORE_ITEM_MAP);
			
			int itemNoInBatch = 0;
			int cnt = 0;
			for(List<RetailCostDTO> retailCostDTOList : retailCostColln){
		        for(RetailCostDTO retailCostDTO:retailCostDTOList){
		        	String upc = PrestoUtil.castUPC(retailCostDTO.getUpc(),false);
		        	String retailerItemCode = retailCostDTO.getRetailerItemCode();
		        	ItemDetailKey itemDetailKey = new ItemDetailKey(upc, retailerItemCode);
		        	retailCostDTO.setItemcode(itemCodeMap.get(itemDetailKey));
		        	if(retailCostDTO.getItemcode() != null)
		        	{
		        		int counter = 0;
		        		long vendorId = 0;
		        		cnt++;
		        		Integer levelId = null;
		        		if(retailCostDTO.getLevelTypeId() == 1)
		        			levelId = priceZoneIdMap.get(retailCostDTO.getLevelId());
		        		else if(retailCostDTO.getLevelTypeId() == 2)
		        			levelId = storeIdMap.get(retailCostDTO.getLevelId());
		        		
		        		if(levelId != null){
		        			if(retailCostDTO.getLevelTypeId() == Constants.ZONE_LEVEL_TYPE_ID){
		        				addBatchForStoreOrZone(statement, retailCostDTO, ignoreUpdatingDistFlagFromRetItemCode,
										vendorIdMap, levelId, usePriceZoneMap, priceZoneIdMap, Constants.ZONE_LEVEL_TYPE_ID);
		        				
		        				if(storeZoneMap != null){
		        					if(storeZoneMap.containsKey(retailCostDTO.getLevelId())){
		        						for(int storeId: storeZoneMap.get(retailCostDTO.getLevelId())){
		        							//retailCostDTO.setlevel
		        							levelId = storeId;
		        							addBatchForStoreOrZone(statement, retailCostDTO, ignoreUpdatingDistFlagFromRetItemCode,
		    										vendorIdMap, levelId, usePriceZoneMap, priceZoneIdMap, Constants.STORE_LEVEL_TYPE_ID);
		        							itemNoInBatch++;
		        						}
		        					}
		        				}
		        			}else if(retailCostDTO.getLevelTypeId() == Constants.STORE_LEVEL_TYPE_ID){
		        				addBatchForStoreOrZone(statement, retailCostDTO, ignoreUpdatingDistFlagFromRetItemCode,
										vendorIdMap, levelId, usePriceZoneMap, priceZoneIdMap, Constants.STORE_LEVEL_TYPE_ID);
		        				itemNoInBatch++;
		        			}
		        			
		        			
				        	
		        		}else{
		        			if(retailCostDTO.getLevelTypeId() == 1){
		        				// This could be a dept zone level record
		        				// Processing starts for dept zone level record
		        				List<Integer> levelIdList = deptZoneMap.get(retailCostDTO.getLevelId());
		        				if(levelIdList != null && levelIdList.size() > 0){
		        					for(int levelIdTemp : levelIdList){
		        						counter = 0;
		        						statement.setInt(++counter, retailCostDTO.getLevelTypeId());
		    				        	statement.setInt(++counter, levelIdTemp);
		    				        	statement.setString(++counter, retailCostDTO.getItemcode());
		    				        	statement.setString(++counter, Constants.COST_INDICATOR);
		    				        	if(!ignoreUpdatingDistFlagFromRetItemCode){
											if (retailCostDTO.getRetailerItemCode() != null
													&& !retailCostDTO.getRetailerItemCode().isEmpty()
													&& Integer.parseInt(retailCostDTO
															.getRetailerItemCode()) > Constants.MAX_RET_ITEM_CODE) {
												statement.setString(++counter, Constants.EMPTY + Constants.DSD);
											} else {
												statement.setString(++counter, Constants.EMPTY + Constants.WAREHOUSE);
											}
		    				        	}else{
		    				        		if(retailCostDTO.getVendorNumber() == null 
		    				        				|| Constants.EMPTY.equals(retailCostDTO.getVendorNumber())){
		    				        			statement.setString(++counter, Constants.EMPTY + Constants.WAREHOUSE);
		    				        		}else{
		    				        			statement.setString(++counter, Constants.EMPTY + Constants.DSD);
		    				        		}
		    				        	}
		    				        	if(vendorIdMap.containsKey(retailCostDTO.getVendorNumber())){
		    					        	vendorId = vendorIdMap.get(retailCostDTO.getVendorNumber());
		    					        	}
		    				        	if(vendorId != 0){
		    				        		statement.setLong(++counter, vendorId);
		    				        	}
		    				        	else{
		    				        		statement.setLong(++counter, 1);
		    				        	}
		    				        	if(usePriceZoneMap){
		    					        	if(priceZoneIdMap.get(retailCostDTO.getZoneNbr()) == null ){
		    					        		statement.setNull(++counter, Types.NULL);
		    					        	}else{
		    					        		statement.setInt(++counter, priceZoneIdMap.get(retailCostDTO.getZoneNbr()));
		    					        	}
		    				        	}
		    				        	statement.setString(++counter, Constants.AUTHORIZED_ITEM);
		    				        	statement.addBatch();
		    				        	itemNoInBatch++;
		    				        	
		    				        	if(itemNoInBatch % Constants.BATCH_UPDATE_COUNT == 0){
		    				        		statement.executeBatch();
		    				        		statement.clearBatch();
		    				        		itemNoInBatch = 0;
		    				        	}
		        					}
		        				}
		        				// Processing for dept zone level record ends
		        			}
		        		}
			        	
		        		if(cnt % 100000 == 0){
			        		logger.info("Number of records processed  - " + cnt);
			        	}
		        		
		        		try{
				        	if(itemNoInBatch % Constants.BATCH_UPDATE_COUNT == 0){
				        		statement.executeBatch();
				        		statement.clearBatch();
				        		itemNoInBatch = 0;
				        	}
				        	if(cnt % 25000 == 0){
				        		conn.commit();
				        	}
		        		}
		        		catch(SQLException exception){
		        			logger.error("Error while executing MERGE_STORE_ITEM_MAP" + exception);
		        		}
		        		
		        	}else{
		        		noItemCodeSet.add(retailCostDTO.getUpc());
		        	}
		        }
			}
			if(itemNoInBatch > 0){
				statement.executeBatch();
        		statement.clearBatch();
	        }
	    }
		catch (SQLException e)
		{
			logger.error("Error while executing MERGE_STORE_ITEM_MAP" + e);
		}finally{
			PristineDBUtil.close(statement);
		}
		
	}
	
	
	
	private void addBatchForStoreOrZone(PreparedStatement statement, 
			RetailCostDTO retailCostDTO, 
			boolean ignoreUpdatingDistFlagFromRetItemCode, 
			HashMap<String, Long> vendorIdMap, 
			int levelId, 
			boolean usePriceZoneMap, 
			HashMap<String, Integer> priceZoneIdMap, int levelTypeId) throws SQLException{
		int counter = 0;
		long vendorId = 0;
		statement.setInt(++counter, levelTypeId);
    	statement.setInt(++counter, levelId);
    	statement.setString(++counter, retailCostDTO.getItemcode());
    	statement.setString(++counter, Constants.COST_INDICATOR);
    	if(!ignoreUpdatingDistFlagFromRetItemCode){
			if (retailCostDTO.getRetailerItemCode() != null
					&& !retailCostDTO.getRetailerItemCode().isEmpty()
					&& Integer.parseInt(retailCostDTO
							.getRetailerItemCode()) > Constants.MAX_RET_ITEM_CODE) {
				statement.setString(++counter, Constants.EMPTY + Constants.DSD);
			} else {
				statement.setString(++counter, Constants.EMPTY + Constants.WAREHOUSE);
			}
    	}else{
    		if(retailCostDTO.getVendorNumber() == null 
    				|| Constants.EMPTY.equals(retailCostDTO.getVendorNumber())){
    			statement.setString(++counter, Constants.EMPTY + Constants.WAREHOUSE);
    		}else{
    			statement.setString(++counter, Constants.EMPTY + Constants.DSD);
    		}
    	}
    	if(vendorIdMap.containsKey(retailCostDTO.getVendorNumber())){
    		vendorId = vendorIdMap.get(retailCostDTO.getVendorNumber());
    	}
    	if(vendorId != 0){
    		statement.setLong(++counter, vendorId);
    	}
    	else{
    		statement.setLong(++counter, 1);
    	}
    	if(usePriceZoneMap){
        	if(priceZoneIdMap.get(retailCostDTO.getZoneNbr()) == null ){
        		statement.setNull(++counter, Types.NULL);
        	}else{
        		statement.setInt(++counter, priceZoneIdMap.get(retailCostDTO.getZoneNbr()));
        	}
    	}
    	statement.setString(++counter, Constants.AUTHORIZED_ITEM);
    	statement.addBatch();
	}
	
	/**
	 * This method updates retail cost data in RETAIL_COST_INFO table
	 * @param conn					Connection
	 * @param toBeUpdatedList		List of records that needs to be updated in retail_cost_info
	 * @throws GeneralException
	 */
	public void updateRetailCostData(Connection conn, List<RetailCostDTO> toBeUpdatedList) throws GeneralException{
		logger.debug("Inside updateRetailCostData() of RetailCostDAO");
		PreparedStatement statement = null;
	    try{
			statement = conn.prepareStatement(UPDATE_RETAIL_COST_INFO);
	        
			int itemNoInBatch = 0;
	        for(RetailCostDTO retailCostDTO:toBeUpdatedList){
	        	int counter = 0;
	        	statement.setFloat(++counter, retailCostDTO.getListCost());
	        	statement.setFloat(++counter, retailCostDTO.getDealCost());
	        	statement.setString(++counter,retailCostDTO.getPromotionFlag());
	        	statement.setString(++counter, retailCostDTO.getEffListCostDate());
	        	statement.setString(++counter, retailCostDTO.getDealStartDate());
	        	statement.setString(++counter, retailCostDTO.getDealEndDate());
	        	statement.setFloat(++counter, retailCostDTO.getVipCost());
	        	if(retailCostDTO.getLocationId() == null)
	        		statement.setNull(++counter, Types.NULL);
	        	else
	        		statement.setInt(++counter, retailCostDTO.getLocationId());
	        	if(retailCostDTO.isWhseZoneRolledUpRecord()){
	        		statement.setString(++counter, String.valueOf(Constants.YES));
	        	}else{
	        		statement.setString(++counter, String.valueOf(Constants.NO));
	        	}
	        	statement.setInt(++counter, retailCostDTO.getCalendarId());
	        	statement.setString(++counter, retailCostDTO.getItemcode());
	        	statement.setInt(++counter, retailCostDTO.getLevelTypeId());
	        	statement.setString(++counter, retailCostDTO.getLevelId());
	        	statement.addBatch();
	        	itemNoInBatch++;
	        	
	        	if(itemNoInBatch % Constants.BATCH_UPDATE_COUNT == 0){
	        		int[] count = statement.executeBatch();
	        		statement.clearBatch();
	        		itemNoInBatch = 0;
	        	}
	        }
	        if(itemNoInBatch > 0){
	        	int[] count = statement.executeBatch();
	        	statement.clearBatch();
	        }
	    }
		catch (SQLException e)
		{
			logger.error("Error while executing UPDATE_RETAIL_COST_INFO " + e);
			throw new GeneralException("Error while executing UPDATE_RETAIL_COST_INFO" + e);
		}finally{
			PristineDBUtil.close(statement);
			
		}
	}
	
	/**
	 * This method deletes retail cost data into RETAIL_COST_INFO table
	 * @param conn				Connection
	 * @param toBeDeletedList	List of records that needs to be deleted from retail_cost_info
	 * @throws GeneralException
	 */
	public void deleteRetailCostData(Connection conn, List<RetailCostDTO> toBeDeletedList) throws GeneralException{
		logger.debug("Inside deleteRetailCostData() of RetailCostDAO");
		PreparedStatement statement = null;
		try{
			statement = conn.prepareStatement(DELETE_RETAIL_COST_INFO);
	        
			int itemNoInBatch = 0;
	        for(RetailCostDTO retailCostDTO:toBeDeletedList){
	        	int counter = 0;
	        	statement.setInt(++counter, retailCostDTO.getCalendarId());
	        	statement.setString(++counter, retailCostDTO.getItemcode());
	        	statement.setInt(++counter, retailCostDTO.getLevelTypeId());
	        	statement.setString(++counter, retailCostDTO.getLevelId());
	        		        	
	        	statement.addBatch();
	        	itemNoInBatch++;
	        	
	        	if(itemNoInBatch % Constants.BATCH_UPDATE_COUNT == 0){
	        		int[] count = statement.executeBatch();
	        		logger.debug("Batch executed");
	        		statement.clearBatch();
	        		itemNoInBatch = 0;
	        	}
	        }
	        if(itemNoInBatch > 0){
	        	int[] count = statement.executeBatch();
        		statement.clearBatch();
	        }
		}
		catch (SQLException e)
		{
			logger.error("Error while executing DELETE_RETAIL_COST_INFO");
			throw new GeneralException("Error while executing DELETE_RETAIL_COST_INFO", e);
		}
		finally{
			PristineDBUtil.close(statement);
		}
	}
	
	/**
	 * This method deletes retail cost data into RETAIL_COST_INFO table
	 * @param conn				Connection
	 * @param toBeDeletedList	List of records that needs to be deleted from retail_cost_info
	 * @throws GeneralException
	 */
	public void deleteRetailCostData(Connection conn, int calendarId, List<String> itemCodeList) throws GeneralException{
		logger.debug("Inside deleteRetailCostData() of RetailCostDAO");
		PreparedStatement statement = null;
		try{
			statement = conn.prepareStatement(DELETE_RETAIL_COST_INFO_WEEKLY_ITEMS);
	        
			int itemNoInBatch = 0;
	        for(String itemCode : itemCodeList){
	        	int counter = 0;
	        	statement.setInt(++counter, calendarId);
	        	statement.setString(++counter, itemCode);
	        		        	
	        	statement.addBatch();
	        	itemNoInBatch++;
	        	
	        	if(itemNoInBatch % Constants.BATCH_UPDATE_COUNT == 0){
	        		int[] count = statement.executeBatch();
	        		logger.debug("Batch executed");
	        		statement.clearBatch();
	        		itemNoInBatch = 0;
	        	}
	        }
	        if(itemNoInBatch > 0){
	        	int[] count = statement.executeBatch();
        		statement.clearBatch();
	        }
		}
		catch (SQLException e)
		{
			logger.error("Error while executing DELETE_RETAIL_COST_INFO_WEEKLY_ITEMS");
			throw new GeneralException("Error while executing DELETE_RETAIL_COST_INFO_WEEKLY_ITEMS", e);
		}
		finally{
			PristineDBUtil.close(statement);
		}
	}
	
	/**
	 * Deletes data from retail_cost_info table for given calendar id
	 * @param conn			Database connection
	 * @param calendarId	Calendar Id
	 * @throws GeneralException
	 */
	public void deleteRetailCostData(Connection conn, int calendarId) throws GeneralException{
		logger.debug("Inside deleteRetailCostData() of RetailCostDAO");
		PreparedStatement statement = null;
		try{
			statement = conn.prepareStatement(DELETE_RETAIL_COST_INFO_WEEKLY);
	        
	        statement.setInt(1, calendarId);
	        int count = statement.executeUpdate();
		}
		catch (SQLException e)
		{
			logger.error("Error while executing DELETE_RETAIL_COST_INFO_WEEKLY_ITEMS");
			throw new GeneralException("Error while executing DELETE_RETAIL_COST_INFO_WEEKLY_ITEMS", e);
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
	public boolean setupRetailCostData(Connection conn, int calendarId, int prevWkCalendarId) throws GeneralException{
		logger.debug("Inside setupRetailCostData() of RetailCostDAO");
		PreparedStatement statement = null;
		int count = 0;
		try{
			statement = conn.prepareStatement(SETUP_RETAIL_COST_DATA);
	        statement.setInt(1, calendarId);
	        statement.setInt(2, prevWkCalendarId);
	        statement.setInt(3, calendarId);
	        int updCnt = statement.executeUpdate();
	        logger.info("No of rows copied - " + updCnt);
		}
		catch (SQLException e)
		{
			logger.error("Error while executing SETUP_RETAIL_COST_DATA");
			throw new GeneralException("Error while executing SETUP_RETAIL_COST_DATA", e);
		}
		finally{
			PristineDBUtil.close(statement);
		}
		return count > 0;
	}

	
	
	/**
	 * Setup retail cost data for given calendar id from previous week's calendar id
	 * @param conn			Database connection
	 * @param calendarId	Calendar Id
	 * @throws GeneralException
	 */
	public boolean setupRetailCostDataV2(Connection conn, int calendarId, int prevWkCalendarId) throws GeneralException{
		logger.debug("Inside setupRetailCostData() of RetailCostDAO");
		PreparedStatement statement = null;
		int count = 0;
		try{
			statement = conn.prepareStatement(SETUP_RETAIL_COST_DATAV2);
	        statement.setInt(1, calendarId);
	        statement.setInt(2, prevWkCalendarId);
	        count = statement.executeUpdate();
		}
		catch (SQLException e)
		{
			logger.error("Error while executing DELETE_RETAIL_COST_INFO_WEEKLY_ITEMS");
			throw new GeneralException("Error while executing DELETE_RETAIL_COST_INFO_WEEKLY_ITEMS", e);
		}
		finally{
			PristineDBUtil.close(statement);
		}
		return count > 0;
	}
	
	
	
	
	/**
	 * copies current week data into TEMP table.
	 * @param conn			Database connection
	 * @param calendarId	Calendar Id
	 * @throws GeneralException
	 */
	public boolean moveCurrentDataToTemp(Connection conn, int calendarId) throws GeneralException{
		logger.debug("Inside setupRetailCostData() of RetailCostDAO");
		PreparedStatement statement = null;
		int count = 0;
		try{
			statement = conn.prepareStatement(SETUP_TEMP_RETAIL_COST_DATA);
	        statement.setInt(1, calendarId);
	        count = statement.executeUpdate();
		}
		catch (SQLException e)
		{
			logger.error("Error while moving data to TEMP table");
			throw new GeneralException("Error while moving data to TEMP table", e);
		}
		finally{
			PristineDBUtil.close(statement);
		}
		return count > 0;
	}
	
	/**
	 * restore current week data from TEMP table to main table.
	 * @param conn			Database connection
	 * @param calendarId	Calendar Id
	 * @throws GeneralException
	 */
	public boolean restoreDataFromTemp(Connection conn, int calendarId) throws GeneralException{
		logger.debug("Inside setupRetailCostData() of RetailCostDAO");
		PreparedStatement statement = null;
		int count = 0;
		try{
			statement = conn.prepareStatement(RESTORE_RETAIL_COST_DATA_FROM_TEMP);
	        statement.setInt(1, calendarId);
	        count = statement.executeUpdate();
		}
		catch (SQLException e)
		{
			logger.error("Error while restoring data from TEMP table");
			throw new GeneralException("Error while restoring data from TEMP table", e);
		}
		finally{
			PristineDBUtil.close(statement);
		}
		return count > 0;
	}
	
	public RetailCostDTO getChainLevelCost(Connection conn, int calendarId, int itemCode){
		PreparedStatement statement = null;
	    ResultSet resultSet = null;
	    RetailCostDTO rcDTO = null;
		try{
			statement = conn.prepareStatement(GET_CHAIN_LEVEL_COST);
	        statement.setInt(1, calendarId);
	        statement.setInt(2, itemCode);
	        resultSet = statement.executeQuery();
	        if(resultSet.next()){
	        	rcDTO = new RetailCostDTO();
	        	rcDTO.setListCost(resultSet.getFloat("LIST_COST"));
	        	rcDTO.setDealCost(resultSet.getFloat("DEAL_COST"));
	        }
		}
		catch (SQLException e)
		{
			logger.error("Error while executing GET_CHAIN_LEVEL_COST " + e);
		}
		finally{
			PristineDBUtil.close(resultSet);
			PristineDBUtil.close(statement);
		}
		return rcDTO;
	}
	/**
	 * This method checks STORE_ITEM_MAP table to track the item for given store
	 * @param conn
	 * @param chainId
	 * @param strNo
	 * @param upc
	 * @return
	 * @throws GeneralException
	 */
	public boolean checkItemWithStores(Connection conn, String chainId, String strNo, String upc) throws GeneralException{
		PreparedStatement statement = null;
		ResultSet resultSet = null;
		boolean isItemAvailable = false;
	    try{
	    	int counter = 0;
			statement = conn.prepareStatement(CHECK_UPC_FOR_WITH_STORE);
			statement.setString(++counter, chainId);
			statement.setString(++counter, upc);
			statement.setString(++counter, strNo);
			resultSet = statement.executeQuery();
			while(resultSet.next()){
				isItemAvailable = true;
			}
	    }
	    catch (SQLException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
	    finally{
			PristineDBUtil.close(resultSet);
			PristineDBUtil.close(statement);
		}
	    return isItemAvailable;
	}
	
	public void saveSkippedRecords(Connection conn, List<RetailCostDTO> skippedList, String fileName, String effectiveDate){
		PreparedStatement statement = null;
		int recordCount = 0;
		try{
		statement = conn.prepareStatement(INSERT_UNPROCESSED_RECORDS);
		for(RetailCostDTO retailCostDTO: skippedList){
			int counter = 0;
			statement.setString(++counter, fileName);
			statement.setString(++counter, effectiveDate);
			statement.setString(++counter, retailCostDTO.getRetailerItemCode());
			statement.setString(++counter, retailCostDTO.getLevelId());
			statement.setString(++counter, retailCostDTO.getEffListCostDate());
			statement.setFloat(++counter, retailCostDTO.getVipCost());
			statement.setFloat(++counter, retailCostDTO.getAvgCost());
			statement.setString(++counter, retailCostDTO.getVendorNumber());
			statement.setFloat(++counter, retailCostDTO.getListCost());
			statement.setString(++counter, retailCostDTO.getVendorName());
			statement.addBatch();
			recordCount++;
			if(recordCount % Constants.LIMIT_COUNT == 0){
				statement.executeBatch();
				statement.clearBatch();
				recordCount = 0;
			}
		}
		if(recordCount > 0){
			statement.executeBatch();
			statement.clearBatch();
		}
		}
		catch (SQLException e) {
			logger.error("Error inserting unprocessed cost records - "  + e);
		}
	}
	
	public HashMap<StoreItemKey, List<String>> getStoreItemMap(Connection conn, String inputDate, Set<String> itemCodeSet) throws GeneralException{
		logger.debug("Inside getStoreItemMap() of CostDAO");
		int limitcount=0;
		List<String> itemCodeList = new ArrayList<String>();
		
		HashMap<StoreItemKey, List<String>> storeItemMap = new HashMap<StoreItemKey, List<String>>();
		for(String itemCode:itemCodeSet){
			itemCodeList.add(itemCode);
			limitcount++;
			if( limitcount > 0 && (limitcount%this.commitCount == 0)){
				Object[] values = itemCodeList.toArray();
				retrieveItemStoreMapping(conn, inputDate, storeItemMap, values);
				itemCodeList.clear();
            }
			if(limitcount > 0 && (limitcount%Constants.RECS_PROCESSED == 0)){
				logger.info("Fetched store item map for " + limitcount + " items");
			}
		}
		if(itemCodeList.size() > 0){
			Object[] values = itemCodeList.toArray();
			retrieveItemStoreMapping(conn, inputDate, storeItemMap, values);
			itemCodeList.clear();
		}
		
		
		return storeItemMap;
	}
	
	public void retrieveItemStoreMapping(Connection conn, String inputDate, HashMap<StoreItemKey, List<String>> storeItemMap, Object[] values) throws GeneralException{
		logger.debug("Inside retrieveItemStoreMapping() of CostDAO");
		List<String> itemList = new ArrayList<String>();
		PreparedStatement statement = null;
	    ResultSet resultSet = null;
	    Date inputDt = DateUtil.toSQLDate(inputDate);
	    
		try{
			statement = conn.prepareStatement(String.format(RETRIEVE_ITEM_STORE_MAPPING, PristineDBUtil.preparePlaceHolders(values.length)));
			PristineDBUtil.setValues(statement, values);
			resultSet = statement.executeQuery();
	        int levelTypeId = -1;
	        String levelId = null;
	        String itemCode = null;
	        StoreItemKey key = null;
	        boolean itemToBeAdded = false;
	        while(resultSet.next()){
	        	itemToBeAdded = true;
	        	if(itemToBeAdded){
		        	levelTypeId = resultSet.getInt("LEVEL_TYPE_ID");
		        	levelId = resultSet.getString("LEVEL_ID");
		        	itemCode = resultSet.getString("ITEM_CODE");
		        	key = new StoreItemKey(levelTypeId, levelId);
		        	if(storeItemMap.get(key) != null){
		        		itemList = storeItemMap.get(key);
		        		itemList.add(itemCode);
		        	}else{
		        		itemList = new ArrayList<String>();
		        		itemList.add(itemCode);
		        	}
		        	storeItemMap.put(key, itemList);
	        	}
	        }
		}
		catch (SQLException e)
		{
			logger.error("Error while executing RETRIEVE_ITEM_STORE_MAPPING - " + e);
			throw new GeneralException("Error while executing RETRIEVE_ITEM_STORE_MAPPING", e);
		}finally{
			PristineDBUtil.close(resultSet);
			PristineDBUtil.close(statement);
		}
	}
	
	
	/**
	 * 
	 * @param conn
	 * @param itemCodeSet
	 * @return item store mapping
	 * @throws GeneralException
	 */
	public HashMap<String, HashMap<String, List<String>>> getStoreItemMapAtZonelevel(Connection conn, 
			Set<String> itemCodeSet, String compStrNo) throws GeneralException{
		logger.debug("Inside getStoreItemMap() of CostDAO");
		int limitcount=0;
		List<String> itemCodeList = new ArrayList<String>();
		
		HashMap<String, HashMap<String, List<String>>> storeItemMap = 
				new HashMap<String, HashMap<String, List<String>>>();
		for(String itemCode:itemCodeSet){
			itemCodeList.add(itemCode);
			limitcount++;
			if( limitcount > 0 && (limitcount%this.commitCount == 0)){
				Object[] values = itemCodeList.toArray();
				retrieveItemAtZonelevel(conn, storeItemMap, values, compStrNo);
				itemCodeList.clear();
            }
			if(limitcount > 0 && (limitcount%Constants.RECS_PROCESSED == 0)){
				logger.info("Fetched store item map for " + limitcount + " items");
			}
		}
		if(itemCodeList.size() > 0){
			Object[] values = itemCodeList.toArray();
			retrieveItemAtZonelevel(conn, storeItemMap, values, compStrNo);
			itemCodeList.clear();
		}
		
		
		return storeItemMap;
	}
	
	public void retrieveItemAtZonelevel(Connection conn, 
			HashMap<String, HashMap<String, List<String>>> storeItemMap, 
			Object[] values, String compStrNo) throws GeneralException{
		logger.debug("Inside retrieveItemStoreMapping() of CostDAO");
		PreparedStatement statement = null;
	    ResultSet resultSet = null;
		try{
			StringBuilder sb = new StringBuilder(RETRIEVE_ITEM_STORE_MAPPING_WITH_ZONE);
			
			if(compStrNo != null){
				sb.append(" AND COMP_STR_NO = '" + compStrNo + "'");
			}
			statement = conn.prepareStatement(String.format(sb.toString(), PristineDBUtil.preparePlaceHolders(values.length)));
			PristineDBUtil.setValues(statement, values);
			if(compStrNo == null)
				statement.setFetchSize(300000);
			resultSet = statement.executeQuery();
	        String levelId = null;
	        String itemCode = null;
	        //StoreItemKey key = null;
	        String zoneKey = null;
	        boolean itemToBeAdded = false;
	        int counter = 0;
	        while(resultSet.next()){
	        	counter++;
	        	itemToBeAdded = true;
	        	if(itemToBeAdded){
		        	levelId = resultSet.getString("COMP_STR_NO");
		        	itemCode = resultSet.getString("ITEM_CODE");
		        	zoneKey = resultSet.getString("ZONE_NUM");
		        	//key = new StoreItemKey(levelTypeId, levelId);
		        	if(storeItemMap.get(itemCode) != null){
		        		HashMap<String, List<String>> tempMap = storeItemMap.get(itemCode);
		        		if(tempMap.get(zoneKey) != null){
		        			List<String> tempList = tempMap.get(zoneKey);
		        			tempList.add(levelId);
		        			tempMap.put(zoneKey, tempList);
		        		}else{
		        			List<String> tempList = new ArrayList<>();
		        			tempList.add(levelId);
		        			tempMap.put(zoneKey, tempList);
		        		}
		        		storeItemMap.put(itemCode, tempMap);
		        	}else{
		        		HashMap<String, List<String>> tempMap = new HashMap<>();
		        		List<String> tempList = new ArrayList<>();
	        			tempList.add(levelId);
	        			tempMap.put(zoneKey, tempList);
		        		storeItemMap.put(itemCode, tempMap);
		        	}
	        	}
	        }
	        logger.debug("retrieveItemAtZonelevel() - # of records retrieved -> " + counter);
		}
		catch (SQLException e)
		{
			logger.error("Error while executing RETRIEVE_ITEM_STORE_MAPPING_WITH_ZONE - " + e);
			throw new GeneralException("Error while executing RETRIEVE_ITEM_STORE_MAPPING_WITH_ZONE", e);
		}finally{
			PristineDBUtil.close(resultSet);
			PristineDBUtil.close(statement);
			System.gc();
		}
	}
	
	
	/**
	 * 
	 * @param conn
	 * @param itemCodeSet
	 * @return item store mapping
	 * @throws GeneralException
	 */
	public void getStoreItemMapAtZonelevel(Connection conn, 
			Set<String> itemCodeSet, String compStrNo, 
			HashMap<String, HashMap<String, List<String>>> storeItemMap) throws GeneralException{
		logger.debug("Inside getStoreItemMap() of CostDAO");
		int limitcount=0;
		List<String> itemCodeList = new ArrayList<String>();
		
		for(String itemCode:itemCodeSet){
			itemCodeList.add(itemCode);
			limitcount++;
			if( limitcount > 0 && (limitcount%this.commitCount == 0)){
				Object[] values = itemCodeList.toArray();
				retrieveItemAtZonelevel(conn, storeItemMap, values, compStrNo);
				itemCodeList.clear();
            }
			if(limitcount > 0 && (limitcount%Constants.RECS_PROCESSED == 0)){
				logger.info("Fetched store item map for " + limitcount + " items");
			}
		}
		if(itemCodeList.size() > 0){
			Object[] values = itemCodeList.toArray();
			retrieveItemAtZonelevel(conn, storeItemMap, values, compStrNo);
			itemCodeList.clear();
		}
	}
	
	
	/**
	 * Clears DEAL_COST, DEAL_START_DATE, DEAL_END_dATE fields
	 * @param conn
	 * @param calendarId
	 * @throws GeneralException
	 */
	public void clearDealCost(Connection conn, int calendarId) throws GeneralException{
		PreparedStatement statement = null;
		try{
			statement = conn.prepareStatement(CLEAR_DEAL_COST);
	        statement.setInt(1, calendarId);
	        int updCnt = statement.executeUpdate();
	        logger.info("No of rows with deal cost cleared - " + updCnt);
		}catch (SQLException e){
			logger.error("Error while executing CLEAR_DEAL_COST " + e);
		}finally{
			PristineDBUtil.close(statement);
		}
	}
	
	/**
	 * Populate level2cost as deal cost
	 * @param conn
	 * @param calendarId
	 * @throws GeneralException
	 */
	public void populateDealAsLevel2(Connection conn, int calendarId) throws GeneralException{
		PreparedStatement statement = null;
		try{
			statement = conn.prepareStatement(POPULATE_DEAL_AS_LEVEL2);
	        statement.setInt(1, calendarId);
	        int updCnt = statement.executeUpdate();
	        logger.info("No of rows updated with level2cost as deal cost - " + updCnt);
		}catch (SQLException e){
			logger.error("Error while executing POPULATE_DEAL_AS_LEVEL2 " + e);
		}finally{
			PristineDBUtil.close(statement);
		}
	}
	
	/**
	 * Checks cost availability for a week
	 * @throws GeneralException 
	 * 
	 */
	public boolean checkCostAvailablity(Connection conn, int calendarId) throws GeneralException{
		PreparedStatement statement = null;
		ResultSet resultSet = null;
		boolean isCostAvailable = false;
		try{
			statement = conn.prepareStatement(CHECK_COST_AVAILABILITY_FOR_WEEK);
			int counter = 0;
			statement.setInt(++counter, calendarId);
			resultSet = statement.executeQuery();
			if(resultSet.next()){
				isCostAvailable = true;
			}
		}
		catch(SQLException sqlE){
			logger.error("Error while checking cost availability - " + sqlE.toString());
			throw new GeneralException("Error while checking cost availability", sqlE);
		}
		
		return isCostAvailable;
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
			statement = conn.prepareStatement(SAVE_IGNORED_COST_RECORDS);
	        
			int recordCnt = 0;
			int itemNoInBatch = 0;
	        for(PriceAndCostDTO priceAndCostDTO:ignoredRecords){
	        	int counter = 0;
	        	//CALENDAR_ID, RECORD_TYPE, SRC_VENDOR_AND_ITEM_ID, UPC, LEVEL_ID, STORE_OR_ZONE_NO, EFF_DATE, 
	        	//LIST_COST, PROMO_EFF_START_DATE, PROMO_EFF_END_DATE, PROMO_COST, COMPANY_PACK, PROCESSED_DATE 
	        	statement.setInt(++counter, priceAndCostDTO.getCalendarId());
	        	statement.setString(++counter, priceAndCostDTO.getRecordType());
	        	statement.setString(++counter, priceAndCostDTO.getVendorNo() + priceAndCostDTO.getItemNo());
	        	statement.setString(++counter, priceAndCostDTO.getUpc());
	        	statement.setString(++counter, priceAndCostDTO.getSourceCode());
	        	statement.setString(++counter, priceAndCostDTO.getZone());
	        	statement.setString(++counter, priceAndCostDTO.getCostEffDate());
	        	statement.setString(++counter, priceAndCostDTO.getStrCurrentCost());
	        	if(Integer.parseInt(priceAndCostDTO.getPromoCostEffDate()) > 0)
	        		statement.setString(++counter, priceAndCostDTO.getPromoCostEffDate());
	        	else
	        		statement.setNull(++counter, Types.NULL);
	        	if(Integer.parseInt(priceAndCostDTO.getPromoCostEndDate()) > 0)
	        		statement.setString(++counter, priceAndCostDTO.getPromoCostEndDate());
	        	else
	        		statement.setNull(++counter, Types.NULL);
	        	statement.setString(++counter, priceAndCostDTO.getStrPromoCost());
	        	statement.setString(++counter, priceAndCostDTO.getCompanyPack());
	        	statement.addBatch();
	        	itemNoInBatch++;
	        	recordCnt++;
	        	if(itemNoInBatch % Constants.BATCH_UPDATE_COUNT == 0){
	        		int[] count = statement.executeBatch();
	        		statement.clearBatch();
	        		itemNoInBatch = 0;
	        	}
	        	if(recordCnt % 10000 == 0){
	        		conn.commit();
	        	}
	        }
	        if(itemNoInBatch > 0){
	        	int[] count = statement.executeBatch();
        		statement.clearBatch();
	        }
		}
		catch (SQLException e)
		{
			logger.error("Error while executing SAVE_IGNORED_RECORDS");
			throw new GeneralException("Error while executing SAVE_IGNORED_RECORDS", e);
		}
	    catch (Exception e)
		{
			logger.error("Error while executing SAVE_IGNORED_RECORDS");
			throw new GeneralException("Error while executing SAVE_IGNORED_RECORDS", e);
		}
	    finally{
			PristineDBUtil.close(statement);
		}
	}
	
	
	/**
	 * 
	 * @param conn
	 * @param calendarId
	 * @return item level cost info from temp table.
	 * @throws GeneralException 
	 */
	public HashMap<String, List<RetailCostDTO>> getRetailCostFromTemp(Connection conn, int calendarId) throws GeneralException{
		HashMap<String, List<RetailCostDTO>> tempCostMap = new HashMap<>();
		List<RetailCostDTO> retailCostDBList = new ArrayList<>();
		PreparedStatement statement = null;
		ResultSet resultSet = null;
		try{
			statement = conn.prepareStatement(GET_RETAIL_COST_INFO_TEMP);
			statement.setInt(1, calendarId);
			resultSet = statement.executeQuery();
			while(resultSet.next()){
				RetailCostDTO retailCostDTO = new RetailCostDTO();
	        	retailCostDTO.setItemcode(resultSet.getString("ITEM_CODE"));
	        	retailCostDTO.setCalendarId(resultSet.getInt("CALENDAR_ID"));
	        	retailCostDTO.setLevelTypeId(resultSet.getInt("LEVEL_TYPE_ID"));
	        	retailCostDTO.setLevelId(resultSet.getString("LEVEL_ID"));
	        	retailCostDTO.setListCost(resultSet.getFloat("LIST_COST"));
	        	retailCostDTO.setEffListCostDate(resultSet.getString("EFF_LIST_COST_DATE"));
	        	retailCostDTO.setDealCost(resultSet.getFloat("DEAL_COST"));
	        	retailCostDTO.setDealStartDate(resultSet.getString("DEAL_START_DATE"));
	        	retailCostDTO.setDealEndDate(resultSet.getString("DEAL_END_DATE"));
	        	retailCostDTO.setPromotionFlag(resultSet.getString("PROMOTION_FLG"));
	        	retailCostDTO.setLevel2Cost(resultSet.getFloat("LEVEL2COST"));
	        	retailCostDTO.setVipCost(resultSet.getFloat("VIP_COST"));
	        	retailCostDTO.setFutureAsRegular(true);
	        	retailCostDBList.add(retailCostDTO);
			}
			
			for(RetailCostDTO retailCostDTO:retailCostDBList){
				String dbItemCOde = retailCostDTO.getItemcode();
				if(tempCostMap.get(dbItemCOde) != null){
	        		List<RetailCostDTO> tempList = tempCostMap.get(dbItemCOde);
	        		tempList.add(retailCostDTO);
	        		tempCostMap.put(dbItemCOde, tempList);
	        	}else{
	        		List<RetailCostDTO> tempList = new ArrayList<RetailCostDTO>();
	        		tempList.add(retailCostDTO);
	        		tempCostMap.put(dbItemCOde, tempList);
	        	}
			}
			
		}catch(SQLException sqlE){
			throw new GeneralException("Error while getting records from TEMP_RETAIL_COST_INFO", sqlE);
		}
		finally{
			PristineDBUtil.close(resultSet);
			PristineDBUtil.close(statement);
		}
		return tempCostMap;
	}
/**
 * 
 * @param conn
 * @param calendarId
 * @throws GeneralException
 */
	public void deleteTempRecords(Connection conn, int calendarId) throws GeneralException{
		PreparedStatement statement = null;
		try{
			statement = conn.prepareStatement("DELETE FROM TEMP_RETAIL_COST_INFO WHERE CALENDAR_ID = " + calendarId);
			statement.executeUpdate();
		}catch(SQLException sqlE){
			throw new GeneralException("Error while deleting temp records.", sqlE);
		}
		finally{
			PristineDBUtil.close(statement);
		}
	}
	
	/**
	 * To get price zone from store item map based on the item codes..
	 * @param conn
	 * @param itemCodes
	 * @return
	 * @throws GeneralException
	 */
	public HashMap<String, List<String>> getPrizeZoneFromStoreItemMap(Connection conn, List<String> itemCodes) throws GeneralException{
		HashMap<String, List<String>> itemCodeAndPriceZoneMap = new HashMap<String, List<String>>();
		PreparedStatement stmt = null;
		ResultSet rs = null;
		List<String> itemCodeList = new ArrayList<String>();
		for(String itemCode:itemCodes){
			itemCodeList.add(itemCode);
		}
		String itemCodeValue = PRCommonUtil.getCommaSeperatedStringFromStrArray(itemCodeList);
		try{
			String query = new String(GET_PRICE_ZONE_FROM_STORE_ITEM_MAP);
			query = query.replaceAll("%ITEM_CODE%", itemCodeValue);
			stmt = conn.prepareStatement(query);
			rs = stmt.executeQuery();
			while(rs.next()){
				List<String> priceZoneList = new ArrayList<String>();
				if(itemCodeAndPriceZoneMap.containsKey(rs.getString("ITEM_CODE"))){
					priceZoneList = itemCodeAndPriceZoneMap.get(rs.getString("ITEM_CODE"));
				}
				priceZoneList.add(rs.getString("PRICE_ZONE"));
				itemCodeAndPriceZoneMap.put(rs.getString("ITEM_CODE"), priceZoneList);
			}
		}catch(SQLException exception){
			logger.error("Error in getPrizeZoneFromStoreItemMap() - " + exception);
			throw new GeneralException("Exception in getPrizeZoneFromStoreItemMap() " + exception);
		}finally{
			PristineDBUtil.close(rs);
			PristineDBUtil.close(stmt);
		}
		return itemCodeAndPriceZoneMap;
	}
	
	
	public void deleteCostInfoUsingScanBackItems(Connection conn, int calendarId, String itemCodes)
			throws GeneralException {
		logger.debug("Inside deleteRetailCostData() of RetailCostDAO");
		PreparedStatement statement = null;
		try {
			String query = new String(DELETE_RETAIL_COST_INFO_WEEKLY_USING_SCANBACK_ITEMS);
			query = query.replaceAll("%ITEM_CODE%", itemCodes);
			statement = conn.prepareStatement(query);
			int counter = 0;
			statement.setInt(++counter, calendarId);
			statement.executeUpdate();
			statement.clearBatch();
		} catch (SQLException e) {
			logger.error("Error while executing deleteCostInfoUsingScanBackItems");
			throw new GeneralException("Error while executing deleteCostInfoUsingScanBackItems", e);
		} finally {
			PristineDBUtil.close(statement);
		}
	}
	
	/**
	 * To update list cost2 in Retail price info for GE
	 * @param conn
	 * @param calendarId
	 * @throws GeneralException
	 */
	public void updateListCost2(Connection conn, int calendarId) throws GeneralException{
		logger.debug("Inside updateListCost2() of RetailCostDAO");
		PreparedStatement statement = null;
		try {
			String query = new String(UPDATE_LISTCOST2_IN_RETAIL_PRICE_INFO);
			statement = conn.prepareStatement(query);
			int counter = 0;
			statement.setInt(++counter, calendarId);
			statement.executeUpdate();
			statement.clearBatch();
		} catch (SQLException e) {
			logger.error("Error while executing updateListCost2");
			throw new GeneralException("Error while executing updateListCost2", e);
		} finally {
			PristineDBUtil.close(statement);
		}
	}
	
	
	/**
	 * To update list cost2 in Retail price info for GE
	 * @param conn
	 * @param calendarId
	 * @throws GeneralException
	 */
	public void updateListCost2Final(Connection conn, int calendarId) throws GeneralException{
		logger.debug("Inside updateListCost2() of RetailCostDAO");
		PreparedStatement statement = null;
		try {
			String query = new String(UPDATE_LISTCOST2_IN_RETAIL_PRICE_INFO_CORRECTION);
			statement = conn.prepareStatement(query);
			int counter = 0;
			statement.setInt(++counter, calendarId);
			statement.executeUpdate();
			statement.clearBatch();
		} catch (SQLException e) {
			logger.error("Error while executing updateListCost2");
			throw new GeneralException("Error while executing updateListCost2", e);
		} finally {
			PristineDBUtil.close(statement);
		}
	}
	
	/**
	 * This method deletes retail cost data into RETAIL_COST_INFO table
	 * @param conn				Connection
	 * @param toBeDeletedList	List of records that needs to be deleted from retail_cost_info
	 * @throws GeneralException
	 */
	public void deleteFutureRetailCostData(Connection conn, int calendarId) throws GeneralException{
		logger.debug("Inside deleteFutureRetailCostData() of RetailCostDAO");
		PreparedStatement statement = null;
		try{
			String query = new String(DELETE_FUTURE_RETAIL_COST_INFO);
			logger.debug(query);
			statement = conn.prepareStatement(query);
	        	logger.debug("Records to be delted where calendar id greater than "+calendarId);
	        	statement.setInt(1, calendarId);
	        	 int updCnt =statement.executeUpdate();
	        	 logger.debug("# of rows deleted: "+updCnt);
		}
		catch (SQLException e)
		{
			logger.error("Error while executing DELETE_FUTURE_RETAIL_COST_INFO");
			throw new GeneralException("Error while executing DELETE_FUTURE_RETAIL_COST_INFO", e);
		}
		finally{
			PristineDBUtil.close(statement);
		}
	}
	
	
	/**
	 * 
	 * @param conn
	 * @return mapping dsd and warehouse zones
	 * @throws GeneralException
	 */
	public HashMap<Integer, HashMap<String, String>> getDSDAndWHSEZoneMap(Connection conn, String cats) throws GeneralException {
		logger.debug("Inside retrieveItemStoreMapping() of CostDAO");
		PreparedStatement statement = null;
		ResultSet resultSet = null;
		HashMap<Integer, HashMap<String, String>> dsdWhseZoneMapping = new HashMap<>();
		try {
			StringBuilder sb = new StringBuilder();
			int defaultProdId = Integer
					.parseInt(PropertyManager.getProperty("DEFAULT_PRODUCT_ID_IN_PROD_LOC_MAPPING", "0"));
			sb.append(" SELECT PLM.PRODUCT_ID, RPZ_WHSE.ZONE_NUM WHSE_ZONE, RPZ_DSD.ZONE_NUM DSD_ZONE ");
			sb.append(" FROM PR_PRODUCT_LOCATION_MAPPING PLM ");
			sb.append(" LEFT JOIN RETAIL_PRICE_ZONE RPZ_DSD ON RPZ_DSD.PRICE_ZONE_ID = PLM.LOCATION_ID ");
			sb.append(" LEFT JOIN RETAIL_PRICE_ZONE RPZ_WHSE ON RPZ_WHSE.PRICE_ZONE_ID = PLM.PARENT_LOCATION_ID ");
			sb.append(" WHERE PLM.PRODUCT_ID <> " + defaultProdId + " AND PLM.PARENT_LOCATION_ID IS NOT NULL ");
			if(cats != null){
				sb.append(" AND PLM.PRODUCT_ID IN ( " + cats + " ) ");
			}
			
			logger.debug("Zone Mapping Query: " + sb.toString());

			statement = conn.prepareStatement(sb.toString());
			statement.setFetchSize(3000000);
			resultSet = statement.executeQuery();
			String whseZone = null;
			String dsdZone = null;
			int productId = 0;
			
			while (resultSet.next()) {
				whseZone = resultSet.getString("WHSE_ZONE");
				dsdZone = resultSet.getString("DSD_ZONE");
				productId = resultSet.getInt("PRODUCT_ID");
				if(dsdWhseZoneMapping.containsKey(productId)){
					HashMap<String, String> zoneMap = dsdWhseZoneMapping.get(productId);
					zoneMap.put(dsdZone, whseZone);
					dsdWhseZoneMapping.put(productId, zoneMap);
				}else{
					HashMap<String, String> zoneMap = new HashMap<>();
					zoneMap.put(dsdZone, whseZone);
					dsdWhseZoneMapping.put(productId, zoneMap);
				}
				
			}
		} catch (SQLException e) {
			logger.error("Error while executing getDSDAndWHSEZoneMap() - ", e);
			throw new GeneralException("Error while executing getDSDAndWHSEZoneMap()", e);
		} finally {
			PristineDBUtil.close(resultSet);
			PristineDBUtil.close(statement);
		}
		
		return dsdWhseZoneMapping;
	}
	
	
	/**
	 * 
	 * @param conn
	 * @param itemCodeSet
	 * @return dsd zone to whse zone mapping for given items
	 * @throws GeneralException
	 */
	public HashMap<String, HashMap<String, List<String>>> 
				getDSDandWHSEZoneMapping(Connection conn,Set<String> itemCodeSet) throws GeneralException {
		
		List<String> itemCodeList = new ArrayList<String>();
		HashMap<String, HashMap<String, List<String>>> dsdAndWhseZoneMapping = new HashMap<>();
		int limitcount = 0;
		for(String itemCode:itemCodeSet){
			itemCodeList.add(itemCode);
			limitcount++;
			if( limitcount > 0 && (limitcount%this.commitCount == 0)){
				Object[] values = itemCodeList.toArray();
				retrieveZoneMapping(conn, dsdAndWhseZoneMapping, values);
				itemCodeList.clear();
            }
		}
		if(itemCodeList.size() > 0){
			Object[] values = itemCodeList.toArray();
			retrieveZoneMapping(conn, dsdAndWhseZoneMapping, values);
			itemCodeList.clear();
		}
		
		return dsdAndWhseZoneMapping;

	}
	
	
	/**
	 * 
	 * @param conn
	 * @param dsdAndWhseZoneMapping
	 * @param values
	 * @throws GeneralException
	 */
	public void retrieveZoneMapping(Connection conn, 
			HashMap<String, HashMap<String, List<String>>> dsdAndWhseZoneMapping, 
			Object[] values) throws GeneralException{
		logger.debug("Inside retrieveItemStoreMapping() of CostDAO");
		PreparedStatement statement = null;
	    ResultSet resultSet = null;
		try{
			StringBuilder sb = new StringBuilder();
			
			sb.append("SELECT ITEM_CODE, DSD_ZONE_NUM, WHSE_ZONE_NUM FROM ");
			sb.append(" (SELECT DISTINCT WHSE_ZONE_ID, DSD_ZONE_ID, ITEM_CODE, ");
			sb.append(" DSD_ZONE.ZONE_NUM DSD_ZONE_NUM, WHSE_ZONE.ZONE_NUM WHSE_ZONE_NUM ");
			sb.append(" FROM (SELECT DSD.ITEM_CODE, WHSE.PRICE_ZONE_ID WHSE_ZONE_ID, ");
			sb.append(" DSD.PRICE_ZONE_ID DSD_ZONE_ID FROM STORE_ITEM_MAP WHSE ");
			sb.append(" JOIN STORE_ITEM_MAP DSD ON DSD.LEVEL_ID = WHSE.LEVEL_ID ");
			sb.append(" AND DSD.DIST_FLAG = '" + Constants.DSD + "' AND WHSE.DIST_FLAG = '" + Constants.WAREHOUSE + "' ");
			sb.append(" WHERE WHSE.IS_AUTHORIZED = 'Y' AND DSD.IS_AUTHORIZED = 'Y') STM ");
			sb.append(" LEFT JOIN RETAIL_PRICE_ZONE DSD_ZONE ON DSD_ZONE.PRICE_ZONE_ID = STM.DSD_ZONE_ID ");
			sb.append(" LEFT JOIN RETAIL_PRICE_ZONE WHSE_ZONE ON WHSE_ZONE.PRICE_ZONE_ID = STM.WHSE_ZONE_ID) ");
			sb.append(" WHERE ITEM_CODE IN (%s) ");

			statement = conn.prepareStatement(String.format(sb.toString(), PristineDBUtil.preparePlaceHolders(values.length)));
			
			PristineDBUtil.setValues(statement, values);
			resultSet = statement.executeQuery();
	        String whseZone = null;
	        String itemCode = null;
	        //StoreItemKey key = null;
	        String dsdZone = null;
	        boolean itemToBeAdded = false;
	        int counter = 0;
	        while(resultSet.next()){
	        	counter++;
	        	itemToBeAdded = true;
	        	if(itemToBeAdded){
	        		whseZone = resultSet.getString("WHSE_ZONE_NUM");
		        	itemCode = resultSet.getString("ITEM_CODE");
		        	dsdZone = resultSet.getString("DSD_ZONE_NUM");
		        	if(dsdAndWhseZoneMapping.get(itemCode) != null){
		        		HashMap<String, List<String>> tempMap = dsdAndWhseZoneMapping.get(itemCode);
		        		if(tempMap.get(dsdZone) != null){
		        			List<String> tempList = tempMap.get(dsdZone);
		        			tempList.add(whseZone);
		        			tempMap.put(dsdZone, tempList);
		        		}else{
		        			List<String> tempList = new ArrayList<>();
		        			tempList.add(whseZone);
		        			tempMap.put(dsdZone, tempList);
		        		}
		        		dsdAndWhseZoneMapping.put(itemCode, tempMap);
		        	}else{
		        		HashMap<String, List<String>> tempMap = new HashMap<>();
		        		List<String> tempList = new ArrayList<>();
	        			tempList.add(whseZone);
	        			tempMap.put(dsdZone, tempList);
		        		dsdAndWhseZoneMapping.put(itemCode, tempMap);
		        	}
	        	}
	        }
	        logger.debug("retrieveZoneMapping() - # of records retrieved -> " + counter);
		}
		catch (SQLException e)
		{
			logger.error("Error while executing retrieveZoneMapping() - ", e);
			throw new GeneralException("Error while executing retrieveZoneMapping()", e);
		}finally{
			PristineDBUtil.close(resultSet);
			PristineDBUtil.close(statement);
		}
	}
	
	/**
	 * Deletes Current and Future cost form retail_cost_info table from given calendar id
	 * @param conn			Database connection
	 * @param calendarId	Calendar Id
	 * @throws GeneralException
	 */
	public void deleteCurAndFutureRetailCostData(Connection conn, String weekStartDate, int noOfWeeks) throws GeneralException{
		logger.debug("Inside deleteRetailCostData() of RetailCostDAO");
		PreparedStatement statement = null;
		try{
			statement = conn.prepareStatement(DELETE_CUR_AND_FUTURE_RETAIL_COST_INFO);
	        
	        statement.setString(1, weekStartDate);
	        statement.setString(2, weekStartDate);
	        statement.setInt(3, noOfWeeks);
	        int count = statement.executeUpdate();
		}
		catch (SQLException e)
		{
			logger.error("Error while executing DELETE_RETAIL_COST_INFO_WEEKLY_ITEMS");
			throw new GeneralException("Error while executing DELETE_RETAIL_COST_INFO_WEEKLY_ITEMS", e);
		}
		finally{
			PristineDBUtil.close(statement);
		}
	}
}	
