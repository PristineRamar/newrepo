package com.pristine.dao;

import java.io.InputStream;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Types;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Date;
import java.util.Enumeration;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Hashtable;
import java.util.List;
import java.util.Map;
import java.util.Properties;

import javax.sql.rowset.CachedRowSet;

import org.apache.log4j.Logger;

import com.pristine.dto.MovementDTO;
import com.pristine.dto.OOSMovementProductGroupExpDTO;
import com.pristine.dto.OOSPredictedMovementDTO;
import com.pristine.dto.OOSSummaryDTO;
import com.pristine.dto.ScheduleInfoDTO;
import com.pristine.dto.ShippingInfoDTO;
import com.pristine.exception.GeneralException;
import com.pristine.util.Constants;
import com.pristine.util.NumberUtil;
import com.pristine.util.PristineDBUtil;
import com.pristine.util.PropertyManager;
import com.sun.rowset.CachedRowSetImpl;


public class OOSCalcsDAO implements IDAO {

	private String oosReportSQL;
	private String weeklyAdSQL;
	private String lastMovementSQL;
	private String itemListforSubstituteSQL;
	private Properties properties = new Properties();
	
	public OOSCalcsDAO() {
		super();
		String sqlFileName = "sql/SQLDefinitions.xml";
		 InputStream is = this.getClass().getClassLoader().getResourceAsStream(sqlFileName);
		 try {
			properties.loadFromXML(is);
		} catch (Exception e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
		 oosReportSQL = properties.getProperty("OOSReportSQL");
		 weeklyAdSQL = properties.getProperty("weeklyAdSQL");
		 lastMovementSQL = properties.getProperty("lastMovementSQL");
		 itemListforSubstituteSQL = properties.getProperty("itemListforSubstituteSQL");
		 
	}

	private static Logger	logger	= Logger.getLogger("OOSCalcsDAO");
	private static final String TABLE_NAME_MOVEMENT_DAILY = "MOVEMENT_DAILY_OOS";
	private static String lineSeparator = System.lineSeparator();
	
	private Hashtable<Integer,ArrayList<MovementDTO>> movementCache = new Hashtable<Integer,ArrayList<MovementDTO>>();
	
	public CachedRowSet getLIGItemList ( Connection conn, int checkListId ) throws GeneralException {
		
		StringBuffer buffer = new StringBuffer();
		buffer.append(" SELECT * FROM ( ");
		buffer.append(" SELECT DISTINCT D.ITEM_CODE, C.RET_LIR_ID, C.RET_LIR_ITEM_CODE ");
		buffer.append(" FROM PRICE_CHECK_LIST_ITEMS A, ITEM_LOOKUP B, RETAILER_LIKE_ITEM_GROUP C, ITEM_LOOKUP D ");
		buffer.append(" WHERE A.PRICE_CHECK_LIST_ID = ").append(checkListId);
		buffer.append(" AND B.ITEM_CODE = A.ITEM_CODE ");
		buffer.append(" AND B.RET_LIR_ID > 0 ");
		buffer.append(" AND C.RET_LIR_ID = B.RET_LIR_ID ");
		buffer.append(" AND D.RET_LIR_ID = C.RET_LIR_ID )");
		buffer.append(" ORDER BY RET_LIR_ID ");
		CachedRowSet result = PristineDBUtil.executeQuery(conn, buffer, "getCompDataForItem");
		return result;

	}
	

	public CachedRowSet getItemListForAnalysis ( Connection conn ) throws GeneralException {
		
		StringBuffer buffer = new StringBuffer();
		buffer.append(" SELECT ITEM_CODE, -1 RET_LIR_ID, ITEM_CODE RET_LIR_ITEM_CODE ");
		buffer.append(" FROM ITEM_LOOKUP  ");
		buffer.append(" WHERE " +
				"	 dept_id In (22,23,15,35,36,32,19,14,16,17,26,28,18,29,33 )and active_indicator = 'Y'");
		buffer.append(" ORDER BY ITEM_CODE");
		CachedRowSet result = PristineDBUtil.executeQuery(conn, buffer, "getItemListForAnalysis");
		return result;

	}

	/*
	 dept_id In (
22,
23,
15,
35,
36,
32,
19,
14,
16,
17,
26,
28,
18,
29,
33 )
and active_indicator = 'Y'; 
	 */
	
	public CachedRowSet getNonLIGItemList ( Connection conn, int checkListId ) throws GeneralException {
		StringBuffer buffer = new StringBuffer();
		buffer.append(" SELECT A.ITEM_CODE, -1 RET_LIR_ID, A.ITEM_CODE RET_LIR_ITEM_CODE ");
		buffer.append(" FROM PRICE_CHECK_LIST_ITEMS A, ITEM_LOOKUP B");
		buffer.append(" WHERE PRICE_CHECK_LIST_ID = ").append(checkListId);
		buffer.append(" AND B.ITEM_CODE = A.ITEM_CODE ");
		//buffer.append(" AND B.RET_LIR_ID IS NULL ");
//		buffer.append(" AND B.ITEM_CODE = 893172");  // FOR DEBUG
		CachedRowSet result = PristineDBUtil.executeQuery(conn, buffer, "getCompDataForItem");
		return result;
		
		
	}

	public CachedRowSet getPreProcessItemList ( Connection conn, String storeNum,  int storeId, String analysisDate ) throws GeneralException {
		return getDynamicItemList(conn,storeId,analysisDate);
		
		
	}

	
	public CachedRowSet getDynamicItemList ( Connection conn,   int storeId, String analysisDate ) throws GeneralException {
		StringBuffer buffer = new StringBuffer();
		
		buffer.append(" select item_id ITEM_CODE, item_id RET_LIR_ID, item_id RET_LIR_ITEM_CODE from (" +
				"select distinct item_id from (select item_id , calendar_id ,quantity from transaction_log m" +
				" where calendar_id >= (select calendar_id from retail_calendar where start_date = to_date('"+analysisDate+"','MM/dd/yyyy')-15 and row_type ='D')" +
				" and quantity > 0 and store_id = "+storeId+" ) where" +
				" item_id in (select item_code from item_lookup where dept_id In (22, 23,15,35,36,32,19,14,16,17,26,28,18,29,33) and active_indicator = 'Y') " +
				" and calendar_id in (select calendar_id from retail_calendar where start_date >= to_date('"+analysisDate+"','MM/dd/yyyy')-15 " +
						"and start_date < to_date('"+analysisDate+"','MM/dd/yyyy') and row_type ='D')" +
						")");
		
		/*

		 
		 select aI ITEM_CODE, aI RET_LIR_ID, aI RET_LIR_ITEM_CODE 
from (select distinct item_id aI 
from transaction_log m, item_lookup il, retail_calendar c 
where m.item_id = il.item_code
and m.calendar_id = c.calendar_id and  
store_id = 5651 and c.start_date > to_date('07/23/2013','MM/dd/yyyy')-15 
and dept_id In (22, 23,15,35,36,32,19,14,16,17,26,28,18,29,33) and active_indicator = 'Y') 

		 
		 
		 dept_id In (
22, / BEAUTY CARE /
23, /  BEVERAGE/SODA/SNACKS /
15, /  BREAKFAST  BAKING  /
35, /  BULK  /
36, /  COFFEE COCOA CANDY JUICE PBJ  /
32, /  CONDIMENT COOKIE CRACKER BREAD  /
19, /  DAIRY  /
14, /  ENHANCERS  /
16, /  ETHNIC/SPEC/KOSHER/NO  /
17, /  FROZEN  /
26, /  GENERAL MERCHANDISE /
28, /  HEALTH CARE  /
18, /  HOUSEHOLD  /
29, /  MAIN MEAL /
33 / seafood /)
and active_indicator = 'Y' 

		 */
//		buffer = new StringBuffer("select 835108 ITEM_CODE, 835108 RET_LIR_ID, 835108 RET_LIR_ITEM_CODE from dual"); // for debug
		
		CachedRowSet result = PristineDBUtil.executeQuery(conn, buffer, "getPreProcessItemList");
		return result;
		
		
	}


	public CachedRowSet getDynamicItemListExt ( Connection conn,   int storeId, String analysisDate ) throws GeneralException {
		StringBuffer buffer = new StringBuffer();
		
		buffer.append(" select cat_name,segment_name,item_code,item_name  from item_lookup_view where item_code in (" +
				"select distinct item_id from (select item_id , calendar_id ,quantity from transaction_log m" +
				" where calendar_id >= (select calendar_id from retail_calendar where start_date = to_date('"+analysisDate+"','MM/dd/yyyy')-15 and row_type ='D')" +
				" and quantity > 0 and store_id = "+storeId+" ) where" +
				" item_id in (select item_code from item_lookup where dept_id In (22, 23,15,35,36,32,19,14,16,17,26,28,18,29,33) and active_indicator = 'Y') " +
				" and calendar_id in (select calendar_id from retail_calendar where start_date >= to_date('"+analysisDate+"','MM/dd/yyyy')-15 " +
						"and start_date < to_date('"+analysisDate+"','MM/dd/yyyy') and row_type ='D')" +
						")");
		
		
		CachedRowSet result = PristineDBUtil.executeQuery(conn, buffer, "getPreProcessItemList");
		return result;
		
		
	}

	
	public Hashtable<Integer,ShippingInfoDTO> getShipmentInfoForDynamicItemList ( Connection conn,   int storeId, String analysisDate ) throws GeneralException {
		StringBuffer buffer = new StringBuffer();
		
		buffer.append(" select shipment_date, quantity, item_code, cases from shipping_info where latest_ind = 'Y' and quantity > 0 and store_id = "+storeId+" and item_code in (" +
				"select distinct item_id from (select item_id , calendar_id ,quantity from transaction_log m" +
				" where calendar_id >= (select calendar_id from retail_calendar where start_date = to_date('"+analysisDate+"','MM/dd/yyyy')-15 and row_type ='D')" +
				" and quantity > 0 and store_id = "+storeId+" ) where" +
				" item_id in (select item_code from item_lookup where dept_id In (22, 23,15,35,36,32,19,14,16,17,26,28,18,29,33) and active_indicator = 'Y') " +
				" and calendar_id in (select calendar_id from retail_calendar where start_date >= to_date('"+analysisDate+"','MM/dd/yyyy')-15 " +
						"and start_date < to_date('"+analysisDate+"','MM/dd/yyyy') and row_type ='D')" +
						")");
		
		CachedRowSet result = PristineDBUtil.executeQuery(conn, buffer, "getPreProcessItemList");
		Hashtable<Integer,ShippingInfoDTO> shipmentInfo = new Hashtable<Integer,ShippingInfoDTO>(); 
		try {
			while(result.next()){
				ShippingInfoDTO sDTO = new ShippingInfoDTO();
				sDTO.setShipmentDate(result.getDate("shipment_date"));
				sDTO.setQuantity(result.getInt("quantity"));
				sDTO.setCasesShipped(result.getInt("cases"));
				Integer itemCode = result.getInt("item_code");
				ShippingInfoDTO sDTO1 = shipmentInfo.get(itemCode);
				if(sDTO1 !=null){
					if(sDTO.getShipmentDate().after(sDTO1.getShipmentDate())){
						shipmentInfo.put(result.getInt("item_code"),sDTO);
					}
				}else{
					shipmentInfo.put(result.getInt("item_code"),sDTO);
				}
			}
			result.close();
		} catch (SQLException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
			throw new GeneralException(e.getMessage());
		}
		return shipmentInfo;
		
		
	}

	public Hashtable<Integer,Integer> getTotalShipmentForDynamicItemList ( Connection conn,   int storeId, String analysisDate, String analysisStartDate ) throws GeneralException {
		StringBuffer buffer = new StringBuffer();
		
		buffer.append(" select sum(quantity) total_qty, item_code from shipping_info where shipment_date > to_date('"+analysisStartDate+"','MM/dd/yyyy')  and store_id = "+storeId+" and item_code in (" +
				"select distinct item_id from (select item_id , calendar_id ,quantity from transaction_log m" +
				" where calendar_id >= (select calendar_id from retail_calendar where start_date = to_date('"+analysisDate+"','MM/dd/yyyy')-15 and row_type ='D')" +
				" and quantity > 0 and store_id = "+storeId+" ) where" +
				" item_id in (select item_code from item_lookup where dept_id In (22, 23,15,35,36,32,19,14,16,17,26,28,18,29,33) and active_indicator = 'Y') " +
				" and calendar_id in (select calendar_id from retail_calendar where start_date >= to_date('"+analysisDate+"','MM/dd/yyyy')-15 " +
						"and start_date < to_date('"+analysisDate+"','MM/dd/yyyy') and row_type ='D')" +
						")  group by item_code");
		
		CachedRowSet result = PristineDBUtil.executeQuery(conn, buffer, "getPreProcessItemList");
		Hashtable<Integer,Integer> shipmentInfo = new Hashtable<Integer,Integer>(); 
		try {
			while(result.next()){
				shipmentInfo.put(result.getInt("item_code"),result.getInt("total_qty"));
			}
			result.close();
		} catch (SQLException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
			throw new GeneralException(e.getMessage());
		}
		return shipmentInfo;
		
		
	}

	public Hashtable<Integer,Double> getMovementSummarySince ( Connection conn,   int storeId, String analysisDate, String analysisStartDate ) throws GeneralException {
		StringBuffer buffer = new StringBuffer();
		
		buffer.append(" select sum(quantity_total) total_qty, item_code from summary_daily_mov s, schedule ss " +
				"where s.schedule_id = ss.schedule_id and start_date + dow_id -1 > to_date('"+analysisStartDate+"','MM/dd/yyyy')  " +
						"and tod_id = 4 and comp_str_id = "+storeId+" and item_code in (" +
				"select distinct item_id from (select item_id , calendar_id ,quantity from transaction_log m" +
				" where calendar_id >= (select calendar_id from retail_calendar where start_date = to_date('"+analysisDate+"','MM/dd/yyyy')-15 and row_type ='D')" +
				" and quantity > 0 and store_id = "+storeId+" ) where" +
				" item_id in (select item_code from item_lookup where dept_id In (22, 23,15,35,36,32,19,14,16,17,26,28,18,29,33) and active_indicator = 'Y') " +
				" and calendar_id in (select calendar_id from retail_calendar where start_date >= to_date('"+analysisDate+"','MM/dd/yyyy')-15 " +
						"and start_date < to_date('"+analysisDate+"','MM/dd/yyyy') and row_type ='D')" +
						")  group by item_code");
		
		logger.info("movement since date sql");
		logger.info(buffer);
		CachedRowSet result = PristineDBUtil.executeQuery(conn, buffer, "getPreProcessItemList");
		Hashtable<Integer,Double> shipmentInfo = new Hashtable<Integer,Double>(); 
		try {
			while(result.next()){
				shipmentInfo.put(result.getInt("item_code"),result.getDouble("total_qty"));
			}
			result.close();
		} catch (SQLException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
			throw new GeneralException(e.getMessage());
		}
		return shipmentInfo;
		
		
	}
	
	public Hashtable<Integer,Double> getWeeklyPredictionForDynItemList( Connection conn,   int storeId,String strAnalysisDate,  String strWeekStart ) throws GeneralException {
		StringBuffer buffer = new StringBuffer();

		 buffer.append("select item_code, sum(pred) wpred from predicted_exp	" +
		 		"	 where comp_str_id = "+storeId+" " +
		 				"and calendar_id in (select calendar_id from retail_calendar where start_date >= to_date('"+strWeekStart+"','MM/dd/yyyy')  " +
		 						"and start_date <= to_date('"+strWeekStart+"','MM/dd/yyyy')+6 )		 " +
		 								"group by item_code"); 
		
		logger.info("weekly prediction");
		logger.info(buffer);
		CachedRowSet result = PristineDBUtil.executeQuery(conn, buffer, "getWeeklyPrediction");
		Hashtable<Integer,Double> weeklyPred = new Hashtable<Integer,Double>(); 
		try {
			while(result.next()){
				weeklyPred.put(result.getInt("item_code"),result.getDouble("wpred"));
			}
			result.close();
		} catch (SQLException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
			throw new GeneralException(e.getMessage());
		}
		return weeklyPred;
		
	}
	
	public Integer getMovementSummaryForItemSince ( Connection conn,   int storeId, String analysisStartDate, int item_code ) throws GeneralException {
		StringBuffer buffer = new StringBuffer();
		
		buffer.append(" select sum(quantity_total)/2 total_qty from summary_daily_mov s, schedule ss where " +
				"s.schedule_id = ss.schedule_id and start_date + dow_id -1 >= to_date('"+analysisStartDate+"','MM/dd/yyyy')  and comp_str_id = "+storeId+" " +
						"and item_code ="+item_code+"");
		
		CachedRowSet result = PristineDBUtil.executeQuery(conn, buffer, "getPreProcessItemList");
		Integer mvmt=null; 
		try {
			while(result.next()){
				mvmt = (int) result.getDouble("total_qty");
			}
			result.close();
		} catch (SQLException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
			throw new GeneralException(e.getMessage());
		}
		return mvmt;
	}
	
	
	public ArrayList <MovementDTO> getMovementData(Connection conn, String storeNum,
			String itemGroup, String startPeriod, String endPeriod, boolean isArchive) throws GeneralException{
		

		ArrayList <MovementDTO> movementList= new ArrayList <MovementDTO> ();
		Integer itemCode = new Integer(itemGroup);
		logger.debug("movement cache size:"+movementCache.size());
		logger.debug("looking for in movement cache :"+itemCode);
		
		
		if(!movementCache.containsKey(itemCode)){
			logger.debug("looking for in movement cache :"+itemCode+" not found");
			return movementList;
		}
		logger.debug("looking for in movement cache :"+itemCode+" FOUND !!");
		List<MovementDTO> mDTOL = movementCache.get(itemCode);
	
		// TODO  sort by timestamp
		Collections.sort(mDTOL);
		
		int i = 0;
		for(MovementDTO mDTO: mDTOL){
			if( i == 0){
				movementList.add(mDTO);
			}else {
				MovementDTO prevMovementDTO = movementList.get(movementList.size()-1);
				int curTranNo = mDTO.getTransactionNo();
				if( prevMovementDTO.getItemTransum().equals(curTranNo) ){
					//Merge the current record with the previous record
					prevMovementDTO.setTranTimeStamp(mDTO.getTranTimeStamp());
					prevMovementDTO.setExtnQty(prevMovementDTO.getExtnQty()  + mDTO.getExtnQty());
					prevMovementDTO.setExtnWeight(prevMovementDTO.getExtnWeight() + mDTO.getExtnWeight());
					prevMovementDTO.setItemNetPrice(prevMovementDTO.getItemNetPrice() + mDTO.getItemNetPrice());
					prevMovementDTO.setExtendedGrossPrice(prevMovementDTO.getExtendedGrossPrice() + mDTO.getExtendedGrossPrice());
				}else {
					movementList.add(mDTO);
				}
	
			}
		i++;	
		}
		return movementList;
		
	}


	public ArrayList <MovementDTO> getMovementDataOld(Connection conn, String storeNum,
			String itemGroup, String startPeriod, String endPeriod, boolean isArchive) throws GeneralException{
		
		//select * from MOVEMENT_DAILY where storeId, date >= and date <= and item_code in 
		//select count(*) from movement_daily A, item_lookup B where pos_timestamp > '1-JUN-11'
		//and  A.UPC = SUBSTR(B.UPC, 2)
		
		//itemGroup = "23422";
		StringBuffer buffer = new StringBuffer();
		buffer.append(" SELECT COMP_STR_NO, UPC, TO_CHAR( POS_TIMESTAMP, 'MM/DD/YYYY HH:MI AM') TRAN_TIMESTAMP, ");
		buffer.append(" UNIT_PRICE, SALE_FLAG, QUANTITY, WEIGHT, "); 
		buffer.append(" PRICE, WEEKDAY, CUSTOMER_CARD_NO, TRANSACTION_NO ");
		if( isArchive)
			buffer.append(" FROM MOVEMENT_DAILY_ARCHIVE ");
		else
			buffer.append(" FROM "+TABLE_NAME_MOVEMENT_DAILY+" ");
		buffer.append(" WHERE COMP_STR_NO = '").append(storeNum).append("'");
		buffer.append(" AND UPC IN (");
		buffer.append(" SELECT SUBSTR(UPC, 2) FROM ITEM_LOOKUP WHERE ITEM_CODE IN (").append(itemGroup).append("))");
		buffer.append(" AND POS_TIMESTAMP >=").append( "TO_DATE('").append(startPeriod).append("','MM/DD/YYYY HH:MI AM')");
		buffer.append(" AND POS_TIMESTAMP <=").append( "TO_DATE('").append(endPeriod).append("','MM/DD/YYYY HH:MI AM')");
		buffer.append(" ORDER BY POS_TIMESTAMP ASC" );
		logger.debug( buffer.toString());
		CachedRowSet result = PristineDBUtil.executeQuery(conn, buffer, "getCompDataForItem");
		
		ArrayList <MovementDTO> movementList= new ArrayList <MovementDTO> (); 
		int i = 0;
		try {
			while (result.next()){
				if( i == 0){
					movementList.add(prepareMovementRecord(result));
				}else {
					//Get the last record, 
					//see if the tran number matches the current one, if so, merge it. 
					MovementDTO prevMovementDTO = movementList.get(movementList.size()-1);
					String curTranNo = result.getString("TRANSACTION_NO");
					if( prevMovementDTO.getItemTransum().equals(curTranNo) ){
						//Merge the current record with the previous record
						prevMovementDTO.setTranTimeStamp(result.getString("TRAN_TIMESTAMP"));
						prevMovementDTO.setExtnQty(prevMovementDTO.getExtnQty()  + result.getDouble("QUANTITY"));
						prevMovementDTO.setExtnWeight(prevMovementDTO.getExtnWeight() + result.getDouble("WEIGHT"));
						prevMovementDTO.setItemNetPrice(prevMovementDTO.getItemNetPrice() + result.getDouble("PRICE"));
					}else {
						movementList.add(prepareMovementRecord(result));
					}
						
				}
				i++;				
			}
		}catch( SQLException sqle){
			throw new GeneralException("Error in accessing cachedRow set", sqle);
		}
		return movementList;
		
	}

	
	private MovementDTO prepareMovementRecord(CachedRowSet movementCrs) throws SQLException {
		MovementDTO movementInfo = new MovementDTO ();
		
		movementInfo.setItemStore(movementCrs.getString("COMP_STR_NO"));
		movementInfo.setItemUPC(movementCrs.getString("UPC"));
		movementInfo.setTranTimeStamp(movementCrs.getString("TRAN_TIMESTAMP"));
		if(movementCrs.getString("SALE_FLAG").equalsIgnoreCase("Y")){
			movementInfo.setSaleFlag(true);
		}
		else{
			movementInfo.setSaleFlag(false);
		}
		movementInfo.setItemCode(movementCrs.getInt("ITEM_CODE"));
		movementInfo.setExtnQty(movementCrs.getDouble("QUANTITY"));
		movementInfo.setExtnWeight(movementCrs.getDouble("WEIGHT"));
		movementInfo.setItemNetPrice(movementCrs.getDouble("PRICE"));
		movementInfo.setCustomerId(movementCrs.getString("CUSTOMER_CARD_NO"));
		movementInfo.setItemTransum(movementCrs.getString("TRANSACTION_NO"));
		movementInfo.setItemDateTime((Date) movementCrs.getObject("POS_TIMESTAMP"));
		movementInfo.setExtendedGrossPrice(movementCrs.getDouble("EXTENDED_GROSS_PRICE"));
		
		return movementInfo;
	}

	public void insertOSSCalcs(Connection conn, List<OOSSummaryDTO> oosSummaryList, boolean allslotsWithMovement) throws GeneralException {

		PreparedStatement pstmt= null;
		logger.debug("Begin insert into db");

		
		StringBuffer sb = new StringBuffer();
		
		sb.append(" INSERT INTO SUMMARY_DAILY_MOV ( ");
		sb.append(" SCHEDULE_ID, DOW_ID, SPECIAL_EVENT, SPECIAL_FACT, TOD_ID, " );
		sb.append(" ITEM_CODE, QUANTITY_TOTAL, QUANTITY_REG, QUANTITY_SALE, ");
		sb.append(" REV_TOTAL, REV_REG, REV_SALE, MARGIN, PROMOTIONAL_IND, VISIT_COUNT, ");
		sb.append(" MAX_DURATION,  MIN_DURATION, AVG_DURATION, MAX_DURATION_95PCT, MIN_DURATION_95PCT, " );
		sb.append(" AVG_QTY_VISIT, MODAL_QTY_VISIT, MODAL_QTY2_VISIT, MAX_QTY_VISIT,");
		sb.append(" MOV_VISIT_RATIO, UNIT_REG_PRICE, UNIT_SALE_PRICE, ");
		sb.append(" MARGIN_OPP, REV_OPP, TOTAL_VISIT_COUNT, OOS_IND,OOS_CLASSIFICATION_ID,OOS_SCORE," +
				"LAST_7DAY_EXP,LAST_7DAY_OBS,LAST_7DAY_OBS_USED,LAST_28DAY_EXP,LAST_28DAY_OBS,LAST_28DAY_OBS_USED,LAST_180DAY_EXP,LAST_180DAY_OBS,LAST_180DAY_OBS_USED,LAST_7DAY_AVG_EXP,LAST_7DAY_AVG_OBS,PREV_DAY_EXP, OOS_METHOD, " +
				" LAST_7DAY_DAYS_MOVED,LAST_28DAY_DAYS_MOVED,LAST_180DAY_DAYS_MOVED, MAPE,SIGMA_USED,MVD_TWO_PLUS_LAST_N_DAYS," +
				"AD_FACTOR_EXP,AD_FACTOR_SIGMA,AD_FACTOR_METHOD,PRED_MODEL_EXP,FOCUS_ITEM,LAST_MOVEMENT) ");
		sb.append(" VALUES " );
		sb.append(" (?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?) ");

		try{
        pstmt=conn.prepareStatement(sb.toString());

		
		for(OOSSummaryDTO oosSummary: oosSummaryList){

		int counter=0;	
		pstmt.setInt(++counter,oosSummary.scheduleId);	
		pstmt.setInt(++counter,oosSummary.dayofweekId);	
		pstmt.setInt(++counter,oosSummary.specialEventId);	
		pstmt.setInt(++counter,oosSummary.specialFactId);	
		pstmt.setInt(++counter,oosSummary.timeOfDayId);	

		pstmt.setInt(++counter,oosSummary.itemCode);	

		pstmt.setDouble(++counter,oosSummary.totalQty);	
		pstmt.setDouble(++counter,oosSummary.regQty);	
		pstmt.setDouble(++counter,oosSummary.saleQty);	
		pstmt.setDouble(++counter,oosSummary.totalRev);	
		pstmt.setDouble(++counter,oosSummary.regRev);	
		pstmt.setDouble(++counter,oosSummary.saleRev);	

		pstmt.setDouble(++counter,oosSummary.margin);	

		pstmt.setString(++counter,(oosSummary.onSale)? "Y":"N");	
		pstmt.setInt(++counter,oosSummary.visitCount);	

		pstmt.setInt(++counter,oosSummary.maxDurationBtwVisit);	
		pstmt.setInt(++counter,oosSummary.minDurationBtwVisit);	
		pstmt.setInt(++counter,oosSummary.avgDurationBtwVisit);	
		pstmt.setInt(++counter,oosSummary.max95PctDurationBtwVisit);	
		pstmt.setInt(++counter,oosSummary.min95PctDurationBtwVisit);	

		pstmt.setDouble(++counter,oosSummary.avgQtyPerVisit);	
		
		if( oosSummary.modalQtyPerVisit > 0){
		pstmt.setDouble(++counter,oosSummary.modalQtyPerVisit);
		}
		else{
			pstmt.setNull(++counter,Types.DOUBLE);
		}
		
		if( oosSummary.modal2QtyPerVisit > 0){
		pstmt.setDouble(++counter,oosSummary.modal2QtyPerVisit);
		}
		else{
			pstmt.setNull(++counter,Types.DOUBLE);
		}

		pstmt.setDouble(++counter,oosSummary.maxQtyPerVisit);
		pstmt.setDouble(++counter,NumberUtil.RoundFloat(oosSummary.movVisitRatio,4));
		pstmt.setDouble(++counter,NumberUtil.RoundFloat(oosSummary.unitRegPrice,2));
		pstmt.setDouble(++counter,NumberUtil.RoundFloat(oosSummary.unitSalePrice,2));
		
		if( oosSummary.marginOpp  > 0){
		pstmt.setDouble(++counter,oosSummary.marginOpp );
		}
		else{
			pstmt.setNull(++counter,Types.DOUBLE);
		}
		if( oosSummary.revOpp  > 0){
		pstmt.setDouble(++counter,oosSummary.revOpp );
		}
		else{
			pstmt.setNull(++counter,Types.DOUBLE);
		}

		pstmt.setInt(++counter,oosSummary.totalCustVisitCount);	
		
		if(allslotsWithMovement){
			pstmt.setString(++counter,"N");	
			pstmt.setInt(++counter, 0);
			pstmt.setInt(++counter, 0);
		}else{
			pstmt.setString(++counter,oosSummary.ossInd);	
			pstmt.setInt(++counter, oosSummary.ossClassification);
			pstmt.setInt(++counter, oosSummary.ossScore);
			
		}
		

		pstmt.setDouble(++counter,oosSummary.last7dayexp* oosSummary.totalCustVisitCount);
		pstmt.setDouble(++counter,oosSummary.last7dayobs);
		pstmt.setDouble(++counter,oosSummary.last7dayOused);
		pstmt.setDouble(++counter,oosSummary.last28dayexp* oosSummary.totalCustVisitCount);
		pstmt.setDouble(++counter,oosSummary.last28dayobs);
		pstmt.setDouble(++counter,oosSummary.last28dayOused);
		pstmt.setDouble(++counter,oosSummary.last180dayexp* oosSummary.totalCustVisitCount);
		pstmt.setDouble(++counter,oosSummary.last180dayobs);
		pstmt.setDouble(++counter,oosSummary.last180dayOused);
		pstmt.setDouble(++counter,oosSummary.last7dayavgexp* oosSummary.totalCustVisitCount);
		pstmt.setDouble(++counter,oosSummary.last7dayavgobs);
		pstmt.setDouble(++counter,oosSummary.prevdayexp * oosSummary.totalCustVisitCount);
		
		

		if( oosSummary.oosMethod  !=null ){
		pstmt.setString(++counter,oosSummary.oosMethod);
		}
		else{
			pstmt.setNull(++counter,Types.VARCHAR);
		}

		pstmt.setInt(++counter,oosSummary.last7dayDaysMoved);
		pstmt.setInt(++counter,oosSummary.last28dayDaysMoved);
		pstmt.setInt(++counter,oosSummary.last180dayDaysMoved);

		double mape = oosSummary.mape;
		pstmt.setDouble(++counter,mape);

		double sigma = oosSummary.sigmaUsed * oosSummary.totalCustVisitCount;
		pstmt.setDouble(++counter,sigma);
		pstmt.setString(++counter,oosSummary.moved2PlusInAllDaysInPrevNdays);	

		pstmt.setDouble(++counter,oosSummary.expWithAd * oosSummary.totalCustVisitCount);
		pstmt.setDouble(++counter,oosSummary.sigmaWithAd * oosSummary.totalCustVisitCount);

		pstmt.setString(++counter,oosSummary.expMethodWithAd);

		if( oosSummary.predictedExp != null && oosSummary.predictedExp > 0){
			pstmt.setDouble(++counter,oosSummary.predictedExp);
		}
		else{
			pstmt.setNull(++counter,Types.DOUBLE);
		}

		pstmt.setString(++counter,oosSummary.focusItem);	
		if( oosSummary.lastMovementTS  !=null ){
		pstmt.setString(++counter,oosSummary.lastMovementTS);
		}
		else{
			pstmt.setNull(++counter,Types.VARCHAR);
		}

		
		pstmt.addBatch();

		}
		
		logger.debug("Begin Execute insert into db");
    	int[] c = pstmt.executeBatch();
    	PristineDBUtil.commitTransaction(conn, "OOS Summary Insert");
    	pstmt.clearBatch();
    	pstmt.close();
		logger.debug("End Execute insert into db");
		if(c!=null && c.length>0){
		logger.debug("The number of records inserted: "+c[0]);
		}else{
			logger.debug("The number of records inserted:0");

		}
		
		}catch(Exception e){
			logger.error("Exception inserting OOS Summary :"+e.getMessage());
			logger.error("Item Code:"+oosSummaryList.get(0).itemCode);
			e.printStackTrace();
			
		}
	}
	
	public void deleteMovementExpectation(Connection conn, int strId, int calendarId,  int dow) throws GeneralException {
		StringBuffer sb = new StringBuffer();
		sb.append(" delete from PREDICTED_MOVEMENT WHERE COMP_STR_ID = ").append(strId);
//		sb.append(" AND ITEM_CODE = "+itemCode);  
		sb.append(" AND CALENDAR_ID = "+calendarId);  
		sb.append(" AND DOW_ID = "+dow);  
		
		PristineDBUtil.execute(conn, sb, "deleteExpectation");
		
	}


	public void insertMovementExpectation(Connection conn, List<OOSSummaryDTO> oosSummaryList, int strId, int calendarWeekId, int dow) throws GeneralException {

		PreparedStatement pstmt= null;
		logger.debug("Begin insert into db");

		
		StringBuffer sb = new StringBuffer();
		
		sb.append(" INSERT INTO PREDICTED_MOVEMENT ( ");
		sb.append(" COMP_STR_ID, CALENDAR_ID, DOW_ID,TOD_ID, " );
		sb.append(" ITEM_CODE,  ");
		sb.append("LAST_7DAY_EXP,LAST_7DAY_OBS,LAST_7DAY_OBS_USED," +
				"LAST_28DAY_EXP,LAST_28DAY_OBS,LAST_28DAY_OBS_USED," +
				"LAST_180DAY_EXP,LAST_180DAY_OBS,LAST_180DAY_OBS_USED," +
				"LAST_7DAY_AVG_EXP,LAST_7DAY_AVG_OBS,LAST_7DAY_AVG_OBS_USED," +
				"PREV_DAY_EXP, OOS_METHOD, " +
				" LAST_7DAY_DAYS_MOVED,LAST_28DAY_DAYS_MOVED,LAST_180DAY_DAYS_MOVED, MAPE,SIGMA_USED, AD_FACTOR_EXP,AD_FACTOR_SIGMA,AD_FACTOR_METHOD) ");
		sb.append(" VALUES " );
		sb.append(" (?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?) ");

		try{
        pstmt=conn.prepareStatement(sb.toString());

		
		for(OOSSummaryDTO oosSummary: oosSummaryList){

		int counter=0;	
		pstmt.setInt(++counter,strId);	
		pstmt.setInt(++counter,calendarWeekId);	
		pstmt.setInt(++counter,dow);	
		pstmt.setInt(++counter,oosSummary.timeOfDayId);	

		pstmt.setInt(++counter,oosSummary.itemCode);	

		

		pstmt.setDouble(++counter,oosSummary.last7dayexp);
		pstmt.setDouble(++counter,oosSummary.last7dayobs);
		pstmt.setDouble(++counter,oosSummary.last7dayOused);
		
		pstmt.setDouble(++counter,oosSummary.last28dayexp);
		pstmt.setDouble(++counter,oosSummary.last28dayobs);
		pstmt.setDouble(++counter,oosSummary.last28dayOused);
		
		pstmt.setDouble(++counter,oosSummary.last180dayexp);
		pstmt.setDouble(++counter,oosSummary.last180dayobs);
		pstmt.setDouble(++counter,oosSummary.last180dayOused);
		

		pstmt.setDouble(++counter,oosSummary.last7dayavgexp);
		pstmt.setDouble(++counter,oosSummary.last7dayavgobs);
		pstmt.setDouble(++counter,oosSummary.last7dayavgOused);
		
		pstmt.setDouble(++counter,oosSummary.prevdayexp );
		
		

		if( oosSummary.oosMethod  !=null ){
		pstmt.setString(++counter,oosSummary.oosMethod);
		}
		else{
			pstmt.setNull(++counter,Types.VARCHAR);
		}

		pstmt.setInt(++counter,oosSummary.last7dayDaysMoved);
		pstmt.setInt(++counter,oosSummary.last28dayDaysMoved);
		pstmt.setInt(++counter,oosSummary.last180dayDaysMoved);

		double mape = oosSummary.mape;
		pstmt.setDouble(++counter,mape);

		double sigma = oosSummary.sigmaUsed;
		pstmt.setDouble(++counter,sigma);
		
		pstmt.setDouble(++counter,oosSummary.expWithAd );
		pstmt.setDouble(++counter,oosSummary.sigmaWithAd);

		if( oosSummary.expMethodWithAd  !=null ){
		pstmt.setString(++counter,oosSummary.expMethodWithAd);
		}
		else{
			pstmt.setNull(++counter,Types.VARCHAR);
		}

		
		pstmt.addBatch();

		}
		
		logger.debug("Begin Execute insert into db");
    	int[] c = pstmt.executeBatch();
    	PristineDBUtil.commitTransaction(conn, "OOS Summary Insert");
    	pstmt.clearBatch();
    	pstmt.close();
		logger.debug("End Execute insert into db");
		if(c!=null && c.length>0){
		logger.debug("The number of records inserted: "+c[0]);
		}else{
			logger.debug("The number of records inserted:0");

		}
		
		}catch(Exception e){
			logger.error("Exception inserting Expectation :"+e.getMessage());
			logger.error("Item Code:"+oosSummaryList.get(0).itemCode);
			e.printStackTrace();
			
		}
	}

	public void deleteOSSCalcs(Connection conn, int scheduleId, int dow) throws GeneralException {
		StringBuffer sb = new StringBuffer();
		sb.append(" delete from SUMMARY_DAILY_MOV WHERE SCHEDULE_ID = ").append(scheduleId);
		sb.append(" AND DOW_ID = ").append(dow);
//		sb.append(" AND ITEM_CODE = 893172");  // DEBUG
		
		PristineDBUtil.execute(conn, sb, "deleteOOSDataForSchedule");
		
	}

	public OOSSummaryDTO getOOSItemStats(Connection conn, String schIdList,
			int itemCode, int dayofweekId, int timeOfDayId, boolean onSale, int totalNoOfObs) throws GeneralException, SQLException {
		StringBuffer sb = new StringBuffer();
		sb.append(" SELECT  SCHEDULE_ID, ");
		sb.append(" QUANTITY_TOTAL QUANTITY_TOTAL, ");
		sb.append(" QUANTITY_REG QUANTITY_REG, ");
		sb.append(" QUANTITY_REG QUANTITY_REG_MIN, "); //Earlier Min/Max functions used, hence the columns are repeated
		sb.append(" QUANTITY_REG QUANTITY_REG_MAX, ");
		sb.append(" QUANTITY_SALE QUANTITY_SALE, ");
		sb.append(" QUANTITY_SALE QUANTITY_SALE_MIN, ");
		sb.append(" QUANTITY_SALE QUANTITY_SALE_MAX, ");
		sb.append(" REV_TOTAL REV_TOTAL, ");
		sb.append(" REV_REG REV_REG, ");
		sb.append(" REV_SALE REV_SALE, ");
		sb.append(" MARGIN, ");
		sb.append(" PROMOTIONAL_IND, "); 
		sb.append(" ROUND(VISIT_COUNT,0) VISIT_COUNT, ");
		sb.append(" MOV_VISIT_RATIO, "); 
		sb.append(" ROUND(MAX_DURATION,0) MAX_DURATION, ");
		sb.append(" ROUND(MIN_DURATION,0) MIN_DURATION, ");
		sb.append(" ROUND(AVG_DURATION,0) AVG_DURATION, ");
		sb.append(" ROUND(MAX_DURATION_95PCT,0) MAX_DURATION_95PCT, ");
		sb.append(" ROUND(MIN_DURATION_95PCT,0) MIN_DURATION_95PCT, ");
		sb.append(" AVG_QTY_VISIT AVG_QTY_VISIT, ");
		sb.append(" MODAL_QTY_VISIT MODAL_QTY_VISIT, ");
		sb.append(" MODAL_QTY2_VISIT MODAL_QTY2_VISIT, ");
		sb.append(" TOTAL_VISIT_COUNT, ");
		//sb.append(" COUNT(A.ITEM_CODE) OBS_COUNT,  ");
		sb.append(" A.ITEM_CODE, ");
		sb.append(" B.ITEM_NAME || ' ' || ITEM_SIZE || UOM_NAME ITEM_NAME, ");
		sb.append(" B.RETAILER_UPC, ");
		sb.append(" B.RETAILER_ITEM_CODE, ");
		sb.append(" B.DEPT_NAME, ");
		sb.append(" B.CAT_NAME ");
		sb.append(" FROM SUMMARY_DAILY_MOV A, ITEM_LOOKUP_VIEW B WHERE ");
		sb.append(" SCHEDULE_ID in (").append(schIdList).append(")") ;
		sb.append(" AND A.ITEM_CODE = ").append(itemCode) ;
		sb.append(" AND A.DOW_ID = ").append(dayofweekId) ;
		if( timeOfDayId > 0 )
			sb.append(" AND A.TOD_ID = ").append(timeOfDayId) ;
		sb.append(" AND B.ITEM_CODE = A.ITEM_CODE ");
		sb.append(" AND TOTAL_VISIT_COUNT > 0 ");
		
		if( onSale ) {
			sb.append(" AND A.PROMOTIONAL_IND = 'Y'");
			sb.append(" ORDER BY A.QUANTITY_SALE ");
		}
		else{
			sb.append(" AND A.PROMOTIONAL_IND = 'N'");
			sb.append(" ORDER BY A.QUANTITY_REG ");
		}
		
		//sb.append(" GROUP BY A.ITEM_CODE, B.ITEM_NAME, B.DEPT_NAME, B.CAT_NAME ");
		
		
		CachedRowSet result = PristineDBUtil.executeQuery(conn, sb, "getCompDataForItem");
		
		OOSSummaryDTO oosSummary = null;
		
		int recCount =0;
		int []qtyRegArray = null;
		int []qtySaleArray = null;
		
		ArrayList <Integer> schList = new ArrayList <Integer> ();
		float[] movVisitRatioRegArray = null;
		float[] movVisitRatioSaleArray = null;
		float movVisitRatioRegTotal = 0f;
		float movVisitRatioSaleTotal = 0f;
		
		float[] modalQtyRegArray = null;
		int countModalReg = 0;
		float[] modalQtySaleArray = null;
		int countModalSale = 0;
		int outlierCount = 0;
		
		if( result.size() > totalNoOfObs){
			totalNoOfObs = result.size();
		}
		
		boolean removeLowerEnd  = false;
		boolean removeHigherEnd  = false;
		if( totalNoOfObs > 10 && !onSale) {
			if (result.size()>= totalNoOfObs){
				removeLowerEnd = true;
				outlierCount++;
			}
			outlierCount++;
			removeHigherEnd = true;
			totalNoOfObs = totalNoOfObs - 2;
		}
		while( result.next()) {
			recCount++;
			//skip the First and Last record to not skew the average (only for regular)
			//Commented for new analysis
			if( removeLowerEnd && recCount == 1){
				continue;
			}
			if( removeHigherEnd && recCount == result.size())
				continue;

			
			
			if( oosSummary == null){
				
				
				oosSummary = new OOSSummaryDTO ();
				
				//Commented for new analysis
				int qtyArraySize =  result.size()-outlierCount;
				qtyRegArray = new int[qtyArraySize];
				qtySaleArray = new int[qtyArraySize];

				movVisitRatioRegArray = new float[qtyArraySize];
				movVisitRatioSaleArray = new float[qtyArraySize];
				
				modalQtyRegArray = new float[qtyArraySize];
				modalQtySaleArray= new float[qtyArraySize];
				
				oosSummary.dayofweekId = dayofweekId;
				oosSummary.timeOfDayId = timeOfDayId;
				//Note - minimum will be the initial record after skipping since the record is sorted on qty
				/* Commented for new analysis
				oosSummary.minRegQty = result.getFloat("QUANTITY_REG_MIN");;
				oosSummary.minSaleQty = result.getFloat("QUANTITY_SALE_MIN");
				*/
			}
			
			
			boolean saleRec = result.getString("PROMOTIONAL_IND").equals("Y")? true:false;

			/*
			int scheduleId = result.getInt("SCHEDULE_ID");
			if ( !schList.contains( scheduleId))
				schList.add(scheduleId);
			*/
			oosSummary.itemCode = result.getInt("ITEM_CODE");
			oosSummary.totalQty += result.getFloat("QUANTITY_TOTAL");
			
			if( !saleRec)
				oosSummary.regQty += result.getFloat("QUANTITY_REG");
			else
				oosSummary.saleQty += result.getFloat("QUANTITY_SALE");
			
			oosSummary.totalRev += result.getDouble("REV_TOTAL");
			
			if( !saleRec)
				oosSummary.regRev += result.getDouble("REV_REG");
			else
				oosSummary.saleRev += result.getDouble("REV_SALE");
			if( !saleRec)
				oosSummary.regVisitCount += result.getInt("VISIT_COUNT");
			else
				oosSummary.saleVisitCount += result.getInt("VISIT_COUNT");
			
			oosSummary.totalCustVisitCount +=result.getInt("TOTAL_VISIT_COUNT");
			if( !saleRec)
				oosSummary.regMargin+= result.getFloat("MARGIN");
			else
				oosSummary.saleMargin += result.getFloat("MARGIN");

			
			if( !saleRec)
				oosSummary.regMovVisitRatio+= result.getFloat("MOV_VISIT_RATIO");
			else
				oosSummary.saleMovVisitRatio+= result.getFloat("MOV_VISIT_RATIO");

			
			oosSummary.maxDurationBtwVisit += result.getInt("MAX_DURATION");
			oosSummary.minDurationBtwVisit += result.getInt("MIN_DURATION");
			oosSummary.avgDurationBtwVisit += result.getInt("AVG_DURATION");
			//oosSummary.max95PctDurationBtwVisit = result.getInt("MAX_DURATION_95PCT");
			//oosSummary.min95PctDurationBtwVisit =  result.getInt("MIN_DURATION_95PCT");
			float modalQty = result.getFloat("MODAL_QTY_VISIT");
			if (modalQty > 0){
				if( !saleRec){
					modalQtyRegArray[countModalReg] = modalQty;
					countModalReg++;
				}
				else{
					modalQtyRegArray[countModalSale] = modalQty;
					countModalSale++;
				}
			}
			oosSummary.avgQtyPerVisit += result.getFloat("AVG_QTY_VISIT");
			

			//Note - Maximum will be the last record since the record is sorted ascending
			/* Commented for new analysis
			oosSummary.maxRegQty = result.getFloat("QUANTITY_REG_MAX");
			oosSummary.maxSaleQty = result.getFloat("QUANTITY_SALE_MAX");
			*/
			oosSummary.deptName= result.getString("DEPT_NAME");
			oosSummary.catName= result.getString("CAT_NAME");
			oosSummary.itemName= result.getString("ITEM_NAME");
			oosSummary.upc = result.getString("RETAILER_UPC");
			oosSummary.retailerItemCode = result.getString("RETAILER_ITEM_CODE");

			if( !saleRec){
				qtyRegArray[oosSummary.noOfObs] = (int)result.getFloat("QUANTITY_REG");
				movVisitRatioRegArray[oosSummary.noOfObs] = result.getFloat("MOV_VISIT_RATIO"); 
				movVisitRatioRegTotal = movVisitRatioRegTotal + result.getFloat("MOV_VISIT_RATIO");
				oosSummary.noOfObs++;
				
			}else{
				qtySaleArray[oosSummary.noOfSaleObs] = (int)result.getFloat("QUANTITY_SALE");
				movVisitRatioSaleArray [oosSummary.noOfSaleObs]= result.getFloat("MOV_VISIT_RATIO");
				movVisitRatioSaleTotal = movVisitRatioSaleTotal + result.getFloat("MOV_VISIT_RATIO");
				oosSummary.noOfSaleObs++;
			}

		}
		
		if( oosSummary != null){
			
			int totalObs = oosSummary.noOfObs + oosSummary.noOfSaleObs;
			
			logger.debug("No of zero observations = " + (totalNoOfObs - totalObs));
			totalObs =  totalNoOfObs;
			if( totalObs < Constants.MIN_RECS_FOR_OOS_ANALYSIS) return null;

			oosSummary.totalQty = oosSummary.totalQty/totalObs;
			oosSummary.totalRev = oosSummary.totalRev/totalObs;
			oosSummary.maxDurationBtwVisit = oosSummary.maxDurationBtwVisit/totalObs;
			oosSummary.minDurationBtwVisit = oosSummary.minDurationBtwVisit/totalObs; 
			oosSummary.avgDurationBtwVisit = oosSummary.avgDurationBtwVisit/totalObs; 
			oosSummary.avgQtyPerVisit = oosSummary.avgQtyPerVisit/totalObs;
			int d = recCount;
			if(removeLowerEnd){
				d--;
			}
			if(removeHigherEnd){
				d--;
			}
			oosSummary.totalCustVisitCount = oosSummary.totalCustVisitCount/d;        //totalObs;
			//Regular values
			if( oosSummary.noOfObs >= Constants.MIN_RECS_FOR_OOS_ANALYSIS){
				int noOfObsWithRegMovement = oosSummary.noOfObs;  
				oosSummary.noOfObs = totalObs; //Actual no of observation considering 0 movement
				oosSummary.regQty = oosSummary.regQty/oosSummary.noOfObs;
				oosSummary.regRev = oosSummary.regRev/oosSummary.noOfObs;
				oosSummary.regVisitCount = (int)NumberUtil.RoundFloat((float)oosSummary.regVisitCount/oosSummary.noOfObs, 0);
				oosSummary.modalQtyPerVisit = calculateModalQty(modalQtyRegArray,countModalReg);
				oosSummary.regMargin = oosSummary.regMargin/oosSummary.noOfObs;
				oosSummary.regMovVisitRatio = movVisitRatioRegTotal/oosSummary.noOfObs;
//				oosSummary.regMovVisitRatio = calculateCutOffRatio( movVisitRatioRegArray, oosSummary.noOfObs);
				//oosSummary.regMovVisitRatio = oosSummary.regQty /oosSummary.totalCustVisitCount ;
			}else{
				oosSummary.regQty = 0;
				oosSummary.regRev = 0;
				oosSummary.regVisitCount = 0;
				oosSummary.modalQtyPerVisit = 0;
				oosSummary.regMargin = 0;
				oosSummary.regMovVisitRatio = 0;
				oosSummary.noOfObs = 0;
			}
				

			if( oosSummary.noOfSaleObs >= Constants.MIN_RECS_FOR_OOS_ANALYSIS){
				oosSummary.noOfSaleObs = totalObs; //Actual no of observation consider 0 movement
				oosSummary.saleQty = oosSummary.saleQty/oosSummary.noOfSaleObs;
				oosSummary.saleRev = oosSummary.saleRev/oosSummary.noOfSaleObs;
				oosSummary.saleVisitCount = (int)NumberUtil.RoundFloat((float)oosSummary.saleVisitCount/oosSummary.noOfSaleObs,0);
				oosSummary.modal2QtyPerVisit = calculateModalQty(modalQtySaleArray,countModalSale);
				oosSummary.saleMargin = oosSummary.saleMargin/oosSummary.noOfSaleObs;
				oosSummary.saleMovVisitRatio = movVisitRatioSaleTotal/oosSummary.noOfSaleObs;
//				oosSummary.saleMovVisitRatio = calculateCutOffRatio( movVisitRatioSaleArray, oosSummary.noOfSaleObs);
				//oosSummary.saleMovVisitRatio = oosSummary.saleQty/oosSummary.totalCustVisitCount;

			}else{
				oosSummary.saleQty = 0;
				oosSummary.saleRev = 0;
				oosSummary.saleVisitCount = 0;
				oosSummary.modal2QtyPerVisit = 0;
				oosSummary.saleMargin = 0;
				oosSummary.saleMovVisitRatio = 0;
				oosSummary.noOfSaleObs = 0;
			}
			//oosSummary.max95PctDurationBtwVisit = result.getInt("MAX_DURATION_95PCT");
			//oosSummary.min95PctDurationBtwVisit =  result.getInt("MIN_DURATION_95PCT");
			
			/*
			int []modeArr = NumberUtil.mode(qtyArray);
			oosSummary.qtyMode = modeArr[0];
			*/
		}
		return oosSummary ;
	}
	
	
	
	
	
	private float calculateModalQty(float[] modalQtyArray, int noOfObs) {
		// TODO Auto-generated method stub
		int [] myModalQtyArray = new int [noOfObs] ;
		for ( int i = 0; i < noOfObs; i++)
			myModalQtyArray[i] = (int)modalQtyArray[i];
		int mode = NumberUtil.mode(myModalQtyArray)[0];
		if( mode < 0 ) mode = 0;
		return mode;
	}

	private float calculateCutOffRatio(float[] movVisitRatioArray,
			int noOfObs) {
			 
		float [] myMovVisitArray = new float [noOfObs] ;
		//Zero movement filling
		for ( int i = 0; i < noOfObs - movVisitRatioArray.length; i++)
			myMovVisitArray[i] = 0;
		int j = 0;
		for ( int i = noOfObs - movVisitRatioArray.length; i < noOfObs; i++){
			myMovVisitArray[i] = movVisitRatioArray[j];
			j++;
		}
		java.util.Arrays.sort(myMovVisitArray);
		
		int cutOffVal = (int)NumberUtil.RoundFloat((float)(noOfObs * 0.6),0);
		return myMovVisitArray[cutOffVal-1];
		
	}



	public void deleteExpectations(Connection conn, int strId) throws GeneralException {
		// TODO Auto-generated method stub
		StringBuffer sb = new StringBuffer();
		sb.append(" delete from SUMMARY_DAILY_MOV_EXP WHERE STORE_ID = ").append(strId);
//		sb.append(" AND ITEM_CODE = 838385");  // DEBUG
		
		PristineDBUtil.execute(conn, sb, "deleteExpectations");
		
	}

	public void insertMovementExp(Connection conn, List<OOSSummaryDTO> sbL) throws GeneralException {
		
		PreparedStatement pstmt= null;

		StringBuffer sb = new StringBuffer();
		sb.append(" INSERT INTO SUMMARY_DAILY_MOV_EXP ( ");
		sb.append(" STORE_ID, DOW_ID, TOD_ID, ITEM_CODE, ");
		sb.append(" EXP_QUANTITY_REG, EXP_REV_REG, EXP_REG_VISIT_COUNT, OOS_MOV_VISIT_RATIO_REG, EXP_REG_MARGIN,");
		sb.append("   NO_OF_REG_OBS, EXP_MODAL_QTY_VISIT, ");
		sb.append(" EXP_QUANTITY_SALE, EXP_REV_SALE, EXP_SALE_VISIT_COUNT, OOS_MOV_VISIT_RATIO_SALE, EXP_SALE_MARGIN, "); 
		sb.append(" NO_OF_SALE_OBS, EXP_MODAL_QTY2_VISIT, ");
		sb.append("  EXP_MAX_DURATION, EXP_MIN_DURATION, EXP_AVG_DURATION, EXP_AVG_QTY_VISIT, EXP_TOTAL_VISIT_COUNT  ");
		sb.append("  ) VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)" ); 	
		
		try{
	        pstmt=conn.prepareStatement(sb.toString());
	        for(OOSSummaryDTO oosSummaryBase:sbL){
			
	        	int counter=0;	
	        	pstmt.setInt(++counter, oosSummaryBase.storeId);
	        	pstmt.setInt(++counter, oosSummaryBase.dayofweekId);
	        	pstmt.setInt(++counter, oosSummaryBase.timeOfDayId);
	        	pstmt.setInt(++counter, oosSummaryBase.itemCode);
	        	
		if( oosSummaryBase.noOfObs > 0){
        	pstmt.setDouble(++counter, oosSummaryBase.regQty);
        	pstmt.setDouble(++counter, oosSummaryBase.regRev);
        	pstmt.setDouble(++counter, oosSummaryBase.regVisitCount);
        	pstmt.setDouble(++counter, oosSummaryBase.regMovVisitRatio);
        	pstmt.setDouble(++counter, oosSummaryBase.regMargin);
        	pstmt.setInt(++counter, oosSummaryBase.noOfObs);
			if( oosSummaryBase.modalQtyPerVisit > 0){
	        	pstmt.setDouble(++counter, oosSummaryBase.modalQtyPerVisit);
			}
			else{
				pstmt.setNull(++counter,Types.DOUBLE);
			}
			
		}else{
			pstmt.setNull(++counter,Types.DOUBLE);
			pstmt.setNull(++counter,Types.DOUBLE);
			pstmt.setNull(++counter,Types.DOUBLE);
			pstmt.setNull(++counter,Types.DOUBLE);
			pstmt.setNull(++counter,Types.DOUBLE);
			pstmt.setNull(++counter,Types.INTEGER);
			pstmt.setNull(++counter,Types.DOUBLE);
		}
		if( oosSummaryBase.noOfSaleObs > 0){
        	pstmt.setDouble(++counter, oosSummaryBase.saleQty);
        	pstmt.setDouble(++counter, oosSummaryBase.saleRev);
        	pstmt.setDouble(++counter, oosSummaryBase.saleVisitCount);
        	pstmt.setDouble(++counter, oosSummaryBase.saleMovVisitRatio);
        	pstmt.setDouble(++counter, oosSummaryBase.saleMargin);
        	pstmt.setInt(++counter, oosSummaryBase.noOfSaleObs);
			if( oosSummaryBase.modal2QtyPerVisit > 0){
	        	pstmt.setDouble(++counter, oosSummaryBase.modal2QtyPerVisit);
			}
			else{
				pstmt.setNull(++counter,Types.DOUBLE);
			}
			
		}else{
			pstmt.setNull(++counter,Types.DOUBLE);
			pstmt.setNull(++counter,Types.DOUBLE);
			pstmt.setNull(++counter,Types.DOUBLE);
			pstmt.setNull(++counter,Types.DOUBLE);
			pstmt.setNull(++counter,Types.DOUBLE);
			pstmt.setNull(++counter,Types.INTEGER);
			pstmt.setNull(++counter,Types.DOUBLE);
		}
		if( (oosSummaryBase.noOfObs + oosSummaryBase.noOfSaleObs) > 0){
        	pstmt.setInt(++counter, oosSummaryBase.maxDurationBtwVisit);
        	pstmt.setInt(++counter, oosSummaryBase.minDurationBtwVisit);
        	pstmt.setDouble(++counter, oosSummaryBase.avgDurationBtwVisit);
        	pstmt.setDouble(++counter, oosSummaryBase.avgQtyPerVisit);
        	pstmt.setInt(++counter, oosSummaryBase.totalCustVisitCount);
					
		}else{
			pstmt.setNull(++counter,Types.INTEGER);
			pstmt.setNull(++counter,Types.INTEGER);
			pstmt.setNull(++counter,Types.DOUBLE);
			pstmt.setNull(++counter,Types.DOUBLE);
			pstmt.setNull(++counter,Types.INTEGER);
		}
		
			pstmt.addBatch();
		}
		logger.debug("Begin Execute insert into db");
    	int[] c = pstmt.executeBatch();
    	PristineDBUtil.commitTransaction(conn, "Move Expectation Insert");
    	pstmt.clearBatch();
    	pstmt.close();
		logger.debug("End Execute insert into db");
		if(c!=null && c.length>0){
		logger.debug("The number of records inserted: "+c[0]);
		}else{
			logger.debug("The number of records inserted:0");

		}
		
		}catch(Exception e){
			logger.error("Exception inserting Move Expectation :"+e.getMessage());
			e.printStackTrace();
		}

	}


	
	
	public int updateExpectationsWithTotalVisits(Connection conn, int storeId) throws GeneralException {
		for(int i = 1;i<=7;i++){
		StringBuffer buffer = new StringBuffer();
		buffer.append(" update summary_daily_mov_exp e set exp_total_visit_count = " +
				"(select avg(total_visit_count) " +
				"from (select distinct total_visit_count, s.schedule_id, dow_id, tod_id  " +
				"from summary_daily_mov sd, schedule s where s.comp_str_id = "+storeId+" and s.schedule_id = sd.schedule_id ) ssd " +
				"where e.dow_id = ssd.dow_id  and e.tod_id = ssd.tod_id ) " +
				"where e.dow_id = "+i+" and e.store_id = "+storeId);
		
		PristineDBUtil.executeUpdate(conn, buffer, "updateExpectationsWithTotalVisits");
		}
		PristineDBUtil.commitTransaction(conn, "Update Expectations with Total Visits Commited");
		return 1;
		
	}

	
	public int getVisitCountOLDNEW(Connection conn, int storeId, String startPeriod, String endPeriod, 
			String excludeDeptId, boolean isArchive, String analysisDate) throws GeneralException {
		
		int visitCount = 0;
		StringBuffer buffer = new StringBuffer();
		buffer.append(" SELECT count (DISTINCT TRX_NO) from (Select * ");
		if( isArchive )
			buffer.append(" FROM MOVEMENT_DAILY_ARCHIVE A, ");
		else
			buffer.append(" FROM Transaction_log A, item_lookup IL,");
		
		buffer.append("retail_calendar rc");
		buffer.append(" WHERE A.STORE_ID = ").append(storeId).append("");
		buffer.append(" and A.item_id = IL.item_code and A.calendar_id = rc.calendar_id	and (rc.start_date >= TO_DATE('"+analysisDate+"','MM/DD/YYYY')) and (rc.start_date <= (TO_DATE('"+analysisDate+"','MM/DD/YYYY')+1))");
		if( excludeDeptId != null && !excludeDeptId.equals(""))
			buffer.append(" AND IL.DEPT_ID NOT IN (").append(excludeDeptId).append(")");

		buffer.append(")");
		
		buffer.append(" where TRX_TIME >=").append( "TO_DATE('").append(startPeriod).append("','MM/DD/YYYY HH:MI AM')");
		buffer.append(" AND TRX_TIME <=").append( "TO_DATE('").append(endPeriod).append("','MM/DD/YYYY HH:MI AM')");
		logger.debug( buffer.toString());
		
		String result = PristineDBUtil.getSingleColumnVal(conn, buffer, "getVisitCount");
		visitCount = Integer.parseInt(result);
		return visitCount;
	}
	
	
	public HashMap<Integer,Integer> getVisitCount(Connection conn, int storeId, String startPeriod, String endPeriod, 
			String excludeDeptId, boolean isArchive, String analysisDate, int calendarId) throws GeneralException, Exception {
		
		StringBuffer buffer = new StringBuffer();
		
		buffer.append("	  SELECT ts, count (TRX_NO) vc from (Select distinct TRX_NO, " +
				"case when TRX_TIME <= TO_DATE('"+analysisDate+" 8:00 AM','MM/DD/YYYY HH:MI AM') then 11 " +
				"when TRX_TIME > TO_DATE('"+analysisDate+" 8:00 AM','MM/DD/YYYY HH:MI AM') " +
				"AND TRX_TIME <=TO_DATE('"+analysisDate+" 11:00 AM','MM/DD/YYYY HH:MI AM') then 5 " +
				"when TRX_TIME > TO_DATE('"+analysisDate+" 11:00 AM','MM/DD/YYYY HH:MI AM') " +
				"AND TRX_TIME <=TO_DATE('"+analysisDate+" 2:00 PM','MM/DD/YYYY HH:MI AM') then 6 " +
				"when TRX_TIME > TO_DATE('"+analysisDate+" 2:00 AM','MM/DD/YYYY HH:MI AM') " +
				"AND TRX_TIME <=TO_DATE('"+analysisDate+" 4:00 PM','MM/DD/YYYY HH:MI AM') then 7 " +
				"when TRX_TIME > TO_DATE('"+analysisDate+" 4:00 AM','MM/DD/YYYY HH:MI AM') " +
				"AND TRX_TIME <=TO_DATE('"+analysisDate+" 6:00 PM','MM/DD/YYYY HH:MI AM') then 8 " +
				"when TRX_TIME > TO_DATE('"+analysisDate+" 6:00 AM','MM/DD/YYYY HH:MI AM') " +
				"AND TRX_TIME <=TO_DATE('"+analysisDate+" 9:00 PM','MM/DD/YYYY HH:MI AM') then 9 " +
				"when TRX_TIME > TO_DATE('"+analysisDate+" 9:00 AM','MM/DD/YYYY HH:MI AM') then 10 " +
				"end ts FROM " +
				"Transaction_log A, item_lookup IL,retail_calendar rc " +
				"WHERE A.STORE_ID = "+storeId+" and A.item_id = IL.item_code and A.calendar_id = rc.calendar_id and A.calendar_id = "+calendarId );
		if( excludeDeptId != null && !excludeDeptId.equals(""))
			buffer.append(" AND IL.DEPT_ID NOT IN (").append(excludeDeptId).append(")");

				buffer.append(") group by ts");
		
		
		logger.debug( buffer.toString());
		HashMap<Integer,Integer> visitL = new HashMap<Integer,Integer>();
		CachedRowSet movement = PristineDBUtil.executeQuery(conn, buffer,
				"getVisitCount");
		Integer tvc = 0;
		while(movement.next()){
			Integer ts = movement.getInt(1);
			Integer vc = movement.getInt(2);
			visitL.put(ts, vc);
			tvc = tvc+ vc;
		}
		visitL.put(4, tvc);
		return visitL;
	}

	/*
	 
	  
	  SELECT ts, count (TRX_NO) vc from (Select distinct TRX_NO,  
case when TRX_TIME >=TO_DATE('08/05/2013 11:00 AM','MM/DD/YYYY HH:MI AM') AND TRX_TIME <=TO_DATE('08/05/2013 2:00 PM','MM/DD/YYYY HH:MI AM') then 6
else -1 end ts
FROM Transaction_log A, item_lookup IL,retail_calendar rc
WHERE A.STORE_ID = 5713 and A.item_id = IL.item_code and A.calendar_id = rc.calendar_id 
and A.calendar_id = 3526
AND IL.DEPT_ID NOT IN (21,34,37)) 
group by ts

	  
	  
	 */

	
	/*
	
		public int getVisitCount(Connection conn, String storeNum, String startPeriod, String endPeriod, 
			String excludeDeptId, boolean isArchive, String analysisDate) throws GeneralException {
		
		int visitCount = 0;
		StringBuffer buffer = new StringBuffer();
		buffer.append(" SELECT count (DISTINCT TRANSACTION_NO) from (Select * ");
		if( isArchive )
			buffer.append(" FROM MOVEMENT_DAILY_ARCHIVE A, ");
		else
			buffer.append(" FROM "+TABLE_NAME_MOVEMENT_DAILY+" A, ");
		
		buffer.append(" ITEM_LOOKUP B,retail_calendar rc");
		buffer.append(" WHERE A.COMP_STR_NO = '").append(storeNum).append("'");
		buffer.append("and A.calendar_id = rc.calendar_id	and (rc.start_date >= TO_DATE('"+analysisDate+"','MM/DD/YYYY')) and (rc.start_date <= (TO_DATE('"+analysisDate+"','MM/DD/YYYY')+1))");
//		buffer.append(" AND SUBSTR(B.UPC, 2)= A.UPC");
		if( excludeDeptId != null && !excludeDeptId.equals(""))
			buffer.append(" AND B.DEPT_ID NOT IN (").append(excludeDeptId).append(")");

		buffer.append(")");
		
		buffer.append(" where POS_TIMESTAMP >=").append( "TO_DATE('").append(startPeriod).append("','MM/DD/YYYY HH:MI AM')");
		buffer.append(" AND POS_TIMESTAMP <=").append( "TO_DATE('").append(endPeriod).append("','MM/DD/YYYY HH:MI AM')");
		logger.debug( buffer.toString());
		
		String result = PristineDBUtil.getSingleColumnVal(conn, buffer, "getCompDataForItem");
		visitCount = Integer.parseInt(result);
		return visitCount;
	}

	
	
	
	*/
	
	public int getVisitCountOld(Connection conn, String storeNum, String startPeriod, String endPeriod, 
			String excludeDeptId, boolean isArchive) throws GeneralException {
		
		int visitCount = 0;
		StringBuffer buffer = new StringBuffer();
		buffer.append(" SELECT count (DISTINCT TRANSACTION_NO) ");
		if( isArchive )
			buffer.append(" FROM MOVEMENT_DAILY_ARCHIVE A, ");
		else
			buffer.append(" FROM "+TABLE_NAME_MOVEMENT_DAILY+" A, ");
		
		buffer.append(" ITEM_LOOKUP B");
		buffer.append(" WHERE A.COMP_STR_NO = '").append(storeNum).append("'");
		buffer.append(" AND A.POS_TIMESTAMP >=").append( "TO_DATE('").append(startPeriod).append("','MM/DD/YYYY HH:MI AM')");
		buffer.append(" AND A.POS_TIMESTAMP <=").append( "TO_DATE('").append(endPeriod).append("','MM/DD/YYYY HH:MI AM')");
		buffer.append(" AND SUBSTR(B.UPC, 2)= A.UPC ");
		if( excludeDeptId != null && !excludeDeptId.equals(""))
			buffer.append(" AND B.DEPT_ID NOT IN (").append(excludeDeptId).append(")");
		logger.debug( buffer.toString());
		
		String result = PristineDBUtil.getSingleColumnVal(conn, buffer, "getCompDataForItem");
		visitCount = Integer.parseInt(result);
		return visitCount;
	}

	/*
	 * 
	 * 
	 
	 SELECT count (DISTINCT TRANSACTION_NO)  from
(Select * 
FROM MOVEMENT_DAILY A,  ITEM_LOOKUP B ,retail_calendar rc
WHERE A.COMP_STR_NO = '0108' 
and A.calendar_id = rc.calendar_id
and (rc.start_date >= TO_DATE('01/12/2013','MM/DD/YYYY')) and (rc.start_date <= TO_DATE('01/13/2013','MM/DD/YYYY'))
AND SUBSTR(B.UPC, 2)= A.UPC  AND B.DEPT_ID NOT IN (21,34,37)
)
WHERE POS_TIMESTAMP >=TO_DATE('01/12/2013 8:00 AM','MM/DD/YYYY HH:MI AM') 
AND POS_TIMESTAMP <=TO_DATE('01/13/2013 8:00 AM','MM/DD/YYYY HH:MI AM') 

	 
	 
	 */

	public Integer get7dayDaysMoved(Connection conn, int s1, int dow, String analysisDate, int strId,int itemCode) 
			throws GeneralException, SQLException {
			int daysMoved =0;
			// get s0  given current schedule get the schedule corresponding to last week
			StringBuffer sql = new StringBuffer("Select count(*) from (Select distinct schedule_id, dow_id  from summary_daily_mov where item_code = "+itemCode+" " +
					"and ((schedule_id = "+s1+" and dow_id < "+dow+") or " +
							"(schedule_id = (Select schedule_id from schedule s, retail_calendar rc where comp_str_id = "+strId + " and s.start_date = rc.start_date and " +
					" to_date('"+analysisDate+"','MM/DD/YYYY')-7 >= rc.start_date and" +
					" to_date('"+analysisDate+"','MM/DD/YYYY')-7 <= rc.end_date and row_type='W') and dow_id >="+dow+")))  ");
			
			CachedRowSet movement = PristineDBUtil.executeQuery(conn, sql,
					"get7dayDaysMoved");
			while(movement.next()){
				daysMoved = movement.getInt(1);
			}
			
			return daysMoved;
		}

	
	public CachedRowSet get7dayMovement(Connection conn, int s1, int dow, String analysisDate, int strId,int itemCode, String saleInd, float price) 
		throws GeneralException, SQLException {

		// get s0  given current schedule get the schedule corresponding to last week
		StringBuffer sql = new StringBuffer("Select tod_id, dow_id, quantity_total, total_visit_count, schedule_id, oos_ind from summary_daily_mov where item_code = "+itemCode+" " +
				"and ((schedule_id = "+s1+" and dow_id < "+dow+") or " +
						"(schedule_id = (Select schedule_id from schedule s, retail_calendar rc where comp_str_id = "+strId + " and s.start_date = rc.start_date and " +
				" to_date('"+analysisDate+"','MM/DD/YYYY')-7 >= rc.start_date and" +
				" to_date('"+analysisDate+"','MM/DD/YYYY')-7 <= rc.end_date and row_type='W') and dow_id >="+dow+"))  and " +
						" ((promotional_ind ='Y' and (unit_sale_price=round("+price+",2))) or (promotional_ind ='N') and (unit_reg_price=round("+price+",2)))" +
								" and promotional_ind ='"+saleInd+"'");
		
		CachedRowSet movement = PristineDBUtil.executeQuery(conn, sql,
				"get7dayMovement");
		
		
		return movement;
	}

	public CachedRowSet get7dayAvgMovement(Connection conn, int s1, int dow, String analysisDate, int strId,int itemCode, String saleInd, float price) 
			throws GeneralException, SQLException {

			// get s0  given current schedule get the schedule corresponding to last week
			StringBuffer sql = new StringBuffer("Select tod_id, dow_id, quantity_total, total_visit_count, schedule_id, oos_ind from summary_daily_mov where item_code = "+itemCode+" " +
					"and ((schedule_id = "+s1+" and dow_id < "+dow+") or " +
							"(schedule_id = (Select schedule_id from schedule s, retail_calendar rc where comp_str_id = "+strId + " and s.start_date = rc.start_date and " +
					" to_date('"+analysisDate+"','MM/DD/YYYY')-7 >= rc.start_date and" +
					" to_date('"+analysisDate+"','MM/DD/YYYY')-7 <= rc.end_date and row_type='W') and dow_id >="+dow+"))");
			
			CachedRowSet movement = PristineDBUtil.executeQuery(conn, sql,
					"get7dayMovement");
			
			
			return movement;
		}
	public CachedRowSet getMapeScore(Connection conn, int strId,int itemCode) 
			throws GeneralException, SQLException {

		// get mean movement
		StringBuffer sql = new StringBuffer("select avg(quantity_total) from summary_daily_mov sd, schedule s where sd.schedule_id = s.schedule_id and " +
				"s.comp_str_id = "+strId+" and item_code ="+itemCode );
		
		CachedRowSet result = PristineDBUtil.executeQuery(conn, sql,
				"getPrevDayMovement");
		double mean = 0;
		while(result.next()){
			mean = result.getDouble(1);
		}
		if(mean ==0) {
			return null;
		}
		// get mape
		sql = new StringBuffer("select tod_id, sum(abs(quantity_total - case when oos_method = 'prev day' then prev_day_exp  when oos_method = 'last 7 days'  then last_7day_exp" +
				"  when oos_method = 'last 28 days' then last_28day_exp  " +
				"when oos_method = 'last 180 days' then last_180day_exp  else 0 end )/(case when quantity_total >0 then quantity_total else "+mean+" end))/count(*) mape " +
				"from summary_daily_mov sd, schedule s where sd.schedule_id = s.schedule_id and " +
				"s.comp_str_id = "+strId+" and item_code ="+itemCode +" and " +
						"(OOS_METHOD = 'prev day' or OOS_METHOD = 'last 7 days' or OOS_METHOD = 'last 28 days' or OOS_METHOD = 'last 180 days') group by tod_id");
			
			result = PristineDBUtil.executeQuery(conn, sql,
					"getPrevDayMovement");
			
			
			return result;
		}

	
	public CachedRowSet getPrevdayMovement(Connection conn, int s1, int dow, String analysisDate, int strId,int itemCode, String saleInd, float price) 
			throws GeneralException, SQLException {

			// get s0  given current schedule get the schedule corresponding to previous day
			if(dow ==1){
				dow =7;
			}else{
				dow = dow-1;
			}
			
			
			StringBuffer sql = new StringBuffer("Select tod_id, dow_id, quantity_total, total_visit_count, schedule_id, oos_ind from summary_daily_mov " +
					"where item_code = "+itemCode+" " +
					"and (" +
							"(schedule_id = (Select schedule_id from schedule s, retail_calendar rc where comp_str_id = "+strId 
							+ " and s.start_date = rc.start_date and " +
					" to_date('"+analysisDate+"','MM/DD/YYYY')-1 >= rc.start_date and" +
					" to_date('"+analysisDate+"','MM/DD/YYYY')-1 <= rc.end_date and row_type='W') and dow_id ="+dow+"))  and " +
							" ((promotional_ind ='Y' and (unit_sale_price=round("+price+",2))) or (promotional_ind ='N') and (unit_reg_price=round("+price+",2)))" +
									" and promotional_ind ='"+saleInd+"'");
			
			CachedRowSet movement = PristineDBUtil.executeQuery(conn, sql,
					"getPrevDayMovement");
			
			
			return movement;
		}


	public CachedRowSet getPrevdayMovementWithAd(Connection conn, int s1, int dow, String analysisDate, int strId,int itemCode, String saleInd, float price, String displayType, int calendarWeekId) 
			throws GeneralException, SQLException {

			// get s0  given current schedule get the schedule corresponding to previous day
			if(dow ==1){
				dow =7;
			}else{
				dow = dow-1;
			}
			
			String displayTypeCheck = "";
			if(displayType == null || displayType.trim().length()==0){
				displayTypeCheck = " ad.display_type is null ";
			}else{
//				displayTypeCheck = " ad.display_type = '"+displayType+"' ";
				displayTypeCheck = " ad.display_type is not null ";
			}

			
			StringBuffer sql = new StringBuffer("Select tod_id, dow_id, quantity_total, total_visit_count, schedule_id, oos_ind from summary_daily_mov_view ss, ad_info_view ad " +
					" where item_code = "+itemCode+" and  comp_str_id = "+strId +
					" and (" +
					" to_date('"+analysisDate+"','MM/DD/YYYY')-1 >= start_date and" +
					" to_date('"+analysisDate+"','MM/DD/YYYY')-1 <= end_date) and dow_id ="+dow+
							"  and " +
							" ((promotional_ind ='Y' and (unit_sale_price=round("+price+",2))) or (promotional_ind ='N') and (unit_reg_price=round("+price+",2)))" +
									" and promotional_ind ='"+saleInd+"' and " +
											" ss.item_code = ad.presto_item_code (+) " +
											" and ss.calendar_id = ad.calendar_id (+) " +
											" and " + displayTypeCheck);
			
			CachedRowSet movement = PristineDBUtil.executeQuery(conn, sql,
					"getPrevDayMovement");
			
			
			return movement;
		}

	
	public Integer get28dayDaysMoved(Connection conn, int s1, int dow, String analysisDate, int strId,int itemCode) 
			throws GeneralException, SQLException {
		int daysMoved =0;
		// get s0  given current schedule get the schedule corresponding to last week
			StringBuffer sql = new StringBuffer("Select count(*) from (Select distinct schedule_id , dow_id from summary_daily_mov where item_code = "+itemCode+" " +
					"and ((schedule_id = "+s1+" and dow_id < "+dow+") or " +
							"(schedule_id = (Select schedule_id from schedule s, retail_calendar rc where comp_str_id = "+strId + " and s.start_date = rc.start_date and " +
					" to_date('"+analysisDate+"','MM/DD/YYYY')-28 >= rc.start_date and" +
					" to_date('"+analysisDate+"','MM/DD/YYYY')-28 <= rc.end_date and row_type='W') and dow_id >="+dow+")" +
							"or (schedule_id = (Select schedule_id from schedule s, retail_calendar rc where comp_str_id = "+strId + " and s.start_date = rc.start_date and " +
					" to_date('"+analysisDate+"','MM/DD/YYYY')-7 >= rc.start_date and" +
					" to_date('"+analysisDate+"','MM/DD/YYYY')-7 <= rc.end_date and row_type='W')) or (schedule_id = (Select schedule_id from schedule s, retail_calendar rc where comp_str_id = "+strId + " and s.start_date = rc.start_date and " +
					" to_date('"+analysisDate+"','MM/DD/YYYY')-14 >= rc.start_date and" +
					" to_date('"+analysisDate+"','MM/DD/YYYY')-14 <= rc.end_date and row_type='W')) or (schedule_id = (Select schedule_id from schedule s, retail_calendar rc where comp_str_id = "+strId + " and s.start_date = rc.start_date and " +
					" to_date('"+analysisDate+"','MM/DD/YYYY')-21 >= rc.start_date and" +
					" to_date('"+analysisDate+"','MM/DD/YYYY')-21 <= rc.end_date and row_type='W'))) ) ");
			
			CachedRowSet movement = PristineDBUtil.executeQuery(conn, sql,
					"get28dayDaysMoved");
			while(movement.next()){
				daysMoved = movement.getInt(1);
			}
			
			return daysMoved;
		}
	
	public CachedRowSet getPrevNdayMovement(Connection conn, int n, int s1, int dow, String analysisDate, int strId,int itemCode, String saleInd, float price) 
			throws GeneralException, SQLException {
			StringBuffer sql = new StringBuffer("select  s.tod_id, s.dow_id, s.quantity_total, s.total_visit_count, s.schedule_id , s.oos_ind  " +
					"from summary_daily_mov s, schedule ss, retail_calendar rc " +
					"where s.schedule_id = ss.schedule_id and ss.comp_str_id = "+strId+" and item_code = "+itemCode +" and ss.start_date = rc.start_date and rc.row_type = 'W' " +
							"and( (rc.start_date <= to_date('"+analysisDate+"','MM/dd/yyyy')-"+n+" and rc.end_date >= to_date('"+analysisDate+"','MM/dd/yyyy')-"+n+" " +
					"and (rc.start_date + s.dow_id-1 >= to_date('"+analysisDate+"','MM/dd/yyyy')-"+n+")) or " +
					"(rc.start_date >= to_date('"+analysisDate+"','MM/dd/yyyy')-"+n+" and rc.end_date <= to_date('"+analysisDate+"','MM/dd/yyyy')-1 ) or " +
					"(rc.start_date <= to_date('"+analysisDate+"','MM/dd/yyyy')-1 and rc.end_date >= to_date('"+analysisDate+"','MM/dd/yyyy')-1 ) " +
					"and (rc.start_date + s.dow_id-1 <= to_date('"+analysisDate+"','MM/dd/yyyy')-1)) and " +
					"((promotional_ind ='Y' and unit_sale_price=round("+price+",2)) or (promotional_ind ='N' and unit_reg_price=round("+price+",2)))");
			
			CachedRowSet movement = PristineDBUtil.executeQuery(conn, sql,
					"getPrevNdayMovement");
			
			return movement;
		}

	public CachedRowSet getPrevNdayMovementWithAd(Connection conn, int n, int s1, int dow, String analysisDate, int strId,int itemCode, String saleInd, float price, String displayType, int calendarWeekId) 
			throws GeneralException, SQLException {
		
			String displayTypeCheck = "";
			if(displayType == null || displayType.trim().length()==0){
				displayTypeCheck = " ad.display_type is null ";
			}else{
				displayTypeCheck = " ad.display_type is not null ";
//				displayTypeCheck = " ad.display_type = '"+displayType+"' ";
			}
			StringBuffer sql = new StringBuffer("select  s.tod_id, s.dow_id, s.quantity_total, s.total_visit_count, s.schedule_id , s.oos_ind  " +
					"from summary_daily_mov_view s, ad_info_view ad " +
					"where  comp_str_id = "+strId+" and item_code = "+itemCode +
							" and( (start_date <= to_date('"+analysisDate+"','MM/dd/yyyy')-"+n+" and end_date >= to_date('"+analysisDate+"','MM/dd/yyyy')-"+n+" " +
					"and (start_date + s.dow_id-1 >= to_date('"+analysisDate+"','MM/dd/yyyy')-"+n+")) or " +
					"(start_date >= to_date('"+analysisDate+"','MM/dd/yyyy')-"+n+" and end_date <= to_date('"+analysisDate+"','MM/dd/yyyy')-1 ) or " +
					"(start_date <= to_date('"+analysisDate+"','MM/dd/yyyy')-1 and end_date >= to_date('"+analysisDate+"','MM/dd/yyyy')-1 ) " +
					"and (start_date + s.dow_id-1 <= to_date('"+analysisDate+"','MM/dd/yyyy')-1)) and " +
					"((promotional_ind ='Y' and unit_sale_price=round("+price+",2)) or (promotional_ind ='N' and unit_reg_price=round("+price+",2))) " +
							" and s.item_code = ad.presto_item_code (+) " +
							" and s.calendar_id = ad.calendar_id (+) " + " and " +
		displayTypeCheck);
			

			/*
			 
			 			StringBuffer sql = new StringBuffer("select  s.tod_id, s.dow_id, s.quantity_total, s.total_visit_count, s.schedule_id , s.oos_ind  " +
					"from summary_daily_mov s, schedule ss, retail_calendar rc, ad_info_view ad " +
					"where s.schedule_id = ss.schedule_id and ss.comp_str_id = "+strId+" and item_code = "+itemCode +" and ss.start_date = rc.start_date and rc.row_type = 'W' " +
							"and( (rc.start_date <= to_date('"+analysisDate+"','MM/dd/yyyy')-"+n+" and rc.end_date >= to_date('"+analysisDate+"','MM/dd/yyyy')-"+n+" " +
					"and (rc.start_date + s.dow_id-1 >= to_date('"+analysisDate+"','MM/dd/yyyy')-"+n+")) or " +
					"(rc.start_date >= to_date('"+analysisDate+"','MM/dd/yyyy')-"+n+" and rc.end_date <= to_date('"+analysisDate+"','MM/dd/yyyy')-1 ) or " +
					"(rc.start_date <= to_date('"+analysisDate+"','MM/dd/yyyy')-1 and rc.end_date >= to_date('"+analysisDate+"','MM/dd/yyyy')-1 ) " +
					"and (rc.start_date + s.dow_id-1 <= to_date('"+analysisDate+"','MM/dd/yyyy')-1)) and " +
					"((promotional_ind ='Y' and unit_sale_price=round("+price+",2)) or (promotional_ind ='N' and unit_reg_price=round("+price+",2))) " +
							" and s.item_code = ad.presto_item_code (+) and " +
							" ad.calendar_id = "+ calendarWeekId + " and " +
		displayTypeCheck);

			 
			 
			 
			 
			 */
			
			
			CachedRowSet movement = PristineDBUtil.executeQuery(conn, sql,
					"getPrevNdayMovement");
			
			return movement;
		}

	
	public boolean getMoved2plusInAllDaysInPrevNdays(Connection conn, int n, int s1, int dow, String analysisDate, int strId,int itemCode) 
			throws GeneralException, SQLException {
			StringBuffer sql = new StringBuffer("select count(*) from (select  s.tod_id, s.dow_id, s.quantity_total, s.total_visit_count, s.schedule_id , s.oos_ind  " +
					"from summary_daily_mov s, schedule ss, retail_calendar rc " +
					"where s.schedule_id = ss.schedule_id and ss.comp_str_id = "+strId+" and item_code = "+itemCode +" and ss.start_date = rc.start_date and rc.row_type = 'W' " +
							"and( (rc.start_date <= to_date('"+analysisDate+"','MM/dd/yyyy')-"+n+" and rc.end_date >= to_date('"+analysisDate+"','MM/dd/yyyy')-"+n+" " +
					"and (rc.start_date + s.dow_id-1 >= to_date('"+analysisDate+"','MM/dd/yyyy')-"+n+")) or " +
					"(rc.start_date >= to_date('"+analysisDate+"','MM/dd/yyyy')-"+n+" and rc.end_date <= to_date('"+analysisDate+"','MM/dd/yyyy')-1 ) or " +
					"(rc.start_date <= to_date('"+analysisDate+"','MM/dd/yyyy')-1 and rc.end_date >= to_date('"+analysisDate+"','MM/dd/yyyy')-1 ) " +
					"and (rc.start_date + s.dow_id-1 <= to_date('"+analysisDate+"','MM/dd/yyyy')-1)) and quantity_total > 2" +
					")");
			
			CachedRowSet movement = PristineDBUtil.executeQuery(conn, sql,
					"getMoved2plusInAllDaysInPrevNdays");
			
			boolean movedAllDays = false;
			int count =0;
			if(movement.next()){
				count = movement.getInt(1);
			}
			
			return count == n;
		}

	
	public Integer getPrevNdayDaysMovedNEW(Connection conn, int n, int s1, int dow, String analysisDate, int strId,int itemCode) 
			throws GeneralException, SQLException {
			Integer count = 0;
			StringBuffer sql = new StringBuffer("select count(*) from (select  distinct s.schedule_id,s.dow_id  " +
					"from summary_daily_mov s, schedule ss, retail_calendar rc " +
					"where s.schedule_id = ss.schedule_id and ss.comp_str_id = "+strId+" and item_code = "+itemCode +" and ss.start_date = rc.start_date and rc.row_type = 'W' " +
							"and( (rc.start_date <= to_date('"+analysisDate+"','MM/dd/yyyy')-"+n+" and rc.end_date >= to_date('"+analysisDate+"','MM/dd/yyyy')-"+n+" " +
					"and (rc.start_date + s.dow_id-1 >= to_date('"+analysisDate+"','MM/dd/yyyy')-"+n+")) or " +
					"(rc.start_date >= to_date('"+analysisDate+"','MM/dd/yyyy')-"+n+" and rc.end_date <= to_date('"+analysisDate+"','MM/dd/yyyy')-1 ) or " +
					"(rc.start_date <= to_date('"+analysisDate+"','MM/dd/yyyy')-1 and rc.end_date >= to_date('"+analysisDate+"','MM/dd/yyyy')-1 ) " +
					"and (rc.start_date + s.dow_id-1 <= to_date('"+analysisDate+"','MM/dd/yyyy')-1))   " +
							")");
			
			
			/*
			 * 

select count(*) from 
(select  distinct s.schedule_id,s.dow_id  
from summary_daily_mov s, schedule ss, retail_calendar rc 
where s.schedule_id = ss.schedule_id and ss.comp_str_id = 5670 and item_code = 24350 and 
ss.start_date = rc.start_date and rc.row_type = 'W' 
and
( 

(rc.start_date <= to_date('06/20/2013','MM/dd/yyyy')-120 and rc.end_date >= to_date('06/20/2013','MM/dd/yyyy')-120 
and (rc.start_date + s.dow_id-1 >= to_date('06/20/2013','MM/dd/yyyy')-120)) 
)

union

select  distinct s.schedule_id,s.dow_id  
from summary_daily_mov s, schedule ss, retail_calendar rc 
where s.schedule_id = ss.schedule_id and ss.comp_str_id = 5670 and item_code = 24350 and 
ss.start_date = rc.start_date and rc.row_type = 'W' 
and
( 

(rc.start_date >= to_date('06/20/2013','MM/dd/yyyy')-120 and rc.end_date <= to_date('06/20/2013','MM/dd/yyyy')-1 ) 

)

union

select  distinct s.schedule_id,s.dow_id  
from summary_daily_mov s, schedule ss, retail_calendar rc 
where s.schedule_id = ss.schedule_id and ss.comp_str_id = 5670 and item_code = 24350 and 
ss.start_date = rc.start_date and rc.row_type = 'W' 
and
( 

(rc.start_date <= to_date('06/20/2013','MM/dd/yyyy')-1 and rc.end_date >= to_date('06/20/2013','MM/dd/yyyy')-1 ) 
and (rc.start_date + s.dow_id-1 <= to_date('06/20/2013','MM/dd/yyyy')-1)

)
)





			 * 
			 */
			
			
			CachedRowSet movement = PristineDBUtil.executeQuery(conn, sql,
					"getPrevNdayMovement");
			if(movement.next()){
				count = movement.getInt(1);
			}
			
			return count;
		}

	public Integer getPrevNdayDaysMoved(Connection conn, int n, int s1, int dow, String analysisDate, int strId,int itemCode) 
			throws GeneralException, SQLException {
			Integer count = 0;
			StringBuffer sql = new StringBuffer("select count(*) from (select  distinct s.schedule_id,s.dow_id  " +
					"from summary_daily_mov s, schedule ss, retail_calendar rc " +
					"where s.schedule_id = ss.schedule_id and ss.comp_str_id = "+strId+" and item_code = "+itemCode +" and ss.start_date = rc.start_date and rc.row_type = 'W' " +
							"and( (rc.start_date <= to_date('"+analysisDate+"','MM/dd/yyyy')-"+n+" and rc.end_date >= to_date('"+analysisDate+"','MM/dd/yyyy')-"+n+" " +
					"and (rc.start_date + s.dow_id-1 >= to_date('"+analysisDate+"','MM/dd/yyyy')-"+n+")) or " +
					"(rc.start_date >= to_date('"+analysisDate+"','MM/dd/yyyy')-"+n+" and rc.end_date <= to_date('"+analysisDate+"','MM/dd/yyyy')-1 ) or " +
					"(rc.start_date <= to_date('"+analysisDate+"','MM/dd/yyyy')-1 and rc.end_date >= to_date('"+analysisDate+"','MM/dd/yyyy')-1 ) " +
					"and (rc.start_date + s.dow_id-1 <= to_date('"+analysisDate+"','MM/dd/yyyy')-1))   " +
							")");
			
			CachedRowSet movement = PristineDBUtil.executeQuery(conn, sql,
					"getPrevNdayMovement");
			if(movement.next()){
				count = movement.getInt(1);
			}
			
			return count;
		}

	public Integer getPrevNdayDaysToIgnore(Connection conn, int n, int s1, int dow, String analysisDate, int strId,int itemCode, String saleInd, float price) 
			throws GeneralException, SQLException {
		// This complex query will fetch the number of days within a date range, where the item was not offered at the store,
		// by eliminating dates that fall in weekly calendar id, where the entire week had no movement.
		
			Integer count = 0;
			StringBuffer sql = new StringBuffer("select count(*) from" +
					"( select * from retail_calendar dc, " +
					"(select rc.start_date sd, rc.end_date ed " +
					"from schedule s1, retail_calendar rc " +
					"where s1.comp_str_id = "+strId+" and s1.start_date = rc.start_date and rc.calendar_id in " +
					"((select calendar_id from retail_calendar where ((start_date >= to_date('"+analysisDate+"','MM/dd/yyyy') - "+n+" " +
					"and end_date <= to_date('"+analysisDate+"','MM/dd/yyyy')-1) or (start_date <= to_date('"+analysisDate+"','MM/dd/yyyy') - "+n+" " +
					"and end_date >= to_date('"+analysisDate+"','MM/dd/yyyy')-"+n+") or (start_date <= to_date('"+analysisDate+"','MM/dd/yyyy') -1 " +
					"and end_date >= to_date('"+analysisDate+"','MM/dd/yyyy')-1)) and row_type = 'W') ) " +
					"and s1.schedule_id not in  (select  distinct s.schedule_id from summary_daily_mov s, schedule ss, retail_calendar rc " +
					"where ((s.promotional_ind = 'Y' and s.unit_sale_price = "+price+") or (s.promotional_ind = 'N' and s.unit_reg_price = "+price+")) and " +
					" s.schedule_id = ss.schedule_id and ss.comp_str_id = "+strId+" and s.tod_id = 4 and ss.start_date = rc.start_date and rc.row_type = 'W' " +
					"and( (rc.start_date >= to_date('"+analysisDate+"','MM/dd/yyyy')-"+n+" and rc.end_date <= to_date('"+analysisDate+"','MM/dd/yyyy')-1 ) " +
					"or (rc.start_date <= to_date('"+analysisDate+"','MM/dd/yyyy')-1 and rc.end_date >= to_date('"+analysisDate+"','MM/dd/yyyy')-1 ) or" +
					" (rc.start_date <= to_date('"+analysisDate+"','MM/dd/yyyy')-"+n+" and rc.end_date >= to_date('"+analysisDate+"','MM/dd/yyyy')-"+n+" )) " +
					"and item_code = "+itemCode+" )) TabA " +
					"where dc.start_date >= tabA.sd and dc.start_date  <= tabA.ed " +
					"and dc.start_date >= to_date('"+analysisDate+"','MM/dd/yyyy')-"+n+" " +
					"and dc.start_date <= to_date('"+analysisDate+"','MM/dd/yyyy')-1 " +
							"and dc.row_type = 'D')");
			
			CachedRowSet movement = PristineDBUtil.executeQuery(conn, sql,
					"getPrevNdayDaysToIgnore");
			
			if(movement.next()){
				count = movement.getInt(1);
			}
			
			
			return count;
		}

	public Integer getPrevNdayDaysToIgnoreWithAd(Connection conn, int n, int s1, int dow, String analysisDate, int strId,int itemCode, String saleInd, 
			float price, String displayType, int calendarWeekId) 
			throws GeneralException, SQLException {
		// This complex query will fetch the number of days within a date range, where the item was not offered at the store,
		// by eliminating dates that fall in weekly calendar id, where the entire week had no movement.
		
		String displayTypeCheck = "";
		if(displayType == null || displayType.trim().length()==0){
			displayTypeCheck = " ad.display_type is null ";
		}else{
//			displayTypeCheck = " ad.display_type = '"+displayType+"' ";
			displayTypeCheck = " ad.display_type is not null ";
		}

		
			Integer count = 0;
			StringBuffer sql = new StringBuffer("select count(*) from" +
					"( select * from retail_calendar dc, " +
					"(select rc.start_date sd, rc.end_date ed " +
					"from schedule s1, retail_calendar rc " +
					"where s1.comp_str_id = "+strId+" and s1.start_date = rc.start_date and rc.calendar_id in " +
					"((select calendar_id from retail_calendar where ((start_date >= to_date('"+analysisDate+"','MM/dd/yyyy') - "+n+" " +
					"and end_date <= to_date('"+analysisDate+"','MM/dd/yyyy')-1) or (start_date <= to_date('"+analysisDate+"','MM/dd/yyyy') - "+n+" " +
					"and end_date >= to_date('"+analysisDate+"','MM/dd/yyyy')-"+n+") or (start_date <= to_date('"+analysisDate+"','MM/dd/yyyy') -1 " +
					"and end_date >= to_date('"+analysisDate+"','MM/dd/yyyy')-1)) and row_type = 'W') ) " +
					"and s1.schedule_id not in  " +
					
					"(select  distinct s.schedule_id from summary_daily_mov_view s,  ad_info_view ad " +
					"where ((s.promotional_ind = 'Y' and s.unit_sale_price = "+price+") or (s.promotional_ind = 'N' and s.unit_reg_price = "+price+")) " +
							"and s.item_code = ad.presto_item_code (+) and " +
							" s.calendar_id = ad.calendar_id (+)  and "+
						displayTypeCheck+" and " +
					" comp_str_id = "+strId+" and s.tod_id = 4  " +
					"and( (start_date >= to_date('"+analysisDate+"','MM/dd/yyyy')-"+n+" and end_date <= to_date('"+analysisDate+"','MM/dd/yyyy')-1 ) " +
					"or (start_date <= to_date('"+analysisDate+"','MM/dd/yyyy')-1 and end_date >= to_date('"+analysisDate+"','MM/dd/yyyy')-1 ) or" +
					" (start_date <= to_date('"+analysisDate+"','MM/dd/yyyy')-"+n+" and end_date >= to_date('"+analysisDate+"','MM/dd/yyyy')-"+n+" )) " +
					"and item_code = "+itemCode+" )" +
					
							") TabA " +
					"where dc.start_date >= tabA.sd and dc.start_date  <= tabA.ed " +
					"and dc.start_date >= to_date('"+analysisDate+"','MM/dd/yyyy')-"+n+" " +
					"and dc.start_date <= to_date('"+analysisDate+"','MM/dd/yyyy')-1 " +
							"and dc.row_type = 'D')");

			
			/*
			 
			 			StringBuffer sql = new StringBuffer("select count(*) from" +
					"( select * from retail_calendar dc, " +
					"(select rc.start_date sd, rc.end_date ed " +
					"from schedule s1, retail_calendar rc " +
					"where s1.comp_str_id = "+strId+" and s1.start_date = rc.start_date and rc.calendar_id in " +
					"((select calendar_id from retail_calendar where ((start_date >= to_date('"+analysisDate+"','MM/dd/yyyy') - "+n+" " +
					"and end_date <= to_date('"+analysisDate+"','MM/dd/yyyy')-1) or (start_date <= to_date('"+analysisDate+"','MM/dd/yyyy') - "+n+" " +
					"and end_date >= to_date('"+analysisDate+"','MM/dd/yyyy')-"+n+") or (start_date <= to_date('"+analysisDate+"','MM/dd/yyyy') -1 " +
					"and end_date >= to_date('"+analysisDate+"','MM/dd/yyyy')-1)) and row_type = 'W') ) " +
					"and s1.schedule_id not in  " +
					
					"(select  distinct s.schedule_id from summary_daily_mov s, schedule ss, retail_calendar rc, ad_info_view ad " +
					"where ((s.promotional_ind = 'Y' and s.unit_sale_price = "+price+") or (s.promotional_ind = 'N' and s.unit_reg_price = "+price+")) " +
							"and s.item_code = ad.presto_item_code (+) and " +
							" ad.calendar_id = "+ calendarWeekId +" and "+
						displayTypeCheck+" and " +
					" s.schedule_id = ss.schedule_id and ss.comp_str_id = "+strId+" and s.tod_id = 4 and ss.start_date = rc.start_date and rc.row_type = 'W' " +
					"and( (rc.start_date >= to_date('"+analysisDate+"','MM/dd/yyyy')-"+n+" and rc.end_date <= to_date('"+analysisDate+"','MM/dd/yyyy')-1 ) " +
					"or (rc.start_date <= to_date('"+analysisDate+"','MM/dd/yyyy')-1 and rc.end_date >= to_date('"+analysisDate+"','MM/dd/yyyy')-1 ) or" +
					" (rc.start_date <= to_date('"+analysisDate+"','MM/dd/yyyy')-"+n+" and rc.end_date >= to_date('"+analysisDate+"','MM/dd/yyyy')-"+n+" )) " +
					"and item_code = "+itemCode+" )" +
					
							") TabA " +
					"where dc.start_date >= tabA.sd and dc.start_date  <= tabA.ed " +
					"and dc.start_date >= to_date('"+analysisDate+"','MM/dd/yyyy')-"+n+" " +
					"and dc.start_date <= to_date('"+analysisDate+"','MM/dd/yyyy')-1 " +
							"and dc.row_type = 'D')");

			 
			 
			 
			 * 
			 */
			
			CachedRowSet movement = PristineDBUtil.executeQuery(conn, sql,
					"getPrevNdayDaysToIgnore");
			
			if(movement.next()){
				count = movement.getInt(1);
			}
			
			
			return count;
		}

	
	public Integer getPrevNdayDaysToIgnoreZeroMovement(Connection conn, int n, int s1, int dow, String analysisDate, int strId,int itemCode, String saleInd, float price) 
			throws GeneralException, SQLException {
		// This complex query will fetch the number of days within a date range, where the item was not offered at the store,
		// by eliminating dates that fall in weekly calendar id, where the entire week had no movement.
		
			Integer count = 0;
			StringBuffer sql = new StringBuffer("select count(*) from" +
					"( select * from retail_calendar dc, " +
					"(select rc.start_date sd, rc.end_date ed " +
					"from schedule s1, retail_calendar rc " +
					"where s1.comp_str_id = "+strId+" and s1.start_date = rc.start_date and rc.calendar_id in " +
					"((select calendar_id from retail_calendar where ((start_date >= to_date('"+analysisDate+"','MM/dd/yyyy') - "+n+" " +
					"and end_date <= to_date('"+analysisDate+"','MM/dd/yyyy')-1) or (start_date <= to_date('"+analysisDate+"','MM/dd/yyyy') - "+n+" " +
					"and end_date >= to_date('"+analysisDate+"','MM/dd/yyyy')-"+n+") or (start_date <= to_date('"+analysisDate+"','MM/dd/yyyy') -1 " +
					"and end_date >= to_date('"+analysisDate+"','MM/dd/yyyy')-1)) and row_type = 'W') ) " +
					"and s1.schedule_id not in  (select  distinct s.schedule_id from summary_daily_mov s, schedule ss, retail_calendar rc " +
					"where  " +
					" s.schedule_id = ss.schedule_id and ss.comp_str_id = "+strId+" and s.tod_id = 4 and ss.start_date = rc.start_date and rc.row_type = 'W' " +
					"and( (rc.start_date >= to_date('"+analysisDate+"','MM/dd/yyyy')-"+n+" and rc.end_date <= to_date('"+analysisDate+"','MM/dd/yyyy')-1 ) " +
					"or (rc.start_date <= to_date('"+analysisDate+"','MM/dd/yyyy')-1 and rc.end_date >= to_date('"+analysisDate+"','MM/dd/yyyy')-1 ) or" +
					" (rc.start_date <= to_date('"+analysisDate+"','MM/dd/yyyy')-"+n+" and rc.end_date >= to_date('"+analysisDate+"','MM/dd/yyyy')-"+n+" )) " +
					"and item_code = "+itemCode+" )) TabA " +
					"where dc.start_date >= tabA.sd and dc.start_date  <= tabA.ed " +
					"and dc.start_date >= to_date('"+analysisDate+"','MM/dd/yyyy')-"+n+" " +
					"and dc.start_date <= to_date('"+analysisDate+"','MM/dd/yyyy')-1 " +
							"and dc.row_type = 'D')");
			
			CachedRowSet movement = PristineDBUtil.executeQuery(conn, sql,
					"getPrevNdayDaysToIgnore");
			
			if(movement.next()){
				count = movement.getInt(1);
			}
			
			
			return count;
		}

	
	
	public CachedRowSet get28dayMovement(Connection conn, int s1, int dow, String analysisDate, int strId,int itemCode, String saleInd, float price) 
			throws GeneralException, SQLException {
		//TODO need to adjust SQL for 28 days
			// get s0  given current schedule get the schedule corresponding to last week
			StringBuffer sql = new StringBuffer("Select tod_id, dow_id, quantity_total, total_visit_count, schedule_id , oos_ind from summary_daily_mov where item_code = "+itemCode+" " +
					"and ((schedule_id = "+s1+" and dow_id < "+dow+") or " +
							"(schedule_id = (Select schedule_id from schedule s, retail_calendar rc where comp_str_id = "+strId + " and s.start_date = rc.start_date and " +
					" to_date('"+analysisDate+"','MM/DD/YYYY')-28 >= rc.start_date and" +
					" to_date('"+analysisDate+"','MM/DD/YYYY')-28 <= rc.end_date and row_type='W') and dow_id >="+dow+")" +
							"or (schedule_id = (Select schedule_id from schedule s, retail_calendar rc where comp_str_id = "+strId + " and s.start_date = rc.start_date and " +
					" to_date('"+analysisDate+"','MM/DD/YYYY')-7 >= rc.start_date and" +
					" to_date('"+analysisDate+"','MM/DD/YYYY')-7 <= rc.end_date and row_type='W')) or (schedule_id = (Select schedule_id from schedule s, retail_calendar rc where comp_str_id = "+strId + " and s.start_date = rc.start_date and " +
					" to_date('"+analysisDate+"','MM/DD/YYYY')-14 >= rc.start_date and" +
					" to_date('"+analysisDate+"','MM/DD/YYYY')-14 <= rc.end_date and row_type='W')) or (schedule_id = (Select schedule_id from schedule s, retail_calendar rc where comp_str_id = "+strId + " and s.start_date = rc.start_date and " +
					" to_date('"+analysisDate+"','MM/DD/YYYY')-21 >= rc.start_date and" +
					" to_date('"+analysisDate+"','MM/DD/YYYY')-21 <= rc.end_date and row_type='W')))  and " +
					"((promotional_ind ='Y' and unit_sale_price=round("+price+",2)) or (promotional_ind ='N' and unit_reg_price=round("+price+",2)))");
			
			CachedRowSet movement = PristineDBUtil.executeQuery(conn, sql,
					"get28dayMovement");
			
			return movement;
		}

	
	public double getdailyAvgStoreVisitCount(Connection conn,int storeId, String analysisDate) throws GeneralException
	{
		double ct = 0;
		StringBuffer sb = new StringBuffer();
		sb.append(" select avg(total_visit_count) from summary_daily_mov s, schedule ss where s.schedule_id = ss.schedule_id and ss.start_date >= to_date('"+analysisDate+"','MM/DD/YYYY')-42 and comp_str_id = "+storeId+" and tod_id = 4 ");
		String result = PristineDBUtil.getSingleColumnVal(conn, sb,
				"getdailyAvgStoreVisitCount");
		if(result != null)
			ct = Double.parseDouble(result);
		return ct;
	}

	
	public CachedRowSet get42dayAvgMovement(Connection conn, int s1, int dow, String analysisDate, int strId,int itemCode, String saleInd, float price) 
			throws GeneralException, SQLException {
			StringBuffer sql = new StringBuffer("Select tod_id, dow_id, quantity_total, total_visit_count, schedule_id , oos_ind from summary_daily_mov where item_code = "+itemCode+" " +
					"and ((schedule_id = "+s1+" and dow_id < "+dow+") or " +
					
					"(schedule_id = (Select schedule_id from schedule s, retail_calendar rc where comp_str_id = "+strId + " and s.start_date = rc.start_date and " +
					" to_date('"+analysisDate+"','MM/DD/YYYY')-42 >= rc.start_date and" +
					" to_date('"+analysisDate+"','MM/DD/YYYY')-42 <= rc.end_date and row_type='W') and dow_id >="+dow+")" +
	
					" or (schedule_id = (Select schedule_id from schedule s, retail_calendar rc where comp_str_id = "+strId + " and s.start_date = rc.start_date and " +
					" to_date('"+analysisDate+"','MM/DD/YYYY')-7 >= rc.start_date and" +
					" to_date('"+analysisDate+"','MM/DD/YYYY')-7 <= rc.end_date and row_type='W')) " +

					" or (schedule_id = (Select schedule_id from schedule s, retail_calendar rc where comp_str_id = "+strId + " and s.start_date = rc.start_date and " +
					" to_date('"+analysisDate+"','MM/DD/YYYY')-14 >= rc.start_date and" +
					" to_date('"+analysisDate+"','MM/DD/YYYY')-14 <= rc.end_date and row_type='W')) " +

					" or (schedule_id = (Select schedule_id from schedule s, retail_calendar rc where comp_str_id = "+strId + " and s.start_date = rc.start_date and " +
					" to_date('"+analysisDate+"','MM/DD/YYYY')-21 >= rc.start_date and" +
					" to_date('"+analysisDate+"','MM/DD/YYYY')-21 <= rc.end_date and row_type='W')) " +

					" or (schedule_id = (Select schedule_id from schedule s, retail_calendar rc where comp_str_id = "+strId + " and s.start_date = rc.start_date and " +
					" to_date('"+analysisDate+"','MM/DD/YYYY')-28 >= rc.start_date and" +
					" to_date('"+analysisDate+"','MM/DD/YYYY')-28 <= rc.end_date and row_type='W')) " +

					" or (schedule_id = (Select schedule_id from schedule s, retail_calendar rc where comp_str_id = "+strId + " and s.start_date = rc.start_date and " +
					" to_date('"+analysisDate+"','MM/DD/YYYY')-35 >= rc.start_date and" +
					" to_date('"+analysisDate+"','MM/DD/YYYY')-35 <= rc.end_date and row_type='W')))  " +
							
					"");
			
			CachedRowSet movement = PristineDBUtil.executeQuery(conn, sql,
					"get42dayMovement");
			
			return movement;
		}

	

	public Integer get180dayDaysMoved(Connection conn, int s1, int dow, String analysisDate, int strId,int itemCode) 
			throws GeneralException, SQLException {
			int daysMoved  = 0;
			// get s0  given current schedule get the schedule corresponding to last week
			StringBuffer sql = new StringBuffer("Select count(*) from (Select distinct schedule_id, dow_id from summary_daily_mov where item_code = "+itemCode+" " +
					"and ((schedule_id = "+s1+" and dow_id < "+dow+") or " +
							"(schedule_id in (Select schedule_id from schedule s, retail_calendar rc where comp_str_id = "+strId + " and s.start_date = rc.start_date and " +
					" rc.start_date < to_date('"+analysisDate+"','MM/DD/YYYY')  and" +
					" rc.start_date >= to_date('"+analysisDate+"','MM/DD/YYYY')-120 and row_type='W') )))" );
			
			CachedRowSet movement = PristineDBUtil.executeQuery(conn, sql,
					"get180dayDaysMoved");
			while(movement.next()){
				daysMoved = movement.getInt(1);
			}
			
			return daysMoved;
		}

	public CachedRowSet get180dayMovement(Connection conn, int s1, int dow, String analysisDate, int strId,int itemCode, String saleInd, float price) 
			throws GeneralException, SQLException {
		//  NOTE WE ARE USE 120 days NOT 180 thought he name says so..check SQL below
			// get s0  given current schedule get the schedule corresponding to last week
			StringBuffer sql = new StringBuffer("Select tod_id, dow_id, quantity_total, total_visit_count, schedule_id from summary_daily_mov where item_code = "+itemCode+" " +
					"and ((schedule_id = "+s1+" and dow_id < "+dow+") or " +
							"(schedule_id <> "+s1+" and schedule_id in (Select schedule_id from schedule s, retail_calendar rc where comp_str_id = "+strId + " and s.start_date = rc.start_date and " +
					" rc.start_date < to_date('"+analysisDate+"','MM/DD/YYYY')  and" +
					" rc.start_date >= to_date('"+analysisDate+"','MM/DD/YYYY')-120 and row_type='W') ))  and " +
							" ((promotional_ind ='Y' and (unit_sale_price=round("+price+",2))) or (promotional_ind ='N') and (unit_reg_price=round("+price+",2)))" );
			
			CachedRowSet movement = PristineDBUtil.executeQuery(conn, sql,
					"get180dayMovement");
			
			return movement;
		}

	
	public OOSSummaryDTO getMovementExp(Connection conn, OOSSummaryDTO oosSummary) 
			throws GeneralException, SQLException {
			
			StringBuffer sb = new StringBuffer();
			sb.append(" SELECT  EXP_QUANTITY_REG, EXP_QUANTITY_SALE, ");
			sb.append(" OOS_MOV_VISIT_RATIO_REG, OOS_MOV_VISIT_RATIO_SALE ");
			sb.append(" FROM SUMMARY_DAILY_MOV_EXP ");
			sb.append(" WHERE STORE_ID = ").append(oosSummary.storeId);
			sb.append(" AND ITEM_CODE = ").append(oosSummary.itemCode);
			sb.append(" AND DOW_ID = ").append(oosSummary.dayofweekId);
			sb.append(" AND TOD_ID = ").append(oosSummary.timeOfDayId);
			CachedRowSet result = PristineDBUtil.executeQuery(conn, sb, "getMovementExp");
			OOSSummaryDTO ossSummaryExp = null; 
			
			if( result.next()){
		
				ossSummaryExp = new OOSSummaryDTO();
				ossSummaryExp.regQty = result.getFloat("EXP_QUANTITY_REG");
				ossSummaryExp.saleQty = result.getFloat("EXP_QUANTITY_SALE");
				ossSummaryExp.regMovVisitRatio = result.getFloat("OOS_MOV_VISIT_RATIO_REG");
				ossSummaryExp.saleMovVisitRatio = result.getFloat("OOS_MOV_VISIT_RATIO_SALE");
				ossSummaryExp.storeId = oosSummary.storeId;
				ossSummaryExp.dayofweekId=oosSummary.dayofweekId;
				ossSummaryExp.timeOfDayId=oosSummary.timeOfDayId;
				ossSummaryExp.itemCode=oosSummary.itemCode;
			}
			return ossSummaryExp;
		}
		

	
	/**
	 * Delete all the records from the table SUMMARY_DAILY_MOV_PRDGRP_EXP
	 * @param conn - Sql Connection 
	 * @param strId - Competitor Store ID 
	 * @throws GeneralException
	 */
	public void deleteSegmentLevelExpectations(Connection conn, int strId, int dayOfWeekId, int productLevelId)
			throws GeneralException {
		
		StringBuffer sb = new StringBuffer();
		sb.append(" DELETE FROM SUMMARY_DAILY_MOV_PRDGRP_EXP WHERE STORE_ID = ")
				.append(strId);
		sb.append(" AND DOW_ID = ").append(dayOfWeekId);
		sb.append(" AND PRODUCT_LEVEL_ID = ").append(productLevelId);

		PristineDBUtil.execute(conn, sb, "deleteExpectations");

	}

	/**
	 * Return segments and its items in a HashMap. 
	 * Items whose PRICE_CHECK_LIST_ID is equal to @param checkListId 
	 * are only considered.
	 * @param conn- Sql Connection 
	 * @param checkListId- Price Check List Id 
	 * @return
	 * @throws GeneralException
	 * @throws SQLException
	 */
	public Map<Integer, ArrayList<Integer>> getItemsBySegment(Connection conn,
			int checkListId) throws GeneralException, SQLException {
		int curSegmentId, preSegmentId = 0, curItemCode, rowCount = 0, rowSize = 0;
		Map<Integer, ArrayList<Integer>> itemsBySegment = new HashMap<Integer, ArrayList<Integer>>();
		StringBuffer sb = new StringBuffer();
		ArrayList<Integer> itemCollection = new ArrayList<Integer>();
		ArrayList<Integer> preItemCollection = new ArrayList<Integer>();

		// Get the Segment Id
		// sb.append(" SELECT DISTINCT(SEGMENT_ID) AS SEGMENT_ID ");
		sb.append(" SELECT IL.SEGMENT_ID AS SEGMENT_ID,IL.ITEM_CODE AS ITEM_CODE ");
		sb.append(" FROM ITEM_LOOKUP IL, PRICE_CHECK_LIST_ITEMS PC ");
		sb.append(
				" WHERE  IL.ITEM_CODE = PC.ITEM_CODE AND PC.PRICE_CHECK_LIST_ID = ")
				.append(checkListId);
		//sb.append(" AND (IL.SEGMENT_ID = 3026) ");
		sb.append(" ORDER BY IL.SEGMENT_ID ");

		CachedRowSet segmentIds = PristineDBUtil.executeQuery(conn, sb,
				"getItemsBySegment");
		//sb = new StringBuffer();
		rowSize = segmentIds.size();
		while (segmentIds.next()) {
			rowCount = rowCount + 1;
			curSegmentId = segmentIds.getInt("SEGMENT_ID");
			curItemCode = segmentIds.getInt("ITEM_CODE");
			itemCollection.add(curItemCode);

			// If cachedrowset has only one segment and one item in it
			if(rowSize == 1)
			{
				itemsBySegment.put(curSegmentId, itemCollection);
				return itemsBySegment;
			}
			
			// Triggers when segment id changes, when last rows is fetched 
			if (preSegmentId != 0 && (curSegmentId != preSegmentId)) {			
				itemsBySegment.put(preSegmentId, preItemCollection);
				preItemCollection = new ArrayList<Integer>();
				itemCollection = new ArrayList<Integer>();
			}

			preSegmentId = curSegmentId;
			preItemCollection.add(curItemCode);
			
			// Last segment and last item
			if(rowSize == rowCount)
			{
				itemsBySegment.put(preSegmentId, preItemCollection);				
			}

			// Get the items in the segment
			/*
			 * sb.append(" SELECT IL.ITEM_CODE AS ITEM_CODE ");
			 * sb.append(" FROM ITEM_LOOKUP IL, PRICE_CHECK_LIST_ITEMS PC ");
			 * sb.append(
			 * " WHERE  IL.ITEM_CODE = PC.ITEM_CODE AND PC.PRICE_CHECK_LIST_ID = "
			 * ).append(checkListId);
			 * sb.append(" AND IL.SEGMENT_ID = ").append(segmentId);
			 * CachedRowSet itemCodes = PristineDBUtil.executeQuery(conn, sb,
			 * "getItemsBySegment");
			 * 
			 * while ( itemCodes.next()){
			 * itemCollection.add(itemCodes.getInt("ITEM_CODE")); }
			 * 
			 * if(itemCollection.size() > 0){ itemsBySegment.put(segmentId,
			 * itemCollection); }
			 */
		}
		return itemsBySegment;
	}

	/**
	 * Calculates expected quantity and expected revenue of a product id
	 * @param conn- Sql Connection 
	 * @param movPrdGrpExp - Instance of OOSMovementProductGroupExpDTO
	 * @param schIdList - Schedule list as string
	 * @param dayofweekId - Day of Week
	 * @param timeofDayId - Time of Day
	 * @param itemList - Item List as string
	 * @return
	 * @throws GeneralException
	 * @throws SQLException
	 */
	public OOSMovementProductGroupExpDTO getProductGroupMovExp(Connection conn,
			OOSMovementProductGroupExpDTO movPrdGrpExp, String schIdList,
			int dayofweekId, int timeofDayId, String itemList)
			throws GeneralException, SQLException {
		StringBuffer sb = new StringBuffer();
		int noOfObservation = 0;
		float totalQuantity = 0;
		double totalRevenue = 0;

		sb.append(" SELECT A.SCHEDULE_ID, A.DOW_ID, A.TOD_ID ");
		sb.append(" , SUM(A.QUANTITY_TOTAL)AS QUANTITY_TOTAL");
		sb.append(" , SUM(A.REV_TOTAL) AS REVENUE_TOTAL ");
		sb.append(" FROM SUMMARY_DAILY_MOV A  WHERE A.SCHEDULE_ID IN (")
				.append(schIdList).append(")");
		sb.append(" AND A.DOW_ID = ").append(dayofweekId);
		sb.append(" AND A.TOD_ID = ").append(timeofDayId);
		sb.append(" AND A.ITEM_CODE IN (").append(itemList).append(")");
		sb.append(" AND A.TOTAL_VISIT_COUNT > 0 ");
		sb.append(" GROUP BY SCHEDULE_ID, DOW_ID, TOD_ID ");
		sb.append(" ORDER BY SCHEDULE_ID ");

		CachedRowSet result = PristineDBUtil.executeQuery(conn, sb,
				"getProductGroupMovExp");

		while (result.next()) {
			noOfObservation++;
			totalQuantity = totalQuantity + result.getFloat("QUANTITY_TOTAL");
			totalRevenue = totalRevenue + result.getFloat("REVENUE_TOTAL");
		}

		movPrdGrpExp.expQuantity = totalQuantity / noOfObservation;
		movPrdGrpExp.expRevenue = totalRevenue / noOfObservation;

		return movPrdGrpExp;
	}

	/**
	 * Performs batch insert
	 * @param conn- Sql Connection 
	 * @param movPrdGrpExpCol - Collection of OOSMovementProductGroupExpDTO
	 * @throws SQLException
	 */
	public void InsertToSummaryDailyMovPrdGrpExp(Connection conn,
			ArrayList<OOSMovementProductGroupExpDTO> movPrdGrpExpCol)
			throws SQLException {
		PreparedStatement psmt = conn
				.prepareStatement(InsertSqlForSummaryDailyMovPrdGrpExp());

		for (OOSMovementProductGroupExpDTO movPrdGrpExp : movPrdGrpExpCol) {
			psmt.setObject(1, movPrdGrpExp.storeId);
			psmt.setObject(2, movPrdGrpExp.dayOfWeek);
			psmt.setObject(3, movPrdGrpExp.timeOfDay);
			psmt.setObject(4, movPrdGrpExp.productLevelId);
			psmt.setObject(5, movPrdGrpExp.productId);
			psmt.setObject(6, movPrdGrpExp.expRevenue);
			psmt.setObject(7, movPrdGrpExp.expQuantity);
			psmt.addBatch();
		}

		psmt.executeBatch();		 
		conn.commit();
	}

	/**
	 * Forms insert query for the table SUMMARY_DAILY_MOV_PRDGRP_EXP
	 * @return
	 */
	private String InsertSqlForSummaryDailyMovPrdGrpExp() {

		StringBuffer Sql = new StringBuffer();

		Sql.append(" INSERT into SUMMARY_DAILY_MOV_PRDGRP_EXP (STORE_ID, DOW_ID, TOD_ID, PRODUCT_LEVEL_ID,PRODUCT_ID ");
		Sql.append(", EXP_REVENUE , EXP_QUANTITY ) ");
		Sql.append(" values (?,?,?,?,?,?,?)");

		return Sql.toString();
	}

	/**
	 * Gets the expected revenue of a Product Id from SUMMARY_DAILY_MOV_PRDGRP_EXP. 
	 * Returns 0  if expected revenue is not found for the Product Id
	 * @param conn
	 * @param storeId
	 * @param dayofweekId
	 * @param timeofDayId
	 * @param productLevelId
	 * @param productId
	 * @return
	 * @throws SQLException
	 * @throws GeneralException
	 */
	public double getExpRevOfProductId(Connection conn, int storeId, int dayofweekId, int timeofDayId, int productLevelId, int productId ) throws SQLException, GeneralException{
		
		double expRevenue = 0;
		StringBuffer sb = new StringBuffer();
		sb.append(" SELECT EXP_REVENUE FROM SUMMARY_DAILY_MOV_PRDGRP_EXP ");
		sb.append(" WHERE STORE_ID =  ").append(storeId);
		sb.append(" AND DOW_ID = ").append(dayofweekId);
		sb.append(" AND TOD_ID = ").append(timeofDayId);
		sb.append(" AND PRODUCT_LEVEL_ID = ").append(productLevelId);
		sb.append(" AND PRODUCT_ID = ").append(productId);
		
		String result = PristineDBUtil.getSingleColumnVal(conn, sb,
				"getExpRevOfProductId");
		if(result != null)
		expRevenue = Double.parseDouble(result);
		return expRevenue;		
	}
	
	/**
	 * Gets the Segment id of the item. Returns 0 if segment id is not found for the item
	 * @param conn
	 * @param itemCode
	 * @return
	 * @throws GeneralException
	 */
	public int getSegmentIdOfItem(Connection conn,int itemCode) throws GeneralException
	{
		int segmentId = 0;
		StringBuffer sb = new StringBuffer();
		sb.append(" SELECT SEGMENT_ID FROM ITEM_LOOKUP WHERE ITEM_CODE = ").append(itemCode);
		String result = PristineDBUtil.getSingleColumnVal(conn, sb,
				"getExpRevOfProductId");
		if(result != null)
			segmentId = Integer.parseInt(result);
		return segmentId;
	}
	
	/**
	 * Gets all the items of a segment.
	 * @param conn
	 * @param segmentId
	 * @param checkListId
	 * @return
	 * @throws GeneralException
	 * @throws SQLException
	 */
	public String getAllItemsOfSegment(Connection conn, int segmentId, int checkListId) throws GeneralException, SQLException
	{
		StringBuffer sb = new StringBuffer();
		String items = "";
		
		sb.append(" SELECT IL.ITEM_CODE AS ITEM_CODE ");
		sb.append(" FROM ITEM_LOOKUP IL, PRICE_CHECK_LIST_ITEMS PC ");
		sb.append(
				" WHERE  IL.ITEM_CODE = PC.ITEM_CODE AND PC.PRICE_CHECK_LIST_ID = ")
				.append(checkListId);
		sb.append(" AND IL.SEGMENT_ID = ").append(segmentId);
		
		CachedRowSet result = PristineDBUtil.executeQuery(conn, sb,
				"getAllItemsOfSegment");
		while (result.next()) {
			if(items == "")
			{
				items =result.getString("ITEM_CODE"); 
			}
			else
			{
				items = items + "," + result.getString("ITEM_CODE");
			}
			
		}				
		return items;		 
	}
	
	
	/**
	 * Calculate the sum of actual revenue of all the items
	 * @param conn
	 * @param scheduleId
	 * @param dayofweekId
	 * @param timeofDayId
	 * @param items
	 * @return
	 * @throws GeneralException
	 * @throws SQLException 
	 */
	public double getActualRevenueOfItems(Connection conn, int scheduleId, int dayofweekId, int timeofDayId, String items ) throws GeneralException, SQLException
	{
		double totalRevenue = 0;
		StringBuffer sb = new StringBuffer();
		 
		sb.append(" SELECT -1 DUMMYCOLUMN, SUM(REV_TOTAL) AS REV_TOTAL FROM SUMMARY_DAILY_MOV ");
		sb.append(" WHERE SCHEDULE_ID = ").append(scheduleId);
		sb.append(" AND DOW_ID = ").append(dayofweekId);
		sb.append(" AND TOD_ID = ").append(timeofDayId);
		sb.append(" AND ITEM_CODE IN (").append(items).append(")");
		
		CachedRowSet result = PristineDBUtil.executeQuery(conn, sb,
				"getActualRevenueOfItems");
		while(result.next())
		{
		totalRevenue =   result.getDouble("REV_TOTAL");
		}
		return totalRevenue;
	}
	
	
	/**
	 * Performs batch insert
	 * @param conn- Sql Connection 
	 * @param oosSummaryCollection - Collection of OOSSummaryDTO
	 * @throws SQLException
	 */
	public void InsertToSummaryDailyMov(Connection conn,
			ArrayList<OOSSummaryDTO> oosSummaryCollection)
			throws SQLException {
		PreparedStatement psmt = conn
				.prepareStatement(InsertSqlForSummaryDailyMov());

		for (OOSSummaryDTO oosSummary : oosSummaryCollection) {
			psmt.setObject(1, oosSummary.scheduleId);
			psmt.setObject(2, oosSummary.dayofweekId);
			psmt.setObject(3, oosSummary.specialEventId);
			psmt.setObject(4, oosSummary.specialFactId);
			psmt.setObject(5, oosSummary.timeOfDayId);
			psmt.setObject(6, oosSummary.itemCode);
			psmt.setObject(7, oosSummary.totalQty);
			psmt.setObject(8, oosSummary.regQty);
			psmt.setObject(9, oosSummary.saleQty);
			psmt.setObject(10, oosSummary.totalRev);
			psmt.setObject(11, oosSummary.regRev);
			psmt.setObject(12, oosSummary.saleRev);
			psmt.setObject(13, oosSummary.margin);
			
			psmt.setObject(14, (oosSummary.onSale) ? "'Y'" : "'N'");
			
			psmt.setObject(15, oosSummary.visitCount);
			psmt.setObject(16, oosSummary.maxDurationBtwVisit);
			psmt.setObject(17, oosSummary.minDurationBtwVisit);
			psmt.setObject(18, oosSummary.avgDurationBtwVisit);
			psmt.setObject(19, oosSummary.max95PctDurationBtwVisit);
			psmt.setObject(20, oosSummary.min95PctDurationBtwVisit);
			psmt.setObject(21, oosSummary.avgQtyPerVisit);
			
			if (oosSummary.modalQtyPerVisit > 0)
				psmt.setObject(22, oosSummary.modalQtyPerVisit);
			else
				psmt.setObject(22, "");
			
			if (oosSummary.modal2QtyPerVisit > 0)
				psmt.setObject(23, oosSummary.modal2QtyPerVisit);
			else
				psmt.setObject(23, "");
				
			psmt.setObject(24, oosSummary.maxQtyPerVisit);	
			psmt.setObject(25,NumberUtil.RoundFloat(oosSummary.movVisitRatio, 4));
			psmt.setObject(26,NumberUtil.RoundFloat(oosSummary.unitRegPrice, 2));
			psmt.setObject(27,NumberUtil.RoundFloat(oosSummary.unitSalePrice, 2));

			 

			if (oosSummary.marginOpp > 0)
				psmt.setObject(28, oosSummary.marginOpp);				
			else
				psmt.setObject(28, "");

			if (oosSummary.revOpp > 0)
				psmt.setObject(29, oosSummary.revOpp);				
			else
				psmt.setObject(29, "");

			psmt.setObject(30, oosSummary.totalCustVisitCount);	
			psmt.setObject(31, "'" +  oosSummary.ossInd + "'");
			
			
			psmt.addBatch();
		}

		psmt.executeBatch();		 
		//conn.commit();
	}

	/**
	 * Forms insert query for the table SUMMARY_DAILY_MOV
	 * @return
	 */
	private String InsertSqlForSummaryDailyMov() {

		StringBuffer sb = new StringBuffer();

		sb.append(" INSERT INTO SUMMARY_DAILY_MOV ( ");
		sb.append(" SCHEDULE_ID, DOW_ID, SPECIAL_EVENT, SPECIAL_FACT, TOD_ID, ");
		sb.append(" ITEM_CODE, QUANTITY_TOTAL, QUANTITY_REG, QUANTITY_SALE, ");
		sb.append(" REV_TOTAL, REV_REG, REV_SALE, MARGIN, PROMOTIONAL_IND, VISIT_COUNT, ");
		sb.append(" MAX_DURATION,  MIN_DURATION, AVG_DURATION, MAX_DURATION_95PCT, MIN_DURATION_95PCT, ");
		sb.append(" AVG_QTY_VISIT, MODAL_QTY_VISIT, MODAL_QTY2_VISIT, MAX_QTY_VISIT,");
		sb.append(" MOV_VISIT_RATIO, UNIT_REG_PRICE, UNIT_SALE_PRICE, ");
		sb.append(" MARGIN_OPP, REV_OPP, TOTAL_VISIT_COUNT, OOS_IND, OOS_CLASSIFICATION_ID, OOS_SCORE ) ");
		sb.append(" values (?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)");

		return sb.toString();
	}

	/**
	 * Performs batch insert
	 * @param conn- Sql Connection 
	 * @param oosSummaryCollection - Collection of OOSSummaryDTO
	 * @throws SQLException
	 */
	public void updateOOSClassificationSummaryDailyMov(Connection conn,
			ArrayList<OOSSummaryDTO> oosSummaryCollection)
			throws SQLException {
		String sql = new String(" UPDATE SUMMARY_DAILY_MOV SET OOS_CLASSIFICATION = ?, OOS_SCORE = ? WHERE SCHEDULE_ID = ? AND DOW_ID=? and TOD_ID = ? and ITEM_CODE =? ");

		PreparedStatement psmt = conn
				.prepareStatement(sql);

		for (OOSSummaryDTO oosSummary : oosSummaryCollection) {
			psmt.setObject(1, oosSummary.ossClassification);
			psmt.setObject(2, oosSummary.ossScore);
			psmt.setObject(3, oosSummary.scheduleId);
			psmt.setObject(4, oosSummary.dayofweekId);
			psmt.setObject(5, oosSummary.timeOfDayId);
			psmt.setObject(6, oosSummary.itemCode);
			 
			psmt.addBatch();
		}

		psmt.executeBatch();		 
		//conn.commit();
	}

	
	public void loadMovementDataCache(Connection conn,String storeNum, int calendarId, boolean isArchive) throws GeneralException{
		// load movement for store and calendar , order by item_code
		// load it into a hash based on item_code
		
		StringBuffer buffer = new StringBuffer();
		buffer.append(" SELECT ITEM_CODE, COMP_STR_NO, UPC, POS_TIMESTAMP,TO_CHAR( POS_TIMESTAMP, 'MM/DD/YYYY HH:MI AM') TRAN_TIMESTAMP, ");
		buffer.append(" UNIT_PRICE, EXTENDED_GROSS_PRICE,SALE_FLAG, QUANTITY, WEIGHT, "); 
		buffer.append(" PRICE,  CUSTOMER_CARD_NO, TRANSACTION_NO ");
		if( isArchive)
			buffer.append(" FROM MOVEMENT_DAILY_ARCHIVE ");
		else
			buffer.append(" FROM "+TABLE_NAME_MOVEMENT_DAILY+" ");
		buffer.append(" WHERE COMP_STR_NO = '").append(storeNum).append("'");
		buffer.append(" AND CALENDAR_ID ="+calendarId );
//		buffer.append(" AND ITEM_CODE =893172" );  // for DEBUG
//		buffer.append(" AND customer_card_no = '046102014189'");  // for DEBUG
		logger.debug( buffer.toString());
		CachedRowSet result = PristineDBUtil.executeQuery(conn, buffer, "getMovementDataForDate");

		movementCache = new Hashtable<Integer,ArrayList<MovementDTO>>();
		
		int ctr=0;
		try {
			while (result.next()){
				ctr++;
				MovementDTO mDTO = prepareMovementRecord(result);
				Integer itemCode = mDTO.getItemCode();
				if(!movementCache.containsKey(itemCode)){
					movementCache.put(itemCode, new ArrayList<MovementDTO>());
				}
				
				List<MovementDTO> mDTOL = movementCache.get(itemCode);
				mDTOL.add(mDTO);
			}
		}catch( SQLException sqle){
			throw new GeneralException("Error in accessing cachedRow set", sqle);
		}

		logger.debug("Loaded movement cache: ctr:"+ctr+"count:"+movementCache.size());
		
	}

	public void clearMovementDataCache() {
		// TODO Auto-generated method stub
		
	}

	
	public ArrayList<ScheduleInfoDTO> getSchedulesForStore( Connection conn, int storeId, int dowId )throws GeneralException {
		
		ScheduleInfoDTO schInfo = null;
		CachedRowSet crs = null;
		StringBuffer sb = new StringBuffer();

		sb.append(" SELECT DISTINCT A.schedule_id, A.start_date  begin_date, TO_CHAR(A.start_date, 'MM/DD/YYYY') START_DATE, ");
		sb.append(" TO_CHAR(A.end_date, 'MM/DD/YYYY') END_DATE, ");
		sb.append(" A.comp_str_id ");
		sb.append(" From schedule A,  SUMMARY_DAILY_MOV B WHERE A.comp_str_id = ");
		sb.append(storeId);
		sb.append(" and B.tod_id = 4 "); // for all day
		if( dowId > 0){
			sb.append(" and B.dow_id =  " + dowId); 
		}
		sb.append(" and B.TOTAL_VISIT_COUNT  > 100 "); //assumes if the store is open, there will be at least 10 visits per day
		sb.append(" and B.schedule_id = A.schedule_id ");
		sb.append(" order by begin_date ");
		logger.debug(sb.toString());
		crs = PristineDBUtil.executeQuery(conn, sb, "getSchedulesForStore");
		ArrayList <ScheduleInfoDTO> schList = new ArrayList <ScheduleInfoDTO> ();
		try {
			while (crs.next()){
				schInfo = new ScheduleInfoDTO();
				schInfo.setScheduleId(crs.getInt("schedule_id"));
				schInfo.setStoreId(crs.getInt("comp_str_id"));
				schInfo.setStartDate(crs.getString("START_DATE"));
				schInfo.setEndDate(crs.getString("END_DATE"));
				schList.add(schInfo);
			}
		}catch (SQLException sqlce){
			throw new GeneralException( "Accessing cached Rowset exception", sqlce);
		}
		
		return schList;
	}

	public int getNoOfSaleObjs(Connection conn, String schIdList,
			int itemCode, int dow) throws GeneralException {
		// TODO Auto-generated method stub
		StringBuffer sb = new StringBuffer();

		sb.append(" SELECT COUNT( schedule_id ) FROM COMPETITIVE_DATA");
		sb.append(" WHERE SCHEDULE_ID in (").append(schIdList).append(")") ;
		sb.append(" AND ITEM_CODE = ").append(itemCode) ;
		sb.append(" AND PROMOTION_FLG = 'Y'") ;
		
		String strSaleObjs = PristineDBUtil.getSingleColumnVal( conn, sb, "GetNoOfSaleObs");
		return Integer.parseInt(strSaleObjs);
		
	}


	
	
	public Hashtable loadTotalVisitCountCache(Connection conn,int storeid) throws GeneralException{
		// load movement for store and calendar , order by item_code
		// load it into a hash based on item_code
		
		StringBuffer buffer = new StringBuffer();
		buffer.append("select dow_id,tod_id, avg(total_visit_count) tvc" +
				" from (select distinct sd.schedule_id,dow_id, tod_id, total_visit_count " +
				" from summary_daily_mov sd, schedule s where sd.schedule_id = s.schedule_id and s.comp_str_id = "+storeid+ ") " +
				" group by dow_id,tod_id");
		logger.debug( buffer.toString());
		CachedRowSet result = PristineDBUtil.executeQuery(conn, buffer, "loadTotalVisitCountCache");

		Hashtable<String,Integer> tvcH = new Hashtable<String,Integer>();
		
		int ctr=0;
		try {
			while (result.next()){
				ctr++;
				Integer dowId = result.getInt("dow_id");
				Integer todId = result.getInt("tod_id");
				Integer tvc = (int) result.getDouble("tvc");
				tvcH.put(dowId+":"+todId, tvc);
			}
			logger.debug("Loaded movement cache: ctr:"+ctr+"count:"+tvcH.size());
			return tvcH;
		}catch( SQLException sqle){
			throw new GeneralException("Error in accessing cachedRow set", sqle);
		}
	}


	public CachedRowSet generateOOSReport(Connection conn,int storeId, int weekCalendarId, int dowId, boolean onlyOOS) {

		PreparedStatement pstmt= null;
		ResultSet s = null;
		CachedRowSet crs = null;
		try{
		String sql = new String(oosReportSQL);
		if(onlyOOS){
		sql = sql.replace("__OOS__", "Y");
		}else{
			sql = sql.replace("__OOS__", "N");
			
		}
		pstmt=conn.prepareStatement(sql);
		
		pstmt.setInt(1, weekCalendarId);
		pstmt.setInt(2, dowId);
		pstmt.setInt(3, storeId);
		pstmt.setInt(4, weekCalendarId);
		pstmt.setInt(5, weekCalendarId);
		
		crs = new CachedRowSetImpl();
		
		s = pstmt.executeQuery();
		crs.populate(s);
		logger.info("Result set size:"+crs.size());
		s.close();
		}catch(Exception e){
			logger.error("Exception generating OOS report :"+e.getMessage());
			e.printStackTrace();
			
		}
		finally{
			try {
				s.close();
			} catch (SQLException e) {
				// TODO Auto-generated catch block
				logger.error(e.getMessage());
				e.printStackTrace();
			}
		}
		return crs;
		
		
	}


	public HashMap<Integer,HashMap<Integer, Double>> loadPredictedExpCache(Connection conn,
			Integer storeId, int calendarId) throws GeneralException, Exception {
		StringBuffer sql = new StringBuffer("Select item_code,pred from PREDICTED_EXP where calendar_id = "+calendarId+" and comp_str_id = '"+storeId +"'");
		CachedRowSet pred = PristineDBUtil.executeQuery(conn, sql,
				"loadPredictedExpCache");
		HashMap<Integer,HashMap<Integer, Double>> predExp = new HashMap<Integer,HashMap<Integer, Double>>(); 
		try{
		while(pred.next()){
			Integer itemCode = pred.getInt(1);
			Double predValue = pred.getDouble(2);
			Integer tod = 4;
			if(predExp.containsKey(itemCode)){
				predExp.get(itemCode).put(tod, predValue);
			}else{
				HashMap<Integer, Double> predExpTOD = new HashMap<Integer, Double>();
				predExpTOD.put(tod, predValue);
				predExp.put(itemCode, predExpTOD);
			}
				
			
		}
		}catch(Exception e){
			throw new Exception(e.getMessage());
		}

		return predExp;
	}

	public HashMap<Integer,HashMap<Integer, Double>> loadPredictedExpFocusCache(Connection conn,
			Integer storeId, int calendarId) throws GeneralException, Exception {
		StringBuffer sql = new StringBuffer("Select item_code,pred, tod_id from PREDICTED_EXP_FOCUS where calendar_id = "+calendarId+" and comp_str_id = '"+storeId +"'");
		CachedRowSet pred = PristineDBUtil.executeQuery(conn, sql,
				"loadPredictedExpCache");
		HashMap<Integer,HashMap<Integer, Double>> predExp = new HashMap<Integer,HashMap<Integer, Double>>(); 
		try{
		while(pred.next()){
			Integer itemCode = pred.getInt(1);
			Double predValue = pred.getDouble(2);
			Integer tod = pred.getInt(3);
			if(predExp.containsKey(itemCode)){
				predExp.get(itemCode).put(tod, predValue);
			}else{
				HashMap<Integer, Double> predExpTOD = new HashMap<Integer, Double>();
				predExpTOD.put(tod, predValue);
				predExp.put(itemCode, predExpTOD);
			}
				
			
		}
		}catch(Exception e){
			throw new Exception(e.getMessage());
		}

		return predExp;
	}

	public HashMap<Integer,String> loadLastMovementDateTime(Connection conn,
			Integer storeId, String analysisDate) throws GeneralException, Exception {
		String sql = new String(lastMovementSQL);
		sql = sql.replace("__GE__", " >= ");
		sql = sql.replace("__LT__", " < ");
		sql = sql.replace("__THE_STORE_ID__", " "+storeId +" ");
		sql = sql.replace("__ANALYSIS_DATE__", analysisDate);

		
		
		CachedRowSet lastMovement = PristineDBUtil.executeQuery(conn, new StringBuffer(sql),
				"loadLastMovmentCache");
		HashMap<Integer,String> lastMov = new HashMap<Integer,String>(); 
		try{
		while(lastMovement.next()){
			Integer itemCode = lastMovement.getInt(1);
			String lM  = lastMovement.getString(2);
			lastMov.put(itemCode, lM);
		}
		}catch(Exception e){
			throw new Exception(e.getMessage());
		}

		return lastMov;
	}



	public HashMap<Integer,HashMap<Integer, Double>> loadPredictedExpHighMoverCache(Connection conn,
			Integer storeId, int calendarId) throws GeneralException, Exception {
		StringBuffer sql = new StringBuffer("Select item_code,pred, tod_id from PREDICTED_EXP_FOCUS where calendar_id = "+calendarId+" and comp_str_id = '"+storeId +"' and HIGH_MOVER ='Y'");
		CachedRowSet pred = PristineDBUtil.executeQuery(conn, sql,
				"loadPredictedExpHighMoverCache");
		HashMap<Integer,HashMap<Integer, Double>> predExp = new HashMap<Integer,HashMap<Integer, Double>>(); 
		try{
		while(pred.next()){
			Integer itemCode = pred.getInt(1);
			Double predValue = pred.getDouble(2);
			Integer tod = pred.getInt(3);
			if(predExp.containsKey(itemCode)){
				predExp.get(itemCode).put(tod, predValue);
			}else{
				HashMap<Integer, Double> predExpTOD = new HashMap<Integer, Double>();
				predExpTOD.put(tod, predValue);
				predExp.put(itemCode, predExpTOD);
			}
				
			
		}
		}catch(Exception e){
			throw new Exception(e.getMessage());
		}

		return predExp;
	}

	
	
	public HashMap<Integer, HashSet<OOSPredictedMovementDTO>> loadPredictedMovementCache(Connection conn,
			int storeId, int calendarWeekId, int dow) throws GeneralException, Exception {
		StringBuffer sql = new StringBuffer("Select " +
				"TOD_ID,ITEM_CODE,LAST_7DAY_EXP,LAST_7DAY_OBS,LAST_7DAY_OBS_USED," +
				"LAST_28DAY_EXP,LAST_28DAY_OBS,LAST_28DAY_OBS_USED," +
				"LAST_180DAY_EXP,LAST_180DAY_OBS,LAST_180DAY_OBS_USED," +
				"PREV_DAY_EXP,OOS_METHOD,LAST_7DAY_DAYS_MOVED," +
				"LAST_28DAY_DAYS_MOVED,LAST_180DAY_DAYS_MOVED,MAPE,SIGMA_USED,LAST_7DAY_AVG_EXP,LAST_7DAY_AVG_OBS,LAST_7DAY_AVG_OBS_USED,PREV_DAY_EXP,STATS_EXP,COMP_STR_ID,CALENDAR_ID,DOW_ID," +
				"AD_FACTOR_EXP,AD_FACTOR_METHOD,AD_FACTOR_SIGMA " +
				"from PREDICTED_MOVEMENT where calendar_id = "+calendarWeekId+" and comp_str_id = "+storeId +" and dow_id ="+dow);
		CachedRowSet pred = PristineDBUtil.executeQuery(conn, sql,
				"loadPredictedExpCache");
		
		HashMap<Integer, HashSet<OOSPredictedMovementDTO>> predictedMov = new HashMap<Integer, HashSet<OOSPredictedMovementDTO>>(); 
		try{
		while(pred.next()){
			OOSPredictedMovementDTO pm = new OOSPredictedMovementDTO();
			pm.itemCode = pred.getInt("ITEM_CODE");
			pm.timeOfDayId = pred.getInt("TOD_ID");
			
			pm.last7dayDaysMoved = pred.getInt("LAST_7DAY_DAYS_MOVED");
			pm.last7dayobs = pred.getInt("LAST_7DAY_OBS");
			pm.last7dayobsUsed = pred.getInt("LAST_7DAY_OBS_USED");
			pm.last7dayexp = pred.getDouble("LAST_7DAY_EXP");
//			pm.last7daysigma = pred.getDouble("LAST_7DAY_SIGMA");

			pm.last28dayDaysMoved = pred.getInt("LAST_28DAY_DAYS_MOVED");
			pm.last28dayobs = pred.getInt("LAST_28DAY_OBS");
			pm.last28dayobsUsed = pred.getInt("LAST_28DAY_OBS_USED");
			pm.last28dayexp = pred.getDouble("LAST_28DAY_EXP");
//			pm.last28daysigma = pred.getDouble("LAST_28DAY_SIGMA");
			
			pm.last180dayDaysMoved = pred.getInt("LAST_180DAY_DAYS_MOVED");
			pm.last180dayobs = pred.getInt("LAST_180DAY_OBS");
			pm.last180dayobsUsed = pred.getInt("LAST_180DAY_OBS_USED");
			pm.last180dayexp = pred.getDouble("LAST_180DAY_EXP");
//			pm.last180daysigma = pred.getDouble("LAST_180DAY_SIGMA");
			
			pm.prevdayexp = pred.getDouble("PREV_DAY_EXP");
			
			pm.last7dayavgobs = pred.getInt("LAST_7DAY_AVG_OBS");
			pm.last7dayavgobsUsed = pred.getInt("LAST_7DAY_AVG_OBS_USED");
			pm.last7dayavgexp = pred.getDouble("LAST_7DAY_AVG_EXP");
//			pm.last7dayavgsigma = pred.getDouble("LAST_7DAY_AVG_SIGMA");
			
			pm.expWithAd = pred.getDouble("AD_FACTOR_EXP");
			pm.expMethodWithAd = pred.getString("AD_FACTOR_METHOD");
			pm.sigmaWithAd = pred.getDouble("AD_FACTOR_SIGMA");
			
			pm.sigmaUsed = pred.getDouble("SIGMA_USED");
			if(predictedMov.containsKey(pm.itemCode)){
				predictedMov.get(pm.itemCode).add(pm);
			}else{
				HashSet<OOSPredictedMovementDTO> itemCodeLevelPred = new HashSet<OOSPredictedMovementDTO>();
				itemCodeLevelPred.add(pm);
				predictedMov.put(pm.itemCode, itemCodeLevelPred);
			}
			
		}
		}catch(Exception e){
			throw new Exception(e.getMessage());
		}

		return predictedMov;
	}

	
	public HashMap<Integer, String> loadWeeklyAdCache(Connection conn,
			String storeNum, int calendarId) throws GeneralException, Exception {
		String adSQL = weeklyAdSQL.replace("__WEEKLY_CAL_ID__", calendarId+"");
		StringBuffer sql = new StringBuffer(adSQL);
		CachedRowSet pred = PristineDBUtil.executeQuery(conn, sql,
				"loadweeklyAdCache");
		HashMap<Integer, String> weeklyAd = new HashMap<Integer, String>(); 
		try{
		while(pred.next()){
			Integer itemCode = pred.getInt(1);
			String displayType = pred.getString(2);
			if (displayType == null){
				displayType = "-";
			}
			weeklyAd.put(itemCode, displayType.toLowerCase());
			
		}
		}catch(Exception e){
			throw new Exception(e.getMessage());
		}

		return weeklyAd;
	}


	public CachedRowSet getItemsForSubstituteAnalysis(Connection conn,
			Integer storeId, int categoryId, int subcategoryId, String analysisDate, String listType) throws GeneralException, Exception {
		
		StringBuffer sql = new StringBuffer("select il.ret_lir_id, ret_lir_name, item_code, item_name, " +
		"cat_name, sub_category_id, sub_cat_name, segment_name, segment_id, uom_name uom_nm, item_size it_size, pr from item_lookup_view il, " +
		"(select item_id, pr from " +
		"( " +
		"select count(calendar_id) days_moved, item_id, avg(price) pr " +
		"from( " +
		  "select calendar_id, item_id, sum(quantity) mvt, avg(unit_price) price " +
		    "from ( " +
		      "select item_id , t.calendar_id wcal_id ,quantity, unit_price, rc.calendar_id ");
		
		sql.append("from (select tl.*, il.dept_id from transaction_log tl inner join item_lookup il on tl.item_id = il.item_code " + 
				" where active_indicator = 'Y' ");
		if ( (PropertyManager.getProperty("SA_ELIGIBLE_DEPARTMENT_IDS") != "ALL") && (PropertyManager.getProperty("SA_ELIGIBLE_DEPARTMENT_IDS") != "") )
			sql.append("and dept_id In (" + PropertyManager.getProperty("SA_ELIGIBLE_DEPARTMENT_IDS") + ") ");

		if (subcategoryId >= 0)
		{
			sql.append(" AND sub_category_id = " + subcategoryId + ") t, ");
		}
		else if (categoryId >= 0)
		{
			sql.append("AND category_id = " + categoryId + ") t, ");
		}
		else
		{
			sql.append(") t, ");
		}		
		
		sql.append("retail_calendar rc where t.calendar_id in (select calendar_id from retail_calendar where start_date >= to_date('" + analysisDate + "','MM/dd/yyyy') - " + PropertyManager.getProperty("SA_PERIOD_DAYS_BEFORE_FOR_ITEM") + " " + 
		      " and start_date < to_date('" + analysisDate + "','MM/dd/yyyy') and row_type ='D') " +
		          "and store_id =  " + storeId); 

		sql.append("and t.trx_time >= rc.start_date and t.trx_time <= rc.end_date and rc.row_type='W' " + 
		    ")group by calendar_id, item_id " +
		") where mvt  >= " + PropertyManager.getProperty("SA_MINIMUM_MOVED_QUANTITY_FOR_ITEM") +  " " +
		" group by item_id " +
		")   " +
		"where days_moved  >=  " + PropertyManager.getProperty("SA_MINIMUM_MOVED_DAYS_FOR_ITEM") +  " " +
		" ) A  " +
		"where il.item_code = A.item_id " + 
		"and il.active_indicator='Y' ");		//and ret_lir_id is null  REMOVED to enable all lirs treated as items.  
		
		logger.debug(sql);
		CachedRowSet iList = null;
		try{
		iList = PristineDBUtil.executeQuery(conn, new StringBuffer(sql),
				"getItemsForSubstituteAnalysis");
		
		}catch(Exception e){
			throw new Exception(e.getMessage());
		}

		return iList;
	}


	public CachedRowSet getLIRsForSubstituteAnalysis(Connection conn,
			Integer storeId, int categoryId, int subcategoryId, String analysisDate, String listType) throws GeneralException, Exception {

		StringBuffer sql = new StringBuffer("select ret_lir_id, sub_category_id, segment_id, uom_nm, it_size, price pr from" + 
		"(select ret_lir_id, sub_category_id, segment_id, count(calendar_id) cc,  wm_concat(distinct uom) uom_nm, wm_concat(distinct isize) it_size, avg(pr) price " +
		"from " + 
		"(select * from  " +
		"(select ret_lir_id, sub_category_id, segment_id, rc.calendar_id, count(trx_no) tcount , wm_concat(distinct uom_name) uom, " +
		 "wm_concat(distinct item_size) isize, avg(unit_price) pr ");
		sql.append("from (select tl.*, il.dept_id , il.sub_category_id, il.ret_lir_id, il.segment_id, il.item_size, uom_lookup.name uom_name " +
		 "from transaction_log tl inner join item_lookup il on tl.item_id = il.item_code " + 
				"  left join uom_lookup on il.uom_id = uom_lookup.id " +
				" where active_indicator = 'Y' ");
		if ( (PropertyManager.getProperty("SA_ELIGIBLE_DEPARTMENT_IDS") != "ALL") && (PropertyManager.getProperty("SA_ELIGIBLE_DEPARTMENT_IDS") != "") )
			sql.append("and dept_id In (" + PropertyManager.getProperty("SA_ELIGIBLE_DEPARTMENT_IDS") + ") ");

		if (subcategoryId >= 0)
		{
			sql.append(" AND sub_category_id = " + subcategoryId + ") t, ");
		}
		else if (categoryId >= 0)
		{
			sql.append("AND category_id = " + categoryId + ") t, ");
		}
		else
		{
			sql.append(") t, ");
		}		
		
		sql.append(" retail_calendar rc " + 
		    "where t.calendar_id in (select calendar_id from retail_calendar where start_date >= to_date('" + analysisDate + "','MM/dd/yyyy') - " + PropertyManager.getProperty("SA_PERIOD_DAYS_BEFORE_FOR_LIR") + " " +
		      "and start_date < to_date('" + analysisDate + "','MM/dd/yyyy') and row_type ='D')		  " +
		       "and store_id = " + +storeId);

		sql.append(" and ret_lir_id is not null " +
		       "and t.trx_time >= rc.start_date and t.trx_time <= rc.end_date and rc.row_type='W' " +
		"group by ret_lir_id, sub_category_id, segment_id, rc.calendar_id) " +
		"where tcount > " + PropertyManager.getProperty("SA_MINIMUM_TRX_FOR_LIR") +  ") " +
		"group by ret_lir_id, sub_category_id, segment_id ) " +
		"where cc > " + PropertyManager.getProperty("SA_MINIMUM_MOVED_DAYS_FOR_LIR") +  " " +
		"order by ret_lir_id");
				
		logger.debug(sql);
		CachedRowSet iList = null;
		try{
		iList = PristineDBUtil.executeQuery(conn, new StringBuffer(sql),
				"getLIRsForSubstituteAnalysis");
		
		}catch(Exception e){
			throw new Exception(e.getMessage());
		}

		return iList;
	}


	public Hashtable<String,Integer> getSubsitutionAnalysisMetricForItem(Connection conn, Integer a, Integer b, Integer storeId,String analysisDate) throws GeneralException {
		// TODO Auto-generated method stub
		StringBuffer sb = new StringBuffer();
		Hashtable<String,Integer> result = new Hashtable<String,Integer>();
		String strResult ="-1";
		String sql = "select count(*) from (SELECT distinct trx_no ct, calendar_id from transaction_log where " +
				"store_id = "+storeId+" and calendar_id IN (select calendar_id from retail_calendar where start_date > to_date('"+analysisDate+"','MM/dd/yyyy')-180)" +
						" and (item_id =  "+a+" or item_id ="+b+"))";
		
		strResult = PristineDBUtil.getSingleColumnVal( conn, new StringBuffer(sql), "substitueAnalysis");
		result.put("AorB", Integer.parseInt(strResult));
		
		
		
		sql = 		"select count(*) from " +
				"(select count(item_id) ct, trx_no from (" +
				"SELECT distinct trx_no, calendar_id, item_id from transaction_log t " +
				" where store_id = "+storeId+" and calendar_id IN(select calendar_id from retail_calendar where start_date > to_date('"+analysisDate+"','MM/dd/yyyy')-180)" +
						" and (item_id ="+a+" or item_id ="+b+")) group by trx_no) where ct=2";
		strResult = PristineDBUtil.getSingleColumnVal( conn, new StringBuffer(sql), "substitueAnalysis");
		result.put("AandB", Integer.parseInt(strResult));
		

		
		return result;
		
	}


	public Hashtable<String,Integer> getSubsitutionAnalysisMetricForLIG(Connection conn, Integer a, Integer b, Integer segId, Integer storeId,String analysisDate) throws GeneralException {
		// TODO Auto-generated method stub
		StringBuffer sb = new StringBuffer();
		Hashtable<String,Integer> result = new Hashtable<String,Integer>();
		String strResult ="-1";
		
		String sql = "select count(*) from (SELECT distinct trx_no, calendar_id from transaction_log where " +
				"store_id = "+storeId+" and calendar_id IN (select calendar_id from retail_calendar where start_date > to_date('"+analysisDate+"','MM/dd/yyyy')-180)" +
						" and (item_id in(select item_code from item_lookup where ret_lir_id =  "+a
						+" and segment_id = "+segId
						+") or " +
								"item_id IN (select item_code from item_lookup where ret_lir_id ="+b
								+" and segment_id = "+segId+"" 
								+" )))";
		strResult = PristineDBUtil.getSingleColumnVal( conn, new StringBuffer(sql), "substitueAnalysis");
		result.put("AorB", Integer.parseInt(strResult));
		
		
		
		sql = 		"select count(*) from " +
				"(select count(ret_lir_id) ct, trx_no from (" +
				"SELECT distinct trx_no, calendar_id, ret_lir_id from transaction_log t, item_lookup il where t.item_id = il.item_code " +
				" and store_id = "+storeId+" and calendar_id IN(select calendar_id from retail_calendar where start_date > to_date('"+analysisDate+"','MM/dd/yyyy')-180)" +
						" and (item_id IN  (select item_code from item_lookup where ret_lir_id ="+a+
						"  and segment_id = "+segId+"" +
								") or item_id IN (select item_code from item_lookup where ret_lir_id ="+b+
								"  and segment_id = "+segId+"" +
										"))) group by trx_no) where ct=2";
		strResult = PristineDBUtil.getSingleColumnVal( conn, new StringBuffer(sql), "substitueAnalysis");
		result.put("AandB", Integer.parseInt(strResult));
		

		
		return result;
		
	}


	public Hashtable<String,Hashtable<Integer,Integer>> getCustomerWiseSubsitutionAnalysisMetricForItem(Connection conn, Integer a, Integer b, Integer storeId,String analysisDate) throws GeneralException {
		// TODO Auto-generated method stub
		StringBuffer sb = new StringBuffer();
		Hashtable<String,Hashtable<Integer,Integer>> result = new Hashtable<String,Hashtable<Integer,Integer>>();

		// A
		String sql = "select customer_id, count(*) ct from (SELECT distinct trx_no ct, calendar_id, customer_id from transaction_log where " +
				"store_id = "+storeId+" and calendar_id IN (select calendar_id from retail_calendar where start_date > to_date('"+analysisDate+"','MM/dd/yyyy')-180)" +
						" and (item_id =  "+a+ ")) group by customer_id order by ct desc";
		
		Hashtable<Integer,Integer> ahash = new Hashtable<Integer,Integer>();
		CachedRowSet res = PristineDBUtil.executeQuery(conn, new StringBuffer(sql), "substitueAnalysis");
		try{
		while(res.next()){
				ahash.put(res.getInt("customer_id"), res.getInt("ct"));
		}
		}
		catch(Exception e){
			throw new GeneralException(e.getMessage());
		}
		result.put("A", ahash);

		
		// B
		sql = "select customer_id, count(*) ct from (SELECT distinct trx_no ct, calendar_id, customer_id from transaction_log where " +
				"store_id = "+storeId+" and calendar_id IN (select calendar_id from retail_calendar where start_date > to_date('"+analysisDate+"','MM/dd/yyyy')-180)" +
						" and (item_id ="+b+")) group by customer_id order by ct desc";
		
		Hashtable<Integer,Integer> bhash = new Hashtable<Integer,Integer>();
		res = PristineDBUtil.executeQuery(conn, new StringBuffer(sql), "substitueAnalysis");
		try{
		while(res.next()){
			bhash.put(res.getInt("customer_id"), res.getInt("ct"));
		}
		}
		catch(Exception e){
			throw new GeneralException(e.getMessage());
		}
		result.put("B", bhash);

		// A not B
		Hashtable<Integer,Integer> aNotbhash = new Hashtable<Integer,Integer>();
		Hashtable<Integer,Integer> aAsWellAsbhash = new Hashtable<Integer,Integer>();
		Enumeration<Integer> enumA = ahash.keys();
		while(enumA.hasMoreElements()){
			Integer cId = enumA.nextElement();
			if(!bhash.containsKey(cId)){
				aNotbhash.put(cId, 1);
			}else{
				aAsWellAsbhash.put(cId,1);

			}
		}
		result.put("AnotB", aNotbhash);
		result.put("AasWellasB", aAsWellAsbhash);
		
		// A or B
		sql = "select customer_id, count(*) ct from (SELECT distinct trx_no ct, calendar_id, customer_id from transaction_log where " +
				"store_id = "+storeId+" and calendar_id IN (select calendar_id from retail_calendar where start_date > to_date('"+analysisDate+"','MM/dd/yyyy')-180)" +
						" and (item_id =  "+a+" or item_id ="+b+")) group by customer_id order by ct desc";
		
		Hashtable<Integer,Integer> aOrBhash = new Hashtable<Integer,Integer>();
		res = PristineDBUtil.executeQuery(conn, new StringBuffer(sql), "substitueAnalysis");
		try{
		while(res.next()){
			Integer cId =res.getInt("customer_id"); 
			aOrBhash.put(cId, res.getInt("ct"));
		}
		}
		catch(Exception e){
			throw new GeneralException(e.getMessage());
		}
		result.put("AorB", aOrBhash);
		
		
		// A and B
		sql = 		"Select tabA.customer_id customer_id, count(*) ct from" +
				" (Select customer_id,trx_no,calendar_id from transaction_log where store_id = "+storeId+" and item_id = "+a+" and calendar_id IN(select calendar_id from retail_calendar where start_date > to_date('"+analysisDate+"','MM/dd/yyyy')-180)) tabA," +
				" (Select customer_id,trx_no,calendar_id from transaction_log where store_id = "+storeId+" and item_id = "+b+" and calendar_id IN(select calendar_id from retail_calendar where start_date > to_date('"+analysisDate+"','MM/dd/yyyy')-180)) tabB " +
						"where tabA.trx_no = tabB.trx_no and tabA.calendar_id = tabB.calendar_id group by tabA.customer_id order by ct desc";
		Hashtable<Integer,Integer> aAndBhash = new Hashtable<Integer,Integer>();
		res = PristineDBUtil.executeQuery(conn, new StringBuffer(sql), "substitueAnalysis");
		try{
		while(res.next()){
			aAndBhash.put(res.getInt("customer_id"), res.getInt("ct"));
		}
		}
		catch(Exception e){
			throw new GeneralException(e.getMessage());
		}
		result.put("AandB", aAndBhash);
		
		return result;
		
	}


	public Hashtable<String,Hashtable<Integer,Integer>> getCustomerWiseSubsitutionAnalysisMetricForLIG(Connection conn, Integer a, Integer b, Integer segId, Integer storeId,String analysisDate) throws GeneralException {
		// TODO Auto-generated method stub
		StringBuffer sb = new StringBuffer();
		Hashtable<String,Hashtable<Integer,Integer>> result = new Hashtable<String,Hashtable<Integer,Integer>>();
		
		String sql="select customer_id, count(*) ct from " +
		 		"(SELECT distinct trx_no ct, calendar_id, customer_id from transaction_log tlog, item_lookup il where " +
		 		"calendar_id IN (select calendar_id from retail_calendar where start_date > to_date('"+analysisDate+"','MM/dd/yyyy')-180) and " +
		 				"store_id = "+storeId+" and tlog.item_id = il.item_code and  " +
		 						"(ret_lir_id =  "+a+" ) and segment_id = "+segId+" ) group by customer_id order by ct desc";
				
				Hashtable<Integer,Integer> ahash = new Hashtable<Integer,Integer>();
				CachedRowSet res = PristineDBUtil.executeQuery(conn, new StringBuffer(sql), "substitueAnalysis");
				try{
				while(res.next()){
						ahash.put(res.getInt("customer_id"), res.getInt("ct"));
				}
				}
				catch(Exception e){
					throw new GeneralException(e.getMessage());
				}
				result.put("A", ahash);

				if(a==38 && b==118)logger.info("A SQL:"+sql);
				
				sql="select customer_id, count(*) ct from " +
				 		"(SELECT distinct trx_no ct, calendar_id, customer_id from transaction_log tlog, item_lookup il where " +
				 		"calendar_id IN (select calendar_id from retail_calendar where start_date > to_date('"+analysisDate+"','MM/dd/yyyy')-180) and " +
				 				"store_id = "+storeId+" and tlog.item_id = il.item_code and  " +
				 						"(ret_lir_id =  "+b+" ) and segment_id = "+segId+" ) group by customer_id order by ct desc";
						
						Hashtable<Integer,Integer> bhash = new Hashtable<Integer,Integer>();
						res = PristineDBUtil.executeQuery(conn, new StringBuffer(sql), "substitueAnalysis");
						try{
						while(res.next()){
							bhash.put(res.getInt("customer_id"), res.getInt("ct"));
						}
						}
						catch(Exception e){
							throw new GeneralException(e.getMessage());
						}
						result.put("B", bhash);
						if(a==38 && b==118)logger.info("B SQL:"+sql);
				

						// A not B
						Hashtable<Integer,Integer> aNotbhash = new Hashtable<Integer,Integer>();
						Hashtable<Integer,Integer> aAsWellAsbhash = new Hashtable<Integer,Integer>();
						Enumeration<Integer> enumA = ahash.keys();
						while(enumA.hasMoreElements()){
							Integer cId = enumA.nextElement();
							if(!bhash.containsKey(cId)){
								aNotbhash.put(cId, 1);
							}else{
								aAsWellAsbhash.put(cId,1);
								
							}
						}
						result.put("AnotB", aNotbhash);
						result.put("AasWellasB", aAsWellAsbhash);
						
		
 sql="select customer_id, count(*) ct from " +
 		"(SELECT distinct trx_no ct, calendar_id, customer_id from transaction_log tlog, item_lookup il where " +
 		"calendar_id IN (select calendar_id from retail_calendar where start_date > to_date('"+analysisDate+"','MM/dd/yyyy')-180) and " +
 				"store_id = "+storeId+" and tlog.item_id = il.item_code and  " +
 						"(ret_lir_id =  "+a+" or ret_lir_id =  "+b+" ) and segment_id = "+segId+" ) group by customer_id order by ct desc";
		
		Hashtable<Integer,Integer> aOrBhash = new Hashtable<Integer,Integer>();
		res = PristineDBUtil.executeQuery(conn, new StringBuffer(sql), "substitueAnalysis");
		try{
		while(res.next()){
			Integer cId =res.getInt("customer_id"); 
			aOrBhash.put(cId, res.getInt("ct"));
		}
		}
		catch(Exception e){
			throw new GeneralException(e.getMessage());
		}
		result.put("AorB", aOrBhash);
		
		sql = "select customer_id, count(*) ct from (select customer_id,trx_no,calendar_id ,count(*) cct  from (" +
				"Select distinct customer_id,trx_no,calendar_id, ret_lir_id from transaction_log, item_lookup il where " +
				"calendar_id IN(select calendar_id from retail_calendar where start_date > to_date('"+analysisDate+"','MM/dd/yyyy')-180) " +
				"and store_id = "+storeId+" and item_id = il.item_code and (ret_lir_id = "+a+" or ret_lir_id = "+b+") and segment_id = 1196) " +
				"group by customer_id, trx_no,calendar_id) where cct = 2 group by customer_id";

		Hashtable<Integer,Integer> aAndBhash = new Hashtable<Integer,Integer>();
		res = PristineDBUtil.executeQuery(conn, new StringBuffer(sql), "substitueAnalysis");
		try{
		while(res.next()){
			aAndBhash.put(res.getInt("customer_id"), res.getInt("ct"));
		}
		}
		catch(Exception e){
			throw new GeneralException(e.getMessage());
		}
		result.put("AandB", aAndBhash);
		
		return result;
		
	}

	
	
	public int getTrxCountForItem(Connection conn, Integer itemA, Integer storeId, String analysisDate) throws GeneralException {
		// TODO Auto-generated method stub
		StringBuffer sb = new StringBuffer();

		sb = sb.append("select count(*) from (SELECT distinct trx_no, calendar_id ct from transaction_log where store_id = "+storeId+" " +
				"and calendar_id IN (select calendar_id from retail_calendar where start_date > to_date('"+analysisDate+"','MM/dd/yyyy')-180) and item_id =  "+itemA+")");
		
		String strResult = PristineDBUtil.getSingleColumnVal( conn, sb, "substitueAnalysis");
		return Integer.parseInt(strResult);
		
	}

	public int getTrxCountForLIG(Connection conn, Integer lir_id, Integer segId, Integer storeId, String analysisDate) throws GeneralException {
		// TODO Auto-generated method stub
		StringBuffer sb = new StringBuffer();

		sb = sb.append("select count(*) from (SELECT distinct trx_no, calendar_id ct from transaction_log where " +
				"calendar_id IN (select calendar_id from retail_calendar where start_date > to_date('"+analysisDate+"','MM/dd/yyyy')-180) " +
				"and store_id = "+storeId+"  and item_id in  (select item_code from item_lookup where " +
						"segment_id = "+segId+
						" and " +
						"ret_lir_id ="+lir_id +"))");
		
		String strResult = PristineDBUtil.getSingleColumnVal( conn, sb, "substitueAnalysis");
		return Integer.parseInt(strResult);
		
	}

	public int getTrxCountForSubCat(Connection conn, Integer seg_id, Integer storeId, String analysisDate) throws GeneralException {
		// TODO Auto-generated method stub
		StringBuffer sb = new StringBuffer();

		sb = sb.append("select count(*) from (SELECT distinct trx_no, calendar_id ct from transaction_log where " +
				"calendar_id IN (select calendar_id from retail_calendar where start_date > to_date('"+analysisDate+"','MM/dd/yyyy')-180) " +
				"and store_id = "+storeId+"  and item_id in  (select item_code from item_lookup where sub_category_id ="+seg_id +"))");
		
		String strResult = PristineDBUtil.getSingleColumnVal( conn, sb, "substitueAnalysis");
		return Integer.parseInt(strResult);
		
	}

	public int getCustCountForSubCat(Connection conn, Integer seg_id, Integer storeId, String analysisDate) throws GeneralException {
		// TODO Auto-generated method stub
		StringBuffer sb = new StringBuffer();

		sb = sb.append("select count(*) from (SELECT distinct customer_id ct from transaction_log where " +
				"calendar_id IN (select calendar_id from retail_calendar where start_date > to_date('"+analysisDate+"','MM/dd/yyyy')-180) " +
				"and store_id = "+storeId+"  and item_id in  (select item_code from item_lookup where sub_category_id ="+seg_id +"))");
		
		String strResult = PristineDBUtil.getSingleColumnVal( conn, sb, "substitueAnalysis");
		return Integer.parseInt(strResult);
		
	}
	public boolean insertItemSubstitutes(Connection conn, StringBuffer sql) throws GeneralException {

		boolean insertFlag = false;
		//logger.debug("Begin insert into db");
		
		PristineDBUtil.execute(conn, sql, "Item Substitutes - Insert Substitutes");
		insertFlag = true;

		return insertFlag;

	}	
	
	private StringBuffer getEligibleMainItemsQuery(String analysisDate, int store_location_type_id, int storeId, int item_type, String itemFilter)
	{
	    StringBuffer sql = new StringBuffer();
	    
	    sql.append(" select sub_category_id, item_id, count(distinct calendar_id) days_moved,  avg(avg_price) avg_price from " + lineSeparator +
        "( select calendar_id, item_id, sum(quantity) mvt, avg(unit_price) avg_price from  " + lineSeparator +
          "( select days.calendar_id day_id, weeks.calendar_id week_id from  " + lineSeparator +
            "retail_calendar days left join retail_calendar weeks on days.start_date >= weeks.start_date and days.start_date <= weeks.end_date and weeks.row_type = 'W' " + lineSeparator +
            "where days.start_date >= to_date('" + analysisDate + "','MM/dd/yyyy') - " + PropertyManager.getProperty("SA_PERIOD_DAYS_BEFORE_FOR_ITEM") + " and days.start_date  <  to_date('" + analysisDate + "','MM/dd/yyyy') and days.row_type ='D' " + lineSeparator +
          ") calendar " + lineSeparator +
          "left join ( select calendar_id, item_id, quantity, unit_price from " +  lineSeparator +
            "transaction_log where store_id = " + storeId + " and item_id in (select item_code from item_lookup where " + itemFilter + " and active_indicator = 'Y') " + lineSeparator +
            ") trx on calendar.day_id = trx.calendar_id " + lineSeparator +
          "group by  calendar_id, item_id " + lineSeparator +
          "having sum(quantity) >= " + PropertyManager.getProperty("SA_MINIMUM_MOVED_QUANTITY_FOR_ITEM") + " " + lineSeparator +
        ") filtered_trx  " + lineSeparator +
        "left join item_lookup on filtered_trx.item_id = item_lookup.item_code " + lineSeparator +
        "group by sub_category_id, item_id " + lineSeparator +
        "having count(calendar_id) >= " + PropertyManager.getProperty("SA_MINIMUM_MOVED_DAYS_FOR_ITEM") + " ");
	    
	    return sql;
	}
	
	private StringBuffer getEligibleSubstituteItemsQuery(String analysisDate, int store_location_type_id, int storeId, int item_type, String itemFilter)
	{
	    StringBuffer sql = new StringBuffer();
	    
	    sql.append(" select sub_category_id, item_id, count(distinct calendar_id) days_moved,  avg(avg_price) avg_price from " + lineSeparator +
                 "( select calendar_id, item_id, sum(quantity) mvt, avg(unit_price) avg_price from " + lineSeparator +
                   "( select days.calendar_id day_id, weeks.calendar_id week_id from " + lineSeparator +
                   "retail_calendar days left join retail_calendar weeks on days.start_date >= weeks.start_date and days.start_date <= weeks.end_date and weeks.row_type = 'W' " + lineSeparator +
                   "where days.start_date >= to_date('" + analysisDate + "','MM/dd/yyyy') - " + PropertyManager.getProperty("SA_PERIOD_DAYS_BEFORE_FOR_ITEM") + " and days.start_date  <  to_date('" + analysisDate + "','MM/dd/yyyy') and days.row_type ='D' " + lineSeparator +
                   ") calendar " + lineSeparator +
                   "left join ( select calendar_id, item_id, quantity, unit_price from " +  lineSeparator +
                     "transaction_log where store_id = " + storeId + " and item_id in (select item_code from item_lookup where " + itemFilter + " and active_indicator = 'Y') " + lineSeparator +
                     ") trx on calendar.day_id = trx.calendar_id " + lineSeparator +
                   "group by  calendar_id, item_id " + lineSeparator +
                   "having sum(quantity) >= " + PropertyManager.getProperty("SA_MINIMUM_MOVED_QUANTITY_FOR_ITEM") + " " + lineSeparator +
                 ") filtered_trx " + lineSeparator +
                 "left join item_lookup on filtered_trx.item_id = item_lookup.item_code " + lineSeparator +
                 "group by sub_category_id, item_id " + lineSeparator +
                 /* Note the division by 3 */
                 "having count(calendar_id) >= " + Integer.parseInt(PropertyManager.getProperty("SA_MINIMUM_MOVED_DAYS_FOR_ITEM")) / 3 + " ");
	    
	    
	    //PropertyManager.getProperty("SA_ELIGIBLE_DEPARTMENT_IDS")
	    return sql;
	}	
	
	private StringBuffer getEligibleTrxLineItemsQuery(String analysisDate, int store_location_type_id, int storeId, int item_type, String itemFilter)
	{
	    StringBuffer sql = new StringBuffer();
	    
	    sql.append(" select customer_id, trx_no, item_id " + 
		"from transaction_log inner join item_lookup on transaction_log.item_id = item_lookup.item_code " + 
		"where store_id = " + storeId + " and calendar_id in " +
			"( select calendar_id from retail_calendar where start_date >= to_date('" + analysisDate + "', 'MM/dd/yyyy') - " + 
			Integer.parseInt(PropertyManager.getProperty("SA_PERIOD_DAYS_BEFORE_FOR_ANALYSIS_ITEM")) + 
			" and start_date  <  to_date('" + analysisDate + "', 'MM/dd/yyyy') and row_type ='D' ) " +
			"AND " + itemFilter);
	    return sql;
	}
	
	private StringBuffer getItemSubstitutesQuery(String analysisDate, int store_location_type_id, int storeId, int item_type, String itemFilter)
	{
	    StringBuffer sql = new StringBuffer();
	    
	    sql.append(" select ip_filtered_scores.*, (case when overall_score >= 8 then 3 when overall_score >= 6 then 2 when overall_score >= 3 then 1 else 0 end) overall_strength from " + lineSeparator +
	    "( select ip_scores.*, (aawab_cst_pct_score + aawab_b_li_pct_score + aawab_aandb_li_pct_score + size_diff_pct_score + price_diff_pct_score + group_score) overall_score from " + lineSeparator +
	      "( select ip_pct.*, (case when aawab_cst_pct > 5 then 2 when aawab_cst_pct > 3 then 1 else 0 end) aawab_cst_pct_score, " + lineSeparator +
	        "(case when aawab_b_li_pct > 40 then 2 when aawab_b_li_pct > 30 then 1 else 0 end) aawab_b_li_pct_score, " + lineSeparator +
	        "(case when aawab_aandb_li_pct < 5 then 1 else 0 end) aawab_aandb_li_pct_score, " + lineSeparator +
	        "(case when size_diff_pct < 40 then 1 else 0 end) size_diff_pct_score, " + lineSeparator +
	        "(case when price_diff_pct < 40 then 1 else 0 end) price_diff_pct_score, " + lineSeparator +
	        "(case when a_segment = b_segment then 2 when a_lig = b_lig then 1 else 0 end) group_score from " + lineSeparator + 
	        "( select ip_aggr.*, round(100*aawab_cst_count/nullif(aorb_cst_count, 0), 2) aawab_cst_pct, round(100*aawab_b_li_count/nullif(aawab_aorb_li_count, 0), 2) aawab_b_li_pct, round(100*aawab_aandb_li_count/nullif(aawab_aorb_li_count, 0), 2) aawab_aandb_li_pct, " + lineSeparator +
	          "a_items.segment_id a_segment, b_items.segment_id b_segment, a_items.ret_lir_id a_lig, b_items.ret_lir_id b_lig, " + lineSeparator + 
	          "round((100*abs(a_items.item_size - b_items.item_size))/nullif(a_items.item_size, 0), 2) size_diff_pct, " + lineSeparator +
	          "round((100*abs(a_avg_price - b_avg_price))/nullif(a_avg_price, 0), 2) price_diff_pct from " + lineSeparator +
	          "( select a_item_id, b_item_id, sum(a_li_count) a_li_count, sum(b_li_count) b_li_count, sum(bonly_li_count) bonly_li_count, sum(aonly_li_count) aonly_li_count, " + lineSeparator +
	            "sum(aorb_li_count) aorb_li_count, sum(aandb_li_count) aandb_li_count, " + lineSeparator +
	            "sum(case when a_li_count > 0 then 1 else 0 end) a_cst_count, sum(case when b_li_count > 0 then 1 else 0 end) b_cst_count, " +  lineSeparator +
	            "sum(case when bonly_li_count > 0 then 1 else 0 end) bonly_cst_count, sum(case when aonly_li_count > 0 then 1 else 0 end) aonly_cst_count, " +  lineSeparator +
	            "sum(case when aorb_li_count > 0 then 1 else 0 end) aorb_cst_count, sum(case when aandb_li_count > 0 then 1 else 0 end) aandb_cst_count, " + lineSeparator +
	            "sum(aawab_indicator) aawab_cst_count, " + lineSeparator +
	            "sum(case when aawab_indicator = 1 then a_li_count else 0 end) awab_a_li_count, sum(case when aawab_indicator = 1 then b_li_count else 0 end) aawab_b_li_count, " +  lineSeparator +
	            "sum(case when aawab_indicator = 1 then aorb_li_count else 0 end) aawab_aorb_li_count, sum(case when aawab_indicator = 1 then aandb_li_count else 0 end) aawab_aandb_li_count, avg(a_avg_price) a_avg_price, avg(b_avg_price) b_avg_price from " + lineSeparator +
	            "( select a_item_id, b_item_id, customer_id, " + lineSeparator +
	              "sum(a_li_count) a_li_count, sum(b_li_count) b_li_count, sum(bonly_li_count) bonly_li_count, sum(aonly_li_count) aonly_li_count, " +  lineSeparator +
	              "sum(aorb_li_count) aorb_li_count, sum(aandb_li_count) aandb_li_count, case when sum(a_li_count) > 0 and sum(b_li_count) > 0 then 1 else 0 end aawab_indicator, avg(a_avg_price) a_avg_price, avg(b_avg_price) b_avg_price from " + lineSeparator +
	              "( select a_item_id, b_item_id, customer_id, trx_no, max(a_li_count) a_li_count, max(b_li_count) b_li_count, " + lineSeparator +
	                "( case when max(a_li_count) = 0 then 1 else 0 end ) bonly_li_count, ( case when max(b_li_count) = 0 then 1 else 0 end ) aonly_li_count, " + lineSeparator + 
	    				"1 aorb_li_count, " + lineSeparator +
	                "( case when max(a_li_count) + max(b_li_count) >= 2 then 1 else 0 end ) aandb_li_count, avg(a_avg_price) a_avg_price, avg(b_avg_price) b_avg_price from " + lineSeparator +
	                "( select a_item_id, b_item_id, customer_id, trx_no, " + lineSeparator +
	                  "(case when item_id = a_item_id then 1 else 0 end) a_li_count, (case when item_id = b_item_id then 1 else 0 end) b_li_count, a_avg_price, b_avg_price " + lineSeparator + 
	                  "from " + lineSeparator +
	                  lineSeparator +
	                  /* Making item pairs */
	                  "( select item_a.item_id a_item_id, item_b.item_id b_item_id, item_a.avg_price a_avg_price, item_b.avg_price b_avg_price " + lineSeparator + 
	                    "from " + lineSeparator +
	                     lineSeparator +
	                    "( " + lineSeparator +
	                    	/* Get the list of items ('a_item_id's) for which substitute is to be found */
	    					getEligibleMainItemsQuery(analysisDate, store_location_type_id, storeId, item_type, itemFilter) + lineSeparator +
	                    ") item_a " + lineSeparator +
	                    "" + lineSeparator +
	                    "inner join " + lineSeparator +
	                    "( " + lineSeparator +
	                    	/* Get the list of items ('b_item_id's) that can become substitutes to other items */
	    					getEligibleSubstituteItemsQuery(analysisDate, store_location_type_id, storeId, item_type, itemFilter) + lineSeparator +
	                    ") item_b " + lineSeparator +
	                      "on item_a.sub_category_id = item_b.sub_category_id and item_a.item_id <> item_b.item_id " + lineSeparator + 
	                      lineSeparator +    
	                  ") item_pair " + lineSeparator +
	                  lineSeparator +
	                  "inner join " +  lineSeparator +
	                  "( " + lineSeparator +
	                  
	                  /* Get the transactiona and line items for each item pair */
	    				getEligibleTrxLineItemsQuery(analysisDate, store_location_type_id, storeId, item_type, itemFilter) + lineSeparator +
	                  ") li on ( item_pair.a_item_id = li.item_id or item_pair.b_item_id = li.item_id ) " +  lineSeparator +
	                ") item_pair_li " + lineSeparator +
	                "group by a_item_id, b_item_id, customer_id, trx_no " + lineSeparator +
	              ") item_pair_trx " + lineSeparator +
	              "group by a_item_id, b_item_id, customer_id " + lineSeparator +
	            ") ip_customers " + lineSeparator +
	          "group by a_item_id, b_item_id " + lineSeparator +
	          ") ip_aggr " + lineSeparator +
	          "left join item_lookup a_items on a_item_id = a_items.item_code left join item_lookup b_items on b_item_id = b_items.item_code " + lineSeparator +
	        ") ip_pct " + lineSeparator +
	      ") ip_scores " + lineSeparator +
	    ") ip_filtered_scores " + lineSeparator +
	      
	    /* Ignore low score item substitutes */
	    "where overall_score >= " + PropertyManager.getProperty("SA_FILTER_OVERALL_SCORE_LOWER_LIMIT") + " ");    
	    
	    return sql;
	}	
	
	public boolean getItemSubstitutes(Connection conn, String batchid, String analysisDate, int store_location_type_id, int storeId, int item_type, String itemFilter)
	{
		
	    StringBuffer sql = new StringBuffer();
	    
	    sql.append(" insert into item_substitutes " + lineSeparator +
	    "select  " + lineSeparator +
	     "ITEM_SUBSTITUTES_SEQ.NEXTVAL, " + store_location_type_id + ", " + storeId + ", 1, a_item_id, b_item_id, 0, NULL, 0, 0, NULL, 0, overall_strength, 0, 0, 0, 0, " + lineSeparator +
	     "aorb_cst_count, aawab_cst_count, aandb_cst_count, aonly_cst_count, aawab_aorb_li_count, aawab_aandb_li_count, awab_a_li_count, aawab_b_li_count, " + lineSeparator + 
	     "nvl(aawab_cst_pct, 0), 0, 0, nvl(aawab_b_li_pct, 0), nvl(aawab_aandb_li_pct, 0), case when size_diff_pct > 999 then 999 else nvl(size_diff_pct, 0) end size_diff_pct, 0, 0, case when price_diff_pct > 999 then 999 else nvl(price_diff_pct, 0) end price_diff_pct, " + lineSeparator +
	     "aawab_cst_pct_score, aawab_b_li_pct_score, size_diff_pct_score, price_diff_pct_score, group_score, aawab_aandb_li_pct_score, overall_score, " + batchid + ", sysdate, 'Y' " + lineSeparator + 
	    "from " + lineSeparator +
	    "( " + lineSeparator +
	    	getItemSubstitutesQuery(analysisDate, store_location_type_id, storeId, item_type, itemFilter) + lineSeparator +
	    ") " + lineSeparator );
	    
	    try
	    {
	    	PristineDBUtil.execute(conn, sql, "Item Substitutes DAO - Analyse and Save Substitutes.");
	    	conn.commit();
	    	
	    	return true;
	    }
	    catch (GeneralException e) {
			e.printStackTrace();
	    	return false;
	    }
		catch (SQLException e) {
			e.printStackTrace();
			return false;
	    }
	}    
	
	
	public List<Integer> getEligibleCategories(Connection conn)
	{
		StringBuffer sql = new StringBuffer();
		List<Integer> categories = new ArrayList<Integer>();

		try
		{
			/*sql.append("select sub_category.id from sub_category inner join category on sub_category.category_id = category.id " + 
					"where dept_id in ( " + PropertyManager.getProperty("SA_ELIGIBLE_DEPARTMENT_IDS") + " )");*/
			sql.append("select distinct item_segment.id from item_segment inner join item_lookup on item_segment.id = item_lookup.segment_id " + lineSeparator + 
					"where item_lookup.dept_id in ( " + PropertyManager.getProperty("SA_ELIGIBLE_DEPARTMENT_IDS") + lineSeparator + 
					" ) and item_lookup.active_indicator = 'Y'" + lineSeparator +
					" and segment_id not in (select distinct segment_id from item_substitutes inner join item_lookup on item_substitutes.a_item_id = item_lookup.item_code)");


			//sql.append("select id from sub_category where dept_id in ( " + PropertyManager.getProperty("SA_ELIGIBLE_DEPARTMENT_IDS") + " )");					
			CachedRowSet result = PristineDBUtil.executeQuery(conn, sql, "getEligibleCategories() - Get eligible categories.");		
			
			while ( result.next() ) {
				categories.add(result.getInt("id"));
			}
			return categories;
		}
		catch (GeneralException e) {
			e.printStackTrace();
			return categories;
		}
		catch (SQLException e) {
			e.printStackTrace();
			return categories;
		}
		
	}
			
	public List<Integer> getEligibleSegments(Connection conn)
	{
		StringBuffer sql = new StringBuffer();
		List<Integer> categories = new ArrayList<Integer>();

		try
		{
			sql.append("select distinct item_segment.id from item_segment inner join item_lookup on item_segment.id = item_lookup.segment_id " + lineSeparator + 
					"where item_lookup.dept_id in ( " + PropertyManager.getProperty("SA_ELIGIBLE_DEPARTMENT_IDS") + lineSeparator + 
					" ) and item_lookup.active_indicator = 'Y'");
					
			CachedRowSet result = PristineDBUtil.executeQuery(conn, sql, "getEligibleCategories() - Get eligible categories.");		
			
			while ( result.next() ) {
				categories.add(result.getInt("id"));
			}
			
			return categories;
		}
		catch (GeneralException e) {
			e.printStackTrace();
			return categories;
		}
		catch (SQLException e) {
			e.printStackTrace();
			return categories;
		}
		
	}
			
}


