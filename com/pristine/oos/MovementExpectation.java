package com.pristine.oos;

import java.sql.Connection;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.Date;
import java.util.HashMap;
import java.util.Hashtable;
import java.util.List;
import java.util.Map;

import javax.sql.rowset.CachedRowSet;

import org.apache.log4j.Logger;
import org.apache.log4j.PropertyConfigurator;

import com.pristine.dao.CompetitiveDataDAO;
import com.pristine.dao.DBManager;
import com.pristine.dao.MovementDAO;
import com.pristine.dao.OOSCalcsDAO;
import com.pristine.dao.ScheduleDAO;
import com.pristine.dao.StoreDAO;
import com.pristine.dto.OOSMovementProductGroupExpDTO;
import com.pristine.dto.OOSSummaryDTO;
import com.pristine.dto.OOSTimeFactor;
import com.pristine.dto.ScheduleInfoDTO;
import com.pristine.exception.GeneralException;
import com.pristine.util.Constants;
import com.pristine.util.DateUtil;
import com.pristine.util.PristineDBUtil;
import com.pristine.util.PropertyManager;

public class MovementExpectation {
	private static Logger  logger = Logger.getLogger("MovExp");
	Connection conn = null;
	int strId, productLevelId, priceCheckListId;
	String schIdList;
	private OOSCalcsDAO ossCalcDao = new OOSCalcsDAO ();
	private OOSAnalysis oosAnalyzer = new OOSAnalysis();
	private OOSTimeFactor timeSlots [];
	
	private Hashtable<String,Integer> totalVisitCountCache = new Hashtable();

	private final int STOPLIMIT = -1;
	
	private int obsByDay [] = new int[8];
	private String schCSVByDay [] = new String[8]; 

	
	public static void main(String[] args) {
		// TODO Auto-generated method stub

		//Inputs - Store Id
		//Date
		PropertyConfigurator.configure("log4j-oos.properties");
		
		PropertyManager.initialize("analysis.properties");
		
		MovementExpectation movExp = new MovementExpectation ();		
		
		if (args.length == 2) {
			if (args[1].equalsIgnoreCase("ITEMLEVEL")) {
				movExp.setupExpectation(args[0]);
			} else if (args[1].equalsIgnoreCase("SEGMENTLEVEL")) {
				movExp.setupSegmentLevelExpectation(args[0]);
			} else {
				logger.info("Incorrect Argument Value - Expected Values are ITEMLEVEL OR SEGMENTLEVEL ");
				System.exit(1);
			}
		} 
		else{ 
			logger.info("Incorrect Arguments - MovementExpectation StoreNumber ");
			System.exit(1);
		}

	}
	

	/**
	 * Calculates Movement Expectation in segment level for a store
	 * 
	 * @param storeNo
	 */
	private void setupSegmentLevelExpectation(String storeNo) {
		try {
			logger.info("Segment level calculation Started for Store - "
					+ storeNo);
			// get DB Connection
			conn = DBManager.getConnection();

			// Get the Time Slots
			timeSlots = oosAnalyzer.prepareTimeSlots();

			// Get Store Id
			getStoreId(storeNo);

			// Get the check list
			getCheckListId();

			// Get the Product Level Id
			getProductLevelId();

			// Get all the schedules
			getSchedules();
			logger.info("Schedule List : " + schIdList);

			Map<Integer, ArrayList<Integer>> itemsByProductLevel = new HashMap<Integer, ArrayList<Integer>>();
			// Segments and its items - only top 6000 items are considered
			logger.info(" Fetching Product Id and its items is started");
			itemsByProductLevel = ossCalcDao.getItemsBySegment(conn,
					priceCheckListId);
			logger.info(" Fetching Product Id and its items is completed");

			performSegmentLevelMovExpCalc(itemsByProductLevel);
			ossCalcDao.updateExpectationsWithTotalVisits(conn, strId);
		} catch (GeneralException ge) {
			logger.error("Error in Mov Exp Calculation", ge);
			PristineDBUtil.rollbackTransaction(conn,
					"MovExp Segment Level Calcs");
			System.exit(1);
		} catch (SQLException e) {
			logger.error("Error in Mov Exp Calculation", e);
			System.exit(1);
		}

	}

	/**
	 * Calculates Expected Revenue and Expected Quantity of each product id for
	 * all the days and its time slot
	 * 
	 * @param itemsByProductLevel
	 *            - HashMap contains product id and its items
	 * @throws SQLException
	 * @throws GeneralException
	 */
	private void performSegmentLevelMovExpCalc(
			Map<Integer, ArrayList<Integer>> itemsByProductLevel)
			throws SQLException, GeneralException {

		OOSMovementProductGroupExpDTO movPrdGrpExp;
		ArrayList<OOSMovementProductGroupExpDTO> movPrdGrpExpCol ; 
		int slotLen = timeSlots.length;
		ArrayList<Integer> itemCollection;
		String items;

		// Calculate Expected Revenue and Expected Quantity

		for (int dow = 1; dow < 8; dow++) {
			movPrdGrpExpCol = new ArrayList<OOSMovementProductGroupExpDTO>();
			for (int j = 0; j < slotLen - 1; j++) {
				logger.info(" Expection calculation for day : " + dow
						+ " and time : " + timeSlots[j].todId + " is started");
				// Segment Loop
				for (Map.Entry<Integer, ArrayList<Integer>> productId : itemsByProductLevel
						.entrySet()) {
					logger.info(" Expection calculation for day : " + dow
							+ " and time : " + timeSlots[j].todId + " and for segment: " + productId.getKey() + " is started");
					movPrdGrpExp = new OOSMovementProductGroupExpDTO();
					movPrdGrpExp.storeId = strId;
					movPrdGrpExp.dayOfWeek =   dow;
					movPrdGrpExp.timeOfDay =   timeSlots[j].todId;
					movPrdGrpExp.productLevelId = productLevelId;
					movPrdGrpExp.productId = productId.getKey();

					itemCollection = new ArrayList<Integer>();
					itemCollection = productId.getValue();
					items = getItemsCSV(itemCollection);

					movPrdGrpExp = ossCalcDao.getProductGroupMovExp(conn,
							movPrdGrpExp, schIdList, movPrdGrpExp.dayOfWeek,
							movPrdGrpExp.timeOfDay, items);
					movPrdGrpExpCol.add(movPrdGrpExp);
					logger.info(" Expection calculation for day : " + dow
							+ " and time : " + timeSlots[j].todId + " and for segment: " + productId.getKey() + " is completed");
				}
				logger.info(" Expection calculation for day : " + dow
						+ " and time : " + timeSlots[j].todId + " is completed");
			}
			
			// Daily
			logger.info(" Expection calculation for day : " + dow
					+ " and time : 4  is started");
			for (Map.Entry<Integer, ArrayList<Integer>> productId : itemsByProductLevel
					.entrySet()) {
				movPrdGrpExp = new OOSMovementProductGroupExpDTO();
				movPrdGrpExp.storeId = strId;
				movPrdGrpExp.dayOfWeek = dow;
				movPrdGrpExp.timeOfDay = 4;
				movPrdGrpExp.productLevelId = productLevelId;
				movPrdGrpExp.productId = productId.getKey();

				itemCollection = new ArrayList<Integer>();
				itemCollection = productId.getValue();
				items = getItemsCSV(itemCollection);

				movPrdGrpExp = ossCalcDao.getProductGroupMovExp(conn,
						movPrdGrpExp, schIdList, movPrdGrpExp.dayOfWeek,
						movPrdGrpExp.timeOfDay, items);
				movPrdGrpExpCol.add(movPrdGrpExp);
			}
			logger.info(" Expection calculation for day : " + dow
					+ " and time : 4  is completed");
		
			// Delete any previous calculations for the day
			logger.info(" Records deletion for the day : " + dow +  " is started");
			ossCalcDao.deleteSegmentLevelExpectations(conn, strId, dow, productLevelId);
			logger.info(" Records deletion for the day : " + dow +  " is completed");
			
			// Batch Insert
			logger.info(" Batch Insert for the day : " + dow +  " is started");
			ossCalcDao.InsertToSummaryDailyMovPrdGrpExp(conn, movPrdGrpExpCol);	
			logger.info(" Batch Insert for the day : " + dow +  " is completed");
		}
	}

	/**
	 * Converts items of a ArrayList to a comma separated string
	 * 
	 * @param itemCollection
	 *            - ArrayList of type Integer
	 * @return
	 */
	private String getItemsCSV(ArrayList<Integer> itemCollection) {
		StringBuilder itemList;
		String items;

		itemList = new StringBuilder();
		for (Integer itemCode : itemCollection) {
			if (itemList.length() > 0) {
				itemList.append(',');
			}
			itemList.append(itemCode.toString());
		}
		items = itemList.toString();

		return items;
	}

	/**
	 * Get Competitor Store Id from Competitor Store Number
	 * 
	 * @param storeNo
	 *            - Competitor Store Number
	 * @throws GeneralException
	 */
	private void getStoreId(String storeNo) throws GeneralException {
		StoreDAO strdao = new StoreDAO();
		strId = strdao.getStoreID(conn, storeNo, null, -1);
		if (strId <= 0) {
			throw new GeneralException(
					"Invalid Store Number, Store Number passed = " + storeNo);
		}
	}
	
	


	/**
	 * Get all the schedule id's as a comma separated string
	 * 
	 * @throws GeneralException
	 */
	private void getSchedules() throws GeneralException {
		String startDate = DateUtil.getWeekStartDate(65);
		String endDate = DateUtil.getWeekStartDate(0); // get schedules for
														// Store
		ScheduleDAO schDao = new ScheduleDAO();
		ArrayList<ScheduleInfoDTO> schList = schDao.getSchedulesForStore(conn,
				strId, -1, startDate, endDate);
		schIdList = ScheduleDAO.getScheduleIdCSV(schList);
	}

	/**
	 * Get the Price Check List Id from the analysi.properties. Returns -1 if
	 * not found
	 * 
	 * @throws GeneralException
	 */
	private void getCheckListId() throws GeneralException {
		priceCheckListId = Integer.parseInt(PropertyManager.getProperty(
				"OOS_ANALYSIS.CHECKLIST_ID", "-1"));
		if (priceCheckListId <= 0) {
			throw new GeneralException(
					"Invalid CheckList Id, please set the OOS_ANALYSIS.CHECKLIST_ID property ");
		}
	}

	/**
	 * Get the Product Level Id from the analysis.properties Returns -1 if not
	 * found
	 * 
	 * @throws GeneralException
	 */
	private void getProductLevelId() throws GeneralException {
		productLevelId = Integer.parseInt(PropertyManager.getProperty(
				"OOS_MOV_PRD_GRP_EXP_PRD_LEVEL_ID", "-1"));
		if (productLevelId <= 0) {
			throw new GeneralException(
					"Invalid Product Level Id, please set the OOS_MOV_PRD_GRP_EXP_PRD_LEVEL_ID property ");
		}
	}

	private void setupExpectation(String storeNo) {
		// TODO Auto-generated method stub
		logger.info("Calculation Started for Store - " + storeNo );
		
		try {
			// get DB Connection
			conn = DBManager.getConnection();
			
			timeSlots = oosAnalyzer.prepareTimeSlots();

			
			//Get Store Id
			StoreDAO strdao = new StoreDAO();
			strId = strdao.getStoreID(conn, storeNo,null,-1);
			if( strId <= 0){
				throw new GeneralException ("Invalid Store Number, Store Number passed = " + storeNo );
			}
			
			totalVisitCountCache = ossCalcDao.loadTotalVisitCountCache(conn, strId);
			
			//Delete any previous calculations
			ossCalcDao.deleteExpectations(conn, strId);
			
			//Get the check list
			int checkListId = Integer.parseInt(PropertyManager.getProperty("OOS_ANALYSIS.CHECKLIST_ID", "222")); //DEBUG
			if( checkListId <= 0){
				throw new GeneralException ("Invalid CheckList Id, please set the OOS_ANALYSIS.CHECKLIST_ID property " );
			}

			/**SG - Make Start and end date it as inp */
			for ( int i = 1; i < 8; i++){
				ArrayList<ScheduleInfoDTO> schList = ossCalcDao.getSchedulesForStore(conn,strId, i);
				String schIdCSVList = ScheduleDAO.getScheduleIdCSV(schList);
				obsByDay [i] =  schList.size();
				schCSVByDay[i]= schIdCSVList;
				logger.info("Observations for " + i + " = " + obsByDay [i] );
			}
			

			ArrayList<ScheduleInfoDTO> schList = ossCalcDao.getSchedulesForStore(conn,strId, -1);

			if( schList.size() > 0 ){
				//first schedule
				ScheduleInfoDTO schInfo = schList.get(0); 
				String startDate = schInfo.getStartDate();
				
				//last Schedule
				schInfo = schList.get(schList.size() - 1);
				String endDate = schInfo.getEndDate();
				
				java.util.Date startDateObj = DateUtil.toDate(startDate);
				java.util.Date endDateObj = DateUtil.toDate(endDate);
				
				int noOfObservations = (int)DateUtil.getDateDiff(endDateObj, startDateObj);
				
				logger.info ( "SUMMARY START DATE = " + startDate);
				logger.info ( "SUMMARY END DATE = " + endDate);
				logger.info ( "No of Days to analyze = " + noOfObservations);
				if( noOfObservations <= 0){
					throw new GeneralException ("Summary data not available for creating expectation" );
				}


			}
			
			
			//Get the list of items for the check list by retId
			//CachedRowSet crs = ossCalcDao.getLIGItemList(conn, checkListId);
			//performMovExpAnalysisCalcsForLIG(crs);
			
			CachedRowSet crs = ossCalcDao.getNonLIGItemList(conn, checkListId);
			performMovExpAnalysisCalcsForNonLIG(crs);
//			ossCalcDao.updateExpectationsWithTotalVisits(conn, strId);
		}catch (GeneralException ge) {
			logger.error("Error in Mov Exp Calculation", ge);
			PristineDBUtil.rollbackTransaction(conn, "MovExp Calcs");
			System.exit(1);
		}

		try {
			if (PropertyManager.getProperty("DATALOAD.COMMIT", "")
					.equalsIgnoreCase("TRUE")) {
				logger.info("Committing transacation");
				PristineDBUtil.commitTransaction(conn, "OOS Data Load");
			} else {
				logger.info("Rolling back transacation");
				PristineDBUtil.rollbackTransaction(conn, "OOS Data Load");
			}
		} catch (GeneralException ge) {
			logger.error("Error in commit", ge);
			System.exit(1);
		} finally {
			PristineDBUtil.close(conn);
		}
		logger.info("Calculation Completed for Store - " + storeNo);

		
	}


	private void performMovExpAnalysisCalcsForLIG(CachedRowSet itemListCrs) 
		throws GeneralException{
		try{
			int currentLirId = -1;
			int curFamilyItemCode = -1;
			ArrayList<Integer> itemCodeList = new ArrayList <Integer>();;
			int count = 0;
			while( itemListCrs.next()){
				count++;
				int newLirId  = itemListCrs.getInt("RET_LIR_ID");;
				int newFamilyItemCode = itemListCrs.getInt("RET_LIR_ITEM_CODE");
				int currentItemCode  = itemListCrs.getInt("ITEM_CODE");
				if( currentLirId == -1 ){
					//First Row
					itemCodeList.clear();
					itemCodeList.add(currentItemCode); 
					currentLirId = newLirId;
					curFamilyItemCode = newFamilyItemCode;
				}else if ( currentLirId == newLirId){
					itemCodeList.add(currentItemCode); 
				}else {
					// Process the list;
					//if( curFamilyItemCode == 23129 || curFamilyItemCode == 423219)
					setupMovExpectations( itemCodeList, curFamilyItemCode); 
					
					itemCodeList.clear();
					itemCodeList.add(currentItemCode); 
					currentLirId = newLirId;
					curFamilyItemCode = newFamilyItemCode;

				}
				if( STOPLIMIT > 0 && count >= STOPLIMIT) break;
			}
			//if( curFamilyItemCode == 23129 || curFamilyItemCode == 423219)
			setupMovExpectations(itemCodeList, curFamilyItemCode);
			
		}catch( SQLException sqle){
			throw new GeneralException( "Cached Rowset access exception", sqle);
		}
		
	}

	private void performMovExpAnalysisCalcsForNonLIG(CachedRowSet itemListCrs)
	throws GeneralException
	{
	try{
		ArrayList<Integer> itemCodeList = new ArrayList<Integer> ();
		int count = 0;
		while( itemListCrs.next()){
			count++;
			int newLirId  = itemListCrs.getInt("RET_LIR_ID");;
			int newFamilyItemCode = itemListCrs.getInt("RET_LIR_ITEM_CODE");
			int currentItemCode  = itemListCrs.getInt("ITEM_CODE");
			itemCodeList.clear();
			itemCodeList.add(currentItemCode);
			setupMovExpectations(itemCodeList, newFamilyItemCode);
			if( STOPLIMIT > 0 && count >= STOPLIMIT) break;
		}
	}catch( SQLException sqle){
		throw new GeneralException( "Cached Rowset access exception", sqle);
	}
	
}

	private void setupMovExpectations(ArrayList<Integer> itemCodeList,
			int curFamilyItemCode ) throws GeneralException, SQLException {
		logger.info( "Doing Analysis for " + curFamilyItemCode);
		int slotLen = timeSlots.length;
		
		List<OOSSummaryDTO> sbL = new ArrayList<OOSSummaryDTO>();
		
		for ( int dow = 1; dow < 8; dow++){ //for each day
			//SG** Get the count of weeks when the item was in sale
			int noOfSaleObs = ossCalcDao.getNoOfSaleObjs(conn, schCSVByDay[dow], curFamilyItemCode, dow);
			
			int totalnoOfObs = obsByDay [dow]; 
			 for (int j = 0; j < slotLen - 1; j++){
				 int tod = timeSlots[j].todId;
				 OOSSummaryDTO oosSummaryBase = ossCalcDao.getOOSItemStats( conn, schCSVByDay[dow], curFamilyItemCode, dow, tod,
					false, totalnoOfObs - noOfSaleObs); 
				 OOSSummaryDTO oosSummaryBaseSale = ossCalcDao.getOOSItemStats( conn, schCSVByDay[dow], curFamilyItemCode, dow, tod,
							true, noOfSaleObs); 
				 if( oosSummaryBaseSale != null && oosSummaryBase != null )
					 copySaleMetrics( oosSummaryBase, oosSummaryBaseSale);
				 else if( oosSummaryBaseSale != null && oosSummaryBase == null ){
					 oosSummaryBase = oosSummaryBaseSale; 
				 }
					 
				 if( oosSummaryBase != null ) {
					 oosSummaryBase.storeId = strId; 
//					 ossCalcDao.insertMovementExp(conn, oosSummaryBase);
					 oosSummaryBase.totalCustVisitCount = totalVisitCountCache.get(dow+":"+tod);
					 sbL.add(oosSummaryBase);
				 }
			 }
			 //Daily
			 OOSSummaryDTO oosSummaryDayLevel = ossCalcDao.getOOSItemStats( conn, schCSVByDay[dow], curFamilyItemCode, dow, 4, false, totalnoOfObs - noOfSaleObs); 
			 OOSSummaryDTO oosSummarySaleDayLevel = ossCalcDao.getOOSItemStats( conn, schCSVByDay[dow], curFamilyItemCode, dow, 4, true, noOfSaleObs);
			 if( oosSummarySaleDayLevel != null && oosSummaryDayLevel!= null )
				 copySaleMetrics( oosSummaryDayLevel, oosSummarySaleDayLevel);
			 else if( oosSummarySaleDayLevel != null && oosSummaryDayLevel == null ){
				 oosSummaryDayLevel= oosSummarySaleDayLevel; 
			 }

			 if( oosSummaryDayLevel != null ) {
				 oosSummaryDayLevel.storeId = strId; 
//				 ossCalcDao.insertMovementExp(conn, oosSummaryDayLevel);
				 oosSummaryDayLevel.totalCustVisitCount = totalVisitCountCache.get(dow+":4");
				 sbL.add(oosSummaryDayLevel);
			 }
		}
		ossCalcDao.insertMovementExp(conn, sbL);
	}

	private void copySaleMetrics(OOSSummaryDTO oosSummaryBase,
			OOSSummaryDTO oosSummaryBaseSale) {

		oosSummaryBase.saleQty = oosSummaryBaseSale.saleQty;
		oosSummaryBase.saleRev = oosSummaryBaseSale.saleRev;
		oosSummaryBase.saleVisitCount = oosSummaryBaseSale.saleVisitCount ;
		oosSummaryBase.modal2QtyPerVisit = oosSummaryBaseSale.modal2QtyPerVisit ;
		oosSummaryBase.saleMargin = oosSummaryBaseSale.saleMargin ;
		oosSummaryBase.saleMovVisitRatio = oosSummaryBaseSale.saleMovVisitRatio ;
		oosSummaryBase.noOfSaleObs = oosSummaryBaseSale.noOfSaleObs ;
		
	}
}
