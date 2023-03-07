/*
 * Title: TOPS Summary Daily Movement
 *
 *******************************************************************************
 * Modification History
 *------------------------------------------------------------------------------
 * Version		Date		Name			Remarks
 *------------------------------------------------------------------------------
 * Version 0.1	23/02/2012	Dinesh Kumar	Initial Version 
 * Version 0.2  23/04/2012  Dinesh Kumar    add the Last summary daily id 
 * Version 0.3  02/08/2012  Dinesh Kumar	Movement by volume added
 * Version 0.4  07/08/2012  Dinesh Kumar    CTD SEQ name changed
 * Version 0.5  14/08/2012  Dinesh Kumar    Repair Full Process added
 * Version 0.6  22/08/2012  Dinesh Kumar    Margin Calculation added
 * Version 0.7  04/09/2012  Dinesh Kumar    Aggregation added for fin dept process
 * Version 0.8  21/01/2013  John Britto     Log re-factor
 * Version 0.9  21/08/2015  John Britto     Margin calculation, names re-factored
 * Version 1.0  21/08/2015  John Britto     Margin calculation, names re-factored
 **********************************************************************************
 */
package com.pristine.dataload.salesanalysis;

import java.sql.Connection;
import java.sql.SQLException;
import java.text.DateFormat;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Calendar;
import java.util.Date;
import java.util.HashMap;
import java.util.List;
import org.apache.log4j.Logger;
import org.apache.log4j.PropertyConfigurator;
import com.pristine.business.entity.SalesaggregationbusinessV2;
import com.pristine.dao.CompStoreDAO;
import com.pristine.dao.DBManager;
import com.pristine.dao.ItemDAO;
import com.pristine.dao.MovementDAO;
import com.pristine.dao.RetailCalendarDAO;
import com.pristine.dao.salesanalysis.SalesAggregationDao;
import com.pristine.dao.salesanalysis.SalesAggregationProductGroupDAO;
import com.pristine.dao.salesanalysis.SalesAggregationDailyDao;
import com.pristine.dao.salesanalysis.SalesAggregationStatusDao;
import com.pristine.dataload.tops.CostDataLoad;
import com.pristine.dto.MovementDailyAggregateDTO;
import com.pristine.dto.RetailCalendarDTO;
import com.pristine.dto.salesanalysis.ProductGroupDTO;
import com.pristine.dto.salesanalysis.SpecialCriteriaDTO;
import com.pristine.dto.salesanalysis.SummaryDataDTO;
import com.pristine.exception.GeneralException;
import com.pristine.util.Constants;
import com.pristine.util.PristineDBUtil;
import com.pristine.util.PropertyManager;

public class SummaryDailyV2 {

	static Logger logger = Logger.getLogger("SummaryDaily");
	private String _storeNum = null; // Processing store number
	private String _districtNum = null; // Processing district number
	private Connection _conn = null; // DB connection
	private Date _startdate = null; // Processing StartDate
	private Date _endDate = null; // Processing EndDate
	private String _repairFlag = "NORMAL"; // Processing RepairFlagc
	private String _sourceTable = "MD"; // Processing RepairFlagc
	private HashMap<String, String> _uomMapDB = new HashMap<String, String>();
	private HashMap<String, String> _uomMapPro = new HashMap<String, String>();
	// Product Group and its child info
	ArrayList<ProductGroupDTO> _productGroup = new ArrayList<ProductGroupDTO>();
	
	//Array to hold list of product which has item as child
	ArrayList<Integer> _leastProductLevel = new ArrayList<Integer>();
	HashMap<Integer, Integer> _productGroupType = new HashMap<Integer, Integer>();
	HashMap<Integer, Integer> _gasItems = new HashMap<Integer, Integer>(); 
	int _couponAdjustmentRequired = 0;
	boolean _marginCalcReq = false;
	boolean _updateOnlyPLMetrics = false;
	/*
	 * ****************************************************************
	 * Class constructor
	 * 
	 * @throws Exception,GeneralException
	 * ****************************************************************
	 */
	public SummaryDailyV2(Date startDate, Date endDate, String storeno,
			String district, String repairFlag, String sourceTable,
			String updatePLOnly) {

		if (storeno == "" && district == "") {
			logger.error(" Store Numebr / District Id is missing in Input");
			System.exit(1);
		}

		if (district != "") {
			storeno = "";
		}

		// If there is no date input then set the default process date
		if (startDate == null) {
			Calendar cal = Calendar.getInstance();
			cal.setTime(new Date());
			cal.add(Calendar.DATE, -1);
			startDate = cal.getTime();
			endDate = null; // Set the end date as null
		}

		if (endDate != null) {
			logger.info("Process Start Date: "
					+ startDate.toString());
			logger.info("Process End Date: "
					+ endDate.toString());

		} else {
			logger.info("Process Date: "
					+ startDate.toString());
			endDate = startDate;
		}

		if (repairFlag.equalsIgnoreCase("FULL")){
			_repairFlag = "FULL";
			logger.info("Process mode:FULL, Process all the non-processed days");
		}
		else{
			_repairFlag = "NORMAL";
			logger.info("Process mode:NORMAL, Process data only for specific days");
		}

		if (sourceTable.equalsIgnoreCase("TL")){
			_sourceTable = "TL";
			logger.info("Process mode:FULL, Process all the non-processed days");
		}
		
		if (updatePLOnly.equalsIgnoreCase("Y"))
			_updateOnlyPLMetrics = true;
		
		_storeNum = storeno;
		_districtNum = district;
		_startdate = startDate;
		_endDate = endDate;

		PropertyManager.initialize("analysis.properties");
		try {
			_conn = DBManager.getConnection();

		} catch (GeneralException exe) {
			logger.error("Error while connecting DB:" + exe);
			logger.info("Summary Daily Process Ends unsucessfully");
			System.exit(1);
		}
	}

	/*
	 * ****************************************************************
	 * Main method of Batch. Get the inputs from command line argument Argument
	 * 1: Store Number Argument 2: District Number Argument 3: Start Date
	 * Argument 4: End Date If the Date is not specified then process for
	 * yesterday If the District or Store Number is mandatory. If both are are
	 * specified then consider district alone
	 * ****************************************************************
	 */

	public static void main(String[] args) {
		PropertyConfigurator.configure("log4j-summary-daily.properties");
		logger.info("*** Summary Daily Process Begins ***");
		logCommand(args);
		Date startDate = null;
		Date endDate = null;
		String storeno = "";
		String district = "";

		// Variable added for repair process
		String repairFlag = "";
		
		// Variable for Source table
		String sourceTable = "";
		String updatePLOnly = "N";
		
		DateFormat dateFormat = new SimpleDateFormat("MM/dd/yyyy");
		SummaryDailyV2 summaryDaily = null;
		try {
			for (int ii = 0; ii < args.length; ii++) {
				String arg = args[ii];

				// Get the date / start date (for date range) from command line
				if (arg.startsWith("STARTDATE")) {
					String Inputdate = arg.substring("STARTDATE=".length());
					try {
						startDate = dateFormat.parse(Inputdate);
					} catch (ParseException par) {
						logger.error("Start Date Parsing Error, check Input");
					}
				}

				/* Get the End date if process is for date range This date is
				 * valid only if start date is specified in input */
				if (arg.startsWith("ENDDATE")) {
					String Inputdate = arg.substring("ENDDATE=".length());
					try {
						endDate = dateFormat.parse(Inputdate);
					} catch (ParseException par) {
						logger.error("End Date Parse Error " + par);
					}
				}

				// Get the Store number from command line
				if (arg.startsWith("STORE")) {
					storeno = arg.substring("STORE=".length());
				}

				// Get the District id from command line
				if (arg.startsWith("DISTRICT")) {
					district = arg.substring("DISTRICT=".length());
				}

				// get the RepairFlag from command Line
				if (arg.startsWith("REPAIRFLAG")) {
					repairFlag = arg.substring("REPAIRFLAG=".length());
				}

				// get T-Log source from command Line
				if (arg.startsWith("SOURCETABLE")) {
					sourceTable = arg.substring("SOURCETABLE=".length());
				}				
				if (arg.startsWith("UPDATEPLONLY")) {
					updatePLOnly = arg.substring("UPDATEPLONLY=".length());
				}				

			}

			// Create object for summary daily
			summaryDaily = new SummaryDailyV2(startDate, endDate, storeno,
					district, repairFlag, sourceTable, updatePLOnly);

			// Call summary calculation process
			summaryDaily.processSummaryData(startDate, endDate);

		} catch (GeneralException e) {
			logger.error(e.getMessage());

		} finally {
			logger.info("*** Summary Daily Process Ends ***");
			PristineDBUtil.close(summaryDaily._conn);
		}

	}

	/*
	 * ****************************************************************
	 * Get the Calendar Id list for the given date range. Get the Group Data
	 * (Group and its child information) Call the aggregation for each calendar
	 * Id 
	 * Argument 1: Aggregation process Start Date 
	 * Argument 2: Aggregation process End Date
	 * ****************************************************************
	 */
	private void processSummaryData(Date startDate, Date endDate)
			throws GeneralException {

		// Object for RetailCalendarDAO
		RetailCalendarDAO objCalendarDao = new RetailCalendarDAO();

		/*
		 * // Object for 
		 */

		// Object for CompStoreDAO
		CompStoreDAO objStoreDao = new CompStoreDAO();

		// Object for SalesAggregationDailyDao
		SalesAggregationDailyDao objSADaily = new SalesAggregationDailyDao();
		
		// Object for SalesAggregationStatusDao
		SalesAggregationStatusDao objSAStat = new SalesAggregationStatusDao(); 
		
			
			// Object for SpecialCriteriaDTO
		SpecialCriteriaDTO objSpecialDto = new SpecialCriteriaDTO();

		try {

			List<RetailCalendarDTO> calendarList = null;

			// No repair process means get the processing calendar id from
			// retail calendar table.
			if (_repairFlag.equalsIgnoreCase("NORMAL")) {

				logger.debug("Get Calendar Id for given date range");
				calendarList = objCalendarDao.dayCalendarList(_conn,
						_startdate, _endDate, Constants.CALENDAR_DAY);
			}

			//Get the group and its child details
			getProductGroupData();

			//Get the UOM and its details
			getUomData();

			// Load the SpecialCriteria
			objSpecialDto = loadSpecialCriteria();

			//Get Location-id list for a give District / Store
			List<SummaryDataDTO> storeList = objStoreDao.getStoreNumebrs(_conn,
					_districtNum, _storeNum);

			logger.info("Total Stores count:" + storeList.size());

			//Iterate for each store
			for (int sL = 0; sL < storeList.size(); sL++) {

				SummaryDataDTO objStoreDto = storeList.get(sL);

				logger.info("Process begins at Store level. Store:"
						+ objStoreDto.getstoreNumber());

				if (_repairFlag.equalsIgnoreCase("FULL")) {

					logger.debug(" Repair Process Begins");

					// get the
					String repairStage = PropertyManager.getProperty(
							"REPAIR_PROCESS_STAGE", null);

					// get the not-processing calendar list from movement daily
					 logger.debug("Get not processed Calendar List");
					List<RetailCalendarDTO> repairCalendarList = new ArrayList<RetailCalendarDTO>();
					
					if (_sourceTable.equalsIgnoreCase("MD"))
						repairCalendarList = objSADaily.getNotProcessingCalendar(
							_conn, objStoreDto.getstoreNumber(), _startdate, repairStage);
					else
						repairCalendarList = objSADaily.getTLNotProcessingCalendar(
										_conn, objStoreDto.getLocationId());

					logger.debug("Total Non Processed Days:"
												+ repairCalendarList.size());

					for (int rC = 0; rC < repairCalendarList.size(); rC++) {

						RetailCalendarDTO calendarDTO = repairCalendarList
								.get(rC);

						logger.info("Process begins at Calendar level. Calendar Id:"
								+ calendarDTO.getCalendarId());

						// get last Year calendar id for a given calendar id
						logger.debug("Get last year Calendar Id");
						int lastCalendarId = objCalendarDao
								.getLastYearCalendarId(_conn,
										calendarDTO.getCalendarId());
						
						calendarDTO.setlstCalendarId(lastCalendarId);

						// Call Aggregation process for each calendar Id
						ComputeSummaryAggregationForDay(calendarDTO,
								 objStoreDto, objSADaily,
								 objSpecialDto, objSAStat);

						logger.debug("Process end for Calendar Id:"
												+ calendarDTO.getCalendarId());

					}
				}
				else if (_repairFlag.equalsIgnoreCase("NORMAL")) {

					logger.debug("Total Processing Calendar Id count:"
							+ calendarList.size());

					// Loop for each calendar Id
					for (int dd = 0; dd < calendarList.size(); dd++) {

						RetailCalendarDTO calendarDTO = calendarList.get(dd);

						logger.info("Process begins at Calendar level. Calendar Id:"
								+ calendarDTO.getCalendarId());

						// Call Aggregation process for each calendar Id
						ComputeSummaryAggregationForDay(calendarDTO,
								 objStoreDto, objSADaily,
								 objSpecialDto, objSAStat);

						logger.debug("Process end for Calendar Id:"
												+ calendarDTO.getCalendarId());
					}
				}
			}
		} 
		catch (Exception exe) {
			throw new GeneralException("processSummaryData", exe);
		}
	}

	/*
	 * ****************************************************************
	 * Get the Store Number and Visitor Count Call the aggregation process for
	 * each Store Number and calendar Id 
	 * Argument 1: RetailCalendarDTO (Calendar Id and Process Date) 
	 * Argument 2: MovementDAO 
	 * Argument 3: SummaryDataDTO 
	 * Argument 4: SalesAggregationDailyDao 
	 * Argument 5: SalesAggregationDao
	 * Argument 6 : CostDataLoadV2
	 * @throws ParseException, GeneralException
	 * ****************************************************************
	 */
	public void ComputeSummaryAggregationForDay(RetailCalendarDTO calendarDTO,
		SummaryDataDTO objStoreDto, SalesAggregationDailyDao objSADaily,
		SpecialCriteriaDTO objSpecialDto, SalesAggregationStatusDao objSAStat) 
				throws ParseException, GeneralException {

		try {

			if (!_updateOnlyPLMetrics){
				// delete the previous aggregation for the store and calendar id
				logger.debug("Delete previous Aggregation Data...");
				objSADaily.deletePreviousAggregation(_conn,
						calendarDTO.getCalendarId(), objStoreDto.getLocationId());
	
				logger.debug("Delete previous SA Status ...");
				objSAStat.deleteSAStatus(_conn, calendarDTO.getCalendarId(), 
						Constants.STORE_LEVEL_ID, objStoreDto.getLocationId());
				
				if ( _marginCalcReq){
					objSADaily.deletePreviousNoCostItemsInfo(_conn,
						calendarDTO.getCalendarId(), objStoreDto.getLocationId());
				}
			}
			
			// Call method to do sales aggregation for store
			ComputeDailyAggregationForStore(calendarDTO, objSADaily,
					calendarDTO, objStoreDto,
					objSpecialDto, objSAStat);
			logger.debug("Process end for Store:"
											+ objStoreDto.getstoreNumber());

		} catch (Exception exe) {
			logger.error(" Daily Aggregation Error....", exe);
			throw new GeneralException("ComputeSummaryAggregationForDay", exe);
		}
	}

	/*
	 * ****************************************************************
	 * Get the Movement Daily Data for the given Store and Date Call Function to
	 * Aggregate Data for Sub Cat, Cat and Department Call Function to Aggregate
	 * Data for Financial Department Call Function to Aggregate Data for
	 * Merchandise Department Call Function to Aggregate Data for Portfolio Call
	 * Function to Aggregate Data for Segment 
	 * Argument 1: RetailCalendarDTO (Calendar Id and Process Date) 
	 * Argument 2: StoreDTO (Store Number, Store Id and Visit Count) 
	 * Argument 3: SalesAggregationDailyDao 
	 * Argument 4: MovementDAO 
	 * Argument 5 :calendar_id 
	 * Argument 6 :SummaryDataDTO 
	 * Argument 7 :SalesAggregationDao 
	 * Argument 8 : CostDataLoadV2
	 * ****************************************************************
	 */
	private int ComputeDailyAggregationForStore(RetailCalendarDTO retailDTO,
			SalesAggregationDailyDao objSADaily, 
			RetailCalendarDTO objCalendarDto, SummaryDataDTO objStoreDto,
			SpecialCriteriaDTO objSpecialDto, SalesAggregationStatusDao objSAStat) throws GeneralException {

		// Object for Summary Sales Aggregation Business
		SalesaggregationbusinessV2 summaryBusiness = new SalesaggregationbusinessV2();
		
		// Object for CostDataLoadV2
		CostDataLoad objCostLoad = new CostDataLoad();
		
		// Object for SalesAggregationDDao
		SalesAggregationDao objSalesDao = new SalesAggregationDao();
		
		// Object for MovementDAO
		MovementDAO objMovement = new MovementDAO();
		
		// Objects to hold the summary information
		HashMap<Integer, HashMap<String, SummaryDataDTO>> productMap = 
							new HashMap<Integer, HashMap<String, SummaryDataDTO>>();

		// hold the last year summary daily id
		// key location_level_id and location_id
		// value Summary_Daily_Id
		HashMap<String, Long> lastSummaryList = new HashMap<String, Long>();

		int processStatus = 0;
		
		try {
			logger.debug("Get Movement data...");
			// add movement by volume
			// change the method and move the aggregation level into transaction
			// remove the two arguments specialDTO , _repairFlag
			String categoryIdStr = PropertyManager.getProperty("MOVEMENT_EXCLUDE_CATEGORY");
			
			List<MovementDailyAggregateDTO> movementDataList = new ArrayList<MovementDailyAggregateDTO>(); 
					
			
			if (_sourceTable.equalsIgnoreCase("MD"))
				movementDataList = objMovement.getMovementDailyDataV2(_conn, 
					objStoreDto.getstoreNumber(), objCalendarDto.getCalendarId(), 
									categoryIdStr, _leastProductLevel, _gasItems);
			else if (_sourceTable.equalsIgnoreCase("TL") )
				movementDataList = objMovement.getTransationDailyData(_conn, 
					objStoreDto.getLocationId(), objCalendarDto.getCalendarId(), 
									categoryIdStr, _leastProductLevel, _gasItems);
			
			if (movementDataList.size() > 0) {

				HashMap<String, Double> itemCostMap = new HashMap<String, Double>();
				
				// Block to fetch the Cost - Begins
				//Do the % calculation based on the configuration settings
				if ( _marginCalcReq){
					// get the distinct item code list from movement_daily for
					// getting deal cost
					logger.debug("Get distinct item code list");
					List<String> itemCodeList = GetItemList(objMovement, 
											objCalendarDto.getCalendarId(), 
												objStoreDto.getstoreNumber(), 
												objStoreDto.getLocationId());
					
					// get the deal cost info from cost loader for margin calculation
					logger.debug("Get the cost info");
					
					itemCostMap = objCostLoad.getRetailCost(_conn, 
								objStoreDto.getstoreNumber(), itemCodeList, 
											objCalendarDto.getCalendarId());
					
					if (itemCostMap.size() <1)
						logger.info("Cost data not found");
					else{
						/*for (String itemKey : itemCostMap.keySet()) {
						    logger.debug(itemKey + '|' + itemCostMap.get(itemKey));
						}*/
					}
				}
				// Block to fetch the Cost - Ends
				
				
				//Get store level visit count
				logger.debug(" Get Store Visit Count");
				Double storeVisitCount = 0.0;
				Double plStoreVisitCount = 0.0;
				if (_sourceTable.equalsIgnoreCase("MD")){
					
					if (!_updateOnlyPLMetrics){
						storeVisitCount = objMovement.getStoreVisitorSummary(
										_conn, objStoreDto.getstoreNumber(), 
							objCalendarDto.getCalendarId(), categoryIdStr, 0);
					}

					plStoreVisitCount = objMovement.getStoreVisitorSummary(
							_conn, objStoreDto.getstoreNumber(), 
							objCalendarDto.getCalendarId(), categoryIdStr, 1);
				}
				else if (_sourceTable.equalsIgnoreCase("TL")){
					if (!_updateOnlyPLMetrics){
						storeVisitCount = objMovement.getStoreVisitorSummaryTL(
								_conn, objStoreDto.getLocationId(), 
								objCalendarDto.getCalendarId(), categoryIdStr, 0);
					}
					
					plStoreVisitCount = objMovement.getStoreVisitorSummaryTL(
							_conn, objStoreDto.getLocationId(), 
							objCalendarDto.getCalendarId(), categoryIdStr, 1);
				}
				// Block to fetch the Coupon data - Begins
				//Get coupon data for coupon based aggregation based on configuration
				HashMap<String, Double> couponAggregation = new HashMap<String, Double>(); 
				 
				if (_couponAdjustmentRequired == 1){
					logger.debug(" Get the Coupon Data");
					couponAggregation = objMovement.getCouponBasedAggregation(
										_conn, objStoreDto.getstoreNumber(), 
											objCalendarDto.getCalendarId());
					
					if (couponAggregation.size()<1)
						logger.info("Coupon data not found");
				 }
				 else
					 logger.debug("There is no coupon adjustment");

				// Block to fetch the Coupon data - Ends

				//double unadjustedCouponRev = 0;
				//double totalcouponRev = 0;


				logger.debug("Start Data Aggregation for all levels");

				//calculateStoreSummary.
				
				HashMap<String, Double> noCostItemMap = 
										new HashMap<String, Double>(); 
				
				logger.debug("Store level Data Aggregation");
				summaryBusiness.calculateStoreSummary(movementDataList, 
							 productMap, objStoreDto, _uomMapDB, _uomMapPro, 
							storeVisitCount, plStoreVisitCount, couponAggregation, 
							itemCostMap, _gasItems, noCostItemMap, _marginCalcReq);
				
				logger.debug("least product leve Data Aggregation");
				for (int pl=0; pl < _leastProductLevel.size(); pl++) {
					
					int leaseProductLevelId = _leastProductLevel.get(pl);
					logger.debug("Populate data for Product Level ID:" + 
													leaseProductLevelId);
					
					HashMap<String, Integer> leastProductVisitMap = 
												new HashMap<String, Integer>();

					HashMap<String, Integer> leastPLProductVisitMap = 
							new HashMap<String, Integer>();
					
					if (_sourceTable.equalsIgnoreCase("MD")){
						if (!_updateOnlyPLMetrics){
							leastProductVisitMap = objMovement.getProductVisit(
											_conn, objStoreDto.getstoreNumber(), 
											objCalendarDto.getCalendarId(), 
													leaseProductLevelId, null, 0);
						}
						
						leastPLProductVisitMap = objMovement.getProductVisit(
								_conn, objStoreDto.getstoreNumber(), 
								objCalendarDto.getCalendarId(), 
										leaseProductLevelId, null, 1);
					
					}
					else if (_sourceTable.equalsIgnoreCase("TL")){
						if (!_updateOnlyPLMetrics){
							leastProductVisitMap = objMovement.getProductVisitTL(
									_conn, objStoreDto.getLocationId(), 
									objCalendarDto.getCalendarId(), 
											leaseProductLevelId, null, 0);
						}

						leastPLProductVisitMap = objMovement.getProductVisitTL(
								_conn, objStoreDto.getLocationId(), 
								objCalendarDto.getCalendarId(), 
										leaseProductLevelId, null, 1);
					}
						
					summaryBusiness.calculateProductSummary(
						movementDataList, productMap, objStoreDto, _uomMapDB, 
						_uomMapPro, leastProductVisitMap, leastPLProductVisitMap, 
						itemCostMap, leaseProductLevelId, _gasItems, 
												objSpecialDto, _marginCalcReq);
				}
				
				/* This block is for POS Department. It is exception from 
				 * dynamic product aggregation - Britto 18/04/2013 - Begins */
				//Get Visit count for POS Department
				// Is this block required - we have to decide
				HashMap<String, Integer> POSVisitMap = new HashMap<String, Integer>();
				//objMovement.getPOSVisit(_conn, objStoreDto.getstoreNumber(), objCalendarDto.getCalendarId(), 0); 

				HashMap<String, Integer> plPOSVisitMap = new HashMap<String, Integer>();
				//objMovement.getPOSVisit(_conn, objStoreDto.getstoreNumber(), objCalendarDto.getCalendarId(), 1); 				
				
				//Do aggregation for POS Department
				summaryBusiness.calculateProductSummary(
						movementDataList, productMap, objStoreDto, _uomMapDB, 
						_uomMapPro, POSVisitMap, plPOSVisitMap, itemCostMap, 
						Constants.POSDEPARTMENT, _gasItems, objSpecialDto, _marginCalcReq);
				
				//If coupon adjustment is required, do adjustment for POS Department
				if (_couponAdjustmentRequired == 1)
					summaryBusiness.adjustCouponInfo(productMap, couponAggregation);
				
				/* This block is for POS Department. It is exception from 
				 * dynamic product aggregation - Britto 18/04/2013 - Ends */
				
				ArrayList<ProductGroupDTO> _tempGroupList = new ArrayList<ProductGroupDTO>();

				for (int ii = 0; ii < _productGroup.size(); ii++) {
					ProductGroupDTO prDto = _productGroup.get(ii);
					_tempGroupList.add(prDto);
				}

				// Data aggregation for product
				logger.debug("Aggregate for product data based on least product");				
				productGroupAggrIterator(productMap, _tempGroupList,
						summaryBusiness, objMovement, objStoreDto.getstoreNumber(),  
						objStoreDto.getLocationId(), objCalendarDto.getCalendarId());
				
				String store_Status = "";

				if (!_updateOnlyPLMetrics){

					// get Last year summaryDaily List
					logger.debug("Get last year summary Ids");			
					lastSummaryList = objSalesDao.getLastYearSalesAggrList(_conn,
							retailDTO.getlstCalendarId(),
							objStoreDto.getLocationId(), Constants.STORE_LEVEL_ID,
							"SALES_AGGR_DAILY", "SUMMARY_DAILY_ID");
					
					//Get store identical/new status
					logger.debug("Get store identical/new status");
					store_Status = objSalesDao.getStoreStatus(_conn,
							objCalendarDto.getStartDate(), objStoreDto,
							Constants.CALENDAR_WEEK);

					// Call the InsertSummaryDailyBatch
					logger.debug("Insert Sales Aggregation data");
					objSADaily.insertSummaryDailyBatchV2(_conn,
							retailDTO.getCalendarId(), objStoreDto.getLocationId(),
					_productGroupType, productMap, lastSummaryList,store_Status);
					
					logger.debug("Insert Sales Aggregation status");
					objSAStat.insertSAStatus(_conn, retailDTO.getCalendarId(), 
						Constants.STORE_LEVEL_ID, objStoreDto.getLocationId());
	
					logger.debug("Update Process Flag");
					// Updating ProcessFlag as 'Y' in movemen_Daily
					// process added for repair Process
					// used to change non-process records to processed records
					
					if (_sourceTable.equalsIgnoreCase("MD"))
						objMovement.updateProcessFlag(_conn, 
							retailDTO.getCalendarId(), objStoreDto.getstoreNumber());
					else if (_sourceTable.equalsIgnoreCase("TL"))
						objMovement.updateTLProcessFlag(_conn, 
							retailDTO.getCalendarId(), objStoreDto.getLocationId());
					
					if (_marginCalcReq){
						objSADaily.insertNoCostItemData(_conn, 
							retailDTO.getCalendarId(), objStoreDto.getLocationId(),
							noCostItemMap);
					}
				}
				else{
					// Call the InsertSummaryDailyBatch
					logger.debug("Insert Sales Aggregation data");
					objSADaily.updateSummaryDailyBatchV2(_conn,
							retailDTO.getCalendarId(), objStoreDto.getLocationId(),
					_productGroupType, productMap, lastSummaryList,store_Status);
				}
					
				PristineDBUtil.commitTransaction(_conn,	"Commit Daily Aggregation Process");
				
				processStatus = 1;
			}
			else
			{
				logger.warn("Movement data not found");
			}
		} catch (Exception ex) {
			logger.error("Error in Daily Aggregation", ex);
			throw new GeneralException(" Daily Aggregation Error", ex);
		}
		
		return processStatus;
	}

	/*
	 * ****************************************************************
	 * Function to get the aggregation data for Finance Department,
	 * Merchandise Department, Portfolio and Segment 
	 * Argument 1: ProductGroupDTO List (group and its child info data) 
	 * Argument 2: groupChildTypeId
	 * Argument 3: Sub-category Map 
	 * Argument 4: Category Map 
	 * Argument 5: Department Map 
	 * Argument 6: summaryBusiness Object
	 * ****************************************************************
	 */

	private void productGroupAggr(ArrayList<ProductGroupDTO> proGroupData,
			HashMap<Integer, HashMap<String, SummaryDataDTO>> productMap,
			SalesaggregationbusinessV2 summaryBusiness, MovementDAO objMovement,
			String storeNumber, int storeId, int calendar_id) throws GeneralException {

		try {
			//String categoryIdStr = PropertyManager.getProperty("MOVEMENT_EXCLUDE_CATEGORY");

			for (int pM = 0; pM < proGroupData.size(); pM++) {
				ProductGroupDTO prodGroupobj = proGroupData.get(pM);
				
				// Find child availability
				if (productMap.containsKey(prodGroupobj.getChildLevelId())) {
					
					logger.debug("Processing for Product Level:" + prodGroupobj.getProductLevelId());
					
					HashMap<String, Integer> productVisitCount = 
												new HashMap<String, Integer>();

					HashMap<String, Integer> plProductVisitCount = 
							new HashMap<String, Integer>();
					boolean visitSum = true;
					
					if (CheckChildPOS(prodGroupobj.getChildProductData()) == 0)
					{
						logger.debug("Get Product visit count");
						
						if (_sourceTable.equalsIgnoreCase("MD")){
							
							if (!_updateOnlyPLMetrics){
								productVisitCount = objMovement.getProductVisit(_conn, 
								storeNumber, calendar_id, 
								prodGroupobj.getProductLevelId(), 
								prodGroupobj.getChildProductData(), 0);
							}
							
							plProductVisitCount = objMovement.getProductVisit(_conn, 
							storeNumber, calendar_id, 
							prodGroupobj.getProductLevelId(), 
							prodGroupobj.getChildProductData(), 1);
						}
						else if (_sourceTable.equalsIgnoreCase("TL")){
							if (!_updateOnlyPLMetrics){
								productVisitCount = objMovement.getProductVisitTL(_conn, 
								storeId, calendar_id, 
								prodGroupobj.getProductLevelId(), 
								prodGroupobj.getChildProductData(), 0);
							}
							
							plProductVisitCount = objMovement.getProductVisitTL(_conn, 
							storeId, calendar_id, 
							prodGroupobj.getProductLevelId(), 
							prodGroupobj.getChildProductData(), 1);
						}
						
						visitSum = false;
					}
					else if (prodGroupobj.getProductLevelId() == Constants.FINANCEDEPARTMENT)
					{
						if (_sourceTable.equalsIgnoreCase("MD")){
							if (!_updateOnlyPLMetrics){
								productVisitCount = objMovement.getProductVisitForFinDept(_conn, 
										storeNumber, calendar_id, prodGroupobj.getProductLevelId(), 0);
							}
							
							plProductVisitCount = objMovement.getProductVisitForFinDept(_conn, 
									storeNumber, calendar_id, prodGroupobj.getProductLevelId(), 1);							
						}
						else if (_sourceTable.equalsIgnoreCase("TL")){
							if (!_updateOnlyPLMetrics){
								productVisitCount = objMovement.getProductVisitForFinDeptTL(_conn, 
										storeId, calendar_id, prodGroupobj.getProductLevelId(), 0);
							}							

							plProductVisitCount = objMovement.getProductVisitForFinDeptTL(_conn, 
									storeId, calendar_id, prodGroupobj.getProductLevelId(), 1);
						}
						
						visitSum = false;
					}
					
					summaryBusiness.calculateGroupMetricsV2(productMap,
							prodGroupobj, productVisitCount, plProductVisitCount, visitSum);

					proGroupData.remove(prodGroupobj);
				}
			}
		} catch (Exception exe) {
			logger.error("Error while doing product Aggregation ", exe);
			throw new GeneralException(" Daily Aggregation Error....", exe);
		}
	}
	
	
	private  int CheckChildPOS(ArrayList<ProductGroupDTO> childProdutLevelData)
	{
		int posExist = 0;
		
		if (childProdutLevelData !=null && childProdutLevelData.size() > 0){
			
			for(int i=0; i < childProdutLevelData.size(); i++){
				
				ProductGroupDTO childProductDTO = childProdutLevelData.get(i);
				
				if (childProductDTO.getChildLevelId() == Constants.POSDEPARTMENT)
					posExist =1;
			}
		}
		else
			posExist = 1;
		
		return posExist;
		
	}

	/*
	 * ****************************************************************
	 * Function to get the aggregation data for Financial Department,
	 * Merchandise Department, Portfolio and Segment 
	 * Argument 1: ProductGroupDTO List (group and its child info data) 
	 * Argument 2: groupChildTypeId
	 * Argument 3: Sub-category Map 
	 * Argument 4: Category Map 
	 * Argument 5: Department Map 
	 * Argument 6: summaryBusiness Object
	 * ****************************************************************
	 */

	private void productGroupAggrIterator(
			HashMap<Integer, HashMap<String, SummaryDataDTO>> productMap,
			ArrayList<ProductGroupDTO> proGroupData,
			SalesaggregationbusinessV2 summaryBusiness, MovementDAO objMovement,
			String StoreNumber, int storeId, int calendar_id) throws GeneralException {
		try {
			int maxIteration = 0;
			while (proGroupData.size() > 0) {
				productGroupAggr(proGroupData, productMap, summaryBusiness,
						objMovement, StoreNumber, storeId, calendar_id);
				maxIteration++;
				if (maxIteration > 15)
					break;

			}
		} catch (Exception exe) {
			logger.error(" Daily Aggregation Error....", exe);
			throw new GeneralException(" Daily Aggregation Error....", exe);
		}
	}

	/*
	 * ****************************************************************
	 * Method to get the Group Data information
	 * ****************************************************************
	 */
	public void getProductGroupData() throws GeneralException, SQLException {

		SalesAggregationProductGroupDAO objProdGroupDAO = new SalesAggregationProductGroupDAO();

		//Get product group type data 
		logger.debug("Get Product Type to check aggregation required or not");
		_productGroupType = objProdGroupDAO.getProductGroupTypeData(_conn);
		
		if (_productGroupType.size() > 0 ){

			//Get the list of product mapped with item
			logger.debug("Get least product level which is mapped with Item");
			_leastProductLevel = objProdGroupDAO.getLeastProductParent(_conn);

			if (_leastProductLevel.size() < 1){
				logger.warn("There is no product data with mapped with Item, Aggregation cannot be done");
				System.exit(0);
			}
			
			//Get product group and its child information
			logger.debug("Get Product group master data product above Item");
			_productGroup = objProdGroupDAO.getProductGroupDetails(_conn);
			
			if (_productGroup.size() > 1){
				if (_productGroup.size() > 0){
					for (int l=0; l < _productGroup.size(); l++){
						
					}
				}
			}
			else
				logger.info("There is no Product Group data");

			
			
			//Get item_code for GAS UPCs
			String gasUPS = PropertyManager.getProperty("SA_GAS_UPC", null);
			String[] UPSArray = gasUPS.split(",");
			StringBuffer gasUPSString = new StringBuffer();;

			for (int ii = 0; ii < UPSArray.length; ii++) {
				if (ii > 0) 
					gasUPSString.append(", ");
				
					gasUPSString.append("'").append(UPSArray[ii]).append("'");
			}
		
			_gasItems = objProdGroupDAO.getItemForGasUPC(_conn, gasUPSString.toString());
		
		}
		else{
			logger.info("There is no Product Group Type data");
			System.exit(1);
		}
		
	}

	/*
	 * Method used to get the uom data from UOM_LOOKUP table
	 */

	public void getUomData() throws GeneralException {

		// Object for ItemDao
		ItemDAO objItemDao = new ItemDAO();
		logger.debug("Get UOM master data");
		_uomMapDB = objItemDao.getUOMList(_conn, "SALES_AGGR_DAILY");
		
		if (_uomMapDB.size() < 1)
			logger.info("There is no UOM data");

	}

	/*
	 * ****************************************************************
	 * Load the SpecialCriteria and uom for movement by volume
	 * Argument 1 : specialDTO Return specialDTO
	 * ****************************************************************
	 */

	public SpecialCriteriaDTO loadSpecialCriteria() {

		// added for getting UOM details for Movement by volume
		String uomName = PropertyManager.getProperty("MOVEMENT_VOLUME_UOM",
				null);
		logger.info("UOMs for Movement by volume calculation:" + uomName);

		String couponPosId = PropertyManager.getProperty(
				"COUPON_POS_DEPARTMENT", null);
		logger.info("Coupon POS Department ID:" + couponPosId);

		// Getting the values for identify the gas value
		String storeGasDetails = PropertyManager.getProperty(
				"STATE_BASED_GAS_VALUE", null);
		logger.info("State based Gas Revenue:" + storeGasDetails);

		// Object for SpecialCriteriaDTO
		SpecialCriteriaDTO specialDTO = new SpecialCriteriaDTO();

		// add the uom Name to hashMap
		String[] uomArray = uomName.split(",");

		for (int ii = 0; ii < uomArray.length; ii++) {
			_uomMapPro.put(uomArray[ii], uomArray[ii]);
		}

		if (couponPosId != null) {
			specialDTO.setcouponPosId(Integer.valueOf(couponPosId));
		} else {
			logger.error("COUPON_POS_DEPARTMENT property Missed in Analysis file");
			System.exit(1);
		}

		if (storeGasDetails != null) {
			HashMap<String, Double> gasValueMap = new HashMap<String, Double>();
			String[] gasArray = storeGasDetails.split(",");
			for (int i = 0; i < gasArray.length; i++) {
				String[] splitedValue = gasArray[i].split("=");
				gasValueMap.put(splitedValue[0].trim(),
						Double.valueOf(splitedValue[1]));

			}
			specialDTO.setstoreBasedGasValue(gasValueMap);
		} else {
			logger.error("STATE_BASED_GAS_VALUE property Missed in Analysis file");
			System.exit(1);
		}

		// Getting the values for identify the gas value
		_couponAdjustmentRequired = Integer.parseInt(PropertyManager.getProperty(
				"SALES_ANALYSIS.COUPON_ADJUSTMENT", "0"));

		// Get configuration to check margin calculation is required or not 
		String marginReq = PropertyManager.getProperty("MARGIN_CALCULATION_REQUIRED",null);
		if ( marginReq.equalsIgnoreCase("YES"))
			_marginCalcReq = true; 
		
		
		if (_couponAdjustmentRequired == 1)
			logger.debug("Coupon adjustment is required");
		else
			logger.debug("Coupon adjustment is not required");

			return specialDTO;
	}

	private List<String> GetItemList(MovementDAO objMovement, int calendarId, String storeNo, int storeId) throws GeneralException{
		List<String> itemCodeList = new ArrayList<String>();
		if (_sourceTable.equalsIgnoreCase("MD"))
			itemCodeList = objMovement.getDistinctItemCodeMD(
				_conn, storeNo, calendarId);
		else if (_sourceTable.equalsIgnoreCase("TL"))
			itemCodeList = objMovement.getDistinctItemCodeTL(
				_conn, storeId, calendarId);
		
		return itemCodeList;
		
	}
	
	/**
	 ***************************************************************** 
	 * Static method to log the command line arguments
	 ***************************************************************** 
	 */
	private static void logCommand(String[] args) {
		StringBuffer sb = new StringBuffer("Command: SummaryDailyV2 ");
		for (int ii = 0; ii < args.length; ii++) {
			if (ii > 0)
				sb.append(' ');
			sb.append(args[ii]);
		}

		logger.info(sb.toString());
	}

}