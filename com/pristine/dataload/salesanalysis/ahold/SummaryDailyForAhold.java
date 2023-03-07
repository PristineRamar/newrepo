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
 *******************************************************************************
 */
package com.pristine.dataload.salesanalysis.ahold;

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
import com.pristine.business.entity.ahold.SalesaggregationbusinessV2;
import com.pristine.dao.CompStoreDAO;
import com.pristine.dao.DBManager;
import com.pristine.dao.ItemDAO;
import com.pristine.dao.ItemMetricsSummaryDAO;
import com.pristine.dao.MovementDAO;
import com.pristine.dao.RetailCalendarDAO;
import com.pristine.dao.salesanalysis.SalesAggregationDao;
import com.pristine.dao.salesanalysis.SalesAggregationProductGroupDAO;
import com.pristine.dao.salesanalysis.SalesAggregationDailyDao;
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

public class SummaryDailyForAhold {

	static Logger logger = Logger.getLogger("SummaryDaily");
	private String _storeNum = null; // Processing store number
	private String _districtNum = null; // Processing district number
	private Connection _conn = null; // DB connection
	private Date _startdate = null; // Processing StartDate
	private Date _endDate = null; // Processing EndDate
	private String _repairFlag = "NORMAL"; // Processing RepairFlagc
	private HashMap<String, String> _uomMapDB = new HashMap<String, String>();
	private HashMap<String, String> _uomMapPro = new HashMap<String, String>();
	// Product Group and its child info
	ArrayList<ProductGroupDTO> _productGroup = new ArrayList<ProductGroupDTO>();
	
	//Array to hold list of product which has item as child
	ArrayList<Integer> _leastProductLevel = new ArrayList<Integer>();
	HashMap<Integer, Integer> _productGroupType = new HashMap<Integer, Integer>();
	HashMap<Integer, Integer> _gasItems = new HashMap<Integer, Integer>(); 
	int _couponAdjustmentRequired = 0;
	String _revenuePercCalc = null;
	int _visitCalcRequired = 0;

	/*
	 * ****************************************************************
	 * Class constructor
	 * 
	 * @throws Exception,GeneralException
	 * ****************************************************************
	 */
	public SummaryDailyForAhold(Date startDate, Date endDate, String storeno,
			String district, String repairFlag) {

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
		DateFormat dateFormat = new SimpleDateFormat("MM/dd/yyyy");
		SummaryDailyForAhold summaryDaily = null;
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

			}

			// Create object for summary daily
			summaryDaily = new SummaryDailyForAhold(startDate, endDate, storeno,
					district, repairFlag);

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
					List<RetailCalendarDTO> repairCalendarList = objSADaily
							.getNotProcessingCalendar(_conn,
									objStoreDto.getstoreNumber(), _startdate,
									repairStage);

					logger.debug("Total Non Processed Days:"
												+ repairCalendarList.size());

					for (int rC = 0; rC < repairCalendarList.size(); rC++) {

						RetailCalendarDTO calendarDTO = repairCalendarList
								.get(rC);

						logger.info("Process begins at Calendar level. Calendar Id:"
								+ calendarDTO.getCalendarId());

						// Move the non Processed Calendar id based
						// Sales_Aggr_Daily records moved to temp Table.
						logger.debug("Move existing daily data to temp table");
						objSADaily.moveTempTable(_conn,
								calendarDTO.getCalendarId(),
								objStoreDto.getLocationId());

						// get last Year calendar id for a given calendar id
						logger.debug("Get last year Calendar Id");
						int lastCalendarId = objCalendarDao
								.getLastYearCalendarId(_conn,
										calendarDTO.getCalendarId());
						logger.debug("Last year Calendar Id :" + calendarDTO.getCalendarId());
						
						calendarDTO.setlstCalendarId(lastCalendarId);

						// Call Aggregation process for each calendar Id
						ComputeSummaryAggregationForDay(calendarDTO,
								 objStoreDto, objSADaily,
								 objSpecialDto, objCalendarDao);

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
								 objSpecialDto, objCalendarDao);

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
			 SummaryDataDTO objStoreDto,
			SalesAggregationDailyDao objSADaily,
			SpecialCriteriaDTO objSpecialDto,
			RetailCalendarDAO objCalendarDao) throws ParseException,
			GeneralException {

		try {

			// delete the previous aggregation for the store and calendar id
			logger.debug("Delete previous Aggregation Data...");
			objSADaily.deletePreviousAggregation(_conn,
					calendarDTO.getCalendarId(), objStoreDto.getLocationId());

			// Call method to do sales aggregation for store
			ComputeDailyAggregationForStore(calendarDTO, objSADaily,
					calendarDTO, objStoreDto,
					objSpecialDto, objCalendarDao);
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
			SpecialCriteriaDTO objSpecialDto, RetailCalendarDAO objCalendarDao)
													throws GeneralException {

		// Object for Summary Sales Aggregation Business
		SalesaggregationbusinessV2 summaryBusiness = new SalesaggregationbusinessV2();
		
		// Object for CostDataLoadV2
		CostDataLoad objCostLoad = new CostDataLoad();
		
		// Object for SalesAggregationDDao
		SalesAggregationDao objSalesDao = new SalesAggregationDao();
		
		// Object for MovementDAO
		MovementDAO objMovement = new MovementDAO();
		ItemMetricsSummaryDAO objItemMetrics = new ItemMetricsSummaryDAO();
		
		// Objects to hold the summary information
		HashMap<Integer, HashMap<String, SummaryDataDTO>> productMap = 
							new HashMap<Integer, HashMap<String, SummaryDataDTO>>();

		// hold the last year summary daily id
		// key location_level_id and location_id
		// value Summary_Daily_Id
		HashMap<String, Long> lastSummaryList = new HashMap<String, Long>();

		int processStatus = 0;
		
		try {
			
			logger.debug("Get Weekly Calendar ID...");
			RetailCalendarDTO weekCalDto = objCalendarDao.getCalendarIdForWeek(_conn, objCalendarDto.getCalendarId());			
			
			logger.debug("Get Movement data...");
			// add movement by volume
			// change the method and move the aggregation level into transaction
			// remove the two arguments specialDTO , _repairFlag
			String categoryIdStr = PropertyManager.getProperty("MOVEMENT_EXCLUDE_CATEGORY");
			
			List<MovementDailyAggregateDTO> movementDataList = objItemMetrics
					.getItemWeeklySummaryByDayCalendarId(_conn, 
							objStoreDto.getLocationId(), weekCalDto.getCalendarId(), 
									categoryIdStr, _leastProductLevel, _gasItems);

			if (movementDataList.size() > 0) {

				HashMap<String, Double> dealCostMap = new HashMap<String, Double>();
				
				// Block to fetch the Deal cost - Begins
				//Do the % calculation based on the configutation settings
				if ( _revenuePercCalc.equalsIgnoreCase("YES")){
					// get the distinct item code list from movement_daily for
					// getting deal cost
					logger.debug("Get distinct item code list");
					List<String> itemCodeList = objMovement.getDistinctItemCode(
							_conn, objStoreDto.getstoreNumber(), objCalendarDto.getCalendarId());
					
					// get the deal cost info from cost loader for margin calculation
					logger.debug("Get the cost info");
					dealCostMap = objCostLoad.getRetailCost(_conn, 
								objStoreDto.getstoreNumber(), itemCodeList, 
											objCalendarDto.getCalendarId());
					
					if (dealCostMap.size() <1)
						logger.info("Cost data not found");
				}
				// Block to fetch the Deal cost - Endss
				
				//Get store level visit count
				Double storeVisitCount = 0.0;
				if (_visitCalcRequired == 1) {				
					logger.debug(" Get Store Visit Count");
					storeVisitCount = objMovement.getStoreVisitorSummary(
									_conn, objStoreDto.getstoreNumber(), 
									objCalendarDto.getCalendarId(), categoryIdStr, 0);		
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
				logger.debug("Store level Data Aggregation");
				summaryBusiness.calculateStoreSummary(movementDataList, 
						productMap, objStoreDto, _uomMapDB, _uomMapPro, 
						storeVisitCount, couponAggregation, dealCostMap, 
															_gasItems);
				
				logger.debug("least product leve Data Aggregation");
				for (int pl=0; pl < _leastProductLevel.size(); pl++) {
					
					int leaseProductLevelId = _leastProductLevel.get(pl);
					logger.debug("Populate data for Product Level ID:" + 
													leaseProductLevelId);
					
					HashMap<String, Integer> leastProductVisitMap = 
												new HashMap<String, Integer>();
					
					leastProductVisitMap = objMovement.getProductVisit(
										_conn, objStoreDto.getstoreNumber(), 
										objCalendarDto.getCalendarId(), 
												leaseProductLevelId, null, 0);
					
					summaryBusiness.calculateProductSummary(
						movementDataList, productMap, objStoreDto, _uomMapDB, 
						_uomMapPro, leastProductVisitMap, dealCostMap, 
								leaseProductLevelId, _gasItems, objSpecialDto);
				
				}
				
				/* This block is for POS Department. It is exception from 
				 * dynamic product aggregation - Britto 18/04/2013 - Begins */
				//Get Visit count for POS Department
				HashMap<String, Integer> POSVisitMap = new HashMap<String, Integer>();
				
				if (_visitCalcRequired == 1){
					POSVisitMap = objMovement.getPOSVisit(
							_conn, objStoreDto.getstoreNumber(), objCalendarDto.getCalendarId(), 0); 
				}
										
				//Do aggregation for POS Department
				summaryBusiness.calculateProductSummary(
						movementDataList, productMap, objStoreDto, _uomMapDB, 
						_uomMapPro, POSVisitMap, dealCostMap, 
						Constants.POSDEPARTMENT, _gasItems, objSpecialDto);
				
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
						summaryBusiness, objMovement,
						objStoreDto.getstoreNumber(), objCalendarDto.getCalendarId());

				// get Last year summaryDaily List
				logger.debug("Get last year summary Ids");			
				lastSummaryList = objSalesDao.getLastYearSalesAggrList(_conn,
						retailDTO.getlstCalendarId(),
						objStoreDto.getLocationId(), Constants.STORE_LEVEL_ID,
						"SALES_AGGR_DAILY", "SUMMARY_DAILY_ID");
				
				//Get store identical/new status
				logger.debug("Get store identical/new status");
				String store_Status = objSalesDao.getStoreStatus(_conn,
						objCalendarDto.getStartDate(), objStoreDto,
						Constants.CALENDAR_WEEK);

				// Call the InsertSummaryDailyBatch
				logger.debug("Insert Sales Aggregation data");
				objSADaily.insertSummaryDailyBatchV2(_conn,
						retailDTO.getCalendarId(), objStoreDto.getLocationId(),
				_productGroupType, productMap, lastSummaryList,store_Status);

				logger.debug("Update Process Flag");
				// Updating ProcessFlag as 'Y' in movemen_Daily
				// process added for repair Process
				// used to change non-process records to processed records
				objMovement.updateProcessFlag(_conn, retailDTO.getCalendarId(),
						objStoreDto.getstoreNumber());

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
			String storeNumber, int calendar_id) throws GeneralException {

		try {
			//String categoryIdStr = PropertyManager.getProperty("MOVEMENT_EXCLUDE_CATEGORY");

			for (int pM = 0; pM < proGroupData.size(); pM++) {
				ProductGroupDTO prodGroupobj = proGroupData.get(pM);
				
				// Find child availability
				if (productMap.containsKey(prodGroupobj.getChildLevelId())) {
					
					logger.debug("Processing for Product Level:" + prodGroupobj.getProductLevelId());
					
					HashMap<String, Integer> productVisitCount = 
												new HashMap<String, Integer>();

					boolean visitSum = true;
					
					if (CheckChildPOS(prodGroupobj.getChildProductData()) == 0)
					{
						
						if (_visitCalcRequired == 1){
							logger.debug("Get Product visit count");
							productVisitCount = objMovement.getProductVisit(_conn, 
								storeNumber, calendar_id, 
								prodGroupobj.getProductLevelId(), 
								prodGroupobj.getChildProductData(), 0);
							
							visitSum = false;
						}
					}
					else if (prodGroupobj.getProductLevelId() == Constants.FINANCEDEPARTMENT)
					{
						if (_visitCalcRequired == 1){
							productVisitCount = objMovement.getProductVisitForFinDept(_conn, 
									storeNumber, calendar_id, prodGroupobj.getProductLevelId(), 0);
								
								visitSum = false;
						}
					}
					
					summaryBusiness.calculateGroupMetricsV2(productMap,
							prodGroupobj, productVisitCount, visitSum);

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
			String storeNumber, int calendar_id) throws GeneralException {
		try {
			int maxIteration = 0;
			while (proGroupData.size() > 0) {
				productGroupAggr(proGroupData, productMap, summaryBusiness,
						objMovement, storeNumber, calendar_id);
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

		_revenuePercCalc = PropertyManager.getProperty("MARGIN_CALCULATION_REQUIRED",null);

		_visitCalcRequired = Integer.parseInt(PropertyManager.getProperty(
				"SALES_ANALYSIS.VISIT_CALCULATION", "0"));		

		
		if (_couponAdjustmentRequired == 1)
			logger.debug("Coupon adjustment is required");
		else
			logger.debug("Coupon adjustment is not required");

			return specialDTO;
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