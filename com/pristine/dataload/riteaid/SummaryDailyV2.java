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
 * Version 0.4  07/08/2012  Dinesh Kumar    Ctd seq name changed
 * Version 0.5  14/08/2012  Dinesh Kumar    Repair Full Process added
 * Version 0.6  22/08/2012  Dinesh Kumar    Margin Calculation added
 * Version 0.7  04/09/2012  Dinesh Kumar    Prmo aggregation added into finance department process
 *******************************************************************************
 */
package com.pristine.dataload.riteaid;

import java.sql.Connection;
import java.text.DateFormat;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.Calendar;
import java.util.Date;
import java.util.HashMap;
import java.util.List;
import org.apache.log4j.Logger;
import org.apache.log4j.PropertyConfigurator;
import com.pristine.business.entity.riteaid.Salesaggregationbusiness;
import com.pristine.dao.CompStoreDAO;
import com.pristine.dao.DBManager;
import com.pristine.dao.ItemDAO;
import com.pristine.dao.riteaid.MovementDAO;
import com.pristine.dao.RetailCalendarDAO;
import com.pristine.dao.salesanalysis.SalesAggregationDao;
import com.pristine.dao.salesanalysis.SalesAggregationDailyDao;
import com.pristine.dataload.tops.CostDataLoad;
import com.pristine.dto.MovementDailyAggregateDTO;
import com.pristine.dto.RetailCalendarDTO;
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
	private String _repairFlag = null; // Processing RepairFlagc
	private HashMap<String, String> _uomMapDB = new HashMap<String, String>();
	private HashMap<String, String> _uomMapPro = new HashMap<String, String>();

	/*
	 * ****************************************************************
	 * Class constructor
	 * 
	 * @throws Exception,GeneralException
	 * ****************************************************************
	 */
	public SummaryDailyV2(Date startDate, Date endDate, String storeno,
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

		// logger.info("Processing Daily Summary Starts ");

		if (endDate != null) {
			logger.info("Summary Daily Process Start Date......"
					+ startDate.toString());
			logger.info("Summary Daily Process End  Date......."
					+ endDate.toString());

		} else {
			logger.info("Summary Daily Process Date............"
					+ startDate.toString());
			endDate = startDate;
		}

		if (storeno != "")
			logger.info("Summary Daily Process Store..........." + storeno);

		if (district != "")
			logger.info("Summary Daily Process District........" + district);

		if (repairFlag == null) {
			logger.error(" Repair Flag is missing in Input");
			System.exit(1);

		}

		_storeNum = storeno;
		_districtNum = district;
		_startdate = startDate;
		_endDate = endDate;
		_repairFlag = repairFlag;

		PropertyManager.initialize("analysis.properties");
		try {
			_conn = DBManager.getConnection();

		} catch (GeneralException exe) {
			logger.error(" Connection Failed.....", exe);
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
		logger.info("Summary Daily Process Starts");
		logCommand(args);
		Date startDate = null;
		Date endDate = null;
		String storeno = "";
		String district = "";

		// Variable added for repair process
		String repairFlag = "";
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

				/*
				 * Get the End date if process is for date range This date is
				 * valid only if start date is specified in input
				 */
				if (arg.startsWith("ENDDATE")) {
					String Inputdate = arg.substring("ENDDATE=".length());
					try {
						endDate = dateFormat.parse(Inputdate);
					} catch (ParseException par) {
						logger.error("End Date Parsing Error, checkInput");
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
			summaryDaily = new SummaryDailyV2(startDate, endDate, storeno,
					district, repairFlag);

			// Call summary calculation process
			summaryDaily.processSummaryData(startDate, endDate);

		} catch (GeneralException e) {
			logger.error(e);
		} finally {

			PristineDBUtil.close(summaryDaily._conn);
		}

	}

	/*
	 * ****************************************************************
	 * Get the Calendar Id list for the given date range. Get the Group Data
	 * (Group and its child information) Call the aggregation for each calendar
	 * Id Argument 1: Aggregation process Start Date Argument 2: Aggregation
	 * process End Date
	 * ****************************************************************
	 */
	private void processSummaryData(Date startDate, Date endDate)
			throws GeneralException {

		// Object for RetailCalendarDAO
		RetailCalendarDAO objCalendarDao = new RetailCalendarDAO();

		/*
		 * // Object for SpecialCriteriaDTO SpecialCriteriaDTO objSpecialDto =
		 * new SpecialCriteriaDTO();
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
			// retailcalendar table.
			if (_repairFlag.equalsIgnoreCase("NORMAL")) {

				calendarList = objCalendarDao.dayCalendarList(_conn,
						_startdate, _endDate, Constants.CALENDAR_DAY);
			}

			// get the uom and its details
			getUomData();

			// Load the SpecialCriteria
			objSpecialDto = loadSpecialCriteria();

			/* loadCustomerProperties(objSpecialDto); */

			// get Location-id list for a give District / Store
			List<SummaryDataDTO> storeList = objStoreDao.getStoreNumebrs(_conn,
					_districtNum, _storeNum);

			logger.info("Total Processing Store count.........."
					+ storeList.size());

			// itreate the storeList in loop
			for (int sL = 0; sL < storeList.size(); sL++) {

				SummaryDataDTO objStoreDto = storeList.get(sL);

				logger.info("Process begin for Store............... "
						+ objStoreDto.getstoreNumber());

				logger.info(" Repair Flag is..... " + _repairFlag);

				if (_repairFlag.equalsIgnoreCase("FULL")) {

					logger.info(" Repair Process Begins.......");

					// get the
					String repairStage = PropertyManager.getProperty(
							"REPAIR_PROCESS_STAGE", null);

					// get the not-processing calendar list from movement daily
					List<RetailCalendarDTO> repairCalendarList = objSADaily
							.getNotProcessingCalendar(_conn,
									objStoreDto.getstoreNumber(), _startdate,
									repairStage);

					logger.info(" Total Non Processing Calendar List.... "
							+ repairCalendarList.size());

					for (int rC = 0; rC < repairCalendarList.size(); rC++) {

						RetailCalendarDTO calendarDTO = repairCalendarList
								.get(rC);

						logger.info("Process start for Calendar Id..........."
								+ calendarDTO.getCalendarId());

						// Move the non Processed Calendar id based
						// Sales_Aggr_Daily records moved to temp Table.
						objSADaily.moveTempTable(_conn,
								calendarDTO.getCalendarId(),
								objStoreDto.getLocationId());

						// get last Year calendar id for a given calendar id
						int lastCalendarId = objCalendarDao
								.getLastYearCalendarId(_conn,
										calendarDTO.getCalendarId());

						calendarDTO.setlstCalendarId(lastCalendarId);

						// Call Aggregation process for each calendar Id
						ComputeSummaryAggregationForDay(calendarDTO,
								 objStoreDto, objSADaily,
								 objSpecialDto);

						logger.info("Process end for Calendar Id..........."
								+ calendarDTO.getCalendarId());

					}

				}

				else if (_repairFlag.equalsIgnoreCase("NORMAL")) {

					logger.info(" Total Processing Calendar List.... "
							+ calendarList.size());

					// Loop for each calendar Id
					for (int dd = 0; dd < calendarList.size(); dd++) {

						RetailCalendarDTO calendarDTO = calendarList.get(dd);

						logger.info("Process start for Calendar Id..........."
								+ calendarDTO.getCalendarId());

						// Call Aggregation process for each calendar Id
						ComputeSummaryAggregationForDay(calendarDTO,
								 objStoreDto, objSADaily,
								 objSpecialDto);

						logger.info("Process end for Calendar Id..........."
								+ calendarDTO.getCalendarId());
					}

				}

			}

		} catch (Exception exe) {
			logger.error(" Daily Aggregation Error....", exe);
			throw new GeneralException("processSummaryData", exe);
		}
	}

	/*
	 * ****************************************************************
	 * Get the Store Number and Visitor Count Call the aggregation process for
	 * each Store Number and calendar Id Argument 1: RetailCalendarDTO (Calendar
	 * Id and Process Date) Argument 2: MovementDAO Argument 3: SummaryDataDTO
	 * Argument 4: SalesAggregationDailyDao Argument 5: SalesAggregationDao
	 * Arguemnt 6 : CostDataLoadV2
	 * 
	 * @throws ParseException, GeneralException
	 * ****************************************************************
	 */
	public void ComputeSummaryAggregationForDay(RetailCalendarDTO calendarDTO,
			 SummaryDataDTO objStoreDto,
			SalesAggregationDailyDao objSADaily,
			SpecialCriteriaDTO objSpecialDto) throws ParseException,
			GeneralException {

		try {

			// delete the previous aggregation for the store and calendar id
			logger.info("Delete previous Aggregation Data....");
			objSADaily.deletePreviousAggregation(_conn,
					calendarDTO.getCalendarId(), objStoreDto.getLocationId());

			// Call method to do sales aggregation for store
			ComputeDailyAggregationForStore(calendarDTO, objSADaily,
					calendarDTO, objStoreDto,
					objSpecialDto);
			logger.info("Process end for Store................."
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
	 * Merchantise Department Call Function to Aggregate Data for Portfolio Call
	 * Function to Aggregate Data for Segment 
	 * Argument 1: RetailCalendarDTO (Calendar Id and Process Date) 
	 * Argument 2: StoreDTO (Store Number, Store Id and Visit Count) 
	 * Argument 3: SalesAggregationDailyDao 
	 * Argument 4: MovementDAO 
	 * Argument 5: calendar_id 
	 * Argument 6: SummaryDataDTO 
	 * Argument 7: SalesAggregationDao 
	 * Argument 8: CostDataLoadV2
	 * ****************************************************************
	 */
	private void ComputeDailyAggregationForStore(RetailCalendarDTO retailDTO,
			SalesAggregationDailyDao objSADaily, 
			RetailCalendarDTO objCalendarDto, SummaryDataDTO objStoreDto,
			SpecialCriteriaDTO objSpecialDto) throws GeneralException {

		// Object for Summary Sales Aggregation Business
		Salesaggregationbusiness summaryBusiness = new Salesaggregationbusiness();
		
		// Object for CostDataLoadV2
		CostDataLoad objCostLoad = new CostDataLoad();
		
		// Object for SalesAggregationDDao
		SalesAggregationDao objSalesDao = new SalesAggregationDao();
		
		// Object for MovementDAO
		MovementDAO objMovement = new MovementDAO();
		
		// Objects to hold the summary information
		HashMap<Integer, HashMap<String, SummaryDataDTO>> productMap = 
							new HashMap<Integer, HashMap<String, SummaryDataDTO>>();

		// hold the last year summarydaily id
		// key location_level_id and location_id
		// value Summary_Daily_Id
		HashMap<String, Long> lastSummaryList = new HashMap<String, Long>();

		try {
			logger.info("Get Movement data.....");

			// add movement by voulum
			// change the method and move the aggregation level into transaction
			// remove the two arguments specialDTO , _repairFlag
			List<MovementDailyAggregateDTO> movementDataList = objMovement
					.getMovementDailyData(_conn, objStoreDto.getstoreNumber(),
							objCalendarDto.getCalendarId());

			if (movementDataList.size() > 0) {

				// get the distinct item code list from movement_daily for
				// getting deal cost
				List<String> itemCodeList = objMovement.getDistinctItemCode(
						_conn, objStoreDto.getstoreNumber(), objCalendarDto.getCalendarId());

				// get the deal cost info from costloader for margin calculation
				HashMap<String, Double> dealCostMap = objCostLoad
						.getRetailCost(_conn, objStoreDto.getstoreNumber(),
								itemCodeList, objCalendarDto.getCalendarId());

				logger.debug(" Get Sub category Visit Count.....");
				// get sub category visit count
				HashMap<String, Integer> subCateVisitMap = objMovement
						.VisitorSummary(_conn, objStoreDto.getstoreNumber(),
								objCalendarDto.getCalendarId(), Constants.SUBCATEGORYLEVELID);

				logger.debug(" Get Category Visit Count......");
				// get Category Visit count
				HashMap<String, Integer> CateVisitMap = objMovement
						.VisitorSummary(_conn, objStoreDto.getstoreNumber(),
								objCalendarDto.getCalendarId(), Constants.CATEGORYLEVELID);

				logger.debug(" Get Department Visit Count.....");
				// get Department Visit Map
				HashMap<String, Integer> departmentVisitMap = objMovement
						.VisitorSummary(_conn, objStoreDto.getstoreNumber(),
								objCalendarDto.getCalendarId(), Constants.DEPARTMENTLEVELID);

				logger.debug(" Get Store Visit Count.....");
				// get the store level visit count
				HashMap<String, Integer> storeVisitCount = objMovement
						.VisitorSummary(_conn, objStoreDto.getstoreNumber(),
								objCalendarDto.getCalendarId(), Constants.LOCATIONLEVELID);

				summaryBusiness.calculateProductSummary(movementDataList,
						productMap, objStoreDto, _uomMapDB, _uomMapPro,
						subCateVisitMap, CateVisitMap, departmentVisitMap,
						storeVisitCount, null, objSpecialDto,
						dealCostMap);

				// get Last year summaryDaily List
				lastSummaryList = objSalesDao.getLastYearSalesAggrList(_conn,
						retailDTO.getlstCalendarId(),
						objStoreDto.getLocationId(), Constants.STORE_LEVEL_ID,
						"SALES_AGGR_DAILY", "SUMMARY_DAILY_ID");
				
				
				String store_Status = objSalesDao.getStoreStatus(_conn,
						objCalendarDto.getStartDate(), objStoreDto,
						Constants.CALENDAR_WEEK);

				// Call the InsertSummaryDailyBatch
				logger.info("Update Sales Aggregation data...");
				objSADaily.insertSummaryDailyBatch(_conn,
						retailDTO.getCalendarId(), objStoreDto.getLocationId(),
						productMap, lastSummaryList,store_Status);

				logger.info(" Process Flag Update Process begins..");
				// Updating ProcessFlag as 'Y' in movemen_Daily
				// process added for repair Process
				// used to change non-process records to processd records
				objMovement.updateProcessFlag(_conn, retailDTO.getCalendarId(),
						objStoreDto.getstoreNumber());

				PristineDBUtil.commitTransaction(_conn,	"Commit Daily Aggregation Process");
				
				 
				
			}
		} catch (Exception ex) {
			logger.error(" Daily Aggregation Error....", ex);
			throw new GeneralException(" Daily Aggregation Error....", ex);

		}
	}

/*	
	 * ****************************************************************
	 * Function to get the aggregation data for Financial Department,
	 * Merchantise Department, Portfolio and Segment Argument 1: ProductGroupDTO
	 * List (group and its child info data) Argument 2: groupChildTypeId
	 * Argument 3: Subcategory Map Argument 4: Category Map Argument 5:
	 * Department Map Argument 6: summaryBusiness Object
	 * ****************************************************************
	 

	private void productGroupAggr(ArrayList<ProductGroupDTO> proGroupData,
			HashMap<Integer, HashMap<String, SummaryDataDTO>> productMap,
			Salesaggregationbusiness summaryBusiness, MovementDAO objMovement,
			String storeNumber, int calendar_id) throws GeneralException {

		try {
			for (int pM = 0; pM < proGroupData.size(); pM++) {
				ProductGroupDTO prodGroupobj = proGroupData.get(pM);

				// Find child availability
				if (productMap.containsKey(prodGroupobj.getChildLevelId())) {

					HashMap<String, Integer> productVisitCount = null;

					// check the get visit

					if (prodGroupobj.getChildLevelId() == Constants.SUBCATEGORYLEVELID
							|| prodGroupobj.getChildLevelId() == Constants.CATEGORYLEVELID
							|| prodGroupobj.getChildLevelId() == Constants.DEPARTMENTLEVELID
							|| prodGroupobj.getChildLevelId() == Constants.POSDEPARTMENT) {

						productVisitCount = objMovement.VisitorSummary(_conn,
								storeNumber, calendar_id,
								prodGroupobj.getProductLevelId());
					}

					// If available
					summaryBusiness.calculateGroupMetrics(productMap,
							prodGroupobj, productVisitCount);

					proGroupData.remove(prodGroupobj);
				}
			}
		} catch (Exception exe) {
			logger.error(" Daily Aggregation Error....", exe);
			throw new GeneralException(" Daily Aggregation Error....", exe);
		}
	}

	
	 * ****************************************************************
	 * Function to get the aggregation data for Financial Department,
	 * Merchantise Department, Portfolio and Segment Argument 1: ProductGroupDTO
	 * List (group and its child info data) Argument 2: groupChildTypeId
	 * Argument 3: Subcategory Map Argument 4: Category Map Argument 5:
	 * Department Map Argument 6: summaryBusiness Object
	 * ****************************************************************
	 
	
	*//**  Comented for Rite Aid                   *//*

	private void productGroupAggregation(
			HashMap<Integer, HashMap<String, SummaryDataDTO>> productMap,
			ArrayList<ProductGroupDTO> proGroupData,
			Salesaggregationbusiness summaryBusiness, MovementDAO objMovement,
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

	
	 * ****************************************************************
	 * Method to get the Group Data information
	 * ****************************************************************
	 
	public void getGroupData() throws GeneralException {
		// Object for SummaryDailyDAO - Change to ProductGroupDAO
		try {
			SalesAggregationProductGroupDAO objProdGroupDAO = new SalesAggregationProductGroupDAO();

			// change the method on 17-8-2012 for add the finance to its levels
			_productGroup = objProdGroupDAO.getProductGroupDetails(_conn);
		} catch (Exception exe) {
			logger.error(" Daily Aggregation Error....", exe);
			throw new GeneralException(" Daily Aggregation Error....", exe);
		}
	}

	/*
	 * Method used to get the uom data from UOM_LOOKUP table
	 
*/
	private void getUomData() throws GeneralException {

		// Object for ItemDao
		ItemDAO objItemDao = new ItemDAO();
		try {
			_uomMapDB = objItemDao.getUOMList(_conn, "SALES_AGGR_DAILY");
		} catch (GeneralException gex) {
			logger.error("Error In getUomData .... ", gex);
			throw gex;
		}

	}

	/*
	 * ****************************************************************
	 * Load the SpecialCriteria and uom for movement by volume
	 * 
	 * Argument 1 : specialDTO Return specialDTO
	 * ****************************************************************
	 */

	public SpecialCriteriaDTO loadSpecialCriteria() {

		// code hided on 21-08-2012
		// exclude department and include item no need for further
		// we can use pos_department < = 37

		/*
		 * String excludeDepts =
		 * PropertyManager.getProperty("PI_REVENUE_EXCLUDE_DEPARTMENTS", null);
		 * String includeItems =
		 * PropertyManager.getProperty("PI_REVENUE_INCLUDE_ITEMS", null); String
		 * gasUpc = PropertyManager.getProperty("SA_GAS_UPC", null);
		 */

		// added for getting uom details for Movement by volume
		String uomName = PropertyManager.getProperty("MOVEMENT_VOLUME_UOM",
				null);

		String couponPosId = PropertyManager.getProperty(
				"COUPON_POS_DEPARTMENT", null);

		// Getting the values for identify the gas value
		String storeGasDetails = PropertyManager.getProperty(
				"STATE_BASED_GAS_VALUE", null);

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

		return specialDTO;
	}

	/*
	 * public void loadCustomerProperties(SpecialCriteriaDTO splCriteriaDTO)
	 * throws GeneralException { // Load customer properties try{ String
	 * excludeDepts = PropertyManager.getProperty(
	 * "PI_REVENUE_EXCLUDE_DEPARTMENTS", null); String includeItems =
	 * PropertyManager.getProperty( "PI_REVENUE_INCLUDE_ITEMS", null); String
	 * gasUPCDetails = PropertyManager.getProperty("SA_GAS_UPC", null);
	 * 
	 * logger.info("Excluding departments................." + excludeDepts);
	 * logger.info("Including items......................." + includeItems);
	 * logger.info("GAS UPC..............................." + gasUPCDetails);
	 * 
	 * if (excludeDepts != null) { String[] depts = excludeDepts.split(","); for
	 * (int ii = 0; ii < depts.length; ii++) { _ExcludeDeptMap.put(depts[ii],
	 * depts[ii]); } }
	 * 
	 * if (includeItems != null) { String[] items = includeItems.split(","); for
	 * (int ii = 0; ii < items.length; ii++) { _IncludeItemMap.put(items[ii],
	 * items[ii]); } }
	 * 
	 * if (gasUPCDetails != null) { String[] gasItem = gasUPCDetails.split(",");
	 * for (int ii = 0; ii < gasItem.length; ii++) {
	 * _gasUPCMap.put(gasItem[ii].trim(), gasItem[ii].trim()); } }
	 * 
	 * splCriteriaDTO.setExcludeDepartmentMap(_ExcludeDeptMap);
	 * splCriteriaDTO.setIncludeItemMap(_IncludeItemMap);
	 * splCriteriaDTO.setGasDetailsMap(_gasUPCMap); }catch(Exception exe){
	 * logger.error(" Daily Aggregation Error...." , exe); throw new
	 * GeneralException(" Daily Aggregation Error...." , exe); }
	 * 
	 * }
	 */

	/**
	 ***************************************************************** 
	 * Static method to log the command line arguments
	 ***************************************************************** 
	 */
	private static void logCommand(String[] args) {
		logger.info("*****************************************");
		StringBuffer sb = new StringBuffer("Command: SummaryDaily ");
		for (int ii = 0; ii < args.length; ii++) {
			if (ii > 0)
				sb.append(' ');
			sb.append(args[ii]);
		}

		logger.info(sb.toString());
	}

}