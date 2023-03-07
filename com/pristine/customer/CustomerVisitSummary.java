/*
 * Title: TOPS Summary Daily Movement
 *
 *******************************************************************************
 * Modification History
 *------------------------------------------------------------------------------
 * Version		Date		Name			Remarks
 *------------------------------------------------------------------------------
 * Version 0.1	15/12/2012	Raj Kumar		Initial version 
 *******************************************************************************
 */
package com.pristine.customer;

import java.sql.Connection;
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
import com.pristine.business.entity.Salesaggregationbusiness;
import com.pristine.dao.CompStoreDAO;
import com.pristine.dao.DBManager;
import com.pristine.dao.ItemDAO;
import com.pristine.dao.RetailCalendarDAO;
import com.pristine.dao.TransactionLogDAO;
import com.pristine.dao.customer.CustomerDAO;
import com.pristine.dao.customer.CustomerVisitSummaryDAO;
import com.pristine.dataload.tops.CostDataLoad;
import com.pristine.dto.MovementDailyAggregateDTO;
import com.pristine.dto.RetailCalendarDTO;
import com.pristine.dto.salesanalysis.SpecialCriteriaDTO;
import com.pristine.dto.salesanalysis.SummaryDataDTO;
import com.pristine.exception.GeneralException;
import com.pristine.util.Constants;
import com.pristine.util.PristineDBUtil;
import com.pristine.util.PropertyManager;

public class CustomerVisitSummary {

	static Logger logger = Logger.getLogger("SummaryVisitSummary");
	private String _storeNum = null; // Processing store number
	private String _districtNum = null; // Processing district number
	private Connection _conn = null; // DB connection
	private Date _startdate = null; // Processing StartDate
	private Date _endDate = null; // Processing EndDate
	private HashMap<String, String> _uomMapDB = new HashMap<String, String>();
	private HashMap<String, String> _uomMapPro = new HashMap<String, String>();
	private HashMap<String, Integer> _custMap;
	int _maxPOS = 0;

	/*
	 * ****************************************************************
	 * Class constructor
	 * 
	 * @throws Exception,GeneralException
	 * ****************************************************************
	 */
	public CustomerVisitSummary(Date startDate, Date endDate, String storeno,
														String district) {

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
			logger.info("Process Start Date:"	+ startDate.toString());
			logger.info("Process End  Date:" + endDate.toString());

		} else {
			logger.info("Process Date:" + startDate.toString());
			endDate = startDate;
		}

		if (storeno != "")
			logger.info("Process Store:" + storeno);

		if (district != "")
			logger.info("Process District:" + district);

		_storeNum = storeno;
		_districtNum = district;
		_startdate = startDate;
		_endDate = endDate;

		_custMap = new HashMap<String, Integer>(); 
		
		PropertyManager.initialize("analysis.properties");
		

		_maxPOS = Integer.parseInt(PropertyManager.
				getProperty("SALES_ANALYSIS.MAX_REV_POS_DEPARTMENT", "0"));
		
		try {
			_conn = DBManager.getConnection();

		} catch (GeneralException exe) {
			logger.error(" Connection Failed...", exe);
			System.exit(1);
		}
	}

	/*
	 * ****************************************************************
	 * Main method of Batch. Get the inputs from command line argument 
	 * Argument 1: Store Number 
	 * Argument 2: District Number 
	 * Argument 3: Start Date 
	 * Argument 4: End Date 
	 * If the Date is not specified then process for yesterday 
	 * District or Store Number is mandatory. If both are are
	 * specified then consider district alone
	 * ****************************************************************
	 */

	public static void main(String[] args) {
		PropertyConfigurator.configure("log4j-customer-visit-summary.properties");
		logger.info("*** Customer Visit Summary begins ***");
		logCommand(args);
		Date startDate = null;
		Date endDate = null;
		String storeno = "";
		String district = "";

		// Variable added for repair process
		DateFormat dateFormat = new SimpleDateFormat("MM/dd/yyyy");
		CustomerVisitSummary summaryDaily = null;
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

			}

			// Create object for summary daily
			summaryDaily = new CustomerVisitSummary(startDate, endDate, storeno,
																district);

			// Call summary calculation process
			summaryDaily.processVisitData(startDate, endDate);

		} catch (GeneralException e) {
			logger.error(e);
		} finally {
			logger.info("*** Customer Visit Summary ends ***");
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
	private void processVisitData(Date startDate, Date endDate)
			throws GeneralException {

		//Object for RetailCalendarDAO
		RetailCalendarDAO objCalendarDao = new RetailCalendarDAO();

		//Object for CompStoreDAO
		CompStoreDAO objStoreDao = new CompStoreDAO();

		//Object for SalesAggregationDailyDao
		CustomerVisitSummaryDAO objCVS = new CustomerVisitSummaryDAO();
			
		//Object for SpecialCriteriaDTO
		SpecialCriteriaDTO objSpecialDto = new SpecialCriteriaDTO();

		try {

			List<RetailCalendarDTO> calendarList = null;

			calendarList = objCalendarDao.dayCalendarList(_conn,
						_startdate, _endDate, Constants.CALENDAR_DAY);

			//Get the unit of measure (UOM) and its details
			getUomData();

			//Load the SpecialCriteria
			objSpecialDto = loadSpecialCriteria();

			// get Location-id list for a give District / Store
			logger.debug("Get store data");
			List<SummaryDataDTO> storeList = objStoreDao.getStoreNumebrs(_conn,
					_districtNum, _storeNum);

			logger.info("Total Processing Store count:" + storeList.size());

			//Iterate the storeList in loop
			for (int sL = 0; sL < storeList.size(); sL++) {

				SummaryDataDTO objStoreDto = storeList.get(sL);

				logger.info("Process begin at Store level. Store No:" + 
												objStoreDto.getstoreNumber());

					logger.info(" Total Processing Calendar Id count:" 
														+ calendarList.size());

					// Loop for each calendar Id
					for (int dd = 0; dd < calendarList.size(); dd++) {

						RetailCalendarDTO calendarDTO = calendarList.get(dd);

						logger.info("Process begins at Calendar level. Calendar Id:"
											+ calendarDTO.getCalendarId());

						// Call Aggregation process for each calendar Id
						ComputeVisitSummary(calendarDTO, objStoreDto, 
													objSpecialDto, objCVS);

						logger.debug("Process ends for Calendar Id:"
								+ calendarDTO.getCalendarId());
					}
			}

		} catch (Exception exe) {
			logger.error(" Customer Visit Summary Error....", exe);
			throw new GeneralException("processVisitData", exe);
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
	public void ComputeVisitSummary(RetailCalendarDTO calendarDTO,
			 SummaryDataDTO objStoreDto, SpecialCriteriaDTO objSpecialDto, 
			 CustomerVisitSummaryDAO objCVS) throws ParseException,
			 											GeneralException {
		try {

			int storeId = objStoreDto.getLocationId();
			String storeNumber = objStoreDto.getstoreNumber();
			int calendarId = calendarDTO.getCalendarId();

			//Delete the previous aggregation for the store and calendar id
			logger.debug("Delete previous Visit summary Data");
			objCVS.deleteCustomerVisitSummary(_conn, storeId, calendarId);

			//Object for Transaction Log DAO
			TransactionLogDAO objTLog = new TransactionLogDAO(); 
			
			//Object for CostDataLoad
			CostDataLoad objCostLoad = new CostDataLoad();
			
			String categoryIdStr = PropertyManager.
									getProperty("MOVEMENT_EXCLUDE_CATEGORY");
			
			logger.debug("Get Transaction Log data");
			List<MovementDailyAggregateDTO> tlogDataList = objTLog
							.getTLogByTransaction(_conn, storeId, 
										calendarId, categoryIdStr, _maxPOS);
			
			if (tlogDataList.size() > 0) {

				//Get distinct item code list from movement_daily to get deal cost
				List<String> itemCodeList = objTLog.getDistinctItemCode(
									_conn, storeId, calendarId, _maxPOS);

				//Get the deal cost info from cost loader for margin calculation
				HashMap<String, Double> dealCostMap = objCostLoad.getRetailCost(
								_conn, storeNumber,	itemCodeList, calendarId);
				
				List<SummaryDataDTO> summaryDataList = aggregateVisitSummary
							(tlogDataList, dealCostMap, storeId, calendarId);
				
				if (summaryDataList.size() > 0)
				{
					logger.debug("Total Transactions count:" + summaryDataList.size());
					objCVS.insertCustomerVisitSummary(_conn, summaryDataList);
				}
			}
			
			logger.debug("Process ends for Store:"+ objStoreDto.getstoreNumber());

		} catch (Exception exe) {
			logger.error(" Daily Aggregation Error....", exe);
			throw new GeneralException("ComputeSummaryAggregationForDay", exe);
		}
	}
	

	private List<SummaryDataDTO> aggregateVisitSummary(
		List<MovementDailyAggregateDTO> tlogDList, 
		HashMap<String, Double> dealCostDataMap, int storeId, int calendarId) 
													throws GeneralException
	{
		int customerId =0;
		int transactionNo = 0;
		double totRegularMovement = 0;
		double totSaleMovement = 0;
		double totRegularRevenue = 0;
		double totSaleRevenue = 0;
		double storeSaleDealCost = 0;
		double storeRegularDealCost =0;
		double totRegVolumeMovement = 0;
		double totSaleVolumeMovement = 0;
		
		Salesaggregationbusiness summaryBusiness = new Salesaggregationbusiness();
		
		List<SummaryDataDTO> summaryDataList = new ArrayList<SummaryDataDTO>();
		
		for (int ii = 0; ii < tlogDList.size(); ii++) {
			
			MovementDailyAggregateDTO objMovementInDto = tlogDList.get(ii);
			
			if(transactionNo > 0 && transactionNo != objMovementInDto.getTransactionNo()){
				
				/*logger.debug("================================================");
				logger.debug("transactionNo         " + transactionNo);						
				logger.debug("totRegularMovement    " + totRegularMovement);
				logger.debug("totSaleMovement       " + totSaleMovement);
				logger.debug("totRegularRevenue     " + totRegularRevenue);
				logger.debug("totSaleRevenue        " + totSaleRevenue);
				logger.debug("totRegVolumeMovement  " + totRegVolumeMovement);
				logger.debug("totSaleVolumeMovement " + totSaleVolumeMovement);
				logger.debug("storeSaleDealCost     " + storeSaleDealCost);
				logger.debug("storeRegularDealCost  " + storeRegularDealCost);
				logger.debug("storeId               " + storeId);
				logger.debug("calendarId            " + calendarId);
				logger.debug("customerId            " + customerId);*/
				
				SummaryDataDTO objSummaryDto = new SummaryDataDTO();

				objSummaryDto.setRegularMovement(totRegularMovement);
				objSummaryDto.setSaleMovement(totSaleMovement);
				objSummaryDto.setRegularRevenue(totRegularRevenue);
				objSummaryDto.setSaleRevenue(totSaleRevenue);
				objSummaryDto.setregMovementVolume(totRegVolumeMovement);
				objSummaryDto.setsaleMovementVolume(totSaleVolumeMovement);
				objSummaryDto.setSaleDealCost(storeSaleDealCost);
				objSummaryDto.setRegularDealCost(storeRegularDealCost);

				summaryBusiness.CalculateMetrixInformation(objSummaryDto, true);
				objSummaryDto.setLocationId(storeId);
				objSummaryDto.setcalendarId(calendarId);
				objSummaryDto.setTransactionNo(transactionNo);
				objSummaryDto.setCustomerId(customerId);
				
				summaryDataList.add(objSummaryDto);
				
				totRegularMovement = 0;
				totSaleMovement = 0;
				totRegularRevenue = 0;
				totSaleRevenue = 0;
				storeSaleDealCost = 0;
				storeRegularDealCost =0;
				totRegVolumeMovement = 0;
				totSaleVolumeMovement = 0;
			}						
				
				// Aggregation process for TotalRevenue
				totRegularRevenue += objMovementInDto.getRevenueRegular();
				totSaleRevenue += objMovementInDto.getRevenueSale();
				
				// Aggregation process for movement
				totRegularMovement += objMovementInDto.get_regularQuantity();
				totSaleMovement += objMovementInDto.get_saleQuantity();
				
				// Aggregation process for movement by volume
				summaryBusiness.processMovementByVolume(_uomMapDB, _uomMapPro, objMovementInDto);
				totRegVolumeMovement += objMovementInDto.getregMovementVolume();
				totSaleVolumeMovement += objMovementInDto.getsaleMovementVolume();

				// Aggregation process deal cost
				double processDealCost = summaryBusiness.getDealCost(dealCostDataMap, objMovementInDto);

				if (objMovementInDto.getFlag().equalsIgnoreCase("Y"))
					storeSaleDealCost += processDealCost;
				else
					storeRegularDealCost += processDealCost;
			
				customerId = Integer.parseInt(objMovementInDto.getCustomerId());
				transactionNo = objMovementInDto.getTransactionNo();
		}
		
		//Processing last transaction
		SummaryDataDTO objSummaryDto = new SummaryDataDTO();

		objSummaryDto.setRegularMovement(totRegularMovement);
		objSummaryDto.setSaleMovement(totSaleMovement);
		objSummaryDto.setRegularRevenue(totRegularRevenue);
		objSummaryDto.setSaleRevenue(totSaleRevenue);
		objSummaryDto.setregMovementVolume(totRegVolumeMovement);
		objSummaryDto.setsaleMovementVolume(totSaleVolumeMovement);
		objSummaryDto.setSaleDealCost(storeSaleDealCost);
		objSummaryDto.setRegularDealCost(storeRegularDealCost);
		objSummaryDto.setCustomerId(customerId);

		summaryBusiness.CalculateMetrixInformation(objSummaryDto, true);
		objSummaryDto.setLocationId(storeId);
		objSummaryDto.setcalendarId(calendarId);
		objSummaryDto.setTransactionNo(transactionNo);
		objSummaryDto.setCustomerId(customerId);				
		summaryDataList.add(objSummaryDto);

		logger.debug("Total Transactions count:" + summaryDataList.size());
		
		return summaryDataList;
		
	}
	
	
	
	/*
	 * Method used to get the uom data from UOM_LOOKUP table
	 */

	public void getUomData() throws GeneralException {

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

	public int getCustomerId(Connection conn, String customerCardNo, 
						int storeId) throws GeneralException, Exception{

		int customerId;

		if(_custMap.containsKey(customerCardNo)) {
			customerId =  _custMap.get(customerCardNo);
		}
		else {
			CustomerDAO custdao = new CustomerDAO();	
			customerId = custdao.getCustomerId(conn, customerCardNo, storeId);
			_custMap.put(customerCardNo, customerId);
		}
	
		return customerId;
	}
	
	
	/**
	 ***************************************************************** 
	 * Static method to log the command line arguments
	 ***************************************************************** 
	 */
	private static void logCommand(String[] args) {
		StringBuffer sb = new StringBuffer("Command: Customer Visit Summary ");
		for (int ii = 0; ii < args.length; ii++) {
			if (ii > 0)
				sb.append(' ');
			sb.append(args[ii]);
		}

		logger.info(sb.toString());
	}

}