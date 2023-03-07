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
import com.pristine.dao.salesanalysis.SalesAggregationDailyDaoV3;
import com.pristine.dao.salesanalysis.SalesAggregationDailyTLogDao;
import com.pristine.dao.salesanalysis.SalesAggregationProductGroupDAO;
import com.pristine.dto.MovementDailyAggregateDTO;
import com.pristine.dto.RetailCalendarDTO;
import com.pristine.dto.salesanalysis.ProductGroupDTO;
import com.pristine.dto.salesanalysis.SpecialCriteriaDTO;
import com.pristine.dto.salesanalysis.SummaryDataDTO;
import com.pristine.exception.GeneralException;
import com.pristine.util.Constants;
import com.pristine.util.DateUtil;
import com.pristine.util.PristineDBUtil;
import com.pristine.util.PropertyManager;

public class SummaryDailyV3 {

	static Logger logger = Logger.getLogger("SummaryDailyV3");
	private String _storeNum = null; // Processing store number
	private String _districtNum = null; // Processing district number
	private Connection _conn = null; // DB connection
	private Date _startdate = null; // Processing StartDate
	private Date _endDate = null; // Processing EndDate
	
	ArrayList<Integer> _leastProductLevel = new ArrayList<Integer>();
	HashMap<Integer, Integer> _gasItems = new HashMap<Integer, Integer>(); 
	HashMap<Integer, ArrayList<ProductGroupDTO> > _prodHierachy = new HashMap<Integer, ArrayList<ProductGroupDTO> >(); 
	int _couponAdjustmentRequired = 0;
	String _revenuePercCalc = null;
	
	/*
	 * ****************************************************************
	 * Class constructor
	 * 
	 * @throws Exception,GeneralException
	 * ****************************************************************
	 */
	public SummaryDailyV3(Date startDate, Date endDate, String storeno,
			String district) {

		if (storeno == "" && district == "") {
			logger.error(" Store Numebr / District Id is missing in Input");
			System.exit(1);
		}

		if (district != "") {
			storeno = "";
		}

		// If there is no date input then set the default process date
		/*if (startDate == null) {
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
		}*/

		_storeNum = (storeno.equals("")) ? "0" : storeno;
		_districtNum = (district.equals("")) ? "0" : district;
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

	public static void main(String[] args)  {
		PropertyConfigurator.configure("log4j-summary-daily.properties");
		logger.info("*** Summary Daily Process Begins ***");
		//logCommand(args);
		Date startDate = null;
		Date endDate = null;
		String storeno = "";
		String district = "";

		// Variable added for repair process
		String repairFlag = "";
		DateFormat dateFormat = new SimpleDateFormat("MM/dd/yyyy");
		SummaryDailyV3 summaryDaily = null;
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

			}

			// Create object for summary daily
			summaryDaily = new SummaryDailyV3(startDate, (endDate == null) ? startDate : endDate, storeno,
					district);

			// Call summary calculation process
			summaryDaily.processSummaryData();

		} catch (GeneralException e) {
			logger.error(e.getMessage());
		} catch (SQLException e) {
			// TODO Auto-generated catch block
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
	private void processSummaryData()
			throws GeneralException, SQLException {

		// Object for RetailCalendarDAO
		RetailCalendarDAO objCalendarDao = new RetailCalendarDAO();
		
		SalesAggregationProductGroupDAO objProdGroupDAO = new SalesAggregationProductGroupDAO();
		_leastProductLevel = objProdGroupDAO.getLeastProductParent(_conn);
		
		//Get all parents for the least product level and form the child hierarchy for each parent
		for(int leastLevel : _leastProductLevel){
			String parents = objProdGroupDAO.getParents(_conn, leastLevel);
			for(String parent : parents.split(",")){
				if(!_prodHierachy.containsKey(new Integer( Integer.parseInt(parent) )))
					_prodHierachy.put(new Integer( Integer.parseInt(parent) ), objProdGroupDAO.getChildHierarchy(_conn, Integer.parseInt(parent)) );
			}
		}
		
		//Get Exclude category information
		int categoryId = Integer.parseInt( PropertyManager.getProperty("MOVEMENT_EXCLUDE_CATEGORY","0") );
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
		
		// Object for SalesAggregationDailyDao
		SalesAggregationDailyDaoV3 objSADaily = new SalesAggregationDailyDaoV3();
		//SalesAggregationDailyTLogDao objSADaily = new SalesAggregationDailyTLogDao();
			
			// Object for SpecialCriteriaDTO
		SpecialCriteriaDTO objSpecialDto = loadSpecialCriteria();
		
		try {

			List<RetailCalendarDTO> calendarList = null;

			//logger.debug("Get Calendar Id for given date range");
			//calendarList = objCalendarDao.dayCalendarList(_conn, _startdate, _endDate, Constants.CALENDAR_DAY);
			
			// get the calendar list
			if( _startdate != null ){
				calendarList = new ArrayList<RetailCalendarDTO>();
				calendarList.add( objCalendarDao.getCalendarId(_conn, DateUtil.dateToString(_startdate, Constants.APP_DATE_FORMAT) , Constants.CALENDAR_DAY) );
			} else {
				int lastCalendarId = objSADaily.getLastUpdatedCalendarId(_conn, _storeNum, _districtNum, Constants.CALENDAR_DAY);
				
				logger.debug("Last updated Calendar Id:" + lastCalendarId);
				calendarList = objCalendarDao.getCalendarIdsForDataLoading(_conn,lastCalendarId,Constants.CALENDAR_DAY);
			}

			logger.debug("Total Processing Calendar Id count:"
					+ calendarList.size());

			// Loop for each calendar Id
			for (int dd = 0; dd < calendarList.size(); dd++) {

				RetailCalendarDTO calendarDTO = calendarList.get(dd);
				RetailCalendarDTO weekCalendarDTO =  objCalendarDao.getCalendarIdForWeek(_conn, calendarDTO.getCalendarId());

				logger.info("Process begins at Calendar level. Calendar Id:"
						+ calendarDTO.getCalendarId());

				// Call Aggregation process for each calendar Id
				ComputeSummaryAggregationForDay(calendarDTO, weekCalendarDTO, objSADaily, objSpecialDto, categoryId);

				logger.debug("Process end for Calendar Id:"	+ calendarDTO.getCalendarId());
			}
		} 
		catch (Exception exe) {
			throw new GeneralException("processSummaryData", exe);
		}
	}

	/*
	 * *********************	Process Flow	************************
	 * Delete Previous Aggregation for the current date
	 * 			|
	 * 			v
	 * Load the movement daily data to a temporary table with least product level data
	 * 			|
	 * 			v 
	 * Retail Deal cost setup for the current date
	 * 			|
	 * 			v  
	 * Separate the temporary movement daily data to Regular and Sale data 
	 * and perform ActualWeight,SaleQuantity, RegularQuantity calculations
	 * 			|
	 * 			v
	 * Perform Movement volume update to temporary movement daily data 
	 * and perform Movement, Revenue and Margin calculations to temporary movement daily data
	 * 			|
	 * 			v 
	 * Update Store parameters (store type, etc..)
	 * 			|
	 * 			v 
	 * Aggregate the data based on parent product levels( based on least product id) and store level
	 * and insert to Sales Aggr Daily for one specific calendar id.
	 * 
	 * ****************************************************************
	 * Call the aggregation process for a Store Number or district number and calendar Id 
	 * Argument 1: RetailCalendarDTO (Calendar Id and Process Date) 
	 * Argument 2: SalesAggregationDailyDao 
	 * Argument 3: SpecialCriteria
	 * @throws GeneralException
	 * ****************************************************************
	 */
	public void ComputeSummaryAggregationForDay(RetailCalendarDTO calendarDTO,RetailCalendarDTO weekCalendarDTO,
			SalesAggregationDailyDaoV3 objSADaily,//SalesAggregationDailyTLogDao objSADaily,
			SpecialCriteriaDTO objSpecialDto, int categoryId) throws GeneralException {

		try {
			
			// delete the previous aggregation for the store and calendar id
			logger.debug("Delete previous Aggregation Data...");
			objSADaily.deletePreviousAggregation(_conn, calendarDTO.getCalendarId(), _storeNum, _districtNum) ;

			if ( _revenuePercCalc.equalsIgnoreCase("YES")){
				objSADaily.deletePreviousNoCostItemsInfo(_conn,	calendarDTO.getCalendarId(), _storeNum, _districtNum);
			}
			
			objSADaily.insertMovementDailyTemp(_conn,calendarDTO.getCalendarId(),_leastProductLevel,_storeNum,_districtNum, categoryId);
			
			objSADaily.retailDealCostUpdateToTemp(_conn, calendarDTO.getCalendarId(),weekCalendarDTO.getCalendarId());
			
			objSADaily.segregateSaleAndRegularFromTemp(_conn);
			
			if(!_gasItems.isEmpty())
				objSADaily.POSUpdateToTemp(_conn, _gasItems, objSpecialDto.getstoreBasedGasValue(), _couponAdjustmentRequired );
			
			objSADaily.movementVolumeUpdateToTemp(_conn);
			
			objSADaily.storeTypeUpdateToTemp(_conn,calendarDTO.getCalendarId(),_storeNum,_districtNum);
			
			for(ArrayList<ProductGroupDTO> arr : _prodHierachy.values()){
				objSADaily.moveTempToSalesAggregation(_conn, arr, calendarDTO.getCalendarId(), _storeNum, _districtNum,categoryId);
			}
			
			//Adjust coupoun revenue is required only for Store Summary and POS summary
			//-------------------------------------------------------------------------
			if(_couponAdjustmentRequired > 0 )
				objSADaily.couponAdjustment(_conn, calendarDTO.getCalendarId(), _storeNum, _districtNum);
			
			objSADaily.moveTempToStoreSalesAggregation(_conn, calendarDTO.getCalendarId(), _storeNum, _districtNum,_gasItems,categoryId);

			objSADaily.moveTempToPOSSalesAggregation(_conn, calendarDTO.getCalendarId(), _storeNum, _districtNum, _gasItems,categoryId);
			
			objSADaily.UpdateLastYearSalesAggrList(_conn, calendarDTO.getCalendarId(), calendarDTO.getlstCalendarId());
			
			objSADaily.markProcessCompletionForMovementDaily(_conn);
			
			if ( _revenuePercCalc.equalsIgnoreCase("YES")){
				objSADaily.addNoCostItemsInfoFromTemp(_conn);
			}
			
			logger.debug("Process ends");

		} catch (Exception exe) {
			logger.error(" Daily Aggregation Error....", exe);
			throw new GeneralException("ComputeSummaryAggregationForDay", exe);
		}
	}
	
	
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
		_couponAdjustmentRequired = Integer.parseInt(PropertyManager.getProperty( "SALES_ANALYSIS.COUPON_ADJUSTMENT", "0"));

		_revenuePercCalc = PropertyManager.getProperty("MARGIN_CALCULATION_REQUIRED",null);

		
		
		if (_couponAdjustmentRequired == 1)
			logger.debug("Coupon adjustment is required");
		else
			logger.debug("Coupon adjustment is not required");

			return specialDTO;
	}
}