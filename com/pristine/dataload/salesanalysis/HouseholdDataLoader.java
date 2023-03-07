/*
 * Title: Household data loader for Sales Analysis
 *
 *******************************************************************************
 * Modification History
 *------------------------------------------------------------------------------
 * Version		Date		Name			Remarks
 *------------------------------------------------------------------------------
 * Version 0.1	08/10/2016	John Britto		Initial Version 
 *******************************************************************************
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
import com.pristine.dao.CompStoreDAO;
import com.pristine.dao.DBManager;
import com.pristine.dao.MovementDAO;
import com.pristine.dao.salesanalysis.SalesAggregationDao;
import com.pristine.dao.salesanalysis.SalesAggregationProductGroupDAO;
import com.pristine.dao.salesanalysis.SalesAggregationStatusDao;
import com.pristine.dto.RetailCalendarDTO;
import com.pristine.dto.salesanalysis.HouseholdDTO;
import com.pristine.dto.salesanalysis.ProductGroupDTO;
import com.pristine.dto.salesanalysis.SummaryDataDTO;
import com.pristine.exception.GeneralException;
import com.pristine.util.Constants;
import com.pristine.util.PristineDBUtil;
import com.pristine.util.PropertyManager;

public class HouseholdDataLoader {

	static Logger logger = Logger.getLogger("HouseholdDataLoader");

	// global variables
	private Date _startDate	= null;		// hold the input start date
	private Date _endDate = null;       // hold the input end date
	private Connection _conn = null;    // hold the database connection
	String _storeNo = null;           	// hold the district id
	String _districtId = null;           	// hold the district id
	String _regionId = null;           	// hold the region id
	String _divisionId = null;           	// hold the division id
	String _chainId   = null;           	// hold the chain id
	String _processType = null;          // hold the repair mode flag
	
	HashMap<Integer, Integer> _productGroupType = new HashMap<Integer, Integer>();
	ArrayList<ProductGroupDTO> _productGroup = new ArrayList<ProductGroupDTO>();
	ArrayList<Integer> _householdReqProductLevelList = new ArrayList<Integer>();
	String _categoryIdStr = "";
	int _storeLevelRequired = 0;
	
	/*
	 * ***********************************************************************
	 * Class constructor
	 * Argument 1 : Start Date 
	 * Argument 2 : End Date 
	 * Argument 3 : Store Number
	 * Argument 4 : District ID
	 * Argument 5 : Region ID
	 * Argument 6 : Division ID
	 * Argument 7 : Chain ID
	 * Argument 8 : Process Type
	 * Argument 9 : Calendar Type
	 * Argument 10 : Processing Location level ID
	 * @catch GeneralException
	 * ***********************************************************************
	 */
	public HouseholdDataLoader(Date startDate, Date endDate, String storeNo, 
		String districtId, String regionId, String divisionId, String chainId, 
		String processType, String calendarType, int locationLevel) {
		PropertyManager.initialize("analysis.properties");

		if (calendarType == null){
			logger.error("Input error, Calendar Type required for Household calculation");
			logger.info("Household data loading ends unsucessfully!");
			System.exit(1);
		}
		
		if (locationLevel == 0){
			logger.error("Input error, Location level ID required for Household calculation");
			logger.info("Household data loading ends unsucessfully!");
			System.exit(1);
		}
		
		if (storeNo == null && districtId == null && regionId == null && 
									divisionId == null && chainId == null)
		{
			logger.error("Input error, Store/District/Region/Division/Chain required for Household calculation");
			logger.info("Household data loading ends unsucessfully!");
			System.exit(1);
		}
		
		if (startDate == null) {
			Calendar cal = Calendar.getInstance();
			cal.setTime(new Date());
			cal.add(Calendar.DATE, -1);
			startDate = cal.getTime();
		}

		if (endDate == null) {
			endDate = startDate;
		}

		if (locationLevel == Constants.STORE_LEVEL_ID){
			if (storeNo != null ) {		// Assign the store Id	
				_storeNo = storeNo;
				logger.info("Household data population for Store: " + storeNo);
			}
			else if (districtId != null ) {		// Assign the district Id
				_districtId = districtId;
				logger.info("Household data population for Store under District: " + districtId);
			}
		}
		else if (locationLevel == Constants.DISTRICT_LEVEL_ID){
			if (districtId != null ) {		// Assign the district Id
				_districtId = districtId;
				logger.info("Household data population for District: " + districtId);
			}
		}
		else if (locationLevel == Constants.REGION_LEVEL_ID){
			if (regionId != null ) {		// Assign the RegionId
				_regionId = regionId;
				logger.info("Household data population for Region:" + regionId);
			}
		}
		else if (locationLevel == Constants.DIVISION_LEVEL_ID){
			if (divisionId != null ) {		// Assign the DivisionId
				_divisionId = divisionId;
				logger.info("Household data population for Division:" + divisionId);
			}
		}
		else if (locationLevel == Constants.CHAIN_LEVEL_ID){
			if (chainId != null ) {		// Assign the Chain Id
				_chainId = chainId;
				logger.info("Household data population for Chain:" + chainId);
			}
		}
		
		if (processType.equalsIgnoreCase(Constants.PROCESS_DATA_FULL)){
			_processType = Constants.PROCESS_DATA_FULL;
			logger.info("Process mode:FULL, Process all the non-processed records");
		}
		else{
			_processType = Constants.PROCESS_DATA_NORMAL;
			logger.info("Process mode:NORMAL, Process data only for specific Calendar Ids");
		}

		 if (calendarType.equalsIgnoreCase(Constants.CALENDAR_DAY)){
			logger.info("Processing for Day level data");
		 }
		 else if (calendarType.equalsIgnoreCase(Constants.CALENDAR_WEEK)){
			logger.info("Processing for Week level data");
		 }		 
		 else if (calendarType.equalsIgnoreCase(Constants.CALENDAR_PERIOD)){
			logger.info("Processing for Period level data");
		 }
		 else if (calendarType.equalsIgnoreCase(Constants.CALENDAR_QUARTER)){
			logger.info("Processing for Quarter level data");
		 }
		 else if (calendarType.equalsIgnoreCase(Constants.CALENDAR_YEAR)){
			logger.info("Processing for Year level data");
		 }
		 else{
			 logger.error("Input error, Invalid calendar Type");
			 
			 logger.info("Household data loading ends unsucessfully!");
		 	System.exit(1);			 
		 }		 
	 
		// assign the inputs to global variables
		_startDate = startDate;
		_endDate  = endDate;

		// get the DB connection
		try {
			_conn = DBManager.getConnection();
			
		} catch (GeneralException gex) {
			logger.error("Error while connecting DB:" + gex);
			logger.info("Household data loading ends unsucessfully!");
			System.exit(1);
		}
	}

	/*
	 * ****************************************************************
	 * Main method of Batch. Get the inputs from command line argument 
	 * Argument 1: Start Date 
	 * Argument 2: End Date 
	 * If the StartDate and EndDate is not specified then process for past week
	 * ****************************************************************
	 */

	public static void main(String[] args) throws GeneralException {
		
		// Initialize the summary-weekly log4j
		PropertyConfigurator.configure("log4j-sales-aggr-household.properties");

		logger.info("Sales Analysis Household data popuation Process Starts");
		logCommand (args);
		Date startDate 			= null; // get the input start date
		Date endDate 			= null; // get the input end date
		String storeNo 			= null; // get the input store Id
		String districtId 		= null; // get the input district Id
		String regionId 		= null; // get the input region Id
		String divisionId 		= null; // get the input division Id
		String chainId 			= null; // get the input chain id
		String processType      = ""; // get the input repair flag value
		String calendarType     = ""; // get the input calendar type value
		int locationLevel = 0;
		DateFormat dateFormat 	= new SimpleDateFormat("MM/dd/yyyy"); // date format
		HouseholdDataLoader householdLoaderObj = null;
		try {
			for (int iP = 0; iP < args.length; iP++) {
				String arg = args[iP];

				// get the start date
				if (arg.startsWith("STARTDATE")) {
					String inputDate = arg.substring("STARTDATE=".length());
					try {
						startDate = dateFormat.parse(inputDate);
					} catch (ParseException pxe) {
						logger.error("The given start date is invalid");
						System.exit(1);
					}
				}
				// get the end date
				if (arg.startsWith("ENDDATE")) {
					String inputDate = arg.substring("ENDDATE=".length());
					try {
						endDate = dateFormat.parse(inputDate);
					} catch (ParseException pxe) {
						logger.error("The given end date is invalid");
						System.exit(1);
					}
				}

				if (arg.startsWith("LOCATIONLEVEL")) {	// get the districtId
					String locationLevelStr = arg.substring("LOCATIONLEVEL=".length());
					
					try{
						locationLevel = Integer.parseInt(locationLevelStr);
					}
					catch(Exception ex){
						logger.error("The given end date is invalid");
						System.exit(1);						
					}

				}				

				if (arg.startsWith("STORE")) {	// get the districtId
					storeNo = arg.substring("STORE=".length());
				}
				else if (arg.startsWith("DISTRICT")) {	// get the districtId
					districtId = arg.substring("DISTRICT=".length());
				}
				else if (arg.startsWith("REGION")) {	// get the Region Id
					regionId = arg.substring("REGION=".length());
				}
				else if (arg.startsWith("DIVISION")) {	// get the Division Id
					divisionId = arg.substring("DIVISION=".length());
				}
				else if (arg.startsWith("CHAIN")) {		// get the Chain Id
					chainId = arg.substring("CHAIN=".length());
				}
				
				// get the repair flag value
				if( arg.startsWith("PROCESSTYPE")){
					 processType = arg.substring("PROCESSTYPE=".length());
				}

				// get the calendar Type
				if( arg.startsWith("CALENDARTYPE")){
					calendarType = arg.substring("CALENDARTYPE=".length());
				}
			}

			// call the Constructor
			householdLoaderObj = new HouseholdDataLoader(startDate, endDate,
							storeNo, districtId, regionId, divisionId, chainId, 
									processType, calendarType, locationLevel);
			
			// Get configuration data
			householdLoaderObj.GetConfigData();
			
			// Get product group information 
			householdLoaderObj.getProductGroupData();
			
			if (locationLevel == Constants.STORE_LEVEL_ID)
				householdLoaderObj.processStoreHouseholdPopulation(calendarType, locationLevel);
			else
				householdLoaderObj.processRollupHouseholdPopulation(calendarType, locationLevel);
				
			logger.info("Household data population ends sucessfully!");

		} catch (Exception exe) {
			logger.error(" Error in rollup main class..." , exe);
			logger.info("Summary Roll up process ends unsucessfully");
			throw new GeneralException(" Error in rollup main class..." , exe);
		} finally {
			PristineDBUtil.close(householdLoaderObj._conn);
		}
	}
	
	private void processStoreHouseholdPopulation(String calendarType, 
			int locationLevel) throws GeneralException, SQLException {
		CompStoreDAO objStoreDao = new CompStoreDAO();
		
		// Get list of store based on selected district/store
		List<SummaryDataDTO> storeList = objStoreDao.getStoreNumebrs(_conn,
				_districtId, _storeNo);
		logger.info("Total Stores count:" + storeList.size());
		
		//Iterate for each store
		for (int sL = 0; sL < storeList.size(); sL++) {
			SummaryDataDTO objStoreDto = storeList.get(sL);
			logger.info("Processing for Store: " + objStoreDto.getstoreNumber());
			
			householdAggregation(calendarType, locationLevel, 
					objStoreDto.getstoreNumber(), objStoreDto.getLocationId());
		}
	}
	
	private void processRollupHouseholdPopulation(
							String calendarType, int locationLevel) 
		throws NumberFormatException, SQLException, GeneralException{

		if (locationLevel == Constants.DISTRICT_LEVEL_ID){
			logger.info("Processing for District: " + _districtId);
			householdAggregation(calendarType, locationLevel, "", Integer.parseInt(_districtId));
		}
		else if (locationLevel == Constants.REGION_LEVEL_ID){
			logger.info("Processing for Region: " + _regionId);
			householdAggregation(calendarType, locationLevel, "", Integer.parseInt(_regionId));
		}
		else if (locationLevel == Constants.DIVISION_LEVEL_ID){
			logger.info("Processing for Division: " + _divisionId);
			householdAggregation(calendarType, locationLevel, "", Integer.parseInt(_divisionId));
		}
		else if (locationLevel == Constants.CHAIN_LEVEL_ID){
			logger.info("Processing for Chain: " + _chainId);
			householdAggregation(calendarType, locationLevel, "", Integer.parseInt(_chainId));
		}
	}

	
	/*
	 * ***********************************************************************
	 * Get the Calendar Id list for the given date range. Call the aggregation
	 * for calendar list 
	 * Argument 1 : Start Date 
	 * Argument 2 : End Date 
	 * Argument 3 : Calendar RowType
	 * @catch GeneralException
	 * ***********************************************************************
	 */
	private void householdAggregation(String calendarType, int locationLevel, 
		String storeNumber, int locationId) throws SQLException, GeneralException {
		// Step 1: Get location details in case of store
		// Step 2: Get non processed calendar detail
		// Step 3: Get house hold count
		// Step 4: Update household count into SA tables
		
		// Object For SalesAggregationDao
		SalesAggregationDao objSalesDao = new SalesAggregationDao();

		// Object For SalesAggregationDao
		SalesAggregationStatusDao objSAStatusDao = new SalesAggregationStatusDao();
		List<RetailCalendarDTO> calendarList = new ArrayList<RetailCalendarDTO>();
		
		try {
			calendarList = objSAStatusDao.getHouseholdProcessingCalendarData(
								_conn, locationLevel, locationId, calendarType, 
										_processType, _startDate, _endDate);
			
			// Check the calendar list availability for further process
			if (calendarList.size() > 0) {
				logger.info("Number of Calendar to process: " + calendarList.size());

				// Process for each calendar ID
				for (int cL = 0; cL < calendarList.size(); cL++) {
					RetailCalendarDTO calendarDto = calendarList.get(cL);
					logger.info("Process begins at Calendar level. Calendar Id:"
												+ calendarDto.getCalendarId());

					HashMap<Integer, HashMap<Integer, HouseholdDTO>> householdCountMap =  getHouseholdCount(
							_conn, locationLevel , locationId, calendarType, calendarDto.getCalendarId(),  _categoryIdStr);
					
					if (householdCountMap != null && householdCountMap.size() > 0){
						// Update house hold data
						objSalesDao.updateHouseholdMetrics(_conn, calendarDto.getCalendarId(),
								calendarType, locationLevel, locationId, 
								householdCountMap);
						
						// Update SA status
						objSAStatusDao.updateSAStatusForHouseholdData(_conn, 
								calendarDto.getCalendarId(), locationLevel, 
												locationId, calendarType);
						
						// If everything goes fine, commit the transaction
						PristineDBUtil.commitTransaction(_conn,
								"Commit the update household Process");
					}
					else {
						logger.info("There is no new household data to process!");
					}
				}
			} else {
				logger.info("There is no new data to process for any calendar!");
			}

		} catch (Exception exe) {
			logger.error(exe);
			PristineDBUtil.rollbackTransaction(_conn, "RollBack the Process");
			throw new GeneralException("Daily Rollup Error", exe);
		}
	}

	
	private HashMap<Integer, HashMap<Integer, HouseholdDTO>> getHouseholdCount(Connection _conn, int locationLevelId, 
				int locationId, String calendarType, int calendarId,  String categoryIdStr) throws GeneralException{
		
		MovementDAO objMovement = new MovementDAO();
		
		HashMap<Integer, HashMap<Integer, HouseholdDTO>> householdCountMap = new HashMap<Integer, HashMap<Integer, HouseholdDTO>>();
		
		HashMap<Integer, HouseholdDTO> producthouseholdCountMap = new HashMap<Integer, HouseholdDTO>();

		// Get store level house hold information - Begin
		
		if (_storeLevelRequired == 1){
			HouseholdDTO householdCount = objMovement.getLocationHouseholdSummary(
					_conn, locationLevelId, locationId,	calendarId, categoryIdStr, calendarType);
			
			if (householdCount != null && householdCount.getHouseholdCount() > 0){
				producthouseholdCountMap.put(0, householdCount);
				householdCountMap.put(0, producthouseholdCountMap);
			}
		}
		// Get store level house hold information - Ends

		// Get product level house hold information - Starts
		if (_productGroup != null && _productGroup.size() >0){
			
			ProductGroupDTO prodGroupobj;
			
			for (int i=0; i < _productGroup.size(); i++){
				prodGroupobj = new ProductGroupDTO();				
				prodGroupobj = _productGroup.get(i);
				
				if (HouseholdCountRequired(prodGroupobj.getProductLevelId())){
					logger.debug("Get house hold information for product level: " + prodGroupobj.getProductLevelId());
					// Get house hold count for product finance department	
					if (prodGroupobj.getProductLevelId() == Constants.FINANCEDEPARTMENT)
						producthouseholdCountMap = objMovement.getProductHouseholdCountForFinDept(
							_conn, locationLevelId, locationId, 
							calendarId, prodGroupobj.getProductLevelId(), calendarType);
					else // Get house hold count for products other than finance department	
						producthouseholdCountMap = objMovement.getProductHouseholdCount(
							_conn, locationLevelId, locationId, calendarId, 
							prodGroupobj.getProductLevelId(), prodGroupobj.getChildProductData(), calendarType);
					
					if (producthouseholdCountMap != null && producthouseholdCountMap.size() > 0)
						householdCountMap.put(prodGroupobj.getProductLevelId(), producthouseholdCountMap);
				}				
			}
		}
		// Get product level house hold information - Ends
		
		return householdCountMap;
	}
	
	/*
	 * ****************************************************************
	 * Method to get the Group Data information
	 * ****************************************************************
	 */
	public void getProductGroupData() throws GeneralException, SQLException {

		SalesAggregationProductGroupDAO objProdGroupDAO = new SalesAggregationProductGroupDAO();

		if (_householdReqProductLevelList != null && _householdReqProductLevelList.size() > 0){
			//Get product group type data 
			logger.debug("Get Product Type to check aggregation required or not");
			_productGroupType = objProdGroupDAO.getProductGroupTypeData(_conn);
			
			if (_productGroupType.size() > 0 ){
				//Get the list of product mapped with item
	
				//Get product group and its child information
				logger.debug("Get Product group master data product above Item");
				_productGroup = objProdGroupDAO.getProductGroupDetails(_conn);
				
				// This block required ????
				if (_productGroup.size() > 1)
				{}
				else
					logger.info("There is no Product Group data");
			}
			else{
				logger.info("There is no Product Group Type data");
			}
		}
	}

	
	// Function to check that house hold count is required or not for product level
	private boolean HouseholdCountRequired(int productLevel){
		
		boolean _householdCountRequired = false;
		
		if (_householdReqProductLevelList != null && _householdReqProductLevelList.size() >0){
			for (int i=0; i < _householdReqProductLevelList.size(); i++){
				if (_householdReqProductLevelList.get(i) == productLevel)
					_householdCountRequired = true;
			}
		}
		return _householdCountRequired;
	}	

	
	/**
	 *****************************************************************
	 * Method to get configuration settings
	 *****************************************************************
	 */	

	private void GetConfigData(){

		// Get exclude category details - Begins
		try{
			 _categoryIdStr = PropertyManager.getProperty("MOVEMENT_EXCLUDE_CATEGORY");
		 }
		 catch(Exception ex) {
		 	logger.warn("Property MOVEMENT_EXCLUDE_CATEGORY not found in configuration"); 
		 }  
		// Get exclude category details - Ends
		
		// Get list of product levels which need house hold count - Begins
		String householdProductLevels[];
		
		String householdProductLevelsStr = PropertyManager.getProperty(
				"SALES_ANALYSIS.HOUSEHOLD_PRODUCT_LEVELS", null);
		
		
		
		_storeLevelRequired = Integer.parseInt(PropertyManager.getProperty(
				"SALES_ANALYSIS.HOUSEHOLD_STORE_LEVEL", "0"));
		
		if (_storeLevelRequired == 1)
			logger.debug("Location level household data required");
		else
			logger.debug("Location level household data not required");
		
		if (householdProductLevelsStr != null && householdProductLevelsStr.length() > 0){
			logger.debug("Production level houshold aggregation required for " + householdProductLevelsStr);
			householdProductLevels = householdProductLevelsStr.split(",");
			
			for (int i=0; i < householdProductLevels.length; i++)
				_householdReqProductLevelList.add( Integer.parseInt(householdProductLevels[i]));
		}
		else
			logger.debug("Production level houshold aggregation not required");
		// Get list of product levels which need house hold count - Ends		

	}

	
	/**
	 *****************************************************************
	 * Static method to log the command line arguments
	 *****************************************************************
	 */	
    private static void logCommand (String[] args)
    {
		StringBuffer sb = new StringBuffer("Command: HouseholdDataLoader ");
		for ( int ii = 0; ii < args.length; ii++ ) {
			if ( ii > 0 ) sb.append(' ');
			sb.append (args[ii]);
		}
		
		logger.info(sb.toString());
    }

}
