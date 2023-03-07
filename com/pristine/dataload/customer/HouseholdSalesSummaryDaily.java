/*
 * Title: Household sales summary daily
 *
 *******************************************************************************
 * Modification History
 *------------------------------------------------------------------------------
 * Version		Date		Name			Remarks
 *------------------------------------------------------------------------------
 * Version 0.1	14/03/2017	John Britto		Initial Version 
 **********************************************************************************
 */
package com.pristine.dataload.customer;

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
import com.pristine.dao.DBManager;
import com.pristine.dao.ItemDAO;
import com.pristine.dto.RetailCalendarDTO;
import com.pristine.dto.customer.HouseholdSummaryDailyInDTO;
import com.pristine.dto.salesanalysis.SpecialCriteriaDTO;
import com.pristine.dto.salesanalysis.SummaryDataDTO;
import com.pristine.exception.GeneralException;
import com.pristine.service.RetailCalendarService;
import com.pristine.service.RetailStoreService;
import com.pristine.service.customer.HouseholdSummaryService;
import com.pristine.util.Constants;
import com.pristine.util.PristineDBUtil;
import com.pristine.util.PropertyManager;

public class HouseholdSalesSummaryDaily {
	// Instance for log 
	static Logger logger = Logger.getLogger("HouseholdSalesSummaryDaily");
	// DB connection
	private Connection _conn = null;
	// Object for SpecialCriteriaDTO
	SpecialCriteriaDTO _objSpecialDto = new SpecialCriteriaDTO();
	// Collection for Unit of measures
	private HashMap<String, String> _uomMapDB = new HashMap<String, String>();
	private HashMap<String, String> _uomMapPro = new HashMap<String, String>();
	// Processing store number
	private String _storeNo = null; 
	// Processing District number
	private String _districtNo = null;
	// Process start date
	private Date _startDate = null;
	// Process end date
	private Date _endDate = null;
	// Source table reference
	private String _sourceTable = "MD";
	private String _excludeCategory = null;
    private	String _excludePOS = null;
	private int _maxPos =0;
	
	// collection to hold Gas items, we may need to exclude this from the calculation
	HashMap<Integer, Integer> _gasItems = new HashMap<Integer, Integer>();

	/*
	 * ****************************************************************
	 * Class constructor
	 * @throws Exception,GeneralException
	 * ****************************************************************
	 */
	public HouseholdSalesSummaryDaily(Date startDate, Date endDate, 
				String storeNo, String districtNo, String sourceTable) {

		if (storeNo == "" && districtNo == "") {
			logger.error(" Store No / District Id is missing in Input");
			System.exit(1);
		}

		if (districtNo != "")
			storeNo = "";

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

		if (sourceTable.equalsIgnoreCase("TL")){
			_sourceTable = "TL";
			logger.info("Source table is Transaction_Log");
		}
		
		_storeNo = storeNo;
		_districtNo = districtNo;
		_startDate = startDate;
		_endDate = endDate;

		PropertyManager.initialize("analysis.properties");

		try {
			_conn = DBManager.getConnection();
		} catch (GeneralException exe) {
			logger.error("Error while connecting DB:" + exe);
			logger.info("Household sales summay daily ends unsucessfully");
			System.exit(1);
		}
	}

	/*
	 * ****************************************************************
	 * Main method of Batch. Get the inputs from command line argument 
	 * Argument 1: Process start date
	 * Argument 2: Process end date
	 * Argument 3: Store Number 
	 * Argument 4: District Number 
	 * Argument 5: Target table
	 * ****************************************************************
	 */

	public static void main(String[] args) {
		PropertyConfigurator.configure("log4j-household-sales-summary-daily.properties");
		logger.info("*** Household sales summary daily process begins ***");
		logCommand(args);
		Date startDate = null;
		Date endDate = null;
		String storeNo = "";
		String districtNo = "";
		String sourceTable = "";
		
		DateFormat dateFormat = new SimpleDateFormat("MM/dd/yyyy");
		HouseholdSalesSummaryDaily summaryObj = null;
		try {
			for (int ii = 0; ii < args.length; ii++) {
				String arg = args[ii];

				/* Get the Start date if process is for date range*/
				if (arg.startsWith("STARTDATE")) {
					String Inputdate = arg.substring("STARTDATE=".length());
					try {
						startDate = dateFormat.parse(Inputdate);
					} catch (ParseException par) {
						logger.error("Start Date Parse Error " + par);
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
					storeNo = arg.substring("STORE=".length());
				}

				// Get the District id from command line
				if (arg.startsWith("DISTRICT")) {
					districtNo = arg.substring("DISTRICT=".length());
				}

				// get T-Log source from command Line
				if (arg.startsWith("SOURCETABLE")) {
					sourceTable = arg.substring("SOURCETABLE=".length());
				}				
			}

			// Create object for summary daily
			summaryObj = new HouseholdSalesSummaryDaily(startDate, endDate, 
											storeNo, districtNo, sourceTable);
			
			//Get the UOM and its details
			summaryObj._uomMapDB = summaryObj.getUomData();

			// Load the SpecialCriteria
			summaryObj._objSpecialDto = summaryObj.loadSpecialCriteria();
			
			// Call summary calculation process
			summaryObj.initiateSummaryDataProcess();

		} catch (GeneralException e) {
			logger.error(e.getMessage());

		} finally {
			logger.info("*** Summary Daily Process Ends ***");
			PristineDBUtil.close(summaryObj._conn);
		}

	}

	/*
	 * ****************************************************************
	 * Get the Calendar Id list for the given date range. 
	 * Get the Group Data (Group and its child information) 
	 * Call the aggregation for each calendar Id 
	 * ****************************************************************
	 */
	private void initiateSummaryDataProcess() throws GeneralException {

		// Object for RetailCalendarDAO
		RetailCalendarService calServiceObj = new RetailCalendarService();

		// Object for RetailStoreService
		RetailStoreService storeServiceObj = new RetailStoreService();
		
		// Object for Household Service
		HouseholdSummaryService hhServiceObj = new HouseholdSummaryService();
	
		try {
			logger.debug("Get Calendar Id for given date range");
			List<RetailCalendarDTO> calendarList = 
				calServiceObj.getDayCalendarList(_conn, _startDate, 
									_endDate, Constants.CALENDAR_DAY);

			//Get store list for the given store/district
			List<SummaryDataDTO> storeList = storeServiceObj.getStoreNumebrs(
											_conn, _districtNo, _storeNo);
			
			logger.info("Total stores count: " + storeList.size());

			HouseholdSummaryDailyInDTO inDto;
			
			// Iterate for each store
			for (int sLoop = 0; sLoop < storeList.size(); sLoop++) {

				SummaryDataDTO objStoreDto = storeList.get(sLoop);

				logger.info("Process begins at Store level. Store: "
											+ objStoreDto.getstoreNumber());

				logger.debug("Total Processing Calendar Id count: "
													+ calendarList.size());

				// Loop for each calendar Id
				for (int calLoop = 0; calLoop < calendarList.size(); calLoop++) {

					// Set initial values for counters
					 inDto = new HouseholdSummaryDailyInDTO();
					
					RetailCalendarDTO calendarDTO = calendarList.get(calLoop);

					logger.info("Process begins at Calendar level. Calendar Id:"
							+ calendarDTO.getCalendarId());

					// Set the input parameters
					inDto.setcalendarId(calendarDTO.getCalendarId());
					inDto.setStoreId(objStoreDto.getLocationId());
					inDto.setStoreNo(objStoreDto.getstoreNumber());
					inDto.setMaxPos(_maxPos);
					inDto.setExcludePOS(_excludePOS);
					inDto.setExcludeCategory(_excludeCategory);
					inDto.setTargetTable(_sourceTable);
					inDto.setGasItems(_gasItems);
					inDto.setUOMLookup(_uomMapDB);
					inDto.setUOMConvReq(_uomMapPro);
					
					// Call Aggregation process for each calendar Id
					hhServiceObj.processDailySummaryData(_conn, inDto);					

					logger.debug("Process end for Calendar Id:" + 
												calendarDTO.getCalendarId());
				}				
			}
		} 
		catch (Exception exe) {
			throw new GeneralException("processSummaryData", exe);
		}
	}

	/*
	 * ****************************************************************
	 * Load the SpecialCriteria and UOM for movement by volume
	 * Argument 1 : specialDTO Return specialDTO
	 * ****************************************************************
	 */

	public SpecialCriteriaDTO loadSpecialCriteria() {

		// added for getting UOM details for Movement by volume
		String uomName = PropertyManager.getProperty("MOVEMENT_VOLUME_UOM",	
																		null);
		logger.info("UOMs for Movement by volume calculation:" + uomName);

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

		String categoryIdStr = PropertyManager.getProperty("MOVEMENT_EXCLUDE_CATEGORY");
		_excludeCategory = categoryIdStr;
		

		String posIdStr = PropertyManager.getProperty("MOVEMENT_EXCLUDE_POS");
		_excludePOS = posIdStr;
		
		try {
			_maxPos = Integer.parseInt(PropertyManager.getProperty("SALES_ANALYSIS.MAX_REV_POS_DEPARTMENT", "0")) ;	
		} catch (Exception par) {
			logger.error("Error in parsing max POS" + par);
		}		
		
		return specialDTO;
	}

	/*
	 * ****************************************************************
	 * Method used to get the UOM data from UOM_LOOKUP table
	 * ****************************************************************
	 */
	public HashMap<String, String> getUomData() throws GeneralException {
		HashMap<String, String> uomMapDB = new HashMap<String, String>();
		
		// Object for ItemDao
		ItemDAO objItemDao = new ItemDAO();
		logger.debug("Get UOM master data");
		uomMapDB = objItemDao.getUOMList(_conn, "SALES_AGGR_DAILY");
		
		if (uomMapDB.size() < 1)
			logger.info("There is no UOM data");
		
		return uomMapDB;

	}
	
	
	/**
	 ***************************************************************** 
	 * Static method to log the command line arguments
	 ***************************************************************** 
	 */
	private static void logCommand(String[] args) {
		StringBuffer sb = new StringBuffer("Command: Household sales summary daily ");
		for (int ii = 0; ii < args.length; ii++) {
			if (ii > 0)
				sb.append(' ');
			sb.append(args[ii]);
		}

		logger.info(sb.toString());
	}
}