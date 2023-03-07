/*
 * Title: Search Item Analysis Daily
 *
 *******************************************************************************
 * Modification History
 *------------------------------------------------------------------------------
 * Version		Date		Name			Remarks
 *------------------------------------------------------------------------------
 * Version 0.1	08/02/2013	Stalin			Initial Version 
 * 				03/06/2013  Ganapathy 		Added competitor data processing
 *******************************************************************************
 */
package com.pristine.dataload.searchanalysis;

import java.sql.Connection;
import java.sql.SQLException;
import java.text.DateFormat;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Calendar;
import java.util.Date;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
//import java.util.Map.Entry;
import java.util.Set;

import org.apache.log4j.Logger;
import org.apache.log4j.PropertyConfigurator;

import com.pristine.dao.CompStoreDAO;
import com.pristine.dao.DBManager;
//import com.pristine.dao.ItemDAO;
import com.pristine.dao.MovementDAO;
import com.pristine.dao.RetailCalendarDAO;
import com.pristine.dao.RetailCostDAO;
import com.pristine.dao.RetailPriceDAO;
import com.pristine.dao.salesanalysis.SalesAggregationProductGroupDAO;
import com.pristine.dao.searchanalysis.ItemMetricsSummaryDao;
import com.pristine.dataload.tops.CostDataLoad;
import com.pristine.dataload.tops.PriceDataLoad;
import com.pristine.dto.ProductMetricsDataDTO;
import com.pristine.dto.RetailCalendarDTO;
import com.pristine.dto.RetailCostDTO;
import com.pristine.dto.RetailPriceDTO;
import com.pristine.dto.salesanalysis.SummaryDataDTO;
import com.pristine.exception.GeneralException;
import com.pristine.util.Constants;
import com.pristine.util.PristineDBUtil;
import com.pristine.util.PropertyManager;
import com.pristine.util.offermgmt.PRCommonUtil;

public class SearchItemDaily {

	static Logger logger = Logger.getLogger("SearchItemDaily");
	
	private Date _startdate = null; // Processing StartDate
	private Date _endDate = null; // Processing EndDate
	private String _storeNum = null; // Processing store number
	private boolean _noDelete  = false;
	private Connection _conn = null; // DB connection
	static HashSet<Integer> costNotFound;
	static HashSet<Integer> priceNotFound;
	static boolean processingHistory = false;
	
	// Object for CostDataLoadV2
	CostDataLoad objCostLoad;

	// Object for PriceDataLoad
	PriceDataLoad objPriceLoad;


	/*
	 * ****************************************************************
	 * Class constructor
	 * 
	 * @throws Exception,GeneralException
	 * ****************************************************************
	 */
	public SearchItemDaily(Date startDate, Date endDate, String storeno, boolean noDel) {
		objCostLoad = new CostDataLoad();
		objPriceLoad = new PriceDataLoad();

		// If there is no date input then set the default process date 
		// which is the day prior to the date the process is run
		if (startDate == null ) {
			Calendar cal = Calendar.getInstance();
			cal.setTime(new Date());
			cal.add(Calendar.DATE, -1);
			startDate = cal.getTime();
			endDate = null; // Set the end date as null
		}

		if (endDate != null) {
			logger.info("Process Start Date: " + startDate.toString());
			logger.info("Process End Date: " + endDate.toString());

		} else {
			logger.info("Process Date: " + startDate.toString());
			endDate = startDate;
		}

		_storeNum = storeno;
		_startdate = startDate;
		_endDate = endDate;
		_noDelete = noDel;
		
		PropertyManager.initialize("analysis.properties");

		try {
			_conn = DBManager.getConnection();

		} catch (GeneralException exe) {
			logger.error("Error while connecting DB:" + exe);
			logger.info("Search List Summary Daily Ends unsucessfully");
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
		
		PropertyConfigurator.configure("log4j-search-item-daily.properties");
		logger.info("*** Search Item Analysis Daily Process Begins ***");
		logCommand(args);
		boolean noDel = false;
		Date startDate = null;
		Date endDate = null;
		String storeno = "";
		int scheduleId = -1;
		boolean processCompetitorStore=false;
		costNotFound = new HashSet<Integer>();
		priceNotFound = new HashSet<Integer>();
		// Variable added for repair process
		DateFormat dateFormat = new SimpleDateFormat("MM/dd/yyyy");

		SearchItemDaily summaryDaily = null;

		try {
			for (int ii = 0; ii < args.length; ii++) {
				String arg = args[ii];

				if (arg.startsWith("COMPETITOR")) {
						processCompetitorStore = true;
				}

				
				// Get the start/end date (for date range) from command line
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
						logger.error("End Date Parse Error " + par);
					}
				}

				// Get the Store number from command line
				if (arg.startsWith("STORE")) {
					storeno = arg.substring("STORE=".length());
				}

				// Get the Store number from command line
				if (arg.startsWith("SCHEDULE")) {
					try {
						scheduleId = Integer.parseInt(arg.substring("SCHEDULE=".length()));
					} catch (Exception par) {
						logger.error("Schedule Id Parse Error " + par);
					}
				}
				
				if (arg.startsWith("NODELETE")) {
					try {
						noDel = true;
					} catch (Exception par) {
						logger.error("Schedule Id Parse Error " + par);
					}
				}

				//To indicate history processing
				if (arg.startsWith("PROCESSING_HISTORY")) {
					processingHistory=Boolean.parseBoolean(arg.substring("PROCESSING_HISTORY=".length()));
				}

			}

			// Create object for summary daily
			summaryDaily = new SearchItemDaily(startDate, endDate, storeno,noDel);
			
			//Check cost and price availability for processing week. 
			summaryDaily.checkPriceAndCostForCurrentWeek();
			
			if(processCompetitorStore){
				if(scheduleId != -1){
					summaryDaily.processItemMetricsDataComp(scheduleId); 
				}else{
					summaryDaily.processItemMetricsDataComp(startDate, endDate);
				}
			}else{
				// Call summary calculation process for Base Store
				summaryDaily.processItemMetricsData(startDate, endDate);
			}
			summaryDaily.toDisplayCostandPriceNotFoundList();
		} catch (GeneralException e) {
			logger.error(e.getMessage());

		} finally {
			logger.info("*** Search List Summary Daily Process Ends ***");
			PristineDBUtil.close(summaryDaily._conn);
		}

	}

	
	
	private void checkPriceAndCostForCurrentWeek() throws GeneralException{
			DateFormat dateFormat = new SimpleDateFormat("MM/dd/yyyy");
			RetailPriceDAO retailPriceDAO = new RetailPriceDAO();
			RetailCostDAO retailCostDAO = new RetailCostDAO();
			RetailCalendarDAO retailCalendarDAO = new RetailCalendarDAO();
			String startDate = dateFormat.format(_startdate);
			RetailCalendarDTO retailCalendarDTO = retailCalendarDAO.getCalendarId(_conn, startDate, Constants.CALENDAR_WEEK);
			int weekCalendarId = retailCalendarDTO.getCalendarId();
			boolean isCostAvailable = retailCostDAO.checkCostAvailablity(_conn, weekCalendarId) ;
			boolean isPriceAvailable = retailPriceDAO.checkPriceAvailablity(_conn, weekCalendarId);
			StringBuilder sb = new StringBuilder();
			if(!isCostAvailable){
				sb.append("CRITICAL! - There is an issue in cost loading for the week of " + retailCalendarDTO.getStartDate() + ". IMS Daily cannot be processed. \n");
			}
			
			if(!isPriceAvailable){
				sb.append("CRITICAL! - There is an issue in price loading for the week of " + retailCalendarDTO.getStartDate() + ". IMS Daily cannot be processed." );
			}
			
			if(sb.length() > 0){
				throw new GeneralException(sb.toString());
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
	private void processItemMetricsData(Date startDate, Date endDate)
			throws GeneralException {

		// Object for RetailCalendarDAO
		RetailCalendarDAO objCalendarDao = new RetailCalendarDAO();

		// Object for CompStoreDAO
		CompStoreDAO objStoreDao = new CompStoreDAO();
		
		MovementDAO movementDao = new MovementDAO();

		// Object for Item MetricS DAO
		ItemMetricsSummaryDao objItemMetrics = new ItemMetricsSummaryDao();

		try {

			HashMap<String, Double> gasRevMap = new HashMap<String, Double>();
			gasRevMap = loadGasBasedRevenueData();
			
			HashMap<Integer, Integer> gasItems = new HashMap<Integer, Integer>();
			gasItems = loadGasItems();
			
			List<RetailCalendarDTO> calendarList = null;

			logger.debug("Get Calendar Id for given date range");
			calendarList = objCalendarDao.dayCalendarList(_conn, _startdate,
					_endDate, Constants.CALENDAR_DAY);

			List<SummaryDataDTO> storeList = new ArrayList<SummaryDataDTO>();

			// get Store list for day if store is not specified
			if(_storeNum.trim().length()==0){
				List<String> storeNumList =  movementDao.getDistinctStores(_conn, calendarList.get(0).getCalendarId());
				for(String storeNum:storeNumList){
					storeList.add(objStoreDao.getStoreDetails(_conn, storeNum));
				}
			}else{
				storeList.add(objStoreDao.getStoreDetails(_conn, _storeNum));
			}

			logger.info("Total processing stores count:" + storeList.size());

			// iterate the storeList in loop
			for (int sL = 0; sL < storeList.size(); sL++) {

				SummaryDataDTO objStoreDto = storeList.get(sL);

				logger.info("Process begins at Store level. Store:"
						+ objStoreDto.getstoreNumber());

				logger.debug("Total Processing Calendar Id count:"
						+ calendarList.size());
				long startTime = System.currentTimeMillis();
				// Loop for each calendar Id
				for (int dd = 0; dd < calendarList.size(); dd++) {

					RetailCalendarDTO calendarDTO = calendarList.get(dd);

					logger.info("Process begins at Calendar level. Calendar Id:"
							+ calendarDTO.getCalendarId());

					String categoryIdStr = PropertyManager.getProperty("MOVEMENT_EXCLUDE_CATEGORY");
					
					// Call Aggregation process for each calendar Id
					ComputeItemMetrics(calendarDTO, objStoreDto, objItemMetrics, gasRevMap, gasItems, categoryIdStr);

					logger.debug("Process end for Calendar Id: "
							+ calendarDTO.getCalendarId());
				}
				long endTime = System.currentTimeMillis();
				logger.info("Time taken to process a store - " + (endTime - startTime) + " ms");

			}

		} catch (Exception exe) {
			logger.error(exe.getMessage());
			throw new GeneralException("processSummaryData", exe);
		}
	}
	
	/*
	 * ****************************************************************
	 * for a given range of dates, process competitor data
	 * store id is necessary and is picked up from the input _strNum and it cannot be empty
	 * ****************************************************************
	 */
	private void processItemMetricsDataComp(Date startDate, Date endDate)
			throws GeneralException {

		// Object for RetailCalendarDAO
		RetailCalendarDAO objCalendarDao = new RetailCalendarDAO();

		// Object for CompStoreDAO
		CompStoreDAO objStoreDao = new CompStoreDAO();
		

		// Object for Item MetricS DAO
		ItemMetricsSummaryDao objItemMetrics = new ItemMetricsSummaryDao();

		try {

			List<RetailCalendarDTO> calendarList = null;

			logger.debug("Get Calendar Id for given date range");
			calendarList = objCalendarDao.dayCalendarList(_conn, _startdate,
					_endDate, Constants.CALENDAR_WEEK);


			
			if(_storeNum.trim().length()==0){
				throw new Exception("Invalid Store Number");
			}
			
			SummaryDataDTO	objStoreDto = objStoreDao.getStoreDetails(_conn, _storeNum);
			



				logger.info("Process begins at Store level. Store:"
						+ objStoreDto.getstoreNumber());

				logger.debug("Total Processing Calendar Id count:"
						+ calendarList.size());

				// Loop for each calendar Id
				for (int dd = 0; dd < calendarList.size(); dd++) {

					RetailCalendarDTO calendarDTO = calendarList.get(dd);

					logger.info("Process begins at Calendar level. Calendar Id:"
							+ calendarDTO.getCalendarId());

					// Delete old entries if they exist for same store/date combination
					objItemMetrics.deletePreviousItemMetricsComp(_conn, calendarDTO.getCalendarId(), objStoreDto.getLocationId());
					
					// Insert new entries
					objItemMetrics.InsertItemMetricsDataComp(_conn, calendarDTO.getCalendarId(), objStoreDto.getLocationId());

					logger.debug("Process end for Calendar Id: "
							+ calendarDTO.getCalendarId());
				}


		} catch (Exception exe) {
			throw new GeneralException("processSummaryDataComp", exe);
		}
	}

	/*
	 * ****************************************************************
	 * for a given range of dates, process competitor data
	 * store id is necessary and is picked up from the input _strNum and it cannot be empty
	 * ****************************************************************
	 */
	private void processItemMetricsDataComp(int scheduleId)
			throws GeneralException {

		// Object for RetailCalendarDAO
		RetailCalendarDAO objCalendarDao = new RetailCalendarDAO();

		// Object for CompStoreDAO
		CompStoreDAO objStoreDao = new CompStoreDAO();
		

		// Object for Item MetricS DAO
		ItemMetricsSummaryDao objItemMetrics = new ItemMetricsSummaryDao();

		try {



				logger.info("Process begins for Schedule Id:"
						+ scheduleId);

					// Delete old entries if they exist for same store/date combination
					objItemMetrics.deletePreviousItemMetricsComp(_conn, scheduleId);
					
					// Insert new entries
					objItemMetrics.InsertItemMetricsDataComp(_conn, scheduleId);

					logger.debug("Process end for Schedule Id: "
							+ scheduleId);

		} catch (Exception exe) {
			throw new GeneralException("processSummaryDataComp", exe);
		}
	}

	
	/*
	 * **************************************************************************
	 * Get the Store Number and Visitor Count Call the aggregation process for
	 * each Store Number and calendar Id Argument 1: RetailCalendarDTO (Calendar
	 * Id and Process Date) Argument 2: MovementDAO Argument 3: SummaryDataDTO
	 * Argument 4: ItemMetricsSummaryDao 
	 * 
	 * @throws ParseException, GeneralException
	 * **************************************************************************
	 */
	public void ComputeItemMetrics(RetailCalendarDTO calendarDTO,
			SummaryDataDTO objStoreDto, ItemMetricsSummaryDao objItemMS,
			HashMap<String, Double> gasRevMap, 
			HashMap<Integer, Integer> gasItemMap, String categoryIdStr)
			throws ParseException, GeneralException {

		try {

			// delete the previous aggregation for the store and calendar id
			logger.debug("Delete previous Item Metrics Data...");
			long startTime = System.currentTimeMillis();
			//To avoid delete in location level while loading History
			if(!processingHistory){
				objItemMS.deletePreviousItemMetrics(_conn,
						calendarDTO.getCalendarId(), objStoreDto.getLocationId());
				long endTime = System.currentTimeMillis();
				logger.debug("Time taken to delete records - " + (endTime - startTime) + " ms");
			}
			
			
			// Call method to do sales aggregation for store
			ComputeItemMetricsForStore(calendarDTO, objItemMS, calendarDTO,
					objStoreDto, gasRevMap, gasItemMap, categoryIdStr);

			logger.info("Process end for Store..."
					+ objStoreDto.getstoreNumber());

		} catch (Exception exe) {
			logger.error(" Daily Aggregation Error....", exe);
			throw new GeneralException("ComputeSummaryAggregationForDay", exe);
		}
	}

	private int ComputeItemMetricsForStore(RetailCalendarDTO retailDTO,
			ItemMetricsSummaryDao objItemDao, RetailCalendarDTO objCalendarDto,
			SummaryDataDTO objStoreDto, HashMap<String, Double> gasRevMap, 
			HashMap<Integer, Integer> gasItemMap, String categoryIdStr) 
												throws GeneralException {

		// Object for MovementDAO
		MovementDAO objMovement = new MovementDAO();

		int processStatus = 0;

		try {

			logger.debug("Get Movement data...");
			long startTime = System.currentTimeMillis();
			List<ProductMetricsDataDTO> movementDataList = objMovement
					.getMovementForItemSearch(_conn,
							objStoreDto.getLocationId(),
							objCalendarDto.getCalendarId(), -1, categoryIdStr); // debug 432858
			long endTime = System.currentTimeMillis();
			logger.debug("Time taken to retrieve movements for item search " + (endTime - startTime) + " ms");

			if (movementDataList.size() > 0) {

				// Get the distinct item code list from movement_daily
				logger.debug("Get distinct item code list");
				startTime = System.currentTimeMillis();
				List<String> itemCodeList = objMovement.getDistinctItemCode(
						_conn, objStoreDto.getstoreNumber(),
						objCalendarDto.getCalendarId());
				endTime = System.currentTimeMillis();
				logger.debug("Time taken to retrieve distinct items - " + (endTime - startTime) + " ms");
				// for DEBUG
//				List<String> itemCodeList = new ArrayList<String>();
//				itemCodeList.add("432858");
				boolean useStoreItemMap = Boolean.parseBoolean(PropertyManager.
						getProperty("USE_STORE_ITEM_MAP_FOR_UNROLLING", "FALSE"));
				HashMap<String, HashMap<String, List<String>>> itemStoreMapping = null;
				if(useStoreItemMap){
					Set<String> itemCodes = new HashSet<>(itemCodeList);
					startTime = System.currentTimeMillis();
					RetailCostDAO retailCostDAO = new RetailCostDAO();
					itemStoreMapping = retailCostDAO
							.getStoreItemMapAtZonelevel(_conn, itemCodes, objStoreDto.getstoreNumber());
					endTime = System.currentTimeMillis();
					logger.info("Time taken to retrieve items from store_item_map - " + (endTime - startTime) + "ms");
					logger.info("store_item_map size - " + itemStoreMapping.size());
				}
				
				
				// Get the cost information for distinct item code
				logger.debug("Get the cost info");
				startTime = System.currentTimeMillis();
				HashMap<String, RetailCostDTO> costDataMap = objCostLoad
						.getRetailCostV2(_conn, objStoreDto.getstoreNumber(),
								itemCodeList, objCalendarDto.getCalendarId(), itemStoreMapping, Constants.STORE_LEVEL_TYPE_ID);
				endTime = System.currentTimeMillis();
				logger.debug("Time taken to retrieve cost data - " + (endTime - startTime) + " ms");
				
				if (costDataMap.size() < 1)
					logger.info("Cost not found");

				// Get the price information for distinct item code
				logger.debug("Get the price info");
				startTime = System.currentTimeMillis();
				HashMap<String, RetailPriceDTO> priceDataMap = objPriceLoad
						.getRetailPrice(_conn, objStoreDto.getstoreNumber(),
								itemCodeList, objCalendarDto.getCalendarId(), itemStoreMapping, Constants.STORE_LEVEL_TYPE_ID);
				endTime = System.currentTimeMillis();
				logger.debug("Time taken to retrieve price data - " + (endTime - startTime) + " ms");

				if (priceDataMap.size() < 1)
					logger.info("Price not found");

				logger.debug(" Get Item Visit Count");

				/*HashMap<Double, Integer> itemVisitCount = objMovement
						.VisitorSummarybyItem(_conn,
								objStoreDto.getstoreNumber(),
								objCalendarDto.getCalendarId());*/
				
				// for DEBUG				
//				HashMap<Double, Integer> itemVisitCount = new HashMap<Double, Integer>();
//				itemVisitCount.put(432858.0,1);

				
				logger.debug(" create item summary");
				movementDataList = populateItemData(movementDataList, 
						costDataMap, priceDataMap, gasRevMap, gasItemMap, objStoreDto);
				if(processingHistory){
					logger.info("Deleting given items based on calendar id and Store id");
					objItemDao.deleteItemMetricsInItemLevel(_conn, movementDataList,
							objCalendarDto.getCalendarId(),
							objStoreDto.getLocationId());
				}
				
				startTime = System.currentTimeMillis();
				
				logger.debug(" Begin Insert summary");
				objItemDao.InsertItemMetricsData(_conn, movementDataList,
						objCalendarDto.getCalendarId(),
						objStoreDto.getLocationId());
				logger.debug(" End Insert summary");
				endTime = System.currentTimeMillis();
				logger.debug("Time taken to insert records - " + (endTime - startTime) + " ms");

			} else {
				logger.warn("Movement data not found");
			}
		} catch (Exception ex) {
			logger.error("Error in Daily Aggregation", ex);
			throw new GeneralException(" Daily Aggregation Error", ex);
		}

		return processStatus;
	}

	private List<ProductMetricsDataDTO> populateItemData(
			List<ProductMetricsDataDTO> movementDataList,
			HashMap<String, RetailCostDTO> costDataMap,
			HashMap<String, RetailPriceDTO> priceDataMap, 
			HashMap<String, Double> gasRevMap, 
			HashMap<Integer, Integer> gasItemMap,
			SummaryDataDTO objStoreDto) {

		List<ProductMetricsDataDTO> movementOutList = new ArrayList<ProductMetricsDataDTO>();

		//Get the State based Gas value - Begins 
		double gasRevValue = 1.0;
		
		if (gasItemMap.size() > 0 && gasRevMap != null)
		{
			if (gasRevMap.containsKey(objStoreDto.getstoreState()))
				gasRevValue = gasRevMap.get(objStoreDto.getstoreState());
		}
		
		logger.debug("State:" +objStoreDto.getstoreState() + ".....Rev Value:" + gasRevValue);
		
		//Get the State based Gas value - Ends
		
		for (ProductMetricsDataDTO itemDataDto : movementDataList) {

			double itemCode = itemDataDto.getProductId();
			
			//logger.debug("Processing item: " + itemDataDto.getProductId());

			//Apply State based GAS value for GAS items - Begins
			if (gasItemMap.size() > 0 && gasItemMap.containsKey(itemDataDto.getProductId()))
			{
				//logger.debug("Item:" + itemDataDto.getProductId() + " -  Before Sale:" + itemDataDto.getSaleRevenue() + "Reg: " +itemDataDto.getRegularPrice());
				if (itemDataDto.getSaleRevenue() != 0)
					itemDataDto.setSaleRevenue(itemDataDto.getSaleRevenue() * gasRevValue);
				
				if (itemDataDto.getRegularPrice() !=0)
					itemDataDto.setRegularPrice(itemDataDto.getRegularPrice() * gasRevValue);
				
				//logger.debug("Item:" + itemDataDto.getProductId() + " -   After Sale:" + itemDataDto.getSaleRevenue() + "Reg: " +itemDataDto.getRegularPrice());
			}
			//Apply State based GAS value for GAS items - Begins
			
			RetailCostDTO retailCostDTO = getCost(costDataMap, itemDataDto);

			RetailPriceDTO retailPriceDTO = getPrice(priceDataMap, itemDataDto);
			
			
			if(retailCostDTO.getListCost() == 0){
				//Add all the values into hash set and then display it finally at the end of function 
				costNotFound.add(itemDataDto.getProductId());
//				logger.error("PopulateItemData() - Ignoring current item... Cost cannot be zero for Presto item code  - " + itemDataDto.getProductId());
				continue;
			}
			
			if(retailPriceDTO.getRegPrice() == 0 && retailPriceDTO.getRegMPrice() == 0){
				//Add all the values into hash set and then display it finally at the end of function 
				priceNotFound.add(itemDataDto.getProductId());
//				logger.error("PopulateItemData() - Ignoring current item... Price cannot be zero for Presto item code  - " + itemDataDto.getProductId());
				continue;
			}

			itemDataDto.setRegularMPack(retailPriceDTO.getRegQty());
			itemDataDto.setSaleMPack(retailPriceDTO.getSaleQty());

			// Calculate sale margin and sale margin percentage
			if (retailCostDTO.getDealCost() > 0) {
				
				double saleMargin = itemDataDto.getSaleRevenue() - retailCostDTO.getDealCost()* itemDataDto.getSaleMovement();
				double saleMarginPct = 0.0;
				if(itemDataDto.getSaleRevenue()>0){
					saleMarginPct = saleMargin / itemDataDto.getSaleRevenue() * 100;
				}
				itemDataDto.setDealPrice(retailCostDTO.getDealCost());
				itemDataDto.setSaleMargin(saleMargin);
				itemDataDto.setSaleMarginPct(saleMarginPct);
			}

			double totalMovement = itemDataDto.getRegularMovement()+ itemDataDto.getSaleMovement();

			
			// Calculate regular margin and regular margin percentage
			if (retailCostDTO.getListCost() > 0) {
				double regularMargin = (itemDataDto.getRegularPrice() - retailCostDTO.getListCost()) ;
				double regularMarginPct = 0.0;
				if(itemDataDto.getRegularPrice() > 0 && retailCostDTO.getListCost() > 0){
					regularMarginPct = (regularMargin / itemDataDto.getRegularPrice()) * 100;
				}
				itemDataDto.setListPrice(retailCostDTO.getListCost());
				itemDataDto.setRegularMargin(regularMargin);
				itemDataDto.setRegularMarginPct(regularMarginPct);
			}

			itemDataDto.setRegularPrice(retailPriceDTO.getRegPrice());
			itemDataDto.setSalePrice(retailPriceDTO.getSalePrice());
			itemDataDto.setRegularMPrice(retailPriceDTO.getRegMPrice());
			itemDataDto.setSaleMPrice(retailPriceDTO.getSaleMPrice());
			
			//Condition added to set sale flag. 
			// Added by Pradeep on 08/10/2015
			if(itemDataDto.getSaleMPrice() > 0 || itemDataDto.getSalePrice() > 0)
				itemDataDto.setSaleFlag("Y");
			else
				itemDataDto.setSaleFlag("N");
			
			
			
			/*// Little crazy... cleanup needed... But it works
			if(itemDataDto.getSaleMPack() > 0 || itemDataDto.getRegularMPack() > 1){
				double saleP = (itemDataDto.getSaleMPack()>0)?itemDataDto.getSaleMPrice()/itemDataDto.getSaleMPack():0.0;
				double regP  = (itemDataDto.getRegularMPack()>0)?(itemDataDto.getRegularMPrice() + itemDataDto.getRegularPrice())/itemDataDto.getRegularMPack():0;
				
				if(itemDataDto.getSaleMPrice() ==0 || (itemDataDto.getRegularMPrice() + itemDataDto.getRegularPrice())  ==0){
					itemDataDto.setFinalPrice(Math.max(saleP,regP));
				}else{
					itemDataDto.setFinalPrice(Math.min(saleP,regP));
				}
			}else{
				if(itemDataDto.getSalePrice()==0 || itemDataDto.getRegularPrice()==0){
					itemDataDto.setFinalPrice(Math.max(itemDataDto.getSalePrice(), itemDataDto.getRegularPrice()));
				}else {
					itemDataDto.setFinalPrice(Math.min(itemDataDto.getSalePrice(), itemDataDto.getRegularPrice()));
				}
			}*/
			
			if(itemDataDto.getDealPrice() ==0 || itemDataDto.getListPrice()==0){
				itemDataDto.setFinalCost(Math.max(itemDataDto.getDealPrice(), itemDataDto.getListPrice()));
			}else {
				itemDataDto.setFinalCost(Math.min(itemDataDto.getDealPrice(), itemDataDto.getListPrice()));
			}
			
			

		    double saleP = 0;
		    double regP = 0;
		   
			/* find sale price */
		    if (itemDataDto.getSaleMPack() > 0) {
		       saleP = itemDataDto.getSaleMPrice() / itemDataDto.getSaleMPack();
		    }
		    else {
		       saleP = itemDataDto.getSalePrice();
		    }
			/* find regular price */
		    if (itemDataDto.getRegularMPack() > 0) {
		        regP = itemDataDto.getRegularMPrice() / itemDataDto.getRegularMPack();
		    }
		    else {
		        regP = itemDataDto.getRegularPrice();
		    }
		    /* find final price */
			if (saleP > 0 && saleP < regP) {
			   itemDataDto.setFinalPrice(saleP);
			}
			else {
			   itemDataDto.setFinalPrice(regP);
			}

			
			

			// Calculate Visits
			/*if (itemVisitCount.containsKey(itemCode)) {
				if (itemVisitCount.get(itemCode) > 0) {
					double totalRevenue = itemDataDto.getRegularRevenue() + itemDataDto.getSaleRevenue();
					itemDataDto.setTotalVisits(itemVisitCount.get(itemCode));
					itemDataDto.setAvgOrderSize(totalRevenue / itemVisitCount.get(itemCode));
				}
			}*/
			
			// Net margin and Net margin percentage
			//
			
			double totalRevenue = itemDataDto.getRegularRevenue() + itemDataDto.getSaleRevenue();
			double netMargin = totalRevenue-(itemDataDto.getFinalCost()*(totalMovement));
			itemDataDto.setNetMargin(netMargin);
			
			if(totalRevenue > 0){
				double netMarginPct = netMargin * 100 / totalRevenue;
				itemDataDto.setNetMarginPct(netMarginPct);
			}
			movementOutList.add(itemDataDto);

		}

		return movementOutList;
	}

	/*
	 * ****************************************************************
	 * Function to get Cost for item
	 * 1: Cost Hashmap Argument 2: Daily Movement Data DTO
	 * ****************************************************************
	 */

	public RetailCostDTO getCost(HashMap<String, RetailCostDTO> costDataMap,
			ProductMetricsDataDTO objMoveDto) {

		RetailCostDTO retailCostDTO = new RetailCostDTO();
		if (costDataMap.containsKey(String.valueOf(objMoveDto.getProductId()))) {
			retailCostDTO = costDataMap.get(String.valueOf(objMoveDto.getProductId()));
		}
		return retailCostDTO;

	}

	/*
	 * ****************************************************************
	 * Function to get Price for item
	 * 1: Price Hashmap Argument 2: Daily Movement Data DTO
	 * ****************************************************************
	 */
	
	public RetailPriceDTO getPrice(HashMap<String, RetailPriceDTO> priceDataMap,
			ProductMetricsDataDTO objMoveDto) {

		RetailPriceDTO retailPriceDTO = new RetailPriceDTO();
		if (priceDataMap.containsKey(String.valueOf(objMoveDto.getProductId()))){
			retailPriceDTO = priceDataMap.get(String.valueOf(objMoveDto.getProductId()));
		}
		return retailPriceDTO;

	}


	private HashMap<String, Double> loadGasBasedRevenueData(){
		
		// Getting the values for identify the gas value
		String storeGasDetails = PropertyManager.getProperty(
				"STATE_BASED_GAS_VALUE", null);
		logger.info("State based Gas Revenue:" + storeGasDetails);
		
			HashMap<String, Double> gasValueMap = new HashMap<String, Double>();
			String[] gasArray = storeGasDetails.split(",");
			for (int i = 0; i < gasArray.length; i++) {
				String[] splitedValue = gasArray[i].split("=");
				gasValueMap.put(splitedValue[0].trim(),
						Double.valueOf(splitedValue[1]));

			}

			return gasValueMap;
	}
	
	
	private HashMap<Integer, Integer> loadGasItems() throws SQLException, GeneralException{
		
		//Get item_code for GAS UPCs
		String gasUPS = PropertyManager.getProperty("SA_GAS_UPC", null);
		String[] UPSArray = gasUPS.split(",");
		StringBuffer gasUPSString = new StringBuffer();;

		HashMap<Integer, Integer> _gasItems = new HashMap<Integer, Integer>();
		
		for (int ii = 0; ii < UPSArray.length; ii++) {
			if (ii > 0) 
				gasUPSString.append(", ");
			
				gasUPSString.append("'").append(UPSArray[ii]).append("'");
		}
	
		SalesAggregationProductGroupDAO objProdGroupDAO = new SalesAggregationProductGroupDAO();
		_gasItems = objProdGroupDAO.getItemForGasUPC(_conn, gasUPSString.toString());

		return _gasItems;
	} 
	
	
	
	/**
	 ***************************************************************** 
	 * Static method to log the command line arguments
	 ***************************************************************** 
	 */

	private static void logCommand(String[] args) {
		StringBuffer sb = new StringBuffer("Command: SearchListDailyV2 ");
		for (int ii = 0; ii < args.length; ii++) {
			if (ii > 0)
				sb.append(' ');
			sb.append(args[ii]);
		}

		logger.info(sb.toString());
	}
	private void toDisplayCostandPriceNotFoundList(){
		if(costNotFound != null){
			String listOfCostRec = PRCommonUtil.getCommaSeperatedStringFromIntSet(costNotFound);
			logger.warn("Cost not found for these item code:" +listOfCostRec);
		}
		if(priceNotFound != null){
			String listOfpriceRec = PRCommonUtil.getCommaSeperatedStringFromIntSet(priceNotFound);
			logger.warn("Price not found for these Presto item code:" +listOfpriceRec);
		}
	}

}

