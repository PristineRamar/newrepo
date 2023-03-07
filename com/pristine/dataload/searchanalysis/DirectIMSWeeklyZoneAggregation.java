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
import java.text.DateFormat;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.time.LocalDate;
import java.time.format.DateTimeFormatter;
import java.time.temporal.ChronoUnit;
import java.util.ArrayList;
import java.util.Calendar;
import java.util.Date;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.Set;

import java.util.Map.Entry;
import org.apache.log4j.Logger;
import org.apache.log4j.PropertyConfigurator;

import com.pristine.dao.DBManager;
import com.pristine.dao.MovementDAO;
import com.pristine.dao.PriceTestDAO;
import com.pristine.dao.RetailCalendarDAO;
import com.pristine.dao.RetailPriceZoneDAO;
import com.pristine.dao.searchanalysis.ItemMetricsSummaryDao;
import com.pristine.dataload.tops.CostDataLoad;
import com.pristine.dataload.tops.PriceDataLoad;
import com.pristine.dto.PriceZoneDTO;
import com.pristine.dto.ProductMetricsDataDTO;
import com.pristine.dto.RetailCalendarDTO;
import com.pristine.dto.RetailCostDTO;
import com.pristine.dto.RetailPriceDTO;
import com.pristine.exception.GeneralException;
import com.pristine.lookup.offermgmt.PriceTestStatusLookUp;
import com.pristine.util.Constants;
import com.pristine.util.DateUtil;
import com.pristine.util.PriceTestUtil;
import com.pristine.util.PristineDBUtil;
import com.pristine.util.PropertyManager;
import com.pristine.util.offermgmt.PRCommonUtil;


/**
 * 
 * @author Pradeepkumar
 *
 */


public class DirectIMSWeeklyZoneAggregation {

	static Logger logger = Logger.getLogger("DirectIMSWeeklyZoneAggregation");
	
	private Date _startdate = null; // Processing StartDate
	private Date _endDate = null; // Processing EndDate
	private String _zoneNum = null; // Processing store number
	private static Connection _conn = null; // DB connection
	static HashSet<Integer> costNotFound;
	static HashSet<Integer> priceNotFound;
	boolean isAholdIMSAggr = false;
	boolean costDataAvailable = false;
	boolean useProductLocationMapforZoneIMSAggr = false;
	boolean usePromoCalendar = false;
	DateTimeFormatter formatter = DateTimeFormatter.ofPattern(Constants.APP_DATE_FORMAT);
	String promoCalStartDate = null;
	// Object for CostDataLoadV2
	CostDataLoad objCostLoad;

	// Object for PriceDataLoad
	PriceDataLoad objPriceLoad;
	private static int productID;
	private static int productlevelID;

	static SimpleDateFormat sdf = new SimpleDateFormat(Constants.APP_DATE_FORMAT);
	static PriceTestDAO pDao = new PriceTestDAO();
	static List<Integer> storeList ;
	static List<Integer> itemList ;
	static boolean isTestZone;
	static int tempPriceZoneId;
	static String tempZoneNum;
	static int checkTestData ;
	static int testZoneId;
	static PriceTestDAO priceTestDAO = new PriceTestDAO();
	/*
	 * ****************************************************************
	 * Class constructor
	 * 
	 * @throws Exception,GeneralException
	 * ****************************************************************
	 */
	public DirectIMSWeeklyZoneAggregation(Date startDate, Date endDate, 
			boolean noDel, String zoneNum) {
		objCostLoad = new CostDataLoad();
		objPriceLoad = new PriceDataLoad();
		isAholdIMSAggr = Boolean.parseBoolean(PropertyManager.getProperty("IMS_ZONE_AGGR_FOR_AHOLD", "FALSE"));
		useProductLocationMapforZoneIMSAggr = Boolean.parseBoolean(PropertyManager.getProperty("USE_PRODUCT_LOCATION_FOR_ZONE_STORE_MAP", "FALSE"));
		usePromoCalendar = Boolean.parseBoolean(PropertyManager.getProperty("IMS_ZONE_AGGR_USE_PROMO_CALENDAR", "FALSE"));
		// If there is no date input then set the default process date
		// which is the day prior to the date the process is run
		if (startDate == null) {
			Calendar cal = Calendar.getInstance();
			cal.setTime(new Date());
			cal.add(Calendar.DATE, -1);
			startDate = cal.getTime();
			endDate = null; // Set the end date as null
		}

		if (endDate != null) {
			logger.debug("Process Start Date: " + startDate.toString());
			logger.debug("Process End Date: " + endDate.toString());

		} else {
			logger.debug("Process Date: " + startDate.toString());
			endDate = startDate;
		}

		_startdate = startDate;
		_endDate = endDate;
		_zoneNum = zoneNum;
		if(isAholdIMSAggr) {
			promoCalStartDate = formatter
					.format(LocalDate.parse(new SimpleDateFormat("MM/dd/yyyy").format(_startdate), formatter).plus(2, ChronoUnit.DAYS));
			logger.debug("Promo calendar Start date: "+promoCalStartDate);
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

	@SuppressWarnings("static-access")
	public static void main(String[] args) throws ParseException, GeneralException {
		
	
		PropertyManager.initialize("analysis.properties");
		
		logCommand(args);
		logger.info("*** Search Item Analysis Daily Process Begins ***");
		boolean noDel = false;
		Date startDate = null;
		Date endDate = null;
		String zoneNo = "";
		String weekStartDate = "";
		String xweeksStartDate = "";
		int noOfweeks=0;
		costNotFound = new HashSet<Integer>();
		priceNotFound = new HashSet<Integer>();
		// Variable added for repair process
		DateFormat dateFormat = new SimpleDateFormat("MM/dd/yyyy");

		DirectIMSWeeklyZoneAggregation directZoneAggr = null;

		try {
			for (int ii = 0; ii < args.length; ii++) {
				String arg = args[ii];
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

				// Get the zone number from command line
				if (arg.startsWith("ZONE")) {
					zoneNo = arg.substring("ZONE=".length());
				}
				//ADDED FOR PRICETEST IMS LOADING FOR AZ BY KARISHMA
				if (arg.startsWith("PRODUCT_ID")) {
					productID = Integer.parseInt(arg.substring("PRODUCT_ID=".length()));
				}
				if (arg.startsWith("PRODUCT_LEVEL_ID")) {
					productlevelID = Integer.parseInt(arg.substring("PRODUCT_LEVEL_ID=".length()));
				}
				if (arg.startsWith("NUMBER_OF_WEEKS_FOR_IMS")) {
					noOfweeks = Integer.parseInt(arg.substring("NUMBER_OF_WEEKS_FOR_IMS=".length()));
				}
			}

		
			//DB INITIALIZATION 
			try {
				_conn = DBManager.getConnection();

			} catch (GeneralException exe) {
				logger.error("Error while connecting DB:" + exe);
				logger.info("Search List Summary Daily Ends unsucessfully");
				System.exit(1);
			}
			  
			if (startDate != null) {
				directZoneAggr = new DirectIMSWeeklyZoneAggregation(startDate, endDate, noDel, zoneNo);
				
				LoadIMS(directZoneAggr,startDate,endDate);
				
				if (testZoneId != 0) {
					priceTestDAO.updatePriceTestStatus(_conn,
							PriceTestStatusLookUp.DATA_AGGREGATION_COMPLETE.getPriceTestTypeLookupId(), testZoneId,
							Constants.ZONE_LEVEL_ID, productID, productlevelID);
				}

			} //IMS FOR PRICE TEST AZ
				else if(startDate==null && noOfweeks>0) {
				
				weekStartDate = DateUtil.getWeekStartDate(new Date(), 0);
				xweeksStartDate = DateUtil.getWeekStartDate(DateUtil.toDate(weekStartDate), noOfweeks);
				checkTestData = 0;
				tempPriceZoneId = 0;
				isTestZone = false;
				tempZoneNum = "";
				testZoneId = 0;
				storeList = null;
				itemList = null;
				try {
				HashMap<String, String> dateMap = pDao.getWeekDates(_conn, xweeksStartDate, weekStartDate);
				
				logger.info("dateMap Size: " + dateMap.size() );
				
				for (Entry<String, String> dates : dateMap.entrySet()) {

					// Create object for summary daily
					directZoneAggr = new DirectIMSWeeklyZoneAggregation(sdf.parse(dates.getKey()),
							sdf.parse(dates.getValue()), noDel, zoneNo);

					logger.info("Processing data for week: " + dates.getKey() + "," + dates.getValue());
					
					LoadIMS(directZoneAggr,sdf.parse(dates.getKey()), sdf.parse(dates.getValue()));


					}
					if (testZoneId != 0) {
						priceTestDAO.updatePriceTestStatus(_conn,
								PriceTestStatusLookUp.DATA_AGGREGATION_COMPLETE.getPriceTestTypeLookupId(), testZoneId,
								Constants.ZONE_LEVEL_ID, productID, productlevelID);
					}

				} catch (Exception e) {
					if (testZoneId != 0) {
						priceTestDAO.updatePriceTestStatus(_conn,
								PriceTestStatusLookUp.ERROR_IN_IMS.getPriceTestTypeLookupId(), testZoneId,
								Constants.ZONE_LEVEL_ID, productID, productlevelID);
					}

				}
			}
			//get the previous week date and load IMS..Added for scheduling IMS   
			else {
				startDate = dateFormat.parse(DateUtil.getWeekStartDate(new Date(), 1));
				endDate = dateFormat.parse(DateUtil.getWeekEndDate(startDate, Constants.APP_DATE_FORMAT));
				directZoneAggr = new DirectIMSWeeklyZoneAggregation(startDate, endDate, noDel, zoneNo);
				logger.info("starteDate: " + startDate + "end date : " + endDate);
				LoadIMS(directZoneAggr, startDate, endDate);

			}
		}catch (GeneralException e) {
			
			logger.error("error in IMS"+ e.getMessage());

		} finally {
			logger.info("*** Search List Summary Daily Process Ends ***");
			PristineDBUtil.close(directZoneAggr._conn);
		}

	}


	
	private void checkPriceAndCostForCurrentWeek() throws GeneralException{
			logger.debug("IMS Aggration for Ahold : "+ isAholdIMSAggr);
			DateFormat dateFormat = new SimpleDateFormat("MM/dd/yyyy");
	
			RetailCalendarDAO retailCalendarDAO = new RetailCalendarDAO();
			String startDate = dateFormat.format(_startdate);
			@SuppressWarnings("unused")
			RetailCalendarDTO retailCalendarDTO;
			
			if(isAholdIMSAggr) {
				if(!usePromoCalendar)
				{
					retailCalendarDTO = retailCalendarDAO.getCalendarId(_conn, startDate, Constants.CALENDAR_WEEK);
				}
				else
				{
				retailCalendarDTO = retailCalendarDAO.getCalendarId(_conn, promoCalStartDate, Constants.CALENDAR_WEEK);
				}
			}else {
				retailCalendarDTO = retailCalendarDAO.getCalendarId(_conn, startDate, Constants.CALENDAR_WEEK);
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
		//CompStoreDAO objStoreDao = new CompStoreDAO();
		
		MovementDAO movementDao = new MovementDAO();

		// Object for Item MetricS DAO
		ItemMetricsSummaryDao objItemMetrics = new ItemMetricsSummaryDao();
		
		// Object fro getting StoreList and Item List
		

		try {
			
			List<RetailCalendarDTO> calendarList = null;

			logger.debug("Get Calendar Id for given date range");
			calendarList = objCalendarDao.dayCalendarList(_conn, _startdate,
					_endDate, Constants.CALENDAR_DAY);
			DateFormat dateFormat = new SimpleDateFormat("MM/dd/yyyy");
			String startDateStr = dateFormat.format(_startdate);
			
			//Week Calendar details
			RetailCalendarDTO calDTO = null;
			if(isAholdIMSAggr) {
				if(usePromoCalendar) {
					calDTO = objCalendarDao.getPromoCalendarId(_conn, startDateStr, Constants.CALENDAR_WEEK);
				}
				else
				{
					calDTO = objCalendarDao.getCalendarId(_conn, startDateStr, Constants.CALENDAR_WEEK);
				}
			}else {
				calDTO = objCalendarDao.getCalendarId(_conn, startDateStr, Constants.CALENDAR_WEEK);
			}
			
			logger.debug("Weekly retail calendar Id: "+ calDTO.getCalendarId()+" start date: " + calDTO.getStartDate());
			//List<SummaryDataDTO> storeList = new ArrayList<SummaryDataDTO>();

			if(_zoneNum.trim().length() > 0){
			
				RetailPriceZoneDAO priceZoneDAO = new RetailPriceZoneDAO();
				
				PriceZoneDTO priceZoneDTO = null;
				if(isAholdIMSAggr) {
					priceZoneDTO = priceZoneDAO.getRetailPriceZoneUsingForecastTable(_conn, _zoneNum);
				}else {
					priceZoneDTO = priceZoneDAO.getRetailPriceZone(_conn, _zoneNum);
				}
				
				// Do not process zones which do not have stores in them - Ends
				if(priceZoneDTO == null){
					logger.error("Invalid Zone Number - No data found");
					System.exit(-1);
				}
				
				priceZoneDTO.setPriceZoneNum(_zoneNum);
				isTestZone = priceZoneDTO.isTestZone();
				testZoneId = priceZoneDTO.getPriceZoneId();
		
				logger.debug("Week Calendar Id:" + calDTO.getCalendarId());

				
				String categoryIdStr = PropertyManager.getProperty("MOVEMENT_INCLUDE_CATEGORY");
					
				String calendarIdStr = Constants.EMPTY;
				int sampleCalId = 0;
				for(int i = 0;i < calendarList.size(); i++){
					if(i > 0){
						calendarIdStr = calendarIdStr + "," + calendarList.get(i).getCalendarId(); 
					}else{
						calendarIdStr = String.valueOf(calendarList.get(i).getCalendarId());
						sampleCalId = calendarList.get(i).getCalendarId();
					}
				}
				
				// If its a testZone the get the storeList and itemList and Load IMS .It should check only for one time 
				//and for other weeks it will use same value
				if (isTestZone && checkTestData == 0) {
					storeList = new ArrayList<Integer>();
					itemList = new ArrayList<Integer>();

					storeList = pDao.getStoreIdsforStoreList(_conn, Constants.ZONE_LEVEL_ID,
							priceZoneDTO.getPriceZoneId(), productID, productlevelID);
					itemList = pDao.getPriceTestItemList(_conn, Constants.ZONE_LEVEL_ID, priceZoneDTO.getPriceZoneId(),
							productID, productlevelID);

					logger.info("StoreList size : - " + storeList.size() + " ItemList size:-  " + itemList.size());
					// get the zone with max stores from storeList for getting price,cost and
					// saleInfo from TLog

					String zoneInfo = new PriceTestUtil().getZoneId(_conn, storeList);
					tempZoneNum = zoneInfo.split(";")[1];
					tempPriceZoneId = Integer.parseInt(zoneInfo.split(";")[0]);
					checkTestData=1;
					logger.info ("Temp zone Id:" + tempPriceZoneId);
					
					priceTestDAO.updatePriceTestStatus(_conn, PriceTestStatusLookUp.DATA_AGGREGATION_INPROGRESS.getPriceTestTypeLookupId(), priceZoneDTO.getPriceZoneId(), Constants.ZONE_LEVEL_ID,productID,productlevelID);
					PristineDBUtil.commitTransaction(_conn,"status update");
				}

				logger.debug("Get Movement data...");
				long startTime = System.currentTimeMillis();
				List<ProductMetricsDataDTO> movementDataList = new ArrayList<ProductMetricsDataDTO>();
				if (isTestZone) {

					movementDao.getWeeklyMovementAtTestZoneLevel(_conn, calendarIdStr, priceZoneDTO, storeList,
							itemList, movementDataList);
				} else
					movementDataList = movementDao.getWeeklyMovementAtZoneLevel(_conn, calendarIdStr, -1, categoryIdStr,
							isAholdIMSAggr, useProductLocationMapforZoneIMSAggr, priceZoneDTO);

				long endTime = System.currentTimeMillis();
				logger.debug("Time taken to retrieve movements for item search " + (endTime - startTime) + " ms");

				if (movementDataList.size() > 0) {
					if(isAholdIMSAggr) {
						RetailCalendarDTO retailCalStartCalId = objCalendarDao.getCalendarId(_conn, promoCalStartDate, Constants.CALENDAR_DAY);
						
						ComputeItemMetricsForStore(calDTO, objItemMetrics, priceZoneDTO, categoryIdStr,
								calendarIdStr, retailCalStartCalId.getCalendarId(), movementDataList, movementDao,tempZoneNum,priceZoneDTO.isTestZone(),tempPriceZoneId);
					}else {
						ComputeItemMetricsForStore(calDTO, objItemMetrics, priceZoneDTO, categoryIdStr,
								calendarIdStr, sampleCalId, movementDataList, movementDao,tempZoneNum,priceZoneDTO.isTestZone(),tempPriceZoneId);
					}
				}
			}

		} catch (Exception exe) {
			logger.error(exe.getMessage());
			throw new GeneralException("processSummaryData", exe);
		}
	}
	
	
	/**
	 * 
	 * @param retailDTO
	 * @param objItemDao
	 * @param objCalendarDto
	 * @param processingZone
	 * @param gasRevMap
	 * @param gasItemMap
	 * @param categoryIdStr
	 * @param calendarIdStr
	 * @param sampleCalIDForWeek
	 * @param movementDataList
	 * @param tempLocationId 
	 * @param isTestZone 
	 * @return
	 * @throws GeneralException
	 */

	private int ComputeItemMetricsForStore(RetailCalendarDTO retailDTO,
			ItemMetricsSummaryDao objItemDao, PriceZoneDTO processingZone, String categoryIdStr,
			String calendarIdStr, int sampleCalIDForWeek, List<ProductMetricsDataDTO> movementDataList, MovementDAO movementDao, String tempZoneNum, boolean isTestZone,int tempLocationId) 
												throws GeneralException {
		int processStatus = 0;

		try {
			ItemMetricsSummaryDao objItemMS = new ItemMetricsSummaryDao();
			// delete the previous aggregation for the store and calendar id
			logger.info("Delete previous Item Metrics Data...");
			long startTime = System.currentTimeMillis();
		
			if (isTestZone)
				objItemMS.deletePreviousItemMetricsforTestZones(_conn, retailDTO.getCalendarId(),
						processingZone.getPriceZoneId(), Constants.ZONE_LEVEL_ID,itemList);
			else
				objItemMS.deletePreviousItemMetricsWeeklyV2(_conn, retailDTO.getCalendarId(),
						processingZone.getPriceZoneId(), Constants.ZONE_LEVEL_ID);
				
			long endTime = System.currentTimeMillis();
			logger.debug("Time taken to delete records - " + (endTime - startTime) + " ms");
			if (movementDataList.size() > 0) {
				// Get the distinct item code list from movement_daily
				logger.debug("Get distinct item code list");

				List<String> itemCodeList = new ArrayList<>();
				for(ProductMetricsDataDTO productMetricsDataDTO: movementDataList){
					itemCodeList.add(String.valueOf(productMetricsDataDTO.getProductId()));
				}
				logger.info("ComputeItemMetricsForStore() - distinct item code list to fetch cost,Price and sale Data: "+ itemCodeList.size());
				
				logger.debug("Time taken to retrieve distinct items - " + (endTime - startTime) + " ms");
				HashMap<String, RetailCostDTO> costDataMap = new HashMap<>();
								
				
				// Get the cost information for distinct item code
				logger.debug("Get the cost info");
				startTime = System.currentTimeMillis();

				if (isTestZone) {
					costDataMap = objCostLoad.getRetailCostV2(_conn, tempZoneNum, itemCodeList,
							sampleCalIDForWeek, null, Constants.ZONE_LEVEL_TYPE_ID);
				} else

					costDataMap = objCostLoad.getRetailCostV2(_conn, processingZone.getPriceZoneNum(), itemCodeList,
							sampleCalIDForWeek, null, Constants.ZONE_LEVEL_TYPE_ID);
					 
				endTime = System.currentTimeMillis();
				logger.debug("Time taken to retrieve cost data - " + (endTime - startTime) + " ms");

				if (costDataMap.size() < 1)
					logger.info("Cost not found");
			
				
				// Get the price information for distinct item code
				logger.debug("Get the price info");
				startTime = System.currentTimeMillis();
				
				HashMap<String, RetailPriceDTO> priceDataMap =new HashMap<String, RetailPriceDTO>();
				if (isTestZone) {
					
					priceDataMap = objPriceLoad.getRetailPrice(_conn, tempZoneNum, itemCodeList,
							sampleCalIDForWeek, null, Constants.ZONE_LEVEL_TYPE_ID);
				} else

					priceDataMap = objPriceLoad.getRetailPrice(_conn, processingZone.getPriceZoneNum(), itemCodeList,
							sampleCalIDForWeek, null, Constants.ZONE_LEVEL_TYPE_ID);
				
				
				endTime = System.currentTimeMillis();
				logger.debug("Time taken to retrieve price data - " + (endTime - startTime) + " ms");

				
					
				// Added by RB
				List<String> priceNotFoundItems = new ArrayList<>();
				for (String itemCode : itemCodeList) {
					if (!priceDataMap.containsKey(itemCode)) {
						priceNotFoundItems.add(itemCode);
					}
				}
				HashMap<String, RetailPriceDTO> regPriceDetails = new HashMap<>();
				// Added by RB
				if (priceNotFoundItems.size() >= 1) {
					
					if (isTestZone) {
						regPriceDetails = movementDao.getRegPriceDetailsFromTLog(_conn,
								new SimpleDateFormat("MM/dd/yyyy").format(_startdate),
								new SimpleDateFormat("MM/dd/yyyy").format(_endDate), priceNotFoundItems,
								tempLocationId);
					}
					else
					regPriceDetails = movementDao.getRegPriceDetailsFromTLog(_conn,
							new SimpleDateFormat("MM/dd/yyyy").format(_startdate),
							new SimpleDateFormat("MM/dd/yyyy").format(_endDate), priceNotFoundItems,
							processingZone.getPriceZoneId());
				}

				// Added Regular price details taken from tlog in PriceDataMap
				if (regPriceDetails.size() > 0) {

					for (Map.Entry<String, RetailPriceDTO> regPriceDetail : regPriceDetails.entrySet()) {
						priceDataMap.put(regPriceDetail.getKey(), regPriceDetail.getValue());
					}

				}
				
				Set<String> itemcodeforSalePrice = new HashSet<String>();
				priceDataMap.forEach((key, value) -> {
					if((value.getSalePrice() == 0 && value.getSaleMPrice() == 0))
					{
						itemcodeforSalePrice.add(key);
					}
				});
				
				HashMap<String, RetailPriceDTO> salePriceDetails =  new HashMap<String, RetailPriceDTO>();
				if (isTestZone) {
					salePriceDetails = movementDao.getSaleDetailsFromTLog(_conn,
							new SimpleDateFormat("MM/dd/yyyy").format(_startdate),
							new SimpleDateFormat("MM/dd/yyyy").format(_endDate), priceDataMap.keySet(), tempLocationId);
				} else
					salePriceDetails = movementDao.getSaleDetailsFromTLog(_conn, new SimpleDateFormat("MM/dd/yyyy").format(_startdate),
							new SimpleDateFormat("MM/dd/yyyy").format(_endDate), priceDataMap.keySet(),
							processingZone.getPriceZoneId());

				// merge sale price with regular price
				mergeSalePriceWithRegPrice(priceDataMap, salePriceDetails);
				
					
				logger.debug(" create item summary");
				movementDataList = populateItemData(movementDataList, 
						costDataMap, priceDataMap, processingZone, retailDTO);

				startTime = System.currentTimeMillis();
				logger.debug(" Begin Insert summary");
				objItemDao.insertItemMetricsSummaryWeeklyZone(_conn, movementDataList, 100);
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
			PriceZoneDTO processingZone, RetailCalendarDTO objCalendarDto) {

		List<ProductMetricsDataDTO> movementOutList = new ArrayList<ProductMetricsDataDTO>();

		for (ProductMetricsDataDTO itemDataDto : movementDataList) {
			
			itemDataDto.setCalendarId(objCalendarDto.getCalendarId());
			itemDataDto.setProductLevelId(Constants.ITEMLEVELID);
			itemDataDto.setLocationLevelId(Constants.ZONE_LEVEL_ID);
			itemDataDto.setLocationId(processingZone.getPriceZoneId());
			
			RetailCostDTO retailCostDTO = getCost(costDataMap, itemDataDto);

			RetailPriceDTO retailPriceDTO = getPrice(priceDataMap, itemDataDto);
			
			
			if(retailCostDTO.getListCost() == 0){
				//Add all the values into hash set and then display it finally at the end of function 
				/*
				 * costNotFound.add(itemDataDto.getProductId()); costDataAvailable =
				 * Boolean.parseBoolean(PropertyManager.getProperty("COST_DATA_AVAILABLE",
				 * "FALSE")); if(!costDataAvailable) { continue; }
				 */				
				// Below statement is commented for loading items for which the cost is not found
				// Commented by Kirthi on 08/02/2019
				//continue;
			}
			
			if(retailPriceDTO.getRegPrice() == 0 && retailPriceDTO.getRegMPrice() == 0){
				//Add all the values into hash set and then display it finally at the end of function 
				priceNotFound.add(itemDataDto.getProductId());
			}

			itemDataDto.setRegularMPack(retailPriceDTO.getRegQty());
			itemDataDto.setSaleMPack(retailPriceDTO.getSaleQty());
			itemDataDto.setRegularPrice(retailPriceDTO.getRegPrice());
			itemDataDto.setSalePrice(retailPriceDTO.getSalePrice());
			itemDataDto.setRegularMPrice(retailPriceDTO.getRegMPrice());
			itemDataDto.setSaleMPrice(retailPriceDTO.getSaleMPrice());
			
			
			double saleP = 0;
		    double regP = 0;
		   
			/* find sale price */
		    if (itemDataDto.getSaleMPack() > 0 && itemDataDto.getSaleMPrice() > 0) {
		       saleP = itemDataDto.getSaleMPrice() / itemDataDto.getSaleMPack();
		    }
		    else {
		       saleP = itemDataDto.getSalePrice();
		    }
			/* find regular price */
		    if (itemDataDto.getRegularMPack() > 0 && itemDataDto.getRegularMPrice() > 0) {
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
			
			if ((regP - saleP) < 0.01 || regP < saleP || 
					(retailPriceDTO.getPromotionFlag() != null 
					&& retailPriceDTO.getPromotionFlag().equals("N"))){
				itemDataDto.setSaleMPack(0);
				itemDataDto.setSalePrice(0);
				itemDataDto.setSaleMPrice(0);
				itemDataDto.setFinalPrice(regP);
			}
			
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

			itemDataDto.setTotalMovement(totalMovement);
			// Calculate regular margin and regular margin percentage
			if (retailCostDTO.getListCost() > 0) {
				double regularMargin = (regP - retailCostDTO.getListCost()) ;
				double regularMarginPct = 0.0;
				if(regP > 0 && retailCostDTO.getListCost() > 0){
					regularMarginPct = (regularMargin / regP) * 100;
				}
				itemDataDto.setListPrice(retailCostDTO.getListCost());
				itemDataDto.setRegularMargin(regularMargin);
				itemDataDto.setRegularMarginPct(regularMarginPct);
			}

			
			
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
			itemDataDto.setTotalRevenue(totalRevenue);
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


	
	
	
	
	/**
	 ***************************************************************** 
	 * Static method to log the command line arguments
	 * @throws GeneralException 
	 ***************************************************************** 
	 */

	private static void logCommand(String[] args) throws GeneralException {
		String zoneNum="";
		String productId="";
		
		StringBuffer sb = new StringBuffer("Command: SearchListDailyV2 ");
		for (int ii = 0; ii < args.length; ii++) {
			if (args[ii].startsWith("ZONE")) {
				zoneNum = args[ii].substring("ZONE=".length());
			}if (args[ii].startsWith("PRODUCT_ID")) {
				productId = args[ii].substring("PRODUCT_ID=".length());
			}
			
			if (ii > 0)
				sb.append(' ');
			sb.append(args[ii]);
		}

		setLog4jProperties(zoneNum,productId);
		logger.info(sb.toString());
	}
	private void toDisplayCostandPriceNotFoundList(){
		if(costNotFound != null && costNotFound.size()>0){
			String listOfCostRec = PRCommonUtil.getCommaSeperatedStringFromIntSet(costNotFound);
			logger.warn("Cost not found for these item code:" +listOfCostRec);
		}
		if(priceNotFound != null && priceNotFound.size()>0){
			String listOfpriceRec = PRCommonUtil.getCommaSeperatedStringFromIntSet(priceNotFound);
			logger.warn("Price not found for these Presto item code:" +listOfpriceRec);
		}
	}

	private void mergeSalePriceWithRegPrice(HashMap<String, RetailPriceDTO> priceDataMap,HashMap<String, RetailPriceDTO> salePriceDetails) {
		
		salePriceDetails.forEach((key,value)->{
			
			if(priceDataMap.containsKey(key)) {
				RetailPriceDTO retailPriceDTO = priceDataMap.get(key);
				
				//Set Sale Price and Sale qty
				retailPriceDTO.setSalePrice(value.getSalePrice());
				retailPriceDTO.setSaleQty(value.getSaleQty());
				retailPriceDTO.setSaleMPrice(0);
				retailPriceDTO.setPromotionFlag(value.getPromotionFlag());
			}
		});
	}
	
	public static void setLog4jProperties(String zoneNum, String productId) throws GeneralException {
		String logTypes = PropertyManager.getProperty("log4j.rootLogger");
		String appender = PropertyManager.getProperty("log4j.appender.logFile");
		String logPath = PropertyManager.getProperty("log4j.appender.logFile.File");
		String maxFileSize = PropertyManager.getProperty("log4j.appender.logFile.MaxFileSize");
		String patternLayout = PropertyManager.getProperty("log4j.appender.logFile.layout");
		String conversionPattern = PropertyManager.getProperty("log4j.appender.logFile.layout.ConversionPattern");

		String appenderConsole = PropertyManager.getProperty("log4j.appender.console");
		String appenderConsoleLayout = PropertyManager.getProperty("log4j.appender.console.layout");
		String appenderConsoleLayoutPattern = PropertyManager
				.getProperty("log4j.appender.console.layout.ConversionPattern");

		String curWkStartDate = DateUtil.getWeekStartDate(new Date(), 0);
		Date recWeekStartDate = DateUtil.toDate(curWkStartDate);
		SimpleDateFormat nf = new SimpleDateFormat("MM-dd-yyy");
		String dateInLog = nf.format(recWeekStartDate);

		
		String timeStamp = new SimpleDateFormat("ddMMyyyy_HHmmss").format(new java.util.Date());
		logPath = logPath + "/"+ " IMS_"+ productId + "_" + zoneNum + "_"+ dateInLog + "_" + timeStamp + ".log";

		Properties props = new Properties();
		props.setProperty("log4j.rootLogger", logTypes);
		props.setProperty("log4j.appender.logFile", appender);
		props.setProperty("log4j.appender.logFile.File", logPath);
		props.setProperty("log4j.appender.logFile.MaxFileSize", maxFileSize);
		props.setProperty("log4j.appender.logFile.layout", patternLayout);
		props.setProperty("log4j.appender.logFile.layout.ConversionPattern", conversionPattern);

		props.setProperty("log4j.appender.console", appenderConsole);
		props.setProperty("log4j.appender.console.layout", appenderConsoleLayout);
		props.setProperty("log4j.appender.console.layout.ConversionPattern", appenderConsoleLayoutPattern);
		PropertyConfigurator.configure(props);
	}

	
	public static void LoadIMS(DirectIMSWeeklyZoneAggregation directZoneAggr, Date startDate, Date endDate)
			throws GeneralException {
		// Check cost and price availability for processing week.
		directZoneAggr.checkPriceAndCostForCurrentWeek();
		// Call summary calculation process for Base Store
		logger.info("Processing data for: " + startDate + "," + startDate);
		directZoneAggr.processItemMetricsData(startDate, endDate);

		directZoneAggr.toDisplayCostandPriceNotFoundList();

	}
	
	
}

