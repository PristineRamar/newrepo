package com.pristine.service.offermgmt.oos;

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
import java.util.Set;

import org.apache.log4j.Logger;
import org.apache.log4j.PropertyConfigurator;

import com.pristine.dao.DBManager;
import com.pristine.dao.RetailCalendarDAO;
//import com.pristine.dao.offermgmt.oos.OOSAlertDAO;
import com.pristine.dao.offermgmt.oos.OOSAnalysisDAO;
import com.pristine.dao.offermgmt.oos.OOSExpectationDAO;
import com.pristine.dto.RetailCalendarDTO;
import com.pristine.dto.offermgmt.DayPartLookupDTO;
import com.pristine.dto.offermgmt.ProductKey;
import com.pristine.dto.offermgmt.oos.OOSExpectationDTO;
import com.pristine.dto.offermgmt.oos.OOSItemDTO;
import com.pristine.dto.offermgmt.oos.WeeklyAvgMovement;
import com.pristine.exception.GeneralException;
import com.pristine.util.Constants;
import com.pristine.util.DateUtil;
import com.pristine.util.PristineDBUtil;
import com.pristine.util.PropertyManager;
//import com.pristine.util.offermgmt.PRFormatHelper;

/**
 * Find out of stock item by comparing the predicted movement
 * and movement percentage
 * @author NAGARAJ
 *
 */
public class OOSAnalysis {
	private static Logger logger = Logger.getLogger("OOSAnalysis");
	private Connection conn = null;
	private String inputDateString = "";
	private Date inputDate = null;
	private Date previousDate = null;
	DateFormat formatter = new SimpleDateFormat("MM/dd/yy");
	private DayPartLookupDTO processingDayPartLookup = null;
	List<DayPartLookupDTO> dayPartLookupDTO = null;
	private int locationId = 0, locationLevelId = 0;
	private int procesingDayCalendarId = 0, processingDayId = 0, processingDayPartId = 0;
	private int weekCalendarId = 0, preDayCalendarId = 0, preDayId = 0;
	int noOfWeeksHistory = 0;
	HashSet<Integer> persisablesDepartments = new HashSet<Integer>();
	
	public OOSAnalysis() {
		PropertyConfigurator.configure("log4j-oos-analysis.properties");
		PropertyManager.initialize("recommendation.properties");
		try {
			conn = DBManager.getConnection();
		} catch (GeneralException exe) {
			logger.error("Error while connecting to DB:" + exe);
			System.exit(1);
		}
	}
	
	public static void main(String[] args) {
		OOSAnalysis oosAnalysis = new OOSAnalysis();
		//If day part id is passed, then analysis for that time slot, otherwise for previous time slot
		//If date is mentioned, then analysis for that date, otherwise for the current date
		//Get the input arguments
		if(args.length > 0){
			for(String arg : args){
				if(arg.startsWith("LOCATION_LEVEL_ID")){
					oosAnalysis.setLocationLevelId(Integer.parseInt(arg.substring("LOCATION_LEVEL_ID=".length())));
				}else if(arg.startsWith("LOCATION_ID")){
					oosAnalysis.setLocationId(Integer.parseInt(arg.substring("LOCATION_ID=".length())));
				}else if(arg.startsWith("DAY_PART_ID")){
					oosAnalysis.setDayPartId(Integer.parseInt(arg.substring("DAY_PART_ID=".length())));
				}else if(arg.startsWith("DATE")){
					oosAnalysis.setInputDate(String.valueOf(arg.substring("DATE=".length())));
				}
			}			
		}
		logger.info("**********************************************");
		oosAnalysis.outOfStockAnalysis();
		logger.info("**********************************************");
	}

	private void outOfStockAnalysis() {
		boolean validationPassed = validateInput();
		OOSAnalysisDAO oosAnalysisDAO = new OOSAnalysisDAO();
//		OOSAlertDAO oosAlertDAO = new OOSAlertDAO();
		if (validationPassed) {
			try {
				List<OOSItemDTO> oosAnalysisItems = new ArrayList<OOSItemDTO>();
				HashMap<ProductKey, Double> storeLevelAvgMovMap = new HashMap<ProductKey, Double>();
				HashMap<OOSDayPartItemAvgMovKey, WeeklyAvgMovement> dayPartLevelAvgMovMap = new HashMap<OOSDayPartItemAvgMovKey, WeeklyAvgMovement>();
				HashMap<Integer, HashMap<ProductKey, OOSExpectationDTO>> itemAndLigLevelTransaction = new HashMap<Integer, HashMap<ProductKey, OOSExpectationDTO>>();
				HashMap<OOSDayPartDetailKey, OOSDayPartDetail> prevAndprocDayPartDetail = new HashMap<OOSDayPartDetailKey, OOSDayPartDetail>();
				// Find day part id, day id and date
				initializeVariables();

				logger.info("*** OOS Analysis for location id: " + locationId + ", date: " + formatter.format(inputDate) + ", day-part: "
						+ processingDayPartLookup.getStartTime() + "--" + processingDayPartLookup.getEndTime() + " is Started... ***");

				// Get the store average movement
				logger.info("Getting store level average movement of all the items is started...");
				storeLevelAvgMovMap = oosAnalysisDAO.getWeeklyItemAvgMovement(conn, locationLevelId, locationId);
				logger.info("Getting store level average movement of all the items is completed...");

				// Get the day part average movement
				logger.info("Getting day part level average movement of all the items is started...");
				List<Integer> dayIds = new ArrayList<Integer>();
				dayIds.add(processingDayId);
				dayIds.add(preDayId);
				dayPartLevelAvgMovMap = oosAnalysisDAO.getDayPartItemAvgMovement(conn, locationLevelId, locationId, dayIds);
				logger.info("Getting day part level average movement of all the items is completed...");

				if (storeLevelAvgMovMap != null && storeLevelAvgMovMap.size() > 0 && dayPartLevelAvgMovMap != null
						&& dayPartLevelAvgMovMap.size() > 0) {

					// Get items for which out of stock to be analyzed
					logger.info("Getting OOS Candidate Item is Started...");
					oosAnalysisItems = getOOSAnalysisItems();
					logger.info("Getting OOS Candidate Item is Completed...");
					logger.info("# of OOS candidate items - " + oosAnalysisItems.size());

					// Get the actual movement for all the above items
					logger.info("Finding Actual Movement of OOS Candidate Item for the Time Slot: " + processingDayPartLookup.getStartTime() + "--"
							+ processingDayPartLookup.getEndTime() + " is Started...");
					prevAndprocDayPartDetail = getPreviousAndProcessingDayPartDetail(oosAnalysisItems);
					logger.info("Finding Actual Movement of OOS Candidate Item for the Time Slot: " + processingDayPartLookup.getStartTime() + "--"
							+ processingDayPartLookup.getEndTime() + " is Completed...");

					// Find last x weeks analysis for all the above items
					logger.info("Getting Last X Weeks Info of OOS Candidate Items is Started...");
					//HashMap<ProductKey, List<OOSItemDTO>> itemAndItsMovements = new HashMap<ProductKey, List<OOSItemDTO>>();
					HashMap<ProductKey, List<OOSItemDTO>> itemAndItsMovements = getLastXWeeksInfo(oosAnalysisItems);
					logger.info("Getting Last X Weeks Info of OOS Candidate Items is Completed...");

//					findDayPartPrediction(oosAnalysisItems, storeLevelAvgMovMap, dayPartLevelAvgMovMap);
//
//					// Get previous time slots data
//					List<Integer> prevDayPartIds = new ArrayList<Integer>();
//					for (DayPartLookupDTO dpl : dayPartLookupDTO) {
//						if (dpl.getOrder() < processingDayPartLookup.getOrder()) {
//							prevDayPartIds.add(dpl.getDayPartId());
//						}
//					}
//					HashMap<OOSAlertItemKey, OOSItemDTO> prevTimeSlotItems = new HashMap<OOSAlertItemKey, OOSItemDTO>();
//					if (prevDayPartIds.size() > 0)
//						prevTimeSlotItems = oosAlertDAO.getOOSItems(conn, locationId, procesingDayCalendarId, prevDayPartIds);

					// Get Previous day same time slot data
					// HashMap<OOSAlertItemKey, OOSItemDTO> prevDaySameTimeSlotItems = new HashMap<OOSAlertItemKey, OOSItemDTO>();
					// prevDayPartIds = new ArrayList<Integer>();
					// prevDayPartIds.add(dayPartId);
					// prevDaySameTimeSlotItems = oosAlertDAO.getOOSItems(conn, locationId, preDayCalendarId, prevDayPartIds);

					// Get Previous day transaction detail
					getTransactionDetails(procesingDayCalendarId, oosAnalysisItems, itemAndLigLevelTransaction);

					// Get previous day transaction detail of previous calendar id
					getTransactionDetails(preDayCalendarId, oosAnalysisItems, itemAndLigLevelTransaction);

					// Find out of stock items
					logger.info("Finding Out Of Stocks is Started...");
					// findOutOfStockItems(oosAnalysisItems, prevTimeSlotItems, prevDaySameTimeSlotItems, itemAndLigLevelTransaction, itemAndItsMovements);
					findOutOfStockItems(oosAnalysisItems, storeLevelAvgMovMap, dayPartLevelAvgMovMap, prevAndprocDayPartDetail,
							itemAndLigLevelTransaction, itemAndItsMovements);
					logger.info("Finding Out Of Stocks is Completed...");

					// Insert out of stock items
					logger.info("Inserting Out Of Stock Items");
					logger.info("No of items - " + oosAnalysisItems.size());
					insertOutOfStockItems(oosAnalysisItems);

					// Commit the transaction
					PristineDBUtil.commitTransaction(conn, "Transaction is Committed");
				} else {
					logger.error("Unable to Find - Out Of Stock Items. There is no Average Movement at Store or Day Part Level");
				}
			} catch (Exception | GeneralException e) {
				e.printStackTrace();
				PristineDBUtil.rollbackTransaction(conn, "Transaction is Rollbacked");
				logger.error("Exception in outOfStockAnalysis and transaction is rollbacked " + e.toString() + e + e.getMessage());
			} finally {
				PristineDBUtil.close(conn);
			}

			logger.info("*** OOS Analysis for location id: " + locationId + ", date: " + formatter.format(inputDate) + ", day-part: "
					+ processingDayPartLookup.getStartTime() + "--" + processingDayPartLookup.getEndTime() + " is Completed... ***");
		} else {
			logger.error("Invalid Input Arguments. Program Terminated");
		}
	}
	
	private void setLocationLevelId(int locationLevelId){
		this.locationLevelId = locationLevelId;
	}
	
	private void setLocationId(int locationId){
		this.locationId = locationId;
	}
	
	private void setDayPartId(int dayPartId){
		this.processingDayPartId = dayPartId;
	}
	
	private void setInputDate(String inputDate){
		this.inputDateString = inputDate;
	}
	
	private void initializeVariables() throws ParseException, GeneralException {
		OOSAnalysisDAO oosAnalysisDAO = new OOSAnalysisDAO();
		dayPartLookupDTO = oosAnalysisDAO.getDayPartLookup(conn);
		OOSService oosService = new OOSService();
		RetailCalendarDAO retailCalendarDAO = new RetailCalendarDAO();

		noOfWeeksHistory = Integer.parseInt(PropertyManager.getProperty("OOS_REPORT_LAST_X_WEEKS"));
		// Set Date
		if (inputDateString == "") {
			/*
			 * Calendar cal = Calendar.getInstance(); String dateInString =
			 * formatter.format(cal.getTime()); inputDate =
			 * formatter.parse(dateInString);
			 */
			inputDate = new Date();
		} else {
			// If date is defined, consider that date
			inputDate = formatter.parse(inputDateString);
		}
		// Set Day Part Id
		if (processingDayPartId == 0) {
			// Get previous day part id, if the previous day part id spans
			// across date then set the date as previous date
			DayPartLookupDTO tempDayPartLookup = oosService.getPreviousDayPart(dayPartLookupDTO, inputDate);
			processingDayPartId = tempDayPartLookup.getDayPartId();
			processingDayPartLookup = tempDayPartLookup;
			if (tempDayPartLookup.isDayPartSpanPrevDay() || tempDayPartLookup.isSlotSpanDays()) {
				inputDate = DateUtil.incrementDate(inputDate, -1);
			}
		} else {
			for (DayPartLookupDTO dpl : dayPartLookupDTO) {
				if (dpl.getDayPartId() == processingDayPartId) {
					processingDayPartLookup = dpl;
					break;
				}
			}
		}
		// Set Day Id
		if (processingDayId == 0) {
			Calendar c = Calendar.getInstance();
			c.setTime(inputDate);
			processingDayId = c.get(Calendar.DAY_OF_WEEK);
		}

		DateFormat tempFormatter = new SimpleDateFormat(Constants.APP_DATE_FORMAT);
		// Get the week calendar id of the day
		RetailCalendarDTO retailCalendarDTO = retailCalendarDAO.getWeekCalendarFromDate(conn, tempFormatter.format(inputDate));
		weekCalendarId = retailCalendarDTO.getCalendarId();

		retailCalendarDTO = retailCalendarDAO.getCalendarId(conn, tempFormatter.format(inputDate), Constants.CALENDAR_DAY);
		procesingDayCalendarId = retailCalendarDTO.getCalendarId();
		
		//Work only within the week, if it is Sunday, then there is no previous day
		if (processingDayId != Calendar.SUNDAY) {
			previousDate = DateUtil.incrementDate(inputDate, -1);
			retailCalendarDTO = retailCalendarDAO.getCalendarId(conn, tempFormatter.format(previousDate), Constants.CALENDAR_DAY);
			preDayCalendarId = retailCalendarDTO.getCalendarId();

			Calendar c = Calendar.getInstance();
			c.setTime(previousDate);
			preDayId = c.get(Calendar.DAY_OF_WEEK);
		}
		
		String perishablesDeptIds = PropertyManager.getProperty("PERISHABLES_DEPARTMENT_IDS");
		if (perishablesDeptIds != "") {
			String[] departmentIds = perishablesDeptIds.split(",");
			for (String departId : departmentIds) {
				persisablesDepartments.add(Integer.valueOf(departId));
			}
		}
	}
	
	/**
	 * Validate the input data
	 * @return
	 */
	private boolean validateInput() {
		boolean validationPassed = false;
		//TODO:: Later
		validationPassed = true;
		
		return validationPassed;
	}
	
	private List<OOSItemDTO> getOOSAnalysisItems() throws GeneralException {
		OOSCandidateItemImpl oosCandidateItemImpl = new OOSCandidateItemImpl();
		List<OOSItemDTO> oosAnalysisItems = oosCandidateItemImpl.getOOSCandidateItems(conn, locationLevelId, locationId, weekCalendarId);
		// Get candidate items
		/*
		 * List<PRItemDTO> finalItemList =
		 * oosCandidateItemImpl.getAuthorizedItemOfWeeklyAd(conn, 0, 0,
		 * locationLevelId, locationId, weekCalendarId, chainId);
		 */

		// Convert to OOS item object
		for (OOSItemDTO itemDTO : oosAnalysisItems) {
			itemDTO.setLocationId(locationId);
			itemDTO.setLocationLevelId(locationLevelId);
			itemDTO.setCalendarId(procesingDayCalendarId);
			itemDTO.setDayPartId(processingDayPartId);
		}
		return oosAnalysisItems;
	}
	
//	/***
//	 * Find the day part movement average percent against the weekly movement
//	 * @param locationLevelAvgMov
//	 * @param dayPartLevelAvgMov
//	 * @return
//	 */
//	private double findDayPartMovPercent(OOSItemDTO oosItemDTO, HashMap<ProductKey, Double> storeLevelAvgMovMap,
//			HashMap<OOSDayPartItemAvgMovKey, WeeklyAvgMovement> dayPartLevelAvgMovMap, int dayPartId, String operType) {
//		double dayPartMovPercent = 0;
//		double storeLevelAvgMov = 0, dayPartLevelAvgMov = 0;
//		
//		ProductKey productKey = new ProductKey(oosItemDTO.getProductLevelId(), oosItemDTO.getProductId());
//		OOSDayPartItemAvgMovKey oosDayPartItemAvgMovKey = new OOSDayPartItemAvgMovKey(processingDayId, dayPartId, oosItemDTO.getProductLevelId(),
//				oosItemDTO.getProductId());
//		
//		if (storeLevelAvgMovMap.get(productKey) != null)
//			storeLevelAvgMov = storeLevelAvgMovMap.get(productKey);
//		if (dayPartLevelAvgMovMap.get(oosDayPartItemAvgMovKey) != null)
//			dayPartLevelAvgMov = dayPartLevelAvgMovMap.get(oosDayPartItemAvgMovKey).getAverageMovement();
//		if (storeLevelAvgMov > 0) {
//			dayPartMovPercent = Double
//					.valueOf(PRFormatHelper.roundToTwoDecimalDigit((dayPartLevelAvgMov / storeLevelAvgMov) * 100));
//		}
//		//logger.debug("storeLevelAvgMov: " + storeLevelAvgMov + ",dayPartLevelAvgMov: " + dayPartLevelAvgMov);
//		if (operType != "FUTURE_DAY_PART") {
//			oosItemDTO.setStoreLevelAvgMov(storeLevelAvgMov);
//			oosItemDTO.setDayPartLevelAvgMov(dayPartLevelAvgMov);
//		}
//		
//		return dayPartMovPercent;
//	}
	
	/**
	 * Get actual movement of the item in the time slot
	 * @param oosAnalysisItem
	 * @throws GeneralException
	 * @throws SQLException 
	 * @throws ParseException 
	 */
	private HashMap<OOSDayPartDetailKey, OOSDayPartDetail> getPreviousAndProcessingDayPartDetail(List<OOSItemDTO> oosAnalysisItem)
			throws GeneralException, SQLException, ParseException {
		// Get Actual movement of 2 time slots, if it is first time slot of the
		// day, then take only that time slot movement
		// If the time slot spans date, then take transaction from current date
		// to next date
		HashMap<OOSDayPartDetailKey, OOSDayPartDetail> prevAndProcDayPartDetail =  new HashMap<OOSDayPartDetailKey, OOSDayPartDetail>();
		OOSAnalysisDAO oosAnalysisDAO = new OOSAnalysisDAO();
		OOSExpectationDAO oosExpectationDAO = new OOSExpectationDAO();
		String startTime = "", endTime = "";
		DateFormat formatter = new SimpleDateFormat("dd-MMM-yy");
		//String dateInString = "";
		DayPartLookupDTO firstDayPart = null;
		//Get first time slot
		for (DayPartLookupDTO dpl : dayPartLookupDTO) {
			if (dpl.getOrder() == 1) {
				firstDayPart = dpl;
				break;
			}
		}
		
		//Get previous day actual movement also
		if(preDayCalendarId > 0) {
			//dateInString = formatter.format(previousDate) + " " + firstDayPart.getStartTime() + ":00";
			startTime = formatter.format(previousDate) + " " + firstDayPart.getStartTime() + ":00";
		}else {
			//dateInString = formatter.format(inputDate) + " " + firstDayPart.getStartTime() + ":00";
			startTime = formatter.format(inputDate) + " " + firstDayPart.getStartTime() + ":00";
		}
		
		if (processingDayPartLookup.isSlotSpanDays()) {
			// Date nextDate = new Date(inputDate.getTime() + 2);
			Date nextDate = DateUtil.incrementDate(inputDate, 1);
			String nextDateInString = formatter.format(nextDate);
			//startTime = dateInString + " " + processingDayPartLookup.getStartTime() + ":00";
			endTime = nextDateInString + " " + processingDayPartLookup.getEndTime() + ":00";
		} else {
			//startTime = dateInString + " " + processingDayPartLookup.getStartTime() + ":00";
			endTime = formatter.format(inputDate) + " " + processingDayPartLookup.getEndTime() + ":00";
		}

		DateFormat format = new SimpleDateFormat("dd-MMM-yy H:m:s");
		java.util.Date utilDate = format.parse(startTime);
		java.sql.Timestamp startTimeDate = new java.sql.Timestamp(utilDate.getTime());
		utilDate = format.parse(endTime);
		java.sql.Timestamp endTimeDate = new java.sql.Timestamp(utilDate.getTime());

		prevAndProcDayPartDetail = oosAnalysisDAO.getDayPartDetail(conn, dayPartLookupDTO, locationId, startTimeDate, endTimeDate);
		logger.debug("prevAndProcDayPartDetail.size():" + prevAndProcDayPartDetail.size());
		//Get total transaction at store for the time slot
		oosExpectationDAO.getDayPartTotalTransOfStore(conn, dayPartLookupDTO, prevAndProcDayPartDetail, locationId, startTimeDate, endTimeDate);
		logger.debug("prevAndProcDayPartDetail.size():" + prevAndProcDayPartDetail.size());
		
		// Update processing and previous day part actual movement
//		for (OOSItemDTO oosItemDTO : oosAnalysisItem) {
//			ProductKey productKey = new ProductKey(oosItemDTO.getProductLevelId(), oosItemDTO.getProductId());
//			if (processingDayPartDetail.get(productKey) != null) {
//				oosItemDTO.getOOSCriteriaData().setActualMovOfProcessingTimeSlotOfItemOrLig(
//						processingDayPartDetail.get(productKey).getOOSCriteriaData().getActualMovOfProcessingTimeSlotOfItemOrLig());				
//			}
//			//Set same value for all items
//			oosItemDTO.getOOSCriteriaData().setTrxCntOfProcessingTimeSlotOfStore(transCntOfProcessingTimeSlot);
//		}
		return prevAndProcDayPartDetail;
	}
	
	/***
	 * Get and fill last x weeks data
	 * @param oosAnalysisItem
	 * @throws GeneralException
	 */
	private HashMap<ProductKey, List<OOSItemDTO>> getLastXWeeksInfo(List<OOSItemDTO> oosAnalysisItem) throws GeneralException{
		HashMap<ProductKey, List<OOSItemDTO>> itemAndItsMovements = new HashMap<ProductKey, List<OOSItemDTO>>();
		OOSAnalysisDAO oosAnalysisDAO = new OOSAnalysisDAO();
		OOSService oosService = new OOSService();
		List<Integer> itemCodeList = new ArrayList<Integer>();
		int limitcount = 0;
		Set<Integer> distinctItems = new HashSet<Integer>();
		
		//Keep distinct item codes, this is done to handle same item appears more than
		//and movement is counted twice, as a result no of zero mov goes in negative
		//e.g. a item appears twice and it moved 7 times, so it consider it as 14 times moved
		//and 13-14, give us -1
		for (OOSItemDTO oosItemDTO : oosAnalysisItem) {
			distinctItems.add(oosItemDTO.getProductId());
		}
		
		for (Integer itemCode : distinctItems) {
			itemCodeList.add(itemCode);
			limitcount++;
			if (limitcount > 0 && (limitcount % Constants.LIMIT_COUNT == 0)) {
				Object[] values = itemCodeList.toArray();
				oosAnalysisDAO.getLastXWeeksInfo(conn, dayPartLookupDTO, itemAndItsMovements, noOfWeeksHistory,
						locationId, processingDayId, processingDayPartId, values);
				itemCodeList.clear();
			}
		}
		if (itemCodeList.size() > 0) {
			Object[] values = itemCodeList.toArray();
			oosAnalysisDAO.getLastXWeeksInfo(conn, dayPartLookupDTO, itemAndItsMovements, noOfWeeksHistory, locationId,
					processingDayId, processingDayPartId, values);
			itemCodeList.clear();
		}
		
		//Find last x weeks info
		//Loop each item
		for (OOSItemDTO oosItemDTO : oosAnalysisItem) {
			ProductKey productKey = new ProductKey(oosItemDTO.getProductLevelId(), oosItemDTO.getProductId());
			List<Double> movements = null;
			if(itemAndItsMovements.get(productKey) != null){
				movements = new ArrayList<Double>();
				List<OOSItemDTO> movementList = itemAndItsMovements.get(productKey);
				for(OOSItemDTO movement: movementList){
					movements.add(movement.getActualWeeklyMovOfDayPart());
				}
			}
			oosService.setMovementInfo(movements, oosItemDTO, noOfWeeksHistory);
		}
		
		
		return itemAndItsMovements;
		// For debugging
//		for (OOSItemDTO oosItemDTO : oosAnalysisItem) {
//			logger.debug("Item Code:" + oosItemDTO.getItemCode() + ", No of Time Moved in Last X Weeks:" + oosItemDTO.getNoOfTimeMovedInLastXWeeks());
//		}
	}
		
//	private void findDayPartPrediction(List<OOSItemDTO> oosAnalysisItem, HashMap<ProductKey, Double> storeLevelAvgMovMap,
//			HashMap<OOSDayPartItemAvgMovKey, WeeklyAvgMovement> dayPartLevelAvgMovMap) {
//		for (OOSItemDTO oosItemDTO : oosAnalysisItem) {
//			//logger.debug("** Item Code:" + oosItemDTO.getProductId() + ",WeeklyPredictedMovement:"
//					//+ (oosItemDTO.isPersishableOrDSD() ? oosItemDTO.getClientWeeklyPredictedMovement() : oosItemDTO.getWeeklyPredictedMovement()));
//			
//			// Find the day part prediction of item
//			double dayPartMovPercent = findDayPartMovPercent(oosItemDTO, storeLevelAvgMovMap, dayPartLevelAvgMovMap, processingDayPartId, "CURRENT_DAY_PART");
//			oosItemDTO.setDayPartMovPercent(dayPartMovPercent);
//			// Use client's forecast if the item is perishable or dsd items
//			// Set client day part predicted movement
//			oosItemDTO.setClientDayPartPredictedMovement(Math.round((oosItemDTO.getClientWeeklyPredictedMovement() * dayPartMovPercent) / 100));
//
//			if (oosItemDTO.getDistFlag() == Constants.DSD || persisablesDepartments.contains(oosItemDTO.getDeptProductId())) {
//				long dayPartPredictedMovement = Math.round((oosItemDTO.getClientWeeklyPredictedMovement() * dayPartMovPercent) / 100);
//
//				oosItemDTO.getOOSCriteriaData().setForecastMovOfProcessingTimeSlotOfItemOrLig(dayPartPredictedMovement);
//				oosItemDTO.getOOSCriteriaData().setLowerConfidenceOfProcessingTimeSlotOfItemOrLig(dayPartPredictedMovement);
//				oosItemDTO.getOOSCriteriaData().setUpperConfidenceOfProcessingTimeSlotOfItemOrLig(dayPartPredictedMovement);
//
//				oosItemDTO.setPersishableOrDSD(true);
//			} else {
//				long dayPartPredictedMovement = Math.round((oosItemDTO.getWeeklyPredictedMovement() * dayPartMovPercent) / 100);
//				oosItemDTO.getOOSCriteriaData().setForecastMovOfProcessingTimeSlotOfItemOrLig(dayPartPredictedMovement);
//
//				long dayPartConfidenceLevelLower = Double.valueOf((oosItemDTO.getWeeklyConfidenceLevelLower() * dayPartMovPercent) / 100).longValue();
//				oosItemDTO.getOOSCriteriaData().setLowerConfidenceOfProcessingTimeSlotOfItemOrLig(dayPartConfidenceLevelLower);
//
//				long dayPartConfidenceLevelUpper = Double.valueOf((oosItemDTO.getWeeklyConfidenceLevelUpper() * dayPartMovPercent) / 100).longValue();
//				oosItemDTO.getOOSCriteriaData().setUpperConfidenceOfProcessingTimeSlotOfItemOrLig(dayPartConfidenceLevelUpper);
//
//				oosItemDTO.setPersishableOrDSD(false);
//			}
//
//			
//			// Find rest of the day prediction
//			// Find day part mov percent
//			long restOfTheDayPredMov = 0;
//			for (DayPartLookupDTO dpl : dayPartLookupDTO) {
//				// look for only future time slots
//				if (dpl.getDayPartId() > processingDayPartId) {
//					//logger.debug("Future Day Part Id:" + dpl.getDayPartId());
//					dayPartMovPercent = findDayPartMovPercent(oosItemDTO, storeLevelAvgMovMap, dayPartLevelAvgMovMap, dpl.getDayPartId(),
//							"FUTURE_DAY_PART");
//					// Find day part predicted movement
//					if (oosItemDTO.isPersishableOrDSD()) {
//						restOfTheDayPredMov = restOfTheDayPredMov
//								+ Math.round((oosItemDTO.getClientWeeklyPredictedMovement() * dayPartMovPercent) / 100);
//					} else {
//						restOfTheDayPredMov = restOfTheDayPredMov + Math.round((oosItemDTO.getWeeklyPredictedMovement() * dayPartMovPercent) / 100);
//					}
//					//logger.debug("dayPartMovPercent:" + dayPartMovPercent + ",restOfTheDayPredMov:" + restOfTheDayPredMov);
//				}
//			}
//			oosItemDTO.getOOSCriteriaData().setForecastMovOfRestOfTheDayOfItemOrLig(restOfTheDayPredMov);
//		}
//	}
	
	private void getTransactionDetails(int dayCalendarId, List<OOSItemDTO> oosAnalysisItem,
			HashMap<Integer, HashMap<ProductKey, OOSExpectationDTO>> itemAndLigLevelTransaction) throws GeneralException {
		OOSAnalysisDAO oosAnalysisDAO = new OOSAnalysisDAO();
		List<Integer> processedList = new ArrayList<Integer>();
		int limitcount = 0;

		// Distinct item codes
		Set<Integer> distinctItems = new HashSet<Integer>();
		for (OOSItemDTO oosItemDTO : oosAnalysisItem) {
			if (oosItemDTO.getProductLevelId() == Constants.ITEMLEVELID)
				distinctItems.add(oosItemDTO.getProductId());
		}

		// Distinct ret lir id
		Set<Integer> distinctLirId = new HashSet<Integer>();
		for (OOSItemDTO oosItemDTO : oosAnalysisItem) {
			if (oosItemDTO.getRetLirId() > 0)
				distinctLirId.add(oosItemDTO.getRetLirId());
		}

		// Get item transaction
		for (Integer itemCode : distinctItems) {
			processedList.add(itemCode);
			limitcount++;
			if (limitcount > 0 && (limitcount % Constants.LIMIT_COUNT == 0)) {
				Object[] values = processedList.toArray();
				oosAnalysisDAO.getTransactionDetail(conn, dayCalendarId, locationId, Constants.ITEMLEVELID, itemAndLigLevelTransaction, values);
				processedList.clear();
			}
		}
		if (processedList.size() > 0) {
			Object[] values = processedList.toArray();
			oosAnalysisDAO.getTransactionDetail(conn, dayCalendarId, locationId, Constants.ITEMLEVELID, itemAndLigLevelTransaction, values);
			processedList.clear();
		}

		// Get lig transaction
		limitcount = 0;
		for (Integer lirId : distinctLirId) {
			processedList.add(lirId);
			limitcount++;
			if (limitcount > 0 && (limitcount % Constants.LIMIT_COUNT == 0)) {
				Object[] values = processedList.toArray();
				oosAnalysisDAO.getTransactionDetail(conn, dayCalendarId, locationId, Constants.PRODUCT_LEVEL_ID_LIG, itemAndLigLevelTransaction,
						values);
				processedList.clear();
			}
		}
		if (processedList.size() > 0) {
			Object[] values = processedList.toArray();
			oosAnalysisDAO.getTransactionDetail(conn, dayCalendarId, locationId, Constants.PRODUCT_LEVEL_ID_LIG, itemAndLigLevelTransaction, values);
			processedList.clear();
		}
	}
	
	/***
	 * Core function which find if an item is out of stock or not
	 * @param oosAnalysisItem
	 * @param outOfStockItems
	 */
	private void findOutOfStockItems(List<OOSItemDTO> oosAnalysisItem, HashMap<ProductKey, Double> storeLevelAvgMovMap,
			HashMap<OOSDayPartItemAvgMovKey, WeeklyAvgMovement> dayPartLevelAvgMovMap,
			HashMap<OOSDayPartDetailKey, OOSDayPartDetail> prevAndprocDayPartDetail,
			HashMap<Integer, HashMap<ProductKey, OOSExpectationDTO>> itemAndLigTransactionDetail,
			HashMap<ProductKey, List<OOSItemDTO>> itemAndItsMovements) {
		OOSCriteria oosCriteria = new OOSCriteria(procesingDayCalendarId, processingDayId, processingDayPartId, preDayCalendarId,
				preDayId, processingDayPartLookup, dayPartLookupDTO, persisablesDepartments, storeLevelAvgMovMap, dayPartLevelAvgMovMap,
				prevAndprocDayPartDetail, itemAndLigTransactionDetail, itemAndItsMovements);
		oosCriteria.findOutOfStockItems(oosAnalysisItem);
	}

	/***
	 * Insert out of stock items to OOS_ITEM table
	 * @param conn
	 * @param outOfStockItems
	 * @throws Exception
	 */
	private void insertOutOfStockItems(List<OOSItemDTO> oosAnalysisItem) throws Exception {
		OOSAnalysisDAO oosAnalysisDAO = new OOSAnalysisDAO();
		//Delete existing out of stock items
		oosAnalysisDAO.deleteOOSItems(conn, locationLevelId, locationId, procesingDayCalendarId, processingDayPartId);
		if(oosAnalysisItem.size() >0)
			oosAnalysisDAO.insertOOSItems(conn, oosAnalysisItem);
	}
}
