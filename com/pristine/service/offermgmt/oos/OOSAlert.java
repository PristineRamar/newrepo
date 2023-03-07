package com.pristine.service.offermgmt.oos;

import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.sql.Connection;
import java.text.ParseException;
//import java.text.DateFormat;
//import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
//import java.util.Arrays;
import java.util.Collections;
import java.util.Comparator;
import java.util.Date;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.TreeMap;
import org.apache.log4j.Logger;
import org.apache.log4j.PropertyConfigurator;
import org.apache.poi.xssf.usermodel.XSSFCell;
import org.apache.poi.xssf.usermodel.XSSFRow;
import org.apache.poi.xssf.usermodel.XSSFSheet;
import org.apache.poi.xssf.usermodel.XSSFWorkbook;
import com.pristine.dao.DBManager;
import com.pristine.dao.RetailCalendarDAO;
import com.pristine.dao.StoreDAO;
import com.pristine.dao.offermgmt.oos.OOSAlertDAO;
import com.pristine.dao.offermgmt.oos.OOSAnalysisDAO;
import com.pristine.dao.offermgmt.promotion.DisplayTypeDAO;
import com.pristine.dto.offermgmt.LocationKey;
import com.pristine.dto.offermgmt.ProductKey;
import com.pristine.dto.offermgmt.oos.OOSAlertInputDTO;
import com.pristine.dto.StoreDTO;
import com.pristine.dto.offermgmt.DayPartLookupDTO;
import com.pristine.dto.offermgmt.oos.OOSEmailAlertDTO;
import com.pristine.dto.offermgmt.oos.OOSItemDTO;
import com.pristine.exception.GeneralException;
import com.pristine.service.email.EmailService;
import com.pristine.util.Constants;
import com.pristine.util.DateUtil;
import com.pristine.util.PristineDBUtil;
import com.pristine.util.PropertyManager;


/**
 * Generates out of stock reports store by store and sends the report through
 * mail
 * 
 * @author Pradeep
 * 
 */

public class OOSAlert {
	String outputPath = null;
	String processDateStr = null;
	String rootPath = null;
	String templatePath = null;
	String timeSlot = null;
	int firstRowAdSheet = 3;
	int firstRowOOSSheet = 3;
	private static final String LOCATION_ID = "LOCATION_ID=";
	private static final String LOCATION_LEVEL_ID = "LOCATION_LEVEL_ID=";
	private static final String DAY_PART_ID = "DAY_PART_ID=";
	private static final String DATE = "DATE=";
	private static final String INTERNAL_REPORT = "INTERNAL_REPORT=";
	private static Logger logger = Logger.getLogger("OOSAlert");
	private HashMap<Integer, String> displayTypeLookup = new HashMap<Integer, String>();
	private DayPartLookupDTO processingDayPartLookup = null;
	boolean isInternalReport = false;
	String oosSheet = "OOS";
	String adItemsSheet = "Ad Items";
	String inputLocationName = null;
	Connection conn = null;
	private int inputLocationLevelId = 0;
	private int inputLocationId = 0;
//	private int prevDayPart1, prevDayPart2;
//	private int prevDayPart1CalendarId, prevDayPart2CalendarId;
	List<DayPartLookupDTO> dayPartLookup = null;
	
	public OOSAlert() {

	}

	public static void main(String[] args) {
		PropertyConfigurator.configure("log4j-oos-alert.properties");
		PropertyManager.initialize("recommendation.properties");
		String location = null;
		String locationLevelId = null;
		String dayPart = null;
		String date = null;

		OOSAlert oosAlert = new OOSAlert();

		for (String arg : args) {
			if (arg.startsWith(LOCATION_ID))
				location = arg.substring(LOCATION_ID.length());
			else if (arg.startsWith(LOCATION_LEVEL_ID))
				locationLevelId = arg.substring(LOCATION_LEVEL_ID.length());
			else if (arg.startsWith(DAY_PART_ID))
				dayPart = arg.substring(DAY_PART_ID.length());
			else if (arg.startsWith(DATE))
				date = arg.substring(DATE.length());
			else if(arg.startsWith(INTERNAL_REPORT)) 
				oosAlert.isInternalReport = Boolean.parseBoolean(arg.substring(INTERNAL_REPORT.length()));
		}
		logger.info("**********************************************");
		oosAlert.processOOSAlert(location, locationLevelId, dayPart, date);
	}

	/**
	 * Processes inputs, gets all the items in oos alert, generates excel report
	 * and sends report to client
	 * 
	 * @throws GeneralException
	 * @param location
	 * @param locationLevel
	 * @param dayPart
	 * @param date
	 * 
	 */
	public void processOOSAlert(String location, String locationLevelId, String dayPart, String date) {
		OOSAlertDAO oosAlertDAO = new OOSAlertDAO();
		try {
			logger.info("Sending OOS Alert is Started...");
			conn = getConnection();
			
			// Get display types
			getDisplayTypeLookup(conn);
						
			// Validate inputs and form required inputs in object
			OOSAlertInputDTO oosAlertInputDTO = validateInputsAndFormInputObject(conn, location, locationLevelId, dayPart,
					date, processingDayPartLookup);
			
			initializeVariables(oosAlertInputDTO);

			// Get data from OOS_ITEM with input object.
			logger.info("*** OOS Alert for location id: " + location + ", date: " + processDateStr + ", day-part: "
					+ processingDayPartLookup.getStartTime() + "--" + processingDayPartLookup.getEndTime()
					+ " is Started... ***");

			logger.info("Getting all items from OOS_ITEM Table is Started...");
			Set<ProductKey> distinctExportedItems = oosAlertDAO.getDistinctExportedItems(conn, oosAlertInputDTO);
			logger.info("Getting all items from OOS_ITEM Table is Completed.");
			
			logger.info("Getting items from OOS_ITEM Table is Started...");
			HashMap<LocationKey, List<OOSItemDTO>> itemsInOOSAlert = oosAlertDAO.getItemsInOOSAlert(conn,
					oosAlertInputDTO);
			logger.info("Getting items from OOS_ITEM Table is Completed.");
			
			List<OOSItemDTO> adItemsList = new ArrayList<OOSItemDTO>();
			List<OOSItemDTO> oosItemsList = new ArrayList<OOSItemDTO>();
			boolean isOOSPresent = false;
			String outputFile = "";
			for (Map.Entry<LocationKey, List<OOSItemDTO>> entry : itemsInOOSAlert.entrySet()) {
				setRequiredData(entry.getValue(), processDateStr, processingDayPartLookup);
				timeSlot = processingDayPartLookup.getStartTime() + " - " + processingDayPartLookup.getEndTime();
				generateDataToWrite(adItemsList, oosItemsList, distinctExportedItems, entry.getValue());
			}

			
			
			
			//Don't attach file if there are no oos items
			if (oosItemsList.size() > 0 || adItemsList.size() > 0) {
				outputFile = writeExcelFile(oosItemsList, adItemsList);
			} else {
				logger.info("NO OOS Item found");
			}
			
			logger.info("File - " + outputFile + " is generated.");

			// Get email list for the given location.
			logger.info("Getting mail list...");
			OOSEmailAlertDTO oosEmailAlertDTO = oosAlertDAO.getMailList(conn, inputLocationId, inputLocationLevelId,
					isInternalReport);
			if (oosEmailAlertDTO != null) {
				// end mail with attachment for the given location
				isOOSPresent = oosItemsList.size() > 0 ? true : false;
				asyncServiceThread(conn, oosEmailAlertDTO, outputFile, oosItemsList, true, isOOSPresent);
			}
			else{
				logger.warn("No mail list found for location - " + inputLocationId + ". Program Terminated");
				return;
			}
			logger.info("*** OOS Alert for location id: " + location + ", date: " + processDateStr + ", day-part: "
					+ processingDayPartLookup.getStartTime() + "--" + processingDayPartLookup.getEndTime()
					+ " is Completed... ***");
		} catch (GeneralException | Exception e) {
			e.printStackTrace();
			logger.error("Exception in processOOSAlert()" + e + e.getMessage());
		}
	}

	
	public void generateDataToWrite(List<OOSItemDTO> adItems, List<OOSItemDTO> oosItems,
			Set<ProductKey> distinctExportedItems, List<OOSItemDTO> currDayPartList) {
		// Find oos items if the item is already sent in previous day parts for
		// the day.
		
		eleminateLIGMember(currDayPartList);
//		int minTimeMovedInLastXWeeks = Integer.parseInt(PropertyManager.getProperty("OOS_ALERT_AD_ITEM_MIN_TIMES_MOVED_IN_X_WEEKS"));
		
		for (OOSItemDTO oosItemDTO : currDayPartList) {
			if(oosItemDTO.getRetailerItemCode() == null){
				oosItemDTO.setRetailerItemCode(Constants.EMPTY);
			}
			if(oosItemDTO.getCategoryName() == null){
				oosItemDTO.setCategoryName(Constants.EMPTY);
			}
			if(oosItemDTO.getDepartmentName() == null){
				oosItemDTO.setDepartmentName(Constants.EMPTY);
			}
			//Add it only if the minimum move in last 13 weeks is at least X
//			if ((oosItemDTO.getOOSCriteriaData().getActualMovOfProcessingTimeSlotOfItemOrLig() == 0l
//					&& oosItemDTO.getNoOfTimeMovedInLastXWeeksOfItemOrLig() >= minTimeMovedInLastXWeeks)
//					|| oosItemDTO.getOOSCriteriaId() == OOSCriteriaLookup.CRITERIA_6.getOOSCriteriaid()) {
				adItems.add(oosItemDTO);
//			}
			ProductKey productKey = new ProductKey(oosItemDTO.getProductLevelId(), oosItemDTO.getProductId());
			if (!distinctExportedItems.contains(productKey)) {
				if (oosItemDTO.getIsSendToClient()) {
					oosItems.add(oosItemDTO);
				}
			}
		}
	}

	/**
	 * Validates input and forms input object
	 * 
	 * @param location
	 * @param locationLevel
	 * @param dayPart
	 * @param date
	 * @return OOSAlertInputDTO input object
	 * @throws GeneralException
	 */
	private OOSAlertInputDTO validateInputsAndFormInputObject(Connection conn, String location, String locationLevel,
			String dayPart, String date, DayPartLookupDTO oosDayPartLookupDTO) throws GeneralException {
		OOSAnalysisDAO oosAnalysisDAO = new OOSAnalysisDAO();
		OOSAlertInputDTO oosAlertInputDTO = new OOSAlertInputDTO();
		List<Integer> locationList = new ArrayList<Integer>();
		//List<Integer> dayPartList = new ArrayList<Integer>();
		int dayPartId = 0;
		RetailCalendarDAO retailCalendarDAO = new RetailCalendarDAO();
		OOSService oosService = new OOSService();

		dayPartLookup = oosAnalysisDAO.getDayPartLookup(conn);

		try {
			SimpleDateFormat dateFormat = new SimpleDateFormat(Constants.APP_DATE_FORMAT);
			int locationLevelId;
			int calendarId = 0;
			Date processingDate = null;
			if (locationLevel == null)
				locationLevelId = Constants.STORE_LEVEL_ID;
			else
				locationLevelId = Integer.parseInt(locationLevel);
			inputLocationLevelId = locationLevelId;
			inputLocationId = Integer.parseInt(location);
			
			if (date == null) {
				processingDate = new Date();
				processDateStr = dateFormat.format(processingDate);
			} else {
				processDateStr = date;
				processingDate = dateFormat.parse(processDateStr);
			}
			
			if (dayPart == null) {
				// Assigns previous day part if day part is not given
				processingDayPartLookup = oosService.getPreviousDayPart(dayPartLookup, processingDate);
				dayPartId = processingDayPartLookup.getDayPartId();
				//dayPartList.add(processingDayPartLookup.getDayPartId());
				// If day part spans previous day, decrement current date to
				// prev date.
				if (processingDayPartLookup.isDayPartSpanPrevDay() || processingDayPartLookup.isSlotSpanDays()) {
					processingDate = DateUtil.incrementDate(processingDate, -1);
					processDateStr = dateFormat.format(processingDate);
				}
			} else {
				for (DayPartLookupDTO dpl : dayPartLookup) {
					if (dpl.getDayPartId() == Integer.parseInt(dayPart)) {
						processingDayPartLookup = dpl;
						//dayPartList.add(Integer.parseInt(dayPart));
						dayPartId = Integer.parseInt(dayPart);
						break;
					}
				}
			}
			try {
				calendarId = retailCalendarDAO.getCalendarId(conn, processDateStr, Constants.CALENDAR_DAY)
						.getCalendarId();
			} catch (GeneralException ge) {
				logger.error("Error while getting calendar id - " + ge.toString());
				throw new GeneralException("Error while getting calendar id", ge);
			}
			
			//Find stores to process
			if (locationLevelId == Constants.STORE_LEVEL_ID) {
				locationList.add(inputLocationId);
			} else if (locationLevelId == Constants.DISTRICT_LEVEL_ID) {
				// Not all the stores in the district is processed, use OOS_ALERT
				// table to determined which store to consider for the district
				OOSAlertDAO alertDAO = new OOSAlertDAO();
				locationList = alertDAO.getStoresInDistrictFromOOSItem(conn, calendarId, inputLocationId);
			}
			
			oosAlertInputDTO.setCalendarId(calendarId);
			oosAlertInputDTO.setDayPartId(dayPartId);
			oosAlertInputDTO.setDayPartExecOrder(processingDayPartLookup.getOrder());
			oosAlertInputDTO.setLocationLevelId(Constants.STORE_LEVEL_ID);
			oosAlertInputDTO.setLocationList(locationList);
		} catch (Exception e) {
			logger.error("Error while processing inputs", e);
			throw new GeneralException("Error while processing inputs", e);
		}
		return oosAlertInputDTO;
	}

	/**
	 * Writes excel file from the object.
	 * 
	 * @param itemsInOOSAlert
	 * @return output file
	 * @throws GeneralException
	 */
	public String writeExcelFile(List<OOSItemDTO> itemsInOOSAlert, List<OOSItemDTO> adItemList) throws GeneralException {
		String outFile = outputPath + "/OOS_" + inputLocationName + "_Date_" + processDateStr.replaceAll("/", "-")
				+ "_Time_" + timeSlot.replaceAll(":", ".").replaceAll(" ", "") + ".xlsx";
		try {
			FileInputStream inputStream = new FileInputStream(templatePath);
			XSSFWorkbook workbook = new XSSFWorkbook(inputStream);
			writeDataToSheet(workbook, itemsInOOSAlert, oosSheet, firstRowOOSSheet);
			writeDataToSheet(workbook, adItemList, adItemsSheet, firstRowAdSheet);
			FileOutputStream outputStream = new FileOutputStream(outFile);
			workbook.write(outputStream);
			workbook.close();
			inputStream.close();
			outputStream.close();
		} catch (Exception e) {
			throw new GeneralException("Error while writing excel file", e);
		}
		return outFile;
	}
	
	private void writeDataToSheet(XSSFWorkbook workbook, List<OOSItemDTO> itemList, String sheetName, int firstRowIndex) {
		if (itemList.size() > 0) {
			Collections.sort(itemList, new Comparator<OOSItemDTO>() {
				@Override
				public int compare(final OOSItemDTO oosItemDTO1, final OOSItemDTO oosItemDTO2) {
					int c;
			/*	c = oosItemDTO1.getDistrictName().compareTo(oosItemDTO2.getDistrictName());
					if (c == 0)*/
						c = oosItemDTO1.getStoreNo().compareTo(oosItemDTO2.getStoreNo());
					/*if (c == 0)
						c = oosItemDTO1.getDepartmentName().compareTo(oosItemDTO2.getDepartmentName());*/
					if (c == 0)
						c = oosItemDTO1.getCategoryName().compareTo(oosItemDTO2.getCategoryName());
					if (c == 0)
						c = Long.compare(oosItemDTO2.getOOSCriteriaData().getForecastMovOfProcessingTimeSlotOfItemOrLig(),
								oosItemDTO1.getOOSCriteriaData().getForecastMovOfProcessingTimeSlotOfItemOrLig());
					return c;
				}
			});
			XSSFSheet sheet = workbook.getSheet(sheetName);
			editHeading(sheet);
			int rowNum = firstRowIndex;
			for (OOSItemDTO oosItemDTO : itemList) {
				XSSFRow row = sheet.createRow(rowNum++);
				writeToCell(oosItemDTO, row, sheetName);
			}
		}
	}

	/**
	 * creates cell and writes data to cell
	 * 
	 * @param oosItemDTO
	 * @param row
	 */
	private void writeToCell(OOSItemDTO oosItemDTO, XSSFRow row, String sheetName) {
		TreeMap<Integer, Object> objMap = mapObjectWithCellNum(oosItemDTO, sheetName);
		for (Map.Entry<Integer, Object> entry : objMap.entrySet()) {
			int cellNum = entry.getKey();
			XSSFCell cell = row.createCell(cellNum);
			setValue(entry.getValue(), cell);
		}
	}

	/**
	 * sets value to cell
	 * 
	 * @param value
	 * @param cell
	 */
	private void setValue(Object value, XSSFCell cell) {
		if (value instanceof String) {
			cell.setCellValue(String.valueOf(value));
		} else if (value instanceof Integer) {
				cell.setCellValue((Integer) value);
		} else if (value instanceof Long) {
				cell.setCellValue((Long) value);
		}
	}

	/**
	 * Gives map of the OOSItemDTO with cell number
	 * 
	 * @param oosItemDTO
	 * @return Map of the object
	 */
	private TreeMap<Integer, Object> mapObjectWithCellNum(OOSItemDTO oosItemDTO, String sheetName) {
		int cellCount = 0;
		
		TreeMap<Integer, Object> fields = new TreeMap<Integer, Object>();
		//fields.put(cellCount++, oosItemDTO.getDistrictName());
		// Store #
		fields.put(cellCount++, oosItemDTO.getStoreNo()); 
		// retailer item code
		fields.put(cellCount++, oosItemDTO.getRetailerItemCode()); 
		// item name
		fields.put(cellCount++, oosItemDTO.getItemName()); 
		// Category name
		fields.put(cellCount++, oosItemDTO.getCategoryName()); 
		// processing date
		//fields.put(cellCount++, oosItemDTO.getProcessingDate()); 
		// time slot															 
		//fields.put(cellCount++, oosItemDTO.getTimeSlot()); 
		// regular unit price
		fields.put(cellCount++, oosItemDTO.getRegularPriceString()); 
		// sale unit price
		fields.put(cellCount++, oosItemDTO.getSalePriceString());
		// Page #
		fields.put(cellCount++, (oosItemDTO.getAdPageNo() > 0 ? oosItemDTO.getAdPageNo() : Constants.EMPTY)); 
		// # of facings
		fields.put(cellCount++, (oosItemDTO.getNoOfFacings() > 0 ? oosItemDTO.getNoOfFacings() : Constants.EMPTY));
		// display
		fields.put(cellCount++, displayTypeLookup.get(oosItemDTO.getDisplayTypeId()) == null ? Constants.EMPTY
				: displayTypeLookup.get(oosItemDTO.getDisplayTypeId()));
		// # of zero movement occurrences
		fields.put(cellCount++, oosItemDTO.getNoOfZeroMovInLastXWeeks());
		// Minimum units across 13 weeks
		fields.put(cellCount++, oosItemDTO.getMinMovementInLastXWeeks()); 
		// Max units across 13 weeks
		fields.put(cellCount++, oosItemDTO.getMaxMovementInLastXWeeks());
		// Avg units across 13 weeks
		fields.put(cellCount++, oosItemDTO.getAvgMovementInLastXWeeks());	
		
		// day part predicted movement
		fields.put(cellCount++, oosItemDTO.getOOSCriteriaData().getForecastMovOfProcessingTimeSlotOfItemOrLig());
		//client forecast
		fields.put(cellCount++, oosItemDTO.getClientDayPartPredictedMovement());
		// confidence level range lower
		fields.put(cellCount++, oosItemDTO.getOOSCriteriaData().getLowerConfidenceOfProcessingTimeSlotOfItemOrLig());
		// confidence level range higher
		fields.put(cellCount++, oosItemDTO.getOOSCriteriaData().getUpperConfidenceOfProcessingTimeSlotOfItemOrLig());
		
		//Weekly forecast
		fields.put(cellCount++, oosItemDTO.getWeeklyPredictedMovement());
		
		
		// day part actual movement
		fields.put(cellCount++, oosItemDTO.getOOSCriteriaData().getActualMovOfProcessingTimeSlotOfItemOrLig());
		
	
		fields.put(cellCount++,
				(oosItemDTO.getOOSCriteriaData().getNoOfConsecutiveTimeSlotItemDidNotMove() == null
						|| oosItemDTO.getOOSCriteriaData().getNoOfConsecutiveTimeSlotItemDidNotMove() == 0 ? ""
								: oosItemDTO.getOOSCriteriaData().getNoOfConsecutiveTimeSlotItemDidNotMove()));
		
		if (oosItemDTO.getOOSCriteriaId() == 0) {
			fields.put(cellCount++, Constants.EMPTY);
		} else if (oosItemDTO.getOOSCriteriaId() == OOSCriteriaLookup.CRITERIA_6.getOOSCriteriaId()) {
			fields.put(cellCount++, "Potential OOS");
		} else {
			fields.put(cellCount++, "Criteria " + oosItemDTO.getOOSCriteriaId());
		}
		
		//Department
		fields.put(cellCount++, oosItemDTO.getDepartmentName());
		
		if(sheetName.equals(adItemsSheet)){
			//Latest time slot 
			//a.	Expected units based on # of store transactions (Already available)
			fields.put(cellCount++, oosItemDTO.getOOSCriteriaData().getTrxBasedPredOfProcessingTimeSlotOfItemOrLig() == -1 ?
					Constants.EMPTY : oosItemDTO.getOOSCriteriaData().getTrxBasedPredOfProcessingTimeSlotOfItemOrLig());
			//b.	# of total store transactions  (same for all items)
			fields.put(cellCount++, oosItemDTO.getOOSCriteriaData().getTrxCntOfProcessingTimeSlotOfStore() == -1 ?
					Constants.EMPTY : oosItemDTO.getOOSCriteriaData().getTrxCntOfProcessingTimeSlotOfStore());
			//previous days of the week
			//a.	# of store transactions  (same for all items)
			fields.put(cellCount++, oosItemDTO.getOOSCriteriaData().getTrxCntOfPrevDaysOfStore() == -1 ?
					Constants.EMPTY : oosItemDTO.getOOSCriteriaData().getTrxCntOfPrevDaysOfStore());
			//b.	# of transactions in which this item was bought 
			fields.put(cellCount++, oosItemDTO.getOOSCriteriaData().getTrxCntOfPrevDaysOfItemOrLig() == -1 ?
					Constants.EMPTY : oosItemDTO.getOOSCriteriaData().getTrxCntOfPrevDaysOfItemOrLig());
			//c.	# of units sold
			fields.put(cellCount++, oosItemDTO.getOOSCriteriaData().getActualMovOfPrevDaysOfItemOrLig() == -1 ?
					Constants.EMPTY : oosItemDTO.getOOSCriteriaData().getActualMovOfPrevDaysOfItemOrLig());
			
			//No of units sold for the day. (Add actual movement of processing timeslot and actual movement of previous time slot(s).
			fields.put(cellCount++, oosItemDTO.getOOSCriteriaData().getActualMovOfProcessingDayItemOrLig());
			
			//Shelf Capacity
			fields.put(cellCount++, oosItemDTO.getOOSCriteriaData().getShelfCapacityOfItemOrLig() > 0
					? oosItemDTO.getOOSCriteriaData().getShelfCapacityOfItemOrLig() : Constants.EMPTY);
		}
		
		fields.put(cellCount++, "");// blank columns for manual verification
		fields.put(cellCount++, "");// blank columns for manual verification
		fields.put(cellCount++, "");// blank columns for manual verification
		fields.put(cellCount++, "");// blank columns for manual verification
		fields.put(cellCount++, "");// blank columns for manual verification
		return fields;
	}

	/**
	 * calls mail service and sends mail with produced excel report
	 * 
	 * @param oosEmailAlertDTO
	 * @param file
	 * @return mail status
	 * @throws GeneralException
	 */
	private boolean sendAlertThroughMail(OOSEmailAlertDTO oosEmailAlertDTO, String file, boolean isOOSPresent) throws GeneralException {
		boolean mailStatus = false;
		String from = PropertyManager.getProperty("MAIL.FROMADDR", "");
		String mailSubject = "";
		if (from.isEmpty())
			throw new GeneralException("Unable to send mail without MAIL.FROMADDR property");
		String subject = "OOS Verification Alert for " + inputLocationName + " for " + processDateStr + " for time slot "
				+ timeSlot;
		List<String> fileList = new ArrayList<String>();
		
		if(file != "") {
			fileList.add(file);
		}
		
		if(isOOSPresent)
			mailSubject = "Attached file has out of stock alert ";
		else
			mailSubject = "Presto did not find any out of stock items ";
		
		String mailConetent = mailSubject + "for " + inputLocationName + " for the time slot "
				+ processDateStr + " " + timeSlot + "\n \n" + Constants.MAIL_CONFIDENTIAL_NOTE;
		mailStatus = EmailService.sendEmail(from, oosEmailAlertDTO.getMailToList(), oosEmailAlertDTO.getMailCCList(),
				oosEmailAlertDTO.getMailBCCList(), subject, mailConetent, fileList);
		return mailStatus;
	}

	/**
	 * asynchronously triggers mail for each location
	 * 
	 * @param conn
	 * @param oosEmailAlertDTO
	 * @param file
	 * @param updateSet
	 * @param closeConnection
	 */
	private void asyncServiceThread(final Connection conn, final OOSEmailAlertDTO oosEmailAlertDTO, final String file,
			final List<OOSItemDTO> updateList, final boolean closeConnection, final boolean isOOSPresent) {
		Runnable task = new Runnable() {
			@Override
			public void run() {
				try {
					logger.info("Sending mail...");
					OOSAlertDAO oosAlertDAO = new OOSAlertDAO();
					boolean mailStatus = sendAlertThroughMail(oosEmailAlertDTO, file, isOOSPresent);
					if (mailStatus) {
						// update alert sending status.
						logger.info("Updating alert sending status for location - " + oosEmailAlertDTO.getLocationId());
						oosAlertDAO.updateAlertSendingStatus(conn, updateList);
						// commit transaction
						PristineDBUtil.commitTransaction(conn, "OOS alert sending");
					}
				} catch (GeneralException e) {
					logger.error(e.getMessage(), e);
					PristineDBUtil.rollbackTransaction(conn, "OOS alert sending");
				} finally {
					if (closeConnection) {
						logger.info("Alerts sent successfully");
						logger.info("**********************************************");
						PristineDBUtil.close(conn);
					}
				}
			}
		};
		new Thread(task, "ServiceThread").start();
	}

	/**
	 * Sets rootpath, output
	 * 
	 * @throws GeneralException
	 * @throws ParseException 
	 */
	private void initializeVariables(OOSAlertInputDTO oosAlertInputDTO) throws GeneralException, ParseException {
		rootPath = PropertyManager.getProperty("DATALOAD.ROOTPATH");
		outputPath = PropertyManager.getProperty("OOS_ALERT_MAIL_ATTACH_PATH");
		templatePath = PropertyManager.getProperty("OOS_ALERT_TEMPLATE_PATH");
//		SimpleDateFormat dateFormat = new SimpleDateFormat(Constants.APP_DATE_FORMAT);
//		RetailCalendarDAO retailCalendarDAO = new RetailCalendarDAO();
		
		// Fill location Name
		if (inputLocationLevelId == Constants.STORE_LEVEL_ID) {
			StoreDAO storeDAO = new StoreDAO();
			StoreDTO store = storeDAO.getStoreInfo(conn, inputLocationId);
			inputLocationName = store.strNum;
		} else if (inputLocationLevelId == Constants.DISTRICT_LEVEL_ID) {
			OOSAnalysisDAO analysisDAO = new OOSAnalysisDAO();
			inputLocationName = analysisDAO.getDistrictName(conn, inputLocationId);
		}
		
//		HashMap<Integer, DayPartLookupDTO> dayPartLookupMap = new HashMap<Integer, DayPartLookupDTO>();
//
//		for (DayPartLookupDTO oosDayPartLookupDTO : dayPartLookup) {
//			dayPartLookupMap.put(oosDayPartLookupDTO.getOrder(), oosDayPartLookupDTO);
//		}
//
//		Set<Integer> orderSet = dayPartLookupMap.keySet();
//		int maxDayPartOrder = Collections.max(orderSet);
//		Date processingDate = null;
//		
//		processingDate = dateFormat.parse(processDateStr);
//		processingDate = DateUtil.incrementDate(processingDate, -1);
//		int prevCalendarId = retailCalendarDAO.getCalendarId(conn, dateFormat.format(processingDate), Constants.CALENDAR_DAY)
//				.getCalendarId();
//		
//		//Get previous two time slot and its calendar id
//		if (processingDayPartLookup.getOrder() > 2) {
//			prevDayPart1 = processingDayPartLookup.getOrder() - 1;
//			prevDayPart2 = processingDayPartLookup.getOrder() - 2;
//			prevDayPart1CalendarId = oosAlertInputDTO.getCalendarId();
//			prevDayPart2CalendarId = oosAlertInputDTO.getCalendarId();
//		} else if (processingDayPartLookup.getOrder() == 2) {
//			prevDayPart1 = processingDayPartLookup.getOrder() - 1;
//			prevDayPart1CalendarId = oosAlertInputDTO.getCalendarId();
//			prevDayPart2 = maxDayPartOrder;
//			prevDayPart2CalendarId = prevCalendarId;
//		} else if (processingDayPartLookup.getOrder() == 1) {
//			prevDayPart1 = maxDayPartOrder;
//			prevDayPart2 = maxDayPartOrder - 1;
//			prevDayPart1CalendarId = prevCalendarId;
//			prevDayPart2CalendarId = prevCalendarId;
//		}
	}
	
	/**
	 * Sets processing Date and time slot for each item
	 * 
	 * @param itemList
	 * @param processingDate
	 * @param dayPartLookupDTO
	 */
	private void setRequiredData(List<OOSItemDTO> itemList, String processingDate, DayPartLookupDTO dayPartLookupDTO) {
		for (OOSItemDTO oosItemDTO : itemList) {
			oosItemDTO.setProcessingDate(processingDate);
			oosItemDTO.setTimeSlot(dayPartLookupDTO.getStartTime() + " - " + dayPartLookupDTO.getEndTime());
		}
	}

	/**
	 * Assigns display type lookup
	 * 
	 * @param conn
	 */
	private void getDisplayTypeLookup(Connection conn) {
		DisplayTypeDAO displayTypeDAO = new DisplayTypeDAO(conn);
		HashMap<String, Integer> displayMap = displayTypeDAO.getDisplayTypes();
		// Swap key as value and value as key
		for (Map.Entry<String, Integer> entry : displayMap.entrySet()) {
			displayTypeLookup.put(entry.getValue(), entry.getKey());
		}
	}

	/**
	 * Gives DB connection
	 * 
	 * @return Connection
	 */
	private Connection getConnection() {
		Connection conn = null;
		try {
			conn = DBManager.getConnection();
		} catch (GeneralException ge) {
			logger.error("Unable to connect database", ge);
			System.exit(1);
		}
		return conn;
	}
	
	/**
	 * 
	 * @param items
	 * Eliminate lig members when an lig record is added.
	 * 
	 */
	
	private void eleminateLIGMember(List<OOSItemDTO> items){
		List<OOSItemDTO> ligList = new ArrayList<OOSItemDTO>();
		List<OOSItemDTO> itemsToBeRemoved = new ArrayList<OOSItemDTO>();
		for(OOSItemDTO oosItemDTO: items){
			if(oosItemDTO.getProductLevelId() == Constants.PRODUCT_LEVEL_ID_LIG){
				//Set lir Name as Item name for lig records..
				oosItemDTO.setItemName(oosItemDTO.getLirName());
				ligList.add(oosItemDTO);
			}
		}
		
		for(OOSItemDTO ligItem: ligList){
			int retLirId = ligItem.getRetLirId();
			for(OOSItemDTO oosItem: items){
				if(oosItem.getProductLevelId() == Constants.ITEMLEVELID){
					if(retLirId == oosItem.getRetLirId()){
						//Remove lig members from list if lig record of this item is added.
						setAddtionalInfoForLIG(ligItem, oosItem);
						itemsToBeRemoved.add(oosItem);
					}
				}
			}
		}
		
		for(OOSItemDTO oosItemDTO: itemsToBeRemoved){
			items.remove(oosItemDTO);
		}
	}
	
	private void setAddtionalInfoForLIG(OOSItemDTO ligItem, OOSItemDTO memberItem){
		ligItem.setCategoryName(memberItem.getCategoryName());
		ligItem.setDepartmentName(memberItem.getDepartmentName());
		ligItem.setRetailerItemCode(Constants.EMPTY);
	}
	
	
	private void editHeading(XSSFSheet sheet){
		XSSFRow row = sheet.getRow(0);
		XSSFCell cell = row.getCell(0);
	    String cellContents = cell.getStringCellValue(); 
	    //Modify the cellContents here
	     cellContents = cellContents + " for " + inputLocationName + " for the time slot "
					+ processDateStr + " " + timeSlot;
	    // Write the output to a file
	    cell.setCellValue(cellContents); 
		
	}
}
