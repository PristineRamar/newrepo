package com.pristine.service.offermgmt.oos;

import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.sql.Connection;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Comparator;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.TreeMap;
import org.apache.log4j.Logger;
import org.apache.log4j.PropertyConfigurator;
import org.apache.poi.ss.usermodel.IndexedColors;
import org.apache.poi.xssf.usermodel.XSSFCell;
import org.apache.poi.xssf.usermodel.XSSFCellStyle;
import org.apache.poi.xssf.usermodel.XSSFColor;
import org.apache.poi.xssf.usermodel.XSSFRow;
import org.apache.poi.xssf.usermodel.XSSFSheet;
import org.apache.poi.xssf.usermodel.XSSFWorkbook;
import com.pristine.dao.DBManager;
import com.pristine.dao.RetailCalendarDAO;
import com.pristine.dao.StoreDAO;
import com.pristine.dao.offermgmt.oos.OOSAlertDAO;
import com.pristine.dao.offermgmt.oos.OOSAnalysisDAO;
import com.pristine.dao.offermgmt.promotion.DisplayTypeDAO;
import com.pristine.dao.offermgmt.promotion.PromotionDAO;
import com.pristine.dto.AdKey;
import com.pristine.dto.RetailCalendarDTO;
import com.pristine.dto.StoreDTO;
import com.pristine.dto.offermgmt.oos.OOSEmailAlertDTO;
import com.pristine.dto.offermgmt.oos.OOSItemDTO;
import com.pristine.exception.GeneralException;
import com.pristine.service.email.EmailService;
import com.pristine.service.offermgmt.oos.OOSCandidateItemImpl;
import com.pristine.util.Constants;
import com.pristine.util.PristineDBUtil;
import com.pristine.util.PropertyManager;



/**
 * Sends weekly forcast movement for all items in a given location
 * @author Pradeep
 *
 */

public class OOSPredictionMailer {
	Connection conn = null;
	String outputPath = null;
	String processDateStr = null;
	String weekEndingDateStr = null;
	String inputLocationName = null;
	String rootPath = null;
	String templatePath = null;
	int firstRowIndex = 3;
	private static final String LOCATION_ID = "LOCATION_ID=";
	private static final String LOCATION_LEVEL_ID = "LOCATION_LEVEL_ID=";
	private static final String WEEK_END_DATE = "WEEK_END_DATE=";
	private static final String INTERNAL_REPORT = "INTERNAL_REPORT=";
	private static final String REPORT_TYPE = "REPORT_TYPE=";
	private static final String PAGE_BLOCK_LEVEL = "PAGE_BLOCK_LEVEL=";
	private static Logger  logger = Logger.getLogger("OOSPredictionMailer");
	int locationId;
	int locationLevelId;
	int weekCalendarId;
	private HashMap<Integer, String> displayTypeLookup = new HashMap<Integer, String>();
	private HashMap<Integer, String> promoTypeLookup = null;
	boolean isInternalReport = false;
	private List<Integer> storesToProcess = new ArrayList<Integer>();
	private boolean isChainLevelReport = false;
	private boolean isPageBlockLevelReport = false;
	
	public OOSPredictionMailer(){
		try{
			conn = DBManager.getConnection();
		}
		catch(GeneralException ge){
			logger.error("Unable to connect database", ge);
			System.exit(1);
		}
	}
	
	public static void main(String[] args) {
		PropertyConfigurator.configure("log4j-oos-prediction-mailer.properties");
		PropertyManager.initialize("recommendation.properties");
		String location = null;
		String locationLevel = null;
		String date = null;
		String reportType = null;
		OOSPredictionMailer weeklyPredictionMailer = new OOSPredictionMailer();
		for(String arg: args){
			if(arg.startsWith(LOCATION_ID))
				location = arg.substring(LOCATION_ID.length());
			else if(arg.startsWith(LOCATION_LEVEL_ID))
				locationLevel = arg.substring(LOCATION_LEVEL_ID.length());
			else if(arg.startsWith(WEEK_END_DATE))
				date = arg.substring(WEEK_END_DATE.length());
			else if(arg.startsWith(INTERNAL_REPORT)) 
			    weeklyPredictionMailer.isInternalReport = Boolean.parseBoolean(arg.substring(INTERNAL_REPORT.length()));
			else if(arg.startsWith(REPORT_TYPE))
				reportType = arg.substring(REPORT_TYPE.length());
			else if(arg.startsWith(PAGE_BLOCK_LEVEL))
				weeklyPredictionMailer.isPageBlockLevelReport = 
				Boolean.parseBoolean(arg.substring(PAGE_BLOCK_LEVEL.length()));
		}
		logger.info("**********************************************");
		weeklyPredictionMailer.processWeeklyAdMailing(location, locationLevel, date, reportType);
		logger.info("**********************************************");
	}
	
	/**
	 * Gets items, price, predicted movements, writes into a file and sends to mail lists
	 * @param location
	 * @param locationLevel
	 * @param date
	 */
	private void processWeeklyAdMailing(String location, String locationLevel, String date, String reportType){
		try{
			logger.info("Processing weekly predictions...");
			PromotionDAO promotionDAO = new PromotionDAO(conn);
			
			logger.info("Sending Weekly Prediction Alert for location" + locationLevel + "-" + location
					+ " for week start date: " + date + " is Started...");
			
			validateInputs(location, locationLevel, date, reportType);
			initializeVariables();
			
			//Preload display types
			getDisplayTypeLookup();
		
			//preload promo types
			promoTypeLookup = promotionDAO.getPromoTypeLookup();
			
			logger.info("Getting already predicted items...");
			if(isChainLevelReport){
				//1. Get Candidate items with chain level forecast
				processChainLevelForecast();
			}
			else{
				processStoreLevelForecast();
			}
		}
		catch(GeneralException e){
			e.printStackTrace();
			logger.error("Exception in processWeeklyAdMailing()" + e + e.getMessage());	
		} catch (CloneNotSupportedException e) {
			e.printStackTrace();
			logger.error("Exception in processWeeklyAdMailing()" + e + e.getMessage());	
		}
		finally{
			PristineDBUtil.close(conn);
		}
		
		logger.info("Sending Weekly Prediction Alert for location" + locationLevel + "-" + location
				+ " for week start date: " + date + " is Completed...");
	}
	
	/**
	 * Gets forecast for given stores and generates the report.
	 * @throws GeneralException
	 */
	private void processStoreLevelForecast() throws GeneralException {
		OOSAlertDAO oosAlertDAO = new OOSAlertDAO();
		OOSCandidateItemImpl oosCandidateItemImpl = new OOSCandidateItemImpl();
		// Get stores to process
		getStoresToProcess();

		List<OOSItemDTO> finalItemList = new ArrayList<OOSItemDTO>();

		// Loop each store
		for (Integer storeId : storesToProcess) {
			/*
			 * // Get candidate items List<PRItemDTO> candidateItems =
			 * oosCandidateItemImpl.getAuthorizedItemOfWeeklyAd(conn, 0, 0,
			 * Constants.STORE_LEVEL_ID, storeId, weekCalendarId, chainId); if
			 * (candidateItems.size() == 0) { logger.warn(
			 * "No items found for location - " + locationId); }
			 * 
			 * // Convert to oos items List<OOSItemDTO> finalList =
			 * convertToOOSItem(candidateItems);
			 * 
			 * // get predicted movement
			 * oosService.getPredictionsFromOOSForecast(conn,
			 * Constants.STORE_LEVEL_ID, storeId, weekCalendarId, finalList);
			 */
			List<OOSItemDTO> finalList = oosCandidateItemImpl.getOOSCandidateItems(conn, Constants.STORE_LEVEL_ID,
					storeId, weekCalendarId);
			// Put in final list
			finalItemList.addAll(finalList);
		}

		// Write data into file
		String outFile = writeExcelFile(finalItemList);

		logger.info("Getting mail list...");
		OOSEmailAlertDTO oosEmailAlertDTO = oosAlertDAO.getMailList(conn, locationId, locationLevelId,
				isInternalReport);
		if (oosEmailAlertDTO == null)
			throw new GeneralException("No mail list found for location - " + locationId);

		logger.info("Sending mail...");
		sendAlertThroughMail(oosEmailAlertDTO, outFile);
	}
	
	
	/**
	 * Gets forecast for each item at chain level and generates the report.
	 * @throws GeneralException
	 * @throws CloneNotSupportedException 
	 */
	private void processChainLevelForecast() throws GeneralException, CloneNotSupportedException{
		
		OOSAlertDAO oosAlertDAO = new OOSAlertDAO();
		OOSCandidateItemImpl oosCandidateItemImpl = new OOSCandidateItemImpl();
		OOSService oosService = new OOSService();
				
		int topsStoreListId = Integer.parseInt(PropertyManager.
				getProperty("TOPS_WITHOUT_GU_STORE_LIST_ID"));
		
		int guStoreListId = Integer.parseInt(PropertyManager.
				getProperty("TOPS_GU_STORE_LIST_ID"));
		
		String locationIds = topsStoreListId + ", " + guStoreListId;
		
		logger.info("Getting chain level forecast...");
		
		List<OOSItemDTO> itemList = oosCandidateItemImpl.getItemsWithChainLevelForecast(conn, 
				Constants.STORE_LIST_LEVEL_ID, locationIds, weekCalendarId, topsStoreListId, guStoreListId, isPageBlockLevelReport);
		logger.info("Getting chain level forecast is completed.");
		List<OOSItemDTO> finalList = oosService.processLIGAndNonLigData(itemList);
		//Ignore categories for which if there is an item with status code 3
		logger.info("Applying filter to elminate categories with ERROR status code...");
		finalList = oosService.filterItemsByPredictionStatus(finalList);
		logger.info(finalList.size() + " items filtered.");
		
		if(finalList.size() == 0){
			logger.info("No items found. Exiting...");
			return;
		}
		
		String outFile = null;
		if(isPageBlockLevelReport){
			//Group items by page and block
			HashMap<AdKey, OOSItemDTO> pageBlockMap = new HashMap<>();
			oosService.groupItemsByPageAndBlock(finalList, pageBlockMap);
			//Write data by page and block level
			TreeMap<AdKey, OOSItemDTO> sortedMap = new TreeMap<>(pageBlockMap);
			logger.info("Writing data into file...");
			outFile = writeExcelFile(sortedMap);
			logger.info("File " + outFile + " is created successfully.");
		}
		else{
			logger.info("Writing data into file...");
			outFile = writeExcelFile(finalList);
			logger.info("File " + outFile + " is created successfully.");
		}
		logger.info("Getting mail list...");
		OOSEmailAlertDTO oosEmailAlertDTO = oosAlertDAO.getMailList(conn, locationId, locationLevelId,
				isInternalReport);
		if (oosEmailAlertDTO == null)
			throw new GeneralException("No mail list found for location - " + locationId);
	
		sendAlertThroughMail(oosEmailAlertDTO, outFile);
	}
	

	/**
	 * Validates and prepares input variables
	 * 
	 * @param location
	 * @param locationLevel
	 * @param date
	 * @throws GeneralException
	 */
	private void validateInputs(String location, String locationLevel, String date, String reportType) throws GeneralException {
		RetailCalendarDAO retailCalendarDAO = new RetailCalendarDAO();
		if (locationLevel == null)
			locationLevelId = Constants.STORE_LEVEL_ID;
		else
			locationLevelId = Integer.parseInt(locationLevel);

		if (location == null) {
			throw new GeneralException("Unable to proceed without LOCATION_ID parameter");
		} else
			locationId = Integer.parseInt(location);

		if (date == null) {
			throw new GeneralException("Unable to proceed without WEEK_END_DATE parameter");
		} else {
			RetailCalendarDTO retailCalendarDTO = retailCalendarDAO.getCalendarId(conn, date, Constants.CALENDAR_WEEK);
			weekCalendarId = retailCalendarDTO.getCalendarId();
			weekEndingDateStr = retailCalendarDTO.getEndDate();
		}
		
		if(reportType == null){
			this.isChainLevelReport = false;
		}
		else{
			if(reportType.equals(Constants.CHAIN)){
				this.isChainLevelReport = true;
			}
		}
	}
	
	/**
	 * Sets rootpath, outputPath
	 * 
	 * @throws GeneralException
	 */
	private void initializeVariables() throws GeneralException {
		rootPath = PropertyManager.getProperty("DATALOAD.ROOTPATH");
		outputPath = PropertyManager.getProperty("WEEKLY_PRED_MAIL_ATTACH_PATH");
		if (isInternalReport)
			templatePath = PropertyManager.getProperty("WEEKLY_PRED_MAIL_TEMPLATE_PATH_INTERNAL");
		else
			templatePath = PropertyManager.getProperty("WEEKLY_PRED_MAIL_TEMPLATE_PATH");
		
		if(isChainLevelReport){
			templatePath = PropertyManager.getProperty("WEEKLY_PRED_MAIL_CHAIN_LEVEL_TEMPLATE_PATH");
			if(isInternalReport){
				templatePath = PropertyManager.getProperty("WEEKLY_PRED_MAIL_CHAIN_LEVEL_TEMPLATE_PATH_INTERNAL");
			}
		}

		// Fill location Name
		if (locationLevelId == Constants.STORE_LEVEL_ID) {
			StoreDAO storeDAO = new StoreDAO();
			StoreDTO store = storeDAO.getStoreInfo(conn, locationId);
			inputLocationName = store.strNum;
		} else if (locationLevelId == Constants.DISTRICT_LEVEL_ID) {
			OOSAnalysisDAO analysisDAO = new OOSAnalysisDAO();
			inputLocationName = analysisDAO.getDistrictName(conn, locationId);
		}
		else{
			inputLocationName = "TOPS";
		}
		
		if(isChainLevelReport){
			firstRowIndex = 4;
		}
	}
	
	/**
	 * Gets display type lookup
	 */
	private void getDisplayTypeLookup() {
		DisplayTypeDAO displayTypeDAO = new DisplayTypeDAO(conn);
		HashMap<String, Integer> displayMap = displayTypeDAO.getDisplayTypes();
		// Swap key as value and value as key
		for (Map.Entry<String, Integer> entry : displayMap.entrySet()) {
			displayTypeLookup.put(entry.getValue(), entry.getKey());
		}
	}
	
	private void getStoresToProcess() throws GeneralException {
		if (locationLevelId == Constants.STORE_LEVEL_ID) {
			storesToProcess.add(locationId);
		} else if (locationLevelId == Constants.DISTRICT_LEVEL_ID) {
			// Not all the stores in the district is processed, use OOS_CANDIDATE_ITEM
			// table to determined which store to consider for the district
			OOSAnalysisDAO analysisDAO = new OOSAnalysisDAO();
			storesToProcess = analysisDAO.getStoresInDistrictFromOOSForecastTable(conn, weekCalendarId, locationId);
		}
	}
	
	/**
	 * Writes excel file from the object.
	 * @param itemsInOOSAlert
	 * @return output file
	 * @throws GeneralException
	 */
	public String writeExcelFile(List<OOSItemDTO> itemsInOOSAlert) throws GeneralException {
		//inputLocationName.replaceAll(" ", "-")
		String outFile = outputPath + "/Weekly_Forecast_" + inputLocationName + "_WE_"
				+ weekEndingDateStr.replaceAll("/", "-") + ".xlsx";

		try {
			FileInputStream inputStream = new FileInputStream(templatePath);
			XSSFWorkbook workbook = new XSSFWorkbook(inputStream);
			XSSFSheet sheet = workbook.getSheet("Prediction");
			int rowNum = firstRowIndex;

			sortAlertItems(itemsInOOSAlert);
			for (OOSItemDTO oosItemDTO : itemsInOOSAlert) {
				XSSFRow row = sheet.getRow(rowNum);
				if(row == null){
					row = sheet.createRow(rowNum);
				}
				rowNum++;
				writeToCell(oosItemDTO, row, sheet, null);
			}

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
	
	
	/**
	 * Writes excel file from the object.
	 * @param pageBlockMap
	 * @return output file
	 * @throws GeneralException
	 */
	public String writeExcelFile(TreeMap<AdKey, OOSItemDTO> pageBlockMap) throws GeneralException {
		//inputLocationName.replaceAll(" ", "-")
		String outFile = outputPath + "/Weekly_Forecast_" + inputLocationName + "_WE_"
				+ weekEndingDateStr.replaceAll("/", "-") + ".xlsx";
		try {
			FileInputStream inputStream = new FileInputStream(templatePath);
			XSSFWorkbook workbook = new XSSFWorkbook(inputStream);
			XSSFSheet sheet = workbook.getSheet("Prediction");
			int rowNum = firstRowIndex;
			int rowCount = 0;
			XSSFCellStyle cellStyle = workbook.createCellStyle();
			XSSFColor darkYellow =new XSSFColor(new java.awt.Color(245,222,95));
			XSSFColor lightYellow =new XSSFColor(new java.awt.Color(250,236,160));
			List<OOSItemDTO> lastRowList = new ArrayList<>();
			for(Map.Entry<AdKey, OOSItemDTO> entry: pageBlockMap.entrySet()){
				int itemCount = 0;
				if(entry.getKey().getPageNumber() < 0 && entry.getKey().getBlockNumber() < 0){
					lastRowList.add(entry.getValue());
					continue;
				}
				rowCount++;
				if(rowCount % 2 == 0){
					cellStyle = (XSSFCellStyle) cellStyle.clone();
					cellStyle.setFillForegroundColor(lightYellow);
					cellStyle.setFillPattern(XSSFCellStyle.SOLID_FOREGROUND);
				}
				else{
					cellStyle = (XSSFCellStyle) cellStyle.clone();
					cellStyle.setFillForegroundColor(darkYellow);
					cellStyle.setFillPattern(XSSFCellStyle.SOLID_FOREGROUND);
				}
				OOSItemDTO oosItemDTO = entry.getValue();
//				for (OOSItemDTO oosItemDTO : entry.getValue()) {
					itemCount++;
					//Clear Presto total chain forecast, Client forecast and actual movement
					//if a page and block combination has more than one item. 
					if(itemCount > 1){
						oosItemDTO.setWeeklyActualMovement(0l);
						oosItemDTO.setClientWeeklyPredictedMovement(0l);
						oosItemDTO.setWeeklyPredictedMovement(0l);
					}
					XSSFRow row = sheet.getRow(rowNum);
					if(row == null){
						row = sheet.createRow(rowNum);
					}
					rowNum++;
					writeToCell(oosItemDTO, row, sheet, cellStyle);
//				}
			}
			rowCount++;
			if(rowCount % 2 == 0){
				cellStyle = (XSSFCellStyle) cellStyle.clone();
				cellStyle.setFillForegroundColor(lightYellow);
				cellStyle.setFillPattern(XSSFCellStyle.SOLID_FOREGROUND);
			}
			else{
				cellStyle = (XSSFCellStyle) cellStyle.clone();
				cellStyle.setFillForegroundColor(darkYellow);
				cellStyle.setFillPattern(XSSFCellStyle.SOLID_FOREGROUND);
			}
			for(OOSItemDTO oosItemDTO: lastRowList){
				XSSFRow row = sheet.getRow(rowNum);
				if(row == null){
					row = sheet.createRow(rowNum);
				}
				rowNum++;
				writeToCell(oosItemDTO, row, sheet, cellStyle);
			}
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

	/**
	 * Sorts by category name and item name
	 * @param itemsInOOSAlert
	 */
	private void sortAlertItems(List<OOSItemDTO> itemsInOOSAlert){
		Collections.sort(itemsInOOSAlert, new Comparator<OOSItemDTO>() {
			@Override
			public int compare(final OOSItemDTO oosItemDTO1, final OOSItemDTO oosItemDTO2) {
				int c;
				if(isChainLevelReport){
						c = oosItemDTO1.getCategoryName().compareTo(oosItemDTO2.getCategoryName());
						if (c == 0)
							c = oosItemDTO1.getItemName().compareTo(oosItemDTO2.getItemName());
				}
				else{
					c = oosItemDTO1.getDistrictName().compareTo(oosItemDTO2.getDistrictName());
					if (c == 0)
						c = oosItemDTO1.getStoreNo().compareTo(oosItemDTO2.getStoreNo());
					if (c == 0)
						c = oosItemDTO1.getCategoryName().compareTo(oosItemDTO2.getCategoryName());
				}
				
				return c;
			}
		});		
	}
	
	/**
	 * creates cells and applies values
	 * @param oosItemDTO
	 * @param row
	 */
	private void writeToCell(OOSItemDTO oosItemDTO, XSSFRow row, XSSFSheet sheet, XSSFCellStyle style){
		Object[] objectArr = null;
		if(isChainLevelReport){
			writeDataToCell(oosItemDTO, row, sheet, style);
		}
		else{
				objectArr = getArrayOfObject(oosItemDTO);	
				int cellNum = 0;
				for(Object value: objectArr){
					XSSFCell cell = row.createCell(cellNum++);
					if(value instanceof String){
						cell.setCellValue(String.valueOf(value));
					}
					else if(value instanceof Integer){
						cell.setCellValue((Integer) value);
					}
					else if(value instanceof Long){
						cell.setCellValue((Long) value);
					}
				}
		}
	}
	
	/**
	 * gives array of object
	 * @param oosItemDTO
	 * @return Object[]
	 */
	private Object[] getArrayOfObject(OOSItemDTO oosItemDTO) {
		// logger.debug("getArrayOfObject(): " + oosItemDTO.toString());
		Object[] object = new Object[] { 
				//District Name
				oosItemDTO.getDistrictName(),
				// Store #
				oosItemDTO.getStoreNo(), 
				// Retailer item code
				oosItemDTO.getRetailerItemCode(), 
				// Item name
				oosItemDTO.getItemName(), 
				// Category Name
				oosItemDTO.getCategoryName(), 
				// Week End Date
				weekEndingDateStr, 
				// Reg Retail
				oosItemDTO.getRegularPriceString(), 
				// Sale Retail
				oosItemDTO.getSalePriceString(), 
				// Promo Type
				promoTypeLookup.get(oosItemDTO.getPromoTypeId()) == null ? Constants.EMPTY
						: promoTypeLookup.get(oosItemDTO.getPromoTypeId()), 
				// Page No
				(oosItemDTO.getAdPageNo() > 0 ? oosItemDTO.getAdPageNo() : Constants.EMPTY),
				// Block No
				(oosItemDTO.getBlockNo() > 0 ? oosItemDTO.getBlockNo() : Constants.EMPTY),
				// Display
				displayTypeLookup.get(oosItemDTO.getDisplayTypeId()) == null ? Constants.EMPTY
						: displayTypeLookup.get(oosItemDTO.getDisplayTypeId()), 
				// Presto Weekly Predicted Movement
				(oosItemDTO.getWeeklyPredictedMovement() > 0 ? oosItemDTO.getWeeklyPredictedMovement() : 0),
				// Presto Confidence Range
				oosItemDTO.getWeeklyConfidenceLevelRange(), 
				// Client Weekly Predicted Movement
				(oosItemDTO.getClientWeeklyPredictedMovement() > 0 ? oosItemDTO.getClientWeeklyPredictedMovement() : 0),
				// Actual weekly movement
				(isInternalReport ? oosItemDTO.getWeeklyActualMovement(): "")
		};
		return object;
	}
	
	
	/**
	 *	Writes into cell
	 * @param oosItemDTO
	 * @return Object[]
	 */
	private void writeDataToCell(OOSItemDTO oosItemDTO, XSSFRow row, XSSFSheet sheet, XSSFCellStyle style) {
		// logger.debug("getArrayOfObject(): " + oosItemDTO.toString());
			int colCount = -1;
				// Retailer item code
				setCellValue(row, ++colCount, (oosItemDTO.getRetLirId() == 0 ? oosItemDTO.getRetailerItemCode() : Constants.EMPTY), sheet, style);
				// Item name
				setCellValue(row, ++colCount, oosItemDTO.getItemName(), sheet, style);
				// Category Name
				setCellValue(row, ++colCount, oosItemDTO.getCategoryName(), sheet, style);
				// Week End Date
				setCellValue(row, ++colCount, weekEndingDateStr, sheet, style);
				// Reg Retail
				if(oosItemDTO.getRegularPriceString().contains("/")){
					setCellValue(row, ++colCount, 
						oosItemDTO.getRegularPriceString(), sheet, style);
				}
				else{
					setCellValue(row, ++colCount, 
							Double.parseDouble(oosItemDTO.getRegularPriceString()), sheet, style);
				}
				// Sale Retail
				if(oosItemDTO.getSalePriceString().contains("/")){
					setCellValue(row, ++colCount, oosItemDTO.getSalePriceString(), sheet, style);
				}
				else{
					setCellValue(row, ++colCount, Double.parseDouble(oosItemDTO.getSalePriceString()), sheet, style);
				}
				// Promo Type
				setCellValue(row, ++colCount, promoTypeLookup.get(oosItemDTO.getPromoTypeId()) == null ? Constants.EMPTY
						: promoTypeLookup.get(oosItemDTO.getPromoTypeId()), sheet, style);
				// Page No
				if(oosItemDTO.getAdPageNo() > 0){
					setCellValue(row, ++colCount, oosItemDTO.getAdPageNo(), sheet, style);
				}
				else{
					setCellValue(row, ++colCount,Constants.EMPTY, sheet, style);
				}
				// Block No
				if(oosItemDTO.getBlockNo() > 0){
					setCellValue(row, ++colCount, oosItemDTO.getBlockNo(), sheet, style);
				}
				else{
					setCellValue(row, ++colCount,Constants.EMPTY, sheet, style);
				}

				/*// GU Page No
				if(oosItemDTO.getAdPageNoGU() > 0){
					setCellValue(row, ++colCount, oosItemDTO.getAdPageNoGU(), sheet, style);
				}
				else{
					setCellValue(row, ++colCount,Constants.EMPTY, sheet, style);
				}
				// GU Block No
				if(oosItemDTO.getBlockNoGU() > 0){
					setCellValue(row, ++colCount, oosItemDTO.getBlockNoGU(), sheet, style);
				}
				else{
					setCellValue(row, ++colCount,Constants.EMPTY, sheet, style);
				}
				*/
				// Display
				setCellValue(row, ++colCount, displayTypeLookup.get(oosItemDTO.getDisplayTypeId()) == null ? Constants.EMPTY
						: displayTypeLookup.get(oosItemDTO.getDisplayTypeId()), sheet, style);
				/*// Tops Corp Forecast
				if(oosItemDTO.getWeeklyPredictedMovementTops() > 0){
					setCellValue(row, ++colCount, 
							oosItemDTO.getWeeklyPredictedMovementTops(), sheet, style);
				}
				else{
					setCellValue(row, ++colCount, 
							Constants.EMPTY, sheet, style);
				}
				// GU Forecast
				if(oosItemDTO.getWeeklyPredictedMovementGU() > 0){
					setCellValue(row, ++colCount, 
							oosItemDTO.getWeeklyPredictedMovementGU(), sheet, style);
				}
				else{
					setCellValue(row, ++colCount, 
							Constants.EMPTY, sheet, style);
				}*/
				//Total chain
				if(oosItemDTO.getWeeklyPredictedMovement() > 0){
					setCellValue(row, ++colCount, 
							oosItemDTO.getWeeklyPredictedMovement(), sheet, style);
				}
				else{
					setCellValue(row, ++colCount, 
							Constants.EMPTY, sheet, style);
				}
				// Client Weekly Predicted Movement
				if(oosItemDTO.getClientWeeklyPredictedMovement() > 0){
					setCellValue(row, ++colCount, 
							oosItemDTO.getClientWeeklyPredictedMovement(), sheet, style);
				}
				else{
					setCellValue(row, ++colCount, 
							Constants.EMPTY, sheet, style);
				}
				
				if(isInternalReport){
					// Actual movement
					if(oosItemDTO.getWeeklyActualMovement() > 0){
						setCellValue(row, ++colCount, 
								oosItemDTO.getWeeklyActualMovement(), sheet, style);
					}
					else{
						setCellValue(row, ++colCount, 
								Constants.EMPTY, sheet, style);
					}	
				}
		}
	
	/**
	 * calls mail service and sends mail with produced excel report
	 * @param oosEmailAlertDTO
	 * @param file
	 * @return mail status
	 * @throws GeneralException
	 */
	private boolean sendAlertThroughMail(OOSEmailAlertDTO oosEmailAlertDTO, String file) throws GeneralException {
		boolean mailStatus = false;
		String from = PropertyManager.getProperty("MAIL.FROMADDR", "");
		if (from.isEmpty())
			throw new GeneralException("Unable to send mail without MAIL.FROMADDR property in analysis.properties");
		String foreCastType = "Forecast";
		if(isChainLevelReport){
			foreCastType = "Chain Level Forecast";
		}
		String subject = "Weekly " + foreCastType + " for " + inputLocationName + " for WE " + weekEndingDateStr;
		List<String> fileList = new ArrayList<String>();
		fileList.add(file);
		String mailContent = "Attached file has Weekly " + foreCastType + " for " + inputLocationName + " for week ending "
				+ weekEndingDateStr + " \n \n" + Constants.MAIL_CONFIDENTIAL_NOTE;
		mailStatus = EmailService.sendEmail(from, oosEmailAlertDTO.getMailToList(), oosEmailAlertDTO.getMailCCList(),
				oosEmailAlertDTO.getMailBCCList(), subject, mailContent, fileList);
		return mailStatus;
	}
	
	/**
	 * Sets String value to the cell
	 * @param row
	 * @param colCount
	 * @param value
	 * @param sheet
	 * @param cellStyle
	 */
	private void setCellValue(XSSFRow row, int colCount, String value, XSSFSheet sheet, XSSFCellStyle cellStyle) {
		XSSFCell cell;
		cell = row.getCell(colCount);
		if (cell == null)    
		    cell = row.createCell(colCount);
		cell.setCellValue(value);	
		if(cellStyle != null){
			short format = sheet.getColumnStyle(colCount).getDataFormat();
			short align = sheet.getColumnStyle(colCount).getAlignment();
			cellStyle = (XSSFCellStyle) cellStyle.clone();
			cellStyle.setBorderBottom(XSSFCellStyle.BORDER_THIN);
			cellStyle.setBorderTop(XSSFCellStyle.BORDER_THIN);
			cellStyle.setBorderLeft(XSSFCellStyle.BORDER_THIN);
			cellStyle.setBorderRight(XSSFCellStyle.BORDER_THIN);
			cellStyle.setBottomBorderColor(IndexedColors.BLUE_GREY.index);
			cellStyle.setTopBorderColor(IndexedColors.BLUE_GREY.index);
			cellStyle.setLeftBorderColor(IndexedColors.BLUE_GREY.index);
			cellStyle.setRightBorderColor(IndexedColors.BLUE_GREY.index);
			//cellStyle.setDataFormat(format);
			cellStyle.setDataFormat(format);
			cellStyle.setAlignment(align);
			cell.setCellStyle(cellStyle);
			
		}else{
			cell.setCellStyle(sheet.getColumnStyle(colCount));
		}
	}
	
	/**
	 * Sets double value to the cell
	 * @param row
	 * @param colCount
	 * @param value
	 * @param sheet
	 * @param cellStyle
	 */
	private void setCellValue(XSSFRow row, int colCount, double value,  XSSFSheet sheet, XSSFCellStyle cellStyle) {
		XSSFCell cell;
		cell = row.getCell(colCount);
		if (cell == null)    
		    cell = row.createCell(colCount);
		cell.setCellValue(value);	
		if(cellStyle != null){
			short format = sheet.getColumnStyle(colCount).getDataFormat();
			short align = sheet.getColumnStyle(colCount).getAlignment();
			cellStyle = (XSSFCellStyle) cellStyle.clone();
			cellStyle.setBorderBottom(XSSFCellStyle.BORDER_THIN);
			cellStyle.setBorderTop(XSSFCellStyle.BORDER_THIN);
			cellStyle.setBorderLeft(XSSFCellStyle.BORDER_THIN);
			cellStyle.setBorderRight(XSSFCellStyle.BORDER_THIN);
			cellStyle.setBottomBorderColor(IndexedColors.BLUE_GREY.index);
			cellStyle.setTopBorderColor(IndexedColors.BLUE_GREY.index);
			cellStyle.setLeftBorderColor(IndexedColors.BLUE_GREY.index);
			cellStyle.setRightBorderColor(IndexedColors.BLUE_GREY.index);
			//cellStyle.setDataFormat(format);
			cellStyle.setDataFormat(format);
			cellStyle.setAlignment(align);
			cell.setCellStyle(cellStyle);
		}else{
			cell.setCellStyle(sheet.getColumnStyle(colCount));
		}
	}
}
