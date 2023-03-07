package com.pristine.service.offermgmt.oos;

import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.io.IOException;
import java.lang.reflect.Field;
import java.sql.Connection;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Calendar;
import java.util.Collections;
import java.util.Comparator;
import java.util.Date;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.TreeMap;
import org.apache.log4j.Logger;
import org.apache.log4j.PropertyConfigurator;
//import org.apache.poi.hssf.usermodel.HSSFFont;
import org.apache.poi.xssf.usermodel.XSSFCell;
import org.apache.poi.xssf.usermodel.XSSFCellStyle;
import org.apache.poi.xssf.usermodel.XSSFColor;
import org.apache.poi.xssf.usermodel.XSSFFont;
import org.apache.poi.xssf.usermodel.XSSFRow;
import org.apache.poi.xssf.usermodel.XSSFSheet;
import org.apache.poi.xssf.usermodel.XSSFWorkbook;
import org.rosuda.JRI.RFactor;
import org.rosuda.JRI.Rengine;
import com.pristine.dao.DBManager;
import com.pristine.dao.RetailCalendarDAO;
import com.pristine.dao.RetailPriceDAO;
import com.pristine.dao.offermgmt.oos.OOSAlertDAO;
import com.pristine.dao.offermgmt.prediction.PredictionAccuracyReportDAO;
import com.pristine.dao.offermgmt.weeklyad.WeeklyAdDAO;
import com.pristine.dto.AdKey;
//import com.pristine.dto.CompetitiveDataDTO;
import com.pristine.dto.RetailCalendarDTO;
//import com.pristine.dto.offermgmt.PRItemDTO;
//import com.pristine.dto.offermgmt.ProductKey;
import com.pristine.dto.offermgmt.oos.OOSEmailAlertDTO;
import com.pristine.dto.offermgmt.oos.OOSItemDTO;
import com.pristine.dto.offermgmt.prediction.AccuracyComparisonDetailDTO;
import com.pristine.dto.offermgmt.prediction.AccuracyComparisonHeaderDTO;
import com.pristine.dto.offermgmt.prediction.PredictionAccuracyReportDTO;
import com.pristine.dto.offermgmt.weeklyad.WeeklyAdBlock;
import com.pristine.exception.GeneralException;
//import com.pristine.exception.OfferManagementException;
import com.pristine.service.email.EmailService;
//import com.pristine.service.offermgmt.RecommendationErrorCode;
import com.pristine.service.offermgmt.prediction.PredictionAccuracyMetrics;
import com.pristine.service.offermgmt.prediction.PredictionException;
import com.pristine.service.offermgmt.prediction.PredictionStatus;
import com.pristine.util.Constants;
import com.pristine.util.DateUtil;
import com.pristine.util.PrestoUtil;
import com.pristine.util.PristineDBUtil;
import com.pristine.util.PropertyManager;

/**
 * Generates accuracy report for chain level prediction
 * 
 * @author Pradeepkumar
 *
 */

public class PredictionAccuracyReport {
	Connection conn = null;
	String outputPath = null;
	String inputPath = null;
	String processDateStr = null;
	String weekEndingDateStr = null;
	String inputLocationName = null;
	String rootPath = null;
	String templatePath = null;
	int firstRowIndex = 3;
	int TOP_INACCURATE_FORECAST = 3;
	private static final int REPORT_TYPE_ID = 1;
	int subReportTypeId = 0;
	int inaccurateItemsRow = 43;
	int inaccurateItemMaxCount = 0;
	private static final String LOCATION_ID = "LOCATION_ID=";
	private static final String LOCATION_LEVEL_ID = "LOCATION_LEVEL_ID=";
	private static final String WEEK_END_DATE = "WEEK_END_DATE=";
	private static final String INTERNAL_REPORT = "INTERNAL_REPORT=";
	private static final String INPUT_FILE = "INPUT_FILE=";
	private static final String PAGE_BLOCK_LEVEL = "PAGE_BLOCK_LEVEL=";
	private static final String VALUE_TYPE_PRESTO = "PRESTO";
	private static final String VALUE_TYPE_CLIENT = "CLIENT";
	private static final String WEEK_TYPE = "WEEK_TYPE=";
	private static Logger logger = Logger.getLogger("PredictionAccuracyReport");
	int locationId;
	int locationLevelId;
	int weekCalendarId;
	int chainId;
	String weekType = null;
	WeeklyAdDAO weeklyAdDAO;
	HashMap<AdKey, List<String>> actualItemsMap;
	HashMap<AdKey, List<String>> adplexItemsMap;
	
	boolean isInternalReport = false;
	boolean isPageBlockLevelReport = false;

	public PredictionAccuracyReport() {
		try {
			conn = DBManager.getConnection();
		} catch (GeneralException ge) {
			logger.error("Unable to connect database", ge);
			System.exit(1);
		}
	}

	public static void main(String[] args) {
		PropertyConfigurator.configure("log4j-oos-prediction-accuracy-mailer.properties");
		PropertyManager.initialize("recommendation.properties");
		String location = null;
		String locationLevel = null;
		String date = null;
		String inputFile = null;
		PredictionAccuracyReport predictionAccuracyReport = new PredictionAccuracyReport();
		for (String arg : args) {
			if (arg.startsWith(LOCATION_ID))
				location = arg.substring(LOCATION_ID.length());
			else if (arg.startsWith(LOCATION_LEVEL_ID))
				locationLevel = arg.substring(LOCATION_LEVEL_ID.length());
			else if (arg.startsWith(WEEK_END_DATE))
				date = arg.substring(WEEK_END_DATE.length());
			else if (arg.startsWith(INTERNAL_REPORT))
				predictionAccuracyReport.isInternalReport = Boolean.parseBoolean(arg.substring(INTERNAL_REPORT.length()));
			else if (arg.startsWith(INPUT_FILE))
				inputFile = arg.substring(INPUT_FILE.length());
			else if (arg.startsWith(PAGE_BLOCK_LEVEL))
				predictionAccuracyReport.isPageBlockLevelReport = Boolean.parseBoolean(arg.substring(PAGE_BLOCK_LEVEL.length()));
			else if (arg.startsWith(WEEK_TYPE))
				predictionAccuracyReport.weekType = arg.substring(WEEK_TYPE.length());

		}
		logger.info("**********************************************");
		predictionAccuracyReport.processAccuracyReport(location, locationLevel, date, inputFile);
		logger.info("**********************************************");
	}

	private void processAccuracyReport(String location, String locationLevel, String date, String inputFile) {
		logger.info(
				"Sending Weekly Prediction Alert for location" + locationLevel + "-" + location + " for week start date: " + date + " is Started...");
		PredictionAccuracyReportDAO predictionAccuracyReportDAO = new PredictionAccuracyReportDAO();
		try {
			RetailPriceDAO retailPriceDAO = new RetailPriceDAO();
			OOSAlertDAO oosAlertDAO = new OOSAlertDAO();
			OOSService oosService = new OOSService();
			weeklyAdDAO = new WeeklyAdDAO(conn);
			chainId = Integer.parseInt(retailPriceDAO.getChainId(conn));
			HashMap<AdKey, OOSItemDTO> pageBlockMap = new HashMap<>();
			HashMap<AdKey, WeeklyAdBlock> adBlocks = new HashMap<AdKey, WeeklyAdBlock>();
			
			// initialize variables
			initialize(location, locationLevel, date);

			// Delete any files from the R input folder
			ArrayList<String> csvFiles = PrestoUtil.getAllFilesInADirectory(inputPath, "csv");
			PrestoUtil.deleteFiles(csvFiles);

			String inFile = null;
			List<OOSItemDTO> itemList = new ArrayList<OOSItemDTO>();
			if (inputFile == null) {
				// Get already predicted chain level forecast
				itemList = getForecast();
				// List<OOSItemDTO> itemList = new ArrayList<OOSItemDTO>();
				if (itemList.size() == 0) {
					logger.info("No items found. Exiting...");
					return;
				}

				// Write csv file to call R service
				if (isPageBlockLevelReport) {
//					HashMap<AdKey, List<OOSItemDTO>> pageBlockMap = new HashMap<>();
					
					logger.debug("itemList.size():" + itemList.size());
					oosService.groupItemsByPageAndBlock(itemList, pageBlockMap);
					// 18th Jun 2016
					// Get actual total no of items in each page and block
					adBlocks = weeklyAdDAO.getAllBlocks(locationLevelId, locationId, weekCalendarId);
					inFile = writeCSVFile(pageBlockMap, adBlocks);
				} else {
					inFile = writeCSVFile(itemList);
				}
			} else {
				inFile = inputPath + "/" + inputFile;
			}

			// Call accuracy R service...
			PredictionAccuracyReportDTO predictionAccuracyReportDTO = new PredictionAccuracyReportDTO();
			predictionAccuracyReportDTO = executeAccuracyReport(inFile);

			// Get other details
			predictionAccuracyReportDAO.getAccuracySummary(predictionAccuracyReportDTO, conn, weekCalendarId, chainId, locationLevelId, locationId);

			// Save to database
			saveToDatabase(predictionAccuracyReportDTO);

			logger.debug("Accuracy Report Commitis strated...");
			PristineDBUtil.commitTransaction(conn, "Accuracy Report Commit");
			logger.debug("Accuracy Report Commitis completed...");

			// if (!isInternalReport) {
			// Write to excel
			logger.debug("Writing output excel is strated...");
			String outFile = writeExcelFile(predictionAccuracyReportDTO, itemList, pageBlockMap, adBlocks);
			logger.debug("Writing output excel is completed...");

			// Send mail
				OOSEmailAlertDTO oosEmailAlertDTO = oosAlertDAO.getMailList(conn, locationId, locationLevelId, isInternalReport);

			if (oosEmailAlertDTO == null)
				throw new GeneralException("No mail list found for location - " + locationId);

			sendAlertThroughMail(oosEmailAlertDTO, outFile);
			// }
		} catch (GeneralException | Exception e) {
			e.printStackTrace();
			logger.error("Error -- processAccuracyReport() - " + e.toString(), e);
			PristineDBUtil.rollbackTransaction(conn, "Exception in processAccuracyReport()");
		} finally {
			PristineDBUtil.close(conn);
		}
	}

	/**
	 * 
	 * @return list of items with Chain level forecast
	 * @throws CloneNotSupportedException
	 * @throws GeneralException
	 */
	private List<OOSItemDTO> getForecast() throws CloneNotSupportedException, GeneralException {
		PredictionAccuracyReportDAO predAccuracyReportDAO = new PredictionAccuracyReportDAO();
		OOSService oosService = new OOSService();

//		int topsStoreListId = Integer.parseInt(PropertyManager.getProperty("TOPS_WITHOUT_GU_STORE_LIST_ID"));
//		int guStoreListId = Integer.parseInt(PropertyManager.getProperty("TOPS_GU_STORE_LIST_ID"));

		// String locationIds = topsStoreListId + ", " + guStoreListId;
		// String locationIds = String.valueOf(topsStoreListId);

		logger.info("Getting chain level forecast...");

		List<OOSItemDTO> itemList = predAccuracyReportDAO.getItemsWithForecast(conn, locationLevelId, locationId, weekCalendarId);

		logger.info("itemList.size()-0:" + itemList.size());

		List<OOSItemDTO> filteredList = null;
		if (!isPageBlockLevelReport) {
			filteredList = new ArrayList<OOSItemDTO>();
			for (OOSItemDTO itemDTO : itemList) {
				if (itemDTO.getNoOfLigOrNonLig() == 1) {
					filteredList.add(itemDTO);
				}
			}
		} else {
			filteredList = itemList;
		}

		List<OOSItemDTO> finalList = oosService.processLIGAndNonLigData(filteredList);

		// Ignore categories for which if there is an item with status code 3
		/// logger.info("Applying filter to eliminate categories with ERROR
		// status code...");
		// finalList = oosService.filterItemsByPredictionStatus(finalList);

		logger.info("Retreived " + finalList.size() + " items with chain forecast.");

		return finalList;
	}

	private void initialize(String location, String locationLevel, String date) throws GeneralException {
		validateInputs(location, locationLevel, date);
		initializeVariables();
	}

	/**
	 * Sets rootpath, outputPath
	 * 
	 * @throws GeneralException
	 */
	private void initializeVariables() throws GeneralException {
		rootPath = PropertyManager.getProperty("DATALOAD.ROOTPATH");
		inputPath = PropertyManager.getProperty("WEEKLY_CHAIN_PRED_ACCURACY_INPUT");
		outputPath = PropertyManager.getProperty("WEEKLY_CHAIN_PRED_ACCURACY_OUTPUT");
		if (isPageBlockLevelReport) {
			templatePath = PropertyManager.getProperty("WEEKLY_CHAIN_PRED_ACCURACY_PB_TEMPLATE");
			subReportTypeId = 2;
		} else {
			templatePath = PropertyManager.getProperty("WEEKLY_CHAIN_PRED_ACCURACY_ITEM_TEMPLATE");
			subReportTypeId = 1;
		}
		inputLocationName = "TOPS";
	}

	/**
	 * Validates and prepares input variables
	 * 
	 * @param location
	 * @param locationLevel
	 * @param date
	 * @throws GeneralException
	 */
	private void validateInputs(String location, String locationLevel, String date) throws GeneralException {
		RetailCalendarDAO retailCalendarDAO = new RetailCalendarDAO();
		SimpleDateFormat dateFormat = new SimpleDateFormat(Constants.APP_DATE_FORMAT);

		if (locationLevel == null)
			locationLevelId = Constants.STORE_LEVEL_ID;
		else
			locationLevelId = Integer.parseInt(locationLevel);

		if (location == null) {
			throw new GeneralException("Unable to proceed without LOCATION_ID parameter");
		} else
			locationId = Integer.parseInt(location);

		RetailCalendarDTO retailCalendarDTO = null;
		if (date != null) {
			retailCalendarDTO = retailCalendarDAO.getCalendarId(conn, date, Constants.CALENDAR_WEEK);
			weekCalendarId = retailCalendarDTO.getCalendarId();
			weekEndingDateStr = retailCalendarDTO.getEndDate();
		} else if (weekType.equalsIgnoreCase(Constants.CURRENT_WEEK)) {
			String currDateStr = dateFormat.format(new Date());
			retailCalendarDTO = retailCalendarDAO.getCalendarId(conn, currDateStr, Constants.CALENDAR_WEEK);
		} else if (weekType.equalsIgnoreCase(Constants.LAST_WEEK)) {
			Calendar c = Calendar.getInstance();
			c.set(Calendar.DAY_OF_WEEK, Calendar.SUNDAY);
			c.add(Calendar.DATE, -7);
			String dateStr = dateFormat.format(c.getTime());
			retailCalendarDTO = retailCalendarDAO.getCalendarId(conn, dateStr, Constants.CALENDAR_WEEK);
		}

		weekCalendarId = retailCalendarDTO.getCalendarId();
		weekEndingDateStr = retailCalendarDTO.getEndDate();
	}

	/**
	 * Generates CSV input file.
	 * 
	 * @param itemList
	 * @return
	 * @throws IOException
	 */

	private String writeCSVFile(List<OOSItemDTO> itemList) throws IOException {
		String outFile = inputPath + "/Accuracy_Input_WE_" + weekEndingDateStr.replaceAll("/", "-") + ".csv";
		String fileName = "Accuracy_Input_WE_" + weekEndingDateStr.replaceAll("/", "-") + ".csv";
		logger.info("Writing input file...");
		StringBuilder sb = new StringBuilder();
		writeHeader(sb);
		for (OOSItemDTO oosItemDTO : itemList) {
			writeToCell(oosItemDTO, sb);
		}
		FileOutputStream outputStream = new FileOutputStream(outFile);
		outputStream.write(sb.toString().getBytes());
		outputStream.close();
		logger.info("File " + outFile + " has been created successfully.");
		return fileName;
	}

	/**
	 * Generates CSV input file.
	 * 
	 * @param itemList
	 * @return
	 * @throws IOException
	 * @throws GeneralException
	 */

	private String writeCSVFile(HashMap<AdKey, OOSItemDTO> pageBlockMap, HashMap<AdKey, WeeklyAdBlock> adBlocks)
			throws IOException, GeneralException {
		String outFile = inputPath + "/Accuracy_Input_WE_" + weekEndingDateStr.replaceAll("/", "-") + ".csv";
		String fileName = "Accuracy_Input_WE_" + weekEndingDateStr.replaceAll("/", "-") + ".csv";
		logger.info("Writing input file...");
		StringBuilder sb = new StringBuilder();
		actualItemsMap = weeklyAdDAO.getActualItems(locationLevelId, locationId, weekCalendarId);
		adplexItemsMap = weeklyAdDAO.getAdplexItems(locationLevelId, locationId, weekCalendarId);
		writeHeaderPageBlock(sb);
		int ignoredBlocks = 0, matchingBlocks = 0;
		logger.debug("pageBlockMap.entrySet():" + pageBlockMap.size());
		for (Map.Entry<AdKey, OOSItemDTO> entry : pageBlockMap.entrySet()) {
			int counter = 0;
			boolean ignoreBlock = true;
			OOSItemDTO oosItemDTO = entry.getValue();
			WeeklyAdBlock block = adBlocks.get(entry.getKey());
			// Exclude blocks where total no of items are not matching with
			// actual total no of items(i.e. ad plex feed)
			if (block != null && checkItemsCodesAreEqual(actualItemsMap, adplexItemsMap, entry.getKey())) {
				ignoreBlock = false;
			}
			logger.debug("Page No: " + entry.getKey().getPageNumber() + ",Block No:" + entry.getKey().getBlockNumber()
					+ ",Actual Item Count:" + (block != null ? block.getActualTotalItems() : 0) 
					+ ",No Of Items:" + (block != null ? block.getAdPlexTotalItems() : 0));
			if (!ignoreBlock) {
				// for (OOSItemDTO oosItemDTO : entry.getValue()) {
				counter++;
				// If the header record is written other records can be
				// skipped.
				if (counter > 1) {
					break;
				}
				// Retailer item code
				sb.append(Constants.CHAIN_LEVEL_ID);
				// Week end date
				sb.append(",");
				// Category Name
				sb.append(weekEndingDateStr);
				sb.append(",");
				// Category Name
				sb.append(entry.getKey().getPageNumber());
				sb.append(",");
				// Item name
				sb.append(entry.getKey().getBlockNumber());
				sb.append(",");
				// LIG indicator
				// sb.append(entry.getValue().size());
				sb.append(0);
				sb.append(",");
				// Actual movement
					sb.append(oosItemDTO.getWeeklyActualMovement() == 0 ? Constants.EMPTY : oosItemDTO.getWeeklyActualMovement());
				sb.append(",");
				// Client Prediction
					sb.append(oosItemDTO.getClientWeeklyPredictedMovement() == 0 ? Constants.EMPTY : oosItemDTO.getClientWeeklyPredictedMovement());
				sb.append(",");
				// Total chain
					sb.append(oosItemDTO.getWeeklyPredictedMovement() == 0 ? Constants.EMPTY : oosItemDTO.getWeeklyPredictedMovement());
				sb.append("\n");
				// }
				matchingBlocks++;
			} else {
				ignoredBlocks++;
				logger.info("Page Number: " + entry.getKey().getPageNumber() + ",Block Number:" + entry.getKey().getBlockNumber()
						+ " is ignored as no of items between ad plex feed and promotion feed is not matching");
			}
		}

		logger.info("No of blocks considered:" + matchingBlocks + ",No of blocks ignored:" + ignoredBlocks);

		FileOutputStream outputStream = new FileOutputStream(outFile);
		outputStream.write(sb.toString().getBytes());
		outputStream.close();
		logger.info("File " + outFile + " has been created successfully.");
		return fileName;
	}

	/**
	 * Writes each value to cell
	 * 
	 * @param row
	 * @param oosItemDTO
	 * @param sheet
	 */
	private void writeToCell(OOSItemDTO oosItemDTO, StringBuilder sb) {
		// Retailer item code
		sb.append(Constants.CHAIN_LEVEL_ID);
		// Week end date
		sb.append(",");
		// Category Name
		sb.append(weekEndingDateStr);
		sb.append(",");
		// Category Name
		sb.append(oosItemDTO.getCategoryName());
		sb.append(",");
		// Item name
		sb.append(oosItemDTO.getItemName() != null ? oosItemDTO.getItemName().replaceAll(",", " ").replaceAll("\"", "") : "");
		sb.append(",");
		// LIG indicator
		sb.append((oosItemDTO.getRetLirId() == 0 ? 0 : 1));
		sb.append(",");
		// Actual movement
		sb.append(oosItemDTO.getWeeklyActualMovement() == 0 ? Constants.EMPTY : oosItemDTO.getWeeklyActualMovement());
		sb.append(",");
		// Client Prediction
		sb.append(oosItemDTO.getClientWeeklyPredictedMovement() == 0 ? Constants.EMPTY : oosItemDTO.getClientWeeklyPredictedMovement());
		sb.append(",");
		// Total chain
		sb.append(oosItemDTO.getWeeklyPredictedMovement() == 0 ? Constants.EMPTY : oosItemDTO.getWeeklyPredictedMovement());
		sb.append("\n");
	}

	/**
	 * Write header record
	 * 
	 * @param sheet
	 */
	private void writeHeader(StringBuilder sb) {
		// LOCATION_LEVEL_ID,WEEK_END_DATE,CATEGORY_NAME,
		// LIG_OR_ITEM_NAME,LIG_INDICATOR,ACTUAL_MOVEMENT,
		// TOPS_PREDICTION,PRESTO_PREDICTION
		sb.append("LOCATION_LEVEL_ID");
		sb.append(",");
		sb.append("WEEK_END_DATE");
		sb.append(",");
		sb.append("CATEGORY_NAME");
		sb.append(",");
		sb.append("LIG_OR_ITEM_NAME");
		sb.append(",");
		sb.append("LIG_INDICATOR");
		sb.append(",");
		sb.append("ACTUAL_MOVEMENT");
		sb.append(",");
		sb.append("TOPS_PREDICTION");
		sb.append(",");
		sb.append("PRESTO_PREDICTION");
		sb.append("\n");
	}

	/**
	 * Write header record
	 * 
	 * @param sheet
	 */
	private void writeHeaderPageBlock(StringBuilder sb) {
		// LOCATION_LEVEL_ID,WEEK_END_DATE,PAGE,BLOCK,NO_OF_LIG_AND_ITEM,
		// ACTUAL_MOVEMENT,TOPS_PREDICTION,PRESTO_PREDICTION
		sb.append("LOCATION_LEVEL_ID");
		sb.append(",");
		sb.append("WEEK_END_DATE");
		sb.append(",");
		sb.append("PAGE");
		sb.append(",");
		sb.append("BLOCK");
		sb.append(",");
		sb.append("NO_OF_LIG_AND_ITEM");
		sb.append(",");
		sb.append("ACTUAL_MOVEMENT");
		sb.append(",");
		sb.append("TOPS_PREDICTION");
		sb.append(",");
		sb.append("PRESTO_PREDICTION");
		sb.append("\n");
	}

	/**
	 * 
	 * @param inputFile
	 * @return output file
	 * @throws PredictionException
	 * @throws GeneralException
	 */
	private PredictionAccuracyReportDTO executeAccuracyReport(String inputFile) throws PredictionException, GeneralException {
		String pathToR = null;
		if (isPageBlockLevelReport) {
			pathToR = PropertyManager.getProperty("WEEKLY_CHAIN_PRED_PAGE_BLOCK_LEVEL_R_PATH");
		} else {
			pathToR = PropertyManager.getProperty("WEEKLY_CHAIN_PRED_ITEM_LEVEL_R_PATH");
		}

		// String outFile = null;
		Rengine rengine = getREngine();
		PredictionAccuracyReportDTO predictionAccuracyReportDTO = new PredictionAccuracyReportDTO();
//		List<OOSItemDTO> itemList = new ArrayList<>();
		try {

			// rengine.eval("input_file_name <- '" +
			// inputFile.replaceAll(".csv", "") + "';", false);
			rengine.eval("source('" + pathToR + "')", false);
			SimpleDateFormat format = new SimpleDateFormat(Constants.APP_DATE_FORMAT);
			Date endDate = format.parse(weekEndingDateStr);
			Date startDate = DateUtil.incrementDate(endDate, -6);
			String startDateStr = format.format(startDate);
			predictionAccuracyReportDTO.FROM_DATE = startDateStr;
			predictionAccuracyReportDTO.TO_DATE = weekEndingDateStr;
			predictionAccuracyReportDTO.TOTAL_ITEMS_DATA = rengine.eval("TOTAL_ITEMS_DATA").asInt();
			if (isPageBlockLevelReport)
				predictionAccuracyReportDTO.NO_OF_ITEMS_OR_BLOCKS_IGNORED = rengine.eval("NO_OF_BLOCKS_IGNORED").asInt();
			else
				predictionAccuracyReportDTO.NO_OF_ITEMS_OR_BLOCKS_IGNORED = rengine.eval("NO_OF_ITEMS_IGNORED").asInt();

			predictionAccuracyReportDTO.NO_OF_ITEMS_OR_BLOCKS_CONSIDERED = predictionAccuracyReportDTO.TOTAL_ITEMS_DATA
					- predictionAccuracyReportDTO.NO_OF_ITEMS_OR_BLOCKS_IGNORED;

			predictionAccuracyReportDTO.ACT_MOV_LT_X1_UNITS = rengine.eval("ACT_MOV_LT_X1_UNITS").asInt();
			predictionAccuracyReportDTO.ACT_MOV_ZERO = rengine.eval("ACT_MOV_ZERO").asInt();
			predictionAccuracyReportDTO.ACT_MOV_LT_X2_UNITS = rengine.eval("ACT_MOV_LT_X2_UNITS").asInt();
			predictionAccuracyReportDTO.ACT_MOV_BT_X1_X2_UNITS = rengine.eval("ACT_MOV_BT_X1_X2_UNITS").asInt();
			predictionAccuracyReportDTO.ACT_MOV_GT_X1_UNITS = rengine.eval("ACT_MOV_GT_X1_UNITS").asInt();
			predictionAccuracyReportDTO.ACT_MOV_LT_X3_UNITS = rengine.eval("ACT_MOV_LT_X3_UNITS").asInt();
			predictionAccuracyReportDTO.RMSE_PRESTO = checkEmptyAndReturnVal(rengine.eval("RMSE_PRESTO").asVector().at(0).asFactor().at(0));
			logger.info("rengine.eval(\"RMSE_CLIENT\") = " + rengine.eval("RMSE_CLIENT"));
			predictionAccuracyReportDTO.RMSE_CLIENT = checkEmptyAndReturnVal(rengine.eval("RMSE_CLIENT").asVector().at(0).asFactor().at(0));
			predictionAccuracyReportDTO.RMSE_HIGH_MOVER_PRESTO = checkEmptyAndReturnVal(
					rengine.eval("RMSE_HIGH_MOVER_PRESTO").asVector().at(0).asFactor().at(0));
			logger.info("rengine.eval(\"RMSE_HIGH_MOVER_CLIENT\") = " + rengine.eval("RMSE_HIGH_MOVER_CLIENT"));
			predictionAccuracyReportDTO.RMSE_HIGH_MOVER_CLIENT = checkEmptyAndReturnVal(
					rengine.eval("RMSE_HIGH_MOVER_CLIENT").asVector().at(0).asFactor().at(0));

			predictionAccuracyReportDTO.RMSE_MEDIUM_MOVER_PRESTO = checkEmptyAndReturnVal(
					rengine.eval("RMSE_MEDIUM_MOVER_PRESTO").asVector().at(0).asFactor().at(0));
			predictionAccuracyReportDTO.RMSE_MEDIUM_MOVER_CLIENT = checkEmptyAndReturnVal(
					rengine.eval("RMSE_MEDIUM_MOVER_CLIENT").asVector().at(0).asFactor().at(0));

			predictionAccuracyReportDTO.RMSE_SLOW_MOVER_PRESTO = checkEmptyAndReturnVal(
					rengine.eval("RMSE_SLOW_MOVER_PRESTO").asVector().at(0).asFactor().at(0));
			predictionAccuracyReportDTO.RMSE_SLOW_MOVER_CLIENT = checkEmptyAndReturnVal(
					rengine.eval("RMSE_SLOW_MOVER_CLIENT").asVector().at(0).asFactor().at(0));
			logger.debug(rengine.eval("AVG_ERR_PCT_PRESTO"));
			predictionAccuracyReportDTO.AVG_ERR_PCT_PRESTO = checkEmptyAndReturnVal(rengine.eval("AVG_ERR_PCT_PRESTO").asString());
			predictionAccuracyReportDTO.AVG_ERR_PCT_CLIENT = checkEmptyAndReturnVal(rengine.eval("AVG_ERR_PCT_CLIENT").asString());
			predictionAccuracyReportDTO.AVG_ERR_HIGH_MOVER_PCT_PRESTO = checkEmptyAndReturnVal(
					rengine.eval("AVG_ERR_HIGH_MOVER_PCT_PRESTO").asString());
			predictionAccuracyReportDTO.AVG_ERR_HIGH_MOVER_PCT_CLIENT = checkEmptyAndReturnVal(
					rengine.eval("AVG_ERR_HIGH_MOVER_PCT_CLIENT").asString());

			predictionAccuracyReportDTO.AVG_ERR_MEDIUM_MOVER_PCT_PRESTO = checkEmptyAndReturnVal(
					rengine.eval("AVG_ERR_MEDIUM_MOVER_PCT_PRESTO").asString());
			predictionAccuracyReportDTO.AVG_ERR_MEDIUM_MOVER_PCT_CLIENT = checkEmptyAndReturnVal(
					rengine.eval("AVG_ERR_MEDIUM_MOVER_PCT_CLIENT").asString());

			predictionAccuracyReportDTO.AVG_ERR_SLOW_MOVER_PCT_PRESTO = checkEmptyAndReturnVal(
					rengine.eval("AVG_ERR_SLOW_MOVER_PCT_PRESTO").asString());
			predictionAccuracyReportDTO.AVG_ERR_SLOW_MOVER_PCT_CLIENT = checkEmptyAndReturnVal(
					rengine.eval("AVG_ERR_SLOW_MOVER_PCT_CLIENT").asString());

			predictionAccuracyReportDTO.FORCST_CLOSER_PRESTO = checkEmptyAndReturnVal(rengine.eval("FORCST_CLOSER_PRESTO").asString());
			predictionAccuracyReportDTO.FORCST_CLOSER_PCT_PRESTO = checkEmptyAndReturnVal(rengine.eval("FORCST_CLOSER_PCT_PRESTO").asString());
			predictionAccuracyReportDTO.FORCST_CLOSER_CLIENT = checkEmptyAndReturnVal(rengine.eval("FORCST_CLOSER_CLIENT").asString());
			predictionAccuracyReportDTO.FORCST_CLOSER_PCT_CLIENT = checkEmptyAndReturnVal(rengine.eval("FORCST_CLOSER_PCT_CLIENT").asString());
			logger.debug("FORCST_CLOSER_PRESTO:" + rengine.eval("FORCST_CLOSER_PRESTO").asString()
					+ ",FORCST_CLOSER_PCT_PRESTO:" + rengine.eval("FORCST_CLOSER_PCT_PRESTO").asString()
					+ ",FORCST_CLOSER_CLIENT:" + rengine.eval("FORCST_CLOSER_CLIENT").asString()
					+ ",FORCST_CLOSER_PCT_CLIENT:" + rengine.eval("FORCST_CLOSER_PCT_CLIENT").asString());

			predictionAccuracyReportDTO.FORCST_HIGHER_PRESTO = checkEmptyAndReturnVal(rengine.eval("FORCST_HIGHER_PRESTO").asString());
			predictionAccuracyReportDTO.FORCST_HIGHER_PCT_PRESTO = checkEmptyAndReturnVal(rengine.eval("FORCST_HIGHER_PCT_PRESTO").asString());
			predictionAccuracyReportDTO.FORCST_HIGHER_CLIENT = checkEmptyAndReturnVal(rengine.eval("FORCST_HIGHER_CLIENT").asString());
			predictionAccuracyReportDTO.FORCST_HIGHER_PCT_CLIENT = checkEmptyAndReturnVal(rengine.eval("FORCST_HIGHER_PCT_CLIENT").asString());
			logger.debug("FORCST_HIGHER_PRESTO:" + rengine.eval("FORCST_HIGHER_PRESTO").asString()
					+ ",FORCST_HIGHER_PCT_PRESTO:" + rengine.eval("FORCST_HIGHER_PCT_PRESTO").asString()
					+ ",FORCST_HIGHER_CLIENT:" + rengine.eval("FORCST_HIGHER_CLIENT").asString()
					+ ",FORCST_HIGHER_PCT_CLIENT:" + rengine.eval("FORCST_HIGHER_PCT_CLIENT").asString());

			predictionAccuracyReportDTO.AVG_HIGHER_FORCST_PCT_PRESTO = checkEmptyAndReturnVal(
					rengine.eval("AVG_HIGHER_FORCST_PCT_PRESTO").asString());
			predictionAccuracyReportDTO.AVG_HIGHER_FORCST_PCT_CLIENT = checkEmptyAndReturnVal(
					rengine.eval("AVG_HIGHER_FORCST_PCT_CLIENT").asString());
			predictionAccuracyReportDTO.FORCST_LOWER_PRESTO = checkEmptyAndReturnVal(rengine.eval("FORCST_LOWER_PRESTO").asString());
			predictionAccuracyReportDTO.FORCST_LOWER_PCT_PRESTO = checkEmptyAndReturnVal(rengine.eval("FORCST_LOWER_PCT_PRESTO").asString());
			predictionAccuracyReportDTO.FORCST_LOWER_CLIENT = checkEmptyAndReturnVal(rengine.eval("FORCST_LOWER_CLIENT").asString());
			predictionAccuracyReportDTO.FORCST_LOWER_PCT_CLIENT = checkEmptyAndReturnVal(rengine.eval("FORCST_LOWER_PCT_CLIENT").asString());
			predictionAccuracyReportDTO.AVG_LOWER_FORCST_PCT_PRESTO = checkEmptyAndReturnVal(rengine.eval("AVG_LOWER_FORCST_PCT_PRESTO").asString());
			predictionAccuracyReportDTO.AVG_LOWER_FORCST_PCT_CLIENT = checkEmptyAndReturnVal(rengine.eval("AVG_LOWER_FORCST_PCT_CLIENT").asString());
			predictionAccuracyReportDTO.FORCST_GT_ACT_X1_TIMES_PRESTO = checkEmptyAndReturnVal(
					rengine.eval("FORCST_GT_ACT_X1_TIMES_PRESTO").asString());
			predictionAccuracyReportDTO.FORCST_GT_ACT_X1_TIMES_PCT_PRESTO = checkEmptyAndReturnVal(
					rengine.eval("FORCST_GT_ACT_X1_TIMES_PCT_PRESTO").asString());
			predictionAccuracyReportDTO.FORCST_GT_ACT_X1_TIMES_CLIENT = checkEmptyAndReturnVal(
					rengine.eval("FORCST_GT_ACT_X1_TIMES_CLIENT").asString());
			predictionAccuracyReportDTO.FORCST_GT_ACT_X1_TIMES_PCT_CLIENT = checkEmptyAndReturnVal(
					rengine.eval("FORCST_GT_ACT_X1_TIMES_PCT_CLIENT").asString());
			predictionAccuracyReportDTO.FORCST_LT_ACT_X1_TIMES_PRESTO = checkEmptyAndReturnVal(
					rengine.eval("FORCST_LT_ACT_X1_TIMES_PRESTO").asString());
			predictionAccuracyReportDTO.FORCST_LT_ACT_X1_TIMES_PCT_PRESTO = checkEmptyAndReturnVal(
					rengine.eval("FORCST_LT_ACT_X1_TIMES_PCT_PRESTO").asString());
			predictionAccuracyReportDTO.FORCST_LT_ACT_X1_TIMES_CLIENT = checkEmptyAndReturnVal(
					rengine.eval("FORCST_LT_ACT_X1_TIMES_CLIENT").asString());
			predictionAccuracyReportDTO.FORCST_LT_ACT_X1_TIMES_PCT_CLIENT = checkEmptyAndReturnVal(
					rengine.eval("FORCST_LT_ACT_X1_TIMES_PCT_CLIENT").asString());
			predictionAccuracyReportDTO.FORCST_WITHIN_X1_PCT_TOT_PRESTO = checkEmptyAndReturnVal(
					rengine.eval("FORCST_WITHIN_X1_PCT_TOT_PRESTO").asString());
			predictionAccuracyReportDTO.FORCST_WITHIN_X1_PCT_PCT_PRESTO = checkEmptyAndReturnVal(
					rengine.eval("FORCST_WITHIN_X1_PCT_PCT_PRESTO").asString());
			predictionAccuracyReportDTO.FORCST_WITHIN_X1_PCT_TOT_CLIENT = checkEmptyAndReturnVal(
					rengine.eval("FORCST_WITHIN_X1_PCT_TOT_CLIENT").asString());
			predictionAccuracyReportDTO.FORCST_WITHIN_X1_PCT_PCT_CLIENT = checkEmptyAndReturnVal(
					rengine.eval("FORCST_WITHIN_X1_PCT_PCT_CLIENT").asString());
			predictionAccuracyReportDTO.MAX_FORCST_INCOR_PCT_PRESTO = checkEmptyAndReturnVal(rengine.eval("MAX_FORCST_INCOR_PCT_PRESTO").asString());
			predictionAccuracyReportDTO.MAX_FORCST_INCOR_PCT_CLIENT = checkEmptyAndReturnVal(rengine.eval("MAX_FORCST_INCOR_PCT_CLIENT").asString());
			predictionAccuracyReportDTO.NO_FORCST_ITEMS_PRESTO = checkEmptyAndReturnVal(rengine.eval("NO_FORCST_ITEMS_PRESTO").asString());
			predictionAccuracyReportDTO.NO_FORCST_ITEMS_PCT_PRESTO = checkEmptyAndReturnVal(rengine.eval("NO_FORCST_ITEMS_PCT_PRESTO").asString());
			predictionAccuracyReportDTO.NO_FORCST_ITEMS_CLIENT = checkEmptyAndReturnVal(rengine.eval("NO_FORCST_ITEMS_CLIENT").asString());
			predictionAccuracyReportDTO.NO_FORCST_ITEMS_PCT_CLIENT = checkEmptyAndReturnVal(rengine.eval("NO_FORCST_ITEMS_PCT_CLIENT").asString());
			predictionAccuracyReportDTO.BOTH_FORCST_HIGHER = checkEmptyAndReturnVal(rengine.eval("BOTH_FORCST_HIGHER").asString());
			predictionAccuracyReportDTO.BOTH_FORCST_HIGHER_PCT = checkEmptyAndReturnVal(rengine.eval("BOTH_FORCST_HIGHER_PCT").asString());
			predictionAccuracyReportDTO.BOTH_FORCST_LOWER = checkEmptyAndReturnVal(rengine.eval("BOTH_FORCST_LOWER").asString());
			predictionAccuracyReportDTO.BOTH_FORCST_LOWER_PCT = checkEmptyAndReturnVal(rengine.eval("BOTH_FORCST_LOWER_PCT").asString());
			logger.debug(rengine.eval("RMSE_CLIENT") + ", " + rengine.eval("FORCST_CLOSER_PRESTO"));

			RFactor itemNames = rengine.eval("ITEM_NAME").asFactor();
			RFactor forecastPresto = rengine.eval("FORCST_PRESTO").asFactor();
			RFactor actualMov = rengine.eval("ACTUAL_PRESTO").asFactor();
			RFactor forecastClient = rengine.eval("FORCST_CLIENT").asFactor();
			// In Accurate Items By Absolute Errors
			RFactor itemNameABSERR = rengine.eval("ITEM_NAME_ABS_ERR").asFactor();
			RFactor forcstPrestoABSERR = rengine.eval("FORCST_PRESTO_ABS_ERR").asFactor();
			RFactor actualPrestoABSERR = rengine.eval("ACTUAL_PRESTO_ABS_ERR").asFactor();
			RFactor forcastClientABSERR = rengine.eval("FORCST_CLIENT_ABS_ERR").asFactor();
		
			predictionAccuracyReportDTO.inAccurateItems = getAccuracyReportList(itemNames, forecastPresto, actualMov,
					forecastClient);
			//To get in accurate item Absolute error list
			predictionAccuracyReportDTO.inAccurateItemsByAbsError = getAccuracyReportList(itemNameABSERR,
					forcstPrestoABSERR, actualPrestoABSERR, forcastClientABSERR);
		} catch (Exception e) {
			// rengine.end();
			logger.error("Error -- executeAccuracyReport", e);
			throw new GeneralException("Error -- executeAccuracyReport", e);
		} finally {
			rengine.end();
		}

		return predictionAccuracyReportDTO;
	}
	
	/**
	 * 
	 * @param val
	 * @return double value
	 */
	private double checkEmptyAndReturnVal(String val) {
		double retVal = 0;
		if (val != null) {
			if (!val.isEmpty()) {
				retVal = Double.parseDouble(val);
			}
		}
		return retVal;
	}

	/**
	 * 
	 * @return Rengine
	 * @throws PredictionException
	 */

	private Rengine getREngine() throws PredictionException {
		logger.info("getREngine() start");
		if (!Rengine.versionCheck()) {
			logger.error("** Version mismatch - Java files don't match library version.");
			throw new PredictionException("** Version mismatch - Java files don't match library version.");
		}

		logger.info("Creating Rengine (with arguments)");

		Rengine engine = Rengine.getMainEngine();
		if (engine == null) {
			logger.debug("Creating R Instance");
			engine = new Rengine(new String[] { "--vanilla" }, false, null);
		} else {
			logger.debug("R Instance already Exists");
		}
		logger.info("getREngine() end");
		return engine;
	}

	/**
	 * Writes excel file from the object.
	 * 
	 * @param itemsList,
	 *            Pre
	 * @return output file
	 * @throws GeneralException
	 */
	private String writeExcelFile(PredictionAccuracyReportDTO predictionAccuracyReportDTO, List<OOSItemDTO> itemList,
			HashMap<AdKey, OOSItemDTO> pageBlockMap, HashMap<AdKey, WeeklyAdBlock> adBlocks) throws GeneralException {

		// inputLocationName.replaceAll(" ", "-")
		String outFile = outputPath + "/Pristine_Tops_Weekly_Forecast_Accuracy_Report_Chainlevel_PB_WE"
				+ weekEndingDateStr.replaceAll("/", "").substring(0, 4) + ".xlsx";
		try {
			logger.debug("outFile:" + outFile);
			FileInputStream inputStream = new FileInputStream(templatePath);
			XSSFWorkbook workbook = new XSSFWorkbook(inputStream);
			XSSFSheet accuracyReportsheet = workbook.getSheet("Accuracy_Report");
			XSSFSheet detailsSheet = workbook.getSheet("Details");
			XSSFSheet blockDetailsSheet = workbook.getSheet("BlockDetails");

			// Fill Accuracy Report Sheet
			fillAccuracyReportSheet(workbook, predictionAccuracyReportDTO, accuracyReportsheet, itemList);

			// Fill detail sheet
			fillDetailsSheet(workbook, predictionAccuracyReportDTO, detailsSheet);
			
			//Fill Block Detail sheet
			fillBlockDetailsSheet(workbook, blockDetailsSheet, itemList, pageBlockMap, adBlocks);

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

	private void fillAccuracyReportSheet(XSSFWorkbook workbook, PredictionAccuracyReportDTO predictionAccuracyReportDTO,
			XSSFSheet accuracyReportsheet, List<OOSItemDTO> itemList)
			throws NoSuchFieldException, SecurityException, IllegalArgumentException, IllegalAccessException {
//		ExcelStyle excelStyle = new ExcelStyle();
		
		TreeMap<String, String> fieldNames = getAccuracyReportSheetFieldNames();
		for (Map.Entry<String, String> entry : fieldNames.entrySet()) {
			String[] rowAndCol = entry.getKey().split(Constants.INDEX_DELIMITER);
			int rowNum = Integer.parseInt(rowAndCol[0]);
			int cellNum = Integer.parseInt(rowAndCol[1]);
			Field field = predictionAccuracyReportDTO.getClass().getDeclaredField(entry.getValue());
			Object value = field.get(predictionAccuracyReportDTO);
			setCellValue(accuracyReportsheet, rowNum, cellNum, value);
		}
		//To assign values for inAccurateItems.
		setValuesInCell(workbook, accuracyReportsheet, itemList, predictionAccuracyReportDTO.inAccurateItems,false);
		
		setValuesInCell(workbook, accuracyReportsheet, itemList, predictionAccuracyReportDTO.inAccurateItemsByAbsError,true);
		
	}
	
	private void setValuesInCell(XSSFWorkbook workbook, XSSFSheet accuracyReportsheet, List<OOSItemDTO> itemList, List<OOSItemDTO> cellValuesList,
			boolean isAccurateItemsByAbsError) {
		ExcelStyle excelStyle = new ExcelStyle();
		int inAccurateCount = 0;
		
		//If isAccurateItemsByAbsError is true then add a row with header and increase the row number by one.
		if(isAccurateItemsByAbsError){
			XSSFRow row = accuracyReportsheet.getRow(inaccurateItemsRow);
			XSSFFont font = workbook.createFont();
			XSSFColor lightGrey =new XSSFColor(new java.awt.Color(242,242,242));
			XSSFCellStyle style = workbook.createCellStyle();
			font.setBold(false);
			style.setFont(font);
			style.setFillForegroundColor(lightGrey);
			style.setFillPattern(XSSFCellStyle.SOLID_FOREGROUND);
			style.setBorderTop(XSSFCellStyle.BORDER_THIN);
			style.setBorderBottom(XSSFCellStyle.BORDER_THIN);
			style.setBorderRight(XSSFCellStyle.BORDER_THIN);
			style.setBorderLeft(XSSFCellStyle.BORDER_THIN);
			XSSFCell cell1 = row.createCell(1);
			cell1.setCellValue("Pristine top 3 inaccurate forecasts (by Absolute Error)");
			cell1.setCellStyle(style);
			row.createCell(2).setCellStyle(style);
			row.createCell(3).setCellStyle(style);
			row.createCell(4).setCellStyle(style);
			row.createCell(5).setCellStyle(style);
						
			XSSFCell cell2 = row.createCell(7);
			cell2.setCellValue("Tops Markets top 3 inaccurate forecasts (by Absolute Error)");
			cell2.setCellStyle(style);
			
			row.createCell(8).setCellStyle(style);
			row.createCell(9).setCellStyle(style);
			row.createCell(10).setCellStyle(style);
			
			row.createCell(11).setCellStyle(style);
			
			inaccurateItemsRow++;
		}
		
		
		for (OOSItemDTO oosItemDTO : cellValuesList) {
			int colCount;
			//If it is list of absolute error then take max value of inaccurateItemMaxCount and add one
			if(isAccurateItemsByAbsError && inAccurateCount == 3){
				inaccurateItemsRow = inaccurateItemMaxCount + 2;
			}
			//If it is inAccurateItems then process normally as it does
			else if (inAccurateCount == 3) {
				// Assign the maximum number to insert into excel row for inAccurate Items By Absolute Error items..
				 if(inaccurateItemsRow > inaccurateItemMaxCount){
					inaccurateItemMaxCount = inaccurateItemsRow;
				}
				inaccurateItemsRow = 43;
			}
			if (inAccurateCount >= 3) {
				colCount = 6;
			} else {
				colCount = 0;
			}
			XSSFRow row = accuracyReportsheet.getRow(inaccurateItemsRow);
			logger.debug("oosItemDTO.getItemName(): " + oosItemDTO.getItemName());
			setCellValue(workbook, row, ++colCount, oosItemDTO.getItemName(), excelStyle);
			setCellValue(workbook, row, ++colCount, oosItemDTO.getWeeklyPredictedMovement(), excelStyle);
			setCellValue(workbook, row, ++colCount, oosItemDTO.getWeeklyActualMovement(), excelStyle);
			setCellValue(workbook, row, ++colCount, oosItemDTO.getClientWeeklyPredictedMovement(), excelStyle);
			setCellValue(workbook, row, ++colCount, oosItemDTO.getWeeklyActualMovement(), excelStyle);

			// NU: 28th Jul 2016, Write all item inside the block
			if (isPageBlockLevelReport) {
				ExcelStyle excelStyle1 = new ExcelStyle();
				excelStyle1.indention = 2;
				excelStyle1.cloneStyle = true;
				int pageNo = Integer.valueOf(oosItemDTO.getItemName().split("_")[0]);
				int blockNo = Integer.valueOf(oosItemDTO.getItemName().split("_")[1].trim());
				for (OOSItemDTO itemDTO : itemList) {
					if (itemDTO.getAdPageNo() == pageNo && itemDTO.getBlockNo() == blockNo) {
						inaccurateItemsRow++;
						if (inAccurateCount >= 3) {
							colCount = 6;
						} else {
							colCount = 0;
						}

						XSSFRow row1 = accuracyReportsheet.getRow(inaccurateItemsRow);
						logger.debug("inaccurateItemsRow:" + inaccurateItemsRow);
						setCellValue(workbook, row1, ++colCount, itemDTO.getItemName(), excelStyle1);
						logger.debug("oosItemDTO.getItemName(): " + oosItemDTO.getItemName());
						setCellValue(workbook, row1, ++colCount, itemDTO.getWeeklyPredictedMovement(), excelStyle);
						logger.debug("oosItemDTO.getWeeklyPredictedMovement(): " + oosItemDTO.getWeeklyPredictedMovement());
						setCellValue(workbook, row1, ++colCount, itemDTO.getWeeklyActualMovement(), excelStyle);
						logger.debug("oosItemDTO.getWeeklyActualMovement(): " + oosItemDTO.getWeeklyActualMovement());
					}
				}
			}

			inaccurateItemsRow++;
			inAccurateCount++;
		}
		if(!isAccurateItemsByAbsError){
			inaccurateItemsRow = inaccurateItemMaxCount +1;
		}
	}

	private void fillDetailsSheet(XSSFWorkbook workbook, PredictionAccuracyReportDTO predictionAccuracyReportDTO, XSSFSheet detailsSheet)
			throws NoSuchFieldException, SecurityException, IllegalArgumentException, IllegalAccessException {
		ExcelStyle excelStyle = new ExcelStyle();
		int categoriesWithNoPred = 31;
		TreeMap<String, String> fieldNames = getDetailsSheetFieldNames();
		for (Map.Entry<String, String> entry : fieldNames.entrySet()) {
			String[] rowAndCol = entry.getKey().split(Constants.INDEX_DELIMITER);
			int rowNum = Integer.parseInt(rowAndCol[0]);
			int cellNum = Integer.parseInt(rowAndCol[1]);
			Field field = predictionAccuracyReportDTO.getClass().getDeclaredField(entry.getValue());
			Object value = field.get(predictionAccuracyReportDTO);
			setCellValue(detailsSheet, rowNum, cellNum, value);
		}

		for (Map.Entry<String, Integer> entry : predictionAccuracyReportDTO.totNoOfNonPerishablesCategoriesWOPrediction.entrySet()) {
			int predictionStatus = entry.getValue();
			XSSFRow row = detailsSheet.getRow(categoriesWithNoPred);
			logger.debug("categoriesWithNoPred:" + categoriesWithNoPred);
			int catNameColCount = 0, catReasonColCount = 5;
			setCellValue(workbook, row, ++catNameColCount, entry.getKey(), excelStyle);
			if (predictionStatus == PredictionStatus.ERROR_MODEL_UNAVAILABLE.getStatusCode()
					|| predictionStatus == PredictionStatus.ERROR_CAL_CURVE_AVERAGE.getStatusCode()
					|| predictionStatus == PredictionStatus.ERROR_CAL_CURVE_AVERAGE_1.getStatusCode()
					|| predictionStatus == PredictionStatus.ERROR_CAL_CURVE_AVERAGE_2.getStatusCode()) {
				setCellValue(workbook, row, ++catReasonColCount, "Model was not build", excelStyle);
			} else if (predictionStatus == PredictionStatus.ERROR_COMPUTING_HIERARCHY.getStatusCode() ||
					predictionStatus == PredictionStatus.PREDICTION_APP_EXCEPTION.getStatusCode() ||
					predictionStatus == PredictionStatus.ERROR_DATA_PREPERATION.getStatusCode()) {
				setCellValue(workbook, row, ++catReasonColCount, "Prediction Technical Issue", excelStyle);
			} else {
				setCellValue(workbook, row, ++catReasonColCount, "Java Technical Issue", excelStyle);
			}

			categoriesWithNoPred++;
		}
	}

	private void saveToDatabase(PredictionAccuracyReportDTO predictionAccuracyReportDTO) throws GeneralException {
		PredictionAccuracyReportDAO accuracyReportDAO = new PredictionAccuracyReportDAO();
		AccuracyComparisonHeaderDTO accuracyComparisonHeaderDTO = new AccuracyComparisonHeaderDTO();
		List<AccuracyComparisonDetailDTO> accuaryComparisonDetails = new ArrayList<AccuracyComparisonDetailDTO>();

		try {

			// Fill header dto
			fillAccuracyComparisonHeader(accuracyComparisonHeaderDTO);

			// Fill detail dto
			fillAccuracyComparisonDetail(predictionAccuracyReportDTO, accuaryComparisonDetails);

			// Delete header and detail
			accuracyReportDAO.deleteComparisonHeader(conn, accuracyComparisonHeaderDTO);
			accuracyReportDAO.deleteComparisonDetail(conn, accuracyComparisonHeaderDTO);

			// Insert header
			accuracyReportDAO.insertAccuracyComparisonHeader(conn, accuracyComparisonHeaderDTO);

			// Insert details
			accuracyReportDAO.insertAccuracyComparisonDetail(conn, accuracyComparisonHeaderDTO, accuaryComparisonDetails);
		} catch (GeneralException e) {
			e.printStackTrace();
			logger.error("Error -- saveToDatabase() - " + e.toString(), e);
			throw new GeneralException("Error in saveToDatabase() - " + e.toString());
		}
	}

	private void fillAccuracyComparisonHeader(AccuracyComparisonHeaderDTO accuracyComparisonHeaderDTO) throws GeneralException {
		PredictionAccuracyReportDAO dao = new PredictionAccuracyReportDAO();
		long comparisonHeaderId = dao.getMaxComparisonHeaderId(conn);
		comparisonHeaderId = comparisonHeaderId + 1;
		accuracyComparisonHeaderDTO.setComparisonHeaderId(comparisonHeaderId);
		accuracyComparisonHeaderDTO.setCalendarId(weekCalendarId);
		accuracyComparisonHeaderDTO.setLocationLevelId(locationLevelId);
		accuracyComparisonHeaderDTO.setLocationId(locationId);
		accuracyComparisonHeaderDTO.setProductLevelId(0);
		accuracyComparisonHeaderDTO.setProductId(0);
		accuracyComparisonHeaderDTO.setReportTypeId(REPORT_TYPE_ID);
		accuracyComparisonHeaderDTO.setSubReportTypeId(subReportTypeId);
	}

	private void fillAccuracyComparisonDetail(PredictionAccuracyReportDTO predictionAccuracyReportDTO,
			List<AccuracyComparisonDetailDTO> accuaryComparisonDetails) {
		AccuracyComparisonDetailDTO accuracyComparisonDetailDTO = null;

		accuracyComparisonDetailDTO = new AccuracyComparisonDetailDTO();
		accuracyComparisonDetailDTO.setComparisonParamId(PredictionAccuracyMetrics.NO_ITEMS.getMetricsId());
		accuracyComparisonDetailDTO.setParameterValue(predictionAccuracyReportDTO.TOTAL_ITEMS_DATA);
		accuaryComparisonDetails.add(accuracyComparisonDetailDTO);

		accuracyComparisonDetailDTO = new AccuracyComparisonDetailDTO();
		accuracyComparisonDetailDTO.setComparisonParamId(PredictionAccuracyMetrics.IGNORED_NO.getMetricsId());
		accuracyComparisonDetailDTO.setParameterValue(predictionAccuracyReportDTO.NO_OF_ITEMS_OR_BLOCKS_IGNORED);
		accuaryComparisonDetails.add(accuracyComparisonDetailDTO);

		accuracyComparisonDetailDTO = new AccuracyComparisonDetailDTO();
		accuracyComparisonDetailDTO.setComparisonParamId(PredictionAccuracyMetrics.ACTUAL_MOV_ZERO_NO.getMetricsId());
		accuracyComparisonDetailDTO.setParameterValue(predictionAccuracyReportDTO.ACT_MOV_ZERO);
		accuaryComparisonDetails.add(accuracyComparisonDetailDTO);

		accuracyComparisonDetailDTO = new AccuracyComparisonDetailDTO();
		accuracyComparisonDetailDTO.setComparisonParamId(PredictionAccuracyMetrics.ACTUAL_MOV_GT_5000_NO.getMetricsId());
		accuracyComparisonDetailDTO.setParameterValue(predictionAccuracyReportDTO.ACT_MOV_GT_X1_UNITS);
		accuaryComparisonDetails.add(accuracyComparisonDetailDTO);

		accuracyComparisonDetailDTO = new AccuracyComparisonDetailDTO();
		accuracyComparisonDetailDTO.setComparisonParamId(PredictionAccuracyMetrics.ACTUAL_MOV_LT_500_NO.getMetricsId());
		accuracyComparisonDetailDTO.setParameterValue(predictionAccuracyReportDTO.ACT_MOV_LT_X3_UNITS);
		accuaryComparisonDetails.add(accuracyComparisonDetailDTO);

		// RMSE
		accuracyComparisonDetailDTO = new AccuracyComparisonDetailDTO();
		accuracyComparisonDetailDTO.setComparisonParamId(PredictionAccuracyMetrics.RMSE.getMetricsId());
		accuracyComparisonDetailDTO.setParameterValue(predictionAccuracyReportDTO.RMSE_PRESTO);
		accuracyComparisonDetailDTO.setVersionType(VALUE_TYPE_PRESTO);
		accuaryComparisonDetails.add(accuracyComparisonDetailDTO);
		accuracyComparisonDetailDTO = new AccuracyComparisonDetailDTO();
		accuracyComparisonDetailDTO.setComparisonParamId(PredictionAccuracyMetrics.RMSE.getMetricsId());
		accuracyComparisonDetailDTO.setParameterValue(predictionAccuracyReportDTO.RMSE_CLIENT);
		accuracyComparisonDetailDTO.setVersionType(VALUE_TYPE_CLIENT);
		accuaryComparisonDetails.add(accuracyComparisonDetailDTO);
		accuracyComparisonDetailDTO = new AccuracyComparisonDetailDTO();
		accuracyComparisonDetailDTO.setComparisonParamId(PredictionAccuracyMetrics.RMSE_HIGHER_MOVER.getMetricsId());
		accuracyComparisonDetailDTO.setParameterValue(predictionAccuracyReportDTO.RMSE_HIGH_MOVER_PRESTO);
		accuracyComparisonDetailDTO.setVersionType(VALUE_TYPE_PRESTO);
		accuaryComparisonDetails.add(accuracyComparisonDetailDTO);
		accuracyComparisonDetailDTO = new AccuracyComparisonDetailDTO();
		accuracyComparisonDetailDTO.setComparisonParamId(PredictionAccuracyMetrics.RMSE_HIGHER_MOVER.getMetricsId());
		accuracyComparisonDetailDTO.setParameterValue(predictionAccuracyReportDTO.RMSE_HIGH_MOVER_CLIENT);
		accuracyComparisonDetailDTO.setVersionType(VALUE_TYPE_CLIENT);
		accuaryComparisonDetails.add(accuracyComparisonDetailDTO);
		accuracyComparisonDetailDTO = new AccuracyComparisonDetailDTO();
		accuracyComparisonDetailDTO.setComparisonParamId(PredictionAccuracyMetrics.RMSE_SLOWER_MOVER.getMetricsId());
		accuracyComparisonDetailDTO.setParameterValue(predictionAccuracyReportDTO.RMSE_SLOW_MOVER_PRESTO);
		accuracyComparisonDetailDTO.setVersionType(VALUE_TYPE_PRESTO);
		accuaryComparisonDetails.add(accuracyComparisonDetailDTO);
		accuracyComparisonDetailDTO = new AccuracyComparisonDetailDTO();
		accuracyComparisonDetailDTO.setComparisonParamId(PredictionAccuracyMetrics.RMSE_SLOWER_MOVER.getMetricsId());
		accuracyComparisonDetailDTO.setParameterValue(predictionAccuracyReportDTO.RMSE_SLOW_MOVER_CLIENT);
		accuracyComparisonDetailDTO.setVersionType(VALUE_TYPE_CLIENT);
		accuaryComparisonDetails.add(accuracyComparisonDetailDTO);

		// Average % Error
		accuracyComparisonDetailDTO = new AccuracyComparisonDetailDTO();
		accuracyComparisonDetailDTO.setComparisonParamId(PredictionAccuracyMetrics.AVG_ERR_PCT.getMetricsId());
		accuracyComparisonDetailDTO.setParameterValue(predictionAccuracyReportDTO.AVG_ERR_PCT_PRESTO);
		accuracyComparisonDetailDTO.setVersionType(VALUE_TYPE_PRESTO);
		accuaryComparisonDetails.add(accuracyComparisonDetailDTO);
		accuracyComparisonDetailDTO = new AccuracyComparisonDetailDTO();
		accuracyComparisonDetailDTO.setComparisonParamId(PredictionAccuracyMetrics.AVG_ERR_PCT.getMetricsId());
		accuracyComparisonDetailDTO.setParameterValue(predictionAccuracyReportDTO.AVG_ERR_PCT_CLIENT);
		accuracyComparisonDetailDTO.setVersionType(VALUE_TYPE_CLIENT);
		accuaryComparisonDetails.add(accuracyComparisonDetailDTO);
		accuracyComparisonDetailDTO = new AccuracyComparisonDetailDTO();
		accuracyComparisonDetailDTO.setComparisonParamId(PredictionAccuracyMetrics.AVG_ERR_PCT_HIGHER_MOVER.getMetricsId());
		accuracyComparisonDetailDTO.setParameterValue(predictionAccuracyReportDTO.AVG_ERR_HIGH_MOVER_PCT_PRESTO);
		accuracyComparisonDetailDTO.setVersionType(VALUE_TYPE_PRESTO);
		accuaryComparisonDetails.add(accuracyComparisonDetailDTO);
		accuracyComparisonDetailDTO = new AccuracyComparisonDetailDTO();
		accuracyComparisonDetailDTO.setComparisonParamId(PredictionAccuracyMetrics.AVG_ERR_PCT_HIGHER_MOVER.getMetricsId());
		accuracyComparisonDetailDTO.setParameterValue(predictionAccuracyReportDTO.AVG_ERR_HIGH_MOVER_PCT_CLIENT);
		accuracyComparisonDetailDTO.setVersionType(VALUE_TYPE_CLIENT);
		accuaryComparisonDetails.add(accuracyComparisonDetailDTO);
		accuracyComparisonDetailDTO = new AccuracyComparisonDetailDTO();
		accuracyComparisonDetailDTO.setComparisonParamId(PredictionAccuracyMetrics.AVG_ERR_PCT_SLOWER_MOVER.getMetricsId());
		accuracyComparisonDetailDTO.setParameterValue(predictionAccuracyReportDTO.AVG_ERR_SLOW_MOVER_PCT_PRESTO);
		accuracyComparisonDetailDTO.setVersionType(VALUE_TYPE_PRESTO);
		accuaryComparisonDetails.add(accuracyComparisonDetailDTO);
		accuracyComparisonDetailDTO = new AccuracyComparisonDetailDTO();
		accuracyComparisonDetailDTO.setComparisonParamId(PredictionAccuracyMetrics.AVG_ERR_PCT_SLOWER_MOVER.getMetricsId());
		accuracyComparisonDetailDTO.setParameterValue(predictionAccuracyReportDTO.AVG_ERR_SLOW_MOVER_PCT_CLIENT);
		accuracyComparisonDetailDTO.setVersionType(VALUE_TYPE_CLIENT);
		accuaryComparisonDetails.add(accuracyComparisonDetailDTO);

		// forecast closer
		accuracyComparisonDetailDTO = new AccuracyComparisonDetailDTO();
		accuracyComparisonDetailDTO.setComparisonParamId(PredictionAccuracyMetrics.FORECAST_CLOSER_NO.getMetricsId());
		accuracyComparisonDetailDTO.setParameterValue(predictionAccuracyReportDTO.FORCST_CLOSER_PRESTO);
		accuracyComparisonDetailDTO.setVersionType(VALUE_TYPE_PRESTO);
		accuaryComparisonDetails.add(accuracyComparisonDetailDTO);
		accuracyComparisonDetailDTO = new AccuracyComparisonDetailDTO();
		accuracyComparisonDetailDTO.setComparisonParamId(PredictionAccuracyMetrics.FORECAST_CLOSER_NO.getMetricsId());
		accuracyComparisonDetailDTO.setParameterValue(predictionAccuracyReportDTO.FORCST_CLOSER_CLIENT);
		accuracyComparisonDetailDTO.setVersionType(VALUE_TYPE_CLIENT);
		accuaryComparisonDetails.add(accuracyComparisonDetailDTO);
		accuracyComparisonDetailDTO = new AccuracyComparisonDetailDTO();
		accuracyComparisonDetailDTO.setComparisonParamId(PredictionAccuracyMetrics.FORECAST_CLOSER_PCT.getMetricsId());
		accuracyComparisonDetailDTO.setParameterValue(predictionAccuracyReportDTO.FORCST_CLOSER_PCT_PRESTO);
		accuracyComparisonDetailDTO.setVersionType(VALUE_TYPE_PRESTO);
		accuaryComparisonDetails.add(accuracyComparisonDetailDTO);
		accuracyComparisonDetailDTO = new AccuracyComparisonDetailDTO();
		accuracyComparisonDetailDTO.setComparisonParamId(PredictionAccuracyMetrics.FORECAST_CLOSER_PCT.getMetricsId());
		accuracyComparisonDetailDTO.setParameterValue(predictionAccuracyReportDTO.FORCST_CLOSER_PCT_CLIENT);
		accuracyComparisonDetailDTO.setVersionType(VALUE_TYPE_CLIENT);
		accuaryComparisonDetails.add(accuracyComparisonDetailDTO);

		// forecast higher
		accuracyComparisonDetailDTO = new AccuracyComparisonDetailDTO();
		accuracyComparisonDetailDTO.setComparisonParamId(PredictionAccuracyMetrics.FORECAST_HIGHER_NO.getMetricsId());
		accuracyComparisonDetailDTO.setParameterValue(predictionAccuracyReportDTO.FORCST_HIGHER_PRESTO);
		accuracyComparisonDetailDTO.setVersionType(VALUE_TYPE_PRESTO);
		accuaryComparisonDetails.add(accuracyComparisonDetailDTO);
		accuracyComparisonDetailDTO = new AccuracyComparisonDetailDTO();
		accuracyComparisonDetailDTO.setComparisonParamId(PredictionAccuracyMetrics.FORECAST_HIGHER_NO.getMetricsId());
		accuracyComparisonDetailDTO.setParameterValue(predictionAccuracyReportDTO.FORCST_HIGHER_CLIENT);
		accuracyComparisonDetailDTO.setVersionType(VALUE_TYPE_CLIENT);
		accuaryComparisonDetails.add(accuracyComparisonDetailDTO);
		accuracyComparisonDetailDTO = new AccuracyComparisonDetailDTO();
		accuracyComparisonDetailDTO.setComparisonParamId(PredictionAccuracyMetrics.FORECAST_HIGHER_PCT.getMetricsId());
		accuracyComparisonDetailDTO.setParameterValue(predictionAccuracyReportDTO.FORCST_HIGHER_PCT_PRESTO);
		accuracyComparisonDetailDTO.setVersionType(VALUE_TYPE_PRESTO);
		accuaryComparisonDetails.add(accuracyComparisonDetailDTO);
		accuracyComparisonDetailDTO = new AccuracyComparisonDetailDTO();
		accuracyComparisonDetailDTO.setComparisonParamId(PredictionAccuracyMetrics.FORECAST_HIGHER_PCT.getMetricsId());
		accuracyComparisonDetailDTO.setParameterValue(predictionAccuracyReportDTO.FORCST_HIGHER_PCT_CLIENT);
		accuracyComparisonDetailDTO.setVersionType(VALUE_TYPE_CLIENT);
		accuaryComparisonDetails.add(accuracyComparisonDetailDTO);
		accuracyComparisonDetailDTO = new AccuracyComparisonDetailDTO();
		accuracyComparisonDetailDTO.setComparisonParamId(PredictionAccuracyMetrics.FORECAST_HIGHER_AVG_PCT.getMetricsId());
		accuracyComparisonDetailDTO.setParameterValue(predictionAccuracyReportDTO.AVG_HIGHER_FORCST_PCT_PRESTO);
		accuracyComparisonDetailDTO.setVersionType(VALUE_TYPE_PRESTO);
		accuaryComparisonDetails.add(accuracyComparisonDetailDTO);
		accuracyComparisonDetailDTO = new AccuracyComparisonDetailDTO();
		accuracyComparisonDetailDTO.setComparisonParamId(PredictionAccuracyMetrics.FORECAST_HIGHER_AVG_PCT.getMetricsId());
		accuracyComparisonDetailDTO.setParameterValue(predictionAccuracyReportDTO.AVG_HIGHER_FORCST_PCT_CLIENT);
		accuracyComparisonDetailDTO.setVersionType(VALUE_TYPE_CLIENT);
		accuaryComparisonDetails.add(accuracyComparisonDetailDTO);

		// forecast lower
		accuracyComparisonDetailDTO = new AccuracyComparisonDetailDTO();
		accuracyComparisonDetailDTO.setComparisonParamId(PredictionAccuracyMetrics.FORECAST_LOWER_NO.getMetricsId());
		accuracyComparisonDetailDTO.setParameterValue(predictionAccuracyReportDTO.FORCST_LOWER_PRESTO);
		accuracyComparisonDetailDTO.setVersionType(VALUE_TYPE_PRESTO);
		accuaryComparisonDetails.add(accuracyComparisonDetailDTO);
		accuracyComparisonDetailDTO = new AccuracyComparisonDetailDTO();
		accuracyComparisonDetailDTO.setComparisonParamId(PredictionAccuracyMetrics.FORECAST_LOWER_NO.getMetricsId());
		accuracyComparisonDetailDTO.setParameterValue(predictionAccuracyReportDTO.FORCST_LOWER_CLIENT);
		accuracyComparisonDetailDTO.setVersionType(VALUE_TYPE_CLIENT);
		accuaryComparisonDetails.add(accuracyComparisonDetailDTO);
		accuracyComparisonDetailDTO = new AccuracyComparisonDetailDTO();
		accuracyComparisonDetailDTO.setComparisonParamId(PredictionAccuracyMetrics.FORECAST_LOWER_PCT.getMetricsId());
		accuracyComparisonDetailDTO.setParameterValue(predictionAccuracyReportDTO.FORCST_LOWER_PCT_PRESTO);
		accuracyComparisonDetailDTO.setVersionType(VALUE_TYPE_PRESTO);
		accuaryComparisonDetails.add(accuracyComparisonDetailDTO);
		accuracyComparisonDetailDTO = new AccuracyComparisonDetailDTO();
		accuracyComparisonDetailDTO.setComparisonParamId(PredictionAccuracyMetrics.FORECAST_LOWER_PCT.getMetricsId());
		accuracyComparisonDetailDTO.setParameterValue(predictionAccuracyReportDTO.FORCST_LOWER_PCT_CLIENT);
		accuracyComparisonDetailDTO.setVersionType(VALUE_TYPE_CLIENT);
		accuaryComparisonDetails.add(accuracyComparisonDetailDTO);
		accuracyComparisonDetailDTO = new AccuracyComparisonDetailDTO();
		accuracyComparisonDetailDTO.setComparisonParamId(PredictionAccuracyMetrics.FORECAST_LOWER_AVG_PCT.getMetricsId());
		accuracyComparisonDetailDTO.setParameterValue(predictionAccuracyReportDTO.AVG_LOWER_FORCST_PCT_PRESTO);
		accuracyComparisonDetailDTO.setVersionType(VALUE_TYPE_PRESTO);
		accuaryComparisonDetails.add(accuracyComparisonDetailDTO);
		accuracyComparisonDetailDTO = new AccuracyComparisonDetailDTO();
		accuracyComparisonDetailDTO.setComparisonParamId(PredictionAccuracyMetrics.FORECAST_LOWER_AVG_PCT.getMetricsId());
		accuracyComparisonDetailDTO.setParameterValue(predictionAccuracyReportDTO.AVG_LOWER_FORCST_PCT_CLIENT);
		accuracyComparisonDetailDTO.setVersionType(VALUE_TYPE_CLIENT);
		accuaryComparisonDetails.add(accuracyComparisonDetailDTO);

		// forecast 3 times
		accuracyComparisonDetailDTO = new AccuracyComparisonDetailDTO();
		accuracyComparisonDetailDTO.setComparisonParamId(PredictionAccuracyMetrics.FORECAST_GT_3_TIMES_OF_ACTUAL_NO.getMetricsId());
		accuracyComparisonDetailDTO.setParameterValue(predictionAccuracyReportDTO.FORCST_GT_ACT_X1_TIMES_PRESTO);
		accuracyComparisonDetailDTO.setVersionType(VALUE_TYPE_PRESTO);
		accuaryComparisonDetails.add(accuracyComparisonDetailDTO);
		accuracyComparisonDetailDTO = new AccuracyComparisonDetailDTO();
		accuracyComparisonDetailDTO.setComparisonParamId(PredictionAccuracyMetrics.FORECAST_GT_3_TIMES_OF_ACTUAL_NO.getMetricsId());
		accuracyComparisonDetailDTO.setParameterValue(predictionAccuracyReportDTO.FORCST_GT_ACT_X1_TIMES_CLIENT);
		accuracyComparisonDetailDTO.setVersionType(VALUE_TYPE_CLIENT);
		accuaryComparisonDetails.add(accuracyComparisonDetailDTO);
		accuracyComparisonDetailDTO = new AccuracyComparisonDetailDTO();
		accuracyComparisonDetailDTO.setComparisonParamId(PredictionAccuracyMetrics.FORECAST_GT_3_TIMES_OF_ACTUAL_PCT.getMetricsId());
		accuracyComparisonDetailDTO.setParameterValue(predictionAccuracyReportDTO.FORCST_GT_ACT_X1_TIMES_PCT_PRESTO);
		accuracyComparisonDetailDTO.setVersionType(VALUE_TYPE_PRESTO);
		accuaryComparisonDetails.add(accuracyComparisonDetailDTO);
		accuracyComparisonDetailDTO = new AccuracyComparisonDetailDTO();
		accuracyComparisonDetailDTO.setComparisonParamId(PredictionAccuracyMetrics.FORECAST_GT_3_TIMES_OF_ACTUAL_PCT.getMetricsId());
		accuracyComparisonDetailDTO.setParameterValue(predictionAccuracyReportDTO.FORCST_GT_ACT_X1_TIMES_PCT_CLIENT);
		accuracyComparisonDetailDTO.setVersionType(VALUE_TYPE_CLIENT);
		accuaryComparisonDetails.add(accuracyComparisonDetailDTO);

		// forecast 1/3
		accuracyComparisonDetailDTO = new AccuracyComparisonDetailDTO();
		accuracyComparisonDetailDTO.setComparisonParamId(PredictionAccuracyMetrics.FORECAST_LT_ONE_THIRD_OF_ACTUAL_NO.getMetricsId());
		accuracyComparisonDetailDTO.setParameterValue(predictionAccuracyReportDTO.FORCST_LT_ACT_X1_TIMES_PRESTO);
		accuracyComparisonDetailDTO.setVersionType(VALUE_TYPE_PRESTO);
		accuaryComparisonDetails.add(accuracyComparisonDetailDTO);
		accuracyComparisonDetailDTO = new AccuracyComparisonDetailDTO();
		accuracyComparisonDetailDTO.setComparisonParamId(PredictionAccuracyMetrics.FORECAST_LT_ONE_THIRD_OF_ACTUAL_NO.getMetricsId());
		accuracyComparisonDetailDTO.setParameterValue(predictionAccuracyReportDTO.FORCST_LT_ACT_X1_TIMES_CLIENT);
		accuracyComparisonDetailDTO.setVersionType(VALUE_TYPE_CLIENT);
		accuaryComparisonDetails.add(accuracyComparisonDetailDTO);
		accuracyComparisonDetailDTO = new AccuracyComparisonDetailDTO();
		accuracyComparisonDetailDTO.setComparisonParamId(PredictionAccuracyMetrics.FORECAST_LT_ONE_THIRD_OF_ACTUAL_PCT.getMetricsId());
		accuracyComparisonDetailDTO.setParameterValue(predictionAccuracyReportDTO.FORCST_LT_ACT_X1_TIMES_PCT_PRESTO);
		accuracyComparisonDetailDTO.setVersionType(VALUE_TYPE_PRESTO);
		accuaryComparisonDetails.add(accuracyComparisonDetailDTO);
		accuracyComparisonDetailDTO = new AccuracyComparisonDetailDTO();
		accuracyComparisonDetailDTO.setComparisonParamId(PredictionAccuracyMetrics.FORECAST_LT_ONE_THIRD_OF_ACTUAL_PCT.getMetricsId());
		accuracyComparisonDetailDTO.setParameterValue(predictionAccuracyReportDTO.FORCST_LT_ACT_X1_TIMES_PCT_CLIENT);
		accuracyComparisonDetailDTO.setVersionType(VALUE_TYPE_CLIENT);
		accuaryComparisonDetails.add(accuracyComparisonDetailDTO);

		// forecast within 25%
		accuracyComparisonDetailDTO = new AccuracyComparisonDetailDTO();
		accuracyComparisonDetailDTO.setComparisonParamId(PredictionAccuracyMetrics.FORECAST_WITHIN_25_PCT_NO.getMetricsId());
		accuracyComparisonDetailDTO.setParameterValue(predictionAccuracyReportDTO.FORCST_WITHIN_X1_PCT_TOT_PRESTO);
		accuracyComparisonDetailDTO.setVersionType(VALUE_TYPE_PRESTO);
		accuaryComparisonDetails.add(accuracyComparisonDetailDTO);
		accuracyComparisonDetailDTO = new AccuracyComparisonDetailDTO();
		accuracyComparisonDetailDTO.setComparisonParamId(PredictionAccuracyMetrics.FORECAST_WITHIN_25_PCT_NO.getMetricsId());
		accuracyComparisonDetailDTO.setParameterValue(predictionAccuracyReportDTO.FORCST_WITHIN_X1_PCT_TOT_CLIENT);
		accuracyComparisonDetailDTO.setVersionType(VALUE_TYPE_CLIENT);
		accuaryComparisonDetails.add(accuracyComparisonDetailDTO);
		accuracyComparisonDetailDTO = new AccuracyComparisonDetailDTO();
		accuracyComparisonDetailDTO.setComparisonParamId(PredictionAccuracyMetrics.FORECAST_WITHIN_25_PCT_PCT.getMetricsId());
		accuracyComparisonDetailDTO.setParameterValue(predictionAccuracyReportDTO.FORCST_WITHIN_X1_PCT_PCT_PRESTO);
		accuracyComparisonDetailDTO.setVersionType(VALUE_TYPE_PRESTO);
		accuaryComparisonDetails.add(accuracyComparisonDetailDTO);
		accuracyComparisonDetailDTO = new AccuracyComparisonDetailDTO();
		accuracyComparisonDetailDTO.setComparisonParamId(PredictionAccuracyMetrics.FORECAST_WITHIN_25_PCT_PCT.getMetricsId());
		accuracyComparisonDetailDTO.setParameterValue(predictionAccuracyReportDTO.FORCST_WITHIN_X1_PCT_PCT_CLIENT);
		accuracyComparisonDetailDTO.setVersionType(VALUE_TYPE_CLIENT);
		accuaryComparisonDetails.add(accuracyComparisonDetailDTO);

		// Maximum % by which Forecast was inaccurate
		accuracyComparisonDetailDTO = new AccuracyComparisonDetailDTO();
		accuracyComparisonDetailDTO.setComparisonParamId(PredictionAccuracyMetrics.FORECAST_INACCURATE_MAX_PCT.getMetricsId());
		accuracyComparisonDetailDTO.setParameterValue(predictionAccuracyReportDTO.MAX_FORCST_INCOR_PCT_PRESTO);
		accuracyComparisonDetailDTO.setVersionType(VALUE_TYPE_PRESTO);
		accuaryComparisonDetails.add(accuracyComparisonDetailDTO);
		accuracyComparisonDetailDTO = new AccuracyComparisonDetailDTO();
		accuracyComparisonDetailDTO.setComparisonParamId(PredictionAccuracyMetrics.FORECAST_INACCURATE_MAX_PCT.getMetricsId());
		accuracyComparisonDetailDTO.setParameterValue(predictionAccuracyReportDTO.MAX_FORCST_INCOR_PCT_CLIENT);
		accuracyComparisonDetailDTO.setVersionType(VALUE_TYPE_CLIENT);
		accuaryComparisonDetails.add(accuracyComparisonDetailDTO);

		// both forecast higher
		accuracyComparisonDetailDTO = new AccuracyComparisonDetailDTO();
		accuracyComparisonDetailDTO.setComparisonParamId(PredictionAccuracyMetrics.BOTH_FORECAST_HIGHER_NO.getMetricsId());
		accuracyComparisonDetailDTO.setParameterValue(predictionAccuracyReportDTO.BOTH_FORCST_HIGHER);
		accuaryComparisonDetails.add(accuracyComparisonDetailDTO);
		accuracyComparisonDetailDTO = new AccuracyComparisonDetailDTO();
		accuracyComparisonDetailDTO.setComparisonParamId(PredictionAccuracyMetrics.BOTH_FORECAST_HIGHER_PCT.getMetricsId());
		accuracyComparisonDetailDTO.setParameterValue(predictionAccuracyReportDTO.BOTH_FORCST_HIGHER_PCT);
		accuaryComparisonDetails.add(accuracyComparisonDetailDTO);

		// both forecast lower
		accuracyComparisonDetailDTO = new AccuracyComparisonDetailDTO();
		accuracyComparisonDetailDTO.setComparisonParamId(PredictionAccuracyMetrics.BOTH_FORECAST_LOWER_NO.getMetricsId());
		accuracyComparisonDetailDTO.setParameterValue(predictionAccuracyReportDTO.BOTH_FORCST_LOWER);
		accuaryComparisonDetails.add(accuracyComparisonDetailDTO);
		accuracyComparisonDetailDTO = new AccuracyComparisonDetailDTO();
		accuracyComparisonDetailDTO.setComparisonParamId(PredictionAccuracyMetrics.BOTH_FORECAST_LOWER_PCT.getMetricsId());
		accuracyComparisonDetailDTO.setParameterValue(predictionAccuracyReportDTO.BOTH_FORCST_LOWER_PCT);
		accuaryComparisonDetails.add(accuracyComparisonDetailDTO);

		// no forecast
		accuracyComparisonDetailDTO = new AccuracyComparisonDetailDTO();
		accuracyComparisonDetailDTO.setComparisonParamId(PredictionAccuracyMetrics.WITHOUT_FORECAST_NO.getMetricsId());
		accuracyComparisonDetailDTO.setParameterValue(predictionAccuracyReportDTO.NO_FORCST_ITEMS_PRESTO);
		accuracyComparisonDetailDTO.setVersionType(VALUE_TYPE_PRESTO);
		accuaryComparisonDetails.add(accuracyComparisonDetailDTO);
		accuracyComparisonDetailDTO = new AccuracyComparisonDetailDTO();
		accuracyComparisonDetailDTO.setComparisonParamId(PredictionAccuracyMetrics.WITHOUT_FORECAST_NO.getMetricsId());
		accuracyComparisonDetailDTO.setParameterValue(predictionAccuracyReportDTO.NO_FORCST_ITEMS_CLIENT);
		accuracyComparisonDetailDTO.setVersionType(VALUE_TYPE_CLIENT);
		accuaryComparisonDetails.add(accuracyComparisonDetailDTO);

		// no forecast pct
		accuracyComparisonDetailDTO = new AccuracyComparisonDetailDTO();
		accuracyComparisonDetailDTO.setComparisonParamId(PredictionAccuracyMetrics.WITHOUT_FORECAST_PCT.getMetricsId());
		accuracyComparisonDetailDTO.setParameterValue(predictionAccuracyReportDTO.NO_FORCST_ITEMS_PCT_PRESTO);
		accuracyComparisonDetailDTO.setVersionType(VALUE_TYPE_PRESTO);
		accuaryComparisonDetails.add(accuracyComparisonDetailDTO);
		accuracyComparisonDetailDTO = new AccuracyComparisonDetailDTO();
		accuracyComparisonDetailDTO.setComparisonParamId(PredictionAccuracyMetrics.WITHOUT_FORECAST_PCT.getMetricsId());
		accuracyComparisonDetailDTO.setParameterValue(predictionAccuracyReportDTO.NO_FORCST_ITEMS_PCT_CLIENT);
		accuracyComparisonDetailDTO.setVersionType(VALUE_TYPE_CLIENT);
		accuaryComparisonDetails.add(accuracyComparisonDetailDTO);

		int orderNo = 1;
		// inaccurate forecast presto
		for (OOSItemDTO oosItemDTO : predictionAccuracyReportDTO.inAccurateItems) {
			if (orderNo <= TOP_INACCURATE_FORECAST) {
				accuracyComparisonDetailDTO = new AccuracyComparisonDetailDTO();
				accuracyComparisonDetailDTO.setComparisonParamId(PredictionAccuracyMetrics.PRESTO_TOP_INACCURATE_NAME.getMetricsId());
				accuracyComparisonDetailDTO.setParameterValueText(oosItemDTO.getItemName());
				accuracyComparisonDetailDTO.setOrderNo(orderNo);
				accuaryComparisonDetails.add(accuracyComparisonDetailDTO);
				accuracyComparisonDetailDTO = new AccuracyComparisonDetailDTO();
				accuracyComparisonDetailDTO.setComparisonParamId(PredictionAccuracyMetrics.PRESTO_TOP_INACCURATE_FORECAST.getMetricsId());
				accuracyComparisonDetailDTO.setParameterValue(oosItemDTO.getWeeklyPredictedMovement());
				accuracyComparisonDetailDTO.setVersionType(VALUE_TYPE_PRESTO);
				accuracyComparisonDetailDTO.setOrderNo(orderNo);
				accuaryComparisonDetails.add(accuracyComparisonDetailDTO);
				accuracyComparisonDetailDTO = new AccuracyComparisonDetailDTO();
				accuracyComparisonDetailDTO.setComparisonParamId(PredictionAccuracyMetrics.PRESTO_TOP_INACCURATE_FORECAST.getMetricsId());
				accuracyComparisonDetailDTO.setParameterValue(oosItemDTO.getClientWeeklyPredictedMovement());
				accuracyComparisonDetailDTO.setVersionType(VALUE_TYPE_CLIENT);
				accuracyComparisonDetailDTO.setOrderNo(orderNo);
				accuaryComparisonDetails.add(accuracyComparisonDetailDTO);
				accuracyComparisonDetailDTO = new AccuracyComparisonDetailDTO();
				accuracyComparisonDetailDTO.setComparisonParamId(PredictionAccuracyMetrics.PRESTO_TOP_INACCURATE_ACTUAL.getMetricsId());
				accuracyComparisonDetailDTO.setParameterValue(oosItemDTO.getWeeklyActualMovement());
				accuracyComparisonDetailDTO.setOrderNo(orderNo);
				accuaryComparisonDetails.add(accuracyComparisonDetailDTO);
				orderNo = orderNo + 1;
			}
		}

		int loopCount = 1;
		orderNo = 1;
		// inaccurate forecast client
		for (OOSItemDTO oosItemDTO : predictionAccuracyReportDTO.inAccurateItems) {
			if (loopCount > TOP_INACCURATE_FORECAST) {
				accuracyComparisonDetailDTO = new AccuracyComparisonDetailDTO();
				accuracyComparisonDetailDTO.setComparisonParamId(PredictionAccuracyMetrics.CLIENT_TOP_INACCURATE_NAME.getMetricsId());
				accuracyComparisonDetailDTO.setParameterValueText(oosItemDTO.getItemName());
				accuracyComparisonDetailDTO.setOrderNo(orderNo);
				accuaryComparisonDetails.add(accuracyComparisonDetailDTO);
				accuracyComparisonDetailDTO = new AccuracyComparisonDetailDTO();
				accuracyComparisonDetailDTO.setComparisonParamId(PredictionAccuracyMetrics.CLIENT_TOP_INACCURATE_FORECAST.getMetricsId());
				accuracyComparisonDetailDTO.setParameterValue(oosItemDTO.getWeeklyPredictedMovement());
				accuracyComparisonDetailDTO.setVersionType(VALUE_TYPE_PRESTO);
				accuracyComparisonDetailDTO.setOrderNo(orderNo);
				accuaryComparisonDetails.add(accuracyComparisonDetailDTO);
				accuracyComparisonDetailDTO = new AccuracyComparisonDetailDTO();
				accuracyComparisonDetailDTO.setComparisonParamId(PredictionAccuracyMetrics.CLIENT_TOP_INACCURATE_FORECAST.getMetricsId());
				accuracyComparisonDetailDTO.setParameterValue(oosItemDTO.getClientWeeklyPredictedMovement());
				accuracyComparisonDetailDTO.setVersionType(VALUE_TYPE_CLIENT);
				accuracyComparisonDetailDTO.setOrderNo(orderNo);
				accuaryComparisonDetails.add(accuracyComparisonDetailDTO);
				accuracyComparisonDetailDTO = new AccuracyComparisonDetailDTO();
				accuracyComparisonDetailDTO.setComparisonParamId(PredictionAccuracyMetrics.CLIENT_TOP_INACCURATE_ACTUAL.getMetricsId());
				accuracyComparisonDetailDTO.setParameterValue(oosItemDTO.getWeeklyActualMovement());
				accuracyComparisonDetailDTO.setOrderNo(orderNo);
				accuaryComparisonDetails.add(accuracyComparisonDetailDTO);
				orderNo = orderNo + 1;
			}
			loopCount = loopCount + 1;
		}
	}
	
	/**
	 * TO fill all item with respect to Page and Block in Block details Sheet
	 * @param workbook
	 * @param blockDetailsSheet
	 * @param itemList
	 * @param pageBlockMap
	 * @param adBlocks
	 */
	private void fillBlockDetailsSheet(XSSFWorkbook workbook, XSSFSheet blockDetailsSheet, List<OOSItemDTO> itemList,
			HashMap<AdKey, OOSItemDTO> pageBlockMap, HashMap<AdKey, WeeklyAdBlock> adBlocks) {
		ExcelStyle excelStyle = new ExcelStyle();
		XSSFCellStyle style = workbook.createCellStyle();
		style.setBorderTop(XSSFCellStyle.BORDER_THIN);
		style.setBorderBottom(XSSFCellStyle.BORDER_THIN);
		style.setBorderRight(XSSFCellStyle.BORDER_THIN);
		style.setBorderLeft(XSSFCellStyle.BORDER_THIN);
		int blockDetailSheetRow = 2;
		XSSFCellStyle style1 = (XSSFCellStyle) style.clone();
		excelStyle.indention = 2;
		style1.setIndention(excelStyle.indention);
		// To Arrange Page and Block number in ascending order...
		List<AdKey> adkeyList = new ArrayList<AdKey>();
		for (Map.Entry<AdKey, OOSItemDTO> entry : pageBlockMap.entrySet()) {
			adkeyList.add(entry.getKey());
		}
		// Sort the key values in ascending order..
		sortByAdKey(adkeyList);
		// Using Key list iterate the values.
		for (AdKey adKey : adkeyList) {
			boolean ignoreBlock = true;
			OOSItemDTO oosItemDTO = pageBlockMap.get(adKey);
			// AdKey adKey = entry.getKey();
			WeeklyAdBlock block = adBlocks.get(adKey);
			// Exclude blocks where total no of items are not matching with
			// actual total no of items(i.e. ad plex feed)
			if (block != null && checkItemsCodesAreEqual(actualItemsMap, adplexItemsMap, adKey)) {
				ignoreBlock = false;
			}
			XSSFRow row;
			if (blockDetailsSheet.getRow(blockDetailSheetRow) == null) {
				row = blockDetailsSheet.createRow(blockDetailSheetRow);
			} else {
				row = blockDetailsSheet.getRow(blockDetailSheetRow);
			}
			if (!ignoreBlock) {
				int colCount = -1;
				// Write page and block
				String pageAndBlockValue = adKey.getPageNumber() + "_" + adKey.getBlockNumber();
				setCellValue(workbook, row, ++colCount, pageAndBlockValue, excelStyle);
				row.getCell(colCount).setCellStyle(style);
				setCellValue(workbook, row, ++colCount, oosItemDTO.getWeeklyPredictedMovement(), excelStyle);
				row.getCell(colCount).setCellStyle(style);
				setCellValue(workbook, row, ++colCount, oosItemDTO.getWeeklyActualMovement(), excelStyle);
				row.getCell(colCount).setCellStyle(style);
				setCellValue(workbook, row, ++colCount, oosItemDTO.getClientWeeklyPredictedMovement(), excelStyle);
				row.getCell(colCount).setCellStyle(style);
				setCellValue(workbook, row, ++colCount, oosItemDTO.getWeeklyActualMovement(), excelStyle);
				row.getCell(colCount).setCellStyle(style);
				blockDetailSheetRow++;

				for (OOSItemDTO itemDTO : itemList) {
					ExcelStyle excelStyle1 = new ExcelStyle();
					excelStyle1.indention = 2;
					excelStyle1.cloneStyle = true;
					if (itemDTO.getAdPageNo() == adKey.getPageNumber()
							&& itemDTO.getBlockNo() == adKey.getBlockNumber()) {
						// Write all its items
						colCount = -1;
						XSSFRow row1;
						if (blockDetailsSheet.getRow(blockDetailSheetRow) == null) {
							row1 = blockDetailsSheet.createRow(blockDetailSheetRow);
						} else {
							row1 = blockDetailsSheet.getRow(blockDetailSheetRow);
						}
						setCellValue(workbook, row1, ++colCount, itemDTO.getItemName(), excelStyle1);
						row1.getCell(colCount).setCellStyle(style1);
						setCellValue(workbook, row1, ++colCount, itemDTO.getWeeklyPredictedMovement(), excelStyle);
						row1.getCell(colCount).setCellStyle(style);
						setCellValue(workbook, row1, ++colCount, itemDTO.getWeeklyActualMovement(), excelStyle);
						row1.getCell(colCount).setCellStyle(style);
						row1.createCell(++colCount).setCellStyle(style);
						row1.createCell(++colCount).setCellStyle(style);
						blockDetailSheetRow++;
					}
				}
			}
		}

	}

	/**
	 * Sets object value to cell
	 * 
	 * @param sheet
	 * @param rowNum
	 * @param cellNum
	 * @param value
	 */
	private void setCellValue(XSSFSheet sheet, int rowNum, int cellNum, Object value) {
		XSSFRow row = sheet.getRow(rowNum);
		XSSFCell cell = row.getCell(cellNum);
		if (value instanceof String) {
			cell.setCellValue(String.valueOf(value));
		} else if (value instanceof Double) {
			cell.setCellValue((Double) value);
		} else if (value instanceof Long) {
			cell.setCellValue((Long) value);
		} else if (value instanceof Integer) {
			cell.setCellValue((Integer) value);
		}
		cell.setCellStyle(cell.getCellStyle());
	}

	/**
	 * 
	 * @return mapping cells on the template
	 */
	private TreeMap<String, String> getAccuracyReportSheetFieldNames() {
		TreeMap<String, String> fields2 = new TreeMap<String, String>();

		fields2.put("2-3", "FROM_DATE");
		fields2.put("2-5", "TO_DATE");
		fields2.put("4-2", "TOTAL_ITEMS_DATA");

		fields2.put("5-2", "NO_OF_ITEMS_OR_BLOCKS_CONSIDERED");
		fields2.put("6-2", "ACT_MOV_GT_X1_UNITS");
		fields2.put("7-2", "ACT_MOV_BT_X1_X2_UNITS");
		fields2.put("8-2", "ACT_MOV_LT_X3_UNITS");

		fields2.put("9-2", "NO_OF_ITEMS_OR_BLOCKS_IGNORED");
		fields2.put("10-2", "NO_FORCST_ITEMS_PRESTO");
		fields2.put("11-2", "NO_FORCST_ITEMS_CLIENT");
		fields2.put("12-2", "ACT_MOV_ZERO");

		fields2.put("17-2", "RMSE_PRESTO");
		fields2.put("17-4", "RMSE_CLIENT");
		fields2.put("18-2", "RMSE_HIGH_MOVER_PRESTO");
		fields2.put("18-4", "RMSE_HIGH_MOVER_CLIENT");
		fields2.put("19-2", "RMSE_MEDIUM_MOVER_PRESTO");
		fields2.put("19-4", "RMSE_MEDIUM_MOVER_CLIENT");
		fields2.put("20-2", "RMSE_SLOW_MOVER_PRESTO");
		fields2.put("20-4", "RMSE_SLOW_MOVER_CLIENT");
		fields2.put("21-3", "AVG_ERR_PCT_PRESTO");
		fields2.put("21-5", "AVG_ERR_PCT_CLIENT");
		fields2.put("22-3", "AVG_ERR_HIGH_MOVER_PCT_PRESTO");
		fields2.put("22-5", "AVG_ERR_HIGH_MOVER_PCT_CLIENT");
		fields2.put("23-3", "AVG_ERR_MEDIUM_MOVER_PCT_PRESTO");
		fields2.put("23-5", "AVG_ERR_MEDIUM_MOVER_PCT_CLIENT");
		fields2.put("24-3", "AVG_ERR_SLOW_MOVER_PCT_PRESTO");
		fields2.put("24-5", "AVG_ERR_SLOW_MOVER_PCT_CLIENT");
		fields2.put("25-2", "FORCST_CLOSER_PRESTO");
		fields2.put("25-3", "FORCST_CLOSER_PCT_PRESTO");
		fields2.put("25-4", "FORCST_CLOSER_CLIENT");
		fields2.put("25-5", "FORCST_CLOSER_PCT_CLIENT");
		fields2.put("26-2", "FORCST_HIGHER_PRESTO");
		fields2.put("26-3", "FORCST_HIGHER_PCT_PRESTO");
		fields2.put("26-4", "FORCST_HIGHER_CLIENT");
		fields2.put("26-5", "FORCST_HIGHER_PCT_CLIENT");
		fields2.put("27-3", "AVG_HIGHER_FORCST_PCT_PRESTO");
		fields2.put("27-5", "AVG_HIGHER_FORCST_PCT_CLIENT");
		fields2.put("28-2", "FORCST_LOWER_PRESTO");
		fields2.put("28-3", "FORCST_LOWER_PCT_PRESTO");
		fields2.put("28-4", "FORCST_LOWER_CLIENT");
		fields2.put("28-5", "FORCST_LOWER_PCT_CLIENT");
		fields2.put("29-3", "AVG_LOWER_FORCST_PCT_PRESTO");
		fields2.put("29-5", "AVG_LOWER_FORCST_PCT_CLIENT");
		fields2.put("30-2", "FORCST_GT_ACT_X1_TIMES_PRESTO");
		fields2.put("30-3", "FORCST_GT_ACT_X1_TIMES_PCT_PRESTO");
		fields2.put("30-4", "FORCST_GT_ACT_X1_TIMES_CLIENT");
		fields2.put("30-5", "FORCST_GT_ACT_X1_TIMES_PCT_CLIENT");
		fields2.put("31-2", "FORCST_LT_ACT_X1_TIMES_PRESTO");
		fields2.put("31-3", "FORCST_LT_ACT_X1_TIMES_PCT_PRESTO");
		fields2.put("31-4", "FORCST_LT_ACT_X1_TIMES_CLIENT");
		fields2.put("31-5", "FORCST_LT_ACT_X1_TIMES_PCT_CLIENT");
		fields2.put("32-2", "FORCST_WITHIN_X1_PCT_TOT_PRESTO");
		fields2.put("32-3", "FORCST_WITHIN_X1_PCT_PCT_PRESTO");
		fields2.put("32-4", "FORCST_WITHIN_X1_PCT_TOT_CLIENT");
		fields2.put("32-5", "FORCST_WITHIN_X1_PCT_PCT_CLIENT");
		fields2.put("33-3", "MAX_FORCST_INCOR_PCT_PRESTO");
		fields2.put("33-5", "MAX_FORCST_INCOR_PCT_CLIENT");
		fields2.put("34-2", "NO_FORCST_ITEMS_PRESTO");
		fields2.put("34-3", "NO_FORCST_ITEMS_PCT_PRESTO");
		fields2.put("34-4", "NO_FORCST_ITEMS_CLIENT");
		fields2.put("34-5", "NO_FORCST_ITEMS_PCT_CLIENT");
		fields2.put("35-2", "BOTH_FORCST_HIGHER");
		fields2.put("35-3", "BOTH_FORCST_HIGHER_PCT");
		fields2.put("36-2", "BOTH_FORCST_LOWER");
		fields2.put("36-3", "BOTH_FORCST_LOWER_PCT");

		return fields2;
	}

	private TreeMap<String, String> getDetailsSheetFieldNames() {
		TreeMap<String, String> fields2 = new TreeMap<String, String>();

		fields2.put("1-14", "totNoOfAdCategories");
		fields2.put("2-14", "totNoOfAdItems");
		fields2.put("3-14", "totNoOfLIGs");
		fields2.put("4-14", "totNoOfNonLIGs");

		fields2.put("6-14", "totNoOfNonPerishablesAdCategories");
		fields2.put("7-14", "totNoOfNonPerishablesAdItems");
		fields2.put("8-14", "totNoOfNonPerishablesLIGs");
		fields2.put("9-14", "totNoOfNonPerishablesNonLIGs");

		fields2.put("11-14", "totNoOfNonPerishablesWith1LIGor1NonLigAdCategories");
		fields2.put("12-14", "totNoOfNonPerishablesWith1LIGor1NonLigAdItems");
		fields2.put("13-14", "totNoOfNonPerishablesWith1LIGor1NonLigLIGs");
		fields2.put("14-14", "totNoOfNonPerishablesWith1LIGor1NonLigNonLIGs");
		fields2.put("15-14", "totNoOfNonPerishablesWith1LIGor1NonLigAdCategoriesWPred");
		fields2.put("16-14", "totNoOfNonPerishablesWith1LIGor1NonLigAdCategoriesWoPred");

		fields2.put("18-14", "totNoOfNonPerishablesAdCategoriesWPred");
		fields2.put("19-14", "totNoOfNonPerishablesAdItemsWPred");
		fields2.put("20-14", "totNoOfNonPerishablesAdItemsWZeroPred");
		fields2.put("21-14", "totNoOfNonPerishablesLIGsWPred");
		fields2.put("22-14", "totNoOfNonPerishablesLIGsWZeroPred");
		fields2.put("23-14", "totNoOfNonPerishablesNonLIGsWPred");
		fields2.put("24-14", "totNoOfNonPerishablesNonLIGsWZeroPred");
		fields2.put("25-14", "totNoOfNonPerishablesAdCategoriesWOPred");
		fields2.put("26-14", "totNoOfNonPerishablesAdItemsWOPred");
		fields2.put("27-14", "totNoOfNonPerishablesLIGsWOPred");
		fields2.put("28-14", "totNoOfNonPerishablesNonLIGsWOPred");

		return fields2;
	}

	/**
	 * Sets string value to cell
	 * 
	 * @param row
	 * @param colCount
	 * @param value
	 * @param sheet
	 */
	private void setCellValue(XSSFWorkbook workbook, XSSFRow row, int colCount, Object value, ExcelStyle excelStyle) {
		
		XSSFCell cell;
		if(row.getCell(colCount) == null){
			cell = row.createCell(colCount);
		}else{
			cell = row.getCell(colCount);
		}

		if (excelStyle.cloneStyle) {
			XSSFCellStyle cellStyle = workbook.createCellStyle();
			cellStyle = (XSSFCellStyle) cellStyle.clone();
			cellStyle.setIndention(excelStyle.indention);
			cellStyle.setBorderLeft(cell.getCellStyle().getBorderLeft());
			cellStyle.setBorderRight(cell.getCellStyle().getBorderRight());
			cell.setCellStyle(cellStyle);
		}

		if (value instanceof String) {
			cell.setCellValue(String.valueOf(value));
		} else if (value instanceof Double) {
			cell.setCellValue((Double) value);
		} else if (value instanceof Long) {
			cell.setCellValue((Long) value);
		} else if (value instanceof Integer) {
			cell.setCellValue((Integer) value);
		}
	}

	/**
	 * calls mail service and sends mail with produced excel report
	 * 
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
		String subject = "Weekly " + foreCastType + " for " + inputLocationName + " for WE " + weekEndingDateStr;
		List<String> fileList = new ArrayList<String>();
		fileList.add(file);
		String mailContent = "Attached file has Weekly " + foreCastType + " for " + inputLocationName + " for week ending " + weekEndingDateStr
				+ " \n \n" + Constants.MAIL_CONFIDENTIAL_NOTE;
		mailStatus = EmailService.sendEmail(from, oosEmailAlertDTO.getMailToList(), oosEmailAlertDTO.getMailCCList(),
				oosEmailAlertDTO.getMailBCCList(), subject, mailContent, fileList);
		return mailStatus;
	}
	/**
	 * Get the accuracy report list by passing Rfactor values 
	 * @param itemNames
	 * @param forecastPresto
	 * @param actualMov
	 * @param forecastClient
	 * @return
	 */
	private List<OOSItemDTO> getAccuracyReportList(RFactor itemNames, RFactor forecastPresto, RFactor actualMov,
			RFactor forecastClient) {
		List<OOSItemDTO> itemList = new ArrayList<>();

		for (int i = 0; i < itemNames.size(); i++) {
			OOSItemDTO oosItemDTO = new OOSItemDTO();
			oosItemDTO.setItemName(itemNames.at(i));
			if (!forecastPresto.at(i).contains("NA")) {
				oosItemDTO.setWeeklyPredictedMovement(Long.parseLong(forecastPresto.at(i).replaceAll(",", "").trim()));
			} else {
				oosItemDTO.setWeeklyPredictedMovement(0);
			}
			if (!actualMov.at(i).contains("NA")) {
				oosItemDTO.setWeeklyActualMovement(Long.parseLong(actualMov.at(i).replaceAll(",", "").trim()));
			} else {
				oosItemDTO.setWeeklyActualMovement(0);
			}
			if (!forecastClient.at(i).contains("NA")) {
				oosItemDTO.setClientWeeklyPredictedMovement(
						Long.parseLong(forecastClient.at(i).replaceAll(",", "").trim()));
			} else {
				oosItemDTO.setClientWeeklyPredictedMovement(0);
			}

			itemList.add(oosItemDTO);
		}
		return itemList;

	}
	
	/**
	 * To sort the Ad key values in ascending order..
	 * @param adKey
	 */
	public void sortByAdKey(List<AdKey> adKey) {

		Collections.sort(adKey, new Comparator<AdKey>() {
			public int compare(final AdKey adKey1, final AdKey adKey2) {
				int compare = 0;
				try {
					if (adKey1 == null && adKey2 == null) {
						compare = 0;
					} else if (adKey1 == null && adKey2 != null) {
						compare = -1;
					} else if (adKey2 != null && adKey1 == null) {
						compare = 1;
					} else {
						compare = adKey1.compareTo(adKey2);
					}
				} catch (Exception ex) {
					logger.error("Error while executing sortByAdKey() ");
					try {
						throw new GeneralException("Error in sortByAdKey()", ex);
					} catch (GeneralException e1) {
						e1.printStackTrace();
					}
				}
				return compare;
			}
		});
	}
	/**
	 * To compare the Retailer item code and Return boolean value
	 * @param actualItemsMap
	 * @param adplexItemsMap
	 * @param adKey
	 * @return
	 */
	public boolean checkItemsCodesAreEqual(HashMap<AdKey, List<String>> actualItemsMap, HashMap<AdKey, List<String>> adplexItemsMap,
			AdKey adKey) {
		boolean itemCodesAreEqual = true;
		List<String> actualItems = actualItemsMap.get(adKey);
		List<String> adPlexItems = adplexItemsMap.get(adKey);
		for (String actualItem : actualItems) {
			// Actual retailer item code and Ad_plex retailer item code needs to
			// be matched for each page and Block combination
			// else change ignore block value as false..
			if (!(adPlexItems.contains(actualItem))) {
				itemCodesAreEqual = false;
				break;
			}
		}
		return itemCodesAreEqual;

	}
}

class ExcelStyle {
	public short indention = 0;
	public boolean cloneStyle = false;
}
