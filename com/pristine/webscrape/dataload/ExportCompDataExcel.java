package com.pristine.webscrape.dataload;

import java.io.FileOutputStream;
import java.io.FileWriter;
import java.io.IOException;
import java.io.PrintWriter;
import java.sql.Connection;
import java.text.DateFormat;
import java.text.DecimalFormat;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Calendar;
import java.util.Date;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.TreeMap;

import org.apache.log4j.Logger;
import org.apache.log4j.PropertyConfigurator;
import org.apache.poi.hssf.usermodel.HSSFDataFormat;
import org.apache.poi.ss.usermodel.Cell;
import org.apache.poi.xssf.usermodel.XSSFCell;
import org.apache.poi.xssf.usermodel.XSSFCellStyle;
import org.apache.poi.xssf.usermodel.XSSFColor;
import org.apache.poi.xssf.usermodel.XSSFDataFormat;
import org.apache.poi.xssf.usermodel.XSSFFont;
import org.apache.poi.xssf.usermodel.XSSFRow;
import org.apache.poi.xssf.usermodel.XSSFSheet;
import org.apache.poi.xssf.usermodel.XSSFWorkbook;

import com.pristine.dao.CompetitiveDataDAO;
import com.pristine.dao.DBManager;
import com.pristine.dto.CompStoreItemDetailsKey;
import com.pristine.dto.CompetitiveDataDTO;
import com.pristine.exception.GeneralException;
import com.pristine.parsinginterface.ExcelFileParser;
import com.pristine.service.offermgmt.PricingEngineService;
import com.pristine.util.Constants;
import com.pristine.util.DateUtil;
import com.pristine.util.PrestoUtil;
import com.pristine.util.PristineDBUtil;
import com.pristine.util.PropertyManager;
import com.pristine.util.offermgmt.PRCommonUtil;

public class ExportCompDataExcel {
	private static Logger logger = Logger.getLogger("ExportCompDataExcel");
	private final static String INPUT_FOLDER = "INPUT_FOLDER=";
	private final static String OUTPUT_FOLDER = "OUTPUT_FOLDER=";
	DateFormat appDateFormatter = new SimpleDateFormat(Constants.APP_DATE_FORMAT);
	private final static String STORE_NAME = "STORE_NAME=";
	private static String storeName;
	private static String startDate = null;
	static Connection conn = null;
	PricingEngineService pricingEngineService = new PricingEngineService();
	static String outFolder = null;
	static String inFolder = null;
	List<CompetitiveDataDTO> CompDataList;
	String rootPath = PropertyManager.getProperty("DATALOAD.ROOTPATH", "");
	DecimalFormat df = new DecimalFormat("####0.00");
	List<String> missingUPC = new ArrayList<String>();

	public ExportCompDataExcel() {
		try {
			conn = DBManager.getConnection();
		} catch (GeneralException var1_1) {
			logger.error("Error when creating connection - " + var1_1);
		}
	}

	public static void main(String[] args) throws ParseException {

		PropertyManager.initialize("analysis.properties");
		PropertyConfigurator.configure("log4j-exportRawCompDataExcel.properties");

		for (String arg : args) {
			if (arg.startsWith(INPUT_FOLDER)) {
				inFolder = arg.substring(INPUT_FOLDER.length());
			}
			if (arg.startsWith(OUTPUT_FOLDER)) {
				outFolder = arg.substring(OUTPUT_FOLDER.length());
			}
			if (arg.startsWith(STORE_NAME)) {
				storeName = arg.substring(STORE_NAME.length());
			}
		}

		// Default week type to current week if it is not specified

		Calendar c = Calendar.getInstance();
		int dayIndex = 0;
		if (PropertyManager.getProperty("CUSTOM_WEEK_START_INDEX") != null) {
			dayIndex = Integer.parseInt(PropertyManager.getProperty("CUSTOM_WEEK_START_INDEX"));
		}
		if (dayIndex > 0)
			c.set(Calendar.DAY_OF_WEEK, dayIndex + 1);
		else
			c.set(Calendar.DAY_OF_WEEK, Calendar.SUNDAY);
		DateFormat dateFormat = new SimpleDateFormat("MM/dd/yyyy");
		if (args.length > 2) {
			if (Constants.CURRENT_WEEK.equalsIgnoreCase(args[3])) {
				startDate = dateFormat.format(c.getTime());
			} else if (Constants.NEXT_WEEK.equalsIgnoreCase(args[3])) {
				c.add(Calendar.DATE, 7);
				startDate = dateFormat.format(c.getTime());
			} else if (Constants.LAST_WEEK.equalsIgnoreCase(args[3])) {
				c.add(Calendar.DATE, -7);
				startDate = dateFormat.format(c.getTime());
			} else if (Constants.SPECIFIC_WEEK.equalsIgnoreCase(args[3])) {
				try {
					String tempStartDate = DateUtil.getWeekStartDate(DateUtil.toDate(args[4]), 0);
					Date date = dateFormat.parse(tempStartDate);
					startDate = dateFormat.format(date);
					logger.info("Date:" + startDate);
				} catch (GeneralException exception) {
					logger.error("Error when parsing date - " + exception.toString());
					System.exit(-1);
				}
			}
		}

		logger.info("********************************************");
		ExportCompDataExcel exportCompData = new ExportCompDataExcel();
		try {
			exportCompData.processCompData();
		} catch (GeneralException e) {
			logger.error("Error in the exportCompData programs", e);
			e.printStackTrace();
		} finally {
			PristineDBUtil.close(conn);
		}
		logger.info("********************************************");
	}

	private void processCompData() throws GeneralException {
		CompetitiveDataDAO cometitiveDataDAO = new CompetitiveDataDAO(conn);
		String storeNumbers = null;
		if (storeName.equals("WEGMANS")) {
			storeNumbers = PropertyManager.getProperty("WEGMANS_STORE_LIST");
		} else if (storeName.equals("HANNAFORD")) {
			storeNumbers = PropertyManager.getProperty("HANNAFORDS_STORE_LIST");
		}
		// Get Raw data from excel files
		loadCompData();
		String fourWeekBeforeStartDate = DateUtil.getWeekStartDate(DateUtil.toDate(startDate), 4);
		logger.info("Get Raw Competitive data for last 4 weeks");
		HashMap<String, List<CompetitiveDataDTO>> rawCompDataMap = cometitiveDataDAO
				.getRawCompetitiveData(fourWeekBeforeStartDate, storeNumbers);
		// Group current week latest items using check date and create a key using
		// item,upc.
		logger.info("Processing the list to get Current items based on the Latest Date for each items");
		HashMap<String, CompetitiveDataDTO> storeDetails = cometitiveDataDAO.getCompStoreInfo();
		HashMap<CompStoreItemDetailsKey, CompetitiveDataDTO> currentWeekItemMap = getCurrentWeekItemMap();

		HashMap<CompStoreItemDetailsKey, CompetitiveDataDTO> prevHistoryItemMap = getPreviousHistoryItems(
				rawCompDataMap, storeDetails);
		updatePriceChangeInd(currentWeekItemMap, prevHistoryItemMap);
		writeCompDataIntoExcel(currentWeekItemMap);
		logger.info("Number of UPC not found in Previous weeks:" + missingUPC.size());
		logger.info("Items not found in the Previous month for UPC: "
				+ PRCommonUtil.getCommaSeperatedStringFromStrArray(missingUPC));
	}

	/**
	 * To generate Giant Eagle ad feed file
	 * 
	 * @param outFolder
	 * @param geAdFeedList
	 * @throws GeneralException
	 */
	private void writeCompDataIntoExcel(HashMap<CompStoreItemDetailsKey, CompetitiveDataDTO> currentWeekItemMap)
			throws GeneralException {
		logger.info("Processing CompData Records ...");
		String date = startDate.replace("/", "");
		String outputFileName = null;
		if (storeName.equals("WEGMANS")) {
			outputFileName = "Wegmans-butter-milk-and-eggs_" + date + ".xlsx";
		} else if (storeName.equals("HANNAFORD")) {
			outputFileName = "Hannaford-butter-milk-and-eggs_" + date + ".xlsx";
		}
		logger.info("Output File name - " + outputFileName);
		String outputPath = rootPath + "/" + outFolder + "/" + outputFileName;

		XSSFWorkbook workbook = new XSSFWorkbook();
		XSSFSheet feedSheet = workbook.createSheet("sheet0");
		List<CompetitiveDataDTO> competitiveDataDTOs = new ArrayList<CompetitiveDataDTO>();
		for (Map.Entry<CompStoreItemDetailsKey, CompetitiveDataDTO> entry : currentWeekItemMap.entrySet()) {
			competitiveDataDTOs.add(entry.getValue());
		}

		try {
			processCachedList(workbook, feedSheet, competitiveDataDTOs);
			competitiveDataDTOs = new ArrayList<>();
			FileOutputStream outputStream = new FileOutputStream(outputPath);
			workbook.write(outputStream);
			workbook.close();
			outputStream.close();
		} catch (CloneNotSupportedException | IOException e) {
			e.printStackTrace();
			throw new GeneralException("Error in writeCompDataIntoExcel()", e);
		}

	}

	/**
	 * Processes given list and generates a excel file
	 * 
	 * @throws GeneralException
	 * @throws CloneNotSupportedException
	 */
	private void processCachedList(XSSFWorkbook workbook, XSSFSheet feedSheet,
			List<CompetitiveDataDTO> competitiveDataDTOs) throws GeneralException, CloneNotSupportedException {
		HashMap<String, String> duplicatesList = new HashMap<String, String>();
		int currentRow = 0;
		writeHeader(workbook, feedSheet, currentRow);
		currentRow++;
		for (CompetitiveDataDTO competitiveDataDTO : competitiveDataDTOs) {
			int columnIndex = 0;
			XSSFRow row = feedSheet.createRow(currentRow);
			row.createCell(columnIndex++).setCellValue(competitiveDataDTO.storeName);
			row.createCell(columnIndex++).setCellValue(competitiveDataDTO.compStrNo);
			row.createCell(columnIndex++).setCellValue(competitiveDataDTO.checkDate);
			if (storeName.equals("WEGMANS")) {
				row.createCell(columnIndex++).setCellValue(competitiveDataDTO.itemcode);
			}
			if (competitiveDataDTO.itemName != null) {
				competitiveDataDTO.itemName = competitiveDataDTO.itemName.replace("'", "");
			}
			row.createCell(columnIndex++).setCellValue((competitiveDataDTO.itemName));
			row.createCell(columnIndex++).setCellValue(competitiveDataDTO.regMPack);
			// float value = Float.parseFloat(df.format(competitiveDataDTO.regPrice));
			XSSFCell cell1 = row.createCell(columnIndex++);
			cell1.setCellValue(new Double(df.format(round(competitiveDataDTO.regPrice, 2))));
			// cell1.setCellType(cell);
			row.createCell(columnIndex++).setCellValue(competitiveDataDTO.size);
			row.createCell(columnIndex++)
					.setCellValue(String.format("%12s", competitiveDataDTO.upc).replace(' ', '0').toUpperCase());
			row.createCell(columnIndex++).setCellValue(competitiveDataDTO.outSideRangeInd);
			row.createCell(columnIndex++).setCellValue(competitiveDataDTO.saleMPack);
			XSSFCell cell2 = row.createCell(columnIndex++);
			cell2.setCellValue(new Double(df.format(round(competitiveDataDTO.fSalePrice, 2))));
			// cell2.setCellType(Cell.CELL_TYPE_NUMERIC);
			row.createCell(columnIndex++).setCellValue(competitiveDataDTO.categoryName);
			if (storeName.equals("WEGMANS")) {
				row.createCell(columnIndex++).setCellValue(competitiveDataDTO.aisle);
			}
			row.createCell(columnIndex++).setCellValue(competitiveDataDTO.plFlag);
			row.createCell(columnIndex++).setCellValue(competitiveDataDTO.priceChangeInd);
			currentRow++;
		}

	}

	/**
	 * To get current week items based on the latest date available
	 * 
	 * @param rawCompDataMap
	 * @return
	 */
	private HashMap<CompStoreItemDetailsKey, CompetitiveDataDTO> getCurrentWeekItemMap() {
		HashMap<CompStoreItemDetailsKey, CompetitiveDataDTO> currentWeekItemMap = new HashMap<CompStoreItemDetailsKey, CompetitiveDataDTO>();
		// Sort list to get items based on most recent dates
		// pricingEngineService.sortByCheckDate(CompDataList);
		for (CompetitiveDataDTO competitiveDataDTO : CompDataList) {
			for (int i = competitiveDataDTO.upc.length(); i < 12; i++) {
				competitiveDataDTO.upc = "0" + competitiveDataDTO.upc;
			}
			logger.debug(competitiveDataDTO.compStrNo + " " + competitiveDataDTO.upc + " "
					+ String.valueOf(competitiveDataDTO.itemcode));
			CompStoreItemDetailsKey key = new CompStoreItemDetailsKey(competitiveDataDTO.compStrNo,
					competitiveDataDTO.upc, String.valueOf(competitiveDataDTO.itemcode));
			// Consider only Items which latest check date
			if (!(currentWeekItemMap.containsKey(key))) {
				currentWeekItemMap.put(key, competitiveDataDTO);
			}
		}
		return currentWeekItemMap;
	}

	/**
	 * To get the history latest items and its details
	 * 
	 * @param rawCompData
	 * @return
	 */
	private HashMap<CompStoreItemDetailsKey, CompetitiveDataDTO> getPreviousHistoryItems(
			HashMap<String, List<CompetitiveDataDTO>> rawCompData, HashMap<String, CompetitiveDataDTO> storeDetails) {
		HashMap<CompStoreItemDetailsKey, CompetitiveDataDTO> previousCompDataMap = new HashMap<CompStoreItemDetailsKey, CompetitiveDataDTO>();
		HashMap<String, CompetitiveDataDTO> itemsWithCheckDateAsKey = new HashMap<String, CompetitiveDataDTO>();
		List<CompetitiveDataDTO> compDatalist = new ArrayList<CompetitiveDataDTO>();
		// Get items from all the weeks in the list
		for (Map.Entry<String, List<CompetitiveDataDTO>> compDataMap : rawCompData.entrySet()) {
			for (CompetitiveDataDTO competitiveDataDTO : compDataMap.getValue()) {
				// To Skip the Current week data
				if (!competitiveDataDTO.weekStartDate.equals(startDate)) {
					compDatalist.add(competitiveDataDTO);
				}
			}
		}
		// Sort list to get items based on most recent dates
		pricingEngineService.sortByCheckDate(compDatalist);
		for (CompetitiveDataDTO competitiveDataDTO : compDatalist) {
			for (int i = competitiveDataDTO.upc.length(); i < 12; i++) {
				competitiveDataDTO.upc = "0" + competitiveDataDTO.upc;
			}
			// Change store number to actual Store Id
			if (storeDetails.containsKey(competitiveDataDTO.compStrNo)) {
				CompetitiveDataDTO competitiveDataDTO2 = storeDetails.get(competitiveDataDTO.compStrNo);
				competitiveDataDTO.compStrNo = competitiveDataDTO2.storeNo;
				competitiveDataDTO.storeName = competitiveDataDTO2.storeName;
				logger.debug(competitiveDataDTO.compStrNo + " " + competitiveDataDTO.upc + " "
						+ String.valueOf(competitiveDataDTO.itemcode));
				CompStoreItemDetailsKey itemKey = new CompStoreItemDetailsKey(competitiveDataDTO.compStrNo,
						competitiveDataDTO.upc, String.valueOf(competitiveDataDTO.itemcode));
				// Check item exiting in the Map else add..
				if (!(previousCompDataMap.containsKey(itemKey))) {
					previousCompDataMap.put(itemKey, competitiveDataDTO);
				}
			}
		}
		return previousCompDataMap;
	}

	private void updatePriceChangeInd(HashMap<CompStoreItemDetailsKey, CompetitiveDataDTO> currentWeekItemMap,
			HashMap<CompStoreItemDetailsKey, CompetitiveDataDTO> prevHistoryItemMap) {

		for (Map.Entry<CompStoreItemDetailsKey, CompetitiveDataDTO> entryMap : currentWeekItemMap.entrySet()) {
			CompetitiveDataDTO currentWeekItemDetails = entryMap.getValue();
			CompetitiveDataDTO prevHistoryItemDetail = new CompetitiveDataDTO();
			// Assign default Ind if the item is not available in the Previous week
			currentWeekItemDetails.priceChangeInd = "N";
			if (prevHistoryItemMap.containsKey(entryMap.getKey())) {
				prevHistoryItemDetail = prevHistoryItemMap.get(entryMap.getKey());
				// Check the Reg Price, Reg qty, Sale Price and Sale qty were same..
				if ((currentWeekItemDetails.regPrice == prevHistoryItemDetail.regPrice)
						&& (currentWeekItemDetails.regMPack == prevHistoryItemDetail.regMPack)
						&& (currentWeekItemDetails.fSalePrice == prevHistoryItemDetail.fSalePrice)
						&& (currentWeekItemDetails.saleMPack == prevHistoryItemDetail.saleMPack)) {
					currentWeekItemDetails.priceChangeInd = "N";
				}

				else if ((currentWeekItemDetails.regPrice != prevHistoryItemDetail.regPrice)
						|| (currentWeekItemDetails.regMPack != prevHistoryItemDetail.regMPack)
						|| (currentWeekItemDetails.fSalePrice != prevHistoryItemDetail.fSalePrice)
						|| (currentWeekItemDetails.saleMPack != prevHistoryItemDetail.saleMPack)) {
					currentWeekItemDetails.priceChangeInd = "Y";
				}
			} else {
				missingUPC.add(currentWeekItemDetails.upc);
			}

		}

	}

	private void loadCompData() {
		ExcelFileParser parser = new ExcelFileParser();
		parser.setFirstRowToProcess(1); // skip header row
		ArrayList<String> fileList = parser.getFiles(inFolder);
		TreeMap<Integer, String> fieldNames = new TreeMap<Integer, String>();
		mapFieldNames(fieldNames);
		String file = null;
		CompDataList = new ArrayList<CompetitiveDataDTO>();
		try {
			for (String fileName : fileList) {
				file = fileName;
				logger.info("Processing file - " + fileName);
				List<CompetitiveDataDTO> rawList = parser.parseExcelFile(CompetitiveDataDTO.class, fileName, 0,
						fieldNames);
				PrestoUtil.moveFile(file, rootPath + "/" + inFolder + "/" + Constants.COMPLETED_FOLDER);
				CompDataList.addAll(rawList);
			}
		} catch (GeneralException ge) {
			logger.error("Error when parsing or processing input file", ge);
			PrestoUtil.moveFile(file, rootPath + "/" + inFolder + "/" + Constants.BAD_FOLDER);
		}
	}

	private void writeHeader(XSSFWorkbook workbook, XSSFSheet sheet, int currentRow) {
		int columnIndex = 0;
		XSSFColor lightGrey = new XSSFColor(new java.awt.Color(0, 0, 128));
		XSSFColor white = new XSSFColor(new java.awt.Color(255, 255, 255));
		XSSFCellStyle style = workbook.createCellStyle();
		XSSFFont font = workbook.createFont();
		font.setBold(true);
		font.setColor(white);
		style.setFont(font);
		style.setFillForegroundColor(lightGrey);
		style.setFillPattern(XSSFCellStyle.SOLID_FOREGROUND);
		style.setBorderTop(XSSFCellStyle.BORDER_THIN);
		style.setBorderBottom(XSSFCellStyle.BORDER_THIN);
		style.setBorderRight(XSSFCellStyle.BORDER_THIN);
		style.setBorderLeft(XSSFCellStyle.BORDER_THIN);
		XSSFRow row = sheet.createRow(currentRow);
		XSSFCell cell1 = row.createCell(columnIndex++);
		cell1.setCellValue("Store Name");
		cell1.setCellStyle(style);
		XSSFCell cell2 = row.createCell(columnIndex++);
		cell2.setCellValue("Comp Store Num");
		cell2.setCellStyle(style);
		XSSFCell cell3 = row.createCell(columnIndex++);
		cell3.setCellValue("Check Date");
		cell3.setCellStyle(style);
		if (storeName.equals("WEGMANS")) {
			XSSFCell cell4 = row.createCell(columnIndex++);
			cell4.setCellValue("Item Code");
			cell4.setCellStyle(style);
		}
		XSSFCell cell5 = row.createCell(columnIndex++);
		cell5.setCellValue("Item Desc");
		cell5.setCellStyle(style);
		XSSFCell cell6 = row.createCell(columnIndex++);
		cell6.setCellValue("Reg Quantity");
		cell6.setCellStyle(style);
		XSSFCell cell7 = row.createCell(columnIndex++);
		cell7.setCellValue("Reg Price");
		cell7.setCellStyle(style);
		XSSFCell cell8 = row.createCell(columnIndex++);
		cell8.setCellValue("Size");
		cell8.setCellStyle(style);
		XSSFCell cell9 = row.createCell(columnIndex++);
		cell9.setCellValue("UPC");
		cell9.setCellStyle(style);
		XSSFCell cell10 = row.createCell(columnIndex++);
		cell10.setCellValue("Outside Indicator");
		cell10.setCellStyle(style);
		XSSFCell cell11 = row.createCell(columnIndex++);
		cell11.setCellValue("Sale Quantity");
		cell11.setCellStyle(style);
		XSSFCell cell12 = row.createCell(columnIndex++);
		cell12.setCellValue("Sale Price");
		cell12.setCellStyle(style);
		XSSFCell cell13 = row.createCell(columnIndex++);
		cell13.setCellValue("Category");
		cell13.setCellStyle(style);
		if (storeName.equals("WEGMANS")) {
			XSSFCell cell14 = row.createCell(columnIndex++);
			cell14.setCellValue("Aisle");
			cell14.setCellStyle(style);
		}
		XSSFCell cell15 = row.createCell(columnIndex++);
		cell15.setCellValue("PL Flag");
		cell15.setCellStyle(style);
		XSSFCell cell16 = row.createCell(columnIndex++);
		cell16.setCellValue("Price Change Ind");
		cell16.setCellStyle(style);

		// row.createCell(columnIndex++).setCellValue("Comp Store Num");
		// row.createCell(columnIndex++).setCellValue("Check Date");
		// row.createCell(columnIndex++).setCellValue("");
		// row.createCell(columnIndex++).setCellValue("");
		// row.createCell(columnIndex++).setCellValue("");
		// row.createCell(columnIndex++).setCellValue("");
		// row.createCell(columnIndex++).setCellValue("");
		// row.createCell(columnIndex++).setCellValue("");
		// row.createCell(columnIndex++).setCellValue("");
		// row.createCell(columnIndex++).setCellValue("");
		// row.createCell(columnIndex++).setCellValue("");
		// row.createCell(columnIndex++).setCellValue("");
		// row.createCell(columnIndex++).setCellValue("");
		// row.createCell(columnIndex++).setCellValue("");

	}

	private void mapFieldNames(TreeMap<Integer, String> fieldNames) {
		if (storeName.equals("WEGMANS")) {
			fieldNames.put(0, "storeName");
			fieldNames.put(1, "compStrNo");
			fieldNames.put(2, "checkDate");
			fieldNames.put(3, "itemcode");
			fieldNames.put(4, "itemName");
			fieldNames.put(5, "regMPack");
			fieldNames.put(6, "regPrice");
			fieldNames.put(7, "size");
			fieldNames.put(8, "upc");
			fieldNames.put(9, "outSideRangeInd");
			fieldNames.put(10, "saleMPack");
			fieldNames.put(11, "fSalePrice");
			fieldNames.put(12, "categoryName");
			fieldNames.put(13, "aisle");
			fieldNames.put(14, "plFlag");
		} else if (storeName.equals("HANNAFORD")) {
			fieldNames.put(0, "storeName");
			fieldNames.put(1, "compStrNo");
			fieldNames.put(2, "checkDate");
			fieldNames.put(3, "itemcode");
			fieldNames.put(4, "itemName");
			fieldNames.put(5, "regMPack");
			fieldNames.put(6, "regPrice");
			fieldNames.put(7, "size");
			fieldNames.put(8, "upc");
			fieldNames.put(9, "outSideRangeInd");
			fieldNames.put(10, "saleMPack");
			fieldNames.put(11, "fSalePrice");
			fieldNames.put(12, "categoryName");
			fieldNames.put(13, "plFlag");
		}
	}

	public static double round(double value, int places) {
		if (places < 0)
			throw new IllegalArgumentException();

		long factor = (long) Math.pow(10, places);
		value = value * factor;
		long tmp = Math.round(value);
		return (double) tmp / factor;
	}
}
