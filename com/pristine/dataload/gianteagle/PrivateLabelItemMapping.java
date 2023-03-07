package com.pristine.dataload.gianteagle;

import java.io.FileOutputStream;
import java.io.IOException;
import java.sql.Connection;
import java.text.DateFormat;
import java.text.DecimalFormat;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Calendar;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.TreeMap;

import org.apache.log4j.Logger;
import org.apache.log4j.PropertyConfigurator;
import org.apache.poi.xssf.usermodel.XSSFCell;
import org.apache.poi.xssf.usermodel.XSSFCellStyle;
import org.apache.poi.xssf.usermodel.XSSFColor;
import org.apache.poi.xssf.usermodel.XSSFFont;
import org.apache.poi.xssf.usermodel.XSSFRow;
import org.apache.poi.xssf.usermodel.XSSFSheet;
import org.apache.poi.xssf.usermodel.XSSFWorkbook;

import com.pristine.dao.DBManager;
import com.pristine.dao.ItemDAO;
import com.pristine.dao.RetailCalendarDAO;
import com.pristine.dto.CompetitiveDataDTO;
import com.pristine.dto.ItemDetailKey;
import com.pristine.dto.ItemMappingDTO;
import com.pristine.dto.RetailCalendarDTO;
import com.pristine.exception.GeneralException;
import com.pristine.parsinginterface.ExcelFileParser;
import com.pristine.util.*;
import com.pristine.util.offermgmt.PRCommonUtil;

public class PrivateLabelItemMapping {
	private static Logger logger = Logger.getLogger("PrivateLabelItemMapping");
	private final static String INPUT_FOLDER = "INPUT_FOLDER=";
	private final static String OUTPUT_FOLDER = "OUTPUT_FOLDER=";
	private final static String STORE_NAME = "STORE_NAME=";
	// private static final boolean String = false;
	private String rootPath;
	private Connection conn = null;
	private static String relativeInputPath, relativeOutputPath;
	List<String> skippedItem;
	static String dateStr = null;
	static String storeName = null;
	private List<String> compStorePLNameList = new ArrayList<String>();
	private List<String> baseStorePLNameList = new ArrayList<String>();
	private HashMap<String, List<String>> plCompCatMap = new HashMap<String, List<String>>();
	HashMap<String, List<String>> categoriesNotMatching = new HashMap<String, List<String>>();

	private PrivateLabelItemMapping() {
		rootPath = PropertyManager.getProperty("DATALOAD.ROOTPATH", "");
		try {
			conn = DBManager.getConnection();
		} catch (GeneralException gex) {
			logger.error("Error when creating connection - " + gex);
		}
	}

	public static void main(String[] args) {
		PropertyConfigurator.configure("log4j-pl-item-mapping.properties");
		PropertyManager.initialize("analysis.properties");
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
		for (String arg : args) {
			if (arg.startsWith(INPUT_FOLDER)) {
				relativeInputPath = arg.substring(INPUT_FOLDER.length());
			} else if (arg.startsWith(OUTPUT_FOLDER)) {
				relativeOutputPath = arg.substring(OUTPUT_FOLDER.length());
			} else if (arg.startsWith(STORE_NAME)) {
				storeName = arg.substring(STORE_NAME.length()).toUpperCase().trim();
			}
			for (int ii = 0; ii < args.length; ii++) {
				String argu = args[ii];
				if (argu.startsWith("PROCESS_WEEK")) {
					String processWeek = arg.substring("PROCESS_WEEK=".length());
					if (Constants.CURRENT_WEEK.equalsIgnoreCase(processWeek)) {
						dateStr = dateFormat.format(c.getTime());
					} else if (Constants.NEXT_WEEK.equalsIgnoreCase(processWeek)) {
						c.add(Calendar.DATE, 7);
						dateStr = dateFormat.format(c.getTime());
					} else if (Constants.LAST_WEEK.equalsIgnoreCase(processWeek)) {
						c.add(Calendar.DATE, -7);
						dateStr = dateFormat.format(c.getTime());
					} else if (Constants.SPECIFIC_WEEK.equalsIgnoreCase(processWeek)) {
						try {
							String weekStart_date = null;
							for (int j = 0; j < args.length; j++) {
								String arg1 = args[j];
								if (arg1.startsWith("WEEK_START_DATE")) {
									weekStart_date = arg1.substring("WEEK_START_DATE=".length());
								}
							}
							dateStr = DateUtil.getWeekStartDate(DateUtil.toDate(weekStart_date), 0);
						} catch (GeneralException exception) {
							logger.error("Error when parsing date - " + exception.toString());
							System.exit(-1);
						}
					}
				}
			}
		}
		PrivateLabelItemMapping privateLabelItemMapping = new PrivateLabelItemMapping();
		try {
			privateLabelItemMapping.processItemMapping();
		} catch (GeneralException e) {
			e.printStackTrace();
		}
	}

	private void processItemMapping() throws GeneralException {
		RetailCalendarDAO retailCalendarDAO = new RetailCalendarDAO();
		RetailCalendarDTO calendarDTO = retailCalendarDAO.getCalendarId(conn, dateStr, Constants.CALENDAR_WEEK);
		logger.info("Calendar Id - " + calendarDTO.getCalendarId());
		int calendarId = calendarDTO.getCalendarId();
		ItemDAO itemDAO = new ItemDAO();
		plCompCatMap = itemDAO.getPLCompCatMapping(conn, storeName);
		List<CompetitiveDataDTO> compDataList = processFile();
		// Private labels based on Given Comp store
		if (storeName.toUpperCase().trim().equals("WALMART")) {
			addCompPrivateLabelNameInList("WALMART_PRIVATE_LABEL");
			addCompPrivateLabelNameInList("WALMART_BRAND");
			addCompPrivateLabelNameInList("WALMART_ORGANIC");
			addCompPrivateLabelNameInList("WALMART_TOP_CARE");
			addCompPrivateLabelNameInList("WALMART_ELECTRIX");
			addCompPrivateLabelNameInList("WALMAART_PAWS");
			addCompPrivateLabelNameInList("WALMART_TIPPY_TOES");
			addCompPrivateLabelNameInList("WALMART_VALUE_TIME");
			
		} else if (storeName.toUpperCase().trim().equals("KROGER")) {
			addCompPrivateLabelNameInList("KROGER_PRIVATE_LABEL");
			addCompPrivateLabelNameInList("KROGER_PRIVATE_SELECTION");
			addCompPrivateLabelNameInList("KROGER_SIMPLE_TRUTH");
			addCompPrivateLabelNameInList("KROGER_BRAND");
			addCompPrivateLabelNameInList("KROGER_ORGANIC");
			addCompPrivateLabelNameInList("KROGER_ACADEMIX");
			addCompPrivateLabelNameInList("KROGER_ELECTRIX");
			addCompPrivateLabelNameInList("KROGER_PAWS");
			addCompPrivateLabelNameInList("KROGER_TIPPY_TOES");
		}

		if (PropertyManager.getProperty("GIANT_EAGLE_PRIVATE_LABEL") != null) {
			addBasePrivateLabelNameInList("GIANT_EAGLE_PRIVATE_LABEL");
			addBasePrivateLabelNameInList("GIANT_EAGLE_NATURES_BASEKET_PL");
			addBasePrivateLabelNameInList("GIANT_EAGLE_MARKET_DISTRICT");
			addBasePrivateLabelNameInList("GIANT_EAGLE_NATURES_BASEKET_ORGANIC");
			addBasePrivateLabelNameInList("GIANT_EAGLE_TOP_CARE");
			addBasePrivateLabelNameInList("GIANT_EAGLE_ACADEMIX");
			addBasePrivateLabelNameInList("GIANT_EAGLE_ELECTRIX");
			addBasePrivateLabelNameInList("GIANT_EAGLE_PAWS");
			addBasePrivateLabelNameInList("GIANT_EAGLE_TIPPY_TOES");
			addBasePrivateLabelNameInList("GIANT_EAGLE_VALUE_TIME");
		}
		List<CompetitiveDataDTO> plCompDataList = getPLItemFromCompData(compDataList);
		HashMap<ItemDetailKey, ItemMappingDTO> basePLItems = new HashMap<ItemDetailKey, ItemMappingDTO>();
		HashMap<ItemDetailKey, List<CompetitiveDataDTO>> compPLItems = new HashMap<ItemDetailKey, List<CompetitiveDataDTO>>();
		HashMap<String, List<ItemMappingDTO>> baseItemDetails = itemDAO.getBaseItemDetails(conn, calendarId);
		if (plCompDataList.size() > 0 && baseItemDetails != null) {
			logger.info("Base items processing started..");
			baseItemDetails.forEach((category, itemDetails) -> {
				try {
					logger.info("Processing for category: " + category);
					getCompPriceForEachItems(plCompDataList, itemDetails, basePLItems, compPLItems);
					HashMap<ItemDetailKey, List<CompetitiveDataDTO>> filteredCompPLItems = removeLessMatchingCompItems(compPLItems);
					exportItemsInExcel(basePLItems, filteredCompPLItems);
				} catch (Exception | GeneralException e) {
					e.printStackTrace();
					logger.equals("Private label mapping got some errors." + e);
				}
			});

		}
		// To find the categories were item names are matching but categories are matching
		if (categoriesNotMatching.size() > 0) {
			categoriesNotMatching.forEach((key, value) -> {
				String listOfCat = PRCommonUtil.getCommaSeperatedStringFromStrArray(value);
				logger.error("Categories which can be mapped manually for Base category: " + key + " With Comp categories: " + listOfCat);
			});
		}
	}

	private List<String> getSplittedWordsFromProperties(String propertyName) {
		List<String> tempList = new ArrayList<String>();
		String storePLName = PropertyManager.getProperty(propertyName);
		String[] storePl = storePLName.split(",");
		for (int i = 0; i < storePl.length; i++) {
			tempList.add(storePl[i].toUpperCase().trim());
		}
		return tempList;
	}

	private List<CompetitiveDataDTO> processFile() throws GeneralException {
		List<CompetitiveDataDTO> compDataList = new ArrayList<CompetitiveDataDTO>();
		ExcelFileParser parser = new ExcelFileParser();
		String inputPath = rootPath + "/" + relativeInputPath;
		parser.setFirstRowToProcess(2); // skip header row
		TreeMap<Integer, String> fieldNames = new TreeMap<Integer, String>();
		mapFieldNames(fieldNames);
		ArrayList<String> fileList = parser.getFiles(relativeInputPath);
		if (fileList.size() == 0) {
			logger.error("No comp data file available to process private label item mapping");
		}
		String fileName = null;
		try {
			for (String processigFile : fileList) {
				logger.info("Processing file - " + processigFile);
				fileName = processigFile;
				compDataList = parser.parseExcelFile(CompetitiveDataDTO.class, fileName, 0, fieldNames);
				PrestoUtil.moveFile(fileName, inputPath + "/" + Constants.COMPLETED_FOLDER);
			}
		} catch (GeneralException ge) {
			logger.error("Error when parsing or processing input file", ge);
			PrestoUtil.moveFile(fileName, inputPath + "/" + Constants.BAD_FOLDER);
		}
		return compDataList;
	}

	private void mapFieldNames(TreeMap<Integer, String> fieldNames) {

		if (storeName.toUpperCase().trim().equals("WALMART")) {
			fieldNames.put(1, "storeName");
			fieldNames.put(2, "upc");
			fieldNames.put(3, "itemName");
			fieldNames.put(4, "size");
			fieldNames.put(5, "uom");
			fieldNames.put(6, "deptName");
			fieldNames.put(7, "categoryName");
			fieldNames.put(8, "regPrice");
			fieldNames.put(9, "fSalePrice");
		} else if (storeName.toUpperCase().trim().equals("KROGER")) {
			fieldNames.put(1, "upc");
			fieldNames.put(2, "itemName");
			fieldNames.put(3, "size");
			fieldNames.put(4, "deptName");
			fieldNames.put(5, "categoryName");
			fieldNames.put(6, "subCategory");
			fieldNames.put(7, "regPrice");
			fieldNames.put(8, "fSalePrice");
		}

	}

	// To get Comp details using Base items
	private void getCompPriceForEachItems(List<CompetitiveDataDTO> plCompDataList, List<ItemMappingDTO> categoryBasedItems,
			HashMap<ItemDetailKey, ItemMappingDTO> basePLItems, HashMap<ItemDetailKey, List<CompetitiveDataDTO>> compPLItems)
			throws GeneralException, CloneNotSupportedException {

		// Iterate each item to find the comp price...
		for (ItemMappingDTO itemDetail : categoryBasedItems) {
			String baseItemName;
			if (itemDetail.getBaseItemFriendlyName() != null && !itemDetail.getBaseItemFriendlyName().isEmpty()) {
				baseItemName = itemDetail.getBaseItemFriendlyName().toUpperCase().replace("-", " ").replace(":", "").replace(",", "").replace("&", "")
						.replace("/", " ");
			} else {
				baseItemName = itemDetail.getBaseItemName().toUpperCase().replace("-", " ").replace(":", "").replace(",", "").replace("&", "")
						.replace("/", " ");
			}

			// baseItemName = itemDetail.getBaseItemName().toUpperCase().replace("-", " ").replace(":", "")
			// .replace(",", "").replace("&", "");

			String[] splittedItemName = baseItemName.split("\\b+");
			boolean processItem = false;
			List<String> splittedNames = new ArrayList<String>();
			// To check the given base item is private label item which needs to be processed or not
			for (int i = 0; i < splittedItemName.length; i++) {
				if (!(splittedItemName[i].toUpperCase().trim().replace(" ", "").isEmpty())) {
					splittedNames.add(splittedItemName[i].toUpperCase().trim());
				}
			}
			// Condition to check the Base item is private label item
			for (String plName : baseStorePLNameList) {
				if (baseItemName.contains(plName) && baseItemName.startsWith(plName)) {
					processItem = true;
				}
			}

			if (processItem) {

				List<CompetitiveDataDTO> filteredPLCompItemList = new ArrayList<CompetitiveDataDTO>();
				// filteredPLCompItemList =getCompItemsBasedOnBasePLItems(plCompDataList, baseItemName);
				if (storeName.toUpperCase().trim().equals("KROGER")) {
					filteredPLCompItemList = getKrogerItemsBasedOnBasePLItems(plCompDataList, baseItemName);
				} else {
					filteredPLCompItemList = getWalmartItemsBasedOnBasePLItems(plCompDataList, baseItemName);
				}
				List<CompetitiveDataDTO> tempCompItems = new ArrayList<CompetitiveDataDTO>();
				logger.info("Processing for Base item: " + itemDetail.getBaseItemName());
				List<CompetitiveDataDTO> compDataBasedOnCategory = getCompDataBasedOnCategory(filteredPLCompItemList, itemDetail);
				double itemSize = itemDetail.getBaseSize();

				ItemDetailKey key = new ItemDetailKey(itemDetail.getUpc(), itemDetail.getRetailerItemCode());

				getItemBasedOnSize(tempCompItems, compDataBasedOnCategory, itemSize, 10, 10);

				// Walmart may have lesser price comparing to base items. So consider items whose price is lesser than 40%
				List<CompetitiveDataDTO> itemsBasedOnPrice;
				if (storeName.toUpperCase().trim().equals("WALMART")) {
					itemsBasedOnPrice = getItemBasedOnRegPrice(tempCompItems, itemDetail.getBaseRegPrice(), 50, 20);
				} else {
					itemsBasedOnPrice = getItemBasedOnRegPrice(tempCompItems, itemDetail.getBaseRegPrice(), 40, 20);
				}

				// Remove private label name from Base Item name
				for (String plName : baseStorePLNameList) {
					if (baseItemName.startsWith(plName)) {
						baseItemName = baseItemName.toUpperCase().trim().replace(plName, "");
						break;
					}
				}
				getCompItemsBasedOnName(itemsBasedOnPrice, baseItemName, compPLItems, key);

				// Get Comp item name Score based on base item
				getCompItemNameScore(baseItemName, compPLItems, key);

				// Check keywords were present in Comp item and add addl details
				checkKeywordsMatch(baseItemName, compPLItems, key);

				// If comp items were available then add base item into Map
				if (compPLItems.containsKey(key)) {
					basePLItems.put(key, itemDetail);
				}
			}

		}
	}

	// Based on Private label item choose Comp private label items
	private List<CompetitiveDataDTO> getKrogerItemsBasedOnBasePLItems(List<CompetitiveDataDTO> plCompDataList, String baseItemName)
			throws CloneNotSupportedException {
		List<CompetitiveDataDTO> newPLCompDataList = new ArrayList<CompetitiveDataDTO>();

		boolean isPLItemFound = false;
		// Looping each Base private label items

		// TOP Care
		if (!isPLItemFound) {
			List<String> storePLName = getSplittedWordsFromProperties("GIANT_EAGLE_TOP_CARE");
			for (String plName : storePLName) {
				// Base item name has given pl name then process below steps
				if (!isPLItemFound && baseItemName.contains(plName)) {
					// matching with comp pl name based on Base PL name
					List<String> compStorePLName = getSplittedWordsFromProperties("KROGER_BRAND");
					getCompItemBasedOnPLNames(compStorePLName, plCompDataList, newPLCompDataList);
					isPLItemFound = true;
				}
			}
		}

		// Value time
		if (!isPLItemFound) {
			List<String> storePLName = getSplittedWordsFromProperties("GIANT_EAGLE_VALUE_TIME");
			for (String plName : storePLName) {
				// Base item name has given pl name then process below steps
				if (!isPLItemFound && baseItemName.contains(plName)) {
					// matching with comp pl name based on Base PL name
					List<String> compStorePLName = getSplittedWordsFromProperties("KROGER_VALUE_TIME");
					getCompItemBasedOnPLNames(compStorePLName, plCompDataList, newPLCompDataList);
					isPLItemFound = true;
				}
			}
		}

		// TIPPY_TOES
		if (!isPLItemFound) {
			List<String> storePLName = getSplittedWordsFromProperties("GIANT_EAGLE_TIPPY_TOES");
			for (String plName : storePLName) {
				// Base item name has given pl name then process below steps
				if (!isPLItemFound && baseItemName.contains(plName)) {
					// matching with comp pl name based on Base PL name
					List<String> compStorePLName = getSplittedWordsFromProperties("KROGER_TIPPY_TOES");
					getCompItemBasedOnPLNames(compStorePLName, plCompDataList, newPLCompDataList);
					isPLItemFound = true;
				}
			}
		}

		// PAWS
		if (!isPLItemFound) {
			List<String> storePLName = getSplittedWordsFromProperties("GIANT_EAGLE_PAWS");
			for (String plName : storePLName) {
				// Base item name has given pl name then process below steps
				if (!isPLItemFound && baseItemName.contains(plName)) {
					// matching with comp pl name based on Base PL name
					List<String> compStorePLName = getSplittedWordsFromProperties("KROGER_PAWS");
					getCompItemBasedOnPLNames(compStorePLName, plCompDataList, newPLCompDataList);
					isPLItemFound = true;
				}
			}
		}

		// ELECTRIX
		if (!isPLItemFound) {
			List<String> storePLName = getSplittedWordsFromProperties("GIANT_EAGLE_ELECTRIX");
			for (String plName : storePLName) {
				// Base item name has given pl name then process below steps
				if (!isPLItemFound && baseItemName.contains(plName)) {
					// matching with comp pl name based on Base PL name
					List<String> compStorePLName = getSplittedWordsFromProperties("KROGER_ELECTRIX");
					getCompItemBasedOnPLNames(compStorePLName, plCompDataList, newPLCompDataList);
					isPLItemFound = true;
				}
			}
		}

		// ACADEMIX
		if (!isPLItemFound) {
			List<String> storePLName = getSplittedWordsFromProperties("GIANT_EAGLE_ACADEMIX");
			for (String plName : storePLName) {
				// Base item name has given pl name then process below steps
				if (!isPLItemFound && baseItemName.contains(plName)) {
					// matching with comp pl name based on Base PL name
					List<String> compStorePLName = getSplittedWordsFromProperties("KROGER_ACADEMIX");
					getCompItemBasedOnPLNames(compStorePLName, plCompDataList, newPLCompDataList);
					isPLItemFound = true;
				}
			}
		}
		// If Base items were not found in above base PL items.
		if (!isPLItemFound) {
			List<String> storePLName = getSplittedWordsFromProperties("GIANT_EAGLE_MARKET_DISTRICT");
			// Looping each Base private label items
			for (String plName : storePLName) {
				// Base item name has given pl name then process below steps
				if (!isPLItemFound && baseItemName.contains(plName)) {
					// matching with comp pl name based on Base PL name
					List<String> compStorePLName = getSplittedWordsFromProperties("KROGER_PRIVATE_SELECTION");
					getCompItemBasedOnPLNames(compStorePLName, plCompDataList, newPLCompDataList);
					isPLItemFound = true;
				}
			}
		}

		// GIANT_EAGLE_ORGANIC
		if (!isPLItemFound) {
			List<String> storePLName = getSplittedWordsFromProperties("GIANT_EAGLE_NATURES_BASEKET_ORGANIC");
			// Looping each Base private label items
			for (String plName : storePLName) {
				// Base item name has given pl name then process below steps
				if (!isPLItemFound && baseItemName.contains(plName)) {
					// matching with comp pl name based on Base PL name
					List<String> compStorePLName = getSplittedWordsFromProperties("KROGER_ORGANIC");
					getCompItemBasedOnPLNames(compStorePLName, plCompDataList, newPLCompDataList);
					isPLItemFound = true;
				}
			}
		}

		if (!isPLItemFound) {
			List<String> naturesBasket = getSplittedWordsFromProperties("GIANT_EAGLE_NATURES_BASEKET_PL");
			// Looping each Base private label items
			for (String plName : naturesBasket) {
				// Base item name has given pl name then process below steps
				if (!isPLItemFound && baseItemName.contains(plName)) {
					// matching with comp pl name based on Base PL name
					List<String> compStorePLName = getSplittedWordsFromProperties("KROGER_SIMPLE_TRUTH");
					getCompItemBasedOnPLNames(compStorePLName, plCompDataList, newPLCompDataList);
					isPLItemFound = true;
				}
			}
		}
		if (!isPLItemFound) {
			List<String> storePLName = getSplittedWordsFromProperties("GIANT_EAGLE_PRIVATE_LABEL");
			for (String plName : storePLName) {
				// Base item name has given pl name then process below steps
				if (!isPLItemFound && baseItemName.contains(plName)) {
					// matching with comp pl name based on Base PL name
					List<String> compStorePLName = getSplittedWordsFromProperties("KROGER_PRIVATE_LABEL");
					getCompItemBasedOnPLNames(compStorePLName, plCompDataList, newPLCompDataList);
					isPLItemFound = true;
				}
			}
		}
		return newPLCompDataList;
	}

	// Based on Private label item choose Comp private label items
	private List<CompetitiveDataDTO> getWalmartItemsBasedOnBasePLItems(List<CompetitiveDataDTO> plCompDataList, String baseItemName)
			throws CloneNotSupportedException {
		boolean isPLItemFound = false;
		List<CompetitiveDataDTO> newPLCompDataList = new ArrayList<CompetitiveDataDTO>();

		// Value Time
		if (!isPLItemFound) {
			List<String> storePLName = getSplittedWordsFromProperties("GIANT_EAGLE_VALUE_TIME");
			// Looping each Base private label items
			for (String plName : storePLName) {
				// Base item name has given pl name then process below steps
				if (!isPLItemFound && baseItemName.contains(plName)) {
					// matching with comp pl name based on Base PL name
					List<String> compStorePLName = getSplittedWordsFromProperties("WALMART_VALUE_TIME");
					getCompItemBasedOnPLNames(compStorePLName, plCompDataList, newPLCompDataList);
					isPLItemFound = true;
				}
			}
		}

		// TIPPY_TOES
		if (!isPLItemFound) {
			List<String> storePLName = getSplittedWordsFromProperties("GIANT_EAGLE_TIPPY_TOES");
			// Looping each Base private label items
			for (String plName : storePLName) {
				// Base item name has given pl name then process below steps
				if (!isPLItemFound && baseItemName.contains(plName)) {
					// matching with comp pl name based on Base PL name
					List<String> compStorePLName = getSplittedWordsFromProperties("WALMART_TIPPY_TOES");
					getCompItemBasedOnPLNames(compStorePLName, plCompDataList, newPLCompDataList);
					isPLItemFound = true;
				}
			}
		}

		// PAWS
		if (!isPLItemFound) {
			List<String> storePLName = getSplittedWordsFromProperties("GIANT_EAGLE_PAWS");
			// Looping each Base private label items
			for (String plName : storePLName) {
				// Base item name has given pl name then process below steps
				if (!isPLItemFound && baseItemName.contains(plName)) {
					// matching with comp pl name based on Base PL name
					List<String> compStorePLName = getSplittedWordsFromProperties("WALMAART_PAWS");
					getCompItemBasedOnPLNames(compStorePLName, plCompDataList, newPLCompDataList);
					isPLItemFound = true;
				}
			}
		}

		// ELECTRIX
		if (!isPLItemFound) {
			List<String> storePLName = getSplittedWordsFromProperties("GIANT_EAGLE_ELECTRIX");
			// Looping each Base private label items
			for (String plName : storePLName) {
				// Base item name has given pl name then process below steps
				if (!isPLItemFound && baseItemName.contains(plName)) {
					// matching with comp pl name based on Base PL name
					List<String> compStorePLName = getSplittedWordsFromProperties("WALMART_ELECTRIX");
					getCompItemBasedOnPLNames(compStorePLName, plCompDataList, newPLCompDataList);
					isPLItemFound = true;
				}
			}
		}

		// ACADEMIX
		if (!isPLItemFound) {
			List<String> storePLName = getSplittedWordsFromProperties("GIANT_EAGLE_ACADEMIX");
			// Looping each Base private label items
			for (String plName : storePLName) {
				// Base item name has given pl name then process below steps
				if (!isPLItemFound && baseItemName.contains(plName)) {
					// matching with comp pl name based on Base PL name
					List<String> compStorePLName = getSplittedWordsFromProperties("WALMART_BRAND");
					getCompItemBasedOnPLNames(compStorePLName, plCompDataList, newPLCompDataList);
					isPLItemFound = true;
				}
			}
		}

		// TOP CARE
		if (!isPLItemFound) {
			List<String> storePLName = getSplittedWordsFromProperties("GIANT_EAGLE_TOP_CARE");
			// Looping each Base private label items
			for (String plName : storePLName) {
				// Base item name has given pl name then process below steps
				if (!isPLItemFound && baseItemName.contains(plName)) {
					// matching with comp pl name based on Base PL name
					List<String> compStorePLName = getSplittedWordsFromProperties("WALMART_TOP_CARE");
					getCompItemBasedOnPLNames(compStorePLName, plCompDataList, newPLCompDataList);
					isPLItemFound = true;
				}
			}
		}

		// Natures Basket
		if (!isPLItemFound) {
			List<String> storePLName = getSplittedWordsFromProperties("GIANT_EAGLE_NATURES_BASEKET_PL");
			// Looping each Base private label items
			for (String plName : storePLName) {
				// Base item name has given pl name then process below steps
				if (!isPLItemFound && baseItemName.contains(plName)) {
					// matching with comp pl name based on Base PL name
					List<String> compStorePLName = getSplittedWordsFromProperties("WALMART_MARKET_DISTRICT_AND_NB");
					getCompItemBasedOnPLNames(compStorePLName, plCompDataList, newPLCompDataList);
					isPLItemFound = true;
				}
			}
		}

		// Market District
		if (!isPLItemFound) {
			List<String> storePLName = getSplittedWordsFromProperties("GIANT_EAGLE_MARKET_DISTRICT");
			// Looping each Base private label items
			for (String plName : storePLName) {
				// Base item name has given pl name then process below steps
				if (!isPLItemFound && baseItemName.contains(plName)) {
					// matching with comp pl name based on Base PL name
					List<String> compStorePLName = getSplittedWordsFromProperties("WALMART_MARKET_DISTRICT_AND_NB");
					getCompItemBasedOnPLNames(compStorePLName, plCompDataList, newPLCompDataList);
					isPLItemFound = true;
				}
			}
		}
		if (!isPLItemFound) {
			List<String> naturesBasket = getSplittedWordsFromProperties("GIANT_EAGLE_NATURES_BASEKET_ORGANIC");
			// Looping each Base private label items
			for (String plName : naturesBasket) {
				// Base item name has given pl name then process below steps
				if (!isPLItemFound && baseItemName.contains(plName)) {
					// matching with comp pl name based on Base PL name
					List<String> compStorePLName = getSplittedWordsFromProperties("WALMART_ORGANIC");
					getCompItemBasedOnPLNames(compStorePLName, plCompDataList, newPLCompDataList);
					isPLItemFound = true;
				}
			}
		}

		if (!isPLItemFound) {
			List<String> storePLName = getSplittedWordsFromProperties("GIANT_EAGLE_PRIVATE_LABEL");

			// Looping each Base private label items
			for (String plName : storePLName) {
				// Base item name has given pl name then process below steps
				if (!isPLItemFound && baseItemName.contains(plName)) {
					// matching with comp pl name based on Base PL name
					List<String> compStorePLName = getSplittedWordsFromProperties("WALMART_PRIVATE_LABEL");
					getCompItemBasedOnPLNames(compStorePLName, plCompDataList, newPLCompDataList);
					isPLItemFound = true;
				}
			}
		}
		return newPLCompDataList;
	}

	// Filter comp data based on category
	private List<CompetitiveDataDTO> getCompDataBasedOnCategory(List<CompetitiveDataDTO> plCompDataList, ItemMappingDTO categoryBasedItems) {
		List<CompetitiveDataDTO> categoryBasedCompData = new ArrayList<CompetitiveDataDTO>();
		List<CompetitiveDataDTO> filteredCompData = new ArrayList<CompetitiveDataDTO>();
		HashMap<ItemDetailKey, CompetitiveDataDTO> categoryBasedCompMap = new HashMap<ItemDetailKey, CompetitiveDataDTO>();
		List<String> splittedNames = new ArrayList<String>();
		List<String> splittedSubCatNames = new ArrayList<String>();

		String category = categoryBasedItems.getCategoryName().toUpperCase().replace("-", " ").replace(":", "").replace(",", "").replace("&", "")
				.replace("(", "").replace(")", "").replace("/", " ");
		String[] splittedCategory = category.split("\\b+");

		String subCategory = categoryBasedItems.getSubCatName().toUpperCase().replace("-", " ").replace(":", "").replace(",", "").replace("&", "")
				.replace("(", "").replace(")", "").replace("/", " ");

		String[] splittedSubCategory = subCategory.split("\\b+");
		// Include Department name in the list which needs to be considered.
		String department = categoryBasedItems.getDepartmentName().toUpperCase().replace("-", " ").replace(":", "").replace(",", "").replace("&", "")
				.replace("(", "").replace(")", "").replace("/", " ");
		String[] splittedDept = department.split("\\b+");
		for (int i = 0; i < splittedCategory.length; i++) {
			if (!(splittedCategory[i].toUpperCase().trim().replace(" ", "").isEmpty())) {
				splittedNames.add(splittedCategory[i].toUpperCase().trim());
				splittedSubCatNames.add(splittedCategory[i].toUpperCase().trim());
			}
		}
		for (int i = 0; i < splittedDept.length; i++) {
			if (!(splittedDept[i].toUpperCase().trim().replace(" ", "").isEmpty())) {
				splittedNames.add(splittedDept[i].toUpperCase().trim());
			}
		}
		for (int i = 0; i < splittedSubCategory.length; i++) {
			if (!(splittedSubCategory[i].toUpperCase().trim().replace(" ", "").isEmpty())) {
				splittedNames.add(splittedSubCategory[i].toUpperCase().trim());
				splittedSubCatNames.add(splittedSubCategory[i].toUpperCase().trim());
			}
		}

		// Add Categories which manually found to be a matching items(Added this logic to avoid skipping of items
		// which is comes under different category)
		if (plCompCatMap.containsKey(category)) {
			List<String> manualCategoryList = plCompCatMap.get(category);
			for (String catName : manualCategoryList) {
				String[] catNames = catName.split("\\b+");
				for (int i = 0; i < catNames.length; i++) {
					if (!(catNames[i].toUpperCase().trim().replace(" ", "").replace("&", "").isEmpty())) {
						splittedNames.add(catNames[i].toUpperCase().trim());
						splittedSubCatNames.add(catNames[i].toUpperCase().trim());
					}
				}
			}
		}
		// To find the Categories whose item name matches but Categories not matching with each other
		// for(String word: splittedNames){
		// HashMap<ItemDetailKey, List<CompetitiveDataDTO>> compPLItems = new HashMap<ItemDetailKey, List<CompetitiveDataDTO>>();
		//
		// ItemDetailKey tempKey = new ItemDetailKey(categoryBasedItems.getUpc(),categoryBasedItems.getRetailerItemCode());
		// String itemName = categoryBasedItems.getBaseItemName();
		// getCompItemsBasedOnName(plCompDataList,itemName,compPLItems,tempKey);
		// List<CompetitiveDataDTO> tempCompList = compPLItems.get(tempKey);
		// if(tempCompList != null){
		// tempCompList.stream()
		// .forEach(compItem ->{
		// List<String> compCatList = new ArrayList<String>();
		// if(categoriesNotMatching.containsKey(categoryBasedItems.getCategoryName())){
		// compCatList = categoriesNotMatching.get(categoryBasedItems.getCategoryName());
		// }
		// compCatList.add(compItem.categoryName);
		// Set<String> tempList = new HashSet<String>(compCatList);
		// List<String> arrayListCat = new ArrayList<String>(tempList);
		// categoriesNotMatching.put(categoryBasedItems.getCategoryName(), arrayListCat);
		// });
		// }
		// }
		for (String word : splittedNames) {
			// Filter comp items based on associates name(Dept, Category and Sub Category)
			plCompDataList.stream().filter(compItem -> (compItem.categoryName).toUpperCase().contains(word)).forEach(compItem -> {

				CompetitiveDataDTO competitiveDataDTO = new CompetitiveDataDTO();
				try {
					competitiveDataDTO = (CompetitiveDataDTO) compItem.clone();
					ItemDetailKey key = new ItemDetailKey(competitiveDataDTO.upc, competitiveDataDTO.retailerItemCode);
					categoryBasedCompMap.put(key, competitiveDataDTO);
				} catch (Exception e) {
					e.printStackTrace();
				}

			});

			// Filter comp details like Department or sub category for some items may not be matching with category name
			// Department
			plCompDataList.stream().filter(compItem -> (compItem.deptName).toUpperCase().contains(word)).forEach(compItem -> {
				CompetitiveDataDTO competitiveDataDTO = new CompetitiveDataDTO();
				try {
					competitiveDataDTO = (CompetitiveDataDTO) compItem.clone();
					ItemDetailKey key = new ItemDetailKey(competitiveDataDTO.upc, competitiveDataDTO.retailerItemCode);
					categoryBasedCompMap.put(key, competitiveDataDTO);
				} catch (Exception e) {
					e.printStackTrace();
				}
			});
			// Sub category
			for (CompetitiveDataDTO compItem : plCompDataList) {
				if (compItem.subCategory != null && !compItem.subCategory.isEmpty() && compItem.subCategory.toUpperCase().contains(word)) {
					CompetitiveDataDTO competitiveDataDTO = new CompetitiveDataDTO();
					try {
						competitiveDataDTO = (CompetitiveDataDTO) compItem.clone();
						ItemDetailKey key = new ItemDetailKey(competitiveDataDTO.upc, competitiveDataDTO.retailerItemCode);
						categoryBasedCompMap.put(key, competitiveDataDTO);
					} catch (Exception e) {
						e.printStackTrace();
					}
				}
			}
		}
		categoryBasedCompMap.forEach((key, value) -> {
			filteredCompData.add(value);
		});
		// To filter comp items based on Base item category and Sub category comparing with Comp category or sub category
		for (CompetitiveDataDTO competitiveDataDTO : filteredCompData) {
			String compCatAndSubCat = competitiveDataDTO.subCategory + " " + competitiveDataDTO.categoryName;
			if (storeName.toUpperCase().trim().equals("KROGER") && wordMatchingUtil(splittedSubCatNames, compCatAndSubCat)) {
				categoryBasedCompData.add(competitiveDataDTO);
			} else if (storeName.toUpperCase().trim().equals("WALMART")) {
				categoryBasedCompData.add(competitiveDataDTO);
			}
		}
		return categoryBasedCompData;

	}

	private boolean wordMatchingUtil(List<String> splittedBase, String compName) {
		boolean isWordMatching = false;
		List<String> splittedComp = getSplittedWordsAsList(compName);
		// String compItem = compName.toUpperCase().replace("-", " ").replace(":", "")
		// .replace(",", "").replace("&", "").replace("(", "").replace(")", "");
		// String[] splittedCompName = compItem.split("\\b+");
		//
		// for (int i = 0; i < splittedCompName.length; i++) {
		// if(!(splittedCompName[i].toUpperCase().trim().replace(" ", "").isEmpty())){
		// splittedComp.add(splittedCompName[i].toUpperCase().trim());
		// }
		// }
		// Filter by words b/w base and comp data
		for (String baseWord : splittedBase) {
			for (String compWord : splittedComp) {
				if (compWord.equals(baseWord)) {
					isWordMatching = true;
					break;
				} else if (compWord.length() >= 3) {
					// To match comp words with base item words by using sub string methods
					for (int i = compWord.length(); i >= 3; i--) {
						if (compWord.substring(0, i).equals(baseWord)) {
							isWordMatching = true;
							break;
						}
					}
				}
			}
		}
		return isWordMatching;

	}

	// To get Item list based on given Item size
	private void getItemBasedOnSize(List<CompetitiveDataDTO> tempCompItems, List<CompetitiveDataDTO> compDataList, double itemSize, int minDiff,
			int maxDiff) {
		logger.debug("Item Size:" + itemSize);
		// If Min and Max difference percent available then apply those values
		// to get comp items which comes within given range
		if (minDiff > 0 && maxDiff > 0) {
			double minItemSize = ((itemSize / 100) * (100 - minDiff));
			double maxItemSize = ((itemSize / 100) * (100 + maxDiff));
			// logger.debug("Min Item Size:" + minItemSize + " Max Item Size:" + maxItemSize);
			// text.replaceAll("[^0-9?!\\.]","") -> This will replace string char other digits and
			compDataList.stream().forEach(compDetail -> {
				if ((compDetail.size).replaceAll("[^0-9?!\\.]", "").trim() != null
						&& !((compDetail.size).replaceAll("[^0-9?!\\.]", "").trim().isEmpty())) {
					if (isDoubleValue(compDetail.size)) {

						// To calculate item size scoring based on Base item size with comp item size.
						if (Double.parseDouble((compDetail.size).replaceAll("[^0-9?!\\.]", "").trim()) >= minItemSize
								&& Double.parseDouble((compDetail.size).replaceAll("[^0-9?!\\.]", "").trim()) <= maxItemSize) {
							if (Double.parseDouble((compDetail.size).replaceAll("[^0-9?!\\.]", "").trim()) == itemSize) {
								compDetail.itemSizeScore = 2;
							} else if (Double.parseDouble((compDetail.size).replaceAll("[^0-9?!\\.]", "").trim()) >= minItemSize
									&& Double.parseDouble((compDetail.size).replaceAll("[^0-9?!\\.]", "").trim()) <= maxItemSize) {
								compDetail.itemSizeScore = 1;
							}
							// logger.debug("Base Size: " + itemSize + " comp Size: "
							// + Double.parseDouble((compDetail.size).replaceAll("[^0-9?!\\.]", "").trim()) + " itemSizeScore: "
							// + compDetail.itemSizeScore);
							compDetail.size = String.valueOf(Double.parseDouble((compDetail.size).replaceAll("[^0-9?!\\.]", "").trim()));
							tempCompItems.add(compDetail);
						}
					}
				}

			});
		} else {
			compDataList.stream().forEach(compDetail -> {
				if ((compDetail.size).replaceAll("[^0-9?!\\.]", "").replace(" ", "").trim() != null
						&& !(compDetail.size).replaceAll("[^0-9?!\\.]", "").replace(" ", "").trim().equals("")) {
					if (isDoubleValue(compDetail.size)) {
						if (Double.parseDouble((compDetail.size).replaceAll("[^0-9?!\\.]", "").trim()) == itemSize) {
							compDetail.size = String.valueOf(Double.parseDouble((compDetail.size).replaceAll("[^0-9?!\\.]", "").trim()));
							tempCompItems.add(compDetail);
						}
					}
				}
			});
		}
		// logger.info("No of filetered items found based on given size:" + tempCompItems.size());
	}

	private boolean isDoubleValue(String size) {
		boolean isDouble = false;
		try {
			Double.parseDouble((size).replaceAll("[^0-9?!\\.]", "").trim());
			isDouble = true;
		} catch (Exception e) {
			isDouble = false;
		}
		return isDouble;
	}

	private List<CompetitiveDataDTO> getItemBasedOnRegPrice(List<CompetitiveDataDTO> tempCompItems, double regPrice, int minDiff, int maxDiff) {
		List<CompetitiveDataDTO> itemsBasedOnSize = new ArrayList<CompetitiveDataDTO>();
		if (minDiff > 0 && maxDiff > 0) {

			// logger.debug("Base item Reg Price:" + regPrice);
			double minPriceDiff = ((regPrice / 100) * (100 - minDiff));
			double maxPriceDiff = ((regPrice / 100) * (100 + maxDiff));
			for (CompetitiveDataDTO compDetail : tempCompItems) {
				// logger.debug("Min Price:" + minPriceDiff + "Max Price:" + maxPriceDiff);
				// logger.debug("Comp Size:" + compDetail.regPrice);
				if (compDetail.regPrice >= minPriceDiff && compDetail.regPrice <= maxPriceDiff) {
					double minPrice = ((regPrice / 100) * (90));
					double maxPrice = ((regPrice / 100) * (110));
					// logger.debug("Base item Reg Price:"+regPrice+" Comp Item Price: "+compDetail.regPrice);
					// if(compDetail.regPrice == regPrice){
					// compDetail.itemPriceScore = 2;
					// }else if(compDetail.regPrice >= minPrice
					// && compDetail.regPrice <= maxPrice){
					// compDetail.itemPriceScore = 1;
					// }else{
					// compDetail.itemPriceScore = 0;
					// }
					itemsBasedOnSize.add(compDetail);
				}
			}
		} else {
			itemsBasedOnSize.addAll(tempCompItems);
		}
		return itemsBasedOnSize;
	}

	private List<CompetitiveDataDTO> getPLItemFromCompData(List<CompetitiveDataDTO> compDataList) {
		List<CompetitiveDataDTO> plCompDataList = new ArrayList<CompetitiveDataDTO>();
		// If Private labels were available for the given stores
		if (compStorePLNameList.size() > 0) {
			// Iterate each Private label and check item name matches with given name
			for (String plName : compStorePLNameList) {
				for (CompetitiveDataDTO competitiveDataDTO : compDataList) {
					// logger.debug("Item Name:" +competitiveDataDTO.itemName.toUpperCase().trim());
					if (competitiveDataDTO.itemName.toUpperCase().trim().contains(plName)) {
						plCompDataList.add(competitiveDataDTO);
					}
				}
			}
		} else {
			logger.error("Private label items were not given for store: " + storeName);
		}
		return plCompDataList;
	}

	private void getCompItemsBasedOnName(List<CompetitiveDataDTO> compItems, String baseItemName,
			HashMap<ItemDetailKey, List<CompetitiveDataDTO>> compPLItems, ItemDetailKey key) {

		HashMap<ItemDetailKey, List<CompetitiveDataDTO>> compPLItemsTemp = new HashMap<ItemDetailKey, List<CompetitiveDataDTO>>();
		List<String> splittedNames = getSplittedWordsAsList(baseItemName);

		int noOfWords = splittedNames.size();
		// Loop each comp data to match item name
		if (compItems.size() > 0) {
			for (CompetitiveDataDTO compItem : compItems) {
				int count = 0;
				String compItemTemp = null;
				for (String word : splittedNames) {
					word = word.trim();
					compItemTemp = compItem.itemName.replace("-", " ").replace(":", "").replace(",", "").replace("&", "").replace("/", " ");
					compItemTemp = compItemTemp.toUpperCase().trim();
					List<String> compNameList = getSplittedWordsAsList(compItemTemp);
					// If # of count > 0 then add into Map
					for (String compWord : compNameList) {
						if (compWord.equals(word)) {
							count++;
							break;
						} else if (compWord.length() > 2) {
							// To match comp words with base item words by using sub string methods
							for (int i = compWord.length(); i > 2; i--) {
								if (compWord.substring(0, i).equals(word)) {
									count++;
									break;
								}
							}
						}
					}
					// if(compItemTemp.contains(word)){
					// count++;
					// }
				}

				if (count > 0) {
					List<CompetitiveDataDTO> tempCompItems = new ArrayList<CompetitiveDataDTO>();
					if (compPLItemsTemp.containsKey(key)) {
						tempCompItems = compPLItemsTemp.get(key);
					}
					tempCompItems.add(compItem);
					compPLItemsTemp.put(key, tempCompItems);
				}
			}

			// Based on Key get the items to match with word count
			if (compPLItemsTemp.containsKey(key)) {
				List<CompetitiveDataDTO> compPLItemList = compPLItemsTemp.get(key);
				HashMap<Integer, List<CompetitiveDataDTO>> compItemTempMap = new HashMap<Integer, List<CompetitiveDataDTO>>();
				for (CompetitiveDataDTO competitiveDataDTO : compPLItemList) {
					String compItemName = competitiveDataDTO.itemName.replace("-", " ").replace(":", "").replace(",", "").replace("&", "")
							.replace("/", " ").toUpperCase().trim();
					// String[] splited = compItemName.split("\\b+");
					// String compTempName = compItemName.replace("-", " ").replace(":", "")
					// .replace(",", "").replace("&", "").replace(competitiveDataDTO.size, "").replace(competitiveDataDTO.uom,
					// "");
					List<String> compNameList = getSplittedWordsAsList(compItemName);
					int matchingWordCount = 0;

					// To match Base item name by using substring method and to find Matching count
					for (String word : splittedNames) {
						for (String compWord : compNameList) {
							if (compWord.equals(word)) {
								matchingWordCount++;
							} else if (compWord.length() > 2) {
								// To match comp words with base item words by using sub string methods
								for (int i = compWord.length(); i > 2; i--) {
									if (compWord.substring(0, i).equals(word)) {
										matchingWordCount++;
									}
								}
							}
						}
						// if(Arrays.asList(splited).contains(word)){
						// matchingWordCount++;
						// }
					}
					List<CompetitiveDataDTO> tempList = new ArrayList<CompetitiveDataDTO>();
					if (compItemTempMap.containsKey(matchingWordCount)) {
						tempList = compItemTempMap.get(matchingWordCount);
					}
					tempList.add(competitiveDataDTO);
					compItemTempMap.put(matchingWordCount, tempList);

				}

				// Check the maximum no of words match and add in HashMap
				// boolean isCompItemProcessed = false;
				for (int matchCount = noOfWords; matchCount >= 2; matchCount--) {
					if (compItemTempMap.containsKey(matchCount)) {
						List<CompetitiveDataDTO> finalItemList = compItemTempMap.get(matchCount);
						List<CompetitiveDataDTO> tempCompItemList = new ArrayList<CompetitiveDataDTO>();
						for (CompetitiveDataDTO competitiveDataDTO : finalItemList) {
							String itemName = competitiveDataDTO.itemName.replace("-", " ").replace(":", "").replace(",", "").replace("&", "")
									.replace("/", " ");
							competitiveDataDTO.itemNameScore = itemNameMatchingScore(splittedNames, itemName, true);
							tempCompItemList.add(competitiveDataDTO);
						}

						compPLItems.put(key, finalItemList);
						// isCompItemProcessed = true;
						break;
					}
				}
				// To process the items were No of items size itself one(E.g: GE LASAGNA)
				if (noOfWords <= 2) {
					// Sorting the Hashmap key values in ascending order to get max value
					Set<Integer> compItemNameMatchCount = compItemTempMap.keySet();
					List<Integer> tempList = new ArrayList<Integer>(compItemNameMatchCount);
					tempList.sort((comp1, comp2) -> comp2 - (comp1));
					// After sorting, iterate the Hashmap to get value for max count and break the loop
					for (int matchCount : tempList) {
						if (compItemTempMap.containsKey(matchCount)) {
							List<CompetitiveDataDTO> finalItemList = compItemTempMap.get(matchCount);
							List<CompetitiveDataDTO> tempCompItemList = new ArrayList<CompetitiveDataDTO>();
							for (CompetitiveDataDTO competitiveDataDTO : finalItemList) {
								String itemName = competitiveDataDTO.itemName.replace("-", " ").replace(":", "").replace(",", "").replace("&", "")
										.replace("/", " ");
								competitiveDataDTO.itemNameScore = itemNameMatchingScore(splittedNames, itemName, true);
								tempCompItemList.add(competitiveDataDTO);
							}
							compPLItems.put(key, finalItemList);
							break;
						}
					}
				}
				// To consider items when it matches with name, size and price and only one comp items is available for given base
				// item
				// if(!isCompItemProcessed && compItems.size() ==1 && compItemTempMap.size()>0){
				// for(Map.Entry<Integer, List<CompetitiveDataDTO>> finalEntry:compItemTempMap.entrySet()){
				// List<CompetitiveDataDTO> finalItemList = finalEntry.getValue();
				// List<CompetitiveDataDTO> tempCompItemList = new ArrayList<CompetitiveDataDTO>();
				// for(CompetitiveDataDTO competitiveDataDTO: finalItemList){
				// competitiveDataDTO.itemNameScore = 4;
				//// competitiveDataDTO.itemNameScore = compItemNameMatchingScore(splittedNames,competitiveDataDTO.itemName);
				// tempCompItemList.add(competitiveDataDTO);
				// }
				//
				// compPLItems.put(key, finalItemList);
				// }
				//
				// }
			} else {
				logger.warn("Comp item not for the given base item:" + baseItemName);
			}
		}

	}

	// Assign score for comp name based on Base item name.
	private int itemNameMatchingScore(List<String> baseNameList, String compItemName, boolean getScore) {
		int matchingScore = 0;
		double actualBaseWordCount = baseNameList.size();
		double matchingCount = 0;
		// To check the words matching between Base and comp item name
		List<String> compNameList = getSplittedWordsAsList(compItemName);
		double compWordCount = compNameList.size();
		for (String baseWord : baseNameList) {
			for (String compWord : compNameList) {
				if (compWord.equals(baseWord)) {
					matchingCount++;
				} else if (compWord.length() > 2 && baseWord.length() > 2) {
					// To match comp words with base item words by using sub string methods
					for (int j = baseWord.length(); j > 3; j--) {
						boolean wordMatched = false;
						String tempBaseWord = baseWord.substring(0, j);
						for (int i = compWord.length(); i > 2; i--) {
							if (compWord.substring(0, i).equals(tempBaseWord)) {
								matchingCount++;
								wordMatched = true;
								break;
							}
						}
						if (wordMatched) {
							break;
						}
					}

				}
			}
		}
		// logger.debug("Base Name count: "+actualBaseWordCount+" # of words matching: "+matchingCount+" # of Percentage:
		// "+((matchingCount/actualBaseWordCount)*100));
		// Scores based on # of percentage (90 %-> 4 points, 75% -> 3 Points, 50%-> 2 Points, > 50% -> 1 points )
		if (getScore && matchingCount > 0) {
			if (((matchingCount / actualBaseWordCount)) * 100 >= 90) {
				matchingScore = 4;
			} else if ((matchingCount / actualBaseWordCount) * 100 >= 75) {
				matchingScore = 3;
			} else if ((matchingCount / actualBaseWordCount) * 100 >= 50) {
				matchingScore = 2;
			} else {
				matchingScore = 1;
			}
		} else if (matchingCount > 0) {
			matchingScore = (int) ((matchingCount / actualBaseWordCount) * 100);
		}
		return matchingScore;

	}

	// Simple method to split the given group of words into list
	private List<String> getSplittedWordsAsList(String itemName) {
		List<String> splittedNames = new ArrayList<String>();
		String name = itemName.toUpperCase().replace("-", " ").replace(":", "").replace(",", "").replace("&", "");
		String[] splittedItemName = name.split("\\b+");
		for (int i = 0; i < splittedItemName.length; i++) {
			if (!(splittedItemName[i].toUpperCase().trim().replace(" ", "").isEmpty())) {
				splittedNames.add(splittedItemName[i].toUpperCase().trim());
			}
		}
		return splittedNames;
	}

	private void exportItemsInExcel(HashMap<ItemDetailKey, ItemMappingDTO> basePLItems, HashMap<ItemDetailKey, List<CompetitiveDataDTO>> compPLItems)
			throws GeneralException {
		String date = dateStr.replace("/", "_");
		String outputFileName = "GE_PRIVATE_LABEL_MAPPING_WITH_" + storeName + "_" + date + ".xlsx";
		String outputPath = rootPath + "/" + relativeOutputPath + "/" + outputFileName;

		XSSFWorkbook workbook = new XSSFWorkbook();
		XSSFSheet workSheet = workbook.createSheet("sheet1");
		XSSFCellStyle normalStyle = getNormalStyle(workbook);
		XSSFCellStyle headerStyle = getHeaderStyle(workbook);
		try {
			processCachedList(workbook, workSheet, basePLItems, compPLItems, normalStyle, headerStyle);
			autoSizeColumns(workbook);
			FileOutputStream outputStream = new FileOutputStream(outputPath);
			workbook.write(outputStream);
			workbook.close();
			outputStream.close();
		} catch (IOException e) {
			e.printStackTrace();
			throw new GeneralException("Error in exportItemsInExcel()", e);
		}
	}

	private void processCachedList(XSSFWorkbook workbook, XSSFSheet workSheet, HashMap<ItemDetailKey, ItemMappingDTO> basePLItems,
			HashMap<ItemDetailKey, List<CompetitiveDataDTO>> compPLItems, XSSFCellStyle normalStyle, XSSFCellStyle headerStyle) {
		int currentRow = 1;
		writeHeader(workbook, workSheet, currentRow, headerStyle);
		currentRow++;
		for (Map.Entry<ItemDetailKey, ItemMappingDTO> entry : basePLItems.entrySet()) {
			ItemDetailKey key = entry.getKey();
			ItemMappingDTO itemMappingDTO = entry.getValue();
			int columnIndex = 1;
			XSSFRow row = workSheet.createRow(currentRow);
			setCellValue(row, columnIndex++, itemMappingDTO.getUpc(), normalStyle);
			setCellValue(row, columnIndex++, itemMappingDTO.getRetailerItemCode(), normalStyle);
			setCellValue(row, columnIndex++, itemMappingDTO.getDepartmentName(), normalStyle);
			setCellValue(row, columnIndex++, itemMappingDTO.getCategoryName(), normalStyle);
			setCellValue(row, columnIndex++, itemMappingDTO.getSubCatName(), normalStyle);
			setCellValue(row, columnIndex++, itemMappingDTO.getBaseItemName(), normalStyle);
			setCellValue(row, columnIndex++, itemMappingDTO.getBaseItemFriendlyName(), normalStyle);
			setCellValue(row, columnIndex++, String.valueOf(itemMappingDTO.getBaseSize()), normalStyle);
			setCellValue(row, columnIndex++, String.valueOf(itemMappingDTO.getBaseRegPrice()), normalStyle);
			row.createCell(columnIndex++);
			currentRow = writeCompDetail(workbook, workSheet, row, compPLItems, currentRow, columnIndex, key, normalStyle);
		}
	}

	private int writeCompDetail(XSSFWorkbook workbook, XSSFSheet sheet, XSSFRow row, HashMap<ItemDetailKey, List<CompetitiveDataDTO>> compPLItems,
			int currentRow, int columnIndex, ItemDetailKey key, XSSFCellStyle normalStyle) {
		int processingRow = currentRow;
		boolean isFirstRow = true;
		if (compPLItems.containsKey(key)) {
			List<CompetitiveDataDTO> compDetails = compPLItems.get(key);
			// compDetails.forEach(compItem -> {
			// compItem.totalScore = compItem.itemNameScore + compItem.itemSizeScore;
			// });
			// Sorting based on total scores
			int maxTotalScore = 0;
			compDetails.sort((comp1, comp2) -> comp2.getTotalScore() - (comp1.getTotalScore()));
			for (CompetitiveDataDTO competitiveDataDTO : compDetails) {
				if (maxTotalScore == 0) {
					maxTotalScore = competitiveDataDTO.getTotalScore();
				}
				if (maxTotalScore == competitiveDataDTO.getTotalScore() && maxTotalScore > 3) {
					int index = columnIndex;
					if (isFirstRow) {
						setCellValue(row, index++, String.valueOf(competitiveDataDTO.getTotalScore()), normalStyle);
						setCellValue(row, index++, String.valueOf(competitiveDataDTO.itemSizeScore), normalStyle);
						setCellValue(row, index++, String.valueOf(competitiveDataDTO.compItemNameScore), normalStyle);
						setCellValue(row, index++, String.valueOf(competitiveDataDTO.itemNameScore), normalStyle);
						if (competitiveDataDTO.upc != null && !competitiveDataDTO.upc.isEmpty()) {
							setCellValue(row, index++, Long.toString((long) (Double.valueOf(competitiveDataDTO.upc).longValue())), normalStyle);
						} else {
							setCellValue(row, index++, "", normalStyle);
						}
						setCellValue(row, index++, competitiveDataDTO.itemName, normalStyle);
						setCellValue(row, index++, String.valueOf(competitiveDataDTO.size), normalStyle);
						setCellValue(row, index++, competitiveDataDTO.deptName, normalStyle);
						setCellValue(row, index++, competitiveDataDTO.categoryName, normalStyle);
						if (storeName.toUpperCase().trim().equals("KROGER")) {
							setCellValue(row, index++, competitiveDataDTO.subCategory, normalStyle);
						}
						setCellValue(row, index++, new DecimalFormat("#0.00").format(competitiveDataDTO.regPrice), normalStyle);
						setCellValue(row, index++, competitiveDataDTO.getCompItemAddlDesc(), normalStyle);
						isFirstRow = false;
						processingRow++;
					} else {
						index = 1;
						XSSFRow row1 = sheet.createRow(processingRow);
						setCellValue(row1, index++, "", normalStyle);
						setCellValue(row1, index++, "", normalStyle);
						setCellValue(row1, index++, "", normalStyle);
						setCellValue(row1, index++, "", normalStyle);
						setCellValue(row1, index++, "", normalStyle);
						setCellValue(row1, index++, "", normalStyle);
						setCellValue(row1, index++, "", normalStyle);
						setCellValue(row1, index++, "", normalStyle);
						setCellValue(row1, index++, "", normalStyle);
						row1.createCell(index++);
						setCellValue(row1, index++, String.valueOf(competitiveDataDTO.getTotalScore()), normalStyle);
						setCellValue(row1, index++, String.valueOf(competitiveDataDTO.itemSizeScore), normalStyle);
						setCellValue(row1, index++, String.valueOf(competitiveDataDTO.compItemNameScore), normalStyle);
						setCellValue(row1, index++, String.valueOf(competitiveDataDTO.itemNameScore), normalStyle);
						if (competitiveDataDTO.upc != null && !competitiveDataDTO.upc.isEmpty()) {
							setCellValue(row1, index++, Long.toString((long) (Double.valueOf(competitiveDataDTO.upc).longValue())), normalStyle);
						} else {
							setCellValue(row1, index++, "", normalStyle);
						}
						setCellValue(row1, index++, competitiveDataDTO.itemName, normalStyle);
						setCellValue(row1, index++, String.valueOf(competitiveDataDTO.size), normalStyle);
						setCellValue(row1, index++, competitiveDataDTO.deptName, normalStyle);
						setCellValue(row1, index++, competitiveDataDTO.categoryName, normalStyle);
						if (storeName.toUpperCase().trim().equals("KROGER")) {
							setCellValue(row1, index++, competitiveDataDTO.subCategory, normalStyle);
						}
						setCellValue(row1, index++, new DecimalFormat("#0.00").format(competitiveDataDTO.regPrice), normalStyle);
						setCellValue(row1, index++, competitiveDataDTO.getCompItemAddlDesc(), normalStyle);
						isFirstRow = false;
						processingRow++;
					}
				}
			}
		}
		return processingRow;
	}

	private void setCellValue(XSSFRow row, int index, String cellValue, XSSFCellStyle style) {
		XSSFCell cell = row.createCell(index);
		cell.setCellValue(cellValue);
		cell.setCellStyle(style);
	}

	private void writeHeader(XSSFWorkbook workbook, XSSFSheet sheet, int currentRow, XSSFCellStyle headerStyle) {
		int columnIndex = 1;
		XSSFRow row = sheet.createRow(currentRow);
		setCellValue(row, columnIndex++, "UPC", headerStyle);
		setCellValue(row, columnIndex++, "RETAILER ITEM CODE", headerStyle);
		setCellValue(row, columnIndex++, "DEPARTMENT", headerStyle);
		setCellValue(row, columnIndex++, "CATEGORY", headerStyle);
		setCellValue(row, columnIndex++, "SUB CATEGORY", headerStyle);
		setCellValue(row, columnIndex++, "ITEM NAME", headerStyle);
		setCellValue(row, columnIndex++, "FRIENDLY ITEM NAME", headerStyle);
		setCellValue(row, columnIndex++, "ITEM SIZE", headerStyle);
		setCellValue(row, columnIndex++, "REGULAR PRICE", headerStyle);
		row.createCell(columnIndex++);
		setCellValue(row, columnIndex++, "TOTAL SCORE", headerStyle);
		setCellValue(row, columnIndex++, "ITEM SIZE SCORE", headerStyle);
		setCellValue(row, columnIndex++, "COMP ITEM NAME SCORE", headerStyle);
		setCellValue(row, columnIndex++, "ITEM NAME SCORE", headerStyle);
		setCellValue(row, columnIndex++, "COMP UPC", headerStyle);
		setCellValue(row, columnIndex++, "COMP ITEM NAME", headerStyle);
		setCellValue(row, columnIndex++, "SIZE/UOM", headerStyle);
		setCellValue(row, columnIndex++, "DEPARTMENT", headerStyle);
		setCellValue(row, columnIndex++, "CATEGORY", headerStyle);
		if (storeName.toUpperCase().trim().equals("KROGER")) {
			setCellValue(row, columnIndex++, "SUB CATEGORY", headerStyle);
		}
		setCellValue(row, columnIndex++, "REGULAR PRICE", headerStyle);
		setCellValue(row, columnIndex++, "ADDITIONAL COMMENTS", headerStyle);
	}

	private XSSFCellStyle getNormalStyle(XSSFWorkbook workbook) {
		XSSFColor lightGrey = new XSSFColor(new java.awt.Color(0, 0, 128));
		XSSFColor white = new XSSFColor(new java.awt.Color(255, 255, 255));
		XSSFCellStyle style = workbook.createCellStyle();
		XSSFFont font = workbook.createFont();
		font.setBold(false);
		// font.setColor(white);
		style.setFont(font);
		// style.setFillForegroundColor(white);
		// style.setFillPattern(XSSFCellStyle.SOLID_FOREGROUND);
		style.setBorderTop(XSSFCellStyle.BORDER_THIN);
		style.setBorderBottom(XSSFCellStyle.BORDER_THIN);
		style.setBorderRight(XSSFCellStyle.BORDER_THIN);
		style.setBorderLeft(XSSFCellStyle.BORDER_THIN);
		style.setShrinkToFit(true);
		return style;
	}

	private XSSFCellStyle getHeaderStyle(XSSFWorkbook workbook) {
		XSSFColor lightGrey = new XSSFColor(new java.awt.Color(0, 0, 128));
		XSSFColor white = new XSSFColor(new java.awt.Color(255, 255, 255));
		XSSFCellStyle style = workbook.createCellStyle();
		XSSFFont font = workbook.createFont();
		font.setBold(true);
		// font.setColor(white);
		style.setFont(font);
		style.setShrinkToFit(true);
		// style.setFillForegroundColor(lightGrey);
		// style.setFillPattern(XSSFCellStyle.SOLID_FOREGROUND);
		style.setBorderTop(XSSFCellStyle.BORDER_MEDIUM);
		style.setBorderBottom(XSSFCellStyle.BORDER_MEDIUM);
		style.setBorderRight(XSSFCellStyle.BORDER_MEDIUM);
		style.setBorderLeft(XSSFCellStyle.BORDER_MEDIUM);
		return style;
	}

	private void autoSizeColumns(XSSFWorkbook workbook) {
		int sheetCount = workbook.getNumberOfSheets();
		for (int i = 0; i < sheetCount; i++) {
			XSSFSheet sheet = workbook.getSheetAt(i);
			for (int colnum = 0; colnum <= sheet.getLastRowNum(); colnum++)
				sheet.autoSizeColumn(colnum);
		}
	}

	/**
	 * To get comp item name score based on Base item name, which can be used to filter multiple item matching for an base item
	 * 
	 * @param baseItemName
	 * @param compPLItems
	 * @param key
	 */
	private void getCompItemNameScore(String baseItemName, HashMap<ItemDetailKey, List<CompetitiveDataDTO>> compPLItems, ItemDetailKey key) {
		List<String> splittedNames = getSplittedWordsAsList(baseItemName);
		if (compPLItems.containsKey(key)) {
			for (CompetitiveDataDTO compItem : compPLItems.get(key)) {
				if (compItem.itemNameWOCompName != null && !compItem.itemNameWOCompName.equals("")) {
					String compItemName = compItem.itemNameWOCompName.replace("-", " ").replace(":", "").replace(",", "").replace("&", "");
					if (compItem.size != null && !compItem.size.equals("")) {
						compItemName = compItemName.replace(compItem.size, "");
					}
					if (compItem.uom != null && !compItem.uom.equals("")) {
						compItemName = compItemName.replace(compItem.uom, "");
					}
					compItemName = compItemName.toUpperCase().trim();
					List<String> compNameList = getSplittedWordsAsList(compItemName);
					compItem.setCompItemNameScore(itemNameMatchingScore(compNameList, baseItemName, false));
				}
			}
		}
	}

	private HashMap<ItemDetailKey, List<CompetitiveDataDTO>> removeLessMatchingCompItems(
			HashMap<ItemDetailKey, List<CompetitiveDataDTO>> compPLItems) {

		HashMap<ItemDetailKey, List<CompetitiveDataDTO>> filteredCompPLItems = new HashMap<ItemDetailKey, List<CompetitiveDataDTO>>();

		// Calculate Total score for each items
		for (Map.Entry<ItemDetailKey, List<CompetitiveDataDTO>> entry : compPLItems.entrySet()) {
			entry.getValue().forEach(compItem -> {
				compItem.setTotalScore(compItem.itemNameScore + compItem.itemSizeScore);
			});
		}

		// Group each item based on total score
		for (Map.Entry<ItemDetailKey, List<CompetitiveDataDTO>> entry : compPLItems.entrySet()) {

			List<CompetitiveDataDTO> filteredItemsUsingCompItemScore = new ArrayList<CompetitiveDataDTO>();
			HashMap<Integer, List<CompetitiveDataDTO>> itemBasedOnTotalItemScore = new HashMap<Integer, List<CompetitiveDataDTO>>();

			// Group items based on total score
			entry.getValue().forEach(compItem -> {
				List<CompetitiveDataDTO> competitiveDataDTOs = new ArrayList<CompetitiveDataDTO>();
				if (itemBasedOnTotalItemScore.get(compItem.getTotalScore()) != null) {
					competitiveDataDTOs = itemBasedOnTotalItemScore.get(compItem.getTotalScore());
				}
				competitiveDataDTOs.add(compItem);
				itemBasedOnTotalItemScore.put(compItem.getTotalScore(), competitiveDataDTOs);
			});

			// Sort items based on Comp item Name score and filter items which is having maximum score
			itemBasedOnTotalItemScore.forEach((key, compItemList) -> {
				compItemList.sort((comp1, comp2) -> comp2.getCompItemNameScore() - (comp1.getCompItemNameScore()));
				int maxCompItemScore = 0;

				for (CompetitiveDataDTO compItem : compItemList) {
					if (maxCompItemScore == 0) {
						maxCompItemScore = compItem.getCompItemNameScore();
					}
					if (compItem.getCompItemNameScore() == maxCompItemScore) {
						filteredItemsUsingCompItemScore.add(compItem);
					}
				}
			});

			filteredCompPLItems.put(entry.getKey(), filteredItemsUsingCompItemScore);
		}
		return filteredCompPLItems;
	}

	private void checkKeywordsMatch(String baseItemName, HashMap<ItemDetailKey, List<CompetitiveDataDTO>> compPLItems, ItemDetailKey key)
			throws GeneralException {
		List<String> privateSelection = getSplittedWordsFromProperties("PL_MAPPING_KEY_WORDS");

		try {
			// If keyword is found in Base item name, then try to find same key word in Comp Item name
			for (String keyToMatch : privateSelection) {
				if (baseItemName.contains(keyToMatch) && compPLItems.get(key) != null) {
					compPLItems.get(key).forEach(compItem -> {
						String compName = compItem.itemName.toUpperCase().trim();
						if (!compName.contains(keyToMatch)) {
							compItem.setCompItemAddlDesc(keyToMatch + " Key word not Found");
						}
					});
				}
			}

			// Match percentage if available in Base item name.
			if (baseItemName.contains("%")) {
				String baseItemTempName = baseItemName.replace(" %", "%");
				List<String> splittedNames = getSplittedWordsAsList(baseItemTempName);
				for (String itemName : splittedNames) {
					if (itemName.contains("%") && compPLItems.get(key) != null) {
						compPLItems.get(key).forEach(compItem -> {
							if (compItem.itemName != null && !compItem.itemName.equals("") && compItem.itemName.contains("%")) {
								String compItemTempName = compItem.itemName.replace(" %", "%");
								List<String> compNameSplitted = getSplittedWordsAsList(compItemTempName);
								for (String compName : compNameSplitted) {
									if (compName.contains("%")) {
										if (!itemName.equals(compName)) {
											String addlDesc;
											if (compItem.getCompItemAddlDesc() != null && !compItem.getCompItemAddlDesc().isEmpty()) {
												addlDesc = compItem.getCompItemAddlDesc() + ", Base item % is not matching";
												compItem.setCompItemAddlDesc(addlDesc);
											} else {
												compItem.setCompItemAddlDesc("Base item % is not matching");
											}
										}
									}
								}
							}
						});
					}
				}
			}
		} catch (Exception e) {
			e.printStackTrace();
			logger.error("Error in checkKeywordsMatch()", e);
			throw new GeneralException("Error in checkKeywordsMatch()", e);
		}

	}
	
	private void addBasePrivateLabelNameInList(String propertyName){
		List<String> storePLName = getSplittedWordsFromProperties(propertyName);
		for (String plName : storePLName) {
			baseStorePLNameList.add(plName.toUpperCase().trim());
		}
	}
	
	private void addCompPrivateLabelNameInList(String propertyName){
		List<String> storePLName = getSplittedWordsFromProperties(propertyName);
		for (String plName : storePLName) {
			compStorePLNameList.add(plName.toUpperCase().trim());
		}
	}
	
	private void getCompItemBasedOnPLNames(List<String> compStorePLName, List<CompetitiveDataDTO> plCompDataList,
			List<CompetitiveDataDTO> newPLCompDataList) throws CloneNotSupportedException {
		for (String compPLName : compStorePLName) {
			for (CompetitiveDataDTO competitiveDataDTO : plCompDataList) {
				if (competitiveDataDTO.itemName.toUpperCase().trim().contains(compPLName)) {
					// if (competitiveDataDTO.upc != null && !competitiveDataDTO.upc.isEmpty()) {
					// CompetitiveDataDTO competitiveData = (CompetitiveDataDTO) competitiveDataDTO.clone();
					// competitiveData.itemNameWOCompName = competitiveDataDTO.itemName.toUpperCase().trim().replace(compPLName,
					// "");
					// tempCompMap.put(competitiveDataDTO.upc, competitiveData);
					// } else {
					CompetitiveDataDTO competitiveData = (CompetitiveDataDTO) competitiveDataDTO.clone();
					competitiveData.itemNameWOCompName = competitiveDataDTO.itemName.toUpperCase().trim().replace(compPLName, "");
					newPLCompDataList.add(competitiveData);
					// }
				}
			}
		}
		// tempCompMap.forEach((key, value) -> {
		// newPLCompDataList.add(value);
		// });
	}
}
