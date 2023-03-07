package com.pristine.dataload.prestoload;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileReader;
import java.io.IOException;
import java.sql.Connection;
import java.text.DateFormat;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Calendar;
import java.util.Collection;
import java.util.Date;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.TimeUnit;

import org.apache.log4j.Logger;
import org.apache.log4j.PropertyConfigurator;

import com.csvreader.CsvReader;
import com.pristine.dao.CostDAO;
import com.pristine.dao.ItemDAO;
import com.pristine.dao.RetailCalendarDAO;
import com.pristine.dao.RetailCostDAO;
import com.pristine.dao.RetailPriceDAO;
import com.pristine.dao.RetailPriceZoneDAO;
import com.pristine.dao.offermgmt.ProductLocationMappingDAO;
import com.pristine.dataload.service.StoreItemMapService;
import com.pristine.dto.ItemDetailKey;
import com.pristine.dto.RetailCalendarDTO;
import com.pristine.dto.RetailCostDTO;
import com.pristine.dto.StoreItemKey;
import com.pristine.exception.GeneralException;
import com.pristine.parsinginterface.PristineFileParser;
import com.pristine.util.Constants;
import com.pristine.util.DateUtil;
import com.pristine.util.PrestoUtil;
import com.pristine.util.PristineDBUtil;
import com.pristine.util.PropertyManager;

@SuppressWarnings("rawtypes")
public class RetailCostSetup extends PristineFileParser {

	private static Logger logger = Logger.getLogger("RetailCostSetup");

	private Connection conn = null;
	private static int columnCount = 9;
	private String chainId = null;
	private Set<String> storeIdSet;
	private HashMap<Integer, HashMap<ItemDetailKey, List<RetailCostDTO>>> retailCostDataMap;
	private HashMap<String, String> storeMap;
	private HashMap<String, Integer> priceZoneIdMap;
	private HashMap<String, Integer> storeIdMap;
	private HashMap<String, List<Integer>> deptZoneMap;
	HashMap<String, List<String>> zoneStoreMap = null;
	private Set<String> noItemCodeSet = new HashSet<String>();
	private int recordCount = 0;
	private int ignoreCount = 0;
	private int calendarId = -1;
	// Change to handle stores with no zone
	private Date weekEndDate = null; // Changes to handle future cost records
	private HashMap<String, Integer> calendarMap = new HashMap<String, Integer>(); // Changes
																					// to
																					// handle
																					// future
																					// cost
																					// records
	private Set<String> storesWithNoZone = new HashSet<String>();
	private static String ARG_IGN_STR_NOT_IN_DB = "IGN_STR_NOT_IN_DB=";
	public static final String IGN_COPY_EXISTING_DATA = "IGN_COPY_EXIST=";
	private static String ignoreStoresNotInDB = "";

	List<RetailCostDTO> currentProcCostList = new ArrayList<RetailCostDTO>();
	int noOfRecsToBeProcessed = 0;
	int commitRecCount = 1000;
	int prevCalendarId = -1;
	String startDate = null;
	String prevWkStartDate = null;
	double maxCostAllowed = 99999.99;
	// Changes to handle future cost records
	private static String LOAD_FUTURE_PRICE = "LOAD_FUTURE_COST=";
	private static String LOAD_VENDOR_INFO = "LOAD_VENDOR_INFO=";
	private static String SYNC_STORE_ITEM = "UPDATE_STORE_ITEM_MAP=";
	private static boolean loadFutureCost = false;
	private static boolean syncStoreItemMap = false;
	private static boolean loadVendorInfo = false;
	private int futureCostRecordsSkipped = 0;
	private RetailCalendarDAO retailCalendarDAO = null;
	private static String mode = null;
	// Changes to handle future cost records Ends
	private CostDAO costDAO = null;
	private RetailPriceDAO retailPriceDAO = null;
	private RetailCostDAO retailCostDAO = null;
	private HashMap<ItemDetailKey, String> itemLookupMap= null;
	// Changes to retrieve item_code using upc and retailer_item_code
	boolean checkRetailerItemCode = false;
	//Changes for populating location_id in RETAIL_COST_INFO
  	HashMap<String, Integer> storeIdsMap = new HashMap<String, Integer>();
  	HashMap<String, Integer> retailPriceZone = new HashMap<String, Integer>();
  	boolean useVendorCodeFromZone = false;
  	boolean useZoneStoreMapping = false;
  	HashMap<String, List<String>> zoneToStoreMapping = null;
  	private HashMap<String, List<Integer>> storeZoneMap = null;
  	private boolean rollupDSDToWhseEnabled = false;
 	private HashMap<Integer, HashMap<String, String>> dsdAndWhseZoneMap = null;
 	private HashMap<Integer, Integer> itemCodeCategoryMap = null;
 	HashMap<String, String> primaryMatchingZone = null;
 	private static String DELETE_EXITING_CUR_AND_FUTURE_COST = "DELETE_EXITING_CUR_AND_FUTURE_COST=";
 	private static boolean delExitingCurAndFutCost = false;
 	//changes to load the deal cost
 	private static final String LOAD_DEAL_COST_ONLY = "LOAD_DEAL_COST_ONLY=";
 	static boolean loadDealOnly = false;
 	
	public RetailCostSetup() {
		retailCalendarDAO = new RetailCalendarDAO();
	}

	/**
	 * Main method of Retail Cost Setup Batch
	 * 
	 * @param args
	 *            [0] Relative path of the input file
	 * @param args
	 *            [1] specificweek/currentweek/nextweek
	 * @param args
	 *            [2] Will be present only if args[1] is specificweek
	 */
	public static void main(String[] args) {
		RetailCostSetup retailCostSetup = new RetailCostSetup();

		PropertyConfigurator.configure("log4j-retail-cost-setup.properties");
		PropertyManager.initialize("analysis.properties");

		// Change to handle stores with no zone
		if (args[1].indexOf(ARG_IGN_STR_NOT_IN_DB) >= 0) {
			ignoreStoresNotInDB = args[1].substring(ARG_IGN_STR_NOT_IN_DB
					.length());
		} else {
			logger.info("Invalid Argument, args[1] ARG_IGN_STR_NOT_IN_DB=[true/false/empty string]");
			System.exit(-1);
		}
		// Change to handle stores with no zone - Ends

		if (!(Constants.NEXT_WEEK.equalsIgnoreCase(args[2])
				|| Constants.CURRENT_WEEK.equalsIgnoreCase(args[2])
				|| Constants.SPECIFIC_WEEK.equalsIgnoreCase(args[2]) || Constants.LAST_WEEK
					.equalsIgnoreCase(args[2]))) {
			logger.info("Invalid Argument, args[2] should be lastweek/currentweek/nextweek/specificweek");
			System.exit(-1);
		}

		/*
		 * if(Constants.SPECIFIC_WEEK.equalsIgnoreCase(args[2]) && (args.length
		 * != 4)){ logger.info(
		 * "Invalid Arguments, args[0] should be relative path, args[2] lastweek/currentweek/nextweek/specificweek, args[3] data for a specific week"
		 * ); System.exit(-1); }
		 */

		String dateStr = null;
		String listFile = null;
		boolean loadVendor = false;
		boolean copyExisting = false;
		Calendar c = Calendar.getInstance();
		int dayIndex = 0;
		if(PropertyManager.getProperty("CUSTOM_WEEK_START_INDEX") != null){
			dayIndex = Integer.parseInt(PropertyManager.
					getProperty("CUSTOM_WEEK_START_INDEX"));
		}
		if(dayIndex > 0)
			c.set(Calendar.DAY_OF_WEEK, dayIndex + 1);
		else
			c.set(Calendar.DAY_OF_WEEK, Calendar.SUNDAY);
		DateFormat dateFormat = new SimpleDateFormat("MM/dd/yyyy");
		if (Constants.CURRENT_WEEK.equalsIgnoreCase(args[2])) {
			dateStr = dateFormat.format(c.getTime());
		} else if (Constants.NEXT_WEEK.equalsIgnoreCase(args[2])) {
			c.add(Calendar.DATE, 7);
			dateStr = dateFormat.format(c.getTime());
		} else if (Constants.LAST_WEEK.equalsIgnoreCase(args[2])) {
			c.add(Calendar.DATE, -7);
			dateStr = dateFormat.format(c.getTime());
		} else if (Constants.SPECIFIC_WEEK.equalsIgnoreCase(args[2])) {
			dateStr = args[3];
		}

		// Changes to handle future cost
		for (String arg : args) {
			if (arg.indexOf(LOAD_FUTURE_PRICE) != -1) {
				loadFutureCost = Boolean.valueOf(arg
						.substring(LOAD_FUTURE_PRICE.length()));
			} else if (arg.indexOf(Constants.DATA_LOAD_MODE) != -1) {
				mode = arg.substring(Constants.DATA_LOAD_MODE.length());
			} else if (arg.indexOf(LOAD_VENDOR_INFO) != -1) {
				loadVendor = Boolean.valueOf(arg.substring(LOAD_VENDOR_INFO
						.length()));
			} else if (arg.indexOf(IGN_COPY_EXISTING_DATA) != -1) {
				copyExisting = Boolean.valueOf(arg
						.substring(IGN_COPY_EXISTING_DATA.length()));
			} else if (arg.indexOf(SYNC_STORE_ITEM) != -1) {
				syncStoreItemMap = Boolean.valueOf(arg
						.substring(SYNC_STORE_ITEM.length()));
			} 
			else if (arg.startsWith(DELETE_EXITING_CUR_AND_FUTURE_COST)) {
				delExitingCurAndFutCost = Boolean.parseBoolean(arg.substring(DELETE_EXITING_CUR_AND_FUTURE_COST.length()));
			}
			if (arg.startsWith("LIST_FILE")) {
				listFile = arg.substring("LIST_FILE=".length());
			}
			else
				listFile = "";
			
			if (arg.startsWith(LOAD_DEAL_COST_ONLY)) {
				loadDealOnly = Boolean.valueOf(arg.substring(LOAD_DEAL_COST_ONLY.length()));
			}
			
			
		}
		if (loadVendor) {
			columnCount = 13;
			loadVendorInfo = true;
		}
		

		retailCostSetup.setupObjects(dateStr, mode, copyExisting);
		retailCostSetup.processRetailCostFile(args[0], dateStr, listFile);
		
		
	}
	


	/**
	 * Sets up class level objects for the given date
	 * 
	 * @param dateStr
	 */
	private void setupObjects(String dateStr, String mode, boolean copyExisting) {
		try {

			String tempCommitCount = PropertyManager.getProperty(
					"DATALOAD.COMMITRECOUNT", "1000");
			commitRecCount = Integer.parseInt(tempCommitCount);

			checkRetailerItemCode = Boolean.parseBoolean(
					PropertyManager.getProperty("ITEM_LOAD.CHECK_RETAILER_ITEM_CODE", "FALSE"));
			populateCalendarId(dateStr);

			costDAO = new CostDAO();
			ItemDAO itemDAO = new ItemDAO();
			retailPriceDAO = new RetailPriceDAO();
			retailCostDAO = new RetailCostDAO();
			RetailPriceZoneDAO retailPriceZoneDAO = new RetailPriceZoneDAO();
			// Retrieve Subscriber Chain Id
			chainId = retailPriceDAO.getChainId(conn);
			logger.info("Subscriber Chain Id - " + chainId);

			// Retrieve all stores and its corresponding zone#
			storeMap = costDAO.getStoreZoneInfo(conn, chainId);
			// castZoneNumbers(storeInfoMap);
			logger.info("No of store available - " + storeMap.size());

			// Populate a map with zone# as key and list of corresponding stores
			// as value
			zoneStoreMap = getZoneMapping(storeMap);
			logger.info("No of zones available - " + zoneStoreMap.size());
			
			
			useZoneStoreMapping = Boolean.parseBoolean
					(PropertyManager.getProperty("USE_PRODUCT_LOCATION_FOR_ZONE_STORE_MAP", "FALSE"));
			//Changes to decide chain level price when there are only two zone level prices
			if(useZoneStoreMapping){
				zoneToStoreMapping = retailPriceDAO.getZoneStoreMapping(conn);
			}
			
			
			//Added for getting store id and zone id to update proper location_id saving price records.
			//Added by Pradeep 08-21-2015
			retailPriceDAO = new RetailPriceDAO();
			long startTime  = System.currentTimeMillis();
			storeIdsMap = retailPriceDAO.getStoreIdMap(conn, Integer.parseInt(chainId));
			long endTime = System.currentTimeMillis();
			logger.info("setupObjects() - Time taken to retreive store id mapping - " + (endTime - startTime) + "ms");
			startTime = System.currentTimeMillis();
			retailPriceZone = retailPriceDAO.getRetailPriceZone(conn);
			endTime = System.currentTimeMillis();
			
			
			storeZoneMap = retailPriceZoneDAO.getZoneStoreMap(conn);
			
			logger.info("setupObjects() - Time taken to retreive zone id mapping - " + (endTime - startTime) + "ms");
			//Changes ends.
			
			
			//Read config
			useVendorCodeFromZone = Boolean.parseBoolean
					(PropertyManager.getProperty("USE_VENDOR_CODE_FROM_ZONE", "FALSE"));
			
			rollupDSDToWhseEnabled = Boolean.parseBoolean(PropertyManager.
					getProperty("ROLL_UP_DSD_TO_WARHOUSE_ZONE", "FALSE"));
			if(rollupDSDToWhseEnabled){
				
				logger.info("setupObjects() - ***ROLL_UP_DSD_TO_WARHOUSE_ZONE is enabled***");
				logger.info("setupObjects() - Getting DSD and Warehouse zone mapping...");
				dsdAndWhseZoneMap = retailCostDAO.getDSDAndWHSEZoneMap(conn, null);
				logger.info("setupObjects() - Getting DSD and Warehouse zone mapping is completed.");
				
				logger.info("setupObjects() - Getting Item and category mapping...");
				itemCodeCategoryMap = itemDAO.getCategoryAndItemCodeMap(conn, null);
				logger.info("setupObjects() - Getting Item and category mapping is completed.");
				
				ProductLocationMappingDAO productLocationMappingDAO = new ProductLocationMappingDAO();
				
				primaryMatchingZone = productLocationMappingDAO.getPrimaryMatchingZoneNum(conn);
			}
			// Delete from retail_cost_info for the week being processed

			//
			if (mode != null && mode.equals(Constants.DATA_LOAD_DELTA)
					&& copyExisting) {
				// copy current week data into temp tabale as a backup
				logger.info("setupObjects() - Moving current data to temp table...");
				startTime = System.currentTimeMillis();
				retailCostDAO.moveCurrentDataToTemp(conn, calendarId);
				endTime = System.currentTimeMillis();
				logger.info("setupObjects() - Time taken to "
						+ "Move current data to temp table - " + (endTime - startTime)  + "ms");
				// delete all current week data.
				logger.info("setupObjects() - Deleting current data for calendar id - " + calendarId);
				startTime = System.currentTimeMillis();
				retailCostDAO.deleteRetailCostData(conn, calendarId);
				endTime = System.currentTimeMillis();
				logger.info("setupObjects() - Time taken to delete - " + (endTime - startTime) + "ms");
				// Insert previous week retail cost value
				logger.info("setupObjects() - Copying date from calendar id " + prevCalendarId + 
						" to calendar id " + calendarId);
				startTime = System.currentTimeMillis();
				retailCostDAO.setupRetailCostDataV2(conn, calendarId,
						prevCalendarId);
				endTime = System.currentTimeMillis();
				logger.info("setupObjects() - Time taken to delete - " + (endTime - startTime) + "ms");
				// restore data from temp to main.
				//retailCostDAO.restoreDataFromTemp(conn, calendarId);
			}
		} catch (GeneralException ge) {
			logger.error("Error in setting up objects", ge);
			return;
		}
	}

	/**
	 * Sets input week's calendar id and its previous week's calendar id
	 * 
	 * @param weekStartDate
	 *            Input Date
	 * @throws GeneralException
	 */
	private void populateCalendarId(String weekStartDate)
			throws GeneralException {
		conn = getOracleConnection();

		RetailCalendarDAO retailCalendarDAO = new RetailCalendarDAO();
		RetailCalendarDTO calendarDTO = retailCalendarDAO.getCalendarId(conn,
				weekStartDate, Constants.CALENDAR_WEEK);
		logger.info("Calendar Id - " + calendarDTO.getCalendarId());
		calendarId = calendarDTO.getCalendarId();
		startDate = calendarDTO.getStartDate();

		String prevWeekStartDate = DateUtil.getWeekStartDate(
				DateUtil.toDate(weekStartDate), 1);
		calendarDTO = retailCalendarDAO.getCalendarId(conn, prevWeekStartDate,
				Constants.CALENDAR_WEEK);
		prevWkStartDate = calendarDTO.getStartDate();
		logger.info("Previous Week Calendar Id - "
				+ calendarDTO.getCalendarId());

		prevCalendarId = calendarDTO.getCalendarId();
	}
	
	
	
	
	private ArrayList<String> getFileList(String subFolder, String listFile) throws GeneralException {
		ArrayList<String> fileList = new ArrayList<String>();

		String filePath = getRootPath() + "/" + subFolder + "/"  + listFile;
		File file = new File(filePath);
		if (!file.exists()){
    		logger.error("getFileList - File doesn't exist : " + filePath);
			throw new GeneralException("File: " + filePath + " doesn't exist");
		}
		
		BufferedReader br = null;
		try {
			String line;
			FileReader textFile = new FileReader(file);
			boolean allFilesExists = true;
			br = new BufferedReader(textFile);
			while ((line = br.readLine()) != null) {
				if (!line.trim().equals(""))
				{
					String strFile = getRootPath() + "/" + subFolder + "/" + line;
					File file1 = new File(strFile);
					if (file1.exists()) {
						fileList.add(strFile);
					}
					else
					{
						allFilesExists = false;
			    		logger.error("getFileList - File doesn't exist : " + strFile);
					}
				}
			}

			if (!allFilesExists){
				throw new GeneralException("Some files are missing.");
			}
		} catch (Exception e) {
        	throw new GeneralException("Error in getFileList()",e);
		} finally {
			try {
				if (br != null)br.close();
			} catch (IOException e) {
				e.printStackTrace();
			}
		}
		return fileList;
	}
	

	/**
	 * This method parses input text file and populates data as a list of
	 * RetailCostDTO
	 * 
	 * @param relativePath
	 *            Relative path of the input file
	 * @param inputDate
	 *            Date of the week
	 */
	@SuppressWarnings("unchecked")
	private void processRetailCostFile(String relativePath, String inputDate, String listFile) {
		logger.info("Retail Cost Setup Starts");
		conn = getOracleConnection();

		try {
			RetailCalendarDAO retailCalendarDAO = new RetailCalendarDAO();
			RetailCalendarDTO calendarDTO = retailCalendarDAO.getCalendarId(
					conn, inputDate, Constants.CALENDAR_WEEK);
			logger.info("Calendar Id - " + calendarDTO.getCalendarId());
			calendarId = calendarDTO.getCalendarId();
			weekEndDate = DateUtil.toDate(calendarDTO.getEndDate()); // Changes
																		// to
																		// handle
																		// future
																		// cost
																		// records
			String tempCommitCount = PropertyManager.getProperty(
					"DATALOAD.COMMITRECOUNT", "1000");
			commitRecCount = Integer.parseInt(tempCommitCount);

			String recStr = PropertyManager.getProperty("DATALOAD.LOADRECORDS",
					String.valueOf(Constants.RECS_TOBE_PROCESSED));
			noOfRecsToBeProcessed = Integer.parseInt(recStr);
			
			
			logger.info("processRetailCostFile() - Caching item lookup");
			ItemDAO itemDAO = new ItemDAO();
			itemLookupMap = itemDAO.getAllItemsFromItemLookup(conn);
			logger.info("processRetailCostFile() - Cached items size - " + itemLookupMap.size());

			// getzip files
			ArrayList<String> zipFileList = getZipFiles(relativePath);

			// Start with -1 so that if any regular files are present, they are
			// processed first
			int curZipFileCount = -1;
			boolean processZipFile = false;

			String zipFilePath = getRootPath() + "/" + relativePath;
			do {
				ArrayList<String> fileList = null;
				boolean commit = true;

				

				try {
					if (processZipFile)
						PrestoUtil.unzip(zipFileList.get(curZipFileCount),
								zipFilePath);

					if (listFile.trim().isEmpty())
						fileList = getFiles(relativePath);
					else
						fileList = getFileList(relativePath, listFile);
					
					//Dinesh:: 12-Mar-2018, Added Code to delete Current and future weeks Cost for GE based on configuration
					int noOfFutureWeeksToLoad = Integer.parseInt(PropertyManager.getProperty("NO_OF_FUTURE_WEEKS_TO_LOAD_IN_COST","13"));
					if (delExitingCurAndFutCost) {
						logger.info("Deleting Existing Current and Future cost...");
						retailCostDAO.deleteCurAndFutureRetailCostData(conn, startDate, noOfFutureWeeksToLoad);
					}

					for (int i = 0; i < fileList.size(); i++) {

						long fileProcessStartTime = System.currentTimeMillis();

						recordCount = 0;
						ignoreCount = 0;
						retailCostDataMap = new HashMap<Integer, HashMap<ItemDetailKey, List<RetailCostDTO>>>();
						storeIdSet = new HashSet<String>();
						noItemCodeSet = new HashSet<String>();
						deptZoneMap = new HashMap<String, List<Integer>>();
						storeIdMap = new HashMap<String, Integer>();
						
						
						String files = fileList.get(i);
						logger.info("File Name - " + fileList.get(i));
						int stopCount = Integer.parseInt(PropertyManager
								.getProperty("DATALOAD.STOP_AFTER", "-1"));

						String fieldNames[]  = initializeFieldMap(files, '|');

						String[] fileFields = RetailPriceSetup.mapFileField(
								fieldNames, columnCount);

						logger.info("Processing Retail Cost Records ...");
						super.parseDelimitedFile(RetailCostDTO.class, files,
								'|', fileFields, stopCount);
						
						logger.info("Record Count - " + recordCount);
						logger.info("No of records Ignored - " + ignoreCount);
						if(Boolean.parseBoolean(PropertyManager.
								getProperty("LOAD_ALLOWANCE_AMT_AND_LONG_TRM_FLG", "FALSE"))){
							logger.debug("Calendar id: "+calendarId);
							retailCostDAO.updateListCost2(conn, calendarId);
						}
						PristineDBUtil.commitTransaction(conn,
								"Retail Cost Data Setup");

						logger.info("UPCs with no item code - "
								+ noItemCodeSet.size());

						// Change to handle stores with no zone
						logger.info("Number of Stores with no Zones - "
								+ storesWithNoZone.size());
						StringBuffer storesWithNoZoneStr = new StringBuffer();
						for (String storeNo : storesWithNoZone) {
							storesWithNoZoneStr.append(storeNo + "\t");
						}
						if (storesWithNoZoneStr.length() > 0)
							logger.info("Stores with no Zones - "
									+ storesWithNoZoneStr.toString());
						// Change to handle stores with no zone - Ends

						long fileProcessEndTime = System.currentTimeMillis();

						logger.info("Time taken to process the file - "
								+ (fileProcessEndTime - fileProcessStartTime)
								+ "ms");
						if(futureCostRecordsSkipped > 0){
							logger.info("processRetailCostFile() - # of future records skipped: " + futureCostRecordsSkipped);
						}
					}
				} catch (GeneralException ex) {
					logger.error("GeneralException", ex);
					commit = false;
				} catch (Exception ex) {
					logger.error("JavaException", ex);
					commit = false;
				}

				if (processZipFile) {
					PrestoUtil.deleteFiles(fileList);
					fileList.clear();
					fileList.add(zipFileList.get(curZipFileCount));
				}
				String archivePath = getRootPath() + "/" + relativePath + "/";

				if (commit) {
					PrestoUtil.moveFiles(fileList, archivePath
							+ Constants.COMPLETED_FOLDER);
				} else {
					PrestoUtil.moveFiles(fileList, archivePath
							+ Constants.BAD_FOLDER);
				}
				curZipFileCount++;
				processZipFile = true;
			} while (curZipFileCount < zipFileList.size());
		} catch (GeneralException ex) {
			logger.error("Outer Exception -  GeneralException", ex);
		} catch (Exception ex) {
			logger.error("Outer Exception - JavaException", ex);
		}

		logger.info("Retail Cost Setup Complete");

		super.performCloseOperation(conn, true);
		return;
	}

	/**
	 * 
	 * @param file
	 * @param delimiter
	 * @return field name map
	 * @throws GeneralException
	 */
	public String[] initializeFieldMap(String file, char delimiter) throws GeneralException {
		String fieldNames[] = null;
		try {
			CsvReader reader = new CsvReader(new FileReader(file));
			reader.setDelimiter(delimiter);
			String line[] = null;
			if (reader.readRecord()) {
				line = reader.getValues();
			}

			columnCount = line.length;
			fieldNames = new String[line.length];
			fieldNames[0] = "upc";
			fieldNames[1] = "retailerItemCode";
			fieldNames[2] = "levelTypeId";
			fieldNames[3] = "levelId";
			fieldNames[4] = "effListCostDate";
			fieldNames[5] = "listCost";
			fieldNames[6] = "dealStartDate";
			fieldNames[7] = "dealEndDate";
			fieldNames[8] = "dealCost";
			boolean loadLongTermFlgAndAllowance = Boolean.parseBoolean(PropertyManager.
					getProperty("LOAD_ALLOWANCE_AMT_AND_LONG_TRM_FLG", "FALSE"));
			if(loadLongTermFlgAndAllowance){
				logger.debug("Using allowance flag");
				fieldNames[9] = "allowanceAmount";
				fieldNames[10] = "longTermFlag";
				fieldNames[11] = "prcGrpCode";
			}else if (line.length > 9) {
				fieldNames[9] = "vipCost";
				fieldNames[10] = "avgCost";
				fieldNames[11] = "vendorName";
				fieldNames[12] = "vendorNumber";
			}
		} catch (Exception e) {
			throw new GeneralException("Error initializing feild map", e);
		}
		
		return fieldNames;
	}

	/**
	 * This method add the elements of a list to a hashmap with upc as the key.
	 * 
	 * @param listobj
	 *            List of RetailCostDTO parsed from input file
	 */
	@SuppressWarnings({ "unchecked" })
	@Override
	public void processRecords(List listobj) throws GeneralException {
		logger.debug("Inside processRecords of RetailCostSetup");
		List<RetailCostDTO> retailCostDataList = (List<RetailCostDTO>) listobj;

		if (retailCostDataList != null && retailCostDataList.size() > 0) {
			if (retailCostDataList.size() >= commitRecCount) {
				if (currentProcCostList.size() >= noOfRecsToBeProcessed) {
					String lastUPC = currentProcCostList.get(
							currentProcCostList.size() - 1).getUpc();
					boolean eofFlag = true;
					for (RetailCostDTO costDTO : retailCostDataList) {
						
						if (costDTO.getUpc().equals(lastUPC)) {
							currentProcCostList.add(costDTO);
						} else {
							if (currentProcCostList.size() >= noOfRecsToBeProcessed) {
								processCostRecords(currentProcCostList);
								if (currentProcCostList != null)
									currentProcCostList.clear();
								eofFlag = false;
							}
							currentProcCostList.add(costDTO);
						}
					}
					if (eofFlag) {
						processCostRecords(currentProcCostList);
						if (currentProcCostList != null)
							currentProcCostList.clear();
					}
				} else {
					currentProcCostList.addAll(retailCostDataList);
				}
			} else {
				currentProcCostList.addAll(retailCostDataList);
				processCostRecords(currentProcCostList);
				if (currentProcCostList != null)
					currentProcCostList.clear();
			}
		}
	}

	/**
	 * Processes Cost records (Populates retailCostDataMap with UPC as key and
	 * corresponding list of price records as value)
	 * 
	 * @param priceList
	 *            List of Cost records to be processed
	 */
	private void processCostRecords(List<RetailCostDTO> retailCostDataList) {
		// Populate HashMap from the list with UPC as key and list of
		// corresponding RetailCostDTO as it value
		try {
			for (RetailCostDTO retailCostDTO : retailCostDataList) {
				recordCount++;
				retailCostDTO.setCalendarId(calendarId);
				String upc = PrestoUtil.castUPC(retailCostDTO.getUpc(), false);
				String retailerItemCode = retailCostDTO.getRetailerItemCode();
				ItemDetailKey itemDetailKey =  new ItemDetailKey(upc, retailerItemCode);
				retailCostDTO.setPromotionFlag("N");
				if (retailCostDTO.getDealCost() != 0
						&& retailCostDTO.getDealCost() < retailCostDTO
								.getListCost()) {
					retailCostDTO.setPromotionFlag("Y");
				}
				
				// Changes to validate deal start and end date with processing week's start and end 
				if (loadDealOnly) {
					SimpleDateFormat dateFormat = new SimpleDateFormat(Constants.APP_DATE_FORMAT);
					Date prStart = dateFormat.parse(retailCostDTO.getDealStartDate());
					Date prEnd = dateFormat.parse(retailCostDTO.getDealEndDate());
					Date weekStart = dateFormat.parse(startDate);
					if (prStart.after(weekEndDate) || prEnd.before(weekStart)) {
						continue;
					}
				}
				
				
				retailCostDTO.setAllowStartDate(retailCostDTO.getDealStartDate());
				retailCostDTO.setAllowEndDate(retailCostDTO.getDealEndDate());
				// Populate StoreId in the global Set
				if (retailCostDTO.getLevelTypeId() == Constants.STORE_LEVEL_TYPE_ID)
					storeIdSet.add(retailCostDTO.getLevelId());
	
				if (retailCostDTO.getListCost() > maxCostAllowed
						|| retailCostDTO.getListCost() > maxCostAllowed) {
					ignoreCount++;
					continue;
				}
				
				if(useVendorCodeFromZone){
					String zoneArr[] = retailCostDTO.getLevelId().split("-");
					if(zoneArr.length > 3){
						retailCostDTO.setVendorName("VENDOR-" + zoneArr[3]);
						retailCostDTO.setVendorNumber(zoneArr[3]);
					}
				}
	
				// Handling future data in cost file starts...
				boolean isFutureCost = isFutureCost(
						retailCostDTO.getEffListCostDate(), weekEndDate);
				if (isFutureCost && !loadFutureCost) {
					futureCostRecordsSkipped++;
					continue;
				}
	
				if (isFutureCost) {
					if (calendarMap.get(retailCostDTO.getEffListCostDate()) != null) {
						int calId = calendarMap.get(retailCostDTO
								.getEffListCostDate());
						retailCostDTO.setCalendarId(calId);
					} else {
						try {
							RetailCalendarDTO calendarDTO = retailCalendarDAO
									.getCalendarId(conn,
											retailCostDTO.getEffListCostDate(),
											Constants.CALENDAR_WEEK);
							int calId = calendarDTO.getCalendarId();
							retailCostDTO.setCalendarId(calId);
							calendarMap.put(retailCostDTO.getEffListCostDate(),
									calId);
							logger.info("processCostRecords() - Future Cost Effective Date - "
									+ retailCostDTO.getEffListCostDate()
									+ "\t Calendar Id - " + calId);
						} catch (GeneralException exception) {
							logger.error("processCostRecords() - Error when retrieving calendar id for "
									+ retailCostDTO.getEffListCostDate()
									+ " "
									+ exception);
						}
					}
				}
				// Handling future data in cost file ends.
	
				boolean loadFutureDealInfo = Boolean.parseBoolean(PropertyManager.getProperty("LOAD_FUTURE_DEAL_INFO", "FALSE"));
				if(loadFutureDealInfo) {
					addFutureDealCostEntries(retailCostDTO, itemDetailKey);
				}else {
					boolean canBeAdded = true;
					if(retailCostDTO.getCalendarId() > 0){
						if (retailCostDataMap.get(retailCostDTO.getCalendarId()) != null) {
							HashMap<ItemDetailKey, List<RetailCostDTO>> tMap = retailCostDataMap
									.get(retailCostDTO.getCalendarId());
							if (tMap.get(itemDetailKey) != null) {
								List<RetailCostDTO> tempList = tMap.get(itemDetailKey);
			
								// Logic to handle duplicates in input file
								for (RetailCostDTO tempDTO : tempList) {
									if (tempDTO.getLevelId().equals(
											retailCostDTO.getLevelId())) {
										canBeAdded = false;
										// If dates are not equal compare dates and update
										// retail cost dto with most recent cost.
										compareDatesInRetailCost(tempDTO, retailCostDTO);
									}
								}
			
								if (canBeAdded) {
									tempList.add(retailCostDTO);
									tMap.put(itemDetailKey, tempList);
								}
							} else {
								List<RetailCostDTO> tempList = new ArrayList<RetailCostDTO>();
								tempList.add(retailCostDTO);
								tMap.put(itemDetailKey, tempList);
							}
							retailCostDataMap.put(retailCostDTO.getCalendarId(), tMap);
						} else {
							HashMap<ItemDetailKey, List<RetailCostDTO>> tMap = new HashMap<ItemDetailKey, List<RetailCostDTO>>();
							List<RetailCostDTO> tempList = new ArrayList<RetailCostDTO>();
							tempList.add(retailCostDTO);
							tMap.put(itemDetailKey, tempList);
							retailCostDataMap.put(retailCostDTO.getCalendarId(), tMap);
						}
					}else{
						logger.warn("processCostRecords() - Calendar Id not found for " + retailCostDTO.getEffListCostDate());
					}
				}
				if (recordCount % Constants.RECS_TOBE_PROCESSED == 0)
					logger.info("No of cost records Processed " + recordCount);
			}
			logger.info("No of cost records Processed - " + recordCount);
	
			retailCostDataList = null;
			List<RetailCostDTO> futureCostDeleteList = new ArrayList<>();
			List<RetailCostDTO> futureCostList = new ArrayList<>();
			
			loadCostData(futureCostList, futureCostDeleteList);

			logger.info("# of records with future price: " + futureCostList.size());
			
			loadFutureCostData(futureCostList);
			
			deleteTempRecords();
			
			deleteFutureCostsForCostChangedItems(futureCostDeleteList);
		
		} catch (GeneralException exception) {
			logger.error("Exception in processCostRecords of RetailCostSetup - "
					+ exception);
			exception.printStackTrace();
		}
		catch (Exception exception) {
			logger.error("Exception in processCostRecords of RetailCostSetup - "
					+ exception);
			exception.printStackTrace();
		}
	}

	
	/**
	 * 
	 * @param retailCostDTO
	 * @param itemDetailKey
	 * @throws Exception
	 */
	private void addFutureDealCostEntries(RetailCostDTO retailCostDTO, ItemDetailKey itemDetailKey) throws Exception {
		if(itemLookupMap.containsKey(itemDetailKey)) {
			List<RetailCostDTO> futureCostList = new ArrayList<>();
			int noOfFutureWeeksToLoad = Integer
					.parseInt(PropertyManager.getProperty("NO_OF_FUTURE_WEEKS_TO_LOAD_IN_COST", "13"));
			
			int daysToBeAdded = Integer.parseInt(PropertyManager.getProperty("DAYS_TO_ADD_FOR_BUSINESS_CAL", "0"));
			boolean isFutureDealStart = isFutureCost(retailCostDTO.getDealStartDate(), weekEndDate);

			boolean isFutureDealEnd = isFutureCost(retailCostDTO.getDealEndDate(), weekEndDate);
			SimpleDateFormat dateFormat = new SimpleDateFormat(Constants.APP_DATE_FORMAT);
			Date prStart = dateFormat.parse(retailCostDTO.getDealStartDate());
			Date prEnd = dateFormat.parse(retailCostDTO.getDealEndDate());
			Date prActualEnd = dateFormat.parse(retailCostDTO.getDealEndDate());
			
			long diff = prEnd.getTime() - prStart.getTime();
			long diffDates = TimeUnit.DAYS.convert(diff, TimeUnit.MILLISECONDS);
			if (isFutureDealStart && isFutureDealEnd) {
				if (diffDates > 7) {
					for (int i = 0; i < noOfFutureWeeksToLoad; i++) {
						RetailCostDTO newCostDTO = (RetailCostDTO) retailCostDTO.clone();
						newCostDTO.setDealStartDate(dateFormat.format(prStart));
						prEnd = DateUtil.incrementDate(prStart, 6);
						prStart = DateUtil.incrementDate(prStart, 7);
						newCostDTO.setDealEndDate(dateFormat.format(prEnd));
						if(prStart.after(prActualEnd)) {
							break;
						}
						futureCostList.add(newCostDTO);
					}
				} else {
					futureCostList.add(retailCostDTO);
				}
			}

			if (!isFutureDealStart && isFutureDealEnd) {
				if (diffDates > 7) {
					for (int i = 0; i < noOfFutureWeeksToLoad; i++) {
						RetailCostDTO newCostDTO = (RetailCostDTO) retailCostDTO.clone();
						if(i == 0) {
							prStart = DateUtil.incrementDate(weekEndDate, 1);	
						}
						newCostDTO.setDealStartDate(dateFormat.format(prStart));
						prEnd = DateUtil.incrementDate(prStart, 6);
						prStart = DateUtil.incrementDate(prStart, 7);
						newCostDTO.setDealEndDate(dateFormat.format(prEnd));
						if(prStart.after(prActualEnd)) {
							break;
						}
						futureCostList.add(newCostDTO);
					}
				} else {
					futureCostList.add(retailCostDTO);
				}
			}

			futureCostList.forEach(retailCost -> {

				try {
					String dealStartDate = dateFormat.format(
							DateUtil.incrementDate(dateFormat.parse(retailCost.getDealStartDate()), daysToBeAdded));
					if (calendarMap.get(dealStartDate) != null) {
						int calId = calendarMap.get(dealStartDate);
						retailCost.setCalendarId(calId);
					} else {
						try {
							RetailCalendarDTO calendarDTO = retailCalendarDAO.getCalendarId(conn,
									dealStartDate, Constants.CALENDAR_WEEK);
							int calId = calendarDTO.getCalendarId();
							retailCost.setCalendarId(calId);
							calendarMap.put(dealStartDate, calId);
						} catch (GeneralException exception) {
							logger.error("processCostRecords() - Error when retrieving calendar id for "
									+ retailCost.getDealStartDate() + " " + exception);
						}
					}

					if (retailCost.getCalendarId() > 0) {
						if (retailCostDataMap.get(retailCost.getCalendarId()) != null) {
							HashMap<ItemDetailKey, List<RetailCostDTO>> tMap = retailCostDataMap
									.get(retailCost.getCalendarId());
							List<RetailCostDTO> tempList = new ArrayList<>();
							if (tMap.get(itemDetailKey) != null) {
								tempList = tMap.get(itemDetailKey);
							}
							tempList.add(retailCost);
							tMap.put(itemDetailKey, tempList);
							retailCostDataMap.put(retailCost.getCalendarId(), tMap);
						} else {
							HashMap<ItemDetailKey, List<RetailCostDTO>> tMap = new HashMap<ItemDetailKey, List<RetailCostDTO>>();
							List<RetailCostDTO> tempList = new ArrayList<RetailCostDTO>();
							tempList.add(retailCost);
							tMap.put(itemDetailKey, tempList);
							retailCostDataMap.put(retailCost.getCalendarId(), tMap);
						}
					}
				} catch (ParseException e) {
					// TODO Auto-generated catch block
					e.printStackTrace();
				}
			});
		}else {
			noItemCodeSet.add(itemDetailKey.getUpc());
		}
	}
	
	/**
	 * @param futureCostList
	 * @param futureCostDeleteList
	 * @throws GeneralException
	 * @throws Exception
	 */
	private void loadCostData(List<RetailCostDTO> futureCostList, 
			List<RetailCostDTO> futureCostDeleteList) throws GeneralException, Exception {
		RetailPriceDAO retailPriceDAO = new RetailPriceDAO();
		RetailCostDAO retailCostDAO = new RetailCostDAO();
		chainId = retailPriceDAO.getChainId(conn);
		HashSet<ItemDetailKey> itemDetailSet = new HashSet<ItemDetailKey>();
		for (Map.Entry<Integer, HashMap<ItemDetailKey, List<RetailCostDTO>>> entry : retailCostDataMap
				.entrySet()) {
			List<RetailCostDTO> toBeInserted = new ArrayList<RetailCostDTO>();
			List<RetailCostDTO> toBeUpdated = new ArrayList<RetailCostDTO>();
			List<RetailCostDTO> toBeDeleted = new ArrayList<RetailCostDTO>();
			
			// Populate ITEM_CODE for every UPC
			logger.info("Number of distinct UPCs - " + entry.getValue().size());
			logger.info("Number of stores - " + storeIdSet.size());

			
			//Added for handling multiple retailer item code with same upc. Pradeep 03/27/2015.
			Collection<List<RetailCostDTO>> costCollection  = entry.getValue().values();
			for(List<RetailCostDTO> list: costCollection){
				for(RetailCostDTO inputCost: list){
					ItemDetailKey itemDetailKey =  new ItemDetailKey(PrestoUtil.castUPC(inputCost.getUpc(), false), inputCost.getRetailerItemCode());
					itemDetailSet.add(itemDetailKey);
					String itemcode = itemLookupMap.get(itemDetailKey);
					inputCost.setItemcode(itemcode);
				}
			}
			
			logger.info("No of items - " + itemDetailSet.size());

			/*long getItemCodeStartTime = System.currentTimeMillis();
			itemCodeMap = retailPriceDAO.getItemCode(conn, entry.getValue()
					.keySet());
			long getItemCodeEndTime = System.currentTimeMillis();
			logger.info("Time taken to retrieve item codes - "
					+ (getItemCodeEndTime - getItemCodeStartTime) + "ms");*/
			priceZoneIdMap = retailPriceDAO.getPriceZoneData(conn);
			storeIdMap = retailPriceDAO.getStoreIdMap(conn, Integer.parseInt(chainId));
			deptZoneMap = retailPriceDAO.getDeptZoneInfo(conn, chainId);
			// populate vendor info into vendor lookup...
			HashMap<String, Long> vendorIdMap = new HashMap<>();
			StoreItemMapService storeItemMapService = new StoreItemMapService();
			if(loadVendorInfo){
				logger.info("Setting up vendor information...");
				long vendorStart = System.currentTimeMillis();
				vendorIdMap = storeItemMapService.setupVendorInfo(conn, entry
						.getValue().values());
				long vendorEnd = System.currentTimeMillis();
				logger.info("Time taken to setup vendor information - "
						+ (vendorEnd - vendorStart) + "ms");
			}
			// merging records with store item map...
			if (syncStoreItemMap) {
				storeItemMapService.mapItemsWithStore(conn, entry.getValue().values(), itemLookupMap, storeIdMap,
						priceZoneIdMap, noItemCodeSet, deptZoneMap, vendorIdMap, storeZoneMap);
			}

			if (mode != null
					&& mode.equalsIgnoreCase(Constants.DATA_LOAD_DELTA)) {
				//Identify calendars which are future
				if(entry.getKey() != calendarId){
					for(List<RetailCostDTO> list: costCollection){
						futureCostList.addAll(list);
					}
					continue;
				}
				
				// Item code List to be processed
				HashMap<ItemDetailKey, List<RetailCostDTO>> retailCostDTOMap = new HashMap<ItemDetailKey, List<RetailCostDTO>>();
				Set<String> itemCodeSet = new HashSet<String>();
				for (ItemDetailKey itemDetailKey : entry.getValue().keySet()) {
					String itemCode = itemLookupMap.get(itemDetailKey);
					if (itemCode != null) {
						itemCodeSet.add(itemCode);
					}
				}

				// Retrieve item store mapping from store_item_map table for
				// items in
				// itemCodeList
				long startTime = System.currentTimeMillis();
				HashMap<StoreItemKey, List<String>> itemStoreMapping = retailCostDAO
						.getStoreItemMap(conn, prevWkStartDate, itemCodeSet);
				long endTime = System.currentTimeMillis();
				logger.info("Time taken to retrieve items from store_item_map - "
						+ (endTime - startTime) + "ms");
				logger.info("store_item_map size - " + itemStoreMapping.size());
				// Retrieve from Retail_Cost_Info for items in itemCodeList
				
				HashMap<String, List<RetailCostDTO>> tempMapForCurrentWeek = retailCostDAO.getRetailCostFromTemp(conn, calendarId);
				
				for (String itemCode: tempMapForCurrentWeek.keySet()) {
					itemCodeSet.add(itemCode);
				}
				
				startTime = System.currentTimeMillis();
				HashMap<String, List<RetailCostDTO>> costRolledUpMapForItems = retailCostDAO
						.getRetailCostInfo(conn, itemCodeSet, calendarId, false);
				endTime = System.currentTimeMillis();
				logger.info("Time taken to retrieve items from retail_cost_info - "
						+ (endTime - startTime) + "ms");
				
				// Unroll previous week's cost data for items in itemCodeList
				HashMap<String, List<RetailCostDTO>> unrolledCostMapForItems = new HashMap<String, List<RetailCostDTO>>();
				startTime = System.currentTimeMillis();
				if (costRolledUpMapForItems != null
						&& costRolledUpMapForItems.size() > 0) {
					/*
					 * MovementWeeklyDataLoadV2 movementDataLoad = new
					 * MovementWeeklyDataLoadV2( conn);
					 */
					HashMap<UnrollingKey, List<RetailCostDTO>> unrolledCostMap = unrollRetailCostInfo(
							costRolledUpMapForItems, storeMap.keySet(),
							zoneStoreMap, retailPriceDAO, itemStoreMapping,
							storeMap);
					

					logger.info("loadCostData() - Data unrolled.");

					for (List<RetailCostDTO> retailCostDTOList : unrolledCostMap
							.values()) {
						for (RetailCostDTO retailCostDTO : retailCostDTOList) {
							if (unrolledCostMapForItems.get(retailCostDTO
									.getItemcode()) != null) {
								List<RetailCostDTO> tempList = unrolledCostMapForItems
										.get(retailCostDTO.getItemcode());
								tempList.add(retailCostDTO);
								unrolledCostMapForItems.put(
										retailCostDTO.getItemcode(), tempList);
							} else {
								List<RetailCostDTO> tempList = new ArrayList<RetailCostDTO>();
								tempList.add(retailCostDTO);
								unrolledCostMapForItems.put(
										retailCostDTO.getItemcode(), tempList);
							}
						}
					}
				}
				
				endTime = System.currentTimeMillis();
				logger.info("Time taken to unroll cost data - "
						+ (endTime - startTime) + "ms");

				HashMap<ItemDetailKey, List<RetailCostDTO>> inputRetailCost = entry
						.getValue();
				
				identifyAndConvertFutureAsRegular(inputRetailCost, tempMapForCurrentWeek);
				
				for (ItemDetailKey itemDetailKey : entry.getValue().keySet()) {
					String itemCode = itemLookupMap.get(itemDetailKey);
					List<RetailCostDTO> finalListToBeInserted = new ArrayList<RetailCostDTO>();
					if (itemCode != null) {
						List<RetailCostDTO> costDataList = inputRetailCost
								.get(itemDetailKey);
						List<RetailCostDTO> retailCostList = unrolledCostMapForItems
								.get(itemCode);

						if (retailCostList != null && retailCostList.size() > 0) {
							logger.debug("Inside comparison with unrolled data..");
							for (RetailCostDTO inputRetailDTO : costDataList) {
								for (RetailCostDTO retailCostDTO : retailCostList) {
									if (retailCostDTO.getLevelId().equals(
											inputRetailDTO.getLevelId())
											&& retailCostDTO.getLevelTypeId() == inputRetailDTO
													.getLevelTypeId()) {
										identifyCostChange(inputRetailDTO, retailCostDTO, futureCostDeleteList);
										// added by kirthi, to load the deal cost if exist
										if (loadDealOnly) {
											retailCostDTO.setDealCost(inputRetailDTO.getDealCost());
											retailCostDTO.setDealStartDate(inputRetailDTO.getDealStartDate());
											retailCostDTO.setDealEndDate(inputRetailDTO.getDealEndDate());
										}
										
										else {
											retailCostDTO.copy(inputRetailDTO);
										}
										
										
										logger.debug("Inside comparison with unrolled data..");
										retailCostDTO.setCalendarId(calendarId);
										retailCostDTO.setUpc(itemDetailKey.getUpc());
										inputRetailDTO.setProcessedFlag(true);
										retailCostDTO.setProcessedFlag(true);
										finalListToBeInserted
												.add(retailCostDTO);
									}else if(retailCostDTO.getLevelTypeId() == Constants.STORE_LEVEL_TYPE_ID
											&& inputRetailDTO.getLevelTypeId() == Constants.ZONE_LEVEL_TYPE_ID
											&& loadDealOnly) {
										if(zoneStoreMap.containsKey(inputRetailDTO.getLevelId())) {
											List<String> stores = zoneStoreMap.get(inputRetailDTO.getLevelId());
											if(stores.contains(retailCostDTO.getLevelId())) {
												retailCostDTO.setDealCost(inputRetailDTO.getDealCost());
												retailCostDTO.setDealStartDate(inputRetailDTO.getDealStartDate());
												retailCostDTO.setDealEndDate(inputRetailDTO.getDealEndDate());
												logger.debug("Inside comparison with unrolled data..");
												retailCostDTO.setCalendarId(calendarId);
												retailCostDTO.setUpc(itemDetailKey.getUpc());
												inputRetailDTO.setProcessedFlag(true);
												retailCostDTO.setProcessedFlag(true);
												finalListToBeInserted
														.add(retailCostDTO);
											}
										}
									}
								}
							}
						}
						
						if(!loadDealOnly) {
							// Process cost of an item in a store that is present
							// only in
							// input file and not in retail_cost_info
							for (RetailCostDTO inputDTO : costDataList) {
								if (!inputDTO.isProcessedFlag()) {
									RetailCostDTO retailCostDTO = new RetailCostDTO();
									retailCostDTO.copy(inputDTO);
									retailCostDTO.setCalendarId(calendarId);
									retailCostDTO.setItemcode(itemCode);
									retailCostDTO.setUpc(itemDetailKey.getUpc());
									inputDTO.setProcessedFlag(true);
									finalListToBeInserted.add(retailCostDTO);
								}
							}	
						}
						

						retailCostDTOMap.put(itemDetailKey, finalListToBeInserted);
					}

				}
				// Process cost of an item in a store that is present only in
				// retail_cost_info and not in input file
				for (String itemCode : costRolledUpMapForItems.keySet()) {
					ItemDetailKey itemDetailKey = getItemDetailKey(itemCode);
					if(itemDetailKey == null){
						throw new Exception("Unable to get item detail");
					}
					List<RetailCostDTO> finalRetailCostList = new ArrayList<RetailCostDTO>();
					List<RetailCostDTO> retailCostList = unrolledCostMapForItems
							.get(itemCode);
					if (retailCostList != null && retailCostList.size() > 0) {
						for (RetailCostDTO retailCostDTO : retailCostList) {
							retailCostDTO.setCalendarId(calendarId);
							if (!retailCostDTO.isProcessedFlag()) {
								finalRetailCostList.add(retailCostDTO);
							}
						}
					}
					if (retailCostDTOMap.get(itemDetailKey) != null) {
						List<RetailCostDTO> tempList = retailCostDTOMap
								.get(itemDetailKey);
						tempList.addAll(finalRetailCostList);
						retailCostDTOMap.put(itemDetailKey, tempList);
					} else {
						retailCostDTOMap.put(itemDetailKey, finalRetailCostList);
					}

				}

				logger.debug("No of records to be rolled up - " + retailCostDTOMap.size());
				costRolledUpMapForItems = null;
				unrolledCostMapForItems = null;
				itemStoreMapping = null;
				retailCostDataMap = new HashMap<Integer, HashMap<ItemDetailKey, List<RetailCostDTO>>>();
				long costRollupStartTime = System.currentTimeMillis();
				HashMap<String, List<RetailCostDTO>> costRolledUpMap = costRollUpV2(
						retailCostDTOMap, itemLookupMap, storeMap, noItemCodeSet,
						chainId);
				
				logger.debug("No of records to be rolled up - " + costRolledUpMap.size());
				long costRollupEndTime = System.currentTimeMillis();
				logger.info("Time taken for cost rollup - "
						+ (costRollupEndTime - costRollupStartTime) + "ms");
				retailCostDTOMap = new HashMap<ItemDetailKey, List<RetailCostDTO>>();
				// Delete existing data
				startTime = System.currentTimeMillis();
				List<String> itemCodeList = new ArrayList<String>(itemCodeSet);
				retailCostDAO.deleteRetailCostData(conn, calendarId,
						itemCodeList);
				endTime = System.currentTimeMillis();
				logger.info("Time taken for deleting data from retail_cost_info - "
						+ (endTime - startTime));

				// Insert into retail_cost_info table
				List<RetailCostDTO> insertList = new ArrayList<RetailCostDTO>();
				for (List<RetailCostDTO> costDTOList : costRolledUpMap.values()) {
					for (RetailCostDTO retailCostDTO : costDTOList) {
						insertList.add(retailCostDTO);
					}
				}
				
				updateLocationId(insertList);
				
				startTime = System.currentTimeMillis();
				retailCostDAO.saveRetailCostData(conn, insertList);
				endTime = System.currentTimeMillis();
				logger.info("Time taken for inserting data into retail_cost_info - "
						+ (endTime - startTime));

			}

			else {

				if(rollupDSDToWhseEnabled){
		    		long dsdRollupStartTime = System.currentTimeMillis();
		    		//Rollup DSD level prices to warehouse level. Add an artificial entry 
					rollupDSDZonesToWhseZone(entry.getValue(), itemLookupMap, itemCodeCategoryMap, dsdAndWhseZoneMap);	
			    	long dsdRollupEndTime = System.currentTimeMillis();
			    	
			    	logger.info("Time taken for DSD rollup - "
							+ (dsdRollupEndTime - dsdRollupStartTime) + "ms");
		    	}
				
				// Cost Roll Up
				long costRollupStartTime = System.currentTimeMillis();
				HashMap<String, List<RetailCostDTO>> costRolledUpMap = costRollUpV2(
						entry.getValue(), itemLookupMap, storeMap, noItemCodeSet,
						chainId);
				long costRollupEndTime = System.currentTimeMillis();
				logger.info("Time taken for cost rollup - "
						+ (costRollupEndTime - costRollupStartTime) + "ms");

				// Make the object available for GC
				retailCostDataMap = new HashMap<Integer, HashMap<ItemDetailKey, List<RetailCostDTO>>>();

				// Compare new records with existing records in the database
				long compareEntriesStartTime = System.currentTimeMillis();
				compareExistingEntries(conn, retailCostDAO, costRolledUpMap,
						toBeInserted, toBeUpdated, toBeDeleted, entry.getKey());
				long compareEntriesEndTime = System.currentTimeMillis();
				logger.info("Time taken to compare records - "
						+ (compareEntriesEndTime - compareEntriesStartTime)
						+ "ms");
				logger.info("No of records to be inserted - "
						+ toBeInserted.size());
				logger.info("No of records to be deleted - "
						+ toBeDeleted.size());
				logger.info("No of records to be updated - "
						+ toBeUpdated.size());

				// Delete retail cost data
				long deleteStartTime = System.currentTimeMillis();
				retailCostDAO.deleteRetailCostData(conn, toBeDeleted);
				long deleteEndTime = System.currentTimeMillis();
				logger.info("Time taken for deleting records - "
						+ (deleteEndTime - deleteStartTime) + "ms");

				updateLocationId(toBeUpdated);
				
				// Update retail cost data
				long updateStartTime = System.currentTimeMillis();
				retailCostDAO.updateRetailCostData(conn, toBeUpdated);
				long updateEndTime = System.currentTimeMillis();
				logger.info("Time taken for updating records - "
						+ (updateEndTime - updateStartTime) + "ms");

				updateLocationId(toBeInserted);
				
				// Insert retail cost data
				long insertStartTime = System.currentTimeMillis();
				retailCostDAO.saveRetailCostData(conn, toBeInserted);
				long insertEndTime = System.currentTimeMillis();
				logger.info("Time taken for inserting records - "
						+ (insertEndTime - insertStartTime) + "ms");
				// Restoring current week data from temp table.
			}
		}

	}
	
	
	/**
	 * Clears and Loads future cost data
	 * @throws GeneralException
	 */
	private void loadFutureCostData(List<RetailCostDTO> futureCostList) throws GeneralException{
		if(futureCostList != null && futureCostList.size() > 0){
			logger.info("loadFutureCostData() - Handling future cost data...");
			updateLocationId(futureCostList);
			// Insert retail cost data
			logger.info("loadFutureCostData() - Deleting future cost...");
			long startTime = System.currentTimeMillis();
			retailCostDAO.deleteFutureCostData(conn, futureCostList, startDate);
			long endTime = System.currentTimeMillis();
			logger.info("loadFutureCostData() - Time taken for deleting records - "
					+ (endTime - startTime) + "ms");
			logger.info("loadFutureCostData() - Inserting future cost...");
			long insertStartTime = System.currentTimeMillis();
			retailCostDAO.saveRetailCostData(conn, futureCostList);
			long insertEndTime = System.currentTimeMillis();
			logger.info("loadFutureCostData() - Time taken for inserting records - "
					+ (insertEndTime - insertStartTime) + "ms");
		}
	}
	
	
	/**
	 * Deletes record in temp table records.
	 * @throws GeneralException
	 */
	private void deleteTempRecords() throws GeneralException{
		if(mode != null
				&& mode.equalsIgnoreCase(Constants.DATA_LOAD_DELTA)){
			logger.info("deleteTempRecords() - Deleting temp records...");
			long startTime = System.currentTimeMillis();
			retailCostDAO.deleteTempRecords(conn, calendarId);
			long endTime = System.currentTimeMillis();
			logger.info("deleteTempRecords() - Time taken for deleting records - "
					+ (endTime - startTime) + "ms");
		}
	}
	
	/**
	 * Deletes future costs for which the cost change is happened
	 * @param futureCostDeleteList
	 * @throws GeneralException
	 */
	private void deleteFutureCostsForCostChangedItems(List<RetailCostDTO> futureCostDeleteList) 
			throws GeneralException{
		if(futureCostDeleteList != null && futureCostDeleteList.size() > 0){
			logger.info("deleteFutureCostsForCurrentCostChangeItems() - Deleting future cost for " 
						+ futureCostDeleteList.size() + " records.");
			long startTime = System.currentTimeMillis();
			retailCostDAO.deleteFutureCostData(conn, futureCostDeleteList, startDate);
			long endTime = System.currentTimeMillis();
			logger.info("deleteFutureCostsForCurrentCostChangeItems() - Time taken for deleting records - "
					+ (endTime - startTime) + "ms");
		}
	}
	
	/**
	 * This method compares dates in two retail cost dto and sets the most
	 * recent price values
	 * 
	 * @param tempDTO
	 *            RetailCostDTO to be compared for the dates
	 * @param retailCostDTO
	 *            RetailCostDTO to be compared for the dates
	 */
	private void compareDatesInRetailCost(RetailCostDTO tempDTO,
			RetailCostDTO retailCostDTO) {
		logger.debug("Inside compareDatesInRetailCost() of RetailCostSetup");
		try {
			if (!tempDTO.getEffListCostDate().isEmpty()
					&& !retailCostDTO.getEffListCostDate().isEmpty())
				if (!tempDTO.getEffListCostDate().equals(
						retailCostDTO.getEffListCostDate())) {
					SimpleDateFormat sdf = new SimpleDateFormat("MM/dd/yy");
					java.util.Date tempDate = sdf.parse(tempDTO
							.getEffListCostDate());
					java.util.Date retailDate = sdf.parse(retailCostDTO
							.getEffListCostDate());
					// Changes to handle future cost data
					boolean isTempDateInFuture = isFutureCost(
							tempDTO.getEffListCostDate(), weekEndDate);
					boolean isRetailDateInFuture = isFutureCost(
							retailCostDTO.getEffListCostDate(), weekEndDate);

					if (!isTempDateInFuture && !isRetailDateInFuture)
						// Changes to handle future cost data Ends
						if (compareTo(tempDate, retailDate) > 0) {
							// tempDate is after retailDate
							retailCostDTO.setEffListCostDate(tempDTO
									.getEffListCostDate());
							retailCostDTO.setListCost(tempDTO.getListCost());
						}
				}

			/*
			 * if (PrestoUtil.compareTo(tempDate, retailDate) > 0) { // tempDate
			 * is after retailDate
			 * retailCostDTO.setEffListCostDate(tempDTO.getEffListCostDate());
			 * retailCostDTO.setListCost(tempDTO.getListCost()); }
			 */

			if (!tempDTO.getDealStartDate().isEmpty()
					&& !retailCostDTO.getDealStartDate().isEmpty())
				if (!tempDTO.getDealStartDate().equals(
						retailCostDTO.getDealStartDate())) {
					SimpleDateFormat sdf = new SimpleDateFormat("MM/dd/yy");
					java.util.Date tempDate = sdf.parse(tempDTO
							.getDealStartDate());
					java.util.Date retailDate = sdf.parse(retailCostDTO
							.getDealStartDate());

					if (PrestoUtil.compareTo(tempDate, retailDate) > 0) {
						// tempDate is after retailDate
						retailCostDTO.setDealStartDate(tempDTO
								.getDealStartDate());
						retailCostDTO.setDealEndDate(tempDTO.getDealEndDate());
						retailCostDTO.setDealCost(tempDTO.getDealCost());
					}
				}
		} catch (ParseException pe) {
			logger.error("Parser Error - " + pe.getMessage());
		}
	}

	
	
	

	/**
	 * This method compares two dates
	 * 
	 * @param date1
	 *            Date to compare
	 * @param date2
	 *            Date to compare
	 * @return
	 */
	public static long compareTo(java.util.Date date1, java.util.Date date2) {
		// returns negative value if date1 is before date2
		// returns 0 if dates are even
		// returns positive value if date1 is after date2
		return date1.getTime() - date2.getTime();
	}

	/**
	 * This method compares cost rolled up records with existing entries in
	 * database and determines what records needs to be inserted, updated,
	 * deleted.
	 * 
	 * @param costRolledUpMap
	 *            Contains Item Code as key and list of cost rolled up records
	 *            for that item code as value
	 */
	public void compareExistingEntries(Connection conn,
			RetailCostDAO retailCostDAO,
			HashMap<String, List<RetailCostDTO>> costRolledUpMap,
			List<RetailCostDTO> toBeInserted, List<RetailCostDTO> toBeUpdated,
			List<RetailCostDTO> toBeDeleted, int proCalendarId)
			throws GeneralException {
		logger.debug("Inside compareExistingEntries() of RetailCostSetup");

		HashMap<String, List<RetailCostDTO>> retailCostDBMap = retailCostDAO
				.getRetailCostInfo(conn, costRolledUpMap.keySet(),
						proCalendarId, false);

		for (Map.Entry<String, List<RetailCostDTO>> entry : costRolledUpMap
				.entrySet()) {

			List<RetailCostDTO> retailCostDBList = retailCostDBMap.get(entry
					.getKey());

			// If there are no existing records for that Item Code insert all
			// new records
			if (retailCostDBList == null
					|| (retailCostDBList != null && retailCostDBList.isEmpty())) {
				toBeInserted.addAll(entry.getValue());
				continue;
			}

			if (retailCostDBList != null && !retailCostDBList.isEmpty()) {
				// Compare new list and db list to find out records that needs
				// to be updated
				for (RetailCostDTO retailCostNew : entry.getValue()) {

					for (RetailCostDTO retailCostDB : retailCostDBList) {
						if (retailCostNew.getLevelTypeId() == retailCostDB
								.getLevelTypeId()
								&& retailCostNew.getLevelId().equals(
										retailCostDB.getLevelId())) {
							if (retailCostNew.getPromotionFlag().equals(
									retailCostDB.getPromotionFlag())) {
								if (!((retailCostNew.getListCost() == retailCostDB
										.getListCost())
										&& (retailCostNew.getDealCost() == retailCostDB
												.getDealCost()) && (retailCostNew
											.getVipCost() == retailCostDB
										.getVipCost()))) {
									toBeUpdated.add(retailCostNew);
								}
							} else {
								toBeUpdated.add(retailCostNew);
							}
							retailCostNew.setProcessedFlag(true);
							retailCostDB.setProcessedFlag(true);
						}
					}

				}

				// Check for not processed records in new list for inserting
				// into database
				for (RetailCostDTO retailCostNew : entry.getValue()) {
					if (!retailCostNew.isProcessedFlag()) {
						toBeInserted.add(retailCostNew);
					}
				}

				// Check for not processed records in db list for deleting from
				// database
				for (RetailCostDTO retailCostDB : retailCostDBList) {
					if (!retailCostDB.isProcessedFlag()) {
						toBeDeleted.add(retailCostDB);
					}
				}
			}
		}
	}

	private boolean isFutureCost(String regEffectiveDate, Date weekEndDate) {
		boolean isFuturePrice = false;
		if (regEffectiveDate != null) {
			try {
				Date regEffDate = DateUtil.toDate(regEffectiveDate);
				if (compareTo(regEffDate, weekEndDate) > 0) {
					isFuturePrice = true;
				}
			} catch (GeneralException exception) {
				logger.error("Error when parsing date - " + regEffectiveDate);
			}
		}
		return isFuturePrice;
	}

	

	public void setupVipCost(RetailCostDTO retailCostDTO) {
		// Make Vip cost as zero when list cost and vip cost are same.
		if (retailCostDTO.getVipCost() == retailCostDTO.getListCost()) {
			retailCostDTO.setVipCost(0);
		}
	}

	public void castZoneNumbers(HashMap<String, String> storeNumberMap) {
		for (Map.Entry<String, String> entry : storeNumberMap.entrySet()) {
			String zone = PrestoUtil.castZoneNumber(entry.getValue());
			storeNumberMap.put(entry.getKey(), zone);
		}
	}

	/**
	 * Returns a Map containing zone numbers and list of stores under each zone
	 * 
	 * @param storeNumberMap
	 *            HashMap containing store no and its corresponding zone no
	 * @return HashMap
	 */
	public HashMap<String, List<String>> getZoneMapping(
			HashMap<String, String> storeNumberMap) {
		HashMap<String, List<String>> zoneStoreMap = new HashMap<String, List<String>>();
		String storeNo = null, zoneNo = null;
		for (Map.Entry<String, String> entry : storeNumberMap.entrySet()) {
			storeNo = entry.getKey();
			zoneNo = PrestoUtil.castZoneNumber(entry.getValue());
			if (zoneStoreMap.get(zoneNo) != null) {
				List<String> storeNoList = zoneStoreMap.get(zoneNo);
				storeNoList.add(storeNo);
				zoneStoreMap.put(zoneNo, storeNoList);
			} else {
				List<String> storeNoList = new ArrayList<String>();
				storeNoList.add(storeNo);
				zoneStoreMap.put(zoneNo, storeNoList);
			}
		}
		return zoneStoreMap;
	}

	/**
	 * This method unrolls data from retail cost info to be loaded in movement
	 * weekly table
	 * 
	 * @param costRolledUpMap
	 *            Map containing item code as key and list of rolled up cost
	 *            from retail cost info as value
	 * @param storeNumbers
	 *            Set of stores available for Price Index
	 * @param zoneStoreMap
	 *            Map containing zone number as key and store number as value
	 * @return HashMap containing store number as key and list of unrolled
	 *         retail cost info as value
	 */
	public HashMap<UnrollingKey, List<RetailCostDTO>> unrollRetailCostInfo(
			HashMap<String, List<RetailCostDTO>> costRolledUpMap,
			Set<String> storeNumbers,
			HashMap<String, List<String>> zoneStoreMap,
			RetailPriceDAO retailPriceDAO,
			HashMap<StoreItemKey, List<String>> storeItemMap,
			HashMap<String, String> storeNumberMap) throws GeneralException {
		logger.debug("Inside unrollRetailCostInfo() of MovementWeeklyDataLoadV2");
		HashMap<UnrollingKey, List<RetailCostDTO>> unrolledCostMap = new HashMap<UnrollingKey, List<RetailCostDTO>>();

		RetailCostDTO chainLevelData = null;
		boolean isChainLevelPresent = false;
		RetailCostDTO zoneLevelData = null;
		Set<String> unrolledStores = null;
		HashMap<String, List<String>> deptZoneMap = retailPriceDAO
				.getDeptZoneMap(conn, chainId);
		HashMap<String, List<String>> storeDeptZoneMap = retailPriceDAO
				.getStoreDeptZoneMap(conn, chainId);
		logger.info("unrollRetailCostInfo() - Getting store ids..");
		HashMap<String, Integer> storeIdsMap = retailPriceDAO.getStoreId(conn,
				Integer.parseInt(chainId), storeNumbers);
		logger.info("unrollRetailCostInfo() - Store ids fetched. Size - " + storeIdsMap.size());
		for (Map.Entry<String, List<RetailCostDTO>> entry : costRolledUpMap
				.entrySet()) {
			unrolledStores = new HashSet<String>();
			isChainLevelPresent = false;
			RetailCostDTO chainLevelDTO = null;
			RetailCostDTO zoneLevelDTO = null;
			for (RetailCostDTO retailCostDTO : entry.getValue()) {
				if (Constants.CHAIN_LEVEL_TYPE_ID == retailCostDTO
						.getLevelTypeId()) {
					isChainLevelPresent = true;
					chainLevelData = retailCostDTO;
				} else if (Constants.ZONE_LEVEL_TYPE_ID == retailCostDTO
						.getLevelTypeId()) {
					zoneLevelData = retailCostDTO;
					// Unroll cost for zone level data
					if (!(null == zoneStoreMap.get(retailCostDTO.getLevelId()))) {
						for (String storeNo : zoneStoreMap.get(retailCostDTO
								.getLevelId())) {
							if (!unrolledStores.contains(storeNo)) {
								zoneLevelDTO = new RetailCostDTO();
								zoneLevelDTO.copy(zoneLevelData);
								zoneLevelDTO.setLevelId(storeNo);
								zoneLevelDTO
										.setLevelTypeId(Constants.STORE_LEVEL_TYPE_ID);
								/*String zoneKey = Constants.ZONE_LEVEL_TYPE_ID
										+ Constants.INDEX_DELIMITER
										+ zoneLevelData.getLevelId();*/
								StoreItemKey zoneKey = new StoreItemKey(Constants.ZONE_LEVEL_TYPE_ID, zoneLevelData.getLevelId());
								/*String storeKey = Constants.STORE_LEVEL_TYPE_ID
										+ Constants.INDEX_DELIMITER
										+ storeIdsMap.get(zoneLevelDTO
												.getLevelId());*/
								StoreItemKey storeKey = new StoreItemKey(Constants.STORE_LEVEL_TYPE_ID, storeIdsMap.get(zoneLevelDTO
												.getLevelId()).toString());
								
								// Check if this item exists for this store
								boolean isPopulated = false;
								if (storeItemMap.get(zoneKey) != null) {
									if (storeItemMap.get(zoneKey).contains(
											zoneLevelDTO.getItemcode())) {
										populateMap(unrolledCostMap,
												zoneLevelDTO);
										isPopulated = true;
									}
								}

								if (!isPopulated)
									if (storeItemMap.get(storeKey) != null) {
										if (storeItemMap.get(storeKey)
												.contains(
														zoneLevelDTO
																.getItemcode())) {
											populateMap(unrolledCostMap,
													zoneLevelDTO);
										}
									}

								unrolledStores.add(storeNo);
							}
						}
					} else {
						// To handle items priced at dept zone level
						List<String> stores = deptZoneMap.get(retailCostDTO
								.getLevelId());
						if (stores != null && stores.size() > 0) {
							for (String store : stores) {
								if (!unrolledStores.contains(store)) {
									zoneLevelDTO = new RetailCostDTO();
									zoneLevelDTO.copy(zoneLevelData);
									zoneLevelDTO.setLevelId(store);
									zoneLevelDTO
											.setLevelTypeId(Constants.STORE_LEVEL_TYPE_ID);
									/*String storeKey = Constants.STORE_LEVEL_TYPE_ID
											+ Constants.INDEX_DELIMITER
											+ storeIdsMap.get(zoneLevelDTO
													.getLevelId());*/
									StoreItemKey storeKey = new StoreItemKey(Constants.STORE_LEVEL_TYPE_ID, storeIdsMap.get(zoneLevelDTO
													.getLevelId()).toString());

									if (storeItemMap.get(storeKey) != null) {
										if (storeItemMap.get(storeKey)
												.contains(
														zoneLevelDTO
																.getItemcode())) {
											populateMap(unrolledCostMap,
													zoneLevelDTO);
										}
									}

									unrolledStores.add(store);
								}
							}
						}
					}
				} else {
					if (storeNumbers.contains(retailCostDTO.getLevelId())) {
						populateMap(unrolledCostMap, retailCostDTO);
						unrolledStores.add(retailCostDTO.getLevelId());
					}
				}
			}

			// Unroll cost for chain level data
			if (isChainLevelPresent)
				for (String storeNo : storeNumbers) {
					if (!unrolledStores.contains(storeNo)) {
						chainLevelDTO = new RetailCostDTO();
						chainLevelDTO.copy(chainLevelData);
						chainLevelDTO.setLevelId(storeNo);
						chainLevelDTO
								.setLevelTypeId(Constants.STORE_LEVEL_TYPE_ID);
						/*String storeKey = Constants.STORE_LEVEL_TYPE_ID
								+ Constants.INDEX_DELIMITER
								+ storeIdsMap.get(chainLevelDTO.getLevelId());*/
						StoreItemKey storeKey = new StoreItemKey(Constants.STORE_LEVEL_TYPE_ID, 
								storeIdsMap.get(chainLevelDTO.getLevelId()).toString());
						/*String zoneKey = Constants.ZONE_LEVEL_TYPE_ID
								+ Constants.INDEX_DELIMITER
								+ storeNumberMap
										.get(chainLevelDTO.getLevelId());*/
						
						StoreItemKey zoneKey = new StoreItemKey(Constants.ZONE_LEVEL_TYPE_ID, storeNumberMap
										.get(chainLevelDTO.getLevelId()));
						// Check if this item exists for this store
						boolean isPopulated = false;
						if (storeItemMap.get(storeKey) != null) {
							if (storeItemMap.get(storeKey).contains(
									chainLevelDTO.getItemcode())) {
								populateMap(unrolledCostMap, chainLevelDTO);
								isPopulated = true;
							}
						}

						if (!isPopulated)
							if (storeItemMap.get(zoneKey) !=  null) {
								if (storeItemMap.get(zoneKey).contains(
										chainLevelDTO.getItemcode())) {
									populateMap(unrolledCostMap, chainLevelDTO);
									isPopulated = true;
								}
							}

						// To handle items priced at dept zone level
						if (!isPopulated) {
							if (storeDeptZoneMap != null
									&& storeDeptZoneMap.size() > 0) {
								List<String> deptZones = storeDeptZoneMap
										.get(storeNo);
								if (deptZones != null && deptZones.size() > 0) {
									for (String deptZone : deptZones) {
										/*String deptZoneKey = Constants.ZONE_LEVEL_TYPE_ID
												+ Constants.INDEX_DELIMITER
												+ deptZone;*/
										StoreItemKey deptZoneKey = new StoreItemKey(Constants.ZONE_LEVEL_TYPE_ID, deptZone);
										if (storeItemMap
												.get(deptZoneKey) != null) {
											if (storeItemMap
													.get(deptZoneKey)
													.contains(
															chainLevelDTO
																	.getItemcode())) {
												populateMap(unrolledCostMap,
														chainLevelDTO);
											}
										}
									}
								}
							}
						}
					}
				}
		}

		return unrolledCostMap;
	}

	
	/**
	 * Performs cost roll up logic for Retail Cost Data
	 * 
	 * @return List Map Containing Item Code as key and list of RetailCostDTO as
	 *         value to be compared against existing database entries
	 */
	public HashMap<String, List<RetailCostDTO>> costRollUpV2(
			HashMap<ItemDetailKey, List<RetailCostDTO>> retailCostDataMap,
			HashMap<ItemDetailKey, String> itemCodeMap,
			HashMap<String, String> storeInfoMap, Set<String> noItemCodeSet,
			String chainId) {
		logger.debug("Inside costRollUp of RetailCostSetup");

		HashMap<String, List<RetailCostDTO>> costRolledUpMap = new HashMap<String, List<RetailCostDTO>>();
		List<RetailCostDTO> costRolledUpList;

		for (List<RetailCostDTO> retailCostDTOList : retailCostDataMap.values()) {

			HashMap<String, List<RetailCostDTO>> chainLevelMap = new HashMap<String, List<RetailCostDTO>>();
			HashMap<String, HashMap<String, List<RetailCostDTO>>> zoneLevelMap = new HashMap<String, HashMap<String, List<RetailCostDTO>>>();

			costRolledUpList = new ArrayList<RetailCostDTO>();
			String itemCode = null;
			String upc = null;
			boolean isAtStoreLevel = false;
			for (RetailCostDTO retailCostDTO : retailCostDTOList) {

				String costStr = Constants.EMPTY;

				retailCostDTO.setPromotionFlag("N");

				/*
				 * if(retailCostDTO.getListCost() >= 0){ costStr = costStr +
				 * retailCostDTO.getListCost(); }
				 */

				costStr = costStr + retailCostDTO.getListCost();

				if (retailCostDTO.getDealCost() > 0) {
					costStr = costStr + Constants.INDEX_DELIMITER
							+ retailCostDTO.getDealCost();
				}

				// Changes for TOPS Cost/Billback dataload
				if (retailCostDTO.getLevel2Cost() > 0) {
					costStr = costStr + Constants.INDEX_DELIMITER
							+ retailCostDTO.getLevel2Cost();
				}

				// Changes for Ahold VIP Cost
				if (retailCostDTO.getVipCost() > 0) {
					costStr = costStr + Constants.INDEX_DELIMITER
							+ retailCostDTO.getVipCost();
				}

				costStr = costStr + Constants.INDEX_DELIMITER
						+ retailCostDTO.getEffListCostDate();

				if (retailCostDTO.getDealCost() != 0
						&& retailCostDTO.getDealCost() < retailCostDTO
								.getListCost()) {
					retailCostDTO.setPromotionFlag("Y");
				}

				if (retailCostDTO.getLongTermFlag() != null) {
					costStr = costStr + Constants.INDEX_DELIMITER + retailCostDTO.getLongTermFlag();
				}
				
				if (retailCostDTO.getFinalListCost() > 0) {
					costStr = costStr + Constants.INDEX_DELIMITER + retailCostDTO.getFinalListCost();
				}
				
				// Set Item Code
				if (retailCostDTO.getItemcode() != null) {
					itemCode = retailCostDTO.getItemcode();
				}

				if (retailCostDTO.getUpc() != null) {
					if(itemCode == null){
						String inputUpc = 
								PrestoUtil.castUPC(
										retailCostDTO.getUpc(), false);
						String retailerItemCode = retailCostDTO.getRetailerItemCode();
						ItemDetailKey itemDetailKey = new ItemDetailKey(inputUpc, retailerItemCode);
						itemCode = itemCodeMap.get(itemDetailKey);
						retailCostDTO.setItemcode(itemCode);
					}
					upc = retailCostDTO.getUpc();
				}

				// Set Zone# and Store#
				if (retailCostDTO.getLevelTypeId() == Constants.STORE_LEVEL_TYPE_ID) {
					retailCostDTO.setStoreNbr(retailCostDTO.getLevelId());
					retailCostDTO.setZoneNbr(storeInfoMap.get(retailCostDTO
							.getStoreNbr()));
					// Change to handle stores with no zone
					if (storeInfoMap.get(retailCostDTO.getStoreNbr()) == null)
						storesWithNoZone.add(retailCostDTO.getStoreNbr());
					// Change to handle stores with no zone - Ends
				} else if (retailCostDTO.getLevelTypeId() == Constants.ZONE_LEVEL_TYPE_ID) {
					retailCostDTO.setZoneNbr(retailCostDTO.getLevelId());
				}

				// If item is priced at zone level populate a hashmap with cost
				// as key and list of corresponding retailCostDTO as its value
				if (Constants.ZONE_LEVEL_TYPE_ID == retailCostDTO
						.getLevelTypeId()) {
					List<RetailCostDTO> tempList = null;
					if (chainLevelMap.get(costStr) != null) {
						tempList = chainLevelMap.get(costStr);
						tempList.add(retailCostDTO);
					} else {
						tempList = new ArrayList<RetailCostDTO>();
						tempList.add(retailCostDTO);
					}
					chainLevelMap.put(costStr, tempList);
				} else {
					/*
					 * If item is priced at store level populate a hashmap with
					 * zone number as key and hashmap containing cost and its
					 * list of retail cost info as value
					 */
					isAtStoreLevel = true;
					HashMap<String, List<RetailCostDTO>> zoneCostMap = null;
					if (zoneLevelMap.get(retailCostDTO.getZoneNbr()) != null) {
						zoneCostMap = zoneLevelMap.get(retailCostDTO
								.getZoneNbr());
						List<RetailCostDTO> tempList = null;
						if (zoneCostMap.get(costStr) != null) {
							tempList = zoneCostMap.get(costStr);
							tempList.add(retailCostDTO);
						} else {
							tempList = new ArrayList<RetailCostDTO>();
							tempList.add(retailCostDTO);
						}
						zoneCostMap.put(costStr, tempList);
					} else {
						zoneCostMap = new HashMap<String, List<RetailCostDTO>>();
						List<RetailCostDTO> tempList = new ArrayList<RetailCostDTO>();
						tempList.add(retailCostDTO);
						zoneCostMap.put(costStr, tempList);
					}
					zoneLevelMap.put(retailCostDTO.getZoneNbr(), zoneCostMap);
				}
			}

			// If item is priced at store level perform an additional step of
			// rolling it up to zone level.
			if (isAtStoreLevel) {

				for (Map.Entry<String, HashMap<String, List<RetailCostDTO>>> entry : zoneLevelMap
						.entrySet()) {
					String mostPrevalentCostAtZone = null;
					int mostPrevalentCntAtZone = 0;
					if (entry.getKey() != null) {
						for (Map.Entry<String, List<RetailCostDTO>> inEntry : entry
								.getValue().entrySet()) {
							if (inEntry.getValue().size() > mostPrevalentCntAtZone) {
								mostPrevalentCntAtZone = inEntry.getValue()
										.size();
								mostPrevalentCostAtZone = inEntry.getKey();
							}
						}

						for (Map.Entry<String, List<RetailCostDTO>> inEntry : entry
								.getValue().entrySet()) {
							if (inEntry.getKey()
									.equals(mostPrevalentCostAtZone)) {
								RetailCostDTO retailCostDTO = inEntry
										.getValue().get(0);
								retailCostDTO.setLevelId(entry.getKey());
								retailCostDTO
										.setLevelTypeId(Constants.ZONE_LEVEL_TYPE_ID);

								List<RetailCostDTO> tempList = null;
								if (chainLevelMap.get(mostPrevalentCostAtZone) != null) {
									tempList = chainLevelMap
											.get(mostPrevalentCostAtZone);
									tempList.add(retailCostDTO);
								} else {
									tempList = new ArrayList<RetailCostDTO>();
									tempList.add(retailCostDTO);
								}
								chainLevelMap.put(mostPrevalentCostAtZone,
										tempList);
							} else {
								List<RetailCostDTO> tempList = null;
								if (chainLevelMap.get(inEntry.getKey()) != null) {
									tempList = chainLevelMap.get(inEntry
											.getKey());
									tempList.addAll(inEntry.getValue());
								} else {
									tempList = new ArrayList<RetailCostDTO>();
									tempList.addAll(inEntry.getValue());
								}
								chainLevelMap.put(inEntry.getKey(), tempList);
							}
						}
					} else {
						// Changes for TOPS Cost/Billback dataload - To handle
						// stores with null zone ids
						if (!ignoreStoresNotInDB
								.equalsIgnoreCase(Constants.IGN_STR_NOT_IN_DB_TRUE)) {
							for (Map.Entry<String, List<RetailCostDTO>> inEntry : entry
									.getValue().entrySet()) {
								for (RetailCostDTO retailCostDTO : inEntry
										.getValue()) {
									String costStr = Constants.EMPTY;

									retailCostDTO.setPromotionFlag("N");
									if (retailCostDTO.getListCost() >= 0) {
										costStr = costStr
												+ retailCostDTO.getListCost();
									}
									if (retailCostDTO.getDealCost() > 0) {
										costStr = costStr
												+ Constants.INDEX_DELIMITER
												+ retailCostDTO.getDealCost();
										retailCostDTO.setPromotionFlag("Y");
									}

									if (retailCostDTO.getLevel2Cost() > 0) {
										costStr = costStr
												+ Constants.INDEX_DELIMITER
												+ retailCostDTO.getLevel2Cost();
									}
									setupVipCost(retailCostDTO);
									List<RetailCostDTO> tempList = null;
									if (chainLevelMap.get(costStr) != null) {
										tempList = chainLevelMap.get(costStr);
										retailCostDTO
												.setLevelTypeId(Constants.STORE_LEVEL_TYPE_ID);
										retailCostDTO.setLevelId(retailCostDTO
												.getStoreNbr());
										tempList.add(retailCostDTO);
									} else {
										tempList = new ArrayList<RetailCostDTO>();
										retailCostDTO
												.setLevelTypeId(Constants.STORE_LEVEL_TYPE_ID);
										retailCostDTO.setLevelId(retailCostDTO
												.getStoreNbr());
										tempList.add(retailCostDTO);
									}
									chainLevelMap.put(inEntry.getKey(),
											tempList);
								}
							}
						}
					}
				}
			}

			// Determine most prevalent cost for this UPC
			String mostPrevalentCost = null;
			int mostPrevalentCnt = 0;
			int tempCnt = 0;
			for (Map.Entry<String, List<RetailCostDTO>> entry : chainLevelMap
					.entrySet()) {
				/*
				 * if(entry.getValue().size() > mostPrevalentCnt){
				 * mostPrevalentCnt = entry.getValue().size();
				 * mostPrevalentPrice = entry.getKey(); }
				 */
				List<RetailCostDTO> retailCostDTOLst = entry.getValue();
				tempCnt = 0;
				for (RetailCostDTO retailCostDTO : retailCostDTOLst) {
					if (retailCostDTO.getLevelTypeId() == Constants.ZONE_LEVEL_TYPE_ID) {
						// Check if there are two common prices
						// Confusion in picking chain level price
						if(chainLevelMap.size() == 2 && useZoneStoreMapping){
							if(zoneToStoreMapping.get(retailCostDTO.getLevelId()) != null){
								logger.debug("using zone store mapping...");
								List<String> stores = zoneToStoreMapping.get(retailCostDTO.getLevelId());
								tempCnt = tempCnt + stores.size();
							}else{
								tempCnt = tempCnt + 1;
							}
						}
						else{
							tempCnt = tempCnt + 1;
						}
					}
				}

				if (tempCnt > mostPrevalentCnt) {
					mostPrevalentCnt = tempCnt;
					mostPrevalentCost = entry.getKey();
				}
			}

			// Add Items to the final list to be compared against the database
			for (Map.Entry<String, List<RetailCostDTO>> entry : chainLevelMap
					.entrySet()) {
				if (entry.getKey().equals(mostPrevalentCost)) {
					RetailCostDTO retailCostDTO = entry.getValue().get(0);
					logger.debug("Long term flag: " + retailCostDTO.getLongTermFlag());
					logger.debug("Level ID: " + retailCostDTO.getLongTermFlag());
					RetailCostDTO chainLevelDTO = new RetailCostDTO();
					chainLevelDTO.copy(retailCostDTO);
					setupVipCost(chainLevelDTO);
					chainLevelDTO.setLevelId(chainId);
					chainLevelDTO.setLevelTypeId(Constants.CHAIN_LEVEL_TYPE_ID);
					costRolledUpList.add(chainLevelDTO);
					for (RetailCostDTO retailCost : entry.getValue()) {
						if (retailCost.getLevelTypeId() == Constants.STORE_LEVEL_TYPE_ID) {
							setupVipCost(retailCost);
							costRolledUpList.add(retailCost);
						}
					}
				} else {
					for (RetailCostDTO retailCostDTO : entry.getValue()) {
						setupVipCost(retailCostDTO);
						costRolledUpList.add(retailCostDTO);
					}
				}
			}

			if (itemCode != null)
				costRolledUpMap.put(itemCode, costRolledUpList);
			else {
				if (upc != null)
					noItemCodeSet.add(upc);
			}
		}
		return costRolledUpMap;
	}

	/**
	 * This method populates a HashMap with store number as key and a list of
	 * its corresponding retailCostDTO as its value
	 * 
	 * @param unrolledCostMap
	 *            Map that needs to be populated with with store number as key
	 *            and a list of its corresponding retailCostDTO as its value
	 * @param retailCostDTO
	 *            Retail Cost DTO that needs to be added to the Map
	 */
	public void populateMap(
			HashMap<UnrollingKey, List<RetailCostDTO>> unrolledCostMap,
			RetailCostDTO retailCostDTO) {
		UnrollingKey unrollingKey = new UnrollingKey(retailCostDTO.getItemcode());
		
		if (unrolledCostMap.get(unrollingKey) != null) {
			List<RetailCostDTO> tempList = unrolledCostMap.get(unrollingKey);
			tempList.add(retailCostDTO);
			unrolledCostMap.put(unrollingKey, tempList);
		} else {
			List<RetailCostDTO> tempList = new ArrayList<RetailCostDTO>();
			tempList.add(retailCostDTO);
			unrolledCostMap.put(unrollingKey, tempList);
		}
	}
	
	
	private ItemDetailKey getItemDetailKey(String itemCode){
	ItemDetailKey itemDetailKey = null;
		for(Map.Entry<ItemDetailKey, String> itemEntry: itemLookupMap.entrySet()){
			Object itemObject = itemEntry.getValue();
			if(itemObject.equals(itemCode)){
				itemDetailKey = itemEntry.getKey(); 
				return itemDetailKey;
			}
		}
		return itemDetailKey;
		
	}
	

	private void updateLocationId(List<RetailCostDTO> insertList){
		for(RetailCostDTO retailCostDTO: insertList){
			//Update chain id for zone level records
			if(retailCostDTO.getLevelTypeId() == Constants.CHAIN_LEVEL_TYPE_ID)
				retailCostDTO.setLocationId(Integer.parseInt(retailCostDTO.getLevelId()));
			//Update price zone id from the cache when there is a zone level record 
			else if(retailCostDTO.getLevelTypeId() == Constants.ZONE_LEVEL_TYPE_ID){
				//Set location id only if it is available else put it as null 
				if(retailPriceZone.get(retailCostDTO.getLevelId()) != null)
					retailCostDTO.setLocationId(retailPriceZone.get(retailCostDTO.getLevelId()));
			}
			//Update comp_str_id from the cache when there is a store level record
			else if(retailCostDTO.getLevelTypeId() == Constants.STORE_LEVEL_TYPE_ID){
				//Set location id only if it is available else put it as null
				if(storeIdsMap.get(retailCostDTO.getLevelId()) != null)
					retailCostDTO.setLocationId(storeIdsMap.get(retailCostDTO.getLevelId()));
			}
		}
	}

	/**
	 * Convert already added future cost as regular cost
	 * @param inputFileMap
	 * @param tempCostMap
	 */
	private void identifyAndConvertFutureAsRegular(
			HashMap<ItemDetailKey, List<RetailCostDTO>> inputFileMap, 
			HashMap<String, List<RetailCostDTO>> tempCostMap){
		for(Map.Entry<String, List<RetailCostDTO>> tempCostEntry: tempCostMap.entrySet()){
			ItemDetailKey itemDetailKey = getItemDetailKey(tempCostEntry.getKey());
			List<RetailCostDTO> inputList = inputFileMap.get(itemDetailKey);
			if(inputList != null){
				for(RetailCostDTO tempDTO: tempCostEntry.getValue()){
					boolean isLocationFound = false;
					for(RetailCostDTO inputDTO: inputList){
						if(inputDTO.getLevelId().equals(tempDTO.getLevelId())
								&& inputDTO.getLevelTypeId() == tempDTO.getLevelTypeId()){
							isLocationFound = true;
						}
					}
					if(!isLocationFound){
						inputList.add(tempDTO);
					}
				}
			}else{
				inputList = new ArrayList<>();
				inputList.addAll(tempCostEntry.getValue());
			}
			inputFileMap.put(itemDetailKey, inputList);
		}
	}
	
	/**
	 * Identfies cost changed items and fills records to be deleted list
	 * @param inputDTO
	 * @param retailCostDTO
	 * @param futureCostDeleteList
	 */
	private void identifyCostChange(RetailCostDTO inputDTO, RetailCostDTO retailCostDTO, 
			List<RetailCostDTO> futureCostDeleteList){
		String inputEffDateStr = inputDTO.getEffListCostDate();
		String dbEffDateStr = retailCostDTO.getEffListCostDate();
		SimpleDateFormat dateFormat = new SimpleDateFormat(Constants.APP_DATE_FORMAT);
		try{
			Date inputEffDate = dateFormat.parse(inputEffDateStr);	
			Date dbEffDate =  dateFormat.parse(dbEffDateStr);
			if(inputEffDate.after(dbEffDate) 
					&& inputDTO.getListCost() != retailCostDTO.getListCost()
					&& !inputDTO.isFutureAsRegular()){
				futureCostDeleteList.add(retailCostDTO);
			}
		}catch(Exception e){
			logger.error("Error while parsing effective date.", e);
		}
	}
	
	
	/**
	 * 
	 * @param costInputMap
	 * @throws GeneralException 
	 * @throws CloneNotSupportedException 
	 */
	public void rollupDSDZonesToWhseZone(HashMap<ItemDetailKey, List<RetailCostDTO>> costInputMap,
			HashMap<ItemDetailKey, String> itemLookupMap,
			HashMap<Integer, Integer> itemCodeCategoryMap,
			HashMap<Integer, HashMap<String, String>> dsdAndWhseZoneMap)
			throws GeneralException, CloneNotSupportedException {

		HashMap<ItemDetailKey, List<RetailCostDTO>> DSDCostMap = new HashMap<>();
		// Loop through the input map and identify all DSD zone prices
		for (Map.Entry<ItemDetailKey, List<RetailCostDTO>> costInputEntry : costInputMap.entrySet()) {
			for (RetailCostDTO retailCostDTO : costInputEntry.getValue()) {
				if (retailCostDTO.getPrcGrpCode().equals(Constants.GE_PRC_GRP_CD_DSD)) {
					List<RetailCostDTO> tempList = new ArrayList<>();			
					if (DSDCostMap.containsKey(costInputEntry.getKey())) {
						tempList = DSDCostMap.get(costInputEntry.getKey());
					}
					tempList.add(retailCostDTO);
					DSDCostMap.put(costInputEntry.getKey(), tempList);
				}
			}
		}

		// Loop all the DSD items and rollup to warehouse level
		for (Map.Entry<ItemDetailKey, List<RetailCostDTO>> DSDCostEntry : DSDCostMap.entrySet()) {
			List<RetailCostDTO> whseCostList = new ArrayList<>();
			// If item present in zone mapping cache
			if (itemLookupMap.containsKey(DSDCostEntry.getKey())) {
				String itemCode = itemLookupMap.get(DSDCostEntry.getKey());
				if (itemCodeCategoryMap.containsKey(Integer.parseInt(itemCode))) {
					int productId = itemCodeCategoryMap.get(Integer.parseInt(itemCode));
					if (dsdAndWhseZoneMap.containsKey(productId)) {
						HashMap<String, String> zoneMap = dsdAndWhseZoneMap.get(productId);
						for (RetailCostDTO retailCostDTO : DSDCostEntry.getValue()) {
							if (retailCostDTO.getLevelTypeId() == Constants.ZONE_LEVEL_TYPE_ID) {
								if (zoneMap.containsKey(retailCostDTO.getLevelId())) {
									// Get warehouse zones
									String whseZone = zoneMap.get(retailCostDTO.getLevelId());
									//Below logic is commented because this is handled in product location mapping itself
									//Commented on 03/14/2018 by Pradeep
									//As per suggestion by Anand
									/*if (zoneMap.containsKey(whseZone)) {
										whseZone = zoneMap.get(whseZone);
									}*/
									RetailCostDTO retailCostDTO2 = (RetailCostDTO) retailCostDTO.clone();
									retailCostDTO2.setLevelId(whseZone);
									retailCostDTO2.setLevelTypeId(Constants.ZONE_LEVEL_TYPE_ID);
									whseCostList.add(retailCostDTO2);
								}
							}
						}
						// Attach warehouse price entry for each item
						if (whseCostList.size() > 0) {
							HashMap<String, List<RetailCostDTO>> costGrp = new HashMap<>();
							for (RetailCostDTO retailCostDTO : whseCostList) {
								List<RetailCostDTO> tempList = new ArrayList<>();			
								if (DSDCostMap.containsKey(retailCostDTO.getLevelId())) {
									tempList = costGrp.get(retailCostDTO.getLevelId());
								}
								tempList.add(retailCostDTO);
								costGrp.put(retailCostDTO.getLevelId(), tempList);
							}

							for (Map.Entry<String, List<RetailCostDTO>> costEntry : costGrp.entrySet()) {
								RetailCostDTO maxCostDTO = null;
								for (RetailCostDTO retailCostDTO : costEntry.getValue()) {
									if (maxCostDTO == null) {
										maxCostDTO = retailCostDTO;
									} else if (maxCostDTO.getListCost() < retailCostDTO.getListCost()) {
										maxCostDTO = retailCostDTO;
									}
								}
								maxCostDTO.setWhseZoneRolledUpRecord(true);
								// Attach warehouse cost
								List<RetailCostDTO> availbleEntries = costInputMap.get(DSDCostEntry.getKey());
								availbleEntries.add(maxCostDTO);
								costInputMap.put(DSDCostEntry.getKey(), availbleEntries);
							}
						}
					}
				}
			}
		}
	}
	
	public class UnrollingKey{
		private String key;
		UnrollingKey(String keyValue){
			this.key = keyValue;
		}
		
		public String getKey() {
			return key;
		}
		public void setKey(String key) {
			this.key = key;
		}
		@Override
		public int hashCode() {
			final int prime = 31;
			int result = 1;
			result = prime * result + getOuterType().hashCode();
			result = prime * result + ((key == null) ? 0 : key.hashCode());
			return result;
		}
		@Override
		public boolean equals(Object obj) {
			if (this == obj)
				return true;
			if (obj == null)
				return false;
			if (getClass() != obj.getClass())
				return false;
			UnrollingKey other = (UnrollingKey) obj;
			if (!getOuterType().equals(other.getOuterType()))
				return false;
			if (key == null) {
				if (other.key != null)
					return false;
			} else if (!key.equals(other.key))
				return false;
			return true;
		}
		
		private RetailCostSetup getOuterType() {
			return RetailCostSetup.this;
		}
	}
}
