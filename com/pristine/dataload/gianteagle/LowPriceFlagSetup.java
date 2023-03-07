package com.pristine.dataload.gianteagle;

import java.sql.Connection;
import java.text.DateFormat;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Calendar;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.TreeMap;
import java.util.stream.Collectors;
import org.apache.log4j.Logger;
import org.apache.log4j.PropertyConfigurator;
import com.pristine.dao.DBManager;
import com.pristine.dao.ItemDAO;
import com.pristine.dao.LowPriceFlagSetupDAO;
import com.pristine.dao.RetailCalendarDAO;
import com.pristine.dao.RetailCostDAO;
import com.pristine.dao.RetailPriceZoneDAO;
import com.pristine.dto.ItemDetailKey;
import com.pristine.dto.RetailCalendarDTO;
import com.pristine.dto.fileformatter.gianteagle.GiantEaglePriceDTO;
import com.pristine.exception.GeneralException;
import com.pristine.parsinginterface.PristineFileParser;
import com.pristine.service.LowPriceFlagSetupService;
import com.pristine.util.Constants;
import com.pristine.util.DateUtil;
import com.pristine.util.PrestoUtil;
import com.pristine.util.PristineDBUtil;
import com.pristine.util.PropertyManager;

public class LowPriceFlagSetup<T> extends PristineFileParser<T>{

	private static Logger logger = Logger.getLogger("LowPriceFlagSetup");
	private Connection conn = null;
	private static String relativeInputPath;
	private final static String INPUT_FOLDER = "INPUT_FOLDER=";
	private static String dateStr = null;
	private List<GiantEaglePriceDTO> priceList = null;
	private HashMap<String, Integer> zoneIdMap = null;
	private HashMap<ItemDetailKey, String> itemCodeMap = null;
	private static TreeMap<Integer, String> allColumns = new TreeMap<Integer, String>();
	private Set<String> skippedRetailerItemcodes = null;
	private int calendarId;
	private HashMap<Integer, HashMap<String, String>> dsdAndWhseZoneMap = null;
 	private HashMap<Integer, Integer> itemCodeCategoryMap = null;
	
	public LowPriceFlagSetup() {
		
		PropertyManager.initialize("analysis.properties");

		try {
			
			conn = DBManager.getConnection();

		} catch (GeneralException exe) {
			logger.error("Error while connecting DB:" + exe);
			System.exit(1);
		}
	}
	
	
	@SuppressWarnings("rawtypes")
	public static void main(String[] args) {

		PropertyConfigurator.configure("log4j-low-price-flag-setup.properties");
		for (String arg : args) {
			if (arg.startsWith(INPUT_FOLDER)) {
				relativeInputPath = arg.substring(INPUT_FOLDER.length());
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
		if (Constants.CURRENT_WEEK.equalsIgnoreCase(args[1])) {
			dateStr = dateFormat.format(c.getTime());
		} else if (Constants.NEXT_WEEK.equalsIgnoreCase(args[1])) {
			c.add(Calendar.DATE, 7);
			dateStr = dateFormat.format(c.getTime());
		} else if (Constants.LAST_WEEK.equalsIgnoreCase(args[1])) {
			c.add(Calendar.DATE, -7);
			dateStr = dateFormat.format(c.getTime());
		} else if (Constants.SPECIFIC_WEEK.equalsIgnoreCase(args[1])) {
			try {
				dateStr = DateUtil.getWeekStartDate(DateUtil.toDate(args[2]), 0);
			} catch (GeneralException exception) {
				logger.error("Error when parsing date - " + exception.toString());
				System.exit(-1);
			}
		}
		
		logger.info("*************************************************************");
		LowPriceFlagSetup lowPriceFlagSetup = new LowPriceFlagSetup();
		lowPriceFlagSetup.processLowPriceFlag();
		logger.info("*************************************************************");
		
	}
	
	
	/**
	 * processes files and save it in DB
	 */
	private void processLowPriceFlag(){
		
		try{
			
			initialize();
			
			processFiles();
			
		}catch(GeneralException | Exception e){
			PristineDBUtil.rollbackTransaction(conn, "Failed setting up low price flags");
			logger.error("processLowPriceFlag() - Unable to process", e);
		}finally {
			PristineDBUtil.close(conn);
		}
	}

	/**
	 * 
	 * @throws GeneralException
	 */
	private void initialize() throws GeneralException{
		priceList = new ArrayList<>();
		skippedRetailerItemcodes = new HashSet<>();
		super.headerPresent = true;
		
		RetailPriceZoneDAO retailPriceZoneDAO = new RetailPriceZoneDAO();
		ItemDAO itemDAO = new ItemDAO();
		
		zoneIdMap = retailPriceZoneDAO.getZoneIdMap(conn);
		
		long startTime  = System.currentTimeMillis();
		itemCodeMap = itemDAO.getAllItemsFromItemLookup(conn);
		long endTime  = System.currentTimeMillis();
		logger.info("initialize() - Time taken to cache all items - " + ((endTime - startTime)/1000) + " seconds");
		
		
		RetailCalendarDAO retailCalendarDAO = new RetailCalendarDAO();
		RetailCalendarDTO calendarDTO = retailCalendarDAO.getCalendarId(conn,
				dateStr, Constants.CALENDAR_WEEK);
		logger.info("Calendar Id - " + calendarDTO.getCalendarId());
		logger.info("Week Start Date - " + calendarDTO.getStartDate());
		calendarId = calendarDTO.getCalendarId();
		
		logger.info("setupObjects() - ***ROLL_UP_DSD_TO_WARHOUSE_ZONE is enabled***");
		logger.info("setupObjects() - Getting DSD and Warehouse zone mapping...");
		dsdAndWhseZoneMap = new RetailCostDAO().getDSDAndWHSEZoneMap(conn, null);
		logger.info("setupObjects() - Getting DSD and Warehouse zone mapping is completed.");
		
		logger.info("setupObjects() - Getting Item and category mapping...");
		itemCodeCategoryMap = itemDAO.getCategoryAndItemCodeMap(conn, null);
		logger.info("setupObjects() - Getting Item and category mapping is completed.");
		
	}
	
	
	/**
	 * 
	 * @throws GeneralException
	 * @throws Exception
	 */
	private void processFiles() throws GeneralException, Exception {
		String zipFilePath = getRootPath() + "/" + relativeInputPath;
		String archivePath = getRootPath() + "/" + relativeInputPath + "/";
		LowPriceFlagSetupService lowPriceFlagSetupService = new LowPriceFlagSetupService();
		fillAllColumns();
		String fieldNames[] = new String[allColumns.size()];
		int k = 0;
		for (Map.Entry<Integer, String> columns : allColumns.entrySet()) {
			fieldNames[k] = columns.getValue();
			k++;
		}

		ArrayList<String> zipFileList = getZipFiles(relativeInputPath);

		int curZipFileCount = -1;
		boolean processZipFile = false;
		boolean commit = true;
		do {
			if (processZipFile)
				PrestoUtil.unzip(zipFileList.get(curZipFileCount), zipFilePath);

			ArrayList<String> fileList = getFiles(relativeInputPath);
			try {
				
				for (int i = 0; i < fileList.size(); i++) {

					String files = fileList.get(i);

					int stopCount = Integer.parseInt(PropertyManager.getProperty("DATALOAD.STOP_AFTER", "-1"));

					parseDelimitedFile(GiantEaglePriceDTO.class, files, '|', fieldNames, stopCount);

					//List<GiantEaglePriceDTO> filteredLPFlags = filterLowPriceFlags(priceList);
					
					HashMap<String, List<String>> upcMap = getUPCMapForRITEMNOS(priceList);
					
					List<GiantEaglePriceDTO> itemsWithLPFlags = lowPriceFlagSetupService.prepareItemsWithLowPriceFlags(
							priceList, itemCodeMap, zoneIdMap, skippedRetailerItemcodes, upcMap, calendarId,
							dsdAndWhseZoneMap, itemCodeCategoryMap, dateStr);

					updateToDatabase(itemsWithLPFlags);

					logNotProcessedItems();

				}

			} catch (GeneralException | Exception e) {
				logger.error("processFiles() - Error", e);
				commit = false;
			}

			if (processZipFile) {
				PrestoUtil.deleteFiles(fileList);
				fileList.clear();
				fileList.add(zipFileList.get(curZipFileCount));
			}

			if (commit) {
				PrestoUtil.moveFiles(fileList, archivePath + Constants.COMPLETED_FOLDER);
			} else {
				PrestoUtil.moveFiles(fileList, archivePath + Constants.BAD_FOLDER);
				throw new GeneralException("Error processing files");
			}
			curZipFileCount++;
			processZipFile = true;
		} while (curZipFileCount < zipFileList.size());
	}
	
	@SuppressWarnings({ "unchecked", "rawtypes" })
	@Override
	public void processRecords(List listobj) throws GeneralException {
		
		List<GiantEaglePriceDTO> priceCol = listobj;
		
		List<GiantEaglePriceDTO> filteredList = filterLowPriceFlags(priceCol);
		
		priceList.addAll(filteredList);
	}
	
	
	/**
	 * 
	 * @param priceList
	 * @return filtered LP price flags
	 */
	private List<GiantEaglePriceDTO> filterLowPriceFlags(List<GiantEaglePriceDTO> priceList){
		List<GiantEaglePriceDTO> filteredList = priceList.stream()
				.filter(giantEaglePriceDTO -> giantEaglePriceDTO.getLOW_PRC_FG().equals("Y")
						|| giantEaglePriceDTO.getNEW_LOW_PRC_FG().equals("Y"))
				.collect(Collectors.toList());
		
		return filteredList;
	}
	
	
	/**
	 * 
	 * @param filteredList
	 * @return upc map
	 * @throws GeneralException
	 */
	private HashMap<String, List<String>> getUPCMapForRITEMNOS(List<GiantEaglePriceDTO> filteredList) throws GeneralException{
		ItemDAO itemDAO = new ItemDAO();
		
		Set<String> retailerItemCodes = filteredList.stream().map(GiantEaglePriceDTO::getRITEM_NO)
				.collect(Collectors.toSet());

		logger.info("getUPCMapForRITEMNOS() - Distinct retailer item code size: " + retailerItemCodes.size());

		long startTimeUPCFetch = System.currentTimeMillis();

		HashMap<String, List<String>> upcMap = itemDAO.getUPCListForRetItemcodes(conn, retailerItemCodes);

		long endTimeUPCFetch = System.currentTimeMillis();

		logger.info("getUPCMapForRITEMNOS() - Time taken to fetch UPCs -> "
				+ (endTimeUPCFetch - startTimeUPCFetch) + " ms");
		
		return upcMap;
	}
	

	/**
	 * 
	 * @param lowPriceFlags
	 * @throws GeneralException
	 */
	private void updateToDatabase(List<GiantEaglePriceDTO> lowPriceFlags) throws GeneralException{
		LowPriceFlagSetupDAO lowPriceFlagSetupDAO = new LowPriceFlagSetupDAO();
		
		long startTime = System.currentTimeMillis();
		
		int rowsAffected = lowPriceFlagSetupDAO.deleteLowPriceFlagsArchive(conn);
		
		long endTime = System.currentTimeMillis();
		
		logger.info("updateToDatabase() - Rows deleted from archive: " + rowsAffected);
		
		logger.info("updateToDatabase() - Time taken to delete archive data: " + (endTime - startTime) + "ms.");
		
		startTime = System.currentTimeMillis();
		
		int rowsArchived = lowPriceFlagSetupDAO.archiveLowPriceFlags(conn);
		
		endTime = System.currentTimeMillis();
		
		logger.info("updateToDatabase() - Rows archived: " + rowsArchived);
		
		logger.info("updateToDatabase() - Time taken to archive existing data: " + (endTime - startTime) + "ms.");
		
		startTime = System.currentTimeMillis();
		
		int rowsCleanedUp = lowPriceFlagSetupDAO.deleteLowPriceFlags(conn);
		
		endTime = System.currentTimeMillis();
		
		logger.info("updateToDatabase() - Rows deleted from base table: " + rowsCleanedUp);
		
		logger.info("updateToDatabase() - Time taken to delete existing data: " + (endTime - startTime) + "ms.");
		
		startTime = System.currentTimeMillis();
		
		lowPriceFlagSetupDAO.insertLowPriceFlags(conn, lowPriceFlags);
		
		endTime = System.currentTimeMillis();
		
		logger.info("updateToDatabase() - Rows inserted: " + lowPriceFlags.size());
		
		logger.info("updateToDatabase() - Time taken to insert low price flags: " + (endTime - startTime) + "ms.");
		
	}
	
	
	/**
	 * logs not processed records
	 */
	private void logNotProcessedItems(){
		
		logger.warn("Items/Locations not found for items: "
				+ skippedRetailerItemcodes.stream().collect(Collectors.joining(",")));
		
	}
	
	/***
	 * Fill all possible columns of the csv file with key
	 */
	private void fillAllColumns() {
		int i = 0;
		allColumns.put(++i, "RITEM_NO");
		allColumns.put(++i, "PRC_STRT_DTE");
		allColumns.put(++i, "PRC_END_DTE");		
		allColumns.put(++i, "CURR_PRC");
		allColumns.put(++i, "MUNIT_CNT");
		allColumns.put(++i, "PRC_STAT_CD");
		allColumns.put(++i, "PROM_CD");
		allColumns.put(++i, "PROM_PCT");
		allColumns.put(++i, "PROM_AMT_OFF");
		allColumns.put(++i, "ZN_NO");
		allColumns.put(++i, "PRC_GRP_CD");
		allColumns.put(++i, "SPLR_NO");
		allColumns.put(++i, "PRC_TYP_IND");
		allColumns.put(++i, "DEAL_ID");
		allColumns.put(++i, "OFFER_ID");
		allColumns.put(++i, "OFFER_DSCR");
		allColumns.put(++i, "AD_TYP_DSCR");
		allColumns.put(++i, "AD_LOCN_DSCR");
		allColumns.put(++i, "PCT_OF_PGE");
		allColumns.put(++i, "BNR_CD");
		allColumns.put(++i, "NEW_LOW_PRC_FG");
		allColumns.put(++i, "NEW_LOW_PRC_END_DTE");
		allColumns.put(++i, "LOW_PRC_FG");
		allColumns.put(++i, "LOW_PRC_END_DTE");
	}

}
