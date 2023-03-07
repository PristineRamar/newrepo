package com.pristine.dataload.searchanalysis;

import java.sql.Connection;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Date;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

import org.apache.log4j.Logger;
import org.apache.log4j.PropertyConfigurator;

import com.pristine.dao.DBManager;
import com.pristine.dao.RetailCalendarDAO;
import com.pristine.dao.StoreDAO;
import com.pristine.dao.searchanalysis.ItemMetricsSummaryDao;
import com.pristine.dataload.MovementDataLoad;
import com.pristine.dto.ProductMetricsDataDTO;
import com.pristine.dto.RetailCalendarDTO;
import com.pristine.dto.fileformatter.AholdMovementFile;
import com.pristine.exception.GeneralException;
import com.pristine.parsinginterface.PristineFileParser;
import com.pristine.util.Constants;
import com.pristine.util.DateUtil;
import com.pristine.util.PrestoUtil;
import com.pristine.util.PristineDBUtil;
import com.pristine.util.PropertyManager;

public class LoadItemMetricSummaryWeekly extends PristineFileParser{
	
	private static Logger logger = Logger.getLogger("SearchItemWeekly");
	
	static HashMap<Integer, String> allColumns = new HashMap<Integer, String>();
	private HashMap<String, Integer> calendarIdMap = new HashMap<String, Integer>();
	private HashMap<String, Integer> storeIdMap = new HashMap<String, Integer>();
	
	private List<AholdMovementFile> allRecords = new ArrayList<AholdMovementFile>();
	
	private ItemMetricsSummaryDao itemMetricsDao = null;
	private MovementDataLoad movementLoad = null;
	
	private int stopCount = -1;
	private HashMap<String, Integer> recordCount = new HashMap<String, Integer>();
	private HashMap<String, Integer> recordCountInFile = new HashMap<String, Integer>();
	private HashMap<String, Integer> ignoredCount = new HashMap<String, Integer>();
	private HashMap<String, Integer> storesNotPresent = new HashMap<String, Integer>();
	private HashMap<String, Integer> upcNotPresent = new HashMap<String, Integer>();
	private Set<String> upcNotFoundSet = new HashSet<>();
	private int processRecordCount = 0;
	private int batchUpdateCount = 0;
	private int recCnt = 0;
	
	private String rootPath;
	private String startDateStr;
	private String endDateStr;
	private Date startDate;
	private Date endDate;
	private String storeNo;
	
	private boolean isStorePresent = false;
	private boolean isDatePresent = false;
	private boolean checkRetailerItemCode = false; // Changes to retrieve item_code using upc and retailer_item_code
	
	Connection conn = null;
	
	/**
	 * Initializes database connection and store list
	 */
	public LoadItemMetricSummaryWeekly(String startDate, String endDate, String storeNo)
	{
		PropertyManager.initialize("analysis.properties");
		try {	
			//Create DB connection
			conn = DBManager.getConnection();
			stopCount = Integer.parseInt(PropertyManager.getProperty("DATALOAD.STOP_AFTER", "-1"));
			processRecordCount = Integer.parseInt(PropertyManager.getProperty("AHOLDHISTORYLOAD.PROCESS_RECORD_COUNT", "1000000"));
			batchUpdateCount = Integer.parseInt(PropertyManager.getProperty("AHOLDHISTORYLOAD.BATCH_UPDATE_COUNT", "5000"));
			
			this.startDateStr = startDate;
			this.endDateStr = endDate;
			this.storeNo = storeNo;
			
			if(this.storeNo != null)
				isStorePresent = true;
			
			if(startDate != null && endDate != null)
				isDatePresent = true;
			
			itemMetricsDao =  new ItemMetricsSummaryDao();
			movementLoad = new com.pristine.dataload.MovementDataLoad("N", true);
			
			SimpleDateFormat sdf = new SimpleDateFormat("MM/dd/yyyy");  
			try{
				if(startDateStr != null)
					this.startDate = sdf.parse(startDateStr);
				if(endDateStr != null)
					this.endDate = sdf.parse(endDateStr);
			}catch(ParseException exception){
				logger.error("Unable to parse input dates");
				System.exit(-1);
			}
			
			// Changes to retrieve item_code using upc and retailer_item_code
			checkRetailerItemCode = Boolean.parseBoolean(PropertyManager.getProperty("ITEM_LOAD.CHECK_RETAILER_ITEM_CODE", "FALSE")); //Changes to consider retailer item code along with upc when ret
		}
		catch (GeneralException ex) {
			logger.error("Error when retrieving Connection");
			stopCount = -1;
		}
	}
	
	/**
	 * Main method to process movement data
	 * @param args[0]	Relative path of movement files
	 */
	public static void main(String[] args) throws GeneralException{
		PropertyConfigurator.configure("log4j-item-metric-weekly.properties");
		logCommand(args);
		
		if (args.length < 1) {
			logger.info("Invalid Arguments,  args[0] - Movement File Input Relative Path");
			System.exit(-1);
		}
		
		String startDate = null;
		String endDate = null;
		String storeNo = null;
		Boolean isCompDataLoad = false; // Changes for Competitor Data Load
		
		for (int ii = 0; ii < args.length; ii++) {
			String arg = args[ii];

			if (arg.startsWith("STARTDATE")) {
				startDate = arg.substring("STARTDATE=".length());
			}

			if (arg.startsWith("ENDDATE")) {
				endDate = arg.substring("ENDDATE=".length());
			}

			if (arg.startsWith("STORE")) {
				storeNo = arg.substring("STORE=".length());
			}
			
			// Changes for Competitor Data Load
			if(arg.startsWith("COMPETITOR")) {
				isCompDataLoad = true;
			}
			// Changes for Competitor Data Load - Ends
		}
		
		LoadItemMetricSummaryWeekly searchItemWeekly = new LoadItemMetricSummaryWeekly(startDate, endDate, storeNo);
		try{
			if(!isCompDataLoad){
				searchItemWeekly.setStoreIdMap();
				searchItemWeekly.parseFile(args[0]);
			}
			// Changes for Competitor Data Load
			else{
				searchItemWeekly.loadCompetitiveData();
			}
			// Changes for Competitor Data Load - Ends
		}catch(GeneralException ex){
			logger.error("Error in setting up item metrics weekly - " + ex.getMessage());
		}
	}

	private void setStoreIdMap() throws GeneralException{
		this.storeIdMap = new StoreDAO().getStoreIdMap(conn);
	}

	private void parseFile(String relativePath) throws GeneralException{
		super.headerPresent = true;
		rootPath = PropertyManager.getProperty("DATALOAD.ROOTPATH", "");
		fillAllColumns();
		String fieldNames[] = new String[allColumns.size()];
		int i = 0;
		for (Map.Entry<Integer, String> columns : allColumns.entrySet()) {
			fieldNames[i] = columns.getValue();
			i++;
		}

		//Clear hashmap as it is not required from here
		allColumns = null;
		
		//getzip files
		ArrayList<String> zipFileList = getZipFiles(relativePath);
		
		//Start with -1 so that if any regular files are present, they are processed first
		int curZipFileCount = -1;
		boolean processZipFile = false;
		
		String zipFilePath = getRootPath() + "/" + relativePath;
		do {
			ArrayList<String> fileList = null;
			boolean commit = true;
			
			if( processZipFile)
				PrestoUtil.unzip(zipFileList.get(curZipFileCount), zipFilePath) ;
			
			fileList = getFiles(relativePath);
			
			for (int j = 0; j < fileList.size(); j++) {
				logger.info("Reading of file " + fileList.get(j)
						+ " is Started... ");
				try {
					recordCount = new HashMap<String, Integer>();
					recordCountInFile = new HashMap<String, Integer>();
					storesNotPresent = new HashMap<String, Integer>();
					upcNotPresent = new HashMap<String, Integer>();
					ignoredCount = new HashMap<String, Integer>();
					recCnt = 0;
					long startTime = System.currentTimeMillis();
					parseDelimitedFile(AholdMovementFile.class, fileList.get(j), '|', fieldNames, stopCount);
					logger.info("Reading of file " + fileList.get(j) + " is Complete... ");
					if(allRecords.size() > 0){
						logger.info("Processing last batch " + allRecords.size());
						processInputData(allRecords);
						allRecords.clear();
					}
					PristineDBUtil.commitTransaction(conn, "History Load");
					
					logger.info("No of records in file");
					for(Map.Entry<String, Integer> entry : recordCountInFile.entrySet()){
						logger.info("Week : " + entry.getKey() + "\t" + "Value : " + entry.getValue());
					}
					logger.info("****************************************");
					logger.info("No of records inserted");
					for(Map.Entry<String, Integer> entry : recordCount.entrySet()){
						logger.info("Week : " + entry.getKey() + "\t" + "Value : " + entry.getValue());
					}
					logger.info("****************************************");
					logger.info("No of records with stores not in database");
					for(Map.Entry<String, Integer> entry : storesNotPresent.entrySet()){
						logger.info("Week : " + entry.getKey() + "\t" + "Value : " + entry.getValue());
					}
					logger.info("****************************************");
					logger.info("No of records with UPCs not in database");
					for(Map.Entry<String, Integer> entry : upcNotPresent.entrySet()){
						logger.info("Week : " + entry.getKey() + "\t" + "Value : " + entry.getValue());
					}
					logger.info("****************************************");
					logger.info("****************************************");
					logger.info("No of distinct UPCs not in database: " + upcNotFoundSet.size());
					logger.info("****************************************");
					
					logger.info("No of records Ignored");
					for(Map.Entry<String, Integer> entry : ignoredCount.entrySet()){
						logger.info("Week : " + entry.getKey() + "\t" + "Value : " + entry.getValue());
					}
					logger.info("****************************************");
					long endTime = System.currentTimeMillis();
					logger.info("Time taken to process the file - " + (endTime - startTime));
				} catch (GeneralException ge) {
					logger.error("File Skipped. Exception while reading of file "
							+ fileList.get(j));
					commit = false;
				}
				
			}
			
			if( processZipFile){
		    	PrestoUtil.deleteFiles(fileList);
		    	fileList.clear();
		    	fileList.add(zipFileList.get(curZipFileCount));
		    }
		    String archivePath = getRootPath() + "/" + relativePath + "/";
		    
		    if( commit ){
				PrestoUtil.moveFiles(fileList, archivePath + Constants.COMPLETED_FOLDER);
			}
			else{
				PrestoUtil.moveFiles(fileList, archivePath + Constants.BAD_FOLDER);
			}
			curZipFileCount++;
			processZipFile = true;
		}while (curZipFileCount < zipFileList.size());
		
		super.performCloseOperation(conn, true);
	}

	public void processRecords(List listObject) throws GeneralException {			
		List<AholdMovementFile> aholdMovFile = (List<AholdMovementFile>) listObject;
		allRecords.addAll(aholdMovFile);
		//Batch process. Keeping all data in list may lead to out-of-memory exception
		if(allRecords.size()>= processRecordCount )
		{				
			processInputData(allRecords);
			allRecords.clear();				
		}				
	}
	
	/**
	 * This method reads and loads item summary weekly from the input files
	 * under specified path.
	 * @param relativePath	Relative Path from which files needs to be read
	 */
	private void processInputData(List<AholdMovementFile> allRecords) {
		try{	
			// Populate Movement List
			ArrayList<ProductMetricsDataDTO> metricsDtoList = new ArrayList<ProductMetricsDataDTO>();
			for(AholdMovementFile aholdMovementData : allRecords){
				if(recordCountInFile.get(aholdMovementData.getWK_DESC()) != null){
					int count = recordCountInFile.get(aholdMovementData.getWK_DESC()) + 1;
					recordCountInFile.put(aholdMovementData.getWK_DESC(), count);
				}else{
					recordCountInFile.put(aholdMovementData.getWK_DESC(), 1);
				}
				
				aholdMovementData.initializeVariables();
				if(aholdMovementData.isIgnoreItemForHistoryLoad()){
					logger.debug("UPC :" + aholdMovementData.getUPC_CD() + " Comp Str No :" + aholdMovementData.getOPCO_LOC_CD() + "Week :" + aholdMovementData.getWK_DESC());
					if(ignoredCount.get(aholdMovementData.getWK_DESC()) != null){
						int count = ignoredCount.get(aholdMovementData.getWK_DESC()) + 1;
						ignoredCount.put(aholdMovementData.getWK_DESC(), count);
					}else{
						ignoredCount.put(aholdMovementData.getWK_DESC(), 1);
					}
					continue;
				}
				
				if(isDatePresent){
					SimpleDateFormat format = new SimpleDateFormat("MM/dd/yyyy");
					Date recordDate = format.parse(aholdMovementData.getWeekEndDate());
					if(!(PrestoUtil.compareTo(recordDate, startDate) >= 0 && PrestoUtil.compareTo(recordDate, endDate) <= 0)){
						continue;
					}
				}
				
				if(isStorePresent){
					String storeNo = aholdMovementData.getUPC_CD();
					if(!this.storeNo.equalsIgnoreCase(storeNo)){
						continue;
					}
				}

				ProductMetricsDataDTO metricsDto = new ProductMetricsDataDTO();
				if (calendarIdMap.get(aholdMovementData.getWeekEndDate()) != null) {
					metricsDto.setCalendarId(calendarIdMap.get(aholdMovementData.getWeekEndDate()));
				}
				else{
					RetailCalendarDAO calDAO = new RetailCalendarDAO();
					RetailCalendarDTO calDto = calDAO.getCalendarId(conn, aholdMovementData.getWeekEndDate(), Constants.CALENDAR_WEEK);
					metricsDto.setCalendarId(calDto.getCalendarId());
					calendarIdMap.put(aholdMovementData.getWeekEndDate(), calDto.getCalendarId());
				}

				metricsDto.setLocationLevelId(Constants.STORE_LEVEL_ID);

				if (storeIdMap.get(aholdMovementData.getOPCO_LOC_CD()) != null)
					metricsDto.setLocationId(storeIdMap.get(aholdMovementData.getOPCO_LOC_CD()));
				else {
					if (storesNotPresent.get(aholdMovementData.getWK_DESC()) != null) {
						int count = storesNotPresent.get(aholdMovementData.getWK_DESC()) + 1;
						storesNotPresent.put(aholdMovementData.getWK_DESC(), count);
					} else {
						storesNotPresent.put(aholdMovementData.getWK_DESC(), 1);
					}
					continue;
				}

				metricsDto.setProductLevelId(Constants.ITEMLEVELID);

				// Changes to retrieve item_code using upc and retailer_item_code
				int itemCode = -1;
				if (!checkRetailerItemCode)
					itemCode = movementLoad.getItemCode(aholdMovementData.getUPC_CD(), 0);
				else
					itemCode = movementLoad.getItemCode(aholdMovementData.getUPC_CD(), 0, metricsDto.getLocationId());
				// Changes to retrieve item_code using upc and retailer_item_code - Ends 

				if (itemCode > 0)
					metricsDto.setProductId(itemCode);
				else {
					if (upcNotPresent.get(aholdMovementData.getWK_DESC()) != null) {
						int count = upcNotPresent.get(aholdMovementData.getWK_DESC()) + 1;
						upcNotPresent.put(aholdMovementData.getWK_DESC(), count);
					} else {
						upcNotPresent.put(aholdMovementData.getWK_DESC(), 1);
					}
					upcNotFoundSet.add(aholdMovementData.getUPC_CD());
					logger.debug("UPC not found: " + aholdMovementData.getUPC_CD());
					continue;
				}

				if (aholdMovementData.isItemOnSale())
					metricsDto.setSaleFlag("Y");
				else
					metricsDto.setSaleFlag("N");

				metricsDto.setRegularPrice(Double.parseDouble(aholdMovementData.regularRetail()));
				metricsDto.setRegularQuantity(Double.parseDouble(aholdMovementData.regRetailQty()));
				if (aholdMovementData.saleRetail() != null && !"".equalsIgnoreCase(aholdMovementData.saleRetail()))
					metricsDto.setSalePrice(Double.parseDouble(aholdMovementData.saleRetail()));
				else
					metricsDto.setSalePrice(0);
				if(aholdMovementData.saleRetailQty() != null && !"".equalsIgnoreCase(aholdMovementData.saleRetailQty()))
					metricsDto.setSaleQuantity(Double.parseDouble(aholdMovementData.saleRetailQty()));
				else
					metricsDto.setSaleQuantity(0);
				if (metricsDto.getSalePrice() > 0)
					metricsDto.setFinalPrice(metricsDto.getSalePrice());
				else
					metricsDto.setFinalPrice(metricsDto.getRegularPrice());

				metricsDto.setListPrice(Double.parseDouble(aholdMovementData.listCost()));
				if (metricsDto.getListPrice() < 0)
					metricsDto.setListPrice(0);

				if (aholdMovementData.dealCost() != null && !"".equalsIgnoreCase(aholdMovementData.dealCost()))
					metricsDto.setDealPrice(Double.parseDouble(aholdMovementData.dealCost()));
				else
					metricsDto.setDealPrice(0);
				if (metricsDto.getDealPrice() < 0)
					metricsDto.setDealPrice(0);

				if (metricsDto.getDealPrice() > 0)
					metricsDto.setFinalCost(metricsDto.getDealPrice());
				else
					metricsDto.setFinalCost(metricsDto.getListPrice());

				double netRevenue = Double.parseDouble(aholdMovementData.getNETDOLLARS());
				if (aholdMovementData.isItemOnSale()) {
					metricsDto.setSaleRevenue(netRevenue);
					metricsDto.setSaleMovement(aholdMovementData.getUnitCount());
					metricsDto.setRegularRevenue(0);
					metricsDto.setRegularMovement(0);
				} else {
					metricsDto.setRegularRevenue(netRevenue);
					metricsDto.setRegularMovement(aholdMovementData.getUnitCount());
					metricsDto.setSaleRevenue(0);
					metricsDto.setSaleMovement(0);
				}

				metricsDto.setTotalMovement(metricsDto.getRegularMovement() + metricsDto.getSaleMovement());
				metricsDto.setTotalRevenue(metricsDto.getRegularRevenue() + metricsDto.getSaleRevenue());

				if (metricsDto.getTotalRevenue() > 0 && metricsDto.getFinalCost() > 0) {
					double netMargin = metricsDto.getTotalRevenue() - (metricsDto.getFinalCost() * metricsDto.getTotalMovement());
					double netMarginPct = (netMargin * 100) / metricsDto.getTotalRevenue();
					if (netMargin > 99999.99)
						netMargin = 99999.99;
					else if (netMargin < -99999.99)
						netMargin = -99999.99;
					if (netMarginPct > 999.99)
						netMarginPct = 999.99;
					else if (netMarginPct < -999.99)
						netMarginPct = -999.99;
					metricsDto.setNetMargin(netMargin);
					metricsDto.setNetMarginPct(netMarginPct);
				}

				if (metricsDto.getRegularPrice() > 0 && metricsDto.getListPrice() > 0 && metricsDto.getTotalMovement()>0) {
					double regularMargin = (metricsDto.getRegularPrice() - metricsDto.getListPrice()) * metricsDto.getTotalMovement();
					double regularMarginPct = (regularMargin * 100) / (metricsDto.getRegularPrice() * metricsDto.getTotalMovement());
					if (regularMargin > 99999.99)
						regularMargin = 99999.99;
					else if (regularMargin < -99999.99)
						regularMargin = -99999.99;
					if (regularMarginPct > 999.99)
						regularMarginPct = 999.99;
					else if (regularMarginPct < -999.99)
						regularMarginPct = -999.99;
					metricsDto.setRegularMargin(regularMargin);
					metricsDto.setRegularMarginPct(regularMarginPct);
				}
				// To avoid loading items whose movement is 0. By Dinesh(06/15/2017)
				if (metricsDto.getTotalMovement() > 0) {
					metricsDtoList.add(metricsDto);

					recCnt++;

					if (recordCount.get(aholdMovementData.getWK_DESC()) != null) {
						int count = recordCount.get(aholdMovementData.getWK_DESC()) + 1;
						recordCount.put(aholdMovementData.getWK_DESC(), count);
					} else {
						recordCount.put(aholdMovementData.getWK_DESC(), 1);
					}

					if (recCnt % 100000 == 0) {
						logger.info("Processed " + recCnt + " records");
						// break;
					}
				}

			}
			logger.info("Processed " + recCnt + " records");
			long delStartTime = System.currentTimeMillis();
			itemMetricsDao.deleteItemMetricsSummaryWeeklyByItem(conn, metricsDtoList, batchUpdateCount);
			long delEndTime = System.currentTimeMillis();
			logger.info("Time taken to delete records : " + (delEndTime - delStartTime) + " ms");
			
			long startTime = System.currentTimeMillis();
			itemMetricsDao.insertItemMetricsSummaryWeekly(conn, metricsDtoList, batchUpdateCount);
			long endTime = System.currentTimeMillis();
			logger.info("Time taken to insert records : " + (endTime - startTime) + " ms");
			
			PristineDBUtil.commitTransaction(conn, "History Load");
		}catch (GeneralException ex) {
            logger.error("Error " + ex);
            ex.printStackTrace();
        }catch (Exception ex) {
            logger.error("Error " + ex);
            ex.printStackTrace();
        }
		
	}
	
	/***
	 * Fill all possible columns of the csv file with key
	 */
	private void fillAllColumns() {
		allColumns.put(1, "CUSTOM_ATTR2_DESC");
		allColumns.put(2, "UPC_CD");
		allColumns.put(3, "LIST_MEMBER");
		allColumns.put(4, "OPCO_LOC_CD");
		allColumns.put(5, "WK_KEY");
		allColumns.put(6, "WK_DESC");
		allColumns.put(7, "SCANNEDUNITS");
		allColumns.put(8, "GROSSDOLLARS");
		allColumns.put(9, "NETDOLLARS");
		allColumns.put(10, "RETAILWEIGHT");
		allColumns.put(11, "POUNDORUNITS");
		allColumns.put(12, "EXTLEVEL3ELEMENTCOST");
		allColumns.put(13, "EXTLEVEL4ELEMENTCOST");
	}
	/***
	 * Fill all possible columns of the csv file with key
	 */
	/*private void fillAllColumns() {
		allColumns.put(1, "CUSTOM_ATTR2_DESC");
		allColumns.put(2, "UPC_CD");
		allColumns.put(3, "OPCO_LOC_CD");
		allColumns.put(4, "WK_KEY");
		allColumns.put(5, "WK_DESC");
		allColumns.put(6, "SCANNEDUNITS");
		allColumns.put(7, "GROSSDOLLARS");
		allColumns.put(8, "NETDOLLARS");
		allColumns.put(9, "RETAILWEIGHT");
		allColumns.put(10, "POUNDORUNITS");
		allColumns.put(11, "EXTLEVEL3ELEMENTCOST");
		allColumns.put(12, "EXTLEVEL4ELEMENTCOST");
	}*/
	
	/**
	 ***************************************************************** 
	 * Static method to log the command line arguments
	 ***************************************************************** 
	 */

	private static void logCommand(String[] args) {
		StringBuffer sb = new StringBuffer("Command: SearchItemWeekly ");
		for (int ii = 0; ii < args.length; ii++) {
			if (ii > 0)
				sb.append(' ');
			sb.append(args[ii]);
		}

		logger.info(sb.toString());
	}
	
	/**
	 * This method loads competitor data information in item_metric_summary_wk_comp
	 */
	private void loadCompetitiveData() throws GeneralException{
		List<RetailCalendarDTO> calDTOList = new ArrayList<RetailCalendarDTO>();
		RetailCalendarDAO retailCalendarDAO = new RetailCalendarDAO();
		if(isDatePresent){
			calDTOList = retailCalendarDAO.dayCalendarList(conn, startDate, endDate, Constants.CALENDAR_WEEK);
		}else{
			String lastWkStartDate = DateUtil.getWeekStartDate(1);
			RetailCalendarDTO calDTO = retailCalendarDAO.getCalendarId(conn, lastWkStartDate, Constants.CALENDAR_WEEK);
			calDTOList.add(calDTO);
		}
		
		if(calDTOList == null || calDTOList.size() == 0){
			logger.info("No calendar ids to process");
		}
		
		// Load competitor data for each calendar Id
		for(RetailCalendarDTO calDTO : calDTOList){
			logger.info("Process begins at Calendar level. Calendar Id:" + calDTO.getCalendarId());
			List<Integer> scheduleList = itemMetricsDao.getSchedulesForCalendar(conn, calDTO.getCalendarId());
			
			if(scheduleList == null || scheduleList.size() == 0){
				logger.info("No schedules in calendar id: " + calDTO.getCalendarId());
			}
			
			// Load competitive data for each schedule in the week
			for(Integer schId : scheduleList){
				int count = itemMetricsDao.insertItemMetricsSummaryWeekly(conn, schId);
				logger.info("No of records inserted for schedule " + schId + " - " + count);
			}
			logger.info("Process end at Calendar level. Calendar Id:" + calDTO.getCalendarId());
		}
	}
}
