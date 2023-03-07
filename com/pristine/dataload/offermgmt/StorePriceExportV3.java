package com.pristine.dataload.offermgmt;

import java.io.File;
import java.io.FileWriter;
import java.io.IOException;
import java.io.PrintWriter;
import java.sql.Connection;
import java.sql.SQLException;
import java.text.DateFormat;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.time.LocalDate;
import java.time.LocalDateTime;
import java.time.format.DateTimeFormatter;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Date;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.stream.Collectors;

import org.apache.log4j.Logger;
import org.apache.log4j.PropertyConfigurator;

import com.pristine.dao.ChainDAO;
import com.pristine.dao.DBManager;
import com.pristine.dao.ItemLoaderDAO;
import com.pristine.dao.RetailCalendarDAO;
import com.pristine.dao.RetailPriceDAO;
import com.pristine.dao.RetailPriceZoneDAO;
import com.pristine.dao.offermgmt.ItemDAO;
import com.pristine.dao.offermgmt.PriceExportDAOV2;
import com.pristine.dataload.tops.PriceDataLoad;
import com.pristine.dto.RetailCalendarDTO;
import com.pristine.dto.RetailPriceDTO;
import com.pristine.dto.StoreDTO;
import com.pristine.dto.fileformatter.gianteagle.ZoneDTO;
import com.pristine.dto.offermgmt.MultiplePrice;
import com.pristine.dto.offermgmt.NotificationDetailInputDTO;
import com.pristine.dto.offermgmt.PRExportRunHeader;
import com.pristine.dto.offermgmt.PRItemDTO;
import com.pristine.dto.offermgmt.PriceExportDTO;
import com.pristine.exception.GeneralException;
import com.pristine.lookup.AuditTrailStatusLookup;
import com.pristine.lookup.AuditTrailTypeLookup;
import com.pristine.service.offermgmt.AuditTrailService;
import com.pristine.service.offermgmt.NotificationService;
import com.pristine.service.offermgmt.StorePriceExportAZServiceV3;
import com.pristine.util.Constants;
import com.pristine.util.DateUtil;
import com.pristine.util.PrestoUtil;
import com.pristine.util.PristineDBUtil;
import com.pristine.util.PropertyManager;
import com.pristine.util.offermgmt.PRCommonUtil;
import com.pristine.util.offermgmt.PRConstants;

public class StorePriceExportV3 {

	private static Logger logger = Logger.getLogger("StorePriceExportV3");

	public Connection conn = null;
	
	private String priceExportType;
	private String priceExportDate;
	private String priceExportTime;
	
	//Properties to run the porgram starts
	private String saleFloorLimitStr;
	public String globalZone;
	private String virtualZoneNum;
	private String excludeZonesForVirtualZone;
	private String rootPath;		
	private String fileNameConfig;	
	private String emergencyClearanceApprovingDay;
	//Properties to run the porgram ends
	
	public PriceExportDAOV2 priceExportDAOV2;
	public PriceDataLoad priceDataDAO;
	public ItemLoaderDAO itemLoaderDAO;	
	public ItemDAO itemDAO;	
	public RetailPriceDAO retailPriceDAO;
	public RetailCalendarDAO retailCalendarDAO;
	public ChainDAO chainDAO;
	public RetailPriceZoneDAO retailPriceZoneDAO;
	public StorePriceExportAZServiceV3 storePriceExportAZSerive;
	
	//variable for cache starts
	public HashMap<String, Integer> storeZoneMap = new HashMap<>();
	public List<ZoneDTO> allZoneData = new ArrayList<>();
	public List<ZoneDTO> globalZoneData = new ArrayList<>();
	public List<StoreDTO> storeData = new ArrayList<>();
	public HashMap<String, Integer> itemData = new HashMap<>();
	public List<PriceExportDTO> itemRUData = new ArrayList<>();
	public HashMap<Integer, String> zoneIdAndNoMap =null;
	 String baseChainId="";
	//variable for cache ends
	
	public boolean emergency = false;
	public boolean noRunIds = false;
	public boolean noCandidates = false;
	private boolean runAsBatch;
		
	LocalDate today = LocalDate.now();
	DateTimeFormatter dateFormatter = DateTimeFormatter.ofPattern("MM/dd/yyyy hh:mm:ss"); 	
	
	public List<Integer> excludeZoneIdForVirtualZone = new ArrayList<Integer>();
	private List<PriceExportDTO> salesFloorFiltered = new ArrayList<>();
	private List<PriceExportDTO> dataToClearDuplicate = new ArrayList<>();
		
	public StorePriceExportV3(boolean isBatch) {
		runAsBatch = isBatch;
		if(runAsBatch) {
			initializeProperties();
		}
		initializeConnection();
		
		priceDataDAO = new PriceDataLoad(false);
		priceExportDAOV2 = new PriceExportDAOV2();
		itemLoaderDAO = new ItemLoaderDAO();
		itemDAO = new ItemDAO();
		retailPriceDAO = new RetailPriceDAO();
		retailCalendarDAO = new RetailCalendarDAO();
		chainDAO = new ChainDAO();
		retailPriceZoneDAO = new RetailPriceZoneDAO();
		storePriceExportAZSerive = new StorePriceExportAZServiceV3();
		
		setSaleFloorLimitStr(PropertyManager.getProperty("SALE_FLOORITEM_LIMIT"));
		globalZone = PropertyManager.getProperty("NATIONAL_LEVEL_ZONE");
		virtualZoneNum = PropertyManager.getProperty("VIRTUAL_ZONE");
		excludeZonesForVirtualZone = PropertyManager.getProperty("EXCLUDE_ZONE_FOR_VIRTUAL_ZONE");
		rootPath = isBatch ? PropertyManager.getProperty("FILEPATH") : PropertyManager.getProperty("FILEPATHSIMULATION");
		fileNameConfig = PropertyManager.getProperty("FILENAME");
		emergencyClearanceApprovingDay = PropertyManager.getProperty("CLEARANCE_EMERGENCY_APRVD_DAY");
		
	}
	
	/**
	 * This constructor may be used for passing mocked DAO and other objects when instantiating this class from a test.
	 * All the external dependencies of this class that need to be mocked for testing should be assigned here.
	 */
	public StorePriceExportV3(boolean isBatch, PriceDataLoad priceDataLoad, PriceExportDAOV2 priceExportDAOV2, ItemLoaderDAO itemLoaderDAO, 
			ItemDAO itemDAO, RetailPriceDAO retailPriceDAO, RetailCalendarDAO retailCalendarDAO, ChainDAO chainDAO, 
			RetailPriceZoneDAO retailPriceZoneDAO) {
		runAsBatch = isBatch;
		if(runAsBatch) {
			initializeProperties();
		}
		initializeConnection();
		
		priceDataDAO = priceDataLoad;
		this.priceExportDAOV2 = priceExportDAOV2;
		this.itemLoaderDAO = itemLoaderDAO;
		this.itemDAO = itemDAO;
		this.retailPriceDAO = retailPriceDAO;
		this.retailCalendarDAO = retailCalendarDAO;
		this.chainDAO = chainDAO;
		this.retailPriceZoneDAO = retailPriceZoneDAO;
		storePriceExportAZSerive = new StorePriceExportAZServiceV3();
		
		setSaleFloorLimitStr(PropertyManager.getProperty("SALE_FLOORITEM_LIMIT"));
		globalZone = PropertyManager.getProperty("NATIONAL_LEVEL_ZONE");
		virtualZoneNum = PropertyManager.getProperty("VIRTUAL_ZONE");
		excludeZonesForVirtualZone = PropertyManager.getProperty("EXCLUDE_ZONE_FOR_VIRTUAL_ZONE");
		rootPath = isBatch ? PropertyManager.getProperty("FILEPATH") : PropertyManager.getProperty("FILEPATHSIMULATION");
		fileNameConfig = PropertyManager.getProperty("FILENAME");
		emergencyClearanceApprovingDay = PropertyManager.getProperty("CLEARANCE_EMERGENCY_APRVD_DAY");
		
	}
	
	public StorePriceExportV3(Connection conn, String priceExportType, String globalZone, String excludeZonesForVirtualZone,
			String fileNameConfig, String emergencyClearanceApprovingDay, String excludeZoneIdForVirtualZone,
			String exportDateStr) {
		this.conn = conn;
		this.priceExportType = priceExportType;
		this.globalZone = globalZone;
		this.excludeZonesForVirtualZone = excludeZonesForVirtualZone;
		this.fileNameConfig = fileNameConfig;
		this.emergencyClearanceApprovingDay = emergencyClearanceApprovingDay;
		this.excludeZonesForVirtualZone = excludeZoneIdForVirtualZone;
	}

	public static void main(String[] args) throws GeneralException {	

		String priceExportTypeFromArg = "";
		String priceExportDateFromArg = "";
		 
		// parse command line arguments
		for (int i = 0; i < args.length; i++) {
			if (args[i].startsWith("PRICE_EXPORT_TYPE")) {
				priceExportTypeFromArg = args[i].substring("PRICE_EXPORT_TYPE=".length());
			}
			//Date to be passed in MMddyyyy format
			if (args[i].startsWith("PRICE_EXPORT_DATE")) {
				priceExportDateFromArg = args[i].substring("PRICE_EXPORT_DATE=".length());
			}
			
		}	

		logger.info("*****************************************************************");		

		if(validateExportType(priceExportTypeFromArg)) {
			StorePriceExportV3 export = new StorePriceExportV3(true);
			export.setPriceExportType(priceExportTypeFromArg);
			export.setPriceExportDate(priceExportDateFromArg);
			export.exportApprovedPricesToCSV();
		}
		else {
			logger.error("The provided batch parameter is wrong!!");
			logger.info("Choose among: E / S / H / ES / EH / SH / ESH");
			System.exit(1);
		}
		
		logger.info("*****************************************************************");
	}
	
	private void setPriceExportType(String priceExportTypeFromArg) {
		this.priceExportType = priceExportTypeFromArg;
	}

	private void setPriceExportDate(String priceExportDateFromArg) {
		// Take current date if not passed from Args
		if (priceExportDateFromArg == null || priceExportDateFromArg.equals("")) {
			this.priceExportDate = DateUtil.localDateToString(LocalDate.now(), "MMddyyyy");
		} else {
			this.priceExportDate = priceExportDateFromArg;
		}
		
	}

	/**
	 * 
	 * @param priceExportType
	 * 
	 * Export approved prices to CSV
	 * @return String for the absolute path of the CSV file.
	 * 
	 */
	public String exportApprovedPricesToCSV() {
		
		return generateStorePriceExport(priceExportType, priceExportDate);
	}

	/**
	 * This method runs the generateStorePriceExport method in a new thread.
	 * @param priceExportType should be S, H, or SH
	 * @param priceExportDate should be in MMddyyyy format
	 * @param userId should be a string. 
	 * @return String representation of the exportId for the simulation. the exportId can be used 
	 * to query the PR_EXPORT_RUN_HEADER table to get information about and updates on the export being run. 
	 */
	public String asyncExport(String priceExportType, String priceExportDate, String userId) {
		PRExportRunHeader prExportRunHeader = new PRExportRunHeader();
		prExportRunHeader.setExportType(priceExportType);
		prExportRunHeader.setEffectiveDate(priceExportDate);
		prExportRunHeader.setRunType(runAsBatch?'B':'D');
		prExportRunHeader.setSfThreshold(Integer.parseInt(saleFloorLimitStr));
		prExportRunHeader.setStatus(0);
		prExportRunHeader.setUserId(userId);
		long exportId = priceExportDAOV2.insertInPRExportRunHeader(conn, prExportRunHeader);
		String exportIdStr = String.valueOf(exportId);
		if(exportId != -1) {
			Runnable exportTask = new Runnable() {
	
				@Override
				public void run() {
					generateStorePriceExport(priceExportType, priceExportDate);
				}
				
			};
			new Thread(exportTask,"exportThread").run();
		}
		else {
			logger.error("There was an error in generating an exportId for this export! Abandonning export!");
			priceExportDAOV2.updateExportRunHeaderStatus(conn, exportId, 3);
			priceExportDAOV2.updateExportRunHeaderEndDate(conn, exportId);
			
		}
		
		return exportIdStr;
	}
	
	public String generateStorePriceExport(String priceExportType, String priceExportDate) {	
		logger.debug("generateStorePriceExport starts .... ");
		logger.debug(String.format("priceExportType: %s, priceExportDate: %s", priceExportType, priceExportDate));
		logger.debug(String.format("salesFloorItemLimit set to: %s", getSaleFloorLimitStr()));
			
		String[] approvedDayArr = emergencyClearanceApprovingDay.split(",");
		List<String> approvedDays = Arrays.asList(approvedDayArr);
	    //String timeStamp = format.format(priceExportDate);
		StringBuilder fileNameBuilder = new StringBuilder(fileNameConfig);
		fileNameBuilder.append('-');
		fileNameBuilder.append(priceExportDate);
		if(!runAsBatch && null!=priceExportTime) {
			fileNameBuilder.append('_');
			fileNameBuilder.append(priceExportTime);
		}
		String fileName = fileNameBuilder.toString();
		fileNameBuilder = new StringBuilder(rootPath);
		fileNameBuilder.append('/');
		fileNameBuilder.append(fileName);
		fileNameBuilder.append(".txt");
		File sourceFile = new File(fileNameBuilder.toString());
		fileNameBuilder.append('.');
		fileNameBuilder.append(Constants.DONE);
		File targetFile = new File(fileNameBuilder.toString());
		
//		************************************************ FLOW OF THE PROGRAM ************************************************

		try {
			// populate caches for price export
			logger.info("Caching Data for store Export started... ");
			cacheDataForStoreExport();
			logger.info("Caching Data for store Export Completed... ");
			
			//Get count of items by Run id from export queue table .
			logger.info("Getting the existing count of Items grouped by Run id from Export Queue ... ");
			HashMap<Long, Integer> runIdandItemsCtMapFrmExportQueue = priceExportDAOV2.getItemCountByRunIdFromExportQueue(conn);
			logger.info("Getting the existing count of Items grouped by Run id from Export Queue Completed ... ");
			logger.info("# of Run id's in Export Queue :  "+ runIdandItemsCtMapFrmExportQueue.size());
			
			//populate approved run ids
			logger.info("Populating run id list for price export starts...");
			HashMap<String, List<Long>> runIdMap = populateRunIds(priceExportType, priceExportDate);
			
			// Populate approved Items from normal/emergency Run ids
			logger.info(" Get approved  items for price export started.. ");
			List<PriceExportDTO> itemsInQueue = getApprovedNormalRecommendedItems(runIdMap, priceExportType, priceExportDate);		
			logger.info(" Get approved  items for price export Completed and # of records :"+ itemsInQueue.size() );
			
			List<PriceExportDTO> approvedItemsList = unpackGlobalZoneApprovals(itemsInQueue, globalZoneData);
			
			int distinctApprovedItems = approvedItemsList.stream()
					.filter(priceExportDTO -> (null != priceExportDTO.getRetailerItemCode() 
					&& !priceExportDTO.getRetailerItemCode().isEmpty()))
					.collect(Collectors.groupingBy(PriceExportDTO::getRetailerItemCode))
					.size();
			logger.debug("Number of distinct approved items: " + distinctApprovedItems);
			
//			 populate emergency and clearance items based on approved day -- check this bhargavi
			HashMap<String,List<PriceExportDTO>> emergencyAndClearanceItems = null;
			if (approvedDays.contains(today.getDayOfWeek().toString()) 
					&& (Constants.EMERGENCY_OR_HARDPART.equals(priceExportType) 
							|| Constants.EMERGENCY_OR_SALESFLOOR.equals(priceExportType)
							|| Constants.ALL_SALESFLOOR_AND_HARDPART_AND_EMERGENCY.equals(priceExportType)
							)
					) {

				logger.info("Getting Emergency and Clearance Items.");
				List<PriceExportDTO> itemsFromEmergencyAndClearanceLists = priceExportDAOV2.getEmergencyAndClearanceItems(conn);
				logger.info("Retrieved items from all item lists of type 'Emergency' or 'Clearance'." );
				
//				TODO What's the rationale behind using the first approved item as a reference point?
				PriceExportDTO refObj = approvedItemsList.isEmpty() ? new PriceExportDTO() : approvedItemsList.get(0);
				
				String curWkStartDate = DateUtil.getWeekStartDate(0);
				RetailCalendarDTO retailCalendarDTO = retailCalendarDAO.getCalendarId(conn, curWkStartDate, Constants.CALENDAR_WEEK);
				
				Set<String> ecItemCodesAsStringsSet = new HashSet<>();
				itemsFromEmergencyAndClearanceLists.forEach(items -> {
					ecItemCodesAsStringsSet.add(String.valueOf(items.getItemCode()));
				});
				logger.info("Getting price info for " + ecItemCodesAsStringsSet.size() + " items.");
				logger.info("Item Codes are " + String.join(",", ecItemCodesAsStringsSet));
				List<Integer> historyCalendarList = new ArrayList<>();
		    	if(null == retailCalendarDTO.getStartDate()) {
		    		logger.debug("NO Start Date Found!!!");
		    	}else {
		    		historyCalendarList = getCalendarIdHistory(conn, retailCalendarDTO.getStartDate());
		    	}
				HashMap<String, List<RetailPriceDTO>> priceDataMap = retailPriceDAO.getRetailPriceInfoWithHistory(
						conn, ecItemCodesAsStringsSet, retailCalendarDTO.getCalendarId(), false, historyCalendarList);
				logger.info("Retrieved price info for emergency and clearance items.");

				List<String> excludeEcomStores = storePriceExportAZSerive.getExcludedEcomStores();
				List<String> excludedStores = storePriceExportAZSerive.getStoresExcludeZoneLevelData();

				emergencyAndClearanceItems = applyPriceDataToEmergencyAndClearanceItems(
						itemsFromEmergencyAndClearanceLists, refObj, retailCalendarDTO, priceDataMap, 
						excludeEcomStores, excludedStores, storeData, itemRUData, excludeZoneIdForVirtualZone, LocalDateTime.now());
//				
//				emergencyAndClearanceItems = populateEmergencyAndClearancePrices(storeData,
//								approvedItemsList, LocalDateTime.now());
//				
				updateDBWithECData(emergencyAndClearanceItems);
			}
			
//			 Aggregating zone 30
			populateVirtualZone(approvedItemsList); 
			
//			 decide export items based on emergency items
			List<PriceExportDTO> candidateItemList = populateExportItemsBasedOnCEitems(priceExportType,
					emergencyAndClearanceItems, approvedItemsList, runIdMap, priceExportDate);
			approvedItemsList = null;

//			Explode global zone prices to zones under zone 1000 - 19B changes
//			List<PriceExportDTO> exportItemList = new ArrayList<>();
//			exportItemList = explodeGlobalZonePriceToItsZones(candidateItemList);
			
//			 populate test zone data from test zone run ids
			HashMap<Integer, List<PriceExportDTO>> priceTestItemData = null;

			if (runIdMap.get("T") != null && runIdMap.get("T").size() > 0) {

				priceTestItemData = populateTestZonePricesV2(priceExportType, runIdMap.get("T"), priceExportDate,
						emergencyAndClearanceItems);
				if (priceTestItemData.size() > 0) {
					for (Map.Entry<Integer, List<PriceExportDTO>> entry : priceTestItemData.entrySet()) {
						candidateItemList.addAll(entry.getValue());
					}
				}
				logger.debug("# priceTestItemData: " + priceTestItemData.size());
			}
			
//			 list to update status
			List<PriceExportDTO> exportItemsToUpdate = populateExportListToUpdateStatus(candidateItemList);
			
			//write the export data to file
			writeExportDataInCSV(candidateItemList, fileName, sourceFile, targetFile);	
			
//			 PUT AUDIT LOG ON COUNT OF ITEMS
			auditLogs(candidateItemList);
			
			candidateItemList.clear();
			
			if(runAsBatch) {
//				 populate store lock items ('A' records approved items)
				HashMap<String, List<PriceExportDTO>> storeLockItemMap = populateStoreLockItems(priceExportType, priceTestItemData, 
						exportItemsToUpdate);
				
//				 Audit Store lock items from check list 
				if (storeLockItemMap.size() > 0 && (storeLockItemMap.get(Constants.STORE_LOCK_ITEMS_WITH_PRICE) != null
						&& storeLockItemMap.get(Constants.STORE_LOCK_ITEMS_WITH_PRICE).size() > 0)) {
					logger.info(" storeLockItemMap size for A records with price  : "
							+ storeLockItemMap.get(Constants.STORE_LOCK_ITEMS_WITH_PRICE).size());
//					 condition corrected by Karishma
					if (priceTestItemData == null) {
						candidateItemList.addAll(storeLockItemMap.get(Constants.STORE_LOCK_ITEMS_WITH_PRICE));
						logger.info("Export Items List size inside1: " + candidateItemList.size());
//						Audit the store lock items
//						AuditStoreLockItems(conn, storeLockItemMap.get(Constants.STORE_LOCK_ITEMS));
						AuditStoreLockItems(conn, storeLockItemMap.get(Constants.STORE_LOCK_ITEMS_WITH_PRICE));
					}
					else {
	
//						 Decide store lock items to export based on price test data
						List<PriceExportDTO> filteredStoreLockItems = exportItemListDataBasedOnPriceTest(storeLockItemMap,
								priceTestItemData);
						candidateItemList.addAll(filteredStoreLockItems);
						AuditStoreLockItems(conn, filteredStoreLockItems);
					}			
				}			
				storeLockItemMap.clear();
				
//				write the export data to file
				writeExportDataInCSV(candidateItemList, fileName, sourceFile, targetFile);	
				candidateItemList.clear();

//				 populate expiry items (store lock expires for items - 'D' records)
				HashMap<String,PriceExportDTO> expiryItems = populateExpiryItemList(priceExportType, priceTestItemData,exportItemsToUpdate, 
						fileName, sourceFile, targetFile);
				
//				Added on 08/18/2021
//				Audit trial for D records - PROM-2248		
//				Populate audit for count of distinct items, distinct stores, export time for each location-RU combination
				if (expiryItems.size() > 0) {
					AuditExpiryItems(expiryItems);
					expiryItems.clear();
				}
			}
			else {
				logger.debug("Skipped all the processing of store lock items (A and D records) as this is a simmulation run.");
			}
				
//			rename txt file to txt.done file
			rename(sourceFile, targetFile);		
			
			if(runAsBatch) {
//				status update for exported items.
//				Added new parameter to compare the existing count with initial count of items from Export queue table
				updateExportStatus(exportItemsToUpdate, runIdMap,runIdandItemsCtMapFrmExportQueue);
	
//				populate data when no run id/approved data available
				changeEffDataIfNoApprovedData(priceExportType, emergencyAndClearanceItems, runIdMap);
	
//				status update in notification and audit service
				updateExportStatusInNotificationAndAuditTrial(runIdMap, exportItemsToUpdate);
			}
			else {
				logger.debug("Skipped export status update and export status update in notification and audit trail.");
				logger.debug("Skipped changing effective date when there's no approved data.");
			}
			
		}
		catch(GeneralException | Exception e) {
			logger.error("exportPrice() - Error while exporting prices", e);
			PristineDBUtil.rollbackTransaction(conn, "Price export update status");
		} finally {
			PristineDBUtil.close(conn);
		}
		return targetFile.getAbsolutePath();
		
	}
	
	/**
	 * 
	 * @param referenceItemList
	 * @return
	 * 
	 * copy global zone price to the zones under it (zone 4 and zone 16 currently)
	 */
	public List<PriceExportDTO> explodeGlobalZonePriceToItsZones(List<PriceExportDTO> referenceItemList) {
		
		HashMap<String, Integer> zoneMap = new HashMap<>();
		HashMap<Integer, String> zoneIdandNameMap = new HashMap<>();
		for (ZoneDTO zones : globalZoneData) {
			zoneMap.put(zones.getZnNo(), zones.getZnId());
			zoneIdandNameMap.put(zones.getZnId(), zones.getZnName());
		}
		
		List<PriceExportDTO> referenceList = new ArrayList<>();
		referenceItemList.forEach(referenceItem -> {
			if (referenceItem.isGlobalZoneRecommended()) {
				zoneMap.forEach((zoneNum, priceZoneId) -> {
					String zoneName = zoneIdandNameMap.get(priceZoneId);
					try {
						PriceExportDTO zoneItem = (PriceExportDTO) referenceItem.clone();
						zoneItem.setPriceZoneId(priceZoneId);
						zoneItem.setPriceZoneNo(zoneNum);
						zoneItem.setZoneName(zoneName);
						zoneItem.setGlobalZoneRecommended(true);
						referenceList.add(zoneItem);

					} catch (Exception e) {
						logger.error("applyGlobalZonePriceToAllZones() - Error while setting global zone's price : "+ e);
					}
				});
			} else {
				referenceList.add(referenceItem);
			}
		});
		
		return referenceList;
	}
	
	public void initializeConnection() {
		if (conn == null) {
			try {
				conn = DBManager.getConnection();
			} catch (GeneralException exe) {
				logger.error("Error while connecting to DB:" + exe);
				System.exit(1);
			}
		}
	}
	
	/**
	 * Initializing property values required to export 
	 */
	public void initializeProperties() {
		logger.info("Reading the .properties file ");
		logger.debug("analysis.properties");
		PropertyManager.initialize("analysis.properties");
		logger.info("Properties initialized");
		logger.debug("from analysis.properties");
		PropertyConfigurator.configure("log4j-price-export.properties");
		
	}

	/**
	 * @param exportFileRoot the absolute path of the directory where the export file will be created.
	 * 
	 */
	public void setRootPath(String exportFileRoot) {
		rootPath = exportFileRoot;
	}

	/**
	 * Sets the sales floor limit
	 * @param limit specifies the threshold for the number of sales floor items that can be exported.
	 */
	public void setSaleFloorLimitStr(String limit) {
		saleFloorLimitStr = limit;
	}
	
	public String getSaleFloorLimitStr() {
		return this.saleFloorLimitStr;
	}

	/**
	 * 
	 * @param sourceFile
	 * @param targetFile
	 * @throws Exception
	 * 
	 * Rename the text file to .done file extension
	 */
	private void rename(File sourceFile, File targetFile) throws Exception {
		if (sourceFile.equals(targetFile)) {
			logger.warn("rename() - Source and target files are the same [" + sourceFile + "]. Skipping.");
			return;
		}

		if (sourceFile.exists()) {

			boolean result = sourceFile.renameTo(targetFile);

			if (result) {
				logger.info("Renamed file [" + sourceFile + "] to [" + targetFile + "]");
				logger.debug("1kb file renamed");
			} else {
				logger.warn("rename() - Failed to rename file [" + sourceFile + "] as [" + targetFile + "].");
			}
		} else {
			throw new Exception("File [" + sourceFile + "] does not exist.");
		}

	}

	/**
	 * 
	 * @param storeLockItemMap
	 * @param itemStoreCombinationsFromPriceTest
	 * @return filteredStoreLockItems
	 * 
	 * Filters the store lock items based on the item-store combination in test zone.
	 * i.e. export item-store data of store lock which is not in price test data
	 * 
	 */
	private List<PriceExportDTO> exportItemListDataBasedOnPriceTest(HashMap<String, List<PriceExportDTO>> storeLockItemMap,
			HashMap<Integer, List<PriceExportDTO>> itemStoreCombinationsFromPriceTest) {
		
		List<PriceExportDTO> filteredStoreLockItems = new ArrayList<>();
		
		HashMap<String,String> storeNumIDMap = new HashMap<>();
		for(StoreDTO stores : storeData) {
			storeNumIDMap.put(stores.strNum, String.valueOf(stores.strId));
		}
		
		if (storeLockItemMap.size() > 0 && (storeLockItemMap.get(Constants.STORE_LOCK_ITEMS_WITH_PRICE) != null &&
				storeLockItemMap.get(Constants.STORE_LOCK_ITEMS_WITH_PRICE).size() > 0)) {
			List<PriceExportDTO> storeLockItemsWithPrice = storeLockItemMap.get(Constants.STORE_LOCK_ITEMS_WITH_PRICE);

			if (itemStoreCombinationsFromPriceTest!=null && itemStoreCombinationsFromPriceTest.size() > 0) {
				logger.debug("filterAndAddItemsBasedOnPriceTestData starts..");
				filteredStoreLockItems = storePriceExportAZSerive.filterAndAddItemsBasedOnPriceTestDataV3(
						itemStoreCombinationsFromPriceTest, storeLockItemsWithPrice, storeNumIDMap);
				logger.debug("filterAndAddItemsBasedOnPriceTestData ends..");
			} else {
				filteredStoreLockItems.addAll(storeLockItemsWithPrice);
			}
		}
		return filteredStoreLockItems;
	}

	/**
	 * 
	 * @param priceExportType
	 * @param testZoneRunids
	 * @param todaysDate
	 * @param emergencyAndClearanceItems
	 * @return
	 * 
	 * 
	 * Collect the emergency clearance items, 
	 * if there are ec items matching with test zone items then export ec item data ignore test zone item data
	 */
	private HashMap<Integer, List<PriceExportDTO>> populateTestZonePricesV2(String priceExportType, List<Long> testZoneRunids, 
			String todaysDate, HashMap<String, List<PriceExportDTO>> emergencyAndClearanceItems) {

		HashMap<Integer, List<PriceExportDTO>> itemStoreCombinationsFromPriceTest = new HashMap<>();

		logger.info("populateTestZonePrices() - Populating Test zone data ");

		HashMap<String, String> storeNumIDMap = new HashMap<>();
		for (StoreDTO stores : storeData) {
			storeNumIDMap.put(stores.strNum, String.valueOf(stores.strId));
		}
		
		Set<Integer> ecItems = new HashSet<>();
		if (emergencyAndClearanceItems != null && emergencyAndClearanceItems.get("EC ITEMS WITH PRICE") != null) {
			List<PriceExportDTO> emeregencyItem = emergencyAndClearanceItems.get("EC ITEMS WITH PRICE");
			ecItems = emeregencyItem.stream().collect(Collectors.groupingBy(PriceExportDTO::getItemCode)).keySet();
		}
		
		itemStoreCombinationsFromPriceTest = getPriceTestStoreLevelDataV2(testZoneRunids, priceExportType, todaysDate, ecItems);
		return itemStoreCombinationsFromPriceTest;
	}
	
	/**
	 * 
	 * @param testZoneRunIdList
	 * @param input
	 * @param todaysDate
	 * @param ecItems
	 * @return
	 * 
	 * Get the approved test zone data
	 * filter the items that are not in emergency and clearance items
	 * get the test zone data across the stores which it is requested
	 * 
	 * Add the store level test data in to export
	 * 
	 */
	private HashMap<Integer, List<PriceExportDTO>> getPriceTestStoreLevelDataV2(List<Long> testZoneRunIdList,
			String input, String todaysDate, Set<Integer> ecItems) {
		
		HashMap<Integer, List<PriceExportDTO>> itemStoreCombinationsFromPriceTest = new HashMap<>();
		List<PriceExportDTO> priceTestItemZoneLevelEntries = new ArrayList<>();	
		try {
			//get approved items and price
			priceTestItemZoneLevelEntries.addAll(priceExportDAOV2.getItemsFromApprovedRecommendationsV3(conn, testZoneRunIdList,
					input, emergency,todaysDate));
			
			HashMap<Integer, List<PriceExportDTO>> ZoneItemMap = new HashMap<>();
			if(ecItems != null && ecItems.size() > 0) {
				ZoneItemMap = (HashMap<Integer, List<PriceExportDTO>>) priceTestItemZoneLevelEntries.stream()
						.filter(e -> !ecItems.contains(e.getItemCode()))
						.distinct().collect(Collectors.groupingBy(PriceExportDTO::getPriceZoneId));
			}
			else {
				ZoneItemMap = (HashMap<Integer, List<PriceExportDTO>>) priceTestItemZoneLevelEntries.stream()
						.distinct().collect(Collectors.groupingBy(PriceExportDTO::getPriceZoneId));
			}
			
			
			//get store details of test zone requested
			HashMap<Integer,List<PriceExportDTO>> ZoneStoreMap = new HashMap<>();
			ZoneStoreMap = priceExportDAOV2.getTestZoneStoreCombinationsDictionary(conn, testZoneRunIdList);
			
			for(Map.Entry<Integer, List<PriceExportDTO>> zone : ZoneStoreMap.entrySet()) {
				List<PriceExportDTO> storeList = zone.getValue();
				List<PriceExportDTO> zoneItemData = ZoneItemMap.get(zone.getKey());
				if(zoneItemData!= null && storeList!=null) {
					for(PriceExportDTO IZ : zoneItemData){
						List<PriceExportDTO> testDataToExport = new ArrayList<>();
						List<PriceExportDTO> filteredStoreListByRunId = storeList.stream().filter(e->e.getRunId() == IZ.getRunId())
								.collect(Collectors.toList());
						Set<String> stores = new HashSet<>();
						for(PriceExportDTO testStoreObj : filteredStoreListByRunId) {
							stores.add(testStoreObj.getStoreNo());
						}
						for(PriceExportDTO testStoreObj : filteredStoreListByRunId) {
							PriceExportDTO IZobj = (PriceExportDTO) IZ.clone();
							IZobj.setStoreId(testStoreObj.getStoreId());
							IZobj.setStoreNo(testStoreObj.getStoreNo());
							IZobj.setZoneName(testStoreObj.getZoneName());
							IZobj.setPriceZoneNo(testStoreObj.getPriceZoneNo());
							IZobj.setPriceZoneId(testStoreObj.getPriceZoneId());
							IZobj.setChildLocationLevelId(Constants.STORE_LEVEL_TYPE_ID);
							IZobj.setStoreLockExpiryFlag(Constants.EXPORT_ADD_DATA);
							IZobj.setStoreNums(stores);
							testDataToExport.add(IZobj);
						}
						itemStoreCombinationsFromPriceTest.put(IZ.getItemCode(), testDataToExport);
					}						
					}
				}
			
		} catch (GeneralException | CloneNotSupportedException e) {

			logger.error("GetPriceTestStoreLevelData(): Error in Getting Price Check List Data: "+e.getMessage());
		}
		
		
		return itemStoreCombinationsFromPriceTest;
	}

	private void cacheDataForStoreExport() {
		logger.info("cacheDataForStoreExport() Started .....");
		
		try {
			storeZoneMap = retailPriceZoneDAO.getStoreZoneMapping(conn);
			logger.info("cacheDataForStoreExport() - # records in storeNumber and Zone ID Map :- " + storeZoneMap.size());
			
			storeData = priceExportDAOV2.getStoreData(conn);
			logger.info("cacheDataForStoreExport() - # records in storeData List  : " + storeData.size());
			
			allZoneData = priceExportDAOV2.getZoneData(conn, false);
			logger.info("cacheDataForStoreExport() - # records in allZoneData List  :  " + allZoneData.size());
			
			globalZoneData = priceExportDAOV2.getZoneData(conn, true);
			logger.info("cacheDataForStoreExport() - # records in globalZoneData List  :  " + globalZoneData.size());
			
			itemData = itemDAO.getActiveItems(conn);
			logger.info("cacheDataForStoreExport() - # of active Items fetched  :  " + itemData.size());
			
			itemRUData = priceExportDAOV2.getRUofItems(conn);
			logger.info("cacheDataForStoreExport() - # of itemRUData fetched  : " + itemRUData.size());
			
			zoneIdAndNoMap = retailPriceZoneDAO.getZoneIdAndNoMap(conn);
			logger.info("cacheDataForStoreExport()-# of Actice Zones: " + zoneIdAndNoMap.size());
			
			baseChainId = chainDAO.getBaseChainId(conn);
			logger.info("cacheDataForStoreExport()-baseChainId: " + baseChainId);
			
			String[] arrayOfExcludeZone = excludeZonesForVirtualZone.split(",");
			List<String> excludeStoreListForVitualZone = Arrays.asList(arrayOfExcludeZone);
			HashMap<String, List<ZoneDTO>> zoneMap = (HashMap<String, List<ZoneDTO>>) allZoneData.stream()
					.collect(Collectors.groupingBy(ZoneDTO :: getZnNo));
			for (String zoneNum : excludeStoreListForVitualZone) {
				excludeZoneIdForVirtualZone.add(zoneMap.get(zoneNum).get(0).getZnId());
			}
			
			
		} catch (GeneralException e) {
			logger.info("cacheDataForStoreExport() - Error while getting cache" + e);
		}
	}

	private static boolean validateExportType(String priceExportType) {

		if (Constants.EMERGENCY_OR_HARDPART.equals(priceExportType)
				|| Constants.EMERGENCY_OR_SALESFLOOR.equals(priceExportType)
				|| Constants.EMERGENCY.equals(priceExportType)
				|| Constants.ALL_SALESFLOOR_AND_HARDPART_AND_EMERGENCY.equals(priceExportType)
				|| Constants.BOTH_SALESFLOOR_AND_HARDPART.equals(priceExportType)
				|| Constants.SALE_FLOOR_ITEMS.equals(priceExportType)
				|| Constants.HARD_PART_ITEMS.equals(priceExportType)) {

			return true;
		} else {
			return false;
		}
	}
	
	/**
	 *  @param input String representing the export type. Permitted values: S, H, SH, and ESH.
	 *  @param todaysDate String representation of a date in the MMddyyyy format.
	 *  @return HashMap with 2 entries with the String keys "N" and "T".
	 *  The value of the entry with key "N" is a List of run IDs of type Long for normal zones (zone type "W", or zone type "I")
	 *  The value of the entry with key "T" is a List of run IDs of type Long for test zones (zone type "T")
	 *  
	 */
	public HashMap<String, List<Long>> populateRunIds(String input, String todaysDate) {

		HashMap<String, List<Long>> runIdMap = new HashMap<>();

		try {
			List<Long> normalRunIdList = new ArrayList<>();
			List<Long> emergencyRunIdList = new ArrayList<>();
			List<Long> runIdList = new ArrayList<>();

			//populating emergency run ids
			if(Constants.ALL_SALESFLOOR_AND_HARDPART_AND_EMERGENCY.equals(input) || 
					Constants.EMERGENCY_OR_HARDPART.equals(input) || Constants.EMERGENCY_OR_SALESFLOOR.equals(input)) {
				emergencyRunIdList = priceExportDAOV2.getRunIdsByType(conn, todaysDate, Constants.EMERGENCY);
			}

			if (emergencyRunIdList.size() > 0) {
				logger.info("populateRunIds()- Emergency RunId List "+ emergencyRunIdList.size());
				emergency = true;
				runIdList.addAll(emergencyRunIdList);
			} else {
				// populating normal run ids only when emergency runids are not available
				normalRunIdList = priceExportDAOV2.getRunIdsByType(conn, todaysDate, Constants.NORMAL);
				logger.info("populateRunIds()- normalRunIdList RunId List:  "+ normalRunIdList.size());
				if (normalRunIdList.size() > 0) {
					runIdList.addAll(normalRunIdList);
				}
			}
			
			//separating test zone run ids and regular zone run ids
			if(runIdList.size() > 0) {
				runIdMap = priceExportDAOV2.separateTestZoneAndRegularZoneRunIds(conn, runIdList);			
			}
			else {
				logger.info("populateRunIds()-  No recommended data found to export!");
				noRunIds = true;
			}
		} catch (GeneralException e) {
			logger.error("populateRunIds()-  Exception while fetching runId's for export: "+ e);
		}

		return runIdMap;

	}
	
	/**
	 * 
	 * @param runIdMap
	 * @param priceExportType S/H/SH/ES/EH/ESH
	 * @param todaysDate
	 * @return
	 * @throws GeneralException
	 * 
	 * populate approved items by filtering export type, current date as effective date, 
	 * regular price <> recom price when impact = 0, overriden price <> recom price
	 * 
	 */
	public List<PriceExportDTO> getApprovedNormalRecommendedItems(HashMap<String, List<Long>> runIdMap, 
			String priceExportType, String todaysDate) throws GeneralException {

		List<PriceExportDTO> itemsInQueue = new ArrayList<>();
		if (runIdMap.size() > 0) {
			List<Long> normalRunIdList = runIdMap.get("N");
			try {
				itemsInQueue = priceExportDAOV2.getItemsFromApprovedRecommendationsV3(conn,normalRunIdList, priceExportType, 
						emergency, todaysDate);
			} catch (GeneralException e) {
				logger.info("populateApprovedItemList() - Error while populating approved items: " + e);
				throw new GeneralException("populateApprovedItemList() - Error while populating approved items: ", e);
			}
		}
		return itemsInQueue;
}

	/**
	 * If the supplied itemList is null or empty then com.pristine.dataload.offermgmt.StorePriceExportV3.noCandidates is set to true 
	 * and the itemList is returned back.
	 * If no items have been approved for the global zone then the list returned is the same as itemList.
	 * Approvals for zones not under the global zone stay unchanged.
	 * Global zone approvals are replicated as approvals for zones under the global zone.
	 * If approvals already exist for zones under the global zone for the same items then 
	 * the latest approval is retained and the older one is discarded.
	 * 
	 * @param itemList List of PriceExportDTO objects for approved items. 
	 * @param globalZoneData List of ZoneDTO objects for the zones that fall under the global zone.
	 * @return List<PriceExportDTO> created from itemList by replacing the elements for the global 
	 * zone with elements for the zones that fall under the global zone.
	 * 
	 */
	public List<PriceExportDTO> unpackGlobalZoneApprovals(List<PriceExportDTO> itemList, List<ZoneDTO> globalZoneData) {

		if (null == itemList || itemList.isEmpty()) {
			logger.info("applyGlobalZonePriceToAllZones() - No approved items found!");
			noCandidates = true;
			return itemList;
		}
		logger.info("unpackGlobalZoneApprovals() - Splitting global zone approvals into approvals for constituent zones.... ");
		
		List<PriceExportDTO> itemListWithGlobalZoneExpanded = new ArrayList<PriceExportDTO>();

//		Data for zones that fall under the global zone (4 and 16 when this code was written) mapped by their zone numbers.
		Map<String, List<ZoneDTO>> globalZoneNoToGlobalZoneDataMap = globalZoneData.stream()
				.collect(Collectors.groupingBy(ZoneDTO::getZnNo));
		
//		Map item code to list of approvals for zones for that item.
		HashMap<Integer, List<PriceExportDTO>> approvalsGroupedByItemCode = (HashMap<Integer, List<PriceExportDTO>>) itemList.stream()
				.collect(Collectors.groupingBy(PriceExportDTO::getItemCode));
		
		List<PriceExportDTO> approvalsForCurrentItemCode, currentItemApprovalsWithGlobalZoneSplit;
		for(Map.Entry<Integer, List<PriceExportDTO>> itemApprovals : approvalsGroupedByItemCode.entrySet()) {
			approvalsForCurrentItemCode = itemApprovals.getValue();
			currentItemApprovalsWithGlobalZoneSplit = unpackGlobalZoneApprovalForOneItem(approvalsForCurrentItemCode, 
					globalZoneNoToGlobalZoneDataMap);
			itemListWithGlobalZoneExpanded.addAll(currentItemApprovalsWithGlobalZoneSplit);
		}
		logger.info("unpackGlobalZoneApprovals- no. of approvals after sliptting global zone approvals:"+ itemListWithGlobalZoneExpanded.size());
		return itemListWithGlobalZoneExpanded;
	}
	
	/**
	 * Similar to unpackGlobalZoneApprovals but for approvals for a single item.
	 *
	 * @param List<PriceExportDTO> approvalsForAnItemCode List of PriceExportDTO objects with the same itemCode. 
	 * @param Map<String, List<ZoneDTO>> globalZoneNoToGlobalZoneData List of ZoneDTO objects for the zones that fall under the global zone mapped to the zone number.
	 * @return List<PriceExportDTO> created from approvalsForAnItemCode by replacing the element for the global 
	 * zone with elements for the zones that fall under the global zone.
	 */
	private List<PriceExportDTO> unpackGlobalZoneApprovalForOneItem(
			List<PriceExportDTO> approvalsForAnItemCode, 
			Map<String, List<ZoneDTO>> globalZoneNoToGlobalZoneData) {
		/* Note: The approvals referred to here are for price recommendations for an item to be sold at a particular location 
		 * (i.e. zone for the scope of this method). Now, selling an item at the same location for multiple prices makes no sense.
		 * This is why, this method assumes that one item will/should only have one approval for a given zone. 
		 * As such, when the list of approvals (List<PriceExportDTO>) for one item is grouped by zones, 
		 * the resultant mapping will have only one element/approval per zone. When the data-type of such a grouping is a 
		 * Map<String, List<PriceExportDTO>> then the value of each map entry is essentially going to be a list of size 1.
		 * */
		List<PriceExportDTO> approvalsForThisItemWithGlobalZoneUnpacked = new ArrayList<>();

		List<PriceExportDTO> tempPriceExportDTOList = approvalsForAnItemCode.stream()
				.filter(x -> x.getPriceZoneNo().equalsIgnoreCase(Constants.AZ_GLOBAL_ZONE))
				.collect(Collectors.toList());
		if(null==tempPriceExportDTOList || tempPriceExportDTOList.isEmpty()) {// this item code has not been approved for the global zone
			approvalsForThisItemWithGlobalZoneUnpacked.addAll(approvalsForAnItemCode); // add all approval elements to the result
			return approvalsForThisItemWithGlobalZoneUnpacked; // return the result. Nothing more needs to be done in this method.
		}// cheers!
		PriceExportDTO globalZoneApprovalForThisItemCode = tempPriceExportDTOList.get(0);//refer Note above for why hard coded .get(0).
		tempPriceExportDTOList = null;
		
//		Get approvals for this item code for those zones that do NOT fall under the global zone (5, 49, 66, 67, etc.)
//		Map them to their zone numbers. Let's call such zones individual zones.
		Map<String, List<PriceExportDTO>> approvalsForIndividualZonesForThisItemCode = approvalsForAnItemCode.stream()
				.filter(x -> !x.getPriceZoneNo().equalsIgnoreCase(Constants.AZ_GLOBAL_ZONE) //Not the global zone and 
						&& !globalZoneNoToGlobalZoneData.containsKey(x.getPriceZoneNo())) //not a zone that falls under the global zone.
				.collect(Collectors.groupingBy(PriceExportDTO::getPriceZoneNo));
		
		if(null==approvalsForIndividualZonesForThisItemCode||approvalsForIndividualZonesForThisItemCode.isEmpty()) {
//			this item code does not have approvals for any of the "Individual Zones".
		}
		else {
			PriceExportDTO individualZoneApprovalClone;//TODO Why to clone? why not use original?
			for(Map.Entry<String, List<PriceExportDTO>> indididualZoneApproval : approvalsForIndividualZonesForThisItemCode.entrySet()) {
				try {
					individualZoneApprovalClone = (PriceExportDTO) indididualZoneApproval.getValue().get(0).clone();//refer Note above for why hard coded .get(0).
					individualZoneApprovalClone.setGlobalZoneRecommended(true);
					approvalsForThisItemWithGlobalZoneUnpacked.add(individualZoneApprovalClone);
				} catch (Exception e) {
					logger.debug("unpackGlobalZoneApprovalForOneItem() - Error while cloning individual zone approval.");
				}
			}
		}
		
//		Unpack the global zone approval into approvals for the zones that fall under the global zone 
//		(4 and 16 when this code was written). Let's call these zones regular zones.
		Map<String, List<PriceExportDTO>> unpackedGlobalZoneApprovalForThisItemCode = new HashMap<String, List<PriceExportDTO>>();
		PriceExportDTO globalZoneApprovalForThisItemCodeClone;
		List<PriceExportDTO> globalZoneApprovalForThisItemCodeCloneList;
		ZoneDTO regularZone;
		for(Map.Entry<String, List<ZoneDTO>> noToDTO : globalZoneNoToGlobalZoneData.entrySet()) {
			regularZone = noToDTO.getValue().get(0);//refer Note above for why hard coded .get(0).
			try {
				globalZoneApprovalForThisItemCodeClone = (PriceExportDTO) globalZoneApprovalForThisItemCode.clone();
				globalZoneApprovalForThisItemCodeClone.setPriceZoneId(regularZone.getZnId())
				.setPriceZoneNo(regularZone.getZnNo())
				.setZoneName(regularZone.getZnName())
				.setGlobalZoneRecommended(true);
				globalZoneApprovalForThisItemCodeCloneList = new ArrayList<>();
				globalZoneApprovalForThisItemCodeCloneList.add(globalZoneApprovalForThisItemCodeClone);
				unpackedGlobalZoneApprovalForThisItemCode.put(
						globalZoneApprovalForThisItemCodeClone.getPriceZoneNo(), 
						globalZoneApprovalForThisItemCodeCloneList);
			} catch (CloneNotSupportedException e) {
				logger.error("unpackGlobalZoneApprovalForOneItem() - Error while cloning global zone approval!", e);
			}
		}
		dataToClearDuplicate.add(globalZoneApprovalForThisItemCode);
		
//		Get approvals for this item code for those zones that fall under the global zone (4 and 16 when this code was written)
//		Map them to their zone numbers.
		Map<String, List<PriceExportDTO>> approvedZoneNosUnderGlobalZone = approvalsForAnItemCode.stream()
		.filter(x -> globalZoneNoToGlobalZoneData.containsKey(x.getPriceZoneNo()))
		.collect(Collectors.groupingBy(PriceExportDTO::getPriceZoneNo));

		if(null==approvedZoneNosUnderGlobalZone || approvedZoneNosUnderGlobalZone.isEmpty()) {
//			This item code does not have approvals for any of the zones that fall under the global zone.
//			The approvals created for zones under the global zone by cloning the global zone approval can be added to the result.
			for(Map.Entry<String, List<PriceExportDTO>> regularZoneToregularZoneApprovals : 
				unpackedGlobalZoneApprovalForThisItemCode.entrySet()) {
				approvalsForThisItemWithGlobalZoneUnpacked.add(regularZoneToregularZoneApprovals.getValue().get(0));//refer Note above for why hard coded .get(0).
			}
		}
		else {//This item has been approved for one or more zones that fall under the global zone.
			/*
			 * Now we have to perform a 
			 * LEFT OUTER 'kind-of-a-JOIN' on unpackedGlobalZoneApprovalForThisItemCode and approvedZoneNosUnderGlobalZone.
			 * It's 'kind-of-a-JOIN' because the values for the common keys will not be the same. 
			 * We take the value that has the latest approval date. 
			 * It's LEFT OUTTER because if there are no original approvals for some zone/s under the global zone then, 
			 * the approvals generated for such zone/s by cloning the global zone approval will have to be passed on to the result.
			*/ 
			SimpleDateFormat dateFormater = new SimpleDateFormat("MM/dd/yyyy HH:mm:ss");
			Date globalZoneApprovalDate, regularZoneApprovalDate;
			try {
				globalZoneApprovalDate = dateFormater.parse(globalZoneApprovalForThisItemCode.getApprovedOn());
				
				Map<String, List<PriceExportDTO>> notToCompare = unpackedGlobalZoneApprovalForThisItemCode.entrySet().stream()
				.filter(x -> !approvedZoneNosUnderGlobalZone.keySet().contains(x.getKey()))
				.collect(Collectors.toMap(x -> x.getKey(), x -> x.getValue()));
				if(null==notToCompare||notToCompare.isEmpty()) {
//					This item has approvals for all the zones that fall under the global zone.
				}
				else {//This item was NOT approved for some zone/s that fall under the global zone.
					for(Map.Entry<String, List<PriceExportDTO>> entry : notToCompare.entrySet()) {
						//add the approvals generated by global zone unpacking for such zones.
						approvalsForThisItemWithGlobalZoneUnpacked.add(entry.getValue().get(0));//refer Note above for why hard coded .get(0).
					} // Takes care of the LEFT OUTTER part
				}
				
				Map<String, List<PriceExportDTO>> toCompare = unpackedGlobalZoneApprovalForThisItemCode.entrySet().stream()
						.filter(x -> approvedZoneNosUnderGlobalZone.keySet().contains(x.getKey()))
						.collect(Collectors.toMap(x -> x.getKey(), x -> x.getValue()));
//				Takes care of the 'kind-of-a-JOIN' part.
				List<PriceExportDTO> regularZoneApprovalList;
				PriceExportDTO regularZoneApproval, approvalCreatedFromGlobalZoneApproval;
				Boolean regularZoneApprovalIsLatest;
				for(Map.Entry<String, List<PriceExportDTO>> entry : toCompare.entrySet()) {
					approvalCreatedFromGlobalZoneApproval = entry.getValue().get(0);//refer Note above for why hard coded .get(0).
					regularZoneApprovalList = approvedZoneNosUnderGlobalZone.get(entry.getKey());
					regularZoneApproval = regularZoneApprovalList.get(0);//refer Note above for why hard coded .get(0).
					regularZoneApprovalDate = dateFormater.parse(regularZoneApproval.getApprovedOn());
					regularZoneApprovalIsLatest = regularZoneApprovalDate.compareTo(globalZoneApprovalDate)>=0;
					if(regularZoneApprovalIsLatest) {
						approvalsForThisItemWithGlobalZoneUnpacked.add(regularZoneApproval);
//						No need to mark approvalCreatedFromGlobalZoneApproval for deletion as it was never in the list of approvals (export queue).
					}
					else {
						approvalsForThisItemWithGlobalZoneUnpacked.add(approvalCreatedFromGlobalZoneApproval);
//						The approval for this zone will not be added to the result list as the approval resulting from the 
//						global zone approval is taking its place. It now needs to be stored somewhere so that it may be 
//						remembered for deletion from the list of approvals.
						dataToClearDuplicate.add(regularZoneApproval);
					}
				}
			} catch (ParseException parseException) {
				logger.error("Error while parsing approval date of regular or global zone approval.", parseException);
			}
		}
		return approvalsForThisItemWithGlobalZoneUnpacked;
	}

	/**
	 * populate list of emergency and clearance data, set VDP retail = EC retail, effetcive date = start date of item in item list 
	 * If there is no regular price for the item dont export that item
	 * 
	 * Once emergency export is done update and insert the emergency and clearance item list in to table 
	 * called PR_EC_EXPORT_HEADER and PR_EC_EXPORT_DETAIL
	 * 
	 * Once the data is exported flag the items as is_exported = 'Y'
	 * @param itemsFromEmergencyAndClearanceLists
	 * @param storeDatCache
	 * @param approvedItemList
	 * @param retailCalendarDTO
	 * @param priceDataMap
	 * @param excludeEcomStores
	 * @param excludedStores
	 * @param itemRUDataCache
	 * @param excludeZoneIdForVirtualZone
	 * @param now
	 * @return
	 * @throws GeneralException
	 */
	public HashMap<String, List<PriceExportDTO>> populateEmergencyAndClearancePrices(
			List<PriceExportDTO> itemsFromEmergencyAndClearanceLists, List<StoreDTO> storeDatCache,
			List<PriceExportDTO> approvedItemList, 
			RetailCalendarDTO retailCalendarDTO, 
			HashMap<String, List<RetailPriceDTO>> priceDataMap, 
			List<String> excludeEcomStores, 
			List<String> excludedStores, 
			List<PriceExportDTO> itemRUDataCache, 
			List<Integer> excludeZoneIdForVirtualZone, LocalDateTime now) throws GeneralException {
		
		HashMap<String, List<PriceExportDTO>> emergencyAndClearanceItems = new HashMap<>();

//		TODO What's the rationale behind using the first approved item as a reference point?
		PriceExportDTO refObj = approvedItemList.isEmpty() ? new PriceExportDTO() : approvedItemList.get(0);
		emergencyAndClearanceItems = applyPriceDataToEmergencyAndClearanceItems(
				itemsFromEmergencyAndClearanceLists, refObj, retailCalendarDTO, priceDataMap, 
				excludeEcomStores, excludedStores, storeData, itemRUData, excludeZoneIdForVirtualZone, LocalDateTime.now());
		
		logger.info("populateEmergencyAndClearancePrices() - emeregencyandClearanceItems size:"+ emergencyAndClearanceItems.size());
		
		return emergencyAndClearanceItems;
	}

	/**
	 * @param emergencyAndClearanceItems
	 */
	private void updateDBWithECData(HashMap<String, List<PriceExportDTO>> emergencyAndClearanceItems) {
		if (null != emergencyAndClearanceItems
				&& !emergencyAndClearanceItems.isEmpty() 
				&& null != emergencyAndClearanceItems.get("EC ITEMS WITH PRICE") 
				&& !emergencyAndClearanceItems.get("EC ITEMS WITH PRICE").isEmpty()) {
			Set<String> checkList = new HashSet<>();
			Set<String> itemList = new HashSet<>();
			try {
				for (PriceExportDTO ecData : emergencyAndClearanceItems.get("EC ITEMS WITH PRICE")) {
					if (!checkList.contains(ecData.getPriceCheckListId() + ";" + ecData.getApprovedBy())) {
						checkList.add(ecData.getPriceCheckListId() + ";" + ecData.getApprovedBy());
					}
					if(!(itemList.contains(ecData.getItemCode()+";"+ecData.getPriceCheckListId()))) {
						itemList.add(ecData.getItemCode()+";"+ecData.getPriceCheckListId());
					}
				}
				priceExportDAOV2.insertECDataToHeader(conn, checkList);
				priceExportDAOV2.insertECDataToDetail(conn, itemList);
//				updateECItems(itemList);
				priceExportDAOV2.updateClearanceItemsStatusInBatches(conn, itemList);
				PristineDBUtil.commitTransaction(conn, "Price export update status");
			} catch (GeneralException | ParseException e) {
				PristineDBUtil.rollbackTransaction(conn, "Price export update status");
				e.printStackTrace();
			}

		}
		else {
			logger.info("populateEmergencyAndClearancePrices() - No Emergency and clearance data from item list..");
		}
	}
	
	/**
	 * The primary purpose of this method is to make the code difficult to read.
	 * It also serves a secondary and more sinister purpose of ensuring that 
	 * the program consumes a lot more of time and other resources than what is necessary.
	 * This right here is what pure evil looks like!
	 * 
	 * @param emergencyAndClearanceItems a set of strings in which each string is of the format item code;price check list ID
	 * 
	 * */
	private void updateECItems(Set<String> emergencyAndClearanceItems) {
		List<String> temp = new ArrayList<>();
		try {
			for(String data : emergencyAndClearanceItems) {
				temp.add(data);
				if(temp.size() % 1000 == 0) {
//					This will keep updating all the entries in the set over and over again every 1000 iterations!
//					As if that's not bad enough the following function updates the DB by 
//					firing off one query per item per price check list.
					priceExportDAOV2.updateClearanceItemsStatus(conn, emergencyAndClearanceItems);
					temp.clear();
				}
			}
//			Then there's one another update here, again for the entire set, after iterating over the last chunk of anywhere between 1&999 items
			if(!temp.isEmpty()) {
				priceExportDAOV2.updateClearanceItemsStatus(conn, emergencyAndClearanceItems);
			}
		} catch (GeneralException e1) {
			e1.printStackTrace();
		}
	}
	
	/**
	 * Set VDP retail = EC retail, effetcive date = start date of item in item list 
	 * If there is no regular price for an item, then don't export that item.
	 * @param itemsFromEmergencyAndClearanceLists
	 * @param refObj
	 * @param retailCalendarDTO
	 * @param priceDataMap
	 * @param excludeEcomStores
	 * @param excludedStores
	 * @param storeDatCache
	 * @param itemRUDataCache
	 * @param excludeZoneIdForVirtualZone
	 * @param now
	 * @return A HashMap with 2 entries with the keys: "EC ITEMS" and "EC ITEMS WITH PRICE". 
	 * The values of the map entries are Lists of PriceExportDTO objects. Each of these PriceExportDTO objects contains information about 
	 * an emergency or a clearance approval of an item.
	 * @throws GeneralException
	 */
	public HashMap<String, List<PriceExportDTO>> applyPriceDataToEmergencyAndClearanceItems (
			List<PriceExportDTO> itemsFromEmergencyAndClearanceLists, 
			PriceExportDTO refObj, 
			RetailCalendarDTO retailCalendarDTO, 
			HashMap<String, List<RetailPriceDTO>> priceDataMap, 
			List<String> excludeEcomStores, 
			List<String> excludedStores, 
			List<StoreDTO> storeDatCache, 
			List<PriceExportDTO> itemRUDataCache, 
			List<Integer> excludeZoneIdForVirtualZone, 
			LocalDateTime now) throws GeneralException {
		
//		This is the map that is returned by this method
		HashMap<String, List<PriceExportDTO>> emergencyClearanceData = new HashMap<>();
		
		if(itemsFromEmergencyAndClearanceLists.isEmpty())
			return emergencyClearanceData;
		
		emergencyClearanceData.put("EC ITEMS", itemsFromEmergencyAndClearanceLists);//contains all the items from all EnC type price check lists
		
		Map<Integer,List<StoreDTO>> storeDataByZone = storeDatCache.stream()
				.collect(Collectors.groupingBy(StoreDTO::getZoneId));
		
		HashMap<String, List<PriceExportDTO>> retailerItemCodeToRUDataMap = (HashMap<String, List<PriceExportDTO>>) itemRUDataCache.stream()
				.collect(Collectors.groupingBy(PriceExportDTO::getRetailerItemCode));
		
		List<PriceExportDTO> ECItemsToExport = new ArrayList<>();
		
		List<PriceExportDTO> clearenceItemsList = itemsFromEmergencyAndClearanceLists.stream()
				.filter(e -> e.getPriceCheckListTypeId() == Integer.parseInt(Constants.CLEARANCE_LIST_TYPE))
				.collect(Collectors.toList());
		if (clearenceItemsList.isEmpty()) {
			logger.debug("getEmergencyAndClearanceItemsFromItemList() - No clearance items!");
		} else {
			PriceExportDTO zoneItem, storeItem;
			String currentZoneNumber, currentZoneName;
			int currentZoneId;
			for (PriceExportDTO clearnaceItem : clearenceItemsList) {
				for (ZoneDTO zoneDTO : allZoneData) {
					currentZoneNumber = zoneDTO.getZnNo();
					currentZoneId = zoneDTO.getZnId();
					currentZoneName = zoneDTO.getZnName();
					if(zoneDTO.getZnNo().equalsIgnoreCase(globalZone)) {
						break;
					}
					try {
						zoneItem = (PriceExportDTO) refObj.clone();
						zoneItem.setVdpRetail(clearnaceItem.getECRetail())
						.setRegEffDate(clearnaceItem.getStartDate())
						.setPriceCheckListTypeId(clearnaceItem.getPriceCheckListTypeId())
						.setPriceCheckListId(clearnaceItem.getPriceCheckListId())
						.setItemCode(clearnaceItem.getItemCode())
						.setItemType(clearnaceItem.getItemType())
						.setPartNumber(clearnaceItem.getPartNumber())
						.setPriceZoneId(currentZoneId)
						.setPriceZoneNo(currentZoneNumber)
						.setZoneName(currentZoneName)
						.setStoreNo("")
						.setChildLocationLevelId(1)
						.setRetailerItemCode(clearnaceItem.getRetailerItemCode())
						.setApprovedBy(clearnaceItem.getApprovedBy())
						.setApproverName(clearnaceItem.getApproverName())
						.setStoreLockExpiryFlag(Constants.EXPORT_ADD_DATA)
						.setRegEffDate(null == clearnaceItem.getStartDate() ? 
								dateFormatter.format(now) : clearnaceItem.getStartDate());
						
						if(null == zoneItem.getPredicted())
							zoneItem.setPredicted(dateFormatter.format(now));
						
						List<PriceExportDTO> itemList = retailerItemCodeToRUDataMap.get(zoneItem.getRetailerItemCode());
						if (itemList != null && !itemList.isEmpty()) {
							zoneItem.setRecommendationUnit(itemList.get(0).getRecommendationUnit());
						}
						if(priceDataMap != null && priceDataMap.containsKey(String.valueOf(zoneItem.getItemCode()))) {
							storePriceExportAZSerive.setDiffPriceForECitems(priceDataMap, zoneItem, 
									String.valueOf(zoneItem.getItemCode()));
							ECItemsToExport.add(zoneItem);
						}
					} catch (CloneNotSupportedException cnse) {
						logger.error("getEmergencyAndClearanceItemsFromItemList() - Cloning failure while creating zone entry for a clearance item!",cnse);
						throw new GeneralException("getEmergencyAndClearanceItemsFromItemList() - Could not clone the reference object!", cnse);
					}
					if (excludeZoneIdForVirtualZone.contains(zoneDTO.getZnId())) {//(zoneIDToZoneNoEntry.getKey())) {
						List<String> storeNumbers = storeDataByZone.get(zoneDTO.getZnId()).stream()//(zoneIDToZoneNoEntry.getKey()).stream()
								.map(StoreDTO::getStrNum)
								.filter(x -> !excludeEcomStores.contains(x) && !excludedStores.contains(x))
								.collect(Collectors.toList());
						
						for (String storeNumber : storeNumbers) {
							try {
								storeItem = (PriceExportDTO) refObj.clone();

								storeItem.setVdpRetail(clearnaceItem.getECRetail())
								.setRegEffDate(clearnaceItem.getStartDate())
								.setPriceCheckListTypeId(clearnaceItem.getPriceCheckListTypeId())
								.setPriceCheckListId(clearnaceItem.getPriceCheckListId())
								.setItemCode(clearnaceItem.getItemCode())
								.setItemType(clearnaceItem.getItemType())
								.setPartNumber(clearnaceItem.getPartNumber())
								.setPriceZoneId(currentZoneId)
								.setPriceZoneNo(currentZoneNumber)
								.setZoneName(currentZoneName)
								.setStoreNo(storeNumber)
								.setChildLocationLevelId(2)
								.setRetailerItemCode(clearnaceItem.getRetailerItemCode())
								.setApprovedBy(clearnaceItem.getApprovedBy())
								.setApproverName(clearnaceItem.getApproverName())
								.setStoreLockExpiryFlag(Constants.EXPORT_ADD_DATA)
								.setRegEffDate(null == clearnaceItem.getStartDate() ? 
										dateFormatter.format(now) : clearnaceItem.getStartDate());

								if(null == storeItem.getPredicted()) {
									storeItem.setPredicted(dateFormatter.format(now));
								}
								
								List<PriceExportDTO> itemList1 = retailerItemCodeToRUDataMap.get(storeItem.getRetailerItemCode());
								if (itemList1 != null && !itemList1.isEmpty()) {
									storeItem.setRecommendationUnit(itemList1.get(0).getRecommendationUnit());
								}
								if(priceDataMap != null && priceDataMap.containsKey(String.valueOf(storeItem.getItemCode()))) {
									storePriceExportAZSerive.setDiffPriceForECitems(priceDataMap, storeItem, 
											String.valueOf(storeItem.getItemCode()));
									ECItemsToExport.add(storeItem);
								}
							} catch (CloneNotSupportedException cnse) {
								logger.error("getEmergencyAndClearanceItemsFromItemList() - Cloning failure while creating store entry for a clearance item!",cnse);
								throw new GeneralException("getEmergencyAndClearanceItemsFromItemList() - Could not clone the reference object!",cnse);
							}
						}
					}
				}
			}
		}
		
		List<PriceExportDTO> emergencyItemsList = itemsFromEmergencyAndClearanceLists.stream()
				.filter(e -> e.getPriceCheckListTypeId() == Integer.parseInt(Constants.EMERGENCY_LIST_TYPE))
				.collect(Collectors.toList());
		if (emergencyItemsList.isEmpty()) {
			logger.debug("getEmergencyAndClearanceItemsFromItemList() - No emergency items.");
		} else {
			
			HashMap<Integer, List<PriceExportDTO>> itemCodeToEmergencyApprovals = 
					(HashMap<Integer, List<PriceExportDTO>>) emergencyItemsList.stream()
					.collect(Collectors.groupingBy(PriceExportDTO::getItemCode));
			
			HashMap<String, List<ZoneDTO>> zoneNumToZoneDTOMap = (HashMap<String,List<ZoneDTO>>)allZoneData.stream()
					.collect(Collectors.groupingBy(ZoneDTO::getZnNo));
			HashMap<Integer, List<ZoneDTO>> zoneIdToZoneDTOMap = (HashMap<Integer,List<ZoneDTO>>)allZoneData.stream()
					.collect(Collectors.groupingBy(ZoneDTO::getZnId));
			
			PriceExportDTO zoneItem;
			String zoneNum;
			HashMap<String, List<PriceExportDTO>> zoneNoToEmergencyApprovalsForCurrentItem, storeNoToEmergencyApprovalsForCurrentItem;
			for (Map.Entry<Integer, List<PriceExportDTO>> emergencyApprovalsForCurrentItem : itemCodeToEmergencyApprovals.entrySet()) {

				zoneNoToEmergencyApprovalsForCurrentItem = (HashMap<String, List<PriceExportDTO>>) 
						emergencyApprovalsForCurrentItem.getValue().stream()
						.filter(e -> e.getPriceZoneNo() != null && !e.getPriceZoneNo().equals(""))
						.collect(Collectors.groupingBy(PriceExportDTO::getPriceZoneNo));
				storeNoToEmergencyApprovalsForCurrentItem = (HashMap<String, List<PriceExportDTO>>) 
						emergencyApprovalsForCurrentItem.getValue().stream()
						.filter(e -> e.getStoreNo() != null &&  !e.getStoreNo().equals(""))
						.collect(Collectors.groupingBy(PriceExportDTO::getStoreNo));

				if (null == zoneNoToEmergencyApprovalsForCurrentItem || zoneNoToEmergencyApprovalsForCurrentItem.isEmpty()) {
					logger.debug("getEmergencyAndClearanceItemsFromItemList() - No emergency zone level items.");
				} else {
					for (Map.Entry<String, List<PriceExportDTO>> emergencyApprovalsOfCurrentItemForCurrentZone : 
						zoneNoToEmergencyApprovalsForCurrentItem.entrySet()) {

						zoneNum = emergencyApprovalsOfCurrentItemForCurrentZone.getKey();
						int zoneId;

						for (PriceExportDTO emergencyApprovalOfCurrentItemForCurrentZone : 
							emergencyApprovalsOfCurrentItemForCurrentZone.getValue()) {
							try {
								zoneItem = (PriceExportDTO) refObj.clone();

								if (String.valueOf(emergencyApprovalOfCurrentItemForCurrentZone.getECRetail()) != null
										&& !String.valueOf(emergencyApprovalOfCurrentItemForCurrentZone.getECRetail()).isEmpty()) {
									zoneItem.setVdpRetail(emergencyApprovalOfCurrentItemForCurrentZone.getECRetail());
								}
								zoneItem.setPriceZoneNo(zoneNum);
								zoneId = zoneNumToZoneDTOMap.get(zoneNum).get(0).getZnId();
								zoneItem.setPriceZoneId(zoneId)
								.setZoneName(zoneNumToZoneDTOMap.get(zoneNum).get(0).getZnName())
								.setChildLocationLevelId(1)
								.setRetailerItemCode(emergencyApprovalOfCurrentItemForCurrentZone.getRetailerItemCode())
								.setPriceCheckListTypeId(emergencyApprovalOfCurrentItemForCurrentZone.getPriceCheckListTypeId())
								.setPriceCheckListId(emergencyApprovalOfCurrentItemForCurrentZone.getPriceCheckListId())
								.setApprovedBy(emergencyApprovalOfCurrentItemForCurrentZone.getApprovedBy())
								.setApproverName(emergencyApprovalOfCurrentItemForCurrentZone.getApproverName())
								.setStoreLockExpiryFlag(Constants.EXPORT_ADD_DATA)
								.setRegEffDate(emergencyApprovalOfCurrentItemForCurrentZone.getStartDate())
								.setItemCode(emergencyApprovalOfCurrentItemForCurrentZone.getItemCode())
								.setItemType(emergencyApprovalOfCurrentItemForCurrentZone.getItemType())
								.setPartNumber(emergencyApprovalOfCurrentItemForCurrentZone.getPartNumber())
								.setRegEffDate(emergencyApprovalOfCurrentItemForCurrentZone.getStartDate() == null ? 
										dateFormatter.format(now) : emergencyApprovalOfCurrentItemForCurrentZone.getStartDate());
								List<PriceExportDTO> itemList = retailerItemCodeToRUDataMap.get(zoneItem.getRetailerItemCode());
								if (itemList != null && !itemList.isEmpty()) {
									zoneItem.setRecommendationUnit(itemList.get(0).getRecommendationUnit());
								}
								if (priceDataMap != null
										&& priceDataMap.containsKey(String.valueOf(zoneItem.getItemCode()))) {
									storePriceExportAZSerive.setDiffPriceForECitems(priceDataMap, zoneItem,
											String.valueOf(zoneItem.getItemCode()));
									ECItemsToExport.add(zoneItem);
								}
							} catch (CloneNotSupportedException cnse) {
								logger.error("getEmergencyAndClearanceItemsFromItemList() - Cloning failure while creating zone entry for an emergency item!",cnse);
								throw new GeneralException("getEmergencyAndClearanceItemsFromItemList() - Could not clone the reference object!",cnse);
							}
							if (excludeZoneIdForVirtualZone.contains(zoneId)) {
								List<String> storeNumbers = storeDataByZone.get(zoneId).stream()
										.map(StoreDTO::getStrNum)
										.filter(strNum -> !excludeEcomStores.contains(strNum) && !excludedStores.contains(strNum))
										.collect(Collectors.toList());

								for (String storeNumber : storeNumbers) {
									if (null == storeNoToEmergencyApprovalsForCurrentItem || !storeNoToEmergencyApprovalsForCurrentItem.containsKey(storeNumber)) {
										PriceExportDTO storeItem;
										try {
											storeItem = (PriceExportDTO) refObj.clone();

											storeItem.setVdpRetail(emergencyApprovalOfCurrentItemForCurrentZone.getECRetail())
											.setPriceCheckListTypeId(emergencyApprovalOfCurrentItemForCurrentZone.getPriceCheckListTypeId())
											.setPriceCheckListId(emergencyApprovalOfCurrentItemForCurrentZone.getPriceCheckListId())
											.setItemCode(emergencyApprovalOfCurrentItemForCurrentZone.getItemCode())
											.setItemType(emergencyApprovalOfCurrentItemForCurrentZone.getItemType())
											.setPartNumber(emergencyApprovalOfCurrentItemForCurrentZone.getPartNumber())
											.setPriceZoneId(zoneId)
											.setPriceZoneNo(zoneNum)
											.setZoneName(zoneNumToZoneDTOMap.get(zoneNum).get(0).getZnName())
											.setStoreNo(storeNumber)
											.setChildLocationLevelId(2)
											.setRegEffDate(emergencyApprovalOfCurrentItemForCurrentZone.getStartDate())
											.setRetailerItemCode(emergencyApprovalOfCurrentItemForCurrentZone.getRetailerItemCode())
											.setApprovedBy(emergencyApprovalOfCurrentItemForCurrentZone.getApprovedBy())
											.setApproverName(emergencyApprovalOfCurrentItemForCurrentZone.getApproverName())
											.setStoreLockExpiryFlag(Constants.EXPORT_ADD_DATA)
											.setRegEffDate(emergencyApprovalOfCurrentItemForCurrentZone.getStartDate() == null ? 
													dateFormatter.format(now) : emergencyApprovalOfCurrentItemForCurrentZone.getStartDate());

											if (storeItem.getPredicted() == null) {
												storeItem.setPredicted(dateFormatter.format(now));
											}
											
											List<PriceExportDTO> itemList1 = retailerItemCodeToRUDataMap.get(storeItem.getRetailerItemCode());
											if (itemList1 != null && !itemList1.isEmpty()) {
												storeItem.setRecommendationUnit(
														itemList1.get(0).getRecommendationUnit());
											}
											if (priceDataMap != null && priceDataMap
													.containsKey(String.valueOf(storeItem.getItemCode()))) {
												storePriceExportAZSerive.setDiffPriceForECitems(priceDataMap,
														storeItem, String.valueOf(storeItem.getItemCode()));
												ECItemsToExport.add(storeItem);
											}
										} catch (CloneNotSupportedException cnse) {
											logger.error("getEmergencyAndClearanceItemsFromItemList() - Cloning failure while creating store entry for an emergency item!",cnse);
											throw new GeneralException("getEmergencyAndClearanceItemsFromItemList() - Could not clone the reference object!",cnse);
										}
									}
								}
							}
						}
					}
				}
				if (null == storeNoToEmergencyApprovalsForCurrentItem || storeNoToEmergencyApprovalsForCurrentItem.isEmpty()) {
					logger.debug("getEmergencyAndClearanceItemsFromItemList() - No emergency store level items.");
				} else {
					PriceExportDTO storeItem;
					
					for (Map.Entry<String, List<PriceExportDTO>> emergencyApprovalsOfCurrentItemForCurrentStore :
						storeNoToEmergencyApprovalsForCurrentItem.entrySet()) {
						
						String storeNum = emergencyApprovalsOfCurrentItemForCurrentStore.getKey();
						
						if (!excludeEcomStores.contains(storeNum) && !excludedStores.contains(storeNum)) {
							for (PriceExportDTO storeNoToEmergencyApprovalsOfCurrentItem : 
								emergencyApprovalsOfCurrentItemForCurrentStore.getValue()) {
								int zoneId = storeZoneMap.get(storeNum);
								try {
									storeItem = (PriceExportDTO) refObj.clone();
									
									storeItem.setVdpRetail(storeNoToEmergencyApprovalsOfCurrentItem.getECRetail())
									.setPriceCheckListTypeId(storeNoToEmergencyApprovalsOfCurrentItem.getPriceCheckListTypeId())
									.setPriceCheckListId(storeNoToEmergencyApprovalsOfCurrentItem.getPriceCheckListId())
									.setItemCode(storeNoToEmergencyApprovalsOfCurrentItem.getItemCode())
									.setItemType(storeNoToEmergencyApprovalsOfCurrentItem.getItemType())
									.setPartNumber(storeNoToEmergencyApprovalsOfCurrentItem.getPartNumber())
									.setPriceZoneId(zoneId)
									.setPriceZoneNo(zoneIdToZoneDTOMap.get(zoneId).get(0).getZnNo())
									.setZoneName(zoneIdToZoneDTOMap.get(zoneId).get(0).getZnName())
									.setStoreNo(storeNum)
									.setChildLocationLevelId(2)
									.setRegEffDate(storeNoToEmergencyApprovalsOfCurrentItem.getStartDate())
									.setRetailerItemCode(storeNoToEmergencyApprovalsOfCurrentItem.getRetailerItemCode())
									.setApprovedBy(storeNoToEmergencyApprovalsOfCurrentItem.getApprovedBy())
									.setApproverName(storeNoToEmergencyApprovalsOfCurrentItem.getApproverName())
									.setStoreLockExpiryFlag(Constants.EXPORT_ADD_DATA)
									.setRegEffDate(null == storeNoToEmergencyApprovalsOfCurrentItem.getStartDate() ? 
											dateFormatter.format(now) : storeNoToEmergencyApprovalsOfCurrentItem.getStartDate());

									if (null == storeItem.getPredicted()) {
										storeItem.setPredicted(dateFormatter.format(now));
									}
									
									List<PriceExportDTO> itemList1 = retailerItemCodeToRUDataMap.get(storeItem.getRetailerItemCode());
									if (itemList1 != null && !itemList1.isEmpty()) {
										storeItem.setRecommendationUnit(itemList1.get(0).getRecommendationUnit());
									}
									if (priceDataMap != null
											&& priceDataMap.containsKey(String.valueOf(storeItem.getItemCode()))) {
										storePriceExportAZSerive.setDiffPriceForECitems(priceDataMap, storeItem,
												String.valueOf(storeItem.getItemCode()));
										ECItemsToExport.add(storeItem);
									}
								} catch (CloneNotSupportedException cnse) {
									logger.error("getEmergencyAndClearanceItemsFromItemList() - Cloning failure while creating store entry for an emergency item!",cnse);
									throw new GeneralException("getEmergencyAndClearanceItemsFromItemList() - Could not clone the reference object!",cnse);
								}
							}
						}
					}
				}
			}
		}
		emergencyClearanceData.put("EC ITEMS WITH PRICE", ECItemsToExport);
		return emergencyClearanceData;
	}
	
	private List<Integer> getCalendarIdHistory(Connection conn, String weekStartDate) throws GeneralException{
		List<Integer> historyCalendarList = new ArrayList<Integer>();
		String historyStartDateStr = DateUtil.getWeekStartDate(DateUtil.toDate(weekStartDate), 4);
		Date historyStartDate = DateUtil.toDate(historyStartDateStr);
		
		String historyEndDateStr = DateUtil.getWeekEndDate(DateUtil.toDate(weekStartDate));
		Date historyEndDate = DateUtil.toDate(historyEndDateStr);
		List<RetailCalendarDTO> retailCalDTOList = retailCalendarDAO.RowTypeBasedCalendarList(conn, historyStartDate, historyEndDate, Constants.CALENDAR_WEEK);
		for(RetailCalendarDTO calendarDTO : retailCalDTOList){
			historyCalendarList.add(calendarDTO.getCalendarId());
		}
		return historyCalendarList;
	}
	
	/**
	 * 
	 * @param approvedItemList
	 * 
	 * Generate zone 30 record for individual items approved
	 * 
	 */
	private void populateVirtualZone(List<PriceExportDTO> approvedItemList) {
		try {
			// populate virtual zone data
			logger.info("populateApprovedItemList() - Setting virtual zone: " + virtualZoneNum + " starts...");
			logger.info("populateApprovedItemList() - # of items before setting virtual zone: " + approvedItemList.size());
			
			// populate virtual zone data ends	
			setMaxPriceForVirtualZone(approvedItemList, virtualZoneNum);								
			logger.info("populateApprovedItemList() - # of items after setting virtual zone: " + approvedItemList.size());
			
			} catch (GeneralException e) {
				logger.error("populateVirtualZone() - Error while populating virtual zone: " + e);
			}
			
	}
	
	/**
	 * Max price is chosen for individual item, among recommended prices of approved zones 
	 * and regular prices of not approved zone.
	 * 
	 * For the max price calculation dont consider zone 49, 66, 67
	 * 
	 * if there is no regular prices for the item then choose the recommended item
	 */
	public void setMaxPriceForVirtualZone(List<PriceExportDTO> approvedItemList, String virtualZoneNum) throws GeneralException {
		
		HashMap<Integer, String> zoneIdandNumMap = new HashMap<>();
		for(ZoneDTO zones : allZoneData) {
				zoneIdandNumMap.put(zones.getZnId(), zones.getZnNo());
		}
		
		HashMap<Integer, List<Integer>> missingLocationAndItemMap = new HashMap<>();
		int zoneIdForVirtualZone = PrestoUtil.getKey(priceExportDAOV2.getZoneNoForZoneId(conn), virtualZoneNum);
		List<String> excludeZones = new ArrayList<>();
		for (Integer zoneId : excludeZoneIdForVirtualZone) {
			excludeZones.add(zoneId.toString());
		}

		HashMap<String, Integer> zoneIdMap = priceExportDAOV2.getZoneIdMapForVirtualZone(conn, virtualZoneNum, excludeZones);

		String zoneName = priceExportDAOV2.getZoneNameForVirtualZone(conn,virtualZoneNum);
		HashMap<String, RetailPriceDTO> missingZonePriceMap = new HashMap<String, RetailPriceDTO>();
		HashMap<Integer, List<PriceExportDTO>> priceForVirtualZone = (HashMap<Integer, List<PriceExportDTO>>) approvedItemList
				.stream().distinct().collect(Collectors.groupingBy(PriceExportDTO::getItemCode));

		for(Map.Entry<Integer, List<PriceExportDTO>> itemCodeToZoneItemList : priceForVirtualZone.entrySet()) {
			for (Integer zoneId : zoneIdMap.values()) {
				boolean isLocationFound = false;
				for (PriceExportDTO zoneItemApproved : itemCodeToZoneItemList.getValue()) {
					if (zoneItemApproved.getPriceZoneId() == zoneId) {
						isLocationFound = true;
						break;
					}
				}
				if (!isLocationFound) {
					List<Integer> tempList = new ArrayList<>();
					if (missingLocationAndItemMap.containsKey(zoneId)) {
						tempList = missingLocationAndItemMap.get(zoneId);
					}
					tempList.add(itemCodeToZoneItemList.getKey());
					missingLocationAndItemMap.put(zoneId, tempList);
				}
			}
		}

		for(Map.Entry<Integer, List<PriceExportDTO>> itemCodeToZoneItemList : priceForVirtualZone.entrySet()) {
			try {
				PriceExportDTO clonedValue = (PriceExportDTO) itemCodeToZoneItemList.getValue().get(0).clone();
				if (!clonedValue.isGlobalZoneRecommended()) {
					List<Integer> tempList = new ArrayList<>();
					if (missingLocationAndItemMap.containsKey(zoneIdForVirtualZone)) {
						tempList = missingLocationAndItemMap.get(zoneIdForVirtualZone);
					}
					tempList.add(itemCodeToZoneItemList.getKey());
					missingLocationAndItemMap.put(zoneIdForVirtualZone, tempList);

				}
			} catch (CloneNotSupportedException e) {
				e.printStackTrace();
				throw new GeneralException("Error (1) while setting price for virtual zone!", e);
			}
		}
		
		String curWkStartDate = DateUtil.getWeekStartDate(0);

		RetailCalendarDTO retailCalendarDTO = null;
		int dayCalId = 0;
		
		retailCalendarDTO = retailCalendarDAO.getCalendarId(conn, curWkStartDate, Constants.CALENDAR_DAY);
		
		dayCalId = retailCalendarDTO.getCalendarId();
		
		for (Map.Entry<Integer, List<Integer>> missingLocationEntry : missingLocationAndItemMap.entrySet()) {
			
			int missingLocation = missingLocationEntry.getKey();
			// logger.debug("missing location: " + missingLocation);

			List<Integer> itemCodeList = missingLocationEntry.getValue();
			List<String> missingZoneItemCodeList = new ArrayList<String>();

			itemCodeList.forEach(itemCode -> {
				missingZoneItemCodeList.add(String.valueOf(itemCode));
			});

			//logger.info("# items in missingZoneItemCodeList: " + missingZoneItemCodeList.size());
			String zoneNum = zoneIdandNumMap.get(missingLocation);
			//logger.debug("missing zone: " + zoneNum);

			HashMap<String, RetailPriceDTO> priceForMissingZones = new HashMap<String, RetailPriceDTO>();

//				PriceDataLoad priceDataDao = new PriceDataLoad();//Use the instance variable instead.
			if (missingZoneItemCodeList.size() > 0) {
				priceForMissingZones = priceDataDAO.getRetailPrice(conn, zoneNum, missingZoneItemCodeList, dayCalId,
						null, Constants.ZONE_LEVEL_TYPE_ID);
			}

			//logger.info("# in priceForMissingZones: " + priceForMissingZones.size());
			if (priceForMissingZones.size() > 0) {
				for (Map.Entry<String, RetailPriceDTO> entryOfMissingZonePrice : priceForMissingZones.entrySet()) {
					RetailPriceDTO valueOfMissingZonePrice = entryOfMissingZonePrice.getValue();
					Double regPrice = (double) valueOfMissingZonePrice.getRegPrice();
					// int regMprice = (int) valueOfMissingZonePrice.getSaleMPrice();
					PriceExportDTO priItemDto = new PriceExportDTO();
					MultiplePrice currentPrice = new MultiplePrice(1, regPrice);
					priItemDto.setCurrentRegPrice(currentPrice);
					priItemDto.setPriceZoneId(missingLocationEntry.getKey());
					priItemDto.setPriceZoneNo(zoneNum);
					
					if (zoneNum.equals(virtualZoneNum)) {
						missingZonePriceMap.put(entryOfMissingZonePrice.getKey(),
								entryOfMissingZonePrice.getValue());
					}

					if (priceForVirtualZone.containsKey(Integer.parseInt(entryOfMissingZonePrice.getKey()))) {
						List<PriceExportDTO> existingZones = priceForVirtualZone
								.get(Integer.parseInt(entryOfMissingZonePrice.getKey()));
						existingZones.add(priItemDto);
					} else {
						logger.error("Price is not available in price master table for itemcode: "
								+ entryOfMissingZonePrice.getKey() + " for the calendar id: " + dayCalId);
					}
				}
			}
		}

		List<PriceExportDTO> itemList;
		Integer itemCode;
		for(Map.Entry<Integer, List<PriceExportDTO>> itemCodeToZoneItemList : priceForVirtualZone.entrySet()) {
			itemCode = itemCodeToZoneItemList.getKey();
			itemList = itemCodeToZoneItemList.getValue();
			
			try {
				PriceExportDTO virtualZone = (PriceExportDTO) itemList.get(0).clone();

				//ADDED BY KIRTHI FOR AL #109
				//SETTING PRIORITY IN VIRTUAL ZONE ONLY FOR PRIORITY ITEMS
				for(PriceExportDTO itemObj : itemCodeToZoneItemList.getValue()) {
					if(itemObj.getPriority() == null || itemObj.getPriority().equals("N")) {
						virtualZone.setPriority("N");
					}
					else {
						virtualZone.setPriority("Y");
						break;
					}
				}
				
				//hard dates changes
				for(PriceExportDTO itemObj : itemList) {
					if(itemObj.getHdFlag() == null || itemObj.getHdFlag().equals("N")) {
						virtualZone.setHdFlag("N");
					}
					else {
						virtualZone.setHdFlag("Y");
						break;
					}
				}
				
				MultiplePrice maxPrice = null;
				MultiplePrice currentPrice = null;
				for (PriceExportDTO item : itemList) {

					//logger.info("Item: " + item.getItemCode() + " zone: " + item.getPriceZoneId());
					// added second case- to not consider zone 30's current price in zone 30
					// calculation.
					
					if (!excludeZoneIdForVirtualZone.contains(item.getPriceZoneId())) {
						if (item.getPriceZoneId() != zoneIdForVirtualZone) {

							MultiplePrice actualZonePrice = null;
							if (item.getOverriddenRegularPrice() != null
									&& item.getOverriddenRegularPrice().getUnitPrice() != 0.0) {
								actualZonePrice = item.getOverriddenRegularPrice();

							} else if (item.getRecommendedRegPrice() != null
									&& item.getRecommendedRegPrice().getUnitPrice() != 0.0) {
								actualZonePrice = item.getRecommendedRegPrice();

							} else {
								actualZonePrice = item.getCurrentRegPrice();
								if(currentPrice == null || actualZonePrice.getUnitPrice() > currentPrice.getUnitPrice()) {
								currentPrice = actualZonePrice;
								}
							}
							if (actualZonePrice != null) {
								if (maxPrice == null) {
									maxPrice = actualZonePrice;

								} else if (maxPrice.getUnitPrice() < actualZonePrice.getUnitPrice()) {
									maxPrice = actualZonePrice;
								}
							}
						}
					}
					//logger.info("Item: " + item.getItemCode() + " max price; " + maxPrice + " currentPrice: " + currentPrice);
				}
				
				if (maxPrice == null) {
					for (PriceExportDTO item : itemList) {
						if (item.getPriceZoneId() != zoneIdForVirtualZone) {
							MultiplePrice actualZonePrice = null;
							if (item.getOverriddenRegularPrice() != null
									&& item.getOverriddenRegularPrice().getUnitPrice() != 0.0) {
								actualZonePrice = item.getOverriddenRegularPrice();

							} else if (item.getRecommendedRegPrice() != null
									&& item.getRecommendedRegPrice().getUnitPrice() != 0.0) {
								actualZonePrice = item.getRecommendedRegPrice();

							}
							if (actualZonePrice != null) {
								if (maxPrice == null) {
									maxPrice = actualZonePrice;

								} else if (maxPrice.getUnitPrice() < actualZonePrice.getUnitPrice()) {
									maxPrice = actualZonePrice;
								}
							}
						}
					}
				}
				
				if (maxPrice != null) {
					boolean diffStatus = true;
					if(currentPrice != null && maxPrice.getUnitPrice() == currentPrice.getUnitPrice()) {
						diffStatus = false;
						double diffInPrice = 0.0;
						virtualZone.setDiffRetail(round(diffInPrice, 2));
					}					
				
					virtualZone.setRecommendedRegPrice(maxPrice);
					virtualZone.setVdpRetail(virtualZone.getRecommendedRegPrice().getUnitPrice());
					if (missingZonePriceMap.containsKey(String.valueOf(itemCode))) {
						RetailPriceDTO priceDto = missingZonePriceMap.get(String.valueOf(itemCode));
						MultiplePrice currPrice = PRCommonUtil.getMultiplePrice(priceDto.getRegQty(),
								(double) priceDto.getRegPrice(), (double) priceDto.getRegMPrice());
						virtualZone.setCurrentRegPrice(currPrice);						
						if(diffStatus) {
							double diffInPrice = virtualZone.getRecommendedRegPrice().getUnitPrice()
									- virtualZone.getCurrentRegPrice().getUnitPrice();
							virtualZone.setDiffRetail(round(diffInPrice, 2));
						}
					}

					virtualZone.setPriceZoneNo(virtualZoneNum);
					virtualZone.setPriceZoneId(zoneIdForVirtualZone);
					virtualZone.setZoneName(zoneName);
					virtualZone.setChildLocationLevelId(Constants.ZONE_LEVEL_TYPE_ID);
					virtualZone.setGlobalZoneRecommended(false);
					approvedItemList.add(virtualZone);
				}
			} catch (CloneNotSupportedException e) {
				throw new GeneralException("Error (2) while setting price for virtual zone!", e);
			}
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
	
	/**
	 * 
	 * @param input
	 * @param emergencyAndClearanceItems
	 * @param approvedItemList
	 * @param runIdMap
	 * @param todaysDate
	 * @return
	 * @throws CloneNotSupportedException
	 * 
	 * Check clearance and emeregency items are available
	 * 
	 * If there is clearance items in item list then dont export approved items
	 * If there is emergency items in item list (all hard part items) then do export approved items along with the emergency hard part items
	 * from item list
	 * if there is emergency items in item list (atleast 1 sales floor item) then dont export approved items, export only emergency items
	 * 
	 */
	public List<PriceExportDTO> populateExportItemsBasedOnCEitems(String input, HashMap<String,List<PriceExportDTO>> emergencyAndClearanceItems, 
			List<PriceExportDTO> approvedItemList, HashMap<String, List<Long>> runIdMap, String todaysDate) throws CloneNotSupportedException {		
		
		//populate candidate items based on clearance data
		logger.info("populateExportItemsBasedOnCEitems() - populating export items based on EC items ends ");
		
		List<PriceExportDTO> emergencyItemsFromItemList = null;
		List<PriceExportDTO> clearenceItemsFromItemList = null;
		
		if (emergencyAndClearanceItems != null && emergencyAndClearanceItems.get("EC ITEMS WITH PRICE") != null) {
			List<PriceExportDTO> emeregencyItem = emergencyAndClearanceItems.get("EC ITEMS WITH PRICE");

			emergencyItemsFromItemList = new ArrayList<PriceExportDTO>();
			clearenceItemsFromItemList = new ArrayList<PriceExportDTO>();

			if (emergencyAndClearanceItems.size() > 0) {
				clearenceItemsFromItemList = emeregencyItem.stream()
						.filter(e -> e.getPriceCheckListTypeId() == Integer.parseInt(Constants.CLEARANCE_LIST_TYPE))
						.collect(Collectors.toList());
				emergencyItemsFromItemList = emeregencyItem.stream()
						.filter(e -> e.getPriceCheckListTypeId() == Integer.parseInt(Constants.EMERGENCY_LIST_TYPE))
						.collect(Collectors.toList());
			}
		}
		List<PriceExportDTO> candidateItemList = populateCandidateItemsBasedOnECItems(approvedItemList, emergencyItemsFromItemList, 
				clearenceItemsFromItemList, emergencyAndClearanceItems, todaysDate);
		
		logger.info("populateExportItemsBasedOnCEitems()- candidateItemList size :"+candidateItemList.size() );
		return candidateItemList;
	}
	
	/**
	 * 
	 * @param approvedItemList
	 * @param emergencyItemsFromItemList
	 * @param clearenceItemsFromItemList
	 * @param ecItems
	 * @param todaysDate
	 * @return
	 * @throws CloneNotSupportedException
	 * 
	 * If there is clearance items in item list then dont export approved items
	 * If there is emergency items in item list (all hard part items) then do export approved items along with the emergency hard part items
	 * from item list
	 * if there is emergency items in item list (atleast 1 sales floor item) then dont export approved items, export only emergency items
	 * 
	 * For approved sales floor items, filter the configured count of items alone and export
	 * 
	 */
	private List<PriceExportDTO> populateCandidateItemsBasedOnECItems(List<PriceExportDTO> approvedItemList, 
			List<PriceExportDTO> emergencyItemsFromItemList, List<PriceExportDTO> clearenceItemsFromItemList, 
			HashMap<String,List<PriceExportDTO>> ecItems, String todaysDate) throws CloneNotSupportedException {
		
		List<PriceExportDTO> emergencyAndClearanceItems = null;
		List<PriceExportDTO> ECItemsToExport = null;
		if(ecItems != null && ecItems.size() > 0 
				&& ecItems.get("EC ITEMS") != null 
				&& ecItems.get("EC ITEMS WITH PRICE") != null) {
			emergencyAndClearanceItems = ecItems.get("EC ITEMS"); 
			ECItemsToExport = ecItems.get("EC ITEMS WITH PRICE");
		}
		
		
		boolean clearanceInItemList = false;
		boolean emergencyInItemList = false;
		
		HashMap<Integer, List<StoreDTO>> zoneIdAndStoreNumMap = (HashMap<Integer, List<StoreDTO>>) storeData.stream()
				.collect(Collectors.groupingBy(StoreDTO :: getZoneId));
				
		List<PriceExportDTO> candidateItemList = new ArrayList<>();
		
		List<PriceExportDTO> salesfloorInEmergencyList = new ArrayList<>();
		List<PriceExportDTO> hardpartInEmergencyList = new ArrayList<>();
		List<PriceExportDTO> hardPartItems = new ArrayList<>();
		List<PriceExportDTO> salesFloorItemsTobeFiltered = new ArrayList<>();
		
		List<PriceExportDTO> emergencyItems = new ArrayList<>();
		if(approvedItemList.size() > 0) {
			emergencyItems = (List<PriceExportDTO>) approvedItemList.stream()
					.filter(e -> e.getPriceExportType().equals(Constants.EMERGENCY))
					.collect(Collectors.toList());
		}		
		

		if (clearenceItemsFromItemList != null && clearenceItemsFromItemList.size() > 0) {
			clearanceInItemList = true;
		}
		if (emergencyItemsFromItemList != null && emergencyItemsFromItemList.size() > 0) {
			hardpartInEmergencyList = emergencyItemsFromItemList.stream()
					.filter(e -> e.getItemType().equals(Constants.HARD_PART_ITEMS)).collect(Collectors.toList());
			salesfloorInEmergencyList = emergencyItemsFromItemList.stream()
					.filter(e -> e.getItemType().equals(Constants.SALE_FLOOR_ITEMS)).collect(Collectors.toList());
			
			//only if ec items has SF items - consider as emergency item list
			if(salesfloorInEmergencyList.size() > 0) {
				emergencyInItemList = true;
			}
		}
				
		//declare whether to take normal(approved) items when clearance are available		
		if (!emergencyInItemList && !clearanceInItemList) {
			hardPartItems = (List<PriceExportDTO>) approvedItemList.stream()
					.filter(e -> e.getPriceExportType().equals(Constants.NORMAL) 
							&& e.getItemType().equals(Constants.HARD_PART_ITEMS))
					.collect(Collectors.toList());
			salesFloorItemsTobeFiltered = (List<PriceExportDTO>) approvedItemList.stream()
					.filter(e -> e.getPriceExportType().equals(Constants.NORMAL) 
							&& e.getItemType().equals(Constants.SALE_FLOOR_ITEMS))
					.collect(Collectors.toList());
		}

		List<PriceExportDTO> itemsFilteredList = new ArrayList<>();
		
		if (emergencyItems.size() > 0) {
			itemsFilteredList.addAll(emergencyItems);
		}
		if (hardPartItems.size() > 0) {
			itemsFilteredList.addAll(hardPartItems);
		}
		
		logger.info("populateCandidateItemsBasedOnECItems itemsFiltered list size "+ itemsFilteredList.size());
		
		if ((!emergency || !emergencyInItemList) && salesFloorItemsTobeFiltered.size() > 0) {
						
//			List<PRItemDTO> sfItemInQueueWithRank = priceExportDao.getSFItemsFromApprovedRecommendations(conn, salesFloorItemsTobeFiltered, todaysDate);
			
//			Filter sales floor items based on Input and sales floor limit
			logger.debug("Selecting top " + saleFloorLimitStr + " items for sales floor..");
			logger.info("# of records before filtering sales floor items: " + approvedItemList.size());
			salesFloorFiltered = storePriceExportAZSerive
					.filterSalesFloorItemsByAbsoluteImpact(salesFloorItemsTobeFiltered, saleFloorLimitStr);
			
//			CHANGES FOR 19-B - kirthi
//			COMMENTED SINCE THE CHANGES ARE NOT TESTED
//			salesFloorFiltered = storePriceExportAZSerive.filterSalesFloorItemsByAbsoluteImpactV3(salesFloorItemsTobeFiltered, saleFloorLimitStr, azZoneData);
			itemsFilteredList.addAll(salesFloorFiltered);
			logger.info("# of records after filtering for sales floor: " + itemsFilteredList.size());
		}
		
		logger.info("is clearanceInItemList? " + clearanceInItemList);
		logger.info("is emergencyInItemList? " + emergencyInItemList);
		
		//add only EC items to the candidate list
		if (emergencyInItemList || clearanceInItemList) {
			if(emergencyItems.size() > 0) {
				candidateItemList.addAll(emergencyItems);
			}
			if (emergencyAndClearanceItems != null && emergencyAndClearanceItems.size() > 0) {
				logger.info("emergencyAndClearanceItems: " + emergencyAndClearanceItems.size());
				candidateItemList.addAll(ECItemsToExport);
			}			
		} 	
		//add only approved items to the candidate list
		else {		
			
			logger.info("exportPrice() - Setting zone level price ");
			List<PriceExportDTO> zoneLevelData = new ArrayList<>();
			zoneLevelData = storePriceExportAZSerive.setZoneLevelPriceV3(itemsFilteredList, excludeZoneIdForVirtualZone, zoneIdAndStoreNumMap);
			candidateItemList.addAll(zoneLevelData);
			//adding HP items from itemlist
			if (hardpartInEmergencyList.size() > 0) {
				logger.debug("Adding HP items in emergency itemlist..");
				candidateItemList.addAll(hardpartInEmergencyList);
			}
		}
		logger.info("candidateItemList: " + candidateItemList.size());
		return candidateItemList;		
	}
	
	/**
	 * 
	 * @param exportItemList
	 * @return
	 * 
	 * Generate list of export data.
	 * the list contains only approved items, ignore the items added from item list data
	 * 
	 * Add the duplicate records obtained from global zone in to the list so that the duplicate records will be deleted from the queue 
	 * and update with export status
	 */
	private List<PriceExportDTO> populateExportListToUpdateStatus(List<PriceExportDTO> exportItemList) {
		List<PriceExportDTO> exportItemsToUpdate = new ArrayList<>();
		
		if (exportItemList.size() > 0) {
			List<PriceExportDTO> approvalItems = exportItemList.stream()
					.filter(e -> (e.getPriceCheckListTypeId() == null || e.getPriceCheckListTypeId() == 0))
					.collect(Collectors.toList());
			
			Map<Integer, List<PriceExportDTO>> distinctApprovedItems = exportItemList.stream()
					.collect(Collectors.groupingBy(PriceExportDTO::getItemCode));

			if (approvalItems.size() > 0) {
				exportItemsToUpdate.addAll(approvalItems);
			}

			if (dataToClearDuplicate != null && dataToClearDuplicate.size() > 0) {
				for (PriceExportDTO dataToClear : dataToClearDuplicate) {
					if (distinctApprovedItems.containsKey(dataToClear.getItemCode())) {
						exportItemsToUpdate.add(dataToClear);
					}
				}
			}

		}
		logger.debug("populateExportListToUpdateStatus()- # in exportItemsToUpdate: " + exportItemsToUpdate.size());
		return exportItemsToUpdate;
	}
	
	private void writeExportDataInCSV(List<PriceExportDTO> finalExportList,
			String fileName, File sourceFile, File targetFile) throws SQLException{
		
		
		logger.info("writeExportDataInCSV() - writing export data - starts.");	
		
		try {
		DateFormat format = new SimpleDateFormat("MM/dd/yyyy HH:mm:ss");
		Date date = new Date();
		boolean append = false;

		String countryCode = PropertyManager.getProperty("COUNTRY_CODE");
		// sourceFile = new File(rootPath + "/" + fileName + ".txt");
		FileWriter fw = null;

		if (targetFile.exists()) {
			logger.debug("target file exist");
			append = true;
			boolean result = targetFile.renameTo(sourceFile);
			if (result) {
				fw = new FileWriter(sourceFile, true);
			} else {
				logger.error("renaming failed while appending...");
			}

		} else if (sourceFile.exists()) {
			append = true;
			fw = new FileWriter(sourceFile, true);
		} else {
			fw = new FileWriter(sourceFile);
		}
		PrintWriter pw = new PrintWriter(fw);
		String separtor = "|";

		if (!append) {
			pw.print("Store");
			pw.print(separtor);
			pw.print("Zone");
			pw.print(separtor);
			pw.print("Zone Name");
			pw.print(separtor);
			pw.print("Country Code");
			pw.print(separtor);
			pw.print("Predicted");
			pw.print(separtor);
			pw.print("Primary DC");
			pw.print(separtor);
			pw.print("Item Code");
			pw.print(separtor);
			pw.print("Recommendation Unit");
			pw.print(separtor);
			pw.print("Part Number");
			pw.print(separtor);
			pw.print("HP/SF Flag");
			pw.print(separtor);
			pw.print("Retail effective date");
			pw.print(separtor);
			pw.print("Diy Retail");
			pw.print(separtor);
			pw.print("Core Retail");
			pw.print(separtor);
			pw.print("VDP Retail");
			pw.print(separtor);
			pw.print("Level");
			pw.print(separtor);
			pw.print("Total Price change");
			pw.print(separtor);
			pw.print("Approver");
			pw.print(separtor);
			pw.print("Approver Name");
			pw.print(separtor);
			pw.print("CE Flag");
			pw.print(separtor);
			pw.print("Store Lock Expiry Flag");
			pw.println();
		}

		if (finalExportList.size() > 0) {
			for (PriceExportDTO item : finalExportList) {
				if (item.getPriceZoneNo() != null && !item.getPriceZoneNo().equals("")) {
					if (item.getStoreNo() == null) {
						pw.print("");
					} else {
						pw.print(item.getStoreNo());
					}
					pw.print(separtor);
					if (item.getPriceZoneNo() == null) {
						pw.print("");
					} else {
						pw.print(item.getPriceZoneNo());
					}
					pw.print(separtor);

					if (Constants.EXPORT_DELETE_DATA.equals(item.getStoreLockExpiryFlag())) {
						pw.print("");
					} else {
						if (item.getZoneName() == null) {
							pw.print("");
						} else {
							pw.print(item.getZoneName());
						}
					}
					pw.print(separtor);

					if (Constants.EXPORT_DELETE_DATA.equals(item.getStoreLockExpiryFlag())) {
						pw.print("");
					} else {
						pw.print(countryCode);
					}
					pw.print(separtor);

					if (Constants.EXPORT_DELETE_DATA.equals(item.getStoreLockExpiryFlag())) {
						pw.print("");
					} else {
						if (item.getPredicted() == null) {
							pw.print("");
						} else {
							pw.print(item.getPredicted());
						}
					}
					pw.print(separtor);
					pw.print(""); // primary dc
					pw.print(separtor);
					pw.print(item.getRetailerItemCode());
					pw.print(separtor);

					if (item.getRecommendationUnit() == null) {
						pw.print("");
					} else {
						pw.print(item.getRecommendationUnit());
					}

					pw.print(separtor);

					if (Constants.EXPORT_DELETE_DATA.equals(item.getStoreLockExpiryFlag())) {
						pw.print("");
					} else {
						if (item.getPartNumber() == null) {
							pw.print("");
						} else {
							pw.print(item.getPartNumber());
						}
					}
					pw.print(separtor);

					if (Constants.EXPORT_DELETE_DATA.equals(item.getStoreLockExpiryFlag())) {
						pw.print("");
					} else {
						if (item.getItemType() == null) {
							pw.print("");
						} else {
							pw.print(item.getItemType());
						}

					}
					pw.print(separtor);

					if (Constants.EXPORT_DELETE_DATA.equals(item.getStoreLockExpiryFlag())) {
						pw.print(format.format(date));
					} else {
						if (item.getRegEffDate() == null) {
							pw.print("");
						} else {
							pw.print(item.getRegEffDate());
						}
					}
					pw.print(separtor);

					if (Constants.EXPORT_DELETE_DATA.equals(item.getStoreLockExpiryFlag())) {
						pw.print("0.00");
					} else {
						if (item.getVdpRetail() == null) {
							pw.print("0.00");
						} else {
							pw.print(item.getVdpRetail());
						}
					}
					pw.print(separtor);

					if (Constants.EXPORT_DELETE_DATA.equals(item.getStoreLockExpiryFlag())) {
						pw.print("0.00");
					} else {
						if (item.getCoreRetail() == null) {
							pw.print("0.00");
						} else {
							pw.print(item.getCoreRetail());
						}
					}
					pw.print(separtor);

					if (Constants.EXPORT_DELETE_DATA.equals(item.getStoreLockExpiryFlag())) {
						pw.print("");
					} else {
						if (item.getVdpRetail() == null) {
							pw.print("0.00");
						} else {
							pw.print(item.getVdpRetail());
						}
					}
					pw.print(separtor);

					if (Constants.EXPORT_DELETE_DATA.equals(item.getStoreLockExpiryFlag())) {
						pw.print("");
					} else {
						if (item.getChildLocationLevelId() < 0) {
							pw.print("");
						} else {
							pw.print(item.getChildLocationLevelId());
						}
					}
					pw.print(separtor);

					if (Constants.EXPORT_DELETE_DATA.equals(item.getStoreLockExpiryFlag())) {
						pw.print("");
					} else {
						if (item.getDiffRetail() == null) {
							pw.print("0.00");
						} else {
							pw.print(item.getDiffRetail());
						}
					}
					pw.print(separtor);

					if (Constants.EXPORT_DELETE_DATA.equals(item.getStoreLockExpiryFlag())) {
						pw.print("");
					} else {
						if (item.getApprovedBy() == null) {
							pw.print("");
						} else {
							pw.print(item.getApprovedBy());// approver
						}
					}
					pw.print(separtor);

					if (Constants.EXPORT_DELETE_DATA.equals(item.getStoreLockExpiryFlag())) {
						pw.print("");
					} else {
						if (item.getApproverName() == null) {
							pw.print("");
						} else {
							pw.print(item.getApproverName());
						}
					}
					pw.print(separtor);

					if (Constants.EXPORT_DELETE_DATA.equals(item.getStoreLockExpiryFlag())) {
						pw.print("");
					} else {
						if (item.getPriceCheckListTypeId() == null) {
							if (Constants.EMERGENCY.equals(item.getPriceExportType())) {
								pw.print(Constants.EMERGENCY);
							} else {
								pw.print(""); // CE FLAG
							}
						} else if (item.getPriceCheckListTypeId() == Integer
								.parseInt(Constants.EMERGENCY_LIST_TYPE)) {
							pw.print(Constants.EMERGENCY);
						} else if (item.getPriceCheckListTypeId() == Integer
								.parseInt(Constants.CLEARANCE_LIST_TYPE)) {
							pw.print(Constants.CLEARANCE);
						} else {
							// for store lock
							pw.print("");
						}
					}
					pw.print(separtor);

					if (Constants.EXPORT_DELETE_DATA.equals(item.getStoreLockExpiryFlag())
							|| Constants.EXPORT_ADD_DATA.equals(item.getStoreLockExpiryFlag())) {
						pw.print(item.getStoreLockExpiryFlag());
					} else {
						pw.print("");
					}
					pw.println();
				}
			}
		}
		pw.flush();
		fw.flush();
		pw.close();
		fw.close();
		
		logger.info("writeExportDataInCSV() - writing export data - ends.");	
		
		}
		catch(IOException e) {
			logger.info("writeExportDataInCSV() - Error writing export data - " + e);
		}		
	}
	
	private void auditLogs(List<PriceExportDTO> exportItemList) {
		/*
		 * 1. Summary of what RU?s have been exported in a given day. 2. Number of
		 * distinct items in the export vs item count in RU on Dashboard. 3. Number of
		 * item-store records for ?A? compared to the item list for approved RU. 4.
		 * Number of item-store records for ?D? compared to the item list that has
		 * ended. Also for the ?D? records in the export file, need hierarchy
		 * information as that is not being populated in the export file. 5. Distinct
		 * dates in a given export file for that day.
		 */
		Set<String> uniqueRU = new HashSet<>();
		Set<String> uniqueEffDate = new HashSet<>();
		if (exportItemList.size() > 0) {			
			for(PriceExportDTO data : exportItemList) {
				uniqueRU.add(data.getRecommendationUnit());
				uniqueEffDate.add(data.getRegEffDate());
			}

			HashMap<String, List<PriceExportDTO>> exportDataByRetaileritem = (HashMap<String, List<PriceExportDTO>>) exportItemList
					.stream().filter(e -> (e.getRetailerItemCode() != null || !e.getRetailerItemCode().isEmpty()))
					.collect(Collectors.groupingBy(PriceExportDTO::getRetailerItemCode));


			logger.info("****************************************************************************");
			if (uniqueRU != null) {
				logger.info("RUs EXPORTED: ");
				logger.info(String.join(",", uniqueRU));
			}
			if (exportDataByRetaileritem != null) {
				logger.info("DISTINCT COUNT OF ITEMS EXPORTED: " + exportDataByRetaileritem.keySet().size());
			}
			if (uniqueEffDate != null) {
				logger.info("DISTINCT EFFECTIVE DATE EXPORTED: " + String.join(",", uniqueEffDate));
			}
			logger.info("****************************************************************************");
		} else {
			logger.info("auditLogs() - NO items to export!");
		}

	}

/**
 * 
 * @param input
 * @param priceTestItemData 
 * @param itemStoreCombinationsFromPriceTest
 * @param approvedItemList
 * @return storeLockItemMap
 * @throws GeneralException
 * 
 * 
 * return map, where key1 - store lock items from DB
 * key2 - aggregated store lock item with regular price
 * 
 * Method will generate store lock items created in item list or/and store lock items approved
 * 
 */ 
	public HashMap<String, List<PriceExportDTO>>  populateStoreLockItems(String input, 
			HashMap<Integer, List<PriceExportDTO>> priceTestItemData, List<PriceExportDTO> approvedItemList) throws GeneralException {
		logger.info("populateStoreLockItemList() - Populating Store lock items....");
		
		HashMap<String, List<PriceExportDTO>> storeLockItemMap = new HashMap<>();
		HashMap<String,String> storeNumIDMap = new HashMap<>();
		for(StoreDTO stores : storeData) {
			storeNumIDMap.put(stores.strNum, String.valueOf(stores.strId));
		}
		try {
			storeLockItemMap = getStoreLockPriceItems(approvedItemList, input, storeNumIDMap);			
			
		}catch(GeneralException e) {
			logger.info("populateStoreLockItemList() - Error while populating store lock items: " + e);
		}
		
		return storeLockItemMap;
		
	}
	
	//Audit Store lock items from Check list
	//Addded on 08/18/2021 - PROM-2248
	/**
	 * 
	 * @param storeLockItems
	 * 
	 * Add audit for store lock items, # of store lock items across # of stores
	 */
	private void AuditStoreLockItems(Connection conn, List<PriceExportDTO> storeLockItems) {

		try {

			HashMap<String, List<PriceExportDTO>> locationProductMap = new HashMap<>();

			for (PriceExportDTO plObj : storeLockItems) {
				List<PriceExportDTO> tempList = new ArrayList<>();
				if (locationProductMap.containsKey(plObj.getRecommendationUnitId() + ";" + plObj.getLocationId())) {
					tempList = locationProductMap.get(plObj.getRecommendationUnitId() + ";" + plObj.getLocationId());
				}
				tempList.add(plObj);
				locationProductMap.put(plObj.getRecommendationUnitId() + ";" + plObj.getLocationId(), tempList);
			}

			for (Map.Entry<String, List<PriceExportDTO>> listEntry : locationProductMap.entrySet()) {
				int location = Integer.parseInt(listEntry.getKey().split(";")[1]);
				int ru = Integer.parseInt(listEntry.getKey().split(";")[0]);

				Set<Integer> distinctAItems = new HashSet<>();
				Set<String> distinctAStores = new HashSet<>();

				for (PriceExportDTO plObj : listEntry.getValue()) {
					distinctAItems.add(plObj.getItemCode());
					distinctAStores.add(plObj.getStoreNo());
				}

				callAuditTrail(Constants.STORE_LIST_LEVEL_ID, location, Constants.RECOMMENDATIONUNIT, ru,
						AuditTrailTypeLookup.RECOMMENDATION.getAuditTrailTypeId(), 0, 0L, PRConstants.BATCH_USER,
						AuditTrailStatusLookup.AUDIT_TYPE.getAuditTrailTypeId(),
						AuditTrailStatusLookup.AUDIT_SUB_TYPE_EXPORT.getAuditTrailTypeId(),
						AuditTrailStatusLookup.SUB_STATUS_TYPE_A_RECORDS.getAuditTrailTypeId(), distinctAItems.size(),
						distinctAStores.size(), listEntry.getValue().get(0).getCalendarId());
			}
		} catch (GeneralException e) {
			logger.info("AuditStoreLockItems() - Error calling audit store lock item");
		}
	}
	
	/**
	 * 
	 * @param input
	 * @param itemStoreCombinationsFromPriceTest
	 * @param candidateItemList
	 * @param fileName
	 * @param sourceFile
	 * @param targetFile
	 * @return expiryItems
	 * 
	 * populate expiry items i.e., store lock items expired in last 7 days of export/ date of export
	 * 
	 */
	public HashMap<String,PriceExportDTO> populateExpiryItemList(String input, HashMap<Integer,List<PriceExportDTO>> itemStoreCombinationsFromPriceTest, 
			List<PriceExportDTO> candidateItemList, String fileName, File sourceFile, File targetFile) {
		logger.info("populateExpiryItemList() - Populating expiry items for price export.");
		HashMap<String,PriceExportDTO> expiryItems = new HashMap<>();
		
		HashMap<String, List<PriceExportDTO>> ruItemMap = (HashMap<String, List<PriceExportDTO>>) itemRUData.stream()
				.collect(Collectors.groupingBy(PriceExportDTO::getRetailerItemCode));
		
		List<PriceExportDTO> exportItems = new ArrayList<>();
		try {
			boolean noDataInQueue = false;
			if(candidateItemList == null || candidateItemList.size() <= 0) {
				noDataInQueue = true;
			}
			
			expiryItems = getStoreLockExpiryItems(candidateItemList, input, noDataInQueue, itemStoreCombinationsFromPriceTest, 
					ruItemMap, exportItems, fileName, sourceFile, targetFile);
			
			logger.info("# in expiryItems: " + expiryItems.size());			

		}
		catch(GeneralException e) {
			logger.info("populateExpiryItemList() - Error while populating expiry items: " + e);
		}	
		return expiryItems;
	}
	
	/**
	 * 
	 * @param expiryItems
	 * 
	 * Method to audit D records,  count of items and stores exported
	 */
	private void AuditExpiryItems(HashMap<String,PriceExportDTO> expiryItems) {

		logger.debug("populateExpiryItemList() - Auditing D records`");
		try {
			for (Map.Entry<String, PriceExportDTO> listEntry : expiryItems.entrySet()) {
				int location = Integer.parseInt(listEntry.getKey().split(";")[1]);
				int ru = Integer.parseInt(listEntry.getKey().split(";")[0]);

				int distinctDItems = listEntry.getValue().getItemCount().size();
				int distinctDStores = listEntry.getValue().getStoreCount().size();
				int calId = listEntry.getValue().getCalendarId();

				callAuditTrail(Constants.STORE_LIST_LEVEL_ID, location, Constants.RECOMMENDATIONUNIT, ru,
						AuditTrailTypeLookup.RECOMMENDATION.getAuditTrailTypeId(), 0, 0L, PRConstants.BATCH_USER,
						AuditTrailStatusLookup.AUDIT_TYPE.getAuditTrailTypeId(),
						AuditTrailStatusLookup.AUDIT_SUB_TYPE_EXPORT.getAuditTrailTypeId(),
						AuditTrailStatusLookup.SUB_STATUS_TYPE_D_RECORDS.getAuditTrailTypeId(), distinctDItems,
						distinctDStores, calId);
			}
		} catch (GeneralException e) {
			logger.info("populateExpiryItemList() - Error while Auditing D records: " + e);
		}
	}
	
	/**
	 * 
	 * @param candidateItemList
	 * @param runIdMap
	 * 
	 * delete the export items from PR_PRICE_EXPORT table
	 * insert exported approved items to PR_PRICE_EXPORT_TRACK
	 * update is_exported = 'Y' for the items which are exported in PR_QUARTER_REC_ITEM table
	 * update exported date and exported by and status = 7/8 in PR_QUARTERREC_HEADER
	 * insert record with status 7/8 in the PR_QUARTER_REC_STATUS table
	 * @param runIdandItemsCtMapFrmExportQueue 
	 */
	private void updateExportStatus(List<PriceExportDTO> candidateItemList, HashMap<String, List<Long>> runIdMap,
			HashMap<Long, Integer> runIdandItemsCtMapFrmExportQueue) {
		List<Long> runIdList = new ArrayList<>();
		if (runIdMap.get("N") == null || runIdMap.get("N").size() == 0) {
			logger.debug("No regular zone items to update status");
		}else {
			runIdList.addAll(runIdMap.get("N"));
		}
		if (runIdMap.get("T") == null || runIdMap.get("T").size() == 0) {
			logger.debug("No test zone items to update status");
		}
		else {
			runIdList.addAll(runIdMap.get("T"));
		}
		
		if (candidateItemList.size() > 0) {
			logger.info("# of data to update status: " + candidateItemList.size());
			try {
				logger.info("updateExportStatus() - Deleting the processed data started ....");
				priceExportDAOV2.deleteProcessedRunIdsV3(conn, candidateItemList);

				logger.info("updateExportStatus() - Status update for exported items started ...");
				priceExportDAOV2.updateExportItemsV3(conn, candidateItemList);

				logger.info("updateExportStatus() - Status update for LIG..");
				priceExportDAOV2.updateExportLigItemsV3(conn, candidateItemList);

				logger.info("updateExportStatus() - Status update for Rec Header table ..");
				priceExportDAOV2.updateExportStatusV3(conn, candidateItemList, runIdList,
						runIdandItemsCtMapFrmExportQueue);

				logger.info("updateExportStatus() - Inserting the Export Status for Rec Status table ....");
				priceExportDAOV2.insertExportStatusV3(conn, runIdList, candidateItemList,
						runIdandItemsCtMapFrmExportQueue);

				logger.info("updateExportStatus() - Insert for Export tracker Table started ..");
				priceExportDAOV2.insertExportTrackIdV3(conn, candidateItemList);

				logger.info("updateExportStatus() - Status Updates Completed.....");

				PristineDBUtil.commitTransaction(conn, "Price export update status");
			} catch (GeneralException e) {
				logger.info("updateExportStatus() Error while updating export status: " + e);
			}
		}		
	}
	
	/**
	 * 
	 * @param input
	 * @param emergencyAndClearanceItems
	 * @param runIdMap
	 * 
	 * Change the effective dates of remaning items in the queue
	 * date of export +7 days to sales floor items
	 * date of export +1 day to hard part items
	 * 
	 */
	private void changeEffDataIfNoApprovedData(String input, HashMap<String,List<PriceExportDTO>> emergencyAndClearanceItems,
			HashMap<String, List<Long>> runIdMap) {
		logger.info("changeEffDataIfNoApprovedData() - postponding effective date for remaining items");

		try {
			List<Long> runIdList = new ArrayList<>();
			if (runIdMap != null && runIdMap.size() > 0) {

				if (runIdMap.get("N") != null || runIdMap.get("N").size() > 0) {
					runIdList.addAll(runIdMap.get("N"));
				}
				if (runIdMap.get("T") != null || runIdMap.get("T").size() > 0) {
					runIdList.addAll(runIdMap.get("T"));
				}

				postEffectiveDateChange(input, emergencyAndClearanceItems, runIdList, salesFloorFiltered);

				PristineDBUtil.commitTransaction(conn, "Price export update status");
			}

		} catch (GeneralException | Exception e) {
			logger.info("changeEffDataIfNoApprovedData() - Error while postponding effective date for remaining items: "
					+ e);
		}
	}
	
	private void postEffectiveDateChange(String input, HashMap<String,List<PriceExportDTO>> ecItems, List<Long> runIdList, 
			List<PriceExportDTO> salesFloorFiltered) throws GeneralException {
		
		List<PriceExportDTO> postDatedLigItems = new ArrayList<>();

		if ((Constants.EMERGENCY_OR_HARDPART.equals(input) && emergency)
					|| (Constants.ALL_SALESFLOOR_AND_HARDPART_AND_EMERGENCY.equals(input) && emergency)
					|| (ecItems!=null && ecItems.size() > 0)) {
				logger.info("postEffectiveDateChange() - Exchange Effective date for HardPart Items when price "
						+ "export Type is : " + input);
				List<PriceExportDTO> leftoutHardPartItems = priceExportDAOV2.getHardpartItemLeftoutV3(conn,
						runIdList);
				if (leftoutHardPartItems.size() == 0) {
					logger.info("postEffectiveDateChange() - No hard part items left on " + today + " batch run..");
				} else {
					logger.info("postEffectiveDateChange() - # of left out hardpart item on " + today + " batch run is: "
							+ leftoutHardPartItems.size());
				priceExportDAOV2.changeEffectiveDateV3(conn, leftoutHardPartItems, 1);
				
				// get the LIG items which are postdated.Code added by Karishma
				if (leftoutHardPartItems.size() > 0) {
					List<PriceExportDTO> ligHPItems = leftoutHardPartItems.stream().filter(c -> c.getRetLirId() > 0)
							.collect(Collectors.toList());
					logger.info("postEffectiveDateChange()-HP LIG items count :" + ligHPItems.size());
					postDatedLigItems = priceExportDAOV2.getLIGItemsPostDated(conn, ligHPItems);
				}

			}
			}
		
		
		if(Constants.BOTH_SALESFLOOR_AND_HARDPART.equals(input) || Constants.SALE_FLOOR_ITEMS.equals(input)
				|| (Constants.EMERGENCY_OR_SALESFLOOR.equals(input))
				|| (Constants.ALL_SALESFLOOR_AND_HARDPART_AND_EMERGENCY.equals(input) && emergency)) {
			
			logger.info("postEffectiveDateChange() - Exchange Effective date for SalesFloor Items when price "
					+ "export Type is : " + input);
			List<PriceExportDTO> leftoutSalesFloorItems = priceExportDAOV2.getSalesFloorItemsLeftoutV3(conn,
					salesFloorFiltered);
			if (leftoutSalesFloorItems.size() == 0) {
				logger.info("postEffectiveDateChange() - No sales floor items left on " + today + "batch run..");
			}
			else {
				logger.info("postEffectiveDateChange() - # of left out salesfloor item on " + today + "batch run is: "
						+ leftoutSalesFloorItems.size());
				priceExportDAOV2.changeEffectiveDateV3(conn, leftoutSalesFloorItems, 7);
			
				// get the LIG items which are postdated
				if (leftoutSalesFloorItems.size() > 0) {
					List<PriceExportDTO> ligSFItems = leftoutSalesFloorItems.stream().filter(c -> c.getRetLirId() > 0)
							.collect(Collectors.toList());
					logger.info("postEffectiveDateChange()-SalesFloor LIG items count :" + ligSFItems.size());
					postDatedLigItems = priceExportDAOV2.getLIGItemsPostDated(conn, ligSFItems);
				}
			}
		}
		
		// if there are LIG members which are postDated then change the reg Eff date of
		// the LIG Rep item
		// If LIG has SF and HP both then LIG rep will have the date of the most recent
		// eff date
		logger.info("postEffectiveDateChange()-# items in postDatedLigItems List: " +postDatedLigItems.size());
		if (postDatedLigItems.size() > 0) {

			Map<Integer, List<PriceExportDTO>> postDatedLIGItemsMap = postDatedLigItems.parallelStream()
					.collect(Collectors.groupingBy(PriceExportDTO::getRetLirId));
			setPostDatedEffDataForLIG(postDatedLIGItemsMap, salesFloorFiltered);

		}
	} 
	
	/**
	 * 
	 * @param runIdMap
	 * @param exportItemsToUpdate
	 * 
	 * Audit trail the exported product and zone
	 * Add notification to the prod and zone for which it is exported
	 */
	private void updateExportStatusInNotificationAndAuditTrial( 
			HashMap<String,List<Long>> runIdMap, List<PriceExportDTO> exportItemsToUpdate) {
		
		if (exportItemsToUpdate.size() > 0) {

			List<Long> runIdList = new ArrayList<>();
			if (runIdMap.get("N") == null || runIdMap.get("N").size() < 0) {
				logger.debug("No regular zone items to update status");
			} else {
				runIdList.addAll(runIdMap.get("N"));
			}
			if (runIdMap.get("T") == null || runIdMap.get("T").size() < 0) {
				logger.debug("No test zone items to update status");
			} else {
				runIdList.addAll(runIdMap.get("T"));
			}

			try {
				if (runIdList.size() > 0) {
					logger.info("exportPrice() - Adding notification of export data..");
					addNotification(conn, runIdList, emergency, priceExportDAOV2);

					logger.info("exportPrice() - Audit trial for Price export status");
					auditTrialExportData(conn, runIdList, emergency, priceExportDAOV2);
				}

				PristineDBUtil.commitTransaction(conn, "Price export update status");
			} catch (SQLException | GeneralException e) {
				logger.info(
						"updateExportStatusInNotificationAndAuditTrial() - Error while updating export status in notification and audit trial: "
								+ e);
			}
		}
	}
	
	private void addNotification(Connection conn, List<Long> runIdList, boolean emergency, PriceExportDAOV2 priceExportDao)
			throws SQLException, GeneralException {
		HashMap<Long, Integer> runIdAndStatusCode = priceExportDao.getRunIdWithStatusCode(conn, runIdList);
		int statusCode = 0;

		NotificationService notificationService = new NotificationService();
		for (Map.Entry<Long, Integer> runIdAndStatusEntry : runIdAndStatusCode.entrySet()) {

			List<NotificationDetailInputDTO> notificationDetailDTOs = new ArrayList<NotificationDetailInputDTO>();
			long runId = runIdAndStatusEntry.getKey();

			logger.debug("Export Status code: 7 - Partially exported, 8 - Exported, 10 - Emergency Approved");
			//if (/* priceExportType.equals(Constants.NORMAL) || */!emergency) {
				statusCode = runIdAndStatusEntry.getValue();
				logger.debug("addNotification() - Exported Status code : " + statusCode);
			/*
			 * } else if (emergency) { statusCode = PRConstants.EXPORT_EMERGENCY;
			 * logger.debug("addNotification() - Exported Status code : " + statusCode); }
			 */
			NotificationDetailInputDTO notificationDetailInputDTO = new NotificationDetailInputDTO();
			notificationDetailInputDTO.setModuleId(PRConstants.REC_MODULE_ID);
			notificationDetailInputDTO.setNotificationTypeId(statusCode);
			notificationDetailInputDTO.setNotificationKey1(runId);
			notificationDetailDTOs.add(notificationDetailInputDTO);
			// logger.info("notificationDetailDTOs size: " + notificationDetailDTOs.size());
			notificationService.addNotificationsBatch(conn, notificationDetailDTOs, true);

		}
	}
	
	private void auditTrialExportData(Connection conn, List<Long> runIdList, 
			boolean emergency, PriceExportDAOV2 priceExportDao) throws GeneralException {

		// int auditTrialTypeId;
		int statusCode = 0;
		HashMap<Long, List<PRItemDTO>> productLocationDataForRunId = priceExportDao.getProductLocationDetail(conn,
				runIdList);

		
		List<PRItemDTO> productLocationObjects = new ArrayList<PRItemDTO>();
		for (Map.Entry<Long, List<PRItemDTO>> productLocationEntry : productLocationDataForRunId.entrySet()) {
			// long runId = productLocationEntry.getKey();
			productLocationObjects = productLocationEntry.getValue();

			for (PRItemDTO plObj : productLocationObjects) {
				PRItemDTO obj = plObj;
				//if (!emergency) {
					statusCode = obj.getStatusCode();
					logger.debug("AuditTrialExportData() - Exported Status code : " + statusCode);
				//} 
			
				if (Constants.PARTIALLY_EXPORTED_STATUS_CODE == statusCode) {
					callAuditTrail(plObj.getChildLocationLevelId(), obj.getPriceZoneId(), obj.getProductLevelId(),
							obj.getItemCode(), AuditTrailTypeLookup.RECOMMENDATION.getAuditTrailTypeId(), statusCode,
							productLocationEntry.getKey(), PRConstants.BATCH_USER,
							AuditTrailStatusLookup.AUDIT_TYPE.getAuditTrailTypeId(),
							AuditTrailStatusLookup.AUDIT_SUB_TYPE_EXPORT.getAuditTrailTypeId(),
							AuditTrailStatusLookup.SUB_STATUS_TYPE_PARTIAL_EXPORT.getAuditTrailTypeId(),0,0,0);
				} else if(Constants.EXPORTED_STATUS_CODE == statusCode){
					callAuditTrail(plObj.getChildLocationLevelId(), obj.getPriceZoneId(), obj.getProductLevelId(),
							obj.getItemCode(), AuditTrailTypeLookup.RECOMMENDATION.getAuditTrailTypeId(), statusCode,
							productLocationEntry.getKey(), PRConstants.BATCH_USER,
							AuditTrailStatusLookup.AUDIT_TYPE.getAuditTrailTypeId(),
							AuditTrailStatusLookup.AUDIT_SUB_TYPE_EXPORT.getAuditTrailTypeId(),
							AuditTrailStatusLookup.SUB_STATUS_TYPE_EXPORT.getAuditTrailTypeId(),0,0,0);
				}
			}
		}
	}
	
	private void callAuditTrail(int locationLevelId, int locationId, int productLevelId, int productId,
			int auditTrailTypeId, int statusCode, long runId, String batchUser, int auditType, int auditSubType, int subStatusType, 
			int countOfItems, int countOfStores, int exportTime) throws GeneralException {
		
		AuditTrailService auditTrailService = new AuditTrailService();
		auditTrailService.auditRecommendation(conn, locationLevelId, locationId, productLevelId, productId,
				auditTrailTypeId, statusCode, runId, batchUser,auditType, auditSubType, subStatusType, countOfItems, countOfStores, exportTime);

	}
	
	private HashMap<String, List<PriceExportDTO>> getStoreLockPriceItems(List<PriceExportDTO> approvedItems, String priceExportType, 
			HashMap<String, String> storeNumIDMap) throws GeneralException {
		HashMap<String, List<PriceExportDTO>> storeLockMap = new HashMap<>();
		List<PriceExportDTO> storeLockItemsToExport = new ArrayList<>();
		List<PriceExportDTO> storeLockItems = new ArrayList<>();
		Set<Integer> itemCodes = new HashSet<>();
		Set<String> itemCodeStrSet = new HashSet<>();
		Set<String> priceNotFound = new HashSet<>();
		Set<Integer> zones = new HashSet<>();
		String curWkStartDate = DateUtil.getWeekStartDate(0);
		HashMap<Integer, HashMap<Integer,List<PriceExportDTO>>> itemStoreLockMap = new HashMap<>();
		
		RetailCalendarDTO retailCalendarDTO = retailCalendarDAO.getCalendarId(conn, curWkStartDate,
				Constants.CALENDAR_WEEK);
		int calendarId = retailCalendarDTO.getCalendarId();
		
		int lockListTypeId = Integer.parseInt(PropertyManager.getProperty("PRICE_CHECK_LIST_TYPE_ID"));
		
		approvedItems.forEach(item -> {
			itemCodes.add(item.getItemCode());
			zones.add(item.getPriceZoneId());
		});

		
		logger.info("getStoreLockItems() - Getting store lock for approved items for store Level  ...");
		
		itemStoreLockMap = priceExportDAOV2.getExcludeStoresFromStoreLockListV2(conn,itemCodes, zones, lockListTypeId,
				Constants.STORE_LEVEL_ID, priceExportType);

		logger.info("getStoreLockItems() - Getting store lock for approved items for store Level  Completed  and # of records : "+ itemStoreLockMap.size()  );

		logger.info("getStoreLockItems() - Getting store locks for approved items for store lists...");
		HashMap<Integer, HashMap<Integer,List<PriceExportDTO>>> itemStoreListLockMap = priceExportDAOV2.getExcludeStoresFromStoreLockListV2(conn, itemCodes, zones, lockListTypeId,
				Constants.STORE_LIST_LEVEL_ID, priceExportType);
		
		itemStoreLockMap.putAll(itemStoreListLockMap);
		
		logger.info("getStoreLockItems() -  # of records in itemStoreLockMap : " + itemStoreListLockMap.size());
		
		//changes added by kirthi
		HashMap<Integer, HashMap<Integer,List<PriceExportDTO>>> itemZoneCombMap = new HashMap<>();
		HashMap<Integer, List<PriceExportDTO>> approvedItemsByItem = (HashMap<Integer, List<PriceExportDTO>>)approvedItems.stream()
				.collect(Collectors.groupingBy(PriceExportDTO::getItemCode));
		
		approvedItemsByItem.forEach((item, data)-> {
			HashMap<Integer, List<PriceExportDTO>> approvedItemsByZone = (HashMap<Integer, List<PriceExportDTO>>)data.stream()
					.collect(Collectors.groupingBy(PriceExportDTO::getPriceZoneId));
			itemZoneCombMap.put(item, approvedItemsByZone);
		});
		
		if (itemStoreLockMap.size() == 0) {
			logger.info("getStoreLockItems() - No store Lock data is present ");
		}
		else {
			
			for(Map.Entry<Integer, HashMap<Integer,List<PriceExportDTO>>> itemZoneEntry : itemZoneCombMap.entrySet()){
				int item = itemZoneEntry.getKey();
				HashMap<Integer,List<PriceExportDTO>> zoneDataMap = itemZoneEntry.getValue();
				if (itemStoreListLockMap.containsKey(item)) {
					HashMap<Integer, List<PriceExportDTO>> zoneDataOfStoreLockItems = itemStoreListLockMap.get(item);
					for(Map.Entry<Integer,List<PriceExportDTO>> zoneDataEntry : zoneDataMap.entrySet()){
						Integer zoneId = zoneDataEntry.getKey();
						List<PriceExportDTO> storeList = zoneDataEntry.getValue();
						if (zoneDataOfStoreLockItems.containsKey(zoneId)) {
							List<PriceExportDTO> storeLockData = zoneDataOfStoreLockItems.get(zoneId);

							try {
							// iterate every store lock items and set
							for (PriceExportDTO approvedObj : storeList) {
								for (PriceExportDTO storeLockObj : storeLockData) {
									PriceExportDTO itemStoreObj = (PriceExportDTO) approvedObj.clone();
									itemStoreObj.setStoreNo(storeLockObj.getStoreNo());
									String storeId = storeNumIDMap.get(itemStoreObj.getStoreNo());
									itemStoreObj.setStoreId(storeId);
									itemStoreObj.setCalendarId(calendarId);
									itemStoreObj.setChildLocationLevelId(Constants.STORE_LEVEL_TYPE_ID);
									itemStoreObj.setStoreLockExpiryFlag(Constants.EXPORT_ADD_DATA);
									itemStoreObj.setLocationId(storeLockObj.getLocationId());
									itemStoreObj.setPriceCheckListTypeId(lockListTypeId);
									storeLockItems.add(itemStoreObj);
								}
							}
							}catch(Exception e) {
								logger.info("Error while cloning for store lock items"+ e);
							}
						}
					}
				}
			}
		}
		

		logger.info("getStoreLockItems() - # of items with store lock: " + itemStoreLockMap.size());

		itemStoreLockMap.keySet().forEach(itemCode -> {
			itemCodeStrSet.add(String.valueOf(itemCode));
		});

		logger.info("getStoreLockItems() - Getting price info for items : " + itemCodeStrSet.size() );
		
		List<Integer> historyCalendarList = new ArrayList<>();
		if (retailCalendarDTO.getStartDate() == null) {

		} else {
			historyCalendarList = getCalendarIdHistory(conn, retailCalendarDTO.getStartDate());
		}
		
    	logger.debug("getStoreLockItems() -calendar Ids: " + historyCalendarList);
    	
		HashMap<String, List<RetailPriceDTO>> priceDataMap = retailPriceDAO.getRetailPriceInfoWithHistory(conn, itemCodeStrSet,
				calendarId, false, historyCalendarList);
		
		logger.debug("getStoreLockItems() - # in priceDataMap: " + priceDataMap.size());
		
		for (Map.Entry<Integer, HashMap<Integer, List<PriceExportDTO>>> itemZoneEntry : itemZoneCombMap.entrySet()) {
			int item = itemZoneEntry.getKey();
			HashMap<Integer, List<PriceExportDTO>> zoneDataMap = itemZoneEntry.getValue();

			// only if store lock items are approved proceed
			if (itemStoreListLockMap.containsKey(item)) {

				// only if price is available for the item proceed
				if (priceDataMap != null && priceDataMap.containsKey(String.valueOf(item))) {


					HashMap<Integer, List<PriceExportDTO>> zoneDataOfStoreLockItems = itemStoreListLockMap.get(item);
					for (Map.Entry<Integer, List<PriceExportDTO>> zoneDataEntry : zoneDataMap.entrySet()) {
						Integer zoneId = zoneDataEntry.getKey();
						PriceExportDTO approvedObj  = zoneDataEntry.getValue().get(0);

						// only if store lock zone are approved proceed
						if (zoneDataOfStoreLockItems.containsKey(zoneId)) {

							List<PriceExportDTO> storeLockData = zoneDataOfStoreLockItems.get(zoneId);
							Set<String> storesUnderStoreLock = storeLockData.get(0).getStoreNums();
							
							// get item price for stores locked
							HashMap<String, RetailPriceDTO> storeLockItemsPrice = storePriceExportAZSerive
									.getCurrentPriceForStoreLockItems(storesUnderStoreLock, item,
											priceDataMap.get(String.valueOf(item)),zoneIdAndNoMap,storeZoneMap,baseChainId);
						
							try {
								// iterate every store lock items and set
									for (String storeNumber : storesUnderStoreLock) {
										PriceExportDTO itemStoreObj = (PriceExportDTO) approvedObj.clone();
										itemStoreObj.setStoreNo(storeNumber);
										String storeId = storeNumIDMap.get(itemStoreObj.getStoreNo());
										itemStoreObj.setStoreId(storeId);
										itemStoreObj.setCalendarId(calendarId);
										itemStoreObj.setChildLocationLevelId(Constants.STORE_LEVEL_TYPE_ID);
										itemStoreObj.setStoreLockExpiryFlag(Constants.EXPORT_ADD_DATA);
										itemStoreObj.setLocationId(storeLockData.get(0).getLocationId());
										itemStoreObj.setPriceCheckListTypeId(lockListTypeId);

										try {
											double retailPrice = storeLockItemsPrice.get(itemStoreObj.getStoreNo())
													.getRegPrice();
											MultiplePrice currentPrice = new MultiplePrice(1, retailPrice);
											itemStoreObj.setRecommendedRegPrice(currentPrice);
											itemStoreObj
													.setVdpRetail(itemStoreObj.getRecommendedRegPrice().getUnitPrice());
											itemStoreObj.setDiffRetail(0D);
											storeLockItemsToExport.add(itemStoreObj);
									} catch (Exception ex) {
										logger.error("getStoreLockItems()-current price not found for item : "
												+ itemStoreObj.getItemCode() + "and store No:"
												+ itemStoreObj.getStoreNo());
									}

									}
								
							} catch (Exception e) {
								logger.info("Error while cloning for store lock items "+ e);
							}
						}
					}
				} else {
					logger.info("Current price is not found for presto item: " + item);
				}
			}
			else if (itemStoreLockMap.containsKey(item)) {

				// only if price is available for the item proceed
				if (priceDataMap != null && priceDataMap.containsKey(String.valueOf(item))) {

					HashMap<Integer, List<PriceExportDTO>> zoneDataOfStoreLockItems = itemStoreLockMap.get(item);
					for (Map.Entry<Integer, List<PriceExportDTO>> zoneDataEntry : zoneDataMap.entrySet()) {
						Integer zoneId = zoneDataEntry.getKey();
						PriceExportDTO approvedObj  = zoneDataEntry.getValue().get(0);

						// only if store lock zone are approved proceed
						if (zoneDataOfStoreLockItems.containsKey(zoneId)) {

							List<PriceExportDTO> storeLockData = zoneDataOfStoreLockItems.get(zoneId);
							Set<String> storesUnderStoreLock = storeLockData.get(0).getStoreNums();
							
							// get item price for stores locked
							HashMap<String, RetailPriceDTO> storeLockItemsPrice = storePriceExportAZSerive
									.getCurrentPriceForStoreLockItems(storesUnderStoreLock, item,
											priceDataMap.get(String.valueOf(item)),zoneIdAndNoMap,storeZoneMap,baseChainId);
						
							try {
								// iterate every store lock items and set
									for (String storeNumber : storesUnderStoreLock) {
										PriceExportDTO itemStoreObj = (PriceExportDTO) approvedObj.clone();
										itemStoreObj.setStoreNo(storeNumber);
										String storeId = storeNumIDMap.get(itemStoreObj.getStoreNo());
										itemStoreObj.setStoreId(storeId);
										itemStoreObj.setCalendarId(calendarId);
										itemStoreObj.setChildLocationLevelId(Constants.STORE_LEVEL_TYPE_ID);
										itemStoreObj.setStoreLockExpiryFlag(Constants.EXPORT_ADD_DATA);
										itemStoreObj.setLocationId(storeLockData.get(0).getLocationId());
										itemStoreObj.setPriceCheckListTypeId(lockListTypeId);

										try {
											double retailPrice = storeLockItemsPrice.get(itemStoreObj.getStoreNo())
													.getRegPrice();
											MultiplePrice currentPrice = new MultiplePrice(1, retailPrice);
											itemStoreObj.setRecommendedRegPrice(currentPrice);
											itemStoreObj
													.setVdpRetail(itemStoreObj.getRecommendedRegPrice().getUnitPrice());
											itemStoreObj.setDiffRetail(0D);
											storeLockItemsToExport.add(itemStoreObj);
									} catch (Exception ex) {
										logger.error("getStoreLockItems()-current price not found for item : "
												+ itemStoreObj.getItemCode() + "and store No:"
												+ itemStoreObj.getStoreNo());
									}

									}
								
							} catch (Exception e) {
								logger.info("Error while cloning for store lock items "+ e);
							}
						}
					}
				} else {
					logger.info("Current price is not found for presto item: " + item);
				}
			}
		}
		
		if (priceNotFound.size() > 0) {
			logger.warn("getStoreLockItems() - Items with no current price in calendar id " + calendarId + ": "
					+ PRCommonUtil.getCommaSeperatedStringFromStrSet(priceNotFound));
		}

		//check the logic
		if(storeLockItems.size() > 0) {
			storeLockMap.put(Constants.STORE_LOCK_ITEMS, storeLockItems);
		}if(storeLockItemsToExport.size() > 0) {
			storeLockMap.put(Constants.STORE_LOCK_ITEMS_WITH_PRICE, storeLockItemsToExport);
		}

		logger.info("getStoreLockPriceItems()- # items in storeLockItems: " + storeLockItems.size());
		logger.info("getStoreLockPriceItems()- # items in storeLockItemsToExport: " + storeLockItemsToExport.size());
	
		return storeLockMap;
	}
	
	/**
	 * 
	 * @param candidateItemList
	 * @param input
	 * @param noDataInQueue
	 * @param itemStoreCombinationsFromPriceTest
	 * @param ruItemMap
	 * @param exportItemList
	 * @param fileName
	 * @param sourceFile
	 * @param targetFile
	 * @return
	 * @throws GeneralException
	 * 
	 * Collect the expiry items from item list
	 * If there are items-stores in expiry matches with price test zone data then dont export those items-stores of expiry items
	 * 
	 * Do the process in chunk of 9000 to optimize the performance
	 * Do populate the data for Auditing D recods in the chunk processing itself
	 * 
	 */
	private HashMap<String, PriceExportDTO> getStoreLockExpiryItems(List<PriceExportDTO> candidateItemList,String input, boolean noDataInQueue,
			HashMap<Integer,List<PriceExportDTO>> itemStoreCombinationsFromPriceTest, HashMap<String, List<PriceExportDTO>> ruItemMap, 
			List<PriceExportDTO> exportItemList, String fileName, File sourceFile, File targetFile) throws GeneralException {
		
		List<PriceExportDTO> expiryItemsToExport = new ArrayList<>();
		HashMap<String, PriceExportDTO> locationProductMapForAudit = new HashMap<>();
		int lockListTypeId = Integer.parseInt(PropertyManager.getProperty("PRICE_CHECK_LIST_TYPE_ID"));
		
		HashMap<Integer, String>  zoneIdandNumMap = new HashMap<>();
		for(ZoneDTO zones : allZoneData) {
				zoneIdandNumMap.put(zones.getZnId(), zones.getZnNo());
		}
		HashMap<String,String> storeNumIDMap = new HashMap<>();
		for(StoreDTO stores : storeData) {
			storeNumIDMap.put(stores.strNum, String.valueOf(stores.strId));
		}
		
		//Get expiry items 
		HashMap<Integer, List<PriceExportDTO>> expiryItems = priceExportDAOV2.setPriceForExpiryItems(false, conn,
				input);
		
		//Populate fields for D records
		if (expiryItems.size() > 0) {
			int expiryItemCount = 0;
			logger.debug("setPriceForExpiryItems() - # in expiryItems: " + expiryItems.size());
			
			for (Map.Entry<Integer, List<PriceExportDTO>> excludeStoreValue : expiryItems.entrySet()) {
				
				int itemCode = excludeStoreValue.getKey();
				String retItem = PrestoUtil.getKey(itemData,itemCode);
				for (PriceExportDTO items : excludeStoreValue.getValue()) {

					expiryItemCount++;
					//Populate by chunck of 90000
					//Added on 08/18/2021 - PROM-2253
					if (expiryItemCount % 90000 == 0) {

						int priceZoneId = 0;
						if (items.getStoreNo() == null || items.getStoreNo().isEmpty()) {
							items.setStoreNo("");
						} else {
							if (storeZoneMap.containsKey(items.getStoreNo())) {
								priceZoneId = storeZoneMap.get(items.getStoreNo());
							}
						}

						String zoneNum = "";
						if (priceZoneId > 0) {
							if (zoneIdandNumMap.containsKey(priceZoneId)) {
								zoneNum = zoneIdandNumMap.get(priceZoneId);
							}
						}

						try {
							PriceExportDTO storeInstanceClone = null;
							if (noDataInQueue) {
								storeInstanceClone = new PriceExportDTO();
							} else {
								storeInstanceClone = (PriceExportDTO) candidateItemList.get(0).clone();
							}
							storeInstanceClone.setStoreNo(items.getStoreNo());
							storeInstanceClone.setPriceCheckListId(items.getPriceCheckListId());
							storeInstanceClone.setPriceCheckListTypeId(lockListTypeId);
							storeInstanceClone.setPriceZoneNo(zoneNum);
							storeInstanceClone.setRetailerItemCode(retItem);
							storeInstanceClone.setItemCode(itemCode);
							storeInstanceClone.setCalendarId(items.getCalendarId());
							storeInstanceClone.setLocationId(items.getLocationId());
							storeInstanceClone.setStoreLockExpiryFlag(Constants.EXPORT_DELETE_DATA);
							storeInstanceClone.setStoreListExpiry(items.isStoreListExpiry());
							storeInstanceClone.setStoreId(storeNumIDMap.get(storeInstanceClone.getStoreNo()));
							List<PriceExportDTO> itemList = ruItemMap.get(storeInstanceClone.getRetailerItemCode());
							if (itemList != null && !itemList.isEmpty()) {
								storeInstanceClone.setRecommendationUnit(itemList.get(0).getRecommendationUnit());
								storeInstanceClone.setRecommendationUnitId(itemList.get(0).getRecommendationUnitId());
							}
							expiryItemsToExport.add(storeInstanceClone);
							

						} catch (Exception e) {
							logger.error("setPriceForExpiryItems() - Error setting price for expiry items: Store - "
									+ items.getStoreNo() + ", Item: " + items.getItemCode() + e);
						}
					
									
						if(itemStoreCombinationsFromPriceTest!=null && itemStoreCombinationsFromPriceTest.size()>0) {
							if (expiryItemsToExport.size() > 0) {
								for (PriceExportDTO S : expiryItemsToExport) {
									//String storeId = storeNumIdMap.get(S.getStoreNo());
									if (!itemStoreCombinationsFromPriceTest.containsKey(S.getItemCode())){
										//	&& !itemStoreCombinationsFromPriceTest.get(S.getItemCode()).contains(storeId)) {
										Set<String> stores = itemStoreCombinationsFromPriceTest.get(S.getItemCode()).get(0).getStoreNums();
										if(!stores.contains(S.getStoreNo())) {
											exportItemList.add(S);
										}										
									}
								}
							}
						}
						else {
							exportItemList.addAll(expiryItemsToExport);
						}
						
						try {
							
							writeExportDataInCSV(exportItemList, fileName, sourceFile, targetFile);
							
							if (exportItemList.size() > 0) {
								// update export flag for store lock items such that only once the store lock
								// item with flag status is sent
								logger.debug("updating expiry item - " + exportItemList.size());
								updateExpiryItemsStatus(exportItemList);
								
							}							

							logger.info("# in locationProductMapForAudit before: " + locationProductMapForAudit.size());
							populateDrecordForAudit(exportItemList, locationProductMapForAudit);
							
							
						} catch (SQLException e) {
							logger.info("Error while writing D records");
							e.printStackTrace();
						}
						
						
						expiryItemsToExport.clear();
						exportItemList.clear();
					}
					else {

						int priceZoneId = 0;
						if (items.getStoreNo() == null || items.getStoreNo().isEmpty()) {
							items.setStoreNo("");
						} else {
							if (storeZoneMap.containsKey(items.getStoreNo())) {
								priceZoneId = storeZoneMap.get(items.getStoreNo());
							}
						}

						String zoneNum = "";
						if (priceZoneId > 0) {
							if (zoneIdandNumMap.containsKey(priceZoneId)) {
								zoneNum = zoneIdandNumMap.get(priceZoneId);
							}
						}

						try {
							PriceExportDTO storeInstanceClone = null;
							if (noDataInQueue) {
								storeInstanceClone = new PriceExportDTO();
							} else {
								storeInstanceClone = (PriceExportDTO) candidateItemList.get(0).clone();
							}
							storeInstanceClone.setStoreNo(items.getStoreNo());
							storeInstanceClone.setPriceCheckListId(items.getPriceCheckListId());
							storeInstanceClone.setPriceCheckListTypeId(lockListTypeId);
							storeInstanceClone.setPriceZoneNo(zoneNum);
							storeInstanceClone.setRetailerItemCode(retItem);
							storeInstanceClone.setItemCode(itemCode);
							storeInstanceClone.setCalendarId(items.getCalendarId());
							storeInstanceClone.setLocationId(items.getLocationId());
							storeInstanceClone.setStoreLockExpiryFlag(Constants.EXPORT_DELETE_DATA);
							storeInstanceClone.setStoreListExpiry(items.isStoreListExpiry());
							storeInstanceClone.setStoreId(storeNumIDMap.get(storeInstanceClone.getStoreNo()));
							List<PriceExportDTO> itemList = ruItemMap.get(storeInstanceClone.getRetailerItemCode());
							if (itemList != null && !itemList.isEmpty()) {
								storeInstanceClone.setRecommendationUnit(itemList.get(0).getRecommendationUnit());
								storeInstanceClone.setRecommendationUnitId(itemList.get(0).getRecommendationUnitId());
							}
							expiryItemsToExport.add(storeInstanceClone);

						} catch (Exception e) {
							logger.error("setPriceForExpiryItems() - Error setting price for expiry items: Store - "
									+ items.getStoreNo() + ", Item: " + items.getItemCode() + e);
						}
					}
				}
			}
			logger.debug("setPriceForExpiryItems() - collecting expiry item ends");
		}
		
		//Process remaining data from chunk
		if (expiryItemsToExport.size() > 0) {
			
			if(itemStoreCombinationsFromPriceTest!=null && itemStoreCombinationsFromPriceTest.size()>0) {
				if (expiryItemsToExport.size() > 0) {
					for (PriceExportDTO S : expiryItemsToExport) {
						if (!itemStoreCombinationsFromPriceTest.containsKey(S.getItemCode())){
							//	&& !itemStoreCombinationsFromPriceTest.get(S.getItemCode()).contains(storeId)) {
							Set<String> stores = itemStoreCombinationsFromPriceTest.get(S.getItemCode()).get(0).getStoreNums();
							if(!stores.contains(S.getStoreNo())) {
								exportItemList.add(S);
							}										
						}
					}
				}
			}
			else {
				exportItemList.addAll(expiryItemsToExport);
			}
			
			try {
				writeExportDataInCSV(exportItemList, fileName, sourceFile, targetFile);
				
				logger.info("# in locationProductMapForAudit before: " + locationProductMapForAudit.size());
				populateDrecordForAudit(exportItemList, locationProductMapForAudit);
							
				// update export flag for store lock items such that only once the store lock
				// item with flag status is sent
				logger.debug("updating expiry item - " + exportItemList.size());
				updateExpiryItemsStatus(exportItemList);
				
			} catch (SQLException e) {
				logger.info("Error while writing D records");
				e.printStackTrace();
			}
			
		}
				
		return locationProductMapForAudit;
	}
	
	private void populateDrecordForAudit(List<PriceExportDTO> exportItemList, HashMap<String, PriceExportDTO> locationProductMapForAudit) {
		
		try {
		HashMap<String, List<PriceExportDTO>> locationProductMap = new HashMap<>();
		for (PriceExportDTO data : exportItemList) {
			List<PriceExportDTO> tempList = new ArrayList<>();
			if (locationProductMap.containsKey(data.getRecommendationUnitId() + ";" + data.getLocationId())) {				
				tempList = locationProductMap.get(data.getRecommendationUnitId() + ";" + data.getLocationId());				
			}
			tempList.add(data);
			locationProductMap.put(data.getRecommendationUnitId() + ";" + data.getLocationId(),
						tempList);			
		}
		logger.info("# in locationProductMap " + locationProductMap.size());
		for (Map.Entry<String, List<PriceExportDTO>> listEntry : locationProductMap.entrySet()) {

			int calId = listEntry.getValue().get(0).getCalendarId();
			Set<String> distinctStores = new HashSet<>();
			Set<String> distinctItems = new HashSet<>();
			for (PriceExportDTO plObj : listEntry.getValue()) {
				distinctStores.add(plObj.getStoreNo());
				distinctItems.add(plObj.getRetailerItemCode());
			}

			for (PriceExportDTO plObj : listEntry.getValue()) {

				if (locationProductMapForAudit.containsKey(listEntry.getKey())) {
					PriceExportDTO value = locationProductMapForAudit.get(listEntry.getKey());
					value.setCalendarId(calId);
					value.getItemCount().add(plObj.getStoreNo());
					value.getItemCount().add(plObj.getRetailerItemCode());
					
					locationProductMapForAudit.put(listEntry.getKey(),value);

				} else {
					PriceExportDTO runningObj = new PriceExportDTO();
					runningObj.setItemCount(distinctItems);
					runningObj.setStoreCount(distinctStores);
					runningObj.setCalendarId(calId);
					locationProductMapForAudit.put(listEntry.getKey(),runningObj);
					break;					
				}
			}
		}
		}catch(Exception e) {
			logger.info("populateDrecordForAudit() - Error while populating Drecord For Audit: " + e);
		}
		logger.info("# in locationProductMapForAudit " + locationProductMapForAudit.size());
	}
	
	private void updateExpiryItemsStatus(List<PriceExportDTO> expiryItems) throws GeneralException {

		List<PriceExportDTO> expiryItemListAtStoreList = expiryItems.stream().filter(e -> e.isStoreListExpiry())
				.collect(Collectors.toList());
		List<PriceExportDTO> expiryItemListRegular = expiryItems.stream().filter(e -> !e.isStoreListExpiry())
				.collect(Collectors.toList());

		logger.debug("# in expiryItemListRegular: " + expiryItemListRegular.size());
		logger.debug("# in expiryItemListAtStoreList: " + expiryItemListAtStoreList.size());
		if (expiryItemListRegular.size() > 0) {
			priceExportDAOV2.updateExpiryExportFlagForRegularItemList(conn, expiryItemListRegular);
		}
		if (expiryItemListAtStoreList.size() > 0) {
			priceExportDAOV2.updateExpiryExportFlagForStoreList(conn, expiryItemListAtStoreList);
		}
	}

	public List<ZoneDTO> getAllZoneData(){
		return this.allZoneData;
	}

	public List<ZoneDTO> getAzZoneData(){
		return this.globalZoneData;
	}
	
	public List<StoreDTO> getStoreData(){
		return this.storeData;
	}
	
	public HashMap<String, Integer> getStoreZoneMap(){
		return this.storeZoneMap;
	}
	
	public HashMap<String, Integer> getItemData(){
		return this.itemData;
	}
	
	public List<PriceExportDTO> getItemRUData(){
		return this.itemRUData;
	}
	
	public Connection getConnection() {
		return this.conn;
	}
	
	/**
	 * 
	 * @param postDatedLIGItemsMap
	 * @throws GeneralException 
	 */
	private void setPostDatedEffDataForLIG(Map<Integer, List<PriceExportDTO>> postDatedLIGItemsMap, List<PriceExportDTO> salesFloorFiltered)
			throws GeneralException {

		Map<String, String> ligItemMap = new HashMap<>();
		
		List<PriceExportDTO> allNonExportedSalesFloorItems = priceExportDAOV2.getAllSalesFloorItemsV3(conn,
				salesFloorFiltered);

		Map<Integer, List<PriceExportDTO>> postDatedAllLIGItemsMap = allNonExportedSalesFloorItems.parallelStream()
				.collect(Collectors.groupingBy(PriceExportDTO::getRetLirId));
		
		for (Map.Entry<Integer, List<PriceExportDTO>> ligItems : postDatedAllLIGItemsMap.entrySet()) {

			String key = ligItems.getKey() + " - " + ligItems.getValue().get(0).getRunId();
			String effDate = "";
			// If only one member is to postdated then set the same for LIG ,
			// else get the most recent reggEff date from the members and set it at LIG
			// Level
			if (ligItems.getValue().size() == 1) {
				if (ligItems.getValue().get(0).getRegEffDate() != null
						&& ligItems.getValue().get(0).getRegEffDate() != "") {
					ligItemMap.put(key, ligItems.getValue().get(0).getRegEffDate());
				} else {
					logger.error(
							"setPostDatedEffDataForLIG() part1 - Effective date not found for LIG and  Run_id  " + key);
				}
			} else {

				int counter = 0;
				for (PriceExportDTO item : ligItems.getValue()) {
					if (counter == 0) {
						effDate = item.getRegEffDate();
					} else {
						if (effDate != "" && item.getRegEffDate() != null && item.getRegEffDate() != "")
							effDate = DateUtil.getRecentDate(effDate, item.getRegEffDate());
					}
					counter++;
				}
				if (effDate != null) {
					ligItemMap.put(key, effDate);
				} else {
					logger.error(
							"setPostDatedEffDataForLIG() part1 - Effective date not found for LIG and  Run_id " + key);
				}

			}
		}

		logger.info("setPostDatedEffDataForLIG()- # items in LIG map for updating the regEffDate :" + ligItemMap.size());
		if (ligItemMap.size() > 0) {
			// Update the postdated regEffective Date for LIG row
			priceExportDAOV2.updateLIGItemRegEffectiveDate(conn, ligItemMap);
		}

	}

	public String getPriceExportTime() {
		return priceExportTime;
	}

	public void setPriceExportTime(String priceExportTime) {
		this.priceExportTime = priceExportTime;
	}

}
