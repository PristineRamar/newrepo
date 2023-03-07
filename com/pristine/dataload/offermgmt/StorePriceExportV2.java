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
import java.time.DayOfWeek;
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

import com.pristine.dao.DBManager;
import com.pristine.dao.ItemLoaderDAO;
import com.pristine.dao.RetailCalendarDAO;import com.pristine.dao.RetailPriceDAO;
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
import com.pristine.dto.offermgmt.PRItemDTO;
import com.pristine.exception.GeneralException;
import com.pristine.lookup.AuditTrailStatusLookup;
import com.pristine.lookup.AuditTrailTypeLookup;
import com.pristine.service.offermgmt.AuditTrailService;
import com.pristine.service.offermgmt.NotificationService;
import com.pristine.service.offermgmt.StorePriceExportAZServiceV2;
import com.pristine.util.Constants;
import com.pristine.util.DateUtil;
import com.pristine.util.PrestoUtil;
import com.pristine.util.PristineDBUtil;
import com.pristine.util.PropertyManager;
import com.pristine.util.offermgmt.PRCommonUtil;
import com.pristine.util.offermgmt.PRConstants;

/**
 * 
 * @author Kirthi
 * 
 * This program exports approved prices against zone/stores, exporting clearance/Emergency prices, 
 * exporting store lock and expiry items and exporting test zone prices against stores.
 *
 */
public class StorePriceExportV2 {
	
	private static Logger logger = Logger.getLogger("StorePriceExportV2");

	private Connection conn = null;
	
	PriceExportDAOV2 priceExportDao = new PriceExportDAOV2();
	PriceDataLoad priceDataDao = new PriceDataLoad();
	ItemLoaderDAO itemLoaderDAO = new ItemLoaderDAO();
	
	ItemDAO itemDao = new ItemDAO();
	
	StorePriceExportAZServiceV2 storePriceExportAZSerive = new StorePriceExportAZServiceV2();
	
	HashMap<String, Integer> storeZoneMap = new HashMap<>();
	List<ZoneDTO> allZoneData = new ArrayList<>();
	List<ZoneDTO> azZoneData = new ArrayList<>();
	List<StoreDTO> storeData = new ArrayList<>();
	HashMap<String, Integer> itemData = new HashMap<>();
	List<PRItemDTO> itemRUData = new ArrayList<>();
	
	List<PRItemDTO> dataToClearDuplicate = new ArrayList<>();
	
	boolean emergency = false;
	boolean noRunIds = false;
	boolean noCandidates = false;
	
	List<PRItemDTO> salesFloorFiltered = new ArrayList<>();
	
	LocalDate today = LocalDate.now();
	DateTimeFormatter dateFormatter = DateTimeFormatter.ofPattern("MM/dd/yyyy hh:mm:ss"); 	

	String saleFloorLimitStr = null;
	String globalZone = null;
	String virtualZoneNum = null;
	String excludeZonesForVirtualZone = null;
	String rootPath = null;		
	String fileNameConfig = null;	

	List<Integer> excludeZoneIdForVirtualZone = new ArrayList<Integer>();
		
	public StorePriceExportV2() {

		if (conn == null) {
			try {
				conn = DBManager.getConnection();
			} catch (GeneralException exe) {
				logger.error("Error while connecting to DB:" + exe);
				System.exit(1);
			}
		}
	}
	
	public static void main(String[] args) {

		PropertyManager.initialize("analysis.properties");
		PropertyConfigurator.configure("log4j-price-export.properties");
		StorePriceExportV2 export = new StorePriceExportV2();
		String priceExportType = "";
		

		// setting up the inputs
		for (int i = 0; i < args.length; i++) {
			String arg = args[i];
			if (arg.startsWith("PRICE_EXPORT_TYPE")) {
				priceExportType = arg.substring("PRICE_EXPORT_TYPE=".length());
			}
		}

		logger.info("*****************************************************************");		

		boolean processExport = validateExportType(priceExportType);
		if(processExport) {
			export.exportPrice(priceExportType);
		}
		else {
			logger.error("The provided batch parameter is wrong!!");
			logger.info("Choose among: E / S / H / ES / EH / SH / ESH");
			System.exit(1);
		}
		
		logger.info("*****************************************************************");
	}

	private void exportPrice(String priceExportType) {		

		//********************************** DATA FROM PROPERTIES **************************************************//

		saleFloorLimitStr = PropertyManager.getProperty("SALE_FLOORITEM_LIMIT");
		globalZone = PropertyManager.getProperty("NATIONAL_LEVEL_ZONE");
		virtualZoneNum = PropertyManager.getProperty("VIRTUAL_ZONE");
		excludeZonesForVirtualZone = PropertyManager.getProperty("EXCLUDE_ZONE_FOR_VIRTUAL_ZONE");
		rootPath = PropertyManager.getProperty("FILEPATH");
		fileNameConfig = PropertyManager.getProperty("FILENAME");

		// ************************************** ENDS *************************************************************//

		List<PRItemDTO> eCItemsToExport = new ArrayList<>();
		List<PRItemDTO> exportItemsToUpdate = new ArrayList<>();

		List<String> itemStoreCombinationsFromPriceTest = new ArrayList<>();
		List<PRItemDTO> priceTestItemZoneLevelEntries = new ArrayList<>();	

		String todaysDate = "";
		DateTimeFormatter dtf = DateTimeFormatter.ofPattern("MM/dd/yyyy");
		LocalDateTime now = LocalDateTime.now();
		todaysDate = dtf.format(now);

		DateFormat format = new SimpleDateFormat("MMddyyyy");
		String timeStamp = format.format(new Date());
		String fileName = fileNameConfig + "-" + timeStamp;
		File sourceFile = new File(rootPath + "/" + fileName + ".txt");
		File targetFile = new File(rootPath + "/" + fileName + ".txt." + Constants.DONE);

		// ************************************* FLOW OF THE PROGRAM ************************************************//

		try {

			// populate caches for price export
			initializeZoneAndStoreData();

			// populate approved run ids
			HashMap<String, List<Long>> runIdMap = populateRunIds(priceExportType, todaysDate);

			// Populate approved Items from normal/emergency Run ids
			// If global zone recommendation is available aggregate the data for all zones
			List<PRItemDTO> approvedItemList = populateApprovedItemList(priceExportType, runIdMap, todaysDate);		
			
			logger.debug("distinct approved items: " + (approvedItemList
					.stream().filter(e -> (e.getRetailerItemCode() != null || !e.getRetailerItemCode().isEmpty()))
					.collect(Collectors.groupingBy(PRItemDTO::getRetailerItemCode))).size());

			// populate emergency and clearance items in item list
			List<PRItemDTO> emergencyAndClearanceItems = populateEmergencyAndClearancePrices(priceExportType,
					eCItemsToExport, approvedItemList, now);

			// Aggregating zone 30
			populateVirtualZone(approvedItemList); 

			// Filter sales floor items
			List<PRItemDTO> exportItemList = populateExportItemsBasedOnCEitems(priceExportType,
					emergencyAndClearanceItems, eCItemsToExport, approvedItemList, runIdMap, todaysDate);

			// populate test zone data from test zone run ids
			List<PRItemDTO> priceTestItemStoreLevelEntries = populateTestZonePrices(priceExportType, exportItemList,
					runIdMap, itemStoreCombinationsFromPriceTest, priceTestItemZoneLevelEntries, eCItemsToExport,
					todaysDate);
			logger.debug("# priceTestItemStoreLevelEntries: " + priceTestItemStoreLevelEntries.size());
			
			// list to update status
			exportItemsToUpdate = populateExportListToUpdateStatus(exportItemList/*, priceTestItemStoreLevelEntries*/);

			// PUT AUDIT LOG ON COUNT OF ITEMS
			auditLogs(exportItemList);
			
			// populate store lock items
			// Audit Store lock items from check list 
			List<PRItemDTO> storeLockItems = populateStoreLockItemList(priceExportType,
					itemStoreCombinationsFromPriceTest, exportItemsToUpdate, exportItemList);
						
			try {
			// CSV writer			
			writeExportDataInCSV(exportItemList, fileName, sourceFile, targetFile);	

			// populate expiry items
			List<PRItemDTO> expiryItems = populateExpiryItemList(priceExportType, itemStoreCombinationsFromPriceTest,
					exportItemsToUpdate, fileName, sourceFile, targetFile);
				
			//rename txt file to txt.done file
			rename(sourceFile, targetFile);		

			// status update for exported items
			updateExportStatus(exportItemsToUpdate, runIdMap);

			// populate data when no run id/approved data available
			changeEffDataIfNoApprovedData(priceExportType, emergencyAndClearanceItems, runIdMap);

			// status update in notification and audit service
			updateExportStatusInNotificationAndAuditTrial(runIdMap, exportItemsToUpdate);
			} catch (Exception e1) {
				PristineDBUtil.rollbackTransaction(conn, "Price export update status");
			}

		} catch (Exception | GeneralException e) {
			logger.error("exportPrice() - Error while exporting prices", e);
			PristineDBUtil.rollbackTransaction(conn, "Price export update status");
			// clearErrorExportFiles(rootPath);
		} finally {
			PristineDBUtil.close(conn);
		}
		

		//***************************************** ENDS *******************************************//
				
	}

	
	private List<PRItemDTO> populateExportListToUpdateStatus(List<PRItemDTO> exportItemList/*,
			List<PRItemDTO> priceTestItemStoreLevelEntries*/) {
		List<PRItemDTO> exportItemsToUpdate = new ArrayList<>();
		
		if (exportItemList.size() > 0) {
			List<PRItemDTO> approvalItems = exportItemList.stream()
					.filter(e -> (e.getPriceCheckListTypeId() == null || e.getPriceCheckListTypeId() == 0))
					.collect(Collectors.toList());
			logger.debug("# in approvalItems items: " + approvalItems.size());

			Map<Integer, List<PRItemDTO>> distinctApprovedItems = exportItemList.stream()
					.collect(Collectors.groupingBy(PRItemDTO::getItemCode));

			if (approvalItems.size() > 0) {
				exportItemsToUpdate.addAll(approvalItems);
			}

			if (dataToClearDuplicate != null && dataToClearDuplicate.size() > 0) {
				for (PRItemDTO dataToClear : dataToClearDuplicate) {
					if (distinctApprovedItems.containsKey(dataToClear.getItemCode())) {
						exportItemsToUpdate.add(dataToClear);
					}
				}
			}

		}
		logger.debug("# in exportItemsToUpdate: " + exportItemsToUpdate.size());
		return exportItemsToUpdate;
	}

	private void auditLogs(List<PRItemDTO> exportItemList) {
		/*
		 * 1. Summary of what RU’s have been exported in a given day. 2. Number of
		 * distinct items in the export vs item count in RU on Dashboard. 3. Number of
		 * item-store records for ‘A’ compared to the item list for approved RU. 4.
		 * Number of item-store records for ‘D’ compared to the item list that has
		 * ended. Also for the ‘D’ records in the export file, need hierarchy
		 * information as that is not being populated in the export file. 5. Distinct
		 * dates in a given export file for that day.
		 */
		Set<String> uniqueRU = new HashSet<>();
		Set<String> uniqueEffDate = new HashSet<>();
		if (exportItemList.size() > 0) {			
			for(PRItemDTO data : exportItemList) {
				uniqueRU.add(data.getRecommendationUnit());
				uniqueEffDate.add(data.getRegEffDate());
			}

			HashMap<String, List<PRItemDTO>> exportDataByRetaileritem = (HashMap<String, List<PRItemDTO>>) exportItemList
					.stream().filter(e -> (e.getRetailerItemCode() != null || !e.getRetailerItemCode().isEmpty()))
					.collect(Collectors.groupingBy(PRItemDTO::getRetailerItemCode));


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

	private void initializeZoneAndStoreData() {
		logger.info("initializeZoneAndStoreData() - Initializing zone and store data.");
		boolean convertToUppercase = true;
		try {
			storeZoneMap = new RetailPriceZoneDAO().getStoreZoneMapping(conn);
			storeData = priceExportDao.getStoreData(conn);
			allZoneData = priceExportDao.getZoneData(conn,false);
			azZoneData = priceExportDao.getZoneData(conn,true);
			itemData = itemLoaderDAO.getTwoColumnsCache(conn, "ITEM_CODE", "RETAILER_ITEM_CODE", "ITEM_LOOKUP",
					convertToUppercase);
			itemRUData = itemDao.getRUofItems(conn);

			String[] arrayOfExcludeZone = excludeZonesForVirtualZone.split(",");
			List<String> excludeStoreListForVitualZone = Arrays.asList(arrayOfExcludeZone);
			for (String zoneNum : excludeStoreListForVitualZone) {
				HashMap<String, List<ZoneDTO>> zoneMap = (HashMap<String, List<ZoneDTO>>) allZoneData.stream()
						.collect(Collectors.groupingBy(ZoneDTO :: getZnNo));
				excludeZoneIdForVirtualZone.add(zoneMap.get(zoneNum).get(0).getZnId());
			}
			
		} catch (GeneralException e) {
			logger.info("initializeZoneAndStoreData() - Error while initializing zone and store data " + e);
		}
	}

	private void populateVirtualZone(List<PRItemDTO> approvedItemList) {
		try {
			// populate virtual zone data
			logger.info("populateApprovedItemList() - Setting virtual zone: " + virtualZoneNum + " starts");
			logger.info("populateApprovedItemList() - # of items before setting virtual zone: " + approvedItemList.size());
			
			// populate virtual zone data ends	
			setMaxPriceForVirtualZone(approvedItemList, virtualZoneNum);								
			logger.info("populateApprovedItemList() - # of items after setting virtual zone: " + approvedItemList.size());
			
			} catch (GeneralException e) {
				logger.info("populateVirtualZone() - Error while populating virtual zone: " + e);
			}
			
	}

	private void changeEffDataIfNoApprovedData(String input, List<PRItemDTO> emergencyAndClearanceItems,
			HashMap<String, List<Long>> runIdMap) {

		//if (noRunIds || noCandidates) {

			logger.info("changeEffDataIfNoApprovedData() - postponding effective date for remaining items");

			try {
				List<Long> runIdList = new ArrayList<>();
				if (runIdMap.size() > 0) {
					
					if(runIdMap.get("N") != null || runIdMap.get("N").size() > 0) {
						runIdList.addAll(runIdMap.get("N"));
					}
					if(runIdMap.get("T") != null || runIdMap.get("T").size() > 0) {
						runIdList.addAll(runIdMap.get("T"));
					}		

					postEffectiveDateChange(input, emergencyAndClearanceItems, runIdList, getFilteredSFItems());

					PristineDBUtil.commitTransaction(conn, "Price export update status");
				}

			} catch (GeneralException | Exception e) {
				logger.info("changeEffDataIfNoApprovedData() - Error while postponding effective date for remaining items: " + e);
			}
		//}
	}

	private void rename(File src, File target) throws Exception {
		if (src.equals(target)) {
			logger.warn("rename() - Source and target files are the same [" + src + "]. Skipping.");
			return;
		}

		if (src.exists()) {

			boolean result = src.renameTo(target);

			if (result) {
				logger.info("Renamed file [" + src + "] to [" + target + "]");
				logger.debug("1kb file renamed");
			} else {
				logger.warn("rename() - Failed to rename file [" + src + "] as [" + target + "].");
			}
		} else {
			throw new Exception("File [" + src + "] does not exist.");
		}

	}

		// write into file
		private void writeExportDataInCSV(List<PRItemDTO> finalExportList,
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
				for (PRItemDTO item : finalExportList) {

					if(item.getStoreNo() == null) {
						pw.print("");				
					}else {
						pw.print(item.getStoreNo());
					}
					pw.print(separtor);
					if (item.getPriceZoneNo() == null) {
						pw.print("");
					} else {
						pw.print(item.getPriceZoneNo());
					}
					pw.print(separtor);

					if (item.getStoreLockExpiryFlag().equals(Constants.EXPORT_DELETE_DATA)) {
						pw.print("");
					} else {
						if (item.getZoneName() == null) {
							pw.print("");
						} else {
							pw.print(item.getZoneName());
						}
					}
					pw.print(separtor);

					if (item.getStoreLockExpiryFlag().equals(Constants.EXPORT_DELETE_DATA)) {
						pw.print("");
					} else {
						pw.print(countryCode);
					}
					pw.print(separtor);

					if (item.getStoreLockExpiryFlag().equals(Constants.EXPORT_DELETE_DATA)) {
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

					if (item.getStoreLockExpiryFlag().equals(Constants.EXPORT_DELETE_DATA)) {
						pw.print("");
					} else {
						if (item.getPartNumber() == null) {
							pw.print("");
						} else {
							pw.print(item.getPartNumber());
						}
					}
					pw.print(separtor);

					if (item.getStoreLockExpiryFlag().equals(Constants.EXPORT_DELETE_DATA)) {
						pw.print("");
					} else {
						if (item.getItemType() == null) {
							pw.print("");
						} else {
							pw.print(item.getItemType());
						}

					}
					pw.print(separtor);

					if (item.getStoreLockExpiryFlag().equals(Constants.EXPORT_DELETE_DATA)) {
						pw.print(format.format(date));
					} else {
						if (item.getRegEffDate() == null) {
							pw.print("");
						} else {
							pw.print(item.getRegEffDate());
						}
					}
					pw.print(separtor);

					if (item.getStoreLockExpiryFlag().equals(Constants.EXPORT_DELETE_DATA)) {
						pw.print("0.00");
					} else {
						if(item.getVdpRetail() == null) {
							pw.print("0.00");
						}else {
						pw.print(item.getVdpRetail());
						}
					}
					pw.print(separtor);

					if (item.getStoreLockExpiryFlag().equals(Constants.EXPORT_DELETE_DATA)) {
						pw.print("0.00");
					} else {
						if (item.getCoreRetail() == null) {
							pw.print("0.00");
						} else {
							pw.print(item.getCoreRetail());
						}
					}
					pw.print(separtor);

					if (item.getStoreLockExpiryFlag().equals(Constants.EXPORT_DELETE_DATA)) {
						pw.print("");
					} else {
						if(item.getVdpRetail() == null) {
							pw.print("0.00");
						}else {
						pw.print(item.getVdpRetail());
						}
					}
					pw.print(separtor);

					if (item.getStoreLockExpiryFlag().equals(Constants.EXPORT_DELETE_DATA)) {
						pw.print("");
					} else {
						if (item.getChildLocationLevelId() < 0) {
							pw.print("");
						} else {
							pw.print(item.getChildLocationLevelId());
						}
					}
					pw.print(separtor);

					if (item.getStoreLockExpiryFlag().equals(Constants.EXPORT_DELETE_DATA)) {
						pw.print("");
					} else {
						if (item.getDiffRetail() == null) {
							pw.print("0.00");
						} else {
							pw.print(item.getDiffRetail());
						}
					}
					pw.print(separtor);

					if (item.getStoreLockExpiryFlag().equals(Constants.EXPORT_DELETE_DATA)) {
						pw.print("");
					} else {
						if (item.getApprovedBy() == null) {
							pw.print("");
						} else {
							pw.print(item.getApprovedBy());// approver
						}
					}
					pw.print(separtor);

					if (item.getStoreLockExpiryFlag().equals(Constants.EXPORT_DELETE_DATA)) {
						pw.print("");
					} else {
						if (item.getApproverName() == null) {
							pw.print("");
						} else {
							pw.print(item.getApproverName());
						}
					}
					pw.print(separtor);

					if (item.getStoreLockExpiryFlag().equals(Constants.EXPORT_DELETE_DATA)) {
						pw.print("");
					} else {
						if (item.getPriceCheckListTypeId() == null) {
							if (item.getPriceExportType().equals(Constants.EMERGENCY)) {
								pw.print(Constants.EMERGENCY);
							} else {
								pw.print(""); // CE FLAG
							}						
						} else if (item.getPriceCheckListTypeId() == Integer.parseInt(Constants.EMERGENCY_LIST_TYPE)) {
							pw.print(Constants.EMERGENCY);
						} else if (item.getPriceCheckListTypeId() == Integer.parseInt(Constants.CLEARANCE_LIST_TYPE)) {
							pw.print(Constants.CLEARANCE);
						}	else {
							//for store lock
							pw.print("");
						}				
					}
					pw.print(separtor);

					if (item.getStoreLockExpiryFlag().equals(Constants.EXPORT_DELETE_DATA) || item.getStoreLockExpiryFlag().equals(Constants.EXPORT_ADD_DATA)) {
						pw.print(item.getStoreLockExpiryFlag());
					} else {
						pw.print("");
					}
					pw.println();

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

	private void postEffectiveDateChange(String input, List<PRItemDTO> ecItems, List<Long> runIdList, List<PRItemDTO> salesFloorFiltered) throws GeneralException {
		//post effective date change
		//if (today.getDayOfWeek().equals(DayOfWeek.MONDAY) || today.getDayOfWeek().equals(DayOfWeek.TUESDAY)
			//	|| today.getDayOfWeek().equals(DayOfWeek.WEDNESDAY)) {
			
			//if we need to run Clearance on Thursday/ Friday -- as currently in production
			if (today.getDayOfWeek().equals(DayOfWeek.MONDAY) || today.getDayOfWeek().equals(DayOfWeek.TUESDAY)
					|| today.getDayOfWeek().equals(DayOfWeek.WEDNESDAY) || today.getDayOfWeek().equals(DayOfWeek.THURSDAY)) {
			
			if ((input.equals(Constants.EMERGENCY_OR_HARDPART) && emergency)
					|| (input.equals(Constants.ALL_SALESFLOOR_AND_HARDPART_AND_EMERGENCY) && emergency)
					|| (ecItems.size() > 0)) {
				logger.info("postEffectiveDateChange() - Exchange Effective date for HardPart Items when price "
						+ "export Type is : " + input);
				List<PRItemDTO> leftoutHardPartItems = priceExportDao.getHardpartItemLeftout(conn,
						runIdList);
				if (leftoutHardPartItems.size() < 0 || leftoutHardPartItems.size() == 0) {
					logger.info("postEffectiveDateChange() - No hard part items left on " + today + " batch run..");
				} else {
					logger.info("postEffectiveDateChange() - # of left out hardpart item on " + today + " batch run is: "
							+ leftoutHardPartItems.size());
					priceExportDao.changeEffectiveDate(conn, leftoutHardPartItems, 1);
				}

			}
		}
		
		if(input.equals(Constants.BOTH_SALESFLOOR_AND_HARDPART) || input.equals(Constants.SALE_FLOOR_ITEMS)
				|| (input.equals(Constants.EMERGENCY_OR_SALESFLOOR))
				|| (input.equals(Constants.ALL_SALESFLOOR_AND_HARDPART_AND_EMERGENCY) && emergency)) {
			
			logger.info("postEffectiveDateChange() - Exchange Effective date for SalesFloor Items when price "
					+ "export Type is : " + input);
			List<PRItemDTO> leftoutSalesFloorItems = priceExportDao.getSalesFloorItemsLeftout(conn, salesFloorFiltered);
			if(leftoutSalesFloorItems.size() < 0) {
				logger.info("postEffectiveDateChange() - No sales floor items left on " + today + "batch run..");
			}
			else {
				logger.info("postEffectiveDateChange() - # of left out salesfloor item on " + today + "batch run is: "
						+ leftoutSalesFloorItems.size());
				priceExportDao.changeEffectiveDate(conn, leftoutSalesFloorItems, 7);
			}
		}
		
	} 

	
	
	private void updateExportStatusInNotificationAndAuditTrial( 
			HashMap<String,List<Long>> runIdMap, List<PRItemDTO> exportItemsToUpdate) {
		
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
					addNotification(conn, runIdList, emergency, priceExportDao);

					logger.info("exportPrice() - Audit trial for Price export status");
					auditTrialExportData(conn, runIdList, emergency, priceExportDao);
				}

				PristineDBUtil.commitTransaction(conn, "Price export update status");
			} catch (SQLException | GeneralException e) {
				logger.info(
						"updateExportStatusInNotificationAndAuditTrial() - Error while updating export status in notification and audit trial: "
								+ e);
			}
		}
	}

	private void auditTrialStoreLockListRecords(Connection conn, List<PRItemDTO> storeLockItems) throws GeneralException {

		HashMap<String, List<PRItemDTO>> locationProductMap = new HashMap<>();

		for (PRItemDTO plObj : storeLockItems) {
			List<PRItemDTO> tempList = new ArrayList<>();
			if (locationProductMap.containsKey(plObj.getRecommendationUnitId() + ";" + plObj.getLocationId())) {
				tempList = locationProductMap.get(plObj.getRecommendationUnitId() + ";" + plObj.getLocationId());
			}
			tempList.add(plObj);
			locationProductMap.put(plObj.getRecommendationUnitId() + ";" + plObj.getLocationId(), tempList);
		}

		for (Map.Entry<String, List<PRItemDTO>> listEntry : locationProductMap.entrySet()) {
			int location = Integer.parseInt(listEntry.getKey().split(";")[1]);
			int ru = Integer.parseInt(listEntry.getKey().split(";")[0]);

			Set<Integer> distinctAItems = new HashSet<>();
			Set<String> distinctAStores = new HashSet<>();

			for (PRItemDTO plObj : listEntry.getValue()) {
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
	
	private void updateExportStatus(List<PRItemDTO> candidateItemList, HashMap<String,List<Long>> runIdMap) {
		
		List<Long> runIdList = new ArrayList<>();
		if(runIdMap.get("N") == null || runIdMap.get("N").size()<0) {
			logger.debug("No regular zone items to update status");
		}else {
			runIdList.addAll(runIdMap.get("N"));
		}
		if(runIdMap.get("T") == null || runIdMap.get("T").size()<0) {
			logger.debug("No test zone items to update status");
		}
		else {
			runIdList.addAll(runIdMap.get("T"));
		}
		
		if (candidateItemList.size() > 0) {
			logger.info("# of data to update status: " + candidateItemList.size());
			try {
				logger.info("exportPrice() - Deleting the processed data..");
				priceExportDao.deleteProcessedRunIds(conn, candidateItemList);

				logger.info("exportPrice() - Status update for export items");
				priceExportDao.updateExportItems(conn, candidateItemList);

				priceExportDao.updateExportLigItems(conn, candidateItemList);

				priceExportDao.updateExportStatus(conn, candidateItemList, runIdList);

				priceExportDao.insertExportStatus(conn, runIdList, candidateItemList);

				priceExportDao.insertExportTrackId(conn, candidateItemList);

				PristineDBUtil.commitTransaction(conn, "Price export update status");
			} catch (GeneralException e) {
				logger.info("updateExportStatus() Error while updating export status: " + e);
			}
		}
		
	}

	public List<PRItemDTO> populateExpiryItemList(String input, List<String> itemStoreCombinationsFromPriceTest, 
			List<PRItemDTO> candidateItemList, String fileName, File sourceFile, File targetFile) {
		logger.info("populateExpiryItemList() - Populating expiry items for price export.");
		HashMap<String,PRItemDTO> expiryItems = new HashMap<>();
		
		HashMap<String, List<PRItemDTO>> ruItemMap = (HashMap<String, List<PRItemDTO>>) itemRUData.stream()
				.collect(Collectors.groupingBy(PRItemDTO::getRetailerItemCode));
		
		List<PRItemDTO> exportItems = new ArrayList<>();
		try {
			boolean noDataInQueue = false;
			if(candidateItemList == null || candidateItemList.size() <= 0) {
				noDataInQueue = true;
			}
			
			expiryItems = getStoreLockExpiryItems(candidateItemList, input, noDataInQueue, itemStoreCombinationsFromPriceTest, 
					ruItemMap, exportItems, fileName, sourceFile, targetFile);
			
			logger.info("# in expiryItems: " + expiryItems.size());
			
			//Added on 08/18/2021
			//Audit trial for D records - PROM-2248		
			//Populate audit for count of distinct items, distinct stores, export time for each location-RU combination
			if (expiryItems.size() > 0) {
				logger.debug("populateExpiryItemList() - Auditing D records`");
				try {
					for (Map.Entry<String, PRItemDTO> listEntry : expiryItems.entrySet()) {
						int location = Integer.parseInt(listEntry.getKey().split(";")[1]);
						int ru = Integer.parseInt(listEntry.getKey().split(";")[0]);

						int distinctDItems = listEntry.getValue().getItemCount().size();
						int distinctDStores = listEntry.getValue().getStoreCount().size();
						int calId = listEntry.getValue().getCalendarId();

						callAuditTrail(Constants.STORE_LIST_LEVEL_ID, location, Constants.RECOMMENDATIONUNIT, ru,
								AuditTrailTypeLookup.RECOMMENDATION.getAuditTrailTypeId(), 0, 0L,
								PRConstants.BATCH_USER, AuditTrailStatusLookup.AUDIT_TYPE.getAuditTrailTypeId(),
								AuditTrailStatusLookup.AUDIT_SUB_TYPE_EXPORT.getAuditTrailTypeId(),
								AuditTrailStatusLookup.SUB_STATUS_TYPE_D_RECORDS.getAuditTrailTypeId(), distinctDItems,
								distinctDStores, calId);
					}
				}catch(Exception e) {
					logger.info("populateExpiryItemList() - Error while Auditing D records: "  +e);
				}
			}
		}
		catch(GeneralException e) {
			logger.info("populateExpiryItemList() - Error while populating expiry items: " + e);
		}
		
		return exportItems;
			
	}

	private HashMap<String, PRItemDTO> getStoreLockExpiryItems(List<PRItemDTO> candidateItemList,String input, boolean noDataInQueue,
			List<String> itemStoreCombinationsFromPriceTest, HashMap<String, List<PRItemDTO>> ruItemMap, 
			List<PRItemDTO> exportItemList, String fileName, File sourceFile, File targetFile) throws GeneralException {
		
		List<PRItemDTO> expiryItemsToExport = new ArrayList<>();
		HashMap<String, PRItemDTO> locationProductMapForAudit = new HashMap<>();
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
		HashMap<Integer, List<PRItemDTO>> expiryItems = priceExportDao.setPriceForExpiryItemsV3(false, conn,
				input);
		
		//Populate fields for D records
		if (expiryItems.size() > 0) {
			int expiryItemCount = 0;
			logger.debug("setPriceForExpiryItems() - # in expiryItems: " + expiryItems.size());
			
			for (Map.Entry<Integer, List<PRItemDTO>> excludeStoreValue : expiryItems.entrySet()) {
				
				int itemCode = excludeStoreValue.getKey();
				String retItem = PrestoUtil.getKey(itemData,itemCode);
				for (PRItemDTO items : excludeStoreValue.getValue()) {

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
							PRItemDTO storeInstanceClone = null;
							if (noDataInQueue) {
								storeInstanceClone = new PRItemDTO();
							} else {
								storeInstanceClone = (PRItemDTO) candidateItemList.get(0).clone();
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
							List<PRItemDTO> itemList = ruItemMap.get(storeInstanceClone.getRetailerItemCode());
							if (itemList != null && !itemList.isEmpty()) {
								storeInstanceClone.setRecommendationUnit(itemList.get(0).getRecommendationUnit());
								storeInstanceClone.setRecommendationUnitId(itemList.get(0).getRecommendationUnitId());
							}
							expiryItemsToExport.add(storeInstanceClone);
							

						} catch (Exception e) {
							logger.error("setPriceForExpiryItems() - Error setting price for expiry items: Store - "
									+ items.getStoreNo() + ", Item: " + items.getItemCode() + e);
						}
						
						HashMap<String,String> storeNumIdMap = new HashMap<>();
						for(StoreDTO stores : storeData) {
							storeNumIDMap.put(stores.strNum, String.valueOf(stores.strId));
						}
									
						if(itemStoreCombinationsFromPriceTest.size()>0) {
							if (expiryItemsToExport.size() > 0) {
								for (PRItemDTO S : expiryItemsToExport) {
									String storeId = storeNumIdMap.get(S.getStoreNo());
									String key = S.getItemCode() + "_" + storeId;
									if (!itemStoreCombinationsFromPriceTest.contains(key)) {
										exportItemList.add(S);
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
							PRItemDTO storeInstanceClone = null;
							if (noDataInQueue) {
								storeInstanceClone = new PRItemDTO();
							} else {
								storeInstanceClone = (PRItemDTO) candidateItemList.get(0).clone();
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
							List<PRItemDTO> itemList = ruItemMap.get(storeInstanceClone.getRetailerItemCode());
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
			
			HashMap<String,String> storeNumIdMap = new HashMap<>();
			for(StoreDTO stores : storeData) {
				storeNumIDMap.put(stores.strNum, String.valueOf(stores.strId));
			}
						
			if(itemStoreCombinationsFromPriceTest.size()>0) {
				if (expiryItemsToExport.size() > 0) {
					for (PRItemDTO S : expiryItemsToExport) {
						String storeId = storeNumIdMap.get(S.getStoreNo());
						String key = S.getItemCode() + "_" + storeId;
						if (!itemStoreCombinationsFromPriceTest.contains(key)) {
							exportItemList.add(S);
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

	private void populateDrecordForAudit(List<PRItemDTO> exportItemList, HashMap<String, PRItemDTO> locationProductMapForAudit) {
	
		try {
		HashMap<String, List<PRItemDTO>> locationProductMap = new HashMap<>();
		for (PRItemDTO data : exportItemList) {
			List<PRItemDTO> tempList = new ArrayList<>();
			if (locationProductMap.containsKey(data.getRecommendationUnitId() + ";" + data.getLocationId())) {				
				tempList = locationProductMap.get(data.getRecommendationUnitId() + ";" + data.getLocationId());				
			}
			tempList.add(data);
			locationProductMap.put(data.getRecommendationUnitId() + ";" + data.getLocationId(),
						tempList);			
		}
		logger.info("# in locationProductMap " + locationProductMap.size());
		for (Map.Entry<String, List<PRItemDTO>> listEntry : locationProductMap.entrySet()) {

			int calId = listEntry.getValue().get(0).getCalendarId();
			Set<String> distinctStores = new HashSet<>();
			Set<String> distinctItems = new HashSet<>();
			for (PRItemDTO plObj : listEntry.getValue()) {
				distinctStores.add(plObj.getStoreNo());
				distinctItems.add(plObj.getRetailerItemCode());
			}

			for (PRItemDTO plObj : listEntry.getValue()) {

				if (locationProductMapForAudit.containsKey(listEntry.getKey())) {
					PRItemDTO value = locationProductMapForAudit.get(listEntry.getKey());
					value.setCalendarId(calId);
					value.getItemCount().add(plObj.getStoreNo());
					value.getItemCount().add(plObj.getRetailerItemCode());
					//existingStores.add(plObj.getStoreNo());
					//existingItems.add(plObj.getRetailerItemCode());
					
					locationProductMapForAudit.put(listEntry.getKey(),value);

				} else {
					PRItemDTO runningObj = new PRItemDTO();
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

	private void updateExpiryItemsStatus(List<PRItemDTO> expiryItems) throws GeneralException {
		/*List<PRItemDTO> expiryListItems = new ArrayList<PRItemDTO>();
		expiryItems.forEach((itemCode, expiryList) -> {
			expiryList.forEach(item -> {
				item.setItemCode(itemCode);
			});
			expiryListItems.addAll(expiryList);
		});*/

		List<PRItemDTO> expiryItemListAtStoreList = expiryItems.stream().filter(e -> e.isStoreListExpiry())
				.collect(Collectors.toList());
		List<PRItemDTO> expiryItemListRegular = expiryItems.stream().filter(e -> !e.isStoreListExpiry())
				.collect(Collectors.toList());

		logger.debug("# in expiryItemListRegular: " + expiryItemListRegular.size());
		logger.debug("# in expiryItemListAtStoreList: " + expiryItemListAtStoreList.size());
		if (expiryItemListRegular.size() > 0) {
			priceExportDao.updateExpiryExportFlagForRegularItemListV3(conn, expiryItemListRegular);
		}
		if (expiryItemListAtStoreList.size() > 0) {
			priceExportDao.updateExpiryExportFlagForStoreListV3(conn, expiryItemListAtStoreList);
		}
	}

	public List<PRItemDTO> populateExportItemsBasedOnCEitems(String input, List<PRItemDTO> emergencyAndClearanceItems, 
			List<PRItemDTO> eCItemsToExport, List<PRItemDTO> approvedItemList, HashMap<String, List<Long>> runIdMap, String todaysDate) {		
		
		//populate candidate items based on clearance data
		logger.info("populateExportItemsBasedOnCEitems() - populating export items based on EC items ends ");
				
		List<PRItemDTO> emergencyItemsFromItemList = new ArrayList<PRItemDTO>();
		List<PRItemDTO> clearenceItemsFromItemList = new ArrayList<PRItemDTO>();
		
		if (emergencyAndClearanceItems.size() > 0) {
			clearenceItemsFromItemList = emergencyAndClearanceItems.stream()
					.filter(e -> e.getPriceCheckListTypeId() == Integer.parseInt(Constants.CLEARANCE_LIST_TYPE))
					.collect(Collectors.toList());
			emergencyItemsFromItemList = emergencyAndClearanceItems.stream()
					.filter(e -> e.getPriceCheckListTypeId() == Integer.parseInt(Constants.EMERGENCY_LIST_TYPE))
					.collect(Collectors.toList());
		}

		List<PRItemDTO> candidateItemList = populateCandidateItemsBasedOnECItems(approvedItemList, emergencyItemsFromItemList, 
				clearenceItemsFromItemList, emergencyAndClearanceItems, eCItemsToExport, todaysDate);
		
		//populate candidate items based on clearance data ends	
		logger.debug("distinct candidate items: " + (candidateItemList
				.stream().filter(e -> (e.getRetailerItemCode() != null || !e.getRetailerItemCode().isEmpty()))
				.collect(Collectors.groupingBy(PRItemDTO::getRetailerItemCode))).size());
		
		return candidateItemList;
	}

	public void setMaxPriceForVirtualZone(List<PRItemDTO> approvedItemList, String virtualZoneNum) throws GeneralException {
		
		HashMap<Integer, String> zoneIdandNumMap = new HashMap<>();
		for(ZoneDTO zones : allZoneData) {
			zoneIdandNumMap.put(zones.getZnId(), zones.getZnNo());
		}
		
		HashMap<Integer, List<Integer>> missingLocationAndItemMap = new HashMap<>();
		int zoneIdForVirtualZone = PrestoUtil.getKey(priceExportDao.getZoneNoForZoneId(conn), virtualZoneNum);
		List<String> excludeZones = new ArrayList<>();
		for (Integer zoneId : excludeZoneIdForVirtualZone) {
			excludeZones.add(zoneId.toString());
		}

		HashMap<String, Integer> zoneIdMap = priceExportDao.getZoneIdMapForVirtualZone(conn, virtualZoneNum,
				excludeZones);

		String zoneName = priceExportDao.getZoneNameForVirtualZone(conn,virtualZoneNum);
		HashMap<String, RetailPriceDTO> missingZonePriceMap = new HashMap<String, RetailPriceDTO>();
		HashMap<Integer, List<PRItemDTO>> priceForVirtualZone = (HashMap<Integer, List<PRItemDTO>>) approvedItemList
				.stream().distinct().collect(Collectors.groupingBy(PRItemDTO::getItemCode));

		priceForVirtualZone.forEach((itemCode, zoneItemList) -> {

			for (Integer zoneId : zoneIdMap.values()) {
				boolean isLocationFound = false;
				for (PRItemDTO zoneItemApproved : zoneItemList) {

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
					tempList.add(itemCode);
					//logger.debug("priceForVirtualZone() - missingZone" + zoneId + " itemcode: " + itemCode);
					missingLocationAndItemMap.put(zoneId, tempList);
				}
			}
		});

		priceForVirtualZone.forEach((itemCode, zoneItemList) -> {
			try {
				PRItemDTO clonedValue = (PRItemDTO) zoneItemList.get(0).clone();
				// commented on 27/06/2020
				// to include missing zone 5 when zone 1000 is recommended
				if (!clonedValue.isGlobalZoneRecommended()) {
					List<Integer> tempList = new ArrayList<>();
					if (missingLocationAndItemMap.containsKey(zoneIdForVirtualZone)) {
						tempList = missingLocationAndItemMap.get(zoneIdForVirtualZone);
					}
					tempList.add(itemCode);
					missingLocationAndItemMap.put(zoneIdForVirtualZone, tempList);

				}

			} catch (CloneNotSupportedException e) {
				e.printStackTrace();
			}
		});

		RetailCalendarDAO retailCalendarDAO = new RetailCalendarDAO();
		String curWkStartDate = DateUtil.getWeekStartDate(0);

		RetailCalendarDTO retailCalendarDTO = null;
		int dayCalId = 0;
		try {
			retailCalendarDTO = retailCalendarDAO.getCalendarId(conn, curWkStartDate,
					Constants.CALENDAR_DAY);
		
		dayCalId = retailCalendarDTO.getCalendarId();
		//dayCalId = 5005;
		} catch (GeneralException e1) {
			e1.printStackTrace();
		}
		for (Map.Entry<Integer, List<Integer>> missingLocationEntry : missingLocationAndItemMap.entrySet()) {
			try{
			int missingLocation = missingLocationEntry.getKey();
			//logger.debug("missing location: " + missingLocation);

			List<Integer> itemCodeList = missingLocationEntry.getValue();
			List<String> missingZoneItemCodeList = new ArrayList<String>();

			itemCodeList.forEach(itemCode -> {
				missingZoneItemCodeList.add(String.valueOf(itemCode));
			});

			logger.debug("# items in missingZoneItemCodeList: " + missingZoneItemCodeList.size());
			String zoneNum = zoneIdandNumMap.get(missingLocation);

			HashMap<String, RetailPriceDTO> priceForMissingZones = new HashMap<String, RetailPriceDTO>();

			PriceDataLoad priceDataDao = new PriceDataLoad();
			if (missingZoneItemCodeList.size() > 0) {
				priceForMissingZones = priceDataDao.getRetailPrice(conn, zoneNum, missingZoneItemCodeList, dayCalId,
						null, Constants.ZONE_LEVEL_TYPE_ID);
			}
	
			
			if (priceForMissingZones.size() > 0) {
				for (Map.Entry<String, RetailPriceDTO> entryOfMissingZonePrice : priceForMissingZones.entrySet()) {
					RetailPriceDTO valueOfMissingZonePrice = entryOfMissingZonePrice.getValue();
					Double regPrice = (double) valueOfMissingZonePrice.getRegPrice();
					// int regMprice = (int) valueOfMissingZonePrice.getSaleMPrice();
					PRItemDTO priItemDto = new PRItemDTO();
					MultiplePrice currentPrice = new MultiplePrice(1, regPrice);
					priItemDto.setCurrentRegPrice(currentPrice);
					priItemDto.setPriceZoneId(missingLocationEntry.getKey());
					priItemDto.setPriceZoneNo(zoneNum);

					if (zoneNum.equals(virtualZoneNum)) {
						missingZonePriceMap.put(entryOfMissingZonePrice.getKey(), entryOfMissingZonePrice.getValue());
					}

					if (priceForVirtualZone.containsKey(Integer.parseInt(entryOfMissingZonePrice.getKey()))) {
						List<PRItemDTO> existingZones = priceForVirtualZone
								.get(Integer.parseInt(entryOfMissingZonePrice.getKey()));
						existingZones.add(priItemDto);
					} else {
						logger.error("Price is not available in price master table for itemcode: "
								+ entryOfMissingZonePrice.getKey() + " for the calendar id: " + dayCalId);
					}
				}
			}
		} catch (GeneralException e1) {
			e1.printStackTrace();
		}
		}

		priceForVirtualZone.forEach((itemCode, itemList) -> {
			try {
				PRItemDTO virtualZone = (PRItemDTO) itemList.get(0).clone();

				MultiplePrice maxPrice = null;
				MultiplePrice currentPrice = null;
				for (PRItemDTO item : itemList) {

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
				}
				
				if (maxPrice == null) {
					for (PRItemDTO item : itemList) {
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
					//logger.info("diffStatus: " + diffStatus);
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
					approvedItemList.add(virtualZone);
				}
			} catch (CloneNotSupportedException e) {
				e.printStackTrace();
			}
		
		});

	}

	private List<PRItemDTO> populateCandidateItemsBasedOnECItems(List<PRItemDTO> approvedItemList, 
			List<PRItemDTO> emergencyItemsFromItemList, List<PRItemDTO> clearenceItemsFromItemList, 
			List<PRItemDTO> emergencyAndClearanceItems, List<PRItemDTO> ECItemsToExport, String todaysDate) {
		
		
		boolean clearanceInItemList = false;
		boolean emergencyInItemList = false;
		
		HashMap<Integer, List<StoreDTO>> zoneIdAndStoreNumMap = (HashMap<Integer, List<StoreDTO>>) storeData.stream()
				.collect(Collectors.groupingBy(StoreDTO :: getZoneId));
				
		List<PRItemDTO> candidateItemList = new ArrayList<>();
		
		List<PRItemDTO> salesfloorInEmergencyList = new ArrayList<>();
		List<PRItemDTO> hardpartInEmergencyList = new ArrayList<>();
		List<PRItemDTO> hardPartItems = new ArrayList<>();
		List<PRItemDTO> salesFloorFiltered = new ArrayList<>();
		List<PRItemDTO> salesFloorItemsTobeFiltered = new ArrayList<>();
		
		List<PRItemDTO> emergencyItems = new ArrayList<>();
		if(approvedItemList.size() > 0) {
			emergencyItems = (List<PRItemDTO>) approvedItemList.stream()
					.filter(e -> e.getPriceExportType().equals(Constants.EMERGENCY))
					.collect(Collectors.toList());
		}		
		
		if (clearenceItemsFromItemList.size() > 0) {
			clearanceInItemList = true;
		}
		if (emergencyItemsFromItemList.size() > 0) {
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
		if (!emergencyInItemList) {
			if (!clearanceInItemList) {
				List<PRItemDTO> normalGroupItems = (List<PRItemDTO>) approvedItemList.stream()
						.filter(e -> e.getPriceExportType().equals(Constants.NORMAL))
						.collect(Collectors.toList());

				if (normalGroupItems.size() > 0) {
					hardPartItems = (List<PRItemDTO>) normalGroupItems.stream()
							.filter(e -> e.getItemType().equals(Constants.HARD_PART_ITEMS))
							.collect(Collectors.toList());
				}

				if (normalGroupItems.size() > 0) {
					salesFloorItemsTobeFiltered = (List<PRItemDTO>) normalGroupItems.stream()
							.filter(e -> e.getItemType().equals(Constants.SALE_FLOOR_ITEMS))
							.collect(Collectors.toList());
				}
			}
		}

		List<PRItemDTO> itemsFilteredList = new ArrayList<>();
		
		if (emergencyItems.size() > 0) {
			itemsFilteredList.addAll(emergencyItems);
		}
		if (hardPartItems.size() > 0) {
			itemsFilteredList.addAll(hardPartItems);
		}
		
		
		if ((!emergency || !emergencyInItemList) && salesFloorItemsTobeFiltered.size() > 0) {
						
			//List<PRItemDTO> sfItemInQueueWithRank = priceExportDao.getSFItemsFromApprovedRecommendations(conn, salesFloorItemsTobeFiltered, todaysDate);
			
			// Filter sales floor items based on Input and sales floor limit
			logger.debug("Selecting top " + saleFloorLimitStr + " items for sales floor..");
			logger.info("# of records before filtering sales floor items: " + approvedItemList.size());
			salesFloorFiltered = storePriceExportAZSerive
					.filterSalesFloorItemsByAbsoluteImpact(salesFloorItemsTobeFiltered, saleFloorLimitStr);
			itemsFilteredList.addAll(salesFloorFiltered);
			logger.info("# of records after filtering for sales floor: " + itemsFilteredList.size());
		}
//		logger.info("clearanceInItemList: " + clearanceInItemList.size());
		logger.info("emergencyAndClearanceItems: " + emergencyAndClearanceItems.size());
		//EC items
		if (emergencyInItemList || clearanceInItemList) {
			if(emergencyItems.size() > 0) {
				candidateItemList.addAll(emergencyItems);
			}
			if (emergencyAndClearanceItems.size() > 0) {
				candidateItemList.addAll(ECItemsToExport);
			}			
		} 	
		//approved items
		else {		
			
			logger.info("exportPrice() - Setting zone level price ");
			List<PRItemDTO> zoneLevelData = new ArrayList<>();
			zoneLevelData = storePriceExportAZSerive.setZoneLevelPrice(itemsFilteredList, excludeZoneIdForVirtualZone, zoneIdAndStoreNumMap);
			candidateItemList.addAll(zoneLevelData);
			//adding HP items from itemlist
			if (hardpartInEmergencyList.size() > 0) {
				logger.debug("Adding HP items in emergency itemlist..");
				candidateItemList.addAll(hardpartInEmergencyList);
			}
		}
		logger.info("# in candidateItemList: " + candidateItemList.size());
		return candidateItemList;		
	}

	public List<PRItemDTO>  populateStoreLockItemList(String input, List<String> itemStoreCombinationsFromPriceTest,
			List<PRItemDTO> approvedItemList, List<PRItemDTO> exportItemList) throws GeneralException {
		
		List<PRItemDTO> storeLockItems = new ArrayList<>();
		
		logger.info("populateStoreLockItemList() - Populating Store lock items.");
		
		HashMap<String,String> storeNumIDMap = new HashMap<>();
		for(StoreDTO stores : storeData) {
			storeNumIDMap.put(stores.strNum, String.valueOf(stores.strId));
		}
		List<PRItemDTO> storeLockListItems = new ArrayList<>();
		try {
			storeLockItems = getStoreLockItems(approvedItemList, input, storeLockListItems);
		
		if(itemStoreCombinationsFromPriceTest.size()>0) {
			storePriceExportAZSerive.filterAndAddItemsBasedOnPriceTestData(itemStoreCombinationsFromPriceTest,
					storeLockItems,exportItemList,storeNumIDMap);
		}else {
			exportItemList.addAll(storeLockItems);
		}
		}catch(GeneralException e) {
			logger.info("populateStoreLockItemList() - Error while populating store lock items: " + e);
		}
			
		//Audit Store lock items from Check list
		//Addded on 08/18/2021 - PROM-2248
		if(storeLockListItems != null && storeLockListItems.size() > 0) {
			List<PRItemDTO> tempExportItemList = new ArrayList<>();
			if(itemStoreCombinationsFromPriceTest.size()>0) {
				storePriceExportAZSerive.filterAndAddItemsBasedOnPriceTestData(itemStoreCombinationsFromPriceTest,
						storeLockListItems,tempExportItemList,storeNumIDMap);
			}else {
				tempExportItemList.addAll(storeLockListItems);
			}
			auditTrialStoreLockListRecords(conn, tempExportItemList);
		}
		
		return storeLockItems;
		
	}

	private List<PRItemDTO> getStoreLockItems(List<PRItemDTO> exportListAtZoneLevel, String priceExportType, 
			List<PRItemDTO> storeLockListItems) throws GeneralException {
		List<PRItemDTO> storeLockItems = new ArrayList<>();
		Set<Integer> itemCodes = new HashSet<>();
		Set<String> itemCodeStrSet = new HashSet<>();
		Set<String> priceNotFound = new HashSet<>();
		Set<Integer> zones = new HashSet<>();
		RetailPriceDAO retailPriceDAO = new RetailPriceDAO();
		RetailCalendarDAO retailCalendarDAO = new RetailCalendarDAO();
		String curWkStartDate = DateUtil.getWeekStartDate(0);
		RetailCalendarDTO retailCalendarDTO = retailCalendarDAO.getCalendarId(conn, curWkStartDate,
				Constants.CALENDAR_WEEK);
		int calendarId = retailCalendarDTO.getCalendarId();
		//int calendarId = 5014;
		
		Set<Long> runIds = new HashSet<>();
		int lockListTypeId = Integer.parseInt(PropertyManager.getProperty("PRICE_CHECK_LIST_TYPE_ID"));
		exportListAtZoneLevel.forEach(item -> {
			itemCodes.add(item.getItemCode());
			zones.add(item.getPriceZoneId());
			runIds.add(item.getRunId());
		});

		logger.debug("items added in store lock: " + itemCodes);
		HashMap<Integer, HashMap<Integer, Set<String>>> itemStoreLockMap = new HashMap<>();
		
		logger.info("getStoreLockItems() - Getting store lock from regular list...");
		
		itemStoreLockMap = priceExportDao.getExcludeStoresFromStoreLockList(conn, itemCodes, zones, lockListTypeId,
				Constants.STORE_LEVEL_ID, priceExportType);

		logger.info("getStoreLockItems() - Getting store lock from regular list is completed");

		logger.info("getStoreLockItems() - Getting store lock from store list level...");
		HashMap<Integer, HashMap<Integer, Set<String>>> itemStoreListLockMap = priceExportDao.getExcludeStoresFromStoreLockList(conn, itemCodes, zones, 
				lockListTypeId, Constants.STORE_LIST_LEVEL_ID, priceExportType);
		itemStoreLockMap.putAll(itemStoreListLockMap);
		logger.info("getStoreLockItems() - Getting store lock from store list level is completed");
		
		if (itemStoreListLockMap == null || itemStoreListLockMap.size() <= 0) {
			logger.info("getStoreLockItems() - No store lock data from item list");
		}
		else {
			for (PRItemDTO item : exportListAtZoneLevel) {
				if (itemStoreListLockMap.containsKey(item.getItemCode())) {
					HashMap<Integer, Set<String>> zoneStoreListMap = itemStoreListLockMap.get(item.getItemCode());
					if (zoneStoreListMap.containsKey(item.getPriceZoneId())) {
						Set<String> stores = zoneStoreListMap.get(item.getPriceZoneId());
						stores.forEach(store -> {
							String[] strLocArr = store.split(";");
							try {
								PRItemDTO itemStoreObj = (PRItemDTO) item.clone();
								itemStoreObj.setStoreNo(strLocArr[0]);
								itemStoreObj.setCalendarId(calendarId);
								itemStoreObj.setChildLocationLevelId(Constants.STORE_LEVEL_TYPE_ID);
								itemStoreObj.setStoreLockExpiryFlag(Constants.EXPORT_ADD_DATA);
								itemStoreObj.setLocationId(Integer.valueOf(strLocArr[1]));
								itemStoreObj.setPriceCheckListTypeId(lockListTypeId);
								storeLockListItems.add(itemStoreObj);
							} catch (Exception e) {
								logger.error("getStoreLockItems() - Error cloning obj", e);
							}
						});
					}
				}
			}
		}
		

		logger.info("getStoreLockItems() - # of items with store lock: " + itemStoreLockMap.size());


		itemStoreLockMap.keySet().forEach(itemCode -> {
			itemCodeStrSet.add(String.valueOf(itemCode));
		});

		logger.info("getStoreLockItems() - Getting price info for " + itemCodeStrSet.size() + " items...");
		
		List<Integer> historyCalendarList = new ArrayList<>();
    	if(retailCalendarDTO.getStartDate() == null) {
    		
    	}else {
    		historyCalendarList = getCalendarIdHistory(conn, retailCalendarDTO.getStartDate());
    	}
		
		HashMap<String, List<RetailPriceDTO>> priceDataMap = retailPriceDAO.getRetailPriceInfoWithHistory(conn, itemCodeStrSet,
				calendarId, false, historyCalendarList);
		logger.info("getStoreLockItems() - Getting price info is compeleted");

		for(PRItemDTO item : exportListAtZoneLevel) {
			if (itemStoreLockMap.containsKey(item.getItemCode())) {
				HashMap<Integer, Set<String>> zoneStoreMap = itemStoreLockMap.get(item.getItemCode());
				if (zoneStoreMap.containsKey(item.getPriceZoneId())) {
					Set<String> stores = zoneStoreMap.get(item.getPriceZoneId());
					stores.forEach(store -> {
						String[] strLocArr = store.split(";");
						try {
							PRItemDTO itemStoreObj = (PRItemDTO) item.clone();
							String itemCodeStr = String.valueOf(itemStoreObj.getItemCode());
							itemStoreObj.setStoreNo(strLocArr[0]);
							itemStoreObj.setCalendarId(calendarId);
							itemStoreObj.setChildLocationLevelId(Constants.STORE_LEVEL_TYPE_ID);
							itemStoreObj.setStoreLockExpiryFlag(Constants.EXPORT_ADD_DATA);
							itemStoreObj.setLocationId(Integer.valueOf(strLocArr[1]));
							itemStoreObj.setPriceCheckListTypeId(lockListTypeId);
							if (priceDataMap.containsKey(itemCodeStr)) {
								storePriceExportAZSerive.setCurrentPriceForLockedStore(priceDataMap, itemStoreObj, itemCodeStr);
								storeLockItems.add(itemStoreObj);
							} else {
								priceNotFound.add(itemCodeStr);
							}
						} catch (Exception e) {
							logger.error("getStoreLockItems() - Error cloning obj", e);
						}
					});
				}
			}
		}

		if (priceNotFound.size() > 0) {
			logger.warn("getStoreLockItems() - Items with no current price in calendar id " + calendarId + ": "
					+ PRCommonUtil.getCommaSeperatedStringFromStrSet(priceNotFound));
		}

		return storeLockItems;
	}

	private List<Integer> getCalendarIdHistory(Connection conn, String weekStartDate) throws GeneralException{
		List<Integer> historyCalendarList = new ArrayList<Integer>();
		String historyStartDateStr = DateUtil.getWeekStartDate(DateUtil.toDate(weekStartDate), 4);
		Date historyStartDate = DateUtil.toDate(historyStartDateStr);
		
		String historyEndDateStr = DateUtil.getWeekEndDate(DateUtil.toDate(weekStartDate));
		Date historyEndDate = DateUtil.toDate(historyEndDateStr);
		
		RetailCalendarDAO calDAO = new RetailCalendarDAO();
		List<RetailCalendarDTO> retailCalDTOList = calDAO.RowTypeBasedCalendarList(conn, historyStartDate, historyEndDate, Constants.CALENDAR_WEEK);
		for(RetailCalendarDTO calendarDTO : retailCalDTOList){
			historyCalendarList.add(calendarDTO.getCalendarId());
		}
		return historyCalendarList;
	}
	

	public List<PRItemDTO> populateApprovedItemList(String input, HashMap<String, List<Long>> runIdMap,
			String todaysDate) {

		logger.info("populateApprovedItemList() - Populating candidate items for price export ");

		List<PRItemDTO> approvedItems = new ArrayList<>();
		if (runIdMap.size() > 0) {
			try {
				// populating approved items
				List<Long> normalRunIdList = runIdMap.get("N");
				List<PRItemDTO> itemsInQueue = priceExportDao.getItemsFromApprovedRecommendations(conn,
						normalRunIdList, input, emergency, todaysDate);

				// only if items are there in queue
				if (itemsInQueue != null && itemsInQueue.size() > 0) {

					// populating global zone data if global zone approved
					logger.info("populateApprovedItemList() - Populating global zone items ");
					approvedItems = applyGlobalZonePriceToAllZonesV2(itemsInQueue, globalZone);

				} else {
					logger.info("populateApprovedItemList() - No approved items found");
					noCandidates = true;

				}
			} catch (GeneralException e) {
				logger.info("populateApprovedItemList() - Error while populating approved items: " + e);
			}
		}
		return approvedItems;

	}

	private List<PRItemDTO> applyGlobalZonePriceToAllZones(List<PRItemDTO> itemList,String globalZone)
			throws GeneralException {

		HashMap<String, Integer> zoneMap = new HashMap<>();
		HashMap<Integer, String> zoneIdandNameMap = new HashMap<>();
		for(ZoneDTO zones : azZoneData) {
			zoneMap.put(zones.getZnNo(), zones.getZnId());
			zoneIdandNameMap.put(zones.getZnId(), zones.getZnName());
		}

		List<PRItemDTO> allZonesMergedList = new ArrayList<PRItemDTO>();
		List<PRItemDTO> itemsFilteredByGlobalZone = new ArrayList<PRItemDTO>();

		// grouped by item code
		HashMap<Integer, List<PRItemDTO>> itemZoneMap = (HashMap<Integer, List<PRItemDTO>>) itemList.stream()
				.collect(Collectors.groupingBy(PRItemDTO::getItemCode));

		List<PRItemDTO> itemDataList = new ArrayList<>();
		itemZoneMap.forEach((itemCode, itemZoneList) -> {
			//boolean globalZoneRecommended = false;
			//PRItemDTO testZoneItem = null;
									
			for (PRItemDTO item : itemZoneList) {

				PRItemDTO globalZoneItem = null;
				if (item.getPriceZoneNo().equals(globalZone)) {
					globalZoneItem = item;
				}

				if (globalZoneItem != null) {
					itemsFilteredByGlobalZone.add(globalZoneItem);
				} 				
				else {
					itemsFilteredByGlobalZone.add(item);
				}
			}
			
		});

		//added on 27/06/2020
		//to set global recom. true when any other zones are recommended along with zone 1000.
		//so that the particular item will not come in missing items (considered to take current price 
		//in max price calculation for virtual zone)
		List<PRItemDTO> itemsFromGlobalZone = new ArrayList<PRItemDTO>();
		HashMap<Integer, List<PRItemDTO>> grpByItemCode = (HashMap<Integer, List<PRItemDTO>>) itemsFilteredByGlobalZone.stream()
				.collect(Collectors.groupingBy(PRItemDTO :: getItemCode));
		grpByItemCode.forEach((itemCode, itemZoneList) -> {	
			boolean globalZoneFound = false;
			for (PRItemDTO item : itemZoneList) {
				if(item.getPriceZoneNo().equals(globalZone)) {
					globalZoneFound = true;
					break;
				}
			}		
			if(itemZoneList.size() > 1 && globalZoneFound) {
				for (PRItemDTO item : itemZoneList) {
					item.setGlobalZoneRecommended(true);
					itemsFromGlobalZone.add(item);
				}
			}
			else {
				itemsFromGlobalZone.addAll(itemZoneList);
			}
		});

		if (itemsFromGlobalZone.size() > 0) {
			itemsFromGlobalZone.forEach(item -> {
				// if an item is recommended @ zone 1000, then apply the recommended retail to
				// all zones
				if (item.getPriceZoneNo().equals(globalZone)) {
				//if (item.isGlobalZoneRecommended()) {
					zoneMap.forEach((zoneNum, priceZoneId) -> {

						String zoneName = zoneIdandNameMap.get(priceZoneId);

						try {
							PRItemDTO zoneItem = (PRItemDTO) item.clone();
							// zoneItem.setRecommendedRegPrice(price);
							zoneItem.setPriceZoneId(priceZoneId);
							zoneItem.setPriceZoneNo(zoneNum);
							zoneItem.setZoneName(zoneName);
							allZonesMergedList.add(zoneItem);

						} catch (Exception e) {
							logger.debug("applyGlobalZonePriceToAllZones() - Error while setting global zone's price");
						}
					});
				} else {
					// if an item is recommended @ regular zones, add the item as it is
					logger.debug("No price recommended for Global zone");
					allZonesMergedList.add(item);
				}
			});			
		}

		return allZonesMergedList;
	}

	private List<PRItemDTO> applyGlobalZonePriceToAllZonesV2(List<PRItemDTO> itemList,String globalZone)
			throws GeneralException {

		HashMap<String, Integer> zoneMap = new HashMap<>();
		HashMap<Integer, String> zoneIdandNameMap = new HashMap<>();
		for(ZoneDTO zones : azZoneData) {
			zoneMap.put(zones.getZnNo(), zones.getZnId());
			zoneIdandNameMap.put(zones.getZnId(), zones.getZnName());
		}

		List<PRItemDTO> allZonesMergedList = new ArrayList<PRItemDTO>();

		// grouped by item code
		HashMap<Integer, List<PRItemDTO>> itemZoneMap = (HashMap<Integer, List<PRItemDTO>>) itemList.stream()
				.collect(Collectors.groupingBy(PRItemDTO::getItemCode));

		itemZoneMap.forEach((itemCode, itemZoneList) -> {
			//group by zone
			HashMap<String, List<PRItemDTO>> dataByZone = (HashMap<String, List<PRItemDTO>>) itemZoneList.stream()
					.collect(Collectors.groupingBy(PRItemDTO::getPriceZoneNo));	
			
			//getting individual zones 5, 49, 66, 67
			List<String> individualZoneList = new ArrayList<>();
			for(String zone : dataByZone.keySet()) {
				if(!zone.equals(Constants.AZ_GLOBAL_ZONE) && !zoneMap.containsKey(zone)) {
					individualZoneList.add(zone);
				}				
			}
			
			//if no global zone
			if(!(dataByZone.keySet().contains(Constants.AZ_GLOBAL_ZONE))) {				
				//itemDataList.addAll(itemZoneList);
				allZonesMergedList.addAll(itemZoneList);
			}
			//if only global zone
			else if((dataByZone.keySet().size() == 1 && dataByZone.keySet().contains(Constants.AZ_GLOBAL_ZONE))) {
				//dataByZone.get(Constants.AZ_GLOBAL_ZONE).get(0).setGlobalZoneRecommended(true);
				//itemDataList.add(dataByZone.get(Constants.AZ_GLOBAL_ZONE).get(0));
				List<PRItemDTO> referenceList = getGlobalZoneData(dataByZone.get(Constants.AZ_GLOBAL_ZONE).get(0), zoneMap, 
						zoneIdandNameMap);
				allZonesMergedList.addAll(referenceList);
			}	
			//for regular zones & global zone
			else {
				SimpleDateFormat sdformat = new SimpleDateFormat("MM/dd/yyyy HH:mm:ss");
				PRItemDTO referenceData = dataByZone.get(Constants.AZ_GLOBAL_ZONE).get(0);	
								
				for(Map.Entry<String, Integer> zoneData : zoneMap.entrySet()) {//zone 4, 16, 12, 13
	
					if (dataByZone.keySet().contains(zoneData.getKey())) {
					try {	
						Date regZoneAprvd = sdformat.parse(dataByZone.get(zoneData.getKey()).get(0).getApprovedOn());
						Date globalZoneAprvd = sdformat.parse(referenceData.getApprovedOn());
						//if approved
						
							if (regZoneAprvd.compareTo(globalZoneAprvd) >= 0) {
								dataToClearDuplicate.add(referenceData);
								for(PRItemDTO item : dataByZone.get(zoneData.getKey())) {
									try {
										PRItemDTO approvedZoneItem = (PRItemDTO) item.clone();
										approvedZoneItem.setGlobalZoneRecommended(true);									
										allZonesMergedList.add(approvedZoneItem);
									} catch (Exception e) {
										logger.debug(
												"applyGlobalZonePriceToAllZones() - Error while setting global zone's price - " + e);
									}
								}
							}
							else {
								dataToClearDuplicate.add(dataByZone.get(zoneData.getKey()).get(0));
								String zoneName = zoneIdandNameMap.get(zoneData.getValue());
								try {									
									PRItemDTO approvedZoneItem = (PRItemDTO) referenceData.clone();
									approvedZoneItem.setPriceZoneId(zoneData.getValue());
									approvedZoneItem.setPriceZoneNo(zoneData.getKey());
									approvedZoneItem.setZoneName(zoneName);
									approvedZoneItem.setGlobalZoneRecommended(true);									
									allZonesMergedList.add(approvedZoneItem);

								} catch (Exception e) {
									logger.debug("applyGlobalZonePriceToAllZones() - Error while setting global zone's price - " + e);
								}
							}
						} catch (ParseException e) {
							e.printStackTrace();
						}
					}
					//if not approved
					else {
						String zoneName = zoneIdandNameMap.get(zoneData.getValue());
						try {
							PRItemDTO zoneItem = (PRItemDTO) referenceData.clone();
							zoneItem.setPriceZoneId(zoneData.getValue());
							zoneItem.setPriceZoneNo(zoneData.getKey());
							zoneItem.setZoneName(zoneName);
							zoneItem.setGlobalZoneRecommended(true);
							allZonesMergedList.add(zoneItem);
						} catch (Exception e) {
							logger.debug("applyGlobalZonePriceToAllZones() - Error while setting global zone's price - " + e);
						}
					}
				}
				//for individual zones
				for(String zone : individualZoneList) {
					if (dataByZone.keySet().contains(zone)) {
						try {
							PRItemDTO zoneItem = (PRItemDTO) dataByZone.get(zone).get(0).clone();
							zoneItem.setGlobalZoneRecommended(true);
							allZonesMergedList.add(zoneItem);
						} catch (Exception e) {
							logger.debug(
										"applyGlobalZonePriceToAllZones() - Error while setting global zone's price");
						}
					}
				}
			}
		});

		return allZonesMergedList;
	}
	
	public List<PRItemDTO> getGlobalZoneData(PRItemDTO referenceItem, HashMap<String, Integer> zoneMap, 
			HashMap<Integer, String>zoneIdandNameMap){
		List<PRItemDTO> referenceList = new ArrayList<>();
		zoneMap.forEach((zoneNum, priceZoneId) -> {

			String zoneName = zoneIdandNameMap.get(priceZoneId);
			try {
				PRItemDTO zoneItem = (PRItemDTO) referenceItem.clone();
				zoneItem.setPriceZoneId(priceZoneId);
				zoneItem.setPriceZoneNo(zoneNum);
				zoneItem.setZoneName(zoneName);
				zoneItem.setGlobalZoneRecommended(true);
				referenceList.add(zoneItem);

			} catch (Exception e) {
				logger.debug("applyGlobalZonePriceToAllZones() - Error while setting global zone's price");
			}
		});
		return referenceList;
	}

	
	public List<PRItemDTO> populateEmergencyAndClearancePrices(String priceExportType, List<PRItemDTO> eCItemsToExport, 
			List<PRItemDTO> approvedItemList, LocalDateTime now) throws GeneralException, CloneNotSupportedException {
		
		logger.info("populateEmergencyAndClearancePrices() - Populating emergency and clearance items "
				+ "for price export starts. ");
		
		List<PRItemDTO> emergencyAndClearanceItems = new ArrayList<>();
		
		Map<Integer,List<StoreDTO>> storeDataByZone = storeData.stream()
				.collect(Collectors.groupingBy(StoreDTO::getZoneId));
		
		if(approvedItemList.size()>0) {
			emergencyAndClearanceItems = getEmergencyAndClearanceItemsFromItemList(approvedItemList, priceExportType, eCItemsToExport,
					false, excludeZoneIdForVirtualZone, now, storeDataByZone);		
		}else {
			emergencyAndClearanceItems = getEmergencyAndClearanceItemsFromItemList(approvedItemList, priceExportType, eCItemsToExport, 
					true, excludeZoneIdForVirtualZone, now, storeDataByZone);	
		}
		
		if (eCItemsToExport.size() > 0) {
			Set<String> checkList = new HashSet<>();
			Set<String> itemList = new HashSet<>();
			try {
				for (PRItemDTO ecData : eCItemsToExport) {
					if (!checkList.contains(ecData.getPriceCheckListId() + ";" + ecData.getApprovedBy())) {
						checkList.add(ecData.getPriceCheckListId() + ";" + ecData.getApprovedBy());
					}
					if(!(itemList.contains(ecData.getItemCode()+";"+ecData.getPriceCheckListId()))) {
						itemList.add(ecData.getItemCode()+";"+ecData.getPriceCheckListId());
					}
				}
				priceExportDao.insertECDataToHeader(conn, checkList);
				priceExportDao.insertECDataToDetail(conn, itemList);
				updateECItems(itemList);
				PristineDBUtil.commitTransaction(conn, "Price export update status");
			} catch (GeneralException | ParseException e) {
				PristineDBUtil.rollbackTransaction(conn, "Price export update status");
				e.printStackTrace();
			}

		}
		else {
			logger.info("populateEmergencyAndClearancePrices() - No Emergency and clearance data from item list..");
		}
		return emergencyAndClearanceItems;
	}

	private void updateECItems(Set<String> emergencyAndClearanceItems) {
		List<String> temp = new ArrayList<>();
		try {
			for(String data : emergencyAndClearanceItems) {
				temp.add(data);
				if(temp.size() % 1000 == 0) {
					priceExportDao.updateClearanceItemsStatus(conn, emergencyAndClearanceItems);
					temp.clear();
				}
			}
			if(temp.size() > 0) {
				priceExportDao.updateClearanceItemsStatus(conn, emergencyAndClearanceItems);
			}
		} catch (GeneralException e1) {
			e1.printStackTrace();
		}
	}

	public List<PRItemDTO> populateTestZonePrices(String input, List<PRItemDTO> candidateItemList,
			HashMap<String, List<Long>> runIdMap, List<String> itemStoreCombinationsFromPriceTest,
			List<PRItemDTO> priceTestItemZoneLevelEntries, List<PRItemDTO> eCItemsToExport, String todaysDate) {

		List<PRItemDTO> priceTestItemStoreLevelEntries = new ArrayList<>();
		if (eCItemsToExport == null || eCItemsToExport.size() <= 0) {
			
			if (runIdMap.get("T") == null || runIdMap.get("T").size() <= 0) {
			} else {
				logger.info("populateTestZonePrices() - Populating Test zone data ");

				HashMap<String, String> storeNumIDMap = new HashMap<>();
				for (StoreDTO stores : storeData) {
					storeNumIDMap.put(stores.strNum, String.valueOf(stores.strId));
				}

				try {
					priceTestItemStoreLevelEntries = getPriceTestStoreLevelData(runIdMap.get("T"),
							itemStoreCombinationsFromPriceTest, input, priceTestItemZoneLevelEntries, todaysDate);

					// test zone data based on clearance
					if (priceTestItemStoreLevelEntries.size() > 0) {
						// storePriceExportAZSerive.insertPriceTestItemsBasedOnEmergencyClearenceItems(candidateItemList,
						// priceTestItemStoreLevelEntries,eCItemsToExport,itemStoreCombinationsFromPriceTest,storeNumIDMap);
						candidateItemList.addAll(priceTestItemStoreLevelEntries);
					}
				} catch (CloneNotSupportedException e) {
					logger.info("populateTestZonePrices() - Error while populating test zone: " + e);
				}
			}
		}
		return priceTestItemStoreLevelEntries;		
	}

	public HashMap<String, List<Long>> populateRunIds(String input, String todaysDate) {

		logger.info("populateRunIds() - Populating run id list for price export starts.");

		HashMap<String, List<Long>> runIdMap = new HashMap<>();

		try {
			List<Long> normalRunIdList = new ArrayList<>();
			List<Long> emergencyRunIdList = new ArrayList<>();
			List<Long> runIdList = new ArrayList<>();

			//populating emergency run ids
			if(Constants.ALL_SALESFLOOR_AND_HARDPART_AND_EMERGENCY.equals(input) || 
					Constants.EMERGENCY_OR_HARDPART.equals(input) || input.equals(Constants.EMERGENCY_OR_SALESFLOOR)) {
				emergencyRunIdList = priceExportDao.getRunIdsByType(conn, todaysDate, Constants.EMERGENCY);
			}

			if (emergencyRunIdList.size() > 0) {
				emergency = true;
				runIdList.addAll(emergencyRunIdList);
			} else {
				// populating normal run ids only when emergency runids are not available
				normalRunIdList = priceExportDao.getRunIdsByType(conn, todaysDate, Constants.NORMAL);
				if (normalRunIdList.size() > 0) {
					runIdList.addAll(normalRunIdList);
				}
			}
			
			//separating test zone run ids and regular zone run ids
			if(runIdList.size() > 0) {
				runIdMap = priceExportDao.separateTestZoneAndRegularZoneRunIds(conn, runIdList);			
			}
			else {
				logger.info("No recommended data found to export!");
				noRunIds = true;
			}
		} catch (GeneralException e) {
			e.printStackTrace();
		}

		return runIdMap;

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

	public List<PRItemDTO> getEmergencyAndClearanceItemsFromItemList(List<PRItemDTO> finalExportList,
			String priceExportType, List<PRItemDTO> ECItemsToExport, boolean noDataInQueue, 
			List<Integer> excludeZoneIdForVirtualZone,  LocalDateTime now, Map<Integer, List<StoreDTO>> storeDataByZone) 
					throws GeneralException, CloneNotSupportedException {
		
		List<PRItemDTO> ECItemsList = new ArrayList<PRItemDTO>();
		RetailPriceDAO retailPriceDAO = new RetailPriceDAO();
		RetailCalendarDAO retailCalendarDAO = new RetailCalendarDAO();
		String curWkStartDate = DateUtil.getWeekStartDate(0);
		RetailCalendarDTO retailCalendarDTO = retailCalendarDAO.getCalendarId(conn, curWkStartDate,
				Constants.CALENDAR_WEEK);
		int calendarId = retailCalendarDTO.getCalendarId();
		//int calendarId = 5014;
		HashMap<String, List<PRItemDTO>> ruItemMap = (HashMap<String, List<PRItemDTO>>) itemRUData.stream()
				.collect(Collectors.groupingBy(PRItemDTO::getRetailerItemCode));
		
		HashMap<Integer, String> zoneIdandNumMap = new HashMap<>();
		HashMap<Integer, String> zoneIdandNameMap = new HashMap<>();
		for (ZoneDTO zones : allZoneData) {
			if (!zones.getZoneType().equals("T")) {
				zoneIdandNumMap.put(zones.getZnId(), zones.getZnNo());
				zoneIdandNameMap.put(zones.getZnId(), zones.getZnName());
			}
		}
		
	//	if (today.getDayOfWeek().equals(DayOfWeek.MONDAY) || today.getDayOfWeek().equals(DayOfWeek.TUESDAY)
			//	|| today.getDayOfWeek().equals(DayOfWeek.WEDNESDAY)) {
			
			//if we need to run Clearance on Thursday/ Friday -- as currently in production
			if (today.getDayOfWeek().equals(DayOfWeek.MONDAY) || today.getDayOfWeek().equals(DayOfWeek.TUESDAY)
					|| today.getDayOfWeek().equals(DayOfWeek.WEDNESDAY) || today.getDayOfWeek().equals(DayOfWeek.THURSDAY)) {
			
		logger.info("getEmergencyAndClearanceItemsFromItemList() - Getting Emergency and Clearance Item List...");
		ECItemsList = priceExportDao.getEmergencyAndClearanceItemsV3(conn);
			if (ECItemsList.size() > 0) {
				
				Set<String> itemCodeStrSet = new HashSet<>();
				ECItemsList.forEach(items -> {
					itemCodeStrSet.add(String.valueOf(items.getItemCode()));
				});

				logger.info("getEmergencyAndClearanceItemsFromItemList() - Getting price info for " + itemCodeStrSet.size() + " items...");
				logger.info("getEmergencyAndClearanceItemsFromItemList() - Items are " + String.join(",", itemCodeStrSet));
				List<Integer> historyCalendarList = new ArrayList<>();
		    	if(retailCalendarDTO.getStartDate() == null) {
		    		
		    	}else {
		    		historyCalendarList = getCalendarIdHistory(conn, retailCalendarDTO.getStartDate());
		    	}
				
				HashMap<String, List<RetailPriceDTO>> priceDataMap = retailPriceDAO.getRetailPriceInfoWithHistory(conn, itemCodeStrSet,
						calendarId, false, historyCalendarList);
				
				List<PRItemDTO>  clearenceItemsFromItemList = ECItemsList.stream()
						.filter(e -> e.getPriceCheckListTypeId() == Integer.parseInt(Constants.CLEARANCE_LIST_TYPE))
						.collect(Collectors.toList());

				List<PRItemDTO> emergencyItemsFromItemList = ECItemsList.stream()
						.filter(e -> e.getPriceCheckListTypeId() == Integer.parseInt(Constants.EMERGENCY_LIST_TYPE))
						.collect(Collectors.toList());
				
				List<String> excludeEcomStores = storePriceExportAZSerive.getExcludedEcomStores();
				List<String> excludedStores = storePriceExportAZSerive.getStoresExcludeZoneLevelData();

				if (clearenceItemsFromItemList.size() > 0) {

					for (PRItemDTO clearnaceItem : clearenceItemsFromItemList) {
						for (Map.Entry<Integer, String> zoneIDandNumEntry : zoneIdandNumMap.entrySet()) {						
							try {
								PRItemDTO zoneItem = null;
								PRItemDTO storeItem = null;
								if (noDataInQueue) {
									zoneItem = new PRItemDTO();
								} else {
									zoneItem = (PRItemDTO) finalExportList.get(0).clone();
								}
								// MultiplePrice recommendedPrice = new MultiplePrice(1,
								// clearanceItems.getECRetail());
								// zoneItem.setRecommendedRegPrice(recommendedPrice);
								if (!(zoneIDandNumEntry.getValue().equals(globalZone))) {
									zoneItem.setVdpRetail(clearnaceItem.getECRetail());
									zoneItem.setPriceCheckListTypeId(clearnaceItem.getPriceCheckListTypeId());
									zoneItem.setPriceCheckListId(clearnaceItem.getPriceCheckListId());
									zoneItem.setItemCode(clearnaceItem.getItemCode());									
									zoneItem.setItemType(clearnaceItem.getItemType());
									zoneItem.setPartNumber(clearnaceItem.getPartNumber());
									zoneItem.setPriceZoneId(zoneIDandNumEntry.getKey());
									zoneItem.setPriceZoneNo(zoneIDandNumEntry.getValue());
									zoneItem.setZoneName(zoneIdandNameMap.get(zoneIDandNumEntry.getKey()));
									zoneItem.setStoreNo("");
									zoneItem.setChildLocationLevelId(1);
									zoneItem.setRegEffDate(clearnaceItem.getStartDate());
									zoneItem.setRetailerItemCode(clearnaceItem.getRetailerItemCode());
									zoneItem.setApprovedBy(clearnaceItem.getApprovedBy());
									zoneItem.setApproverName(clearnaceItem.getApproverName());
									
									if(zoneItem.getPredicted() == null) {
										zoneItem.setPredicted(dateFormatter.format(now));
									}
									
									if (clearnaceItem.getStartDate() == null) {
										zoneItem.setRegEffDate(dateFormatter.format(now));
									} else {
										zoneItem.setRegEffDate(clearnaceItem.getStartDate());
									}
									zoneItem.setStoreLockExpiryFlag(Constants.EXPORT_ADD_DATA);
									List<PRItemDTO> itemList = ruItemMap.get(zoneItem.getRetailerItemCode());
									if (itemList != null && !itemList.isEmpty()) {
										zoneItem.setRecommendationUnit(itemList.get(0).getRecommendationUnit());
									}
									if(priceDataMap != null && priceDataMap.containsKey(String.valueOf(zoneItem.getItemCode()))) {
										storePriceExportAZSerive.setDiffPriceForECitems(priceDataMap, zoneItem, 
												String.valueOf(zoneItem.getItemCode()));
										ECItemsToExport.add(zoneItem);
									}
									
									if (excludeZoneIdForVirtualZone.contains(zoneIDandNumEntry.getKey())) {
										for (String storeNums : getStoreNoOfZones(zoneIDandNumEntry.getKey())) {
											if (!excludeEcomStores.contains(storeNums)) {
												if (!excludedStores.contains(storeNums)) {
													if (noDataInQueue) {
														storeItem = new PRItemDTO();
													} else {
														storeItem = (PRItemDTO) finalExportList.get(0).clone();
													}

													storeItem.setVdpRetail(clearnaceItem.getECRetail());
													storeItem.setPriceCheckListTypeId(
															clearnaceItem.getPriceCheckListTypeId());
													storeItem.setPriceCheckListId(clearnaceItem.getPriceCheckListId());
													storeItem.setItemCode(clearnaceItem.getItemCode());
													storeItem.setItemType(clearnaceItem.getItemType());
													storeItem.setPartNumber(clearnaceItem.getPartNumber());
													storeItem.setPriceZoneId(zoneIDandNumEntry.getKey());
													storeItem.setPriceZoneNo(zoneIDandNumEntry.getValue());
													storeItem.setZoneName(
															zoneIdandNameMap.get(zoneIDandNumEntry.getKey()));
													storeItem.setStoreNo(storeNums);
													storeItem.setChildLocationLevelId(2);
													storeItem.setRegEffDate(clearnaceItem.getStartDate());
													storeItem.setRetailerItemCode(clearnaceItem.getRetailerItemCode());
													storeItem.setApprovedBy(clearnaceItem.getApprovedBy());
													storeItem.setApproverName(clearnaceItem.getApproverName());

													if(storeItem.getPredicted() == null) {
														storeItem.setPredicted(dateFormatter.format(now));
													}
													if (clearnaceItem.getStartDate() == null) {
														storeItem.setRegEffDate(dateFormatter.format(now));
													} else {
														storeItem.setRegEffDate(clearnaceItem.getStartDate());
													}
													storeItem.setStoreLockExpiryFlag(Constants.EXPORT_ADD_DATA);
													List<PRItemDTO> itemList1 = ruItemMap.get(storeItem.getRetailerItemCode());
													if (itemList1 != null && !itemList1.isEmpty()) {
														storeItem.setRecommendationUnit(itemList1.get(0).getRecommendationUnit());
													}
													if(priceDataMap != null && priceDataMap.containsKey(String.valueOf(storeItem.getItemCode()))) {
														storePriceExportAZSerive.setDiffPriceForECitems(priceDataMap, storeItem, 
																String.valueOf(storeItem.getItemCode()));
														ECItemsToExport.add(storeItem);
													}
												}
											}
										}
									}
								}

							} catch (Exception ex) {
								ex.printStackTrace();
								logger.error(
										"getEmergencyAndClearanceItemsFromItemList() - Error when getting clearance items from item list - "
												+ ex);
							}
						}
					}
				}
			if (emergencyItemsFromItemList.size() > 0) {
				
				HashMap<Integer, List<PRItemDTO>> emergencyItemByItem = (HashMap<Integer, List<PRItemDTO>>) emergencyItemsFromItemList
						.stream().collect(Collectors.groupingBy(PRItemDTO::getItemCode));
				
				for (Map.Entry<Integer, List<PRItemDTO>> emergencyEntry : emergencyItemByItem.entrySet()) {

					HashMap<String, List<PRItemDTO>> emergencyItemByZone = (HashMap<String, List<PRItemDTO>>) emergencyEntry.getValue()
							.stream().filter(e -> !e.getPriceZoneNo().equals(""))
							.collect(Collectors.groupingBy(PRItemDTO::getPriceZoneNo));
					HashMap<String, List<PRItemDTO>> emergencyItemByStore = (HashMap<String, List<PRItemDTO>>)  emergencyEntry.getValue()
							.stream().filter(e -> !e.getStoreNo().equals(""))
							.collect(Collectors.groupingBy(PRItemDTO::getStoreNo));

					for (Map.Entry<String, List<PRItemDTO>> emergencyZoneEntry : emergencyItemByZone.entrySet()) {

						String zoneNum = emergencyZoneEntry.getKey();
						int zoneId = 0;

						for (PRItemDTO emergencyItem : emergencyZoneEntry.getValue()) {
							PRItemDTO zoneItem = null;
							if (noDataInQueue) {
								zoneItem = new PRItemDTO();
							} else {
								zoneItem = (PRItemDTO) finalExportList.get(0).clone();
							}
							if (String.valueOf(emergencyItem.getECRetail()) != null
									|| !String.valueOf(emergencyItem.getECRetail()).isEmpty()) {
								zoneItem.setVdpRetail(emergencyItem.getECRetail());
							}
							
							zoneItem.setPriceZoneNo(zoneNum);							
							zoneId = priceExportDao.getKey(zoneIdandNumMap, zoneItem.getPriceZoneNo());
							zoneItem.setPriceZoneId(zoneId);
							zoneItem.setZoneName(zoneIdandNameMap.get(zoneItem.getPriceZoneId()));
							
							zoneItem.setChildLocationLevelId(1);
							zoneItem.setRetailerItemCode(emergencyItem.getRetailerItemCode());
							zoneItem.setPriceCheckListTypeId(emergencyItem.getPriceCheckListTypeId());
							zoneItem.setPriceCheckListId(emergencyItem.getPriceCheckListId());
							zoneItem.setApprovedBy(emergencyItem.getApprovedBy());
							zoneItem.setApproverName(emergencyItem.getApproverName());
							zoneItem.setStoreLockExpiryFlag(Constants.EXPORT_ADD_DATA);
							zoneItem.setRegEffDate(emergencyItem.getStartDate());
							zoneItem.setItemCode(emergencyItem.getItemCode());
							zoneItem.setItemType(emergencyItem.getItemType());
							zoneItem.setPartNumber(emergencyItem.getPartNumber());
							List<PRItemDTO> itemList = ruItemMap.get(zoneItem.getRetailerItemCode());
							if (itemList != null && !itemList.isEmpty()) {
								zoneItem.setRecommendationUnit(itemList.get(0).getRecommendationUnit());
							}
							if (emergencyItem.getStartDate() == null) {
								zoneItem.setRegEffDate(dateFormatter.format(now));
							} else {
								zoneItem.setRegEffDate(emergencyItem.getStartDate());
							}
							if (priceDataMap != null
									&& priceDataMap.containsKey(String.valueOf(zoneItem.getItemCode()))) {
								storePriceExportAZSerive.setDiffPriceForECitems(priceDataMap, zoneItem,
										String.valueOf(zoneItem.getItemCode()));
								ECItemsToExport.add(zoneItem);
							}
						}

						if (excludeZoneIdForVirtualZone.contains(zoneId)) {
							for (PRItemDTO emergencyItem : emergencyZoneEntry.getValue()) {
								List<String> stores = storeDataByZone.get(zoneId).stream().map(StoreDTO::getStrNum)
										.collect(Collectors.toList());

								for (String storeNums : stores) {
									if (!excludeEcomStores.contains(storeNums)) {
										if (!excludedStores.contains(storeNums)) {
											if (emergencyItemByStore == null
													|| !emergencyItemByStore.containsKey(storeNums)) {
												PRItemDTO storeItem = null;
												if (noDataInQueue) {
													storeItem = new PRItemDTO();
												} else {
													storeItem = (PRItemDTO) finalExportList.get(0).clone();
												}
												
												storeItem.setVdpRetail(emergencyItem.getECRetail());
												storeItem.setPriceCheckListTypeId(
														emergencyItem.getPriceCheckListTypeId());
												storeItem.setPriceCheckListId(emergencyItem.getPriceCheckListId());
												storeItem.setItemCode(emergencyItem.getItemCode());
												storeItem.setItemType(emergencyItem.getItemType());
												storeItem.setPartNumber(emergencyItem.getPartNumber());
												storeItem.setPriceZoneId(zoneId);
												storeItem.setPriceZoneNo(zoneNum);
												storeItem.setZoneName(zoneIdandNameMap.get(zoneId));
												storeItem.setStoreNo(storeNums);
												storeItem.setChildLocationLevelId(2);
												storeItem.setRegEffDate(emergencyItem.getStartDate());
												storeItem.setRetailerItemCode(emergencyItem.getRetailerItemCode());
												storeItem.setApprovedBy(emergencyItem.getApprovedBy());
												storeItem.setApproverName(emergencyItem.getApproverName());

												if (storeItem.getPredicted() == null) {
													storeItem.setPredicted(dateFormatter.format(now));
												}
												if (emergencyItem.getStartDate() == null) {
													storeItem.setRegEffDate(dateFormatter.format(now));
												} else {
													storeItem.setRegEffDate(emergencyItem.getStartDate());
												}
												storeItem.setStoreLockExpiryFlag(Constants.EXPORT_ADD_DATA);
												List<PRItemDTO> itemList1 = ruItemMap
														.get(storeItem.getRetailerItemCode());
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
											}
										}
									}
								}
							}
						}
					}

					for (Map.Entry<String, List<PRItemDTO>> emergencyStoreEntry : emergencyItemByStore.entrySet()) {
						String storeNum = emergencyStoreEntry.getKey();
						if (storeZoneMap.get(storeNum) != null) {
							if (!excludeEcomStores.contains(storeNum)) {
								if (!excludedStores.contains(storeNum)) {
									for (PRItemDTO emergencyItem : emergencyStoreEntry.getValue()) {
										PRItemDTO storeItem = null;
										if (noDataInQueue) {
											storeItem = new PRItemDTO();
										} else {
											storeItem = (PRItemDTO) finalExportList.get(0).clone();
										}

										storeItem.setVdpRetail(emergencyItem.getECRetail());
										storeItem.setPriceCheckListTypeId(emergencyItem.getPriceCheckListTypeId());
										storeItem.setPriceCheckListId(emergencyItem.getPriceCheckListId());
										storeItem.setItemCode(emergencyItem.getItemCode());
										storeItem.setItemType(emergencyItem.getItemType());
										storeItem.setPartNumber(emergencyItem.getPartNumber());

										storeItem.setPriceZoneId(storeZoneMap.get(storeNum));
										storeItem.setPriceZoneNo(zoneIdandNumMap.get(storeItem.getPriceZoneId()));
										storeItem.setZoneName(zoneIdandNameMap.get(storeItem.getPriceZoneId()));

										storeItem.setStoreNo(storeNum);
										storeItem.setChildLocationLevelId(2);
										storeItem.setRegEffDate(emergencyItem.getStartDate());
										storeItem.setRetailerItemCode(emergencyItem.getRetailerItemCode());
										storeItem.setApprovedBy(emergencyItem.getApprovedBy());
										storeItem.setApproverName(emergencyItem.getApproverName());

										if (storeItem.getPredicted() == null) {
											storeItem.setPredicted(dateFormatter.format(now));
										}
										if (emergencyItem.getStartDate() == null) {
											storeItem.setRegEffDate(dateFormatter.format(now));
										} else {
											storeItem.setRegEffDate(emergencyItem.getStartDate());
										}
										storeItem.setStoreLockExpiryFlag(Constants.EXPORT_ADD_DATA);
										List<PRItemDTO> itemList1 = ruItemMap.get(storeItem.getRetailerItemCode());
										if (itemList1 != null && !itemList1.isEmpty()) {
											storeItem.setRecommendationUnit(itemList1.get(0).getRecommendationUnit());
										}
										if (priceDataMap != null
												&& priceDataMap.containsKey(String.valueOf(storeItem.getItemCode()))) {
											storePriceExportAZSerive.setDiffPriceForECitems(priceDataMap, storeItem,
													String.valueOf(storeItem.getItemCode()));
											ECItemsToExport.add(storeItem);
										}
									}
								}
							}
						}
					}
				}
			}
		  }
		}
		return ECItemsList;
	}

	public List<String> getStoreNoOfZones(int priceZoneId) {
		return priceExportDao.getStoresOfZones(conn, priceZoneId);
	}


	private List<PRItemDTO> getPriceTestStoreLevelData(List<Long> testZoneRunIdList,
			List<String> itemStoreCombinationsFromPriceTest, String input, 
			List<PRItemDTO> priceTestItemZoneLevelEntries, String currWeekEndDate) throws CloneNotSupportedException {
		List<PRItemDTO> PriceTestRecomData = new ArrayList<>();
		
		try {
			priceTestItemZoneLevelEntries.addAll(priceExportDao.getItemsFromApprovedRecommendations(conn, testZoneRunIdList,
					input, emergency,currWeekEndDate));
			
			HashMap<Integer, List<PRItemDTO>> ZoneItemMap = (HashMap<Integer, List<PRItemDTO>>) priceTestItemZoneLevelEntries.stream()
					.distinct().collect(Collectors.groupingBy(PRItemDTO::getPriceZoneId));
			
			HashMap<Integer,List<PRItemDTO>> ZoneStoreMap = new HashMap<>();
			ZoneStoreMap = priceExportDao.getTestZoneStoreCombinationsDictionaryV3(conn, testZoneRunIdList);
			
			for(Map.Entry<Integer, List<PRItemDTO>> zone : ZoneStoreMap.entrySet()) {
				List<PRItemDTO> StoreList = zone.getValue();
				List<PRItemDTO> ZoneItemData = ZoneItemMap.get(zone.getKey());
				if(ZoneItemData!= null && StoreList!=null) {
					for(PRItemDTO IZ : ZoneItemData)
					{
						for(PRItemDTO StoreRunId : StoreList) {
							if(StoreRunId.getRunId()==IZ.getRunId()) {
								PRItemDTO I = (PRItemDTO)IZ.clone();
								I.setStoreId(StoreRunId.getStoreId());
								I.setStoreNo(StoreRunId.getStoreNo());
								I.setZoneName(StoreRunId.getZoneName());
								I.setPriceZoneNo(StoreRunId.getPriceZoneNo());
								I.setPriceZoneId(StoreRunId.getPriceZoneId());
								I.setChildLocationLevelId(Constants.STORE_LEVEL_TYPE_ID);
								I.setStoreLockExpiryFlag(Constants.EXPORT_ADD_DATA);
								PriceTestRecomData.add(I);
								
								String key ="";
								key = I.getItemCode()+"_"+StoreRunId.getStoreId();
								if(!itemStoreCombinationsFromPriceTest.contains(key)) {
									itemStoreCombinationsFromPriceTest.add(key);
								}
							}
						}	
					}
				}
	
				
			}
			
		} catch (GeneralException e) {
			logger.error("GetPriceTestStoreLevelData(): Error in Getting Price Check List Data: "+e.getMessage());
		}
		
		
		return PriceTestRecomData;
	}
	

	public static double round(double value, int places) {
		if (places < 0)
			throw new IllegalArgumentException();

		long factor = (long) Math.pow(10, places);
		value = value * factor;
		long tmp = Math.round(value);
		return (double) tmp / factor;
	}
	
	public List<ZoneDTO> getAllZoneData(){
		return this.allZoneData;
	}
	public List<ZoneDTO> getAzZoneData(){
		return this.azZoneData;
	}
	public List<StoreDTO> getStoreData(){
		return this.storeData;
	}
	public HashMap<String, Integer> getStoreZoneMap(){
		return this.storeZoneMap;
	}
	public boolean[] getBooleanValuesInGlobal(){
		boolean[] statusArray = {this.emergency, this.noRunIds, this.noCandidates};
		return statusArray;
	}
	public PriceExportDAOV2 getExportDao() {
		return this.priceExportDao;
	}
	public PriceDataLoad getPriceDao() {
		return this.priceDataDao;
	}
	public StorePriceExportAZServiceV2 getExportService() {
		return this.storePriceExportAZSerive;
	}
	public Connection getConnection() {
		return this.conn;
	}
	public List<PRItemDTO> getFilteredSFItems() {
		return this.salesFloorFiltered;
	}
	public List<Integer> getExcludeZoneIdForVirtualZone() {
		return this.excludeZoneIdForVirtualZone;
	}
	
public LocalDateTime getLocalDate(String date) {
		
		if (date == null) {
			return null;
		} else {
			try {
				DateFormat format = new SimpleDateFormat("MM/dd/yyyy HH:mm:ss");
				logger.info("Date: "  +format.format(format.parse(date)));
				return LocalDateTime.parse(format.format(format.parse(date)));
			} catch (Exception e) {
				logger.info("Exception in getting local date: " + e);
				return null;
			}
		}
	}
}
