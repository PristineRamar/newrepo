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
import java.util.Collections;
import java.util.Comparator;
import java.util.Date;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Set;
import java.util.stream.Collectors;

import org.apache.log4j.Logger;
import org.apache.log4j.PropertyConfigurator;

import com.pristine.dao.DBManager;
import com.pristine.dao.ItemDAO;
import com.pristine.dao.RetailCalendarDAO;
import com.pristine.dao.RetailPriceDAO;
import com.pristine.dao.RetailPriceZoneDAO;
import com.pristine.dao.offermgmt.PriceExportDAO;
import com.pristine.dao.priceChangePerformance.PriceDataDAO;
import com.pristine.dataload.tops.PriceDataLoad;
import com.pristine.dto.RetailCalendarDTO;
import com.pristine.dto.RetailPriceDTO;
import com.pristine.dto.offermgmt.MultiplePrice;
import com.pristine.dto.offermgmt.NotificationDetailInputDTO;
import com.pristine.dto.offermgmt.PRItemDTO;
import com.pristine.exception.GeneralException;
import com.pristine.lookup.AuditTrailStatusLookup;
import com.pristine.lookup.AuditTrailTypeLookup;
import com.pristine.service.email.EmailService;
import com.pristine.service.offermgmt.AuditTrailService;
import com.pristine.service.offermgmt.NotificationService;
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
 */
public class StorePriceExport {

	private static Logger logger = Logger.getLogger("StorePriceExport");

	static String priceExportType;
	private Connection conn = null;
	
	DateTimeFormatter dateFormatter = DateTimeFormatter.ofPattern("MM/dd/yyyy hh:mm:ss");  
    LocalDateTime now = LocalDateTime.now();   
	LocalDate today = LocalDate.now();
	double max = 0;
	PriceExportDAO priceExportDao = new PriceExportDAO();
	PriceDataLoad priceDataDao = new PriceDataLoad();
	
	List<PRItemDTO> emergencyItemsFromItemList = new ArrayList<PRItemDTO>();
	List<PRItemDTO> clearenceItemsFromItemList = new ArrayList<PRItemDTO>();

	public StorePriceExport() {

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
		StorePriceExport export = new StorePriceExport();

		// setting up the inputs
		for (int ii = 0; ii < args.length; ii++) {
			String arg = args[ii];
			if (arg.startsWith("PRICE_EXPORT_TYPE")) {
				priceExportType = arg.substring("PRICE_EXPORT_TYPE=".length());
			}
		}

		logger.info("*****************************************************************");
		logger.info("main() - Exporting prices for " + priceExportType);

		boolean batchStatus = true;

		if (priceExportType.equals(Constants.EMERGENCY_OR_HARDPART)
				|| priceExportType.equals(Constants.EMERGENCY_OR_SALESFLOOR)
				|| priceExportType.equals(Constants.EMERGENCY)
				|| priceExportType.equals(Constants.ALL_SALESFLOOR_AND_HARDPART_AND_EMERGENCY)
				|| priceExportType.equals(Constants.BOTH_SALESFLOOR_AND_HARDPART)
				|| priceExportType.equals(Constants.SALE_FLOOR_ITEMS)
				|| priceExportType.equals(Constants.HARD_PART_ITEMS)) {

			export.exportPrice();
		} else {
			batchStatus = false;
			logger.info("The provided batch parameter is wrong!!");
			logger.info("Choose among: E / S / H / ES / EH / SH / ESH");
		}

		if (batchStatus) {
			logger.info("main() - Price export is completed.");
		} else {
			logger.info("main() - Change the Batch Parameter.");
		}
		logger.info("*****************************************************************");
	}

	/**
	 * exports recommended prices for given inputs. Inputs can be, 1. ES -
	 * Emergency/Sales floor items 2. EH - Emergency/Hard part items 3. E -
	 * Emergency 4. ESH - If Emergency exist emergency is exported, if no - sales floor and Hard part 5. SH - Both
	 * Sales floor and Hard part 6. S - Sales floor alone 7. H - Hard Part alone.
	 * 
	 * If it is exporting sales floor items, Only X number of items will be exported
	 * If it is exporting hard part items/emergency items, all items will be
	 * exported
	 * 
	 * 
	 */
	private void exportPrice() {

		/*
		 * boolean emergencyInHardPart = true; boolean emergencyInSaleFloor = true;
		 */
		boolean emergency = false;
		boolean emergencyInItemList = false;
		boolean clearanceInItemList = false;
		boolean regularRecom = false;
		/*
		 * LocalDate now = LocalDate.now(); LocalDate today =
		 * now.with(DayOfWeek.MONDAY);
		 */

		String exportStatus = "";
		String SFItemLimitStr = PropertyManager.getProperty("SALE_FLOORITEM_LIMIT");
		String globalZone = PropertyManager.getProperty("NATIONAL_LEVEL_ZONE");
		String virtualZoneNum = PropertyManager.getProperty("VIRTUAL_ZONE");
		String excludeZonesForVirtualZone = PropertyManager.getProperty("EXCLUDE_ZONE_FOR_VIRTUAL_ZONE");

		String[] arrayOfExcludeZone = excludeZonesForVirtualZone.split(",");
		List<String> excludeStoreListForVitualZone = Arrays.asList(arrayOfExcludeZone);

		// for filewriter
		DateFormat format = new SimpleDateFormat("MMddyyyy");
		String rootPath = PropertyManager.getProperty("FILEPATH");
		String timeStamp = format.format(new Date());
		String fileNameConfig = PropertyManager.getProperty("FILENAME");
		String fileName = fileNameConfig + "-" + timeStamp;
		File sourceFile = new File(rootPath + "/" + fileName + ".txt");
		File targetFile = new File(rootPath + "/" + fileName + ".txt." + Constants.DONE);

		List<PRItemDTO> finalExportList = new ArrayList<PRItemDTO>();
		List<PRItemDTO> excludeExportList = new ArrayList<PRItemDTO>();
		List<PRItemDTO> itemsFiltered = new ArrayList<PRItemDTO>();
		List<Long> runIdList = new ArrayList<Long>();
		List<PRItemDTO> salesFloorItemsTobeFiltered = new ArrayList<PRItemDTO>();
		List<PRItemDTO> hardPartItems = new ArrayList<PRItemDTO>();
		HashMap<Integer, List<PRItemDTO>> expiryItem = new HashMap<Integer, List<PRItemDTO>>();
		List<PRItemDTO> ECItems = new ArrayList<PRItemDTO>();
		List<PRItemDTO> ECItemsToExport = new ArrayList<PRItemDTO>();	
		
		
		List<PRItemDTO> hardpartInEmergencyList =  new ArrayList<>();
		List<PRItemDTO> salesfloorInEmergencyList =  new ArrayList<>();
		
		List<PRItemDTO> salesFloorFiltered = new ArrayList<PRItemDTO>();

		try {
			// caches
			HashMap<String, Integer> storeZoneMap = new RetailPriceZoneDAO().getStoreZoneMapping(conn);
			HashMap<Integer, String> zoneIdandNumMap = priceExportDao.getZoneNoForZoneId(conn);
			HashMap<Integer, String> zoneIdandNumMapWithoutTestZone = priceExportDao.getZoneNoForZoneIdWithoutTestZone(conn);
			HashMap<Integer, String> zoneIdandNameMap = priceExportDao.getZoneNameForZoneId(conn);
			HashMap<String, Integer> zoneMap = priceExportDao.getZoneIdMap(conn);
			HashMap<String, Integer> zonesPartOfGlobalZone = priceExportDao.getZonesPartOfGlobalZone(conn);
			HashMap<String, String> storeNumIDMap = priceExportDao.getStoreIdForNum(conn);
			
			List<Integer> excludeZoneIdForVirtualZone = new ArrayList<Integer>();
			for (String zoneNum : excludeStoreListForVitualZone) {
				excludeZoneIdForVirtualZone.add(zoneMap.get(zoneNum));
			}
			
			if(priceExportType.equals(Constants.BOTH_SALESFLOOR_AND_HARDPART) || 
					priceExportType.equals(Constants.SALE_FLOOR_ITEMS) ||
					priceExportType.equals(Constants.HARD_PART_ITEMS)){
				regularRecom = true;
			}

			//Get Current Week End Date 
			//String CurrWeekEndDate = priceExportDao.getCurrWeekEndDate(conn); 
			String TodaysDate ="";
			 DateTimeFormatter dtf = DateTimeFormatter.ofPattern("MM/dd/yyyy");  
			 LocalDateTime now = LocalDateTime.now();  
			TodaysDate = dtf.format(now); 
			// Get list of approved run ids
			// active when input is E / when E is available in ES /when E is available in EH
			// / when E is available in ESH
			logger.info("Getting runIds..");
			if (!regularRecom) {
				List<Long> emergencyRunIdList = priceExportDao.getEmergencyRunIds(conn,TodaysDate);
				if (emergencyRunIdList.size() > 0) {
					emergency = true;
					runIdList.addAll(emergencyRunIdList);
				}
			}

			//commented on 02/07/2020
			//since in ESH, E is given priority. if no emergency items, the below run ids are added are taken
			if (!emergency /*
							 * ||
							 * (priceExportType.equals(Constants.ALL_SALESFLOOR_AND_HARDPART_AND_EMERGENCY))
							 */|| regularRecom) {
				List<Long> normalRunIdList = priceExportDao.getNormalRunIds(conn,TodaysDate);
				if (normalRunIdList.size() > 0) {
					// add this when ESH or normal price export type
					// active when S / H / when S/H is available in ES / EH / ESH					
					runIdList.addAll(normalRunIdList);					
				}
			}

			if (runIdList.size() > 0) {
				logger.info("Total # of runIds: " + runIdList.size());								

				//Added by Prasad on 09/10/2020
				//Separate the Approved Test Zone Run Ids From the Rest 
				List<Long> TestZoneRunIdList = new ArrayList<>();
				priceExportDao.SeparateTestZoneRunIdsAndRegularRunIds(conn, runIdList, TestZoneRunIdList);
				logger.info("# of Test Zone runIds: " + TestZoneRunIdList.size());								
				logger.info("# of Regular Zone runIds: " + runIdList.size());								
				//End Separate the Approved Test Zone Run Ids From the Rest
				
				// Get items for approved category/recommendation unit and zone combination -
				// HP/SF are choosen here

				logger.info("Getting approved items from locations..");
				List<PRItemDTO> approvedItemList = priceExportDao.getItemsFromApprovedRecommendations(conn, runIdList,
						priceExportType, emergency,TodaysDate);
				

				if (approvedItemList.size() > 0||TestZoneRunIdList.size()>0) {
					logger.info("Setting Global zone - " + globalZone);
					List<PRItemDTO> itemList = applyGlobalZonePriceToAllZones(approvedItemList, zoneIdandNameMap,
							zonesPartOfGlobalZone, globalZone);

					logger.info("# of processed items: " + itemList.size());

					logger.info("Setting virtual zone - " + virtualZoneNum);
					logger.info("# of items before setting virtual zone: " + itemList.size());
					setMaxPriceForVirtualZone(itemList, virtualZoneNum, excludeZoneIdForVirtualZone, zoneIdandNumMap);

					List<PRItemDTO> emergencyItems = (List<PRItemDTO>) itemList.stream()
							.filter(e -> e.getPriceExportType().equals(Constants.EMERGENCY))
							.collect(Collectors.toList());
					
					//get emergency and clearance items from item list
					if(itemList.size()>0)
						ECItems = getEmergencyAndClearanceItemsFromItemList(itemList, priceExportType, ECItemsToExport,
							zoneIdandNameMap, zoneIdandNumMapWithoutTestZone, false, globalZone, storeZoneMap, excludeZoneIdForVirtualZone);		
					else
						ECItems = getEmergencyAndClearanceItemsFromItemList(itemList, priceExportType, ECItemsToExport,
								zoneIdandNameMap, zoneIdandNumMapWithoutTestZone, true, globalZone, storeZoneMap, excludeZoneIdForVirtualZone);		
						
					if(emergencyItemsFromItemList.size() > 0) {
					hardpartInEmergencyList = emergencyItemsFromItemList.stream().filter(e -> e.getItemType()
							.equals(Constants.HARD_PART_ITEMS)).collect(Collectors.toList());
					salesfloorInEmergencyList = emergencyItemsFromItemList.stream().filter(e -> e.getItemType()
							.equals(Constants.SALE_FLOOR_ITEMS)).collect(Collectors.toList());
					}
					
					if (ECItems.size() > 0) {
						
						// update export flag for clearance items
						logger.debug("updating EC items..");
						updateECItems(ECItems);

						//priceExportDao.clearRetailOfClearanceItems(conn, ECItems);
						
						priceExportDao.insertECDataToHeader(conn, ECItems);
						priceExportDao.insertECDataToDetail(conn, ECItems);
						
					}
					else {
						logger.info("No Emergency and clearance data from item list..");
					}	
					
					
					if(clearenceItemsFromItemList.size() > 0) {
						clearanceInItemList = true;
					}
					
					if(salesfloorInEmergencyList.size() <= 0 && hardpartInEmergencyList.size() > 0) {
						emergencyInItemList = false;
					}
					else if (salesfloorInEmergencyList.size() > 0){
						emergencyInItemList = true;
					}

					//declare whether to take normal items when clearance are available
					
					if (!emergencyInItemList) {
						if (!clearanceInItemList) {
							
							List<PRItemDTO> normalGroupItems = (List<PRItemDTO>) itemList.stream()
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

					// itemsFiltered = itemList;
					if (emergencyItems.size() > 0) {
						itemsFiltered.addAll(emergencyItems);
					}
					if (hardPartItems.size() > 0) {
						itemsFiltered.addAll(hardPartItems);
					}

					// handle sale floor items
					//this block is carried out when
					// if no emergency and clearance items available in item list
					// if no emergency approval
					if ((!emergency || !emergencyInItemList)  && salesFloorItemsTobeFiltered.size() > 0) {
						logger.debug("Selecting top " + SFItemLimitStr + " items for sales floor..");
						logger.info("# of records before filtering sales floor items: " + itemList.size());
						salesFloorFiltered = filterSalesFloorItemsByAbsoluteImpact(
								salesFloorItemsTobeFiltered, SFItemLimitStr);
						itemsFiltered.addAll(salesFloorFiltered);
						logger.info("# of records after filtering for sales floor: " + itemsFiltered.size());

					}

					logger.info("exportPrice() - Setting zone level price ");
					finalExportList = setZoneLevelPrice(itemsFiltered, excludeZoneIdForVirtualZone);					
					
					
					logger.info("exportPrice() - Setting price for store lock items");
					/*
					 * excludeExportList = setPriceForExcludedStores(itemsFiltered, virtualZoneNum,
					 * storeZoneMap, zoneIdandNameMap);
					 */
					
					//Added by Prasad on 09/10/2020
					//Changes for Getting Items from Test Zone Recommendations
					logger.info("exportPrice() - # of records in final list before including Price Test Item data: "
							+ finalExportList.size());
					List<String> ItemStoreCombinationsFromPriceTest = new ArrayList<>();
					List<PRItemDTO> PriceTestItemStoreLevelEntries = new ArrayList<>();
					List<PRItemDTO> PriceTestItemZoneLevelEntries = new ArrayList<>();

					PriceTestItemStoreLevelEntries = GetPriceTestStoreLevelData(conn,TestZoneRunIdList,ItemStoreCombinationsFromPriceTest,priceExportType, emergency,PriceTestItemZoneLevelEntries,TodaysDate);
					
					if(ECItemsToExport.size()>0)
						InsertPriceTestItemsBasedOnEmergencyClearenceItems(finalExportList,PriceTestItemStoreLevelEntries,ECItemsToExport,ItemStoreCombinationsFromPriceTest,storeNumIDMap);
					else
						finalExportList.addAll(PriceTestItemStoreLevelEntries);

					logger.info("exportPrice() - # of records in final list after including Price Test Item data: "
							+ finalExportList.size());
					//End Changes for Getting Items from Test Zone Recommendations 
					excludeExportList = getStoreLockItems(itemsFiltered, priceExportType);

					logger.info("exportPrice() - # of records in final list before including store lock Item data: "
							+ finalExportList.size());
					if(ItemStoreCombinationsFromPriceTest.size()>0) {
						FilterAndAddItemsBasedOnPriceTestData(ItemStoreCombinationsFromPriceTest,excludeExportList,finalExportList,storeNumIDMap);
					}else {
						finalExportList.addAll(excludeExportList);
					}
					
					logger.info("exportPrice() - # of records in final list after including store lock Item data: "
							+ finalExportList.size());

					logger.info("exportPrice() - Setting price for expired items in store lock ");
					expiryItem = getStoreLockExpiryItems(finalExportList, storeZoneMap, zoneIdandNumMap,
							priceExportType, false,ItemStoreCombinationsFromPriceTest, storeNumIDMap);
					logger.info("exportPrice() - # of records in final list after including expiry Item data: "
							+ finalExportList.size());	
						
					if(ECItems.size() > 0) {
						finalExportList.addAll(ECItemsToExport);
						
					}
					
					//Added by Prasad
					//Adding run ids and items data for price test data so that they considered here after in the process after 
					//export to mark or delete data from the tables of processed items
					if(PriceTestItemZoneLevelEntries.size()>0)
					itemsFiltered.addAll(PriceTestItemZoneLevelEntries);
					
					if(TestZoneRunIdList.size()>0)
					runIdList.addAll(TestZoneRunIdList);
					
					//End
					
					try {
						logger.info("exportPrice() - # of records to be written in file: " + finalExportList.size());
						writeCSVForPriceExport(finalExportList, rootPath, fileNameConfig, fileName, sourceFile, targetFile);
						logger.info("exportPrice() - Items are written in to the file..");
						logger.info("exportPrice() - Renamming file [" + sourceFile + "] to [" + targetFile + "]");
						// renameFile(sourceFile, targetFile);
						rename(sourceFile, targetFile);
					} catch (ParseException e) {
						logger.error("exportPrice() - writeCSVForPriceExport() - error while writing eport file.");
						throw new GeneralException("writeCSVForPriceExport() - error while writing eport file. ", e);
					}

					if (expiryItem.size() > 0) {
						// update export flag for store lock items such that only once the store lock
						// item with flag status is sent
						logger.debug("updating expiry items..");
						updateExpiryItemsStatus(expiryItem);
					}

				} 
				//when no item data found
				else {					
					toDoWhenNoItemsFoundForGivenInput(finalExportList, storeZoneMap, zoneIdandNumMap, zoneIdandNumMapWithoutTestZone,
							priceExportType, expiryItem, ECItems, zoneIdandNameMap, globalZone, ECItemsToExport,
							rootPath, fileNameConfig, fileName, sourceFile, targetFile, emergency, runIdList, salesFloorFiltered, 
							emergencyItemsFromItemList, clearenceItemsFromItemList, excludeZoneIdForVirtualZone, storeNumIDMap);					
				}

				
				if (itemsFiltered.size() > 0) {

					logger.info("exportPrice() - Deleting the processed data..");
					priceExportDao.deleteProcessedRunIds(conn, itemsFiltered);

					logger.info("exportPrice() - Status update for export items");
					priceExportDao.updateExportItems(conn, itemsFiltered);

					priceExportDao.updateExportLigItems(conn, itemsFiltered);

					priceExportDao.updateExportStatus(conn, itemsFiltered, runIdList);

					priceExportDao.insertExportStatus(conn, exportStatus, runIdList, itemsFiltered);

					priceExportDao.insertExportTrackId(conn, runIdList);

					logger.info("exportPrice() - Adding notification of export data..");
					addNotification(conn, runIdList, emergency);

					logger.info("exportPrice() - Audit trial for Price export status");
					AuditTrialExportData(conn, runIdList, itemsFiltered, emergency);			

					PristineDBUtil.commitTransaction(conn, "Price export update status");					
				
				}
				
				postEffectiveDateChange(ECItems, emergency, runIdList, salesFloorFiltered);
				PristineDBUtil.commitTransaction(conn, "Price export update status");	
			} 
			else {
				logger.info("No recommended data found to export!");
				toDoWhenNoItemsFoundForRunId(finalExportList, storeZoneMap, zoneIdandNumMap, zoneIdandNumMapWithoutTestZone,
						timeStamp, expiryItem, ECItems, zoneIdandNameMap, globalZone, ECItemsToExport, 
						rootPath, fileNameConfig, fileName, sourceFile, targetFile, emergency, runIdList, salesFloorFiltered, 
						emergencyItemsFromItemList, clearenceItemsFromItemList, excludeZoneIdForVirtualZone, storeNumIDMap);	
			}

		} catch (GeneralException | Exception e) {
			logger.error("exportPrice() - Error while exporting prices", e);
			PristineDBUtil.rollbackTransaction(conn, "Price export update status");
			clearErrorExportFiles(rootPath);
		} finally {
			PristineDBUtil.close(conn);
		}

	}



	private void FilterAndAddItemsBasedOnPriceTestData(List<String> itemStoreCombinationsFromPriceTest,
			List<PRItemDTO> source, List<PRItemDTO> destination, HashMap<String, String> storeNumIDMap) {
				for(PRItemDTO S : source) {
					String storeId = storeNumIDMap.get(S.getStoreNo());
					String key = S.getItemCode()+"_"+storeId;
					if(!itemStoreCombinationsFromPriceTest.contains(key)) 
					{
						destination.add(S);
					}
				}
		}

	private void toDoWhenNoItemsFoundForRunId(List<PRItemDTO> finalExportList, HashMap<String, Integer> storeZoneMap,
			HashMap<Integer, String> zoneIdandNumMap, HashMap<Integer, String> zoneIdandNumMapWithoutTestZone, String timeStamp, HashMap<Integer, List<PRItemDTO>> expiryItem,
			List<PRItemDTO> ECItems, HashMap<Integer, String> zoneIdandNameMap, String globalZone,
			List<PRItemDTO> ECItemsToExport, String rootPath, String fileNameConfig, String fileName, File sourceFile,
			File targetFile, boolean emergency, List<Long> runIdList, List<PRItemDTO> salesFloorFiltered,
			List<PRItemDTO> emergencyItems, List<PRItemDTO> clearenceItemsFromItemList, List<Integer> excludeZoneIdForVirtualZone, HashMap<String, String> storeNumIDMap) throws GeneralException, Exception {
		
		logger.info("exportPrice() - Setting price for expired items in store lock ");

		//Added by Prasad 09/10/2020
		//Added empty string list as when no data found no price test would be approved
		List<String> PriceTestItemStoreCombinations = new ArrayList();
		//
		
		expiryItem = getStoreLockExpiryItems(finalExportList, storeZoneMap, zoneIdandNumMap, priceExportType,
				true,PriceTestItemStoreCombinations, storeNumIDMap);
		logger.info("exportPrice() - # of records in final list after including expiry Item data: "
				+ finalExportList.size());

		if (expiryItem.size() > 0) {
			// update export flag for store lock items such that only once the store lock
			// item with flag status is sent
			logger.debug("updating expiry items..");
			updateExpiryItemsStatus(expiryItem);
		}

		ECItems = getEmergencyAndClearanceItemsFromItemList(finalExportList, priceExportType, ECItemsToExport, zoneIdandNameMap,
				zoneIdandNumMapWithoutTestZone, true, globalZone, storeZoneMap, excludeZoneIdForVirtualZone);
		logger.info("exportPrice() - # of records in final list after including EC items: "
				+ finalExportList.size());
		if (ECItems.size() > 0) {
			finalExportList.addAll(ECItemsToExport);
			// writeClearanceItem(clearanceItem);
			// emailClearanceItems();
			priceExportDao.insertECDataToHeader(conn, ECItems);
			priceExportDao.insertECDataToDetail(conn, ECItems);
		}

		if (ECItems.size() > 0) {
			// update export flag for clearance items
			logger.debug("updating expiry items..");
			updateECItems(ECItems);

			//logger.debug("Clearing the retail of clearance Items..");
			//priceExportDao.clearRetailOfClearanceItems(conn, ECItems);
		}	
		
		postEffectiveDateChange(ECItems, emergency, runIdList, salesFloorFiltered);
		
		PristineDBUtil.commitTransaction(conn, "Price export update status");
		writeCSVForPriceExport(finalExportList, rootPath, fileNameConfig, fileName, sourceFile, targetFile);
		rename(sourceFile, targetFile);
		
	}

	private void toDoWhenNoItemsFoundForGivenInput(List<PRItemDTO> finalExportList,
			HashMap<String, Integer> storeZoneMap, HashMap<Integer, String> zoneIdandNumMap, HashMap<Integer, String> zoneIdandNumMapWithoutTestZone, String priceExportType, 
			HashMap<Integer, List<PRItemDTO>> expiryItem, List<PRItemDTO> ECItems, HashMap<Integer, String> zoneIdandNameMap, 
			String globalZone, List<PRItemDTO> ECItemsToExport, String rootPath, String fileNameConfig, 
			String fileName, File sourceFile, File targetFile, boolean emergency, List<Long> runIdList,
			List<PRItemDTO> salesFloorFiltered, List<PRItemDTO> emergencyItemsFromItemList, List<PRItemDTO> clearenceItemsFromItemList, List<Integer> excludeZoneIdForVirtualZone, 
			HashMap<String, String> storeNumIDMap) throws GeneralException, Exception {
		
		logger.info("No items found to export for given input: " + priceExportType);					

		logger.info("exportPrice() - Setting price for expired items in store lock ");
		
		//Added by Prasad 09/10/2020
		//Added empty string list as when no data found no price test would be approved
		List<String> PriceTestItemStoreCombinations = new ArrayList();
		//
		expiryItem = getStoreLockExpiryItems(finalExportList, storeZoneMap, zoneIdandNumMap, priceExportType,
				true,PriceTestItemStoreCombinations, storeNumIDMap);
		logger.info("exportPrice() - # of records in final list after including expiry Item data: "
				+ finalExportList.size());

		if (expiryItem.size() > 0) {
			// update export flag for store lock items such that only once the store lock
			// item with flag status is sent
			logger.debug("updating expiry items..");
			updateExpiryItemsStatus(expiryItem);
		}
		
		ECItems = getEmergencyAndClearanceItemsFromItemList(finalExportList, priceExportType, ECItemsToExport, zoneIdandNameMap,
				zoneIdandNumMapWithoutTestZone, true, globalZone, storeZoneMap, excludeZoneIdForVirtualZone);
		logger.info("exportPrice() - # of records in final list after including EC items: "
				+ finalExportList.size());
		if (ECItems.size() > 0) {
			finalExportList.addAll(ECItemsToExport);
			// writeClearanceItem(clearanceItem);
			// emailClearanceItems();
			priceExportDao.insertECDataToHeader(conn, ECItems);
			priceExportDao.insertECDataToDetail(conn, ECItems);
		}

		if (ECItems.size() > 0) {
			
			// update export flag for clearance items
			logger.debug("updating expiry items..");
			updateECItems(ECItems);

			//logger.debug("Clearing the retail of clearance Items..");
			//priceExportDao.clearRetailOfClearanceItems(conn, ECItems);
		}
		
		postEffectiveDateChange(ECItems, emergency, runIdList, salesFloorFiltered);
		
		PristineDBUtil.commitTransaction(conn, "Price export update status");
		writeCSVForPriceExport(finalExportList, rootPath, fileNameConfig, fileName, sourceFile, targetFile);
		rename(sourceFile, targetFile);
		
	}

	private void postEffectiveDateChange(List<PRItemDTO> ecItems, boolean emergency, List<Long> runIdList, List<PRItemDTO> salesFloorFiltered) throws GeneralException {
		//post effective date change
		if (today.getDayOfWeek().equals(DayOfWeek.MONDAY) || today.getDayOfWeek().equals(DayOfWeek.TUESDAY)
				|| today.getDayOfWeek().equals(DayOfWeek.WEDNESDAY)) {
			if ((priceExportType.equals(Constants.EMERGENCY_OR_HARDPART) && emergency)
					|| (priceExportType.equals(Constants.ALL_SALESFLOOR_AND_HARDPART_AND_EMERGENCY) && emergency)
					|| (ecItems.size() > 0)) {
				logger.info("exportPrice() - Exchange Effective date for HardPart Items when price "
						+ "export Type is : " + priceExportType);
				List<PRItemDTO> leftoutHardPartItems = priceExportDao.getHardpartItemLeftout(conn,
						runIdList);
				if (leftoutHardPartItems.size() < 0 || leftoutHardPartItems.size() == 0) {
					logger.info("exportPrice() - No hard part items left on " + today + " batch run..");
				} else {
					logger.info("exportPrice() - # of left out hardpart item on " + today + " batch run is: "
							+ leftoutHardPartItems.size());
					priceExportDao.changeEffectiveDate(conn, leftoutHardPartItems, 1);
				}

			}
		}
		
		if(priceExportType.equals(Constants.BOTH_SALESFLOOR_AND_HARDPART) || priceExportType.equals(Constants.SALE_FLOOR_ITEMS)
				|| (priceExportType.equals(Constants.EMERGENCY_OR_SALESFLOOR))
				|| (priceExportType.equals(Constants.ALL_SALESFLOOR_AND_HARDPART_AND_EMERGENCY) && emergency)) {
			
			logger.info("exportPrice() - Exchange Effective date for SalesFloor Items when price "
					+ "export Type is : " + priceExportType);
			List<PRItemDTO> leftoutSalesFloorItems = priceExportDao.getSalesFloorItemsLeftout(conn, salesFloorFiltered);
			if(leftoutSalesFloorItems.size() < 0) {
				logger.info("exportPrice() - No sales floor items left on " + today + "batch run..");
			}
			else {
				logger.info("exportPrice() - # of left out hardpart item on " + today + "batch run is: "
						+ leftoutSalesFloorItems.size());
				priceExportDao.changeEffectiveDate(conn, leftoutSalesFloorItems, 7);
			}
		}
		
	}

	private void updateECItems(List<PRItemDTO> ecItems) throws GeneralException {

		List<PRItemDTO> clearanceItems = ecItems.stream()
				.filter(e -> e.getPriceCheckListTypeId() == Integer.parseInt(Constants.CLEARANCE_LIST_TYPE))
				.collect(Collectors.toList());

		List<PRItemDTO> emergencyItems = ecItems.stream()
				.filter(e -> e.getPriceCheckListTypeId() == Integer.parseInt(Constants.EMERGENCY_LIST_TYPE))
				.collect(Collectors.toList());
		if (clearanceItems.size() > 0) {
			priceExportDao.updateClearanceItemsStatus(conn, clearanceItems);
		}
		if (emergencyItems.size() > 0) {
			priceExportDao.updateEmergencyItemsStatus(conn, emergencyItems);
		}
	}

	public List<PRItemDTO> getEmergencyAndClearanceItemsFromItemList(List<PRItemDTO> finalExportList,
			String priceExportType, List<PRItemDTO> ECItemsToExport, HashMap<Integer, String> zoneIdandNameMap, HashMap<Integer, String> zoneIdandNumMap,
			boolean noDataInQueue, String globalZone,HashMap<String, Integer> storeZoneMap, List<Integer> excludeZoneIdForVirtualZone) {
		List<PRItemDTO> ECItemsList = new ArrayList<PRItemDTO>();
		
		if (today.getDayOfWeek().equals(DayOfWeek.MONDAY) || today.getDayOfWeek().equals(DayOfWeek.TUESDAY)
				|| today.getDayOfWeek().equals(DayOfWeek.WEDNESDAY)) {
			
		logger.info("getEmergencyAndClearanceItemsFromItemList() - Getting Emergency and Clearance Item List...");
		ECItemsList = priceExportDao.getEmergencyAndClearanceItems(conn);
			if (ECItemsList.size() > 0) {
				clearenceItemsFromItemList = ECItemsList.stream()
						.filter(e -> e.getPriceCheckListTypeId() == Integer.parseInt(Constants.CLEARANCE_LIST_TYPE))
						.collect(Collectors.toList());

				emergencyItemsFromItemList = ECItemsList.stream()
						.filter(e -> e.getPriceCheckListTypeId() == Integer.parseInt(Constants.EMERGENCY_LIST_TYPE))
						.collect(Collectors.toList());
				
				List<String> excludeEcomStores = getExcludedEcomStores();
				List<String> excludedStores = getStoresExcludeZoneLevelData();

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
									zoneItem.setItemCode(clearnaceItem.getItemCode());
									zoneItem.setItemType(clearnaceItem.getItemType());
									zoneItem.setPartNumber(clearnaceItem.getPartNumber());
									zoneItem.setPriceZoneId(zoneIDandNumEntry.getKey());
									zoneItem.setPriceZoneNo(zoneIDandNumEntry.getValue());
									zoneItem.setZoneName(zoneIdandNameMap.get(zoneIDandNumEntry.getKey()));
									zoneItem.setChildLocationLevelId(1);
									zoneItem.setRegEffDate(clearnaceItem.getStartDate());
									zoneItem.setRetailerItemCode(clearnaceItem.getRetailerItemCode());
									zoneItem.setApprovedBy(clearnaceItem.getApprovedBy());
									zoneItem.setApproverName(clearnaceItem.getApproverName());
									if (clearnaceItem.getStartDate() == null) {
										zoneItem.setRegEffDate(dateFormatter.format(now));
									} else {
										zoneItem.setRegEffDate(clearnaceItem.getStartDate());
									}
									zoneItem.setStoreLockExpiryFlag("A");
									zoneItem.setRecommendationUnit(
											priceExportDao.getRecomName(conn, zoneItem.getItemCode(), 7));

									ECItemsToExport.add(zoneItem);
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
													if (clearnaceItem.getStartDate() == null) {
														storeItem.setRegEffDate(dateFormatter.format(now));
													} else {
														storeItem.setRegEffDate(clearnaceItem.getStartDate());
													}
													storeItem.setStoreLockExpiryFlag("A");
													storeItem.setRecommendationUnit(priceExportDao.getRecomName(conn,
															zoneItem.getItemCode(), 7));
													ECItemsToExport.add(storeItem);
												}
											}
										}
									}
								}

							} catch (Exception ex) {
								logger.error(
										"getEmergencyAndClearanceItemsFromItemList() - Error when getting clearance items from item list - "
												+ ex.getMessage());
							}
						}
					}
				}
			if (emergencyItemsFromItemList.size() > 0) {
				for (PRItemDTO emergencyItem : emergencyItemsFromItemList) {

					try {

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
						if (emergencyItem.getPriceZoneNo() == null || emergencyItem.getPriceZoneNo().equals("")) {
							zoneItem.setPriceZoneNo("");
							zoneItem.setZoneName("");
						} else {
							zoneItem.setPriceZoneNo(emergencyItem.getPriceZoneNo());
							int zoneId = priceExportDao.getKey(zoneIdandNumMap, zoneItem.getPriceZoneNo());
							zoneItem.setPriceZoneId(zoneId);
							zoneItem.setZoneName(zoneIdandNameMap.get(zoneItem.getPriceZoneId()));
							zoneItem.setChildLocationLevelId(1);
						}
						if (emergencyItem.getStoreNo() == null || emergencyItem.getStoreNo().equals("")) {
							zoneItem.setStoreNo("");
						} else {
							zoneItem.setStoreNo(emergencyItem.getStoreNo());
							zoneItem.setPriceZoneId(storeZoneMap.get(zoneItem.getStoreNo()));
							zoneItem.setPriceZoneNo(zoneIdandNumMap.get(zoneItem.getPriceZoneId()));
							zoneItem.setZoneName(zoneIdandNameMap.get(zoneItem.getPriceZoneId()));
							zoneItem.setChildLocationLevelId(2);
						}
						
						zoneItem.setRetailerItemCode(emergencyItem.getRetailerItemCode());
						zoneItem.setPriceCheckListTypeId(emergencyItem.getPriceCheckListTypeId());
						zoneItem.setApprovedBy(emergencyItem.getApprovedBy());
						zoneItem.setApproverName(emergencyItem.getApproverName());
						zoneItem.setStoreLockExpiryFlag("A");
						zoneItem.setRegEffDate(emergencyItem.getStartDate());
						zoneItem.setItemCode(emergencyItem.getItemCode());
						zoneItem.setItemType(emergencyItem.getItemType());
						zoneItem.setPartNumber(emergencyItem.getPartNumber());
						zoneItem.setRecommendationUnit(priceExportDao.getRecomName(conn, zoneItem.getItemCode(), 7));
						if(emergencyItem.getStartDate() == null) {
							zoneItem.setRegEffDate(dateFormatter.format(now));
						}else {
							zoneItem.setRegEffDate(emergencyItem.getStartDate());
						}
						
						ECItemsToExport.add(zoneItem);

					} catch (Exception ex) {
						logger.error(
								"getEmergencyAndClearanceItemsFromItemList() - Error when getting Emergency and clearance items from item list - "
										+ ex.getMessage());
					}

				}
			}
		}
		}
		return ECItemsList;
	}

	private HashMap<Integer, List<PRItemDTO>> getStoreLockExpiryItems(List<PRItemDTO> finalExportList,
			HashMap<String, Integer> storeZoneMap, HashMap<Integer, String> zoneIdandNumMap, String priceExportType,
			boolean noDataInQueue, List<String> itemStoreCombinationsFromPriceTest, HashMap<String, String> storeNumIDMap) throws GeneralException {

		HashMap<Integer, List<PRItemDTO>> expiryItems = priceExportDao.setPriceForExpiryItems(false, conn,
				priceExportType);
		boolean isPriceTestDataAvailable = false;
		
		if(itemStoreCombinationsFromPriceTest.size()>0) {
			isPriceTestDataAvailable = true;
		}
		
		if (expiryItems.size() > 0) {

			logger.debug("setPriceForExpiryItems() - # in expiryItems: " + expiryItems.size());

			for (Map.Entry<Integer, List<PRItemDTO>> excludeStoreValue : expiryItems.entrySet()) {
				// List<String> itemCodeListOfExcludedStore = new ArrayList<String>();

				int itemCode = excludeStoreValue.getKey();
				for (PRItemDTO items : excludeStoreValue.getValue()) {
					// getting retailer_item_code for giving item code
					String retailerItemCode = new ItemDAO().getRetailerItemCode(conn, itemCode);

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
							logger.debug("setPriceForExpiryItems() - Candidate zone Number: " + zoneNum);
						}
					}

					logger.debug("setPriceForExpiryItems() - expiry store num in store lock items: "
							+ items.getStoreNo() + " where item code is " + itemCode + "it's price check list id is: "
							+ items.getPriceCheckListId());

					try {
						PRItemDTO storeInstanceClone = null;
						if (noDataInQueue) {
							storeInstanceClone = new PRItemDTO();
						} else {
							storeInstanceClone = (PRItemDTO) finalExportList.get(0).clone();
						}
						storeInstanceClone.setStoreNo(items.getStoreNo());
						storeInstanceClone.setPriceZoneNo(zoneNum);
						storeInstanceClone.setRetailerItemCode(retailerItemCode);
						storeInstanceClone.setStoreLockExpiryFlag("D");
						
						if(isPriceTestDataAvailable)
						{
							String storeId = storeNumIDMap.get(storeInstanceClone.getStoreNo());
							String Key = storeInstanceClone.getItemCode()+"_"+storeId;
							if(!itemStoreCombinationsFromPriceTest.contains(Key)) {
								finalExportList.add(storeInstanceClone);
							}
						}
						else {
							finalExportList.add(storeInstanceClone);	
						}
						
						logger.debug("setPriceForExpiryItems() - Final Export List after setting expiry items: "
								+ finalExportList.size());
					} catch (Exception e) {
						logger.error("setPriceForExpiryItems() - Error setting price for expiry items: "
								+ items.getStoreNo() + ", Item: " + items.getItemCode() + e);
					}
				}
			}
		}
		return expiryItems;
	}

	private List<PRItemDTO> setPriceForExcludedStores(List<PRItemDTO> itemsFiltered, String virtualZoneNum,
			HashMap<String, Integer> storeZoneMap, HashMap<Integer, String> zoneIdandNameMap)
			throws GeneralException, CloneNotSupportedException {

		List<PRItemDTO> finalExportList = new ArrayList<PRItemDTO>();

		HashMap<Integer, List<PRItemDTO>> exportDataMap = (HashMap<Integer, List<PRItemDTO>>) itemsFiltered.stream()
				.distinct().collect(Collectors.groupingBy(PRItemDTO::getItemCode));

		// getting day cal id
		RetailCalendarDAO retailCalendarDAO = new RetailCalendarDAO();
		String curWkStartDate = DateUtil.getWeekStartDate(0);
		RetailCalendarDTO retailCalendarDTO = retailCalendarDAO.getCalendarId(conn, curWkStartDate,
				Constants.CALENDAR_DAY);
		int dayCalId = retailCalendarDTO.getCalendarId();
		// int dayCalId = 4567;

		for (Map.Entry<Integer, List<PRItemDTO>> exportDataValue : exportDataMap.entrySet()) {

			int itemCode = exportDataValue.getKey();

			List<PRItemDTO> exportObj = exportDataMap.get(itemCode);

			for (PRItemDTO itemDTO : exportObj) {

				// getting excluded stores from store lock
				HashMap<String, Integer> excludedStoresFromItemList = priceExportDao.getExcludedStoresFromItemList(conn,
						itemCode, itemDTO.getPriceZoneId(), true);

				logger.debug("setPriceForExcludedStores() - # of records in store lock items: "
						+ excludedStoresFromItemList.size());

				if (excludedStoresFromItemList.size() > 0) {

					for (Map.Entry<String, Integer> excludeStoreValue : excludedStoresFromItemList.entrySet()) {
						List<String> itemCodeListOfExcludedStoreOfFutExpiryDate = new ArrayList<String>();

						String storeNum = "";
						int itemCodeFromExcludeList = excludeStoreValue.getValue();
						storeNum = excludeStoreValue.getKey();

						if (!itemCodeListOfExcludedStoreOfFutExpiryDate
								.contains(String.valueOf(itemCodeFromExcludeList))) {
							itemCodeListOfExcludedStoreOfFutExpiryDate.add(String.valueOf(itemCodeFromExcludeList));
						}
						String retailerItemCode = new ItemDAO().getRetailerItemCode(conn, itemCodeFromExcludeList);

						logger.debug("setPriceForExcludedStores() - excluded store num in store lock items: " + storeNum
								+ " # of item code is " + itemCodeListOfExcludedStoreOfFutExpiryDate.size()
								+ " and item code is" + itemCodeListOfExcludedStoreOfFutExpiryDate);
						// int calId = 4564;

						int priceZoneId = 0;
						if (!storeNum.isEmpty()) {
							if (storeZoneMap.containsKey(storeNum)) {
								priceZoneId = storeZoneMap.get(storeNum);
								logger.debug("setPriceForExcludedStores() - Candidate zone: " + priceZoneId);
							}
						}

						HashMap<String, RetailPriceDTO> priceOfItemsInStoreLockOfStoreLevel = priceDataDao
								.getRetailPrice(conn, storeNum, itemCodeListOfExcludedStoreOfFutExpiryDate, dayCalId,
										null, Constants.STORE_LEVEL_TYPE_ID);

						for (PRItemDTO itemDto : itemsFiltered) {
							if (itemDto.getItemCode() == itemCodeFromExcludeList) {
								if (itemDto.getPriceZoneId() == priceZoneId) {
									if (priceOfItemsInStoreLockOfStoreLevel
											.containsKey(String.valueOf(excludeStoreValue.getValue()))) {

										String zoneName = "";
										if (priceZoneId > 0) {
											zoneName = zoneIdandNameMap.get(priceZoneId);
										}

										RetailPriceDTO priceDataObj = priceOfItemsInStoreLockOfStoreLevel
												.get(String.valueOf(excludeStoreValue.getValue()));

										double retailPrice = priceDataObj.getRegPrice();
										try {
											PRItemDTO storeInstanceClone = (PRItemDTO) itemDto.clone();
											storeInstanceClone.setStoreNo(storeNum);
											storeInstanceClone.setRetailerItemCode(retailerItemCode);
											storeInstanceClone.setChildLocationLevelId(Constants.STORE_LEVEL_TYPE_ID);
											storeInstanceClone.setStoreLockExpiryFlag("A");
											storeInstanceClone.setPriceZoneId(priceZoneId);
											storeInstanceClone.setZoneName(zoneName);

											MultiplePrice currentPrice = new MultiplePrice(1, retailPrice);
											storeInstanceClone.setRecommendedRegPrice(currentPrice);
											storeInstanceClone.setVdpRetail(
													storeInstanceClone.getRecommendedRegPrice().getUnitPrice());
											storeInstanceClone.setDiffRetail(
													storeInstanceClone.getRecommendedRegPrice().getUnitPrice()
															- currentPrice.getUnitPrice());
											double diffInPrice = round(storeInstanceClone.getDiffRetail(), 2);
											storeInstanceClone.setDiffRetail(diffInPrice);
											finalExportList.add(storeInstanceClone);
											logger.debug(
													"setPriceForExcludedStores() - Final Export List after setting store level price: "
															+ finalExportList.size());
										} catch (Exception e) {
											logger.error(
													"setPriceForExcludedStores() - Error setting price for exclude store: "
															+ storeNum + ", Item: " + itemDto.getItemCode());
										}
									} else {
										logger.debug("setPriceForExcludedStores() - No price in RPI for item: "
												+ excludeStoreValue.getValue() + " for calendar id: " + dayCalId);
									}
								}
							}
						}
					}
				}
			}
		}

		return finalExportList;

	}

	private void AuditTrialExportData(Connection conn, List<Long> runIdList, List<PRItemDTO> itemsFiltered,
			boolean emergency) throws GeneralException {

		// int auditTrialTypeId;
		int statusCode = 0;
		HashMap<Long, List<PRItemDTO>> productLocationDataForRunId = priceExportDao.getProductLocationDetail(conn,
				runIdList);

		// commented since including detailMessage using JSON properties is not
		// implemented
		/*
		 * if (emergencyInHardPart || emergencyInSaleFloor) { HashMap<Integer,
		 * List<PRItemDTO>> finalListByEnergencyItems = (HashMap<Integer,
		 * List<PRItemDTO>>) itemsFiltered
		 * .stream().distinct().collect(Collectors.groupingBy(PRItemDTO::getItemCode));
		 * detailMessage = "Emergency or Clearance Pricing Export – " +
		 * finalListByEnergencyItems.size() + " of retail changes generate";
		 * 
		 * } else { HashMap<Integer, List<PRItemDTO>> finalListByItemTypeSalesFloor =
		 * (HashMap<Integer, List<PRItemDTO>>) itemsFiltered .stream().filter(e ->
		 * e.getItemType().equals(Constants.SALE_FLOOR_ITEMS)).distinct()
		 * .collect(Collectors.groupingBy(PRItemDTO::getItemCode)); detailMessage =
		 * "Sales Floor Pricing Export – " + finalListByItemTypeSalesFloor.size() +
		 * " of retail changes generate";
		 * 
		 * HashMap<Integer, List<PRItemDTO>> finalListByItemTypeHardPart =
		 * (HashMap<Integer, List<PRItemDTO>>) itemsFiltered .stream().filter(e ->
		 * e.getItemType().equals(Constants.HARD_PART_ITEMS)).distinct()
		 * .collect(Collectors.groupingBy(PRItemDTO::getItemCode)); detailMessage =
		 * "Hard Parts Pricing Export – " + finalListByItemTypeHardPart.size() +
		 * " of retail changes generate";
		 * 
		 * // auditTrialTypeId =
		 * AuditTrailTypeLookup.RECOMMENDATION.getAuditTrailTypeId(); }
		 */
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
			/*
					 * else if (emergency) { statusCode = PRConstants.EXPORT_EMERGENCY;
					 * logger.debug("AuditTrialExportData() - Exported Status code : " +
					 * statusCode); }
					 */
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

	public void callAuditTrail(int locationLevelId, int locationId, int productLevelId, int productId,
			int auditTrailTypeId, int statusCode, long runId, String batchUser, int auditType, int auditSubType, int subStatusType, 
			int countOfItems, int countOfStores, int exportTime) throws GeneralException {
		
		AuditTrailService auditTrailService = new AuditTrailService();
		auditTrailService.auditRecommendation(conn, locationLevelId, locationId, productLevelId, productId,
				auditTrailTypeId, statusCode, runId, batchUser,auditType, auditSubType, subStatusType, countOfItems, countOfStores, exportTime);

	}

	private List<PRItemDTO> applyGlobalZonePriceToAllZones(List<PRItemDTO> itemList,
			HashMap<Integer, String> zoneIdandNameMap, HashMap<String, Integer> zoneMap, String globalZone)
			throws GeneralException {
		List<PRItemDTO> allZonesMergedList = new ArrayList<PRItemDTO>();

		List<PRItemDTO> itemsFilteredByGlobalZone = new ArrayList<PRItemDTO>();

		// grouped by item code
		HashMap<Integer, List<PRItemDTO>> itemZoneMap = (HashMap<Integer, List<PRItemDTO>>) itemList.stream()
				.collect(Collectors.groupingBy(PRItemDTO::getItemCode));

		/*
		 * itemZoneMap.forEach((itemCode, itemZoneList) -> {
		 * itemsFilteredByGlobalZone.addAll(itemZoneList); });
		 */		
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
		//to set set global recom. true when any other zones are recommended along with zone 1000.
		//so that the particular item will not come in missing items (cosidered to take current price in max price calculation for virtual zone)
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

					//commented on 27/06/2020
					//since to make it true item wise, it is moved to line: 1039
					//item.setGlobalZoneRecommended(true);
					
					// globalZoneItems.add(item.getItemCode());

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
					logger.debug("No price recommended forGlobal zone");
					allZonesMergedList.add(item);
				}
			});
				//tempChange
			//});
		}

		return allZonesMergedList;
	}

	// getting runIdList for a recent quarter
	public List<Long> getRunIdList(String priceExportType, boolean emergencyInHardPart, boolean emergencyInSaleFloor)
			throws GeneralException, SQLException {

		/*
		 * RetailCalendarDAO retailCalendarDAO = new RetailCalendarDAO(); String
		 * curWkStartDate = DateUtil.getWeekStartDate(0); RetailCalendarDTO
		 * retailCalendarDTO = retailCalendarDAO.getCalendarId(conn, curWkStartDate,
		 * Constants.CALENDAR_QUARTER); RetailCalendarDTO retailStartDTO =
		 * retailCalendarDAO.getCalendarId(conn, retailCalendarDTO.getStartDate(),
		 * Constants.CALENDAR_WEEK); RetailCalendarDTO retailEndDTO =
		 * retailCalendarDAO.getCalendarId(conn, retailCalendarDTO.getEndDate(),
		 * Constants.CALENDAR_WEEK);
		 */
		return priceExportDao.getRunId(conn, priceExportType, emergencyInHardPart, emergencyInSaleFloor);
	}

	// handling SF items
	public List<PRItemDTO> filterSalesFloorItemsByAbsoluteImpact(List<PRItemDTO> salesFloorItems,
			String SFItemLimitStr) {

		int SFItemLimit = Integer.parseInt(SFItemLimitStr);

		List<PRItemDTO> finalExportList = new ArrayList<PRItemDTO>();
		List<PRItemDTO> itemsWithOverallImpact = new ArrayList<PRItemDTO>();
		String thresholdStr = PropertyManager.getProperty("THRESHOLD");
		int threshold = Integer.parseInt(thresholdStr);

		// map produced to set key as item code of non lig member
		HashMap<Integer, List<PRItemDTO>> totalExportListByItem = (HashMap<Integer, List<PRItemDTO>>) salesFloorItems
				.stream().collect(Collectors.groupingBy(PRItemDTO::getItemCode));

		// map produced to set key as item code of lig member
		HashMap<Integer, List<PRItemDTO>> exportListByLigItem = (HashMap<Integer, List<PRItemDTO>>) salesFloorItems
				.stream().filter(p -> p.getRetLirId() > 0).collect(Collectors.groupingBy(PRItemDTO::getRetLirId));

		for (Map.Entry<Integer, List<PRItemDTO>> totalExportListEntry : totalExportListByItem.entrySet()) {

			List<PRItemDTO> exportItemValues = totalExportListEntry.getValue();
			PRItemDTO exportItem = totalExportListEntry.getValue().get(0);
			PRItemDTO clonedObjOfExportItem;
			try {
				clonedObjOfExportItem = (PRItemDTO) exportItem.clone();

				double totalImpact = exportItemValues.stream().mapToDouble(p -> p.getImpact()).sum();
				clonedObjOfExportItem.setImpact(Math.abs(totalImpact));
				itemsWithOverallImpact.add(clonedObjOfExportItem);
				// exportItem.setImpact(exportItem.getI);
				logger.debug("filterSalesFloorItemsByAbsoluteImpact() - Total Impact: " + totalImpact);
				// sort in descending order of price change impact
				Comparator<PRItemDTO> compareByImpact = (PRItemDTO o1, PRItemDTO o2) -> o1.getImpact()
						.compareTo(o2.getImpact());

				Collections.sort(itemsWithOverallImpact, compareByImpact.reversed());

			} catch (CloneNotSupportedException e) {
				e.printStackTrace();
			}
		}
		int counter = 0;
		int thresholdMax = (int) (SFItemLimit + (SFItemLimit * (threshold / 100)));
		// iterating the ranking list
		for (PRItemDTO itemWithConsolidatedRanking : itemsWithOverallImpact) {

			List<PRItemDTO> membersInAllLocations = exportListByLigItem.get(itemWithConsolidatedRanking.getRetLirId());

			List<PRItemDTO> itemInAllLocations = totalExportListByItem.get(itemWithConsolidatedRanking.getItemCode());

			// group the ranking list by ret lir id
			HashMap<Integer, List<PRItemDTO>> rankingOrderByLig = (HashMap<Integer, List<PRItemDTO>>) itemsWithOverallImpact
					.stream().collect(Collectors.groupingBy(PRItemDTO::getRetLirId));

			if (!itemWithConsolidatedRanking.getIsMemberProcessed()) {
				// int diffPct = 0;

				// lig items
				if (itemWithConsolidatedRanking.getRetLirId() > 0) {

					logger.debug(
							"filterSalesFloorItemsByImpact() - LIG member: " + itemWithConsolidatedRanking.getItemCode()
									+ ", impact: " + itemWithConsolidatedRanking.getImpact());

					List<PRItemDTO> memberListObj = rankingOrderByLig.get(itemWithConsolidatedRanking.getRetLirId());
					counter = counter + memberListObj.size();

					// diffPct = ((counter - SFItemLimit) * thresholdMax) * 100;
					/*
					 * if (counter < SFItemLimit || (counter > SFItemLimit && diffPct <= threshold))
					 * {
					 */
					if (counter <= SFItemLimit || (counter > SFItemLimit && counter == thresholdMax)) {
						logger.debug("filterSalesFloorItemsByImpact() - Added " + memberListObj.size()
								+ " members for LIG: " + itemWithConsolidatedRanking.getRetLirId());
						memberListObj.forEach(item -> {
							logger.debug("filterSalesFloorItemsByAbsoluteImpact() - Member items of LIG group are: "
									+ item.getItemCode() + ", its impact: " + item.getImpact());
						});

						finalExportList.addAll(membersInAllLocations);
						logger.debug("filterSalesFloorItemsByImpact() - # of items added: " + counter);
						for (PRItemDTO flagSet : memberListObj) {
							flagSet.setIsMemberProcessed(true);
						}
					} else if (counter > SFItemLimit) {
						logger.debug("filterSalesFloorItemsByImpact() - Threshold Limit exceeded. Skipping "
								+ memberListObj.size() + " LIG members for LIG: "
								+ itemWithConsolidatedRanking.getRetLirId());
						logger.debug("filterSalesFloorItemsByImpact() - # of items added: " + counter
								+ ". Stopped adding items.");
						break;
					}

				}
				// non lig items
				else {
					logger.debug("filterSalesFloorItemsByImpact() - Non LIG item: "
							+ itemWithConsolidatedRanking.getItemCode() + ", impact: "
							+ itemWithConsolidatedRanking.getImpact());

					counter++;
					// diffPct = ((counter - SFItemLimit) * thresholdMax) * 100;
					/*
					 * if (counter < SFItemLimit || (counter > SFItemLimit && diffPct <= threshold))
					 * {
					 */
					if (counter <= SFItemLimit) {
						logger.debug("filterSalesFloorItemsByImpact() - Adding non LIG item: "
								+ itemWithConsolidatedRanking.getItemCode());
						finalExportList.addAll(itemInAllLocations);
						logger.debug("filterSalesFloorItemsByImpact() - # of items added: " + counter);
					} else if (counter > SFItemLimit) {
						logger.debug("filterSalesFloorItemsByImpact() - Limit exceeded. Skipping item: "
								+ itemWithConsolidatedRanking.getItemCode());
						logger.debug("filterSalesFloorItemsByImpact() - # of items added: " + counter
								+ ". Stopped adding items.");
						break;
					}
				}
			}
		}
		return finalExportList;
	}

	// setting up the price for zone 30
	public void setMaxPriceForVirtualZone(List<PRItemDTO> exportList, String virtualZoneNum,
			List<Integer> excludeZoneIdForVirtualZone, HashMap<Integer, String> zoneIdandNumMap)
			throws GeneralException {
		PriceExportDAO priceExportDao = new PriceExportDAO();
		int zoneIdForVirtualZone = getKey(priceExportDao.getZoneNoForZoneId(conn), virtualZoneNum);
		List<String> excludeStores = new ArrayList<>();
		for(Integer strId : excludeZoneIdForVirtualZone) {
			excludeStores.add(strId.toString());
		}
		HashMap<String, Integer> zoneIdMap = priceExportDao.getZoneIdMapForVirtualZone(conn, virtualZoneNum, excludeStores);
		
		String zoneName = priceExportDao.getZoneNameForVirtualZone(conn, virtualZoneNum);
		HashMap<String, RetailPriceDTO> missingZonePriceMap = new HashMap<String, RetailPriceDTO>();
		HashMap<Integer, List<PRItemDTO>> priceForVirtualZone = (HashMap<Integer, List<PRItemDTO>>) exportList.stream()
				.distinct().collect(Collectors.groupingBy(PRItemDTO::getItemCode));

		HashMap<Integer, List<Integer>> missingLocationAndItemMap = new HashMap<Integer, List<Integer>>();

		/*
		 * HashMap<Integer, List<PRItemDTO>> itemListInAllLocations =
		 * priceExportDao.getItemListInAllLocations(conn, priceExportType,
		 * emergencyInHardPart, emergencyInSaleFloor);
		 * 
		 * 
		 * priceForVirtualZone.forEach((itemCode, zoneItemList) -> { if
		 * (itemListInAllLocations.containsKey(itemCode)) { List<PRItemDTO>
		 * disapprovedLocations = itemListInAllLocations.get(itemCode);
		 * //logger.debug(disapprovedLocations.size()); for (PRItemDTO zoneItem :
		 * disapprovedLocations) { boolean isLocationFound = false; for (PRItemDTO
		 * zoneItemApproved : zoneItemList) { //logger.debug("" +
		 * zoneItemApproved.getPriceZoneId() + " " + zoneItem.getPriceZoneId()); if
		 * (zoneItemApproved.getPriceZoneId() == zoneItem.getPriceZoneId()) {
		 * isLocationFound = true; break; } }
		 * 
		 * if (!isLocationFound) { //logger.debug(zoneItem.getPriceZoneId());
		 * zoneItemList.add(zoneItem); } } } });
		 */

		priceForVirtualZone.forEach((itemCode, zoneItemList) -> {
			
			//if (!zoneItemList.get(0).isGlobalZoneRecommended()) {
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
						logger.info("priceForVirtualZone() - missingZone" + zoneId + " itemcode: " + itemCode);
						missingLocationAndItemMap.put(zoneId, tempList);
					}
				}
			//}
		});

		priceForVirtualZone.forEach((itemCode, zoneItemList) -> {
			try {
				PRItemDTO clonedValue = (PRItemDTO) zoneItemList.get(0).clone();
				//commented on 27/06/2020
				//to include missing zone 5 when zone 1000 is recommended
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

		RetailCalendarDTO retailCalendarDTO = retailCalendarDAO.getCalendarId(conn, curWkStartDate,
				Constants.CALENDAR_DAY);

		int dayCalId = retailCalendarDTO.getCalendarId();
		//int dayCalId = 4984;

		for (Map.Entry<Integer, List<Integer>> missingLocationEntry : missingLocationAndItemMap.entrySet()) {

			int missingLocation = missingLocationEntry.getKey();
			logger.info("missing location: " + missingLocation);
			List<Integer> itemCodeList = missingLocationEntry.getValue();

			List<String> tempItemCodeList = new ArrayList<String>();
			
				itemCodeList.forEach(itemCode -> {
					tempItemCodeList.add(String.valueOf(itemCode));
				});
			
			logger.debug("tempItemCodeList size: " + tempItemCodeList.size());

			String zoneNum = zoneIdandNumMap.get(missingLocation);
			// String zoneNum = getKey(priceExportDao.getZoneNoForZoneId(conn),
			// missingLocation);
			// logger.debug("" + zoneNum);

			// if (!excludeStoreListForVitualZone.contains(zoneNum)) {

			HashMap<String, RetailPriceDTO> priceForMissingZones = new HashMap<String, RetailPriceDTO>();

			PriceDataLoad priceDataDao = new PriceDataLoad();
			if(tempItemCodeList.size() > 0) {
				priceForMissingZones = priceDataDao.getRetailPrice(conn, zoneNum, tempItemCodeList, dayCalId, null,
						Constants.ZONE_LEVEL_TYPE_ID);
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
		}

		priceForVirtualZone.forEach((itemCode, itemList) -> {
			try {
				PRItemDTO virtualZone = (PRItemDTO) itemList.get(0).clone();

				MultiplePrice maxPrice = null;

				for (PRItemDTO item : itemList) {

					//added second case-  to not consider zone 30's current price in zone 30 calculation.
					if (!excludeZoneIdForVirtualZone.contains(item.getPriceZoneId())) {
						if (item.getPriceZoneId() != zoneIdForVirtualZone){
						
						MultiplePrice actualZonePrice = null;
						if (item.getOverriddenRegularPrice() != null
								&& item.getOverriddenRegularPrice().getUnitPrice() != 0.0) {
							actualZonePrice = item.getOverriddenRegularPrice();

						} else if (item.getRecommendedRegPrice() != null
								&& item.getRecommendedRegPrice().getUnitPrice() != 0.0) {
							actualZonePrice = item.getRecommendedRegPrice();

						} else {
							actualZonePrice = item.getCurrentRegPrice();
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
					virtualZone.setRecommendedRegPrice(maxPrice);
					virtualZone.setVdpRetail(virtualZone.getRecommendedRegPrice().getUnitPrice());
					if (missingZonePriceMap.containsKey(String.valueOf(itemCode))) {
						RetailPriceDTO priceDto = missingZonePriceMap.get(String.valueOf(itemCode));
						MultiplePrice currPrice = PRCommonUtil.getMultiplePrice(priceDto.getRegQty(),
								(double) priceDto.getRegPrice(), (double) priceDto.getRegMPrice());
						virtualZone.setCurrentRegPrice(currPrice);
						double diffInPrice = virtualZone.getRecommendedRegPrice().getUnitPrice()
								- virtualZone.getCurrentRegPrice().getUnitPrice();
						virtualZone.setDiffRetail(round(diffInPrice, 2));
					}
					
					virtualZone.setPriceZoneNo(virtualZoneNum);
					virtualZone.setPriceZoneId(zoneIdForVirtualZone);
					virtualZone.setZoneName(zoneName);
					virtualZone.setChildLocationLevelId(Constants.ZONE_LEVEL_TYPE_ID);
					exportList.add(virtualZone);
				}
			} catch (CloneNotSupportedException e) {
				e.printStackTrace();
			}
		});

		logger.info("setMaxPriceForVirtualZone() - # of records after virtual zone: " + exportList.size());

	}

	public static double round(double value, int places) {
		if (places < 0)
			throw new IllegalArgumentException();

		long factor = (long) Math.pow(10, places);
		value = value * factor;
		long tmp = Math.round(value);
		return (double) tmp / factor;
	}

	// explode zone level price to store level
	private List<PRItemDTO> setZoneLevelPrice(List<PRItemDTO> exportList, List<Integer> excludeZoneIdForVirtualZone) {
		
		HashMap<Integer, List<PRItemDTO>> exportDataMap = (HashMap<Integer, List<PRItemDTO>>) exportList.stream()
				.distinct().collect(Collectors.groupingBy(PRItemDTO::getItemCode));

		List<PRItemDTO> finalExportList = new ArrayList<PRItemDTO>();

		if(exportDataMap.size()>0) {
		exportDataMap.forEach((itemCode, exportObj) -> {
			exportObj.forEach(itemDTO -> {

				List<String> excludeEcomStores = getExcludedEcomStores();
				List<String> excludedStores = getStoresExcludeZoneLevelData();
				 

				PRItemDTO zoneLevelData = itemDTO;
				zoneLevelData.setChildLocationLevelId(Constants.ZONE_LEVEL_TYPE_ID);
				zoneLevelData.setStoreLockExpiryFlag("A");
				finalExportList.add(zoneLevelData);
				
				if(excludeZoneIdForVirtualZone.contains(zoneLevelData.getPriceZoneId())) {
				logger.info("setZoneLevelPrice() - Exploding store level data for zone id- " + zoneLevelData.getPriceZoneId());
				// gets store list for given zone
				 List<String> storeNumList = getStoreNoOfZones(itemDTO.getPriceZoneId());

				// get excluded stores from item list

					/*HashMap<String, Integer> excludedStoresFromItemList = priceExportDao.getExcludedStoresFromItemList(
							conn, itemDTO.getItemCode(), itemDTO.getPriceZoneId(), emergencyInSaleFloor, false, false);*/

					//List<String> excludedStoresFromExcel = priceExportDao.getExcludedStoresFromList(conn);
				 	if(storeNumList == null) {
				 		logger.info("No Stores are available for zone: " + itemDTO.getPriceZoneNo());
				 	}
				 	else if (storeNumList.size() > 0) {
						storeNumList.forEach(storeNo -> {

							if (!excludedStores.contains(storeNo)) {
								if(!excludeEcomStores.contains(storeNo)) {
								/*if (excludedStoresFromItemList.size() < 0 || excludedStoresFromItemList == null
										|| !excludedStoresFromItemList.containsKey(storeNo)) {*/

									try {
										PRItemDTO storeLevelData = (PRItemDTO) zoneLevelData.clone();
										storeLevelData.setStoreNo(storeNo); 
										storeLevelData.setChildLocationLevelId(Constants.STORE_LEVEL_TYPE_ID);
										storeLevelData.setStoreLockExpiryFlag("A");
										finalExportList.add(storeLevelData);
									} catch (CloneNotSupportedException e) {
										e.printStackTrace();
									}

								}
							}

						});
					}
			}
			});
		});
		}
		return finalExportList;
		
	}

	// Filter only price changed items
	public List<PRItemDTO> filterRecommendedOrOverridenItems(List<PRItemDTO> exportList) {

		List<PRItemDTO> priceChangedItems = new ArrayList<PRItemDTO>();
		// logger.info("overriden: "+ getOverriddenRegularPrice());

		List<PRItemDTO> priceChangeComparisonwithOverridenPrice = exportList.stream()
				.filter(e -> !(e.getOverriddenRegularPrice().equals(e.getCurrentRegPrice())))
				.collect(Collectors.toList());

		List<PRItemDTO> priceChangeComparisonwithRecPrice = priceChangeComparisonwithOverridenPrice.stream()
				.filter(e -> !(e.getRecommendedRegPrice().equals(e.getCurrentRegPrice()))).collect(Collectors.toList());

		priceChangedItems.addAll(priceChangeComparisonwithRecPrice);

		return priceChangedItems;
	}

	private void addNotification(Connection conn, List<Long> runIdList, boolean emergency)
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

	// write into file
	private void writeCSVForPriceExport(List<PRItemDTO> finalExportList, String rootPath, String fileNameConfig,
			String fileName, File sourceFile, File targetFile)
			throws IOException, GeneralException, SQLException, ParseException {
		
		
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
						pw.print("");
					}else {
					pw.print(item.getVdpRetail());
					}
				}
				pw.print(separtor);

				if (item.getStoreLockExpiryFlag().equals(Constants.EXPORT_DELETE_DATA)) {
					pw.print("0.00");
				} else {
					if (item.getCoreRetail() == null) {
						pw.print("");
					} else {
						pw.print(item.getCoreRetail());
					}
				}
				pw.print(separtor);

				if (item.getStoreLockExpiryFlag().equals(Constants.EXPORT_DELETE_DATA)) {
					pw.print("");
				} else {
					if(item.getVdpRetail() == null) {
						pw.print("");
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
						pw.print("");
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

	private void rename(File src, File target) throws Exception {
		if (src.equals(target)) {
			logger.warn("rename() - Source and target files are the same [" + src + "]. Skipping.");
			return;
		}

		if (src.exists()) {

			boolean result = src.renameTo(target);

			if (result) {
				logger.info("Renamed file [" + src + "] to [" + target + "]");

			} else {
				logger.warn("rename() - Failed to rename file [" + src + "] as [" + target + "].");
			}
		} else {
			throw new Exception("File [" + src + "] does not exist.");
		}

	}

	// NOT HANDLED SINCE STORE LEVEL EXPORT IS NOT REQUIRED.
	// REASON: CAN NOT MANAGE LARGE FILES

	/*
	 * // get stores of zone public List<String> getStoreNoOfZones(int priceZoneId)
	 * { return priceExportDao.getStoresOfZones(conn, priceZoneId); }
	 * 
	 * // helper method deriving the excluded Ecommerce stores private List<String>
	 * getExcludedEcomStores() { String ecommerceStores =
	 * PropertyManager.getProperty("ECOMMERCE_STORES"); String[]
	 * excludedEcomStoresArray = ecommerceStores.split(","); List<String>
	 * excludedEcomStoresList = Arrays.asList(excludedEcomStoresArray); return
	 * excludedEcomStoresList; }
	 * 
	 * // helper method deriving the excluded stores private List<String>
	 * getStoresExcludeZoneLevelData() { String excludedStores =
	 * PropertyManager.getProperty("STORES_EXCLUDE_ZONE_LEVEL_PRICES"); String[]
	 * excludedStoresArray = excludedStores.split(","); List<String>
	 * excludedStoresList = Arrays.asList(excludedStoresArray); return
	 * excludedStoresList; }
	 */

	public static <K, V> K getKey(Map<K, V> map, V value) {
		for (K key : map.keySet()) {
			if (value.equals(map.get(key))) {
				return key;
			}
		}
		return null;
	}

	/*
	 * private void renameFile(File sourceFile, File targetFile) throws IOException
	 * { FileUtils.moveFile(sourceFile, targetFile); }
	 */

	private void updateExpiryItemsStatus(HashMap<Integer, List<PRItemDTO>> expiryItems) throws GeneralException {
		List<PRItemDTO> expiryListItems = new ArrayList<PRItemDTO>();
		expiryItems.forEach((itemCode, expiryList) -> {
			expiryList.forEach(item -> {
				item.setItemCode(itemCode);
			});
			expiryListItems.addAll(expiryList);
		});

		List<PRItemDTO> expiryItemListAtStoreList = expiryListItems.stream().filter(e -> e.isStoreListExpiry())
				.collect(Collectors.toList());
		List<PRItemDTO> expiryItemListRegular = expiryListItems.stream().filter(e -> !e.isStoreListExpiry())
				.collect(Collectors.toList());

		if (expiryItemListRegular.size() > 0) {
			priceExportDao.updateExpiryExportFlagForRegularItemList(conn, expiryItemListRegular);
		}
		if (expiryItemListAtStoreList.size() > 0) {
			priceExportDao.updateExpiryExportFlagForStoreList(conn, expiryItemListAtStoreList);
		}
	}

	/**
	 * 
	 * @param exportListAtZoneLevel
	 * @param priceExportType2 
	 * @return store lock items
	 * @throws GeneralException
	 */
	private List<PRItemDTO> getStoreLockItems(List<PRItemDTO> exportListAtZoneLevel, String priceExportType) throws GeneralException {
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
		//int calendarId = 4984;
		
		int lockListTypeId = Integer.parseInt(PropertyManager.getProperty("PRICE_CHECK_LIST_TYPE_ID"));
		exportListAtZoneLevel.forEach(item -> {
			itemCodes.add(item.getItemCode());
			zones.add(item.getPriceZoneId());
		});

		HashMap<Integer, HashMap<Integer, Set<String>>> itemStoreLockMap = new HashMap<>();

		logger.info("getStoreLockItems() - Getting store lock from regular list...");
		priceExportDao.getExcludeStoresFromStoreLockList(conn, itemCodes, zones, lockListTypeId,
				Constants.STORE_LEVEL_ID, itemStoreLockMap, priceExportType);

		logger.info("getStoreLockItems() - Getting store lock from regular list is completed");

		logger.info("getStoreLockItems() - Getting store lock from store list level...");
		priceExportDao.getExcludeStoresFromStoreLockList(conn, itemCodes, zones, lockListTypeId,
				Constants.STORE_LIST_LEVEL_ID, itemStoreLockMap, priceExportType);

		logger.info("getStoreLockItems() - Getting store lock from store list level is completed");

		logger.info("getStoreLockItems() - # of items with store lock: " + itemStoreLockMap.size());

		logger.info("getStoreLockItems() - Getting price info for " + itemCodeStrSet.size() + " items...");

		itemStoreLockMap.keySet().forEach(itemCode -> {
			itemCodeStrSet.add(String.valueOf(itemCode));
		});

		HashMap<String, List<RetailPriceDTO>> priceDataMap = retailPriceDAO.getRetailPriceInfo(conn, itemCodeStrSet,
				calendarId, false);
		logger.info("getStoreLockItems() - Getting price info is compeleted");

		exportListAtZoneLevel.forEach(item -> {
			if (itemStoreLockMap.containsKey(item.getItemCode())) {
				HashMap<Integer, Set<String>> zoneStoreMap = itemStoreLockMap.get(item.getItemCode());
				if (zoneStoreMap.containsKey(item.getPriceZoneId())) {
					Set<String> stores = zoneStoreMap.get(item.getPriceZoneId());
					stores.forEach(store -> {
						try {
							PRItemDTO itemStoreObj = (PRItemDTO) item.clone();
							String itemCodeStr = String.valueOf(itemStoreObj.getItemCode());
							itemStoreObj.setStoreNo(store);
							itemStoreObj.setChildLocationLevelId(Constants.STORE_LEVEL_TYPE_ID);
							itemStoreObj.setStoreLockExpiryFlag("A");
							if (priceDataMap.containsKey(itemCodeStr)) {
								setCurrentPriceForLockedStore(priceDataMap, itemStoreObj, itemCodeStr);
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
		});

		if (priceNotFound.size() > 0) {
			logger.warn("getStoreLockItems() - Items with no current price in calendar id " + calendarId + ": "
					+ PRCommonUtil.getCommaSeperatedStringFromStrSet(priceNotFound));
		}

		return storeLockItems;
	}

	/**
	 * 
	 * @param priceDataMap
	 * @param itemStoreObj
	 */
	private void setCurrentPriceForLockedStore(HashMap<String, List<RetailPriceDTO>> priceDataMap,
			PRItemDTO itemStoreObj, String itemCodeStr) {
		if (priceDataMap.containsKey(itemCodeStr)) {
			List<RetailPriceDTO> priceList = priceDataMap.get(itemCodeStr);
			for (RetailPriceDTO priceDTO : priceList) {
				if (priceDTO.getLevelTypeId() == Constants.STORE_LEVEL_TYPE_ID
						&& itemStoreObj.getStoreNo().equals(priceDTO.getLevelId())) {
					double retailPrice = priceDTO.getRegPrice();
					MultiplePrice currentPrice = new MultiplePrice(1, retailPrice);
					itemStoreObj.setRecommendedRegPrice(currentPrice);
					itemStoreObj.setVdpRetail(itemStoreObj.getRecommendedRegPrice().getUnitPrice());
					itemStoreObj.setDiffRetail(0D);
					break;
				} else if (priceDTO.getLevelTypeId() == Constants.ZONE_LEVEL_TYPE_ID
						&& itemStoreObj.getPriceZoneNo().equals(priceDTO.getLevelId())) {
					double retailPrice = priceDTO.getRegPrice();
					MultiplePrice currentPrice = new MultiplePrice(1, retailPrice);
					itemStoreObj.setRecommendedRegPrice(currentPrice);
					itemStoreObj.setVdpRetail(itemStoreObj.getRecommendedRegPrice().getUnitPrice());
					itemStoreObj.setDiffRetail(0D);
					break;
				} else if (priceDTO.getLevelTypeId() == Constants.CHAIN_LEVEL_TYPE_ID) {
					double retailPrice = priceDTO.getRegPrice();
					MultiplePrice currentPrice = new MultiplePrice(1, retailPrice);
					itemStoreObj.setRecommendedRegPrice(currentPrice);
					itemStoreObj.setVdpRetail(itemStoreObj.getRecommendedRegPrice().getUnitPrice());
					itemStoreObj.setDiffRetail(0D);
					break;
				}
			}
		}
	}

	/**
	 * 
	 * @param rootPath
	 */
	private void clearErrorExportFiles(String rootPath) {
		// Lists all files in folder
		File folder = new File(rootPath);
		File fList[] = folder.listFiles();
		// Searchs .lck
		for (int i = 0; i < fList.length; i++) {
			File file = fList[i];
			if (file.getName().contains(".txt") || file.getName().contains(".done")) {
				file.delete();
			}
		}
	}
	
	private List<PRItemDTO> GetPriceTestStoreLevelData(Connection conn2, List<Long> testZoneRunIdList,
			List<String> itemStoreCombinationsFromPriceTest, String priceExportType2, boolean emergency, List<PRItemDTO> priceTestItemZoneLevelEntries, String currWeekEndDate) throws CloneNotSupportedException {
		List<PRItemDTO> PriceTestRecomData = new ArrayList<>();
		
		try {
			priceTestItemZoneLevelEntries.addAll(priceExportDao.getItemsFromApprovedRecommendations(conn, testZoneRunIdList,
					priceExportType, emergency,currWeekEndDate));
			
			HashMap<Integer, List<PRItemDTO>> ZoneItemMap = (HashMap<Integer, List<PRItemDTO>>) priceTestItemZoneLevelEntries.stream()
					.distinct().collect(Collectors.groupingBy(PRItemDTO::getPriceZoneId));
			
			HashMap<Integer,List<PRItemDTO>> ZoneStoreMap = new HashMap();
			ZoneStoreMap = priceExportDao.getTestZoneStoreCombinationsDictionary(conn, testZoneRunIdList);
			
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
								I.setStoreLockExpiryFlag("A");
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
	
	
	private void InsertPriceTestItemsBasedOnEmergencyClearenceItems(List<PRItemDTO> finalExportList,
			List<PRItemDTO> PriceTestItemStoreLevelEntries, List<PRItemDTO> eCItemsToExport,
			List<String> itemStoreCombinationsFromPriceTest, HashMap<String, String> storeNumIDMap) {
		List<String> DictReference = new ArrayList();
		for (PRItemDTO I : eCItemsToExport) {
			if (I.getChildLocationLevelId() == Constants.STORE_LEVEL_TYPE_ID) {
				String storeId = storeNumIDMap.get(I.getStoreNo());
				String Key = I.getItemCode() + "_" + storeId;
				DictReference.add(Key);
			}
		}

		for (PRItemDTO I : PriceTestItemStoreLevelEntries) {
			String storeId = storeNumIDMap.get(I.getStoreNo());
			String Key = I.getItemCode() + "_" + storeId;
			if (!DictReference.contains(Key)) {
				finalExportList.add(I);
			}
		}

	}
	
	// helper method deriving the excluded Ecommerce stores
		private List<String> getExcludedEcomStores() {
			String ecommerceStores = PropertyManager.getProperty("ECOMMERCE_STORES");
			String[] excludedEcomStoresArray = ecommerceStores.split(",");
			List<String> excludedEcomStoresList = Arrays.asList(excludedEcomStoresArray);
			return excludedEcomStoresList;
		}

		// helper method deriving the excluded stores
		private List<String> getStoresExcludeZoneLevelData() {
			String excludedStores = PropertyManager.getProperty("STORES_EXCLUDE_ZONE_LEVEL_PRICES");
			String[] excludedStoresArray = excludedStores.split(",");
			List<String> excludedStoresList = Arrays.asList(excludedStoresArray);
			return excludedStoresList;
		}

		// get stores of zone
		public List<String> getStoreNoOfZones(int priceZoneId) {
			return priceExportDao.getStoresOfZones(conn, priceZoneId);
		}
	
}
