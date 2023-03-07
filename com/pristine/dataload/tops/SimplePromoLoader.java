/**
 * Loads Promotions from Promo price feed into Promotion tables
 * for TOPS
 */
package com.pristine.dataload.tops;

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
import java.util.Collections;
import java.util.Date;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.concurrent.TimeUnit;

import org.apache.commons.io.FileUtils;
import org.apache.log4j.Logger;
import org.apache.log4j.PropertyConfigurator;

import com.pristine.dao.ItemDAO;
import com.pristine.dao.RetailCalendarDAO;
import com.pristine.dao.RetailCostDAO;
import com.pristine.dao.RetailPriceDAO;
import com.pristine.dao.RetailPriceZoneDAO;
import com.pristine.dao.StoreDAO;
import com.pristine.dao.StoreItemMapDAO;
import com.pristine.dao.offermgmt.promotion.PromotionDAO;
import com.pristine.dto.PromoDataDTO;
import com.pristine.dto.RetailCalendarDTO;
import com.pristine.dto.RetailCostDTO;
import com.pristine.dto.RetailerLikeItemGroupDTO;
import com.pristine.dto.StoreDTO;
import com.pristine.dto.StoreItemMapDTO;
import com.pristine.dto.offermgmt.MultiplePrice;
import com.pristine.dto.offermgmt.promotion.PromoBuyItems;
import com.pristine.dto.offermgmt.promotion.PromoBuyRequirement;
import com.pristine.dto.offermgmt.promotion.PromoDefinition;
import com.pristine.dto.offermgmt.promotion.PromoLocation;
import com.pristine.dto.offermgmt.promotion.PromoOfferDetail;
import com.pristine.exception.GeneralException;
import com.pristine.parsinginterface.PristineFileParser;
import com.pristine.util.Constants;
import com.pristine.util.DateUtil;
import com.pristine.util.PrestoUtil;
import com.pristine.util.PristineDBUtil;
import com.pristine.util.PropertyManager;

@SuppressWarnings("rawtypes")
public class SimplePromoLoader extends PristineFileParser {

	private static Logger logger = Logger.getLogger("PromoLoader");
	private Connection conn = null;
	private ItemDAO itemDAO = null;
	private RetailCalendarDAO calDAO = null;
	private RetailPriceDAO rpDAO = null;
	private RetailCostDAO rcDAO = null;

	int promoRecordCount = 0, chainId = -1;
	float minItemPrice = 0;
	List<PromoDataDTO> currentProcPromoList = new ArrayList<PromoDataDTO>();
	HashMap<String, Integer> calendarMap = new HashMap<String, Integer>();
	HashMap<String, Integer> weekCalendarMap = new HashMap<String, Integer>();
	HashMap<String, Integer> locationIdMap = new HashMap<String, Integer>();
	HashMap<String, Integer> storeNoAndId = new HashMap<String, Integer>();
	HashMap<Integer, StoreDTO> storeIdAndStore = new HashMap<Integer, StoreDTO>();
	HashMap<String, Integer> zoneNoAndId = new HashMap<String, Integer>();
	HashSet<String> noItemCodeList = new HashSet<String>();
	HashMap<String, List<Integer>> itemCodeMap = null;
	HashMap<Integer, Date> closedStores = null;
	HashMap<String, RetailerLikeItemGroupDTO> lirIdMap = null;
	HashMap<Integer, List<StoreItemMapDTO>> storeItemMap = new HashMap<Integer, List<StoreItemMapDTO>>();
	List<StoreDTO> storeDetails = null;
	int guStoreListId = 0, topsWithoutGUStoreListId = 0;
	List<String> ignoredStoreList = null;
	String processingDate = null;
	int NO_OF_FUTURE_WEEKS_TO_PROCESS = 0;
	List<Date> processingWeeks = null;
	DateFormat formatterMMddyy = new SimpleDateFormat("MM/dd/yy");
	DateFormat appDateFormatter = new SimpleDateFormat(Constants.APP_DATE_FORMAT);
	HashMap<SimplePromoKey, List<PromoDataDTO>> groupedByWeekAndItem = null;
	private boolean isTesting = Boolean.parseBoolean(PropertyManager.getProperty("IS_DEBUG_MODE", "FALSE"));
	boolean isLoadWeeklyAdAndDisplay = false;
	String weeklyAdFileRelativePath = "", displayFileRelativePath = "", overlayFileRelativePath = "", overlayTypeIDs="",
			weeklyAdAndDisplayFileSourceRelativePath = "";
	HashMap<String, RetailCalendarDTO> calendarCache = new HashMap<String, RetailCalendarDTO>();
	private HashMap<Integer, HashMap<String, String>> dsdAndWhseZoneMap = null;
 	private HashMap<Integer, Integer> itemCodeCategoryMap = null;
 	private boolean rollupDSDToWhseEnabled = false;
	public SimplePromoLoader() {
		StoreDAO storeDAO = new StoreDAO();
		StoreItemMapDAO storeItemMapDAO = new StoreItemMapDAO();
		conn = getOracleConnection();
		itemDAO = new ItemDAO();
		calDAO = new RetailCalendarDAO();
		storeDAO = new StoreDAO();
		rpDAO = new RetailPriceDAO();
		rcDAO = new RetailCostDAO();
		closedStores = new HashMap<Integer, Date>();
		NO_OF_FUTURE_WEEKS_TO_PROCESS = Integer.parseInt(PropertyManager.getProperty("NO_OF_FUTURE_PROMO_TO_BE_LOADED", "3"));
		guStoreListId = Integer.parseInt(PropertyManager.getProperty("TOPS_GU_STORE_LIST_ID", "0"));
		topsWithoutGUStoreListId = Integer.parseInt(PropertyManager.getProperty("TOPS_WITHOUT_GU_STORE_LIST_ID", "0"));

		try {
			chainId = Integer.parseInt(rpDAO.getChainId(conn));
			storeDetails = storeDAO.getStoresDetail(conn, chainId);

			for (StoreDTO storeDTO : storeDetails) {
				storeDTO.strNum = PrestoUtil.castStoreNumber(storeDTO.strNum.trim());
				storeDTO.zoneNum = storeDTO.zoneNum != null ? PrestoUtil.castZoneNumber(storeDTO.zoneNum.trim()) : "";

				storeNoAndId.put(storeDTO.strNum, storeDTO.strId);
				if (storeDTO.storeCloseDateAsDate != null) {
					closedStores.put(storeDTO.strId, storeDTO.storeCloseDateAsDate);
				}
				if (storeDTO.zoneNum != "")
					zoneNoAndId.put(storeDTO.zoneNum, storeDTO.zoneId);

				storeIdAndStore.put(storeDTO.strId, storeDTO);
			}
			//NU:: 7th Nov 2016, for Gaint Eagle, there won't zone id against the store in competitor_store table
			//So the zone no and its id is directly from retail price zone table
			if(zoneNoAndId.size() == 0) {
				RetailPriceZoneDAO retailPriceZoneDAO = new RetailPriceZoneDAO();
				zoneNoAndId = retailPriceZoneDAO.getZoneIdMap(conn);
			}
			
			if (!isTesting) {
				logger.info("Loading Store Item Map is started...");
				storeItemMap = storeItemMapDAO.getAuthorizedStoresOfItems(conn);
				logger.info("Loading Store Item Map is completed...");
				logger.info("Loading Retailer Item Code Map is started...");
				itemCodeMap = itemDAO.getRetailerItemCodeMap(conn);
				logger.info("Loading Retailer Item Code Map is completed...");
				logger.info("Loading Ret Lir Id is started...");
				lirIdMap = itemDAO.getAllRetLirIdWithAltItemCode(conn);
				logger.info("Loading Ret Lir Id is completed...");
				
				rollupDSDToWhseEnabled = Boolean.parseBoolean(PropertyManager.
						getProperty("ROLL_UP_DSD_TO_WARHOUSE_ZONE", "FALSE"));
				
				
				if(rollupDSDToWhseEnabled){
					logger.info("setupObjects() - ***ROLL_UP_DSD_TO_WARHOUSE_ZONE is enabled***");
					logger.info("setupObjects() - Getting DSD and Warehouse zone mapping...");
					dsdAndWhseZoneMap = rcDAO.getDSDAndWHSEZoneMap(conn, null);
					logger.info("setupObjects() - Getting DSD and Warehouse zone mapping is completed.");
					
					logger.info("setupObjects() - Getting Item and category mapping...");
					itemCodeCategoryMap = itemDAO.getCategoryAndItemCodeMap(conn, null);
					logger.info("setupObjects() - Getting Item and category mapping is completed.");
				}
				
			} else {
				storeItemMap = new HashMap<Integer, List<StoreItemMapDTO>>();
				itemCodeMap = new HashMap<String, List<Integer>>();
				lirIdMap = new HashMap<String, RetailerLikeItemGroupDTO>();
			}
		} catch (GeneralException ge) {

		}
		storeDAO = null;
		rpDAO = null;
	}

	/**
	 * Arguments args[0] Relative path of Promo File
	 * 
	 * @param args
	 */
	public static void main(String[] args) {
		SimplePromoLoader dataload = new SimplePromoLoader();
		PropertyConfigurator.configure("log4j-promo-loader.properties");
		String listFile = "";
		for (String arg : args) {
			if (arg.startsWith("LIST_FILE")) {
				listFile = arg.substring("LIST_FILE=".length());
			} else if (arg.startsWith("LOAD_AD_AND_DISPLAY=")) {
				dataload.isLoadWeeklyAdAndDisplay = Boolean.parseBoolean(arg.substring("LOAD_AD_AND_DISPLAY=".length()));
			} else if (arg.startsWith("WEEKLY_AD_FILE_RELATIVE_PATH=")) {
				dataload.weeklyAdFileRelativePath = arg.substring("WEEKLY_AD_FILE_RELATIVE_PATH=".length());
			} else if (arg.startsWith("DISPLAY_FILE_RELATIVE_PATH=")) {
				dataload.displayFileRelativePath = arg.substring("DISPLAY_FILE_RELATIVE_PATH=".length());
			} else if (arg.startsWith("AD_DISPLAY_FILE_SOURCE_RELATIVE_PATH=")) {
				dataload.weeklyAdAndDisplayFileSourceRelativePath = arg.substring("AD_DISPLAY_FILE_SOURCE_RELATIVE_PATH=".length());
			} else if (arg.startsWith("OVERLAY_FILE_RELATIVE_PATH=")) {
				dataload.overlayFileRelativePath = arg.substring("OVERLAY_FILE_RELATIVE_PATH=".length());
			}else if (arg.startsWith("OVERLAY_TYPE_IDS=")) {
				dataload.overlayTypeIDs = arg.substring("OVERLAY_TYPE_IDS=".length());
			}
		}
		dataload.processPromoFile(args[0], listFile);
	}

	@SuppressWarnings("unchecked")
	private void processPromoFile(String relativePath, String listFile) {
		String minPrice = PropertyManager.getProperty("SUBSCRIBER_DATALOAD.MIN_ITEM_PRICE", "0.10");
		minItemPrice = Float.parseFloat(minPrice);

		try {
			logger.info("Promo Data Load Started ....");

			// getzip files
			ArrayList<String> zipFileList = new ArrayList<String>();
			if (listFile.trim().isEmpty())
				zipFileList = getZipFiles(relativePath);
			else
				zipFileList = getFileList(relativePath, listFile);

			// Start with -1 so that if any regular files are present, they are
			// processed first
			int curZipFileCount = -1;
			boolean processZipFile = false;

			String zipFilePath = getRootPath() + "/" + relativePath;
			do {
				ArrayList<String> fileList = null; 
				try {
					if (processZipFile) {
						String fileName = zipFileList.get(curZipFileCount);
						String[] splittedName = fileName.split("_");
						// promotion_07182015.zip
						// For loading historic data, append processing week
						// date in the file like with '-'. 
						if (splittedName.length > 1) {
							// Parse year, month and day from filename to form
							// the processing week date.
							String partTwo = splittedName[1];
							String month = partTwo.substring(0, 2);
							String day = partTwo.substring(2, 4);
							String year = partTwo.substring(4, 8);
							processingDate = month + "/" + day + "/" + year;

						}
						PrestoUtil.unzip(zipFileList.get(curZipFileCount), zipFilePath);
					}

					fileList = getFiles(relativePath);

					for (int i = 0; i < fileList.size(); i++) {
						String archivePath = getRootPath() + "/" + relativePath + "/";
						String files = fileList.get(i);
						String fileName = (processZipFile ? zipFileList.get(curZipFileCount) : files);
						String fieldNames[] = setFieldNames();
						int stopCount = Integer.parseInt(PropertyManager.getProperty("DATALOAD.STOP_AFTER", "-1"));
						try {
							logger.info("Processing of file " + fileName + " is Started...");
							clearGlobalVariables();
							if (!processZipFile) {
								String[] splittedName = files.split("_");
								// promotion_07182015.txt
								// For loading historic data
								if (splittedName.length > 1) {
									// Parse year, month and day from filename
									// to
									// form the processing week date.
									String partTwo = splittedName[1];
									String month = partTwo.substring(0, 2);
									String day = partTwo.substring(2, 4);
									String year = partTwo.substring(4, 8);
									processingDate = month + "/" + day + "/" + year;

								}
							}
							super.headerPresent = true;
							super.parseDelimitedFile(PromoDataDTO.class, files, '|', fieldNames, stopCount);
							processPromoRecords(currentProcPromoList);

							StringBuilder sb = new StringBuilder("Retailer item codes which are not availble in DB\n");
							for (String retailerItemCode : noItemCodeList) {
								sb.append(retailerItemCode + ",");
							}
							logger.info(sb);

							PristineDBUtil.commitTransaction(conn, "Promo Loading Commit");
							logger.info("Transaction is committed for file  " + fileName);
							PrestoUtil.moveFile(fileName, archivePath + Constants.COMPLETED_FOLDER);
							logger.info("Processing of file " + fileName + " is Completed...");

							if (isLoadWeeklyAdAndDisplay) {
								loadAdAndDisplay();
							}
							
						} catch (GeneralException | Exception ex) {
							logger.error("Inner Exception - GeneralException", ex);
							logger.error("Exception while processing the file " + fileName + ". It is not processed...");
							PristineDBUtil.rollbackTransaction(conn, "Exception in processPromoRecords of PromoDataLoad");
							logger.info("Transaction is rollbacked for file  " + fileName);
							PrestoUtil.moveFile(fileName, archivePath + Constants.BAD_FOLDER);
						}
					}

					if (processZipFile) {
						PrestoUtil.deleteFiles(fileList);
						fileList.clear();
					}
				} catch (GeneralException | Exception ex) {
					logger.error("Inner Exception - GeneralException", ex);
				}

				curZipFileCount++;
				processZipFile = true;
			} while (curZipFileCount < zipFileList.size());
		} catch (GeneralException ex) {
			logger.error("Outer Exception -  GeneralException", ex);
		} catch (Exception ex) {
			logger.error("Outer Exception - JavaException", ex);
		}

		// super.performCloseOperation(conn, true);
		PristineDBUtil.close(conn);
		return;
	}
	
	private void loadAdAndDisplay() {
		logger.info("Loading of Weekly Ad and Display is Started...");
		
		String adAndDisplaySourcePath = getRootPath() + "/" + weeklyAdAndDisplayFileSourceRelativePath;
		String adInputPath = getRootPath() + "/" + weeklyAdFileRelativePath;
		String displayInputPath = getRootPath() + "/" + displayFileRelativePath;
		String overlayInputPath = getRootPath() + "/" + overlayFileRelativePath;
		List<String> zipFilesForAdAndDisplay = new ArrayList<String>();

		try {
			ArrayList<String> allSourceZipFiles = PrestoUtil.getAllFilesInADirectory(adAndDisplaySourcePath, "zip");
			for (Date processingWeekStartDate : processingWeeks) {
				SimpleDateFormat nf = new SimpleDateFormat("MM_dd_yy");
				String dateToLookFor = nf.format(processingWeekStartDate);
				boolean isFilePresent = false;
				
				for (String sourceZipFile : allSourceZipFiles) {
					//Check if the file is present in source folder
					isFilePresent = PrestoUtil.checkIfFileIsPresentInAZipFile(sourceZipFile, dateToLookFor);
					if(isFilePresent) {
						zipFilesForAdAndDisplay.add(sourceZipFile);
						break;
					}
				}

				if (!isFilePresent) {
					logger.warn("No matching Ad file found for week: " + dateToLookFor);
				}
			}

		} catch (Exception | GeneralException ex) {
			logger.error("Error while finding Weekly Ad or Display File");
		}

		try {
			boolean copyAdAndDisplayFile = Boolean.parseBoolean(PropertyManager.getProperty("COPY_AD_AND_DISPLAY_FILES", "TRUE"));
			logger.debug("No of Ad Files: " + zipFilesForAdAndDisplay.size());
			//To process Tops by copying Ad, Display and overlays files
			if (copyAdAndDisplayFile && zipFilesForAdAndDisplay.size() > 0) {
				//Copy the files to ad and display input folders
				for (String sourceAdPath : zipFilesForAdAndDisplay) {
					String destPath = "";
					File sourceFilePath = new File(sourceAdPath);
					File tempFile = new File(sourceAdPath);
					
					//Copy to Ad loader input folder
					destPath = adInputPath + "/" + tempFile.getName();
					File destFilePath = new File(destPath);
					logger.debug("Source Path: " + sourceFilePath + ",Dest Path: " + destFilePath);
					FileUtils.copyFile(sourceFilePath, destFilePath);
					
					//Copy to Display loader input folder
					destPath = "";
					destPath = displayInputPath + "/" + tempFile.getName();
					destFilePath = new File(destPath);
					logger.debug("Source Path: " + sourceFilePath + ",Dest Path: " + destFilePath);
					FileUtils.copyFile(sourceFilePath, destFilePath);
					
					//Copy to Overlay input folder
					destPath = "";
					destPath = overlayInputPath + "/" + tempFile.getName();
					destFilePath = new File(destPath);
					logger.debug("Source Path: " + sourceFilePath + ",Dest Path: " + destFilePath);
					FileUtils.copyFile(sourceFilePath, destFilePath);
				}

				// Call the Weekly Ad Loader
				if(weeklyAdFileRelativePath != null && !weeklyAdFileRelativePath.isEmpty()){
					String argsAd[] = { "ADPLEX_FILE_PATH="+ weeklyAdFileRelativePath };
					AdplexLoader.main(argsAd);
				}
				// Call the Display Ad Loader
				if(displayFileRelativePath != null && !displayFileRelativePath.isEmpty()){
					String argsDisplay[] = { displayFileRelativePath };
					DisplayTypeLoader.main(argsDisplay);
				}
				// Call the overlay Ad Loader
				if(overlayFileRelativePath != null && !overlayFileRelativePath.isEmpty() && overlayTypeIDs !=null && !overlayTypeIDs.isEmpty()){
					String argsOverlay[] = { "OVERLAY_FILE_RELATIVE_PATH="+overlayFileRelativePath, "OVERLAY_TYPE_IDS="+overlayTypeIDs };
					LoadOverlays.main(argsOverlay);
				}
			} 
			//To process Adplex file in GE
			else if(weeklyAdFileRelativePath != null && !weeklyAdFileRelativePath.isEmpty()){
				String argsAd[] = { "ADPLEX_FILE_PATH="+ weeklyAdFileRelativePath };
				AdplexLoader.main(argsAd);
			}else {
				logger.info("No Ad, Display or Overlay file found...");
			}
		} catch (Exception ex) {
			logger.error("Error while loading Weekly Ad or Display File");
		}

		logger.info("Loading of Weekly Ad and Display is Completed...");
	}

	private void clearGlobalVariables() {
		groupedByWeekAndItem = new HashMap<SimplePromoKey, List<PromoDataDTO>>();
		processingWeeks = new ArrayList<Date>();
		promoRecordCount = 0;
		noItemCodeList = new HashSet<String>();
		currentProcPromoList.clear();
	}

	private String[] setFieldNames() {
		String fieldNames[] = new String[22];
		fieldNames[0] = "buyerCode";
		fieldNames[1] = "adDate1";
		fieldNames[2] = "adDate2";
		fieldNames[3] = "adDate3";
		fieldNames[4] = "adDate4";
		fieldNames[5] = "promoStartDate";
		fieldNames[6] = "promoEndDate";
		fieldNames[7] = "sourceVendorNo";
		fieldNames[8] = "itemNo";
		fieldNames[9] = "itemDesc";
		fieldNames[10] = "priceZone";
		fieldNames[11] = "storeNo";
		fieldNames[12] = "storeZoneData";
		fieldNames[13] = "regQty";
		fieldNames[14] = "regPrice";
		fieldNames[15] = "saleQty";
		fieldNames[16] = "salePrice";
		fieldNames[17] = "saveAmt";
		fieldNames[18] = "pageNo";
		fieldNames[19] = "blockNo";
		fieldNames[20] = "userChange";
		fieldNames[21] = "promoNo";
		return fieldNames;
	}

	@SuppressWarnings("unchecked")
	@Override
	public void processRecords(List listobj) throws GeneralException {
		List<PromoDataDTO> promoDataList = (List<PromoDataDTO>) listobj;
		if (promoDataList != null && promoDataList.size() > 0) {
			currentProcPromoList.addAll(promoDataList);
		}
		// fileReadCount = fileReadCount + 1;
	}

	/***
	 * Group data by start, end date, item code
	 * 
	 * @param promoDataList
	 * @throws ParseException
	 * @throws GeneralException
	 */
	private void processPromoRecords(List<PromoDataDTO> promoDataList) throws ParseException, GeneralException {
		HashMap<SimplePromoKey, List<PromoDataDTO>> promotionMap = new HashMap<SimplePromoKey, List<PromoDataDTO>>();
		try {
			int productId = 0;
			String retailerItemCode = "", lirIdOrRetailerItemCode = "";
			SimplePromoKey simplePromoKey = null;
			logger.debug("size of promoDataList:" + promoDataList.size());

			for (PromoDataDTO promoDTO : promoDataList) {
				retailerItemCode = "";
				lirIdOrRetailerItemCode = "";
				promoRecordCount++;
				if (promoRecordCount % 100000 == 0)
					logger.info("No of promo records Processed " + promoRecordCount);

				if (promoDTO.getSalePrice() < 0.01f && promoDTO.getSaleQty() < 0.01f) {
					continue;
				}

				if (!isValidPromoNo(promoDTO.getPromoNo())) {
					continue;
				}

				if (promoDTO.getRegPrice() <= minItemPrice)
					continue;
				
				retailerItemCode = promoDTO.getSourceVendorNo() + promoDTO.getItemNo();
				productId = Constants.ITEMLEVELID;
				
				if(promoDTO.getItemDesc() == null || (promoDTO.getItemDesc() != null &&  promoDTO.getItemDesc().equals(""))) {
					logger.info("Item with no Name: VendorNo: " + promoDTO.getSourceVendorNo()  + ",Item No" + promoDTO.getItemNo());
					if(promoDTO.getPromoNo() != "") {
						promoDTO.setItemDesc(promoDTO.getPromoNo());
					}
					else {
						promoDTO.setItemDesc(promoDTO.getSourceVendorNo()  + "-" + promoDTO.getItemNo());
					}
				}

				if (itemCodeMap.get(retailerItemCode) == null) {
					// If retailer item code is not found in Presto DB
					noItemCodeList.add(retailerItemCode);
					continue;
				}
				lirIdOrRetailerItemCode = retailerItemCode;

				simplePromoKey = new SimplePromoKey(promoDTO.getPromoStartDate(), promoDTO.getPromoEndDate(), productId, lirIdOrRetailerItemCode);
				promoDTO.setNoOfDaysInPromoDuration(updateNoOfDaysInPromoDuration(simplePromoKey));
				//
				List<PromoDataDTO> promoDataDTO = null;
				if (promotionMap.get(simplePromoKey) != null)
					promoDataDTO = promotionMap.get(simplePromoKey);
				else
					promoDataDTO = new ArrayList<PromoDataDTO>();

				promoDataDTO.add(promoDTO);
				promotionMap.put(simplePromoKey, promoDataDTO);
			}
			logger.debug("size of promotionMap:" + promotionMap.size());
			logger.debug("Breaking of promotions to weekly is started...");

			// Process for current and next 6 weeks, if processing date is
			// passed as input then run only for that week
			HashMap<SimplePromoKey, Integer> processedPromoSet = new HashMap<SimplePromoKey, Integer>();
			if (processingDate == null) {
				// get current week end date
				Calendar cal = Calendar.getInstance();
				String dateInString = appDateFormatter.format(cal.getTime());
				Date today = appDateFormatter.parse(dateInString);
				Date currentWeekStartDate = getFirstDateOfWeek(today);
				Date currentWeekEndDate = getLastDateOfWeek(today);
				logger.debug("today:" + today + ",currentWeekStartDate:" + currentWeekStartDate + ",currentWeekEndDate:" + currentWeekEndDate);
				// Get current week promotions
				breakToWeeklyPromotions(currentWeekStartDate, currentWeekEndDate, processedPromoSet, promotionMap);
				processingWeeks.add(currentWeekEndDate);
				// Get future week promotions
				for (int i = 0; i < NO_OF_FUTURE_WEEKS_TO_PROCESS; i++) {
					Date futureWeekStartDate = getFirstDateOfWeek(DateUtil.incrementDate(currentWeekEndDate, (7 * (i + 1))));
					Date futureWeekEndDate = getLastDateOfWeek(futureWeekStartDate);
					processingWeeks.add(futureWeekEndDate);
					logger.debug("futureWeekStartDate:" + futureWeekStartDate + ",futureWeekEndDate:" + futureWeekEndDate);
					breakToWeeklyPromotions(futureWeekStartDate, futureWeekEndDate, processedPromoSet, promotionMap);
				}
			} else {
				Date procDate = appDateFormatter.parse(processingDate);
				processingWeeks.add(procDate);
				logger.debug("procDate:" + procDate);
				// Get processing week promotions
				breakToWeeklyPromotions(getFirstDateOfWeek(procDate), getLastDateOfWeek(procDate), processedPromoSet, promotionMap);
			}
			logger.debug("Breaking of promotions to weekly is completed...");
			// For debugging alone
			/*
			 * for (Map.Entry<SimplePromoKey, List<PromoDataDTO>> entry :
			 * groupedByWeekAndItem.entrySet()) { logger.debug("Key:" +
			 * entry.getKey().toString()); String items = "", promoType = "", sp
			 * = ""; for (PromoDataDTO promoDataDTO : entry.getValue()) { items
			 * = items + "," + promoDataDTO.getSourceVendorNo() + "|" +
			 * promoDataDTO.getItemNo(); promoTypeId =
			 * findPromoTypeId(promoDataDTO); salePrice = new
			 * MultiplePrice(promoDataDTO.getSaleQty(),
			 * Double.valueOf(String.valueOf(promoDataDTO.getSalePrice())));
			 * promoType = promoType + "," + promoTypeId; sp = sp + "," +
			 * salePrice; } logger.debug("RetailerItemCodes:" + items +
			 * ",promoTypeId:" + promoType + ",salePrice:" + sp); }
			 */

			loadPromoData();

		} catch (GeneralException e) {
			e.printStackTrace();
			logger.error("Exception in processPromoRecords of PromoDataLoad - " + e.toString() + e + e.getMessage());

			throw new GeneralException("Exception in processPromoRecords of PromoDataLoad", e);
		}
	}

	// private int findPromoTypeId(PromoDataDTO promoDTO) {
	// int promoTypeId = 0;
	// if (Math.abs(promoDTO.getRegPrice() - promoDTO.getSalePrice()) < 0.01f &&
	// promoDTO.getSaleQty() == 1 && promoDTO.getRegPrice() > 0) {
	// promoTypeId = Constants.PROMO_TYPE_BOGO;
	//
	// } else {
	// promoTypeId = Constants.PROMO_TYPE_STANDARD;
	// }
	// return promoTypeId;
	// }

	private void breakToWeeklyPromotions(Date weekStartDate, Date weekEndDate, HashMap<SimplePromoKey, Integer> processedPromoSet,
			HashMap<SimplePromoKey, List<PromoDataDTO>> promotionMap) throws ParseException, GeneralException {
		String startDate = appDateFormatter.format(weekStartDate);
		String endDate = appDateFormatter.format(weekEndDate);
		RetailCalendarDTO startCalendarDTO = calDAO.getCalendarId(conn, startDate, Constants.CALENDAR_DAY);
		RetailCalendarDTO endCalendarDTO = calDAO.getCalendarId(conn, endDate, Constants.CALENDAR_DAY);
		// finalPromotionMap will have a week, a ret lir id or item code and all
		// its item
		// If calendar id is not available, ignore it
		if (startCalendarDTO.getCalendarId() > 0 && endCalendarDTO.getCalendarId() > 0) {
			calendarMap.put(startDate, startCalendarDTO.getCalendarId());
			calendarMap.put(endDate, endCalendarDTO.getCalendarId());

			// Look for promotion defined for the week
			for (Map.Entry<SimplePromoKey, List<PromoDataDTO>> entry : promotionMap.entrySet()) {
				SimplePromoKey spk = entry.getKey();

				// Convert actual promo start date to week start date
				// and actual promo end date to week end date, so that if the
				// current processing week range is is checked easily
				Date startDateOfPromo = getFirstDateOfWeek(formatterMMddyy.parse(spk.getPromoStartDate()));
				Date endDateOfPromo = getLastDateOfWeek(formatterMMddyy.parse(spk.getPromoEndDate()));

				// check if start or end date falls between the actual promo
				// date range
				if (((weekStartDate.equals(startDateOfPromo) || weekStartDate.after(startDateOfPromo))
						&& ((weekEndDate.equals(endDateOfPromo) || weekEndDate.before(endDateOfPromo))))) {

					SimplePromoKey weekBasedPromoKey = new SimplePromoKey(appDateFormatter.format(weekStartDate),
							appDateFormatter.format(weekEndDate), spk.getProductLevelId(), spk.getProductId());

					List<PromoDataDTO> itemsInAWeek = new ArrayList<PromoDataDTO>();
					if (groupedByWeekAndItem.get(weekBasedPromoKey) != null) {
						itemsInAWeek = groupedByWeekAndItem.get(weekBasedPromoKey);
					}
					itemsInAWeek.addAll(entry.getValue());
					groupedByWeekAndItem.put(weekBasedPromoKey, itemsInAWeek);
				}
			}
		}
	}

	private long updateNoOfDaysInPromoDuration(SimplePromoKey simplePromoKey) throws ParseException, GeneralException {
		Date endDateOfPromo = getLastDateOfWeek(formatterMMddyy.parse(simplePromoKey.getPromoEndDate()));
		Date startDateTemp = getFirstDateOfWeek(formatterMMddyy.parse(simplePromoKey.getPromoStartDate()));

		long diff = endDateOfPromo.getTime() - startDateTemp.getTime();
		// System.out.println ("Days: " + TimeUnit.DAYS.convert(diff,
		// TimeUnit.MILLISECONDS));
		return TimeUnit.DAYS.convert(diff, TimeUnit.MILLISECONDS);
	}

	/*private Date getFirstDateOfWeek(Date inputDate) throws ParseException {
		Calendar cal = Calendar.getInstance();
		cal.setTime(inputDate);
		int weekStartDayIndex = cal.getFirstDayOfWeek();
		if(PropertyManager.getProperty("CUSTOM_WEEK_START_INDEX")!=null) {
			weekStartDayIndex = Integer.parseInt(PropertyManager.getProperty("CUSTOM_WEEK_START_INDEX", String.valueOf(cal.getFirstDayOfWeek())));
			weekStartDayIndex = weekStartDayIndex + 1;
		}
		 
		//int startDay = (cal.get(Calendar.DAY_OF_WEEK) - cal.getFirstDayOfWeek());
		int startDay = (cal.get(Calendar.DAY_OF_WEEK) - weekStartDayIndex);
		Date outputDate = DateUtil.incrementDate(inputDate, -startDay);
		return outputDate;
	}

	private Date getLastDateOfWeek(Date inputDate) throws ParseException {
		Calendar cal = Calendar.getInstance();
		cal.setTime(inputDate);
		Date outputDate = DateUtil.incrementDate(inputDate, 7 - cal.get(Calendar.DAY_OF_WEEK));
		return outputDate;
	}*/
	
	
	private Date getFirstDateOfWeek(Date inputDate) throws ParseException {
		String strDate = DateUtil.getWeekStartDate(inputDate, 0);
		Date outputDate = appDateFormatter.parse(strDate);
		return outputDate;
	}

	private Date getLastDateOfWeek(Date inputDate) throws ParseException {
		String strDate = DateUtil.getWeekStartDate(inputDate, 0);
		Date endDate = appDateFormatter.parse(strDate);
		Date outputDate = DateUtil.incrementDate(endDate, 6);
		return outputDate;
	}
	/*private Date getFirstDateOfWeek(Date inputDate) throws ParseException, GeneralException {
		RetailCalendarDAO retailCalendarDAO = new RetailCalendarDAO();
		RetailCalendarDTO retailCalendarDTO = null;
		
		if(calendarCache.get(appDateFormatter.format(inputDate)) != null) {
			retailCalendarDTO = calendarCache.get(appDateFormatter.format(inputDate));
		} else {
			retailCalendarDTO = retailCalendarDAO.getCalendarId(conn, appDateFormatter.format(inputDate), Constants.CALENDAR_WEEK);
			calendarCache.put(appDateFormatter.format(inputDate), retailCalendarDTO);
		}
		
		return formatterMMddyy.parse(retailCalendarDTO.getStartDate());
	}

	private Date getLastDateOfWeek(Date inputDate) throws ParseException, GeneralException {
		RetailCalendarDAO retailCalendarDAO = new RetailCalendarDAO();
		RetailCalendarDTO retailCalendarDTO = null;
		
		if(calendarCache.get(appDateFormatter.format(inputDate)) != null) {
			retailCalendarDTO = calendarCache.get(appDateFormatter.format(inputDate));
		} else {
			retailCalendarDTO = retailCalendarDAO.getCalendarId(conn, appDateFormatter.format(inputDate), Constants.CALENDAR_WEEK);
			calendarCache.put(appDateFormatter.format(inputDate), retailCalendarDTO);
		}
		return formatterMMddyy.parse(retailCalendarDTO.getEndDate());
	}*/
	
	/**
	 * Processes Promo records (Populates promoDataMap with Promo number as key
	 * and corresponding list of promo records as value)
	 * 
	 * @param promoDataList
	 *            List of Promo records to be processed
	 */
	private void loadPromoData() throws GeneralException {
		/**
		 * groupedByWeekAndItem will have all the rows from the feed for a week
		 * and an item. (it would have already handled if an item appears in
		 * more than once in different date range)
		 * 
		 * This function tries to group items and create promotion to a set of
		 * item. The grouping is done by LIG, then sale price, then location. It
		 * will handle if lig members are put at different sale price at same or
		 * different location if different lig members in a lig are promoted at
		 * different locations
		 * 
		 */
		List<PromoDefinition> allPromotions = new ArrayList<PromoDefinition>();
		try {
			HashMap<SimplePromoKey, List<PromoDataDTO>> groupedByLig = new HashMap<SimplePromoKey, List<PromoDataDTO>>();
			HashMap<SimplePromoKey, List<PromoDataDTO>> groupedByNonLig = new HashMap<SimplePromoKey, List<PromoDataDTO>>();

			// Group item by LIG and Non-LIG
			logger.info("Grouping by LIG or Non-Lig is started...");
			groupItemByLigOrNonLig(groupedByLig, groupedByNonLig);
			logger.info("Grouping by LIG or Non-Lig is completed...");

			// Process LIG
			logger.info("Processing of LIG is started...");
			processLig(groupedByLig, allPromotions);
			logger.info("Processing of LIG is completed...");

			// Process Non-LIG
			logger.info("Processing of NON-LIG is started...");
			processNonLig(groupedByNonLig, allPromotions);
			logger.info("Processing of NON-LIG is completed...");

			logger.info("Insertion of Promotions is started...");
			insertPromotions(allPromotions);
			logger.info("Insertion of Promotions is completed...");
		} catch (Exception e) {
			logger.error("Error while processing records");
			throw new GeneralException("Error while processing records", e);
		}
	}

	/***
	 * Group the items by LIG and NON-LIG
	 * 
	 * @param groupedByLig
	 * @param groupedByNonLig
	 * @param allItemsInAWeek
	 */
	private void groupItemByLigOrNonLig(HashMap<SimplePromoKey, List<PromoDataDTO>> groupedByLig,
			HashMap<SimplePromoKey, List<PromoDataDTO>> groupedByNonLig) {
		// HashMap<String, Integer> tempNonLigItems = new HashMap<String,
		// Integer>();
		for (Map.Entry<SimplePromoKey, List<PromoDataDTO>> entry : groupedByWeekAndItem.entrySet()) {
			SimplePromoKey actualKey = entry.getKey();
			List<PromoDataDTO> actualValues = entry.getValue();
			String itemCode = actualKey.getProductId();
			if (lirIdMap.get(itemCode) != null) {
				// Group items by lig
				RetailerLikeItemGroupDTO ligDTO = lirIdMap.get(itemCode);
				SimplePromoKey groupByLirIdKey = new SimplePromoKey(actualKey.getPromoStartDate(), actualKey.getPromoEndDate(),
						Constants.PRODUCT_LEVEL_ID_LIG, String.valueOf(ligDTO.getRetLirId()));

				List<PromoDataDTO> ligItems = null;
				if (groupedByLig.get(groupByLirIdKey) != null) {
					ligItems = groupedByLig.get(groupByLirIdKey);
				} else {
					ligItems = new ArrayList<PromoDataDTO>();
				}
				ligItems.addAll(actualValues);
				groupedByLig.put(groupByLirIdKey, ligItems);
			} else {
				// Group items by non-lig
				HashSet<Integer> distinctItemCodes = new HashSet<Integer>();
				int prestoItemCode = 0;
				for (Integer itemId : itemCodeMap.get(itemCode)) {
					distinctItemCodes.add(itemId);
					prestoItemCode = itemId;
				}
				// tempNonLigItems.put(itemCode, prestoItemCode);
				// Different vendor+item code may have same item code, in this
				// case create
				// a single promotion
				SimplePromoKey groupByItemCodeKey = null;
				if (distinctItemCodes.size() == 1) {
					groupByItemCodeKey = new SimplePromoKey(actualKey.getPromoStartDate(), actualKey.getPromoEndDate(), Constants.ITEMLEVELID,
							String.valueOf(prestoItemCode));
				} else if (distinctItemCodes.size() > 1) {
					Integer minItemCode = Collections.min(distinctItemCodes);
					// Different vendor+item code may have different item codes
					groupByItemCodeKey = new SimplePromoKey(actualKey.getPromoStartDate(), actualKey.getPromoEndDate(), Constants.ITEMLEVELID,
							String.valueOf(minItemCode));
				}

				List<PromoDataDTO> nonLigItems = null;
				if (groupedByNonLig.get(groupByItemCodeKey) != null) {
					nonLigItems = groupedByNonLig.get(groupByItemCodeKey);
				} else {
					nonLigItems = new ArrayList<PromoDataDTO>();
				}
				nonLigItems.addAll(actualValues);
				groupedByNonLig.put(groupByItemCodeKey, nonLigItems);

				// for debugging
				// is item has 2 different item code
				// if (distinctItemCodes.size() > 1) {
				// logger.debug("Non-LIG: " + itemCode + " has " +
				// distinctItemCodes.size() + " presto item codes ");
				// }
			}
		}
	}

	private void processLig(HashMap<SimplePromoKey, List<PromoDataDTO>> groupedByLig, List<PromoDefinition> allPromotions)
			throws CloneNotSupportedException, ParseException, GeneralException {
		HashSet<MultiplePrice> distinctSalePrice = null;

		// Process each lig in each week
		try {
			for (Map.Entry<SimplePromoKey, List<PromoDataDTO>> itemsOfLig : groupedByLig.entrySet()) {
				String retLirId = itemsOfLig.getKey().getProductId();

				List<PromoDataDTO> distinctItems = chooseItems(itemsOfLig.getValue());
				int noOfPromotionCreated = 0;
				HashMap<Integer, List<PromoDataDTO>> groupedByLocation = new HashMap<Integer, List<PromoDataDTO>>();
				// group by store or zone
				groupByLocationLevel(distinctItems, groupedByLocation);
				// if a item for same location appears in different date range,
				// it will pick and put it here
				// 12/27/2015|02/13/2016|007059|239276 |PA DUTCH MEDIUM NOOD|018
				// |000 |000
				// 12/24/2015|01/09/2016|007059|239276 |PA DUTCH MEDIUM NOOD|018
				// |237 |000

				if (groupedByLocation.size() > 1) {
					logger.debug("LIG: " + retLirId + " is having " + groupedByLocation.size() + " locations ");
				}

				// for (Map.Entry<Integer, List<PromoDataDTO>> entry :
				// groupedByLocation.entrySet()) {

				// Get distinct sale price in a lig
				distinctSalePrice = getDistinctSalePrice(distinctItems);

				if (distinctSalePrice.size() > 1) {
					logger.debug("LIG is promoted with " + distinctSalePrice.size() + "  different prices, " + "Promo No: "
							+ distinctItems.get(0).getPromoNo() + ",RetLirId:" + retLirId);
				}

				// Group by sale price, process each distinct sale price
				for (MultiplePrice multiplePrice : distinctSalePrice) {
					List<PromoDataDTO> groupedBySalePrice = new ArrayList<PromoDataDTO>();
					// Get all matching sale price rows
					for (PromoDataDTO promoData : distinctItems) {
						MultiplePrice salePrice = new MultiplePrice(promoData.getSaleQty(), Double.valueOf(String.valueOf(promoData.getSalePrice())));
						if (multiplePrice.equals(salePrice)) {
							groupedBySalePrice.add(promoData);
						}
					}

					/****/
					// To handle vendor+item with same item code or different
					// multiple code
					HashMap<Integer, List<PromoDataDTO>> groupedByPrestoItemCode = new HashMap<Integer, List<PromoDataDTO>>();
					for (PromoDataDTO promoData : groupedBySalePrice) {
						HashSet<Integer> distinctItemCodes = new HashSet<Integer>();
						int prestoItemCode = 0;
						String itemCode = getItemCode(promoData);
						for (Integer itemId : itemCodeMap.get(itemCode)) {
							distinctItemCodes.add(itemId);
							prestoItemCode = itemId;
						}
						// Different vendor+item code may have same item code,
						// in this case create
						// a single promotion
						if (distinctItemCodes.size() > 1) {
							// Different vendor+item code may have different
							// item codes
							prestoItemCode = Collections.min(distinctItemCodes);
						}

						List<PromoDataDTO> nonLigItems = null;
						if (groupedByPrestoItemCode.get(prestoItemCode) != null) {
							nonLigItems = groupedByPrestoItemCode.get(prestoItemCode);
						} else {
							nonLigItems = new ArrayList<PromoDataDTO>();
						}
						promoData.setPrestoItemCode(prestoItemCode);
						nonLigItems.add(promoData);
						groupedByPrestoItemCode.put(prestoItemCode, nonLigItems);

						// for debugging
						// is item has 2 different item code
						// if (distinctItemCodes.size() > 1) {
						// logger.debug("Non-LIG: " + itemCode + " has " +
						// distinctItemCodes.size() + " presto item codes ");
						// }
					}
					/****/
					// int test = 0;
					// for (Map.Entry<Integer, List<PromoDataDTO>> ent1 :
					// groupedByItemCode.entrySet()) {
					// boolean isOnlyAuthorizedStoresPresent =
					// isPromotedInAllAuthorizedStores(ent1.getValue());
					// if(isOnlyAuthorizedStoresPresent) {
					// test = test + 1;
					// }
					// }
					// logger.debug("test:" + test);

					// Find distinct set of stores
					HashMap<HashSet<String>, List<PromoDataDTO>> distinctLocationSet = new HashMap<HashSet<String>, List<PromoDataDTO>>();
					for (Map.Entry<Integer, List<PromoDataDTO>> ent : groupedByPrestoItemCode.entrySet()) {
						List<PromoDataDTO> sameItemsAcrossLoc = ent.getValue();
						HashSet<String> distinctLocations = new HashSet<String>();
						// If a item has two entries with zone and store, then
						// consider it as zone
						groupedByLocation = new HashMap<Integer, List<PromoDataDTO>>();
						// group by store or zone
						groupByLocationLevel(sameItemsAcrossLoc, groupedByLocation);

						for (PromoDataDTO promoData : sameItemsAcrossLoc) {
							if (groupedByLocation.size() > 1) {
								distinctLocations.add(getZoneNo(promoData));
							} else {
								distinctLocations.add(getStoreOrZoneNo(promoData));
							}

							// if (isZonePriced(promoData)) {
							// String zoneNo = getStoreOrZoneNo(promoData);
							// distinctLocations.addAll(getAllStoresNoInAZone(zoneNo));
							// } else {
							// distinctLocations.add(getStoreOrZoneNo(promoData));
							// }
							// distinctLocations.add(getZoneNo(promoData));
						}
						if (distinctLocationSet.get(distinctLocations) != null)
							distinctLocationSet.get(distinctLocations).addAll(sameItemsAcrossLoc);
						else
							distinctLocationSet.put(distinctLocations, sameItemsAcrossLoc);
					}
					logger.debug("distinct location set count:" + distinctLocationSet.size());

					List<PromoDataDTO> itemsWithOnlyAuthorizedStores = new ArrayList<PromoDataDTO>();
					HashMap<List<PromoLocation>, List<PromoDataDTO>> itemsWithSameLocSetButNotAuthorizedInAllStores = new HashMap<List<PromoLocation>, List<PromoDataDTO>>();
					for (Map.Entry<HashSet<String>, List<PromoDataDTO>> ent : distinctLocationSet.entrySet()) {
						// Check if all the items inside is authorized
						HashMap<Integer, List<PromoDataDTO>> groupAgainByPrestoItemCode = new HashMap<Integer, List<PromoDataDTO>>();
						for (PromoDataDTO promoData : ent.getValue()) {
							List<PromoDataDTO> tempList = null;
							int prestoItemCode = promoData.getPrestoItemCode();
							if (groupAgainByPrestoItemCode.get(prestoItemCode) != null)
								tempList = groupAgainByPrestoItemCode.get(prestoItemCode);
							else
								tempList = new ArrayList<PromoDataDTO>();

							tempList.add(promoData);
							groupAgainByPrestoItemCode.put(prestoItemCode, tempList);
						}

						boolean isPromotedOnlyInAuthorizedStores = true;
						// List<PromoDataDTO>
						// itemsWithSameLocSetButNotAuthorizedInAllStores = new
						// ArrayList<PromoDataDTO>();
						for (Map.Entry<Integer, List<PromoDataDTO>> ent1 : groupAgainByPrestoItemCode.entrySet()) {
							boolean isOnlyAuthorizedStoresPresent = isPromotedInAllAuthorizedStores(ent1.getValue());
							if (!isOnlyAuthorizedStoresPresent) {
								isPromotedOnlyInAuthorizedStores = false;
								break;
							}
							/*
							 * if (isOnlyAuthorizedStoresPresent) {
							 * itemsWithOnlyAuthorizedStores.addAll(ent1.
							 * getValue()); } else {
							 * itemsWithSameLocSetButNotAuthorizedInAllStores.
							 * addAll(ent1.getValue()); }
							 */
						}

						if (isPromotedOnlyInAuthorizedStores) {
							itemsWithOnlyAuthorizedStores.addAll(ent.getValue());
						} else {
							// Find location, this is done as no of stores may
							// vary for each item,
							// but they will have same authorized zone. e.g,
							// item 1 10 stores belong to zone1,2
							// item 2 12 stores belong to zone1,2. where item is
							// authorized in only 10 and item 2 in
							// 12 stores
							List<PromoLocation> promoLocations = setLocationAsInInput(ent.getValue());
							List<PromoDataDTO> promoDataDTO = new ArrayList<PromoDataDTO>();
							if (itemsWithSameLocSetButNotAuthorizedInAllStores.get(promoLocations) != null) {
								promoDataDTO = itemsWithSameLocSetButNotAuthorizedInAllStores.get(promoLocations);
							}
							promoDataDTO.addAll(ent.getValue());
							itemsWithSameLocSetButNotAuthorizedInAllStores.put(promoLocations, promoDataDTO);
						}

						/*
						 * if (itemsWithSameLocSetButNotAuthorizedInAllStores !=
						 * null &&
						 * itemsWithSameLocSetButNotAuthorizedInAllStores.size()
						 * > 0) { noOfPromotionCreated = noOfPromotionCreated +
						 * 1; processPromotions(itemsOfLig.getKey(),
						 * itemsWithSameLocSetButNotAuthorizedInAllStores,
						 * allPromotions, false); }
						 */
					}

					// Item promoted in all authorized stores
					if (itemsWithOnlyAuthorizedStores != null && itemsWithOnlyAuthorizedStores.size() > 0) {
						noOfPromotionCreated = noOfPromotionCreated + 1;
						processPromotions(itemsOfLig.getKey(), itemsWithOnlyAuthorizedStores, allPromotions, true);
					}

					// Items promoted only in few authorized stores
					for (List<PromoDataDTO> promoStoreSpecific : itemsWithSameLocSetButNotAuthorizedInAllStores.values()) {
						noOfPromotionCreated = noOfPromotionCreated + 1;
						processPromotions(itemsOfLig.getKey(), promoStoreSpecific, allPromotions, false);
					}
				}
				if (noOfPromotionCreated > 1 && noOfPromotionCreated > distinctSalePrice.size())
					logger.debug("No Of promotion created for Ret Lir Id : " + retLirId + "," + noOfPromotionCreated + ",PromoNo:"
							+ itemsOfLig.getValue().get(0).getPromoNo());
			}
		} catch (Exception ex) {
			ex.printStackTrace();
			// logger.error("Exception in processLig()");
			throw new GeneralException("Exception in processLig()");
		}
	}

	private List<PromoDataDTO> chooseItems(List<PromoDataDTO> items) {
		// if a item for same location appears in different date range, it will
		// pick one and put it here
		HashMap<ItemAndLocationKey, PromoDataDTO> distinctItems = new HashMap<ItemAndLocationKey, PromoDataDTO>();
		List<PromoDataDTO> finalItems = new ArrayList<PromoDataDTO>();
		for (PromoDataDTO promoData : items) {
			ItemAndLocationKey itemAndLocationKey = new ItemAndLocationKey(getItemCode(promoData), getAllLocation(promoData));
			if (distinctItems.get(itemAndLocationKey) != null) {
				// give preference to shortest date range
				// if the date range is same, then give preference to
				// latest occurrence, but in map it will be in first
				long noOfDaysOfPreInstance = distinctItems.get(itemAndLocationKey).getNoOfDaysInPromoDuration();
				long noOfDays = promoData.getNoOfDaysInPromoDuration();
				if (noOfDays <= noOfDaysOfPreInstance) {
					// replace
					distinctItems.put(itemAndLocationKey, promoData);
					finalItems.add(promoData);
				}
			} else {
				distinctItems.put(itemAndLocationKey, promoData);
				finalItems.add(promoData);
			}
		}
		return finalItems;
	}

	private void groupByLocationLevel(List<PromoDataDTO> itemsOfLig, HashMap<Integer, List<PromoDataDTO>> groupedByLocation) {
		// To handle one item is promoted at store level for one week,
		// same item is promoted at zone level for same week
		// 12/27/2015|02/13/2016|007059|239276 |PA DUTCH MEDIUM NOOD|024 |000
		// |000
		// 12/24/2015|01/09/2016|007059|239276 |PA DUTCH MEDIUM NOOD|018 |237
		// |000
		for (PromoDataDTO promoData : itemsOfLig) {
			List<PromoDataDTO> tempList = new ArrayList<PromoDataDTO>();
			if (isZonePriced(promoData)) {
				if (groupedByLocation.get(Constants.ZONE_LEVEL_ID) != null)
					tempList = groupedByLocation.get(Constants.ZONE_LEVEL_ID);
				tempList.add(promoData);
				groupedByLocation.put(Constants.ZONE_LEVEL_ID, tempList);
			} else {
				if (groupedByLocation.get(Constants.STORE_LEVEL_ID) != null)
					tempList = groupedByLocation.get(Constants.STORE_LEVEL_ID);
				tempList.add(promoData);
				groupedByLocation.put(Constants.STORE_LEVEL_ID, tempList);
			}
		}
	}

	private boolean isPromotedInAllAuthorizedStores(List<PromoDataDTO> sameItemsAcrossLoc) {
		HashSet<Integer> authorizedStore = new HashSet<Integer>();
		HashSet<Integer> distinctStores = new HashSet<Integer>();
		boolean isOnlyAuthorizedStoresPresent = true;
		// String itemCode = ent.getKey();

		// Get all stores where the item is promoted
		for (PromoDataDTO promoData : sameItemsAcrossLoc) {
			String storeOrZoneNo = getStoreOrZoneNo(promoData);
			// Find if location is store or zone
			// If zone get all stores
			boolean isZonePriced = isZonePriced(promoData);
			if (isZonePriced) {
				distinctStores.addAll(getAllStoresIdInAZone(storeOrZoneNo));
			} else {
				distinctStores.add(storeNoAndId.get(storeOrZoneNo));
			}

			// Get all authorized store of the item
			String itemCode = getItemCode(promoData);
			for (Integer itemId : itemCodeMap.get(itemCode)) {
				if (storeItemMap.get(itemId) != null) {
					for (StoreItemMapDTO sim : storeItemMap.get(itemId)) {
						authorizedStore.add(sim.getCompStrId());
					}
				}
			}
		}

		// Check if it has all the authorized stores
		for (Integer storeId : authorizedStore) {
			if (!distinctStores.contains(storeId)) {
				isOnlyAuthorizedStoresPresent = false;
				break;
			}
		}

		return isOnlyAuthorizedStoresPresent;
	}

	private List<Integer> getAllStoresIdInAZone(String zoneNo) {
		HashSet<Integer> distinctStores = new HashSet<Integer>();
		List<Integer> storeIds = new ArrayList<Integer>();
		for (StoreDTO store : storeDetails) {
			if (store.zoneNum.equals(zoneNo)) {
				if (!distinctStores.contains(store.strId)) {
					distinctStores.add(store.strId);
					storeIds.add(store.strId);
				}
			}
		}
		return storeIds;
	}

	// private List<String> getAllStoresNoInAZone(String zoneNo) {
	// HashSet<String> distinctStores = new HashSet<String>();
	// List<String> storeNos = new ArrayList<String>();
	// for (StoreDTO store : storeDetails) {
	// if (store.zoneNum.equals(zoneNo)) {
	// if (!distinctStores.contains(store.strNum)) {
	// distinctStores.add(store.strNum);
	// storeNos.add(store.strNum);
	// }
	// }
	// }
	// return storeNos;
	//
	// // for (StoreDTO store : storeDetails) {
	// // if (store.zoneNum.equals(storeOrZoneNo))
	// // distinctStores.add(store.strId);
	// // }
	// }

	private List<Integer> getAllAuthorizedStoresIdInAZone(List<PromoDataDTO> promoItems) {
		HashSet<Integer> distinctStores = new HashSet<Integer>();
		List<Integer> storeIds = new ArrayList<Integer>();
		for (PromoDataDTO promoData : promoItems) {
			String itemCode = getItemCode(promoData);
			// All presto item codes
			for (Integer itemId : itemCodeMap.get(itemCode)) {
				if (storeItemMap.get(itemId) != null) {
					// All the stores
					for (StoreItemMapDTO sim : storeItemMap.get(itemId)) {
						// Get zone no of the store
						Integer storeId = sim.getCompStrId();
						if (!distinctStores.contains(storeId)) {
							distinctStores.add(storeId);
							storeIds.add(storeId);
						}
					}
				}
			}
		}
		return storeIds;
	}

	// private String getZoneNoFromStoreId(int strId){
	// String zoneNo = "";
	// for(StoreDTO store : storeDetails) {
	// if(store.strId == strId && store.zoneNum != "") {
	// zoneNo = store.zoneNum;
	// break;
	// }
	// }
	// return zoneNo;
	// }
	//
	// private String getStoreNoFromStoreId(int strId){
	// String storeNo = "";
	// for(StoreDTO store : storeDetails) {
	// if(store.strId == strId) {
	// storeNo = store.strNum;
	// break;
	// }
	// }
	// return storeNo;
	// }
	/***
	 * group the non-lig items by sale price
	 * 
	 * @param groupedByNonLig
	 * @param allPromotions
	 * @throws GeneralException
	 * @throws ParseException
	 * @throws CloneNotSupportedException
	 */
	private void processNonLig(HashMap<SimplePromoKey, List<PromoDataDTO>> groupedByNonLig, List<PromoDefinition> allPromotions)
			throws CloneNotSupportedException, ParseException, GeneralException {
		HashSet<MultiplePrice> distinctSalePrice = null;
		// Loop each non-lig item
		for (Map.Entry<SimplePromoKey, List<PromoDataDTO>> itemsOfNonLig : groupedByNonLig.entrySet()) {
			List<PromoDataDTO> distinctItems = chooseItems(itemsOfNonLig.getValue());
			HashMap<Integer, List<PromoDataDTO>> groupedByLocation = new HashMap<Integer, List<PromoDataDTO>>();
			groupByLocationLevel(distinctItems, groupedByLocation);
			// if a item for same location appears in different date range, it
			// will pick and put it here
			// 12/27/2015|02/13/2016|007059|239276 |PA DUTCH MEDIUM NOOD|018
			// |000 |000
			// 12/24/2015|01/09/2016|007059|239276 |PA DUTCH MEDIUM NOOD|018
			// |237 |000

			if (groupedByLocation.size() > 1) {
				logger.debug("Non-LIG : " + getItemCode(distinctItems.get(0)) + " is promoted in " + groupedByLocation.size() + " locations");
			}

			// for (Map.Entry<Integer, List<PromoDataDTO>> entry :
			// groupedByLocation.entrySet()) {
			distinctSalePrice = getDistinctSalePrice(distinctItems);

			if (distinctSalePrice.size() > 1) {
				logger.debug(
						"Non-LIG : " + getItemCode(distinctItems.get(0)) + " is promoted with " + distinctSalePrice.size() + " different prices");
			}
			// Group by sale price
			for (MultiplePrice multiplePrice : distinctSalePrice) {
				List<PromoDataDTO> promosBySalePrice = new ArrayList<PromoDataDTO>();
				// Get all matching sale price rows
				for (PromoDataDTO promoData : distinctItems) {
					MultiplePrice salePrice = new MultiplePrice(promoData.getSaleQty(), Double.valueOf(String.valueOf(promoData.getSalePrice())));
					if (multiplePrice.equals(salePrice)) {
						promosBySalePrice.add(promoData);
					}
				}
				//Bug Fix: 1st Jul 2016, when same item is promoted with different sale price in different location
				//then the location of the promotion must be found based on sale price group
				//boolean isOnlyAuthorizedStoresPresent = isPromotedInAllAuthorizedStores(distinctItems);
				boolean isOnlyAuthorizedStoresPresent = isPromotedInAllAuthorizedStores(promosBySalePrice);
				if (isOnlyAuthorizedStoresPresent) {
					logger.debug("Non-LIG : " + getItemCode(distinctItems.get(0)) + " is promoted in all stores");
				}
				processPromotions(itemsOfNonLig.getKey(), promosBySalePrice, allPromotions, isOnlyAuthorizedStoresPresent);
			}
		}
		// }
	}

	private HashSet<MultiplePrice> getDistinctSalePrice(List<PromoDataDTO> promoDatas) {
		HashSet<MultiplePrice> distinctSalePrice = new HashSet<MultiplePrice>();
		for (PromoDataDTO promoData : promoDatas) {
			MultiplePrice salePrice = new MultiplePrice(promoData.getSaleQty(), Double.valueOf(String.valueOf(promoData.getSalePrice())));
			distinctSalePrice.add(salePrice);
		}
		return distinctSalePrice;
	}

	private String getStoreOrZoneNo(PromoDataDTO promoData) {
		String zoneOrStoreNo = "";
		if (promoData.getStoreNo().equals("000") && promoData.getStoreZoneData().equals("000")) {
			zoneOrStoreNo = PrestoUtil.castZoneNumber(promoData.getPriceZone());
		} else {
			if (!promoData.getStoreNo().equals("000")) {
				zoneOrStoreNo = PrestoUtil.castStoreNumber(promoData.getStoreNo());
			} else {
				zoneOrStoreNo = PrestoUtil.castStoreNumber(promoData.getPriceZone());
			}
		}
		return zoneOrStoreNo;
	}

	private String getZoneNo(PromoDataDTO promoData) {
		String zoneNo = "";
		if (!promoData.getStoreZoneData().equals("000")) {
			zoneNo = PrestoUtil.castZoneNumber(promoData.getStoreZoneData());
		} else {
			zoneNo = PrestoUtil.castStoreNumber(promoData.getPriceZone());
		}
		return zoneNo;
	}

	private boolean isZonePriced(PromoDataDTO promoData) {
		boolean isZonePriced = false;
		if (promoData.getStoreNo().equals("000")) {
			if (promoData.getStoreZoneData().equals("000")) {
				isZonePriced = true;
			}
		}
		return isZonePriced;
	}

	private void processPromotions(SimplePromoKey simplePromoKey, List<PromoDataDTO> promoItems, List<PromoDefinition> allPromotions,
			boolean isPromotedOnlyInAuthorizedStores) throws GeneralException, CloneNotSupportedException, ParseException {
		PromoDefinition promoDefinition = new PromoDefinition();
		PromoDataDTO representingItemOfPromo = promoItems.get(0);
		promoDefinition.setPromoNumber(representingItemOfPromo.getPromoNo());
		if (simplePromoKey.getProductLevelId() == Constants.PRODUCT_LEVEL_ID_LIG) {
			RetailerLikeItemGroupDTO ligDTO = lirIdMap.get(representingItemOfPromo.getSourceVendorNo() + representingItemOfPromo.getItemNo());
			promoDefinition.setPromoName(ligDTO.getRetLirName());
		} else {
			promoDefinition.setPromoName(representingItemOfPromo.getItemDesc());
			}
		
		Date promoEndDate = appDateFormatter.parse(simplePromoKey.getPromoEndDate());
		Date promoStartDate = appDateFormatter.parse(simplePromoKey.getPromoStartDate());

		if (calendarMap.get(simplePromoKey.getPromoStartDate()) == null) {
			int startCalId = ((RetailCalendarDTO) calDAO.getCalendarId(conn, simplePromoKey.getPromoStartDate(), Constants.CALENDAR_DAY))
					.getCalendarId();
			calendarMap.put(simplePromoKey.getPromoStartDate(), startCalId);
		}
		if (calendarMap.get(simplePromoKey.getPromoEndDate()) == null) {
			int endCalId = ((RetailCalendarDTO) calDAO.getCalendarId(conn, simplePromoKey.getPromoEndDate(), Constants.CALENDAR_DAY)).getCalendarId();
			calendarMap.put(simplePromoKey.getPromoEndDate(), endCalId);
		}
		int weekCalendarId = -1;
		if (weekCalendarMap.get(simplePromoKey.getPromoStartDate()) == null) {
			int startCalId = ((RetailCalendarDTO) calDAO.getCalendarId(conn, simplePromoKey.getPromoStartDate(), Constants.CALENDAR_WEEK))
					.getCalendarId();
			weekCalendarMap.put(simplePromoKey.getPromoStartDate(), startCalId);
			weekCalendarId = startCalId;
		} else
			weekCalendarId = weekCalendarMap.get(simplePromoKey.getPromoStartDate());
		promoDefinition.setWeekStartDate(simplePromoKey.getPromoStartDate());
		promoDefinition.setStartCalId(calendarMap.get(simplePromoKey.getPromoStartDate()));
		promoDefinition.setEndCalId(calendarMap.get(simplePromoKey.getPromoEndDate()));
		promoDefinition.setPromoStartDate(promoStartDate);
		promoDefinition.setPromoEndDate(promoEndDate);
		promoDefinition.setCreatedBy(Constants.BATCH_USER);
		promoDefinition.setApprovedBy(Constants.BATCH_USER);

		if (simplePromoKey.getProductLevelId() == Constants.PRODUCT_LEVEL_ID_LIG) {
			promoDefinition.setRetLirId(Integer.valueOf(simplePromoKey.getProductId()));
			promoDefinition.setRetailerItemCode(null);
		} else {
			promoDefinition.setRetLirId(-1);
			promoDefinition.setRetailerItemCode(simplePromoKey.getProductId());
		}

		// Set buy requirement and offer detail
		setBuyReqAndOfferDetail(promoDefinition, representingItemOfPromo);

		// Set buy items
		setBuyItems(simplePromoKey, promoItems, weekCalendarId, promoDefinition);

		List<PromoLocation> promoLocations = null;
		// Set locations
		// boolean isZonePriced = isZonePriced(representingItemOfPromo);
		if (isPromotedOnlyInAuthorizedStores) {
			// logger.debug("Promoted in all stores, Item Codes: " +
			// getAllItems(promoItems) + ",PromoNo:" +
			// representingItemOfPromo.getPromoNo());
			promoLocations = new ArrayList<PromoLocation>();
			addLocation(promoLocations, Constants.CHAIN_LEVEL_ID, chainId);
			promoDefinition.setPromoLocation(promoLocations);
			// setLocationOfPromotion(promoItems, promoDefinition,
			// isZonePriced);
		} else {
			promoLocations = setLocationAsInInput(promoItems);
			promoDefinition.setPromoLocation(promoLocations);
		}

		allPromotions.add(promoDefinition);
	}


	private List<PromoLocation> setLocationAsInInput(List<PromoDataDTO> promoItems) {
		List<PromoLocation> promoLocations = new ArrayList<PromoLocation>();
		// one row has zone, other row has store
		// 09/29/2015|02/13/2016|007059|372116 |GERTEX MENS TEXTIN|015 |000 |000
		// | 1| 1.99| 0| 1.49|
		// 09/29/2015|02/13/2016|007059|372116 |GERTEX MENS TEXTIN|014 |601 |000
		// | 1| 1.99| 0| 1.49|

		// 09/29/2015|02/13/2016|007059|372116 |GERTEX MENS TEXTIN|014 |000 |000
		// | 1| 1.99| 0| 1.49|
		// 09/29/2015|02/13/2016|007059|372116 |GERTEX MENS TEXTIN|014 |601 |000
		// | 1| 1.99| 0| 1.49|

		HashSet<String> distinctZones = new HashSet<String>();
		HashSet<String> distinctStores = new HashSet<String>();
		for (PromoDataDTO promoDataDTO : promoItems) {
			if (isZonePriced(promoDataDTO)) {
				distinctZones.add(getStoreOrZoneNo(promoDataDTO));
			} else {
				distinctStores.add(getStoreOrZoneNo(promoDataDTO));
			}
		}

		List<Integer> authorizedStores = getAllAuthorizedStoresIdInAZone(promoItems);

		// logger.debug("distinctStores:" + distinctStores.toString());

		// Group to zone level
		for (Map.Entry<String, Integer> zoneNo : zoneNoAndId.entrySet()) {
			List<String> storesInZone = new ArrayList<String>();
			// get all authorized stores in the zone
			// storesInZone.addAll(getAllStoresNoInAZone(zoneNo.getKey()));
			for (Integer storeId : authorizedStores) {
				StoreDTO storeDto = storeIdAndStore.get(storeId);
				if (storeDto != null) {
					if (storeDto.zoneNum.equals(zoneNo.getKey()))
						storesInZone.add(storeDto.strNum);
				}
			}
			// If all authorized store of a zone is present
			if (storesInZone.size() > 0 && distinctStores.containsAll(storesInZone)) {
				// logger.debug("passed - storesInZone:" +
				// storesInZone.toString());
				distinctZones.add(zoneNo.getKey());
			} else {
				// logger.debug("failed - storesInZone:" +
				// storesInZone.toString());
			}
		}

		for (String storeNo : distinctStores) {
			String zoneNum = "";
			// get zone no of the store
			for (StoreDTO store : storeDetails) {
				if (store.strNum.equals(storeNo) && store.zoneNum != "") {
					zoneNum = store.zoneNum;
				}
			}
			// If a zone is already present, then don't add those stores
			if (!distinctZones.contains(zoneNum))
				addLocation(promoLocations, Constants.STORE_LEVEL_ID, storeNoAndId.get(storeNo));
		}

		// Add locations to promotion
		for (String zoneNo : distinctZones) {
			if (zoneNoAndId.get(zoneNo) != null) {
				addLocation(promoLocations, Constants.ZONE_LEVEL_ID, zoneNoAndId.get(zoneNo));
			}

			if (rollupDSDToWhseEnabled) {
				PromoDataDTO promoData = promoItems.get(0);
				String itemCode = getItemCode(promoData);
				if(itemCodeMap.containsKey(itemCode)){
					int itemId = itemCodeMap.get(itemCode).get(0);
					if(itemCodeCategoryMap.containsKey(itemId)){
						int productId = itemCodeCategoryMap.get(itemId);
						if (dsdAndWhseZoneMap.containsKey(productId)) {
							HashMap<String, String> zoneMap = dsdAndWhseZoneMap.get(productId);
							if (zoneMap.containsKey(zoneNo)) {
								String whseZone = zoneMap.get(zoneNo);
								if (zoneNoAndId.get(whseZone) != null) {
									addLocation(promoLocations, Constants.ZONE_LEVEL_ID, zoneNoAndId.get(whseZone));
								}
							}
						}	
					}
				}
			}
		}
		return promoLocations;
	}

	private void addLocation(List<PromoLocation> promoLocations, int locationLevelId, int locationId) {
		PromoLocation location = new PromoLocation();
		location.setLocationLevelId(locationLevelId);
		location.setLocationId(locationId);
		promoLocations.add(location);
	}

	private void setBuyReqAndOfferDetail(PromoDefinition promoDefinition, PromoDataDTO samplePromo) {
		
		// Dinesh:: 16-Feb-18, added new condition to process bogo type, since in GE there is a issue in input feed, so 
		// Standard Promo or normal items were loaded as BOGO. TO avoid that new conditions to were introduced.
		// To avoid BOGO loading in GE
		boolean processBogo = Boolean.parseBoolean(PropertyManager.getProperty("PROCESS_BOGO_IN_PROMO_LOADER", "TRUE"));
		
		// Add Promo Buy Requirement and Offer detail for BOGO promotions
		if (Math.abs(samplePromo.getRegPrice() - samplePromo.getSalePrice()) < 0.01f && samplePromo.getSaleQty() == 1
				&& samplePromo.getRegPrice() > 0 && processBogo) {
			promoDefinition.setPromoTypeId(Constants.PROMO_TYPE_BOGO);

			PromoBuyRequirement buyReq = new PromoBuyRequirement();
			buyReq.setBuyAndGetIsSame(String.valueOf(Constants.YES));
			buyReq.setBuyX(1);

			PromoOfferDetail offerDetail = new PromoOfferDetail();
			offerDetail.setPromoOfferTypeId(Constants.OFFER_TYPE_FREE_ITEM);
			offerDetail.setOfferUnitType(Constants.OFFER_UNIT_TYPE_NUMBER);
			offerDetail.setOfferUnitCount(1);
			buyReq.addOfferDetail(offerDetail);
			promoDefinition.addPromoBuyRequirement(buyReq);
		} else {
			promoDefinition.setPromoTypeId(Constants.PROMO_TYPE_STANDARD);
		}
	}

	private void setBuyItems(SimplePromoKey simplePromoKey, List<PromoDataDTO> promoItems, int weekCalendarId, PromoDefinition promoDefinition)
			throws ParseException, GeneralException {
		HashMap<Integer, List<PromoDataDTO>> curWorkMap = new HashMap<Integer, List<PromoDataDTO>>();
		List<PromoDataDTO> tList = null;

		// Keep item code and its promo data
		for (PromoDataDTO promoData : promoItems) {
			String tempRetailerItemCode = getItemCode(promoData);
			if (itemCodeMap.get(tempRetailerItemCode) != null) {
				for (Integer itemCode : itemCodeMap.get(tempRetailerItemCode)) {
					if (curWorkMap.get(itemCode) != null) {
						tList = curWorkMap.get(itemCode);
					} else {
						tList = new ArrayList<PromoDataDTO>();
					}
					tList.add(promoData);
					curWorkMap.put(itemCode, tList);
				}
			} else {
				noItemCodeList.add(tempRetailerItemCode);
			}
		}

		// Add items under promotions
		for (Map.Entry<Integer, List<PromoDataDTO>> inEntry : curWorkMap.entrySet()) {
			Integer itemCode = inEntry.getKey();
			PromoBuyItems buyItem = new PromoBuyItems();
			PromoDataDTO repBuyItem = inEntry.getValue().get(0);
			buyItem.setItemCode(itemCode);

			// Changed on 22nd Sep 2015, after discussion to keep quantity as 2
			// as price is saved as 2 in retail price info table
			if (promoDefinition.getPromoTypeId() == Constants.PROMO_TYPE_BOGO) {
				repBuyItem.setSaleQty(2);
			}

			// Set REG_M_PRICE when REG_QTY is greater than 1.
			if (repBuyItem.getRegQty() > 1) {
				buyItem.setRegQty(repBuyItem.getRegQty());
				buyItem.setRegMPrice(repBuyItem.getRegPrice());
			} else {
				int regQty = repBuyItem.getRegQty() == 0 ? 1 : repBuyItem.getRegQty();
				buyItem.setRegQty(regQty);
				buyItem.setRegPrice(repBuyItem.getRegPrice());
			}

			// Set SALE_M_PRICE when SALE_QTY is greater than 1.
			if (repBuyItem.getSaleQty() > 1) {
				buyItem.setSaleQty(repBuyItem.getSaleQty());
				buyItem.setSaleMPrice(repBuyItem.getSalePrice());
			} else {
				int saleQty = repBuyItem.getSaleQty() == 0 ? 1 : repBuyItem.getSaleQty();
				buyItem.setSaleQty(saleQty);
				buyItem.setSalePrice(repBuyItem.getSalePrice());
			}

			// logger.debug("Week Calendar Id - " + weekCalendarId);
			if (weekCalendarId > 0) {
				RetailCostDTO retailCostDTO = rcDAO.getChainLevelCost(conn, weekCalendarId, itemCode);
				if (retailCostDTO != null) {
					// logger.debug("List COst - " +
					// retailCostDTO.getListCost());
					buyItem.setListCost(retailCostDTO.getListCost());
					buyItem.setDealCost(retailCostDTO.getDealCost());
					buyItem.setOffInvoiceCost(retailCostDTO.getLevel2Cost());
				}
			}

			// set if item is in ad
			if (isPromoInWeeklyAd(repBuyItem, simplePromoKey.getPromoStartDate(), simplePromoKey.getPromoEndDate()))
				buyItem.setIsInAd(String.valueOf(Constants.YES));
			else
				buyItem.setIsInAd(String.valueOf(Constants.NO));

			// Check if promotion falls or end in mid of week
			Date promoStartDate = appDateFormatter.parse(simplePromoKey.getPromoStartDate());
			Date promoEndDate = appDateFormatter.parse(simplePromoKey.getPromoEndDate());
			//Date actualPromoStartDate = appDateFormatter.parse(simplePromoKey.getPromoStartDate());
			//Date actualPromoEndDate = appDateFormatter.parse(simplePromoKey.getPromoEndDate());
			//Bug Fix: 11th Jul 2016, actual date is incorrectly set for buy items
			Date actualPromoStartDate = appDateFormatter.parse(repBuyItem.getPromoStartDate());
			Date actualPromoEndDate = appDateFormatter.parse(repBuyItem.getPromoEndDate());
			
			//NU Bug Fix:8th Sep 2016, actual promo date is still incorrectly set
			String startDate = appDateFormatter.format(actualPromoStartDate);
			buyItem.setActualStartCalId(getCalendarId(startDate));
			
			String endDate = appDateFormatter.format(actualPromoEndDate);
			buyItem.setActualEndCalId(getCalendarId(endDate));

			// set actual start and end date
			if (((promoStartDate.equals(actualPromoStartDate) || promoStartDate.after(actualPromoStartDate))
					&& ((promoEndDate.equals(actualPromoEndDate) || promoEndDate.before(actualPromoEndDate))))) {
				buyItem.setPromoInMidOfWeek(false);
				//NU Bug Fix:8th Sep 2016, actual promo date is still incorrectly set
				//buyItem.setActualStartCalId(promoDefinition.getStartCalId());
				//buyItem.setActualEndCalId(promoDefinition.getEndCalId());
			} else {
				// if actual duration spans weeks
				buyItem.setPromoInMidOfWeek(true);
				//Bug Fix: 11th Jul 2016, actual date is incorrectly set for buy items
//				if (promoStartDate.equals(actualPromoStartDate)) {
//					buyItem.setActualStartCalId(promoDefinition.getStartCalId());
//				} else if (promoStartDate.before(actualPromoStartDate)) {
//					String startDate = appDateFormatter.format(actualPromoStartDate);
//					buyItem.setActualStartCalId(getCalendarId(startDate));
//				}
//				if (promoEndDate.equals(actualPromoEndDate)) {
//					buyItem.setActualEndCalId(promoDefinition.getEndCalId());
//				} else if (promoEndDate.after(actualPromoEndDate)) {
//					String endDate = appDateFormatter.format(actualPromoEndDate);
//					buyItem.setActualEndCalId(getCalendarId(endDate));
//				}
				//NU Bug Fix:8th Sep 2016, actual promo date is still incorrectly set
				/*if (promoStartDate.equals(actualPromoStartDate)) {
					buyItem.setActualStartCalId(promoDefinition.getStartCalId());
				} else {
					String startDate = appDateFormatter.format(actualPromoStartDate);
					buyItem.setActualStartCalId(getCalendarId(startDate));
				}
				if (promoEndDate.equals(actualPromoEndDate)) {
					buyItem.setActualEndCalId(promoDefinition.getEndCalId());
				} else {
					String endDate = appDateFormatter.format(actualPromoEndDate);
					buyItem.setActualEndCalId(getCalendarId(endDate));
				}*/
			}

			promoDefinition.addBuyItems(buyItem);
		}
	}

	private int getCalendarId(String inputDate) throws GeneralException {
		if (calendarMap.get(inputDate) == null) {
			int startCalId = ((RetailCalendarDTO) calDAO.getCalendarId(conn, inputDate, Constants.CALENDAR_DAY)).getCalendarId();
			calendarMap.put(inputDate, startCalId);
		}
		return calendarMap.get(inputDate);
	}

	private boolean isValidPromoNo(String promoNo) {
		boolean retVal = true;
		try {
			int intPromoNo = Integer.parseInt(promoNo);
			if (intPromoNo < 10000) {
				retVal = false;
			}
		} catch (Exception e) {

		}
		return retVal;
	}

	private ArrayList<String> getFileList(String subFolder, String listFile) throws GeneralException {
		ArrayList<String> fileList = new ArrayList<String>();

		String filePath = getRootPath() + "/" + subFolder + "/" + listFile;
		File file = new File(filePath);
		if (!file.exists()) {
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
				if (!line.trim().equals("")) {
					String strFile = getRootPath() + "/" + subFolder + "/" + line;
					File file1 = new File(strFile);
					if (file1.exists()) {
						fileList.add(strFile);
					} else {
						allFilesExists = false;
						logger.error("getFileList - File doesn't exist : " + strFile);
					}
				}
			}

			if (!allFilesExists) {
				throw new GeneralException("Some files are missing.");
			}
		} catch (Exception e) {
			throw new GeneralException("Error in getFileList()", e);
		} finally {
			try {
				if (br != null)
					br.close();
			} catch (IOException e) {
				e.printStackTrace();
			}
		}
		return fileList;
	}

	private void insertPromotions(List<PromoDefinition> allPromotions) throws GeneralException {
		SimpleDateFormat format = new SimpleDateFormat(Constants.APP_DATE_FORMAT);
		PromotionDAO promoDAO = new PromotionDAO(conn);
		logger.debug("No of Promotions to be Created/Updated: " + allPromotions.size() + " is Started");
		// Assign processing week date from file name. If filename doesn't
		// contain date, then use current week date.
		Date processingWeek = null;
		if (processingDate != null)
			try {
				processingWeek = format.parse(processingDate);
			} catch (ParseException e) {
				logger.error("Error while parsing week end date - ");
			}
		else
			processingWeek = new Date();

		logger.info("Saving of promotion( " + allPromotions.size() + " )" + " is Started");
		promoDAO.savePromotionV2(allPromotions, processingWeek, false);
		logger.info("Saving of promotion( " + allPromotions.size() + " )" + " is Completed");
		logger.info("No of Promotions to be Created/Updated: " + allPromotions.size() + " is Completed");
	}

	private boolean isPromoInWeeklyAd(PromoDataDTO promoDataDTO, String procWeekStartDate, String proceWeekEndDate) throws ParseException {
		boolean isPromoInAd = false;
		HashSet<String> advDates = new HashSet<String>();

		advDates.add(promoDataDTO.getAdDate1());
		advDates.add(promoDataDTO.getAdDate2());
		advDates.add(promoDataDTO.getAdDate3());
		advDates.add(promoDataDTO.getAdDate4());

		// If either start and end date of processing week is present
		// in any one of the four advanced date

		if (advDates.contains(formatterMMddyy.format(formatterMMddyy.parse(procWeekStartDate)))
				|| advDates.contains(formatterMMddyy.format(formatterMMddyy.parse(proceWeekEndDate))))
			isPromoInAd = true;

		return isPromoInAd;
	}

	private String getItemCode(PromoDataDTO promoData) {
		return promoData.getSourceVendorNo() + promoData.getItemNo();
	}

	private String getAllLocation(PromoDataDTO promoData) {
		return promoData.getPriceZone() + promoData.getStoreNo() + promoData.getStoreZoneData();
	}

	@SuppressWarnings("unused")
	private String getAllItems(List<PromoDataDTO> promoItems) {
		String allItems = "";
		for (PromoDataDTO promoData : promoItems) {
			allItems = allItems + "," + getItemCode(promoData);
		}
		return allItems;
	}
}
