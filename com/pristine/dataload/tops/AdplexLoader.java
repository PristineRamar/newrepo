package com.pristine.dataload.tops;

import java.io.File;
//import java.io.IOException;
import java.sql.Connection;
import java.text.DateFormat;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Date;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.TreeMap;

import org.apache.commons.io.FileUtils;
import org.apache.commons.io.FilenameUtils;
import org.apache.log4j.Logger;
import org.apache.log4j.PropertyConfigurator;
import com.pristine.dao.DBManager;
import com.pristine.dao.ItemDAO;
import com.pristine.dao.RetailCalendarDAO;
import com.pristine.dao.RetailPriceDAO;
import com.pristine.dao.offermgmt.promotion.PromotionDAO;
import com.pristine.dao.offermgmt.weeklyad.WeeklyAdDAO;
import com.pristine.dto.AdKey;
import com.pristine.dto.ItemDTO;
import com.pristine.dto.RetailCalendarDTO;
import com.pristine.dto.offermgmt.promotion.PromoBuyItems;
import com.pristine.dto.offermgmt.promotion.PromoDefinition;
import com.pristine.dto.offermgmt.weeklyad.RawWeeklyAd;
import com.pristine.dto.offermgmt.weeklyad.WeeklyAd;
import com.pristine.dto.offermgmt.weeklyad.WeeklyAdBlock;
import com.pristine.dto.offermgmt.weeklyad.WeeklyAdPage;
import com.pristine.exception.GeneralException;
import com.pristine.parsinginterface.ExcelFileParser;
import com.pristine.util.Constants;
import com.pristine.util.PrestoUtil;
import com.pristine.util.PristineDBUtil;
import com.pristine.util.PropertyManager;


/**
 * @author Pradeepkumar
 */

public class AdplexLoader {
	private static Logger logger = Logger.getLogger("AdplexLoader");
	private Connection _Conn = null;
	private static String ADPLEX_FILE_PATH = "ADPLEX_FILE_PATH=";
	ItemDAO itemDAO = null;
	int chainId;
	//private HashMap<String, List<String>> itemCodeMap = null;
	private HashMap<String, List<ItemDTO>> retItemCodeMap = null;
	//private HashMap<Integer, List<String>> retLirMap = null;
	private String weeklyAdSheetName = "Ad Financials";
	private String adName = null;
	private String weekStartDate = null;
	private String weekEndDate = null;
	private int calendarId = -1;
	private int startCalendarId = -1;
	private int endCalendarId = -1;
	private int locationId = -1;
	private String userName = "BATCH";
	private PromotionDAO promoDAO = null;
	private WeeklyAdDAO adDAO = null;
	private boolean isTesting = Boolean.parseBoolean(PropertyManager.getProperty("IS_DEBUG_MODE", "FALSE"));
	private int topsWithoutGUStoreListId = Integer.parseInt(PropertyManager.getProperty("TOPS_WITHOUT_GU_STORE_LIST_ID"));
	private int guStoreListId = Integer.parseInt(PropertyManager.getProperty("TOPS_GU_STORE_LIST_ID"));
	private int geStoreListId = Integer.parseInt(PropertyManager.getProperty("GE_STORE_LIST_ID"));
	private int miStoreListId = Integer.parseInt(PropertyManager.getProperty("MI_STORE_LIST_ID"));
	public AdplexLoader() {
		try {
			_Conn = DBManager.getConnection();
			RetailPriceDAO rpDAO = new RetailPriceDAO();
			chainId = Integer.parseInt(rpDAO.getChainId(_Conn));
		} catch (GeneralException gex) {
			logger.error("Error when creating connection - " + gex);
		}
	}

	public static void main(String[] args) {
		PropertyConfigurator.configure("log4j-adplex-loader.properties");
		PropertyManager.initialize("analysis.properties");
		String relativePath = null;
		AdplexLoader adplexLoader = new AdplexLoader();
		for (String arg : args) {
			if (arg.startsWith(ADPLEX_FILE_PATH)) {
				relativePath = arg.substring(ADPLEX_FILE_PATH.length());
			}
		}
		logger.info("***********************************************");
		adplexLoader.processAdplexFile(relativePath);
		logger.info("***********************************************");
	}

	/**
	 * processes ad plex file and creates weekly ad.
	 * @param relativePath
	 */
	private void processAdplexFile(String relativePath) {
		TreeMap<Integer, String> fieldNames = new TreeMap<>();
		ExcelFileParser<RawWeeklyAd> parser = null;
		promoDAO = new PromotionDAO(_Conn);
		adDAO = new WeeklyAdDAO(_Conn);
		itemDAO = new ItemDAO();
		
		List<RawWeeklyAd> rawAdplexTopsStores = null;
		List<RawWeeklyAd> rawAdplexGUStores = null;
		List<RawWeeklyAd> rawWeeklyAd = null;
		//NU:: 10th Nov, for gaint eagle weekly ad creation
		List<RawWeeklyAd> rawAdplexGEStores = null;
		List<RawWeeklyAd> rawAdplexMIStores = null;
		String rootPath = PropertyManager.getProperty("DATALOAD.ROOTPATH", "");
		String inputFilePath = rootPath + "/" + relativePath;
		String tempFolderPath = rootPath + "/" + relativePath + "/temp";

		try {
			if (isTesting) {
				retItemCodeMap = new HashMap<>();
			} else {
				logger.info("Caching retailer item code map...");
				retItemCodeMap = itemDAO.getItemcodeMap(_Conn);
				logger.info("Caching retailer item code map is completed.");
			}
			//create temp folder if not exists
			PrestoUtil.createDirIfNotExists(tempFolderPath);
			
			// Get all the zip files from the input folder
			ArrayList<String> zipFileList = PrestoUtil.getAllFilesInADirectory(inputFilePath, "zip");

			// Loop the zip files
			for (String zipFile : zipFileList) {
				try {
					logger.info("Processing of file: " +  zipFile + " is Started..." );
					File tempFile =new File(zipFile);
					String unZipFileFolder = tempFolderPath + "/" + FilenameUtils.getBaseName(tempFile.getName());
					// unzip the file in a temp folder
					PrestoUtil.unzipIncludingSubDirectories(zipFile, tempFolderPath);

					parser = new ExcelFileParser<RawWeeklyAd>();
					ArrayList<String> fileList = parser
							.getAllExcelVariantFiles(relativePath + "/temp/" + FilenameUtils.getBaseName(tempFile.getName()));

					for (String fileName : fileList) {
						// look for both ad plex GU, Tops and weekly ad file
						fieldNames = new TreeMap<>();
						parser = new ExcelFileParser<RawWeeklyAd>();
						if (fileName.toLowerCase().contains("tops adjusted")) {
							setCalendarId(fileName);
							mapFieldNames(fieldNames);
							parser.setFirstRowToProcess(3);
							rawWeeklyAd = parser.parseExcelFileV2(RawWeeklyAd.class, fileName, weeklyAdSheetName, fieldNames);
						} else if (fileName.toLowerCase().contains("ny adplex page")) {
							mapFieldNamesForAdPlex(fieldNames);
							parser.setFirstRowToProcess(1);
							rawAdplexTopsStores = parser.parseExcelFileV2(RawWeeklyAd.class, fileName, "Sheet1", fieldNames);
						} else if (fileName.toLowerCase().contains("adr adplex page")) {
							mapFieldNamesForAdPlex(fieldNames);
							parser.setFirstRowToProcess(1);
							rawAdplexGUStores = parser.parseExcelFileV2(RawWeeklyAd.class, fileName, "Sheet1", fieldNames);
						} else if (fileName.toLowerCase().contains("ge adfeed") || fileName.toLowerCase().contains("mi adfeed")) {
							mapFieldNamesForAdPlex(fieldNames);
							parser.setFirstRowToProcess(1);
							setCalendarId(fileName);
							if (fileName.toLowerCase().contains("ge adfeed")) {
								rawAdplexGEStores = parser.parseExcelFileV2(RawWeeklyAd.class, fileName, "Sheet1", fieldNames);
							} else if (fileName.toLowerCase().contains("mi adfeed")) {
								rawAdplexMIStores = parser.parseExcelFileV2(RawWeeklyAd.class, fileName, "Sheet1", fieldNames);
							}
						}
					}
					
					//Financial file and ad plex TOPS or GU is must
					if ((rawAdplexTopsStores != null || rawAdplexGUStores != null || rawAdplexGEStores != null || rawAdplexMIStores != null)) {
						// Process Ad plex of TOPS store 
						if(rawAdplexTopsStores != null)
							loadAdData(rawWeeklyAd, rawAdplexTopsStores, topsWithoutGUStoreListId);
						
						//Process Ad plex of GU stores
						if(rawAdplexGUStores != null)
							loadAdData(rawWeeklyAd, rawAdplexGUStores, guStoreListId);
						
						//Process Giant Eagle feed
						if(rawAdplexGEStores != null)
							loadAdData(rawWeeklyAd, rawAdplexGEStores, geStoreListId);
						
						//Process Giant Eagle (MI Banner) feed
						if(rawAdplexMIStores != null)
							loadAdData(rawWeeklyAd, rawAdplexMIStores, miStoreListId);
						
						PrestoUtil.moveFile(zipFile, inputFilePath + "/" + Constants.COMPLETED_FOLDER);
						//Delete the files from temp folder
						FileUtils.deleteDirectory(new File(unZipFileFolder));
						PristineDBUtil.commitTransaction(_Conn, "Weekly Ad");
					} else {
						//Delete the files from temp folder
						FileUtils.deleteDirectory(new File(unZipFileFolder));
						PrestoUtil.moveFile(zipFile, inputFilePath + "/" + Constants.COMPLETED_FOLDER);
						logger.error("File " + zipFile + " is not processed. Either "
								+ " Ad Plex of Tops or GU stores is required to process this file");
					}
					logger.info("Processing of file: " +  zipFile + " is Completed..." );
				} catch (GeneralException | Exception ge) {
					ge.printStackTrace();
					PristineDBUtil.rollbackTransaction(_Conn, "Weekly Ad");
					logger.error("Error while processing zip file - " + zipFile + "---" + ge);
					PrestoUtil.moveFile(zipFile, inputFilePath + "/" +  Constants.BAD_FOLDER);
				}
			}
		} catch (GeneralException ge) {
			ge.printStackTrace();
			logger.error("Error in processAdplexFile() - " + ge);
		} finally {
			PristineDBUtil.close(_Conn);
		}
	}

	/**
	 * loads the processed the data.
	 * @param adList
	 * @param adPlexList
	 * @throws GeneralException 
	 */
	private void loadAdData(List<RawWeeklyAd> adList, List<RawWeeklyAd> adPlexList, int locationId) throws GeneralException {
		HashMap<AdKey, Integer> clientForecastMap = new HashMap<>();
		HashMap<AdKey, List<RawWeeklyAd>> adPlexMap = new HashMap<>();
		// Create Weekly Ad
		WeeklyAd weeklyAd = new WeeklyAd();
		weeklyAd.setAdName(getAdName());
		weeklyAd.setLocationLevelId(Constants.STORE_LIST_LEVEL_ID);
		weeklyAd.setLocationId(locationId);
		weeklyAd.setCalendarId(getCalendarId());
		weeklyAd.setWeekStartDate(getWeekStartDate());
		weeklyAd.setCreatedBy(userName);
		weeklyAd.setApprovedBy(userName);
		try {
			logger.debug("Processing for location id:" + locationId);
			getClientForecastMapFromAdData(locationId, adList, clientForecastMap);
			getAdplexMap(adPlexList, adPlexMap);

			logger.info("loadAdData() - Proceessing Ad plex at Page & Block level...");
			int counter = 0;
			List<PromoBuyItems> updateList = new ArrayList<>();
			for (Map.Entry<AdKey, List<RawWeeklyAd>> entry : adPlexMap.entrySet()) {
				logger.debug("loadAdData() - Checking items from Page -> " + entry.getKey().getPageNumber()
						+ " and Block -> " + entry.getKey().getBlockNumber());
				counter++;
				WeeklyAdPage adPage = addPage(entry.getKey().getPageNumber(), weeklyAd);
				WeeklyAdBlock adBlock = addBlock(entry.getKey().getBlockNumber(), adPage);
				Set<String> itemsToBeChecked = new  HashSet<>();
				ArrayList<PromoDefinition> promotionsToUpdate = new ArrayList<PromoDefinition>();
				Set<String> outputSet = new HashSet<>();
				Set<String> inputSet = new HashSet<>();
				
				//18th Jun 2016 Set actual item count
				adBlock.setActualTotalItems(entry.getValue().size());
				if (clientForecastMap.get(entry.getKey()) != null) {
					adBlock.setAdjustedUnits(clientForecastMap.get(entry.getKey()));
				}
				
				for (RawWeeklyAd adPlex : entry.getValue()) {
					getItemsToBeChecked(itemsToBeChecked, adPlex, inputSet);
					if(itemsToBeChecked.size() == 0){
						logger.warn("loadAdData() - Items not found -> [" + "Page: " + entry.getKey().getPageNumber() +
								" |Block: " + entry.getKey().getBlockNumber() + 
								" |ItemCode: " + adPlex.getItemCode() + "]");
						continue;
					}
				}
				if (itemsToBeChecked.size() > 0) {
					promotionsToUpdate = (ArrayList<PromoDefinition>) promoDAO.getPromotionsByItems(_Conn,
							itemsToBeChecked, getStartCalendarId(), getEndCalendarId(), weeklyAd.getLocationLevelId(),
							weeklyAd.getLocationId(), chainId);
					if (promotionsToUpdate.size() > 0) {
						//Group at promotion level.
						List<PromoDefinition> actPromotions = groupPromotions(promotionsToUpdate);
						for (PromoDefinition promotion : actPromotions) {
							promotion.setCreatedBy(Constants.BATCH_USER);
							promotion.setApprovedBy(Constants.BATCH_USER);
							adBlock.addPromotion(promotion);
							//For debugging.....
							for(PromoBuyItems promoBuyItems: promotion.getBuyItems()){
								outputSet.add(String.valueOf(promoBuyItems.getItemCode()));
								if(inputSet.contains(String.valueOf(promoBuyItems.getItemCode()))){
									updateList.add(promoBuyItems);
								}
							}
						}
//						if (clientForecastMap.get(entry.getKey()) != null) {
//							adBlock.setAdjustedUnits(clientForecastMap.get(entry.getKey()));
//						}
						
						logNotMatchedItems(inputSet, outputSet, entry.getKey());
//						adPage.addBlock(adBlock);
//						weeklyAd.addPage(adPage);
					}
					else{
						logger.warn("loadAdData() - Promotions not found -> [Page: " + entry.getKey().getPageNumber() + 
								" |Block: " + entry.getKey().getBlockNumber() + "]");
					}
				}

				// Add blocks, even when there is no items
				adPage.addBlock(adBlock);
				weeklyAd.addPage(adPage);
					
				logger.debug("loadAdData() - # of items proccessed -> " + entry.getValue().size());
				if(counter % 50 == 0){
					logger.info("loadAdData() - # of Page & Block combinations processed - " + counter);
				}
			}
			
			logger.info("loadAdData() - Updating Ad plex flag...");
			promoDAO.updateAdplexFlag(_Conn, updateList);
			logger.info("loadAdData() - Updating Ad plex flag is completed.");
			logger.info("loadAdData() - Saving Weekly Ad...");
			setTotalPages(weeklyAd);
			adDAO.saveWeeklyAd(weeklyAd);
			logger.info("loadAdData() - Saving Weekly Ad is completed.");
			logger.info("loadAdData() - Getting # of Lig and Non Lig count at block level...");
			HashMap<Long, Integer> blockItemMap = 
					promoDAO.getNoOfLigAndNonLigAtBlockLevel(_Conn, getWeekStartDate(), getWeekEndDate());
			logger.info("loadAdData() - Getting # of Lig and Non Lig count at block level is completed.");
			logger.info("loadAdData() - Updating blocks with # of Lig and Non Lig count...");
			promoDAO.updateBlocksWithItemCount(_Conn, blockItemMap);
			logger.info("loadAdData() - Updating blocks is completed.");
			setAdPlexItemValues(adPlexList, weeklyAd);
			logger.info("Updating AdPlexItem table is completed...");
			
		} catch (Exception e) {
			e.printStackTrace();
			logger.error("Error -- loadAdData() " + e.toString(), e);
			throw new GeneralException("Error -- loadAdData()", e);
		}
	}
	
	
	/**
	 * Fills list of items for an retailer item code
	 * @param itemsToBeChecked
	 * @param adPlex
	 * @param inputSet
	 * @param outputSet
	 */
	private void getItemsToBeChecked(Set<String> itemsToBeChecked, 
			RawWeeklyAd adPlex, Set<String> inputSet){
		
		//Get items from RETAILER_ITEM_CODE_MAP (Last 6 numbers of RETAILER_ITEM_CODE column)...
		List<ItemDTO> itemCodes =  getItemsFromRetailerItemCodeMap(adPlex.getItemCode());
		/*if (itemCodes.size() == 0) {
			//If not found in RETAILER_ITEM_CODE_MAP, Check in ITEM_LOOKUP (RETAILER_ITEM_CODE column)...
			List<String> items = getItemsFromItemLookup(adPlex.getItemCode());
			if(items.size() > 0){
				itemsToBeChecked.addAll(items);
				inputSet.addAll(items);
			}
		}		
		else{*/
			//If items found, check the item belongs to an lir, 
			//if so get all relevant items and add it to the list. 
		if(itemCodes.size() > 0){
			for(ItemDTO itemDTO: itemCodes){
				inputSet.add(String.valueOf(itemDTO.itemCode));
				itemsToBeChecked.add(String.valueOf(itemDTO.itemCode));
			}
		}
		//}
	}
	
	/**
	 * Logs the not matched items from the Promotions
	 * @param inSet
	 * @param outSet
	 * @param adKey
	 */
	private void logNotMatchedItems(Set<String> inSet, Set<String> outSet, AdKey adKey)
	{
		StringBuilder sb = new StringBuilder();
		for(String itemCode: inSet){
			if(!outSet.contains(itemCode)){
				sb.append(itemCode + ", ");
			}
		}
		logger.debug("Given " + inSet.size() + " items.");
		logger.debug("Retrieved " + outSet.size() + " items.");
		if(!sb.toString().isEmpty()){
			logger.warn("[Page: " + adKey.getPageNumber()+ 
					" |Block: " + adKey.getBlockNumber() + 
					"] -> Promotion not found for Presto Item codes: " + sb.toString());	
		}
	}
	/**
	 * 
	 * @param promotionsToUpdate
	 * @return gives list of promotions after grouping from item level
	 */
	private List<PromoDefinition> groupPromotions(ArrayList<PromoDefinition> promotionsToUpdate){
		List<PromoDefinition> actualPromotions = new ArrayList<>();
		HashMap<Long, List<PromoDefinition>> promoMap = new HashMap<>();
		for(PromoDefinition promotion: promotionsToUpdate){
			if(promoMap.get(promotion.getPromoDefnId()) == null){
				List<PromoDefinition> tempList = new ArrayList<>();
				tempList.add(promotion);
				promoMap.put(promotion.getPromoDefnId(), tempList);
			}
			else{
				List<PromoDefinition> tempList = promoMap.get(promotion.getPromoDefnId());
				tempList.add(promotion);
				promoMap.put(promotion.getPromoDefnId(), tempList);
			}
		}
		
		for(Map.Entry<Long, List<PromoDefinition>> entry: promoMap.entrySet()){
			PromoDefinition promoDefinition = entry.getValue().get(0);
			for(PromoDefinition promotion: entry.getValue()){
				PromoBuyItems promoBuyItems = new PromoBuyItems();
				promoBuyItems.setPromoDefnId(promotion.getPromoDefnId());
				promoBuyItems.setItemCode(promotion.getItemCode());
				promoDefinition.addBuyItems(promoBuyItems);
			}
			promoDefinition.setTotalItems(promoDefinition.getBuyItems().size());
			actualPromotions.add(promoDefinition);
		}
		
		return actualPromotions;
	}
		
	/**
	 * Sets total # of pages for a weekly ad.
	 * @param weeklyAd
	 */
	private void setTotalPages(WeeklyAd weeklyAd) {
		if (weeklyAd.getAdPages() != null && weeklyAd.getAdPages().size() > 0) {
			int totalPages = weeklyAd.getAdPages().lastKey();
			if (totalPages % 2 == 0)
				weeklyAd.setTotalPages(totalPages);
			else
				weeklyAd.setTotalPages(totalPages + 1);
		} else {
			weeklyAd.setTotalPages(0);
		}
	}

	/**
	 * @param retailerItemCode
	 * @return list of Presto item codes.
	 */
	private List<ItemDTO> getItemsFromRetailerItemCodeMap(String retailerItemCode) {
		ItemDTO itemIN = new ItemDTO();
		itemIN.setUpc(retailerItemCode);
		itemIN.setRetailerItemCode(retailerItemCode);
		List<ItemDTO> itemList = new ArrayList<ItemDTO>();
		if(isTesting){
			//For debugging
			try {
				itemList = itemDAO.getItemDetailsFuzzyLookupV2(_Conn, itemIN);
			} catch (GeneralException ge) {
				logger.error("Error when retrieving item code - " + ge);
			}
		}
		else{
			if(retItemCodeMap.get(retailerItemCode) != null){
				itemList = retItemCodeMap.get(retailerItemCode);
			}
		}
		return itemList;
	}
	

	/**
	 * Gives distinct map of page and blocks along with items.
	 * @param adPlexList
	 * @param adPlexMap
	 */
	private void getAdplexMap(List<RawWeeklyAd> adPlexList, HashMap<AdKey, List<RawWeeklyAd>> adPlexMap) {
		String[] ignorePageNumbers = PropertyManager.getProperty("IGNORE_PAGE_NUMBERS").split(",");
		List<Integer> ignorePageList = new ArrayList<>();
		for (String pageNumber : ignorePageNumbers) {
			ignorePageList.add(Integer.parseInt(pageNumber));
		}
		for (RawWeeklyAd adPlex : adPlexList) {
			if (ignorePageList.contains(adPlex.getPageNo()) || (adPlex.getPageNo() == 0)) {
				continue;
			}
			AdKey key = new AdKey(adPlex.getPageNo(), adPlex.getBlockNo());
			if (adPlexMap.get(key) == null) {
				List<RawWeeklyAd> adPlexItems = new ArrayList<>();
				adPlexItems.add(adPlex);
				adPlexMap.put(key, adPlexItems);
			} else {
				List<RawWeeklyAd> adPlexItems = adPlexMap.get(key);
				adPlexItems.add(adPlex);
				adPlexMap.put(key, adPlexItems);
			}
		}
	}

	/**
	 * Gives client forecast retrieved from Ad file.
	 * @param adList
	 * @param clientForecastMap
	 */
	private void getClientForecastMapFromAdData(int locationId, List<RawWeeklyAd> adList, HashMap<AdKey, Integer> clientForecastMap) {
		if (adList != null) {
			for (RawWeeklyAd ad : adList) {
				String adjustedUnits = "";
				int pageNo = 0, blockNo = 0;
				if (locationId == guStoreListId) {
					pageNo = ad.getGuPage();
					blockNo = ad.getGuBlock();
					adjustedUnits = ad.getGuAdjustedUnits();
				} else if (locationId == topsWithoutGUStoreListId) {
					pageNo = ad.getTopsPage();
					blockNo = ad.getTopsBlock();
					adjustedUnits = ad.getTopsAdjustedUnits();
				}

				if (pageNo == 0 || adjustedUnits == null || adjustedUnits.trim().isEmpty()) {
					continue;
				}
				AdKey key = new AdKey(pageNo, blockNo);
				if(isNumeric(adjustedUnits)){
				if (clientForecastMap.get(key) == null) {
					logger.debug("ad:" + ad.toString());
					logger.debug("pageNo:" + pageNo + ",blockNo:" + blockNo + ",adjustedUnits:" + adjustedUnits);
					int clientForecast = (int) Double.parseDouble(adjustedUnits);
					if (clientForecast > 0l) {
						clientForecastMap.put(key, clientForecast);
					}
				} else {
					int clientForecast = (int) Double.parseDouble(adjustedUnits);
					if (clientForecast > 0l) {
						int clientForecastTemp = clientForecastMap.get(key);
						clientForecastMap.put(key, clientForecast + clientForecastTemp);
					}
				}
				}else{
					logger.warn("AdjustedUnits having String Values pageNo:" + pageNo + ",blockNo:" + blockNo + ",adjustedUnits:" + adjustedUnits);
				}
			}
		}
	}

	/**
	 * Sets required information for Ad processing
	 * @param fileName
	 * @throws ParseException 
	 * @throws GeneralException 
	 */
	private void setCalendarId(String fileName) throws Exception, GeneralException {
		int index = fileName.lastIndexOf("/");
		String xlFileName = fileName.substring(index + 1, fileName.length());
		String strDate = xlFileName.replaceAll("_", "").substring(0, 6);
		setAdName(strDate + "_Ad_Data");
		SimpleDateFormat sf = new SimpleDateFormat("MMddyy");
		Date d = sf.parse(strDate);
		SimpleDateFormat nf = new SimpleDateFormat("MM/dd/yyyy");
		strDate = nf.format(d);
		RetailCalendarDAO calendarDAO = new RetailCalendarDAO();
		RetailCalendarDTO calDTO = calendarDAO.getCalendarId(_Conn, strDate, Constants.CALENDAR_WEEK);
		setCalendarId(calDTO.getCalendarId());
		setWeekStartDate(calDTO.getStartDate());
		setWeekEndDate(calDTO.getEndDate());
		calDTO = calendarDAO.getCalendarId(_Conn, getWeekStartDate(), Constants.CALENDAR_DAY);
		setStartCalendarId(calDTO.getCalendarId());
		calDTO = calendarDAO.getCalendarId(_Conn, getWeekEndDate(), Constants.CALENDAR_DAY);
		setEndCalendarId(calDTO.getCalendarId());

	}

	/**
	 * Updates skipped items
	 *//*
	private void updateIgnoredItems(){
		if(missedItems.size() > 0){
			StringBuilder sb = new StringBuilder();
			for(String itemCode: missedItems){
				sb.append(itemCode + ", ");
			}
			logger.info("updateIgnoredItems() - # of items skipped -> " + missedItems.size());
			logger.info("updateIgnoredItems() - Skipped Items(No Presto Itemcode): " + sb.toString());
		}
		if(missedPageBlocks.size() > 0){
			StringBuilder sb = new StringBuilder();
			for(String pageBlock: missedPageBlocks){
				sb.append(pageBlock + ", ");
			}
			logger.info("updateIgnoredItems() - # of Page & Block combinations skipped -> " + missedPageBlocks.size());
			logger.info("updateIgnoredItems() - Skipped Page & Block combinations(No Promotion): " + sb.toString());
		}
	}*/
	
	/**
	 * Gives map of AdPlex file
	 * @param fieldNames
	 */
	private void mapFieldNamesForAdPlex(TreeMap<Integer, String> fieldNames) {
		fieldNames.put(1, "pageNo");
		fieldNames.put(2, "blockNo");
		fieldNames.put(4, "itemCode");
	}

	/**
	 * Gives map of Ad file
	 * @param fieldNames
	 * @throws ParseException 
	 */
	private void mapFieldNames(TreeMap<Integer, String> fieldNames) throws ParseException {
		DateFormat formatterMMddyy = new SimpleDateFormat("MM/dd/yy");
		Date processingWeekDate = formatterMMddyy.parse(getWeekStartDate());
		Date tempWeekDate1 = formatterMMddyy.parse("07/25/15");
		Date tempWeekStartDate2 = formatterMMddyy.parse("10/30/16");
		// Positions Changed from 1st week of Sept 2015
		// NU: 14th Nov 2016, Positions changed again from week of 10/30 2016
		fieldNames.put(0, "topsPage");
		fieldNames.put(1, "topsBlock");
		fieldNames.put(3, "itemCode");
		fieldNames.put(4, "itemDesc");
		
		//if week start date <= 07/25/15
		if(processingWeekDate.before(tempWeekDate1) || processingWeekDate.equals(tempWeekDate1)) {
			fieldNames.put(41, "guPage");
			fieldNames.put(42, "guBlock");
			fieldNames.put(30, "topsAdjustedUnits");
			fieldNames.put(58, "guAdjustedUnits");
		} else if (processingWeekDate.equals(tempWeekStartDate2) || processingWeekDate.before(tempWeekStartDate2)) { 
			fieldNames.put(42, "guPage");
			fieldNames.put(43, "guBlock");
			fieldNames.put(31, "topsAdjustedUnits");
			fieldNames.put(59, "guAdjustedUnits");
		} else {
			fieldNames.put(56, "guPage");
			fieldNames.put(57, "guBlock");
			fieldNames.put(33, "topsAdjustedUnits");
			fieldNames.put(73, "guAdjustedUnits");
		}
	}
	
	/**
	 * Checks existing pages and adds new page if not added.
	 * @param page
	 * @param weeklyAd
	 * @return WeeklyAdPage
	 */

	private WeeklyAdPage addPage(int page, WeeklyAd weeklyAd) {
		WeeklyAdPage adPage = null;
		if (weeklyAd.getAdPages().get(page) != null)
			adPage = weeklyAd.getAdPages().get(page);
		else {
			adPage = new WeeklyAdPage();
			adPage.setPageNumber(page);
		}
		return adPage;
	}

	/**
	 * Checks existing blocks and adds new block if not added.
	 * @param block
	 * @param adPage
	 * @return WeeklyAdBlock
	 */
	private WeeklyAdBlock addBlock(int block, WeeklyAdPage adPage) {
		WeeklyAdBlock adBlock = null;
		if (adPage.getAdBlocks().get(block) != null)
			adBlock = adPage.getAdBlocks().get(block);
		else {
			adBlock = new WeeklyAdBlock();
			adBlock.setBlockNumber(block);
		}
		return adBlock;
	}
	

	/**
	 * To insert block id and retailer item code into AdPlexItem table....
	 * @param adPlexList
	 * @throws GeneralException
	 */
	public void setAdPlexItemValues(List<RawWeeklyAd> adPlexList, WeeklyAd weeklyAd) throws GeneralException{
		HashMap<Long, List<String>> blockIdandRetailerItemCode = new HashMap<Long, List<String>>();
		//Get Block id values with respect to Page and Block number
		HashMap<AdKey, Long> blockIdMap = adDAO.getBlockIdMap(weeklyAd);
		//Set values in map with respect to Block id by looping adPlexList...
		for(RawWeeklyAd adPlex : adPlexList){
			AdKey key = new AdKey(adPlex.getPageNo(), adPlex.getBlockNo());
//			String key = adPlex.getPageNo()+"-"+adPlex.getBlockNo();
			if(blockIdMap.get(key)!= null){
				Long blockId = blockIdMap.get(key);
				List<String> itemCodeList = new ArrayList<String>();
				if(blockIdandRetailerItemCode.get(blockId)!= null){
					itemCodeList = blockIdandRetailerItemCode.get(blockId);
				}	
				itemCodeList.add(adPlex.getItemCode());
				blockIdandRetailerItemCode.put(blockId, itemCodeList);																			
			}else{
				logger.debug("block id is null for the key: "+key);
			}
		}
		//Iterate blockIdandRetailerItemCode map and insert those values in database...
		int noOfValuesInserted = adDAO.insertintoAdPlexItems(blockIdandRetailerItemCode);
		logger.info("Number of item inserted in AdPlexItem table: "+noOfValuesInserted);
	}
	
	public int getCalendarId() {
		return calendarId;
	}

	public void setCalendarId(int calendarId) {
		this.calendarId = calendarId;
	}

	public String getAdName() {
		return adName;
	}

	public void setAdName(String adName) {
		this.adName = adName;
	}

	public String getWeekStartDate() {
		return weekStartDate;
	}

	public void setWeekStartDate(String weekStartDate) {
		this.weekStartDate = weekStartDate;
	}

	public String getWeekEndDate() {
		return weekEndDate;
	}

	public void setWeekEndDate(String weekEndDate) {
		this.weekEndDate = weekEndDate;
	}

	public int getLocationId() {
		return locationId;
	}

	public void setLocationId(int locationId) {
		this.locationId = locationId;
	}

	public int getStartCalendarId() {
		return startCalendarId;
	}

	public void setStartCalendarId(int startCalendarId) {
		this.startCalendarId = startCalendarId;
	}

	public int getEndCalendarId() {
		return endCalendarId;
	}

	public void setEndCalendarId(int endCalendarId) {
		this.endCalendarId = endCalendarId;
	}
	
	/**
	 * To check string is Number or not
	 * @param str
	 * @return
	 */
	public static boolean isNumeric(String str)  
	{  
	  try  
	  {  
	    double d = Double.parseDouble(str);  
	  }  
	  catch(NumberFormatException nfe)  
	  {  
	    return false;  
	  }  
	  return true;  
	}
	
}
