package com.pristine.dataload.tops;

import java.sql.Connection;
import java.text.DateFormat;
//import java.text.DecimalFormat;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Date;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
//import java.util.Map;
import java.util.Set;
import java.util.TreeMap;

import org.apache.log4j.Logger;
import org.apache.log4j.PropertyConfigurator;

import com.pristine.dao.DBManager;
import com.pristine.dao.ItemDAO;
//import com.pristine.dao.LocationGroupDAO;
import com.pristine.dao.RetailCalendarDAO;
import com.pristine.dao.RetailPriceDAO;
//import com.pristine.dao.offermgmt.promotion.DisplayTypeDAO;
import com.pristine.dao.offermgmt.promotion.PromotionDAO;
import com.pristine.dao.offermgmt.weeklyad.WeeklyAdDAO;
import com.pristine.dto.ItemDTO;
//import com.pristine.dto.PromoDataDTO;
import com.pristine.dto.RetailCalendarDTO;
//import com.pristine.dto.RetailPriceDTO;
//import com.pristine.dto.offermgmt.promotion.PromoBuyItems;
import com.pristine.dto.offermgmt.promotion.PromoDefinition;
//import com.pristine.dto.offermgmt.promotion.PromoDisplay;
import com.pristine.dto.offermgmt.promotion.PromoLocation;
import com.pristine.dto.offermgmt.weeklyad.RawWeeklyAd;
import com.pristine.dto.offermgmt.weeklyad.WeeklyAd;
import com.pristine.dto.offermgmt.weeklyad.WeeklyAdBlock;
import com.pristine.dto.offermgmt.weeklyad.WeeklyAdPage;
import com.pristine.exception.GeneralException;
import com.pristine.parsinginterface.ExcelFileParser;
import com.pristine.parsinginterface.PristineFileParser;
import com.pristine.util.Constants;
import com.pristine.util.PrestoUtil;
import com.pristine.util.PristineDBUtil;
import com.pristine.util.PropertyManager;

/**
 * Note: The excel cell position changes once in a while, if the position is changed 
 * then the entire program may not work as expected, 
 * files from week ending 08/01 had some shift in positions  
 * */

public class AdLoader extends PristineFileParser<Object>{
	private static Logger  logger = Logger.getLogger("AdLoader");
	private Connection 	_Conn = null;
	private String sheetName = "Ad Financials";
	private String userName = "BATCH";
	private String adName = null;
	private String weekStartDate = null;
	private String weekEndDate = null;
	private int calendarId = -1;
	private int startCalendarId = -1;
	private int endCalendarId = -1;
	private int locationId = -1;
//	private DecimalFormat df = new DecimalFormat("#######.##");
	HashMap<String, Integer> calendarMap = new HashMap<String, Integer>();	
//	private static final String STORE_LOCATION_BOTH = "B";
	private static final String STORE_LOCATION_TOPS = "T";
	private static final String STORE_LOCATION_GU = "G";

//	private HashMap<String, Integer> locationIdMap = new HashMap<String, Integer>();
	
	private WeeklyAdDAO adDAO = null;
//	private LocationGroupDAO locationGroupDAO = null;
	private PromotionDAO promoDAO = null;
//	private RetailCalendarDAO calDAO = null;
	ItemDAO itemDAO = null;
	int chainId;
	private HashMap<String, List<String>> itemCodeMap = null;
	/**
	 * Constructor
	 */
	public AdLoader ()
	{
        try
		{
        	_Conn = DBManager.getConnection();
        	RetailPriceDAO rpDAO = new RetailPriceDAO();
        	chainId = Integer.parseInt(rpDAO.getChainId(_Conn));
	    }catch (GeneralException gex) {
	        logger.error("Error when creating connection - " + gex);
	    }
        
        adDAO = new WeeklyAdDAO(_Conn);
        promoDAO = new PromotionDAO(_Conn);
//        calDAO = new RetailCalendarDAO();
//        locationGroupDAO = new LocationGroupDAO(_Conn);
        itemDAO = new ItemDAO();
	}
	
	/**
	 * Main method
	 * @param args[0] inputFilePath
	 * @throws Exception
	 */
	public static void main(String[] args) throws Exception 
	{
		PropertyConfigurator.configure("log4j-ad-loader.properties");
		PropertyManager.initialize("analysis.properties");
        
		String relativePath = null;
		if(args.length > 0){
			relativePath = args[0];
		}else{
			logger.error("Invalid input - Input File Path is mandatory");
		}
		
		AdLoader adLoader = new AdLoader();
		
		adLoader.loadAdData(relativePath);
	}
	
	public void loadAdData(String relativePath){
		ExcelFileParser<RawWeeklyAd> parser = new ExcelFileParser<RawWeeklyAd>();
		parser.setFirstRowToProcess(3);
		//ArrayList<String> fileList = parser.getFiles(relativePath);
		ArrayList<String> fileList = parser.getAllExcelVariantFiles(relativePath);
		ArrayList<String> fileList1 = null;	
		TreeMap<Integer, String> fieldNames = new TreeMap<Integer, String>();
		 String archivePath = getRootPath() + "/" + relativePath + "/";
		
		try{	
			itemCodeMap = itemDAO.getItemCodesForRetailerItemcode(_Conn);
			//itemCodeMap = new HashMap<String, List<String>>(); 
			
			logger.info("Distinct items size - " + itemCodeMap.size());
			
		}catch(GeneralException ge){
			logger.error("Error retrieving item cache. Unable to proced. " + ge);
			return;
		}
		try{
			for(String fileName : fileList){
				fileList1 = new ArrayList<String>();
				fieldNames = new TreeMap<Integer, String>();
				logger.info("Processing file - " + fileName);
				setCalendarId(fileName);
				mapFieldNames(fieldNames);
				setLocationId(_Conn);
				List<RawWeeklyAd> rawList = parser.parseExcelFileV2(RawWeeklyAd.class, fileName, sheetName, fieldNames);
				processAdData(rawList);
				PristineDBUtil.commitTransaction(_Conn, "Ad data loaded");
				fileList1.add(fileName);
				PrestoUtil.moveFiles(fileList1, archivePath + Constants.COMPLETED_FOLDER);
			}
			
			
		}catch(GeneralException | ParseException ge){
			logger.error("Error while processing ad - " + ge);
			PrestoUtil.moveFiles(fileList1, archivePath + Constants.BAD_FOLDER);
		}
		PristineDBUtil.close(_Conn);
	}
	
	public void processAdData(List<RawWeeklyAd> rawList) throws GeneralException{
		int page0Count = 0;
//		int ignoreCount = 0;
//		int multipleEntries = 0;
//		int noitemInDB = 0;
		
		int guStoreListId = Integer.parseInt(PropertyManager.getProperty("TOPS_GU_STORE_LIST_ID"));
		int topsWithoutGUStoreListId = Integer.parseInt(PropertyManager.getProperty("TOPS_WITHOUT_GU_STORE_LIST_ID"));
	 
//		RetailPriceDAO rpDAO = new RetailPriceDAO();
//		DisplayTypeDAO displayDAO = new DisplayTypeDAO(_Conn);
		HashSet<Integer> itemSet = new HashSet<Integer>();
//		HashMap<String, Integer> displayTypeMap = displayDAO.getDisplayTypes();
		List<PromoDefinition> updateList = new ArrayList<PromoDefinition>();
//		List<PromoDisplay> displayList = new ArrayList<PromoDisplay>();
		
		// Create Weekly Ad
		WeeklyAd weeklyAdTops = new WeeklyAd();
		weeklyAdTops.setAdName(getAdName());
		weeklyAdTops.setLocationLevelId(Constants.STORE_LIST_LEVEL_ID);
		weeklyAdTops.setLocationId(topsWithoutGUStoreListId);
		weeklyAdTops.setCalendarId(getCalendarId());
		weeklyAdTops.setWeekStartDate(getWeekStartDate());
		weeklyAdTops.setCreatedBy(userName);
		weeklyAdTops.setApprovedBy(userName);
		
		WeeklyAd weeklyAdGU = new WeeklyAd();
		weeklyAdGU.setAdName(getAdName());
		weeklyAdGU.setLocationLevelId(Constants.STORE_LIST_LEVEL_ID);
		weeklyAdGU.setLocationId(guStoreListId);
		weeklyAdGU.setCalendarId(getCalendarId());
		weeklyAdGU.setWeekStartDate(getWeekStartDate());
		weeklyAdGU.setCreatedBy(userName);
		weeklyAdGU.setApprovedBy(userName);
		
		
		List<String> ignoredItems = new ArrayList<String>();
		
		int counter = 0;
		try{
		for(RawWeeklyAd ad : rawList){
			ArrayList<PromoDefinition> promotionsToUpdate = new ArrayList<PromoDefinition>();
			counter++;
			if(counter%100 == 0)
				logger.info("# of records processed - " + counter);
			boolean isPromoOnly = false;
			String itemDesc = ad.getItemDesc();
			if(itemDesc == null || itemDesc.equals(Constants.EMPTY)){
//				ignoreCount++;
				//logger.debug("Ignoring " + itemDesc);
				continue;
			}
			
			boolean ignore = isIgnorePromo(itemDesc, ad.getAdRetail());
			if(ignore){
//				ignoreCount++;
				logger.debug("Ignoring " + itemDesc);
				continue;
			}	
			
			if(ad.getItemCode() == null || ad.getItemCode().equals(Constants.EMPTY)){
//				ignoreCount++;
				logger.debug("Ignoring " + itemDesc);
				continue;
			}
			
			String retailerItemCode = "";   //ad.getItemCode().replaceAll("[^-0-9]", "");
			
				if(ad.getItemCode().contains("-")){
					String[] itemCodeArray = ad.getItemCode().split("-");
					//Get second part of code as retailer item code if the code is separated by "-".
					if(itemCodeArray.length > 1)
						retailerItemCode = itemCodeArray[1]; 
				}
				else{
					retailerItemCode = ad.getItemCode(); 
				}
			retailerItemCode = retailerItemCode.replaceAll("[^\\d.]", "");
			
			if(ad.getTopsPage() == 0 && ad.getGuPage() == 0){
				logger.debug("Page number not found for " + itemDesc);
				page0Count++;
				isPromoOnly = true;
			}
			
			WeeklyAdPage adPageTops = null;
			WeeklyAdPage adPageGU = null;
			WeeklyAdBlock adBlockTops = null;
			WeeklyAdBlock adBlockGU = null;
			
			if(!isPromoOnly){
				// Create Weekly Ad Page
//				int page = ad.getPage();
//				if(weeklyAdTops.getAdPages().get(page) != null)
//					adPage = weeklyAdTops.getAdPages().get(page); 
//				else{
//					adPage = new WeeklyAdPage();
//					adPage.setPageNumber(page);
//				}
				
				// Create Weekly Ad Block
//				int block = ad.getBlock();
//				if(adPage.getAdBlocks().get(block) != null)
//					adBlock = adPage.getAdBlocks().get(block);
//				else{
//					adBlock = new WeeklyAdBlock();
//					adBlock.setBlockNumber(block);
//				}
				
				if(ad.getStoreLocation().equals(STORE_LOCATION_TOPS)) {
					adPageTops = addPage(ad, weeklyAdTops, STORE_LOCATION_TOPS);
				} else if (ad.getStoreLocation().equals(STORE_LOCATION_GU)) {
					adPageGU = addPage(ad, weeklyAdGU, STORE_LOCATION_GU);
				}else {
					adPageTops = addPage(ad, weeklyAdTops, STORE_LOCATION_TOPS);
					adPageGU = addPage(ad, weeklyAdGU, STORE_LOCATION_GU);
				}
				
				if(ad.getStoreLocation().equals(STORE_LOCATION_TOPS)) {
					adBlockTops = addBlock(ad, adPageTops, STORE_LOCATION_TOPS);
				} else if (ad.getStoreLocation().equals(STORE_LOCATION_GU)) {
					adBlockGU =  addBlock(ad, adPageGU, STORE_LOCATION_GU);
				}else {
					adBlockTops = addBlock(ad, adPageTops, STORE_LOCATION_TOPS);
					adBlockGU = addBlock(ad, adPageGU, STORE_LOCATION_GU);
				}
				
			}
				List<String> itemsToBeChecked = getItemsToBeChecked(retailerItemCode, itemDesc, itemSet, ignoredItems);
//				Set<String> itemsSet = new HashSet<String>();
//				itemsSet.addAll(itemsToBeChecked);
				if(itemsToBeChecked != null && itemsToBeChecked.size() > 0){
					/* int startCalId = 0;
					 if(calendarMap.get(getWeekStartDate()) == null){
						 try{
							 startCalId = ((RetailCalendarDTO)calDAO.getCalendarId(_Conn, getWeekEndDate(), Constants.CALENDAR_DAY)).getCalendarId();
							 calendarMap.put(getWeekStartDate(), startCalId);
						 }
						 catch(GeneralException e){
							 logger.error("Error while getting calendar id." + e);
						 }
					 }
					 else{
						 startCalId = calendarMap.get(getWeekStartDate()) ; 
					 }
					 
					if(startCalId > 0)*/
					promotionsToUpdate = (ArrayList<PromoDefinition>) promoDAO.getPromotionsByItemsForAdFile(_Conn, itemsToBeChecked,
							getWeekStartDate(), getWeekEndDate());
						 
					if(promotionsToUpdate.size() > 0){
						//Update promotions with ad and display
						//int displayTypeId = 0;
						//if(ad.getDisplayType() != null)
							//displayTypeId = decodeDisplayType(displayTypeMap, ad.getDisplayType());
						//Temporary solution. Update subdisplay id as displaytypeid. 
						//int subDisplayTypeId = displayTypeId;
						for(PromoDefinition promotion : promotionsToUpdate){
							promotion.setWeekStartDate(getWeekStartDate());
							promotion.setPromoName(itemDesc);
							promotion.setCreatedBy(Constants.BATCH_USER);
							promotion.setApprovedBy(Constants.BATCH_USER);
							try{
								promotion.setAdjustedUnits(Math.round(Double.valueOf(ad.getTopsAdjustedUnits()))
										+ Math.round(Double.valueOf(ad.getGuAdjustedUnits())));
							} catch(Exception ex){
								
							}
							//promotion.setAdjustedUnits(Math.round(ad.getTopsAdjustedUnits()) + Math.round(ad.getGuAdjustedUnits()));
//							if(displayTypeId > 0){
//								PromoDisplay promoDisplay = new PromoDisplay();
//								promoDisplay.setDisplayTypeId(displayTypeId);
//								promoDisplay.setSubDisplayTypeId(subDisplayTypeId);
//								promoDisplay.setLocationId(getLocationId());
//								promoDisplay.setLocationLevelId(Constants.CHAIN_LEVEL_ID);
//								promoDisplay.setPromoDefId(promotion.getPromoDefnId());
//								promoDisplay.setCalendarId(getCalendarId());
//								if(!displayList.contains(promoDisplay))
//									displayList.add(promoDisplay);
//							}
					}
						if(!isPromoOnly){
							for(PromoDefinition promotion: promotionsToUpdate){
								//adBlock.addPromotion(promotion);
								
								//For debugging to check if promotion location are same as in 
								//Weekly ad. It is expected to be the same. No harm if below codes are comments
								validatePromoLocationForDebug(ad, topsWithoutGUStoreListId, guStoreListId, promotion);
								
								if(ad.getStoreLocation().equals(STORE_LOCATION_TOPS)) {
									//promotion.setAdjustedUnits(ad.getTopsAdjustedUnits());
									adBlockTops.addPromotion(promotion);
								} else if (ad.getStoreLocation().equals(STORE_LOCATION_GU)) {
									//promotion.setAdjustedUnits(ad.getGuAdjustedUnits());
									adBlockGU.addPromotion(promotion);
								}else {
									//Take copy of promotion as adjusted units varies
									//PromoDefinition topsPromotion = new PromoDefinition();
									//PromoDefinition guPromotion = new PromoDefinition();
									//copyPromotionDetails(promotion, topsPromotion, ad, STORE_LOCATION_TOPS);
									//adBlockTops.addPromotion(topsPromotion);
									//copyPromotionDetails(promotion, guPromotion, ad, STORE_LOCATION_GU);
									//adBlockGU.addPromotion(guPromotion);	
									adBlockTops.addPromotion(promotion);
									adBlockGU.addPromotion(promotion);
								}
							}
							//weeklyAdTops.addPage(adPage);
							
							if(ad.getStoreLocation().equals(STORE_LOCATION_TOPS)) {
								adPageTops.addBlock(adBlockTops);
								weeklyAdTops.addPage(adPageTops);
							} else if (ad.getStoreLocation().equals(STORE_LOCATION_GU)) {
								adPageGU.addBlock(adBlockGU);
								weeklyAdGU.addPage(adPageGU);
							}else {
								if(adBlockTops.getBlockNumber() > 0 && adPageTops.getPageNumber() > 0){
									adPageTops.addBlock(adBlockTops);
									weeklyAdTops.addPage(adPageTops);
								}
								if(adBlockGU.getBlockNumber() > 0 && adPageGU.getPageNumber() > 0){
									adPageGU.addBlock(adBlockGU);
									weeklyAdGU.addPage(adPageGU);				
								}
							}
						}
						updateList.addAll(promotionsToUpdate);
					}
					else{
						ignoredItems.add("Promotion not found for " + retailerItemCode + ", " + itemDesc);
						continue;
					}
				}
				else{
					ignoredItems.add("Item not found Database for " + retailerItemCode + ", " + itemDesc);
				}
		/*	if(!isPromoOnly){
				adBlock.addPromotion(promotion);
				adPage.addBlock(adBlock);
				weeklyAd.addPage(adPage);
			}else{
				promoDAO.savePromotion(promotion);
			}*/
		}
		/*logger.info("Updating promotions...");
		try{
			promoDAO.updatePromo(_Conn, updateList);
			//29th July 2015 - Don't update display from Ad Page File
			//displayDAO.insertOrUpdatePromoDispay(_Conn, displayList);
		}
		catch(GeneralException ge){
			logger.error("Error while updating promotion" + ge);
		}*/
		
		logger.info("Inserting weekly ads...");
		setTotalPages(weeklyAdTops);
		adDAO.saveWeeklyAd(weeklyAdTops);
		setTotalPages(weeklyAdGU);
		adDAO.saveWeeklyAd(weeklyAdGU);
		logger.info("Not in Ad Promo Only - " + page0Count);
		logger.info("Ignored items - " + ignoredItems.size());
		
		logger.info("Ignored Items");
		logger.info("**************************");
		for(String ignoredItem: ignoredItems){
			logger.info(ignoredItem);
		}
		
		}
		catch(Exception e){
			logger.error("Error while processing ad records" + e);
			e.printStackTrace();
			throw new GeneralException("Error while processing ad records", e);
		}
		logger.info("Ad loading is completed.");
	}
	
	/**
	 * For debugging to check if promotion location are same as in Weekly ad. 
	 * It is expected to be the same. No harm if below codes are comments
	 */
	private void validatePromoLocationForDebug(RawWeeklyAd ad, int topsWithoutGUStoreListId, int guStoreListId,
			PromoDefinition promotion) {
		String actLocation = "";
		List<PromoLocation> promoLocations = promoDAO.getPromotionLocation(promotion.getPromoDefnId());
		for (PromoLocation promoLoc : promoLocations) {
			actLocation = actLocation + "," + promoLoc.getLocationLevelId() + "-" + promoLoc.getLocationId();
		}
		String expectedLocation = "";
		if(ad.getStoreLocation().equals(STORE_LOCATION_TOPS)) {
			expectedLocation  = Constants.STORE_LIST_LEVEL_ID + "-" + topsWithoutGUStoreListId;
		} else if (ad.getStoreLocation().equals(STORE_LOCATION_GU)) {
			expectedLocation  = Constants.STORE_LIST_LEVEL_ID + "-" + guStoreListId;
		}else {
			expectedLocation  = Constants.CHAIN_LEVEL_ID + "-" + chainId;
		}
		
		String locationNotMatchingLog = "Promotions locations is not matching for promo ("
				+ promotion.getPromoDefnId() + " - " + promotion.getPromoName() 
				+ ") Expected : " + expectedLocation + ",Actual:" + actLocation;
		
		
		if (promoLocations.size() == 0 || promoLocations.size() > 1) {
			logger.warn(locationNotMatchingLog);
		} else {
			boolean isLocationMatches = false;
			if (ad.getStoreLocation().equals(STORE_LOCATION_TOPS)) {
				if (promoLocations.get(0).getLocationLevelId() == Constants.STORE_LIST_LEVEL_ID
						&& promoLocations.get(0).getLocationId() == topsWithoutGUStoreListId) {
					isLocationMatches = true;
				}
			} else if (ad.getStoreLocation().equals(STORE_LOCATION_GU)) {
				if (promoLocations.get(0).getLocationLevelId() == Constants.STORE_LIST_LEVEL_ID
						&& promoLocations.get(0).getLocationId() == guStoreListId) {
					isLocationMatches = true;
				}
			} else {
				if (promoLocations.get(0).getLocationLevelId() == Constants.CHAIN_LEVEL_ID
						&& promoLocations.get(0).getLocationId() == chainId) {
					isLocationMatches = true;
				}
			}
			if (!isLocationMatches) {
				logger.warn(locationNotMatchingLog);
			}
		}
	}
	
	private void setTotalPages(WeeklyAd weeklyAd){
		if(weeklyAd.getAdPages() != null && weeklyAd.getAdPages().size() > 0){
			int totalPages = weeklyAd.getAdPages().lastKey();
			if(totalPages % 2 == 0)
				weeklyAd.setTotalPages(totalPages);
			else
				weeklyAd.setTotalPages(totalPages + 1);
		}else{
			weeklyAd.setTotalPages(0);
		}
	}
	
//	private void copyPromotionDetails(PromoDefinition sourcePromo, PromoDefinition destPromo, RawWeeklyAd rawWeeklyAd, String storeLocation){
//		destPromo.setPromoDefnId(sourcePromo.getPromoDefnId());
//		destPromo.setStartCalId(sourcePromo.getStartCalId());
//		destPromo.setEndCalId(sourcePromo.getEndCalId());
//		destPromo.setPromoName(sourcePromo.getPromoName());
//		destPromo.setPromoNumber(sourcePromo.getPromoNumber());
//		destPromo.setTotalItems(sourcePromo.getTotalItems());
//		destPromo.setWeekStartDate(sourcePromo.getWeekStartDate());
//		destPromo.setPromoName(sourcePromo.getPromoName());
//		destPromo.setCreatedBy(sourcePromo.getCreatedBy());
//		destPromo.setApprovedBy(sourcePromo.getApprovedBy());
//		
//		if(storeLocation.equals(STORE_LOCATION_TOPS)) {
//			destPromo.setAdjustedUnits(rawWeeklyAd.getTopsAdjustedUnits());
//		} else {
//			destPromo.setAdjustedUnits(rawWeeklyAd.getGuAdjustedUnits());
//		} 
//	}
	
	private WeeklyAdPage addPage(RawWeeklyAd ad, WeeklyAd weeklyAd, String storeLocation){
		WeeklyAdPage adPage = null;
		int page = 0;
		if(storeLocation.equals(STORE_LOCATION_TOPS)) {
			page = ad.getTopsPage();
		} else if (storeLocation.equals(STORE_LOCATION_GU)) {
			page = ad.getGuPage();
		}
		if(weeklyAd.getAdPages().get(page) != null)
			adPage = weeklyAd.getAdPages().get(page); 
		else{
			adPage = new WeeklyAdPage();
			adPage.setPageNumber(page);
		}
		return adPage;
	}
	
	private WeeklyAdBlock addBlock(RawWeeklyAd ad, WeeklyAdPage adPage, String storeLocation){
		WeeklyAdBlock adBlock = null;
		int block = 0;
		if(storeLocation.equals(STORE_LOCATION_TOPS)) {
			block = ad.getTopsBlock();
		} else if (storeLocation.equals(STORE_LOCATION_GU)) {
			block = ad.getGuBlock();
		}
		if(adPage.getAdBlocks().get(block) != null)
			adBlock = adPage.getAdBlocks().get(block);
		else{
			adBlock = new WeeklyAdBlock();
			adBlock.setBlockNumber(block);
		}
		return adBlock;
	}
	
	private List<String> getItemsToBeChecked(String retailerItemCode, String itemDesc, Set<Integer> itemSet, List<String> ignoredItems){
		List<String> itemsToBeChecked = new ArrayList<String>();
		ItemDTO itemIN = new ItemDTO();
		itemIN.setUpc(retailerItemCode);
		itemIN.setRetailerItemCode(retailerItemCode);
		itemIN.setItemName(itemDesc.trim());
		//ItemDTO item = null;
		List<ItemDTO> itemList = new ArrayList<ItemDTO>();
		try{
			itemList = itemDAO.getItemDetailsFuzzyLookupV2(_Conn,itemIN);
		}catch(GeneralException ge){
			logger.error("Error when retrieving item code - " + ge);
		}
		
		if(itemList.size() == 0){
			//If item not found through RETAILER_ITEM_CODE_MAP and ITEM_LOOKUP check in retailer item code cache to get the item code directly.
			if(itemCodeMap.get(retailerItemCode) != null){
				itemsToBeChecked = itemCodeMap.get(retailerItemCode);
			}
			else{
				for(String key: itemCodeMap.keySet())
					//Sometimes the retailer item code is padded with '0'. Sometimes not. To handle this,
					//Convert to whole number at both Cache and input retailer item code. if both matches get the item code using actual key.
					if(!retailerItemCode.equals("") && retailerItemCode != null && !key.equals("null") && retailerItemCode.length() < 8 && key.length() < 8){
						String tempkey = String.valueOf(Integer.parseInt(key));
						String tempretItemCode = String.valueOf(Integer.parseInt(retailerItemCode));
						if(tempkey.equals(tempretItemCode)){
							itemsToBeChecked = itemCodeMap.get(key); 
							break;
						}
					}
				}
		}/*else if(itemSet.contains(item.getItemCode())){
			ignoredItems.add("Multiple entries found for " + retailerItemCode + ", " + itemDesc);
		}*/else{
			//itemSet.add(item.getItemCode());
			ArrayList<Integer> groupItems = null;
			try{
				for(ItemDTO item : itemList){
					if(item.getLikeItemId() > 0){
						groupItems = itemDAO.getItemsInGroup(_Conn, item.getLikeItemId());
						for(int itemCode: groupItems){
							itemsToBeChecked.add(String.valueOf(itemCode));
						}
					}
					else{
						itemsToBeChecked.add(String.valueOf(item.getItemCode()));
					}
				}
			}catch(GeneralException ge){
				logger.error("Error when retrieving items in group - " + ge);
			}
		}
		return itemsToBeChecked;
	}
	
//	private void setLocationLevelForPromotion(String storeLocation,	PromoDefinition promotion) {
//		PromoLocation location = new PromoLocation();
//		if(STORE_LOCATION_TOPS.equalsIgnoreCase(storeLocation)){
//			location.setLocationLevelId(Constants.STORE_LIST_LEVEL_ID);
//			if(locationIdMap.get("TOPS NO GU") != null){
//				location.setLocationId(locationIdMap.get("TOPS NO GU"));
//			}else{
//				int locationId = locationGroupDAO.getLocationId("TOPS NO GU");
//				location.setLocationId(locationId);
//				locationIdMap.put("TOPS NO GU", locationId);
//			}
//		}else if(STORE_LOCATION_GU.equalsIgnoreCase(storeLocation)){
//			location.setLocationLevelId(Constants.STORE_LIST_LEVEL_ID);
//			if(locationIdMap.get("GRAND UNION STORES") != null){
//				location.setLocationId(locationIdMap.get("GRAND UNION STORES"));
//			}else{
//				int locationId = locationGroupDAO.getLocationId("GRAND UNION STORES");
//				location.setLocationId(locationId);
//				locationIdMap.put("GRAND UNION STORES", locationId);
//			}
//		}else{
//			location.setLocationLevelId(Constants.STORE_LIST_LEVEL_ID);
//			if(locationIdMap.get("TOPS CORP") != null){
//				location.setLocationId(locationIdMap.get("TOPS CORP"));
//			}else{
//				int locationId = locationGroupDAO.getLocationId("TOPS CORP");
//				location.setLocationId(locationId);
//				locationIdMap.put("TOPS CORP", locationId);
//			}
//		}
//		promotion.addPromoLocation(location);
//	}

//	private int decodeDisplayType(HashMap<String, Integer> displayTypeMap, String displayType) {
//		int displayTypeId = 0; 
//		displayType = displayType.toLowerCase();
//		
//		if(displayType.toLowerCase().equals("fw") || displayType.toLowerCase().contains("fast")){
//			return displayTypeMap.get(Constants.DISPLAY_TYPE_FASTWALL);
//		}else if(displayType.toLowerCase().contains("jump")){
//			return displayTypeMap.get(Constants.DISPLAY_TYPE_JUMPSHELF);
//		}else if(displayType.toLowerCase().contains("bread")){
//			return displayTypeMap.get(Constants.DISPLAY_TYPE_BREADTABLE);
//		}else if(displayType.toLowerCase().contains("shipper")){
//			return displayTypeMap.get(Constants.DISPLAY_TYPE_SHIPPER);
//		}else if(displayType.toLowerCase().contains("combo end")){
//			return displayTypeMap.get(Constants.DISPLAY_TYPE_COMBOEND);
//		}else if(displayType.toLowerCase().contains("wing")){
//			return displayTypeMap.get(Constants.DISPLAY_TYPE_WING);
//		}else if(displayType.toLowerCase().contains("mod")){
//			return displayTypeMap.get(Constants.DISPLAY_TYPE_MOD);
//		}else if(displayType.toLowerCase().contains("lobby")){
//			return displayTypeMap.get(Constants.DISPLAY_TYPE_LOBBY);
//		}else if(displayType.toLowerCase().contains("enddsd")){
//			return displayTypeMap.get(Constants.DISPLAY_TYPE_END_DSD);
//		}else if(displayType.toLowerCase().contains("end")){
//			return displayTypeMap.get(Constants.DISPLAY_TYPE_END);
//		}else{
//			return displayTypeId;
//		}
//	}

//	private void setSalePrice(RawWeeklyAd ad, PromoBuyItems promoBuyItem) {
//		String adRetail = ad.getAdRetail();
//		if(!adRetail.equalsIgnoreCase("BOGO")){
//			if(adRetail.indexOf("/") != -1){
//				adRetail = adRetail.replaceAll("\\$", "");
//				String data[] = adRetail.split("/");
//				if(ad.getTpr() != null && String.valueOf(Constants.YES).equals(ad.getTpr())){
//					promoBuyItem.setTprQty(Integer.parseInt(data[0].trim()));
//					promoBuyItem.setTprMPrice(Double.parseDouble(data[1].trim()));
//				}else{
//					promoBuyItem.setSaleQty(Integer.parseInt(data[0].trim()));
//					promoBuyItem.setSaleMPrice(Double.parseDouble(data[1].trim()));
//				}
//			}else{
//				if(ad.getTpr() != null && String.valueOf(Constants.YES).equals(ad.getTpr())){
//					promoBuyItem.setTprQty(1);
//					promoBuyItem.setTprPrice(ad.getUnitPrice());
//				}else{
//					promoBuyItem.setSaleQty(1);
//					promoBuyItem.setSalePrice(ad.getUnitPrice());
//				}
//			}
//		}else{
//			if(ad.getTpr() != null && String.valueOf(Constants.YES).equals(ad.getTpr())){
//				promoBuyItem.setTprQty(2);
//				promoBuyItem.setTprMPrice(ad.getUnitPrice());
//			}else{
//				promoBuyItem.setSaleQty(2);
//				promoBuyItem.setSaleMPrice(ad.getUnitPrice());
//			}
//		}
//	}

	private void setCalendarId(String fileName){
		int index = fileName.lastIndexOf("/");
		String xlFileName = fileName.substring(index + 1, fileName.length());
		String strDate = xlFileName.substring(0, 8).replace("_", "/");
		try{
			setAdName(strDate + "_Ad_Data");
			SimpleDateFormat sf = new SimpleDateFormat("MM/dd/yy");
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
		}catch(ParseException p){
			logger.error("Error when parsing date from filename - " + p);
		}catch(GeneralException g){
			logger.error("Error when retrieving calendar id for the week - " + g);
		}
	}

	private void setLocationId(Connection conn){
		try{
			RetailPriceDAO rpDao = new RetailPriceDAO();
			String chainId = rpDao.getChainId(conn);
			setLocationId(Integer.parseInt(chainId));
		}catch(GeneralException ge){
			logger.error("Error when setting location level id - " + ge);
		}
	}
	
	private boolean isIgnorePromo(String itemDesc, String adRetail){
		// Ignore must buy promotions
		if(itemDesc.toLowerCase().indexOf("must buy") != -1)
			return true;
		// Ignore BXGY promotions
		else if(!adRetail.equalsIgnoreCase("BOGO") && (adRetail.startsWith("B") || adRetail.endsWith("b")))
			return true;
		else
			return false;
	}
	
//	private int getPromoTypeId(String adRetail){
//		if(adRetail.equalsIgnoreCase("BOGO"))
//			return 1;
//		else
//			return 9;
//	}
	
	@SuppressWarnings("unused")
	private void mapFieldNames_16092015(TreeMap<Integer, String> fieldNames){
		fieldNames.put(0, "topsPage");
		fieldNames.put(1, "topsBlock");
		fieldNames.put(3, "itemCode");
		fieldNames.put(4, "itemDesc");
		fieldNames.put(5, "storeLocation");
		fieldNames.put(7, "adLocation");
		fieldNames.put(8, "casePack");
		fieldNames.put(10, "listCost");
		fieldNames.put(13, "tpr");
		fieldNames.put(14, "offInvoiceCost");
		fieldNames.put(15, "billbackCost");
		fieldNames.put(16, "scanCost");
		fieldNames.put(17, "dealCost");
		fieldNames.put(18, "unitPrice");
		fieldNames.put(19, "adRetail");
		fieldNames.put(33, "displayType");
		fieldNames.put(30, "topsAdjustedUnits");
		fieldNames.put(42, "guPage");
		fieldNames.put(43, "guBlock");
		fieldNames.put(58, "guAdjustedUnits");
	}
	
//	private void mapFieldNames(TreeMap<Integer, String> fieldNames){
//		//Positions Changed on 16th Sept 2015
//		fieldNames.put(0, "topsPage");
//		fieldNames.put(1, "topsBlock");
//		fieldNames.put(3, "itemCode");
//		fieldNames.put(4, "itemDesc");
//		fieldNames.put(5, "storeLocation");
//		fieldNames.put(8, "adLocation");
//		fieldNames.put(9, "casePack");
//		fieldNames.put(11, "listCost");
//		fieldNames.put(14, "tpr");
//		fieldNames.put(15, "offInvoiceCost");
//		fieldNames.put(16, "billbackCost");
//		fieldNames.put(17, "scanCost");
//		fieldNames.put(18, "dealCost");
//		fieldNames.put(19, "unitPrice");
//		fieldNames.put(20, "adRetail");
//		fieldNames.put(31, "topsAdjustedUnits");
//		fieldNames.put(34, "displayType");
//		fieldNames.put(42, "guPage");
//		fieldNames.put(43, "guBlock");
//		fieldNames.put(59, "guAdjustedUnits");
//	}
	
	private void mapFieldNames(TreeMap<Integer, String> fieldNames) throws ParseException {
		DateFormat formatterMMddyy = new SimpleDateFormat("MM/dd/yy");
		Date processingWeekDate = formatterMMddyy.parse(getWeekStartDate());
		Date tempWeekDate1 = formatterMMddyy.parse("07/25/15");
		
		fieldNames.put(0, "topsPage");
		fieldNames.put(1, "topsBlock");
		fieldNames.put(3, "itemCode");
		fieldNames.put(4, "itemDesc");
		fieldNames.put(5, "storeLocation");
		fieldNames.put(8, "adLocation");
		fieldNames.put(9, "casePack");
		fieldNames.put(11, "listCost");
		fieldNames.put(14, "tpr");
		fieldNames.put(15, "offInvoiceCost");
		fieldNames.put(16, "billbackCost");
		fieldNames.put(17, "scanCost");
		fieldNames.put(18, "dealCost");
		fieldNames.put(19, "unitPrice");
		fieldNames.put(20, "adRetail");
		
		//// Positions Changed. if week start date <= 07/25/15
		if(processingWeekDate.before(tempWeekDate1) || processingWeekDate.equals(tempWeekDate1)) {
			fieldNames.put(30, "topsAdjustedUnits");
			fieldNames.put(33, "displayType");
			fieldNames.put(41, "guPage");
			fieldNames.put(42, "guBlock");
			fieldNames.put(58, "guAdjustedUnits");
		} else {
			fieldNames.put(31, "topsAdjustedUnits");
			fieldNames.put(34, "displayType");
			fieldNames.put(42, "guPage");
			fieldNames.put(43, "guBlock");
			fieldNames.put(59, "guAdjustedUnits");
		}
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

	@Override
	public void processRecords(List<Object> listobj) throws GeneralException {
		// TODO Auto-generated method stub
		
	}
}
