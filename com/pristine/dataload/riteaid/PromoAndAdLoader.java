package com.pristine.dataload.riteaid;


/*
 * 
 * Author : Pradeep
 * Class : LoadOverlaysV2
 * Input : Promo input feed.
 * Compatible only for RiteAid
 *  
 */


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
import java.util.concurrent.TimeUnit;
import java.util.regex.Pattern;
import java.util.stream.Collectors;

import org.apache.log4j.Logger;
import org.apache.log4j.PropertyConfigurator;
import com.pristine.dao.DBManager;
import com.pristine.dao.ItemDAO;
import com.pristine.dao.RetailCalendarDAO;
import com.pristine.dao.RetailCostDAO;
import com.pristine.dao.RetailPriceDAO;
import com.pristine.dao.offermgmt.promotion.PromotionDAO;
import com.pristine.dao.offermgmt.weeklyad.WeeklyAdDAO;
import com.pristine.dataload.service.PromotionService;
import com.pristine.dto.ItemDTO;
import com.pristine.dto.RetailCalendarDTO;
import com.pristine.dto.RetailCostDTO;
import com.pristine.dto.offermgmt.promotion.PromoBuyItems;
import com.pristine.dto.offermgmt.promotion.PromoBuyRequirement;
import com.pristine.dto.offermgmt.promotion.PromoDefinition;
import com.pristine.dto.offermgmt.promotion.PromoLocation;
import com.pristine.dto.offermgmt.promotion.PromoOfferDetail;
import com.pristine.dto.offermgmt.weeklyad.WeeklyAd;
import com.pristine.dto.offermgmt.weeklyad.WeeklyAdBlock;
import com.pristine.dto.offermgmt.weeklyad.WeeklyAdPage;
import com.pristine.dto.riteaid.RAPromoFileDTO;
import com.pristine.exception.GeneralException;
import com.pristine.parsinginterface.ExcelFileParserV2;
import com.pristine.util.Constants;
import com.pristine.util.DateUtil;
import com.pristine.util.PrestoUtil;
import com.pristine.util.PristineDBUtil;
import com.pristine.util.PropertyManager;

@SuppressWarnings({ "unchecked", "rawtypes" })
public class PromoAndAdLoader {
	private static Logger  logger = Logger.getLogger("PromoAndAdLoader");
	private Connection 	_Conn = null;
	
	private String rootPath = null;
	
	private PromotionDAO promoDAO = null;
	private ExcelFileParserV2 parser = null;
	private RetailPriceDAO priceDAO = null;
	private WeeklyAdDAO adDAO = null;
	int chainId = 0;
	DateFormat appDateFormatter = new SimpleDateFormat(Constants.APP_DATE_FORMAT);
	HashMap<String, List<String>> orignalItemCodeMap = null;
	HashMap<String, List<String>> currentItemCodeMap = null;
	Set<String> itemCodeNotFound = null;
	Set<String> notProcessedPromotions = null;
	Set<String> notProcessedCouponDesc = null;
	HashMap<String, Integer> weekCalendarMap = new HashMap<String, Integer>();
	HashMap<String, Integer> calendarMap = new HashMap<String, Integer>();
	HashMap<Pattern, Integer> compiledPatterns = new HashMap<>();
	RetailCalendarDAO calDAO = new RetailCalendarDAO();
	RetailCostDAO rcDAO = new RetailCostDAO();
	Date processingWeek = null;
	PromotionService promotionService = new PromotionService();
	boolean processOldFiles;
	private static final String PROCESS_OLD_FORMAT = "PROCESS_OLD_FORMAT=";
	/**
	 * Constructor
	 */
	public PromoAndAdLoader (boolean connectDb)
	{
		rootPath = PropertyManager.getProperty("DATALOAD.ROOTPATH", "");
        try
		{
        	_Conn = DBManager.getConnection();   
	    }catch (GeneralException gex) {
	        logger.error("Error when creating connection - " + gex);
	    }
        
        promoDAO = new PromotionDAO(_Conn);
        parser = new ExcelFileParserV2(); 
        priceDAO = new RetailPriceDAO();
        adDAO = new WeeklyAdDAO(_Conn);
        parser.setFirstRowToProcess(1);
	}
	
	public PromoAndAdLoader(){
		
	}
	
	/**
	 * Main method
	 * @param args[0] inputFilePath
	 * @throws Exception
	 */
	public static void main(String[] args) throws Exception 
	{
		PropertyConfigurator.configure("log4j-promo-ad-loader.properties");
		PropertyManager.initialize("analysis.properties");
        
		String relativePath = null;
		if(args.length > 0){
			relativePath = args[0];
		}else{
			logger.error("Invalid input - Input File Path is mandatory");
		}
		
		PromoAndAdLoader overlays = new PromoAndAdLoader(true);
		
		
		for(String arg: args){
			if(arg.startsWith(PROCESS_OLD_FORMAT)){
				overlays.processOldFiles = Boolean.parseBoolean(arg.substring(PROCESS_OLD_FORMAT.length()));
			}
		}
		
		overlays.process(relativePath);
	}
	
	
	
	/**
	 * 
	 * @param relativePath
	 */
	private void process(String relativePath) {
		try {

			initialize();

			processPromotionFiles(relativePath);

		} catch (GeneralException | Exception e) {
			PristineDBUtil.rollbackTransaction(_Conn, "Promo loader exception");
			logger.error("process() - Error while processing promotion files", e);
		} finally {
			PristineDBUtil.close(_Conn);
		}
	}
	
	/**
	 * 
	 * @throws GeneralException
	 */
	private void initialize() throws GeneralException{
		//Retrieve all items
		try{
			
			orignalItemCodeMap = new HashMap<>();
			currentItemCodeMap = new HashMap<>();
			itemCodeNotFound = new HashSet<>();
			notProcessedPromotions = new HashSet<>();
			notProcessedCouponDesc = new HashSet<>();
			long startTime  = System.currentTimeMillis();
			ItemDAO itemDAO = new ItemDAO();
			List<ItemDTO> allItems = itemDAO.getItemsByOrigAndCurrItemNumber(_Conn);
			long endTime = System.currentTimeMillis();
			
			//For original item number cache
			fillItemCodeMaps(true, allItems);
			//For current item number cache
			fillItemCodeMaps(false, allItems);
			
			logger.info("initializeCache() - Time taken to cache all items - " + ((endTime - startTime)/1000) + " seconds");
			
			chainId = Integer.parseInt(priceDAO.getChainId(_Conn));
			
			
			compilePatterns();
			
		}catch(GeneralException | Exception e){
			throw new GeneralException("initializeCache() - Error while initializing cache", e);
		}
	}
	
	private void fillItemCodeMaps(boolean isOrginalItemMap, List<ItemDTO> allItems){
		for(ItemDTO itemDTO: allItems){
			List<String> itemCodes = new ArrayList<>();
			if(isOrginalItemMap){
				if(orignalItemCodeMap.containsKey(itemDTO.getAltRetailerItemCode())){
					itemCodes = orignalItemCodeMap.get(itemDTO.getAltRetailerItemCode());
				}
				itemCodes.add(String.valueOf(itemDTO.getItemCode()));
				orignalItemCodeMap.put(itemDTO.getAltRetailerItemCode(), itemCodes);
			}else{
				if(currentItemCodeMap.containsKey(itemDTO.getRetailerItemCode())){
					itemCodes = currentItemCodeMap.get(itemDTO.getRetailerItemCode());
				}
				itemCodes.add(String.valueOf(itemDTO.getItemCode()));
				currentItemCodeMap.put(itemDTO.getRetailerItemCode(), itemCodes);
			}
		}
	}
	
	/**
	 * creates the patterns for coupon descriptions
	 */
	public HashMap<Pattern, Integer> compilePatterns(){
		//Get $2.00/Get $2
		Pattern pattern1 = Pattern.compile("([G]{1}[E]{1}[T]{1})\\s([$]{1}\\d+\\.?\\d*)");
		compiledPatterns.put(pattern1, 2);
		//Earn $2.00/Earn $2
		Pattern pattern2 = Pattern.compile("([E]{1}[A]{1}[R]{1}[N]{1})\\s([$]{1}\\d+\\.?\\d*)");
		compiledPatterns.put(pattern2, 2);
		//$2 With/$2.00 With
		Pattern pattern3 = Pattern.compile("([$]{1}\\d+\\.?\\d*)\\s([W]{1}[I]{1}[T]{1}[H]{1})");
		compiledPatterns.put(pattern3, 1);
		//$2 Bonus/$2.00 Bonus
		Pattern pattern4 = Pattern.compile("([$]{1}\\d+\\.?\\d*)\\s([B]{1}[O]{1}[N]{1}[U]{1}[S]{1})");
		compiledPatterns.put(pattern4, 1);
		//$2 BC/$2.00 BC
		Pattern pattern5 = Pattern.compile("([$]{1}\\d+\\.?\\d*)\\s([B]{1}[C]{1})");
		compiledPatterns.put(pattern5, 1);
		return compiledPatterns;
	}
	
	/**
	 * Loads promotion data
	 * @param relativePath
	 * @throws GeneralException 
	 */
	public void processPromotionFiles(String relativePath) throws GeneralException{
		String archivePath = rootPath + "/" + relativePath + "/";
		String fName = null;
		ArrayList<String> fileList = parser.getFiles(relativePath);
		TreeMap<Integer, String> fieldNames = new TreeMap<Integer, String>();
		if(processOldFiles){
			mapFieldNamesOld(fieldNames);
		}else{
			mapFieldNames(fieldNames);	
		}
		
		try{
			for (String fileName : fileList) {
				logger.info("processPromotionFiles() - Processing file: " + fileName);
				fName = fileName;
				
				String[] fileExtArr = fileName.split("\\.");
				String fileExtension = fileExtArr[fileExtArr.length - 1];
				
				List<RAPromoFileDTO> promoData = null;
				if(fileExtension.equalsIgnoreCase("XLS")){
					promoData = parser.parseExcelFile(RAPromoFileDTO.class, fileName, 0, fieldNames);
				}else if(fileExtension.equalsIgnoreCase("CSV") || fileExtension.equalsIgnoreCase("TXT")){
					promoData = parser.parseCSVFile(RAPromoFileDTO.class, fileName, fieldNames, ',');
				}
				
				logger.info("processPromotionFiles() - Total # of records to be processed: " + promoData.size());
				
				processPromoRecords(promoData);
				
				PrestoUtil.moveFile(fileName, archivePath + Constants.COMPLETED_FOLDER);
				PristineDBUtil.commitTransaction(_Conn, "Ad data load Commit");
				logger.info("processPromotionFiles() - End of Processing file: " + fileName);
			}
		}catch(GeneralException | Exception ge){
			PrestoUtil.moveFile(fName, archivePath + Constants.BAD_FOLDER);
			throw new GeneralException("Exception while processing file - " + fName, ge);
		}
	}

	
	/**
	 * 
	 * @param promoList
	 * @throws GeneralException 
	 * @throws Exception 
	 */
	private void processPromoRecords(List<RAPromoFileDTO> promoList) 
			throws GeneralException, Exception{
		
		//Group Weekly Ad promotions and create weekly ad objects
		List<WeeklyAd> weeklyAds = groupAndCreateWeeklyAdObjects(promoList);
		
		logger.info("Ad size:" + weeklyAds.size());
		
		//Get processing week. Min weekly ad can be processing week
		processingWeek = weeklyAds.stream().map(u -> u.getStartDate()).min(Date::compareTo).get();
		
		logger.info("processPromoRecords() - processing week: "
				+ DateUtil.dateToString(processingWeek, Constants.APP_DATE_FORMAT));
		
		//Group non weekly Ad promotions
		List<PromoDefinition> nonAdPromotions = groupAndCreateNonAdPromoObjects(promoList);
		
		//Check overlapping of ad and non ad promotions
		handleNonAdAndAdPromotions(weeklyAds, nonAdPromotions);
		
		//Save promotions and ad
		saveAdAndPromotions(weeklyAds, nonAdPromotions);
		
		//Log Error records
		logNotProcessed();
	}
	
	/**
	 * 
	 * @param weeklyAds
	 * @throws GeneralException
	 */
	private void saveAdAndPromotions(List<WeeklyAd> weeklyAds, 
			List<PromoDefinition> nonAdPromotions) throws GeneralException {
		// Save Weekly Ad and promotions
		List<PromoDefinition> finalPromotions = new ArrayList<>();
		
		nonAdPromotions.stream().filter(promo -> promo.isCanBeAdded()).forEach(
				promotion ->{
			finalPromotions.add(promotion);
		});
		
		for (WeeklyAd weeklyAd : weeklyAds) {
			for (Map.Entry<Integer, WeeklyAdPage> pageEntry : weeklyAd.getAdPages().entrySet()) {
				for (Map.Entry<Integer, WeeklyAdBlock> blockEntry : pageEntry.getValue().getAdBlocks().entrySet()) {
					blockEntry.getValue().getPromotions().stream().filter(promo -> promo.isCanBeAdded()).forEach(promotion -> {
						finalPromotions.add(promotion);
					});
				}
			}
		}
		
		logger.info("processPromoRecords() - Total # of promotions: " + finalPromotions.size());
		long startTime = System.currentTimeMillis();
		
		promoDAO.savePromotionV2(finalPromotions, processingWeek, false);
		
		long endTime = System.currentTimeMillis();
		logger.info("processPromoRecords() - Time taken to save promotions: " + (endTime - startTime) + " ms.");
		
		
		logger.info("processPromoRecords() - Total # of Ads to be processed: " + weeklyAds.size());
		startTime = System.currentTimeMillis();
		
		for (WeeklyAd weeklyAd : weeklyAds) {
			adDAO.saveWeeklyAd(weeklyAd);
		}
		
		endTime = System.currentTimeMillis();
		logger.info("processPromoRecords() - Time taken to save Weekly Ads: " + (endTime - startTime) + " ms.");
	}
	
	/**
	 * log skipped items
	 */
	private void logNotProcessed(){
		StringBuilder sb = new StringBuilder();
		for(String itemNotFound: itemCodeNotFound){
			sb.append(itemNotFound + "\n");
		}
		logger.info("***Item code not found records***");
		logger.info(sb.toString());
		
		sb = new StringBuilder();
		for(String notProcessed: notProcessedPromotions){
			sb.append(notProcessed + "\n");
		}
		logger.info("***Not Processed Promo Records***");
		logger.info(sb.toString());
		
		
		sb = new StringBuilder();
		for(String notProcessed: notProcessedCouponDesc){
			sb.append(notProcessed + "\n");
		}
		logger.info("***Not Processed Coupon Descriptions***");
		logger.info(sb.toString());
	}
	
	/**
	 * 
	 * @param promoList
	 * @return list of weekly ad objects
	 * @throws GeneralException 
	 */
	private List<WeeklyAd> groupAndCreateWeeklyAdObjects(List<RAPromoFileDTO> promoList) throws GeneralException{
		List<WeeklyAd> weeklyAdList = new ArrayList<>();
		
		HashMap<WeeklyAdKey, List<RAPromoFileDTO>> weeklyAdMap = new HashMap<>();
		
		//Filter and find weekly ad promotions
		promoList.stream().filter(promo -> promo.getAdPage() > 0 && promo.getBlockNum() > 0 ).forEach(item -> {
			
			WeeklyAdKey weeklyAdKey = new WeeklyAdKey(item.getPromoStartDate(), item.getPromoEndDate());
			
			List<RAPromoFileDTO> tempList = new ArrayList<>();
			tempList.add(item);
			
			if(weeklyAdMap.containsKey(weeklyAdKey)){
				tempList.addAll(weeklyAdMap.get(weeklyAdKey));
			}
			
			weeklyAdMap.put(weeklyAdKey, tempList);
			
		});
		
		for(Map.Entry<WeeklyAdKey, List<RAPromoFileDTO>> weeklyAdEntry: weeklyAdMap.entrySet()){
			WeeklyAd weeklyAd = new WeeklyAd();
			weeklyAd.setApprovedBy(Constants.BATCH_USER);
			weeklyAd.setCreatedBy(Constants.BATCH_USER);
			weeklyAd.setLocationLevelId(Constants.CHAIN_LEVEL_ID);
			weeklyAd.setLocationId(chainId);
			weeklyAd.setStatus(3);
			RAPromoFileDTO sampleObj = weeklyAdEntry.getValue().get(0);
			weeklyAd.setCalendarId(setCalIdforAd(sampleObj.getPromoStartDate()));
			weeklyAd.setWeekStartDate(sampleObj.getPromoStartDate());
			weeklyAd.setStartDate(DateUtil.toDate(sampleObj.getPromoStartDate()));
			weeklyAd.setAdName(sampleObj.getEvtId_Desc());
			weeklyAd.setAdNumber(sampleObj.getEvtId_AdGroup());
			HashMap<String, PromoDefinition> promoGroups = getPromotionGroups(weeklyAdEntry.getValue(), true);
			
			setupWeeklyAdAndPromotions(promoGroups, weeklyAd);
			
			weeklyAdList.add(weeklyAd);
		}
		return weeklyAdList;
	}
	
	/**
	 * 	
	 * @param promoGroups
	 * @param weeklyAd
	 */
	private void setupWeeklyAdAndPromotions(HashMap<String, PromoDefinition> promoGroups, WeeklyAd weeklyAd){
		
		
		HashMap<String, HashMap<String, List<PromoDefinition>>>  pageBlockMap = new HashMap<>();
		
		
		for(Map.Entry<String, PromoDefinition> promoEntry: promoGroups.entrySet()){
			PromoDefinition promoDefinition = promoEntry.getValue();
			
			HashMap<String, List<PromoDefinition>> blockMap = null;
			
			if(pageBlockMap.containsKey(promoDefinition.getAdpage())){
				blockMap = pageBlockMap.get(promoDefinition.getAdpage());
			}else{
				blockMap = new HashMap<>();
			}
			
			pageBlockMap.put(promoDefinition.getAdpage(), blockMap);
			
			if(blockMap.containsKey(promoDefinition.getBlockNum())){
				List<PromoDefinition> promoList = blockMap.get(promoDefinition.getBlockNum());
				promoList.add(promoDefinition);
				blockMap.put(promoDefinition.getBlockNum(), promoList);
			}else{
				List<PromoDefinition> promoList = new ArrayList<>();
				promoList.add(promoDefinition);
				blockMap.put(promoDefinition.getBlockNum(), promoList);
			}
		}
		
		logger.info("setupWeeklyAdAndPromotions() - # of pages for Ad " + weeklyAd.getWeekStartDate() + ": " +  pageBlockMap.size());
		//Set total pages
		weeklyAd.setTotalPages(pageBlockMap.size());
		
		for(Map.Entry<String, HashMap<String, List<PromoDefinition>>> pageBlockEntry: pageBlockMap.entrySet()){
			//Create new page
			WeeklyAdPage weeklyAdPage = new WeeklyAdPage();
			weeklyAdPage.setPageNumber(Integer.parseInt(pageBlockEntry.getKey()));
			weeklyAdPage.setTotalBlocks(pageBlockEntry.getValue().size());
			weeklyAdPage.setStatus(3);
			
			for(Map.Entry<String, List<PromoDefinition>> blockEntry: pageBlockEntry.getValue().entrySet()){
				//Create new block
				WeeklyAdBlock weeklyAdBlock =  new WeeklyAdBlock();
				
				weeklyAdBlock.setBlockNumber(Integer.parseInt(blockEntry.getKey()));
				weeklyAdBlock.setStatus(3);
				
				//Set promotions
				ArrayList<PromoDefinition> promotions = new ArrayList<>(blockEntry.getValue());
				weeklyAdBlock.setPromotions(promotions);
				
				//Attach block to page
				weeklyAdPage.addBlock(weeklyAdBlock);
			}
			
			//Attach page to weekly Ad
			weeklyAd.addPage(weeklyAdPage);
		}
	}
	
	
	/**
	 * 
	 * @param promoFileDTOList
	 * @param isInAd
	 * @return collection of promotions
	 * @throws GeneralException
	 */
	private HashMap<String, PromoDefinition> getPromotionGroups(List<RAPromoFileDTO> promoFileDTOList, 
			boolean isInAd) throws GeneralException {
		HashMap<String, PromoDefinition> promoGroups = new HashMap<>();
		try {
			for (RAPromoFileDTO promoFileDTO : promoFileDTOList) {

				PromoDefinition promoDefinition = null;

				int promoTypeId = promotionService.getPromotioType(promoFileDTO, new PromoDefinition());
				
				// Key for promotion group
				String promoGroupKey = promoFileDTO.getGroupLookupId_PromoGroup() + "-" + promoFileDTO.getPromoStartDate() + "-"
						+ promoFileDTO.getPromoEndDate() + "-" + promoTypeId;

				// Get buy item object
				List<PromoBuyItems> promoBuyItems = setupPromoBuyItems(promoFileDTO, isInAd);
				if (promoBuyItems != null) {
					for (PromoBuyItems promoBuyItem : promoBuyItems) {
						// Group promotions
						if (promoGroups.containsKey(promoGroupKey)) {
							promoDefinition = promoGroups.get(promoGroupKey);
							List<PromoBuyItems> buyItems = promoDefinition.getBuyItems();
							// Get distinct items from collection
							Set<Integer> itemCodes = buyItems.stream().map(PromoBuyItems::getItemCode).collect(Collectors.toSet());

							// Check if the current item is alrady added into the list.
							// If current is not present, then add it into buy items to keep buy items unique
							// There will be multiple entries for same item in promo feed with different event ids
							// Since the ad and promo created at chain level, duplicates can be ignored
							if (!itemCodes.contains(promoBuyItem.getItemCode())) {
								buyItems.add(promoBuyItem);
							}
						} else {
							promoDefinition = convertFileObjToPromoDefinition(promoFileDTO);
							List<PromoBuyItems> buyItems = new ArrayList<>();
							buyItems.add(promoBuyItem);
							promoDefinition.setBuyItems(buyItems);
							setupBuyReqAndOfferDetails(promoDefinition);
						}
					}
					
					// Set long term promotion dates
					setActualCalendarInfoForLongTermPromo(promoDefinition, promoDefinition.getPromoStartDate(), promoDefinition.getPromoEndDate());
					promoGroups.put(promoGroupKey, promoDefinition);
				}
			}

			// Remove duplicated items in multiple groups
			handleItemsInMultiplePromoGroups(promoGroups);

		} catch (GeneralException | Exception e) {
			throw new GeneralException("getPromotionGroups() - Error while grouping promotions", e);
		}
		return promoGroups;
	}
	
	/**
	 * 
	 * @param promoFileDTO
	 * @return promotion defintion object
	 * @throws GeneralException
	 */
	private PromoDefinition convertFileObjToPromoDefinition(
			RAPromoFileDTO promoFileDTO) throws GeneralException {
		
		PromoDefinition promoDefinition = new PromoDefinition();
		
		//Set Promo start and end date
		promoDefinition.setPromoStartDate(DateUtil.toDate(promoFileDTO.getPromoStartDate()));
		promoDefinition.setPromoEndDate(DateUtil.toDate(promoFileDTO.getPromoEndDate()));
		
		//Set page and block numbers for referrence
		promoDefinition.setBlockNum(String.valueOf(promoFileDTO.getBlockNum()));
		promoDefinition.setAdpage(String.valueOf(promoFileDTO.getAdPage()));
		
		//Set promo number and name
		promoDefinition.setPromoNumber(promoFileDTO.getGroupLookupId_PromoGroup());
		if(promoFileDTO.getGroupDesc_PromoName() == Constants.EMPTY){
			promoDefinition.setPromoName(promoFileDTO.getGroupLookupId_PromoGroup());	
		}else{
			promoDefinition.setPromoName(promoFileDTO.getGroupDesc_PromoName());	
		}
		
		
		//Set coupon description and coupon amount
		promoDefinition.setCouponDesc(promoFileDTO.getCpnDesc());
		promoDefinition.setAmountOff(String.valueOf(promoFileDTO.getCpnAmount()));
		
		//Set buy qty and get qty
		promoDefinition.setBuyQuantity(promoFileDTO.getBuyQty());
		promoDefinition.setGetQuantity(promoFileDTO.getGetQty());
		
		//Set pct off and price code
		promoDefinition.setPctOff(promoFileDTO.getSalePrice());
		promoDefinition.setItemPriceCode(promoFileDTO.getItemPriceCode());
		
		//For must buy, set # of items required to get the deal
		promoDefinition.setNoOfItems(promoFileDTO.getSaleQty());
		promoDefinition.setRetailForPurchase(String.valueOf(promoFileDTO.getSalePrice()));
		
		promoDefinition.setApprovedBy(Constants.BATCH_USER);
		promoDefinition.setCreatedBy(Constants.BATCH_USER);
		
		promoDefinition.setThresholdValue(promoFileDTO.getThresholdValue());
		promoDefinition.setThresholdType(promoFileDTO.getThresholdType());
		//Set location
		setLocation(promoDefinition);

		//Set promotion type
		promotionService.identifyAndSetPromotionType(promoFileDTO, promoDefinition);
		
		if(!promoDefinition.isCanBeAdded()){
			notProcessedPromotions.add("Unknown promo type: " + promoDefinition.getPromoNumber());
		}
		
		//Set start and end calendar ids
		setCalendarId(promoDefinition);

		return promoDefinition;
	}
	
	
	
	/**
	 * 
	 * @param promoFileDTO
	 * @param isInAd
	 * @return promo buy item object
	 * @throws GeneralException
	 */
	private List<PromoBuyItems> setupPromoBuyItems(RAPromoFileDTO promoFileDTO, boolean isInAd)
			throws GeneralException {
		List<PromoBuyItems> itemList = new ArrayList<>();
		try{
			Set<String> allMatchingItems = new HashSet<>();
			if(orignalItemCodeMap.containsKey(promoFileDTO.getOrigItemNumber())){
				allMatchingItems.addAll(orignalItemCodeMap.get(promoFileDTO.getOrigItemNumber()));
			}
			if(currentItemCodeMap.containsKey(promoFileDTO.getItemNumber())){
				allMatchingItems.addAll(currentItemCodeMap.get(promoFileDTO.getItemNumber()));
			}
			
			if(allMatchingItems.size() == 0){
				itemCodeNotFound.add("Orig. Item Number: " + promoFileDTO.getOrigItemNumber() 
												+ "Curr. Item Number: " + promoFileDTO.getItemNumber());
				return null;
			}
			for(String itemCode: allMatchingItems){
				PromoBuyItems promoBuyItems = new PromoBuyItems();
				promoFileDTO.setUpc(PrestoUtil.castUPC(promoFileDTO.getUpc(), false));
				promoBuyItems.setUpc(promoFileDTO.getUpc());
				promoBuyItems.setRegPrice(promoFileDTO.getRegPrice());
				promoBuyItems.setRegQty(1);
				promoBuyItems.setItemCode(Integer.parseInt(itemCode));
				// set sale price based on item price code
				promotionService.setSalePrice(promoBuyItems, promoFileDTO);

				promoBuyItems.setBillbackCost(promoFileDTO.getScanBillBackAllowance());
				promoBuyItems.setOffInvoiceCost(promoFileDTO.getOffInvoiceAmt());

				int weekCalendarId = -1;
				if (weekCalendarMap.get(promoFileDTO.getPromoStartDate()) == null) {
					int startCalId = ((RetailCalendarDTO) calDAO.getCalendarId(_Conn, promoFileDTO.getPromoStartDate(),
							Constants.CALENDAR_WEEK)).getCalendarId();
					weekCalendarMap.put(promoFileDTO.getPromoStartDate(), startCalId);
					weekCalendarId = startCalId;
				} else
					weekCalendarId = weekCalendarMap.get(promoFileDTO.getPromoStartDate());

				// logger.debug("Week Calendar Id - " + weekCalendarId);
				if (weekCalendarId > 0) {
					RetailCostDTO retailCostDTO = rcDAO.getChainLevelCost(_Conn, weekCalendarId, promoBuyItems.getItemCode());
					if (retailCostDTO != null) {
						promoBuyItems.setListCost(retailCostDTO.getListCost());
						promoBuyItems.setDealCost(retailCostDTO.getDealCost());
					}
				}

				// Set ad flag
				promoBuyItems.setIsInAd(isInAd ? String.valueOf(Constants.YES) : String.valueOf(Constants.NO));
				
				itemList.add(promoBuyItems);
			}
			

		}catch(GeneralException | Exception e){
			throw new GeneralException("setupPromoBuyItemObj() - Error while setting up promo buy item", e);
		}
		
		return itemList;
	}

	
	
	
	/**
	 * 
	 * @param promoList
	 * @return non promo objects
	 * @throws GeneralException 
	 * @throws Exception 
	 */
	private List<PromoDefinition> groupAndCreateNonAdPromoObjects
				(List<RAPromoFileDTO> promoList) throws GeneralException, Exception{
		List<PromoDefinition> promoDefList = new ArrayList<>();
		
		HashMap<String, PromoDefinition> promoGroups = getPromotionGroups(promoList, false);
		
		for(Map.Entry<String, PromoDefinition> promoEntry: promoGroups.entrySet()){
			
			List<PromoDefinition> weeklyPromotions = splitPromoByWeek(promoEntry.getValue(), processingWeek);
			
			promoDefList.addAll(weeklyPromotions);
		}
		
		//Sort to ascending
		promoDefList.sort((p1, p2) -> p1.getPromoStartDate().compareTo(p2.getPromoStartDate()));
		
		return promoDefList;
	}
	
	
	/**
	 * 
	 * @param promotion
	 * @return list of weekly promotions derived from longer duration promotions
	 * @throws Exception
	 * @throws GeneralException 
	 */
	public List<PromoDefinition> splitPromoByWeek(PromoDefinition promotion, Date processingWeek) throws Exception, GeneralException {
		List<PromoDefinition> promoList = new ArrayList<>();

		Date promoStartDate = getFirstDateOfWeek(promotion.getPromoStartDate());
		Date promoEndDate = getLastDateOfWeek(promotion.getPromoEndDate());

		promotion.setPromoStartDate(promoStartDate);
		promotion.setPromoEndDate(promoEndDate);

		long diff = promotion.getPromoEndDate().getTime() - promotion.getPromoStartDate().getTime();

		long diffDates = TimeUnit.DAYS.convert(diff, TimeUnit.MILLISECONDS);

		if (diffDates > 7) {
			float dif = (float) diffDates / 7;
			int noOfWeeks = (int) Math.ceil(dif);

			for (int i = 0; i < noOfWeeks; i++) {
				PromoDefinition promoNew = (PromoDefinition) promotion.clone();
				promoNew.setPromoStartDate(getFirstDateOfWeek(promoStartDate));
				promoEndDate = DateUtil.incrementDate(promoStartDate, 6);
				promoStartDate = DateUtil.incrementDate(promoStartDate, 7);
				promoNew.setPromoEndDate(getLastDateOfWeek(promoEndDate));
				if (promoNew.getPromoStartDate().compareTo(processingWeek) >= 0) {
					setCalendarId(promoNew);
					boolean isValidCalendarId = setActualCalendarInfoForLongTermPromo(promotion, promotion.getPromoStartDate(), promotion.getPromoEndDate());
					if(isValidCalendarId && promoNew.getStartCalId()>0 && promoNew.getEndCalId()>0){
						promoList.add(promoNew);
					}
				}
			}
		} else {
			if (promotion.getPromoStartDate().compareTo(processingWeek) >= 0) {
				setCalendarId(promotion);
				boolean isValidCalendarId = setActualCalendarInfoForLongTermPromo(promotion, promoStartDate, promoEndDate);
				if(isValidCalendarId && promotion.getStartCalId()>0 && promotion.getEndCalId()>0){
					promoList.add(promotion);
				}
			}
		}

		return promoList;
	}
	
	/**
	 * Handles ad and non ad promotions overlapping
	 * @param weeklyAds
	 * @param nonAdPromotions
	 */
	private void handleNonAdAndAdPromotions(List<WeeklyAd> weeklyAds, List<PromoDefinition> nonAdPromotions){
		for(WeeklyAd ad : weeklyAds){
			for(Map.Entry<Integer, WeeklyAdPage> pageEntry: ad.getAdPages().entrySet()){
				for(Map.Entry<Integer, WeeklyAdBlock> blockEntry: pageEntry.getValue().getAdBlocks().entrySet()){
					for(PromoDefinition adPromotion: blockEntry.getValue().getPromotions()){
						Date adStartDate = adPromotion.getPromoStartDate();
						Date adEndDate = adPromotion.getPromoEndDate();
						for(PromoDefinition nonAdPromotion: nonAdPromotions){
							//get overlapping promotions and remove them from them the collection
							Date promoStartDate = nonAdPromotion.getPromoStartDate();
							Date promoEndDate = nonAdPromotion.getPromoEndDate();
							if(promoStartDate.compareTo(adStartDate) >= 0
									&& promoEndDate.compareTo(adEndDate) <= 0){
								adjustBuyItemsInMultipleGroups(adPromotion, nonAdPromotion);
							}
						}
					}
				}
			}
		}
	}
	
	/**
	 * Removes participating items in multiple groups
	 * @param promoGroup
	 */
	private void handleItemsInMultiplePromoGroups(HashMap<String, PromoDefinition> promoGroup){
		for(Map.Entry<String, PromoDefinition> promoEnrty1: promoGroup.entrySet()){
			Date promo1StartDate = promoEnrty1.getValue().getPromoStartDate();
			Date promo1EndDate = promoEnrty1.getValue().getPromoEndDate();
			for(Map.Entry<String, PromoDefinition> promoEnrty2: promoGroup.entrySet()){
				Date promoStartDate = promoEnrty2.getValue().getPromoStartDate();
				Date promoEndDate = promoEnrty2.getValue().getPromoEndDate();
				if(promoStartDate.compareTo(promo1StartDate) >= 0
						&& promoEndDate.compareTo(promo1EndDate) <= 0
						&& !promoEnrty1.getKey().equals(promoEnrty2.getKey())){
					adjustBuyItemsInMultipleGroups(promoEnrty1.getValue(), promoEnrty2.getValue());
				}
			}	
		}
	}
	
	/**
	 * Adjusts items in multiple groups
	 * @param promotion1
	 * @param promotion2
	 */
	private void adjustBuyItemsInMultipleGroups(PromoDefinition promotion1, PromoDefinition promotion2){
		boolean isAdjustmentRequired = false;
		for(PromoBuyItems buyItem1: promotion1.getBuyItems()){
			for(PromoBuyItems buyItem2: promotion2.getBuyItems()){
				if(buyItem2.getItemCode() == buyItem1.getItemCode()){
					isAdjustmentRequired = true;
					buyItem2.setItemOverlappingInAd(true);
				}
			}
		}
		
		if(isAdjustmentRequired){
			List<PromoBuyItems> newItemList = new ArrayList<>();
			promotion2.getBuyItems().stream().filter(item -> !item.isItemOverlappingInAd()).forEach(promoBuyItem ->{
				newItemList.add(promoBuyItem);
			});
			
			if(newItemList.size() == 0){
				promotion2.setCanBeAdded(false);
			}else{
				promotion2.setBuyItems(newItemList);
			}
		}
		
	}
	
	/**
	 * Sets calendar id  for actual start date and end date
	 * @param promotion
	 * @throws GeneralException 
	 */
	private boolean setActualCalendarInfoForLongTermPromo(PromoDefinition promotion, 
			Date startDate, Date endDate) throws GeneralException{
		boolean isValidCalendarId = true;
		String promoStartDate = DateUtil.dateToString(startDate, "MM/dd/yyyy");
		String promoEndDate = DateUtil.dateToString(endDate, "MM/dd/yyyy");
		
		int startCalId = getCalendarId(promoStartDate);
		int endCalId = getCalendarId(promoEndDate);
		if(startCalId>0 && endCalId>0){
			isValidCalendarId = false;
		}
		for(PromoBuyItems promoBuyItems: promotion.getBuyItems()){
			promoBuyItems.setActualStartCalId(startCalId);
			promoBuyItems.setActualEndCalId(endCalId);
		}
		return isValidCalendarId;
	}
	
	/**
	 * Sets calendar id in promotion object from promo start and end date 
	 * @param promotion
	 * @throws GeneralException
	 */
	private void setCalendarId(PromoDefinition promoDTO) throws GeneralException{
		String promoStartDate = DateUtil.dateToString(promoDTO.getPromoStartDate(), "MM/dd/yyyy");
		String promoEndDate = DateUtil.dateToString(promoDTO.getPromoEndDate(), "MM/dd/yyyy");
		
		promoDTO.setWeekStartDate(promoStartDate);
		
		//Get and set start calendar id
		promoDTO.setStartCalId(getCalendarId(promoStartDate));
		if(promoDTO.getStartCalId()==0){
			logger.debug("Promo Start date: "+promoStartDate);
		}
		//Get and set end calendar id
		promoDTO.setEndCalId(getCalendarId(promoEndDate));
	}
	
	/**
	 * 
	 * @param inputDate
	 * @return day calendar id for given input 
	 * @throws GeneralException
	 */
	private int getCalendarId(String inputDate) throws GeneralException {
		if (calendarMap.get(inputDate) == null) {
			int startCalId = ((RetailCalendarDTO) calDAO.getCalendarId(_Conn, inputDate, Constants.CALENDAR_DAY)).getCalendarId();
			calendarMap.put(inputDate, startCalId);
		}
		return calendarMap.get(inputDate);
	}
	
	/**
	 * 
	 * @param startDate
	 * @return weekly calendar id for given input date
	 */
	private int setCalIdforAd(String startDate){
		int calId = 0;
		try {
			if (weekCalendarMap.get(startDate) == null) {
				int startCalId = ((RetailCalendarDTO) calDAO.getCalendarId(_Conn,startDate,
						Constants.CALENDAR_WEEK)).getCalendarId();
				weekCalendarMap.put(startDate, startCalId);
				calId = startCalId;
			} else
				calId = weekCalendarMap.get(startDate);
		} catch (GeneralException e) {
			e.printStackTrace();
		}
		return calId;
	}
	
	/**
	 * 
	 * @param inputDate
	 * @return week start date for a given date
	 * @throws ParseException
	 */
	private Date getFirstDateOfWeek(Date inputDate) throws ParseException {
		String strDate = DateUtil.getWeekStartDate(inputDate, 0);
		Date outputDate = appDateFormatter.parse(strDate);
		return outputDate;
	}

	/**
	 * 
	 * @param inputDate
	 * @return week end date for a given date
	 * @throws ParseException
	 */
	private Date getLastDateOfWeek(Date inputDate) throws ParseException {
		String strDate = DateUtil.getWeekStartDate(inputDate, 0);
		Date endDate = appDateFormatter.parse(strDate);
		Date outputDate = DateUtil.incrementDate(endDate, 6);
		return outputDate;
	}

	/**
	 * Fields to be parsed to different promotion types
	 * @param fieldNames
	 */
	private void mapFieldNames(TreeMap<Integer, String> fieldNames) {
		fieldNames.put(1, "promoStartDate");
		fieldNames.put(2, "promoEndDate");
		fieldNames.put(3, "evtId_AdGroup");
		fieldNames.put(4, "evtId_Desc");
		fieldNames.put(6, "origItemNumber");
		fieldNames.put(7, "itemNumber");
		fieldNames.put(8, "upc");
		fieldNames.put(10, "itemDesc");
		fieldNames.put(11, "groupLookupId_PromoGroup");
		fieldNames.put(12, "groupDesc_PromoName");
		fieldNames.put(13, "groupPromoType");
		fieldNames.put(14, "blockNum");
		fieldNames.put(18, "saleQty");
		fieldNames.put(19, "salePrice");
		fieldNames.put(20, "regPrice");
		fieldNames.put(21, "itemPriceCode");
		fieldNames.put(22, "offInvoiceAmt");
		fieldNames.put(23, "scanBillBackAllowance");
		fieldNames.put(26, "adPage");
		fieldNames.put(27, "itemInAdInd");
		fieldNames.put(28, "cardReqIndicator");
		fieldNames.put(29, "buyQty");
		fieldNames.put(30, "getQty");
		fieldNames.put(31, "secondItemPrice");
		fieldNames.put(32, "secondItemQty");
		fieldNames.put(33, "secondItemPriceCode");
		fieldNames.put(34, "VICPS_UPC");
		fieldNames.put(35, "cpnDesc");
		fieldNames.put(36, "cpnAmount");
		fieldNames.put(37, "thresholdValue");
		fieldNames.put(38, "thresholdType");
	}
	
	
	
	/**
	 * Fields to be parsed to different promotion types
	 * @param fieldNames
	 */
	private void mapFieldNamesOld(TreeMap<Integer, String> fieldNames) {
		fieldNames.put(1, "promoStartDate");
		fieldNames.put(2, "promoEndDate");
		fieldNames.put(3, "evtId_AdGroup");
		fieldNames.put(4, "evtId_Desc");
		fieldNames.put(6, "origItemNumber");
		fieldNames.put(7, "itemNumber");
		fieldNames.put(8, "upc");
		fieldNames.put(10, "itemDesc");
		fieldNames.put(11, "groupLookupId_PromoGroup");
		fieldNames.put(12, "groupDesc_PromoName");
		fieldNames.put(13, "groupPromoType");
		fieldNames.put(14, "adPage");
		fieldNames.put(15, "blockNum");
		fieldNames.put(19, "saleQty");
		fieldNames.put(20, "salePrice");
		fieldNames.put(21, "regPrice");
		fieldNames.put(22, "itemPriceCode");
		fieldNames.put(23, "offInvoiceAmt");
		fieldNames.put(24, "scanBillBackAllowance");
		fieldNames.put(28, "itemInAdInd");
		fieldNames.put(29, "cardReqIndicator");
		fieldNames.put(30, "buyQty");
		fieldNames.put(31, "getQty");
		fieldNames.put(32, "secondItemPrice");
		fieldNames.put(33, "secondItemQty");
		fieldNames.put(34, "secondItemPriceCode");
		fieldNames.put(35, "VICPS_UPC");
		fieldNames.put(36, "cpnDesc");
		fieldNames.put(37, "cpnAmount");
		fieldNames.put(38, "thresholdValue");
		fieldNames.put(39, "thresholdType");
	}
	
	/**
	 * Parses location information from file name and sets it in promotion object
	 * @param promotion
	 * @throws GeneralException
	 */
	private void setLocation(PromoDefinition promotion){
		PromoLocation location = new PromoLocation();
		location.setLocationLevelId(Constants.CHAIN_LEVEL_ID);
		location.setLocationId(chainId);
		promotion.addPromoLocation(location);
	}
	
	
	/**
	 * Parses other data with respect to promotion and sets it in promotion object
	 * @param promotion
	 * @param fileName
	 * @throws GeneralException
	 */
	private void setupBuyReqAndOfferDetails(PromoDefinition promotion) throws GeneralException {
		if (promotion.getPromoTypeId() == Constants.PROMO_TYPE_BXGY_SAME) {
			parseBXGY(promotion);
		} else if (promotion.getPromoTypeId() == Constants.PROMO_TYPE_MUST_BUY) {
			parseMustBuy(promotion);
		} else if (promotion.getPromoTypeId() == Constants.PROMO_TYPE_BOGO) {
			parseBXGY(promotion);
		} else if (promotion.getPromoTypeId() == Constants.PROMO_TYPE_IN_AD_COUPON) {
			parseInAdCoupon(promotion);
		} else if (promotion.getPromoTypeId() == Constants.PROMO_TYPE_EBONUS_COUPON) {
			parseEBonusCoupon(promotion);
		}
	}
	
	/**
	 * 
	 * @param promotion
	 * @throws GeneralException
	 */
	private void parseBXGY(PromoDefinition promotion) throws GeneralException {
		PromoBuyRequirement buyReq = new PromoBuyRequirement();
		PromoOfferDetail offerDetail = new PromoOfferDetail();
		if (promotion.getPctOff() > 0 && promotion.getItemPriceCode().equals("P")) {
			offerDetail.setPromoOfferTypeId(Constants.OFFER_TYPE_OFF);
			offerDetail.setOfferUnitType(Constants.OFFER_UNIT_TYPE_PERCENTAGE);
			offerDetail.setOfferValue(promotion.getPctOff());
			offerDetail.setOfferUnitCount(promotion.getGetQuantity());
		} else {
			offerDetail.setPromoOfferTypeId(Constants.OFFER_TYPE_FREE_ITEM);
			offerDetail.setOfferUnitType(Constants.OFFER_UNIT_TYPE_NUMBER);
			offerDetail.setOfferUnitCount(promotion.getGetQuantity());
		}
		buyReq.setBuyAndGetIsSame(String.valueOf(Constants.YES));
		buyReq.setBuyX(promotion.getBuyQuantity());

		promotionService.parseCouponDescAndSetAddlDetails(promotion, buyReq, compiledPatterns, notProcessedCouponDesc);

		buyReq.addOfferDetail(offerDetail);
		promotion.addPromoBuyRequirement(buyReq);
	}

	/**
	 * 
	 * @param promotion
	 * @throws GeneralException
	 */
	private void parseMustBuy(PromoDefinition promotion) throws GeneralException{
		PromoBuyRequirement buyReq = new PromoBuyRequirement();
		buyReq.setMustBuyQty(promotion.getNoOfItems());
		String oStr = promotion.getRetailForPurchase();
		if (oStr != null)
			buyReq.setMustBuyAmt(Double.parseDouble(oStr));
		else {
			logger.error("Must Buy Amount cannot be parsed");
			promotion.setCanBeAdded(false);
			notProcessedPromotions.add("Unable to process Must Buy Amount for Promo: " + promotion.getPromoNumber());
			return;
		}
		promotionService.parseCouponDescAndSetAddlDetails(promotion, buyReq, compiledPatterns, notProcessedCouponDesc);
		promotion.addPromoBuyRequirement(buyReq);
	}
	
	/**
	 * 
	 * @param promotion
	 * @throws GeneralException
	 */
	private void parseInAdCoupon(PromoDefinition promotion) throws GeneralException{
		promotion.setPromoTypeId(Constants.PROMO_TYPE_STANDARD);
		promotion.setPromoDefnTypeId(Constants.PROMO_TYPE_STANDARD);
		PromoBuyRequirement buyReq = new PromoBuyRequirement();
		PromoOfferDetail promoOfferDetail = new PromoOfferDetail();
		promoOfferDetail.setPromoOfferTypeId(Constants.OFFER_TYPE_COUPON);
		promoOfferDetail.setOfferUnitType(Constants.OFFER_UNIT_TYPE_DOLLAR);
		double cpnAmount = Double.parseDouble(promotion.getAmountOff());
		if(cpnAmount < 100){
			promoOfferDetail.setOfferValue(Double.parseDouble(promotion.getAmountOff()));
			buyReq.addOfferDetail(promoOfferDetail);
			promotion.addPromoBuyRequirement(buyReq);	
		}else{
			promotion.setCanBeAdded(false);
			notProcessedPromotions.add("Plenti points in In Ad for Promo: " + promotion.getPromoNumber());
		}
	}
	
	/**
	 * 
	 * @param promotion
	 * @throws GeneralException
	 */
	private void parseEBonusCoupon(PromoDefinition promotion) throws GeneralException{
		promotion.setPromoTypeId(Constants.PROMO_TYPE_STANDARD);
		promotion.setPromoDefnTypeId(Constants.PROMO_TYPE_STANDARD);
		PromoBuyRequirement buyReq = new PromoBuyRequirement();
		promotionService.parseCouponDescAndSetAddlDetails(promotion, buyReq, compiledPatterns, notProcessedCouponDesc);
		promotion.addPromoBuyRequirement(buyReq);
	}
	
	
	
	
	class WeeklyAdKey{
		public String startDate;
		public String endDate;
		
		public WeeklyAdKey(String startDate, String endDate) {
			this.startDate = startDate;
			this.endDate = endDate;
		}

		@Override
		public int hashCode() {
			final int prime = 31;
			int result = 1;
			result = prime * result + getOuterType().hashCode();
			result = prime * result + ((endDate == null) ? 0 : endDate.hashCode());
			result = prime * result + ((startDate == null) ? 0 : startDate.hashCode());
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
			WeeklyAdKey other = (WeeklyAdKey) obj;
			if (!getOuterType().equals(other.getOuterType()))
				return false;
			if (endDate == null) {
				if (other.endDate != null)
					return false;
			} else if (!endDate.equals(other.endDate))
				return false;
			if (startDate == null) {
				if (other.startDate != null)
					return false;
			} else if (!startDate.equals(other.startDate))
				return false;
			return true;
		}

		private PromoAndAdLoader getOuterType() {
			return PromoAndAdLoader.this;
		}
	}
}

