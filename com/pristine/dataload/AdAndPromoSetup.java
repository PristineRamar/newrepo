package com.pristine.dataload;

import java.sql.Connection;
import java.text.DateFormat;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Date;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
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
import com.pristine.dto.ItemDetailKey;
import com.pristine.dto.PromoDataStandardDTO;
import com.pristine.dto.RetailCalendarDTO;
import com.pristine.dto.RetailCostDTO;
import com.pristine.dto.fileformatter.ahold.AholdPromoInputDTO;
import com.pristine.dto.offermgmt.promotion.PromoBuyItems;
import com.pristine.dto.offermgmt.promotion.PromoBuyRequirement;
import com.pristine.dto.offermgmt.promotion.PromoDefinition;
import com.pristine.dto.offermgmt.promotion.PromoLocation;
import com.pristine.dto.offermgmt.promotion.PromoOfferDetail;
import com.pristine.dto.offermgmt.promotion.PromoOfferItem;
import com.pristine.dto.offermgmt.weeklyad.WeeklyAd;
import com.pristine.dto.offermgmt.weeklyad.WeeklyAdBlock;
import com.pristine.dto.offermgmt.weeklyad.WeeklyAdPage;
import com.pristine.exception.GeneralException;
import com.pristine.lookup.offermgmt.promotion.PromoOfferTypeLookup;
import com.pristine.parsinginterface.ExcelFileParser;
import com.pristine.parsinginterface.ExcelFileParserV2;
import com.pristine.util.Constants;
import com.pristine.util.DateUtil;
import com.pristine.util.PrestoUtil;
import com.pristine.util.PristineDBUtil;
import com.pristine.util.PropertyManager;

@SuppressWarnings({ "unused", "rawtypes" })
public class AdAndPromoSetup {

	private static Logger logger = Logger.getLogger("AdAndPromoSetup");

	private String rootPath = null;
	private RetailPriceDAO retailPriceDAO = null;
	private ItemDAO itemDAO = null;
	RetailCalendarDAO calDAO = new RetailCalendarDAO();
	private static String inputFilePath;
	private static String week;
	private Connection conn = null;
	private final static String INPUT_FOLDER = "INPUT_FOLDER=";
	private final static String WEEK = "WEEK=";
	private final static String MODE = "MODE=";
	int chainId = 0;

	private ExcelFileParserV2 parser = null;
	List<ItemDTO> activeItems = null;
	HashMap<String, Integer> storeIdMap;
	HashMap<String, Integer> zoneMap;
	Set<String> itemCodeNotFound = null;
	Set<String> locationNotfound = null;
	Set<String> saleDeptgreater = null;
	Set<String> itemsNotprocessed = null;
	Set<String> ignoredItems = null;
	Set<String> notOnSale = null;
	Set<String> salePriceNotFound = null;
	private static HashMap<String, String> divisionNameMap = new HashMap<String, String>();
	HashMap<String, List<ItemDTO>> retailerItemCodeAndItsItem = null;
	HashMap<String, List<ItemDTO>> upcAndItsItem = null;
	HashMap<ItemDetailKey, Integer> itemDetailKeyItems = null;
	List<RetailCalendarDTO> calendarDetails = new ArrayList<RetailCalendarDTO>();
	HashMap<String, Integer> weekCalendarMap = new HashMap<String, Integer>();
	PromotionService promotionService = new PromotionService();
	RetailCostDAO rcDAO = new RetailCostDAO();
	Set<String> notProcessedPromotions = null;
	HashMap<Pattern, Integer> compiledPatterns = new HashMap<>();
	HashMap<String, Integer> calendarMap = new HashMap<String, Integer>();
	DateFormat appDateFormatter = new SimpleDateFormat(Constants.APP_DATE_FORMAT);
	private PromotionDAO promoDAO = null;
	private WeeklyAdDAO adDAO = null;
	Date processingWeek = null;
	boolean isDeltaMode = false;

	public AdAndPromoSetup() {
		PropertyManager.initialize("analysis.properties");
		rootPath = PropertyManager.getProperty("DATALOAD.ROOTPATH", "");
		try {
			conn = DBManager.getConnection();
		} catch (GeneralException ex) {
			logger.error("Error when creating connection - " + ex);
		}

	}

	public AdAndPromoSetup(Connection conn, List<ItemDTO> activeItems, boolean isDeltaMode) {
		this.conn = conn;
		this.activeItems = activeItems;
		this.isDeltaMode = isDeltaMode;

	}

	public static void main(String[] args) {

		AdAndPromoSetup promoLoader = new AdAndPromoSetup();

		PropertyConfigurator.configure("log4j-ad-promo-loader.properties");

		try {
			// Read input arguments
			for (String arg : args) {
				if (arg.startsWith(INPUT_FOLDER)) {
					inputFilePath = arg.substring(INPUT_FOLDER.length());
				}
				if (arg.startsWith(WEEK)) {
					week = arg.substring(WEEK.length());
				}

				if (arg.startsWith(MODE)) {
					promoLoader.isDeltaMode = arg.substring(MODE.length()).equalsIgnoreCase("DELTA");
				}
			}

			promoLoader.process();

		} catch (Exception e) {
			e.printStackTrace();
			logger.error("Exception in main()", e);
		}

	}

	private void process() {

		try {
			logger.info("Initialization Start....");
			initializeData();
			logger.info("Initialization Complete....");

			logger.info("processPromoFile Start....");
			processPromoFile();

		} catch (GeneralException e) {
			e.printStackTrace();
			logger.error("Error in Process" + e);
		}

	}

	/**
	 * Loads promotion data
	 * 
	 * @throws GeneralException
	 */
	@SuppressWarnings("unchecked")
	private void processPromoFile() throws GeneralException {
		logger.info("processPromoFile() - Processing start");
		String fName = null;
		String inputPath = inputFilePath;
		ExcelFileParser<PromoDataStandardDTO> parser2 = new ExcelFileParser<PromoDataStandardDTO>();
		parser2.setFirstRowToProcess(1);
		String archivePath = inputFilePath + "/";
		ArrayList<String> fileList = parser.getFiles(inputPath);
		TreeMap<Integer, String> fieldNames = new TreeMap<Integer, String>();
		mapFieldNames(fieldNames);

		try {
			for (String fileName : fileList) {
				logger.info("processPromoFile() - Processing file: " + fileName);
				fName = fileName;
				
				String[] filePath = fName.split("/");
				
				String file = filePath[filePath.length - 1];
				
				String dateStr = file.replaceAll("PromoFeedFile_", "").replaceAll(".csv", ""); 
				
				SimpleDateFormat dateFormat = new SimpleDateFormat("yyyy-MM-dd");
				SimpleDateFormat appDateFormat = new SimpleDateFormat(Constants.APP_DATE_FORMAT);
				processingWeek = dateFormat.parse(dateStr);
				
				
				week = appDateFormat.format(processingWeek);
				
				List<PromoDataStandardDTO> promoData = null;
				List<PromoDataStandardDTO> finalPromoData = new ArrayList<PromoDataStandardDTO>();
				parser.setFirstRowToProcess(1);
				String[] fileExtArr = fileName.split("\\.");
				String fileExtension = fileExtArr[fileExtArr.length - 1];

				if (fileExtension.equalsIgnoreCase("XLS") || fileExtension.equalsIgnoreCase("XLSX")) {
					promoData = parser2.parseExcelFile(PromoDataStandardDTO.class, fileName, 0, fieldNames);
				} else if (fileExtension.equalsIgnoreCase("CSV") || fileExtension.equalsIgnoreCase("TXT")) {
					promoData = parser.parseCSVFile(PromoDataStandardDTO.class, fileName, fieldNames, ',');
				}

				logger.info("processPromoFile() - Total # of records added to process: " + promoData.size());
				
				logger.info("processPromoFile() - Week From File: " + week);
				setupPromoData(promoData, week);
				
				PrestoUtil.moveFile(fileName, archivePath + Constants.COMPLETED_FOLDER);
				PristineDBUtil.commitTransaction(conn, "Ad data load Commit");
				logger.info("processPromotionFiles() - End of Processing file: " + fileName);
			}
		} catch (Exception ex) {
			logger.error("processPromoFile()-Exception while processing file : " + fName, ex);
			PristineDBUtil.rollbackTransaction(conn, "processPromoFile()-Exception while processing file");
		} finally {
			PristineDBUtil.commitTransaction(conn, "Promo loading");
			PristineDBUtil.close(conn);
		}

	}

	/**
	 * 
	 * @param promoList
	 * @throws GeneralException
	 * @throws Exception
	 */

	public void setupPromoData(List<PromoDataStandardDTO> promoData, String week) throws GeneralException, Exception {

		initializeData();

		List<WeeklyAd> weeklyAds = null;

		logger.info("processPromoRecords() - promoData Size: " + promoData.size());
		weeklyAds = groupAndCreateWeeklyAdObjects(promoData);

		List<PromoDefinition> nonAdPromotions = groupAndCreateNonAdPromoObjects(promoData, week);

		logger.info("processPromoRecords() - Non Ad Promotions: " + nonAdPromotions.size());

		handleNonAdAndAdPromotions(weeklyAds, nonAdPromotions);

		logger.info("processPromoRecords() - Non Ad Promotions after handle: " + nonAdPromotions.size());

		saveAdAndPromotions(weeklyAds, nonAdPromotions);

		// Log Error records
		logNotProcessed();

	}

	private void saveAdAndPromotions(List<WeeklyAd> weeklyAds, List<PromoDefinition> nonAdPromotions)
			throws GeneralException {

		int ignoresaledepthAbove = Integer.parseInt(PropertyManager.getProperty("IGNORE_SALE_DEPTH_ABOVE", "0"));

		List<PromoDefinition> allPromotions = new ArrayList<>();
		/* List<PromoDefinition> nonAdPromo = new ArrayList<>(); */
		List<WeeklyAd> weeklyAdsnew = new ArrayList<>();

		List<PromoDefinition> finalPromotions = new ArrayList<>();

		nonAdPromotions.stream().filter(promo -> promo.isCanBeAdded()).forEach(promotion -> {
			allPromotions.add(promotion);
		});

		logger.info("Final Promo size for nonAdPromotions:  " + allPromotions.size());
		for (WeeklyAd weeklyAd : weeklyAds) {
			for (Map.Entry<Integer, WeeklyAdPage> pageEntry : weeklyAd.getAdPages().entrySet()) {
				for (Map.Entry<Integer, WeeklyAdBlock> blockEntry : pageEntry.getValue().getAdBlocks().entrySet()) {

					ArrayList<PromoDefinition> filteredPromo = (ArrayList<PromoDefinition>) blockEntry.getValue()
							.getPromotions().stream().filter(promo -> promo.isCanBeAdded())
							.collect(Collectors.toList());

					allPromotions.addAll(filteredPromo);

					blockEntry.getValue().setPromotions(filteredPromo);
				}
			}
		}

		for (PromoDefinition promodef : allPromotions) {
			List<PromoBuyItems> buyitems = promodef.getBuyItems();
			boolean isValidPromo = true;
			int offerItempresent = 0;
			if (promodef.getOfferItem() != null) {
				if (promodef.getOfferItem().isEmpty()) {
					offerItempresent = 0;
				} else {
					offerItempresent = 1;
				}

			}
	
			for (PromoBuyItems byItems : buyitems) {
				double unitRegprice = byItems.getRegPrice() / byItems.getRegQty();
				double unitSalePrice = byItems.getSalePrice() / byItems.getSaleQty();

				if ((byItems.getRegPrice() / byItems.getRegQty()) == (byItems.getSalePrice() / byItems.getSaleQty())) {
					logger.debug("Items regPrice is same as salePrice for UPC :" + promodef.getUPC() + "; promogroup:  "
							+ promodef.getPromoGroup() + "; PromoId:  " + promodef.getPromoNumber());
					ignoredItems
							.add(promodef.getUPC() + ";" + promodef.getPromoGroup() + ";" + promodef.getPromoNumber());
					isValidPromo = false;
				}
				if (byItems.getSalePrice() / byItems.getSaleQty() > byItems.getRegPrice() / byItems.getRegQty()) {
					logger.debug("SalePrice  is greater than regPrice for UPC :" + promodef.getUPC() + "; promogroup:  "
							+ promodef.getPromoGroup() + "; PromoId:  " + promodef.getPromoNumber());
					notOnSale.add(promodef.getUPC() + ";" + promodef.getPromoGroup() + ";" + promodef.getPromoNumber());
					isValidPromo = false;
				}
				if (byItems.getSalePrice() == 0 && promodef.getNoOfItems() == 0 && offerItempresent == 0) {
					logger.debug("SalePrice not found:" + promodef.getUPC() + "; promogroup:  "
							+ promodef.getPromoGroup() + "; PromoId:  " + promodef.getPromoNumber());
					salePriceNotFound
							.add(promodef.getUPC() + ";" + promodef.getPromoGroup() + ";" + promodef.getPromoNumber());
					isValidPromo = false;
				}
				if (ignoresaledepthAbove > 0) {
					double saleDepth = ((unitRegprice - unitSalePrice) / unitRegprice) * 100;
					if (saleDepth < 0 || saleDepth >= ignoresaledepthAbove) {
						logger.debug("Sale depth greater than " + ignoresaledepthAbove + " : " + promodef.getUPC()
								+ "; promogroup:  " + promodef.getPromoGroup() + "; PromoId:  "
								+ promodef.getPromoNumber());
						saleDeptgreater.add(
								promodef.getUPC() + ";" + promodef.getPromoGroup() + ";" + promodef.getPromoNumber());
						isValidPromo = false;
					}
				}
			}

			if (isValidPromo) {
				// nonadpr.setPromoNumber(Constants.EMPTY);
				finalPromotions.add(promodef);
			}
		}

		logger.info("finalpromo size to insert : " + finalPromotions.size());

		/*
		 * for (PromoDefinition finalpr : finalPromotions) {
		 * 
		 * logger.info("PROMONAME: " + finalpr.getPromoName() + "UPC :  " +
		 * finalpr.getUPC() + " CODE : " + finalpr.getItemCode() + " PROMO TYPE ID : " +
		 * finalpr.getPromoTypeId() + " BMSM DOLLAROFF " +
		 * finalpr.getBmsmDollaroffperunits() + "  PCT OFF " + finalpr.getPctOff() +
		 * "BMSM PCT OFF  " + finalpr.getBmsmPctoffperunit() + " DOLLAR OFF  " +
		 * finalpr.getDollarOff() + "SP: " + finalpr.getSalePrice() + " SQ :" +
		 * finalpr.getBmsmsaleQty());
		 * 
		 * List<PromoBuyItems> buyitems = finalpr.getBuyItems();
		 * 
		 * for (PromoBuyItems buy : buyitems) {
		 * 
		 * logger.info("BUY TEMS"); logger.info("REG PR :  " + buy.getRegPrice() +
		 * " REG QTY : " + buy.getRegQty() + "SALE PRICE: " + buy.getSalePrice() +
		 * " SALE QTY " + buy.getSaleQty() + "  DISCOUNT DOLLAR" +
		 * buy.getDiscountRegDollar()); } List<PromoBuyRequirement> buyre =
		 * finalpr.getPromoBuyRequirement(); List<PromoLocation> promoLoc =
		 * finalpr.getPromoLocation(); for (PromoLocation loc : promoLoc) {
		 * logger.info("LocationId :" + loc.getLocationId() + "LoctionLevel : " +
		 * loc.getLocationLevelId()); }
		 * 
		 * 
		 * for (PromoBuyRequirement by : buyre) { logger.info("BUYREQ");
		 * logger.info("BUYX :  " + by.getBuyX() + " getMinQtyReqd : " +
		 * by.getMinQtyReqd() + "getMustBuyQty: " + by.getMustBuyQty() +
		 * " getMustBuyAmt " + by.getMustBuyAmt()); }
		 * 
		 * }
		 */

		logger.info("processPromoRecords() - Total # of promotions: " + finalPromotions.size());
		long startTime = System.currentTimeMillis();

		logger.info("Processing Mode: " + (isDeltaMode ? "Delta" : "Full"));
		
		promoDAO.savePromotionV2(finalPromotions, processingWeek, isDeltaMode);

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
	private void logNotProcessed() {
		StringBuilder sb = new StringBuilder();
		for (String itemNotFound : itemCodeNotFound) {
			sb.append(itemNotFound + "\n");
		}
		logger.info("***Item code not found records***" + itemCodeNotFound.size());
		logger.info(sb.toString());

		sb = new StringBuilder();
		for (String notProcessed : notProcessedPromotions) {
			sb.append(notProcessed + "\n");
		}
		logger.info("***Not Processed Promo Records***" + notProcessedPromotions.size());
		logger.info(sb.toString());

		/*
		 * sb = new StringBuilder(); for (String notProcessed : notProcessedCouponDesc)
		 * { sb.append(notProcessed + "\n"); }
		 * logger.info("***Not Processed Coupon Descriptions***");
		 * logger.info(sb.toString());
		 */
		
		sb = new StringBuilder();
		for (String ignoreditems : ignoredItems) {
			sb.append(ignoreditems + "\n");
		}
		logger.info("***Ignored as Reg and sale price are same ***" + ignoredItems.size());
		logger.info(sb.toString());

		sb = new StringBuilder();
		for (String notProcessed : locationNotfound) {
			sb.append(notProcessed + "\n");
		}
		logger.info("***Not Processed Location Descriptions***" + locationNotfound.size());
		logger.info(sb.toString());

		sb = new StringBuilder();
		for (String notProcessed : itemsNotprocessed) {
			sb.append(notProcessed + "\n");
		}
		logger.info("***Not Processed Items from file due to invalid data ***" + itemsNotprocessed.size());
		logger.info(sb.toString());

		sb = new StringBuilder();
		for (String notonsale : notOnSale) {
			sb.append(notonsale + "\n");
		}
		logger.info("***salePrice greater than regPrice ***" + notOnSale.size());
		logger.info(sb.toString());

		sb = new StringBuilder();
		for (String notonsale : salePriceNotFound) {
			sb.append(notonsale + "\n");
		}

		logger.info("***salePrice not Found***" + salePriceNotFound.size());
		logger.info(sb.toString());

		sb = new StringBuilder();
		for (String saledepthgreater : saleDeptgreater) {
			sb.append(saledepthgreater + "\n");
		}
		logger.info("***saledepth greater ***" + saleDeptgreater.size());
		logger.info(sb.toString());

	}

	/**
	 * 
	 * @param promoList
	 * @return non promo objects
	 * @throws GeneralException
	 * @throws Exception
	 */
	private List<PromoDefinition> groupAndCreateNonAdPromoObjects(List<PromoDataStandardDTO> promoList, String week)
			throws GeneralException, Exception {
		List<PromoDefinition> promoDefList = new ArrayList<>();

		logger.debug("Inside groupAndCreateNonAdPromoObjects ()- ");

		HashMap<String, PromoDefinition> promoGroups = getPromotionGroups(promoList, false);

		Date weekdate = new SimpleDateFormat(Constants.APP_DATE_FORMAT).parse(week);
		logger.info("weekdate" + weekdate);
		for (Map.Entry<String, PromoDefinition> promoEntry : promoGroups.entrySet()) {

			List<PromoDefinition> weeklyPromotions = splitPromoByWeek(promoEntry.getValue(), weekdate);

			promoDefList.addAll(weeklyPromotions);
		}

		// Sort to ascending
		promoDefList.sort((p1, p2) -> p1.getPromoStartDate().compareTo(p2.getPromoStartDate()));
		logger.debug(" groupAndCreateNonAdPromoObjects ()- returning : " + promoDefList.size());
		return promoDefList;
	}

	/**
	 * 
	 * @param promotion
	 * @return list of weekly promotions derived from longer duration promotions
	 * @throws Exception
	 * @throws GeneralException
	 */
	public List<PromoDefinition> splitPromoByWeek(PromoDefinition promotion, Date processingWeek)
			throws Exception, GeneralException {
		List<PromoDefinition> promoList = new ArrayList<>();
	
		Date promoStartDate = promotion.getPromoStartDate();
		Date promoEndDate = promotion.getPromoEndDate();

		promotion.setPromoStartDate(promoStartDate);
		promotion.setPromoEndDate(promoEndDate);

		long diff = promotion.getPromoEndDate().getTime() - promotion.getPromoStartDate().getTime();

		long diffDates = TimeUnit.DAYS.convert(diff, TimeUnit.MILLISECONDS);

		if (diffDates > 7) {
			float dif = (float) diffDates / 7;
			int noOfWeeks = (int) Math.ceil(dif);

			for (int i = 0; i < noOfWeeks; i++) {
				PromoDefinition promoNew = (PromoDefinition) promotion.clone();
				promoNew.setPromoStartDate(promoStartDate);
				promoEndDate = DateUtil.incrementDate(promoStartDate, 6);
				promoStartDate = DateUtil.incrementDate(promoStartDate, 7);
				promoNew.setPromoEndDate(promoEndDate);
				if (promoNew.getPromoStartDate().compareTo(processingWeek) >= 0) {
					setCalendarId(promoNew);
					boolean isValidCalendarId = setActualCalendarInfoForLongTermPromo(promotion,
							promotion.getPromoStartDate(), promotion.getPromoEndDate());
					if (isValidCalendarId && promoNew.getStartCalId() > 0 && promoNew.getEndCalId() > 0) {

						promoList.add(promoNew);
					}
				}
			}
		} else {

			if (promotion.getPromoStartDate().compareTo(processingWeek) >= 0) {
				setCalendarId(promotion);
				boolean isValidCalendarId = setActualCalendarInfoForLongTermPromo(promotion, promoStartDate,
						promoEndDate);
				if (isValidCalendarId && promotion.getStartCalId() > 0 && promotion.getEndCalId() > 0) {

					promoList.add(promotion);
				}
			}
		}
		logger.debug("splitPromoByWeek size is " + promoList.size());
		return promoList;
	}

	/**
	 * Handles ad and non ad promotions overlapping
	 * 
	 * @param weeklyAds
	 * @param nonAdPromotions
	 */
	private void handleNonAdAndAdPromotions(List<WeeklyAd> weeklyAds, List<PromoDefinition> nonAdPromotions) {
		logger.debug("Inside handleNonAdAndAdPromotions()");
		for (WeeklyAd ad : weeklyAds) {
			for (Map.Entry<Integer, WeeklyAdPage> pageEntry : ad.getAdPages().entrySet()) {
				for (Map.Entry<Integer, WeeklyAdBlock> blockEntry : pageEntry.getValue().getAdBlocks().entrySet()) {
					for (PromoDefinition adPromotion : blockEntry.getValue().getPromotions()) {
						Date adStartDate = adPromotion.getPromoStartDate();
						Date adEndDate = adPromotion.getPromoEndDate();
						for (PromoDefinition nonAdPromotion : nonAdPromotions) {
							// get overlapping promotions and remove them from them the collection
							Date promoStartDate = nonAdPromotion.getPromoStartDate();
							Date promoEndDate = nonAdPromotion.getPromoEndDate();
							if (promoStartDate.compareTo(adStartDate) >= 0 && promoEndDate.compareTo(adEndDate) <= 0) {
								adjustBuyItemsInMultipleGroups(adPromotion, nonAdPromotion);
							}
						}
					}
				}
			}
		}
	}

	private List<WeeklyAd> groupAndCreateWeeklyAdObjects(List<PromoDataStandardDTO> promoData) throws GeneralException {
		List<WeeklyAd> weeklyAdList = new ArrayList<>();
		HashMap<WeeklyAdKey, List<PromoDataStandardDTO>> weeklyAdMap = new HashMap<>();

		promoData.stream()
				.filter(promo -> (promo.getPageNumber() != null && !Constants.EMPTY.equals(promo.getPageNumber())
						&& promo.getBlockNumber() != null && !Constants.EMPTY.equals(promo.getBlockNumber())))
				.forEach(item -> {

					WeeklyAdKey weeklyAdKey = new WeeklyAdKey(item.getPromoStartDate(), item.getPromoEndDate());

					List<PromoDataStandardDTO> tempList = new ArrayList<>();
					tempList.add(item);

					if (weeklyAdMap.containsKey(weeklyAdKey)) {
						tempList.addAll(weeklyAdMap.get(weeklyAdKey));
					}

					weeklyAdMap.put(weeklyAdKey, tempList);

				});
		logger.debug(" groupAndCreateWeeklyAdObjects() - weeklyAdMap size   " + weeklyAdMap.size());

		if (weeklyAdMap.size() > 0) {

			for (Entry<WeeklyAdKey, List<PromoDataStandardDTO>> weeklyAdEntry : weeklyAdMap.entrySet()) {

				WeeklyAd weeklyAd = new WeeklyAd();
				weeklyAd.setApprovedBy(Constants.BATCH_USER);
				weeklyAd.setCreatedBy(Constants.BATCH_USER);
				/*
				 * weeklyAd.setLocationLevelId(Constants.CHAIN_LEVEL_ID);
				 * weeklyAd.setLocationId(chainId);
				 */
				weeklyAd.setStatus(3);
				PromoDataStandardDTO sampleObj = weeklyAdEntry.getValue().get(0);
				logger.debug(" location No:" + sampleObj.getLocationNo());
				logger.debug(" location Level:" + sampleObj.getLocationLevel());

				if (sampleObj.getLocationLevel().toUpperCase().equals(Constants.ZONE)) {

					String zoneNo = PrestoUtil.castZoneNumber(sampleObj.getLocationNo());
					if (zoneMap.containsKey(zoneNo)) {
						weeklyAd.setLocationLevelId(Constants.ZONE_LEVEL_ID);
						weeklyAd.setLocationId(zoneMap.get(zoneNo));
					} else
						locationNotfound
								.add("Location not found " + sampleObj.getUpc() + ";" + sampleObj.getItemCode());
				} else if (sampleObj.getLocationLevel().equals(Constants.CHAIN)) {
					weeklyAd.setLocationLevelId(Constants.CHAIN_LEVEL_ID);
					weeklyAd.setLocationId(chainId);
				} else if (sampleObj.getLocationLevel().toUpperCase().equals(Constants.DIVISION)) {
					if (divisionNameMap.containsKey(sampleObj.getLocationNo())) {
						weeklyAd.setLocationLevelId(Constants.DIVISION_LEVEL_ID);
						// weeklyAd.setLocationId(divisionNameMap.get(sampleObj.getLocationNo()));
					}
					if (sampleObj.getLocationLevel().toUpperCase() == Constants.STORELEVEL) {
						if (storeIdMap.containsKey(sampleObj.getLocationNo())) {
							weeklyAd.setLocationLevelId(Constants.STORE_LEVEL_ID);
							weeklyAd.setLocationId(storeIdMap.get(sampleObj.getLocationNo()));
						} else
							locationNotfound
									.add("Location not found " + sampleObj.getUpc() + ";" + sampleObj.getItemCode());
					}
				} else if (Constants.EMPTY.equals(sampleObj.getLocationLevel())) {
					if (zoneMap.containsKey(sampleObj.getLocationNo())) {
						weeklyAd.setLocationLevelId(Constants.ZONE_LEVEL_ID);
						weeklyAd.setLocationId(zoneMap.get(sampleObj.getLocationNo()));
					} else
						locationNotfound
								.add("Location not found " + sampleObj.getUpc() + ";" + sampleObj.getItemCode());
				} else {

					locationNotfound.add("New location level " + sampleObj.getLocationLevel() + "not  found for "
							+ sampleObj.getUpc() + ";" + sampleObj.getItemCode());
				}
				weeklyAd.setCalendarId(setCalIdforAd(sampleObj.getPromoStartDate()));
				weeklyAd.setWeekStartDate(sampleObj.getPromoStartDate());
				weeklyAd.setStartDate(DateUtil.toDate(sampleObj.getPromoStartDate()));

				weeklyAd.setAdName("Ad - " + sampleObj.getLocationNo() + " - " + weeklyAd.getWeekStartDate());
				weeklyAd.setAdNumber(weeklyAd.getWeekStartDate().replaceAll("/", ""));

				HashMap<String, PromoDefinition> promoGroups = getPromotionGroups(weeklyAdEntry.getValue(), true);

				setupWeeklyAdAndPromotions(promoGroups, weeklyAd);

				weeklyAdList.add(weeklyAd);
			}
		}
		logger.debug(" groupAndCreateWeeklyAdObjects() - complete");
		return weeklyAdList;
	}

	/**
	 * 
	 * @param promoGroups
	 * @param weeklyAd
	 */
	private void setupWeeklyAdAndPromotions(HashMap<String, PromoDefinition> promoGroups, WeeklyAd weeklyAd) {

		HashMap<String, HashMap<String, List<PromoDefinition>>> pageBlockMap = new HashMap<>();

		for (Map.Entry<String, PromoDefinition> promoEntry : promoGroups.entrySet()) {
			PromoDefinition promoDefinition = promoEntry.getValue();

			HashMap<String, List<PromoDefinition>> blockMap = null;

			if (pageBlockMap.containsKey(promoDefinition.getAdpage())) {
				blockMap = pageBlockMap.get(promoDefinition.getAdpage());
			} else {
				blockMap = new HashMap<>();
			}

			pageBlockMap.put(promoDefinition.getAdpage(), blockMap);

			if (blockMap.containsKey(promoDefinition.getBlockNum())) {
				List<PromoDefinition> promoList = blockMap.get(promoDefinition.getBlockNum());
				promoList.add(promoDefinition);
				blockMap.put(promoDefinition.getBlockNum(), promoList);
			} else {
				List<PromoDefinition> promoList = new ArrayList<>();
				promoList.add(promoDefinition);
				blockMap.put(promoDefinition.getBlockNum(), promoList);
			}
		}

		logger.info("setupWeeklyAdAndPromotions() - # of pages for Ad " + weeklyAd.getWeekStartDate() + ": "
				+ pageBlockMap.size());
		// Set total pages
		weeklyAd.setTotalPages(pageBlockMap.size());

		for (Map.Entry<String, HashMap<String, List<PromoDefinition>>> pageBlockEntry : pageBlockMap.entrySet()) {
			// Create new page
			WeeklyAdPage weeklyAdPage = new WeeklyAdPage();
			weeklyAdPage.setPageNumber(Integer.parseInt(pageBlockEntry.getKey()));
			weeklyAdPage.setTotalBlocks(pageBlockEntry.getValue().size());
			weeklyAdPage.setStatus(3);

			for (Map.Entry<String, List<PromoDefinition>> blockEntry : pageBlockEntry.getValue().entrySet()) {
				// Create new block
				WeeklyAdBlock weeklyAdBlock = new WeeklyAdBlock();

				weeklyAdBlock.setBlockNumber(Integer.parseInt(blockEntry.getKey()));
				weeklyAdBlock.setStatus(3);

				// Set promotions
				ArrayList<PromoDefinition> promotions = new ArrayList<>(blockEntry.getValue());
				weeklyAdBlock.setPromotions(promotions);

				// Attach block to page
				weeklyAdPage.addBlock(weeklyAdBlock);
			}

			// Attach page to weekly Ad
			weeklyAd.addPage(weeklyAdPage);
		}
	}

	private void mapFieldNames(TreeMap<Integer, String> fieldNames) {
		fieldNames.put(0, "category");
		fieldNames.put(1, "promoStartDate");
		fieldNames.put(2, "promoEndDate");
		fieldNames.put(3, "promoID");
		fieldNames.put(4, "promoDescription");
		fieldNames.put(5, "itemCode");
		fieldNames.put(6, "upc");
		fieldNames.put(7, "ItemName");
		fieldNames.put(8, "lirName");
		fieldNames.put(9, "promoGroup");
		fieldNames.put(10, "everdayQty");
		fieldNames.put(11, "everydayPrice");
		fieldNames.put(12, "saleQty");
		fieldNames.put(13, "salePrice");
		fieldNames.put(14, "mustBuyQty");
		fieldNames.put(15, "mustbuyPrice");
		fieldNames.put(16, "dollarOff");
		fieldNames.put(17, "pctOff");
		fieldNames.put(18, "buyQty");
		fieldNames.put(19, "getQty");
		fieldNames.put(20, "minimumQty");
		fieldNames.put(21, "minimumAmt");
		fieldNames.put(22, "bmsmDollaroffperunits");
		fieldNames.put(23, "bmsmPctoffperunit");
		fieldNames.put(24, "bmsmsaleQty");
		fieldNames.put(25, "bmsmsalePrice");
		fieldNames.put(26, "status");
		fieldNames.put(27, "locationLevel");
		fieldNames.put(28, "locationNo");
		fieldNames.put(29, "pageNumber");
		fieldNames.put(30, "blockNumber");
		fieldNames.put(31, "displayOffer");
		fieldNames.put(32, "description");
		fieldNames.put(33, "offerItemUpc");
		fieldNames.put(34, "offerItemCode");
		fieldNames.put(35, "couponType");
		fieldNames.put(36, "couponAmt");
	}

	private void initializeData() throws GeneralException {
		try {
			itemCodeNotFound = new HashSet<>();
			locationNotfound = new HashSet<>();
			itemsNotprocessed = new HashSet<>();
			ignoredItems = new HashSet<>();
			notOnSale = new HashSet<>();
			saleDeptgreater = new HashSet<>();
			salePriceNotFound = new HashSet<>();
			parser = new ExcelFileParserV2();
			// activeItems = new ArrayList<ItemDTO>();
			retailPriceDAO = new RetailPriceDAO();
			adDAO = new WeeklyAdDAO(conn);
			ItemDAO itemDAO = new ItemDAO();
			parser.setFirstRowToProcess(1);
			promoDAO = new PromotionDAO(conn);
			retailerItemCodeAndItsItem = new HashMap<String, List<ItemDTO>>();
			upcAndItsItem = new HashMap<String, List<ItemDTO>>();
			itemDetailKeyItems = new HashMap<ItemDetailKey, Integer>();

			if (activeItems == null) {
				logger.info("initializeData() - Getting all items started...");
				activeItems = itemDAO.getAllActiveItems(conn);
				logger.info("initializeData() -	Active items" + activeItems.size());

				logger.info("initializeData() - Getting all items complete.....");
			}
			logger.info("initializeData() - groupItemByRetailerItemCode started...");
			groupItemByRetailerItemCode();
			logger.info("initializeData() - groupItemByRetailerItemCodes end...");

			logger.info("initializeData() -groupItemByUPC started...");
			groupItemByUPC();
			// groupBy ItemRetailkey for
			logger.info("initializeData() - groupItemByUPC ended...");

			for (ItemDTO itemDTO : activeItems) {

				ItemDetailKey itemDetailkey = new ItemDetailKey(itemDTO.upc, itemDTO.retailerItemCode);

				itemDetailKeyItems.put(itemDetailkey, itemDTO.itemCode);
			}

			chainId = Integer.parseInt(retailPriceDAO.getChainId(conn));

			logger.info("initializeData() - Getting getStoreDetails started.....");
			storeIdMap = retailPriceDAO.getStoreIdMap(conn, chainId);
			logger.info("initializeData() - Getting getStoreDetails complete.....");

			logger.info("initializeData() - Getting zoneDetails started.....");
			zoneMap = retailPriceDAO.getRetailPriceZone(conn);
			logger.info("initializeData() - Getting zoneDetails complete.....");

			mapDivision();

			// calendarDetails = calDAO.getFullRetailCalendar(conn);
			notProcessedPromotions = new HashSet<>();
		} catch (Exception e) {
			e.printStackTrace();
			logger.error("Exception in Initialize()", e);
		}
	}

	private void groupItemByRetailerItemCode() {
		retailerItemCodeAndItsItem = (HashMap<String, List<ItemDTO>>) activeItems.stream()
				.filter(p -> p.getRetailerItemCode() != null && !p.getRetailerItemCode().equals("null"))
				.collect(Collectors.groupingBy(ItemDTO::getRetailerItemCode));
	}

	private void groupItemByUPC() {
		upcAndItsItem = (HashMap<String, List<ItemDTO>>) activeItems.stream()
				.filter(p -> p.getUpc() != null && !p.getUpc().equals("null"))
				.collect(Collectors.groupingBy(ItemDTO::getUpc));
	}

	/*
	 * private void groupItemByItemDetailKey() { itemDetailKeyItems =
	 * (HashMap<ItemDetailKey, Integer>) activeItems.stream()
	 * .collect(Collectors.toMap(ItemDTO::getItemDetailKey, ItemDTO::getItemCode));
	 * 
	 * }
	 */

	private void mapDivision() {
		divisionNameMap.put("NE", "NEW ENGLAND");
		divisionNameMap.put("GC", "GIANTC");
		divisionNameMap.put("GL", "GIANT");
		divisionNameMap.put("NY", "NEW YORK METRO");
		divisionNameMap.put("SSS", "NEW YORK METRO");
		divisionNameMap.put("SSN", "NEW ENGLAND");
	}

	/**
	 * 
	 * @param startDate
	 * @return weekly calendar id for given input date
	 */
	private int setCalIdforAd(String startDate) {
		int calId = 0;
		try {
			if (weekCalendarMap.get(startDate) == null) {
				int startCalId = ((RetailCalendarDTO) calDAO.getCalendarId(conn, startDate, Constants.CALENDAR_WEEK))
						.getCalendarId();
				weekCalendarMap.put(startDate, startCalId);
				calId = startCalId;
			} else
				calId = weekCalendarMap.get(startDate);
		} catch (GeneralException e) {
			e.printStackTrace();
		}
		return calId;
	}

	class WeeklyAdKey {
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

		private AdAndPromoSetup getOuterType() {
			return AdAndPromoSetup.this;
		}
	}

	/**
	 * 
	 * @param promoFileDTOList
	 * @param isInAd
	 * @return collection of promotions
	 * @throws GeneralException
	 */
	private HashMap<String, PromoDefinition> getPromotionGroups(List<PromoDataStandardDTO> promoFileDTOList,
			boolean isInAd) throws GeneralException {
		logger.info("getPromotionGroups inside");
		HashMap<String, PromoDefinition> promoGroups = new HashMap<>();
		String promoGroup = "";
		String promoGroupKey = "";
		try {
			for (PromoDataStandardDTO promoFileDTO : promoFileDTOList) {

				PromoDefinition promoDefinition = null;

				int promoTypeId = promotionService.getPromotionType(promoFileDTO, new PromoDefinition());
				logger.debug("promoTypeId " + promoTypeId);
				// Key for promotion group
				if (!Constants.EMPTY.equals(promoFileDTO.getPromoGroup()) && promoFileDTO.getPromoGroup() != null)

				{
					if (!Constants.EMPTY.equals(promoFileDTO.getPromoStartDate())
							&& !Constants.EMPTY.equals(promoFileDTO.getPromoEndDate())
							&& !Constants.EMPTY.equals(promoFileDTO.getLocationLevel())
							&& !Constants.EMPTY.equals(promoFileDTO.getLocationNo())
							&& !(promoFileDTO.getLocationLevel().equals(null))
							&& !promoFileDTO.getLocationNo().equals(null)
							&& !(promoFileDTO.getPromoStartDate().equals(null))
							&& !promoFileDTO.getPromoEndDate().equals(null) && promoTypeId != 0) {
						promoGroupKey = promoFileDTO.getPromoStartDate() + "-" + promoFileDTO.getPromoEndDate() + "-"
								+ promoTypeId + promoFileDTO.getLocationLevel() + "-" + promoFileDTO.getLocationNo()
								+ "-" + promoFileDTO.getPromoGroup();
					}

				} else {
					if (!Constants.EMPTY.equals(promoFileDTO.getItemCode())
							&& !Constants.EMPTY.equals(promoFileDTO.getUpc())
							&& !Constants.EMPTY.equals(promoFileDTO.getPromoStartDate())
							&& !Constants.EMPTY.equals(promoFileDTO.getPromoEndDate())
							&& !Constants.EMPTY.equals(promoFileDTO.getLocationLevel())
							&& !Constants.EMPTY.equals(promoFileDTO.getLocationNo())
							&& !(promoFileDTO.getLocationLevel().equals(null))
							&& !promoFileDTO.getLocationNo().equals(null)
							&& !(promoFileDTO.getPromoStartDate().equals(null))
							&& !promoFileDTO.getPromoEndDate().equals(null) && promoTypeId != 0) {
						promoGroupKey = promoFileDTO.getItemCode() + "-" + promoFileDTO.getUpc() + "-"
								+ promoFileDTO.getPromoStartDate() + "-" + promoFileDTO.getPromoEndDate() + "-"
								+ promoTypeId + promoFileDTO.getLocationLevel() + "-" + promoFileDTO.getLocationNo()
								+ "-" + promoFileDTO.getPromoGroup();
					}

				}

				// Get buy item object
				List<PromoBuyItems> promoBuyItems = setupPromoBuyItems(promoFileDTO, isInAd);
				logger.debug("promoBuyItems final size : " + promoBuyItems.size());

				if (promoBuyItems.size() > 0) {

					for (PromoBuyItems promoBuyItem : promoBuyItems) {
						// Group promotions

						if (promoGroups.containsKey(promoGroupKey)) {

							promoDefinition = promoGroups.get(promoGroupKey);
							List<PromoBuyItems> buyItems = promoDefinition.getBuyItems();
							// Get distinct items from collection
							Set<Integer> itemCodes = buyItems.stream().map(PromoBuyItems::getItemCode)
									.collect(Collectors.toSet());

							// Check if the current item is alrady added into the list.
							// If current is not present, then add it into buy items to keep buy items
							// unique
							// There will be multiple entries for same item in promo feed with different
							// event ids
							// Since the ad and promo created at chain level, duplicates can be ignored
							if (!itemCodes.contains(promoBuyItem.getItemCode())) {
								buyItems.add(promoBuyItem);
							}

						} else {

							promoDefinition = convertFileObjToPromoDefinition(promoFileDTO);
							List<PromoBuyItems> buyItems = new ArrayList<>();
							buyItems.add(promoBuyItem);
							logger.debug("buyItems size :" + buyItems.size());
							promoDefinition.setBuyItems(buyItems);
							setupBuyReqAndOfferDetails(promoDefinition);
						}
					}

					// Set long term promotion dates
					setActualCalendarInfoForLongTermPromo(promoDefinition, promoDefinition.getPromoStartDate(),
							promoDefinition.getPromoEndDate());
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
	 * Sets calendar id for actual start date and end date
	 * 
	 * @param promotion
	 * @throws GeneralException
	 */
	private boolean setActualCalendarInfoForLongTermPromo(PromoDefinition promotion, Date startDate, Date endDate)
			throws GeneralException {
		boolean isValidCalendarId = true;
		String promoStartDate = DateUtil.dateToString(startDate, "MM/dd/yyyy");
		String promoEndDate = DateUtil.dateToString(endDate, "MM/dd/yyyy");

		int startCalId = getCalendarId(promoStartDate);
		int endCalId = getCalendarId(promoEndDate);
		if (startCalId == 0 || endCalId == 0) {
			isValidCalendarId = false;
		}
		for (PromoBuyItems promoBuyItems : promotion.getBuyItems()) {
			promoBuyItems.setActualStartCalId(startCalId);
			promoBuyItems.setActualEndCalId(endCalId);
		}
		return isValidCalendarId;
	}

	/**
	 * Removes participating items in multiple groups
	 * 
	 * @param promoGroup
	 */
	private void handleItemsInMultiplePromoGroups(HashMap<String, PromoDefinition> promoGroup) {
		for (Map.Entry<String, PromoDefinition> promoEnrty1 : promoGroup.entrySet()) {
			Date promo1StartDate = promoEnrty1.getValue().getPromoStartDate();
			Date promo1EndDate = promoEnrty1.getValue().getPromoEndDate();
			for (Map.Entry<String, PromoDefinition> promoEnrty2 : promoGroup.entrySet()) {
				Date promoStartDate = promoEnrty2.getValue().getPromoStartDate();
				Date promoEndDate = promoEnrty2.getValue().getPromoEndDate();
				if (promoStartDate.compareTo(promo1StartDate) >= 0 && promoEndDate.compareTo(promo1EndDate) <= 0
						&& !promoEnrty1.getKey().equals(promoEnrty2.getKey())) {
					adjustBuyItemsInMultipleGroups(promoEnrty1.getValue(), promoEnrty2.getValue());
				}
			}
		}
	}

	/**
	 * Parses other data with respect to promotion and sets it in promotion object
	 * 
	 * @param promotion
	 * @param fileName
	 * @throws GeneralException
	 */
	private void setupBuyReqAndOfferDetails(PromoDefinition promotion) throws GeneralException {
		logger.debug("setupBuyReqAndOfferDetails");
		if (promotion.getPromoTypeId() == Constants.PROMO_TYPE_BXGY_SAME) {
			parseBXGY(promotion);
		} else if (promotion.getPromoTypeId() == Constants.PROMO_TYPE_MUST_BUY) {
			parseMustBuy(promotion);
		} else if (promotion.getPromoTypeId() == Constants.PROMO_TYPE_BOGO) {
			parseBXGY(promotion);
		} else if (promotion.getPromoTypeId() == Constants.PROMO_TYPE_BMSM) {
			parseBMSM(promotion);
		} else if (promotion.getPromoTypeId() == Constants.PROMO_TYPE_STANDARD) {
			parseStandard(promotion);
		} else if (promotion.getPromoTypeId() == Constants.PROMO_TYPE_DIGITAL_COUPON
				|| promotion.getPromoTypeId() == Constants.PROMO_TYPE_IN_AD_COUPON) {
			parseCoupon(promotion);
		} else if (promotion.getPromoTypeId() == Constants.PROMO_TYPE_BXGY_DIFF) {
			parseBAGB(promotion);
		}
	}

	/**
	 * Adjusts items in multiple groups
	 * 
	 * @param promotion1
	 * @param promotion2
	 */
	private void adjustBuyItemsInMultipleGroups(PromoDefinition promotion1, PromoDefinition promotion2) {
		boolean isAdjustmentRequired = false;
		if (promotion1.getLocationLevelId() == promotion2.getLocationLevelId()
				&& promotion1.getLocationId() == promotion2.getLocationId()) {
			for (PromoBuyItems buyItem1 : promotion1.getBuyItems()) {
				for (PromoBuyItems buyItem2 : promotion2.getBuyItems()) {
					if (buyItem2.getItemCode() == buyItem1.getItemCode()) {
						isAdjustmentRequired = true;
						buyItem2.setItemOverlappingInAd(true);
					}
				}
			}
		}

		if (isAdjustmentRequired) {
			List<PromoBuyItems> newItemList = new ArrayList<>();
			promotion2.getBuyItems().stream().filter(item -> !item.isItemOverlappingInAd()).forEach(promoBuyItem -> {
				newItemList.add(promoBuyItem);
			});

			if (newItemList.size() == 0) {
				logger.debug("***Buy items adjustment*** for " + promotion2.getPromoName());
				promotion2.setCanBeAdded(false);
			} else {
				promotion2.setBuyItems(newItemList);
			}
		}

	}

	/**
	 * 
	 * @param promotion
	 * @throws GeneralException
	 */
	private void parseInAdCoupon(PromoDefinition promotion) throws GeneralException {
		promotion.setPromoTypeId(Constants.PROMO_TYPE_STANDARD);
		promotion.setPromoDefnTypeId(Constants.PROMO_TYPE_STANDARD);
		PromoBuyRequirement buyReq = new PromoBuyRequirement();
		PromoOfferDetail promoOfferDetail = new PromoOfferDetail();
		promoOfferDetail.setPromoOfferTypeId(Constants.OFFER_TYPE_COUPON);
		promoOfferDetail.setOfferUnitType(Constants.OFFER_UNIT_TYPE_DOLLAR);
		double cpnAmount = Double.parseDouble(promotion.getAmountOff());
		if (cpnAmount < 100) {
			promoOfferDetail.setOfferValue(Double.parseDouble(promotion.getAmountOff()));
			buyReq.addOfferDetail(promoOfferDetail);
			promotion.addPromoBuyRequirement(buyReq);
		} else {
			promotion.setCanBeAdded(false);
			notProcessedPromotions.add("Plenti points in In Ad for Promo: " + promotion.getPromoNumber());
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
		if (promotion.getPctOff() > 0) {
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

		// promotionService.parseCouponDescAndSetAddlDetails(promotion, buyReq,
		// compiledPatterns, notProcessedCouponDesc);

		addCouponInfo(promotion, buyReq);

		buyReq.addOfferDetail(offerDetail);
		promotion.addPromoBuyRequirement(buyReq);
	}

	/**
	 * 
	 * @param promotion
	 * @throws GeneralException
	 */
	private void parseBAGB(PromoDefinition promotion) throws GeneralException {
		PromoBuyRequirement buyReq = new PromoBuyRequirement();
		PromoOfferDetail offerDetail = new PromoOfferDetail();
		logger.info("inside parse BAGB");
		ArrayList<PromoOfferItem> promoOffer = new ArrayList<>();

		logger.info("size of retailerItemCodeAndItsItem" + retailerItemCodeAndItsItem.size());
		logger.info("size of itemDetailKeyItems" + itemDetailKeyItems.size());

		if (retailerItemCodeAndItsItem.containsKey(promotion.getOfferItem())) {
			List<ItemDTO> itemcodeList = retailerItemCodeAndItsItem.get(promotion.getOfferItem());

			logger.info("itemcodeList" + itemcodeList.size());
			for (ItemDTO itemCode : itemcodeList) {

				PromoOfferItem pr = new PromoOfferItem();
				pr.setItemCode(Integer.parseInt(String.valueOf(itemCode.getItemCode())));
				promoOffer.add(pr);
				buyReq.setBuyAndGetIsSame(String.valueOf(Constants.NO));
				buyReq.setBuyX(promotion.getBuyQuantity());

				// promotionService.parseCouponDescAndSetAddlDetails(promotion, buyReq,
				// compiledPatterns, notProcessedCouponDesc);

				addCouponInfo(promotion, buyReq);

				buyReq.addOfferDetail(offerDetail);
				promotion.addPromoBuyRequirement(buyReq);
			}

			logger.info("setting offerDetails" + promoOffer.size());
			offerDetail.setOfferItems(promoOffer);
			offerDetail.setPromoOfferTypeId(Constants.OFFER_TYPE_FREE_ITEM);
			offerDetail.setOfferUnitType(Constants.OFFER_UNIT_TYPE_NUMBER);
			offerDetail.setOfferUnitCount(promotion.getGetQuantity());
		} else {
			logger.info("Itemcode not found");
			itemCodeNotFound.add(promotion.getPromoNumber() + "," + promotion.getPromoGroup() + "for offer item"
					+ promotion.getOfferItem());
			promotion.setCanBeAdded(false);
		}

	}

	/**
	 * 
	 * @param promotion
	 * @throws GeneralException
	 */
	private void parseStandard(PromoDefinition promotion) throws GeneralException {
		PromoBuyRequirement buyReq = new PromoBuyRequirement();
		PromoOfferDetail offerDetail = new PromoOfferDetail();

		boolean isOfferPresent = false;
		if (promotion.getPctOff() > 0) {
			offerDetail.setPromoOfferTypeId(Constants.OFFER_TYPE_OFF);
			offerDetail.setOfferUnitType(Constants.OFFER_UNIT_TYPE_PERCENTAGE);
			offerDetail.setOfferValue(promotion.getPctOff());
			isOfferPresent = true;
		} else if (promotion.getDollarOff() > 0) {
			offerDetail.setPromoOfferTypeId(Constants.OFFER_TYPE_OFF);
			offerDetail.setOfferUnitType(Constants.OFFER_UNIT_TYPE_DOLLAR);
			offerDetail.setOfferValue(promotion.getDollarOff());
			isOfferPresent = true;
		}

		if (isOfferPresent || addCouponInfo(promotion, buyReq)) {
			if (isOfferPresent) {
				buyReq.addOfferDetail(offerDetail);
			}
			promotion.addPromoBuyRequirement(buyReq);
		}
	}

	/**
	 * 
	 * @param promotion
	 * @throws GeneralException
	 */

	private void parseBMSM(PromoDefinition promotion) throws GeneralException {

		PromoBuyRequirement buyReq = new PromoBuyRequirement();
		PromoOfferDetail offerDetail = new PromoOfferDetail();
		boolean isOfferPresent = false;
		if (promotion.getBmsmPctoffperunit() > 0) {
			offerDetail.setPromoOfferTypeId(Constants.OFFER_TYPE_OFF);
			offerDetail.setOfferUnitType(Constants.OFFER_UNIT_TYPE_PERCENTAGE);
			offerDetail.setOfferValue(promotion.getBmsmPctoffperunit());
			buyReq.setMinQtyReqd(promotion.getMinimumQty());
			isOfferPresent = true;
		} else if (promotion.getBmsmDollaroffperunits() > 0) {
			offerDetail.setPromoOfferTypeId(Constants.OFFER_TYPE_OFF);
			offerDetail.setOfferUnitType(Constants.OFFER_UNIT_TYPE_DOLLAR);
			offerDetail.setOfferValue(promotion.getBmsmDollaroffperunits());
			buyReq.setMinQtyReqd(promotion.getMinimumQty());
			isOfferPresent = true;
		} else if (promotion.getBmsmsalePrice() > 0) {
			buyReq.setMinQtyReqd(promotion.getBmsmsaleQty());
			isOfferPresent = false;
			promotion.addPromoBuyRequirement(buyReq);
		}

		addCouponInfo(promotion, buyReq);

		if (isOfferPresent) {
			buyReq.addOfferDetail(offerDetail);
			promotion.addPromoBuyRequirement(buyReq);
		}

		// promotionService.parseCouponDescAndSetAddlDetails(promotion, buyReq,
		// compiledPatterns, notProcessedCouponDesc);

	}

	/**
	 * 
	 * @param promotion
	 * @throws GeneralException
	 */
	private void parseMustBuy(PromoDefinition promotion) throws GeneralException {
		PromoBuyRequirement buyReq = new PromoBuyRequirement();
		buyReq.setMustBuyQty(promotion.getNoOfItems());
		String oStr = promotion.getRetailForPurchase();
		if (oStr != null)
		{
			buyReq.setMustBuyAmt(Double.parseDouble(oStr));
			buyReq.setBuyX(promotion.getNoOfItems());
		}
		/*buyReq.setBuyX(promot);*/
		else {
			logger.error("Must Buy Amount cannot be parsed");
			promotion.setCanBeAdded(false);
			notProcessedPromotions.add("Unable to process Must Buy Amount for Promo: " + promotion.getPromoNumber());
			return;
		}
		// promotionService.parseCouponDescAndSetAddlDetails(promotion, buyReq,
		// compiledPatterns, notProcessedCouponDesc);
		addCouponInfo(promotion, buyReq);

		promotion.addPromoBuyRequirement(buyReq);
	}

	/**
	 * 
	 * @param promoFileDTO
	 * @return promotion defintion object
	 * @throws GeneralException
	 */
	private PromoDefinition convertFileObjToPromoDefinition(PromoDataStandardDTO promoFileDTO) throws GeneralException {
		PromoDefinition promoDefinition = new PromoDefinition();

		logger.debug(" In convertFileObjToPromoDefinition");

		// Set Promo start and end date
		promoDefinition.setPromoStartDate(DateUtil.toDate(promoFileDTO.getPromoStartDate()));
		promoDefinition.setPromoEndDate(DateUtil.toDate(promoFileDTO.getPromoEndDate()));

		// Set page and block numbers for referrence
		promoDefinition.setBlockNum(String.valueOf(promoFileDTO.getBlockNumber()));
		promoDefinition.setAdpage(String.valueOf(promoFileDTO.getPageNumber()));

		// Set promo number and name

		promoDefinition.setPromoNumber(promoFileDTO.getPromoID());

		if (Constants.EMPTY.equals(promoFileDTO.getPromoDescription()) || promoFileDTO.getPromoDescription() == null) {

			promoDefinition.setPromoName(promoFileDTO.getItemName());
		} else {

			promoDefinition.setPromoName(promoFileDTO.getPromoDescription());
		}

		promoDefinition.setMinimumQty(promoFileDTO.getMinimumQty());
		promoDefinition.setDollarOff(promoFileDTO.getDollarOff());
		promoDefinition.setBmsmDollaroffperunits(promoFileDTO.getBmsmDollaroffperunits());
		promoDefinition.setBmsmPctoffperunit(promoFileDTO.getBmsmPctoffperunit());
		promoDefinition.setBmsmsalePrice(promoFileDTO.getBmsmsalePrice());
		promoDefinition.setBmsmsaleQty(promoFileDTO.getBmsmsaleQty());
		/*
		 * //Set coupon description and coupon amount
		 * promoDefinition.setCouponDesc(promoFileDTO.getCpnDesc());
		 * promoDefinition.setAmountOff(String.valueOf(promoFileDTO.getCpnAmount()));
		 */

		promoDefinition.setPromoGroup(promoFileDTO.getPromoGroup());
		// Set buy qty and get qty

		// Set pct off and price code
		promoDefinition.setPctOff(promoFileDTO.getPctOff());

		// promoDefinition.setItemPriceCode(promoFileDTO.getItemPriceCode());

		// For must buy, set # of items required to get the deal
		promoDefinition.setNoOfItems(promoFileDTO.getMustBuyQty());
		promoDefinition.setRetailForPurchase(String.valueOf(promoFileDTO.getMustbuyPrice()));

		promoDefinition.setApprovedBy(Constants.BATCH_USER);
		promoDefinition.setCreatedBy(Constants.BATCH_USER);

		promoDefinition.setSalePrice(promoFileDTO.getSalePrice());
		promoDefinition.setSaleQty(promoFileDTO.getSaleQty());

		promoDefinition.setUPC(promoFileDTO.getUpc());
		promoDefinition.setItemCode(Integer.parseInt(promoFileDTO.getItemCode()));

		/*
		 * promoDefinition.setThresholdValue(promoFileDTO.getThresholdValue());
		 * promoDefinition.setThresholdType(promoFileDTO.getThresholdType());
		 */
		// Set location
		setLocation(promoDefinition, promoFileDTO);

		promoDefinition.setBuyQuantity(promoFileDTO.getBuyQty());
		promoDefinition.setGetQuantity(promoFileDTO.getGetQty());

		// Coupon type
		promoDefinition.setCouponDesc(promoFileDTO.getCouponType());
		promoDefinition.setAmountOff(String.valueOf(promoFileDTO.getCouponAmt()));

		// set offeritem

		promoDefinition.setOfferItem(promoFileDTO.getAnotherItem());

		// Set promotion type
		promotionService.identifyAndSetPromotionType(promoFileDTO, promoDefinition);

		if (!promoDefinition.isCanBeAdded()) {
			notProcessedPromotions.add("Unknown promo type: " + promoDefinition.getPromoNumber());
		}

		// Set start and end calendar ids
		setCalendarId(promoDefinition);
		logger.debug(" complete  convertFileObjToPromoDefinition");

		return promoDefinition;
	}

	/**
	 * 
	 * @param promotion
	 * @throws GeneralException
	 */
	/*
	 * private void parseEBonusCoupon(PromoDefinition promotion) throws
	 * GeneralException { promotion.setPromoTypeId(Constants.PROMO_TYPE_STANDARD);
	 * promotion.setPromoDefnTypeId(Constants.PROMO_TYPE_STANDARD);
	 * PromoBuyRequirement buyReq = new PromoBuyRequirement();
	 * promotionService.parseCouponDescAndSetAddlDetails(promotion, buyReq,
	 * compiledPatterns, notProcessedCouponDesc);
	 * promotion.addPromoBuyRequirement(buyReq); }
	 */

	/**
	 * Parses location information from file name and sets it in promotion object
	 * 
	 * @param promotion
	 * @throws GeneralException
	 */
	private void setLocation(PromoDefinition promotion, PromoDataStandardDTO promoFileDTO) {

		PromoLocation location = new PromoLocation();

		if (promoFileDTO.getLocationLevel().toUpperCase().equals(Constants.ZONE)) {

			String zoneNo = PrestoUtil.castZoneNumber(promoFileDTO.getLocationNo());
			if (zoneMap.containsKey(zoneNo)) {
				location.setLocationLevelId(Constants.ZONE_LEVEL_ID);
				location.setLocationId(zoneMap.get(zoneNo));

			}
		} else if (promoFileDTO.getLocationLevel().toUpperCase().equals(Constants.CHAIN)) {
			location.setLocationLevelId(Constants.CHAIN_LEVEL_ID);
			location.setLocationId(chainId);
		} else if (promoFileDTO.getLocationLevel().toUpperCase().equals(Constants.DIVISION)) {
			if (divisionNameMap.containsKey(promoFileDTO.getLocationNo())) {
				location.setLocationLevelId(Constants.DIVISION_LEVEL_ID);
				// weeklyAd.setLocationId(divisionNameMap.get(sampleObj.getLocationNo()));
			}
		} else if (promoFileDTO.getLocationLevel().toUpperCase().equals(Constants.STORELEVEL)) {
			if (storeIdMap.containsKey(promoFileDTO.getLocationNo())) {
				location.setLocationLevelId(Constants.STORE_LEVEL_ID);
				location.setLocationId(storeIdMap.get(promoFileDTO.getLocationNo()));
			}
		} else if (Constants.EMPTY.equals(promoFileDTO.getLocationLevel())) {
			if (zoneMap.containsKey(promoFileDTO.getLocationNo())) {
				location.setLocationLevelId(Constants.ZONE_LEVEL_ID);
				location.setLocationId(zoneMap.get(promoFileDTO.getLocationNo()));
			}
		} else {
			locationNotfound
					.add("location not found for :" + promoFileDTO.getPromoID() + ";" + promoFileDTO.getLocationNo());
		}
		promotion.setLocationLevelId(location.getLocationLevelId());
		promotion.setLocationId(location.getLocationId());
		promotion.addPromoLocation(location);

	}

	/**
	 * 
	 * @param promoFileDTO
	 * @param isInAd
	 * @return promo buy item object
	 * @throws GeneralException
	 */
	private List<PromoBuyItems> setupPromoBuyItems(PromoDataStandardDTO promoFileDTO, boolean isInAd)
			throws GeneralException {
		List<PromoBuyItems> itemList = new ArrayList<>();

		ItemDetailKey itemKey = new ItemDetailKey(PrestoUtil.castUPC(promoFileDTO.getUpc(), false),
				promoFileDTO.getItemCode());
		try {
			Set<String> allMatchingItems = new HashSet<>();

			// File contains only UPC and no retailer item code. Get all matching items for
			// UPC
			if (upcAndItsItem.containsKey(PrestoUtil.castUPC(promoFileDTO.getUpc(), false))
					&& (Constants.EMPTY.equals(promoFileDTO.getItemCode()) || promoFileDTO.getItemCode() == null)) {

				List<ItemDTO> items = upcAndItsItem.get(PrestoUtil.castUPC(promoFileDTO.getUpc(), false));
				for (ItemDTO item : items) {
					allMatchingItems.add(String.valueOf(item.itemCode));
				}

			}
			// File contains only retailer item code and no UPC. Get all matching items for
			// retailer item code
			if (retailerItemCodeAndItsItem.containsKey(promoFileDTO.getItemCode())
					&& (Constants.EMPTY.equals(promoFileDTO.getUpc()) || promoFileDTO.getItemCode() == null)) {
				List<ItemDTO> items = retailerItemCodeAndItsItem.get(promoFileDTO.getItemCode());

				for (ItemDTO item : items) {

					allMatchingItems.add(String.valueOf(item.itemCode));
				}

			}

			// File contains both retailer item code and UPC. Get matching item for
			// combination
			if (itemDetailKeyItems.containsKey(itemKey)) {

				allMatchingItems.add(String.valueOf(itemDetailKeyItems.get(itemKey)));
			}

			if (allMatchingItems.size() == 0) {

				itemCodeNotFound.add("Orig. Item Number: " + promoFileDTO.getItemCode() + "Curr. Item Number: "
						+ promoFileDTO.getItemCode());
			}

			for (String itemCode : allMatchingItems) {
				PromoBuyItems promoBuyItems = new PromoBuyItems();
				promoFileDTO.setUpc(PrestoUtil.castUPC(promoFileDTO.getUpc(), false));
				promoBuyItems.setUpc(promoFileDTO.getUpc());
				promoBuyItems.setRegPrice(Double.parseDouble(promoFileDTO.getEverydayPrice()));
				promoBuyItems.setRegQty(Integer.parseInt(promoFileDTO.getEverdayQty()));
				promoBuyItems.setItemCode(Integer.parseInt(itemCode));
				promotionService.setPromoItemsSalePrice(promoBuyItems, promoFileDTO);

				/*
				 * promoBuyItems.setBillbackCost(promoFileDTO.getScanBillBackAllowance());
				 * promoBuyItems.setOffInvoiceCost(promoFileDTO.getOffInvoiceAmt());
				 */
				int weekCalendarId = -1;

				/*
				 * if (calendarMap.get(inputDate) == null) { int startCalId = 0; String calType
				 * = PropertyManager.getProperty("RETAIL_CALENDAR_TYPE",
				 * Constants.RETAIL_CALENDAR_BUSINESS); if
				 * (calType.equals(Constants.RETAIL_CALENDAR_PROMO)) { startCalId =
				 * ((RetailCalendarDTO) calDAO.getPromocalId(conn, inputDate,
				 * Constants.CALENDAR_DAY)) .getCalendarId(); } else { startCalId =
				 * ((RetailCalendarDTO) calDAO.getCalendarId(conn, inputDate,
				 * Constants.CALENDAR_DAY)) .getCalendarId(); }
				 * logger.info("getCalendarId returns  " + startCalId);
				 * 
				 * calendarMap.put(inputDate, startCalId); } return calendarMap.get(inputDate);
				 */

				if (weekCalendarMap.get(promoFileDTO.getPromoStartDate()) == null) {

					int startCalId = 0;
					String calType = PropertyManager.getProperty("RETAIL_CALENDAR_TYPE",
							Constants.RETAIL_CALENDAR_BUSINESS);
					if (calType.equals(Constants.RETAIL_CALENDAR_PROMO)) {
						startCalId = ((RetailCalendarDTO) calDAO.getPromocalId(conn, promoFileDTO.getPromoStartDate(),
								Constants.CALENDAR_WEEK)).getCalendarId();
					} else {
						startCalId = ((RetailCalendarDTO) calDAO.getCalendarId(conn, promoFileDTO.getPromoStartDate(),
								Constants.CALENDAR_WEEK)).getCalendarId();
					}

					weekCalendarMap.put(promoFileDTO.getPromoStartDate(), startCalId);
					weekCalendarId = startCalId;
				} else
					weekCalendarId = weekCalendarMap.get(promoFileDTO.getPromoStartDate());

				// Set ad flag
				promoBuyItems.setIsInAd(isInAd ? String.valueOf(Constants.YES) : String.valueOf(Constants.NO));

				itemList.add(promoBuyItems);
			}

		} catch (GeneralException | Exception e) {
			throw new GeneralException("setupPromoBuyItemObj() - Error while setting up promo buy item", e);
		}

		return itemList;
	}

	/**
	 * Sets calendar id in promotion object from promo start and end date
	 * 
	 * @param promotion
	 * @throws GeneralException
	 */
	private void setCalendarId(PromoDefinition promoDTO) throws GeneralException {
		String promoStartDate = DateUtil.dateToString(promoDTO.getPromoStartDate(), "MM/dd/yyyy");
		String promoEndDate = DateUtil.dateToString(promoDTO.getPromoEndDate(), "MM/dd/yyyy");

		promoDTO.setWeekStartDate(promoStartDate);

		// Get and set start calendar id
		promoDTO.setStartCalId(getCalendarId(promoStartDate));
		if (promoDTO.getStartCalId() == 0) {
			logger.debug("Promo Start date: " + promoStartDate);
		}
		// Get and set end calendar id
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
			int startCalId = 0;
			String calType = PropertyManager.getProperty("RETAIL_CALENDAR_TYPE", Constants.RETAIL_CALENDAR_BUSINESS);
			if (calType.equals(Constants.RETAIL_CALENDAR_PROMO)) {
				startCalId = ((RetailCalendarDTO) calDAO.getPromocalId(conn, inputDate, Constants.CALENDAR_DAY))
						.getCalendarId();
			} else {
				startCalId = ((RetailCalendarDTO) calDAO.getCalendarId(conn, inputDate, Constants.CALENDAR_DAY))
						.getCalendarId();
			}
			logger.debug("getCalendarId returns  " + startCalId);

			calendarMap.put(inputDate, startCalId);
		}
		return calendarMap.get(inputDate);
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
	 * 
	 * @param promoDefinition
	 * @param promoBuyRequirement
	 */
	private boolean addCouponInfo(PromoDefinition promoDefinition, PromoBuyRequirement promoBuyRequirement) {
		PromoOfferDetail offerDetail = new PromoOfferDetail();
		boolean isCouponPresent = false;
		if (promoDefinition.getCouponDesc() != null && !Constants.EMPTY.equals(promoDefinition.getCouponDesc())
				&& promoDefinition.getAmountOff() != null && !Constants.EMPTY.equals(promoDefinition.getAmountOff())) {
			double amtOff = Double.parseDouble(promoDefinition.getAmountOff());
			if (promoDefinition.getCouponDesc().toUpperCase().equals(Constants.DIGITAL_COUPON)) {
				isCouponPresent = true;
				offerDetail.setPromoOfferTypeId(PromoOfferTypeLookup.DIGITAL_COUPON.getPromoOfferTypeId());
				offerDetail.setOfferUnitType(Constants.OFFER_UNIT_TYPE_DOLLAR);
				offerDetail.setOfferValue(amtOff);
			} else if (promoDefinition.getCouponDesc().toUpperCase().equals(Constants.NORMAL_COUPON)) {
				isCouponPresent = true;
				offerDetail.setPromoOfferTypeId(PromoOfferTypeLookup.COUPON.getPromoOfferTypeId());
				offerDetail.setOfferUnitType(Constants.OFFER_UNIT_TYPE_DOLLAR);
				offerDetail.setOfferValue(amtOff);
			} else if (promoDefinition.getCouponDesc().toUpperCase().equals(Constants.AD_COUPON)) {
				isCouponPresent = true;
				offerDetail.setPromoOfferTypeId(PromoOfferTypeLookup.COUPON.getPromoOfferTypeId());
				offerDetail.setOfferUnitType(Constants.OFFER_UNIT_TYPE_DOLLAR);
				offerDetail.setOfferValue(amtOff);
			}
		}

		if (isCouponPresent) {
			promoBuyRequirement.addOfferDetail(offerDetail);
		}

		return isCouponPresent;
	}

	/**
	 * 
	 * @param promotion
	 * @throws GeneralException
	 */
	private void parseCoupon(PromoDefinition promotion) throws GeneralException {
		PromoBuyRequirement buyReq = new PromoBuyRequirement();
		if (addCouponInfo(promotion, buyReq)) {
			promotion.addPromoBuyRequirement(buyReq);
		}
	}

}
