package com.pristine.dataload.tops;

import java.io.File;
import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.sql.Connection;
import java.sql.Types;
import java.text.DateFormat;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Calendar;
import java.util.Date;
import java.util.HashMap;
import java.util.List;
import java.util.TreeMap;
import java.util.concurrent.TimeUnit;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import org.apache.log4j.Logger;
import org.apache.log4j.PropertyConfigurator;

import com.pristine.dao.DBManager;
import com.pristine.dao.ItemDAO;
import com.pristine.dao.LocationGroupDAO;
import com.pristine.dao.RetailCalendarDAO;
import com.pristine.dao.RetailCostDAO;
import com.pristine.dao.RetailPriceDAO;
import com.pristine.dao.offermgmt.promotion.PromotionDAO;
import com.pristine.dao.offermgmt.weeklyad.WeeklyAdDAO;
import com.pristine.dto.ItemDetailKey;
import com.pristine.dto.RetailCalendarDTO;
import com.pristine.dto.RetailCostDTO;
import com.pristine.dto.RetailPriceDTO;
import com.pristine.dto.offermgmt.promotion.PromoBuyItems;
import com.pristine.dto.offermgmt.promotion.PromoBuyRequirement;
import com.pristine.dto.offermgmt.promotion.PromoDefinition;
import com.pristine.dto.offermgmt.promotion.PromoLocation;
import com.pristine.dto.offermgmt.promotion.PromoOfferDetail;
import com.pristine.dto.offermgmt.promotion.PromoOfferItem;
import com.pristine.exception.GeneralException;
import com.pristine.lookup.offermgmt.promotion.PromoOfferTypeLookup;
import com.pristine.lookup.offermgmt.promotion.PromoTypeLookup;
import com.pristine.parsinginterface.ExcelFileParser;
import com.pristine.util.Constants;
import com.pristine.util.DateUtil;
import com.pristine.util.PrestoUtil;
import com.pristine.util.PristineDBUtil;
import com.pristine.util.PropertyManager;

public class LoadOverlays {
	private static Logger logger = Logger.getLogger("LoadOverlays");
	private Connection _Conn = null;

	private String rootPath = null;

	private PromotionDAO promoDAO = null;
	private ExcelFileParser parser = null;
	private RetailPriceDAO priceDAO = null;
	private RetailCostDAO costDAO = null;
	private RetailCalendarDAO calendarDAO = null;
	private WeeklyAdDAO adDAO = null;
	
	private HashMap<String, Integer> locationIdMap = new HashMap<String, Integer>();
	HashMap<ItemDetailKey, String> itemCodeMap = null;
	DateFormat formatterMMddyy = new SimpleDateFormat("MM/dd/yy");
	private Pattern pattern1 = null;
	private Pattern pattern2 = null;
	private Pattern pattern3 = null;
	private Pattern pattern4 = null;
	private Pattern pattern5 = null;
	private Pattern pattern6 = null;
	private Pattern pattern7 = null;
	private Pattern pattern8 = null;
	private Pattern pattern9 = null;
	private Pattern pattern10 = null;
	private Pattern pattern11 = null;
	private Pattern pattern12 = null;
	private Pattern pattern13 = null;
	private Pattern pattern14 = null;
	HashMap<String, Integer> weekCalendarMap = new HashMap<String, Integer>();
	private final static String OVERLAY_TYPE_IDS = "OVERLAY_TYPE_IDS=";
	private final static String OVERLAY_FILE_RELATIVE_PATH="OVERLAY_FILE_RELATIVE_PATH=";
	private static String[] overlaysTypeIds = null;
	/**
	 * Constructor
	 */
	public LoadOverlays() {
		rootPath = PropertyManager.getProperty("DATALOAD.ROOTPATH", "");
		try {
			_Conn = DBManager.getConnection();
		} catch (GeneralException gex) {
			logger.error("Error when creating connection - " + gex);
		}

		promoDAO = new PromotionDAO(_Conn);
		parser = new ExcelFileParser();
		priceDAO = new RetailPriceDAO();
		costDAO = new RetailCostDAO();
		calendarDAO = new RetailCalendarDAO();
		adDAO = new WeeklyAdDAO(_Conn);

		//[] - match any character
		//{m} - The preceding element or subexpression must occur exactly m times.
		//\d - any digits
		//+ - more than one appearance
		//D - any non-digit
		//?i: ignore casing
		pattern1 = Pattern.compile("([$]{1}\\d+[.]\\d+)");
		pattern2 = Pattern.compile("([$]{1}\\d+)");
		pattern3 = Pattern.compile("(\\d+[.]\\d+)");
		pattern4 = Pattern.compile("(\\d+)");
		pattern5 = Pattern.compile("([$]{1}\\d+[.]\\d+[O]{1}[F]{1}[F]{1}\\d+)");
		pattern6 = Pattern.compile("([$]{1}\\d+[O]{1}[F]{1}[F]{1}\\d+)");
		
		//$2.00 off or $2 off
		pattern7 = Pattern.compile("([$]{1}\\d+\\.?\\d*)\\s((?i:Off{1}))");
		
		//2.00 off or 2 off
		pattern8 = Pattern.compile("(\\d+\\.?\\d*)\\s((?i:Off{1}))");
		
		//$1.00 off 2 or $1 off 2
		pattern9 = Pattern.compile("([$]{1}\\d+\\.?\\d*)\\s((?i:Off{1}))\\s(\\d+)");
		
		//1.00 off 2 or 1 off 2
		pattern10 = Pattern.compile("(\\d+\\.?\\d*)\\s((?i:Off{1}))\\s(\\d+)");
		
		//$20.00 purchase or $20 purchase
		pattern11 = Pattern.compile("([$]{1}\\d+\\.?\\d*)\\s((?i:purchase{1}))");
		//20.00 purchase or 20 purchase
		pattern12 = Pattern.compile("(\\d+\\.?\\d*)\\s((?i:purchase{1}))");
		
		//$20.00 minimum purchase or $20 minimum purchase
		pattern13 = Pattern.compile("([$]{1}\\d+\\.?\\d*)\\s((?i:minimum purchase{1}))");
		//20.00 minimum purchase or 20 minimum purchase
		pattern14 = Pattern.compile("(\\d+\\.?\\d*)\\s((?i:minimum purchase{1}))");
	}

	/**
	 * Main method
	 * 
	 * @param args[0]
	 * inputFilePath
	 * @throws Exception
	 * @throws GeneralException 
	 */
	public static void main(String[] args){
		PropertyConfigurator.configure("log4j-load-overlays.properties");
		PropertyManager.initialize("analysis.properties");

		String relativePath = null;
		for (String arg : args) {
			if(arg.startsWith(OVERLAY_FILE_RELATIVE_PATH)){
				relativePath = arg.substring(OVERLAY_FILE_RELATIVE_PATH.length());
			}
			if(arg.startsWith(OVERLAY_TYPE_IDS)){
				String promoTypeId=arg.substring(OVERLAY_TYPE_IDS.length());
				overlaysTypeIds = promoTypeId.split(",");
			}

		}
		if (relativePath == null) {
			logger.error("Invalid input - Input File Path is mandatory");		
		} 
		
		LoadOverlays overlays = new LoadOverlays();

		try {
			overlays.loadPromotion(relativePath);
		} catch (ParseException e) {
			e.printStackTrace();
		} catch (IOException e) {
			e.printStackTrace();
		} catch (GeneralException e) {
			e.printStackTrace();
		}
	}
	
	/**
	 * Loads promotion data
	 * 
	 * @param relativePath
	 * @throws ParseException 
	 * @throws GeneralException 
	 * @throws IOException 
	 */
	public void loadPromotion(String relativePath) throws ParseException, GeneralException, IOException {
		String archivePath = rootPath + "/" + relativePath + "/";
		String fName = null;
		String inputFilePath = rootPath + "/" + relativePath;// + "/";
		String tempFolderPath = rootPath + "/" + relativePath + "/temp";

		// create temp folder if not exists
		PrestoUtil.createDirIfNotExists(tempFolderPath);
		// Get all the zip files from the input folder
		ArrayList<String> zipFileList = PrestoUtil.getAllFilesInADirectory(inputFilePath, "zip");
		// Loop the zip files to find Super coupon
		for (String zipFile : zipFileList) {
			try {
				logger.info("Processing of file: " + zipFile + " is Started...");
				// unzip the file in a temp folder
				PrestoUtil.unzipIncludingSubDirectories(zipFile, tempFolderPath);
				File sourceFile =new File(tempFolderPath);
				File finalPath = new File(inputFilePath);
				//To copy all files from given Directory
				copyFilesFromDir(sourceFile, finalPath);
				//Delete the temp folder created
				deleteFilesAndDir(sourceFile);
				PrestoUtil.moveFile(zipFile, inputFilePath + "/" + Constants.COMPLETED_FOLDER);
				
			}catch (GeneralException | Exception ge) {
				ge.printStackTrace();
				logger.error("Error while processing Super Coupon zip file - " + zipFile + "---" + ge);
				
				PrestoUtil.moveFile(zipFile, inputFilePath + "/" + Constants.BAD_FOLDER);
			}
		}
		TreeMap<Integer, TreeMap<String, String>> fieldNames = new TreeMap<Integer, TreeMap<String, String>>();
		mapFieldNames(fieldNames);

		try {
			// Retrieve all items
			// Changes by Pradeep on 07/09/2014.
			long startTime = System.currentTimeMillis();
			ItemDAO itemDAO = new ItemDAO();
			 itemCodeMap = new HashMap<ItemDetailKey, String>();

			try {
				itemCodeMap = itemDAO.getAllItemsFromItemLookupV2(_Conn);
			} catch (GeneralException e) {
				e.printStackTrace();
			}
			long endTime = System.currentTimeMillis();
			logger.debug(
					"setupObjects() - Time taken to cache all items from item lookup- " + ((endTime - startTime) / 1000) + " seconds");
		
			@SuppressWarnings("unchecked")
			ArrayList<String> overlaysFileList = parser.getFiles(relativePath);
			for (String overlayFile : overlaysFileList) {
				boolean isFileDeleted = false;
				int currentFilePromoId = 0;
				try {
					fName = overlayFile;
					PromoDefinition promotion = null;
					// To delete other coupons other Super coupon
					try {
						// Delete File if File parser throws error or Promo type
						// Id is not Matching with given Overlays Type id
						promotion = (PromoDefinition) parser.parsePromoOverview(PromoDefinition.class, fName,
								fieldNames);
						// If overlay type given to process or Promo type id is
						// equals to zero then delete file
						currentFilePromoId = promotion.getPromoTypeId();
						if (overlaysTypeIds != null || promotion == null) {
							for (String promoTypeId : overlaysTypeIds) {

								if (promotion == null ||(!Integer.valueOf(promoTypeId).equals(promotion.getPromoTypeId()))) {
									logger.info("Deleting File due to PromoTypeId not matching- " + fName);
									File file = new File(fName);
									deleteFilesAndDir(file);
									isFileDeleted = true;
								}
							}
						}

					} catch (GeneralException e) {
						// If parser throws error then delete file
						logger.info("Deleting File which can't able to Parse - " + fName);
						File file = new File(fName);
						deleteFilesAndDir(file);
						isFileDeleted = true;
					}
					if (!isFileDeleted) {
						// to update Actual Start Date and End date of Super
						// Coupon
						logger.info("*** Processing file - " + overlayFile);
						setSuperCouponCalendarId(promotion);
						long noOfDays = updateNoOfDaysInPromoDuration(promotion);

						int noOfWeeksToProcess = (int) Math.ceil((double) noOfDays / 7);
						Date startDate;
						Date endDate;
						// To handle multiple weeks
						parseOtherData(promotion, fName);
						for (int i = 1; i <= noOfWeeksToProcess; i++) {
							startDate = promotion.getPromoStartDate();
							endDate = promotion.getPromoEndDate();
							// First the start date will be same but end date
							// needs to be updated..
							if (i == 1) {
								// To get Current End date using Start Date
								endDate = getCurrentWeekEndDate(startDate);
								promotion.setPromoEndDate(endDate);
							}
							// For Next week the start date and End date needs
							// to be changed using Current week dates..
							if (i > 1) {
								startDate = getNextWeekDate(startDate);
								endDate = getCurrentWeekEndDate(startDate);
								promotion.setPromoStartDate(startDate);
								promotion.setPromoEndDate(endDate);
							}
							setCalendarId(promotion);

							if (promotion.isCanBeAdded()) {
								promoDAO.savePromotion(promotion);
							}
						}
						PristineDBUtil.commitTransaction(_Conn, "Super coupon data load Commit");
						PrestoUtil.moveFile(fName, archivePath + Constants.COMPLETED_FOLDER);
						logger.info("*** End of Process - " + fName);
					}
				} catch (GeneralException ge) {
					logger.error("Error when parsing input file - " + fName);
					String unProcessedFolder = archivePath + "/" + "UnProcessed" + "/"
							+ DateUtil.dateToString(new Date(), "MM-dd-yyyy") + "/" + currentFilePromoId;
					PrestoUtil.createDirIfNotExists(unProcessedFolder);
					PrestoUtil.moveFile(fName, unProcessedFolder);
					PristineDBUtil.rollbackTransaction(_Conn, "Promo data load Rollback");
				}
			}
		} finally {
			PristineDBUtil.close(_Conn);
		}
	}
	
	
	/**
	 * Parses other data with respect to promotion and sets it in promotion
	 * object
	 * 
	 * @param promotion
	 * @param fileName
	 * @throws GeneralException
	 */
	private void parseOtherData(PromoDefinition promotion, String fileName) throws GeneralException {
		parseLocation(promotion, fileName);
		if (promotion.getPromoTypeId() == PromoTypeLookup.BUX_X_GET_Y_SAME.getPromoTypeId()) {
			logger.info("Promo Name - " + promotion.getPromoName() + "\t" + "Promo Type - BXGY");
			parseBXGY(promotion, fileName);
		} else if (promotion.getPromoTypeId() == PromoTypeLookup.MUST_BUY.getPromoTypeId()) {
			logger.info("Promo Name - " + promotion.getPromoName() + "\t" + "Promo Type - Must Buy");
			parseMustBuy(promotion, fileName);
		} else if (promotion.getPromoTypeId() == Constants.PROMO_TYPE_CATALINA) {
			logger.info("Promo Name - " + promotion.getPromoName() + "\t" + "Promo Type - Catalina/Instant Savings");
			parseCatalina(promotion, fileName);
		} else if (promotion.getPromoTypeId() == PromoTypeLookup.SUPER_COUPON.getPromoTypeId()) {
			logger.info("Promo Name - " + promotion.getPromoName() + "\t" + "Promo Type - Super Coupon");
			parseSuperCoupon(promotion, fileName);
		} else if (promotion.getPromoTypeId() == Constants.PROMO_TYPE_EBONUS_COUPON) {
			logger.info("Promo Name - " + promotion.getPromoName() + "\t" + "Promo Type - e-Bonus Coupon");
			parseEBonusCoupon(promotion, fileName);
		} else if (promotion.getPromoTypeId() == PromoTypeLookup.MEAL_DEAL.getPromoTypeId()) {
			logger.info("Promo Name - " + promotion.getPromoName() + "\t" + "Promo Type - Meal Deal");
			parseMealDeal(promotion, fileName);
		} else if (promotion.getPromoTypeId() == Constants.PROMO_TYPE_GAS_POINTS) {
			logger.info("Promo Name - " + promotion.getPromoName() + "\t" + "Promo Type - Gas Points Program");
			parseGasPoints(promotion, fileName);
		}

		promotion.setCreatedBy(Constants.BATCH_USER);
		promotion.setApprovedBy(Constants.BATCH_USER);
	}

	/**
	 * Parses location information from file name and sets it in promotion
	 * object
	 * 
	 * @param promotion
	 * @param fileName
	 * @throws GeneralException
	 */
	private void parseLocation(PromoDefinition promotion, String fileName) throws GeneralException {
		boolean isGU = false;
		boolean isTOPS = false;
		LocationGroupDAO locationGroupDAO = new LocationGroupDAO(_Conn);
		PromoLocation location = new PromoLocation();
		String tFileName = fileName.toUpperCase();
		if (tFileName.contains("GU") || tFileName.contains("GRANDUNION") || tFileName.contains("GRAND UNION")) {
			isGU = true;
		}
		if (tFileName.contains("TOPS")) {
			isTOPS = true;
		}

		if ((!isGU && !isTOPS) || (isGU && isTOPS)) {
			location.setLocationLevelId(Constants.CHAIN_LEVEL_ID);
			if (locationIdMap.get(Constants.CHAIN) != null) {
				location.setLocationId(locationIdMap.get(Constants.CHAIN));
			} else {
				int locationId = Integer.parseInt(priceDAO.getChainId(_Conn));
				location.setLocationId(locationId);
				locationIdMap.put(Constants.CHAIN, locationId);
			}
		} else if (isGU && !isTOPS) {
			location.setLocationLevelId(Constants.STORE_LIST_LEVEL_ID);
			if (locationIdMap.get(Constants.GU_STORES) != null) {
				location.setLocationId(locationIdMap.get(Constants.GU_STORES));
			} else {
				int locationId = locationGroupDAO.getLocationId(Constants.GU_STORES);
				location.setLocationId(locationId);
				locationIdMap.put(Constants.GU_STORES, locationId);
			}
		} else if (!isGU && isTOPS) {
			location.setLocationLevelId(Constants.STORE_LIST_LEVEL_ID);
			if (locationIdMap.get(Constants.TOPS_STORES) != null) {
				location.setLocationId(locationIdMap.get(Constants.TOPS_STORES));
			} else {
				int locationId = locationGroupDAO.getLocationId(Constants.TOPS_STORES);
				location.setLocationId(locationId);
				locationIdMap.put(Constants.TOPS_STORES, locationId);
			}
		}

		promotion.addPromoLocation(location);
		logger.info("Location Level Id - " + location.getLocationLevelId() + "\t" + "Location Id - "
				+ location.getLocationId());
	}

	/**
	 * Parses BXGY types of promotion
	 * 
	 * @param promotion
	 * @param fileName
	 * @throws GeneralException
	 */
	private void parseBXGY(PromoDefinition promotion, String fileName) throws GeneralException {
		boolean patternFound = false;
		PromoBuyRequirement buyReq = new PromoBuyRequirement();
		PromoOfferDetail offerDetail = new PromoOfferDetail();
		offerDetail.setPromoOfferTypeId(Constants.OFFER_TYPE_FREE_ITEM);
		offerDetail.setOfferUnitType(Constants.OFFER_UNIT_TYPE_NUMBER);

		String tFileName = fileName.replaceAll(" ", "").toUpperCase();
		Pattern pattern1 = Pattern.compile("([B]{1}[U]{1}[Y]{1}\\d+[G]{1}[E]{1}[T]{1}\\d+)");
		Pattern pattern2 = Pattern.compile("([B]{1}\\d+[G]{1}\\d+)");
		Matcher matcher = pattern1.matcher(tFileName);
		if (matcher.find()) {
			patternFound = true;
			String promoDef = matcher.group(0).toUpperCase();
			buyReq.setBuyAndGetIsSame(String.valueOf(Constants.YES));
			buyReq.setBuyX(Integer.parseInt(promoDef.substring(promoDef.indexOf("Y") + 1, promoDef.indexOf("G") - 1)));

			offerDetail.setOfferUnitCount(
					Integer.parseInt(promoDef.substring(promoDef.indexOf("T") + 1, promoDef.length())));
		}
		matcher = pattern2.matcher(tFileName);
		if (matcher.find()) {
			patternFound = true;
			String promoDef = matcher.group(0);
			buyReq.setBuyAndGetIsSame(String.valueOf(Constants.YES));
			buyReq.setBuyX(Integer.parseInt(promoDef.substring(promoDef.indexOf("B") + 1, promoDef.indexOf("G"))));

			offerDetail.setOfferUnitCount(
					Integer.parseInt(promoDef.substring(promoDef.indexOf("G") + 1, promoDef.length())));
		}

		logger.info("Min Qty Reqd  - " + buyReq.getBuyX());
		logger.info("Offer Count  - " + offerDetail.getOfferUnitCount());
		if (patternFound) {
			List<ItemDetailKey> participatingItems = getParticipatingBuyItems(promotion, fileName);
			promotion.setBuyItems(getParticipatingItemData(participatingItems, promotion));
			logger.info("No of participating items - " + participatingItems.size());

			buyReq.addOfferDetail(offerDetail);
			promotion.addPromoBuyRequirement(buyReq);
		}

	}

	/**
	 * Parses Must Buy Promotions
	 * 
	 * @param promotion
	 * @param fileName
	 * @throws GeneralException
	 */
	private void parseMustBuy(PromoDefinition promotion, String fileName) throws GeneralException {
		PromoBuyRequirement buyReq = new PromoBuyRequirement();
		buyReq.setMustBuyQty(promotion.getNoOfItems());
		buyReq.setBuyAndGetIsSame(String.valueOf(Constants.NO));
		try {
			String oStr = matchDollarPatterns(promotion.getRetailForPurchase());
			if (oStr != null)
				buyReq.setMustBuyAmt(Double.parseDouble(oStr));
			else {
				oStr = matchNumberPatterns(promotion.getRetailForPurchase());
				if (oStr != null)
					buyReq.setMustBuyAmt(Double.parseDouble(oStr));
				else {
					logger.error("Must Buy Amount cannot be parsed");
					promotion.setCanBeAdded(false);
					return;
				}
			}
		} catch (Exception e) {
			logger.info("Unable to parse must buy amount - " + promotion.getRetailForPurchase() + "\t" + e);
			promotion.setCanBeAdded(false);
			return;
		}

		logger.info("Must Buy Qty  - " + buyReq.getMustBuyQty());
		logger.info("Must Buy Amount  - " + buyReq.getMustBuyAmt());

		List<ItemDetailKey> participatingItems = getParticipatingBuyItems(promotion, fileName);
		promotion.setBuyItems(getParticipatingItemData(participatingItems, promotion));
		logger.info("No of participating items - " + participatingItems.size());

		if (promotion.getAddtlQtyRetail() != null) {
			String oStr = matchDollarPatterns(promotion.getAddtlQtyRetail());
			if (oStr != null) {
				for (PromoBuyItems buyItem : promotion.getBuyItems()) {
					buyItem.setSaleQty(1);
					buyItem.setSalePrice(Double.parseDouble(oStr));
				}
			}
		}

		promotion.addPromoBuyRequirement(buyReq);
	}

	/**
	 * Parses Catalina Promotions
	 * 
	 * @param promotion
	 * @param fileName
	 * @throws GeneralException
	 */
	private void parseCatalina(PromoDefinition promotion, String fileName) throws GeneralException {
		promotion.setPromoTypeId(Constants.PROMO_TYPE_STANDARD);
		PromoBuyRequirement buyReq = new PromoBuyRequirement();
		buyReq.setBuyAndGetIsSame(String.valueOf(Constants.NO));
		try {
			String oStr = matchDollarPatterns(promotion.getTier1Req());
			if (oStr != null)
				buyReq.setMinAmtReqd(Double.parseDouble(oStr));
			else {
				oStr = matchNumberPatterns(promotion.getTier1Req());
				if (oStr != null)
					buyReq.setMinQtyReqd(Integer.parseInt(oStr));
				else {
					logger.error("Min Amt/Min Qty cannot be parsed");
					promotion.setCanBeAdded(false);
					return;
				}
			}

			PromoOfferDetail offerDetail = new PromoOfferDetail();
			offerDetail.setPromoOfferTypeId(Constants.OFFER_TYPE_COUPON);
			offerDetail.setOfferUnitType(Constants.OFFER_UNIT_TYPE_DOLLAR);

			oStr = matchDollarPatterns(promotion.getTier1Amt());
			if (oStr != null)
				offerDetail.setOfferValue(Double.parseDouble(oStr));
			else {
				oStr = matchNumberPatterns(promotion.getTier1Amt());
				if (oStr != null)
					offerDetail.setOfferValue(Double.parseDouble(oStr));
				else {
					logger.error("Offer Amount cannot be parsed");
					promotion.setCanBeAdded(false);
					return;
				}
			}

			buyReq.addOfferDetail(offerDetail);
			promotion.addPromoBuyRequirement(buyReq);
			logger.info("Min Qty/Amount  - " + promotion.getTier1Req());
			logger.info("Offer Amount  - " + promotion.getTier1Amt());

			oStr = null;
			if (promotion.getTier2Req() != null && promotion.getTier2Req().trim().length() > 0) {
				boolean canBeAdded = true;
				PromoBuyRequirement buyReq2 = new PromoBuyRequirement();
				buyReq2.setBuyAndGetIsSame(String.valueOf(Constants.NO));
				oStr = matchDollarPatterns(promotion.getTier2Req());
				if (oStr != null)
					buyReq2.setMinAmtReqd(Double.parseDouble(oStr));
				else {
					oStr = matchNumberPatterns(promotion.getTier2Req());
					if (oStr != null)
						buyReq2.setMinQtyReqd(Integer.parseInt(oStr));
					else {
						canBeAdded = false;
					}
				}

				PromoOfferDetail offerDetail2 = new PromoOfferDetail();
				offerDetail2.setPromoOfferTypeId(Constants.OFFER_TYPE_COUPON);
				offerDetail2.setOfferUnitType(Constants.OFFER_UNIT_TYPE_DOLLAR);

				oStr = matchDollarPatterns(promotion.getTier2Amt());
				if (oStr != null)
					offerDetail2.setOfferValue(Double.parseDouble(oStr));
				else {
					oStr = matchNumberPatterns(promotion.getTier2Amt());
					if (oStr != null)
						offerDetail2.setOfferValue(Double.parseDouble(oStr));
					else {
						canBeAdded = false;
					}
				}

				if (canBeAdded) {
					buyReq2.addOfferDetail(offerDetail2);
					promotion.addPromoBuyRequirement(buyReq2);
					logger.info("2. Min Qty/Amount  - " + promotion.getTier1Req());
					logger.info("2. Offer Amount  - " + promotion.getTier1Amt());
				}
			}
		} catch (Exception e) {
			logger.info("Unable to parse must buy amount - " + promotion.getRetailForPurchase() + "\t" + e);
		}

		List<ItemDetailKey> participatingItems = getParticipatingBuyItems(promotion, fileName);
		promotion.setBuyItems(getParticipatingItemData(participatingItems, promotion));
		logger.info("No of participating items - " + participatingItems.size());
	}
	
	
	/**
	 * Parses Super Coupon
	 * 
	 * @param promotion
	 * @param fileName
	 * @throws GeneralException
	 */
	private void parseSuperCoupon(PromoDefinition promotion, String fileName) throws GeneralException {
		promotion.setPromoDefnTypeId(PromoTypeLookup.SUPER_COUPON.getPromoTypeId());
		boolean isBuyItemReq = true;
		PromoBuyRequirement buyReq = new PromoBuyRequirement();
		List<PromoOfferDetail> offerDetailsList = new ArrayList<PromoOfferDetail>();
		PromoOfferDetail offerDetail = new PromoOfferDetail();
		buyReq.setBuyAndGetIsSame(String.valueOf(Constants.NO));
		try {
			
			Matcher dollarOffWithMinReq = matchValueWithMinQtyPattern(promotion);
			String dollarOff = matchValueWithOffPattern(promotion);			
			String dollarWithDecimal = matchDollarPatterns(promotion.getAmountOff());
			String dollarWODecimal = matchNumberPatterns(promotion.getAmountOff());
			
			if(dollarOffWithMinReq!= null) {
				//$1 off 2 or $1.99 off 2 or 1 off 2 or 1.99 off 2
				offerDetail.setPromoOfferTypeId(PromoOfferTypeLookup.OFF.getPromoOfferTypeId());
				buyReq.setMinQtyReqd(Integer.parseInt(dollarOffWithMinReq.group(3)));
				String offerValue = dollarOffWithMinReq.group(1).replaceAll("\\$", "");
				offerDetail.setOfferValue(Double.parseDouble(offerValue));
				
			} else if (dollarOff != null){
				//$1.99 off  or $1 off or 1.99 off or 1 off
				offerDetail.setPromoOfferTypeId(PromoOfferTypeLookup.OFF.getPromoOfferTypeId());
				offerDetail.setOfferValue(Double.parseDouble(dollarOff));
			} else if (dollarWithDecimal != null || dollarWODecimal != null) {
				//$2.99 or $2 or 2.99 or 2
				String finalDollar = dollarWithDecimal != null ? dollarWithDecimal : dollarWODecimal;
				offerDetail.setPromoOfferTypeId(PromoOfferTypeLookup.COUPON.getPromoOfferTypeId());
				offerDetail.setOfferValue(Double.parseDouble(finalDollar));
			} 
			//TODO: Needs to sort the values for free items price...
			else if (promotion.getAmountOff() != null && promotion.getAmountOff().equals("FREE")) {
				isBuyItemReq = false;
				// TODO: get min amt or some other condition based on the req
				offerDetail.setPromoOfferTypeId(PromoOfferTypeLookup.FREE_ITEM.getPromoOfferTypeId());
			}
			else {
				logger.error("Coupon Amount cannot be parsed");
//				promotion.setCanBeAdded(false);
				return;
			}
			//TO check Min amount of Purchase in additional Promotion details
			buyReq.setMinAmtReqd(checkMinAmtInPromoDetails(promotion));
		} catch (Exception e) {
			logger.error("Unable to parse coupon amount - " + promotion.getRetailForPurchase() + "\t" + e);
			logger.info("Unable to parse coupon amount for file name- " + fileName);
			promotion.setCanBeAdded(false);
			throw new GeneralException("Exception in parseSuperCoupon() " + e);
		}

		List<ItemDetailKey> participatingItems = getParticipatingBuyItems(promotion, fileName);
		// Added condition to check the super coupon is free or having some price...
		if (isBuyItemReq) {
			// Price is true then add participating item details in Buy items..
			promotion.setBuyItems(getParticipatingItemData(participatingItems, promotion));
			if(promotion.getBuyItems().isEmpty()){
				logger.error("No participitaing items are matching for Buy items");
				throw new GeneralException("No participitaing items are matching for Buy items ");
			}
		} else {
			// If price is free then add participating items in Offer items table..
			ArrayList<PromoOfferItem> itemCodeList = new ArrayList<PromoOfferItem>();
			
			ArrayList<PromoBuyItems> participatingItemDetails = getParticipatingItemData(participatingItems, promotion);
			for (PromoBuyItems promoBuyItems : participatingItemDetails) {
				PromoOfferItem promoOfferItem = new PromoOfferItem();
				promoOfferItem.setItemCode(promoBuyItems.getItemCode());
				itemCodeList.add(promoOfferItem);
			}
		
			offerDetail.setOfferItems(itemCodeList);
		}
		offerDetail.setOfferUnitType("D");
		offerDetailsList.add(offerDetail);
		buyReq.setOfferDetail(offerDetailsList);
		logger.info("No of participating items - " + participatingItems.size());

		if (promotion.getAddtlQtyRetail() != null) {
			String oStr = matchDollarPatterns(promotion.getAddtlQtyRetail());
			if (oStr != null) {
				for (PromoBuyItems buyItem : promotion.getBuyItems()) {
					buyItem.setSaleQty(1);
					buyItem.setSalePrice(Double.parseDouble(oStr));
				}
			}
		}

		promotion.addPromoBuyRequirement(buyReq);
	}
	
	private Double checkMinAmtInPromoDetails(PromoDefinition promotion){
		String minAmtReq = null;
		boolean patternFound = false;
		Matcher matcher = pattern13.matcher(promotion.getAddtlDetails());
		if (matcher.find()) {
			minAmtReq = matcher.group(1).replaceAll("\\$", "");
			patternFound = true;
		}
		if (!patternFound) {
			matcher = pattern14.matcher(promotion.getAmountOff());
			if (matcher.find()) {
				minAmtReq = matcher.group(1).replaceAll("\\$", "");
				patternFound = true;
			}
		}
		if (!patternFound) {
		matcher = pattern11.matcher(promotion.getAddtlDetails());
			if (matcher.find()) {
				minAmtReq = matcher.group(1).replaceAll("\\$", "");
				patternFound = true;
			}
		}
		if (!patternFound) {
			matcher = pattern12.matcher(promotion.getAmountOff());
			if (matcher.find()) {
				minAmtReq = matcher.group(1).replaceAll("\\$", "");
			}
		}
		
		return minAmtReq!= null ? Double.parseDouble(minAmtReq) : Types.NULL;
	}
	/**
	 * Parses EBonus Coupon
	 * 
	 * @param promotion
	 * @param fileName
	 * @throws GeneralException
	 */
	private void parseEBonusCoupon(PromoDefinition promotion, String fileName) throws GeneralException {
		promotion.setPromoTypeId(Constants.PROMO_TYPE_STANDARD);
		PromoBuyRequirement buyReq = new PromoBuyRequirement();
		buyReq.setBuyAndGetIsSame(String.valueOf(Constants.NO));
		boolean couponAmountSet = false;
		String oStr = matchDollarPatterns(promotion.getAmountOff());
		if (oStr != null) {
			buyReq.setStoreCoupon(Double.parseDouble(oStr));
			couponAmountSet = true;
		} else {
			oStr = matchNumberPatterns(promotion.getAmountOff());
			if (oStr != null) {
				buyReq.setStoreCoupon(Double.parseDouble(oStr));
				couponAmountSet = true;
			}
		}

		if (!couponAmountSet) {
			String couponDesc = promotion.getCouponDesc().replaceAll(" ", "").toUpperCase();

			oStr = matchDollarOffPatterns(couponDesc);
			if (oStr != null) {
				buyReq.setStoreCoupon(Double.parseDouble(oStr));
				couponAmountSet = true;
			} else {
				oStr = matchDollarPatterns(couponDesc);
				if (oStr != null) {
					buyReq.setStoreCoupon(Double.parseDouble(oStr));
					couponAmountSet = true;
				}
			}
		}

		if (!couponAmountSet) {
			String addtlInfo = promotion.getAddtlDetails().replaceAll(" ", "").toUpperCase();

			oStr = matchDollarOffPatterns(addtlInfo);
			if (oStr != null) {
				buyReq.setStoreCoupon(Double.parseDouble(oStr));
				couponAmountSet = true;
			} else {
				oStr = matchDollarPatterns(addtlInfo);
				if (oStr != null) {
					buyReq.setStoreCoupon(Double.parseDouble(oStr));
					couponAmountSet = true;
				}
			}
		}

		if (!couponAmountSet) {
			logger.error("Unable to parse coupon amount");
			promotion.setCanBeAdded(false);
			return;
		}
		logger.info("Coupon Amount  - " + buyReq.getStoreCoupon());

		List<ItemDetailKey> participatingItems = getParticipatingBuyItems(promotion, fileName);
		promotion.setBuyItems(getParticipatingItemData(participatingItems, promotion));
		logger.info("No of participating items - " + participatingItems.size());

		/*
		 * if(promotion.getAddtlQtyRetail() != null){ PromoOfferDetail
		 * offerDetail = new PromoOfferDetail();
		 * offerDetail.setPromoOfferTypeId(Constants.OFFER_TYPE_PLUS_UP);
		 * offerDetail.setOfferUnitType(Constants.OFFER_UNIT_TYPE_DOLLAR);
		 * offerDetail.setOfferValue(Double.parseDouble(promotion.
		 * getAddtlQtyRetail().replaceAll("\\$", "")));
		 * buyReq.addOfferDetail(offerDetail); }
		 */

		promotion.addPromoBuyRequirement(buyReq);
	}

	/**
	 * Parses Meal Deal Promotions
	 * 
	 * @param promotion
	 * @param fileName
	 * @throws GeneralException
	 */
	private void parseMealDeal(PromoDefinition promotion, String fileName) throws GeneralException {
		PromoBuyRequirement buyReq = new PromoBuyRequirement();
		buyReq.setBuyAndGetIsSame(String.valueOf(Constants.NO));

		List<ItemDetailKey> participatingBuyItems = getParticipatingBuyItems(promotion, fileName);
		promotion.setBuyItems(getParticipatingItemData(participatingBuyItems, promotion));
		logger.info("No of participating Buy items - " + participatingBuyItems.size());

		PromoOfferDetail offerDetail = new PromoOfferDetail();
		offerDetail.setPromoOfferTypeId(Constants.OFFER_TYPE_FREE_ITEM);
		offerDetail.setOfferUnitType(Constants.OFFER_UNIT_TYPE_NUMBER);
		List<ItemDetailKey> participatingFreeItems = getParticipatingFreeItems(promotion, fileName);
		HashMap<ItemDetailKey, String> upcItemCodeMap = new HashMap<ItemDetailKey, String>();
		for (ItemDetailKey itemDetailKey : participatingFreeItems) {
			String itemCode = itemCodeMap.get(itemDetailKey);
			if (itemCode != null) {
				if (!upcItemCodeMap.values().contains(itemCode))
					upcItemCodeMap.put(itemDetailKey, itemCode);
			}
		}
		for (String itemCode : upcItemCodeMap.values()) {
			PromoOfferItem offerItem = new PromoOfferItem();
			offerItem.setItemCode(Integer.parseInt(itemCode));
			offerDetail.addOfferItems(offerItem);
		}
		logger.info("No of participating Free items - " + offerDetail.getOfferItems().size());
		buyReq.addOfferDetail(offerDetail);
		promotion.addPromoBuyRequirement(buyReq);
	}

	/**
	 * Parses Gas Points Promotion
	 * 
	 * @param promotion
	 * @param fileName
	 * @throws GeneralException
	 */
	private void parseGasPoints(PromoDefinition promotion, String fileName) throws GeneralException {
		promotion.setPromoTypeId(Constants.PROMO_TYPE_STANDARD);

		try {
			PromoBuyRequirement buyReq = new PromoBuyRequirement();
			buyReq.setBuyAndGetIsSame(String.valueOf(Constants.NO));
			buyReq.setMinQtyReqd(Integer.parseInt(promotion.getTier1Req()));

			String gasPoints = promotion.getTier1Amt().toUpperCase();
			gasPoints = gasPoints.replaceAll(" ", "").replaceAll("\\$", "").replaceAll("POINTS", "")
					.replaceAll("PTS", "").replaceAll("GAS", "");

			PromoOfferDetail offerDetail = new PromoOfferDetail();
			offerDetail.setPromoOfferTypeId(Constants.OFFER_TYPE_GAS_POINTS);
			offerDetail.setOfferUnitType(Constants.OFFER_UNIT_TYPE_NUMBER);
			offerDetail.setOfferUnitCount(Integer.parseInt(gasPoints));
			buyReq.addOfferDetail(offerDetail);
			promotion.addPromoBuyRequirement(buyReq);
		} catch (Exception e) {
			logger.info("Unable to parse - " + e);
			promotion.setCanBeAdded(false);
			return;
		}

		List<ItemDetailKey> participatingItems = getParticipatingBuyItems(promotion, fileName);
		promotion.setBuyItems(getParticipatingItemData(participatingItems, promotion));
		logger.info("No of participating items - " + participatingItems.size());

		if (promotion.getTier2Req() != null && promotion.getTier2Req().trim().length() > 0) {
			try {
				PromoBuyRequirement buyReq = new PromoBuyRequirement();
				buyReq.setBuyAndGetIsSame(String.valueOf(Constants.NO));
				buyReq.setMinQtyReqd(Integer.parseInt(promotion.getTier2Req()));

				String gasPoints = promotion.getTier2Amt().toUpperCase();
				gasPoints = gasPoints.replaceAll(" ", "").replaceAll("\\$", "").replaceAll("POINTS", "")
						.replaceAll("PTS", "").replaceAll("GAS", "");

				PromoOfferDetail offerDetail = new PromoOfferDetail();
				offerDetail.setPromoOfferTypeId(Constants.OFFER_TYPE_GAS_POINTS);
				offerDetail.setOfferUnitType(Constants.OFFER_UNIT_TYPE_NUMBER);
				offerDetail.setOfferUnitCount(Integer.parseInt(gasPoints));
				buyReq.addOfferDetail(offerDetail);
				promotion.addPromoBuyRequirement(buyReq);
			} catch (Exception e) {
				logger.info("Unable to parse - " + e);
			}
		}
	}

	/**
	 * Gets retail price and cost info for participating items
	 * 
	 * @param participatingItems
	 * @param calendarId
	 * @return
	 * @throws GeneralException
	 */
	private ArrayList<PromoBuyItems> getParticipatingItemData(List<ItemDetailKey> participatingItems,
			PromoDefinition promotion) throws GeneralException {
		ArrayList<PromoBuyItems> buyItems = new ArrayList<PromoBuyItems>();
		// HashMap<String, String> upcItemCodeMap = priceDAO.getItemCode(_Conn,
		// new HashSet<ItemDetailKey>(participatingItems));
		int weekCalendarId = -1;
		String promoStartDate = DateUtil.dateToString(promotion.getPromoStartDate(), "MM/dd/yyyy");
		if (weekCalendarMap.get(promoStartDate) == null) {
			int startCalId = ((RetailCalendarDTO) calendarDAO.getCalendarId(_Conn, promoStartDate,
					Constants.CALENDAR_WEEK)).getCalendarId();
			weekCalendarMap.put(promoStartDate, startCalId);
			weekCalendarId = startCalId;
		} else
			weekCalendarId = weekCalendarMap.get(promoStartDate);
		HashMap<ItemDetailKey, String> upcItemCodeMap = new HashMap<ItemDetailKey, String>();
		for (ItemDetailKey itemDetailKey : participatingItems) {
			String itemCode = itemCodeMap.get(itemDetailKey);
			if (itemCode != null) {
				if (!upcItemCodeMap.values().contains(itemCode))
					upcItemCodeMap.put(itemDetailKey, itemCode);
			}
		}
		for (String itemCode : upcItemCodeMap.values()) {
			PromoBuyItems buyItem = new PromoBuyItems();
			buyItem.setItemCode(Integer.parseInt(itemCode));
			//Changed to use Super coupon Actual Start and End calendar id By Dinesh(10/15/2016)
			buyItem.setActualStartCalId(promotion.getSuperCouponStartCalID() > 0 ? promotion.getSuperCouponStartCalID() : promotion.getStartCalId());
			buyItem.setActualEndCalId(promotion.getSuperCouponEndCalId() > 0 ? promotion.getSuperCouponEndCalId() : promotion.getEndCalId());
			RetailPriceDTO rpDTO = priceDAO.getChainLevelPrice(_Conn, weekCalendarId, Integer.parseInt(itemCode));
			if (rpDTO != null) {
				buyItem.setRegQty(rpDTO.getRegQty());
				buyItem.setRegPrice(rpDTO.getRegPrice());
				buyItem.setRegMPrice(rpDTO.getRegMPrice());
			}
			RetailCostDTO rcDTO = costDAO.getChainLevelCost(_Conn, weekCalendarId, Integer.parseInt(itemCode));
			if (rcDTO != null) {
				buyItem.setListCost(rcDTO.getListCost());
				buyItem.setDealCost(rcDTO.getDealCost());
			}
			buyItems.add(buyItem);
		}
		return buyItems;
	}

	/**
	 * Parses participating buy items from input file
	 * 
	 * @param promotion
	 * @param fileName
	 * @return
	 */
	private List<ItemDetailKey> getParticipatingBuyItems(PromoDefinition promotion, String fileName) {
		List<ItemDetailKey> itemList = null;
		try {
			itemList = parser.parseParticipatingBuyItems(fileName);
		} catch (GeneralException ge) {
			logger.error("Error when parsing participating items - " + ge);
		}
		return itemList;
	}

	/**
	 * Parses participating free items from input file
	 * 
	 * @param promotion
	 * @param fileName
	 * @return
	 */
	private List<ItemDetailKey> getParticipatingFreeItems(PromoDefinition promotion, String fileName) {
		List<ItemDetailKey> itemList = null;
		try {
			itemList = parser.parseParticipatingFreeItems(fileName);
		} catch (GeneralException ge) {
			logger.error("Error when parsing participating items - " + ge);
		}
		return itemList;
	}

	/**
	 * Sets calendar id in promotion object from promo start and end date
	 * 
	 * @param promotion
	 * @throws GeneralException
	 */
	private void setCalendarId(PromoDefinition promotion) throws GeneralException {
		String promoStartDate = DateUtil.dateToString(promotion.getPromoStartDate(), "MM/dd/yyyy");
		String promoEndDate = DateUtil.dateToString(promotion.getPromoEndDate(), "MM/dd/yyyy");
		logger.info("Promo Start Date - " + promoStartDate + "\t" + "Promo End Date - " + promoEndDate);
		promotion.setWeekStartDate(promoStartDate);
		promotion.setStartCalId(
				calendarDAO.getCalendarId(_Conn, promoStartDate, Constants.CALENDAR_DAY).getCalendarId());
		promotion.setEndCalId(calendarDAO.getCalendarId(_Conn, promoEndDate, Constants.CALENDAR_DAY).getCalendarId());
		
	}

	private String matchDollarPatterns(String inputString) {
		String matchedString = null;
		boolean patternFound = false;
		Matcher matcher = pattern1.matcher(inputString);
		if (matcher.find()) {
			matchedString = matcher.group(0).replaceAll("\\$", "");
			patternFound = true;
		}

		if (!patternFound) {
			matcher = pattern2.matcher(inputString);
			if (matcher.find()) {
				matchedString = matcher.group(0).replaceAll("\\$", "");
			}
		}
		return matchedString;
	}

	private String matchNumberPatterns(String inputString) {
		String matchedString = null;
		boolean patternFound = false;
		Matcher matcher = pattern3.matcher(inputString);

		if (matcher.find()) {
			matchedString = matcher.group(0);
			patternFound = true;
		}

		if (!patternFound) {
			matcher = pattern4.matcher(inputString);
			if (matcher.find()) {
				matchedString = matcher.group(0);
			}
		}

		return matchedString;
	}

	private String matchDollarOffPatterns(String inputString) {
		String matchedString = null;
		boolean patternFound = false;
		Matcher matcher = pattern5.matcher(inputString);

		if (matcher.find()) {
			matchedString = matcher.group(0);
			patternFound = true;
		}

		if (!patternFound) {
			matcher = pattern6.matcher(inputString);
			if (matcher.find()) {
				matchedString = matcher.group(0);
			}
		}

		return matchedString;
	}

	/**
	 * Fields to be parsed to different promotion types
	 * 
	 * @param fieldNames
	 */
	private void mapFieldNames(TreeMap<Integer, TreeMap<String, String>> fieldNames) {
		TreeMap<String, String> fields = new TreeMap<String, String>();
		fields.put("11-2", "promoStartDate");
		fields.put("11-7", "promoEndDate");
		fields.put("16-2", "promoName");
		fields.put("28-1", "addtlDetails");

		fieldNames.put(PromoTypeLookup.BUX_X_GET_Y_SAME.getPromoTypeId(), fields);

		TreeMap<String, String> fields1 = new TreeMap<String, String>();
		fields1.put("11-2", "promoStartDate");
		fields1.put("11-7", "promoEndDate");
		fields1.put("16-2", "promoName");
		fields1.put("20-1", "noOfItems");
		fields1.put("24-1", "retailForPurchase");
		fields1.put("24-6", "addtlQtyRetail");
		fields1.put("28-1", "addtlDetails");

		fieldNames.put(PromoTypeLookup.MUST_BUY.getPromoTypeId(), fields1);

		TreeMap<String, String> fields2 = new TreeMap<String, String>();
		fields2.put("11-2", "promoStartDate");
		fields2.put("11-7", "promoEndDate");
		fields2.put("16-2", "promoName");
		fields2.put("20-1", "tier1Req");
		fields2.put("20-6", "tier2Req");
		fields2.put("24-1", "tier1Amt");
		fields2.put("24-6", "tier2Amt");
		fields2.put("28-1", "addtlDetails");

		fieldNames.put(Constants.PROMO_TYPE_CATALINA, fields2);

		TreeMap<String, String> fields3 = new TreeMap<String, String>();
		fields3.put("11-2", "promoStartDate");
		fields3.put("11-7", "promoEndDate");
		fields3.put("16-2", "promoName");
		fields3.put("20-1", "couponDesc");
		fields3.put("24-1", "amountOff");
		fields3.put("24-6", "addtlQtyRetail");
		fields3.put("28-1", "addtlDetails");

		fieldNames.put(PromoTypeLookup.SUPER_COUPON.getPromoTypeId(), fields3);
		fieldNames.put(Constants.PROMO_TYPE_EBONUS_COUPON, fields3);

		TreeMap<String, String> fields4 = new TreeMap<String, String>();
		fields4.put("11-2", "promoStartDate");
		fields4.put("11-7", "promoEndDate");
		fields4.put("16-2", "promoName");
		fields4.put("28-1", "addtlDetails");

		fieldNames.put(PromoTypeLookup.MEAL_DEAL.getPromoTypeId(), fields4);

		TreeMap<String, String> fields5 = new TreeMap<String, String>();
		fields5.put("11-2", "promoStartDate");
		fields5.put("11-7", "promoEndDate");
		fields5.put("16-2", "promoName");
		fields5.put("20-1", "tier1Req");
		fields5.put("20-6", "tier2Req");
		fields5.put("24-1", "tier1Amt");
		fields5.put("24-6", "tier2Amt");
		fields5.put("28-1", "addtlDetails");

		fieldNames.put(Constants.PROMO_TYPE_GAS_POINTS, fields5);

	}
	
	private long updateNoOfDaysInPromoDuration(PromoDefinition promotion) throws ParseException {
		
		Date endDateOfPromo = getLastDateOfWeek(promotion.getPromoEndDate());
		Date startDateTemp = getFirstDateOfWeek(promotion.getPromoStartDate());

		long diff = endDateOfPromo.getTime() - startDateTemp.getTime();
		// System.out.println ("Days: " + TimeUnit.DAYS.convert(diff,TimeUnit.MILLISECONDS));
		return TimeUnit.DAYS.convert(diff, TimeUnit.MILLISECONDS);
	}
	
	private Date getFirstDateOfWeek(Date inputDate) throws ParseException {
		Calendar cal = Calendar.getInstance();
		cal.setTime(inputDate);
		int startDay = (cal.get(Calendar.DAY_OF_WEEK) - cal.getFirstDayOfWeek());
		Date outputDate = DateUtil.incrementDate(inputDate, -startDay);
		return outputDate;
	}

	private Date getLastDateOfWeek(Date inputDate) throws ParseException {
		Calendar cal = Calendar.getInstance();
		cal.setTime(inputDate);
		Date outputDate = DateUtil.incrementDate(inputDate, 7 - cal.get(Calendar.DAY_OF_WEEK));
		return outputDate;
	}
	
	private Date getNextWeekDate(Date inputDate) throws ParseException {
		Calendar cal = Calendar.getInstance();
		cal.setTime(inputDate);
		Date outputDate = DateUtil.incrementDate(inputDate, 6 + cal.get(Calendar.DAY_OF_WEEK));
		return outputDate;
	}
	private Date getCurrentWeekEndDate(Date inputDate) throws ParseException {
		Calendar cal = Calendar.getInstance();
		cal.setTime(inputDate);
		Date outputDate = DateUtil.incrementDate(inputDate, 5 + cal.get(Calendar.DAY_OF_WEEK));
		return outputDate;
	}
	
	
	/**
	 * To match the pattern like ($1.99 off of $1 off)..
	 * @param promotion
	 * @param offerDetail
	 * @param patternFound
	 */
	private String matchValueWithOffPattern(PromoDefinition promotion) {
		String matchedString = null;
		boolean patternFound = false;
		Matcher matcher = pattern7.matcher(promotion.getAmountOff());
		if (matcher.find()) {
			matchedString = matcher.group(1).replaceAll("\\$", "");
			patternFound = true;
		}
		if (!patternFound) {
			matcher = pattern8.matcher(promotion.getAmountOff());
			if (matcher.find()) {
				matchedString = matcher.group(1).replaceAll("\\$", "");
			}
		}
		return matchedString;
	}
	
	
	/**
	 * To match the pattern like  ($1 off 2 or $1.99 off 2)..
	 * @param promotion
	 * @param offerDetail
	 * @param buyReq
	 * @param patternFound
	 */
	private Matcher matchValueWithMinQtyPattern(PromoDefinition promotion){
		Matcher matchedString = null;
		boolean patternFound = false;
		Matcher matcher = pattern9.matcher(promotion.getAmountOff());
		if (matcher.find()) {
			matchedString = matcher;
			patternFound = true;
		}
		if (!patternFound) {
			matcher = pattern10.matcher(promotion.getAmountOff());
			if (matcher.find()) {
				matchedString = matcher;
			}
		}
		return matchedString;
	}
	
	/**
	 * To set Actual start calendar id and End Calendar Id
	 * @param promotion
	 * @throws GeneralException
	 */
	private void setSuperCouponCalendarId(PromoDefinition promotion) throws GeneralException{
			String superCouponStartDate = DateUtil.dateToString(promotion.getPromoStartDate(), "MM/dd/yyyy");
			String superCouponEndDate = DateUtil.dateToString(promotion.getPromoEndDate(), "MM/dd/yyyy");
			logger.info("Super Coupon Start Date - " + superCouponStartDate + "\t" + "Super Coupon End Date - " + superCouponEndDate);
			promotion.setSuperCouponStartCalID(
					calendarDAO.getCalendarId(_Conn, superCouponStartDate, Constants.CALENDAR_DAY).getCalendarId());
			promotion.setSuperCouponEndCalId(calendarDAO.getCalendarId(_Conn, superCouponEndDate, Constants.CALENDAR_DAY).getCalendarId());
	}
	
	/**
	 * To delete the Directory and it's files
	 * @param file
	 * @throws IOException
	 */
	private void deleteFilesAndDir(File file) throws IOException {
		if (file.isDirectory()) {
			// directory is empty, then delete it
			if (file.list().length == 0) {
				file.delete();
				System.out.println("Directory is deleted : " + file.getAbsolutePath());
			} else {
				// list all the directory contents
				String files[] = file.list();
				for (String temp : files) {
					File fileDelete = new File(file, temp);
					deleteFilesAndDir(fileDelete);
				}
				// check the directory again, if empty then delete it
				if (file.list().length == 0) {
					file.delete();
					logger.debug("Directory is deleted : " + file.getAbsolutePath());
				}
			}

		} else {
			// if file, then delete it
			file.delete();
			logger.debug("File is deleted : " + file.getAbsolutePath());
		}
	}
	
	/**
	 * To check the Coupon type to process
	 * @param fileName
	 * @param couponType
	 * @return
	 * @throws GeneralException
	 */
	public boolean isOverlaysFile(String fileName,TreeMap<Integer, TreeMap<String, String>> fieldNames){
		boolean isOverlaysFile = false;
		try{
			
			@SuppressWarnings("unchecked")
			PromoDefinition overlayDetails = null;
			try {
				overlayDetails = (PromoDefinition) parser.parsePromoOverview(PromoDefinition.class, fileName,
						fieldNames);
			} catch (GeneralException e) {
				isOverlaysFile = false;
			}
			for(String promoTypeID: overlaysTypeIds){
				if(Integer.valueOf(promoTypeID).equals(overlayDetails.getPromoTypeId())){
					isOverlaysFile = true;
				}
			}
			
		}catch(Exception e){
			isOverlaysFile = false;
		}
		return isOverlaysFile;
	}
	

	/**
	 * To find the directory in a given path
	 * @param inputFolder
	 * @param outputFolder
	 * @param onlyDir
	 * @throws IOException
	 */
	public void copyFilesFromDir(final File inputFolder, File outputFolder) throws IOException {
		for (final File fileEntry : inputFolder.listFiles()) {

			if (fileEntry.isDirectory()) {
				copyFilesFromDir(fileEntry, outputFolder);
			} else {
				copyDirectory(fileEntry, new File(outputFolder,fileEntry.getName()));
//				copyDirectory(inputFolder, outputFolder);
			}
		}
	}
	
	/**
	 * To copy files from source to destination folder
	 * @param inputFolder
	 * @param outputFolder
	 * @throws IOException
	 */
	public void copyFilesFromFolder(File inputFolder, File outputFolder) throws IOException {
		for (final File fileEntry : inputFolder.listFiles()) {
			 if (fileEntry.isDirectory()) {
				 copyFilesFromDir(inputFolder,outputFolder);
			 }
			copyDirectory(fileEntry, new File(outputFolder,fileEntry.getName()));
			
		}
	 }
	
	/**
	 * Copy directory
	 * @param sourceLocation
	 * @param targetLocation
	 * @throws IOException
	 */
	 public void copyDirectory(File sourceLocation , File targetLocation)
			    throws IOException {

			        if (sourceLocation.isDirectory()) {
			            if (!targetLocation.exists()) {
			                targetLocation.mkdir();
			            }

			            String[] children = sourceLocation.list();
			            for (int i=0; i<children.length; i++) {
//			            	if(children[i].toLowerCase().contains("xlx")||children[i].toLowerCase().contains("xlsx")){
			            		copyDirectory(new File(sourceLocation, children[i]),
				                        new File(targetLocation, children[i]));
//			            	}
			            }
			        } else {

			            InputStream in = new FileInputStream(sourceLocation);
			            OutputStream out = new FileOutputStream(targetLocation);
			            // Copy the bits from instream to outstream
			            byte[] buf = new byte[1024];
			            int len;
			            while ((len = in.read(buf)) > 0) {
			                out.write(buf, 0, len);
			            }
			            in.close();
			            out.close();
			        }
			    }
}
