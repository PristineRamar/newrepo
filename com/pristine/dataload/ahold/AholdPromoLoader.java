package com.pristine.dataload.ahold;

import java.sql.Connection;
import java.text.DateFormat;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.TreeMap;
import java.util.function.Function;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import java.util.stream.Collectors;

import org.apache.log4j.Logger;
import org.apache.log4j.PropertyConfigurator;

import com.pristine.dao.DBManager;
import com.pristine.dao.ItemDAO;
//import com.pristine.dao.RetailCalendarDAO;
import com.pristine.dao.RetailCalendarPromoDAO;
import com.pristine.dao.RetailDivisionDAO;
import com.pristine.dao.offermgmt.promotion.PromotionDAO;
import com.pristine.dao.offermgmt.weeklyad.WeeklyAdDAO;
import com.pristine.dataload.service.PromotionService;
import com.pristine.dto.ItemDTO;
import com.pristine.dto.LocationDTO;
import com.pristine.dto.RetailCalendarDTO;
import com.pristine.dto.RetailerLikeItemGroupDTO;
import com.pristine.dto.fileformatter.ahold.AholdPromoInputDTO;
import com.pristine.dto.offermgmt.LocationKey;
import com.pristine.dto.offermgmt.MultiplePrice;
import com.pristine.dto.offermgmt.promotion.PromoBuyItems;
import com.pristine.dto.offermgmt.promotion.PromoBuyRequirement;
import com.pristine.dto.offermgmt.promotion.PromoDefinition;
import com.pristine.dto.offermgmt.promotion.PromoLocation;
import com.pristine.dto.offermgmt.promotion.PromoOfferDetail;
import com.pristine.dto.offermgmt.promotion.PromoOfferItem;
import com.pristine.dto.offermgmt.weeklyad.WeeklyAd;
import com.pristine.exception.GeneralException;
import com.pristine.lookup.offermgmt.promotion.PromoStatusLookup;
import com.pristine.lookup.offermgmt.promotion.PromoTypeLookup;
import com.pristine.parsinginterface.ExcelFileParser;
import com.pristine.service.RetailCalendarPromoCacheService;
import com.pristine.util.Constants;
import com.pristine.util.PrestoUtil;
import com.pristine.util.PristineDBUtil;
import com.pristine.util.PropertyManager;
import com.pristine.util.offermgmt.PRFormatHelper;

public class AholdPromoLoader {

	private static Logger logger = Logger.getLogger("AholdPromoLoader");

	// Cache data
	// List<RetailCalendarDTO> retailCalendarCache = null;
	List<RetailCalendarDTO> retailCalendarPromoCache = new ArrayList<RetailCalendarDTO>();
	List<LocationDTO> divisionCache = new ArrayList<LocationDTO>();
	List<ItemDTO> itemCache = new ArrayList<ItemDTO>();
	List<RetailerLikeItemGroupDTO> lirCache = new ArrayList<RetailerLikeItemGroupDTO>();
	HashMap<String, List<ItemDTO>> retailerItemCodeAndItsItem = new HashMap<String, List<ItemDTO>>();
	HashMap<Integer, List<ItemDTO>> retLirIdAndItsItem = new HashMap<Integer, List<ItemDTO>>();
	HashMap<String, RetailerLikeItemGroupDTO> retLirNameAndLIG = new HashMap<String, RetailerLikeItemGroupDTO>();

	// Global variables
	private static HashMap<String, String> divisionNameMap = new HashMap<String, String>();
	private static String inputFilePath;
	private String rootPath;
	private Connection conn = null;	
	private static DateFormat appDateFormatter = new SimpleDateFormat(Constants.APP_DATE_FORMAT);
	private static String processingWeek = "";
	private static List<AholdPromoInputDTO> ignoredPromos = new ArrayList<AholdPromoInputDTO>();
	private static Set<String> unkownStatus = new HashSet<>();
	
	
	// Promotion pattern identifier
	private final static Pattern salePricePattern = Pattern.compile("((\\d+)[/]{1})?[$]{1}(\\d+\\.?\\d*)$");
	private final static Pattern dollarOffPattern = Pattern.compile("^[$]{1}(\\d+\\.?\\d*)\\sOFF$");
	private final static Pattern pctOffPattern = Pattern.compile("^(\\d+)[%]{1}\\sOFF$");	
	private final static Pattern bxgyPattern = Pattern.compile("B([1-9])G([1-9])$");
	private final static Pattern mustBuyPattern = Pattern.compile("^(\\d+)[/]{1}[$]{1}(\\d+\\.?\\d*)\\sWYB");
	private final static Pattern btgoPattern = Pattern.compile(".*BUY\\s.*GET\\s.*");
	
	private final static Pattern bxGDollarPattern = Pattern.compile("^[B]{1}(\\d+)[G]{1}\\({1}[$]{1}(\\d+\\.?\\d*)\\)");
	private final static Pattern bDollarGDollarPattern = Pattern.compile("^[B]{1}\\({1}[$]{1}(\\d+\\.?\\d*)\\)[G]{1}\\({1}[$]{1}(\\d+\\.?\\d*)\\)");
	

	// Constants
	private final static String INPUT_FOLDER = "INPUT_FOLDER=";
	private final static String BOGO = "B1G1";
	private final static String NON_AD = "NON-AD";
	private final static String EDLP = "EDLP";
	private final static String PENNY = "PENNY";
	private final static String DOLLAR_OFF = "DOLLAR_OFF";
	private final static String PCT_OFF = "PCT_OFF";
	private final static String APPROVED = "Approved";
	private final static String PROCESSED = "PROCESSED";
	private final static String PENDING = "PENDING";
	private final static String MODIFY = "MODIFY";
	private final static String WAITING_FOR_APPROVAL = "WAITING FOR MERCHANT APPROVAL";
	private final static String RETURNED_FROM_PROCESSED = "RETURNED FROM PROCESSED"; 
	private final static String RETURNED_CM = "RETURNED: CM";  
	private final static String RETURNED = "RETURNED";
	private final static String GAS = "GAS";
	private final static String BTGO1 = "BUY THEIRS GET OURS";
	private final static String BTGO2 = "BTGO";
	private final static String BTGO_OWN_BRAND = "OWN BRAND";
	private final static String AD_OTHER = "AD - OTHER";
	private final static String AD_FRONT = "AD - FRONT";
	private final static String AD_CLIP = "AD - CLIP";
	private final static String AD_WRAP = "AD - WRAP";
	
	//Ignore reason
	private final static String IGNORE_REASON_INVALID_ITEM = "Invalid Items";
	
	public AholdPromoLoader() {
		PropertyManager.initialize("analysis.properties");
		rootPath = PropertyManager.getProperty("DATALOAD.ROOTPATH", "");
		try {
			conn = DBManager.getConnection();
		} catch (GeneralException gex) {
			logger.error("Error when creating connection - " + gex);
		}
	}

	public static void main(String[] args) {
		AholdPromoLoader promoLoader = new AholdPromoLoader();
		PropertyConfigurator.configure("log4j-ahold-promo-loader.properties");

		try {
			// Read input arguments
			for (String arg : args) {
				if (arg.startsWith(INPUT_FOLDER)) {
					inputFilePath = arg.substring(INPUT_FOLDER.length());
				}
			}

			// Call entry point
			logger.debug("Start Process...");
			promoLoader.processPromoFile();
		} catch (Exception e) {
			e.printStackTrace();
			logger.error("Exception in main()", e);
		}

	}

	private void processPromoFile() {
		String fileName = null;
		String inputPath = rootPath + "/" + inputFilePath;
		
		try {
			ExcelFileParser<AholdPromoInputDTO> parser = new ExcelFileParser<AholdPromoInputDTO>();
			parser.setFirstRowToProcess(1); // skip header row
			TreeMap<Integer, String> fieldNames = new TreeMap<Integer, String>();
			mapFieldNames(fieldNames);
			ArrayList<String> fileList = parser.getFiles(inputFilePath);

			// Initialize data
			initializeData();

			// Loop through each file
			for (String processigFile : fileList) {
				try {
					List<AholdPromoInputDTO> promoInputDTO= null;
					ignoredPromos = new ArrayList<AholdPromoInputDTO>();

					// Process each file
					logger.info("Processing file - " + processigFile);

					fileName = processigFile;
					promoInputDTO = parser.parseExcelFile(AholdPromoInputDTO.class, fileName, 0, fieldNames);

					if (promoInputDTO.size() > 0) {
						processPromotion(promoInputDTO);
					} else {
						logger.info("Unable to process: 0 rows");
					}

					PrestoUtil.moveFile(fileName, inputPath + "/" + Constants.COMPLETED_FOLDER);
					PristineDBUtil.commitTransaction(conn, "batch record update");
				} catch (GeneralException | ParseException ge) {
					logger.error("Error while processing input file" + processigFile, ge);
					PrestoUtil.moveFile(fileName, inputPath + "/" + Constants.BAD_FOLDER);
					PristineDBUtil.rollbackTransaction(conn, "Unexpected Exception");
				}
			}
		} catch (GeneralException ex) {
			logger.error("Error while intitializing data");
		} finally {
			PristineDBUtil.close(conn);
		}
	}

	private void initializeData() throws GeneralException {
		// Cache all required data
		cacheData();

		// Map division full name
		mapDivision();

		// group item by retailer item code
		groupItemByRetailerItemCode();

		// group item by ret lir id
		groupItemByRetLirId();

		// group lig by ret lir id
		groupLIGByRetLirName();
	}

	private void cacheData() throws GeneralException {
		// Cache retail calendar
		// logger.debug("Caching of Retail Calendar is started...");
		// retailCalendarCache = new RetailCalendarDAO().getFullRetailCalendar(conn);
		// logger.debug("Caching of Retail Calendar is started...");

		// Cache promotion calendar
		logger.debug("Caching of Retail Calendar Promo is started...");
		retailCalendarPromoCache = new RetailCalendarPromoDAO().getFullRetailCalendarPromo(conn);
		logger.debug("Caching of Retail Calendar Promo is started...");

		// Cache division table
		divisionCache = new RetailDivisionDAO().getAllDivision(conn);

		// Cache item lookup table
		logger.debug("Caching of item is started...");
		itemCache = new ItemDAO().getAllActiveItems(conn);
		logger.debug("Caching of item is completed...");

		// Cache retailer like item table
		logger.debug("Caching of LIG is started...");
		lirCache = new ItemDAO().getAllLIR(conn);
		logger.debug("Caching of LIG is completed...");
	}

	private void mapDivision() {
		divisionNameMap.put("NE", "NEW ENGLAND");
		divisionNameMap.put("GC", "GIANTC");
		divisionNameMap.put("GL", "GIANT");
		divisionNameMap.put("NY", "NEW YORK METRO");
		divisionNameMap.put("SSS", "NEW YORK METRO");
		divisionNameMap.put("SSN", "NEW ENGLAND");
	}

	private void groupItemByRetailerItemCode() {
		retailerItemCodeAndItsItem = (HashMap<String, List<ItemDTO>>) itemCache.stream()
				.filter(p -> p.getRetailerItemCode() != null && !p.getRetailerItemCode().equals("null"))
				.collect(Collectors.groupingBy(ItemDTO::getRetailerItemCode));
	}

	private void groupItemByRetLirId() {
		retLirIdAndItsItem = (HashMap<Integer, List<ItemDTO>>) itemCache.stream().filter(p -> p.getLikeItemId() > 0)
				.collect(Collectors.groupingBy(ItemDTO::getLikeItemId));
	}

	private void groupLIGByRetLirName() {
		retLirNameAndLIG = (HashMap<String, RetailerLikeItemGroupDTO>) lirCache.stream().filter(p -> p.getRetLirId() > 0)
				.collect(Collectors.toMap(RetailerLikeItemGroupDTO::getRetLirName, Function.identity()));
	}

	private void processPromotion(List<AholdPromoInputDTO> allPromos) throws ParseException, GeneralException {
		HashMap<String, List<AholdPromoInputDTO>> promoByWeek = new HashMap<String, List<AholdPromoInputDTO>>();
		WeeklyAdDAO adDAO = new WeeklyAdDAO(conn);
		
		logger.debug("Total Rows:" + allPromos.size());

		// Filter by status
		//List<AholdPromoInputDTO> approvedPromos = getApprovedPromo(allPromos);

		// Find promo start and end calendar id
		findAndFillCalendarId(allPromos);

		// Find location Id
		findAndFillLocationId(allPromos);

		// Find item codes and/or ret lir id
		findAndFillProductKey(allPromos);

		// Group by week
		promoByWeek = groupPromoByWeek(allPromos);

		// Process each week
		for (Map.Entry<String, List<AholdPromoInputDTO>> entry : promoByWeek.entrySet()) {
			processingWeek = entry.getKey();
			
			logger.info("Processing of week " + processingWeek + " is started...");
			ignoredPromos = new ArrayList<AholdPromoInputDTO>();
			unkownStatus = new HashSet<>();
			List<AholdPromoInputDTO> weekRows = entry.getValue();

			HashMap<String, List<AholdPromoInputDTO>> promoByGroupName = new HashMap<String, List<AholdPromoInputDTO>>();
			HashMap<String, List<AholdPromoInputDTO>> promoByPromoGroup = new HashMap<String, List<AholdPromoInputDTO>>();

			// Group by group name
			promoByGroupName = groupPromoByGroupName(weekRows);

			// Group by promo group
			promoByPromoGroup = groupPromoByPromoGroup(weekRows);

			// Process group
			List<PromoDefinition> finalPromotions = processGroup(promoByGroupName);

			// Process promo group
			finalPromotions.addAll(processGroup(promoByPromoGroup));
			
			// Get ad promotions
			List<PromoDefinition> adPromotions = getAdPromotionMap(finalPromotions);
			
			// Group promotions by Location
			HashMap<LocationKey, List<PromoDefinition>> promoByLocation = getPromotionsByLocation(adPromotions);
			
			// Create weekly ad objects
			List<WeeklyAd> weeklyAds = setupWeeklyAd(promoByLocation);
			
			
			// Insert promotions
			if (finalPromotions.size() > 0) {
				new PromotionDAO(conn).savePromotionV2(finalPromotions, null, false);
			}

			
			logger.info("processPromoRecords() - Total # of Ads to be processed: " + weeklyAds.size());
			long startTime = System.currentTimeMillis();
			
			for (WeeklyAd weeklyAd : weeklyAds) {
				adDAO.saveWeeklyAd(weeklyAd);
			}
			
			long endTime = System.currentTimeMillis();
			logger.info("processPromoRecords() - Time taken to save Weekly Ads: " + (endTime - startTime) + " ms.");
			
			// Log invalid or ignored promotions
			logIgnoredPromos(weekRows.size());

			logger.info("Processing of week " + entry.getKey() + " is completed...");
		}

	}
	
	
	private HashMap<LocationKey, List<PromoDefinition>> getPromotionsByLocation(List<PromoDefinition> adPromotions){
		
		HashMap<LocationKey, List<PromoDefinition>> promoByLocation = new HashMap<>();
		adPromotions.stream().forEach(promo -> {
			promo.getPromoLocation().forEach(promoLocation -> {
				LocationKey locationKey = new LocationKey(promoLocation.getLocationLevelId(), promoLocation.getLocationId());
				
				List<PromoDefinition> promoList = new ArrayList<>();
				if(promoByLocation.containsKey(locationKey)){
					promoList = promoByLocation.get(locationKey);
				}
				promoList.add(promo);
				promoByLocation.put(locationKey, promoList);
			});
		});
		
		return promoByLocation;
		
	}
	
	private List<WeeklyAd> setupWeeklyAd(HashMap<LocationKey, List<PromoDefinition>> promoByLocation) {
		List<WeeklyAd> weeklyAds = new ArrayList<>();
		RetailCalendarPromoCacheService promoCacheService = new RetailCalendarPromoCacheService(retailCalendarPromoCache);
		HashMap<LocationKey, String> divisionNameLookup = getDivisionNameLookup();
		PromotionService promotionService = new PromotionService();
		promoByLocation.forEach((k, v) -> {
			WeeklyAd weeklyAd = new WeeklyAd();
			PromoDefinition samplePromo = v.get(0);
			weeklyAd.setApprovedBy(Constants.BATCH_USER);
			weeklyAd.setCreatedBy(Constants.BATCH_USER);
			weeklyAd.setLocationLevelId(k.getLocationLevelId());
			weeklyAd.setLocationId(k.getLocationId());
			weeklyAd.setStatus(3);
			RetailCalendarDTO retailCalendarDTO = promoCacheService.getWeekCalendarId(appDateFormatter.format(samplePromo.getPromoStartDate()));
			weeklyAd.setCalendarId(retailCalendarDTO.getCalendarId());
			weeklyAd.setWeekStartDate(retailCalendarDTO.getStartDate());
			weeklyAd.setStartDate(samplePromo.getPromoStartDate());
			weeklyAd.setAdName(divisionNameLookup.get(k) + " - Ad " + retailCalendarDTO.getStartDate());
			weeklyAd.setAdNumber(retailCalendarDTO.getStartDate().replaceAll("/", ""));
			
			
			promotionService.setupWeeklyAdAndPromotions(v, weeklyAd);
			
			weeklyAds.add(weeklyAd);
		});

		return weeklyAds;
	}

	
	private List<PromoDefinition> getAdPromotionMap(List<PromoDefinition> finalPromotions){
		
		List<PromoDefinition> adFilteredPromos = finalPromotions.stream()
				.filter(promo -> promo.getAdpage() != null && !Constants.EMPTY.equals(promo.getAdpage()))
				.collect(Collectors.toList());
		
		return adFilteredPromos;
	}
	
	
	private HashMap<LocationKey, String> getDivisionNameLookup(){
		return (HashMap<LocationKey, String>) divisionCache.stream().collect(Collectors.toMap(LocationDTO::getLocationKey, LocationDTO::getName));
	}
	
	/*private List<AholdPromoInputDTO> getApprovedPromo(List<AholdPromoInputDTO> allPromos) {
		return allPromos.stream().filter(p -> p.getStatus().equalsIgnoreCase(APPROVED)).collect(Collectors.toList());
	}*/

	private void findAndFillCalendarId(List<AholdPromoInputDTO> approvedPromos) throws ParseException {
		RetailCalendarPromoCacheService promoCacheService = new RetailCalendarPromoCacheService(retailCalendarPromoCache);

		for (AholdPromoInputDTO promo : approvedPromos) {
			logger.debug("promo.getPromoWeekNo():" + promo.getPromoWeekNo() + ",promo.getPromoYear():" + promo.getPromoYear());
			logger.debug(promo.toDebugLog());
			RetailCalendarDTO startCalendar = promoCacheService.getDayCalendarOfWeekStartDate(promo.getPromoWeekNo(), promo.getPromoYear());
			RetailCalendarDTO endCalendar = promoCacheService.getDayCalendarOfWeekEndDate(promo.getPromoWeekNo(), promo.getPromoYear());

			promo.setWeekStartDayCalendarId(startCalendar.getCalendarId());
			promo.setWeekEndDayCalendarId(endCalendar.getCalendarId());

			promo.setWeekStartDay(appDateFormatter.parse(startCalendar.getStartDate()));
			promo.setWeekEndDay(appDateFormatter.parse(endCalendar.getStartDate()));
		}
	}

	private void findAndFillLocationId(List<AholdPromoInputDTO> approvedPromos) {
		HashMap<String, LocationDTO> divisionIdMap = new HashMap<String, LocationDTO>();

		// find division id
		divisionNameMap.forEach((k, v) -> {
			Optional<LocationDTO> divisionDTO = null;
			//look for description
			divisionDTO = divisionCache.stream().filter(p -> p.desc != null && p.desc.equalsIgnoreCase(v)).findFirst();
			//if it's not matching, 
			if (divisionDTO.isPresent()) {
				divisionIdMap.put(k, divisionDTO.get());
			} else {
				divisionDTO = divisionCache.stream().filter(p -> p.name != null && p.name.equalsIgnoreCase(v)).findFirst();
				if (divisionDTO.isPresent()) {
					divisionIdMap.put(k, divisionDTO.get());
				}
			}
		});

		// Fill location id
		approvedPromos.forEach(promo -> {
			LocationDTO divisionDTO = divisionIdMap.get(promo.getSingleDivision().trim().toUpperCase());
			if (divisionDTO != null) {
				promo.setLocationKey(new LocationKey(divisionDTO.levelId, divisionDTO.locationId));
			} else {
				promo.setLocationKey(new LocationKey(0, 0));
			}
		});
	}

	private void findAndFillProductKey(List<AholdPromoInputDTO> approvedPromos) {
		approvedPromos.forEach(promo -> {
			if (promo.isItem()) {
				String retailerItemCode = promo.parseRetailerItemCode();
//				logger.debug("retailerItemCode:" + retailerItemCode);
				List<ItemDTO> allNonLigItems = retailerItemCodeAndItsItem.get(retailerItemCode);
				//Look for active items, if there are no active items, pick all in-active items
				//this is to handle loading past files, where many of the items would have become
				//inactive now
//				List<ItemDTO> activeNonLigItems = allNonLigItems.stream().filter(p -> p.isActive()).collect(Collectors.toList());
//				List<ItemDTO> finalItems = activeNonLigItems.size() > 0 ?  activeNonLigItems : allNonLigItems;
				
				if (allNonLigItems != null) {
					allNonLigItems.forEach(item -> {
						promo.getPromoItemCodes().add(item.getItemCode());
					});

				}
			} else {
				// LIG
				RetailerLikeItemGroupDTO likeItemGroupDTO = retLirNameAndLIG.get(promo.getPricelineName());
				int retLirId = likeItemGroupDTO != null ? likeItemGroupDTO.getRetLirId() : 0;
				List<ItemDTO> ligMembers = retLirIdAndItsItem.get(retLirId);
				if (ligMembers != null) {
					ligMembers.forEach(item -> {
						promo.getPromoItemCodes().add(item.getItemCode());
					});
				}
			}
		});
	}

	private HashMap<String, List<AholdPromoInputDTO>> groupPromoByWeek(List<AholdPromoInputDTO> approvedPromos) {
		return (HashMap<String, List<AholdPromoInputDTO>>) approvedPromos.stream().collect(Collectors.groupingBy(AholdPromoInputDTO::getWeek));
	}

	private HashMap<String, List<AholdPromoInputDTO>> groupPromoByGroupName(List<AholdPromoInputDTO> approvedPromos) {
		return (HashMap<String, List<AholdPromoInputDTO>>) approvedPromos.stream()
				.filter(p -> p.getGroupName() != null && !p.getGroupName().isEmpty())
				.collect(Collectors.groupingBy(AholdPromoInputDTO::getGroupName));
	}

	private HashMap<String, List<AholdPromoInputDTO>> groupPromoByPromoGroup(List<AholdPromoInputDTO> approvedPromos) {
		return (HashMap<String, List<AholdPromoInputDTO>>) approvedPromos.stream().filter(p -> p.getGroupName() == null || p.getGroupName().isEmpty())
				.collect(Collectors.groupingBy(AholdPromoInputDTO::getPromoGroup));
	}

	private List<PromoDefinition> processGroup(HashMap<String, List<AholdPromoInputDTO>> promoGroup) {
		List<PromoDefinition> finalPromotions = new ArrayList<PromoDefinition>();
		// each promo group (grouped by group name or promo group)
		promoGroup.forEach((k, v) -> {
			// Group promotion by location
			HashMap<LocationKey, List<AholdPromoInputDTO>> promoByLocation = groupPromoByLocation(v);
			HashMap<LocationKey, List<PromoDefinition>> locationAndItsPromo = new HashMap<LocationKey, List<PromoDefinition>>();

			// Process each location
			promoByLocation.forEach((locationKey, v1) -> {

				// Identify if promotion is BTGO
				boolean isBTGOPromo = isBTGO(v1);

				if (isBTGOPromo) {
					// Get validate rows
					validatePromotionData(v1);
					
					List<AholdPromoInputDTO> validRows = v1.stream().filter(p -> !p.isRowIgnored()).collect(Collectors.toList());
					
					// validate promotion
					if (validRows.size() > 0 && validateBTGO(validRows)) {
						// process promotion
						PromoDefinition promoDefinition = handleBTGO(validRows);

						// add promotion
						addPromoToLocation(promoDefinition, locationKey, locationAndItsPromo);
					} else {
						logger.error("Invalid BTGO is ignored :" + v1.get(0).logError());
					}

				} else {

					List<AholdPromoInputDTO> filteredRows = ignoreNonAdRowWhenAdRowIsThere(v1);

					List<AholdPromoInputDTO> filteredRows2 = ignoreEDLPWhenOtherPromoIsThere(filteredRows);

					HashMap<String, List<AholdPromoInputDTO>> groupByPrimaryTactics = groupByPrimaryTactics(filteredRows2);

					// Process each promotion
					groupByPrimaryTactics.forEach((k2, v2) -> {

						assignPromoType(k2, v2);

						findSalePrice(v2);

						// Log if there is no calendar id or location id or product key or promo type id is null or average
						// EDLP is 0 ignore that entire group. At this point the entire promotion will be validated
						
						validatePromotionData(v2);
						List<AholdPromoInputDTO> validRows = v2.stream().filter(p -> !p.isRowIgnored()).collect(Collectors.toList());
						
						if (validRows.size() > 0) {
							PromoDefinition promoDefinition = processEachPromotion(validRows);
							addPromoToLocation(promoDefinition, locationKey, locationAndItsPromo);
						}
					});
				}
			});

			// Group similar promotion (so that same promotion for different locations are not duplicated)
			HashMap<PromoDefinition, List<LocationKey>> groupByPromotion = groupSimilarPromotions(locationAndItsPromo);

			// Assign location
			assignLocation(groupByPromotion);

			finalPromotions.addAll(groupByPromotion.keySet());
		});

		return finalPromotions;
	}

	private HashMap<LocationKey, List<AholdPromoInputDTO>> groupPromoByLocation(List<AholdPromoInputDTO> promoRows) {
		return (HashMap<LocationKey, List<AholdPromoInputDTO>>) promoRows.stream().collect(Collectors.groupingBy(AholdPromoInputDTO::getLocationKey));
	}

	private boolean isBTGO(List<AholdPromoInputDTO> promoRows) {
		boolean isBTGO = false;

		for (AholdPromoInputDTO promoRow : promoRows) {

			boolean isBTGOInOverlayTactics = false;
			if(promoRow.getOverlayTactic() != null){
				Matcher m = btgoPattern.matcher(promoRow.getOverlayTactic());
				if (m.find()) {
					isBTGOInOverlayTactics = true;
				}	
			}else{
				logger.debug(promoRow.toDebugLog());
			}
			

			if (promoRow.getGroupName().toUpperCase().contains(BTGO1) || promoRow.getGroupName().toUpperCase().contains(BTGO2)
					|| isBTGOInOverlayTactics) {
				isBTGO = true;
				break;
			}
		}

		promoRows.forEach(promoRow -> {
			// Set promo type
			promoRow.setPromoType(PromoTypeLookup.BUX_X_GET_Y_DIFF);
		});

		return isBTGO;
	}

	private void addPromoToLocation(PromoDefinition promoDefinition, LocationKey locationKey,
			HashMap<LocationKey, List<PromoDefinition>> locationAndItsPromo) {

		if (promoDefinition != null) {
			List<PromoDefinition> promos = new ArrayList<PromoDefinition>();
			if (locationAndItsPromo.get(locationKey) != null) {
				promos = locationAndItsPromo.get(locationKey);
			}
			promos.add(promoDefinition);
			locationAndItsPromo.put(locationKey, promos);
		}
	}

	/**
	 * If there are rows with both "Ad" & "non-ad", then ignore rows with "non-ad" within same promo group
	 * 
	 * @param promoRows
	 * @return
	 */
	private List<AholdPromoInputDTO> ignoreNonAdRowWhenAdRowIsThere(List<AholdPromoInputDTO> promoRows) {
		List<AholdPromoInputDTO> output = promoRows;

		List<AholdPromoInputDTO> nonAdRows = promoRows.stream().filter(p -> p.getAd().toUpperCase().equals(NON_AD)).collect(Collectors.toList());
		List<AholdPromoInputDTO> adRows = promoRows.stream().filter(p -> !p.getAd().toUpperCase().equals(NON_AD)).collect(Collectors.toList());

		if (adRows.size() > 0 && nonAdRows.size() > 0) {
			output = adRows;
		}

		return output;
	}

	/**
	 * If there is EDLP promotion and other promotion, ignore EDLP rows within same promo group
	 * 
	 * @param promoRows
	 * @return
	 */
	private List<AholdPromoInputDTO> ignoreEDLPWhenOtherPromoIsThere(List<AholdPromoInputDTO> promoRows) {
		List<AholdPromoInputDTO> output = promoRows;

		List<AholdPromoInputDTO> edlpRows = promoRows.stream().filter(p -> p.getPrimaryTactic().equals(EDLP)).collect(Collectors.toList());
		List<AholdPromoInputDTO> nonEDLPRows = promoRows.stream().filter(p -> !p.getPrimaryTactic().equals(EDLP)).collect(Collectors.toList());

		if (edlpRows.size() > 0 && nonEDLPRows.size() > 0) {
			output = nonEDLPRows;
		}

		return output;
	}

	private HashMap<String, List<AholdPromoInputDTO>> groupByPrimaryTactics(List<AholdPromoInputDTO> promoRows) {
		return (HashMap<String, List<AholdPromoInputDTO>>) promoRows.stream().collect(Collectors.groupingBy(AholdPromoInputDTO::getPrimaryTactic));
	}

	private boolean validateBTGO(List<AholdPromoInputDTO> promoRows) {
		boolean isValidBTGO = false;
		boolean isOwnBrandPresent = false, isOtherBrandPresent = false;
		// Check if there are more than one rows and one row with "OWN BRAND" and other with other text

		if (promoRows.size() >= 2) {
			for (AholdPromoInputDTO promoRow : promoRows) {
				if (promoRow.getBrand().toUpperCase().contains(BTGO_OWN_BRAND)) {
					isOwnBrandPresent = true;
				} else {
					isOtherBrandPresent = true;
				}
			}

			if (isOwnBrandPresent && isOtherBrandPresent) {
				isValidBTGO = true;
			} else {
				for (AholdPromoInputDTO promoRow : promoRows) {
					promoRow.setIgnoreReason(promoRow.getIgnoreReason() + ",Invalid BTGO - Own Brand & National Brand not present");
				}
			}
		} else {
			for (AholdPromoInputDTO promoRow : promoRows) {
				promoRow.setIgnoreReason(promoRow.getIgnoreReason() + ",Invalid BTGO - Only one row present");
			}
		}

		return isValidBTGO;
	}

	private boolean validatePromotionData(List<AholdPromoInputDTO> promoRows) {
		boolean isValidPromotion = true;

		for (AholdPromoInputDTO promoRow : promoRows) {

			if (promoRow.getLocationKey().getLocationLevelId() == 0) {
				promoRow.setRowIgnored(true);
				promoRow.setIgnoreReason("Invalid Location");
			} else if (promoRow.getWeekStartDayCalendarId() == 0) {
				promoRow.setRowIgnored(true);
				promoRow.setIgnoreReason("Invalid Week");
			} else if (promoRow.getWeekEndDayCalendarId() == 0) {
				promoRow.setRowIgnored(true);
				promoRow.setIgnoreReason("Invalid Week");
			} else if (promoRow.getPromoType() == null) {
				promoRow.setRowIgnored(true);
				promoRow.setIgnoreReason("Invalid Promo Type");
			} else if (promoRow.getPromoItemCodes().size() == 0) {
				promoRow.setRowIgnored(true);
				promoRow.setIgnoreReason(IGNORE_REASON_INVALID_ITEM);
			} else if (promoRow.getAverageEDLP() == 0) {
				promoRow.setRowIgnored(true);
				promoRow.setIgnoreReason("Average EDLP is 0");
			}

			if (promoRow.isRowIgnored()) {
				ignoredPromos.add(promoRow);
				isValidPromotion = false;
//				logger.debug("AholdPromoInputDTO:" + promoRow.toDebugLog());
//				break;
			}
		}
		return isValidPromotion;
	}

	private void assignPromoType(String primaryTactics, List<AholdPromoInputDTO> promoRows) {

		PromoTypeLookup promoTypeLookup = null;
		String subPromoType = "";

		if (primaryTactics.equals(BOGO)) {
			promoTypeLookup = PromoTypeLookup.BOGO;
		} else if (dollarOffPattern.matcher(primaryTactics).find()) {
			promoTypeLookup = PromoTypeLookup.STANDARD;
			subPromoType = DOLLAR_OFF;
		} else if (primaryTactics.contains(PENNY)) {
			promoTypeLookup = PromoTypeLookup.STANDARD;
			subPromoType = PENNY;
		} else if (pctOffPattern.matcher(primaryTactics).find()) {
			promoTypeLookup = PromoTypeLookup.STANDARD;
			subPromoType = PCT_OFF;
		} else if (salePricePattern.matcher(primaryTactics).find()) {
			promoTypeLookup = PromoTypeLookup.STANDARD;
		} else if (mustBuyPattern.matcher(primaryTactics).find()) {
			promoTypeLookup = PromoTypeLookup.MUST_BUY;
		} else if (bxgyPattern.matcher(primaryTactics).find()) {
			promoTypeLookup = PromoTypeLookup.BUX_X_GET_Y_SAME;
		} else if (primaryTactics.equals(EDLP)) {
			promoTypeLookup = PromoTypeLookup.STANDARD;
			subPromoType = EDLP;
		}

		for (AholdPromoInputDTO promoRow : promoRows) {
			promoRow.setPromoType(promoTypeLookup);
			promoRow.setSubPromoType(subPromoType);
		}
	}

	private void findSalePrice(List<AholdPromoInputDTO> promoRows) {

		promoRows.forEach(r -> {
			MultiplePrice salePrice = null;
			if (r.getPromoType() == PromoTypeLookup.BOGO) {
				salePrice = new MultiplePrice(2, r.getAverageEDLP());
			} else if (r.getPromoType() == PromoTypeLookup.STANDARD && r.getSubPromoType().equals(DOLLAR_OFF)) {
				if (r.getTargetRetailPerUnit() > 0) {
					salePrice = new MultiplePrice(1, r.getTargetRetailPerUnit());
				} else if (r.getAverageEDLP() > 0) {
					Double dollarOff = getDollarOff(r);
					if (dollarOff != null) {
						salePrice = new MultiplePrice(1, (r.getAverageEDLP() - dollarOff));
					}
				}
			} else if (r.getPromoType() == PromoTypeLookup.STANDARD && r.getSubPromoType().equals(PENNY)) {
				if (r.getTargetRetailPerUnit() > 0) {
					salePrice = new MultiplePrice(1, r.getTargetRetailPerUnit());
				} else if (r.getAverageEDLP() > 0 && r.getPack() > 0) {
					double finalPrice = r.getAverageEDLP() - ((r.getPromoBIB() / r.getPack()) + r.getPerUnitScan() + r.getAdditionalScan());
					salePrice = new MultiplePrice(1, finalPrice);
				}
			} else if (r.getPromoType() == PromoTypeLookup.STANDARD && r.getSubPromoType().equals(PCT_OFF)) {
				if (r.getTargetRetailPerUnit() > 0) {
					salePrice = new MultiplePrice(1, r.getTargetRetailPerUnit());
				} else if (r.getAverageEDLP() > 0) {
					double pctOff = getPCTOff(r);
					double finalPrice = PRFormatHelper
							.roundToTwoDecimalDigitAsDouble(((double) r.getAverageEDLP() / (r.getAverageEDLP() * (pctOff / 100))));
					salePrice = new MultiplePrice(1, finalPrice);
				}
			} else if (r.getPromoType() == PromoTypeLookup.STANDARD && r.getSubPromoType().isEmpty()) {
				Matcher m = salePricePattern.matcher(r.getPrimaryTactic());
				if(m.find()) {
					if (m.group(1) == null) {
						salePrice = new MultiplePrice(1, Double.valueOf(m.group(3)));
					} else {
						salePrice = new MultiplePrice(Integer.valueOf(m.group(2)), Double.valueOf(m.group(3)));
					}
				}
			} else if (r.getPromoType() == PromoTypeLookup.MUST_BUY) {
				Matcher m = mustBuyPattern.matcher(r.getPrimaryTactic());
				if(m.find()) {
					salePrice = new MultiplePrice(Integer.valueOf(m.group(1)), Double.valueOf(m.group(2)));
				}
			}
			r.setSalePrice(salePrice);
		});
	}

	private PromoDefinition processEachPromotion(List<AholdPromoInputDTO> promoRows) {
		PromoDefinition promoDefinition = null;

		AholdPromoInputDTO representingData = promoRows.get(0);
		// Identify promotion type
		if (representingData.getPromoType() == PromoTypeLookup.BOGO) {
			promoDefinition = handleBOGO(promoRows);
		} else if (representingData.getPromoType() == PromoTypeLookup.STANDARD && representingData.getSubPromoType().equals(DOLLAR_OFF)) {
			promoDefinition = handleStandardDollarOff(promoRows);
		} else if (representingData.getPromoType() == PromoTypeLookup.STANDARD && representingData.getSubPromoType().equals(PENNY)) {
			promoDefinition = handleStandardPenny(promoRows);
		} else if (representingData.getPromoType() == PromoTypeLookup.STANDARD && representingData.getSubPromoType().equals(PCT_OFF)) {
			promoDefinition = handleStandardPCTOff(promoRows);
		} else if (representingData.getPromoType() == PromoTypeLookup.STANDARD && representingData.getSubPromoType().equals("")) {
			promoDefinition = handleStandard(promoRows);
		} else if (representingData.getPromoType() == PromoTypeLookup.MUST_BUY) {
			promoDefinition = handleMustBuy(promoRows);
		} else if (representingData.getPromoType() == PromoTypeLookup.BUX_X_GET_Y_SAME) {
			promoDefinition = handleBxGySame(promoRows);
		} else if (representingData.getPromoType() == PromoTypeLookup.BUX_X_GET_Y_DIFF) {
			promoDefinition = handleBTGO(promoRows);
		} else if (representingData.getPromoType() == PromoTypeLookup.STANDARD && representingData.getSubPromoType().equals(EDLP)) {
			promoDefinition = handleEDLP(promoRows);
		}

		return promoDefinition;
	}

	private PromoDefinition handleStandardPCTOff(List<AholdPromoInputDTO> promoRows) {
		AholdPromoInputDTO repRow = promoRows.get(0);
		// PM_PROMO_DEFINITION
		PromoDefinition promoDefinition = setPromoDefinition(PromoTypeLookup.STANDARD.getPromoTypeId(), promoRows);

		promoDefinition.setEdlpPromoFlag(String.valueOf(Constants.YES));
		
		// PM_PROMO_BUY_REQUIREMENT
		PromoBuyRequirement buyReq = new PromoBuyRequirement();
		buyReq.setBuyX(repRow.getSalePrice().multiple);

		// PM_PROMO_BUY_ITEM
		promoDefinition.setBuyItems(setPromoBuyItem(promoRows));

		// PM_PROMO_OFFER_DETAIL
		PromoOfferDetail promoOfferDetail = new PromoOfferDetail();
		promoOfferDetail.setPromoOfferTypeId(Constants.OFFER_TYPE_OFF);
		promoOfferDetail.setOfferUnitType(Constants.OFFER_UNIT_TYPE_PERCENTAGE);
		promoOfferDetail.setOfferValue(getPCTOff(repRow));
		buyReq.addOfferDetail(promoOfferDetail);

		promoDefinition.addPromoBuyRequirement(buyReq);

		addAdditionalOffers(promoDefinition, repRow);

		return promoDefinition;
	}

	private PromoDefinition handleStandardPenny(List<AholdPromoInputDTO> promoRows) {
		AholdPromoInputDTO repRow = promoRows.get(0);

		// PM_PROMO_DEFINITION
		PromoDefinition promoDefinition = setPromoDefinition(PromoTypeLookup.STANDARD.getPromoTypeId(), promoRows);

		promoDefinition.setEdlpPromoFlag(String.valueOf(Constants.YES));
		
		// PM_PROMO_BUY_ITEM
		promoDefinition.setBuyItems(setPromoBuyItem(promoRows));

		double off = 0;
		if (repRow.getPerUnitScan() > 0) {
			off = repRow.getPerUnitScan();
		} else if (repRow.getAverageEDLP() > 0 && repRow.getPack() > 0) {
			off = (repRow.getPromoBIB() / repRow.getPack()) + repRow.getPerUnitScan() + repRow.getAdditionalScan();
		}
		
		// Promo offer details
		if(off > 0){
			PromoBuyRequirement buyReq = new PromoBuyRequirement();
			
			PromoOfferDetail promoOfferDetail = new PromoOfferDetail();
			promoOfferDetail.setPromoOfferTypeId(Constants.OFFER_TYPE_OFF);
			promoOfferDetail.setOfferUnitType(Constants.OFFER_UNIT_TYPE_DOLLAR);
			
			promoOfferDetail.setOfferValue(off);
			buyReq.addOfferDetail(promoOfferDetail);
			promoDefinition.addPromoBuyRequirement(buyReq);
		}
		
		addAdditionalOffers(promoDefinition, repRow);

		return promoDefinition;
	}

	private PromoDefinition handleStandardDollarOff(List<AholdPromoInputDTO> promoRows) {
		AholdPromoInputDTO repRow = promoRows.get(0);

		// PM_PROMO_DEFINITION
		PromoDefinition promoDefinition = setPromoDefinition(PromoTypeLookup.STANDARD.getPromoTypeId(), promoRows);

		promoDefinition.setEdlpPromoFlag(String.valueOf(Constants.YES));
		
		// PM_PROMO_BUY_REQUIREMENT
		PromoBuyRequirement buyReq = new PromoBuyRequirement();
		buyReq.setBuyX(repRow.getSalePrice().multiple);

		// PM_PROMO_BUY_ITEM
		promoDefinition.setBuyItems(setPromoBuyItem(promoRows));

		// PM_PROMO_OFFER_DETAIL
		PromoOfferDetail promoOfferDetail = new PromoOfferDetail();
		promoOfferDetail.setPromoOfferTypeId(Constants.OFFER_TYPE_OFF);
		promoOfferDetail.setOfferUnitType(Constants.OFFER_UNIT_TYPE_DOLLAR);
		promoOfferDetail.setOfferValue(getDollarOff(repRow));
		buyReq.addOfferDetail(promoOfferDetail);

		promoDefinition.addPromoBuyRequirement(buyReq);

		addAdditionalOffers(promoDefinition, repRow);

		return promoDefinition;
	}

	private PromoDefinition handleStandard(List<AholdPromoInputDTO> promoRows) {
		AholdPromoInputDTO repRow = promoRows.get(0);

		// PM_PROMO_DEFINITION
		PromoDefinition promoDefinition = setPromoDefinition(PromoTypeLookup.STANDARD.getPromoTypeId(), promoRows);

		// PM_PROMO_BUY_ITEM
		promoDefinition.setBuyItems(setPromoBuyItem(promoRows));

		addAdditionalOffers(promoDefinition, repRow);

		return promoDefinition;
	}

	private PromoDefinition handleMustBuy(List<AholdPromoInputDTO> promoRows) {
		AholdPromoInputDTO repRow = promoRows.get(0);
		// PM_PROMO_DEFINITION
		PromoDefinition promoDefinition = setPromoDefinition(PromoTypeLookup.MUST_BUY.getPromoTypeId(), promoRows);

		// PM_PROMO_BUY_REQUIREMENT
		PromoBuyRequirement buyReq = new PromoBuyRequirement();
		buyReq.setMustBuyQty(repRow.getSalePrice().multiple);
		buyReq.setMustBuyAmt(repRow.getSalePrice().price);

		// PM_PROMO_BUY_ITEM
		promoDefinition.setBuyItems(setPromoBuyItem(promoRows));

		promoDefinition.addPromoBuyRequirement(buyReq);

		addAdditionalOffers(promoDefinition, repRow);

		return promoDefinition;
	}

	private PromoDefinition handleBOGO(List<AholdPromoInputDTO> promoRows) {
		AholdPromoInputDTO repRow = promoRows.get(0);

		// PM_PROMO_DEFINITION
		PromoDefinition promoDefinition = setPromoDefinition(PromoTypeLookup.BOGO.getPromoTypeId(), promoRows);

		// PM_PROMO_BUY_REQUIREMENT
		PromoBuyRequirement buyReq = new PromoBuyRequirement();
		buyReq.setBuyAndGetIsSame(String.valueOf(Constants.YES));
		buyReq.setBuyX(1);

		// PM_PROMO_BUY_ITEM
		promoDefinition.setBuyItems(setPromoBuyItem(promoRows));

		// PM_PROMO_OFFER_DETAIL
		PromoOfferDetail offerDetail = new PromoOfferDetail();
		offerDetail.setPromoOfferTypeId(Constants.OFFER_TYPE_FREE_ITEM);
		offerDetail.setOfferUnitType(Constants.OFFER_UNIT_TYPE_NUMBER);
		offerDetail.setOfferUnitCount(1);
		buyReq.addOfferDetail(offerDetail);

		promoDefinition.addPromoBuyRequirement(buyReq);

		addAdditionalOffers(promoDefinition, repRow);

		return promoDefinition;
	}

	private PromoDefinition handleEDLP(List<AholdPromoInputDTO> promoRows) {
		AholdPromoInputDTO repRow = promoRows.get(0);

		// PM_PROMO_DEFINITION
		PromoDefinition promoDefinition = setPromoDefinition(PromoTypeLookup.STANDARD.getPromoTypeId(), promoRows);

		// Set flag for EDLP promotion
		promoDefinition.setEdlpPromoFlag(String.valueOf(Constants.YES));
		
		// PM_PROMO_BUY_ITEM
		promoDefinition.setBuyItems(setPromoBuyItem(promoRows));

		addAdditionalOffers(promoDefinition, repRow);

		return promoDefinition;
	}

	private PromoDefinition handleBxGySame(List<AholdPromoInputDTO> promoRows) {
		AholdPromoInputDTO repRow = promoRows.get(0);

		Matcher m = bxgyPattern.matcher(repRow.getPrimaryTactic());

		// PM_PROMO_DEFINITION
		PromoDefinition promoDefinition = setPromoDefinition(PromoTypeLookup.BUX_X_GET_Y_SAME.getPromoTypeId(), promoRows);
		int buyX = 0, getY = 0;
				
		if (m.find()) {
			buyX = Integer.valueOf(m.group(1));
			getY = Integer.valueOf(m.group(2));
		}
		
		// PM_PROMO_BUY_REQUIREMENT
		PromoBuyRequirement buyReq = new PromoBuyRequirement();
		buyReq.setBuyAndGetIsSame(String.valueOf(Constants.YES));
		buyReq.setBuyX(buyX);

		// PM_PROMO_BUY_ITEM
		promoDefinition.setBuyItems(setPromoBuyItem(promoRows));

		// PM_PROMO_OFFER_DETAIL
		PromoOfferDetail offerDetail = new PromoOfferDetail();
		offerDetail.setPromoOfferTypeId(Constants.OFFER_TYPE_FREE_ITEM);
		offerDetail.setOfferUnitType(Constants.OFFER_UNIT_TYPE_NUMBER);
		offerDetail.setOfferUnitCount(getY);
		buyReq.addOfferDetail(offerDetail);

		promoDefinition.addPromoBuyRequirement(buyReq);

		addAdditionalOffers(promoDefinition, repRow);

		return promoDefinition;
	}

	private PromoDefinition handleBTGO(List<AholdPromoInputDTO> promoRows) {
		// PM_PROMO_DEFINITION
		PromoDefinition promoDefinition = setPromoDefinition(PromoTypeLookup.BUX_X_GET_Y_DIFF.getPromoTypeId(), promoRows);

		// PM_PROMO_BUY_REQUIREMENT
		PromoBuyRequirement buyReq = new PromoBuyRequirement();
		buyReq.setBuyAndGetIsSame(String.valueOf(Constants.NO));
		buyReq.setBuyX(1);

		// Filter promo items
		List<AholdPromoInputDTO> otherBrandRows = promoRows.stream().filter(p -> !p.getBrand().toUpperCase().equals(BTGO_OWN_BRAND))
				.collect(Collectors.toList());

		// PM_PROMO_BUY_ITEM
		promoDefinition.setBuyItems(setPromoBuyItem(otherBrandRows));

		// PM_PROMO_OFFER_DETAIL
		PromoOfferDetail offerDetail = new PromoOfferDetail();
		offerDetail.setPromoOfferTypeId(Constants.OFFER_TYPE_FREE_ITEM);
		offerDetail.setOfferUnitType(Constants.OFFER_UNIT_TYPE_NUMBER);
		offerDetail.setOfferUnitCount(1);

		List<AholdPromoInputDTO> ownBrands = promoRows.stream().filter(p -> p.getBrand().toUpperCase().equals(BTGO_OWN_BRAND))
				.collect(Collectors.toList());

		// PM_PROMO_OFFER_ITEM
		for (AholdPromoInputDTO promoRow : ownBrands) {
			promoRow.getPromoItemCodes().forEach(itemCode -> {
				PromoOfferItem offerItem = new PromoOfferItem();
				offerItem.setItemCode(itemCode);
				offerDetail.addOfferItems(offerItem);
			});
		}

		buyReq.addOfferDetail(offerDetail);

		promoDefinition.addPromoBuyRequirement(buyReq);

		return promoDefinition;
	}

	private void addAdditionalOffers(PromoDefinition promoDefinition, AholdPromoInputDTO promoRow) {
		boolean addGas = false, bxGetDollar = false, buyDollarGetDollar = false;
		boolean hasAdditionalOffer = false;

		if (promoRow.getOverlayTactic() != null || !promoRow.getOverlayTactic().isEmpty()) {
			if (promoRow.getOverlayTactic().toUpperCase().contains(GAS)) {
				addGas = true;
				hasAdditionalOffer = true;
			} else if (bxGDollarPattern.matcher(promoRow.getOverlayTactic()).find()) {
				bxGetDollar = true;
				hasAdditionalOffer = true;
			} else if (bDollarGDollarPattern.matcher(promoRow.getOverlayTactic()).find()) {
				buyDollarGetDollar = true;
				hasAdditionalOffer = true;
			} 
		}
		
		if (hasAdditionalOffer && addGas) {
			PromoBuyRequirement buyReq = new PromoBuyRequirement();

			PromoOfferDetail promoOfferDetail = new PromoOfferDetail();
			promoOfferDetail.setPromoOfferTypeId(Constants.OFFER_TYPE_GAS_POINTS);
			promoOfferDetail.setOfferUnitType(Constants.OFFER_UNIT_TYPE_NUMBER);
			// There is no gas points in the feed
			promoOfferDetail.setOfferValue(0d);
			buyReq.addOfferDetail(promoOfferDetail);

			promoDefinition.addPromoBuyRequirement(buyReq);

		} else if (hasAdditionalOffer && bxGetDollar) {
			PromoBuyRequirement buyReq = new PromoBuyRequirement();
			Matcher m = bxGDollarPattern.matcher(promoRow.getOverlayTactic());
			if (m.find()) {
				int buyX = Integer.valueOf(m.group(1));
				Double getDollar = Double.valueOf(m.group(2));

				buyReq.setBuyX(buyX);

				PromoOfferDetail promoOfferDetail = new PromoOfferDetail();
				promoOfferDetail.setPromoOfferTypeId(Constants.OFFER_TYPE_INSTANT_SAVINGS);
				promoOfferDetail.setOfferUnitType(Constants.OFFER_UNIT_TYPE_DOLLAR);
				promoOfferDetail.setOfferValue(getDollar);
				buyReq.addOfferDetail(promoOfferDetail);

				promoDefinition.addPromoBuyRequirement(buyReq);
			}

		} else if (hasAdditionalOffer && buyDollarGetDollar) {
			PromoBuyRequirement buyReq = new PromoBuyRequirement();
			Matcher m = bDollarGDollarPattern.matcher(promoRow.getOverlayTactic());
			if (m.find()) {
				Double buyDollar = Double.valueOf(m.group(1));
				Double getDollar = Double.valueOf(m.group(2));

				buyReq.setMinAmtReqd(buyDollar);

				PromoOfferDetail promoOfferDetail = new PromoOfferDetail();
				promoOfferDetail.setPromoOfferTypeId(Constants.OFFER_TYPE_INSTANT_SAVINGS);
				promoOfferDetail.setOfferUnitType(Constants.OFFER_UNIT_TYPE_DOLLAR);
				promoOfferDetail.setOfferValue(getDollar);
				buyReq.addOfferDetail(promoOfferDetail);

				promoDefinition.addPromoBuyRequirement(buyReq);
			}
		} else if (hasAdditionalOffer) {
			logger.error("Unhandled additional offer found for :" + promoRow.logError());
		}
	}

	private PromoDefinition setPromoDefinition(int promoTypeId, List<AholdPromoInputDTO> promoRows) {
		AholdPromoInputDTO representingData = promoRows.get(0);
		PromoDefinition promoDefinition = new PromoDefinition();
		promoDefinition.setPromoTypeId(promoTypeId);
		promoDefinition.setStartCalId(representingData.getWeekStartDayCalendarId());
		promoDefinition.setEndCalId(representingData.getWeekEndDayCalendarId());
		promoDefinition.setPromoStartDate(representingData.getWeekStartDay());
		promoDefinition.setPromoEndDate(representingData.getWeekEndDay());
		String adDesc = representingData.getAd().replaceAll(" ", "").toLowerCase();
		if (adDesc.equals(AD_OTHER.replaceAll(" ", "").toLowerCase())) {

			promoDefinition.setAdpage("2");
			promoDefinition.setBlockNum("1");
			
		} else if (adDesc.equals(AD_FRONT.replaceAll(" ", "").toLowerCase())) {
			
			promoDefinition.setAdpage("1");
			promoDefinition.setBlockNum("1");
			
		} else if (adDesc.equals(AD_CLIP.replaceAll(" ", "").toLowerCase())) {

			promoDefinition.setAdpage("3");
			promoDefinition.setBlockNum("1");
			
		} else if (adDesc.equals(AD_WRAP.replaceAll(" ", "").toLowerCase())){
			
			promoDefinition.setAdpage("4");
			promoDefinition.setBlockNum("1");
		}
		//find name based on promo type as certain type of promotions are 
		//grouped at "Group name" level
		if(promoTypeId == PromoTypeLookup.BOGO.getPromoTypeId() || promoTypeId == PromoTypeLookup.BUX_X_GET_Y_DIFF.getPromoTypeId() 
				 || promoTypeId == PromoTypeLookup.BUX_X_GET_Y_SAME.getPromoTypeId()) {
			promoDefinition.setPromoName(representingData.getGroupName().isEmpty() ? representingData.getPromoGroup() : representingData.getGroupName());	
		} else {
			promoDefinition.setPromoName(representingData.getPromoGroup());
		}
		
		promoDefinition.setAddtlDetails(representingData.getDescription());
		promoDefinition.setCreatedBy(Constants.BATCH_USER);
		promoDefinition.setApprovedBy(Constants.BATCH_USER);
		
		setPromotionStatus(promoDefinition, representingData);
		
		return promoDefinition;
	}
	
	/**
	 * Sets promotion status
	 * @param promoDefinition
	 * @param aholdPromoInputDTO
	 */
	private void setPromotionStatus(PromoDefinition promoDefinition, AholdPromoInputDTO aholdPromoInputDTO){
		
		if (aholdPromoInputDTO.getStatus().equalsIgnoreCase(APPROVED)) {

			promoDefinition.setStatus(PromoStatusLookup.APPROVED.getPromoStatusId());
		
		} else if (aholdPromoInputDTO.getStatus().equalsIgnoreCase(PENDING)) {
		
			promoDefinition.setStatus(PromoStatusLookup.TO_BE_REVIEWED.getPromoStatusId());
		
		} else if (aholdPromoInputDTO.getStatus().equalsIgnoreCase(MODIFY)) {
		
			promoDefinition.setStatus(PromoStatusLookup.MODIFY.getPromoStatusId());
		
		} else if (aholdPromoInputDTO.getStatus().equalsIgnoreCase(PROCESSED)) {
			
			promoDefinition.setStatus(PromoStatusLookup.PROCESSED.getPromoStatusId());

		} else if (aholdPromoInputDTO.getStatus().equalsIgnoreCase(WAITING_FOR_APPROVAL)) {

			promoDefinition.setStatus(PromoStatusLookup.WAITING_FOR_APPROVAL.getPromoStatusId());
			
		} else if (aholdPromoInputDTO.getStatus().equalsIgnoreCase(RETURNED_FROM_PROCESSED)) {

			promoDefinition.setStatus(PromoStatusLookup.RETURNED_FROM_PROCESSED.getPromoStatusId());
			
		} else if (aholdPromoInputDTO.getStatus().equalsIgnoreCase(RETURNED_CM)) {

			promoDefinition.setStatus(PromoStatusLookup.RETURNED_FROM_CM.getPromoStatusId());
			
		} else if (aholdPromoInputDTO.getStatus().equalsIgnoreCase(RETURNED)) {

			promoDefinition.setStatus(PromoStatusLookup.RETURNED.getPromoStatusId());
			
		} else {
			
			promoDefinition.setStatus(PromoStatusLookup.UNKNOWN.getPromoStatusId());
			unkownStatus.add(aholdPromoInputDTO.getStatus());
		
		}
		
	}

	private List<PromoBuyItems> setPromoBuyItem(List<AholdPromoInputDTO> promoRows) {

		Set<Integer> itemCodes = new HashSet<Integer>();
		List<PromoBuyItems> buyItems = new ArrayList<>();
		promoRows.forEach(promoRow -> {
			promoRow.getPromoItemCodes().forEach(item -> {
				PromoBuyItems buyItem = new PromoBuyItems();
				buyItem.setItemCode(item);
				buyItem.setActualStartCalId(promoRow.getWeekStartDayCalendarId());
				buyItem.setActualEndCalId(promoRow.getWeekEndDayCalendarId());
				buyItem.setCasePack(promoRow.getPack());
				buyItem.setRegQty(1);
				buyItem.setRegPrice(promoRow.getAverageEDLP());

				if (promoRow.getSalePrice() != null) {
					buyItem.setSaleQty(promoRow.getSalePrice().multiple);
					if (promoRow.getSalePrice().multiple > 1) {
						buyItem.setSaleMPrice(promoRow.getSalePrice().price);
					} else {
						buyItem.setSalePrice(promoRow.getSalePrice().price);
					}
				}
				// Item may get duplicated
				if(!itemCodes.contains(item)) {
					buyItems.add(buyItem);	
				}
				
				itemCodes.add(item);
			});

		});
		
		//Just fo debugging
		HashMap<Integer, List<PromoBuyItems>> groupByItemCode = (HashMap<Integer, List<PromoBuyItems>>) buyItems.stream()
				.collect(Collectors.groupingBy(PromoBuyItems::getItemCode));

		groupByItemCode.forEach((k,v) -> {
			if(v.size()>1) {
				logger.debug(promoRows.get(0).toDebugLog());	
			}
		});
		//Just fo debugging
		
		return buyItems;
	}

	private HashMap<PromoDefinition, List<LocationKey>> groupSimilarPromotions(HashMap<LocationKey, List<PromoDefinition>> locationAndItsPromo) {
		HashMap<PromoDefinition, List<LocationKey>> groupByPromotion = new HashMap<PromoDefinition, List<LocationKey>>();
		// Group by promotion
		locationAndItsPromo.forEach((locationKey, promos) -> {
			promos.forEach(promo -> {
				List<LocationKey> locations = new ArrayList<LocationKey>();
				if (groupByPromotion.get(promo) != null) {
					locations = groupByPromotion.get(promo);
				}
				locations.add(locationKey);
				groupByPromotion.put(promo, locations);
			});
		});
		return groupByPromotion;
	}

	private void assignLocation(HashMap<PromoDefinition, List<LocationKey>> groupByPromotion) {
		groupByPromotion.forEach((promo, locations) -> {
			List<PromoLocation> promoLocations = new ArrayList<PromoLocation>();
			locations.forEach(location -> {
				PromoLocation promoLocation = new PromoLocation();
				promoLocation.setLocationLevelId(location.getLocationLevelId());
				promoLocation.setLocationId(location.getLocationId());
				promoLocations.add(promoLocation);
			});
			promo.setPromoLocation(promoLocations);
		});
	}

	private void logIgnoredPromos(int totalRows) {
		if (ignoredPromos.size() > 0) {
			logger.error("Following promo's are ignored due to one of the following reasons. "
					+ "1.No matching calendar Id. 2.No matching location Id. 3.No matching item. 4.No matching promotion. " + "5.Average EDLP is 0");
			
			logger.error("** Error Summary for week: " + processingWeek + " ***");
			
			HashMap<String, List<AholdPromoInputDTO>> groupedError = (HashMap<String, List<AholdPromoInputDTO>>) ignoredPromos.stream()
					.collect(Collectors.groupingBy(AholdPromoInputDTO::getIgnoreReason));
			
			groupedError.forEach((k, v) -> {
				if (k.equals(IGNORE_REASON_INVALID_ITEM)) {
					HashMap<String, List<AholdPromoInputDTO>> distinctLig = (HashMap<String, List<AholdPromoInputDTO>>) v.stream()
							.filter(p -> !p.isItem()).collect(Collectors.groupingBy(AholdPromoInputDTO::getPricelineName));

					HashMap<String, List<AholdPromoInputDTO>> distinctNewLineLig = (HashMap<String, List<AholdPromoInputDTO>>) v.stream()
							.filter(p -> !p.isItem() && p.getPricelineName().startsWith("NEW_"))
							.collect(Collectors.groupingBy(AholdPromoInputDTO::getPricelineName));

					HashMap<String, List<AholdPromoInputDTO>> distinctNonLig = (HashMap<String, List<AholdPromoInputDTO>>) v.stream()
							.filter(p -> p.isItem()).collect(Collectors.groupingBy(AholdPromoInputDTO::getPricelineName));

					logger.error("No of rows ignored because of: **" + k + "**:" + v.size() + "/" + totalRows);

					logger.error("------No of distinct LIGs (including NEW_*) not found in Presto -> (DistinctLig/TotalRows)" + "("
							+ distinctLig.size() + "/" + v.stream().filter(p -> !p.isItem()).distinct().count() + ")");

					logger.error("------No of distinct NEW_* LIGs -> (DistinctLig/TotalRows)" + "(" + distinctNewLineLig.size() + "/"
							+ v.stream().filter(p -> !p.isItem() && p.getPricelineName().startsWith("NEW_")).distinct().count() + ")");

					logger.error("------No of distinct non-ligs not found in Presto -> (DistinctNonLig/TotalRows)" + "(" + distinctNonLig.size() + "/"
							+ v.stream().filter(p -> p.isItem()).distinct().count() + ")");
				} else {
					logger.error("No of rows ignored because of: **" + k + "**:" + v.size() + "/" + totalRows);
				}
			});
			
			if(unkownStatus.size() > 0)
				logger.warn("** Unkown status: "
					+ unkownStatus.stream().collect(Collectors.joining(",")));

			logger.error("** Error Summary ***");
			
			ignoredPromos.forEach(promoRow -> {
				logger.debug(promoRow.logError());
			});
		}
	}

	private void mapFieldNames(TreeMap<Integer, String> fieldNames) {
		fieldNames.put(0, "categoryName");
		fieldNames.put(1, "timeSelection");
		fieldNames.put(2, "week");
		fieldNames.put(3, "groupName");
		fieldNames.put(4, "promoGroup");
		fieldNames.put(5, "pricelineKey");
		fieldNames.put(6, "pricelineName");
		fieldNames.put(7, "division");
		fieldNames.put(8, "singleDivision");
		fieldNames.put(9, "description");
		fieldNames.put(10, "brand");
		fieldNames.put(11, "leadItemCode");
		fieldNames.put(12, "status");
		fieldNames.put(13, "statusDetails");
		fieldNames.put(14, "primaryTactic");
		fieldNames.put(15, "overlayTactic");
		fieldNames.put(16, "ad");
		fieldNames.put(17, "display");
		fieldNames.put(18, "pack");
		fieldNames.put(19, "caseListCost");
		fieldNames.put(20, "unitListCost");
		fieldNames.put(21, "everydayOI");
		fieldNames.put(22, "promoOI");
		fieldNames.put(23, "everdayAccrual");
		fieldNames.put(24, "promoBIB");
		fieldNames.put(25, "perUnitScan");
		fieldNames.put(26, "additionalScan");
		fieldNames.put(27, "netCaseCost");
		fieldNames.put(28, "netUnitCost");
		fieldNames.put(29, "averageEDLP");
		fieldNames.put(30, "edlpMargin");
		fieldNames.put(31, "targetRetailPerUnit");
		fieldNames.put(32, "pennyProfitPerUnit");
		//fieldNames.put(33, "targetMargin");
		fieldNames.put(33, "adWeekStartDate");
		fieldNames.put(34, "adWeekEndDate");
		fieldNames.put(35, "eventId");
		fieldNames.put(36, "dealStartDate");
		fieldNames.put(37, "dealEndDate");
	}

	private double getPCTOff(AholdPromoInputDTO promoRow) {
		Double pctOff = null;
		Matcher m = pctOffPattern.matcher(promoRow.getPrimaryTactic());
		if (m.find()) {
			pctOff = Double.valueOf(m.group(1));
		}
		return pctOff;
	}

	private double getDollarOff(AholdPromoInputDTO promoRow) {
		Double dolarOff = null;
		Matcher m = dollarOffPattern.matcher(promoRow.getPrimaryTactic());
		if (m.find()) {
			dolarOff = Double.valueOf(m.group(1));
		}
		return dolarOff;
	}
}
