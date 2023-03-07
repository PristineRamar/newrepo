package com.pristine.dataload.gianteagle;

import java.io.IOException;
import java.sql.Connection;
import java.text.DateFormat;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.time.LocalDate;
import java.time.format.DateTimeFormatter;
import java.time.temporal.ChronoUnit;
import java.util.ArrayList;
import java.util.Calendar;
import java.util.Date;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.concurrent.TimeUnit;
import java.util.Set;
import java.util.TreeMap;
import java.util.stream.Collectors;

import org.apache.log4j.Logger;
import org.apache.log4j.PropertyConfigurator;

import com.pristine.dao.CompStoreDAO;
import com.pristine.dao.DBManager;
import com.pristine.dao.ItemDAO;
import com.pristine.dao.RetailCalendarDAO;
import com.pristine.dao.RetailCostDAO;
import com.pristine.dao.RetailPriceDAO;
import com.pristine.dao.offermgmt.promotion.PromotionDAO;
import com.pristine.dataload.tops.PriceDataLoad;
import com.pristine.dto.ItemDTO;
import com.pristine.dto.RetailCalendarDTO;
import com.pristine.dto.RetailPriceDTO;
import com.pristine.dto.fileformatter.gianteagle.GERewardPromotionDTO;
import com.pristine.dto.offermgmt.LocationKey;
import com.pristine.dto.offermgmt.MultiplePrice;
import com.pristine.dto.offermgmt.promotion.PromoBuyItems;
import com.pristine.dto.offermgmt.promotion.PromoBuyRequirement;
import com.pristine.dto.offermgmt.promotion.PromoDefinition;
import com.pristine.dto.offermgmt.promotion.PromoLocation;
import com.pristine.dto.offermgmt.promotion.PromoOfferDetail;
import com.pristine.dto.offermgmt.promotion.PromoOfferItem;
import com.pristine.exception.GeneralException;
import com.pristine.lookup.offermgmt.promotion.GEPromoCodeLookup;
import com.pristine.lookup.offermgmt.promotion.PromoTypeLookup;
import com.pristine.parsinginterface.PristineFileParser;
import com.pristine.util.Constants;
import com.pristine.util.DateUtil;
import com.pristine.util.PrestoUtil;
import com.pristine.util.PristineDBUtil;
import com.pristine.util.PropertyManager;
import com.pristine.util.offermgmt.PRFormatHelper;

@SuppressWarnings("rawtypes")
public class RewardPromotionSetup extends PristineFileParser {
	private static Logger logger = Logger.getLogger("RewardPromotionSetup");
	private final static String INPUT_FOLDER = "INPUT_FOLDER=";
	@SuppressWarnings("unused")
	private String rootPath, startDate = null;
	private static String relativeInputPath, processingFile;
	static TreeMap<Integer, String> allColumns = new TreeMap<Integer, String>();
	static TreeMap<Integer, String> allColumns1 = new TreeMap<Integer, String>();
	static TreeMap<Integer, String> allColumns2 = new TreeMap<Integer, String>();
	static TreeMap<Integer, String> allColumns3 = new TreeMap<Integer, String>();
	private Connection conn = null;
	static String dateStr = null;

	HashMap<String, Integer> retailPriceZone = new HashMap<String, Integer>();
	HashMap<String, Set<ItemDTO>> retailerItemCodeAndPrestoCode = new HashMap<>();
	int chainId, dayCalendarId, prevCalendarId = -1;
	HashMap<String, Integer> compStoreDetails = new HashMap<>();
	List<GERewardPromotionDTO> promotionDetail = new ArrayList<>();
	List<GERewardPromotionDTO> itemDetail = new ArrayList<>();
	List<GERewardPromotionDTO> priceItemGroup = new ArrayList<>();
	List<GERewardPromotionDTO> locationDetail = new ArrayList<>();
	PriceDataLoad objPriceLoad;

	Set<String> ignoredZoneNo = new HashSet<>();
	Set<String> ignoredStoreNo = new HashSet<>();
	Set<String> newPromoCodes = new HashSet<>();
	Set<String> priceNotFound = new HashSet<>();
	Set<String> priceNotFoundForLocation = new HashSet<>();
	DateFormat appDateFormatter = new SimpleDateFormat(Constants.APP_DATE_FORMAT);
	HashMap<String, Integer> weekCalendarMap = new HashMap<String, Integer>();
	HashMap<String, Integer> calendarMap = new HashMap<String, Integer>();
	RetailCalendarDAO calDAO = new RetailCalendarDAO();
	int NO_OF_FUTURE_WEEKS_TO_PROCESS = Integer
			.parseInt(PropertyManager.getProperty("NO_OF_FUTURE_PROMO_TO_BE_LOADED", "13"));
	DateTimeFormatter formatter = DateTimeFormatter.ofPattern(Constants.APP_DATE_FORMAT);
	private HashMap<Integer, HashMap<String, String>> dsdAndWhseZoneMap = null;
 	private HashMap<Integer, Integer> itemCodeCategoryMap = null;
 	
	public RewardPromotionSetup() {
		objPriceLoad = new PriceDataLoad();
		PropertyManager.initialize("analysis.properties");

		try {
			conn = DBManager.getConnection();

		} catch (GeneralException exe) {
			logger.error("Error while connecting DB:" + exe);
			logger.info("Search List Summary Daily Ends unsucessfully");
			System.exit(1);
		}
	}

	public static void main(String[] args) {

		PropertyConfigurator.configure("log4j-Reward-Promotion-Setup.properties");
		PropertyManager.initialize("analysis.properties");
		for (String arg : args) {
			if (arg.startsWith(INPUT_FOLDER)) {
				relativeInputPath = arg.substring(INPUT_FOLDER.length());
			}
		}

		// Default week type to current week if it is not specified

		Calendar c = Calendar.getInstance();
		int dayIndex = 0;
		if (PropertyManager.getProperty("CUSTOM_WEEK_START_INDEX") != null) {
			dayIndex = Integer.parseInt(PropertyManager.getProperty("CUSTOM_WEEK_START_INDEX"));
		}
		if (dayIndex > 0)
			c.set(Calendar.DAY_OF_WEEK, dayIndex + 1);
		else
			c.set(Calendar.DAY_OF_WEEK, Calendar.SUNDAY);
		DateFormat dateFormat = new SimpleDateFormat("MM/dd/yyyy");
		if (Constants.CURRENT_WEEK.equalsIgnoreCase(args[1])) {
			dateStr = dateFormat.format(c.getTime());
		} else if (Constants.NEXT_WEEK.equalsIgnoreCase(args[1])) {
			c.add(Calendar.DATE, 7);
			dateStr = dateFormat.format(c.getTime());
		} else if (Constants.LAST_WEEK.equalsIgnoreCase(args[1])) {
			c.add(Calendar.DATE, -7);
			dateStr = dateFormat.format(c.getTime());
		} else if (Constants.SPECIFIC_WEEK.equalsIgnoreCase(args[1])) {
			try {
				dateStr = DateUtil.getWeekStartDate(DateUtil.toDate(args[2]), 0);
			} catch (GeneralException exception) {
				logger.error("Error when parsing date - " + exception.toString());
				System.exit(-1);
			}
		}
		try {
			RewardPromotionSetup RewardPromo = new RewardPromotionSetup();
			RewardPromo.processFile();
		} catch (GeneralException | Exception e) {
			e.printStackTrace();
			logger.error("Error occured in RewardPromotionSetup() main method", e);
		}
	}

	@SuppressWarnings("unchecked")
	private void processFile() throws GeneralException, Exception {

		super.headerPresent = true;
		rootPath = PropertyManager.getProperty("DATALOAD.ROOTPATH", "");
		populateCalendarId(dateStr);
		fillPristineRewardPromotion();
		fillPristineRewardPromotionItem();
		fillPristineRewardPromotionItemGroup();
		fillPristineRewardPromotionZoneStore();
		
		logger.info("setupObjects() - ***ROLL_UP_DSD_TO_WARHOUSE_ZONE is enabled***");
		logger.info("setupObjects() - Getting DSD and Warehouse zone mapping...");
		dsdAndWhseZoneMap = new RetailCostDAO().getDSDAndWHSEZoneMap(conn, null);
		logger.info("setupObjects() - Getting DSD and Warehouse zone mapping is completed.");
		
		logger.info("setupObjects() - Getting Item and category mapping...");
		itemCodeCategoryMap = new ItemDAO().getCategoryAndItemCodeMap(conn, null);
		logger.info("setupObjects() - Getting Item and category mapping is completed.");

		logger.info("Reward Promotion file processing Started... ");
		try {
			parseFile();
			processRewardPromotion();
			PristineDBUtil.commitTransaction(conn, "batch record update");
			logger.info("Reward Promotion completed Sucessfully...");
		} catch (IOException | GeneralException e) {
			logger.error("Error -- processFile()", e);
			PristineDBUtil.rollbackTransaction(conn, "Unexpected Exception");
		} finally {
			PristineDBUtil.close(getOracleConnection());
		}
	}

	private void processRewardPromotion() throws Exception, GeneralException {

		if (promotionDetail.size() > 0 && itemDetail.size() > 0 && priceItemGroup.size() > 0
				&& locationDetail.size() > 0) {
			PromotionDAO promoDAO = new PromotionDAO(conn);
			HashMap<String, List<GERewardPromotionDTO>> rewardPromotions = mergeRewardPromotions();

			// Assign Presto item code based on retailer item code
			HashMap<String, List<GERewardPromotionDTO>> rewardPromoWithItemCode = setPrestoItemCodes(rewardPromotions);

			// Assign Regular Price for all the items in each Promotion type
			HashMap<String, List<GERewardPromotionDTO>> rewardPromoWithPrice = setPriceForAllItems(
					rewardPromoWithItemCode);

			identifyPromoTypeAndSetSalePrice(rewardPromoWithPrice);

			List<PromoDefinition> promoDefinitionList = convertFileObjToPromoDefinition(rewardPromoWithPrice);

			logger.info("Save promotion started...");
			SimpleDateFormat format = new SimpleDateFormat(Constants.APP_DATE_FORMAT);
			Date processingWeek = null;
			if (dateStr != null)
				try {
					processingWeek = format.parse(dateStr);
				} catch (ParseException e) {
					logger.error("Error while parsing week end date - ");
				}
			else
				processingWeek = new Date();
			promoDAO.savePromotionV2(promoDefinitionList, processingWeek, true);
			// for(PromoDefinition promotion: promoDefinitionList){
			// promoDAO.savePromotion(promotion);
			// }
			logger.info("Reward Promotion related details saved sucessfully....");
			logger.warn("List of Zones skipped which is not available in Db:"
					+ ignoredZoneNo.stream().collect(Collectors.joining(",")));
			logger.warn("List of Stores skipped which is not available in Db:"
					+ ignoredStoreNo.stream().collect(Collectors.joining(",")));
			logger.warn("New Promotion Type found. Promotion Code: "
					+ newPromoCodes.stream().collect(Collectors.joining(",")));
			logger.warn("Price not found for locations: "
					+ priceNotFoundForLocation.stream().collect(Collectors.joining(",")));
			logger.warn("Price not found for items: "
					+ priceNotFound.stream().collect(Collectors.joining(",")));

		} else {
			logger.info("Size of Promo file: " + promotionDetail.size() + " Reward Item file Size: " + itemDetail.size()
					+ " Price Item group size: " + priceItemGroup.size() + " Reward Promotion Location details: "
					+ locationDetail.size());
			throw new Exception("One of the reward Promotion file is not found while processing Reward promotion");

		}
	}

	private List<PromoDefinition> convertFileObjToPromoDefinition(
			HashMap<String, List<GERewardPromotionDTO>> rewardPromoWithItemCode) throws GeneralException, Exception {

		List<PromoDefinition> finalPromotionList = new ArrayList<>();
		for (Map.Entry<String, List<GERewardPromotionDTO>> entry : rewardPromoWithItemCode.entrySet()) {
			HashMap<List<GERewardPromotionDTO>, Set<LocationKey>> promoItemsAndLocation = getLocationBasedOnItemWithPrice(
					entry.getValue());

			for (Entry<List<GERewardPromotionDTO>, Set<LocationKey>> promoItemAndLoc : promoItemsAndLocation
					.entrySet()) {
				PromoDefinition promoDefinition = new PromoDefinition();
				
				List<GERewardPromotionDTO> mainItems = promoItemAndLoc.getKey().stream()
				.filter(item -> item.getItemTypeCode().equals("M")).collect(Collectors.toList());
				if(mainItems.size() > 0){
					GERewardPromotionDTO geRewardPromotionDTO = mainItems.stream().findAny().get();
					setPromoDefAndBuyItemDetails(geRewardPromotionDTO, promoItemAndLoc.getKey(), promoItemAndLoc.getValue(),
							promoDefinition);	
					finalPromotionList.addAll(splitPromoByWeek(promoDefinition, DateUtil.toDate(dateStr)));	
				}
			}
		}
		return finalPromotionList;
	}

	private void setPromoDefAndBuyItemDetails(GERewardPromotionDTO geRewardPromotionDTO,
			List<GERewardPromotionDTO> promoItemList, Set<LocationKey> locations, PromoDefinition promoDefinition)
					throws GeneralException {

		promoDefinition.setPromoStartDate(DateUtil.toDate(geRewardPromotionDTO.getStartDate()));
		promoDefinition.setPromoEndDate(DateUtil.toDate(geRewardPromotionDTO.getEndDate()));
		promoDefinition.setPromoNumber(geRewardPromotionDTO.getPromoCode());
		promoDefinition.setPromoName(
				(geRewardPromotionDTO.getRewardDesc() != null && !geRewardPromotionDTO.getRewardDesc().trim().isEmpty())
						? geRewardPromotionDTO.getRewardDesc() : geRewardPromotionDTO.getPromoDesc());
		promoDefinition.setPromoDefnTypeId(geRewardPromotionDTO.getPromoTypeId());
		promoDefinition.setPromoTypeId(geRewardPromotionDTO.getPromoTypeId());
		promoDefinition.setApprovedBy(Constants.BATCH_USER);
		promoDefinition.setCreatedBy(Constants.BATCH_USER);

		// Set Promo Buy Items
		List<PromoBuyItems> buyItemList = new ArrayList<>();
		promoItemList.forEach(buyItem -> {

			// Consider only Mandatory item in Buy items.
			// Some Promotion types were not handled, in those cases consider both Mandatory and Reward items in Buy items
			if (buyItem.getItemTypeCode().toUpperCase().trim().equals("M")
					|| geRewardPromotionDTO.getPromoTypeId() == PromoTypeLookup.REWARD_PROMO.getPromoTypeId()) {
				PromoBuyItems promoBuyItems = new PromoBuyItems();
				promoBuyItems.setItemCode(buyItem.getItemCode());
				promoBuyItems.setRegQty(buyItem.getRegularPrice().multiple);
				promoBuyItems.setRegPrice(
						Double.valueOf(PRFormatHelper.roundToTwoDecimalDigit(buyItem.getRegularPrice().price)));
				if (buyItem.getSalePrice() != null) {
					promoBuyItems.setSaleQty(buyItem.getSalePrice().multiple);
					promoBuyItems.setSalePrice(
							Double.valueOf(PRFormatHelper.roundToTwoDecimalDigit(buyItem.getSalePrice().price)));
				}

				buyItemList.add(promoBuyItems);
			}
		});
		HashMap<Integer, PromoBuyItems> itemAndPromoBuyItems = new HashMap<>();
		buyItemList.forEach(item -> {
			itemAndPromoBuyItems.put(item.getItemCode(), item);
		});

		promoDefinition.setBuyItems(itemAndPromoBuyItems.values().stream().collect(Collectors.toList()));

		// Set Promo Buy Requirements details
		setBuyReqAndOfferDetail(promoDefinition, geRewardPromotionDTO, promoItemList);

		// Set Location details
		setLocationDetails(promoDefinition, locations);

		// //Set Calendar Id
		// PromoAndAdLoader promoAndAdLoader = new PromoAndAdLoader();
		// promoAndAdLoader.setCalendarId(promoDefinition);
		// promoAndAdLoader.setActualCalendarInfoForLongTermPromo(promoDefinition, promoDefinition.getPromoStartDate(),
		// promoDefinition.getPromoEndDate());
	}

	private void setLocationDetails(PromoDefinition promoDefinition, Set<LocationKey> locations) {
		List<PromoLocation> promoLocations = new ArrayList<>();
		locations.forEach(loc -> {
			PromoLocation promoLocation = new PromoLocation();
			promoLocation.setLocationId(loc.getLocationId());
			promoLocation.setLocationLevelId(loc.getLocationLevelId());
			promoLocations.add(promoLocation);
		});

		promoDefinition.setPromoLocation(promoLocations);
	}

	private void setBuyReqAndOfferDetail(PromoDefinition promoDefinition, GERewardPromotionDTO geRewardPromotionDTO,
			List<GERewardPromotionDTO> promoItemList) {
		List<PromoBuyRequirement> buyReqList = new ArrayList<>();
		PromoBuyRequirement promoBuyRequirement = new PromoBuyRequirement();

		if (geRewardPromotionDTO.getPromoTypeId() == PromoTypeLookup.BOGO.getPromoTypeId()) {
			promoBuyRequirement.setBuyAndGetIsSame(String.valueOf(Constants.YES));
			promoBuyRequirement.setBuyX((geRewardPromotionDTO.getScanQty() - geRewardPromotionDTO.getRewardQty()));
		} else if (geRewardPromotionDTO.getPromoTypeId() == PromoTypeLookup.BUX_X_GET_Y_SAME.getPromoTypeId()) {
			promoBuyRequirement.setBuyAndGetIsSame(String.valueOf(Constants.YES));
			promoBuyRequirement.setBuyX(geRewardPromotionDTO.getScanQty() - geRewardPromotionDTO.getRewardQty());
		} else if (geRewardPromotionDTO.getPromoTypeId() == PromoTypeLookup.BUX_X_GET_Y_DIFF.getPromoTypeId()) {
			promoBuyRequirement.setBuyAndGetIsSame(String.valueOf(Constants.NO));
			promoBuyRequirement.setBuyX(geRewardPromotionDTO.getScanQty());
		} else if (geRewardPromotionDTO.getPromoTypeId() == PromoTypeLookup.MUST_BUY.getPromoTypeId()
				|| geRewardPromotionDTO.getPromoTypeId() == PromoTypeLookup.PICK_5.getPromoTypeId()) {
			promoBuyRequirement.setMustBuyQty(geRewardPromotionDTO.getScanQty());
			promoBuyRequirement.setMustBuyAmt(geRewardPromotionDTO.getSalePrice().price);
		}

		else if (geRewardPromotionDTO.getPromoTypeId() == PromoTypeLookup.REWARD_PROMO.getPromoTypeId()) {
			promoBuyRequirement.setBuyX(geRewardPromotionDTO.getScanQty());
		} else {
			logger.debug("New Promotype Id found in Reward Promotion processing. Promotion Type Id: "
					+ geRewardPromotionDTO.getPromoTypeId());
		}
		promoBuyRequirement.setOfferDetail(setOfferDetailAndOfferItems(geRewardPromotionDTO, promoItemList));
		buyReqList.add(promoBuyRequirement);
		promoDefinition.setPromoBuyRequirement(buyReqList);
	}

	private ArrayList<PromoOfferDetail> setOfferDetailAndOfferItems(GERewardPromotionDTO geRewardPromotionDTO,
			List<GERewardPromotionDTO> promoItemList) {
		ArrayList<PromoOfferDetail> promoOfferDetails = new ArrayList<PromoOfferDetail>();
		PromoOfferDetail promoOfferDetail = null;
		if (geRewardPromotionDTO.getPromoTypeId() == PromoTypeLookup.BOGO.getPromoTypeId()) {
			promoOfferDetail = new PromoOfferDetail();
			if(geRewardPromotionDTO.getPromoRewardPct() > 0){
				promoOfferDetail.setPromoOfferTypeId(Constants.OFFER_TYPE_OFF);
				promoOfferDetail.setOfferUnitType(Constants.OFFER_UNIT_TYPE_PERCENTAGE);
				promoOfferDetail.setOfferValue(geRewardPromotionDTO.getPromoRewardPct());
				promoOfferDetail.setOfferUnitCount(geRewardPromotionDTO.getRewardQty());
			}else{
				promoOfferDetail.setPromoOfferTypeId(Constants.OFFER_TYPE_FREE_ITEM);
				promoOfferDetail.setOfferUnitType(Constants.OFFER_UNIT_TYPE_NUMBER);
				promoOfferDetail.setOfferUnitCount(1);	
			}
		}
		else if (geRewardPromotionDTO.getPromoTypeId() == PromoTypeLookup.BUX_X_GET_Y_SAME.getPromoTypeId()) {
			promoOfferDetail = new PromoOfferDetail();
			if(geRewardPromotionDTO.getPromoRewardPct() > 0){
				promoOfferDetail.setPromoOfferTypeId(Constants.OFFER_TYPE_OFF);
				promoOfferDetail.setOfferUnitType(Constants.OFFER_UNIT_TYPE_PERCENTAGE);
				promoOfferDetail.setOfferValue(geRewardPromotionDTO.getPromoRewardPct());
				promoOfferDetail.setOfferUnitCount(geRewardPromotionDTO.getRewardQty());
			}else if(geRewardPromotionDTO.getPromoRewardAmt() > 0){
				promoOfferDetail.setPromoOfferTypeId(Constants.OFFER_TYPE_OFF);
				promoOfferDetail.setOfferUnitType(Constants.OFFER_UNIT_TYPE_DOLLAR);
				promoOfferDetail.setOfferValue(geRewardPromotionDTO.getPromoRewardAmt());
				promoOfferDetail.setOfferUnitCount(geRewardPromotionDTO.getRewardQty());
			}else{
				promoOfferDetail.setPromoOfferTypeId(Constants.OFFER_TYPE_FREE_ITEM);
				promoOfferDetail.setOfferUnitType(Constants.OFFER_UNIT_TYPE_NUMBER);
				promoOfferDetail.setOfferUnitCount(geRewardPromotionDTO.getRewardQty());
			}
		}
		else if (geRewardPromotionDTO.getPromoTypeId() == PromoTypeLookup.BUX_X_GET_Y_DIFF.getPromoTypeId()) {
			promoOfferDetail = new PromoOfferDetail();
			promoOfferDetail.setPromoOfferTypeId(Constants.OFFER_TYPE_FREE_ITEM);
			promoOfferDetail.setOfferUnitType(Constants.OFFER_UNIT_TYPE_NUMBER);
			promoOfferDetail.setOfferUnitCount(1);

			HashMap<Integer, GERewardPromotionDTO> rewardItemMap = new HashMap<>();
			promoItemList.stream().filter(item -> item.getItemTypeCode().equals("R")).forEach(item -> {
				rewardItemMap.put(item.getItemCode(), item);
			});
			promoOfferDetail.setOfferItems(setofferItems(rewardItemMap));
		}

		// else if(geRewardPromotionDTO.getPromoTypeId() == PromoTypeLookup.MUST_BUY.getPromoTypeId()){
		// promoOfferDetail = new PromoOfferDetail();
		// promoOfferDetail.setPromoOfferTypeId(Constants.OFFER_TYPE_OFF);
		// promoOfferDetail.setOfferUnitType(Constants.OFFER_UNIT_TYPE_DOLLAR);
		// promoOfferDetail.setOfferValue(geRewardPromotionDTO.getPromoRewardAmt());
		// }

		// else if(geRewardPromotionDTO.getPromoTypeId() == PromoTypeLookup.PICK_5.getPromoTypeId()){
		// promoOfferDetail = new PromoOfferDetail();
		// promoOfferDetail.setPromoOfferTypeId(Constants.OFFER_TYPE_INSTANT_SAVINGS);
		// promoOfferDetail.setOfferUnitType(Constants.OFFER_UNIT_TYPE_DOLLAR);
		// promoOfferDetail.setOfferValue(geRewardPromotionDTO.getPromoRewardAmt());
		// }

		// ::TODO Handle GROUP_LEVEL_AMOUNT_OFF & REWARD_PROMO Promotion type
		// else if(geRewardPromotionDTO.getPromoTypeId() == PromoTypeLookup.REWARD_PROMO.getPromoTypeId()){
		//
		// }

		if (promoOfferDetail != null) {
			promoOfferDetails.add(promoOfferDetail);
		}

		return promoOfferDetails;

	}

	private ArrayList<PromoOfferItem> setofferItems(HashMap<Integer, GERewardPromotionDTO> rewardItemMap) {

		ArrayList<PromoOfferItem> offerItems = new ArrayList<>();
		rewardItemMap.keySet().forEach(itemCode -> {
			PromoOfferItem promoOfferItem = new PromoOfferItem();
			promoOfferItem.setItemCode(itemCode);
			offerItems.add(promoOfferItem);
		});
		return offerItems;

	}

	public HashMap<List<GERewardPromotionDTO>, Set<LocationKey>> getLocationBasedOnItemWithPrice(
			List<GERewardPromotionDTO> rewardPromotion) {

		HashMap<Set<ItemAndPriceKey>, List<GERewardPromotionDTO>> itemKeyAndPromoDetails = new HashMap<>();
		HashMap<LocationKey, Set<ItemAndPriceKey>> locationAndItemPriceKey = new HashMap<>();
		HashMap<List<GERewardPromotionDTO>, Set<LocationKey>> promoItemsAndLocation = new HashMap<>();
		HashMap<Set<ItemAndPriceKey>, Set<LocationKey>> itemPriceKeyAndLocation = new HashMap<>();

		// Group By Location
		HashMap<LocationKey, List<GERewardPromotionDTO>> locationAndPromoDetails = new HashMap<>();
		rewardPromotion.stream().filter(promo -> promo.getLocationId() > 0 && promo.getRegularPrice() != null
				&& promo.getRegularPrice().price > 0).forEach(promoItem -> {
					LocationKey key = new LocationKey(promoItem.getLocationLevelId(), promoItem.getLocationId());
					List<GERewardPromotionDTO> tempList = new ArrayList<>();
					if (locationAndPromoDetails.get(key) != null) {
						tempList = locationAndPromoDetails.get(key);
					}
					
					tempList.add(promoItem);
					locationAndPromoDetails.put(key, tempList);
					
					// Added for creating warehouse zone location
					int productId = itemCodeCategoryMap.get(promoItem.getItemCode());
					if (dsdAndWhseZoneMap.containsKey(productId)) {
						HashMap<String, String> zoneMap = dsdAndWhseZoneMap.get(productId);
						if (zoneMap.containsKey(promoItem.getPrestoZoneNum())) {
							String whseZone = zoneMap.get(promoItem.getPrestoZoneNum());
							if (retailPriceZone.get(whseZone) != null) {
								LocationKey whseKey = new LocationKey(Constants.ZONE_LEVEL_ID, retailPriceZone.get(whseZone));
								List<GERewardPromotionDTO> tempListWhse = new ArrayList<>();
								if (locationAndPromoDetails.get(whseKey) != null) {
									tempListWhse = locationAndPromoDetails.get(whseKey);
								}
								tempListWhse.add(promoItem);
								locationAndPromoDetails.put(whseKey, tempListWhse);
							}
						}
					}
				});

		locationAndPromoDetails.forEach((key, value) -> {
			Set<ItemAndPriceKey> itemAndPriceKey = new HashSet<>();
			value.forEach(item -> {
				itemAndPriceKey.add(new ItemAndPriceKey(item.getItemCode(), item.getRegularPrice()));
			});
			itemKeyAndPromoDetails.put(itemAndPriceKey, value);
			locationAndItemPriceKey.put(key, itemAndPriceKey);
		});

		// Get Set of locations which has same item with same price
		locationAndItemPriceKey.forEach((location, promoDetails) -> {

			locationAndItemPriceKey.forEach((innerLoc, comparePromo) -> {
				if (promoDetails.equals(comparePromo)) {

					Set<LocationKey> tempLocList = new HashSet<>();
					if (itemPriceKeyAndLocation.get(promoDetails) != null) {
						tempLocList = itemPriceKeyAndLocation.get(promoDetails);
					}
					tempLocList.add(innerLoc);
					itemPriceKeyAndLocation.put(promoDetails, tempLocList);
				}
			});
		});

		itemPriceKeyAndLocation.forEach((key, value) -> {
			List<GERewardPromotionDTO> promoDetailsList = itemKeyAndPromoDetails.get(key);

			promoItemsAndLocation.put(promoDetailsList, value);
		});
		return promoItemsAndLocation;
	}

	private void identifyPromoTypeAndSetSalePrice(HashMap<String, List<GERewardPromotionDTO>> rewardPromoWithItemCode) {

		rewardPromoWithItemCode.forEach((key, value) -> {
			value.stream().filter(promo -> promo.getRegularPrice() != null && promo.getRegularPrice().price > 0)
					.forEach(promoItem -> {

				double unitPrice = promoItem.getRegularPrice().price / promoItem.getRegularPrice().multiple;

				// ::TODO temp changes to handle reward Promotion
				// Enabled appropriate promotion types rather than populating promotions as reward
				// promoItem.setPromoTypeId(PromoTypeLookup.REWARD_PROMO.getPromoTypeId());
				// True BOGO
				if (Integer.valueOf(promoItem.getPromoCode()) == GEPromoCodeLookup.TRUE_BOGO.getPromoCode()) {
					promoItem.setPromoTypeId(PromoTypeLookup.BOGO.getPromoTypeId());

					// Set Sale price and Qty based on Promo details
					promoItem.setSalePrice(new MultiplePrice(promoItem.getScanQty(), unitPrice));
				}

				// Amount Off
				else if (Integer.valueOf(promoItem.getPromoCode()) == GEPromoCodeLookup.AMOUNT_OFF.getPromoCode()) {
					if (promoItem.getPromoRewardPct() > 0) {
						if (promoItem.getScanQty() == 2) {
							promoItem.setPromoTypeId(PromoTypeLookup.BOGO.getPromoTypeId());
						} else if (promoItem.getScanQty() > 2) {
							promoItem.setPromoTypeId(PromoTypeLookup.BUX_X_GET_Y_SAME.getPromoTypeId());
						}
						double salePrice = (unitPrice * (promoItem.getScanQty() - promoItem.getRewardQty()))
								+ ((unitPrice * promoItem.getRewardQty()) / (100 / promoItem.getPromoRewardPct()));

						promoItem.setSalePrice(new MultiplePrice(promoItem.getScanQty(), salePrice));
						logger.debug("Amount of Sale Price: " + salePrice + " and Qty:"
								+ promoItem.getSalePrice().multiple + " for item code: " + promoItem.getItemCode());
					} else if (promoItem.getPromoRewardAmt() > 0) {
						if (promoItem.getRewardDesc().contains("MUST")) {
							promoItem.setPromoTypeId(PromoTypeLookup.MUST_BUY.getPromoTypeId());

							double salePrice = (promoItem.getScanQty() * unitPrice) - promoItem.getPromoRewardAmt();
							promoItem.setSalePrice(new MultiplePrice(promoItem.getScanQty(), salePrice));
						} else if (promoItem.getRewardDesc().contains("BUY " + promoItem.getScanQty())
								|| promoItem.getRewardDesc().contains("BUY" + promoItem.getScanQty())) {

							promoItem.setPromoTypeId(PromoTypeLookup.BUX_X_GET_Y_SAME.getPromoTypeId());

							double salePrice = (promoItem.getScanQty() * unitPrice) - promoItem.getPromoRewardAmt();
							promoItem.setSalePrice(new MultiplePrice(promoItem.getScanQty(), salePrice));
						} else {
							promoItem.setPromoTypeId(PromoTypeLookup.STANDARD.getPromoTypeId());

							if (promoItem.getPromoRewardAmt() > 0) {
								double salePrice = (promoItem.getScanQty() * unitPrice) - promoItem.getPromoRewardAmt();
								promoItem.setSalePrice(new MultiplePrice(promoItem.getScanQty(), salePrice));
							} else if (promoItem.getPromoRewardPct() > 0) {
								double salePrice = (unitPrice * (promoItem.getScanQty() - promoItem.getRewardQty()))
										+ ((unitPrice * promoItem.getRewardQty())
												/ (100 / promoItem.getPromoRewardPct()));

								promoItem.setSalePrice(new MultiplePrice(promoItem.getScanQty(), salePrice));
							}
						}
					}
				}

				// Free item
				else if (Integer.valueOf(promoItem.getPromoCode()) == GEPromoCodeLookup.FREE_ITEM.getPromoCode()) {
					promoItem.setPromoTypeId(PromoTypeLookup.BUX_X_GET_Y_DIFF.getPromoTypeId());
				}

				// GEAC
				else if (Integer.valueOf(promoItem.getPromoCode()) == GEPromoCodeLookup.GEAC.getPromoCode()) {
					promoItem.setPromoTypeId(PromoTypeLookup.MUST_BUY.getPromoTypeId());

					double salePrice = ((unitPrice * promoItem.getScanQty()) - promoItem.getPromoRewardAmt());
					promoItem.setSalePrice(new MultiplePrice(promoItem.getScanQty(), salePrice));
				}

				// Pick 5
				else if (Integer.valueOf(promoItem.getPromoCode()) == GEPromoCodeLookup.PICK_5.getPromoCode()) {
					promoItem.setPromoTypeId(PromoTypeLookup.PICK_5.getPromoTypeId());

					promoItem.setSalePrice(new MultiplePrice(promoItem.getScanQty(), promoItem.getPromoRewardAmt()));
				}

				// Group Level Promotion
				else if (Integer.valueOf(promoItem.getPromoCode()) == GEPromoCodeLookup.GROUP_LEVEL_AMOUNT_OFF
						.getPromoCode()) {
					promoItem.setPromoTypeId(PromoTypeLookup.REWARD_PROMO.getPromoTypeId());
				}

				// Other Promotion
				else {
					newPromoCodes.add(promoItem.getPromoCode());
					promoItem.setPromoTypeId(PromoTypeLookup.REWARD_PROMO.getPromoTypeId());
				}
			});
		});

	}

	private HashMap<String, List<GERewardPromotionDTO>> setPrestoItemCodes(
			HashMap<String, List<GERewardPromotionDTO>> rewardPromotions) throws GeneralException {
		HashMap<String, List<GERewardPromotionDTO>> finalRewardPromotions = new HashMap<>();
		ItemDAO itemDAO = new ItemDAO();
		retailerItemCodeAndPrestoCode = itemDAO.getActiveItemsFromItemLookup(conn);

		rewardPromotions.forEach((key, value) -> {
			value.forEach(promoItem -> {
				if (retailerItemCodeAndPrestoCode.get(promoItem.getRetailerItemcode()) != null) {
					for (ItemDTO itemDetaill : retailerItemCodeAndPrestoCode.get(promoItem.getRetailerItemcode())) {
						GERewardPromotionDTO promoDetail = null;
						try {
							promoDetail = (GERewardPromotionDTO) promoItem.clone();
							promoDetail.setItemCode(itemDetaill.getItemCode());
							promoDetail.setPriceGroupCode(itemDetaill.getPrcGrpCd());

							List<GERewardPromotionDTO> tempRewardPromo = new ArrayList<>();
							if (finalRewardPromotions.get(key) != null) {
								tempRewardPromo = finalRewardPromotions.get(key);
							}
							tempRewardPromo.add(promoDetail);
							finalRewardPromotions.put(key, tempRewardPromo);

						} catch (Exception e) {
							e.printStackTrace();
						}
					}
				}
			});
		});
		return finalRewardPromotions;

	}

	private HashMap<String, List<GERewardPromotionDTO>> setPriceForAllItems(
			HashMap<String, List<GERewardPromotionDTO>> rewardPromotions)
					throws GeneralException, CloneNotSupportedException {

		RetailPriceDAO retailPriceDAO = new RetailPriceDAO();
		chainId = Integer.parseInt(retailPriceDAO.getChainId(conn));
		retailPriceZone = retailPriceDAO.getRetailPriceZone(conn);

		CompStoreDAO compStoreDAO = new CompStoreDAO();
		compStoreDetails = compStoreDAO.getCompStoreData(conn, chainId, null);
		HashMap<String, List<GERewardPromotionDTO>> promoWithPriceDetails = new HashMap<>();
		// Loop each Promotion type and group item based on Location
		// Find Regular Price of each item based on location
		for (Map.Entry<String, List<GERewardPromotionDTO>> rewardPromotionMap : rewardPromotions.entrySet()) {

			HashMap<LocationKey, List<GERewardPromotionDTO>> locationAndPromotionMap = new HashMap<>();

			// Apply Zone/Store details for Mandatory items
			rewardPromotionMap.getValue().stream().filter(item -> item.getItemTypeCode().equals("M"))
					.forEach(promoItem -> {
						if (promoItem.getZoneNo() != null) {
							String zoneNum = null;
							if (promoItem.getSplrNo() != null && !promoItem.getSplrNo().trim().isEmpty()) {
								zoneNum = promoItem.getBNR_CD() + "-" + promoItem.getZoneNo() + "-"
										+ promoItem.getPriceGroupCode() + "-" + promoItem.getSplrNo();
							} else {
								zoneNum = promoItem.getBNR_CD() + "-" + promoItem.getZoneNo() + "-"
										+ promoItem.getPriceGroupCode();
							}
							if (retailPriceZone.get(zoneNum) != null) {
								promoItem.setPrestoZoneNum(zoneNum);
								promoItem.setLocationId(retailPriceZone.get(zoneNum));
								promoItem.setLocationLevelId(Constants.ZONE_LEVEL_ID);

							} else {
								ignoredZoneNo.add(zoneNum);
							}

						} else if (promoItem.getStoreNo() != null) {
							if (compStoreDetails.get(promoItem.getStoreNo()) != null) {
								promoItem.setLocationLevelId(Constants.STORE_LEVEL_ID);
								promoItem.setLocationId(compStoreDetails.get(promoItem.getStoreNo()));
							} else {
								ignoredStoreNo.add(promoItem.getStoreNo());
							}

						}
						if (promoItem.getLocationId() > 0) {
							List<GERewardPromotionDTO> tempPromotions = new ArrayList<>();
							if (promoWithPriceDetails.get(rewardPromotionMap.getKey()) != null) {
								tempPromotions = promoWithPriceDetails.get(rewardPromotionMap.getKey());
							}
							tempPromotions.add(promoItem);
							promoWithPriceDetails.put(rewardPromotionMap.getKey(), tempPromotions);
						}
					});

			// Apply location details for Reward items
			getApplyRewardPromoLocation(promoWithPriceDetails, rewardPromotionMap.getValue(),
					rewardPromotionMap.getKey());

			if (promoWithPriceDetails.get(rewardPromotionMap.getKey()) != null) {
				promoWithPriceDetails.get(rewardPromotionMap.getKey()).forEach(promoItem -> {
					LocationKey locationKey = new LocationKey(promoItem.getLocationLevelId(),
							promoItem.getLocationId());
					List<GERewardPromotionDTO> tempPromotions = new ArrayList<>();
					if (locationAndPromotionMap.get(locationKey) != null) {
						tempPromotions = locationAndPromotionMap.get(locationKey);
					}
					tempPromotions.add(promoItem);
					locationAndPromotionMap.put(locationKey, tempPromotions);

				});
			}

			for (Map.Entry<LocationKey, List<GERewardPromotionDTO>> locationAndPromotion : locationAndPromotionMap
					.entrySet()) {

				// Get Price detail of each items for a given location
				HashMap<String, RetailPriceDTO> priceDataMap = getPriceDataMap(locationAndPromotion.getKey(),
						locationAndPromotion.getValue());

				locationAndPromotion.getValue().forEach(promoItem -> {
					if (priceDataMap.get(String.valueOf(promoItem.getItemCode())) != null) {
						RetailPriceDTO retailPriceDTO = priceDataMap.get(String.valueOf(promoItem.getItemCode()));
						if (retailPriceDTO.getRegMPrice() > 0) {
							promoItem.setRegularPrice(new MultiplePrice(retailPriceDTO.getRegQty(),
									Double.valueOf(retailPriceDTO.getRegMPrice())));
						} else if (retailPriceDTO.getRegPrice() > 0) {
							promoItem.setRegularPrice(new MultiplePrice(retailPriceDTO.getRegQty(),
									Double.valueOf(retailPriceDTO.getRegPrice())));
						}

						if (retailPriceDTO.getSaleMPrice() > 0) {
							promoItem.setSalePrice(new MultiplePrice(retailPriceDTO.getSaleQty(),
									Double.valueOf(retailPriceDTO.getSaleMPrice())));
						} else if (retailPriceDTO.getSalePrice() > 0) {
							promoItem.setSalePrice(new MultiplePrice(retailPriceDTO.getSaleQty(),
									Double.valueOf(retailPriceDTO.getSalePrice())));
						}

					} else {
						logger.debug("Price not found Item: " + promoItem.getItemCode() + " in Location ID: "
								+ locationAndPromotion.getKey().getLocationId() + " and Location Level Id: "
								+ locationAndPromotion.getKey().getLocationLevelId());
						
						priceNotFound.add(String.valueOf(promoItem.getItemCode()));
					}
				});

			}
		}
		return promoWithPriceDetails;

	}

	private void getApplyRewardPromoLocation(HashMap<String, List<GERewardPromotionDTO>> promoWithPriceDetails,
			List<GERewardPromotionDTO> rewardPromotionMap, String promoCodeKey) throws CloneNotSupportedException {
		// Apply Zone/Store details for rewards items
		HashMap<Integer, GERewardPromotionDTO> rewardItems = new HashMap<>();
		rewardPromotionMap.stream().filter(item -> item.getItemTypeCode().toUpperCase().equals("R"))
				.forEach(promoItem -> {
					rewardItems.put(promoItem.getItemCode(), promoItem);
				});

		if (rewardItems.size() > 0) {
			// Reward items
			for (GERewardPromotionDTO rewardItem : rewardItems.values()) {

				// Mandatory items
				for (GERewardPromotionDTO promoItem : rewardPromotionMap) {

					if (promoItem.getLocationId() > 0) {
						GERewardPromotionDTO geRewardPromotionDTO = new GERewardPromotionDTO();
						geRewardPromotionDTO = (GERewardPromotionDTO) rewardItem.clone();
						if (promoItem.getItemTypeCode().equals("M")) {
							if (Constants.STORE_LEVEL_ID == promoItem.getLocationLevelId()) {
								geRewardPromotionDTO.setStoreNo(promoItem.getStoreNo());
							} else {
								geRewardPromotionDTO.setZoneNo(promoItem.getPrestoZoneNum());
							}
							geRewardPromotionDTO.setLocationId(promoItem.getLocationId());
							geRewardPromotionDTO.setLocationLevelId(promoItem.getLocationLevelId());
							LocationKey locationKey = new LocationKey(geRewardPromotionDTO.getLocationLevelId(),
									geRewardPromotionDTO.getLocationId());

							if (locationKey != null) {
								List<GERewardPromotionDTO> tempPromotions = new ArrayList<>();
								if (promoWithPriceDetails.get(promoCodeKey) != null) {
									tempPromotions = promoWithPriceDetails.get(promoCodeKey);
								}
								tempPromotions.add(geRewardPromotionDTO);
								promoWithPriceDetails.put(promoCodeKey, tempPromotions);
							}
						}
					}
				}
			}
		}
	}

	private HashMap<String, RetailPriceDTO> getPriceDataMap(LocationKey key, List<GERewardPromotionDTO> rewardPromoList)
			throws GeneralException {

		HashMap<String, RetailPriceDTO> priceDataMap = new HashMap<>();
		HashMap<String, HashMap<String, List<String>>> itemStoreMapping = null;
		String storeNumOrZoneNum = null;
		int levelTypeId = -1;
		Set<Integer> itemCodeSet = rewardPromoList.stream().map(GERewardPromotionDTO::getItemCode)
				.collect(Collectors.toSet());
		Set<String> itemCodes = itemCodeSet.stream().map(Object::toString).collect(Collectors.toSet());

		if (key.getLocationLevelId() == Constants.STORE_LEVEL_ID) {
			storeNumOrZoneNum = rewardPromoList.stream().findAny().get().getStoreNo();
			levelTypeId = Constants.STORE_LEVEL_TYPE_ID;
			RetailCostDAO retailCostDAO = new RetailCostDAO();
			itemStoreMapping = retailCostDAO.getStoreItemMapAtZonelevel(conn, itemCodes, storeNumOrZoneNum);
		}

		else if (key.getLocationLevelId() == Constants.ZONE_LEVEL_ID) {
			storeNumOrZoneNum = rewardPromoList.stream().findAny().get().getPrestoZoneNum();
			levelTypeId = Constants.ZONE_LEVEL_TYPE_ID;
		}

		List<String> itemCodeList = itemCodes.stream().collect(Collectors.toList());
		// Get the price information for distinct item code
		logger.debug("Get the price info");
		priceDataMap = objPriceLoad.getRetailPrice(conn, storeNumOrZoneNum, itemCodeList, dayCalendarId,
				itemStoreMapping, levelTypeId);

		if (priceDataMap.size() < 1)
			priceNotFoundForLocation.add(storeNumOrZoneNum);
		return priceDataMap;
	}

	private HashMap<String, List<GERewardPromotionDTO>> mergeRewardPromotions() {

		Map<String, List<GERewardPromotionDTO>> promoDetailMap = new HashMap<>();
		Map<String, List<GERewardPromotionDTO>> itemDetailMap = new HashMap<>();
		Map<String, List<GERewardPromotionDTO>> priceItemGroupMap = new HashMap<>();
		Map<String, List<GERewardPromotionDTO>> locationDetailMap = new HashMap<>();

		promoDetailMap = promotionDetail.stream().collect(Collectors.groupingBy(GERewardPromotionDTO::getPromoID));
		itemDetailMap = itemDetail.stream().collect(Collectors.groupingBy(GERewardPromotionDTO::getPromoID));
		priceItemGroupMap = priceItemGroup.stream().collect(Collectors.groupingBy(GERewardPromotionDTO::getPromoID));
		locationDetailMap = locationDetail.stream().collect(Collectors.groupingBy(GERewardPromotionDTO::getPromoID));

		// Merge item details with Promotion
		Map<String, List<GERewardPromotionDTO>> promoAndItemDetail = applyPromoDetails(promoDetailMap, itemDetailMap,
				"ITEM_FILE");

		// Merge promoAndItemDetail with Price item group
		Map<String, List<GERewardPromotionDTO>> promoWithPriceItemGroup = applyPromoDetails(promoAndItemDetail,
				priceItemGroupMap, "PRICE_ITEM_GROUP");

		// Merge Location information with Promotion
		Map<String, List<GERewardPromotionDTO>> promoWithLocationDetail = applyPromoDetails(promoWithPriceItemGroup,
				locationDetailMap, "LOCATION_DETAIL");
		return (HashMap<String, List<GERewardPromotionDTO>>) promoWithLocationDetail;

	}

	private Map<String, List<GERewardPromotionDTO>> applyPromoDetails(
			Map<String, List<GERewardPromotionDTO>> promoDetailMap,
			Map<String, List<GERewardPromotionDTO>> itemDetailMap, String processingType) {

		// Key: Promo Id
		HashMap<String, List<GERewardPromotionDTO>> promoAndItemDetail = new HashMap<>();
		// Promo details
		promoDetailMap.forEach((key, value) -> {
			for (GERewardPromotionDTO rewardPromo : value) {

				// Item Details
				if (itemDetailMap.get(key) != null) {
					GERewardPromotionDTO promoDetail = null;
					for (GERewardPromotionDTO itemDetail : itemDetailMap.get(key)) {
						try {
							promoDetail = (GERewardPromotionDTO) rewardPromo.clone();
						} catch (Exception e) {
							e.printStackTrace();
						}
						if (processingType.equals("ITEM_FILE")) {
							promoDetail.setItemTypeCode(itemDetail.getItemTypeCode());
							promoDetail.setItemGroup(itemDetail.getItemGroup());
							promoDetail.setItemScanQty(itemDetail.getItemScanQty());
							promoDetail.setFamilyPromoFlag(itemDetail.getFamilyPromoFlag());
							promoDetail.setRetailerItemcode(itemDetail.getRetailerItemcode());
							promoDetail.setSplrNo(itemDetail.getSplrNo());
						}

						if (processingType.equals("PRICE_ITEM_GROUP")) {
							promoDetail.setPricingItemTypeCode(itemDetail.getPricingItemTypeCode());
							promoDetail.setPricingItemGroup(itemDetail.getPricingItemGroup());
							promoDetail.setPricingRewardAmt(itemDetail.getPricingRewardAmt());
							promoDetail.setPricingRewardPct(itemDetail.getPricingRewardPct());
							promoDetail.setPricingRewardItemFreeFlag(itemDetail.getPricingRewardItemFreeFlag());
						}

						if (processingType.equals("LOCATION_DETAIL")) {
							promoDetail.setZoneNo(itemDetail.getZoneNo());
							promoDetail.setStoreNo(itemDetail.getStoreNo());
							promoDetail.setOverrideNo(itemDetail.getOverrideNo());
						}

						List<GERewardPromotionDTO> promoAndItemDetails = new ArrayList<>();
						if (promoAndItemDetail.get(promoDetail.getPromoID()) != null) {
							promoAndItemDetails = promoAndItemDetail.get(promoDetail.getPromoID());
						}
						promoAndItemDetails.add(promoDetail);
						promoAndItemDetail.put(promoDetail.getPromoID(), promoAndItemDetails);
					}
				} else if (processingType.equals("PRICE_ITEM_GROUP")) {
					try {
						GERewardPromotionDTO promoPriceGroup = (GERewardPromotionDTO) rewardPromo.clone();
						List<GERewardPromotionDTO> promoAndItemDetails = new ArrayList<>();
						if (promoAndItemDetail.get(promoPriceGroup.getPromoID()) != null) {
							promoAndItemDetails = promoAndItemDetail.get(promoPriceGroup.getPromoID());
							promoAndItemDetails.add(promoPriceGroup);
							promoAndItemDetail.put(promoPriceGroup.getPromoID(), promoAndItemDetails);
						}

					} catch (Exception e) {
						logger.error("Error in applyPromoDetails while cloning Price item group related items");
						e.printStackTrace();
					}

				} else {
					logger.error("Item or Zone Infomation is not found for Reward Promo Id: " + key);
				}
			}
		});

		return promoAndItemDetail;
	}

	@SuppressWarnings({ "unchecked" })
	@Override
	public void processRecords(List listobj) throws GeneralException {
		List<GERewardPromotionDTO> rewardPromotionDTO = (List<GERewardPromotionDTO>) listobj;
		if (processingFile.equals("PROMOTION_FILE")) {
			promotionDetail.addAll(rewardPromotionDTO);
		} else if (processingFile.equals("ITEM_FILE")) {
			itemDetail.addAll(rewardPromotionDTO);
		} else if (processingFile.equals("PRICE_ITEM_GROUP")) {
			priceItemGroup.addAll(rewardPromotionDTO);
		} else if (processingFile.equals("LOCATION_DETAIL")) {
			locationDetail.addAll(rewardPromotionDTO);
		}

	}

	/**
	 * Get the Path of Input file, and delimiting.
	 * 
	 * @param fieldNames
	 * @throws GeneralException
	 * @throws IOException
	 */
	@SuppressWarnings("unchecked")
	private void parseFile() throws GeneralException, IOException {

		try {
			// getzip files
			ArrayList<String> zipFileList = getZipFiles(relativeInputPath);
			headerPresent = true;
			// Start with -1 so that if any regular files are present, they are processed first
			int curZipFileCount = -1;
			boolean processZipFile = false;

			String zipFilePath = getRootPath() + "/" + relativeInputPath;
			do {
				ArrayList<String> fileList = null;
				boolean commit = true;

				try {
					if (processZipFile)
						PrestoUtil.unzip(zipFileList.get(curZipFileCount), zipFilePath);

					fileList = getFiles(relativeInputPath);

					for (int i = 0; i < fileList.size(); i++) {

						String files = fileList.get(i);
						logger.info("File Name - " + files);
						int stopCount = Integer.parseInt(PropertyManager.getProperty("DATALOAD.STOP_AFTER", "-1"));

						String outputFileName[] = files.split("/");
						logger.info("Output File name - " + outputFileName[outputFileName.length - 1]);

						String fileName = outputFileName[outputFileName.length - 1].replace(".txt", "").toUpperCase();
						if (fileName.contains("PRISTINEREWARDPROMOTION")
								&& !fileName.contains("PRISTINEREWARDPROMOTIONITEM")
								&& !fileName.contains("PRISTINEREWARDPROMOTIONITEMGROUP")
								&& !fileName.contains("PRISTINEREWARDPROMOTIONZONE")) {
							processingFile = "PROMOTION_FILE";
							String fieldNames[] = new String[allColumns.size()];
							int j = 0;
							for (Map.Entry<Integer, String> columns : allColumns.entrySet()) {
								fieldNames[j] = columns.getValue();
								j++;
							}
							super.parseDelimitedFile(GERewardPromotionDTO.class, fileList.get(i), '|', fieldNames,
									stopCount);
						}

						if (fileName.contains("PRISTINEREWARDPROMOTIONITEM")) {
							processingFile = "ITEM_FILE";
							String fieldNames[] = new String[allColumns1.size()];
							int j = 0;
							for (Map.Entry<Integer, String> columns : allColumns1.entrySet()) {
								fieldNames[j] = columns.getValue();
								j++;
							}
							super.parseDelimitedFile(GERewardPromotionDTO.class, fileList.get(i), '|', fieldNames,
									stopCount);
						}

						if (fileName.contains("PRISTINEREWARDPROMOTIONITEMGROUP")) {
							processingFile = "PRICE_ITEM_GROUP";
							String fieldNames[] = new String[allColumns2.size()];
							int j = 0;
							for (Map.Entry<Integer, String> columns : allColumns2.entrySet()) {
								fieldNames[j] = columns.getValue();
								j++;
							}
							super.parseDelimitedFile(GERewardPromotionDTO.class, fileList.get(i), '|', fieldNames,
									stopCount);
						}

						if (fileName.contains("PRISTINEREWARDPROMOTIONZONE")) {
							processingFile = "LOCATION_DETAIL";
							String fieldNames[] = new String[allColumns3.size()];
							int j = 0;
							for (Map.Entry<Integer, String> columns : allColumns3.entrySet()) {
								fieldNames[j] = columns.getValue();
								j++;
							}
							super.parseDelimitedFile(GERewardPromotionDTO.class, fileList.get(i), '|', fieldNames,
									stopCount);
						}
						logger.info("Processing Retail Records ...");

					}
				} catch (GeneralException | Exception ex) {
					logger.error("GeneralException", ex);
					commit = false;
					PristineDBUtil.rollbackTransaction(getOracleConnection(), "Error updating user attr");
				}
				if (processZipFile) {
					PrestoUtil.deleteFiles(fileList);
					fileList.clear();
					fileList.add(zipFileList.get(curZipFileCount));
				}
				String archivePath = getRootPath() + "/" + relativeInputPath + "/";

				if (commit) {
					PrestoUtil.moveFiles(fileList, archivePath + Constants.COMPLETED_FOLDER);
				} else {
					PrestoUtil.moveFiles(fileList, archivePath + Constants.BAD_FOLDER);
				}
				curZipFileCount++;
				processZipFile = true;
			} while (curZipFileCount < zipFileList.size());

		} catch (GeneralException ex) {
			logger.error("Outer Exception - JavaException", ex);
			ex.printStackTrace();
		} catch (Exception ex) {
			logger.error("Outer Exception - JavaException", ex);
			ex.printStackTrace();
		}
	}

	private void fillPristineRewardPromotion() {
		allColumns.put(1, "promoID");
		allColumns.put(2, "promoCode");
		allColumns.put(3, "BNR_CD");
		allColumns.put(4, "promoDesc");
		allColumns.put(5, "geacFg");
		allColumns.put(6, "rewardDesc");
		allColumns.put(7, "scanQty");
		allColumns.put(8, "anyEachCode");
		allColumns.put(9, "dealLimit");
		allColumns.put(10, "rewardQty");
		allColumns.put(11, "promoRewardAmt");
		allColumns.put(12, "promoRewardPct");
		allColumns.put(13, "priceGroupCode");
		allColumns.put(14, "mktMsgCd");
		allColumns.put(15, "dealRewardId");
		allColumns.put(16, "startDate");
		allColumns.put(17, "endDate");
	}

	private void fillPristineRewardPromotionItem() {
		allColumns1.put(1, "promoID");
		allColumns1.put(2, "itemTypeCode");
		allColumns1.put(3, "itemGroup");
		allColumns1.put(4, "itemScanQty");
		allColumns1.put(5, "familyPromoFlag");
		allColumns1.put(6, "retailerItemcode");
		allColumns1.put(7, "splrNo");
	}

	private void fillPristineRewardPromotionItemGroup() {
		allColumns2.put(1, "promoID");
		allColumns2.put(2, "pricingItemTypeCode");
		allColumns2.put(3, "itemGroup2");
		allColumns2.put(4, "pricingRewardAmt");
		allColumns2.put(5, "pricingRewardPct");
		allColumns2.put(6, "pricingRewardItemFreeFlag");
	}

	private void fillPristineRewardPromotionZoneStore() {
		allColumns3.put(1, "promoID");
		allColumns3.put(2, "zoneNo");
		allColumns3.put(3, "storeNo");
		allColumns3.put(4, "overrideNo");
	}

	/**
	 * Sets input week's calendar id and its previous week's calendar id
	 * 
	 * @param weekStartDate
	 *            Input Date
	 * @throws GeneralException
	 */
	private void populateCalendarId(String weekStartDate) throws GeneralException {
		RetailCalendarDAO retailCalendarDAO = new RetailCalendarDAO();
		RetailCalendarDTO calendarDTO = retailCalendarDAO.getCalendarId(conn, weekStartDate, Constants.CALENDAR_DAY);
		logger.info("Calendar Id - " + calendarDTO.getCalendarId());
		dayCalendarId = calendarDTO.getCalendarId();
		startDate = calendarDTO.getStartDate();
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

		String maxWeekToProcess = formatter
				.format(LocalDate.parse(DateUtil.dateToString(processingWeek, Constants.APP_DATE_FORMAT), formatter)
						.plus(NO_OF_FUTURE_WEEKS_TO_PROCESS, ChronoUnit.WEEKS).plus(6, ChronoUnit.DAYS));

		long promoDuration = ChronoUnit.DAYS.between(LocalDate.parse(maxWeekToProcess, formatter), LocalDate
				.parse(DateUtil.dateToString(promotion.getPromoEndDate(), Constants.APP_DATE_FORMAT), formatter));

		Date promoStartDate = null;
		Date promoEndDate = null;
		Date actualPromoStartDate = null;
		Date actualPromoEndDate = null;

		if (promoDuration > 0) {
			promoStartDate = getFirstDateOfWeek(promotion.getPromoStartDate());
			promoEndDate = getLastDateOfWeek(DateUtil.toDate(maxWeekToProcess));
			actualPromoStartDate = getFirstDateOfWeek(promotion.getPromoStartDate());
			actualPromoEndDate = getLastDateOfWeek(DateUtil.toDate(maxWeekToProcess));
		} else {
			promoStartDate = getFirstDateOfWeek(promotion.getPromoStartDate());
			actualPromoStartDate = getFirstDateOfWeek(promotion.getPromoStartDate());
			promoEndDate = getLastDateOfWeek(promotion.getPromoEndDate());
			actualPromoEndDate = getLastDateOfWeek(promotion.getPromoEndDate());
		}

		// Set Actaul Promo End date. (With Proper Week Start and End date)
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
					setActualCalendarInfoForLongTermPromo(promoNew, actualPromoStartDate, actualPromoEndDate);
					promoList.add(promoNew);
				}
			}
		} else {
			if (promotion.getPromoStartDate().compareTo(processingWeek) >= 0) {
				setCalendarId(promotion);
				setActualCalendarInfoForLongTermPromo(promotion, actualPromoStartDate, actualPromoEndDate);
				promoList.add(promotion);
			}
		}

		return promoList;
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
			int startCalId = ((RetailCalendarDTO) calDAO.getCalendarId(conn, inputDate, Constants.CALENDAR_DAY))
					.getCalendarId();
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
	 * Sets calendar id for actual start date and end date
	 * 
	 * @param promotion
	 * @throws GeneralException
	 */
	private void setActualCalendarInfoForLongTermPromo(PromoDefinition promotion, Date startDate, Date endDate)
			throws GeneralException {
		String promoStartDate = DateUtil.dateToString(startDate, "MM/dd/yyyy");
		String promoEndDate = DateUtil.dateToString(endDate, "MM/dd/yyyy");

		int startCalId = getCalendarId(promoStartDate);
		int endCalId = getCalendarId(promoEndDate);
		for (PromoBuyItems promoBuyItems : promotion.getBuyItems()) {
			promoBuyItems.setActualStartCalId(startCalId);
			promoBuyItems.setActualEndCalId(endCalId);
		}
	}
}
