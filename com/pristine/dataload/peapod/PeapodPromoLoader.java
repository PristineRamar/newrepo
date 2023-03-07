package com.pristine.dataload.peapod;

import java.sql.Connection;
import java.text.DateFormat;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.time.DayOfWeek;
import java.time.LocalDate;
import java.time.temporal.ChronoUnit;
import java.time.temporal.TemporalAdjusters;
import java.util.ArrayList;
import java.util.Calendar;
import java.util.Date;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map.Entry;
import java.util.Set;
import java.util.TreeMap;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;

import org.apache.log4j.Logger;
import org.apache.log4j.PropertyConfigurator;

import com.pristine.dao.DBManager;
import com.pristine.dao.ItemDAO;
import com.pristine.dao.RetailCalendarDAO;
import com.pristine.dao.RetailCalendarPromoDAO;
import com.pristine.dao.RetailPriceDAO;
import com.pristine.dataload.AdAndPromoSetup;
import com.pristine.dataload.tops.PriceDataLoad;
import com.pristine.dto.ItemDTO;
import com.pristine.dto.ItemDetailKey;
import com.pristine.dto.PromoDataStandardDTO;
import com.pristine.dto.RetailCalendarDTO;
import com.pristine.dto.RetailPriceDTO;
import com.pristine.dto.offermgmt.promotion.PromoBuyItems;
import com.pristine.exception.GeneralException;
import com.pristine.parsinginterface.ExcelFileParser;
import com.pristine.parsinginterface.ExcelFileParserV2;
import com.pristine.service.RetailCalendarPromoCacheService;
import com.pristine.util.Constants;
import com.pristine.util.DateUtil;
import com.pristine.util.PrestoUtil;
import com.pristine.util.PristineDBUtil;
import com.pristine.util.PropertyManager;
import com.pristine.util.offermgmt.PRCommonUtil;

@SuppressWarnings({ "unused", "rawtypes" })
public class PeapodPromoLoader {

	private static Logger logger = Logger.getLogger("PeapodPromoLoader");

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
	DateFormat appDateFormatter = new SimpleDateFormat(Constants.APP_DATE_FORMAT);
	private ExcelFileParserV2 parser = null;
	List<ItemDTO> activeItems = null;
	HashMap<String, Integer> storeIdMap;
	HashMap<String, Integer> zoneMap;
	Set<String> itemCodeNotFound = null;
	Set<String> locationNotfound = null;
	Set<String> priceNotFoundList = null;
	private static HashMap<String, String> divisionNameMap = new HashMap<String, String>();
	HashMap<String, List<ItemDTO>> retailerItemCodeAndItsItem = null;
	HashMap<String, List<ItemDTO>> upcAndItsItem = null;
	HashMap<ItemDetailKey, Integer> itemDetailKeyItems = null;
	Set<String> notProcessedPromotions = null;
	HashMap<Integer, List<PromoDataStandardDTO>> errorMap = new HashMap<Integer, List<PromoDataStandardDTO>>();
	HashMap<String, Integer> calendarMap = new HashMap<String, Integer>();
	Date processingWeek = null;
	Date weekdate = null;
	PriceDataLoad objPriceLoad;
	List<RetailCalendarDTO> retailCalendarPromoCache = null;
	boolean isDeltaMode = false;

	public PeapodPromoLoader() {
		PropertyManager.initialize("analysis.properties");
		rootPath = PropertyManager.getProperty("DATALOAD.ROOTPATH", "");
		try {
			conn = DBManager.getConnection();
		} catch (GeneralException ex) {
			logger.error("Error when creating connection - " + ex);
		}

	}

	public static void main(String[] args) {

		PeapodPromoLoader promoLoader = new PeapodPromoLoader();

		PropertyConfigurator.configure("log4j-peapodpromoloader.properties");

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
					logger.info("Mode=" + arg.substring(MODE.length()));
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
			processFile();
			
			logNotProcessed();

		} catch (GeneralException e) {
			e.printStackTrace();
			logger.error("Error in Process" + e);
		}

	}

	private void logNotProcessed() {
		StringBuilder sb = new StringBuilder();
		for (String itemNotFound : priceNotFoundList) {
			sb.append(itemNotFound + "\n");
		}
		logger.info("***priceNotFoundList***" + priceNotFoundList.size());
		logger.info(sb.toString());

	}

	private void initializeData() throws GeneralException {
		try {
			itemCodeNotFound = new HashSet<>();
			locationNotfound = new HashSet<>();
			priceNotFoundList = new HashSet<>();
			parser = new ExcelFileParserV2();
			activeItems = new ArrayList<ItemDTO>();
			retailPriceDAO = new RetailPriceDAO();
			notProcessedPromotions = new HashSet<>();
			ItemDAO itemDAO = new ItemDAO();
			parser.setFirstRowToProcess(1);
			objPriceLoad = new PriceDataLoad();

			retailerItemCodeAndItsItem = new HashMap<String, List<ItemDTO>>();
			upcAndItsItem = new HashMap<String, List<ItemDTO>>();
			itemDetailKeyItems = new HashMap<ItemDetailKey, Integer>();

			logger.info("initializeData() - Getting all items started...");
			activeItems = itemDAO.getAllActiveItems(conn);
			logger.info("initializeData() -	Active items" + activeItems.size());

			logger.info("initializeData() - Getting all items complete.....");

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

			// Cache promotion calendar
			logger.debug("Caching of Retail Calendar Promo is started...");
			retailCalendarPromoCache = new RetailCalendarPromoDAO().getFullRetailCalendarPromo(conn);
			logger.debug("Caching of Retail Calendar Promo is started...");

			mapDivision();

		} catch (Exception e) {
			e.printStackTrace();
			logger.error("Exception in Initialize()", e);
		}
	}

	/**
	 * Loads promotion data
	 * 
	 * @throws GeneralException
	 */
	@SuppressWarnings("unchecked")
	private void processFile() throws GeneralException {
		logger.info("processFile() - Processing start");
		String fName = null;
		String inputPath = inputFilePath;
		ExcelFileParser<PromoDataStandardDTO> parser2 = new ExcelFileParser<PromoDataStandardDTO>();
		parser2.setFirstRowToProcess(1);
		List<PromoDataStandardDTO> recordsToProcess = new ArrayList<PromoDataStandardDTO>();
		ArrayList<String> fileList = parser.getFiles(inputPath);
		TreeMap<Integer, String> fieldNames = new TreeMap<Integer, String>();
		mapFieldNames(fieldNames);

		try {
			for (String fileName : fileList) {
				logger.info("processFile() - Processing file: " + fileName);
				fName = fileName;
				List<PromoDataStandardDTO> promoData = null;

				parser.setFirstRowToProcess(1);
				String[] fileExtArr = fileName.split("\\.");
				String fileExtension = fileExtArr[fileExtArr.length - 1];

				if (fileExtension.equalsIgnoreCase("XLS") || fileExtension.equalsIgnoreCase("XLSX")) {
					promoData = parser2.parseExcelFile(PromoDataStandardDTO.class, fileName, 0, fieldNames);
				} else if (fileExtension.equalsIgnoreCase("CSV") || fileExtension.equalsIgnoreCase("TXT")) {
					promoData = parser.parseCSVFile(PromoDataStandardDTO.class, fileName, fieldNames, ',');
				}
				logger.info("processFile() - Total # of records read: " + promoData.size());

				logger.info("processFile() - Populating item code... ");
				List<PromoDataStandardDTO> newPromoData = populateItemCode(promoData);
				logger.info("processFile() - Populating item code is completed.");

				recordsToProcess = validateandReturnRecords(newPromoData);
				if (errorMap.get(Constants.ERRORCODE_LONG_TERM_PROMO) != null) {
					logger.info("processFile() - # of records with date range above 6 months: "
							+ errorMap.get(Constants.ERRORCODE_LONG_TERM_PROMO).size());
				}

				if (errorMap.get(Constants.ERRORCODE_NEGATIVE_DISCOUNTS) != null) {
					logger.info("processFile() - # of records with negative discounts: "
							+ errorMap.get(Constants.ERRORCODE_NEGATIVE_DISCOUNTS).size());
				}
				
				logger.info("processFile() - Total #records considered for getting regPrice " + recordsToProcess.size());

				
				LocalDate minDate = recordsToProcess.stream().map(PromoDataStandardDTO::getPromoStartDateAsLocalDate)
						.min(LocalDate::compareTo).get();
				
				week = PRCommonUtil.getDateFormatter().format(minDate);
				weekdate = new SimpleDateFormat(Constants.APP_DATE_FORMAT).parse(week);
				
				logger.info("processFile() - Loading promotions starting from " + week);
				
				logger.info("processFile() - Populating regular price...");
				List<PromoDataStandardDTO> finalList = populateRegularPrice(recordsToProcess);
				logger.info("processFile() - Populating regular price is completed.");

				AdAndPromoSetup promoSetup = new AdAndPromoSetup(conn, activeItems, isDeltaMode);

				logger.info("processFile() - Total items to be sent:" + finalList.size());

				logger.info("processFile() - Setting up promotion data...");
				promoSetup.setupPromoData(finalList, week);
				logger.info("processFile() - Setting up promotion data is completed.");

				PrestoUtil.moveFile(fileName, inputPath + "/" + Constants.COMPLETED_FOLDER);
				PristineDBUtil.commitTransaction(conn, "batch record update");

			}
		} catch (Exception ex) {

			logger.error("processFile()-Exception while processing file : " + fName, ex);
			PrestoUtil.moveFile(fName, inputPath + "/" + Constants.BAD_FOLDER);
			PristineDBUtil.rollbackTransaction(conn, "Unexpected Exception");
		}

	}

	//eliminate promotions running for more than 6 months
	//Added Validations to ignore the negative values
	private List<PromoDataStandardDTO> validateandReturnRecords(List<PromoDataStandardDTO> promoData) throws GeneralException {

		List<PromoDataStandardDTO> finalRecords = new ArrayList<>();
		for (PromoDataStandardDTO promodata : promoData) {
			String startDate = DateUtil.dateToString(DateUtil.toDate(promodata.getPromoStartDate()),
					Constants.APP_DATE_FORMAT);
			String endDate = DateUtil.dateToString(DateUtil.toDate(promodata.getPromoEndDate()),
					Constants.APP_DATE_FORMAT);
			long monthsBetween = ChronoUnit.MONTHS.between(LocalDate.parse(startDate, PRCommonUtil.getDateFormatter()),
					LocalDate.parse(endDate, PRCommonUtil.getDateFormatter()));

			logger.debug("monthsBetween: " + monthsBetween);
			if (monthsBetween > 6) {
				addErrorLookup(Constants.ERRORCODE_LONG_TERM_PROMO, promodata);
			} else if (promodata.getBmsmDollaroffperunits() < 0 || promodata.getBmsmPctoffperunit() < 0
					|| promodata.getDollarOff() < 0 || promodata.getPctOff() < 0 || promodata.getBmsmsalePrice() < 0) {
				addErrorLookup(Constants.ERRORCODE_NEGATIVE_DISCOUNTS, promodata);
			} else {
				finalRecords.add(promodata);
			}

		}
		return finalRecords;
	}

	private void addErrorLookup(int errorcode, PromoDataStandardDTO promoData) {
		List<PromoDataStandardDTO> list = new ArrayList<PromoDataStandardDTO>();
		if (errorMap.containsKey(errorcode)) {
			list = errorMap.get(errorcode);
		}
		list.add(promoData);
		errorMap.put(errorcode, list);
	}

	private List<PromoDataStandardDTO> populateItemCode(List<PromoDataStandardDTO> promoData) throws Exception, GeneralException {
		List<PromoDataStandardDTO> newPromoList = new ArrayList<>();
		for (PromoDataStandardDTO item : promoData) {
			if(item.getUpc() == null || Constants.EMPTY.equals(item.getUpc())) {
				if(retailerItemCodeAndItsItem.containsKey(item.getItemCode())){
					List<ItemDTO> items = retailerItemCodeAndItsItem.get(item.getItemCode());
					for(ItemDTO itemDto: items) {
						PromoDataStandardDTO clonedObj = (PromoDataStandardDTO) item.clone();
						clonedObj.setPrestoItemCode(String.valueOf(itemDto.getItemCode()));
						newPromoList.add(clonedObj);
					}
				}
			} else if(item.getItemCode() == null || Constants.EMPTY.equals(item.getItemCode())) {
				if(upcAndItsItem.containsKey(item.getUpc())){
					List<ItemDTO> items = upcAndItsItem.get(item.getUpc());
					for(ItemDTO itemDto: items) {
						PromoDataStandardDTO clonedObj = (PromoDataStandardDTO) item.clone();
						clonedObj.setPrestoItemCode(String.valueOf(itemDto.getItemCode()));
						newPromoList.add(clonedObj);
					}
				}
			} else {
				ItemDetailKey itemKey = new ItemDetailKey(PrestoUtil.castUPC(item.getUpc(), false), item.getItemCode());
				if (itemDetailKeyItems.containsKey(itemKey)) {
					item.setPrestoItemCode(String.valueOf(itemDetailKeyItems.get(itemKey)));
					newPromoList.add(item);
				}
			}
		}
		
		return newPromoList;
	}

	private List<PromoDataStandardDTO> populateRegularPrice(List<PromoDataStandardDTO> promoData)
			throws Exception, GeneralException {

		HashMap<String, List<PromoDataStandardDTO>> promoByWeek = new HashMap<String, List<PromoDataStandardDTO>>();
		List<PromoDataStandardDTO> finalList = new ArrayList<>();
		List<PromoDataStandardDTO> promotempList = new ArrayList<>();

		logger.info("populateRegularPrice() - Weekdate is :" + weekdate);

		for (PromoDataStandardDTO pr : promoData) {
			List<PromoDataStandardDTO> weeklypromo = splitPromoByWeek(pr, weekdate);

			promotempList.addAll(weeklypromo);
		}

		for (PromoDataStandardDTO promo : promotempList) {
			List<PromoDataStandardDTO> tempList = new ArrayList<>();
			tempList.add(promo);
			if (promoByWeek.containsKey(promo.getPromoStartDate())) {
				tempList.addAll(promoByWeek.get(promo.getPromoStartDate()));
			}

			promoByWeek.put(promo.getPromoStartDate(), tempList);
		}

		String currentWeekStartDate = DateUtil.getWeekStartDate(0);
		LocalDate currentDate = LocalDate.parse(currentWeekStartDate, PRCommonUtil.getDateFormatter());
		int currentStartCalendarId = ((RetailCalendarDTO) calDAO.getCalendarId(conn, currentWeekStartDate,
				Constants.CALENDAR_DAY)).getCalendarId();
		for (Entry<String, List<PromoDataStandardDTO>> entry : promoByWeek.entrySet()) {

			PromoDataStandardDTO sampleObj = entry.getValue().get(0);
			int endCalendarId = sampleObj.getEndcalendarID();
			Set<String> itemCodes = entry.getValue().stream().map(PromoDataStandardDTO::getPrestoItemCode)
					.collect(Collectors.toSet());

			List<String> itemCodeList = new ArrayList<>(itemCodes);

			logger.info("populateRegularPrice() - Fetching price for week: " + entry.getKey() + "...");

			logger.info("populateRegularPrice() - # of items: " + itemCodeList.size());

			HashMap<String, RetailPriceDTO> currentWeekPrice = new HashMap<>();

			DateFormat df = new SimpleDateFormat(Constants.APP_DATE_FORMAT);
			Date foramtteDate = new SimpleDateFormat(Constants.APP_DATE_FORMAT).parse(entry.getKey());
			LocalDate weekStartDate = LocalDate.parse(df.format(foramtteDate), PRCommonUtil.getDateFormatter());

			boolean isFuturePromo = false;
			if (weekStartDate.isAfter(currentDate)) {
				currentWeekPrice = objPriceLoad.getRetailPrice(conn, sampleObj.getLocationNo(), itemCodeList,
						currentStartCalendarId, null, Constants.ZONE_LEVEL_TYPE_ID);
				isFuturePromo = true;
				logger.info("populateRegularPrice() - Current Price data retrieved for " + currentWeekPrice.size() + " items.");
			}

			HashMap<String, RetailPriceDTO> priceDataMap = objPriceLoad.getRetailPrice(conn, sampleObj.getLocationNo(),
					itemCodeList, endCalendarId, null, Constants.ZONE_LEVEL_TYPE_ID);

			logger.info("populateRegularPrice() - Price data retrieved for " + priceDataMap.size() + " items.");

			for (PromoDataStandardDTO promoStandardDTO : entry.getValue()) {

				boolean isPricePopulated = false;

				// Check future price or corresponding price data is present is for this item
				if (!isFuturePromo && priceDataMap.containsKey(promoStandardDTO.getPrestoItemCode())) {

					RetailPriceDTO retailPriceDTO = priceDataMap.get(promoStandardDTO.getPrestoItemCode());

					setPrice(promoStandardDTO, retailPriceDTO);

					isPricePopulated = true;

				} else if (currentWeekPrice.containsKey(promoStandardDTO.getPrestoItemCode())) {

					// Check current week price data for this item if it is future and there is no
					// future price data

					RetailPriceDTO retailPriceDTO = currentWeekPrice.get(promoStandardDTO.getPrestoItemCode());

					setPrice(promoStandardDTO, retailPriceDTO);

					isPricePopulated = true;
				} else {
					priceNotFoundList.add(promoStandardDTO.getItemCode());
				}

				// Add items only with price
				if (isPricePopulated) {
					finalList.add(promoStandardDTO);
				}
			}
		}

		return finalList;
	}

	/**
	 * 
	 * @param promoStandardDTO
	 * @param retailPriceDTO
	 */
	private void setPrice(PromoDataStandardDTO promoStandardDTO, RetailPriceDTO retailPriceDTO) {

		promoStandardDTO
				.setEverydayPrice(retailPriceDTO.getRegMPrice() > 0 ? String.valueOf(retailPriceDTO.getRegMPrice())
						: String.valueOf(retailPriceDTO.getRegPrice()));

		promoStandardDTO
				.setEverdayQty(retailPriceDTO.getRegQty() == 0 ? "1" : String.valueOf(retailPriceDTO.getRegQty()));

		// setting dollarOff 0 for peapod data
		if (promoStandardDTO.getDollarOff() != 0) {
			promoStandardDTO.setDollarOff(0);
		}
		// since pctoff is given in decimal we are converting it to percentage
		if (promoStandardDTO.getPctOff() != 0) {
			promoStandardDTO.setPctOff(promoStandardDTO.getPctOff());
		} // since bmsmpctoff is given in decimal we are converting it to percentage
		if (promoStandardDTO.getBmsmPctoffperunit() != 0) {
			promoStandardDTO.setBmsmPctoffperunit(promoStandardDTO.getBmsmPctoffperunit());
		}
		logger.debug("RegPrice found for :" + promoStandardDTO.getItemCode() + "PrestoItem"
				+ promoStandardDTO.getPrestoItemCode() + "RegPrice is " + promoStandardDTO.getEverydayPrice() + "calID"
				+ promoStandardDTO.getStartcalendarID() + ";" + promoStandardDTO.getPromoStartDate());
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
	}

	/**
	 * 
	 * @param promotion
	 * @return list of weekly promotions derived from longer duration promotions
	 * @throws Exception
	 * @throws GeneralException
	 */
	/**
	 
	 */
	public List<PromoDataStandardDTO> splitPromoByWeek(PromoDataStandardDTO promotion, Date processingWeek)
			throws Exception, GeneralException {
		logger.debug("splitPromoByWeek() - inside  ");
		List<PromoDataStandardDTO> promoList = new ArrayList<>();
		RetailCalendarPromoCacheService retailCalendarPromoCacheService = new RetailCalendarPromoCacheService(
				retailCalendarPromoCache);
		SimpleDateFormat dateFormat = new SimpleDateFormat(Constants.APP_DATE_FORMAT);
		Date prStart = dateFormat.parse(promotion.getPromoStartDate());
		Date prEnd = dateFormat.parse(promotion.getPromoEndDate());

		// TODO This is temporary. For Peapod. Week starts from thursday and ends in
		// wednesday

		Calendar startCal = Calendar.getInstance();
		startCal.setTime(prStart);

		Calendar endCal = Calendar.getInstance();
		endCal.setTime(prEnd);

		Date promoStartDate = null;
		Date promoEndDate = null;
		if (startCal.get(Calendar.DAY_OF_WEEK) == Calendar.THURSDAY
				&& endCal.get(Calendar.DAY_OF_WEEK) == Calendar.WEDNESDAY) {
			promoStartDate = DateUtil.incrementDate(prStart, 1);
			promoEndDate = DateUtil.incrementDate(prEnd, 1);
		} else {
			String formattedStartDate = dateFormat.format(prStart);
			String formattedEndDate = dateFormat.format(prEnd);

			RetailCalendarDTO startWeekCalendar = retailCalendarPromoCacheService.getWeekCalendar(formattedStartDate);
			RetailCalendarDTO endWeekCalendar = retailCalendarPromoCacheService.getWeekCalendar(formattedEndDate);

			long diff = prEnd.getTime() - prStart.getTime();
			long diffDates = TimeUnit.DAYS.convert(diff, TimeUnit.MILLISECONDS);

			if (diff > 7) {
				promoStartDate = dateFormat.parse(startWeekCalendar.getStartDate());
				promoEndDate = dateFormat.parse(endWeekCalendar.getEndDate());
			} else {
				promoStartDate = dateFormat.parse(startWeekCalendar.getStartDate());
				promoEndDate = DateUtil.incrementDate(promoStartDate, 6);
			}
		}

		promotion.setpStartDate(promoStartDate);
		promotion.setpEndDate(promoEndDate);
		promotion.setPromoStartDate(dateFormat.format(promoStartDate));
		promotion.setPromoEndDate(dateFormat.format(promoEndDate));

		long diff = promotion.getpEndDate().getTime() - promotion.getpStartDate().getTime();
		long diffDates = TimeUnit.DAYS.convert(diff, TimeUnit.MILLISECONDS);
		if (diffDates > 7) {
			float dif = (float) diffDates / 7;
			int noOfWeeks = (int) Math.ceil(dif);

			for (int i = 0; i < noOfWeeks; i++) {
				PromoDataStandardDTO promoNew = (PromoDataStandardDTO) promotion.clone();
				promoNew.setpStartDate(promoStartDate);
				promoEndDate = DateUtil.incrementDate(promoStartDate, 6);
				promoStartDate = DateUtil.incrementDate(promoStartDate, 7);
				promoNew.setpEndDate(promoEndDate);
				if (promoNew.getpStartDate().compareTo(processingWeek) >= 0) {
					promoNew.setPromoStartDate(dateFormat.format(promoNew.getpStartDate()));
					promoNew.setPromoEndDate(dateFormat.format(promoNew.getpEndDate()));
					setCalendarId(promoNew);
					promoList.add(promoNew);
				}
			}
		} else {
			if (promotion.getpStartDate().compareTo(processingWeek) >= 0) {
				promotion.setPromoStartDate(dateFormat.format(promotion.getpStartDate()));
				promotion.setPromoEndDate(dateFormat.format(promotion.getpEndDate()));
				setCalendarId(promotion);
				promoList.add(promotion);
			}
		}
		logger.debug("splitPromoByWeek size is " + promoList.size());
		return promoList;
	}

	private void setCalendarId(PromoDataStandardDTO promoDTO) throws GeneralException {
		String promoStartDate = (promoDTO.getPromoStartDate());
		String promoEndDate = (promoDTO.getPromoEndDate());

		// promoDTO.setWeekStartDate(promoStartDate);

		// Get and set start calendar id
		promoDTO.setStartcalendarID(getCalendarId(promoStartDate));
		if (promoDTO.getStartcalendarID() == 0) {
			logger.debug("Promo Start date: " + promoStartDate);
		}
		// Get and set end calendar id
		promoDTO.setEndcalendarID(getCalendarId(promoEndDate));
	}

	/**
	 * 
	 * @param inputDate
	 * @return day calendar id for given input
	 * @throws GeneralException
	 */
	private int getCalendarId(String inputDate) throws GeneralException {
		if (calendarMap.get(inputDate) == null) {
			int startCalId = ((RetailCalendarDTO) calDAO.getPromocalId(conn, inputDate, Constants.CALENDAR_DAY))
					.getCalendarId();
			logger.debug("getCalendarId returns  " + startCalId);
			calendarMap.put(inputDate, startCalId);
		}
		return calendarMap.get(inputDate);
	}

	/**
	 * Sets calendar id for actual start date and end date
	 * 
	 * @param promotion
	 * @throws GeneralException
	 */
	private boolean setActualCalendarInfoForLongTermPromo(PromoDataStandardDTO promotion, Date startDate, Date endDate)
			throws GeneralException {
		boolean isValidCalendarId = true;
		String promoStartDate = DateUtil.dateToString(startDate, "MM/dd/yyyy");
		String promoEndDate = DateUtil.dateToString(endDate, "MM/dd/yyyy");

		int startCalId = getCalendarId(promoStartDate);
		int endCalId = getCalendarId(promoEndDate);
		if (startCalId == 0 || endCalId == 0) {
			isValidCalendarId = false;
		}
		/*
		 * for (PromoBuyItems promoBuyItems : promotion.getBuyItems()) {
		 * promoBuyItems.setActualStartCalId(startCalId);
		 * promoBuyItems.setActualEndCalId(endCalId); }
		 */
		return isValidCalendarId;
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

	private void mapDivision() {
		divisionNameMap.put("NE", "NEW ENGLAND");
		divisionNameMap.put("GC", "GIANTC");
		divisionNameMap.put("GL", "GIANT");
		divisionNameMap.put("NY", "NEW YORK METRO");
		divisionNameMap.put("SSS", "NEW YORK METRO");
		divisionNameMap.put("SSN", "NEW ENGLAND");
	}

}
