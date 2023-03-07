package com.pristine.dataload;

import java.sql.Connection;
import java.text.DateFormat;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Calendar;
import java.util.HashMap;
import java.util.List;
import java.util.TreeMap;
import java.util.stream.Collectors;
import org.apache.log4j.Logger;
import org.apache.log4j.PropertyConfigurator;

import com.pristine.dao.DBManager;
import com.pristine.dao.ItemListDAO;
import com.pristine.dao.RetailCalendarDAO;
import com.pristine.dao.RetailPriceZoneDAO;
import com.pristine.dao.offermgmt.PricingEngineDAO;
import com.pristine.dataload.tops.PriceDataLoad;
import com.pristine.dto.CompetitiveDataDTO;
//import com.pristine.dto.ItemDTO;
import com.pristine.dto.ItemListSetupInputDTO;
import com.pristine.dto.PriceZoneDTO;
import com.pristine.dto.RetailCalendarDTO;
import com.pristine.dto.RetailPriceDTO;

import com.pristine.dto.offermgmt.MultiplePrice;
import com.pristine.dto.offermgmt.PRItemDTO;
import com.pristine.exception.GeneralException;
import com.pristine.parsinginterface.ExcelFileParser;
import com.pristine.service.offermgmt.MostOccurrenceData;
import com.pristine.util.Constants;
import com.pristine.util.PristineDBUtil;
import com.pristine.util.PropertyManager;
import com.pristine.util.offermgmt.PRCommonUtil;



/**
 * 
 * @author Kirthi
 *
 */

public class ItemListLoader {

	public static Logger logger = Logger.getLogger("ItemListLoader");

	Connection conn = null;

	public static final String MASTER_LIST_ID = "MASTER_LIST_ID=";
	public static final String PRICE_START_RANGE = "PRICE_START_RANGE=";
	public static final String PRICE_END_RANGE = "PRICE_END_RANGE=";
	private static final String TARGET_LIST_ID = "TARGET_LIST_ID=";
	private static final String PRICE_ZONE_ID = "PRICE_ZONE_ID=";
	private static final String COMP_STR_ID = "COMP_STR_ID=";
	private static final String USE_EXCEL_FILE = "USE_EXCEL_FILE=";
	private static final String INPUT_FOLDER_AND_FILE = "INPUT_FOLDER_AND_FILE=";

	public int masterListId;
	public float startRange;
	public float endRange;
	public int targetListId;
	public int priceZoneId;
	public int compStoreId;
	public String itemType;
	public boolean useExcelFile;
	public String inputFolderAndFile;
	ItemListDAO itemListDAO = new ItemListDAO();
	public ItemListLoader() {
		try {
			conn = DBManager.getConnection();
		} catch (GeneralException e) {
			logger.error("Error while getting connection!");
		}
	}

	public static void main(String[] args) {
		PropertyManager.initialize("analysis.properties");
		PropertyConfigurator.configure("log4j-item-list_setup.properties");

		ItemListLoader itemListLoader = new ItemListLoader();
		for (String arg : args) {
			if (arg.startsWith(MASTER_LIST_ID)) {
				itemListLoader.masterListId = Integer.parseInt(arg.substring(MASTER_LIST_ID.length()));
			}
			if (arg.startsWith(PRICE_START_RANGE)) {
				itemListLoader.startRange = Float.parseFloat(arg.substring(PRICE_START_RANGE.length()));
			}
			if (arg.startsWith(PRICE_END_RANGE)) {
				itemListLoader.endRange = Float.parseFloat(arg.substring(PRICE_END_RANGE.length()));
			}
			if (arg.startsWith(TARGET_LIST_ID)) {
				itemListLoader.targetListId = Integer.parseInt(arg.substring(TARGET_LIST_ID.length()));
			}
			if (arg.startsWith(PRICE_ZONE_ID)) {
				itemListLoader.priceZoneId = Integer.parseInt(arg.substring(PRICE_ZONE_ID.length()));
			}
			if (arg.startsWith(COMP_STR_ID)) {
				itemListLoader.compStoreId = Integer.parseInt(arg.substring(COMP_STR_ID.length()));
			}
			if (arg.startsWith(USE_EXCEL_FILE)) {
				itemListLoader.useExcelFile = Boolean.parseBoolean(arg.substring(USE_EXCEL_FILE.length()));
			}
			if (arg.startsWith(INPUT_FOLDER_AND_FILE)) {
				itemListLoader.inputFolderAndFile = arg.substring(INPUT_FOLDER_AND_FILE.length()); // Test/file1.xlsx
			} 
		}

		logger.info("***************************************************");
		itemListLoader.setupItemLists();
		logger.info("***************************************************");
	}

	
	
	private void setupItemLists() {
		try {
			if(useExcelFile) {
				readExcelFileAndLoadItemList();
			}else {
				
				logger.info("setupItemLists() - Setting up list for: " + targetListId + "...");
				
				setupItemList(itemType, masterListId, targetListId, compStoreId, priceZoneId, startRange, endRange);
				
				logger.info("setupItemLists() - Setting up list for: " + targetListId + " is completed");
			}
		} catch (GeneralException | Exception ge) {
			PristineDBUtil.rollbackTransaction(conn, "Error--setupItemList()");
			logger.error("Error--setupItemList()", ge);
		} finally {
			PristineDBUtil.close(conn);
		}
	}
	
	
	/**
	 * 
	 * @throws ParseException
	 * @throws GeneralException
	 */
	private void readExcelFileAndLoadItemList() throws ParseException, GeneralException {
		ExcelFileParser<ItemListSetupInputDTO> parser = new ExcelFileParser<ItemListSetupInputDTO>();
		parser.setFirstRowToProcess(1);                //ExcelFileParser line 351

		String rootPath = PropertyManager.getProperty("DATALOAD.ROOTPATH", "");

		String file = rootPath + "/" + inputFolderAndFile;

		List<ItemListSetupInputDTO> listsToBeSetup = parser.parseExcelFileV2(ItemListSetupInputDTO.class, file,       //parseExcelFileV2 line 513
				"Sheet1", mapFieldNames());

		logger.info("readExcelFileAndLoadItemList() - Total # of lists to be setup: " + listsToBeSetup.size());

		for (ItemListSetupInputDTO itemListInput : listsToBeSetup) {

			logger.info("readExcelFileAndLoadItemList() - Setting up list for: " + itemListInput.toString() + "...");

			setupItemList(itemListInput.getItemType(), itemListInput.getMasterListId(), itemListInput.getTargetListId(),
					itemListInput.getCompStoreId(), itemListInput.getPriceZoneId(), itemListInput.getStartRange(),
					itemListInput.getEndRange());

			logger.info("readExcelFileAndLoadItemList() - Setting up list is completed.");
		}
	}
	
	
	/**
	 * 
	 * @return
	 * @throws ParseException
	 */
	private TreeMap<Integer, String> mapFieldNames() throws ParseException {
		TreeMap<Integer, String> fieldNames = new TreeMap<Integer, String>();
		fieldNames.put(0, "masterListId");
		fieldNames.put(1, "targetListId");
		fieldNames.put(2, "priceZoneId");
		fieldNames.put(3, "compStoreId");
		fieldNames.put(4, "itemType");
		fieldNames.put(5, "startRange");
		fieldNames.put(6, "endRange");
		return fieldNames;
	}
	
	
	/**
	 * 
	 * @param itemType
	 * @param masterListId
	 * @param targetListId
	 * @param compStoreId
	 * @param priceZoneId
	 * @param startRange
	 * @param endRange
	 * @throws GeneralException
	 */
	private void setupItemList(String itemType, int masterListId, int targetListId, int compStoreId, int priceZoneId,
			double startRange, double endRange) throws GeneralException {

		
			logger.info("setupItemList() - Getting base items from master list: " + masterListId + " and type: " + itemType);
			// 1. Get base item list
			List<PRItemDTO> baseItemList = getBaseItemList(itemType, masterListId, compStoreId, priceZoneId);

			logger.info("setupItemList() - # of items retrieved from master list: " + baseItemList.size());
			
			// 2. Process base item list and filter target items
			List<PRItemDTO> targetItemList = getTargetListItemsUsingPriceRange(baseItemList, startRange, endRange);

			logger.info("setupItemList() - # of items falling in given price range ["+ startRange + " to " + endRange + "]: " + targetItemList.size());
			
			
			logger.info("setupItemList() - deleting items from list " +  targetListId + "...");
			
			// 3. Delete items from target list
			itemListDAO.deleteItemsFromTargetItemList(conn, targetListId);

			logger.info("setupItemList() - deleting items from list " +  targetListId + " is completed");
			
			
			logger.info("setupItemList() - Saving items to list " +  targetListId + "...");
			
			// 4. Save target items
			itemListDAO.saveItemsIntoTargetItemList(conn, targetItemList, targetListId);

			logger.info("setupItemList() - Saving items to list " +  targetListId + " is completed");
			
			PristineDBUtil.commitTransaction(conn, "Setup item list");
	}

	/**
	 * 
	 * @return base item list with price and comp price
	 * @throws GeneralException
	 */
	private List<PRItemDTO> getBaseItemList(String itemType, int masterListId, int compStrId, int priceZoneId)
			throws GeneralException {

		// 1. Get list of target items from master list
		List<PRItemDTO> masterItems = itemListDAO.getMasterItemList(conn, itemType, masterListId);

		HashMap<Integer, List<PRItemDTO>> retLirMap = (HashMap<Integer, List<PRItemDTO>>) masterItems.stream()
				.filter(p -> p.getRetLirId() > 0).collect(Collectors.groupingBy(PRItemDTO::getRetLirId));

		
		logger.info("getBaseItemList() - Populating comp price for " + masterItems.size() + " items");
		
		populateCompPriceData(masterItems, compStrId);

		logger.info("getBaseItemList() - Populating comp price for " + masterItems.size() + " items is completed");
		
		
		logger.info("getBaseItemList() - Populating current price for " + masterItems.size() + " items");
		
		populateCurrentPriceData(masterItems, priceZoneId);

		logger.info("getBaseItemList() - Populating current price for " + masterItems.size() + " items is completed");
		
		updateLigLevelPrice(retLirMap);

		return masterItems;
	}

	/**
	 * 
	 * @param masterItems
	 * @throws GeneralException
	 */
	private void populateCompPriceData(List<PRItemDTO> masterItems, int compStrId) throws GeneralException {

		int compHistory = Integer.parseInt(PropertyManager.getProperty("PR_COMP_HISTORY"));    //line311

		PricingEngineDAO pricingEngineDAO = new PricingEngineDAO();

		List<Integer> itemCodes = masterItems.stream().map(PRItemDTO::getItemCode).collect(Collectors.toList());

		HashMap<Integer, CompetitiveDataDTO> compDataMap = pricingEngineDAO.getCompPriceDataForItems(conn, compStrId,
				getWeekStartDate(), compHistory * 7, 1, itemCodes);

		masterItems.forEach(itemDTO -> {
			if (compDataMap.containsKey(itemDTO.getItemCode())) {
				CompetitiveDataDTO competitiveDataDTO = compDataMap.get(itemDTO.getItemCode());
				MultiplePrice compPrice = PRCommonUtil.getMultiplePrice(competitiveDataDTO.getRegMPack(),
						(double) competitiveDataDTO.getRegPrice(), (double) competitiveDataDTO.getRegMPrice());
				itemDTO.setCompPrice(compPrice);
			}
		});
	}

	/**
	 * 
	 * @param masterItems
	 * @throws GeneralException
	 */
	private void populateCurrentPriceData(List<PRItemDTO> masterItems, int priceZoneId) throws GeneralException {
		PriceDataLoad priceDataLoad = new PriceDataLoad();             
		RetailCalendarDAO retailCalendarDAO = new RetailCalendarDAO();
		RetailPriceZoneDAO retailPriceZoneDAO = new RetailPriceZoneDAO();
		PriceZoneDTO priceZoneDTO = retailPriceZoneDAO.getPriceZoneInfo(conn, priceZoneId);

		List<String> itemCodes = new ArrayList<String>();
		masterItems.forEach(itemDTO -> {
			itemCodes.add(String.valueOf(itemDTO.getItemCode()));
		});

		RetailCalendarDTO retailCalendarDTO = retailCalendarDAO.getCalendarId(conn, getWeekStartDate(),
				Constants.CALENDAR_DAY);

		HashMap<String, RetailPriceDTO> priceDataMap = priceDataLoad.getRetailPrice(conn,
				priceZoneDTO.getPriceZoneNum(), itemCodes, retailCalendarDTO.getCalendarId(), null,
				Constants.ZONE_LEVEL_TYPE_ID);                  //line176

		masterItems.forEach(itemDTO -> {
			if (priceDataMap.containsKey(String.valueOf(itemDTO.getItemCode()))) {
				RetailPriceDTO retailPriceDTO = priceDataMap.get(String.valueOf(itemDTO.getItemCode()));
				MultiplePrice currentPrice = PRCommonUtil.getMultiplePrice(retailPriceDTO.getRegQty(),
						(double) retailPriceDTO.getRegPrice(), (double) retailPriceDTO.getRegMPrice());
				itemDTO.setRecommendedRegPrice(currentPrice);
			}
		});
	}

	/**
	 * 
	 * @return current week start date
	 */
	private String getWeekStartDate() {
		int dayIndex = 0;
		String dateStr = null;
		DateFormat dateFormat = new SimpleDateFormat(Constants.APP_DATE_FORMAT);
		Calendar c = Calendar.getInstance();
		if (PropertyManager.getProperty("CUSTOM_WEEK_START_INDEX") != null) {
			dayIndex = Integer.parseInt(PropertyManager.getProperty("CUSTOM_WEEK_START_INDEX"));
		}
		if (dayIndex > 0)
			c.set(Calendar.DAY_OF_WEEK, dayIndex + 1);
		else
			c.set(Calendar.DAY_OF_WEEK, Calendar.SUNDAY);    //line 353

		dateStr = dateFormat.format(c.getTime());

		logger.debug("Start date:" + dateStr);
		
		return dateStr;
	}

	/**
	 * @param masterList
	 * @param retLirMap
	 */
	private void updateLigLevelPrice(HashMap<Integer, List<PRItemDTO>> retLirMap) {
		MostOccurrenceData mostOccurrenceData = new MostOccurrenceData();
		retLirMap.forEach((lirId, itemList) -> {
			MultiplePrice regPrice = (MultiplePrice) mostOccurrenceData.getMaxOccurance(itemList, "RecRegPrice");
			MultiplePrice compPrice = (MultiplePrice) mostOccurrenceData.getMaxOccurance(itemList, "CurCompRegPrice");
			itemList.forEach(itemDTO -> {
				itemDTO.setRecommendedRegPrice(regPrice);
				itemDTO.setCompPrice(compPrice);
			});
		});
	}

	/**
	 * @param baseItemList
	 * @return filters items by price range using comp price and current price
	 */
	private List<PRItemDTO> getTargetListItemsUsingPriceRange(List<PRItemDTO> baseItemList, double startRange,
			double endRange) {
		List<PRItemDTO> targetPrices = new ArrayList<>();

		baseItemList.forEach(itemDTO -> {
			if (itemDTO.getCompPrice() != null) {
				double compPrice = PRCommonUtil.getUnitPrice(itemDTO.getCompPrice(), true);
				
				logger.debug("Itemcode: " + itemDTO.getItemCode() + ", compPrice: " + compPrice + ", MultiPrice: "
						+ itemDTO.getCompPrice());
				
				if (compPrice >= startRange && compPrice <= endRange) {
					targetPrices.add(itemDTO);
				}
			} else {
				double currentPrice = PRCommonUtil.getUnitPrice(itemDTO.getRecommendedRegPrice(), true);
				
				logger.debug("Itemcode: " + itemDTO.getItemCode() + ", currentPrice: " + currentPrice + ", MultiPrice: "
						+ itemDTO.getRecommendedRegPrice());
				if (currentPrice >= startRange && currentPrice <= endRange) {
					targetPrices.add(itemDTO);
				}

			}
		});

		return targetPrices;
	}
}
