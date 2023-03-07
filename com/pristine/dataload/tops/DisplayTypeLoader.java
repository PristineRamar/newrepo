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
import com.pristine.dao.offermgmt.promotion.DisplayTypeDAO;
import com.pristine.dao.offermgmt.promotion.PromotionDAO;
import com.pristine.dto.ItemDTO;
import com.pristine.dto.RetailCalendarDTO;
import com.pristine.dto.offermgmt.promotion.PromoDefinition;
import com.pristine.dto.offermgmt.promotion.PromoDisplay;
import com.pristine.dto.offermgmt.weeklyad.RawWeeklyAd;
import com.pristine.exception.GeneralException;
import com.pristine.parsinginterface.ExcelFileParser;
import com.pristine.util.Constants;
import com.pristine.util.PrestoUtil;
import com.pristine.util.PristineDBUtil;
import com.pristine.util.PropertyManager;

public class DisplayTypeLoader{
	private static Logger  logger = Logger.getLogger("DisplayTypeLoader");
	private Connection 	_Conn = null;
	private int calendarId = -1;
	private int startCalendarId = -1;
	private int endCalendarId = -1;
	private DisplayTypeDAO displayTypeDAO = null;
	private PromotionDAO promoDAO = null;
	ItemDAO itemDAO = null;	
	private static final String sheetName_fastwall = "Fastwall";
	private static final String sheetName_end = "End";
	private static final String sheetName_dsd = "DSD";
	private HashMap<String, List<String>> itemCodeMap = null;
	private String weekStartDate = null;
	private String weekEndDate = null;
	private int locationId = -1;
	private String rootPath;
	private boolean isTesting = Boolean.parseBoolean(PropertyManager.getProperty("IS_DEBUG_MODE", "FALSE"));
	/**
	 * Constructor
	 */
	public DisplayTypeLoader ()
	{
        try
		{
        	_Conn = DBManager.getConnection();   
	    }catch (GeneralException gex) {
	    	logger.error("Error when creating connection. Unable to proceed." + gex);
	        System.exit(0);
	    }
        
        displayTypeDAO = new DisplayTypeDAO(_Conn);
        promoDAO = new PromotionDAO(_Conn);
        itemDAO = new ItemDAO();
        try{
	        RetailPriceDAO rpDAO = new RetailPriceDAO();
	    	setLocationId(Integer.parseInt(rpDAO.getChainId(_Conn)));
        }
        catch(GeneralException ge){
        	logger.error("Error while getting chain id. Unable to proceed.");
        	System.exit(0);
        }
	}
	
	/**
	 * Main method
	 * @param args[0] inputFilePath
	 * @throws Exception
	 */
	public static void main(String[] args) throws Exception 
	{
		PropertyConfigurator.configure("log4j-display-type-loader.properties");
		PropertyManager.initialize("analysis.properties");
        
		String relativePath = null;
		if(args.length > 0){
			relativePath = args[0];
		}else{
			logger.error("Invalid input - Input File Path is mandatory");
		}
		
		DisplayTypeLoader displayLoader = new DisplayTypeLoader();
		displayLoader.updateDisplayType(relativePath);
	}
	
	public void updateDisplayType(String relativePath) {
		HashMap<String, Integer> displayTypeMap = displayTypeDAO.getDisplayTypes();
		try {
			setRootPath();
			String inputFilePath = rootPath + "/" + relativePath;// + "/";
			String tempFolderPath = rootPath + "/" + relativePath + "/temp";
			
			if (isTesting) {
				itemCodeMap = new HashMap<>();
			} else {
			itemCodeMap = itemDAO.getItemCodesForRetailerItemcode(_Conn);
			}
			logger.info("Distinct items size - " + itemCodeMap.size());

			// create temp folder if not exists
			PrestoUtil.createDirIfNotExists(tempFolderPath);

			// Get all the zip files from the input folder
			ArrayList<String> zipFileList = PrestoUtil.getAllFilesInADirectory(inputFilePath, "zip");

			ExcelFileParser<RawWeeklyAd> parser = new ExcelFileParser<RawWeeklyAd>();
			// Loop the zip files
			for (String zipFile : zipFileList) {
				try {
					logger.info("Processing of file: " + zipFile + " is Started...");
					parser = new ExcelFileParser<RawWeeklyAd>();
					List<RawWeeklyAd> rawList = null;
					TreeMap<Integer, String> fieldNames = null;
					boolean isDisplayFilePresent = false;
					String fileName = "";
					
					File tempFile = new File(zipFile);
					String unZipFileFolder = tempFolderPath + "/" + FilenameUtils.getBaseName(tempFile.getName());
					
					// unzip the file in a temp folder
					PrestoUtil.unzipIncludingSubDirectories(zipFile, tempFolderPath);
					
					ArrayList<String> fileList = parser
							.getAllExcelVariantFiles(relativePath + "/temp/" + FilenameUtils.getBaseName(tempFile.getName()));
					
					for (String fn : fileList) {
						if (fn.toLowerCase().contains("gdp.xls") || fn.toLowerCase().contains("gdp.xlsx")) {
							isDisplayFilePresent = true;
							fileName = fn;
							break;
						}
					}

					// Display file is must
					if (isDisplayFilePresent) {
						setCalendarId(fileName);
						
						fieldNames = new TreeMap<Integer, String>();
//						fieldNames.put(0, "itemDesc");
//						fieldNames.put(2, "adRetail");
//						fieldNames.put(9, "itemCode");
						mapFieldNames(fieldNames, sheetName_fastwall);
						parser = new ExcelFileParser<RawWeeklyAd>();
						rawList = parser.parseExcelFileV2(RawWeeklyAd.class, fileName, sheetName_fastwall, fieldNames);
						updateDisplayTypeByDecode(rawList, displayTypeMap, displayTypeMap.get(Constants.DISPLAY_TYPE_FASTWALL));

						// Parse and update end displays.
						fieldNames = new TreeMap<Integer, String>();
//						fieldNames.put(0, "itemDesc");
//						fieldNames.put(2, "adRetail");
//						fieldNames.put(7, "displayType");
//						fieldNames.put(9, "itemCode");
						mapFieldNames(fieldNames, sheetName_end);
						rawList = parser.parseExcelFileV2(RawWeeklyAd.class, fileName, sheetName_end, fieldNames);
						updateDisplayTypeByDecode(rawList, displayTypeMap, 0);

						// Parse and update DSD displays
						rawList = parser.parseExcelFileV2(RawWeeklyAd.class, fileName, sheetName_dsd, fieldNames);
						updateDisplayTypeByDecode(rawList, displayTypeMap, 0);

						PrestoUtil.moveFile(zipFile, inputFilePath + "/" + Constants.COMPLETED_FOLDER);
						// Delete the files from temp folder
						FileUtils.deleteDirectory(new File(unZipFileFolder));
						PristineDBUtil.commitTransaction(_Conn, "Display Loader");
					} else {
						// Delete the files from temp folder
						FileUtils.deleteDirectory(new File(unZipFileFolder));
						PrestoUtil.moveFile(zipFile, inputFilePath + "/" + Constants.COMPLETED_FOLDER);
						logger.error("File " + zipFile + " is not processed. Display File is required to process this file");
					}

					logger.info("Processing of file: " + zipFile + " is Completed...");
				} catch (GeneralException | Exception ge) {
					ge.printStackTrace();
					PristineDBUtil.rollbackTransaction(_Conn, "Weekly Ad");
					logger.error("Error while processing zip file - " + zipFile + "---" + ge);
					PrestoUtil.moveFile(zipFile, inputFilePath + "/" + Constants.BAD_FOLDER);
				}
			}
		} catch (GeneralException ge) {
			ge.printStackTrace();
			logger.error("Error in updateDisplayType() - " + ge);
		} finally {
			PristineDBUtil.close(_Conn);
		}
	}

	/*private void updateDisplayType(List<RawWeeklyAd> rawList, Integer displayTypeId) {
		Set<String> itemDescSet = new HashSet<String>(); 
		
		HashMap<String, Integer> updateMap = new HashMap<String, Integer>();
		for(RawWeeklyAd ad : rawList){
			if(ad.getAdRetail() != null){
				String itemDesc = decodeItemDesc(ad.getItemDesc());
				System.out.println(itemDesc);
				
				if(!itemDescSet.contains(itemDesc)){
					itemDescSet.add(itemDesc);
					updateMap.put(itemDesc, displayTypeId);
				}
			}
		}
		
		promoDAO.updateDisplayType(updateMap, calendarId);
	}
*/
	private void updateDisplayTypeByDecode(List<RawWeeklyAd> rawList, HashMap<String, Integer> displayTypeMap, Integer displayTypeId) throws GeneralException {
		DisplayTypeDAO displayDAO = new DisplayTypeDAO(_Conn);
		List<String> ignoredItems = new ArrayList<String>();
		List<PromoDisplay> displayList = new ArrayList<PromoDisplay>();
		for(RawWeeklyAd ad : rawList){
			ArrayList<PromoDefinition> promotionsToUpdate = new ArrayList<PromoDefinition>();
			//Ignore if there is no item code
			if(ad.getItemCode() == null || ad.getItemCode().equals(Constants.EMPTY))
				continue;
			
			String retailerItemCode = "";   //ad.getItemCode().replaceAll("[^-0-9]", "");
			
			if(ad.getItemCode().contains("-")){
				String[] itemCodeArray = ad.getItemCode().split("-");
				//Get second part of code as retailer item code if the code is separated by "-".
				if(itemCodeArray.length > 1)
					retailerItemCode = itemCodeArray[1]; 
			}
			else 
				retailerItemCode = ad.getItemCode(); 
			//Replace if there are any other text is padded with item code other than item code.
			retailerItemCode = retailerItemCode.replaceAll("[^\\d.]", "");
			
			//Get list of items to be checked with promotion definition.
			Set<String> itemsToBeChecked = new HashSet<>();
			List<String> items = getItemsToBeChecked(retailerItemCode, ignoredItems);
			itemsToBeChecked.addAll(items);
			if(itemsToBeChecked != null && itemsToBeChecked.size() > 0){
					 promotionsToUpdate = (ArrayList<PromoDefinition>) 
							 promoDAO.getPromotionsByItems(_Conn, 
									 itemsToBeChecked, getStartCalendarId(), getEndCalendarId(),0,0, 0);
				if(promotionsToUpdate.size() > 0){
					for(PromoDefinition promotion: promotionsToUpdate){
						//Check if the display type is already identified from the sheet or display type from the file is not null. 
						if(displayTypeId == 0)
							displayTypeId = decodeDisplayType(displayTypeMap, ad.getDisplayType());// Identify the display type id.
						if(displayTypeId != null){
							if(displayTypeId > 0){
								PromoDisplay promoDisplay = new PromoDisplay();
								promoDisplay.setDisplayTypeId(displayTypeId);
								promoDisplay.setSubDisplayTypeId(displayTypeId);
								promoDisplay.setLocationId(getLocationId());
								promoDisplay.setLocationLevelId(Constants.CHAIN_LEVEL_ID);
								promoDisplay.setPromoDefId(promotion.getPromoDefnId());
								promoDisplay.setCalendarId(getCalendarId());
								displayList.add(promoDisplay);
							}
						}
					}
				}
			}
		}
		//Update display for each promotion
		try{
			//remove any duplicates added.
			List<PromoDisplay> finalList = new ArrayList<PromoDisplay>();
			Set<String> uniqueList = new HashSet<String>(); 
			for(PromoDisplay display: displayList){
				String uniqueKey = display.getPromoDefId() + "-" + display.getCalendarId();
				if(!uniqueList.contains(uniqueKey)){
					uniqueList.add(uniqueKey);
					finalList.add(display);
				}
			}
			
			displayDAO.insertOrUpdatePromoDispay(_Conn, finalList);
		}
		catch(GeneralException ge){
			logger.error("Error while updating displays - " + ge.toString());
		}
	}
	
/*	private String decodeItemDesc(String itemDesc){
		String newItemDesc = null;
		if(itemDesc.indexOf("CTC") != -1){
			newItemDesc = itemDesc.substring(0, itemDesc.indexOf("CTC"));
		}else if(itemDesc.indexOf("C2C") != -1){
			newItemDesc = itemDesc.substring(0, itemDesc.indexOf("C2C"));
		}else if(itemDesc.indexOf("TPR") != -1){
			newItemDesc = itemDesc.substring(0, itemDesc.indexOf("TPR"));
		}else{
			newItemDesc = itemDesc;
		}
		
		return newItemDesc.trim();
	}
*/	
	private Integer decodeDisplayType(HashMap<String, Integer> displayTypeMap, String displayType) {
		displayType = displayType.toLowerCase();
		
		if(displayType == null)
			return null;
		
		if(displayType.equals("fw") || displayType.contains("fast")){
			return displayTypeMap.get(Constants.DISPLAY_TYPE_FASTWALL);
		}else if(displayType.contains("jump")){
			return displayTypeMap.get(Constants.DISPLAY_TYPE_JUMPSHELF);
		}else if(displayType.contains("bread")){
			return displayTypeMap.get(Constants.DISPLAY_TYPE_BREADTABLE);
		}else if(displayType.contains("shipper")){
			return displayTypeMap.get(Constants.DISPLAY_TYPE_SHIPPER);
		}else if(displayType.contains("combo end")){
			return displayTypeMap.get(Constants.DISPLAY_TYPE_COMBOEND);
		}else if(displayType.contains("wing")){
			return displayTypeMap.get(Constants.DISPLAY_TYPE_WING);
		}else if(displayType.contains("mod")){
			return displayTypeMap.get(Constants.DISPLAY_TYPE_MOD);
		}else if(displayType.contains("lobby")){
			return displayTypeMap.get(Constants.DISPLAY_TYPE_LOBBY);
		}else if(displayType.contains("enddsd") || displayType.equals("dsd end")){
			return displayTypeMap.get(Constants.DISPLAY_TYPE_END_DSD);
		}else if(displayType.equals("dsd wing")){
			return displayTypeMap.get(Constants.DISPLAY_TYPE_WING_DSD);
		}else if(displayType.contains("end")){
			return displayTypeMap.get(Constants.DISPLAY_TYPE_END);
		}else{
			return null;
		}
	}
	
	private void setCalendarId(String fileName) throws Exception, GeneralException {
		String[] fileNameArr = null;
		String strDate = "";
		if (fileName.contains("/")) {
			fileNameArr = fileName.split("/");
		}
		if (fileNameArr != null && fileNameArr.length > 0) {
			String actualFilename = fileNameArr[fileNameArr.length - 1];
			File tempFile =new File(actualFilename);
			
			actualFilename = FilenameUtils.getBaseName(tempFile.getName());
			actualFilename = actualFilename.toLowerCase().replace("gdp", "").trim();
			
			String month = actualFilename.substring(actualFilename.length() - 6, (actualFilename.length() - 6) + 2);
			String day = actualFilename.substring(actualFilename.length() - 4, (actualFilename.length() - 4) + 2);
			String year = actualFilename.substring(actualFilename.length() - 2, actualFilename.length());
			
//			if (fileNameArr[fileNameArr.length - 1].toLowerCase().contains(" ")) {
//				// actualFilename = fileNameArr[fileNameArr.length -
//				// 1].toLowerCase().replace("final ", "");
//				actualFilename = fileNameArr[fileNameArr.length - 1].toLowerCase().split(" ")[1];
//			}
//			String month = actualFilename.substring(0, 2);
//			String day = actualFilename.substring(2, 4);
//			String year = actualFilename.substring(4, 6);
			
			strDate = month + "/" + day + "/" + year;
		} else {
			strDate = fileName.substring(0, 6);
		}

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

	}
	
	public int getCalendarId() {
		return calendarId;
	}

	public void setCalendarId(int calendarId) {
		this.calendarId = calendarId;
	}
	
	private List<String> getItemsToBeChecked(String retailerItemCode, List<String> ignoredItems){
		List<String> itemsToBeChecked = new ArrayList<String>();
		ItemDTO itemIN = new ItemDTO();
		itemIN.setUpc(retailerItemCode);
		itemIN.setRetailerItemCode(retailerItemCode);
		//itemIN.setItemName(itemDesc.trim());
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
				for(String key: itemCodeMap.keySet()) {
					//Sometimes the retailer item code is padded with '0'. Sometimes not. To handle this,
					//Convert to whole number at both Cache and input retailer item code. if both matches get the item code using actual key.
					if(!retailerItemCode.equals("") && retailerItemCode != null && !key.equals("null") && retailerItemCode.length() < 8 && key.length() < 8){
						String tempkey = String.valueOf(Integer.parseInt(key));
						//String tempretItemCode = String.valueOf(Integer.parseInt(retailerItemCode));
						//NU:: 21st Nov 2016, retailer item code from excel is read as double, so it was
						//not getting converted properly.
						String tempretItemCode = String.valueOf((int) Double.parseDouble(retailerItemCode));
						if(tempkey.equals(tempretItemCode)){
							itemsToBeChecked = itemCodeMap.get(key); 
							break;
						}
					}
				}
			}
		}
		else{
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
	
	private void mapFieldNames(TreeMap<Integer, String> fieldNames, String sheetName) throws ParseException {
		DateFormat formatterMMddyy = new SimpleDateFormat("MM/dd/yy");
		Date processingWeekDate = formatterMMddyy.parse(getWeekStartDate());
		Date tempWeekDate1 = formatterMMddyy.parse("07/04/15");

		// if week start date <= 07/04/15
		if (processingWeekDate.before(tempWeekDate1) || processingWeekDate.equals(tempWeekDate1)) {
			if (sheetName.equals(sheetName_fastwall)) {
				fieldNames.put(0, "itemDesc");
				fieldNames.put(2, "adRetail");
				fieldNames.put(8, "itemCode");
			}  else if (sheetName.equals(sheetName_end) || sheetName.equals(sheetName_dsd)) {
				fieldNames.put(0, "itemDesc");
				fieldNames.put(2, "adRetail");
				fieldNames.put(7, "displayType");
				fieldNames.put(8, "itemCode");
			}
		} else {
			if (sheetName.equals(sheetName_fastwall)) {
				fieldNames.put(0, "itemDesc");
				fieldNames.put(2, "adRetail");
				fieldNames.put(9, "itemCode");
			}  else if (sheetName.equals(sheetName_end) || sheetName.equals(sheetName_dsd)) {
				fieldNames.put(0, "itemDesc");
				fieldNames.put(2, "adRetail");
				fieldNames.put(7, "displayType");
				fieldNames.put(9, "itemCode");
			}
		}
	}
	
	public String getWeekStartDate() {
		return weekStartDate;
	}

	public void setWeekStartDate(String weekStartDate) {
		this.weekStartDate = weekStartDate;
	}

	public int getLocationId() {
		return locationId;
	}

	public void setLocationId(int locationId) {
		this.locationId = locationId;
	}
	
	public String getWeekEndDate() {
		return weekEndDate;
	}

	public void setWeekEndDate(String weekEndDate) {
		this.weekEndDate = weekEndDate;
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
	 * Sets rootpath where input and output files are there
	 * @throws GeneralException
	 */
	private void setRootPath() throws GeneralException{
		rootPath = PropertyManager.getProperty("DATALOAD.ROOTPATH");
		if(rootPath == null){
			throw new GeneralException("Unable to proceed further without DATALOAD.ROOTPATH property in analysis.properties");
		}
	}
	
}
