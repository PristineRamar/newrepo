package com.pristine.dataload.gianteagle;

import java.io.IOException;
import java.sql.Connection;
import java.text.DateFormat;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Calendar;
import java.util.Date;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.TreeMap;

import org.apache.log4j.Logger;
import org.apache.log4j.PropertyConfigurator;

import com.pristine.dao.ItemDAO;
import com.pristine.dao.ItemLoaderDAO;
import com.pristine.dao.MarkerDataDAO;
import com.pristine.dao.RetailCalendarDAO;
import com.pristine.dto.ItemDTO;
import com.pristine.dto.MarketDataDTO;
import com.pristine.exception.GeneralException;
import com.pristine.parsinginterface.PristineFileParser;
import com.pristine.util.Constants;
import com.pristine.util.DateUtil;
import com.pristine.util.PrestoUtil;
import com.pristine.util.PristineDBUtil;
import com.pristine.util.PropertyManager;

public class MarketDataLoader extends PristineFileParser{
	private static Logger logger = Logger.getLogger("MarketDataLoader");
	private final static String INPUT_FOLDER = "INPUT_FOLDER=";
	private static String startDate = null;
	Connection conn = null;
	static TreeMap<Integer, String> allColumns = new TreeMap<Integer, String>();
	DateFormat appDateFormatter = new SimpleDateFormat(Constants.APP_DATE_FORMAT);
	 HashMap <String, String> uomMap = new HashMap <String, String> (); 
	HashMap <String, String> translateUOMMap = new HashMap <String, String> ();
	HashMap<String, Integer> itemCodeandUPCMap;
	HashMap<String, Integer> brandIDandNameMap;
	HashMap<String, MarketDataDTO> mdBasedOnUpc = new HashMap<String, MarketDataDTO>();
	int noOfRecords;
	ItemDAO itemdao = new ItemDAO ();
	public MarketDataLoader() {
		super();
		super.headerPresent = true;
		conn = getOracleConnection();
	}
	
	public static void main(String[] args) throws ParseException {
		String inFolder = null;
		PropertyManager.initialize("analysis.properties");
		PropertyConfigurator.configure("log4j-market_data_loader.properties");
		MarketDataLoader marketDataLoader = new MarketDataLoader();
		for (String arg : args) {
			if (arg.startsWith(INPUT_FOLDER)) {
				inFolder = arg.substring(INPUT_FOLDER.length());
			}
		}

		// Default week type to current week if it is not specified

		Calendar c = Calendar.getInstance();
		int dayIndex = 0;
		if (PropertyManager.getProperty("CUSTOM_WEEK_START_INDEX") != null) {
			dayIndex = Integer.parseInt(PropertyManager.getProperty("CUSTOM_WEEK_START_INDEX"));
		}
//		if (dayIndex > 0)
//			c.set(Calendar.DAY_OF_WEEK, dayIndex + 1);
//		else
		c.set(Calendar.DAY_OF_WEEK, Calendar.SUNDAY);
		DateFormat dateFormat = new SimpleDateFormat("MM/dd/yy");
		if (args.length > 1) {
			if ("CURRENT_MONTH".equalsIgnoreCase(args[1])) {
				startDate = dateFormat.format(c.getTime());
			} else if ("NEXT_MONTH".equalsIgnoreCase(args[1])) {
				c.add(Calendar.DATE, 30);
				startDate = dateFormat.format(c.getTime());
			} else if ("LAST_MONTH".equalsIgnoreCase(args[1])) {
				c.add(Calendar.DATE, -30);
				startDate = dateFormat.format(c.getTime());
			} else if ("SPECIFIC_MONTH".equalsIgnoreCase(args[1])) {
				try {
					String tempStartDate = DateUtil.getWeekStartDate(DateUtil.toDate(args[2]), 0);
					Date date = dateFormat.parse(args[2]);
					startDate = dateFormat.format(date);
					logger.info("Date:" + startDate);
				} catch (GeneralException exception) {
					logger.error("Error when parsing date - " + exception.toString());
					System.exit(-1);
				}
			}
		}
		try {
			logger.info("Given Start date: "+startDate);
			marketDataLoader.parseFile(inFolder);
		} catch (IOException | GeneralException e) {
			e.printStackTrace();
			logger.error("Error in MarketDataLoader()",e);
		}
	}
	
	private void parseFile(String inFolder) throws GeneralException, IOException {

		try {
			setupUOMTranslation();
			RetailCalendarDAO retailCalendarDAO = new RetailCalendarDAO();
			String processingMonth = retailCalendarDAO.getMonthStartDate(conn, startDate);
			logger.info("Processing month: "+processingMonth);
			ItemLoaderDAO itemLoaderDAO = new ItemLoaderDAO();
			uomMap = itemdao.getUOMList(conn,"COMP_DATALOAD");
			logger.info("No Of UOMs: "+uomMap.size());
			logger.info("Caching Item details");
//			itemCodeandUPCMap =itemdao.getItemCodeBasedOnUPC(conn); 
			itemCodeandUPCMap=	itemLoaderDAO.getTwoColumnsCache(conn, "ITEM_CODE", "UPC", "ITEM_LOOKUP",
					false);
			brandIDandNameMap = itemLoaderDAO.getTwoColumnsCache(conn, "BRAND_ID", "BRAND_NAME", "BRAND_LOOKUP",
					true);
			fillAllColumns1();
			String fieldNames[] = new String[allColumns.size()];
			int k = 0;
			for (Map.Entry<Integer, String> columns : allColumns.entrySet()) {
				fieldNames[k] = columns.getValue();
				k++;
			}

			// getzip files
			ArrayList<String> zipFileList = getZipFiles(inFolder);

			// Start with -1 so that if any regular files are present, they are
			// processed first
			int curZipFileCount = -1;
			boolean processZipFile = false;

			String zipFilePath = getRootPath() + "/" + inFolder;
			do {
				ArrayList<String> fileList = null;
				boolean commit = true;
				try {
					if (processZipFile)
						PrestoUtil.unzip(zipFileList.get(curZipFileCount), zipFilePath);

					fileList = getFiles(inFolder);

					for (int i = 0; i < fileList.size(); i++) {
						String files = fileList.get(i);
						logger.info("File Name - " + files);
						int stopCount = Integer.parseInt(PropertyManager.getProperty("DATALOAD.STOP_AFTER", "-1"));
						super.parseDelimitedFile(MarketDataDTO.class, fileList.get(i), '|', fieldNames, stopCount);
					}
					logger.info("Total No of records processed: "+noOfRecords);
					//Clone for multiple Item codes
					logger.info("Total no of market data in HashMap: "+mdBasedOnUpc.size());
//					HashMap<String, List<MarketDataDTO>> mdWithItemCode = applyMDForMultipleItemCode(mdBasedOnUpc);
//					logger.info("# of Market data based on UPC after Grouping: "+mdWithItemCode.size());
					//Delete existing records if available  based on processing date
					MarkerDataDAO markerDataDAO = new MarkerDataDAO();
					markerDataDAO.deleteExistingMarketData(conn, processingMonth);
					//Insert records
					markerDataDAO.insertMarketData(conn, processingMonth, mdBasedOnUpc);
					PristineDBUtil.commitTransaction(conn, "batch record update");
				} catch (GeneralException ex) {
					logger.error("GeneralException", ex);
					PristineDBUtil.rollbackTransaction(conn, "Unexpected Exception");
					commit = false;
					throw new GeneralException("Outer Exception - JavaException", ex);
				} catch (Exception ex) {
					logger.error("JavaException", ex);
					PristineDBUtil.rollbackTransaction(conn, "Unexpected Exception");
					commit = false;
					throw new GeneralException("Outer Exception - JavaException", ex);
				}

				if (processZipFile) {
					PrestoUtil.deleteFiles(fileList);
					fileList.clear();
					fileList.add(zipFileList.get(curZipFileCount));
				}
				String archivePath = getRootPath() + "/" + inFolder + "/";

				if (commit) {
					PrestoUtil.moveFiles(fileList, archivePath + Constants.COMPLETED_FOLDER);
				} else {
					PrestoUtil.moveFiles(fileList, archivePath + Constants.BAD_FOLDER);
				}
				curZipFileCount++;
				processZipFile = true;
			} while (curZipFileCount < zipFileList.size());

		} catch (GeneralException | Exception ex) {
			logger.error("Error occured while processing parseFile()");
			throw new GeneralException("Outer Exception - JavaException", ex);
		} finally {
			PristineDBUtil.close(getOracleConnection());
		}
	}
	
//	private HashMap<String, List<MarketDataDTO>> applyMDForMultipleItemCode(HashMap<String, MarketDataDTO> mdBasedOnUpc){
//		HashMap<String, List<MarketDataDTO>> mdWithMutliItemCode = new HashMap<String, List<MarketDataDTO>>();
//		mdBasedOnUpc.forEach((key,value)->{
//			//If UPC have multiple item code 
//			if(itemCodeandUPCMap.containsKey(key)){
////				List<Integer> itemCodeList = itemCodeandUPCMap.get(key);
//				for(int itemCode: itemCodeList){
//					try {
//						List<MarketDataDTO> tempMdList = new ArrayList<MarketDataDTO>();
//						MarketDataDTO marketDataDTO = (MarketDataDTO) value.clone();
//						marketDataDTO.setItemCode(itemCode);
//						if(mdWithMutliItemCode.containsKey(key)){
//							tempMdList = mdWithMutliItemCode.get(key);
//						}
//						tempMdList.add(marketDataDTO);
//						mdWithMutliItemCode.put(key, tempMdList);
//					} catch (Exception e) {
//						logger.error("Error inside in applyMDForMultipleItemCode()",e);
//						e.printStackTrace();
//					}
//				}
//			}else{
//				List<MarketDataDTO> tempMdList = new ArrayList<MarketDataDTO>();
//				tempMdList.add(value);
//				mdWithMutliItemCode.put(key, tempMdList);
//			}
//		});
//		
//		return mdWithMutliItemCode;
//		
//	}
	
//	private void fillAllColumns() {
//		allColumns.put(1, "upc");
//		allColumns.put(2, "LOB");
//		allColumns.put(3, "group");
//		allColumns.put(4, "subGroup");
//		allColumns.put(5, "productDescription");
//		allColumns.put(6, "orgSize");
//		allColumns.put(7, "brandLow");
//		allColumns.put(8, "brandHigh");
//		allColumns.put(9, "brandOwnLow");
//		allColumns.put(10, "brandOwnHigh");
//		allColumns.put(11, "packageShape");
//		allColumns.put(12, "commonName");
//		allColumns.put(13, "marketDisplayName");
//		allColumns.put(14, "marketDisplayNameDesc");
////		allColumns.put(15, "sales");
////		allColumns.put(16, "units");
////		allColumns.put(17, "nonPromoSales");
////		allColumns.put(18, "nonPromoUnits");
////		allColumns.put(19, "pctACV");
////		allColumns.put(20, "perMMACV");
//		allColumns.put(15, "periodDesc");
//		allColumns.put(16, "sales");
//		allColumns.put(17, "units");
//		allColumns.put(18, "nonPromoSales");
//		allColumns.put(19, "nonPromoUnits");
//		allColumns.put(20, "pctACV");
//		allColumns.put(21, "perMMACV");
//	}
	private void fillAllColumns() {
		allColumns.put(1, "shortProductDescription");
		allColumns.put(2, "LOB");
		allColumns.put(3, "group");
		allColumns.put(4, "subGroup");
		allColumns.put(5, "upc");
		allColumns.put(6, "productDescription");
		allColumns.put(7, "orgSize");
		allColumns.put(8, "brandLow");
		allColumns.put(9, "brandHigh");
		allColumns.put(10, "brandOwnLow");
		allColumns.put(11, "brandOwnHigh");
		allColumns.put(12, "packageShape");
		allColumns.put(13, "commonName");
		allColumns.put(14, "commodityGroup");
		allColumns.put(15, "productSize");
		allColumns.put(16, "totalSize");
		allColumns.put(17, "packSize");
		allColumns.put(18, "form");
		allColumns.put(19, "usdaOraganicSeal");
		allColumns.put(20, "flavour");
		allColumns.put(21, "bonusPack");
		allColumns.put(22, "baseFlavour");
		allColumns.put(23, "marketDisplayName");
		allColumns.put(24, "marketDisplayNameDesc");
		allColumns.put(25, "periodDesc");
		allColumns.put(26, "sales");
		allColumns.put(27, "units");
		allColumns.put(28, "nonPromoSales");
		allColumns.put(29, "nonPromoUnits");
		allColumns.put(30, "pctACV");
		allColumns.put(31, "perMMACV");
	}
	
	private void fillAllColumns1() {
		allColumns.put(0, "marketDisplayName");
		allColumns.put(1, "shortProductDescription");
		allColumns.put(2, "LOB");
		allColumns.put(3, "group");
		allColumns.put(4, "subGroup");
		allColumns.put(5, "upc");
		allColumns.put(6, "productDescription");
		allColumns.put(7, "orgSize");
		allColumns.put(8, "brandLow");
		allColumns.put(9, "brandHigh");
		allColumns.put(10, "brandOwnLow");
		allColumns.put(11, "brandOwnHigh");
		allColumns.put(12, "packageShape");
		allColumns.put(13, "commonName");
		//allColumns.put(14, "marketDisplayName");
//		allColumns.put(15, "sales");
//		allColumns.put(16, "units");
//		allColumns.put(17, "nonPromoSales");
//		allColumns.put(18, "nonPromoUnits");
//		allColumns.put(19, "pctACV");
//		allColumns.put(20, "perMMACV");
		//allColumns.put(15, "marketDisplayNameDesc");
		allColumns.put(14, "periodDesc");
		allColumns.put(15, "sales");
		allColumns.put(16, "units");
		allColumns.put(17, "nonPromoSales");
		allColumns.put(18, "nonPromoUnits");
		allColumns.put(19, "pctACV");
		allColumns.put(20, "perMMACV");
	}
	@Override
	public void processRecords(List listobj) throws GeneralException {
		
		List<MarketDataDTO> mdList = (List<MarketDataDTO>) listobj;
		for (MarketDataDTO mdDetails : mdList) {
			MarketDataDTO marketDataDTO = new MarketDataDTO();
			// UPC
			String upc = PrestoUtil.castUPC(mdDetails.getUpc().trim(), false);
			
			if(!mdBasedOnUpc.containsKey(upc)){
				marketDataDTO.setUpc(upc);
				marketDataDTO.setLOB(mdDetails.getLOB());
				marketDataDTO.setGroup(mdDetails.getGroup());
				marketDataDTO.setSubGroup(mdDetails.getSubGroup());
				marketDataDTO.setProductDescription(mdDetails.getProductDescription());
				marketDataDTO.setShortProductDescription(mdDetails.getShortProductDescription());
				marketDataDTO.setCommodityGroup(mdDetails.getCommodityGroup());
				marketDataDTO.setOrgSize(mdDetails.getOrgSize());
				marketDataDTO.setBrandLow(mdDetails.getBrandLow());
				marketDataDTO.setBrandHigh(mdDetails.getBrandHigh());
				marketDataDTO.setBrandOwnLow(mdDetails.getBrandOwnLow());
				marketDataDTO.setBrandOwnHigh(mdDetails.getBrandOwnHigh());
				marketDataDTO.setPackageShape(mdDetails.getPackageShape());
				marketDataDTO.setCommonName(mdDetails.getCommonName());
				if(itemCodeandUPCMap.containsKey(upc)){
					marketDataDTO.setItemCode(itemCodeandUPCMap.get(upc));
				}
				
				String baseSize = mdDetails.getOrgSize();
				// To separate size from base size
				StringBuilder sb = new StringBuilder();
				for (int i = 0; i < baseSize.length(); i++) {
					char value = baseSize.charAt(i);
					if (value != '.') {
						sb.append(String.valueOf(value).replaceAll("\\D+", ""));
					} else {
						sb.append(String.valueOf(value));
					}
				}
				//Size
				marketDataDTO.setSize(sb.toString());
				
				//UOM Id
				String uomName = mdDetails.getOrgSize().replaceAll("\\d+","").replace(".", "").trim().toUpperCase();
				if (uomMap.containsKey(uomName)) {
					marketDataDTO.setUOMId(uomMap.get(uomName));
				} else if (marketDataDTO.getUOMId() == null && translateUOMMap.containsKey(uomName)) {
					uomName = translateUOMMap.get(uomName);
					if (uomMap.containsKey(uomName))
						marketDataDTO.setUOMId(uomMap.get(uomName));
					else {
						logger.error("UOM id not found for UOM Name: " + uomName);
					} 
				}
				if(brandIDandNameMap.containsKey(mdDetails.getBrandHigh().trim().toUpperCase())){
					marketDataDTO.setBrandId(brandIDandNameMap.get(mdDetails.getBrandHigh().trim().toUpperCase()));
				}else{
					ItemDTO itemDTO = new ItemDTO();
					itemDTO.brandName = mdDetails.getBrandHigh().trim().toUpperCase();
					itemdao.populateBrand(conn, itemDTO);
					if(itemDTO.brandId>0){
						brandIDandNameMap.put(itemDTO.brandName, itemDTO.brandId);
						marketDataDTO.setBrandId(itemDTO.brandId);
					}else{
						logger.info("Brand Id is not availble for Brand Name: "+mdDetails.getBrandHigh().trim().toUpperCase());
					}
				}
			}else{
				marketDataDTO = mdBasedOnUpc.get(upc);
			}
			
			boolean currentYear = true;
			//To get Period End date and to find current year or last year processing
			currentYear = getProcessingYearAndEndData(mdDetails, marketDataDTO);

			if (!mdDetails.getMarketDisplayName().isEmpty() && mdDetails.getMarketDisplayName() != null) {
				if (currentYear) {
					currentYearMetricsData(mdDetails, marketDataDTO);
				} else {
					previousYearMetricsData(mdDetails, marketDataDTO);
				}
			} else {
				logger.error("Market Display name is empty for upc: " + mdDetails.getUpc());
			}
			mdBasedOnUpc.put(marketDataDTO.getUpc(), marketDataDTO);
			noOfRecords++;
			if(noOfRecords > 0 && noOfRecords%10000 == 0){
				logger.info("No Of Records processed: "+noOfRecords);
			}
		}
	}
	
	private void currentYearMetricsData(MarketDataDTO mdDetails,MarketDataDTO marketDataDTO){
		//Giant Eagle brand
		if(mdDetails.getMarketDisplayName().trim().toUpperCase().equals("GIANT EAGLE TOTAL TA")){
			marketDataDTO.setOurTASales(mdDetails.getSales());
			marketDataDTO.setOurTAUnits(mdDetails.getUnits());
			marketDataDTO.setOurTANonPromoSales(mdDetails.getNonPromoSales());
			marketDataDTO.setOurTANonPromoUnits(mdDetails.getNonPromoUnits());
			marketDataDTO.setOurTAPctACV(mdDetails.getPctACV());
			marketDataDTO.setOurTAPerMMACV(mdDetails.getPerMMACV());
		}else if(mdDetails.getMarketDisplayName().trim().toUpperCase().equals("GIANT EAGLE TOTAL REM")){
			marketDataDTO.setRemTASales(mdDetails.getSales());
			marketDataDTO.setRemTAUnits(mdDetails.getUnits());
			marketDataDTO.setRemTANonPromoSales(mdDetails.getNonPromoSales());
			marketDataDTO.setRemTANonPromoUnits(mdDetails.getNonPromoUnits());
			marketDataDTO.setRemTAPctACV(mdDetails.getPctACV());
			marketDataDTO.setRemTAPerMMACV(mdDetails.getPerMMACV());
		}else if(mdDetails.getMarketDisplayName().trim().toUpperCase().equals("GIANT EAGLE TOTAL XAOC REM")){
			marketDataDTO.setTotRemTASales(mdDetails.getSales());
			marketDataDTO.setTotRemTAUnits(mdDetails.getUnits());
			marketDataDTO.setTotRemTANonPromoSales(mdDetails.getNonPromoSales());
			marketDataDTO.setTotRemTANonPromoUnits(mdDetails.getNonPromoUnits());
			marketDataDTO.setTotRemTAPctACV(mdDetails.getPctACV());
			marketDataDTO.setTotRemTAPerMMACV(mdDetails.getPerMMACV());
		}
	}
	
	private void previousYearMetricsData(MarketDataDTO mdDetails,MarketDataDTO marketDataDTO){
		//Giant Eagle brand
		if(mdDetails.getMarketDisplayName().trim().toUpperCase().equals("GIANT EAGLE TOTAL TA")){
			marketDataDTO.setLyOurTASales(mdDetails.getSales());
			marketDataDTO.setLyOurTAUnits(mdDetails.getUnits());
			marketDataDTO.setLyOurTANonPromoSales(mdDetails.getNonPromoSales());
			marketDataDTO.setLyOurTANonPromoUnits(mdDetails.getNonPromoUnits());
			marketDataDTO.setLyOurTAPctACV(mdDetails.getPctACV());
			marketDataDTO.setLyOurTAPerMMACV(mdDetails.getPerMMACV());
		}else if(mdDetails.getMarketDisplayName().trim().toUpperCase().equals("GIANT EAGLE TOTAL REM")){
			marketDataDTO.setLyRemTASales(mdDetails.getSales());
			marketDataDTO.setLyRemTAUnits(mdDetails.getUnits());
			marketDataDTO.setLyRemTANonPromoSales(mdDetails.getNonPromoSales());
			marketDataDTO.setLyRemTANonPromoUnits(mdDetails.getNonPromoUnits());
			marketDataDTO.setLyRemTAPctACV(mdDetails.getPctACV());
			marketDataDTO.setLyRemTAPerMMACV(mdDetails.getPerMMACV());
		}else if(mdDetails.getMarketDisplayName().trim().toUpperCase().equals("GIANT EAGLE TOTAL XAOC REM")){
			marketDataDTO.setLyTotRemTASales(mdDetails.getSales());
			marketDataDTO.setLyTotRemTAUnits(mdDetails.getUnits());
			marketDataDTO.setLyTotRemTANonPromoSales(mdDetails.getNonPromoSales());
			marketDataDTO.setLyTotRemTANonPromoUnits(mdDetails.getNonPromoUnits());
			marketDataDTO.setLyTotRemTAPctACV(mdDetails.getPctACV());
			marketDataDTO.setLyTotRemTAPerMMACV(mdDetails.getPerMMACV());
		}
	}
	
	private boolean getProcessingYearAndEndData(MarketDataDTO mdDetails, MarketDataDTO marketDataDTO)
			throws GeneralException {
		boolean currentYear = true;
		String periodDetail = mdDetails.getPeriodDesc();
		
		logger.debug(periodDetail);
		String[] value = periodDetail.split("-");
		String periodEndDate = null;
		logger.debug(value.length);
		if (value.length == 2) {

			for (int i = 0; i < value.length; i++) {
				if (value[i].contains("YA")) {
					currentYear = false;
					periodEndDate = value[i].replaceAll("\\D+", "").trim();
				} else {
					periodEndDate = value[i].replaceAll("\\D+", "").trim();
				}
			}
		}
		// Period End date
		if(currentYear){
			logger.debug(periodEndDate);
			marketDataDTO.setPeriodEndDate(
					DateUtil.dateToString(DateUtil.toDate(periodEndDate, "MMddyy"), Constants.APP_DATE_FORMAT));
		}else{
			marketDataDTO.setLyPeriodEndDate(
					DateUtil.dateToString(DateUtil.toDate(periodEndDate, "MMddyy"), Constants.APP_DATE_FORMAT));
		}
		return currentYear;
		
	}
	private void setupUOMTranslation(){
		translateUOMMap.put("GALLO", "GA");
		translateUOMMap.put("SQF", "SF");
		translateUOMMap.put("OOZ", "OZ");
		translateUOMMap.put("OUNC", "OZ");
		translateUOMMap.put("SQ FT", "SF");
		translateUOMMap.put("COUN", "CT");
		translateUOMMap.put("EACH", "EA");
		translateUOMMap.put("QUART", "QT");
		translateUOMMap.put("POUND", "LB");
		translateUOMMap.put("GRAM", "GR");
		translateUOMMap.put("ROLL", "ROL");
		translateUOMMap.put("PINT", "PT");
		translateUOMMap.put("INCH", "IN");
		translateUOMMap.put("FLOZ", "FZ");
		translateUOMMap.put("PACK", "PK");
		translateUOMMap.put("YDS", "YD");
		translateUOMMap.put("OZS", "OZ");
		translateUOMMap.put("OZZ", "OZ");
		translateUOMMap.put("LBA", "LB");
		translateUOMMap.put("OZO", "OZ");
		translateUOMMap.put("POUN", "LB");
		translateUOMMap.put("PKG", "PK");
		translateUOMMap.put("EAC", "EA");
		translateUOMMap.put("PKT", "PK");
		translateUOMMap.put("LBS", "LB");
		translateUOMMap.put("GAL", "GA");
		translateUOMMap.put("GALLON", "GA");
		translateUOMMap.put("DOZ", "DZ");
		translateUOMMap.put("EACHN", "EA");
		translateUOMMap.put("LITER", "LT");
		translateUOMMap.put("LTR", "LT");
		translateUOMMap.put("SINGLE", "EA");
		translateUOMMap.put("SINGL", "EA");
		translateUOMMap.put("SQ", "SF");
		translateUOMMap.put("SFT", "SF");
		translateUOMMap.put("OUNCE", "OZ");
		translateUOMMap.put("PNT", "PT");
		translateUOMMap.put("PAC", "PK");
		translateUOMMap.put("PACKAG", "PK");
		translateUOMMap.put("FLZ", "FZ");
		translateUOMMap.put("FLO", "FZ");
		translateUOMMap.put("FL", "FZ");
		translateUOMMap.put("FOZ", "FZ");
		translateUOMMap.put("OZF", "FZ");
		
		translateUOMMap.put("GRMS", "GR");
		translateUOMMap.put("CO", "CT");
		translateUOMMap.put("FLUID OUNCE", "FZ");
		translateUOMMap.put("OUNCE", "OZ");
		translateUOMMap.put("COUNT", "CT");
		translateUOMMap.put("MILLILITER", "ML");
		translateUOMMap.put("SQUARE FOOT", "SF");
		translateUOMMap.put("FOOT", "FT");
		translateUOMMap.put("YARD", "YD");

	}
}
