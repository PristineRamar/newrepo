package com.pristine.dataload;

import java.io.File;
import java.io.FileReader;
import java.sql.Connection;
import java.util.ArrayList;
import java.util.Calendar;
import java.util.Date;
import java.util.HashMap;
import java.util.List;
import java.util.stream.Collectors;

import org.apache.log4j.Logger;
import org.apache.log4j.PropertyConfigurator;

import com.csvreader.CsvReader;
import com.pristine.dao.DBManager;
import com.pristine.dao.ItemDAO;
import com.pristine.dao.RetailCalendarDAO;
import com.pristine.dao.RetailPriceDAO;
import com.pristine.dao.marketDataDAO;
import com.pristine.dto.ItemDTO;
import com.pristine.dto.NelsonMarketDataDTO;
import com.pristine.exception.GeneralException;
import com.pristine.util.Constants;
import com.pristine.util.DateUtil;
import com.pristine.util.NumberUtil;
import com.pristine.util.PrestoUtil;
import com.pristine.util.PristineDBUtil;
import com.pristine.util.PropertyManager;

public class MarketDataLoader {

	private static Logger logger = Logger.getLogger("MarketDataLoader");
	private static String FILE_PATH = "FILE_PATH=";
	@SuppressWarnings("unused")
	private static String OUTPUT_PATH = "OUTPUT_PATH=";
	private static String relativeInputPath;
	@SuppressWarnings("unused")
	private static String relativeOutputPath;
	private RetailPriceDAO retailPriceDAO = new RetailPriceDAO();
	RetailCalendarDAO calDAO = new RetailCalendarDAO();
	HashMap<String, Integer> zoneMap;
	HashMap<String, List<ItemDTO>> upcAndItsItem = null;
	Connection conn = null;
	HashMap<String, List<ItemDTO>> retailerItemCodeAndItsItem = null;
	String rootPath = PropertyManager.getProperty("DATALOAD.ROOTPATH", "");
	List<ItemDTO> activeItems = null;

	DateUtil dateUtil = new DateUtil();

	RetailCalendarDAO calendarDAO = new RetailCalendarDAO();
	marketDataDAO marketDAO = new marketDataDAO();

	public MarketDataLoader() {
		try {
			conn = DBManager.getConnection();
		} catch (GeneralException var1_1) {
			logger.error("Error when creating connection - " + var1_1);
		}
	}

	public static void main(String args[]) throws GeneralException, Exception {

		PropertyConfigurator.configure("log4j-MarketDataLoader.properties");
		PropertyManager.initialize("analysis.properties");

		MarketDataLoader marketLoader = new MarketDataLoader();

		for (String arg : args) {
			if (arg.startsWith(FILE_PATH)) {
				relativeInputPath = arg.substring(FILE_PATH.length());
			}
			/*
			 * if (arg.startsWith(OUTPUT_PATH)) { relativeOutputPath =
			 * arg.substring(OUTPUT_PATH.length()); }
			 */
		}

		marketLoader.initializeData();

		logger.info("ProcessCompData started.......");
		marketLoader.processCompData();

		logger.info("ProcessCompData completed......");

	}

	private void processCompData() throws Exception, GeneralException {
		File myDirectory = new File(rootPath + "/" + relativeInputPath);
		logger.info(myDirectory);
		String[] fileList = myDirectory.list();
		String fName = "";

		List<NelsonMarketDataDTO> marketData = null;
		for (String fileName : fileList) {

			logger.info("Reading  file" + fileName);
			String file = myDirectory + "/" + fileName;
			marketData = readPromofiles(file);

			logger.info("processCompData()- # no of items Read :" + marketData.size());
			fName = fileName;
			int counter = 0;
			int totalRecords = 0;

			List<NelsonMarketDataDTO> tempList = new ArrayList<>();
			for (NelsonMarketDataDTO data : marketData) {
				tempList.add(data);
				counter++;
				if (counter == 1000) {

					totalRecords = totalRecords + counter;
					Startprocessing(tempList, fName);
					tempList = new ArrayList<>();
					counter = 0;
				}
			}
			if (counter > 0) {
				Startprocessing(tempList, fName);
				totalRecords = totalRecords + counter;
			}
			logger.info("Records inserted from file " + fName + "are :" + totalRecords);
		}

		// PrestoUtil.moveFile(fName, rootPath + "/" + relativeOutputPath + "/" +
		// Constants.COMPLETED_FOLDER);

	}

	public void Startprocessing(List<NelsonMarketDataDTO> tempList, String fName)
			throws CloneNotSupportedException, GeneralException {

		List<NelsonMarketDataDTO> zonelevelmapping = GroupbyZone(tempList);

		List<NelsonMarketDataDTO> items = getPrestoItemCode(zonelevelmapping);

		List<NelsonMarketDataDTO> insertList = parseTimeString(items);

		logger.info("insert started for Items" + insertList.size());
		int status = marketDAO.insertMarketDataBatch(insertList, conn);

		if (status > 0) {
			try {
				PristineDBUtil.commitTransaction(conn, "batch record update");
				logger.info("insert commited");
			} catch (Exception ex) {

				logger.error("processFile()-Exception while inserting  for file : " + fName);

				PristineDBUtil.rollbackTransaction(conn, "Unexpected Exception");
			}
		}

	}

	private List<NelsonMarketDataDTO> parseTimeString(List<NelsonMarketDataDTO> itemList) throws GeneralException {

		for (NelsonMarketDataDTO nelsonMarketDataDTO : itemList)

		{
			String timePeriod = nelsonMarketDataDTO.getTimePeriod().trim();

			if (timePeriod.length() == 25 && timePeriod.contains("Weeks ending")) {

				int noOfweeks = Integer.parseInt(timePeriod.substring(0, 1));
				String endDate = timePeriod.substring(14, timePeriod.length());
				String startDate = getWeekStartDate(DateUtil.toDate(endDate), noOfweeks);
				nelsonMarketDataDTO.setStartDate(startDate);
				nelsonMarketDataDTO.setEndDate(endDate);
				String calendarInfo = calendarDAO.getCalendarType(conn, nelsonMarketDataDTO);

				String[] cal = calendarInfo.split(";");

				nelsonMarketDataDTO.setCalendarType(cal[0]);
				nelsonMarketDataDTO.setCalendarId(Integer.parseInt(cal[1]));
			}

		}

		return itemList;

	}

	private void initializeData() throws GeneralException {

		try {

			ItemDAO itemDAO = new ItemDAO();

			logger.info("initializeData() - Getting all items started...");
			activeItems = itemDAO.getAllActiveItems(conn);
			groupItemByUPC();
			logger.info("initializeData() -	Active items" + activeItems.size());
			logger.info("initializeData() - Getting zoneDetails started.....");
			zoneMap = retailPriceDAO.getRetailPriceZone(conn);
			logger.info("initializeData() - Getting zoneDetails complete.....");

		} catch (GeneralException | Exception e) {
			throw new GeneralException("initializeData() - Error while initializing cache", e);
		}
	}

	private void groupItemByUPC() {
		upcAndItsItem = (HashMap<String, List<ItemDTO>>) activeItems.stream()
				.filter(p -> p.getUpc() != null && !p.getUpc().equals("null"))
				.collect(Collectors.groupingBy(ItemDTO::getUpc));
	}

	private List<NelsonMarketDataDTO> getPrestoItemCode(List<NelsonMarketDataDTO> zonelevelmapping)
			throws CloneNotSupportedException {

		List<NelsonMarketDataDTO> finalList = new ArrayList<>();

		for (NelsonMarketDataDTO nelsondto : zonelevelmapping) {
			String UPC = PrestoUtil.castUPC(nelsondto.getUPC(), false);
			// logger.info("UPC is " + UPC);
			if (upcAndItsItem.containsKey(UPC)) {
				// logger.info("found ");
				List<ItemDTO> itemcodeList = upcAndItsItem.get(UPC);
				// logger.info("itemcodeList size " + itemcodeList.size());
				if (itemcodeList.size() > 0) {
					for (ItemDTO itemCode : itemcodeList) {

						NelsonMarketDataDTO nelsonmarketDTO = (NelsonMarketDataDTO) nelsondto.clone();
						nelsonmarketDTO.setItemCode(itemCode.getItemCode());
						nelsonmarketDTO.setIscarriedItem("C");
						nelsonmarketDTO.setUPC(UPC);
						finalList.add(nelsonmarketDTO);

					}
				}
			} else {
				nelsondto.setIscarriedItem("NC");
				finalList.add(nelsondto);
			}
		}
		logger.info("getPrestoItemCode()-# items in list" + finalList.size());
		return finalList;

	}

	private List<NelsonMarketDataDTO> GroupbyZone(List<NelsonMarketDataDTO> marketData) {

		List<NelsonMarketDataDTO> zonelevel = new ArrayList<>();

		for (NelsonMarketDataDTO dataDTO : marketData) {
			if (dataDTO.getMarketName().trim().length() == 4) {
				String getZoneNo = PrestoUtil.castZoneNumber(dataDTO.getMarketName().trim().substring(0, 3));

				if (zoneMap.containsKey(getZoneNo)) {
					dataDTO.setLocationNo((getZoneNo));
					dataDTO.setLocationLevel(Constants.ZONE);
					dataDTO.setLocationlevelID(Constants.ZONE_LEVEL_ID);
					dataDTO.setZoneId(zoneMap.get(getZoneNo));
					zonelevel.add(dataDTO);
				}

			} else if (dataDTO.getMarketName().trim().length() == 3) {
				String zoneNum = PrestoUtil.castZoneNumber(dataDTO.getMarketName());
				if (zoneMap.containsKey(zoneNum)) {
					dataDTO.setLocationNo((zoneNum));
					dataDTO.setLocationLevel(Constants.ZONE);
					dataDTO.setLocationlevelID(Constants.ZONE_LEVEL_ID);
					dataDTO.setZoneId(zoneMap.get(zoneNum));
					zonelevel.add(dataDTO);
				}
			} else if (dataDTO.getMarketName().trim().length() > 4) {

				zonelevel.add(dataDTO);
			}

		}

		return zonelevel;
	}

	private List<NelsonMarketDataDTO> readPromofiles(String filePath) throws Exception {
		List<NelsonMarketDataDTO> marketData = new ArrayList<>();
		CsvReader csvReader = readFile(filePath, ',');

		String line[];
		int counter = 0;
		while (csvReader.readRecord()) {
			if (counter != 0) {
				line = csvReader.getValues();
				NelsonMarketDataDTO mktData = new NelsonMarketDataDTO();
				mktData.setMarketName(line[0]);
				mktData.setUPC(line[1]);
				mktData.setUPCDesc(line[2]);
				mktData.setTimePeriod(line[3]);
				mktData.setRetailCondn(line[4]);			
				mktData.setActUnitsProj(NumberUtil.parseStringToInteger(line[5], 0));
				mktData.setActUnitsProjCompMkt(NumberUtil.parseStringToInteger(line[6], 0));
				mktData.setActSalesProj(NumberUtil.parseStringToInteger(line[7], 0));
				mktData.setActsalesProjCompMkt(NumberUtil.parseStringToInteger(line[8], 0));
				mktData.setAvgPrc(NumberUtil.parseStringToDouble(line[9], 0.0));
				mktData.setAvglPrcCompMkt(NumberUtil.parseStringToDouble(line[10], 0.0));
				mktData.setElasticityEstFcsMKt(NumberUtil.parseStringToDouble(line[11], 0.0));
				mktData.setElasticityEstTotMkt(NumberUtil.parseStringToDouble(line[12], 0.0));
				mktData.setNf2FcsMkt(NumberUtil.parseStringToDouble(line[13], 0.0));
				mktData.setNf3FcsMkt(NumberUtil.parseStringToDouble(line[14], 0.0));
				mktData.setNf4FcsMkt(NumberUtil.parseStringToDouble(line[15], 0.0));
				mktData.setEstFcsDisMkt(NumberUtil.parseStringToDouble(line[16], 0.0));
				mktData.setEstAdFcsMkt(NumberUtil.parseStringToDouble(line[17], 0.0));
				mktData.setEstDisAdFcsMkt(NumberUtil.parseStringToDouble(line[18], 0.0));
				mktData.setMcpCompMkt(NumberUtil.parseStringToDouble(line[19], 0.0));
				mktData.setUnitPerMcpCompMkt(NumberUtil.parseStringToDouble(line[20], 0.0));
				mktData.setFivePercentilePrc(NumberUtil.parseStringToDouble(line[21], 0.0));
				mktData.setTenPercentilePrc(NumberUtil.parseStringToDouble(line[22], 0.0));
				mktData.setFifteenPercentilePrc(NumberUtil.parseStringToDouble(line[23], 0.0));
				mktData.setTwentyPercentilePrc(NumberUtil.parseStringToDouble(line[24], 0.0));
				mktData.setTwentyFivePercentilePrc(NumberUtil.parseStringToDouble(line[25], 0.0));
				mktData.setThirtyPercentilePrc(NumberUtil.parseStringToDouble(line[26], 0.0));
				mktData.setThirtyFivePercentilePrc(NumberUtil.parseStringToDouble(line[27], 0.0));
				mktData.setFourtyPercentilePrc(NumberUtil.parseStringToDouble(line[28], 0.0));
				mktData.setFourtyFivePercentilePrc(NumberUtil.parseStringToDouble(line[29], 0.0));
				mktData.setFiftyPercentilePrc(NumberUtil.parseStringToDouble(line[30], 0.0));
				mktData.setFiftyfivePercentilePrc(NumberUtil.parseStringToDouble(line[31], 0.0));
				mktData.setSixtyPercentilePrc(NumberUtil.parseStringToDouble(line[32], 0.0));
				mktData.setSixtyFivePercentilePrc(NumberUtil.parseStringToDouble(line[33], 0.0));
				mktData.setSeventyPercentilePrc(NumberUtil.parseStringToDouble(line[34], 0.0));
				mktData.setSeventyFivePercentilePrc(NumberUtil.parseStringToDouble(line[35], 0.0));
				mktData.setEightyPercentilePrc(NumberUtil.parseStringToDouble(line[36], 0.0));
				mktData.setEightyFivePercentilePrc(NumberUtil.parseStringToDouble(line[37], 0.0));
				mktData.setNinetyPercentilePrc(NumberUtil.parseStringToDouble(line[38], 0.0));
				mktData.setNinetyFivePercentilePrc(NumberUtil.parseStringToDouble(line[39], 0.0));

				marketData.add(mktData);
			}
			counter++;

		}

		csvReader.close();
		return marketData;
	}

	protected CsvReader readFile(String fileName, char delimiter) throws Exception {
		CsvReader reader = null;
		try {
			reader = new CsvReader(new FileReader(fileName));
			if (delimiter != '0') {
				reader.setDelimiter(delimiter);
			}
		} catch (Exception e) {
			throw new Exception("File read error ", e);
		}
		return reader;
	}

	public static String getWeekStartDate(Date currentDate, int prevNoOfWeeks) {
		return getWeekStartDate(currentDate, prevNoOfWeeks, Constants.APP_DATE_FORMAT);
	}

	public static String getWeekStartDate(Date currentDate, int prevNoOfWeeks, String dateFormat) {
		java.text.SimpleDateFormat sdf = new java.text.SimpleDateFormat(dateFormat);
		Calendar c1 = Calendar.getInstance();
		c1.setTime(currentDate);
		c1.getTime();
		int dow = c1.get(Calendar.DAY_OF_WEEK);

		int dayIndex = 0;
		if (PropertyManager.getProperty("CUSTOM_WEEK_START_INDEX") != null) {
			dayIndex = Integer.parseInt(PropertyManager.getProperty("CUSTOM_WEEK_START_INDEX"));
		}
		if (dayIndex > 0) {
			c1.setFirstDayOfWeek(dayIndex + 1);
			c1.set(Calendar.DAY_OF_WEEK, dayIndex + 1);
			int weekStartDays = 7 * prevNoOfWeeks;
			c1.add(Calendar.DATE, weekStartDays * -1);
		} else {
			int weekStartDays = (7 * (prevNoOfWeeks) - 1);
			c1.add(Calendar.DATE, weekStartDays * -1);
		}
		String weekStartDate = sdf.format(c1.getTime());
		return weekStartDate;
	}

}// end
