package com.pristine.fileformatter.riteaid;

import java.io.File;
import java.io.FileReader;
import java.io.FileWriter;
import java.io.IOException;
import java.io.PrintWriter;
import java.sql.Connection;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.stream.Collectors;

import org.apache.log4j.Logger;
import org.apache.log4j.PropertyConfigurator;

import com.csvreader.CsvReader;
import com.pristine.dao.DBManager;
import com.pristine.dao.ItemDAO;
import com.pristine.dao.RetailCalendarDAO;
import com.pristine.dao.StoreDAO;
import com.pristine.dao.marketDataDAO;
import com.pristine.dto.ItemDTO;
import com.pristine.dto.NelsonMarketDataDTO;
import com.pristine.dto.StoreDTO;
import com.pristine.dto.fileformatter.RiteAid.CompetitorDTO;
import com.pristine.exception.GeneralException;
import com.pristine.util.Constants;
import com.pristine.util.PrestoUtil;
import com.pristine.util.PristineDBUtil;
import com.pristine.util.PropertyManager;

public class CompetitorFileGenerator {

	private static Logger logger = Logger.getLogger("CompetitorFileGenerator");

	private static String PRICE_TYPE = "PRICE_TYPE=";
	private static String FILE_PATH = "FILE_PATH=";
	private static String relativeInputPath;
	private static String relativeOutputPath;
	private static String OUTPUT_PATH = "OUTPUT_PATH=";
	private static String PriceType;

	HashMap<String, Integer> zoneMap;
	HashMap<String, List<ItemDTO>> upcAndItsItem = null;
	Connection conn = null;
	HashMap<String, List<ItemDTO>> retailerItemCodeAndItsItem = null;
	String rootPath = PropertyManager.getProperty("DATALOAD.ROOTPATH", "");
	List<ItemDTO> activeItems = null;
	List<String> storeList = new ArrayList<String>();

	List<NelsonMarketDataDTO> finalList = new ArrayList<>();
	RetailCalendarDAO calendarDAO = new RetailCalendarDAO();
	marketDataDAO marketDAO = new marketDataDAO();
	HashMap<String, List<NelsonMarketDataDTO>> zoneUPcMap = new HashMap<>();
	String processingFile = "";
	private FileWriter fw = null;
	private PrintWriter pw = null;
	int ChainId;
	StoreDAO storeDAO = new StoreDAO();
	HashSet<String> distinctStores = new HashSet<String>();
	static HashSet<String> ignoredRecords = new HashSet<String>();

	public CompetitorFileGenerator() {
		try {
			conn = DBManager.getConnection();
		} catch (GeneralException var1_1) {
			logger.error("Error when creating connection - " + var1_1);
		}
	}

	public static void main(String args[]) throws GeneralException, Exception {

		PropertyConfigurator.configure("log4j-CompetitorFileGenerator.properties");
		PropertyManager.initialize("analysis.properties");

		CompetitorFileGenerator compData = new CompetitorFileGenerator();

		for (String arg : args) {

			if (arg.startsWith(PRICE_TYPE)) {
				PriceType = arg.substring(PRICE_TYPE.length());
			}
			if (arg.startsWith(FILE_PATH)) {
				relativeInputPath = arg.substring(FILE_PATH.length());
			}
			if (arg.startsWith(OUTPUT_PATH)) {
				relativeOutputPath = arg.substring(OUTPUT_PATH.length());
			}

		}

		logger.info("Read market File started.......");
		compData.initializeData();
		compData.ReadmarketFile();

		List<CompetitorDTO> compDataList = compData.SetUpCompData(PriceType);

		logger.info("Competitor Data set for records# : " + " " + compDataList.size());
		logger.info("Competitor Data notset for records# : " + " " + ignoredRecords.size());

		compData.SetupCompStore();

		compData.writeToCsv(compDataList);

		logger.info("ProcessCompData completed......");

	}

	private void SetupCompStore() throws GeneralException {

		for (String stores : distinctStores) {

			String[] storeInfo = stores.split(";");
			String storeNum = storeInfo[0];
			String storeName = storeInfo[1];

			if (!storeList.contains(storeNum)) {
				
				StoreDTO storeDTO = new StoreDTO();
				String[] storeSplit = storeNum.split("-");
				String ZoneNum = PrestoUtil.castZoneNumber(storeSplit[1]);
				storeDTO.strNum = storeNum;
				storeDTO.strName = storeName;
				storeDTO.chainId = ChainId;
				storeDTO.sourceInfo = "MARKET-DATA";
				storeDTO = storeDAO.setStoreDetails(conn, storeDTO, ZoneNum);

				try {
					storeDAO.insertStore(conn, storeDTO);
					PristineDBUtil.commitTransaction(conn, "Store Data Setup");
				} catch (Exception ex) {
					PristineDBUtil.rollbackTransaction(conn, "Unexpected Exception");
					logger.info("Exception in insert" + ex);
				}

				logger.info("StoreInsertComplete for " + storeInfo[0]);
			}

		}

	}

	private List<CompetitorDTO> SetUpCompData(String priceType) {

		List<CompetitorDTO> finalList = new ArrayList<>();

		zoneUPcMap.forEach((k, v) -> {

			CompetitorDTO competitorDTO = new CompetitorDTO();
			double regPrice = 0;
			double salePrice = 0;
			double totalPrice = 0;

			List<NelsonMarketDataDTO> getZoneInfo = zoneUPcMap.get(k);

			// SetStoreNum
			String[] SplitKey = k.split(" - ");
			String zoneNumber = SplitKey[0];
			// String UPC = SplitKey[1];

			String StoreName = generateStoreNameAndNum(priceType);

			String[] SplitStore = StoreName.split(";");
			String storeName = SplitStore[0];
			String storeNum = SplitStore[1];

			// set store Name and number
			competitorDTO.setCompetitor_Store_Num("MD" + "-" + zoneNumber + "-" + storeNum);
			competitorDTO.setCompetitor_Store_Name("MD" + "-" + zoneNumber + "-" + storeName);

			// Setdate
			NelsonMarketDataDTO firstField = getZoneInfo.get(0);
			String timePeriod = firstField.getTimePeriod();

			if (timePeriod.length() == 25 && timePeriod.contains("Weeks ending")) {
				String enddate = timePeriod.substring(14, timePeriod.length());
				competitorDTO.setPrice_Check_Date(enddate.trim());
			}
			competitorDTO.setItem_Description(firstField.getUPCDesc());
			SetItemCode(firstField, competitorDTO);

			competitorDTO.setOutside_Indicator("N");

			for (NelsonMarketDataDTO zoneInfo : getZoneInfo) {

				if (zoneInfo.getRetailCondn().equals(Constants.NONPROMO)) {
					competitorDTO.setUPC(zoneInfo.getUPC());
					regPrice = getRetailorSaleInfo(priceType, zoneInfo, competitorDTO);

				}
				if (zoneInfo.getRetailCondn().equals(Constants.PROMO)) {
					salePrice = getRetailorSaleInfo(priceType, zoneInfo, competitorDTO);

				}
				if (zoneInfo.getRetailCondn().equals(Constants.TOTAL)) {
					competitorDTO.setUPC(zoneInfo.getUPC());
					totalPrice = getRetailorSaleInfo(priceType, zoneInfo, competitorDTO);
				}
			}

			if (regPrice == 0) {

				if (salePrice <= totalPrice) {

					if (totalPrice != 0) {
						competitorDTO.setRegular_Retail(totalPrice);
						competitorDTO.setReg_Qty(1);

						if (salePrice < totalPrice) {
							competitorDTO.setSale_Retail(salePrice);
							if (salePrice > 0) {
								competitorDTO.setSale_Qty(1);
							} else {
								competitorDTO.setSale_Qty(0);
							}
						}

						distinctStores.add(competitorDTO.getCompetitor_Store_Num() + ";"
								+ competitorDTO.getCompetitor_Store_Name());
						finalList.add(competitorDTO);
					} else {

						ignoredRecords.add(zoneNumber + "-" + firstField.getUPC());
						logger.info("Competitor Data not set for " + zoneNumber + " upc" + firstField.getUPC());
					}
				} else {

					ignoredRecords.add(zoneNumber + "-" + firstField.getUPC());
					logger.info("Competitor Data not set for " + zoneNumber + " upc" + firstField.getUPC());
				}
			} else {
							
				competitorDTO.setRegular_Retail(regPrice);
				competitorDTO.setReg_Qty(1);

				if (regPrice != salePrice) {
					if (salePrice < regPrice) {
						competitorDTO.setSale_Retail(salePrice);
						if (salePrice > 0) {
							competitorDTO.setSale_Qty(1);
						} else {
							competitorDTO.setSale_Qty(0);
						}
					}

				} else {
					logger.info("Regprice is same as SP for  " + zoneNumber + " UPC:" + firstField.getUPC()
							+ "Hence setting salePrice =0");
				}
				distinctStores
						.add(competitorDTO.getCompetitor_Store_Num() + ";" + competitorDTO.getCompetitor_Store_Name());
				finalList.add(competitorDTO);
			}

		});

		return finalList;

	}

	private Double getRetailorSaleInfo(String priceType, NelsonMarketDataDTO zoneInfo, CompetitorDTO competitorDTO) {

		double Price = 0;
		switch (priceType) {
		case "MCP":
			Price = (zoneInfo.getMcpCompMkt());
			break;
		case "FIVEPCNTILE":
			Price = (zoneInfo.getFivePercentilePrc());
			break;
		case "TENPCNTILE":
			Price = (zoneInfo.getTenPercentilePrc());
			break;
		case "FIFTEENPCNTILE":
			Price = (zoneInfo.getFifteenPercentilePrc());
			break;
		case "TWENTYPCNTILE":
			Price = (zoneInfo.getTwentyPercentilePrc());
			break;
		case "TWENTYFIVEPCNTILE":
			Price = (zoneInfo.getTwentyFivePercentilePrc());
			break;
		case "THIRTYPCNTILE":
			Price = (zoneInfo.getThirtyPercentilePrc());
			break;
		case "THIRTYFIVEPCNTILE":
			Price = (zoneInfo.getThirtyFivePercentilePrc());
			break;
		case "FOURTYPCNTILE":
			Price = (zoneInfo.getFourtyPercentilePrc());
			break;
		case "FOURTYFIVEPCNTILE":
			Price = (zoneInfo.getFourtyFivePercentilePrc());
			break;
		case "FIFTYPCNTILE":
			Price = (zoneInfo.getFiftyPercentilePrc());
			break;
		case "FIFTYFIVEPCNTILE":
			Price = (zoneInfo.getFiftyfivePercentilePrc());
			break;
		case "SIXTYPCNTILE":
			Price = (zoneInfo.getSixtyPercentilePrc());
			break;
		case "SIXTYFIVEPCNTILE":
			Price = (zoneInfo.getSixtyFivePercentilePrc());
			break;
		case "SEVENTYPCNTILE":
			Price = (zoneInfo.getSeventyPercentilePrc());
			break;
		case "SEVENTYFIVEPCNTILE":
			Price = (zoneInfo.getSeventyFivePercentilePrc());
			break;
		case "EIGHTYPCNTILE":
			Price = (zoneInfo.getEightyPercentilePrc());
			break;
		case "EIGHTYFIVEPCNTILE":
			Price = (zoneInfo.getEightyFivePercentilePrc());
			break;
		case "NINETYPCNTILE":
			Price = (zoneInfo.getNinetyPercentilePrc());
			break;
		case "NINETYFIVEPCNTILE":
			Price = (zoneInfo.getNinetyFivePercentilePrc());
			break;
		case "AVGPRICE":
			Price = (zoneInfo.getAvgPrc());
			break;
		case "AVGPRICECMP":
			Price = (zoneInfo.getAvglPrcCompMkt());
			break;
		}

		return Price;
	}

	private void SetItemCode(NelsonMarketDataDTO firstField, CompetitorDTO competitorDTO) {
		String UPC = PrestoUtil.castUPC(firstField.getUPC(), false);

		if (upcAndItsItem.containsKey(UPC)) {

			List<ItemDTO> itemcodeList = upcAndItsItem.get(UPC);

			if (itemcodeList.size() > 0) {
				for (ItemDTO itemCode : itemcodeList) {

					competitorDTO.setItem_Code(itemCode.getRetailerItemCode());
				}
			}

		} else {
			competitorDTO.setItem_Code("");
			logger.info("ItemCode not found for UPC" + UPC);
		}
	}

	private void initializeData() throws GeneralException {

		try {
			ChainId = Integer.parseInt(PropertyManager.getProperty("MARKETCHAINID", "213"));
			ItemDAO itemDAO = new ItemDAO();

			logger.info("initializeData() - Getting all items started...");
			activeItems = itemDAO.getAllActiveItems(conn);

			storeList = storeDAO.getExistingStores(conn, ChainId);
			logger.info("Existing stores Count" + storeList.size());

			groupItemByUPC();
			logger.info("initializeData() -	Active items" + activeItems.size());

		} catch (GeneralException | Exception e) {
			throw new GeneralException("initializeData() - Error while initializing cache", e);
		}
	}

	private void groupItemByUPC() {
		upcAndItsItem = (HashMap<String, List<ItemDTO>>) activeItems.stream()
				.filter(p -> p.getUpc() != null && !p.getUpc().equals("null"))
				.collect(Collectors.groupingBy(ItemDTO::getUpc));
	}

	private String generateStoreNameAndNum(String priceType) {
		String storeInfo = "";
		String storeName = "";
		String storeNum = "";

		switch (priceType) {
		case "MCP":
			storeName = "Most Common Price";
			storeNum = "MCP";
			break;
		case "FIVEPCNTILE":
			storeName = "5thPercentile";
			storeNum = "5P";
			break;
		case "TENPCNTILE":
			storeName = "10thPercentile";
			storeNum = "10P";
			break;
		case "FIFTEENPCNTILE":
			storeName = "15thPercentile";
			storeNum = "15P";
			break;
		case "TWENTYPCNTILE":
			storeName = "20thPercentile";
			storeNum = "20P";
			break;
		case "TWENTYFIVEPCNTILE":
			storeName = "25thPercentile";
			storeNum = "25P";
			break;
		case "THIRTYPCNTILE":
			storeName = "30thPercentile";
			storeNum = "30P";
			break;
		case "THIRTYFIVEPCNTILE":
			storeName = "35thPercentile";
			storeNum = "35P";
			break;

		case "FOURTYPCNTILE":
			storeName = "40thPercentile";
			storeNum = "40P";
			break;
		case "FOURTYFIVEPCNTILE":
			storeName = "45thPercentile";
			storeNum = "45P";
			break;

		case "FIFTYPCNTILE":
			storeName = "50thPercentile";
			storeNum = "50P";
			break;
		case "FIFTYFIVEPCNTILE":
			storeName = "55thPercentile";
			storeNum = "55P";
			break;
		case "SIXTYPCNTILE":
			storeName = "60thPercentile";
			storeNum = "60P";
			break;
		case "SIXTYFIVEPCNTILE":
			storeName = "50thPercentile";
			storeNum = "65P";
			break;
		case "SEVENTYPCNTILE":
			storeName = "70thPercentile";
			storeNum = "70P";
			break;
		case "SEVENTYFIVEPCNTILE":
			storeName = "75thPercentile";
			storeNum = "75P";
			break;
		case "EIGHTYPCNTILE":
			storeName = "80thPercentile";
			storeNum = "80P";
			break;
		case "EIGHTYFIVEPCNTILE":
			storeName = "85thPercentile";
			storeNum = "85P";
			break;
		case "NINETYPCNTILE":
			storeName = "90thPercentile";
			storeNum = "90P";
			break;
		case "NINETYFIVEPCNTILE":
			storeName = "95thPercentile";
			storeNum = "95P";
			break;
		case "AVGPRICE":
			storeName = "AvgPrc";
			storeNum = "AP";
			break;
		case "AVGPRICECMP":
			storeName = "AvgPrcCmp";
			storeNum = "APC";
			break;
		}
		storeInfo = storeName + ";" + storeNum;

		return storeInfo;
	}

	private void ReadmarketFile() throws Exception {
		File myDirectory = new File(rootPath + "/" + relativeInputPath);
		logger.info(myDirectory);
		String[] fileList = myDirectory.list();

		for (String fileName : fileList) {

			processingFile = fileName;
			logger.info("Reading  file : " + fileName);
			String file = myDirectory + "/" + fileName;
			readPromofiles(file);
		}
	}

	private List<NelsonMarketDataDTO> readPromofiles(String filePath) throws Exception {
		List<NelsonMarketDataDTO> marketData = new ArrayList<>();
		CsvReader csvReader = readFile(filePath, ',');

		String UPC = "";
		String line[];
		int counter = 0;
		while (csvReader.readRecord()) {
			String marketName = "";
			if (counter != 0) {
				line = csvReader.getValues();
				NelsonMarketDataDTO mktData = new NelsonMarketDataDTO();
				mktData.setMarketName(line[0]);
				if (line[0].trim().length() == 3) {
					marketName = line[0];
				}
				mktData.setUPC(line[1]);
				UPC = (line[1]);
				mktData.setUPCDesc(line[2]);
				mktData.setTimePeriod(line[3]);
				mktData.setRetailCondn(line[4]);
				mktData.setActUnitsProj(Integer.parseInt(line[5]));
				mktData.setActUnitsProjCompMkt(Integer.parseInt(line[6]));
				mktData.setActSalesProj(Integer.parseInt(line[7]));
				mktData.setActsalesProjCompMkt(Integer.parseInt(line[8]));
				mktData.setAvgPrc(Double.parseDouble(line[9]));
				mktData.setAvglPrcCompMkt(Double.parseDouble(line[10]));
				mktData.setElasticityEstFcsMKt(Double.parseDouble(line[11]));
				mktData.setElasticityEstTotMkt(Double.parseDouble(line[12]));
				mktData.setNf2FcsMkt(Double.parseDouble(line[13]));
				mktData.setNf3FcsMkt(Double.parseDouble(line[14]));
				mktData.setNf4FcsMkt(Double.parseDouble(line[15]));
				mktData.setEstFcsDisMkt(Double.parseDouble(line[16]));
				mktData.setEstAdFcsMkt(Double.parseDouble(line[17]));
				mktData.setEstDisAdFcsMkt(Double.parseDouble(line[18]));
				mktData.setMcpCompMkt(Double.parseDouble(line[19]));
				mktData.setUnitPerMcpCompMkt(Double.parseDouble(line[20]));
				mktData.setFivePercentilePrc(Double.parseDouble(line[21]));
				mktData.setTenPercentilePrc(Double.parseDouble(line[22]));
				mktData.setFifteenPercentilePrc(Double.parseDouble(line[23]));
				mktData.setTwentyPercentilePrc(Double.parseDouble(line[24]));
				mktData.setTwentyFivePercentilePrc(Double.parseDouble(line[25]));
				mktData.setThirtyPercentilePrc(Double.parseDouble(line[26]));
				mktData.setThirtyFivePercentilePrc(Double.parseDouble(line[27]));
				mktData.setFourtyPercentilePrc(Double.parseDouble(line[28]));
				mktData.setFourtyFivePercentilePrc(Double.parseDouble(line[29]));
				mktData.setFiftyPercentilePrc(Double.parseDouble(line[30]));
				mktData.setFiftyfivePercentilePrc(Double.parseDouble(line[31]));
				mktData.setSixtyPercentilePrc(Double.parseDouble(line[32]));
				mktData.setSixtyFivePercentilePrc(Double.parseDouble(line[33]));
				mktData.setSeventyPercentilePrc(Double.parseDouble(line[34]));
				mktData.setSeventyFivePercentilePrc(Double.parseDouble(line[35]));
				mktData.setEightyPercentilePrc(Double.parseDouble(line[36]));
				mktData.setEightyFivePercentilePrc(Double.parseDouble(line[37]));
				mktData.setNinetyPercentilePrc(Double.parseDouble(line[38]));
				mktData.setNinetyFivePercentilePrc(Double.parseDouble(line[39]));

				marketData.add(mktData);

				List<NelsonMarketDataDTO> tempList = new ArrayList<>();
				if (!marketName.equals("")) {
					if (zoneUPcMap.containsKey(marketName + " - " + UPC)) {
						tempList.addAll(zoneUPcMap.get(marketName + " - " + UPC));
					}
					tempList.add(mktData);
					zoneUPcMap.put(marketName + " - " + UPC, tempList);

				}
			}
			counter++;

		}

		csvReader.close();
		logger.info("#Total records read :" + marketData.size());

		logger.info("Total records to be considered for generating CompFile: " + zoneUPcMap.size());
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

	private void writeToCsv(List<CompetitorDTO> compDataList) throws GeneralException, IOException {
		String csvOutputPath = rootPath + "/" + relativeOutputPath + "/" + PriceType + "-"+ processingFile  ;
		logger.info("csvOutputPath" + csvOutputPath);

		try {
			fw = new FileWriter(csvOutputPath);
			pw = new PrintWriter(fw);
			// writeHeader();

			for (CompetitorDTO competitorDTO : compDataList) {

				String separator = PropertyManager.getProperty("SEPARATOR", ",");
				
				pw.print(competitorDTO.getCompetitor_Store_Num() != null ? competitorDTO.getCompetitor_Store_Num()
						: Constants.EMPTY);
				pw.print(separator);
				pw.print(competitorDTO.getPrice_Check_Date() != null ? competitorDTO.getPrice_Check_Date()
						: Constants.EMPTY);
				pw.print(separator);
				pw.print(competitorDTO.getItem_Code() != "" ? competitorDTO.getItem_Code() : Constants.EMPTY);
				pw.print(separator);
				pw.print(competitorDTO.getItem_Description() != null ? competitorDTO.getItem_Description()
						: Constants.EMPTY);
				pw.print(separator);
				pw.print(competitorDTO.getReg_Qty() != 0 ? competitorDTO.getReg_Qty() : 0);
				pw.print(separator);
				pw.print(competitorDTO.getRegular_Retail() != 0 ? competitorDTO.getRegular_Retail() : 0);
				pw.print(separator);
				pw.print(competitorDTO.getSize() != null ? competitorDTO.getSize() : Constants.EMPTY);
				pw.print(separator);
				pw.print(competitorDTO.getUPC() != null ? competitorDTO.getUPC() : Constants.EMPTY);
				pw.print(separator);
				pw.print(competitorDTO.getOutside_Indicator() != null ? competitorDTO.getOutside_Indicator()
						: Constants.EMPTY);
				pw.print(separator);
				pw.print(competitorDTO.getAdditional_Info() != null ? competitorDTO.getAdditional_Info()
						: Constants.EMPTY);
				pw.print(separator);
				pw.print(competitorDTO.getSale_Qty() != 0 ? competitorDTO.getSale_Qty() : 0);
				pw.print(separator);
				pw.print(competitorDTO.getSale_Retail() != 0 ? competitorDTO.getSale_Retail() : 0);
				pw.print(separator);
				pw.print(competitorDTO.getSale_Date() != null ? competitorDTO.getSale_Date() : Constants.EMPTY);
				pw.println();

				fw.flush();
				pw.flush();

			}
			logger.info("#records written are:" + compDataList.size());
		} catch (Exception e) {
			logger.info("writeToCsv() exception" + e);
		}

	}

	private void writeHeader() {

		String separator = PropertyManager.getProperty("SEPARATOR", ",");
		pw.print("COMPETITOR_STORE_NO");
		pw.print(separator);
		/*
		 * pw.print("COMPETITOR_NAME"); pw.print(separator);
		 */
		pw.print("PRICECHECK_DATE");
		pw.print(separator);
		pw.print("ITEMCODE");
		pw.print(separator);
		pw.print("ITEM_DESCRIPTION");
		pw.print(separator);
		pw.print("REGULAR_QTY");
		pw.print(separator);
		pw.print("REGULAR_RETAIL");
		pw.print(separator);
		pw.print("SIZE");
		pw.print(separator);
		pw.print("UPC");
		pw.print(separator);
		pw.print("OUTSIDE_INDICATOR");
		pw.print(separator);
		pw.print("ADDITIONAL_INFO");
		pw.print(separator);
		pw.print("SALE_QTY");
		pw.print(separator);
		pw.print("SALE_RETAIL");
		pw.print(separator);
		pw.print("SALE_DATE");
		pw.println();

	}

}
