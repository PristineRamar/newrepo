package com.pristine.webscrape.dataload.fairway;

import java.io.File;
import java.io.FileReader;
import java.io.FileWriter;
import java.io.IOException;
import java.sql.Connection;
import java.text.DateFormat;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;

import org.apache.log4j.Logger;
import org.apache.log4j.PropertyConfigurator;
import org.supercsv.cellprocessor.constraint.NotNull;
import org.supercsv.cellprocessor.ift.CellProcessor;
import org.supercsv.io.CsvBeanWriter;
import org.supercsv.io.ICsvBeanWriter;
import org.supercsv.prefs.CsvPreference;

import com.csvreader.CsvReader;
import com.pristine.dao.CompetitiveDataDAO;
import com.pristine.dao.DBManager;
import com.pristine.dto.CompetitiveDataDTO;
import com.pristine.exception.GeneralException;
import com.pristine.service.offermgmt.PricingEngineService;
import com.pristine.util.Constants;
import com.pristine.util.DateUtil;
import com.pristine.util.PropertyManager;

public class ShopriteExcelExport {

	private static Logger logger = Logger.getLogger("ExportCompData");
	private static String FILE_PATH = "FILE_PATH=";
	private static String relativeInputPath;
	private final static String OUTPUT_FOLDER = "OUTPUT_FOLDER=";
	DateFormat appDateFormatter = new SimpleDateFormat(Constants.APP_DATE_FORMAT);
	private static String startDate = "";
	public String storeNumber = "";
	public String storeName = "";
	static String outFolder = null;
	Connection conn = null;
	PricingEngineService pricingEngineService = new PricingEngineService();
	String rootPath = PropertyManager.getProperty("DATALOAD.ROOTPATH", "");
	ICsvBeanWriter beanWriter;
	List<CompetitiveDataDTO> ProcessedList = new ArrayList<CompetitiveDataDTO>();
	HashMap<String, List<CompetitiveDataDTO>> compDataMap = new HashMap<String, List<CompetitiveDataDTO>>();
	public ShopriteExcelExport() {

		try {
			conn = DBManager.getConnection();
		} catch (GeneralException var1_1) {
			logger.error("Error when creating connection - " + var1_1);
		}
	}

	public static void main(String args[]) throws GeneralException, Exception {

		PropertyConfigurator.configure("log4j-exportRawCompData.properties");
		PropertyManager.initialize("analysis.properties");

		ShopriteExcelExport exportCompdata = new ShopriteExcelExport();

		for (String arg : args) {
			if (arg.startsWith(FILE_PATH)) {
				relativeInputPath = arg.substring(FILE_PATH.length());
			}
			if (arg.startsWith(OUTPUT_FOLDER)) {
				outFolder = arg.substring(OUTPUT_FOLDER.length());
			}
		}
		logger.info("ProcessCompData started.......");
		exportCompdata.processCompData();
		logger.info("ProcessCompData completed......");
	}

	private void processCompData() throws GeneralException, Exception {

		File myDirectory = new File(rootPath + "/" + relativeInputPath);
		String[] fileList = myDirectory.list();

		for (String fileName : fileList) {

			String csvOutputFolder = PropertyManager.getProperty("CSV_OUTPUT_FOLDER");
			boolean writeHeader = true;
			logger.info("Reading  file" + fileName);
			String file = myDirectory + "/" + fileName;
			List<CompetitiveDataDTO> shopRiteItems = readShopRiteInputFile(file);

			logger.info("Reading  file " + fileName + "Complete" + "total items read :" + shopRiteItems.size());

			// get startDate and storeNo
			CompetitiveDataDTO firstItem = shopRiteItems.get(0);
			startDate = firstItem.checkDate;
			storeNumber = "'" + firstItem.storeNo + "'";
			storeName = PropertyManager.getProperty(storeNumber);

			// Convert current items list to hashmap
			HashMap<String, CompetitiveDataDTO> currentItemMap = new HashMap<String, CompetitiveDataDTO>();
			int count = 0;
			for (CompetitiveDataDTO items : shopRiteItems) {
				items.storeName = storeName;

				if (items.saleMPack != 0) {
					if (currentItemMap.containsKey(items.itemCodeNo))
						count++;
					else
						currentItemMap.put(items.itemCodeNo, items);
				} else {
					ProcessedList.add(items);
				}
			}

			logger.info("Duplicate items count:" + count);
			logger.info("Total items  added in currentItemMap :" + currentItemMap.size());

			CompetitiveDataDAO competitiveDataDAO = new CompetitiveDataDAO(conn);

			// get last 12th week date
			String twelveWeeksBeforestartDate = DateUtil.getWeekStartDate(DateUtil.toDate(startDate), 12);

			logger.info("Get Raw Competitive data for last 12  weeks:");
			HashMap<String, List<CompetitiveDataDTO>> rawCompDataMap = competitiveDataDAO
					.getRawCompetitiveData(twelveWeeksBeforestartDate, storeNumber);

			// code for getting Shoprite prices
			setPrice(currentItemMap, rawCompDataMap, startDate);

			// write to CSv file
			String csvOutputPath = rootPath + "/" + csvOutputFolder + "/" + storeName + ".csv";
			try {
				beanWriter = new CsvBeanWriter(new FileWriter(csvOutputPath), CsvPreference.STANDARD_PREFERENCE);
				writeCsvFile(ProcessedList, beanWriter, writeHeader);
				writeHeader = false;

			} catch (Exception e) {
				logger.error("CsvBeanWriter() - Exception while writing ...", e);
			}

			if (beanWriter != null) {
				try {
					beanWriter.close();
				} catch (IOException e) {
					logger.error("scrape() - IOException", e);
				}
			}
		}
	}

	private void setPrice(HashMap<String, CompetitiveDataDTO> currentItemMap,
			HashMap<String, List<CompetitiveDataDTO>> rawCompDataMap, String Startdate) {

		HashMap<String, List<CompetitiveDataDTO>> compDataMap = getHistory(rawCompDataMap);

		try {

			for (Entry<String, CompetitiveDataDTO> currentItem : currentItemMap.entrySet()) {

				String currentItemcode = currentItem.getKey();
				CompetitiveDataDTO currentItemValue = currentItem.getValue();
				HashSet<Float> regPfornosale = new HashSet<Float>();
				HashSet<Float> regPforsale = new HashSet<Float>();

				if (compDataMap.containsKey(currentItemcode)) {

					List<CompetitiveDataDTO> historyItems = compDataMap.get(currentItemcode);
					for (CompetitiveDataDTO compData : historyItems) {
						System.out.println(compData.weekStartDate);
						if (compData.fSalePrice != 0) {
							regPforsale.add(compData.regPrice);

						} else {
							regPfornosale.add(compData.regPrice);
						}

					}

					if (regPforsale.size() > 0 && regPfornosale.size() == 0) {

						float maxRegprice = Collections.max(regPforsale);

						if (maxRegprice > currentItemValue.regPrice)
							currentItemValue.regPrice = maxRegprice;

						logger.info("Case 4: item has only salehistory: " + currentItemValue.itemCodeNo);
						ProcessedList.add(currentItemValue);

					} else {

						float val = Collections.max(regPfornosale);

						if (currentItemValue.regMPrice == val) {
							logger.info("Case 1: RegPrice same as history for: " + currentItemValue.itemCodeNo);
							ProcessedList.add(currentItemValue);
						} else if (currentItemValue.regMPrice < val) {
							logger.info("Case 2: LoyaltyPrice for : " + currentItemValue.itemCodeNo);
							currentItemValue.LoyaltyPrice = currentItemValue.fSalePrice;
							currentItemValue.LoyaltyQuantity = currentItemValue.saleMPack;
							currentItemValue.fSalePrice = currentItemValue.regMPrice;
							currentItemValue.saleMPack = currentItemValue.regMPack;
							currentItemValue.regMPrice = val;
							ProcessedList.add(currentItemValue);
						} else if (currentItemValue.regMPrice > val) {
							logger.info(
									"Case 3: CurrentRegP greater than previous RegP : " + currentItemValue.itemCodeNo);
							currentItemValue.regMPrice = val;
							ProcessedList.add(currentItemValue);
						}

					}

				} else {
					logger.info("Case 5: No history found");
					ProcessedList.add(currentItemValue);
				}
			}

		} catch (Exception e) {
			logger.info("Exception", e);
		}

	}

	public HashMap<String, List<CompetitiveDataDTO>> getHistory(
			HashMap<String, List<CompetitiveDataDTO>> rawCompDataMap) {

		logger.info("getHistoryValues()- Getting  history items in list from hashmap");

		for (Map.Entry<String, List<CompetitiveDataDTO>> historyitems : rawCompDataMap.entrySet()) {

			for (CompetitiveDataDTO compData : historyitems.getValue()) {

				List<CompetitiveDataDTO> tempList = new ArrayList<CompetitiveDataDTO>();
				if (compDataMap.containsKey(compData.itemCodeNo)) {
					tempList = compDataMap.get(compData.itemCodeNo);
				}

				tempList.add(compData);
				compDataMap.put(compData.itemCodeNo, tempList);
			}

		}

		logger.info("getHistoryValues()- Total items added" + compDataMap.size());
		return compDataMap;
	}

	private List<CompetitiveDataDTO> readShopRiteInputFile(String filePath) throws Exception {
		List<CompetitiveDataDTO> shopriteItem = new ArrayList<>();
		CsvReader csvReader = readFile(filePath, ',');

		String line[];
		int counter = 0;
		while (csvReader.readRecord()) {
			if (counter != 0) {

				line = csvReader.getValues();

				CompetitiveDataDTO competitiveData = new CompetitiveDataDTO();
				competitiveData.storeNo = line[0];

				competitiveData.checkDate = line[1];
				competitiveData.itemCodeNo = line[2];
				competitiveData.itemName = line[3];
				competitiveData.regMPack = Integer.parseInt(line[4]);
				competitiveData.regMPrice = Float.parseFloat(line[5]);
				competitiveData.size = line[6];
				competitiveData.upcwithcheck = (line[7]);
				competitiveData.upcwithoutcheck = GetUPCwithoutchkdigit(competitiveData.upcwithcheck);
				competitiveData.saleMPack = Integer.parseInt(line[8]);
				competitiveData.fSalePrice = Float.parseFloat(line[9]);
				competitiveData.categoryName = line[10];
				competitiveData.compItemAddlDesc = line[11];

				if (competitiveData.compItemAddlDesc != "") {
					String AddInfo = competitiveData.compItemAddlDesc;
					AddInfo = AddInfo.replaceAll("\\s", "");
					int length = AddInfo.length();
					int s = AddInfo.indexOf("from");
					int t = AddInfo.indexOf("until");
					competitiveData.effSaleStartDate = AddInfo.substring(s + 4, t);
					competitiveData.effSaleEndDate = AddInfo.substring(t + 5, length);

				}

				shopriteItem.add(competitiveData);
			}
			counter++;

		}

		csvReader.close();
		return shopriteItem;

	}

	public String GetUPCwithoutchkdigit(String uPC) {
		uPC = uPC.substring(0, uPC.length() - 1);
		return uPC;
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

	public void writeCsvFile(List<CompetitiveDataDTO> itemDTOs, ICsvBeanWriter beanWriter, boolean writeHeader)
			throws Exception {
		String[] header = new String[] { "Store Name", "Store Number", "Check Date", "Competitor's Item Code",
				"Item Desc", "Reg Qty", "Reg Price", "Effective Reg Price Date", "Sale Qty", "Sale Price",
				"Loyalty Qty", " Loyalty Price", "Category", "Sale Start Date", "Sale End Date", "Size",
				"UPC With CHK Digit", "UPC Without CHK Digit" };

		String[] columnsToBeAdded = new String[] { "StoreName", "storeNo", "checkDate", "ItemCodeNo", "itemName",
				"regMPack", "regMPrice", "EffectiveRegPriceDate", "saleMPack", "fSalePrice", "LoyaltyQuantity",
				"LoyaltyPrice", "categoryName", "effSaleStartDate", "effSaleEndDate", "Size", "upcwithcheck",
				"upcwithoutcheck" };

		CellProcessor[] processors = new CellProcessor[] { new NotNull(), new NotNull(), new NotNull(), null,
				new NotNull(), new NotNull(), new NotNull(), null, null, null, null, null, new NotNull(), null, null,
				new NotNull(), null, null };

		writeCSVData(itemDTOs, columnsToBeAdded, processors, header, beanWriter, writeHeader);

	}

	protected void writeCSVData(List<CompetitiveDataDTO> itemDTOs, String[] columns, CellProcessor[] processors,
			String[] header, ICsvBeanWriter beanWriter, boolean writeHeader) throws Exception {
		if (writeHeader) {
			beanWriter.writeHeader(header);
		}
		for (CompetitiveDataDTO itemDTO : itemDTOs) {
			if (itemDTO.getRegMPrice() != 0) {
				beanWriter.write(itemDTO, columns, processors);
			}
		}

		logger.info("Write Complete");
	}

}
