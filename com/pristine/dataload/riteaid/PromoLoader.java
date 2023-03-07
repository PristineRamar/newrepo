package com.pristine.dataload.riteaid;

import java.io.File;
import java.io.FileReader;
import java.sql.Connection;
import java.text.DateFormat;
import java.text.SimpleDateFormat;
import java.time.LocalDate;
import java.util.ArrayList;
import java.util.Date;
import java.util.List;
import java.util.Map;
import java.util.TreeMap;

import org.apache.log4j.Logger;
import org.apache.log4j.PropertyConfigurator;

import com.csvreader.CsvReader;
import com.pristine.dao.DBManager;
import com.pristine.dao.ItemDAO;
import com.pristine.dataload.AdAndPromoSetup;
import com.pristine.dto.ItemDTO;
import com.pristine.dto.PromoDataStandardDTO;
import com.pristine.exception.GeneralException;
import com.pristine.service.offermgmt.PricingEngineService;
import com.pristine.util.Constants;
import com.pristine.util.DateUtil;
import com.pristine.util.PristineDBUtil;
import com.pristine.util.PropertyManager;
import com.pristine.util.offermgmt.PRCommonUtil;

public class PromoLoader {

	private static Logger logger = Logger.getLogger("PromoLoader");
	private static String FILE_PATH = "FILE_PATH=";
	private static String relativeInputPath;

	DateFormat appDateFormatter = new SimpleDateFormat(Constants.APP_DATE_FORMAT);
	Connection conn = null;
	PricingEngineService pricingEngineService = new PricingEngineService();
	String rootPath = PropertyManager.getProperty("DATALOAD.ROOTPATH", "");
	private final static String MODE = "MODE=";
	private final static String DATE = "DATE=";
	private static boolean isDeltaMode = false;
	private static String dateToCompare = "";
	List<ItemDTO> activeItems = null;
	List<String> ignoredItem = null;
	String fnName = "";
	private static String week;

	TreeMap<String, List<PromoDataStandardDTO>> itemsList = new TreeMap<>();

	public PromoLoader() {

		try {
			conn = DBManager.getConnection();
		} catch (GeneralException var1_1) {
			logger.error("Error when creating connection - " + var1_1);
		}
	}

	public static void main(String args[]) throws GeneralException, Exception {

		PropertyConfigurator.configure("log4j-PromoLoader.properties");
		PropertyManager.initialize("analysis.properties");

		PromoLoader promoLoader = new PromoLoader();

		for (String arg : args) {
			if (arg.startsWith(FILE_PATH)) {
				relativeInputPath = arg.substring(FILE_PATH.length());
			}
			if (arg.startsWith(MODE)) {
				logger.info("Mode=" + arg.substring(MODE.length()));
				isDeltaMode = arg.substring(MODE.length()).equalsIgnoreCase("DELTA");
			}
			if (arg.startsWith(DATE)) {
				logger.info("DATE=" + arg.substring(DATE.length()));
				dateToCompare = arg.substring(DATE.length());
			}
		}

		promoLoader.initializeData();

		logger.info("ProcessCompData started.......");

		promoLoader.processCompData();

		logger.info("ProcessCompData completed......");
	}

	@SuppressWarnings("static-access")
	private void processCompData() throws Exception, GeneralException {
		File myDirectory = new File(rootPath + "/" + relativeInputPath);
		String[] fileList = myDirectory.list();

		for (String fileName : fileList) {

			logger.info("Reading  file" + fileName);
			String file = myDirectory + "/" + fileName;
			List<PromoDataStandardDTO> promoItems = readPromofiles(file);
			fnName = fileName;
			logger.info("Reading  file " + fileName + "Complete");
			logger.info("# items read :" + promoItems.size());
			SimpleDateFormat formatter = new SimpleDateFormat(Constants.APP_DATE_FORMAT);
			Date date = new Date();
			DateUtil dt = new DateUtil();

			String currentDate = "";
			if (!dateToCompare.contentEquals(Constants.EMPTY)) {
				currentDate = dateToCompare;
			} else {
				currentDate = formatter.format(date);
			}

			String da = DateUtil.getWeekStartDate(DateUtil.toDate(currentDate), 0);
			logger.info("date tocompare"+ da);

			Date ctDate = dt.toDate(da);
			for (PromoDataStandardDTO promoItem : promoItems) {

				if (promoItem.getPromoGroup().equals("null")) {
					logger.debug(promoItem.getPromoID() + " is ignored as no groupId");
					ignoredItem.add(promoItem.getPromoID());
				} else {
					Date promoStartDate = dt.toDate(promoItem.getPromoStartDate());
					List<PromoDataStandardDTO> temp = new ArrayList<>();
					if (promoStartDate.compareTo(ctDate) >= 0) {
						if (itemsList.containsKey(promoItem.getPromoStartDate()))

						{
							temp.addAll(itemsList.get(promoItem.getPromoStartDate()));
						}

						temp.add(promoItem);
						itemsList.put(promoItem.getPromoStartDate(), temp);
						// finalListtoProcess.add(promoItem);
					} else {
						logger.debug("Ignoring since past Promo" + promoItem.getPromoStartDate() + ";"
								+ promoItem.getPromoID());
					}

				}
			}

		}
		
		/*
		 * Set<String> promoList = new HashSet<String>(); for (Map.Entry<String,
		 * List<PromoDataStandardDTO>> entry : itemsList.entrySet()) {
		 * promoList.add(entry.getKey()) ;
		 * 
		 * }
		 * 
		 * 
		 * logger.debug("UniqueDates"); for (String date : promoList) {
		 * logger.debug(date); }
		 */
		logger.info("ItemList size"+ itemsList.size());
		if (itemsList.size() > 0)

		{
			

			for (Map.Entry<String, List<PromoDataStandardDTO>> entry : itemsList.entrySet()) {
				List<PromoDataStandardDTO> finalListtoProcess = new ArrayList<>();
				logger.info("Processing for:" + entry.getKey());
				finalListtoProcess = entry.getValue();
				LocalDate minDate = finalListtoProcess.stream().map(PromoDataStandardDTO::getPromoStart)
						.min(LocalDate::compareTo).get();
				week = PRCommonUtil.getDateFormatter().format(minDate);
				callPromoLoader(finalListtoProcess);
				logger.info("Processing Complete :" + entry.getKey());

			}

		} else {
			logger.info(" File " + fnName + "does not have FuturePromo");
		}

	}

	private void initializeData() throws GeneralException {
		ignoredItem = new ArrayList<>();
		try {

			ItemDAO itemDAO = new ItemDAO();

			logger.info("initializeData() - Getting all items started...");
			activeItems = itemDAO.getAllActiveItems(conn);
			logger.info("initializeData() -	Active items" + activeItems.size());

		} catch (GeneralException | Exception e) {
			throw new GeneralException("initializeData() - Error while initializing cache", e);
		}
	}

	private void callPromoLoader(List<PromoDataStandardDTO> temp) throws GeneralException {

		try {
			

			
			  AdAndPromoSetup promoSetup = new AdAndPromoSetup(conn, activeItems,
			  isDeltaMode); logger.info("processFile() - Setting up promotion data...");
			  promoSetup.setupPromoData(temp, week);
			  logger.info("processFile() - Setting up promotion data is completed.");
			  PristineDBUtil.commitTransaction(conn, "batch record update");
			 

		} catch (Exception ex) {

			logger.error("processFile()-Exception while processing file  for :  " + week);
			logger.error(ex);
			PristineDBUtil.rollbackTransaction(conn, "Unexpected Exception");
		}
	}

	private List<PromoDataStandardDTO> readPromofiles(String filePath) throws Exception {
		List<PromoDataStandardDTO> promoItem = new ArrayList<>();
		CsvReader csvReader = readFile(filePath, ',');

		String line[];
		int counter = 0;
		while (csvReader.readRecord()) {
			if (counter != 0) {

				line = csvReader.getValues();

				PromoDataStandardDTO promoData = new PromoDataStandardDTO();
				promoData.setCategory(line[0]);

				promoData.setPromoStartDate(line[1]);
				promoData.setPromoEndDate(line[2]);
				promoData.setPromoID(line[3]);
				promoData.setPromoDescription(line[4]);
				promoData.setItemCode(line[5]);
				promoData.setUpc(line[6]);
				promoData.setItemName(line[7]);

				promoData.setLirName(line[8]);
				promoData.setPromoGroup(line[9]);
				promoData.setEverdayQty(line[10]);
				promoData.setEverydayPrice(line[11]);

				promoData.setSaleQty(Integer.parseInt(line[12]));
				promoData.setSalePrice(Double.parseDouble(line[13]));
				promoData.setMustBuyQty(Integer.parseInt(line[14]));
				promoData.setMustbuyPrice(Double.parseDouble(line[15]));
				promoData.setDollarOff(Double.parseDouble(line[16]));
				promoData.setPctOff(Double.parseDouble(line[17]));
				promoData.setBuyQty(Integer.parseInt(line[18]));
				promoData.setGetQty(Integer.parseInt(line[19]));
				promoData.setMinimumQty(Integer.parseInt(line[20]));
				promoData.setMinimumAmt(Double.parseDouble(line[21]));
				promoData.setBmsmDollaroffperunits(Double.parseDouble(line[22]));
				promoData.setBmsmPctoffperunit(Double.parseDouble(line[23]));
				promoData.setBmsmsaleQty(Integer.parseInt(line[24]));
				promoData.setBmsmsalePrice(Double.parseDouble(line[25]));
				promoData.setStatus(line[26]);
				promoData.setLocationLevel(line[27]);
				promoData.setLocationNo(line[28]);
				promoData.setPageNumber(line[29]);
				promoData.setBlockNumber(line[30]);
				promoData.setDisplayOffer(line[31]);
				promoData.setDescription(line[32]);
				promoData.setCouponType(line[33]);
				promoData.setCouponAmt(Double.parseDouble(line[34]));
				promoData.setTypeCode(line[35]);
				promoData.setAnotherItem(line[36]);

				promoItem.add(promoData);
			}
			counter++;

		}

		csvReader.close();
		return promoItem;
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

}
