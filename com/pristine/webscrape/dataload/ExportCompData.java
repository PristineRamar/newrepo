package com.pristine.webscrape.dataload;

import java.io.File;
import java.io.FileWriter;
import java.io.IOException;
import java.io.PrintWriter;
import java.sql.Connection;
import java.text.DateFormat;
import java.text.DecimalFormat;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.time.LocalDate;
import java.util.ArrayList;
import java.util.Calendar;
import java.util.Date;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;

import org.apache.log4j.Logger;
import org.apache.log4j.PropertyConfigurator;

import com.pristine.dao.CompetitiveDataDAO;
import com.pristine.dao.DBManager;
import com.pristine.dto.CompetitiveDataDTO;
import com.pristine.dto.CompStoreItemDetailsKey;
import com.pristine.exception.GeneralException;
import com.pristine.service.offermgmt.PricingEngineService;
import com.pristine.util.Constants;
import com.pristine.util.DateUtil;
import com.pristine.util.PropertyManager;

public class ExportCompData {
	private static Logger logger = Logger.getLogger("ExportCompData");
	private final static String OUTPUT_FOLDER = "OUTPUT_FOLDER=";
	DateFormat appDateFormatter = new SimpleDateFormat(Constants.APP_DATE_FORMAT);
	private static String storeName;
	private static String startDate = null;
	Connection conn = null;
	PricingEngineService pricingEngineService = new PricingEngineService();
	private FileWriter fw = null;
	private PrintWriter pw = null;
	String rootPath = PropertyManager.getProperty("DATALOAD.ROOTPATH", "");
	static String outFolder = null;
	
	public ExportCompData(){
		 try {
	            conn = DBManager.getConnection();
	        }
	        catch (GeneralException var1_1) {
	        	logger.error("Error when creating connection - " + var1_1);
	        }
	}

	public static void main(String[] args) throws ParseException {
		
		PropertyManager.initialize("analysis.properties");
		PropertyConfigurator.configure("log4j-exportRawCompData.properties");
		
		for (String arg : args) {
			if (arg.startsWith(OUTPUT_FOLDER)) {
				outFolder = arg.substring(OUTPUT_FOLDER.length());
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
		if(args.length>1){
		if (Constants.CURRENT_WEEK.equalsIgnoreCase(args[1])) {
			startDate = dateFormat.format(c.getTime());
		} else if (Constants.NEXT_WEEK.equalsIgnoreCase(args[1])) {
			c.add(Calendar.DATE, 7);
			startDate = dateFormat.format(c.getTime());
		} else if (Constants.LAST_WEEK.equalsIgnoreCase(args[1])) {
			c.add(Calendar.DATE, -7);
			startDate = dateFormat.format(c.getTime());
		} else if (Constants.SPECIFIC_WEEK.equalsIgnoreCase(args[1])) {
			try {
				String tempStartDate = DateUtil.getWeekStartDate(DateUtil.toDate(args[2]), 0);
				Date date = dateFormat.parse(tempStartDate);
				startDate = dateFormat.format(date);
				logger.info("Date:"+startDate);
			} catch (GeneralException exception){
				logger.error("Error when parsing date - " + exception.toString());
				System.exit(-1);
			}
		}
		}
		logger.info("********************************************");
		ExportCompData exportCompData = new ExportCompData();
		try {
			try {
				exportCompData.processCompData();
			} catch (IOException e) {
				e.printStackTrace();
			}
		} catch (GeneralException e) {
			logger.error("Error in the exportCompData programs", e);
			e.printStackTrace();
		}
		logger.info("********************************************");
	}
	
	private void processCompData() throws GeneralException, IOException{
		String date = startDate.replace("/", "");
		String outputPath = null;
		String storeNumbers = null;
		if(storeName.equals("WEGMANS")){
			outputPath = rootPath + "/" + outFolder + "/"
					+ "Wegmans-web-scraping"+date+".txt";
			storeNumbers = PropertyManager.getProperty("WEGMANS_STORE_LIST");
		}else if(storeName.equals("HANNAFORD")){
			outputPath = rootPath + "/" + outFolder + "/"
					+ "Hannaford-web-scraping"+date+".txt";
			storeNumbers = PropertyManager.getProperty("HANNAFORDS_STORE_LIST");
		}
		File file = new File(outputPath);
		if (!file.exists())
			fw = new FileWriter(outputPath);
		else
			fw = new FileWriter(outputPath, true);
		pw = new PrintWriter(fw);

		CompetitiveDataDAO cometitiveDataDAO = new CompetitiveDataDAO(conn);
		
		//Get current and last 3 weeks data from DB
		String fourWeekBeforeStartDate = DateUtil.getWeekStartDate(DateUtil.toDate(startDate), 4);
		logger.info("Get Raw Competitive data for last 4 weeks");
		HashMap<String, List<CompetitiveDataDTO>> rawCompDataMap = cometitiveDataDAO.getRawCompetitiveData(fourWeekBeforeStartDate,storeNumbers);
		HashMap<String, CompetitiveDataDTO> storeDetails =cometitiveDataDAO.getCompStoreInfo();
		//Group current week latest items using check date and create a key using item,upc.
		logger.info("Processing the list to get Current items based on the Latest Date for each items");
		HashMap<CompStoreItemDetailsKey, CompetitiveDataDTO> currentWeekItemMap = getCurrentWeekItemMap(rawCompDataMap.get(startDate));
		generateCompDatafile(currentWeekItemMap, storeDetails);
		logger.info("Updating is exported indicator");
		cometitiveDataDAO.updateExportInd(currentWeekItemMap.keySet(), startDate);
		pw.flush();
		fw.flush();
		pw.close();
		fw.close();
	}
	
	/**
	 * To get current week items based on the latest date available 
	 * @param rawCompDataMap
	 * @return
	 */
	private HashMap<CompStoreItemDetailsKey, CompetitiveDataDTO> getCurrentWeekItemMap(List<CompetitiveDataDTO> rawCompDataMap){
		HashMap<CompStoreItemDetailsKey, CompetitiveDataDTO> currentWeekItemMap = new HashMap<CompStoreItemDetailsKey, CompetitiveDataDTO>();
		//Sort list to get items based on most recent dates
		pricingEngineService.sortByCheckDate(rawCompDataMap);
		for(CompetitiveDataDTO competitiveDataDTO: rawCompDataMap){
			CompStoreItemDetailsKey key = new CompStoreItemDetailsKey(competitiveDataDTO.compStrNo,competitiveDataDTO.upc, String.valueOf(competitiveDataDTO.itemcode));
			//Consider only Items which latest check date
			if(!(currentWeekItemMap.containsKey(key))){
				currentWeekItemMap.put(key, competitiveDataDTO);
			}
		}
		
		return currentWeekItemMap;
	}
	
	/**
	 * To generate Competitor Store details 
	 * @param currentWeekItemMap
	 * @param storeDetails
	 * @throws GeneralException
	 */
	private void generateCompDatafile(HashMap<CompStoreItemDetailsKey, CompetitiveDataDTO> currentWeekItemMap, 
			HashMap<String, CompetitiveDataDTO> storeDetails) throws GeneralException{
		 String separator = " ";
		HashMap<String, List<CompetitiveDataDTO>> compStoreDetails = new HashMap<String, List<CompetitiveDataDTO>>();
		//Group by Comp store number and item details
		for(Map.Entry<CompStoreItemDetailsKey, CompetitiveDataDTO> entry: currentWeekItemMap.entrySet()){
			String compStoreNo = entry.getKey().getCompStrNo();
			List<CompetitiveDataDTO> storeDetailsList = new ArrayList<CompetitiveDataDTO>();
			if(compStoreDetails.containsKey(compStoreNo)){
				storeDetailsList.addAll(compStoreDetails.get(compStoreNo));
			}
			storeDetailsList.add(entry.getValue());
			compStoreDetails.put(compStoreNo, storeDetailsList);
		}
		
		for(Map.Entry<String, List<CompetitiveDataDTO>> itemDetailsBasedOnStore: compStoreDetails.entrySet()){
			
			writeHeader(itemDetailsBasedOnStore.getKey(),storeDetails);

			List<CompetitiveDataDTO> compStoreDetail = itemDetailsBasedOnStore.getValue();

			for(CompetitiveDataDTO competitiveDataDTO: compStoreDetail){
				pw.print(String.format("%14s", competitiveDataDTO.upc).replace(' ', '0').toUpperCase()); //UPC(14 char)
				pw.print(separator);
				pw.print(new DecimalFormat("000").format(competitiveDataDTO.regMPack));// Reg Unit(3 char)
				pw.print(separator);
				pw.print(new DecimalFormat("000.00").format(competitiveDataDTO.regPrice));//Reg price (6 char)
				pw.print(separator);
				pw.print("W");//Sale indicator(1 char)
				pw.print("     ");
				pw.print(DateUtil.dateToString(DateUtil.toDate(competitiveDataDTO.checkDate), "MMddYY"));//Check date(6 char)
				pw.print(separator);
				pw.print(new DecimalFormat("000").format(competitiveDataDTO.saleMPack));//Sale qty(3 char)
				pw.print(separator);
				pw.print(new DecimalFormat("000.00").format(competitiveDataDTO.fSalePrice));//Sale Price (6 char)
				pw.print(separator);
				pw.print("V");//Sale ind(1 char)
				pw.print("     ");
				pw.print("   ");//RDS3-Sale qty
				pw.print(separator);
				pw.print("      ");//RDS3-Sale price
				pw.print(separator);
				pw.print(" ");//RDS3-Sale Ind
				pw.print("     ");
				pw.print(fillWhiteSpace(competitiveDataDTO.itemName, 30).toUpperCase());//Item Desc(30 Char)
				pw.print(separator);
				pw.print(fillWhiteSpace(competitiveDataDTO.size,11).toUpperCase());//Item Size
				pw.println(separator);
			}
			
		}
	}
	
	private String fillWhiteSpace(String inputString, int noOfChar){
		if(inputString.length()>noOfChar){
			inputString = inputString.substring(0, noOfChar);
		}else if(inputString.length()<noOfChar){
			for(int i = inputString.length();i < noOfChar;i++){
				inputString = inputString+" ";
			}
		}
		return inputString;
	}
	
	private void writeHeader(String compStrNo, HashMap<String, CompetitiveDataDTO> storeDetails) throws GeneralException{
		if(storeDetails.containsKey(compStrNo)){
			CompetitiveDataDTO competitiveDataDTO2 = storeDetails.get(compStrNo);
			pw.print("H");
			pw.print(startDate.replace("/", ""));
			pw.print((competitiveDataDTO2.storeNo).toUpperCase());
			pw.println(fillWhiteSpace(competitiveDataDTO2.storeAddr, 80).toUpperCase());
		}else{
			logger.error("Store details is not found for Comp store no: "+compStrNo);
			throw new GeneralException("Store details is not found");
		}
	}
}
