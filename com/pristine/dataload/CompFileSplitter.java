package com.pristine.dataload;

import java.io.File;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.nio.file.StandardOpenOption;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Date;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Scanner;

import org.apache.log4j.Logger;
import org.apache.log4j.PropertyConfigurator;

import com.pristine.util.Constants;
import com.pristine.util.DateUtil;
import com.pristine.util.PropertyManager;

public class CompFileSplitter {

	private final static String INPUT_FOLDER = "INPUT_FOLDER=";
	private final static String FILE = "FILE=";
	private static String inputFilePath;
	private static String inputFile;
	private static Logger logger = Logger.getLogger("CompFileSplitter");
	public static void main(String args[]) {
		CompFileSplitter compFileSplitter = new CompFileSplitter();
		PropertyManager.initialize("analysis.properties");
		PropertyConfigurator.configure("log4j-comp-file-splitter.properties");
		// Read input arguments
		for (String arg : args) {
			if (arg.startsWith(INPUT_FOLDER)) {
				inputFilePath = arg.substring(INPUT_FOLDER.length());
			}
			if (arg.startsWith(FILE)) {
				inputFile = arg.substring(FILE.length());
			}
		}
		compFileSplitter.splitFileByDate();
		
		logger.info("Program successfully completed");
	}

	private void splitFileByDate() {
		String line = null;
		int errorCounter = 0;
		try {
			HashMap<String, String> chainLookup = new HashMap<>();
			chainLookup.put("ADV", "44");
			chainLookup.put("AMAZ", "102");
			chainLookup.put("CRID", "105");
			chainLookup.put("JEGS", "103");
			chainLookup.put("NAPA", "46");
			chainLookup.put("ORLY", "84");
			chainLookup.put("PEP", "55");
			chainLookup.put("PTGK", "106");
			chainLookup.put("ROCK", "101");
			chainLookup.put("SUMT", "104");
			chainLookup.put("USAU", "107");
			chainLookup.put("WLMT", "95");
			SimpleDateFormat dateFormat = new SimpleDateFormat(Constants.APP_DATE_FORMAT);
			String date = "01/01/2017";
			Date dateCap = dateFormat.parse(date);
			String header = "competitor_store|price_check_date|item_code|item_description|regular_quantity"
					+ "|regular_retail|item_size|upc|outside_indicator|additional_info|sale_quantity|sale_retail"
					+ "|sale_date|part_number|category_desc|sub_category_desc|sub_ctgy2_ds|user_defn_fld_ds|ctgy_ds"
					+ "|app_yr_nb|app_make_nm|app_mdl_nm|app_sub_mdl_nm|app_engn_ds|app_othr_ds|ctgy_fltr_ds"
					+ "|contains_fltr1_ds|contains_fltr2_ds|brand_fltr_ds|part_nb_ds|warr_ds|on_ln_avl_tx|store_post_cd"
					+ "|core_am|shop_type_cd|asked_qstn_lvl1_tx|asked_qstn_lvl2_tx|asked_qstn_lvl3_tx|asked_qstn_lvl4_tx"
					+ "|spec1_ds|spec2_ds|spec3_ds|spec4_ds|spec5_ds|spec6_ds|spec7_ds|spec8_ds|spec9_ds|spec10_ds"
					+ "|spec11_ds|spec12_ds|spec13_ds|spec14_ds|spec15_ds|spec16_ds|spec17_ds|spec18_ds|spec19_ds"
					+ "|spec20_ds" + System.lineSeparator();
			HashMap<String, List<String>> fileContent = new HashMap<>(); 
			
			try (Scanner scanner = new Scanner(new File(inputFilePath + "/" + inputFile),"UTF-8")) {
				int lineCounter = 0;
				while (scanner.hasNextLine()) {
					try {
						lineCounter++;
						errorCounter = lineCounter;
						if (lineCounter == 1) {
							line = scanner.nextLine();
							continue;
						}
						
						line = scanner.nextLine();
						String lineArr[] = line.split("\\|");

						String storeNo = lineArr[0];
						
						String storeNoArr[] = storeNo.split("-");
						
						String chainName = storeNoArr[0];
						
						String chainId = chainLookup.containsKey(chainName) ? chainLookup.get(chainName) : chainName; 
						
						StringBuilder newStrNo = new StringBuilder(chainId);
						if(storeNoArr.length > 1) {
							String strNo = storeNoArr[1].trim();
							if(strNo.equals("")) {
								newStrNo.append("-0000");
							} else if(strNo.equals("00")) {
								newStrNo.append("-0000");
							} else {
								newStrNo.append("-").append(strNo);
							}
						}
						
						StringBuilder transformedLine = new StringBuilder();
						for (int i = 0; i < lineArr.length; i++) {
							String val = i == 0 ? newStrNo.toString() : lineArr[i];
							transformedLine.append(val);
							if (lineArr.length - 1 != i) {
								transformedLine.append("|");
							}
						}
						
						String checkDate = lineArr[1];

						String weekStartDate = DateUtil.getWeekStartDate(dateFormat.parse(checkDate), 0);

						Date startDate = dateFormat.parse(weekStartDate);

						if (startDate.before(dateCap)) {
							continue;
						}

						line = transformedLine.toString();
						
						if(fileContent.containsKey(weekStartDate)) {
							List<String> existing = fileContent.get(weekStartDate);
							existing.add(line);
							fileContent.put(weekStartDate, existing);
						} else {
							List<String> newCont = new ArrayList<>();
							newCont.add(line);
							fileContent.put(weekStartDate, newCont);
						}
						
						if (lineCounter % 100000 == 0) {
							for (Map.Entry<String, List<String>> entry : fileContent.entrySet()) {
								String weekStartKey = entry.getKey();
								List<String> content = entry.getValue();
								String fileName = inputFilePath + "/comp_price_" + weekStartKey.replaceAll("/", "")
										+ ".csv";
	
								File file = new File(fileName);
								Path path = Paths.get(fileName);
	
								if (file.exists()) {
									Files.write(path, content, StandardOpenOption.APPEND);
								} else {
									Files.write(path, header.getBytes(), StandardOpenOption.CREATE_NEW);
									Files.write(path, content, StandardOpenOption.APPEND);
								}
							}
							fileContent.clear();
							logger.info("Processed - " + lineCounter + " records");
						}
					} catch (Exception e) {
						logger.error("Error at line # " + lineCounter);
						//errorRecords.add(line);
					}
				}
				
				if(fileContent.size() > 0) {
					for (Map.Entry<String, List<String>> entry : fileContent.entrySet()) {
						String weekStartKey = entry.getKey();
						List<String> content = entry.getValue();
						String fileName = inputFilePath + "/comp_price_" + weekStartKey.replaceAll("/", "")
								+ ".csv";

						File file = new File(fileName);
						Path path = Paths.get(fileName);

						if (file.exists()) {
							Files.write(path, content, StandardOpenOption.APPEND);
						} else {
							Files.write(path, header.getBytes(), StandardOpenOption.CREATE_NEW);
							Files.write(path, content, StandardOpenOption.APPEND);
						}
					}
					fileContent.clear();
					logger.info("Processed: " + lineCounter + " records");	
				}
			}
			catch (Exception e) {
				if(line != null)
					logger.error(line);
				logger.error("Error", e);
				logger.error("Error in line: " + errorCounter);	
			}
		} catch (Exception e) {
			if(line != null)
				logger.error(line);
			logger.error("Error", e);
			logger.error("Error in line: " + errorCounter);	
		}
	}
}
