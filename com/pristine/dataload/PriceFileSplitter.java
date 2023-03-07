package com.pristine.dataload;

import java.io.File;
import java.io.IOException;
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

public class PriceFileSplitter {

	private final static String INPUT_FOLDER = "INPUT_FOLDER=";
	private final static String FILE = "FILE=";
	private final static String PRICE_LEVEL = "PRICE_LEVEL=";
	private static String inputFilePath;
	private static String inputFile;
	private static String priceLevel;
	private static Logger logger = Logger.getLogger("CompFileSplitter");

	private List<String> errorRecords = new ArrayList<>();
	public static void main(String args[]) {
		PriceFileSplitter priceFileSplitter = new PriceFileSplitter();
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
			if (arg.startsWith(PRICE_LEVEL)) {
				priceLevel = arg.substring(PRICE_LEVEL.length());
			}
		}

		priceFileSplitter.splitFileByDate();
	}

	private void splitFileByDate() {
		String line = null;
		try {
			SimpleDateFormat dateFormat = new SimpleDateFormat(Constants.APP_DATE_FORMAT);
			String date = "01/01/2017";
			Date dateCap = dateFormat.parse(date);
			String header = "UPC|ITEM|PROCESS_LEVEL|ZONE_STORE|REGULAR_RETAIL_EFFECTIVE_DATE"
					+ "|REGULAR_RETAIL|QUANTITY|SALE_RETAIL_START_DATE|SALE_RETAIL_END_DATE"
					+ "|SALE_RETAIL|SALE_RETAIL_QTY|CORE_RETAIL|VDP_RETAIL" + System.lineSeparator();
			HashMap<String, List<String>> fileContent = new HashMap<>(); 
			
			try (Scanner scanner = new Scanner(new File(inputFilePath + "/" + inputFile))) {
				int lineCounter = 0;
				while (scanner.hasNextLine()) {
					//try {
						lineCounter++;
						if (lineCounter == 1) {
							line = scanner.nextLine();
							continue;
						}
						
						line = scanner.nextLine();
						String lineArr[] = line.split("\\|");

						String effDate = lineArr[4];

						String actualWeekStartDate = DateUtil.getWeekStartDate(dateFormat.parse(effDate), 0);

						String weekStartDate = DateUtil.getWeekStartDate(dateFormat.parse(actualWeekStartDate), 1);
						
						Date startDate = dateFormat.parse(weekStartDate);

						if (startDate.before(dateCap)) {
							continue;
						}

						//RTL_CHG_ID|ITEM|ZONE|RTL_PRICE| RTL_PRICE_CHANGE_DATE |PROPOSED_RTL|PROCESS_DATE|PRICE_CHG_TYPE
						//RTL_CHG_ID|ITEM|STORE|RTL_PRICE| RTL_PRICE_CHANGE_DATE |PROPOSED_RTL|PROCESS_DATE|PRICE_CHG_TYPE
						StringBuilder formattedLine = new StringBuilder();
						formattedLine.append("0"); // UPC
						formattedLine.append("|");
						formattedLine.append(lineArr[1]); //ITEM
						formattedLine.append("|");
						formattedLine.append(priceLevel); // PROCESS_LEVEL
						formattedLine.append("|");
						formattedLine.append(lineArr[2]); //ZONE
						formattedLine.append("|");
						formattedLine.append(""); //EFF_DATE
						formattedLine.append("|");
						if(priceLevel.equals("1")) {
							formattedLine.append(lineArr[3]); //PROPOSED_RTL	
						}else {
							formattedLine.append(lineArr[3]); //RTL_PRICE
						}
						formattedLine.append("|");
						formattedLine.append("1"); // QTY
						formattedLine.append("|");
						formattedLine.append("");
						formattedLine.append("|");
						formattedLine.append("");
						formattedLine.append("|");
						formattedLine.append("");
						formattedLine.append("|");
						formattedLine.append("");
						formattedLine.append("|");
						formattedLine.append("");
						formattedLine.append("|");
						formattedLine.append("");
						
						line = formattedLine.toString();
						
						//line = line  + System.lineSeparator();
						
						
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
							String fileName = inputFilePath + "/price-history_" + weekStartKey.replaceAll("/", "")
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
						logger.info("Processed " + lineCounter + " records");
					}
					/*} catch (Exception e) {
						logger.error("Error at line # " + lineCounter);
						errorRecords.add(line);
					}*/
				}
				
				if(fileContent.size() > 0) {
					for (Map.Entry<String, List<String>> entry : fileContent.entrySet()) {
						String weekStartKey = entry.getKey();
						List<String> content = entry.getValue();
						String fileName = inputFilePath + "/price-history_" + weekStartKey.replaceAll("/", "")
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
					logger.info("Processed " + lineCounter + " records");	
				}
			}
			
			File file = new File(inputFilePath + "/error-records.csv");
			Path path = Paths.get(inputFilePath + "/error-records.csv");
			errorRecords.forEach(error -> {
				try {
					Files.write(path, error.getBytes(), StandardOpenOption.WRITE);
				} catch (IOException e) {
					logger.error("Error");
				}
			});
			
		} catch (Exception e) {
			if(line != null)
				logger.error(line);
			logger.error("Error", e);
		}
	}
}
