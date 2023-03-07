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

public class PromoFileSplitter {

	private final static String INPUT_FOLDER = "INPUT_FOLDER=";
	private final static String FILE = "FILE=";
	private final static String PRICE_LEVEL = "PRICE_LEVEL=";
	private static String inputFilePath;
	private static String inputFile;
	private static String priceLevel;
	private static Logger logger = Logger.getLogger("PromoFileSplitter");

	private List<String> errorRecords = new ArrayList<>();
	public static void main(String args[]) {
		PromoFileSplitter priceFileSplitter = new PromoFileSplitter();
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
			SimpleDateFormat dateFormat = new SimpleDateFormat("MM/dd/yyyy HH:mm:ss");
			String header = "DEAL_ID|DEAL_START_DATE|DEAL_END_DATE|ITEM|ITEM_INCLUDED_EXCLUDED|"
					+ "CONSTRAINT|DEAL_DESCRIPTION|MINIMUM_TOTAL_TICKET|MAXIMUM_TOTAL_TICKET|COUNTRY_CODE|"
					+ "PRIORITY|COMMENTS|CUSTOMER_TYPE|MINIMUM_CUMULATIVE_PURCHASE|MAXIMUM_CUMULATIVE_PURCHASE|"
					+ "MINIMUM_QUANTITY|MAXIMUM_QUANTITY|DISCOUNT_MONEY|DISCOUNT_PERCENT|STORE|"
					+ "INCLUDE_EXCLUDE_FLAG" + System.lineSeparator();
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

						String startDate = lineArr[1];

						String weekStartDate = DateUtil.getWeekStartDate(dateFormat.parse(startDate), 0);
						
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
							String fileName = inputFilePath + "/promo-history_" + weekStartKey.replaceAll("/", "")
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
				
				if(fileContent.size() > 0) {
					for (Map.Entry<String, List<String>> entry : fileContent.entrySet()) {
						String weekStartKey = entry.getKey();
						List<String> content = entry.getValue();
						String fileName = inputFilePath + "/promo-history_" + weekStartKey.replaceAll("/", "")
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
		} catch (Exception e) {
			if(line != null)
				logger.error(line);
			logger.error("Error", e);
		}
	}
}
