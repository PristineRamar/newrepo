package com.pristine.dataload;

import java.io.File;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.nio.file.StandardOpenOption;
import java.util.ArrayList;
import java.util.List;
import java.util.Scanner;
import org.apache.log4j.Logger;
import org.apache.log4j.PropertyConfigurator;
import com.pristine.util.PropertyManager;

public class FileSplitter {

	private final static String INPUT_FOLDER = "INPUT_FOLDER=";
	private final static String FILE = "FILE=";
	private static String inputFilePath;
	private static String inputFile;
	private static Logger logger = Logger.getLogger("FileSplitter");
	public static void main(String args[]) {
		FileSplitter priceFileSplitter = new FileSplitter();
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

		priceFileSplitter.splitFileByDate();
	}

	private void splitFileByDate() {
		String line = null;
		try {
			String header = "RECORD_TYPE|CHAIN_ID|STORE_NUMBER|TERMINAL_NUMBER|TRANSACTION_TYPE|TRANSACTION_NUMBER|"
					+ "TRANSACTION_DATETIME|UPC|UNIT_SALE_PRICE|UNIT_REGULAR_PRICE|ITEM_COUNT|ITEM_WEIGHT|"
					+ "GROSS_AMOUNT_PAID_FOR_ITEM|TPR_DISCOUNT|LOYALTY_CARD_DISCOUNT|OTHER_DISCOUNTS|"
					+ "NET_AMOUNT_PAID_FOR_ITEM|LOYALTY_CARD_NUMBER|STORE_COUPON_USED|MANUFACTURER_COUPON_USED|"
					+ "AD_EVENT|AD_PAGE|AD_BLOCK|ITEM_CODE|DEAL_CODE|DEAL_QUANTITY|PRICE_OVERRIDE_TOTAL_AMT|"
					+ "PRICE_OVERRIDE_REASON_CODE" + System.lineSeparator();
			List<String> fileContent = new ArrayList<>(); 
			int fileCount = 0;
			try (Scanner scanner = new Scanner(new File(inputFilePath + "/" + inputFile))) {
				int lineCounter = 0;
				while (scanner.hasNextLine()) {
						lineCounter++;
						if (lineCounter == 1) {
							line = scanner.nextLine();
							continue;
						}
						
						line = scanner.nextLine();
						fileContent.add(line);
					if (lineCounter % 2000000 == 0) {
						fileCount++;
						String fileName = inputFilePath + "/tlog_file_" + fileCount + ".csv";
						File file = new File(fileName);
						Path path = Paths.get(fileName);
						if (file.exists()) {
							Files.write(path, fileContent, StandardOpenOption.APPEND);
						} else {
							Files.write(path, header.getBytes(), StandardOpenOption.CREATE_NEW);
							Files.write(path, fileContent, StandardOpenOption.APPEND);
						}
						fileContent.clear();
						logger.info("Processed " + lineCounter + " records");
					}
				}
				
				if(fileContent.size() > 0) {
					fileCount++;
					String fileName = inputFilePath + "/tlog_file_" + fileCount + ".csv";
					File file = new File(fileName);
					Path path = Paths.get(fileName);
					if (file.exists()) {
						Files.write(path, fileContent, StandardOpenOption.APPEND);
					} else {
						Files.write(path, header.getBytes(), StandardOpenOption.CREATE_NEW);
						Files.write(path, fileContent, StandardOpenOption.APPEND);
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
