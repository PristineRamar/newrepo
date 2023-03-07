package com.pristine.test;

import java.io.BufferedReader;
import java.io.FileReader;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

import org.apache.log4j.Logger;
import org.apache.log4j.PropertyConfigurator;
import org.rosuda.JRI.Rengine;

import com.pristine.service.offermgmt.prediction.PredictionEngineBatchImpl;
import com.pristine.service.offermgmt.prediction.PredictionEngineInput;
import com.pristine.service.offermgmt.prediction.PredictionEngineItem;
import com.pristine.service.offermgmt.prediction.PredictionEngineOutput;
import com.pristine.service.offermgmt.prediction.PredictionException;
import com.pristine.util.Constants;
import com.pristine.util.PropertyManager;

public class PredictionEngineCall {

	private static Logger logger = Logger.getLogger("PredictionEngineCallTest");
	private static final String PRODUCT_LEVEL_ID = "PRODUCT_LEVEL_ID=";
	private static final String PRODUCT_ID = "PRODUCT_ID=";

	public static void main(String[] args) throws NumberFormatException, IOException, PredictionException {

		String rScriptRootPath = "";
		String rScriptSourcePath = "";
		String filePath = "";
		int productId = 0;
		int productlevelId = 0;

		if (args.length > 0) {
			for (String arg : args) {
				if (arg.startsWith(PRODUCT_LEVEL_ID)) {
					productlevelId = (Integer.parseInt(arg.substring(PRODUCT_LEVEL_ID.length())));
				} else if (arg.startsWith(PRODUCT_ID)) {
					productId = (Integer.parseInt(arg.substring(PRODUCT_ID.length())));
				} 
			}
		}
		
		PropertyConfigurator.configure("log4j.properties");
		PropertyManager.initialize("recommendation.properties");

		filePath=PropertyManager.getProperty("INPUT_FILE_PATH");
		rScriptSourcePath=PropertyManager.getProperty("PREDICTION_R_SRC_PATH");
		rScriptRootPath=PropertyManager.getProperty("PREDICTION_R_SCRIPT_PATH");
		
		List<PredictionEngineItem> input=readFile(filePath);

		PredictionEngineBatchImpl p = new PredictionEngineBatchImpl(rScriptRootPath,
				rScriptSourcePath, 1, false);
	
		Rengine re = getREngine();

		PredictionEngineOutput peo=p.predict(re, new PredictionEngineInput(productlevelId, productId, 'B', input, ""), false, false, false);
		logger.info("Prediction output returned and map size"+ peo.getPredictionMap().size());
		System.exit(0);
		
	}



	private static List<PredictionEngineItem> readFile(String filePath) throws NumberFormatException, IOException {

		// List<CategoryItem> urlList = getURLList();
		String[] fileInput = new String[50];
		List<PredictionEngineItem> inpitItemFile = new ArrayList<>();

		@SuppressWarnings("resource")
		BufferedReader br = new BufferedReader(new FileReader(filePath));
		String line;
		int counter = 0;
		while ((line = br.readLine()) != null) {
			counter++;
			if (counter == 1) {
				continue;
			}

			fileInput = line.split(",");
			
			int itemCode = Integer.parseInt(fileInput[1]);

			String upcfromFile = fileInput[2];
			String upc = "";
			try {
				if (upcfromFile != null && upcfromFile !="")
					upc =upcfromFile;
			} catch (Exception ex) {

			}

			int locationLevelId = Integer.parseInt(fileInput[8]);
			int locationId = Integer.parseInt(fileInput[9]);
			int dealType = 0;
		

			double regPrice = Double.parseDouble(fileInput[15]);
			int regQuantity = Integer.parseInt(fileInput[16]);
			char useSubstFlag = Constants.NO;
			char mainOrImpactFlag = Constants.NO;
			Boolean usePrediction = true;
			Boolean isAvgMovAlreadySet = false;
			String predictionStartDateStr = "2022-06-05";
			int saleQuantity = 0;
			double salePrice = 0;

			try {
				if (fileInput[19] != null && fileInput[19] != "")
					salePrice = Double.parseDouble(fileInput[19]);
				saleQuantity = Integer.parseInt(fileInput[18]);
			} catch (Exception ex) {

			}

			int adPageNo = 0;
			int adBlockNo = 0;
			int displayTypeId = 0;
			int promoTypeId = 0;
			try {
				if (fileInput[13] != null && fileInput[13] !="")
					promoTypeId = Integer.parseInt(fileInput[13]);
			} catch (Exception ex) {

			}

			int retLirId = 0;
			String id = fileInput[44];
			try {
				if (id != null && id != "" && !id.equalsIgnoreCase("NA"))
					retLirId = Integer.parseInt(id);
			} catch (Exception ex) {

			}

			int subsScenarioId = 0;

			try {
				if (fileInput[31] != null && fileInput[31] != "")
					subsScenarioId = Integer.parseInt(fileInput[31]);
			} catch (Exception ex) {

			}

			inpitItemFile.add(new PredictionEngineItem(locationLevelId, locationId, predictionStartDateStr, dealType,
					itemCode, upc, regPrice, regQuantity, useSubstFlag, mainOrImpactFlag, usePrediction,
					isAvgMovAlreadySet, predictionStartDateStr, saleQuantity, salePrice, adPageNo, adBlockNo,
					promoTypeId, displayTypeId, subsScenarioId, 0, 0, retLirId));
		}

		logger.info("readFile() Total records Read: " + inpitItemFile.size());
		return inpitItemFile;
	}
	
	private static Rengine getREngine() throws PredictionException {
		logger.info("getREngine() start");
		if (!Rengine.versionCheck()) {
			logger.error("** Version mismatch - Java files don't match library version.");
			throw new PredictionException("** Version mismatch - Java files don't match library version.");
		}

		logger.info("Creating Rengine (with arguments)");

		Rengine engine = Rengine.getMainEngine();
		if (engine == null) {
			logger.info("Creating R Instance");
			engine = new Rengine(new String[] { "--vanilla" }, false, null);
		} else {
			logger.info("R Instance already Exists");
		}
		logger.info("getREngine() end");
		return engine;
	}


}