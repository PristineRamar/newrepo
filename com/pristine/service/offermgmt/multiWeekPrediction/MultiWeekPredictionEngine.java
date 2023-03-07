package com.pristine.service.offermgmt.multiWeekPrediction;

import java.io.File;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Date;
import java.util.List;

import org.apache.log4j.Logger;
import org.rosuda.JRI.REXP;
import org.rosuda.JRI.Rengine;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.pristine.dto.offermgmt.multiWeekPrediction.MultiWeekPredEngineItemDTO;
import com.pristine.service.offermgmt.prediction.PredictionException;
import com.pristine.util.PrestoUtil;
import com.pristine.util.PropertyManager;

/**
 * 
 * @author Nagarajan, Pawan
 *
 */
public class MultiWeekPredictionEngine {

	private static Logger logger = Logger.getLogger("MultiWeekPredictionEngine");
	
	String rScriptRootPath = "";
	String rScriptSourcePath = "";
	int ignoreProductIdItemCnt = 60;
	boolean isOnline = false;

	/**
	 * Default constructor to initialize properties
	 */
	public MultiWeekPredictionEngine(String rScriptRootPath, String rScriptSourcePath, int ignoreProductIdItemCnt, boolean isOnline) {
		super();
		this.rScriptRootPath = rScriptRootPath;
		this.rScriptSourcePath = rScriptSourcePath;
		//Product id will not be send to prediction if the no of items to be predicted < ignoreProductIdItemCnt
		this.ignoreProductIdItemCnt = ignoreProductIdItemCnt;
		this.isOnline = isOnline;
	}

	
	/**
	 * Main method to be called for multi week prediction
	 * 
	 * @param multiWeekPredEngineItemDTOInputCol
	 * @return list of MultiWeekPredEngineItemDTO with prediction output
	 */
	public List<MultiWeekPredEngineItemDTO> predict(Rengine re,
			List<MultiWeekPredEngineItemDTO> multiWeekPredEngineItemDTOInputCol) {
			logger.info("predict() : start");
			List<MultiWeekPredEngineItemDTO> multiWeekPredEngineItemDTOOutputCol = 
					new ArrayList<MultiWeekPredEngineItemDTO>();
			 
		multiWeekPredEngineItemDTOOutputCol = 
				callPredictionEngine(re, multiWeekPredEngineItemDTOInputCol);
			logger.info("predict() : end");
			return multiWeekPredEngineItemDTOOutputCol;
	}

	
	/**
	 * This method is converting input list to Json String and calling REngine.
	 * Further converting output Json String to list of objects
	 * 
	 * @param multiWeekPredEngineItemDTOInputCol as List<MultiWeekPredEngineItemDTO>
	 * @return list of MultiWeekPredEngineItemDTO with prediction output
	 */
	private List<MultiWeekPredEngineItemDTO> callPredictionEngine(Rengine re,
			List<MultiWeekPredEngineItemDTO> multiWeekPredEngineItemDTOInputCol) {
		// When more than one user calls the multi week prediction from UI, do
		// sequentially
		logger.info("callPredictionEngine() : start");
		List<MultiWeekPredEngineItemDTO> multiWeekPredEngineItemDTOOutputCol = 
				new ArrayList<MultiWeekPredEngineItemDTO>();
		String inPut = "";
		String outPut = "";
//		Rengine rengine = null;

		try {
			ObjectMapper mapper = new ObjectMapper();

//			logger.debug("Generating input Json String for Prediction Engine");
			inPut = mapper.writeValueAsString(multiWeekPredEngineItemDTOInputCol);
			
			String logPath = String.valueOf(PropertyManager.getProperty("MW_PREDICTION_LOG_PATH"));
			String timeStamp = new SimpleDateFormat("MMddyyyy_HHmmss").format(new Date());
			
			MultiWeekPredEngineItemDTO multiWeekPredEngineItemDTO = multiWeekPredEngineItemDTOInputCol.iterator()
					.next();

			if (logger.isDebugEnabled()) {
				mapper.writeValue(
						new File(logPath + "/" + multiWeekPredEngineItemDTO.getProductId() + "_"
								+ multiWeekPredEngineItemDTO.getLocationId() + "_input_" + timeStamp + ".json"),
						multiWeekPredEngineItemDTOInputCol);
			}
			
			//logger.debug("Input String is : " + inPut);
			logger.info("Initializing REngine");
//			rengine = getREngine();

			logger.info("Executing Multi Week Prediction");
			if (multiWeekPredEngineItemDTOInputCol != null && 
					multiWeekPredEngineItemDTOInputCol.size() > 0) {
				outPut = executeMultiWeekPrediction(re, inPut);
				//logger.debug("Output String is : " + outPut);
			}

//			logger.info("Transforming the returning output String to list of "
//					+ "MultiWeekPredEngineItemDTO objects");
			if (outPut != null && outPut.length() > 0) {
				multiWeekPredEngineItemDTOOutputCol = Arrays
						.asList(mapper.readValue(outPut, MultiWeekPredEngineItemDTO[].class));
				
//				logger.debug("Validating the Output of R");
				
				timeStamp = new SimpleDateFormat("MMddyyyy_HHmmss").format(new Date());
				
				if (logger.isDebugEnabled()) {
					mapper.writeValue(
							new File(logPath + "/" + multiWeekPredEngineItemDTO.getProductId() + "_"
									+ multiWeekPredEngineItemDTO.getLocationId() + "_output_" + timeStamp + ".json"),
							multiWeekPredEngineItemDTOOutputCol);
				}
				
				/*if (validateOutput(multiWeekPredEngineItemDTOInputCol, 
						multiWeekPredEngineItemDTOOutputCol)){
					logger.debug("Valid Output returned from - R ");
				} else{
					logger.debug("Invalid Output returned from - R . "
							+ "Compare R Input and R Output JSONs to analyze the rootcause of issue.");
					throw new PredictionException("Invalid Output returned from R Engine.");
				}*/
				
			} else {
				logger.error("Error :: R Engine output is null or Empty");
			}

		} catch (Exception e) {
			logger.error("Error while executing Multi Week Prediction Engine(): " 
					+ e.getMessage(), e);
		} finally {
//			if (!isOnline){
//				logger.info("Shutting down R Engine");
//				rengine.end();
//			}
		}
		logger.info("callPredictionEngine() : end");
		return multiWeekPredEngineItemDTOOutputCol;
	}

	
	/**
	 * This method returns the instance of R Engine
	 * 
	 * @return instance of REngine
	 * @throws PredictionException
	 */
//	private Rengine getREngine() throws PredictionException {
//
//		logger.info("getREngine() start");
//		if (!Rengine.versionCheck()) {
//			logger.error("** Version mismatch - Java files don't match library version.");
//			throw new PredictionException("** Version mismatch - "
//					+ "Java files don't match library version.");
//		}
//
//		logger.info("Creating Rengine (with arguments)");
//
//		Rengine engine = Rengine.getMainEngine();
//		logger.debug(engine);
//		if (engine == null) {
//			logger.debug("Creating R Instance");
//			engine = new Rengine(new String[] { "--vanilla" }, false, null);
//		} else {
//			logger.debug("R Instance already Exists");
//		}
//		logger.info("getREngine() end");
//		return engine;
//	}

	/**
	 * Executing R scripts and passing input String and converting R output to Java String
	 * 
	 * @param rEngine
	 * @param predictionEngineInput
	 * @return
	 * @throws PredictionException
	 */
	private String executeMultiWeekPrediction(Rengine rEngine, String predictionEngineInput)
			throws PredictionException {

		long tStartTime = System.currentTimeMillis();
		long tEndTime = System.currentTimeMillis();
		String modelPath = "";
		String logPath = "";
//		String rScriptRootPath = "";
//		String rScriptSourcePath = "";
		String outPut = "";

		try {
			logger.info("executeMultiWeekPrediction() start ");

			REXP rObject;

			modelPath = String.valueOf(PropertyManager.getProperty("MW_PREDICTION_MODEL_PATH"));
			logPath = String.valueOf(PropertyManager.getProperty("MW_PREDICTION_LOG_PATH"));

//			rScriptRootPath = String.valueOf(PropertyManager.getProperty("MW_PREDICTION_R_SCRIPT_PATH"));
//			rScriptSourcePath = String.valueOf(PropertyManager.getProperty("MW_PREDICTION_R_SRC_PATH"));
			
//			logger.debug("Model Path :" + modelPath);
//			logger.debug("Log Path :" + logPath);
//			logger.debug("R Script path: " + rScriptRootPath + rScriptSourcePath);
			
			rEngine.assign("input", predictionEngineInput);
			rEngine.assign("root.dir", rScriptRootPath);
			rEngine.assign("model.path", modelPath);
			rEngine.assign("log.path", logPath);

			tStartTime = System.currentTimeMillis();
			logger.info("R Engine execution start... ");
			
			rEngine.eval("source('" + rScriptRootPath + rScriptSourcePath + "')", false);
			
			tEndTime = System.currentTimeMillis();
			logger.info("executeMultiWeekPrediction() - Time taken to predict: "
					+ PrestoUtil.getTimeTakenInMins(tStartTime, tEndTime));
			logger.info("R Engine execution end... ");

			logger.info("Capturing R Engine output");
			rObject = rEngine.eval("pred");

			if (rObject == null) {
				logger.error("*** R Engine output is null *** ");
				throw new PredictionException("null object returned from R");
			}

			outPut = rObject.asString();

		} catch (Exception e) {
			logger.error("Error while executing prediction engine: " + e.getMessage(), e);
			throw new PredictionException(e);
		} finally {			
//		     if (rEngine != null){
//                 rEngine.end();
//		     } 		
		}
		return outPut;
	}
	
	/**
	 * This function validates the R Output and input on various paremeters such as number
	 * of lines returned, key values match
	 * 
	 * @param multiWeekPredEngineItemDTOInputCol, multiWeekPredEngineItemDTOOutputCol
	 * @return boolean (status of validation)
	 */
	@SuppressWarnings("unused")
	private boolean validateOutput(List<MultiWeekPredEngineItemDTO> multiWeekPredEngineItemDTOInputCol,
			List<MultiWeekPredEngineItemDTO> multiWeekPredEngineItemDTOOutputCol){
		
//		logger.debug("Inside validateOutput()");
		
		//Initializing valid output as true
		boolean isValidOutput = true;
		boolean matchFound = true;
		
		//Returning false if number of rows does not match in output
		if(multiWeekPredEngineItemDTOInputCol.size() != multiWeekPredEngineItemDTOOutputCol.size()){
			isValidOutput = false;
			return isValidOutput;
		}
		
		//Returning false if number of rows does not match in output
		//Based on assertion that there will be a matching record in
		//output for given input record
		for(MultiWeekPredEngineItemDTO mwpInDTO : multiWeekPredEngineItemDTOInputCol){
			if(!matchFound){
				isValidOutput = false;
				break;
			}
			//reinitializing the matchFound as true for next 
			matchFound = false;
			for(MultiWeekPredEngineItemDTO mwpOutDTO : multiWeekPredEngineItemDTOOutputCol){
				if(mwpInDTO.getItemCode() == mwpOutDTO.getItemCode() && 
						mwpInDTO.getItemCode() == mwpOutDTO.getItemCode() && 
						mwpInDTO.getLocationId() == mwpOutDTO.getLocationId() && 
						mwpInDTO.getLocationLevelId() == mwpOutDTO.getLocationLevelId() && 
						mwpInDTO.getProductId() == mwpOutDTO.getProductId() && 
						mwpInDTO.getProductLevelId() == mwpOutDTO.getProductLevelId() &&  
						mwpInDTO.getStartDate().equalsIgnoreCase(mwpOutDTO.getStartDate()) && 
						mwpInDTO.getPromoTypeId() == mwpOutDTO.getPromoTypeId() && 
						mwpInDTO.getRegMultiple() == mwpOutDTO.getRegMultiple() && 
						mwpInDTO.getRegPrice() == mwpOutDTO.getRegPrice() && 
						mwpInDTO.getSaleMultiple() == mwpOutDTO.getSaleMultiple() &&  
						mwpInDTO.getSalePrice() == mwpOutDTO.getSalePrice() && 
						mwpInDTO.getScenarioId() == mwpOutDTO.getScenarioId() && 
						mwpInDTO.getUPC().equalsIgnoreCase(mwpOutDTO.getUPC()) && 
						mwpInDTO.getAdBlockNo() == mwpOutDTO.getAdBlockNo() && 
						mwpInDTO.getAdPageNo() == mwpOutDTO.getAdPageNo() && 
						mwpInDTO.getDisplayTypeId() == mwpOutDTO.getDisplayTypeId()) {
					
					//match found : breaking inner loop
					matchFound = true;
					break;
				}
			}
		}

		return isValidOutput;
		
	}
}
		

