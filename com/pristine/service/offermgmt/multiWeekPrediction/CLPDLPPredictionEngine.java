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
import com.pristine.dto.offermgmt.mwr.CLPDLPPredictionDTO;
import com.pristine.service.offermgmt.prediction.PredictionException;
import com.pristine.util.PropertyManager;

public class CLPDLPPredictionEngine {


	private static Logger logger = Logger.getLogger("MultiWeekPredictionEngine");
	
	String rScriptRootPath = "";
	String rScriptSourcePath = "";
	int ignoreProductIdItemCnt = 60;
	boolean isOnline = false;

	/**
	 * Default constructor to initialize properties
	 */
	public CLPDLPPredictionEngine(String rScriptRootPath, String rScriptSourcePath, int ignoreProductIdItemCnt,
			boolean isOnline) {
		this.rScriptRootPath = rScriptRootPath;
		this.rScriptSourcePath = rScriptSourcePath;
		// Product id will not be send to prediction if the no of items to be predicted < ignoreProductIdItemCnt
		this.ignoreProductIdItemCnt = ignoreProductIdItemCnt;
		this.isOnline = isOnline;
	}

	
	/**
	 * Main method to be called for multi week prediction
	 * 
	 * @param multiWeekPredEngineItemDTOInputCol
	 * @return list of MultiWeekPredEngineItemDTO with prediction output
	 */
	public List<CLPDLPPredictionDTO> predict(Rengine re,
			List<CLPDLPPredictionDTO> clpDlpInputCollection) {
			logger.info("predict() : start");
			List<CLPDLPPredictionDTO> clpDlpOuputCollection = 
					new ArrayList<CLPDLPPredictionDTO>();
			 
			clpDlpOuputCollection = 
				callPredictionEngine(re, clpDlpInputCollection);
			logger.info("predict() : end");
			return clpDlpOuputCollection;
	}

	
	/**
	 * This method is converting input list to Json String and calling REngine.
	 * Further converting output Json String to list of objects
	 * 
	 * @param clpDlpInputCollection as List<CLPDLPPredictionDTO>
	 * @return list of MultiWeekPredEngineItemDTO with prediction output
	 */
	private List<CLPDLPPredictionDTO> callPredictionEngine(Rengine re,
			List<CLPDLPPredictionDTO> clpDlpInputCollection) {
		// When more than one user calls the multi week prediction from UI, do
		// sequentially
		logger.info("callPredictionEngine() : start");
		List<CLPDLPPredictionDTO> clpDlpOutputCollection = 
				new ArrayList<CLPDLPPredictionDTO>();
		String inPut = "";
		String outPut = "";
//		Rengine rengine = null;

		try {
			ObjectMapper mapper = new ObjectMapper();

			logger.debug("Generating input Json String for Prediction Engine");
			inPut = mapper.writeValueAsString(clpDlpInputCollection);

			String logPath = String.valueOf(PropertyManager.getProperty("CLP_DLP_PREDICTION_LOG_PATH"));
			String timeStamp = new SimpleDateFormat("MMddyyyy_HHmmss").format(new Date());
			CLPDLPPredictionDTO clpdlPPredictionDTO = clpDlpInputCollection.iterator().next();
			mapper.writeValue(
					new File(logPath + "/" + clpdlPPredictionDTO.getProductId() + "_"
							+ clpdlPPredictionDTO.getLocationId() + "_input_" + timeStamp + ".json"),
					clpDlpInputCollection);
			
			//logger.debug("Input String is : " + inPut);
			logger.info("Initializing REngine");
//			rengine = getREngine();

			logger.info("Executing Multi Week Prediction");
			if (clpDlpInputCollection != null && 
					clpDlpInputCollection.size() > 0) {
				outPut = executeMultiWeekPrediction(re, inPut);
				//logger.debug("Output String is : " + outPut);
			}

			logger.info("Transforming the returning output String to list of "
					+ "MultiWeekPredEngineItemDTO objects");
			if (outPut != null && outPut.length() > 0) {

				logger.info("Output json: " + outPut);
				clpDlpOutputCollection = Arrays
						.asList(mapper.readValue(outPut, CLPDLPPredictionDTO[].class));
				
				logger.debug("Validating the Output of R");
				
				timeStamp = new SimpleDateFormat("MMddyyyy_HHmmss").format(new Date());
				
				mapper.writeValue(
						new File(logPath + "/" + clpdlPPredictionDTO.getProductId() + "_"
								+ clpdlPPredictionDTO.getLocationId() + "_output_" + timeStamp + ".json"),
						clpDlpOutputCollection);
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
		return clpDlpOutputCollection;
	}


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

			modelPath = String.valueOf(PropertyManager.getProperty("CLP_DLP_PREDICTION_MODEL_PATH"));
			logPath = String.valueOf(PropertyManager.getProperty("CLP_DLP_PREDICTION_LOG_PATH"));

//			rScriptRootPath = String.valueOf(PropertyManager.getProperty("MW_PREDICTION_R_SCRIPT_PATH"));
//			rScriptSourcePath = String.valueOf(PropertyManager.getProperty("MW_PREDICTION_R_SRC_PATH"));
			
			logger.debug("Model Path :" + modelPath);
			logger.debug("Log Path :" + logPath);
			logger.debug("R Script path: " + rScriptRootPath + rScriptSourcePath);
			
			rEngine.assign("input", predictionEngineInput);
			rEngine.assign("root.dir", rScriptRootPath);
			rEngine.assign("model.path", modelPath);
			rEngine.assign("log.path", logPath);

			tStartTime = System.currentTimeMillis();
			logger.info("R Engine execution start... ");
			
			rEngine.eval("source('" + rScriptRootPath + rScriptSourcePath + "')", false);
			
			tEndTime = System.currentTimeMillis();
			logger.debug("^^^ Time -- Prediction Multi Week --> " +
			((tEndTime - tStartTime) / 1000) + " s ^^^");
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
}
