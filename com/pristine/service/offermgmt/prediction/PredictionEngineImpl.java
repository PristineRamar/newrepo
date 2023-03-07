package com.pristine.service.offermgmt.prediction;

import java.util.ArrayList;
import java.util.List;

import org.apache.log4j.Logger;
import org.rosuda.JRI.Rengine;

import com.pristine.dto.offermgmt.multiWeekPrediction.MultiWeekPredEngineItemDTO;
import com.pristine.dto.offermgmt.mwr.CLPDLPPredictionDTO;
import com.pristine.service.offermgmt.multiWeekPrediction.CLPDLPPredictionEngine;
import com.pristine.service.offermgmt.multiWeekPrediction.MultiWeekPredictionEngine;

public class PredictionEngineImpl implements PredictionEngine {
	private static Logger logger = Logger.getLogger("PredictionEngineImpl");

	String rScriptRootPath = "";
	String rScriptSourcePath = "";
	int ignoreProductIdItemCnt = 60;
	boolean isOnline = false;

	private List<MultiWeekPredEngineItemDTO> multiWeekPredEngineItemDTOInputCol = new ArrayList<MultiWeekPredEngineItemDTO>();
	private List<CLPDLPPredictionDTO> clpDlpInputCol = new ArrayList<CLPDLPPredictionDTO>();

	private PredictionEngineInput predictionEngineInput = null;
	private boolean isGetOnlyExplain = false;
	private boolean isPromotion = false;
	private boolean isSubstitute = false;

	public PredictionEngineImpl(String rScriptRootPath, String rScriptSourcePath, int ignoreProductIdItemCnt, boolean isOnline) {
		super();
		this.rScriptRootPath = rScriptRootPath;
		this.rScriptSourcePath = rScriptSourcePath;
		// Product id will not be send to prediction if the no of items to be predicted < ignoreProductIdItemCnt
		this.ignoreProductIdItemCnt = ignoreProductIdItemCnt;
		this.isOnline = isOnline;
	}

	@Override
	public PredictionEngineOutput weeklyPrediction(PredictionEngineInput predictionEngineInput, boolean isGetOnlyExplain, boolean isPromotion,
			boolean isSubstitute) throws PredictionException {
		PredictionEngineOutput predictionEngineOutput = null;

		this.predictionEngineInput = predictionEngineInput;
		this.isGetOnlyExplain = isGetOnlyExplain;
		this.isPromotion = isPromotion;
		this.isSubstitute = isSubstitute;

		predictionEngineOutput = (PredictionEngineOutput) predict("WEEKLY");

		return predictionEngineOutput;
	}

	@SuppressWarnings("unchecked")
	@Override
	public List<MultiWeekPredEngineItemDTO> multiWeekPrediction(List<MultiWeekPredEngineItemDTO> multiWeekPredEngineItemDTOInputCol)
			throws PredictionException {
		List<MultiWeekPredEngineItemDTO> multiWeekPredEngineItemDTOOutputCol = null;

		this.multiWeekPredEngineItemDTOInputCol = multiWeekPredEngineItemDTOInputCol;
		multiWeekPredEngineItemDTOOutputCol = (List<MultiWeekPredEngineItemDTO>) predict("MULTI_WEEK");
		
		return multiWeekPredEngineItemDTOOutputCol;
	}

	private Object predict(String predictionType) throws PredictionException {
		synchronized (PredictionEngineImpl.class) {
			Object outputObject = null;
			try {
				Rengine re = getREngine();

				try {
					if (predictionType.equals("WEEKLY")) {
						PredictionEngineBatchImpl p = new PredictionEngineBatchImpl(this.rScriptRootPath, this.rScriptSourcePath,
								this.ignoreProductIdItemCnt, this.isOnline);

						outputObject = p.predict(re, this.predictionEngineInput, this.isGetOnlyExplain, this.isPromotion, this.isSubstitute);

					} else if(predictionType.equals("MULTI_WEEK")) {
						MultiWeekPredictionEngine mwpe = new MultiWeekPredictionEngine(this.rScriptRootPath, this.rScriptSourcePath,
								this.ignoreProductIdItemCnt, this.isOnline);

						outputObject = mwpe.predict(re, this.multiWeekPredEngineItemDTOInputCol);
					} else if(predictionType.equals("CLP_DLP")) {
						CLPDLPPredictionEngine clpdlpPredictionEngine = new CLPDLPPredictionEngine(this.rScriptRootPath, this.rScriptSourcePath,
								this.ignoreProductIdItemCnt, this.isOnline);
						
						outputObject = clpdlpPredictionEngine.predict(re, this.clpDlpInputCol);
					}
				} catch (Exception e) {
					logger.error("Error in predict(): " + e.getMessage(), e);
					throw new PredictionException(e);
				} finally {
					// Keep the thread if it is called from the batch program
					if (!isOnline) {
						re.end();
						logger.info("R Instance is closed###");
					}
				}
			} catch (Exception e) {
				logger.error("Error while creating Rengine in predict(): " + e.getMessage(), e);
				throw new PredictionException(e);
			}
			return outputObject;
		}
	}

	private Rengine getREngine() throws PredictionException {
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

	@SuppressWarnings("unchecked")
	@Override
	public List<CLPDLPPredictionDTO> clpDlpPrediction(List<CLPDLPPredictionDTO> clpDlpPredictionInput)
			throws PredictionException {
		
		List<CLPDLPPredictionDTO> clpDlpPredictionOutput = null;

		this.clpDlpInputCol = clpDlpPredictionInput;
		
		clpDlpPredictionOutput = (List<CLPDLPPredictionDTO>) predict("CLP_DLP");
		
		return clpDlpPredictionOutput;
	}

}
