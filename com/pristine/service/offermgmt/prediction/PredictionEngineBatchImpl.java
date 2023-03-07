package com.pristine.service.offermgmt.prediction;

//import java.text.SimpleDateFormat;
import java.util.*;

import org.apache.log4j.Logger;
import org.rosuda.JRI.*;

import com.pristine.util.PrestoUtil;
import com.pristine.util.PropertyManager;
import com.pristine.util.offermgmt.PRConstants;
import com.pristine.util.offermgmt.PRFormatHelper;


/**
 * @author anand
 *
 */
public class PredictionEngineBatchImpl  {
	private static Logger logger = Logger.getLogger("PredictionEngineBatchImpl");
	//private static Rengine re;
	//private Rengine re;
	String rScriptRootPath = "D:/WorkspaceR/code/api/api_java.R";
	String rScriptSourcePath = "";
	int ignoreProductIdItemCnt = 60;
	boolean isOnline = false;
	//String pathToRScript = "G:/64SQ_Data/TESTING/3/code/api/api_java.R";
	public PredictionEngineBatchImpl(String rScriptRootPath, String rScriptSourcePath, int ignoreProductIdItemCnt, boolean isOnline) {
		super();
		this.rScriptRootPath = rScriptRootPath;
		this.rScriptSourcePath = rScriptSourcePath;
		//Product id will not be send to prediction if the no of items to be predicted < ignoreProductIdItemCnt
		this.ignoreProductIdItemCnt = ignoreProductIdItemCnt;
		this.isOnline = isOnline;
	}

//	@Override
//	public PredictionEngineOutput predictMovement(PredictionEngineInput predictionEngineInput) {
//		// TODO Auto-generated method stub
//		return null;
//	}
	
	/**
	 * Predicts movement
	 * @return
	 * @throws PredictionException
	 */
	public PredictionEngineOutput predict(Rengine re, PredictionEngineInput predictionEngineInput, boolean isGetOnlyExplain,
			boolean isPromotion, boolean isSubstitute) throws PredictionException{
		//When more than one user calls the prediction from UI, do sequentially
//		synchronized (PredictionEngineBatchImpl.class) {
			//logger.info("predict() start"); 
			PredictionEngineOutput peo = new PredictionEngineOutput();
//			try {
				
//				Rengine re = getREngine();
//				logger.debug("re.waitForR()" + re.waitForR());
				//re = getREngine();
				try {
					//HashMap<PredictionEngineKey, PredictionEngineValue> predictionMap = executePrediction(re, predictionEngineInput);
					//peo.setPredictionMap(predictionMap);
					peo = executePrediction(re, predictionEngineInput, isGetOnlyExplain, isPromotion, isSubstitute);
					// re.end();
				} catch (Exception e) {
					// System.out.println("EX:"+e.getMessage());
					logger.error("Error in predict(): " + e.getMessage(), e);
					throw new PredictionException(e);
				} finally {
					//Keep the thread if it is called from the batch program
//					if(!isOnline) {
//						re.end();
//						logger.info("R Instance is closed###");
//					}
						
				}
//			} catch (Exception e) {
				// System.out.println("EX:"+e.getMessage());
//				logger.error("Error while creation Rengine in predict(): " + e.getMessage(), e);
//				throw new PredictionException(e);
//			}
//			logger.info("predict() end"); 
			return peo;
//		}		
	}
	
	
	
//	private Rengine getREngine() throws PredictionException{
//		logger.info("getREngine() start"); 
//		if (!Rengine.versionCheck()) {		    
//		    logger.error("** Version mismatch - Java files don't match library version.");
//		    throw new PredictionException("** Version mismatch - Java files don't match library version.");
//		}	
//        
//        logger.info("Creating Rengine (with arguments)");
//	
//		Rengine engine = Rengine.getMainEngine();
//		if(engine == null){
//			logger.info("Creating R Instance");
//		    engine = new Rengine(new String[] {"--vanilla"}, false, null);
//		}else{
//			logger.info("R Instance already Exists");
//		}
//		logger.info("getREngine() end"); 
//		return engine;
//	}
	
	
	
    /**
     * Runs R script and creates PredictionMap of (upc,price) and predicted movement
     * @param re
     * @return
     * @throws PredictionException
     */
    private PredictionEngineOutput executePrediction(Rengine re, 
    		PredictionEngineInput predictionEngineInput, boolean isGetOnlyExplain, 
    		boolean isPromotion, boolean isSubstitute) throws PredictionException {
    	String scoringStartDateType = String.valueOf(PropertyManager.getProperty("SCORING_START_DATE_TYPE"));
    	String modelPath = "";
    	String logPath  = "";
		if (isPromotion) {
			modelPath = String.valueOf(PropertyManager.getProperty("PROMO_PREDICTION_MODEL_PATH"));
			logPath = String.valueOf(PropertyManager.getProperty("PROMO_PREDICTION_LOG_PATH"));
		} else {
			modelPath = String.valueOf(PropertyManager.getProperty("PREDICTION_MODEL_PATH"));
			logPath = String.valueOf(PropertyManager.getProperty("PREDICTION_LOG_PATH"));
		}
    		
    	scoringStartDateType = scoringStartDateType.toUpperCase().trim();
    	HashMap<PredictionEngineKey,PredictionEngineValue> predictionMap = new HashMap<PredictionEngineKey,PredictionEngineValue>();
    	PredictionEngineOutput predictionEngineOutput = new PredictionEngineOutput();
    	List<PredictionExplain> predictionExplain  = new ArrayList<PredictionExplain>();
		logger.info("executePrediction() start"); 
//		logger.debug("scoringStartDateType:" + scoringStartDateType);
		/*logger.debug("Executing Prediction for Location : " + predictionEngineInput.getLocationLevelId() + "-" + predictionEngineInput.getLocationId() 
					+ " and Product: "  + predictionEngineInput.getProductLevelId() + "-" + predictionEngineInput.getProductId());*/
//		logger.debug("Executing Prediction for Product: "  + predictionEngineInput.getProductLevelId() + "-" + predictionEngineInput.getProductId());
//		logger.debug("Total prices points for prediction: " + predictionEngineInput.predictionEngineItems.size() ); 
		
//		logger.debug("parallel = " + predictionEngineInput.getIsParallel());
//		logger.debug("locationLevelIdInput = " + predictionEngineInput.getCommaSeparatedLocationLevelIds());
//		logger.debug("locationIdInput = " + predictionEngineInput.getCommaSeparatedLocationIds());
		if(scoringStartDateType.equals("WEEK"))
			logger.debug("startDateInput = " + predictionEngineInput.getCommaSeparatedPredictionStartDates());
		else
			logger.debug("startDateInput = " + predictionEngineInput.getCommaSeparatedPredictionPeriodStartDates());
		logger.debug("itemCodes = " + predictionEngineInput.getCommaSeparatedItemCodes() ); 
     	logger.debug("upcs = " + predictionEngineInput.getCommaSeparatedUpcs() ); 
		logger.debug("regPrices = " + predictionEngineInput.getCommaSeparatedRegPrices() ); 
		logger.debug("regQuantities = " + predictionEngineInput.getCommaSeparatedRegQuantities());
    	logger.debug("useSubstFlag = " + predictionEngineInput.getCommaSeparatedUseSubstFlag());
		logger.debug("mainOrImpactFlag = " + predictionEngineInput.getCommaSeparatedMainOrImpactFlag());		
		logger.debug("saleQuantities = " + predictionEngineInput.getCommaSeparatedSaleQuantities());
		logger.debug("salePrices = " + predictionEngineInput.getCommaSeparatedSalePrices());
		logger.debug("adPages = " + predictionEngineInput.getCommaSeparatedAdPageNo());
		logger.debug("adBlocks = " + predictionEngineInput.getCommaSeparatedAdBlockNo());
		logger.debug("promo_type_id = " + predictionEngineInput.getCommaSeparatedPromoTypeId());
		logger.debug("display_type_id = " + predictionEngineInput.getCommaSeparatedDisplayTypeId());
		
		if(isSubstitute) {
			logger.debug("scenarioIds = " + predictionEngineInput.getCommaSeparatedSubScenarioIds());
			logger.debug("pred = " + predictionEngineInput.getCommaSeparatedPredMov());
			logger.debug("FLAG = " + predictionEngineInput.getCommaSeparatedPredStatus());
		}
		
		long tStartTime = System.currentTimeMillis();
		long tEndTime = System.currentTimeMillis();	
    	
		
		
    	try{
	        REXP x;
	        REXP explainLog;
	        //int ignoreProductIdItemCnt = Integer.parseInt(PropertyManager.getProperty("PREDICTION_IGNORE_PRODUCT_ID_ITEM_COUNT"));
			//Added on 12th Jan 2015, As part of performance improvement
			//Update Product Level Id as 1 and product id as 0 when there is lesser no of items
	        //If the promotion is predicted, don't pas product level id and product id, as promotion may have item from one category (
	        //mostly it should not happen, as it is not restricted in the front end, this is required here)
	        //Following if block pushed to be commented as prediction made mandatory to pass product level id and product id always
	        /*if(isPromotion){
	        	re.eval("productLevelIdInput <- " + 0 + ";", false);
	        }else*/ if(predictionEngineInput.getItemCodeArray().length > ignoreProductIdItemCnt){
				re.eval("productLevelIdInput <- " + predictionEngineInput.getProductLevelId() + ";", false);
				re.eval("productIdInput <- " + predictionEngineInput.getProductId() + ";", false);
			}else{
				re.eval("productLevelIdInput <- " + 0 + ";", false);
			}
			
			//re.eval("locationLevelIdInput <- " + predictionEngineInput.getLocationLevelId() + ";", false);
			//re.eval("locationIdInput <- " + predictionEngineInput.getLocationId() + ";", false);
			//re.eval("startDateInput <- '" + predictionEngineInput.getPredictionStartDateStr() + "';", false);
			//re.eval("useSubstFlag <- '" + predictionEngineInput.getUseSubstFlag() + "';", false);
			re.eval("parallel <- '" + predictionEngineInput.getIsParallel() + "';", false);
			if(isGetOnlyExplain)
				re.eval("explain_input <- " + "T" + ";", false);
			else
				re.eval("explain_input <- " + "F" + ";", false);
			
			re.assign("locationLevelIdInput", predictionEngineInput.getLocationLevelIdArray());
			re.assign("locationIdInput", predictionEngineInput.getLocationIdArray());
			//re.assign("startDateInput", predictionEngineInput.getPredictionStartDateArray());
			//For tops scoring is for each week and for ahold the scoring is for each period
			if(scoringStartDateType.equals("WEEK"))
				re.assign("startDateInput", predictionEngineInput.getPredictionStartDateArray());
			else
				re.assign("startDateInput", predictionEngineInput.getPredictionPeriodStartDateArray());
				
//			logger.debug("Dates:" + re.eval("startDateInput"));
			re.assign("itemCodes", predictionEngineInput.getItemCodeArray());
			//logger.debug("Assigned itemCodes"); 
			re.assign("upcs", predictionEngineInput.getUPCArray());
			//logger.debug("Assigned upcs");
			re.assign("regPrices", predictionEngineInput.getRegPriceArray());
			//logger.debug("Assigned regPrices");
			re.assign("regQuantities", predictionEngineInput.getRegQuantityArray());
			//logger.debug("Assigned regQuanities");
			re.assign("useSubstFlag", predictionEngineInput.getUseSubstFlagArray());
			//logger.debug("Assigned useSubstFlag");
			re.assign("mainOrImpactFlag", predictionEngineInput.getMainOrImpactFlagArray());
			//logger.debug("Assigned mainOrImpactFlag");
			
//			logger.debug("productLevelIdInput: " + re.eval("productLevelIdInput") + " ,productIdInput: " + re.eval("productIdInput")
//					+ " ,locationLevelIdInput: " + re.eval("locationLevelIdInput") + " ,locationIdInput: " + re.eval("locationIdInput") 
//					+ " ,startDateInput: " + re.eval("startDateInput") + " ,useSubstFlag: " + re.eval("useSubstFlag"));
			
			re.assign("salePrices", predictionEngineInput.getSalePriceArray());
			re.assign("saleQuantities", predictionEngineInput.getSaleQuantityArray());
			re.assign("adPages", predictionEngineInput.getAdPageNoArray());
			re.assign("adBlocks", predictionEngineInput.getAdBlockNoArray());
			re.assign("promo_type_id", predictionEngineInput.getPromoTypeIdArray());
			re.assign("display_type_id", predictionEngineInput.getDisplayTypeIdArray());
			
			if(isSubstitute) {
				re.assign("scenarioIds", predictionEngineInput.getSubsScenarioIdArray());
				re.assign("pred", predictionEngineInput.getPredMovArray());
				re.assign("FLAG", predictionEngineInput.getPredStatusArray());
			}
			
			
//			logger.debug("productLevelIdInput: " + re.eval("productLevelIdInput") + " ,productIdInput: "
//					+ re.eval("productIdInput") + " ,locationLevelIdInput: " + re.eval("locationLevelIdInput")
//					+ " ,locationIdInput: " + re.eval("locationIdInput") + " ,startDateInput: "
//					+ re.eval("startDateInput") + " ,useSubstFlag: " + re.eval("useSubstFlag") + " ,itemCodes: "
//					+ re.eval("itemCodes") + " ,upc:" + re.eval("upcs") + " ,regPrices:" + re.eval("regPrices")
//					+ " ,regQuantities: " + re.eval("regQuantities") + " ,useSubstFlag: " + re.eval("useSubstFlag")
//					+ ", mainOrImpactFlag: " + re.eval("mainOrImpactFlag") + " ,salePrices:" + re.eval("salePrices")
//					+ " ,saleQuantities: " + re.eval("saleQuantities") + " ,adPages:" + re.eval("adPages")
//					+ " ,adBlocks:" + re.eval("adBlocks") + " ,promo_type_id:" + re.eval("promo_type_id")
//					+ " ,display_type_id:" + re.eval("display_type_id"));
			
//			logger.debug("productLevelIdInput: " + re.eval("productLevelIdInput") + " ,productIdInput: " + re.eval("productIdInput")
//					+ " ,parallel: " + re.eval("parallel"));
			
			//source script
			logger.debug("R Script path: " + rScriptRootPath + rScriptSourcePath); 
			re.assign("root.dir", rScriptRootPath);
			re.assign("model.path", modelPath);
			re.assign("log.path", logPath);
			tStartTime = System.currentTimeMillis();
			re.eval("source('" + rScriptRootPath + rScriptSourcePath + "')", false);
		    tEndTime = System.currentTimeMillis();
			logger.info("executePrediction() - Time taken to predict: "
					+ PrestoUtil.getTimeTakenInMins(tStartTime, tEndTime));
		    
		    
		    //re.eval("output <- score(categoryId,zoneId,startDate,itemCodes,upcs,regPrices,regQuantities)", false);
		    //re.eval("output <- score()", false);
//		    Calendar cal = Calendar.getInstance();
//	        SimpleDateFormat sdf = new SimpleDateFormat("HH-mm-ss");
		    
//			re.eval("write.csv(ret_api[['predictions']], file='F:/Application/Deployment/TOPS/Deployment/App"
//					+ "/BatchServices/OfferManagement/op_" + sdf.format(cal.getTime()) + ".csv')");
	        
			//re.eval("write.csv(ret_api[['predictions']], file='B:/PrestoTops/APP/BatchServices/DataLoad/OfferManagement"
					//+ "/prediction_output_temp/op_" + sdf.format(cal.getTime()) + ".csv')");
		    
			
		    x = re.eval("ret_api[['prediction']]");
		    
//		    logger.debug("x:" + x);
		    	 
		    if (x == null) throw new PredictionException("null object returned from R");
			RList l = x.asList();
//			 logger.debug("l:" + x);
		    if (l == null) throw new PredictionException("null list returned");
		    
//			logger.debug("Converting location level id to Int array");
			double[] locLvlIds = l.at("LOCATION_LEVEL_ID").asDoubleArray();	// get location level id
//			logger.debug("LOCATION_LEVEL_ID:" + locLvlIds.length);
			
//			logger.debug("Converting location id to Int array");
			double[] locIds = l.at("LOCATION_ID").asDoubleArray();	// get location id
//			logger.debug("LOCATION_ID:" + locIds.length);
			
//		    logger.debug("Converting upc to double array"); 
			//double[] upcs = l.at("UPC").asDoubleArray(); //get upcs
//			logger.debug("UPC:" + upcs.length);
			
			//logger.debug("Converting MIN_PRICE to double array");
			//double[] minPrices = l.at("MIN_PRICE").asDoubleArray(); // get minPrices
			//logger.debug("MIN_PRICE:" + locLvlIds.length);
			
//			logger.debug("Converting pred to double array");
			double[] predictedMovments = l.at("pred").asDoubleArray();	// get predicted movements
//			logger.debug("pred:" + predictedMovments.length);
			
//			try {
//				logger.debug("Converting confidenceLevelLower to double array");
				double[] confidenceLevelLower = null;
				if(l.at("pred_lower") != null) {
					confidenceLevelLower = l.at("pred_lower").asDoubleArray();
//					logger.debug("pred_lower:" + confidenceLevelLower.length);
				}
				//double[] confidenceLevelLower = l.at("pred_lower").asDoubleArray();
				
//			} catch (Exception ex) {
//			}
//			try {
//				logger.debug("Converting confidenceLevelUpper to double array");
			double[] confidenceLevelUpper = null;
			if (l.at("pred_upper") != null) {
				confidenceLevelUpper = l.at("pred_upper").asDoubleArray();
//				logger.debug("pred_upper:" + confidenceLevelUpper.length);
			}
//			} catch (Exception ex) {
//			}
//			try {
//				logger.debug("Converting FLAG to Int array");
				double[] statusCodes = l.at("FLAG").asDoubleArray(); 
//				logger.debug("FLAG:" + statusCodes.length);
//			} catch (Exception ex) {
//			}
//			try {
				double[] minImpact = l.at("MIN_IMPACT").asDoubleArray();
//				logger.debug("MIN_IMPACT:" + minImpact.length);
//			} catch (Exception ex) {
//			}
//			try {
				double[] maxImpact = l.at("MAX_IMPACT").asDoubleArray();
//				logger.debug("MAX_IMPACT:" + maxImpact.length);
//			} catch (Exception ex) {
//			}
//			try {
//				logger.debug("Converting ITEM_CODE to Int array");
				//double[] itemCodes = l.at("ITEM_CODE").asDoubleArray();
				//Nagaraj::Changed on 25th Nov 2015 to reflect prediction version 3.4 changes
				double[] itemCodes = l.at("PRESTO_ITEM_CODE").asDoubleArray();
//				logger.debug("ITEM_CODE:" + itemCodes.length);
//			} catch (Exception ex) {
//			}
//			try {
//				logger.debug("Converting REG_QUANTITY to Int array");
				double[] regQuantities = l.at("REG_QUANTITY").asDoubleArray();
//				logger.debug("REG_QUANTITY:" + regQuantities.length);
//			} catch (Exception ex) {
//			}
//			try {
				logger.debug("Converting REG_PRICE to Int array");
				double[] regPrices = l.at("REG_PRICE").asDoubleArray();
//				logger.debug("REG_PRICE:" + regPrices.length);
//			} catch (Exception ex) {
//			}
//			try {
//				logger.debug("Converting SALE_QUANTITY to Int array");
				double[] saleQuantities = l.at("SALE_QUANTITY").asDoubleArray();
//				logger.debug("SALE_QUANTITY:" + saleQuantities.length);
//			} catch (Exception ex) {
//			}
//			try {
//				logger.debug("Converting SALE_PRICE to Int array");
				double[] salePrices = l.at("SALE_PRICE").asDoubleArray();
//				logger.debug("SALE_PRICE:" + salePrices.length);
//			} catch (Exception ex) {
//			}
//			try {
//				logger.debug("Converting AD_PAGE_NO to Int array");
				double[] pageNos = l.at("AD_PAGE_NO").asDoubleArray();
//				logger.debug("AD_PAGE_NO:" + pageNos.length);
//			} catch (Exception ex) {
//			}
//			try {
//				logger.debug("Converting AD_BLOCK_NO to Int array");
				double[] blockNos = l.at("AD_BLOCK_NO").asDoubleArray();
//				logger.debug("AD_BLOCK_NO:" + blockNos.length);
//			} catch (Exception ex) {
//			}
//			try {
//				logger.debug("Converting PROMO_TYPE_ID to Int array");
				double[] promoTypeIds = l.at("PROMO_TYPE_ID").asDoubleArray();
//				logger.debug("PROMO_TYPE_ID:" + promoTypeIds.length);
//			} catch (Exception ex) {
//			}
//			try {
//				logger.debug("Converting DISPLAY_TYPE_ID to Int array");
				double[] displayTypeIds = l.at("DISPLAY_TYPE_ID").asDoubleArray();
//				logger.debug("DISPLAY_TYPE_ID:" + displayTypeIds.length);
//			} catch (Exception ex) {
//			}
				
			double[] predBeforeSubsAdj = null;	
			if (isSubstitute) {
//				logger.debug("Converting pred_before_subs_adj to Double array");
				predBeforeSubsAdj = l.at("pred_before_subs_adj").asDoubleArray();
//				logger.debug("pred_before_subs_adj:" + predBeforeSubsAdj.length);
				
			}
			
			String minImp = "", maxImp = "";
			double predictedMov, confLevelLower = 0, confLevelUpper = 0, predictedMovBeforeSubAdj = 0;
			for (int i = 0; i < itemCodes.length; i++) {				
//				logger.debug("locLvlIds:" + locLvlIds[i] + "locIds:" + locIds[i] + "itemCodes:" + itemCodes[i] + ",regQuantities:" + regQuantities[i]
//						+ ",regPrices:" + regPrices[i] + ",saleQuantities:" + saleQuantities[i] + ",salePrices:" + salePrices[i] + ",pageNos:"
//						+ pageNos[i] + ",blockNos:" + blockNos[i] + ",promoTypeIds:" + promoTypeIds[i] + ",displayTypeIds:" + displayTypeIds[i]
//						+ ",statusCodes:" + statusCodes[i] + ",predictedMovments:" + predictedMovments[i] + ",predBeforeSubsAdj:"
//						+ (isSubstitute ? predBeforeSubsAdj[i] : 0));

				PredictionEngineKey key = new PredictionEngineKey((int) locLvlIds[i], (int) locIds[i], (int)itemCodes[i], 
						(int)regQuantities[i], regPrices[i], (int)saleQuantities[i], salePrices[i], (int)pageNos[i],
						(int)blockNos[i], (int) promoTypeIds[i], (int)displayTypeIds[i]);

				PredictionStatus predictionStatus = (predictedMovments[i] < 0 && 
						PredictionStatus.get((int)statusCodes[i]) == PredictionStatus.SUCCESS) ? PredictionStatus.UNDEFINED
								: PredictionStatus.get((int)statusCodes[i]);	

				minImp = (minImpact[i] == -999 ? PRConstants.NOT_APPLICABLE : PRFormatHelper.doubleToTwoDigitString(minImpact[i] * 100));
				maxImp = (maxImpact[i] == -999 ? PRConstants.NOT_APPLICABLE : PRFormatHelper.doubleToTwoDigitString(maxImpact[i] * 100));

				predictedMov = PRFormatHelper.roundToTwoDecimalDigitAsDouble(predictedMovments[i]);
				//17th July 2017, these 2 variables are removed from the output of prediction engine
				confLevelLower = PRFormatHelper.roundToTwoDecimalDigitAsDouble(confidenceLevelLower != null ? confidenceLevelLower[i] : 0);
				confLevelUpper = PRFormatHelper.roundToTwoDecimalDigitAsDouble(confidenceLevelUpper != null ? confidenceLevelUpper[i] : 0);
				if(isSubstitute) {
					predictedMovBeforeSubAdj = PRFormatHelper.roundToTwoDecimalDigitAsDouble(predBeforeSubsAdj[i]);	
				}
				
//				PredictionEngineValue value = new PredictionEngineValue(predictedMovments[i], predictionStatus, minImp,
//						maxImp, confidenceLevelLower[i], confidenceLevelUpper[i]);
				
				PredictionEngineValue value = new PredictionEngineValue(predictedMov, predictionStatus, minImp,
						maxImp, confLevelLower, confLevelUpper, predictedMovBeforeSubAdj);
				
				predictionMap.put(key, value);
			}
			explainLog = re.eval("ret_api[['explain_new']]");
			predictionExplain = getPredictionExplain(explainLog);
			 
			predictionEngineOutput.setPredictionMap(predictionMap);
			predictionEngineOutput.predictionExplain = predictionExplain;
			logger.info("executePrediction() end");
			return predictionEngineOutput;			
		} catch (Exception e) {
			//logger.error("Error while executing prediction engine: " + e.getMessage(),e); 
			throw new PredictionException(e);
		}finally{
			System.gc();
		}
    }

	private List<PredictionExplain> getPredictionExplain(REXP explainLog) {
		List<PredictionExplain> predictionExplainCol = new ArrayList<PredictionExplain>();
		PredictionExplain predictionExplain = new PredictionExplain();
		try {
			if (explainLog != null) {
				RList explainColumns = explainLog.asList();
				if (explainColumns != null) {
					double[] expUPC = explainColumns.at("RET_LIR_ID_OR_ITEM_CODE").asDoubleArray();
//					logger.debug("RET_LIR_ID_OR_ITEM_CODE length:" + expUPC.length);
					double[] expIsLIG = explainColumns.at("IS_LIG").asDoubleArray();
//					logger.debug("IS_LIG length:" + expIsLIG.length);
					double[] expRegPrice = explainColumns.at("REG_PRICE").asDoubleArray();
//					logger.debug("REG_PRICE length:" + expRegPrice.length);
					double[] expSalePrice = explainColumns.at("SALE_PRICE").asDoubleArray();
//					logger.debug("SALE_PRICE length:" + expSalePrice.length);
					double[] expMinPrice = explainColumns.at("MIN_PRICE").asDoubleArray();
//					logger.debug("MIN_PRICE length:" + expMinPrice.length);
					double[] expPage = explainColumns.at("PAGE").asDoubleArray();
//					logger.debug("PAGE length:" + expPage.length);
					double[] expAd = explainColumns.at("AD").asDoubleArray();
//					logger.debug("AD length:" + expAd.length);
					String[] expDisplayClean = explainColumns.at("DISPLAY_CLEAN").asStringArray();
//					logger.debug("DISPLAY_CLEAN length:" + expDisplayClean.length);
					double[] expNoOfIns = explainColumns.at("NO_INSTANCES").asDoubleArray();
//					logger.debug("No_INSTANCES length:" + expNoOfIns.length);
					double[] expAvgMov = explainColumns.at("AVG_MOVEMENT").asDoubleArray();
//					logger.debug("AVG_MOVEMENT length:" + expAvgMov.length);
					String[] expLastObs =  explainColumns.at("LAST_OBSERVED").asStringArray();
//					logger.debug("LAST_OBSERVED length:" + expLastObs.length);
					
					for (int i = 0; i < expUPC.length; i++) {
//						logger.debug("RET_LIR_ID_OR_ITEM_CODE:" + expUPC[i] + ",IS.IS_LIG:" + expIsLIG[i] + ",REG_PRICE:" + expRegPrice[i]
//								+ ",SALE_PRICE:" + expSalePrice[i] + ",MIN_PRICE:" + expMinPrice[i] + ",PAGE:"
//								+ expPage[i] + ",AD:" + expAd[i] + ",DISPLAY_CLEAN:" + expDisplayClean[i] 
//								+ ",No.Instances:" + expNoOfIns[i] + ",AVG_MOVEMENT:" + expAvgMov[i]
//								+ ",LAST_OBSERVED:" + expLastObs[i]);
//						
						predictionExplain = new PredictionExplain();
						predictionExplain.setLirIdOrItemCode((int) expUPC[i]);
						predictionExplain.setIsLIG((int) expIsLIG[i]);
						predictionExplain.setRegPrice(expRegPrice[i]);
						predictionExplain.setSalePrice(expSalePrice[i]);
						predictionExplain.setMinPrice(expMinPrice[i]);
						predictionExplain.setPage((int) expPage[i]);
						predictionExplain.setAd((int) expAd[i]);
						predictionExplain.setDisplay(expDisplayClean[i]);
						predictionExplain.setTotalInstances((int)expNoOfIns[i]);
						predictionExplain.setAvgMov(Math.round(expAvgMov[i]));
						predictionExplain.setLastObservedDate(expLastObs[i]);
						
						predictionExplainCol.add(predictionExplain);
					}
				} else {
					logger.debug("Explain columns are null");
				}
			} else {
				logger.debug("Explain log is null");
			}
		} catch (Exception ex) {
			logger.error("Error while fetching Prediction Explain" + ex.toString());
		}
		
		return predictionExplainCol;
	}
    
//	public static void main(String[] args) {
//    	try{
//    		PredictionEngineInput pei = initializePredictionEngineInput();
//	    	PredictionEngineBatchImpl p = new PredictionEngineBatchImpl("D:/WorkspaceR/code/api/api_java.R");
//	    	PredictionEngineOutput peo = p.predict(pei);
//	    	System.out.print(peo);
//    	}catch(PredictionException pe)
//    	{
//			System.out.println("EX:"+pe.getMessage());
//			//pe.printStackTrace();
//    	}
//    }

//	private static PredictionEngineInput initializePredictionEngineInput() {
//		PredictionEngineInput pi = new PredictionEngineInput();
//		pi.setProductLevelId(4); //category level
//		pi.setProductId(263); // category id
//		pi.setLocationLevelId(6); // zone level
//		pi.setLocationId(66); // zone id
//		pi.setPredictionStartDateStr("2014-03-16"); //start day of week i.e. sunday
//		
//		List<PredictionEngineItem> itemList = new ArrayList<PredictionEngineItem>();
//		itemList.add(new PredictionEngineItem(1, 173643, 980080005, 1.59, 1, Constants.NO, true, false));
//		itemList.add(new PredictionEngineItem(1, 180184, 980089220, 8.49, 1, Constants.NO, true, false));
//		itemList.add(new PredictionEngineItem(1, 51539, 980089500, 3.39, 1, Constants.NO, true, false));
//		pi.setPredictionEngineItems(itemList);
//	
//		return pi;
//	}	

}
