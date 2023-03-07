package com.pristine.service.offermgmt.multiWeekPrediction;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.apache.log4j.Logger;
import com.pristine.dto.offermgmt.multiWeekPrediction.MultiWeekPredEngineItemDTO;
import com.pristine.dto.offermgmt.multiWeekPrediction.MultiWeekPredHeaderAPIDTO;
import com.pristine.dto.offermgmt.multiWeekPrediction.MultiWeekPredItemAPIDTO;
import com.pristine.dto.offermgmt.multiWeekPrediction.MultiWeekPredScenarioAPIDTO;
import com.pristine.dto.offermgmt.mwr.CLPDLPPredictionDTO;
import com.pristine.service.offermgmt.prediction.PredictionEngine;
import com.pristine.service.offermgmt.prediction.PredictionEngineImpl;
import com.pristine.service.offermgmt.prediction.PredictionException;
import com.pristine.util.PropertyManager;


public class MultiWeekPredictionService {
	
	private static Logger logger = Logger.getLogger("MultiWeekPredictionService");
	

	public MultiWeekPredHeaderAPIDTO onDemandPrediction(MultiWeekPredHeaderAPIDTO multiWeekPredHeaderAPIDTOInput) {
		MultiWeekPredHeaderAPIDTO multiWeekPredHeaderAPIDTOOutput = new MultiWeekPredHeaderAPIDTO();

		logger.info("onDemandPrediction() : start");
		logger.debug("Converting to prediction engine input DTO");
		// Convert to prediction engine input DTO
		List<MultiWeekPredEngineItemDTO> multiWeekPredEngineItemDTOInputCol = converAPIInputToPredEngineInput(multiWeekPredHeaderAPIDTOInput);

		if (multiWeekPredEngineItemDTOInputCol.size() > 0) {
			// Call prediction engine
			List<MultiWeekPredEngineItemDTO> multiWeekPredEngineItemDTOOutputCol = new ArrayList<MultiWeekPredEngineItemDTO>();
			try {
				multiWeekPredEngineItemDTOOutputCol = callPredictionEngine(multiWeekPredEngineItemDTOInputCol, true);
			} catch (PredictionException e) {
				e.printStackTrace();
				logger.error("Error in com.pristine.service.offermgmt.multiWeekPrediction.onDemandPrediction" + e.toString());
			}
			logger.debug("Prediction engine input DTO size is : " + multiWeekPredEngineItemDTOOutputCol.size());
			
			// Convert back to API understandable format
			multiWeekPredHeaderAPIDTOOutput = converPredEngineInputToAPIInput(multiWeekPredEngineItemDTOOutputCol);
			logger.debug("Converted back to API understandable format");
		}
		logger.info("onDemandPrediction() : end");
		return multiWeekPredHeaderAPIDTOOutput;
	}

	private List<MultiWeekPredEngineItemDTO> converAPIInputToPredEngineInput(MultiWeekPredHeaderAPIDTO multiWeekPredHeaderAPIDTOInput) {
		List<MultiWeekPredEngineItemDTO> multiWeekPredEngineItemDTOInputCol = new ArrayList<MultiWeekPredEngineItemDTO>();

		logger.info("converAPIInputToPredEngineInput() : start");
		if (multiWeekPredHeaderAPIDTOInput.getScenarios() != null && multiWeekPredHeaderAPIDTOInput.getScenarios().size() > 0) {
			// Loop each scenarios
			for (MultiWeekPredScenarioAPIDTO scenarios : multiWeekPredHeaderAPIDTOInput.getScenarios()) {
				if (scenarios.getItems() != null && scenarios.getItems().size() > 0) {
					// Loop items inside the scenario
					for (MultiWeekPredItemAPIDTO multiWeekPredItemDTO : scenarios.getItems()) {
						MultiWeekPredEngineItemDTO multiWeekPredEngineItemDTO = new MultiWeekPredEngineItemDTO();

						multiWeekPredEngineItemDTO.setLocationLevelId(multiWeekPredHeaderAPIDTOInput.getLocationLevelId());
						multiWeekPredEngineItemDTO.setLocationId(multiWeekPredHeaderAPIDTOInput.getLocationId());
						multiWeekPredEngineItemDTO.setScenarioId(scenarios.getScenarioId());
						multiWeekPredEngineItemDTO.setItemCode(multiWeekPredItemDTO.getItemCode());
						multiWeekPredEngineItemDTO.setUPC(multiWeekPredItemDTO.getUPC());
						multiWeekPredEngineItemDTO.setStartDate(multiWeekPredItemDTO.getStartDate());
						multiWeekPredEngineItemDTO.setProductLevelId(multiWeekPredItemDTO.getProductLevelId());
						multiWeekPredEngineItemDTO.setProductId(multiWeekPredItemDTO.getProductId());
						multiWeekPredEngineItemDTO.setRegMultiple(multiWeekPredItemDTO.getRegMultiple());
						multiWeekPredEngineItemDTO.setRegPrice(multiWeekPredItemDTO.getRegPrice());
						multiWeekPredEngineItemDTO.setSaleMultiple(multiWeekPredItemDTO.getSaleMultiple());
						multiWeekPredEngineItemDTO.setSalePrice(multiWeekPredItemDTO.getSalePrice());
						multiWeekPredEngineItemDTO.setPromoTypeId(multiWeekPredItemDTO.getPromoTypeId());
						multiWeekPredEngineItemDTO.setAdPageNo(multiWeekPredItemDTO.getAdPageNo());
						multiWeekPredEngineItemDTO.setAdBlockNo(multiWeekPredItemDTO.getAdBlockNo());
						multiWeekPredEngineItemDTO.setDisplayTypeId(multiWeekPredItemDTO.getDisplayTypeId());

						multiWeekPredEngineItemDTOInputCol.add(multiWeekPredEngineItemDTO);
					}
				}
			}
		}
		logger.info("converAPIInputToPredEngineInput() : end");
		return multiWeekPredEngineItemDTOInputCol;
	}

	public List<MultiWeekPredEngineItemDTO> callPredictionEngine(List<MultiWeekPredEngineItemDTO> multiWeekPredEngineItemDTOInputCol
			, boolean isOnline) throws PredictionException {
//		logger.info("callPredictionEngine() : start");
		//MultiWeekPredictionEngine multiWeekPredictionEngine = new MultiWeekPredictionEngine();
		
		String rScriptRootPath = String.valueOf(PropertyManager.getProperty("MW_PREDICTION_R_SCRIPT_PATH"));
		String rScriptSourcePath = String.valueOf(PropertyManager.getProperty("MW_PREDICTION_R_SRC_PATH"));
		
		PredictionEngine p = new PredictionEngineImpl(rScriptRootPath, rScriptSourcePath, 0, isOnline);
		
		//List<MultiWeekPredEngineItemDTO> multiWeekPredEngineItemDTOOutputCol = multiWeekPredictionEngine.predict(multiWeekPredEngineItemDTOInputCol);
		List<MultiWeekPredEngineItemDTO> multiWeekPredEngineItemDTOOutputCol = p.multiWeekPrediction(multiWeekPredEngineItemDTOInputCol);
//		logger.info("callPredictionEngine() : end");
		return multiWeekPredEngineItemDTOOutputCol;
	}

	private MultiWeekPredHeaderAPIDTO converPredEngineInputToAPIInput(List<MultiWeekPredEngineItemDTO> multiWeekPredEngineItemDTOOutputCol) {
		logger.info("converPredEngineInputToAPIInput() : start");
		MultiWeekPredHeaderAPIDTO multiWeekPredHeaderAPIDTO = new MultiWeekPredHeaderAPIDTO();
		HashMap<Integer, List<MultiWeekPredEngineItemDTO>> scenarioMap = new HashMap<Integer, List<MultiWeekPredEngineItemDTO>>();
		List<MultiWeekPredEngineItemDTO> multiWeekPredEngineItemDTOCol = null;

		try{
		
			for (MultiWeekPredEngineItemDTO multiWeekPredEngineItemDTO : multiWeekPredEngineItemDTOOutputCol) {
	
				if (scenarioMap.get(multiWeekPredEngineItemDTO.getScenarioId()) != null) {
					multiWeekPredEngineItemDTOCol = scenarioMap.get(multiWeekPredEngineItemDTO.getScenarioId());
				} else {
					multiWeekPredEngineItemDTOCol = new ArrayList<MultiWeekPredEngineItemDTO>();
				}
	
				multiWeekPredEngineItemDTOCol.add(multiWeekPredEngineItemDTO);
				scenarioMap.put(multiWeekPredEngineItemDTO.getScenarioId(), multiWeekPredEngineItemDTOCol);
			}
	
			multiWeekPredHeaderAPIDTO.setLocationLevelId(multiWeekPredEngineItemDTOOutputCol.get(0).getLocationLevelId());
			multiWeekPredHeaderAPIDTO.setLocationId(multiWeekPredEngineItemDTOOutputCol.get(0).getLocationId());
	
			List<MultiWeekPredScenarioAPIDTO> multiWeekPredScenarioAPIDTOCol = new ArrayList<MultiWeekPredScenarioAPIDTO>();
			for (Map.Entry<Integer, List<MultiWeekPredEngineItemDTO>> scenario : scenarioMap.entrySet()) {
				MultiWeekPredScenarioAPIDTO multiWeekPredScenarioAPIDTO = new MultiWeekPredScenarioAPIDTO();
				multiWeekPredScenarioAPIDTO.setScenarioId(scenario.getKey());
				List<MultiWeekPredItemAPIDTO> multiWeekPredItemAPIDTOCol = new ArrayList<MultiWeekPredItemAPIDTO>();
	
				for (MultiWeekPredEngineItemDTO multiWeekPredEngineItemDTO : scenario.getValue()) {
					MultiWeekPredItemAPIDTO multiWeekPredItemAPIDTO = new MultiWeekPredItemAPIDTO();
	
					multiWeekPredItemAPIDTO.setItemCode(multiWeekPredEngineItemDTO.getItemCode());
					multiWeekPredItemAPIDTO.setUPC(multiWeekPredEngineItemDTO.getUPC());
					multiWeekPredItemAPIDTO.setStartDate(multiWeekPredEngineItemDTO.getStartDate());
					multiWeekPredItemAPIDTO.setProductLevelId(multiWeekPredEngineItemDTO.getProductLevelId());
					multiWeekPredItemAPIDTO.setProductId(multiWeekPredEngineItemDTO.getProductId());
					multiWeekPredItemAPIDTO.setRegMultiple(multiWeekPredEngineItemDTO.getRegMultiple());
					multiWeekPredItemAPIDTO.setRegPrice(multiWeekPredEngineItemDTO.getRegPrice());
					multiWeekPredItemAPIDTO.setSaleMultiple(multiWeekPredEngineItemDTO.getSaleMultiple());
					multiWeekPredItemAPIDTO.setSalePrice(multiWeekPredEngineItemDTO.getSalePrice());
					multiWeekPredItemAPIDTO.setPromoTypeId(multiWeekPredEngineItemDTO.getPromoTypeId());
					multiWeekPredItemAPIDTO.setAdPageNo(multiWeekPredEngineItemDTO.getAdPageNo());
					multiWeekPredItemAPIDTO.setAdBlockNo(multiWeekPredEngineItemDTO.getAdBlockNo());
					multiWeekPredItemAPIDTO.setDisplayTypeId(multiWeekPredEngineItemDTO.getDisplayTypeId());
					multiWeekPredItemAPIDTO.setPredictedMovement(multiWeekPredEngineItemDTO.getPredictedMovement());
					multiWeekPredItemAPIDTO.setPredictionStatus(multiWeekPredEngineItemDTO.getPredictionStatus());
	
					multiWeekPredItemAPIDTOCol.add(multiWeekPredItemAPIDTO);
				}
				multiWeekPredScenarioAPIDTO.setItems(multiWeekPredItemAPIDTOCol);
				multiWeekPredScenarioAPIDTOCol.add(multiWeekPredScenarioAPIDTO);
			}
	
			multiWeekPredHeaderAPIDTO.setScenarios(multiWeekPredScenarioAPIDTOCol);
		
		}catch (Exception e){
			logger.info("Exception occurred while converting to API format DTO : " + e.getMessage());
		}
		logger.info("converPredEngineInputToAPIInput() : end");
		return multiWeekPredHeaderAPIDTO;
	}
	
	
	public List<CLPDLPPredictionDTO> callCLPDLPPredictionEngine(List<CLPDLPPredictionDTO> clpDlpInputs
			, boolean isOnline) throws PredictionException {
		String rScriptRootPath = String.valueOf(PropertyManager.getProperty("CLP_DLP_PREDICTION_R_SCRIPT_PATH"));
		String rScriptSourcePath = String.valueOf(PropertyManager.getProperty("CLP_DLP_PREDICTION_R_SRC_PATH"));
		
		PredictionEngine p = new PredictionEngineImpl(rScriptRootPath, rScriptSourcePath, 0, isOnline);

		List<CLPDLPPredictionDTO> multiWeekPredEngineItemDTOOutputCol = p.clpDlpPrediction(clpDlpInputs);
		return multiWeekPredEngineItemDTOOutputCol;
	}
}
