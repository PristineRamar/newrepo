package com.pristine.service.offermgmt.prediction;

import java.util.HashMap;
import java.util.List;

public class PredictionEngineOutput {

	HashMap<PredictionEngineKey,PredictionEngineValue> predictionMap;
	List<PredictionExplain> predictionExplain;
	
	public PredictionEngineOutput() {
		super();
	}

	public PredictionEngineOutput(
			HashMap<PredictionEngineKey, PredictionEngineValue> predictionMap, List<PredictionExplain> predictionExplain) {
		super();
		this.predictionMap = predictionMap;
		this.predictionExplain = predictionExplain;
	}

	public HashMap<PredictionEngineKey, PredictionEngineValue> getPredictionMap() {
		return predictionMap;
	}

	public void setPredictionMap(
			HashMap<PredictionEngineKey, PredictionEngineValue> predictionMap) {
		this.predictionMap = predictionMap;
	}
}
