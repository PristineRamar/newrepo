package com.pristine.service.offermgmt.prediction;

import java.util.List;

import com.pristine.dto.offermgmt.multiWeekPrediction.MultiWeekPredEngineItemDTO;
import com.pristine.dto.offermgmt.mwr.CLPDLPPredictionDTO;

public interface PredictionEngine {

	// public PredictionEngineOutput predictMovement(PredictionEngineInput predictionEngineInput);
	// public PredictionEngineOutput predict(PredictionEngineInput predictionEngineInput, boolean isGetOnlyExplain,
	// boolean isPromotion) throws PredictionException;
	public PredictionEngineOutput weeklyPrediction(PredictionEngineInput predictionEngineInput, boolean isGetOnlyExplain, boolean isPromotion,
			boolean isSubstitute) throws PredictionException;

	public List<MultiWeekPredEngineItemDTO> multiWeekPrediction(List<MultiWeekPredEngineItemDTO> multiWeekPredEngineItemDTOInputCol)
			throws PredictionException;
	
	public List<CLPDLPPredictionDTO> clpDlpPrediction(List<CLPDLPPredictionDTO> clpDlpPredictionInput)
			throws PredictionException;

}
