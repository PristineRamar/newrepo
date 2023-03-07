package com.pristine.service.offermgmt.multiWeekPrediction;

import java.util.List;

import org.apache.log4j.Logger;

import com.pristine.dto.offermgmt.multiWeekPrediction.MultiWeekPredEngineItemDTO;
import com.pristine.service.offermgmt.prediction.PredictionException;
import com.pristine.util.offermgmt.PRConstants;

public class MultiWeekPredictionThread implements Runnable{

	private static Logger logger = Logger.getLogger("MultiWeekPredictionThread");
	
	private List<MultiWeekPredEngineItemDTO> multiWeekPredInputList = null;
	private List<MultiWeekPredEngineItemDTO> multiWeekPredOutputList = null;
	private String errorMessage = null;
	private String status = null;
	private boolean threadProcessed = false;
	public MultiWeekPredictionThread(List<MultiWeekPredEngineItemDTO> multiWeekPredInputList) {
		this.multiWeekPredInputList = multiWeekPredInputList; 
	}
	
	@Override
	public void run() {

		MultiWeekPredictionService multiWeekPredictionService = new MultiWeekPredictionService();
		try {
			
			multiWeekPredictionService.callPredictionEngine(multiWeekPredInputList, true);
			
			setStatus(PRConstants.PREDICTION_STATUS_SUCCESS);
			
		} catch (PredictionException e) {
			setStatus(PRConstants.PREDICTION_STATUS_ERROR);
			setErrorMessage("Error in getting prediction - " + e.getMessage());
			logger.error("Error in getting prediction - ", e);
		}
	}

	
	public List<MultiWeekPredEngineItemDTO> getMultiWeekPredOutputList() {
		return multiWeekPredOutputList;
	}

	public void setMultiWeekPredOutputList(List<MultiWeekPredEngineItemDTO> multiWeekPredOutputList) {
		this.multiWeekPredOutputList = multiWeekPredOutputList;
	}

	public String getErrorMessage() {
		return errorMessage;
	}

	public void setErrorMessage(String errorMessage) {
		this.errorMessage = errorMessage;
	}

	public String getStatus() {
		return status;
	}

	public void setStatus(String statusCode) {
		this.status = statusCode;
	}

	public boolean isThreadProcessed() {
		return threadProcessed;
	}

	public void setThreadProcessed(boolean threadProcessed) {
		this.threadProcessed = threadProcessed;
	}
}
