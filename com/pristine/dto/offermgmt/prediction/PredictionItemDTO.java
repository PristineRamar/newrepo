package com.pristine.dto.offermgmt.prediction;

//import java.util.ArrayList;
import java.util.List;

import com.fasterxml.jackson.annotation.JsonProperty;
import com.pristine.service.offermgmt.prediction.PredictionStatus;
import com.pristine.util.Constants;

public class PredictionItemDTO {
	// NU:: 9th Feb 2017, for substitutes adjustment
	@JsonProperty("s-s-i")
	public int subsScenarioId;
	@JsonProperty("i-c-o-l-i")
	public int itemCodeOrLirId;
	@JsonProperty("u")
	public String upc;
	@JsonProperty("l-i")
	public Boolean lirInd;
	@JsonProperty("p-p")
	public List<PricePointDTO> pricePoints;
//	@JsonProperty("u-s-f")
//	public char useSubstFlag = Constants.NO;
	@JsonProperty("m-o-i-f")
	public char mainOrImpactFlag = Constants.NO;
	
	/* Used only in back end program. isAvgMovAlreadySet is added not to fetch the avg mov from the database again.
	 * If the prediction is called during recommendation avg mov is already known and doesn't
	 * need to be found again. This will be applicable for Categories & Store Items which requires Avg mov as
	 * predicted units.
	 */
	public Boolean isAvgMovAlreadySet = null;
	
	/* Used only in back end program. usePrediction is added to know whether
	 * prediction engine or avg move to be used as prediction. This will be set to true for
	 * categories which requires avg movement, store items, zone items with store recommendation,
	 * items during update prediction function(may vary from item to item)
	 * 
	 */
	public Boolean usePrediction = null;
	public Boolean isForcePrediction = false;
	
	
	// NU:: 9th Feb 2017, for substitutes adjustment, pred and flag is passed, as it will be there while calling substitute adjustment
	// so that substitute need not call the prediction again
	public double predictedMovement = 0;
	public int predictionStatus = PredictionStatus.PREDICTION_APP_EXCEPTION.getStatusCode();
	
	// NU:: 1st Mar 2018, making sure all LIG members are passed together always to prediction (LIG model)
	private int retLirId = 0;
	private boolean isInputItem = true;
	private boolean isNorecentWeeksMov;
	private boolean sendToPrediction;
	private boolean useAvgMovAsPred;
	private double AvgMovement;
	@Override
	public String toString() {
		return "PredictionItemDTO [itemCodeOrLirId=" + itemCodeOrLirId
				+ ", upc=" + upc + ", lirInd=" + lirInd + ", pricePoints="
				+ pricePoints + "]";
	}
	
	public void copy(PredictionItemDTO predictionItemDTO){
		this.itemCodeOrLirId = predictionItemDTO.itemCodeOrLirId;
		this.upc = predictionItemDTO.upc;
		this.lirInd = predictionItemDTO.lirInd;
	}

	public int getRetLirId() {
		return retLirId;
	}

	public void setRetLirId(int retLirId) {
		this.retLirId = retLirId;
	}

	public boolean isInputItem() {
		return isInputItem;
	}

	public void setInputItem(boolean isInputItem) {
		this.isInputItem = isInputItem;
	}

	public int getItemCodeOrLirId() {
		return itemCodeOrLirId;
	}

	public void setItemCodeOrLirId(int itemCodeOrLirId) {
		this.itemCodeOrLirId = itemCodeOrLirId;
	}

	public boolean isNorecentWeeksMov() {
		return isNorecentWeeksMov;
	}

	public void setNorecentWeeksMov(boolean ignorePrediction) {
		this.isNorecentWeeksMov = ignorePrediction;
	}

	public boolean isSendToPrediction() {
		return sendToPrediction;
	}

	public void setSendToPrediction(boolean isStrRevenueLess) {
		this.sendToPrediction = isStrRevenueLess;
	}

	public boolean isUseAvgMovAsPred() {
		return useAvgMovAsPred;
	}

	public void setUseAvgMovAsPred(boolean useAvgMovAspred) {
		this.useAvgMovAsPred = useAvgMovAspred;
	}

	
	public double getAvgMovement() {
		return AvgMovement;
	}

	public void setAvgMovement(double avgMovement) {
		AvgMovement = avgMovement;
	}
	
	
}
