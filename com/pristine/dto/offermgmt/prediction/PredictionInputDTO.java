package com.pristine.dto.offermgmt.prediction;

import java.util.List;

import javax.xml.bind.annotation.XmlRootElement;

import com.fasterxml.jackson.annotation.JsonProperty;
import com.pristine.util.Constants;


@XmlRootElement
public class PredictionInputDTO {	
	@JsonProperty("s-c-i")
	public int startCalendarId;
	@JsonProperty("e-c-i")
	public int endCalendarId;	
	@JsonProperty("s-w-d")
	public String startWeekDate;
	@JsonProperty("e-w-d")
	public String endWeekDate;
	@JsonProperty("p-b")
	public String predictedBy;
	@JsonProperty("r-t")
	public char runType;
	@JsonProperty("i-f-p")
	public Boolean isForcePrediction = false;
	@JsonProperty("u-s-f")
	public String useSubstFlag = String.valueOf(Constants.NO);

	/* Used only in back end program. usePrediction is added not to find whether avg mov or prediction to be used for category.
	 * If the prediction is called during recommendation this property is already found and doesn't
	 * need to be found again. 
	 */
	public Boolean usePrediction = null;
	public String startPeriodDate;
	
	@JsonProperty("l-l-i")
	public int locationLevelId;
	@JsonProperty("l-i")
	public int locationId;
	@JsonProperty("l-n")
	public String locationName = "";	
	@JsonProperty("r-r-i")
	public long recommendationRunId = 0;
	@JsonProperty("p-l-i")
	public int productLevelId;
	@JsonProperty("p-i")
	public int productId;
	/*@JsonProperty("i-a-m-a-p")
	public boolean isAvgMovAlreadyPresent = false;*/
	@JsonProperty("i-g-a-m")
	public boolean isGetAvgMovement = false;
	@JsonProperty("p-items")
	public List<PredictionItemDTO> predictionItems;
	
	
	public void copy(PredictionInputDTO predictionInputDTO){
		this.recommendationRunId = predictionInputDTO.recommendationRunId;
		this.productLevelId = predictionInputDTO.productLevelId;
		this.productId = predictionInputDTO.productId;
		this.locationLevelId = predictionInputDTO.locationLevelId;
		this.locationId = predictionInputDTO.locationId;
		this.locationName = predictionInputDTO.locationName;
		this.startCalendarId = predictionInputDTO.startCalendarId;
		this.endCalendarId = predictionInputDTO.endCalendarId;
		this.startWeekDate = predictionInputDTO.startWeekDate;
		this.endWeekDate = predictionInputDTO.endWeekDate;
		this.predictedBy = predictionInputDTO.predictedBy;
		this.runType = predictionInputDTO.runType;
		this.isForcePrediction = predictionInputDTO.isForcePrediction;
		this.useSubstFlag = predictionInputDTO.useSubstFlag;
		this.predictionItems = predictionInputDTO.predictionItems;
		this.startPeriodDate = predictionInputDTO.startPeriodDate;
	}

	@Override
	public String toString() {
		return "PredictionInputDTO [recommendationRunId=" + recommendationRunId + ", productLevelId=" + productLevelId + ", productId="
				+ productId + ", locationLevelId=" + locationLevelId + ", locationId=" + locationId + ", startCalendarId="
				+ startCalendarId + ", endCalendarId=" + endCalendarId + ", startWeekDate=" + startWeekDate + ", endWeekDate="
				+ endWeekDate + ", predictedBy=" + predictedBy + ", runType=" + runType + ", isForcePrediction=" + isForcePrediction
				//+ ", isAvgMovAlreadyPresent=" + isAvgMovAlreadyPresent 
				+ ", predictionItems=" + predictionItems + "]";
	}
}
