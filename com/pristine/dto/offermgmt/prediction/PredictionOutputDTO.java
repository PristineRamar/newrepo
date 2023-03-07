package com.pristine.dto.offermgmt.prediction;

//import java.util.ArrayList;
import java.util.List;

public class PredictionOutputDTO {
	public int locationLevelId;
	public int locationId;
	public int startCalendarId;
	public int endCalendarId;	
	public List<PredictionItemDTO> predictionItems;
	
	
	@Override
	public String toString() {
		return "PredictionOutputDTO [locationLevelId=" + locationLevelId
				+ ", locationId=" + locationId + ", startCalendarId="
				+ startCalendarId + ", endCalendarId=" + endCalendarId
				+ ", predictionItems=" + predictionItems + "]";
	}	
	
}
