package com.pristine.dto.offermgmt.multiWeekPrediction;

import java.util.ArrayList;
import java.util.List;

import com.fasterxml.jackson.annotation.JsonProperty;

public class MultiWeekPredHeaderAPIDTO {

	@JsonProperty("l-l-i")
	private int locationLevelId;
	@JsonProperty("l-i")
	private int locationId;
	
	@JsonProperty("s")
	private List<MultiWeekPredScenarioAPIDTO> scenarios = new ArrayList<MultiWeekPredScenarioAPIDTO>();

	public int getLocationLevelId() {
		return locationLevelId;
	}

	public void setLocationLevelId(int locationLevelId) {
		this.locationLevelId = locationLevelId;
	}

	public int getLocationId() {
		return locationId;
	}

	public void setLocationId(int locationId) {
		this.locationId = locationId;
	}

	public List<MultiWeekPredScenarioAPIDTO> getScenarios() {
		return scenarios;
	}

	public void setScenarios(List<MultiWeekPredScenarioAPIDTO> scenarios) {
		this.scenarios = scenarios;
	}

	 
}
