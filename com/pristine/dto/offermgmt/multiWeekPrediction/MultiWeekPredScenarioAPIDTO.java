package com.pristine.dto.offermgmt.multiWeekPrediction;

import java.util.ArrayList;
import java.util.List;

import com.fasterxml.jackson.annotation.JsonProperty;

public class MultiWeekPredScenarioAPIDTO {
	@JsonProperty("s-i")
	private int scenarioId;
	
	@JsonProperty("i")
	private List<MultiWeekPredItemAPIDTO> items = new ArrayList<MultiWeekPredItemAPIDTO>();
	
	public int getScenarioId() {
		return scenarioId;
	}
	public void setScenarioId(int scenarioId) {
		this.scenarioId = scenarioId;
	}
	public List<MultiWeekPredItemAPIDTO> getItems() {
		return items;
	}
	public void setItems(List<MultiWeekPredItemAPIDTO> items) {
		this.items = items;
	}
	
}