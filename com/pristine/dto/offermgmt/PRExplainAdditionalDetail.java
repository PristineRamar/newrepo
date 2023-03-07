package com.pristine.dto.offermgmt;

import java.util.List;

import com.fasterxml.jackson.annotation.JsonProperty;

public class PRExplainAdditionalDetail {

	@JsonProperty("n-t-i")
	private int explainRetailNoteTypeLookupId;
	
	@JsonProperty("n-v")
	private List<String> noteValues;

	public int getExplainRetailNoteTypeLookupId() {
		return explainRetailNoteTypeLookupId;
	}

	public void setExplainRetailNoteTypeLookupId(int explainRetailNoteTypeLookupId) {
		this.explainRetailNoteTypeLookupId = explainRetailNoteTypeLookupId;
	}

	public List<String> getNoteValues() {
		return noteValues;
	}

	public void setNoteValues(List<String> noteValues) {
		this.noteValues = noteValues;
	}
	
	
}
