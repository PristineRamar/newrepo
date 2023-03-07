package com.pristine.dto.offermgmt;

public class RecommendationRuleMapDTO {
	private String ruleCode;
	private Integer objectiveTypeId;
	private boolean isEnabled;
	
	public RecommendationRuleMapDTO(){
		
	}
	
	public RecommendationRuleMapDTO(String ruleCode, Integer objectiveTypeId, boolean isEnabled) {
		this.ruleCode = ruleCode;
		this.objectiveTypeId = objectiveTypeId;
		this.isEnabled = isEnabled;
	}
	
	public String getRuleCode() {
		return ruleCode;
	}
	public void setRuleCode(String ruleCode) {
		this.ruleCode = ruleCode;
	}
	public Integer getObjectiveTypeId() {
		return objectiveTypeId;
	}
	public void setObjectiveTypeId(Integer objectiveTypeId) {
		this.objectiveTypeId = objectiveTypeId;
	}
	public boolean isEnabled() {
		return isEnabled;
	}
	public void setEnabled(boolean isEnabled) {
		this.isEnabled = isEnabled;
	}
}
