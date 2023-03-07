package com.pristine.dto.offermgmt;

public class CriteriaDTO {
	private int criteriaId;
	private int criteriaDetailId;
	private int criteriaTypeId;
	private String oprertorText;
	private String valueType;
	private double minValue;
	private double maxValue;
	private String value;

	public int getCriteriaId() {
		return criteriaId;
	}
	public void setCriteriaId(int criteriaId) {
		this.criteriaId = criteriaId;
	}
	public int getCriteriaDetailId() {
		return criteriaDetailId;
	}
	public void setCriteriaDetailId(int criteriaDetailId) {
		this.criteriaDetailId = criteriaDetailId;
	}
	public int getCriteriaTypeId() {
		return criteriaTypeId;
	}
	public void setCriteriaTypeId(int criteriaTypeId) {
		this.criteriaTypeId = criteriaTypeId;
	}
	public String getOprertorText() {
		return oprertorText;
	}
	public void setOprertorText(String oprertorText) {
		this.oprertorText = oprertorText;
	}
	public String getValueType() {
		return valueType;
	}
	public void setValueType(String valueType) {
		this.valueType = valueType;
	}
	public double getMinValue() {
		return minValue;
	}
	public void setMinValue(double minValue) {
		this.minValue = minValue;
	}
	public double getMaxValue() {
		return maxValue;
	}
	public void setMaxValue(double maxValue) {
		this.maxValue = maxValue;
	}
	public String getValue() {
		return value;
	}
	public void setValue(String value) {
		this.value = value;
	}
}
