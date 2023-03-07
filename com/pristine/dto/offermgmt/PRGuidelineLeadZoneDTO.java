package com.pristine.dto.offermgmt;

public class PRGuidelineLeadZoneDTO implements Cloneable{
	private long guidelineId;
	private int locationLevelId;
	private int locationId;
	private String operatorText;
	private char valueType;
	private double minValue;
	private double maxValue;
	
	public long getGuidelineId() {
		return guidelineId;
	}
	public void setGuidelineId(long guidelineId) {
		this.guidelineId = guidelineId;
	}
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
	public String getOperatorText() {
		return operatorText;
	}
	public void setOperatorText(String operatorText) {
		this.operatorText = operatorText;
	}
	public char getValueType() {
		return valueType;
	}
	public void setValueType(char valueType) {
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
	
	@Override
    protected Object clone() throws CloneNotSupportedException {
		return super.clone();
    }
}
