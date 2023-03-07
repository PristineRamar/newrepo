package com.pristine.dto.offermgmt;

public class PRGuidelineCompDetail implements Cloneable {
	private long guidelineCompDetailId;	
	private int compStrId; 
	private char valueType;
	private double minValue;
	private double maxValue;	
	private int relationalOperatorId;
	private String relationalOperatorText;
	private boolean isExclude;
	private String guidelineText;
	
	public long getGuidelineCompDetailId() {
		return guidelineCompDetailId;
	}
	public void setGuidelineCompDetailId(long guidelineCompDetailId) {
		this.guidelineCompDetailId = guidelineCompDetailId;
	}
	public int getCompStrId() {
		return compStrId;
	}
	public void setCompStrId(int compStrId) {
		this.compStrId = compStrId;
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
	public int getRelationalOperatorId() {
		return relationalOperatorId;
	}
	public void setRelationalOperatorId(int relationalOperatorId) {
		this.relationalOperatorId = relationalOperatorId;
	}
	public String getRelationalOperatorText() {
		return relationalOperatorText;
	}
	public void setRelationalOperatorText(String relationalOperatorText) {
		this.relationalOperatorText = relationalOperatorText;
	}
	public boolean getIsExclude() {
		return isExclude;
	}
	public void setIsExclude(boolean isExclude) {
		this.isExclude = isExclude;
	}
	
	public String getGuidelineText() {
		return guidelineText;
	}
	public void setGuidelineText(String guidelineText) {
		this.guidelineText = guidelineText;
	}
	
	@Override
	public String toString() {
		return "PRGuidelineCompDetail [compStrId=" + compStrId + ", valueType=" + valueType + ", minValue=" + minValue + ", maxValue="
				+ maxValue + ", relationalOperatorText=" + relationalOperatorText + "]";
	}
	
	@Override
    protected Object clone() throws CloneNotSupportedException {
		return super.clone();
    }
	
}
