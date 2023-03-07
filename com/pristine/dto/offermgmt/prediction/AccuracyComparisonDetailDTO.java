package com.pristine.dto.offermgmt.prediction;

public class AccuracyComparisonDetailDTO {
	private long comparisonHeaderId;
	private int comparisonParamId;
	private double parameterValue;
	private String parameterValueText;
	private int orderNo;
	private String versionType;
	
	public long getComparisonHeaderId() {
		return comparisonHeaderId;
	}
	public void setComparisonHeaderId(long comparisonHeaderId) {
		this.comparisonHeaderId = comparisonHeaderId;
	}
	public int getComparisonParamId() {
		return comparisonParamId;
	}
	public void setComparisonParamId(int comparisonParamId) {
		this.comparisonParamId = comparisonParamId;
	}
	public double getParameterValue() {
		return parameterValue;
	}
	public void setParameterValue(double parameterValue) {
		this.parameterValue = parameterValue;
	}
	public String getParameterValueText() {
		return parameterValueText;
	}
	public void setParameterValueText(String parameterValueText) {
		this.parameterValueText = parameterValueText;
	}
	public int getOrderNo() {
		return orderNo;
	}
	public void setOrderNo(int orderNo) {
		this.orderNo = orderNo;
	}
	public String getVersionType() {
		return versionType;
	}
	public void setVersionType(String versionType) {
		this.versionType = versionType;
	}
}
