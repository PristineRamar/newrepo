package com.pristine.dto.offermgmt.audittool;

public class AuditParameterDTO {
	private long parametersId;
	private long paramHeaderId;
	private String paramsType;
	private String valueType;
	private double parameterValue;
	private long apVerId;
	public long getApVerId() {
		return apVerId;
	}
	public void setApVerId(long apVerId) {
		this.apVerId = apVerId;
	}
	public long getParametersId() {
		return parametersId;
	}
	public void setParametersId(long parametersId) {
		this.parametersId = parametersId;
	}
	public long getParamHeaderId() {
		return paramHeaderId;
	}
	public void setParamHeaderId(long paramHeaderId) {
		this.paramHeaderId = paramHeaderId;
	}
	public String getParamsType() {
		return paramsType;
	}
	public void setParamsType(String paramsType) {
		this.paramsType = paramsType;
	}
	public String getValueType() {
		return valueType;
	}
	public void setValueType(String valueType) {
		this.valueType = valueType;
	}
	public double getParameterValue() {
		return parameterValue;
	}
	public void setParameterValue(double parameterValue) {
		this.parameterValue = parameterValue;
	}
}
