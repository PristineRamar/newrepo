package com.pristine.dto.offermgmt.audittool;

public class AuditParameterHeaderDTO {
	private long paramHeaderId;
	private String paramName;
	private boolean inUse;
	private long apVerId;

	public long getApVerId() {
		return apVerId;
	}
	public void setApVerId(long apVerId) {
		this.apVerId = apVerId;
	}
	public long getParamHeaderId() {
		return paramHeaderId;
	}
	public void setParamHeaderId(long paramHeaderId) {
		this.paramHeaderId = paramHeaderId;
	}
	public String getParamName() {
		return paramName;
	}
	public void setParamName(String paramName) {
		this.paramName = paramName;
	}
	public boolean isInUse() {
		return inUse;
	}
	public void setInUse(boolean inUse) {
		this.inUse = inUse;
	}
	
}
