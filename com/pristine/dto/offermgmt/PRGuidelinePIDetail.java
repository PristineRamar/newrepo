package com.pristine.dto.offermgmt;

public class PRGuidelinePIDetail implements Cloneable {
	private long guidelinePIDetailId;
	private int guidelinePIId;
	private int locationLevelId;
	private int locationId;
	private char valueType;
	private double actualValue;
	private boolean isExclude;
	
	public long getGuidelinePIDetailId() {
		return guidelinePIDetailId;
	}
	public void setGuidelinePIDetailId(long guidelinePIDetailId) {
		this.guidelinePIDetailId = guidelinePIDetailId;
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
	public char getValueType() {
		return valueType;
	}
	public void setValueType(char valueType) {
		this.valueType = valueType;
	}
	public double getActualValue() {
		return actualValue;
	}
	public void setActualValue(double actualValue) {
		this.actualValue = actualValue;
	}
	public boolean isExclude() {
		return isExclude;
	}
	public void setExclude(boolean isExclude) {
		this.isExclude = isExclude;
	}

	@Override
    protected Object clone() throws CloneNotSupportedException {
		return super.clone();
    }
	public int getGuidelinePIId() {
		return guidelinePIId;
	}
	public void setGuidelinePIId(int guidelinePIId) {
		this.guidelinePIId = guidelinePIId;
	}
}
