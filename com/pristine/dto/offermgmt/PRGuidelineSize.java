package com.pristine.dto.offermgmt;


public class PRGuidelineSize implements Cloneable{
	private long gSizeId;
	private long gId;
	//private char value;
	private char valueType;
	private float minValue;
	private float maxValue;
	private String operatorText;
	private char htol;
	private float shelfValue;
	
	public long getgSizeId() {
		return gSizeId;
	}
	public void setgSizeId(long gSizeId) {
		this.gSizeId = gSizeId;
	}
	public long getgId() {
		return gId;
	}
	public void setgId(long gId) {
		this.gId = gId;
	}
	/*public char getValue() {
		return value;
	}
	public void setValue(char value) {
		this.value = value;
	}*/
	public char getValueType() {
		return valueType;
	}
	public void setValueType(char valueType) {
		this.valueType = valueType;
	}
	public float getMinValue() {
		return minValue;
	}
	public void setMinValue(float minValue) {
		this.minValue = minValue;
	}
	public float getMaxValue() {
		return maxValue;
	}
	public void setMaxValue(float maxValue) {
		this.maxValue = maxValue;
	}
	public String getOperatorText() {
		return operatorText;
	}
	public void setOperatorText(String operatorText) {
		this.operatorText = operatorText;
	}
	public char getHtol() {
		return htol;
	}
	public void setHtol(char htol) {
		this.htol = htol;
	}
	public float getShelfValue() {
		return shelfValue;
	}
	public void setShelfValue(float shelfValue) {
		this.shelfValue = shelfValue;
	}
	
	/*public PRRange getPriceRange(double price, int itemSize){
		PRRange range = new PRRange();
		range.setEndVal((price - 0.01) * itemSize);
		return range;
	}*/
	
	@Override
    protected Object clone() throws CloneNotSupportedException {
        return super.clone();
    }
}
