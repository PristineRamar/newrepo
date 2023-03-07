package com.pristine.dto.offermgmt;

public class PRConstraintLIG  implements  Cloneable {
	private long cLIGId;
	private long cId;
	private char value;
	private String ligConstraintText;
	public long getcLIGId() {
		return cLIGId;
	}
	public void setcLIGId(long cLIGId) {
		this.cLIGId = cLIGId;
	}
	public long getcId() {
		return cId;
	}
	public void setgId(long cId) {
		this.cId = cId;
	}
	public char getValue() {
		return value;
	}
	public void setValue(char value) {
		this.value = value;
	}
	@Override
    protected Object clone() throws CloneNotSupportedException {
        return super.clone();
    }
	public String getLigConstraintText() {
		return ligConstraintText;
	}
	public void setLigConstraintText(String ligConstraintText) {
		this.ligConstraintText = ligConstraintText;
	}
}
