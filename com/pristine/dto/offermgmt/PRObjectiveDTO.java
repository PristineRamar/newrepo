package com.pristine.dto.offermgmt;

import java.io.Serializable;

//public class PRObjectiveDTO implements Serializable, Cloneable{
	public class PRObjectiveDTO implements  Cloneable{
	private int objectiveTypeId;
	private String objectiveTypeName;
	private double minObjVal;
	private double maxObjVal;
	private double targetObjectiveValue;
	private char targetObjectiveValueType;
	public int getObjectiveTypeId() {
		return objectiveTypeId;
	}
	public void setObjectiveTypeId(int objectiveTypeId) {
		this.objectiveTypeId = objectiveTypeId;
	}
	public String getObjectiveTypeName() {
		return objectiveTypeName;
	}
	public void setObjectiveTypeName(String objectiveTypeName) {
		this.objectiveTypeName = objectiveTypeName;
	}
	public double getMinObjVal() {
		return minObjVal;
	}
	public void setMinObjVal(double minObjVal) {
		this.minObjVal = minObjVal;
	}
	public double getMaxObjVal() {
		return maxObjVal;
	}
	public void setMaxObjVal(double maxObjVal) {
		this.maxObjVal = maxObjVal;
	}
	
	@Override
    protected Object clone() throws CloneNotSupportedException {
		return super.clone();
    }
	public double getTargetObjectiveValue() {
		return targetObjectiveValue;
	}
	public void setTargetObjectiveValue(double targetObjectiveValue) {
		this.targetObjectiveValue = targetObjectiveValue;
	}
	public char getTargetObjectiveValueType() {
		return targetObjectiveValueType;
	}
	public void setTargetObjectiveValueType(char targetObjectiveValueType) {
		this.targetObjectiveValueType = targetObjectiveValueType;
	}
}
