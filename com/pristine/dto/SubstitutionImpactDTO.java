package com.pristine.dto;

public class SubstitutionImpactDTO {
	
	private int mainProductId;
	private int mainProductLevelId;
	private int subsProductId;
	public int getMainProductId() {
		return mainProductId;
	}
	public void setMainProductId(int mainProductId) {
		this.mainProductId = mainProductId;
	}
	public int getMainProductLevelId() {
		return mainProductLevelId;
	}
	public void setMainProductLevelId(int mainProductLevelId) {
		this.mainProductLevelId = mainProductLevelId;
	}
	public int getSubsProductId() {
		return subsProductId;
	}
	public void setSubsProductId(int subsProductId) {
		this.subsProductId = subsProductId;
	}
	public int getSubsProductLevelId() {
		return subsProductLevelId;
	}
	public void setSubsProductLevelId(int subsProductLevelId) {
		this.subsProductLevelId = subsProductLevelId;
	}
	public float getMinImpact() {
		return minImpact;
	}
	public void setMinImpact(float minImpact) {
		this.minImpact = minImpact;
	}
	public float getMaxImpact() {
		return maxImpact;
	}
	public void setMaxImpact(float maxImpact) {
		this.maxImpact = maxImpact;
	}
	private int subsProductLevelId;
	private float minImpact;
	private float maxImpact;

}
