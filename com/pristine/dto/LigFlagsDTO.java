package com.pristine.dto;

import com.pristine.dto.offermgmt.MultiplePrice;
import com.pristine.dto.offermgmt.PRExplainLog;

public class LigFlagsDTO {

	private String isImpactIncluded;
	private int isnewPriceRecommended;
	transient private PRExplainLog explainLog = new PRExplainLog();
	private boolean isUserOverrideFlag = false;
	private int overrideRemoved = 0;
	private MultiplePrice overriddenRegularPrice = null;
	private int ispendingRetailRecommended = 0;
	private Double regRevenue;
	private Double regUnits;
	private Double regMargin;
	private Double finalPriceRevenue;
	private Double finalpredictedMovement;
	private Double finalPriceMargin;

	
	public int getIspendingRetailRecommended() {
		return ispendingRetailRecommended;
	}

	public void setIspendingRetailRecommended(int ispendingRetailRecommended) {
		this.ispendingRetailRecommended = ispendingRetailRecommended;
	}

	
	public boolean isUserOverrideFlag() {
		return isUserOverrideFlag;
	}

	public void setUserOverrideFlag(boolean isUserOverrideFlag) {
		this.isUserOverrideFlag = isUserOverrideFlag;
	}

	public int getOverrideRemoved() {
		return overrideRemoved;
	}

	public void setOverrideRemoved(int overrideRemoved) {
		this.overrideRemoved = overrideRemoved;
	}

	public MultiplePrice getOverriddenRegularPrice() {
		return overriddenRegularPrice;
	}

	public void setOverriddenRegularPrice(MultiplePrice overriddenRegularPrice) {
		this.overriddenRegularPrice = overriddenRegularPrice;
	}

	public String getIsImpactIncluded() {
		return isImpactIncluded;
	}

	public void setIsImpactIncluded(String isImpactIncluded) {
		this.isImpactIncluded = isImpactIncluded;
	}

	public int getIsnewPriceRecommended() {
		return isnewPriceRecommended;
	}

	public void setIsnewPriceRecommended(int isnewPriceRecommended) {
		this.isnewPriceRecommended = isnewPriceRecommended;
	}

	public PRExplainLog getExplainLog() {
		return explainLog;
	}

	public void setExplainLog(PRExplainLog explainLog) {
		this.explainLog = explainLog;
	}

	public Double getRegRevenue() {
		return regRevenue;
	}

	public void setRegRevenue(Double regRevenue) {
		this.regRevenue = regRevenue;
	}

	public Double getRegUnits() {
		return regUnits;
	}

	public void setRegUnits(Double regUnits) {
		this.regUnits = regUnits;
	}

	public Double getRegMargin() {
		return regMargin;
	}

	public void setRegMargin(Double regMargin) {
		this.regMargin = regMargin;
	}

	public Double getFinalPriceRevenue() {
		return finalPriceRevenue;
	}

	public void setFinalPriceRevenue(Double finalPriceRevenue) {
		this.finalPriceRevenue = finalPriceRevenue;
	}

	public Double getFinalpredictedMovement() {
		return finalpredictedMovement;
	}

	public void setFinalpredictedMovement(Double finalpredictedMovement) {
		this.finalpredictedMovement = finalpredictedMovement;
	}

	public Double getFinalPriceMargin() {
		return finalPriceMargin;
	}

	public void setFinalPriceMargin(Double finalPriceMargin) {
		this.finalPriceMargin = finalPriceMargin;
	}

}
