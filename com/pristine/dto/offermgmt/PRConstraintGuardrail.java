package com.pristine.dto.offermgmt;

import java.util.ArrayList;
import java.util.List;

public class PRConstraintGuardrail implements Cloneable {
	
	private int relationalOperatorId;
	private String relationalOperatorText;
	private long guardRailCompId;	
	private long guidelineId;
	private char groupPriceType;
	private int latestPriceObservationDays;
	private boolean ignoreCompBelowCost;
	private boolean isZonePresent;

	private List<PRConstraintGuardRailDetail> competitorDetails = new ArrayList<PRConstraintGuardRailDetail>();
	
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
	
	public long getGuardRailCompId() {
		return guardRailCompId;
	}
	public void setGuardRailCompId(long guardRailCompId) {
		this.guardRailCompId = guardRailCompId;
	}
	public long getGuidelineId() {
		return guidelineId;
	}
	public void setGuidelineId(long guidelineId) {
		this.guidelineId = guidelineId;
	}
	public char getGroupPriceType() {
		return groupPriceType;
	}
	public void setGroupPriceType(char groupPriceType) {
		this.groupPriceType = groupPriceType;
	}
	public int getLatestPriceObservationDays() {
		return latestPriceObservationDays;
	}
	public void setLatestPriceObservationDays(int latestPriceObservationDays) {
		this.latestPriceObservationDays = latestPriceObservationDays;
	}
	public boolean isIgnoreCompBelowCost() {
		return ignoreCompBelowCost;
	}
	public void setIgnoreCompBelowCost(boolean ignoreCompBelowCost) {
		this.ignoreCompBelowCost = ignoreCompBelowCost;
	}
	
	public List<PRConstraintGuardRailDetail> getCompetitorDetails() {
		return competitorDetails;
	}
	public void setCompetitorDetails(List<PRConstraintGuardRailDetail> competitorDetails) {
		this.competitorDetails = competitorDetails;
	}
	
	public boolean isZonePresent() {
		return isZonePresent;
	}

	public void setZonePresent(boolean isZonePresent) {
		this.isZonePresent = isZonePresent;
	}
	@Override
	protected Object clone() throws CloneNotSupportedException {
        return super.clone();
    }
}
