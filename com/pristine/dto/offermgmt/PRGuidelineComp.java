package com.pristine.dto.offermgmt;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Comparator;
import java.util.List;

public class PRGuidelineComp  implements Cloneable{		
	private long guidelineCompId;	
	private long guidelineId;
	private int relationalOperatorId;
	private String relationalOperatorText;
	private char groupPriceType;
	private int latestPriceObservationDays;
	private boolean ignoreCompBelowCost;
	private List<PRGuidelineCompDetail> competitorDetails = new ArrayList<PRGuidelineCompDetail>();

	public long getGuidelineCompId() {
		return guidelineCompId;
	}
	public void setGuidelineCompId(long guidelineCompId) {
		this.guidelineCompId = guidelineCompId;
	}
	public long getGuidelineId() {
		return guidelineId;
	}
	public void setGuidelineId(long guidelineId) {
		this.guidelineId = guidelineId;
	}
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
	public List<PRGuidelineCompDetail> getCompetitorDetails() {
		return competitorDetails;
	}
	public void setCompetitorDetails(List<PRGuidelineCompDetail> competitorDetails) {
		this.competitorDetails = competitorDetails;
	}
	
	public void sortByCompDetailId(List<PRGuidelineCompDetail> guidelineCompDetail) {
		// Sort by comp str id, this is add to compare the output of
		// strategy from old and new function
		if (guidelineCompDetail.size() > 0) {
			Collections.sort(guidelineCompDetail, new Comparator<PRGuidelineCompDetail>() {
				public int compare(PRGuidelineCompDetail c1, PRGuidelineCompDetail c2) {
					if (c1.getGuidelineCompDetailId() < c2.getGuidelineCompDetailId())
						return 1;
					if (c1.getGuidelineCompDetailId() > c2.getGuidelineCompDetailId())
						return -1;
					return 0;
				}
			});
		}
	}
	
	@Override
	protected Object clone() throws CloneNotSupportedException {
		PRGuidelineComp cloned = (PRGuidelineComp) super.clone();
		List<PRGuidelineCompDetail> clonedList = new ArrayList<PRGuidelineCompDetail>();
		for (PRGuidelineCompDetail guidelineCompDetail : cloned.getCompetitorDetails()) {
			clonedList.add((PRGuidelineCompDetail) guidelineCompDetail.clone());
		}
		cloned.setCompetitorDetails(clonedList);
		return cloned;
	}
	public boolean isIgnoreCompBelowCost() {
		return ignoreCompBelowCost;
	}
	public void setIgnoreCompBelowCost(boolean ignoreCompBelowCost) {
		this.ignoreCompBelowCost = ignoreCompBelowCost;
	}
}