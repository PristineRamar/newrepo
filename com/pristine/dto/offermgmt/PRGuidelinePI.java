package com.pristine.dto.offermgmt;

//import java.io.Serializable;
import java.util.ArrayList;
import java.util.List;

import com.pristine.util.Constants;
import com.pristine.util.PropertyManager;

public class PRGuidelinePI  implements Cloneable{
	private long gPIId;
	private long gId;
	private double minValue;
	private double maxValue;
	private char itemLevelFlag;
	private int compStrId;
	
	public long getgPIId() {
		return gPIId;
	}
	public void setgPIId(long gPIId) {
		this.gPIId = gPIId;
	}
	public long getgId() {
		return gId;
	}
	public void setgId(long gId) {
		this.gId = gId;
	}
	public double getMinValue() {
		return minValue;
	}
	public void setMinValue(double minValue) {
		this.minValue = minValue;
	}
	public double getMaxValue() {
		return maxValue;
	}
	public void setMaxValue(double maxValue) {
		this.maxValue = maxValue;
	}
	public char getItemLevelFlag() {
		return itemLevelFlag;
	}
	public void setItemLevelFlag(char itemLevelFlag) {
		this.itemLevelFlag = itemLevelFlag;
	}
	
	/**
	 * Methods that returns Price Range for PI Guideline
	 * @return
	 */
	public PRRange getPriceRange(double price) {
		String indexCalcType = PropertyManager.getProperty("PI_CALC_TYPE", "").trim();
		PRRange range = new PRRange();

		// 13th May 2016, Added reverse price index support
		if (indexCalcType.equals(Constants.PI_CALC_TYPE_REVERSE)) {
			//Base price / comp
			if (minValue != Constants.DEFAULT_NA) {
				double startVal = price / (100 / minValue);
				range.setStartVal(startVal);
			}
			if (maxValue != Constants.DEFAULT_NA) {
				double endVal = price / (100 / maxValue);
				range.setEndVal(endVal);
			}
		} else {
			if (maxValue != Constants.DEFAULT_NA) {
				double startVal = price / (maxValue / 100);
				range.setStartVal(startVal);
			}
			if (minValue != Constants.DEFAULT_NA) {
				double endVal = price / (minValue / 100);
				range.setEndVal(endVal);
			}
		}
		return range;
	}
	
	@Override
	protected Object clone() throws CloneNotSupportedException {
        return super.clone();
		}
	
	public PRGuidelinePI getCategoryLevelPIGuideline(List<PRGuidelinePI> piGuidelines) {
		PRGuidelinePI catLevelGuideline = null;
		List<PRGuidelinePI> catLevelPIGuidelines = new ArrayList<PRGuidelinePI>();
		List<PRGuidelinePI> itemLevelPIGuidelines = new ArrayList<PRGuidelinePI>();
		if (piGuidelines != null) {
			for (PRGuidelinePI guidelinePI : piGuidelines) {
				if (guidelinePI.getItemLevelFlag() == Constants.NO)
					catLevelPIGuidelines.add(guidelinePI);
				else
					itemLevelPIGuidelines.add(guidelinePI);
			}
			if (catLevelPIGuidelines.size() > 0)
				catLevelGuideline = catLevelPIGuidelines.get(0);
			else {
				// If there is no category level guideline, then pick item level
				// guideline,
				// TODO:: this code has to be commented, when category level and
				// item level pi is enabled in ui
				if (itemLevelPIGuidelines.size() > 0)
					catLevelGuideline = itemLevelPIGuidelines.get(0);
			}
		}
		return catLevelGuideline;
	}
	public int getCompStrId() {
		return compStrId;
	}
	public void setCompStrId(int compStrId) {
		this.compStrId = compStrId;
	}
	}
