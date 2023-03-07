package com.pristine.dto.offermgmt;

//import java.io.Serializable;


import net.sf.ehcache.pool.sizeof.annotations.IgnoreSizeOf;

//import org.apache.log4j.Logger;

//import com.pristine.util.Constants;
//import com.pristine.util.PropertyManager;
//import com.pristine.util.offermgmt.PRConstants;

@IgnoreSizeOf
//public class PRGuidelineMargin implements Serializable, Cloneable{
	public class PRGuidelineMargin implements  Cloneable{
//	private static Logger logger = Logger.getLogger("PRGuidelineMargin");
	
	private long gMarginId;
	private long gId;
	private char valueType;
	private double minMarginPct;
	private double maxMarginPct;
	private double minMargin$;
	private double maxMargin$;
	private char currentMargin;
	private char costFlag;
	private char itemLevelFlag;
	private int priceIncrease;
	private int priceDecrease;
	private char itemFlag;
	
	public long getgMarginId() {
		return gMarginId;
	}
	public void setgMarginId(long gMarginId) {
		this.gMarginId = gMarginId;
	}
	public long getgId() {
		return gId;
	}
	public void setgId(long gId) {
		this.gId = gId;
	}
	public char getValueType() {
		return valueType;
	}
	public void setValueType(char valueType) {
		this.valueType = valueType;
	}
	public double getMinMarginPct() {
		return minMarginPct;
	}
	public void setMinMarginPct(double minMarginPct) {
		this.minMarginPct = minMarginPct;
	}
	public double getMaxMarginPct() {
		return maxMarginPct;
	}
	public void setMaxMarginPct(double maxMarginPct) {
		this.maxMarginPct = maxMarginPct;
	}
	public double getMinMargin$() {
		return minMargin$;
	}
	public void setMinMargin$(double minMargin$) {
		this.minMargin$ = minMargin$;
	}
	public double getMaxMargin$() {
		return maxMargin$;
	}
	public void setMaxMargin$(double maxMargin$) {
		this.maxMargin$ = maxMargin$;
	}
	public char getCurrentMargin() {
		return currentMargin;
	}
	public void setCurrentMargin(char currentMargin) {
		this.currentMargin = currentMargin;
	}
	public char getCostFlag() {
		return costFlag;
	}
	public void setCostFlag(char costFlag) {
		this.costFlag = costFlag;
	}
	public char getItemLevelFlag() {
		return itemLevelFlag;
	}
	public void setItemLevelFlag(char itemLevelFlag) {
		this.itemLevelFlag = itemLevelFlag;
	}
	
	
	public int getPriceIncrease() {
		return priceIncrease;
	}
	public void setPriceIncrease(int priceIncrease) {
		this.priceIncrease = priceIncrease;
	}
	public int getPriceDecrease() {
		return priceDecrease;
	}
	public void setPriceDecrease(int priceDecrease) {
		this.priceDecrease = priceDecrease;
	}
	
	public char getItemFlag() {
		return itemFlag;
	}

	public void setItemFlag(char itemFlag) {
		this.itemFlag = itemFlag;
	}
	@Override
    protected Object clone() throws CloneNotSupportedException {
        return super.clone();
    }
	
	public void copy(PRGuidelineMargin marginGuideline) {
		this.gMarginId = marginGuideline.gMarginId;
		this.gId = marginGuideline.gId;
		this.valueType = marginGuideline.valueType;
		this.minMarginPct = marginGuideline.minMarginPct;
		this.maxMarginPct = marginGuideline.maxMarginPct;
		this.minMargin$ = marginGuideline.minMargin$;
		this.maxMargin$ = marginGuideline.maxMargin$;
		this.currentMargin = marginGuideline.currentMargin;
		this.priceIncrease=marginGuideline.priceIncrease;
		this.priceDecrease=marginGuideline.priceDecrease;
		this.itemFlag=marginGuideline.itemFlag;
	}
	}
