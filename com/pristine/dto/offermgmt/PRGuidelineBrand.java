package com.pristine.dto.offermgmt;

import org.apache.log4j.Logger;

import com.pristine.util.Constants;
import com.pristine.util.offermgmt.PRConstants;

public class PRGuidelineBrand implements Cloneable{
	private static Logger logger = Logger.getLogger("PRGuidelineBrand");
	
	private long gBrandId;
	private long gId;
	private char valueType;
	private float minValue;
	private float maxValue;
	private char retailType;
	private String operatorText;
	private int brandTierId1;
	private int brandTierId2;
	private int brandId1;
	private int brandId2;
	private boolean AUCOverrideEnabled;
	private String brandTier1;
	private String brandTier2;
	private boolean compOverrideEnabled;
	private int compOverrideId;
	
	public long getgBrandId() {
		return gBrandId;
	}
	public void setgBrandId(long gBrandId) {
		this.gBrandId = gBrandId;
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
	public char getRetailType() {
		return retailType;
	}
	public void setRetailType(char isLower) {
		this.retailType = isLower;
	}
	public String getOperatorText() {
		return operatorText;
	}
	public void setOperatorText(String operatorText) {
		this.operatorText = operatorText;
	}
	public int getBrandTierId1() {
		return brandTierId1;
	}
	public void setBrandTierId1(int brandTierId1) {
		this.brandTierId1 = brandTierId1;
	}
	public int getBrandTierId2() {
		return brandTierId2;
	}
	public void setBrandTierId2(int brandTierId2) {
		this.brandTierId2 = brandTierId2;
	}
	public int getBrandId1() {
		return brandId1;
	}
	public void setBrandId1(int brandId1) {
		this.brandId1 = brandId1;
	}
	public int getBrandId2() {
		return brandId2;
	}
	public void setBrandId2(int brandId2) {
		this.brandId2 = brandId2;
	}
	
	/**
	 * Methods that returns Price Range for Index Guideline
	 * @return
	 */
	/*public PRRange getPriceRange(double price){
		PRRange range = null;
		if(PRConstants.BRAND_GUIDELINE_IS_LOWER == retailType){
			range = getLowerPriceRange(price);
		}else if(PRConstants.VALUE_TYPE_PCT == valueType){
			range = getPriceRangeWithPct(price);
		}else if(PRConstants.VALUE_TYPE_$ == valueType){
			range = getPriceRangeWith$(price);
		}
		return range;
	}*/
	
	/**
	 * Returns Price Range for Comp guideline specified in Pct
	 * @param price
	 * @return
	 */
	public PRRange getPriceRangeWithPct(double price){
		PRRange range = new PRRange();
		if(minValue != Constants.DEFAULT_NA && maxValue != Constants.DEFAULT_NA){
			range.setStartVal(price - (price * (minValue/100)));
			range.setEndVal(price + (price * (maxValue/100)));
		}else if(minValue != Constants.DEFAULT_NA){
			range.setEndVal(price - (price * (minValue/100)));
		}else if(maxValue != Constants.DEFAULT_NA){
			range.setEndVal(price + (price * (maxValue/100)));
		}
		return range;
	}
	
	/**
	 * Returns Price Range for Comp guideline specified in Dollar
	 * @param price
	 * @return
	 */
	public PRRange getPriceRangeWith$(double price){
		PRRange range = new PRRange();
		if(minValue != Constants.DEFAULT_NA && maxValue != Constants.DEFAULT_NA){
			range.setStartVal(price - minValue);
			range.setEndVal(price + maxValue);
		}else if(minValue != Constants.DEFAULT_NA){
			range.setEndVal(price - minValue);
		}else if(maxValue != Constants.DEFAULT_NA){
			range.setEndVal(price - maxValue);
		}
		return range;
	}
	
	public PRRange getLowerPriceRange(double price){
		PRRange range = new PRRange();
		range.setEndVal(price);
		return range;
	}
	
	@Override
	public Object clone() throws CloneNotSupportedException {
        return super.clone();
    }
	public boolean isAUCOverrideEnabled() {
		return AUCOverrideEnabled;
	}
	public void setAUCOverrideEnabled(boolean aUCOverrideEnabled) {
		AUCOverrideEnabled = aUCOverrideEnabled;
	}
	public String getBrandTier1() {
		return brandTier1;
	}
	public void setBrandTier1(String brandTier1) {
		this.brandTier1 = brandTier1;
	}
	public String getBrandTier2() {
		return brandTier2;
	}
	public void setBrandTier2(String brandTier2) {
		this.brandTier2 = brandTier2;
	}
	public boolean isCompOverrideEnabled() {
		return compOverrideEnabled;
	}
	public void setCompOverrideEnabled(boolean compOverrideEnabled) {
		this.compOverrideEnabled = compOverrideEnabled;
	}
	public int getCompOverrideId() {
		return compOverrideId;
	}
	public void setCompOverrideId(int compOverrideId) {
		this.compOverrideId = compOverrideId;
	}
}
