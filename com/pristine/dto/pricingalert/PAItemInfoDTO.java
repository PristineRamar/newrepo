package com.pristine.dto.pricingalert;

import com.pristine.util.Constants;

public class PAItemInfoDTO implements Comparable<PAItemInfoDTO> {
	private int paMasterDataId;
	private int calendarId;
	private int locationCompetitorMapId;
	private int alertTypesId;
	
	private int itemCode;
	private boolean isKVIItem;
	private double avgRevenue;
	
	private double baseCurRegPrice = Constants.DEFAULT_NA;
	private String baseCurRegPriceEffDate;
	private double basePreRegPrice = Constants.DEFAULT_NA;
	private double baseFutRegPrice = Constants.DEFAULT_NA;
	private String baseFutRegPriceEffDate;
	
	private double baseCurListCost = Constants.DEFAULT_NA;
	private String baseCurListCostEffDate;
	private double basePreListCost = Constants.DEFAULT_NA;
	private double baseFutListCost = Constants.DEFAULT_NA;
	private String baseFutListCostEffDate;
	
	private double compCurRegPrice = Constants.DEFAULT_NA;
	private String compCurRegPriceEffDate;
	private String compCurRegPriceLastObsDate;
	private double compPreRegPrice = Constants.DEFAULT_NA;
	private double compCurRegPriceInBaseRange = Constants.DEFAULT_NA;
	
	private String retailerItemCode;
	private String majorCategory;
	private String portfolio;
	private String category;
	private String itemName;
	private String upc;
	private String lirItemName;
	private String lirCode;
	private int retLirId;
	private String locNum;
	private String compName;
	private String compNo;
	private int compLirItemCode;
	private String kviDesc;
	
	private String itemSize;
	private int brandId;
	private String brandName;
	
	private boolean outsidePIRange = false;
	private boolean outsideMarginRange = false;
	
	private double comp2CurRegPrice = Constants.DEFAULT_NA;
	private double comp2PreRegPrice = Constants.DEFAULT_NA;
	private double basePreRegPrice13w = Constants.DEFAULT_NA;
	private char compLocationType;
	
	public int getPaMasterDataId() {
		return paMasterDataId;
	}
	public void setPaMasterDataId(int paMasterDataId) {
		this.paMasterDataId = paMasterDataId;
	}
	public int getCalendarId() {
		return calendarId;
	}
	public void setCalendarId(int calendarId) {
		this.calendarId = calendarId;
	}
	public int getLocationCompetitorMapId() {
		return locationCompetitorMapId;
	}
	public void setLocationCompetitorMapId(int locationCompetitorMapId) {
		this.locationCompetitorMapId = locationCompetitorMapId;
	}
	public int getAlertTypesId() {
		return alertTypesId;
	}
	public void setAlertTypesId(int alertTypesId) {
		this.alertTypesId = alertTypesId;
	}
	public int getItemCode() {
		return itemCode;
	}
	public void setItemCode(int itemCode) {
		this.itemCode = itemCode;
	}
	public boolean isKVIItem() {
		return isKVIItem;
	}
	public void setKVIItem(boolean isKVIItem) {
		this.isKVIItem = isKVIItem;
	}
	public double getAvgRevenue() {
		return avgRevenue;
	}
	public void setAvgRevenue(double avgRevenue) {
		this.avgRevenue = avgRevenue;
	}
	public double getBaseCurRegPrice() {
		return baseCurRegPrice;
	}
	public void setBaseCurRegPrice(double baseCurRegPrice) {
		this.baseCurRegPrice = baseCurRegPrice;
	}
	public String getBaseCurRegPriceEffDate() {
		return baseCurRegPriceEffDate;
	}
	public void setBaseCurRegPriceEffDate(String baseCurRegPriceEffDate) {
		this.baseCurRegPriceEffDate = baseCurRegPriceEffDate;
	}
	public double getBasePreRegPrice() {
		return basePreRegPrice;
	}
	public void setBasePreRegPrice(double basePreRegPrice) {
		this.basePreRegPrice = basePreRegPrice;
	}
	public double getBaseFutRegPrice() {
		return baseFutRegPrice;
	}
	public void setBaseFutRegPrice(double baseFutRegPrice) {
		this.baseFutRegPrice = baseFutRegPrice;
	}
	public String getBaseFutRegPriceEffDate() {
		return baseFutRegPriceEffDate;
	}
	public void setBaseFutRegPriceEffDate(String baseFutRegPriceEffDate) {
		this.baseFutRegPriceEffDate = baseFutRegPriceEffDate;
	}
	public double getBaseCurListCost() {
		return baseCurListCost;
	}
	public void setBaseCurListCost(double baseCurListCost) {
		this.baseCurListCost = baseCurListCost;
	}
	public String getBaseCurListCostEffDate() {
		return baseCurListCostEffDate;
	}
	public void setBaseCurListCostEffDate(String baseCurListCostEffDate) {
		this.baseCurListCostEffDate = baseCurListCostEffDate;
	}
	public double getBasePreListCost() {
		return basePreListCost;
	}
	public void setBasePreListCost(double basePreListCost) {
		this.basePreListCost = basePreListCost;
	}
	public double getBaseFutListCost() {
		return baseFutListCost;
	}
	public void setBaseFutListCost(double baseFutListCost) {
		this.baseFutListCost = baseFutListCost;
	}
	public String getBaseFutListCostEffDate() {
		return baseFutListCostEffDate;
	}
	public void setBaseFutListCostEffDate(String baseFutListCostEffDate) {
		this.baseFutListCostEffDate = baseFutListCostEffDate;
	}
	public double getCompCurRegPrice() {
		return compCurRegPrice;
	}
	public void setCompCurRegPrice(double compCurRegPrice) {
		this.compCurRegPrice = compCurRegPrice;
	}
	public String getCompCurRegPriceEffDate() {
		return compCurRegPriceEffDate;
	}
	public void setCompCurRegPriceEffDate(String compCurRegPriceEffDate) {
		this.compCurRegPriceEffDate = compCurRegPriceEffDate;
	}
	public String getCompCurRegPriceLastObsDate() {
		return compCurRegPriceLastObsDate;
	}
	public void setCompCurRegPriceLastObsDate(String compCurRegPriceLastObsDate) {
		this.compCurRegPriceLastObsDate = compCurRegPriceLastObsDate;
	}
	public double getCompPreRegPrice() {
		return compPreRegPrice;
	}
	public void setCompPreRegPrice(double compPreRegPrice) {
		this.compPreRegPrice = compPreRegPrice;
	}
	public double getCompCurRegPriceInBaseRange() {
		return compCurRegPriceInBaseRange;
	}
	public void setCompCurRegPriceInBaseRange(double compCurRegPriceInBaseRange) {
		this.compCurRegPriceInBaseRange = compCurRegPriceInBaseRange;
	}
	public String getRetailerItemCode() {
		return retailerItemCode;
	}
	public void setRetailerItemCode(String retailerItemCode) {
		this.retailerItemCode = retailerItemCode;
	}
	public String getMajorCategory() {
		return majorCategory;
	}
	public void setMajorCategory(String majorCategory) {
		this.majorCategory = majorCategory;
	}
	public String getPortfolio() {
		return portfolio;
	}
	public void setPortfolio(String portfolio) {
		this.portfolio = portfolio;
	}
	public String getCategory() {
		return category;
	}
	public void setCategory(String category) {
		this.category = category;
	}
	public String getItemName() {
		return itemName;
	}
	public void setItemName(String itemName) {
		this.itemName = itemName;
	}
	public String getUpc() {
		return upc;
	}
	public void setUpc(String upc) {
		this.upc = upc;
	}
	public String getLirItemName() {
		return lirItemName;
	}
	public void setLirItemName(String lirItemName) {
		this.lirItemName = lirItemName;
	}
	public String getLirCode() {
		return lirCode;
	}
	public void setLirCode(String lirCode) {
		this.lirCode = lirCode;
	}
	public int getRetLirId() {
		return retLirId;
	}
	public void setRetLirId(int retLirId) {
		this.retLirId = retLirId;
	}
	public String getLocNum() {
		return locNum;
	}
	public void setLocNum(String locNum) {
		this.locNum = locNum;
	}
	public String getCompName() {
		return compName;
	}
	public void setCompName(String compName) {
		this.compName = compName;
	}
	public String getCompNo() {
		return compNo;
	}
	public void setCompNo(String compNo) {
		this.compNo = compNo;
	}
	public int getCompLirItemCode() {
		return compLirItemCode;
	}
	public void setCompLirItemCode(int compLirItemCode) {
		this.compLirItemCode = compLirItemCode;
	}
	public String getItemSize() {
		return itemSize;
	}
	public void setItemSize(String itemSize) {
		this.itemSize = itemSize;
	}
	public String getKviDesc() {
		if(kviDesc == null || kviDesc.length() == 0){
			if(isKVIItem)
				kviDesc = "Yes";
			else
				kviDesc = "No";
		}
		return kviDesc;
	}
	public void setKviDesc(String kviDesc) {
		this.kviDesc = kviDesc;
	}
	public int compareTo(PAItemInfoDTO dto)
	{
	     if(getAvgRevenue() == dto.getAvgRevenue())
	    	 return 0;
	     else if(getAvgRevenue() < dto.getAvgRevenue())
	    	 return 1;
	     else
	    	 return -1;
	}
	public int getBrandId() {
		return brandId;
	}
	public void setBrandId(int brandId) {
		this.brandId = brandId;
	}
	public String getBrandName() {
		return brandName;
	}
	public void setBrandName(String brandName) {
		this.brandName = brandName;
	}
	
	public boolean isOutsidePIRange() {
		return outsidePIRange;
	}
	public void setOutsidePIRange(boolean outsidePIRange) {
		this.outsidePIRange = outsidePIRange;
	}
	public boolean isOutsideMarginRange() {
		return outsideMarginRange;
	}
	public void setOutsideMarginRange(boolean outsideMarginRange) {
		this.outsideMarginRange = outsideMarginRange;
	}
	public double getComp2CurRegPrice() {
		return comp2CurRegPrice;
	}
	public void setComp2CurRegPrice(double comp2CurRegPrice) {
		this.comp2CurRegPrice = comp2CurRegPrice;
	}
	public double getComp2PreRegPrice() {
		return comp2PreRegPrice;
	}
	public void setComp2PreRegPrice(double comp2PreRegPrice) {
		this.comp2PreRegPrice = comp2PreRegPrice;
	}
	public double getBasePreRegPrice13w() {
		return basePreRegPrice13w;
	}
	public void setBasePreRegPrice13w(double basePreRegPrice13w) {
		this.basePreRegPrice13w = basePreRegPrice13w;
	}
	public char getCompLocationType() {
		return compLocationType;
	}
	public void setCompLocationType(char compLocationType) {
		this.compLocationType = compLocationType;
	}
}
