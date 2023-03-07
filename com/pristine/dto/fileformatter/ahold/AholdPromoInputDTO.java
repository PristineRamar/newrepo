package com.pristine.dto.fileformatter.ahold;

import java.util.Date;
import java.util.HashSet;
import java.util.Set;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import com.pristine.dto.offermgmt.LocationKey;
import com.pristine.dto.offermgmt.MultiplePrice;
import com.pristine.lookup.offermgmt.promotion.PromoTypeLookup;

public class AholdPromoInputDTO {

	private final static Pattern retailerItemCodePattern = Pattern.compile("\\(([0-9.0-9].*)\\)");

	private String categoryName;
	private String timeSelection;
	private String week;
	private String groupName;
	private String promoGroup;
	private String pricelineKey;
	private String pricelineName;
	private String division;
	private String singleDivision;
	private String description;
	private String brand;
	private String leadItemCode;
	private String status;
	private String statusDetails;
	private String primaryTactic;
	private String overlayTactic;
	private String ad;
	private String display;
	private int pack;
	private double caseListCost;
	private double unitListCost;
	private double everydayOI;
	private double promoOI;
	private double everdayAccrual;
	private double promoBIB;
	private double perUnitScan;
	private double additionalScan;
	private double netCaseCost;
	private double netUnitCost;
	private double averageEDLP;
	private double edlpMargin;
	private double targetRetailPerUnit;
	private double pennyProfitPerUnit;
	//private double targetMargin;
	
	private String adWeekStartDate;
	private String adWeekEndDate;
	private int eventId;
	private String dealStartDate;
	private String dealEndDate;
	
	/***** Other than input attributes ***/
	private int weekStartDayCalendarId = 0;
	private int weekEndDayCalendarId = 0;
	private Date weekStartDay = null;
	private Date weekEndDay = null;
	private LocationKey locationKey = null;
	private Set<Integer> promoItemCodes = new HashSet<Integer>();
	private PromoTypeLookup promoType = null;
	private String subPromoType = null;
	private MultiplePrice salePrice = null;
	private String ignoreReason = "";
	private boolean isRowIgnored = false;
	/***** Other than input attributes ***/

	public String getCategoryName() {
		return categoryName;
	}

	public void setCategoryName(String categoryName) {
		this.categoryName = categoryName;
	}

	public String getTimeSelection() {
		return timeSelection;
	}

	public void setTimeSelection(String timeSelection) {
		this.timeSelection = timeSelection;
	}

	public String getWeek() {
		return week;
	}

	public void setWeek(String week) {
		this.week = week;
	}

	public String getGroupName() {
		return groupName;
	}

	public void setGroupName(String groupName) {
		this.groupName = groupName;
	}

	public String getPromoGroup() {
		return promoGroup;
	}

	public void setPromoGroup(String promoGroup) {
		this.promoGroup = promoGroup;
	}

	public String getPricelineKey() {
		return pricelineKey;
	}

	public void setPricelineKey(String pricelineKey) {
		this.pricelineKey = pricelineKey;
	}

	public String getPricelineName() {
		return pricelineName;
	}

	public void setPricelineName(String pricelineName) {
		this.pricelineName = pricelineName;
	}

	public String getDivision() {
		return division;
	}

	public void setDivision(String division) {
		this.division = division;
	}

	public String getSingleDivision() {
		return singleDivision;
	}

	public void setSingleDivision(String singleDivision) {
		this.singleDivision = singleDivision;
	}

	public String getDescription() {
		return description;
	}

	public void setDescription(String description) {
		this.description = description;
	}

	public String getBrand() {
		return brand;
	}

	public void setBrand(String brand) {
		this.brand = brand;
	}

	public String getLeadItemCode() {
		return leadItemCode;
	}

	public void setLeadItemCode(String leadItemCode) {
		this.leadItemCode = leadItemCode;
	}

	public String getStatus() {
		return status;
	}

	public void setStatus(String status) {
		this.status = status;
	}

	public String getStatusDetails() {
		return statusDetails;
	}

	public void setStatusDetails(String statusDetails) {
		this.statusDetails = statusDetails;
	}

	public String getPrimaryTactic() {
		if(primaryTactic != null) {
			primaryTactic = primaryTactic.toUpperCase().trim();
		}
		return primaryTactic;
	}

	public void setPrimaryTactic(String primaryTactic) {
		this.primaryTactic = primaryTactic;
	}

	public String getOverlayTactic() {
		if(overlayTactic != null) {
			overlayTactic = overlayTactic.toUpperCase().trim();
		}
		return overlayTactic;
	}

	public void setOverlayTactic(String overlayTactic) {
		this.overlayTactic = overlayTactic;
	}

	public String getAd() {
		return ad;
	}

	public void setAd(String ad) {
		this.ad = ad;
	}

	public String getDisplay() {
		return display;
	}

	public void setDisplay(String display) {
		this.display = display;
	}

	public int getPack() {
		return pack;
	}

	public void setPack(int pack) {
		this.pack = pack;
	}

	public double getCaseListCost() {
		return caseListCost;
	}

	public void setCaseListCost(double caseListCost) {
		this.caseListCost = caseListCost;
	}

	public double getUnitListCost() {
		return unitListCost;
	}

	public void setUnitListCost(double unitListCost) {
		this.unitListCost = unitListCost;
	}

	public double getEverydayOI() {
		return everydayOI;
	}

	public void setEverydayOI(double everydayOI) {
		this.everydayOI = everydayOI;
	}

	public double getPromoOI() {
		return promoOI;
	}

	public void setPromoOI(double promoOI) {
		this.promoOI = promoOI;
	}

	public double getEverdayAccrual() {
		return everdayAccrual;
	}

	public void setEverdayAccrual(double everdayAccrual) {
		this.everdayAccrual = everdayAccrual;
	}

	public double getPromoBIB() {
		return promoBIB;
	}

	public void setPromoBIB(double promoBIB) {
		this.promoBIB = promoBIB;
	}

	public double getPerUnitScan() {
		return perUnitScan;
	}

	public void setPerUnitScan(double perUnitScan) {
		this.perUnitScan = perUnitScan;
	}

	public double getAdditionalScan() {
		return additionalScan;
	}

	public void setAdditionalScan(double additionalScan) {
		this.additionalScan = additionalScan;
	}

	public double getNetCaseCost() {
		return netCaseCost;
	}

	public void setNetCaseCost(double netCaseCost) {
		this.netCaseCost = netCaseCost;
	}

	public double getNetUnitCost() {
		return netUnitCost;
	}

	public void setNetUnitCost(double netUnitCost) {
		this.netUnitCost = netUnitCost;
	}

	public double getAverageEDLP() {
		return averageEDLP;
	}

	public void setAverageEDLP(double averageEDLP) {
		this.averageEDLP = averageEDLP;
	}

	public double getEdlpMargin() {
		return edlpMargin;
	}

	public void setEdlpMargin(double edlpMargin) {
		this.edlpMargin = edlpMargin;
	}

	public double getTargetRetailPerUnit() {
		return targetRetailPerUnit;
	}

	public void setTargetRetailPerUnit(double targetRetailPerUnit) {
		this.targetRetailPerUnit = targetRetailPerUnit;
	}

	public double getPennyProfitPerUnit() {
		return pennyProfitPerUnit;
	}

	public void setPennyProfitPerUnit(double pennyProfitPerUnit) {
		this.pennyProfitPerUnit = pennyProfitPerUnit;
	}

	//public double getTargetMargin() {
		//return targetMargin;
	//}

	//public void setTargetMargin(double targetMargin) {
		//this.targetMargin = targetMargin;
	//}

	public String getAdWeekStartDate() {
		return adWeekStartDate;
	}

	public void setAdWeekStartDate(String adWeekStartDate) {
		this.adWeekStartDate = adWeekStartDate;
	}
	
	public String getAdWeekEndDate() {
		return adWeekEndDate;
	}

	public void setAdWeekEndDate(String adWeekEndDate) {
		this.adWeekEndDate = adWeekEndDate;
	}
	
	public String getDealStartDate() {
		return dealStartDate;
	}
	
	public int getEventId() {
		return eventId;
	}

	public void setEventId(int eventId) {
		this.eventId = eventId;
	}

	public void setDealStartDate(String dealStartDate) {
		this.dealStartDate = dealStartDate;
	}
	
	public String getDealEndDate() {
		return dealEndDate;
	}

	public void setDealEndDate(String dealEndDate) {
		this.dealEndDate = dealEndDate;
	}
	
	public int getWeekStartDayCalendarId() {
		return weekStartDayCalendarId;
	}

	public void setWeekStartDayCalendarId(int weekStartDayCalendarId) {
		this.weekStartDayCalendarId = weekStartDayCalendarId;
	}

	public int getWeekEndDayCalendarId() {
		return weekEndDayCalendarId;
	}

	public void setWeekEndDayCalendarId(int weekEndDayCalendarId) {
		this.weekEndDayCalendarId = weekEndDayCalendarId;
	}

	public LocationKey getLocationKey() {
		return locationKey;
	}

	public void setLocationKey(LocationKey locationKey) {
		this.locationKey = locationKey;
	}


	public int getPromoWeekNo() {
		int weekNo = 0;
		if (Pattern.matches("[0-9]{6}", this.week)) {
			weekNo = Integer.valueOf(this.week.substring(0, 2));
		}
		return weekNo;
	}

	public int getPromoYear() {
		int year = 0;
		if (Pattern.matches("[0-9]{6}", this.week)) {
			year = Integer.valueOf(this.week.substring(2, 6));
		}
		return year;
	}

	public boolean isItem() {
		return this.pricelineName.startsWith("(SS)-") ? true : false;
	}

	public String parseRetailerItemCode() {
		String retailerItemCode = "";
		if (isItem()) {
			Matcher m = retailerItemCodePattern.matcher(this.pricelineName);

			if (m.find()) {// Finds Matching Pattern in String
				retailerItemCode = String.valueOf(Long.valueOf(m.group(1).substring(0, m.group(1).indexOf("."))));
			}
		}
		return retailerItemCode;
	}

	public Set<Integer> getPromoItemCodes() {
		return promoItemCodes;
	}

	public void setPromoItemCodes(Set<Integer> promoItemCodes) {
		this.promoItemCodes = promoItemCodes;
	}

	public PromoTypeLookup getPromoType() {
		return promoType;
	}

	public void setPromoType(PromoTypeLookup promoType) {
		this.promoType = promoType;
	}

	public String getSubPromoType() {
		return subPromoType;
	}

	public void setSubPromoType(String subPromoType) {
		this.subPromoType = subPromoType;
	}

	public MultiplePrice getSalePrice() {
		return salePrice;
	}

	public void setSalePrice(MultiplePrice salePrice) {
		this.salePrice = salePrice;
	}

	public String logError() {
		String errorLog = "";
		errorLog = "Week:" + this.week + ",SingleDivision:" + this.singleDivision + ",GroupName:" + this.groupName + ",PromoGroup:" + this.promoGroup
				+ ",PriceLineName:" + this.pricelineName + ",PrimaryTactic:" + this.primaryTactic + ",AverageEDLP:" + this.averageEDLP
				+ ",IgnoreReason:" + this.ignoreReason;

		return errorLog;
	}

	public Date getWeekStartDay() {
		return weekStartDay;
	}

	public void setWeekStartDay(Date weekStartDay) {
		this.weekStartDay = weekStartDay;
	}

	public Date getWeekEndDay() {
		return weekEndDay;
	}

	public void setWeekEndDay(Date weekEndDay) {
		this.weekEndDay = weekEndDay;
	}
	
	public String toDebugLog() {
		return "AholdPromoInputDTO [categoryName=" + categoryName + ", timeSelection=" + timeSelection + ", week=" + week + ", groupName=" + groupName
				+ ", promoGroup=" + promoGroup + ", pricelineKey=" + pricelineKey + ", pricelineName=" + pricelineName + ", division=" + division
				+ ", singleDivision=" + singleDivision + ", description=" + description + ", brand=" + brand + ", leadItemCode=" + leadItemCode
				+ ", status=" + status + ", statusDetails=" + statusDetails + ", primaryTactic=" + primaryTactic + ", overlayTactic=" + overlayTactic
				+ ", ad=" + ad + ", display=" + display + ", pack=" + pack + ", caseListCost=" + caseListCost + ", unitListCost=" + unitListCost
				+ ", everydayOI=" + everydayOI + ", promoOI=" + promoOI + ", everdayAccrual=" + everdayAccrual + ", promoBIB=" + promoBIB
				+ ", perUnitScan=" + perUnitScan + ", additionalScan=" + additionalScan + ", netCaseCost=" + netCaseCost + ", netUnitCost="
				+ netUnitCost + ", averageEDLP=" + averageEDLP + ", edlpMargin=" + edlpMargin + ", targetRetailPerUnit=" + targetRetailPerUnit
				+ ", pennyProfitPerUnit=" + pennyProfitPerUnit + ", weekStartDayCalendarId="
				+ weekStartDayCalendarId + ", weekEndDayCalendarId=" + weekEndDayCalendarId + ", weekStartDay=" + weekStartDay + ", weekEndDay="
				+ weekEndDay + ", locationKey=" + locationKey + ", promoItemCodes=" + promoItemCodes + ", promoType="
				+ promoType + ", subPromoType=" + subPromoType + ", salePrice=" + salePrice + "]";
	}

	public String getIgnoreReason() {
		return ignoreReason;
	}

	public void setIgnoreReason(String ignoreReason) {
		this.ignoreReason = ignoreReason;
	}

	public boolean isRowIgnored() {
		return isRowIgnored;
	}

	public void setRowIgnored(boolean isRowIgnored) {
		this.isRowIgnored = isRowIgnored;
	}
}
