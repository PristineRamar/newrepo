package com.pristine.dto.offermgmt.oos;

import java.util.ArrayList;
import java.util.List;

import com.pristine.dto.offermgmt.MultiplePrice;
import com.pristine.service.offermgmt.oos.OOSCriteriaData;
import com.pristine.util.Constants;
import com.pristine.util.offermgmt.PRFormatHelper;

public class OOSItemDTO implements Cloneable{
	private int locationLevelId;
	private int locationId;
	private int calendarId;
	private int productLevelId;
	private int productId;
	private int dayPartId;
	private int dayPartExecOrder;
	//private int itemCode;
	private int deptProductId;
	private int catProductId;
	private int retLirId;
	private boolean alertSendStatus;
	private String storeNo ="";
	private String retailerItemCode;
	private String upc;
	private String itemName;
	private String lirName;
	private String itemSize;
	private String uom;
	private String departmentName;	
	private String categoryName;
	private String processingDate;
	private String timeSlot;
	private MultiplePrice regPrice = null;
	private MultiplePrice salePrice = null;
	
	private int adPageNo = Constants.NULLID;
	private int blockNo = Constants.NULLID;
	private int adPageNoGU = Constants.NULLID;
	private int blockNoGU = Constants.NULLID;
	private int displayTypeId = Constants.NULLID;
	private int promoTypeId = Constants.NULLID;
	private int noOfFacings;
	private long weeklyActualMovement = 0l;
	
	private long weeklyPredictedMovement = 0l;
	private int weeklyPredictionStatus;
	private long weeklyConfidenceLevelLower = 0l;
	private long weeklyConfidenceLevelUpper = 0l;
	
	private long previousDayPartPredictedMovement = 0l;
	
	private int noOfTimeMovedInLastXWeeks = 0;
	private int noOfZeroMovInLastXWeeks = 0;
	private long minMovementInLastXWeeks = 0;
	private long maxMovementInLastXWeeks = 0;
	private long avgMovementInLastXWeeks = 0;
	
	private long clientWeeklyPredictedMovement =0l;
	private long clientChainLevelWeeklyMov = 0l;
	
	private long xWeeksStoreLevelAvgMov = 0l;
	private long xWeeksChainLevelAvgMov = 0l;
	private double xWeeksStoreToChainPercent = 0f;
	
	private boolean isOutOfStockItem = false;
	private boolean sendToClient = false;
	private String districtName = "";
	private long clientDayPartPredictedMovement = 0l;
	
	private List<Long> previousTimeSlotActualMov = new ArrayList<Long>();
	
	private int oosCriteriaId = 0;
	private boolean isPersishableOrDSD = false;
	private double dayPartMovPercent = 0;
	private double storeLevelAvgMov = 0;
	private double dayPartLevelAvgMov = 0;
	private OOSCriteriaData oosCriteriaData = new OOSCriteriaData();
	private boolean isFindOOS = false;
	private double actualWeeklyMovOfDayPart = 0;
	private char isOOSAnalysis = 'N';
	private int shelfCapacity;
	private long weeklyPredictedMovementTops = 0l;
	private long weeklyPredictedMovementGU = 0l;
	private int noOfShelfLocations = 0;
	private int noOfLigOrNonLig = 0;
	
	public int getNoOfLigOrNonLig() {
		return noOfLigOrNonLig;
	}
	public void setNoOfLigOrNonLig(int noOfLigOrNonLig) {
		this.noOfLigOrNonLig = noOfLigOrNonLig;
	}
	public int getAdPageNoGU() {
		return adPageNoGU;
	}
	public void setAdPageNoGU(int adPageNoGU) {
		this.adPageNoGU = adPageNoGU;
	}
	public int getBlockNoGU() {
		return blockNoGU;
	}
	public void setBlockNoGU(int blockNoGU) {
		this.blockNoGU = blockNoGU;
	}
	public long getWeeklyPredictedMovementTops() {
		return weeklyPredictedMovementTops;
	}
	public void setWeeklyPredictedMovementTops(long weeklyPredictedMovementTops) {
		this.weeklyPredictedMovementTops = weeklyPredictedMovementTops;
	}
	public long getWeeklyPredictedMovementGU() {
		return weeklyPredictedMovementGU;
	}
	public void setWeeklyPredictedMovementGU(long weeklyPredictedMovementGU) {
		this.weeklyPredictedMovementGU = weeklyPredictedMovementGU;
	}
	public char getIsOOSAnalysis() {
		return isOOSAnalysis;
	}
	public void setIsOOSAnalysis(char isOOSAnalysis) {
		this.isOOSAnalysis = isOOSAnalysis;
	}
	public double getActualWeeklyMovOfDayPart() {
		return actualWeeklyMovOfDayPart;
	}
	public void setActualWeeklyMovOfDayPart(double actualWeeklyMovOfDayPart) {
		this.actualWeeklyMovOfDayPart = actualWeeklyMovOfDayPart;
	}
	public long getClientDayPartPredictedMovement() {
		return clientDayPartPredictedMovement;
	}
	public void setClientDayPartPredictedMovement(long clientDayPartPredictedMovement) {
		this.clientDayPartPredictedMovement = clientDayPartPredictedMovement;
	}
	public String getDistrictName() {
		return districtName;
	}
	public void setDistrictName(String districtName) {
		this.districtName = districtName;
	}
	public boolean getIsOutOfStockItem() {
		return isOutOfStockItem;
	}
	public void setIsOutOfStockItem(boolean isOutOfStockItem) {
		this.isOutOfStockItem = isOutOfStockItem;
	}
	
	public long getClientWeeklyPredictedMovement() {
		return clientWeeklyPredictedMovement;
	}
	public void setClientWeeklyPredictedMovement(long clientWeeklyPredictedMovement) {
		this.clientWeeklyPredictedMovement = clientWeeklyPredictedMovement;
	}
	public long getClientChainLevelWeeklyMov() {
		return clientChainLevelWeeklyMov;
	}
	public void setClientChainLevelWeeklyMov(long clientChainLevelWeeklyMov) {
		this.clientChainLevelWeeklyMov = clientChainLevelWeeklyMov;
	}
	public long getxWeeksStoreLevelAvgMov() {
		return xWeeksStoreLevelAvgMov;
	}
	public void setxWeeksStoreLevelAvgMov(long xWeeksStoreLevelAvgMov) {
		this.xWeeksStoreLevelAvgMov = xWeeksStoreLevelAvgMov;
	}
	public long getxWeeksChainLevelAvgMov() {
		return xWeeksChainLevelAvgMov;
	}
	public void setxWeeksChainLevelAvgMov(long xWeeksChainLevelAvgMov) {
		this.xWeeksChainLevelAvgMov = xWeeksChainLevelAvgMov;
	}
	private char distFlag;
	
	public char getDistFlag() {
		return distFlag;
	}
	public void setDistFlag(char distFlag) {
		this.distFlag = distFlag;
	}

	public String getStoreNo() {
		return storeNo;
	}
	public void setStoreNo(String storeNo) {
		this.storeNo = storeNo;
	}
	public String getRetailerItemCode() {
		return retailerItemCode;
	}
	public void setRetailerItemCode(String retailerItemCode) {
		this.retailerItemCode = retailerItemCode;
	}
	public String getUpc() {
		return upc;
	}
	public void setUpc(String upc) {
		this.upc = upc;
	}
	public String getItemName() {
		return itemName;
	}
	public void setItemName(String itemName) {
		this.itemName = itemName;
	}
	public String getLirName() {
		return lirName;
	}
	public void setLirName(String lirName) {
		this.lirName = lirName;
	}
	public String getItemSize() {
		return itemSize;
	}
	public void setItemSize(String itemSize) {
		this.itemSize = itemSize;
	}
	public String getUom() {
		return uom;
	}
	public void setUom(String uom) {
		this.uom = uom;
	}
	public int getLocationLevelId() {
		return locationLevelId;
	}
	public void setLocationLevelId(int locationLevelId) {
		this.locationLevelId = locationLevelId;
	}
	public int getLocationId() {
		return locationId;
	}
	public void setLocationId(int locationId) {
		this.locationId = locationId;
	}
	public int getCalendarId() {
		return calendarId;
	}
	public void setCalendarId(int calendarId) {
		this.calendarId = calendarId;
	}
	public int getDayPartId() {
		return dayPartId;
	}
	public void setDayPartId(int dayPartId) {
		this.dayPartId = dayPartId;
	}
//	public int getItemCode() {
//		return itemCode;
//	}
//	public void setItemCode(int itemCode) {
//		this.itemCode = itemCode;
//	}
	public boolean isAlertSendStatus() {
		return alertSendStatus;
	}
	public void setAlertSendStatus(boolean alertSendStatus) {
		this.alertSendStatus = alertSendStatus;
	}
	
	@Override
    public Object clone() throws CloneNotSupportedException {
		OOSItemDTO cloned = (OOSItemDTO)super.clone();
		return cloned;
	}
	public int getAdPageNo() {
		return adPageNo;
	}
	public void setAdPageNo(int adPageNo) {
		this.adPageNo = adPageNo;
	}
	public int getBlockNo() {
		return blockNo;
	}
	public void setBlockNo(int blockNo) {
		this.blockNo = blockNo;
	}
	public int getDisplayTypeId() {
		return displayTypeId;
	}
	public void setDisplayTypeId(int displayTypeId) {
		this.displayTypeId = displayTypeId;
	}
	public int getPromoTypeId() {
		return promoTypeId;
	}
	public void setPromoTypeId(int promoTypeId) {
		this.promoTypeId = promoTypeId;
	}
	public long getWeeklyPredictedMovement() {
		return weeklyPredictedMovement;
	}
	public void setWeeklyPredictedMovement(long weeklyPredictedMovement) {
		this.weeklyPredictedMovement = weeklyPredictedMovement;
	}
	public int getWeeklyPredictionStatus() {
		return weeklyPredictionStatus;
	}
	public void setWeeklyPredictionStatus(int weeklyPredictionStatus) {
		this.weeklyPredictionStatus = weeklyPredictionStatus;
	}
	public long getPreviousDayPartPredictedMovement() {
		return previousDayPartPredictedMovement;
	}
	public void setPreviousDayPartPredictedMovement(long previousDayPartPredictedMovement) {
		this.previousDayPartPredictedMovement = previousDayPartPredictedMovement;
	}
	public void setRegPrice(MultiplePrice regPrice) {
		this.regPrice = regPrice;
	}
	public void setSalePrice(MultiplePrice salePrice) {
		this.salePrice = salePrice;
	}
	public MultiplePrice getRegPrice() {
		return regPrice;
	}
	public MultiplePrice getSalePrice() {
		return salePrice;
	}
	public long getWeeklyConfidenceLevelLower() {
		return weeklyConfidenceLevelLower;
	}
	public void setWeeklyConfidenceLevelLower(long weeklyConfidenceLevelLower) {
		this.weeklyConfidenceLevelLower = weeklyConfidenceLevelLower;
	}
	public long getWeeklyConfidenceLevelUpper() {
		return weeklyConfidenceLevelUpper;
	}
	public void setWeeklyConfidenceLevelUpper(long weeklyConfidenceLevelUpper) {
		this.weeklyConfidenceLevelUpper = weeklyConfidenceLevelUpper;
	}
	
	public String getWeeklyConfidenceLevelRange(){
		if(this.weeklyConfidenceLevelLower <= 0 || this.weeklyConfidenceLevelUpper <= 0)
			return Constants.EMPTY;
		else 
			return this.weeklyConfidenceLevelLower + " - " + this.weeklyConfidenceLevelUpper; 
	}
	
	public String getRegularPriceString(){
		if(this.regPrice == null){
			return Constants.EMPTY;
		}
		else{
			if(this.regPrice.multiple > 1){
				return this.regPrice.multiple + " / " + PRFormatHelper.doubleToTwoDigitString(this.regPrice.price);  
			}
			else{
				return String.valueOf(this.regPrice.price);
			}
		}
	}
	
	public String getSalePriceString(){
		if(this.salePrice == null){
			return Constants.EMPTY;
		}
		else{
			if(this.salePrice .multiple > 1){
				return this.salePrice.multiple + " / " + PRFormatHelper.doubleToTwoDigitString(this.salePrice.price);  
			}
			else{
				return String.valueOf(this.salePrice.price);
			}
		}
	}
	public int getNoOfTimeMovedInLastXWeeksOfItemOrLig() {
		return noOfTimeMovedInLastXWeeks;
	}
	public void setNoOfTimeMovedInLastXWeeksOfItemOrLig(int noOfTimeMovedInLastXWeeks) {
		this.noOfTimeMovedInLastXWeeks = noOfTimeMovedInLastXWeeks;
	}
	public long getMinMovementInLastXWeeks() {
		return minMovementInLastXWeeks;
	}
	public void setMinMovementInLastXWeeks(long minMovementInLastXWeeks) {
		this.minMovementInLastXWeeks = minMovementInLastXWeeks;
	}
	public long getMaxMovementInLastXWeeks() {
		return maxMovementInLastXWeeks;
	}
	public void setMaxMovementInLastXWeeks(long maxMovementInLastXWeeks) {
		this.maxMovementInLastXWeeks = maxMovementInLastXWeeks;
	}
	public long getAvgMovementInLastXWeeks() {
		return avgMovementInLastXWeeks;
	}
	public void setAvgMovementInLastXWeeks(long avgMovementInLastXWeeks) {
		this.avgMovementInLastXWeeks = avgMovementInLastXWeeks;
	}
	public int getNoOfZeroMovInLastXWeeks() {
		return noOfZeroMovInLastXWeeks;
	}
	public void setNoOfZeroMovInLastXWeeks(int noOfZeroMovInLastXWeeks) {
		this.noOfZeroMovInLastXWeeks = noOfZeroMovInLastXWeeks;
	}
	public String getCategoryName() {
		return categoryName;
	}
	public void setCategoryName(String categoryName) {
		this.categoryName = categoryName;
	}
	public String getProcessingDate() {
		return processingDate;
	}
	public void setProcessingDate(String processingDate) {
		this.processingDate = processingDate;
	}
	public String getTimeSlot() {
		return timeSlot;
	}
	public void setTimeSlot(String timeSlot) {
		this.timeSlot = timeSlot;
	}
	public int getNoOfFacings() {
		return noOfFacings;
	}
	public void setNoOfFacings(int noOfFacings) {
		this.noOfFacings = noOfFacings;
	}
	public int getDeptProductId() {
		return deptProductId;
	}
	public void setDeptProductId(int deptProductId) {
		this.deptProductId = deptProductId;
	}
	public long getWeeklyActualMovement() {
		return weeklyActualMovement;
	}
	public void setWeeklyActualMovement(long weeklyActualMovement) {
		this.weeklyActualMovement = weeklyActualMovement;
	}
	
	@Override
	public String toString() {
		return "OOSItemDTO [locationLevelId=" + locationLevelId + ", locationId=" + locationId + ", calendarId="
				+ calendarId + ", productLevelId=" + productLevelId + ", productId= " + productId +  
				", regPrice=" + (regPrice != null ? (regPrice.multiple + "/" +  regPrice.price) : "0/0")
				+ ", salePrice=" + (salePrice != null ? (salePrice.multiple + "/" +  salePrice.price) : "0/0")
				+ ", adPageNo=" + adPageNo + ", blockNo=" + blockNo + ", displayTypeId=" + displayTypeId
				+ ", promoTypeId=" + promoTypeId + ", weeklyPredictedMovement=" + weeklyPredictedMovement + "]";
	}
	public double getxWeeksStoreToChainPercent() {
		return xWeeksStoreToChainPercent;
	}
	public void setxWeeksStoreToChainPercent(double xWeeksStoreToChainPercent) {
		this.xWeeksStoreToChainPercent = xWeeksStoreToChainPercent;
	}
	public boolean getIsSendToClient() {
		return sendToClient;
	}
	public void setIsSendToClient(boolean sendToClient) {
		this.sendToClient = sendToClient;
	}
	public String getDepartmentName() {
		return departmentName;
	}
	public void setDepartmentName(String departmentName) {
		this.departmentName = departmentName;
	}
	public List<Long> getPreviousTimeSlotActualMov() {
		return previousTimeSlotActualMov;
	}
	public void setPreviousTimeSlotActualMov(List<Long> previousTimeSlotActualMov) {
		this.previousTimeSlotActualMov = previousTimeSlotActualMov;
	}
	public int getDayPartExecOrder() {
		return dayPartExecOrder;
	}
	public void setDayPartExecOrder(int dayPartExecOrder) {
		this.dayPartExecOrder = dayPartExecOrder;
	}
	public boolean isPersishableOrDSD() {
		return isPersishableOrDSD;
	}
	public void setPersishableOrDSD(boolean isPersishableOrDSD) {
		this.isPersishableOrDSD = isPersishableOrDSD;
	}
	public double getDayPartMovPercent() {
		return dayPartMovPercent;
	}
	public void setDayPartMovPercent(double dayPartMovPercent) {
		this.dayPartMovPercent = dayPartMovPercent;
	}
	public int getRetLirId() {
		return retLirId;
	}
	public void setRetLirId(int retLirId) {
		this.retLirId = retLirId;
	}
	public double getStoreLevelAvgMov() {
		return storeLevelAvgMov;
	}
	public void setStoreLevelAvgMov(double storeLevelAvgMov) {
		this.storeLevelAvgMov = storeLevelAvgMov;
	}
	public double getDayPartLevelAvgMov() {
		return dayPartLevelAvgMov;
	}
	public void setDayPartLevelAvgMov(double dayPartLevelAvgMov) {
		this.dayPartLevelAvgMov = dayPartLevelAvgMov;
	}
	public OOSCriteriaData getOOSCriteriaData() {
		return oosCriteriaData;
	}
	public void setOOSCriteriaData(OOSCriteriaData oosCriteriaData) {
		this.oosCriteriaData = oosCriteriaData;
	}
	public int getOOSCriteriaId() {
		return oosCriteriaId;
	}
	public void setOOSCriteriaId(int oosCriteriaId) {
		this.oosCriteriaId = oosCriteriaId;
	}
	public int getProductLevelId() {
		return productLevelId;
	}
	public void setProductLevelId(int productLevelId) {
		this.productLevelId = productLevelId;
	}
	public int getProductId() {
		return productId;
	}
	public void setProductId(int productId) {
		this.productId = productId;
	}
	public boolean isFindOOS() {
		return isFindOOS;
	}
	public void setFindOOS(boolean isFindOOS) {
		this.isFindOOS = isFindOOS;
	}
	public int getShelfCapacity() {
		return shelfCapacity;
	}
	public void setShelfCapacity(int shelfCapacity) {
		this.shelfCapacity = shelfCapacity;
	}
	public int getCatProductId() {
		return catProductId;
	}
	public void setCatProductId(int catProductId) {
		this.catProductId = catProductId;
	}
	public int getNoOfShelfLocations() {
		return noOfShelfLocations;
	}
	public void setNoOfShelfLocations(int noOfShelfLocations) {
		this.noOfShelfLocations = noOfShelfLocations;
	}
}

