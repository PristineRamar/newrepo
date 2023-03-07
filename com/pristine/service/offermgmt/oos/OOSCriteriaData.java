package com.pristine.service.offermgmt.oos;

public class OOSCriteriaData {
	private long forecastMovOfProcessingTimeSlotOfItemOrLig = 0;
	private long forecastMovOfPrevTimeSlotOfItemOrLig = 0;
	private long forecastMovOfAllPrevTimeSlotsOfItemOrLig = 0;
	private long forecastMovOfRestOfTheDayOfItemOrLig = 0;
	private long actualMovOfProcessingTimeSlotOfItemOrLig = 0;
	private long actualMovOfPrevTimeSlotOfItemOrLig = 0;
	private long actualMovOfAllPrevTimeSlotsOfItemOrLig = 0;
	private int actualMovOfPrevDaysOfItemOrLig = -1;
//	private long actualMovOfPrevDaySameTimeSlotOfItemOrLig = 0;
	private long lowerConfidenceOfProcessingTimeSlotOfItemOrLig = 0;
	private long upperConfidenceOfProcessingTimeSlotOfItemOrLig = 0l;
	private int trxCntOfProcessingTimeSlotOfStore = -1;
	private int trxCntOfPrevDaysOfItemOrLig = -1;
	private int trxCntOfPrevDaysOfStore = -1;
	private long trxBasedPredOfProcessingTimeSlotOfItemOrLig = -1;
	private Integer noOfConsecutiveTimeSlotItemDidNotMove = null;
//	private boolean isItemMovedMoreThanXUnitsInEachXPrevTimeSlot = false;
	private double unitSalePrice = 0;
	private long actualMovOfProcessingDayItemOrLig = 0;
	private int shelfCapacityOfItemOrLig = 0; 
	private int noOfShelfLocationsOfItemOrLig = 0;
	private double shelfCapacityXPCTOfItemOrLig = 0; 
	
	private long combinedXTimeSlotForecastOfItemOrLig = 0;
	private long combinedXTimeSlotLowerConfOfItemOrLig = 0;
	private long combinedTimeSlotTrxBasedExpCriteria2OfItemOrLig = 0;
	private long combinedTimeSlotTrxBasedExpCriteria3OfItemOrLig = 0;
	private long combinedTimeSlotActualMovCriteria2OfItemOrLig = 0;
	private long combinedTimeSlotActualMovCriteria3OfItemOrLig = 0;
	
	public long getActualMovOfProcessingDayItemOrLig() {
		return actualMovOfProcessingDayItemOrLig;
	}
	public void setActualMovOfProcessingDayItemOrLig(long actualMovOfProcessingDay) {
		this.actualMovOfProcessingDayItemOrLig = actualMovOfProcessingDay;
	}
	 
//	public boolean isItemMovedMoreThanXUnitsInEachXPrevTimeSlot() {
//		return isItemMovedMoreThanXUnitsInEachXPrevTimeSlot;
//	}
//	public void setItemMovedMoreThanXUnitsInEachXPrevTimeSlot(boolean isItemMovedMoreThanXUnitsInEachXPrevTimeSlot) {
//		this.isItemMovedMoreThanXUnitsInEachXPrevTimeSlot = isItemMovedMoreThanXUnitsInEachXPrevTimeSlot;
//	}
	public long getUpperConfidenceOfProcessingTimeSlotOfItemOrLig () {
		return upperConfidenceOfProcessingTimeSlotOfItemOrLig ;
	}
	public void setUpperConfidenceOfProcessingTimeSlotOfItemOrLig (long upperConfidenceOfProcessingTimeSlotOfItemOrLig) {
		this.upperConfidenceOfProcessingTimeSlotOfItemOrLig  = upperConfidenceOfProcessingTimeSlotOfItemOrLig;
	}
	public double getUnitSalePrice() {
		return unitSalePrice;
	}
	public void setUnitSalePrice(double unitSalePrice) {
		this.unitSalePrice = unitSalePrice;
	}
	public int getActualMovOfPrevDaysOfItemOrLig() {
		return actualMovOfPrevDaysOfItemOrLig;
	}
	public void setActualMovOfPrevDaysOfItemOrLig(int actualMovOfPrevDaysOfItemOrLig) {
		this.actualMovOfPrevDaysOfItemOrLig = actualMovOfPrevDaysOfItemOrLig;
	}
//	public long getActualMovOfPrevDaySameTimeSlotOfItemOrLig() {
//		return actualMovOfPrevDaySameTimeSlotOfItemOrLig;
//	}
//	public void setActualMovOfPrevDaySameTimeSlotOfItemOrLig(long actualMovOfPrevDaySameTimeSlotOfItemOrLig) {
//		this.actualMovOfPrevDaySameTimeSlotOfItemOrLig = actualMovOfPrevDaySameTimeSlotOfItemOrLig;
//	}
	public int getTrxCntOfProcessingTimeSlotOfStore() {
		return trxCntOfProcessingTimeSlotOfStore;
	}
	public void setTrxCntOfProcessingTimeSlotOfStore(int trxCntOfProcessingTimeSlotOfStore) {
		this.trxCntOfProcessingTimeSlotOfStore = trxCntOfProcessingTimeSlotOfStore;
	}
	public int getTrxCntOfPrevDaysOfItemOrLig() {
		return trxCntOfPrevDaysOfItemOrLig;
	}
	public void setTrxCntOfPrevDaysOfItemOrLig(int trxCntOfPrevDaysOfItemOrLig) {
		this.trxCntOfPrevDaysOfItemOrLig = trxCntOfPrevDaysOfItemOrLig;
	}
	public int getTrxCntOfPrevDaysOfStore() {
		return trxCntOfPrevDaysOfStore;
	}
	public void setTrxCntOfPrevDaysOfStore(int trxCntOfPrevDaysOfStore) {
		this.trxCntOfPrevDaysOfStore = trxCntOfPrevDaysOfStore;
	}
	public long getTrxBasedPredOfProcessingTimeSlotOfItemOrLig() {
		return trxBasedPredOfProcessingTimeSlotOfItemOrLig;
	}
	public void setTrxBasedPredOfProcessingTimeSlotOfItemOrLig(long trxBasedPredOfProcessingTimeSlotOfItemOrLig) {
		this.trxBasedPredOfProcessingTimeSlotOfItemOrLig = trxBasedPredOfProcessingTimeSlotOfItemOrLig;
	}
	public long getForecastMovOfPrevTimeSlotOfItemOrLig() {
		return forecastMovOfPrevTimeSlotOfItemOrLig;
	}
	public void setForecastMovOfPrevTimeSlotOfItemOrLig(long forecastMovOfPrevTimeSlotOfItemOrLig) {
		this.forecastMovOfPrevTimeSlotOfItemOrLig = forecastMovOfPrevTimeSlotOfItemOrLig;
	}
	public long getActualMovOfPrevTimeSlotOfItemOrLig() {
		return actualMovOfPrevTimeSlotOfItemOrLig;
	}
	public void setActualMovOfPrevTimeSlotOfItemOrLig(long actualMovOfPrevTimeSlotOfItemOrLig) {
		this.actualMovOfPrevTimeSlotOfItemOrLig = actualMovOfPrevTimeSlotOfItemOrLig;
	}
	public long getForecastMovOfProcessingTimeSlotOfItemOrLig() {
		return forecastMovOfProcessingTimeSlotOfItemOrLig;
	}
	public void setForecastMovOfProcessingTimeSlotOfItemOrLig(long forecastMovOfProcessingTimeSlotOfItemOrLig) {
		this.forecastMovOfProcessingTimeSlotOfItemOrLig = forecastMovOfProcessingTimeSlotOfItemOrLig;
	}
	public long getActualMovOfProcessingTimeSlotOfItemOrLig() {
		return actualMovOfProcessingTimeSlotOfItemOrLig;
	}
	public void setActualMovOfProcessingTimeSlotOfItemOrLig(long actualMovOfProcessingTimeSlotOfItemOrLig) {
		this.actualMovOfProcessingTimeSlotOfItemOrLig = actualMovOfProcessingTimeSlotOfItemOrLig;
	}
	public long getLowerConfidenceOfProcessingTimeSlotOfItemOrLig() {
		return lowerConfidenceOfProcessingTimeSlotOfItemOrLig;
	}
	public void setLowerConfidenceOfProcessingTimeSlotOfItemOrLig(long lowerConfidenceOfProcessingTimeSlotOfItemOrLig) {
		this.lowerConfidenceOfProcessingTimeSlotOfItemOrLig = lowerConfidenceOfProcessingTimeSlotOfItemOrLig;
	}
	public int getShelfCapacityOfItemOrLig() {
		return shelfCapacityOfItemOrLig;
	}
	public void setShelfCapacityOfItemOrLig(int shelfCapacityOfItemOrLig) {
		this.shelfCapacityOfItemOrLig = shelfCapacityOfItemOrLig;
	}
	public long getCombinedXTimeSlotForecastOfItemOrLig() {
		return combinedXTimeSlotForecastOfItemOrLig;
	}
	public void setCombinedXTimeSlotForecastOfItemOrLig(long combinedXTimeSlotForecastOfItemOrLig) {
		this.combinedXTimeSlotForecastOfItemOrLig = combinedXTimeSlotForecastOfItemOrLig;
	}
	public long getCombinedXTimeSlotLowerConfOfItemOrLig() {
		return combinedXTimeSlotLowerConfOfItemOrLig;
	}
	public void setCombinedXTimeSlotLowerConfOfItemOrLig(long combinedXTimeSlotLowerConfOfItemOrLig) {
		this.combinedXTimeSlotLowerConfOfItemOrLig = combinedXTimeSlotLowerConfOfItemOrLig;
	}
	public Integer getNoOfConsecutiveTimeSlotItemDidNotMove() {
		return noOfConsecutiveTimeSlotItemDidNotMove;
	}
	public void setNoOfConsecutiveTimeSlotItemDidNotMove(Integer noOfConsecutiveTimeSlotItemDidNotMove) {
		this.noOfConsecutiveTimeSlotItemDidNotMove = noOfConsecutiveTimeSlotItemDidNotMove;
	}
	public long getForecastMovOfRestOfTheDayOfItemOrLig() {
		return forecastMovOfRestOfTheDayOfItemOrLig;
	}
	public void setForecastMovOfRestOfTheDayOfItemOrLig(long forecastMovOfRestOfTheDayOfItemOrLig) {
		this.forecastMovOfRestOfTheDayOfItemOrLig = forecastMovOfRestOfTheDayOfItemOrLig;
	}
	public long getForecastMovOfAllPrevTimeSlotsOfItemOrLig() {
		return forecastMovOfAllPrevTimeSlotsOfItemOrLig;
	}
	public void setForecastMovOfAllPrevTimeSlotsOfItemOrLig(long forecastMovOfAllPrevTimeSlotsOfItemOrLig) {
		this.forecastMovOfAllPrevTimeSlotsOfItemOrLig = forecastMovOfAllPrevTimeSlotsOfItemOrLig;
	}
	public long getActualMovOfAllPrevTimeSlotsOfItemOrLig() {
		return actualMovOfAllPrevTimeSlotsOfItemOrLig;
	}
	public void setActualMovOfAllPrevTimeSlotsOfItemOrLig(long actualMovOfAllPrevTimeSlotsOfItemOrLig) {
		this.actualMovOfAllPrevTimeSlotsOfItemOrLig = actualMovOfAllPrevTimeSlotsOfItemOrLig;
	}
	public long getCombinedTimeSlotTrxBasedExpCriteria2OfItemOrLig() {
		return combinedTimeSlotTrxBasedExpCriteria2OfItemOrLig;
	}
	public void setCombinedTimeSlotTrxBasedExpCriteria2OfItemOrLig(long combinedTimeSlotTrxBasedExpCriteria2OfItemOrLig) {
		this.combinedTimeSlotTrxBasedExpCriteria2OfItemOrLig = combinedTimeSlotTrxBasedExpCriteria2OfItemOrLig;
	}
	public long getCombinedTimeSlotTrxBasedExpCriteria3OfItemOrLig() {
		return combinedTimeSlotTrxBasedExpCriteria3OfItemOrLig;
	}
	public void setCombinedTimeSlotTrxBasedExpCriteria3OfItemOrLig(long combinedTimeSlotTrxBasedExpCriteria3OfItemOrLig) {
		this.combinedTimeSlotTrxBasedExpCriteria3OfItemOrLig = combinedTimeSlotTrxBasedExpCriteria3OfItemOrLig;
	}
	public double getShelfCapacityXPCTOfItemOrLig() {
		return shelfCapacityXPCTOfItemOrLig;
	}
	public void setShelfCapacityXPCTOfItemOrLig(double shelfCapacityXPCTOfItemOrLig) {
		this.shelfCapacityXPCTOfItemOrLig = shelfCapacityXPCTOfItemOrLig;
	}
	public long getCombinedTimeSlotActualMovCriteria2OfItemOrLig() {
		return combinedTimeSlotActualMovCriteria2OfItemOrLig;
	}
	public void setCombinedTimeSlotActualMovCriteria2OfItemOrLig(long combinedTimeSlotActualMovCriteria2OfItemOrLig) {
		this.combinedTimeSlotActualMovCriteria2OfItemOrLig = combinedTimeSlotActualMovCriteria2OfItemOrLig;
	}
	public long getCombinedTimeSlotActualMovCriteria3OfItemOrLig() {
		return combinedTimeSlotActualMovCriteria3OfItemOrLig;
	}
	public void setCombinedTimeSlotActualMovCriteria3OfItemOrLig(long combinedTimeSlotActualMovCriteria3OfItemOrLig) {
		this.combinedTimeSlotActualMovCriteria3OfItemOrLig = combinedTimeSlotActualMovCriteria3OfItemOrLig;
	}
	public int getNoOfShelfLocationsOfItemOrLig() {
		return noOfShelfLocationsOfItemOrLig;
	}
	public void setNoOfShelfLocationsOfItemOrLig(int noOfShelfLocationsOfItemOrLig) {
		this.noOfShelfLocationsOfItemOrLig = noOfShelfLocationsOfItemOrLig;
	}
}
