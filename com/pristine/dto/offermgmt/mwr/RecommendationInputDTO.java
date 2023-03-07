package com.pristine.dto.offermgmt.mwr;

public class RecommendationInputDTO implements Cloneable{

	private int locationLevelId;
	private int locationId;
	private int productLevelId;
	private int productId;
	private String recType;
	private String startWeek;
	private String endWeek;
	private String actualStartWeek;
	private String actualEndWeek;
	private String quarterStartDate;
	private String quarterEndDate;
	private int startCalendarId;
	private int endCalendarId;
	private int actualStartCalendarId;
	private int actualEndCalendarId;
	private long runId;
	private int chainId;
	private int divisionId;
	private int leadZoneId;
	private int leadZoneDivisionId;
	private int noOfWeeksInAdvance;
	private String baseWeek;
	private boolean isOnGoingDateRange;
	private MWRRunHeader mwrRunHeader;
	private String calType;
	private long promoHeaderId;
	private int leadLocationLevelId;
	private int leadLocationId;
	private String startWeekEndDate;
	private char runMode;
	private boolean usePrediction;
	private boolean isGlobalZone;
	private String userId;
	private char runType;
	private long strategyId;
	private long queueId;
	private boolean runOnlyTempStrats=false;
	private boolean priceTestZone=false;
	private int tempLocationID=0;
	
	@Override
	public RecommendationInputDTO clone() throws CloneNotSupportedException {
		return (RecommendationInputDTO) super.clone();
	}
	
	
	public boolean isOnGoingDateRange() {
		return isOnGoingDateRange;
	}
	public void setOnGoingDateRange(boolean isOnGoingDateRange) {
		this.isOnGoingDateRange = isOnGoingDateRange;
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
	public String getRecType() {
		return recType;
	}
	public void setRecType(String recType) {
		this.recType = recType;
	}
	public String getStartWeek() {
		return startWeek;
	}
	public void setStartWeek(String startWeek) {
		this.startWeek = startWeek;
	}
	public String getEndWeek() {
		return endWeek;
	}
	public void setEndWeek(String endWeek) {
		this.endWeek = endWeek;
	}
	public String getQuarterStartDate() {
		return quarterStartDate;
	}
	public void setQuarterStartDate(String quarterStartDate) {
		this.quarterStartDate = quarterStartDate;
	}
	public int getStartCalendarId() {
		return startCalendarId;
	}
	public void setStartCalendarId(int startCalendarId) {
		this.startCalendarId = startCalendarId;
	}
	public int getEndCalendarId() {
		return endCalendarId;
	}
	public void setEndCalendarId(int endCalendarId) {
		this.endCalendarId = endCalendarId;
	}
	public int getActualStartCalendarId() {
		return actualStartCalendarId;
	}
	public void setActualStartCalendarId(int actualStartCalendarId) {
		this.actualStartCalendarId = actualStartCalendarId;
	}
	public int getActualEndCalendarId() {
		return actualEndCalendarId;
	}
	public void setActualEndCalendarId(int actualEndCalendarId) {
		this.actualEndCalendarId = actualEndCalendarId;
	}
	public long getRunId() {
		return runId;
	}
	public void setRunId(long runId) {
		this.runId = runId;
	}
	public int getChainId() {
		return chainId;
	}
	public void setChainId(int chainId) {
		this.chainId = chainId;
	}
	public int getDivisionId() {
		return divisionId;
	}
	public void setDivisionId(int divisionId) {
		this.divisionId = divisionId;
	}
	public int getLeadZoneId() {
		return leadZoneId;
	}
	public void setLeadZoneId(int leadZoneId) {
		this.leadZoneId = leadZoneId;
	}
	public int getLeadZoneDivisionId() {
		return leadZoneDivisionId;
	}
	public void setLeadZoneDivisionId(int leadZoneDivisionId) {
		this.leadZoneDivisionId = leadZoneDivisionId;
	}
	public String getBaseWeek() {
		return baseWeek;
	}
	public void setBaseWeek(String baseWeek) {
		this.baseWeek = baseWeek;
	}
	public String getQuarterEndDate() {
		return quarterEndDate;
	}
	public void setQuarterEndDate(String quarterEndDate) {
		this.quarterEndDate = quarterEndDate;
	}
	public int getNoOfWeeksInAdvance() {
		return noOfWeeksInAdvance;
	}
	public void setNoOfWeeksInAdvance(int noOfWeeksInAdvance) {
		this.noOfWeeksInAdvance = noOfWeeksInAdvance;
	}
	public String getActualStartWeek() {
		return actualStartWeek;
	}
	public void setActualStartWeek(String actualStartWeek) {
		this.actualStartWeek = actualStartWeek;
	}
	public String getActualEndWeek() {
		return actualEndWeek;
	}
	public void setActualEndWeek(String actualEndWeek) {
		this.actualEndWeek = actualEndWeek;
	}
	public MWRRunHeader getMwrRunHeader() {
		return mwrRunHeader;
	}
	public void setMwrRunHeader(MWRRunHeader mwrRunHeader) {
		this.mwrRunHeader = mwrRunHeader;
	}
	public String getCalType() {
		return calType;
	}
	public void setCalType(String calType) {
		this.calType = calType;
	}
	public long getPromoHeaderId() {
		return promoHeaderId;
	}
	public void setPromoHeaderId(long promoHeaderId) {
		this.promoHeaderId = promoHeaderId;
	}
	public int getLeadLocationLevelId() {
		return leadLocationLevelId;
	}
	public void setLeadLocationLevelId(int leadLocationLevelId) {
		this.leadLocationLevelId = leadLocationLevelId;
	}
	public int getLeadLocationId() {
		return leadLocationId;
	}
	public void setLeadLocationId(int leadLocationId) {
		this.leadLocationId = leadLocationId;
	}
	public String getStartWeekEndDate() {
		return startWeekEndDate;
	}
	public void setStartWeekEndDate(String startWeekEndDate) {
		this.startWeekEndDate = startWeekEndDate;
	}
	@Override
	public String toString() {
		return "RecommendationInputDTO [locationLevelId=" + locationLevelId + ", locationId=" + locationId
				+ ", productLevelId=" + productLevelId + ", productId=" + productId + ", recType=" + recType
				+ ", startWeek=" + startWeek + ", endWeek=" + endWeek + ", actualStartWeek=" + actualStartWeek
				+ ", actualEndWeek=" + actualEndWeek + ", quarterStartDate=" + quarterStartDate + ", quarterEndDate="
				+ quarterEndDate + "]";
	}
	public char getRunMode() {
		return runMode;
	}
	public void setRunMode(char runMode) {
		this.runMode = runMode;
	}
	public boolean isUsePrediction() {
		return usePrediction;
	}
	public void setUsePrediction(boolean usePrediction) {
		this.usePrediction = usePrediction;
	}
	public boolean isGlobalZone() {
		return isGlobalZone;
	}
	public void setGlobalZone(boolean isGlobalZone) {
		this.isGlobalZone = isGlobalZone;
	}
	public String getUserId() {
		return userId;
	}
	public void setUserId(String userId) {
		this.userId = userId;
	}
	public char getRunType() {
		return runType;
	}
	public void setRunType(char runType) {
		this.runType = runType;
	}
	public long getStrategyId() {
		return strategyId;
	}
	public void setStrategyId(long strategyId) {
		this.strategyId = strategyId;
	}
	public long getQueueId() {
		return queueId;
	}
	public void setQueueId(long queueId) {
		this.queueId = queueId;
	}
	public boolean isRunOnlyTempStrats() {
		return runOnlyTempStrats;
	}
	public void setRunOnlyTempStrats(boolean runOnlyTempStrats) {
		this.runOnlyTempStrats = runOnlyTempStrats;
	}
	public boolean isPriceTestZone() {
		return priceTestZone;
	}
	public void setPriceTestZone(boolean priceTestZone) {
		this.priceTestZone = priceTestZone;
	}
	
	public int getTempLocationID() {
		return tempLocationID;
	}
	public void setTempLocationID(int tempLocationID) {
		this.tempLocationID = tempLocationID;
	}
}
