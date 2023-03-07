package com.pristine.dto;

public class PriceCheckStatsDTO implements IValueObject {

	/*  Vaibhav 2 - This is a Data transfer Object
	 *  Map the PerformanceStatsView Elements
	 */ 
	
	private int scheduleId;
	private String priceCheckerId;
	private String priceCheckerName;
	private String startDate;
	private String endDate;
	private String compStrNo;
	private String address1;
	private String address2;
	private String city;
	private String state;
	private String ZIP;
	private int compChainId;
	private String compChainName;
	private int itemCT;
	private int itemsCheckedCT;
	private float itemsCheckedPCT;
	private int itemsNotCheckedCT;
	private float itemsNotCheckedPCT;
	private int itemsNotFoundCT;
	private float itemsNotFoundPCT;
	private int priceNotFoundCT;
	private float priceNotFoundPCT;
	private int outOfRangeCT;
	private float outOfRangePCT;
	private int outOfReasonabilityCT;
	private float outOfReasonabilityPCT;
	private float duration;
	private float avgItemsPerHr;
	private int onSaleCT;
	private float onSalePCT;
	private int wentUpCT;
	private float wentUpPCT;
	private int wentDownCT;
	private float wentDownPCT;
	private int noChangeXTimesCT;
	private float noChangeXPCT;
	private int noChangeCT;
	private float noChangePCT;
	private String startTime;
	private String endTime;
	private String gpsLat;
	private String gpsLong;
	private String gpsViolation;
	private int weekThreshold;
	private int compStrId;
	private String statusChgDate;
	private String currentStatus;
	private int performanceGoalId;
	private String itemsNotCheckedPASS;
	private String itemsNotFoundPASS;
	private String avgItemsPerHrPASS;
	private String itemsOnSalePASS;
	private String rangeCheckPASS;
	private String reasonabilityCheckPASS;
	private String itemsFixedLaterPASS;
	private int noOfItemsFixedLater;
	private int goalId;
	private int plItemsCount;
	public int getScheduleId()
	{
		return scheduleId;
	}
	public void setScheduleId(int scheduleId)
	{
		this.scheduleId = scheduleId;
	}
	public String getPriceCheckerId()
	{
		return priceCheckerId;
	}
	public void setPriceCheckerId(String priceCheckerId)
	{
		this.priceCheckerId = priceCheckerId;
	}
	public String getPriceCheckerName()
	{
		return priceCheckerName;
	}
	public void setPriceCheckerName(String priceCheckerName)
	{
		this.priceCheckerName = priceCheckerName;
	}
	public String getStartDate()
	{
		return startDate;
	}
	public void setStartDate(String startDate)
	{
		this.startDate = startDate;
	}
	public String getEndDate()
	{
		return endDate;
	}
	public void setEndDate(String endDate)
	{
		this.endDate = endDate;
	}
	public String getCompStrNo()
	{
		return compStrNo;
	}
	public void setCompStrNo(String compStrNo)
	{
		this.compStrNo = compStrNo;
	}
	public String getAddress1()
	{
		return address1;
	}
	public void setAddress1(String address1)
	{
		this.address1 = address1;
	}
	public String getAddress2()
	{
		return address2;
	}
	public void setAddress2(String address2)
	{
		this.address2 = address2;
	}
	public String getCity()
	{
		return city;
	}
	public void setCity(String city)
	{
		this.city = city;
	}
	public String getState()
	{
		return state;
	}
	public void setState(String state)
	{
		this.state = state;
	}
	public String getZIP()
	{
		return ZIP;
	}
	public void setZIP(String zIP)
	{
		ZIP = zIP;
	}
	public int getCompChainId()
	{
		return compChainId;
	}
	public void setCompChainId(int compChainId)
	{
		this.compChainId = compChainId;
	}
	public String getCompChainName()
	{
		return compChainName;
	}
	public void setCompChainName(String compChainName)
	{
		this.compChainName = compChainName;
	}
	public int getItemCT()
	{
		return itemCT;
	}
	public void setItemCT(int itemCT)
	{
		this.itemCT = itemCT;
	}
	public int getItemsCheckedCT()
	{
		return itemsCheckedCT;
	}
	public void setItemsCheckedCT(int itemsCheckedCT)
	{
		this.itemsCheckedCT = itemsCheckedCT;
	}
	public float getItemsCheckedPCT()
	{
		return itemsCheckedPCT;
	}
	public void setItemsCheckedPCT(float itemsCheckedPCT)
	{
		this.itemsCheckedPCT = itemsCheckedPCT;
	}
	public int getItemsNotCheckedCT()
	{
		return itemsNotCheckedCT;
	}
	public void setItemsNotCheckedCT(int itemsNotCheckedCT)
	{
		this.itemsNotCheckedCT = itemsNotCheckedCT;
	}
	public float getItemsNotCheckedPCT()
	{
		return itemsNotCheckedPCT;
	}
	public void setItemsNotCheckedPCT(float itemsNotCheckedPCT)
	{
		this.itemsNotCheckedPCT = itemsNotCheckedPCT;
	}
	public int getItemsNotFoundCT()
	{
		return itemsNotFoundCT;
	}
	public void setItemsNotFoundCT(int itemsNotFoundCT)
	{
		this.itemsNotFoundCT = itemsNotFoundCT;
	}
	public float getItemsNotFoundPCT()
	{
		return itemsNotFoundPCT;
	}
	public void setItemsNotFoundPCT(float itemsNotFoundPCT)
	{
		this.itemsNotFoundPCT = itemsNotFoundPCT;
	}
	public int getPriceNotFoundCT()
	{
		return priceNotFoundCT;
	}
	public void setPriceNotFoundCT(int priceNotFoundCT)
	{
		this.priceNotFoundCT = priceNotFoundCT;
	}
	public float getPriceNotFoundPCT()
	{
		return priceNotFoundPCT;
	}
	public void setPriceNotFoundPCT(float priceNotFoundPCT)
	{
		this.priceNotFoundPCT = priceNotFoundPCT;
	}
	public int getOutOfRangeCT()
	{
		return outOfRangeCT;
	}
	public void setOutOfRangeCT(int outOfRangeCT)
	{
		this.outOfRangeCT = outOfRangeCT;
	}
	public float getOutOfRangePCT()
	{
		return outOfRangePCT;
	}
	public void setOutOfRangePCT(float outOfRangePCT)
	{
		this.outOfRangePCT = outOfRangePCT;
	}
	public int getOutOfReasonabilityCT()
	{
		return outOfReasonabilityCT;
	}
	public void setOutOfReasonabilityCT(int outOfReasonabilityCT)
	{
		this.outOfReasonabilityCT = outOfReasonabilityCT;
	}
	public float getOutOfReasonabilityPCT()
	{
		return outOfReasonabilityPCT;
	}
	public void setOutOfReasonabilityPCT(float outOfReasonabilityPCT)
	{
		this.outOfReasonabilityPCT = outOfReasonabilityPCT;
	}
	public float getDuration()
	{
		return duration;
	}
	public void setDuration(float duration)
	{
		this.duration = duration;
	}
	public float getAvgItemsPerHr()
	{
		return avgItemsPerHr;
	}
	public void setAvgItemsPerHr(float avgItemsPerHr)
	{
		this.avgItemsPerHr = avgItemsPerHr;
	}
	public int getOnSaleCT()
	{
		return onSaleCT;
	}
	public void setOnSaleCT(int onSaleCT)
	{
		this.onSaleCT = onSaleCT;
	}
	public float getOnSalePCT()
	{
		return onSalePCT;
	}
	public void setOnSalePCT(float onSalePCT)
	{
		this.onSalePCT = onSalePCT;
	}
	public int getWentUpCT()
	{
		return wentUpCT;
	}
	public void setWentUpCT(int wentUpCT)
	{
		this.wentUpCT = wentUpCT;
	}
	public float getWentUpPCT()
	{
		return wentUpPCT;
	}
	public void setWentUpPCT(float wentUpPCT)
	{
		this.wentUpPCT = wentUpPCT;
	}
	public int getWentDownCT()
	{
		return wentDownCT;
	}
	public void setWentDownCT(int wentDownCT)
	{
		this.wentDownCT = wentDownCT;
	}
	public float getWentDownPCT()
	{
		return wentDownPCT;
	}
	public void setWentDownPCT(float wendDownPCT)
	{
		this.wentDownPCT = wendDownPCT;
	}
	public int getNoChangeXTimesCT()
	{
		return noChangeXTimesCT;
	}
	public void setNoChangeXTimesCT(int noChangeXTimesCT)
	{
		this.noChangeXTimesCT = noChangeXTimesCT;
	}
	public float getNoChangeXPCT()
	{
		return noChangeXPCT;
	}
	public void setNoChangeXPCT(float noChangeXPCT)
	{
		this.noChangeXPCT = noChangeXPCT;
	}
	public String getStartTime()
	{
		return startTime;
	}
	public void setStartTime(String startTime)
	{
		this.startTime = startTime;
	}
	public String getEndTime()
	{
		return endTime;
	}
	public void setEndTime(String endTime)
	{
		this.endTime = endTime;
	}
	public String getGpsLat()
	{
		return gpsLat;
	}
	public void setGpsLat(String gpsLat)
	{
		this.gpsLat = gpsLat;
	}
	public String getGpsLong()
	{
		return gpsLong;
	}
	public void setGpsLong(String gpsLong)
	{
		this.gpsLong = gpsLong;
	}
	public String getGpsViolation()
	{
		return gpsViolation;
	}
	public void setGpsViolation(String gpsViolation)
	{
		this.gpsViolation = gpsViolation;
	}
	public int getWeekThreshold()
	{
		return weekThreshold;
	}
	public void setWeekThreshold(int weekThreshold)
	{
		this.weekThreshold = weekThreshold;
	}
	public int getCompStrId()
	{
		return compStrId;
	}
	public void setCompStrId(int compStrId)
	{
		this.compStrId = compStrId;
	}
	public String getStatusChgDate()
	{
		return statusChgDate;
	}
	public void setStatusChgDate(String statusChgDate)
	{
		this.statusChgDate = statusChgDate;
	}
	public String getCurrentStatus()
	{
		return currentStatus;
	}
	public void setCurrentStatus(String currentStatus)
	{
		this.currentStatus = currentStatus;
	}
	public int getNoChangeCT()
	{
		return noChangeCT;
	}
	public void setNoChangeCT(int noChangeCT)
	{
		this.noChangeCT = noChangeCT;
	}
	public float getNoChangePCT()
	{
		return noChangePCT;
	}
	public void setNoChangePCT(float noChangePCT)
	{
		this.noChangePCT = noChangePCT;
	}
	public int getPerformanceGoalId()
	{
		return performanceGoalId;
	}
	public void setPerformanceGoalId(int performanceGoalId)
	{
		this.performanceGoalId = performanceGoalId;
	}
	public String getItemsNotCheckedPASS()
	{
		return itemsNotCheckedPASS;
	}
	public void setItemsNotCheckedPASS(String itemsNotCheckedPASS)
	{
		this.itemsNotCheckedPASS = itemsNotCheckedPASS;
	}
	public String getItemsNotFoundPASS()
	{
		return itemsNotFoundPASS;
	}
	public void setItemsNotFoundPASS(String itemsNotFoundPASS)
	{
		this.itemsNotFoundPASS = itemsNotFoundPASS;
	}
	public String getAvgItemsPerHrPASS()
	{
		return avgItemsPerHrPASS;
	}
	public void setAvgItemsPerHrPASS(String avgItemsPerHrPASS)
	{
		this.avgItemsPerHrPASS = avgItemsPerHrPASS;
	}
	public String getItemsOnSalePASS()
	{
		return itemsOnSalePASS;
	}
	public void setItemsOnSalePASS(String itemsOnSalePASS)
	{
		this.itemsOnSalePASS = itemsOnSalePASS;
	}
	public String getRangeCheckPASS()
	{
		return rangeCheckPASS;
	}
	public void setRangeCheckPASS(String rangeCheckPASS)
	{
		this.rangeCheckPASS = rangeCheckPASS;
	}
	public String getReasonabilityCheckPASS()
	{
		return reasonabilityCheckPASS;
	}
	public void setReasonabilityCheckPASS(String reasonabilityCheckPASS)
	{
		this.reasonabilityCheckPASS = reasonabilityCheckPASS;
	}
	public String getItemsFixedLaterPASS()
	{
		return itemsFixedLaterPASS;
	}
	public void setItemsFixedLaterPASS(String itemsFixedLaterPASS)
	{
		this.itemsFixedLaterPASS = itemsFixedLaterPASS;
	}
	public int getNoOfItemsFixedLater()
	{
		return noOfItemsFixedLater;
	}
	public void setNoOfItemsFixedLater(int noOfItemsFixedLater)
	{
		this.noOfItemsFixedLater = noOfItemsFixedLater;
	}
	
	public int getGoalId()
	{
		return goalId;
	}
	public void setGoalId(int goalId)
	{
		this.goalId = goalId;
	}
	public int getPlItemsCount() {
		return plItemsCount;
	}
	public void setPlItemsCount(int plItemsCount) {
		this.plItemsCount = plItemsCount;
	}
	
	
	
	
	
	
}
