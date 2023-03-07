package com.pristine.dto.offermgmt.promotion;

import java.util.HashMap;

public class AdDetail {
	private String adStartDate = "";
	private int locationLevelId = 0;
	private int locationId = 0;
	private HashMap<Integer, PageDetail> pageMap = new HashMap<Integer, PageDetail>();
	private PredictionMetrics predictionMetrics = new PredictionMetrics();
	private long totalHHReachCnt;
	
	public HashMap<Integer, PageDetail> getPageMap() {
		return pageMap;
	}
	public void setPageMap(HashMap<Integer, PageDetail> pageMap) {
		this.pageMap = pageMap;
	}
	public PredictionMetrics getPredictionMetrics() {
		return predictionMetrics;
	}
	public void setPredictionMetrics(PredictionMetrics predictionMetrics) {
		this.predictionMetrics = predictionMetrics;
	}
	public long getTotalHHReachCnt() {
		return totalHHReachCnt;
	}
	public void setTotalHHReachCnt(long totalHHReachCnt) {
		this.totalHHReachCnt = totalHHReachCnt;
	}
	public String getAdStartDate() {
		return adStartDate;
	}
	public void setAdStartDate(String adStartDate) {
		this.adStartDate = adStartDate;
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
	
	@Override
	public String toString() {
		return "AdDetail [adStartDate=" + adStartDate + ", locationLevelId=" + locationLevelId + ", locationId=" + locationId + ", pageMap=" + pageMap
				+ ", predictionMetrics=" + predictionMetrics + ", totalHHReachCnt=" + totalHHReachCnt + "]";
	}
}
