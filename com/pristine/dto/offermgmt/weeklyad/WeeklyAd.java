package com.pristine.dto.offermgmt.weeklyad;

import java.text.DecimalFormat;
import java.util.Date;
import java.util.TreeMap;


public class WeeklyAd {
	private long adId;
	private String adName;
	private int locationLevelId;
	private int locationId;
	private int calendarId;
	private int totalPages;
	private int status;
	private String createdBy;
	private String modifiedBy;
	private String approvedBy;
	private String adNumber;
	private String weekStartDate;
	private Date startDate;

	private TreeMap<Integer, WeeklyAdPage> adPages = new TreeMap<Integer, WeeklyAdPage>();
	
	public long getAdId() {
		return adId;
	}

	public void setAdId(long adId) {
		this.adId = adId;
	}

	public String getAdName() {
		return adName;
	}

	public void setAdName(String adName) {
		this.adName = adName;
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

	public int getTotalPages() {
		return totalPages;
	}

	public void setTotalPages(int totalPages) {
		this.totalPages = totalPages;
	}

	public int getStatus() {
		return status;
	}

	public void setStatus(int status) {
		this.status = status;
	}

	public String getCreatedBy() {
		return createdBy;
	}

	public void setCreatedBy(String createdBy) {
		this.createdBy = createdBy;
	}

	public String getModifiedBy() {
		return modifiedBy;
	}

	public void setModifiedBy(String modifiedBy) {
		this.modifiedBy = modifiedBy;
	}

	public String getApprovedBy() {
		return approvedBy;
	}

	public void setApprovedBy(String approvedBy) {
		this.approvedBy = approvedBy;
	}
	
	public String getAdNumber() {
		return adNumber;
	}

	public void setAdNumber(String adNumber) {
		this.adNumber = adNumber;
	}

	public TreeMap<Integer, WeeklyAdPage> getAdPages() {
		return adPages;
	}

	public void setAdPages(TreeMap<Integer, WeeklyAdPage> adPages) {
		this.adPages = adPages;
	}
	
	public void addPage(WeeklyAdPage adPage){
		this.adPages.put(adPage.getPageNumber(), adPage);
	}
	
	public String getWeekStartDate() {
		return weekStartDate;
	}

	public void setWeekStartDate(String weekStartDate) {
		this.weekStartDate = weekStartDate;
	}

	public Date getStartDate() {
		return startDate;
	}

	public void setStartDate(Date startDate) {
		this.startDate = startDate;
	}
}
