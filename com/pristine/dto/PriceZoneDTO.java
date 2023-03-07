package com.pristine.dto;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;

public class PriceZoneDTO {
	private int priceZoneId;
	private String priceZoneName;
	private String priceZoneNum;
	private int primaryCompetitor;
	private	int secComp1;
	private	int secComp2;
	private int secComp3;
	private List<Integer> compStrId = new ArrayList<Integer>();
	private List<String> compStrNo = new ArrayList<String>();
	private HashMap<String, List<String>> deptZoneStoreMap = null; 
	private int locationLevelId;
	private int locationId;
	private boolean isGlobalZone;
	private boolean isTestZone;
	
	public int getPriceZoneId() {
		return priceZoneId;
	}
	public void setPriceZoneId(int priceZoneId) {
		this.priceZoneId = priceZoneId;
	}
	public String getPriceZoneName() {
		return priceZoneName;
	}
	public void setPriceZoneName(String priceZoneName) {
		this.priceZoneName = priceZoneName;
	}
	public String getPriceZoneNum() {
		return priceZoneNum;
	}
	public void setPriceZoneNum(String priceZoneNum) {
		this.priceZoneNum = priceZoneNum;
	}
	public int getPrimaryCompetitor() {
		return primaryCompetitor;
	}
	public void setPrimaryCompetitor(int primaryCompetitor) {
		this.primaryCompetitor = primaryCompetitor;
	}
	public int getSecComp1() {
		return secComp1;
	}
	public void setSecComp1(int secComp1) {
		this.secComp1 = secComp1;
	}
	public int getSecComp2() {
		return secComp2;
	}
	public void setSecComp2(int secComp2) {
		this.secComp2 = secComp2;
	}
	public int getSecComp3() {
		return secComp3;
	}
	public void setSecComp3(int secComp3) {
		this.secComp3 = secComp3;
	}
	public List<Integer> getCompStrId() {
		return compStrId;
	}
	public void setCompStrId(List<Integer> compStrId) {
		this.compStrId = compStrId;
	}
	public List<String> getCompStrNo() {
		return compStrNo;
	}
	public void setCompStrNo(List<String> compStrNo) {
		this.compStrNo = compStrNo;
	}
	public HashMap<String, List<String>> getDeptZoneStoreMap() {
		return deptZoneStoreMap;
	}
	public void setDeptZoneStoreMap(HashMap<String, List<String>> deptZoneStoreMap) {
		this.deptZoneStoreMap = deptZoneStoreMap;
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
	public boolean isGlobalZone() {
		return isGlobalZone;
	}
	public void setGlobalZone(boolean isGlobalZone) {
		this.isGlobalZone = isGlobalZone;
	}
	public boolean isTestZone() {
		return isTestZone;
	}
	public void setTestZone(boolean isTestZone) {
		this.isTestZone = isTestZone;
	}
	
	@Override
	public String toString() {
		return "PriceZoneDTO [priceZoneId=" + priceZoneId + ", priceZoneName=" + priceZoneName + ", priceZoneNum="
				+ priceZoneNum + ", primaryCompetitor=" + primaryCompetitor + ", secComp1=" + secComp1 + ", secComp2="
				+ secComp2 + ", secComp3=" + secComp3 + ", compStrId=" + compStrId + ", compStrNo=" + compStrNo
				+ ", deptZoneStoreMap=" + deptZoneStoreMap + ", locationLevelId=" + locationLevelId + ", locationId="
				+ locationId + ", isGlobalZone=" + isGlobalZone + ", isTestZone=" + isTestZone + "]";
	}
}
