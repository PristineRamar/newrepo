package com.pristine.dto;

import java.util.Date;

public class StoreDTO implements IValueObject {
	public int strId;
	public String strNum;
	public int chainId;
	public String chainName;

	public String strName;
	public String addrLine1;
	public String addrLine2;
	public String city;
	public String state;
	public String zip;
	public String timeZone;
	public String is24HrInd;
	public String pharmacyInd;
	public String gasStationInd;
	public String bankInd;
	public String fastFoodInd;
	public String coffeeShopInd;
	public String isActiveInd;
	public String gps_lat;
	public String gps_long;
	public String gblLocNum;//GLN
	
	public String zoneNum;
	public String zoneName;
	public int zoneId;
	
	public String storeMgrName;
	public String storePhoneNo;
	public String storeFaxNo;
	
	public String distName;
	public String distMgrName;
	public String distPhoneNo;
	public String distFaxNo;
	public String distNum;
	public int 	  distId;

	public String regName;
	public String regMgrName;
	public String regPhoneNo;
	public String regFaxNo;
	public String regNum;
	public int regId;

	public String divName;
	public String divMgrName;
	public String divPhoneNo;
	public String divFaxNo;
	public String divNum;
	public int divId;
	
	public String storeClass;
	public String storeType;
	public String addlType1;
	public String addlType2;
	
	public String storeOpenDate;
	public String storeReModelDate;
	public String storeAcqDate;
	public String storeCloseDate;
	public String storeAnnvDate;
	
	public float sqFootage;
	
	public String dept1ZoneNum;
	public String dept2ZoneNum;
	public String dept3ZoneNum;
	public String storeComment;
	public Date storeCloseDateAsDate;
	
	private int priceZoneId;
	private int priceZoneId3;
	
	public int getPriceZoneId() {
		return priceZoneId;
	}



	public void setPriceZoneId(int priceZoneId) {
		this.priceZoneId = priceZoneId;
	}



	public int getPriceZoneId3() {
		return priceZoneId3;
	}



	public void setPriceZoneId3(int priceZoneId3) {
		this.priceZoneId3 = priceZoneId3;
	}

	public String sourceInfo;



	public int getStrId() {
		return strId;
	}



	public void setStrId(int strId) {
		this.strId = strId;
	}



	public String getStrNum() {
		return strNum;
	}



	public void setStrNum(String strNum) {
		this.strNum = strNum;
	}



	public int getChainId() {
		return chainId;
	}



	public void setChainId(int chainId) {
		this.chainId = chainId;
	}



	public String getChainName() {
		return chainName;
	}



	public void setChainName(String chainName) {
		this.chainName = chainName;
	}



	public String getStrName() {
		return strName;
	}



	public void setStrName(String strName) {
		this.strName = strName;
	}



	public String getAddrLine1() {
		return addrLine1;
	}



	public void setAddrLine1(String addrLine1) {
		this.addrLine1 = addrLine1;
	}



	public String getAddrLine2() {
		return addrLine2;
	}



	public void setAddrLine2(String addrLine2) {
		this.addrLine2 = addrLine2;
	}



	public String getCity() {
		return city;
	}



	public void setCity(String city) {
		this.city = city;
	}



	public String getState() {
		return state;
	}



	public void setState(String state) {
		this.state = state;
	}



	public String getZip() {
		return zip;
	}



	public void setZip(String zip) {
		this.zip = zip;
	}



	public String getTimeZone() {
		return timeZone;
	}



	public void setTimeZone(String timeZone) {
		this.timeZone = timeZone;
	}



	public String getIs24HrInd() {
		return is24HrInd;
	}



	public void setIs24HrInd(String is24HrInd) {
		this.is24HrInd = is24HrInd;
	}



	public String getPharmacyInd() {
		return pharmacyInd;
	}



	public void setPharmacyInd(String pharmacyInd) {
		this.pharmacyInd = pharmacyInd;
	}



	public String getGasStationInd() {
		return gasStationInd;
	}



	public void setGasStationInd(String gasStationInd) {
		this.gasStationInd = gasStationInd;
	}



	public String getBankInd() {
		return bankInd;
	}



	public void setBankInd(String bankInd) {
		this.bankInd = bankInd;
	}



	public String getFastFoodInd() {
		return fastFoodInd;
	}



	public void setFastFoodInd(String fastFoodInd) {
		this.fastFoodInd = fastFoodInd;
	}



	public String getCoffeeShopInd() {
		return coffeeShopInd;
	}



	public void setCoffeeShopInd(String coffeeShopInd) {
		this.coffeeShopInd = coffeeShopInd;
	}



	public String getIsActiveInd() {
		return isActiveInd;
	}



	public void setIsActiveInd(String isActiveInd) {
		this.isActiveInd = isActiveInd;
	}



	public String getGps_lat() {
		return gps_lat;
	}



	public void setGps_lat(String gps_lat) {
		this.gps_lat = gps_lat;
	}



	public String getGps_long() {
		return gps_long;
	}



	public void setGps_long(String gps_long) {
		this.gps_long = gps_long;
	}



	public String getGblLocNum() {
		return gblLocNum;
	}



	public void setGblLocNum(String gblLocNum) {
		this.gblLocNum = gblLocNum;
	}



	public String getZoneNum() {
		return zoneNum;
	}



	public void setZoneNum(String zoneNum) {
		this.zoneNum = zoneNum;
	}



	public String getZoneName() {
		return zoneName;
	}



	public void setZoneName(String zoneName) {
		this.zoneName = zoneName;
	}



	public int getZoneId() {
		return zoneId;
	}



	public void setZoneId(int zoneId) {
		this.zoneId = zoneId;
	}



	public String getStoreMgrName() {
		return storeMgrName;
	}



	public void setStoreMgrName(String storeMgrName) {
		this.storeMgrName = storeMgrName;
	}



	public String getStorePhoneNo() {
		return storePhoneNo;
	}



	public void setStorePhoneNo(String storePhoneNo) {
		this.storePhoneNo = storePhoneNo;
	}



	public String getStoreFaxNo() {
		return storeFaxNo;
	}



	public void setStoreFaxNo(String storeFaxNo) {
		this.storeFaxNo = storeFaxNo;
	}



	public String getDistName() {
		return distName;
	}



	public void setDistName(String distName) {
		this.distName = distName;
	}



	public String getDistMgrName() {
		return distMgrName;
	}



	public void setDistMgrName(String distMgrName) {
		this.distMgrName = distMgrName;
	}



	public String getDistPhoneNo() {
		return distPhoneNo;
	}



	public void setDistPhoneNo(String distPhoneNo) {
		this.distPhoneNo = distPhoneNo;
	}



	public String getDistFaxNo() {
		return distFaxNo;
	}



	public void setDistFaxNo(String distFaxNo) {
		this.distFaxNo = distFaxNo;
	}



	public String getDistNum() {
		return distNum;
	}



	public void setDistNum(String distNum) {
		this.distNum = distNum;
	}



	public int getDistId() {
		return distId;
	}



	public void setDistId(int distId) {
		this.distId = distId;
	}



	public String getRegName() {
		return regName;
	}



	public void setRegName(String regName) {
		this.regName = regName;
	}



	public String getRegMgrName() {
		return regMgrName;
	}



	public void setRegMgrName(String regMgrName) {
		this.regMgrName = regMgrName;
	}



	public String getRegPhoneNo() {
		return regPhoneNo;
	}



	public void setRegPhoneNo(String regPhoneNo) {
		this.regPhoneNo = regPhoneNo;
	}



	public String getRegFaxNo() {
		return regFaxNo;
	}



	public void setRegFaxNo(String regFaxNo) {
		this.regFaxNo = regFaxNo;
	}



	public String getRegNum() {
		return regNum;
	}



	public void setRegNum(String regNum) {
		this.regNum = regNum;
	}



	public int getRegId() {
		return regId;
	}



	public void setRegId(int regId) {
		this.regId = regId;
	}



	public String getDivName() {
		return divName;
	}



	public void setDivName(String divName) {
		this.divName = divName;
	}



	public String getDivMgrName() {
		return divMgrName;
	}



	public void setDivMgrName(String divMgrName) {
		this.divMgrName = divMgrName;
	}



	public String getDivPhoneNo() {
		return divPhoneNo;
	}



	public void setDivPhoneNo(String divPhoneNo) {
		this.divPhoneNo = divPhoneNo;
	}



	public String getDivFaxNo() {
		return divFaxNo;
	}



	public void setDivFaxNo(String divFaxNo) {
		this.divFaxNo = divFaxNo;
	}



	public String getDivNum() {
		return divNum;
	}



	public void setDivNum(String divNum) {
		this.divNum = divNum;
	}



	public int getDivId() {
		return divId;
	}



	public void setDivId(int divId) {
		this.divId = divId;
	}



	public String getStoreClass() {
		return storeClass;
	}



	public void setStoreClass(String storeClass) {
		this.storeClass = storeClass;
	}



	public String getStoreType() {
		return storeType;
	}



	public void setStoreType(String storeType) {
		this.storeType = storeType;
	}



	public String getAddlType1() {
		return addlType1;
	}



	public void setAddlType1(String addlType1) {
		this.addlType1 = addlType1;
	}



	public String getAddlType2() {
		return addlType2;
	}



	public void setAddlType2(String addlType2) {
		this.addlType2 = addlType2;
	}



	public String getStoreOpenDate() {
		return storeOpenDate;
	}



	public void setStoreOpenDate(String storeOpenDate) {
		this.storeOpenDate = storeOpenDate;
	}



	public String getStoreReModelDate() {
		return storeReModelDate;
	}



	public void setStoreReModelDate(String storeReModelDate) {
		this.storeReModelDate = storeReModelDate;
	}



	public String getStoreAcqDate() {
		return storeAcqDate;
	}



	public void setStoreAcqDate(String storeAcqDate) {
		this.storeAcqDate = storeAcqDate;
	}



	public String getStoreCloseDate() {
		return storeCloseDate;
	}



	public void setStoreCloseDate(String storeCloseDate) {
		this.storeCloseDate = storeCloseDate;
	}



	public String getStoreAnnvDate() {
		return storeAnnvDate;
	}



	public void setStoreAnnvDate(String storeAnnvDate) {
		this.storeAnnvDate = storeAnnvDate;
	}



	public float getSqFootage() {
		return sqFootage;
	}



	public void setSqFootage(float sqFootage) {
		this.sqFootage = sqFootage;
	}



	public String getDept1ZoneNum() {
		return dept1ZoneNum;
	}



	public void setDept1ZoneNum(String dept1ZoneNum) {
		this.dept1ZoneNum = dept1ZoneNum;
	}



	public String getDept2ZoneNum() {
		return dept2ZoneNum;
	}



	public void setDept2ZoneNum(String dept2ZoneNum) {
		this.dept2ZoneNum = dept2ZoneNum;
	}



	public String getDept3ZoneNum() {
		return dept3ZoneNum;
	}



	public void setDept3ZoneNum(String dept3ZoneNum) {
		this.dept3ZoneNum = dept3ZoneNum;
	}



	public String getStoreComment() {
		return storeComment;
	}



	public void setStoreComment(String storeComment) {
		this.storeComment = storeComment;
	}



	public Date getStoreCloseDateAsDate() {
		return storeCloseDateAsDate;
	}



	public void setStoreCloseDateAsDate(Date storeCloseDateAsDate) {
		this.storeCloseDateAsDate = storeCloseDateAsDate;
	}



	public String getSourceInfo() {
		return sourceInfo;
	}



	public void setSourceInfo(String sourceInfo) {
		this.sourceInfo = sourceInfo;
	}
	
	public StoreDTO() {
		// TODO Auto-generated constructor stub
	}

	public StoreDTO(String strNum, int strId,  int zoneId) {
		super();
		this.strId = strId;
		this.strNum = strNum;
		this.zoneId = zoneId;
	}
}