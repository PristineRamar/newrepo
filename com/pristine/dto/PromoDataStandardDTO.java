package com.pristine.dto;

import java.time.LocalDate;
import java.util.ArrayList;
import java.util.Date;
import java.util.HashMap;
import java.util.List;

import com.pristine.dto.offermgmt.promotion.PromoBuyItems;
import com.pristine.dto.offermgmt.promotion.PromoBuyRequirement;
import com.pristine.dto.offermgmt.promotion.PromoLocation;
import com.pristine.util.Constants;
import com.pristine.util.DateUtil;
import com.pristine.util.PrestoUtil;

public class PromoDataStandardDTO implements Cloneable {

	String category;
	String promoStartDate;
	String promoEndDate;
	String promoID;
	String promoDescription;
	String itemCode;
	String upc;
	String ItemName;
	String lirName;
	String promoGroup;
	String everdayQty;
	String everydayPrice;
	int saleQty;
	double salePrice;
	int mustBuyQty;
	double mustbuyPrice;
	double dollarOff;
	double pctOff;
	int buyQty;
	int getQty;
	int minimumQty;
	double minimumAmt;
	double bmsmDollaroffperunits;
	double bmsmPctoffperunit;
	int bmsmsaleQty;
	double bmsmsalePrice;
	String status;
	String locationLevel;
	String locationNo;
	String pageNumber;
	String blockNumber;
	String displayOffer;
	String description;
	int startcalendarID;
	int endcalendarID;
	String prestoItemCode;
	String couponType;
	double couponAmt;
	String offerItemCode;
	String offerItemUpc;
	String TypeCode;
	String anotherItem;
	
	String couponType2;
	double couponAmt2;
	private String zoneNo;
	List<String>starDateList=new ArrayList<String>();
	List<String>endDateList=new ArrayList<String>();
	List<String>prestoItemCodeList=new ArrayList<String>();
	
	HashMap<String,String>RegpriceMap=new HashMap<String,String>();
	HashMap<String,List<String>>itemAndItsDetail=new HashMap<String,List<String>>();
	
	//adding for promocorrection
	
	private long promoDefId;
	private int promoTypeId;
	private int promoSubtypeId;
	private String promoName;
	private String eventID;
	private String promoTypeName;
	private int offerCount;
	private double offerValue;
	private String offer_unit_type;
	private String offerType;
	private int locationLevelId;
	private int locationId;
	private String originalStartDate;
	private String originalEndtDate;
	private int offerItem;
	
	
	public HashMap<String, List<String>> getItemAndItsDetail() {
		return itemAndItsDetail;
	}

	public void setItemAndItsDetail(String itemCode, List<String> detail) {
		this.itemAndItsDetail.put(itemCode, detail);
	}

	public List<String> getPrestoItemCodeList() {
		return prestoItemCodeList;
	}

	public void setPrestoItemCodeList(String prestoItm) {
		this.prestoItemCodeList.add(prestoItm);
	}

	public void addRegPrice(String date,String price)
	{
		this.RegpriceMap.put(date, price);
	}
	
	public HashMap<String,String>getPriceMap()

	{
		return RegpriceMap;
	}
	
	public List<String> getStarDateList() {
		return starDateList;
	}

	public void addStarDateList(String starDateList) {
		this.starDateList.add(starDateList);
	}

	public List<String> getEndDateList() {
		return endDateList;
	}

	public void addEndDateList(String endDateList) {
		this.endDateList.add(endDateList);
	}

	public String getZoneNo() {
		return zoneNo;
	}

	public void setZoneNo(String zoneNo) {
		this.zoneNo = zoneNo;
	}

	public String getAnotherItem() {
		return anotherItem;
	}

	public void setAnotherItem(String anotherItem) {
		this.anotherItem = anotherItem;
	}

	public String getTypeCode() {
		return TypeCode;
	}

	public void setTypeCode(String typeCode) {
		TypeCode = typeCode;
	}

	public String getPrestoItemCode() {
		return prestoItemCode;
	}

	public void setPrestoItemCode(String prestoItemCode) {
		this.prestoItemCode = prestoItemCode;
	}

	private List<PromoLocation> promoLocation = new ArrayList<PromoLocation>();

	private List<PromoBuyRequirement> promoBuyRequirement = new ArrayList<PromoBuyRequirement>();

	private List<PromoBuyItems> buyItems = new ArrayList<PromoBuyItems>();

	public List<PromoBuyItems> getBuyItems() {
		return buyItems;
	}

	public void setBuyItems(List<PromoBuyItems> buyItems) {
		this.buyItems = buyItems;
	}

	public List<PromoBuyRequirement> getPromoBuyRequirement() {
		return promoBuyRequirement;
	}

	public void setPromoBuyRequirement(List<PromoBuyRequirement> promoBuyRequirement) {
		this.promoBuyRequirement = promoBuyRequirement;
	}

	public List<PromoLocation> getPromoLocation() {
		return promoLocation;
	}

	public void setPromoLocation(List<PromoLocation> promoLocation) {
		this.promoLocation = promoLocation;
	}

	private Date pStartDate;

	public Date getpStartDate() {
		return pStartDate;
	}

	public void setpStartDate(Date pStartDate) {
		this.pStartDate = pStartDate;
	}

	public Date getpEndDate() {
		return pEndDate;
	}

	public void setpEndDate(Date pEndDate) {
		this.pEndDate = pEndDate;
	}

	private Date pEndDate;

	public int getStartcalendarID() {
		return startcalendarID;
	}

	public void setStartcalendarID(int startcalendarID) {
		this.startcalendarID = startcalendarID;
	}

	public int getEndcalendarID() {
		return endcalendarID;
	}

	public void setEndcalendarID(int endcalendarID) {
		this.endcalendarID = endcalendarID;
	}

	public String getCategory() {
		return category;
	}

	public void setCategory(String category) {
		this.category = category;
	}

	public String getPromoStartDate() {
		return promoStartDate;
	}

	public void setPromoStartDate(String promoStartDate) {
		this.promoStartDate = promoStartDate;
	}

	public String getPromoEndDate() {
		return promoEndDate;
	}

	public void setPromoEndDate(String promoEndDate) {
		this.promoEndDate = promoEndDate;
	}

	public String getPromoID() {
		return promoID;
	}

	public void setPromoID(String promoID) {
		this.promoID = promoID;
	}

	public String getPromoDescription() {
		return promoDescription;
	}

	public void setPromoDescription(String promoDescription) {
		this.promoDescription = promoDescription;
	}

	public String getItemCode() {
		return itemCode;
	}

	public void setItemCode(String itemCode) {
		this.itemCode = itemCode;
	}

	public String getUpc() {
		return upc;
	}

	public void setUpc(String upc) {
		this.upc = upc;
	}

	public String getItemName() {
		return ItemName;
	}

	public void setItemName(String itemName) {
		ItemName = itemName;
	}

	public String getLirName() {
		return lirName;
	}

	public void setLirName(String lirName) {
		this.lirName = lirName;
	}

	public String getPromoGroup() {
		return promoGroup;
	}

	public void setPromoGroup(String promoGroup) {
		this.promoGroup = promoGroup;
	}

	public String getEverdayQty() {
		return everdayQty;
	}

	public void setEverdayQty(String everdayQty) {
		this.everdayQty = everdayQty;
	}

	public String getEverydayPrice() {
		return everydayPrice;
	}

	public void setEverydayPrice(String everydayPrice) {
		this.everydayPrice = everydayPrice;
	}

	public int getSaleQty() {
		return saleQty;
	}

	public void setSaleQty(int saleQty) {
		this.saleQty = saleQty;
	}

	public double getSalePrice() {
		return salePrice;
	}

	public void setSalePrice(double salePrice) {
		this.salePrice = salePrice;
	}

	public int getMustBuyQty() {
		return mustBuyQty;
	}

	public void setMustBuyQty(int mustBuyQty) {
		this.mustBuyQty = mustBuyQty;
	}

	public double getMustbuyPrice() {
		return mustbuyPrice;
	}

	public void setMustbuyPrice(double mustbuyPrice) {
		this.mustbuyPrice = mustbuyPrice;
	}

	public double getDollarOff() {
		return dollarOff;
	}

	public void setDollarOff(double dollarOff) {
		this.dollarOff = dollarOff;
	}

	public double getPctOff() {
		return pctOff;
	}

	public void setPctOff(double pctOff) {
		this.pctOff = pctOff;
	}

	public int getBuyQty() {
		return buyQty;
	}

	public void setBuyQty(int buyQty) {
		this.buyQty = buyQty;
	}

	public int getGetQty() {
		return getQty;
	}

	public void setGetQty(int getQty) {
		this.getQty = getQty;
	}

	public int getMinimumQty() {
		return minimumQty;
	}

	public void setMinimumQty(int minimumQty) {
		this.minimumQty = minimumQty;
	}

	public double getMinimumAmt() {
		return minimumAmt;
	}

	public void setMinimumAmt(double minimumAmt) {
		this.minimumAmt = minimumAmt;
	}

	public double getBmsmDollaroffperunits() {
		return bmsmDollaroffperunits;
	}

	public void setBmsmDollaroffperunits(double bmsmDollaroffperunits) {
		this.bmsmDollaroffperunits = bmsmDollaroffperunits;
	}

	public double getBmsmPctoffperunit() {
		return bmsmPctoffperunit;
	}

	public void setBmsmPctoffperunit(double bmsmPctoffperunit) {
		this.bmsmPctoffperunit = bmsmPctoffperunit;
	}

	public int getBmsmsaleQty() {
		return bmsmsaleQty;
	}

	public void setBmsmsaleQty(int bmsmsaleQty) {
		this.bmsmsaleQty = bmsmsaleQty;
	}

	public double getBmsmsalePrice() {
		return bmsmsalePrice;
	}

	public void setBmsmsalePrice(double bmsmsalePrice) {
		this.bmsmsalePrice = bmsmsalePrice;
	}

	public String getStatus() {
		return status;
	}

	public void setStatus(String status) {
		this.status = status;
	}

	public String getLocationLevel() {
		return locationLevel;
	}

	public void setLocationLevel(String locationLevel) {
		this.locationLevel = locationLevel;
	}

	public String getLocationNo() {
		return locationNo;
	}

	public void setLocationNo(String locationNo) {
		this.locationNo = locationNo;
	}

	public String getPageNumber() {
		return pageNumber;
	}

	public void setPageNumber(String pageNumber) {
		this.pageNumber = pageNumber;
	}

	public String getBlockNumber() {
		return blockNumber;
	}

	public void setBlockNumber(String blockNumber) {
		this.blockNumber = blockNumber;
	}

	public String getDisplayOffer() {
		return displayOffer;
	}

	public void setDisplayOffer(String displayOffer) {
		this.displayOffer = displayOffer;
	}

	public String getDescription() {
		return description;
	}

	public void setDescription(String description) {
		this.description = description;
	}

	public ItemDetailKey getItemDetailKey() {
		return new ItemDetailKey(PrestoUtil.castUPC(upc, false), itemCode);
	}
	
	

	public String getCouponType2() {
		return couponType2;
	}

	public void setCouponType2(String couponType2) {
		this.couponType2 = couponType2;
	}

	public double getCouponAmt2() {
		return couponAmt2;
	}

	public void setCouponAmt2(double couponAmt2) {
		this.couponAmt2 = couponAmt2;
	}

	@Override
	public Object clone() throws CloneNotSupportedException {
		PromoDataStandardDTO cloned = (PromoDataStandardDTO) super.clone();
		if (cloned.getPromoLocation() != null) {
			List<PromoLocation> clonedList = new ArrayList<PromoLocation>();
			for (PromoLocation promoLocation : cloned.getPromoLocation()) {
				clonedList.add((PromoLocation) promoLocation.clone());
			}
			cloned.setPromoLocation(clonedList);
		}

		if (cloned.getPromoBuyRequirement() != null) {
			List<PromoBuyRequirement> clonedList = new ArrayList<PromoBuyRequirement>();
			for (PromoBuyRequirement promoBuyRequirement : cloned.getPromoBuyRequirement()) {
				clonedList.add((PromoBuyRequirement) promoBuyRequirement.clone());
			}
			cloned.setPromoBuyRequirement(clonedList);
		}

		if (cloned.getBuyItems() != null) {
			List<PromoBuyItems> clonedList = new ArrayList<PromoBuyItems>();
			for (PromoBuyItems promoBuyItems : cloned.getBuyItems()) {
				clonedList.add((PromoBuyItems) promoBuyItems.clone());
			}
			cloned.setBuyItems(clonedList);
		}

		return cloned;
	}

	public String getCouponType() {
		return couponType;
	}

	public double getCouponAmt() {
		return couponAmt;
	}

	public void setCouponType(String couponType) {
		this.couponType = couponType;
	}

	public void setCouponAmt(double couponAmt) {
		this.couponAmt = couponAmt;
	}

	public String getOfferItemCode() {
		return offerItemCode;
	}

	public String getOfferItemUpc() {
		return offerItemUpc;
	}

	public void setOfferItemCode(String offerItemCode) {
		this.offerItemCode = offerItemCode;
	}

	public void setOfferItemUpc(String offerItemUpc) {
		this.offerItemUpc = offerItemUpc;
	}

	public LocalDate getPromoStartDateAsLocalDate() {
		if (this.promoStartDate != null) {
			LocalDate startDate = DateUtil.stringToLocalDate(promoStartDate, Constants.APP_LOCAL_DATE_FORMAT);
			return startDate;
		} else {
			return null;
		}
	}

	public LocalDate getPromoEndDateAsLocalDate() {
		if (this.promoEndDate != null) {
			LocalDate endDate = DateUtil.stringToLocalDate(promoEndDate, Constants.APP_LOCAL_DATE_FORMAT);
			return endDate;
		} else {
			return null;
		}
	}

	public LocalDate getPromoStartDateRAAsLocalDate() {
		if (this.promoStartDate != null) {
			LocalDate startDate = DateUtil.stringToLocalDate(promoStartDate, Constants.APP_DATE_YYYYMMDDFORMAT);
			return startDate;
		} else {
			return null;
		}
	}

	public LocalDate getPromoStart() {
		if (this.promoStartDate != null) {
			LocalDate startDate = DateUtil.stringToLocalDate(promoStartDate, Constants.APP_DATE_FORMAT);
			return startDate;
		} else {
			return null;
		}
	}

	public long getPromoDefId() {
		return promoDefId;
	}

	public void setPromoDefId(long promoDefId) {
		this.promoDefId = promoDefId;
	}

	public int getPromoTypeId() {
		return promoTypeId;
	}

	public void setPromoTypeId(int promoTypeId) {
		this.promoTypeId = promoTypeId;
	}

	public int getPromoSubtypeId() {
		return promoSubtypeId;
	}

	public void setPromoSubtypeId(int promoSubtypeId) {
		this.promoSubtypeId = promoSubtypeId;
	}

	public String getPromoName() {
		return promoName;
	}

	public void setPromoName(String promoName) {
		this.promoName = promoName;
	}

	public String getEventID() {
		return eventID;
	}

	public void setEventID(String eventID) {
		this.eventID = eventID;
	}

	public String getPromoTypeName() {
		return promoTypeName;
	}

	public void setPromoTypeName(String promoTypeName) {
		this.promoTypeName = promoTypeName;
	}

	public int getOfferCount() {
		return offerCount;
	}

	public void setOfferCount(int offerCount) {
		this.offerCount = offerCount;
	}

	public double getOfferValue() {
		return offerValue;
	}

	public void setOfferValue(double offerValue) {
		this.offerValue = offerValue;
	}

	public String getOffer_unit_type() {
		return offer_unit_type;
	}

	public void setOffer_unit_type(String offer_unit_type) {
		this.offer_unit_type = offer_unit_type;
	}

	public String getOfferType() {
		return offerType;
	}

	public void setOfferType(String offerType) {
		this.offerType = offerType;
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

	public String getOriginalStartDate() {
		return originalStartDate;
	}

	public void setOriginalStartDate(String originalStartDate) {
		this.originalStartDate = originalStartDate;
	}

	public String getOriginalEndtDate() {
		return originalEndtDate;
	}

	public void setOriginalEndtDate(String originalEndtDate) {
		this.originalEndtDate = originalEndtDate;
	}

	public int getOfferItem() {
		return offerItem;
	}

	public void setOfferItem(int offerItem) {
		this.offerItem = offerItem;
	}
	
	

}
