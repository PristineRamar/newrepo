/*
 * Author : Naimish Start Date : Jul 22, 2009
 * 
 * Change Description Changed By Date
 * --------------------------------------------------------------
 */

package com.pristine.dto;

import java.sql.Date;

public class CompetitiveDataDTO implements Cloneable {

	/*
	 * 9/28/09 - Suresh This class has same variable used two times one for Suspect
	 * (added by Naimish) and another for RDSDataload (Added by Suresh) This has to
	 * be eventually cleanup
	 */

	/* Variables for RDS Data Load - Begin */
	public String comment;
	public int categoryId;
	public String retailerItemCode;
	public String upc;
	public int itemcode;
	public String itemName;
	public String compStrNo;
	public int compStrId;
	public int multiple;
	public float retailPrice;
	public float regPrice;
	public int regMPack;
	public float regMPrice;
	public String retailType;
	public float fSalePrice;
	public String EffectiveRegPriceDate;
	public float LoyaltyQuantity;
	public float LoyaltyPrice;
	public String AdditionalInfo;
	public String OutsideIndicator;
	public String Size;
	
	//Added for autozone
	public String saleDate;
	public String PartNumber;
	public String compChainId;
	public String compChainName;
	public String shortName;
    public String addressLine1;
    public String city;
    public String state;
    public String zip;
	
	
	
	public String getShortName() {
		return shortName;
	}

	public void setShortName(String string) {
		this.shortName = string;
	}

	public String getCompChainName() {
		return compChainName;
	}

	public void setCompChainName(String compChainName) {
		this.compChainName = compChainName;
	}

	public String getCompChainId() {
		return compChainId;
	}

	public void setCompChainId(String compChainId) {
		this.compChainId = compChainId;
	}

	public String getSaleDate() {
		return saleDate;
	}

	public void setSaleDate(String saleDate) {
		this.saleDate = saleDate;
	}

	public String getPartNumber() {
		return PartNumber;
	}

	public void setPartNumber(String partNumber) {
		PartNumber = partNumber;
	}

	public float getLoyaltyPrice() {
		return LoyaltyPrice;
	}

	public void setLoyaltyPrice(float loyaltyPrice) {
		LoyaltyPrice = loyaltyPrice;
	}

	public int shopriteCase;

	public int getShopriteCase() {
		return shopriteCase;
	}

	public void setShopriteCase(int shopriteCase) {
		this.shopriteCase = shopriteCase;
	}

	public float getLoyaltyQuantity() {
		return LoyaltyQuantity;
	}

	public void setLoyaltyQuantity(float loyaltyQuantity) {
		LoyaltyQuantity = loyaltyQuantity;
	}

	public String getComment() {
		return comment;
	}

	public void setComment(String comment) {
		this.comment = comment;
	}

	public int getCategoryId() {
		return categoryId;
	}

	public void setCategoryId(int categoryId) {
		this.categoryId = categoryId;
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

	public String getCompStrNo() {
		return compStrNo;
	}

	public void setCompStrNo(String compStrNo) {
		this.compStrNo = compStrNo;
	}

	public int getCompStrId() {
		return compStrId;
	}

	public void setCompStrId(int compStrId) {
		this.compStrId = compStrId;
	}

	public int getMultiple() {
		return multiple;
	}

	public void setMultiple(int multiple) {
		this.multiple = multiple;
	}

	public float getRetailPrice() {
		return retailPrice;
	}

	public void setRetailPrice(float retailPrice) {
		this.retailPrice = retailPrice;
	}

	public int getRegMPack() {
		return regMPack;
	}

	public void setRegMPack(int regMPack) {
		this.regMPack = regMPack;
	}

	public float getRegMPrice() {
		return regMPrice;
	}

	public void setRegMPrice(float regMPrice) {
		this.regMPrice = regMPrice;
	}

	public String getRetailType() {
		return retailType;
	}

	public void setRetailType(String retailType) {
		this.retailType = retailType;
	}

	public String getEffectiveRegPriceDate() {
		return EffectiveRegPriceDate;
	}

	public void setEffectiveRegPriceDate(String effectiveRegPriceDate) {
		EffectiveRegPriceDate = effectiveRegPriceDate;
	}

	public int getSaleMPack() {
		return saleMPack;
	}

	public void setSaleMPack(int saleMPack) {
		this.saleMPack = saleMPack;
	}

	public float getfSaleMPrice() {
		return fSaleMPrice;
	}

	public void setfSaleMPrice(float fSaleMPrice) {
		this.fSaleMPrice = fSaleMPrice;
	}

	public int getScheduleId() {
		return scheduleId;
	}

	public void setScheduleId(int scheduleId) {
		this.scheduleId = scheduleId;
	}

	public String getItemNotFound() {
		return itemNotFound;
	}

	public void setItemNotFound(String itemNotFound) {
		this.itemNotFound = itemNotFound;
	}

	public String getPriceNotFound() {
		return priceNotFound;
	}

	public void setPriceNotFound(String priceNotFound) {
		this.priceNotFound = priceNotFound;
	}

	public String getSaleInd() {
		return saleInd;
	}

	public void setSaleInd(String saleInd) {
		this.saleInd = saleInd;
	}

	public String getCheckDate() {
		return checkDate;
	}

	public void setCheckDate(String checkDate) {
		this.checkDate = checkDate;
	}

	public String getWeekStartDate() {
		return weekStartDate;
	}

	public void setWeekStartDate(String weekStartDate) {
		this.weekStartDate = weekStartDate;
	}

	public String getWeekEndDate() {
		return weekEndDate;
	}

	public void setWeekEndDate(String weekEndDate) {
		this.weekEndDate = weekEndDate;
	}

	public String getNewUOM() {
		return newUOM;
	}

	public void setNewUOM(String newUOM) {
		this.newUOM = newUOM;
	}

	public int getChgDirection() {
		return chgDirection;
	}

	public void setChgDirection(int chgDirection) {
		this.chgDirection = chgDirection;
	}

	public int getSaleChgDirection() {
		return saleChgDirection;
	}

	public void setSaleChgDirection(int saleChgDirection) {
		this.saleChgDirection = saleChgDirection;
	}

	public int getRankingScore() {
		return rankingScore;
	}

	public void setRankingScore(int rankingScore) {
		this.rankingScore = rankingScore;
	}

	public int getRepresentedStoreId() {
		return representedStoreId;
	}

	public void setRepresentedStoreId(int representedStoreId) {
		this.representedStoreId = representedStoreId;
	}

	public int getRelatedStoreId() {
		return relatedStoreId;
	}

	public void setRelatedStoreId(int relatedStoreId) {
		this.relatedStoreId = relatedStoreId;
	}

	public int getRelatedItemCode() {
		return relatedItemCode;
	}

	public void setRelatedItemCode(int relatedItemCode) {
		this.relatedItemCode = relatedItemCode;
	}

	public String getSize() {
		return size;
	}

	public void setSize(String size) {
		this.size = size;
	}

	public String getUomId() {
		return uomId;
	}

	public void setUomId(String uomId) {
		this.uomId = uomId;
	}

	public String getUom() {
		return uom;
	}

	public void setUom(String uom) {
		this.uom = uom;
	}

	public String getOutSideRangeInd() {
		return outSideRangeInd;
	}

	public void setOutSideRangeInd(String outSideRangeInd) {
		this.outSideRangeInd = outSideRangeInd;
	}

	public String getItemPack() {
		return itemPack;
	}

	public void setItemPack(String itemPack) {
		this.itemPack = itemPack;
	}

	public float getActualPrice() {
		return actualPrice;
	}

	public void setActualPrice(float actualPrice) {
		this.actualPrice = actualPrice;
	}

	public int getSuggestedStrId() {
		return suggestedStrId;
	}

	public void setSuggestedStrId(int suggestedStrId) {
		this.suggestedStrId = suggestedStrId;
	}

	public String getActualCheckItemNotFound() {
		return actualCheckItemNotFound;
	}

	public void setActualCheckItemNotFound(String actualCheckItemNotFound) {
		this.actualCheckItemNotFound = actualCheckItemNotFound;
	}

	public int getCheckItemId() {
		return checkItemId;
	}

	public void setCheckItemId(int checkItemId) {
		this.checkItemId = checkItemId;
	}

	public String getEffSaleEndDate() {
		return effSaleEndDate;
	}

	public void setEffSaleEndDate(String effSaleEndDate) {
		this.effSaleEndDate = effSaleEndDate;
	}

	public String getEffSaleStartDate() {
		return effSaleStartDate;
	}

	public void setEffSaleStartDate(String effSaleStartDate) {
		this.effSaleStartDate = effSaleStartDate;
	}

	public String getEffRegRetailStartDate() {
		return effRegRetailStartDate;
	}

	public void setEffRegRetailStartDate(String effRegRetailStartDate) {
		this.effRegRetailStartDate = effRegRetailStartDate;
	}

	public int getLirId() {
		return lirId;
	}

	public void setLirId(int lirId) {
		this.lirId = lirId;
	}

	public String getPiAnalyzeFlag() {
		return piAnalyzeFlag;
	}

	public void setPiAnalyzeFlag(String piAnalyzeFlag) {
		this.piAnalyzeFlag = piAnalyzeFlag;
	}

	public String getDiscontFlag() {
		return discontFlag;
	}

	public void setDiscontFlag(String discontFlag) {
		this.discontFlag = discontFlag;
	}

	public int getDeptId() {
		return deptId;
	}

	public void setDeptId(int deptId) {
		this.deptId = deptId;
	}

	public String getDeptName() {
		return deptName;
	}

	public void setDeptName(String deptName) {
		this.deptName = deptName;
	}

	public String getCategoryName() {
		return categoryName;
	}

	public void setCategoryName(String categoryName) {
		this.categoryName = categoryName;
	}

	public float getQuantitySold() {
		return quantitySold;
	}

	public void setQuantitySold(float quantitySold) {
		this.quantitySold = quantitySold;
	}

	public String getPromoNumber() {
		return promoNumber;
	}

	public void setPromoNumber(String promoNumber) {
		this.promoNumber = promoNumber;
	}

	public String getOnAd() {
		return onAd;
	}

	public void setOnAd(String onAd) {
		this.onAd = onAd;
	}

	public String getHasLIGRegPriceVariations() {
		return hasLIGRegPriceVariations;
	}

	public void setHasLIGRegPriceVariations(String hasLIGRegPriceVariations) {
		this.hasLIGRegPriceVariations = hasLIGRegPriceVariations;
	}

	public int getBrandId() {
		return brandId;
	}

	public void setBrandId(int brandId) {
		this.brandId = brandId;
	}

	public boolean isZonePriceDiff() {
		return isZonePriceDiff;
	}

	public void setZonePriceDiff(boolean isZonePriceDiff) {
		this.isZonePriceDiff = isZonePriceDiff;
	}

	public String getPlFlag() {
		return plFlag;
	}

	public void setPlFlag(String plFlag) {
		this.plFlag = plFlag;
	}

	public String getAisle() {
		return aisle;
	}

	public void setAisle(String aisle) {
		this.aisle = aisle;
	}

	public String getStoreName() {
		return storeName;
	}

	public void setStoreName(String storeName) {
		this.storeName = storeName;
	}

	public String getStoreAddr() {
		return storeAddr;
	}

	public void setStoreAddr(String storeAddr) {
		this.storeAddr = storeAddr;
	}

	public String getStoreNo() {
		return storeNo;
	}

	public void setStoreNo(String storeNo) {
		this.storeNo = storeNo;
	}

	public String getPriceChangeInd() {
		return priceChangeInd;
	}

	public void setPriceChangeInd(String priceChangeInd) {
		this.priceChangeInd = priceChangeInd;
	}

	public String getSubCategory() {
		return subCategory;
	}

	public void setSubCategory(String subCategory) {
		this.subCategory = subCategory;
	}

	public int getItemSizeScore() {
		return itemSizeScore;
	}

	public void setItemSizeScore(int itemSizeScore) {
		this.itemSizeScore = itemSizeScore;
	}

	public int getItemPriceScore() {
		return itemPriceScore;
	}

	public void setItemPriceScore(int itemPriceScore) {
		this.itemPriceScore = itemPriceScore;
	}

	public int getItemNameScore() {
		return itemNameScore;
	}

	public void setItemNameScore(int itemNameScore) {
		this.itemNameScore = itemNameScore;
	}

	public String getCompSKU() {
		return compSKU;
	}

	public void setCompSKU(String compSKU) {
		this.compSKU = compSKU;
	}

	public String getPodWM() {
		return podWM;
	}

	public void setPodWM(String podWM) {
		this.podWM = podWM;
	}

	public String getCodWM() {
		return codWM;
	}

	public void setCodWM(String codWM) {
		this.codWM = codWM;
	}

	public String getColWM() {
		return colWM;
	}

	public void setColWM(String colWM) {
		this.colWM = colWM;
	}

	public String getColKR() {
		return colKR;
	}

	public void setColKR(String colKR) {
		this.colKR = colKR;
	}

	public String getColMJ() {
		return colMJ;
	}

	public void setColMJ(String colMJ) {
		this.colMJ = colMJ;
	}

	public String getSubCategoryName() {
		return subCategoryName;
	}

	public void setSubCategoryName(String subCategoryName) {
		this.subCategoryName = subCategoryName;
	}

	public String getRegPricePPU() {
		return regPricePPU;
	}

	public void setRegPricePPU(String regPricePPU) {
		this.regPricePPU = regPricePPU;
	}

	public String getSalePricePPU() {
		return salePricePPU;
	}

	public void setSalePricePPU(String salePricePPU) {
		this.salePricePPU = salePricePPU;
	}

	public int getRegMultiple() {
		return regMultiple;
	}

	public void setRegMultiple(int regMultiple) {
		this.regMultiple = regMultiple;
	}

	public int getSaleMultiple() {
		return saleMultiple;
	}

	public void setSaleMultiple(int saleMultiple) {
		this.saleMultiple = saleMultiple;
	}

	public String getItemCodeNo() {
		return itemCodeNo;
	}

	public void setItemCodeNo(String itemCodeNo) {
		this.itemCodeNo = itemCodeNo;
	}

	public String getUpcwithcheck() {
		return upcwithcheck;
	}

	public void setUpcwithcheck(String upcwithcheck) {
		this.upcwithcheck = upcwithcheck;
	}

	public String getUpcwithoutcheck() {
		return upcwithoutcheck;
	}

	public void setUpcwithoutcheck(String upcwithoutcheck) {
		this.upcwithoutcheck = upcwithoutcheck;
	}

	public Character getItemNotFoundFlag() {
		return itemNotFoundFlag;
	}

	public void setItemNotFoundFlag(Character itemNotFoundFlag) {
		this.itemNotFoundFlag = itemNotFoundFlag;
	}

	public Character getPriceNotFoundFlag() {
		return priceNotFoundFlag;
	}

	public void setPriceNotFoundFlag(Character priceNotFoundFlag) {
		this.priceNotFoundFlag = priceNotFoundFlag;
	}

	public Character getItemNotFoundXTimesFlag() {
		return itemNotFoundXTimesFlag;
	}

	public void setItemNotFoundXTimesFlag(Character itemNotFoundXTimesFlag) {
		this.itemNotFoundXTimesFlag = itemNotFoundXTimesFlag;
	}

	public int getItemcode() {
		return itemcode;
	}

	public void setItemcode(int itemcode) {
		this.itemcode = itemcode;
	}

	public float getRegPrice() {
		return regPrice;
	}

	public void setRegPrice(float regPrice) {
		this.regPrice = regPrice;
	}

	public float getfSalePrice() {
		return fSalePrice;
	}

	public void setfSalePrice(float fSalePrice) {
		this.fSalePrice = fSalePrice;
	}

	public int saleMPack;
	public float fSaleMPrice;
	public int scheduleId;
	public String itemNotFound;
	public String priceNotFound;
	public String saleInd;
	public String checkDate;
	public String weekStartDate;
	public String weekEndDate;
	public String newUOM;
	public int chgDirection;
	public int saleChgDirection;
	public int rankingScore;
	public int representedStoreId;
	public int relatedStoreId;
	public int relatedItemCode;
	public String size;
	public String uomId;
	public String uom;
	public String outSideRangeInd;
	public String itemPack;
	public float actualPrice;
	public int suggestedStrId;
	public String actualCheckItemNotFound;
	public int checkItemId;
	public String effSaleEndDate;
	public String effSaleStartDate;
	public String effRegRetailStartDate;
	public int lirId;
	public String piAnalyzeFlag;
	public String discontFlag;
	public int deptId = -1;
	public String deptName;
	public String categoryName;
	public float quantitySold;

	public String promoNumber;
	public String onAd;
	public String hasLIGRegPriceVariations = "N"; // Notification for Price Variation in LIG
	public int brandId;
	public boolean isZonePriceDiff;
	public String plFlag;

	// Added related to Raw Comp Data by Dinesh(02/02/17)
	public String aisle;
	public String storeName;
	public String storeAddr;
	public String storeNo;
	public String priceChangeInd;
	public String subCategory;
	public int itemSizeScore;
	public int itemPriceScore;
	public int itemNameScore;
	public int totalScore;
	public String itemNameWOCompName;
	public int compItemNameScore;
	public String compItemAddlDesc;

	public String compSKU;
	public String podWM;
	public String codWM;
	public String colWM;
	public String colKR;
	public String colMJ;
	public String subCategoryName;
	public String regPricePPU;
	public String salePricePPU;

	public int regMultiple;
	public int saleMultiple;

	public String itemCodeNo;

	public String upcwithcheck;
	public String upcwithoutcheck;

	public void clear() {
		retailerItemCode = "";
		upc = "";
		itemcode = 0;
		compStrNo = "";
		compStrId = 0;
		multiple = 0;
		retailPrice = 0;
		regPrice = 0;
		regMPack = 0;
		regMPrice = 0;
		retailType = "";
		fSalePrice = 0;
		saleMPack = 0;
		fSaleMPrice = 0;
		scheduleId = 0;
		itemNotFound = "";
		priceNotFound = "";
		saleInd = "";
		checkDate = "";
		weekStartDate = "";
		weekEndDate = "";
		newUOM = "";
		chgDirection = 0;
		itemName = "";
		size = "";
		uom = "";
		uomId = "";
		itemPack = "";
		checkItemId = 0;
		representedStoreId = -1;
		relatedStoreId = -1;
		relatedItemCode = -1;
		outSideRangeInd = "";
		effSaleEndDate = "";
		effSaleStartDate = "";
		effRegRetailStartDate = "";
		lirId = -1;
		piAnalyzeFlag = "";
		discontFlag = "";
		deptId = -1;
		deptName = "";
		quantitySold = 0f;
		promoNumber = "";
		onAd = "";
		hasLIGRegPriceVariations = "N"; // Notification for Price Variation in LIG
		regMultiple = 0;
		saleMultiple = 0;
	}

	/* Variables for RDS Data Load - Begin */

	/* Variables for Suspect - Begin */
	private Integer checkDataID;
	private Integer compStrID;
	private Integer scheduleID;
	private Integer itemCode;
	private Float unitRegularPrice;
	private Float unitSalePrice;
	private Float regularPrice;
	private Float salePrice;
	private Float regularMPrice;
	private Float saleMPrice;
	private Integer regularPack;
	private Integer salePack;
	private Character itemNotFoundFlag;
	private Character priceNotFoundFlag;
	private Character itemNotFoundXTimesFlag;
	private Date date;
	private Integer changeDirection;
	private Integer changeDirectionSale;
	private Date startDate;
	private Date endDate;
	private Date statusChangeDate;
	private String districtId;
	private String regionId;
	private String divisionId;
	private String chainId;

	public Character getitemNotFoundXTimesFlag() {
		return itemNotFoundXTimesFlag;
	}

	public void setitemNotFoundXTimesFlag(Character itemNotFoundXFlag) {
		this.itemNotFoundXTimesFlag = itemNotFoundXFlag;
	}

	public void setStatusChangeDate(Date statusDate) {
		this.statusChangeDate = statusDate;
	}

	public Date getStatusChangeDate() {
		return statusChangeDate;
	}

	public Integer getChangeDirectionSale() {
		return changeDirectionSale;
	}

	public void setChangeDirectionSale(Integer changeDirectionSale) {
		this.changeDirectionSale = changeDirectionSale;
	}

	public Date getStartDate() {
		return startDate;
	}

	public void setStartDate(Date startDate) {
		this.startDate = startDate;
	}

	public Date getEndDate() {
		return endDate;
	}

	public void setEndDate(Date endDate) {
		this.endDate = endDate;
	}

	public Float getRegularPrice() {
		return regularPrice;
	}

	public void setRegularPrice(Float regularPrice) {
		this.regularPrice = regularPrice;
	}

	public Float getSalePrice() {
		return salePrice;
	}

	public void setSalePrice(Float salePrice) {
		this.salePrice = salePrice;
	}

	public Float getRegularMPrice() {
		return regularMPrice;
	}

	public void setRegularMPrice(Float regularMPrice) {
		this.regularMPrice = regularMPrice;
	}

	public Float getSaleMPrice() {
		return saleMPrice;
	}

	public void setSaleMPrice(Float saleMPrice) {
		this.saleMPrice = saleMPrice;
	}

	public Integer getRegularPack() {
		return regularPack;
	}

	public void setRegularPack(Integer regularPack) {
		this.regularPack = regularPack;
	}

	public Integer getSalePack() {
		return salePack;
	}

	public void setSalePack(Integer salePack) {
		this.salePack = salePack;
	}

	public Integer getCheckDataID() {
		return checkDataID;
	}

	public void setCheckDataID(Integer checkDataID) {
		this.checkDataID = checkDataID;
	}

	public Integer getScheduleID() {
		return scheduleID;
	}

	public void setScheduleID(Integer scheduleID) {
		this.scheduleID = scheduleID;
	}

	public Integer getItemCode() {
		return itemCode;
	}

	public void setItemCode(Integer itemCode) {
		this.itemCode = itemCode;
	}

	public Float getUnitRegularPrice() {
		return unitRegularPrice;
	}

	public void setUnitRegularPrice(Float regPrice) {
		this.unitRegularPrice = regPrice;
	}

	public Float getUnitSalePrice() {
		return unitSalePrice;
	}

	public void setUnitSalePrice(Float salePrice) {
		this.unitSalePrice = salePrice;
	}

	public Character getItemNotFoundFlg() {
		return itemNotFoundFlag;
	}

	public void setItemNotFoundFlg(Character itemNotFoundFlag) {
		this.itemNotFoundFlag = itemNotFoundFlag;
	}

	public Character getPriceNotFoundFlg() {
		return priceNotFoundFlag;
	}

	public void setPriceNotFoundFlg(Character priceNotFoundFlg) {
		this.priceNotFoundFlag = priceNotFoundFlg;
	}

	public Integer getCompStrID() {
		return compStrID;
	}

	public void setCompStrID(Integer compStrID) {
		this.compStrID = compStrID;
	}

	public Date getDate() {
		return date;
	}

	public void setDate(Date date) {
		this.date = date;
	}

	public Integer getChangeDirection() {
		return changeDirection;
	}

	public void setChangeDirection(Integer changeDirection) {
		this.changeDirection = changeDirection;
	}
	public String getDistrictId() {
		return districtId;
	}
	public void setDistrictId(String districtId) {
		this.districtId = districtId;
	}
	public String getRegionId() {
		return regionId;
	}
	public void setRegionId(String regionId) {
		this.regionId = regionId;
	}
	public String getDivisionId() {
		return divisionId;
	}
	public void setDivisionId(String divisionId) {
		this.divisionId = divisionId;
	}
	public String getChainId() {
		return chainId;
	}
	public void setChainId(String chainId) {
		this.chainId = chainId;
	}

	public void copy(CompetitiveDataDTO compData) {
		this.compStrId = compData.compStrId;
		this.compStrNo = compData.compStrNo;
		this.scheduleId = compData.scheduleId;
		this.newUOM = compData.newUOM;
		this.uom = compData.uom;
		this.uomId = compData.uomId;
		this.upc = compData.upc;
		this.itemName = compData.itemName;
		this.regMPack = compData.regMPack;
		this.regPrice = compData.regPrice;
		this.regMPrice = compData.regMPrice;
		this.saleMPack = compData.saleMPack;
		this.fSalePrice = compData.fSalePrice;
		this.fSaleMPrice = compData.fSaleMPrice;
		this.comment = compData.comment;
		this.outSideRangeInd = compData.outSideRangeInd;
		this.saleInd = compData.saleInd;
		this.priceNotFound = compData.priceNotFound;
		this.itemNotFound = compData.itemNotFound;
		this.checkDate = compData.checkDate;
		this.weekStartDate = compData.weekStartDate;
		this.weekEndDate = compData.weekEndDate;
		this.effRegRetailStartDate = compData.effRegRetailStartDate;
		this.effSaleStartDate = compData.effSaleStartDate;
		this.effSaleEndDate = compData.effSaleEndDate;
		this.compSKU = compData.compSKU;
		this.deptName = compData.deptName;
		this.categoryName = compData.categoryName;
		this.subCategoryName = compData.subCategoryName;
		this.podWM = compData.podWM;
		this.codWM = compData.codWM;
		this.colWM = compData.colWM;
		this.colKR = compData.colKR;
		this.colMJ = compData.colMJ;
		this.retailerItemCode = compData.retailerItemCode;
	}

	@Override
	public Object clone() throws CloneNotSupportedException {
		return super.clone();
	}

	public String getItemNameWOCompName() {
		return itemNameWOCompName;
	}

	public void setItemNameWOCompName(String itemNameWOCompName) {
		this.itemNameWOCompName = itemNameWOCompName;
	}

	public int getCompItemNameScore() {
		return compItemNameScore;
	}

	public void setCompItemNameScore(int compItemNameScore) {
		this.compItemNameScore = compItemNameScore;
	}

	public String getCompItemAddlDesc() {
		return compItemAddlDesc;
	}

	public void setCompItemAddlDesc(String compItemAddlDesc) {
		this.compItemAddlDesc = compItemAddlDesc;
	}

	public int getTotalScore() {
		return totalScore;
	}

	public void setTotalScore(int totalScore) {
		this.totalScore = totalScore;
	}

	public String getOutsideIndicator() {
		return OutsideIndicator;
	}

	public void setOutsideIndicator(String outsideIndicator) {
		OutsideIndicator = outsideIndicator;
	}

	public String getAddressLine1() {
		return addressLine1;
	}

	public void setAddressLine1(String addressLine1) {
		this.addressLine1 = addressLine1;
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
	
	@Override
	public String toString() {
		return "CompetitiveDataDTO [upc=" + upc + ", itemcode=" + itemcode + ", compStrNo=" + compStrNo + ", compStrId="
				+ compStrId + ", regPrice=" + regPrice + ", fSalePrice=" + fSalePrice + ", salePrice=" + salePrice
				+ "]";
	}

}