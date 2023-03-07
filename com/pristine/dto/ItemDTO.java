package com.pristine.dto;

import com.pristine.util.PrestoUtil;

public class ItemDTO implements Cloneable{
	public int itemCode;
	public String majDeptName;
	public String majDeptCode;
	public String itemName;
	public String deptName;
	public int deptID;
	public int catID;
	public String segmentCode;
	public int segmentID;
	public String segmentName;
	public String subCatName;
	public int subCatID;
	public String catName;
	public String retailerItemCode;
	public String altRetailerItemCode;
	
	public String lobName;
	public String lobCode;

	public String upc;
	public String privateLabelCode;
	public String uom;
	public String uomId;
	public String size;
	public String likeItemGrp;
	public String likeItemCode;

	public String pack;
	public String rdsItemName;
	public String manufactCode;
	public String manufactName;
	public int manufactId;
	public String itemRank;
	public String standardUPC;
	public int likeItemId;
	public String deptCode;
	public String deptShortName;
	public String catCode;
	public String subCatCode;
	
	public String categoryMgrName;
	
	public int brandId; 
	public String brandName;
	public String brandCode;
	public String privateLabelFlag;
	
	public boolean updateTimeStamp = true;
	
	

	public String operationMode;
	public String sectorName;
	public String sectorCode;
	public String merchDept;
	public String merchDeptCode;
	public String portfolio;
	public String portfolioCode;
	public String financeDept;
	public String financeDeptCode;
	
	public int levelType; // Loading Product Group Tables - Price Index Portfolio Support
	public String empty; 
	public String posDept;
	public String posDeptDesc;
	
	public String internalItemCode; // Internal Item Code for TOPS
	
	public String prePriceInd;

	public boolean isEmptyDepartment = false;
	public boolean isEmptyCategory = false;
	public boolean isEmptySubCategory = false;
	public String kviCode;
	public int priceCheckTypeId;
	public int ligRepItemCode;
	public boolean lirInd = false;
	
	private String prcGrpCd;
	private String sysCd;
	private String splrTypCd;
	
	private String orgItemCode;
	private String actItemCode;
	
	private String retailerName;
	
	private double prePrice;
	
	private String shelfSize;//Added to check and Convert oz to Lb if the size is wrong
	
	private boolean isActive = true;
	private boolean isLirInd = true;
	private boolean isPrestoAssignedLirInd = false;
	
	private String itemType;
	
	public double getPrePrice() {
		return prePrice;
	}

	public void setPrePrice(double prePrice) {
		this.prePrice = prePrice;
	}
	
	public String getPrePriceInd() {
		return prePriceInd;
	}

	public void setPrePriceInd(String prePriceInd) {
		this.prePriceInd = prePriceInd;
	}

	public String getOperationMode() {
		return operationMode;
	}

	public void setOperationMode(String operationMode) {
		this.operationMode = operationMode;
	}

	public String getSectorName() {
		return sectorName;
	}

	public void setSectorName(String sectorName) {
		this.sectorName = sectorName;
	}

	public String getSectorCode() {
		return sectorCode;
	}

	public void setSectorCode(String sectorCode) {
		this.sectorCode = sectorCode;
	}

	public String getMerchDept() {
		return merchDept;
	}

	public void setMerchDept(String merchDept) {
		this.merchDept = merchDept;
	}

	public String getMerchDeptCode() {
		return merchDeptCode;
	}

	public void setMerchDeptCode(String merchDeptCode) {
		this.merchDeptCode = merchDeptCode;
	}

	public String getPortfolio() {
		return portfolio;
	}

	public void setPortfolio(String portfolio) {
		this.portfolio = portfolio;
	}

	public String getPortfolioCode() {
		return portfolioCode;
	}

	public void setPortfolioCode(String portfolioCode) {
		this.portfolioCode = portfolioCode;
	}

	public String getFinanceDept() {
		return financeDept;
	}

	public void setFinanceDept(String financeDept) {
		this.financeDept = financeDept;
	}

	public String getFinanceDeptCode() {
		return financeDeptCode;
	}

	public void setFinanceDeptCode(String financeDeptCode) {
		this.financeDeptCode = financeDeptCode;
	}

	
	
	
	
	public String getMajDeptName() {
		return majDeptName;
	}

	public void setMajDeptName(String majDeptName) {
		this.majDeptName = majDeptName;
	}

	public String getMajDeptCode() {
		return majDeptCode;
	}

	public void setMajDeptCode(String majDeptCode) {
		this.majDeptCode = majDeptCode;
	}

	public int getSegmentID() {
		return segmentID;
	}

	public void setSegmentID(int segmentID) {
		this.segmentID = segmentID;
	}

	public String getPrivateLabelCode() {
		return privateLabelCode;
	}

	public void setPrivateLabelCode(String privateLabelCode) {
		this.privateLabelCode = privateLabelCode;
	}

	public String getLikeItemCode() {
		return likeItemCode;
	}

	public void setLikeItemCode(String likeItemCode) {
		this.likeItemCode = likeItemCode;
	}

	public String getDeptShortName() {
		return deptShortName;
	}

	public void setDeptShortName(String deptShortName) {
		this.deptShortName = deptShortName;
	}

	public String getPrivateLabelFlag() {
		return privateLabelFlag;
	}

	public void setPrivateLabelFlag(String privateLabelFlag) {
		this.privateLabelFlag = privateLabelFlag;
	}

	
	
	public int getBrandId() {
		return brandId;
	}

	public void setBrandId(int brandId) {
		this.brandId = brandId;
	}

	public String getBrandName() {
		return brandName;
	}

	public void setBrandName(String brandName) {
		this.brandName = brandName;
	}

	public String getBrandCode() {
		return brandCode;
	}

	public void setBrandCode(String brandCode) {
		this.brandCode = brandCode;
	}

	public String getSegmentCode() {
		return segmentCode;
	}

	public void setSegmentCode(String segmentCode) {
		this.segmentCode = segmentCode;
	}

	public String getSegmentName() {
		return segmentName;
	}

	public void setSegmentName(String segmentName) {
		this.segmentName = segmentName;
	}

	public String getCategoryMgrName() {
		return categoryMgrName;
	}

	public void setCategoryMgrName(String categoryMgrName) {
		this.categoryMgrName = categoryMgrName;
	}

	public String getDeptCode() {
		return deptCode;
	}

	public void setDeptCode(String deptCode) {
		this.deptCode = deptCode;
	}

	public String getCatCode() {
		return catCode;
	}

	public void setCatCode(String catCode) {
		this.catCode = catCode;
	}

	public String getSubCatCode() {
		return subCatCode;
	}

	public void setSubCatCode(String subCatCode) {
		this.subCatCode = subCatCode;
	}

	public int getItemCode() {
		return itemCode;
	}

	public void setItemCode(int itemCode) {
		this.itemCode = itemCode;
	}

	public String getItemName() {
		return itemName;
	}

	public void setItemName(String itemName) {
		this.itemName = itemName;
	}

	public String getDeptName() {
		return deptName;
	}

	public void setDeptName(String deptName) {
		this.deptName = deptName;
	}

	public int getDeptID() {
		return deptID;
	}

	public void setDeptID(int deptID) {
		this.deptID = deptID;
	}

	public int getCatID() {
		return catID;
	}

	public void setCatID(int catID) {
		this.catID = catID;
	}

	public String getSubCatName() {
		return subCatName;
	}

	public void setSubCatName(String subCatName) {
		this.subCatName = subCatName;
	}
	
	//Added by RB
	public String getLobName() {
		return lobName;
	}

	public void setLobName(String lobName) {
		this.lobName = lobName;
	}
	
	//Added by RB
	public String getLobCode() {
		return lobCode;
	}

	public void setLobID(String lobCode) {
		this.lobCode = lobCode;
	}

	public int getSubCatID() {
		return subCatID;
	}

	public void setSubCatID(int subCatID) {
		this.subCatID = subCatID;
	}

	public String getCatName() {
		return catName;
	}

	public void setCatName(String catName) {
		this.catName = catName;
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

	public String getPrivateLabelInd() {
		return privateLabelCode;
	}

	public void setPrivateLabelInd(String privateLabelInd) {
		this.privateLabelCode = privateLabelInd;
	}

	public String getUom() {
		return uom;
	}

	public void setUom(String uom) {
		this.uom = uom;
	}

	public String getUomId() {
		return uomId;
	}

	public void setUomId(String uomId) {
		this.uomId = uomId;
	}

	public String getSize() {
		return size;
	}

	public void setSize(String size) {
		this.size = size;
	}

	public String getLikeItemGrp() {
		return likeItemGrp;
	}

	public void setLikeItemGrp(String likeItemGrp) {
		this.likeItemGrp = likeItemGrp;
	}

	public String getPack() {
		return pack;
	}

	public void setPack(String pack) {
		this.pack = pack;
	}

	public String getManufactCode() {
		return manufactCode;
	}

	public void setManufactCode(String manufactCode) {
		this.manufactCode = manufactCode;
	}

	public String getItemRank() {
		return itemRank;
	}

	public void setItemRank(String itemRank) {
		this.itemRank = itemRank;
	}

	public String getStandardUPC() {
		return standardUPC;
	}

	public void setStandardUPC(String standardUPC) {
		this.standardUPC = standardUPC;
	}

	public int getLikeItemId() {
		return likeItemId;
	}

	public void setLikeItemId(int likeItemId) {
		this.likeItemId = likeItemId;
	}

	
	public void clear(){
		itemCode=0;
		majDeptCode = "";
		majDeptName = "";
		itemName="";
		deptName="";
		deptID=-1;
		catID=-1;
		subCatID=-1;
		segmentID=-1;
		catName="";
		retailerItemCode="";
		upc="";
		privateLabelCode="";
		uom="";
		uomId="";
		size ="";
		likeItemGrp="";
		likeItemCode="";
		segmentCode="";
		segmentName="";
		subCatName="";
		pack="";
		rdsItemName="";
		manufactCode="";
		manufactName="";
		manufactId=-1;
		itemRank="";
		standardUPC="";
		likeItemId = -1;
	}
	
	public String toString(){
		StringBuffer sb = new StringBuffer();
		sb.append("Item - ").append(itemName);
		sb.append(", Retailer Code - ").append(retailerItemCode);
		sb.append(", UPC - ").append(upc);
		sb.append(", Size/UOM - ").append(size).append("/").append(uom);
		sb.append(", privateLabelInd - ").append(privateLabelCode);
		sb.append(", rank - ").append(itemRank);
		sb.append(", likeItemGrp - ").append(likeItemGrp);
		sb.append(", deptName/deptCode - ").append(deptName).append("/").append(deptCode);
		sb.append(", catName/deptCode - ").append(catName).append("/").append(catCode);
		sb.append(", subCatName/deptCode - ").append(subCatName).append("/").append(subCatCode);
		return sb.toString();
	}

	// Loading Product Group Tables - Price Index Portfolio Support
	public int getLevelType() {
		return levelType;
	}

	public void setLevelType(int levelType) {
		this.levelType = levelType;
	}
	// Loading Product Group Tables - Price Index Portfolio Support - Ends
	
	public String getPrcGrpCd() {
		return prcGrpCd;
	}

	public void setPrcGrpCd(String prcGrpCd) {
		this.prcGrpCd = prcGrpCd;
	}
	
	public String getOrgItemCode() {
		return orgItemCode;
	}
	
	public void setOrgItemCode(String orgItemCode) {
		this.orgItemCode = orgItemCode;
	}
	
	public String getActItemCode() {
		return actItemCode;
	}
	
	public void setActItemCode(String actItemCode) {
		this.actItemCode = actItemCode;
	}
	
	public String getItemType() {
		return itemType;
	}
	
	public void setItemType(String itemType) {
		this.itemType = itemType;
	}
	
	public String getSysCd() {
		return sysCd;
	}

	public void setSysCd(String sysCd) {
		this.sysCd = sysCd;
	}

	public String getSplrTypCd() {
		return splrTypCd;
	}

	public void setSplrTypCd(String splrTypCd) {
		this.splrTypCd = splrTypCd;
	}

	public String getRetailerName() {
		return retailerName;
	}

	public void setRetailerName(String retailerName) {
		this.retailerName = retailerName;
	}


	// Changes to incorporate Brand
	public String field23;
	public String field24;
	public String field25;
	public String field26;
	public String field27;
	public String field28;
	public String field29;
	public String field30;
	public String field31;
	
	public String UserAttrVal1;
	public String UserAttrVal2;
	public String UserAttrVal3;
	public String UserAttrVal4;
	
	// Changes to incorporate Brand - Ends
	
	public String field20;
	public String field21;
	
	@Override
    public Object clone() throws CloneNotSupportedException {
		ItemDTO cloned = (ItemDTO)super.clone();
		return cloned;
	}

	public String getShelfSize() {
		return shelfSize;
	}

	public void setShelfSize(String shelfSize) {
		this.shelfSize = shelfSize;
	}

	public String getAltRetailerItemCode() {
		return altRetailerItemCode;
	}

	public void setAltRetailerItemCode(String altRetailerItemCode) {
		this.altRetailerItemCode = altRetailerItemCode;
	}

	public boolean isActive() {
		return isActive;
	}

	public void setActive(boolean isActive) {
		this.isActive = isActive;
	}
	
	public boolean isLirInd() {
		return isLirInd;
	}
	
	public void setLirInd(boolean isLirInd) {
		this.isLirInd = isLirInd;
	}
	
	public boolean getPrestoAssignedLirInd() {
		return isPrestoAssignedLirInd;
	}
	
	public void setPrestoAssignedLirInd(boolean isPrestoAssignedLirInd) {
		this.isPrestoAssignedLirInd = isPrestoAssignedLirInd;
	}
	
	public ItemDetailKey getItemDetailKey(){
		return new ItemDetailKey(PrestoUtil.castUPC(upc, false), retailerItemCode);
	}
}
