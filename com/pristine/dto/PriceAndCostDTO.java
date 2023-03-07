/*
 * To change this template, choose Tools | Templates
 * and open the template in the editor.
 */
package com.pristine.dto;

/**
 *
 * @author sakthidasan
 */
public class PriceAndCostDTO {

    private String vendorNo;
    private String itemNo;
    private String deptarmentNo;
    private String sourceCode;
    private String zone;
    private String costEffDate;
    private String strCurrentCost;
    private float currentCost;
    private String promoCostEffDate;
    private String promoCostEndDate;
    private String strPromoCost;
    private float promoCost;
    private String retailEffDate;
    private String strCurrRetail;
    private float currRetail;
    private String promoRetailEffDate;
    private String promoRetailEndDate;
    private String strPromoRetail;
    private float promoRetail;
    private String targetCompRetail;
    private String compSymbol;
    private int rtlQuanity;
    private int promoRtlQuantity;
    private String targetDate;
    private String bbEffDate;
    private String bbEndDate;
    private String strBbAmount;
    private float bbAmount;
    private String baEffDate;
    private String strBaAmount;
    private float baAmount;
    private String bsEffDate;
    private String bsEndDate;
    private String strBsAmount;
    private float bsAmount;
    private String filler;
    private String upc;
    private String commodityCode;
    private String manufacturersCode;
    private String productCode;
    private String strSizeUnits;
    private float sizeUnits;
    private String sizeCode;
    private String companyPack;
    private int casePack;

    private String perishableInd;
    private String majorDeptDesc;
    private String portfolioMgr;
    private String deptName;
    private String deptCode;
    private String catName;
    private String catCode;
    private String subCatName;
    private String subCatCode;
	private String segmentName;
    private String segmentCode;
    private String itemName;
    private String privateLabelInd;
    
    private String recordType;
    private boolean isProcessedFlag = false;
    
    private String billbackType; // Changes to process new billback feed from TOPS
    private int calendarId;

	public int getCalendarId() {
		return calendarId;
	}

	public void setCalendarId(int calendarId) {
		this.calendarId = calendarId;
	}

	public String getPerishableInd() {
		return perishableInd;
	}

	public void setPerishableInd(String perishableInd) {
		this.perishableInd = perishableInd;
	}

	public String getMajorDeptDesc() {
		return majorDeptDesc;
	}

	public void setMajorDeptDesc(String majorDeptDesc) {
		this.majorDeptDesc = majorDeptDesc;
	}

	public String getPortfolioMgr() {
		return portfolioMgr;
	}

	public void setPortfolioMgr(String portfolioMgr) {
		this.portfolioMgr = portfolioMgr;
	}

	public String getDeptName() {
		return deptName;
	}

	public void setDeptName(String deptName) {
		this.deptName = deptName;
	}

	public String getDeptCode() {
		return deptCode;
	}

	public void setDeptCode(String deptCode) {
		this.deptCode = deptCode;
	}

	public String getCatName() {
		return catName;
	}

	public void setCatName(String catName) {
		this.catName = catName;
	}

	public String getCatCode() {
		return catCode;
	}

	public void setCatCode(String catCode) {
		this.catCode = catCode;
	}

	public String getSubCatName() {
		return subCatName;
	}

	public void setSubCatName(String subCatName) {
		this.subCatName = subCatName;
	}

	public String getSegmentName() {
		return segmentName;
	}

	public void setSegmentName(String segmentName) {
		this.segmentName = segmentName;
	}

	public String getSegmentCode() {
		return segmentCode;
	}

	public void setSegmentCode(String segmentCode) {
		this.segmentCode = segmentCode;
	}

	public String getItemName() {
		return itemName;
	}

	public void setItemName(String itemName) {
		this.itemName = itemName;
	}

	public String getPrivateLabelInd() {
		return privateLabelInd;
	}

	public void setPrivateLabelInd(String privateLabelInd) {
		this.privateLabelInd = privateLabelInd;
	}

	
    public String getSubCatCode() {
		return subCatCode;
	}

	public void setSubCatCode(String subCatCode) {
		this.subCatCode = subCatCode;
	}


    public int getCasePack() {
		return casePack;
	}

	public void setCasePack(int casePack) {
		this.casePack = casePack;
	}

	public float getBaAmount() {
        return baAmount;
    }

    public void setBaAmount(float baAmount) {
        this.baAmount = baAmount;
    }

    public String getBaEffDate() {
        return baEffDate;
    }

    public void setBaEffDate(String baEffDate) {
        this.baEffDate = baEffDate;
    }

    public float getBbAmount() {
        return bbAmount;
    }

    public void setBbAmount(float bbAmount) {
        this.bbAmount = bbAmount;
    }

    public String getBbEffDate() {
        return bbEffDate;
    }

    public void setBbEffDate(String bbEffDate) {
        this.bbEffDate = bbEffDate;
    }

    public String getBbEndDate() {
        return bbEndDate;
    }

    public void setBbEndDate(String bbEndDate) {
        this.bbEndDate = bbEndDate;
    }

    public float getBsAmount() {
        return bsAmount;
    }

    public void setBsAmount(float bsAmount) {
        this.bsAmount = bsAmount;
    }

    public String getBsEffDate() {
        return bsEffDate;
    }

    public void setBsEffDate(String bsEffDate) {
        this.bsEffDate = bsEffDate;
    }

    public String getBsEndDate() {
        return bsEndDate;
    }

    public void setBsEndDate(String bsEndDate) {
        this.bsEndDate = bsEndDate;
    }

    public String getCommodityCode() {
        return commodityCode;
    }

    public void setCommodityCode(String commodityCode) {
        this.commodityCode = commodityCode;
    }

    public String getCompanyPack() {
        return companyPack;
    }

    public void setCompanyPack(String companyPack) {
        this.companyPack = companyPack;
    }

    public String getFiller() {
        return filler;
    }

    public void setFiller(String filler) {
        this.filler = filler;
    }

    public String getManufacturersCode() {
        return manufacturersCode;
    }

    public void setManufacturersCode(String manufacturersCode) {
        this.manufacturersCode = manufacturersCode;
    }

    public String getProductCode() {
        return productCode;
    }

    public void setProductCode(String productCode) {
        this.productCode = productCode;
    }

    public String getSizeCode() {
        return sizeCode;
    }

    public void setSizeCode(String sizeCode) {
        this.sizeCode = sizeCode;
    }

    public float getSizeUnits() {
        return sizeUnits;
    }

    public void setSizeUnits(float sizeUnits) {
        this.sizeUnits = sizeUnits;
    }

    public String getStrBaAmount() {
        return strBaAmount;
    }

    public void setStrBaAmount(String strBaAmount) {
        this.strBaAmount = strBaAmount;
    }

    public String getStrBbAmount() {
        return strBbAmount;
    }

    public void setStrBbAmount(String strBbAmount) {
        this.strBbAmount = strBbAmount;
    }

    public String getStrBsAmount() {
        return strBsAmount;
    }

    public void setStrBsAmount(String strBsAmount) {
        this.strBsAmount = strBsAmount;
    }

    public String getStrSizeUnits() {
        return strSizeUnits;
    }

    public void setStrSizeUnits(String strSizeUnits) {
        this.strSizeUnits = strSizeUnits;
    }

    public String getTargetDate() {
        return targetDate;
    }

    public void setTargetDate(String targetDate) {
        this.targetDate = targetDate;
    }

    public String getUpc() {
        return upc;
    }

    public void setUpc(String upc) {
        this.upc = upc;
    }

    
    public String getStrCurrRetail() {
        return strCurrRetail;
    }

    public void setStrCurrRetail(String strCurrRetail) {
        this.strCurrRetail = strCurrRetail;
    }

    public String getStrPromoCost() {
        return strPromoCost;
    }

    public void setStrPromoCost(String strPromoCost) {
        this.strPromoCost = strPromoCost;
    }

    public String getStrPromoRetail() {
        return strPromoRetail;
    }

    public void setStrPromoRetail(String strPromoRetail) {
        this.strPromoRetail = strPromoRetail;
    }

    public int getPromoRtlQuantity() {
        return promoRtlQuantity;
    }

    public void setPromoRtlQuantity(int promoRtlQuantity) {
        this.promoRtlQuantity = promoRtlQuantity;
    }

    public int getRtlQuanity() {
        return rtlQuanity;
    }

    public void setRtlQuanity(int rtlQuanity) {
        this.rtlQuanity = rtlQuanity;
    }

    public String getStrCurrentCost() {
        return strCurrentCost;
    }

    public void setStrCurrentCost(String strCurrentCost) {
        this.strCurrentCost = strCurrentCost;
    }

    public String getTargetCompRetail() {
        return targetCompRetail;
    }

    public void setTargetCompRetail(String targetCompRetail) {
        this.targetCompRetail = targetCompRetail;
    }

    

    public String getCompSymbol() {
        return compSymbol;
    }

    public void setCompSymbol(String compSymbol) {
        this.compSymbol = compSymbol;
    }

    public String getCostEffDate() {
        return costEffDate;
    }

    public void setCostEffDate(String costEffDate) {
        this.costEffDate = costEffDate;
    }

   

    public String getDeptarmentNo() {
        return deptarmentNo;
    }

    public void setDeptarmentNo(String deptarmentNo) {
        this.deptarmentNo = deptarmentNo;
    }

    public String getItemNo() {
        return itemNo;
    }

    public void setItemNo(String itemNo) {
        this.itemNo = itemNo;
    }

   

    public String getPromoCostEffDate() {
        return promoCostEffDate;
    }

    public void setPromoCostEffDate(String promoCostEffDate) {
        this.promoCostEffDate = promoCostEffDate;
    }

    public String getPromoCostEndDate() {
        return promoCostEndDate;
    }

    public void setPromoCostEndDate(String promoCostEndDate) {
        this.promoCostEndDate = promoCostEndDate;
    }

    public float getCurrRetail() {
        return currRetail;
    }

    public void setCurrRetail(float currRetail) {
        this.currRetail = currRetail;
    }

    public float getCurrentCost() {
        return currentCost;
    }

    public void setCurrentCost(float currentCost) {
        this.currentCost = currentCost;
    }

    public float getPromoCost() {
        return promoCost;
    }

    public void setPromoCost(float promoCost) {
        this.promoCost = promoCost;
    }

    public float getPromoRetail() {
        return promoRetail;
    }

    public void setPromoRetail(float promoRetail) {
        this.promoRetail = promoRetail;
    }

   
    public String getPromoRetailEffDate() {
        return promoRetailEffDate;
    }

    public void setPromoRetailEffDate(String promoRetailEffDate) {
        this.promoRetailEffDate = promoRetailEffDate;
    }

    public String getPromoRetailEndDate() {
        return promoRetailEndDate;
    }

    public void setPromoRetailEndDate(String promoRetailEndDate) {
        this.promoRetailEndDate = promoRetailEndDate;
    }

   

    public String getRetailEffDate() {
        return retailEffDate;
    }

    public void setRetailEffDate(String retailEffDate) {
        this.retailEffDate = retailEffDate;
    }

    
    public String getSourceCode() {
        return sourceCode;
    }

    public void setSourceCode(String sourceCode) {
        this.sourceCode = sourceCode;
    }

  

    public String getVendorNo() {
        return vendorNo;
    }

    public void setVendorNo(String vendorNo) {
        this.vendorNo = vendorNo;
    }

    public String getZone() {
        return zone;
    }

    public void setZone(String zone) {
        this.zone = zone;
    }
    
    public String getRecordType() {
		return recordType;
	}

	public void setRecordType(String recordType) {
		this.recordType = recordType;
	}
	
	public boolean isProcessedFlag() {
		return isProcessedFlag;
	}
	public void setProcessedFlag(boolean isProcessedFlag) {
		this.isProcessedFlag = isProcessedFlag;
	}
	
	public void copy(PriceAndCostDTO tempDTO){
		this.recordType = tempDTO.recordType;
		this.vendorNo = tempDTO.vendorNo;
		this.itemNo = tempDTO.itemNo;
		this.upc = tempDTO.upc;
		this.sourceCode = tempDTO.sourceCode;
		this.zone = tempDTO.zone;
		this.costEffDate = tempDTO.costEffDate;
		this.strCurrentCost = tempDTO.strCurrentCost;
		this.promoCostEffDate = tempDTO.promoCostEffDate;
		this.promoCostEndDate = tempDTO.promoCostEndDate;
		this.strPromoCost = tempDTO.strPromoCost;
		this.companyPack = tempDTO.companyPack;
		this.billbackType = tempDTO.billbackType;
	}
	
	// Changes to process new billback feed from TOPS
	public String getBillbackType() {
		return billbackType;
	}

	public void setBillbackType(String billbackType) {
		this.billbackType = billbackType;
	}
	// Changes to process new billback feed from TOPS - Ends
}
