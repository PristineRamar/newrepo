package com.pristine.dto.fileformatter.gianteagle;

import com.pristine.dto.ItemDTO;

public class GiantEagleCostDTO implements Cloneable{
	  private String UPC;
	  private String WHITEM_NO;
	  private String STRT_DTE;
	  private String CST_ZONE_NO;
	  private String SPLR_NO;
	  private String CST_STAT_CD;
	  private double BS_CST_AKA_STORE_CST;
	  private double DLVD_CST_AKA_WHSE_CST;
	  private String LONG_TERM_REFLECT_FG;
	  private String retailerItemcode;
	  private String prcGrpCode;
	  private double dealCost;
	  private String dealStartDate;
	  private String dealEndDate;
	  private String BNR_CD;
	  private double ALLW_AMT;
	  private String errorMessage;
	  
	public String getUPC() {
		return UPC;
	}
	public void setUPC(String uPC) {
		UPC = uPC;
	}
	public String getWHITEM_NO() {
		return WHITEM_NO;
	}
	public void setWHITEM_NO(String wHITEM_NO) {
		WHITEM_NO = wHITEM_NO;
	}
	public String getSTRT_DTE() {
		return STRT_DTE;
	}
	public void setSTRT_DTE(String sTRT_DTE) {
		STRT_DTE = sTRT_DTE;
	}
	public String getCST_ZONE_NO() {
		return CST_ZONE_NO;
	}
	public void setCST_ZONE_NO(String cST_ZONE_NO) {
		CST_ZONE_NO = cST_ZONE_NO;
	}
	public String getSPLR_NO() {
		return SPLR_NO;
	}
	public void setSPLR_NO(String sPLR_NO) {
		SPLR_NO = sPLR_NO;
	}
	public String getCST_STAT_CD() {
		return CST_STAT_CD;
	}
	public void setCST_STAT_CD(String cST_STAT_CD) {
		CST_STAT_CD = cST_STAT_CD;
	}
	public double getBS_CST_AKA_STORE_CST() {
		return BS_CST_AKA_STORE_CST;
	}
	public void setBS_CST_AKA_STORE_CST(double bS_CST_AKA_STORE_CST) {
		BS_CST_AKA_STORE_CST = bS_CST_AKA_STORE_CST;
	}
	public double getDLVD_CST_AKA_WHSE_CST() {
		return DLVD_CST_AKA_WHSE_CST;
	}
	public void setDLVD_CST_AKA_WHSE_CST(double dLVD_CST_AKA_WHSE_CST) {
		DLVD_CST_AKA_WHSE_CST = dLVD_CST_AKA_WHSE_CST;
	}
	public String getLONG_TERM_REFLECT_FG() {
		return LONG_TERM_REFLECT_FG;
	}
	public void setLONG_TERM_REFLECT_FG(String lONG_TERM_REFLECT_FG) {
		LONG_TERM_REFLECT_FG = lONG_TERM_REFLECT_FG;
	}
	public String getRetailerItemcode() {
		return retailerItemcode;
	}
	public void setRetailerItemcode(String retailerItemcode) {
		this.retailerItemcode = retailerItemcode;
	}
	public String getPrcGrpCode() {
		return prcGrpCode;
	}
	public void setPrcGrpCode(String prcGrpCode) {
		this.prcGrpCode = prcGrpCode;
	}
	public double getDealCost() {
		return dealCost;
	}
	public void setDealCost(double dealCost) {
		this.dealCost = dealCost;
	}
	public String getDealStartDate() {
		return dealStartDate;
	}
	public void setDealStartDate(String dealStartDate) {
		this.dealStartDate = dealStartDate;
	}
	public String getDealEndDate() {
		return dealEndDate;
	}
	public void setDealEndDate(String dealEndDate) {
		this.dealEndDate = dealEndDate;
	}
	public String getBNR_CD() {
		return BNR_CD;
	}
	public void setBNR_CD(String bNR_CD) {
		BNR_CD = bNR_CD;
	}
	public double getALLW_AMT() {
		return ALLW_AMT;
	}
	public void setALLW_AMT(double aLLW_AMT) {
		ALLW_AMT = aLLW_AMT;
	}
	  
	@Override
    public Object clone() throws CloneNotSupportedException {
		GiantEagleCostDTO cloned = (GiantEagleCostDTO)super.clone();
		return cloned;
	}
	public String getErrorMessage() {
		return errorMessage;
	}
	public void setErrorMessage(String errorMessage) {
		this.errorMessage = errorMessage;
	}  
}
