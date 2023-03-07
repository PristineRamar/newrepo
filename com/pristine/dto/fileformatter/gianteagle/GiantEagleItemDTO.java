package com.pristine.dto.fileformatter.gianteagle;

public class GiantEagleItemDTO {
	private String RITEM_NO;
	private String UPC;
	private String WHITEM_NO;
	private String ITM_DSCR;
	private String SPLR_NO;
	private String SPLR_TYP_CD;
	private String ITM_SZ;
	private String ITM_SZ_UOM;
	private String ITM_PAK;
	private String CAT_CD;
	private String CAT_DSCR;
	private String DW_LOB_CD;
	private String DW_LOB_DSCR;
	private String EFCT_DTE;
	private String GRP_CD;
	private String GRP_DSCR;
	private String SGRP_CD;
	private String SGRP_DSCR;
	private String PRC_GRP_CD;
	private String PRC_GRP_DSCR;
	private String SYS_CD;
	private String CORP_BRAND_ID;
	private String BRND_LBL_DSCR;
	private String FMLY_CD;
	private String FMLY_DSCR;
	private String LEVEL_TYPE;
	private String EM_PTY;
	private String PORTFOLIO_CODE;
	private String PORTFOLIO_NAME;
	private String privateLabelCode;
	private String TIER;
	private String ITM_STAT;
	
	
	public String getRITEM_NO() {
		return RITEM_NO;
	}
	public void setRITEM_NO(String rITEM_NO) {
		RITEM_NO = rITEM_NO;
	}
	
	public String getWHITEM_NO() {
		return WHITEM_NO;
	}
	public void setWHITEM_NO(String wHITEM_NO) {
		WHITEM_NO = wHITEM_NO;
	}
	public String getUPC() {
		return UPC;
	}
	public void setUPC(String uPC) {
		UPC = uPC;
	}
	public String getITM_DSCR() {
		return ITM_DSCR;
	}
	public void setITM_DSCR(String iTM_DSCR) {
		ITM_DSCR = iTM_DSCR;
	}
	public String getITM_SZ_UOM() {
		return ITM_SZ_UOM;
	}
	public void setITM_SZ_UOM(String iTM_SZ_UOM) {
		ITM_SZ_UOM = iTM_SZ_UOM;
	}
	 
	public String getITM_SZ() {
		return ITM_SZ;
	}
	public void setITM_SZ(String iTM_SZ) {
		ITM_SZ = iTM_SZ;
	}
	
	//Added by RB - LOB Name
	public String getLOB_DSCR() {
		return DW_LOB_DSCR;
	}
	public void setLOB_DSCR(String lOB_DSCR) {
		DW_LOB_DSCR = lOB_DSCR;
	}
	 
	//Added by RB - LOB Code
	public String getLOB_CD() {
		return DW_LOB_CD;
	}
	public void setLOB_CD(String lOB_CD) {
		DW_LOB_CD = lOB_CD;
	}
	
	//Added by RB - Effective Date
	public String getEFCT_DTE() {
		return EFCT_DTE;
	}
	public void setEFCT_DTE(String eFCT_DTE) {
		EFCT_DTE = eFCT_DTE;
	}
	 
	public String getCAT_DSCR() {
		return CAT_DSCR;
	}
	public void setCAT_DSCR(String cAT_DSCR) {
		CAT_DSCR = cAT_DSCR;
	}
	 
	public String getCAT_CD() {
		return CAT_CD;
	}
	public void setCAT_CD(String cAT_CD) {
		CAT_CD = cAT_CD;
	}
	public String getFMLY_CD() {
		return FMLY_CD;
	}
	public void setFMLY_CD(String fMLY_CD) {
		FMLY_CD = fMLY_CD;
	}
	public String getGRP_DSCR() {
		return GRP_DSCR;
	}
	public void setGRP_DSCR(String gRP_DSCR) {
		GRP_DSCR = gRP_DSCR;
	}
	 
	 
	public String getSGRP_DSCR() {
		return SGRP_DSCR;
	}
	public void setSGRP_DSCR(String sGRP_DSCR) {
		SGRP_DSCR = sGRP_DSCR;
	}
	 
	public String getITM_PAK() {
		return ITM_PAK;
	}
	public void setITM_PAK(String iTM_PAK) {
		ITM_PAK = iTM_PAK;
	}
	public String getGRP_CD() {
		return GRP_CD;
	}
	public void setGRP_CD(String gRP_CD) {
		GRP_CD = gRP_CD;
	}
	public String getSGRP_CD() {
		return SGRP_CD;
	}
	public void setSGRP_CD(String sGRP_CD) {
		SGRP_CD = sGRP_CD;
	}
	public String getPRC_GRP_CD() {
		return PRC_GRP_CD;
	}
	public void setPRC_GRP_CD(String pRC_GRP_CD) {
		PRC_GRP_CD = pRC_GRP_CD;
	}
	public String getPRC_GRP_DSCR() {
		return PRC_GRP_DSCR;
	}
	public void setPRC_GRP_DSCR(String pRC_GRP_DSCR) {
		PRC_GRP_DSCR = pRC_GRP_DSCR;
	}
	 
	 
	public String getCORP_BRAND_ID() {
		return CORP_BRAND_ID;
	}
	public void setCORP_BRAND_ID(String cORP_BRAND_ID) {
		CORP_BRAND_ID = cORP_BRAND_ID;
	}
	public String getBRND_LBL_DSCR() {
		return BRND_LBL_DSCR;
	}
	public void setBRND_LBL_DSCR(String bRND_LBL_DSCR) {
		BRND_LBL_DSCR = bRND_LBL_DSCR;
	}
	public String getSPLR_NO() {
		return SPLR_NO;
	}
	public void setSPLR_NO(String sPLR_NO) {
		SPLR_NO = sPLR_NO;
	}
	public String getSPLR_TYP_CD() {
		return SPLR_TYP_CD;
	}
	public void setSPLR_TYP_CD(String sPLR_TYP_CD) {
		SPLR_TYP_CD = sPLR_TYP_CD;
	}
	public String getSYS_CD() {
		return SYS_CD;
	}
	public void setSYS_CD(String sYS_CD) {
		SYS_CD = sYS_CD;
	}
	public String getLEVEL_TYPE() {
		return LEVEL_TYPE;
	}
	public void setLEVEL_TYPE(String lEVEL_TYPE) {
		LEVEL_TYPE = lEVEL_TYPE;
	}
	public String getEM_PTY() {
		return EM_PTY;
	}
	public void setEM_PTY(String eM_PTY) {
		EM_PTY = eM_PTY;
	}
	public String getPORTFOLIO_CODE() {
		return PORTFOLIO_CODE;
	}
	public void setPORTFOLIO_CODE(String pORTFOLIO_CODE) {
		PORTFOLIO_CODE = pORTFOLIO_CODE;
	}
	public String getPORTFOLIO_NAME() {
		return PORTFOLIO_NAME;
	}
	public void setPORTFOLIO_NAME(String pORTFOLIO_NAME) {
		PORTFOLIO_NAME = pORTFOLIO_NAME;
	}
	public String getFMLY_DSCR() {
		return FMLY_DSCR;
	}
	public void setFMLY_DSCR(String fMLY_DSCR) {
		FMLY_DSCR = fMLY_DSCR;
	}
	public String getPrivateLabelCode() {
		return privateLabelCode;
	}
	public void setPrivateLabelCode(String privateLabelCode) {
		this.privateLabelCode = privateLabelCode;
	}
	public String getTIER() {
		return TIER;
	}
	public void setTIER(String tIER) {
		TIER = tIER;
	}
	public String getITM_STAT() {
		return ITM_STAT;
	}
	public void setITM_STAT(String iTM_STAT) {
		ITM_STAT = iTM_STAT;
	}
	
}
