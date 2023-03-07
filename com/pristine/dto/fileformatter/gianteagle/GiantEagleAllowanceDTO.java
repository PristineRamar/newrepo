package com.pristine.dto.fileformatter.gianteagle;

public class GiantEagleAllowanceDTO implements Cloneable {
	 private String UPC;
	 private String WHITEM_NO;
	 private String STRT_DTE;
	 private String END_DTE;
	 private String CST_ZONE_NO;
	 private String SPLR_NO;        
	 private String ALLW_STAT_CD;  
	  //If long term flg is Y, add ALLW_AMT to base cost, it becomes new LIST_COST. Base cost becomes deal cost.
	 private double ALLW_AMT;
	  //- DEAL_COST, GIve preference to DEAL
	 private double DEAL_CST;
	 private String LONG_TERM_REFLECT_FG;
	 private String DEAL_ID;
	 private String BNR_CD;
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
	public String getEND_DTE() {
		return END_DTE;
	}
	public void setEND_DTE(String eND_DTE) {
		END_DTE = eND_DTE;
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
	public String getALLW_STAT_CD() {
		return ALLW_STAT_CD;
	}
	public void setALLW_STAT_CD(String aLLW_STAT_CD) {
		ALLW_STAT_CD = aLLW_STAT_CD;
	}
	
	public String getLONG_TERM_REFLECT_FG() {
		return LONG_TERM_REFLECT_FG;
	}
	public void setLONG_TERM_REFLECT_FG(String lONG_TERM_REFLECT_FG) {
		LONG_TERM_REFLECT_FG = lONG_TERM_REFLECT_FG;
	}
	public String getDEAL_ID() {
		return DEAL_ID;
	}
	public void setDEAL_ID(String dEAL_ID) {
		DEAL_ID = dEAL_ID;
	}
	public double getALLW_AMT() {
		return ALLW_AMT;
	}
	public void setALLW_AMT(double aLLW_AMT) {
		ALLW_AMT = aLLW_AMT;
	}
	public double getDEAL_CST() {
		return DEAL_CST;
	}
	public void setDEAL_CST(double dEAL_CST) {
		DEAL_CST = dEAL_CST;
	}
	public String getBNR_CD() {
		return BNR_CD;
	}
	public void setBNR_CD(String bNR_CD) {
		BNR_CD = bNR_CD;
	}
	 
	@Override
    public Object clone() throws CloneNotSupportedException {
		GiantEagleAllowanceDTO cloned = (GiantEagleAllowanceDTO)super.clone();
		return cloned;
	} 
}
