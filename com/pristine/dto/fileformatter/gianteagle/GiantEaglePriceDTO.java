package com.pristine.dto.fileformatter.gianteagle;

import com.pristine.util.Constants;

public class GiantEaglePriceDTO implements Cloneable {
	private String RITEM_NO;
	private String PRC_STRT_DTE;
	private String PRC_END_DTE;
	private double CURR_PRC;
	private int REG_MUNIT_CNT;
	private int MUNIT_CNT;
	private String PRC_STAT_CD;
	private String PROM_CD;
	private double PROM_PCT;
	private double PROM_AMT_OFF;
	private String ZN_NO;
	private String PRC_GRP_CD;
	private String SPLR_NO;
	private String PRC_TYP_IND;
	private String DEAL_ID;
	private String OFFER_ID;
	private String OFFER_DSCR;
	private String AD_TYP_DSCR;
	private String AD_LOCN_DSCR;
	private String PCT_OF_PGE;
	private int saleQty;
	private double salePrice;
	private String saleStartDate;
	private String saleEndDate;
	private String BNR_CD;
	private double regPrice;
	private String itemDesc;
	private boolean isProcessed;
	private int itemCode;
	private int loctionLevelId;
	private int locationId;
	private String NEW_LOW_PRC_FG;
	private String NEW_LOW_PRC_END_DTE;
	private String LOW_PRC_FG;
	private String LOW_PRC_END_DTE;
	private String upc;
	private int calendarId;
	
	public String getRITEM_NO() {
		return RITEM_NO;
	}
	public void setRITEM_NO(String rITEM_NO) {
		RITEM_NO = rITEM_NO;
	}
	public String getPRC_STRT_DTE() {
		return PRC_STRT_DTE;
	}
	public void setPRC_STRT_DTE(String pRC_STRT_DTE) {
		PRC_STRT_DTE = pRC_STRT_DTE;
	}
	public String getPRC_END_DTE() {
		return PRC_END_DTE;
	}
	public void setPRC_END_DTE(String pRC_END_DTE) {
		PRC_END_DTE = pRC_END_DTE;
	}
	public double getCURR_PRC() {
		return CURR_PRC;
	}
	public void setCURR_PRC(double cURR_PRC) {
		CURR_PRC = cURR_PRC;
	}
	public int getMUNIT_CNT() {
		return MUNIT_CNT;
	}
	public void setMUNIT_CNT(int mUNIT_CNT) {
		MUNIT_CNT = mUNIT_CNT;
	}
	public String getPRC_STAT_CD() {
		return PRC_STAT_CD;
	}
	public void setPRC_STAT_CD(String pRC_STAT_CD) {
		PRC_STAT_CD = pRC_STAT_CD;
	}
	public String getPROM_CD() {
		return PROM_CD;
	}
	public void setPROM_CD(String pROM_CD) {
		PROM_CD = pROM_CD;
	}
	public double getPROM_PCT() {
		return PROM_PCT;
	}
	public void setPROM_PCT(double pROM_PCT) {
		PROM_PCT = pROM_PCT;
	}
	public double getPROM_AMT_OFF() {
		return PROM_AMT_OFF;
	}
	public void setPROM_AMT_OFF(double pROM_AMT_OFF) {
		PROM_AMT_OFF = pROM_AMT_OFF;
	}
	public String getZN_NO() {
		return ZN_NO;
	}
	public void setZN_NO(String zN_NO) {
		ZN_NO = zN_NO;
	}
	public String getPRC_GRP_CD() {
		return PRC_GRP_CD;
	}
	public void setPRC_GRP_CD(String pRC_GRP_CD) {
		PRC_GRP_CD = pRC_GRP_CD;
	}
	public String getSPLR_NO() {
		return SPLR_NO;
	}
	public void setSPLR_NO(String sPLR_NO) {
		SPLR_NO = sPLR_NO;
	}
	public String getPRC_TYP_IND() {
		return PRC_TYP_IND;
	}
	public void setPRC_TYP_IND(String pRC_TYP_IND) {
		PRC_TYP_IND = pRC_TYP_IND;
	}
	public String getDEAL_ID() {
		return DEAL_ID;
	}
	public void setDEAL_ID(String dEAL_ID) {
		DEAL_ID = dEAL_ID;
	}
	public String getOFFER_ID() {
		return OFFER_ID;
	}
	public void setOFFER_ID(String oFFER_ID) {
		OFFER_ID = oFFER_ID;
	}
	public String getOFFER_DSCR() {
		return OFFER_DSCR;
	}
	public void setOFFER_DSCR(String oFFER_DSCR) {
		OFFER_DSCR = oFFER_DSCR;
	}
	public String getAD_TYP_DSCR() {
		return AD_TYP_DSCR;
	}
	public void setAD_TYP_DSCR(String aD_TYP_DSCR) {
		AD_TYP_DSCR = aD_TYP_DSCR;
	}
	public String getAD_LOCN_DSCR() {
		return AD_LOCN_DSCR;
	}
	public void setAD_LOCN_DSCR(String aD_LOCN_DSCR) {
		AD_LOCN_DSCR = aD_LOCN_DSCR;
	}
	public String getPCT_OF_PGE() {
		return PCT_OF_PGE;
	}
	public void setPCT_OF_PGE(String pCT_OF_PGE) {
		PCT_OF_PGE = pCT_OF_PGE;
	}
	
	@Override
    public Object clone() throws CloneNotSupportedException {
		GiantEaglePriceDTO cloned = (GiantEaglePriceDTO)super.clone();
		return cloned;
	}
	public double getSalePrice() {
		return salePrice;
	}
	public void setSalePrice(double salePrice) {
		this.salePrice = salePrice;
	}
	public int getSaleQty() {
		return saleQty;
	}
	public void setSaleQty(int saleQty) {
		this.saleQty = saleQty;
	}
	public String getSaleStartDate() {
		return saleStartDate;
	}
	public void setSaleStartDate(String saleStartDate) {
		this.saleStartDate = saleStartDate;
	}
	public String getSaleEndDate() {
		return saleEndDate;
	}
	public void setSaleEndDate(String saleEndDate) {
		this.saleEndDate = saleEndDate;
	}
	public String getBNR_CD() {
		return BNR_CD;
	}
	public void setBNR_CD(String bNR_CD) {
		BNR_CD = bNR_CD;
	}

	public double getRegPrice() {
		return regPrice;
	}

	public void setRegPrice(double regPrice) {
		this.regPrice = regPrice;
	}

	public String getItemDesc() {
		return itemDesc;
	}

	public void setItemDesc(String itemDesc) {
		this.itemDesc = itemDesc;
	}
	public int getREG_MUNIT_CNT() {
		return REG_MUNIT_CNT;
	}
	public void setREG_MUNIT_CNT(int rEG_MUNIT_CNT) {
		REG_MUNIT_CNT = rEG_MUNIT_CNT;
	}
	public boolean isProcessed() {
		return isProcessed;
	}
	public void setProcessed(boolean isProcessed) {
		this.isProcessed = isProcessed;
	}
	public String getNEW_LOW_PRC_FG() {
		return NEW_LOW_PRC_FG;
	}
	public void setNEW_LOW_PRC_FG(String nEW_LOW_PRC_FG) {
		NEW_LOW_PRC_FG = nEW_LOW_PRC_FG;
	}
	public String getNEW_LOW_PRC_END_DTE() {
		return NEW_LOW_PRC_END_DTE;
	}
	public void setNEW_LOW_PRC_END_DTE(String nEW_LOW_PRC_END_DTE) {
		NEW_LOW_PRC_END_DTE = nEW_LOW_PRC_END_DTE;
	}
	public String getLOW_PRC_FG() {
		return LOW_PRC_FG;
	}
	public void setLOW_PRC_FG(String lOW_PRC_FG) {
		LOW_PRC_FG = lOW_PRC_FG;
	}
	public String getLOW_PRC_END_DTE() {
		return LOW_PRC_END_DTE;
	}
	public void setLOW_PRC_END_DTE(String lOW_PRC_END_DTE) {
		LOW_PRC_END_DTE = lOW_PRC_END_DTE;
	}
	public int getItemCode() {
		return itemCode;
	}
	public void setItemCode(int itemCode) {
		this.itemCode = itemCode;
	}
	public int getLoctionLevelId() {
		return loctionLevelId;
	}
	public void setLoctionLevelId(int loctionLevelId) {
		this.loctionLevelId = loctionLevelId;
	}
	public int getLocationId() {
		return locationId;
	}
	public void setLocationId(int locationId) {
		this.locationId = locationId;
	}
	public String getUpc() {
		return upc;
	}
	public void setUpc(String upc) {
		this.upc = upc;
	}
	
	public String getZoneNumber() {
		String zoneNum = getBNR_CD() + "-" + getZN_NO() + "-"
				+ getPRC_GRP_CD();
		// Append vendor number to zone if it is not blank
		if (!Constants.EMPTY.equals(getSPLR_NO().trim())
				&& getSPLR_NO() != null) {
			zoneNum = zoneNum + "-" + getSPLR_NO();
			return zoneNum;
		}
		
		return zoneNum;
	}
	public int getCalendarId() {
		return calendarId;
	}
	public void setCalendarId(int calendarId) {
		this.calendarId = calendarId;
	}
	
}