package com.pristine.dto.riteaid;

import java.util.HashSet;
import java.util.List;

public class RAPromoDTO implements Cloneable {

	// EventFILE
	private String eventId;
	private int groupId;
	private String typeCode;
	private String subTypeCode;
	private String status;
	private String activityStageId;
	private String actBeginDate;
	private String actEndDate;
	private String storeBeginDate;
	private String storeEndDate;
	private String eventDesc;
	private String themeCode;
	private String reportDate;
	private String adLinkCd;
	private String leadVer;
	private int regionNo;
	private String acqInd;
	// item
	// private String itemEventId;
	private int itemNum;
	private int vendSbsy;
	private int modelItemNum;
	private String mdlSubcls;
	private String currentitemNo;
	private int invUnits;
	private char dspnInd;
	private double totSalesDlr;
	private double totSalesUnit;
	private double couponAmt;
	private double rebateAmt;
	private char newImgInd;
	private double wkAvgSale;
	private char mdAprIind;

	// maapd
	// private String meventId;
	private int versId;
	private String adPageNo;
	private String blockNo;
	private String blockDesc;
	private double xRatio;;
	private double yRatio;
	private double hghtRatio;
	private double lenRatio;
	private String crtoprId;
	private String crtTsmp;
	private String lstmntTsmp;
	private String lstmnTrn;
	private String lstoprId;
	private char prmryInd;
	private String blkPrmoType;
	private String blockPrty;
	private char ovrdInd;

	// marketing

	/*
	 * private String mventId; private int mversId;
	 */

	private String eventGroupId;
	private String prjRegn;
	private String projYearwKSrt;
	private String projYearwKEnd;
	private String evtGrpType;
	/* private String currentItemNo; */
	/*
	 * private String madPageNo; private String mblockNo;
	 */
	private String creator;
	private String lastmainTime;
	private String lastmainOpt;
	private String lastmainTran;
	private String mblockPrty;
	private char aprvlInd;
	private String versPrcCode;
	private double price;
	private int pricMultiple;
	private double scanBkAlw;
	private double scanBkPct;
	private double avgStrSales;
	private char evtInclInd;
	private String createdTime;
	private double prjSalUnit;
	private double prjSalDlr;
	private double prjDmDlr;
	private double prjMkdnDlr;
	private double prjExtCost;
	private double prjItmCnt;
	private String dstnMeth;
	private double oiAlwAmt;
	private double bbAlwAmt;
	private char adPictInd;
	private String cntrcId;
	private double coopDlrAmt;
	private String mixMtchCode;;
	private double mrebateAmt;
	private double mcouponAmt;
	private String adType;
	private String promoNo;
	private char crdReqInd;
	private char grpTypeInd;
	private char crdReqInd2;

	// promo

	// private String peventId;
	// private String pversId;
	private String itemNo;
	// private String pcurrentItemNo;
	// private String pstatus;
	// private double pPrice;
	private double verPrcUom;
	private int priceMultiple;
	private String priceCode;
	private String priceChgInd;
	private double avgStrSale;
	private String pdstnMeth;
	private String poMeth;
	private double pscanBkAlw;
	private String itmRefNo;
	private String adPageId;
	private char adPctrInd;
	private double wgtngFctr;
	private double InvoiceAlwAmt;
	private double pbbAlwAmt;
	private double totProjSales;
	private String pmixMtchCode;
	private int cntrId;
	// private double pcoopDlrAmt;
	private char rvwCode;
	private String projMeth;
	private String itmGrpCode;
	private double actualSalesDlr;
	private double actualSalesUnits;
	private double prjItemSalesDlr;
	private double actItemmdDlr;
	private double prjItemmdDlr;
	private double prjItemGmDlr;
	private double actItemGmDlr;
	private String pvtItlbInd;
	private char dsdInd;
	private double actItemwtAmt;
	private int onhandQty;
	private double itemAvgCost;
	private char bltcrtInd;
	// private String pgroupId;
	private String  mustBuyInd;
	private double avgPromoUnit;
	private double avgPromoRtl;
	private int buyQty;
	private int getQty;
	private double price2;
	private int priceMult2;
	private String priceCode2;

	// store
	// private String sEventId;
	private String locationNo;
	private int sVersId;
	// private String sStatus;
	private String modelStrNo;
	private String projStrInd;
	private char excludeRepInd;
	private String strBeginDate;
	private String strEndDate;

	// PROMOFIELDS
	String everdayQty;
	String everydayPrice;
	int saleQty;
	double salePrice;
	int mustBuyQty;
	double mustbuyPrice;
	double dollarOff;
	double pctOff;
	String locationLevel;
	int statusFlag;

	String getItemCode;
	private String  zoneNo;

	public String getGetItemCode() {
		return getItemCode;
	}

	public void setGetItemCode(String getItemCode) {
		this.getItemCode = getItemCode;
	}

	public int getStatusFlag() {
		return statusFlag;
	}

	public void setStatusFlag(int statusFlag) {
		this.statusFlag = statusFlag;
	}

	public String getLocationLevel() {
		return locationLevel;
	}

	public void setLocationLevel(String locationLevel) {
		this.locationLevel = locationLevel;
	}

	public String getEventGroupId() {
		return eventGroupId;
	}

	public void setEventGroupId(String eventGroupId) {
		this.eventGroupId = eventGroupId;
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

	public String getLastmainTran() {
		return lastmainTran;
	}

	public void setLastmainTran(String lastmainTran) {
		this.lastmainTran = lastmainTran;
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

	public String getEventId() {
		return eventId;
	}

	public void setEventId(String eventId) {
		this.eventId = eventId;
	}

	public int getGroupId() {
		return groupId;
	}

	public void setGroupId(int groupId) {
		this.groupId = groupId;
	}

	public String getTypeCode() {
		return typeCode;
	}

	public void setTypeCode(String typeCode) {
		this.typeCode = typeCode;
	}

	public String getSubTypeCode() {
		return subTypeCode;
	}

	public void setSubTypeCode(String subTypeCode) {
		this.subTypeCode = subTypeCode;
	}

	public String getStatus() {
		return status;
	}

	public void setStatus(String status) {
		this.status = status;
	}

	public String getActivityStageId() {
		return activityStageId;
	}

	public void setActivityStageId(String activityStageId) {
		this.activityStageId = activityStageId;
	}

	public String getActBeginDate() {
		return actBeginDate;
	}

	public void setActBeginDate(String actBeginDate) {
		this.actBeginDate = actBeginDate;
	}

	public String getActEndDate() {
		return actEndDate;
	}

	public void setActEndDate(String actEndDate) {
		this.actEndDate = actEndDate;
	}

	public String getStoreBeginDate() {
		return storeBeginDate;
	}

	public void setStoreBeginDate(String storeBeginDate) {
		this.storeBeginDate = storeBeginDate;
	}

	public String getStoreEndDate() {
		return storeEndDate;
	}

	public void setStoreEndDate(String storeEndDate) {
		this.storeEndDate = storeEndDate;
	}

	public String getEventDesc() {
		return eventDesc;
	}

	public void setEventDesc(String eventDesc) {
		this.eventDesc = eventDesc;
	}

	public String getThemeCode() {
		return themeCode;
	}

	public void setThemeCode(String themeCode) {
		this.themeCode = themeCode;
	}

	public String getReportDate() {
		return reportDate;
	}

	public void setReportDate(String reportDate) {
		this.reportDate = reportDate;
	}

	public String getAdLinkCd() {
		return adLinkCd;
	}

	public void setAdLinkCd(String adLinkCd) {
		this.adLinkCd = adLinkCd;
	}

	public String getLeadVer() {
		return leadVer;
	}

	public void setLeadVer(String leadVer) {
		this.leadVer = leadVer;
	}

	public int getRegionNo() {
		return regionNo;
	}

	public void setRegionNo(int regionNo) {
		this.regionNo = regionNo;
	}

	public String getAcqInd() {
		return acqInd;
	}

	public void setAcqInd(String acqInd) {
		this.acqInd = acqInd;
	}

	public int getItemNum() {
		return itemNum;
	}

	public void setItemNum(int itemNum) {
		this.itemNum = itemNum;
	}

	public int getVendSbsy() {
		return vendSbsy;
	}

	public void setVendSbsy(int vendSbsy) {
		this.vendSbsy = vendSbsy;
	}

	public int getModelItemNum() {
		return modelItemNum;
	}

	public void setModelItemNum(int modelItemNum) {
		this.modelItemNum = modelItemNum;
	}

	public String getMdlSubcls() {
		return mdlSubcls;
	}

	public void setMdlSubcls(String mdlSubcls) {
		this.mdlSubcls = mdlSubcls;
	}

	public String getCurrentitemNo() {
		return currentitemNo;
	}

	public void setCurrentitemNo(String currentitemNo) {
		this.currentitemNo = currentitemNo;
	}

	public int getInvUnits() {
		return invUnits;
	}

	public void setInvUnits(int invUnits) {
		this.invUnits = invUnits;
	}

	public char getDspnInd() {
		return dspnInd;
	}

	public void setDspnInd(char dspnInd) {
		this.dspnInd = dspnInd;
	}

	public double getTotSalesDlr() {
		return totSalesDlr;
	}

	public void setTotSalesDlr(double totSalesDlr) {
		this.totSalesDlr = totSalesDlr;
	}

	public double getTotSalesUnit() {
		return totSalesUnit;
	}

	public void setTotSalesUnit(double totSalesUnit) {
		this.totSalesUnit = totSalesUnit;
	}

	public double getCouponAmt() {
		return couponAmt;
	}

	public void setCouponAmt(double couponAmt) {
		this.couponAmt = couponAmt;
	}

	public double getRebateAmt() {
		return rebateAmt;
	}

	public void setRebateAmt(double rebateAmt) {
		this.rebateAmt = rebateAmt;
	}

	public char getNewImgInd() {
		return newImgInd;
	}

	public void setNewImgInd(char newImgInd) {
		this.newImgInd = newImgInd;
	}

	public double getWkAvgSale() {
		return wkAvgSale;
	}

	public void setWkAvgSale(double wkAvgSale) {
		this.wkAvgSale = wkAvgSale;
	}

	public char getMdAprIind() {
		return mdAprIind;
	}

	public void setMdAprIind(char mdAprIind) {
		this.mdAprIind = mdAprIind;
	}

	public int getVersId() {
		return versId;
	}

	public void setVersId(int versId) {
		this.versId = versId;
	}

	public String getAdPageNo() {
		return adPageNo;
	}

	public void setAdPageNo(String adPageNo) {
		this.adPageNo = adPageNo;
	}

	public String getBlockNo() {
		return blockNo;
	}

	public void setBlockNo(String blockNo) {
		this.blockNo = blockNo;
	}

	public String getBlockDesc() {
		return blockDesc;
	}

	public void setBlockDesc(String blockDesc) {
		this.blockDesc = blockDesc;
	}

	public double getxRatio() {
		return xRatio;
	}

	public void setxRatio(double xRatio) {
		this.xRatio = xRatio;
	}

	public double getyRatio() {
		return yRatio;
	}

	public void setyRatio(double yRatio) {
		this.yRatio = yRatio;
	}

	public double getHghtRatio() {
		return hghtRatio;
	}

	public void setHghtRatio(double hghtRatio) {
		this.hghtRatio = hghtRatio;
	}

	public double getLenRatio() {
		return lenRatio;
	}

	public void setLenRatio(double lenRatio) {
		this.lenRatio = lenRatio;
	}

	public String getCrtoprId() {
		return crtoprId;
	}

	public void setCrtoprId(String crtoprId) {
		this.crtoprId = crtoprId;
	}

	public String getCrtTsmp() {
		return crtTsmp;
	}

	public void setCrtTsmp(String crtTsmp) {
		this.crtTsmp = crtTsmp;
	}

	public String getLstmntTsmp() {
		return lstmntTsmp;
	}

	public void setLstmntTsmp(String lstmntTsmp) {
		this.lstmntTsmp = lstmntTsmp;
	}

	public String getLstmnTrn() {
		return lstmnTrn;
	}

	public void setLstmnTrn(String lstmnTrn) {
		this.lstmnTrn = lstmnTrn;
	}

	public String getLstoprId() {
		return lstoprId;
	}

	public void setLstoprId(String lstoprId) {
		this.lstoprId = lstoprId;
	}

	public char getPrmryInd() {
		return prmryInd;
	}

	public void setPrmryInd(char prmryInd) {
		this.prmryInd = prmryInd;
	}

	public String getBlkPrmoType() {
		return blkPrmoType;
	}

	public void setBlkPrmoType(String blkPrmoType) {
		this.blkPrmoType = blkPrmoType;
	}

	public String getBlockPrty() {
		return blockPrty;
	}

	public void setBlockPrty(String blockPrty) {
		this.blockPrty = blockPrty;
	}

	public char getOvrdInd() {
		return ovrdInd;
	}

	public void setOvrdInd(char ovrdInd) {
		this.ovrdInd = ovrdInd;
	}

	public String getPrjRegn() {
		return prjRegn;
	}

	public void setPrjRegn(String prjRegn) {
		this.prjRegn = prjRegn;
	}

	public String getProjYearwKSrt() {
		return projYearwKSrt;
	}

	public void setProjYearwKSrt(String projYearwKSrt) {
		this.projYearwKSrt = projYearwKSrt;
	}

	public String getProjYearwKEnd() {
		return projYearwKEnd;
	}

	public void setProjYearwKEnd(String projYearwKEnd) {
		this.projYearwKEnd = projYearwKEnd;
	}

	public String getEvtGrpType() {
		return evtGrpType;
	}

	public void setEvtGrpType(String evtGrpType) {
		this.evtGrpType = evtGrpType;
	}

	public String getCreator() {
		return creator;
	}

	public void setCreator(String creator) {
		this.creator = creator;
	}

	public String getLastmainTime() {
		return lastmainTime;
	}

	public void setLastmainTime(String lastmainTime) {
		this.lastmainTime = lastmainTime;
	}

	public String getLastmainOpt() {
		return lastmainOpt;
	}

	public void setLastmainOpt(String lastmainOpt) {
		this.lastmainOpt = lastmainOpt;
	}

	public String getMblockPrty() {
		return mblockPrty;
	}

	public void setMblockPrty(String mblockPrty) {
		this.mblockPrty = mblockPrty;
	}

	public char getAprvlInd() {
		return aprvlInd;
	}

	public void setAprvlInd(char aprvlInd) {
		this.aprvlInd = aprvlInd;
	}

	public String getVersPrcCode() {
		return versPrcCode;
	}

	public void setVersPrcCode(String versPrcCode) {
		this.versPrcCode = versPrcCode;
	}

	public double getPrice() {
		return price;
	}

	public void setPrice(double price) {
		this.price = price;
	}

	public int getPricMultiple() {
		return pricMultiple;
	}

	public void setPricMultiple(int pricMultiple) {
		this.pricMultiple = pricMultiple;
	}

	public double getScanBkAlw() {
		return scanBkAlw;
	}

	public void setScanBkAlw(double scanBkAlw) {
		this.scanBkAlw = scanBkAlw;
	}

	public double getScanBkPct() {
		return scanBkPct;
	}

	public void setScanBkPct(double scanBkPct) {
		this.scanBkPct = scanBkPct;
	}

	public double getAvgStrSales() {
		return avgStrSales;
	}

	public void setAvgStrSales(double avgStrSales) {
		this.avgStrSales = avgStrSales;
	}

	public char getEvtInclInd() {
		return evtInclInd;
	}

	public void setEvtInclInd(char evtInclInd) {
		this.evtInclInd = evtInclInd;
	}

	public String getCreatedTime() {
		return createdTime;
	}

	public void setCreatedTime(String createdTime) {
		this.createdTime = createdTime;
	}

	public double getPrjSalUnit() {
		return prjSalUnit;
	}

	public void setPrjSalUnit(double prjSalUnit) {
		this.prjSalUnit = prjSalUnit;
	}

	public double getPrjSalDlr() {
		return prjSalDlr;
	}

	public void setPrjSalDlr(double prjSalDlr) {
		this.prjSalDlr = prjSalDlr;
	}

	public double getPrjDmDlr() {
		return prjDmDlr;
	}

	public void setPrjDmDlr(double prjDmDlr) {
		this.prjDmDlr = prjDmDlr;
	}

	public double getPrjMkdnDlr() {
		return prjMkdnDlr;
	}

	public void setPrjMkdnDlr(double prjMkdnDlr) {
		this.prjMkdnDlr = prjMkdnDlr;
	}

	public double getPrjExtCost() {
		return prjExtCost;
	}

	public void setPrjExtCost(double prjExtCost) {
		this.prjExtCost = prjExtCost;
	}

	public double getPrjItmCnt() {
		return prjItmCnt;
	}

	public void setPrjItmCnt(double prjItmCnt) {
		this.prjItmCnt = prjItmCnt;
	}

	public String getDstnMeth() {
		return dstnMeth;
	}

	public void setDstnMeth(String dstnMeth) {
		this.dstnMeth = dstnMeth;
	}

	public double getOiAlwAmt() {
		return oiAlwAmt;
	}

	public void setOiAlwAmt(double oiAlwAmt) {
		this.oiAlwAmt = oiAlwAmt;
	}

	public double getBbAlwAmt() {
		return bbAlwAmt;
	}

	public void setBbAlwAmt(double bbAlwAmt) {
		this.bbAlwAmt = bbAlwAmt;
	}

	public char getAdPictInd() {
		return adPictInd;
	}

	public void setAdPictInd(char adPictInd) {
		this.adPictInd = adPictInd;
	}

	public String getCntrcId() {
		return cntrcId;
	}

	public void setCntrcId(String cntrcId) {
		this.cntrcId = cntrcId;
	}

	public double getCoopDlrAmt() {
		return coopDlrAmt;
	}

	public void setCoopDlrAmt(double coopDlrAmt) {
		this.coopDlrAmt = coopDlrAmt;
	}

	public String getMixMtchCode() {
		return mixMtchCode;
	}

	public void setMixMtchCode(String mixMtchCode) {
		this.mixMtchCode = mixMtchCode;
	}

	public double getMrebateAmt() {
		return mrebateAmt;
	}

	public void setMrebateAmt(double mrebateAmt) {
		this.mrebateAmt = mrebateAmt;
	}

	public double getMcouponAmt() {
		return mcouponAmt;
	}

	public void setMcouponAmt(double mcouponAmt) {
		this.mcouponAmt = mcouponAmt;
	}

	public String getAdType() {
		return adType;
	}

	public void setAdType(String adType) {
		this.adType = adType;
	}

	public String getPromoNo() {
		return promoNo;
	}

	public void setPromoNo(String promoNo) {
		this.promoNo = promoNo;
	}

	public char getCrdReqInd() {
		return crdReqInd;
	}

	public void setCrdReqInd(char crdReqInd) {
		this.crdReqInd = crdReqInd;
	}

	public char getGrpTypeInd() {
		return grpTypeInd;
	}

	public void setGrpTypeInd(char grpTypeInd) {
		this.grpTypeInd = grpTypeInd;
	}

	public char getCrdReqInd2() {
		return crdReqInd2;
	}

	public void setCrdReqInd2(char crdReqInd2) {
		this.crdReqInd2 = crdReqInd2;
	}

	public String getItemNo() {
		return itemNo;
	}

	public void setItemNo(String itemNo) {
		this.itemNo = itemNo;
	}

	public double getVerPrcUom() {
		return verPrcUom;
	}

	public void setVerPrcUom(double verPrcUom) {
		this.verPrcUom = verPrcUom;
	}

	public int getPriceMultiple() {
		return priceMultiple;
	}

	public void setPriceMultiple(int priceMultiple) {
		this.priceMultiple = priceMultiple;
	}

	public String getPriceCode() {
		return priceCode;
	}

	public void setPriceCode(String priceCode) {
		this.priceCode = priceCode;
	}

	public String getPriceChgInd() {
		return priceChgInd;
	}

	public void setPriceChgInd(String priceChgInd) {
		this.priceChgInd = priceChgInd;
	}

	public double getAvgStrSale() {
		return avgStrSale;
	}

	public void setAvgStrSale(double avgStrSale) {
		this.avgStrSale = avgStrSale;
	}

	public String getPdstnMeth() {
		return pdstnMeth;
	}

	public void setPdstnMeth(String pdstnMeth) {
		this.pdstnMeth = pdstnMeth;
	}

	public double getPscanBkAlw() {
		return pscanBkAlw;
	}

	public void setPscanBkAlw(double pscanBkAlw) {
		this.pscanBkAlw = pscanBkAlw;
	}

	public String getItmRefNo() {
		return itmRefNo;
	}

	public void setItmRefNo(String itmRefNo) {
		this.itmRefNo = itmRefNo;
	}

	public String getAdPageId() {
		return adPageId;
	}

	public void setAdPageId(String adPageId) {
		this.adPageId = adPageId;
	}

	public char getAdPctrInd() {
		return adPctrInd;
	}

	public void setAdPctrInd(char adPctrInd) {
		this.adPctrInd = adPctrInd;
	}

	public double getWgtngFctr() {
		return wgtngFctr;
	}

	public void setWgtngFctr(double wgtngFctr) {
		this.wgtngFctr = wgtngFctr;
	}

	public double getInvoiceAlwAmt() {
		return InvoiceAlwAmt;
	}

	public void setInvoiceAlwAmt(double invoiceAlwAmt) {
		InvoiceAlwAmt = invoiceAlwAmt;
	}

	public double getPbbAlwAmt() {
		return pbbAlwAmt;
	}

	public void setPbbAlwAmt(double pbbAlwAmt) {
		this.pbbAlwAmt = pbbAlwAmt;
	}

	public double getTotProjSales() {
		return totProjSales;
	}

	public void setTotProjSales(double totProjSales) {
		this.totProjSales = totProjSales;
	}

	public String getPmixMtchCode() {
		return pmixMtchCode;
	}

	public void setPmixMtchCode(String pmixMtchCode) {
		this.pmixMtchCode = pmixMtchCode;
	}

	public int getCntrId() {
		return cntrId;
	}

	public void setCntrId(int cntrId) {
		this.cntrId = cntrId;
	}

	public char getRvwCode() {
		return rvwCode;
	}

	public void setRvwCode(char rvwCode) {
		this.rvwCode = rvwCode;
	}

	public String getProjMeth() {
		return projMeth;
	}

	public void setProjMeth(String projMeth) {
		this.projMeth = projMeth;
	}

	public String getItmGrpCode() {
		return itmGrpCode;
	}

	public void setItmGrpCode(String itmGrpCode) {
		this.itmGrpCode = itmGrpCode;
	}

	public double getActualSalesDlr() {
		return actualSalesDlr;
	}

	public void setActualSalesDlr(double actualSalesDlr) {
		this.actualSalesDlr = actualSalesDlr;
	}

	public double getActualSalesUnits() {
		return actualSalesUnits;
	}

	public void setActualSalesUnits(double actualSalesUnits) {
		this.actualSalesUnits = actualSalesUnits;
	}

	public double getPrjItemSalesDlr() {
		return prjItemSalesDlr;
	}

	public void setPrjItemSalesDlr(double prjItemSalesDlr) {
		this.prjItemSalesDlr = prjItemSalesDlr;
	}

	public double getActItemmdDlr() {
		return actItemmdDlr;
	}

	public void setActItemmdDlr(double actItemmdDlr) {
		this.actItemmdDlr = actItemmdDlr;
	}

	public double getPrjItemmdDlr() {
		return prjItemmdDlr;
	}

	public void setPrjItemmdDlr(double prjItemmdDlr) {
		this.prjItemmdDlr = prjItemmdDlr;
	}

	public double getPrjItemGmDlr() {
		return prjItemGmDlr;
	}

	public void setPrjItemGmDlr(double prjItemGmDlr) {
		this.prjItemGmDlr = prjItemGmDlr;
	}

	public double getActItemGmDlr() {
		return actItemGmDlr;
	}

	public void setActItemGmDlr(double actItemGmDlr) {
		this.actItemGmDlr = actItemGmDlr;
	}

	public String getPvtItlbInd() {
		return pvtItlbInd;
	}

	public void setPvtItlbInd(String pvtItlbInd) {
		this.pvtItlbInd = pvtItlbInd;
	}

	public char getDsdInd() {
		return dsdInd;
	}

	public void setDsdInd(char dsdInd) {
		this.dsdInd = dsdInd;
	}

	public double getActItemwtAmt() {
		return actItemwtAmt;
	}

	public void setActItemwtAmt(double actItemwtAmt) {
		this.actItemwtAmt = actItemwtAmt;
	}

	public int getOnhandQty() {
		return onhandQty;
	}

	public void setOnhandQty(int onhandQty) {
		this.onhandQty = onhandQty;
	}

	public double getItemAvgCost() {
		return itemAvgCost;
	}

	public void setItemAvgCost(double itemAvgCost) {
		this.itemAvgCost = itemAvgCost;
	}

	public char getBltcrtInd() {
		return bltcrtInd;
	}

	public void setBltcrtInd(char bltcrtInd) {
		this.bltcrtInd = bltcrtInd;
	}

	public String getMustBuyInd() {
		return mustBuyInd;
	}

	public void setMustBuyInd(String mustBuyInd) {
		this.mustBuyInd = mustBuyInd;
	}

	public double getAvgPromoUnit() {
		return avgPromoUnit;
	}

	public void setAvgPromoUnit(double avgPromoUnit) {
		this.avgPromoUnit = avgPromoUnit;
	}

	public double getAvgPromoRtl() {
		return avgPromoRtl;
	}

	public void setAvgPromoRtl(double avgPromoRtl) {
		this.avgPromoRtl = avgPromoRtl;
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

	public double getPrice2() {
		return price2;
	}

	public void setPrice2(double price2) {
		this.price2 = price2;
	}

	public int getPriceMult2() {
		return priceMult2;
	}

	public void setPriceMult2(int priceMult2) {
		this.priceMult2 = priceMult2;
	}

	public String getPriceCode2() {
		return priceCode2;
	}

	public void setPriceCode2(String priceCode2) {
		this.priceCode2 = priceCode2;
	}

	public String getLocationNo() {
		return locationNo;
	}

	public void setLocationNo(String locationNo) {
		this.locationNo = locationNo;
	}

	public int getsVersId() {
		return sVersId;
	}

	public void setsVersId(int sVersId) {
		this.sVersId = sVersId;
	}

	public String getModelStrNo() {
		return modelStrNo;
	}

	public void setModelStrNo(String modelStrNo) {
		this.modelStrNo = modelStrNo;
	}

	public String getProjStrInd() {
		return projStrInd;
	}

	public void setProjStrInd(String projStrInd) {
		this.projStrInd = projStrInd;
	}

	public char getExcludeRepInd() {
		return excludeRepInd;
	}

	public void setExcludeRepInd(char excludeRepInd) {
		this.excludeRepInd = excludeRepInd;
	}

	public String getStrBeginDate() {
		return strBeginDate;
	}

	public void setStrBeginDate(String strBeginDate) {
		this.strBeginDate = strBeginDate;
	}

	public String getStrEndDate() {
		return strEndDate;
	}

	public void setStrEndDate(String strEndDate) {
		this.strEndDate = strEndDate;
	}

	public String getPoMeth() {
		return poMeth;
	}

	public void setPoMeth(String poMeth) {
		this.poMeth = poMeth;
	}

	@Override
	public Object clone() throws CloneNotSupportedException {
		RAPromoDTO cloned = (RAPromoDTO) super.clone();
		return cloned;
	}

	public String getZoneNo() {
		return zoneNo;
	}

	public void setZoneNo(String zoneNo) {
		this.zoneNo = zoneNo;
	}

}
