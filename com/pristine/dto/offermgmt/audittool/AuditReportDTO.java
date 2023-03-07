package com.pristine.dto.offermgmt.audittool;

public class AuditReportDTO {
	private long reportDetailId;
	private long reportId;
	private double totalRevenue;
	private double marginRate;
	private double marginDollar;
	private double priceIndex;
	private double movementQty;
	private double movementVol;
	private double projectedTotalRevenue;
	private double projectedMarginRate;
	private double projectedMarginDollar;
	private double projectedPriceIndex;
	private double projectedMovementQty;
	private double projectedMovementVol;
	private int retailChanges;
	private int retailIncreased;
	private int retailDecreased;
	private int retailOverridden;
	private int retailMarked;
	private int retailApproved;
	private int lowerSizeHigherPrice;
	private int lowerHigherPriceVariation;
	private int similarSizeVariation;
	private int retailBelowListCost;
	private int retailBelowListCostNoVIP;
	private int RetailBelowVIPCost;
	private int marginLTPCT;
	private int marginGTPCT;
	private int marginViolation;
	private int brandViolation;
	private int sizeViolation;
	private int retailsLTPCTOfPrimaryComp;
	private int retailsGTPCTOfPrimaryComp;
	private int retailsPrimaryCompViolation;
	private int marginLTPCTAllComp;
	private int marginGTPCTAllComp;
	private int retailChangedLTPCT;
	private int retailChangedGTPCT;
	private int retailChangedViolation;
	private int listCostChangedGTPCT;
	private int compPricePrimaryChangedGTPCT;
	private int retailChangedNoCostOrNoComp;
	private int retailChangedRoundingViol;
	private int retailLirLinePriceViol;
	private int retailChangedNtimes;
	private int retailNotChangedKVIandSK;
	private int reducedRetailsToIncreaseMarginOpp;
	private int KVICompPriceOld;
	private int KVIWithNoCompPriceCount;
	private double KVIWithCompPricePCT;
	private int totalItems;
	private int totalLig;
	private int outOfNorm;
	private int outOfNormLig;
	private String userNotes;
	private int retailChangesUnq;
	private int retailIncreasedUnq;
	private int retailDecreasedUnq;
	private int retailOverriddenUnq;
	private int retailMarkedUnq;
	private int retailApprovedUnq;
	private int lowerSizeHigherPriceUnq;
	private int lowerHigherPriceVariationUnq;
	private int similarSizeVariationUnq;
	private int retailBelowListCostUnq;
	private int retailBelowListCostNoVIPUnq;
	private int RetailBelowVIPCostUnq;
	private int marginLTPCTUnq;
	private int marginGTPCTUnq;
	private int marginViolationUnq;
	private int brandViolationUnq;
	private int sizeViolationUnq;
	private int retailsLTPCTOfPrimaryCompUnq;
	private int retailsGTPCTOfPrimaryCompUnq;
	private int retailsPrimaryCompViolationUnq;
	private int marginLTPCTAllCompUnq;
	private int marginGTPCTAllCompUnq;
	private int retailChangedLTPCTUnq;
	private int retailChangedGTPCTUnq;
	private int retailChangedViolationUnq;
	private int listCostChangedGTPCTUnq;
	private int compPricePrimaryChangedGTPCTUnq;
	private int retailChangedNoCostOrNoCompUnq;
	private int retailChangedRoundingViolUnq;
	private int retailLirLinePriceViolUnq;
	private int retailChangedNtimesUnq;
	private int retailNotChangedKVIandSKUnq;
	private int reducedRetailsToIncreaseMarginOppUnq;
	private int KVICompPriceOldUnq;
	private int KVIWithCompPriceCountUnq;
	private double KVIWithCompPricePCTUnq;
	private int zeroCurrRetails;
	private int zeroCost;
	private int zeroCurrRetailsUnq;
	private int zeroCostUnq;
	private double baseRetAUR;
	private double baseRecRetAUR;
	private double compStr1AUR;
	private double compStr2AUR;
	private double compStr3AUR;
	private double compStr4AUR;
	private double compStr5AUR;
	private String compStrID1;
	private String compStrID2;
	private String compStrID3;
	private String compStrID4;
	private String compStrID5;
	private long runID;
	private double movementQtyILP;
	private double projectedMovQtyILP;
	private double projectedTotalRevenueILP;
	private double projectedMarginRateILP;
	private double projectedMarginDollarILP;
	private double totalRevenueILP;
	private double marginRateILP;
	private double marginDollarILP;
	private int retgreaterComp1;
	private int retlessComp1;
	private int retequalComp1;
	private int retgreaterComp2;
	private int retlessComp2;
	private int retequalComp2;
	private int retgreaterComp3;
	private int retlessComp3;
	private int retequalComp3;
	private int retgreaterComp4;
	private int retlessComp4;
	private int retequalComp4;
	private int retgreaterComp5;
	private int retlessComp5;
	private int retequalComp5;
	private double compStr6AUR;
	private String compStrID6;
	private int retgreaterComp6;
	private int retlessComp6;
	private int retequalComp6;
	
	private int markUP;
	private int markDown;
	private double baseReccVsCompAUR1 = 0;
	private double baseReccVsCompAUR2 = 0;
	private double baseReccVsCompAUR3 = 0;
	private double baseReccVsCompAUR4 = 0;
	private double baseReccVsCompAUR5 = 0;
	private double baseReccVsCompAUR6 = 0;
	private double baseCurrRetailVsCompAUR1 = 0;
	private double baseCurrRetailVsCompAUR2 = 0;
	private double baseCurrRetailVsCompAUR3 = 0;
	private double baseCurrRetailVsCompAUR4 = 0;
	private double baseCurrRetailVsCompAUR5 = 0;
	private double baseCurrRetailVsCompAUR6 = 0;
	private int curRetgreaterComp1;
	private int curRetlessComp1;
	private int curRetequalComp1;
	private int curRetgreaterComp2;
	private int curRetlessComp2;
	private int curRetequalComp2;
	private int curRetgreaterComp3;
	private int curRetlessComp3;
	private int curRetequalComp3;
	private int curRetgreaterComp4;
	private int curRetlessComp4;
	private int curRetequalComp4;
	private int curRetgreaterComp5;
	private int curRetlessComp5;
	private int curRetequalComp5;
	private int curRetgreaterComp6;
	private int curRetlessComp6;
	private int curRetequalComp6;
	
	public int getZeroCurrRetails() {
		return zeroCurrRetails;
	}
	public void setZeroCurrRetails(int zeroCurrRetails) {
		this.zeroCurrRetails = zeroCurrRetails;
	}
	public int getZeroCost() {
		return zeroCost;
	}
	public void setZeroCost(int zeroCost) {
		this.zeroCost = zeroCost;
	}
	public int getZeroCurrRetailsUnq() {
		return zeroCurrRetailsUnq;
	}
	public void setZeroCurrRetailsUnq(int zeroCurrRetailsUnq) {
		this.zeroCurrRetailsUnq = zeroCurrRetailsUnq;
	}
	public int getZeroCostUnq() {
		return zeroCostUnq;
	}
	public void setZeroCostUnq(int zeroCostUnq) {
		this.zeroCostUnq = zeroCostUnq;
	}
	public int getRetailChangesUnq() {
		return retailChangesUnq;
	}
	public void setRetailChangesUnq(int retailChangesUnq) {
		this.retailChangesUnq = retailChangesUnq;
	}
	public int getRetailIncreasedUnq() {
		return retailIncreasedUnq;
	}
	public void setRetailIncreasedUnq(int retailIncreasedUnq) {
		this.retailIncreasedUnq = retailIncreasedUnq;
	}
	public int getRetailDecreasedUnq() {
		return retailDecreasedUnq;
	}
	public void setRetailDecreasedUnq(int retailDecreasedUnq) {
		this.retailDecreasedUnq = retailDecreasedUnq;
	}
	public int getRetailOverriddenUnq() {
		return retailOverriddenUnq;
	}
	public void setRetailOverriddenUnq(int retailOverriddenUnq) {
		this.retailOverriddenUnq = retailOverriddenUnq;
	}
	public int getRetailMarkedUnq() {
		return retailMarkedUnq;
	}
	public void setRetailMarkedUnq(int retailMarkedUnq) {
		this.retailMarkedUnq = retailMarkedUnq;
	}
	public int getRetailApprovedUnq() {
		return retailApprovedUnq;
	}
	public void setRetailApprovedUnq(int retailApprovedUnq) {
		this.retailApprovedUnq = retailApprovedUnq;
	}
	public int getLowerSizeHigherPriceUnq() {
		return lowerSizeHigherPriceUnq;
	}
	public void setLowerSizeHigherPriceUnq(int lowerSizeHigherPriceUnq) {
		this.lowerSizeHigherPriceUnq = lowerSizeHigherPriceUnq;
	}
	public int getLowerHigherPriceVariationUnq() {
		return lowerHigherPriceVariationUnq;
	}
	public void setLowerHigherPriceVariationUnq(int lowerHigherPriceVariationUnq) {
		this.lowerHigherPriceVariationUnq = lowerHigherPriceVariationUnq;
	}
	public int getSimilarSizeVariationUnq() {
		return similarSizeVariationUnq;
	}
	public void setSimilarSizeVariationUnq(int similarSizeVariationUnq) {
		this.similarSizeVariationUnq = similarSizeVariationUnq;
	}
	public int getRetailBelowListCostUnq() {
		return retailBelowListCostUnq;
	}
	public void setRetailBelowListCostUnq(int retailBelowListCostUnq) {
		this.retailBelowListCostUnq = retailBelowListCostUnq;
	}
	public int getRetailBelowListCostNoVIPUnq() {
		return retailBelowListCostNoVIPUnq;
	}
	public void setRetailBelowListCostNoVIPUnq(int retailBelowListCostNoVIPUnq) {
		this.retailBelowListCostNoVIPUnq = retailBelowListCostNoVIPUnq;
	}
	public int getRetailBelowVIPCostUnq() {
		return RetailBelowVIPCostUnq;
	}
	public void setRetailBelowVIPCostUnq(int retailBelowVIPCostUnq) {
		RetailBelowVIPCostUnq = retailBelowVIPCostUnq;
	}
	public int getMarginLTPCTUnq() {
		return marginLTPCTUnq;
	}
	public void setMarginLTPCTUnq(int marginLTPCTUnq) {
		this.marginLTPCTUnq = marginLTPCTUnq;
	}
	public int getMarginGTPCTUnq() {
		return marginGTPCTUnq;
	}
	public void setMarginGTPCTUnq(int marginGTPCTUnq) {
		this.marginGTPCTUnq = marginGTPCTUnq;
	}
	public int getRetailsLTPCTOfPrimaryCompUnq() {
		return retailsLTPCTOfPrimaryCompUnq;
	}
	public void setRetailsLTPCTOfPrimaryCompUnq(int retailsLTPCTOfPrimaryCompUnq) {
		this.retailsLTPCTOfPrimaryCompUnq = retailsLTPCTOfPrimaryCompUnq;
	}
	public int getRetailsGTPCTOfPrimaryCompUnq() {
		return retailsGTPCTOfPrimaryCompUnq;
	}
	public void setRetailsGTPCTOfPrimaryCompUnq(int retailsGTPCTOfPrimaryCompUnq) {
		this.retailsGTPCTOfPrimaryCompUnq = retailsGTPCTOfPrimaryCompUnq;
	}
	public int getMarginLTPCTAllCompUnq() {
		return marginLTPCTAllCompUnq;
	}
	public void setMarginLTPCTAllCompUnq(int marginLTPCTAllCompUnq) {
		this.marginLTPCTAllCompUnq = marginLTPCTAllCompUnq;
	}
	public int getMarginGTPCTAllCompUnq() {
		return marginGTPCTAllCompUnq;
	}
	public void setMarginGTPCTAllCompUnq(int marginGTPCTAllCompUnq) {
		this.marginGTPCTAllCompUnq = marginGTPCTAllCompUnq;
	}
	public int getRetailChangedLTPCTUnq() {
		return retailChangedLTPCTUnq;
	}
	public void setRetailChangedLTPCTUnq(int retailChangedLTPCTUnq) {
		this.retailChangedLTPCTUnq = retailChangedLTPCTUnq;
	}
	public int getRetailChangedGTPCTUnq() {
		return retailChangedGTPCTUnq;
	}
	public void setRetailChangedGTPCTUnq(int retailChangedGTPCTUnq) {
		this.retailChangedGTPCTUnq = retailChangedGTPCTUnq;
	}
	public int getListCostChangedGTPCTUnq() {
		return listCostChangedGTPCTUnq;
	}
	public void setListCostChangedGTPCTUnq(int listCostChangedGTPCTUnq) {
		this.listCostChangedGTPCTUnq = listCostChangedGTPCTUnq;
	}
	public int getCompPricePrimaryChangedGTPCTUnq() {
		return compPricePrimaryChangedGTPCTUnq;
	}
	public void setCompPricePrimaryChangedGTPCTUnq(int compPricePrimaryChangedGTPCTUnq) {
		this.compPricePrimaryChangedGTPCTUnq = compPricePrimaryChangedGTPCTUnq;
	}
	public int getRetailChangedNoCostOrNoCompUnq() {
		return retailChangedNoCostOrNoCompUnq;
	}
	public void setRetailChangedNoCostOrNoCompUnq(int retailChangedNoCostOrNoCompUnq) {
		this.retailChangedNoCostOrNoCompUnq = retailChangedNoCostOrNoCompUnq;
	}
	public int getRetailChangedRoundingViolUnq() {
		return retailChangedRoundingViolUnq;
	}
	public void setRetailChangedRoundingViolUnq(int retailChangedRoundingViolUnq) {
		this.retailChangedRoundingViolUnq = retailChangedRoundingViolUnq;
	}
	public int getRetailLirLinePriceViolUnq() {
		return retailLirLinePriceViolUnq;
	}
	public void setRetailLirLinePriceViolUnq(int retailLirLinePriceViolUnq) {
		this.retailLirLinePriceViolUnq = retailLirLinePriceViolUnq;
	}
	public int getRetailChangedNtimesUnq() {
		return retailChangedNtimesUnq;
	}
	public void setRetailChangedNtimesUnq(int retailChangedNtimesUnq) {
		this.retailChangedNtimesUnq = retailChangedNtimesUnq;
	}
	public int getRetailNotChangedKVIandSKUnq() {
		return retailNotChangedKVIandSKUnq;
	}
	public void setRetailNotChangedKVIandSKUnq(int retailNotChangedKVIandSKUnq) {
		this.retailNotChangedKVIandSKUnq = retailNotChangedKVIandSKUnq;
	}
	public int getReducedRetailsToIncreaseMarginOppUnq() {
		return reducedRetailsToIncreaseMarginOppUnq;
	}
	public void setReducedRetailsToIncreaseMarginOppUnq(int reducedRetailsToIncreaseMarginOppUnq) {
		this.reducedRetailsToIncreaseMarginOppUnq = reducedRetailsToIncreaseMarginOppUnq;
	}
	public int getKVICompPriceOldUnq() {
		return KVICompPriceOldUnq;
	}
	public void setKVICompPriceOldUnq(int kVICompPriceOldUnq) {
		KVICompPriceOldUnq = kVICompPriceOldUnq;
	}
	public int getKVIWithCompPriceCountUnq() {
		return KVIWithCompPriceCountUnq;
	}
	public void setKVIWithNoCompPriceCountUnq(int kVIWithCompPriceCountUnq) {
		KVIWithCompPriceCountUnq = kVIWithCompPriceCountUnq;
	}
	public double getKVIWithCompPricePCTUnq() {
		return KVIWithCompPricePCTUnq;
	}
	public void setKVIWithCompPricePCTUnq(double kVIWithCompPricePCTUnq) {
		KVIWithCompPricePCTUnq = kVIWithCompPricePCTUnq;
	}
	public int getOutOfNormLig() {
		return outOfNormLig;
	}
	public void setOutOfNormLig(int outOfNormLig) {
		this.outOfNormLig = outOfNormLig;
	}
	public int getOutOfNorm() {
		return outOfNorm;
	}
	public void setOutOfNorm(int outOfNorm) {
		this.outOfNorm = outOfNorm;
	}

	public double getProjectedTotalRevenue() {
		return projectedTotalRevenue;
	}
	public void setProjectedTotalRevenue(double projectedTotalRevenue) {
		this.projectedTotalRevenue = projectedTotalRevenue;
	}
	public double getProjectedMarginRate() {
		return projectedMarginRate;
	}
	public void setProjectedMarginRate(double projectedMarginRate) {
		this.projectedMarginRate = projectedMarginRate;
	}
	public double getProjectedMarginDollar() {
		return projectedMarginDollar;
	}
	public void setProjectedMarginDollar(double projectedMarginDollar) {
		this.projectedMarginDollar = projectedMarginDollar;
	}
	public double getProjectedPriceIndex() {
		return projectedPriceIndex;
	}
	public void setProjectedPriceIndex(double projectedPriceIndex) {
		this.projectedPriceIndex = projectedPriceIndex;
	}
	public double getProjectedMovementQty() {
		return projectedMovementQty;
	}
	public void setProjectedMovementQty(double projectedMovementQty) {
		this.projectedMovementQty = projectedMovementQty;
	}
	public double getProjectedMovementVol() {
		return projectedMovementVol;
	}
	public void setProjectedMovementVol(double projectedMovementVol) {
		this.projectedMovementVol = projectedMovementVol;
	}
	public int getTotalItems() {
		return totalItems;
	}
	public void setTotalItems(int totalItems) {
		this.totalItems = totalItems;
	}
	public int getTotalLig() {
		return totalLig;
	}
	public void setTotalLig(int totalLig) {
		this.totalLig = totalLig;
	}

	public long getReportDetailId() {
		return reportDetailId;
	}
	public void setReportDetailId(long reportDetailId) {
		this.reportDetailId = reportDetailId;
	}
	public long getReportId() {
		return reportId;
	}
	public void setReportId(long reportId) {
		this.reportId = reportId;
	}
	public double getTotalRevenue() {
		return totalRevenue;
	}
	public void setTotalRevenue(double totalRevenue) {
		this.totalRevenue = totalRevenue;
	}
	public double getMarginRate() {
		return marginRate;
	}
	public void setMarginRate(double marginRate) {
		this.marginRate = marginRate;
	}
	public double getMarginDollar() {
		return marginDollar;
	}
	public void setMarginDollar(double marginDollar) {
		this.marginDollar = marginDollar;
	}
	public double getPriceIndex() {
		return priceIndex;
	}
	public void setPriceIndex(double priceIndex) {
		this.priceIndex = priceIndex;
	}
	public double getMovementQty() {
		return movementQty;
	}
	public void setMovementQty(double movementQty) {
		this.movementQty = movementQty;
	}
	public double getMovementVol() {
		return movementVol;
	}
	public void setMovementVol(double movementVol) {
		this.movementVol = movementVol;
	}
	public int getRetailChanges() {
		return retailChanges;
	}
	public void setRetailChanges(int retailChanges) {
		this.retailChanges = retailChanges;
	}
	public int getRetailIncreased() {
		return retailIncreased;
	}
	public void setRetailIncreased(int retailIncreased) {
		this.retailIncreased = retailIncreased;
	}
	public int getRetailDecreased() {
		return retailDecreased;
	}
	public void setRetailDecreased(int retailDecreased) {
		this.retailDecreased = retailDecreased;
	}
	public int getRetailOverridden() {
		return retailOverridden;
	}
	public void setRetailOverridden(int retailOverridden) {
		this.retailOverridden = retailOverridden;
	}
	public int getRetailMarked() {
		return retailMarked;
	}
	public void setRetailMarked(int retailMarked) {
		this.retailMarked = retailMarked;
	}
	public int getRetailApproved() {
		return retailApproved;
	}
	public void setRetailApproved(int retailApproved) {
		this.retailApproved = retailApproved;
	}
	public int getLowerSizeHigherPrice() {
		return lowerSizeHigherPrice;
	}
	public void setLowerSizeHigherPrice(int lowerSizeHigherPrice) {
		this.lowerSizeHigherPrice = lowerSizeHigherPrice;
	}
	public int getLowerHigherPriceVariation() {
		return lowerHigherPriceVariation;
	}
	public void setLowerHigherPriceVariation(int lowerHigherPriceVariation) {
		this.lowerHigherPriceVariation = lowerHigherPriceVariation;
	}
	public int getSimilarSizeVariation() {
		return similarSizeVariation;
	}
	public void setSimilarSizeVariation(int similarSizeVariation) {
		this.similarSizeVariation = similarSizeVariation;
	}
	public int getRetailBelowListCost() {
		return retailBelowListCost;
	}
	public void setRetailBelowListCost(int retailBelowListCost) {
		this.retailBelowListCost = retailBelowListCost;
	}
	public int getRetailBelowListCostNoVIP() {
		return retailBelowListCostNoVIP;
	}
	public void setRetailBelowListCostNoVIP(int retailBelowListCostNoVIP) {
		this.retailBelowListCostNoVIP = retailBelowListCostNoVIP;
	}
	public int getRetailBelowVIPCost() {
		return RetailBelowVIPCost;
	}
	public void setRetailBelowVIPCost(int retailBelowVIPCost) {
		RetailBelowVIPCost = retailBelowVIPCost;
	}
	public int getMarginLTPCT() {
		return marginLTPCT;
	}
	public void setMarginLTPCT(int marginLTPCT) {
		this.marginLTPCT = marginLTPCT;
	}
	public int getMarginGTPCT() {
		return marginGTPCT;
	}
	public void setMarginGTPCT(int marginGTPCT) {
		this.marginGTPCT = marginGTPCT;
	}
	public int getRetailsLTPCTOfPrimaryComp() {
		return retailsLTPCTOfPrimaryComp;
	}
	public void setRetailsLTPCTOfPrimaryComp(int retailsLTPCTOfPrimaryComp) {
		this.retailsLTPCTOfPrimaryComp = retailsLTPCTOfPrimaryComp;
	}
	public int getRetailsGTPCTOfPrimaryComp() {
		return retailsGTPCTOfPrimaryComp;
	}
	public void setRetailsGTPCTOfPrimaryComp(int retailsGTPCTOfPrimaryComp) {
		this.retailsGTPCTOfPrimaryComp = retailsGTPCTOfPrimaryComp;
	}
	public int getMarginLTPCTAllComp() {
		return marginLTPCTAllComp;
	}
	public void setMarginLTPCTAllComp(int marginLTPCTAllComp) {
		this.marginLTPCTAllComp = marginLTPCTAllComp;
	}
	public int getMarginGTPCTAllComp() {
		return marginGTPCTAllComp;
	}
	public void setMarginGTPCTAllComp(int marginGTPCTAllComp) {
		this.marginGTPCTAllComp = marginGTPCTAllComp;
	}
	public int getRetailChangedLTPCT() {
		return retailChangedLTPCT;
	}
	public void setRetailChangedLTPCT(int retailChangedLTPCT) {
		this.retailChangedLTPCT = retailChangedLTPCT;
	}
	public int getRetailChangedGTPCT() {
		return retailChangedGTPCT;
	}
	public void setRetailChangedGTPCT(int retailChangedGTPCT) {
		this.retailChangedGTPCT = retailChangedGTPCT;
	}
	public int getListCostChangedGTPCT() {
		return listCostChangedGTPCT;
	}
	public void setListCostChangedGTPCT(int listCostChangedGTPCT) {
		this.listCostChangedGTPCT = listCostChangedGTPCT;
	}
	public int getCompPricePrimaryChangedGTPCT() {
		return compPricePrimaryChangedGTPCT;
	}
	public void setCompPricePrimaryChangedGTPCT(int compPricePrimaryChangedGTPCT) {
		this.compPricePrimaryChangedGTPCT = compPricePrimaryChangedGTPCT;
	}
	public int getRetailChangedNoCostOrNoComp() {
		return retailChangedNoCostOrNoComp;
	}
	public void setRetailChangedNoCostOrNoComp(int retailChangedNoCostOrNoComp) {
		this.retailChangedNoCostOrNoComp = retailChangedNoCostOrNoComp;
	}
	public int getRetailChangedRoundingViol() {
		return retailChangedRoundingViol;
	}
	public void setRetailChangedRoundingViol(int retailChangedRoundingViol) {
		this.retailChangedRoundingViol = retailChangedRoundingViol;
	}
	public int getRetailLirLinePriceViol() {
		return retailLirLinePriceViol;
	}
	public void setRetailLirLinePriceViol(int retailLirLinePriceViol) {
		this.retailLirLinePriceViol = retailLirLinePriceViol;
	}
	public int getRetailChangedNtimes() {
		return retailChangedNtimes;
	}
	public void setRetailChangedNtimes(int retailChangedNtimes) {
		this.retailChangedNtimes = retailChangedNtimes;
	}
	public int getRetailNotChangedKVIandSK() {
		return retailNotChangedKVIandSK;
	}
	public void setRetailNotChangedKVIandSK(int retailNotChangedKVIandSK) {
		this.retailNotChangedKVIandSK = retailNotChangedKVIandSK;
	}
	public int getReducedRetailsToIncreaseMarginOpp() {
		return reducedRetailsToIncreaseMarginOpp;
	}
	public void setReducedRetailsToIncreaseMarginOpp(
			int reducedRetailsToIncreaseMarginOpp) {
		this.reducedRetailsToIncreaseMarginOpp = reducedRetailsToIncreaseMarginOpp;
	}
	public int getKVICompPriceOld() {
		return KVICompPriceOld;
	}
	public void setKVICompPriceOld(int kVICompPriceOld) {
		KVICompPriceOld = kVICompPriceOld;
	}
	public int getKVIWithCompPriceCount() {
		return KVIWithNoCompPriceCount;
	}
	public void setKVIWithNoCompPriceCount(int kVIWithNoCompPriceCount) {
		KVIWithNoCompPriceCount = kVIWithNoCompPriceCount;
	}
	public double getKVIWithCompPricePCT() {
		return KVIWithCompPricePCT;
	}
	public void setKVIWithCompPricePCT(double kVIWithCompPricePCT) {
		KVIWithCompPricePCT = kVIWithCompPricePCT;
	}
	public String getUserNotes() {
		return userNotes;
	}
	public void setUserNotes(String userNotes) {
		this.userNotes = userNotes;
	}
	public int getRetailsPrimaryCompViolation() {
		return retailsPrimaryCompViolation;
	}
	public void setRetailsPrimaryCompViolation(int retailsPrimaryCompViolation) {
		this.retailsPrimaryCompViolation = retailsPrimaryCompViolation;
	}
	public int getRetailsPrimaryCompViolationUnq() {
		return retailsPrimaryCompViolationUnq;
	}
	public void setRetailsPrimaryCompViolationUnq(int retailsPrimaryCompViolationUnq) {
		this.retailsPrimaryCompViolationUnq = retailsPrimaryCompViolationUnq;
	}
	public int getMarginViolationUnq() {
		return marginViolationUnq;
	}
	public void setMarginViolationUnq(int marginViolationUnq) {
		this.marginViolationUnq = marginViolationUnq;
	}
	public int getMarginViolation() {
		return marginViolation;
	}
	public void setMarginViolation(int marginViolation) {
		this.marginViolation = marginViolation;
	}
	public int getRetailChangedViolation() {
		return retailChangedViolation;
	}
	public void setRetailChangedViolation(int retailChangedViolation) {
		this.retailChangedViolation = retailChangedViolation;
	}
	public int getRetailChangedViolationUnq() {
		return retailChangedViolationUnq;
	}
	public void setRetailChangedViolationUnq(int retailChangedViolationUnq) {
		this.retailChangedViolationUnq = retailChangedViolationUnq;
	}
	public int getBrandViolation() {
		return brandViolation;
	}
	public void setBrandViolation(int brandViolation) {
		this.brandViolation = brandViolation;
	}
	public int getSizeViolation() {
		return sizeViolation;
	}
	public void setSizeViolation(int sizeViolation) {
		this.sizeViolation = sizeViolation;
	}
	public int getBrandViolationUnq() {
		return brandViolationUnq;
	}
	public void setBrandViolationUnq(int brandViolationUnq) {
		this.brandViolationUnq = brandViolationUnq;
	}
	public int getSizeViolationUnq() {
		return sizeViolationUnq;
	}
	public void setSizeViolationUnq(int sizeViolationUnq) {
		this.sizeViolationUnq = sizeViolationUnq;
	}
	public double getBaseRetAUR() {
		return baseRetAUR;
	}
	public void setBaseRetAUR(double baseRetAUR) {
		this.baseRetAUR = baseRetAUR;
	}
	public double getBaseRecRetAUR() {
		return baseRecRetAUR;
	}
	public void setBaseRecRetAUR(double baseRecRetAUR) {
		this.baseRecRetAUR = baseRecRetAUR;
	}
	public double getCompStr1AUR() {
		return compStr1AUR;
	}
	public void setCompStr1AUR(double compStr1AUR) {
		this.compStr1AUR = compStr1AUR;
	}
	public double getCompStr2AUR() {
		return compStr2AUR;
	}
	public void setCompStr2AUR(double compStr2AUR) {
		this.compStr2AUR = compStr2AUR;
	}
	public double getCompStr3AUR() {
		return compStr3AUR;
	}
	public void setCompStr3AUR(double compStr3AUR) {
		this.compStr3AUR = compStr3AUR;
	}
	public double getCompStr4AUR() {
		return compStr4AUR;
	}
	public void setCompStr4AUR(double compStr4AUR) {
		this.compStr4AUR = compStr4AUR;
	}
	public double getCompStr5AUR() {
		return compStr5AUR;
	}
	public void setCompStr5AUR(double compStr5AUR) {
		this.compStr5AUR = compStr5AUR;
	}
	public String getCompStrID1() {
		return compStrID1;
	}
	public void setCompStrID1(String compStrID1) {
		this.compStrID1 = compStrID1;
	}
	public String getCompStrID2() {
		return compStrID2;
	}
	public void setCompStrID2(String compStrID2) {
		this.compStrID2 = compStrID2;
	}
	public String getCompStrID3() {
		return compStrID3;
	}
	public void setCompStrID3(String compStrID3) {
		this.compStrID3 = compStrID3;
	}
	public String getCompStrID4() {
		return compStrID4;
	}
	public void setCompStrID4(String compStrID4) {
		this.compStrID4 = compStrID4;
	}
	public String getCompStrID5() {
		return compStrID5;
	}
	public void setCompStrID5(String compStrID5) {
		this.compStrID5 = compStrID5;
	}
	public long getRunID() {
		return runID;
	}
	public void setRunID(long runID) {
		this.runID = runID;
	}
	public double getMovementQtyILP() {
		return movementQtyILP;
	}
	public void setMovementQtyILP(double movementQtyILP) {
		this.movementQtyILP = movementQtyILP;
	}
	public double getProjectedMovQtyILP() {
		return projectedMovQtyILP;
	}
	public void setProjectedMovQtyILP(double projectedMovQtyILP) {
		this.projectedMovQtyILP = projectedMovQtyILP;
	}
	public double getProjectedTotalRevenueILP() {
		return projectedTotalRevenueILP;
	}
	public void setProjectedTotalRevenueILP(double projectedTotalRevenueILP) {
		this.projectedTotalRevenueILP = projectedTotalRevenueILP;
	}
	public double getProjectedMarginRateILP() {
		return projectedMarginRateILP;
	}
	public void setProjectedMarginRateILP(double projectedMarginRateILP) {
		this.projectedMarginRateILP = projectedMarginRateILP;
	}
	public double getProjectedMarginDollarILP() {
		return projectedMarginDollarILP;
	}
	public void setProjectedMarginDollarILP(double projectedMarginDollarILP) {
		this.projectedMarginDollarILP = projectedMarginDollarILP;
	}
	public double getTotalRevenueILP() {
		return totalRevenueILP;
	}
	public void setTotalRevenueILP(double totalRevenueILP) {
		this.totalRevenueILP = totalRevenueILP;
	}
	public double getMarginRateILP() {
		return marginRateILP;
	}
	public void setMarginRateILP(double marginRateILP) {
		this.marginRateILP = marginRateILP;
	}
	public double getMarginDollarILP() {
		return marginDollarILP;
	}
	public void setMarginDollarILP(double marginDollarILP) {
		this.marginDollarILP = marginDollarILP;
	}
	public int getRetgreaterComp1() {
		return retgreaterComp1;
	}
	public void setRetgreaterComp1(int retgreaterComp1) {
		this.retgreaterComp1 = retgreaterComp1;
	}
	public int getRetlessComp1() {
		return retlessComp1;
	}
	public void setRetlessComp1(int retlessComp1) {
		this.retlessComp1 = retlessComp1;
	}
	public int getRetequalComp1() {
		return retequalComp1;
	}
	public void setRetequalComp1(int retequalComp1) {
		this.retequalComp1 = retequalComp1;
	}
	public int getRetgreaterComp2() {
		return retgreaterComp2;
	}
	public void setRetgreaterComp2(int retgreaterComp2) {
		this.retgreaterComp2 = retgreaterComp2;
	}
	public int getRetlessComp2() {
		return retlessComp2;
	}
	public void setRetlessComp2(int retlessComp2) {
		this.retlessComp2 = retlessComp2;
	}
	public int getRetequalComp2() {
		return retequalComp2;
	}
	public void setRetequalComp2(int retequalComp2) {
		this.retequalComp2 = retequalComp2;
	}
	public int getRetgreaterComp3() {
		return retgreaterComp3;
	}
	public void setRetgreaterComp3(int retgreaterComp3) {
		this.retgreaterComp3 = retgreaterComp3;
	}
	public int getRetlessComp3() {
		return retlessComp3;
	}
	public void setRetlessComp3(int retlessComp3) {
		this.retlessComp3 = retlessComp3;
	}
	public int getRetequalComp3() {
		return retequalComp3;
	}
	public void setRetequalComp3(int retequalComp3) {
		this.retequalComp3 = retequalComp3;
	}
	public int getRetgreaterComp4() {
		return retgreaterComp4;
	}
	public void setRetgreaterComp4(int retgreaterComp4) {
		this.retgreaterComp4 = retgreaterComp4;
	}
	public int getRetlessComp4() {
		return retlessComp4;
	}
	public void setRetlessComp4(int retlessComp4) {
		this.retlessComp4 = retlessComp4;
	}
	public int getRetequalComp4() {
		return retequalComp4;
	}
	public void setRetequalComp4(int retequalComp4) {
		this.retequalComp4 = retequalComp4;
	}
	public int getRetgreaterComp5() {
		return retgreaterComp5;
	}
	public void setRetgreaterComp5(int retgreaterComp5) {
		this.retgreaterComp5 = retgreaterComp5;
	}
	public int getRetlessComp5() {
		return retlessComp5;
	}
	public void setRetlessComp5(int retlessComp5) {
		this.retlessComp5 = retlessComp5;
	}
	public int getRetequalComp5() {
		return retequalComp5;
	}
	public void setRetequalComp5(int retequalComp5) {
		this.retequalComp5 = retequalComp5;
	}
	public double getCompStr6AUR() {
		return compStr6AUR;
	}
	public void setCompStr6AUR(double compStr6AUR) {
		this.compStr6AUR = compStr6AUR;
	}
	public String getCompStrID6() {
		return compStrID6;
	}
	public void setCompStrID6(String compStrID6) {
		this.compStrID6 = compStrID6;
	}
	public int getRetgreaterComp6() {
		return retgreaterComp6;
	}
	public void setRetgreaterComp6(int retgreaterComp6) {
		this.retgreaterComp6 = retgreaterComp6;
	}
	public int getRetlessComp6() {
		return retlessComp6;
	}
	public void setRetlessComp6(int retlessComp6) {
		this.retlessComp6 = retlessComp6;
	}
	public int getRetequalComp6() {
		return retequalComp6;
	}
	public void setRetequalComp6(int retequalComp6) {
		this.retequalComp6 = retequalComp6;
	}
	public int getKVIWithNoCompPriceCount() {
		return KVIWithNoCompPriceCount;
	}
	public void setKVIWithCompPriceCountUnq(int kVIWithCompPriceCountUnq) {
		KVIWithCompPriceCountUnq = kVIWithCompPriceCountUnq;
	}
	public int getMarkUP() {
		return markUP;
	}
	public void setMarkUP(int markUP) {
		this.markUP = markUP;
	}
	public int getMarkDown() {
		return markDown;
	}
	public void setMarkDown(int markDown) {
		this.markDown = markDown;
	}

	public double getBaseReccVsCompAUR1() {
		return baseReccVsCompAUR1;
	}

	public void setBaseReccVsCompAUR1(double baseReccVsCompAUR1) {
		this.baseReccVsCompAUR1 = baseReccVsCompAUR1;
	}

	public double getBaseReccVsCompAUR2() {
		return baseReccVsCompAUR2;
	}

	public void setBaseReccVsCompAUR2(double baseReccVsCompAUR2) {
		this.baseReccVsCompAUR2 = baseReccVsCompAUR2;
	}

	public double getBaseReccVsCompAUR3() {
		return baseReccVsCompAUR3;
	}

	public void setBaseReccVsCompAUR3(double baseReccVsCompAUR3) {
		this.baseReccVsCompAUR3 = baseReccVsCompAUR3;
	}

	public double getBaseReccVsCompAUR4() {
		return baseReccVsCompAUR4;
	}

	public void setBaseReccVsCompAUR4(double baseReccVsCompAUR4) {
		this.baseReccVsCompAUR4 = baseReccVsCompAUR4;
	}

	public double getBaseReccVsCompAUR5() {
		return baseReccVsCompAUR5;
	}

	public void setBaseReccVsCompAUR5(double baseReccVsCompAUR5) {
		this.baseReccVsCompAUR5 = baseReccVsCompAUR5;
	}

	public double getBaseReccVsCompAUR6() {
		return baseReccVsCompAUR6;
	}

	public void setBaseReccVsCompAUR6(double baseReccVsCompAUR6) {
		this.baseReccVsCompAUR6 = baseReccVsCompAUR6;
	}

	public double getBaseCurrRetailVsCompAUR1() {
		return baseCurrRetailVsCompAUR1;
	}

	public void setBaseCurrRetailVsCompAUR1(double baseCurrRetailVsCompAUR1) {
		this.baseCurrRetailVsCompAUR1 = baseCurrRetailVsCompAUR1;
	}

	public double getBaseCurrRetailVsCompAUR2() {
		return baseCurrRetailVsCompAUR2;
	}

	public void setBaseCurrRetailVsCompAUR2(double baseCurrRetailVsCompAUR2) {
		this.baseCurrRetailVsCompAUR2 = baseCurrRetailVsCompAUR2;
	}

	public double getBaseCurrRetailVsCompAUR3() {
		return baseCurrRetailVsCompAUR3;
	}

	public void setBaseCurrRetailVsCompAUR3(double baseCurrRetailVsCompAUR3) {
		this.baseCurrRetailVsCompAUR3 = baseCurrRetailVsCompAUR3;
	}

	public double getBaseCurrRetailVsCompAUR4() {
		return baseCurrRetailVsCompAUR4;
	}

	public void setBaseCurrRetailVsCompAUR4(double baseCurrRetailVsCompAUR4) {
		this.baseCurrRetailVsCompAUR4 = baseCurrRetailVsCompAUR4;
	}

	public double getBaseCurrRetailVsCompAUR5() {
		return baseCurrRetailVsCompAUR5;
	}

	public void setBaseCurrRetailVsCompAUR5(double baseCurrRetailVsCompAUR5) {
		this.baseCurrRetailVsCompAUR5 = baseCurrRetailVsCompAUR5;
	}

	public double getBaseCurrRetailVsCompAUR6() {
		return baseCurrRetailVsCompAUR6;
	}

	public void setBaseCurrRetailVsCompAUR6(double baseCurrRetailVsCompAUR6) {
		this.baseCurrRetailVsCompAUR6 = baseCurrRetailVsCompAUR6;
	}

	public int getCurRetgreaterComp1() {
		return curRetgreaterComp1;
	}

	public void setCurRetgreaterComp1(int curRetgreaterComp1) {
		this.curRetgreaterComp1 = curRetgreaterComp1;
	}

	public int getCurRetlessComp1() {
		return curRetlessComp1;
	}

	public void setCurRetlessComp1(int curRetlessComp1) {
		this.curRetlessComp1 = curRetlessComp1;
	}

	public int getCurRetequalComp1() {
		return curRetequalComp1;
	}

	public void setCurRetequalComp1(int curRetequalComp1) {
		this.curRetequalComp1 = curRetequalComp1;
	}

	public int getCurRetgreaterComp2() {
		return curRetgreaterComp2;
	}

	public void setCurRetgreaterComp2(int curRetgreaterComp2) {
		this.curRetgreaterComp2 = curRetgreaterComp2;
	}

	public int getCurRetlessComp2() {
		return curRetlessComp2;
	}

	public void setCurRetlessComp2(int curRetlessComp2) {
		this.curRetlessComp2 = curRetlessComp2;
	}

	public int getCurRetequalComp2() {
		return curRetequalComp2;
	}

	public void setCurRetequalComp2(int curRetequalComp2) {
		this.curRetequalComp2 = curRetequalComp2;
	}

	public int getCurRetgreaterComp3() {
		return curRetgreaterComp3;
	}

	public void setCurRetgreaterComp3(int curRetgreaterComp3) {
		this.curRetgreaterComp3 = curRetgreaterComp3;
	}

	public int getCurRetlessComp3() {
		return curRetlessComp3;
	}

	public void setCurRetlessComp3(int curRetlessComp3) {
		this.curRetlessComp3 = curRetlessComp3;
	}

	public int getCurRetequalComp3() {
		return curRetequalComp3;
	}

	public void setCurRetequalComp3(int curRetequalComp3) {
		this.curRetequalComp3 = curRetequalComp3;
	}

	public int getCurRetgreaterComp4() {
		return curRetgreaterComp4;
	}

	public void setCurRetgreaterComp4(int curRetgreaterComp4) {
		this.curRetgreaterComp4 = curRetgreaterComp4;
	}

	public int getCurRetlessComp4() {
		return curRetlessComp4;
	}

	public void setCurRetlessComp4(int curRetlessComp4) {
		this.curRetlessComp4 = curRetlessComp4;
	}

	public int getCurRetequalComp4() {
		return curRetequalComp4;
	}

	public void setCurRetequalComp4(int curRetequalComp4) {
		this.curRetequalComp4 = curRetequalComp4;
	}

	public int getCurRetgreaterComp5() {
		return curRetgreaterComp5;
	}

	public void setCurRetgreaterComp5(int curRetgreaterComp5) {
		this.curRetgreaterComp5 = curRetgreaterComp5;
	}

	public int getCurRetlessComp5() {
		return curRetlessComp5;
	}

	public void setCurRetlessComp5(int curRetlessComp5) {
		this.curRetlessComp5 = curRetlessComp5;
	}

	public int getCurRetequalComp5() {
		return curRetequalComp5;
	}

	public void setCurRetequalComp5(int curRetequalComp5) {
		this.curRetequalComp5 = curRetequalComp5;
	}

	public int getCurRetgreaterComp6() {
		return curRetgreaterComp6;
	}

	public void setCurRetgreaterComp6(int curRetgreaterComp6) {
		this.curRetgreaterComp6 = curRetgreaterComp6;
	}

	public int getCurRetlessComp6() {
		return curRetlessComp6;
	}

	public void setCurRetlessComp6(int curRetlessComp6) {
		this.curRetlessComp6 = curRetlessComp6;
	}

	public int getCurRetequalComp6() {
		return curRetequalComp6;
	}

	public void setCurRetequalComp6(int curRetequalComp6) {
		this.curRetequalComp6 = curRetequalComp6;
	}

	
}
