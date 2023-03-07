package com.pristine.dto.offermgmt.mwr;

public class MWRSummaryDTO {

	
	/*RUN_ID
	UNITS_ACTUAL
	REV_ACTUAL
	MAR_ACTUAL
	MAR_PCT_ACTUAL
	INBETWEEN_WEEKS_UNITS_PRED
	INBETWEEN_WEEKS_REV_PRED
	INBETWEEN_WEEKS_MAR_PRED
	INBETWEEN_WEEKS_MAR_PCT_PRED
	REC_WEEKS_UNITS_PRED
	REC_WEEKS_REV_PRED
	REC_WEEKS_MAR_PRED
	REC_WEEKS_MAR_PCT_PRED*/

	private long runId;
	private double unitsActual;
	private double salesActual;
	private double marginActual;
	private double marginPctActual;
	private double unitsInBetWeeks;
	private double salesInBetWeeks;
	private double marginInBetWeeks;
	private double marginPctInBetWeeks;
	private double unitsPredicted;
	private double salesPredicted;
	private double marginPredicted;
	private double marginPctPredicted;
	private double unitsTotal;
	private double salesTotal;
	private double marginTotal;
	private double marginPctTotal;
	private double currentUnitsTotal;
	private double currentSalesTotal;
	private double currentMarginTotal;
	private double currentMarginPctTotal;
	private double promoUnits;
	private double promoSales;
	private double promoMargin;
	private double promoMarginPct;
	private int totalItems;
	private int totalDistinctItems;
	private int totalPromoItems;
	private int totalDistinctPromoItems;
	private int totalEDLPItems;
	private int totalDistinctEDLPItems;
	private char isongoingQt;
	private double priceIndex;
	private double priceIndexPromo;
	private double priceIndexEDLPRec;
	private double priceIndexEDLPCurr;
	private double unitsTotalClpDlp;
	private double salesTotalClpDlp;
	private double marginTotalClpDlp;
	private double marginPctTotalClpDlp;
	private double currentUnitsTotalClpDlp;
	private double currentSalesTotalClpDlp;
	private double currentMarginTotalClpDlp;
	private double currentMarginPctTotalClpDlp;
	private int totalRecommendedItems;
	private int totalRecommendedDistinctItems;
	private int totalCostChangedItems;
	private int totalCostChangedDistinctItems;
	private int totalCompChangedItems;
	private int totalCompChangedDistinctItems;
	private double priceChangeImpact;
	private double currentPriceIndex;
	
	private double markUP;
	private double markDown;
	private int retailIncreasedUnq;
	private int retailDecreasedUnq;
	private int retailIncreased;
	private int retailDecreased;
	private int costIncreasedUnq;
	private int costDecreasedUnq;
	private int costIncreased;
	private int costDecreased;
	private int compPriceIncreasedUnq;
	private int compPriceDecreasedUnq;
	private int compPriceIncreased;
	private int compPriceDecreased;
	
	
	public double getPromoUnits() {
		return promoUnits;
	}
	public void setPromoUnits(double promoUnits) {
		this.promoUnits = promoUnits;
	}
	public double getPromoSales() {
		return promoSales;
	}
	public void setPromoSales(double promoSales) {
		this.promoSales = promoSales;
	}
	public double getPromoMargin() {
		return promoMargin;
	}
	public void setPromoMargin(double promoMargin) {
		this.promoMargin = promoMargin;
	}
	public double getPromoMarginPct() {
		return promoMarginPct;
	}
	public void setPromoMarginPct(double promoMarginPct) {
		this.promoMarginPct = promoMarginPct;
	}
	public double getCurrentUnitsTotal() {
		return currentUnitsTotal;
	}
	public void setCurrentUnitsTotal(double currentUnitsTotal) {
		this.currentUnitsTotal = currentUnitsTotal;
	}
	public double getCurrentSalesTotal() {
		return currentSalesTotal;
	}
	public void setCurrentSalesTotal(double currentSalesTotal) {
		this.currentSalesTotal = currentSalesTotal;
	}
	public double getCurrentMarginTotal() {
		return currentMarginTotal;
	}
	public void setCurrentMarginTotal(double currentMarginTotal) {
		this.currentMarginTotal = currentMarginTotal;
	}
	public double getCurrentMarginPctTotal() {
		return currentMarginPctTotal;
	}
	public void setCurrentMarginPctTotal(double currentMarginPctTotal) {
		this.currentMarginPctTotal = currentMarginPctTotal;
	}
	public long getRunId() {
		return runId;
	}
	public void setRunId(long runId) {
		this.runId = runId;
	}
	public double getUnitsActual() {
		return unitsActual;
	}
	public void setUnitsActual(double unitsActual) {
		this.unitsActual = unitsActual;
	}
	public double getSalesActual() {
		return salesActual;
	}
	public void setSalesActual(double salesActual) {
		this.salesActual = salesActual;
	}
	public double getMarginActual() {
		return marginActual;
	}
	public void setMarginActual(double marginActual) {
		this.marginActual = marginActual;
	}
	public double getMarginPctActual() {
		return marginPctActual;
	}
	public void setMarginPctActual(double marginPctActual) {
		this.marginPctActual = marginPctActual;
	}
	public double getUnitsInBetWeeks() {
		return unitsInBetWeeks;
	}
	public void setUnitsInBetWeeks(double unitsInBetWeeks) {
		this.unitsInBetWeeks = unitsInBetWeeks;
	}
	public double getSalesInBetWeeks() {
		return salesInBetWeeks;
	}
	public void setSalesInBetWeeks(double salesInBetWeeks) {
		this.salesInBetWeeks = salesInBetWeeks;
	}
	public double getMarginInBetWeeks() {
		return marginInBetWeeks;
	}
	public void setMarginInBetWeeks(double marginInBetWeeks) {
		this.marginInBetWeeks = marginInBetWeeks;
	}
	public double getMarginPctInBetWeeks() {
		return marginPctInBetWeeks;
	}
	public void setMarginPctInBetWeeks(double marginPctInBetWeeks) {
		this.marginPctInBetWeeks = marginPctInBetWeeks;
	}
	public double getUnitsPredicted() {
		return unitsPredicted;
	}
	public void setUnitsPredicted(double unitsPredicted) {
		this.unitsPredicted = unitsPredicted;
	}
	public double getSalesPredicted() {
		return salesPredicted;
	}
	public void setSalesPredicted(double salesPredicted) {
		this.salesPredicted = salesPredicted;
	}
	public double getMarginPredicted() {
		return marginPredicted;
	}
	public void setMarginPredicted(double marginPredicted) {
		this.marginPredicted = marginPredicted;
	}
	public double getMarginPctPredicted() {
		return marginPctPredicted;
	}
	public void setMarginPctPredicted(double marginPctPredicted) {
		this.marginPctPredicted = marginPctPredicted;
	}
	public double getUnitsTotal() {
		return unitsTotal;
	}
	public void setUnitsTotal(double unitsTotal) {
		this.unitsTotal = unitsTotal;
	}
	public double getSalesTotal() {
		return salesTotal;
	}
	public void setSalesTotal(double salesTotal) {
		this.salesTotal = salesTotal;
	}
	public double getMarginTotal() {
		return marginTotal;
	}
	public void setMarginTotal(double marginTotal) {
		this.marginTotal = marginTotal;
	}
	public double getMarginPctTotal() {
		return marginPctTotal;
	}
	public void setMarginPctTotal(double marginPctTotal) {
		this.marginPctTotal = marginPctTotal;
	}
	public int getTotalDistinctItems() {
		return totalDistinctItems;
	}
	public void setTotalDistinctItems(int totalDistinctItems) {
		this.totalDistinctItems = totalDistinctItems;
	}
	public int getTotalItems() {
		return totalItems;
	}
	public void setTotalItems(int totalItems) {
		this.totalItems = totalItems;
	}
	public int getTotalPromoItems() {
		return totalPromoItems;
	}
	public void setTotalPromoItems(int totalPromoItems) {
		this.totalPromoItems = totalPromoItems;
	}
	public int getTotalDistinctPromoItems() {
		return totalDistinctPromoItems;
	}
	public void setTotalDistinctPromoItems(int totalDistinctPromoItems) {
		this.totalDistinctPromoItems = totalDistinctPromoItems;
	}
	public int getTotalEDLPItems() {
		return totalEDLPItems;
	}
	public void setTotalEDLPItems(int totalEDLPItems) {
		this.totalEDLPItems = totalEDLPItems;
	}
	public int getTotalDistinctEDLPItems() {
		return totalDistinctEDLPItems;
	}
	public void setTotalDistinctEDLPItems(int totalDistinctEDLPItems) {
		this.totalDistinctEDLPItems = totalDistinctEDLPItems;
	}
	public char getIsongoingQt() {
		return isongoingQt;
	}
	public void setIsongoingQt(char isongoingQt) {
		this.isongoingQt = isongoingQt;
	}
	public double getPriceIndex() {
		return priceIndex;
	}
	public void setPriceIndex(double priceIndex) {
		this.priceIndex = priceIndex;
	}
	public double getPriceIndexPromo() {
		return priceIndexPromo;
	}
	public void setPriceIndexPromo(double priceIndexPromo) {
		this.priceIndexPromo = priceIndexPromo;
	}
	public double getUnitsTotalClpDlp() {
		return unitsTotalClpDlp;
	}
	public void setUnitsTotalClpDlp(double unitsTotalClpDlp) {
		this.unitsTotalClpDlp = unitsTotalClpDlp;
	}
	public double getSalesTotalClpDlp() {
		return salesTotalClpDlp;
	}
	public void setSalesTotalClpDlp(double salesTotalClpDlp) {
		this.salesTotalClpDlp = salesTotalClpDlp;
	}
	public double getMarginTotalClpDlp() {
		return marginTotalClpDlp;
	}
	public void setMarginTotalClpDlp(double marginTotalClpDlp) {
		this.marginTotalClpDlp = marginTotalClpDlp;
	}
	public double getMarginPctTotalClpDlp() {
		return marginPctTotalClpDlp;
	}
	public void setMarginPctTotalClpDlp(double marginPctTotalClpDlp) {
		this.marginPctTotalClpDlp = marginPctTotalClpDlp;
	}
	public double getCurrentUnitsTotalClpDlp() {
		return currentUnitsTotalClpDlp;
	}
	public void setCurrentUnitsTotalClpDlp(double currentUnitsTotalClpDlp) {
		this.currentUnitsTotalClpDlp = currentUnitsTotalClpDlp;
	}
	public double getCurrentSalesTotalClpDlp() {
		return currentSalesTotalClpDlp;
	}
	public void setCurrentSalesTotalClpDlp(double currentSalesTotalClpDlp) {
		this.currentSalesTotalClpDlp = currentSalesTotalClpDlp;
	}
	public double getCurrentMarginTotalClpDlp() {
		return currentMarginTotalClpDlp;
	}
	public void setCurrentMarginTotalClpDlp(double currentMarginTotalClpDlp) {
		this.currentMarginTotalClpDlp = currentMarginTotalClpDlp;
	}
	public double getCurrentMarginPctTotalClpDlp() {
		return currentMarginPctTotalClpDlp;
	}
	public void setCurrentMarginPctTotalClpDlp(double currentMarginPctTotalClpDlp) {
		this.currentMarginPctTotalClpDlp = currentMarginPctTotalClpDlp;
	}
	public int getTotalRecommendedItems() {
		return totalRecommendedItems;
	}
	public void setTotalRecommendedItems(int totalRecommendedItems) {
		this.totalRecommendedItems = totalRecommendedItems;
	}
	public int getTotalRecommendedDistinctItems() {
		return totalRecommendedDistinctItems;
	}
	public void setTotalRecommendedDistinctItems(int totalRecommendedDistinctItems) {
		this.totalRecommendedDistinctItems = totalRecommendedDistinctItems;
	}
	public int getTotalCostChangedItems() {
		return totalCostChangedItems;
	}
	public void setTotalCostChangedItems(int totalCostChangedItems) {
		this.totalCostChangedItems = totalCostChangedItems;
	}
	public int getTotalCostChangedDistinctItems() {
		return totalCostChangedDistinctItems;
	}
	public void setTotalCostChangedDistinctItems(int totalCostChangedDistinctItems) {
		this.totalCostChangedDistinctItems = totalCostChangedDistinctItems;
	}
	public int getTotalCompChangedItems() {
		return totalCompChangedItems;
	}
	public void setTotalCompChangedItems(int totalCompChangedItems) {
		this.totalCompChangedItems = totalCompChangedItems;
	}
	public int getTotalCompChangedDistinctItems() {
		return totalCompChangedDistinctItems;
	}
	public void setTotalCompChangedDistinctItems(int totalCompChangedDistinctItems) {
		this.totalCompChangedDistinctItems = totalCompChangedDistinctItems;
	}
	public double getPriceChangeImpact() {
		return priceChangeImpact;
	}
	public void setPriceChangeImpact(double priceChangeImpact) {
		this.priceChangeImpact = priceChangeImpact;
	}
	public double getCurrentPriceIndex() {
		return currentPriceIndex;
	}
	public void setCurrentPriceIndex(double currentPriceIndex) {
		this.currentPriceIndex = currentPriceIndex;
	}
	public double getPriceIndexEDLPRec() {
		return priceIndexEDLPRec;
	}
	public void setPriceIndexEDLPRec(double priceIndexEDLPRec) {
		this.priceIndexEDLPRec = priceIndexEDLPRec;
	}
	public double getPriceIndexEDLPCurr() {
		return priceIndexEDLPCurr;
	}
	public void setPriceIndexEDLPCurr(double priceIndexEDLPCurr) {
		this.priceIndexEDLPCurr = priceIndexEDLPCurr;
	}
	public double getMarkUP() {
		return markUP;
	}
	public void setMarkUP(double markUP) {
		this.markUP = markUP;
	}
	public double getMarkDown() {
		return markDown;
	}
	public void setMarkDown(double markDown) {
		this.markDown = markDown;
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
	public int getCostIncreasedUnq() {
		return costIncreasedUnq;
	}
	public void setCostIncreasedUnq(int costIncreasedUnq) {
		this.costIncreasedUnq = costIncreasedUnq;
	}
	public int getCostDecreasedUnq() {
		return costDecreasedUnq;
	}
	public void setCostDecreasedUnq(int costDecreasedUnq) {
		this.costDecreasedUnq = costDecreasedUnq;
	}
	public int getCostIncreased() {
		return costIncreased;
	}
	public void setCostIncreased(int costIncreased) {
		this.costIncreased = costIncreased;
	}
	public int getCostDecreased() {
		return costDecreased;
	}
	public void setCostDecreased(int costDecreased) {
		this.costDecreased = costDecreased;
	}
	public int getCompPriceIncreasedUnq() {
		return compPriceIncreasedUnq;
	}
	public void setCompPriceIncreasedUnq(int compPriceIncreasedUnq) {
		this.compPriceIncreasedUnq = compPriceIncreasedUnq;
	}
	public int getCompPriceDecreasedUnq() {
		return compPriceDecreasedUnq;
	}
	public void setCompPriceDecreasedUnq(int compPriceDecreasedUnq) {
		this.compPriceDecreasedUnq = compPriceDecreasedUnq;
	}
	public int getCompPriceIncreased() {
		return compPriceIncreased;
	}
	public void setCompPriceIncreased(int compPriceIncreased) {
		this.compPriceIncreased = compPriceIncreased;
	}
	public int getCompPriceDecreased() {
		return compPriceDecreased;
	}
	public void setCompPriceDecreased(int compPriceDecreased) {
		this.compPriceDecreased = compPriceDecreased;
	}

}
