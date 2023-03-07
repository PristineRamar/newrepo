package com.pristine.dto.offermgmt.mwr;

public class SummarySplitupDTO {

/*	RUN_ID
	WEEK_CALENDAR_ID
	UNITS_ACTUAL
	REV_ACTUAL
	MAR_ACTUAL
	MAR_PCT_ACTUAL
	UNITS_PRED
	REV_PRED
	MAR_PRED
	MAR_PCT_PRED
*/

	
	private long runId;
	private int calendarId;
	private double unitsActual;
	private double salesActual;
	private double marginActual;
	private double marginPctActual;
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
	private double edlpPriceIndexRec;
	private double edlpPriceIndexCurr;
	private double totalPriceIndexRec;
	private double totalPriceIndexCurr;
	private double priceIndexPromo;
	private String calType;
	private double unitsTotalClpDlp;
	private double salesTotalClpDlp;
	private double marginTotalClpDlp;
	private double marginPctTotalClpDlp;
	private double currentUnitsTotalClpDlp;
	private double currentSalesTotalClpDlp;
	private double currentMarginTotalClpDlp;
	private double currentMarginPctTotalClpDlp;
	private int periodCalendarId;
	private boolean isCompletedWeek;
	private double priceChangeImpact;
	private double markUp;
	private double markDown;
	
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
	
	
	public long getRunId() {
		return runId;
	}
	public void setRunId(long runId) {
		this.runId = runId;
	}
	public int getCalendarId() {
		return calendarId;
	}
	public void setCalendarId(int calendarId) {
		this.calendarId = calendarId;
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
	public double getMarginTotal() {
		return marginTotal;
	}
	public void setMarginTotal(double marginTotal) {
		this.marginTotal = marginTotal;
	}
	public double getSalesTotal() {
		return salesTotal;
	}
	public void setSalesTotal(double salesTotal) {
		this.salesTotal = salesTotal;
	}
	public double getMarginPctTotal() {
		return marginPctTotal;
	}
	public void setMarginPctTotal(double marginPctTotal) {
		this.marginPctTotal = marginPctTotal;
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
	public double getEdlpPriceIndexRec() {
		return edlpPriceIndexRec;
	}
	public void setEdlpPriceIndexRec(double edlpPriceIndexRec) {
		this.edlpPriceIndexRec = edlpPriceIndexRec;
	}
	public double getEdlpPriceIndexCurr() {
		return edlpPriceIndexCurr;
	}
	public void setEdlpPriceIndexCurr(double edlpPriceIndexCurr) {
		this.edlpPriceIndexCurr = edlpPriceIndexCurr;
	}
	public double getTotalPriceIndexRec() {
		return totalPriceIndexRec;
	}
	public void setTotalPriceIndexRec(double totalPriceIndexRec) {
		this.totalPriceIndexRec = totalPriceIndexRec;
	}
	public double getTotalPriceIndexCurr() {
		return totalPriceIndexCurr;
	}
	public void setTotalPriceIndexCurr(double totalPriceIndexCurr) {
		this.totalPriceIndexCurr = totalPriceIndexCurr;
	}
	public double getPriceIndexPromo() {
		return priceIndexPromo;
	}
	public void setPriceIndexPromo(double priceIndexPromo) {
		this.priceIndexPromo = priceIndexPromo;
	}
	public String getCalType() {
		return calType;
	}
	public void setCalType(String calType) {
		this.calType = calType;
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
	public int getPeriodCalendarId() {
		return periodCalendarId;
	}
	public void setPeriodCalendarId(int periodCalendarId) {
		this.periodCalendarId = periodCalendarId;
	}
	@Override
	public String toString() {
		return "SummarySplitupDTO [runId=" + runId + ", calendarId=" + calendarId + ", unitsActual=" + unitsActual
				+ ", salesActual=" + salesActual + ", marginActual=" + marginActual + ", marginPctActual="
				+ marginPctActual + ", unitsPredicted=" + unitsPredicted + ", salesPredicted=" + salesPredicted
				+ ", marginPredicted=" + marginPredicted + ", marginPctPredicted=" + marginPctPredicted
				+ ", unitsTotal=" + unitsTotal + ", salesTotal=" + salesTotal + ", marginTotal=" + marginTotal
				+ ", marginPctTotal=" + marginPctTotal + ", currentUnitsTotal=" + currentUnitsTotal
				+ ", currentSalesTotal=" + currentSalesTotal + ", currentMarginTotal=" + currentMarginTotal
				+ ", currentMarginPctTotal=" + currentMarginPctTotal + ", promoUnits=" + promoUnits + ", promoSales="
				+ promoSales + ", promoMargin=" + promoMargin + ", promoMarginPct=" + promoMarginPct
				+ ", edlpPriceIndexRec=" + edlpPriceIndexRec + ", edlpPriceIndexCurr=" + edlpPriceIndexCurr
				+ ", totalPriceIndexRec=" + totalPriceIndexRec + ", totalPriceIndexCurr=" + totalPriceIndexCurr
				+ ", priceIndexPromo=" + priceIndexPromo + ", calType=" + calType + ", unitsTotalClpDlp="
				+ unitsTotalClpDlp + ", salesTotalClpDlp=" + salesTotalClpDlp + ", marginTotalClpDlp="
				+ marginTotalClpDlp + ", marginPctTotalClpDlp=" + marginPctTotalClpDlp + ", currentUnitsTotalClpDlp="
				+ currentUnitsTotalClpDlp + ", currentSalesTotalClpDlp=" + currentSalesTotalClpDlp
				+ ", currentMarginTotalClpDlp=" + currentMarginTotalClpDlp + ", currentMarginPctTotalClpDlp="
				+ currentMarginPctTotalClpDlp + ", periodCalendarId=" + periodCalendarId 
				+ ", priceChangeImpact=" + priceChangeImpact + ",markUp =" + markUp + " ,markDown =" + markDown + "]";
	}
	public boolean isCompletedWeek() {
		return isCompletedWeek;
	}
	public void setCompletedWeek(boolean isCompletedWeek) {
		this.isCompletedWeek = isCompletedWeek;
	}
	public double getPriceChangeImpact() {
		return priceChangeImpact;
	}
	public void setPriceChangeImpact(double priceChangeImpact) {
		this.priceChangeImpact = priceChangeImpact;
	}
	public double getMarkUp() {
		return markUp;
	}
	public void setMarkUp(double markUp) {
		this.markUp = markUp;
	}
	public double getMarkDown() {
		return markDown;
	}
	public void setMarkDown(double markDown) {
		this.markDown = markDown;
	}
}
