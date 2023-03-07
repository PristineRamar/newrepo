package com.pristine.dto;

import java.util.Date;

public class MovementWeeklyDTO extends MovementDTO
{
	public int getCompStoreId () { return _CompStoreId; }
	public void setCompStoreId(int v) { _CompStoreId = v; }
	private int _CompStoreId = -1;
	
	public int getDeptId () { return _DeptId; }
	public void setDeptId(int v) { _DeptId = v; }
	private int _DeptId = -1;

	public double get_TotalMovement() {	return _TotalMovement;	}
 	public void set_TotalMovement(double _TotalMovement) {	this._TotalMovement = _TotalMovement;	}
	private double _TotalMovement;
	
	public String getNewItemCode () { return _NewItemCode; }
	public void setNewItemCode(String v) { _NewItemCode = v; }
	private String _NewItemCode;
	
	public int getItemCode () { return _ItemCode; }
	public void setItemCode(int v) { _ItemCode = v; }
	private int _ItemCode = -1;
	
	public int getCheckDataId () { return _CheckDataId; }
	public void setCheckDataId(int v) { _CheckDataId = v; }
	private int _CheckDataId = -1;
	
	public double getTotalPrice () { return _TotalPrice; }
	public void setTotalPrice(double v) { _TotalPrice = v; }
	private double _TotalPrice = 0;
	
	public double getRevenueRegular () { return getExtendedGrossPrice(); }
	public void setRevenueRegular(double v) { setExtendedGrossPrice(v); }
	
	public double getQuantityRegular () { return getExtnQty(); }
	public void setQuantityRegular(double v) { setExtnQty(v); }
	
	public double getRevenueSale () { return getExtendedNetPrice(); }
	public void setRevenueSale(double v) { setExtendedNetPrice(v); }
	
	public double getQuantitySale () { return getExtnSaleQty(); }
	public void setQuantitySale(double v) { setExtnSaleQty(v); }
	
	public boolean getSaleFlag() { return _SaleFlag; }
	public void setSaleFlag(boolean v) { _SaleFlag = v; }
	private boolean _SaleFlag = false;
	
	public double getListCost() { return getUnitCostGross(); }
	public void setListCost(double v) { setUnitCostGross(v); }
	
	public Date getEffListCostDate() { return _EffListCostDate; }
	public void setEffListCostDate(Date v) { _EffListCostDate = v; }
	private Date _EffListCostDate = null;
	
	public double getDealCost() { return getUnitCost(); }
	public void setDealCost(double v) { setUnitCost(v); }
	
	public Date getDealStartDate() { return _DealStartDate; }
	public void setDealStartDate(Date v) { _DealStartDate = v; }
	private Date _DealStartDate = null;
	
	public Date getDealEndDate() { return _DealEndDate; }
	public void setDealEndDate(Date v) { _DealEndDate = v; }
	private Date _DealEndDate = null;
	
	public int getCostChangeDirection() { return _CostChangeDirection; }
	public void setCostChangeDirection(int v) { _CostChangeDirection = v; }
	private int _CostChangeDirection = 0;
	
	public int getMarginChangeDirection() { return _MarginChangeDirection; }
	public void setMarginChangeDirection(int v) { _MarginChangeDirection = v; }
	private int _MarginChangeDirection = 0;
	
	public double getFinalCost() { return _FinalCost; }
	public void setFinalCost(double v) { _FinalCost = v; }
	private double _FinalCost = 0;
	
	public double getMargin () { return _Margin; }
	public void setMargin(double v) { _Margin = v; }
	private double _Margin = 0;
	
	private int locationLevelId;
	private int locationId;
	private double regUnitPrice;
	private double saleUnitPrice;
	
	
	
	public double getMarginPercent()
	{
		//if ( _Margin > 0 )
		{
			double totalRevenue = getTotalRevenue();
			if ( totalRevenue > 0 ) {
				_MarginPercent = _Margin / totalRevenue * 100; 
			}
		}
		return _MarginPercent;
	}
	public void setMarginPercent(double v) { _MarginPercent = v; }
	private double _MarginPercent = 0;
	
	public int getVisitCount () { return _VisitCount; }
	public void setVisitCount(int v) { _VisitCount = v; }
	private int _VisitCount = 0;
	
	public double getTotalRevenue() { return getRevenueRegular() + getRevenueSale(); }
	
	public double getQuantityRegular13Week () { return _QuantityRegular13Week; }
	public void setQuantityRegular13Week (double v) { _QuantityRegular13Week = v; }
	private double _QuantityRegular13Week = 0;
	
	public double getQuantitySale13Week () { return _QuantitySale13Week; }
	public void setQuantitySale13Week (double v) { _QuantitySale13Week = v; }
	private double _QuantitySale13Week = 0;
	
	public int getLirId () { return _LirId; }
	public void setLirId(int v) { _LirId = v; }
	private int _LirId = -1;
	
	public int getLirItemCode () { return _LirItemCode; }
	public void setLirItemCode(int v) { _LirItemCode = v; }
	private int _LirItemCode = -1;
	

	private double _regularQuantity;
	private double _saleQuantity;
	private String flag;
	private String _subcategoryid;
	private String _categoryid;
	private String _departmentid;
	private String _upc;
	private String _uomId;
	private double _regMovementVolume;
	private double _saleMovementVolume;
	private double _igRegVolumeRev;
	private double _igSaleVolumeRev;
	private String _transactionNo;
	
	// dto property added for getting gas revenue
	private double _regGrossRevenue;
	private double _saleGrossRevenue;
	private double _actualWeight;
	private double _actualQuantity;

	
	public String get_subcategoryid() {
		return _subcategoryid;
	}
	
	public void set_subcategoryid(String _subcategoryid) {
		this._subcategoryid = _subcategoryid;
	}
	public String getFlag() {
		return flag;
	}
	 
	public void setFlag(String flag) {
		this.flag = flag;
	}

	 
	public double get_regularQuantity() {
		return _regularQuantity;
	}
	 
	public void set_regularQuantity(double _regularQuantity) {
		this._regularQuantity = _regularQuantity;
	}
	
	 
	public double get_saleQuantity() {
		return _saleQuantity;
	}
	 
	public void set_saleQuantity(double _saleQuantity) {
		this._saleQuantity = _saleQuantity;
	}
	public String get_categoryid() {
		return _categoryid;
	}
	public void set_categoryid(String _categoryid) {
		this._categoryid = _categoryid;
	}
	public String get_departmentid() {
		return _departmentid;
	}
	public void set_departmentid(String _departmentid) {
		this._departmentid = _departmentid;
	}
		
	public String getupc () {
		return _upc; 
	}
	
	public void setupc(String v) {
		_upc = v; 
	}
	
	
	public String getuomId() {
		return _uomId;
	}
	public void setuomId(String _uomId) {
		this._uomId = _uomId;
	}
	public double getregMovementVolume() {
		return _regMovementVolume;
	}
	public void setregMovementVolume(double _regMovementVolume) {
		this._regMovementVolume = _regMovementVolume;
	}
	public double getsaleMovementVolume() {
		return _saleMovementVolume;
	}
	public void setsaleMovementVolume(double _saleMovementVolume) {
		this._saleMovementVolume = _saleMovementVolume;
	}
	
		
	public double getigSaleVolumeRev() {
		return _igSaleVolumeRev;
	}
	public void setigSaleVolumeRev(double _igSaleVolumeRev) {
		this._igSaleVolumeRev = _igSaleVolumeRev;
	}
	public double getigRegVolumeRev() {
		return _igRegVolumeRev;
	}
	public void setigRegVolumeRev(double _igRegVolumeRev) {
		this._igRegVolumeRev = _igRegVolumeRev;
	}

	public String gettransactionNo() {
		return _transactionNo;
	}
	public void settransactionNo(String _transactionNo) {
		this._transactionNo = _transactionNo;
	}
	public double getregGrossRevenue() {
		return _regGrossRevenue;
	}
	public void setregGrossRevenue(double _regGrossRevenue) {
		this._regGrossRevenue = _regGrossRevenue;
	}
	public double getsaleGrossRevenue() {
		return _saleGrossRevenue;
	}
	public void setsaleGrossRevenue(double _saleGrossRevenue) {
		this._saleGrossRevenue = _saleGrossRevenue;
	}
	public double getActualWeight() {
		return _actualWeight;
	}
	public void setActualWeight(double _actualWeight) {
		this._actualWeight = _actualWeight;
	}
	public double getActualQuantity() {
		return _actualQuantity;
	}
	public void setActualQuantity(double _actualQuantity) {
		this._actualQuantity = _actualQuantity;
	}
	
	// Price Index By Price Zone
	private int _priceZoneId = -1;
	public int getPriceZoneId () { return _priceZoneId; }
	public void setPriceZoneId(int v) { _priceZoneId = v; }
	// Price Index By Price Zone - Ends
	
	private String weekStartDate = null;
	private String effListCostDate = null;
	public String getWeekStartDate() {
		return weekStartDate;
	}
	public void setWeekStartDate(String weekStartDate) {
		this.weekStartDate = weekStartDate;
	}
	public void setEffListCostDate(String effListCostDate) {
		this.effListCostDate = effListCostDate;
	}
	public int getLocationLevelId() {
		return locationLevelId;
	}
	public void setLocationLevelId(int locationLevelId) {
		this.locationLevelId = locationLevelId;
	}
	public int getLocationId() {
		return locationId;
	}
	public void setLocationId(int locationId) {
		this.locationId = locationId;
	}
	public double getRegUnitPrice() {
		return regUnitPrice;
	}
	public void setRegUnitPrice(double regUnitPrice) {
		this.regUnitPrice = regUnitPrice;
	}
	public double getSaleUnitPrice() {
		return saleUnitPrice;
	}
	public void setSaleUnitPrice(double saleUnitPrice) {
		this.saleUnitPrice = saleUnitPrice;
	}
}
