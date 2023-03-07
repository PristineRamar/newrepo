package com.pristine.dto.customer;

public class HouseholdSummaryDTO 
{
	private int _calendarId;
	
	private int _locationLevelId;
	private int _locationId;

	private int _productLevelId;
	private int _productId;

	private int _customerId;
	private String _loyaltyCardNo;
	
	private int _transNo;
	
	private double _totalRevenue;
	private double _regularRevenue;
	private double _saleRevenue;
	
	private double _totalMargin;
	private double _regularMargin;
	private double _saleMargin;

	private double _totalMarginPct;
	private double _regularMarginPct;
	private double _saleMarginPct;
	
	private double _totalUnit;
	private double _regularUnit;
	private double _saleUnit;

	private double _totalVolume;
	private double _regularVolume;
	private double _saleVolume;

	private double _totalVisit;
	private double _averageOrderSize;
	
	private double _plTotalRevenue;
	private double _plRegularRevenue;
	private double _plSaleRevenue;
	
	private double _plTotalMargin;
	private double _plRegularMargin;
	private double _plSaleMargin;
	
	private double _plRegularMarginPct;
	private double _plSaleMarginPct;
	private double _plTotalMarginPct;
	
	private double _plTotalUnit;
	private double _plRegularUnit;
	private double _plSaleUnit;

	private double _plTotalVolume;
	private double _plRegularVolume;
	private double _plSaleVolume;
	
	private double _plTotalVisit;
	private double _plAverageOrderSize;
	
	private double _regularCost = 0;
	private double _dealCost = 0;
	private double _plRegularCost = 0;
	private double _plDealCost = 0;
	
	public int getcalendarId() {
		return _calendarId;
	}
	public void setcalendarId(int _calendarId) {
		this._calendarId = _calendarId;
	}
	
	public int getLocationLevelId()
	{
		return _locationLevelId;
	}
	public void setLocationLevelId(int v)
	{
		_locationLevelId = v;
	}
	
	public int getLocationId()
	{
		return _locationId;
	}
	public void setLocationId(int v)
	{
		_locationId = v;
	}

	public int getProductLevelId()
	{
		return _productLevelId;
	}
	public void setProductLevelId(int v)
	{
		_productLevelId = v;
	}
	
	public int getProductId()
	{
		return this._productId;
	}
	public void setProductId(int v)
	{
		this._productId = v;
	}

	public int getCustomerId() {
		return _customerId;
	}
	public void setCustomerId(int v) {
		_customerId = v;
	}

	public String getLoyaltyCardNo() {
		return _loyaltyCardNo;
	}
	public void setLoyaltyCardNo(String v) {
		_loyaltyCardNo = v;
	}	

	public int getTransNo() {
		return _transNo;
	}
	public void setTransNo(int v) {
		_transNo = v;
	}	
	

	
	
	public double getTotalRevenue()
	{
		return _totalRevenue;
	}
	public void setTotalRevenue(double v)
	{
		this._totalRevenue = v;
	}

	public double getRegularRevenue()
	{
		return _regularRevenue;
	}
	public void setRegularRevenue(double v)
	{
		this._regularRevenue = v;
	}
	
	public double getSaleRevenue()
	{
		return _saleRevenue;
	}
	public void setSaleRevenue(double v)
	{
		this._saleRevenue = v;
	}
	
	public double getRegularMargin()
	{
		return _regularMargin;
	}
	public void setRegularMargin(double v)
	{
		this._regularMargin = v;
	}
	
	public double getSaleMargin()
	{
		return _saleMargin;
	}
	public void setSaleMargin(double v)
	{
		this._saleMargin = v;
	}
	
	public double getTotalMargin()
	{
		return _totalMargin;
	}
	public void setTotalMargin(double v)
	{
		this._totalMargin = v;
	}
	
	public double getRegularMarginPct()
	{
		return _regularMarginPct;
	}
	public void setRegularMarginPct(double v)
	{
		this._regularMarginPct = v;
	}
	
	public double getSaleMarginPct()
	{
		return _saleMarginPct;
	}
	public void setSaleMarginPct(double v)
	{
		this._saleMarginPct = v;
	}
	
	public double getTotalMarginPct()
	{
		return _totalMarginPct;
	}
	public void setTotalMarginPct(double v)
	{
		this._totalMarginPct = v;
	}

	
	public double getRegularUnit()
	{
		return _regularUnit;
	}
	public void setRegularUnit(double v)
	{
		this._regularUnit = v;
	}

	public double getSaleUnit()
	{
		return _saleUnit;
	}
	public void setSaleUnit(double v)
	{
		this._saleUnit = v;
	}
	
	public double getTotalUnit()
	{
		return _totalUnit;
	}
	public void setTotalUnit(double v)
	{
		this._totalUnit = v;
	}


	public double getTotalVolume() {
		return _totalVolume;
	}
	public void setTotalVolume(double v) {
		this._totalVolume = v;
	}
	
	public double getRegularVolume() {
		return _regularVolume;
	}
	public void setRegularVolume(double v) {
		this._regularVolume = v;
	}	
	
	public double getSaleVolume() {
		return _saleVolume;
	}
	public void setSaleVolume(double v) {
		this._saleVolume = v;
	}
	
	public double getTotalVisit()
	{
		return _totalVisit;
	}
	public void setTotalVisit(double v)
	{
		this._totalVisit = v;
	}

	public double getAverageOrderSize()
	{
		return _averageOrderSize;
	}
	public void setAverageOrderSize(double v)
	{
		this._averageOrderSize = v;
	}

	public double getPLTotalRevenue()
	{
		return _plTotalRevenue;
	}
	public void setPLTotalRevenue(double v)
	{
		this._plTotalRevenue = v;
	}	
	
	public double getPLRegularRevenue()
	{
		return _plRegularRevenue;
	}
	public void setPLRegularRevenue(double v)
	{
		this._plRegularRevenue = v;
	}
	
	public double getPLSaleRevenue()
	{
		return _plSaleRevenue;
	}
	public void setPLSaleRevenue(double v)
	{
		this._plSaleRevenue = v;
	}
	
	public double getPLTotalMargin()
	{
		return _plTotalMargin;
	}
	public void setPLTotalMargin(double v)
	{
		this._plTotalMargin = v;
	}
	
	public double getPLRegularMargin()
	{
		return _plRegularMargin;
	}
	public void setPLRegularMargin(double v)
	{
		this._plRegularMargin = v;
	}
	
	public double getPLSaleMargin()
	{
		return _plSaleMargin;
	}
	public void setPLSaleMargin(double v)
	{
		this._plSaleMargin = v;
	}
	
	public double getPLTotalMarginPct()
	{
		return _plTotalMarginPct;
	}
	public void setPLTotalMarginPct(double v)
	{
		this._plTotalMarginPct = v;
	}
	
	public double getPLRegularMarginPct()
	{
		return _plRegularMarginPct;
	}
	public void setPLRegularMarginPct(double v)
	{
		this._plRegularMarginPct = v;
	}
	
	public double getPLSaleMarginPct()
	{
		return _plSaleMarginPct;
	}
	public void setPLSaleMarginPct(double v)
	{
		this._plSaleMarginPct = v;
	}
	
	public double getPLTotalUnit()
	{
		return _plTotalUnit;
	}
	public void setPLTotalUnit(double v)
	{
		this._plTotalUnit = v;
	}
	
	public double getPLRegularUnit()
	{
		return _plRegularUnit;
	}
	public void setPLRegularUnit(double v)
	{
		this._plRegularUnit = v;
	}

	public double getPLSaleUnit()
	{
		return _plSaleUnit;
	}
	public void setPLSaleUnit(double v)
	{
		this._plSaleUnit = v;
	}
	
	public double getPLTotalVolume() {
		return _plTotalVolume;
	}
	public void setPLTotalVolume(double v) {
		this._plTotalVolume = v;
	}
	
	public double getPLRegularVolume() {
		return _plRegularVolume;
	}
	public void setPLRegularVolume(double v) {
		this._plRegularVolume = v;
	}	
	
	public double getPLSaleVolume() {
		return _plSaleVolume;
	}
	public void setPLSaleVolume(double v) {
		this._plSaleVolume = v;
	}
	
	public double getPLTotalVisit()
	{
		return _plTotalVisit;
	}
	public void setPLTotalVisit(double v)
	{
		this._plTotalVisit = v;
	}

	public double getPLAverageOrderSize()
	{
		return _plAverageOrderSize;
	}
	public void setPLAverageOrderSize(double v)
	{
		this._plAverageOrderSize = v;
	}
	
	public double getRegularCost() {
		return _regularCost;
	}
	public void setRegularCost(double regularDealCost) {
		this._regularCost = regularDealCost;
	}
	
	public double getDealCost() {
		return _dealCost;
	}
	public void setDealCost(double saleDealCost) {
		this._dealCost = saleDealCost;
	}

	public double getPLRegularCost() {
		return _plRegularCost;
	}
	public void setPLRegularCost(double _v) {
		this._plRegularCost = _v;
	}
	
	public double getPLDealCost() {
		return _plDealCost;
	}
	public void setPLDealCost(double _v) {
		this._plDealCost = _v;
	}
}
