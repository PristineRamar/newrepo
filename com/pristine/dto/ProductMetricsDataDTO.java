package com.pristine.dto;

import java.io.Serializable;

public class ProductMetricsDataDTO implements Serializable
{
	
	/**
	 * 
	 */
	private static final long serialVersionUID = 8472975519327521630L;
	private int _productlevelId;
	private int _productId;
	private int _calendarId;
	private int _locationLevelId;
	private int _locationId;
	private double _UOM;
	private String _saleFlag;
	private double _promoId=0;
	private double _regularMPack;
	private double _regularMPrice;
	private double _regularPrice;
	private double _listPrice;  // listcost ?
	private double _regularRevenue;
	private double _regularMargin;
	private double _regularMarginPct;
	private double _regularMovement;
	private double _regularMovementVolume;
	private double _regularQuantity;
	private double _regularWeight;

	private double _saleMPack;
	private double _saleMPrice;
	private double _salePrice;
	private double _dealPrice;  // dealcost? 
	private double _saleRevenue;
	private double _saleMargin;
	private double _saleMarginPct;
	private double _saleMovement;
	private double _saleMovementVolume;
	private double _saleQuantity;
	private double _saleWeight;
	
	private double _totalRevenue;
	private double _totalMovement;
	private double _netMargin;
	private double _netMarginPct;

	private double _13WeekMovementAvg;
	private double _totalVisits;
	private double _avgOrderSize;
	private double _finalPrice;
	private double _finalCost;
	private double _grossUnits;
	private double _grossSales;
	
	public String getStartDate() {
		return startDate;
	}
	public void setStartDate(String startDate) {
		this.startDate = startDate;
	}
	public String getEndDate() {
		return endDate;
	}
	public void setEndDate(String endDate) {
		this.endDate = endDate;
	}
	private String startDate;
	private String endDate;
	public int getProductLevelId() {
		return _productlevelId;
	}
	public void setProductLevelId(int v) {
		_productlevelId = v;
	}
	
	public int getProductId() {
		return _productId;
	}
	public void setProductId(int v) {
		_productId = v;
	}

	public double getUOM() {
		return _UOM;
	}
	public void setUOM(double v) {
		_UOM = v;
	}
	
	public String isSaleFlag() {
		return _saleFlag;
	}
	public void setSaleFlag(String v) {
		_saleFlag = v;
	}

	public double getPromoId() {
		return _promoId;
	}
	public void setPromoId(double v) {
		_promoId = v;
	}
	
	public double getRegularMPack() {
		return _regularMPack;
	}
	public void setRegularMPack(double v) {
		_regularMPack = v;
	}
	

	public double getRegularMPrice() {
		return _regularMPrice;
	}
	public void setRegularMPrice(double v) {
		_regularMPrice = v;
	}


	public double getRegularPrice() {
		return _regularPrice;
	}
	public void setRegularPrice(double v) {
		_regularPrice = v;
	}	
	

	public double getListPrice() {
		return _listPrice;
	}
	public void setListPrice(double v) {
		_listPrice = v;
	}	
	

	public double getRegularRevenue() {
		return _regularRevenue;
	}
	public void setRegularRevenue(double v) {
		_regularRevenue = v;
	}	

	public double getRegularMargin() {
		return _regularMargin;
	}
	public void setRegularMargin(double v) {
		_regularMargin = v;
	}	

	public double getRegularMarginPct() {
		return _regularMarginPct;
	}
	public void setRegularMarginPct(double v) {
		_regularMarginPct = v;
	}	
	
	public double getRegularMovement() {
		return _regularMovement;
	}
	public void setRegularMovement(double v) {
		_regularMovement = v;
	}

	public double getRegularMovementVolume() {
		return _regularMovementVolume;
	}
	public void setRegularMovementVolume(double v) {
		_regularMovementVolume = v;
	}
	
	public double getRegularQuantity() {
		return _regularQuantity;
	}
	public void setRegularQuantity(double v) {
		_regularQuantity = v;
	}

	public double getRegularWeight() {
		return _regularWeight;
	}
	public void setRegularWeight(double v) {
		_regularWeight = v;
	}
	
	public double getSaleMPack() {
		return _saleMPack;
	}
	public void setSaleMPack(double v) {
		_saleMPack = v;
	}
	

	public double getSaleMPrice() {
		return _saleMPrice;
	}
	public void setSaleMPrice(double v) {
		_saleMPrice = v;
	}


	public double getSalePrice() {
		return _salePrice;
	}
	public void setSalePrice(double v) {
		_salePrice = v;
	}	
	

	public double getDealPrice() {
		return _dealPrice;
	}
	public void setDealPrice(double v) {
		_dealPrice = v;
	}	
	

	public double getSaleRevenue() {
		return _saleRevenue;
	}
	public void setSaleRevenue(double v) {
		_saleRevenue = v;
	}	

	public double getSaleMargin() {
		return _saleMargin;
	}
	public void setSaleMargin(double v) {
		_saleMargin = v;
	}	

	
	public double getSaleMarginPct() {
		return _saleMarginPct;
	}
	public void setSaleMarginPct(double v) {
		_saleMarginPct = v;
	}	
	

	public double getSaleMovement() {
		return _saleMovement;
	}
	public void setSaleMovement(double v) {
		_saleMovement = v;
	}

	public double getSaleMovementVolume() {
		return _saleMovementVolume;
	}
	public void setSaleMovementVolume(double v) {
		_saleMovementVolume = v;
	}
	
	public double getSaleQuantity() {
		return _saleQuantity;
	}
	public void setSaleQuantity(double v) {
		_saleQuantity = v;
	}

	public double getSaleWeight() {
		return _saleWeight;
	}
	public void setSaleWeight(double v) {
		_saleWeight = v;
	}

	public double get13WeekMovementAvg() {
		return _13WeekMovementAvg;
	}
	public void set13WeekMovementAvg(double v) {
		_13WeekMovementAvg = v;
	}

	public double getTotalVisits() {
		return _totalVisits;
	}
	public void setTotalVisits(double v) {
		_totalVisits = v;
	}

	public double getAvgOrderSize() {
		return _avgOrderSize;
	}
	public void setAvgOrderSize(double v) {
		_avgOrderSize = v;
	}
	public void setFinalPrice(double p) {
		// TODO Auto-generated method stub
		_finalPrice = p;
	}
	public void setFinalCost(double c) {
		// TODO Auto-generated method stub
		_finalCost = c;
	}
	public double getFinalPrice(){
		return _finalPrice;
	}

	public double getFinalCost(){
		return _finalCost;
	}
	public int getCalendarId() {
		return _calendarId;
	}
	public void setCalendarId(int _calendarId) {
		this._calendarId = _calendarId;
	}
	public int getLocationLevelId() {
		return _locationLevelId;
	}
	public void setLocationLevelId(int _locationLevelId) {
		this._locationLevelId = _locationLevelId;
	}
	public int getLocationId() {
		return _locationId;
	}
	public void setLocationId(int _locationId) {
		this._locationId = _locationId;
	}
	public double getTotalRevenue() {
		return _totalRevenue;
	}
	public void setTotalRevenue(double _totalRevenue) {
		this._totalRevenue = _totalRevenue;
	}
	public double getTotalMovement() {
		return _totalMovement;
	}
	public void setTotalMovement(double _totalMovement) {
		this._totalMovement = _totalMovement;
	}
	public double getNetMargin() {
		return _netMargin;
	}
	public void setNetMargin(double _netMargin) {
		this._netMargin = _netMargin;
	}
	public double getNetMarginPct() {
		return _netMarginPct;
	}
	public void setNetMarginPct(double _netMarginPct) {
		this._netMarginPct = _netMarginPct;
	}
	
	
	@Override
	public String toString() {
		return "ProductMetricsDataDTO [_productlevelId=" + _productlevelId + ", _productId=" + _productId
				+ ", _calendarId=" + _calendarId + ", _locationLevelId=" + _locationLevelId + ", _locationId="
				+ _locationId + ", _UOM=" + _UOM + ", _saleFlag=" + _saleFlag + ", _promoId=" + _promoId
				+ ", _regularMPack=" + _regularMPack + ", _regularMPrice=" + _regularMPrice + ", _regularPrice="
				+ _regularPrice + ", _listPrice=" + _listPrice + ", _regularRevenue=" + _regularRevenue
				+ ", _regularMargin=" + _regularMargin + ", _regularMarginPct=" + _regularMarginPct
				+ ", _regularMovement=" + _regularMovement + ", _regularMovementVolume=" + _regularMovementVolume
				+ ", _regularQuantity=" + _regularQuantity + ", _regularWeight=" + _regularWeight + ", _saleMPack="
				+ _saleMPack + ", _saleMPrice=" + _saleMPrice + ", _salePrice=" + _salePrice + ", _dealPrice="
				+ _dealPrice + ", _saleRevenue=" + _saleRevenue + ", _saleMargin=" + _saleMargin + ", _saleMarginPct="
				+ _saleMarginPct + ", _saleMovement=" + _saleMovement + ", _saleMovementVolume=" + _saleMovementVolume
				+ ", _saleQuantity=" + _saleQuantity + ", _saleWeight=" + _saleWeight + ", _totalRevenue="
				+ _totalRevenue + ", _totalMovement=" + _totalMovement + ", _netMargin=" + _netMargin
				+ ", _netMarginPct=" + _netMarginPct + ", _13WeekMovementAvg=" + _13WeekMovementAvg + ", _totalVisits="
				+ _totalVisits + ", _avgOrderSize=" + _avgOrderSize + ", _finalPrice=" + _finalPrice + ", _finalCost="
				+ _finalCost + ", startDate=" + startDate + ", endDate=" + endDate + "]";
	}
	public double getGrossUnits() {
		return _grossUnits;
	}
	public void setGrossUnits(double _grossUnits) {
		this._grossUnits = _grossUnits;
	}
	public double getGrossSales() {
		return _grossSales;
	}
	public void setGrossSales(double _grossSales) {
		this._grossSales = _grossSales;
	}
	
	
}
