package com.pristine.dto;

public class SummaryDataDTO 
{
	private int 	_locationLevelId;
	private int 	_locationId;
	private String  _storeNumber;
	private int 	_productLevelId;
	private String 	_productId;
	private int 	_calendarId;
	private double _regularMovement 	= 0;
	private double _saleMovement 		= 0;
	private double _totalMovement		= 0;
	private double _regularRevenue 		= 0;
	private double _saleRevenue 		= 0;
	private double _totalRevenue 		= 0;
	private double _regularMargin 		= 0;
	private double _saleMargin 			= 0;
	private double _totalMargin 		= 0;
	private double _regularMarginPer 	= 0;
	private double _saleMarginPer 		= 0;
	private double _totalMarginPer 		= 0;
	private double _totalVisitCount 	= 0;
	private double _averageOrderSize 	= 0;
	
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
	
	
	public String getProductId()
	{
		return this._productId;
	}
	public void setProductId(String v)
	{
		this._productId = v;
	}
	
	
	public double getRegularMovement()
	{
		return _regularMovement;
	}
	public void setRegularMovement(double v)
	{
		this._regularMovement = v;
	}
	
	
	public double getSaleMovementId()
	{
		return _saleMovement;
	}
	public void setSaleMovement(double v)
	{
		this._saleMovement = v;
	}
	
	
	public double getTotalMovement()
	{
		return _totalMovement;
	}
	public void setTotalMovement(double v)
	{
		this._totalMovement = v;
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
	
	
	public double getTotalRevenue()
	{
		return _totalRevenue;
	}
	public void setTotalRevenue(double v)
	{
		this._totalRevenue = v;
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

	
	public double getRegularMarginPer()
	{
		return _regularMarginPer;
	}
	public void setRegularMarginPer(double v)
	{
		this._regularMarginPer = v;
	}
	
	
	public double getSaleMarginPer()
	{
		return _saleMarginPer;
	}
	public void setSaleMarginPer(double v)
	{
		this._saleMarginPer = v;
	}
	
	
	public double getTotalMarginPer()
	{
		return _totalMarginPer;
	}
	public void setTotalMarginPer(double v)
	{
		this._totalMarginPer = v;
	}
	
	
	public double getTotalVisitCount()
	{
		return _totalVisitCount;
	}
	public void setTotalVisitCount(double v)
	{
		this._totalVisitCount = v;
	}

	
	public double getAverageOrderSize()
	{
		return _averageOrderSize;
	}
	public void setAverageOrderSize(double v)
	{
		this._averageOrderSize = v;
	}
	
	
	public int getcalendarId() {
		return _calendarId;
	}
	public void setcalendarId(int _calendarId) {
		this._calendarId = _calendarId;
	}
	
	
	public String getstoreNumber() {
		return _storeNumber;
	}
	public void setstoreNumber(String _storeNumber) {
		this._storeNumber = _storeNumber;
	}
}
