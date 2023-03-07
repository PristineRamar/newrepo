package com.pristine.dto.salesanalysis;

public class SummaryDataDTO 
{
	private String _storeNumber;
	private int _calendarId;
	private int _locationLevelId;
	private int _locationId;
	private int _productLevelId;
	private String _productId;
	private double _regularRevenue;
	private double _saleRevenue;
	private double _totalRevenue;
	private double _regularMargin;
	private double _saleMargin;
	private double _totalMargin;
	private double _regularMarginPer;
	private double _saleMarginPer;
	private double _totalMarginPer;
	
	private double _regularMovement;
	private double _saleMovement;
	private double _totalMovement;

	private double _regMovementVolume;
	private double _saleMovementVolume;
	private double _totMovementVolume;

	private double _totalVisitCount;
	private double _averageOrderSize;
	
	private double _plRegularMovement;
	private double _plSaleMovement;
	private double _plTotalMovement;

	private double _plRegMovementVolume;
	private double _plSaleMovementVolume;
	private double _plTotMovementVolume;
	
	private double _plRegularRevenue;
	private double _plSaleRevenue;
	private double _plTotalRevenue;
	
	private double _plRegularMargin;
	private double _plSaleMargin;
	private double _plTotalMargin;
	
	private double _plRegularMarginPer;
	private double _plSaleMarginPer;
	private double _plTotalMarginPer;
	
	private double _plTotalVisitCount;
	private double _plAverageOrderSize;
	
	private int _summaryCtdId;
	private int _lastAggrCalendarId;
	private long _lastAggrSalesId;
	private int _customerId;
	private int _transactionNo;
	
	// add the weekDtonew dto for identical aggergation;
	
	private double _iregularMovement;
	private double _isaleMovement;
	private double _itotalMovement;

	private double _idRegMovementVolume;
	private double _idSaleMovementVolume;
	private double _idTotMovementVolume;
	
	private double _iregularRevenue;
	private double _isaleRevenue;
	private double _itotalRevenue;
	
	private double _iregularMargin;
	private double _isaleMargin;
	private double _itotalMargin;
	
	private double _iregularMarginPer;
	private double _isaleMarginPer;
	private double _itotalMarginPer;
	
	private double _itotalVisitCount;
	private double _iaverageOrderSize;

	private double _pliregularMovement;
	private double _plisaleMovement;
	private double _plitotalMovement;

	private double _plidRegMovementVolume;
	private double _plidSaleMovementVolume;
	private double _plidTotMovementVolume;
	
	private double _pliregularRevenue;
	private double _plisaleRevenue;
	private double _plitotalRevenue;
	
	private double _pliregularMargin;
	private double _plisaleMargin;
	private double _plitotalMargin;
	
	private double _pliregularMarginPer;
	private double _plisaleMarginPer;
	private double _plitotalMarginPer;
	
	private double _plitotalVisitCount;
	private double _pliaverageOrderSize;	

	// for calendar Actual number
	private int _actualNo;
	
	
	// for accounts total revenue
	private double adjTotRevenue;
	private double adjIdTotRevenue;
	
	
	// Property added for Movement by volume
	private double _igRegVolumeRev;
	private double _igSaleVolumeRev;
	private double _igtotVolumeRev;
	private double _idIgRegVolumeRev;
	private double _idIgSaleVolumeRev;
	private double _IdIgtotVolumeRev;
		
	// property added for store state
	private String _storeState;
	
	// property added for deal cost
	//private double dealCost = 0;
	private double regularDealCost = 0;
	private double saleDealCost = 0;
	private double plregularDealCost = 0;
	private double plsaleDealCost = 0;

	private String _storeOpenDate;
	
	// Property added for store type
	private String storeType;
	
	
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

	public double getSaleMovement()
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

	public double getregMovementVolume() {
		return _regMovementVolume;
	}
	public void setregMovementVolume(double v) {
		this._regMovementVolume = v;
	}	
	
	public double getsaleMovementVolume() {
		return _saleMovementVolume;
	}
	public void setsaleMovementVolume(double v) {
		this._saleMovementVolume = v;
	}
	
	public double gettotMovementVolume() {
		return _totMovementVolume;
	}
	public void settotMovementVolume(double v) {
		this._totMovementVolume = v;
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
	
	public double getPLRegularMovement()
	{
		return _plRegularMovement;
	}
	public void setPLRegularMovement(double v)
	{
		this._plRegularMovement = v;
	}

	public double getPLSaleMovement()
	{
		return _plSaleMovement;
	}
	public void setPLSaleMovement(double v)
	{
		this._plSaleMovement = v;
	}
	
	public double getPLTotalMovement()
	{
		return _plTotalMovement;
	}
	public void setPLTotalMovement(double v)
	{
		this._plTotalMovement = v;
	}

	public double getPLregMovementVolume() {
		return _plRegMovementVolume;
	}
	public void setPLregMovementVolume(double v) {
		this._plRegMovementVolume = v;
	}	
	
	public double getPLsaleMovementVolume() {
		return _plSaleMovementVolume;
	}
	public void setPLsaleMovementVolume(double v) {
		this._plSaleMovementVolume = v;
	}
	
	public double getPLtotMovementVolume() {
		return _plTotMovementVolume;
	}
	public void setPLtotMovementVolume(double v) {
		this._plTotMovementVolume = v;
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
	
	public double getPLTotalRevenue()
	{
		return _plTotalRevenue;
	}
	public void setPLTotalRevenue(double v)
	{
		this._plTotalRevenue = v;
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
	
	public double getPLTotalMargin()
	{
		return _plTotalMargin;
	}
	public void setPLTotalMargin(double v)
	{
		this._plTotalMargin = v;
	}
	
	public double getPLRegularMarginPer()
	{
		return _plRegularMarginPer;
	}
	public void setPLRegularMarginPer(double v)
	{
		this._plRegularMarginPer = v;
	}
	
	public double getPLSaleMarginPer()
	{
		return _plSaleMarginPer;
	}
	public void setPLSaleMarginPer(double v)
	{
		this._plSaleMarginPer = v;
	}
	
	public double getPLTotalMarginPer()
	{
		return _plTotalMarginPer;
	}
	public void setPLTotalMarginPer(double v)
	{
		this._plTotalMarginPer = v;
	}
	
	public double getPLTotalVisitCount()
	{
		return _plTotalVisitCount;
	}
	public void setPLTotalVisitCount(double v)
	{
		this._plTotalVisitCount = v;
	}

	public double getPLAverageOrderSize()
	{
		return _plAverageOrderSize;
	}
	public void setPLAverageOrderSize(double v)
	{
		this._plAverageOrderSize = v;
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
	
	public int getsummaryCtdId() {
		return _summaryCtdId;
	}
	public void setsummaryCtdId(int _summaryCtdId) {
		this._summaryCtdId = _summaryCtdId;
	}

	public void setlastAggrSalesId(long _lastAggrSalesId) {
		this._lastAggrSalesId = _lastAggrSalesId;
	}
	public long getlastAggrSalesId() {
		return _lastAggrSalesId;
	}
	public void setlastAggrCalendarId(int _lastAggrCalendarId) {
		this._lastAggrCalendarId = _lastAggrCalendarId;
	}
	public int getlastAggrCalendarId() {
		return _lastAggrCalendarId;
	}
	
	public double getiregularMovement() {
		return _iregularMovement;
	}
	public void setiregularMovement(double _iregularMovement) {
		this._iregularMovement = _iregularMovement;
	}
	
	public double getisaleMovement() {
		return _isaleMovement;
	}
	public void setisaleMovement(double _isaleMovement) {
		this._isaleMovement = _isaleMovement;
	}
	
	public double getitotalMovement() {
		return _itotalMovement;
	}
	public void setitotalMovement(double _itotalMovement) {
		this._itotalMovement = _itotalMovement;
	}

	public double getidRegMovementVolume() {
		return _idRegMovementVolume;
	}
	public void setidRegMovementVolume(double _idRegMovementVolume) {
		this._idRegMovementVolume = _idRegMovementVolume;
	}
	public double getidSaleMovementVolume() {
		return _idSaleMovementVolume;
	}
	public void setidSaleMovementVolume(double _idSaleMovementVolume) {
		this._idSaleMovementVolume = _idSaleMovementVolume;
	}
	public double getidTotMovementVolume() {
		return _idTotMovementVolume;
	}
	public void setidTotMovementVolume(double _idTotMovementVolume) {
		this._idTotMovementVolume = _idTotMovementVolume;
	}
	
	public double getiregularRevenue() {
		return _iregularRevenue;
	}
	public void setiregularRevenue(double _iregularRevenue) {
		this._iregularRevenue = _iregularRevenue;
	}
	
	public double getisaleRevenue() {
		return _isaleRevenue;
	}
	public void setisaleRevenue(double _isaleRevenue) {
		this._isaleRevenue = _isaleRevenue;
	}
	
	public double getitotalRevenue() {
		return _itotalRevenue;
	}
	public void setitotalRevenue(double _itotalRevenue) {
		this._itotalRevenue = _itotalRevenue;
	}
	
	public double getiregularMargin() {
		return _iregularMargin;
	}
	public void setiregularMargin(double _iregularMargin) {
		this._iregularMargin = _iregularMargin;
	}
	
	public double getisaleMargin() {
		return _isaleMargin;
	}
	public void setisaleMargin(double _isaleMargin) {
		this._isaleMargin = _isaleMargin;
	}
	
	public double getitotalMargin() {
		return _itotalMargin;
	}
	public void setitotalMargin(double _itotalMargin) {
		this._itotalMargin = _itotalMargin;
	}
	
	public double getiregularMarginPer() {
		return _iregularMarginPer;
	}
	public void setiregularMarginPer(double _iregularMarginPer) {
		this._iregularMarginPer = _iregularMarginPer;
	}
	
	public double getisaleMarginPer() {
		return _isaleMarginPer;
	}
	public void setisaleMarginPer(double _isaleMarginPer) {
		this._isaleMarginPer = _isaleMarginPer;
	}
	
	public double getitotalMarginPer() {
		return _itotalMarginPer;
	}
	public void setitotalMarginPer(double _itotalMarginPer) {
		this._itotalMarginPer = _itotalMarginPer;
	}
	
	public double getiaverageOrderSize() {
		return _iaverageOrderSize;
	}
	public void setiaverageOrderSize(double _iaverageOrderSize) {
		this._iaverageOrderSize = _iaverageOrderSize;
	}
	
	public double getitotalVisitCount() {
		return _itotalVisitCount;
	}
	public void setitotalVisitCount(double _itotalVisitCount) {
		this._itotalVisitCount = _itotalVisitCount;
	}
	
	public double getPLiregularMovement() {
		return _pliregularMovement;
	}
	public void setPLiregularMovement(double _v) {
		this._pliregularMovement = _v;
	}
	
	public double getPLisaleMovement() {
		return _plisaleMovement;
	}
	public void setPLisaleMovement(double _v) {
		this._plisaleMovement = _v;
	}
	
	public double getPLitotalMovement() {
		return _plitotalMovement;
	}
	public void setPLitotalMovement(double _v) {
		this._plitotalMovement = _v;
	}

	public double getPLidRegMovementVolume() {
		return _plidRegMovementVolume;
	}
	public void setPLidRegMovementVolume(double _v) {
		this._plidRegMovementVolume = _v;
	}
	public double getPLidSaleMovementVolume() {
		return _plidSaleMovementVolume;
	}
	public void setPLidSaleMovementVolume(double _v) {
		this._plidSaleMovementVolume = _v;
	}
	public double getPLidTotMovementVolume() {
		return _plidTotMovementVolume;
	}
	public void setPLidTotMovementVolume(double _v) {
		this._plidTotMovementVolume = _v;
	}
	
	public double getPLiregularRevenue() {
		return _pliregularRevenue;
	}
	public void setPLiregularRevenue(double _v) {
		this._pliregularRevenue = _v;
	}
	
	public double getPLisaleRevenue() {
		return _plisaleRevenue;
	}
	public void setPLisaleRevenue(double _v) {
		this._plisaleRevenue = _v;
	}
	
	public double getPLitotalRevenue() {
		return _plitotalRevenue;
	}
	public void setPLitotalRevenue(double _v) {
		this._plitotalRevenue = _v;
	}
	
	public double getPLiregularMargin() {
		return _pliregularMargin;
	}
	public void setPLiregularMargin(double _v) {
		this._pliregularMargin = _v;
	}
	
	public double getPLisaleMargin() {
		return _plisaleMargin;
	}
	public void setPLisaleMargin(double _v) {
		this._plisaleMargin = _v;
	}
	
	public double getPLitotalMargin() {
		return _plitotalMargin;
	}
	public void setPLitotalMargin(double _v) {
		this._plitotalMargin = _v;
	}
	
	public double getPLiregularMarginPer() {
		return _pliregularMarginPer;
	}
	public void setPLiregularMarginPer(double _v) {
		this._pliregularMarginPer = _v;
	}
	
	public double getPLisaleMarginPer() {
		return _plisaleMarginPer;
	}
	public void setPLisaleMarginPer(double _v) {
		this._plisaleMarginPer = _v;
	}
	
	public double getPLitotalMarginPer() {
		return _plitotalMarginPer;
	}
	public void setPLitotalMarginPer(double _v) {
		this._plitotalMarginPer = _v;
	}
	
	public double getPLiaverageOrderSize() {
		return _pliaverageOrderSize;
	}
	public void setPLiaverageOrderSize(double _v) {
		this._pliaverageOrderSize = _v;
	}
	
	public double getPLitotalVisitCount() {
		return _plitotalVisitCount;
	}
	public void setPLitotalVisitCount(double _v) {
		this._plitotalVisitCount = _v;
	}
	
	public void setactualNo(int _actualNo) {
		this._actualNo = _actualNo;
	}
	public int getactualNo() {
		return _actualNo;
	}
	public double getAdjTotRevenue() {
		return adjTotRevenue;
	}
	public void setAdjTotRevenue(double adjTotRevenue) {
		this.adjTotRevenue = adjTotRevenue;
	}
	public double getAdjIdTotRevenue() {
		return adjIdTotRevenue;
	}
	public void setAdjIdTotRevenue(double adjIdTotRevenue) {
		this.adjIdTotRevenue = adjIdTotRevenue;
	}
	
	public double getigRegVolumeRev() {
		return _igRegVolumeRev;
	}
	public void setigRegVolumeRev(double _igRegVolumeRev) {
		this._igRegVolumeRev = _igRegVolumeRev;
	}
	public double getigSaleVolumeRev() {
		return _igSaleVolumeRev;
	}
	public void setigSaleVolumeRev(double _igSaleVolumeRev) {
		this._igSaleVolumeRev = _igSaleVolumeRev;
	}
	public double getigtotVolumeRev() {
		return _igtotVolumeRev;
	}
	public void setigtotVolumeRev(double _igtotVolumeRev) {
		this._igtotVolumeRev = _igtotVolumeRev;
	}
	public double getIdIgtotVolumeRev() {
		return _IdIgtotVolumeRev;
	}
	public void setIdIgtotVolumeRev(double _IdIgtotVolumeRev) {
		this._IdIgtotVolumeRev = _IdIgtotVolumeRev;
	}

	public double getidIgRegVolumeRev() {
		return _idIgRegVolumeRev;
	}
	public void setidIgRegVolumeRev(double _idIgRegVolumeRev) {
		this._idIgRegVolumeRev = _idIgRegVolumeRev;
	}
	public double getidIgSaleVolumeRev() {
		return _idIgSaleVolumeRev;
	}
	public void setidIgSaleVolumeRev(double _idIgSaleVolumeRev) {
		this._idIgSaleVolumeRev = _idIgSaleVolumeRev;
	}
	public String getstoreState() {
		return _storeState;
	}
	public void setstoreState(String _storeState) {
		this._storeState = _storeState;
	}
//	public double getDealCost() {
//		return dealCost;
//	}
//	public void setDealCost(double dealCost) {
//		this.dealCost = dealCost;
//	}
	public String getStoreOpenDate() {
		return _storeOpenDate;
	}
	public void setStoreOpenDate(String _storeOpenDate) {
		this._storeOpenDate = _storeOpenDate;
	}
	public String getStoreStatus() {
		return storeType;
	}
	public void setStoreStatus(String storeStatus) {
		this.storeType = storeStatus;
	}
	
	public double getRegularDealCost() {
		return regularDealCost;
	}
	public void setRegularDealCost(double regularDealCost) {
		this.regularDealCost = regularDealCost;
	}
	
	public double getSaleDealCost() {
		return saleDealCost;
	}
	public void setSaleDealCost(double saleDealCost) {
		this.saleDealCost = saleDealCost;
	}

	public double getPLRegularDealCost() {
		return plregularDealCost;
	}
	public void setPLRegularDealCost(double _v) {
		this.plregularDealCost = _v;
	}
	
	public double getPLSaleDealCost() {
		return plsaleDealCost;
	}
	public void setPLSaleDealCost(double _v) {
		this.plsaleDealCost = _v;
	}
	
	public int getCustomerId() {
		return _customerId;
	}
	public void setCustomerId(int v) {
		_customerId = v;
	}

	public int getTransactionNo() {
		return _transactionNo;
	}
	public void setTransactionNo(int v) {
		_transactionNo = v;
	}	
}
