package com.pristine.dto.salesanalysis;

public class HouseholdDTO 
{
	private int _productLevelId;
	private int _productId;
	private double _totalMovement;
	private double _totalRevenue;
	private double _householdCount;
	private double _averageSpend;
	private double _averageMovement;
	
	public int getProductLevelId()
	{
		return _productLevelId;
	}
	public void setProductLevelId(int v)
	{
		this._productLevelId = v;
	}

	public int getProductId()
	{
		return _productId;
	}
	public void setProductId(int v)
	{
		this._productId = v;
	}	
	
	public double getTotalMovement()
	{
		return _totalMovement;
	}
	public void setTotalMovement(double v)
	{
		this._totalMovement = v;
	}

	public double getTotalRevenue()
	{
		return _totalRevenue;
	}
	public void setTotalRevenue(double v)
	{
		this._totalRevenue = v;
	}

	public double getHouseholdCount()
	{
		return _householdCount;
	}
	public void setHouseholdCount(double v)
	{
		this._householdCount = v;
	}	

	public double getAverageSpend()
	{
		return _averageSpend;
	}
	public void setAverageSpend(double v)
	{
		this._averageSpend = v;
	}		
	
	public double getAverageMovement()
	{
		return _averageMovement;
	}
	public void setAverageMovement(double v)
	{
		this._averageMovement = v;
	}		

}
