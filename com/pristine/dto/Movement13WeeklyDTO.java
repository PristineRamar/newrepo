package com.pristine.dto;

public class Movement13WeeklyDTO
{
	public int getCompStoreId () { return _CompStoreId; }
	public void setCompStoreId(int v) { _CompStoreId = v; }
	private int _CompStoreId = -1;
	
	public int getItemCode () { return _ItemCode; }
	public void setItemCode(int v) { _ItemCode = v; }
	private int _ItemCode = -1;
	
	public int getRetLIRItemCode () { return _RetLIRItemCode; }
	public void setRetLIRItemCode(int v) { _RetLIRItemCode = v; }
	private int _RetLIRItemCode = 0;

	public int getRetLIRId () { return _RetLIRId; }
	public void setRetLIRId(int v) { _RetLIRId = v; }
	private int _RetLIRId = 0;
	
	public int getCheckDataId () { return _CheckDataId; }
	public void setCheckDataId(int v) { _CheckDataId = v; }
	private int _CheckDataId = -1;
	
	public double getQuantityRegular () { return _QuantityRegular; }
	public void setQuantityRegular(double v) { _QuantityRegular = v; }
	private double _QuantityRegular = 0;	
	
	public double getQuantitySale () { return _QuantitySale; }
	public void setQuantitySale(double v) { _QuantitySale = v; }
	private double _QuantitySale = 0;	
	
	public double getQuantityRegular13Week () { return _QuantityRegular13Week; }
	public void setQuantityRegular13Week (double v) { _QuantityRegular13Week = v; }
	private double _QuantityRegular13Week = 0;
	
	public double getQuantitySale13Week () { return _QuantitySale13Week; }
	public void setQuantitySale13Week (double v) { _QuantitySale13Week = v; }
	private double _QuantitySale13Week = 0;
	
	public double getWeek13RecordCount () { return _week13RecordCount; }
	public void setWeek13RecordCount (double v) { _week13RecordCount = v; }
	private double _week13RecordCount = 0;
	
	// Changes for 12 week movement - Janani
	public double getQuantityRegular12Week () { return _QuantityRegular12Week; }
	public void setQuantityRegular12Week (double v) { _QuantityRegular12Week = v; }
	private double _QuantityRegular12Week = 0;
	
	public double getQuantitySale12Week () { return _QuantitySale12Week; }
	public void setQuantitySale12Week (double v) { _QuantitySale12Week = v; }
	private double _QuantitySale12Week = 0;
	// Changes for 12 week movement Ends
}
