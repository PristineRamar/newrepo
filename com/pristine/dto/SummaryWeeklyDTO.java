package com.pristine.dto;

public class SummaryWeeklyDTO
{
	public int getScheduleId () { return _ScheduleId; }
	public void setScheduleId(int v) { _ScheduleId = v; }
	private int _ScheduleId = -1;
	
	public String getCompStoreNum () { return _CompStoreNum; }
	public void setCompStoreNum(String v) { _CompStoreNum = v; }
	private String _CompStoreNum;
	
	public int getCompStoreId () { return _CompStoreId; }
	public void setCompStoreId(int v) { _CompStoreId = v; }
	private int _CompStoreId = -1;
	
	public int getDeptId () { return _DeptId; }
	public void setDeptId(int v) { _DeptId = v; }
	private int _DeptId = -1;
	
	public int getCatId () { return _CatId; }
	public void setCatId(int v) { _CatId = v; }
	private int _CatId = -1;
	
	public int getSubCatId () { return _SubCatId; }
	public void setSubCatId(int v) { _SubCatId = v; }
	private int _SubCatId = -1;
	
	public int getSegmentId () { return _SegmentId; }
	public void setSegmentId(int v) { _SegmentId = v; }
	private int _SegmentId = -1;
	
	public double getRegularRevenue () { return _RegularRevenue; }
	public void setRegularRevenue(double v) { _RegularRevenue = v; }
	private double _RegularRevenue = 0;
	
	public double getRegularQuantity () { return _RegularQuantity; }
	public void setRegularQuantity(double v) { _RegularQuantity = v; }
	private double _RegularQuantity = 0;
	
	public double getSaleRevenue () { return _SaleRevenue; }
	public void setSaleRevenue(double v) { _SaleRevenue = v; }
	private double _SaleRevenue = 0;
	
	public double getSaleQuantity () { return _SaleQuantity; }
	public void setSaleQuantity(double v) { _SaleQuantity = v; }
	private double _SaleQuantity = 0;

	public double getFinalCost () { return _FinalCost; }
	public void setFinalCost(double v) { _FinalCost = v; }
	private double _FinalCost = 0;
	
	public double getMargin () { return _Margin; }
	public void setMargin(double v) { _Margin = v; }
	private double _Margin = 0;
	
	public int getVisitCount () { return _VisitCount; }
	public void setVisitCount(int v) { _VisitCount = v; }
	private int _VisitCount = 0;
	
	public double getVisitCostAverage () { return _VisitCostAverage; }
	public void setVisitCostAverage(double v) { _VisitCostAverage = v; }
	private double _VisitCostAverage = 0;
	
	public double getTotalRevenue() { return _RegularRevenue + _SaleRevenue; }
	
	public double getMarginPercent()
	{
		double ret = 0;
		//if ( _Margin > 0 )
		{
			double totalRevenue = getTotalRevenue();
			if ( totalRevenue > 0 ) {
				ret = _Margin / totalRevenue * 100; 
			}
		}
		return ret;
	}
}
