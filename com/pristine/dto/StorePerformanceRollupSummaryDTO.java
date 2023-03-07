package com.pristine.dto;
 
public class StorePerformanceRollupSummaryDTO
{
	private int _levelID;
	private int _levelTypeID;
	private String _weekEndDate;
	private int _subCategoryID;
	private int _categoryID;
	private int _deptID;
	private int _segmentID;
	private double _revenueRegular;
	private double _revenueSale;
	private double _revenueTotal;
	private double _finalCost;
	private double _margin;
	private double _visitCount;
	private double _visitRevenueAvg;
	private double _marginPCT;

	public int getLevelID () { 
		return _levelID; 
	}
	public void setLevelID (int v) {
		_levelID = v; 
	}
	
	public int getLevelTypeID () {
		return _levelTypeID; 
	}
	public void setLevelTypeID (int v) {
		_levelTypeID = v; 
	}
	
	public int getDeptID () { 
		return _deptID; 
	}
	public void setDeptID (int v) {
		_deptID = v; 
	}

	public int getCategoryID () { 
		return _categoryID; 
	}
	public void setCategoryID (int v) {
		_categoryID = v; 
	}

	public int getSubCategoryID () { 
		return _subCategoryID; 
	}
	public void setSubCategoryID (int v) {
		_subCategoryID = v; 
	}	
	
	public int getSegmentID () { 
		return _segmentID; 
	}
	public void setSegmentID (int v) {
		_segmentID = v; 
	}
	
	public String getWeekEndDate () {
		return _weekEndDate;
	}
	public void setWeekEndDate (String v) {
		_weekEndDate = v;
	}

	public double getRevenueRegular () { 
		return _revenueRegular; 
	}
	public void setRevenueRegular (double v) {
		_revenueRegular = v; 
	}	
	
	public double getRevenueSale () { 
		return _revenueSale; 
	}
	public void setRevenueSale (double v) {
		_revenueSale = v; 
	}	
	
	public double getRevenueTotal () { 
		return _revenueTotal; 
	}
	public void setRevenueTotal (double v) {
		_revenueTotal = v; 
	}	
	
	public double getFinalCost () { 
		return _finalCost; 
	}
	public void setFinalCost (double v) {
		_finalCost = v; 
	}	
	
	public double getMargin () { 
		return _margin; 
	}
	public void setMargin (double v) {
		_margin = v; 
	}	
	
	public double getVisitCount () { 
		return _visitCount; 
	}
	public void setVisitCount (double v) {
		_visitCount = v; 
	}	
	
	public double getVisitRevenueAvg () { 
		return _visitRevenueAvg; 
	}
	public void setVisitRevenueAvg (double v) {
		_visitRevenueAvg = v; 
	}	
	
	public double getMarginPCT () { 
		return _marginPCT; 
	}
	public void setMarginPCT (double v) {
		_marginPCT = v; 
	}	
}