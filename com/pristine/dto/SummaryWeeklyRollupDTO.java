package com.pristine.dto;

public class SummaryWeeklyRollupDTO extends SummaryWeeklyDTO
{
	public int getDistrictId () { return _DistrictId; }
	public void setDistrictId(int v) { _DistrictId = v; }
	private int _DistrictId = -1;
	
	public int getRegionId () { return _RegionId; }
	public void setRegionId (int v) { _RegionId = v; }
	private int _RegionId = -1;
	
	public int getDivisionId () { return _DivisionId; }
	public void setDivisionId (int v) { _DivisionId = v; }
	private int _DivisionId = -1;
	
}
