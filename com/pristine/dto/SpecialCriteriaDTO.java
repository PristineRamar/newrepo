package com.pristine.dto;

public class SpecialCriteriaDTO {

	public String getExcludeDepartments () { return _excludeDepartments; }
	public void setExcludeDepartments(String v) { _excludeDepartments = v; }
	private String _excludeDepartments;
	
	public String getIncludeItems () { return _includeItems; }
	public void setIncludeItems(String v) { _includeItems = v; }
	private String _includeItems;
	
}
