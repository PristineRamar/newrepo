package com.pristine.dto;

public class ItemGroupDTO implements IValueObject {
	public int deptId = -1; 
	public String deptName;
	public String deptCode;
	//public boolean useDeptCode=false;
	public int catId = -1; 
	public String catName;
	public String catCode;
	//public boolean useCatCode = false;
	
	public int subCatId = -1; 
	public String subCatName;
	public String subCatCode;
	
	public int segId = -1; 
	public String segmentName;
	public String segmentCode;
	
	public boolean isEmptyDepartment = false;
	public boolean isEmptyCategory = false;
	public boolean isEmptySubCategory = false;
	
	public String toString(){
		StringBuffer sb = new StringBuffer();
		sb.append("Dept - ").append(deptName).append("/").append(deptCode);
		sb.append(", Cat - ").append(catName).append("/").append(catCode);
		sb.append(", Sub Cat - ").append(subCatName).append("/").append(subCatCode);
		sb.append(", Segment - ").append(segmentName).append("/").append(segmentCode);
		
		return sb.toString();
	}
	
	public void clear(){
		deptId = -1; 
		deptName = null;
		deptCode = null;
		
		catId = -1; 
		catName = null;
		catCode = null;
		
		subCatId = -1; 
		subCatName = null;
		subCatCode = null;
		
		segId = -1; 
		segmentName = null;
		segmentCode = null;
	}
	
}
