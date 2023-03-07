package com.pristine.dto.salesanalysis;

import java.util.HashMap;

public class SpecialCriteriaDTO {

	public String getExcludeDepartments () { return _excludeDepartments; }
	public void setExcludeDepartments(String v) { _excludeDepartments = v; }
	private String _excludeDepartments;
	
	public String getIncludeItems () { return _includeItems; }
	public void setIncludeItems(String v) { _includeItems = v; }
	private String _includeItems;
	
	public HashMap<String, String> getExcludeDepartmentMap() {
		return _excludeDepartmentMap;
	}
	public void setExcludeDepartmentMap(HashMap<String, String> _excludeDepartmentMap) {
		this._excludeDepartmentMap = _excludeDepartmentMap;
	}
	private HashMap<String, String> _excludeDepartmentMap;
	
		
	public HashMap<String, String> getIncludeItemMap() {
		return _includeItemMap;
	}
	public void setIncludeItemMap(HashMap<String, String> _includeItemMap) {
		this._includeItemMap = _includeItemMap;
	}
	
	private HashMap<String, String> _includeItemMap;
	
	public HashMap<String, String> getGasDetailsMap() {
		return _gasDetailsMap;
	}
	public void setGasDetailsMap(HashMap<String, String> _gasDetailsMap) {
		this._gasDetailsMap = _gasDetailsMap;
	}
	private HashMap<String, String> _gasDetailsMap;
	
	
	public int getGasDeptId () { return _gasDeptId; }
	public void setGasDeptId(int v) { _gasDeptId = v; }
	private int _gasDeptId;
			
	public String getIncludeGas() {	return this.includeGas;}
	public void setIncludeGas(String includeGas) {	this.includeGas = includeGas;}
	private  String includeGas;
	
	
	// property added for coupon based pos department
	public int getcouponPosId() {return _couponPosId;}
	public void setcouponPosId(int _couponPosId) {this._couponPosId = _couponPosId;}
	private int _couponPosId = 0;
	
	// for adding for store based gas value
	public HashMap<String, Double> getstoreBasedGasValue() {
		return _storeBasedGasValue;
	}
	public void setstoreBasedGasValue(HashMap<String, Double> storeBasedGasValue) {
		this._storeBasedGasValue = storeBasedGasValue;
	}
	private HashMap<String, Double> _storeBasedGasValue;
	
	
}
