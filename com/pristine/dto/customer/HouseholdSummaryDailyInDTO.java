package com.pristine.dto.customer;

import java.util.HashMap;

public class HouseholdSummaryDailyInDTO 
{
	private int _calendarId;
	private int _storeId;
	private String _storeNo;
	private String _excludeCategory; 
	private int _maxPos; 
	private String _excludePOS;
	private String _targetTable;
	private HashMap<Integer, Integer> _gasItems = new HashMap<Integer, Integer>();
	private HashMap<String, String> _uomLookup = new HashMap<String, String>();
	private HashMap<String, String> _uomConvReq = new HashMap<String, String>();
	
	public int getcalendarId() {
		return _calendarId;
	}
	public void setcalendarId(int _calendarId) {
		this._calendarId = _calendarId;
	}

	public int getStoreId() {
		return _storeId;
	}
	public void setStoreId(int _storeId) {
		this._storeId = _storeId;
	}	

	public String getStoreNo() {
		return _storeNo;
	}
	public void setStoreNo(String _storeNo) {
		this._storeNo = _storeNo;
	}	

	public String getExcludeCategory() {
		return _excludeCategory;
	}
	public void setExcludeCategory(String _excludeCategory) {
		this._excludeCategory = _excludeCategory;
	}		
	
	public int getMaxPos() {
		return _maxPos;
	}
	public void setMaxPos(int _maxPos) {
		this._maxPos = _maxPos;
	}		
	
	public String getExcludePOS() {
		return _excludePOS;
	}
	public void setExcludePOS(String _excludePOS) {
		this._excludePOS = _excludePOS;
	}	

	public String getTargetTable() {
		return _targetTable;
	}
	public void setTargetTable(String _targetTable) {
		this._targetTable = _targetTable;
	}

	public HashMap<Integer, Integer> getGasItems() {
		return _gasItems;
	}
	public void setGasItems(HashMap<Integer, Integer> _gasItems) {
		this._gasItems = _gasItems;
	}

	public HashMap<String, String> getUOMLookup() {
		return _uomLookup;
	}
	public void setUOMLookup(HashMap<String, String> _uomMapDB) {
		this._uomLookup = _uomMapDB;
	}

	public HashMap<String, String> getUOMConvReq() {
		return _uomConvReq;
	}
	public void setUOMConvReq(HashMap<String, String> _uomConvReq) {
		this._uomConvReq = _uomConvReq;
	}
}
