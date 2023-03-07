package com.pristine.dto;

import java.io.Serializable;

public class VariationAnalysisDto implements Serializable {

	private static final long serialVersionUID = 1L;

	private String chainId;
	private String storeId;
	private String compChainName;
	private String storeName;
	private String stateName;
	private String deptId;
	private String deptName;
	private String catId;
	private String catName;
	private String itemCode;
	private String retailItemcode;
	private String itemName;
	private String scheduleId;
	private String scheduleName;

	public String getDeptId() {
		return deptId;
	}

	public void setDeptId(String deptId) {
		this.deptId = deptId;
	}

	public String getDeptName() {
		return deptName;
	}

	public void setDeptName(String deptName) {
		this.deptName = deptName;
	}

	public String getCatId() {
		return catId;
	}

	public void setCatId(String catId) {
		this.catId = catId;
	}

	public String getCatName() {
		return catName;
	}

	public void setCatName(String catName) {
		this.catName = catName;
	}

	public String getItemCode() {
		return itemCode;
	}

	public void setItemCode(String itemCode) {
		this.itemCode = itemCode;
	}

	public String getRetailItemcode() {
		return retailItemcode;
	}

	public void setRetailItemcode(String retailItemcode) {
		this.retailItemcode = retailItemcode;
	}

	public String getItemName() {
		return itemName;
	}

	public void setItemName(String itemName) {
		this.itemName = itemName;
	}

	public String getStateName() {
		return stateName;
	}

	public void setStateName(String stateName) {
		this.stateName = stateName;
	}

	public String getStateCode() {
		return stateCode;
	}

	public void setStateCode(String stateCode) {
		this.stateCode = stateCode;
	}

	private String stateCode;

	public String getChainId() {
		return chainId;
	}

	public void setChainId(String chainId) {
		this.chainId = chainId;
	}

	public String getStoreId() {
		return storeId;
	}

	public void setStoreId(String storeId) {
		this.storeId = storeId;
	}

	public String getCompChainName() {
		return compChainName;
	}

	public void setCompChainName(String compChainName) {
		this.compChainName = compChainName;
	}

	public String getStoreName() {
		return storeName;
	}

	public void setStoreName(String storeName) {
		this.storeName = storeName;
	}

	public String getScheduleId()
	{
		return scheduleId;
	}

	public void setScheduleId(String scheduleId)
	{
		this.scheduleId = scheduleId;
	}

	public String getScheduleName()
	{
		return scheduleName;
	}

	public void setScheduleName(String scheduleName)
	{
		this.scheduleName = scheduleName;
	}

}
