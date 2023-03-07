package com.pristine.dto.offermgmt.promotion;

import com.pristine.dto.offermgmt.ProductKey;


public class PPGGroupDTO {
	private long groupId;
	private ProductKey productKey;
	private boolean isLeadItem;
	private int brandId;
	private String brandName;
	private int categoryId;
	private String categoryName;
	private int deptId;
	private String deptName;
	private int itemCode;
	private String itemName;
	
	public long getGroupId() {
		return groupId;
	}
	public void setGroupId(long groupId) {
		this.groupId = groupId;
	}
	public ProductKey getProductKey() {
		return productKey;
	}
	public void setProductKey(ProductKey productKey) {
		this.productKey = productKey;
	}
	public boolean isLeadItem() {
		return isLeadItem;
	}
	public void setLeadItem(boolean isLeadItem) {
		this.isLeadItem = isLeadItem;
	}
	public int getBrandId() {
		return brandId;
	}
	public void setBrandId(int brandId) {
		this.brandId = brandId;
	}
	public String getBrandName() {
		return brandName;
	}
	public void setBrandName(String brandName) {
		this.brandName = brandName;
	}
	public int getCategoryId() {
		return categoryId;
	}
	public void setCategoryId(int categoryId) {
		this.categoryId = categoryId;
	}
	public String getCategoryName() {
		return categoryName;
	}
	public void setCategoryName(String categoryName) {
		this.categoryName = categoryName;
	}
	public int getDeptId() {
		return deptId;
	}
	public void setDeptId(int deptId) {
		this.deptId = deptId;
	}
	public int getItemCode() {
		return itemCode;
	}
	public void setItemCode(int itemCode) {
		this.itemCode = itemCode;
	}
	public String getDeptName() {
		return deptName;
	}
	public void setDeptName(String deptName) {
		this.deptName = deptName;
	}
	public String getItemName() {
		return itemName;
	}
	public void setItemName(String itemName) {
		this.itemName = itemName;
	}
}
