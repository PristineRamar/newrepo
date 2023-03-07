package com.pristine.dto.offermgmt.promotion;

import com.pristine.dto.offermgmt.ProductKey;

public class PromoAdDetailDTO {
	private ProductKey productKey;
	private int pageNo;
	private int blockNo;
	private String itemNameorLirName;
	private int categoryId;
	private String categoryName;
	private int deptId;
	private String deptName;
	private Integer saleQty;
	private Double salePrice;
	private Double saleMPrice;
	private Integer locationId;
	private Integer locationLevelId;
	private Integer promoTypeID;
	private String UPC;
	private int retLirId;
	
	public ProductKey getProductKey() {
		return productKey;
	}
	public void setProductKey(ProductKey productKey) {
		this.productKey = productKey;
	}
	public int getPageNo() {
		return pageNo;
	}
	public void setPageNo(int pageNo) {
		this.pageNo = pageNo;
	}
	public int getBlockNo() {
		return blockNo;
	}
	public void setBlockNo(int blockNo) {
		this.blockNo = blockNo;
	}
	public String getItemNameorLirName() {
		return itemNameorLirName;
	}
	public void setItemNameorLirName(String itemNameorLirName) {
		this.itemNameorLirName = itemNameorLirName;
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
	public String getDeptName() {
		return deptName;
	}
	public void setDeptName(String deptName) {
		this.deptName = deptName;
	}
	public Integer getSaleQty() {
		return saleQty;
	}
	public void setSaleQty(Integer saleQty) {
		this.saleQty = saleQty;
	}
	public Double getSalePrice() {
		return salePrice;
	}
	public void setSalePrice(Double salePrice) {
		this.salePrice = salePrice;
	}
	public Double getSaleMPrice() {
		return saleMPrice;
	}
	public void setSaleMPrice(Double saleMPrice) {
		this.saleMPrice = saleMPrice;
	}
	public Integer getLocationId() {
		return locationId;
	}
	public void setLocationId(Integer locationId) {
		this.locationId = locationId;
	}
	public Integer getLocationLevelId() {
		return locationLevelId;
	}
	public void setLocationLevelId(Integer locationLevelId) {
		this.locationLevelId = locationLevelId;
	}
	public Integer getPromoTypeID() {
		return promoTypeID;
	}
	public void setPromoTypeID(Integer promoTypeID) {
		this.promoTypeID = promoTypeID;
	}
	public String getUPC() {
		return UPC;
	}
	public void setUPC(String uPC) {
		UPC = uPC;
	}
	public int getRetLirId() {
		return retLirId;
	}
	public void setRetLirId(int retLirId) {
		this.retLirId = retLirId;
	}
}
