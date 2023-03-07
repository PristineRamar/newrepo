package com.pristine.dto.offermgmt;

import java.io.Serializable;

//public class PRBrandDTO implements Serializable  {
public class PRBrandDTO implements Serializable {
	/**
	 * 
	 */
	private static final long serialVersionUID = 2716711895615215393L;
	private int brandRelnId;
	private int brandId;
	private char brandClass;
	private String brandFamily;
	private String brand;
	private int itemCode;
	private int relatedItemCode;
	private String relatedItemBrandClass;
	
	public char getBrandClass() {
		return brandClass;
	}
	public void setBrandClass(char brandClass) {
		this.brandClass = brandClass;
	}
	public String getBrandFamily() {
		return brandFamily;
	}
	public void setBrandFamily(String brandFamily) {
		this.brandFamily = brandFamily;
	}
	public String getBrand() {
		return brand;
	}
	public void setBrand(String brand) {
		this.brand = brand;
	}
	public int getItemCode() {
		return itemCode;
	}
	public void setItemCode(int itemCode) {
		this.itemCode = itemCode;
	}
	public int getRelatedItemCode() {
		return relatedItemCode;
	}
	public void setRelatedItemCode(int relatedItemCode) {
		this.relatedItemCode = relatedItemCode;
	}
	public int getBrandRelnId() {
		return brandRelnId;
	}
	public void setBrandRelnId(int brandRelnId) {
		this.brandRelnId = brandRelnId;
	}
	public int getBrandId() {
		return brandId;
	}
	public void setBrandId(int brandId) {
		this.brandId = brandId;
	}
	public String getRelatedItemBrandClass() {
		return relatedItemBrandClass;
	}
	public void setRelatedItemBrandClass(String relatedItemBrandClass) {
		this.relatedItemBrandClass = relatedItemBrandClass;
	}
	
	public void copySelfInfo(PRBrandDTO brandDTO){
		this.brandId = brandDTO.getBrandId();
		this.brandClass = brandDTO.getBrandClass();
		this.brandFamily = brandDTO.getBrandFamily();
		this.brand = brandDTO.getBrand();
	}
	
	public void copyRelatedInfo(PRBrandDTO brandDTO){
		this.brandRelnId = brandDTO.getBrandRelnId();
		this.brandId = brandDTO.getBrandId();
		this.brandClass = brandDTO.getBrandClass();
		this.brandFamily = brandDTO.getBrandFamily();
		this.brand = brandDTO.getBrand();
		this.itemCode = brandDTO.getItemCode();
		this.relatedItemCode = brandDTO.getRelatedItemCode();
		this.relatedItemBrandClass = brandDTO.getRelatedItemBrandClass();
	}
}