package com.pristine.dto.offermgmt;

import java.util.ArrayList;

public class ProductDTO {
	private int categoryId;
	private int portfolioId;
	private int deptId;
	private ArrayList<Integer> productListId = new ArrayList<Integer>();
	
	public int getCategoryId() {
		return categoryId;
	}
	public void setCategoryId(int categoryId) {
		this.categoryId = categoryId;
	}
	public int getPortfolioId() {
		return portfolioId;
	}
	public void setPortfolioId(int portfolioId) {
		this.portfolioId = portfolioId;
	}
	public int getDeptId() {
		return deptId;
	}
	public void setDeptId(int deptId) {
		this.deptId = deptId;
	}
	public ArrayList<Integer> getProductListId() {
		return productListId;
	}
	public void setProductListId(ArrayList<Integer> productListId) {
		this.productListId = productListId;
	}
	
	public void addProductListId(int productListId){
		this.productListId.add(productListId);
	}
}
