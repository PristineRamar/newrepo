package com.pristine.dto;

public class ProductDTO {
	
	private int _productLevelId;
	private int _productId;
	private int _childProductLevelId;
	private int _childProductId;

	public int getProductLevelId() {
		return _productLevelId;
	}
	public void setProductLevelId(int v) {
		this._productLevelId = v;
	}
	
	public int getProductId() {
		return _productId;
	}
	public void setProductId(int v) {
		this._productId = v;
	}

	public int getChildProductId() {
		return _childProductId;
	}
	public void setChildProductId(int v) {
		this._childProductId = v;
	}

	public int getChildProductLevelId() {
		return _childProductLevelId;
	}

	public void setChildProductLevelId(int v) {
		this._childProductLevelId = v;
	}
}
