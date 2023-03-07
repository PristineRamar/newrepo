package com.pristine.dto.salesanalysis;

public class ProductDTO {
	
	private int _productLevelId;
	private int _productId;
	private int _childProductLevelId;
	private int _childProductId;
	private String productName;
	// Loading Product Group Tables - Price Index Portfolio Support
	private String productCode;

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
	public String getProductName() {
		return productName;
	}
	public void setProductName(String productName) {
		this.productName = productName;
	}
	
	// Loading Product Group Tables - Price Index Portfolio Support
	public String getProductCode() {
		return productCode;
	}
	public void setProductCode(String productCode) {
		this.productCode = productCode;
	}
	// Loading Product Group Tables - Price Index Portfolio Support - Ends
}
