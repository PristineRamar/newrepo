package com.pristine.dto.salesanalysis;

import java.util.ArrayList;


public class ProductGroupDTO {
	
	private int _productLevelId;
	private int _childLevelId;
	private ArrayList<ProductDTO> _ProductDTO;
	private ArrayList<ProductGroupDTO> _childProductData;

	private int _aggrRequired;
	
	public int getProductLevelId() {
		return _productLevelId;
	}
	public void setProductLevelId(int v) {
		this._productLevelId = v;
	}

	public int getChildLevelId() {
		return _childLevelId;
	}

	public void setChildLevelId(int v) {
		this._childLevelId = v;
	}
	
	public ArrayList<ProductDTO> getProductData() {
		return _ProductDTO;
	}
	public void setProductData(ArrayList<ProductDTO> v) {
		this._ProductDTO = v;
	}
	
	public int getAggregationRequired() {
		return _aggrRequired;
	}

	public void setAggregationRequired(int v) {
		this._aggrRequired = v;
	}

	public ArrayList<ProductGroupDTO> getChildProductData() {
		return _childProductData;
	}
	public void setChildProductData(ArrayList<ProductGroupDTO> v) {
		this._childProductData = v;
	}
}
