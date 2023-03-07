package com.pristine.dto;

import java.util.ArrayList;

public class ProductGroupDTO {
	
	private int _productLevelId;
	private int _childLevelId;
	private boolean _productAggrStatus = false;
	private ArrayList<ProductDTO> _ProductDTO;
	
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

	public void setProductAggrStatus(boolean v)
	{
		this._productAggrStatus = v;
	}
	public boolean getProductAggrStatus()
	{
		return this._productAggrStatus;
	}
	
	public ArrayList<ProductDTO> getProductData() {
		return _ProductDTO;
	}
	public void setProductData(ArrayList<ProductDTO> v) {
		this._ProductDTO = v;
	}
}
