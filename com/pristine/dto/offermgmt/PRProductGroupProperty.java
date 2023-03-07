package com.pristine.dto.offermgmt;

//import java.util.List;

public class PRProductGroupProperty {

	private int productLevelId;
	private int productId;
	private boolean isPerishable;
	private boolean isUsePrediction;
	//private boolean isOrderSellCode;
	
	public PRProductGroupProperty(){
		this.isPerishable = false;
		this.isUsePrediction = true;
		//this.isOrderSellCode = false;
	}

	public int getProductLevelId() {
		return productLevelId;
	}

	public void setProductLevelId(int productLevelId) {
		this.productLevelId = productLevelId;
	}

	public int getProductId() {
		return productId;
	}

	public void setProductId(int productId) {
		this.productId = productId;
	}

	public boolean getIsPerishable() {
		return isPerishable;
	}

	public void setIsPerishable(boolean isPerishable) {
		this.isPerishable = isPerishable;
	}

	public boolean getIsUsePrediction() {
		return isUsePrediction;
	}

	public void setIsUsePrediction(boolean isUsePrediction) {
		this.isUsePrediction = isUsePrediction;
	}

//	public boolean getIsOrderSellCode() {
//		return isOrderSellCode;
//	}
//
//	public void setIsOrderSellCode(boolean isOrderSellCode) {
//		this.isOrderSellCode = isOrderSellCode;
//	}
}
