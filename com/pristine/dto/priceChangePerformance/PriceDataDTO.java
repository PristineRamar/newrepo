package com.pristine.dto.priceChangePerformance;

public class PriceDataDTO {
	
	private int ProductId;
	private float LY_RegularPrice;
	private float LY_ListCost;
	//private boolean ListCostNull;
	//private boolean RegularPriceNull;
	
	
	/*public boolean isListCostNull() {
		return ListCostNull;
	}
	public void setListCostNull(boolean listCostNull) {
		ListCostNull = listCostNull;
	}
	public boolean isRegularPriceNull() {
		return RegularPriceNull;
	}
	public void setRegularPriceNull(boolean regularPriceNull) {
		RegularPriceNull = regularPriceNull;
	}*/
	public int getProductId() {
		return ProductId;
	}
	public void setProductId(int productId) {
		ProductId = productId;
	}
	public float getLY_RegularPrice() {
		return LY_RegularPrice;
	}
	public void setLY_RegularPrice(float regularPrice) {
		LY_RegularPrice = regularPrice;
	}
	public float getLY_ListCost() {
		return LY_ListCost;
	}
	public void setLY_ListCost(float listCost) {
		LY_ListCost = listCost;
	}
	
	

}
