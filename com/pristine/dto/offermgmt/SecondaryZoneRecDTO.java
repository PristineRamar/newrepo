package com.pristine.dto.offermgmt;

import com.pristine.service.offermgmt.ItemKey;
import com.pristine.util.Constants;
import com.pristine.util.offermgmt.PRConstants;

public class SecondaryZoneRecDTO implements Cloneable{

	private int productLevelId;
	private int productId;
	private int priceZoneId;
	private String priceZoneNo;
	private MultiplePrice currentRegPrice;
	private MultiplePrice recommendedRegPrice;
	private Double listCost;
	private MultiplePrice overrideRegPrice;
	private Double futureListCost;
	
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
	public int getPriceZoneId() {
		return priceZoneId;
	}
	public void setPriceZoneId(int priceZoneId) {
		this.priceZoneId = priceZoneId;
	}
	public String getPriceZoneNo() {
		return priceZoneNo;
	}
	public void setPriceZoneNo(String priceZoneNo) {
		this.priceZoneNo = priceZoneNo;
	}
	public MultiplePrice getCurrentRegPrice() {
		return currentRegPrice;
	}
	public void setCurrentRegPrice(MultiplePrice currentRegPrice) {
		this.currentRegPrice = currentRegPrice;
	}
	public MultiplePrice getRecommendedRegPrice() {
		return recommendedRegPrice;
	}
	public void setRecommendedRegPrice(MultiplePrice recommendedRegPrice) {
		this.recommendedRegPrice = recommendedRegPrice;
	}
	public Double getListCost() {
		return listCost;
	}
	public void setListCost(Double listCost) {
		this.listCost = listCost;
	}
	public MultiplePrice getOverrideRegPrice() {
		return overrideRegPrice;
	}
	public void setOverrideRegPrice(MultiplePrice overrideRegPrice) {
		this.overrideRegPrice = overrideRegPrice;
	}
	
	@Override
	public Object clone() throws CloneNotSupportedException{
		return super.clone();
	}
	
	public ItemKey getItemKey() {
		return new ItemKey(this.productId,
				this.productLevelId == Constants.PRODUCT_LEVEL_ID_LIG ? PRConstants.LIG_ITEM_INDICATOR
						: PRConstants.NON_LIG_ITEM_INDICATOR);	
	}
	public Double getFutureListCost() {
		return futureListCost;
	}
	public void setFutureListCost(Double futureListCost) {
		this.futureListCost = futureListCost;
	}

	
}
