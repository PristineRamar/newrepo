package com.pristine.dto.offermgmt;

import java.util.ArrayList;
import java.util.List;

public class PROrderCode {

	private String orderCodeDesc = "";
	private List<PRSellCode> sellCodes = new ArrayList<PRSellCode>();
	private Double recommendedRegPrice = 0d;
	private Integer recommendedRegMultiple = 1;
	private int locationLevelId;
	private int locationId;
	private int orderItemCode;
	private int freshRelationHeaderId;

	public String getOrderCodeDesc() {
		return orderCodeDesc;
	}
	public void setOrderCodeDesc(String orderCodeDesc) {
		this.orderCodeDesc = orderCodeDesc;
	}
	public List<PRSellCode> getSellCodes() {
		return sellCodes;
	}
	public void setSellCodes(List<PRSellCode> sellCodes) {
		this.sellCodes = sellCodes;
	}
	public Double getRecommendedRegPrice() {
		return recommendedRegPrice;
	}
	public void setRecommendedRegPrice(Double recommendedRegPrice) {
		this.recommendedRegPrice = recommendedRegPrice;
	}
	public Integer getRecommendedRegMultiple() {
		return recommendedRegMultiple;
	}
	public void setRecommendedRegMultiple(Integer recommendedRegMultiple) {
		this.recommendedRegMultiple = recommendedRegMultiple;
	}
	public int getLocationLevelId() {
		return locationLevelId;
	}
	public void setLocationLevelId(int locationLevelId) {
		this.locationLevelId = locationLevelId;
	}
	public int getLocationId() {
		return locationId;
	}
	public void setLocationId(int locationId) {
		this.locationId = locationId;
	}
	public int getOrderItemCode() {
		return orderItemCode;
	}
	public void setOrderItemCode(int orderItemCode) {
		this.orderItemCode = orderItemCode;
	}
	public int getFreshRelationHeaderId() {
		return freshRelationHeaderId;
	}
	public void setFreshRelationHeaderId(int freshRelationHeaderId) {
		this.freshRelationHeaderId = freshRelationHeaderId;
	} 
	
}
