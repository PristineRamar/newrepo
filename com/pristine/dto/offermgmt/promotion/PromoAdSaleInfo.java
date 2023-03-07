package com.pristine.dto.offermgmt.promotion;

import com.pristine.dto.offermgmt.MultiplePrice;
import com.pristine.util.offermgmt.PRCommonUtil;

public class PromoAdSaleInfo {

	private MultiplePrice salePrice;
	private MultiplePrice regPrice;
	private int adPage;
	private String adStartDate;
	private double listCost;
	private double dealCost;
	private int promoTypeId;
	private double saleDiscountPCT;
	
	public MultiplePrice getSalePricePoint() {
		return salePrice;
	}
	public void setSalePricePoint(MultiplePrice salePricePoint) {
		this.salePrice = salePricePoint;
	}
	public int getAdPage() {
		return adPage;
	}
	public void setAdPage(int adPage) {
		this.adPage = adPage;
	}
	public String getAdStartDate() {
		return adStartDate;
	}
	public void setAdStartDate(String adStartDate) {
		this.adStartDate = adStartDate;
	}
	public MultiplePrice getRegPricePoint() {
		return regPrice;
	}
	public void setRegPricePoint(MultiplePrice regPricePoint) {
		this.regPrice = regPricePoint;
	}
	public double getListCost() {
		return listCost;
	}
	public void setListCost(double listCost) {
		this.listCost = listCost;
	}
	public double getDealCost() {
		return dealCost;
	}
	public void setDealCost(double dealCost) {
		this.dealCost = dealCost;
	}
	public int getPromoTypeId() {
		return promoTypeId;
	}
	public void setPromoTypeId(int promoTypeId) {
		this.promoTypeId = promoTypeId;
	}
	public double getSaleDiscountPCT() {
		return saleDiscountPCT;
	}
	public void setSaleDiscountPCT(double saleDiscountPCT) {
		this.saleDiscountPCT = saleDiscountPCT;
	}
	
	public double getUnitSalePrice () {
		 return PRCommonUtil.getUnitPrice(this.salePrice, true);
	}
	
	public double getSaleMarPCT() {
		if (getUnitSalePrice() > 0) {
			return ((getUnitSalePrice() - dealCost) / getUnitSalePrice());
		} else {
			return 0;
		}
	}
}
