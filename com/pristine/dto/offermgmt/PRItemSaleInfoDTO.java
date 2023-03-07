package com.pristine.dto.offermgmt;

import java.io.Serializable;

import com.pristine.service.offermgmt.prediction.PredictionStatus;

public class PRItemSaleInfoDTO implements Serializable {

	/**
	 * 
	 */
	private static final long serialVersionUID = 9049182817434532299L;
	private MultiplePrice salePrice;
	//private PromoTypeLookup salePromoTypeLookup;
	private String saleStartDate;
	private String saleEndDate;
	private Double salePredMovAtCurReg;
	private Double salePredMovAtCurRegWoSubsEffect;
	private PredictionStatus salePredStatusAtCurReg;
	private Double salePredMovAtRecReg;
	private Double salePredMovAtRecRegWoSubsEffect;
	private PredictionStatus salePredStatusAtRecReg;
	private Integer minQtyReqd;
	private String offerUnitType;
	private Double offerValue;
//	private Double saleCost;
	private String saleWeekStartDate;
	//Added for AZ 
	private int constraint;
	private int promoPriority;
	private int promoTypeId;
	public MultiplePrice getSalePrice() {
		return salePrice;
	}
	public void setSalePrice(MultiplePrice salePrice) {
		this.salePrice = salePrice;
	}

	/*
	 * public PromoTypeLookup getSalePromoTypeLookup() { return salePromoTypeLookup;
	 * } public void setSalePromoTypeLookup(PromoTypeLookup salePromoTypeLookup) {
	 * this.salePromoTypeLookup = salePromoTypeLookup; }
	 */
	public String getSaleStartDate() {
		return saleStartDate;
	}
	public void setSaleStartDate(String saleStartDate) {
		this.saleStartDate = saleStartDate;
	}
	public String getSaleEndDate() {
		return saleEndDate;
	}
	public void setSaleEndDate(String saleEndDate) {
		this.saleEndDate = saleEndDate;
	}
//	public Double getSaleCost() {
//		return saleCost;
//	}
//	public void setSaleCost(Double saleCost) {
//		this.saleCost = saleCost;
//	}
	public String getSaleWeekStartDate() {
		return saleWeekStartDate;
	}
	public void setSaleWeekStartDate(String saleWeekStartDate) {
		this.saleWeekStartDate = saleWeekStartDate;
	}
	public Double getSalePredMovAtCurReg() {
		return salePredMovAtCurReg;
	}
	public void setSalePredMovAtCurReg(Double salePredMovAtCurReg) {
		this.salePredMovAtCurReg = salePredMovAtCurReg;
	}
	public PredictionStatus getSalePredStatusAtCurReg() {
		return salePredStatusAtCurReg;
	}
	public void setSalePredStatusAtCurReg(PredictionStatus salePredStatusAtCurReg) {
		this.salePredStatusAtCurReg = salePredStatusAtCurReg;
	}
	public Double getSalePredMovAtRecReg() {
		return salePredMovAtRecReg;
	}
	public void setSalePredMovAtRecReg(Double salePredMovAtRecReg) {
		this.salePredMovAtRecReg = salePredMovAtRecReg;
	}
	public PredictionStatus getSalePredStatusAtRecReg() {
		return salePredStatusAtRecReg;
	}
	public void setSalePredStatusAtRecReg(PredictionStatus salePredStatusAtRecReg) {
		this.salePredStatusAtRecReg = salePredStatusAtRecReg;
	}
	public Double getSalePredMovAtCurRegWoSubsEffect() {
		return salePredMovAtCurRegWoSubsEffect;
	}
	public void setSalePredMovAtCurRegWoSubsEffect(Double salePredMovAtCurRegWoSubsEffect) {
		this.salePredMovAtCurRegWoSubsEffect = salePredMovAtCurRegWoSubsEffect;
	}
	public Double getSalePredMovAtRecRegWoSubsEffect() {
		return salePredMovAtRecRegWoSubsEffect;
	}
	public void setSalePredMovAtRecRegWoSubsEffect(Double salePredMovAtRecRegWoSubsEffect) {
		this.salePredMovAtRecRegWoSubsEffect = salePredMovAtRecRegWoSubsEffect;
	}
	public Integer getMinQtyReqd() {
		return minQtyReqd;
	}
	public void setMinQtyReqd(Integer minQtyReqd) {
		this.minQtyReqd = minQtyReqd;
	}
	public String getOfferUnitType() {
		return offerUnitType;
	}
	public void setOfferUnitType(String offerUnitType) {
		this.offerUnitType = offerUnitType;
	}
	public Double getOfferValue() {
		return offerValue;
	}
	public void setOfferValue(Double offerValue) {
		this.offerValue = offerValue;
	}
	public int getConstraint() {
		return constraint;
	}
	public void setConstraint(int constraint) {
		this.constraint = constraint;
	}
	public int getPromoPriority() {
		return promoPriority;
	}
	public void setPromoPriority(int promoPriority) {
		this.promoPriority = promoPriority;
	}
	public int getPromoTypeId() {
		return promoTypeId;
	}
	public void setPromoTypeId(int promoTypeId) {
		this.promoTypeId = promoTypeId;
	}
	
	@Override
	public String toString() {
		StringBuilder builder = new StringBuilder();
		builder.append("PRItemSaleInfoDTO [salePrice=");
		builder.append(salePrice);
		builder.append(", saleStartDate=");
		builder.append(saleStartDate);
		builder.append(", saleEndDate=");
		builder.append(saleEndDate);
		builder.append(", salePredMovAtCurReg=");
		builder.append(salePredMovAtCurReg);
		builder.append(", salePredMovAtCurRegWoSubsEffect=");
		builder.append(salePredMovAtCurRegWoSubsEffect);
		builder.append(", salePredStatusAtCurReg=");
		builder.append(salePredStatusAtCurReg);
		builder.append(", salePredMovAtRecReg=");
		builder.append(salePredMovAtRecReg);
		builder.append(", salePredMovAtRecRegWoSubsEffect=");
		builder.append(salePredMovAtRecRegWoSubsEffect);
		builder.append(", salePredStatusAtRecReg=");
		builder.append(salePredStatusAtRecReg);
		builder.append(", minQtyReqd=");
		builder.append(minQtyReqd);
		builder.append(", offerUnitType=");
		builder.append(offerUnitType);
		builder.append(", offerValue=");
		builder.append(offerValue);
		builder.append(", saleWeekStartDate=");
		builder.append(saleWeekStartDate);
		builder.append(", constraint=");
		builder.append(constraint);
		builder.append(", promoPriority=");
		builder.append(promoPriority);
		builder.append(", promoTypeId=");
		builder.append(promoTypeId);
		builder.append("]");
		return builder.toString();
	}
}
