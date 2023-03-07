package com.pristine.dto.offermgmt.mwr;

public class MultiWeekPredKey {

	private int itemCode;
	private int regMultiple;
	private double regPrice;
	private int saleMultiple;
	private double salePrice;
	private int adPageNo;
	private int adBlockNo;
	private int displayTypeId;
	private int promoTypeId;
	private int calendarId;
	
	
	public MultiWeekPredKey(int itemCode, int regMultiple, double regPrice, int saleMultiple, double salePrice,
			int adPageNo, int adBlockNo, int displayTypeId, int promoTypeId, int calendarId) {
		
		this.itemCode = itemCode;
		this.regMultiple = regMultiple;
		this.regPrice = regPrice;
		this.saleMultiple = saleMultiple;
		this.salePrice = salePrice;
		this.adPageNo = adPageNo;
		this.adBlockNo = adBlockNo;
		this.displayTypeId = displayTypeId;
		this.promoTypeId = promoTypeId;
		this.calendarId = calendarId;
	}
	
	public int getItemCode() {
		return itemCode;
	}
	public void setItemCode(int itemCode) {
		this.itemCode = itemCode;
	}
	public int getRegMultiple() {
		return regMultiple;
	}
	public void setRegMultiple(int regMultiple) {
		this.regMultiple = regMultiple;
	}
	public double getRegPrice() {
		return regPrice;
	}
	public void setRegPrice(double regPrice) {
		this.regPrice = regPrice;
	}
	public int getSaleMultiple() {
		return saleMultiple;
	}
	public void setSaleMultiple(int saleMultiple) {
		this.saleMultiple = saleMultiple;
	}
	public double getSalePrice() {
		return salePrice;
	}
	public void setSalePrice(double salePrice) {
		this.salePrice = salePrice;
	}
	public int getAdPageNo() {
		return adPageNo;
	}
	public void setAdPageNo(int adPageNo) {
		this.adPageNo = adPageNo;
	}
	public int getAdBlockNo() {
		return adBlockNo;
	}
	public void setAdBlockNo(int adBlockNo) {
		this.adBlockNo = adBlockNo;
	}
	public int getDisplayTypeId() {
		return displayTypeId;
	}
	public void setDisplayTypeId(int displayTypeId) {
		this.displayTypeId = displayTypeId;
	}
	public int getPromoTypeId() {
		return promoTypeId;
	}
	public void setPromoTypeId(int promoTypeId) {
		this.promoTypeId = promoTypeId;
	}
	public int getCalendarId() {
		return calendarId;
	}
	public void setCalendarId(int calendarId) {
		this.calendarId = calendarId;
	}
	@Override
	public int hashCode() {
		final int prime = 31;
		int result = 1;
		result = prime * result + adBlockNo;
		result = prime * result + adPageNo;
		result = prime * result + calendarId;
		result = prime * result + displayTypeId;
		result = prime * result + itemCode;
		result = prime * result + promoTypeId;
		result = prime * result + regMultiple;
		long temp;
		temp = Double.doubleToLongBits(regPrice);
		result = prime * result + (int) (temp ^ (temp >>> 32));
		result = prime * result + saleMultiple;
		temp = Double.doubleToLongBits(salePrice);
		result = prime * result + (int) (temp ^ (temp >>> 32));
		return result;
	}
	@Override
	public boolean equals(Object obj) {
		if (this == obj)
			return true;
		if (obj == null)
			return false;
		if (getClass() != obj.getClass())
			return false;
		MultiWeekPredKey other = (MultiWeekPredKey) obj;
		if (adBlockNo != other.adBlockNo)
			return false;
		if (adPageNo != other.adPageNo)
			return false;
		if (calendarId != other.calendarId)
			return false;
		if (displayTypeId != other.displayTypeId)
			return false;
		if (itemCode != other.itemCode)
			return false;
		if (promoTypeId != other.promoTypeId)
			return false;
		if (regMultiple != other.regMultiple)
			return false;
		if (Double.doubleToLongBits(regPrice) != Double.doubleToLongBits(other.regPrice))
			return false;
		if (saleMultiple != other.saleMultiple)
			return false;
		if (Double.doubleToLongBits(salePrice) != Double.doubleToLongBits(other.salePrice))
			return false;
		return true;
	}
	
}
