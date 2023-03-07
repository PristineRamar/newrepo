package com.pristine.dto.offermgmt.promotion;

import java.text.DecimalFormat;

public class PromoBuyItems implements Cloneable {
	DecimalFormat df = new DecimalFormat("######.##");
	double maxValue = 999.99;
	private long promoDefnId;
	private int itemCode;
	private String upc;
	private double listCost;
	private double dealCost;
	private double offInvoiceCost;
	private double billbackCost;
	private double scanCost;
	private int regQty;
	private double regPrice;
	private double regMPrice;
	private int saleQty;
	private double salePrice;
	private double saleMPrice;
	private int tprQty;
	private double tprPrice;
	private double tprMPrice;
	private int casePack;
	private double offInvoiceCostCase;
	private double billbackCostCase;
	private String displayTypeFlag;
	private int discountRegPct;
	private double discountRegDollar;
	private String isInAd;
	private int actualStartCalId;
	private int actualEndCalId;
	private boolean isPromoInMidOfWeek = false;
	private boolean itemOverlappingInAd = false;
	public long getPromoDefnId() {
		return promoDefnId;
	}
	public void setPromoDefnId(long promoDefnId) {
		this.promoDefnId = promoDefnId;
	}
	public int getItemCode() {
		return itemCode;
	}
	public void setItemCode(int itemCode) {
		this.itemCode = itemCode;
	}
	public double getListCost() {
		if (isMaxValue(listCost))
			return maxValue;
		else
			return listCost;
	}
	public void setListCost(double listCost) {
		this.listCost = listCost;
	}
	public double getDealCost() {
		if (isMaxValue(dealCost))
			return maxValue;
		else
			return dealCost;
	}
	public void setDealCost(double dealCost) {
		this.dealCost = new Double(df.format(dealCost));
	}
	public double getOffInvoiceCost() {
		return offInvoiceCost;
	}
	public void setOffInvoiceCost(double offInvoiceCost) {
		this.offInvoiceCost = offInvoiceCost;
	}
	public double getBillbackCost() {
		return billbackCost;
	}
	public void setBillbackCost(double billbackCost) {
		this.billbackCost = billbackCost;
	}
	public double getScanCost() {
		return scanCost;
	}
	public void setScanCost(double scanCost) {
		this.scanCost = scanCost;
	}
	public int getRegQty() {
		return regQty;
	}
	public void setRegQty(int regQty) {
		this.regQty = regQty;
	}

	public double getRegPrice() {
		if (isMaxValue(regPrice))
			return maxValue;
		else
			return regPrice;
	}
	public void setRegPrice(double regPrice) {
		this.regPrice = regPrice;
	}
	public double getRegMPrice() {
		if (isMaxValue(regMPrice))
			return maxValue;
		else
			return regMPrice;
	}
	public void setRegMPrice(double regMPrice) {
		this.regMPrice = regMPrice;
	}
	public int getSaleQty() {
		return saleQty;
	}
	public void setSaleQty(int saleQty) {
		this.saleQty = saleQty;
	}
	public double getSalePrice() {
		if (isMaxValue(salePrice))
			return maxValue;
		else
			return salePrice;
	}
	public void setSalePrice(double salePrice) {
		this.salePrice = salePrice;
	}
	public double getSaleMPrice() {
		if (isMaxValue(saleMPrice))
			return maxValue;
		else
			return saleMPrice;
	}
	public void setSaleMPrice(double saleMPrice) {
		this.saleMPrice = saleMPrice;
	}
	public int getTprQty() {
		return tprQty;
	}
	public void setTprQty(int tprQty) {
		this.tprQty = tprQty;
	}
	public double getTprPrice() {
		return tprPrice;
	}
	public void setTprPrice(double tprPrice) {
		this.tprPrice = tprPrice;
	}
	public double getTprMPrice() {
		return tprMPrice;
	}
	public void setTprMPrice(double tprMPrice) {
		this.tprMPrice = tprMPrice;
	}

	public int getCasePack() {
		return casePack;
	}

	public void setCasePack(int casePack) {
		this.casePack = casePack;
	}

	public double getOffInvoiceCostCase() {
		return offInvoiceCostCase;
	}

	public void setOffInvoiceCostCase(double offInvoiceCostCase) {
		this.offInvoiceCostCase = offInvoiceCostCase;
	}

	public double getBillbackCostCase() {
		return billbackCostCase;
	}

	public void setBillbackCostCase(double billbackCostCase) {
		this.billbackCostCase = billbackCostCase;
	}

	public String getDisplayTypeFlag() {
		return displayTypeFlag;
	}

	public void setDisplayTypeFlag(String displayTypeFlag) {
		this.displayTypeFlag = displayTypeFlag;
	}

	public int getDiscountRegPct() {
		return discountRegPct;
	}

	public void setDiscountRegPct(int discountRegPct) {
		this.discountRegPct = discountRegPct;
	}

	public double getDiscountRegDollar() {
		return discountRegDollar;
	}

	public void setDiscountRegDollar(double discountRegDollar) {
		this.discountRegDollar = discountRegDollar;
	}

	public int getActualStartCalId() {
		return actualStartCalId;
	}

	public void setActualStartCalId(int actualStartCalId) {
		this.actualStartCalId = actualStartCalId;
	}

	public int getActualEndCalId() {
		return actualEndCalId;
	}

	public void setActualEndCalId(int actualEndCalId) {
		this.actualEndCalId = actualEndCalId;
	}

	public boolean isPromoInMidOfWeek() {
		return isPromoInMidOfWeek;
	}

	public void setPromoInMidOfWeek(boolean isPromoInMidOfWeek) {
		this.isPromoInMidOfWeek = isPromoInMidOfWeek;
	}

	@Override
	public Object clone() throws CloneNotSupportedException {
		return super.clone();
	}

	public String getIsInAd() {
		return isInAd;
	}

	public void setIsInAd(String isInAd) {
		this.isInAd = isInAd;
	}

	private boolean isMaxValue(double inputValue) {
		if (inputValue > maxValue)
			return true;
		else
			return false;
	}

	public String getUpc() {
		return upc;
	}

	public void setUpc(String upc) {
		this.upc = upc;
	}

	public boolean isItemOverlappingInAd() {
		return itemOverlappingInAd;
	}

	public void setItemOverlappingInAd(boolean itemOverlappingInAd) {
		this.itemOverlappingInAd = itemOverlappingInAd;
	}

	@Override
	public int hashCode() {
		final int prime = 31;
		int result = 1;
		result = prime * result + actualEndCalId;
		result = prime * result + actualStartCalId;
		long temp;
		temp = Double.doubleToLongBits(billbackCost);
		result = prime * result + (int) (temp ^ (temp >>> 32));
		temp = Double.doubleToLongBits(billbackCostCase);
		result = prime * result + (int) (temp ^ (temp >>> 32));
		result = prime * result + casePack;
		temp = Double.doubleToLongBits(dealCost);
		result = prime * result + (int) (temp ^ (temp >>> 32));
		result = prime * result + ((df == null) ? 0 : df.hashCode());
		temp = Double.doubleToLongBits(discountRegDollar);
		result = prime * result + (int) (temp ^ (temp >>> 32));
		result = prime * result + discountRegPct;
		result = prime * result + ((displayTypeFlag == null) ? 0 : displayTypeFlag.hashCode());
		result = prime * result + ((isInAd == null) ? 0 : isInAd.hashCode());
		result = prime * result + (isPromoInMidOfWeek ? 1231 : 1237);
		result = prime * result + itemCode;
		result = prime * result + (itemOverlappingInAd ? 1231 : 1237);
		temp = Double.doubleToLongBits(listCost);
		result = prime * result + (int) (temp ^ (temp >>> 32));
		temp = Double.doubleToLongBits(maxValue);
		result = prime * result + (int) (temp ^ (temp >>> 32));
		temp = Double.doubleToLongBits(offInvoiceCost);
		result = prime * result + (int) (temp ^ (temp >>> 32));
		temp = Double.doubleToLongBits(offInvoiceCostCase);
		result = prime * result + (int) (temp ^ (temp >>> 32));
		result = prime * result + (int) (promoDefnId ^ (promoDefnId >>> 32));
		temp = Double.doubleToLongBits(regMPrice);
		result = prime * result + (int) (temp ^ (temp >>> 32));
		temp = Double.doubleToLongBits(regPrice);
		result = prime * result + (int) (temp ^ (temp >>> 32));
		result = prime * result + regQty;
		temp = Double.doubleToLongBits(saleMPrice);
		result = prime * result + (int) (temp ^ (temp >>> 32));
		temp = Double.doubleToLongBits(salePrice);
		result = prime * result + (int) (temp ^ (temp >>> 32));
		result = prime * result + saleQty;
		temp = Double.doubleToLongBits(scanCost);
		result = prime * result + (int) (temp ^ (temp >>> 32));
		temp = Double.doubleToLongBits(tprMPrice);
		result = prime * result + (int) (temp ^ (temp >>> 32));
		temp = Double.doubleToLongBits(tprPrice);
		result = prime * result + (int) (temp ^ (temp >>> 32));
		result = prime * result + tprQty;
		result = prime * result + ((upc == null) ? 0 : upc.hashCode());
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
		PromoBuyItems other = (PromoBuyItems) obj;
		if (actualEndCalId != other.actualEndCalId)
			return false;
		if (actualStartCalId != other.actualStartCalId)
			return false;
		if (Double.doubleToLongBits(billbackCost) != Double.doubleToLongBits(other.billbackCost))
			return false;
		if (Double.doubleToLongBits(billbackCostCase) != Double.doubleToLongBits(other.billbackCostCase))
			return false;
		if (casePack != other.casePack)
			return false;
		if (Double.doubleToLongBits(dealCost) != Double.doubleToLongBits(other.dealCost))
			return false;
		if (df == null) {
			if (other.df != null)
				return false;
		} else if (!df.equals(other.df))
			return false;
		if (Double.doubleToLongBits(discountRegDollar) != Double.doubleToLongBits(other.discountRegDollar))
			return false;
		if (discountRegPct != other.discountRegPct)
			return false;
		if (displayTypeFlag == null) {
			if (other.displayTypeFlag != null)
				return false;
		} else if (!displayTypeFlag.equals(other.displayTypeFlag))
			return false;
		if (isInAd == null) {
			if (other.isInAd != null)
				return false;
		} else if (!isInAd.equals(other.isInAd))
			return false;
		if (isPromoInMidOfWeek != other.isPromoInMidOfWeek)
			return false;
		if (itemCode != other.itemCode)
			return false;
		if (itemOverlappingInAd != other.itemOverlappingInAd)
			return false;
		if (Double.doubleToLongBits(listCost) != Double.doubleToLongBits(other.listCost))
			return false;
		if (Double.doubleToLongBits(maxValue) != Double.doubleToLongBits(other.maxValue))
			return false;
		if (Double.doubleToLongBits(offInvoiceCost) != Double.doubleToLongBits(other.offInvoiceCost))
			return false;
		if (Double.doubleToLongBits(offInvoiceCostCase) != Double.doubleToLongBits(other.offInvoiceCostCase))
			return false;
		if (promoDefnId != other.promoDefnId)
			return false;
		if (Double.doubleToLongBits(regMPrice) != Double.doubleToLongBits(other.regMPrice))
			return false;
		if (Double.doubleToLongBits(regPrice) != Double.doubleToLongBits(other.regPrice))
			return false;
		if (regQty != other.regQty)
			return false;
		if (Double.doubleToLongBits(saleMPrice) != Double.doubleToLongBits(other.saleMPrice))
			return false;
		if (Double.doubleToLongBits(salePrice) != Double.doubleToLongBits(other.salePrice))
			return false;
		if (saleQty != other.saleQty)
			return false;
		if (Double.doubleToLongBits(scanCost) != Double.doubleToLongBits(other.scanCost))
			return false;
		if (Double.doubleToLongBits(tprMPrice) != Double.doubleToLongBits(other.tprMPrice))
			return false;
		if (Double.doubleToLongBits(tprPrice) != Double.doubleToLongBits(other.tprPrice))
			return false;
		if (tprQty != other.tprQty)
			return false;
		if (upc == null) {
			if (other.upc != null)
				return false;
		} else if (!upc.equals(other.upc))
			return false;
		return true;
	}
}
