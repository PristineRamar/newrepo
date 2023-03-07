/*
 * To change this template, choose Tools | Templates
 * and open the template in the editor.
 */
package com.pristine.dto;

import java.util.Date;

/**
 *
 * @author sakthidasan
 */
public class MovementDTO implements Comparable<MovementDTO>
{
    private String itemCompany;
    private String itemStore;
    private String itemTerminal;
    private String itemTransum;
    private int    transactionNo;
    private String itemOperator;
    private Date   itemDateTime;
    private String customerId = "";
    private String itemUPC;
    private double itemNetPrice = 0;
    private double itemGrossPrice = 0;
    private String department;
    private String itemCoupenCurrent;
    private String itemCoupenPrev;
    private String extnMultiPriceGrp;
    private String extnDealQty;
    private String extnPriceMethod;
    private double extnSaleQty = 0;
    private String extnSalePrice;
    private double extnQty = 0;
    private double extnWeight = 0;
    private String WeightedFlag;
    private int weightedCount;
    private String couponUsed;
    private double extendedNetPrice = 0;
    private double extendedProfit;
    private double unitCost = 0;
    private String movementType;
    private String defaultCostUsed;
    private double percentUsed;
    private double unitCostGross = 0;
    private double extendedGrossPrice = 0;
    private double countOnDeal;
    private double misclFundAmt;
    private double misclFundCount;
    private String storeCpnUsed;
    private String mfrCpnUsed;
    private String dayOfWeek;
    private int itemCode;
    private int calendarId;
    private int posDepartment;
    // Adding Tlog columns for Ahold
    private String date;
    private String time;
    
    // Add Tlog for RiteAid
    private String cardNumber = "";
    private String transactionType;
    private String uncode;
    private double cardDiscount=0;
    private double adDiscount=0;
    private double otherDiscount=0;
    private int isRxBasket=0;
    private String adEvent;
    private String adVersion;
    private String adPageId;

    
    public boolean isSaleFlag() {
		return saleFlag;
	}

	public void setSaleFlag(boolean saleFlag) {
		this.saleFlag = saleFlag;
	}

	public String getTranTimeStamp() {
		return tranTimeStamp;
	}

	public void setTranTimeStamp(String tranTimeStamp) {
		this.tranTimeStamp = tranTimeStamp;
	}

	private boolean saleFlag;
    private String tranTimeStamp;

    public String getWeightedFlag() {
        return WeightedFlag;
    }

    public void setWeightedFlag(String WeightedFlag) {
        this.WeightedFlag = WeightedFlag;
    }

    public double getCountOnDeal() {
        return countOnDeal;
    }

    public void setCountOnDeal(double countOnDeal) {
        this.countOnDeal = countOnDeal;
    }

    public String getCouponUsed() {
        return couponUsed;
    }

    public void setCouponUsed(String couponUsed) {
        this.couponUsed = couponUsed;
    }

    public String getCustomerId() {
        return customerId;
    }

    public void setCustomerId(String customerId) {
        this.customerId = customerId;
    }

    public String getDefaultCostUsed() {
        return defaultCostUsed;
    }

    public void setDefaultCostUsed(String defaultCostUsed) {
        this.defaultCostUsed = defaultCostUsed;
    }

    public String getDepartment() {
        return department;
    }

    public void setDepartment(String department) {
        this.department = department;
    }

    public double getExtendedGrossPrice() {
        return extendedGrossPrice;
    }

    public void setExtendedGrossPrice(double extendedGrossPrice) {
        this.extendedGrossPrice = extendedGrossPrice;
    }

    public double getExtendedNetPrice() {
        return extendedNetPrice;
    }

    public void setExtendedNetPrice(double extendedNetPrice) {
        this.extendedNetPrice = extendedNetPrice;
    }

    public double getExtendedProfit() {
        return extendedProfit;
    }

    public void setExtendedProfit(double extendedProfit) {
        this.extendedProfit = extendedProfit;
    }

    public String getExtnDealQty() {
        return extnDealQty;
    }

    public void setExtnDealQty(String extnDealQty) {
        this.extnDealQty = extnDealQty;
    }

    public String getExtnMultiPriceGrp() {
        return extnMultiPriceGrp;
    }

    public void setExtnMultiPriceGrp(String extnMultiPriceGrp) {
        this.extnMultiPriceGrp = extnMultiPriceGrp;
    }

    public String getExtnPriceMethod() {
        return extnPriceMethod;
    }

    public void setExtnPriceMethod(String extnPriceMethod) {
        this.extnPriceMethod = extnPriceMethod;
    }

    public double getExtnQty() {
        return extnQty;
    }

    public void setExtnQty(double extnQty) {
        this.extnQty = extnQty;
    }

    public String getExtnSalePrice() {
        return extnSalePrice;
    }

    public void setExtnSalePrice(String extnSalePrice) {
        this.extnSalePrice = extnSalePrice;
    }

    public double getExtnSaleQty() {
        return extnSaleQty;
    }

    public void setExtnSaleQty(double extnSaleQty) {
        this.extnSaleQty = extnSaleQty;
    }

    public double getExtnWeight() {
        return extnWeight;
    }

    public void setExtnWeight(double extnWeight) {
        this.extnWeight = extnWeight;
    }

    public String getItemCompany() {
        return itemCompany;
    }

    public void setItemCompany(String itemCompany) {
        this.itemCompany = itemCompany;
    }

    public String getItemCoupenCurrent() {
        return itemCoupenCurrent;
    }

    public void setItemCoupenCurrent(String itemCoupenCurrent) {
        this.itemCoupenCurrent = itemCoupenCurrent;
    }

    public String getItemCoupenPrev() {
        return itemCoupenPrev;
    }

    public void setItemCoupenPrev(String itemCoupenPrev) {
        this.itemCoupenPrev = itemCoupenPrev;
    }

    public Date getItemDateTime() {
        return itemDateTime;
    }

    public void setItemDateTime(Date itemDateTime) {
        this.itemDateTime = itemDateTime;
    }

    public String getDayOfWeek() {
        return dayOfWeek;
    }

    public void setDayOfWeek(String day) {
    	dayOfWeek = day;
    }
    public void setDayOfWeek(int day)
    {
    	String dayStr = "";
    	switch ( day )
    	{
    		case 1:  dayStr = "Sunday"; break;
    		case 2:  dayStr = "Monday"; break;
    		case 3:  dayStr = "Tuesday"; break;
    		case 4:  dayStr = "Wednesday"; break;
    		case 5:  dayStr = "Thursday"; break;
    		case 6:  dayStr = "Friday"; break;
    		case 7:  dayStr = "Saturday"; break;
    	}
    	dayOfWeek = dayStr;
    }

    public double getItemGrossPrice() {
        return itemGrossPrice;
    }

    public void setItemGrossPrice(double itemGrossPrice) {
        this.itemGrossPrice = itemGrossPrice;
    }

    public String getItemOperator() {
        return itemOperator;
    }

    public void setItemOperator(String itemOperator) {
        this.itemOperator = itemOperator;
    }

    public double getItemNetPrice() {
        return itemNetPrice;
    }

    public void setItemNetPrice(double itemRegPrice) {
        this.itemNetPrice = itemRegPrice;
    }

    public String getItemStore() {
        return itemStore;
    }

    public void setItemStore(String itemStore) {
        this.itemStore = itemStore;
    }

    public String getItemTerminal() {
        return itemTerminal;
    }

    public void setItemTerminal(String itemTerminal) {
        this.itemTerminal = itemTerminal;
    }

    public String getItemTransum() {
        return itemTransum;
    }

    public void setItemTransum(String itemTransum) {
        this.itemTransum = itemTransum;
    }

    public String getItemUPC() {
        return itemUPC;
    }

    public int getTransactionNo() {
        return transactionNo;
    }

    public void setTransactionNo(int transactionNo) {
        this.transactionNo = transactionNo;
    }
    
    public void setItemUPC(String itemUPC) {
        this.itemUPC = itemUPC;
    }

    public String getMfrCpnUsed() {
        return mfrCpnUsed;
    }

    public void setMfrCpnUsed(String mfrCpnUsed) {
        this.mfrCpnUsed = mfrCpnUsed;
    }

    public double getMisclFundAmt() {
        return misclFundAmt;
    }

    public void setMisclFundAmt(double misclFundAmt) {
        this.misclFundAmt = misclFundAmt;
    }

    public double getMisclFundCount() {
        return misclFundCount;
    }

    public void setMisclFundCount(double misclFundCount) {
        this.misclFundCount = misclFundCount;
    }

    public String getMovementType() {
        return movementType;
    }

    public void setMovementType(String movementType) {
        this.movementType = movementType;
    }

    public double getPercentUsed() {
        return percentUsed;
    }

    public void setPercentUsed(double percentUsed) {
        this.percentUsed = percentUsed;
    }

    public String getStoreCpnUsed() {
        return storeCpnUsed;
    }

    public void setStoreCpnUsed(String storeCpnUsed) {
        this.storeCpnUsed = storeCpnUsed;
    }

    public double getUnitCost() {
        return unitCost;
    }

    public void setUnitCost(double unitCost) {
        this.unitCost = unitCost;
    }

    public double getUnitCostGross() {
        return unitCostGross;
    }

    public void setUnitCostGross(double unitCostGross) {
        this.unitCostGross = unitCostGross;
    }

    public int getWeightedCount() {
        return weightedCount;
    }

    public void setWeightedCount(int weightedCount) {
        this.weightedCount = weightedCount;
    }

    public int getItemCode() {
        return itemCode;
    }

    public void setItemCode(int itemCode) {
        this.itemCode = itemCode;
    }

    public int getCalendarId() {
        return calendarId;
    }

    public void setCalendarId(int calendarId) {
        this.calendarId = calendarId;
    }

	public int getPosDepartment() {
		return posDepartment;
	}

	public void setPosDepartment(int posDepartment) {
		this.posDepartment = posDepartment;
	}

	// Adding Tlog columns for Ahold
	public String getDate() {
		return date;
	}

	public void setDate(String date) {
		this.date = date;
	}

	public String getTime() {
		return time;
	}

	public void setTime(String time) {
		this.time = time;
	}

	
    public String getCardNumber() {
        return cardNumber;
    }

    public void setCardNumber(String cardNumber) {
        this.cardNumber = cardNumber;
    }

    public String getUnCode() {
        return uncode;
    }

    public void setUnCode(String uncode) {
        this.uncode = uncode;
    }
    

    public String getTransactionType() {
        return transactionType;
    }

    public void setTransactionType(String transactionType) {
        this.transactionType = transactionType;
    }

    
    
    
    public double getCardDiscount() {
        return cardDiscount;
    }

    public void setCardDiscount(double cardDiscount) {
        this.cardDiscount = cardDiscount;
    }

    public double getAdDiscount() {
        return adDiscount;
    }

    public void setAdDiscount(double adDiscount) {
        this.adDiscount = adDiscount;
    }

    public double getOtherDiscount() {
        return otherDiscount;
    }

    public void setOtherDiscount(double otherDiscount) {
        this.otherDiscount = otherDiscount;
    }

    public int getRxBasket() {
        return isRxBasket;
    }

    public void setRxBasket(int isRxBasket) {
        this.isRxBasket = isRxBasket;
    }

    public String getAdEvent() {
        return adEvent;
    }

    public void setAdEvent(String adEvent) {
        this.adEvent = adEvent;
    }
    
    public String getAdVersion() {
        return adVersion;
    }

    public void setAdVersion(String adVersion) {
        this.adVersion = adVersion;
    }
    
    public String getAdPageId() {
        return adPageId;
    }

    public void setAdPageId(String adPageId) {
        this.adPageId = adPageId;
    }
    
    @Override
	public int compareTo(MovementDTO m2) {
		// TODO Auto-generated method stub
		if(this.getItemDateTime()==null && m2.getItemDateTime()==null){
			return 0;
		}
		if(this.getItemDateTime() == null){
			return -1;
		}
		if(m2.getItemDateTime() == null){
			return 1;
		}

		if(this.getItemDateTime().equals(m2.getItemDateTime())) {
			return 0;
		}else if(this.getItemDateTime().before(m2.getItemDateTime())){
			return -1;
		}else{
			return 1;
		}
	}
}
