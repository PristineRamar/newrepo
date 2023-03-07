/*
 * To change this template, choose Tools | Templates
 * and open the template in the editor.
 */
package com.pristine.dto;

import java.util.Date;


public class TransactionLogDTO
{
	
	private	int compStoreId=0; 

    public int getCompStoreId() {
        return compStoreId;
    }

    public void setCompStoreId(int compStoreId) {
        this.compStoreId = compStoreId;
    }
	
	private	String compStoreNumber; 

    public String getCompStoreNumber() {
        return compStoreNumber;
    }

    public void setCompStoreNumber(String compStoreNumber) {
        this.compStoreNumber = compStoreNumber;
    }
    
    private	String customerCardNo; 

    public String getCustomerCardNo() {
        return customerCardNo;
    }

    public void setCustomerCardNo(String  customerCardNo) {
        this.customerCardNo = customerCardNo;
    }
    
    private	int customerId=0; 

    public int getCustomerId() {
        return customerId;
    }

    public void setCustomerId(int customerId) {
        this.customerId = customerId;
    }
    
	private	int transactionNo=0; 

    public int getTransactionNo() {
        return transactionNo;
    }

    public void setTransactionNo(int transactionNo) {
        this.transactionNo = transactionNo;
    }
    
    private Date transationTimeStamp;
    
    public Date getTransationTimeStamp() {
        return transationTimeStamp;
    }

    public void setTransationTimeStamp(Date transationTimeStamp) {
        this.transationTimeStamp = transationTimeStamp;
    }

    int calendarId=0;
    
    public void setCalendarId(int calendarId) {
        this.calendarId = calendarId;
    }

	public int getCalendarId() {
		return calendarId;
	}
	
    private String itemUPC;

    public void setItemUPC(String itemUPC) {
        this.itemUPC = itemUPC;
    }
    
    public String getItemUPC() {
        return itemUPC;
    }

    int itemCode =0;
    
    public int getItemCode() {
        return itemCode;
    }

    public void setItemCode(int itemCode) {
        this.itemCode = itemCode;
    }

    
    int posDepartmentId=0;
	
	public int getPOSDepartmentId() {
		return posDepartmentId;
	}

	public void setPOSDepartmentId(int posDepartmentId) {
		this.posDepartmentId = posDepartmentId;
	}

	double netPrice=0;
	
    public double getItemNetPrice() {
        return netPrice;
    }

    public void setItemNetPrice(double netPrice) {
        this.netPrice = netPrice;
    }
	
	double regularPrice=0;
	
    public double getRegularPrice() {
        return regularPrice;
    }

    public void setGrossPrice(double regularPrice) {
        this.regularPrice = regularPrice;
    }
	
    double scannedQuantity =0;
    
    public double getQuantity() {
        return scannedQuantity;
    }

    public void setQuantity(double scannedQuantity) {
        this.scannedQuantity = scannedQuantity;
    }

    double weight =0;
    
    public double getWeight() {
        return weight;
    }

    public void setWeight(double weight) {
        this.weight = weight;
    }

    double extendedNetPrice = 0;
    
    public double getExtendedNetPrice() {
        return extendedNetPrice;
    }

    public void setExtendedNetPrice(double extendedNetPrice) {
        this.extendedNetPrice = extendedNetPrice;
    }
    
    double extendedGrossPrice = 0;
    
    public double getExtendedGrossPrice() {
        return extendedGrossPrice;
    }

    public void setExtendedGrossPrice(double extendedGrossPrice) {
        this.extendedGrossPrice = extendedGrossPrice;
    }

    String storeCpnUsed;
    
    public String getStoreCouponUsed() {
        return storeCpnUsed;
    }

    public void setStoreCouponUsed(String storeCpnUsed) {
        this.storeCpnUsed = storeCpnUsed;
    }    
    
    String mfrCpnUsed;
    
    public String getMfrCouponUsed() {
        return mfrCpnUsed;
    }

    public void setMfrCouponUsed(String mfrCpnUsed) {
        this.mfrCpnUsed = mfrCpnUsed;
    }
    
    int cardDiscountAmt=0;
    
    public int getCardDiscountAmt() {
        return cardDiscountAmt;
    }

    public void setCardDiscountAmt(int cardDiscountAmt) {
        this.cardDiscountAmt = cardDiscountAmt;
    }

    int adDiscountAmt=0;
    
    public int getAdDiscountAmt() {
        return adDiscountAmt;
    }

    public void setAdDiscountAmt(int adDiscountAmt) {
        this.adDiscountAmt = adDiscountAmt;
    }
    
    double otherDiscountAmt=0;
    
    public double getOtherDiscountAmt() {
        return otherDiscountAmt;
    }

    public void setOtherDiscountAmt(double otherDiscountAmt) {
        this.otherDiscountAmt = otherDiscountAmt;
    }
        
    String errorMessage;
    
    public String getErrorMessage() {
        return errorMessage;
    }

    public void setErrorMessage(String errorMessage) {
        this.errorMessage = errorMessage;
    }

    int processRow;
    
    public int getProcessRow() {
        return processRow;
    }

    public void setProcessRow(int processRow) {
        this.processRow = processRow;
    }

	@Override
	public String toString() {
		return "TransactionLogDTO [compStoreId=" + compStoreId + ", compStoreNumber=" + compStoreNumber
				+ ", customerCardNo=" + customerCardNo + ", customerId=" + customerId + ", transactionNo="
				+ transactionNo + ", transationTimeStamp=" + transationTimeStamp + ", calendarId=" + calendarId
				+ ", itemUPC=" + itemUPC + ", itemCode=" + itemCode + ", posDepartmentId=" + posDepartmentId
				+ ", netPrice=" + netPrice + ", regularPrice=" + regularPrice + ", scannedQuantity=" + scannedQuantity
				+ ", weight=" + weight + ", extendedNetPrice=" + extendedNetPrice + ", extendedGrossPrice="
				+ extendedGrossPrice + ", storeCpnUsed=" + storeCpnUsed + ", mfrCpnUsed=" + mfrCpnUsed
				+ ", cardDiscountAmt=" + cardDiscountAmt + ", adDiscountAmt=" + adDiscountAmt + ", otherDiscountAmt="
				+ otherDiscountAmt + ", errorMessage=" + errorMessage + ", processRow=" + processRow
				+ ", getCompStoreId()=" + getCompStoreId() + ", getCompStoreNumber()=" + getCompStoreNumber()
				+ ", getCustomerCardNo()=" + getCustomerCardNo() + ", getCustomerId()=" + getCustomerId()
				+ ", getTransactionNo()=" + getTransactionNo() + ", getTransationTimeStamp()="
				+ getTransationTimeStamp() + ", getCalendarId()=" + getCalendarId() + ", getItemUPC()=" + getItemUPC()
				+ ", getItemCode()=" + getItemCode() + ", getPOSDepartmentId()=" + getPOSDepartmentId()
				+ ", getItemNetPrice()=" + getItemNetPrice() + ", getRegularPrice()=" + getRegularPrice()
				+ ", getQuantity()=" + getQuantity() + ", getWeight()=" + getWeight() + ", getExtendedNetPrice()="
				+ getExtendedNetPrice() + ", getExtendedGrossPrice()=" + getExtendedGrossPrice()
				+ ", getStoreCouponUsed()=" + getStoreCouponUsed() + ", getMfrCouponUsed()=" + getMfrCouponUsed()
				+ ", getCardDiscountAmt()=" + getCardDiscountAmt() + ", getAdDiscountAmt()=" + getAdDiscountAmt()
				+ ", getOtherDiscountAmt()=" + getOtherDiscountAmt() + ", getErrorMessage()=" + getErrorMessage()
				+ ", getProcessRow()=" + getProcessRow() + ", getClass()=" + getClass() + ", hashCode()=" + hashCode()
				+ ", toString()=" + super.toString() + "]";
	}

    
    
    
}
