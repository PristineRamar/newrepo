package com.pristine.dto.offermgmt.perishable;

public class PerishableOrderDTO {
    private int refId;
    private int itemCode;
    private String ordercode;
    private String itemDesc;
    private int itemUOM;
    private int itemUPC;
    private double listCost;
    private double itemSize;
    private int ingUOMMajor;
    private int ingUOMMinor;
    private double ingSize;
    private int averageCount;
    private double ingCost;
    private double priceRatioFactor;
    
    public void setRefId(int refId){
    	this.refId = refId;
    	}
    public int getRefId(){
    	return refId;
    	}
    
    public void setItemCode(int itemCode){
    	this.itemCode = itemCode;
    	}
    public int getItemCode(){
    	return itemCode;
    	}

    public void setOrderCode(String ordercode){
    	this.ordercode = ordercode;
    	}
    public String getOrderCode(){
    	return ordercode;
    	}

    public void setItemDesc(String itemDesc){
    	this.itemDesc = itemDesc;
    	}
    public String getItemDesc(){
    	return itemDesc;
    	}

    public void setItemUOM(int itemUOM){
    	this.itemUOM = itemUOM;
    	}
    public int getItemUOM(){
    	return itemUOM;
    	}

    public void setItemUPC(int itemUPC){
    	this.itemUPC = itemUPC;
    	}
    public int getItemUPC(){
    	return itemUPC;
    	}

    public void setListCost(double listCost){
    	this.listCost = listCost;
    	}
    public double getListCost(){
    	return listCost;
    	}

    public void setItemSize(double itemSize){
    	this.itemSize = itemSize;
    	}
    public double getItemSize(){
    	return itemSize;
    	}

    public void setIngUOMMajor(int ingUOMMajor){
    	this.ingUOMMajor = ingUOMMajor;
    	}
    public int getIngUOMMajor(){
    	return ingUOMMajor;
    	}

    public void setIngUOMMinor(int ingUOMMinor){
    	this.ingUOMMinor = ingUOMMinor;
    	}
    public int getIngUOMMinor(){
    	return ingUOMMinor;
    	}

    public void setIngSize(double ingSize){
    	this.ingSize = ingSize;
    	}
    public double getIngSize(){
    	return ingSize;
    	}

    public void setAverageCount(int averageCount){
    	this.averageCount = averageCount;
    	}
    public int getAverageCount(){
    	return averageCount;
    	}

    public void setIngCost(double ingCost){
    	this.ingCost = ingCost;
    	}
    public double getIngCost(){
    	return ingCost;
    	}

    public void setPriceRatioFactor(double priceRatioFactor){
    	this.priceRatioFactor = priceRatioFactor;
    	}
    public double getPriceRatioFactor(){
    	return priceRatioFactor;
    	}    
}