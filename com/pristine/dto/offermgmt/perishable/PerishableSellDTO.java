package com.pristine.dto.offermgmt.perishable;

public class PerishableSellDTO {

    private int refId;
    private int categoryId;
    private int itemCode;
    private String sellCode;
    private String itemDesc;
    private String itemUPC;
    private double typicalPrice;
    private double typicalPriceRatio;
    private double itemYield;
    private int itemUOM;
    private double itemSize;
    private int averageCount;
    private double itemCost;
    
    public void setRefId(int refId){
    	this.refId = refId;
    	}    
    public int getRefId(){
    	return refId;
    	}

    public void setCategoryId(int categoryId){
    	this.categoryId = categoryId;
    	}    
    public int getCategoryId(){
    	return categoryId;
    	}
    
    public void setItemCode(int itemCode){
    	this.itemCode = itemCode;
    	}
    public int getItemCode(){
    	return itemCode;
    	}

    public void setSellCode(String sellCode){
    	this.sellCode = sellCode;
    	}    
    public String getSellCode(){
    	return sellCode;
		}
    
    public void setItemDesc(String itemDesc){
    	this.itemDesc = itemDesc;
		}    
    public String getItemDesc(){
    	return itemDesc;
		}
    
    public void setItemUPC(String itemUPC){
    	this.itemUPC = itemUPC;
		}
    public String getItemUPC(){
    	return itemUPC;
		}
    
    public void setTypicalPrice(double typicalPrice){
    	this.typicalPrice = typicalPrice;
    	}    
    public double getTypicalPrice(){
    	return typicalPrice;
    	}
    
    public void setTypicalPriceRatio(double typicalPriceRatio){
    	this.typicalPriceRatio = typicalPriceRatio;
    	}
    public double getTypicalPriceRatio(){
    	return typicalPriceRatio;
    	}
    
    public void setItemYield(double itemYield){
    	this.itemYield = itemYield;
    	}
    public double getItemYield(){
    	return itemYield;
    	}
    
    public void setItemUOM(int itemUOM){
    	this.itemUOM = itemUOM;
    	}
    public int getItemUOM(){
    	return itemUOM;
    	}
    
    public void setItemSize(double itemSize){
    	this.itemSize = itemSize;
    	}
    public double getItemSize(){
    	return itemSize;
    	}
    
    public void setAverageCount(int averageCount){
    	this.averageCount = averageCount;
    	}
    public int getAverageCount(){
    	return averageCount;
    	}
    
    public void setItemCost(double itemCost){
    	this.itemCost = itemCost;
    	}    
    public double getItemCost(){
    	return itemCost;
    	}
}
