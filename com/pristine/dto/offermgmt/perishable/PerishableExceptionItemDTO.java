package com.pristine.dto.offermgmt.perishable;

public class PerishableExceptionItemDTO {
    private int locationLevelId;
    private int locationId;
	private int orderItemCode;
    private int sellItemCode;
    private int reason; // 1: Ingredient cost not found, 2: UOM Mismatch
    
    public void setOrderItemCode(int orderItemCode){
    	this.orderItemCode = orderItemCode;
    	}
    public int getOrderItemCode(){
    	return orderItemCode;
    	}

    public void setSellItemCode(int sellItemCode){
    	this.sellItemCode = sellItemCode;
    	}
    public int getSellItemCode(){
    	return sellItemCode;
    	}
    
    public void setReason(int reason){
    	this.reason = reason;
    	}
    public int getReason(){
    	return reason;
    	}
    
    public void setLocationLevelId(int locationLevelId){
    	this.locationLevelId = locationLevelId;
    	}
    public int getLocationlevelId(){
    	return locationLevelId;
    	}

    public void setLocationId(int locationId){
    	this.locationId = locationId;
    	}
    public int getLocationId(){
    	return locationId;
    	}

}
