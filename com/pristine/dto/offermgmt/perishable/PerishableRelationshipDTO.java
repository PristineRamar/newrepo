package com.pristine.dto.offermgmt.perishable;

import java.util.List;


public class PerishableRelationshipDTO {
    private int freshProductHeaderId;
    private int locationLevelId;
    private int locationId;
    private int relationStartCalId;
    private int relationEndCalId;
    private int freshRelationRefId;
    private String relationStartDate;
    private String relationEndDate;
    private double shrink;
    private double additionalCost;
    private double laborCost;
    private List<PerishableOrderDTO> orderData = null;
    private List<PerishableSellDTO> sellData = null;
    
    public void setFreshProductHeaderId(int freshProductHeaderId){
    	this.freshProductHeaderId = freshProductHeaderId;
    }
     public int getFreshProductHeaderId(){
    	return freshProductHeaderId;
    }

     public void setLocationLevelId(int locationLevelId){
     	this.locationLevelId = locationLevelId;
     }
     public int getLocationLevelId(){
     	return locationLevelId;
     }

     public void setLocationId(int locationId){
     	this.locationId = locationId;
     }
     public int getLocationId(){
     	return locationId;
     }
     
    public void setRelationStartCalId(int relationStartCalId){
    	this.relationStartCalId = relationStartCalId;
    }
    public int getRelationStartCalId(){
    	return relationStartCalId;
    }

    public void setRelationEndCalId(int relationEndCalId){
    	this.relationEndCalId = relationEndCalId;
    }
    public int getRelationEndCalId(){
    	return relationEndCalId;
    }

    public void setFreshRelationRefId(int freshRelationRefId){
    	this.freshRelationRefId = freshRelationRefId;
    }
    public int getFreshRelationRefId(){
    	return freshRelationRefId;
    }
    
    public void setRelationStartDate(String relationStartDate){
    	this.relationStartDate = relationStartDate;
    }
    public String getRelationStartDate(){
    	return relationStartDate;
    }
    
    public void setRelationEndDate(String relationEndDate){
    	this.relationEndDate= relationEndDate;
    }
    public String getRelationEndDate(){
    	return relationEndDate;
    }
    
    public void setShrink(double shrink){
    	this.shrink = shrink;
    }
    public double getShrink(){
    	return shrink;
    }

    public void setAdditionalCost(double additionalCost){
    	this.additionalCost = additionalCost;
    }
    public double getAdditionalCost(){
    	return additionalCost;
    }
    
    public void setLaborCost(double laborCost){
    	this.laborCost= laborCost;
    }
    public double getLaborCost(){
    	return laborCost;
    }

    public List<PerishableOrderDTO> getOrderData() {
		return orderData;
	}
	public void setOrderData(List<PerishableOrderDTO> orderData) {
		this.orderData = orderData;
	}
    
    public List<PerishableSellDTO> getSellData() {
		return sellData;
	}
	public void setSellData(List<PerishableSellDTO> sellData) {
		this.sellData = sellData;
	}
}