package com.pristine.dto.customer;

public class HouseHoldDTO {

	private String retailAbbreviation;
	private String groupID;
	private String consumerID;
	private int consumerIDType;  
	
	
	public String getRetailAbbreviation() {
		return retailAbbreviation;
	}
	public void setRetailAbbreviation(String retailAbbreviation) {
		this.retailAbbreviation = retailAbbreviation;
	}
	public String getGroupID() {
		return groupID;
	}
	public void setGroupID(String groupID) {
		this.groupID = groupID;
	}
	public String getConsumerID() {
		return consumerID;
	}
	public void setConsumerID(String consumerID) {
		this.consumerID = consumerID;
	}
	public int getConsumerIDType() {
		return consumerIDType;
	}
	public void setConsumerIDType(int consumerIDType) {
		this.consumerIDType = consumerIDType;
	}
	   
}

