package com.pristine.dto;

import com.fasterxml.jackson.annotation.JsonProperty;

public class KrogerStoreDetailsDTO {
	public String id;
	public String name;
	public String address1;
	public String address2;
	public String State;
	public String postCode;
	public String phone;
	public String latitude;
	public String longitude;
	public String openDate;
	public String brand;
	public String storeType;
	public StoreInformation storeInformation;
	
	public class StoreInformation{
		public String storeNumber;
		public String legalName;
		public String phoneNumber;
		public String brand;
		public String storeType;
		@JsonProperty("address")
		public KrogerAddress address; 
		public LatLong latLong;
		
		public class LatLong{
			public String latitude;
			public String longitude;
		}
		
		public class KrogerAddress{
			
			public String addressLineOne;
			public String city;
			public String state;
			public String zipCode;
		}
	}
	
	
	public String getBrand() {
		return brand;
	}
	public void setBrand(String brand) {
		this.brand = brand;
	}
	public String getStoreType() {
		return storeType;
	}
	public void setStoreType(String storeType) {
		this.storeType = storeType;
	}
	
	public String getId() {
		return id;
	}
	public void setId(String id) {
		this.id = id;
	}
	public String getName() {
		return name;
	}
	public void setName(String name) {
		this.name = name;
	}
	public String getAddress1() {
		return address1;
	}
	public void setAddress1(String address1) {
		this.address1 = address1;
	}
	public String getAddress2() {
		return address2;
	}
	public void setAddress2(String address2) {
		this.address2 = address2;
	}
	public String getState() {
		return State;
	}
	public void setState(String state) {
		State = state;
	}
	public String getPostCode() {
		return postCode;
	}
	public void setPostCode(String postCode) {
		this.postCode = postCode;
	}
	public String getPhone() {
		return phone;
	}
	public void setPhone(String phone) {
		this.phone = phone;
	}
	public String getLatitude() {
		return latitude;
	}
	public void setLatitude(String latitude) {
		this.latitude = latitude;
	}
	public String getLongitude() {
		return longitude;
	}
	public void setLongitude(String longitude) {
		this.longitude = longitude;
	}
	public String getOpenDate() {
		return openDate;
	}
	public void setOpenDate(String openDate) {
		this.openDate = openDate;
	}
}
