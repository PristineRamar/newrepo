package com.pristine.dto;

public class WalmartStoreAddressFinderDTO {
	public int id;
	public String name;
	public String address1;
	public String address2;
	public String State;
	public String postCode;
	public String phone;
	public String latitude;
	public String longitude;
	public String openDate;
	public Address address;
	public GeoPoint geoPoint;
	public StoreType storeType;
	
	public class StoreType{
		public String name;
	}
	public class Address{
		public String postalCode;
		public String address1;
		public String city;
		public String state;
	}
	public class GeoPoint{
		public String latitude;
		public String longitude;
	}
	
	public String getOpenDate() {
		return openDate;
	}
	public void setOpenDate(String openDate) {
		this.openDate = openDate;
	}
	public String getPhone() {
		return phone;
	}
	public void setPhone(String phone) {
		this.phone = phone;
	}
	public int getId() {
		return id;
	}
	public void setId(int id) {
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
	
	
}
