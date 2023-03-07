package com.pristine.dto;

import java.util.List;

public class SaveALotStoreDTO {
	public String latitude;
	public String longitude;
	public String openDate;
	public Fields Fields;
	public class Fields{
		public String storeNumber;
		public String locationId;
		public String city;
		public String zip;
		public String state;
		public String latlng;
		public String phone;
		public String locationName;
		public String address1;
		public List<String> StoreNumber;
		public List<String> LocationId;
		public List<String> City;
		public List<String> Zip;
		public List<String> State;
		public List<String> Latlng;
		public List<String> Phone;
		public List<String> LocationName;
		public List<String> Address1;
		
		public String getStoreNumber() {
			return storeNumber;
		}
		public void setStoreNumber(String storeNumber) {
			this.storeNumber = storeNumber;
		}
		public String getLocationId() {
			return locationId;
		}
		public void setLocationId(String locationId) {
			this.locationId = locationId;
		}
		public String getCity() {
			return city;
		}
		public void setCity(String city) {
			this.city = city;
		}
		public String getZip() {
			return zip;
		}
		public void setZip(String zip) {
			this.zip = zip;
		}
		public String getState() {
			return state;
		}
		public void setState(String state) {
			this.state = state;
		}
		public String getLatlng() {
			return latlng;
		}
		public void setLatlng(String latlng) {
			this.latlng = latlng;
		}
		public String getPhone() {
			return phone;
		}
		public void setPhone(String phone) {
			this.phone = phone;
		}
		public String getLocationName() {
			return locationName;
		}
		public void setLocationName(String locationName) {
			this.locationName = locationName;
		}
		public String getAddress1() {
			return address1;
		}
		public void setAddress1(String address1) {
			this.address1 = address1;
		}
		
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
