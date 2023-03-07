package com.pristine.dto;



public class WalGreensStoreDetailDTO {
	public String latitude;
	public String longitude;
	public Store store;
	
	public class Store{
		public String storeNumber;
		public String storeType;
		public String storeBrand;
		public WalGreensAddress address;
		public Phone phone;
		
		public class WalGreensAddress{
			public String zip;
			public String city;
			public String street;
			public String state;
		}
		
		public class Phone{
			public String number;
			public String areaCode;
		}
		
	}
}
