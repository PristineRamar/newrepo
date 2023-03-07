package com.pristine.webscrape.Wegmans;

public  class ScrapeItemDTO{
	String dept;
	String category;
	String itemName;
	String friendlyName;
	String retailerItemCode;
	String UPC;
	String wegmansPLUPC;
	String wegmansProductCode;
	String regPrice;
	String salePrice;
	String wegmansItemName;
	String itemUOMSize;
	String itemSize;
	String itemUOMDesc;
	public String shoppingURL;
	
	//Fields for Mapping purpose
	double minPrice;
	double maxPrice;
	double modePrice;
	double dblRegPrice;
	String wegmansPriceFlg;
	String [] itemWords;
	double dblSize;
	int score;
	int pctScore;
	boolean priceMatch = false;
	boolean sizeMatch = false;
	boolean brandMatch = false;
	boolean upcMatch = false;

	public ScrapeItemDTO clone(){
		ScrapeItemDTO cloneItem = new ScrapeItemDTO();
		
		cloneItem.dept = this.dept;
		cloneItem.category = this.category;
		cloneItem.itemName = this.itemName;
		cloneItem.friendlyName = this.friendlyName;
		cloneItem.retailerItemCode = this.retailerItemCode;
		cloneItem.UPC = this.UPC;
		cloneItem.wegmansPLUPC = this.wegmansPLUPC;
		cloneItem.wegmansProductCode = this.wegmansProductCode;
		cloneItem.regPrice = this.regPrice;
		cloneItem.salePrice = this.salePrice;
		cloneItem.wegmansItemName = this.wegmansItemName;
		cloneItem.itemUOMSize = this.itemUOMSize;
		cloneItem.itemSize = this.itemSize;
		cloneItem.itemUOMDesc = this.itemUOMDesc;
		cloneItem.shoppingURL = this.shoppingURL;
		
		//Fields for Mapping purpose
		cloneItem.minPrice = this.minPrice;
		cloneItem.maxPrice = this.maxPrice;
		cloneItem.modePrice = this.modePrice;
		cloneItem.dblRegPrice = this.dblRegPrice;
		cloneItem.wegmansPriceFlg = this.wegmansPriceFlg;
		cloneItem.itemWords = this.itemWords;
		cloneItem.dblSize = this.dblSize;
		cloneItem.score = this.score;
		cloneItem.pctScore =this.pctScore;
		cloneItem.priceMatch = this.priceMatch ;
		cloneItem.sizeMatch= this.sizeMatch;
		cloneItem.brandMatch = this.brandMatch; 
		cloneItem.upcMatch = this.upcMatch;
		return cloneItem;
	}
	
}


	