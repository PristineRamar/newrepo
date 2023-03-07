package com.pristine.dto;

public class LigDTO {
	private String lineGroupIdentifier;
	private String retailerItemCode;
	private String internalItemNo;
	private String lineGroupStatus;
	private String sizeClass;
	private String sizeFamily;
	private String brandClass;
	private String brandFamily;
	private String upc;
	private String kviCode;
	private int priceCheckTypeId ;
	private int itemCode ;
	private int lirId;
	public String getLirCode() {
		return lirCode;
	}
	public void setLirCode(String lirCode) {
		this.lirCode = lirCode;
	}

	private String lirCode;
	
	private boolean printRow = true;
	
	

	public boolean isPrintRow() {
		return printRow;
	}
	public void setPrintRow(boolean printRow) {
		this.printRow = printRow;
	}
	public int getLirId() {
		return lirId;
	}
	public void setLirId(int lirId) {
		this.lirId = lirId;
	}
	public String getRetailerItemCodeNoVer() {
		return retailerItemCodeNoVer;
	}
	public void setRetailerItemCodeNoVer(String retailerItemCodeNoVer) {
		this.retailerItemCodeNoVer = retailerItemCodeNoVer;
	}

	private String retailerItemCodeNoVer;
	
	private String itemSize ="";
	
	private String brandName ="";
	
	private String uomName ="";
	
	private String sizeLead ="";
	private String priceGroupLead ="";
	
	private int tier;
	private String tierOverride = "";
	
	public String getTierOverride() {
		return tierOverride;
	}
	public void setTierOverride(String tierOverride) {
		this.tierOverride = tierOverride;
	}

	private String itemName;
	private String dependentLIG;
	private String dependentItemName;
	private String dependentRetailerItemCode;
	
	private boolean itemLevelSizeRelationship = true;
	private boolean itemLevelBrandRelationship = true;
	
	public boolean isItemLevelSizeRelationship() {
		return itemLevelSizeRelationship;
	}
	public void setItemLevelSizeRelationship(boolean itemLevelSizeRelationship) {
		this.itemLevelSizeRelationship = itemLevelSizeRelationship;
	}
	public boolean isItemLevelBrandRelationship() {
		return itemLevelBrandRelationship;
	}
	public void setItemLevelBrandRelationship(boolean itemLevelBrandRelationship) {
		this.itemLevelBrandRelationship = itemLevelBrandRelationship;
	}

	private boolean itemLevelRelationship = true;
	
	
	public boolean isItemLevelRelationship() {
		return itemLevelRelationship;
	}
	public void setItemLevelRelationship(boolean itemLevelRelationship) {
		this.itemLevelRelationship = itemLevelRelationship;
	}
	public String getItemName() {
		return itemName;
	}
	public void setItemName(String itemName) {
		this.itemName = itemName;
	}
	public String getDependentLIG() {
		return dependentLIG;
	}
	public void setDependentLIG(String dependentLIG) {
		this.dependentLIG = dependentLIG;
	}
	public String getDependentItemName() {
		return dependentItemName;
	}
	public void setDependentItemName(String dependentItemName) {
		this.dependentItemName = dependentItemName;
	}
	public String getDependentRetailerItemCode() {
		return dependentRetailerItemCode;
	}
	public void setDependentRetailerItemCode(String dependentRetailerItemCode) {
		this.dependentRetailerItemCode = dependentRetailerItemCode;
	}
	
	public int getTier() {
		return tier;
	}
	public void setTier(int tier) {
		this.tier = tier;
	}
	
	
	public String getSizeLead() {
		return sizeLead;
	}
	public void setSizeLead(String sizeLead) {
		this.sizeLead = sizeLead;
	}
	public String getPriceGroupLead() {
		return priceGroupLead;
	}
	public void setPriceGroupLead(String priceGroupLead) {
		this.priceGroupLead = priceGroupLead;
	}
	public String getItemSize() {
		return itemSize;
	}
	public void setItemSize(String itemSize) {
		this.itemSize = itemSize;
	}
	public String getBrandName() {
		return brandName;
	}
	public void setBrandName(String brandName) {
		this.brandName = brandName;
	}
	public String getUomName() {
		return uomName;
	}
	public void setUomName(String uomName) {
		this.uomName = uomName;
	}
	public String getLineGroupIdentifier() {
		return lineGroupIdentifier;
	}
	public void setLineGroupIdentifier(String lineGroupIdentifier) {
		this.lineGroupIdentifier = lineGroupIdentifier;
	}
	public String getRetailerItemCode() {
		return retailerItemCode;
	}
	public void setRetailerItemCode(String retailerItemCode) {
		this.retailerItemCode = retailerItemCode;
	}
	public String getInternalItemNo() {
		return internalItemNo;
	}
	public void setInternalItemNo(String internalItemNo) {
		this.internalItemNo = internalItemNo;
	}
	public String getLineGroupStatus() {
		return lineGroupStatus;
	}
	public void setLineGroupStatus(String lineGroupStatus) {
		this.lineGroupStatus = lineGroupStatus;
	}
	public String getSizeClass() {
		return sizeClass;
	}
	public void setSizeClass(String sizeClass) {
		this.sizeClass = sizeClass;
	}
	public String getSizeFamily() {
		return sizeFamily;
	}
	public void setSizeFamily(String sizeFamily) {
		this.sizeFamily = sizeFamily;
	}
	public String getBrandClass() {
		return brandClass;
	}
	public void setBrandClass(String brandClass) {
		this.brandClass = brandClass;
	}
	public String getBrandFamily() {
		return brandFamily;
	}
	public void setBrandFamily(String brandFamily) {
		this.brandFamily = brandFamily;
	}
	public String getUpc() {
		return upc;
	}
	public void setUpc(String upc) {
		this.upc = upc;
	}

	public int getPriceCheckTypeId() {
		return priceCheckTypeId;
	}

	public void setPriceCheckTypeId(int priceCheckTypeId) {
		this.priceCheckTypeId = priceCheckTypeId;
	}

	public String getKviCode() {
		return kviCode;
	}

	public void setKviCode(String kviCode) {
		this.kviCode = kviCode;
	}

	public int getItemCode() {
		return itemCode;
	}

	public void setItemCode(int itemCode) {
		this.itemCode = itemCode;
	}

}
