package com.pristine.dto.offermgmt;

public class VendorFileDTO {

	private String upc;
	private String itemCode;
	private String description;
	private String size;
	private String uom;
	private String store;
	private String vendorCode;
	private String vendorName;
	private long vendorId;
	
	public long getVendorId() {
		return vendorId;
	}
	public void setVendorId(long vendorId) {
		this.vendorId = vendorId;
	}
	public String getUpc() {
		return upc;
	}
	public void setUpc(String upc) {
		this.upc = upc;
	}
	public String getItemCode() {
		return itemCode;
	}
	public void setItemCode(String itemCode) {
		this.itemCode = itemCode;
	}
	public String getDescription() {
		return description;
	}
	public void setDescription(String description) {
		this.description = description;
	}
	public String getSize() {
		return size;
	}
	public void setSize(String size) {
		this.size = size;
	}
	public String getUom() {
		return uom;
	}
	public void setUom(String uom) {
		this.uom = uom;
	}
	public String getStore() {
		return store;
	}
	public void setStore(String store) {
		this.store = store;
	}
	public String getVendorCode() {
		return vendorCode;
	}
	public void setVendorCode(String vendorCode) {
		this.vendorCode = vendorCode;
	}
	public String getVendorName() {
		return vendorName;
	}
	public void setVendorName(String vendorName) {
		this.vendorName = vendorName;
	}
	@Override
	public String toString() {
		return "VendorFileDTO [upc=" + upc + ", itemCode=" + itemCode
				+ ", description=" + description + ", size=" + size + ", uom="
				+ uom + ", store=" + store + ", vendorCode=" + vendorCode
				+ ", vendorName=" + vendorName + "]";
	}
	
	
}
