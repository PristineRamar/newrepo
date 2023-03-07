package com.pristine.dto;

import java.util.Date;

public class ShippingInfoDTO  implements Comparable<ShippingInfoDTO>{
	public Date getInvoiceDate () { return _invoiceDate; }
	public void setInvoiceDate(Date v) { _invoiceDate = v; }
	private Date _invoiceDate = null;
	
	public Date getShipmentDate () { return _shipmentDate; }
	public void setShipmentDate(Date v) { _shipmentDate = v; }
	private Date _shipmentDate = null;

	public int getStoreID () { return _storeID; }
	public void setStoreID(int v) { _storeID = v; }
	private int _storeID = -1;

	public String getStoreNum () { return _storeNum; }
	public void setStoreNum(String v) { _storeNum = v; }
	private String _storeNum = "";

	
	public int getItemCode () { return _itemCode; }
	public void setItemCode(int v) { _itemCode = v; }
	private int _itemCode = -1;

	public String getItemDesc () { return _itemDesc; }
	public void setItemDesc(String v) { _itemDesc = v; }
	private String _itemDesc = "";
	
	public String getRetailerCode() { return _retailerCode ; }
	public void setRetailerCode(String v) { _retailerCode  = v; }
	private String _retailerCode = "";
	
	public String getUPC () { return _upc; }
	public void setUPC(String v) { _upc = v; }
	private String _upc = "";

	public int getStorePack () { return _storePack; }
	public void setStorePack(int v) { _storePack = v; }
	private int _storePack = -1;
	
	public int getCasesShipped () { return _casesShipped; }
	public void setCasesShipped (int v) { _casesShipped = v; }
	private int _casesShipped = -1;
	
	public int getQuantity () { return _quantity; }
	public void setQuantity (int v) { _quantity = v; }
	private int _quantity = -1;
	
	public int compareTo(ShippingInfoDTO s2) {
		// TODO Auto-generated method stub
		if(this.getShipmentDate()==null && s2.getShipmentDate()==null){
			return 0;
		}
		if(this.getShipmentDate() == null){
			return -1;
		}
		if(s2.getShipmentDate() == null){
			return 1;
		}

		if(this.getShipmentDate().equals(s2.getShipmentDate())) {
			return 0;
		}else if(this.getShipmentDate().before(s2.getShipmentDate())){
			return -1;
		}else{
			return 1;
		}
	}


}
