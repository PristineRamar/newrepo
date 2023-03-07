package com.pristine.dto;

import java.util.HashMap;

public class MovementDailyAggregateDTO extends MovementWeeklyDTO
{

	private int _DayOfWeekId = -1;
	private int _TimeOfDayId = -1;
	private String _CustomerCard = "";
	private int _householdNumber =0;
	private double _saleQty;
	private double _regularQty;
	private HashMap<Integer, Integer> _productData = new HashMap<Integer, Integer>();
	private double _unitPrice;	
	private int _privateLabel =0;
	
	public int getDayOfWeekId () { 
		return _DayOfWeekId; 
	}
	public void setDayOfWeekId(int v) { 
		_DayOfWeekId = v; 
	}

	public int getHouseholdNumber () { 
		return _householdNumber; 
	}
	public void setHouseholdNumber(int v) { 
		_householdNumber = v; 
	}
	

	public int getTimeOfDayId () { 
		return _TimeOfDayId; 
	}
	public void setTimeOfDayId(int v) { 
		_TimeOfDayId = v; 
	}

	public String getCustomerCard () { 
		return _CustomerCard; 
	}
	public void setCustomerCard(String v) { 
		_CustomerCard = v; 
	}
	
	public double getRegularQuantity() {
		return _regularQty;
	}
	public void setRegularQuantity(double v) {
		_regularQty = v;
	}
	 
	public double getSaleQuantity() {
		return _saleQty;
	}
	public void setSaleQuantity(double v) {
		_saleQty = v;
	}

	public HashMap<Integer, Integer> getProductData () { 
		return _productData; 
	}
	public void setProductData(HashMap<Integer, Integer> v) { 
		_productData = v; 
	}

	public double getUnitPrie() {
		return _unitPrice;
	}
	public void setUnitPrice(double v) {
		_unitPrice = v;
	}

	public int getPrivateLabel() {
		return _privateLabel;
	}
	public void setPrivateLabel(int v) {
		_privateLabel = v;
	}	
	
}
