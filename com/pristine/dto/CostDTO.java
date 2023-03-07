package com.pristine.dto;

public class CostDTO {

	public int checkItemId;	
	public int itemCode;
	public int compStrId;
	public float listCost;
	public float dealCost;
	public String listCostEffDate;
	public String promoCostStartDate;
	public String promoCostEndDate;
	public int costChgDirection;
	public int scheduleId;
	public String weekStartDate;
	public float price;
	public boolean isPriceChanged;
	
	public boolean hasLIGPriceVariations = false;
}
