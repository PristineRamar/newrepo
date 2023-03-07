/*
 *	Author		: vaibhavkumar
 *	Start Date	: Sep 25, 2009
 *
 *	Change Description					Changed By			Date
 *	--------------------------------------------------------------
 */
package com.pristine.dto;

public class PriceSuggestionDTO
{

	private String itemCode;
	private String itemName;
	private String prediction;
	private String regPrice;
	private String salePrice;
	public String getItemCode()
	{
		return itemCode;
	}
	public void setItemCode(String itemCode)
	{
		this.itemCode = itemCode;
	}
	public String getItemName()
	{
		return itemName;
	}
	public void setItemName(String itemName)
	{
		this.itemName = itemName;
	}
	public String getPrediction()
	{
		return prediction;
	}
	public void setPrediction(String prediction)
	{
		this.prediction = prediction;
	}
	public String getRegPrice()
	{
		return regPrice;
	}
	public void setRegPrice(String regPrice)
	{
		this.regPrice = regPrice;
	}
	public String getSalePrice()
	{
		return salePrice;
	}
	public void setSalePrice(String salePrice)
	{
		this.salePrice = salePrice;
	}
	
	
}
