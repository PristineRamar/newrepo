package com.pristine.dto;

public class PerformanceGoalDTO implements IValueObject {

	/*  Vaibhav 5 - This is a Data transfer Object
	 *  Map the PerformanceGoal table Elements
	 */ 
	
	private int chainId;
	private float itemsNotCheckedGoalPCT;
	private float itemsNotFoundGoalPCT;
	private float avgItemsPerHrGoalPCT;
	private float reasonabilityCheckGoalPCT;
	private float rangeCheckGoalPCT;
	private String AsOfDate;
	private int performanceGoalId;
	private int noOfChecks;
	private float itemsFixedLaterGoalPCT;
	private float itemsOnSaleGoalPCT;
	public int getChainId()
	{
		return chainId;
	}
	public void setChainId(int chainId)
	{
		this.chainId = chainId;
	}
	public float getItemsNotCheckedGoalPCT()
	{
		return itemsNotCheckedGoalPCT;
	}
	public void setItemsNotCheckedGoalPCT(float itemsNotCheckedGoalPCT)
	{
		this.itemsNotCheckedGoalPCT = itemsNotCheckedGoalPCT;
	}
	public float getItemsNotFoundGoalPCT()
	{
		return itemsNotFoundGoalPCT;
	}
	public void setItemsNotFoundGoalPCT(float itemsNotFoundGoalPCT)
	{
		this.itemsNotFoundGoalPCT = itemsNotFoundGoalPCT;
	}
	public float getAvgItemsPerHrGoalPCT()
	{
		return avgItemsPerHrGoalPCT;
	}
	public void setAvgItemsPerHrGoalPCT(float avgItemsPerHrGoalPCT)
	{
		this.avgItemsPerHrGoalPCT = avgItemsPerHrGoalPCT;
	}
	public float getReasonabilityCheckGoalPCT()
	{
		return reasonabilityCheckGoalPCT;
	}
	public void setReasonabilityCheckGoalPCT(float reasonabilityCheckGoalPCT)
	{
		this.reasonabilityCheckGoalPCT = reasonabilityCheckGoalPCT;
	}
	public String getAsOfDate()
	{
		return AsOfDate;
	}
	public void setAsOfDate(String asOfDate)
	{
		AsOfDate = asOfDate;
	}
	public int getPerformanceGoalId()
	{
		return performanceGoalId;
	}
	public void setPerformanceGoalId(int performanceGoalId)
	{
		this.performanceGoalId = performanceGoalId;
	}
	public int getNoOfChecks()
	{
		return noOfChecks;
	}
	public void setNoOfChecks(int noOfChecks)
	{
		this.noOfChecks = noOfChecks;
	}
	public float getItemsFixedLaterGoalPCT()
	{
		return itemsFixedLaterGoalPCT;
	}
	public void setItemsFixedLaterGoalPCT(float itemsFixedLaterGoalPCT)
	{
		this.itemsFixedLaterGoalPCT = itemsFixedLaterGoalPCT;
	}
	public float getItemsOnSaleGoalPCT()
	{
		return itemsOnSaleGoalPCT;
	}
	public void setItemsOnSaleGoalPCT(float itemsOnSaleGoalPCT)
	{
		this.itemsOnSaleGoalPCT = itemsOnSaleGoalPCT;
	}
	public float getRangeCheckGoalPCT()
	{
		return rangeCheckGoalPCT;
	}
	public void setRangeCheckGoalPCT(float rangeCheckGoalPCT)
	{
		this.rangeCheckGoalPCT = rangeCheckGoalPCT;
	}
	
	
}
