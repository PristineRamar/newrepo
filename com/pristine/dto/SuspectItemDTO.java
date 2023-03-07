/*
 * Author : Naimish Start Date : Jul 22, 2009
 * 
 * Change Description Changed By Date
 * --------------------------------------------------------------
 */

package com.pristine.dto;

import java.util.ArrayList;

public class SuspectItemDTO
{
	private Integer							checkDataID;
	private Integer							itemID;
	private Integer							scheduleID;
	private Integer							compStrID;
	private ArrayList<SuspectItemReasonDTO>	suspectReasonDTOs;
	private String							suspectComments		= "";
	private Float							suggestedPrice		= 0f;
	private Float							suggestedQty		= 0f;
	private Integer							suggestedReasonID	= 1;
	private Integer							approvalStatus		= 1;
	private Character						saleInd;
	private Integer							changeDirection;
	private boolean							isRemoved;

	// Following fields are not from suspect items table
	private float							regularUnitPrice	= 0f;
	private float							saleUnitPrice		= 0f;
	private float							regularPrice		= 0f;
	private float							salePrice			= 0f;

	public float getRegularUnitPrice()
	{
		return regularUnitPrice;
	}

	public void setRegularUnitPrice(float regularUnitPrice)
	{
		this.regularUnitPrice = regularUnitPrice;
	}

	public float getSaleUnitPrice()
	{
		return saleUnitPrice;
	}

	public void setSaleUnitPrice(float saleUnitPrice)
	{
		this.saleUnitPrice = saleUnitPrice;
	}

	public float getRegularPrice()
	{
		return regularPrice;
	}

	public void setRegularPrice(float regularPrice)
	{
		this.regularPrice = regularPrice;
	}

	public float getSalePrice()
	{
		return salePrice;
	}

	public void setSalePrice(float salePrice)
	{
		this.salePrice = salePrice;
	}

	public boolean isRemoved()
	{
		return isRemoved;
	}

	public void setIsRemoved(boolean val)
	{
		this.isRemoved = val;
	}

	public ArrayList<SuspectItemReasonDTO> getSuspectReasonDTOs()
	{
		return suspectReasonDTOs;
	}

	public void setSuspectReasonDTOs(ArrayList<SuspectItemReasonDTO> suspectReasonDTOs)
	{
		this.suspectReasonDTOs = suspectReasonDTOs;
	}

	public Integer getCheck_Data_ID()
	{
		return checkDataID;
	}

	public void setCheck_Data_ID(Integer checkDataID)
	{
		this.checkDataID = checkDataID;
	}

	public Integer getItem_ID()
	{
		return itemID;
	}

	public void setItem_ID(Integer itemID)
	{
		this.itemID = itemID;
	}

	public Integer getSchedule_ID()
	{
		return scheduleID;
	}

	public void setSchedule_ID(Integer scheduleID)
	{
		this.scheduleID = scheduleID;
	}

	public Integer getComp_Str_ID()
	{
		return compStrID;
	}

	public void setComp_Str_ID(Integer compStrID)
	{
		this.compStrID = compStrID;
	}

	public String getSuspect_Comments()
	{
		return suspectComments;
	}

	public void setSuspect_Comments(String suspectComments)
	{
		this.suspectComments = suspectComments;
	}

	public Float getSuggested_Price()
	{
		return suggestedPrice;
	}

	public void setSuggested_Price(Float suggestedPrice)
	{
		this.suggestedPrice = suggestedPrice;
	}

	public Float getSuggested_Qty()
	{
		return suggestedQty;
	}

	public void setSuggested_Qty(Float suggestedQty)
	{
		this.suggestedQty = suggestedQty;
	}

	public Integer getSuggested_Reason_ID()
	{
		return suggestedReasonID;
	}

	public void setSuggested_Reason_ID(Integer suggestedReasonID)
	{
		this.suggestedReasonID = suggestedReasonID;
	}

	public Integer getApproval_Status()
	{
		return approvalStatus;
	}

	public void setApproval_Status(Integer approvalStatus)
	{
		this.approvalStatus = approvalStatus;
	}

	public Character getSale_Ind()
	{
		return saleInd;
	}

	public void setSale_Ind(Character saleInd)
	{
		this.saleInd = saleInd;
	}

	public Integer getChangeDirection()
	{
		return changeDirection;
	}

	public void setChangeDirection(Integer changeDirection)
	{
		this.changeDirection = changeDirection;
	}

}