/*
 * Author : Naimish 
 * Start Date : Jul 22, 2009
 * 
 * Change Description 				Changed By 			Date
 * --------------------------------------------------------------
 */

package com.pristine.dto;

public class SuspectItemReasonDTO
{
	private Integer	checkDataID;
	private Integer reasonID;
	private String comments;
	private String suspectDetails;
	private String removalDetails;
	private String priceSuggestionDetails;
	private Character isremoved = 'N';
	private Character isForSale = 'N';
	
	public Character getIsForSale()
	{
		return isForSale;
	}

	public void setIsForSale(Character isForSale)
	{
		this.isForSale = isForSale;
	}

	public Character getIsRemoved()
	{
		return isremoved;
	}

	public void setIsRemoved(Character isremoved)
	{
		this.isremoved = isremoved;
	}

	public String getSuspectDetails()
	{
		return suspectDetails;
	}

	public void setSuspectDetails(String suspectDetails)
	{
		this.suspectDetails = suspectDetails;
	}

	public String getRemovalDetails()
	{
		return removalDetails;
	}

	public void setRemovalDetails(String removalDetails)
	{
		this.removalDetails = removalDetails;
	}

	public String getPriceSuggestionDetails()
	{
		return priceSuggestionDetails;
	}

	public void setPriceSuggestionDetails(String priceSuggestionDetails)
	{
		this.priceSuggestionDetails = priceSuggestionDetails;
	}

	public SuspectItemReasonDTO(Integer checkDataID, Integer reasonID, String comments, String suspectDetails, String removalDetails, String priceSuggestionDetails, Character isForSale)
	{
		this.checkDataID = checkDataID;
		this.reasonID = reasonID;
		this.comments = comments;
		this.suspectDetails = suspectDetails;
		this.removalDetails = removalDetails;
		this.priceSuggestionDetails = priceSuggestionDetails;
		this.isForSale = isForSale;
	}
	
	public Integer getCheckDataID()
	{
		return checkDataID;
	}
	public Integer getReasonID()
	{
		return reasonID;
	}
	public String getComments()
	{
		return comments;
	}

}
