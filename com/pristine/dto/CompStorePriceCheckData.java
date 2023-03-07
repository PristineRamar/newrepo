package com.pristine.dto;

import java.util.ArrayList;
import java.util.HashMap;

public class CompStorePriceCheckData implements IValueObject {
	
	public HashMap<Integer, CompetitiveDataDTO> priceCheckData = new HashMap<Integer, CompetitiveDataDTO>();
	public HashMap<Integer, CompetitiveDataDTO> carriedPriceCheckData = new HashMap<Integer, CompetitiveDataDTO>();
	public int totalScore = -1;
	public int noOfItemsChecked=0;
	public int noOfItemsProxyChecked=0;
	public int compStrId;
	public int foundItemCount=0;
	public int notCarriedItemCount=0;
	public int noofUniquePriceForStore=0;
	public int noofUniqueItemsForStore=0;
	public int carriedNoofUniquePriceForStore=0;
	public int carriedNoofUniqueItemsForStore=0;
	public int noOfCrossStoreRelatedItems = 0;
	public int noOfWithinStoreRelatedItems = 0;
	public int checkRank=-1;
	
	public HashMap<String, Integer> lirPricePoints = new HashMap <String, Integer> ();
	public HashMap<String, ArrayList <String>> crossManPricePoints = new HashMap <String, ArrayList <String>> ();
	public HashMap<String, Integer> sameManPricePoints = new HashMap <String, Integer> ();
	
	public HashMap<Integer, CompetitiveDataDTO> suggestedPriceMap = new HashMap<Integer, CompetitiveDataDTO> (); 
	
	public int priceMatchCount=0; 
	public int priceNotMatchCount=0; 
	public int matchCannotBeDeterminedCount=0;
	
	public String compStrNum;
	public String compStrName;
	public String weekStartDate;
}
