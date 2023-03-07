/**
 * 
 */
package com.pristine.dto.fileformatter;

import java.text.DateFormat;
import java.text.DecimalFormat;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.List;

import com.pristine.exception.GeneralException;
import com.pristine.util.DateUtil;
import com.pristine.util.PrestoUtil;

/**
 * @author Nagarajan 
 * Structure of Ahold Movement File
 */
/**
 * @author Nagaraj
 *
 */
/**
 * @author Nagaraj
 *
 */
/**
 * @author Nagaraj
 *
 */
/**
 * @author Nagaraj
 *
 */
/**
 * @author Nagaraj
 *
 */
public class AholdMovementFile {
	private String CUSTOM_ATTR2_DESC;
	private String UPC_CD;
	private String LIST_MEMBER;
	private String OPCO_LOC_CD;
	private String WK_KEY;
	private String WK_DESC;
	private String SCANNEDUNITS;
	private String GROSSDOLLARS;
	private String NETDOLLARS;
	private String RETAILWEIGHT;
	private String POUNDORUNITS;
	private String EXTLEVEL3ELEMENTCOST;
	private String EXTLEVEL4ELEMENTCOST;
	private Float grossPrice;
	private Float netPrice;
	private Float poundOrUnits;
	private Float level3Cost;
	private Float level4Cost;
	private Float scannedUnits;
	private Float unitCount;
	

	public String getCUSTOM_ATTR2_DESC() {
		return CUSTOM_ATTR2_DESC;
	}

	public void setCUSTOM_ATTR2_DESC(String cUSTOM_ATTR2_DESC) {
		CUSTOM_ATTR2_DESC = cUSTOM_ATTR2_DESC;
	}

	public String getUPC_CD() {
		if (UPC_CD.length() > 12)
			//Keep last 12 digits only
			return UPC_CD.substring(UPC_CD.length() - 12);
		else
			return UPC_CD;
	}

	public void setUPC_CD(String uPC_CD) {
		UPC_CD = uPC_CD;
	}

	public String getOPCO_LOC_CD() {
		if (OPCO_LOC_CD.length() == 5)
			//Keep last 4 digits only
			return OPCO_LOC_CD.substring(OPCO_LOC_CD.length() - 4);
		else
			return OPCO_LOC_CD;
	}

	public void setOPCO_LOC_CD(String oPCO_LOC_CD) {
		OPCO_LOC_CD = oPCO_LOC_CD;
	}

	public String getWK_KEY() {
		return WK_KEY;
	}

	public void setWK_KEY(String wK_KEY) {
		WK_KEY = wK_KEY;
	}

	public String getWK_DESC() {
		return WK_DESC;
	}

	public void setWK_DESC(String wK_DESC) {
		WK_DESC = wK_DESC;
	}

	public String getSCANNEDUNITS() {
		return SCANNEDUNITS;
	}

	public void setSCANNEDUNITS(String sCANNEDUNITS) {
		SCANNEDUNITS = sCANNEDUNITS;
	}

	public String getGROSSDOLLARS() {
		return GROSSDOLLARS;
	}

	public void setGROSSDOLLARS(String gROSSDOLLARS) {		 
		GROSSDOLLARS = gROSSDOLLARS;
	}

	public String getNETDOLLARS() {
		return NETDOLLARS;
	}

	public void setNETDOLLARS(String nETDOLLARS) {
		NETDOLLARS = nETDOLLARS;
	}

	public String getRETAILWEIGHT() {
		return RETAILWEIGHT;
	}

	public void setRETAILWEIGHT(String rETAILWEIGHT) {
		RETAILWEIGHT = rETAILWEIGHT;
	}

	public String getPOUNDORUNITS() {
		return POUNDORUNITS;
	}

	public void setPOUNDORUNITS(String pOUNDORUNITS) {
		POUNDORUNITS = pOUNDORUNITS;
	}

	public String getEXTLEVEL3ELEMENTCOST() {
		return EXTLEVEL3ELEMENTCOST;
	}

	public void setEXTLEVEL3ELEMENTCOST(String eXTLEVEL3ELEMENTCOST) {
		EXTLEVEL3ELEMENTCOST = eXTLEVEL3ELEMENTCOST;
	}

	public String getEXTLEVEL4ELEMENTCOST() {
		return EXTLEVEL4ELEMENTCOST;
	}

	public void setEXTLEVEL4ELEMENTCOST(String eXTLEVEL4ELEMENTCOST) {
		EXTLEVEL4ELEMENTCOST = eXTLEVEL4ELEMENTCOST;
	}
	
	public String getLIST_MEMBER() {
		return LIST_MEMBER;
	}

	public void setLIST_MEMBER(String lIST_MEMBER) {
		LIST_MEMBER = lIST_MEMBER;
	}
	
	/**
	 * Initialize necessary variables. 
	 */
	public void initializeVariables()
	{	
		//Initialize default value as 0 for parameters having empty value
		RETAILWEIGHT = RETAILWEIGHT.equals("")?"0":RETAILWEIGHT;
		SCANNEDUNITS = SCANNEDUNITS.equals("")?"0":SCANNEDUNITS;
		GROSSDOLLARS = GROSSDOLLARS.equals("")?"0":GROSSDOLLARS;
		NETDOLLARS = NETDOLLARS.equals("")?"0":NETDOLLARS;
		POUNDORUNITS = POUNDORUNITS.equals("")?"0":POUNDORUNITS;
		EXTLEVEL3ELEMENTCOST = EXTLEVEL3ELEMENTCOST.equals("")?"0":EXTLEVEL3ELEMENTCOST;
		EXTLEVEL4ELEMENTCOST = EXTLEVEL4ELEMENTCOST.equals("")?"0":EXTLEVEL4ELEMENTCOST;		
		
		//Converts necessary parameter to float
		grossPrice = new Float(GROSSDOLLARS);
		netPrice = new Float(NETDOLLARS);
		poundOrUnits = new Float(POUNDORUNITS);
		level3Cost = new Float(EXTLEVEL3ELEMENTCOST);
		level4Cost = new Float(EXTLEVEL4ELEMENTCOST);
		scannedUnits = new Float(SCANNEDUNITS);
		
		//Find unitcount
		if (Float.compare(scannedUnits,poundOrUnits) == 0)
		{
			unitCount = poundOrUnits;
		}
		else
		{
			unitCount = poundOrUnits;
		}
	}
	
	/**
	 * Find whether the item is onsale or not.	
	 * @return
	 */
	public boolean isItemOnSale() {
		//If grossprice and netprice is different, then the item is considered as onsale		
		if (Float.compare(grossPrice,netPrice) == 0) 
			return false;
		//Hanlde items where netprice is zero but there is gross price, consider these items are as reg items
		else if (netPrice == 0 )
			return false;
		else			
			return true;
	}
	
	/**
	 * Convert the date as string in csv to date format
	 * @return
	 * @throws ParseException
	 */
	private Date convertToDate() throws ParseException
	{		
		String tempDate = WK_DESC.substring(3);
		DateFormat formatter ; 
	    Date date ; 
		formatter = new SimpleDateFormat("yyyy-MM-dd");
		date = (Date)formatter.parse(tempDate);
		return date;
	}

	/**
	 * Compute the regular retail price of the item
	 * @return
	 */
	public String regularRetail() {	 
		float regPrice = 0;
		// If unitcount is greater than 0
		if (unitCount != 0) {
			regPrice = grossPrice / unitCount;
			//return new DecimalFormat("#.##").format(regPrice);
			return Float.toString(PrestoUtil.round(regPrice, 2));
		} else if (unitCount == 0) {
			// Return the original value, if movement is 0
			//return new DecimalFormat("#.##").format(grossPrice);
			return Float.toString(PrestoUtil.round(grossPrice, 2));
		}
		return "";
	}
	
	/**
	 * Compute the regular retail effective date of the item
	 * @return
	 * @throws ParseException
	 */
	public String regRetailEffDate() throws ParseException {
		String regEffDate = "";
		//get the start date of the week
		//if (!isItemOnSale()) {
			regEffDate = DateUtil.getWeekStartDate(convertToDate(), 0);
		//}
		return regEffDate;
	}
	
	/**
	 * Compute the regular retail quantity
	 * @return
	 */
	public String regRetailQty() {
		// Make movements in negative as 0
		if (unitCount < 0 || unitCount == 0)
			return "0";
		else
			return "1";
	}

	/**
	 * Compute the sale retail price of the item
	 * @return
	 */
	public String saleRetail() {		
		float salePrice = 0;
		//If item is onsale and unitcount is greater than 0
		if (isItemOnSale() && unitCount != 0) {						
			salePrice = netPrice / unitCount;
			//return new DecimalFormat("#.##").format(salePrice);
			return Float.toString(PrestoUtil.round(salePrice, 2));
		} else if (isItemOnSale() && unitCount == 0) {
			//Return the original value, if movement is 0
			//return new DecimalFormat("#.##").format(netPrice);
			return Float.toString(PrestoUtil.round(netPrice, 2));
		}
		return "";
	}

	/**
	 * Compute the sale retail start date
	 * @return
	 * @throws ParseException
	 */
	public String saleRetailStartDate() throws ParseException {
		String saleStartDate = "";
		//If item is onsale, get the start date of the week
		if (isItemOnSale()) {
			saleStartDate = DateUtil.getWeekStartDate(convertToDate(), 0);
		}
		return saleStartDate;
	}

	/**
	 * Compute the sale retail end date
	 * @return
	 * @throws ParseException
	 */
	public String saleRetailEndDate() throws ParseException {
		String saleEndDate = "";
		//If item is onsale, get the end date of the week
		if (isItemOnSale()) {
			saleEndDate = DateUtil.getWeekEndDate(convertToDate());
		}
		return saleEndDate;
	}
	
	
	/**
	 * Compute the sale retail quantity
	 * @return
	 */
	public String saleRetailQty() {
		// If item is onsale, then return quantity as 1
		if (isItemOnSale())
			return "1";
		else if (isItemOnSale() && (unitCount < 0 || unitCount == 0))
			// Make movements in negative as 0
			return "0";
		else
			return "";
	}
	
	
	//Retail Cost
	
	/**
	 * Compute whether the item is ondeal or not
	 * @return
	 */
	private boolean isItemOnDeal()
	{
		// if EXTLEVEL4ELEMENTCOST < EXTLEVEL3ELEMENTCOST, then the item is ondeal
		// Ignore deal cost is equal or lesser than 0
		if (level4Cost < level3Cost && level4Cost >0)
			return true;
		else
			return false;
	}
	
	/**
	 * Compute the list cost effective start date of the item
	 * @return
	 * @throws ParseException
	 */
	public String listCostEffDate() throws ParseException {
		String listCostEffDate = "";
		//get the start date of the week
		//if (!isItemOnDeal()) {
			listCostEffDate = DateUtil.getWeekStartDate(convertToDate(), 0);
		//}
		return listCostEffDate;
	}
	
	/**
	 * Compute the list cost of the item
	 * @return Returns "" string when there is no movement for the item otherwise returns list cost
	 */
	public String listCost() {
		float lstCost = 0;
		// unitcount is greater than 0
		if (unitCount != 0) {
			if (level3Cost != 0) // To avoid level3Cost becoming -0
				lstCost = level3Cost / unitCount;
			//return new DecimalFormat("#.##").format(lstCost);
			return Float.toString(PrestoUtil.round(lstCost, 2));
		} else if (unitCount == 0) {
			// Return the original value, if movement is 0
			//return new DecimalFormat("#.##").format(level3Cost);
			return Float.toString(PrestoUtil.round(level3Cost, 2));
		}
		return "";
	}
	
	/**
	 * Compute the deal cost of the item
	 * @return Returns "" string when there is no movement for the item otherwise returns deal cost
	 */
	public String dealCost() {
		float dealCost = 0;
		// If item is ondeal and unitcount is greater than 0
		if (isItemOnDeal() && unitCount != 0) {
			if (level4Cost != 0) // To avoid level4Cost becoming -0
				dealCost = level4Cost / unitCount;
			//return new DecimalFormat("#.##").format(dealCost);
			return Float.toString(PrestoUtil.round(dealCost, 2));
		} else if (isItemOnDeal() && unitCount == 0) {
			// Return the original value, if movement is 0
			//return new DecimalFormat("#.##").format(level4Cost);
			return Float.toString(PrestoUtil.round(level4Cost, 2));
		}
		return "";
	}
	
	/**
	 * Compute the deal cost start date of the item
	 * @return
	 * @throws ParseException
	 */
	public String dealCostStartDate() throws ParseException {
		String dealStartDate = "";
		//If item is ondeal, get the start date of the week
		if (isItemOnDeal()) {
			dealStartDate = DateUtil.getWeekStartDate(convertToDate(), 0);
		}
		return dealStartDate;
	}

	/**
	 * Compute the deal cost end date of the item
	 * @return
	 * @throws ParseException
	 */
	public String dealCostEndDate() throws ParseException {
		String dealEndDate = "";
		//If item is ondeal, get the end date of the week
		if (isItemOnDeal()) {
			dealEndDate = DateUtil.getWeekEndDate(convertToDate());
		}
		return dealEndDate;
	}
	
	public String getWeekStartDate() throws ParseException
	{
		String weekStartDate = "";
		weekStartDate = DateUtil.getWeekStartDate(convertToDate(), 0);
		return weekStartDate;
	}
	
	public String getWeekEndDate() throws ParseException
	{
		String weekEndDate = "";
		weekEndDate = DateUtil.getWeekEndDate(convertToDate());
		return weekEndDate;
	}
	

	/**
	 * Compute the deal cost of a unit
	 * @return
	 */
	public double getDealCostOfUnit() {		 
		double dealCost = 0;
		 
		if (unitCount != 0) {		
			dealCost = level4Cost / unitCount;			 
		}
		return dealCost;
	}
	
	public Float getUnitCount() {
		return unitCount;
	}
	
	
	/**
	 * Check whether an item to be ignored or not
	 * @return
	 */
	private Boolean isIgnorableItem(){
		Boolean ignoreItem = false;
		float regPrice = this.getGROSSDOLLARS().equals("") ? 0 : Float
				.parseFloat(this.getGROSSDOLLARS());
		float salePrice = this.getNETDOLLARS().equals("") ? 0 : Float
				.parseFloat(this.getNETDOLLARS());
		
		// Ignore if all the values are zero
		if (this.getSCANNEDUNITS().equals("0")
				&& this.getGROSSDOLLARS().equals("0")
				&& this.getNETDOLLARS().equals("0")
				&& this.getRETAILWEIGHT().equals("0")
				&& this.getPOUNDORUNITS().equals("0")
				&& this.getEXTLEVEL3ELEMENTCOST().equals("0")
				&& this.getEXTLEVEL4ELEMENTCOST().equals("0")) {
			ignoreItem = true;
		}
		// Ignore if movement is 0 and reg price is 0
		else if (this.getSCANNEDUNITS().equals("0")
				&& this.getPOUNDORUNITS().equals("0")
				&& this.getGROSSDOLLARS().equals("0")) {
			ignoreItem = true;
		}
		// Ignore if movement is 0 and reg/sale price is <0 (in negative)
		else if ((this.getSCANNEDUNITS().equals("0") && this.getPOUNDORUNITS()
				.equals("0")) && (regPrice < 0 || salePrice < 0)) {
			ignoreItem = true;
		}		
	 
		return ignoreItem;
	}
	
	/**
	 * Check whether an item to be ignored or not while loading
	 * Movement file to movement_daily table
	 * @return
	 */
	public boolean isIgnoreItemForMovementDaily()
	{
		Boolean ignoreItem = false;
		//No specific items to be ignored for movement daily apart from common ignorable item
		ignoreItem = isIgnorableItem();
		return ignoreItem;		
	}
	
	/**
	 * Check whether an item to be ignored or not while generating
	 * RetailPrice file
	 * @return
	 */
	public boolean isIgnoreItemForRetailPrice()
	{
		Boolean ignoreItem = false;
		//No specific items to be ignored for Retail Price apart from common ignorable item
		ignoreItem = isIgnorableItem();
		return ignoreItem;		
	}
	
	
	/**
	 * Check whether an item to be ignored or not while generating
	 * retailcost file
	 * @return
	 */
	public boolean isIgnoreItemForRetailCost()
	{
		Boolean ignoreItem = false;
		ignoreItem = isIgnorableItem();
		//Items which has to be ignored exclusively for retailcost		
		if (!ignoreItem) {
			float listCost = this.getEXTLEVEL3ELEMENTCOST().equals("") ? 0 : Float
					.parseFloat(this.getEXTLEVEL3ELEMENTCOST());
			
			// Ignore if list cost is in negative
			if (listCost < 0) {
				ignoreItem = true;
			}
			// 26th Sep 2012 -- Ignore items if the list cost is 0
			else if (listCost == 0) {
				ignoreItem = true;
			}
		}
		return ignoreItem;		
	}

	public boolean isIgnoreItemForHistoryLoad(){
		Boolean ignoreItem = false;
		
		// Ignore if all the values are zero
		if (this.getSCANNEDUNITS().equals("0")
				&& this.getGROSSDOLLARS().equals("0")
				&& this.getNETDOLLARS().equals("0")
				&& this.getRETAILWEIGHT().equals("0")
				&& this.getPOUNDORUNITS().equals("0")
				&& this.getEXTLEVEL3ELEMENTCOST().equals("0")
				&& this.getEXTLEVEL4ELEMENTCOST().equals("0")) {
			ignoreItem = true;
		}
		
		return ignoreItem;
	}
}
