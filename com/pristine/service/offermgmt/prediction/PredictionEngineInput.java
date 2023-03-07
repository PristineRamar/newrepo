package com.pristine.service.offermgmt.prediction;


import java.util.Iterator;
import java.util.List;

import com.pristine.util.Constants;

public class PredictionEngineInput {
	int productLevelId; // category level
	int productId; //category id in which item belongs to
	//int locationLevelId; //zone
	//int locationId; // zone id
	//String predictionStartDateStr; //should be week start date (Sunday) yyyy-mm-dd format
	//String predictionEndDateStr;
	char runType = 'B';
	List<PredictionEngineItem> predictionEngineItems;
	//String useSubstFlag = String.valueOf(Constants.NO);
	String isParallel = String.valueOf(Constants.NO);
	
	public PredictionEngineInput() {
		super();
	}

//	public PredictionEngineInput(int productLevelId, int productId,
//			int locationLevelId, int locationId, String predictionStartDateStr,
//			String predictionEndDateStr, char runType,
//			List<PredictionEngineItem> predictionEngineItems, String useSubstFlag) {
//		super();
//		this.productLevelId = productLevelId;
//		this.productId = productId;
//		this.locationLevelId = locationLevelId;
//		this.locationId = locationId;
//		this.predictionStartDateStr = predictionStartDateStr;
//		this.predictionEndDateStr = predictionEndDateStr;
//		this.runType = runType;
//		this.predictionEngineItems = predictionEngineItems;
//		this.useSubstFlag = useSubstFlag;
//	}
	
	public PredictionEngineInput(int productLevelId, int productId,	char runType, List<PredictionEngineItem> predictionEngineItems,
			String isParallel) {
		super();
		this.productLevelId = productLevelId;
		this.productId = productId;
		this.runType = runType;
		this.predictionEngineItems = predictionEngineItems;
		this.isParallel = isParallel;
	}

	public String[] getPredictionStartDateArray()
	{
		String[] ret = new String[predictionEngineItems.size()];
	    Iterator<PredictionEngineItem> iterator = predictionEngineItems.iterator();
	    for (int i = 0; i < ret.length; i++)
	    {
	        ret[i] = String.valueOf(iterator.next().predictionStartDateStr);
	    }
	    return ret;
	}
	
	public String[] getPredictionPeriodStartDateArray()
	{
		String[] ret = new String[predictionEngineItems.size()];
	    Iterator<PredictionEngineItem> iterator = predictionEngineItems.iterator();
	    for (int i = 0; i < ret.length; i++)
	    {
	        ret[i] = String.valueOf(iterator.next().predictionPeriodStartDateStr);
	    }
	    return ret;
	}
	
	public int[] getLocationLevelIdArray()
	{
	    int[] ret = new int[predictionEngineItems.size()];
	    Iterator<PredictionEngineItem> iterator = predictionEngineItems.iterator();
	    for (int i = 0; i < ret.length; i++)
	    {
	        ret[i] = iterator.next().locationLevelId;
	    }
	    return ret;
	}
	
	public int[] getLocationIdArray()
	{
	    int[] ret = new int[predictionEngineItems.size()];
	    Iterator<PredictionEngineItem> iterator = predictionEngineItems.iterator();
	    for (int i = 0; i < ret.length; i++)
	    {
	        ret[i] = iterator.next().locationId;
	    }
	    return ret;
	}
	
	public int[] getItemCodeArray()
	{
	    int[] ret = new int[predictionEngineItems.size()];
	    Iterator<PredictionEngineItem> iterator = predictionEngineItems.iterator();
	    for (int i = 0; i < ret.length; i++)
	    {
	        ret[i] = iterator.next().itemCode;
	    }
	    return ret;
	}
	
	public String[] getUPCArray()
	{
		String[] ret = new String[predictionEngineItems.size()];
		//String[] ret = new String[predictionEngineItems.size()];
	    Iterator<PredictionEngineItem> iterator = predictionEngineItems.iterator();
	    for (int i = 0; i < ret.length; i++)
	    {
	        //ret[i] = String.valueOf(iterator.next().upc);
	    	ret[i] = iterator.next().upc;
	    }
	    return ret;
	}

	public double[] getRegPriceArray()
	{
	    double[] ret = new double[predictionEngineItems.size()];
	    Iterator<PredictionEngineItem> iterator = predictionEngineItems.iterator();
	    for (int i = 0; i < ret.length; i++)
	    {
	        ret[i] = iterator.next().regPrice;
	    }
	    return ret;
	}

	public int[] getRegQuantityArray()
	{
	    int[] ret = new int[predictionEngineItems.size()];
	    Iterator<PredictionEngineItem> iterator = predictionEngineItems.iterator();
	    for (int i = 0; i < ret.length; i++)
	    {
	        ret[i] = iterator.next().regQuantity;
	    }
	    return ret;
	}
	
	public String[] getUseSubstFlagArray()
	{
		String[] ret = new String[predictionEngineItems.size()];
	    Iterator<PredictionEngineItem> iterator = predictionEngineItems.iterator();
	    for (int i = 0; i < ret.length; i++)
	    {
	        ret[i] = String.valueOf(iterator.next().useSubstFlag);
	    }
	    return ret;
	}
	
	public String[] getMainOrImpactFlagArray()
	{
		String[] ret = new String[predictionEngineItems.size()];
	    Iterator<PredictionEngineItem> iterator = predictionEngineItems.iterator();
	    for (int i = 0; i < ret.length; i++)
	    {
	        ret[i] = String.valueOf(iterator.next().mainOrImpactFlag);
	    }
	    return ret;
	}
	
	public double[] getSalePriceArray()
	{
	    double[] ret = new double[predictionEngineItems.size()];
	    Iterator<PredictionEngineItem> iterator = predictionEngineItems.iterator();
	    for (int i = 0; i < ret.length; i++)
	    {
	        ret[i] = iterator.next().salePrice;
	    }
	    return ret;
	}

	public int[] getSaleQuantityArray()
	{
	    int[] ret = new int[predictionEngineItems.size()];
	    Iterator<PredictionEngineItem> iterator = predictionEngineItems.iterator();
	    for (int i = 0; i < ret.length; i++)
	    {
	        ret[i] = iterator.next().saleQuantity;
	    }
	    return ret;
	}
	
	public int[] getAdPageNoArray()
	{
	    int[] ret = new int[predictionEngineItems.size()];
	    Iterator<PredictionEngineItem> iterator = predictionEngineItems.iterator();
	    for (int i = 0; i < ret.length; i++)
	    {
	        ret[i] = iterator.next().adPageNo;
	    }
	    return ret;
	}
	
	public int[] getAdBlockNoArray()
	{
	    int[] ret = new int[predictionEngineItems.size()];
	    Iterator<PredictionEngineItem> iterator = predictionEngineItems.iterator();
	    for (int i = 0; i < ret.length; i++)
	    {
	        ret[i] = iterator.next().adBlockNo;
	    }
	    return ret;
	}
	
	public int[] getDisplayTypeIdArray()
	{
	    int[] ret = new int[predictionEngineItems.size()];
	    Iterator<PredictionEngineItem> iterator = predictionEngineItems.iterator();
	    for (int i = 0; i < ret.length; i++)
	    {
	        ret[i] = iterator.next().displayTypeId;
	    }
	    return ret;
	}
	
	public int[] getPromoTypeIdArray()
	{
	    int[] ret = new int[predictionEngineItems.size()];
	    Iterator<PredictionEngineItem> iterator = predictionEngineItems.iterator();
	    for (int i = 0; i < ret.length; i++)
	    {
	        ret[i] = iterator.next().promoTypeId;
	    }
	    return ret;
	}
	
	public int[] getSubsScenarioIdArray()
	{
	    int[] ret = new int[predictionEngineItems.size()];
	    Iterator<PredictionEngineItem> iterator = predictionEngineItems.iterator();
	    for (int i = 0; i < ret.length; i++)
	    {
	        ret[i] = iterator.next().subsScenarioId;
	    }
	    return ret;
	}
	
	public double[] getPredMovArray()
	{
		double[] ret = new double[predictionEngineItems.size()];
	    Iterator<PredictionEngineItem> iterator = predictionEngineItems.iterator();
	    for (int i = 0; i < ret.length; i++)
	    {
	        ret[i] = iterator.next().predictedMovement;
	    }
	    return ret;
	}
	
	public int[] getPredStatusArray()
	{
	    int[] ret = new int[predictionEngineItems.size()];
	    Iterator<PredictionEngineItem> iterator = predictionEngineItems.iterator();
	    for (int i = 0; i < ret.length; i++)
	    {
	        ret[i] = iterator.next().predictionStatus;
	    }
	    return ret;
	}
	
	public String getCommaSeparatedLocationLevelIds() {
	    StringBuilder sb = new StringBuilder();
	    Iterator<PredictionEngineItem> iterator = predictionEngineItems.iterator();
	    for (int i = 0; i < predictionEngineItems.size(); i++)
	    {
	    	sb.append(iterator.next().locationLevelId);
	    	if (i <= predictionEngineItems.size() - 2)
	    		sb.append(",");
	    }
	    return sb.toString();
	}
	
	public String getCommaSeparatedLocationIds() {
	    StringBuilder sb = new StringBuilder();
	    Iterator<PredictionEngineItem> iterator = predictionEngineItems.iterator();
	    for (int i = 0; i < predictionEngineItems.size(); i++)
	    {
	    	sb.append(iterator.next().locationId);
	    	if (i <= predictionEngineItems.size() - 2)
	    		sb.append(",");
	    }
	    return sb.toString();
	}
	
	public String getCommaSeparatedPredictionStartDates() {
	    StringBuilder sb = new StringBuilder();
	    Iterator<PredictionEngineItem> iterator = predictionEngineItems.iterator();
	    for (int i = 0; i < predictionEngineItems.size(); i++)
	    {
	    	sb.append(iterator.next().predictionStartDateStr);
	    	if (i <= predictionEngineItems.size() - 2)
	    		sb.append(",");
	    }
	    return sb.toString();
	}
	
	public String getCommaSeparatedPredictionPeriodStartDates() {
	    StringBuilder sb = new StringBuilder();
	    Iterator<PredictionEngineItem> iterator = predictionEngineItems.iterator();
	    for (int i = 0; i < predictionEngineItems.size(); i++)
	    {
	    	sb.append(iterator.next().predictionPeriodStartDateStr);
	    	if (i <= predictionEngineItems.size() - 2)
	    		sb.append(",");
	    }
	    return sb.toString();
	}
	
	public String getCommaSeparatedItemCodes() {
	    StringBuilder sb = new StringBuilder();
	    Iterator<PredictionEngineItem> iterator = predictionEngineItems.iterator();
	    for (int i = 0; i < predictionEngineItems.size(); i++)
	    {
	    	sb.append(iterator.next().itemCode);
	    	if (i <= predictionEngineItems.size() - 2)
	    		sb.append(",");
	    }
	    return sb.toString();
	}

	public String getCommaSeparatedUpcs() {
	    StringBuilder sb = new StringBuilder();
	    Iterator<PredictionEngineItem> iterator = predictionEngineItems.iterator();
	    for (int i = 0; i < predictionEngineItems.size(); i++)
	    {
	    	sb.append(iterator.next().upc);
	    	if (i <= predictionEngineItems.size() - 2)
	    		sb.append(",");
	    }
	    return sb.toString();
	}

	public String getCommaSeparatedRegPrices() {
	    StringBuilder sb = new StringBuilder();
	    Iterator<PredictionEngineItem> iterator = predictionEngineItems.iterator();
	    for (int i = 0; i < predictionEngineItems.size(); i++)
	    {
	    	sb.append(iterator.next().regPrice);
	    	if (i <= predictionEngineItems.size() - 2)
	    		sb.append(",");
	    }
	    return sb.toString();
	}

	public String getCommaSeparatedRegQuantities() {
	    StringBuilder sb = new StringBuilder();
	    Iterator<PredictionEngineItem> iterator = predictionEngineItems.iterator();
	    for (int i = 0; i < predictionEngineItems.size(); i++)
	    {
	    	sb.append(iterator.next().regQuantity);
	    	if (i <= predictionEngineItems.size() - 2)
	    		sb.append(",");
	    }
	    return sb.toString();
	}
	
	public String getCommaSeparatedUseSubstFlag() {
	    StringBuilder sb = new StringBuilder();
	    Iterator<PredictionEngineItem> iterator = predictionEngineItems.iterator();
	    for (int i = 0; i < predictionEngineItems.size(); i++)
	    {
	    	sb.append("\"");
	    	sb.append(String.valueOf(iterator.next().useSubstFlag));
	    	sb.append("\"");
	    	if (i <= predictionEngineItems.size() - 2)
	    		sb.append(",");
	    }
	    return sb.toString();
	}
	
	public String getCommaSeparatedMainOrImpactFlag() {
	    StringBuilder sb = new StringBuilder();
	    Iterator<PredictionEngineItem> iterator = predictionEngineItems.iterator();
	    for (int i = 0; i < predictionEngineItems.size(); i++)
	    {
	    	sb.append("\"");
	    	sb.append(String.valueOf(iterator.next().mainOrImpactFlag));
	    	sb.append("\"");
	    	if (i <= predictionEngineItems.size() - 2)
	    		sb.append(",");
	    }
	    return sb.toString();
	}
	
	public String getCommaSeparatedSalePrices() {
	    StringBuilder sb = new StringBuilder();
	    Iterator<PredictionEngineItem> iterator = predictionEngineItems.iterator();
	    for (int i = 0; i < predictionEngineItems.size(); i++)
	    {
	    	sb.append(iterator.next().salePrice);
	    	if (i <= predictionEngineItems.size() - 2)
	    		sb.append(",");
	    }
	    return sb.toString();
	}

	public String getCommaSeparatedSaleQuantities() {
	    StringBuilder sb = new StringBuilder();
	    Iterator<PredictionEngineItem> iterator = predictionEngineItems.iterator();
	    for (int i = 0; i < predictionEngineItems.size(); i++)
	    {
	    	sb.append(iterator.next().saleQuantity);
	    	if (i <= predictionEngineItems.size() - 2)
	    		sb.append(",");
	    }
	    return sb.toString();
	}
	
	public String getCommaSeparatedAdPageNo() {
	    StringBuilder sb = new StringBuilder();
	    Iterator<PredictionEngineItem> iterator = predictionEngineItems.iterator();
	    for (int i = 0; i < predictionEngineItems.size(); i++)
	    {
	    	sb.append(iterator.next().adPageNo);
	    	if (i <= predictionEngineItems.size() - 2)
	    		sb.append(",");
	    }
	    return sb.toString();
	}
	
	public String getCommaSeparatedAdBlockNo() {
	    StringBuilder sb = new StringBuilder();
	    Iterator<PredictionEngineItem> iterator = predictionEngineItems.iterator();
	    for (int i = 0; i < predictionEngineItems.size(); i++)
	    {
	    	sb.append(iterator.next().adBlockNo);
	    	if (i <= predictionEngineItems.size() - 2)
	    		sb.append(",");
	    }
	    return sb.toString();
	}
	
	public String getCommaSeparatedDisplayTypeId() {
	    StringBuilder sb = new StringBuilder();
	    Iterator<PredictionEngineItem> iterator = predictionEngineItems.iterator();
	    for (int i = 0; i < predictionEngineItems.size(); i++)
	    {
	    	sb.append(iterator.next().displayTypeId);
	    	if (i <= predictionEngineItems.size() - 2)
	    		sb.append(",");
	    }
	    return sb.toString();
	}
	
	public String getCommaSeparatedPromoTypeId() {
	    StringBuilder sb = new StringBuilder();
	    Iterator<PredictionEngineItem> iterator = predictionEngineItems.iterator();
	    for (int i = 0; i < predictionEngineItems.size(); i++)
	    {
	    	sb.append(iterator.next().promoTypeId);
	    	if (i <= predictionEngineItems.size() - 2)
	    		sb.append(",");
	    }
	    return sb.toString();
	}
	
	public String getCommaSeparatedSubScenarioIds() {
	    StringBuilder sb = new StringBuilder();
	    Iterator<PredictionEngineItem> iterator = predictionEngineItems.iterator();
	    for (int i = 0; i < predictionEngineItems.size(); i++)
	    {
	    	sb.append(iterator.next().subsScenarioId);
	    	if (i <= predictionEngineItems.size() - 2)
	    		sb.append(",");
	    }
	    return sb.toString();
	}
	
	public String getCommaSeparatedPredMov() {
	    StringBuilder sb = new StringBuilder();
	    Iterator<PredictionEngineItem> iterator = predictionEngineItems.iterator();
	    for (int i = 0; i < predictionEngineItems.size(); i++)
	    {
	    	sb.append(iterator.next().predictedMovement);
	    	if (i <= predictionEngineItems.size() - 2)
	    		sb.append(",");
	    }
	    return sb.toString();
	}
	
	public String getCommaSeparatedPredStatus() {
	    StringBuilder sb = new StringBuilder();
	    Iterator<PredictionEngineItem> iterator = predictionEngineItems.iterator();
	    for (int i = 0; i < predictionEngineItems.size(); i++)
	    {
	    	sb.append(iterator.next().predictionStatus);
	    	if (i <= predictionEngineItems.size() - 2)
	    		sb.append(",");
	    }
	    return sb.toString();
	}
	
	
	public int getProductLevelId() {
		return productLevelId;
	}

	public void setProductLevelId(int productLevelId) {
		this.productLevelId = productLevelId;
	}

	public int getProductId() {
		return productId;
	}

	public void setProductId(int productId) {
		this.productId = productId;
	}

//	public int getLocationLevelId() {
//		return locationLevelId;
//	}
//
//	public void setLocationLevelId(int locationLevelId) {
//		this.locationLevelId = locationLevelId;
//	}
//
//	public int getLocationId() {
//		return locationId;
//	}
//
//	public void setLocationId(int locationId) {
//		this.locationId = locationId;
//	}

//	public String getPredictionStartDateStr() {
//		return predictionStartDateStr;
//	}
//
//	public void setPredictionStartDateStr(String predictionStartDateStr) {
//		this.predictionStartDateStr = predictionStartDateStr;
//	}
//
//	public String getPredictionEndDateStr() {
//		return predictionEndDateStr;
//	}
//
//	public void setPredictionEndDateStr(String predictionEndDateStr) {
//		this.predictionEndDateStr = predictionEndDateStr;
//	}

	public char getRunType() {
		return runType;
	}

	public void setRunType(char runType) {
		this.runType = runType;
	}

	public List<PredictionEngineItem> getPredictionEngineItems() {
		return predictionEngineItems;
	}

	public void setPredictionEngineItems(
			List<PredictionEngineItem> predictionEngineItems) {
		this.predictionEngineItems = predictionEngineItems;
	}

//	public String getUseSubstFlag() {
//		return useSubstFlag;
//	}
//
//	public void setUseSubstFlag(String useSubstFlag) {
//		this.useSubstFlag = useSubstFlag;
//	}

	public String getIsParallel() {
		return isParallel;
	}

	public void setIsParallel(String isParallel) {
		this.isParallel = isParallel;
	}

	
	
	
}
