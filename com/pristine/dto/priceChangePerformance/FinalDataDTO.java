package com.pristine.dto.priceChangePerformance;

public class FinalDataDTO {
	private String RetailerItemCode;
	private int ProductId;
	private int ProductLevelId;
	private boolean ligMember;

	private String RetailerLIRName;
	private TlogDataDTO NewData;
	private TlogDataDTO OldData;
	private boolean NewDataPresent;
	private boolean OldDataPresent;
	private boolean ToConsiderRecord;
	private double MovementDiffPerc;
	private double RevenueDiffPerc;
	private double MarginDiffPerc;
	private double PriceDiff;
	private double MovementVariance;
	private double RevenueVariance;
	private double MarginVariance;
	private double PredictionAccuracyPerc;
	private double SalePredictionAccuracyPerc;
	private double MarginPredictionAccuracyPerc;
	
	private int priceChangeIndicator;
	private int ItemCount;
	
	private float GoldMarVariance;
	private float GoldMovVariance;
	private float GoldRevVariance;
	
	private float SilMarVariance;
	private float SilMovVariance;
	private float SilRevVariance;
	
	private float RegMarVariance;
	private float RegMovVariance;
	private float RegRevVariance;
	
	
	
	
	public float getGoldMarVariance() {
		return GoldMarVariance;
	}
	public void setGoldMarVariance(float goldMarVariance) {
		GoldMarVariance = goldMarVariance;
	}
	public float getGoldMovVariance() {
		return GoldMovVariance;
	}
	public void setGoldMovVariance(float goldMovVariance) {
		GoldMovVariance = goldMovVariance;
	}
	public float getGoldRevVariance() {
		return GoldRevVariance;
	}
	public void setGoldRevVariance(float goldRevVariance) {
		GoldRevVariance = goldRevVariance;
	}
	public float getSilMarVariance() {
		return SilMarVariance;
	}
	public void setSilMarVariance(float silMarVariance) {
		SilMarVariance = silMarVariance;
	}
	public float getSilMovVariance() {
		return SilMovVariance;
	}
	public void setSilMovVariance(float silMovVariance) {
		SilMovVariance = silMovVariance;
	}
	public float getSilRevVariance() {
		return SilRevVariance;
	}
	public void setSilRevVariance(float silRevVariance) {
		SilRevVariance = silRevVariance;
	}
	public float getRegMarVariance() {
		return RegMarVariance;
	}
	public void setRegMarVariance(float regMarVariance) {
		RegMarVariance = regMarVariance;
	}
	public float getRegMovVariance() {
		return RegMovVariance;
	}
	public void setRegMovVariance(float regMovVariance) {
		RegMovVariance = regMovVariance;
	}
	public float getRegRevVariance() {
		return RegRevVariance;
	}
	public void setRegRevVariance(float regRevVariance) {
		RegRevVariance = regRevVariance;
	}
	public double getPredictionAccuracyPerc() {
		return PredictionAccuracyPerc;
	}
	public void setPredictionAccuracyPerc(double predictionAccuracyPerc) {
		PredictionAccuracyPerc = predictionAccuracyPerc;
	}
	public TlogDataDTO getNewData() {
		return NewData;
	}
	public void setNewData(TlogDataDTO newData) {
		NewData = newData;
	}
	public TlogDataDTO getOldData() {
		return OldData;
	}
	public void setOldData(TlogDataDTO oldData) {
		OldData = oldData;
	}
	public boolean isNewDataPresent() {
		return NewDataPresent;
	}
	public void setNewDataPresent(boolean newDataPresent) {
		NewDataPresent = newDataPresent;
	}
	public boolean isOldDataPresent() {
		return OldDataPresent;
	}
	public void setOldDataPresent(boolean oldDataPresent) {
		OldDataPresent = oldDataPresent;
	}
	public boolean isToConsiderRecord() {
		return ToConsiderRecord;
	}
	public void setToConsiderRecord(boolean toConsiderRecord) {
		ToConsiderRecord = toConsiderRecord;
	}
	public double getMovementDiffPerc() {
		return MovementDiffPerc;
	}
	public void setMovementDiffPerc(double d) {
		MovementDiffPerc = d;
	}
	public double getRevenueDiffPerc() {
		return RevenueDiffPerc;
	}
	public void setRevenueDiffPerc(double d) {
		RevenueDiffPerc = d;
	}
	public double getMarginDiffPerc() {
		return MarginDiffPerc;
	}
	public void setMarginDiffPerc(double d) {
		MarginDiffPerc = d;
	}
	public double getPriceDiff() {
		return PriceDiff;
	}
	public void setPriceDiff(double d) {
		PriceDiff = d;
	}
	public double getMovementVariance() {
		return MovementVariance;
	}
	public void setMovementVariance(double d) {
		MovementVariance = d;
	}
	public double getRevenueVariance() {
		return RevenueVariance;
	}
	public void setRevenueVariance(double d) {
		RevenueVariance = d;
	}
	public double getMarginVariance() {
		return MarginVariance;
	}
	public void setMarginVariance(double marginVariance) {
		MarginVariance = marginVariance;
	}
	public String getRetailerItemCode() {
		return RetailerItemCode;
	}
	public void setRetailerItemCode(String i) {
		RetailerItemCode = i;
	}
	public String getRetailerLIRName() {
		return RetailerLIRName;
	}
	public void setRetailerLIRName(String retailerLIRName) {
		RetailerLIRName = retailerLIRName;
	}
	public int getPriceChangeIndicator() {
		return priceChangeIndicator;
	}
	public void setPriceChangeIndicator(int priceChangeIndicator) {
		this.priceChangeIndicator = priceChangeIndicator;
	}
	public int getItemCount() {
		return ItemCount;
	}
	public void setItemCount(int itemCount) {
		ItemCount = itemCount;
	}
	public int getProductLevelId() {
		return ProductLevelId;
	}
	public void setProductLevelId(int productLevelId) {
		ProductLevelId = productLevelId;
	}
	public boolean isLigMember() {
		return ligMember;
	}
	public void setLigMember(boolean ligMember) {
		this.ligMember = ligMember;
	}
	public int getProductId() {
		return ProductId;
	}
	public void setProductId(int productId) {
		ProductId = productId;
	}

	public double getMarginPredictionAccuracyPerc() {
		return MarginPredictionAccuracyPerc;
	}
	public void setMarginPredictionAccuracyPerc(double marginPredictionAccuracyPerc) {
		MarginPredictionAccuracyPerc = marginPredictionAccuracyPerc;
	}
	public double getSalePredictionAccuracyPerc() {
		return SalePredictionAccuracyPerc;
	}
	public void setSalePredictionAccuracyPerc(double salePredictionAccuracyPerc) {
		SalePredictionAccuracyPerc = salePredictionAccuracyPerc;
	}
}
