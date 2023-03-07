package com.pristine.consolidatedreco;

import java.io.Serializable;
import java.sql.Date;

import com.pristine.dto.offermgmt.MultiplePrice;
import com.pristine.service.offermgmt.prediction.PredictionStatus;


public class PRRecomDTO implements Serializable {

	
	private static final long serialVersionUID = 8084443024039535860L;
	
	private long runId;
	private long recommendationId;
	private int itemCode;
	private String retLirId = "";
	private String lirIndicator;
	private int lirIdOrItemCode;
	private int childLocationLevelId;
	private int childLocationId;
	private MultiplePrice recommendedRegPrice;
//	private Integer recommendedRegMultiple;
//	private int recommendedWeekSaleMultiple;
	private MultiplePrice recommendedWeekSalePrice;
	private int recommendedWeekAdPageNo;
	private int recommendedWeekDisplayTypeId;
	private String recoStartDate;
	private String recoEndDate;
	private Double predictedMovement;
	private Double recSalesD;
	private Double recMarginD;
	private Double recWeekSalePredAtRecReg;
	private Integer recWeekSalePredStatusRec;
	private Double currentListCost;
	private Date compPriceCheckDate;
	private Integer curCompRegMultiple;
	private Double curCompRegPrice;
	private Integer curCompSaleMultiple;
	private Double curCompSalePrice;
	private Integer compLocationTypesId;
	private Integer compStrId;
	private Double curSalesD;
	private Double curMarginD;
	private Double curRegPricePredictedMov;
	private MultiplePrice curRegPrice;
	private Double recWeekSaleCost;
	private boolean isFinalConsolidated = false;
	private Integer rank;
	private Double recWeekSalePredAtCurReg;
	private Integer recWeekSalePredStatusCur;
	//private Integer recRegPricePredConfidence;
	private String regPricePredReasons;
	private String salePricePredReasons;
	private int recPricePredStatus = PredictionStatus.UNDEFINED.getStatusCode();
	
	public PRRecomDTO(){
		
	}
	
	public PRRecomDTO(PRRecomDTO prRecomDTO){
		
		this.runId = prRecomDTO.getRunId();
		this.recommendationId = prRecomDTO.getRecommendationId();
		this.itemCode = prRecomDTO.getItemCode();
		this.retLirId = prRecomDTO.getRetLirId();
		this.lirIndicator = prRecomDTO.getLirIndicator();
		this.lirIdOrItemCode = prRecomDTO.getLirIdOrItemCode();
		this.childLocationLevelId = prRecomDTO.getChildLocationLevelId();
		this.childLocationId = prRecomDTO.getChildLocationId();
		if(prRecomDTO.getRecommendedRegPrice()!=null){
			this.recommendedRegPrice = new MultiplePrice(prRecomDTO.getRecommendedRegPrice().multiple,
					prRecomDTO.getRecommendedRegPrice().price);
		}
		if(prRecomDTO.getRecommendedWeekSalePrice()!=null){
			this.recommendedWeekSalePrice = new MultiplePrice(prRecomDTO.getRecommendedWeekSalePrice().multiple,
					prRecomDTO.getRecommendedWeekSalePrice().price);
		}
		this.recommendedWeekAdPageNo = prRecomDTO.getRecommendedWeekAdPageNo();
		this.recommendedWeekDisplayTypeId = prRecomDTO.getRecommendedWeekDisplayTypeId();
		this.recoStartDate = prRecomDTO.getRecoStartDate();
		this.recoEndDate = prRecomDTO.getRecoEndDate();
		this.predictedMovement = prRecomDTO.getPredictedMovement();
		this.recSalesD = prRecomDTO.getRecSalesD();
		this.recMarginD = prRecomDTO.getRecMarginD();
		this.recWeekSalePredAtRecReg = prRecomDTO.getRecWeekSalePredAtRecReg();
		this.recWeekSalePredStatusRec = prRecomDTO.getRecWeekSalePredStatusRec();
		this.currentListCost = prRecomDTO.getCurrentListCost();
		this.compPriceCheckDate = prRecomDTO.getCompPriceCheckDate();
		this.curCompRegMultiple = prRecomDTO.getCurCompRegMultiple();
		this.curCompRegPrice = prRecomDTO.getCurCompRegPrice();
		this.curCompSaleMultiple = prRecomDTO.getCurCompSaleMultiple();
		this.curCompSalePrice = prRecomDTO.getCurCompSalePrice();
		this.compLocationTypesId = prRecomDTO.getCompLocationTypesId();
		this.compStrId = prRecomDTO.getCompStrId();		
		this.curSalesD = prRecomDTO.getCurSalesD();
		this.curMarginD = prRecomDTO.getCurMarginD();
		this.curRegPricePredictedMov = prRecomDTO.getCurRegPricePredictedMov();		
		this.curRegPrice = prRecomDTO.getCurRegPrice();		
		this.isFinalConsolidated = prRecomDTO.isFinalConsolidated();
		this.rank = prRecomDTO.getRank();
		this.recWeekSalePredAtCurReg = prRecomDTO.getRecWeekSalePredAtCurReg();
		this.recWeekSalePredStatusCur = prRecomDTO.getRecWeekSalePredStatusCur();
		//this.recRegPricePredConfidence = prRecomDTO.getRecRegPricePredConfidence();
		this.regPricePredReasons = prRecomDTO.getRegPricePredReasons();
		this.salePricePredReasons = prRecomDTO.getSalePricePredReasons();
	}
	
	
	public long getRunId() {
		return runId;
	}
	public void setRunId(long runId) {
		this.runId = runId;
	}
	public long getRecommendationId() {
		return recommendationId;
	}
	public void setRecommendationId(long recommendationId) {
		this.recommendationId = recommendationId;
	}
	public int getItemCode() {
		return itemCode;
	}
	public void setItemCode(int itemCode) {
		this.itemCode = itemCode;
	}
	public String getRetLirId() {
		return retLirId;
	}
	public void setRetLirId(String retLirId) {
		this.retLirId = retLirId;
	}
	public String getLirIndicator() {
		return lirIndicator;
	}
	public void setLirIndicator(String lirIndicator) {
		this.lirIndicator = lirIndicator;
	}
	public int getChildLocationLevelId() {
		return childLocationLevelId;
	}
	public void setChildLocationLevelId(int childLocationLevelId) {
		this.childLocationLevelId = childLocationLevelId;
	}
	public int getChildLocationId() {
		return childLocationId;
	}
	public void setChildLocationId(int childLocationId) {
		this.childLocationId = childLocationId;
	}
	public MultiplePrice getRecommendedRegPrice() {
		return recommendedRegPrice;
	}
	public void setRecommendedRegPrice(MultiplePrice recommendedRegPrice) {
		this.recommendedRegPrice = recommendedRegPrice;
	}
	public MultiplePrice getRecommendedWeekSalePrice() {
		return recommendedWeekSalePrice;
	}
	public void setRecommendedWeekSalePrice(MultiplePrice recommendedWeekSalePrice) {
		this.recommendedWeekSalePrice = recommendedWeekSalePrice;
	}
	public int getRecommendedWeekAdPageNo() {
		return recommendedWeekAdPageNo;
	}
	public void setRecommendedWeekAdPageNo(int recommendedWeekAdPageNo) {
		this.recommendedWeekAdPageNo = recommendedWeekAdPageNo;
	}
	public int getRecommendedWeekDisplayTypeId() {
		return recommendedWeekDisplayTypeId;
	}
	public void setRecommendedWeekDisplayTypeId(int recommendedWeekDisplayTypeId) {
		this.recommendedWeekDisplayTypeId = recommendedWeekDisplayTypeId;
	}
	public int getLirIdOrItemCode() {
		return lirIdOrItemCode;
	}
	public void setLirIdOrItemCode(int lirIdOrItemCode) {
		this.lirIdOrItemCode = lirIdOrItemCode;
	}
	public String getRecoStartDate() {
		return recoStartDate;
	}
	public void setRecoStartDate(String recoStartDate) {
		this.recoStartDate = recoStartDate;
	}
	public String getRecoEndDate() {
		return recoEndDate;
	}
	public void setRecoEndDate(String recoEndDate) {
		this.recoEndDate = recoEndDate;
	}
	
	
	public Double getPredictedMovement() {
		return predictedMovement;
	}
	public void setPredictedMovement(Double predictedMovement) {
		this.predictedMovement = predictedMovement;
	}
	public Double getRecSalesD() {
		return recSalesD;
	}
	public void setRecSalesD(Double recSalesD) {
		this.recSalesD = recSalesD;
	}
	public Double getRecMarginD() {
		return recMarginD;
	}
	public void setRecMarginD(Double recMarginD) {
		this.recMarginD = recMarginD;
	}
	public Double getRecWeekSalePredAtRecReg() {
		return recWeekSalePredAtRecReg;
	}
	public void setRecWeekSalePredAtRecReg(Double recWeekSalePredAtRecReg) {
		this.recWeekSalePredAtRecReg = recWeekSalePredAtRecReg;
	}
	public Integer getRecWeekSalePredStatusRec() {
		return recWeekSalePredStatusRec;
	}
	public void setRecWeekSalePredStatusRec(Integer recWeekSalePredStatusRec) {
		this.recWeekSalePredStatusRec = recWeekSalePredStatusRec;
	}
	public Double getCurrentListCost() {
		return currentListCost;
	}
	public void setCurrentListCost(Double currentListCost) {
		this.currentListCost = currentListCost;
	}
	public Date getCompPriceCheckDate() {
		return compPriceCheckDate;
	}
	public void setCompPriceCheckDate(Date compPriceCheckDate) {
		this.compPriceCheckDate = compPriceCheckDate;
	}
	public Integer getCurCompRegMultiple() {
		return curCompRegMultiple;
	}
	public void setCurCompRegMultiple(Integer curCompRegMultiple) {
		this.curCompRegMultiple = curCompRegMultiple;
	}
	public Double getCurCompRegPrice() {
		return curCompRegPrice;
	}
	public void setCurCompRegPrice(Double curCompRegPrice) {
		this.curCompRegPrice = curCompRegPrice;
	}
	public Integer getCurCompSaleMultiple() {
		return curCompSaleMultiple;
	}
	public void setCurCompSaleMultiple(Integer curCompSaleMultiple) {
		this.curCompSaleMultiple = curCompSaleMultiple;
	}
	public Double getCurCompSalePrice() {
		return curCompSalePrice;
	}
	public void setCurCompSalePrice(Double curCompSalePrice) {
		this.curCompSalePrice = curCompSalePrice;
	}
	public Integer getCompLocationTypesId() {
		return compLocationTypesId;
	}
	public void setCompLocationTypesId(Integer compLocationTypesId) {
		this.compLocationTypesId = compLocationTypesId;
	}
	public Integer getCompStrId() {
		return compStrId;
	}
	public void setCompStrId(Integer compStrId) {
		this.compStrId = compStrId;
	}
	public Double getCurSalesD() {
		return curSalesD;
	}
	public void setCurSalesD(Double curSalesD) {
		this.curSalesD = curSalesD;
	}
	public Double getCurMarginD() {
		return curMarginD;
	}
	public void setCurMarginD(Double curMarginD) {
		this.curMarginD = curMarginD;
	}
	public Double getCurRegPricePredictedMov() {
		return curRegPricePredictedMov;
	}
	public void setCurRegPricePredictedMov(Double curRegPricePredictedMov) {
		this.curRegPricePredictedMov = curRegPricePredictedMov;
	}
	public MultiplePrice getCurRegPrice() {
		return curRegPrice;
	}
	public void setCurRegPrice(MultiplePrice curRegPrice) {
		this.curRegPrice = curRegPrice;
	}
	public boolean isFinalConsolidated() {
		return isFinalConsolidated;
	}
	public void setFinalConsolidated(boolean isFinalConsolidated) {
		this.isFinalConsolidated = isFinalConsolidated;
	}
	public Double getRecWeekSaleCost() {
		return recWeekSaleCost;
	}
	public void setRecWeekSaleCost(Double recWeekSaleCost) {
		this.recWeekSaleCost = recWeekSaleCost;
	}
	public Integer getRank() {
		return rank;
	}
	public void setRank(Integer rank) {
		this.rank = rank;
	}
	public Double getRecWeekSalePredAtCurReg() {
		return recWeekSalePredAtCurReg;
	}
	public void setRecWeekSalePredAtCurReg(Double recWeekSalePredAtCurReg) {
		this.recWeekSalePredAtCurReg = recWeekSalePredAtCurReg;
	}
	public Integer getRecWeekSalePredStatusCur() {
		return recWeekSalePredStatusCur;
	}
	public void setRecWeekSalePredStatusCur(Integer recWeekSalePredStatusCur) {
		this.recWeekSalePredStatusCur = recWeekSalePredStatusCur;
	}
	/*public Integer getRecRegPricePredConfidence() {
		return recRegPricePredConfidence;
	}
	public void setRecRegPricePredConfidence(Integer recRegPricePredConfidence) {
		this.recRegPricePredConfidence = recRegPricePredConfidence;
	}*/

	public String getRegPricePredReasons() {
		return regPricePredReasons;
	}

	public void setRegPricePredReasons(String regPricePredReasons) {
		this.regPricePredReasons = regPricePredReasons;
	}

	public String getSalePricePredReasons() {
		return salePricePredReasons;
	}

	public void setSalePricePredReasons(String salePricePredReasons) {
		this.salePricePredReasons = salePricePredReasons;
	}

	public int getRecPricePredStatus() {
		return recPricePredStatus;
	}

	public void setRecPricePredStatus(int recPricePredStatus) {
		this.recPricePredStatus = recPricePredStatus;
	}
	
	
	
}