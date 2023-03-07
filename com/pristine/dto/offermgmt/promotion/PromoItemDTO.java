package com.pristine.dto.offermgmt.promotion;

import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;

import com.pristine.dto.offermgmt.MultiplePrice;
import com.pristine.dto.offermgmt.PRItemAdInfoDTO;
import com.pristine.dto.offermgmt.PRItemDisplayInfoDTO;
import com.pristine.dto.offermgmt.PRItemSaleInfoDTO;
import com.pristine.dto.offermgmt.PRStrategyDTO;
import com.pristine.dto.offermgmt.ProductKey;
import com.pristine.service.offermgmt.prediction.PredictionStatus;
import com.pristine.util.offermgmt.PRCommonUtil;

public class PromoItemDTO implements Cloneable{
	private ProductKey productKey;
	private int categoryId;
	private int retLirId;
	private String upc;
	private MultiplePrice regPrice;
	private MultiplePrice compPrice;
	private PRItemAdInfoDTO adInfo;
	private PRItemDisplayInfoDTO displayInfo;
	private PRItemSaleInfoDTO saleInfo;
	private Double listCost;
	private Double dealCost;
	private Double minDealCost;
	private Double derivedDealCost;
	private Double finalCost;
	private PredictionStatus predStatus;
	private PredictionStatus predStatusReg;
	private Double predMov;
	private double predMar = 0;
	private double predMarRate = 0;
	private double predRev = 0;
	private double predMovReg;
	private double predMarReg = 0;
	private double predRevReg = 0;
	private String additionalDetailForLog = "";
	private String itemName = "";
	private String retLirName = "";
	private boolean isActive = true;
	private List<PastPromoKey> promoKeys = new ArrayList<PastPromoKey>();
	private List<PRItemSaleInfoDTO> pastSaleInfo = new ArrayList<PRItemSaleInfoDTO>();
	private int noOfHHRecommendedTo = 0;
	private boolean isPresentInPreviousWeekAd = false;
	private boolean isOnTPR = false;
	private boolean isPresentInFutureWeekAd = false;
	private boolean isPriceGreaterThanCompPrice = false;
	private boolean isItemCurrentlyOnPromo = false;
	private int subCategoryId;
	private int brandId;
	private int deptId;
	private String deptName;
	private String catName;
	private boolean isPPGLeadItem;
	private HashSet<Long> ppgGroupIds = new HashSet<Long>();;
	private Long ppgGroupId = 0l;
	private boolean isPPGLevelSummary = false;
	private double itemSize;
	private int uomID;
	private int ppgPromoCombinationId = 0;
	private String brandName = "";
	private double predTotalCostOnSalePrice = 0;
	private boolean isFinalized;
	private int objectiveTypeId;
	private int promoGroupId;
	private PRStrategyDTO strategyDTO;
	private boolean isShortListed = true;
	private long scenarioId;
	private long candidateItemsId;
	private boolean isFundingAvailable;
	private int prevPromoDuration;
	private String promoFreqType;
	private int promoFrequency;
	private PromoAnalysisDTO promoAnalysisDTO;
	private double avgIncrementalUnits;
	private double avgIncrementalSales;
	private double avgIncrementalMargin;
	private boolean isDefaultEntry;
	private int rank;
	private int priceCheckListId;
	private int ligRepItemCode;
	private double predIncrementalUnits;
	private double predIncrementalSales;
	private double predIncrementalMargin;
	private boolean isGroupLevelPromo;
	private int anchorProdLevelId;
	private int anchorProdId;
	private boolean isLeadItem;
	private boolean isGuidelinesSatisfiedPromo = true;
	public ProductKey getProductKey() {
		return productKey;
	}
	public void setProductKey(ProductKey productKey) {
		this.productKey = productKey;
	}
	public MultiplePrice getRegPrice() {
		return regPrice;
	}
	public void setRegPrice(MultiplePrice regPrice) {
		this.regPrice = regPrice;
	}
	public PRItemAdInfoDTO getAdInfo() {
		return adInfo;
	}
	public void setAdInfo(PRItemAdInfoDTO adInfo) {
		this.adInfo = adInfo;
	}
	public PRItemDisplayInfoDTO getDisplayInfo() {
		return displayInfo;
	}
	public void setDisplayInfo(PRItemDisplayInfoDTO displayInfo) {
		this.displayInfo = displayInfo;
	}
	public PRItemSaleInfoDTO getSaleInfo() {
		return saleInfo;
	}
	public void setSaleInfo(PRItemSaleInfoDTO saleInfo) {
		this.saleInfo = saleInfo;
	}
	public int getCategoryId() {
		return categoryId;
	}
	public void setCategoryId(int categoryId) {
		this.categoryId = categoryId;
	}
	public String getUpc() {
		return upc;
	}
	public void setUpc(String upc) {
		this.upc = upc;
	}
	public Double getListCost() {
		return listCost;
	}
	public void setListCost(Double listCost) {
		this.listCost = listCost;
	}
	public Double getDealCost() {
		return dealCost;
	}
	public void setDealCost(Double dealCost) {
		this.dealCost = dealCost;
	}
	public PredictionStatus getPredStatus() {
		return predStatus;
	}
	public void setPredStatus(PredictionStatus predictionStatus) {
		this.predStatus = predictionStatus;
	}
	public Double getPredMov() {
		return predMov;
	}
	public void setPredMov(Double predictedMovement) {
		this.predMov = predictedMovement;
	}
	public int getRetLirId() {
		return retLirId;
	}
	public void setRetLirId(int retLirId) {
		this.retLirId = retLirId;
	}
	public double getPredMar() {
		return predMar;
	}
	public void setPredMar(double predictedMargin) {
		this.predMar = predictedMargin;
	}
	public String getAdditionalDetailForLog() {
		return additionalDetailForLog;
	}
	public void setAdditionalDetailForLog(String additionalDetailForLog) {
		this.additionalDetailForLog = additionalDetailForLog;
	}
	public String getItemName() {
		return itemName;
	}
	public void setItemName(String itemName) {
		this.itemName = itemName;
	}
	public String getRetLirName() {
		return retLirName;
	}
	public void setRetLirName(String retLirName) {
		this.retLirName = retLirName;
	}
	public boolean isActive() {
		return isActive;
	}
	public void setActive(boolean isActive) {
		this.isActive = isActive;
	}
	public List<PastPromoKey> getPromoKeys() {
		return promoKeys;
	}
	public void setPromoKeys(List<PastPromoKey> promoKeys) {
		this.promoKeys = promoKeys;
	}
	public List<PRItemSaleInfoDTO> getPastSaleInfo() {
		return pastSaleInfo;
	}
	public void setPastSaleInfo(List<PRItemSaleInfoDTO> pastSaleInfo) {
		this.pastSaleInfo = pastSaleInfo;
	}
	public double getPredRev() {
		return predRev;
	}
	public void setPredRev(double predictedRevnue) {
		this.predRev = predictedRevnue;
	}
	public MultiplePrice getCompPrice() {
		return compPrice;
	}
	public void setCompPrice(MultiplePrice compPrice) {
		this.compPrice = compPrice;
	}
	public int getNoOfHHRecommendedTo() {
		return noOfHHRecommendedTo;
	}
	public void setNoOfHHRecommendedTo(int noOfHHRecommendedTo) {
		this.noOfHHRecommendedTo = noOfHHRecommendedTo;
	}
	public boolean isPresentInPreviousWeekAd() {
		return isPresentInPreviousWeekAd;
	}
	public void setPresentInPreviousWeekAd(boolean isPresentInPreviousWeekAd) {
		this.isPresentInPreviousWeekAd = isPresentInPreviousWeekAd;
	}
	public boolean isOnTPR() {
		return isOnTPR;
	}
	public void setOnTPR(boolean isOnTPR) {
		this.isOnTPR = isOnTPR;
	}
	public boolean isPresentInFutureWeekAd() {
		return isPresentInFutureWeekAd;
	}
	public void setPresentInFutureWeekAd(boolean isPresentInFutureWeekAd) {
		this.isPresentInFutureWeekAd = isPresentInFutureWeekAd;
	}
	public boolean isPriceGreaterThanCompPrice() {
		return isPriceGreaterThanCompPrice;
	}
	public void setPriceGreaterThanCompPrice(boolean isPriceGreaterThanCompPrice) {
		this.isPriceGreaterThanCompPrice = isPriceGreaterThanCompPrice;
	}
	public boolean isItemCurrentlyOnPromo() {
		return isItemCurrentlyOnPromo;
	}
	public void setItemCurrentlyOnPromo(boolean isItemCurrentlyOnPromo) {
		this.isItemCurrentlyOnPromo = isItemCurrentlyOnPromo;
	}
	public int getSubCategoryId() {
		return subCategoryId;
	}
	public void setSubCategoryId(int subCategoryId) {
		this.subCategoryId = subCategoryId;
	}
	public int getBrandId() {
		return brandId;
	}
	public void setBrandId(int brandId) {
		this.brandId = brandId;
	}
	public int getDeptId() {
		return deptId;
	}
	public void setDeptId(int deptId) {
		this.deptId = deptId;
	}
	public String getDeptName() {
		return deptName;
	}
	public void setDeptName(String deptName) {
		this.deptName = deptName;
	}
	public String getCatName() {
		return catName;
	}
	public void setCatName(String catName) {
		this.catName = catName;
	}
	public double getPredMovReg() {
		return predMovReg;
	}
	public void setPredMovReg(double predictedMovementRegularPrice) {
		this.predMovReg = predictedMovementRegularPrice;
	}
	public double getPredMarReg() {
		return predMarReg;
	}
	public void setPredMarReg(double predictedMarginRegularPrice) {
		this.predMarReg = predictedMarginRegularPrice;
	}
	public double getPredRevReg() {
		return predRevReg;
	}
	public void setPredRevReg(double predictedRevnueRegularPrice) {
		this.predRevReg = predictedRevnueRegularPrice;
	}
	public PredictionStatus getPredStatusReg() {
		return predStatusReg;
	}
	public void setPredStatusReg(PredictionStatus predictionStatusRegularPrice) {
		this.predStatusReg = predictionStatusRegularPrice;
	}
	public Double getMinDealCost() {
		return minDealCost;
	}
	public void setMinDealCost(Double minDealCost) {
		this.minDealCost = minDealCost;
	}
	public Double getDerivedDealCost() {
		return derivedDealCost;
	}
	public void setDerivedDealCost(Double derivedDealCost) {
		this.derivedDealCost = derivedDealCost;
	}
	public boolean isPPGLeadItem() {
		return isPPGLeadItem;
	}
	public void setPPGLeadItem(boolean isPPGLeadItem) {
		this.isPPGLeadItem = isPPGLeadItem;
	}
	public HashSet<Long> getPpgGroupIds() {
		return ppgGroupIds;
	}
	public void setPpgGroupIds(HashSet<Long> ppgGroupIds) {
		this.ppgGroupIds = ppgGroupIds;
	}
	public Long getPpgGroupId() {
		return ppgGroupId;
	}
	public void setPpgGroupId(Long ppgGroupId) {
		this.ppgGroupId = ppgGroupId;
	}
	public Double getFinalCost() {
		return finalCost;
	}
	public void setFinalCost(Double finalCost) {
		this.finalCost = finalCost;
	}
	public boolean isPPGLevelSummary() {
		return isPPGLevelSummary;
	}
	public void setPPGLevelSummary(boolean isPPGLevelSummary) {
		this.isPPGLevelSummary = isPPGLevelSummary;
	}	
	
	public double getSaleUnitsLiftPCTAgainstRegUnits() {
		return PRCommonUtil.getLiftPCT(this.predMovReg, this.predMov);
	}
	public double getSaleRevLiftPCTAgainstRegRev() {
		return PRCommonUtil.getLiftPCT(this.predRevReg, this.predRev);
	}
	public double getSaleMarLiftPCTAgainstRegMar() {
		return PRCommonUtil.getLiftPCT(this.predMarReg, this.predMar);
	}
	
	public double getItemSize() {
		return itemSize;
	}
	public void setItemSize(double itemSize) {
		this.itemSize = itemSize;
	}
	public int getUomID() {
		return uomID;
	}
	public void setUomID(int uomID) {
		this.uomID = uomID;
	}
	public double getSaleDiscountPCT() {
		return PRCommonUtil.getSaleDiscountPCT(this.getRegPrice(), (this.getSaleInfo() != null ? this.getSaleInfo().getSalePrice() : null), false);
	}
	@Override
	public String toString() {
		return "PromoItemDTO [productKey=" + productKey + ", saleInfo=" + saleInfo + ", promoAnalysis="
				+ promoAnalysisDTO + ", rank=" + rank + ", AnchorProdLevelId=" + anchorProdLevelId + ", AnchorProdId="
				+ anchorProdId + "]";
	}
	public int getPpgPromoCombinationId() {
		return ppgPromoCombinationId;
	}
	public void setPpgPromoCombinationId(int ppgPromoCombinationId) {
		this.ppgPromoCombinationId = ppgPromoCombinationId;
	}
	public String getBrandName() {
		return brandName;
	}
	public void setBrandName(String brandName) {
		this.brandName = brandName;
	}
	
	public double getSaleMarginPCT() {
		return PRCommonUtil.getMarginPCT(this.predRev, this.predTotalCostOnSalePrice);
	}
	public double getPredTotalCostOnSalePrice() {
		return predTotalCostOnSalePrice;
	}
	public void setPredTotalCostOnSalePrice(double predTotalCostOnSalePrice) {
		this.predTotalCostOnSalePrice = predTotalCostOnSalePrice;
	}
	public boolean isFinalized() {
		return isFinalized;
	}
	public void setFinalized(boolean isFinalized) {
		this.isFinalized = isFinalized;
	}
	public int getObjectiveTypeId() {
		return objectiveTypeId;
	}
	public void setObjectiveTypeId(int objectiveTypeId) {
		this.objectiveTypeId = objectiveTypeId;
	}
	public int getPromoGroupId() {
		return promoGroupId;
	}
	public void setPromoGroupId(int promoGroupId) {
		this.promoGroupId = promoGroupId;
	}
	public PRStrategyDTO getStrategyDTO() {
		return strategyDTO;
	}
	public void setStrategyDTO(PRStrategyDTO strategyDTO) {
		this.strategyDTO = strategyDTO;
	}
	public double getPredMarRate() {
		return predMarRate;
	}
	public void setPredMarRate(double predMarRate) {
		this.predMarRate = predMarRate;
	}
	public boolean isShortListed() {
		return isShortListed;
	}
	public void setShortListed(boolean isShortListed) {
		this.isShortListed = isShortListed;
	}
	public long getScenarioId() {
		return scenarioId;
	}
	public void setScenarioId(long scenarioId) {
		this.scenarioId = scenarioId;
	}
	public long getCandidateItemsId() {
		return candidateItemsId;
	}
	public boolean isFundingAvailable() {
		return isFundingAvailable;
	}
	public int getPrevPromoDuration() {
		return prevPromoDuration;
	}
	public String getPromoFreqType() {
		return promoFreqType;
	}
	public int getPromoFrequency() {
		return promoFrequency;
	}
	public void setCandidateItemsId(long candidateItemsId) {
		this.candidateItemsId = candidateItemsId;
	}
	public void setFundingAvailable(boolean isFundingAvailable) {
		this.isFundingAvailable = isFundingAvailable;
	}
	public void setPrevPromoDuration(int prevPromoDuration) {
		this.prevPromoDuration = prevPromoDuration;
	}
	public void setPromoFreqType(String promoFreqType) {
		this.promoFreqType = promoFreqType;
	}
	public void setPromoFrequency(int promoFrequency) {
		this.promoFrequency = promoFrequency;
	}
	public PromoAnalysisDTO getPromoAnalysisDTO() {
		return promoAnalysisDTO;
	}
	public void setPromoAnalysisDTO(PromoAnalysisDTO promoAnalysisDTO) {
		this.promoAnalysisDTO = promoAnalysisDTO;
	}

	@Override
    public Object clone() throws CloneNotSupportedException {
		return (PromoItemDTO) super.clone();
	}
	public double getAvgIncrementalUnits() {
		return avgIncrementalUnits;
	}
	public void setAvgIncrementalUnits(double avgIncrementalUnits) {
		this.avgIncrementalUnits = avgIncrementalUnits;
	}
	public double getAvgIncrementalSales() {
		return avgIncrementalSales;
	}
	public void setAvgIncrementalSales(double avgIncrementalSales) {
		this.avgIncrementalSales = avgIncrementalSales;
	}
	public double getAvgIncrementalMargin() {
		return avgIncrementalMargin;
	}
	public void setAvgIncrementalMargin(double avgIncrementalMargin) {
		this.avgIncrementalMargin = avgIncrementalMargin;
	}
	public boolean isDefaultEntry() {
		return isDefaultEntry;
	}
	public void setDefaultEntry(boolean isDefaultEntry) {
		this.isDefaultEntry = isDefaultEntry;
	}
	public int getRank() {
		return rank;
	}
	public void setRank(int rank) {
		this.rank = rank;
	}
	public int getPriceCheckListId() {
		return priceCheckListId;
	}
	public void setPriceCheckListId(int priceCheckListId) {
		this.priceCheckListId = priceCheckListId;
	}
	public int getLigRepItemCode() {
		return ligRepItemCode;
	}
	public void setLigRepItemCode(int ligRepItemCode) {
		this.ligRepItemCode = ligRepItemCode;
	}
	public double getPredIncrementalUnits() {
		return predIncrementalUnits;
	}
	public void setPredIncrementalUnits(double predIncrementalUnits) {
		this.predIncrementalUnits = predIncrementalUnits;
	}
	public double getPredIncrementalSales() {
		return predIncrementalSales;
	}
	public void setPredIncrementalSales(double predIncrementalSales) {
		this.predIncrementalSales = predIncrementalSales;
	}
	public double getPredIncrementalMargin() {
		return predIncrementalMargin;
	}
	public void setPredIncrementalMargin(double predIncrementalMargin) {
		this.predIncrementalMargin = predIncrementalMargin;
	}
	public boolean isGroupLevelPromo() {
		return isGroupLevelPromo;
	}
	public void setGroupLevelPromo(boolean isGroupLevelPromo) {
		this.isGroupLevelPromo = isGroupLevelPromo;
	}
	public int getAnchorProdLevelId() {
		return anchorProdLevelId;
	}
	public void setAnchorProdLevelId(int anchorProdLevelId) {
		this.anchorProdLevelId = anchorProdLevelId;
	}
	public int getAnchorProdId() {
		return anchorProdId;
	}
	public void setAnchorProdId(int anchorProdId) {
		this.anchorProdId = anchorProdId;
	}
	public ProductKey getAnchorProductKey() {
		ProductKey anchorProdKey = null;
		if (this.anchorProdLevelId > 0 && this.anchorProdId > 0) {
			anchorProdKey = new ProductKey(anchorProdLevelId, anchorProdId);
		}
		return anchorProdKey;
	}
	public boolean isLeadItem() {
		return isLeadItem;
	}
	public void setLeadItem(boolean isLeadItem) {
		this.isLeadItem = isLeadItem;
	}
	public boolean isGuidelinesSatisfiedPromo() {
		return isGuidelinesSatisfiedPromo;
	}
	public void setGuidelinesSatisfiedPromo(boolean isGuidelinesSatisfiedPromo) {
		this.isGuidelinesSatisfiedPromo = isGuidelinesSatisfiedPromo;
	}
}
