package com.pristine.dto.offermgmt.mwr;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;

import com.pristine.dto.offermgmt.LocationKey;
import com.pristine.dto.offermgmt.MultiplePrice;
import com.pristine.dto.offermgmt.PRExplainLog;
import com.pristine.dto.offermgmt.PRItemDTO;
import com.pristine.dto.offermgmt.PRItemSaleInfoDTO;
import com.pristine.dto.offermgmt.SecondaryZoneRecDTO;
import com.pristine.dto.offermgmt.prediction.PricePointDTO;
import com.pristine.util.Constants;
import com.pristine.util.offermgmt.PRCommonUtil;

public class MWRItemDTO implements Serializable, Cloneable {
	/**
	 * 
	 */
	private static final long serialVersionUID = 1998363574553319529L;
	private long mwRecommendationId;
	private long runId;
	private int weekCalendarId;
	private int periodCalendarId;
	private int productLevelId;
	private int productId;
	private int retLirId;
	private int itemCode;
	private boolean isLir;
	private long strategyId;
	private double itemSize;
	private String uomId;
	private Integer priceCheckListId;
	private Integer ligRepItemCode;
	private Integer regMultiple;
	private Double regPrice;
	private Integer saleMultiple;
	private Double salePrice;
	private Integer promoTypeId;
	private Integer adPageNo;
	private Integer adBlockNo;
	private Integer displayTypeId;
	private String saleStartDate;
	private String saleEndDate;
	private Double listCost = null;
	private Double prevListCost = null;
	private Double dealCost = null;
	private Double vipCost = null;
	private MultiplePrice recommendedRegPrice;
	private MultiplePrice recommendedRegPriceBeforeUpdateRec=null;
	private Double finalPricePredictedMovement;
	private Integer finalPricePredictionStatus;
	transient private PRExplainLog explainLog = new PRExplainLog();
	transient private List<Integer> recErrorCodes = new ArrayList<Integer>();
	private int costChangeIndicator;
	private int compPriceChangeIndicator;
	private Integer compStrId;
	private Integer compRegMultiple;
	private Double compRegPrice;
	private Integer compSaleMultiple;
	private Double compSalePrice;
	private String compPriceCheckDate;
	private Double finalPriceRevenue;
	private Double finalPriceMargin;
	private Double[] priceRange;
	private String upc;
	private int isConflict = 0;
	private boolean isNewPriceRecommended = false;
	private Double regRevenue;
	private Double saleRevenue;
	private Double regUnits;
	private Double saleUnits;
	private Double regMargin;
	private Double saleMargin;
	transient private HashMap<MultiplePrice, PricePointDTO> regPricePredictionMap = new HashMap<MultiplePrice, PricePointDTO>();
	transient private HashMap<MultiplePrice, PricePointDTO> salePricePredictionMap = new HashMap<MultiplePrice, PricePointDTO>();
	private Double currentRegRevenue;
	private Double currentRegUnits;
	private Double currentRegMargin;
	private int recWeekStartCalendarId;
	private MultiplePrice prevCompPrice;
	private String costEffDate;
	private String currPriceEffDate;
	private boolean isRecError = false;
	private String EDLPORPROMO;
	private boolean isRecUpdated;
	private RecWeekKey recWeekKey;
	private String calType;
	private MultiplePrice overrideRegPrice=null;
	private MultiplePrice currentPrice;
	private String weekStartDate;
	private boolean isSystemOverrideFlag;
	private boolean isUserOverrideFlag;
	private PRItemSaleInfoDTO immediatePromoInfo;
	private int isTPR;
	private int isPrePriced;
	private int isLockPriced;
	private int vendorId;
	private MultiplePrice futurePrice;
	private String futurePriceEffDate;
	private Double futureCost=null;
	private String futureCostEffDate=null;
	private Double futureWeekPrice=null; //similar to futureCost. NOT to be confused with futurePrice
	private String futureWeekPriceEffDate=null; //similar to futureCostEffDate. NOT to be confused with futurePriceEffDate
	private boolean sendToPrediction;
	
	private Integer criteriaId;
	private Integer comp1StrId;
	private Integer comp2StrId;
	private Integer comp3StrId;
	private Integer comp4StrId;
	private Integer comp5StrId;
	private MultiplePrice comp1Retail;
	private MultiplePrice comp2Retail;
	private MultiplePrice comp3Retail;
	private MultiplePrice comp4Retail;
	private MultiplePrice comp5Retail;
	private Double coreRetail;
	private Double cwacCoreCost;
	private Double priceChangeImpact;
	private double recentXWeeksMov;
	private boolean noRecentWeeksMovement;
	private double avgMovement;
	private double originalListCost;
	
	private MultiplePrice postdatedPrice;
	private String postDatedPriceEffDate;
	private List<SecondaryZoneRecDTO> secondaryZones;
	private boolean isSecondaryZoneRecPresent;
	
	private Double futureCostForQtrLevel=null;
	private String futureCostEffDateForQtrLevel=null;
	
	private double xWeeksMov;
	private int overrideRemoved;
	private double nipoBaseCost;
	private double familyUnits;
	private int storeCount;
	private int isFreightChargeIncluded=0;
	private double mapRetail=0;
	private boolean isnewImpactCalculated;
	private double recommendedPricewithMap=0;
	private boolean currentPriceBelowMAP=false;
	
	private double cwagBaseCost=0;
	
	private MultiplePrice pendingRetail=null;
	private double approvedImpact=0;
	private int isPendingRetailRecommended=0;
	private char isImpactIncludedInSummaryCalculation;
	private boolean isFuturePricePresent=false;
	
	public long getMwRecommendationId() {
		return mwRecommendationId;
	}

	public void setMwRecommendationId(long mwRecommendationId) {
		this.mwRecommendationId = mwRecommendationId;
	}

	public long getRunId() {
		return runId;
	}

	public void setRunId(long runId) {
		this.runId = runId;
	}

	public int getWeekCalendarId() {
		return weekCalendarId;
	}

	public void setWeekCalendarId(int weekCalendarId) {
		this.weekCalendarId = weekCalendarId;
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

	public int getRetLirId() {
		return retLirId;
	}

	public void setRetLirId(int retLirId) {
		this.retLirId = retLirId;
	}

	public long getStrategyId() {
		return strategyId;
	}

	public void setStrategyId(long strategyId) {
		this.strategyId = strategyId;
	}

	public double getItemSize() {
		return itemSize;
	}

	public void setItemSize(double itemSize) {
		this.itemSize = itemSize;
	}

	public String getUomId() {
		return uomId;
	}

	public void setUomId(String uomId) {
		this.uomId = uomId;
	}

	public Integer getPriceCheckListId() {
		return priceCheckListId;
	}

	public void setPriceCheckListId(Integer priceCheckListId) {
		this.priceCheckListId = priceCheckListId;
	}

	public Integer getLigRepItemCode() {
		return ligRepItemCode;
	}

	public void setLigRepItemCode(Integer ligRepItemCode) {
		this.ligRepItemCode = ligRepItemCode;
	}

	public Integer getRegMultiple() {
		return regMultiple;
	}

	public void setRegMultiple(Integer regMultiple) {
		this.regMultiple = regMultiple;
	}

	public Double getRegPrice() {
		return regPrice;
	}

	public void setRegPrice(Double regPrice) {
		this.regPrice = regPrice;
	}

	public Integer getSaleMultiple() {
		return saleMultiple;
	}

	public void setSaleMultiple(Integer saleMultiple) {
		this.saleMultiple = saleMultiple;
	}

	public Double getSalePrice() {
		return salePrice;
	}

	public void setSalePrice(Double salePrice) {
		this.salePrice = salePrice;
	}

	public Integer getPromoTypeId() {
		return promoTypeId;
	}

	public void setPromoTypeId(Integer promoTypeId) {
		this.promoTypeId = promoTypeId;
	}

	public Integer getAdPageNo() {
		return adPageNo;
	}

	public void setAdPageNo(Integer adPageNo) {
		this.adPageNo = adPageNo;
	}

	public Integer getAdBlockNo() {
		return adBlockNo;
	}

	public void setAdBlockNo(Integer adBlockNo) {
		this.adBlockNo = adBlockNo;
	}

	public Integer getDisplayTypeId() {
		return displayTypeId;
	}

	public void setDisplayTypeId(Integer displayTypeId) {
		this.displayTypeId = displayTypeId;
	}

	public String getSaleStartDate() {
		return saleStartDate;
	}

	public void setSaleStartDate(String saleStartDate) {
		this.saleStartDate = saleStartDate;
	}

	public String getSaleEndDate() {
		return saleEndDate;
	}

	public void setSaleEndDate(String saleEndDate) {
		this.saleEndDate = saleEndDate;
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

	public Double getFinalPricePredictedMovement() {
		return finalPricePredictedMovement;
	}

	public void setFinalPricePredictedMovement(Double finalPricePredictedMovement) {
		this.finalPricePredictedMovement = finalPricePredictedMovement;
	}

	public Integer getFinalPricePredictionStatus() {
		return finalPricePredictionStatus;
	}

	public void setFinalPricePredictionStatus(Integer finalPricePredictionStatus) {
		this.finalPricePredictionStatus = finalPricePredictionStatus;
	}

	public PRExplainLog getExplainLog() {
		return explainLog;
	}

	public void setExplainLog(PRExplainLog explainLog) {
		this.explainLog = explainLog;
	}

	public int getCostChangeIndicator() {
		return costChangeIndicator;
	}

	public void setCostChangeIndicator(int costChangeIndicator) {
		this.costChangeIndicator = costChangeIndicator;
	}

	public int getCompPriceChangeIndicator() {
		return compPriceChangeIndicator;
	}

	public void setCompPriceChangeIndicator(int compPriceChangeIndicator) {
		this.compPriceChangeIndicator = compPriceChangeIndicator;
	}

	public Integer getCompRegMultiple() {
		return compRegMultiple;
	}

	public void setCompRegMultiple(Integer compRegMultiple) {
		this.compRegMultiple = compRegMultiple;
	}

	public Double getCompRegPrice() {
		return compRegPrice;
	}

	public void setCompRegPrice(Double compRegPrice) {
		this.compRegPrice = compRegPrice;
	}

	public Integer getCompSaleMultiple() {
		return compSaleMultiple;
	}

	public void setCompSaleMultiple(Integer compSaleMultiple) {
		this.compSaleMultiple = compSaleMultiple;
	}

	public Double getCompSalePrice() {
		return compSalePrice;
	}

	public void setCompSalePrice(Double compSalePrice) {
		this.compSalePrice = compSalePrice;
	}

	public String getCompPriceCheckDate() {
		return compPriceCheckDate;
	}

	public void setCompPriceCheckDate(String compPriceCheckDate) {
		this.compPriceCheckDate = compPriceCheckDate;
	}

	public Double getFinalPriceRevenue() {
		return finalPriceRevenue;
	}

	public void setFinalPriceRevenue(Double finalPriceRevenue) {
		this.finalPriceRevenue = finalPriceRevenue;
	}

	public Double getFinalPriceMargin() {
		return finalPriceMargin;
	}

	public void setFinalPriceMargin(Double finalPriceMargin) {
		this.finalPriceMargin = finalPriceMargin;
	}

	public Double[] getPriceRange() {
		return priceRange;
	}

	public void setPriceRange(Double[] priceRange) {
		this.priceRange = priceRange;
	}

	
	public Double getCost() {
		if (this.getVipCost() != null && this.getVipCost() > 0)
			return this.getVipCost();
		else
			return this.getListCost();
	}
	
	public Double getFinalCost() {
		if (this.getVipCost() != null && this.getVipCost() > 0)
			return this.getVipCost();
		else if(this.getDealCost() != null && this.getDealCost() > 0)
			return this.getDealCost();
		else 
			return this.getListCost();
	}
	
	/**
	 * copies data from PRItemDTO to MWRItemDTO
	 * 
	 * @param prItemDTO
	 */
	public void copyFromWeeklyRecObj(PRItemDTO prItemDTO) {
		this.mwRecommendationId = prItemDTO.getPrRecommendationId();
		this.runId = prItemDTO.getRunId();
		this.retLirId = prItemDTO.getRetLirId();
		this.itemCode = prItemDTO.getItemCode();
		this.productLevelId = prItemDTO.isLir() ? Constants.PRODUCT_LEVEL_ID_LIG : Constants.ITEMLEVELID;
		this.productId = prItemDTO.getItemCode();
		this.isLir = prItemDTO.isLir();
		this.strategyId = prItemDTO.getStrategyId();
		this.itemSize = prItemDTO.getItemSize();
		this.uomId = prItemDTO.getUOMId();
		this.priceCheckListId = prItemDTO.getPriceCheckListId();
		this.ligRepItemCode = prItemDTO.getLigRepItemCode();
		// Set recommended price as current retail
		MultiplePrice curRegPrice = PRCommonUtil.getMultiplePrice(prItemDTO.getRegMPack(),
				prItemDTO.getRegPrice(), prItemDTO.getRegMPrice());
		if(curRegPrice != null){
			this.regMultiple = curRegPrice.multiple;
			this.regPrice = curRegPrice.price;	
		}
		// Set recommended price as current retail
		MultiplePrice salePrice = PRCommonUtil.getMultiplePrice(prItemDTO.getSaleMPack(),
				prItemDTO.getSalePrice(), prItemDTO.getSaleMPrice());
		if(salePrice != null){
			this.saleMultiple = salePrice.multiple;
			this.salePrice = salePrice.price;	
		}
		this.recommendedRegPrice = prItemDTO.getRecommendedRegPrice();
		this.promoTypeId = prItemDTO.getPromoTypeId();
		this.adPageNo = prItemDTO.getPageNumber();
		this.adBlockNo = prItemDTO.getBlockNumber();
		this.displayTypeId = prItemDTO.getDisplayTypeId();
		this.listCost = prItemDTO.getListCost();
		this.dealCost = prItemDTO.getDealCost();
		/*this.finalPricePredictedMovement = prItemDTO.getPredictedMovement();
		this.finalPricePredictionStatus = prItemDTO.getPredictionStatus();*/
		this.explainLog = prItemDTO.getExplainLog();
		this.costChangeIndicator = prItemDTO.getCostChgIndicator();
		this.prevListCost = prItemDTO.getPreListCost();
		this.setCompStrId(prItemDTO.getCompStrId() == null ? null : prItemDTO.getCompStrId().getLocationId());
		if(this.getCompStrId() != null && this.getCompStrId() > 0) {
			LocationKey locationKey = new LocationKey(Constants.STORE_LEVEL_ID, this.getCompStrId());
			if(prItemDTO.getAllCompPrice().get(locationKey) != null) {
				MultiplePrice compRegPrice = prItemDTO.getAllCompPrice().get(locationKey);
				this.compRegMultiple = compRegPrice.multiple;
				this.compRegPrice = compRegPrice.price;
			}
			if(prItemDTO.getAllCompPreviousPrice().get(locationKey) != null) {
				MultiplePrice compPrevRegPrice = prItemDTO.getAllCompPreviousPrice().get(locationKey);
				this.prevCompPrice = compPrevRegPrice;
			}
			if(prItemDTO.getAllCompSalePrice().get(locationKey) != null) {
				MultiplePrice compSalePrice = prItemDTO.getAllCompSalePrice().get(locationKey);
				this.compSaleMultiple = compSalePrice.multiple;
				this.compSalePrice = compSalePrice.price;	
			}
			if(prItemDTO.getAllCompPriceCheckDate().get(locationKey) != null) {
				this.compPriceCheckDate = prItemDTO.getAllCompPriceCheckDate().get(locationKey);
			}
			if(prItemDTO.getAllCompPriceChgIndicator().get(locationKey) != null) {
				this.compPriceChangeIndicator = prItemDTO.getAllCompPriceChgIndicator().get(locationKey);
			}
		}
		this.priceRange = prItemDTO.getPriceRange();
		this.upc = prItemDTO.getUpc();
		this.isConflict = prItemDTO.getIsConflict();
		this.vipCost = prItemDTO.getVipCost();
		this.costEffDate = prItemDTO.getListCostEffDate();
		this.currPriceEffDate = prItemDTO.getCurRegPriceEffDate();
		this.isRecError = prItemDTO.getIsRecError();
		this.recErrorCodes = prItemDTO.getRecErrorCodes();
		this.immediatePromoInfo = prItemDTO.getRecWeekSaleInfo().getSalePrice() == null ? prItemDTO.getFutWeekSaleInfo()
				: prItemDTO.getRecWeekSaleInfo();
		this.isTPR = prItemDTO.getIsTPR();
		this.isPrePriced = prItemDTO.getIsPrePriced();
		this.isLockPriced = prItemDTO.getIsLocPriced();
		this.futurePrice = prItemDTO.getFutureRecRetail();
		this.futurePriceEffDate = prItemDTO.getRecPriceEffectiveDate();
		this.vendorId = prItemDTO.getVendorId();
		this.criteriaId = prItemDTO.getCriteriaId();
		this.comp1StrId = prItemDTO.getComp1StrId();
		this.comp2StrId = prItemDTO.getComp2StrId();
		this.comp3StrId = prItemDTO.getComp3StrId();
		this.comp4StrId = prItemDTO.getComp4StrId();
		this.comp5StrId = prItemDTO.getComp5StrId();
		this.comp1Retail = prItemDTO.getComp1Retail();
		this.comp2Retail = prItemDTO.getComp2Retail();
		this.comp3Retail = prItemDTO.getComp3Retail();
		this.comp4Retail = prItemDTO.getComp4Retail();
		this.comp5Retail = prItemDTO.getComp5Retail();
		this.cwacCoreCost = prItemDTO.getCwacCoreCost();
		this.coreRetail = prItemDTO.getCoreRetail();
		this.priceChangeImpact = prItemDTO.getPriceChangeImpact();
		this.noRecentWeeksMovement = prItemDTO.isNoRecentWeeksMov();
		this.recentXWeeksMov = prItemDTO.getRecentXWeeksMov();
		this.sendToPrediction=prItemDTO.isSendToPrediction();
		this.avgMovement=prItemDTO.getAvgMovement();
		this.originalListCost=prItemDTO.getOriginalListCost();
		this.secondaryZones = prItemDTO.getSecondaryZones();
		this.isSecondaryZoneRecPresent = prItemDTO.isSecondaryZoneRecPresent();
		this.setxWeeksMov(prItemDTO.getXweekMov());
		this.setOverrideRemoved(prItemDTO.getOverrideRemoved());
		this.setFutureCost(prItemDTO.getFutureListCost());
		this.setFutureCostEffDate(prItemDTO.getFutureCostEffDate());
		this.setNipoBaseCost(prItemDTO.getNipoBaseCost());
		this.setStoreCount(prItemDTO.getNoOfStoresItemAuthorized());
		this.setFamilyUnits(prItemDTO.getFamilyXWeeksMov());
		this.setIsFreightChargeIncluded(prItemDTO.getFreightChargeIncluded());
		this.setMapRetail(prItemDTO.getMapRetail());
		this.recommendedPricewithMap=prItemDTO.getRecommendedPricewithMap();
		this.currentPriceBelowMAP=prItemDTO.isCurrentPriceBelowMAP();
		this.setCwagBaseCost(prItemDTO.getCwagBaseCost());
		//added for copying  the approved impact for pending retails
		this.approvedImpact=prItemDTO.getApprovedImpact();
		this.pendingRetail=prItemDTO.getPendingRetail();
		this.isPendingRetailRecommended=prItemDTO.getIsPendingRetailRecommended();
		this.setFuturePricePresent(prItemDTO.isFuturePricePresent());
		this.setFutureWeekPrice(prItemDTO.getFutureUnitPrice());
		this.setFutureWeekPriceEffDate(prItemDTO.getFuturePriceEffDate());
	
	}

	public int getItemCode() {
		return itemCode;
	}

	public void setItemCode(int itemCode) {
		this.itemCode = itemCode;
	}

	public boolean isLir() {
		return isLir;
	}

	public void setLir(boolean isLir) {
		this.isLir = isLir;
	}

	public String getUpc() {
		return upc;
	}

	public void setUpc(String upc) {
		this.upc = upc;
	}

	public HashMap<MultiplePrice, PricePointDTO> getRegPricePredictionMap() {
		return regPricePredictionMap;
	}

	public void setRegPricePredictionMap(HashMap<MultiplePrice, PricePointDTO> regPricePredictionMap) {
		this.regPricePredictionMap = regPricePredictionMap;
	}

	public void addRegPricePrediction(MultiplePrice multiplePrice, PricePointDTO movement) {
		this.regPricePredictionMap.put(multiplePrice, movement);
	}

	public MultiplePrice getRecommendedRegPrice() {
		return recommendedRegPrice;
	}

	public void setRecommendedRegPrice(MultiplePrice recommendedRegPrice) {
		this.recommendedRegPrice = recommendedRegPrice;
	}

	public int getIsConflict() {
		return isConflict;
	}

	public void setIsConflict(int isConflict) {
		this.isConflict = isConflict;
	}

	public Double getVipCost() {
		return vipCost;
	}

	public void setVipCost(Double vipCost) {
		this.vipCost = vipCost;
	}

	public HashMap<MultiplePrice, PricePointDTO> getSalePricePredictionMap() {
		return salePricePredictionMap;
	}

	public void setSalePricePredictionMap(HashMap<MultiplePrice, PricePointDTO> salePricePredictionMap) {
		this.salePricePredictionMap = salePricePredictionMap;
	}

	public void addSalePricePrediction(MultiplePrice multiplePrice, PricePointDTO movement) {
		this.salePricePredictionMap.put(multiplePrice, movement);
	}

	public Integer getCompStrId() {
		return compStrId;
	}

	public void setCompStrId(Integer compStrId) {
		this.compStrId = compStrId;
	}

	public boolean isNewPriceRecommended() {
		return isNewPriceRecommended;
	}

	public void setNewPriceRecommended(boolean isNewPriceRecommended) {
		this.isNewPriceRecommended = isNewPriceRecommended;
	}
	
	@Override
    public Object clone() throws CloneNotSupportedException {
		MWRItemDTO cloned = (MWRItemDTO)super.clone();
		return cloned;
	}

	public Double getRegRevenue() {
		return regRevenue;
	}

	public void setRegRevenue(Double regRevenue) {
		this.regRevenue = regRevenue;
	}

	public Double getSaleRevenue() {
		return saleRevenue;
	}

	public void setSaleRevenue(Double saleRevenue) {
		this.saleRevenue = saleRevenue;
	}

	public Double getRegUnits() {
		return regUnits;
	}

	public void setRegUnits(Double regUnits) {
		this.regUnits = regUnits;
	}

	public Double getSaleUnits() {
		return saleUnits;
	}

	public void setSaleUnits(Double saleUnits) {
		this.saleUnits = saleUnits;
	}

	public Double getRegMargin() {
		return regMargin;
	}

	public void setRegMargin(Double regMargin) {
		this.regMargin = regMargin;
	}

	public Double getSaleMargin() {
		return saleMargin;
	}

	public void setSaleMargin(Double saleMargin) {
		this.saleMargin = saleMargin;
	}

	public Double getCurrentRegRevenue() {
		return currentRegRevenue;
	}

	public void setCurrentRegRevenue(Double currentRegRevenue) {
		this.currentRegRevenue = currentRegRevenue;
	}

	public Double getCurrentRegUnits() {
		if(currentRegUnits != null) {
			return currentRegUnits == -1 ? 0 : currentRegUnits;	
		} else {
			return currentRegUnits;
		}
	}

	public void setCurrentRegUnits(Double currentRegUnits) {
		this.currentRegUnits = currentRegUnits;
	}

	public Double getCurrentRegMargin() {
		return currentRegMargin;
	}

	public void setCurrentRegMargin(Double currentRegMargin) {
		this.currentRegMargin = currentRegMargin;
	}

	public int getRecWeekStartCalendarId() {
		return recWeekStartCalendarId;
	}

	public void setRecWeekStartCalendarId(int recWeekStartCalendarId) {
		this.recWeekStartCalendarId = recWeekStartCalendarId;
	}

	public Double getPrevListCost() {
		return prevListCost;
	}

	public void setPrevListCost(Double prevListCost) {
		this.prevListCost = prevListCost;
	}

	public String getCostEffDate() {
		return costEffDate;
	}

	public void setCostEffDate(String costEffDate) {
		this.costEffDate = costEffDate;
	}

	public String getCurrPriceEffDate() {
		return currPriceEffDate;
	}

	public void setCurrPriceEffDate(String currPriceEffDate) {
		this.currPriceEffDate = currPriceEffDate;
	}

	public boolean isRecError() {
		return isRecError;
	}

	public void setRecError(boolean isRecError) {
		this.isRecError = isRecError;
	}

	public List<Integer> getRecErrorCodes() {
		return recErrorCodes;
	}

	public void setRecErrorCodes(List<Integer> recErrorCodes) {
		this.recErrorCodes = recErrorCodes;
	}

	public String getEDLPORPROMO() {
		return EDLPORPROMO;
	}

	public void setEDLPORPROMO(String eDLPORPROMO) {
		EDLPORPROMO = eDLPORPROMO;
	}

	public boolean isRecUpdated() {
		return isRecUpdated;
	}

	public void setRecUpdated(boolean isRecUpdated) {
		this.isRecUpdated = isRecUpdated;
	}

	public int getPeriodCalendarId() {
		return periodCalendarId;
	}

	public void setPeriodCalendarId(int periodCalendarId) {
		this.periodCalendarId = periodCalendarId;
	}

	public RecWeekKey getRecWeekKey() {
		return recWeekKey;
	}

	public void setRecWeekKey(RecWeekKey recWeekKey) {
		this.recWeekKey = recWeekKey;
	}

	public String getCalType() {
		return calType;
	}

	public void setCalType(String calType) {
		this.calType = calType;
	}
	
	public MultiplePrice getFinalRecPrice() {
		return this.overrideRegPrice != null ? this.overrideRegPrice : this.recommendedRegPrice;
	}

	public MultiplePrice getOverrideRegPrice() {
		return overrideRegPrice;
	}

	public void setOverrideRegPrice(MultiplePrice overrideRegPrice) {
		this.overrideRegPrice = overrideRegPrice;
	}

	public MultiplePrice getCurrentPrice() {
		return currentPrice;
	}

	public void setCurrentPrice(MultiplePrice currentPrice) {
		this.currentPrice = currentPrice;
	}

	public String getWeekStartDate() {
		return weekStartDate;
	}

	public void setWeekStartDate(String weekStartDate) {
		this.weekStartDate = weekStartDate;
	}

	public boolean isSystemOverrideFlag() {
		return isSystemOverrideFlag;
	}

	public void setSystemOverrideFlag(boolean isSystemOverrideFlag) {
		this.isSystemOverrideFlag = isSystemOverrideFlag;
	}

	public boolean isUserOverrideFlag() {
		return isUserOverrideFlag;
	}

	public void setUserOverrideFlag(boolean isOverrideOverrideFlag) {
		this.isUserOverrideFlag = isOverrideOverrideFlag;
	}

	public MultiplePrice getRecommendedRegPriceBeforeUpdateRec() {
		return recommendedRegPriceBeforeUpdateRec;
	}

	public void setRecommendedRegPriceBeforeUpdateRec(MultiplePrice recommendedRegPriceBeforeUpdateRec) {
		this.recommendedRegPriceBeforeUpdateRec = recommendedRegPriceBeforeUpdateRec;
	}

	public PRItemSaleInfoDTO getImmediatePromoInfo() {
		return immediatePromoInfo;
	}

	public void setImmediatePromoInfo(PRItemSaleInfoDTO immediatePromoInfo) {
		this.immediatePromoInfo = immediatePromoInfo;
	}

	public int getIsTPR() {
		return isTPR;
	}

	public void setIsTPR(int isTPR) {
		this.isTPR = isTPR;
	}

	public int getIsPrePriced() {
		return isPrePriced;
	}

	public void setIsPrePriced(int isPrePriced) {
		this.isPrePriced = isPrePriced;
	}

	public int getIsLockPriced() {
		return isLockPriced;
	}

	public void setIsLockPriced(int isLockPriced) {
		this.isLockPriced = isLockPriced;
	}

	public int getVendorId() {
		return vendorId;
	}

	public void setVendorId(int vendorId) {
		this.vendorId = vendorId;
	}

	public MultiplePrice getFuturePrice() {
		return futurePrice;
	}

	public void setFuturePrice(MultiplePrice futurePrice) {
		this.futurePrice = futurePrice;
	}

	public String getFuturePriceEffDate() {
		return futurePriceEffDate;
	}

	public void setFuturePriceEffDate(String futurePriceEffDate) {
		this.futurePriceEffDate = futurePriceEffDate;
	}

	public Double getFutureCost() {
		return futureCost;
	}

	public void setFutureCost(Double futureCost) {
		this.futureCost = futureCost;
	}

	public String getFutureCostEffDate() {
		return futureCostEffDate;
	}

	public void setFutureCostEffDate(String futureCostEffDate) {
		this.futureCostEffDate = futureCostEffDate;
	}

	/**
	 * @return the futureWeekPrice NOT to be confused with futurePrice
	 */
	public Double getFutureWeekPrice() {
		return futureWeekPrice;
	}

	/**
	 * @param futureWeekPrice the futureWeekPrice to set NOT to be confused with futurePrice
	 */
	public void setFutureWeekPrice(Double futureWeekPrice) {
		this.futureWeekPrice = futureWeekPrice;
	}

	/**
	 * @return the futureWeekPriceEffDate NOT to be confused with futurePriceEffDate
	 */
	public String getFutureWeekPriceEffDate() {
		return futureWeekPriceEffDate;
	}

	/**
	 * @param futureWeekPriceEffDate the futureWeekPriceEffDate to set NOT to be confused with futurePriceEffDate
	 */
	public void setFutureWeekPriceEffDate(String futureWeekPriceEffDate) {
		this.futureWeekPriceEffDate = futureWeekPriceEffDate;
	}

	public MultiplePrice getPrevCompPrice() {
		return prevCompPrice;
	}

	public void setPrevCompPrice(MultiplePrice prevCompPrice) {
		this.prevCompPrice = prevCompPrice;
	}
	public Integer getCriteriaId() {
		return criteriaId;
	}

	public void setCriteriaId(Integer criteriaId) {
		this.criteriaId = criteriaId;
	}

	public Integer getComp1StrId() {
		return comp1StrId;
	}

	public void setComp1StrId(Integer comp1StrId) {
		this.comp1StrId = comp1StrId;
	}

	public Integer getComp2StrId() {
		return comp2StrId;
	}

	public void setComp2StrId(Integer comp2StrId) {
		this.comp2StrId = comp2StrId;
	}

	public Integer getComp3StrId() {
		return comp3StrId;
	}

	public void setComp3StrId(Integer comp3StrId) {
		this.comp3StrId = comp3StrId;
	}

	public Integer getComp4StrId() {
		return comp4StrId;
	}

	public void setComp4StrId(Integer comp4StrId) {
		this.comp4StrId = comp4StrId;
	}

	public Integer getComp5StrId() {
		return comp5StrId;
	}

	public void setComp5StrId(Integer comp5StrId) {
		this.comp5StrId = comp5StrId;
	}
	
	public MultiplePrice getComp1Retail() {
		return comp1Retail;
	}

	public void setComp1Retail(MultiplePrice comp1Retail) {
		this.comp1Retail = comp1Retail;
	}

	public MultiplePrice getComp2Retail() {
		return comp2Retail;
	}

	public void setComp2Retail(MultiplePrice comp2Retail) {
		this.comp2Retail = comp2Retail;
	}

	public MultiplePrice getComp3Retail() {
		return comp3Retail;
	}

	public void setComp3Retail(MultiplePrice comp3Retail) {
		this.comp3Retail = comp3Retail;
	}

	public MultiplePrice getComp4Retail() {
		return comp4Retail;
	}

	public void setComp4Retail(MultiplePrice comp4Retail) {
		this.comp4Retail = comp4Retail;
	}

	public MultiplePrice getComp5Retail() {
		return comp5Retail;
	}

	public void setComp5Retail(MultiplePrice comp5Retail) {
		this.comp5Retail = comp5Retail;
	}

	public Double getCoreRetail() {
		return coreRetail;
	}

	public void setCoreRetail(Double coreRetail) {
		this.coreRetail = coreRetail;
	}

	public Double getCwacCoreCost() {
		return cwacCoreCost;
	}

	public void setCwacCoreCost(Double cwacCoreCost) {
		this.cwacCoreCost = cwacCoreCost;
	}

	public Double getPriceChangeImpact() {
		return priceChangeImpact;
	}

	public void setPriceChangeImpact(Double priceChangeImpact) {
		this.priceChangeImpact = priceChangeImpact;
	}

	@Override
	public String toString() {
		return "MWRItemDTO [mwRecommendationId=" + mwRecommendationId + ", runId=" + runId + ", weekCalendarId="
				+ weekCalendarId + ", periodCalendarId=" + periodCalendarId + ", productLevelId=" + productLevelId
				+ ", productId=" + productId + ", retLirId=" + retLirId + ", itemCode=" + itemCode + ", isLir=" + isLir
				+ ", strategyId=" + strategyId + ", itemSize=" + itemSize + ", uomId=" + uomId + ", priceCheckListId="
				+ priceCheckListId + ", ligRepItemCode=" + ligRepItemCode + ", regMultiple=" + regMultiple
				+ ", regPrice=" + regPrice + ", saleMultiple=" + saleMultiple + ", salePrice=" + salePrice
				+ ", promoTypeId=" + promoTypeId + ", adPageNo=" + adPageNo + ", adBlockNo=" + adBlockNo
				+ ", displayTypeId=" + displayTypeId + ", saleStartDate=" + saleStartDate + ", saleEndDate="
				+ saleEndDate + ", listCost=" + listCost + ", prevListCost=" + prevListCost + ", dealCost=" + dealCost
				+ ", vipCost=" + vipCost + ", recommendedRegPrice=" + recommendedRegPrice
				+ ", recommendedRegPriceBeforeUpdateRec=" + recommendedRegPriceBeforeUpdateRec
				+ ", finalPricePredictedMovement=" + finalPricePredictedMovement + ", finalPricePredictionStatus="
				+ finalPricePredictionStatus + ", costChangeIndicator=" + costChangeIndicator
				+ ", compPriceChangeIndicator=" + compPriceChangeIndicator + ", compStrId=" + compStrId
				+ ", compRegMultiple=" + compRegMultiple + ", compRegPrice=" + compRegPrice + ", compSaleMultiple="
				+ compSaleMultiple + ", compSalePrice=" + compSalePrice + ", compPriceCheckDate=" + compPriceCheckDate
				+ ", finalPriceRevenue=" + finalPriceRevenue + ", finalPriceMargin=" + finalPriceMargin
				+ ", priceRange=" + Arrays.toString(priceRange) + ", upc=" + upc + ", isConflict=" + isConflict
				+ ", isNewPriceRecommended=" + isNewPriceRecommended + ", regRevenue=" + regRevenue + ", saleRevenue="
				+ saleRevenue + ", regUnits=" + regUnits + ", saleUnits=" + saleUnits + ", regMargin=" + regMargin
				+ ", saleMargin=" + saleMargin + ", currentRegRevenue=" + currentRegRevenue + ", currentRegUnits="
				+ currentRegUnits + ", currentRegMargin=" + currentRegMargin + ", recWeekStartCalendarId="
				+ recWeekStartCalendarId + ", prevCompPrice=" + prevCompPrice + ", costEffDate=" + costEffDate
				+ ", currPriceEffDate=" + currPriceEffDate + ", isRecError=" + isRecError + ", EDLPORPROMO="
				+ EDLPORPROMO + ", isRecUpdated=" + isRecUpdated + ", recWeekKey=" + recWeekKey + ", calType=" + calType
				+ ", overrideRegPrice=" + overrideRegPrice + ", currentPrice=" + currentPrice + ", weekStartDate="
				+ weekStartDate + ", isSystemOverrideFlag=" + isSystemOverrideFlag + ", isUserOverrideFlag="
				+ isUserOverrideFlag + ", immediatePromoInfo=" + immediatePromoInfo + ", isTPR=" + isTPR
				+ ", isPrePriced=" + isPrePriced + ", isLockPriced=" + isLockPriced + ", vendorId=" + vendorId
				+ ", futurePrice=" + futurePrice + ", futurePriceEffDate=" + futurePriceEffDate + ", futureCost="
				+ futureCost + ", futureCostEffDate=" + futureCostEffDate + ", criteriaId=" + criteriaId
				+ ", comp1StrId=" + comp1StrId + ", comp2StrId=" + comp2StrId + ", comp3StrId=" + comp3StrId
				+ ", comp4StrId=" + comp4StrId + ", comp5StrId=" + comp5StrId + ", comp1Retail=" + comp1Retail
				+ ", comp2Retail=" + comp2Retail + ", comp3Retail=" + comp3Retail + ", comp4Retail=" + comp4Retail
				+ ", comp5Retail=" + comp5Retail + ", coreRetail=" + coreRetail + ", cwacCoreCost=" + cwacCoreCost
				+ ", priceChangeImpact=" + priceChangeImpact + " , originalListCost =" +    originalListCost + "]";
	}
	
	public double getRecentXWeeksMov() {
		return recentXWeeksMov;
	}

	public void setRecentXWeeksMov(double recentXWeeksMov) {
		this.recentXWeeksMov = recentXWeeksMov;
	}

	public boolean isNoRecentWeeksMovement() {
		return noRecentWeeksMovement;
	}

	public void setNoRecentWeeksMovement(boolean noRecentWeeksMovement) {
		this.noRecentWeeksMovement = noRecentWeeksMovement;
	}

	public boolean isSendToPrediction() {
		return sendToPrediction;
	}

	public void setSendToPrediction(boolean isStrRevenueLess) {
		this.sendToPrediction = isStrRevenueLess;
	}

	public double getAvgMovement() {
		return avgMovement;
	}

	public void setAvgMovement(double avgMovement) {
		this.avgMovement = avgMovement;
	}

	public MultiplePrice getPostdatedPrice() {
		return postdatedPrice;
	}

	public void setPostdatedPrice(MultiplePrice postdatedPrice) {
		this.postdatedPrice = postdatedPrice;
	}

	public String getPostDatedPriceEffDate() {
		return postDatedPriceEffDate;
	}

	public void setPostDatedPriceEffDate(String postDatedPriceEffDate) {
		this.postDatedPriceEffDate = postDatedPriceEffDate;
	}

	public double getOriginalListCost() {
		return originalListCost;
	}

	public void setOriginalListCost(double originalListCost) {
		this.originalListCost = originalListCost;
	}

	public List<SecondaryZoneRecDTO> getSecondaryZones() {
		return secondaryZones;
	}

	public void setSecondaryZones(List<SecondaryZoneRecDTO> secondaryZones) {
		this.secondaryZones = secondaryZones;
	}

	public boolean isSecondaryZoneRecPresent() {
		return isSecondaryZoneRecPresent;
	}

	public void setSecondaryZoneRecPresent(boolean isSecondaryZoneRecPresent) {
		this.isSecondaryZoneRecPresent = isSecondaryZoneRecPresent;
	}
	

	public double getxWeeksMov() {
		return xWeeksMov;
	}

	public void setxWeeksMov(double xWeeksMov) {
		this.xWeeksMov = xWeeksMov;
	}

	public int getOverrideRemoved() {
		return overrideRemoved;
	}

	public void setOverrideRemoved(int overrideRemoved) {
		this.overrideRemoved = overrideRemoved;
	}

	public Double getFutureCostForQtrLevel() {
		return futureCostForQtrLevel;
	}

	public void setFutureCostForQtrLevel(Double futureCostForQtrLevel) {
		this.futureCostForQtrLevel = futureCostForQtrLevel;
	}

	public String getFutureCostEffDateForQtrLevel() {
		return futureCostEffDateForQtrLevel;
	}

	public void setFutureCostEffDateForQtrLevel(String futureCostEffDateForQtrLevel) {
		this.futureCostEffDateForQtrLevel = futureCostEffDateForQtrLevel;
	}

	public double getNipoBaseCost() {
		return nipoBaseCost;
	}

	public void setNipoBaseCost(double nipoBaseCost) {
		this.nipoBaseCost = nipoBaseCost;
	}

	public int getStoreCount() {
		return storeCount;
	}

	public void setStoreCount(int storeCount) {
		this.storeCount = storeCount;
	}

	public double getFamilyUnits() {
		return familyUnits;
	}

	public void setFamilyUnits(double familyUnits) {
		this.familyUnits = familyUnits;
	}

	public int getIsFreightChargeIncluded() {
		return isFreightChargeIncluded;
	}

	public void setIsFreightChargeIncluded(int isFreightChargeIncluded) {
		this.isFreightChargeIncluded = isFreightChargeIncluded;
	}

	public double getMapRetail() {
		return mapRetail;
	}

	public void setMapRetail(double mapRetail) {
		this.mapRetail = mapRetail;
	}

	public boolean isIsnewImpactCalculated() {
		return isnewImpactCalculated;
	}

	public void setIsnewImpactCalculated(boolean isnewImpactCalculated) {
		this.isnewImpactCalculated = isnewImpactCalculated;
	}

	public double getRecommendedPricewithMap() {
		return recommendedPricewithMap;
	}

	public void setRecommendedPricewithMap(double recommendedPricewithMap) {
		this.recommendedPricewithMap = recommendedPricewithMap;
	}

	public boolean isCurrentPriceBelowMAP() {
		return currentPriceBelowMAP;
	}

	public void setCurrentPriceBelowMAP(boolean currentPriceBelowMAP) {
		this.currentPriceBelowMAP = currentPriceBelowMAP;
	}

	public double getCwagBaseCost() {
		return cwagBaseCost;
	}

	public void setCwagBaseCost(double cwagBaseCost) {
		this.cwagBaseCost = cwagBaseCost;
	}

	public MultiplePrice getPendingRetail() {
		return pendingRetail;
	}
	
	public void setPendingRetail(MultiplePrice pendingRetail) {
		this.pendingRetail = pendingRetail;
}

	public double getApprovedImpact() {
		return approvedImpact;
	}

	public void setApprovedImpact(double approvedImpact) {
		this.approvedImpact = approvedImpact;
	}

	
	
	public String CustomtoString() {
//		DecimalFormat df = new DecimalFormat("############.00");
		String pipe = "|";
		StringBuffer sb = new StringBuffer();
		sb.append("RunId:").append(runId).append(pipe);
		sb.append("ItemCode:").append(itemCode).append(pipe);
		sb.append("RetLirId:").append(retLirId).append(pipe);
		sb.append("CurRegPrice:").append(regPrice).append(pipe);
		sb.append("RecommendedRegprice:").append(recommendedRegPrice).append(pipe);
		sb.append("pendingRetail:").append(pendingRetail).append(pipe);
		sb.append("RecommendedRegpricewithMap:").append(recommendedPricewithMap).append(pipe);
		sb.append("MapRetail:").append(mapRetail).append(pipe);
		sb.append("isCurrentpriceBelowMap:").append(currentPriceBelowMAP).append(pipe);
		sb.append("priceChangeImpact:").append(priceChangeImpact).append(pipe);
		sb.append("isNewPriceRecommended:").append(isNewPriceRecommended).append(pipe);
		sb.append("overrideRegPrice:").append(overrideRegPrice).append(pipe);
		sb.append("approvedImpact:").append(approvedImpact).append(pipe);
		sb.append("userOverideflag:").append(isUserOverrideFlag).append(pipe);
		sb.append("xweeksmov:").append(xWeeksMov).append(pipe);
		
		return sb.toString();
	}

	public int getIsPendingRetailRecommended() {
		return isPendingRetailRecommended;
	}

	public void setIsPendingRetailRecommended(int isPendingRetailRecommended) {
		this.isPendingRetailRecommended = isPendingRetailRecommended;
	}

	public char getIsImpactIncludedInSummaryCalculation() {
		return isImpactIncludedInSummaryCalculation;
	}

	public void setIsImpactIncludedInSummaryCalculation(char isImpactIncludedInSummaryCalculation) {
		this.isImpactIncludedInSummaryCalculation = isImpactIncludedInSummaryCalculation;
	}

	public boolean isFuturePricePresent() {
		return isFuturePricePresent;
	}

	public void setFuturePricePresent(boolean isFuturePricePresent) {
		this.isFuturePricePresent = isFuturePricePresent;
	}

	public static long getSerialversionuid() {
		return serialVersionUID;
	}
	
	
	public String CustomtoString1() {
//		DecimalFormat df = new DecimalFormat("############.00");
		String pipe = "|";
		StringBuffer sb = new StringBuffer();
		sb.append("RunId:").append(runId).append(pipe);
		sb.append("ItemCode:").append(itemCode).append(pipe);
		sb.append("RetLirId:").append(retLirId).append(pipe);
		sb.append("futureWeekPrice:").append(futureWeekPrice).append(pipe);
		sb.append("futureWeekPriceEffDate:").append(futureWeekPriceEffDate).append(pipe);
		sb.append("futureCost:").append(futureCost).append(pipe);
		sb.append("futureCostEffDate:").append(futureCostEffDate).append(pipe);
		return sb.toString();
	}

	
}
	
