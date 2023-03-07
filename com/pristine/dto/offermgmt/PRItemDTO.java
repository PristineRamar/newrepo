package com.pristine.dto.offermgmt;

import java.io.Serializable;
import java.math.BigDecimal;
import java.time.LocalDate;
//import java.text.DecimalFormat;
import java.util.ArrayList;
import java.util.Date;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
//import java.util.Map;
import java.util.Set;

import com.pristine.dto.ProductMetricsDataDTO;
import com.pristine.dto.offermgmt.prediction.PricePointDTO;
import com.pristine.service.offermgmt.ItemKey;
import com.pristine.service.offermgmt.prediction.PredictionStatus;
import com.pristine.util.Constants;
import com.pristine.util.DateUtil;
import com.pristine.util.offermgmt.PRCommonUtil;
import com.pristine.util.offermgmt.PRConstants;
import com.pristine.util.offermgmt.PRFormatHelper;

import net.sf.ehcache.pool.sizeof.annotations.IgnoreSizeOf;


@IgnoreSizeOf
public class PRItemDTO implements Serializable, Cloneable {
	/**
	 * 
	 */
	private static final long serialVersionUID = 8084443024039535860L;
	private int deptProductId;
	private int portfolioProductId;
	private int categoryProductId;
	private int recUnitProductId;
	private int subCatProductId;
	private int segmentProductId;
	private int itemCode;
	private Double regPrice = null;
	private String curRegPriceEffDate;
	private Double listCost = null;
	private Double dealCost = null;
	private String listCostEffDate;
	private Double[] priceRange = null;
//	transient private HashMap<Double, PricePointDTO> priceMovementPrediction = new HashMap<Double, PricePointDTO>();
	// 27th Dec 2016, NU:: Code re-factor Double to MultiplePrice
	transient private HashMap<MultiplePrice, PricePointDTO> regPricePredictionMap = new HashMap<MultiplePrice, PricePointDTO>();
	private Integer regMPack = null;
	private Double regMPrice = null;
	private Double previousCost = null;
	private int costChgIndicator = 0;
	/** Comp Price Related Properties **/
	private LocationKey compStrId;
	private int compTypeId;
	transient private MultiplePrice compPrice = null;
	transient private MultiplePrice compPreviousPrice = null;
	private String compPriceCheckDate;
	private int compPriceChgIndicator = 0;
	// 18th May 2016, to support multiple comp in price index for CVS
	transient private HashMap<LocationKey, MultiplePrice> allCompPrice = new HashMap<LocationKey, MultiplePrice>();
	transient private HashMap<LocationKey, String> allCompPriceCheckDate = new HashMap<LocationKey, String>();
	transient private HashMap<LocationKey, Integer> allCompPriceChgIndicator = new HashMap<LocationKey, Integer>();
	transient private HashMap<LocationKey, MultiplePrice> allCompPreviousPrice = new HashMap<LocationKey, MultiplePrice>();
	transient private HashMap<LocationKey, MultiplePrice> allCompSalePrice = new HashMap<LocationKey, MultiplePrice>();

	/** Comp Price Related Properties **/
	private double avgMovement = 0;
	private double avgRevenue = 0;
	private long runId;
	private long recommendationId;
	private long strategyId;
//	private Double recommendedRegPrice;
	private Double overrideRegPrice = 0.0;
	private Integer overrideRegMultiple;
//	private Integer recommendedRegMultiple;
	private Double predictedMovement;
	private int isPrePriced = 0;
	private int isLocPriced = 0;
	private String isMarkedForReview = "N";
	private Integer priceCheckListId;
	private int calendarId;

	private int objectiveTypeId = -1;
	private int isNewPriceRecommended = 0;
	private int retLirId = -1;
	private Double preRegPrice = null;
	private Double preListCost = null;

	private int isConflict = 0;
	// @IgnoreSizeOf
	transient private HashMap<Integer, PRBrandDTO> relatedBrandData = null;
	private boolean processed = false;
	private int childLocationLevelId;
	private int childLocationId;
	private char distFlag = Constants.WAREHOUSE;
	private Double vipCost = null;
	private Double preVipCost = null;
	private int vipCostChgIndicator = 0;
	private int vendorId = -1;
	// @IgnoreSizeOf
	transient private PRStrategyDTO strategyDTO = null;
	// @IgnoreSizeOf
	transient private PRPriceGroupDTO pgData = null;
	private boolean isLir = false;
	private double itemSize;
	private String UOMId = "";
	private String UOMName = "";
	private String upc;
	private String retailerItemCode;
	// Changes to store opportunities
	transient private List<PricePointDTO> oppurtunities;
	private String isOppurtunity;
	private Double oppurtunityPrice;
	private Integer oppurtunityQty;
	// Changes to store predictionStatus
	private Integer predictionStatus;
	// Changes to store margin for opportunities price point
	// transient private HashMap<Double, Double> opportunitiesMarginMap = new
	// HashMap<Double, Double>();
	// To keep the predicted movement for the Current Reg Price
	private Double curRegPricePredictedMovement = 0d;
	private Integer curRegPricePredictionStatus;
	transient private PRExplainLog explainLog = new PRExplainLog();
	private String costChangeBehavior = PRConstants.COST_CHANGE_GENERIC;
	private String roundingLogic = "";
	private boolean isMarginGuidelineApplied = false;
	private boolean isStoreBrandRelationApplied = false;
	private String storeBrandRelationOperator = "";
	transient private PRRange storeBrandRelationRange = new PRRange();
	transient private PRRange marginGuidelineRange = new PRRange();
	private int brandRelationAppliedCount = 0;
	public boolean isItemLevelStrategyPresent = false;
	transient public PRStrategyDTO itemLevelStrategy = null;
	public boolean isCheckListLevelStrategyPresent = false;
	public boolean isVendorLevelStrategyPresent = false;
	public boolean isStateLevelStrategyPresent = false;
	private Boolean isZoneAndStorePriceSame = null;
	private int stateId = -1;
	private boolean isMostCommonStorePriceRecAsZonePrice = false;
	private boolean isPartOfPriceGroup = false;
	private Double sumOfDifference = 0d;
//	private Double recPriceBeforeAdjustment;
	private boolean isPriceAdjusted = false;
	private int predictionUpdateStatus = 0;
	private Integer priceCheckListTypeId;
	private int brandId;
	private int priceZoneId;
	private String priceZoneNo = "";
	private String zoneType;
	private String globalZone;
	private boolean isRecError = false;
	transient private List<Integer> recErrorCodes = new ArrayList<Integer>();
	private boolean errorButRecommend = true;
	private boolean isCurRetailSameAcrossStores = true;
	private boolean isPartOfSubstituteGroup = false;
	private boolean isSubstituteLeadItem = false;
//	private Double cost = null;
	private Double curRegPredMovWOSubsEffect;
	private Double predMovWOSubsEffect;
	transient private MultiplePrice recPriceBeforeAdjustmentForHighestMargin$ = null;
	private boolean isPriceAdjustedForHighestMargin$ = false;
	private int noOfStoresItemAuthorized = 1;
	private double curRetailSalesDollar;
	private double curRetailMarginDollar;
	private double recRetailSalesDollar;
	private double recRetailMarginDollar;
	private boolean isIncludeForSummaryCalculation;
	private long prRecommendationId;
	private double overrideRetailSalesDollar;
	private double overrideRetailMarginDollar;
	private Double overridePredictedMovement;
	private Integer overridePredictionStatus;
	// Added for promotional info
	private int promoTypeId;
//	private String promoTypeName;
	private String adName;
	private int pageNumber;
	private int blockNumber;
//	private String displayName;
	private Integer displayTypeId;
	private Integer saleMPack;
	private Double saleMPrice;
	private Double salePrice;
	private String categoryName;
	private String itemName;
	private long adjustedUnits;
	private int weeklyAdLocationLevelId;
	private int weeklyAdLocationId;
	private String storeNo = "";
	private String districtName = "";
	private long promoDefinitionId;
	private Date promoCreatedDate;
	private Date promoModifiedDate;
	private int retLirPromoKey;
	// NU: Added on 21st Oct 2015, Till before, relation in price group is supported
	// only between item vs item or lig vs lig or item vs lig
	// Below variable is added to indicate if a lig member alone is in relation to
	// another lig or item or lig member
	private boolean isItemLevelRelation = false;
	private String retLirName;
	private String createTimeStamp;
	private boolean isShipperItem = false;
	private boolean isAllLigMemIsShipperItem = false;
	private long lastXWeeksMov = 0;
	private int uniqueHHCount = 0;
	// NU: Added on 2nd Aug 2016, to save last x weeks movement
	// which will be useful in prediction analysis
	private HashMap<Integer, ProductMetricsDataDTO> lastXWeeksMovDetail = new HashMap<Integer, ProductMetricsDataDTO>();

	private PRItemSaleInfoDTO curSaleInfo = new PRItemSaleInfoDTO();
	private PRItemSaleInfoDTO recWeekSaleInfo = new PRItemSaleInfoDTO();
	private PRItemSaleInfoDTO futWeekSaleInfo = new PRItemSaleInfoDTO();

//	private PRItemAdInfoDTO curAdInfo = new PRItemAdInfoDTO();
	private PRItemAdInfoDTO recWeekAdInfo = new PRItemAdInfoDTO();
	private PRItemAdInfoDTO futWeekAdInfo = new PRItemAdInfoDTO();

//	private PRItemDisplayInfoDTO curDisplayInfo = new PRItemDisplayInfoDTO();
	private PRItemDisplayInfoDTO recWeekDisplayInfo = new PRItemDisplayInfoDTO();
	private PRItemDisplayInfoDTO futWeekDisplayInfo = new PRItemDisplayInfoDTO();

	private MultiplePrice compCurSalePrice = null;
	private int isTPR;
	private int isOnSale;
	private int isOnAd;
	private int deptIdPromotion;

	private Double recWeekDealCost = null;
	private Double recWeekSaleCost = null;

	// NU:: 28th Oct 2016, short term promo item will be recommended with future
	// effective date
	private boolean isFutureRetailRecommended = false;
	private String recPriceEffectiveDate = null;

	private MultiplePrice futureRecRetail = null;

	// NU:: 24th Nov 2016, converted to recommended reg price
	// to MultiplePrice object as part of code optimization
	private MultiplePrice recommendedRegPrice = null;
	private MultiplePrice recPriceBeforeAdjustment = null;
	private boolean isCurPriceRetained = false;
	private String regPricePredReasons = "";
	private String salePricePredReasons = "";

	private Double oppurtunityMovement;
	// Item code which represents the lig
	private int ligRepItemCode = 0;

	private String useLeadZoneStrategy;
	private MultiplePrice overriddenRegularPrice = null;
	private MultiplePrice recRegPriceBeforeReRecommedation = null;
	private boolean systemOverrideFlag = false;
	private int userOverrideFlag = 0;
	private int overrideRemoved = 0;
	private boolean isRelationOverridden = false;
	private boolean relatedItemRelationChanged = false;
	private Double minDealCost;
	private boolean isItemInLongTermPromo = false;
	private String brandName = "";
//	private HashMap<Integer, String> brands = new HashMap<Integer, String>();
	private boolean isOnGoingPromotion = false;
	private boolean isFuturePromotion = false;
	private boolean isPromoEndsWithinXWeeks = true;
	private Double allowanceCost = null;
	private boolean isLongTermAllowanceCost = false;
	private boolean isDSDItem = false;
	private boolean isNonMovingItem;
	private boolean isCurrentPriceRetained = false;
	private boolean isRecProcessCompleted = false;
	private MultiplePrice recRegPriceBeforeOverride = null;
	private String pastOverrideDate;
	private int updateRecommendationStatus = 0;
	private boolean isFutureRetailPresent = false;
	private boolean isAuthorized = false;
	private boolean isActive = false;
	private int criteriaId;
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
	private String userAttr3;
	private String userAttr4;
	private String userAttr5;
	private String userAttr6;
	private String userAttr7;
	private String userAttr8;
	private String userAttr9;
	private String userAttr10;
	private String userAttr11;
	private String userAttr12;
	private String userAttr13;
	private String userAttr14;
	private String itemSetupDate;
	private double familyXWeeksMov = 0;
	private Double coreRetail;
	private Double cwacCoreCost;
	private double priceChangeImpact;
	private double minRetail;
	private double maxRetail;
	private double lockedRetail;
	private String endDate;
	private boolean aucOverride;
	private List<String> missingTierInfo = new ArrayList<>();
	private int pricePointsFiltered;
	private double recentXWeeksMov;
	private double predExcludeWeeksUnits;
	private double predExcludeWeeksSales;
	private double predExcludeSalesPerStore;
	private boolean noRecentWeeksMovement;
	private double xWeeksMovForTotimpact;
	private double xWeeksMovForAddlCriteria;
	private String familyName;
	private BigDecimal totalRevenue;
	private boolean sendToPrediction;
	private double ECRetail;
	private String startDate;
	private String itemListComments;
	private int itemListHeaderId;
	private double xWeeksMovForWAC;
	private double originalListCost;
	private String userAttr15;
	private List<SecondaryZoneRecDTO> secondaryZones;
	private boolean isSecondaryZoneRecPresent;
	private double xweekMov;
	private double weightedRecRetail;
	private double weightedRegretail;
	private double weightedComp1retail;
	private double weightedComp2retail;
	private double weightedComp3retail;
	private double weightedComp4retail;
	private double weightedComp5retail;
	private double weightedComp6retail;
	private ItemKey itemKey;
	private int BrandTierId;
	private int leadTierID = 0;
	private boolean compOverride;
	private char valueType;
	private double movementData;
	private double xWeeksMovForAddlCriteriaAtLIGLevel;
	// ADDED FOR AUDIT
	private Set<String> storeCount;
	private Set<String> itemCount;
	private int comp6StrId;
	private HashMap<Integer, Double> zonePriceMap;
	private boolean brandGuidelineApplied = false;
	private boolean compOverCost = false;
	private int inventory;

	private String approvedOn;
	private String operatorText;

	private PRRange multiCompRange;
	// future Cost
	private Double futureListCost = null;
	private String FutureCostEffDate;

	// for export SF limit
	private int SF_week_rank;
	private int SF_export_rank;
	private int SF_RU_rank;
	private String RU_zone;
	private double total_Impact;
	private boolean familyProcessed;

	// AZ NIPO Base Cost
	private double nipoBaseCost = 0;
	private double nipoCoreCost = 0;

	// AZ zone wise regPrice
	/** PROM-2223 changes **/
	Map<Double, List<Double>> currRetailslOfAllZones = null;
	/** PROM-2223 changes end **/

	// Pending retails
	private double approvedImpact = 0.0;
	private double approvedRetail = 0.0;
	// AZ secondary itemList
	private PriceCheckListDTO secondaryPriceCheckList;

	private double listCostWtotFrChg = 0.0;
	private int freightChargeIncluded = 0;
	private boolean isFreightCostSet = false;

	// Attribute priority added for AI #109
	private String priority;
	private Set<String> storeNums;

	// AZ USA
	private double mapRetail = 0.0;
	private boolean currentPriceBelowMAP = false;
	private double xweekMovForLIGRepItem = 0.0;
	private boolean isFinalObjectiveApplied = false;
	private double recommendedPricewithMap = 0;
	private double cwagBaseCost = 0;
	private boolean isOnHold = false;
	private MultiplePrice pendingRetail = null;
	private int isPendingRetailRecommended = 0;
	private char isImpactIncludedInSummaryCalculation;

	// Added for FF
	private boolean clearanceItem;
	private double clearnaceRetail;
	private String clearanceRetailEffDate;
	private boolean isFuturePricePresent;
	private double futureUnitPrice;
	private String futurePriceEffDate;
	private boolean isPromoStartsWithinXWeeks = false;
	private boolean longTermpromotion = false;
	
	public LocalDate getAprvdDateAsLocalDate() {

		if (this.approvedOn == null) {
			return null;
		} else {
			try {
				return LocalDate.parse(approvedOn, DateUtil.getDateFormatter());
			} catch (Exception e) {
				return null;
			}
		}
	}

	public PRItemDTO(long runId, int roductLevId, int prodId, int locLevId, int locId, String attr6, int retLirId, int zoneId, String zoneNUm, 
			String retItemCOde, MultiplePrice recRegPrc, MultiplePrice recCurPrc, String effDate, String prcType, String apprby,
			String prrvName, double Vdpret, double coreRet, double Impact, String pred, String partnum, int ovrRegM, double ovrReg,
			MultiplePrice ovrPrc, double diff, String zoneName, String recUnit, int priceCheckListTypeId, String priceExportType) {
		this.runId = runId;
		this.productLevelId = roductLevId;
		this.productId = prodId;
		this.locationLevelId = locLevId;
		this.locationId = locId;
		this.userAttr6 = attr6;
		this.retLirId = retLirId;
		this.priceZoneId = zoneId;
		this.priceZoneNo = zoneNUm;
		this.retailerItemCode = retItemCOde;
		this.recommendedRegPrice = recRegPrc;
		this.currentRegPrice = recCurPrc;
		this.regEffDate = effDate;
		this.itemType = prcType;
		this.approvedBy = apprby;
		this.approverName = prrvName;
		this.VdpRetail = Vdpret;
		this.coreRetail = coreRet;
		this.impact = Impact;
		this.predicted = pred;
		this.partNumber = partnum;
		this.overrideRegMultiple = ovrRegM;
		this.overrideRegPrice = ovrReg;
		this.overriddenRegularPrice = ovrPrc;
		this.diffRetail = diff;
		this.zoneName = zoneName;
		this.recommendationUnit = recUnit;
		this.priceCheckListTypeId = priceCheckListTypeId;
		this.priceExportType = priceExportType;
	}

	public PRItemDTO() {

	}

	public PRItemDTO(Long runId, int itemcode) {
		this.runId = runId;
		this.itemCode = itemcode;
	}

    public PRItemDTO(String retItemcode, String storeNo, String zoneNum, String zoneName, String predicted, String recomUnit,
    		String partNum, String itemType, String regEffDate, double vdpPrice, double coreRetail, int childLoc, double diffret,
    		String approvedBy, String apprvName, String priceExportType, String storeLockExpFlag) {
		this.retailerItemCode = retItemcode;
		this.storeNo = storeNo;
		this.priceZoneNo = zoneNum;
		this.zoneName = zoneName;
		this.predicted = predicted;
		this.recommendationUnit = recomUnit;
		this.partNumber = partNum;
		this.itemType = itemType;
		this.regEffDate = regEffDate;
		this.VdpRetail = vdpPrice;
		this.coreRetail = coreRetail;
		this.childLocationLevelId = childLoc;
		this.diffRetail = diffret;
		this.approvedBy = approvedBy;
		this.approverName = apprvName;
		this.priceExportType = priceExportType;
		this.StoreLockExpiryFlag = storeLockExpFlag;
	}

	public double getMovementData() {
		return movementData;
	}

	public void setMovementData(double movementData) {
		this.movementData = movementData;
	}

	public double getMinRetail() {
		return minRetail;
	}

	public void setMinRetail(double minRetail) {
		this.minRetail = minRetail;
	}

	public double getMaxRetail() {
		return maxRetail;
	}

	public void setMaxRetail(double maxRetail) {
		this.maxRetail = maxRetail;
	}

	public double getLockedRetail() {
		return lockedRetail;
	}

	public void setLockedRetail(double lockedRetail) {
		this.lockedRetail = lockedRetail;
	}

	public String getEndDate() {
		return endDate;
	}

	public void setEndDate(String endDate) {
		this.endDate = endDate;
	}

	public String getUserAttr3() {
		return userAttr3;
	}

	public void setUserAttr3(String userAttr3) {
		this.userAttr3 = userAttr3;
	}

	public String getUserAttr4() {
		return userAttr4;
	}

	public void setUserAttr4(String userAttr4) {
		this.userAttr4 = userAttr4;
	}

	public String getUserAttr5() {
		return userAttr5;
	}

	public void setUserAttr5(String userAttr5) {
		this.userAttr5 = userAttr5;
	}

	public String getUserAttr6() {
		return userAttr6;
	}

	public void setUserAttr6(String userAttr6) {
		this.userAttr6 = userAttr6;
	}

	public String getUserAttr7() {
		return userAttr7;
	}

	public void setUserAttr7(String userAttr7) {
		this.userAttr7 = userAttr7;
	}

	public String getUserAttr8() {
		return userAttr8;
	}

	public void setUserAttr8(String userAttr8) {
		this.userAttr8 = userAttr8;
	}

	public String getUserAttr9() {
		return userAttr9;
	}

	public void setUserAttr9(String userAttr9) {
		this.userAttr9 = userAttr9;
	}

	public String getUserAttr10() {
		return userAttr10;
	}

	public void setUserAttr10(String userAttr10) {
		this.userAttr10 = userAttr10;
	}

	public String getUserAttr11() {
		return userAttr11;
	}

	public void setUserAttr11(String userAttr11) {
		this.userAttr11 = userAttr11;
	}

	public String getUserAttr12() {
		return userAttr12;
	}

	public void setUserAttr12(String userAttr12) {
		this.userAttr12 = userAttr12;
	}

	public String getUserAttr13() {
		return userAttr13;
	}

	public void setUserAttr13(String userAttr13) {
		this.userAttr13 = userAttr13;
	}

	public String getUserAttr14() {
		return userAttr14;
	}

	public void setUserAttr14(String userAttr14) {
		this.userAttr14 = userAttr14;
	}

	public MultiplePrice getFutureRecRetail() {
		return futureRecRetail;
	}

	public void setFutureRecRetail(MultiplePrice futureRecRetail) {
		this.futureRecRetail = futureRecRetail;
	}

	public Double getRecWeekSaleCost() {
		return recWeekSaleCost;
	}

	public void setRecWeekSaleCost(Double recWeekSaleCost) {
		this.recWeekSaleCost = recWeekSaleCost;
	}

	public int getRetLirPromoKey() {
		return retLirPromoKey;
	}

	public void setRetLirPromoKey(int retLirPromoKey) {
		this.retLirPromoKey = retLirPromoKey;
	}

	public Date getPromoCreatedDate() {
		return promoCreatedDate;
	}

	public void setPromoCreatedDate(Date promoCreatedDate) {
		this.promoCreatedDate = promoCreatedDate;
	}

	public Date getPromoModifiedDate() {
		return promoModifiedDate;
	}

	public void setPromoModifiedDate(Date promoModifiedDate) {
		this.promoModifiedDate = promoModifiedDate;
	}

	public long getPromoDefinitionId() {
		return promoDefinitionId;
	}

	public void setPromoDefinitionId(long promoDefinitionId) {
		this.promoDefinitionId = promoDefinitionId;
	}

	public int getWeeklyAdLocationLevelId() {
		return weeklyAdLocationLevelId;
	}

	public void setWeeklyAdLocationLevelId(int weeklyAdLocationLevelId) {
		this.weeklyAdLocationLevelId = weeklyAdLocationLevelId;
	}

	public int getWeeklyAdLocationId() {
		return weeklyAdLocationId;
	}

	public void setWeeklyAdLocationId(int weeklyAdLocationId) {
		this.weeklyAdLocationId = weeklyAdLocationId;
	}

	public long getAdjustedUnits() {
		return adjustedUnits;
	}

	public void setAdjustedUnits(long adjustedUnits) {
		this.adjustedUnits = adjustedUnits;
	}

	public String getItemName() {
		return itemName;
	}

	public void setItemName(String itemName) {
		this.itemName = itemName;
	}

	public String getCategoryName() {
		return categoryName;
	}

	public void setCategoryName(String categoryName) {
		this.categoryName = categoryName;
	}

	public Integer getSaleMPack() {
		return saleMPack;
	}

	public void setSaleMPack(Integer saleQty) {
		this.saleMPack = saleQty;
	}

	public Double getSaleMPrice() {
		return saleMPrice;
	}

	public void setSaleMPrice(Double saleMPrice) {
		if (saleMPrice != null)
			this.saleMPrice = Double.valueOf(PRFormatHelper.roundToTwoDecimalDigit(saleMPrice));
		else
			this.saleMPrice = saleMPrice;
	}

	public Double getSalePrice() {
		return salePrice;
	}

	public void setSalePrice(Double salePrice) {
		if (salePrice != null)
			this.salePrice = Double.valueOf(PRFormatHelper.roundToTwoDecimalDigit(salePrice));
		else
			this.salePrice = salePrice;
	}

	// Promotional info ends
	public int getPromoTypeId() {
		return promoTypeId;
	}

	public void setPromoTypeId(int promoTypeId) {
		this.promoTypeId = promoTypeId;
	}

//	public String getPromoTypeName() {
//		return promoTypeName;
//	}
//
//	public void setPromoTypeName(String promoTypeName) {
//		this.promoTypeName = promoTypeName;
//	}

	public String getAdName() {
		return adName;
	}

	public void setAdName(String adName) {
		this.adName = adName;
	}

	public int getPageNumber() {
		return pageNumber;
	}

	public void setPageNumber(int pageNumber) {
		this.pageNumber = pageNumber;
	}

	public int getBlockNumber() {
		return blockNumber;
	}

	public void setBlockNumber(int blockNumber) {
		this.blockNumber = blockNumber;
	}

//	public String getDisplayName() {
//		return displayName;
//	}
//
//	public void setDisplayName(String displayName) {
//		this.displayName = displayName;
//	}

	public Integer getDisplayTypeId() {
		return displayTypeId;
	}

	public void setDisplayTypeId(Integer displayTypeId) {
		this.displayTypeId = displayTypeId;
	}

	// Used in copying item details to representing item
	public void copyToLigRepItem(PRItemDTO tempItem) {
		this.retLirId = tempItem.retLirId;
		// this.retLirItemCode = tempItem.retLirItemCode;
		this.runId = tempItem.runId;
		this.childLocationLevelId = tempItem.childLocationLevelId;
		this.childLocationId = tempItem.childLocationId;
		// For store lig level data
		this.stateId = tempItem.stateId;
		this.distFlag = tempItem.distFlag;
		this.retLirName = tempItem.retLirName;
		this.familyName = tempItem.familyName;
	}

	// Used to copy chosen item's attributes as lig level attributes
	public void copy(PRItemDTO tempItem) {
		this.strategyId = tempItem.strategyId;
		this.strategyDTO = tempItem.strategyDTO;
		this.explainLog = tempItem.explainLog;
		this.pgData = tempItem.pgData;
		// this.actCompTypeId = tempItem.actCompTypeId;
		// this.actCompStrId = tempItem.actCompStrId;
		this.compTypeId = tempItem.compTypeId;
		this.compStrId = tempItem.compStrId;
		// change added tocopy the repItem only when Objective is applied
		if (tempItem.isFinalObjectiveApplied()) {
			this.ligRepItemCode = tempItem.getItemCode();
		}
		this.isFinalObjectiveApplied = tempItem.isFinalObjectiveApplied();
		// copy the pending retail if the rep item has it present
		this.pendingRetail = tempItem.getPendingRetail();
		this.setIsPendingRetailRecommended(tempItem.getIsPendingRetailRecommended());
		this.setPromoStartsWithinXWeeks(tempItem.isPromoStartsWithinXWeeks());
		this.setLongTermpromotion(tempItem.isLongTermpromotion());
	}

	// Used in cache, so that price, cost, movement and comp price is not
	// fetched again
	public void copyFromCache(PRItemDTO cachedItem) {
		this.setRegPrice(cachedItem.getRegPrice());
		this.setRegMPrice(cachedItem.getRegMPrice());
		this.setRegMPack(cachedItem.getRegMPack());
		this.setPreRegPrice(cachedItem.getPreRegPrice());
		this.setCurRegPriceEffDate(cachedItem.getCurRegPriceEffDate());

		this.setListCost(cachedItem.getListCost());
		this.setVipCost(cachedItem.getVipCost());
		// NU:: 14th Nov 2016, To cache deal cost
		this.setRecWeekDealCost(cachedItem.getRecWeekDealCost());
		this.setPreviousCost(cachedItem.getPreviousCost());
		this.setListCostEffDate(cachedItem.getListCostEffDate());
		this.setPreListCost(cachedItem.getPreListCost());
		this.setCostChgIndicator(cachedItem.getCostChgIndicator());
		this.setRecWeekDealCost(cachedItem.getRecWeekDealCost());

		this.setAvgMovement(cachedItem.getAvgMovement());
		this.setAvgRevenue(cachedItem.getAvgRevenue());
		this.setLastXWeeksMovDetail(cachedItem.getLastXWeeksMovDetail());


	}

	@Override
	public String toString() {
//		DecimalFormat df = new DecimalFormat("############.00");
		String pipe = "|";
		StringBuffer sb = new StringBuffer();
		sb.append("RunId:").append(runId).append(pipe);
		sb.append("ItemCode:").append(itemCode).append(pipe);
		sb.append("RetLirId:").append(retLirId).append(pipe);
		sb.append("CurRegMPack:").append(regMPack).append(",");
		sb.append("CurRegMPrice:").append(regMPrice).append(",");
		sb.append("CurRegPrice:").append(regPrice).append(pipe);
		sb.append("PreRegPrice:").append(preRegPrice).append(pipe);
		sb.append("RecommendedRegprice:").append(recommendedRegPrice).append(pipe);
		sb.append("pendingRetail:").append(pendingRetail).append(pipe);
		sb.append("RecommendedRegpricewithMap:").append(recommendedPricewithMap).append(pipe);
		sb.append("MapRetail:").append(mapRetail).append(pipe);
		sb.append("isCurrentRetailRetained:").append(isCurPriceRetained).append(pipe);
		sb.append("isCurrentpriceBelowMap:").append(currentPriceBelowMAP).append(pipe);
		sb.append("priceChangeImpact:").append(priceChangeImpact).append(pipe);
		sb.append("isNewPriceRecommended:").append(isNewPriceRecommended).append(pipe);
		sb.append("overrideRegPrice:").append(overrideRegPrice).append(pipe);
		sb.append("recRegPriceBeforeReRecommedation:").append(recRegPriceBeforeReRecommedation).append(pipe);
		sb.append("approvedImpact:").append(approvedImpact).append(pipe);
		sb.append("userOverideflag:").append(userOverrideFlag).append(pipe);
		
		return sb.toString();
	}

	// Used in dsd items
	public void copyProductInfo(PRItemDTO tempItem) {
		this.deptProductId = tempItem.deptProductId;
		this.portfolioProductId = tempItem.portfolioProductId;
		this.categoryProductId = tempItem.categoryProductId;
		this.subCatProductId = tempItem.subCatProductId;
		this.segmentProductId = tempItem.segmentProductId;
		this.itemSize = tempItem.itemSize;
		this.UOMId = tempItem.UOMId;
		this.UOMName = tempItem.UOMName;
	}

	// Used to copy item info to zone item info
	public void copyZoneItemInfo(PRItemDTO tempItem) {
		this.itemCode = tempItem.itemCode;
		this.retLirId = tempItem.retLirId;
		// this.retLirItemCode = tempItem.retLirItemCode;
		this.retailerItemCode = tempItem.retailerItemCode;
		this.upc = tempItem.upc;
		this.isLir = tempItem.isLir;
		this.itemSize = tempItem.itemSize;
		this.UOMId = tempItem.UOMId;
		this.UOMName = tempItem.UOMName;
		this.brandId = tempItem.brandId;
		this.brandName = tempItem.brandName;
		this.isPrePriced = tempItem.isPrePriced;
		this.deptProductId = tempItem.deptProductId;
		this.portfolioProductId = tempItem.portfolioProductId;
		this.categoryProductId = tempItem.categoryProductId;
		this.subCatProductId = tempItem.subCatProductId;
		this.segmentProductId = tempItem.segmentProductId;
		this.recUnitProductId = tempItem.recUnitProductId;
		this.isShipperItem = tempItem.isShipperItem;
		this.itemName = tempItem.itemName;
		this.retLirName = tempItem.retLirName;
		this.isAuthorized = tempItem.isAuthorized;
		this.isActive = tempItem.isActive;
	}

	// Used to copy item info to store item info
	public void copyStoreItemInfo(PRItemDTO tempItem) {
		this.itemCode = tempItem.itemCode;
		this.retLirId = tempItem.retLirId;
		// this.retLirItemCode = tempItem.retLirItemCode;
		this.retailerItemCode = tempItem.retailerItemCode;
		this.upc = tempItem.upc;
		this.isLir = tempItem.isLir;
		this.itemSize = tempItem.itemSize;
		this.UOMId = tempItem.UOMId;
		this.UOMName = tempItem.UOMName;
		this.brandId = tempItem.brandId;
		this.isPrePriced = tempItem.isPrePriced;
		this.deptProductId = tempItem.deptProductId;
		this.portfolioProductId = tempItem.portfolioProductId;
		this.categoryProductId = tempItem.categoryProductId;
		this.subCatProductId = tempItem.subCatProductId;
		this.segmentProductId = tempItem.segmentProductId;
		// this.runId = tempItem.runId;
		this.childLocationLevelId = tempItem.childLocationLevelId;
		this.childLocationId = tempItem.childLocationId;
		this.vendorId = tempItem.vendorId;
		this.distFlag = tempItem.distFlag;
		this.priceZoneId = tempItem.priceZoneId;
		this.priceZoneNo = tempItem.priceZoneNo;
		this.isShipperItem = tempItem.isShipperItem;
	}

	public String getCostChangeBehavior() {
		return costChangeBehavior;
	}

	public void setCostChangeBehavior(String costChangeBehavior) {
		this.costChangeBehavior = costChangeBehavior;
	}

	public int getDeptProductId() {
		return deptProductId;
	}

	public void setDeptProductId(int deptProductId) {
		this.deptProductId = deptProductId;
	}

	public int getPortfolioProductId() {
		return portfolioProductId;
	}

	public void setPortfolioProductId(int portfolioProductId) {
		this.portfolioProductId = portfolioProductId;
	}

	public int getCategoryProductId() {
		return categoryProductId;
	}

	public void setCategoryProductId(int categoryProductId) {
		this.categoryProductId = categoryProductId;
	}

	public int getSubCatProductId() {
		return subCatProductId;
	}

	public void setSubCatProductId(int subCatProductId) {
		this.subCatProductId = subCatProductId;
	}

	public int getSegmentProductId() {
		return segmentProductId;
	}

	public void setSegmentProductId(int segmentProductId) {
		this.segmentProductId = segmentProductId;
	}

	public int getItemCode() {
		return itemCode;
	}

	public void setItemCode(int itemCode) {
		this.itemCode = itemCode;
	}

	public Double getRegPrice() {
		return regPrice;
	}

	public void setRegPrice(Double regPrice) {
		this.regPrice = regPrice;
	}

	public String getCurRegPriceEffDate() {
		return curRegPriceEffDate;
	}

	public void setCurRegPriceEffDate(String curRegPriceEffDate) {
		this.curRegPriceEffDate = curRegPriceEffDate;
	}

	public Double getListCost() {
		return listCost;
	}

	public void setListCost(Double listCost) {
		this.listCost = listCost;
	}

	public String getListCostEffDate() {
		return listCostEffDate;
	}

	public void setListCostEffDate(String listCostEffDate) {
		this.listCostEffDate = listCostEffDate;
	}

	public Double[] getPriceRange() {
		return priceRange;
	}

	public void setPriceRange(Double[] priceRange) {
		this.priceRange = priceRange;
	}

	public HashMap<MultiplePrice, PricePointDTO> getRegPricePredictionMap() {
		return regPricePredictionMap;
	}

	public void setRegPricePredictionMap(HashMap<MultiplePrice, PricePointDTO> priceMovementPrediction) {
		this.regPricePredictionMap = priceMovementPrediction;
	}

	public void addRegPricePrediction(MultiplePrice multiplePrice, PricePointDTO movement) {
		this.regPricePredictionMap.put(multiplePrice, movement);
	}

	public int getObjectiveTypeId() {
		return objectiveTypeId;
	}

	public void setObjectiveTypeId(int objectiveTypeId) {
		this.objectiveTypeId = objectiveTypeId;
	}

	public Integer getRegMPack() {
		return regMPack;
	}

	public void setRegMPack(Integer regMPack) {
		this.regMPack = regMPack;
	}

	public Double getRegMPrice() {
		return regMPrice;
	}

	public void setRegMPrice(Double regMPrice) {
		if (regMPrice != null)
			this.regMPrice = Double.valueOf(PRFormatHelper.roundToTwoDecimalDigit(regMPrice));
		else
			this.regMPrice = regMPrice;
	}

	public Double getPreviousCost() {
		return previousCost;
	}

	public void setPreviousCost(Double previousCost) {
		this.previousCost = previousCost;
	}

	public int getCostChgIndicator() {
		return costChgIndicator;
	}

	public void setCostChgIndicator(int costChgIndicator) {
		this.costChgIndicator = costChgIndicator;
	}

	public double getAvgMovement() {
		return avgMovement;
	}

	public void setAvgMovement(double avgMovement) {
		this.avgMovement = avgMovement;
	}

	public double getAvgRevenue() {
		return avgRevenue;
	}

	public void setAvgRevenue(double avgRevenue) {
		this.avgRevenue = avgRevenue;
	}

	public long getRunId() {
		return runId;
	}

	public void setRunId(long runId) {
		this.runId = runId;
	}

	public long getStrategyId() {
		return strategyId;
	}

	public void setStrategyId(long strategyId) {
		this.strategyId = strategyId;
	}

	public MultiplePrice getRecommendedRegPrice() {
		return recommendedRegPrice;
	}

	public void setRecommendedRegPrice(MultiplePrice recommendedRegPrice) {
		/*if (recommendedRegPrice != null)
			this.recommendedRegPrice = Double.valueOf(PRFormatHelper.roundToTwoDecimalDigit(recommendedRegPrice));
		else
			this.recommendedRegPrice = recommendedRegPrice;*/

		if (recommendedRegPrice != null) {
			MultiplePrice multiplePrice = new MultiplePrice(recommendedRegPrice.multiple,
					Double.valueOf(PRFormatHelper.roundToTwoDecimalDigit(recommendedRegPrice.price)));
			this.recommendedRegPrice = multiplePrice;
		} else {
			this.recommendedRegPrice = recommendedRegPrice;
		}
	}

//	public Integer getRecommendedRegMultiple() {
//		return recommendedRegMultiple;
//	}
//
//	public void setRecommendedRegMultiple(Integer recommendedRegMultiple) {
//		this.recommendedRegMultiple = recommendedRegMultiple;
//	}

	public Double getPredictedMovement() {
		return predictedMovement;
	}

	public void setPredictedMovement(Double predictedMovement) {
		this.predictedMovement = predictedMovement;
	}

	public int getIsPrePriced() {
		return isPrePriced;
	}

	public void setIsPrePriced(int isPrePriced) {
		this.isPrePriced = isPrePriced;
	}

	public int getIsLocPriced() {
		return isLocPriced;
	}

	public void setIsLocPriced(int isLocPriced) {
		this.isLocPriced = isLocPriced;
	}

	public String getIsMarkedForReview() {
		return isMarkedForReview;
	}

	public void setIsMarkedForReview(String isMarkedForReview) {
		this.isMarkedForReview = isMarkedForReview;
	}

	public Integer getPriceCheckListId() {
		return priceCheckListId;
	}

	public void setPriceCheckListId(Integer priceCheckListId) {
		this.priceCheckListId = priceCheckListId;
	}

	// public StringBuffer getLog() {
	// return log;
	// }
	// public void setLog(StringBuffer log) {
	// this.log = log;
	// }

	public int getIsNewPriceRecommended() {
		if (isNewPriceRecommended())
			isNewPriceRecommended = 1;

		return isNewPriceRecommended;
	}

	public void setIsNewPriceRecommended(int isNewPriceRecommended) {
		this.isNewPriceRecommended = isNewPriceRecommended;
	}

	public int getRetLirId() {
		return retLirId;
	}

	public void setRetLirId(int retLirId) {
		this.retLirId = retLirId;
	}

	// public int getRetLirItemCode() {
	// return retLirItemCode;
	// }
	// public void setRetLirItemCode(int retLirItemCode) {
	// this.retLirItemCode = retLirItemCode;
	// }
	public Double getPreRegPrice() {
		return preRegPrice;
	}

	public void setPreRegPrice(Double preRegPrice) {
		this.preRegPrice = preRegPrice;
	}

	public String getUpc() {
		return upc;
	}

	public void setUpc(String upc) {
		this.upc = upc;
	}

	public HashMap<Integer, PRBrandDTO> getRelatedBrandData() {
		return relatedBrandData;
	}

	public void setRelatedBrandData(HashMap<Integer, PRBrandDTO> brandData) {
		this.relatedBrandData = brandData;
	}

	public int getIsConflict() {
		return isConflict;
	}

	public void setIsConflict(int isConflict) {
		this.isConflict = isConflict;
	}

	// public PRSizeDTO getSizeData() {
	// return sizeData;
	// }
	// public void setSizeData(PRSizeDTO sizeData) {
	// this.sizeData = sizeData;
	// }
	public boolean isProcessed() {
		return processed;
	}

	public void setProcessed(boolean processed) {
		this.processed = processed;
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

	public Double getPreListCost() {
		return preListCost;
	}

	public void setPreListCost(Double preListCost) {
		if (preListCost != null)
			this.preListCost = Double.valueOf(PRFormatHelper.roundToTwoDecimalDigit(preListCost));
		else
			this.preListCost = preListCost;
		// this.preListCost = preListCost;
	}

	// public PRBrandDTO getBrandData() {
	// return brandData;
	// }
	// public void setBrandData(PRBrandDTO brandData) {
	// this.brandData = brandData;
	// }
	public PRStrategyDTO getStrategyDTO() {
		return strategyDTO;
	}

	public void setStrategyDTO(PRStrategyDTO strategyDTO) {
		this.strategyDTO = strategyDTO;
	}

	public char getDistFlag() {
		return distFlag;
	}

	public void setDistFlag(char distFlag) {
		this.distFlag = distFlag;
	}

	public Double getVipCost() {
		return vipCost;
	}

	public void setVipCost(Double vipCost) {
		if (vipCost != null)
			this.vipCost = Double.valueOf(PRFormatHelper.roundToTwoDecimalDigit(vipCost));
		else
			this.vipCost = vipCost;
		// this.vipCost = vipCost;
	}

	public int getVendorId() {
		return vendorId;
	}

	public void setVendorId(int vendorId) {
		this.vendorId = vendorId;
	}

	public PRPriceGroupDTO getPgData() {
		return pgData;
	}

	public void setPgData(PRPriceGroupDTO pgData) {
		this.pgData = pgData;
	}

	public boolean isLir() {
		return isLir;
	}

	public void setLir(boolean isLir) {
		this.isLir = isLir;
	}

	public double getItemSize() {
		return itemSize;
	}

	public void setItemSize(double itemSize) {
		this.itemSize = itemSize;
	}

	public String getRetailerItemCode() {
		return retailerItemCode;
	}

	public void setRetailerItemCode(String retailerItemCode) {
		this.retailerItemCode = retailerItemCode;
	}

	// Changes to store opportunities
	public List<PricePointDTO> getOppurtunities() {
		return oppurtunities;
	}

	public void setOppurtunities(ArrayList<PricePointDTO> oppurtunities) {
		this.oppurtunities = oppurtunities;
	}

	public String getIsOppurtunity() {
		return isOppurtunity;
	}

	public void setIsOppurtunity(String isOppurtunity) {
		this.isOppurtunity = isOppurtunity;
	}

	public Double getOppurtunityPrice() {
		return oppurtunityPrice;
	}

	public void setOppurtunityPrice(Double oppurtunityPrice) {
		this.oppurtunityPrice = oppurtunityPrice;
	}

	public Integer getOppurtunityQty() {
		return oppurtunityQty;
	}

	public void setOppurtunityQty(Integer oppurtunityQty) {
		this.oppurtunityQty = oppurtunityQty;
	}

	public void addOpportunities(int quantity, double price) {
		if (oppurtunities == null) {
			oppurtunities = new ArrayList<PricePointDTO>();
		}
		PricePointDTO pricePoint = new PricePointDTO();
		pricePoint.setRegPrice(price);
		pricePoint.setRegQuantity(quantity);
		oppurtunities.add(pricePoint);
	}

	// Changes to store opportunities - Ends

	// Changes to store prediction status
	public Integer getPredictionStatus() {
		return predictionStatus;
	}

	public void setPredictionStatus(Integer predictionStatus) {
		this.predictionStatus = predictionStatus;
	}

	// Changes to store margin for opportunities price point
	/*
	 * public HashMap<Double, Double> getOpportunitiesMarginMap() { return
	 * opportunitiesMarginMap; }
	 * 
	 * public void setOpportunitiesMarginMap(HashMap<Double, Double>
	 * opportunitiesMarginMap) { this.opportunitiesMarginMap =
	 * opportunitiesMarginMap; }
	 */

	/*
	 * public void addOpportunitiesMargin(Double regPrice, Double margin) {
	 * this.opportunitiesMarginMap.put(regPrice, margin); }
	 */

	// Changes to store margin for opportunities price point - Ends

	public Double getCurRegPricePredictedMovement() {
		return curRegPricePredictedMovement;
	}

	public void setCurRegPricePredictedMovement(Double curRegPricePredictedMovement) {
		this.curRegPricePredictedMovement = curRegPricePredictedMovement;
	}

	public boolean getIsMarginGuidelineApplied() {
		return isMarginGuidelineApplied;
	}

	public void setIsMarginGuidelineApplied(boolean isMarginGuidelineApplied) {
		this.isMarginGuidelineApplied = isMarginGuidelineApplied;
	}

	public boolean getIsStoreBrandRelationApplied() {
		return isStoreBrandRelationApplied;
	}

	public void setIsStoreBrandRelationApplied(boolean isStoreBrandRelationApplied) {
		this.isStoreBrandRelationApplied = isStoreBrandRelationApplied;
	}

	public String getStoreBrandRelationOperator() {
		return storeBrandRelationOperator;
	}

	public void setStoreBrandRelationOperator(String storeBrandRelationOperator) {
		this.storeBrandRelationOperator = storeBrandRelationOperator;
	}

	public PRRange getStoreBrandRelationRange() {
		return storeBrandRelationRange;
	}

	public void setStoreBrandRelationRange(PRRange storeBrandRelationRange) {
		this.storeBrandRelationRange = storeBrandRelationRange;
	}

	public PRRange getMarginGuidelineRange() {
		return marginGuidelineRange;
	}

	public void setMarginGuidelineRange(PRRange marginGuidelineRange) {
		this.marginGuidelineRange = marginGuidelineRange;
	}

	public PRExplainLog getExplainLog() {
		return explainLog;
	}

	public void setExplainLog(PRExplainLog explainLog) {
		this.explainLog = explainLog;
	}

	public int getBrandRelationAppliedCount() {
		return brandRelationAppliedCount;
	}

	public void setBrandRelationAppliedCount(int brandRelationAppliedCount) {
		this.brandRelationAppliedCount = brandRelationAppliedCount;
	}

	public String getRoundingLogic() {
		return roundingLogic;
	}

	public void setRoundingLogic(String roundingLogic) {
		this.roundingLogic = roundingLogic;
	}

	public Double getPreVipCost() {
		return preVipCost;
	}

	public void setPreVipCost(Double preVipCost) {
		if (preVipCost != null)
			this.preVipCost = Double.valueOf(PRFormatHelper.roundToTwoDecimalDigit(preVipCost));
		else
			this.preVipCost = preVipCost;
		// this.preVipCost = preVipCost;
	}

	public int getVipCostChgIndicator() {
		return vipCostChgIndicator;
	}

	public void setVipCostChgIndicator(int vipCostChgIndicator) {
		this.vipCostChgIndicator = vipCostChgIndicator;
	}

	public String getUOMName() {
		return UOMName;
	}

	public void setUOMName(String uOMName) {
		UOMName = uOMName;
	}

	public String getUOMId() {
		return UOMId;
	}

	public void setUOMId(String uOMId) {
		UOMId = uOMId;
	}

	public Boolean getIsZoneAndStorePriceSame() {
		return isZoneAndStorePriceSame;
	}

	public void setIsZoneAndStorePriceSame(Boolean isZoneAndStorePriceSame) {
		this.isZoneAndStorePriceSame = isZoneAndStorePriceSame;
	}

	public int getStateId() {
		return stateId;
	}

	public void setStateId(int stateId) {
		this.stateId = stateId;
	}

	public boolean getIsMostCommonStorePriceRecAsZonePrice() {
		return isMostCommonStorePriceRecAsZonePrice;
	}

	public void setIsMostCommonStorePriceRecAsZonePrice(boolean isMostCommonStorePriceRecAsZonePrice) {
		this.isMostCommonStorePriceRecAsZonePrice = isMostCommonStorePriceRecAsZonePrice;
	}

	public long getRecommendationId() {
		return recommendationId;
	}

	public void setRecommendationId(long recommendationId) {
		this.recommendationId = recommendationId;
	}

	public Double getOverrideRegPrice() {
		return overrideRegPrice;
	}

	public void setOverrideRegPrice(Double overrideRegPrice) {
		this.overrideRegPrice = overrideRegPrice;
	}

	public boolean getIsPartOfPriceGroup() {
		return isPartOfPriceGroup;
	}

	public void setIsPartOfPriceGroup(boolean isPartOfPriceGroup) {
		this.isPartOfPriceGroup = isPartOfPriceGroup;
	}

	public Double getSumOfDifference() {
		return sumOfDifference;
	}

	public void setSumOfDifference(Double sumOfDifference) {
		this.sumOfDifference = sumOfDifference;
	}

	/*public Double getRecPriceBeforeAdjustment() {
		return recPriceBeforeAdjustment;
	}

	public void setRecPriceBeforeAdjustment(Double recPriceBeforeAdjustment) {
		this.recPriceBeforeAdjustment = recPriceBeforeAdjustment;
	}*/
	
	public MultiplePrice getRecPriceBeforeAdjustment() {
		return recPriceBeforeAdjustment;
	}

	public void setRecPriceBeforeAdjustment(MultiplePrice recPriceBeforeAdjustment) {
		this.recPriceBeforeAdjustment = recPriceBeforeAdjustment;
	}

	public boolean getIsPriceAdjusted() {
		return isPriceAdjusted;
	}

	public void setIsPriceAdjusted(boolean isPriceAdjusted) {
		this.isPriceAdjusted = isPriceAdjusted;
	}

	public int getPredictionUpdateStatus() {
		return predictionUpdateStatus;
	}

	public void setPredictionUpdateStatus(int predictionUpdateStatus) {
		this.predictionUpdateStatus = predictionUpdateStatus;
	}

	// public int getStartCalendarId() {
	// return startCalendarId;
	// }
	//
	// public void setStartCalendarId(int startCalendarId) {
	// this.startCalendarId = startCalendarId;
	// }
	//
	// public int getEndCalendarId() {
	// return endCalendarId;
	// }
	//
	// public void setEndCalendarId(int endCalendarId) {
	// this.endCalendarId = endCalendarId;
	// }

	public Integer getPriceCheckListTypeId() {
		return priceCheckListTypeId;
	}

	public void setPriceCheckListTypeId(Integer priceCheckListTypeId) {
		this.priceCheckListTypeId = priceCheckListTypeId;
	}

	public int getBrandId() {
		return brandId;
	}

	public void setBrandId(int brandId) {
		this.brandId = brandId;
	}

	public int getPriceZoneId() {
		return priceZoneId;
	}

	public void setPriceZoneId(int priceZoneId) {
		this.priceZoneId = priceZoneId;
	}

	public String getPriceZoneNo() {
		return priceZoneNo;
	}

	public void setPriceZoneNo(String priceZoneNo) {
		this.priceZoneNo = priceZoneNo;
	}

	public boolean getIsRecError() {
		return isRecError;
	}

	public void setIsRecError(boolean isRecError) {
		this.isRecError = isRecError;
	}

	public List<Integer> getRecErrorCodes() {
		return recErrorCodes;
	}

	public void setRecErrorCodes(List<Integer> recErrorCodes) {
		this.recErrorCodes = recErrorCodes;
	}

	public boolean getErrorButRecommend() {
		return errorButRecommend;
	}

	public void setErrorButRecommend(boolean errorButRecommend) {
		this.errorButRecommend = errorButRecommend;
	}

	/** Comp Price Related Properties **/
	public LocationKey getCompStrId() {
		return compStrId;
	}

	public void setCompStrId(LocationKey compStrId) {
		this.compStrId = compStrId;
	}

	public int getCompTypeId() {
		return compTypeId;
	}

	public void setCompTypeId(int compTypeId) {
		this.compTypeId = compTypeId;
	}

	public MultiplePrice getCompPrice() {
		return compPrice;
	}

	public void setCompPrice(MultiplePrice compPrice) {
		this.compPrice = compPrice;
	}

	public MultiplePrice getCompPreviousPrice() {
		return compPreviousPrice;
	}

	public void setCompPreviousPrice(MultiplePrice compPreviousPrice) {
		this.compPreviousPrice = compPreviousPrice;
	}

	public String getCompPriceCheckDate() {
		return compPriceCheckDate;
	}

	public void setCompPriceCheckDate(String compPriceCheckDate) {
		this.compPriceCheckDate = compPriceCheckDate;
	}

	public int getCompPriceChgIndicator() {
		return compPriceChgIndicator;
	}

	public void setCompPriceChgIndicator(int compPriceChgIndicator) {
		this.compPriceChgIndicator = compPriceChgIndicator;
	}

	public HashMap<LocationKey, MultiplePrice> getAllCompPrice() {
		return allCompPrice;
	}

	public void setAllCompPrice(HashMap<LocationKey, MultiplePrice> allCompPrice) {
		this.allCompPrice = allCompPrice;
	}

	public HashMap<LocationKey, String> getAllCompPriceCheckDate() {
		return allCompPriceCheckDate;
	}

	public void setAllCompPriceCheckDate(HashMap<LocationKey, String> allCompPriceCheckDate) {
		this.allCompPriceCheckDate = allCompPriceCheckDate;
	}

	public HashMap<LocationKey, Integer> getAllCompPriceChgIndicator() {
		return allCompPriceChgIndicator;
	}

	public void setAllCompPriceChgIndicator(HashMap<LocationKey, Integer> allCompPriceChgIndicator) {
		this.allCompPriceChgIndicator = allCompPriceChgIndicator;
	}

	public HashMap<LocationKey, MultiplePrice> getAllCompPreviousPrice() {
		return allCompPreviousPrice;
	}

	public void setAllCompPreviousPrice(HashMap<LocationKey, MultiplePrice> allCompPreviousPrice) {
		this.allCompPreviousPrice = allCompPreviousPrice;
	}

	public void addAllCompPrice(LocationKey locationKey, MultiplePrice compPrice) {
		this.allCompPrice.put(locationKey, compPrice);
	}

	public void addAllCompPriceCheckDate(LocationKey locationKey, String date) {
		this.allCompPriceCheckDate.put(locationKey, date);
	}

	public void addAllCompPriceChgIndicator(LocationKey locationKey, int chgIndicator) {
		this.allCompPriceChgIndicator.put(locationKey, chgIndicator);
	}

	public void addAllCompPreviousPrice(LocationKey locationKey, MultiplePrice compPrice) {
		this.allCompPreviousPrice.put(locationKey, compPrice);
	}
	/** Comp Price Related Properties **/

	public boolean getIsCurRetailSameAcrossStores() {
		return isCurRetailSameAcrossStores;
	}

	public void setIsCurRetailSameAcrossStores(boolean isCurRetailSameAcrossStores) {
		this.isCurRetailSameAcrossStores = isCurRetailSameAcrossStores;
	}

	public boolean getIsPartOfSubstituteGroup() {
		return isPartOfSubstituteGroup;
	}

	public void setIsPartOfSubstituteGroup(boolean isPartOfSubstituteGroup) {
		this.isPartOfSubstituteGroup = isPartOfSubstituteGroup;
	}

	public boolean getIsSubstituteLeadItem() {
		return isSubstituteLeadItem;
	}

	public void setIsSubstituteLeadItem(boolean isSubstituteLeadItem) {
		this.isSubstituteLeadItem = isSubstituteLeadItem;
	}

	public Double getCost() {
		if (this.getVipCost() != null && this.getVipCost() > 0)
			return this.getVipCost();
		else
			return this.getListCost();
	}

	private boolean isNewPriceRecommended() {
		boolean isNewPriceRecommended = false;
//		DecimalFormat df = new DecimalFormat("######.##");
		MultiplePrice curRegPrice = PRCommonUtil.getCurRegPrice(this);

		if (this.getRecommendedRegPrice() != null && !this.getRecommendedRegPrice().equals(curRegPrice)) {
			isNewPriceRecommended = true;
		}

		/*if (this.getRecommendedRegPrice() != null) {
			if (this.getRegPrice() != null) {
				
				Double tRegPrice = new Double(df.format(this.getRegPrice()));
				Double tRecPrice = new Double(df.format(this.getRecommendedRegPrice()));
				Integer tRegMultiple = 1;
//				Integer tRecMultiple = this.getRecommendedRegMultiple() == null ? 1 : this.getRecommendedRegMultiple();
				Integer tRecMultiple = this.getRecommendedRegPrice().multiple == null ? 1 : this.getRecommendedRegPrice().multiple;

				// if recommended price is in multiples, then check against
				// regmprice (pre-price/loc-price)
				if (this.getRegMPack() != null && this.getRegMPack() > 1 && this.getRegMPrice() != null
						&& this.getRegMPrice() > 0) {
					tRegMultiple = this.getRegMPack();
					tRegPrice = this.getRegMPrice();
				}

				// if (tRegPrice.doubleValue() != tRecPrice.doubleValue() &&
				// tRegMultiple != tRecMultiple) {
				if (!(tRegPrice.doubleValue() == tRecPrice.doubleValue() && tRegMultiple == tRecMultiple)) {
					isNewPriceRecommended = true;
				}
			} else {
				//if (this.getRecommendedRegPrice().doubleValue() > 0)
				if (this.getRecommendedRegPrice().price != null &&  this.getRecommendedRegPrice().price > 0)
					isNewPriceRecommended = true;
			}
		}*/
		return isNewPriceRecommended;
	}

	public Double getCurRegPredMovWOSubsEffect() {
		return curRegPredMovWOSubsEffect;
	}

	public void setCurRegPredMovWOSubsEffect(Double curRegPredMovWOSubsEffect) {
		this.curRegPredMovWOSubsEffect = curRegPredMovWOSubsEffect;
	}

	public Double getPredMovWOSubsEffect() {
		return predMovWOSubsEffect;
	}

	public void setPredMovWOSubsEffect(Double predMovWOSubsEffect) {
		this.predMovWOSubsEffect = predMovWOSubsEffect;
	}

	public MultiplePrice getRecPriceBeforeAdjustmentForHighestMargin$() {
		return recPriceBeforeAdjustmentForHighestMargin$;
	}

	public void setRecPriceBeforeAdjustmentForHighestMargin$(MultiplePrice recPriceBeforeAdjustmentForHighestMargin$) {
		this.recPriceBeforeAdjustmentForHighestMargin$ = recPriceBeforeAdjustmentForHighestMargin$;
	}

	public boolean getIsPriceAdjustedForHighestMargin$() {
		return isPriceAdjustedForHighestMargin$;
	}

	public void setIsPriceAdjustedForHighestMargin$(boolean isPriceAdjustedForHighestMargin$) {
		this.isPriceAdjustedForHighestMargin$ = isPriceAdjustedForHighestMargin$;
	}

	public int getNoOfStoresItemAuthorized() {
		return noOfStoresItemAuthorized;
	}

	public void setNoOfStoresItemAuthorized(int noOfStoresItemAuthorized) {
		this.noOfStoresItemAuthorized = noOfStoresItemAuthorized;
	}

	public double getCurRetailSalesDollar() {
		// Lig level sales will be calculated in lig constraint
		if (!this.isLir()) {
			MultiplePrice multiplePrice = PRCommonUtil.getMultiplePrice(this.getRegMPack(), this.getRegPrice(),
					this.getRegMPrice());
			Double salesDollar = PRCommonUtil.getSalesDollar(multiplePrice, this.getCurRegPricePredictedMovement());
			curRetailSalesDollar = (salesDollar == null ? 0 : salesDollar);
		}
		// 12th Apr 2016, don't round, as in the front end it is multiplied with 13
		// and rounding here will show bigger number in the front end
		// return Math.round(curRetailSalesDollar);
		return PRFormatHelper.roundToTwoDecimalDigitAsDouble(curRetailSalesDollar);
	}

	public void setCurRetailSalesDollar(double curRetailSalesDollar) {
		this.curRetailSalesDollar = curRetailSalesDollar;
	}

	public double getCurRetailMarginDollar() {
		// Lig level margin will be calculated in lig constraint
		if (!this.isLir()) {
			MultiplePrice multiplePrice = PRCommonUtil.getMultiplePrice(this.getRegMPack(), this.getRegPrice(),
					this.getRegMPrice());
			Double marginDollar = PRCommonUtil.getMarginDollar(multiplePrice, this.getCost(),
					this.getCurRegPricePredictedMovement());
			curRetailMarginDollar = (marginDollar == null ? 0 : marginDollar);
		}
		// 12th Apr 2016, don't round, as in the front end it is multiplied with
		// 13 and rounding here will show bigger number in the front end
		// return Math.round(curRetailMarginDollar);
		return PRFormatHelper.roundToTwoDecimalDigitAsDouble(curRetailMarginDollar);
	}

	public void setCurRetailMarginDollar(double curRetailMarginDollar) {
		this.curRetailMarginDollar = curRetailMarginDollar;
	}

	public double getRecRetailSalesDollar() {
		// Lig level sales will be calculated in lig constraint
		if (!this.isLir()) {
//			MultiplePrice multiplePrice = new MultiplePrice(this.getRecommendedRegMultiple(), this.getRecommendedRegPrice());
//			Double salesDollar = PRCommonUtil.getSalesDollar(multiplePrice, this.getPredictedMovement());
			Double salesDollar = PRCommonUtil.getSalesDollar(this.getRecommendedRegPrice(), this.getPredictedMovement());
			recRetailSalesDollar = (salesDollar == null ? 0 : salesDollar);
		}
		// 12th Apr 2016, don't round, as in the front end it is multiplied with
		// 13 and rounding here will show bigger number in the front end
		// return Math.round(recRetailSalesDollar);
		return PRFormatHelper.roundToTwoDecimalDigitAsDouble(recRetailSalesDollar);
	}

	public void setRecRetailSalesDollar(double recRetailSalesDollar) {
		this.recRetailSalesDollar = recRetailSalesDollar;
	}

	public double getRecRetailMarginDollar() {
		// Lig level margin will be calculated in lig constraint
		if (!this.isLir()) {
//			MultiplePrice multiplePrice = new MultiplePrice(this.getRecommendedRegMultiple(), this.getRecommendedRegPrice());
//			Double marginDollar = PRCommonUtil.getMarginDollar(multiplePrice, this.getCost(), this.getPredictedMovement());
			Double marginDollar = PRCommonUtil.getMarginDollar(this.getRecommendedRegPrice(), this.getCost(), this.getPredictedMovement());
			recRetailMarginDollar = (marginDollar == null ? 0 : marginDollar);
		}
		// 12th Apr 2016, don't round, as in the front end it is multiplied with
		// 13 and rounding here will show bigger number in the front end
		// return Math.round(recRetailMarginDollar);
		return PRFormatHelper.roundToTwoDecimalDigitAsDouble(recRetailMarginDollar);
	}

	public void setRecRetailMarginDollar(double recRetailMarginDollar) {
		this.recRetailMarginDollar = recRetailMarginDollar;
	}

	public boolean getIsIncludeForSummaryCalculation() {
		if (!this.isLir()) {
			MultiplePrice curRegMultiplePrice = null;
			curRegMultiplePrice = PRCommonUtil.getMultiplePrice(this.getRegMPack(), this.getRegPrice(),
					this.getRegMPrice());
			if(PRCommonUtil.canConsiderItemForCalculation(curRegMultiplePrice, this.getCost(), this.getPredictionStatus()))
				isIncludeForSummaryCalculation = true;
			else
				isIncludeForSummaryCalculation = false;
		} else {
			isIncludeForSummaryCalculation = false;
		}
		return isIncludeForSummaryCalculation;
	}

	public long getPrRecommendationId() {
		return prRecommendationId;
	}

	public void setPrRecommendationId(long prRecommendationId) {
		this.prRecommendationId = prRecommendationId;
	}

	public Double getOverridePredictedMovement() {
		return overridePredictedMovement;
	}

	public void setOverridePredictedMovement(Double overridePredictedMovement) {
		this.overridePredictedMovement = overridePredictedMovement;
	}

	public Integer getOverridePredictionStatus() {
		return overridePredictionStatus;
	}

	public void setOverridePredictionStatus(Integer overridePredictionStatus) {
		this.overridePredictionStatus = overridePredictionStatus;
	}

	public double getOverrideRetailSalesDollar() {
		// Lig level sales will be calculated in lig constraint
		if (!this.isLir()) {
			MultiplePrice multiplePrice = new MultiplePrice(this.getOverrideRegMultiple(), this.getOverrideRegPrice());
			Double salesDollar = PRCommonUtil.getSalesDollar(multiplePrice, this.getOverridePredictedMovement());
			overrideRetailSalesDollar = (salesDollar == null ? 0 : salesDollar);
		}
		// 12th Apr 2016, don't round, as in the front end it is multiplied with
		// 13 and rounding here will show bigger number in the front end
//		return Math.round(overrideRetailSalesDollar);
		return PRFormatHelper.roundToTwoDecimalDigitAsDouble(overrideRetailSalesDollar);
	}

	public void setOverrideRetailSalesDollar(double overrideRetailSalesDollar) {
		this.overrideRetailSalesDollar = overrideRetailSalesDollar;
	}

	public double getOverrideRetailMarginDollar() {
		// Lig level margin will be calculated separately
		if (!this.isLir()) {
			MultiplePrice multiplePrice = new MultiplePrice(this.getOverrideRegMultiple(), this.getOverrideRegPrice());
			Double marginDollar = PRCommonUtil.getMarginDollar(multiplePrice, this.getCost(),
					this.getOverridePredictedMovement());
			overrideRetailMarginDollar = (marginDollar == null ? 0 : marginDollar);
		}
		// 12th Apr 2016, don't round, as in the front end it is multiplied with
		// 13 and rounding here will show bigger number in the front end
//		return Math.round(overrideRetailMarginDollar);
		return PRFormatHelper.roundToTwoDecimalDigitAsDouble(overrideRetailMarginDollar);
	}

	public void setOverrideRetailMarginDollar(double overrideRetailMarginDollar) {
		this.overrideRetailMarginDollar = overrideRetailMarginDollar;
	}

	public Integer getOverrideRegMultiple() {
		return overrideRegMultiple;
	}

	public void setOverrideRegMultiple(Integer overrideRegMultiple) {
		this.overrideRegMultiple = overrideRegMultiple;
	}

	public String getStoreNo() {
		return storeNo;
	}

	public void setStoreNo(String storeNo) {
		this.storeNo = storeNo;
	}

	public String getDistrictName() {
		return districtName;
	}

	public void setDistrictName(String districtName) {
		this.districtName = districtName;
	}

	public boolean isItemLevelRelation() {
		return isItemLevelRelation;
	}

	public void setItemLevelRelation(boolean isItemLevelRelation) {
		this.isItemLevelRelation = isItemLevelRelation;
	}

	public String getRetLirName() {
		return retLirName;
	}

	public void setRetLirName(String retLirName) {
		this.retLirName = retLirName;
	}

	public String getCreateTimeStamp() {
		return createTimeStamp;
	}

	public void setCreateTimeStamp(String createTimeStamp) {
		this.createTimeStamp = createTimeStamp;
	}

	public boolean isShipperItem() {
		return isShipperItem;
	}

	public void setShipperItem(boolean isShipperItem) {
		this.isShipperItem = isShipperItem;
	}

	public boolean isAllLigMemIsShipperItem() {
		return isAllLigMemIsShipperItem;
	}

	public void setAllLigMemIsShipperItem(boolean isAllLigMemIsShipperItem) {
		this.isAllLigMemIsShipperItem = isAllLigMemIsShipperItem;
	}

	public long getLastXWeeksMov() {
		return lastXWeeksMov;
	}

	public void setLastXWeeksMov(long lastXWeeksMov) {
		this.lastXWeeksMov = lastXWeeksMov;
	}

	public HashMap<Integer, ProductMetricsDataDTO> getLastXWeeksMovDetail() {
		return lastXWeeksMovDetail;
	}

	public void setLastXWeeksMovDetail(HashMap<Integer, ProductMetricsDataDTO> lastXWeeksMovDetail) {
		this.lastXWeeksMovDetail = lastXWeeksMovDetail;
	}

//	public MultiplePrice getCurSaleMultiplePrice() {
//		return curSaleMultiplePrice;
//	}
//
//	public void setCurSaleMultiplePrice(MultiplePrice curSaleMultiplePrice) {
//		this.curSaleMultiplePrice = curSaleMultiplePrice;
//	}
//
//	public String getCurSaleEffectiveStartDate() {
//		return curSaleEffectiveStartDate;
//	}
//
//	public void setCurSaleEffectiveStartDate(String curSaleEffectiveStartDate) {
//		this.curSaleEffectiveStartDate = curSaleEffectiveStartDate;
//	}
//
//	public String getCurSaleEffectiveEndDate() {
//		return curSaleEffectiveEndDate;
//	}
//
//	public void setCurSaleEffectiveEndDate(String curSaleEffectiveEndDate) {
//		this.curSaleEffectiveEndDate = curSaleEffectiveEndDate;
//	}
//
//	public String getWeeklyAdStartDate() {
//		return weeklyAdStartDate;
//	}
//
//	public void setWeeklyAdStartDate(String weeklyAdStartDate) {
//		this.weeklyAdStartDate = weeklyAdStartDate;
//	}
//
//	public String getPromoWeekStartDate() {
//		return promoWeekStartDate;
//	}
//
//	public void setPromoWeekStartDate(String promoWeekStartDate) {
//		this.promoWeekStartDate = promoWeekStartDate;
//	}
//
//	public String getDisplayWeekStartDate() {
//		return displayWeekStartDate;
//	}
//
//	public void setDisplayWeekStartDate(String displayWeekStartDate) {
//		this.displayWeekStartDate = displayWeekStartDate;
//	}
		
		
	public String customToString1() {
		String delimitor = ",";
		StringBuffer sb = new StringBuffer();
		sb.append("RetLirId:").append(this.retLirId).append(delimitor);
		sb.append("CurRegMPack:").append(this.regMPack).append(",");
		sb.append("CurRegMPrice:").append(this.regMPrice).append(",");
		sb.append("CurRegPrice:").append(this.regPrice).append(delimitor);
		sb.append("RecPricePredictedMov:").append(this.predictedMovement).append(delimitor);
		sb.append("CurPricePredictedMov:").append(this.curRegPricePredictedMovement).append(delimitor);
		return sb.toString();
	}

	public PRItemSaleInfoDTO getCurSaleInfo() {
		return curSaleInfo;
	}

	public void setCurSaleInfo(PRItemSaleInfoDTO curSaleInfo) {
		this.curSaleInfo = curSaleInfo;
	}

	public PRItemSaleInfoDTO getRecWeekSaleInfo() {
		return recWeekSaleInfo;
	}

	public void setRecWeekSaleInfo(PRItemSaleInfoDTO recWeekSaleInfo) {
		this.recWeekSaleInfo = recWeekSaleInfo;
	}

	public PRItemSaleInfoDTO getFutWeekSaleInfo() {
		return futWeekSaleInfo;
	}

	public void setFutWeekSaleInfo(PRItemSaleInfoDTO futWeekSaleInfo) {
		this.futWeekSaleInfo = futWeekSaleInfo;
	}

	public PRItemAdInfoDTO getRecWeekAdInfo() {
		return recWeekAdInfo;
	}

	public void setRecWeekAdInfo(PRItemAdInfoDTO recWeekAdInfo) {
		this.recWeekAdInfo = recWeekAdInfo;
	}

	public PRItemAdInfoDTO getFutWeekAdInfo() {
		return futWeekAdInfo;
	}

	public void setFutWeekAdInfo(PRItemAdInfoDTO futWeekAdInfo) {
		this.futWeekAdInfo = futWeekAdInfo;
	}

//	public PRItemDisplayInfoDTO getCurDisplayInfo() {
//		return curDisplayInfo;
//	}
//
//	public void setCurDisplayInfo(PRItemDisplayInfoDTO curDisplayInfo) {
//		this.curDisplayInfo = curDisplayInfo;
//	}

	public PRItemDisplayInfoDTO getRecWeekDisplayInfo() {
		return recWeekDisplayInfo;
	}

	public void setRecWeekDisplayInfo(PRItemDisplayInfoDTO recWeekDisplayInfo) {
		this.recWeekDisplayInfo = recWeekDisplayInfo;
	}

	public PRItemDisplayInfoDTO getFutWeekDisplayInfo() {
		return futWeekDisplayInfo;
	}

	public void setFutWeekDisplayInfo(PRItemDisplayInfoDTO futWeekDisplayInfo) {
		this.futWeekDisplayInfo = futWeekDisplayInfo;
	}

	public boolean isItemPromotedForRecWeek() {
		boolean isPromoted = false;
		if (this.recWeekSaleInfo.getSalePrice() != null || this.recWeekAdInfo.getAdPageNo() > 0
				|| this.recWeekDisplayInfo.getDisplayWeekStartDate() != null) {
			isPromoted = true;
		}
		return isPromoted;
	}

	public boolean isItemPromotedForFutWeek() {
		boolean isPromoted = false;
		if (this.futWeekSaleInfo.getSalePrice() != null || this.futWeekAdInfo.getAdPageNo() > 0) {
			isPromoted = true;
		}
		return isPromoted;
	}

	public MultiplePrice getCompCurSalePrice() {
		return compCurSalePrice;
	}

	public void setCompCurSalePrice(MultiplePrice compCurSalePrice) {
		this.compCurSalePrice = compCurSalePrice;
	}

	public int getIsTPR() {
		return isTPR;
	}

	public void setIsTPR(int isTPR) {
		this.isTPR = isTPR;
	}

	public int getIsOnSale() {
		return isOnSale;
	}

	public void setIsOnSale(int isOnSale) {
		this.isOnSale = isOnSale;
	}

	public int getIsOnAd() {
		return isOnAd;
	}

	public void setIsOnAd(int isOnAd) {
		this.isOnAd = isOnAd;
	}

	public HashMap<LocationKey, MultiplePrice> getAllCompSalePrice() {
		return allCompSalePrice;
	}

	public void setAllCompSalePrice(HashMap<LocationKey, MultiplePrice> allCompSalePrice) {
		this.allCompSalePrice = allCompSalePrice;
	}

	public void addAllCompSalePrice(LocationKey locationKey, MultiplePrice compSalePrice) {
		this.allCompSalePrice.put(locationKey, compSalePrice);
	}

	public Double getRecWeekDealCost() {
		return recWeekDealCost;
	}

	public void setRecWeekDealCost(Double recWeekDealCost) {
		this.recWeekDealCost = recWeekDealCost;
	}

	public boolean isFutureRetailRecommended() {
		return isFutureRetailRecommended;
	}

	public void setFutureRetailRecommended(boolean isFutureRetailRecommended) {
		this.isFutureRetailRecommended = isFutureRetailRecommended;
	}

	public String getRecPriceEffectiveDate() {
		return recPriceEffectiveDate;
	}

	public void setRecPriceEffectiveDate(String recPriceEffectiveDate) {
		this.recPriceEffectiveDate = recPriceEffectiveDate;
	}

//	public void setExplainRetailNoteTypeLookupId(int noteTypeLookupId) {
//		if (this.getExplainLog() == null) {
//			this.setExplainLog(new PRExplainLog());
//		}
//		this.getExplainLog().setExplainRetailNoteTypeLookupId(noteTypeLookupId);
//	}

	public Double getPredictedMovOfPricePoint(MultiplePrice multiplePrice) {
		Double predMov = null;
		if (this.getRegPricePredictionMap().get(multiplePrice) != null) {
			predMov = this.getRegPricePredictionMap().get(multiplePrice).getPredictedMovement();
		}

		return predMov;
	}

	public int getPredictedStatusOfPricePoint(MultiplePrice multiplePrice) {
		int predStatus = PredictionStatus.UNDEFINED.getStatusCode();
		if (this.getRegPricePredictionMap().get(multiplePrice) != null) {
			predStatus = this.getRegPricePredictionMap().get(multiplePrice).getPredictionStatus().getStatusCode();
		}

		return predStatus;
	}

	public Integer getCurRegPricePredictionStatus() {
		return curRegPricePredictionStatus;
	}

	public void setCurRegPricePredictionStatus(Integer curRegPricePredictionStatus) {
		this.curRegPricePredictionStatus = curRegPricePredictionStatus;
	}

	public boolean isCurPriceRetained() {
		return isCurPriceRetained;
	}

	public void setCurPriceRetained(boolean isCurPriceRetained) {
		this.isCurPriceRetained = isCurPriceRetained;
	}

	public int getLigRepItemCode() {
		return ligRepItemCode;
	}

	public void setLigRepItemCode(int ligRepItemCode) {
		this.ligRepItemCode = ligRepItemCode;
	}

	/*
	 * public int getRecRegPricePredictionConfidence() { return
	 * recRegPricePredictionConfidence; }
	 * 
	 * public void setRecRegPricePredictionConfidence(int
	 * recRegPricePredictionConfidence) { this.recRegPricePredictionConfidence =
	 * recRegPricePredictionConfidence; }
	 */

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

	public Double getDealCost() {
		return dealCost;
	}

	public void setDealCost(Double dealCost) {
		this.dealCost = dealCost;
	}

	public Double getOppurtunityMovement() {
		return oppurtunityMovement;
	}

	public void setOppurtunityMovement(Double oppurtunityMovement) {
		this.oppurtunityMovement = oppurtunityMovement;
	}
	public int getDeptIdPromotion() {
		return deptIdPromotion;
	}

	public void setDeptIdPromotion(int deptIdPromotion) {
		this.deptIdPromotion = deptIdPromotion;
	}

	public int getUniqueHHCount() {
		return uniqueHHCount;
	}

	public void setUniqueHHCount(int uniqueHHCount) {
		this.uniqueHHCount = uniqueHHCount;
	}

	public String getUseLeadZoneStrategy() {
		return useLeadZoneStrategy;
	}

	public void setUseLeadZoneStrategy(String useLeadZoneStrategy) {
		this.useLeadZoneStrategy = useLeadZoneStrategy;
	}

	public MultiplePrice getOverriddenRegularPrice() {
		return overriddenRegularPrice;
	}

	public void setOverriddenRegularPrice(MultiplePrice overriddenRegularPrice) {
		this.overriddenRegularPrice = overriddenRegularPrice;
	}

	public boolean isSystemOverrideFlag() {
		return systemOverrideFlag;
	}

	public void setSystemOverrideFlag(boolean systemOverrideFlag) {
		this.systemOverrideFlag = systemOverrideFlag;
	}

	public int getUserOverrideFlag() {
		return userOverrideFlag;
	}

	public void setUserOverrideFlag(int userOverrideFlag) {
		this.userOverrideFlag = userOverrideFlag;
	}

	public int getOverrideRemoved() {
		return overrideRemoved;
	}

	public void setOverrideRemoved(int overrideRemoved) {
		this.overrideRemoved = overrideRemoved;
	}

	public MultiplePrice getRecRegPriceBeforeReRecommedation() {
		return recRegPriceBeforeReRecommedation;
	}

	public void setRecRegPriceBeforeReRecommedation(MultiplePrice recRegPriceBeforeReRecommedation) {
		this.recRegPriceBeforeReRecommedation = recRegPriceBeforeReRecommedation;
	}

	public boolean isRelationOverridden() {
		return isRelationOverridden;
	}

	public void setRelationOverridden(boolean isRelationOverridden) {
		this.isRelationOverridden = isRelationOverridden;
	}

	public boolean isRelatedItemRelationChanged() {
		return relatedItemRelationChanged;
	}

	public void setRelatedItemRelationChanged(boolean relatedItemRelationChanged) {
		this.relatedItemRelationChanged = relatedItemRelationChanged;
	}

	public Double getMinDealCost() {
		return minDealCost;
	}

	public void setMinDealCost(Double minDealCost) {
		this.minDealCost = minDealCost;
	}

	public boolean isItemInLongTermPromo() {
		return isItemInLongTermPromo;
	}

	public void setItemInLongTermPromo(boolean isItemInLongTermPromo) {
		this.isItemInLongTermPromo = isItemInLongTermPromo;
	}

	public String getBrandName() {
		return brandName;
	}

	public void setBrandName(String brandName) {
		this.brandName = brandName;
	}

	/*
	 * public HashMap<Integer, String> getBrands() { return brands; }
	 * 
	 * public void setBrands(HashMap<Integer, String> brands) { this.brands =
	 * brands; }
	 */

	public ProductKey getProductKey() {
		ProductKey productKey = null;
		if (this.isLir) {
			productKey = new ProductKey(Constants.PRODUCT_LEVEL_ID_LIG, this.retLirId);
		} else {
			productKey = new ProductKey(Constants.ITEMLEVELID, this.itemCode);
		}
		return productKey;
	}

	public Double getAllowanceCost() {
		return allowanceCost;
	}

	public void setAllowanceCost(Double allowanceCost) {
		this.allowanceCost = allowanceCost;
	}

	public boolean isLongTermAllowanceCost() {
		return isLongTermAllowanceCost;
	}

	public void setLongTermAllowanceCost(boolean isLongTermAllowanceCost) {
		this.isLongTermAllowanceCost = isLongTermAllowanceCost;
	}

	public boolean isOnGoingPromotion() {
		return isOnGoingPromotion;
	}

	public void setOnGoingPromotion(boolean isOnGoingPromotion) {
		this.isOnGoingPromotion = isOnGoingPromotion;
	}

	public boolean isFuturePromotion() {
		return isFuturePromotion;
	}

	public void setFuturePromotion(boolean isFuturePromotion) {
		this.isFuturePromotion = isFuturePromotion;
	}

	public boolean isPromoEndsWithinXWeeks() {
		return isPromoEndsWithinXWeeks;
	}

	public void setPromoEndsWithinXWeeks(boolean isPromoEndsWithinXWeeks) {
		this.isPromoEndsWithinXWeeks = isPromoEndsWithinXWeeks;
	}

	public boolean hasValidCost() {
		return this.listCost != null && this.listCost > 0 ? true : false;
	}

	public boolean hasValidCurrentRetail() {
		MultiplePrice curRegPrice = PRCommonUtil.getMultiplePrice(this.regMPack, this.regPrice, this.regMPrice);
		return curRegPrice != null && curRegPrice.price > 0 ? true : false;
	}

	public boolean isDSDItem() {
		return isDSDItem;
	}

	public void setDSDItem(boolean isDSDItem) {
		this.isDSDItem = isDSDItem;
	}

	public boolean isNonMovingItem() {
		return isNonMovingItem;
	}

	public void setNonMovingItem(boolean isNonMovingItem) {
		this.isNonMovingItem = isNonMovingItem;
	}

	public boolean isCurrentPriceRetained() {
		return isCurrentPriceRetained;
	}

	public void setCurrentPriceRetained(boolean isCurrentPriceRetained) {
		this.isCurrentPriceRetained = isCurrentPriceRetained;
	}

	public boolean isRecProcessCompleted() {
		return isRecProcessCompleted;
	}

	public void setRecProcessCompleted(boolean isRecProcessCompleted) {
		this.isRecProcessCompleted = isRecProcessCompleted;
	}

	public MultiplePrice getRecRegPriceBeforeOverride() {
		return recRegPriceBeforeOverride;
	}

	public void setRecRegPriceBeforeOverride(MultiplePrice recRegPriceBeforeOverride) {
		this.recRegPriceBeforeOverride = recRegPriceBeforeOverride;
	}

	public String getPastOverrideDate() {
		return pastOverrideDate;
	}

	public void setPastOverrideDate(String pastOverrideDate) {
		this.pastOverrideDate = pastOverrideDate;
	}

	public int getUpdateRecommendationStatus() {
		return updateRecommendationStatus;
	}

	public void setUpdateRecommendationStatus(int updateRecommendationStatus) {
		this.updateRecommendationStatus = updateRecommendationStatus;
	}

	public boolean isFutureRetailPresent() {
		return isFutureRetailPresent;
	}

	public void setFutureRetailPresent(boolean isFutureRetailPresent) {
		this.isFutureRetailPresent = isFutureRetailPresent;
	}

	public boolean isAuthorized() {
		return isAuthorized;
	}

	public void setAuthorized(boolean isAuthorized) {
		this.isAuthorized = isAuthorized;
	}

	public boolean isActive() {
		return isActive;
	}

	public void setActive(boolean isActive) {
		this.isActive = isActive;
	}

	public int getCriteriaId() {
		return criteriaId;
	}

	public void setCriteriaId(int criteriaId) {
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

	public String getItemSetupDate() {
		return itemSetupDate;
	}

	public void setItemSetupDate(String itemSetupDate) {
		this.itemSetupDate = itemSetupDate;
	}

	public double getFamilyXWeeksMov() {
		return familyXWeeksMov;
	}

	public void setFamilyXWeeksMov(double familyAvgMovement) {
		this.familyXWeeksMov = familyAvgMovement;
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

	public double getPriceChangeImpact() {
		return priceChangeImpact;
	}

	public void setPriceChangeImpact(double priceChangeImpact) {
		this.priceChangeImpact = priceChangeImpact;
	}

	public int getRecUnitProductId() {
		return recUnitProductId;
	}

	public void setRecUnitProductId(int recUnitProductId) {
		this.recUnitProductId = recUnitProductId;
	}

	// for price export
	private int productLevelId;
	private int productId;
	private int locationLevelId;
	private int locationId;
	private String itemType;
	private int newRegQty;
	private String newRetail;
	private String exported;
	private String regEffDate;
	private String approved;
	private String approvedBy;
	private Double VdpRetail;
	private Double impact;
	private int oldQty;
	private String oldRetail;
	private String predicted;
	private String partNumber;
	private MultiplePrice currentRegPrice = null;
	private int primaryDC;
	private String storeId;
	private String zoneName;
	private String recommendationUnit;
	private int recommendationUnitId;
	private Double diffRetail = 0D;
	private boolean isMemberProcessed;
	private String exportStatus;
	private int statusCode;
	private String StoreLockExpiryFlag;
	private char exportFlagOfStoreLockItem;
	private char expiryOnCurrentDate;
	private boolean emergencyInHardPart;
	private boolean emergencyInSaleFloor;
	private int levelId;
	private int levelTypeId;
	private String approverName;
	private String priceExportType;
	private boolean globalZoneRecommended;
	private boolean storeListExpiry;

	
	public int getRecommendationUnitId() {
		return recommendationUnitId;
	}

	public void setRecommendationUnitId(int recommendationUnitId) {
		this.recommendationUnitId = recommendationUnitId;
	}

	public String getPartNumber() {
		return partNumber;
	}

	public void setPartNumber(String partNumber) {
		this.partNumber = partNumber;
	}

	public String getPredicted() {
		return predicted;
	}

	public void setPredicted(String predicted) {
		this.predicted = predicted;
	}

	public String getExported() {
		return exported;
	}

	public void setExported(String exported) {
		this.exported = exported;
	}

	public String getApproved() {
		return approved;
	}

	public void setApproved(String approved) {
		this.approved = approved;
	}

	public String getApprovedBy() {
		return approvedBy;
	}

	public void setApprovedBy(String approvedBy) {
		this.approvedBy = approvedBy;
	}

	public Double getImpact() {
		return impact;
	}

	public void setImpact(Double impact) {
		this.impact = impact;
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

	public int getLocationLevelId() {
		return locationLevelId;
	}

	public void setLocationLevelId(int locationLevelId) {
		this.locationLevelId = locationLevelId;
	}

	public int getLocationId() {
		return locationId;
	}

	public void setLocationId(int locationId) {
		this.locationId = locationId;
	}

	public String getItemType() {
		return itemType;
	}

	public void setItemType(String itemType) {
		this.itemType = itemType;
	}

	public int getRetLirID() {
		return retLirId;
	}

	public void setRetailerId(int retailerId) {
		this.retLirId = retailerId;
	}

	public int getNewRegQty() {
		return newRegQty;
	}

	public void setNewRegQty(int newRegQty) {
		this.newRegQty = newRegQty;
	}

	public String getNewRetail() {
		return newRetail;
	}

	public void setNewRetail(String newRetail) {
		this.newRetail = newRetail;
	}

	public String getRegEffDate() {
		return regEffDate;
	}

	public void setRegEffDate(String regEffDate) {
		this.regEffDate = regEffDate;
	}

	public Double getVdpRetail() {
		return VdpRetail;
	}

	public void setVdpRetail(Double vdpRetail) {
		VdpRetail = vdpRetail;
	}

	public int getOldQty() {
		return oldQty;
	}

	public void setOldQty(int oldQty) {
		this.oldQty = oldQty;
	}

	public String getOldRetail() {
		return oldRetail;
	}

	public void setOldRetail(String oldRetail) {
		this.oldRetail = oldRetail;
	}

	@Override
	public Object clone() throws CloneNotSupportedException {
		PRItemDTO cloned = (PRItemDTO) super.clone();
		return cloned;
	}

	public MultiplePrice getCurrentRegPrice() {
		return currentRegPrice;
	}

	public void setCurrentRegPrice(MultiplePrice currentRegPrice) {
		this.currentRegPrice = currentRegPrice;
	}

	public String getStoreId() {
		return storeId;
	}

	public void setStoreId(String storeId) {
		this.storeId = storeId;
	}

	public int getPrimaryDC() {
		return primaryDC;
	}

	public void setPrimaryDC(int primaryDC) {
		this.primaryDC = primaryDC;
	}

	public String getZoneName() {
		return zoneName;
	}

	public void setZoneName(String zoneName) {
		this.zoneName = zoneName;
	}

	public String getRecommendationUnit() {
		return recommendationUnit;
	}

	public void setRecommendationUnit(String recommendationUnit) {
		this.recommendationUnit = recommendationUnit;
	}

	public Double getDiffRetail() {
		return diffRetail;
	}

	public void setDiffRetail(Double diffRetail) {
		this.diffRetail = diffRetail;
	}

	public boolean getIsMemberProcessed() {
		return isMemberProcessed;
	}

	public void setIsMemberProcessed(boolean isMemberProcessed) {
		this.isMemberProcessed = isMemberProcessed;
	}

	public String getExportStatus() {
		return exportStatus;
	}

	public void setExportStatus(String exportStatus) {
		this.exportStatus = exportStatus;
	}

	public int getStatusCode() {
		return statusCode;
	}

	public void setStatusCode(int statusCode) {
		this.statusCode = statusCode;
	}

	public boolean isEmergencyInHardPart() {
		return emergencyInHardPart;
	}

	public void setEmergencyInHardPart(boolean emergencyInHardPart) {
		this.emergencyInHardPart = emergencyInHardPart;
	}

	public boolean isEmergencyInSaleFloor() {
		return emergencyInSaleFloor;
	}

	public void setEmergencyInSaleFloor(boolean emergencyInSaleFloor) {
		this.emergencyInSaleFloor = emergencyInSaleFloor;
	}

	public int getLevelId() {
		return levelId;
	}

	public void setLevelId(int levelId) {
		this.levelId = levelId;
	}

	public int getLevelTypeId() {
		return levelTypeId;
	}

	public void setLevelTypeId(int levelTypeId) {
		this.levelTypeId = levelTypeId;
	}

	public boolean isAucOverride() {
		return aucOverride;
	}

	public void setAucOverride(boolean aucOverride) {
		this.aucOverride = aucOverride;
	}

	public List<String> getMissingTierInfo() {
		return missingTierInfo;
	}

	public void setMissingTierInfo(List<String> missingTierInfo) {
		this.missingTierInfo = missingTierInfo;
	}

	public String getStoreLockExpiryFlag() {
		return StoreLockExpiryFlag;
	}

	public void setStoreLockExpiryFlag(String storeLockExpiryFlag) {
		StoreLockExpiryFlag = storeLockExpiryFlag;
	}

	public char getExportFlagOfStoreLockItem() {
		return exportFlagOfStoreLockItem;
	}

	public void setExportFlagOfStoreLockItem(char exportFlagOfStoreLockItem) {
		this.exportFlagOfStoreLockItem = exportFlagOfStoreLockItem;
	}

	public char getExpiryOnCurrentDate() {
		return expiryOnCurrentDate;
	}

	public void setExpiryOnCurrentDate(char expiryOnCurrentDate) {
		this.expiryOnCurrentDate = expiryOnCurrentDate;
	}

	public int getPricePointsFiltered() {
		return pricePointsFiltered;
	}

	public void setPricePointsFiltered(int pricePointsFiltered) {
		this.pricePointsFiltered = pricePointsFiltered;
	}

	public String getApproverName() {
		return approverName;
	}

	public void setApproverName(String approverName) {
		this.approverName = approverName;
	}

	public String getPriceExportType() {
		return priceExportType;
	}

	public void setPriceExportType(String priceExportType) {
		this.priceExportType = priceExportType;
	}

	public boolean isGlobalZoneRecommended() {
		return globalZoneRecommended;
	}

	public void setGlobalZoneRecommended(boolean globalZoneRecommended) {
		this.globalZoneRecommended = globalZoneRecommended;
	}

	public double getRecentXWeeksMov() {
		return recentXWeeksMov;
	}

	public void setRecentXWeeksMov(double recentXWeeksMov) {
		this.recentXWeeksMov = recentXWeeksMov;
	}

	public boolean isNoRecentWeeksMov() {
		return noRecentWeeksMovement;
	}

	public void setNoRecentWeeksMov(boolean noRecentWeeksMov) {
		this.noRecentWeeksMovement = noRecentWeeksMov;
	}

	public double getPredExcludeWeeksUnits() {
		return predExcludeWeeksUnits;
	}

	public void setPredExcludeWeeksUnits(double predExcludeWeeksUnits) {
		this.predExcludeWeeksUnits = predExcludeWeeksUnits;
	}

	public double getPredExcludeWeeksSales() {
		return predExcludeWeeksSales;
	}

	public void setPredExcludeWeeksSales(double predExcludeWeeksSales) {
		this.predExcludeWeeksSales = predExcludeWeeksSales;
	}

	public double getPredExcludeSalesPerStore() {
		return predExcludeSalesPerStore;
	}

	public void setPredExcludeSalesPerStore(double predExcludeSalesPerStore) {
		this.predExcludeSalesPerStore = predExcludeSalesPerStore;
	}

	public boolean isStoreListExpiry() {
		return storeListExpiry;
	}

	public void setStoreListExpiry(boolean storeListExpiry) {
		this.storeListExpiry = storeListExpiry;
	}

	public double getxWeeksMovForTotimpact() {
		return xWeeksMovForTotimpact;
	}

	public void setxWeeksMovForTotimpact(double xWeeksMovForTotimpact) {
		this.xWeeksMovForTotimpact = xWeeksMovForTotimpact;
	}

	public double getxWeeksMovForAddlCriteria() {
		return xWeeksMovForAddlCriteria;
	}

	public void setxWeeksMovForAddlCriteria(double xWeeksMovement) {
		this.xWeeksMovForAddlCriteria = xWeeksMovement;
	}

	public String getFamilyName() {
		return familyName;
	}

	public void setFamilyName(String familyName) {
		this.familyName = familyName;
	}

	
	

	public boolean isSendToPrediction() {
		return sendToPrediction;
	}

	public void setSendToPrediction(boolean isStrRevenueLess) {
		this.sendToPrediction = isStrRevenueLess;
	}

	public BigDecimal getTotalRevenue() {
		return totalRevenue;
	}

	public void setTotalRevenue(BigDecimal totalRevenue) {
		this.totalRevenue = totalRevenue;
	}

	public double getECRetail() {
		return ECRetail;
	}

	public void setECRetail(double eCRetail) {
		ECRetail = eCRetail;
	}

	public String getStartDate() {
		return startDate;
	}

	public void setStartDate(String startDate) {
		this.startDate = startDate;
	}

	public String getItemListComments() {
		return itemListComments;
	}

	public void setItemListComments(String itemListComments) {
		this.itemListComments = itemListComments;
	}

	public int getItemListHeaderId() {
		return itemListHeaderId;
	}

	public void setItemListHeaderId(int itemListHeaderId) {
		this.itemListHeaderId = itemListHeaderId;
	}

	public double getxWeeksMovForWAC() {
		return xWeeksMovForWAC;
	}

	public void setxWeeksMovForWAC(double xWeeksMovForWAC) {
		this.xWeeksMovForWAC = xWeeksMovForWAC;
	}

	public double getOriginalListCost() {
		return originalListCost;
	}

	public void setOriginalListCost(double originalListCost) {
		this.originalListCost = originalListCost;
	}

	public String getUserAttr15() {
		return userAttr15;
	}

	public void setUserAttr15(String userAttr15) {
		this.userAttr15 = userAttr15;
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

	public void setXweekMov(double xweekMov) {
		this.xweekMov = xweekMov;
	}

	public double getWeightedRecRetail() {
		return weightedRecRetail;
	}

	public void setWeightedRecRetail(double weightedRecRetail) {
		this.weightedRecRetail = weightedRecRetail;
	}

	public ItemKey getItemKey() {
		return itemKey;
	}

	public void setItemKey(ItemKey itemKey) {
		this.itemKey = itemKey;
	}

	public int getBrandTierId() {
		return BrandTierId;
	}

	public void setBrandTierId(int brandTierId) {
		BrandTierId = brandTierId;
	}

	public int getLeadTierID() {
		return leadTierID;
	}

	public void setLeadTierID(int leadTierID) {
		this.leadTierID = leadTierID;
	}

	public boolean isCompOverride() {
		return compOverride;
	}

	public void setCompOverride(boolean compOverride) {
		this.compOverride = compOverride;
	}

	public char getValueType() {
		return valueType;
	}

	public void setValueType(char valueType) {
		this.valueType = valueType;
	}

	public double getWeightedComp6retail() {
		return weightedComp6retail;
	}

	public void setWeightedComp6retail(double weightedComp6retail) {
		this.weightedComp6retail = weightedComp6retail;
	}

	public int getComp6StrId() {
		return comp6StrId;
	}

	public void setComp6StrId(int comp6StrId) {
		this.comp6StrId = comp6StrId;
	}

	public double getWeightedRegretail() {
		return weightedRegretail;
	}

	public void setWeightedRegretail(double weightedRegretail) {
		this.weightedRegretail = weightedRegretail;
	}

	public double getWeightedComp1retail() {
		return weightedComp1retail;
	}

	public void setWeightedComp1retail(double weightedComp1retail) {
		this.weightedComp1retail = weightedComp1retail;
	}

	public double getWeightedComp2retail() {
		return weightedComp2retail;
	}

	public void setWeightedComp2retail(double weightedComp2retail) {
		this.weightedComp2retail = weightedComp2retail;
	}

	public double getWeightedComp3retail() {
		return weightedComp3retail;
	}

	public void setWeightedComp3retail(double weightedComp3retail) {
		this.weightedComp3retail = weightedComp3retail;
	}

	public double getWeightedComp4retail() {
		return weightedComp4retail;
	}

	public void setWeightedComp4retail(double weightedComp4retail) {
		this.weightedComp4retail = weightedComp4retail;
	}

	public double getWeightedComp5retail() {
		return weightedComp5retail;
	}

	public void setWeightedComp5retail(double weightedComp5retail) {
		this.weightedComp5retail = weightedComp5retail;
	}

	public double getXweekMov() {
		return xweekMov;
	}

	public double getxWeeksMovForAddlCriteriaAtLIGLevel() {
		return xWeeksMovForAddlCriteriaAtLIGLevel;
	}

	public void setxWeeksMovForAddlCriteriaAtLIGLevel(double xWeeksMovForAddlCriteriaAtLIGLevel) {
		this.xWeeksMovForAddlCriteriaAtLIGLevel = xWeeksMovForAddlCriteriaAtLIGLevel;
	}

	public HashMap<Integer, Double> getZonePriceMap() {
		return zonePriceMap;
	}

	public void setZonePriceMap(HashMap<Integer, Double> zonePriceMap) {
		this.zonePriceMap = zonePriceMap;
	}

	public boolean isBrandGuidelineApplied() {
		return brandGuidelineApplied;
	}

	public void setBrandGuidelineApplied(boolean brandGuidelineApplied) {
		this.brandGuidelineApplied = brandGuidelineApplied;
	}

	public boolean isCompOverCost() {
		return compOverCost;
	}

	public void setCompOverCost(boolean compOverCost) {
		this.compOverCost = compOverCost;
	}

	public int getInventory() {
		return inventory;
	}

	public void setInventory(int inventory) {
		this.inventory = inventory;
	}

	public String getZoneType() {
		return zoneType;
	}

	public void setZoneType(String zoneType) {
		this.zoneType = zoneType;
	}

	public String getGlobalZone() {
		return globalZone;
	}

	public void setGlobalZone(String globalZone) {
		this.globalZone = globalZone;
	}

	public String getApprovedOn() {
		return approvedOn;
	}

	public void setApprovedOn(String approvedOn) {
		this.approvedOn = approvedOn;
	}

	public String getOperatorText() {
		return operatorText;
	}

	public void setOperatorText(String operatorText) {
		this.operatorText = operatorText;
	}

	public PRRange getMultiCompRange() {
		return multiCompRange;
	}

	public void setMultiCompRange(PRRange multiCompRange) {
		this.multiCompRange = multiCompRange;
	}

	public int getSF_week_rank() {
		return SF_week_rank;
	}

	public void setSF_week_rank(int sF_week_rank) {
		SF_week_rank = sF_week_rank;
	}

	public int getSF_export_rank() {
		return SF_export_rank;
	}

	public void setSF_export_rank(int sF_export_rank) {
		SF_export_rank = sF_export_rank;
	}

	public int getSF_RU_rank() {
		return SF_RU_rank;
	}

	public void setSF_RU_rank(int sF_RU_rank) {
		SF_RU_rank = sF_RU_rank;
	}

	public String getRU_zone() {
		return RU_zone;
	}

	public void setRU_zone(String rU_zone) {
		RU_zone = rU_zone;
	}

	public double getTotal_Impact() {
		return total_Impact;
	}

	public void setTotal_Impact(double total_Impact) {
		this.total_Impact = total_Impact;
	}

	public boolean isFamilyProcessed() {
		return familyProcessed;
	}

	public void setFamilyProcessed(boolean familyProcessed) {
		this.familyProcessed = familyProcessed;
	}

	public int getCalendarId() {
		return calendarId;
	}

	public void setCalendarId(int calendarId) {
		this.calendarId = calendarId;
	}

	public Set<String> getItemCount() {
		return itemCount;
	}

	public void setItemCount(Set<String> itemCount) {
		this.itemCount = itemCount;
	}

	public Set<String> getStoreCount() {
		return storeCount;
	}

	public void setStoreCount(Set<String> storeCount) {
		this.storeCount = storeCount;
	}

	public Double getFutureListCost() {
		return futureListCost;
	}

	public void setFutureListCost(Double futureListCost) {
		this.futureListCost = futureListCost;
	}

	public String getFutureCostEffDate() {
		return FutureCostEffDate;
	}

	public void setFutureCostEffDate(String futureCostEffDate) {
		FutureCostEffDate = futureCostEffDate;
	}

	public double getNipoBaseCost() {
		return nipoBaseCost;
	}

	public void setNipoBaseCost(double nipoBaseCost) {
		this.nipoBaseCost = nipoBaseCost;
	}

	public Map<Double, List<Double>> getCurrRetailslOfAllZones() {
		return currRetailslOfAllZones;
	}

	public void setCurrRetailslOfAllZones(Map<Double, List<Double>> currRetailslOfAllZones) {
		this.currRetailslOfAllZones = currRetailslOfAllZones;
	}

	public double getApprovedImpact() {
		return approvedImpact;
	}

	public double getApprovedRetail() {
		return approvedRetail;
	}

	public void setApprovedImpact(double approvedImpact) {
		this.approvedImpact = approvedImpact;
	}

	public void setApprovedRetail(double approvedRetail) {
		this.approvedRetail = approvedRetail;
	}

	public double getListCostWtotFrChg() {
		return listCostWtotFrChg;
	}

	public void setListCostWtotFrChg(double listCostWtotFrChg) {
		this.listCostWtotFrChg = listCostWtotFrChg;
	}

	public int getFreightChargeIncluded() {
		return freightChargeIncluded;
	}

	public void setFreightChargeIncluded(int freightChargeIncluded) {
		this.freightChargeIncluded = freightChargeIncluded;
	}

	public boolean isFreightCostSet() {
		return isFreightCostSet;
	}

	public void setFreightCostSet(boolean isFreightCostSet) {
		this.isFreightCostSet = isFreightCostSet;
	}

	
	public String getPriority() {
		return priority;
	}

	public void setPriority(String priority) {
		this.priority = priority;
	}

	public double getMapRetail() {
		return mapRetail;
	}

	public void setMapRetail(double mapRetail) {
		this.mapRetail = mapRetail;
	}

	public double getNipoCoreCost() {
		return nipoCoreCost;
	}

	public void setNipoCoreCost(double nipoCoreCost) {
		this.nipoCoreCost = nipoCoreCost;
	}

	private Set<String> strNum;

		
	public Set<String> getStrNums() {
		return strNum;
	}

	public void setStrNums(Set<String> strNums) {
		this.strNum = strNums;
	}

	public boolean isCurrentPriceBelowMAP() {
		return currentPriceBelowMAP;
	}

	public void setCurrentPriceBelowMAP(boolean currentPriceBelowMAP) {
		this.currentPriceBelowMAP = currentPriceBelowMAP;
	}

	public double getXweekMovForLIGRepItem() {
		return xweekMovForLIGRepItem;
	}

	public void setXweekMovForLIGRepItem(double xweekMovForLIGRepItem) {
		this.xweekMovForLIGRepItem = xweekMovForLIGRepItem;
	}

	public boolean isFinalObjectiveApplied() {
		return isFinalObjectiveApplied;
	}

	public void setFinalObjectiveApplied(boolean isFinalObjectiveApplied) {
		this.isFinalObjectiveApplied = isFinalObjectiveApplied;
	}

	public double getRecommendedPricewithMap() {
		return recommendedPricewithMap;
	}

	public void setRecommendedPricewithMap(double recommendedPricewithMap) {
		this.recommendedPricewithMap = recommendedPricewithMap;
	}

	public double getCwagBaseCost() {
		return cwagBaseCost;
	}

	public void setCwagBaseCost(double cwagBaseCost) {
		this.cwagBaseCost = cwagBaseCost;
	}

	public Set<String> getStoreNums() {
		return storeNums;
	}

	public void setStoreNums(Set<String> storeNums) {
		this.storeNums = storeNums;
	}

	public boolean isOnHold() {
		return isOnHold;
	}

	public void setOnHold(boolean isOnHold) {
		this.isOnHold = isOnHold;
	}

	public MultiplePrice getPendingRetail() {
		return pendingRetail;
	}

	public void setPendingRetail(MultiplePrice pendingRetail) {
		this.pendingRetail = pendingRetail;
	}

	public int getIsPendingRetailRecommended() {
		return isPendingRetailRecommended;
	}

	public void setIsPendingRetailRecommended(int isPendingRetailRecommended) {
		this.isPendingRetailRecommended = isPendingRetailRecommended;
	}

	public PriceCheckListDTO getSecondaryPriceCheckList() {
		return secondaryPriceCheckList;
	}

	public void setSecondaryPriceCheckList(PriceCheckListDTO secondaryPriceCheckList) {
		this.secondaryPriceCheckList = secondaryPriceCheckList;
	}

	public char getIsImpactIncludedInSummaryCalculation() {
		return isImpactIncludedInSummaryCalculation;
	}

	public void setIsImpactIncludedInSummaryCalculation(char isImpactIncludedInSummaryCalculation) {
		this.isImpactIncludedInSummaryCalculation = isImpactIncludedInSummaryCalculation;
	}

	public boolean isClearanceItem() {
		return clearanceItem;
	}

	public void setClearanceItem(boolean clearanceItem) {
		this.clearanceItem = clearanceItem;
	}

	public double getClearanceRetail() {
		return clearnaceRetail;
	}

	public void setClearanceRetail(double clearnaceRetail) {
		this.clearnaceRetail = clearnaceRetail;
	}

	public String getClearanceRetailEffDate() {
		return clearanceRetailEffDate;
	}

	public void setClearanceRetailEffDate(String clearanceRetailEffDate) {
		this.clearanceRetailEffDate = clearanceRetailEffDate;
	}

	public boolean isFuturePricePresent() {
		return isFuturePricePresent;
	}

	public void setFuturePricePresent(boolean isFuturePricePresent) {
		this.isFuturePricePresent = isFuturePricePresent;
	}

	public double getFutureUnitPrice() {
		return futureUnitPrice;
	}

	public void setFutureUnitPrice(double futureUnitPrice) {
		this.futureUnitPrice = futureUnitPrice;
	}

	public String getFuturePriceEffDate() {
		return futurePriceEffDate;
	}

	public void setFuturePriceEffDate(String futurePriceEffDate) {
		this.futurePriceEffDate = futurePriceEffDate;
	}

	public boolean isPromoStartsWithinXWeeks() {
		return isPromoStartsWithinXWeeks;
	}

	public void setPromoStartsWithinXWeeks(boolean isPromoStartsWithinXWeeks) {
		this.isPromoStartsWithinXWeeks = isPromoStartsWithinXWeeks;
	}

	public boolean isLongTermpromotion() {
		return longTermpromotion;
	}

	public void setLongTermpromotion(boolean longTermpromotion) {
		this.longTermpromotion = longTermpromotion;
	}

}