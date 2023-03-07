package com.pristine.dto.offermgmt;

import java.io.Serializable;
import java.util.HashMap;
import java.util.List;
import java.util.Set;

import com.pristine.service.offermgmt.ItemKey;
import com.pristine.util.offermgmt.PRFormatHelper;

public class PriceExportDTO  implements Serializable, Cloneable{
	private static final long serialVersionUID = 8084443024039535860L;

	private int itemCode;
	private long runId;
	private Double regPrice = null;
	private String curRegPriceEffDate;
	private Double overrideRegPrice;
	private Integer overrideRegMultiple;
	private Integer priceCheckListId;
	private int calendarId;
	private int isNewPriceRecommended = 0;
	private int isConflict = 0;
	private boolean processed = false;
	private int childLocationLevelId;
	private int childLocationId;
	private Double vipCost = null;
	private int vendorId = -1;
	private String upc;
	private String retailerItemCode;
	private Integer priceCheckListTypeId;
	private int priceZoneId;
	private String priceZoneNo = "";
	private String zoneType;
	private String globalZone;
	private String itemName;
	private String storeNo = "";
	private MultiplePrice overriddenRegularPrice = null;
	private Double coreRetail;
	private double priceChangeImpact;
	private double minRetail;
	private double maxRetail;
	private double lockedRetail;
	private String endDate;
	private String familyName;
	private double ECRetail;
	private String startDate;
	private String itemListComments;
	private List<SecondaryZoneRecDTO> secondaryZones;
	private ItemKey itemKey;
	private Set<String> storeCount;
	private Set<String> itemCount;
	private HashMap<Integer, Double> zonePriceMap;
	private String approvedOn;
	private int SF_week_rank;
	private int SF_export_rank;
	private int SF_RU_rank;
	private String RU_zone;
	private double total_Impact;
	private boolean familyProcessed;
	private String priority;
	private int hardReasonCode;
	private String hdFlag;
	private String testZoneNumReq;
	private Set<String> storeNums;
	private List<PriceExportDTO> duplicateRecToDelete;
	private MultiplePrice recommendedRegPrice;
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
	private int retLirId;

	public PriceExportDTO() {

	}

	public PriceExportDTO(long runId, int itemCode) {
		this.runId = runId;
		this.itemCode = itemCode;
	}

	public PriceExportDTO(int itemcode, String retItem, String recUnit, int recUnitId) {
		this.itemCode = itemcode;
		this.retailerItemCode = retItem;
		this.recommendationUnitId = recUnitId;
		this.recommendationUnit = recUnit;
	}

	public PriceExportDTO(String retItemcode, String storeNo, String zoneNum, String zoneName, String predicted,
			String recomUnit, String partNum, String itemType, String regEffDate, double vdpPrice, double coreRetail,
			int childLoc, double diffret, String approvedBy, String apprvName, String priceExportType,
			String storeLockExpFlag) {
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

	public PriceExportDTO(int itemcode, String partNum, String itemtype, String retItem, double ecRet, String startDate,
			String endDate, int priceCkListId, int priceCkListTypeId, String aprvdBy, String storeNum, String storeId,
			String zoneNum, String aprvName) {
		this.itemCode = itemcode;
		this.partNumber = partNum;
		this.itemType = itemtype;
		this.retailerItemCode = retItem;
		this.ECRetail = ecRet;
		this.startDate = startDate;
		this.endDate = endDate;
		this.priceCheckListId = priceCkListId;
		this.priceCheckListTypeId = priceCkListTypeId;
		this.approvedBy = aprvdBy;
		this.storeNo = storeNum;
		this.storeId = storeId;
		this.priceZoneNo = zoneNum;
		this.approverName = aprvName;
	}

	public PriceExportDTO(long runId, int roductLevId, int prodId, int locLevId, int locId, String attr6, int retLirId,
			int zoneId, String zoneNUm, String retItemCOde, MultiplePrice recRegPrc, MultiplePrice recCurPrc,
			String effDate, String prcType, String apprby, String prrvName, double Vdpret, double coreRet,
			double Impact, String pred, String partnum, int ovrRegM, double ovrReg, MultiplePrice ovrPrc, double diff,
			String zoneName, String recUnit, int priceCheckListTypeId, String priceExportType, String approvedOn, int itemcode) {
		this.runId = runId;
		this.productLevelId = roductLevId;
		this.productId = prodId;
		this.locationLevelId = locLevId;
		this.locationId = locId;
		this.retLirId = retLirId;
		this.priceZoneId = zoneId;
		this.priceZoneNo = zoneNUm;
		this.retailerItemCode = retItemCOde;
		this.recommendedRegPrice = recRegPrc;
		this.currentRegPrice = recCurPrc;
		this.regEffDate = effDate;
		this.itemType = prcType;
		this.approvedBy = apprby;
		this.approvedOn = approvedOn;
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
		this.itemCode = itemcode;
	}
	
	public PriceExportDTO(int itemCode, long runId, Double regPrice, String curRegPriceEffDate, Double overrideRegPrice,
			Integer overrideRegMultiple, Integer priceCheckListId, int calendarId, int isNewPriceRecommended,
			int isConflict, boolean processed, int childLocationLevelId, int childLocationId, Double vipCost,
			int vendorId, String upc, String retailerItemCode, Integer priceCheckListTypeId, int priceZoneId,
			String priceZoneNo, String zoneType, String globalZone, String itemName, String storeNo,
			MultiplePrice overriddenRegularPrice, Double coreRetail, double priceChangeImpact, double minRetail,
			double maxRetail, double lockedRetail, String endDate, String familyName, double eCRetail, String startDate,
			String itemListComments, List<SecondaryZoneRecDTO> secondaryZones, ItemKey itemKey, Set<String> storeCount,
			Set<String> itemCount, HashMap<Integer, Double> zonePriceMap, String approvedOn, int sF_week_rank,
			int sF_export_rank, int sF_RU_rank, String rU_zone, double total_Impact, boolean familyProcessed,
			String priority, String testZoneNumReq, Set<String> storeNums, List<PriceExportDTO> duplicateRecToDelete,
			MultiplePrice recommendedRegPrice, int productLevelId, int productId, int locationLevelId, int locationId,
			String itemType, int newRegQty, String newRetail, String exported, String regEffDate, String approved,
			String approvedBy, Double vdpRetail, Double impact, int oldQty, String oldRetail, String predicted,
			String partNumber, MultiplePrice currentRegPrice, int primaryDC, String storeId, String zoneName,
			String recommendationUnit, int recommendationUnitId, Double diffRetail, boolean isMemberProcessed,
			String exportStatus, int statusCode, String storeLockExpiryFlag, char exportFlagOfStoreLockItem,
			char expiryOnCurrentDate, boolean emergencyInHardPart, boolean emergencyInSaleFloor, int levelId,
			int levelTypeId, String approverName, String priceExportType, boolean globalZoneRecommended,
			boolean storeListExpiry, int retLirId) {
		super();
		this.itemCode = itemCode;
		this.runId = runId;
		this.regPrice = regPrice;
		this.curRegPriceEffDate = curRegPriceEffDate;
		this.overrideRegPrice = overrideRegPrice;
		this.overrideRegMultiple = overrideRegMultiple;
		this.priceCheckListId = priceCheckListId;
		this.calendarId = calendarId;
		this.isNewPriceRecommended = isNewPriceRecommended;
		this.isConflict = isConflict;
		this.processed = processed;
		this.childLocationLevelId = childLocationLevelId;
		this.childLocationId = childLocationId;
		this.vipCost = vipCost;
		this.vendorId = vendorId;
		this.upc = upc;
		this.retailerItemCode = retailerItemCode;
		this.priceCheckListTypeId = priceCheckListTypeId;
		this.priceZoneId = priceZoneId;
		this.priceZoneNo = priceZoneNo;
		this.zoneType = zoneType;
		this.globalZone = globalZone;
		this.itemName = itemName;
		this.storeNo = storeNo;
		this.overriddenRegularPrice = overriddenRegularPrice;
		this.coreRetail = coreRetail;
		this.priceChangeImpact = priceChangeImpact;
		this.minRetail = minRetail;
		this.maxRetail = maxRetail;
		this.lockedRetail = lockedRetail;
		this.endDate = endDate;
		this.familyName = familyName;
		this.ECRetail = eCRetail;
		this.startDate = startDate;
		this.itemListComments = itemListComments;
		this.secondaryZones = secondaryZones;
		this.itemKey = itemKey;
		this.storeCount = storeCount;
		this.itemCount = itemCount;
		this.zonePriceMap = zonePriceMap;
		this.approvedOn = approvedOn;
		this.SF_week_rank = sF_week_rank;
		this.SF_export_rank = sF_export_rank;
		this.SF_RU_rank = sF_RU_rank;
		this.RU_zone = rU_zone;
		this.total_Impact = total_Impact;
		this.familyProcessed = familyProcessed;
		this.priority = priority;
		this.testZoneNumReq = testZoneNumReq;
		this.storeNums = storeNums;
		this.duplicateRecToDelete = duplicateRecToDelete;
		this.recommendedRegPrice = recommendedRegPrice;
		this.productLevelId = productLevelId;
		this.productId = productId;
		this.locationLevelId = locationLevelId;
		this.locationId = locationId;
		this.itemType = itemType;
		this.newRegQty = newRegQty;
		this.newRetail = newRetail;
		this.exported = exported;
		this.regEffDate = regEffDate;
		this.approved = approved;
		this.approvedBy = approvedBy;
		this.VdpRetail = vdpRetail;
		this.impact = impact;
		this.oldQty = oldQty;
		this.oldRetail = oldRetail;
		this.predicted = predicted;
		this.partNumber = partNumber;
		this.currentRegPrice = currentRegPrice;
		this.primaryDC = primaryDC;
		this.storeId = storeId;
		this.zoneName = zoneName;
		this.recommendationUnit = recommendationUnit;
		this.recommendationUnitId = recommendationUnitId;
		this.diffRetail = diffRetail;
		this.isMemberProcessed = isMemberProcessed;
		this.exportStatus = exportStatus;
		this.statusCode = statusCode;
		this.StoreLockExpiryFlag = storeLockExpiryFlag;
		this.exportFlagOfStoreLockItem = exportFlagOfStoreLockItem;
		this.expiryOnCurrentDate = expiryOnCurrentDate;
		this.emergencyInHardPart = emergencyInHardPart;
		this.emergencyInSaleFloor = emergencyInSaleFloor;
		this.levelId = levelId;
		this.levelTypeId = levelTypeId;
		this.approverName = approverName;
		this.priceExportType = priceExportType;
		this.globalZoneRecommended = globalZoneRecommended;
		this.storeListExpiry = storeListExpiry;
		this.retLirId = retLirId;
	}
	
	@Override
	public String toString() {
		return "PriceExportDTO [itemCode=" + itemCode + ", runId=" + runId + ", regPrice=" + regPrice
				+ ", retailerItemCode=" + retailerItemCode + ", priceZoneNo=" + priceZoneNo + ", globalZone="
				+ globalZone + ", storeNo=" + storeNo + ", coreRetail=" + coreRetail + ", priceChangeImpact="
				+ priceChangeImpact + ", RU_zone=" + RU_zone + ", total_Impact=" + total_Impact + ", priority="
				+ priority + ", hardReasonCode=" + hardReasonCode + ", hdFlag=" + hdFlag
				+ ", StoreLockExpiryFlag=" + StoreLockExpiryFlag + ", retLirId=" + retLirId + ", regEffDate=" + regEffDate + "]";
	}

	@Override
	public Object clone() throws CloneNotSupportedException {
		PriceExportDTO cloned = (PriceExportDTO) super.clone();
		return cloned;
	}

	public PriceExportDTO setRecommendedRegPrice(MultiplePrice recommendedRegPrice) {

		if (recommendedRegPrice != null) {
			MultiplePrice multiplePrice = new MultiplePrice(recommendedRegPrice.multiple,
					Double.valueOf(PRFormatHelper.roundToTwoDecimalDigit(recommendedRegPrice.price)));
			this.recommendedRegPrice = multiplePrice;
		} else {
			this.recommendedRegPrice = recommendedRegPrice;
		}
		return this;
	}

	public int getItemCode() {
		return itemCode;
	}

	public PriceExportDTO setItemCode(int itemCode) {
		this.itemCode = itemCode;
		return this;
	}

	public Double getRegPrice() {
		return regPrice;
	}

	public PriceExportDTO setRegPrice(Double regPrice) {
		this.regPrice = regPrice;
		return this;
	}

	public String getCurRegPriceEffDate() {
		return curRegPriceEffDate;
	}

	public PriceExportDTO setCurRegPriceEffDate(String curRegPriceEffDate) {
		this.curRegPriceEffDate = curRegPriceEffDate;
		return this;
	}

	public Double getOverrideRegPrice() {
		return overrideRegPrice;
	}

	public PriceExportDTO setOverrideRegPrice(Double overrideRegPrice) {
		this.overrideRegPrice = overrideRegPrice;
		return this;
	}

	public Integer getOverrideRegMultiple() {
		return overrideRegMultiple;
	}

	public PriceExportDTO setOverrideRegMultiple(Integer overrideRegMultiple) {
		this.overrideRegMultiple = overrideRegMultiple;
		return this;
	}

	public Integer getPriceCheckListId() {
		return priceCheckListId;
	}

	public PriceExportDTO setPriceCheckListId(Integer priceCheckListId) {
		this.priceCheckListId = priceCheckListId;
		return this;
	}

	public int getCalendarId() {
		return calendarId;
	}

	public PriceExportDTO setCalendarId(int calendarId) {
		this.calendarId = calendarId;
		return this;
	}

	public int getIsNewPriceRecommended() {
		return isNewPriceRecommended;
	}

	public PriceExportDTO setIsNewPriceRecommended(int isNewPriceRecommended) {
		this.isNewPriceRecommended = isNewPriceRecommended;
		return this;
	}

	public int getIsConflict() {
		return isConflict;
	}

	public PriceExportDTO setIsConflict(int isConflict) {
		this.isConflict = isConflict;
		return this;
	}

	public boolean isProcessed() {
		return processed;
	}

	public PriceExportDTO setProcessed(boolean processed) {
		this.processed = processed;
		return this;
	}

	public int getChildLocationLevelId() {
		return childLocationLevelId;
	}

	public PriceExportDTO setChildLocationLevelId(int childLocationLevelId) {
		this.childLocationLevelId = childLocationLevelId;
		return this;
	}

	public int getChildLocationId() {
		return childLocationId;
	}

	public PriceExportDTO setChildLocationId(int childLocationId) {
		this.childLocationId = childLocationId;
		return this;
	}

	public Double getVipCost() {
		return vipCost;
	}

	public PriceExportDTO setVipCost(Double vipCost) {
		this.vipCost = vipCost;
		return this;
	}

	public int getVendorId() {
		return vendorId;
	}

	public PriceExportDTO setVendorId(int vendorId) {
		this.vendorId = vendorId;
		return this;
	}

	public String getUpc() {
		return upc;
	}

	public PriceExportDTO setUpc(String upc) {
		this.upc = upc;
		return this;
	}

	public String getRetailerItemCode() {
		return retailerItemCode;
	}

	public PriceExportDTO setRetailerItemCode(String retailerItemCode) {
		this.retailerItemCode = retailerItemCode;
		return this;
	}

	public Integer getPriceCheckListTypeId() {
		return priceCheckListTypeId;
	}

	public PriceExportDTO setPriceCheckListTypeId(Integer priceCheckListTypeId) {
		this.priceCheckListTypeId = priceCheckListTypeId;
		return this;
	}

	public int getPriceZoneId() {
		return priceZoneId;
	}

	public PriceExportDTO setPriceZoneId(int priceZoneId) {
		this.priceZoneId = priceZoneId;
		return this;
	}

	public String getPriceZoneNo() {
		return priceZoneNo;
	}

	public PriceExportDTO setPriceZoneNo(String priceZoneNo) {
		this.priceZoneNo = priceZoneNo;
		return this;
	}

	public String getZoneType() {
		return zoneType;
	}

	public PriceExportDTO setZoneType(String zoneType) {
		this.zoneType = zoneType;
		return this;
	}

	public String getGlobalZone() {
		return globalZone;
	}

	public PriceExportDTO setGlobalZone(String globalZone) {
		this.globalZone = globalZone;
		return this;
	}

	public String getItemName() {
		return itemName;
	}

	public PriceExportDTO setItemName(String itemName) {
		this.itemName = itemName;
		return this;
	}

	public String getStoreNo() {
		return storeNo;
	}

	public PriceExportDTO setStoreNo(String storeNo) {
		this.storeNo = storeNo;
		return this;
	}

	public MultiplePrice getOverriddenRegularPrice() {
		return overriddenRegularPrice;
	}

	public PriceExportDTO setOverriddenRegularPrice(MultiplePrice overriddenRegularPrice) {
		this.overriddenRegularPrice = overriddenRegularPrice;
		return this;
	}

	public Double getCoreRetail() {
		return coreRetail;
	}

	public PriceExportDTO setCoreRetail(Double coreRetail) {
		this.coreRetail = coreRetail;
		return this;
	}

	public double getPriceChangeImpact() {
		return priceChangeImpact;
	}

	public PriceExportDTO setPriceChangeImpact(double priceChangeImpact) {
		this.priceChangeImpact = priceChangeImpact;
		return this;
	}

	public double getMinRetail() {
		return minRetail;
	}

	public PriceExportDTO setMinRetail(double minRetail) {
		this.minRetail = minRetail;
		return this;
	}

	public double getMaxRetail() {
		return maxRetail;
	}

	public PriceExportDTO setMaxRetail(double maxRetail) {
		this.maxRetail = maxRetail;
		return this;
	}

	public double getLockedRetail() {
		return lockedRetail;
	}

	public PriceExportDTO setLockedRetail(double lockedRetail) {
		this.lockedRetail = lockedRetail;
		return this;
	}

	public String getEndDate() {
		return endDate;
	}

	public PriceExportDTO setEndDate(String endDate) {
		this.endDate = endDate;
		return this;
	}

	public String getFamilyName() {
		return familyName;
	}

	public PriceExportDTO setFamilyName(String familyName) {
		this.familyName = familyName;
		return this;
	}

	public double getECRetail() {
		return ECRetail;
	}

	public PriceExportDTO setECRetail(double eCRetail) {
		ECRetail = eCRetail;
		return this;
	}

	public String getStartDate() {
		return startDate;
	}

	public PriceExportDTO setStartDate(String startDate) {
		this.startDate = startDate;
		return this;
	}

	public String getItemListComments() {
		return itemListComments;
	}

	public PriceExportDTO setItemListComments(String itemListComments) {
		this.itemListComments = itemListComments;
		return this;
	}

	public List<SecondaryZoneRecDTO> getSecondaryZones() {
		return secondaryZones;
	}

	public PriceExportDTO setSecondaryZones(List<SecondaryZoneRecDTO> secondaryZones) {
		this.secondaryZones = secondaryZones;
		return this;
	}

	public ItemKey getItemKey() {
		return itemKey;
	}

	public PriceExportDTO setItemKey(ItemKey itemKey) {
		this.itemKey = itemKey;
		return this;
	}

	public Set<String> getStoreCount() {
		return storeCount;
	}

	public PriceExportDTO setStoreCount(Set<String> storeCount) {
		this.storeCount = storeCount;
		return this;
	}

	public Set<String> getItemCount() {
		return itemCount;
	}

	public PriceExportDTO setItemCount(Set<String> itemCount) {
		this.itemCount = itemCount;
		return this;
	}

	public HashMap<Integer, Double> getZonePriceMap() {
		return zonePriceMap;
	}

	public PriceExportDTO setZonePriceMap(HashMap<Integer, Double> zonePriceMap) {
		this.zonePriceMap = zonePriceMap;
		return this;
	}

	public String getApprovedOn() {
		return approvedOn;
	}

	public PriceExportDTO setApprovedOn(String approvedOn) {
		this.approvedOn = approvedOn;
		return this;
	}

	public int getSF_week_rank() {
		return SF_week_rank;
	}

	public PriceExportDTO setSF_week_rank(int sF_week_rank) {
		SF_week_rank = sF_week_rank;
		return this;
	}

	public int getSF_export_rank() {
		return SF_export_rank;
	}

	public PriceExportDTO setSF_export_rank(int sF_export_rank) {
		SF_export_rank = sF_export_rank;
		return this;
	}

	public int getSF_RU_rank() {
		return SF_RU_rank;
	}

	public PriceExportDTO setSF_RU_rank(int sF_RU_rank) {
		SF_RU_rank = sF_RU_rank;
		return this;
	}

	public String getRU_zone() {
		return RU_zone;
	}

	public PriceExportDTO setRU_zone(String rU_zone) {
		RU_zone = rU_zone;
		return this;
	}

	public double getTotal_Impact() {
		return total_Impact;
	}

	public PriceExportDTO setTotal_Impact(double total_Impact) {
		this.total_Impact = total_Impact;
		return this;
	}

	public boolean isFamilyProcessed() {
		return familyProcessed;
	}

	public PriceExportDTO setFamilyProcessed(boolean familyProcessed) {
		this.familyProcessed = familyProcessed;
		return this;
	}

	public String getPriority() {
		return priority;
	}

	public PriceExportDTO setPriority(String priority) {
		this.priority = priority;
		return this;
	}

	
	public int getHardReasonCode() {
		return hardReasonCode;
	}

	public PriceExportDTO setHardReasonCode(int hardReasonCode) {
		this.hardReasonCode = hardReasonCode;
		return this;
	}

	public String getHdFlag() {
		return hdFlag;
	}

	public PriceExportDTO setHdFlag(String hdFlag) {
		this.hdFlag = hdFlag;
		return this;
	}

	public String getTestZoneNumReq() {
		return testZoneNumReq;
	}

	public PriceExportDTO setTestZoneNumReq(String testZoneNumReq) {
		this.testZoneNumReq = testZoneNumReq;
		return this;
	}

	public Set<String> getStoreNums() {
		return storeNums;
	}

	public PriceExportDTO setStoreNums(Set<String> storeNums) {
		this.storeNums = storeNums;
		return this;
	}

	public List<PriceExportDTO> getDuplicateRecToDelete() {
		return duplicateRecToDelete;
	}

	public PriceExportDTO setDuplicateRecToDelete(List<PriceExportDTO> duplicateRecToDelete) {
		this.duplicateRecToDelete = duplicateRecToDelete;
		return this;
	}

	public int getProductLevelId() {
		return productLevelId;
	}

	public PriceExportDTO setProductLevelId(int productLevelId) {
		this.productLevelId = productLevelId;
		return this;
	}

	public int getProductId() {
		return productId;
	}

	public PriceExportDTO setProductId(int productId) {
		this.productId = productId;
		return this;
	}

	public int getLocationLevelId() {
		return locationLevelId;
	}

	public PriceExportDTO setLocationLevelId(int locationLevelId) {
		this.locationLevelId = locationLevelId;
		return this;
	}

	public int getLocationId() {
		return locationId;
	}

	public PriceExportDTO setLocationId(int locationId) {
		this.locationId = locationId;
		return this;
	}

	public String getItemType() {
		return itemType;
	}

	public PriceExportDTO setItemType(String itemType) {
		this.itemType = itemType;
		return this;
	}

	public int getNewRegQty() {
		return newRegQty;
	}

	public PriceExportDTO setNewRegQty(int newRegQty) {
		this.newRegQty = newRegQty;
		return this;
	}

	public String getNewRetail() {
		return newRetail;
	}

	public PriceExportDTO setNewRetail(String newRetail) {
		this.newRetail = newRetail;
		return this;
	}

	public String getExported() {
		return exported;
	}

	public PriceExportDTO setExported(String exported) {
		this.exported = exported;
		return this;
	}

	public String getRegEffDate() {
		return regEffDate;
	}

	public PriceExportDTO setRegEffDate(String regEffDate) {
		this.regEffDate = regEffDate;
		return this;
	}

	public String getApproved() {
		return approved;
	}

	public PriceExportDTO setApproved(String approved) {
		this.approved = approved;
		return this;
	}

	public String getApprovedBy() {
		return approvedBy;
	}

	public PriceExportDTO setApprovedBy(String approvedBy) {
		this.approvedBy = approvedBy;
		return this;
	}

	public Double getVdpRetail() {
		return VdpRetail;
	}

	public PriceExportDTO setVdpRetail(Double vdpRetail) {
		VdpRetail = vdpRetail;
		return this;
	}

	public Double getImpact() {
		return impact;
	}

	public PriceExportDTO setImpact(Double impact) {
		this.impact = impact;
		return this;
	}

	public int getOldQty() {
		return oldQty;
	}

	public PriceExportDTO setOldQty(int oldQty) {
		this.oldQty = oldQty;
		return this;
	}

	public String getOldRetail() {
		return oldRetail;
	}

	public PriceExportDTO setOldRetail(String oldRetail) {
		this.oldRetail = oldRetail;
		return this;
	}

	public String getPredicted() {
		return predicted;
	}

	public PriceExportDTO setPredicted(String predicted) {
		this.predicted = predicted;
		return this;
	}

	public String getPartNumber() {
		return partNumber;
	}

	public PriceExportDTO setPartNumber(String partNumber) {
		this.partNumber = partNumber;
		return this;
	}

	public MultiplePrice getCurrentRegPrice() {
		return currentRegPrice;
	}

	public PriceExportDTO setCurrentRegPrice(MultiplePrice currentRegPrice) {
		this.currentRegPrice = currentRegPrice;
		return this;
	}

	public int getPrimaryDC() {
		return primaryDC;
	}

	public PriceExportDTO setPrimaryDC(int primaryDC) {
		this.primaryDC = primaryDC;
		return this;
	}

	public String getStoreId() {
		return storeId;
	}

	public PriceExportDTO setStoreId(String storeId) {
		this.storeId = storeId;
		return this;
	}

	public String getZoneName() {
		return zoneName;
	}

	public PriceExportDTO setZoneName(String zoneName) {
		this.zoneName = zoneName;
		return this;
	}

	public String getRecommendationUnit() {
		return recommendationUnit;
	}

	public PriceExportDTO setRecommendationUnit(String recommendationUnit) {
		this.recommendationUnit = recommendationUnit;
		return this;
	}

	public int getRecommendationUnitId() {
		return recommendationUnitId;
	}

	public PriceExportDTO setRecommendationUnitId(int recommendationUnitId) {
		this.recommendationUnitId = recommendationUnitId;
		return this;
	}

	public Double getDiffRetail() {
		return diffRetail;
	}

	public PriceExportDTO setDiffRetail(Double diffRetail) {
		this.diffRetail = diffRetail;
		return this;
	}

	public boolean isMemberProcessed() {
		return isMemberProcessed;
	}

	public PriceExportDTO setMemberProcessed(boolean isMemberProcessed) {
		this.isMemberProcessed = isMemberProcessed;
		return this;
	}

	public String getExportStatus() {
		return exportStatus;
	}

	public PriceExportDTO setExportStatus(String exportStatus) {
		this.exportStatus = exportStatus;
		return this;
	}

	public int getStatusCode() {
		return statusCode;
	}

	public PriceExportDTO setStatusCode(int statusCode) {
		this.statusCode = statusCode;
		return this;
	}

	public String getStoreLockExpiryFlag() {
		return StoreLockExpiryFlag;
	}

	public PriceExportDTO setStoreLockExpiryFlag(String storeLockExpiryFlag) {
		StoreLockExpiryFlag = storeLockExpiryFlag;
		return this;
	}

	public char getExportFlagOfStoreLockItem() {
		return exportFlagOfStoreLockItem;
	}

	public PriceExportDTO setExportFlagOfStoreLockItem(char exportFlagOfStoreLockItem) {
		this.exportFlagOfStoreLockItem = exportFlagOfStoreLockItem;
		return this;
	}

	public char getExpiryOnCurrentDate() {
		return expiryOnCurrentDate;
	}

	public PriceExportDTO setExpiryOnCurrentDate(char expiryOnCurrentDate) {
		this.expiryOnCurrentDate = expiryOnCurrentDate;
		return this;
	}

	public boolean isEmergencyInHardPart() {
		return emergencyInHardPart;
	}

	public PriceExportDTO setEmergencyInHardPart(boolean emergencyInHardPart) {
		this.emergencyInHardPart = emergencyInHardPart;
		return this;
	}

	public boolean isEmergencyInSaleFloor() {
		return emergencyInSaleFloor;
	}

	public PriceExportDTO setEmergencyInSaleFloor(boolean emergencyInSaleFloor) {
		this.emergencyInSaleFloor = emergencyInSaleFloor;
		return this;
	}

	public int getLevelId() {
		return levelId;
	}

	public PriceExportDTO setLevelId(int levelId) {
		this.levelId = levelId;
		return this;
	}

	public int getLevelTypeId() {
		return levelTypeId;
	}

	public PriceExportDTO setLevelTypeId(int levelTypeId) {
		this.levelTypeId = levelTypeId;
		return this;
	}

	public String getApproverName() {
		return approverName;
	}

	public PriceExportDTO setApproverName(String approverName) {
		this.approverName = approverName;
		return this;
	}

	public String getPriceExportType() {
		return priceExportType;
	}

	public PriceExportDTO setPriceExportType(String priceExportType) {
		this.priceExportType = priceExportType;
		return this;
	}

	public boolean isGlobalZoneRecommended() {
		return globalZoneRecommended;
	}

	public PriceExportDTO setGlobalZoneRecommended(boolean globalZoneRecommended) {
		this.globalZoneRecommended = globalZoneRecommended;
		return this;
	}

	public boolean isStoreListExpiry() {
		return storeListExpiry;
	}

	public PriceExportDTO setStoreListExpiry(boolean storeListExpiry) {
		this.storeListExpiry = storeListExpiry;
		return this;
	}

	public MultiplePrice getRecommendedRegPrice() {
		return recommendedRegPrice;
	}

	public long getRunId() {
		return runId;
	}

	public PriceExportDTO setRunId(long runId) {
		this.runId = runId;
		return this;
	}

	public int getRetLirId() {
		return retLirId;
	}

	public PriceExportDTO setRetLirId(int retLirId) {
		this.retLirId = retLirId;
		return this;
	}

}
