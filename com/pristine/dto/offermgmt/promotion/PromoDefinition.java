package com.pristine.dto.offermgmt.promotion;

import java.util.ArrayList;
import java.util.Date;
import java.util.List;

import com.pristine.dto.offermgmt.LocationKey;
import com.pristine.util.Constants;

public class PromoDefinition implements Cloneable {
	private long promoDefnId;
	private int promoTypeId;
	private String promoName;
	private int startCalId;
	private int endCalId;
	private int status;
	private String createdBy;
	private String modifiedBy;
	private String approvedBy;
	private String weekStartDate;
	private String pomoNum;
	private String themeCode;
	private List<PromoLocation> promoLocation = new ArrayList<PromoLocation>();
	private List<PromoBuyRequirement> promoBuyRequirement = new ArrayList<PromoBuyRequirement>();
	private List<PromoBuyItems> buyItems = new ArrayList<PromoBuyItems>();
	private List<String> participatingItems = new ArrayList<String>();
	private Date promoStartDate;
	private Date promoEndDate;
	private String addtlDetails;
	private String adPage;
	private String blockNum;
	private String adDesc;
	private String addDetail;
	private String isInAd;
	private String manuCpn;
	// Added for Must Buy Promotion
	private int noOfItems;
	private String retailForPurchase;
	private String addtlQtyRetail;
	
	// Added for Catalina Promotions
	private String tier1Req;
	private String tier1Amt;
	private String tier2Req;
	private String tier2Amt;
	
	// Added for Super Coupon and e-Bonus Coupon
	private String couponDesc;
	private String amountOff;
	
	private String UPC;

	// Added for BOGO
	private int buyQty;
	private int getQty;
	private boolean canBeAdded = true;
	private int totalItems;
	private long adjustedUnits;
	private int retLirId;
	private String retailerItemCode;
	private int itemCode;
	private int promoDefnTypeId;
	private Date superCouponStartDate;
	private Date superCouponEndDate;
	private int superCouponStartCalID;
	private int superCouponEndCalId;
	private int regQty;
	private double regPrice;
	private int saleQty;
	private double salePrice;
	private double pctOff;
	private String itemPriceCode;
	private double thresholdValue;
	private String thresholdType;
	private int promoSubTypeId;
	private int locationLevelId;
	private int locationId;
    private  double dollarOff;	
	// Added for BMSM
	private double minimumAmt;
	int minimumQty;
	private double bmsmDollaroffperunits;
	private double bmsmPctoffperunit;
	private int bmsmsaleQty;
	private double bmsmsalePrice;
	//for peapod BMSM promo
	private String promoGroup;
	
	//added for RiteAid BAGB promotion
	private String offerItem;
	

	public String getOfferItem() {
		return offerItem;
	}
	public void setOfferItem(String offerItem) {
		this.offerItem = offerItem;
	}

	private String edlpPromoFlag = String.valueOf(Constants.NO);
	public int getItemCode() {
		return itemCode;
	}
	public void setItemCode(int itemCode) {
		this.itemCode = itemCode;
	}
	public String getRetailerItemCode() {
		return retailerItemCode;
	}
	public void setRetailerItemCode(String retailerItemCode) {
		this.retailerItemCode = retailerItemCode;
	}
	public int getRetLirId() {
		return retLirId;
	}
	public void setRetLirId(int retLirId) {
		this.retLirId = retLirId;
	}
	public long getAdjustedUnits() {
		return adjustedUnits;
	}
	public void setAdjustedUnits(long adjustedUnits) {
		this.adjustedUnits = adjustedUnits;
	}
	public int getTotalItems() {
		return totalItems;
	}
	public void setTotalItems(int totalItems) {
		this.totalItems = totalItems;
	}
	public void setPromoNumber(String Num){
		this.pomoNum = Num;
	}
	public String getPromoNumber(){
		return pomoNum;
	}
	public void setIsinAd(String adInd){
		this.isInAd = adInd;
	}
	public String getIsinAd(){
		return isInAd;
	}
	public void setAdpage(String page){
		this.adPage = page;
	}
	public String getAdpage(){
		return adPage;
	}
	public void setBlockNum(String blkNum){
		this.blockNum = blkNum;
	}
	public String getBlockNum(){
		return blockNum;
	}
	public void setAddesc(String desc){
		this.adDesc = desc;
	}
	public String getAddesc(){
		return adDesc;
	}
	public void setAddDetail(String Dtl){
		this.addDetail = Dtl;
	}
	public String getAddDetail(){
		return addDetail;
	}
	public void setThemeCode(String thmNum){
		this.themeCode = thmNum;
	}
	public String getThemeCode(){
		return themeCode;
	}
	
	public void setBuyQuantity(int qty){
		this.buyQty = qty;
	}
	public void setGetQuantity(int qty){
		this.getQty = qty;
	}
	
	public void setUPC(String UPC){
		this.UPC = UPC;
	}
	public String getUPC(){
		return UPC;
	}
	
	public int getBuyQuantity(){
		return buyQty;
	}
	
	public int getGetQuantity(){
		return getQty;
	}
	
	public long getPromoDefnId() {
		return promoDefnId;
	}

	public void setPromoDefnId(long promoDefnId) {
		this.promoDefnId = promoDefnId;
	}

	public int getPromoTypeId() {
		return promoTypeId;
	}

	public void setPromoTypeId(int promoTypeId) {
		this.promoTypeId = promoTypeId;
	}

	public String getPromoName() {
		return promoName;
	}

	public void setPromoName(String promoName) {
		if(promoName.length() > 40){
			promoName = promoName.substring(0, 39);
		}
		this.promoName = promoName;
	}

	public int getStartCalId() {
		return startCalId;
	}

	public void setStartCalId(int startCalId) {
		this.startCalId = startCalId;
	}

	public int getEndCalId() {
		return endCalId;
	}

	public void setEndCalId(int endCalId) {
		this.endCalId = endCalId;
	}

	public int getStatus() {
		return status;
	}

	public void setStatus(int status) {
		this.status = status;
	}

	public String getCreatedBy() {
		return createdBy;
	}

	public void setCreatedBy(String createdBy) {
		this.createdBy = createdBy;
	}

	public String getModifiedBy() {
		return modifiedBy;
	}

	public void setModifiedBy(String modifiedBy) {
		this.modifiedBy = modifiedBy;
	}

	public String getApprovedBy() {
		return approvedBy;
	}

	public void setApprovedBy(String approvedBy) {
		this.approvedBy = approvedBy;
	}

	public List<PromoBuyItems> getBuyItems() {
		return buyItems;
	}

	public void setBuyItems(List<PromoBuyItems> buyItems) {
		this.buyItems = buyItems;
	}
	
	public void addBuyItems(PromoBuyItems promoBuyItem){
		this.buyItems.add(promoBuyItem);
	}
	
	public String getWeekStartDate() {
		return weekStartDate;
	}

	public void setWeekStartDate(String weekStartDate) {
		this.weekStartDate = weekStartDate;
	}
	
	public Date getPromoStartDate() {
		return promoStartDate;
	}

	public void setPromoStartDate(Date promoStartDate) {
		this.promoStartDate = promoStartDate;
	}

	public Date getPromoEndDate() {
		return promoEndDate;
	}

	public void setPromoEndDate(Date promoEndDate) {
		this.promoEndDate = promoEndDate;
	}
	
	public String getAddtlDetails() {
		return addtlDetails;
	}

	public void setAddtlDetails(String addtlDetails) {
		this.addtlDetails = addtlDetails;
	}
	
	public List<PromoLocation> getPromoLocation() {
		return promoLocation;
	}

	public void setPromoLocation(List<PromoLocation> promoLocation) {
		this.promoLocation = promoLocation;
	}
	
	public void addPromoLocation(PromoLocation promoLocation) {
		this.promoLocation.add(promoLocation);
	}

	public List<PromoBuyRequirement> getPromoBuyRequirement() {
		return promoBuyRequirement;
	}

	public void setPromoBuyRequirement(List<PromoBuyRequirement> promoBuyRequirement) {
		this.promoBuyRequirement = promoBuyRequirement;
	}
	public void addPromoBuyRequirement(PromoBuyRequirement buyRequirement){
		this.promoBuyRequirement.add(buyRequirement);
	}
	public int getNoOfItems() {
		return noOfItems;
	}

	public void setNoOfItems(int noOfItems) {
		this.noOfItems = noOfItems;
	}

	public String getRetailForPurchase() {
		return retailForPurchase;
	}

	public void setRetailForPurchase(String retailForPurchase) {
		this.retailForPurchase = retailForPurchase;
	}

	public String getAddtlQtyRetail() {
		return addtlQtyRetail;
	}

	public void setAddtlQtyRetail(String addtlQtyRetail) {
		this.addtlQtyRetail = addtlQtyRetail;
	}

	public String getTier1Req() {
		return tier1Req;
	}

	public void setTier1Req(String tier1Req) {
		this.tier1Req = tier1Req;
	}

	public String getTier1Amt() {
		return tier1Amt;
	}

	public void setTier1Amt(String tier1Amt) {
		this.tier1Amt = tier1Amt;
	}

	public String getTier2Req() {
		return tier2Req;
	}

	public void setTier2Req(String tier2Req) {
		this.tier2Req = tier2Req;
	}

	public String getTier2Amt() {
		return tier2Amt;
	}

	public void setTier2Amt(String tier2Amt) {
		this.tier2Amt = tier2Amt;
	}

	public boolean isCanBeAdded() {
		return canBeAdded;
	}

	public void setCanBeAdded(boolean canBeAdded) {
		this.canBeAdded = canBeAdded;
	}
	
	public String getCouponDesc() {
		return couponDesc;
	}

	public void setCouponDesc(String couponDesc) {
		this.couponDesc = couponDesc;
	}

	public String getAmountOff() {
		return amountOff;
	}

	public void setAmountOff(String amountOff) {
		this.amountOff = amountOff;
	}

	public void setParticipatingItems(List<String> UPC){
		this.participatingItems = UPC;
	}
	
	public List<String> getParticipatingItems(){
		return participatingItems;
	}
	
	public String getManuCpn() {
		return manuCpn;
	}

	public void setManuCpn(String manuCpn) {
		this.manuCpn = manuCpn;
	}
	
	
	/*public void copy(PromoDefinition promoDefinition){
		this.promoDefnId = promoDefinition.getPromoDefnId();
		this.promoTypeId = promoDefinition.getPromoTypeId();
		this.promoName = promoDefinition.getPromoName();
		this.startCalId = promoDefinition.getStartCalId();
		this.endCalId = promoDefinition.getEndCalId();
		this.status = promoDefinition.getStatus();
		this.createdBy = promoDefinition.getCreatedBy();
		this.modifiedBy = promoDefinition.getModifiedBy();
		this.approvedBy = promoDefinition.getApprovedBy();
		this.weekStartDate = promoDefinition.getWeekStartDate();
		this.pomoNum = promoDefinition.getPromoNumber();
		this.themeCode = promoDefinition.getThemeCode();
		this.promoLocation = promoDefinition.getPromoLocation();
		this.promoBuyRequirement = promoDefinition.getPromoBuyRequirement();
		this.buyItems = promoDefinition.getBuyItems();
		this.participatingItems = promoDefinition.getParticipatingItems();
		this.promoStartDate = promoDefinition.getPromoStartDate();
		this.promoEndDate = promoDefinition.getPromoEndDate();
		this.addtlDetails = promoDefinition.getAddtlDetails();
		this.adPage = promoDefinition.getAdpage();
		this.blockNum = promoDefinition.getBlockNum();
		this.adDesc = promoDefinition.getAddesc();
		this.addDetail = promoDefinition.getAddDetail();
		this.isInAd = promoDefinition.getIsinAd();
		this.manuCpn = promoDefinition.getManuCpn();
		// Added for Must Buy Promotion
		this.noOfItems = promoDefinition.getNoOfItems();
		this.retailForPurchase = promoDefinition.getRetailForPurchase();
		this.addtlQtyRetail = promoDefinition.getAddtlQtyRetail();
		
		// Added for Catalina Promotions
		this.tier1Req = promoDefinition.getTier1Req();
		this.tier1Amt = promoDefinition.getTier1Amt();
		this.tier2Req = promoDefinition.getTier2Req();
		this.tier2Amt = promoDefinition.getTier2Amt();
		
		// Added for Super Coupon and e-Bonus Coupon
		this.couponDesc = promoDefinition.getCouponDesc();
		this.amountOff = promoDefinition.getAmountOff();
		
		this.UPC = promoDefinition.getUPC();

		// Added for BOGO
		this.buyQty = promoDefinition.getBuyQuantity();
		this.getQty = promoDefinition.getGetQuantity();
		this.canBeAdded = promoDefinition.isCanBeAdded();
		this.totalItems = promoDefinition.getTotalItems();
		this.adjustedUnits = promoDefinition.getAdjustedUnits();
		this.retLirId = promoDefinition.getRetLirId();
		this.retailerItemCode = promoDefinition.getRetailerItemCode();
	}*/

	@Override
    public Object clone() throws CloneNotSupportedException {
		PromoDefinition cloned = (PromoDefinition)super.clone();
		if (cloned.getPromoLocation() != null) {
			List<PromoLocation> clonedList = new ArrayList<PromoLocation>();
			for (PromoLocation promoLocation : cloned.getPromoLocation()) {
				clonedList.add((PromoLocation) promoLocation.clone());
			}
			cloned.setPromoLocation(clonedList);
		}
		
		if (cloned.getPromoBuyRequirement() != null) {
			List<PromoBuyRequirement> clonedList = new ArrayList<PromoBuyRequirement>();
			for (PromoBuyRequirement promoBuyRequirement : cloned.getPromoBuyRequirement()) {
				clonedList.add((PromoBuyRequirement) promoBuyRequirement.clone());
			}
			cloned.setPromoBuyRequirement(clonedList);
		}
		
		if (cloned.getBuyItems() != null) {
			List<PromoBuyItems> clonedList = new ArrayList<PromoBuyItems>();
			for (PromoBuyItems promoBuyItems : cloned.getBuyItems()) {
				clonedList.add((PromoBuyItems) promoBuyItems.clone());
			}
			cloned.setBuyItems(clonedList);
		}
		
        return cloned;
    }
	public int getPromoDefnTypeId() {
		return promoDefnTypeId;
	}
	public void setPromoDefnTypeId(int promoDefnTypeId) {
		this.promoDefnTypeId = promoDefnTypeId;
	}
	public Date getSuperCouponEndDate() {
		return superCouponEndDate;
	}
	public void setSuperCouponEndDate(Date superCouponEndDate) {
		this.superCouponEndDate = superCouponEndDate;
	}
	public Date getSuperCouponStartDate() {
		return superCouponStartDate;
	}
	public void setSuperCouponStartDate(Date superCouponStartDate) {
		this.superCouponStartDate = superCouponStartDate;
	}
	public int getSuperCouponStartCalID() {
		return superCouponStartCalID;
	}
	public void setSuperCouponStartCalID(int superCouponStartCalID) {
		this.superCouponStartCalID = superCouponStartCalID;
	}
	public int getSuperCouponEndCalId() {
		return superCouponEndCalId;
	}
	public void setSuperCouponEndCalId(int superCouponEndCalId) {
		this.superCouponEndCalId = superCouponEndCalId;
	}
	public int getRegQty() {
		return regQty;
	}
	public void setRegQty(int regQty) {
		this.regQty = regQty;
	}
	public double getRegPrice() {
		return regPrice;
	}
	public void setRegPrice(double regPrice) {
		this.regPrice = regPrice;
	}
	public int getSaleQty() {
		return saleQty;
	}
	public void setSaleQty(int saleQty) {
		this.saleQty = saleQty;
	}
	public double getSalePrice() {
		return salePrice;
	}
	public void setSalePrice(double salePrice) {
		this.salePrice = salePrice;
	}
	public double getPctOff() {
		return pctOff;
	}
	public void setPctOff(double pctOffOnSeconItem) {
		this.pctOff = pctOffOnSeconItem;
	}
	public String getItemPriceCode() {
		return itemPriceCode;
	}
	public void setItemPriceCode(String itemPriceCode) {
		this.itemPriceCode = itemPriceCode;
	}
	public double getThresholdValue() {
		return thresholdValue;
	}
	public void setThresholdValue(double thresholdValue) {
		this.thresholdValue = thresholdValue;
	}
	public String getThresholdType() {
		return thresholdType;
	}
	public void setThresholdType(String thresholdType) {
		this.thresholdType = thresholdType;
	}
	public int getPromoSubTypeId() {
		return promoSubTypeId;
	}
	public void setPromoSubTypeId(int promoSubTypeId) {
		this.promoSubTypeId = promoSubTypeId;
	}
	@Override
	public int hashCode() {
		final int prime = 31;
		int result = 1;
		result = prime * result + ((UPC == null) ? 0 : UPC.hashCode());
		result = prime * result + ((adDesc == null) ? 0 : adDesc.hashCode());
		result = prime * result + ((adPage == null) ? 0 : adPage.hashCode());
		result = prime * result + ((addDetail == null) ? 0 : addDetail.hashCode());
		result = prime * result + ((addtlDetails == null) ? 0 : addtlDetails.hashCode());
		result = prime * result + ((addtlQtyRetail == null) ? 0 : addtlQtyRetail.hashCode());
		result = prime * result + (int) (adjustedUnits ^ (adjustedUnits >>> 32));
		result = prime * result + ((amountOff == null) ? 0 : amountOff.hashCode());
		result = prime * result + ((approvedBy == null) ? 0 : approvedBy.hashCode());
		result = prime * result + ((blockNum == null) ? 0 : blockNum.hashCode());
		result = prime * result + ((buyItems == null) ? 0 : buyItems.hashCode());
		result = prime * result + buyQty;
		result = prime * result + (canBeAdded ? 1231 : 1237);
		result = prime * result + ((couponDesc == null) ? 0 : couponDesc.hashCode());
		result = prime * result + ((createdBy == null) ? 0 : createdBy.hashCode());
		result = prime * result + endCalId;
		result = prime * result + getQty;
		result = prime * result + ((isInAd == null) ? 0 : isInAd.hashCode());
		result = prime * result + itemCode;
		result = prime * result + ((itemPriceCode == null) ? 0 : itemPriceCode.hashCode());
		result = prime * result + ((manuCpn == null) ? 0 : manuCpn.hashCode());
		result = prime * result + ((modifiedBy == null) ? 0 : modifiedBy.hashCode());
		result = prime * result + noOfItems;
		result = prime * result + ((participatingItems == null) ? 0 : participatingItems.hashCode());
		long temp;
		temp = Double.doubleToLongBits(pctOff);
		result = prime * result + (int) (temp ^ (temp >>> 32));
		result = prime * result + ((pomoNum == null) ? 0 : pomoNum.hashCode());
		result = prime * result + ((promoBuyRequirement == null) ? 0 : promoBuyRequirement.hashCode());
		result = prime * result + (int) (promoDefnId ^ (promoDefnId >>> 32));
		result = prime * result + promoDefnTypeId;
		result = prime * result + ((promoEndDate == null) ? 0 : promoEndDate.hashCode());
		result = prime * result + ((promoLocation == null) ? 0 : promoLocation.hashCode());
		result = prime * result + ((promoName == null) ? 0 : promoName.hashCode());
		result = prime * result + ((promoStartDate == null) ? 0 : promoStartDate.hashCode());
		result = prime * result + promoSubTypeId;
		result = prime * result + promoTypeId;
		temp = Double.doubleToLongBits(regPrice);
		result = prime * result + (int) (temp ^ (temp >>> 32));
		result = prime * result + regQty;
		result = prime * result + retLirId;
		result = prime * result + ((retailForPurchase == null) ? 0 : retailForPurchase.hashCode());
		result = prime * result + ((retailerItemCode == null) ? 0 : retailerItemCode.hashCode());
		temp = Double.doubleToLongBits(salePrice);
		result = prime * result + (int) (temp ^ (temp >>> 32));
		result = prime * result + saleQty;
		result = prime * result + startCalId;
		result = prime * result + status;
		result = prime * result + superCouponEndCalId;
		result = prime * result + ((superCouponEndDate == null) ? 0 : superCouponEndDate.hashCode());
		result = prime * result + superCouponStartCalID;
		result = prime * result + ((superCouponStartDate == null) ? 0 : superCouponStartDate.hashCode());
		result = prime * result + ((themeCode == null) ? 0 : themeCode.hashCode());
		result = prime * result + ((thresholdType == null) ? 0 : thresholdType.hashCode());
		temp = Double.doubleToLongBits(thresholdValue);
		result = prime * result + (int) (temp ^ (temp >>> 32));
		result = prime * result + ((tier1Amt == null) ? 0 : tier1Amt.hashCode());
		result = prime * result + ((tier1Req == null) ? 0 : tier1Req.hashCode());
		result = prime * result + ((tier2Amt == null) ? 0 : tier2Amt.hashCode());
		result = prime * result + ((tier2Req == null) ? 0 : tier2Req.hashCode());
		result = prime * result + totalItems;
		result = prime * result + ((weekStartDate == null) ? 0 : weekStartDate.hashCode());
		return result;
	}
	@Override
	public boolean equals(Object obj) {
		if (this == obj)
			return true;
		if (obj == null)
			return false;
		if (getClass() != obj.getClass())
			return false;
		PromoDefinition other = (PromoDefinition) obj;
		if (UPC == null) {
			if (other.UPC != null)
				return false;
		} else if (!UPC.equals(other.UPC))
			return false;
		if (adDesc == null) {
			if (other.adDesc != null)
				return false;
		} else if (!adDesc.equals(other.adDesc))
			return false;
		if (adPage == null) {
			if (other.adPage != null)
				return false;
		} else if (!adPage.equals(other.adPage))
			return false;
		if (addDetail == null) {
			if (other.addDetail != null)
				return false;
		} else if (!addDetail.equals(other.addDetail))
			return false;
		if (addtlDetails == null) {
			if (other.addtlDetails != null)
				return false;
		} else if (!addtlDetails.equals(other.addtlDetails))
			return false;
		if (addtlQtyRetail == null) {
			if (other.addtlQtyRetail != null)
				return false;
		} else if (!addtlQtyRetail.equals(other.addtlQtyRetail))
			return false;
		if (adjustedUnits != other.adjustedUnits)
			return false;
		if (amountOff == null) {
			if (other.amountOff != null)
				return false;
		} else if (!amountOff.equals(other.amountOff))
			return false;
		if (approvedBy == null) {
			if (other.approvedBy != null)
				return false;
		} else if (!approvedBy.equals(other.approvedBy))
			return false;
		if (blockNum == null) {
			if (other.blockNum != null)
				return false;
		} else if (!blockNum.equals(other.blockNum))
			return false;
		if (buyItems == null) {
			if (other.buyItems != null)
				return false;
		} else if (!buyItems.equals(other.buyItems))
			return false;
		if (buyQty != other.buyQty)
			return false;
		if (canBeAdded != other.canBeAdded)
			return false;
		if (couponDesc == null) {
			if (other.couponDesc != null)
				return false;
		} else if (!couponDesc.equals(other.couponDesc))
			return false;
		if (createdBy == null) {
			if (other.createdBy != null)
				return false;
		} else if (!createdBy.equals(other.createdBy))
			return false;
		if (endCalId != other.endCalId)
			return false;
		if (getQty != other.getQty)
			return false;
		if (isInAd == null) {
			if (other.isInAd != null)
				return false;
		} else if (!isInAd.equals(other.isInAd))
			return false;
		if (itemCode != other.itemCode)
			return false;
		if (itemPriceCode == null) {
			if (other.itemPriceCode != null)
				return false;
		} else if (!itemPriceCode.equals(other.itemPriceCode))
			return false;
		if (manuCpn == null) {
			if (other.manuCpn != null)
				return false;
		} else if (!manuCpn.equals(other.manuCpn))
			return false;
		if (modifiedBy == null) {
			if (other.modifiedBy != null)
				return false;
		} else if (!modifiedBy.equals(other.modifiedBy))
			return false;
		if (noOfItems != other.noOfItems)
			return false;
		if (participatingItems == null) {
			if (other.participatingItems != null)
				return false;
		} else if (!participatingItems.equals(other.participatingItems))
			return false;
		if (Double.doubleToLongBits(pctOff) != Double.doubleToLongBits(other.pctOff))
			return false;
		if (pomoNum == null) {
			if (other.pomoNum != null)
				return false;
		} else if (!pomoNum.equals(other.pomoNum))
			return false;
		if (promoBuyRequirement == null) {
			if (other.promoBuyRequirement != null)
				return false;
		} else if (!promoBuyRequirement.equals(other.promoBuyRequirement))
			return false;
		if (promoDefnId != other.promoDefnId)
			return false;
		if (promoDefnTypeId != other.promoDefnTypeId)
			return false;
		if (promoEndDate == null) {
			if (other.promoEndDate != null)
				return false;
		} else if (!promoEndDate.equals(other.promoEndDate))
			return false;
		if (promoLocation == null) {
			if (other.promoLocation != null)
				return false;
		} else if (!promoLocation.equals(other.promoLocation))
			return false;
		if (promoName == null) {
			if (other.promoName != null)
				return false;
		} else if (!promoName.equals(other.promoName))
			return false;
		if (promoStartDate == null) {
			if (other.promoStartDate != null)
				return false;
		} else if (!promoStartDate.equals(other.promoStartDate))
			return false;
		if (promoSubTypeId != other.promoSubTypeId)
			return false;
		if (promoTypeId != other.promoTypeId)
			return false;
		if (Double.doubleToLongBits(regPrice) != Double.doubleToLongBits(other.regPrice))
			return false;
		if (regQty != other.regQty)
			return false;
		if (retLirId != other.retLirId)
			return false;
		if (retailForPurchase == null) {
			if (other.retailForPurchase != null)
				return false;
		} else if (!retailForPurchase.equals(other.retailForPurchase))
			return false;
		if (retailerItemCode == null) {
			if (other.retailerItemCode != null)
				return false;
		} else if (!retailerItemCode.equals(other.retailerItemCode))
			return false;
		if (Double.doubleToLongBits(salePrice) != Double.doubleToLongBits(other.salePrice))
			return false;
		if (saleQty != other.saleQty)
			return false;
		if (startCalId != other.startCalId)
			return false;
		if (status != other.status)
			return false;
		if (superCouponEndCalId != other.superCouponEndCalId)
			return false;
		if (superCouponEndDate == null) {
			if (other.superCouponEndDate != null)
				return false;
		} else if (!superCouponEndDate.equals(other.superCouponEndDate))
			return false;
		if (superCouponStartCalID != other.superCouponStartCalID)
			return false;
		if (superCouponStartDate == null) {
			if (other.superCouponStartDate != null)
				return false;
		} else if (!superCouponStartDate.equals(other.superCouponStartDate))
			return false;
		if (themeCode == null) {
			if (other.themeCode != null)
				return false;
		} else if (!themeCode.equals(other.themeCode))
			return false;
		if (thresholdType == null) {
			if (other.thresholdType != null)
				return false;
		} else if (!thresholdType.equals(other.thresholdType))
			return false;
		if (Double.doubleToLongBits(thresholdValue) != Double.doubleToLongBits(other.thresholdValue))
			return false;
		if (tier1Amt == null) {
			if (other.tier1Amt != null)
				return false;
		} else if (!tier1Amt.equals(other.tier1Amt))
			return false;
		if (tier1Req == null) {
			if (other.tier1Req != null)
				return false;
		} else if (!tier1Req.equals(other.tier1Req))
			return false;
		if (tier2Amt == null) {
			if (other.tier2Amt != null)
				return false;
		} else if (!tier2Amt.equals(other.tier2Amt))
			return false;
		if (tier2Req == null) {
			if (other.tier2Req != null)
				return false;
		} else if (!tier2Req.equals(other.tier2Req))
			return false;
		if (totalItems != other.totalItems)
			return false;
		if (weekStartDate == null) {
			if (other.weekStartDate != null)
				return false;
		} else if (!weekStartDate.equals(other.weekStartDate))
			return false;
		return true;
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
	public String getEdlpPromoFlag() {
		return edlpPromoFlag;
	}
	public void setEdlpPromoFlag(String edlpPromoFlag) {
		this.edlpPromoFlag = edlpPromoFlag;
	}
	
	
	public int getMinimumQty() {
		return minimumQty;
	}

	public void setMinimumQty(int minimumQty) {
		this.minimumQty = minimumQty;
	}

	public double getMinimumAmt() {
		return minimumAmt;
	}

	public void setMinimumAmt(double minimumAmt) {
		this.minimumAmt = minimumAmt;
	}

	public double getBmsmDollaroffperunits() {
		return bmsmDollaroffperunits;
	}

	public void setBmsmDollaroffperunits(double bmsmDollaroffperunits) {
		this.bmsmDollaroffperunits = bmsmDollaroffperunits;
	}

	public double getBmsmPctoffperunit() {
		return bmsmPctoffperunit;
	}

	public void setBmsmPctoffperunit(double bmsmPctoffperunit) {
		this.bmsmPctoffperunit = bmsmPctoffperunit;
	}

	public int getBmsmsaleQty() {
		return bmsmsaleQty;
	}

	public void setBmsmsaleQty(int bmsmsaleQty) {
		this.bmsmsaleQty = bmsmsaleQty;
	}

	public double getBmsmsalePrice() {
		return bmsmsalePrice;
	}

	public void setBmsmsalePrice(double bmsmsalePrice) {
		this.bmsmsalePrice = bmsmsalePrice;
	}

	public double getDollarOff() {
		return dollarOff;
	}

	public void setDollarOff(double dollarOff) {
		this.dollarOff = dollarOff;
	}
	public String getPromoGroup() {
		return promoGroup;
	}
	public void setPromoGroup(String promoGroup) {
		this.promoGroup = promoGroup;
	}
	
	public LocationKey getLocationKey() {
		return new LocationKey(locationLevelId, locationId);
	}
	
}
