package com.pristine.dto;


public class MarketDataDTO implements Cloneable{
	
	private String dateReceived;
	
	private String upc;
	private int itemCode;
	private String LOB;
	private String group;
	private String subGroup;
	private String productDescription;
	private String shortProductDescription;
	private String orgSize;
	private String Size;
	private String UOMId;
	private String brandLow;
	private String marketDisplayName;
	private String marketDisplayNameDesc;
	private String brandHigh;
	private String brandOwnLow;
	private String brandOwnHigh;
	private String packageShape;
	private String commonName;
	private String periodDesc;
	private String periodEndDate;
	private double sales;
	private double units;
	private double nonPromoSales;
	private double nonPromoUnits;
	private double pctACV;
	private double perMMACV;
	private double ourTASales;
	private double ourTAUnits;
	private double ourTANonPromoSales;
	private double ourTANonPromoUnits;
	private double ourTAPctACV;
	private double ourTAPerMMACV;
	private double remTASales;
	private double remTAUnits;
	private double remTANonPromoSales;
	private double remTANonPromoUnits;
	private double remTAPctACV;
	private double remTAPerMMACV;
	private double totRemTASales;
	private double totRemTAUnits;
	private double totRemTANonPromoSales;
	private double totRemTANonPromoUnits;
	private double totRemTAPctACV;
	private double totRemTAPerMMACV;
	private String lyPeriodEndDate;
	private double lyOurTASales;
	private double lyOurTAUnits;
	private double lyOurTANonPromoSales;
	private double lyOurTANonPromoUnits;
	private double lyOurTAPctACV;
	private double lyOurTAPerMMACV;
	private double lyRemTASales;
	private double lyRemTAUnits;
	private double lyRemTANonPromoSales;
	private double lyRemTANonPromoUnits;
	private double lyRemTAPctACV;
	private double lyRemTAPerMMACV;
	private double lyTotRemTASales;
	private double lyTotRemTAUnits;
	private double lyTotRemTANonPromoSales;
	private double lyTotRemTANonPromoUnits;
	private double lyTotRemTAPctACV;
	private double lyTotRemTAPerMMACV;
	private int brandId;
	private String commodityGroup;
	private String productSize;
	private String packSize;
	private String totalSize;
	private String form;
	private String usdaOraganicSeal;
	private String flavour;
	private String bonusPack;
	private String baseFlavour;

	public String getDateReceived() {
		return dateReceived;
	}
	public void setDateReceived(String dateReceived) {
		this.dateReceived = dateReceived;
	}
	public String getLOB() {
		return LOB;
	}
	public void setLOB(String lOB) {
		LOB = lOB;
	}
	public String getGroup() {
		return group;
	}
	public void setGroup(String group) {
		this.group = group;
	}
	public String getSubGroup() {
		return subGroup;
	}
	public void setSubGroup(String subGroup) {
		this.subGroup = subGroup;
	}
	public String getProductDescription() {
		return productDescription;
	}
	public void setProductDescription(String productDescription) {
		this.productDescription = productDescription;
	}
	public String getOrgSize() {
		return orgSize;
	}
	public void setOrgSize(String orgSize) {
		this.orgSize = orgSize;
	}
	public String getSize() {
		return Size;
	}
	public void setSize(String size) {
		Size = size;
	}
	public String getBrandHigh() {
		return brandHigh;
	}
	public void setBrandHigh(String brandHigh) {
		this.brandHigh = brandHigh;
	}
	public String getBrandOwnLow() {
		return brandOwnLow;
	}
	public void setBrandOwnLow(String brandOwnLow) {
		this.brandOwnLow = brandOwnLow;
	}
	public String getBrandOwnHigh() {
		return brandOwnHigh;
	}
	public void setBrandOwnHigh(String brandOwnHigh) {
		this.brandOwnHigh = brandOwnHigh;
	}
	public String getPackageShape() {
		return packageShape;
	}
	public void setPackageShape(String packageShape) {
		this.packageShape = packageShape;
	}
	public String getCommonName() {
		return commonName;
	}
	public void setCommonName(String commonName) {
		this.commonName = commonName;
	}
	public String getPeriodEndDate() {
		return periodEndDate;
	}
	public void setPeriodEndDate(String periodEndDate) {
		this.periodEndDate = periodEndDate;
	}
	public double getOurTASales() {
		return ourTASales;
	}
	public void setOurTASales(double ourTASales) {
		this.ourTASales = ourTASales;
	}
	public double getOurTAUnits() {
		return ourTAUnits;
	}
	public void setOurTAUnits(double ourTAUnits) {
		this.ourTAUnits = ourTAUnits;
	}
	public double getOurTAPctACV() {
		return ourTAPctACV;
	}
	public void setOurTAPctACV(double ourTAPctACV) {
		this.ourTAPctACV = ourTAPctACV;
	}
	public double getOurTAPerMMACV() {
		return ourTAPerMMACV;
	}
	public void setOurTAPerMMACV(double ourTAPerMMACV) {
		this.ourTAPerMMACV = ourTAPerMMACV;
	}
	public double getRemTASales() {
		return remTASales;
	}
	public void setRemTASales(double remTASales) {
		this.remTASales = remTASales;
	}
	public double getRemTAUnits() {
		return remTAUnits;
	}
	public void setRemTAUnits(double remTAUnits) {
		this.remTAUnits = remTAUnits;
	}
	public double getRemTAPctACV() {
		return remTAPctACV;
	}
	public void setRemTAPctACV(double remTAPctACV) {
		this.remTAPctACV = remTAPctACV;
	}
	public double getRemTAPerMMACV() {
		return remTAPerMMACV;
	}
	public void setRemTAPerMMACV(double remTAPerMMACV) {
		this.remTAPerMMACV = remTAPerMMACV;
	}
	public double getTotRemTASales() {
		return totRemTASales;
	}
	public void setTotRemTASales(double totRemTASales) {
		this.totRemTASales = totRemTASales;
	}
	public double getTotRemTAUnits() {
		return totRemTAUnits;
	}
	public void setTotRemTAUnits(double totRemTAUnits) {
		this.totRemTAUnits = totRemTAUnits;
	}
	public double getTotRemTAPctACV() {
		return totRemTAPctACV;
	}
	public void setTotRemTAPctACV(double totRemTAPctACV) {
		this.totRemTAPctACV = totRemTAPctACV;
	}
	public double getTotRemTAPerMMACV() {
		return totRemTAPerMMACV;
	}
	public void setTotRemTAPerMMACV(double totRemTAPerMMACV) {
		this.totRemTAPerMMACV = totRemTAPerMMACV;
	}
	public String getLyPeriodEndDate() {
		return lyPeriodEndDate;
	}
	public void setLyPeriodEndDate(String string) {
		this.lyPeriodEndDate = string;
	}
	public double getLyOurTASales() {
		return lyOurTASales;
	}
	public void setLyOurTASales(double lyOurTASales) {
		this.lyOurTASales = lyOurTASales;
	}
	public double getLyOurTAUnits() {
		return lyOurTAUnits;
	}
	public void setLyOurTAUnits(double lyOurTAUnits) {
		this.lyOurTAUnits = lyOurTAUnits;
	}
	public double getLyOurTAPctACV() {
		return lyOurTAPctACV;
	}
	public void setLyOurTAPctACV(double lyOurTAPctACV) {
		this.lyOurTAPctACV = lyOurTAPctACV;
	}
	public double getLyOurTAPerMMACV() {
		return lyOurTAPerMMACV;
	}
	public void setLyOurTAPerMMACV(double lyOurTAPerMMACV) {
		this.lyOurTAPerMMACV = lyOurTAPerMMACV;
	}
	public double getLyRemTASales() {
		return lyRemTASales;
	}
	public void setLyRemTASales(double lyRemTASales) {
		this.lyRemTASales = lyRemTASales;
	}
	public double getLyRemTAUnits() {
		return lyRemTAUnits;
	}
	public void setLyRemTAUnits(double lyRemTAUnits) {
		this.lyRemTAUnits = lyRemTAUnits;
	}
	public double getLyRemTAPctACV() {
		return lyRemTAPctACV;
	}
	public void setLyRemTAPctACV(double lyRemTAPctACV) {
		this.lyRemTAPctACV = lyRemTAPctACV;
	}
	public double getLyRemTAPerMMACV() {
		return lyRemTAPerMMACV;
	}
	public void setLyRemTAPerMMACV(double lyRemTAPerMMACV) {
		this.lyRemTAPerMMACV = lyRemTAPerMMACV;
	}
	public double getLyTotRemTASales() {
		return lyTotRemTASales;
	}
	public void setLyTotRemTASales(double lyTotRemTASales) {
		this.lyTotRemTASales = lyTotRemTASales;
	}
	public double getLyTotRemTAUnits() {
		return lyTotRemTAUnits;
	}
	public void setLyTotRemTAUnits(double lyTotRemTAUnits) {
		this.lyTotRemTAUnits = lyTotRemTAUnits;
	}
	public double getLyTotRemTAPctACV() {
		return lyTotRemTAPctACV;
	}
	public void setLyTotRemTAPctACV(double lyTotRemTAPctACV) {
		this.lyTotRemTAPctACV = lyTotRemTAPctACV;
	}
	public double getLyTotRemTAPerMMACV() {
		return lyTotRemTAPerMMACV;
	}
	public void setLyTotRemTAPerMMACV(double lyTotRemTAPerMMACV) {
		this.lyTotRemTAPerMMACV = lyTotRemTAPerMMACV;
	}
	public int getBrandId() {
		return brandId;
	}
	public void setBrandId(int brandId) {
		this.brandId = brandId;
	}
	public String getBrandLow() {
		return brandLow;
	}
	public void setBrandLow(String brandLow) {
		this.brandLow = brandLow;
	}
	public String getMarketDisplayName() {
		return marketDisplayName;
	}
	public void setMarketDisplayName(String marketDisplayName) {
		this.marketDisplayName = marketDisplayName;
	}
	public String getPeriodDesc() {
		return periodDesc;
	}
	public void setPeriodDesc(String periodDesc) {
		this.periodDesc = periodDesc;
	}
	public double getSales() {
		return sales;
	}
	public void setSales(double sales) {
		this.sales = sales;
	}
	public double getUnits() {
		return units;
	}
	public void setUnits(double units) {
		this.units = units;
	}
	public double getPctACV() {
		return pctACV;
	}
	public void setPctACV(double pctACV) {
		this.pctACV = pctACV;
	}
	public double getPerMMACV() {
		return perMMACV;
	}
	public void setPerMMACV(double perMMACV) {
		this.perMMACV = perMMACV;
	}
	public String getUpc() {
		return upc;
	}
	public void setUpc(String upc) {
		this.upc = upc;
	}
	public String getUOMId() {
		return UOMId;
	}
	public void setUOMId(String uOMId) {
		UOMId = uOMId;
	}
	public int getItemCode() {
		return itemCode;
	}
	public void setItemCode(int itemCode) {
		this.itemCode = itemCode;
	}
	public double getOurTANonPromoSales() {
		return ourTANonPromoSales;
	}
	public void setOurTANonPromoSales(double ourTANonPromoSales) {
		this.ourTANonPromoSales = ourTANonPromoSales;
	}
	public double getOurTANonPromoUnits() {
		return ourTANonPromoUnits;
	}
	public void setOurTANonPromoUnits(double ourTANonPromoUnits) {
		this.ourTANonPromoUnits = ourTANonPromoUnits;
	}
	public double getRemTANonPromoSales() {
		return remTANonPromoSales;
	}
	public void setRemTANonPromoSales(double remTANonPromoSales) {
		this.remTANonPromoSales = remTANonPromoSales;
	}
	public double getRemTANonPromoUnits() {
		return remTANonPromoUnits;
	}
	public void setRemTANonPromoUnits(double remTANonPromoUnits) {
		this.remTANonPromoUnits = remTANonPromoUnits;
	}
	public double getTotRemTANonPromoSales() {
		return totRemTANonPromoSales;
	}
	public void setTotRemTANonPromoSales(double totRemTANonPromoSales) {
		this.totRemTANonPromoSales = totRemTANonPromoSales;
	}
	public double getTotRemTANonPromoUnits() {
		return totRemTANonPromoUnits;
	}
	public void setTotRemTANonPromoUnits(double totRemTANonPromoUnits) {
		this.totRemTANonPromoUnits = totRemTANonPromoUnits;
	}
	public double getLyOurTANonPromoSales() {
		return lyOurTANonPromoSales;
	}
	public void setLyOurTANonPromoSales(double lyOurTANonPromoSales) {
		this.lyOurTANonPromoSales = lyOurTANonPromoSales;
	}
	public double getLyOurTANonPromoUnits() {
		return lyOurTANonPromoUnits;
	}
	public void setLyOurTANonPromoUnits(double lyOurTANonPromoUnits) {
		this.lyOurTANonPromoUnits = lyOurTANonPromoUnits;
	}
	public double getLyRemTANonPromoSales() {
		return lyRemTANonPromoSales;
	}
	public void setLyRemTANonPromoSales(double lyRemTANonPromoSales) {
		this.lyRemTANonPromoSales = lyRemTANonPromoSales;
	}
	public double getLyRemTANonPromoUnits() {
		return lyRemTANonPromoUnits;
	}
	public void setLyRemTANonPromoUnits(double lyRemTANonPromoUnits) {
		this.lyRemTANonPromoUnits = lyRemTANonPromoUnits;
	}
	public double getLyTotRemTANonPromoSales() {
		return lyTotRemTANonPromoSales;
	}
	public void setLyTotRemTANonPromoSales(double lyTotRemTANonPromoSales) {
		this.lyTotRemTANonPromoSales = lyTotRemTANonPromoSales;
	}
	public double getLyTotRemTANonPromoUnits() {
		return lyTotRemTANonPromoUnits;
	}
	public void setLyTotRemTANonPromoUnits(double lyTotRemTANonPromoUnits) {
		this.lyTotRemTANonPromoUnits = lyTotRemTANonPromoUnits;
	}
	public double getNonPromoSales() {
		return nonPromoSales;
	}
	public void setNonPromoSales(double nonPromoSales) {
		this.nonPromoSales = nonPromoSales;
	}
	public double getNonPromoUnits() {
		return nonPromoUnits;
	}
	public void setNonPromoUnits(double nonPromoUnits) {
		this.nonPromoUnits = nonPromoUnits;
	}
	@Override
    public Object clone() throws CloneNotSupportedException {
		MarketDataDTO cloned = (MarketDataDTO)super.clone();
		return cloned;
	}
	public String getMarketDisplayNameDesc() {
		return marketDisplayNameDesc;
	}
	public void setMarketDisplayNameDesc(String marketDisplayNameDesc) {
		this.marketDisplayNameDesc = marketDisplayNameDesc;
	}
	public String getShortProductDescription() {
		return shortProductDescription;
	}
	public void setShortProductDescription(String shortProductDescription) {
		this.shortProductDescription = shortProductDescription;
	}
	public String getCommodityGroup() {
		return commodityGroup;
	}
	public void setCommodityGroup(String commodityGroup) {
		this.commodityGroup = commodityGroup;
	}
	public String getPackSize() {
		return packSize;
	}
	public void setPackSize(String packSize) {
		this.packSize = packSize;
	}
	public String getProductSize() {
		return productSize;
	}
	public void setProductSize(String productSize) {
		this.productSize = productSize;
	}
	public String getTotalSize() {
		return totalSize;
	}
	public void setTotalSize(String totalSize) {
		this.totalSize = totalSize;
	}
	public String getForm() {
		return form;
	}
	public void setForm(String form) {
		this.form = form;
	}
	public String getUsdaOraganicSeal() {
		return usdaOraganicSeal;
	}
	public void setUsdaOraganicSeal(String usdaOraganicSeal) {
		this.usdaOraganicSeal = usdaOraganicSeal;
	}
	public String getFlavour() {
		return flavour;
	}
	public void setFlavour(String flavour) {
		this.flavour = flavour;
	}
	public String getBonusPack() {
		return bonusPack;
	}
	public void setBonusPack(String bonusPack) {
		this.bonusPack = bonusPack;
	}
	public String getBaseFlavour() {
		return baseFlavour;
	}
	public void setBaseFlavour(String baseFlavour) {
		this.baseFlavour = baseFlavour;
	}
}
