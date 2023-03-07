package com.pristine.dto;

public class NelsonMarketDataDTO implements Cloneable {

	private String marketName;
	private String timePeriod;
	private long marketDataId;
	private int locationlevelID;
	private String locationLevel;
	private int ZoneId;
	private String locationNo;
	private int itemCode;
	private String UPC;
	private String UPCDesc;
	private String itemType;
	private String startDate;
	private String endDate;
	private int calendarId;
	private String calendarType;
	private String retailCondn;
	private int actUnitsProj;
	private int actUnitsProjCompMkt;
	private int actSalesProj;
	private int actsalesProjCompMkt;
	private double avgPrc;
	private double avglPrcCompMkt;
	private double elasticityEstFcsMKt;
	private double elasticityEstTotMkt;
	private double nf2FcsMkt;
	private double nf3FcsMkt;
	private double nf4FcsMkt;
	private double estFcsDisMkt;
	private double estAdFcsMkt;
	private double estDisAdFcsMkt;
	private double mcpCompMkt;
	private double unitPerMcpCompMkt;
	private double fivePercentilePrc;
	private double tenPercentilePrc;
	private double fifteenPercentilePrc;
	private double twentyPercentilePrc;
	private double twentyFivePercentilePrc;
	private double thirtyPercentilePrc;
	private double thirtyFivePercentilePrc;
	private double fourtyPercentilePrc;
	private double fourtyFivePercentilePrc;
	private double fiftyPercentilePrc;
	private double fiftyfivePercentilePrc;
	private double sixtyPercentilePrc;
	private double sixtyFivePercentilePrc;
	private double seventyPercentilePrc;
	private double seventyFivePercentilePrc;
	private double eightyPercentilePrc;
	private double eightyFivePercentilePrc;
	private double ninetyPercentilePrc;
	private double ninetyFivePercentilePrc;
	private String iscarriedItem;

	
	
	
	public int getZoneId() {
		return ZoneId;
	}

	public void setZoneId(int zoneId) {
		ZoneId = zoneId;
	}

	public String getIscarriedItem() {
		return iscarriedItem;
	}

	public void setIscarriedItem(String iscarriedItem) {
		this.iscarriedItem = iscarriedItem;
	}

	public String getTimePeriod() {
		return timePeriod;
	}

	public void setTimePeriod(String timePeriod) {
		this.timePeriod = timePeriod;
	}

	public String getMarketName() {
		return marketName;
	}

	public void setMarketName(String marketName) {
		this.marketName = marketName;
	}

	public long getMarketDataId() {
		return marketDataId;
	}

	public void setMarketDataId(long marketDataId) {
		this.marketDataId = marketDataId;
	}

	public int getLocationlevelID() {
		return locationlevelID;
	}

	public void setLocationlevelID(int locationlevelID) {
		this.locationlevelID = locationlevelID;
	}

	public String getLocationLevel() {
		return locationLevel;
	}

	public void setLocationLevel(String locationLevel) {
		this.locationLevel = locationLevel;
	}

	public String getLocationNo() {
		return locationNo;
	}

	public void setLocationNo(String locationNo) {
		this.locationNo = locationNo;
	}

	public int getItemCode() {
		return itemCode;
	}

	public void setItemCode(int itemCode) {
		this.itemCode = itemCode;
	}

	public String getUPC() {
		return UPC;
	}

	public void setUPC(String uPC) {
		UPC = uPC;
	}

	public String getUPCDesc() {
		return UPCDesc;
	}

	public void setUPCDesc(String uPCDesc) {
		UPCDesc = uPCDesc;
	}

	public String getItemType() {
		return itemType;
	}

	public void setItemType(String itemType) {
		this.itemType = itemType;
	}

	public String getStartDate() {
		return startDate;
	}

	public void setStartDate(String startDate) {
		this.startDate = startDate;
	}

	public String getEndDate() {
		return endDate;
	}

	public void setEndDate(String endDate) {
		this.endDate = endDate;
	}

	public int getCalendarId() {
		return calendarId;
	}

	public void setCalendarId(int calendarId) {
		this.calendarId = calendarId;
	}

	public String getCalendarType() {
		return calendarType;
	}

	public void setCalendarType(String calendarType) {
		this.calendarType = calendarType;
	}

	public String getRetailCondn() {
		return retailCondn;
	}

	public void setRetailCondn(String retailCondn) {
		this.retailCondn = retailCondn;
	}

	public int getActUnitsProj() {
		return actUnitsProj;
	}

	public void setActUnitsProj(int actUnitsProj) {
		this.actUnitsProj = actUnitsProj;
	}

	public int getActUnitsProjCompMkt() {
		return actUnitsProjCompMkt;
	}

	public void setActUnitsProjCompMkt(int actUnitsProjCompMkt) {
		this.actUnitsProjCompMkt = actUnitsProjCompMkt;
	}

	public int getActSalesProj() {
		return actSalesProj;
	}

	public void setActSalesProj(int actSalesProj) {
		this.actSalesProj = actSalesProj;
	}

	public int getActsalesProjCompMkt() {
		return actsalesProjCompMkt;
	}

	public void setActsalesProjCompMkt(int actsalesProjCompMkt) {
		this.actsalesProjCompMkt = actsalesProjCompMkt;
	}

	public double getAvgPrc() {
		return avgPrc;
	}

	public void setAvgPrc(double avgPrc) {
		this.avgPrc = avgPrc;
	}

	public double getAvglPrcCompMkt() {
		return avglPrcCompMkt;
	}

	public void setAvglPrcCompMkt(double avglPrcCompMkt) {
		this.avglPrcCompMkt = avglPrcCompMkt;
	}

	public double getElasticityEstFcsMKt() {
		return elasticityEstFcsMKt;
	}

	public void setElasticityEstFcsMKt(double elasticityEstFcsMKt) {
		this.elasticityEstFcsMKt = elasticityEstFcsMKt;
	}

	public double getElasticityEstTotMkt() {
		return elasticityEstTotMkt;
	}

	public void setElasticityEstTotMkt(double elasticityEstTotMkt) {
		this.elasticityEstTotMkt = elasticityEstTotMkt;
	}

	public double getNf2FcsMkt() {
		return nf2FcsMkt;
	}

	public void setNf2FcsMkt(double nf2FcsMkt) {
		this.nf2FcsMkt = nf2FcsMkt;
	}

	public double getNf3FcsMkt() {
		return nf3FcsMkt;
	}

	public void setNf3FcsMkt(double nf3FcsMkt) {
		this.nf3FcsMkt = nf3FcsMkt;
	}

	public double getNf4FcsMkt() {
		return nf4FcsMkt;
	}

	public void setNf4FcsMkt(double nf4FcsMkt) {
		this.nf4FcsMkt = nf4FcsMkt;
	}

	public double getEstFcsDisMkt() {
		return estFcsDisMkt;
	}

	public void setEstFcsDisMkt(double estFcsDisMkt) {
		this.estFcsDisMkt = estFcsDisMkt;
	}

	public double getEstAdFcsMkt() {
		return estAdFcsMkt;
	}

	public void setEstAdFcsMkt(double estAdFcsMkt) {
		this.estAdFcsMkt = estAdFcsMkt;
	}

	public double getEstDisAdFcsMkt() {
		return estDisAdFcsMkt;
	}

	public void setEstDisAdFcsMkt(double estDisAdFcsMkt) {
		this.estDisAdFcsMkt = estDisAdFcsMkt;
	}

	public double getMcpCompMkt() {
		return mcpCompMkt;
	}

	public void setMcpCompMkt(double mcpCompMkt) {
		this.mcpCompMkt = mcpCompMkt;
	}

	public double getUnitPerMcpCompMkt() {
		return unitPerMcpCompMkt;
	}

	public void setUnitPerMcpCompMkt(double unitPerMcpCompMkt) {
		this.unitPerMcpCompMkt = unitPerMcpCompMkt;
	}

	public double getFivePercentilePrc() {
		return fivePercentilePrc;
	}

	public void setFivePercentilePrc(double fivePercentilePrc) {
		this.fivePercentilePrc = fivePercentilePrc;
	}

	public double getTenPercentilePrc() {
		return tenPercentilePrc;
	}

	public void setTenPercentilePrc(double tenPercentilePrc) {
		this.tenPercentilePrc = tenPercentilePrc;
	}

	public double getFifteenPercentilePrc() {
		return fifteenPercentilePrc;
	}

	public void setFifteenPercentilePrc(double fifteenPercentilePrc) {
		this.fifteenPercentilePrc = fifteenPercentilePrc;
	}

	public double getTwentyPercentilePrc() {
		return twentyPercentilePrc;
	}

	public void setTwentyPercentilePrc(double twentyPercentilePrc) {
		this.twentyPercentilePrc = twentyPercentilePrc;
	}

	public double getTwentyFivePercentilePrc() {
		return twentyFivePercentilePrc;
	}

	public void setTwentyFivePercentilePrc(double twentyFivePercentilePrc) {
		this.twentyFivePercentilePrc = twentyFivePercentilePrc;
	}

	public double getThirtyPercentilePrc() {
		return thirtyPercentilePrc;
	}

	public void setThirtyPercentilePrc(double thirtyPercentilePrc) {
		this.thirtyPercentilePrc = thirtyPercentilePrc;
	}

	public double getThirtyFivePercentilePrc() {
		return thirtyFivePercentilePrc;
	}

	public void setThirtyFivePercentilePrc(double thirtyFivePercentilePrc) {
		this.thirtyFivePercentilePrc = thirtyFivePercentilePrc;
	}

	public double getFourtyPercentilePrc() {
		return fourtyPercentilePrc;
	}

	public void setFourtyPercentilePrc(double fourtyPercentilePrc) {
		this.fourtyPercentilePrc = fourtyPercentilePrc;
	}

	public double getFourtyFivePercentilePrc() {
		return fourtyFivePercentilePrc;
	}

	public void setFourtyFivePercentilePrc(double fourtyFivePercentilePrc) {
		this.fourtyFivePercentilePrc = fourtyFivePercentilePrc;
	}

	public double getFiftyPercentilePrc() {
		return fiftyPercentilePrc;
	}

	public void setFiftyPercentilePrc(double fiftyPercentilePrc) {
		this.fiftyPercentilePrc = fiftyPercentilePrc;
	}

	public double getFiftyfivePercentilePrc() {
		return fiftyfivePercentilePrc;
	}

	public void setFiftyfivePercentilePrc(double fiftyfivePercentilePrc) {
		this.fiftyfivePercentilePrc = fiftyfivePercentilePrc;
	}

	public double getSixtyPercentilePrc() {
		return sixtyPercentilePrc;
	}

	public void setSixtyPercentilePrc(double sixtyPercentilePrc) {
		this.sixtyPercentilePrc = sixtyPercentilePrc;
	}

	public double getSixtyFivePercentilePrc() {
		return sixtyFivePercentilePrc;
	}

	public void setSixtyFivePercentilePrc(double sixtyFivePercentilePrc) {
		this.sixtyFivePercentilePrc = sixtyFivePercentilePrc;
	}

	public double getSeventyPercentilePrc() {
		return seventyPercentilePrc;
	}

	public void setSeventyPercentilePrc(double seventyPercentilePrc) {
		this.seventyPercentilePrc = seventyPercentilePrc;
	}

	public double getSeventyFivePercentilePrc() {
		return seventyFivePercentilePrc;
	}

	public void setSeventyFivePercentilePrc(double seventyFivePercentilePrc) {
		this.seventyFivePercentilePrc = seventyFivePercentilePrc;
	}

	public double getEightyPercentilePrc() {
		return eightyPercentilePrc;
	}

	public void setEightyPercentilePrc(double eightyPercentilePrc) {
		this.eightyPercentilePrc = eightyPercentilePrc;
	}

	public double getEightyFivePercentilePrc() {
		return eightyFivePercentilePrc;
	}

	public void setEightyFivePercentilePrc(double eightyFivePercentilePrc) {
		this.eightyFivePercentilePrc = eightyFivePercentilePrc;
	}

	public double getNinetyPercentilePrc() {
		return ninetyPercentilePrc;
	}

	public void setNinetyPercentilePrc(double ninetyPercentilePrc) {
		this.ninetyPercentilePrc = ninetyPercentilePrc;
	}

	public double getNinetyFivePercentilePrc() {
		return ninetyFivePercentilePrc;
	}

	public void setNinetyFivePercentilePrc(double ninetyFivePercentilePrc) {
		this.ninetyFivePercentilePrc = ninetyFivePercentilePrc;
	}
	
	@Override
	public Object clone() throws CloneNotSupportedException {
		return super.clone();
	}

}