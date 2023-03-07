package com.pristine.dto.offermgmt;

public class PriceCheckListDTO {
	private int priceCheckListId;
	private int priceCheckListTypeId;
	private int locationLevelId;
	private int locationId;
	private Integer precedence;
	private String useLeadZoneStrategy;
	private long strategyId;
	private String priceCheckListName;
	private double minRetail;
	private double maxRetail;
	private double lockedRetail;
	private String endDate;
	private int itemCode;
	private String createDate;
	private String updateDate;
	private String checkListTypeName;
	private PriceCheckListDTO checkListDTO;
	
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
	public int getPriceCheckListId() {
		return priceCheckListId;
	}
	public void setPriceCheckListId(int priceCheckListId) {
		this.priceCheckListId = priceCheckListId;
	}
	public int getPriceCheckListTypeId() {
		return priceCheckListTypeId;
	}
	public void setPriceCheckListTypeId(int priceCheckListTypeId) {
		this.priceCheckListTypeId = priceCheckListTypeId;
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
	public Integer getPrecedence() {
		return precedence;
	}
	public void setPrecedence(Integer precedence) {
		this.precedence = precedence;
	}
	public String getUseLeadZoneStrategy() {
		return useLeadZoneStrategy;
	}
	public void setUseLeadZoneStrategy(String useLeadZoneStrategy) {
		this.useLeadZoneStrategy = useLeadZoneStrategy;
	}
	public long getStrategyId() {
		return strategyId;
	}
	public void setStrategyId(long l) {
		this.strategyId = l;
	}
	public String getPriceCheckListName() {
		return priceCheckListName;
	}
	public void setPriceCheckListName(String priceCheckListName) {
		this.priceCheckListName = priceCheckListName;
	}
	public int getItemCode() {
		return itemCode;
	}
	public void setItemCode(int itemCode) {
		this.itemCode = itemCode;
	}
	public String getCreateDate() {
		return createDate;
	}
	public void setCreateDate(String createDate) {
		this.createDate = createDate;
	}
	public String getUpdateDate() {
		return updateDate;
	}
	public void setUpdateDate(String updateDate) {
		this.updateDate = updateDate;
	}
	public String getCheckListTypeName() {
		return checkListTypeName;
	}
	public void setCheckListTypeName(String checkListTypeName) {
		this.checkListTypeName = checkListTypeName;
	}
	public PriceCheckListDTO getCheckListDTO() {
		return checkListDTO;
	}
	public void setCheckListDTO(PriceCheckListDTO checkListDTO) {
		this.checkListDTO = checkListDTO;
	}
	
	
}
