package com.pristine.dto.offermgmt.itemClassification;

import com.pristine.dto.offermgmt.ProductKey;

public class ItemClassificationDTO {
	private int rankId;
	private ProductKey productKey;
	private String retLirName;
	private Integer visitCount;
	private Integer uniqueHouseHoldCount;
	private Double annualSpendDollar;
	private int retLirId;
	private int departmentId;
	
	public int getRankId() {
		return rankId;
	}
	public void setRankId(int rankId) {
		this.rankId = rankId;
	}
	public String getRetLirName() {
		return retLirName;
	}
	public void setRetLirName(String retLirName) {
		this.retLirName = retLirName;
	}
	public Integer getVisitCount() {
		return visitCount;
	}
	public void setVisitCount(Integer visitCount) {
		this.visitCount = visitCount;
	}
	public Integer getUniqueHouseHoldCount() {
		return uniqueHouseHoldCount;
	}
	public void setUniqueHouseHoldCount(Integer uniqueHouseHoldCount) {
		this.uniqueHouseHoldCount = uniqueHouseHoldCount;
	}
	public Double getAnnualSpendDollar() {
		return annualSpendDollar;
	}
	public void setAnnualSpendDollar(Double annualSpendDollar) {
		this.annualSpendDollar = annualSpendDollar;
	}
	
	public String toString(){
		return ("productId - " + productKey.getProductId() + " : " + "productLevelId - " + productKey.getProductLevelId() 
				+ " : " + "retLirName - " + retLirName + " : " + "visitCount - " + visitCount 
				+ " : " + " uniqueHouseHoldCount - " + uniqueHouseHoldCount 
				+ " : " + "annualSpendDollar - " + annualSpendDollar);	
	}
	public ProductKey getProductKey() {
		return productKey;
	}
	public void setProductKey(ProductKey productKey) {
		this.productKey = productKey;
	}
	public int getRetLirId() {
		return retLirId;
	}
	public void setRetLirId(int retLirId) {
		this.retLirId = retLirId;
	}
	public int getDepartmentId() {
		return departmentId;
	}
	public void setDepartmentId(int departmentId) {
		this.departmentId = departmentId;
	}
}
