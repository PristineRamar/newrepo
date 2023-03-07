package com.pristine.dto.offermgmt.prediction;

import com.pristine.dto.offermgmt.MultiplePrice;

public class PredictionDiagnosticDataDTO {

	private int itemCode;
	private double minPrice;
	private int pageNo;
	private int blockNo;
	private String display;
	private int saleFlag;
	private MultiplePrice regPrice;
	private MultiplePrice salePrice;
	private String specialDay1;
	private int noOfObservations;
	private long avgMovement;
	public int getItemCode() {
		return itemCode;
	}
	public void setItemCode(int itemCode) {
		this.itemCode = itemCode;
	}
	public double getMinPrice() {
		return minPrice;
	}
	public void setMinPrice(double minPrice) {
		this.minPrice = minPrice;
	}
	public int getPageNo() {
		return pageNo;
	}
	public void setPageNo(int pageNo) {
		this.pageNo = pageNo;
	}
	public int getBlockNo() {
		return blockNo;
	}
	public void setBlockNo(int blockNo) {
		this.blockNo = blockNo;
	}
	public int getSaleFlag() {
		return saleFlag;
	}
	public void setSaleFlag(int saleFlag) {
		this.saleFlag = saleFlag;
	}
	public MultiplePrice getRegPrice() {
		return regPrice;
	}
	public void setRegPrice(MultiplePrice regPrice) {
		this.regPrice = regPrice;
	}
	public MultiplePrice getSalePrice() {
		return salePrice;
	}
	public void setSalePrice(MultiplePrice salePrice) {
		this.salePrice = salePrice;
	}
	public String getSpecialDay1() {
		return specialDay1;
	}
	public void setSpecialDay1(String specialDay1) {
		this.specialDay1 = specialDay1;
	}
	public int getNoOfObservations() {
		return noOfObservations;
	}
	public void setNoOfObservations(int noOfObservations) {
		this.noOfObservations = noOfObservations;
	}
	public long getAvgMovement() {
		return avgMovement;
	}
	public void setAvgMovement(long avgMovement) {
		this.avgMovement = avgMovement;
	}
	public String getDisplay() {
		return display;
	}
	public void setDisplay(String display) {
		this.display = display;
	}
}
