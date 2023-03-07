package com.pristine.dto.customer;

public class CustomerSegmentDefDTO {
	
	private Double segmentId;
	private Double segmentClassification;
	private Double segmentType;
	private String segmentName;
	private String segmentDesc;
	private Double rangeFrom;
	private Double rangeTo;
	private Double sortingOrder;
	private Double revenueMinRange;
	private Double revenueMaxRange;
	public Double getSegmentId() {
		return segmentId;
	}
	public void setSegmentId(Double segmentId) {
		this.segmentId = segmentId;
	}
	public Double getSegmentClassification() {
		return segmentClassification;
	}
	public void setSegmentClassification(Double segmentClassification) {
		this.segmentClassification = segmentClassification;
	}
	public Double getSegmentType() {
		return segmentType;
	}
	public void setSegmentType(Double segmentType) {
		this.segmentType = segmentType;
	}
	public String getSegmentName() {
		return segmentName;
	}
	public void setSegmentName(String segmentName) {
		this.segmentName = segmentName;
	}
	public String getSegmentDesc() {
		return segmentDesc;
	}
	public void setSegmentDesc(String segmentDesc) {
		this.segmentDesc = segmentDesc;
	}
	public Double getRangeFrom() {
		return rangeFrom;
	}
	public void setRangeFrom(Double rangeFrom) {
		this.rangeFrom = rangeFrom;
	}
	public Double getRangeTo() {
		return rangeTo;
	}
	public void setRangeTo(Double rangeTo) {
		this.rangeTo = rangeTo;
	}
	public Double getSortingOrder() {
		return sortingOrder;
	}
	public void setSortingOrder(Double sortingOrder) {
		this.sortingOrder = sortingOrder;
	}

	public Double getRevenueMinRange() {
		return revenueMinRange;
	}
	public void setRevenueMinRange(Double revenueMinRange) {
		this.revenueMinRange = revenueMinRange;
	}
	
	public Double getRevenueMaxRange() {
		return revenueMaxRange;
	}
	public void setRevenueMaxRange(Double revenueMaxRange) {
		this.revenueMaxRange = revenueMaxRange;
	}
}
