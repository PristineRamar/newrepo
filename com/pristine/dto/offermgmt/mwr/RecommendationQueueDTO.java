package com.pristine.dto.offermgmt.mwr;

import javax.print.attribute.standard.DateTimeAtCompleted;

public class RecommendationQueueDTO {

	public String locationLevelId;
	public String locationId;
	public String productLevelId;
	public String productId;
	public long queueId;
	private String submittedTime;
	public String recType;
	public String submittedBy;
	public long strategyId;
	public int considerScenario;
	
	public long getQueueId() {
		return queueId;
	}
	public void setQueueId(long queueId) {
		this.queueId = queueId;
	}
	public String getLocationLevelId() {
		return locationLevelId;
	}
	public void setLocationLevelId(String locationLevelId) {
		this.locationLevelId = locationLevelId;
	}
	public String getLocationId() {
		return locationId;
	}
	public void setLocationId(String locationId) {
		this.locationId = locationId;
	}
	public String getProductLevelId() {
		return productLevelId;
	}
	public void setProductLevelId(String productLevelId) {
		this.productLevelId = productLevelId;
	}
	public String getProductId() {
		return productId;
	}
	public void setProductId(String productId) {
		this.productId = productId;
	}
	public String getSubmittedTime() {
		return submittedTime;
	}
	public void setSubmittedTime(String submittedTime) {
		this.submittedTime = submittedTime;
	}
	public String getRecType() {
		return recType;
	}
	public void setRecType(String recType) {
		this.recType = recType;
	}
	public String getSubmittedBy() {
		return submittedBy;
	}
	public void setSubmittedBy(String submittedBy) {
		this.submittedBy = submittedBy;
	}
	public long getStrategyId() {
		return strategyId;
	}
	public void setStrategyId(long strategyId) {
		this.strategyId = strategyId;
	}
	public int getConsiderScenario() {
		return considerScenario;
	}
	public void setConsiderScenario(int considerScenario) {
		this.considerScenario = considerScenario;
	}
	
}
