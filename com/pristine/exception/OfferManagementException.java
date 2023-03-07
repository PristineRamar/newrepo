package com.pristine.exception;

import org.apache.log4j.Logger;

import com.pristine.service.offermgmt.RecommendationErrorCode;

@SuppressWarnings("serial")
public class OfferManagementException extends Throwable {
	RecommendationErrorCode recommendationErrorCode = RecommendationErrorCode.GENERAL_EXCEPTION;
	static Logger logger = Logger.getLogger(OfferManagementException.class);

	public OfferManagementException(String msg, RecommendationErrorCode recommendationErrorCode) {
		super(msg);
		this.recommendationErrorCode = recommendationErrorCode;
		logException(logger, msg);
	}

	public RecommendationErrorCode getRecommendationErrorCode() {
		return recommendationErrorCode;
	}

	public void setRecommendationErrorCode(RecommendationErrorCode recommendationErrorCode) {
		this.recommendationErrorCode = recommendationErrorCode;
	}

	private void logException(Logger log, String message) {
		log.error(message, this);
	}
}
