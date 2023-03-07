package com.pristine.dataload.offermgmt.mwr.config;

import com.pristine.util.PropertyManager;
import com.pristine.util.offermgmt.PRConstants;

public class MultiWeekRecConfigSettings {

	//Config variables
	private static int mwrXWeeksConfig;
	private static double mwrCostChangeThreshold;
	private static int mwrCostHistory;
	private static int mwrNumberOfWeeksIMS;
	private static int mwrNumberOfWeeksBehind;
	private static int mwrMovHistoryWeeks;
	private static int mwrCompHistory;
	private static int mwrNoOfsaleAdDisplayWeeks;
	private static String mwrCostChangeBehaviour;
	private static double mwrMaxUnitPriceDiff;
	private static int mwrPredictionBatchCount;
	private static String mwrRecommendationMode;
	//Config name constants
	//Number of weeks from current week
	private static final String MWR_X_WEEKS_BEFORE_REC = "MWR_X_WEEKS_BEFORE_REC";
	//Cost change threshold
	private static final String MWR_COST_CHANGE_THRESHOLD = "PR_DASHBOARD.COST_CHANGE_THRESHOLD";
	//Cost history
	private static final String MWR_COST_HISTORY = "PR_COST_HISTORY";
	//Number of weeks IMS data
	private static final String MWR_NO_OF_WEEKS_IMS = "PR_NO_OF_WEEKS_IMS_DATA";
	//Number of weeks behind
	private static final String MWR_NO_OF_WEEKS_BEHIND = "PR_AVG_MOVEMENT";
	//Last X Weeks movement
	private static final String HISTORY_WEEKS_FOR_MOVING_ITEMS = "APPLY_GUIDELINES_CONSTRAINTS_FOR_ITEMS_MOVING_IN_X_WEEKS";
	//Comp history
	private static final String MWR_COMP_HISTORY = "PR_COMP_HISTORY";
	//Number of weeks for Sale details
	private static final String MWR_SALE_AD_DISP_NO_OF_WEEKS = "PR_SALE_AD_DISP_NO_OF_WEEKS";
	//Cost change behaviour
	private static final String MWR_COST_CHANGE_BEHAVIOR = "PR_COST_CHANGE_BEHAVIOR";
	// max unit price difference for multiple price
	private static final String REC_MAX_UNIT_PRICE_DIFF_FOR_MULTPLE_PRICE = "REC_MAX_UNIT_PRICE_DIFF_FOR_MULTPLE_PRICE";
	// Multi week Prediction batch count 
	private static final String MWR_PREDICTION_BATCH_COUNT = "MWR_PREDICTION_BATCH_COUNT";
	// Multi week Rec Mode
	private static final String MWR_RECOMMENDATION_MODE = "MWR_RECOMMENDATION_MODE";
	//Future Cost
	private static final String MWR_FUTURE_COST = "FUTURE_COST_WEEKS";
	
	
	public static int getMwrXWeeksConfig() {
		if(mwrXWeeksConfig == 0){
			mwrXWeeksConfig = Integer.parseInt(PropertyManager.getProperty(MWR_X_WEEKS_BEFORE_REC));
		}
		return mwrXWeeksConfig;
	}

	public static double getMwrCostChangeThreshold() {
		if(mwrCostChangeThreshold == 0){
			mwrCostChangeThreshold = Double.parseDouble(PropertyManager.getProperty(MWR_COST_CHANGE_THRESHOLD, "0"));
		}
		return mwrCostChangeThreshold;
	}

	public static void setMwrCostChangeThreshold(double mwrCostChangeThreshold) {
		MultiWeekRecConfigSettings.mwrCostChangeThreshold = mwrCostChangeThreshold;
	}

	public static int getMwrCostHistory() {
		if(mwrCostHistory == 0){
			mwrCostHistory = Integer.parseInt(PropertyManager.getProperty(MWR_COST_HISTORY, "0"));
		}
		return mwrCostHistory;
	}

	public static void setMwrCostHistory(int mwrCostHistory) {
		MultiWeekRecConfigSettings.mwrCostHistory = mwrCostHistory;
	}

	public static int getMwrNumberOfWeeksIMS() {
		if(mwrNumberOfWeeksIMS == 0){
			mwrNumberOfWeeksIMS = Integer.parseInt(PropertyManager.getProperty(MWR_NO_OF_WEEKS_IMS, "0"));
		}
		return mwrNumberOfWeeksIMS;
	}

	public static void setMwrNumberOfWeeksIMS(int mwrNumberOfWeeksIMS) {
		MultiWeekRecConfigSettings.mwrNumberOfWeeksIMS = mwrNumberOfWeeksIMS;
	}

	public static int getMwrNumberOfWeeksBehind() {
		if(mwrNumberOfWeeksBehind == 0){
			mwrNumberOfWeeksBehind = Integer.parseInt(PropertyManager.getProperty(MWR_NO_OF_WEEKS_BEHIND, "0"));
		}
		return mwrNumberOfWeeksBehind;
	}

	public static void setMwrNumberOfWeeksBehind(int mwrNumberOfWeeksBehind) {
		MultiWeekRecConfigSettings.mwrNumberOfWeeksBehind = mwrNumberOfWeeksBehind;
	}

	public static int getHistoryWeeksForMovingItems() {
		if(mwrMovHistoryWeeks == 0){
			mwrMovHistoryWeeks = Integer.parseInt(PropertyManager.getProperty(HISTORY_WEEKS_FOR_MOVING_ITEMS, "0"));
		}
		return mwrMovHistoryWeeks;
	}

	public static void setHistoryWeeksForMovingItems(int mwrMovHistoryWeeks) {
		MultiWeekRecConfigSettings.mwrMovHistoryWeeks = mwrMovHistoryWeeks;
	}

	public static int getMwrCompHistory() {
		if(mwrCompHistory == 0){
			mwrCompHistory = Integer.parseInt(PropertyManager.getProperty(MWR_COMP_HISTORY, "0"));
		}
		return mwrCompHistory;
	}

	public static void setMwrCompHistory(int mwrCompHistory) {
		MultiWeekRecConfigSettings.mwrCompHistory = mwrCompHistory;
	}

	public static int getMwrNoOfSaleAdDisplayWeeks() {
		if(mwrNoOfsaleAdDisplayWeeks == 0){
			mwrNoOfsaleAdDisplayWeeks = Integer.parseInt(PropertyManager.getProperty(MWR_SALE_AD_DISP_NO_OF_WEEKS, "0"));
		}
		return mwrNoOfsaleAdDisplayWeeks;
	}

	public static void setMwrXWeeksConfig(int mwrXWeeksConfig) {
		MultiWeekRecConfigSettings.mwrXWeeksConfig = mwrXWeeksConfig;
	}

	public static void setMwrNoOfsaleAdDisplayWeeks(int mwrNoOfsaleAdDisplayWeeks) {
		MultiWeekRecConfigSettings.mwrNoOfsaleAdDisplayWeeks = mwrNoOfsaleAdDisplayWeeks;
	}

	public static String getMwrCostChangeBehaviour() {
		if(mwrCostChangeBehaviour == null){
			mwrCostChangeBehaviour = PropertyManager.getProperty(MWR_COST_CHANGE_BEHAVIOR);
		}
		return mwrCostChangeBehaviour;
	}

	public static void setMwrCostChangeBehaviour(String mwrCostChangeBehaviour) {
		MultiWeekRecConfigSettings.mwrCostChangeBehaviour = mwrCostChangeBehaviour;
	}

	public static double getMwrMaxUnitPriceDiff() {
		if(mwrMaxUnitPriceDiff == 0){
			mwrMaxUnitPriceDiff = Double.parseDouble(PropertyManager.getProperty(REC_MAX_UNIT_PRICE_DIFF_FOR_MULTPLE_PRICE, "0"));
		}
		return mwrMaxUnitPriceDiff;
	}

	public static void setMwrMaxUnitPriceDiff(double mwrMaxUnitPriceDiff) {
		MultiWeekRecConfigSettings.mwrMaxUnitPriceDiff = mwrMaxUnitPriceDiff;
	}

	public static int getMwrPredictionBatchCount() {
		if(mwrPredictionBatchCount == 0){
			mwrPredictionBatchCount = Integer.parseInt(PropertyManager.getProperty(MWR_PREDICTION_BATCH_COUNT, "100"));
		}
		return mwrPredictionBatchCount;
	}

	public static void setMwrPredictionBatchCount(int mwrPredictionBatchCount) {
		MultiWeekRecConfigSettings.mwrPredictionBatchCount = mwrPredictionBatchCount;
	}

	public static String getMwrRecommendationMode() {
		if(mwrRecommendationMode == null){
			mwrRecommendationMode = PropertyManager.getProperty(MWR_RECOMMENDATION_MODE, PRConstants.RECOMMEND_BY_SINGLE_WEEK_PRED);
		}
		return mwrRecommendationMode;
	}

	public static void setMwrRecommendationMode(String mwrRecommendationMode) {
		MultiWeekRecConfigSettings.mwrRecommendationMode = mwrRecommendationMode;
	}
	
	public static int getFutureCostWeeksToFetch() {
		if(mwrCostHistory == 0){
			mwrCostHistory = Integer.parseInt(PropertyManager.getProperty(MWR_FUTURE_COST, "0"));
		}
		return mwrCostHistory;
	}

}
