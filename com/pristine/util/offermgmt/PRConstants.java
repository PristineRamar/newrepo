package com.pristine.util.offermgmt;

public class PRConstants {
	public static final char VALUE_TYPE_PCT = 'P';
	public static final char VALUE_TYPE_$ = 'D';
	public static final char VALUE_TYPE_I = 'I';
	public static final char MAR_COST_INCREASED_FLAG = 'I';
	public static final char MAR_COST_DECREASED_FLAG = 'D';
	public static final char MAR_COST_CHANGED_FLAG = 'C';
	public static final char MAR_COST_UNCHANGED_FLAG = 'U';
	public static final char MAR_COST_ALL_FLAG = 'A';
	public static final char MAR_NON_MOVING_FLAG = 'N';
	
	public static final char COMP_TYPE_PRIMARY = 'P';
	public static final char COMP_TYPE_SECONDARY = 'S';
	public static final char COMP_TYPE_TERTIARY = 'T';
	public static final char COMP_TYPE_FOUR = 'F';
	public static final char COMP_TYPE_NONE = 'N';
	
	public static final char LIG_GUIDELINE_SAME = 'S';
	public static final char LIG_GUIDELINE_DIFF = 'D';
	
	public static final char RUN_TYPE_ONLINE = 'O';
	public static final char RUN_TYPE_TEMP = 'T';
	public static final char RUN_TYPE_BATCH = 'B';
	public static final char RUN_TYPE_DASHBOARD = 'D';
	
	public static final double DEFAULT_MAX_PRICE = 9999;
	public static final String DEFAULT_MAX_PRICE_STR = "9999";
	
	public static final char BRAND_GUIDELINE_IS_LOWER = 'Y';
	public static final char NATIONAL_BRAND = 'N';
	public static final char STORE_BRAND = 'S';
	
	public static final int ZONE_LEVEL_TYPE_ID = 6;
	public static final int ZONE_LIST_LEVEL_TYPE_ID = 21;
	public static final int PRODUCT_LIST_LEVEL_TYPE_ID = 21;
	
	public static final String TRUE = "TRUE";
	
	public static final String RUN_STATUS_SUCCESS = "S";
	public static final String RUN_STATUS_ERROR = "E";
	public static final String RUN_STATUS_IN_PROGRESS = "I";
	
	public static final char RETAIL_TYPE_SHELF = 'S';
	public static final char RETAIL_TYPE_UNIT = 'U';
	
	public static final int EXPORT_STATUS = 8;
	public static final int STATUS_PARTIALLY_EXPORTED = 7;
	
	
	public static final String PRICE_GROUP_EXPR_ABOVE = "above";
	public static final String PRICE_GROUP_EXPR_BELOW = "below";
	public static final String PRICE_GROUP_EXPR_EQUAL_SYM = "=";
	public static final String PRICE_GROUP_EXPR_GREATER_SYM = ">";
	public static final String PRICE_GROUP_EXPR_GREATER_THAN_EQUAL_SYM = ">=";
	public static final String PRICE_GROUP_EXPR_LESSER_SYM = "<";
	public static final String PRICE_GROUP_EXPR_LESSER_THAN_EQUAL_SYM = "<=";
	public static final String PRICE_GROUP_EXPR_NOT_GREATER_THAN = "not >";
	public static final String PRICE_GROUP_EXPR_NOT_GREATER_THAN_EQUAL = "not >=";
	public static final String PRICE_GROUP_EXPR_NOT_LESSER_THAN = "not <";
	public static final String PRICE_GROUP_EXPR_NOT_LESSER_THAN_EQUAL = "not <=";
	public static final String PRICE_GROUP_EXPR_LOWER = "lower";
	public static final String PRICE_GROUP_EXPR_WITHIN = "within";
	public static final String PRICE_GROUP_EXPR_EQUAL = "equal";
	
	public static final String PRICE_GROUP_EXPR_MEDIAN = "median";
	public static final String PRICE_GROUP_EXPR_LOWEST_MEDIAN = "lowest-median";
	public static final String PRICE_GROUP_EXPR_AVG = "avg";
	public static final String PRICE_GROUP_EXPR_WEIGHTED = "weighted";
	
	
	
	
	
	
	public static final char BRAND_RELATION = 'B';
	public static final char SIZE_RELATION = 'S';
	public static final char BRAND_SIZE_PRECECENDENCE_NA = 'X';
	
	public static final String BATCH_USER = "BATCH";
	
	public static final int PREDICTION_DEAL_TYPE = 1;	
	
	//Round Logic
	public static final String ROUND_UP = "ROUND_UP";
	public static final String ROUND_DOWN = "ROUND_DOWN";
	public static final String ROUND_CLOSEST = "ROUND_CLOSEST";
	
	//Cost increse, decrease, same
	public static final Integer COST_INCREASE = 1;
	public static final Integer COST_DECREASE = -1;
	public static final Integer COST_NO_CHANGE = 0;
	
	//Cost Change Behavior
	public static final String AHOLD = "AHOLD";
	public static final String COST_CHANGE_GENERIC = "GENERIC";
	public static final String COST_CHANGE_AHOLD_MARGIN = "AHOLD_MARGIN";
	public static final String COST_CHANGE_AHOLD_STORE_BRAND = "AHOLD_STORE_BRAND";
	
	//Multi Comp
	public static final char GROUP_PRICE_TYPE_MIN = 'M';
	public static final char GROUP_PRICE_TYPE_AVG = 'A';
	public static final char GROUP_PRICE_TYPE_RULE = 'R';
	public static final char GROUP_PRICE_TYPE_MAX = 'X';
	
	//Lower Higher Constraint
	public static final char CONSTRAINT_LOWER = 'L';
	public static final char CONSTRAINT_HIGHER = 'H';
	public static final char CONSTRAINT_NO_LOWER_HIGHER = 'N';
	
	public static final String NOT_APPLICABLE = "NA";
	
	public static final char DSD_RECOMMENDATION_ZONE = 'Z';
	public static final char DSD_RECOMMENDATION_STORE = 'S';
	
	//Execution Time Log
	public static final String OVERALL_RECOMMENDATION = "OVERALL_RECOMMENDATION";
	public static final String GET_ALL_ITEMS = "DB_READ_ALL_ITEMS";
	public static final String GET_PRICE_GROUPS = "DB_READ_PRICE_GROUP";
	public static final String GET_ALL_STRATEGIES = "DB_READ_ALL_STRATEGY";
	public static final String GET_STORE_STRATEGIES = "DB_READ_STORE_STRATEGY";
	public static final String GET_DASHBOARD_RESET_DATE = "DB_READ_DASHBOARD_RESET_DATE";
	public static final String GET_STORE_ITEM_MAP = "DB_READ_STORE_ITEMS";
	public static final String GET_STORE_ITEMS = "DB_PROCESS_STORE_ITEMS";
	public static final String PROCESS_STORE_ITEMS = "PROCESS_STORE_ITEMS";
	public static final String PROCESS_ZONE_ITEMS = "PROCESS_ZONE_ITEMS";
	public static final String GET_STORE_PRICE = "DB_PROCESS_STORE_PRICE";
	public static final String GET_ZONE_PRICE = "DB_PROCESS_ZONE_PRICE";
	public static final String GET_STORE_COST = "DB_PROCESS_STORE_COST";
	public static final String GET_ZONE_COST = "DB_PROCESS_ZONE_COST";
	public static final String GET_STORE_MOV = "DB_PROCESS_STORE_MOV";
	public static final String GET_ZONE_MOV = "DB_PROCESS_ZONE_MOV";
	public static final String GET_ZONE_COMP = "DB_PROCESS_ZONE_COMP";
	public static final String CHECK_STORE_REC_REQUIRED = "DB_PROCESS_CHECK_STORE_REC_REQUIRED";
	public static final String GET_IS_PRICE_ZONE = "DB_READ_IS_PRICE_ZONE";
	public static final String GET_STORES_OF_PRICE_ZONE = "DB_READ_PRICE_ZONE_STORES";
	public static final String GET_PRICE_CHECK_LIST_INFO = "DB_READ_PRICE_CHECK_LIST_INFO";
	public static final String GET_STORE_COUNT_OF_ZONE = "DB_READ_STORE_COUNT_OF_ZONE";
	public static final String GET_SUBSTITUTE_ITEMS = "DB_READ_SUBSTITUTE_ITEMS";
	public static final String FIND_ZONE_STRATEGY = "PROCESS_ZONE_FIND_ITEM_STRATEGY";
	public static final String FIND_STORE_STRATEGY = "PROCESS_STORE_FIND_ITEM_STRATEGY";
	public static final String GET_ALL_TEMP_STRATEGIES = "DB_READ_ALL_TEMP_STRATEGY";
	public static final String GET_GLOBAL_STRATEGY = "DB_READ_GLOBAL_STRATEGY";
	public static final String GET_STORE_COUNT_OF_GLOBAL_ZONE = "DB_READ_STORE_COUNT_OF_GLOBAL_ZONE";
	
	public static final String PREDICTION_PRICE_REC = "PREDICTION_DURING_PRICE_RECOMMENDATION";
	public static final String PREDICTION_MAR_OPP = "PREDICTION_DURING_MARGIN_OPPORTUNITY";
	public static final String PREDICTION_SALE = "PREDICTION_SALE";
	public static final String PREDICTION_ON_DEMAND = "PREDICTION_DURING_ON_DEMAND";
	public static final String PREDICTION_SUBSTITUE_IMPACT = "PREDICTION_DURING_SUBSTITUTE_IMPACT";
	public static final String PREDICTION_EXPLAIN = "EXPLAIN_PREDICTION";
	public static final String PREDICTION_PROMOTION = "PREDICTION_PROMOTION";
	public static final String INSERT_ZONE_RECOMMENDATION = "DB_WRITE_ZONE_RECOMMENDATION";
	public static final String INSERT_STORE_RECOMMENDATION = "DB_WRITE_STORE_RECOMMENDATION";
	public static final String UPDATE_VENDOR_ID = "PROCESS_UPDATE_VENDOR_ID";
	
	//
	public static final int BATCH_INSERT_COUNT_STORE_RECOMMENDATION = 30000;
	
	public static final Integer SUCCESS = 0;
	public static final Integer FAILURE = 1;
	
	public static final Integer NON_LIG_ITEM_INDICATOR = 0;
	public static final Integer LIG_ITEM_INDICATOR = 1;
	public static final Integer DEFAULT_REG_MULTIPLE = 1;
	
	//Audit tool constants ***Start***
	
	public static final String MESSAGE_AUDIT_PROGRESS = "Auditing is in progress";
	public static final String MESSAGE_RECOMMENDATION_DETAILS = "Fetching recommendation info";
	public static final String MESSAGE_AUDIT_COMPLETED = "Auditing is completed successfully";
	public static final String MESSAGE_AUDIT_ERROR = "Error";
	public static final String MESSAGE_RECOMMENDATION_NOT_FOUND = "Recommendation not found";
	public static final String MESSAGE_AUDIT_INFO_NOT_AVAILABLE= "Auditing info is not availble";
	public static final String MESSAGE_AUDIT_STARTED = "Auditing is started";
	public static final String AUDIT_COMPLETE_PRECENT_FIFTY = "50";
	public static final String AUDIT_COMPLETE_PRECENT_FULL = "100";
	public static final String AUDIT_COMPLETE_PRECENT_SEVENTY_FIVE = "75";
	public static final int LOWER_SIZE_ITEMS_GT_PCT = 1;
	public static final int SIMILAR_SIZE_ITEMS_GT_PCT = 2;
	public static final int MARGIN_LT_PCT = 3;
	public static final int MARGIN_GT_PCT = 4;
	public static final int RETAIL_LT_PRIME_COMP_PCT = 5;
	public static final int RETAIL_GT_PRIME_COMP_PCT = 6;
	public static final int RETAIL_CHANGED_LT_PCT = 7;
	public static final int RETAIL_CHANGED_GT_PCT = 8;
	public static final int LIST_COST_CHANGE_GT_PCT = 9;
	public static final int PRIMARY_COMP_CHANGE_GT_PCT = 10;
	public static final int RETAIL_CHANGES_X_TIMES = 11;
	public static final int END_DIGIT_VIOL = 12;
	public static final double PREDICTION_DURATION = 3.25;
	//Audit tool constants ***End***
	
	//Subs Related
	public static final Integer SUBS_CURRENT_PRICE = 1;
	public static final Integer SUBS_RECOMMENDED_PRICE = 2;
	public static final Integer SUBS_LOWER_PRICE = 3;
	public static final Integer SUBS_HIGHER_PRICE = 4;
	
	//Notification Analysis 
	public static final Integer REGULAR_PRICE_RECOMMENDATION = 1;
	public static final Integer REC_COMPLETED = 1;
	public static final int STATUS_EXPORTED = 8;
	public static final int STATUS_APPROVED = 6;
	public static final int STATUS_RECOMMENDED = 1;
	
	//Notification Service
	 public static final Integer ERROR_REC = 5;
	 public static final Integer NO_PREDICTIONS = 12;
	 public static final Integer NO_NEW_PRICES = 13;
	 public static final Integer NO_RECOMMENDATION = 14;
	 public static final Integer REC_MODULE_ID = 1;
	 
	 public static final double HUNDRED = 100.0;
	 
	 //Reason Codes for Questionable Prediction
	 public static final int NEW_ITEM_LESS_HISTORY_THAN_X_WEEKS = 2;
	 public static final int LESS_OBSERVATIONS_IN_LAST_X_WEEKS = 3;
	 public static final int LESS_PRICE_CHANGES_IN_LAST_X_WEEKS = 4;
	 public static final String QUESTIONABLE_PREDICTED_MOVEMENT_A = "1A";
	 public static final String QUESTIONABLE_PREDICTED_MOVEMENT_B = "1B";
	 public static final int FIRST_TIME_ON_AD = 5;
	 public static final int FIRST_TIME_ON_PROMO = 6;
	 public static final int DAYS_IN_WEEK = 7;
	 public static final String CURRENT_PRICE_FORECAST_MOV_NOT_WITHIN_RANGE = "2A";
	 public static final String NEW_FORECAST_DIFFERENCE_MORE_THAN_X_PERCENT = "3A";
	 
	 public static final int STORE_EXPORT_FLAG_NLP_END_WEEK_COUNT = 52;
	 public static final int STORE_EXPORT_FLAG_LP_END_WEEK_COUNT = 104;
	 
	 public static final String MW_X_WEEKS_RECOMMENDATION = "X_WEEKS";
	 public static final String MW_QUARTER_RECOMMENDATION = "QUARTER";
	 public static final String MW_WEEK_RECOMMENDATION = "WEEK";
	 
	 public static final String WEEK_TYPE_COMPLETED = "C";
	 public static final String WEEK_TYPE_IN_BETWEEN = "I";
	 public static final String WEEK_TYPE_FUTURE = "F";
	 
	 public static final String PREDICTION_STATUS_SUCCESS = "SUCCESS";
	 public static final String PREDICTION_STATUS_ERROR = "ERROR";
	 public static final String MW_EDLP_ITEM = "EDLP";
	 public static final String MW_PROMO_ITEM = "PROMO";
	 public static final String INDEPENDENT_ITEMS = "I";
	 public static final String DEPENDENT_ITEMS = "D";
	 public static final String RECOMMEND_BY_SINGLE_WEEK_PRED = "SINGLE_WEEK";
	 public static final String RECOMMEND_BY_MULTI_WEEK_PRED = "MULTI_WEEK";
	 public static final String OR_ITEMS_SOURCE_ALREADY_PROMOTED = "PROMO";
	 public static final String OR_ITEMS_SOURCE_DB = "DB";
	 public static final String LIG_MIN_PRICE = "Same price - min price of the group";
	 public static final String LIG_MAX_PRICE = "Same price - max price of the group";
	 public static final String LIG_MODE_PRICE = "Same price - mode price of the group";
	 public static final String LIG_HIGH_MOVER_PRICE = "Same price - highest mover price of the group";
	 public static final int STORE_LOCK_LIST = 25;
	 public static int EXPORT_EMERGENCY=10;
	
	//Added by karishma on 30/3/2020 for selecting the weektype for getting MovemetData
	public static final String RECENT_X_WEEKS="RECENT_X_WEEKS";
	public static final String X_WEEKS_FOR_IMPACT="X_WEEKS_FOR_TOT_IMPACT";
	public static final String X_WEEKS_FOR_ADD_CRITERIA="X_WEEKS_FOR_ADD_CRITERIA";
	public static final String  X_WEEKS_FOR_PRED_EXCLUDE="XWEEKSFORPREDEXCLUDE";
	public static final String  X_WEEKS_FOR_WAC="X_WEEKS_FOR_WAC";
	public static final String  X_WEEKS_MOV="X_WEEKS_MOV";
	public static final String X_WEEKS_MOV_LIG_REP_ITEM = "X_WEEKS_MOV_LIG_REP_ITEM";
	
	//added to set key for competitors in AZ
	public static final int COMP_TYPE_1= 1;
	public static final int COMP_TYPE_2= 2;
	public static final int COMP_TYPE_3= 3;
	public static final int COMP_TYPE_4= 4;
	public static final int COMP_TYPE_5= 5;
	
	public static final int MAX_RET_HT_MIN_RET_LT=1;
	public static final int MAX_RET_HT_MAX_RET_LT=2;
	public static final int MIN_RET_HT_MIN_RET_LT=3;
	public static final int MIN_RET_HT_MAX_RET_LT=4;
	
	public static final String GET_PRICE_CHECK_LIST_INFO_PRICE_TEST = "DB_READ_PRICE_CHECK_LIST_INFO_PRICE_TEST";
	
	public static final int WHAT_IF_SUCCESS=0;
	public static final int WHAT_IF_ERROR=-1;
	
	public static final int RETAIL_INCREASED=1;
	public static final int RETAIL_DECREASED=2;
	public static final int COST_INCREASED=3;
	public static final int COST_DECREASED=4;
	public static final int COMP_INCREASED=5;
	public static final int COMP_DECREASED=6;
	
	
	public static final String CALENDAR_DAY = "D";
	public static final String CALENDAR_WEEK = "W";
	public static final String CALENDAR_PERIOD = "P";
	public static final String CALENDAR_QUARTER = "Q";
	public static final String CALENDAR_YEAR = "Y";
	
	public static final String LAST_YEAR_SIMILAR_WEEKS_MOVEMENT = "LAST_YEAR_SIMILAR_WEEKS_MOVEMENT";
}
