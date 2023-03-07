/*
 * Author : Suresh 
 * Start Date : Jul 22, 2009
 * 
 * Change Description 				Changed By 			Date
 * --------------------------------------------------------------
 * Added few more constants			Naimish		  23rd July 2009
 */
package com.pristine.util;

public class Constants {
	// SCHEDULER CONSTANTS For Late Check

	public final static int LOBLEVELID = 12;
	public static final String LC_SECONDS = "Lc_Seconds";
	public static final String LC_MINUTES = "Lc_Minutes";
	public static final String LC_HOURS = "Lc_Hours";
	public static final String LC_DAY_OF_MONTH = "Lc_Day-of-month";
	public static final String LC_MONTH = "Lc_Month";
	public static final String LC_DAY_OF_WEEK = "Lc_Day-of-Week";
	public static final String LC_YEAR = "Lc_Year";
	// SCHEDULER CONSTANTS For Goal Setting
	public static final String GOAL_SECONDS = "Goal_Seconds";
	public static final String GOAL_MINUTES = "Goal_Minutes";
	public static final String GOAL_HOURS = "Goal_Hours";
	public static final String GOAL_DAY_OF_MONTH = "Goal_Day-of-month";
	public static final String GOAL_MONTH = "Goal_Month";
	public static final String GOAL_DAY_OF_WEEK = "Goal_Day-of-Week";
	public static final String GOAL_YEAR = "Goal_Year";
	// SCHEDULER CONSTANTS For Goal Setting
	public static final String VARIATION_SECONDS = "Variation_Seconds";
	public static final String VARIATION_MINUTES = "Variation_Minutes";
	public static final String VARIATION_HOURS = "Variation_Hours";
	public static final String VARIATION_DAY_OF_MONTH = "Variation_Day-of-month";
	public static final String VARIATION_MONTH = "Variation_Month";
	public static final String VARIATION_DAY_OF_WEEK = "Variation_Day-of-Week";
	public static final String VARIATION_YEAR = "Variation_Year";
	// properties files
	public static final String SCHEDULER_CONFIG_FILE = "sheduler_config.properties";
	public static final String ANALYSIS_CONFIG_FILE = "analysis.properties";
	public static final String LOG4J_PROPERTIES_FILE = "log4j.properties";
	// DB CONSTANTS
	public static final String CONST_DB_FILENAME = "db.properties"; // DB Property File Name
	public static final String CONST_DB_CONNECTION_URL = "DB_CONNECT_STRING";
	public static final String CONST_DB_USERNAME = "USERNAME";
	public static final String CONST_DB_PASSWORD = "PASSWORD";
	public static final String CONST_DB_CACHE_NAME = "PRESTO_CACHE";
	public static final String CONST_DB_CONNECTION_MIN_LIMIT = "MinLimit";
	public static final String CONST_DB_CONNECTION_MAX_LIMIT = "MaxLimit";
	public static final String CONST_DB_CONNECTION_INITIAL_LIMIT = "InitialLimit";
	public static final String CONST_DB_CONNECTION_INACTIVITY_TIMEOUT = "InactivityTimeout";
	public static final String CONST_DB_CONNECTION_ABONDENED_TIMEOUT = "AbandonedConnectionTimeout";
	// For Analysis Programs
	public static final String COMPLETED = "2";
	public static final String PARTIALLY_COMPLETED = "4";
	public static final int NULLID = -1;
	public static final char YES = 'Y';
	public static final char NO = 'N';
	public static final float ZEROLOWERLIMIT = -0.01f;
	public static final float ZEROUPPERLIMIT = 0.01f;
	public static final int INVALIDSTOREID = -1;
	public static final String NOT_CHECKED_PASS = "Items_Not_Checked_PASS";
	public static final String NOT_FOUND_PASS = "Items_Not_Found_PASS";
	public static final String AVG_ITEMS_PER_HR_PASS = "Avg_Items_Per_Hr_PASS";
	public static final String ITEMS_ON_SALE_PASS = "Items_On_Sale_PASS";
	public static final String RANGE_CHECK_PASS = "Out_Of_Range_PASS";
	public static final String REASONABILITY_CHECK_PASS = "Out_Of_Reasonability_PASS";
	public static final String ITEMS_FIXED_LATER_PASS = "Items_Fixed_Later_PASS";
	public static final String DB_DATE_FORMAT = "MM/DD/YYYY";
	public static final String APP_DATE_FORMAT = "MM/dd/yyyy";
	public static final String APP_LOCAL_DATE_FORMAT = "M/d/yyyy";
	public static final String APP_DATE_TIME_FORMAT = "MM/DD/yyyy HH24:mi:ss";
	public static final String APP_DATE_MMDDYYFORMAT = "MM/dd/yy";
	public static final String APP_DATE_YYYYMMDDFORMAT = "yyyy-MM-dd";
	public static final int R_SUCCESS = 0;
	public static final int PRICE_WENT_UP = 1;
	public static final int PRICE_WENT_DOWN = 2;
	public static final int PRICE_NO_CHANGE = 3;
	public static final int PRICE_NA = 4;
	public static int ASSORTMENT_SUMMARY_STORE_LEVEL = 1;
	public static int ASSORTMENT_SUMMARY_CATEGORY_LEVEL = 2;
	public static int ASSORTMENT_SUMMARY_DEPT_LEVEL = 3;
	// These two constants are used for early detection suspect analysis
	public static float NATIONAL_PRICE_ITEM_PCT = 0.9f;
	public static float POSSIBLE_PRICE_VARIATION = 0.3f;
	public static final int MIN_NUM_OF_ITEM_THRESHOLD = 75;
	public static final int MIN_NUM_OF_STORE_THRESHOLD = 4;
	public static final int MIN_NUM_OF_RECORDS_THRESHOLD = 4;
	public static final int MANUFACTURER_START_INDEX = 1;
	public static final int MANUFACTURER_END_INDEX = 6;
	// Constants for Store Relationship or Item Relationships
	public static final int RELATIONSHIP_TRUE = 0;
	public static final int NOT_ENOUGH_OBSERVATIONS = 1;
	public static final int RELATIONSHIP_BROKEN = 2;
	public static final String RELATIONSHIP_TRUE_STR = "RelationshipTrueCount";
	public static final String RELATIONSHIP_BROKEN_STR = "RelationshipBrokenCount";
	public static final String NOT_ENOUGH_OBSERVATIONS_STR = "NotEnoughRelationShipCount";
	public static final String OBS_CANDIDATES_STR = "OBS_CANDIDATES_STR";
	public static final String OBS_BROKEN_STR = "OBS_BROKEN_STR";
	public static final String OBS_TRUE_STR = "OBS_TRUE_STR";
	public static final String ITEM_RELATION_TRUE = "ITEM_RELATIONSHIPTRUE";
	public static final String ITEM_RELATION_BROKEN = "ITEM_RELATIONSHIPBROKEN";
	public static final String ITEM_RELATION_NO_HISTORY = "ITEM_RELATION_NO_HISTORY";
	public static final String ITEM_RELATION_OBS_CANDIDATES = "ITEM_RELATION_OBS_CANDIDATES";
	public static final String ITEM_RELATION_OBS_BROKEN = "ITEM_RELATION_OBS_BROKEN";
	public static final String ITEM_RELATION_OBS_TRUE_RELATION = "ITEM_RELATION_OBS_TRUE_RELATION";
	public static final String PRESTO_SYS_USER = "Presto_Insights";
	public static final String EXTERNAL_PRICECHECK_USER = "external_check";
	public static final String EXTERNAL_PRICECHECK_LIST = "EXTERNAL_CHECKLIST";
	public static final String PRESTO_LOAD_CHECKLIST = "PRESTO_CHECK";
	public static final String PRESTO_LOAD_USER = "pwebcheck";
	public static final String COMPLETED_FOLDER = "CompletedData";
	public static final String BAD_FOLDER = "BadData";
	public static final int MIN_RECS_FOR_OOS_ANALYSIS = 5;
	public static final int TIME_OF_DAY_ALLDAY = 4;

	// File Parser Constants

	public static String PLUS_CHARACTER = "\\+";
	public static String EMPTY = "";
	public static String INDEX_DELIMITER = "-";

	// Sales Aggregation Constants
	public final static int LOCATIONLEVELID = 0;
	public final static int SUBCATEGORYLEVELID = 3;
	public final static int ITEMLEVELID = 1;
	public final static int CATEGORYLEVELID = 4;
	public final static int DEPARTMENTLEVELID = 5;
	public static final int FINANCEDEPARTMENT = 6;
	public static final int MERCHANTISEDEPARTMENT = 7;
	public static final int PORTFOLIO = 8;
	public static final int SECTOR = 9;
	public static final int ALLPRODUCTS = 99;
	public static final int RECOMMENDATIONUNIT = 7;
	// ??? if acceptable move to where product levels are defined
	public static final int PRODUCT_LEVEL_ID_LIG = 11;

	// pos department
	public static final int POSDEPARTMENT = 10;
	public static final int GASPOSDEPARTMENT = 37;
	public final static int SEGMENTLEVELID = 2;

	public static final String CALENDAR_DAY = "D";
	public static final String CALENDAR_WEEK = "W";
	public static final String CALENDAR_PERIOD = "P";
	public static final String CALENDAR_QUARTER = "Q";
	public static final String CALENDAR_YEAR = "Y";

	// location level constants
	public final static int STORE_LEVEL_ID = 5;
	public static final int DISTRICT_LEVEL_ID = 4;
	public static final int REGION_LEVEL_ID = 3;
	public static final int DIVISION_LEVEL_ID = 2;
	public static final int CHAIN_LEVEL_ID = 1;
	public static final int ZONE_LEVEL_ID = 6;
	public static final int STORE_LIST_LEVEL_ID = 7;
	public static final int COMP_STORE_LIST_LEVEL_ID = 8;

	// ??? if acceptable move to where location levels are defined
	public static final int lOCATION_LEVEL_ID_MARKETAREA = 11;

	// Sales Analysis data process types
	public static final String PROCESS_DATA_FULL = "FULL";
	public static final String PROCESS_DATA_NORMAL = "NORMAL";

	// CTD Constants
	public static final int CTD_DAY = 1;
	public static final int CTD_WEEK = 2;
	public static final int CTD_PERIOD = 3;
	public static final int CTD_QUARTER = 4;
	public static final int CTD_YEAR = 5;

	// Retail Price Setup
	public static final int CHAIN_LEVEL_TYPE_ID = 0;
	public static final int ZONE_LEVEL_TYPE_ID = 1;
	public static final int STORE_LEVEL_TYPE_ID = 2;
	public static final int LIMIT_COUNT = 1000;
	public static final int BATCH_UPDATE_COUNT = 1000;
	public static final int RECS_TOBE_PROCESSED = 100000;
	public static final String REG_FLAG = "N";
	public static final String SALES_FLAG = "Y";
	public static final String CURRENT_WEEK = "currentweek";
	public static final String NEXT_WEEK = "nextweek";
	public static final String SPECIFIC_WEEK = "specificweek";
	public static final String LAST_WEEK = "lastweek";

	// Retail Cost Setup
	public static final String UPDATE_FLAG = "U";
	public static final int MAX_ITEMS_ALLOWED = 5000;
	public static final int MAX_RET_ITEM_CODE = 500000;
	public static final int LOG_RECORD_COUNT = 25000;
	public static final String IGN_STR_NOT_IN_DB_TRUE = "TRUE";
	public static final String VALUE_TRUE = "TRUE";

	// Competitive Data PI Setup
	public static final String PI_SETUP_FULL = "PI_SETUP_FULL";
	public static final String PI_SETUP_PARTIAL = "PI_SETUP_PARTIAL";
	public static final String PI_SETUP_ITEMS = "ALL";
	public static final int LIST_SIZE_LIMIT = 150000;

	// 12 Week Movement Computation
	public static final String SCHEDULE_IDS = "%SCHEDULE_IDS%";
	public static final String TARGET_TABLE = "%TARGET_TABLE%";

	public static final String DAILY = "DAILY";
	public static final String WEEKLY = "WEEKLY";

	// TOPS Cost/Billback Dataload
	public static final String FILE_TYPE_COST_DATA = "COST_DATA";
	public static final String FILE_TYPE_BILLBACK_DATA = "BILLBACK_DATA";

	public static final String RECORD_TYPE_ADDED = "A";
	public static final String RECORD_TYPE_UPDATED = "C";
	public static final String RECORD_TYPE_DELETED = "D";

	public static final String ITEM_STATUS_ACTIVE = "A";
	public static final String ITEM_STATUS_INACTIVE = "I";

	public static final String DATA_LOAD_MODE = "MODE=";
	public static final String IGN_COPY_EXISTING_DATA = "IGN_COPY_EXIST=";
	public static final String IGN_CLR_SALE_PRICE = "IGN_CLR_SALE_PRICE=";
	public static final String DATA_LOAD_FULL = "FULL";
	public static final String DATA_LOAD_DELTA = "DELTA";

	// Revenue Correction
	public static final String DAILY_REVENUE_CORRECTION_MODE = "DailyRevenueCorrection";
	public static final String WEEKLY_REVENUE_CORRECTION_FD = "WeeklyFinanceDeptCorrection";
	public static final String WEEKLY_REVENUE_CORRECTION_STORE = "WeeklyStoreCorrection";
	public static final String ROLLUP_TABLENAME_DISTRICT = "RETAIL_DISTRICT";
	public static final String ROLLUP_COLUMNNAME_STORE = "COMP_STR_NO";
	public static final String ROLLUP_COLUMNNAME_STOREID = "COMP_STR_ID";
	public static final String ROLLUP_COLUMNNAME_DISTRICT = "DISTRICT_ID";
	public static final String ROLLUP_TABLENAME_REGION = "RETAIL_REGION";
	public static final String ROLLUP_COLUMNNAME_REGION = "REGION_ID";
	public static final String ROLLUP_TABLENAME_DIVISION = "RETAIL_DIVISION";
	public static final String ROLLUP_COLUMNNAME_DIVISION = "DIVISION_ID";
	public static final String REVENUE_COLUMN = "TOT_REVENUE";
	public static final String AVGORDERSIZE_COLUMN = "AVG_ORDER_SIZE";
	public static final String TOTVISITCNT_COLUMN = "TOT_VISIT_CNT";
	public static final String ID_REVENUE_COLUMN = "ID_TOT_REVENUE";
	public static final String ID_AVGORDERSIZE_COLUMN = "ID_AVG_ORDER_SIZE";
	public static final String ID_TOTVISITCNT_COLUMN = "ID_TOT_VISIT_CNT";
	// Changes for updating Revenue, Visit Count based on configuration parameters
	public static final String WEEKLY_CORRECTION = "WEEKLY_REVENUE_CORRECTION.WEEKLY_CORRECTION";
	public static final String REVENUVE_CORRECTION = "REVENUE";
	public static final String VISITCOUNT_CORRECTION = "VISITCOUNT";
	// Changes for updating Revenue, Visit Count based on configuration parameters -
	// Ends

	// Pos-Gas Department
	public static final int POS_GASDEPARTMENT = 37;

	// Pos promo department
	public static final int POS_PROMODEPARTMENT = 50;

	// Pricing Alerts
	public static final int DEFAULT_NA = -9999;
	public static final String DEFAULT_NA_STRING = "NA";
	public static final String LIG_ALERT_CODES = "LIG001,LIG002";
	public static final int COMP_LOCATION_TYPE_PRIMARY = 1;
	public static final int COMP_LOCATION_TYPE_SECONDARY = 2;
	public static final char COMP_LOCATION_TYPE_P = 'P';
	public static final char COMP_LOCATION_TYPE_S = 'S';
	public static final int _13WEEK = 91;

	// Display Types
	public static final String DISPLAY_TYPE_FASTWALL = "FAST WALL";
	public static final String DISPLAY_TYPE_END = "END";
	public static final String DISPLAY_TYPE_END_DSD = "End - DSD";
	public static final String DISPLAY_TYPE_WING_DSD = "Wing - DSD";
	public static final String DISPLAY_TYPE_WING = "WING";
	public static final String DISPLAY_TYPE_SHIPPER = "SHIPPER";
	public static final String DISPLAY_TYPE_BREADTABLE = "BREAD TABLE";
	public static final String DISPLAY_TYPE_LOBBY = "LOBBY";
	public static final String DISPLAY_TYPE_JUMPSHELF = "JUMPSHELF";
	public static final String DISPLAY_TYPE_COMBOEND = "COMBO END";
	public static final String DISPLAY_TYPE_MOD = "MOD";

	// Promo Types
	public static final int PROMO_TYPE_BOGO = 1;
	public static final int PROMO_TYPE_BXGY_SAME = 2;
	public static final int PROMO_TYPE_MUST_BUY = 3;
	public static final int PROMO_TYPE_MEAL_DEAL = 5;
	public static final int PROMO_TYPE_BXGY_DIFF = 8;
	public static final int PROMO_TYPE_STANDARD = 9;
	public static final int PROMO_TYPE_CATALINA = 101;
	public static final int PROMO_TYPE_SUPER_COUPON = 102;
	public static final int PROMO_TYPE_EBONUS_COUPON = 103;
	public static final int PROMO_TYPE_GAS_POINTS = 104;
	public static final int PROMO_TYPE_IN_AD_COUPON = 105;
	public static final int PROMO_TYPE_BMSM = 106;
	public static final int PROMO_TYPE_DIGITAL_COUPON = 107;
	public static final int PROMO_TYPE_UNKNOWN = 99999;

	// Offer Types
	public static final int OFFER_TYPE_FREE_ITEM = 1;
	public static final int OFFER_TYPE_REWARD_PTS = 2;
	public static final int OFFER_TYPE_PLUS_UP = 3;
	public static final int OFFER_TYPE_COUPON = 4;
	public static final int OFFER_TYPE_GAS_POINTS = 5;
	public static final int OFFER_TYPE_INSTANT_SAVINGS = 6;
	public static final int OFFER_TYPE_OFF = 7;

	// Offer Unit Types
	public static final String OFFER_UNIT_TYPE_PERCENTAGE = "P";
	public static final String OFFER_UNIT_TYPE_NUMBER = "N";
	public static final String OFFER_UNIT_TYPE_DOLLAR = "D";
	// Added for RiteAid
	public static final String OFFER_UNIT_TYPE_STANDARD = "F";

	// Overlay File Loading
	public static final String PROMO_OVERVIEW_TYPE_COMBO_DEAL = "Combo Deal";
	public static final String PROMO_OVERVIEW_TYPE_MUST_BUY = "Must Buy";
	public static final String PROMO_OVERVIEW_TYPE_CATALINA = "Catalina";
	public static final String PROMO_OVERVIEW_TYPE_INSTANT_SAVINGS = "Instant $ Savings";
	public static final String PROMO_OVERVIEW_TYPE_SUPER_COUPON = "Super Coupon";
	public static final String PROMO_OVERVIEW_TYPE_EBONUS_COUPON = "e-Bonus Coupon";
	public static final String PROMO_OVERVIEW_TYPE_MEAL_DEAL = "Meal Deal";
	public static final String PROMO_OVERVIEW_TYPE_GAS_POINTS = "GasPoints";

	public static final String PROMO_OVERVIEW_SHEETNAME = "Promo Overview";
	public static final String PROMO_SHEETNAME = "Circular_Data_Table";
	public static final String PROMO_PARTICIPATING_PRODUCTS_SHEET_NAME = "Participating Products";
	public static final String BATCH_USER = "BATCH";
	public static final int ITEM_LEVEL_CHILD_PRODUCT_LEVEL_ID = 1;
	public static final int SEGMENT_LEVEL_PRODUCT_LEVEL_ID = 2;
	// TOPS Store Lists
	public static final String CHAIN = "CHAIN";
	public static final String TOPS_STORES = "TOPS WITHOUT GU";
	public static final String GU_STORES = "GRAND UNION STORES";

	// Distribution types
	public static final char WAREHOUSE = 'W';
	public static final char DSD = 'D';

	// Warehouse Vendor
	public static final String WAREHOUSE_VENDOR_NUMBER = "000000";
	public static final String WAREHOUSE_VENDOR_NAME = "WAREHOUSE";
	public static final int DEFAULT_DSD_VENDOR_ID = 1;
	public static final String COST_INDICATOR = "Y";
	public static final String COST_INDICATOR_N = "N";
	public static final int RECS_PROCESSED = 10000;

	// Ahold tlog timestamp length
	public static final int LENGTH_TRX_TIMESTAMP_WITH_SECONDS = 14;

	// Default vendor id for warehouse items.
	public static final int DEFAULT_VENDOR_ID = 1;

	// Authorization flags
	public static final String AUTHORIZED_ITEM = "Y";
	public static final String UN_AUTHORIZED_ITEM = "N";

	// Price indicators
	public static final String PRICE_INDICATOR = "Y";
	public static final String PRICE_INDICATOR_N = "N";

	public static final String KVI = "KVI";
	public static final String K2 = "K2";

	// Sub display id default
	public static final int SUB_DISPLAY_ID_NONE = 1;

	// Zone list level type id
	public static final int ZONE_LIST_LEVEL_ID = 21;

	// Loyalty Card Type.
	public static final int LOYALTY_CARD_TYPE_PHONE = 1;
	public static final int LOYALTY_CARD_TYPE_CARD = 2;
	public static final int LOYALTY_CARD_TYPE_EMAIL = 3;

	// mail signature
	public static final String MAIL_CONFIDENTIAL_NOTE = "This email communication including its attachments is intended "
			+ "only for the use of the individuals to whom it has been addressed. It may contain information that is "
			+ "privileged and confidential to Pristine Infotech, Inc. If the reader of this message is not the intended "
			+ "recipient, you are hereby notified that any dissemination, distribution or copying of this communication "
			+ "is strictly prohibited. If you have read this communication in error, please notify us immediately by return e-mail.";

	// Calendar types
	public static final String CAL_TYPE_BUSINESS = "F";
	public static final String CAL_TYPE_MARKETING = "M";
	public static final String CAL_TYPE_BOTH = "B";

	// PI Calc Type
	public static final String PI_CALC_TYPE_REVERSE = "REVERSE";

	// Product Group Unused sequence
	public static final int MAX_PRODUCT_ID_SEQUENCE = 9999;

	// Lir item code sequence
	public static final long MAX_LIR_ITEM_CODE_SEQUENCE = 999999;
	public static final long MIN_LIR_ITEM_CODE_SEQUENCE = 920000;

	// Giant Eagle Coupon Type
	public static final String MANUFACTURE_COUPON_TYPE = "MF";

	public static final String DECIMAL = ".00";

	public static final String GE_PRC_GRP_CD_DSD = "5";
	public static final String GE_PRC_GRP_CD_DSD1 = "10";

	public static final String GE_AUTH_CD_M = "M";
	public static final String GE_AUTH_CD_Y = "Y";
	public static final String GE_AUTH_CD_S = "S";
	public static final String GE_AUTH_CD_R = "R";

	public static final String ZONE_TYPE_W = "W";
	public static final String ZONE_TYPE_V = "V";

	public static final int PERISHABLE_S_ORDER_M_SELL = 1;
	public static final int PERISHABLE_M_ORDER_S_SELL = 2;

	public static int UOM_LB = 1;
	public static int UOM_OZ = 2;
	public static int UOM_CT = 3;
	public static int UOM_ML = 4;
	public static int UOM_DZ = 5;
	public static int UOM_QT = 7;
	public static int UOM_EA = 13;
	public static int UOM_GA = 14;
	public static int UOM_SQF = 15;
	public static int UOM_PT = 16;
	public static int UOM_PK = 18;
	public static int UOM_GR = 20;
	public static int UOM_LT = 21;
	public static int UOM_FZ = 23;
	public static int UOM_PIR = 24;
	public static int UOM_FT = 25;
	public static int UOM_DOS = 26;
	public static int UOM_ROL = 27;
	public static int UOM_CF = 28;
	public static int UOM_IN = 29;
	public static int UOM_YD = 30;
	public static int UOM_CTN = 31;
	public static int UOM_TBLSP = 46;
	public static int UOM_TSP = 47;
	public static int UOM_CUP = 48;

	public static int UOM_MINOR_3_4 = 1;
	public static int UOM_MINOR_2_3 = 2;
	public static int UOM_MINOR_1_2 = 3;
	public static int UOM_MINOR_1_3 = 4;
	public static int UOM_MINOR_1_4 = 5;

	public static double Unit_Conv_LB2OZ = 16;
	public static double Unit_Conv_OZ2OZ = 1;
	public static double Unit_Conv_ML2OZ = 0.0338140225589;
	public static double Unit_Conv_QT2OZ = 32;
	public static double Unit_Conv_GA2OZ = 128;
	public static double Unit_Conv_PT2OZ = 16;
	public static double Unit_Conv_GR2OZ = 0.0352739619;
	public static double Unit_Conv_LT2OZ = 33.8140227;
	public static double Unit_Conv_FZ2OZ = 1;
	public static double Unit_Conv_CUP2OZ = 8;
	public static double Unit_Conv_TBLSP2OZ = 0.5;
	public static double Unit_Conv_TSP2OZ = 0.166666666666667;

	public static double UOM_MINOR_VAL_3_4 = 0.75;
	public static double UOM_MINOR_VAL_2_3 = 0.6666666;
	public static double UOM_MINOR_VAL_1_2 = 0.5;
	public static double UOM_MINOR_VAL_1_3 = 0.3333333;
	public static double UOM_MINOR_VAL_1_4 = 0.25;

	// Promo sub types
	public static final int PROMO_SUB_TYPE_BOGO_X_PCT_OFF = 1;
	public static final int PROMO_SUB_TYPE_BXGY_SAME_X_PCT_OFF = 2;
	public static final int PROMO_SUB_TYPE_STANDARD_PCT_OFF = 3;
	public static final int PROMO_SUB_TYPE_STANDARD_DOLLAR_OFF = 4;
	public static final int PROMO_SUB_TYPE_IN_AD_COUPON = 5;
	public static final int PROMO_SUB_TYPE_BOGO_X_PCT_OFF_WITH_OVERLAY = 6;
	public static final int PROMO_SUB_TYPE_BXGY_SAME_X_PCT_OFF_WITH_OVERLAY = 7;
	public static final int PROMO_SUB_TYPE_STANDARD_PCT_OFF_WITH_OVERLAY = 8;
	public static final int PROMO_SUB_TYPE_STANDARD_DOLLAR_OFF_WITH_OVERLAY = 9;
	public static final int PROMO_SUB_TYPE_OVERLAY = 10;
	public static final int PROMO_SUB_TYPE_BOGO_WITH_OVERLAY = 11;
	public static final int PROMO_SUB_TYPE_BXGY_SAME_WITH_OVERLAY = 12;
	public static final int PROMO_SUB_TYPE_MUST_BUY_WITH_OVERLAY = 13;
	public static final int PROMO_SUB_TYPE_BOGO_X_DOLLAR_OFF = 14;
	public static final int PROMO_SUB_TYPE_BXGY_SAME_X_DOLLAR_OFF = 15;
	public static final int PROMO_SUB_TYPE_BMSM_PCT_OFF = 16;
	public static final int PROMO_SUB_TYPE_BMSM_DOLLAR_OFF = 17;
	public static final int PROMO_SUB_TYPE_MUST_BUY_PCT_OFF = 18;
	public static final int PROMO_SUB_TYPE_MUST_BUY_DOLLAR_OFF = 19;
	/*
	 * public static final int PROMO_TYPE_BXGY_SAME = 2; public static final int
	 * PROMO_TYPE_MUST_BUY = 3; public static final int PROMO_TYPE_MEAL_DEAL = 5;
	 * public static final int PROMO_TYPE_BXGY_DIFF = 8; public static final int
	 * PROMO_TYPE_STANDARD = 9; public static final int PROMO_TYPE_CATALINA = 101;
	 * public static final int PROMO_TYPE_SUPER_COUPON = 102; public static final
	 * int PROMO_TYPE_EBONUS_COUPON = 103; public static final int
	 * PROMO_TYPE_GAS_POINTS = 104; public static final int PROMO_TYPE_IN_AD_COUPON
	 * = 105; public static final int PROMO_TYPE_UNKNOWN = 99999;
	 */

	// LOCATION

	public static final String ZONE = "ZONE";
	public static final String DIVISION = "DIVISION";
	public static final String STORELEVEL = "STORE";

	public static final String RETAIL_CALENDAR_BUSINESS = "B";
	public static final String RETAIL_CALENDAR_PROMO = "P";

	public static final String DIGITAL_COUPON = "DIGITAL";
	public static final String NORMAL_COUPON = "COUPON";
	public static final String AD_COUPON = "AD";

	public static final int ERRORCODE_LONG_TERM_PROMO = 1;
	public static final int ERRORCODE_NEGATIVE_DISCOUNTS = 2;

	// Added for RA promoFile merger
	public static final String COMPLETE = "COMP";
	public static final String ACTIVE = "ACTV";
	public static final String INPROGRESS = "INPR";
	public static final String CANCELLED = "CANC";

	// Added for RA Competitor File
	public static final String PROMO = "PROMO";
	public static final String NONPROMO = "NON-PROMO";
	public static final String TOTAL = "TOTAL";
	public static final String DONE = "done";

	// for price export
	public static final String HARD_PART_ITEMS = "H";
	public static final String SALE_FLOOR_ITEMS = "S";
	public static final String CLEARANCE = "C";
	public static final String EMERGENCY = "E";
	public static final int APPROVED_BY = 1;
	public static final int SUNDAY = 1;
	public static final int MONDAY = 2;
	public static final int TUESDAY = 3;
	public static final int WEDNESDAY = 4;
	public static final int THURSDAY = 5;
	public static final int FRIDAY = 6;
	public static final int SATURDAY = 7;
	public static final String NORMAL = "N";
	public static final String REGULAR = "R";
	public static final String EMERGENCY_OR_HARDPART = "EH";
	public static final String EMERGENCY_OR_SALESFLOOR = "ES";
	public static final String BOTH_SALESFLOOR_AND_HARDPART = "SH";
	public static final String ALL_SALESFLOOR_AND_HARDPART_AND_EMERGENCY = "ESH";
	public static final int APPROVE_STATUS_CODE = 6;	
	public static final String EXPORT_ADD_DATA = "A";
	public static final String EXPORT_DELETE_DATA = "D";
	
	//Added for Autozone Delta Loader
	public static final String IGN_STR = "TRUE";
	
    public static final String LOCKED_RETAIL="26";
    public static final String MIN_MAX="27";
    public static final String STORE_LOCK="25";
	public static final String CLEARANCE_LIST_TYPE = "29";
	public static final String EMERGENCY_LIST_TYPE = "28";
	public static final int PARTIALLY_EXPORTED_STATUS_CODE = 7;
	public static final int EXPORTED_STATUS_CODE = 8;
	public static final String  STORE_INVENTORY = "STORE_INVENTORY";
	public static final String X_WEEKS_MOV = "52";
	public static final String AZ_GLOBAL_ZONE = "1000";
	public static final int ITEM_PRODUCT_LEVEL_ID = 1;
	
	//Added for Store File Export 
	public static final String STORE_LOCK_ITEMS="STORE_LOCK_ITEMS";
	public static final String STORE_LOCK_ITEMS_WITH_PRICE="STORE_LOCK_ITEMS_WITH_PRICE";
	
	//Added for FF to fetch regular retail or clreance retail
	public static final int REGULAR_PRICE_ID=1;
	public static final int CLEARANCE_PRICE_ID=2;
	public static final int REGULAR_COST_ID=1;
	
}
