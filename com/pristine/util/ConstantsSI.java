/*
 * Author : Suresh Start Date : Jul 22, 2009
 * 
 * Change Description Changed By Date
 * -------------------------------------------------------------- Added few more
 * constants Naimish 23rd July 2009
 */

package com.pristine.util;

public class ConstantsSI
{
	public static final String	CONST_DB_CACHE_NAME								= "PRESTO_CACHE";
	public static final String	CONST_DB_CONNECTION_MIN_LIMIT					= "MinLimit";
	public static final String	CONST_DB_CONNECTION_MAX_LIMIT					= "MaxLimit";
	public static final String	CONST_DB_CONNECTION_INITIAL_LIMIT				= "InitialLimit";
	public static final String	CONST_DB_CONNECTION_INACTIVITY_TIMEOUT			= "InactivityTimeout";
	public static final String	CONST_DB_CONNECTION_ABONDENED_TIMEOUT			= "AbandonedConnectionTimeout";

	// SI_ANALYSIS_CONSTANTS
	public static final String	CONST_SI_ANALYSIS_NO_SI							= "No Suspect Items Found";
	public static final String	CONST_SI_ANALYSIS_SUCCESS						= "Success";
	public static final String	CONST_SI_ANALYSIS_FAILURE						= "Failure";
	public static final String	CONST_SI_ANALYSIS_COMPETITIVE_DATA_NOT_FOUND	= "No Competive Data Found.";
	public static final String	CONST_SI_ANALYSIS_SCHEDULE_DOESNOT_EXIST		= "Schedule Doesn't Exist.";
	public static final String	CONST_SI_ANALYSIS_GPS_VIOLATION					= "GPS Violation";

	// Misc
	public static final int	INVALID_ITEM_CODE			= -1;
	public static final int	INVALID_ID					= -1;
	public static final int	DEFAULT_SALE_WEEK_OFFSET 	= 0;
	public static final int	DEFAULT_REGULAR_WEEK_OFFSET = 0;

	public enum CHANGE_DIRECTION
	{
		DUMMY,	// DUMMY needed because WENTUP should begin from 1 
		WENTUP, WENTDOWN, NOCHANGE, NOTAPPLICABLE, WENT_ON_SALE, WENT_OFF_SALE
	}

	public static final int	UNIQUE_PRICE_POINTS	= 1;

	public enum RULES
	{
		// Detection rules
		UNIQUE_PRICE_POINTS, HISTORICAL_AVERAGE, PRICE_CHANGE_OVER_PERIOD, PRICE_CHANGE_FREQUENCY, PRICE_ENDING, SALE_TO_REGULAR_TRANSITION, REGULAR_PRICE_CHANGE_DURING_SALE, GROUP_ITEM_PRICE_NOT_CHANGED, GROUP_ITEM_PRICE_CHANGED,
		REGULAR_PRICE_CHANGE_IN_OPPOSITE_DIRECTION, DIFFERENT_PRICE_CHANGE_PERCENTAGE, ON_SALE_ELSEWHERE, STILL_ON_SALE, SALE_REGULAR_PERCENTAGE_DIFFERENCE, ITEM_NOT_FOUND,
		
		// Removal rules
		WITHIN_HISTORICAL_AVERAGE,
		ITEMS_PRICE_CHANGED_EVERYWHERE, ITEMS_PRICE_NOT_CHANGED_EVERYWHERE, SAME_ON_OFF_BEHAVIOUR_EVERYWHERE, COMPARISON_WITH_LAST_PRICE, PRICE_SAME_EVERYWHERE,
		
		// Price suggestion rules
		SUGGESTION_GROUP_RELATIONSHIP, SUGGESTION_CONSISTENT_FREQUENCY, SUGGESTION_SALE_LAST_WEEK, SUGGESTION_PRICE_TRUNCATION,
		SUGGESTION_PRICE_OF_ANOTHER_STORE_ITEM_CATEGORY
		;
	}
}
