
package com.pristine.dto;

import java.util.HashMap;

import com.pristine.dao.RulesDAO;

public class ConfigSettingDTO
{
	private int		_UniqueStoresRegularPrice					= 0;
	private int		_UniqueStoresSalePrice						= 0;
	private float	_RuleThreshold								= 0.2f;
	private int		_PriceChangePeriodMinNonzeroPercentEntries	= 2;
	private int		_PriceEndingObservations					= 4;
	private int		_ItemPriceDifferenceInPercent				= 0;
	private int		_MaxItemsPerPriceCheck						= 0;
	private int		_RegularPriceChangeMinumum					= 0;
	private int		_PriceEndPoints								= 0;
	private int		_LastObservedPriceWithinWeeks				= 0;
	private float	_HighPercentageThreshold					= 0.75f;
	private int		_MinOtherItems								= 0;
	private int		_ItemNotFoundLastPriceChecks				= 0;
	private int		_UniqueStoresDays							= 0;
	private int 	_UniqueStoresDaysSale 						= 0;
	private int		_UniqueRegularPrices						= 0;
	private int		_UniqueSalePrices							= 0;
	private int		_PriceChangeOverWeeks						= 0;
	private int		_UniquePPMonths								= 0;
	private int		_RemovalUniqueStoreMinimum					= 0;
	private int		_RemovalUniqueStoreDays						= 0;
	private int		_RemovalUniqueStoreDaysSale			    	= 0;
	private int		_UniquePPOccurences							= 0;
	private float 	_HighPercentageThresholdRemoval 			= 0.4f;
	private int		_PriceEndingFirstPass						= 1;
	private int		_PriceEndingSecondPass						= 98;
	
	public ConfigSettingDTO(HashMap<String, String>	configSettingsMap)
	{
		_UniqueStoresRegularPrice					= (configSettingsMap.get("UNIQUE_STORES_BY_RPRICE") != null) ? Integer.parseInt(configSettingsMap.get("UNIQUE_STORES_BY_RPRICE")) : 0;
		_UniqueStoresSalePrice						= (configSettingsMap.get("UNIQUE_STORES_BY_SPRICE") != null) ? Integer.parseInt(configSettingsMap.get("UNIQUE_STORES_BY_SPRICE")) : 0;
		_RuleThreshold								= (configSettingsMap.get("RULES_THRESHOLD") != null) ? Float.parseFloat(configSettingsMap.get("RULES_THRESHOLD")) : 0;
		_PriceChangePeriodMinNonzeroPercentEntries	= (configSettingsMap.get("PRICE_CHANGE_PERIOD_MIN_NON_ZERO_PERCENT_ENTRY") != null) ? Integer.parseInt(configSettingsMap.get("PRICE_CHANGE_PERIOD_MIN_NON_ZERO_PERCENT_ENTRY")) : 0;
		_PriceEndingObservations					= (configSettingsMap.get("PRICE_ENDING_OBSERVATIONS") != null) ? Integer.parseInt(configSettingsMap.get("PRICE_ENDING_OBSERVATIONS")) : 0;
		_ItemPriceDifferenceInPercent				= (configSettingsMap.get("ITEM_PRICE_DIFFERENCE_IN_PERCENT") != null) ? Integer.parseInt(configSettingsMap.get("ITEM_PRICE_DIFFERENCE_IN_PERCENT")) : 0;
		_MaxItemsPerPriceCheck						= (configSettingsMap.get("MAX_ITEMS_PER_PRICE_CHECK") != null) ? Integer.parseInt(configSettingsMap.get("MAX_ITEMS_PER_PRICE_CHECK")) : 0;
		_RegularPriceChangeMinumum					= (configSettingsMap.get("REGULAR_PRICE_CHANGE_MINIMUM") != null) ? Integer.parseInt(configSettingsMap.get("REGULAR_PRICE_CHANGE_MINIMUM")) : 0;
		_PriceEndPoints								= (configSettingsMap.get("PRICE_END_POINTS") != null) ? Integer.parseInt(configSettingsMap.get("PRICE_END_POINTS")) : 0;
		_LastObservedPriceWithinWeeks				= (configSettingsMap.get("LAST_OBSERVED_PRICE_WITHIN_WEEKS") != null) ? Integer.parseInt(configSettingsMap.get("LAST_OBSERVED_PRICE_WITHIN_WEEKS")) : 0;
		_HighPercentageThreshold					= (configSettingsMap.get("HIGH_PERCENTAGE_THRESHOLD") != null) ? Float.parseFloat(configSettingsMap.get("HIGH_PERCENTAGE_THRESHOLD")) : 0;
		_MinOtherItems								= (configSettingsMap.get("MIN_OTHER_ITEMS") != null) ? Integer.parseInt(configSettingsMap.get("MIN_OTHER_ITEMS")) : 0;
		_ItemNotFoundLastPriceChecks				= (configSettingsMap.get("ITEM_NOT_FOUND_LAST_PRICE_CHECKS") != null) ? Integer.parseInt(configSettingsMap.get("ITEM_NOT_FOUND_LAST_PRICE_CHECKS")) : 0;
		_UniqueStoresDays							= (configSettingsMap.get("UNIQUE_STORES_DAYS") != null) ? Integer.parseInt(configSettingsMap.get("UNIQUE_STORES_DAYS")) : 0;
		_UniqueStoresDaysSale 						= (configSettingsMap.get("UNIQUE_STORES_DAYS_SALE") != null) ? Integer.parseInt(configSettingsMap.get("UNIQUE_STORES_DAYS_SALE")) : 0;
		_UniqueRegularPrices						= (configSettingsMap.get("UNIQUE_REGULAR_PRICES") != null) ? Integer.parseInt(configSettingsMap.get("UNIQUE_REGULAR_PRICES")) : 0;
		_UniqueSalePrices							= (configSettingsMap.get("UNIQUE_SALE_PRICES") != null) ? Integer.parseInt(configSettingsMap.get("UNIQUE_SALE_PRICES")) : 0;
		_PriceChangeOverWeeks						= (configSettingsMap.get("PRICE_CHANGE_OVER_WEEKS") != null) ? Integer.parseInt(configSettingsMap.get("PRICE_CHANGE_OVER_WEEKS")) : 0;
		_UniquePPMonths								= (configSettingsMap.get("UNIQUE_PP_MONTHS") != null) ? Integer.parseInt(configSettingsMap.get("UNIQUE_PP_MONTHS")) : 0;
		_RemovalUniqueStoreMinimum					= (configSettingsMap.get("REMOVAL_UNIQUE_STORE_MINIMUM") != null) ? Integer.parseInt(configSettingsMap.get("REMOVAL_UNIQUE_STORE_MINIMUM")) : 0;
		_RemovalUniqueStoreDays						= (configSettingsMap.get("REMOVAL_UNIQUE_STORE_DAYS") != null) ? Integer.parseInt(configSettingsMap.get("REMOVAL_UNIQUE_STORE_DAYS")) : 0;
		_RemovalUniqueStoreDaysSale			    	= (configSettingsMap.get("REMOVAL_UNIQUE_STORE_DAYS_SALE") != null) ? Integer.parseInt(configSettingsMap.get("REMOVAL_UNIQUE_STORE_DAYS_SALE")) : 0;
		_UniquePPOccurences							= (configSettingsMap.get("UNIQUE_PP_OCCURENCES") != null) ? Integer.parseInt(configSettingsMap.get("UNIQUE_PP_OCCURENCES")) : 0;
		_HighPercentageThresholdRemoval 			= (configSettingsMap.get("REMOVAL_HIGH_PERCENTAGE_THRESHOLD") != null) ? Float.parseFloat(configSettingsMap.get("REMOVAL_HIGH_PERCENTAGE_THRESHOLD")) : 0;
		_PriceEndingFirstPass						= (configSettingsMap.get("PRICE_END_MIN_PCT_FIRSTPASS") != null) ? Integer.parseInt(configSettingsMap.get("PRICE_END_MIN_PCT_FIRSTPASS")) : 1;
		_PriceEndingSecondPass						= (configSettingsMap.get("PRICE_END_MIN_PCT_SECONDPASS") != null) ? Integer.parseInt(configSettingsMap.get("PRICE_END_MIN_PCT_SECONDPASS")) : 98;
	}

	//
	// Getters
	//
	public int getPriceEndingFirstPass()
	{
		return _PriceEndingFirstPass;
	}
	
	public int getPriceEndingSecondPass()
	{
		return _PriceEndingSecondPass;
	}
	
	public int getRemovalUniqueStoreDaysSale()
	{
		return _RemovalUniqueStoreDaysSale;
	}

	public int getUniqueStoresDaysSale() {
		return _UniqueStoresDaysSale;
	}

	public float getHighPercentageThresholdRemoval() {
		return _HighPercentageThresholdRemoval;
	}

	public int getUniquePPOccurences()
	{
		return _UniquePPOccurences;
	}

	public int getRemovalUniqueStoreMinimum()
	{
		return _RemovalUniqueStoreMinimum;
	}

	public int getRemovalUniqueStoreDays()
	{
		return _RemovalUniqueStoreDays;
	}

	public int getUniquePPMonths()
	{
		return _UniquePPMonths;
	}

	public int getUniqueSalePrices()
	{
		return _UniqueSalePrices;
	}

	public int getPriceChangeOverWeeks()
	{
		return _PriceChangeOverWeeks;
	}

	public int getUniqueRegularPrices()
	{
		return _UniqueRegularPrices;
	}

	public int getUniqueStoresDays()
	{
		return _UniqueStoresDays;
	}

	public int getItemNotFoundLastPriceChecks()
	{
		return _ItemNotFoundLastPriceChecks;
	}

	public int getMinOtherItems()
	{
		return _MinOtherItems;
	}

	public float getHighPercentageThreshold()
	{
		return _HighPercentageThreshold;
	}

	public int getLastObservedPriceWithinWeeks()
	{
		return _LastObservedPriceWithinWeeks;
	}

	public int getPriceEndPoints()
	{
		return _PriceEndPoints;
	}

	public int getRegularPriceChangeMinumum()
	{
		return _RegularPriceChangeMinumum;
	}

	public int getUniqueStoresRegularPrice()
	{
		return _UniqueStoresRegularPrice;
	}

	public int getUniqueStoresSalePrice()
	{
		return _UniqueStoresSalePrice;
	}

	public float getRuleThreshold()
	{
		return _RuleThreshold;
	}

	public int getPriceChangePeriodMinNonzeroPercentEntries()
	{
		return _PriceChangePeriodMinNonzeroPercentEntries;
	}

	public int getPriceEndingObservations()
	{
		return _PriceEndingObservations;
	}

	public int getItemPriceDifferenceInPercent()
	{
		return _ItemPriceDifferenceInPercent;
	}

	public int getMaxItemsPerPriceCheck()
	{
		return _MaxItemsPerPriceCheck;
	}
}
