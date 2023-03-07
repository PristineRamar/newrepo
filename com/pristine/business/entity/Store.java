package com.pristine.business.entity;

import java.util.HashMap;
import java.util.Iterator;

import com.pristine.exception.GeneralException;
import com.pristine.util.ConstantsSI;
import com.pristine.util.GenericUtil;
import com.pristine.util.NumberUtil;
import com.pristine.dto.CompetitiveDataDTO;

public class Store
{
	public Store (SIApplication app, int storeId)
	{
		_Application = app;
		_StoreId = storeId;
	}

	public Store (SIApplication app, Chain chain, int storeId)
	{
		_Application	= app;
		_Chain			= chain;
		_StoreId 		= storeId;
	}

	// Returns the day (as offset from Sunday, which is 0) when sale for the chain begins 
	public int getSaleWeekOffset ()
	{
		Chain chain = getChain();
		return chain != null ? chain.getSaleWeekOffset() : ConstantsSI.DEFAULT_SALE_WEEK_OFFSET;
	}
	
	// Returns the day (as offset from Sunday, which is 0) when sale for the chain begins 
	public int getRegularWeekOffset ()
	{
		Chain chain = getChain();
		return chain != null ? chain.getRegularWeekOffset() : ConstantsSI.DEFAULT_REGULAR_WEEK_OFFSET;
	}
	
	public StoreItem getStoreItem (int itemCode)
	{
		StoreItem storeItem = _StoreItems.get(itemCode);
		if ( storeItem == null )
		{
			storeItem = new StoreItem (_Application, this, itemCode);
			_StoreItems.put(itemCode, storeItem);
		}
		return storeItem;
	}
	
	public StoreItem getStoreItem (CompetitiveDataDTO compData)
	{
		int itemCode = compData.getItemCode();
		StoreItem storeItem = _StoreItems.get(itemCode);
		if ( storeItem == null )
		{
			storeItem = new StoreItem (_Application, this, compData);
			_StoreItems.put(itemCode, storeItem);
		}
		return storeItem;
	}
	
	public void removeStoreItem (int itemCode)
	{
		StoreItem storeItem = _StoreItems.get(itemCode);
		if ( storeItem != null ) {
			_StoreItems.remove(itemCode);
		}
	}

	/*
	public int getStoreItemSize ()
	{
		int ret = _StoreItems.size();
		return ret;
	}
	*/

	//
	// Getters/Setters
	//
	public int getStoreId ()	{ return _StoreId; }
	public int getChainId ()
	{
		Chain chain = getChain();
		return chain != null ? chain.getChainId() : ConstantsSI.INVALID_ID;
	}

	public void setChain (Chain val) { _Chain = val; } 
	public Chain getChain ()
	{
		if ( _Chain == null ) {
			_Chain = Chain.getChainByStoreId (_Application, _StoreId);
		}
		return _Chain;
	}
	
	public HashMap<Integer, Float> getRegularPriceEnding (java.sql.Date date) throws GeneralException
	{
		// Populate hash map with price ending summary of this store
		if (_RegularPriceEndingMap == null)
		{
			findPriceEnding(date, false);
		}
	
		return _RegularPriceEndingMap;
	}
	
	public HashMap<Integer, Float> getSalePriceEnding( java.sql.Date date) throws GeneralException
	{
		// Populate hash map with price ending summary of this store
		if (_SalePriceEndingMap == null)
		{
			findPriceEnding(date, true);
		}
	
		return _SalePriceEndingMap;
	}
	
	private void findPriceEnding(java.sql.Date date, boolean isSale) throws GeneralException
	{
		HashMap<Integer, Integer> tempPricePointsMap = null; //Key - Price Ending, Value - Count

		if( isSale)
			_SalePriceEndingMap = new HashMap<Integer, Float> ();
		else
			_RegularPriceEndingMap = new HashMap<Integer, Float> (); 
		GenericUtil.logMessage("In findPriceEnding for Store ID " + _StoreId + ((isSale)? " For Sale" : " For Reg.") );

		if( isSale)
			tempPricePointsMap = _Application.getRulesDB().getSalePriceEnding(_StoreId, date);
		else
			tempPricePointsMap = _Application.getRulesDB().getRegularPriceEnding(_StoreId, date);

		int sum = 0;

		if (tempPricePointsMap != null)
		{
			for (Iterator<Integer> iCounter = tempPricePointsMap.keySet().iterator(); iCounter.hasNext();)
			{
				int key = iCounter.next();

				GenericUtil.logMessage(" Price Ending Key " + key + " Value " + tempPricePointsMap.get(key));

				sum += tempPricePointsMap.get(key);

			}
			
			float totalPerc = 0;
			for (Iterator<Integer> iCounter = tempPricePointsMap.keySet().iterator(); iCounter.hasNext();)
			{
				int key = iCounter.next();

				float perc = NumberUtil.RoundFloat((tempPricePointsMap.get(key) * 100f / sum), 1);

				GenericUtil.logMessage("Regular Price Ending Key " + key + " Value (%) " + perc);

				// Identify the ending points which are < 1% occurrence, and
				// remove them from the list. Keep the ones in the list with >=
				// 1%.
				if (perc >= _Application.getConfigSettings().getPriceEndingFirstPass()){
					totalPerc += perc;
					if (isSale)
						_SalePriceEndingMap.put(key, perc);
					else
						_RegularPriceEndingMap.put(key, perc);
					
				}
			}
			if(totalPerc >= _Application.getConfigSettings().getPriceEndingSecondPass())
			{
				if (isSale)
					_CheckSalePriceEnding = true;
				else
					_CheckRegularPriceEnding = true;
			}
		}
	}

	public boolean isCheckRegularPriceEnding() {
		return _CheckRegularPriceEnding;
	}

	public boolean isCheckSalePriceEnding() {
		return _CheckSalePriceEnding;
	}
	//public Application getApplication ()	{ return application; }
	
	//
	// Data members
	//
	private SIApplication	_Application	= null;
	private int				_StoreId		= ConstantsSI.INVALID_ID;
	private Chain			_Chain			= null;
	
	private HashMap<Integer, StoreItem>	_StoreItems = new HashMap<Integer, StoreItem>();

	private HashMap<Integer, Float> _RegularPriceEndingMap = null;
	private boolean _CheckRegularPriceEnding = false;
	
	private HashMap<Integer, Float> _SalePriceEndingMap = null;
	private boolean _CheckSalePriceEnding = false;
}
