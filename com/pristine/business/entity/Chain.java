package com.pristine.business.entity;

import com.pristine.dao.CompStoreDAO;
import com.pristine.exception.GeneralException;
import com.pristine.util.ConstantsSI;

public class Chain
{
	public Chain (int chainId)
	{
		this (null, chainId);
	}
	
	public Chain (SIApplication app, int chainId)
	{
		_Application = app;
		_ChainId = chainId;
	}
	
	public static Chain getChainByStoreId (SIApplication app, int storeId)
	{
		Chain ret = null;
		
		// Retrieve chain id and instantiate Chain object
		try
		{
			CompStoreDAO compStrDao = new CompStoreDAO();
			int chainId = compStrDao.getChainId(app.getConnection(), storeId);
			ret = new Chain (app, chainId);
		}
		catch (GeneralException e) {
		}
		
		return ret;
	}
	
	// Returns the day (as offset from Sunday, which is 0) when sale for the chain begins 
	public int getSaleWeekOffset ()
	{
		return ConstantsSI.DEFAULT_SALE_WEEK_OFFSET;
	}

	// Returns the day (as offset from Sunday, which is 0) when sale for the chain begins 
	public int getRegularWeekOffset ()
	{
		return ConstantsSI.DEFAULT_REGULAR_WEEK_OFFSET;
	}

	//
	// Getters/Setters
	//
	public int getChainId ()	{ return _ChainId; }
	
	public void setApplication (SIApplication v)	{ _Application = v; }
	public SIApplication getApplication ()			{ return _Application; }
	
	//
	// Data members
	//
	private SIApplication	_Application	= null;
	private int				_ChainId		= ConstantsSI.INVALID_ID;
}
