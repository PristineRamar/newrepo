package com.pristine.business.entity;

import java.util.ArrayList;
import java.sql.SQLException;

import javax.sql.rowset.CachedRowSet;

import com.pristine.exception.GeneralException;
import com.pristine.util.ConstantsSI;
import com.pristine.util.NumberUtil;
import com.pristine.dto.CompetitiveDataDTO;

public class StoreItem
{
	public StoreItem (SIApplication app, Store store, int itemCode)
	{
		_Application = app;
		_Store = store;
		_ItemCode = itemCode;
	}
	
	public StoreItem (SIApplication app, Store store, CompetitiveDataDTO compDataDTO)
	{
		_Application = app;
		_Store = store;
		_ItemCode = compDataDTO.getItemCode();
		_CompetitiveDataDTO = compDataDTO;
	}
	
	public ArrayList<String> getUniqueRegularPricePoints (java.sql.Date aDate) throws GeneralException
	{
		return new ArrayList<String>();
		//java.sql.Date date = aDate != null ? aDate : application.getDate();
		//return application.getRulesDB().getUniqueRegularPricePoints(application.getScheduleId(), store.getStoreId(), itemCode, date);
	}

	public ArrayList<String> getUniqueSalePricePoints (java.sql.Date aDate) throws GeneralException
	{
		return new ArrayList<String>();
		//java.sql.Date date = aDate != null ? aDate : application.getDate();
		//return application.getRulesDB().getUniqueSalePricePoints (application.getScheduleId(), store.getStoreId(), itemCode, date);
	}

	public ArrayList<String> getRegularPricePointList (java.sql.Date date) throws GeneralException
	{
		if ( _RegularPriceList == null )
		{
			CachedRowSet lastPriecRowSet = getLastPriceRowset(date);
			_RegularPriceList = new ArrayList<String>();
			
			try
			{
				lastPriecRowSet.beforeFirst();
				int iCounter = 1;
				//logger.debug("Reg Unique PP Occurence " + RulesDAO.getConfigSettingsMap().get("UNIQUE_PP_OCCURENCES"));
				while (lastPriecRowSet.next())
				{
					if ( lastPriecRowSet.getFloat("reg_price") > 0 && !_RegularPriceList.contains(String.valueOf(lastPriecRowSet.getFloat("reg_price"))))
					{
						iCounter++;
						//if (iCounter <= Integer.parseInt(RulesDAO.getConfigSettingsMap().get("UNIQUE_PP_OCCURENCES")))
						{
							_RegularPriceList.add(String.valueOf(NumberUtil.RoundFloat(lastPriecRowSet.getFloat("reg_price"), 2)));
						}
					}
				}
			}
			catch (SQLException sqlce) {
				throw new GeneralException(sqlce.getMessage(), sqlce);
			}
		}
		
		return _RegularPriceList;
	}
	
	public ArrayList<String> getSalePricePointList (java.sql.Date date) throws GeneralException
	{
		if ( _SalePriceList == null )
		{
			CachedRowSet lastPriecRowSet = getLastPriceRowset(date);
			_SalePriceList = new ArrayList<String>();
			
			try
			{
				lastPriecRowSet.beforeFirst();
				int iCounter = 1;
				//logger.debug("Sale Unique PP Occurrence " + RulesDAO.getConfigSettingsMap().get("UNIQUE_PP_OCCURENCES"));
				while ( lastPriecRowSet.next() )
				{
					if ( lastPriecRowSet.getFloat("sale_price") > 0 && !_SalePriceList.contains(String.valueOf(lastPriecRowSet.getFloat("sale_price"))))
					{
						iCounter++;
						//if (iCounter <= Integer.parseInt(RulesDAO.getConfigSettingsMap().get("UNIQUE_PP_OCCURENCES")))
						{
							_SalePriceList.add(String.valueOf(NumberUtil.RoundFloat(lastPriecRowSet.getFloat("sale_price"), 2)));
						}
					}
				}
			}
			catch (SQLException sqlce) {
				throw new GeneralException(sqlce.getMessage(), sqlce);
			}
		}
		
		return _SalePriceList;
	}
	
	//
	// Getters/setters
	//
	public Store getStore ()	{ return _Store; }
	public int getItemCode ()	{ return _ItemCode; }

	public CachedRowSet getLastPriceRowset (java.sql.Date date) throws GeneralException
	{
		if ( _LastPriceRowset == null ) {			
			_LastPriceRowset = _Application.getRulesDB().getLastPriceChecks (_Store.getStoreId(), _ItemCode,
																_Application.getScheduleId(), date, 0, true);
		}
		return _LastPriceRowset;
	}
	
	//
	// Data members
	//
	private SIApplication	_Application	= null;
	private Store			_Store			= null;
	private int 			_ItemCode		= ConstantsSI.INVALID_ITEM_CODE;
	private CompetitiveDataDTO	_CompetitiveDataDTO = null;

	private CachedRowSet	_LastPriceRowset = null;
	private ArrayList<String>	_RegularPriceList	= null;
	private ArrayList<String>	_SalePriceList		= null;
}
