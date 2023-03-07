package com.pristine.business.entity;

import java.sql.Connection;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.ListIterator;

import java.sql.Date;

//import org.apache.log4j.Logger;

import com.pristine.util.*;

import com.pristine.dao.CompetitiveDataDAO;
import com.pristine.dao.DBManager;
import com.pristine.dao.GenericDAO;
import com.pristine.dao.RulesDAO;
import com.pristine.exception.GeneralException;

import com.pristine.dto.ConfigSettingDTO;
import com.pristine.dto.RuleDTOList;
import com.pristine.dto.CompetitiveDataDTO;

public class SIApplication implements SIApplicationInterface
{
	public SIApplication ()
	{
		try
		{
			_Connection = DBManager.getOracleConnection();
			
			if ( _ConfigSettings == null ) {
				loadConfigSettigns();
			}
			if (_RulesList == null) {
				_RulesList = new GenericDAO().getRules();
			}
			if( _RemovalRulesMap == null) {
				_RemovalRulesMap = new GenericDAO().getRulesMapping();
			}
		}
		catch (GeneralException e1)
		{
			GenericUtil.logMessage("Unable to create RulesDAO/RulesMap/CompetitiveData objects in SIApplication constructor!", e1);
		}
	}
	
	public boolean Init (int scheduleId, java.sql.Date date) throws GeneralException
	{
		boolean ret = true;
		
		if ( _Connection == null ) {
			_Connection = DBManager.getOracleConnection();
		}
		
		_RulesDB = new RulesDAO (_Connection, _ConfigSettings);
		_CompetitiveDataDB = new CompetitiveDataDAO(_Connection);
		
		String sDate = _CompetitiveDataDB.getStartDate (scheduleId);
		if ( !sDate.equalsIgnoreCase(ConstantsSI.CONST_SI_ANALYSIS_SCHEDULE_DOESNOT_EXIST) )
		{
			// Schedule exists
			_ScheduleId = scheduleId;
			
			_StartDate = Date.valueOf(sDate);

			//suspectItemDTOs = new ArrayList<SuspectItemDTO>();
			
			// Init chain and store
			_Store = new Store (this, _CompetitiveDataDB.getStoreId(scheduleId));
			_Chain = Chain.getChainByStoreId (this, _Store.getStoreId());
			_Store.setChain(_Chain);
			
			// Set regular and sale days 
			setDateAndNumberOfDays (scheduleId, date);
		}
		else
		{
			// Schedule does not exist
			term ();
			ret = false;
		}
		
		return ret;
	}
	
	public void term () throws GeneralException
	{
		PristineDBUtil.close(_Connection);
		_Connection = null;
		
		_Chain = null;
		_Store = null;
	}

	//
	// Implementation of SIApplicationInterface - BEGIN
	//
	public int getNumberOfRegularDays () { return _NumberOfRegularDays; }
	
	public int getNumberOfSaleDays () { return _NumberOfSaleDays; }

	public RulesDAO getRulesDB () { return _RulesDB; }
	
	public CompetitiveDataDAO getCompetitiveDataDB () { return _CompetitiveDataDB; }
	
	public int getScheduleId ()	{ return _ScheduleId; }
	//
	// Implementation of SIApplicationInterface - END
	//
	
	//
	// Getters/Setters
	//
	public RuleDTOList getRulesList () { return _RulesList; }
	
	public HashMap<Integer, ArrayList<Integer>> getRemovalRulesMap () { return _RemovalRulesMap; }
	
	public Connection getConnection ()	{ return _Connection; }
	
	//public ArrayList<SuspectItemDTO> getSuspectItemDTOs ()	{ return suspectItemDTOs; }
		
	public java.sql.Date getDate()		{ return _Date; }
	public java.sql.Date getStartDate()	{ return _StartDate; }
	public Chain getChain()				{ return _Chain; }
	public Store getStore()				{ return _Store; }

	public StoreItem getStoreItem (int itemCode) throws GeneralException
	{
		Store aStore = getStore();
		return aStore.getStoreItem(itemCode);
	}
	
	public StoreItem getStoreItem (CompetitiveDataDTO compData) throws GeneralException
	{
		Store aStore = getStore();
		return aStore.getStoreItem(compData);
	}
	
	public ConfigSettingDTO getConfigSettings()
	{
		return _ConfigSettings;
	}
	
	public int getInfoCount ()
	{
		if ( _InfoCount == 0 )
		{
			try {
				PropertyManager.initialize(SIApplication.class.getClassLoader().getResourceAsStream(Constants.ANALYSIS_CONFIG_FILE));
			}
			catch (Exception e) {
			}
			
			_InfoCount = Integer.parseInt(PropertyManager.getProperty("SI_ANALYSIS.INFO.COUNT", "50"));
		}
		return _InfoCount;
	}

	public Float[] findAveragePriceChangePct(ArrayList<String> priceList)
	{
		Float averagePriceArray[] = null;
		if( priceList !=null && priceList.size() > 1){ 

			averagePriceArray = new Float[priceList.size() - 1];
			ListIterator<String> priceIterator = priceList.listIterator();
			int index = 0;
			float first = 0;
			float second = 0;

			while (priceIterator.hasNext())
			{
				// Read an element from the ArrayList
				first = Float.parseFloat(priceIterator.next());
	
				if (priceIterator.hasNext())
				{
					second = Float.parseFloat(priceIterator.next());
	
					averagePriceArray[index++] = Math.abs((((first - second) / second) * 100));
	
					priceIterator.previous();
				}
			}
		}
		return averagePriceArray;
	}
	
	//
	// Private methods
	//
	private void loadConfigSettigns () throws GeneralException
	{
		HashMap<String, String>	configSettingsMap = GenericDAO.getConfigSettings(_Connection);
		_ConfigSettings = new ConfigSettingDTO(configSettingsMap);
	}
	
	private void setDateAndNumberOfDays (int scheduleId, java.sql.Date date)
	{
		// Set number of regular and sale days
		_NumberOfRegularDays = _ConfigSettings.getUniqueStoresDays();
		_NumberOfSaleDays = _ConfigSettings.getUniqueStoresDaysSale();
		if ( date == null )
		{
			// Use status change date
			String sDate = _CompetitiveDataDB.getStatusChangeDate (scheduleId);
			_Date = Date.valueOf(sDate);
		}
		else {
			// Use given date
			_Date = date;
		}
		
		int checkDayOffset = (int)DateUtil.getDateDiff(_Date, _StartDate);
		int offsetDiff = checkDayOffset - _Chain.getRegularWeekOffset();
		if ( offsetDiff >= 0 ) {
			_NumberOfRegularDays += offsetDiff + 1;
		}
		else {
			_NumberOfRegularDays -= offsetDiff + 1;
		}
		
		offsetDiff = checkDayOffset - _Chain.getSaleWeekOffset();
		if ( offsetDiff >= 0 ) {
			_NumberOfSaleDays = offsetDiff + 1;
		}
		else {
			_NumberOfSaleDays -=  offsetDiff + 1;
		}
	}
	
	//
	// Data members
	//
	// Input
	private int				_ScheduleId = ConstantsSI.INVALID_ID;
	private java.sql.Date	_Date = null;
	private java.sql.Date	_StartDate = null;
	
	private Chain	_Chain = null;
	private Store	_Store = null;

	private CompetitiveDataDAO	_CompetitiveDataDB	= null;
	//private String			_SuspectDetails		= null;
	private Connection			_Connection			= null;
	//private ArrayList<SuspectItemDTO>	suspectItemDTOs		= null;
	private int					_NumberOfRegularDays	= 14;
	private int					_NumberOfSaleDays		= 7;
	
	// Single instances
	private static RulesDAO			_RulesDB		= null;
	private static RuleDTOList		_RulesList		= null;
	private static ConfigSettingDTO _ConfigSettings	= null;
	private static HashMap<Integer, ArrayList<Integer>>	_RemovalRulesMap	= null;

	private static int _InfoCount = 0;
	//private static Logger	logger	= Logger.getLogger("Application");
}
