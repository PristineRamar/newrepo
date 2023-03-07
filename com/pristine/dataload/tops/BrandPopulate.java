package com.pristine.dataload.tops;

import java.util.*;
import java.sql.Connection;

import java.sql.SQLException;
import javax.sql.rowset.CachedRowSet;

import org.apache.log4j.Logger;
import org.apache.log4j.PropertyConfigurator;

import com.pristine.dao.DBManager;
import com.pristine.dao.ItemDAO;
import com.pristine.exception.GeneralException;
import com.pristine.util.PropertyManager;

public class BrandPopulate
{
    private Connection	_Conn = null;
	static Logger 		logger = Logger.getLogger("BrandPopulate");
	
	public BrandPopulate ()
	{
		try
		{
	        PropertyManager.initialize("analysis.properties");
	        _Conn = DBManager.getConnection();
	        _Conn.setAutoCommit(true);
	        //populateBrand();
		}
		catch (GeneralException ex) {
			
		}
		catch (SQLException sqlEx) {
			
		}
	}
	
	private void populateBrand ()
	{
		Map<String, List<Integer>> brands = new HashMap<String, List<Integer>>();
		
		ItemDAO dao = new ItemDAO();
		CachedRowSet items = dao.getItemsWithoutBrandId(_Conn);
		try
		{
			while ( items.next() )
			{
				String brand;
				try
				{
					String itemName =  items.getString("ITEM_NAME");
					//logger.debug(brand);
					if ( itemName.length() > 8 )
						itemName =  itemName.substring(0, 9);		// first 8 chars
					brand = itemName.trim();
					if ( brand.contains ("'") )		// escape single-quote if any
						brand = brand.replaceAll("'", "''");
					int itemCode =  items.getInt("ITEM_CODE");
					
					List<Integer> itemCodes;
					itemCodes = brands.get(brand);
					if ( itemCodes == null )
						itemCodes = new ArrayList<Integer>();
					itemCodes.add(itemCode);
					
					brands.put(brand, itemCodes);
				}
				catch (SQLException sqlEx) {
					logger.error("populateBrand ERROR: ", sqlEx);
				}
				catch (Exception ex) {
					logger.error("populateBrand ERROR: ", ex);
				}
			}
			
			// update database
			dao.updateBrand(_Conn, brands);
		}
		catch (SQLException sqlEx) {
			logger.error("populateBrand ERROR: ", sqlEx);
		}
	}
	
	public static void main(String[] args)
	{
		PropertyConfigurator.configure("log4j.properties");
		logCommand (args);
		new BrandPopulate().populateBrand();
	}
	
	private static void logCommand (String[] args)
	{
		logger.info("*****************************************");
		StringBuffer sb = new StringBuffer("Command: BrandPopulate ");
		for ( int ii = 0; ii < args.length; ii++ ) {
			if ( ii > 0 ) sb.append(' ');
			sb.append (args[ii]);
		}
		logger.info(sb.toString());
	}
}
