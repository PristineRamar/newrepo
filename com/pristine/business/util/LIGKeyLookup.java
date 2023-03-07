/*
 * Title: LIG Key lookup - common business utility class
 *
 *******************************************************************************
 * Modification History
 *------------------------------------------------------------------------------
 * Version		Date		Name			Remarks
 *------------------------------------------------------------------------------
 * Version 0.1	22/11/2011	John Britto		New Creation
 *******************************************************************************
 */
package com.pristine.business.util;

import java.util.HashMap;
import org.apache.log4j.Logger;
import com.pristine.dao.CompDataDAOLIG;
import javax.sql.rowset.CachedRowSet;
import java.sql.Connection;
import java.sql.SQLException;
import com.pristine.exception.GeneralException;

public class LIGKeyLookup {

	static Logger 		logger = Logger.getLogger("LIGKeyLookup - util");
	private HashMap<Integer, Integer> _LigDataMap;
	
	/*
	 *****************************************************************
	 * Class constructor
	 * Argument 1 : DB connection
	 * Argument 2 : Schedule Id
	 * Return	  : void
	 * @throws Exception
	 * ****************************************************************
	 */	
	public LIGKeyLookup(Connection conn, int scheduleId) {
		_LigDataMap = new HashMap<Integer, Integer>();
		CompDataDAOLIG ObjLIGDAO = new CompDataDAOLIG();
		
		
		try {
			//Populate the LIG key lookup data and keep in hash map
			CachedRowSet resultLig = ObjLIGDAO.getLigItemsMovement(conn, scheduleId);
			//logger.debug("Total Records " + resultLig.size());
			
			while (resultLig.next()) {
				//Add data into hash map, key is item code and value is check data id
				_LigDataMap.put(resultLig.getInt("item_code"), resultLig.getInt("check_data_id"));
				//logger.debug(" Item Code     "+ resultLig.getInt("item_code"));
				//logger.debug(" Check Data ID "+ resultLig.getInt("check_data_id"));
				
			}
			
		} catch (GeneralException ge) {
			logger.error(ge.getMessage());
		}
		catch (SQLException se) {
			logger.error(se.getMessage());		
		}	
	}
	
	/*
	 *****************************************************************
	 * Method to get the check data id by providing Item Code
	 * Argument 1 : Item Code
	 * Return	  : integer - Check Data ID
	 * @throws Exception
	 * ****************************************************************
	 */
	public int getCheckDataId(int itemCode) {
		int checkDataId = -1;
		
		/*Check the existence of Item code in the hash map
		 * If exist then get the check data id	*/
		if (_LigDataMap.containsKey(itemCode)) {		
			checkDataId = _LigDataMap.get(itemCode);
			
		}
	
		return checkDataId;
	}

}
