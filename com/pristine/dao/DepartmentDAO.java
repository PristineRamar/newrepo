package com.pristine.dao;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;

import org.apache.log4j.Logger;

import com.pristine.exception.GeneralException;
import com.pristine.util.PristineDBUtil;

public class DepartmentDAO implements IDAO {
	
	private static Logger logger = Logger.getLogger("DepartmentDAO");
	
	/*private static final String GET_DEPARTMENT_PRODUCT_ID_FOR_UPC = "SELECT A5.PRODUCT_ID DEPT_ID FROM ( ( ( ( PRODUCT_GROUP_RELATION A1" +
																		" JOIN PRODUCT_GROUP_RELATION A2" +
																		" ON A1.PRODUCT_ID        = A2.CHILD_PRODUCT_ID" +
																		" AND A1.PRODUCT_LEVEL_ID = A2.CHILD_PRODUCT_LEVEL_ID " +
																		" AND A1.CHILD_PRODUCT_ID = (SELECT ITEM_CODE FROM ITEM_LOOKUP WHERE UPC = ?) )" +
																		" LEFT JOIN PRODUCT_GROUP_RELATION A3" +
																		" ON A2.PRODUCT_ID        = A3.CHILD_PRODUCT_ID" +
																		" AND A2.PRODUCT_LEVEL_ID = A3.CHILD_PRODUCT_LEVEL_ID )" +
																		" LEFT JOIN PRODUCT_GROUP_RELATION A4" +
																		" ON A3.PRODUCT_ID        = A4.CHILD_PRODUCT_ID" +
																		" AND A3.PRODUCT_LEVEL_ID = A4.CHILD_PRODUCT_LEVEL_ID )" +
																		" LEFT JOIN PRODUCT_GROUP_RELATION A5" +
																		" ON A4.PRODUCT_ID                = A5.CHILD_PRODUCT_ID" +
																		" AND A4.PRODUCT_LEVEL_ID         = A5.CHILD_PRODUCT_LEVEL_ID )";*/
	
	private static final String GET_DEPARTMENT_PRODUCT_ID_FOR_UPC = "SELECT PRODUCT_ID DEPT_ID FROM (SELECT * FROM PRODUCT_GROUP_RELATION PGR " +
																	"start with child_product_level_id = 1 " +
																	"and child_product_id in (select item_code from item_lookup where upc = ?) " +
																	"connect by  prior product_id = child_product_id  and  prior product_level_id = child_product_level_id " +
																	") WHERE PRODUCT_LEVEL_ID = 5";
	
	private static final String GET_DEPT_ID_FOR_UPC = "SELECT DEPT_ID FROM ITEM_LOOKUP WHERE UPC = ?";
	
	
	/**
	 * Returns department product id for UPC passed as input
	 * @param conn		Connection
	 * @param upc		UPC
	 * @return
	 * @throws GeneralException
	 */
	public int getDepartmentProductId(Connection conn, String upc) throws GeneralException{
		PreparedStatement statement = null;
	    ResultSet resultSet = null;
	    int deptProductId = -1;
	    int recordsReturned = 0;
		try{
			statement = conn.prepareStatement(GET_DEPARTMENT_PRODUCT_ID_FOR_UPC);
			statement.setString(1, upc);
	        resultSet = statement.executeQuery();
	        while(resultSet.next()){
	        	deptProductId = resultSet.getInt("DEPT_ID");
	        	recordsReturned++;
	        }
	        
	        if(recordsReturned > 1)
	        	deptProductId = -1;
		}
		catch (SQLException e)
		{
			logger.error("Error while executing GET_DEPT_ID_FOR_UPC");
			throw new GeneralException("Error while executing GET_DEPT_ID_FOR_UPC", e);
		}finally{
			PristineDBUtil.close(resultSet);
			PristineDBUtil.close(statement);
		}
		return deptProductId;
	}
}
