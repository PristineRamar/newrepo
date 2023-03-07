package com.pristine.service;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.HashMap;

import javax.sql.rowset.CachedRowSet;

import org.apache.log4j.Logger;

import com.pristine.dto.salesanalysis.ProductGroupDTO;
import com.pristine.exception.GeneralException;
import com.pristine.util.Constants;
import com.pristine.util.PristineDBUtil;
import com.pristine.util.PropertyManager;
import com.sun.rowset.CachedRowSetImpl;

public class ProductService {
	
	private static Logger logger = Logger.getLogger("ProductService");
	
	private static final String GET_CHILD_HIERARCHY = "SELECT PRODUCT_LEVEL_ID, CHILD_PRODUCT_LEVEL_ID FROM PRODUCT_GROUP_TYPE START WITH PRODUCT_LEVEL_ID = ? " +
													  "CONNECT BY PRIOR CHILD_PRODUCT_LEVEL_ID = PRODUCT_LEVEL_ID ORDER SIBLINGS BY CHILD_PRODUCT_LEVEL_ID ";

	private static final String GET_CHILD_HIERARCHY_REC = "SELECT PRODUCT_LEVEL_ID, CHILD_PRODUCT_LEVEL_ID FROM PRODUCT_GROUP_TYPE_REC START WITH PRODUCT_LEVEL_ID = ? " +
			  "CONNECT BY PRIOR CHILD_PRODUCT_LEVEL_ID = PRODUCT_LEVEL_ID ORDER SIBLINGS BY CHILD_PRODUCT_LEVEL_ID ";
	
	private static final String GET_ALL_ITEMS = "SELECT * FROM ( %PRODUCT_HIERARCHY% ) WHERE %QUERY_CONDITION% ";
	
	private HashMap<Integer, Integer> productLevelRelationMap = new HashMap<Integer, Integer>();
	private HashMap<Integer, Integer> productLevelPointer = new HashMap<Integer, Integer>();
	public ArrayList<ProductGroupDTO> productHierarchy = new ArrayList<ProductGroupDTO>();
	/**
	 * Retrieves parent child product level type relation
	 * @param conn
	 * @param productLevel
	 * @return
	 * @throws GeneralException
	 */
	public ArrayList<ProductGroupDTO> getChildHierarchy(Connection conn, int productLevel) throws GeneralException{
		PreparedStatement stmt = null;
		ResultSet rs = null;
		
		try {				
			int chainId = Integer.parseInt(PropertyManager.getProperty("PRESTO_SUBSCRIBER"));
			
			logger.debug("getChildHierarchy SQL:" + GET_CHILD_HIERARCHY);
			
			stmt = conn.prepareStatement(GET_CHILD_HIERARCHY);
			stmt.setInt(1, Constants.DEPARTMENTLEVELID);
			rs = stmt.executeQuery();
			while (rs.next()){
				ProductGroupDTO childProductDTO = new ProductGroupDTO();
				childProductDTO.setProductLevelId(rs.getInt("PRODUCT_LEVEL_ID"));
				childProductDTO.setChildLevelId(rs.getInt("CHILD_PRODUCT_LEVEL_ID"));
				productLevelRelationMap.put(childProductDTO.getProductLevelId(), childProductDTO.getChildLevelId());
				productHierarchy.add(childProductDTO);
			}
			
			if(productHierarchy.size() > 0){
				int counter = 0;
				for (int i = productHierarchy.size(); i > 1; i--){
					productLevelPointer.put(productHierarchy.get(counter).getProductLevelId(), i-1);
					counter++;
				}
			}
		}catch (SQLException e) {
			logger.error(e);
			throw new GeneralException("Error in getChildHierarchy "+ e);
		}finally{
			PristineDBUtil.close(stmt);
		}
		
		return productHierarchy;
	}
	
	
	/**
	 * Retrieves parent child product level type relation
	 * @param conn
	 * @param productLevel
	 * @return
	 * @throws GeneralException
	 */
	public ArrayList<ProductGroupDTO> getChildHierarchyRec(Connection conn, int productLevel) throws GeneralException{
		PreparedStatement stmt = null;
		ResultSet rs = null;
		
		try {				
			int chainId = Integer.parseInt(PropertyManager.getProperty("PRESTO_SUBSCRIBER"));
			
			logger.debug("getChildHierarchy SQL:" + GET_CHILD_HIERARCHY_REC);
			
			stmt = conn.prepareStatement(GET_CHILD_HIERARCHY_REC);
			stmt.setInt(1, Constants.DEPARTMENTLEVELID);
			rs = stmt.executeQuery();
			while (rs.next()){
				ProductGroupDTO childProductDTO = new ProductGroupDTO();
				childProductDTO.setProductLevelId(rs.getInt("PRODUCT_LEVEL_ID"));
				childProductDTO.setChildLevelId(rs.getInt("CHILD_PRODUCT_LEVEL_ID"));
				productLevelRelationMap.put(childProductDTO.getProductLevelId(), childProductDTO.getChildLevelId());
				productHierarchy.add(childProductDTO);
			}
			
			if(productHierarchy.size() > 0){
				int counter = 0;
				for (int i = productHierarchy.size(); i > 1; i--){
					productLevelPointer.put(productHierarchy.get(counter).getProductLevelId(), i-1);
					counter++;
				}
			}
		}catch (SQLException e) {
			logger.error(e);
			throw new GeneralException("Error in getChildHierarchy "+ e);
		}finally{
			PristineDBUtil.close(stmt);
		}
		
		return productHierarchy;
	}
	
	public HashMap<Integer, Integer> getProductLevelRelationMap() {
		return productLevelRelationMap;
	}
	
	public HashMap<Integer, Integer> getProductLevelPointer() {
		return productLevelPointer;
	}
	
	/**
	 * Returns parent child product level type relationship
	 * @param conn
	 * @param productLevel
	 * @return
	 */
	public HashMap<Integer, Integer> getProductLevelRelationMap(Connection conn, int productLevel) throws GeneralException{
		if(productLevelRelationMap != null && productLevelRelationMap.size() > 0){
			return productLevelRelationMap;
		}else{
			getChildHierarchy(conn, productLevel);
			return productLevelRelationMap;
		}
	}
	
	
	/**
	 * Returns parent child product level type relationship
	 * @param conn
	 * @param productLevel
	 * @return
	 */
	public HashMap<Integer, Integer> getProductLevelRelationMapRec(Connection conn, int productLevel) throws GeneralException{
		if(productLevelRelationMap != null && productLevelRelationMap.size() > 0){
			return productLevelRelationMap;
		}else{
			getChildHierarchyRec(conn, productLevel);
			return productLevelRelationMap;
		}
	}
	
	/**
	 * Returns a map that contains which product level type is present at which index
	 * @return
	 */
	public HashMap<Integer, Integer> getProductLevelPointer(Connection conn, int productLevel) throws GeneralException{
		if(productLevelPointer != null && productLevelPointer.size() > 0){
			return productLevelPointer;
		}else{
			getChildHierarchy(conn, productLevel);
			return productLevelPointer;
		}
	}
	
	/**
	 * Returns a map that contains which product level type is present at which index
	 * @return
	 */
	public HashMap<Integer, Integer> getProductLevelPointerRec(Connection conn, int productLevel) throws GeneralException{
		if(productLevelPointer != null && productLevelPointer.size() > 0){
			return productLevelPointer;
		}else{
			getChildHierarchyRec(conn, productLevel);
			return productLevelPointer;
		}
	}
	
	/**
	 * Returns query string that retrieves all items that belongs to the input product level
	 * @param conn
	 * @param productLevel
	 * @return
	 * @throws SQLException
	 * @throws GeneralException
	 */
	public String getProductHierarchy(Connection conn, int productLevel) throws GeneralException
    {
        StringBuffer sb = new StringBuffer();
        
        if(productHierarchy.size() == 0)
        	getChildHierarchy(conn, productLevel);
        
        int productHierarchyCount = productHierarchy.size();

        sb.append("SELECT ITEM_LOOKUP.LIR_IND, ITEM_LOOKUP.RET_LIR_ID, ITEM_LOOKUP.UPC, ITEM_LOOKUP.ITEM_SIZE, " +
        		"ITEM_LOOKUP.UOM_ID, UL.NAME AS UOM_NAME," +
        		"RETAILER_LIKE_ITEM_GROUP.RET_LIR_ITEM_CODE, PRODUCT_HIERARCHY.* FROM ( ");
        sb.append("SELECT ITEM_CODE as ITEM_CODE_1,");
        for (int i = 1; i < productHierarchyCount; i++)
        {
            sb.append("P_L_ID_" + i + ", P_ID_" + i + ", P" + i + ".NAME NAME_" + i + ",");
        }
        sb.delete((sb.length() - 1), sb.length());
        sb.append(" FROM ");

        for (int s = 1; s < productHierarchyCount; s++)
        {
            sb.append("( ");
        }

        sb.append(" SELECT ");
        for (int j = 1; j < productHierarchyCount; j++)
        {
            if (j == 1)
            {
                sb.append(" A" + j + ".PRODUCT_LEVEL_ID P_L_ID_" + j + ", A" + j + ".PRODUCT_ID P_ID_" + j + ", A" + j + ".CHILD_PRODUCT_ID ITEM_CODE ,");
            }
            else
            {
                sb.append(" A" + j + ".PRODUCT_LEVEL_ID P_L_ID_" + j + ", A" + j + ".PRODUCT_ID P_ID_" + j + ", A" + j + ".CHILD_PRODUCT_ID CHILD_P_ID_" + j + ",");
            }
        }
        sb.delete((sb.length() - 1), sb.length());

        sb.append(" FROM ");
        for (int x = 1; x < productHierarchyCount; x++)
        {
            int loop = productHierarchyCount - 1;
            if (x > loop)
            {
                break;
            }
            sb.append("( ");
        }


        for (int k = 1; k <= productHierarchyCount; k++)
        {
            if (k == 1)
            {
                sb.append(" PRODUCT_GROUP_RELATION A" + k + " ");
            }
            else if (k == 2)
            {
                sb.append(" LEFT JOIN PRODUCT_GROUP_RELATION A" + k + " ");
                break;
            }
        }

        String firstLevel = "";
        String nextLevel = "";
        for (int a = 1; a < productHierarchyCount + 1; a++)
        {
            if (a == 1)
            {
                firstLevel = "A" + a;
            }
            else if (a == 2)
            {
                nextLevel = "A" + a;
                break;
            }
        }
        sb.append("ON " + firstLevel + ".PRODUCT_ID = " + nextLevel + ".CHILD_PRODUCT_ID AND " + firstLevel + ".PRODUCT_LEVEL_ID = " + nextLevel + ".CHILD_PRODUCT_LEVEL_ID) ");

        for (int b = 1; b < productHierarchyCount + 1; b++)
        {
            int loopCheck = b;
            if (b >= 3)
            {
                sb.append("LEFT JOIN PRODUCT_GROUP_RELATION A" + b + " ");
                sb.append(" ON A" + (loopCheck - 1) + ".PRODUCT_ID = A" + b + ".CHILD_PRODUCT_ID AND A" + (loopCheck - 1) + ".PRODUCT_LEVEL_ID = A" + b + ".CHILD_PRODUCT_LEVEL_ID ) ");
            }
        }

        for (int c = 1; c < productHierarchyCount + 1; c++)
        {
            if (c == 1)
            {
                sb.append("WHERE A" + c + ".CHILD_PRODUCT_LEVEL_ID = 1 ");
                break;
            }
        }
        sb.append(") ");

        for (int d = 1; d < productHierarchyCount; d++)
        {
            sb.append("LEFT JOIN PRODUCT_GROUP P" + d + " ON P_ID_" + d + " = P" + d + ".PRODUCT_ID AND P_L_ID_" + d + " = P" + d + ".PRODUCT_LEVEL_ID ) ");
        }
        sb.append(" PRODUCT_HIERARCHY left join ITEM_LOOKUP on PRODUCT_HIERARCHY.ITEM_CODE_1 = ITEM_LOOKUP.ITEM_CODE " );
        sb.append(" LEFT JOIN RETAILER_LIKE_ITEM_GROUP ON ITEM_LOOKUP.RET_LIR_ID = RETAILER_LIKE_ITEM_GROUP.RET_LIR_ID " );
        sb.append(" LEFT JOIN UOM_LOOKUP UL ");
        sb.append(" ON ITEM_LOOKUP.UOM_ID = UL.ID ");
        sb.append(" WHERE ITEM_LOOKUP.ACTIVE_INDICATOR = 'Y' ");
        sb.append(" ORDER BY ");
        
        for (int e = 1; e < productHierarchyCount; e++)
        {
            sb.append(" NAME_" + e + ",");
        }
        sb.delete((sb.length() - 1), sb.length());
        
        return sb.toString();
    }

	public CachedRowSet getAllItems(Connection conn, int productLevelId, int productId) throws GeneralException{
		String productHierarchy = getProductHierarchy(conn, productLevelId);
		String sql = GET_ALL_ITEMS.replaceAll("%PRODUCT_HIERARCHY%", productHierarchy);
		sql = sql.replaceAll("%QUERY_CONDITION%", getQueryCondition(conn, productLevelId));
		
		logger.debug("Get all items sql - " + sql);
		
		PreparedStatement stmt = null;
		ResultSet rs = null;
		CachedRowSet crs = null;
		try{
			stmt = conn.prepareStatement(sql);
			stmt.setInt(1, productLevelId);
			stmt.setInt(2, productId);
			
			rs = stmt.executeQuery();
			crs = new CachedRowSetImpl();
			crs.populate(rs);
		}catch(SQLException exception){
			logger.error("Error when retrieving all items - " + exception);
			throw new GeneralException("Error when retrieving all items - " + exception);
		}finally{
			PristineDBUtil.close(stmt);
		}
		return crs;
	}
	
	public String getQueryCondition(Connection conn, int productLevelId) throws GeneralException{
		StringBuffer subQuery = new StringBuffer("");
		
		if(productLevelPointer.size() == 0){
			getChildHierarchy(conn, productLevelId);
		}
		
		if(productLevelPointer.get(productLevelId) != null){
			int pointer = productLevelPointer.get(productLevelId);
			subQuery = new StringBuffer(" P_L_ID_" + pointer + " = ? ");
			subQuery.append(" AND P_ID_" + pointer + " = ?");
		}
		
		return subQuery.toString();
	}
}
