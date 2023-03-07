package com.pristine.dao.salesanalysis;

import java.sql.Connection;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import javax.sql.rowset.CachedRowSet;
import org.apache.log4j.Logger;
import com.pristine.dto.salesanalysis.ProductDTO;
import com.pristine.dto.salesanalysis.ProductGroupDTO;
import com.pristine.exception.GeneralException;
import com.pristine.util.Constants;
import com.pristine.util.PristineDBUtil;

public class SalesAggregationProductGroupDAO {
	private static Logger	logger	= Logger.getLogger("SalesAggregationProductGroupDAO");



	/*
	 * ****************************************************************
	 * Get the Finance, Merchandise, Sector, Portfolio list 
	 * Argument 1: connection 
	 * Argument 2: grouping mode (F- 6 , M-7 ,P-8,S-9) returns list
	 * @throws GeneralException , SQLException
	 * ****************************************************************
	 */

	public ArrayList<ProductGroupDTO> getProductGroupDetails(Connection _Conn) 
										throws GeneralException, SQLException 
									 {

		//List to hold the group data
		ArrayList<ProductGroupDTO> productGroupList = new ArrayList<ProductGroupDTO>();
		
		StringBuffer sb = new StringBuffer();
		sb.append("SELECT R.PRODUCT_LEVEL_ID,");
		sb.append(" R.CHILD_PRODUCT_LEVEL_ID,");
		sb.append(" R.PRODUCT_ID,");
		sb.append(" R.CHILD_PRODUCT_ID");
		sb.append(", T.AGGR_REQUIRED");
		sb.append(" FROM PRODUCT_GROUP_RELATION R,");
		sb.append(" PRODUCT_GROUP_TYPE T ");
		sb.append(" WHERE R.PRODUCT_LEVEL_ID = T.PRODUCT_LEVEL_ID");
		sb.append(" AND R.CHILD_PRODUCT_LEVEL_ID = T.CHILD_PRODUCT_LEVEL_ID");
		sb.append(" AND T.CHILD_PRODUCT_LEVEL_ID > ");
		sb.append(Constants.ITEMLEVELID);
		sb.append(" ORDER BY R.PRODUCT_LEVEL_ID, R.CHILD_PRODUCT_LEVEL_ID,");
		sb.append(" R.PRODUCT_ID, R.CHILD_PRODUCT_ID");

		logger.debug("getProductGroupData SQL:" + sb.toString());
	
		try {
			CachedRowSet result = PristineDBUtil.executeQuery(_Conn, sb, 
											"getProductGroupDetails");
			
			if (result !=null) {

				logger.info("Total Child Records count:" + result.size());

				//To hold the previous processed record's group id
				int preProductId = -1;
				int rowCount = 0;

				//Array list to hold the Child records list
				ArrayList<ProductDTO> productList = new ArrayList<ProductDTO>();
				//to hold Group DTO
				ProductGroupDTO parentDTO = new ProductGroupDTO();
				
				while (result.next()) {

					int productId = result.getInt("PRODUCT_LEVEL_ID");
					int childId = result.getInt("CHILD_PRODUCT_LEVEL_ID");					
					if (rowCount == 0) //For first record
					{
						preProductId = productId;
						parentDTO.setProductLevelId(productId);
						parentDTO.setChildLevelId(childId);
						parentDTO.setAggregationRequired(result.getInt("AGGR_REQUIRED"));
						ArrayList<ProductGroupDTO> childProductList = 
										getChildHierarchy(_Conn, productId);
						if (childProductList.size() > 0 ){
							parentDTO.setChildProductData(childProductList);	
						}

					}

					//Check change of Group Level
					if (preProductId != productId) {
						//logger.info("End of Group");

						//If changed & at least 1 record processed, add group data
						if (rowCount > 0) {
							parentDTO.setProductData(productList);
							productGroupList.add(parentDTO);
							parentDTO = null;
							productList = null;
							//logger.info("Added into the list");
						}

						
						//create object for new group
						productList = new ArrayList<ProductDTO>();
						parentDTO = new ProductGroupDTO();
						parentDTO.setProductLevelId(productId);
						parentDTO.setChildLevelId(childId);
						
						//Get child product hierarchy info
						ArrayList<ProductGroupDTO> childProductList = 
										getChildHierarchy(_Conn, productId);
						
						if (childProductList.size() > 0 ){
							parentDTO.setChildProductData(childProductList);	
						}
						
						parentDTO.setAggregationRequired(result.getInt("AGGR_REQUIRED"));
					}

					ProductDTO childDTO = new ProductDTO();
					childDTO.setProductId(result.getInt("PRODUCT_ID"));
					childDTO.setChildProductId(result.getInt("CHILD_PRODUCT_ID"));
					productList.add(childDTO);

					preProductId = productId;
					rowCount += 1;
				}
				
				result.close();
				//Add the last group data into the group list 
				parentDTO.setProductData(productList);
				productGroupList.add(parentDTO);
				parentDTO = null;
				productList = null;
				result.close();
			}
			else
			{
				logger.warn("No product Group data found, product level aggregation can not be done");
			}

		}
		
		catch (GeneralException gex){
			logger.error("Error while fetching Product Group data..." + gex.getMessage());
			throw new GeneralException("GetProductGroupDetails ", gex);
		}
		catch (SQLException sex){
			logger.error("Error while fetching Product Group data..."+ sex.getMessage());
			throw new SQLException("GetProductGroupDetails ", sex);

		}
		return productGroupList;
	}
	
	
	/*
	 * Get Productgroup name and product id from product_group table
	 * Argument 1  : Connection
	 * Argument 2  : Product Level Id
	 * Table Name  : Product_Group
	 * @catch Exception
	 * @throws GendralException
	 * 
	 */

	public List<ProductDTO> getProductGroup(Connection _conn, int productLevelId)
			throws GeneralException {

		// return list
		List<ProductDTO> returnList = new ArrayList<ProductDTO>();

		StringBuffer sql = new StringBuffer();
		sql.append(" select PRODUCT_ID,NAME from PRODUCT_GROUP");
		if( productLevelId !=0){
		sql.append(" where PRODUCT_LEVEL_ID=" + productLevelId + "");
		}
		logger.debug(" getProductGroup sql :" + productLevelId);
		try {

			CachedRowSet result = PristineDBUtil.executeQuery(_conn, sql,
					"getProductGroup");

			while (result.next()) {

				ProductDTO objproduct = new ProductDTO();
				objproduct.setProductId(result.getInt("PRODUCT_ID"));
				objproduct.setProductName(result.getString("NAME"));
				returnList.add(objproduct);
			}

		} catch (Exception exe) {
			logger.error("Error While Fetching Results" + exe);
			throw new GeneralException("getProductGroup", exe);
		}

		return returnList;
	}
	
	/*
	 *  Method used to get the  Gas product Id
	 * 
	 */
			
	public int getGasProductId(Connection _conn) throws GeneralException {
	 
		int returnVal = 0;
		
		try {
			StringBuffer sql = new StringBuffer();

			sql.append(" select PRODUCT_ID from PRODUCT_GROUP");
			sql.append(" where ADD_ON_TYPE='GAS'");

			String result = PristineDBUtil.getSingleColumnVal(_conn, sql,"getGasProductId");
			
			returnVal = Integer.parseInt(result);
			
		} catch (GeneralException e) {

			logger.error(e);
			throw new GeneralException("Error in get Gas productId ", e);

		}
	
		return returnVal;
	}
	
	public ArrayList<ProductGroupDTO> getChildHierarchy(Connection _conn, 
					int productLevel) throws SQLException, GeneralException{
		
		ArrayList<ProductGroupDTO> childProducts = new ArrayList<ProductGroupDTO>();
		
		ProductGroupDTO childProductDTO = null;
		
		try {				
			StringBuffer sb = new StringBuffer();
			sb.append("SELECT PRODUCT_LEVEL_ID, CHILD_PRODUCT_LEVEL_ID FROM");
			sb.append(" PRODUCT_GROUP_TYPE START WITH PRODUCT_LEVEL_ID = ");
			sb.append(productLevel).append(" CONNECT BY PRIOR ");
			sb.append(" CHILD_PRODUCT_LEVEL_ID = PRODUCT_LEVEL_ID");
			sb.append(" ORDER SIBLINGS BY CHILD_PRODUCT_LEVEL_ID");		
		
			logger.debug("getChildHierarchy SQL:" + sb.toString());
	
			CachedRowSet productRS = PristineDBUtil.executeQuery(_conn, sb, 
														"getChildHierarchy");
		
			if (productRS.size() > 0){
				while (productRS.next()){
					childProductDTO = new ProductGroupDTO();
					childProductDTO.setProductLevelId(productRS.getInt("PRODUCT_LEVEL_ID"));
					childProductDTO.setChildLevelId(productRS.getInt("CHILD_PRODUCT_LEVEL_ID"));
					childProducts.add(childProductDTO);
				}
			}
		} catch (GeneralException e) {
			logger.error(e);
			throw new GeneralException("Error in getChildHierarchy", e);
		}
		
		return childProducts;
	}
	
	public String  getParents(Connection _conn, int productLevel) throws SQLException, GeneralException{

		ArrayList<ProductGroupDTO> childProducts = new ArrayList<ProductGroupDTO>();
		
		//Key is parent and Value is child
		HashMap<String, String> prodLevelMap = new HashMap<String, String>();
		HashMap<String, String> childParentMap = new HashMap<String, String>();
		HashMap<String, String> tempMap = new HashMap<String, String>();
		String currParent = "";
		String allParent = "";
		
		ProductGroupDTO childProductDTO = null;
		int childProductLevel = productLevel;
		
		try {				
			StringBuffer sb = new StringBuffer();
			String strQ = "SELECT PRODUCT_LEVEL_ID,CHILD_PRODUCT_LEVEL_ID FROM PRODUCT_GROUP_TYPE WHERE AGGR_REQUIRED=1";
			sb.append(String.format(strQ, productLevel+""));
			
			CachedRowSet result = PristineDBUtil.executeQuery(_conn, sb,"getProductGroup");
			
			String childId = "";
			
			//Retrive all the Child to Parent Product Levels
			while (result.next()) {
				String parent = "";
				int prod_level_id = result.getInt("PRODUCT_LEVEL_ID");
				int child_prod_level_id = result.getInt("CHILD_PRODUCT_LEVEL_ID");
				
				if(childParentMap.containsKey(child_prod_level_id+"")){
					parent = childParentMap.get(child_prod_level_id+"");
					childParentMap.remove(child_prod_level_id+"");
					parent += ",";
				}
				
				parent += prod_level_id;
				childParentMap.put(child_prod_level_id+"", parent);
			}
			result.close();
			
			//Find all the parents for the given product level
			if( childParentMap.containsKey(productLevel+"") )
				currParent = childParentMap.get(productLevel+"");
			
			//System.out.println(currParent);
			allParent = currParent;
			boolean searchRequired = true;
			while(searchRequired){
				searchRequired = false;
				
				String currParentTemp = currParent;
				for(String var1 : currParent.split(",")){
					if(childParentMap.containsKey(var1)){
						String var2 = childParentMap.get(var1);
						
						allParent = addToParentList(allParent,var2 );
						currParentTemp = addToParentList(currParentTemp, var2);
						currParentTemp = removeToParentList(currParentTemp, var1);
						searchRequired = true;
					}
				}
				currParent = currParentTemp;
			}
			
			//logger.debug("getChildHierarchy Size for product "+parentProductLevel+" is " + childProducts.size());
		} catch (GeneralException e) {
			logger.error(e);
			throw new GeneralException("Error in getAllParents", e);
		}
		
		
		return allParent;
	}
	
	public String addToParentList(String parentList, String newParents){
		//add new parents
		for(String var1 : newParents.split(",")){
			if((","+parentList+",").contains(","+var1+",")){
				
			}
			else{
				parentList += "," + var1;
			}
		}
		return parentList;
	}
	
	public String removeToParentList(String parentList, String remParents){
		//remove current parent
		String newParentList = "";
		boolean present = false;
		for(String var1 : parentList.split(",")){
			present = false;
			for(String var2 : remParents.split(","))
				if(var1.equalsIgnoreCase(var2)) present = true;
			
			if(!present)
				newParentList += var1 + ",";
		}
		
		if(newParentList.endsWith(","))
			newParentList = newParentList.substring(0, newParentList.length()-1 );
		
		return newParentList;
	}

	
	public ArrayList<Integer> getLeastProductParent(Connection _conn) throws GeneralException, SQLException {
		
		ArrayList<Integer> productList = new ArrayList<Integer>();
		
		try {		
			StringBuffer sb = new StringBuffer();
			sb.append("select PRODUCT_LEVEL_ID from PRODUCT_GROUP_TYPE");
			sb.append(" where CHILD_PRODUCT_LEVEL_ID = " + Constants.ITEMLEVELID);
			//Handle POS dept directly from POS, exclude from product hieratchy - Britto 04/17/2013
			sb.append(" AND PRODUCT_LEVEL_ID !=" + Constants.POSDEPARTMENT);
			logger.debug("getLeastProductParent SQL:" + sb.toString());

			CachedRowSet result = PristineDBUtil.executeQuery(_conn, sb,"getLeastProductParent");
			
			if (result.size() > 0){
				while (result.next()){
					productList.add(result.getInt("PRODUCT_LEVEL_ID"));
				}
			}
			
		} catch (GeneralException e) {

			logger.error(e);
			throw new GeneralException("Error in getLeastProductParent", e);

		}
		return productList;
	}

	public HashMap<Integer, Integer> getProductGroupTypeData(Connection _conn) 
				throws GeneralException, SQLException {
	
		HashMap<Integer, Integer> productType = new HashMap<Integer, Integer>();
		
		try {		
		StringBuffer sb = new StringBuffer();
		sb.append("select PRODUCT_LEVEL_ID, AGGR_REQUIRED");
		sb.append(" from PRODUCT_GROUP_TYPE");
		logger.debug("getProductGroupTypeData SQL:" + sb.toString());
		
		CachedRowSet result = PristineDBUtil.executeQuery(_conn, sb, 
									"getProductGroupTypeData");
		
		if (result.size() > 0){
			while (result.next()){
				productType.put(result.getInt("PRODUCT_LEVEL_ID"), 
									result.getInt("AGGR_REQUIRED"));
			}
		}
		
		} catch (GeneralException e) {
		logger.error("Error in getProductGroupTypeData" + e);
		throw new GeneralException("Error in getLeastProductParent", e);
		}
		return productType;
	}

	public HashMap<Integer, Integer> getItemForGasUPC(Connection _conn, String gasUPS) throws GeneralException, SQLException{
		HashMap<Integer, Integer> ItemUPC = new HashMap<Integer, Integer>(); 
		
		try {
		
			StringBuffer sb = new StringBuffer();
			sb.append("select UPC, ITEM_CODE from ITEM_LOOKUP where UPC IN (");
			sb.append(gasUPS).append(")");
			
			CachedRowSet result = PristineDBUtil.executeQuery(_conn, sb, 
													"getItemForGasUPC");
		
			if (result.size() > 0){
				logger.debug("Total GAS UPCs" + result.size());
				while (result.next()){
					logger.debug("UPC:" +  result.getString("UPC") + " Item Code:" + result.getInt("ITEM_CODE"));
					ItemUPC.put(result.getInt("ITEM_CODE"), result.getInt("ITEM_CODE"));
				}
			}
	
		} catch (GeneralException e) {
		logger.error("Error in getItemForGasUPC" + e);
		throw new GeneralException("Error in getItemForGasUPC", e);
		}
	
		return ItemUPC;
	}
	
}
