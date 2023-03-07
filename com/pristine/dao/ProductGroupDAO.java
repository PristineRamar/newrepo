package com.pristine.dao;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Map;

import javax.sql.rowset.CachedRowSet;
import org.apache.log4j.Logger;
import com.pristine.dto.ItemDTO;
import com.pristine.dto.offermgmt.ProductKey;
import com.pristine.dto.salesanalysis.ProductDTO;
import com.pristine.exception.GeneralException;
import com.pristine.service.ProductService;
import com.pristine.util.Constants;
import com.pristine.util.PrestoUtil;
import com.pristine.util.PristineDBUtil;
import com.pristine.util.PropertyManager;

public class ProductGroupDAO {
	private static Logger	logger	= Logger.getLogger("com.pristine.dao.ProductGroupDAO");
	
	// Loading Product Group Tables - Price Index Portfolio Support
	private static final String GET_PRODUCT_GROUP_LEVELS = "SELECT PRODUCT_LEVEL_ID, CHILD_PRODUCT_LEVEL_ID FROM PRODUCT_GROUP_TYPE";
	private static final String GET_PARENT_CHILD_PRODUCT_LEVELS = "SELECT PRODUCT_LEVEL_ID, CHILD_PRODUCT_LEVEL_ID FROM PRODUCT_GROUP_TYPE WHERE CHILD_PRODUCT_LEVEL_ID IS NOT NULL AND CHILD_PRODUCT_LEVEL_ID <> 0";
	private static final String GET_PARENT_OF_ITEM = "SELECT PRODUCT_LEVEL_ID FROM " +
													 "(SELECT PRODUCT_LEVEL_ID, CHILD_PRODUCT_LEVEL_ID FROM PRODUCT_GROUP_TYPE START WITH PRODUCT_LEVEL_ID = 5 CONNECT BY PRIOR CHILD_PRODUCT_LEVEL_ID = PRODUCT_LEVEL_ID) " +
													 "WHERE CHILD_PRODUCT_LEVEL_ID = 1";
	private static final String GET_PRODUCT_GROUP = "SELECT PG.PRODUCT_ID FROM PRODUCT_GROUP PG LEFT JOIN PRODUCT_GROUP_RELATION PGR ON PGR.CHILD_PRODUCT_ID = PG.PRODUCT_ID "
			+ " AND PGR.CHILD_PRODUCT_LEVEL_ID = PG.PRODUCT_LEVEL_ID WHERE PG.PRODUCT_LEVEL_ID = ? ";
	private static final String INSERT_PRODUCT_GROUP = "INSERT INTO PRODUCT_GROUP (PRODUCT_ID, PRODUCT_LEVEL_ID, NAME, CODE) VALUES (PRODUCT_GROUP_SEQ.NEXTVAL, ?, ?, ?)";
	private static final String UPDATE_PRODUCT_GROUP = "UPDATE PRODUCT_GROUP SET NAME = ? WHERE PRODUCT_LEVEL_ID = ? AND PRODUCT_ID = ?";
	private static final String GET_PARENT_PRODUCT_ID_FROM_RELATION = "SELECT PRODUCT_ID FROM PRODUCT_GROUP_RELATION WHERE PRODUCT_LEVEL_ID = ? AND CHILD_PRODUCT_LEVEL_ID = ? AND CHILD_PRODUCT_ID = ?";
	private static final String INSERT_PRODUCT_GROUP_RELATION = "INSERT INTO PRODUCT_GROUP_RELATION (PRODUCT_ID, CHILD_PRODUCT_ID, CHILD_PRODUCT_LEVEL_ID, PRODUCT_LEVEL_ID) VALUES (?, ?, ?, ?)";
	private static final String UPDATE_PRODUCT_GROUP_RELATION = "UPDATE PRODUCT_GROUP_RELATION SET PRODUCT_ID = ? WHERE PRODUCT_LEVEL_ID = ? AND CHILD_PRODUCT_LEVEL_ID = ? AND CHILD_PRODUCT_ID = ?";
	private static final String UPDATE_PRODUCT_ID = "UPDATE %REF_TABLE_NAME% SET PRODUCT_ID = ? WHERE ID = ?";
	private static final String INSERT_PRODUCT_GROUP_RELATION_FOR_LIR_ITEM = "INSERT INTO PRODUCT_GROUP_RELATION (PRODUCT_ID, CHILD_PRODUCT_ID, CHILD_PRODUCT_LEVEL_ID, PRODUCT_LEVEL_ID) " +
																			 "(SELECT ITS.PRODUCT_ID, ?, 1, %PARENT_LEVEL_ID% FROM ITEM_LOOKUP IL, %TABLE_NAME% ITS WHERE " +
																			 "IL.ITEM_CODE = ? AND IL.%COLUMN_NAME% = ITS.ID)";
	private static final String UPDATE_PRODUCT_GROUP_RELATION_FOR_LIR_ITEM = "UPDATE PRODUCT_GROUP_RELATION SET (PRODUCT_ID, CHILD_PRODUCT_ID, CHILD_PRODUCT_LEVEL_ID, PRODUCT_LEVEL_ID) = " +
																			 "(SELECT ITS.PRODUCT_ID, ?, 1, %PARENT_LEVEL_ID% FROM ITEM_LOOKUP IL, %TABLE_NAME% ITS WHERE " +
																			 "IL.ITEM_CODE = ? AND IL.%COLUMN_NAME% = ITS.ID) WHERE PRODUCT_LEVEL_ID = %PARENT_LEVEL_ID% AND CHILD_PRODUCT_LEVEL_ID = 1 AND CHILD_PRODUCT_ID = ?";

	private static final String GET_CATEGORIES = "SELECT PRODUCT_ID FROM PRODUCT_GROUP WHERE PRODUCT_LEVEL_ID = 4";
	private static final String GET_CATEGORIES_FOR_PARENT_PRODUCT = " SELECT PRODUCT_ID FROM ( " + 
																	" SELECT CHILD_PRODUCT_ID  AS PRODUCT_ID,   " + 
																	"  CHILD_PRODUCT_LEVEL_ID AS PRODUCT_LEVEL_ID   " + 
																	" FROM PRODUCT_GROUP_RELATION PGR   " + 
																	"  START WITH PRODUCT_LEVEL_ID       = ?   " + 
																	" AND PRODUCT_ID                      = ?   " + 
																	"  CONNECT BY PRIOR CHILD_PRODUCT_ID = PRODUCT_ID   " + 
																	" AND PRIOR CHILD_PRODUCT_LEVEL_ID    = PRODUCT_LEVEL_ID " + 
																	" ) WHERE PRODUCT_LEVEL_ID = 4";
	private static final String GET_PRODUCT_LISTS_FOR_PRODUCTS = "SELECT PRODUCT_ID, CHILD_PRODUCT_LEVEL_ID, CHILD_PRODUCT_ID FROM  " + 
																	" (  " + 
																	" SELECT CHILD_PRODUCT_ID, CHILD_PRODUCT_LEVEL_ID, PRODUCT_LEVEL_ID, PRODUCT_ID  " + 
																	" FROM PRODUCT_GROUP_RELATION PGR WHERE PRODUCT_LEVEL_ID  = 21  " + 
																	" ) WHERE(CHILD_PRODUCT_LEVEL_ID, CHILD_PRODUCT_ID) IN " + 
																	" (( " + 
																	" SELECT PRODUCT_ID,   " + 
																	"   PRODUCT_LEVEL_ID   " + 
																	" FROM PRODUCT_GROUP_RELATION PGR   " + 
																	"   START WITH CHILD_PRODUCT_LEVEL_ID = ?  " + 
																	" AND CHILD_PRODUCT_ID                = ?   " + 
																	"   CONNECT BY PRIOR PRODUCT_ID       = CHILD_PRODUCT_ID   " + 
																	" AND PRIOR PRODUCT_LEVEL_ID          = CHILD_PRODUCT_LEVEL_ID   " + 
																	" AND PRODUCT_LEVEL_ID <> 21 " + 
																	" UNION   " + 
																	" SELECT CHILD_PRODUCT_ID  AS PRODUCT_ID,   " + 
																	"   CHILD_PRODUCT_LEVEL_ID AS PRODUCT_LEVEL_ID   " + 
																	" FROM PRODUCT_GROUP_RELATION PGR   " + 
																	"   START WITH PRODUCT_LEVEL_ID       = ?   " + 
																	" AND PRODUCT_ID                      = ?  " + 
																	"   CONNECT BY PRIOR CHILD_PRODUCT_ID = PRODUCT_ID   " + 
																	" AND PRIOR CHILD_PRODUCT_LEVEL_ID    = PRODUCT_LEVEL_ID " + 
																	" AND CHILD_PRODUCT_LEVEL_ID <> 1 " + 
																	" )) OR (CHILD_PRODUCT_LEVEL_ID, CHILD_PRODUCT_ID) IN ((?, ?))";
	// Pricing Alert
	private static final String GET_PRODUCT_NAME = "SELECT NAME FROM PRODUCT_GROUP WHERE PRODUCT_LEVEL_ID = ? AND PRODUCT_ID = ?";
	//To insert product group values using unused sequence
	private static final String INSERT_PRODUCT_GROUP_USING_UNUSED_SEQ = "INSERT INTO PRODUCT_GROUP (PRODUCT_ID, PRODUCT_LEVEL_ID, NAME, CODE) VALUES (?, ?, ?, ?)";
	private static final String GET_UNUSED_SEQ_FROM_PRODUCT_GROUP = " SELECT MIN_PRODUCT_ID - 1 + LEVEL MISSING_PRODUCT_ID FROM( SELECT MIN(PRODUCT_ID) "
			+ "MIN_PRODUCT_ID , MAX(PRODUCT_ID) MAX_PRODUCT_ID FROM PRODUCT_GROUP WHERE PRODUCT_ID < ? )CONNECT "
			+ "BY LEVEL <= MAX_PRODUCT_ID - MIN_PRODUCT_ID + 1 MINUS SELECT PRODUCT_ID FROM PRODUCT_GROUP WHERE PRODUCT_ID < ? ";
	private static final String GET_PRODUCT_GROUP_NEXT_VAL ="SELECT PRODUCT_GROUP_SEQ.NEXTVAL FROM DUAL";
	private static final String GET_UNUSED_PRODUCT_ID = "SELECT PRODUCT_ID FROM PRODUCT_GROUP WHERE PRODUCT_LEVEL_ID = ?"
			+ " AND PRODUCT_ID NOT IN (SELECT PRODUCT_ID FROM PRODUCT_GROUP_RELATION WHERE PRODUCT_LEVEL_ID =?)";
	private static final String UPDATE_PRODUCT_GROUP_ACTIVE_IND ="UPDATE PRODUCT_GROUP SET ACTIVE_INDICATOR='N'";
	
	private static final String GET_DEPARTMENTS = "SELECT PRODUCT_ID,NAME FROM PRODUCT_GROUP WHERE PRODUCT_LEVEL_ID = " + Constants.DEPARTMENTLEVELID ;
	
	private static final String GET_ALL_CATEGORIES = "SELECT PRODUCT_ID,NAME FROM PRODUCT_GROUP WHERE PRODUCT_LEVEL_ID = " + Constants.CATEGORYLEVELID ;
	
	List<Integer> productLevels = null;
	List<Integer> excludeProductLevels  = null;
	HashMap<Integer, List<Integer>> childParentMap = null;
			
	public CachedRowSet getItemList(Connection conn, int chainId, boolean unknownOnly, int itemCode) throws GeneralException {
		
		CachedRowSet crs = null;
		StringBuffer sb = new StringBuffer();
		//Fill in the query
		sb.append(" SELECT ITEM_CODE, ITEM_NAME, DEPT_NAME, CAT_NAME CATEGORY_NAME, ");
		sb.append(" DEPT_ID, CATEGORY_ID, UOM_ID, SUB_CATEGORY_ID, RETAILER_ITEM_CODE,  ALT_ITEM_NAME, " );
		sb.append(" ITEM_SIZE, UOM_NAME UOM,  SUB_CAT_NAME SUB_CATEGORY_NAME, " );
		sb.append(" STANDARD_UPC, RETAILER_UPC, MANUFACT_CODE " );
		sb.append(" FROM ITEM_LOOKUP_VIEW  ");
		boolean whereAdded = false;
		if( unknownOnly){
			sb.append(" WHERE DEPT_NAME = 'UNKNOWN'");
			whereAdded = true;
		}
		if( itemCode > 0) {
			sb.append( whereAdded ? " AND" : " WHERE");
			sb.append(" ITEM_CODE = ").append(itemCode);
			whereAdded = true;
		}
		if( chainId > 0) {
			sb.append( whereAdded ? " AND" : " WHERE");
			whereAdded = true;
			sb.append( " ITEM_CODE IN (" );
			sb.append( " SELECT ITEM_CODE FROM COMPETITIVE_DATA_VIEW WHERE COMP_CHAIN_ID = ").append(chainId);
			sb.append( " AND START_DATE > SYSDATE - 240 )");
		}
				
		
//		sb.append( whereAdded ? " AND" : " WHERE");
//		sb.append("  ROWNUM < 100");
		
		sb.append(" ORDER BY ITEM_CODE"); 
		//logger.debug(sb);
		//execute the statement
		crs = PristineDBUtil.executeQuery(conn, sb, "PGDAO - getItemList");
		//logger.debug(  "Item list size - " + crs.size());
		return crs;
		
	}
	
	public void performCleanup(Connection conn, int chainId) throws GeneralException {
		
		StringBuffer sb = new StringBuffer();
		sb.append("DELETE FROM POTENTIAL_ITEM_PAIRS WHERE COMP_CHAIN_ID = ");
		sb.append(chainId);
		PristineDBUtil.execute(conn, sb, "Product Grouping DAO - delete item pairs");
	}
	
	public void performCleanupLikeItemGroup(Connection conn, int chainId) throws GeneralException {
		
		StringBuffer sb = new StringBuffer();
		sb.append("DELETE FROM LIKE_ITEM_GROUP WHERE COMP_CHAIN_ID = ");
		sb.append(chainId);
		PristineDBUtil.execute(conn, sb, "Product Grouping DAO - delete item pairs");
	}
	
	public ArrayList<ItemDTO> getLikeItems( Connection conn, ItemDTO item) throws GeneralException {
		ArrayList<ItemDTO> likeItemList = new ArrayList<ItemDTO>();
		StringBuffer sb = new StringBuffer();
		sb.append(" SELECT ITEM_CODE, ITEM_NAME, STANDARD_UPC FROM ");
		sb.append(" ITEM_LOOKUP_VIEW WHERE");
		sb.append(" DEPT_ID = ").append(item.deptID);
		sb.append(" AND CATEGORY_ID = ").append(item.catID);
		sb.append(" AND SUB_CATEGORY_ID = ").append(item.subCatID);
		//sb.append(" AND ITEM_SIZE = '").append(item.size).append("'");
		
		//item size checking
		float itemSize = Float.parseFloat(item.size);
		sb.append(" AND ITEM_SIZE >= " + (itemSize - itemSize * 0.05));
		sb.append(" AND ITEM_SIZE <= " + (itemSize + itemSize * 0.05));
		sb.append(" AND UOM_ID = '").append(item.uomId).append("'");
		sb.append(" AND ITEM_CODE > ").append(item.itemCode);
		//sb.append(" AND SUBSTR(STANDARD_UPC,").append(Constants.MANUFACTURER_START_INDEX + 1).append(", 5) ");  
		sb.append(" AND MANUFACT_CODE ");
		sb.append(" LIKE '").append(item.manufactCode).append("%'");
		//logger.debug(sb.toString());
		CachedRowSet crs = PristineDBUtil.executeQuery(conn, sb, "PGDAO - getItemList");
		
		try{
			while ( crs.next()){
				ItemDTO likeItem = new ItemDTO();
				likeItem.itemCode = crs.getInt("ITEM_CODE");
				likeItem.itemName = crs.getString("ITEM_NAME");
				likeItem.upc = crs.getString("STANDARD_UPC");
				likeItem.deptID = item.deptID;
				likeItem.catID =  item.catID;
				likeItem.subCatID = item.subCatID;
				likeItem.size = item.size;
				likeItem.uomId = item.uomId;
				likeItemList.add(likeItem);
			}
		}catch(SQLException sqlce){
			throw new GeneralException("Cached Row access likely", sqlce);
		}
		return likeItemList;
	}
	
	
	public ArrayList<ItemDTO> getCrossDeptLikeItems( Connection conn, ItemDTO item) throws GeneralException {
		ArrayList<ItemDTO> likeItemList = new ArrayList<ItemDTO>();
		StringBuffer sb = new StringBuffer();
		sb.append(" SELECT ITEM_CODE, ITEM_NAME, ALT_ITEM_NAME, STANDARD_UPC, ");
		sb.append(" DEPT_NAME, CAT_NAME CATEGORY_NAME, SUB_CAT_NAME SUB_CATEGORY_NAME, SEGMENT_NAME, " );
		sb.append(" DEPT_ID, CATEGORY_ID, SUB_CATEGORY_ID, SEGMENT_ID FROM " );
		sb.append(" ITEM_LOOKUP_VIEW WHERE");
		sb.append(" DEPT_ID <> ").append(item.deptID);
		sb.append(" AND CATEGORY_ID <> ").append(item.catID);
		sb.append(" AND SUB_CATEGORY_ID <> ").append(item.subCatID);
		//sb.append(" AND ITEM_SIZE = '").append(item.size).append("'");
		//sb.append(" AND UOM_ID = '").append(item.uomId).append("'");
		sb.append(" AND ITEM_CODE <>").append(item.itemCode);
		sb.append(" AND STANDARD_UPC LIKE '").append(item.manufactCode).append("%'");
		sb.append(" ORDER BY DEPT_ID, CATEGORY_ID, SUB_CATEGORY_ID");
		//item.itemName = item.itemName.replaceAll("'", "''");
		//sb.append(" AND RDS_ITEM_NAME LIKE '").append(item.itemName).append("%'");
		
		//logger.debug(sb.toString());
		
		CachedRowSet crs = PristineDBUtil.executeQuery(conn, sb, "PGDAO - getItemList");
		
		try{
			while ( crs.next()){
				ItemDTO likeItem = new ItemDTO();
				likeItem.itemCode = crs.getInt("ITEM_CODE");
				likeItem.itemName = crs.getString("ITEM_NAME");
				//likeItem.likeItemGrp = crs.getString("LIKE_ITEM_GRP");
				likeItem.upc = crs.getString("STANDARD_UPC");
				likeItem.deptID = crs.getInt("DEPT_ID");
				likeItem.catID =  PrestoUtil.getIntValue( crs, "CATEGORY_ID");
				likeItem.subCatID = PrestoUtil.getIntValue( crs, "SUB_CATEGORY_ID");
				likeItem.segmentID = PrestoUtil.getIntValue( crs, "SEGMENT_ID");
				
				likeItem.deptName = crs.getString("DEPT_NAME");
				likeItem.catName =  crs.getString("CATEGORY_NAME");
				likeItem.subCatName = crs.getString("SUB_CATEGORY_NAME");
				likeItem.segmentName = crs.getString("SEGMENT_NAME");
				//likeItem.size = item.size;
				//likeItem.uomId = item.uomId;
				likeItemList.add(likeItem);
			}
		}catch(SQLException sqlce){
			throw new GeneralException("Cached Row access likely", sqlce);
		}
		return likeItemList;
	}
	
	
	public void createPotentialLikeitems ( Connection conn, int chainId, int baseItemCode, int likeItemCode) throws GeneralException {
		StringBuffer sb = new StringBuffer();
		sb.append("INSERT INTO POTENTIAL_ITEM_PAIRS ( COMP_CHAIN_ID, ");
		sb.append(" BASE_ITEM_CODE, RELATED_ITEM_CODE ) VALUES ( ");
		sb.append(chainId).append(", ");
		sb.append(baseItemCode).append(", ");
		sb.append(likeItemCode).append(") ");
		//logger.debug(sb.toString());
		PristineDBUtil.execute(conn, sb, "Product Grouping DAO - insert potential item pairs");
	}
	
	public CachedRowSet getPotentialRelatedItem(Connection conn, int chainId, int scheduleId, 
			int baseItemcode) throws GeneralException {
		CachedRowSet crs = null;
		StringBuffer sb = new StringBuffer();
		sb.append(" SELECT BASE_ITEM_CODE, RELATED_ITEM_CODE ");
		sb.append(" FROM  POTENTIAL_ITEM_PAIRS WHERE ");
		sb.append(" COMP_CHAIN_ID = ").append(chainId).append(" AND ");
		sb.append(" BASE_ITEM_CODE = ").append(baseItemcode).append(" AND ");
		sb.append(" RELATED_ITEM_CODE IN ");
		sb.append(" ( SELECT ITEM_CODE FROM COMPETITIVE_DATA WHERE SCHEDULE_ID = ").append(scheduleId);
		sb.append(" AND ITEM_CODE > ").append(baseItemcode).append(")");
		//logger.debug(sb);
		//execute the statement
		crs = PristineDBUtil.executeQuery(conn, sb, "PGDAO - getRelatedItemList");
		return crs;
	}
	
	public void incrementMatchCount(Connection conn, int chainId, 
			int baseItemcode,int relatedItemcode) throws GeneralException {
		StringBuffer sb = new StringBuffer();
		sb.append("UPDATE POTENTIAL_ITEM_PAIRS SET PRICE_MATCH_COUNT = PRICE_MATCH_COUNT + 1 ");
		sb.append(" WHERE ");
		sb.append(" COMP_CHAIN_ID = ").append(chainId).append(" AND ");
		sb.append(" BASE_ITEM_CODE = ").append(baseItemcode).append(" AND ");
		sb.append(" RELATED_ITEM_CODE = ").append(relatedItemcode);
		//logger.debug(sb.toString());
		int recCount = PristineDBUtil.executeUpdate(conn, sb, "Product Grouping DAO - update Match count");
		if (recCount != 1)
			logger.error("Potential error in Updating matching count");
		return;
	}
	public void incrementUnMatchCount(Connection conn, int chainId, 
			int baseItemcode,int relatedItemcode) throws GeneralException {
		StringBuffer sb = new StringBuffer();
		sb.append("UPDATE POTENTIAL_ITEM_PAIRS SET PRICE_NOTMATCH_COUNT = PRICE_NOTMATCH_COUNT + 1 ");
		sb.append(" WHERE ");
		sb.append(" COMP_CHAIN_ID = ").append(chainId).append(" AND ");
		sb.append(" BASE_ITEM_CODE = ").append(baseItemcode).append(" AND ");
		sb.append(" RELATED_ITEM_CODE = ").append(relatedItemcode);
		//logger.debug(sb.toString());
		int recCount = PristineDBUtil.executeUpdate(conn, sb, "Product Grouping DAO - update Not Match count");
		if (recCount != 1)
			logger.error("Potential error in Updating not matching count");
		return;
	}
	
	public CachedRowSet getDisinctBaseItems(Connection conn, int chainId) throws GeneralException {
		CachedRowSet crs = null;
		StringBuffer sb = new StringBuffer();
		sb.append(" SELECT DISTINCT BASE_ITEM_CODE ");
		sb.append(" FROM  POTENTIAL_ITEM_PAIRS WHERE ");
		sb.append(" COMP_CHAIN_ID = ").append(chainId);
		logger.debug(sb);
		//execute the statement
		crs = PristineDBUtil.executeQuery(conn, sb, "PGDAO - getRelatedItemList");
		return crs;
	}
	
	public CachedRowSet getPotentialItems(Connection conn, int chainId) throws GeneralException {
		CachedRowSet crs = null;
		StringBuffer sb = new StringBuffer();
		sb.append(" SELECT BASE_ITEM_CODE, RELATED_ITEM_CODE ");
		sb.append(" FROM  POTENTIAL_ITEM_PAIRS WHERE ");
		sb.append(" COMP_CHAIN_ID = ").append(chainId);
		//sb.append(" AND ROWNUM < 100");
		//logger.debug(sb);
		//execute the statement
		crs = PristineDBUtil.executeQuery(conn, sb, "PGDAO - getRelatedItemList");
		return crs;
	}
	
	public CachedRowSet getDisinctRelatedItemCode(Connection conn, int chainId) throws GeneralException {
		CachedRowSet crs = null;
		StringBuffer sb = new StringBuffer();
		sb.append(" SELECT DISTINCT RELATED_ITEM_CODE ");
		sb.append(" FROM  POTENTIAL_ITEM_PAIRS WHERE ");
		sb.append(" COMP_CHAIN_ID = ").append(chainId);
		//logger.debug(sb);
		//execute the statement
		crs = PristineDBUtil.executeQuery(conn, sb, "PGDAO - getRelatedItemList");
		return crs;
	}
	
	public CachedRowSet getItemHierarchy(Connection conn, int chainId, int unknownItemCode) throws GeneralException {
		CachedRowSet crs = null;
		StringBuffer sb = new StringBuffer();
		sb.append(" select A.dept_id, A.category_id, A.sub_category_id, count(*)  ");
		sb.append(" from potential_item_pairs B, item_lookup A where ");
		sb.append(" COMP_CHAIN_ID = ").append(chainId);
		sb.append(" AND B.related_item_code = ").append(unknownItemCode);
		sb.append(" AND B.base_item_code = A.item_code ");
		sb.append(" group by A.dept_id, A.category_id, A.sub_category_id ");
		sb.append(" order by count(*) desc");
		//logger.debug(sb);
		//execute the statement
		crs = PristineDBUtil.executeQuery(conn, sb, "PGDAO - getItemHierachyList");
		return crs;
	}
	
	public int updateItemHierarchy(Connection conn,ItemDTO item) throws GeneralException {
		StringBuffer sb = new StringBuffer();
		sb.append("UPDATE ITEM_LOOKUP SET DEPT_ID = ").append(item.deptID).append(",");
		if ( item.catID > 0)
			sb.append(" CATEGORY_ID = ").append(item.catID).append(",");
		else
			sb.append(" CATEGORY_ID = ").append("NULL").append(",");
		if ( item.subCatID > 0)
			sb.append(" SUB_CATEGORY_ID = ").append(item.subCatID).append(",");
		else
			sb.append(" SUB_CATEGORY_ID = ").append("NULL").append(",");
		if ( item.segmentID > 0)
			sb.append(" SEGMENT_ID = ").append(item.segmentID);
		else
			sb.append(" SEGMENT_ID = ").append("NULL");
		
		sb.append(" WHERE ITEM_CODE = ").append(item.itemCode);
		//logger.debug(sb.toString());
		int recCount = PristineDBUtil.executeUpdate(conn, sb, "Product Grouping DAO - update Match count");
		if (recCount != 1)
			logger.error("Potential error in Updating matching count");
		return recCount;		
	}

	public CachedRowSet getAllRelatedItems(Connection conn, int chainId, int itemCode)
				throws GeneralException {
		CachedRowSet crs = null;
		StringBuffer sb = new StringBuffer();
		sb.append(" select RELATED_ITEM_CODE ITEM_CODE FROM potential_item_pairs ");
		sb.append(" where comp_chain_id = ").append(chainId);
		sb.append(" and base_item_code = ").append(itemCode);
		sb.append(" UNION ");
		sb.append(" select BASE_ITEM_CODE ITEM_CODE FROM potential_item_pairs  ");
		sb.append(" where comp_chain_id = ").append(chainId);
		sb.append(" and RELATED_ITEM_CODE = ").append(itemCode);
		//logger.debug(sb);
		//execute the statement
		crs = PristineDBUtil.executeQuery(conn, sb, "PGDAO - getRelatedItemList");
		return crs;
	}
	//csItemCodes represent coma separated Item codes
	public void deleteAllRelatedItems(Connection conn, int chainId, String csItemCodes)
						throws GeneralException {
		
		StringBuffer sb = new StringBuffer();
		sb.append("DELETE FROM POTENTIAL_ITEM_PAIRS WHERE COMP_CHAIN_ID = ");
		sb.append(chainId);
		sb.append( " AND ( BASE_ITEM_CODE in ( ").append(csItemCodes).append(")");
		sb.append( " OR RELATED_ITEM_CODE in ( ").append(csItemCodes).append("))");
		//logger.debug(sb);
		PristineDBUtil.execute(conn, sb, "Product Grouping DAO - delete item pairs");
		return;
	}
	
	public void createLikeItemGroup(Connection conn, int chainId, int baseItemCode, int likeItemCode) 
					throws GeneralException {
		
		StringBuffer sb = new StringBuffer();
		sb.append("INSERT INTO LIKE_ITEM_GROUP ( COMP_CHAIN_ID, ");
		sb.append(" BASE_ITEM_CODE, RELATED_ITEM_CODE ) VALUES ( ");
		sb.append(chainId).append(", ");
		sb.append(baseItemCode).append(", ");
		sb.append(likeItemCode).append(") ");
		//logger.debug(sb.toString());
		PristineDBUtil.execute(conn, sb, "Product Grouping DAO - insert potential item pairs");
	}
	
	public void cleanupCompDataNoFlavor( Connection conn, int scheduleId) throws GeneralException {
		StringBuffer sb = new StringBuffer();
		sb.append("DELETE FROM COMP_DATA_NO_FLAVOR WHERE CHECK_DATA_ID IN ");
		sb.append( "( SELECT CHECK_DATA_ID  FROM COMPETITIVE_DATA WHERE SCHEDULE_ID = ").append(scheduleId).append(")");
			//logger.debug(sb);
		PristineDBUtil.execute(conn, sb, "Product Grouping DAO - delete comp data no flavors");
		return;
	}
	
	public int getPrestoLikeItemCode( Connection conn, int chainId, int relatedItemCode) throws
		GeneralException {
		StringBuffer sb = new StringBuffer();
		sb.append(" select BASE_ITEM_CODE FROM LIKE_ITEM_GROUP ");
		sb.append(" where comp_chain_id = ").append(chainId);
		sb.append(" and RELATED_ITEM_CODE = ").append(relatedItemCode);
		
		int retVal = -1;
		String strVal =  PristineDBUtil.getSingleColumnVal(conn, sb, "Get Presto Like Item");
		if( strVal != null )
			retVal = Integer.parseInt(strVal);
		return retVal;
	}
	
	public void populateCompDataNoFlavor(Connection conn, int checkItemId, int likeItemCode) throws
		GeneralException {
		StringBuffer sb = new StringBuffer();
		sb.append("INSERT INTO COMP_DATA_NO_FLAVOR( CHECK_DATA_ID, ");
		sb.append(" BASE_ITEM_CODE ) VALUES ( ");
		sb.append(checkItemId).append(", ");
		sb.append(likeItemCode).append(") ");
		//logger.debug(sb.toString());
		PristineDBUtil.execute(conn, sb, "Product Grouping DAO - insert potential item pairs");
	}
	
	public CachedRowSet getPriceMatchData( Connection conn,String csSchList, int baseItem, int relatedItem) throws 
		GeneralException {
		CachedRowSet crs = null;
		StringBuffer sb = new StringBuffer();
		sb.append("select ");
		sb.append(" NVL(SUM(DECODE( unitprice(a.reg_price, a.reg_m_price, a.reg_m_pack) - ");
		sb.append("unitprice(b.reg_price, b.reg_m_price, b.reg_m_pack), 0, 1, 0)),0)PRICE_MATCH_CT,");
		sb.append(" NVL(SUM(DECODE( (unitprice(a.reg_price, a.reg_m_price, a.reg_m_pack) - ");
		sb.append("unitprice(b.reg_price, b.reg_m_price, b.reg_m_pack)), 0, 0, 1)),0) PRICE_NOT_MATCH_CT");
		sb.append(" from competitive_data A, competitive_data B where");
		sb.append(" A.schedule_id in (").append(csSchList).append(") and");
		sb.append(" B.schedule_id = A.schedule_id and ");
		sb.append(" A.item_code = ").append(baseItem);
		sb.append(" AND b.item_code = ").append(relatedItem);
		//logger.debug(sb);
		//execute the statement
		crs = PristineDBUtil.executeQuery(conn, sb, "PGDAO - getRelatedItemList");
		return crs;
	}
	
	/*
	 * This function brings out a list of items unknown in current DB but carried in other DB
	 */
	public CachedRowSet getUnknownCarriedInOtherDB(Connection conn) throws GeneralException {
		CachedRowSet crs = null;
		StringBuffer sb = new StringBuffer();
		sb.append("select A.ITEM_CODE, A.ITEM_NAME, A.STANDARD_UPC, A.UOM_ID FROM ITEM_LOOKUP_VIEW A ");
		sb.append(" WHERE DEPT_NAME = 'UNKNOWN'");
		sb.append(" AND A.STANDARD_UPC in (");
		sb.append("SELECT STANDARD_UPC FROM ITEM_LOOKUP_VIEW@").append(DBManager.db_link_name);
		sb.append(" WHERE DEPT_NAME <> 'UNKNOWN')");
//		sb.append(" AND ROWNUM < 50");
		logger.debug(sb);
		crs = PristineDBUtil.executeQuery(conn, sb, "PGDAO - getRelatedItemList");
		return crs;
	}
	
	public ArrayList<ItemDTO> getPeerItemsFromOtherDB(Connection conn,String upc) throws GeneralException{
		CachedRowSet crs = null;
		StringBuffer sb = new StringBuffer();
		sb.append(" SELECT ITEM_CODE, ITEM_NAME, DEPT_NAME, CAT_NAME CATEGORY_NAME, ");
		sb.append(" DEPT_ID, CATEGORY_ID, UOM_ID, SUB_CATEGORY_ID, RETAILER_ITEM_CODE,  ALT_ITEM_NAME, " );
		sb.append(" ITEM_SIZE, UOM_NAME UOM,  SUB_CAT_NAME SUB_CATEGORY_NAME, " );
		sb.append(" STANDARD_UPC, RETAILER_UPC, MANUFACT_CODE " );
		sb.append(" FROM ITEM_LOOKUP_VIEW@").append(DBManager.db_link_name);
		sb.append(" WHERE  (DEPT_ID, CATEGORY_ID, SUB_CATEGORY_ID) = ( SELECT" );
		sb.append(" DEPT_ID, CATEGORY_ID, SUB_CATEGORY_ID ");
		sb.append(" FROM ITEM_LOOKUP_VIEW@").append(DBManager.db_link_name);
		sb.append(" WHERE STANDARD_UPC = '").append(upc).append("')");
		sb.append(" AND STANDARD_UPC <> '").append(upc).append("'");
		sb.append(" AND ROWNUM < 900");
		//logger.debug(sb);
		
		crs = PristineDBUtil.executeQuery(conn, sb, "PGDAO - getPeerItemList");
		
		ArrayList<ItemDTO> peerItemList = new ArrayList<ItemDTO>();
		try{
			while ( crs.next()){
				ItemDTO peerItem = new ItemDTO();
				peerItem.itemCode = crs.getInt("ITEM_CODE");
				peerItem.itemName = crs.getString("ITEM_NAME");
				peerItem.upc = crs.getString("STANDARD_UPC");
				peerItem.deptID = crs.getInt("DEPT_ID");
				peerItem.catID =  crs.getInt("CATEGORY_ID");
				peerItem.subCatID = crs.getInt("SUB_CATEGORY_ID");
				peerItem.deptName = crs.getString("DEPT_NAME");
				peerItem.catName = crs.getString("CATEGORY_NAME");
				peerItem.subCatName = crs.getString("SUB_CATEGORY_NAME");
				peerItemList.add(peerItem);
			}
		}catch(SQLException sqlce){
			throw new GeneralException("Cached Row access likely", sqlce);
		}
		
		return peerItemList;
	}
	
	
	public ArrayList<ItemDTO> getItemHierarchy(Connection conn,String upc, ArrayList<ItemDTO> peerItemList) throws 
		GeneralException {
		
		ArrayList<ItemDTO> similarItemList = new ArrayList<ItemDTO>();
		if( peerItemList.size() > 0){
			CachedRowSet crs = null;
			StringBuffer sb = new StringBuffer();
			sb.append(" SELECT ITEM_CODE, ITEM_NAME, DEPT_NAME, CAT_NAME CATEGORY_NAME, ");
			sb.append(" DEPT_ID, CATEGORY_ID, UOM_ID, SUB_CATEGORY_ID, RETAILER_ITEM_CODE,  ALT_ITEM_NAME, " );
			sb.append(" ITEM_SIZE, UOM_NAME UOM,  SUB_CAT_NAME SUB_CATEGORY_NAME, " );
			sb.append(" STANDARD_UPC, RETAILER_UPC, MANUFACT_CODE, " );
			sb.append(" SEGMENT_NAME, SEGMENT_ID" );
			sb.append(" FROM ITEM_LOOKUP_VIEW " );
			sb.append(" WHERE  STANDARD_UPC IN ( " );
			Iterator<ItemDTO> peerItemListItr = peerItemList.iterator();
			boolean isFirst = true;
			while (peerItemListItr.hasNext()){
				ItemDTO peerItem = peerItemListItr.next();
				if( !isFirst ){
					sb.append(", ");
				}
				isFirst = false;
				sb.append("'").append(peerItem.upc).append("'");
			}
			sb.append(" ) AND DEPT_NAME <> 'UNKNOWN'");
			//logger.debug(sb);
			
			crs = PristineDBUtil.executeQuery(conn, sb, "PGDAO - getPeerItemList");
			
			try{
				while ( crs.next()){
					ItemDTO similarItem = new ItemDTO();
					similarItem.itemCode = crs.getInt("ITEM_CODE");
					similarItem.itemName = crs.getString("ITEM_NAME");
					similarItem.upc = crs.getString("STANDARD_UPC");
					similarItem.deptID = crs.getInt("DEPT_ID");
					similarItem.catID =  PrestoUtil.getIntValue( crs, "CATEGORY_ID");
					similarItem.subCatID = PrestoUtil.getIntValue( crs, "SUB_CATEGORY_ID");
					similarItem.deptName = crs.getString("DEPT_NAME");
					similarItem.catName = crs.getString("CATEGORY_NAME");
					similarItem.subCatName = crs.getString("SUB_CATEGORY_NAME");
					similarItem.segmentID= PrestoUtil.getIntValue( crs, "SEGMENT_ID");
					similarItem.segmentName= crs.getString("SEGMENT_NAME");
					similarItemList.add(similarItem);
				}
			}catch(SQLException sqlce){
				throw new GeneralException("Cached Row access likely", sqlce);
			}
		}
		return similarItemList;
	}
	public ArrayList<ItemDTO> getNarrowItemTree( Connection conn, ItemDTO item) throws GeneralException {
		ArrayList<ItemDTO> likeItemList = new ArrayList<ItemDTO>();
		StringBuffer sb = new StringBuffer();
		sb.append(" SELECT ITEM_CODE, ITEM_NAME, DEPT_NAME, CAT_NAME CATEGORY_NAME, ");
		sb.append(" DEPT_ID, CATEGORY_ID, UOM_ID, SUB_CATEGORY_ID, RETAILER_ITEM_CODE,  ALT_ITEM_NAME, " );
		sb.append(" ITEM_SIZE, UOM_NAME UOM,  SUB_CAT_NAME SUB_CATEGORY_NAME, SEGMENT_NAME, SEGMENT_ID," );
		sb.append(" STANDARD_UPC, RETAILER_UPC, MANUFACT_CODE " );
		sb.append(" FROM ITEM_LOOKUP_VIEW WHERE" );
		sb.append(" ITEM_CODE <>").append(item.itemCode);
		if ( item.manufactCode != null)
			sb.append(" AND STANDARD_UPC LIKE '").append(item.manufactCode).append("%'");
		if(item.deptID >0 )
			sb.append(" AND DEPT_ID = ").append(item.deptID);
		if(item.catID >0 )
			sb.append(" AND CATEGORY_ID = ").append(item.catID);
		else
			sb.append(" AND CATEGORY_ID IS NOT NULL ");
		sb.append(" AND SUB_CATEGORY_ID IS NOT NULL ");
		if ( item.itemName != null)
			sb.append(" AND ITEM_NAME LIKE '").append(item.itemName).append("%' ");
		if ( item.uomId != null)
			sb.append(" AND UOM_ID = '").append(item.uomId).append("' ");
		sb.append(" AND DEPT_NAME <> 'UNKNOWN'");
		sb.append(" ORDER BY DEPT_ID, CATEGORY_ID, SUB_CATEGORY_ID");
		//item.itemName = item.itemName.replaceAll("'", "''");
		//sb.append(" AND RDS_ITEM_NAME LIKE '").append(item.itemName).append("%'");
		
		//logger.debug(sb.toString());
		
		CachedRowSet crs = PristineDBUtil.executeQuery(conn, sb, "PGDAO - getItemList");
		
		try{
			while ( crs.next()){
				ItemDTO likeItem = new ItemDTO();
				likeItem.itemCode = crs.getInt("ITEM_CODE");
				likeItem.itemName = crs.getString("ITEM_NAME");
				//likeItem.likeItemGrp = crs.getString("LIKE_ITEM_GRP");
				likeItem.upc = crs.getString("STANDARD_UPC");
				likeItem.deptID = crs.getInt("DEPT_ID");
				likeItem.catID =   PrestoUtil.getIntValue( crs, "CATEGORY_ID");
				likeItem.subCatID = PrestoUtil.getIntValue( crs, "SUB_CATEGORY_ID");
				likeItem.segmentID= PrestoUtil.getIntValue( crs,"SEGMENT_ID");
				likeItem.segmentName= crs.getString("SEGMENT_NAME");
				likeItem.deptName = crs.getString("DEPT_NAME");
				likeItem.catName = crs.getString("CATEGORY_NAME");
				likeItem.subCatName = crs.getString("SUB_CATEGORY_NAME");
				likeItemList.add(likeItem);
			}
		}catch(SQLException sqlce){
			throw new GeneralException("Cached Row access likely " + sb.toString(), sqlce);
		}
		return likeItemList;
	}
	
	public int getFlavorCount(Connection conn,int chainId,int scheduleId,int likeItemCode, boolean carriedOnly) throws
		GeneralException {
		
		StringBuffer sb = new StringBuffer();
		sb.append(" SELECT COUNT(ITEM_CODE) FROM COMPETITIVE_DATA_VIEW A WHERE ");
		sb.append(" SCHEDULE_ID = ").append(scheduleId);
		sb.append(" AND ITEM_CODE IN ");
		sb.append(" (select RELATED_ITEM_CODE FROM LIKE_ITEM_GROUP ");
		sb.append(" where comp_chain_id = ").append(chainId);
		sb.append(" and BASE_ITEM_CODE = ").append(likeItemCode).append(")");
		if( carriedOnly) 
			sb.append(" and RETAILER_ITEM_CODE <> '0'");
		int retVal = -1;
		String strVal =  PristineDBUtil.getSingleColumnVal(conn, sb, "Get Flavor count");
		if( strVal != null )
			retVal = Integer.parseInt(strVal);
		return retVal;
	}
	
	public boolean isCarriedItem(Connection conn, int itemcode) throws GeneralException {
		StringBuffer sb = new StringBuffer();
		sb.append(" SELECT RETAILER_ITEM_CODE FROM ITEM_LOOKUP WHERE ITEM_CODE = ");
		sb.append(itemcode);
		String strVal =  PristineDBUtil.getSingleColumnVal(conn, sb, "IsCarroieItem");
		boolean carriedItem = true;
		if( strVal.equals("0"))
			carriedItem = false;
		return carriedItem;
	}

	/**
	 * Populates Product_Group and Product_Group_Relation tables
	 * @param conn		Connection
	 * @param item		ItemDTO from which data has to be populated
	 */
	public void populateProductGroupHierarchy(Connection conn, ItemDTO item) throws GeneralException{
		
		if(productLevels == null)
			productLevels = getProductLevels(conn);
		if(excludeProductLevels == null)
			excludeProductLevels = getExcludeProductLevels();
		if(childParentMap == null)
			childParentMap = getParentChildProductLevels(conn);
		
		// Data Setup in Product_Group table starts
		HashMap<Integer, Integer> productIdMap = new HashMap<Integer, Integer>();
		//Sorting the Product level in descending order
		Collections.sort(productLevels, (a,b) -> b.compareTo(a));
		for(Integer productLevel : productLevels){
			if(!excludeProductLevels.contains(productLevel)){
				int productId = setupProductGroup(conn, productLevel, item,productIdMap);
				if(productId != -1){
					logger.debug("Prod Level id"+productLevel +" Prod Id: "+productId);
					productIdMap.put(productLevel, productId);
				}
					
			}
			else if (productLevel == 1){
				productIdMap.put(productLevel, item.itemCode);
			}
		}
		
		// Data Setup in Product_group table ends
		for(Map.Entry<Integer, Integer> entry : productIdMap.entrySet()){
			List<Integer> parentLevels = childParentMap.get(entry.getKey());
			
			if(parentLevels != null){
				for(Integer parentLevel : parentLevels){
					if(!excludeProductLevels.contains(entry.getKey()) || Constants.ITEMLEVELID == (entry.getKey())){
						if(productIdMap.get(parentLevel) == null)
							continue;
						ProductDTO product = new ProductDTO();
						product.setProductLevelId(parentLevel);
						product.setProductId(productIdMap.get(parentLevel));
						product.setChildProductLevelId(entry.getKey());
						product.setChildProductId(entry.getValue());
						
						int parentProductIdInDB = getParentProductIdFromProductRelation(conn, product);
						
						if(parentProductIdInDB != -1){					
							if(parentProductIdInDB != productIdMap.get(parentLevel)){
								updateProductGroupRelation(conn, product);
							}
						}else{
							insertProductGroupRelation(conn, product);
						}
					}
				}
			}
		}
		// Data setup in Product_Group_Relation starts
		
	}

	/**
	 * Returns list of product levels
	 */
	public List<Integer> getProductLevels(Connection conn) throws GeneralException{
		List<Integer> productLevelsList = new ArrayList<Integer>();
		PreparedStatement statement = null;
	    ResultSet resultSet = null;
		try{
			statement = conn.prepareStatement(GET_PRODUCT_GROUP_LEVELS);
	        resultSet = statement.executeQuery();
	        while(resultSet.next()){
	        	productLevelsList.add(resultSet.getInt("PRODUCT_LEVEL_ID"));
	        }
		}
		catch (SQLException e)
		{
			logger.error("Error while executing GET_PRODUCT_GROUP_TYPES");
			throw new GeneralException("Error while executing GET_PRODUCT_GROUP_TYPES", e);
		}finally{
			PristineDBUtil.close(resultSet);
			PristineDBUtil.close(statement);
		}
		return productLevelsList;
	}
	
	/**
	 * Returns list of product levels
	 */
	public HashMap<Integer, List<Integer>> getParentChildProductLevels(Connection conn) throws GeneralException{
		HashMap<Integer, List<Integer>> childParentMap = new HashMap<Integer, List<Integer>>();
		PreparedStatement statement = null;
	    ResultSet resultSet = null;
		try{
			statement = conn.prepareStatement(GET_PARENT_CHILD_PRODUCT_LEVELS);
	        resultSet = statement.executeQuery();
	        while(resultSet.next()){
	        	int childProductLevelId = resultSet.getInt("CHILD_PRODUCT_LEVEL_ID");
	        	if(childParentMap.get(childProductLevelId) != null){
	        		List<Integer> tempList = childParentMap.get(childProductLevelId);
	        		tempList.add(resultSet.getInt("PRODUCT_LEVEL_ID"));
	        		childParentMap.put(childProductLevelId, tempList);
	        	}else{
	        		List<Integer> tempList = new ArrayList<Integer>();
	        		tempList.add(resultSet.getInt("PRODUCT_LEVEL_ID"));
	        		childParentMap.put(childProductLevelId, tempList);
	        	}
	        }
		}
		catch (SQLException e)
		{
			logger.error("Error while executing GET_PARENT_CHILD_PRODUCT_LEVELS");
			throw new GeneralException("Error while executing GET_PARENT_CHILD_PRODUCT_LEVELS", e);
		}finally{
			PristineDBUtil.close(resultSet);
			PristineDBUtil.close(statement);
		}
		return childParentMap;
	}
	
	/**
	 * Returns list of product levels which need not be populated in
	 * product group tables
	 * @return	List of Product Levels
	 */
	public List<Integer> getExcludeProductLevels(){
		List<Integer> excludeList = new ArrayList<Integer>();
		String excludeLevelsStr = PropertyManager.getProperty("ITEM_LOADER.PRODUCT_GROUP_SETUP.EXCLUDELEVELS");
		if(excludeLevelsStr != null){
			String[] productLevelArr = excludeLevelsStr.split(",");
			for(String productLevel : productLevelArr){
				excludeList.add(Integer.parseInt(productLevel));
			}
		}
		
		return excludeList;
	}
	
	/**
	 * Retrieves Product from database. Inserts product into database if not present.
	 * @param conn				Connection
	 * @param productLevelId	Product Level Id
	 * @param itemDTO			Item
	 * @return					Product Id
	 * @throws GeneralException
	 */
	public int setupProductGroup(Connection conn, int productLevelId, ItemDTO itemDTO, HashMap<Integer, Integer> productIdMap) throws GeneralException{
		ProductDTO product = getProductNameAndCode(productLevelId, itemDTO);
		
		logger.debug("Prod code - " + product.getProductCode()+" Prod Name - " + product.getProductName()+" Prod levelId - " + productLevelId);
		product.setProductLevelId(productLevelId);
		
		/*
		 * Ahold gets records with no product name but only product code. This code retrieves product id
		 * if product name is not present and only product code is present when code is available in the database.
		 * But does not insert a new record if product name is empty. 
		 */
		int productId = getProductGroup(conn, product, productIdMap);
		

		if(productId == -1 && (product.getProductName() == null || product.getProductName().equals(""))){
			product.setProductName("UNDEFINED");
		}
		
		if(productId != -1){
			product.setProductId(productId);
			if(!(product.getProductName() == null || product.getProductName().equals(""))){
				updateProductGroup(conn, product);
			}
			updateProductId(conn, productLevelId, productId, itemDTO);
			return productId;
		}else{
			boolean useUnusedSeq = false;
	    	if(PropertyManager.getProperty("USE_UNUSED_PRODUCT_GROUP_SEQUENCE") != null){
	    		useUnusedSeq = Boolean.parseBoolean(
	    				PropertyManager.getProperty("USE_UNUSED_PRODUCT_GROUP_SEQUENCE"));
	    	}
	    	int newProductId = 0;
			if (useUnusedSeq) {
				newProductId = getUnusedSeqFromProductGroup(conn);
			} else if (!useUnusedSeq || newProductId == 0) {
				newProductId = getProductIdNextVal(conn);
			}
	    	insertProductGroup(conn, product, newProductId);
	    	//logger.info("Prod Level id"+productLevelId +" Prod Id: "+newProductId);
	    	productId = newProductId;
			updateProductId(conn, productLevelId, productId, itemDTO);
			return productId;
		}
	}
	
//	/**
//	 * Inserts into Product_Group table
//	 * @param conn				Connection
//	 * @param productLevelId	Product Level Id
//	 * @param productName		Product Name
//	 * @param productCode		Product Code
//	 * @throws GeneralException
//	 */
//	private void insertProductGroup(Connection conn, ProductDTO product) throws GeneralException{
//		PreparedStatement statement = null;
//	    ResultSet resultSet = null;
//		try{
//			statement = conn.prepareStatement(INSERT_PRODUCT_GROUP);
//			statement.setInt(1, product.getProductLevelId());
//			statement.setString(2, product.getProductName());
//			statement.setString(3, product.getProductCode());
//	        statement.executeUpdate();
//		}
//		catch (SQLException e)
//		{
//			logger.error("Error while executing INSERT_PRODUCT_GROUP");
//			throw new GeneralException("Error while executing INSERT_PRODUCT_GROUP", e);
//		}finally{
//			PristineDBUtil.close(resultSet);
//			PristineDBUtil.close(statement);
//		}
//	}
    
	/**
	 * Updates code in Product_Group table
	 * @param conn				Connection
	 * @param productLevelId	Product Level Id
	 * @param productName		Product Name
	 * @param productCode		Product Code
	 * @throws GeneralException
	 */
	private void updateProductGroup(Connection conn, ProductDTO product) throws GeneralException{
		PreparedStatement statement = null;
	    ResultSet resultSet = null;
		try{
			//Update name with respect to Code and Product level id (UPDATED on 08/24/2016 By DINESH)
			statement = conn.prepareStatement(UPDATE_PRODUCT_GROUP);
			statement.setString(1, product.getProductName());
			statement.setInt(2, product.getProductLevelId());
			statement.setInt(3, product.getProductId());
			statement.executeUpdate();
		}
		catch (SQLException e)
		{
			logger.error("Error while executing UPDATE_PRODUCT_GROUP");
			throw new GeneralException("Error while executing UPDATE_PRODUCT_GROUP", e);
		}finally{
			PristineDBUtil.close(resultSet);
			PristineDBUtil.close(statement);
		}
	}
	
    /**
     * Returns Product Id from Product Group Table
     * @param conn				Connection
     * @param productLevelId	Product Level Id
     * @param name				Product Name
     * @param code				Product Code
     * @return					Product Id
     * @throws GeneralException
     */
	public int getProductGroup(Connection conn, ProductDTO productDTO, HashMap<Integer, Integer> productIdMap) throws GeneralException{
		int productId = -1;
		int parentLevelId = 0;
		int parentProductId = -1;
		
		PreparedStatement statement = null;
	    ResultSet resultSet = null;
	    String sql = GET_PRODUCT_GROUP;
	    //If Code is null where condition must include name else only Code needs to be used(UPDATED on 08/24/2016 By DINESH) 
	    if(productDTO.getProductCode() != null && !productDTO.getProductCode().equals("")){
	    	sql = sql + " AND PG.CODE = ?";
	    }
	    else if(productDTO.getProductName() != null &&  !productDTO.getProductName().equals("")){
	    	sql = sql + " AND UPPER(PG.NAME) = UPPER(?)";
	    }else{
	    	sql = sql + " AND PG.CODE IS NULL";
	    }
	    
	   
	    int higerLevelId= Integer.parseInt(PropertyManager.getProperty("ITEM_LOAD.HIGHER_LEVEL_ID"));
	    String levelIds = PropertyManager.getProperty("ITEM_LOAD.PRODUCT_LEVEL_IDS");
	    List<Integer> processLevelIds = new ArrayList<Integer>();
		String[] levelId = levelIds.split(",");
		for (int i = 0; i < levelId.length; i++) {
			processLevelIds.add(Integer.parseInt(levelId[i].trim()));
		}
		
		 if(productDTO.getProductLevelId() < higerLevelId && processLevelIds.contains(productDTO.getProductLevelId())){
			 List<Integer> parentLevelList = childParentMap.get(productDTO.getProductLevelId());
				//To get parent level Id for the given product level id
				for(int parentId: parentLevelList){
					if(processLevelIds.contains(parentId)){
						parentLevelId = parentId;
					}
				}
				
				if(parentLevelId > 0){
					parentProductId = productIdMap.get(parentLevelId);
					if(productIdMap.get(parentLevelId) == null){
						logger.error("Parent product Id is null for the given code: "+productDTO.getProductCode());
					}
				}
		    }
		//Need to process only if Parent Product Id greater than 0
		if(parentProductId >0){
	    	sql = sql + " AND PGR.PRODUCT_ID = ?";
	    }
		try{
			statement = conn.prepareStatement(sql);
			int counter = 0;
			statement.setInt(++counter, productDTO.getProductLevelId());
			
			if(productDTO.getProductCode() != null && !productDTO.getProductCode().equals("")){
				statement.setString(++counter, productDTO.getProductCode());
			}
			else if(productDTO.getProductName() != null && !productDTO.getProductName().equals("")){
				statement.setString(++counter, productDTO.getProductName());
			}
			//Parent product id is added to setup the item hierarchy properly By Dinesh(04/05/2017)
			
			if(parentProductId >0){
				if(parentLevelId > 0){
					statement.setInt(++counter, parentProductId);
				}
			}
	        resultSet = statement.executeQuery();
	        if(resultSet.next()){
	        	productId = resultSet.getInt("PRODUCT_ID");
	        }
		}
		catch (SQLException e)
		{
			logger.error("Error while executing GET_PRODUCT_GROUP");
			throw new GeneralException("Error while executing GET_PRODUCT_GROUP", e);
		}finally{
			PristineDBUtil.close(resultSet);
			PristineDBUtil.close(statement);
		}
		return productId;
	}
	
	/**
	 * Updates Product_Id in respective tables
	 * @param conn
	 * @param productLevelId
	 * @param productId
	 * @param itemDTO
	 * @throws GeneralException
	 */
	public void updateProductId(Connection conn, int productLevelId, int productId, ItemDTO itemDTO) throws GeneralException{
		PreparedStatement statement = null;
	    ResultSet resultSet = null;
	    int refId = -1;
	    String sql = UPDATE_PRODUCT_ID;	    
	    
	    if(productLevelId == 2){
	    	sql = sql.replaceAll("%REF_TABLE_NAME%", "ITEM_SEGMENT");
	    	refId = itemDTO.getSegmentID();
	    }else if(productLevelId == 3){
	    	sql = sql.replaceAll("%REF_TABLE_NAME%", "SUB_CATEGORY");
	    	refId = itemDTO.getSubCatID();
	    }else if(productLevelId == 4){
	    	sql = sql.replaceAll("%REF_TABLE_NAME%", "CATEGORY");
	    	refId = itemDTO.getCatID();
	    }else if(productLevelId == 5){
	    	sql = sql.replaceAll("%REF_TABLE_NAME%", "DEPARTMENT");
	    	refId = itemDTO.getDeptID();
	    }
	    
	    if( refId > 0 ){
			try{
				statement = conn.prepareStatement(sql);
				statement.setInt(1, productId);
				statement.setInt(2, refId);
		        statement.executeUpdate();
			}
			catch (SQLException e)
			{
				logger.error("Error while executing INSERT_PRODUCT_GROUP");
				throw new GeneralException("Error while executing INSERT_PRODUCT_GROUP", e);
			}finally{
				PristineDBUtil.close(resultSet);
				PristineDBUtil.close(statement);
			}
	    }
	}
	
	/**
	 * Returns Product Id from Product Group Relation table
	 * @param conn					Connection
	 * @param product				Product
	 * @return	Parent Product Id
	 * @throws GeneralException
	 */
	public int getParentProductIdFromProductRelation(Connection conn, ProductDTO product) throws GeneralException{
		int productId = -1;
		PreparedStatement statement = null;
	    ResultSet resultSet = null;
		try{
			statement = conn.prepareStatement(GET_PARENT_PRODUCT_ID_FROM_RELATION);
			statement.setInt(1, product.getProductLevelId());
			statement.setInt(2, product.getChildProductLevelId());
			statement.setInt(3, product.getChildProductId());
	        resultSet = statement.executeQuery();
	        if(resultSet.next()){
	        	productId = resultSet.getInt("PRODUCT_ID");
	        }
		}
		catch (SQLException e)
		{
			logger.error("Error while executing GET_PARENT_PRODUCT_ID_FROM_RELATION");
			throw new GeneralException("Error while executing GET_PARENT_PRODUCT_ID_FROM_RELATION", e);
		}finally{
			PristineDBUtil.close(resultSet);
			PristineDBUtil.close(statement);
		}
		return productId;
	}
	
	/**
	 * Inserts into Product Group Relation table
	 * @param conn			Connection
	 * @param productDTO	Product
	 * @throws GeneralException
	 */
	public void insertProductGroupRelation(Connection conn, ProductDTO productDTO) throws GeneralException{
		PreparedStatement statement = null;
	    ResultSet resultSet = null;
		try{
			statement = conn.prepareStatement(INSERT_PRODUCT_GROUP_RELATION);
			statement.setInt(1, productDTO.getProductId());
			statement.setInt(2, productDTO.getChildProductId());
			statement.setInt(3, productDTO.getChildProductLevelId());
			statement.setInt(4, productDTO.getProductLevelId());
			
			statement.executeUpdate();
		}
		catch (SQLException e)
		{
			logger.error("Error while executing INSERT_PRODUCT_GROUP_RELATION");
			throw new GeneralException("Error while executing INSERT_PRODUCT_GROUP_RELATION", e);
		}finally{
			PristineDBUtil.close(resultSet);
			PristineDBUtil.close(statement);
		}
	}
	
	/**
	 * Inserts into Product Group Relation table for LIR Items
	 * @param conn			Connection
	 * @param lirItemCode	LIR Item Code
	 * @param itemCode		Item Code
	 * @throws GeneralException
	 */
	public void insertProductGroupRelation(Connection conn, int lirItemCode, int parentLevel) {
		PreparedStatement statement = null;
	    ResultSet resultSet = null;
		try{
			String sql = INSERT_PRODUCT_GROUP_RELATION_FOR_LIR_ITEM;
			sql = sql.replaceAll("%PARENT_LEVEL_ID%", String.valueOf(parentLevel));
			sql = sql.replaceAll("%TABLE_NAME%", getTableName(parentLevel));
			sql = sql.replaceAll("%COLUMN_NAME%", getColumnName(parentLevel));
			statement = conn.prepareStatement(sql);
			statement.setInt(1, lirItemCode);
			statement.setInt(2, lirItemCode);
			int count = statement.executeUpdate();
		}
		catch (SQLException e)
		{
			logger.error("Error while executing INSERT_PRODUCT_GROUP_RELATION_FOR_LIR_ITEM " + e);
		}finally{
			PristineDBUtil.close(resultSet);
			PristineDBUtil.close(statement);
		}
	}
	
	/**
	 * Updates Product Id in Product Group Relation table
	 * @param conn			Connection
	 * @param productDTO	Product
	 * @throws GeneralException
	 */
	public void updateProductGroupRelation(Connection conn, ProductDTO productDTO) throws GeneralException{
		PreparedStatement statement = null;
	    ResultSet resultSet = null;
		try{
			statement = conn.prepareStatement(UPDATE_PRODUCT_GROUP_RELATION);
			statement.setInt(1, productDTO.getProductId());
			statement.setInt(2, productDTO.getProductLevelId());
			statement.setInt(3, productDTO.getChildProductLevelId());
			statement.setInt(4, productDTO.getChildProductId());			
	        statement.executeUpdate();
		}
		catch (SQLException e)
		{
			logger.error("Error while executing UPDATE_PRODUCT_GROUP_RELATION");
			throw new GeneralException("Error while executing UPDATE_PRODUCT_GROUP_RELATION", e);
		}finally{
			PristineDBUtil.close(resultSet);
			PristineDBUtil.close(statement);
		}
	}
	
	/**
	 * Updates Product Id in Product Group Relation table
	 * @param conn			Connection
	 * @param productDTO	Product
	 * @throws GeneralException
	 */
	public void updateProductGroupRelation(Connection conn, int lirItemCode, int parentLevel) {
		PreparedStatement statement = null;
	    ResultSet resultSet = null;
		try{
			String sql = UPDATE_PRODUCT_GROUP_RELATION_FOR_LIR_ITEM;
			sql = sql.replaceAll("%PARENT_LEVEL_ID%", String.valueOf(parentLevel));
			sql = sql.replaceAll("%TABLE_NAME%", getTableName(parentLevel));
			sql = sql.replaceAll("%COLUMN_NAME%", getColumnName(parentLevel));
			statement = conn.prepareStatement(sql);
			statement.setInt(1, lirItemCode);
			statement.setInt(2, lirItemCode);
			statement.setInt(3, lirItemCode);
	        int count = statement.executeUpdate();
		}
		catch (SQLException e)
		{
			logger.error("Lir Item Code : " + lirItemCode);
			logger.error("Error while executing UPDATE_PRODUCT_GROUP_RELATION_FOR_LIR_ITEM " + e);
		}finally{
			PristineDBUtil.close(resultSet);
			PristineDBUtil.close(statement);
		}
	}
	
	private String getColumnName(int parentLevel) {
		if(parentLevel == 3)
			return "SUB_CATEGORY_ID";
		else
			return "SEGMENT_ID";
	}

	private String getTableName(int parentLevel) {
		if(parentLevel == 3)
			return "SUB_CATEGORY";
		else
			return "ITEM_SEGMENT";
	}

	public ProductDTO getProductNameAndCode(int productLevelId, ItemDTO itemDTO){
		ProductDTO product = new ProductDTO();
		// TODO: Try and remove hardcoding
		// Needs to be added for other levels as well
		// Add on Type Gas
		// Populate Product Id in Dept, Category and Sub Category tables
		if(productLevelId == 2){
			if(itemDTO.segmentName != null)
				product.setProductName(itemDTO.segmentName.replaceAll("''", "'"));
			else
				product.setProductName(itemDTO.segmentName);
			product.setProductCode(itemDTO.segmentCode);
		}else if(productLevelId == 3){
			if(itemDTO.subCatName != null)
				product.setProductName(itemDTO.subCatName.replaceAll("''", "'"));
			else
				product.setProductName(itemDTO.subCatName);
			product.setProductCode(itemDTO.subCatCode);
		}else if(productLevelId == 4){
			if(itemDTO.catName != null)
				product.setProductName(itemDTO.catName.replaceAll("''", "'"));
			else
				product.setProductName(itemDTO.catName);
			product.setProductCode(itemDTO.catCode);
		}else if(productLevelId == 5){
			product.setProductName(itemDTO.deptName);
			product.setProductCode(itemDTO.deptCode);
		}else if(productLevelId == 6){
			product.setProductName(itemDTO.financeDept);
			product.setProductCode(itemDTO.financeDeptCode);
		}else if(productLevelId == 7){
			product.setProductName(itemDTO.merchDept);
			product.setProductCode(itemDTO.merchDeptCode);
		}else if(productLevelId == 8){
			if(itemDTO.getDeptShortName() != null)
				product.setProductName(itemDTO.getDeptShortName() + "-" + itemDTO.portfolio);
			else
				product.setProductName(itemDTO.portfolio);
			product.setProductCode(itemDTO.portfolioCode);
		}else if(productLevelId == 9){
			product.setProductName(itemDTO.sectorName);
			product.setProductCode(itemDTO.sectorCode);
		}else if(productLevelId == 12){
			product.setProductName(itemDTO.lobName);
			product.setProductCode(itemDTO.lobCode);
		}
		return product;
	}
	
	/**
	 * Returns list of categories
	 * @param conn	Connection
	 * @return
	 * @throws GeneralException
	 */
	public ArrayList<Integer> getCategories(Connection conn) throws GeneralException{
		PreparedStatement stmt = null;
		ResultSet rs = null;
		ArrayList<Integer> productIdList = new ArrayList<Integer>();
		try{
			stmt = conn.prepareStatement(GET_CATEGORIES);
			rs = stmt.executeQuery();
			while(rs.next()){
				productIdList.add(rs.getInt("PRODUCT_ID"));
			}
		}catch(SQLException exception){
			logger.error("Error when executing GET_CATEGORIES - " + exception.getMessage());
			throw new GeneralException("Error when executing GET_CATEGORIES - " + exception.getMessage());
		}finally{
			PristineDBUtil.close(rs);
			PristineDBUtil.close(stmt);
		}
		return productIdList;
	}
	
	/**
	 * Returns list of categories for input product
	 * @param conn
	 * @param productLevelId
	 * @param productId
	 * @return
	 * @throws GeneralException
	 */
	public ArrayList<Integer> getCategories(Connection conn, int productLevelId, int productId) throws GeneralException{
		PreparedStatement stmt = null;
		ResultSet rs = null;
		ArrayList<Integer> productIdList = new ArrayList<Integer>();
		try{
			stmt = conn.prepareStatement(GET_CATEGORIES_FOR_PARENT_PRODUCT);
			stmt.setInt(1, productLevelId);
			stmt.setInt(2, productId);
			rs = stmt.executeQuery();
			while(rs.next()){
				productIdList.add(rs.getInt("PRODUCT_ID"));
			}
		}catch(SQLException exception){
			logger.error("Error when executing GET_CATEGORIES_FOR_PARENT_PRODUCT - " + exception.getMessage());
			throw new GeneralException("Error when executing GET_CATEGORIES_FOR_PARENT_PRODUCT - " + exception.getMessage());
		}finally{
			PristineDBUtil.close(rs);
			PristineDBUtil.close(stmt);
		}
		return productIdList;
	}
	
	/**
	 * Returns a map with product key and list of product list ids that contains the product as value
	 * @param conn
	 * @param productLevelId
	 * @param productId
	 * @return
	 */
	public HashMap<String, ArrayList<Integer>> getProductListForProducts(Connection conn, int productLevelId, int productId){
		PreparedStatement stmt = null;
		ResultSet rs = null;
		HashMap<String, ArrayList<Integer>> productListMap = new HashMap<String, ArrayList<Integer>>();
		try{
			stmt = conn.prepareStatement(GET_PRODUCT_LISTS_FOR_PRODUCTS);
			int counter = 0;
			stmt.setInt(++counter, productLevelId);
			stmt.setInt(++counter, productId);
			stmt.setInt(++counter, productLevelId);
			stmt.setInt(++counter, productId);
			stmt.setInt(++counter, productLevelId);
			stmt.setInt(++counter, productId);
			rs = stmt.executeQuery();
			while(rs.next()){
				String proKey = rs.getInt("CHILD_PRODUCT_LEVEL_ID") + "-" + rs.getInt("CHILD_PRODUCT_ID");
				int productListId = rs.getInt("PRODUCT_ID");
				if(productListMap.get(proKey) != null){
					ArrayList<Integer> tList = productListMap.get(proKey);
					tList.add(productListId);
					productListMap.put(proKey, tList);
				}else{
					ArrayList<Integer> tList = new ArrayList<Integer>();
					tList.add(productListId);
					productListMap.put(proKey, tList);
				}
			}
		}catch(SQLException exception){
			logger.error("Error when executing GET_PRODUCT_LISTS_FOR_PRODUCTS - " + exception.getMessage());
		}finally{
			PristineDBUtil.close(rs);
			PristineDBUtil.close(stmt);
		}
		return productListMap;
	}
	
	/**
	 * Returns parent product level of item
	 */
	public int getParentOfItem(Connection conn) throws GeneralException{
		int parentProductLevelOfItem = -1;
		PreparedStatement statement = null;
	    ResultSet resultSet = null;
		try{
			statement = conn.prepareStatement(GET_PARENT_OF_ITEM);
	        resultSet = statement.executeQuery();
	        if(resultSet.next()){
	        	parentProductLevelOfItem = resultSet.getInt("PRODUCT_LEVEL_ID");
	        }
		}
		catch (SQLException e)
		{
			logger.error("Error while executing GET_PARENT_OF_ITEM");
			throw new GeneralException("Error while executing GET_PARENT_OF_ITEM", e);
		}finally{
			PristineDBUtil.close(resultSet);
			PristineDBUtil.close(statement);
		}
		return parentProductLevelOfItem;
	}
	
	public ProductDTO getProductName(Connection connection, int productLevelId, int productId) throws GeneralException{
		PreparedStatement stmt = null;
		ResultSet rs = null;
		ProductDTO productDTO = null;
		try{
			stmt = connection.prepareStatement(GET_PRODUCT_NAME);
			stmt.setInt(1, productLevelId);
			stmt.setInt(2, productId);
			rs = stmt.executeQuery();
			if(rs.next()){
				productDTO = new ProductDTO();
				productDTO.setProductName(rs.getString("NAME"));
			}
		}catch(SQLException exception){
			logger.error("Error when executing GET_PRODUCT_NAME - " + exception);
			throw new GeneralException("Error when executing GET_PRODUCT_NAME - " + exception);
		}
		return productDTO;
	}
	
	/**
	 * To insert into product group table using unused sequence number in product group table
	 * @param conn
	 * @param product
	 * @throws GeneralException
	 */
	private void insertProductGroup(Connection conn, ProductDTO product, int productId) throws GeneralException{
		PreparedStatement statement = null;
	    ResultSet resultSet = null;
//	    String productId = getUnusedSeqFromProductGroup(conn);
		try{
			statement = conn.prepareStatement(INSERT_PRODUCT_GROUP_USING_UNUSED_SEQ);
			statement.setInt(1, productId);
			statement.setInt(2, product.getProductLevelId());
			statement.setString(3, product.getProductName());
			statement.setString(4, product.getProductCode());
			int count =  statement.executeUpdate();
		}
		catch (SQLException e)
		{
			PristineDBUtil.rollbackTransaction(conn, "insertProductGroup()");
			logger.error("Error while executing insertProductGroup", e);
			throw new GeneralException("Error while executing insertProductGroup", e);
		}finally{
			PristineDBUtil.close(resultSet);
			PristineDBUtil.close(statement);
		}
	}
	
	/**
	 * To get sequence number which is not used in product group table
	 * @param conn
	 * @return
	 * @throws GeneralException
	 */
	private int getUnusedSeqFromProductGroup(Connection conn) throws GeneralException{
		PreparedStatement statement = null;
	    ResultSet resultSet = null;
	    int productId = 0;
		try{
			statement = conn.prepareStatement(GET_UNUSED_SEQ_FROM_PRODUCT_GROUP);
			statement.setInt(1, Constants.MAX_PRODUCT_ID_SEQUENCE);
			statement.setInt(2, Constants.MAX_PRODUCT_ID_SEQUENCE);
	        resultSet = statement.executeQuery();
	        if(resultSet.next()){
	        	productId = resultSet.getInt("MISSING_PRODUCT_ID");
	        }
		}
		catch (SQLException e)
		{
			logger.error("Error while executing GET_UNUSED_SEQ_FROM_PRODUCT_GROUP", e);
//			throw new GeneralException("Error while executing GET_UNUSED_SEQ_FROM_PRODUCT_GROUP", e);
		}finally{
			PristineDBUtil.close(resultSet);
			PristineDBUtil.close(statement);
		}
		return productId;
	}
	
	
	/**
	 * To get new sequence number
	 * @param conn
	 * @return
	 * @throws GeneralException
	 */
	private int getProductIdNextVal(Connection conn) throws GeneralException{
		PreparedStatement statement = null;
	    ResultSet resultSet = null;
	    int productId = 0;
		try{
			statement = conn.prepareStatement(GET_PRODUCT_GROUP_NEXT_VAL);
	        resultSet = statement.executeQuery();
	        if(resultSet.next()){
	        	productId = resultSet.getInt("NEXTVAL");
	        }
		}
		catch (SQLException e)
		{
			logger.error("Error while executing getProductIdNextVal", e);
			throw new GeneralException("Error while executing getProductIdNextVal", e);
		}finally{
			PristineDBUtil.close(resultSet);
			PristineDBUtil.close(statement);
		}
		return productId;
	}
	
	/**
	 * To get Product Id's which don't have any child products
	 * @param conn
	 * @param levelId
	 * @param productGroupType
	 * @throws GeneralException
	 */
	public void populateProdGroupActiveInd(Connection conn) throws GeneralException{
		ProductService productService = new ProductService();
		int chainId = Integer.parseInt(PropertyManager.getProperty("PRESTO_SUBSCRIBER"));
		if(productService.productHierarchy.size() == 0){
			//Added by RB
			if(chainId == 53)
			{
				productService.getChildHierarchy(conn, Constants.LOBLEVELID);
			}
			else
			{
				productService.getChildHierarchy(conn, Constants.DEPARTMENTLEVELID);
			}
			
		}
		ItemDAO itemDAO = new ItemDAO();
		int productHierarchyCount = productService.productHierarchy.size();
		//Based on Product hierarchy count the delete different product levels
		logger.info("Updating Product Group active indicator");
		
		//Update all the Product id's Active indicator as N
		updateProductGroupActiveInd(conn);
		int productLevelId = 2;
		for(int i=1; i < productHierarchyCount; i++){
			//Added by RB
			String sql = "";
			if(chainId == 53)
			{
				sql = itemDAO.getQueryBasedOnActiveProductId(conn, Constants.LOBLEVELID, productService, i);
				if(i == 4)
				{
					productLevelId = 12;
				}
				else
				{
					if(productHierarchyCount == 5 && productLevelId == 2){
						productLevelId++;
					}
				}
			}
			else
			{
				sql = itemDAO.getQueryBasedOnActiveProductId(conn, Constants.DEPARTMENTLEVELID, productService, i);
				if(productHierarchyCount == 4 && productLevelId == 2){
					productLevelId++;
				}
			}
			//To change the Product level id if the Item segment were not available.
			
			String query = new String(sql);
			logger.debug("update active Product id in Product group table: "+query);
			updateProductIdActiveInd(conn,productLevelId,query);
			productLevelId++;
		}
		logger.info("Product group active indicators were updated");
	}
	
	private void updateProductIdActiveInd(Connection conn,int productLevelId, String sql) throws GeneralException{
	    PreparedStatement stmt = null;
		
		try{
			stmt = conn.prepareStatement(sql);
			int counter = 0;
			stmt.setInt(++counter, productLevelId);
			stmt.executeUpdate();
		}catch(SQLException exception){
			logger.error("Error while executing updateProductIdActiveInd" + exception);
			throw new GeneralException("Error while executing updateProductIdActiveInd", exception);
		}finally{
			PristineDBUtil.close(stmt);
		}
	}
	
	/**
	 * To update product group Active indicator default value as N for all the Product Id's
	 * @param conn
	 * @throws GeneralException
	 */
	private void updateProductGroupActiveInd(Connection conn) throws GeneralException{
	    PreparedStatement stmt = null;
		
		try{
			stmt = conn.prepareStatement(UPDATE_PRODUCT_GROUP_ACTIVE_IND);
			stmt.executeUpdate();
		}catch(SQLException exception){
			logger.error("Error while executing updateProductGroupActiveInd" + exception);
			throw new GeneralException("Error while executing updateProductGroupActiveInd", exception);
		}finally{
			PristineDBUtil.close(stmt);
		}
	}
	
	public HashMap<ProductKey, ProductDTO> getDepartments(Connection conn) throws GeneralException {
		PreparedStatement stmt = null;
		ResultSet rs = null;
		HashMap<ProductKey, ProductDTO> productMap = new HashMap<ProductKey, ProductDTO>();
		try {
			stmt = conn.prepareStatement(GET_DEPARTMENTS);
			rs = stmt.executeQuery();
			while (rs.next()) {
				ProductDTO productDTO = new ProductDTO();

				productDTO.setProductId(rs.getInt("PRODUCT_ID"));
				productDTO.setProductName(rs.getString("NAME"));

				ProductKey productKey = new ProductKey(Constants.DEPARTMENTLEVELID, productDTO.getProductId());

				if (productMap.get(productKey) == null) {
					productMap.put(productKey, productDTO);
				}
			}
		} catch (SQLException exception) {
			logger.error("Error in getDepartments() - " + exception.getMessage());
			throw new GeneralException("Error in getDepartments() - " + exception.getMessage());
		} finally {
			PristineDBUtil.close(rs);
			PristineDBUtil.close(stmt);
		}
		return productMap;
	}
	
	public HashMap<ProductKey, ProductDTO> getAllCategories(Connection conn) throws GeneralException {
		PreparedStatement stmt = null;
		ResultSet rs = null;
		HashMap<ProductKey, ProductDTO> productMap = new HashMap<ProductKey, ProductDTO>();
		try {
			stmt = conn.prepareStatement(GET_ALL_CATEGORIES);
			rs = stmt.executeQuery();
			while (rs.next()) {
				ProductDTO productDTO = new ProductDTO();

				productDTO.setProductId(rs.getInt("PRODUCT_ID"));
				productDTO.setProductName(rs.getString("NAME"));

				ProductKey productKey = new ProductKey(Constants.CATEGORYLEVELID, productDTO.getProductId());

				if (productMap.get(productKey) == null) {
					productMap.put(productKey, productDTO);
				}
			}
		} catch (SQLException exception) {
			logger.error("Error in getAllCategories() - " + exception.getMessage());
			throw new GeneralException("Error in getAllCategories() - " + exception.getMessage());
		} finally {
			PristineDBUtil.close(rs);
			PristineDBUtil.close(stmt);
		}
		return productMap;
	}
}
