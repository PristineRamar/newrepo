package com.pristine.dao;

import java.sql.Connection;

import javax.sql.rowset.CachedRowSet;

import org.apache.log4j.Logger;

import com.pristine.exception.GeneralException;
import com.pristine.util.PrestoUtil;
import com.pristine.util.PristineDBUtil;

public class CheckOptimizationDAO implements IDAO {
	private static Logger	logger	= Logger.getLogger("CheckOptimizationDAO");

	public void clearRepresentedStoreList(Connection conn, int checkStoreId, String weekStartDate) 
		throws GeneralException {
		
		StringBuffer sb = new StringBuffer();
		sb.append("DELETE FROM COMP_STR_REPRESENTATION WHERE CHECK_STR_ID = ");
		sb.append(checkStoreId);
		sb.append(" AND WEEK_START_DATE  = TO_DATE('").append(weekStartDate).append("', 'MM/DD/YYYY')");
		//logger.debug(sb.toString());
		PristineDBUtil.execute(conn, sb, "clearRepresentedStoreList");
	}
	
	public void insertRepresentedStore( Connection conn, int checkStoreId,  int representedStoreId, 
			int itemCode, String weekStartDate, int basecheckId, int representedCheckId ) throws GeneralException {
		StringBuffer sb = new StringBuffer();
		sb.append("INSERT INTO COMP_STR_REPRESENTATION ( CHECK_STR_ID, REP_STR_ID, ITEM_CODE, ");
		sb.append(" WEEK_START_DATE, CHECK_STR_CHECK_DATA_ID, REP_STR_CHECK_DATA_ID ) VALUES ( ");
		sb.append(checkStoreId).append(", ");
		sb.append(representedStoreId).append(", ");
		sb.append(itemCode).append(", ");
		sb.append("TO_DATE('").append(weekStartDate).append("', 'MM/DD/YYYY'),");
		sb.append(basecheckId).append(", ");
		sb.append(representedCheckId).append(") ");
		//logger.debug(sb.toString());
		PristineDBUtil.execute(conn, sb, "checkOptimizationDAO - insertRepresentedStore");
	}
	
	public void clearStoreRelationship(Connection conn, int baseStoreId, String weekStartDate) 
		throws GeneralException {
	
		StringBuffer sb = new StringBuffer();
		sb.append("DELETE FROM COMP_STR_RELATIONSHIP WHERE BASE_STR_ID = ");
		sb.append(baseStoreId);
		sb.append(" AND WEEK_START_DATE  = TO_DATE('").append(weekStartDate).append("', 'MM/DD/YYYY')");
		//logger.debug(sb.toString());
		PristineDBUtil.execute(conn, sb, "clearRelatedStoreList");
	}
	
	public void insertRelatedStore( Connection conn, int baseStoreId,  int relatedStoreId, 
			int itemCode, String weekStartDate, int basecheckId, int relatedCheckId, float relationship ) throws GeneralException {
		StringBuffer sb = new StringBuffer();
		sb.append("INSERT INTO COMP_STR_RELATIONSHIP ( BASE_STR_ID, RELATED_STR_ID, ITEM_CODE, ");
		sb.append(" WEEK_START_DATE, BASE_STR_CHECK_DATA_ID , RELATED_STR_CHECK_DATA_ID, RELATION_FACTOR ) VALUES ( ");
		sb.append(baseStoreId).append(", ");
		sb.append(relatedStoreId).append(", ");
		sb.append(itemCode).append(", ");
		sb.append("TO_DATE('").append(weekStartDate).append("', 'MM/DD/YYYY'),");
		sb.append(basecheckId).append(", ");
		sb.append(relatedCheckId).append(", ");
		String relationshipFactor = Float.toString(PrestoUtil.round(relationship, 4));
		sb.append(relationshipFactor).append(") ");
		//logger.debug(sb.toString());
		PristineDBUtil.execute(conn, sb, "checkOptimizationDAO - insertRepresentedStore");
	}
	
	public CachedRowSet getStoreRelationShipForItem( Connection conn, 
			int baseStrId, int relatedStrId, int itemCode) throws GeneralException {
		
		CachedRowSet crs = null;
		StringBuffer sb = new StringBuffer();
		//Fill in the query
		sb.append("SELECT BASE_STR_ID, RELATED_STR_ID, ITEM_CODE, RELATION_FACTOR FROM COMP_STR_RELATIONSHIP ");
		sb.append(" WHERE (( BASE_STR_ID = ").append(baseStrId);
		sb.append(" AND RELATED_STR_ID = ").append(relatedStrId).append(")");
		sb.append(" OR ( BASE_STR_ID = ").append(relatedStrId);
		sb.append(" AND RELATED_STR_ID = ").append(baseStrId).append("))");
		sb.append(" AND ITEM_CODE = ").append(itemCode);
		
		//logger.debug(sb);
		//execute the statement
		crs = PristineDBUtil.executeQuery(conn, sb, "CheckOptimizationDao - getStoreRelationShipForItem");
		return crs;
		
	}
	
	
	public void clearInStoreItemRelationship(Connection conn, int storeId, String weekStartDate) 
	throws GeneralException {

		StringBuffer sb = new StringBuffer();
		sb.append("DELETE FROM INSTR_ITEM_RELATIONSHIP WHERE STR_ID = ");
		sb.append(storeId);
		sb.append(" AND WEEK_START_DATE  = TO_DATE('").append(weekStartDate).append("', 'MM/DD/YYYY')");
		//logger.debug(sb.toString());
		PristineDBUtil.execute(conn, sb, "clearRelatedStoreList");
	}

	public void insertInStoreItemRelationship( Connection conn, int storeId,  
			int baseItemCode, int relatedItemCode, String weekStartDate, int basecheckId, int relatedCheckId, float relationship ) throws GeneralException {
		StringBuffer sb = new StringBuffer();
		sb.append("INSERT INTO INSTR_ITEM_RELATIONSHIP ( STR_ID, BASE_ITEM_CODE, RELATED_ITEM_CODE,  ");
		sb.append(" WEEK_START_DATE, BASE_ITEM_CHECK_DATA_ID , RELATED_ITEM_CHECK_DATA_ID, RELATION_FACTOR ) VALUES ( ");
		sb.append(storeId).append(", ");
		sb.append(baseItemCode).append(", ");
		sb.append(relatedItemCode).append(", ");
		sb.append("TO_DATE('").append(weekStartDate).append("', 'MM/DD/YYYY'),");
		sb.append(basecheckId).append(", ");
		sb.append(relatedCheckId).append(", ");
		String relationshipFactor = Float.toString(PrestoUtil.round(relationship, 4));
		sb.append(relationshipFactor).append(") ");
		logger.debug(sb.toString());
		PristineDBUtil.execute(conn, sb, "checkOptimizationDAO - insertRepresentedStore");
	}
	
	public CachedRowSet getInStoreRelationshipForItem( Connection conn, 
			int storeId, int baseItemCode, int relatedItemCode) throws GeneralException {
		
		CachedRowSet crs = null;
		StringBuffer sb = new StringBuffer();
		//Fill in the query
		sb.append("SELECT STR_ID, BASE_ITEM_CODE, RELATED_ITEM_CODE, RELATION_FACTOR FROM INSTR_ITEM_RELATIONSHIP ");
		sb.append(" WHERE (( BASE_ITEM_CODE = ").append(baseItemCode);
		sb.append(" AND RELATED_ITEM_CODE = ").append(relatedItemCode).append(")");
		sb.append(" OR ( BASE_ITEM_CODE = ").append(relatedItemCode);
		sb.append(" AND RELATED_ITEM_CODE = ").append(baseItemCode).append("))");
		sb.append(" AND STR_ID = ").append(storeId);
		
		//logger.debug(sb);
		//execute the statement
		crs = PristineDBUtil.executeQuery(conn, sb, "CheckOptimizationDao - getStoreRelationShipForItem");
		return crs;
		
	}
	
	public CachedRowSet getInStoreRelatedItems( Connection conn, 
			int storeId, int baseItemCode) throws GeneralException {
		
		CachedRowSet crs = null;
		StringBuffer sb = new StringBuffer();
		
		sb.append( " SELECT DISTINCT RELATED_ITEM  FROM (" );
		sb.append(" SELECT RELATED_ITEM_CODE RELATED_ITEM FROM INSTR_ITEM_RELATIONSHIP WHERE " );
		sb.append(" STR_ID = ").append(storeId);
		sb.append(" AND base_item_code = ").append(baseItemCode);
		sb.append(" UNION " );
		//Fill in the query
		sb.append(" SELECT BASE_ITEM_CODE RELATED_ITEM FROM INSTR_ITEM_RELATIONSHIP where ");
		sb.append(" STR_ID = ").append(storeId);
		sb.append(" AND RELATED_ITEM_CODE =  ").append(baseItemCode).append(")");
		
		//logger.debug(sb);
		//execute the statement
		crs = PristineDBUtil.executeQuery(conn, sb, "CheckOptimizationDao - getStoreRelationShipForItem");
		return crs;
		
	}

}
