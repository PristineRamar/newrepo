package com.pristine.dao;

import java.sql.Connection;
import java.sql.SQLException;

import javax.sql.rowset.CachedRowSet;

import org.apache.log4j.Logger;

import com.pristine.dto.VariationBaselineDTO;
import com.pristine.exception.GeneralException;
import com.pristine.util.Constants;
import com.pristine.util.PrestoUtil;
import com.pristine.util.PristineDBUtil;

public class VariationBaselineDAO implements IDAO{
	static private Logger logger = Logger.getLogger("com.pristine.dao.VariationBaselineDAO");
	
	public void insert(Connection conn, VariationBaselineDTO baselineObj) throws GeneralException{
		///Get the chain id
		if( baselineObj.getChainID() == 0 || baselineObj.getChainID() == Constants.NULLID ){
			StoreDAO strdao = new StoreDAO();
			int chainID = strdao.getChainID(conn, baselineObj.getStoreID());
			baselineObj.setChainID(chainID);
		}
		//Delete anything for the last 6 days
		
		// as long as it is not used in VARIATION_RESULTS table - to be done.
		StringBuffer sb = new StringBuffer();
		sb.append("delete from VARIATION_BASELINE where COMP_CHAIN_ID = ");
		sb.append(baselineObj.getChainID());
		sb.append(" AND COMP_STR_ID ");
		if(  baselineObj.getStoreID() > 0)
			sb.append(" = ").append(baselineObj.getStoreID());
		else
			sb.append(" IS NULL ");
		sb.append(" AND DEPT_ID ");
		if(  baselineObj.getDeptID() > 0)
			sb.append(" = ").append(baselineObj.getDeptID());
		else
			sb.append(" IS NULL ");
		//sb.append(" AND AS_OF_DATE >= SYSDATE - 6 ");
		
		logger.debug(sb);
		
		//Note - the above delete could give Referential integrity constraint violation which can
		// be ignored.
		try{
			PristineDBUtil.execute(conn, sb, "Variation Baseline DAO - delete baseline record");
			
			
		}catch (GeneralException e){
			
		}
		//Insert into VariationBaseline
		
		sb = new StringBuffer();
		sb.append( "INSERT INTO VARIATION_BASELINE( BASELINE_ID, COMP_CHAIN_ID, COMP_STR_ID, DEPT_ID, AS_OF_DATE, ");
		sb.append( " AIV, SALE_PCT, REG_NET_PCT, DEPTH_PCT, AIV_LOWER_LIMIT, AIV_UPPER_LIMIT, ");
		sb.append( " SALE_PCT_LOWER_LIMIT, SALE_PCT_UPPER_LIMIT, DEPTH_PCT_LOWER_LIMIT, DEPTH_PCT_UPPER_LIMIT, ");
		sb.append( " REG_NET_PCT_LOWER_LIMIT, REG_NET_PCT_UPPER_LIMIT,  ");
		sb.append( " REG_UP_PCT, REG_UP_PCT_LOWER_LIMIT, REG_UP_PCT_UPPER_LIMIT, ");
		sb.append( " REG_DOWN_PCT, REG_DOWN_PCT_LOWER_LIMIT,  REG_DOWN_PCT_UPPER_LIMIT, " ); 
		sb.append( " NO_OF_ITEMS, NO_OF_STORES) VALUES ( " ); 
		sb.append( " VARIATION_BASELINE_SEQ.NEXTVAL").append(',');
		sb.append( baselineObj.getChainID()).append(',');
		if(  baselineObj.getStoreID() > 0)
			sb.append( baselineObj.getStoreID()).append(',');
		else
			sb.append( " NULL ").append(',');
		if(  baselineObj.getDeptID() > 0)
			sb.append( baselineObj.getDeptID()).append(',');
		else
			sb.append( " NULL ").append(',');

		sb.append( " SYSDATE ").append(',');
		sb.append( PrestoUtil.round( baselineObj.getAiv(), 2)).append(',');
		sb.append( PrestoUtil.round( baselineObj.getSale(), 2)).append(',');
		sb.append( PrestoUtil.round( baselineObj.getNetReg(), 2)).append(',');
		sb.append( PrestoUtil.round( baselineObj.getDepth(), 2)).append(',');
		sb.append( PrestoUtil.round( baselineObj.getAivLowerLimit(), 2)).append(',');
		sb.append( PrestoUtil.round( baselineObj.getAivUpperLimit(), 2)).append(',');
		sb.append( PrestoUtil.round( baselineObj.getSaleLowerLimit(), 2)).append(',');
		sb.append( PrestoUtil.round( baselineObj.getSaleUpperLimit(), 2)).append(',');
		sb.append( PrestoUtil.round( baselineObj.getDepthLowerLimit(), 2)).append(',');
		sb.append( PrestoUtil.round( baselineObj.getDepthUpperLimit(), 2)).append(',');
		sb.append( PrestoUtil.round( baselineObj.getNetRegLowerLimit(), 2)).append(',');
		sb.append( PrestoUtil.round( baselineObj.getNetRegUpperLimit(), 2)).append(',');
		sb.append( PrestoUtil.round( baselineObj.getRegUp(), 2)).append(',');
		sb.append( PrestoUtil.round( baselineObj.getRegUpLowerLimit(), 2)).append(',');
		sb.append( PrestoUtil.round( baselineObj.getRegUpUpperLimit(), 2)).append(',');
		sb.append( PrestoUtil.round( baselineObj.getRegDown(), 2)).append(',');
		sb.append( PrestoUtil.round( baselineObj.getRegDownLowerLimit(), 2)).append(',');
		sb.append( PrestoUtil.round( baselineObj.getRegDownUpperLimit(), 2)).append(',');
		sb.append(baselineObj.getNumOfItems()).append(',');
		sb.append(baselineObj.getNumOfStores()).append(')');
		
		logger.debug(sb);
		PristineDBUtil.execute(conn, sb, "Variation Baseline DAO - insert baseline record");
	}
	
	public VariationBaselineDTO getBaselineData(Connection conn, int chainID, int storeID, int deptID) throws GeneralException{
		VariationBaselineDTO baselineVO = null;
		if ( chainID == Constants.NULLID )
			throw new GeneralException("Chain ID Not set");
		StringBuffer sb = new StringBuffer();
		sb.append( "SELECT BASELINE_ID, COMP_CHAIN_ID, NVL(COMP_STR_ID, -1), NVL(DEPT_ID, -1), TO_CHAR (AS_OF_DATE, 'MM/DD/YYYY') AS_OF_DATE, ");
		sb.append( " AIV, SALE_PCT, REG_NET_PCT, DEPTH_PCT, REG_UP_PCT, REG_DOWN_PCT, ");
		sb.append( " AIV_LOWER_LIMIT, AIV_UPPER_LIMIT, ");
		sb.append( " SALE_PCT_LOWER_LIMIT, SALE_PCT_UPPER_LIMIT, DEPTH_PCT_LOWER_LIMIT, DEPTH_PCT_UPPER_LIMIT, ");
		sb.append( " REG_NET_PCT_LOWER_LIMIT, REG_NET_PCT_UPPER_LIMIT, " );
		sb.append( " REG_UP_PCT_UPPER_LIMIT, REG_UP_PCT_LOWER_LIMIT, ");
		sb.append( " REG_DOWN_PCT_UPPER_LIMIT, REG_DOWN_PCT_LOWER_LIMIT, " );
		sb.append( " NO_OF_ITEMS, NO_OF_STORES " ); 
		sb.append( " FROM VARIATION_BASELINE " );
		sb.append( " WHERE COMP_CHAIN_ID = " );
		sb.append( chainID ).append(" AND COMP_STR_ID ");
		if ( storeID == Constants.NULLID )
			sb.append(" IS NULL AND DEPT_ID ");
		else
			sb.append(" = ").append(storeID).append(" AND DEPT_ID ");
		
		if ( deptID == Constants.NULLID )
			sb.append(" IS NULL ");
		else
			sb.append(" = ").append(deptID);
		
		sb.append( " AND AS_OF_DATE = ( Select MAX (AS_OF_DATE) from VARIATION_BASELINE " );
		sb.append( " WHERE COMP_CHAIN_ID = " );
		sb.append( chainID ).append(" AND COMP_STR_ID ");
		if ( storeID == Constants.NULLID )
			sb.append(" IS NULL AND DEPT_ID ");
		else
			sb.append(" = ").append(storeID).append(" AND DEPT_ID ");
		
		if ( deptID == Constants.NULLID )
			sb.append(" IS NULL ");
		else
			sb.append(" = ").append(deptID);
		sb.append(" ) ");
		//sb.append(" AND USE_FLAG ='Y' ");
		logger.debug(sb);
		CachedRowSet crs = PristineDBUtil.executeQuery(conn, sb, "getBaselineData");
		try{
			if(crs.next()){
				baselineVO = new VariationBaselineDTO();
				baselineVO.setBaselineID(crs.getInt("BASELINE_ID"));
				baselineVO.setChainID(chainID);
				baselineVO.setStoreID(storeID);
				baselineVO.setDeptID(deptID);
				baselineVO.setAsOfDate(crs.getString("AS_OF_DATE"));
				baselineVO.setAiv(crs.getFloat("AIV"));
				baselineVO.setAivLowerLimit(crs.getFloat("AIV_LOWER_LIMIT"));
				baselineVO.setAivUpperLimit(crs.getFloat("AIV_UPPER_LIMIT"));
				baselineVO.setDepth(crs.getFloat("DEPTH_PCT"));
				baselineVO.setDepthLowerLimit(crs.getFloat("DEPTH_PCT_LOWER_LIMIT"));
				baselineVO.setDepthUpperLimit(crs.getFloat("DEPTH_PCT_UPPER_LIMIT"));
				baselineVO.setNetReg(crs.getFloat("REG_NET_PCT"));
				baselineVO.setNetRegLowerLimit(crs.getFloat("REG_NET_PCT_LOWER_LIMIT"));
				baselineVO.setNetRegUpperLimit(crs.getFloat("REG_NET_PCT_UPPER_LIMIT"));
				
				baselineVO.setRegUp(crs.getFloat("REG_UP_PCT"));
				baselineVO.setRegUpLowerLimit(crs.getFloat("REG_UP_PCT_LOWER_LIMIT"));
				baselineVO.setRegUpUpperLimit(crs.getFloat("REG_UP_PCT_UPPER_LIMIT"));
				
				baselineVO.setRegDown(crs.getFloat("REG_DOWN_PCT"));
				baselineVO.setRegDownLowerLimit(crs.getFloat("REG_DOWN_PCT_LOWER_LIMIT"));
				baselineVO.setRegDownUpperLimit(crs.getFloat("REG_DOWN_PCT_UPPER_LIMIT"));
				
				
				baselineVO.setSale(crs.getFloat("SALE_PCT"));
				baselineVO.setSaleLowerLimit(crs.getFloat("SALE_PCT_LOWER_LIMIT"));
				baselineVO.setSaleUpperLimit(crs.getFloat("SALE_PCT_UPPER_LIMIT"));
				baselineVO.setNumOfStores(crs.getInt("NO_OF_STORES"));
				baselineVO.setNumOfItems(crs.getInt("NO_OF_ITEMS"));
				
			}
		}catch (SQLException sqle){
			throw new GeneralException(" VariationBaselineDAO - getBaselineData ", sqle);
		}
		return baselineVO;
	}
		

	public void resetUseFlag(Connection conn) throws GeneralException{
		StringBuffer sb = new StringBuffer();
		sb.append( "Update variation_baseline set USE_FLAG = 'N' where USE_FLAG = 'Y'"); 
		PristineDBUtil.execute(conn, sb, "Variation Baseline DAO - update baseline record use flag");
	}
	
	public void cleanupBaseItemList(Connection conn, int chainId) throws GeneralException{
		StringBuffer sb = new StringBuffer();
		sb.append( "DELETE FROM VARIATION_BASE_ITEM_LIST WHERE COMP_CHAIN_ID = ");
		sb.append( chainId);
		//sb.append( " AND KEYWORD <> '").append(keyword.toUpperCase()).append("'");
		PristineDBUtil.execute(conn, sb, "Variation Baseline DAO - cleanup variation baseline items");
	}
	
	public void addToBaseItemList(Connection conn, int chainId, int  itemCode, boolean presentInAllStores) throws
					GeneralException {
		StringBuffer sb = new StringBuffer();
		sb.append( "UPDATE VARIATION_BASE_ITEM_LIST SET");
		sb.append( "  COUNT = COUNT + 1  ");
		if( !presentInAllStores)
			sb.append( "  , PRESENT_IN_ALL_STRS_IND = 'N' ");
		sb.append( " WHERE COMP_CHAIN_ID = ").append(chainId);
		sb.append( " AND ITEM_CODE = ").append(itemCode);
		
		int recCount = PristineDBUtil.executeUpdate(conn, sb, "Variation Baseline DAO - update variation baseline items");
		if( recCount == 0){
			sb = new StringBuffer();
			sb.append( "INSERT INTO VARIATION_BASE_ITEM_LIST (");
			sb.append( "COMP_CHAIN_ID, ITEM_CODE, PRESENT_IN_ALL_STRS_IND, COUNT) VALUES (");
			sb.append(chainId).append(", ");
			sb.append(itemCode).append(", ");
			if( presentInAllStores)
				sb.append("'Y', ");
			else
				sb.append("'N', ");
			sb.append("1 )");
			PristineDBUtil.execute(conn, sb, "Variation Baseline DAO - insert variation baseline items");
		}
		return;
	}
	
	
	public CachedRowSet getVariationAnalysisStores(Connection conn, int chainId, String startDate, String endDate) throws GeneralException
	{
		StringBuffer sb = new StringBuffer();
		sb.append("select DISTINCT STR_ID FROM VARIATION_RESULTS WHERE ");
		sb.append(" CHAIN_ID = ").append(chainId);
		sb.append(" AND STR_ID IS NOT NULL ");
		sb.append(" AND WEEK_START_DATE >= TO_DATE('").append(startDate).append("','MM/DD/YY') ");
		sb.append(" AND WEEK_END_DATE <= TO_DATE('").append(endDate).append("','MM/DD/YY')");
		CachedRowSet result = PristineDBUtil.executeQuery(conn, sb, "PerformanceGoal-GetActiveChainDetails");
		logger.debug(sb.toString());
		return result;

	}
	
}
