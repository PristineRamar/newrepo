package com.pristine.dao;

import java.sql.Connection;

import org.apache.log4j.Logger;

import com.pristine.exception.GeneralException;
import com.pristine.util.Constants;
import com.pristine.util.PristineDBUtil;

public class CheckListDAO implements IDAO {
	
	private static Logger	logger	= Logger.getLogger("CheckListDAO");
	
	public int getCheckListId(Connection conn, String checkListName) throws GeneralException {
		StringBuffer sb;
		sb = new StringBuffer();	
		sb.append(" SELECT ID FROM PRICE_CHECK_LIST WHERE UPPER(NAME) = ");
		sb.append("'" + checkListName.toUpperCase()+  "'");	
		String idStr = PristineDBUtil.getSingleColumnVal(conn, sb, "getCheckListId");
		int retVal = -1;
		if( idStr != null){
			retVal = Integer.parseInt(idStr);
		}
		return retVal;
	}

	public int deleteCheckListId(Connection conn, int listId) throws GeneralException {
		StringBuffer sb;
		sb = new StringBuffer();	
		sb.append(" DELETE FROM PRICE_CHECK_LIST_ITEMS WHERE PRICE_CHECK_LIST_ID = ");
		sb.append(listId);	
		return PristineDBUtil.executeUpdate(conn, sb, "CHECKLIST - DELETE");
	}
	
	public boolean insertCheckListItem( Connection conn, int listId, int itemCode, String upc) throws GeneralException {
		
		boolean insertFlag = false;
		StringBuffer sb;
		if( itemCode < 0) {
			//get item Code
			sb = new StringBuffer();	
			sb.append(" SELECT ITEM_CODE FROM ITEM_LOOKUP WHERE UPC = ");
			sb.append("'" + upc+  "'");	
			//logger.debug(sb.toString());
			String idStr = PristineDBUtil.getSingleColumnVal(conn, sb, "getItemCode");
			if( idStr != null){
				itemCode = Integer.parseInt(idStr);
			}
			//logger.debug("Item code for UPC " + upc + " is " +  itemCode);
		}
		if( itemCode > 0){
			sb = new StringBuffer();	
			sb.append(" INSERT INTO PRICE_CHECK_LIST_ITEMS ( PRICE_CHECK_LIST_ID, ITEM_CODE)");
			sb.append(" VALUES ( ");
			sb.append(listId).append(",");
			sb.append(itemCode).append(")");
			PristineDBUtil.execute(conn, sb, "CheckList - insertItem");
			insertFlag = true;
		}
		return insertFlag;
	}
	
	public void createCheckList(Connection conn, String checkListName) throws GeneralException {
		StringBuffer sb;
		sb = new StringBuffer();	
		sb.append(" INSERT INTO PRICE_CHECK_LIST ( ID, NAME, LIST_DESC, CREATE_DATETIME,");
		sb.append(" CREATE_USER_ID, UPDATE_DATETIME, ACTIVE_INDICATOR, DIRECTED_CHECK, " );
		sb.append(" EXPECTED_HRS, EXPECTED_MIN, DRIVE_HRS, DRIVE_MIN ) VALUES (" );
		sb.append(" PRICECHECK_LIST_SEQ.NEXTVAL, ");
		sb.append("'").append(checkListName).append("',");
		sb.append("'").append(checkListName).append("',");
		sb.append( "sysdate, ");
		sb.append("'").append( Constants.PRESTO_SYS_USER).append("',");
		sb.append( "sysdate, ");
		sb.append( "'Y','Y',1,0,1,0) ");
		PristineDBUtil.execute(conn, sb, "CheckList - insertCheckList");
		return;
	}

	public void setupTopCheckList(Connection conn, int checkListId, int deptId, String itemRank) throws GeneralException {
		StringBuffer sb = new StringBuffer();	
		
		
		sb.append( "insert into PRICE_CHECK_LIST_ITEMS ( PRICE_CHECK_LIST_ID, ITEM_CODE)"); 
		sb.append( " select ").append(checkListId).append(", item_code from item_lookup where " ); 
		sb.append( " dept_id in (").append( deptId ).append(") and ");
		sb.append( " standard_upc in (");
		
		//Insert LIG items
		StringBuffer sb2 = new StringBuffer();
		sb2.append(sb);
		sb2.append( " select  min (STANDARD_UPC) from item_lookup where item_desc in (");
		sb2.append( itemRank).append(")");
		sb2.append( " and ret_lir_id is not null"); 
		sb2.append( " group by ret_lir_id)");
		PristineDBUtil.execute(conn, sb2, "CheckList creation - ShopRite");
		
		//Insert Non LIG items
		sb2 = new StringBuffer();
		sb2.append(sb);
		sb2.append( " select STANDARD_UPC from item_lookup where item_desc in (");
		sb2.append( itemRank).append(")");
		sb2.append( " and ret_lir_id is null )"); 
		PristineDBUtil.execute(conn, sb2, "CheckList creation - ShopRite");
		
	}
}
