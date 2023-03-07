package com.pristine.dao;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.text.SimpleDateFormat;
import java.util.Date;

import javax.sql.rowset.CachedRowSet;

import org.apache.log4j.Logger;

import com.pristine.exception.GeneralException;
import com.pristine.util.PristineDBUtil;

public class ItemcodeDAO {
	
	static Logger logger = Logger.getLogger("ItemcodeDAO");

	
	public CachedRowSet GetItemcodeList(Connection _Conn,	String storeNumber, Date processfrom, Date processTo) throws GeneralException {
		
		//List<String> list=new ArrayList<String>();
		
		SimpleDateFormat formatter = new SimpleDateFormat("yyyyMMddHHmm");
	
		CachedRowSet result = null;
		
		StringBuffer sql=new StringBuffer();
		
		sql.append("select DISTINCT M.UPC, I.ITEM_CODE from  MOVEMENT_DAILY M");
		sql.append(" left join ITEM_LOOKUP I on (I.UPC = '0' || M.UPC or I.UPC = M.UPC)");
		sql.append(" where M.COMP_STR_NO ='" + storeNumber + "'"  );
		sql.append(" and M.POS_TIMESTAMP >= To_DATE('");		
		sql.append(formatter.format(processfrom) + "', 'YYYYMMDDHH24MI')");
		sql.append(" and M.POS_TIMESTAMP <= To_DATE('") ;
		sql.append(formatter.format(processTo) + "', 'YYYYMMDDHH24MI')") ;		
		logger.debug(" Select UPC SQL "+ sql.toString());
		
		try {
			
			result=PristineDBUtil.executeQuery(_Conn, sql, "GetItemcodelist");
			
		}
		catch(Exception exe) {
			logger.error(exe.getMessage());
			throw new GeneralException("Item Code Fetching Error" , exe);
		}

		return result;
	}

	
	public String UpdateSql() {
	
		StringBuffer sql=new StringBuffer();
		sql.append(" update MOVEMENT_DAILY set ITEM_CODE=?  where UPC=? and COMP_STR_NO=? ");
		sql.append(" and POS_TIMESTAMP >= To_DATE(?, 'YYYYMMDDHH24MI')  ");
		sql.append(" and POS_TIMESTAMP <= To_DATE(?, 'YYYYMMDDHH24MI') ") ;
		logger.info(" Update Item Code SQL "+ sql.toString());
		return sql.toString();
	
	}

	public void UpdateItemcode(Connection _Conn,
			CachedRowSet resultItemCodeList, String storeNo, Date _processFrom,
			Date _processTo) throws SQLException {
		
		PreparedStatement psmt = _Conn.prepareStatement(UpdateSql());
		SimpleDateFormat formatter = new SimpleDateFormat("yyyyMMddHHmm");
		 
		while(resultItemCodeList.next()){
			//logger.info("UPC..." + resultItemCodeList.getString("UPC"));
			
			if ((resultItemCodeList.getString("ITEM_CODE") != null)) {
				psmt.setObject(1, resultItemCodeList.getString("ITEM_CODE"));
				psmt.setObject(2, resultItemCodeList.getString("UPC"));
				psmt.setObject(3, storeNo);
				psmt.setObject(4, formatter.format(_processFrom));
				psmt.setObject(5, formatter.format(_processTo));
				psmt.addBatch();
			} else {
				logger.info("Item Code Not Avilable for Upc  "
						+ resultItemCodeList.getString("UPC"));
			}
		}
		logger.info("Update begins....");
		
		int[] count =psmt.executeBatch();
		
		logger.info(" Insert Count " +count.length);
		psmt.close();
		
	}
}