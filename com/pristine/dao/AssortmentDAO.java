package com.pristine.dao;

import java.sql.Connection;

import javax.sql.rowset.CachedRowSet;

import org.apache.log4j.Logger;
import com.pristine.exception.GeneralException;
import com.pristine.util.PristineDBUtil;

public class AssortmentDAO implements IDAO {
	static Logger	logger	= Logger.getLogger("AssortmentDAO");

	/*
select distinct dept_Id,  dept_name, category_id, category_NAME  
from competitive_data_view where schedule_id in (5637)
	 */
	public CachedRowSet getCheckedCategories( Connection conn, int[] schList, boolean showFlavors, boolean carriedOnly) throws GeneralException {
		
		CachedRowSet crs = null;
		StringBuffer sb = new StringBuffer();
		sb.append("select distinct dept_Id,  dept_name, category_id, category_NAME from  ");
		if ( showFlavors)
			sb.append(" competitive_data_view ");
		else
			sb.append(" comp_data_view_no_flavor ");
		sb.append(" where schedule_id in ( ");
		for ( int i= 0; i < schList.length; i++){
			sb.append(schList[i]);
			if ( i< schList.length -1)
				sb.append(",");
		}
		sb.append(")");
		if(carriedOnly)
			sb.append(" AND RETAILER_ITEM_CODE <> '0'");
		sb.append(" order by dept_name, category_NAME");
		logger.debug(sb.toString());
		crs = PristineDBUtil.executeQuery(conn, sb, "getSchedulesForStore");
		return crs;
	}
	
	public CachedRowSet getCompStoresForItem(Connection conn, String scharr, int itemCode, boolean showFlavors ) throws GeneralException {
		StringBuffer buffer = new StringBuffer();
		buffer.append(" SELECT distinct COMP_STR_ID, COMP_STR_NO ");
		if( showFlavors)
			buffer.append(" from Competitive_data_view A");
		else
			buffer.append(" from comp_data_view_no_flavor A");
		buffer.append(" WHERE schedule_id in (").append( scharr).append(")");
		buffer.append(" AND item_code =" + itemCode);
		buffer.append(" AND ITEM_NOT_FOUND_FLG = 'N'" );
		//logger.debug(buffer.toString());
		CachedRowSet result = PristineDBUtil.executeQuery(conn, buffer, "getCompDataForItem");
		return result;
		
	}

}
