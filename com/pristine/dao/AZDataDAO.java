package com.pristine.dao;

import java.sql.Connection;

import javax.sql.rowset.CachedRowSet;

import org.apache.log4j.Logger;

import com.pristine.exception.GeneralException;
import com.pristine.util.PristineDBUtil;

public class AZDataDAO implements IDAO {
	
	static Logger	logger	= Logger.getLogger("AZDataDAO");
	public CachedRowSet getRelatedFamilyltems( Connection conn, int recUnitId)  throws GeneralException {
		
		StringBuffer sb = new StringBuffer();
		sb.append(" select il.retailer_item_code, il.item_name, il.item_size, ul.name as uom, il.user_attr_14 as family, il.user_attr_4 as tier, lir.ret_lir_name, ");
		sb.append(" lir.ret_lir_code from item_lookup il ");
		sb.append(" left join uom_lookup ul on il.uom_id = ul.id ");
		sb.append(" left join retailer_like_item_group lir on il.ret_lir_id = lir.ret_lir_id where il.item_code in ");
		sb.append(" (SELECT child_product_id FROM (SELECT CHILD_PRODUCT_ID,CHILD_PRODUCT_LEVEL_ID FROM PRODUCT_GROUP_RELATION_REC PGR ");
		sb.append("  start with product_level_id = 7");
		sb.append("  and product_id = ").append(recUnitId);
		sb.append("  connect by prior child_product_id = product_id and prior child_product_level_id = product_level_id ");
		sb.append(" ) WHERE CHILD_PRODUCT_LEVEL_ID = 1) ");
		sb.append(" and il.active_indicator = 'Y' ");
		sb.append(" and il.lir_ind = 'N' ");
		sb.append(" and  il.user_attr_14 in (  ");
		sb.append(" select B.user_attr_14  from item_lookup B ");
		sb.append(" where B.Item_Code in ");
		sb.append(" (SELECT child_product_id FROM (SELECT CHILD_PRODUCT_ID,CHILD_PRODUCT_LEVEL_ID FROM PRODUCT_GROUP_RELATION_REC PGR ");
		sb.append("  start with product_level_id = 7");
		sb.append("  and product_id = ").append(recUnitId);
		sb.append("  connect by prior child_product_id = product_id and prior child_product_level_id = product_level_id ");
		sb.append(" ) WHERE CHILD_PRODUCT_LEVEL_ID = 1)");
		sb.append(" AND B.user_attr_14 is not null ");
		sb.append(" group by b.user_attr_14 ");
		sb.append(" having count(distinct b.user_attr_4) > 1 )");
		//sb.append(" AND ROWNUM < 100");
		sb.append(" order by il.user_attr_14, to_number(il.user_attr_4) ");

		//logger.debug(sb.toString());
		CachedRowSet crs = PristineDBUtil.executeQuery(conn, sb, "getRelatedFamilyItems");
		return crs;
		
	}

}
