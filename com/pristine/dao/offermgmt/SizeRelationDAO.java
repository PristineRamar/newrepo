package com.pristine.dao.offermgmt;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.HashMap;

import org.apache.log4j.Logger;

import com.pristine.dto.offermgmt.PRSizeDTO;
import com.pristine.exception.GeneralException;
import com.pristine.util.PristineDBUtil;

public class SizeRelationDAO {
	private static Logger logger = Logger.getLogger("SizeRelationDAO");
	
	private static final String GET_SIZE_RELATION = "SELECT S.SIZE_RELN_ID, C.CLASS_NAME, S.ITEM_CODE, I.ITEM_SIZE, R.RET_LIR_ITEM_CODE " +
													 "FROM PR_SIZE_RELN_DETAIL S, ITEM_LOOKUP I, RETAILER_LIKE_ITEM_GROUP R, PR_SIZE_CLASS_LOOKUP C " +
													 "WHERE S.ITEM_CODE = I.ITEM_CODE AND I.RET_LIR_ID = R.RET_LIR_ID(+) AND S.SIZE_CLASS_ID = C.SIZE_CLASS_ID";
	
	public HashMap<Integer, PRSizeDTO> getSizeRelation(Connection conn) throws GeneralException{
		PreparedStatement stmt = null;
		ResultSet rs = null;
		HashMap<Integer, PRSizeDTO> sizeRelation = new HashMap<Integer, PRSizeDTO>();
		try{
			stmt = conn.prepareStatement(GET_SIZE_RELATION);
			rs = stmt.executeQuery();
			while(rs.next()){
				int itemCode = -1;
				if(rs.getObject("RET_LIR_ITEM_CODE") != null){
					itemCode = rs.getInt("RET_LIR_ITEM_CODE");
				}else{
					itemCode = rs.getInt("ITEM_CODE");
				}
				PRSizeDTO size = new PRSizeDTO();
				size.setItemCode(itemCode);
				size.setSizeRelnId(rs.getInt("SIZE_RELN_ID"));
				size.setSizeClass(rs.getString("CLASS_NAME"));
				size.setItemSize(rs.getInt("ITEM_SIZE"));
				sizeRelation.put(itemCode, size);
			}
		}catch(SQLException exception){
			logger.error("Error when retrieving size relation - " + exception.toString());
			throw new GeneralException("Error when retrieving size relation - " + exception.toString());
		}finally{
			PristineDBUtil.close(rs);
			PristineDBUtil.close(stmt);
		}
		return sizeRelation;
	}
}
