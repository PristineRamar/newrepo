package com.pristine.dao.offermgmt;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.HashMap;

import org.apache.log4j.Logger;

import com.pristine.dto.offermgmt.PRBrandDTO;
import com.pristine.exception.GeneralException;
import com.pristine.util.PristineDBUtil;

public class BrandRelationDAO {
	private static Logger logger = Logger.getLogger("BrandRelationDAO");
	
	private static final String GET_BRAND_RELATION = "SELECT B.BRAND_RELN_ID, I.BRAND_ID, C.CLASS_TYPE, B.ITEM_CODE, R.RET_LIR_ITEM_CODE " +
													 "FROM PR_BRAND_RELN_DETAIL B, ITEM_LOOKUP I, RETAILER_LIKE_ITEM_GROUP R, PR_BRAND_CLASS_LOOKUP C " +
													 "WHERE B.ITEM_CODE = I.ITEM_CODE AND I.RET_LIR_ID = R.RET_LIR_ID(+) AND B.BRAND_CLASS_ID = C.BRAND_CLASS_ID";
	
	public HashMap<Integer, ArrayList<PRBrandDTO>> getBrandRelation(Connection conn) throws GeneralException{
		PreparedStatement stmt = null;
		ResultSet rs = null;
		HashMap<Integer, ArrayList<PRBrandDTO>> brandRelation = new HashMap<Integer, ArrayList<PRBrandDTO>>();
		try{
			stmt = conn.prepareStatement(GET_BRAND_RELATION);
			rs = stmt.executeQuery();
			while(rs.next()){
				int itemCode = -1;
				if(rs.getObject("RET_LIR_ITEM_CODE") != null){
					itemCode = rs.getInt("RET_LIR_ITEM_CODE");
				}else{
					itemCode = rs.getInt("ITEM_CODE");
				}
				PRBrandDTO brand = new PRBrandDTO();
				brand.setItemCode(itemCode);
				brand.setBrandRelnId(rs.getInt("BRAND_RELN_ID"));
				brand.setBrandId(rs.getInt("BRAND_ID"));
				brand.setBrandClass(rs.getString("CLASS_TYPE").charAt(0));
				if(brandRelation.get(itemCode) != null){
					ArrayList<PRBrandDTO> tList = brandRelation.get(itemCode);
					boolean relationExists = false;
					for(PRBrandDTO tBrand : tList){
						if(tBrand.getBrandRelnId() == brand.getBrandRelnId()){
							relationExists = true;
						}
					}
					if(!relationExists){
						tList.add(brand);
						brandRelation.put(itemCode, tList);
					}
				}else{
					ArrayList<PRBrandDTO> tList = new ArrayList<PRBrandDTO>();
					tList.add(brand);
					brandRelation.put(itemCode, tList);
				}
			}
		}catch(SQLException exception){
			logger.error("Error when retrieving brand relation - " + exception.toString());
			throw new GeneralException("Error when retrieving brand relation - " + exception.toString());
		}finally{
			PristineDBUtil.close(rs);
			PristineDBUtil.close(stmt);
		}
		return brandRelation;
	}

	public HashMap<Integer, String> getBrandRelationshipOverride( Connection conn, int categoryLevelId, int categoryId) 
			throws GeneralException {
		
		String GET_BRAND_RELATION_OVERRIDE = " SELECT product_level_id, product_id, tier, brand_relation_ovr FROM PR_RELATION_OVERRIDE " +
		" WHERE PRODUCT_LEVEL_ID = ? AND PRODUCT_ID = ? ";
		
		PreparedStatement stmt = null;
		ResultSet rs = null;
		HashMap<Integer, String> brandRelationOverride = new HashMap<Integer, String>();
		try{
			stmt = conn.prepareStatement(GET_BRAND_RELATION_OVERRIDE);
			stmt.setInt(1, categoryLevelId);
			stmt.setInt(2, categoryId);
			rs = stmt.executeQuery();
			while(rs.next()){
				int tier = rs.getInt("TIER");
				String brandOverride = rs.getString("BRAND_RELATION_OVR");	
				brandRelationOverride.put(tier, brandOverride);
			}
		}catch(SQLException exception){
			logger.error("Error when retrieving brand relationship Override - " + exception.toString());
			throw new GeneralException("Error when retrieving brand relationship Override - " + exception.toString());
		}finally{
			PristineDBUtil.close(rs);
			PristineDBUtil.close(stmt);
		}
		return brandRelationOverride;
	}
	
}