package com.pristine.dao.riteaid;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Types;
import java.util.*;

import org.apache.log4j.Logger;

import com.pristine.dao.IDAO;
import com.pristine.dto.ItemDTO;
import com.pristine.exception.GeneralException;
import com.pristine.util.Constants;
import com.pristine.util.PristineDBUtil;

public class PostLIGSetupDAO implements IDAO {

	static Logger logger = Logger.getLogger("com.pristine.dao.riteaid.PostLIGSetupDAO");
	private static final int commitCount = Constants.LIMIT_COUNT;
	private ArrayList<Integer> retLirIdList = new ArrayList<Integer>(); 
	HashMap<String, Integer> retailerIdandNameMap;
	HashMap<String, Integer> retailerIdandCodeMap;
	private int retLirIdStart = 0;
	
	private static final String UPDATE_LIKE_ITEM_ID_AND_PRESTO_LIR_IND = "UPDATE ITEM_LOOKUP SET RET_LIR_ID = ? ,PRESTO_ASSIGNED_LIR_IND = ? WHERE ITEM_CODE = ?";
	
	public List<ItemDTO> getAllActiveItemsWithOriginalItemCode(Connection conn) throws GeneralException {
		List<ItemDTO> itemLookup = new ArrayList<>();
		String qry = "SELECT USER_ATTR_VAL_1, USER_ATTR_VAL_2, ITEM_NAME, ITEM_CODE, RETAILER_ITEM_CODE, LIR_IND, RET_LIR_ID, PRESTO_ASSIGNED_LIR_IND "
				+ " FROM ITEM_LOOKUP WHERE ACTIVE_INDICATOR = 'Y' AND USER_ATTR_VAL_1 IS NOT NULL ORDER BY USER_ATTR_VAL_1";
		
		PreparedStatement statement = null;
		ResultSet resultSet = null;
		try {
			statement = conn.prepareStatement(qry);
			resultSet = statement.executeQuery();
			while (resultSet.next()) {
				
				ItemDTO itemDTO = new ItemDTO();
				
				itemDTO.setOrgItemCode(resultSet.getString("USER_ATTR_VAL_1"));
				itemDTO.setActItemCode(resultSet.getString("USER_ATTR_VAL_2"));
				
				itemDTO.setItemName(resultSet.getString("ITEM_NAME"));
				itemDTO.setItemCode(resultSet.getInt("ITEM_CODE"));
				itemDTO.setRetailerItemCode(resultSet.getString("RETAILER_ITEM_CODE"));
				if(resultSet.getString("PRESTO_ASSIGNED_LIR_IND") != null)
				{
					itemDTO.setPrestoAssignedLirInd(true);	
				}
				
				/*String lirInd = resultSet.getString("LIR_IND");
				if(String.valueOf(Constants.YES).equalsIgnoreCase(lirInd)){
					itemDTO.setLirInd(true);
				} else {
					itemDTO.setLirInd(false);
				}*/
				
				itemDTO.setLikeItemId(resultSet.getInt("RET_LIR_ID"));
				itemLookup.add(itemDTO);
			}
		} catch (SQLException sqlE) {
			logger.error("Error -- getAllActiveItemsWithOriginalItemCode()", sqlE);
			throw new GeneralException("Error -- getAllActiveItemsWithOriginalItemCode()", sqlE);
		} finally {
			PristineDBUtil.close(resultSet);
			PristineDBUtil.close(statement);
		}

		return itemLookup;
	}
	
	private void setRetLirIdList(Connection conn) {
		String query = "SELECT DISTINCT(RET_LIR_ID) FROM RETAILER_LIKE_ITEM_GROUP";
		ArrayList<Integer> retLirIdList = new ArrayList<Integer>();
		PreparedStatement stmt = null;
		ResultSet rs = null;
		try {
			stmt = conn.prepareStatement(query);
			// stmt.setFetchSize(50000);
			rs = stmt.executeQuery();
			while (rs.next()) {
				retLirIdList.add(rs.getInt("RET_LIR_ID"));
			}
		} catch (SQLException ex) {
			logger.error("Error when retrieving RET_LIR_ID list - " + ex.toString());
		} finally {
			PristineDBUtil.close(stmt);
		}
		this.retLirIdList = retLirIdList;
	}
	
	public int setupRetailerLikeItem(Connection conn, String likeItemCode, String likeItemName) throws GeneralException {
		
		if (retLirIdList.size() <= 0) {
			//etRetLirIdList(conn);
			//Collections.sort(retLirIdList);
		}
		
		String likeItemStr = null;
		StringBuffer sb = new StringBuffer();
		
		if (likeItemName != null && !likeItemName.equals("")) {
			if (retailerIdandNameMap != null) {
				if (retailerIdandNameMap.get(likeItemName.trim().toUpperCase()) != null) {
					likeItemStr = String.valueOf(retailerIdandNameMap.get(likeItemName.trim().toUpperCase()));
				}
			}
			else {
				sb.append(" SELECT RET_LIR_ID FROM RETAILER_LIKE_ITEM_GROUP ");
				sb.append(" WHERE RET_LIR_NAME = '" + likeItemName.replaceAll("'", "''") + "' ");
				likeItemStr = PristineDBUtil.getSingleColumnVal(conn, sb, "getRetailerLikeItem");
			}
			// Update RET_LIR_CODE
			if (likeItemStr != null && likeItemCode != null && !likeItemCode.equals("")) {
				sb = new StringBuffer();
				sb.append(" UPDATE RETAILER_LIKE_ITEM_GROUP SET RET_LIR_CODE = '");
				sb.append(likeItemCode);
				sb.append("' WHERE RET_LIR_NAME = '" + likeItemName.replaceAll("'", "''") + "' ");
				PristineDBUtil.executeUpdate(conn, sb, "Update RET_LIR_CODE");
				if (retailerIdandCodeMap != null) {
					retailerIdandCodeMap.put(likeItemCode, Integer.parseInt(likeItemStr));
				}
			}
		}

		if (likeItemStr == null) {

			sb = new StringBuffer();
			sb.append(" INSERT INTO RETAILER_LIKE_ITEM_GROUP ");
			sb.append("(RET_LIR_ID, RET_LIR_CODE, RET_LIR_NAME)");
			sb.append(" VALUES ( ");

			// Changes to fill gaps in RET_LIR_SEQ
			boolean isRetLirIdSet = false;
			if (retLirIdList != null && retLirIdList.size() > 0)
				for (int i = retLirIdStart + 1; i <= retLirIdList.get(retLirIdList.size() - 1); i++) {
					if (retLirIdList.contains(i))
						continue;
					else {
						sb.append(i);
						sb.append(",");
						retLirIdStart = i;
						isRetLirIdSet = true;
						break;
					}
				}
			if (!isRetLirIdSet) {
				sb.append("RET_LIR_SEQ.NEXTVAL,");
			}
			
			if (likeItemCode != null && !likeItemCode.equals(""))
				sb.append("'").append(likeItemCode).append("',");
			else
				sb.append("NULL,");
			sb.append("'").append(likeItemName.replaceAll("'", "''")).append("')");
			try {
				PristineDBUtil.execute(conn, sb, "ItemDAO - Insert Like Item");
			} catch (GeneralException e) {
				logger.info("Error in Like Item Insert- " + sb.toString());
				throw e;
			}
			
			// Get the Like item code
			sb = new StringBuffer();
			sb.append(" SELECT RET_LIR_ID FROM RETAILER_LIKE_ITEM_GROUP WHERE RET_LIR_NAME = ");
			sb.append("'" + likeItemName.replaceAll("'", "''") + "' ");
			likeItemStr = PristineDBUtil.getSingleColumnVal(conn, sb, "getRetailerLikeItem");
			if (likeItemStr != null) {
				if (retailerIdandNameMap != null) {
					retailerIdandNameMap.put(likeItemName.trim().toUpperCase(), Integer.parseInt(likeItemStr));
				}
				if (retailerIdandCodeMap != null) {
					retailerIdandCodeMap.put(likeItemCode, Integer.parseInt(likeItemStr));
				}
			}
		}
		return Integer.parseInt(likeItemStr);

	}
	
	public void updateLikeItemIdAndPrestoLirInd(Connection conn, List<ItemDTO> itemList) throws GeneralException {
		PreparedStatement statement = null;
		try {
			String sql = UPDATE_LIKE_ITEM_ID_AND_PRESTO_LIR_IND;
			statement = conn.prepareStatement(sql);
			for (ItemDTO item : itemList) {
				int counter = 0;
				if (item.likeItemId > 0)
				{
					statement.setInt(++counter, item.likeItemId);
					if(item.getPrestoAssignedLirInd() == true)
					{
						statement.setString(++counter, "Y");
					}
					else
					{
						statement.setNull(++counter, Types.VARCHAR);
					}
				}
				else
				{
					statement.setNull(++counter, Types.INTEGER);
				}
				statement.setInt(++counter, item.itemCode);
				statement.addBatch();
			}
			int[] count = statement.executeBatch();
			statement.clearBatch();
		} catch (SQLException e) {
			PristineDBUtil.rollbackTransaction(conn, "Error");
			logger.error("Error while executing UPDATE_LIKE_ITEM_ID_AND_PRESTO_LIR_IND");
			throw new GeneralException("Error while executing UPDATE_LIKE_ITEM_ID_AND_PRESTO_LIR_IND " + e);
		} finally {
			PristineDBUtil.close(statement);
		}
	}
	
}
